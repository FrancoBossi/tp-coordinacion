import pika
import random
import string
from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)
from pika.exceptions import (
    AMQPConnectionError,
    AMQPError,
    ConnectionWrongStateError,
    ChannelWrongStateError,
)

#funcion a usar para conectarse a rabbitMQ
def connection_parameters(host):
    return pika.ConnectionParameters(host=host)

#generador de nombres unicos para la cola de un subscriber
def random_queue_name(name):
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=16))
    return f"{name}_{suffix}"

#Convierte errores de Pika en las excepciones definidas por el middleware
def raise_message_error(error):
    if isinstance(
        error,
        (
            AMQPConnectionError,
            ConnectionWrongStateError,
            ChannelWrongStateError,
        ),
    ):
        raise MessageMiddlewareDisconnectedError() from error
    raise MessageMiddlewareMessageError() from error


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self.connection = pika.BlockingConnection(connection_parameters(host)) #Conexion TCP con el broker RabbitMQ
            self.channel = self.connection.channel()#Canal AMQP utilizado para declarar, publicar y consumir mensajes
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.queue_name = queue_name #Nombre de la cola compartida por productores y consumidores
            self.consuming = False #Indica si este middleware esta ejecutando un consumo
        except (AMQPError, OSError) as error:
            raise_message_error(error)

    def start_consuming(self, on_message_callback):
        #Consume mensajes hasta que el callback solicite detenerse
        def callback(channel, method, properties, body):
            # El callback recibe funciones que confirman o rechazan este mensaje.
            def ack():
                #Confirma a RabbitMQ la recepción correcta del mensaje
                try:
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                except (AMQPError, OSError) as error:
                    raise_message_error(error)

            def nack():
                #Rechaza el mensaje y evita que vuelva a encolarse
                try:
                    channel.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=False,
                    )
                except (AMQPError, OSError) as error:
                    raise_message_error(error)

            try:
                on_message_callback(body, ack, nack)
            except (
                MessageMiddlewareDisconnectedError,
                MessageMiddlewareMessageError,
            ):
                raise
            except Exception as error:
                raise MessageMiddlewareMessageError() from error

        try:
            # Un prefetch de uno reparte el trabajo entre consumidores sin
            # entregar nuevos mensajes a uno que aun no confirmo el anterior
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )
            self.consuming = True
            self.channel.start_consuming()
        except (AMQPError, OSError) as error:
            raise_message_error(error)
        finally:
            self.consuming = False

    def stop_consuming(self):
        #Detiene el consumo actual pero si no comenzo entonces  no realiza ninguna accion
        if not self.consuming:
            return
        try:
            self.channel.stop_consuming()
            self.consuming = False
        except (AMQPError, OSError) as error:
            raise_message_error(error)

    def send(self, message):
        #Publica un mensaje directamente en la cola de trabajo
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
            )
        except (AMQPError, OSError) as error:
            raise_message_error(error)

    def close(self):
        #Cerramos la conexion con RabbitMQ si todavia esta abierta
        try:
            if self.connection.is_open:
                self.connection.close()
        except (AMQPError, OSError) as error:
            raise MessageMiddlewareCloseError() from error

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        #Conectamos con un exchange direct y prepara la cola del subscriber
        try:
            self.connection = pika.BlockingConnection(connection_parameters(host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct",
                durable=True,
            )
            # Nombre del exchange direct utilizado por publishers y subscribers
            self.exchange_name = exchange_name
            # Routing keys a las que el subscriber se suscribe o publica
            self.routing_keys = tuple(routing_keys)
            # Nombre  de la cola exclusiva de este subscriber
            self.queue_name = random_queue_name(exchange_name)
            # Cada instancia tiene una cola propia: asi todos los subscribers reciben una copia del mensaje publicado en sus routing keys
            self.channel.queue_declare(
                queue=self.queue_name,
                exclusive=True,
                auto_delete=True,
            )
            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )
            # Indica si este middleware esta ejecutando un consumo.
            self._consuming = False
        except (AMQPError, OSError) as error:
            raise_message_error(error)

    def start_consuming(self, on_message_callback):
        #Consume los mensajes recibidos por los bindings del subscriber
        def callback(channel, method, properties, body):
            # ack y nack operan sobre el mensaje actual mediante su delivery tag.
            def ack():
                #Confirma a RabbitMQ la recepcion correcta del mensaje
                try:
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                except (AMQPError, OSError) as error:
                    raise_message_error(error)

            def nack():
                #Rechaza el mensaje y evita que vuelva a encolarse.
                try:
                    channel.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=False,
                    )
                except (AMQPError, OSError) as error:
                    raise_message_error(error)

            try:
                on_message_callback(body, ack, nack)
            except (
                MessageMiddlewareDisconnectedError,
                MessageMiddlewareMessageError,
            ):
                raise
            except Exception as error:
                raise MessageMiddlewareMessageError() from error

        try:
            # El prefetch limita los mensajes sin confirmar y evita que un
            # subscriber lento acumule trabajo innecesariamente.
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )
            self._consuming = True
            self.channel.start_consuming()
        except (AMQPError, OSError) as error:
            raise_message_error(error)
        finally:
            self._consuming = False

    def stop_consuming(self):
        #Detiene el consumo actual si no comenzo, no realiza ninguna accion
        if not self._consuming:
            return
        try:
            self.channel.stop_consuming()
            self._consuming = False
        except (AMQPError, OSError) as error:
            raise_message_error(error)

    def send(self, message):
        #Publica el mensaje usando todos los routing keys configurados
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except (AMQPError, OSError) as error:
            raise_message_error(error)

    def close(self):
        #Cierra la conexion y elimina la cola  del subscriber
        try:
            if self.connection.is_open:
                self.connection.close()
        except (AMQPError, OSError) as error:
            raise MessageMiddlewareCloseError() from error