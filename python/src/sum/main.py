import os
import logging
import threading
import hashlib
import signal

from common import middleware, message_protocol, fruit_item

# Configuración del nodo desde variables de entorno
ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        # Cola de ingesta principal
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        # Exchanges de salida hacia las replicas de Aggregation
        self.data_output_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

        # Exchanges de envio para comunicacion inter-sum (hilo principal)
        self.sum_inter_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{i}"]
            )
            for i in range(SUM_AMOUNT)
        ]

        # Exchange de recepcion de datos re-enrutados (hilo secundario)
        self.sum_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{ID}"]
        )

        # Diccionarios de estado interno protegidos por lock
        self.amount_by_request = {}
        self.eof_broadcast_sent = set()
        self.eofs_received_by_request = {}
        self.closed_requests = set()

        self.state_lock = threading.Lock()
        self.sum_inter_thread = None
        self.inter_sum_publishers = None
        self.shutdown_requested = False

    def _sum_owner_index(self, request_id: str, fruit: str) -> int:
        #Calcula el ID de la replica de Sum responsable de acumular la fruta dada

        partition_key = f"{request_id}:{fruit}"
        digest = hashlib.sha256(partition_key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % SUM_AMOUNT

    def _aggregation_index(self, request_id: str, fruit: str) -> int:
        #Calcula el ID del Aggregator de destino para enviar los totales de una fruta

        partition_key = f"{request_id}:{fruit}"
        digest = hashlib.sha256(partition_key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % AGGREGATION_AMOUNT

    def _add_local_amount(self, request_id: str, fruit: str, amount: int):
        #Acumula en memoria local el valor especificado utilizando el operador + de la clase FruitItem
       
        with self.state_lock:
            amount_by_fruit = self.amount_by_request.setdefault(request_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _trigger_eof_broadcast(self, request_id: str, publishers=None):
        #Trasmite el mensaje INTER_SUM_EOF a todas las replicas de Sum

        with self.state_lock:
            if request_id in self.eof_broadcast_sent:
                return
            self.eof_broadcast_sent.add(request_id)

        pubs = publishers if publishers is not None else self.sum_inter_exchanges
        for pub in pubs:
            pub.send(
                message_protocol.internal.serialize(
                    [request_id, "INTER_SUM_EOF", ID]
                )
            )

    def process_data_message(self, message: bytes, ack: callable, nack: callable):
        #Callback encargada de procesar los mensajes de la cola de ingesta principal.Redirige el dato al nodo Sum correspondiente segun la funcion de hash o
        #dispara la senial  de finalizacion al recibir un EOF.
        
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 4 and fields[1] == "DATA":
            request_id, _, fruit, amount = fields
            target_sum_id = self._sum_owner_index(request_id, fruit)
            if target_sum_id == ID:
                self._add_local_amount(request_id, fruit, amount)
            else:
                self.sum_inter_exchanges[target_sum_id].send(
                    message_protocol.internal.serialize(
                        [request_id, "INTER_SUM_DATA", fruit, amount]
                    )
                )
        elif len(fields) == 3 and fields[1] == "EOF":
            request_id = fields[0]
            self._trigger_eof_broadcast(request_id)
        else:
            nack()
            return
        ack()

    def _process_inter_sum_message(self, message: bytes, ack: callable, nack: callable):
        #Callback invocada en el hilo secundario para procesar mensajes inter-sum.Acumula registros reenviados por otros nodos Sum y coordina la barrera desincronizacio EOF
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 4 and fields[1] == "INTER_SUM_DATA":
            self._add_local_amount(fields[0], fields[2], fields[3])
        elif len(fields) == 3 and fields[1] == "INTER_SUM_EOF":
            request_id, _, sender_id = fields

            # Disparar broadcast propio si no sabiamos que ya se habia enviado
            self._trigger_eof_broadcast(request_id, self.inter_sum_publishers)

            should_close = False
            with self.state_lock:
                received = self.eofs_received_by_request.setdefault(request_id, set())
                received.add(int(sender_id))
                if len(received) == SUM_AMOUNT and request_id not in self.closed_requests:
                    should_close = True

            if should_close:
                # Agendar el cierre thread-safe sobre la conexion del hilo principal
                self.input_queue.connection.add_callback_threadsafe(
                    lambda req=request_id: self._close_request(req)
                )
        else:
            nack()
            return
        ack()

    def _close_request(self, request_id: str):
        #Cierra el request_id especifico: envia las sumas finales acumuladas a los nodos Aggregator correspondientes y emite los mensajes EOF de salida
        with self.state_lock:
            if request_id in self.closed_requests:
                return
            self.closed_requests.add(request_id)
            amount_by_fruit = self.amount_by_request.pop(request_id, {})
            self.eofs_received_by_request.pop(request_id, None)

        for final_fruit_item in amount_by_fruit.values():
            output_exchange = self.data_output_exchanges[
                self._aggregation_index(request_id, final_fruit_item.fruit)
            ]
            output_exchange.send(
                message_protocol.internal.serialize(
                    [
                        request_id,
                        "DATA",
                        final_fruit_item.fruit,
                        final_fruit_item.amount,
                    ]
                )
            )

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize([request_id, "EOF", ID])
            )

    def start(self):
        #Inicia el hilo secundario para mensajes inter-sum y arranca el consumo de la cola principal
        def consume_inter_sum():
            # Conexiones exclusivas del hilo secundario para evitar race conditions en Pika
            self.inter_sum_publishers = [
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{i}"]
                )
                for i in range(SUM_AMOUNT)
            ]
            self.sum_input_exchange.start_consuming(
                lambda message, ack, nack: self._process_inter_sum_message(
                    message, ack, nack
                )
            )

        self.sum_inter_thread = threading.Thread(target=consume_inter_sum, daemon=True)
        self.sum_inter_thread.start()

        self.input_queue.start_consuming(self.process_data_message)

    def request_shutdown(self):
        #Detiene de forma no bloqueante los bucles de consumo de RabbitMQ
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        self.input_queue.stop_consuming()
        if self.sum_input_exchange is not None:
            self.sum_input_exchange.connection.add_callback_threadsafe(
                self.sum_input_exchange.stop_consuming
            )

    def shutdown(self):
        #Libera de forma limpia todas las conexiones y canales creados en RabbitMQ
        self.request_shutdown()
        if self.sum_inter_thread is not None and self.sum_inter_thread.is_alive():
            self.sum_inter_thread.join(timeout=5)

        self.input_queue.close()
        if self.sum_input_exchange is not None:
            self.sum_input_exchange.close()
        if self.inter_sum_publishers is not None:
            for pub in self.inter_sum_publishers:
                pub.close()
        for ex in self.sum_inter_exchanges:
            ex.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def handle_signal(signum, frame):
        logging.info(f"Signal {signum} received, stopping SumFilter...")
        sum_filter.request_shutdown()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        sum_filter.start()
    finally:
        sum_filter.shutdown()
    return 0


if __name__ == "__main__":
    main()