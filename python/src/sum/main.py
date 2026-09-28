import os
import logging
import threading
import hashlib
import time
import pika

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
SUM_BATCH_SIZE = int(os.environ.get("SUM_BATCH_SIZE", "1000"))

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.control_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["EOF"]
        )
        self.amount_by_request = {}
        self.state_lock = threading.Lock()

    def _aggregation_index(self, request_id, fruit):
        """Distribuye cada fruta de una consulta en un único Aggregator."""
        partition_key = f"{request_id}:{fruit}"
        digest = hashlib.sha256(partition_key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % AGGREGATION_AMOUNT

    def _flush_request(self, request_id):
        """Envía un lote y libera el acumulador de una consulta."""
        amount_by_fruit = self.amount_by_request.pop(request_id, {})
        for final_fruit_item in amount_by_fruit.values():
            data_output_exchange = self.data_output_exchanges[
                self._aggregation_index(request_id, final_fruit_item.fruit)
            ]
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [
                        request_id,
                        "DATA",
                        final_fruit_item.fruit,
                        final_fruit_item.amount,
                    ]
                )
            )

    def _process_data(self, request_id, fruit, amount):
        logging.info(f"Process data")
        with self.state_lock:
            # Cada cliente conserva su propio acumulador para evitar mezclar consultas.
            amount_by_fruit = self.amount_by_request.setdefault(request_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

            if len(amount_by_fruit) >= SUM_BATCH_SIZE:
                logging.info("Flushing data batch")
                self._flush_request(request_id)

    def _process_eof(self, request_id):
        logging.info(f"Broadcasting data messages")
        # Envía el último lote; los anteriores ya fueron liberados al alcanzar el límite.
        self._flush_request(request_id)

        logging.info(f"Publishing EOF notification for sum {ID}")
        self.control_publisher.send(
            message_protocol.internal.serialize([request_id, "EOF", ID])
        )

    def _process_control_message(self, message, ack, nack, output_exchanges):
        """Procesa un EOF difundido para que cada Sum cierre su estado local."""
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 3 or fields[1] != "EOF":
            nack()
            return
        self._wait_for_input_queue_drain()
        with self.state_lock:
            self._flush_request_to_exchanges(fields[0], output_exchanges)
            for data_output_exchange in output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize([fields[0], "EOF", ID])
                )
        ack()

    def _wait_for_input_queue_drain(self):
        """Espera a que todos los datos previos al EOF sean entregados a un Sum."""
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=MOM_HOST)
        )
        channel = connection.channel()
        try:
            while True:
                queue_state = channel.queue_declare(
                    queue=INPUT_QUEUE, passive=True
                )
                if queue_state.method.message_count == 0:
                    return
                time.sleep(0.01)
        finally:
            connection.close()

    def _flush_request_to_exchanges(self, request_id, output_exchanges):
        """Envía un lote usando exchanges pertenecientes al hilo consumidor."""
        amount_by_fruit = self.amount_by_request.pop(request_id, {})
        for final_fruit_item in amount_by_fruit.values():
            output_exchange = output_exchanges[
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

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 4 and fields[1] == "DATA":
            self._process_data(fields[0], fields[2], fields[3])
        elif len(fields) == 2 and fields[1] == "EOF":
            self.control_publisher.send(
                message_protocol.internal.serialize([fields[0], "EOF", ID])
            )
        else:
            nack()
            return
        ack()

    def start(self):
        # El exchange de control permite notificar EOF a todas las réplicas de Sum.
        def consume_control():
            control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, ["EOF"]
            )
            output_exchanges = [
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
                )
                for i in range(AGGREGATION_AMOUNT)
            ]
            control_exchange.start_consuming(
                lambda message, ack, nack: self._process_control_message(
                    message, ack, nack, output_exchanges
                )
            )

        control_thread = threading.Thread(target=consume_control, daemon=True)
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
