import os
import logging
import threading
import hashlib
import signal

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
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["CONTROL"]
        )
        self.amount_by_request = {}
        self.local_processed_by_request = {}
        self.progress_by_request = {}
        self.expected_by_request = {}
        self.closed_requests = set()
        self.state_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        self.control_exchange = None
        self.control_thread = None
        self.shutdown_requested = False

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

    def _process_control_message(self, message, ack, nack):
        """Actualiza la barrera de progreso y cierra consultas completas."""
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 3:
            nack()
            return
        request_id, message_type, value = fields
        with self.state_lock:
            if message_type == "EOF_REQUEST":
                self.expected_by_request[request_id] = int(value)
            elif message_type == "PROGRESS":
                self.progress_by_request.setdefault(request_id, {})[
                    int(value[0])
                ] = int(value[1])
            else:
                nack()
                return
            should_close = self._request_is_complete(request_id)
        if should_close:
            self._schedule_close(request_id)
        ack()

    def _request_is_complete(self, request_id):
        """Indica si todos los registros de una consulta ya fueron procesados."""
        expected = self.expected_by_request.get(request_id)
        progress = self.progress_by_request.get(request_id, {})
        return (
            expected is not None
            and sum(progress.values()) >= expected
            and request_id not in self.closed_requests
        )

    def _schedule_close(self, request_id):
        """Programa el cierre en el hilo que publica los datos de Sum."""
        self.input_queue.connection.add_callback_threadsafe(
            lambda: self._close_request(request_id)
        )

    def _close_request(self, request_id):
        """Publica el cierre después de que todos los Sum procesaron sus datos."""
        with self.state_lock:
            if request_id in self.closed_requests:
                return
            self.closed_requests.add(request_id)
            self._flush_request_to_exchanges(request_id, self.data_output_exchanges)
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize([request_id, "EOF", ID])
                )
            self.local_processed_by_request.pop(request_id, None)
            self.progress_by_request.pop(request_id, None)
            self.expected_by_request.pop(request_id, None)

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
            with self.state_lock:
                processed = (
                    self.local_processed_by_request.get(fields[0], 0) + 1
                )
                self.local_processed_by_request[fields[0]] = processed
            self.control_publisher.send(
                message_protocol.internal.serialize(
                    [fields[0], "PROGRESS", [ID, processed]]
                )
            )
        elif len(fields) == 3 and fields[1] == "EOF":
            self.control_publisher.send(
                message_protocol.internal.serialize(
                    [fields[0], "EOF_REQUEST", fields[2]]
                )
            )
        else:
            nack()
            return
        ack()

    def start(self):
        # El exchange de control permite notificar EOF a todas las réplicas de Sum.
        def consume_control():
            self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, ["CONTROL"]
            )
            self.control_exchange.start_consuming(
                lambda message, ack, nack: self._process_control_message(
                    message, ack, nack
                )
            )

        self.control_thread = threading.Thread(target=consume_control, daemon=True)
        self.control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)

    def request_shutdown(self):
        """Solicita detener los consumidores sin cerrar conexiones activas."""
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        self.shutdown_event.set()
        self.input_queue.stop_consuming()
        if self.control_exchange is not None:
            self.control_exchange.connection.add_callback_threadsafe(
                self.control_exchange.stop_consuming
            )

    def shutdown(self):
        """Cierra las conexiones de Sum después de detener los consumidores."""
        self.request_shutdown()
        if self.control_thread is not None:
            self.control_thread.join(timeout=5)
        self.input_queue.close()
        if self.control_exchange is not None:
            self.control_exchange.close()
        self.control_publisher.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: sum_filter.request_shutdown(),
    )
    try:
        sum_filter.start()
    finally:
        sum_filter.shutdown()
    return 0


if __name__ == "__main__":
    main()
