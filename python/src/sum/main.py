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
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
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
        self.control_publishers = [
            middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_control_{i}"
            )
            for i in range(SUM_AMOUNT)
        ]
        self.inter_sum_control_publishers = [
            middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_control_{i}"
            )
            for i in range(SUM_AMOUNT)
        ]
        self.inter_sum_input = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{SUM_PREFIX}_inter_{ID}"
        )
        self.inter_sum_outputs = [
            middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_inter_{i}"
            )
            for i in range(SUM_AMOUNT)
        ]
        self.amount_by_request = {}
        self.local_processed_by_request = {}
        self.progress_by_request = {}
        self.expected_by_request = {}
        self.closed_requests = set()
        self.state_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        self.control_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{SUM_PREFIX}_control_{ID}"
        )
        self.control_thread = None
        self.inter_sum_thread = None
        self.shutdown_requested = False

    def _aggregation_index(self, request_id, fruit):
        #Distribuye cada fruta de una consulta en un unico Aggregator
        partition_key = f"{request_id}:{fruit}"
        digest = hashlib.sha256(partition_key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % AGGREGATION_AMOUNT

    def _process_data(self, request_id, fruit, amount):
        logging.info(f"Process data")
        with self.state_lock:
            # Cada cliente conserva su propio acumulador para evitar mezclar otras/futuras consultas
            amount_by_fruit = self.amount_by_request.setdefault(request_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _sum_owner_index(self, request_id, fruit):
        partition_key = f"{request_id}:{fruit}"
        digest = hashlib.sha256(partition_key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % SUM_AMOUNT

    def _process_eof(self, request_id):
        logging.info(f"Publishing EOF notification for sum {ID}")
        self._publish_control([request_id, "EOF", ID], self.control_publishers)

    def _process_control_message(self, message, ack, nack):
        #Actualiza la barrera de progreso y cierra consultas completas
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
        #Indica si todos los registros de una consulta ya fueron procesados
        expected = self.expected_by_request.get(request_id)
        progress = self.progress_by_request.get(request_id, {})
        return (
            expected is not None
            and sum(progress.values()) >= expected
            and request_id not in self.closed_requests
        )

    def _schedule_close(self, request_id):
        #Programa el cierre en el hilo que publica los datos de Sum
        self.input_queue.connection.add_callback_threadsafe(
            lambda: self._close_request(request_id)
        )

    def _close_request(self, request_id):
        #Publica el cierre despues de que todos los Sum procesaron sus datos
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
        #Envia un lote usando exchanges pertenecientes al hilo consumidor
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
            request_id, fruit, amount = fields[0], fields[2], fields[3]
            owner_id = self._sum_owner_index(request_id, fruit)
            if owner_id != ID:
                self.inter_sum_outputs[owner_id].send(
                    message_protocol.internal.serialize(
                        [request_id, "INTER_SUM_DATA", fruit, amount]
                    )
                )
            else:
                self._process_owned_data(
                    request_id, fruit, amount, self.control_publishers
                )
        elif len(fields) == 3 and fields[1] == "EOF":
            self._publish_control(
                [fields[0], "EOF_REQUEST", fields[2]], self.control_publishers
            )
        else:
            nack()
            return
        ack()

    def _process_owned_data(self, request_id, fruit, amount, control_publisher):
        self._process_data(request_id, fruit, amount)
        with self.state_lock:
            processed = self.local_processed_by_request.get(request_id, 0) + 1
            self.local_processed_by_request[request_id] = processed
        self._publish_control(
            [request_id, "PROGRESS", [ID, processed]], control_publisher
        )

    @staticmethod
    def _publish_control(fields, publishers):
        message = message_protocol.internal.serialize(fields)
        for publisher in publishers:
            publisher.send(message)

    def process_inter_sum_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 4 or fields[1] != "INTER_SUM_DATA":
            nack()
            return
        request_id, fruit, amount = fields[0], fields[2], fields[3]
        if self._sum_owner_index(request_id, fruit) != ID:
            nack()
            return
        self._process_owned_data(
            request_id, fruit, amount, self.inter_sum_control_publishers
        )
        ack()

    def start(self):
        # El exchange de control permite notificar EOF a todas las replicas de Sum
        def consume_control():
            self.control_queue.start_consuming(
                lambda message, ack, nack: self._process_control_message(
                    message, ack, nack
                )
            )

        self.control_thread = threading.Thread(target=consume_control, daemon=True)
        self.control_thread.start()
        self.inter_sum_thread = threading.Thread(
            target=lambda: self.inter_sum_input.start_consuming(
                self.process_inter_sum_message
            ),
            daemon=True,
        )
        self.inter_sum_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)

    def request_shutdown(self):
        #Solicita detener los consumidores sin cerrar conexiones activas
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        self.shutdown_event.set()
        self.input_queue.stop_consuming()
        self.inter_sum_input.connection.add_callback_threadsafe(
            self.inter_sum_input.stop_consuming
        )
        self.control_queue.connection.add_callback_threadsafe(
            self.control_queue.stop_consuming
        )

    def shutdown(self):
        #Cierra las conexiones de Sum despues de detener los consumidores
        self.request_shutdown()
        if self.control_thread is not None:
            self.control_thread.join(timeout=5)
        if self.inter_sum_thread is not None:
            self.inter_sum_thread.join(timeout=5)
        self.input_queue.close()
        self.inter_sum_input.close()
        for inter_sum_output in self.inter_sum_outputs:
            inter_sum_output.close()
        self.control_queue.close()
        for publisher in self.control_publishers:
            publisher.close()
        for publisher in self.inter_sum_control_publishers:
            publisher.close()
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