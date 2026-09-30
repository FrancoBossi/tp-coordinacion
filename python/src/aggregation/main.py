import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    #se inicializa los componentes middleware y estructuras de estado interno
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_request = {}
        self.completed_sums_by_request = {}
        self.shutdown_requested = False

    def _process_data(self, request_id, fruit, amount):
        logging.info("Processing data message")
        # IInserta el elemento de forma ordenada
        fruit_top = self.fruit_top_by_request.setdefault(request_id, [])
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, int(amount)))

    def _process_eof(self, request_id, sum_id):
        #Registra la recepcion de EOF de una replica de Sum
        logging.info("Received EOF from Sum")
        completed_sums = self.completed_sums_by_request.setdefault(request_id, set())
        completed_sums.add(int(sum_id))
        if len(completed_sums) < SUM_AMOUNT:
            return

        fruit_top = self.fruit_top_by_request.pop(request_id, [])
        self.completed_sums_by_request.pop(request_id, None)

        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()

        serialized_fruit_top = list(
            map(
                lambda item: (item.fruit, item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(
            message_protocol.internal.serialize(
                [request_id, "PARTIAL_TOP", ID, serialized_fruit_top]
            )
        )

    def process_messsage(self, message, ack, nack):
        #Deserializa y procesa los mensajes entrantes (DATA o EOF) administrando el ACK
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 4 and fields[1] == "DATA":
            self._process_data(fields[0], fields[2], fields[3])
        elif len(fields) == 3 and fields[1] == "EOF":
            self._process_eof(fields[0], fields[2])
        else:
            nack()
            return
        ack()

    def start(self):
        #Comienza la recepcion de mensajes desde el exchange de entrada
        self.input_exchange.start_consuming(self.process_messsage)

    def request_shutdown(self):
        #Detiene el bucle de consumo de mensajes
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        self.input_exchange.stop_consuming()

    def shutdown(self):
        #Cierra las conexiones a RabbitMQ
        self.request_shutdown()
        self.input_exchange.close() #se cierra la conexion
        self.output_queue.close() #se cierra la conexion


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    def handle_signal(signum, frame):
        logging.info(f"Signal {signum} received, stopping AggregationFilter...")
        aggregation_filter.request_shutdown()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        aggregation_filter.start()
    finally:
        aggregation_filter.shutdown()
    return 0


if __name__ == "__main__":
    main()