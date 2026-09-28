import os
import logging
import threading

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
        self.amount_by_request = {}

    def _flush_request(self, request_id):
        """Envía un lote y libera el acumulador de una consulta."""
        amount_by_fruit = self.amount_by_request.pop(request_id, {})
        for final_fruit_item in amount_by_fruit.values():
            for data_output_exchange in self.data_output_exchanges:
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

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize([request_id, "EOF"])
            )


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 4 and fields[1] == "DATA":
            self._process_data(fields[0], fields[2], fields[3])
        elif len(fields) == 2 and fields[1] == "EOF":
            self._process_eof(fields[0])
        else:
            nack()
            return
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
