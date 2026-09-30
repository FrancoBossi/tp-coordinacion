import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops_by_request = {}
        self.shutdown_requested = False

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 4 or fields[1] != "PARTIAL_TOP":
            nack()
            return
        request_id, _, aggregation_id, partial_top = fields
        partial_tops = self.partial_tops_by_request.setdefault(request_id, {})
        partial_tops[aggregation_id] = partial_top
        if len(partial_tops) == AGGREGATION_AMOUNT:
            all_items = [
                fruit_item.FruitItem(fruit, int(amount))
                for partial_top in partial_tops.values()
                for fruit, amount in partial_top
            ]
            all_items.sort()
            all_items.reverse()
            top_chunk = all_items[:TOP_SIZE]
            final_top = [(item.fruit, item.amount) for item in top_chunk]

            self.partial_tops_by_request.pop(request_id, None)
            self.output_queue.send(
                message_protocol.internal.serialize(
                    [request_id, "FINAL_TOP", final_top]
                )
            )
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def request_shutdown(self):
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        self.input_queue.stop_consuming()

    def shutdown(self):
        self.request_shutdown()
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def handle_signal(signum, frame):
        logging.info(f"Signal {signum} received, stopping JoinFilter...")
        join_filter.request_shutdown()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        join_filter.start()
    finally:
        join_filter.shutdown()

    return 0


if __name__ == "__main__":
    main()