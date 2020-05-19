from stream_consumer import StreamConsumer
from logger import logger
import json
import io
import yaml

class Consumer:
    def start_streaming(self, connection_data):
        consumer = StreamConsumer(connection_data, lambda event: self.__on_event_callback(event))
        try:
            consumer.run()
        except Exception as e:
            logger.error(traceback.format_exc())
            time.sleep(3)
            raise e

    def __on_event_callback(self, event_json):
        logger.info('Received event')
        decoded_event = json.loads(event_json)
        logger.info('Publishing event {}'.format(decoded_event))


config = yaml.load(io.open('stream.yml'), Loader=yaml.FullLoader)

Consumer().start_streaming(config)
