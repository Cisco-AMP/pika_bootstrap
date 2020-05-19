import time
import traceback
from random import randint
import ssl
import pika
from pika.exceptions import AMQPChannelError, ConnectionClosed, NoFreeChannels
from logger import logger
import pdb


class StreamConsumer(object):
    # Reconnection timeout with 2-second +- random window
    RECONNECT_TIMEOUT = 3
    AMQP_PROTO = 'amqps'

    def __init__(self, connection_data, on_event_callback):
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._queue_name = connection_data['queue_name']
        self._url = "{}://{}:{}@{}:{}".format(self.AMQP_PROTO, connection_data['user_name'],
                                              connection_data['password'], connection_data['host'],
                                              connection_data['port'])
        credentials = pika.PlainCredentials(connection_data['user_name'], connection_data['password'])
        cxt = ssl.create_default_context()
        cxt.check_hostname = connection_data['ssl_check_hostname']
        cxt.verify_mode = eval(connection_data['ssl_verify_mode'])
        ssl_options = pika.SSLOptions(context=cxt)
        self._connection_parameters = pika.ConnectionParameters(host = connection_data['host'],
                                                                port = connection_data['port'],
                                                                virtual_host = connection_data['vhost'],
                                                                credentials = credentials,
                                                                ssl_options = ssl_options)

        logger.warn(self._url)
        self.on_event_callback = on_event_callback

    def run(self):
        try:
            logger.debug('Connecting to {}'.format(self._url))
            self._connection = pika.BlockingConnection(self._connection_parameters)
            self.start_consuming()
        except (AMQPChannelError, ConnectionClosed, NoFreeChannels) as e:
            logger.warn('Connection error ({}, {}: {})! Reconnecting in about {} seconds'
                        .format(time.time(), e.__class__, repr(e), self.RECONNECT_TIMEOUT))
            time.sleep(self.RECONNECT_TIMEOUT + randint(-2, 2))
            self.cleanup_maybe_reconnect()
        except Exception:
            logger.error('Consumer Error that does not look like connection failure! See the traceback below.')
            logger.error(traceback.format_exc())
            self.cleanup_maybe_reconnect(False)

    def cleanup_maybe_reconnect(self, needs_reconnection=True):
        try:
            if self._channel is not None and self._channel.is_open:
                self._channel.stop_consuming()
            if self._connection is not None and self._connection.is_open:
                self._connection.close()
        except ConnectionClosed:
            pass
        if needs_reconnection:
            self.run()

    def start_consuming(self):
        self._channel = self._connection.channel()
        self._channel.queue_declare(self._queue_name, passive=True)
        self._channel.basic_consume(queue=self._queue_name, on_message_callback=self.on_message)
        logger.info('Connected. Starting to consume.')
        self._channel.start_consuming()

    def on_message(self, _channel, basic_deliver, _properties, body):
        logger.debug('Received message with rk:' + basic_deliver.routing_key)
        self.on_event_callback(body)
        self._channel.basic_ack(basic_deliver.delivery_tag)
