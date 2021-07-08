"""This module provides the implementation of the message queue."""
import logging
import logging.config

import pika
import yaml

# Setup the logger.
logging.config.dictConfig(yaml.load(open("logging.yml", 'r'), yaml.FullLoader))
logger = logging.getLogger('rabbit')


class Rabbit:

    def __init__(self, address, port, queue, talking=False, callback=None):
        """Summon a rabbit to deliver messages.

        Args:
            address: the rabbit-server address.
            port: the rabbit-server port.
            queue: the queue name.
            talking: on which mode the rabbit will be, talking or listening?
            callback: the callback function if the rabbit will listen.
        """
        self._recipe = pika.ConnectionParameters(address, port)
        self._connection = None
        self._channel = None
        self._queue = queue
        self._talking = talking
        self._callback = callback

        # Safety check.
        if not self._talking:
            assert self._callback is not None, "A callback function is required for a listening rabbit."
        
        # Ready the rabbit.
        self.stand_up()

    def stand_up(self):
        """The rabbit has to stand up before it speaks."""
        logger.debug("Trying to stand up...")

        self._connection = pika.BlockingConnection(self._recipe)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self._queue, durable=True)

    def speak(self, message):
        """Send a mesage. This process may fail if the other rabbits had waited
        for a long time. So at least try twice.
        """
        for _ in ["once", "twice"]:
            try:
                self._channel.basic_publish(
                    exchange='',
                    routing_key=self._queue,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2))
                succeed = True
            except pika.exceptions.StreamLostError:
                logger.error("The rabbit can not speak, trying again...")
                self.stand_up()
                succeed = False
            finally:
                if succeed:
                    break

        return succeed

    def start_listening(self):
        """Listen to the comming messages."""
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self._queue,
                                    on_message_callback=self._callback,
                                    auto_ack=False)
        self._channel.start_consuming()

    def rest(self):
        """Let the rabbit rest."""
        self._connection.close()
