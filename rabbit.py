"""This module provides the implementation of the message queue."""
import pika


class Rabbit:

    def __init__(self, address, queue, talking=False, callback=None):
        """Summon a rabbit to deliver messages.

        Args:
            address: the rabbit-server address.
            queue: the queue name.
            talking: on which mode the rabbit will be, talking or listening?
            callback: the callback function if the rabbit will listen.
        """
        self.messenger = pika.BlockingConnection(
            pika.ConnectionParameters(address))
        self.channel = self.messenger.channel()
        self.channel.queue_declare(queue=queue, durable=True)
        self.queue = queue

        if not talking:
            assert callback is not None, "A callback function is required for a listening rabbit."
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue,
                                       on_message_callback=callback,
                                       auto_ack=False)

    def speak(self, message):
        """Send a mesage."""
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2))

    def start_listening(self):
        """Listen to the comming messages."""
        self.channel.start_consuming()