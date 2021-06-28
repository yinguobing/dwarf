"""The steward watches the file list and dispatch the tasks."""
import os
import sys

import pika
import yaml


if __name__ == '__main__':
    # Load the configuration file.
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
    with open("config.yml", 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    try:
        # Setup the post office.
        post_office = pika.BlockingConnection(
            pika.ConnectionParameters(cfg['rabbitmq']['address']))
        channel = post_office.channel()

        queue_name = cfg['rabbitmq']['queue']
        channel.queue_declare(queue=queue_name, durable=True)

        def callback(ch, method, properties, body):
            src_file = body.decode()
            print(src_file)
            ch.basic_ack(delivery_tag = method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name,
                              on_message_callback=callback,
                              auto_ack=False)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
