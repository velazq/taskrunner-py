#!/usr/bin/env python3
# coding: utf-8
import json
import pika
import time
import redis
import argparse
import tempfile
import subprocess


def callback(ch, method, body, connection, redisdb):
    msg = json.loads(body)
    task_id = msg["task_id"]
    filepath = msg["filepath"]
    source_code = msg["source_code"]
    f = tempfile.NamedTemporaryFile(mode="w", dir="/tmp/data", suffix=".py")
    f.write(source_code)
    f.flush()
    p = subprocess.Popen(["python3", f.name], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    out, err = p.communicate()
    msg = {"task_id": task_id, "filepath": filepath, "stdout": out.decode(),
        "stderr": err.decode(), "returncode": p.returncode}
    res = json.dumps(msg)
    redisdb.rpush(task_id, res)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    connection.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("rabbitmq_url", help="rabbitmq url, ex: amqp://host1")
    parser.add_argument("redis_url", help="redis url, ex: redis://192.168.1.2")
    args = parser.parse_args()

    redisdb = redis.StrictRedis.from_url(args.redis_url)

    connection = pika.BlockingConnection(pika.URLParameters(args.rabbitmq_url))
    channel = connection.channel()
    channel.queue_declare(queue="task_queue", durable=True)
    channel.basic_qos(prefetch_count=1)
    cb = lambda ch, method, _, body: callback(ch, method, body, connection, redisdb)
    channel.basic_consume(cb, queue="task_queue")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    main()
