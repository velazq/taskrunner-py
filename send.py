#!/usr/bin/env python3
# coding: utf-8
import os
import sys
import json
import pika
import uuid
import redis
import argparse


def send(filepath, source_code, channel):
    task_id = str(uuid.uuid4())
    msg = {"task_id": task_id, "filepath": filepath, "source_code": source_code}
    jsonmsg = json.dumps(msg)
    channel.basic_publish(exchange="",
                          routing_key="task_queue",
                          body=jsonmsg,
                          properties=pika.BasicProperties(
                             delivery_mode=2, # make message persistent
                          ))
    return task_id

def get_reply(task_id, redisdb):
    reply = redisdb.brpop(task_id)[1]
    redisdb.delete(task_id)
    return json.loads(reply)

def get_files(folder, ext):
    return (os.path.abspath(os.path.join(dirpath, filename))
            for (dirpath, dirnames, filenames) in os.walk(folder)
            for filename in filenames
            if filename.endswith(ext))

def dispatch(filepath, channel):
    with open(filepath) as f:
        return send(f.name, f.read(), channel)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("rabbitmq_url", help="rabbitmq url, ex: amqp://host1")
    parser.add_argument("redis_url", help="redis url, ex: redis://192.168.1.2")
    parser.add_argument("folder", help="folder to be sent")
    args = parser.parse_args()

    redisdb = redis.StrictRedis.from_url(args.redis_url)

    connection = pika.BlockingConnection(pika.URLParameters(args.rabbitmq_url))
    channel = connection.channel()
    channel.queue_declare(queue="task_queue", durable=True)

    task_ids = [dispatch(f, channel) for f in get_files(args.folder, ".py")]

    [print(get_reply(task_id, redisdb)) for task_id in task_ids]

    connection.close()


if __name__ == "__main__":
    main()
