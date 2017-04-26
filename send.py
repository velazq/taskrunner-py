#!/usr/bin/env python3
# coding: utf-8
import os
import sys
import json
import pika
import uuid
import redis

host = os.environ["MASTER_IP"]

redisdb = redis.StrictRedis(host=host)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
channel = connection.channel()
channel.queue_declare(queue="task_queue", durable=True)

def send(filepath, source_code):
    task_id = str(uuid.uuid4())
    msg = {"task_id": task_id, "filepath": filepath, "source_code": source_code}
    jsonmsg = json.dumps(msg)
    channel.basic_publish(exchange="",
                          routing_key="task_queue",
                          body=jsonmsg,
                          properties=pika.BasicProperties(
                             delivery_mode=2, # make message persistent
                          ))
    # print(" [x] Sent %r" % jsonmsg)
    return task_id

def get_reply(task_id):
    reply = redisdb.brpop(task_id)[1]
    redisdb.delete(task_id)
    return json.loads(reply)

def get_files(folder, ext):
    return (os.path.abspath(os.path.join(dirpath, filename))
            for (dirpath, dirnames, filenames) in os.walk(folder)
            for filename in filenames
            if filename.endswith(ext))

def dispatch(filepath):
    with open(filepath) as f:
        return send(f.name, f.read())

task_ids = [dispatch(f) for d in sys.argv[1:] for f in get_files(d, "py")]

[print(get_reply(id)) for id in task_ids]

connection.close()
