#!/usr/bin/env python3
# coding: utf-8
import os
import json
import pika
import time
import redis
import tempfile
import subprocess

host = os.environ["MASTER_IP"]

redisdb = redis.StrictRedis(host=host)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
channel = connection.channel()
channel.queue_declare(queue="task_queue", durable=True)

def callback(ch, method, properties, body):
    # print(" [x] Received %r" % body)
    msg = json.loads(body)
    task_id = msg["task_id"]
    filepath = msg["filepath"]
    source_code = msg["source_code"]
    f = tempfile.NamedTemporaryFile(mode="w", dir="/tmp/data", suffix=".py")
    f.write(source_code)
    f.flush()
    p = subprocess.Popen(["python3", f.name],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out, err = p.communicate()
    msg = {"task_id": task_id, "filepath": filepath, "stdout": out.decode(),
        "stderr": err.decode(), "returncode": p.returncode}
    res = json.dumps(msg)
    redisdb.rpush(task_id, res)
    # print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    connection.close()

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue="task_queue")
channel.start_consuming()
