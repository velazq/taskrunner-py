#!/usr/bin/env python3
# coding: utf-8
import os
import sys
import json
import docker
import threading

if len(sys.argv) < 3:
    print("Usage: {} <image_name> <num_containers>".format(sys.argv[0]))
    sys.exit(1)

image_name = sys.argv[1]
num_containers = int(sys.argv[2])

client = docker.from_env()

master_ip = os.environ["MASTER_IP"]

def watch(image):
    for evt in client.events(filters={"image": image}):
        # print(evt, "\n\n")
        json_docs = evt.decode().split("\n")
        for json_doc in json_docs:
            if json_doc:
                event = json.loads(json_doc)
                print(event["id"], " -> ", event["status"])
                if event["status"] == "die":
                    container_id = event["id"]
                    container = client.containers.get(container_id)
                    container.restart()

# thread = threading.Thread(target=watch, args=[image_name])
# thread.start()

for _ in range(num_containers):
    client.containers.run(image_name, detach=True, environment={"MASTER_IP": master_ip})

print("Ready.")

# thread.join()

watch(image_name)