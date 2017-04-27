#!/usr/bin/env python3
# coding: utf-8
import os
import sys
import json
import docker
import multiprocessing

def watch(client, restart_queue, image_name):
    for evt in client.events(filters={"image": image_name}):
        json_docs = evt.decode().split("\n")
        for json_doc in json_docs:
            if json_doc:
                event = json.loads(json_doc)
                if event["status"] == "die":
                    container_id = event["id"]
                    restart_queue.put(container_id)

def restart(client, restart_queue, fallback_image_name, master_ip):
    while True:
        container_id = restart_queue.get()
        if container_id:
            print("Restarting", container_id)
            container = client.containers.get(container_id)
            container.restart()
        else:
            print("Starting new container")
            client.containers.run(fallback_image_name, detach=True,
                environment={"MASTER_IP": master_ip})

def main():
    if len(sys.argv) < 3:
        print("Usage: {} <image_name> <num_containers>".format(sys.argv[0]))
        sys.exit(1)

    image_name = sys.argv[1]
    num_containers = int(sys.argv[2])

    client = docker.from_env()

    master_ip = os.environ["MASTER_IP"]

    restart_queue = multiprocessing.Queue()

    num_restarter_threads = 5

    restarter_threads = [multiprocessing.Process(target=restart,
        args=(client, restart_queue, image_name, master_ip))
        for _ in range(num_restarter_threads)]

    [thread.start() for thread in restarter_threads]

    [restart_queue.put("") for _ in range(num_containers)]

    watch(client, restart_queue, image_name)

    [thread.join() for thread in restarter_threads] # Unreachable

if __name__ == "__main__":
    main()
