#!/usr/bin/env python3
# coding: utf-8
import os
import sys
import json
import docker
import argparse


def run(image_name, rabbitmq_url, redis_url):
    os.system('''docker run --rm -d {} \
        --ulimit nofile=1024:1024 \
        /worker.py {} {} &'''
        .format(image_name, rabbitmq_url, redis_url))

def watch(image_name, rabbitmq_url, redis_url):
    client = docker.from_env()
    for evt in client.events(filters={'image': image_name}):
        json_docs = evt.decode().split('\n')
        for json_doc in json_docs:
            if json_doc:
                event = json.loads(json_doc)
                if event['status'] == 'die':
                    run(image_name, rabbitmq_url, redis_url)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('rabbitmq_url', help='rabbitmq url, ex: amqp://host1')
    parser.add_argument('redis_url', help='redis url, ex: redis://192.168.1.2')
    parser.add_argument('image_name', help='image name, ex: alvelazq/taskrunner-py')
    parser.add_argument('num_containers', help='number of containers on this host', type=int)
    args = parser.parse_args()

    for _ in range(args.num_containers):
        run(args.image_name, args.rabbitmq_url, args.redis_url)

    try:
        watch(args.image_name, args.rabbitmq_url, args.redis_url)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
