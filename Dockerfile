FROM python
RUN mkdir /tmp/data
RUN pip3 install pika redis
COPY *.py /
WORKDIR /
CMD ["python3", "/worker.py"]
