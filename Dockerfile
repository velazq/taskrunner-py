FROM python

RUN pip3 install pika redis
COPY *.py /
RUN chmod +x /*.py

RUN useradd nopriv
USER nopriv
RUN mkdir /tmp/data
