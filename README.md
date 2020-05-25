# esgf-pid

**Deprecated branch**

esgf-pid is a python library that is part of the ESGF data publication. It is responsible for triggering PID creation. To do this, it sends JSON messages to a RabbitMQ Messaging Queue.

The library is tested using python 2.7. Please check out the newer versions that support python 2.7 and 3.7 and more recent pika versions.

Access to a RabbitMQ server with the necessary queues is required. The server data has to be provided to the library.

## Collaborators

* Merret Buurman (DKRZ)

* Katharina Berger (DKRZ)
