# esgf-pid

esgf-pid is a python library that is part of the ESGF data publication. It is responsible for triggering PID creation. To do this, it sends JSON messages to a RabbitMQ Messaging Queue.

The library is tested using python 2.7 and recently using python 3.7.

Up to version 0.7.17, pika==0.11.2 should be used. After that, pika>=1.0.1 should be used.


Access to a RabbitMQ server with the necessary queues is required. The server data has to be provided to the library.

## Collaborators

* Merret Buurman (DKRZ)

* Katharina Berger (DKRZ)

* mauzey1

