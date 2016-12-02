========================================
Overall structure of the CMIP6 PID setup
========================================

(TODO: Extend this documentation!)

PIDs in CMIP6
=============

Datasets and files in CMIP6 receive PIDs (persistent
identifiers). They contain some basic information
about the datasets and files, such as their filenames,
dataset ids, version numbers, ... Information on their
storage locations, on errata and on and previous and
following versions, are also kept track of.

The PIDs are created when the datasets and files are
first published. Then, they are updated during any
event that affects the metadata:

* Republication
* Publication of replicas
* Unpublication
* Addition or removal of errata ids

How is it done?
===============

PIDs are created/updated by a servlet that runs on
a server at DKRZ.

This servlet receives PID creation/update requests
from ESGF software components that carry out the
above events, e.g. the publisher or the errata
modules.

These software components use the esgfpid python
library to issue the requests, which are little JSON
messages.

To avoid blocking those software components if many
PID requests are issued at the same time, a messaging
queue is added in front of the servlet. All requests
are sent to the messaging queue that keeps them until
the PID creation servlet has capacity to process them.


esgfpid library
================

The esgfpid python library is used to send information
about events to the messaging queue (see :ref:`anchor_library`).

Message Queueing System
=========================

The message queuing is done by a federated instance
of RabbitMQ servers.

PID creation servlet
====================

The servlet that creates/updates the PIDs consumes the
queue of messages (thus it is called queueconsumer or
consumer or consuming servlet).
It is a Java servlet that runs on the same server as the
PID database for fast write access.
