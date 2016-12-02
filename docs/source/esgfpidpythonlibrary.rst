

.. _anchor_library:

=====================================
esgfpid Python library documentation
=====================================

esgfpid is a python library that is part of the ESGF data
publication. It is responsible for triggering PID creation.
To do this, it sends JSON messages to a RabbitMQ Messaging Queue.

**Availability**. The library is available from pypi (https://pypi.python.org/pypi/esgfpid/)
and GitHub (https://github.com/IS-ENES-Data/esgf-pid).

**Requirements**. The library is tested using python 2.7.
Access to a RabbitMQ server with the necessary queues is
required. The server data has to be provided to the library
(see :ref:`anchor_init`).

**Dependencies**. The packages "requests" and "pika" have
to be installed.


Usage of the library for perform PID tasks
===========================================

General usage
--------------------------

**(1)** First, you create an instance of the client (see :ref:`anchor_init`).
It holds all necessary config to connect to the RabbitMQ messaging
system and perform PID creations and updates.

  .. code:: python

    connector = esgfpid.Connector(...)

**(2)** Next, start the connection to RabbitMQ (which runs in a parallel
thread, see :ref:`anchor_thread`)

  .. code:: python

    connector.start_messaging_thread()


**(3)** Then, use the client's methods to create/update PIDs for datasets,
files and data carts. Some tasks are simple methods (e.g. unpublication,
see :ref:`anchor_unpubli`), others require the creation of another
object (e.g. dataset publication, see :ref:`anchor_publi`).

  .. code:: python

    connector.unpublish_one_version(...)

**(4)** Finally, stop the connection to RabbitMQ and wait for any pending
messages.

  .. code:: python

    connector.finish_messaging_thread()

.. _anchor_publi:

Task "Publication"
-------------------

Publication includes first publication and republication of both
original copies and replicas.

#. Create a publication assistant: :meth:`~esgfpid.connector.Connector.create_publication_assistant`
#. Add information about all files individually: :meth:`~esgfpid.assistant.publish.DatasetPublicationAssistant.add_file`
#. Tell the assistant that all files have been added and the publication should be finished: :meth:`~esgfpid.assistant.publish.DatasetPublicationAssistant.dataset_publication_finished`.

  .. code:: python

    assistant = connector.create_publication_assistant(...)
    assistant.add_file(...)
    assistant.add_file(...)
    assistant.dataset_publication_finished()

.. _anchor_unpubli:

Task "Unpublication"
---------------------

You can either...

* Unpublish one dataset version: :meth:`~esgfpid.connector.Connector.unpublish_one_version`, or...
* unpublish all available dataset versions: :meth:`~esgfpid.connector.Connector.unpublish_all_versions`

  .. code:: python

    connector.unpublish_one_version(...)
    connector.unpublish_all_versions(...)

Task "Update Errata"
----------------------

Errata ids can be added and removed to dataset PID records.

* Add errata: :meth:`~esgfpid.connector.Connector.add_errata_ids`
* Remove errata: :meth:`~esgfpid.connector.Connector.remove_errata_ids`

  .. code:: python

    connector.add_errata_ids(...)
    connector.remove_errata_ids(...)


Task "Data cart PID"
---------------------

Data carts are ... [TODO]

* Data cart PIDs: :meth:`~esgfpid.connector.Connector.create_shopping_cart_pid`

  .. code:: python

    connector.create_shopping_cart_pid(...)



.. _anchor_thread:

Asynchronous messaging thread
=============================

It is important to start and stop the parallel thread that takes care
of the asynchronous communication with the RabbitMQ messaging queue.

* Use :meth:`~esgfpid.connector.Connector.start_messaging_thread` to start
  the thread.

* Use :meth:`~esgfpid.connector.Connector.finish_messaging_thread` to stop
  the thread, but give it some time to publish remaining messages
  that are waiting in the stack, and to wait for outstanding confirms.

* Use :meth:`~esgfpid.connector.Connector.force_finish_messaging_thread` to
  immediately stop the parallel thread, without giving it some time.

.. important:: Please do not forget to finish the thread at the end,
    using :meth:`~esgfpid.connector.Connector.finish_messaging_thread`
    or :meth:`~esgfpid.connector.Connector.force_finish_messaging_thread`.

.. note:: Please note that KeyboardInterrupts may not work while a
    second thread is running. They need to be explicitly caught in
    the code calling the esgfpid library.

For more technical documentation of how the asynchronous thread
works, please see :ref:`anchor_asynchronous`.


.. _anchor_init:

Initializing the library
========================

.. automethod:: esgfpid.connector.Connector.__init__



Full method documentation
=========================

.. automethod:: esgfpid.connector.Connector.create_publication_assistant

.. automethod:: esgfpid.connector.Connector.unpublish_one_version

.. automethod:: esgfpid.connector.Connector.unpublish_all_versions

.. automethod:: esgfpid.connector.Connector.add_errata_ids

.. automethod:: esgfpid.connector.Connector.remove_errata_ids

.. automethod:: esgfpid.connector.Connector.create_shopping_cart_pid

.. automethod:: esgfpid.connector.Connector.make_handle_from_drsid_and_versionnumber

.. automethod:: esgfpid.connector.Connector.start_messaging_thread

.. automethod:: esgfpid.connector.Connector.finish_messaging_thread

.. automethod:: esgfpid.connector.Connector.force_finish_messaging_thread

.. automethod:: esgfpid.assistant.publish.DatasetPublicationAssistant.get_dataset_handle

.. automethod:: esgfpid.assistant.publish.DatasetPublicationAssistant.add_file

.. automethod:: esgfpid.assistant.publish.DatasetPublicationAssistant.dataset_publication_finished
