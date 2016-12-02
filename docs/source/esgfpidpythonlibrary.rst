==============================
ESGF PID Client documentation
==============================

TODO

General usage
=============


.. important:: You must start and stop the thread before making any
    PID creation/update request. See below.

First, you create an instance of the client. It holds all necessary
config to connect to the RabbitMQ messaging system and perform PID
creations and updates.

  .. code:: python

    connector = esgfpid.Connector(...)


Then, use the client's methods to create/update PIDs for datasets,
files and data carts. Some methods are simple methods, others require
the creation of another object.

Simple method:

  .. code:: python

    connector.unpublish_one_version(...)


Create another object:

  .. code:: python

    assistant = connector.create_publication_assistant(...)
    assistant.add_file(...)
    assistant.add_file(...)
    assistant.dataset_publication_finished()

  
Asynchronous communication with messaging queue
================================================

TODO: Description. Docstring of asynchronous module?

Starting the asynchronous thread
  Use :meth:`~esgfpid.connector.Connector.start_messaging_thread` to start
  the parallel thread that takes care of the asynchronous communication
  with the RabbitMQ messaging queue.

Stopping the asynchronous thread
  Use :meth:`~esgfpid.connector.Connector.finish_messaging_thread` to stop
  the parallel thread, but give it some time to publish remaining messages
  that are waiting in the stack, and to wait for outstanding confirms.
  Use :meth:`~esgfpid.connector.Connector.force_finish_messaging_thread` to
  immediately stop the parallel thread, without giving it some time.
  

Full method documentation
=========================

.. automethod:: esgfpid.connector.Connector.__init__

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
