Manual tests for esgfpid library
=====================================


Merret Buurman, DKRZ, 2020-03-20


Purpose
-----------

These test script are for testing the most common functionalities
and most common mis-configurations of the esgfpid library:

* Connecting to RabbitMQ
* Sending messages to RabbitMQ
* Routing of the messages to the correct queues

These are not unit tests.

They have to be run manually and their results have to be checked
by a person, on RabbitMQ's queues (did the expected messages arrive),
and in the logs (did errors occur), as esgfpid swallows quite some
problems or handles them itself (due to the requirement not to interrupt
the publications).

Preparations
---------------

You need to have a RabbitMQ server on which you can create test queues and
exchanges.

* Create a test user account with access to the 'esgfpid' vhost.
* Create a test exchange "test123" of type "topic" or "direct" (please
  not "fanout", because then we cannot test the unroutable behaviour)
* Create a test queue with any name

Now create bindings between those, using the following routing keys:

* 2114100.HASH.fresh.publi-ds-repli
* 2114100.HASH.fresh.publi-file-repli
* 2114100.HASH.fresh.unpubli-allvers
* 2114100.HASH.fresh.unpubli-onevers
* PREFIX.HASH.fresh.preflightcheck
* UNROUTABLE.UNROUTABLE.fresh.UNROUTABLE
* 2114100.HASH.fresh.datacart
* 2114100.HASH.fresh.errata-add
* 2114100.HASH.fresh.errata-rem 

Run the tests
----------------

Each test script will print the expected outcome at the end, i.e. what
to check for logs/queues.


```
$RABBIT='myrabbit.abcd.ef'
$USER='alice'
$PW='supersecurity'

python3 test01_file_dataset_publication.py $HOST $USER $PW
python3 test02_check_method.py $HOST $USER $PW
python3 test03_unpublication.py $HOST $USER $PW
python3 test04_datacart.py $HOST $USER $PW
python3 test05_errata.py $HOST $USER $PW
python3 test06_force_finish.py $HOST $USER $PW
python3 test07_ssl.py $HOST $USER $PW
python3 test08_wrong_exchange.py $HOST $USER $PW

```

For the remainder, you need to remove some bindings again (to test error handling):

```
# Remove binding "2114100.HASH.fresh.unpubli-allvers"
python3 test09_unroutable_not_drop.py $HOST $USER $PW

# Remove binding "PREFIX.HASH.fresh.preflightcheck"
python3 test10_preflightcheck_wrong_routingkey.py $HOST $USER $PW

# Remove binding "UNROUTABLE.UNROUTABLE.fresh.UNROUTABLE"
# (Remove binding "2114100.HASH.fresh.unpubli-allvers" if not done yet)
python3 test11_unroutable_drop.py $HOST $USER $PW

# Restore bindings for next tests

```