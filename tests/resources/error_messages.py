# Expected error messages for the unit tests
# of the check.py module.

expected_message_ok1 = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication and connection (this.is.my.only.host)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... ok.
 .. checking exchange ...
 .. checking exchange ... ok.
Config for PID module (rabbit messaging queue).. ok.
Successful connection to PID messaging queue at "this.is.my.only.host".'''

expected_message_ok2 = None # no message returned when success occurs!

expected_message_plus_testmsg_ok1 = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication and connection (this.is.my.only.host)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... ok.
 .. checking exchange ...
 .. checking exchange ... ok.
 .. checking message ...
 .. checking message ... ok.
Config for PID module (rabbit messaging queue).. ok.
Successful connection to PID messaging queue at "this.is.my.only.host".'''

expected_message_plus_testmsg_ok2 = None # no message returned when success occurs!

expected_message_first_connection_failed_then_ok1 = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking connection (this.is.my.favourite.host)... FAILED.
 .. giving this node a lower priority..
 .. checking connection (mystery-tour.uk)... FAILED.
 .. giving this node a lower priority..
 .. checking authentication and connection (tomato.salad-with-spam.fr)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... ok.
 .. checking exchange ...
 .. checking exchange ... ok.
Config for PID module (rabbit messaging queue).. ok.
Successful connection to PID messaging queue at "tomato.salad-with-spam.fr".'''

expected_message_first_connection_failed_then_ok2 = None # no message returned when success occurs!

expected_message_connection_failed_unknown1 = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking connection (this.is.my.only.host)... FAILED.
 .. giving this node a lower priority..
Config for PID module (rabbit messaging queue) .. FAILED!'''

expected_message_connection_failed_unknown2 = '''********************************************************************
*** PROBLEM IN SETTING UP                                        ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                          ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:   ***
***  - host "this.is.my.only.host": Unknown connection failure.  ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE. ***
********************************************************************'''


expected_message_connection_failed_only1 = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking connection (this.is.my.only.host)... FAILED.
 .. giving this node a lower priority..
Config for PID module (rabbit messaging queue) .. FAILED!'''

expected_message_connection_failed_only2 = '''*********************************************************************************
*** PROBLEM IN SETTING UP                                                     ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                                       ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:                ***
***  - host "this.is.my.only.host": Connection failure (wrong host or port?). ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE.              ***
*********************************************************************************'''

expected_message_connection_failed_several1 = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking connection (this.is.my.favourite.host)... FAILED.
 .. giving this node a lower priority..
 .. checking connection (mystery-tour.uk)... FAILED.
 .. giving this node a lower priority..
 .. checking connection (tomato.salad-with-spam.fr)... FAILED.
 .. giving this node a lower priority..
Config for PID module (rabbit messaging queue) .. FAILED!'''

expected_message_connection_failed_several2 = '''**************************************************************************************
*** PROBLEM IN SETTING UP                                                          ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                                            ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:                     ***
***  - host "this.is.my.favourite.host": Connection failure (wrong host or port?). ***
***  - host "mystery-tour.uk": Connection failure (wrong host or port?).           ***
***  - host "tomato.salad-with-spam.fr": Connection failure (wrong host or port?). ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE.                   ***
**************************************************************************************'''

expected_message_authentication_failed_only1 = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication (this.is.my.only.host)... FAILED.
 .. giving this node a lower priority..
Config for PID module (rabbit messaging queue) .. FAILED!'''

expected_message_authentication_failed_only2 = '''*************************************************************************************************
*** PROBLEM IN SETTING UP                                                                     ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                                                       ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:                                ***
***  - host "this.is.my.only.host": Authentication failure (user johndoe, password abc123yx). ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE.                              ***
*************************************************************************************************'''

expected_message_authentication_failed_several1 = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication (this.is.my.favourite.host)... FAILED.
 .. giving this node a lower priority..
 .. checking authentication (mystery-tour.uk)... FAILED.
 .. giving this node a lower priority..
 .. checking authentication (tomato.salad-with-spam.fr)... FAILED.
 .. giving this node a lower priority..
Config for PID module (rabbit messaging queue) .. FAILED!'''

expected_message_authentication_failed_several2 = '''******************************************************************************************************
*** PROBLEM IN SETTING UP                                                                          ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                                                            ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:                                     ***
***  - host "this.is.my.favourite.host": Authentication failure (user johndoe, password abc123yx). ***
***  - host "mystery-tour.uk": Authentication failure (user johndoe, password abc123yx).           ***
***  - host "tomato.salad-with-spam.fr": Authentication failure (user johndoe, password abc123yx). ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE.                                   ***
******************************************************************************************************'''


expected_message_channel_closed_only1 = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication and connection (this.is.my.only.host)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... FAILED.
 .. giving this node a lower priority..
Config for PID module (rabbit messaging queue) .. FAILED!'''

expected_message_channel_closed_only2 = '''********************************************************************
*** PROBLEM IN SETTING UP                                        ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                          ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:   ***
***  - host "this.is.my.only.host": Channel failure.             ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE. ***
********************************************************************'''


expected_message_first_channel_closed_then_ok1 = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication and connection (this.is.my.favourite.host)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... FAILED.
 .. giving this node a lower priority..
 .. checking authentication and connection ...
 .. checking authentication and connection (mystery-tour.uk)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... FAILED.
 .. giving this node a lower priority..
 .. checking authentication and connection ...
 .. checking authentication and connection (tomato.salad-with-spam.fr)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... ok.
 .. checking exchange ...
 .. checking exchange ... ok.
Config for PID module (rabbit messaging queue).. ok.
Successful connection to PID messaging queue at "tomato.salad-with-spam.fr".'''

expected_message_first_channel_closed_then_ok2 = None
