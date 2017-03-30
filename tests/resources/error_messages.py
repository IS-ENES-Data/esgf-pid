# Expected error messages for the unit tests
# of the check.py module.

expected_message_ok = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication and connection (this.is.my.only.host)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... ok.
 .. checking exchange ...
 .. checking exchange ... ok.
Config for PID module (rabbit messaging queue).. ok.
Successful connection to PID messaging queue at "this.is.my.only.host".'''


expected_message_first_connection_failed_then_ok = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking connection (this.is.my.favourite.host)... FAILED.
 .. checking connection (mystery-tour.uk)... FAILED.
 .. checking authentication and connection (tomato.salad-with-spam.fr)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... ok.
 .. checking exchange ...
 .. checking exchange ... ok.
Config for PID module (rabbit messaging queue).. ok.
Successful connection to PID messaging queue at "tomato.salad-with-spam.fr".'''


expected_message_connection_failed_unknown = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking connection (this.is.my.only.host)... FAILED.
Config for PID module (rabbit messaging queue) .. FAILED!
********************************************************************
*** PROBLEM IN SETTING UP                                        ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                          ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:   ***
***  - host "this.is.my.only.host": Unknown connection failure.  ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE. ***
********************************************************************'''


expected_message_connection_failed_only = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking connection (this.is.my.only.host)... FAILED.
Config for PID module (rabbit messaging queue) .. FAILED!
********************************************************************
*** PROBLEM IN SETTING UP                                        ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                          ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:   ***
***  - host "this.is.my.only.host": Connection failure.          ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE. ***
********************************************************************'''

expected_message_connection_failed_several = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking connection (this.is.my.favourite.host)... FAILED.
 .. checking connection (mystery-tour.uk)... FAILED.
 .. checking connection (tomato.salad-with-spam.fr)... FAILED.
Config for PID module (rabbit messaging queue) .. FAILED!
********************************************************************
*** PROBLEM IN SETTING UP                                        ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                          ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:   ***
***  - host "this.is.my.favourite.host": Connection failure.     ***
***  - host "mystery-tour.uk": Connection failure.               ***
***  - host "tomato.salad-with-spam.fr": Connection failure.     ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE. ***
********************************************************************'''

expected_message_authentication_failed_only = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication (this.is.my.only.host)... FAILED.
Config for PID module (rabbit messaging queue) .. FAILED!
*************************************************************************************************
*** PROBLEM IN SETTING UP                                                                     ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                                                       ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:                                ***
***  - host "this.is.my.only.host": Authentication failure (user johndoe, password abc123yx). ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE.                              ***
*************************************************************************************************'''

expected_message_authentication_failed_several = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication (this.is.my.favourite.host)... FAILED.
 .. checking authentication (mystery-tour.uk)... FAILED.
 .. checking authentication (tomato.salad-with-spam.fr)... FAILED.
Config for PID module (rabbit messaging queue) .. FAILED!
******************************************************************************************************
*** PROBLEM IN SETTING UP                                                                          ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                                                            ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:                                     ***
***  - host "this.is.my.favourite.host": Authentication failure (user johndoe, password abc123yx). ***
***  - host "mystery-tour.uk": Authentication failure (user johndoe, password abc123yx).           ***
***  - host "tomato.salad-with-spam.fr": Authentication failure (user johndoe, password abc123yx). ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE.                                   ***
******************************************************************************************************'''


expected_message_channel_closed_only = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication and connection (this.is.my.only.host)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... FAILED.
Config for PID module (rabbit messaging queue) .. FAILED!
********************************************************************
*** PROBLEM IN SETTING UP                                        ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                          ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:   ***
***  - host "this.is.my.only.host": Channel failure.             ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE. ***
********************************************************************'''


expected_message_first_channel_closed_then_ok = '''Checking config for PID module (rabbit messaging queue) ...
 .. checking authentication and connection ...
 .. checking authentication and connection (this.is.my.favourite.host)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... FAILED.
 .. checking authentication and connection ...
 .. checking authentication and connection (mystery-tour.uk)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... FAILED.
 .. checking authentication and connection ...
 .. checking authentication and connection (tomato.salad-with-spam.fr)... ok.
 .. checking authentication and connection ... ok.
 .. checking channel ...
 .. checking channel ... ok.
 .. checking exchange ...
 .. checking exchange ... ok.
Config for PID module (rabbit messaging queue).. ok.
Successful connection to PID messaging queue at "tomato.salad-with-spam.fr".'''