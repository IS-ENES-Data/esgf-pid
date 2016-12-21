==========================================
Interfaces between the various components
==========================================

(TODO: Extend this documentation!)

Introduction
=============

The CMIP6 PID system consists of various components
that communicate between each other. Thus, if one of
the interfaces between components is changed on one
side, the other side needs to be adapted accordingly,
and care has to be taken that all components are
deployed in matching versions.

Interfaces
==========

(1) Library API: Publisher/CoG <--> esgfpid library

Outgoing: Publisher/CoG (speaks to esgfpid)
Incoming: library (is spoken to)

(2) Message syntax/vocabulary: esgfpid library <--> consumer

Outgoing: library (speaks to consumer)
Incoming: consumer (is spoken to)

(3) Record profile: consumer <--> any downstream service, e.g. landing page

Outgoing: consumer (writes records)
Incoming: Downstream (reads records)


Thus, there is three pairs of communication in total, library and
consumer both have two interfaces (incoming and outgoing).

Any change in these interfaces should lead to a version change according
to Semantic Versioning principles.

(1) Library API changes
-----------------------

Starting this documentation on 2016-12-13.  Any earlier API changes
are not recorded systematically.

==============================  ===============================  ==============================================  =======================
Change description              Introduced in...                 Reflected in...                                 Backwards incompatible?
------------------------------  -------------------------------  -------------------   -------------  ---------   -----------------------
(library func that req change)  (library version)                (publisher version)   (CoG version)  (errata)   Reflection mandatory?
==============================  ===============================  ==============================================  =======================
New way of passing credentials  0.5 (2016-12-14)                 (not yet)             (not yet)      (not yet)  Yes.
==============================  ===============================  ==============================================  =======================

(2) Message syntax changes
---------------------------

Message syntax changes are driven by the consumer who gets new
functionality and thus may need different infos in the messages.

Starting this documentation on 2016-12-13. Any earlier API changes
are not recorded systematically.

=== ================================================  ===============================  ============================  =======================
No. Change description                                Introduced in...                 Reflected in...               Backwards incompatible?
--- ------------------------------------------------  -------------------------------  ----------------------------  -----------------------
no. (Consumer functionality that requires change)     (consumer version)               (library version)             Reflection mandatory?
=== ================================================  ===============================  ============================  =======================
1   Update content of data carts                      Commit 88816893 (2016-11-07)     Commit 43621ac (2016-11-07)   Yes.
2   Complete incomplete handle records.               (TODO)                           Commit 8148cf9 (2016-11-08)   No.
3   Support host name that differs from file url.     Commit 19dacb90 (2016-11-15)     Commit 517edbd (2016-11-15)   Yes.
    blabasdkasdkj
=== ================================================  ===============================  ============================  =======================

Notes:

* (1): Consumer now recognizes if the same data cart now has some handles that were
  just drs ids before, and updates the record. For this, the library needs to send
  pairs of drs id and handle, as a dictionary.
* (2): Unpublication messages contain more info (drs, version), if available.
* (3): The consumer treats file host and file URL totally separately. For this, the
  library needs to send them separately. (TODO: Does library )

(3) Record profile chances
--------------------------

Starting this documentation on 2016-12-13. Any earlier API changes
are not recorded systematically.

================================================  ======================================  =====================================  =======================
Change description                                Introduced in...                        Reflected in...                        Reflection mandatory?
================================================  ======================================  =====================================  =======================
Different design for data cart landing pages.     Landing Page: November (???)            Consumer: Commit 2a15e7b (2016-11-07)  Not too much.
Provide error messages.                           Consumer: Commit cdc00221 (2016-11-24)  Landing Page: Not yet                  No.
Provide removed hosts.                            Consumer: Commit (TODO)   (2016-12-13)  Landing Page: Not yet                  No.
================================================  ======================================  =====================================  =======================
