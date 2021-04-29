# CMT - Prerequisites Guide

Carefully read the prerequisites and make sure you meet the requirements before you commence the migration. Some of the prerequisites may require code adaptations or database cleanup tasks to prepare for the migration, therefore make sure you reserve enough time so that you can adhere to your project plan.

## Prerequisites

Before you begin, ensure you have met the following requirements:

* Your code base is compatible with the SAP Commerce version required by SAP Commerce Cloud (at minimum).
* The code base is exactly the same in both target and source systems. It includes:
  * platform version
  * custom extensions
  * set of configured extensions
  * type system definition as specified in \*-items.xml
* The attribute data types which are database-specific must be compatible with the target database
* Orphaned-types cleanup has been performed in the source system. Data referencing deleted types has been removed.
* The target system is in a state where it can be initialized and the data imported
* The source system is updated with the same \*-items.xml as deployed on the target system (ie. update system has been performed)
* The connectivity to the source database from SAP Commerce Cloud happens via a secured channel, such as the self-serviced VPN that can be created in SAP Commerce Cloud Portal. It is obligatory, and the customer's responsibility, to secure the data transmission
* Old type systems have been deleted in the source system
* A check for duplicates has been performed and existing duplicates in the source database have been removed
* The task engine has been disabled in all target nodes (cronjob.timertask.loadonstartup=false)
