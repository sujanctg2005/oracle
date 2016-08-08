# Oracle Replication data compare
Pl/SQL script 'data-compare.sql' will check data consistency beteen two active database.

## Getting Started

Oracle golden gate will replicate data between two data center. If there is any issue with last update time stamp of the transaction, then replication will be done properly. Those inconsistences will make problem during failover to standby site. This script will find inconsistent records and make report out of it.


