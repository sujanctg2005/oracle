# Oracle Replication data compare
Pl/SQL script 'data-compare.sql' will check data consistency between two active database.

## Getting Started

Oracle golden gate will replicate data between two data center. If there is any issue with last update time stamp of the transaction, then replication will be done properly. Those inconsistences will make problem during failover to standby site. This script will find inconsistent records and make report out of it.

### Prerequisities

Let say we have two database running in two different data center A and B and we are going to compare table ‘USERS’ data. Frist we have to create new oracle user scheme in either of the database (A or B). Then export USERS table from A and B database using oracle data pump and import tables using oracle data pump into new schema with new table name A_USERS (from A database) AND B_USERS (from B database) because we can import two tables with same name.

```
Create a report table : REPORT_COMPARE_DATA
REPORT_COMPARE_DATA(
TABLE_NAME VARCHAR(32),
PRIMARY_KEY VARCHAR(32),,
STATUS VARCHAR(400))
```

Change table names and primary key of the table in the script in following place.

```
  TAB1_NAME VARCHAR2(30) :='A_USERS'; 
  TAB2_NAME VARCHAR2(30) :='B_USERS'; 
  PRIMARY_KEY VARCHAR2(30) :='USERID';
  
  .....
  .....
  
  
  P_REC  A_USERS%ROWTYPE;
  T_REC  B_USERS%ROWTYPE;
```

Script will compare each record between A_USERS and B_USERS table by the primary key.

Also you can exclude columns if you don't need to compare them.
```
 EXCLUDE_COLS VARCHAR2(400) :='''ADDRESS'', ''EMAIL''';
  --EXCLUDE_COLS VARCHAR2(400) :=NULL;  /* make it null if you want to compare all columns */
```




### Running script

```
@data-compare.sql
```
### Result

You find result of data comparison in report table REPORT_COMPARE_DATA
```
SELECT * FROM REPORT_COMPARE_DATA;
```


    



