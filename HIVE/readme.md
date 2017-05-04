# Hive

[![N|Solid](https://hive.apache.org/images/hive_logo_medium.jpg)](https://hive.apache.org/)

Apache HIVE is a data warehousing tool built for hadoop. Hive allows users to use SQL like queries on data stored in HDFS. HIVE works by imposing schema on read, and converting HiveQL (HQL) into MapReduce, Tez, or Spark jobs.

In this Project:
  - We used HIVE to create the position dimension and the transforms required to do so.

### Usage
To start a hive command line interface:
```
$ hive
Logging initialized using configuration in jar:file:/data01/hadoop/apache-hive-1.2.1-bin/lib/hive-common-1.2.1.jar!/hive-log4j.properties
hive>
```

To run a hive command from bash:
```
$ hive -e "select * from empl"
$ hive -f "hive_script.hql"
```

### Creating the position dimension

The script `create_dim_position_and_dependencies.sh` will grab all of the PS tables from the oracle database and then run the hql scripts required to create the tables in the dimension.

```
[hadoop@hadoop-ns-dev ~]$ cd hive_poc_tools/
[hadoop@hadoop-ns-dev hive_poc_tools]$ ./create_dim_position_and_dependencies.sh
```

Depending on the execution engine set in hive, the script will take around 25 minutes to complete.
More to come on the various execution engines and percent speedup.

### Validation

The script `validationHiveTablesLinuxMode.sh` will take two tables in hive and compare them, row by row and return row count along with the amount of rows that dont compare perfectly.
This script was created by Sourygna Luangsay and full documentation can be found here: (https://community.hortonworks.com/articles/1283/hive-script-to-validate-tables-compare-one-with-an.html)

In order to run the validation script there needts to be a set of tables in hive that are exact cpopies of the tables in the oracle database. This can be acheived by creating another database in Hive, and scooping all of the tables over.

```
./validationHiveTablesLinuxMode.sh <table name>
[hadoop@odi-dev ~]$ ./validationHiveTablesLinuxMode.sh stg_hcm_dim_position
```