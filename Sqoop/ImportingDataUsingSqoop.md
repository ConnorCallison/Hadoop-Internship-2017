# Using Sqoop to import data into HDFS

[![N|Solid](http://sqoop.apache.org/images/sqoop-logo.png)](http://sqoop.apache.org/)

Process for importing data into HDFS using Sqoop:

  1. SSH to odi-dev.humboldt.edu
  
  2. Sudo into the hadoop user:
     ```
     $ sudo su - hadoop
     ```
  3. Set up environment variables
     ```
     $ source hsu_setenv.sh
     ```
  4. Now it's time to import the data into HDFS.
- Required items for the command:
    - Host Address
    - Port number
    - SID 
    - Table Name
    - Username
    - Password 
    - Output Directory
    ```
     sqoop import -connect jdbc:oracle:thin:@<HOST>:<PORT>:<SID> \
     --username <USERNAME> -P \
     -table <TABLE NAME> \
     -target-dir /<OUTPUT DIRECTORY> \
     --hive-import --hive-table <HIVE TABLE NAME> \
     -m 1 --validate
     ```
  5. Sqoop will then connect to the database and begin receiving data. If all goes well, sqoop will notify success.
  
# Using Sqoop to export data back to Oracle for Data Validation

    ```
    sqoop export -connect jdbc:oracle:thin:@<HOST>:<PORT>:<SID> --username <USERNAME> -P -table <TABLE NAME> --export-dir<HIVE TABLE DIRECTORY> --fields-terminated-by '\001' --direct
    ```
