
# Hadoop Cluster Proof Of Concept

This project will provide insight on the tools / knowledge required to create and maintain a Hadoop Cluster.

### What is this repository for? ###

* This repository will contain all files necessary to re-create this project.
* Start Date: 2/7/2017

### Installation ###

1. ##### Creating the NameNode
    ###### Setup machine alias in host file:
    `$ nano /etc/hosts`
    Replace any / all of the lines with the following entry.
    
    `10.0.0.1 NameNode `

    ###### Setup SSH Server
    Generate an SSH key for the root user
    
    `$ ssh-keygen -t rsa -P ""`
    
    If prompted for SSH key file name,         Enter file in which to save the key        (/root/.ssh/id_rsa) and press ENTER.       Next put the key to authorized_keys        directory for future password-less         access.
     
    `$ cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys`
    
    `$ chmod 700 ~/.ssh`
    
    `$ chmod 600 ~/.ssh/authorized_keys`
    
    ###### Download and Install Hadoop distribution
    `root@NameNode:~# cd /usr/local/`
    
    `root@NameNode:/usr/local/# wget http://www-us.apache.org/dist/hadoop/common/stable2/hadoop-2.7.3-src.tar.gz`
    
    `root@NameNode:/usr/local/# tar -xzvf hadoop-2.7.2.tar.gz`
    
    ###### Setup Environment Variables
    Open `.bashrc`
    
    `nano ~/.bashrc`
    
    Append the following lines:
    
    `export JAVA_HOME=/usr`
    
    `export HADOOP_HOME=/data01/hadoop`
    
    `export HADOOP_MAPRED_HOME=$HADOOP_HOME`
    
    `export HADOOP_COMMON_HOME=$HADOOP_HOME`
    
    `export HADOOP_HDFS_HOME=$HADOOP_HOME`
    
    `export YARN_HOME=$HADOOP_HOME`
    
    `export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop`
    
    `export YARN_CONF_DIR=$HADOOP_HOME/etf/hadoop`
    
    You should now be able to test hadoop from the command line:
    `$ hadoop version`
    
2. ##### Configuring the NameNode

    Hadoop uses several XML files located in `/data01/hadoop/etc/hadoop` in order to read all the runtime parameters.
    ###### Configure `core-site.xml`
    Open this file and insert the following lines:
    

        <configuration>
          <property>
            <name>fs.default.name</name>
            <value>hdfs://namenode-hostname:9000</value>
          </property>
          <property>
            <name>hadoop.tmp.dir</name>
            <value>/data01/hadoop/tmp</value>
          </property>
        </configuration>

    
    ###### Configure `hdfs-site.xml`
    Open this file and insert the following lines:

        <configuration>
           <property>
             <name>dfs.replication</name>
             <value>2</value>
           </property>
           <property>
             <name>dfs.permissions</name>
             <value>false</value>
           </property>
        </configuration>

    
    ###### Configure `mapred-site.xml`
    Open this file and insert the following lines:

        <configuration>
         <property>
           <name>mapreduce.framework.name</name>
           <value>yarn</value>
         </property>
        </configuration>
    
    ###### Configure `yarn-site.xml`
    Open this file and insert the following lines:
    
        <configuration>
          <property>
            <name>yarn.nodemanager.aux-services</name>
            <value>mapreduce_shuffle</value>
          </property>
          <property>
            <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
            <value>org.apache.hadoop.mapred.ShuffleHandler</value>
          </property>
          <property>
            <name>yarn.resourcemanager.resource-tracker.address</name>
            <value>namenode-hostname:8025</value>
          </property>
          <property>
            <name>yarn.resourcemanager.scheduler.address</name>
            <value>namenode-hostname:8030</value>
          </property>
          <property>
            <name>yarn.resourcemanager.address</name>
            <value>namenode-hostname:8040</value>
          </property>
        </configuration>
    
    
3. ##### Setup the Master

    Now you need to tell Hadoop NameNode the hostname of Secondary name node. In our case both NameNode & Secondary NameNode resides on the same machine. We do it by editing the masters file:
    
    In `/data01/hadoop/etc/hadoop` create a file called masters.
    Start a new line and simply enter the hostname of the NameNode.
    ex: `namenode-hostname`
    
    Next, we need to format the NameNode.
    
    `$ cd /data01/hadoop/bin/hadoop`
    
    `namenode -format`
    
    ##### Adding the Data Nodes
    In `/data01/hadoop/etc/hadoop` creare a file called slaves.
    On a new line for each, enter the hostnames of the datanodes.
    ex: 
    
    `hostname-datanode-1`
    
    `hostname-datanode-2`
    
    We also need to add these nodes to the hosts file:
    
    `$ nano /etc/hosts`
    
    Append the IP, hostname, and hostname+domain for each node on its own line.
    
    `<IP address> hostname-datanode-1 hostname-datanode-1.domain.com`
    
    `<IP address> hostname-datanode-2 hostname-datanode-2.domain.com`
    
    ##### Configuring the DataNodes
    First we need to update the `/etc/hosts` file on the data nodes.
    Add the same lines we added above with the addition of the namenode.
    
    ex:
    
    `<IP address> hostname-namenode- hostname-namenode.domain.com`
    
    `<IP address> hostname-datanode- hostname-datanode-1.domain.com`
    
    `<IP address> hostname-datanode- hostname-datanode-2.domain.com`
    
4. ##### Startup
    We are now ready to start up the cluster, you can do this by navigating to 
    `/data01/hadoop/sbin`
    and executing the following command:
    `$ ./start-all.sh`

### Usage ###

Navigating / using HDFS:

* Many of the unix commands you are already familiar with are available in HDFS
* Navigation: `$ hdfs dfs -ls /directory/im/looking/for` 
    - (Note: `cd` is not available so you will have to use this command with the full path when navigating directories.)
* Creating a directory: `$hdfs dfs -mkdir /directory/im/creating`

### Hadoop/HDFS web portals (open from MobaXterm)

#### NameNode 
- `firefox http://localhost:50070`
- This web portal is best used to check the staus of the hadoop cluster, it keeps tabs on:
  * HDFS capacity used / available
  * Live nodes / Decomissioned nodes / Offline nodes
  * Allows you to browse HDFS
  * Log viewing

![alt text](http://i.imgur.com/BesQaeq.pngq "Hadoop NameNode Portal")

#### Job Tracker
- `firefox http://localhost:8088`
-  This web portal allows for the monitoring of jobs being run on the cluster.
-  It provides information on:
   * Runnning Jobs
   * Completed Jobs
   * Failed Jobs
   * Queued Jobs

![alt text](http://i.imgur.com/rttqwQ0.png "Hadoop Job Tracker Portal")

#### DataNode 
- `firefox http://localhost:50075`
- These web portals are basically only good for checking if the DataNode is alive.

![alt text](http://i.imgur.com/DGmkXrZ.png "Hadoop DataNode Portal")
=======
Hadoop/HDFS web portals (open from MobaXterm)

* NameNode 
    `firefox http://localhost:50070`
* Job Tracker
    `firefox http://localhost:8088`
* DataNode 
    `firefox http://localhost:50075`

### Who do I talk to? ###

* Connor Callison - ccallison@humboldt.edu
* Other EDM team member