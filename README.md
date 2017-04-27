# Yelp Dataset Analysis with the SMACK stack

## Overview
This application analyses the public yelp dataset, round 9 (https://www.yelp.com/dataset_challenge).

In the first step it extracts the provided tar-file, parses the contained json files and stores the outcome in Cassandra tables.
After that, some basic joins and selects are executed to further analyse the content of the data.

This application is based on the SMACK stack and developed on DC/OS, it should ideally be executed on a [basic
DC/OS AWS instance](https://dcos.io/docs/1.9/installing/cloud/aws/basic/) with the default deployments of Cassandra, beta-HDFS and Spark applications installed.

## Getting started
The Yelp application is provided in the docker container 'ktmeindl/yelp:1.0.0' and can be executed with the job json file conf/yelp-app.json.templatewith default settings
For this a DC/OS-cluster with at least three Cassandra nodes and the default setups of beta-HDFS and Spark is required.

To execute it with the default settings, simply copy the job template, adapt it and submit it via the following command:


```
cp conf/yelp-app.json.template conf/yelp-app.json
# now you have to set the path to the tar-file in the start command
# -> replace <path-to-tar-file> with the real path in the json file
dcos job add conf/yelp-app.json
```

Alternately, you can also directly add a new job in the DC/OS web interface via copying the json content of the job config file.

The job can then be started in the DC/OS web interface under "jobs" -> "yelp-app" -> "Run now".
The output of the program can then be seen in the logs section.

### Storing the tar file in S3

I used s3 to store the Yelp tar-file. If you want to do the same, you have to add the following
parameters to the driver-java-options in the start command of the job json before you submit it:
```
-Ds3.aws.endpoint=<your s3 endpoint>
-Ds3.aws.access.id=<your s3 access key>
-Ds3.aws.access.key=<your s3 secret access key>
```

### What if the tar-file is already extracted
In case you already have successfully extracted the contents of the tar-file and just want to re-run the analysis
part of the application, you can also set the following driver-java-option to avoid untarring the data again:
```
-Dshould.untar.files=false
```


## Setup
There are two ways to create a custom version of this app:

1. Check out the git project and create your own docker container based on this git repository
2. Create a new docker container based on ktmeindl/yelp:1.0.0


### 1. Build docker image from git clone
This git repository contains a script to create the default docker build from this directory.
Before that a Maven build must be executed. The build of the docker image can be done with the following commands:

```
mvn clean package
sh bin/docker.sh <your docker image name in the form username/image:tag>
```

This script creates a new docker image based on a template Spark image and copies some libraries and configuration files into it.

In order to customize the build, you must change the following files:

1. The script that prepares and executes the docker build bin/docker.sh
2. The configuration files in conf/ in order to customize the build for you:
    - log4j.properties for logger settings
    - spark-defaults.conf for the configuration of the spark job
    - yelp-defaults.properties for yelp settings
3. The docker build file docker/Dockerfile

After the configuration is adapted and the image was created according to your requirements, it can be pushed to your docker account:

```
docker push <your docker image name in the form username/image:tag>
```

### 2. Adapt docker image from docker repository

You can also use the provided docker image as a basis for your own Dockerfile:

```
FROM ktmeindl/yelp:1.0.0
RUN ...
```

## Configuration
### HDFS
#### Environment

The HDFS configuration details (conf/core-site.xml, conf/hdfs-site.xml) shipped with this image
are working for a standard DC/OS beta-HDFS setup (currently tested with version 1.3.0-2.6.0-cdh5.9.1-beta).
If you want to configure it for your own cluster, you can extract the *-site.xml files via the DC/OS-CLI and overwrite
them in this image via:

```
 dcos beta-hdfs --name=hdfs endpoints core-site.xml
 dcos beta-hdfs --name=hdfs endpoints hdfs-site.xml
```

Alternately, you could also download the files at start of the marathon job by adding the following commands at
the beginning of the "cmd" entry in the job json file:

```
rm /opt/spark/dist/conf/core-site.xml
rm /opt/spark/dist/conf/hdfs-site.xml
curl http://hdfs.marathon.mesos:<your hdfs mesos port>/v1/endpoints/core-site.xml > /opt/spark/dist/conf/core-site.xml
curl http://hdfs.marathon.mesos:<your hdfs mesos port>/v1/endpoints/hdfs-site.xml > /opt/spark/dist/conf/hdfs-site.xml
```

In order to check whether the untarring of the files to HDFS worked as expected,
you can use the following steps to execute HDFS commands against beta-HDFS:

```
dcos node ssh --master-proxy --leader
docker run -it cloudera/quickstart bash
hdfs dfs -ls hdfs://name-1-node.hdfs.mesos:9001/yelp/data
```

#### Setting the HDFS directory for processing
The program stages the untarred files in HDFS in the directory hdfs://yelp/data. In order to change that,
adapt the configuration file conf/yelp-defaults.properties and change the following property:

```
untarred.files.dir=<HDFS directory where untarred json files should be stored>
```

### Cassandra
#### Environment
The basic setup of Cassandra in DC/OS contains three nodes with a fixed default name and port. These are configured in the
default application properties file.
However, the Cassandra connection details can be changed in conf/yelp-defaults.properties via setting:

```
cassandra.hosts=<comma-separated list of cassandra nodes of the format host:port>
```

#### Setting the Cassandra keyspace and tables for processing
The program stores the parsed data in Cassandra tables within the default keyspace "yelp_dataset".
The keyspace and tablenames can also be changed in the configuration file conf/yelp-defaults.properties via:

```
cassandra.keyspace=<name of cassandra keyspace>
cassandra.table.checkin=<name of checkin table>
cassandra.table.review=<name of review table>
cassandra.table.business=<name of business table>
cassandra.table.tip=<name of tip table>
cassandra.table.user=<name of user table>
```

#### Access CQL-SH

To access the Cassandra CQL-SH, execute the following commands:
```
dcos node ssh --master-proxy --mesos-id leader
docker run -ti cassandra:3.0.10 cqlsh <cassandra host>
```