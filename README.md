# Yelp Dataset Analysis with the SMACK stack

## Overview
This application analyses the public yelp dataset, round 9 (https://www.yelp.com/dataset_challenge).
In the first step it extracts the provided tar-file, parses the contained Json files and stores the outcome in a
scalable data store such as Cassandra or HDFS. After that, some basic Joins and Selects are executed to further analyse
the content of the data.
This application is based on the SMACK stack and developed on DC/OS, it should ideally be executed on a [basic
DC/OS AWS instance](https://dcos.io/docs/1.9/installing/cloud/aws/basic/) with the default versions of Cassandra, HDFS and Spark applications installed.

## Getting started
Start with checking out this git project or simply download the job json file. The application itself is provided
in the docker container 'ktmeindl/yelp:1.0.0' and can be executed with default settings in a DC/OS-cluster with
at least three Cassandra nodes, beta-HDFS and Spark installed.

To execute it with the default settings, simply copy the job template, adapt it and submit it via the following command
or directly add a new job in the DC/OS web interface and copy the json content.

```
cp conf/yelp-app.json.template conf/yelp-app.json
# now you have to set the path to the tar-file in the start command
# -> replace <path-to-tar-file> with the real path in the json file
dcos job add conf/yelp-app.json
```

The job can then be started in the DC/OS web interface under "jobs" -> "yelp-app".
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
In case you already have successfully extracted the contents of the tar-file, you can also set the following
driver-java-option to avoid untarring the data again:
```
-Dshould.untar.files=false
```


## Setup
There are two ways to create a custom version of this app:

1. Check out the git project and create your own docker container
2. Download the docker container, adapt it and upload your own version


### 1. Build docker image from git clone
There is a script provided to create the default docker build from this git repository - don't forget to execute the maven build before that:

```
mvn clean package
sh bin/docker.sh <your docker image name in the form username/image:tag>
```

This script copies some libraries and configuration files into a template Spark image and creates a new one.
In order to customize the build, you must change the following files:

1. The script that prepares and executes the docker build bin/docker.sh
2. The configuration files in conf/ in order to customize the build for you:
    - log4j.properties for logger settings
    - spark-defaults.conf for the configuration of the spark job
    - yelp-defaults.properties for yelp settings (Be aware that this file will be published in docker, so take care that it contains no sensitive information!)
3. The docker build file docker/Dockerfile

After the configuration is adapted and created according to your requirements, the image can be pushed to your docker account:

```
docker push <your docker image name in the form username/image:tag>
```

### 2. Adapt docker image from docker repo

You can also use the provided docker image as a basis for your own Dockerfile:

```
FROM ktmeindl/yelp:1.0.0
RUN ...
```

## Configuration
### HDFS
#### Environment

The standard HDFS connection shipped with this image is for a standard DC/OS beta-HDFS setup. If you want to configure it
for your own cluster, you can extract the files via the DC/OS-CLI and overwrite the *-site.xml files in this image via:

```
 dcos beta-hdfs --name=hdfs endpoints core-site.xml
 dcos beta-hdfs --name=hdfs endpoints hdfs-site.xml
```

Alternately, you could also download the files at start of the marathon job by adding the following commands to the "cmd":

```
rm /opt/spark/dist/conf/core-site.xml
rm /opt/spark/dist/conf/hdfs-site.xml
curl http://hdfs.marathon.mesos:<your hdfs mesos port>/v1/endpoints/core-site.xml > /opt/spark/dist/conf/core-site.xml
curl http://hdfs.marathon.mesos:<your hdfs mesos port>/v1/endpoints/hdfs-site.xml > /opt/spark/dist/conf/hdfs-site.xml
```

In order to check whether the HDFS connection worked as expected, you can use the following steps to execute HDFS commands:

```
dcos node ssh --master-proxy --leader
docker run -it cloudera/quickstart bash
hdfs dfs -ls hdfs://name-1-node.hdfs.mesos:9001/yelp/data
```

#### Setting the HDFS directory for processing
The program stages the untarred files in HDFS, the default directory is /yelp/data. In order to change that, adapt the properties file:

```
vi conf/yelp-defaults.properties
```

Here you have to change the following property:
```
untarred.files.dir=<HDFS directory where untarred json files should be stored>
```


### Cassandra


#### Access CQL-SH

To access the Cassandra CQL-SH, execute the following (you might have to adapt the cassandra version):
```
dcos node ssh --master-proxy --mesos-id leader
docker run -ti cassandra:3.0.10 cqlsh <cassandra host>
```