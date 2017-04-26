# Yelp Dataset Analysis with the SMACK stack

## Overview
This application analyses the public yelp dataset, round 9 (https://www.yelp.com/dataset_challenge).
In the first step it extracts the provided tar-file, parses the contained Json files and stores the outcome in a
scalable data store such as Cassandra or HDFS. After that, some basic Joins and Selects are executed to further analyse
the content of the data.
This application is based on the SMACK stack and developed on DC/OS, it should ideally be executed on a [basic
DC/OS AWS instance](https://dcos.io/docs/1.9/installing/cloud/aws/basic/) with the default versions of Cassandra and Spark applications installed.

## Getting started
Start with checking out this git project or simply download the marathon json file, the applicatoin itself is provided
in the docker container 'ktmeindl/yelp:1.0.0'. It can be executed with default settings in a DC/OS-cluster with
at least three Cassandra nodes and Spark installed, you just have to provide a marathon json file as described here.

To execute it with the default settings, simply copy the marathon app template, adapt it and run it via the following command:

```
cp conf/yelp-app.json.template conf/yelp-app.json
# now you have to set the path to the tar-file in the start command
# replace <path-to-tar-file> with the real path
vi conf/yelp-app.json
dcos marathon app add conf/spark-dcos.json
```

I used s3 to store the Yelp tar-file. If you want to do the same, please also add the following
parameters to the driver-java-options in the start command of the marathon app json before you submit it:
```
-Ds3.aws.endpoint=<your s3 endpoint>
-Ds3.aws.access.id=<your s3 access key>
-Ds3.aws.access.key=<your s3 secret access key>
```

## Configuration
There are two ways to create a custom version of this app:

1. Check out the git project and create your own docker container
2. Download the docker container, adapt it and upload your own version


### 1. Build docker image from git cone
There is a script provided to create the default docker build from this git repository - before that of course
a maven build is required:

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
 - yelp-defaults.properties for yelp settings (Be aware that this file will be published, so take care that it contains no sensitive information!)
3. The docker build file docker/Dockerfile

After the configuration is adapted according to your requirements, the image can be pushed to your docker account:

```
docker run -it
docker push <your docker image name in the form username/image:tag>
```

### 2. Adapt docker image from docker repo

You can also use the docker image as a basis for your own dockerfile:

```
FROM ktmeindl/yelp:1.0.0
RUN ...
```