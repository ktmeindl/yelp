#!/usr/bin/env bash

# Builds and pushes a docker image.
# input parameter: docker image name (username/image:tag)

set -x -e -o pipefail

DOCKER_IMAGE=$1
if [[ -z "$DOCKER_IMAGE" ]]; then
    echo "Wrong usage - please specify docker image name (username/image:tag) as input parameter"
    exit -1
fi

function copy_elements {
    rm -rf $(dirname $0)/../docker/conf
    rm -rf $(dirname $0)/../docker/lib
    rm -rf $(dirname $0)/../docker/bin
    mkdir $(dirname $0)/../docker/conf
    mkdir $(dirname $0)/../docker/lib
    mkdir $(dirname $0)/../docker/bin


    # Configuration
    cp $(dirname $0)/../conf/yelp-defaults.properties $(dirname $0)/../docker/conf/yelp.properties
    cp $(dirname $0)/../conf/log4j.properties $(dirname $0)/../docker/conf
    cp $(dirname $0)/../conf/spark-defaults.conf $(dirname $0)/../docker/conf

    # Scripts
    cp $(dirname $0)/submit.sh $(dirname $0)/../docker/bin

    # Libraries
    cp $(dirname $0)/../target/yelp-1.0.0.jar $(dirname $0)/../docker/lib
    ## Providing some extra libraries required for s3
    cp ~/.m2/repository/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar $(dirname $0)/../docker/lib
    cp ~/.m2/repository/org/apache/hadoop/hadoop-aws/2.6.0/hadoop-aws-2.6.0.jar $(dirname $0)/../docker/lib
    cp ~/.m2/repository/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar $(dirname $0)/../docker/lib
    cp ~/.m2/repository/com/datastax/cassandra/cassandra-driver-core/3.0.0/cassandra-driver-core-3.0.0.jar $(dirname $0)/../docker/lib
    cp ~/.m2/repository/com/datastax/spark/spark-cassandra-connector_2.11/2.0.1/spark-cassandra-connector_2.11-2.0.1.jar $(dirname $0)/../docker/lib
}

function build_docker {
    # build docker
    (cd $(dirname $0) && cd ../docker && docker build -t "${DOCKER_IMAGE}" .)
}

function push_docker {
    # push docker
    docker push "${DOCKER_IMAGE}"
}

copy_elements
build_docker
#push_docker
