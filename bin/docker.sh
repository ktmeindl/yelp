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


    cp $(dirname $0)/../conf/yelp-defaults.properties $(dirname $0)/../docker/conf/yelp.properties
    cp $(dirname $0)/../conf/log4j.properties $(dirname $0)/../docker/conf
    cp $(dirname $0)/../conf/spark-defaults.conf $(dirname $0)/../docker/conf

    cp $(dirname $0)/../target/yelp-1.0.0.jar $(dirname $0)/../docker/lib

    cp $(dirname $0)/submit.sh $(dirname $0)/../docker/bin
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
