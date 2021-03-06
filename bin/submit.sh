#!/usr/bin/env bash
## Input parameter: <path to tar-file containing yelp data>
## Attention: this script is work in progress and not tested

TAR_FILE=$1
if [ -z "${TAR_FILE}" ]; then
    echo "Program argument not set. Run the script with argument: <path to tar-file of yelp dataset>"
    exit 1
fi

YELP_DRIVER_JAVA_OPTIONS="-Dlog4j.configuration=file:/yelp/conf/log4j.properties -Dyelp.properties=file:/yelp/conf/yelp.properties"

/opt/spark/dist/bin/spark-submit \
    --master mesos://leader.mesos:5050 \
    --class de.ktmeindl.yelp.Main \
    --driver-java-options "${YELP_DRIVER_JAVA_OPTIONS}" \
    /yelp/lib/yelp-1.0.0.jar ${TAR_FILE}


