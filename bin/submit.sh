#!/usr/bin/env bash

## Expecting env var: YELP_MASTER=< Master of the form: mesos://<host>:5050 >
## Input parameter: <path to tar-file containing yelp data>

if [[ -z "${YELP_MASTER}" ]]; then
    echo "Mandatory environment variable not set - please specify Spark Master via: YELP_MASTER=mesos://<host>:5050"
    exit -1
fi

TAR_FILE=$1
if [[ -z "${TAR_FILE}" ]]; then
    echo "Program argument not set. Run the script with argument: <path to tar-file of yelp dataset>"
    exit -1
fi

/opt/spark/dist/bin/spark-submit \
    --master ${YELP_MASTER} \
    --class de.ktmeindl.yelp.Main \
    /yelp/lib/yelp-1.0.0.jar ${TAR_FILE}