#!/usr/bin/env bash

## Expecting env var: YELP_MASTER=< Master of the form: mesos://<host>:5050 >
## Input parameter: <path to tar-file containing yelp data>

/opt/spark/dist/bin/spark-submit --master ${YELP_MASTER} --class de.ktmeindl.yelp.Main /yelp/lib/yelp-1.0.0.jar $1