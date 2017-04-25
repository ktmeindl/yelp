

FROM mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.6

# Adding the program specifics to the custom Spark container
ADD . /yelp

RUN cp /yelp/conf/spark-defaults.conf /opt/spark/dist/conf
