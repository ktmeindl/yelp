

FROM mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.6

# Adding the program specifics to the custom Spark container
ADD . /yelp

# Adapt file endings
RUN sudo apt-get install dos2unix
RUN dos2unix /yelp/bin/*
RUN dos2unix /yelp/conf/*
RUN mkdir /yelp/data

# Copy Spark config
RUN cp /yelp/conf/spark-defaults.conf /opt/spark/dist/conf
RUN cp /yelp/conf/hdfs-site.xml /opt/spark/dist/conf
RUN cp /yelp/conf/core-site.xml /opt/spark/dist/conf


