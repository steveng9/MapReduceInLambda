#FROM ubuntu:latest 
FROM apache/spark-py AS build-1
USER root
WORKDIR /tmp
RUN apt-get update
RUN apt-get install nano
RUN pip3 install pyspark==3.4.0
RUN pip install boto3
#RUN apt-get install -y openjdk-8-jdk


#RUN --mount=type=secret,id=aws-access-key,env=ACCESS_KEY \
#    --mount=type=secret,id=aws-secret-key,env=SECRET_KEY

ARG ACCESS_KEY
ARG SECRET_KEY

COPY hadoop-aws-3.2.0.jar /opt/spark/jars/
#COPY aws-java-sdk-1.11.900.jar /opt/spark/jars/
COPY aws-java-sdk-bundle-1.11.375.jar /opt/spark/jars/
COPY pyspark_example.py pyspark_example.py
COPY Moby-Dick.txt Moby-Dick.txt

ENV ACCESS_KEY=${ACCESS_KEY}
ENV SECRET_KEY=${SECRET_KEY}
ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=$SPARK_HOME/conf

FROM build-1 AS build-2
CMD ["python3", "pyspark_example.py"]
#ENTRYPOINT ["/bin/bash"]
