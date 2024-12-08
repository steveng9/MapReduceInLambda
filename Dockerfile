#FROM ubuntu:latest 
FROM apache/spark-py AS build-1
USER root
WORKDIR /tmp
COPY pyspark_example.py pyspark_example.py
#COPY a.txt a.txt
COPY Moby-Dick.txt Moby-Dick.txt
#RUN apt-get install 
RUN apt-get update
RUN apt-get install nano
RUN pip3 install pyspark==3.4.0

FROM build-1 AS build-2

CMD ["python3", "pyspark_example.py"]
#ENTRYPOINT ["/bin/bash"]
