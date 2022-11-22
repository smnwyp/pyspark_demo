from jupyter/pyspark-notebook:latest


WORKDIR /home/jovyan/work
COPY pyspark_demo pyspark_demo
COPY .env .env
COPY run.sh run.sh

CMD ["/bin/bash", "-c", "./run.sh"]



