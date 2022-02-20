FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.2.1}

USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get install -y vim && \
    apt-get install -y wget && \
    apt-get install -y zip && \
    apt-get clean

USER airflow
RUN pip install --upgrade pip

COPY requirements.txt /opt/airflow
WORKDIR /opt/airflow
RUN pip install -r requirements.txt

# for connecting aws in pyspark 
ENV SPARK_HOME=/home/airflow/.local/lib/python3.7/site-packages/pyspark
ENV AWS_PROFILE=default

ADD https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.1/redshift-jdbc42-2.1.0.1.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.2/hadoop-aws-3.1.2.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar $SPARK_HOME/jars

USER root
RUN chmod -R a=rx $SPARK_HOME/jars
