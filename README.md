# Real Time Data Processing
Real-time data streaming enables you to analyze and process data in real time instead of waiting hours, days, or weeks to get answers. 
In this project, I used wikimedia streaming api as streaming data source.

## Architecture design for real time streaming


## Technology Used
* [x] **Apache Spark Structured Streaming**
* [x] **Apache Kafka**
* [x] **MySQL**
* [x] **Python**
* [X] **Docker**

## Prerequisites
1. Docker and Docker compose installed
2. Mysql installed


## Setup Instruction
1. Clone the repository and run docker-compose.yml file.

```bash
    docker compose up
```
2. Change the mysql_destination configs present in `config/config.json` file

2. Create a topic in Kafka using following command: 
```bash
    kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wikimedia_data_topic
```
3. Run Kafka producer as: 
``` bash
    python src/KafkaProducer/main.py
```
4. Run Spark Structured Streaming as: 
``` bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 src/StructuredStreaming/main.py
```
5. If mysql-connector-java -8.0.11.jar file is inside virtualenv then run application as:
``` bash
    spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 main.py
``` 
otherwise 
``` bash
    spark-submit --jars {full_path}/jars/mysql-connector-java-8.0.11.jar  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 main.py` 
```
replace full_path with your path as : /home/name/... 


6. Checking if producer is producing data: 
``` bash
    sudo docker exec -it 281b46827836 /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic wikimedia_data_topic
```
(replace 281b46827836 with your container_id)


