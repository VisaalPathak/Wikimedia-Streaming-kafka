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


## Setup Instruction
1. Clone the repository and run docker-compose.yml file.(`docker compose up`)
2. Create a topic in Kafka using the following command: `kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wikimedia_data_topic`
3. Run Kafka producer file as : `python src/KafkaProducer/main.py`
4. Run Spark Structured Streaming as : `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 src/StructuredStreaming/main.py`
5. Run Streaming using delta-table as sink : `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.1.0 main.py`
6. Checking if producer is producing data : `sudo docker exec -it 281b46827836 /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic wikimedia_data_topic` (replace 281b46827836 with your container_id)



