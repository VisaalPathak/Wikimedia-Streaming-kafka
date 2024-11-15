import sys
import os

root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, root_dir)

from utils.logger import logger
from utils.spark import start_spark
from utils.helper import loadKafkaTopic
from utils.variables import Variables
from streaming import dataCleaning,dataExtraction

var = Variables()

app_name = var["streaming"]["app_name"]
spark_sql_kafka = var["streaming"]["spark_sql_kafka"]
maxOffsetsPerTrigger = var["streaming"]["maxOffsetsPerTrigger"]
startingTime = var["streaming"]["startingTime"]

topic_name = var["kafka_config"]["api_topic"]

def main():
    logger.info(f"spark starting")

    spark = start_spark(app_name=app_name,jar_packages=spark_sql_kafka)
    logger.info(f"spark started")
    data = loadKafkaTopic(topic=topic_name,spark=spark,maxOffsetsPerTrigger=maxOffsetsPerTrigger,startingTime=startingTime)
    logger.info(f"data successfully loaded from kafka topic {topic_name}")
    # logger.info(data.show())
    
    df = dataCleaning(df=data)
    
    logger.info(f"data extraction process")
    df = dataExtraction(df=df)
    
    # Write the output to the console for testing
    query = (df.writeStream 
        .outputMode("append") 
        .format("console") 
        .start())

    # Await termination to keep the streaming running
    query.awaitTermination()
    
if __name__ == "__main__":
    main()