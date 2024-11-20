import sys
import os

root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, root_dir)

from utils.logger import logger
from utils.spark import start_spark
from utils.helper import loadKafkaTopic,writeDataDelta,writeDataSQL
from utils.variables import Variables
from streaming import dataCleaning,dataExtraction

var = Variables()

app_name = var["streaming"]["app_name"]
spark_sql_kafka = var["streaming"]["spark_sql_kafka"]
maxOffsetsPerTrigger = var["streaming"]["maxOffsetsPerTrigger"]
startingTime = var["streaming"]["startingTime"]

topic_name = var["kafka_config"]["api_topic"]

writeData = var["streaming"]["writeData"]

def main():
    logger.info(f"spark281b46827836 starting")

    spark = start_spark(app_name=app_name,home_path=root_dir)
    logger.info(f"spark started")
    data = loadKafkaTopic(topic=topic_name,spark=spark,maxOffsetsPerTrigger=maxOffsetsPerTrigger,startingTime=startingTime)
    logger.info(f"data successfully loaded from kafka topic {topic_name}")
    # logger.info(data.show())
    
    df = dataCleaning(df=data)
    
    logger.info(f"data extraction process")
    df = dataExtraction(df=df)
    
    logger.info("Writing Data to sink")
    
    
    if  "mysql" in writeData:
        query = df.writeStream \
        .foreachBatch(writeDataSQL) \
        .option("checkpointLocation", f"{root_dir}/checkpoint/mysql_{topic_name}") \
        .outputMode("append") \
        .start()
    logger.info(f"mysql table inserted")
     
    if "delta-table" in writeData:
        logger.info(f"delta-table creating")
        query = writeDataDelta(df=df,base_data_dir=root_dir,topic_name=topic_name)
        logger.info(f"delta-table created")
    else:
        query = (df.writeStream 
        .outputMode("append") 
        .format("console") 
        .start())

    # Await termination to keep the streaming running
    query.awaitTermination()
    
if __name__ == "__main__":
    main()