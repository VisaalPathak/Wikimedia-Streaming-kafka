
from utils.variables import Variables
var = Variables()

bootstrap_server = var["kafka_config"]["bootstrap_servers"]

def loadKafkaTopic(topic,spark,maxOffsetsPerTrigger=200,startingTime=1):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",bootstrap_server)
            .option("subscribe",topic)
            .option("maxOffsetsPerTrigger",maxOffsetsPerTrigger)
            .option("startingTimestamp",startingTime)
            .load())
    

