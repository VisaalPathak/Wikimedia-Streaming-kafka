
from utils.variables import Variables
var = Variables()

bootstrap_server = var["kafka_config"]["bootstrap_servers"]
driver= var["streaming"]["mysql_destination"]["driverName"]
host = var["streaming"]["mysql_destination"]["host"]
port = var["streaming"]["mysql_destination"]["port"]
user = var["streaming"]["mysql_destination"]["user"]
password = var["streaming"]["mysql_destination"]["password"]
database = var["streaming"]["mysql_destination"]["database"]
table = var["streaming"]["mysql_destination"]["table"]



def loadKafkaTopic(topic,spark,maxOffsetsPerTrigger=200,startingTime=1):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",bootstrap_server)
            .option("subscribe",topic)
            .option("maxOffsetsPerTrigger",maxOffsetsPerTrigger)
            .option("startingTimestamp",startingTime)
            .load())
    

def writeDataSQL(batch_df,batch_id):
    (batch_df.write \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", f"{driver}://{host}:{port}/{table}") \
    .option("dbtable", database) \
    .option("user", user) \
    .option("password", password) \
    .option("mode","append") \
    .save())

def writeDataDelta(df,base_data_dir,topic_name):
    return (df.writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{base_data_dir}/chekpoint/{topic_name}")
                    .outputMode("append")
                    .toTable(topic_name)
        )
