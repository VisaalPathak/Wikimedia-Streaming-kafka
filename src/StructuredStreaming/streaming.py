import pyspark.sql.functions as F
from utils.logger import logger


def dataCleaning(df):
                
    df = df.withColumn('value',F.col("value").cast("string"))
    parsed_data = df.withColumn("value_clean", F.regexp_replace("value", r'\\\\', r'\\'))
    parsed_data = parsed_data.withColumn(
        "value_clean",
        F.regexp_replace(F.col("value_clean"), r'\\', '')
    )

    parsed_data = parsed_data.withColumn(
        "value_clean",
        F.regexp_replace(F.col("value_clean"), r'^"|"$', '')
    )
    parsed_data = parsed_data.withColumn("value_clean", F.regexp_replace("value_clean", r"\$", ""))
    return parsed_data

def defineSchema():
    return """schema string , meta struct <uri string, request_id string, id string, dt string, domain string, stream string >,
                    id bigint, type string, namespace string, title string, user string, bot string, minor boolean, pantrolled boolean,
                    length struct < old int, new int >,revision struct < old bigint, new bigint>,server_name string, wiki string
            """


def dataExtraction(df):
    df = df.withColumn("data", F.from_json(F.col("value_clean"), defineSchema()))
    df_parsed = (df.selectExpr("data.schema", "data.meta.uri", 
                              "data.meta.request_id","data.meta.id as meta_id","data.meta.dt","data.meta.domain","data.meta.stream",
                              "data.id","data.type","data.namespace","data.title","data.user","data.bot","data.minor","data.pantrolled","data.length.old as length_old","data.length.new length_new",
                              "data.revision.old as revision_old","data.revision.new revision_new","data.server_name","data.wiki"
                              
                              ))
    return df_parsed
        
