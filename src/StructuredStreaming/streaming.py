import pyspark.sql.functions as F
from utils.logger import logger


def dataCleaning(df):
                
    # Replace escaped backslashes with single ones to ensure correct parsing
    df = df.withColumn('value',F.col("value").cast("string"))
    parsed_data = df.withColumn("value_clean", F.regexp_replace("value", r'\\\\', r'\\'))
    # Remove backslashes from the start of key names and values, as well as the quotes at the start and end
    parsed_data = parsed_data.withColumn(
        "value_clean",
        F.regexp_replace(F.col("value_clean"), r'\\', '')
    )

    # Also remove double quotes at the beginning and end if not done already
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
    df_parsed = (df.select("data.schema", "data.meta.uri", 
                                "data.meta.request_id","data.meta.id","data.meta.dt","data.meta.domain","data.meta.stream",
                                "data.id","data.type","data.namespace","data.title","data.user","data.bot","data.minor","data.pantrolled","data.length.old","data.length.new",
                                "data.revision.old","data.revision.new","data.server_name","data.wiki"
                                ))
    return df_parsed
        
