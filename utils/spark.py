"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

from pyspark.sql import SparkSession

def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[]):
    """Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, and
        config dict (only if available).
    """

    # get Spark session 
    spark_builder = (
        SparkSession
        .builder
        .master(master)
        .appName(app_name).enableHiveSupport())

    # create Spark JAR packages string
    spark_jars_packages = ','.join(list(jar_packages))
    spark_builder.config('spark.jars.packages', spark_jars_packages)

    spark_files = ','.join(list(files))
    spark_builder.config('spark.files', spark_files)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    return spark_sess

