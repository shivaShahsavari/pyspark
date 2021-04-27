import log
from pyspark.sql.functions import col, concat_ws, lit
from os import listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import *


def main():
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')
    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data, config['steps_per_floor'])
    load_data(data_transformed)
    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    df = (spark.read.option("header", "true").option("delimiter",";").csv('test/employee.csv'))
    return df


def transform_data(df, steps_per_floor_):
    df_transformed = (
        df.select(col('id'),concat_ws(' ',col('first_name'),col('second_name')).alias('name'),
               (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk')))
    return df_transformed


def load_data(df):
    df_p=df.toPandas()
    df_p.to_csv("test/etl_result.csv", header=True)
    return None



def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    spark_builder = (SparkSession.builder.appName(app_name))
    # create Spark JAR packages string
    spark_jars_packages = ','.join(list(jar_packages))
    spark_builder.config('spark.jars.packages', spark_jars_packages)

    spark_files = ','.join(list(files))
    spark_builder.config('spark.files', spark_files)

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = log.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict


if __name__ == '__main__':
    main()
