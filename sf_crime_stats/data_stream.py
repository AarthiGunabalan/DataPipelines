import logging
import json, time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([StructField("crime_id",StringType(),False),
                     StructField("original_crime_type_name",StringType(),False),
                     StructField("report_date",StringType(),False),
                     StructField("call_date",StringType(),False),
                     StructField("offense_date",StringType(),False),
                     StructField("call_time",StringType(),False),
                     StructField("call_date_time",TimestampType(),False),
                     StructField("disposition",StringType(),False),
                     StructField("address",StringType(),False),
                     StructField("city",StringType(),False),
                     StructField("state",StringType(),False),
                     StructField("agency_id",StringType(),False),
                     StructField("address_type",StringType(),False),
                     StructField("common_location",StringType(),False)
                    ])


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka")\
        .option("subscribe","sf.police.dept.service.calls")\
        .option("kafka.bootstrap.servers","localhost:9092")\
        .option("maxRatePerPartition",100)\
        .option("maxOffsetsPerTrigger",200)\
        .option("numRows",1000)\
        .option("startingOffsets","earliest")\
        .option("stopGracefullyOnShutdown", "true") \
        .load()
        
        

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df\
        .selectExpr("CAST(value as STRING) as value")

    service_table = kafka_df\
        .select(psf.from_json("value", schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table\
        .selectExpr("original_crime_type_name", "disposition", "to_timestamp(call_date_time) as call_date_time").distinct()
        
    
    # count the number of original crime type
    
    agg_df = distinct_table\
                .withWatermark("call_date_time","10 minutes")\
                .groupBy("original_crime_type_name","disposition",psf.window("call_date_time","60 minutes"))\
                .count()\
                .select(psf.col("window").start.cast("string").alias("start"), psf.col("window").end.cast("string").alias("end"), "original_crime_type_name","disposition","count")
    
    """   
    query = agg_df\
        .writeStream\
        .format("console")\
        .outputMode("append")\
        .trigger(processingTime="3 seconds") \
        .option("truncate", False) \
        .start()
    """
    
    #query.awaitTermination()
    
    #time.sleep(100)
        

    # TODO get the right radio code json path
    radio_code_json_filepath = "/home/workspace/radio_code.json"
    radio_code_df = spark.read.option("multiline","true").json(radio_code_json_filepath)
    
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    
    join_query = agg_df.join(radio_code_df,agg_df["disposition"]==radio_code_df["disposition"])\
                    .select("start", "end", "original_crime_type_name","description","count")\
                    .writeStream\
                    .format("console")\
                    .outputMode("append")\
                    .trigger(processingTime="3 seconds") \
                    .option("truncate", False) \
                    .start()
    
    

    join_query.awaitTermination()
    
    #time.sleep(100)



if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
    
    print(spark.sparkContext.getConf().getAll())
    
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
