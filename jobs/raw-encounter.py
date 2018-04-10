import json
import time
from config.config import getConfig
from etl import EtlJob
from spark.helpers import *
from spark.kafkastream import KafkaStream
from spark.sparkstreaming import SparkApp
import pyspark.sql.functions as F
from pyspark.sql.types import *

class RawEncounter(EtlJob):

    def __init__(self):
        self.start()

    def start(self):
        config = getConfig()['spark']
        SparkApp.start_spark(app_name=config['app_name'],
                             master=config['master'],
                             spark_config=config['configs'],
                             callback=self.start_job)

    def start_job(self, spark, ssc):

        # start kafka stream
        config = getConfig()['kafka']
        kafka_stream = KafkaStream.create_source(ssc, config['config'], config['topics'])

        # filter streams
        new_encounters = get_encounter_stream(kafka_stream)
        new_obs = get_new_obs_stream(kafka_stream)
        updated_obs = get_updated_obs_stream(kafka_stream)

        # save encounters to parquet
        save_stream_to_parquet(new_encounters,path="spark-warehouse/encounters")

        #transform new obs
        # to do:
            # 1) add code to deal with updated obs (voided)
            # 2) add code to deal with new obs but existing encounters
            # 3) add code to deal with obs group
        self.transform_data(spark,new_obs)

    def transform_data(self, spark, stream):
        stream.foreachRDD(lambda rdd: transform_and_save(rdd))

        def transform_and_save(rdd):
            if rdd.isEmpty():
                print('--- EMPTY RDD ---'+ time.ctime())
            else:
                obs = rdd.toDF()
                transformed_obs = self.group_obs(spark,obs)
                transformed_obs.write.mode("append").parquet("spark-warehouse/flatobs")

    @staticmethod
    def group_obs(spark,df):

        encounters = spark.read.parquet('spark-warehouse/encounters')

        obSchema = StructType()\
        .add("obs_id", LongType(), nullable=False)\
        .add("obs_voided", LongType(), nullable=True)\
        .add("concept_id", StringType(), nullable=True)\
        .add("value", StringType(), nullable=True)\
        .add("value_type", StringType(), nullable=True)\
        .add("obs_datetime", StringType(), nullable=True)

        # replace "null" with null
        cols = [F.when(~F.col(x).isin("null"), F.col(x)).alias(x) for x in df.columns]

        obs = df.select(*cols)

        filtered_obs_with_value = obs \
            .filter("value_coded is not null")\
            .withColumn("value", F.col("value_coded"))\
            .withColumn("value_type", F.lit("coded"))\
            .union(obs.filter("value_text is not null")
                   .withColumn("value", F.col("value_text"))
                   .withColumn("value_type", F.lit("text")))\
            .union(obs.filter("value_numeric != 'null'")
                   .withColumn("value", F.col("value_numeric"))
                   .withColumn("value_type", F.lit("numeric")))\
            .union(obs.filter("value_datetime is not null")
                   .withColumn("value", F.col("value_datetime"))
                   .withColumn("value_type", F.lit("datetime")))\
            .union(obs.filter("value_modifier is not null")
                   .withColumn("value", F.col("value_modifier"))
                   .withColumn("value_type", F.lit("modifier")))\
            .union(obs.filter("value_drug is not null")
                   .withColumn("value", F.col("value_drug"))
                   .withColumn("value_type", F.lit("drug"))) \
            .withColumn("obs_date", (F.col("obs_datetime")/1000).cast(TimestampType()))\
            .withColumnRenamed("voided", "obs_voided") \
            .select("obs_id", "encounter_id", "obs_group_id", "concept_id", "obs_voided", "value", "value_type", "obs_date") \


        filtered_obs_with_value.show()

        json_string_obs = filtered_obs_with_value\
            .withColumn("strObs",
                        F.to_json(F.struct(F.col("obs_id"), F.col("obs_voided"),
                        F.col("concept_id"), F.col("value"), F.col("value_type"),
                        F.from_unixtime(F.unix_timestamp(F.col("obs_date"),'dd-MM-yyyy')))))


        json_parsed_obs = json_string_obs\
            .withColumn("parsedObs", F.from_json(F.col("strObs"), obSchema))\
            .groupBy("encounter_id")\
            .agg(F.collect_set(F.col("parsedObs")).alias("obs"))\
            .join(encounters, on=["encounter_id"])

        json_string_obs.show()
        json_parsed_obs.show()
        return filtered_obs_with_value



    def load_data(self, df):
        pass

    def extract_data(self,spark):
        # extract_lookup_data(spark)
        pass







def main():
    RawEncounter()


if __name__ == '__main__':
    # entry point for PySpark ETL application
    main()
