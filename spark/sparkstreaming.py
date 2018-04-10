import os

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from dependencies import logging
from pyspark.sql import SparkSession


class SparkApp:

    @staticmethod
    def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                    spark_config={}, ssc_config={'batchDuration':5}, callback=None):

            spark_conf = SparkConf().setAppName(app_name).setMaster(master)

            if(spark_config):
                for config in spark_config:
                    spark_conf.setIfMissing(config,spark_config[config])

            if jar_packages:
                spark_jar_packages = '--packages ' + ','.join(jar_packages)
                os.environ["PYSPARK_SUBMIT_ARGS"] = spark_jar_packages

            sc = SparkContext(conf=spark_conf)
            spark = SparkSession(sparkContext=sc)


            # create session and retrieve Spark logger object
            # spark_logger = logging.Log4j(spark)

            ssc = StreamingContext(sc, ssc_config['batchDuration'])

            if callback:
               callback(spark=spark,ssc=ssc)

            ssc.start()
            ssc.awaitTermination()
