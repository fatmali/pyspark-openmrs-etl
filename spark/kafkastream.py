import json

from pyspark.streaming.kafka import KafkaUtils


class KafkaStream:

    @staticmethod
    def create_source(ssc,config, topics):
        kafkaConfig = config
        kafkaTopics = topics
        return KafkaUtils\
              .createDirectStream(ssc,topics=kafkaTopics,kafkaParams=kafkaConfig) \
              .map(lambda msg: json.loads(msg[1]))\
