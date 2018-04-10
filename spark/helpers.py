import time


def replace_none_with_null(dictionary):
    for key in dictionary:
        if dictionary[key] is None:
            dictionary[key] = 'null'
    return dictionary


def get_encounter_stream(kafka_stream):
    return kafka_stream\
        .filter(lambda msg: msg['schema']['name'] == 'dbserver1.openmrs.encounter.Envelope') \
        .map(lambda msg: replace_none_with_null(msg['payload']['after'])) \
        .map(lambda msg: dict(**msg)) \

def get_new_obs_stream(kafka_stream):
    return kafka_stream \
        .filter(lambda msg: msg['schema']['name'] == 'dbserver1.openmrs.obs.Envelope') \
        .filter(lambda msg: msg['payload']['op'] == 'c')\
        .map(lambda msg: replace_none_with_null(msg['payload']['after']))\
        .map(lambda msg: dict(**msg)) \

def get_updated_obs_stream(kafka_stream):
    return kafka_stream \
        .filter(lambda msg: msg['schema']['name'] == 'dbserver1.openmrs.obs.Envelope') \
        .filter(lambda msg: msg['payload']['op'] == 'u')\
        .map(lambda msg: replace_none_with_null(msg['payload']['after'])) \
        .map(lambda msg: dict(**msg)) \

def display_stream(spark, stream):
    stream \
        .foreachRDD(lambda rdd: display_rdd(spark, rdd))


def display_df(df):
    df.show()


def display_rdd(spark, rdd):
    if rdd.isEmpty():
        print("--- EMPTY RDD ---" + time.ctime())
    else:
        display_df(spark.read.json(rdd))

def save_stream_to_parquet(stream,path):
        stream.foreachRDD(lambda rdd: save_rdd_to_parquet(rdd,path))

def save_rdd_to_parquet(rdd,path):
        if not rdd.isEmpty():
            rdd.toDF().write.mode('append').parquet(path)
            print('--- Saved Parquet! ---' + path)
            return True
        else:
            return False