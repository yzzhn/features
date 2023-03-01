import pyspark
from pyspark import SparkContext
from pyspark.conf import SparkConf
import os
import getpass
import tempfile

LOCAL_PATH = str(os.path.expanduser('~'))

shared_spark_context = None
shared_sql_context = None
spark_tmp_dir = None

def get_shared_spark_context(app_name='features', num_executors = '8', executor_memory='32g', driver_memory='32g', max_result_size = '32g', javaOpts='-XX:+UseG1GC'):
    global shared_spark_context
    global spark_tmp_dir
    if(shared_spark_context == None):
        conf = SparkConf()
        #conf.setMaster('local[{}]'.format(num_executors)).setAppName(app_name).set("spark.executor.memory", "18g").set('spark.driver.memory', driver_memory)
        conf.setAppName(app_name).set("spark.executor.memory", "18g").set('spark.driver.memory', driver_memory)

        #spark_tmp_dir = tempfile.TemporaryDirectory(dir='/home/yz6me/sparktmp', prefix='spark_')
        #print("Setting spark.local.dir to " + spark_tmp_dir.name)
        #conf.set("spark.local.dir", spark_tmp_dir.name)
        conf.set('spark.num.executors', num_executors)
        conf.set('spark.executor.memory', executor_memory)
        conf.set('spark.driver.memory', driver_memory)
        conf.set('spark.driver.maxResultSize', max_result_size)
        conf.set('spark.sql.parquet.cacheMetadata', "false")
        conf.set('spark.driver.extraJavaOptions', javaOpts)
        conf.set('spark.executor.extraJavaOptions', javaOpts)
        conf.set("spark.worker.cleanup.enabled", "true")
        conf.set("spark.worker.cleanup.appDataTtl", 1440*60)
        shared_spark_context = SparkContext( conf=conf )
        shared_spark_context.setLogLevel("WARN")
    return shared_spark_context


def get_shared_sql_context(**kwargs):
    global shared_spark_context
    global shared_sql_context
    if(shared_spark_context == None):
        get_shared_spark_context(**kwargs)
    if(shared_sql_context == None):
        shared_sql_context = pyspark.SQLContext(shared_spark_context)
    return shared_sql_context

