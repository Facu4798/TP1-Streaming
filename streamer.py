###########################
#### install libraries ####
###########################

print("Installing libraries...\n")

import subprocess
import os

libs = subprocess.run(
    ["pip",'list'],
    capture_output=True, 
    text=True
).stdout.splitlines()

libs = [lib.split(" ")[0] for lib in libs[2:]]

required_libs =[
    "pandas",
    "pyspark",
    "numpy",
    "Flask",
    "flask-cors"    
]

for lib in required_libs:
    if lib not in libs:
        print(f"Installing {lib}...")
        subprocess.run(["pip", "install", "-q", lib], check=True)

########################
#### install jdk 17 ####
########################

print("Installing JDK 17...\n")
os.system("sudo apt-get update -qq")
os.system("sudo apt-get install openjdk-17-jdk-headless -qq")
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"



###########################
#### queue data buffer ####
###########################
from queue import Queue
data_queue = Queue(maxsize=1)

#################
#### imports ####
#################

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, functions as sf, types as st, Window



#######################
#### Spark Session ####
#######################


spark = SparkSession.builder\
    .appName("Streaming")\
    .getOrCreate()

sv = spark.version
print(f"Starting Spark session with spark version = {sv}...\n")



##############################################
#### streaming optimization spark configs ####
##############################################

spark.conf.set("spark.sql.shuffle.partitions", "2")
spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint")
spark.conf.set("spark.default.parallelism", "2")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.serialization.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")


query = spark.readStream\
    .format("rate")\
    .option("rowsPerSecond", 10)\
    .load()


def feb(df,epoch_id):
    data = df\
        .withColumn("timestamp", sf.current_timestamp())\
        .withColumn("epoch_id", sf.lit(epoch_id))\
    

    # cast timestamp to string
    for c in data.columns:
        dt = data.schema[c].dataType
        if isinstance(dt, st.TimestampType) or isinstance(dt, st.DateType):
            data = data.withColumn(c, sf.col(c).cast("string"))

    data_d = [row.asDict() for row in data.collect()]
    for row in data_d:
        try:
            _ = data_queue.get_nowait() # remove old data if exists
        except:
            pass
        try:
            data_queue.put_nowait(row) # add new data
        except:
            pass

query.writeStream\
    .format("console")\
    .outputMode("append")\
    .foreachBatch(feb)\
    .start().awaitTermination()
