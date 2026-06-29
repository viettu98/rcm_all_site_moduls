#Import thư viện khác
import pandas as pd
import pytz
from datetime import datetime, timedelta
# from sqlalchemy.engine import create_engine
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional
import pandas as pd
import numpy as np
import datetime
import pytz
from hdfs import InsecureClient
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, expr, lit, avg, countDistinct, max , min, size, collect_list, concat_ws, split, count, collect_set, array, round, abs as F_abs, trim, sum, row_number, mean
import sys
import os
import gc
import time
import re
import random
from dataclasses import dataclass
from typing import NamedTuple, Dict, List
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, ArrayType
from dateutil.relativedelta import relativedelta

#Import Spark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F
import json
import trino


#Tạo đường link
import sys
import os
# from connect_db import config_db
from spark_config_207_test import spark_connect

spark = spark_connect()


props_mapping_dmx = spark.read.parquet(r'hdfs://172.16.38.99:8020/tri/cms/props_mapping_dmx.parquet')
# cateid_1943.select('productname', 'alike_productname').show(300,truncate=False)
props_mapping_dmx.show()

props_mapping_dmx_shorten = props_mapping_dmx.select('PROPERTYID', 'PROPERTYIDMAP').drop_duplicates()

props_mapping_dmx_main = props_mapping_dmx
props_mapping_dmx_sub = props_mapping_dmx.select('PROPERTYIDMAP','VALUEIDMAP').drop_duplicates()

print(props_mapping_dmx_main.count())
print(props_mapping_dmx_sub.count())

props_dmx = spark.read.parquet(r'hdfs://172.16.5.69:8020/tri/cms/props_dmx.parquet')
props_dmx = props_dmx.withColumnRenamed('PROPVALUEID', 'VALUEID')
props_dmx.show()
props_dmx.printSchema()


df_model = spark.read.parquet(r'hdfs://172.16.5.69:8020/tri/check/dmx/product_model_code.parquet')
df_model = df_model.withColumn("productcode_erp", F.trim(F.col("productcode_erp")))

link_product_model = props_dmx.join(df_model.select('model_code', 'productcode_erp'), F.trim(df_model.productcode_erp) == F.trim(props_dmx.PRODUCTCODE), how = 'inner') 
link_product_model = link_product_model.select('PRODUCTCODE', 'model_code', 'CATEGORYID').dropDuplicates()

props_dmx_main = props_mapping_dmx_main.join(props_dmx, on=['PROPERTYID', 'VALUEID'], how = 'left')
props_dmx_main.show()

props_dmx_main = props_dmx_main.select( 'PRODUCTCODE', 'PRODUCTID','PRODUCTNAME','PROPERTYNAME', 'PROPERTYID','VALUEID', 'PROPVALUE','CATEGORYID', 'MANUFACTURERID','STATUSID', 'PROPERTYIDMAP', 'VALUEIDMAP').drop_duplicates()

props_dmx_sub = props_mapping_dmx_sub.join(
    props_dmx, 
    (props_mapping_dmx_sub.PROPERTYIDMAP == props_dmx.PROPERTYID) & 
    (props_mapping_dmx_sub.VALUEIDMAP == props_dmx.VALUEID), 
    how='left'
)
props_dmx_sub.show()

props_dmx_sub = props_dmx_sub.select('PROPERTYIDMAP','VALUEIDMAP','PRODUCTCODE','PRODUCTNAME','CATEGORYID').drop_duplicates()
props_dmx_sub = props_dmx_sub.withColumnRenamed('PRODUCTCODE', 'PRODUCTCODE_SUB')
props_dmx_sub = props_dmx_sub.withColumnRenamed('PRODUCTNAME', 'PRODUCTNAME_SUB')
props_dmx_sub = props_dmx_sub.withColumnRenamed('CATEGORYID', 'CATEGORYID_SUB')

df_main_sub = (
    props_dmx_main
    .join(
        props_dmx_sub,
        on=["PROPERTYIDMAP","VALUEIDMAP"],
        how="inner"
    )
)

df_result = df_main_sub.filter(col("PRODUCTCODE") != col("PRODUCTCODE_SUB"))
df_result.show()

df_result_shorten = df_result.select('PRODUCTCODE', 'PRODUCTID', 'PRODUCTCODE_SUB', 'CATEGORYID_SUB', 'PROPERTYID', 
                                     'PROPERTYIDMAP', 'VALUEIDMAP', 'VALUEID', 'PRODUCTNAME', 'PROPERTYNAME','PRODUCTNAME_SUB', 'PROPVALUE').drop_duplicates()

df_grouped = df_result_shorten.groupBy(
    'PRODUCTCODE', 
    'PRODUCTNAME',
    'PRODUCTCODE_SUB',
	'PRODUCTNAME_SUB',
    'PRODUCTID',
    'CATEGORYID_SUB'
).agg(
    F.count('*').alias('score')
)

df_grouped.show()

# Tạo window spec để partition theo PRODUCTCODE và CATEGORYID_SUB
windowSpec = Window.partitionBy('PRODUCTCODE', 'CATEGORYID_SUB').orderBy(F.desc('score'))

# Thêm rank và lọc lấy rank = 1 (score cao nhất)
df_ranked = df_grouped.withColumn('rank', F.row_number().over(windowSpec)) \
                     .filter(F.col('rank') == 1) \
                     .drop('rank')

df_ranked.show()

df_joined = df_ranked.join(df_model.select('model_code', 'productcode_erp'), F.trim(df_model.productcode_erp) == F.trim(df_ranked.PRODUCTCODE_SUB), how = 'left') 
df_joined = df_joined.drop('productcode_erp')

df_joined.repartition(48) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save(r'hdfs://172.16.5.69:8020/tri/check/dmx/sp_muacung.parquet')	