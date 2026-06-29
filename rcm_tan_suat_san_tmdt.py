#Import thư viện khác
import pandas as pd

from datetime import datetime, timedelta
# from sqlalchemy.engine import create_engine
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional
import pandas as pd
import numpy as np
import pytz
from hdfs import InsecureClient
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, expr, lit, avg, countDistinct, max , min, size, collect_list, concat_ws, split, count, collect_set, array, round, abs as F_abs, trim, sum

import numpy as np
import re
from dataclasses import dataclass
from typing import NamedTuple, Dict, List
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, ArrayType

#Import Spark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F



# from connect_db import config_db
from spark_config_207 import spark_connect

spark = spark_connect()

dmx_hisorde = spark.read.parquet(r'hdfs://172.16.5.69:8020/tri/check/dmx/dmx_hisorde.parquet')
# cateid_1943.select('productname', 'alike_productname').show(300,truncate=False)

dmx_hisorde.show()
customer_outline = spark.read.parquet(r'hdfs://172.16.38.99:8020/tri/tri_an_kh/san_tmdt/customer_outline.parquet')  

dmx_hisorde = dmx_hisorde.join(
    F.broadcast(customer_outline),
    "customer_id",
    "left_anti"
)

# LỌC OUTPUTTYPEID: HÌNH THỨC XUẤT
dmx_hisorde_filter = dmx_hisorde.filter( (F.col('outputtypeid').isin(8, 222, 2163, 2283, 2683, 2884, 3483, 3503, 3625)) 
                                        )

dmx_hisorde_slim = dmx_hisorde_filter.select('productid', 'crmcustomerid', 'saleorderid','outputdatetime', 'categoryid').dropDuplicates()
dmx_hisorde_slim.show()
dmx_hisorde_slim.count()



def top_product_per_category_with_category_info(df: DataFrame) -> DataFrame:
    """
    Tạo bảng tổng hợp:
    - Với mỗi khách hàng, tính tần suất và rank các category
    - Với mỗi category, lấy ra 1 sản phẩm được mua nhiều nhất
    """
    # Chuyển đổi outputdatetime sang timestamp
    dft = df.withColumn('outputdatetime', F.to_timestamp('outputdatetime'))
    
    # === PHẦN 1: Tính thông tin CATEGORY ===
    # Đếm số đơn hàng chứa category
    orders_per_cate = dft.groupBy('crmcustomerid', 'categoryid').agg(
        F.countDistinct('saleorderid').alias('category_order_count')
    )
    
    # Tổng số đơn hàng của mỗi khách hàng
    total_orders_per_cus = dft.groupBy('crmcustomerid').agg(
        F.countDistinct('saleorderid').alias('total_orders')
    )
    
    # Join và tính tần suất category
    category_info = orders_per_cate.join(total_orders_per_cus, on='crmcustomerid', how='left')
    
    category_info = category_info.withColumn(
        'category_freq',
        F.round((F.col('category_order_count') / F.col('total_orders') * 100), 2)
    )
    
    # Rank category - ĐƯA NULL XUỐNG CUỐI
    rank_window = Window.partitionBy('crmcustomerid').orderBy(
        F.col('categoryid').isNull().asc(),  # NULL = False (0) lên trước, NULL = True (1) xuống sau
        F.col('category_order_count').desc(),
        F.col('categoryid').asc()
    )
    
    category_info = category_info.withColumn('category_rank', F.rank().over(rank_window))
    
    # === PHẦN 2: Tính thông tin PRODUCT trong mỗi CATEGORY ===
    # Đếm số đơn hàng chứa sản phẩm trong mỗi category
    orders_per_prod_cate = dft.groupBy('crmcustomerid', 'categoryid', 'productid').agg(
        F.countDistinct('saleorderid').alias('order_count')
    )
    
    # Tổng số đơn hàng có category này của mỗi khách hàng
    total_orders_per_cate = dft.groupBy('crmcustomerid', 'categoryid').agg(
        F.countDistinct('saleorderid').alias('total_orders_in_category')
    )
    
    # Join để tính tần suất sản phẩm
    product_info = orders_per_prod_cate.join(
        total_orders_per_cate, 
        on=['crmcustomerid', 'categoryid'], 
        how='left'
    )
    
    product_info = product_info.withColumn(
        'product_freq_in_category',
        F.round((F.col('order_count') / F.col('total_orders_in_category') * 100), 2)
    )
    
    # Lần mua gần nhất
    last_purchase = dft.groupBy('crmcustomerid', 'categoryid', 'productid').agg(
        F.max('outputdatetime').alias('last_purchase_date')
    )
    
    product_info = product_info.join(
        last_purchase,
        on=['crmcustomerid', 'categoryid', 'productid'],
        how='left'
    )
    
    # Rank sản phẩm trong mỗi category
    product_rank_window = Window.partitionBy('crmcustomerid', 'categoryid').orderBy(
        F.col('order_count').desc(),
        F.col('last_purchase_date').desc(),
        F.col('productid').asc()
    )
    
    product_info = product_info.withColumn(
        'product_rank_in_category', 
        F.rank().over(product_rank_window)
    )
    
    # Chỉ lấy sản phẩm rank 1 trong mỗi category
    product_info = product_info.filter(F.col('product_rank_in_category') == 1)
    
    # === PHẦN 3: JOIN 2 phần lại với nhau ===
    result = product_info.join(
        category_info.select('crmcustomerid', 'categoryid', 'category_freq', 'category_rank'),
        on=['crmcustomerid', 'categoryid'],
        how='left'
    )
    
    # Chọn các cột theo đúng thứ tự yêu cầu
    result = result.select(
        'crmcustomerid',
        'categoryid',
        'productid',
        'order_count',
        'total_orders_in_category',
        'product_freq_in_category',
        'last_purchase_date',
        'category_freq',
        'category_rank'
    ).orderBy('crmcustomerid', 'category_rank')
    
    return result

df_customer_category_product = top_product_per_category_with_category_info(dmx_hisorde_slim)
df_customer_category_product.show(30, truncate=False)

df_customer_sp = df_customer_category_product.withColumnRenamed('crmcustomerid', 'customer_id')
# MAP MODEL CODE:
df_model = spark.read.parquet(r'hdfs://172.16.38.99:8020/tri/tri_an_kh/san_tmdt/productcode_modelcode_mapping.parquet')
df_model = df_model.withColumnRenamed('modelCode', 'model_code')


df_joined = df_customer_sp.join(df_model.select('model_code', 'productCode'), F.trim(df_model.productCode) == F.trim(df_customer_sp.productid), how = 'left') 
df_joined = df_joined.drop('productCode')

# LƯU DATA:
df_joined.write.format("parquet").mode("overwrite").save(r"hdfs://172.16.38.99:8020/tri/tri_an_kh/san_tmdt/customer_product_frequency.parquet")
