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

#Import Spark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F


#Tạo đường link
import sys
import os
# from connect_db import config_db
from spark_config import spark_connect

spark = spark_connect()

#Tạo file log
import logging
logging.basicConfig(filename='execution_time.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s')

### Data product_prop tã
link_props = "hdfs://172.16.5.69:8020/tri/cms/avakids/props.parquet"
props  = spark.read\
                        .format('parquet')\
                        .option("header", "true")\
                        .option("multiline","true")\
                        .load(link_props)
product_prop_ta = props[
    (props['propertyid'] =='size_product_filter')
    & (props['category_code'] == "36")
    ]
product_prop_ta = product_prop_ta.select('productid', 'provalue').drop_duplicates()
product_prop_ta = product_prop_ta.withColumnRenamed("provalue", "filter_name")
product_prop_ta = product_prop_ta.toPandas()



### Data pm_product
pm_product_link = "hdfs://172.16.5.69:8020/tri/cms/avakids/pmproduct.parquet"
pm_product  = spark.read\
                        .format('parquet')\
                        .option("header", "true")\
                        .option("multiline","true")\
                        .load(pm_product_link)      
pm_product.printSchema()
pm_product = pm_product[['productid','productname', 'category_code', 'categoryname','brandid', 'model_code']]
pm_product = pm_product.toPandas()
df_merge = pd.merge(product_prop_ta, pm_product[['productid','productname', 'category_code', 'categoryname','brandid']], on='productid', how='left')
df_merge.count()
df_merge['brandid'] = df_merge['brandid'].fillna(0)

### Data hisorder
hisorder_link = "hdfs://172.16.5.69:8020/tri/tri_an_kh/avakids/hisorder.parquet"
hisorder  = spark.read\
                        .format('parquet')\
                        .option("header", "true")\
                        .option("multiline","true")\
                        .load(hisorder_link)\
                        .select(F.col('customer_id').cast('string').alias('customer_id'),
                                'productid', 'formatted_date') 
hisorder = hisorder.toPandas()
hisorder['formatted_date'] = pd.to_datetime(hisorder['formatted_date']).dt.date
hisorder = hisorder.merge(pm_product, on='productid', how= 'left')
hisorder_filter = hisorder[(hisorder['category_code'] == '99')
                            | (hisorder['category_code'] == '36')
                              ]
hisorder_filter.info()

### Check lịch sử đơn hàng
# fil = hisorder_filter[hisorder_filter['customer_id'] == 1058647870]
# fil



### ta_filter
ta_filter = df_merge[df_merge['category_code'] == '36']
ta_filter

unique_size_ta = ta_filter['filter_name'].unique()
unique_size_ta



### Ranking tã
# Define desired order from smallest to largest
order = ['NB', 'XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL']

# Create ranking mapping
ranking_map = {size: rank+1 for rank, size in enumerate(order)}

# Build DataFrame
df_sizes = pd.DataFrame({
    'filter_name': unique_size_ta
})
df_sizes['ranking'] = df_sizes['filter_name'].map(ranking_map)

# Sort by ranking
df_ranking_ta = df_sizes.sort_values(by='ranking').reset_index(drop=True)

df_merge = pd.merge(df_merge, df_ranking_ta, how = 'left', on= 'filter_name')


ta_filter = df_merge[df_merge['category_code'] == '36']

### sua_filter
product_prop_sua = props[
    ((props['propertyid'] =='age_range')
    | (props['propertyid'] =='age_usp'))
    & (props['category_code'] == "99")
    ]

product_prop_sua = product_prop_sua.select('productid', 'provalue').drop_duplicates()
product_prop_sua = product_prop_sua.withColumnRenamed("provalue", "filter_name")
product_prop_sua = product_prop_sua.toPandas()

product_prop_sua = pd.merge(product_prop_sua, pm_product, on = 'productid', how='left')

unique_filter_types = product_prop_sua['filter_name'].unique()


### Ranking sữa
def parse_label(lbl):
    if lbl == "Trẻ sơ sinh":
        return 0, 1
    m_from = re.match(r"Từ (\d+) tuổi", lbl)
    if m_from:
        low = int(m_from.group(1)) * 12
        return low, np.nan
    m_range = re.match(r"(\d+) - (\d+) (tháng|tuổi)", lbl)
    if m_range:
        low = int(m_range.group(1))
        high = int(m_range.group(2))
        unit = m_range.group(3)
        if unit == "tuổi":
            low *= 12; high *= 12
        return low, high - low
    raise ValueError(lbl)

# Build and sort df
unique_filter_types = pd.DataFrame([{"filter_name":lbl, **dict(zip(["min_age_months","range_months"], parse_label(lbl)))} for lbl in unique_filter_types])
df_sorted = unique_filter_types.sort_values(by=["min_age_months","range_months"], na_position="last").reset_index(drop=True)

# Add ranking column
df_sorted['ranking'] = df_sorted.index + 1

# Display full table
df_ranking_sua = df_sorted



### df_ranking
sua_filter = pd.merge(product_prop_sua, df_ranking_sua , how = 'left', on='filter_name')
sua_filter = sua_filter[['productid', 'ranking', 'brandid']]
sua_filter['productid'] = sua_filter['productid'].astype(str)


ta_filter = ta_filter[['productid', 'ranking', 'brandid']]
ta_filter['ranking'] = ta_filter['ranking'].astype('int64')

df_ranking = pd.concat([sua_filter, ta_filter], ignore_index=True)
df_ranking['brandid'] = df_ranking['brandid'].fillna(0)
df_ranking = df_ranking.sort_values(by=['productid','ranking'], ascending=True)
df_ranking = df_ranking.drop_duplicates(subset=['productid','brandid'], keep='first')

df_ranking

### Check rank Upsize:
# fil = df_ranking[df_ranking['productid'] == '1475087000211']
# fil


### Lấy list customerID, productID có chu kỳ
link = "hdfs://172.16.5.69:8020/tri/tri_an_kh/avakids/mualai/productid_diemchuky.parquet"
productid_diemchuky  = spark.read\
                        .format('parquet')\
                        .option("header", "true")\
                        .option("multiline","true")\
                        .load(link)\
                        .select(F.col('customer_id').cast('string').alias('customer_id'),
                                'categoryid', 'productid', 'score', 'rank')
productid_diemchuky_suata = productid_diemchuky[(productid_diemchuky['categoryid'].isin(99,36))]
# productid_diemchuky = productid_diemchuky.select('customer_id', 'productid', 'rank','categoryid')
productid_diemchuky_suata = productid_diemchuky_suata.withColumnRenamed("rank", "rank_chu_ky")
productid_diemchuky_suata = productid_diemchuky_suata.toPandas()
productid_diemchuky_suata = productid_diemchuky_suata.merge(pm_product, on = 'productid', how='left')


### Check rank chu kỳ
# fil = productid_diemchuky[(productid_diemchuky['productid'] == '1475087000211')
#                           & (productid_diemchuky['customer_id'] == '1058647870')
# 						  ]
# fil



### Lọc list có score >= 0.5
productid_diemchuky_filter = productid_diemchuky_suata[productid_diemchuky_suata['score'] >= 0.5]
productid_diemchuky_filter
##############################


df_customer = productid_diemchuky_filter[['customer_id','productid','brandid']]
df_customer = df_customer.drop_duplicates(subset=['customer_id','productid'], keep='first')

# Merge with ranking DataFrame
df_original = df_customer.merge(df_ranking, on=['productid', 'brandid'])

# Loại bỏ các dòng trùng: mỗi sản phẩm gốc duy nhất 1 lần
df_original = df_original.drop_duplicates(subset=['customer_id', 'productid', 'brandid'])


# Tạo DataFrame cho các dòng 'rcm'
# Tạo danh sách để lưu các dòng rcm
output_rows = []

for _, row in df_original.iterrows():
    customer = row['customer_id']
    original_product = row['productid']
    original_ranking = row['ranking']
    original_brandid = row['brandid']

    # Ghi lại sản phẩm gốc
    output_rows.append({
        'customer_id': customer,
        'productid': original_product,
        'ranking': original_ranking,
        'brandid': original_brandid,
        'type': 'ori'
    })
    

	 # Tìm sản phẩm có rank cao hơn và gần nhất
    higher_rank = df_ranking[
        (df_ranking['brandid'] == original_brandid) &
        (df_ranking['ranking'] > original_ranking) &
        (df_ranking['productid'] != original_product)
    ]

    if not higher_rank.empty:
        # Xác định rank nhỏ nhất lớn hơn original_ranking
        min_higher_rank = higher_rank['ranking'].min()
        candidates = higher_rank[higher_rank['ranking'] == min_higher_rank]

        # Chọn ngẫu nhiên 1 sản phẩm trong số này
        selected_row = candidates.sample(n=1).iloc[0]
        output_rows.append({
            'customer_id': customer,
            'productid': selected_row['productid'],
            'ranking': selected_row['ranking'],
            'brandid': original_brandid,
            'type': 'rcm'
        })

# Kết quả cuối cùng
df_output_ranking = pd.DataFrame(output_rows).sort_values(by=['customer_id', 'ranking'])


### Thêm bớt các cột liên quan
# Xóa trùng
df_add_ons = df_output_ranking.drop_duplicates(subset=['customer_id','productid'], keep='first')

# Đổi tên cột:
df_add_ons.rename(columns={"ranking": "rank_us"}, inplace=True)

# Merge với pm_product
df_add_ons = pd.merge(df_add_ons, pm_product, on=['productid', 'brandid'], how='left')

# Gán row_num cho type = 'rcm'
mask_rcm = df_add_ons['type'] == 'rcm'
df_add_ons.loc[mask_rcm, 'row_num'] = (
    df_add_ons.loc[mask_rcm]
    .groupby(['customer_id', 'category_code'])
    .cumcount() + 1
)

# Gán row_num = 0 cho type = 'ori'
df_add_ons.loc[df_add_ons['type'] == 'ori', 'row_num'] = 0

# Đảm bảo row_num là số nguyên
df_add_ons['row_num'] = df_add_ons['row_num'].astype(int)


# Giả sử df là DataFrame chứa dữ liệu
# Tạo cột 'subgroup' để đánh dấu các nhóm con dựa trên customer_id, category_code và row_num = 0
df_add_ons['subgroup'] = (df_add_ons['row_num'] == 0).groupby([df_add_ons['customer_id'], df_add_ons['category_code'], df_add_ons['brandid']]).cumsum()

# Thêm cột productid_ori bằng cách lấy productid từ bản ghi đầu tiên (row_num = 0) trong mỗi nhóm con
df_add_ons['productid_ori'] = df_add_ons.groupby(['customer_id', 'category_code', 'subgroup','brandid'])['productid'].transform('first')

# (Tùy chọn) Xóa cột subgroup nếu không cần thiết
df_add_ons = df_add_ons.drop(columns=['subgroup'])


# Maping lấy điểm chu kỳ
productid_diemchuky_tini = productid_diemchuky_filter[['customer_id','productid','rank_chu_ky']]
df_add_ons = df_add_ons.merge(productid_diemchuky_tini, on = ['customer_id','productid'], how = 'left')


### Fill rank chu kỳ
df_format_rck = df_add_ons
# Tạo dictionary ánh xạ từ (customer_id, productid) sang formatted_date cho các dòng type='ori'
ori_rck_map = df_format_rck[df_format_rck['type'] == 'ori'][['customer_id', 'productid', 'rank_chu_ky']].set_index(['customer_id', 'productid'])['rank_chu_ky'].to_dict()

# Hàm để fill formatted_date dựa trên customer_id và productid_ori
def fill_rank_ck(row):
    if row['type'] == 'rcm':
        return ori_rck_map.get((row['customer_id'], row['productid_ori']), '')
    return row['rank_chu_ky']

# Áp dụng hàm để fill formatted_date
df_format_rck['rank_chu_ky'] = df_format_rck.apply(fill_rank_ck, axis=1)


df_sorted = df_format_rck 
# Sắp xếp theo ranking tăng dần
df_sorted = df_sorted.sort_values(by=['customer_id', 'rank_chu_ky'])

# Tạo cột rank theo từng customerid
df_sorted['rank'] = df_sorted.groupby('customer_id')['rank_chu_ky'].rank(method='first').astype(int)

### Các record có cùng customer_id, productid_ori, thì chỉ giữ lại đúng 1 sản phẩm có type ori và 1 sản phẩm type rcm có rank_us kế cạnh
def filter_ori_rcm(df):
    results = []

    for _, group in df.groupby(['customer_id', 'productid_ori']):
        # Lấy ori rank nhỏ nhất
        ori_row = group[group['type'] == 'ori'].sort_values('rank_us').head(1)
        results.append(ori_row)

        if not ori_row.empty:
            ori_rank = ori_row['rank_us'].iloc[0]
            rcm_candidates = group[group['type'] == 'rcm'].sort_values('rank_us')

            # Ưu tiên rank liền kề
            rcm_next = rcm_candidates[rcm_candidates['rank_us'] == ori_rank + 1]
            if rcm_next.empty:
                # Nếu không có thì lấy rank lớn hơn gần nhất
                rcm_next = rcm_candidates[rcm_candidates['rank_us'] > ori_rank].head(1)

            if not rcm_next.empty:
                results.append(rcm_next)

    return pd.concat(results, ignore_index=True)

df_shorty = filter_ori_rcm(df_sorted)

# Maping lấy ngày mua (formatted_date)
df_format_date = df_shorty.merge(hisorder_filter[['customer_id','productid','formatted_date']], on = ['customer_id','productid'], how = 'left')
df_format_date = df_format_date.sort_values('formatted_date', ascending=False).drop_duplicates(subset=['customer_id', 'productid','rank_us', 'brandid','type','productname', 'category_code', 'categoryname', 'row_num', 'productid_ori','rank_chu_ky'])



### Fill ngày cho các product rcm

# Tạo dictionary ánh xạ từ (customer_id, productid) sang formatted_date cho các dòng type='ori'
ori_date_map = df_format_date[df_format_date['type'] == 'ori'][['customer_id', 'productid', 'formatted_date']].set_index(['customer_id', 'productid'])['formatted_date'].to_dict()

# Hàm để fill formatted_date dựa trên customer_id và productid_ori
def fill_formatted_date(row):
    if row['type'] == 'rcm':
        return ori_date_map.get((row['customer_id'], row['productid_ori']), '')
    return row['formatted_date']

# Áp dụng hàm để fill formatted_date
df_format_date['formatted_date'] = df_format_date.apply(fill_formatted_date, axis=1)
df_filled_date = df_format_date

### Lấy top sp có ngày mua gần nhất 
# Group by và lấy ngày lớn nhất
# Sau khi groupby lấy ngày max
df_max = df_filled_date.groupby(['customer_id', 'category_code'], as_index=False)['formatted_date'].max()

# Merge với bảng gốc
df_result = df_filled_date.merge(df_max, on=['customer_id', 'category_code', 'formatted_date'], how='inner')
df_result.drop(columns=['rank_us'], inplace=True)
df_result = spark.createDataFrame(df_result)




#############################################  Data gợi ý sản phẩm theo tuổi ##############################################

df_tuoi_link = "hdfs://172.16.5.69:8020/tri/tri_an_kh/avakids/mualai/rcm_theo_tuoi_v1.parquet"
df_tuoi  = spark.read\
                        .format('parquet')\
                        .option("header", "true")\
                        .option("multiline","true")\
                        .load(df_tuoi_link) 

df_tuoi = df_tuoi.select('customer_id', 'productid', 'productname', 'category_code')
pm_product_sp = spark.createDataFrame(pm_product)
pm_product_sp = (
    pm_product_sp
    .withColumn("productid", F.col("productid").cast(StringType())))
# Join với pm_product
df_tuoi = df_tuoi.join(
    pm_product_sp.select("productid", "categoryname", "brandid"), on="productid", how="left"
)

## Reset rank
# Tạo window partition theo customer_id
w1 = Window.partitionBy("customer_id").orderBy("rank")  
# (orderBy cần có 1 cột để đảm bảo thứ tự, bạn thay "productid" bằng cột phù hợp)

df_result_rs_rank = df_result.withColumn(
    "reseted_rank",
    row_number().over(w1)
)
# df_result.filter(F.col('customer_id')==1009279440).show()


### Tìm rank lớn nhất của mỗi Khách hàng ở bảng df_result:
df_max_rank = (
    df_result_rs_rank
    .groupBy("customer_id")
    .agg(max("reseted_rank").alias("max_rank"))
)
# df_max_rank.rename(columns={"rank": "max_rank"}, inplace=True)



### Join để lấy data KH từ df_max_rank
df_tuoi_join = df_tuoi.join(df_max_rank, on='customer_id', how='inner')

# Gán max_rank = 0 nếu khách hàng không có trong df_max_rank
df_tuoi_join = (
    df_tuoi_join
    .fillna({"max_rank": 0})                # thay null = 0
    .withColumn("max_rank", col("max_rank").cast("int"))  # ép kiểu int
)

### Đánh rank mới: bắt đầu từ max_rank + 1 và tăng dần
w2 = Window.partitionBy("customer_id").orderBy("max_rank")  
# 👉 thay "some_column" bằng cột để định nghĩa thứ tự (vd: productid, formatted_date,...)

df_tuoi_join = df_tuoi_join.withColumn(
    "reseted_rank",
    row_number().over(w2) + col("max_rank")
)

w3 = Window.orderBy("customer_id")  # hoặc bất kỳ cột nào đảm bảo ổn định

df_tuoi_join = df_tuoi_join.withColumn(
    "row_num", 
    row_number().over(w3) - 1  # trừ 1 để bắt đầu từ 0 giống pandas reset_index
)

# Thêm cột type
df_tuoi_join = df_tuoi_join.withColumn("type", F.lit("age_rcm"))


df_tuoi_join = df_tuoi_join.select(
    ['customer_id', 'productid', 'reseted_rank', 'brandid', 'category_code', 'categoryname', 'productname', 'type']
)
# df_tuoi_join.show()
df_max_rank.filter(F.col('customer_id')==1009279440).show()

df_tuoi_join.filter(F.col('customer_id')==1009279440).show()


# Kết hợp với df_result
df_tuoi_union = df_result_rs_rank.unionByName(df_tuoi_join, allowMissingColumns=True)


### Chỉ giữ tối đa 30 sản phẩm cho mỗi khách hàng
w4 = Window.partitionBy("customer_id").orderBy("reseted_rank")

df_tuoi_union = (
    df_tuoi_union
    .withColumn("rn", row_number().over(w4))       # đánh số thứ tự trong từng customer_id
    .filter("rn <= 30")                           # lấy top 30 mỗi customer_id
    .drop("rn")                                   # bỏ cột phụ
)


df_tuoi_union = df_tuoi_union.drop("rank")
df_tuoi_union = df_tuoi_union.withColumnRenamed("reseted_rank", "rank")
# Giữ các cột mong muốn
df_tuoi_union = df_tuoi_union.select(
    "customer_id", "productid", "rank", "brandid", "category_code",
    "categoryname", "productname", "rank_chu_ky", "formatted_date",
    "type", "row_num", "productid_ori"
)


############### Tạo 1 df chứa các KH không thuộc list KH đã có tính chu kỳ ################
# Ép kiểu đồng nhất (vd. string) cho cả 2 DF
# productid_diemchuky = spark.createDataFrame(productid_diemchuky)
productid_diemchuky_filter = spark.createDataFrame(productid_diemchuky_filter)

productid_diemchuky       = productid_diemchuky.withColumn("customer_id", F.col("customer_id").cast(StringType()))
productid_diemchuky_filter= productid_diemchuky_filter.withColumn("customer_id", F.col("customer_id").cast(StringType()))

# Tạo danh sách customer_id duy nhất
lst_cus_ck_1 = productid_diemchuky_filter[['customer_id']].drop_duplicates()

# Lọc các bản ghi không có trong lst_cus_ck_1
productid_diemchuky_other = productid_diemchuky.join(
    lst_cus_ck_1, on="customer_id", how="left_anti"
)

# Lấy danh sách customer_id duy nhất
lst_cus_ck = productid_diemchuky_other.select("customer_id").distinct()


df_freq_link = "hdfs://172.16.5.69:8020/tri/tri_an_kh/avakids/muanhieu/productid_tansuat_v1.parquet"
df_freq  = spark.read\
                        .format('parquet')\
                        .option("header", "true")\
                        .option("multiline","true")\
                        .load(df_freq_link)   

pm_product_sp = spark.createDataFrame(pm_product)
df_freq = (
    df_freq
    .withColumn("productid", F.col("productid").cast(StringType())))
      
pm_product_sp = (
    pm_product_sp
    .withColumn("productid", F.col("productid").cast(StringType())))
# Join với pm_product
df_freq = df_freq.join(
    pm_product_sp.select("productid", "categoryname"), on="productid", how="left"
)

# Drop cột
df_freq = df_freq.drop("freq", "freq_by_order")

# Đổi tên cột
df_freq = df_freq.withColumnRenamed("last_date", "formatted_date")

# Ép kiểu customer_id về interger
df_freq = df_freq.withColumn("customer_id", F.col("customer_id").cast("int"))
lst_cus_ck = lst_cus_ck.withColumn("customer_id", F.col("customer_id").cast("int"))
lst_cus_ck.show()
df_freq.show()
lst_cus_ck.printSchema()
df_freq.printSchema()
# Join với lst_cus_ck
lst_cus_ck = lst_cus_ck.join(df_freq, on="customer_id", how="left")

# Bỏ trùng theo customer_id, productid
lst_cus_ck = lst_cus_ck.dropDuplicates(["customer_id", "productid"])

# Đánh rank mới theo customer_id
w = Window.partitionBy("customer_id").orderBy(F.monotonically_increasing_id())
lst_cus_ck = lst_cus_ck.withColumn("rank", F.row_number().over(w))

# Thêm cột type
lst_cus_ck = lst_cus_ck.withColumn("type", F.lit("freg_rcm"))

# Kết hợp với df_tuoi_union
df_final = df_tuoi_union.unionByName(lst_cus_ck, allowMissingColumns=True)
# df_final = df_tuoi_union.unionByName(lst_cus_ck)

# Giữ duy nhất customer_id, productid
df_final = df_final.dropDuplicates(["customer_id", "productid"])

# Chỉ giữ tối đa 30 sản phẩm cho mỗi khách hàng
w2 = Window.partitionBy("customer_id").orderBy("rank")
df_final = (
    df_final.withColumn("rn", F.row_number().over(w2))
            .filter(F.col("rn") <= 30)
            .drop("rn")
)

# Giữ các cột mong muốn
df_final = df_final.select(
    "customer_id", "productid", "rank", "brandid", "category_code",
    "categoryname", "productname", "rank_chu_ky", "formatted_date",
    "type", "row_num", "productid_ori"
)

df_final = (
    df_final
    .withColumn("customer_id", df_final["customer_id"].cast("int"))
    .withColumn("formatted_date", df_final["formatted_date"].cast("double"))
    .withColumn("row_num", df_final["row_num"].cast("long"))
)

df_final = df_final.join(
    pm_product_sp.select("productid", "model_code"), on="productid", how="left"
)
## lưu lại + log file final
begin = time.time()
df_final.write.format("parquet").mode("overwrite").save(r"hdfs://172.16.5.69:8020/tri/tri_an_kh/avakids/mualai/productid_chuky_upsize_v1.parquet")
end = time.time()
timespent = end-begin
noti_log = f"Thoi gian chay productid_chuky_upsize: {timespent} seconds"
logging.info(noti_log)
df_final.show()
print("Đã lưu thành công productid_chuky_upsize")