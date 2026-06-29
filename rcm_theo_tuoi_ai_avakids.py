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
from pyspark.sql.functions import when, col, expr, lit, avg, countDistinct, max , min, size, collect_list, concat_ws, split, count, collect_set, array, round, abs as F_abs, trim, sum, row_number, mean, coalesce
import sys
import os
import gc
import time
import re
from dataclasses import dataclass
from typing import NamedTuple, Dict, List
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, ArrayType

#Import Spark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F
import json
import http

#Tạo đường link
import sys
import os
# from connect_db import config_db
from spark_config import spark_connect

spark = spark_connect()

### Data Chân dung KH - độ tuổi:

link_t12 = "hdfs://172.16.5.69:8020/tri/tri_an_kh/avakids/muanhieu/khach_hang_dotuoi_k5_t12.parquet"
khach_hang_dotuoi_t12  = spark.read\
                        .format('parquet')\
                        .option("header", "true")\
                        .option("multiline","true")\
                        .load(link_t12)

count_null= khach_hang_dotuoi_t12.filter(F.col("stage_last").isNotNull()).count()
print(count_null)


### Transform data
# Bước 1: Tìm mode của stage_last theo nhóm prediction
mode_counts = khach_hang_dotuoi_t12.filter(F.col("stage_last").isNotNull()) \
    .groupBy("prediction", "stage_last") \
    .agg(F.count("*").alias("count"))

# Sử dụng Window để lấy stage_last có count cao nhất
window = Window.partitionBy("prediction").orderBy(F.col("count").desc(), F.col("stage_last"))
mode_df = mode_counts.withColumn("rn", F.row_number().over(window)) \
    .filter(F.col("rn") == 1) \
    .select("prediction", F.col("stage_last").alias("mode_stage_last"))

# Join với df gốc và điền giá trị null
df_stage_last_filled = khach_hang_dotuoi_t12.join(mode_df, on="prediction", how="left") \
    .withColumn(
        "stage_last",
        F.when(F.col("stage_last").isNull(), F.col("mode_stage_last")).otherwise(F.col("stage_last"))
    ) \
    .drop("mode_stage_last")

# Bước 2: Tính mean của expected_age_last theo nhóm prediction và stage_last (record không null)
mean_df = khach_hang_dotuoi_t12.filter(F.col("expected_age_last").isNotNull()) \
    .groupBy("prediction", "stage_last") \
    .agg(F.mean("expected_age_last").alias("mean_expected_age_last"))

# Join để điền giá trị null
df_filled = df_stage_last_filled.join(mean_df, on=["prediction", "stage_last"], how="left") \
    .withColumn(
        "expected_age_last",
        F.when(F.col("expected_age_last").isNull(), F.col("mean_expected_age_last")).otherwise(F.col("expected_age_last"))
    ) \
    .drop("mean_expected_age_last")

df_filled = df_filled.withColumn("expected_age_last", round(col("expected_age_last"), 0))
df_filled = df_filled.drop_duplicates(subset=['customer_id'])
df_final = df_filled.select('customer_id', 'stage_last' ,'expected_age_last')

df_final_pd = df_final.toPandas()

### Data Chiều cao - cân nặng bài tin chỉ số chiều cao cân nặng: https://www.nhathuocankhang.com/ban-tin-suc-khoe/bang-tieu-chuan-can-nang-chieu-cao-cua-be-theo-to-chuc-y-te-the-gioi-who-1161393

df_height_weight = pd.read_excel(r'/data/dhvtu/rcm/chu_ky_sua_ta/data/input/tuoi_chieucao_cannang.xlsx' ,  sheet_name=0)
df_height_weight.head()

df_final_pd = df_final_pd.merge(df_height_weight, right_on='Tuổi (tháng)' , left_on = 'expected_age_last', how='left')
df_final_pd = df_final_pd.drop_duplicates(subset=['customer_id'])

df_sorted = df_final_pd.sort_values(by='expected_age_last', ascending=False)


#  fill giá trị bằng trung bình 2 giá trị  của 2 record giữa khoảng bị null
columns_to_fill = ['Tuổi (tháng)', 'Cân nặng (kg)', ' Cân nặng (lb)', 'Chiều cao (inch)','Chiều cao (cm)']
df_sorted[columns_to_fill] = df_sorted[columns_to_fill].interpolate(method='linear')
df_filled_cus = df_sorted

df_filled_cus = df_filled_cus.sort_values(by='expected_age_last', ascending=True)
df_filled_cus = df_filled_cus.fillna(method='ffill')

df_filled_cus.drop(columns=['Tuổi (tháng)'], inplace=True)
df_filled_cus.columns = df_filled_cus.columns.str.lstrip()
df_filled_cus = df_filled_cus.sort_values(by='customer_id', ascending=True)

### Property values:
link_props = "hdfs://172.16.5.69:8020/tri/cms/avakids/props.parquet"
props  = spark.read\
                        .format('parquet')\
                        .option("header", "true")\
                        .option("multiline","true")\
                        .load(link_props)

view1 = props[
    (props['propertyid'] =='age_range_filter_multi')
    |(props['propertyid'] =='recommended_age_usp')
    |(props['propertyid'] =='age_usp')
    |(props['propertyid'] =='recommended_age')
    |(props['propertyid'] =='age_range')
    |(props['propertyid'] =='children_clothing_size')
             ]
# view1.show(truncate=False)

unique_values = view1.select("provalue").distinct().collect()
unique_values = pd.DataFrame(unique_values, columns=["provalue_cleaned"])

df_clean = unique_values
chuoi_can_bo = ['L', 'M', 'S', 'XS', 'Freesize']
pattern = '|'.join(chuoi_can_bo)  # tạo chuỗi regex: 'abc|xyz|123'
df_clean['provalue_cleaned'] = df_clean['provalue_cleaned'].astype(str)
df_clean = df_clean[~df_clean['provalue_cleaned'].str.contains(pattern, regex=True, na=False)]

# df_clean = df_clean.filter(~col("provalue_cleaned").rlike(pattern))

def extract_age_range(text):
    text = str(text).lower().strip()

    # Trẻ sơ sinh mặc định là 0 tháng
    if "trẻ sơ sinh" in text:
        return 0.0, 2.0

    # Dưới x tuổi / tháng
    match = re.match(r"dưới\s+(\d+(?:[\.,]\d+)?)\s*(tuổi|tháng)", text)
    if match:
        value, unit = match.groups()
        value = float(value.replace(',', '.'))
        return 0.0, value * 12 if unit == 'tuổi' else value

    # Từ x tháng/tuổi đến y tháng/tuổi
    match = re.match(r"từ\s+(\d+(?:[\.,]\d+)?)\s*(tháng|tuổi)\s+đến\s+(\d+(?:[\.,]\d+)?)\s*(tháng|tuổi)", text)
    if match:
        v1, u1, v2, u2 = match.groups()
        v1 = float(v1.replace(',', '.')) * (12 if u1 == 'tuổi' else 1)
        v2 = float(v2.replace(',', '.')) * (12 if u2 == 'tuổi' else 1)
        return v1, v2

    # Từ x tháng/tuổi
    match = re.match(r"từ\s+(\d+(?:[\.,]\d+)?)\s*(tháng|tuổi)", text)
    if match:
        value, unit = match.groups()
        value = float(value.replace(',', '.'))
        return value * 12 if unit == 'tuổi' else value, np.nan

    # Dạng x - y tháng/tuổi
    match = re.match(r"(\d+(?:[\.,]\d+)?)\s*-\s*(\d+(?:[\.,]\d+)?)\s*(tháng|tuổi)", text)
    if match:
        v1, v2, unit = match.groups()
        v1 = float(v1.replace(',', '.'))
        v2 = float(v2.replace(',', '.'))
        if unit == 'tuổi':
            return v1 * 12, v2 * 12
        else:
            return v1, v2

    # Dạng x tháng/tuổi - y tháng/tuổi (phức tạp)
    match = re.match(r"(\d+(?:[\.,]\d+)?)\s*(tháng|tuổi)\s*-\s*(\d+(?:[\.,]\d+)?)\s*(tháng|tuổi)", text)
    if match:
        v1, u1, v2, u2 = match.groups()
        v1 = float(v1.replace(',', '.')) * (12 if u1 == 'tuổi' else 1)
        v2 = float(v2.replace(',', '.')) * (12 if u2 == 'tuổi' else 1)
        return v1, v2

    # Dạng "x tuổi" / "x tháng"
    match = re.match(r"(\d+(?:[\.,]\d+)?)\s*(tuổi|tháng)", text)
    if match:
        value, unit = match.groups()
        value = float(value.replace(',', '.'))
        return value * 12 if unit == 'tuổi' else value, value * 12 if unit == 'tuổi' else value

    # fallback không match gì
    return np.nan, np.nan

# Áp dụng hàm
df_clean[['min_age_months', 'max_age_months']] = df_clean['provalue_cleaned'].apply(
    lambda x: pd.Series(extract_age_range(x))
)
df_clean

max_value = df_clean['max_age_months'].max()
df_age_range = df_clean.copy()
df_age_range['max_age_months'] = df_age_range['max_age_months'].fillna(max_value)
df_age_range['min_age_months'] = df_age_range['min_age_months'].fillna(0)
df_age_range = spark.createDataFrame(df_age_range)
# df_age_range.orderBy('min_age_months', ascending = True).show(120, truncate=False)

props = props.join(
    df_age_range,
    props["provalue"] == df_age_range["provalue_cleaned"],
    how="left"
)
props_tuoi = props.toPandas()

# Ép kiểu int
props_tuoi['category_code'] = props_tuoi['category_code'].astype(int)
# Kết nối tên và thuộc tính sp:
props_tuoi['combined'] = props_tuoi['productname'].astype(str) + '@@' + props_tuoi['provalue'].astype(str)


props_tuoi = props_tuoi[
    props_tuoi['category_code'].isin([
        114, 112, 209, 189, 271, 272, 270, 122, 123, 110, 108, 107, 121, 120,
        124, 125, 126, 127, 106, 105, 113, 109, 116, 102, 103, 111, 279, 273,
        274, 275, 276, 277, 278, 281, 284, 283, 232, 264, 263, 194, 200, 201,
        254, 150, 13, 176, 208, 289, 290, 294, 299, 306, 307, 364, 59, 3, 353,
        236, 237, 238, 239, 240, 241, 242, 243, 206, 193, 18, 38, 33, 28, 24,
        268, 296, 300, 302, 303, 308
    ])
]
props_tuoi = props_tuoi[
    props_tuoi['propertyid'].isin([
        'age_range_filter_multi', 'recommended_age_usp', 'age_usp',
        'recommended_age', 'age_range', 'weight_filter', 'weight_usp',
		'children_clothing_size'
    ])
]
# props_tuoi.head(10)
### Property values:
pm_product_link = "hdfs://172.16.5.69:8020/tri/cms/avakids/pmproduct.parquet"
pm_product  = spark.read\
                        .format('parquet')\
                        .option("header", "true")\
                        .option("multiline","true")\
                        .load(pm_product_link)
pm_product_pd = pm_product.toPandas()
pm_product_pd = pm_product_pd[["productid", "categoryid", "categoryname"]].drop_duplicates()
# product_file = props_tuoi.select("productid", "productname").distinct().toPandas()
product_file = props_tuoi[["productid", "productname", "categoryid", "item_base_id"]].drop_duplicates()
product_file["productname"] = product_file["productname"].str.lower()

# lst_product = [i[0] for i in props_tuoi.select('productname').distinct().collect()]
lst_product = props_tuoi['combined'].drop_duplicates().tolist()

# len(lst_product)


KH_tuoi = df_filled_cus


from collections import defaultdict, deque
from datetime import datetime

import os, re, json, time, http.client, pandas as pd
from datetime import datetime

# ====== THAM SỐ ======
BATCH_SIZE = 500
SLEEP_SEC = 0.25
START_ROW = 0
END_ROW   = 73288  # inclusive
TOP_K_LLM = 20    # cho LLM trả pool lớn hơn

# ====== Tham số đa dạng hóa ======
FINAL_K   = 15   # số sản phẩm cuối
MAX_PER_ITEM = 2  # tối đa biến thể / item_base_id
# =====================

# (Tùy chọn) dùng RapidFuzz nếu có, fallback difflib
try:
    from rapidfuzz import process as rf_process, fuzz as rf_fuzz
    _USE_RF = True
except Exception:
    import difflib
    _USE_RF = False


to_day = datetime.now().strftime("%Y%m%d")
hdfs_out_dir = f"hdfs://172.16.5.69:8020/tri/tri_an_kh/avakids/mualai/rcm_theo_tuoi_ai_v1/ai_goiy_{to_day}"
os.makedirs(hdfs_out_dir, exist_ok=True)

# ====== CHUẨN BỊ DATASET & TỪ ĐIỂN TÊN ======
# product_file: DataFrame có cột ['productid','productname'] (đã tồn tại sẵn theo code bạn)
prod_df = product_file[['productid','productname', 'categoryid','item_base_id']].dropna().drop_duplicates()

prod_df = prod_df.merge(pm_product_pd, on=['productid', 'categoryid'], how ='left')


# map nhanh cho exact/ci
name_set_exact = set(prod_df['productname'])
name_map_lower = {n.lower(): n for n in name_set_exact}
# chuẩn bị list tên để đưa vào prompt (tránh quá dài: có thể cắt theo chiến lược nếu cần)
# Ở đây mình đưa full; nếu danh sách quá lớn => cân nhắc chia theo category / paging.
lst_product = "\n".join(f"- {n}" for n in sorted(name_set_exact))

def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip()).lower()

def _fuzzy_best_match(q: str, choices: list, score_cutoff: int = 89):
    """Trả về (best_match, score) hoặc (None, 0)"""
    if _USE_RF:
        res = rf_process.extractOne(
            q, choices, scorer=rf_fuzz.token_sort_ratio, score_cutoff=score_cutoff
        )
        if res:
            return res[0], int(res[1])
        return None, 0
    else:
        # fallback difflib
        candidates = difflib.get_close_matches(q, choices, n=1, cutoff=score_cutoff/100.0)
        if candidates:
            # difflib không trả score; tạm giả định mức pass
            return candidates[0], 100
        return None, 0

def build_prompt(do_tuoi, can_nang):
    # Prompt ràng buộc mạnh: chỉ được chọn tên Y HỆT; nếu không chắc => trả "[]"
    return f"""Bạn là trợ lý gợi ý sản phẩm cho trẻ em.

DANH SÁCH SẢN PHẨM (chỉ được chọn từ danh sách này, giữ NGUYÊN tên):
{lst_product}

Khách hàng:
- Độ tuổi: {do_tuoi} (tháng)
- Cân nặng: {can_nang} (kg)

YÊU CẦU:
- Trả về đúng {TOP_K_LLM} sản phẩm PHÙ HỢP nhất từ danh sách trên.
- BẮT BUỘC định dạng duy nhất: [Tên sản phẩm 1, Tên sản phẩm 2, ...]
- KHÔNG viết gì khác ngoài đúng một danh sách ở trên.
- Nếu không tự tin chọn đủ, hãy trả về danh sách rỗng: []
- Tránh chọn quá nhiều biến thể của cùng một sản phẩm (không quá 2 biến thể cho mỗi sản phẩm gốc).
- Ưu tiên danh sách đa dạng danh mục (quần áo, đồ chơi, xe…) thay vì tập trung vào 1 danh mục.
"""

def call_llm_get_text(content: str) -> str:
    conn = http.client.HTTPConnection("gwrdinternal.tgdd.vn", timeout=30)
    payload = json.dumps({
        "messages": [{"content": content, "role": "user"}],
        "model": "MWG",
        "max_tokens": 512,
        "temperature": 0,
        "top_p": 0.95,
        "top_k": 50
    })
    headers = {'accept': 'application/json', 'Content-Type': 'application/json'}
    conn.request("POST", "/llm/v1/chat/completions", payload, headers)
    res = conn.getresponse()
    result = res.read().decode("utf-8", errors="ignore")
    conn.close()

    data = json.loads(result)
    return data["choices"][0]["message"]["content"]

def parse_names_from_llm(text: str) -> list:
    """
    Cố gắng parse theo nhiều kiểu:
    - [a, b, c]
    - "a", "b", "c"
    - gạch đầu dòng...
    Trả về list tên (raw) đã strip.
    """
    # 1) ưu tiên khối trong ngoặc vuông
    m = re.search(r"\[(.*?)\]", text, re.DOTALL)
    if m:
        inner = m.group(1)
        parts = [p.strip().strip('"').strip("'") for p in inner.split(",")]
        parts = [re.sub(r"\s+", " ", p) for p in parts if p]
        return parts

    # 2) dòng gạch đầu dòng
    bullets = re.findall(r"^[\-\*\•]\s*(.+)$", text, flags=re.MULTILINE)
    if bullets:
        return [re.sub(r"\s+", " ", b.strip().strip('"').strip("'")) for b in bullets]

    # 3) fallback: split theo xuống dòng
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return [re.sub(r"\s+", " ", ln.strip('"').strip("'")) for ln in lines]

def map_names_to_products(raw_names: list) -> pd.DataFrame:
    """
    Map tên LLM -> product_file theo 3 bước: exact, case-insensitive, fuzzy.
    Giữ thứ tự & loại trùng.
    """
    seen = set()
    mapped_rows = []

    for nm in raw_names:
        # Nếu trong văn bản có '@@', cắt trước
        base = nm.split('@@')[0].strip()
        if not base:
            continue

        # 1) exact
        if base in name_set_exact and base not in seen:
            rows = prod_df.loc[prod_df['productname'] == base, ['productid','productname']]
            for _, r in rows.iterrows():
                mapped_rows.append((r['productid'], r['productname']))
                seen.add(base)
            continue

        # 2) case-insensitive
        lower = base.lower()
        if lower in name_map_lower:
            true_name = name_map_lower[lower]
            if true_name not in seen:
                rows = prod_df.loc[prod_df['productname'] == true_name, ['productid','productname']]
                for _, r in rows.iterrows():
                    mapped_rows.append((r['productid'], r['productname']))
                    seen.add(true_name)
                continue

        # 3) fuzzy
        best, score = _fuzzy_best_match(_normalize(base), [ _normalize(n) for n in name_set_exact ], score_cutoff=89)
        if best:
            # best là version đã normalize; tìm lại tên gốc theo normalize
            # tạo map normalize->original một lần (lười tối ưu hóa: làm inline)
            norm2orig = {}
            for n in name_set_exact:
                norm2orig[_normalize(n)] = n
            true_name = norm2orig.get(best)
            if true_name and true_name not in seen:
                rows = prod_df.loc[prod_df['productname'] == true_name, ['productid','productname']]
                for _, r in rows.iterrows():
                    mapped_rows.append((r['productid'], r['productname']))
                    seen.add(true_name)

    if not mapped_rows:
        return pd.DataFrame(columns=['productid','productname'])
    return pd.DataFrame(mapped_rows, columns=['productid','productname']).drop_duplicates()


def diversify_candidates(df_map: pd.DataFrame, product_file: pd.DataFrame,
                         k: int = 15, max_per_item: int = 2) -> pd.DataFrame:
    """
    - Gom biến thể theo item_base_id
    - Giới hạn số biến thể / item_base_id
    - Đa dạng theo categoryid/categoryname
    """

    # join thêm thông tin sản phẩm
    cols_join = ['productid','productname','categoryid','categoryname','item_base_id']
    cols_join = [c for c in cols_join if c in product_file.columns]
    ext = df_map.merge(product_file[cols_join].drop_duplicates('productid'),
                       on=['productid','productname'], how='left')

    # nếu thiếu item_base_id thì fallback bằng chính productid
    ext['item_base_id'] = ext['item_base_id'].fillna(ext['productid'])

    # giới hạn số biến thể mỗi item_base_id
    ext['rank_in_item'] = ext.groupby('item_base_id').cumcount()
    ext = ext[ext['rank_in_item'] < max_per_item].drop(columns=['rank_in_item'])

    if ext.empty:
        return df_map.head(0)

    # phân bổ đa dạng theo categoryid
    buckets = {cid: g for cid, g in ext.groupby('categoryid')}
    order = list(buckets.keys())  # lấy theo thứ tự xuất hiện
    picks = []
    ptrs = {cid: 0 for cid in buckets}

    # vòng tròn lấy sản phẩm theo cate
    while len(picks) < k and any(ptrs[cid] < len(buckets[cid]) for cid in order):
        for cid in order:
            if len(picks) >= k:
                break
            if ptrs[cid] < len(buckets[cid]):
                picks.append(buckets[cid].iloc[ptrs[cid]])
                ptrs[cid] += 1

    out = pd.DataFrame(picks).drop_duplicates(['productid'])
    return out[["ma_kh","productid","productname","categoryid","categoryname","item_base_id","reason"]].head(k)

def rcm_sp_theo_tuoi(ma_kh, do_tuoi, can_nang):
    content = build_prompt(do_tuoi, can_nang)
    text = call_llm_get_text(content)

    raw_names = parse_names_from_llm(text)
    df_map = map_names_to_products(raw_names)

    if df_map.empty:
        # vẫn lưu reason để debug
        return pd.DataFrame([{
            "ma_kh": ma_kh,
            "productid": None,
            "productname": None,
            "reason": text
        }])

    df_map["ma_kh"] = ma_kh
    df_map["reason"] = text
    
    return df_map[["ma_kh","productid","productname","reason"]]


# ====== CHẠY THEO BATCH ======
row_indices = list(range(START_ROW, END_ROW + 1))
total_batches = (len(row_indices) + BATCH_SIZE - 1) // BATCH_SIZE

total_errors = 0
for b in range(total_batches):
    batch_indices = row_indices[b*BATCH_SIZE : (b+1)*BATCH_SIZE]

    batch_results = []
    batch_errors = 0
    processed_rows = 0

    for row_id in batch_indices:
        try:
            ma_kh   = KH_tuoi.iloc[row_id]["customer_id"]
            do_tuoi = KH_tuoi.iloc[row_id]["expected_age_last"]
            can_nang = KH_tuoi.iloc[row_id]["Cân nặng (kg)"]

            df_res_raw = rcm_sp_theo_tuoi(ma_kh, do_tuoi, can_nang)

            # --- GHÉP DIVERSIFY CHO TỪNG KH  ---
            df_res_valid = df_res_raw.dropna(subset=['productid'])
            if not df_res_valid.empty:
                df_res = diversify_candidates(df_res_valid, prod_df, k=FINAL_K, max_per_item=MAX_PER_ITEM)
            else:
                # giữ lại reason để debug khi không match sản phẩm nào
                df_res = df_res_raw.head(1)

            processed_rows += 1
            # log ngắn gọn
            print(f"Row {row_id} - customer_id: {ma_kh} -> {len(df_res.dropna(subset=['productid']))} match")

            batch_results.append(df_res)

        except Exception as e:
            batch_errors += 1
            total_errors += 1
            print(f"❌ Lỗi tại row {row_id}: {e}")

        time.sleep(SLEEP_SEC)

    # Gộp & lưu file theo số DÒNG MATCH (productid != null)
    if batch_results:
        df_batch = pd.concat(batch_results, ignore_index=True)
        num_rows_matched = len(df_batch.dropna(subset=['productid']))
    else:
        df_batch = pd.DataFrame(columns=["ma_kh", "productid", "productname", "reason"])
        num_rows_matched = 0

    batch_no = b + 1
    batch_path = f"{hdfs_out_dir}/batch={batch_no:03d}_rows={num_rows_matched}"
    spark_df = spark.createDataFrame(df_batch)
    # out_path = os.path.join(out_dir, f"ai_goiy_batch{batch_no:03d}_{num_rows_matched}rows.csv")
    # ghi mỗi batch vào một thư mục parquet, coalesce(1) để một file part/parquet
    spark_df.write.format("parquet").mode("overwrite").save(f"{batch_path}.parquet")

    print(f"✅ Hoàn thành batch {batch_no}/{total_batches}: lưu {num_rows_matched} dòng match, lỗi batch này: {batch_errors}. Tổng lỗi tích lũy: {total_errors}")

print(f"🎉 Hoàn tất {total_batches} batch. Tổng số record lỗi: {total_errors}")