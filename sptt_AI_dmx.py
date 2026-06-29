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
from spark_config_207 import spark_connect

spark = spark_connect()



prop_dmx = spark.read.parquet(r'hdfs://172.16.5.69:8020/tri/cms/props_dmx.parquet')
prop_dmx.filter(F.col('productid')==307973).show(100,truncate=False)



fil_cate = prop_dmx[(prop_dmx['CATEGORYID'] == '7077')
                    & prop_dmx['STATUSID'].isin(4, 5)]

cate_pro = (
    fil_cate
        .select("PRODUCTCODE", "CATEGORYID")
        .drop_duplicates()
        .withColumn("PRODUCTCODE", F.trim(F.col("PRODUCTCODE")))
        .withColumnRenamed("PRODUCTCODE", "productid")
        .withColumnRenamed("CATEGORYID", "categoryid")
)

cate_pro = cate_pro.toPandas()

lst_prop = fil_cate.select('LISTPROPERTY').drop_duplicates()
lst_prop = (
    lst_prop.withColumn("LISTPROPERTY", F.split(F.col("LISTPROPERTY"), ","))  # tách thành mảng
      .withColumn("LISTPROPERTY", F.explode("LISTPROPERTY"))             # bung thành nhiều dòng
      .withColumn("LISTPROPERTY", F.col("LISTPROPERTY").cast("int"))     # ép kiểu số
      .filter(F.col("LISTPROPERTY") > 0)                                 # loại bỏ giá trị âm
)
lst_prop = [row["LISTPROPERTY"] for row in lst_prop.select("LISTPROPERTY").collect()]
print(lst_prop)

property_map = fil_cate[fil_cate['PROPERTYID'].isin(lst_prop)].select('PROPERTYID', 'PROPERTYNAME').drop_duplicates()
lst_propname = property_map.select('PROPERTYNAME').drop_duplicates().toPandas()
lst_propname = lst_propname['PROPERTYNAME'].tolist()
print(lst_propname)

### input:
category_chosen = fil_cate.select('CATEGORYNAME').drop_duplicates().toPandas()
category_chosen = category_chosen['CATEGORYNAME'].iloc[0]
print(category_chosen)



import http.client, json, re
from typing import List, Dict, Any



# ---- 1) Tạo prompt chọn thuộc tính ----
def make_attribute_rank_prompt(
    attributes: List[str],
    category: str = "Tivi",
    top_k: int = 5
) -> str:
    """
    attributes: danh sách thuộc tính đầu vào (ví dụ: ['Kích thước màn hình', 'Độ phân giải', ...])
    """
    attr_block = "\n".join(f"- {a}" for a in attributes)

    guide = f"""Nhiệm vụ:
Bạn là chuyên gia ngành hàng {category}. Từ DANH SÁCH THUỘC TÍNH dưới đây, hãy chọn ra đúng {top_k} thuộc tính QUAN TRỌNG NHẤT để mô tả và so sánh các sản phẩm {category}.

Yêu cầu:
- Chỉ chọn trong danh sách đã cho (không tự nghĩ thêm thuộc tính mới).
- Ưu tiên thuộc tính có tác động lớn tới quyết định mua và thể hiện khác biệt giữa các mẫu {category}.
- Trả về CHÍNH XÁC một mảng JSON gồm {top_k} phần tử, mỗi phần tử là object có dạng:
  {{
    "attribute": "<tên thuộc tính trích nguyên văn từ danh sách>",
    "reason": "<giải thích ngắn gọn 1-2 câu>",
    "weight": <số thực 0..1 biểu thị mức quan trọng tương đối, tổng các weight ≈ 1.0>
  }}
- KHÔNG kèm bất kỳ văn bản nào ngoài JSON. KHÔNG thêm comment hay markdown.
- Không được lặp thuộc tính. Không được để thiếu trường.

DANH SÁCH THUỘC TÍNH:
{attr_block}
"""
    return guide

# ---- 2) Gọi LLM (giữ nguyên style của bạn) ----
def call_llm(api_host: str, content: str,
             model="MWG", max_tokens=768, temperature=0.0, top_p=0.95, top_k=50) -> str:
    """Gọi API LLM nội bộ, trả về message content (string)."""
    conn = http.client.HTTPConnection(api_host)
    payload = json.dumps({
        "messages": [{"content": content, "role": "user"}],
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "top_p": top_p,
        "top_k": top_k
    })
    headers = {'accept': 'application/json', 'Content-Type': 'application/json'}
    conn.request("POST", "/llm/v1/chat/completions", payload, headers)
    res = conn.getresponse()
    raw = res.read().decode("utf-8")
    conn.close()
    data = json.loads(raw)
    return data["choices"][0]["message"]["content"]

# ---- 3) Parse JSON trả về (chịu lỗi tốt) ----
def parse_attribute_selection(raw_text: str) -> List[Dict[str, Any]]:
    """
    Tìm và parse mảng JSON đầu tiên trong response.
    Kỳ vọng mỗi phần tử có keys: attribute, reason, weight.
    """
    # Cố gắng tìm mảng JSON [ ... ]
    match = re.search(r"\[.*\]", raw_text, flags=re.DOTALL)
    text = match.group(0) if match else raw_text
    try:
        arr = json.loads(text)
        # Lọc & chuẩn hóa nhẹ
        cleaned = []
        for item in arr:
            if not isinstance(item, dict):
                continue
            attr = str(item.get("attribute", "")).strip()
            reason = str(item.get("reason", "")).strip()
            weight = item.get("weight", None)
            # ép weight về float hợp lệ
            try:
                weight = float(weight)
            except Exception:
                weight = None
            cleaned.append({"attribute": attr, "reason": reason, "weight": weight})
        # Chỉ giữ item có attribute không rỗng
        cleaned = [x for x in cleaned if x["attribute"]]
        return cleaned
    except Exception:
        # Fallback: nếu không parse được, trả rỗng
        return []

# ---- 4) Hàm tiện ích end-to-end ----
def select_top_attributes(
    api_host: str,
    attributes: List[str],
    category: str = "Tivi",
    top_k: int = 5,
    model: str = "MWG"
) -> List[Dict[str, Any]]:
    prompt = make_attribute_rank_prompt(attributes, category, top_k)
    resp = call_llm(api_host, prompt, model=model, max_tokens=1024, temperature=0.0)
    result = parse_attribute_selection(resp)

    # Hậu kiểm: đảm bảo đúng số lượng và nằm trong danh sách cho phép
    allowed = set(a.strip() for a in attributes)
    result = [r for r in result if r["attribute"] in allowed]
	
    # Nếu thiếu weight, gán đều
    if result:
        missing = [r for r in result if r["weight"] is None]
        if missing:
            equal_w = round(1.0 / len(result), 6)
            for r in result:
                if r["weight"] is None:
                    r["weight"] = equal_w

        # Chuẩn hóa tổng weight ≈ 1.0
        import builtins
        s = builtins.sum((r["weight"] or 0.0) for r in result)
        if s > 0:
            for r in result:
                r["weight"] = builtins.round(r["weight"] / s, 6)

    # Cắt còn đúng top_k (phòng LLM trả thừa)
    return result[:top_k]

# ---- 5) Output ----

top5 = select_top_attributes(api_host="gwrdinternal.tgdd.vn", attributes = lst_propname, category=category_chosen, top_k=1, model="MWG")
df_top5 = pd.DataFrame(top5)
print(df_top5)
df_cate = fil_cate.select('PROPERTYNAME', 'PROPERTYID').drop_duplicates().toPandas()
df_cate = df_cate.merge(df_top5, how = 'inner', left_on = 'PROPERTYNAME', right_on='attribute')
lst_prop_top5 = df_cate['PROPERTYID'].astype(int).to_list()




fil_cate_prop = fil_cate[fil_cate['PROPERTYID'].isin(lst_prop_top5)]

fil_cate_prop_pd = fil_cate_prop.toPandas()

fil_cate_prop_pd = fil_cate_prop_pd[['PRODUCTCODE', 'PRODUCTNAME', 'PROPERTYID', 'PROPERTYNAME', 'PROPVALUE', 'PROPVALUEID', 'CATEGORYID', 'CATEGORYNAME', 'MANUFACTURERID', 'MANUFACTURERNAME']]
fil_cate_prop_pd = fil_cate_prop_pd.drop_duplicates()

# fil_cate_prop_pd['PRODUCTCODE'] = fil_cate_prop_pd['PRODUCTCODE'].astype(int)
# df_bot = fil_cate_prop_pd[fil_cate_prop_pd['PRODUCTCODE'] >=  3000000000000]

import pandas as pd
import numpy as np


def build_product_profiles(df_raw: pd.DataFrame,
                           max_props_per_product: int = 20,
                           keep_order: bool = False) -> pd.DataFrame:
    """Gộp thuộc tính thành text_for_llm cho mỗi PRODUCTCODE; rút gọn để tránh tràn token."""
    df = df_raw.copy()

    # Chuẩn hoá chuỗi
    for c in ["PRODUCTNAME", "PROPERTYNAME", "PROPVALUE", "CATEGORYNAME", "MANUFACTURERNAME"]:
        df[c] = df[c].astype(str).str.strip()

    # Loại trùng thuộc tính lặp (nếu có)
    if not keep_order:
        df = df.drop_duplicates(subset=["PRODUCTCODE", "PROPERTYNAME", "PROPVALUE"])

    # Lấy top N thuộc tính đầu (giảm token)
    df["_row"] = df.groupby("PRODUCTCODE").cumcount()
    df = df[df["_row"] < max_props_per_product]

    # Gộp "Thuộc tính: Giá trị" thành chuỗi ngắn
    df["pair"] = df["PROPERTYNAME"] + ": " + df["PROPVALUE"]
    agg = (df.groupby(["PRODUCTCODE", "PRODUCTNAME", "CATEGORYID", "CATEGORYNAME",
                       "MANUFACTURERID", "MANUFACTURERNAME"])
             .agg(pairs=("pair", lambda s: "; ".join(s)))
             .reset_index())

    # Văn bản cho LLM (đính kèm mã để map ngược): NAME@@CODE | props | category | brand
    agg["text_for_llm"] = (
        agg["PRODUCTNAME"] + "@@" + agg["PRODUCTCODE"].astype(str) +
        " | " + agg["pairs"] +
        " | Category: " + agg["CATEGORYNAME"] +
        " | Brand: " + agg["MANUFACTURERNAME"]
    )
    return agg


import http.client, json, re
from typing import List, Tuple, Optional

def make_similarity_prompt(target_text: str,
                           candidate_texts: List[str],
                           top_n: int = 10) -> str:
    """
    target_text: 'PRODUCTNAME@@PRODUCTCODE | ...props... | Category: ... | Brand: ...'
    candidate_texts: list cùng format như trên
    """
    guide = f"""Nhiệm vụ:
- Dựa vào SẢN PHẨM MỤC TIÊU và DANH SÁCH ỨNG VIÊN, hãy chọn ra {top_n} sản phẩm giống nhất dựa trên thuộc tính (PROPERTYNAME/PROPVALUE), hạn chế các thuộc tính có giá trị Đang cập nhật.
- Không chọn chính sản phẩm mục tiêu.
- Chỉ trả về mảng JSON, mỗi phần tử là object {{"productcode": "...", "productname": "..."}}.
- KHÔNG thêm giải thích, không thêm text ngoài JSON.

SẢN PHẨM MỤC TIÊU:
{target_text}

DANH SÁCH ỨNG VIÊN:
{chr(10).join(candidate_texts)}
"""
    return guide

def call_llm(api_host: str, content: str,
             model="MWG", max_tokens=768, temperature=0.0, top_p=0.95, top_k=50) -> str:
    """
    Gọi API LLM nội bộ, trả về message content.
    Bổ sung kiểm tra lỗi và fallback khi không có 'choices'.
    """
    conn = http.client.HTTPConnection(api_host)
    payload = json.dumps({
        "messages": [{"content": content, "role": "user"}],
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "top_p": top_p,
        "top_k": top_k
    })
    headers = {'accept': 'application/json', 'Content-Type': 'application/json'}

    try:
        conn.request("POST", "/llm/v1/chat/completions", payload, headers)
        res = conn.getresponse()
        raw = res.read().decode("utf-8")

        if res.status != 200:
            print(f"⚠️ LLM API trả lỗi HTTP {res.status}: {raw[:500]}")
            return "[]"

        data = json.loads(raw)
        if not isinstance(data, dict):
            print("⚠️ LLM API trả về không phải JSON hợp lệ.")
            return "[]"

        if "choices" not in data or len(data["choices"]) == 0:
            print(f"⚠️ Không có 'choices' trong phản hồi LLM: {data}")
            return "[]"

        message = data["choices"][0].get("message", {})
        return message.get("content", "[]")

    except Exception as e:
        print(f"❌ call_llm lỗi: {e}")
        return "[]"
    finally:
        conn.close()


def recommend_similar_for_one(product_code: str,
                              df_profiles: pd.DataFrame,
                              top_n: int = 10,
                              same_category_only: bool = True,
                              exclude_same_brand: bool = False,
                              api_host: str = "abc.vn") -> pd.DataFrame:
    """
    Trả về DataFrame:
      [productid, productname, alike_productcode, alike_productname,
       text_for_llm_main, text_for_llm_alike]
    """
    target = df_profiles[df_profiles["PRODUCTCODE"].astype(str) == str(product_code)]
    if target.empty:
        raise ValueError(f"Không tìm thấy PRODUCTCODE={product_code}")
    t = target.iloc[0]

    # Bộ ứng viên
    cand = df_profiles[df_profiles["PRODUCTCODE"].astype(str) != str(product_code)]
    if same_category_only:
        cand = cand[cand["CATEGORYID"] == t["CATEGORYID"]]
    if exclude_same_brand:
        cand = cand[cand["MANUFACTURERID"] != t["MANUFACTURERID"]]

    max_candidates = 1000
    cand_list = cand.head(max_candidates)["text_for_llm"].tolist()

    # Prompt + call API
    content = make_similarity_prompt(
        target_text=t["text_for_llm"],
        candidate_texts=cand_list,
        top_n=top_n
    )

    text = call_llm(api_host=api_host, content=content)

    import re, json
    m = re.search(r"\[.*\]", text, flags=re.DOTALL)
    text_json = m.group().strip() if m else "[]"

    try:
        parsed = json.loads(text_json)
        if isinstance(parsed, dict):
            parsed = [parsed]
    except Exception:
        parsed = []

    rows = []
    for item in parsed[:top_n]:
        code = str(item.get("productcode") or item.get("code") or "").strip()
        name = (item.get("productname") or item.get("name") or "").strip()
        if not code and not name:
            continue

        row = df_profiles[
            (df_profiles["PRODUCTCODE"].astype(str) == code)
            | (df_profiles["PRODUCTNAME"].str.lower() == name.lower())
        ]
        if not row.empty:
            r = row.iloc[0]
            rows.append({
                "productid": t["PRODUCTCODE"],
                "productname": t["PRODUCTNAME"],
                "alike_productcode": r["PRODUCTCODE"],
                "alike_productname": r["PRODUCTNAME"],
                "text_for_llm_main": t["text_for_llm"],
                "text_for_llm_alike": r["text_for_llm"]
            })

    return pd.DataFrame(rows).drop_duplicates(subset=["alike_productcode"]).head(top_n)


import time
import random

import os
import math
import time
import random
from datetime import datetime
from glob import glob

category_chosen_id = fil_cate.select('CATEGORYID').drop_duplicates().toPandas()
category_chosen_id = category_chosen_id['CATEGORYID'].iloc[0]
print(category_chosen_id)

def recommend_similar_batch(df_raw: pd.DataFrame,
                            target_product_codes: Optional[List[str]] = None,
                            top_n: int = 10,
                            same_category_only: bool = True,
                            exclude_same_brand: bool = False,
                            api_host: str = "abc.vn",
                            delay_sec: Tuple[float, float] = (0.5, 1.5),
                            batch_size: int = 100,
                            n_sample: Optional[int] = None,
                            interactive: bool = True,
                            cate_pro: Optional[pd.DataFrame] = None
                            ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Chạy batch với alert/log và lưu kết quả ra parquet  theo từng batch 100 sản phẩm.
    """
    # Chuẩn bị thư mục output
    to_day = datetime.now().strftime("%Y%m%d")
    hdfs_out_dir = f"hdfs://172.16.38.99:8020/tri/check/dmx/sptt/ai_goiy_cateid_{category_chosen_id}"



    df_profiles = build_product_profiles(df_raw)
    if target_product_codes is None:
        target_product_codes = df_profiles["PRODUCTCODE"].astype(str).tolist()

    total = len(target_product_codes)
    print(f"🔹 Tổng số sản phẩm khả dụng: {total}")

    # Lấy số lượng mẫu (tùy chọn)
    if interactive and n_sample is None:
        try:
            n_sample = int(input(f"👉 Nhập số lượng sản phẩm bạn muốn gọi API (1 - {total}): ").strip())
            if n_sample > total or n_sample <= 0:
                print("⚠️ Số lượng không hợp lệ → chạy toàn bộ.")
                n_sample = total
        except:
            print("⚠️ Không nhập được → chạy toàn bộ.")
            n_sample = total

    if n_sample is not None:
        target_product_codes = target_product_codes[:n_sample]
        print(f"🔸 Sẽ gọi API cho {len(target_product_codes)} / {total} sản phẩm.\n")

    total_batches = math.ceil(len(target_product_codes) / batch_size)
    print(f"🚀 Bắt đầu xử lý {total_batches} batch (mỗi batch {batch_size} sản phẩm)...\n")

    all_results = []
    all_errors = []

    for batch_no in range( total_batches):
        start_idx = batch_no * batch_size
        
        import builtins
        end_idx = builtins.min((batch_no + 1) * batch_size, len(target_product_codes))
        batch_codes = target_product_codes[start_idx:end_idx]

        print(f"\n📦 Đang xử lý batch {batch_no+1}/{total_batches} "
              f"({len(batch_codes)} sản phẩm gốc, từ index {start_idx} → {end_idx-1})")

        results, errors = [], []

        for idx, code in enumerate(batch_codes, start=1):
            try:
                recs = recommend_similar_for_one(
                    product_code=code,
                    df_profiles=df_profiles,
                    top_n=top_n,
                    same_category_only=same_category_only,
                    exclude_same_brand=exclude_same_brand,
                    api_host=api_host
                )
                results.append(recs)

                pname = df_profiles.loc[df_profiles["PRODUCTCODE"].astype(str)==str(code), "PRODUCTNAME"].values[0]
                print(f"✔ ({idx}/{len(batch_codes)}) {pname[:50]}... ({len(recs)} gợi ý)")

            except Exception as e:
                pname = df_profiles.loc[df_profiles["PRODUCTCODE"].astype(str)==str(code), "PRODUCTNAME"].values[0] \
                        if not df_profiles.empty else ""
                err_msg = str(e)
                print(f"❌ ({idx}/{len(batch_codes)}) {pname[:50]}... → {err_msg}")
                errors.append({
                    "productcode": code,
                    "productname": pname,
                    "error": err_msg
                })

            time.sleep(random.uniform(*delay_sec))

        # Gộp và lưu từng batch
        df_batch = pd.concat(results, ignore_index=True) if results else pd.DataFrame(
            columns=[
                "productid", "productname",
                "alike_productcode", "alike_productname",
                "text_for_llm_main", "text_for_llm_alike"
            ]
        )
        
        df_err_batch = pd.DataFrame(errors, columns=["productcode", "productname", "error"])


        # ===== Map categoryid cho productid gốc từ cate_pro =====
        if cate_pro is not None and not df_batch.empty:
            df_batch["productid"] = df_batch["productid"].astype(str).str.strip()
            cate_pro["productid"] = cate_pro["productid"].astype(str).str.strip()
            
            cate_map = cate_pro[["productid", "categoryid"]].drop_duplicates()
            df_batch = df_batch.merge(cate_map, on="productid", how="left")

            matched = df_batch["categoryid"].notna().sum()
            unmatched = len(df_batch) - matched
            print(f"🔎 Map categoryid: matched={matched} | unmatched={unmatched}")

            preferred_cols = ["productid", "productname", "categoryid",
                            "alike_productcode", "alike_productname",
                            "text_for_llm_main", "text_for_llm_alike"]
            df_batch = df_batch[[c for c in preferred_cols if c in df_batch.columns]]

            # Thống kê match
            matched = df_batch["categoryid"].notna().sum()
            unmatched = len(df_batch) - matched
            print(f"🔎 Map categoryid: matched={matched} | unmatched={unmatched}")

            # (tuỳ chọn) Sắp cột cho dễ đọc
            preferred_cols = ["productid", "productname", "categoryid",
                            "alike_productcode", "alike_productname",
                            "text_for_llm_main", "text_for_llm_alike"]
            df_batch = df_batch[[c for c in preferred_cols if c in df_batch.columns]]


        num_rows_matched = len(df_batch)
        all_results.append(df_batch)
        all_errors.append(df_err_batch)

        
        batch_path = f"{hdfs_out_dir}/batch={batch_no:03d}_rows={num_rows_matched}"
        spark_df = spark.createDataFrame(df_batch)
        spark_df.write.format("parquet").mode("overwrite").save(f"{batch_path}.parquet")
        
        print(f"✅ Hoàn thành batch {batch_no+1}/{total_batches}"
              f"| Thành công: {len(results)} | Lỗi: {len(errors)}")

    # Hợp nhất tất cả kết quả
    df_result = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
    df_error = pd.concat(all_errors, ignore_index=True) if all_errors else pd.DataFrame()

    print("\n🎯 Hoàn tất toàn bộ quá trình!")
    print(f"   Tổng sản phẩm xử lý: {len(target_product_codes)}")
    print(f"   Thành công: {len(df_result['productid'].unique())} sản phẩm | Lỗi: {len(df_error)}")
    print(f"   File lưu trong thư mục: {hdfs_out_dir}")

    return df_result, df_error


df_ketqua = recommend_similar_batch(fil_cate_prop_pd, top_n= 10, api_host="gwrdinternal.tgdd.vn", batch_size=100, interactive =False, cate_pro=cate_pro)


#  recommend_similar_batch(df_raw: pd.DataFrame,
#                             target_product_codes: Optional[List[str]] = None,
#                             top_n: int = 10,
#                             same_category_only: bool = True,
#                             exclude_same_brand: bool = False,
#                             api_host: str = "abc.vn",
#                             delay_sec: Tuple[float, float] = (0.5, 1.5),
#                             batch_size: int = 100,
#                             n_sample: Optional[int] = None,
#                             interactive: bool = True,
#                             cate_pro: Optional[pd.DataFrame] = None
#                             ) -> Tuple[pd.DataFrame, pd.DataFrame]:
