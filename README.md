# RCM All Site Modules

Hệ thống xử lý dữ liệu và gợi ý sản phẩm (Recommendation) cho các nền tảng thương mại điện tử. Sử dụng Apache Spark và LLM AI để phân tích hành vi khách hàng và tạo gợi ý sản phẩm cá nhân hóa.

---

## 📋 Tổng Quan Dự Án

**RCM All Site Modules** là một bộ công cụ Python chuyên biệt để:
- **Phân tích tần suất mua hàng** của khách hàng theo danh mục sản phẩm
- **Gợi ý sản phẩm thay thế** dựa trên thuộc tính sản phẩm và AI LLM
- **Xử lý dữ liệu theo chu kỳ** cho sản phẩm định kỳ (sữa, tã, quần áo trẻ em)
- **Đề xuất sản phẩm theo độ tuổi** cho trẻ em sử dụng LLM
- **Ranking và đa dạng hóa** danh sách gợi ý sản phẩm

Dự án được thiết kế để hoạt động trên các nền tảng thương mại điện tử như **TGDD (Thế Giới Di Động)** và **AvaKids**, xử lý hàng triệu bản ghi dữ liệu khách hàng.

---

## ✨ Tính Năng Chính

### 1. **Phân Tích Tần Suất Sản Phẩm** (`rcm_tan_suat_san_tmdt.py`)
- Tính tần suất mua hàng theo danh mục cho mỗi khách hàng
- Xác định sản phẩm được mua nhiều nhất trong từng danh mục
- Mapping mã sản phẩm với mã model
- Output: Dữ liệu tần suất sản phẩm cho từng khách hàng

### 2. **Gợi Ý Sản Phẩm Thay Thế** (`sptt_AI_dmx.py`)
- Sử dụng LLM AI để xác định thuộc tính quan trọng nhất của sản phẩm
- Gợi ý sản phẩm tương tự dựa trên thuộc tính và thương hiệu
- Xử lý batch tự động cho hàng trăm sản phẩm
- Lưu kết quả theo batch parquet files

### 3. **Xử Lý Sản Phẩm Mua Cùng** (`sp_muacung_dmx.py`)
- Tìm sản phẩm thường được mua cùng nhau
- Ranking theo điểm số kết hợp
- Ánh xạ model code cho sản phẩm

### 4. **Gợi Ý Sản Phẩm Theo Độ Tuổi** (`rcm_theo_tuoi_ai_avakids.py`)
- Dự đoán độ tuổi của trẻ em dựa trên lịch sử mua hàng
- Khớp với dữ liệu chiều cao - cân nặng tiêu chuẩn
- Sử dụng LLM AI để gợi ý sản phẩm phù hợp theo độ tuổi
- Hỗ trợ 15 sản phẩm gợi ý đa dạng theo danh mục

### 5. **Upsize Chu Kỳ Sữa/Tã** (`upsize_chuky_sua_ta_product_avakids.py`)
- Phân tích chu kỳ mua sữa/tã của trẻ em
- Gợi ý sản phẩm size lớn hơn khi trẻ lớn lên (upsize)
- Kết hợp gợi ý từ nhiều nguồn (chu kỳ, độ tuổi, tần suất)
- Giới hạn tối đa 30 sản phẩm gợi ý cho mỗi khách hàng

---

## 🛠️ Công Nghệ Sử Dụng

| Công Nghệ | Mục Đích |
|-----------|---------|
| **Apache Spark** | Xử lý dữ liệu lớn (Big Data) với PySpark |
| **Python 3** | Ngôn ngữ chính cho các script xử lý |
| **Pandas/NumPy** | Thao tác và phân tích dữ liệu |
| **HDFS** | Lưu trữ dữ liệu phân tán |
| **LLM AI** | API LLM nội bộ cho gợi ý thông minh |
| **Parquet** | Định dạng lưu trữ dữ liệu tối ưu |

---

## 📊 Luồng Xử Lý Dữ Liệu

```
Dữ liệu khách hàng
       ↓
┌──────────────────────────────────────────────────────┐
│   Phân Tích Tần Suất (rcm_tan_suat_san_tmdt.py)     │
│   → Tính tần suất sản phẩm theo danh mục             │
└──────────────────────────────────────────────────────┘
       ↓
┌──────────────────────────────────────────────────────────────────┐
│          Gợi Ý Sản Phẩm (3 nguồn song song)                     │
├──────────────────┬────────────────────┬──────────────────────────┤
│ Sản phẩm thay thế│ Theo độ tuổi (AI)  │ Upsize Sữa/Tã (chu kỳ) │
│(sptt_AI_dmx.py)  │(rcm_theo_tuoi...) │(upsize_chuky_sua_ta..) │
└──────────────────┴────────────────────┴──────────────────────────┘
       ↓
┌──────────────────────────────────────────────────────┐
│   Kết Hợp & Ranking 30 sản phẩm hàng đầu             │
│   → Loại bỏ trùng lặp                                │
│   → Đa dạng theo danh mục                            │
│   → Sắp xếp theo độ liên quan                        │
└──────────────────────────────────────────────────────┘
       ↓
Output: Danh sách gợi ý sản phẩm cho mỗi khách hàng
```

---

## 🚀 Cách Sử Dụng

### Điều Kiện Tiên Quyết
```bash
pip install pyspark pandas numpy pytz hdfs3 python-dateutil
```

### Chạy Các Module

#### 1. Phân Tích Tần Suất Sản Phẩm
```bash
python rcm_tan_suat_san_tmdt.py
```
Output: `customer_product_frequency.parquet`

#### 2. Gợi Ý Sản Phẩm Theo Độ Tuổi (AvaKids)
```bash
python rcm_theo_tuoi_ai_avakids.py
```
Output: `rcm_theo_tuoi_ai_v1/batch_*.parquet`

#### 3. Gợi Ý Sản Phẩm Thay Thế (DMX)
```bash
python sptt_AI_dmx.py
```
Output: `sppt/ai_goiy_cateid_*/batch_*.parquet`

#### 4. Upsize Chu Kỳ Sữa/Tã
```bash
python upsize_chuky_sua_ta_product_avakids.py
```
Output: `productid_chuky_upsize_v1.parquet`

---

## 📈 Kết Quả Dữ Liệu

Các module tạo ra các output chính:

| File Output | Cấu Trúc Dữ Liệu | Số Dòng |
|------------|------------------|---------|
| `customer_product_frequency.parquet` | customer_id, categoryid, productid, frequency, model_code | Tùy dữ liệu đầu vào |
| `rcm_theo_tuoi_v1/batch_*.parquet` | ma_kh, productid, productname, reason | Tới 15 sản phẩm/KH |
| `sppt/ai_goiy_*.parquet` | productid, productname, categoryid, alike_productcode | Tới 10 sản phẩm/nhóm |
| `productid_chuky_upsize_v1.parquet` | customer_id, productid, rank, type, formatted_date | Tới 30 sản phẩm/KH |



## 📝 Tham Số Chính

| Module | Tham Số | Giá Trị Mặc Định | Mô Tả |
|--------|---------|-----------------|-------|
| rcm_theo_tuoi | BATCH_SIZE | 500 | Số khách hàng/batch |
| rcm_theo_tuoi | FINAL_K | 15 | Số sản phẩm gợi ý cuối |
| sptt_AI_dmx | BATCH_SIZE | 100 | Số sản phẩm/batch |
| sptt_AI_dmx | TOP_N | 10 | Số sản phẩm thay thế gợi ý |
| upsize_chuky | MAX_PRODUCTS | 30 | Số sản phẩm tối đa/KH |

---

## 🎯 Cải Tiến Trong Tương Lai

- [ ] Hỗ trợ thêm các nền tảng thương mại điện tử khác
- [ ] Tối ưu hóa tốc độ xử lý batch
- [ ] Thêm các metrics đánh giá chất lượng gợi ý
- [ ] Phát triển dashboard theo dõi kết quả
- [ ] Tích hợp A/B testing cho các chiến lược gợi ý
- [ ] Hỗ trợ thêm ngôn ngữ và thị trường
- [ ] Cải thiện độ chính xác dự đoán độ tuổi
- [ ] Xây dựng model machine learning thay thế LLM

---


## 📚 Tài Liệu Tham Khảo

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Pandas User Guide](https://pandas.pydata.org/docs/)
- [HDFS Architecture](https://hadoop.apache.org/docs/r3.2.1/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)

---

**Last Updated**: 2026-06-30
**Repository**: [viettu98/rcm_all_site_moduls](https://github.com/viettu98/rcm_all_site_moduls)
