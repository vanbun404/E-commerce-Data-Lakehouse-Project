# E-commerce Data Lakehouse

## 1. Clone project

Mở Terminal và chạy:

```bash
git clone https://github.com/camlee05/E-commerce-Data-Lakehouse.git
```

Di chuyển vào thư mục project:

```bash
cd E-commerce-Data-Lakehouse
```

Kiểm tra project:

```bash
git status
```

---

## 2. Tạo Python Virtual Environment

### macOS / Linux

```bash
python3 -m venv .venv
```

### Windows

```bash
py -m venv .venv
```

---

## 3. Kích hoạt Virtual Environment

### macOS / Linux

```bash
source .venv/bin/activate
```

### Windows CMD

```bash
.venv\Scripts\activate
```

### Windows PowerShell

```powershell
.venv\Scripts\Activate.ps1
```

Sau khi kích hoạt thành công, Terminal sẽ có dạng:

```text
(.venv) ...
```

---

## 4. Kiểm tra Python

```bash
python --version
```

Kiểm tra Python đang sử dụng:

### macOS / Linux

```bash
which python
```

### Windows

```bash
where python
```

Đường dẫn phải trỏ đến `.venv`.

---

## 5. Cài đặt thư viện

Đảm bảo Virtual Environment đang được kích hoạt, sau đó chạy:

```bash
pip install -r requirements.txt
```

Kiểm tra:

```bash
pip list
```

---

## 6. Cài Jupyter Kernel

Chạy:

```bash
python -m ipykernel install --user \
  --name ecommerce-lakehouse \
  --display-name "Python (.venv) - E-commerce Lakehouse"
```

---

## 7. Mở project bằng VS Code

Nếu đã cài VS Code:

```bash
code .
```

Nếu lệnh `code` chưa được nhận, mở VS Code → **File → Open Folder** → chọn thư mục:

```text
E-commerce-Data-Lakehouse
```

---

## 8. Chọn Python Kernel

Mở file:

```text
notebooks/01_data_collection.ipynb
```

Trong VS Code, chọn:

```text
Python (.venv) - E-commerce Lakehouse
```

Lưu ý: tất cả notebook của project nên sử dụng kernel này.

---

## 9. Cài Docker Desktop

Project sử dụng Docker để chạy MinIO.

Kiểm tra Docker:

```bash
docker --version
```

Sau đó:

```bash
docker ps
```

Nếu Docker đang hoạt động, có thể tiếp tục chạy MinIO.

---

## 10. Chạy MinIO

```bash
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -v minio_data:/data \
  quay.io/minio/minio \
  server /data --console-address ":9001"
```

Kiểm tra:

```bash
docker ps
```

MinIO Console:

```text
http://localhost:9001
```

Thông tin đăng nhập của project:

```text
Username: minioadmin
Password: minioadmin
```

---

## 11. Dataset

Dataset không được lưu trên GitHub.

Sau khi nhận dataset từ nhóm, đặt các file vào:

```text
data/source/
```

Cấu trúc:

```text
data/
└── source/
    ├── olist_customers_dataset.csv
    ├── olist_products_dataset.csv
    ├── olist_orders_dataset.csv
    ├── olist_order_items_dataset.csv
    ├── olist_order_reviews_dataset.csv
    └── olist_order_payments_dataset.csv
```

---

## 12. Chạy các notebook

Chạy theo đúng thứ tự:

```text
01_data_collection.ipynb
        ↓
02_data_ingestion.ipynb
        ↓
03_object_storage_minio.ipynb
        ↓
04_bronze_layer.ipynb
        ↓
05_silver_layer.ipynb
        ↓
06_gold_layer.ipynb
```

---

## 13. Git workflow

Không làm việc trực tiếp trên `main`.

Tạo branch riêng:

```bash
git checkout -b feature/ten-cong-viec
```

Ví dụ:

```bash
git checkout -b feature/bronze-layer
```

Sau khi hoàn thành:

```bash
git add .
git commit -m "feat: add bronze layer"
git push -u origin feature/bronze-layer
```

Sau đó tạo Pull Request để merge vào `main`.

---

## 14. Không commit các file sau

```text
.venv/
data/source/
data/raw/
.ipynb_checkpoints/
.DS_Store
```

Các file này đã được cấu hình trong `.gitignore`.
