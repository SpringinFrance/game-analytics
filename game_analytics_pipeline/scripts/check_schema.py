from google.cloud import bigquery

def check_schema_mismatch(project_id, dataset_id, table_name, python_columns):
    client = bigquery.Client(project="game-analytics-22")
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    # 1. Lấy schema thực tế của bảng đích trên BigQuery
    try:
        table = client.get_table(table_id)
        bq_columns = {field.name for field in table.schema}
    except Exception as e:
        print(f"Không lấy được bảng {table_id}. Lỗi: {e}")
        return

    # 2. Chuyển list cột trong code Python thành Set
    python_cols_set = set(python_columns)

    # 3. Tìm ra sự khác biệt
    missing_in_bq = python_cols_set - bq_columns  # Có trong code, thiếu trên BQ
    extra_in_bq = bq_columns - python_cols_set    # Có trên BQ, không có trong code

    # 4. Show kết quả
    print("-" * 50) 
    print(f"BÁO CÁO LỆCH SCHEMA CHO BẢNG: {table_id}")
    print("-" * 50)
    
    if not missing_in_bq and not extra_in_bq:
        print("✅ Ngon lành! Schema hai bên hoàn toàn khớp nhau.")
    else:
        if missing_in_bq:
            print(f"❌ Các cột có trong Python nhưng THIẾU trên BigQuery:\n   {missing_in_bq}\n")
        if extra_in_bq:
            print(f"⚠️ Các cột có trên BigQuery nhưng KHÔNG CÓ trong Python:\n   {extra_in_bq}\n")

# --- CÁCH SỬ DỤNG ---
# Giả sử đây là danh sách các cột bạn đang map/query trong code Python
columns_in_code = [
    "event_date", "event_name", "platform", "geo_country", 
    "user_pseudo_id", "is_active_user" # Cột đang gây lỗi
]

# Chạy thử (Nhớ thay bằng tên project/dataset/table thật của bạn)
check_schema_mismatch(
    project_id="game-analytics-22",
    dataset_id="game_raw", 
    table_name="raw_events",
    python_columns=columns_in_code
)