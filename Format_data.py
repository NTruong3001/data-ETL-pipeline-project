import pandas as pd
import json

# Đọc file CSV
df = pd.read_csv('tracking.csv')

# Hàm để xử lý cột 'ed', đảm bảo nó là một chuỗi JSON hợp lệ
def fix_json_format(value):
    try:
        # Kiểm tra nếu giá trị trong cột 'ed' là chuỗi hợp lệ
        if isinstance(value, str):
            # Thử giải mã chuỗi JSON nếu có
            return json.loads(value)  # Chuyển đổi thành JSON
        return value  # Nếu không phải chuỗi JSON, giữ nguyên giá trị
    except json.JSONDecodeError:
        return value  # Nếu gặp lỗi, trả lại giá trị gốc (có thể là NULL hoặc lỗi)

# Áp dụng hàm cho cột 'ed'
df['ed'] = df['ed'].apply(fix_json_format)

# Lưu lại file CSV mới
df.to_csv('updated_tracking.csv', index=False)

print("Đã xử lý thành công và lưu lại file mới.")
