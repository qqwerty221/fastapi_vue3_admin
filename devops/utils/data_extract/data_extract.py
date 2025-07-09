import json
import pandas as pd

# 读取 JSON 文件
with open('input.json', 'r', encoding='utf-8') as file:
    data = json.load(file)  # 读取 JSON 并解析为 Python 字典

# 提取 columns 数据
columns_data = data["data"][0]["columns"]

# 转换为 DataFrame
df = pd.DataFrame(columns_data[1:], columns=columns_data[0])

# 保存到 Excel
df.to_excel("output.xlsx", index=False)

print("Excel 文件已成功保存！")

