import xmltodict
import json


output_file = 'output.json'

with open("aaa", "r", encoding="utf-8") as f:
    xml_data = f.read()

# 解析为字典
dict_data = xmltodict.parse(xml_data)
# 转为 JSON
json_data = json.dumps(dict_data, indent=2, ensure_ascii=False)
# print(json_data)


# 将处理后的XML写入文件
with open("output.json", "w", encoding="utf-8") as f:
    f.write(json_data)