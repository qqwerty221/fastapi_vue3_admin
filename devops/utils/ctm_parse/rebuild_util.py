from xml.etree.ElementTree import Element, SubElement, tostring
import psycopg2
import json
import logging

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(message)s')

# 数据库连接配置
conn = psycopg2.connect(
    dbname="controlm",
    user="fastapi",
    password="fastapi-root",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

def get_all_objects():
    cursor.execute(
        """
        SELECT * FROM parser.controlm_objects;
        """
    )
    return cursor.fetchall()

def get_all_attributes():
    cursor.execute(
        """
        SELECT * FROM parser.controlm_attributes;
        """
    )
    return cursor.fetchall()

objects = get_all_objects()
attributes = get_all_attributes()

# 检查 objects 列表中的对象
logging.info(f"objects 列表中的对象数量: {len(objects)}")
for obj in objects:
    logging.info(f"对象: {obj}")

def reconstruct_xml(object_id, objects, attributes, parent_path=""):
    logging.info(f"正在重建对象 ID 为 {object_id} 的 XML 元素")

    obj = next(obj for obj in objects if obj[0] == object_id)
    parent_path += f"/{obj[2]}" if parent_path else obj[2]

    # 解析 attributes 字段为字典（处理空值或无效数据）
    try:
        element_attributes = json.loads(obj[3]) if obj[3] else {}
        logging.info(f"对象属性: {element_attributes}")
    except json.JSONDecodeError:
        element_attributes = {}
        logging.warning(f"对象 ID 为 {object_id} 的属性解析失败")

    element = Element(obj[2])
    element.attrib = element_attributes

    # 设置属性
    for attr in attributes:
        if attr[1] == object_id:
            logging.info(f"设置属性 {attr[2]}: {attr[3]}")
            element.set(attr[2], attr[3])

    # 插入子对象
    for child in objects:
        if child[5] == object_id:
            element.append(reconstruct_xml(child[0], objects, attributes, parent_path))

    return element

# 重建 XML 文件
try:
    root = next(obj for obj in objects if obj[5] is None)
    root_element = reconstruct_xml(root[0], objects, attributes)

    with open('reconstructed_Workspace_256.txt', 'wb') as f:
        f.write(tostring(root_element, encoding='utf-8', xml_declaration=True))
        logging.info("XML 文件已成功写入 'reconstructed_Workspace_256.txt'")
except StopIteration:
    logging.error("未找到根对象。请检查数据库中的数据。")

conn.close()
logging.info("数据库连接已关闭")
