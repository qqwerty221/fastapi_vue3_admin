import xml.etree.ElementTree as ET
import psycopg2
import json

# 数据库连接配置
conn = psycopg2.connect(
    dbname="controlm",
    user="fastapi",
    password="fastapi-root",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

def insert_object(parent_id, element, hierarchy_level, position_in_parent):
    cursor.execute(
        """
        INSERT INTO parser.controlm_objects (parent_id, object_type, name, attributes, content, hierarchy_level, position_in_parent)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            parent_id,
            element.tag,
            element.attrib.get('NAME', ''),
            json.dumps(element.attrib) if element.attrib else None,  # 将字典转换为 JSON 字符串
            ET.tostring(element, encoding='unicode'),  # 显式指定编码
            hierarchy_level,
            position_in_parent
        )
    )
    object_id = cursor.fetchone()[0]
    conn.commit()

    # 插入子对象nbh
    for i, child in enumerate(element):
        insert_object(object_id, child, hierarchy_level + 1, i)

    # 插入属性
    for i, (attr_name, attr_value) in enumerate(element.items()):
        cursor.execute(
            """
            INSERT INTO parser.controlm_attributes (object_id, attribute_name, attribute_value, position_in_object)
            VALUES (%s, %s, %s, %s);
            """,
            (
                object_id,
                attr_name,
                attr_value,
                i
            )
        )
    conn.commit()

    return object_id

# 解析 XML 文件
tree = ET.parse('../asset/Workspace_256.txt')
root = tree.getroot()

# 插入根对象
insert_object(None, root, 0, 0)

# 关闭数据库连接
conn.close()