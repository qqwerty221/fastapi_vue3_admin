from pathlib import Path

from sqlalchemy import create_engine, Column, Integer, String, JSON, ForeignKey, text, select, ARRAY, or_
from sqlalchemy.orm import relationship, scoped_session, declarative_base, joinedload
from sqlalchemy.orm.session import sessionmaker
from lxml import etree as ET
import time
import os

DATABASE_URI = 'postgresql://fastapi:fastapi-root@localhost/fastapi'
engine = create_engine(DATABASE_URI)
Session = scoped_session(sessionmaker(bind=engine))
session = Session()

# 设置默认 schema 为 parser
session.execute(text("SET search_path TO parser"))

Base = declarative_base()

class ControlMObject(Base):
    __tablename__ = 'controlm_objects'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('controlm_objects.id'))
    object_type = Column(String, nullable=False)
    name = Column(String, nullable=False)
    content = Column(String)
    hierarchy_level = Column(Integer)
    position_in_parent = Column(Integer)
    children = relationship("ControlMObject", back_populates="parent", cascade="all, delete-orphan")
    parent = relationship("ControlMObject", back_populates="children", remote_side=[id])
    attributes = relationship("ControlMAttribute", order_by="ControlMAttribute.position_in_object", back_populates="object")

class ControlMAttribute(Base):
    __tablename__ = 'controlm_attributes'
    id = Column(Integer, primary_key=True)
    object_id = Column(Integer, ForeignKey('controlm_objects.id'))
    attribute_name = Column(String, nullable=False)
    attribute_value = Column(String)
    position_in_object = Column(Integer)
    object = relationship("ControlMObject", back_populates="attributes")

class ControlMObjectWithAttrDepend(Base):
    __tablename__ = 'controlm_object_with_attr_depend'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer)
    object_type = Column(String, nullable=False)
    general_name = Column(String, nullable=False)
    hierarchy_level = Column(Integer, nullable=False)
    position_in_parent = Column(Integer, nullable=False)
    json_object_agg = Column(JSON, nullable=False)
    sub_application = Column(String, nullable=False)
    dependency_name = Column(String, nullable=False)
    from_object = Column(ARRAY(String), nullable=False)

def parse_xml_to_db(file_path, batch_size=10000):
    """
    从 XML 文件解析数据并批量插入数据库。

    :param file_path: XML 文件路径
    :param batch_size: 每批插入的行数，默认为 10000
    """
    tree = ET.parse(file_path)
    root = tree.getroot()
    objects_to_add = []
    attributes_to_add = []
    batch_count = 0

    def _parse_element(element, parent_id, hierarchy_level):
        nonlocal batch_count

        # 查询 nextval
        query = text("SELECT nextval('parser.seq_controlm_object_id') AS next_value")
        object_id = session.execute(query).scalar()

        obj = ControlMObject(
            id=object_id,
            parent_id=parent_id,
            object_type=element.tag,
            name=element.attrib.get('NAME', ''),
            content=element.text.strip() if element.text else None,
            hierarchy_level=hierarchy_level,
            position_in_parent=element.sourceline
        )

        objects_to_add.append(obj)

        for idx, (attr_name, attr_value) in enumerate(element.attrib.items()):
            attr = ControlMAttribute(
                object_id=obj.id,
                attribute_name=attr_name,
                attribute_value=attr_value,
                position_in_object=idx
            )
            attributes_to_add.append(attr)

        batch_count += 1
        if batch_count >= batch_size:
            # 批量插入数据
            session.bulk_insert_mappings(ControlMObject, [obj.__dict__ for obj in objects_to_add])
            session.bulk_insert_mappings(ControlMAttribute, [attr.__dict__ for attr in attributes_to_add])
            session.commit()
            objects_to_add.clear()
            attributes_to_add.clear()
            batch_count = 0
            print(10000)

        for child in element:
            _parse_element(child, parent_id=obj.id, hierarchy_level=hierarchy_level + 1)

    _parse_element(root, parent_id=None, hierarchy_level=0)

    # 插入剩余的数据
    if objects_to_add or attributes_to_add:
        session.bulk_insert_mappings(ControlMObject, [obj.__dict__ for obj in objects_to_add])
        session.bulk_insert_mappings(ControlMAttribute, [attr.__dict__ for attr in attributes_to_add])
        session.commit()

def generate_config_file(
        sub_application_filter: list = None,
        attributes_to_delete: list = None,
        attributes_to_update: dict = None,
        job_filter: str = None,
        output_file: str = "output_config.xml"
):
    """
    从数据库获取数据并生成配置文件。

    :param sub_application_filter: 按 SUB_APPLICATION 筛选 folder, smart_folder, sub_folder, job
    :param attributes_to_delete: 需要批量删除的属性名列表
    :param attributes_to_update: 需要批量更新的属性值字典，格式为 {attribute_name: new_value}
    :param output_file: 输出的 XML 文件路径
    """
    if attributes_to_delete is None:
        attributes_to_delete = []
    if attributes_to_update is None:
        attributes_to_update = {}

    # Building the query
    query = (
        select(ControlMObjectWithAttrDepend)
        .where(
            or_(
                ControlMObjectWithAttrDepend.object_type.in_(['DEFTABLE', 'WORKSPACE']),
                ControlMObjectWithAttrDepend.sub_application.in_(sub_application_filter) if sub_application_filter else True,
            )
        )
    )

    objects = session.execute(query)

    filter_id_list = []
    if job_filter:
        query_filter = text("SELECT id FROM parser.get_dependency_chain('" + job_filter + "','" + ",".join(sub_application_filter) + "') order by id;")
        filter_list = session.execute(query_filter)

        filter_id_list = [id[0] for id in filter_list]

    # 构建 XML 树
    root = ET.Element("ROOT")
    selected_ids = set()
    selected_count = 0

    def attr_value_check(value):
        result = value
        if len(value) >= 64:
            result = value[3:]
        return result

    def process_object(obj, parent_element):

        nonlocal selected_count
        if obj.id in selected_ids:
            return  # 避免重复处理

        if obj.parent_id and not obj.parent_id in selected_ids:
            return  # 有父级 且 父级不在清单内 的不处理

        # 解析元素属性
        attr_dict = obj.json_object_agg

        # if sub_application_filter and attr_dict.get('SUB_APPLICATION') != sub_application_filter:
        #     if obj.object_type in ['SMART_FOLDER', 'JOB', 'FOLDER', 'SUB_FOLDER']:
        #         return  # SMART_FOLDER,JOB 只提取筛选条件内的范围
        # 改在SQL实现

        if job_filter and obj.object_type in ['JOB' ] and obj.id not in filter_id_list:
            return # 启用job_filter后只过滤本身及下游作业

        if obj.object_type == 'JOB' and attr_dict.get('APPL_TYPE') == 'DAYUJOB':
            return # DAYUJOB都不要

        # 创建当前对象的 XML 元素
        element = ET.SubElement(parent_element, obj.object_type)

        # 处理属性
        for attr, value in attr_dict.items():
            if attr in attributes_to_delete:
                continue  # 跳过需要删除的属性

            if obj.object_type in ['JOB','SMART_FOLDER','SUB_FOLDER'] and attr == 'JOBNAME':
                value = attr_value_check(value + attributes_to_update)

            elif obj.object_type == 'FOLDER' and attr == 'FOLDER_NAME':
                value = attr_value_check(value + attributes_to_update)

            elif obj.object_type in ['INCOND','OUTCOND'] and attr == 'NAME':
                cond_list = [attr_value_check(cond + attributes_to_update) for cond in value.split('-TO-')]
                value = '-TO-'.join(cond_list)

            element.set(attr, value)

        # 记录选取的元素 ID 和数量
        selected_ids.add(obj.id)
        selected_count += 1

        query_for_child = query.filter(ControlMObjectWithAttrDepend.parent_id == obj.id)
        children = session.execute(query_for_child)

        # 递归处理子元素
        for child in children:
            process_object(child[0], element)


    def xml_element_process(parent_element, element, new_root_name):
        if element.tag in ['SUB_FOLDER', 'SMART_FOLDER', 'FOLDER'] and not 'JOB' in [child.tag for child in element ]:
            parent_element.remove(element)
            return

        if element.tag in ['JOB', 'SUB_FOLDER']:
            element.attrib['PARENT_FOLDER'] = new_root_name + '/' + '/'.join([path + attributes_to_update for path in element.attrib['PARENT_FOLDER'].split('/')])
            for attr in element.attrib:
                if attr in ['CYCLIC_TIMES_SEQUENCE', 'CYCLIC_TOLERANCE', 'CYCLIC_TYPE', 'IND_CYCLIC', 'CYCLIC']:
                    del element.attrib[attr]

        if element.tag in ['SMART_FOLDER', 'FOLDER']:
            element.tag = 'SUB_FOLDER'
            element.attrib['PARENT_FOLDER'] = attr_value_check(new_root_name)

            # element.set('JOBNAME', element.attrib['FOLDER_NAME'])
            element.set('CREATED_BY', 'inport')

            for attr in element.attrib:
                if attr not in ['JOBISN','APPLICATION','SUB_APPLICATION','JOBNAME','CREATED_BY','RUN_AS','CRITICAL'
                    ,'TASKTYPE','CYCLIC','INTERVAL','CONFIRM','RETRO','MAXWAIT','MAXRERUN','AUTOARCH'
                    ,'MAXDAYS','MAXRUNS','DAYS','JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP'
                    ,'OCT','NOV','DEC','DAYS_AND_OR','PARENT_FOLDER']:
                    del element.attrib[attr]

        for child in element:
                xml_element_process(element, child, new_root_name)

    # 从根节点开始处理
    for obj in objects:
        if not obj[0].parent_id:  # 根节点
            process_object(obj[0], root)

    # 将结果写入 XML 文件
    tree = ET.ElementTree(root)

    # init
    parent_folder = tree.find('DEFTABLE')
    current_unix_time = '_' + str(int(time.time()))

    for sub_application in sub_application_filter:
        one_time_folder = ET.Element("SMART_FOLDER")
        one_time_folder.set('APPLICATION', 'CMSK')
        one_time_folder.set('SUB_APPLICATION', sub_application)
        one_time_folder.set('RUN_AS', sub_application)
        one_time_folder.set('PARENT_FOLDER', sub_application + current_unix_time)
        one_time_folder.set('JOBNAME', sub_application + current_unix_time)
        one_time_folder.set('FOLDER_NAME', sub_application + current_unix_time)
        one_time_folder.set('VERSION_HOST', '100.65.250.72, 100.73.29.67, 100.73.56.60')
        one_time_folder.set('IS_CURRENT_VERSION', 'Y')
        one_time_folder.set('DATACENTER', 'controlm')
        one_time_folder.set('VERSION_OPCODE', 'N')
        one_time_folder.set('VERSION', '919')
        one_time_folder.set('FOLDER_ORDER_METHOD', 'SYSTEM')
        one_time_folder.set('ENFORCE_VALIDATION', 'N')
        one_time_folder.set('RULE_BASED_CALENDAR_RELATIONSHIP', 'O')
        one_time_folder.set('FOLDER_NAME', sub_application + current_unix_time)
        one_time_folder.set('CREATED_BY', 'inport')

        for folder_like_element in parent_folder:
            if folder_like_element.attrib.get('SUB_APPLICATION') and folder_like_element.attrib['SUB_APPLICATION'] == sub_application:
                one_time_folder.append(folder_like_element)

        parent_folder.append(one_time_folder)

        RULE_BASED_CALENDAR = ET.SubElement(one_time_folder, 'RULE_BASED_CALENDAR')
        RULE_BASED_CALENDAR.set('NAME','none')
        RULE_BASED_CALENDAR.set('MAXWAIT','0')

        for attr in ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']:
            RULE_BASED_CALENDAR.set(attr, '1')

        for element in one_time_folder:
            xml_element_process(one_time_folder, element, one_time_folder.attrib['JOBNAME'])

    with open(output_file, "wb") as f:
        f.write(ET.tostring(parent_folder, pretty_print=True, xml_declaration=True, encoding="utf-8"))

    # 验证是否有遗漏
    all_object_ids = {obj.id for obj in objects}
    missing_ids = all_object_ids - selected_ids
    if missing_ids:
        print(f"Warning: Some objects were not processed. Missing IDs: {missing_ids}")

    # 打印选取的元素 ID 和数量
    print(f"Selected {selected_count} elements with IDs: {selected_ids}")

if __name__ == '__main__':

    def parse_init():
        start_time = time.time()
        file_path = Path(__file__).parent / "asset" / "Workspace_271.xml"
        parse_xml_to_db(file_path=file_path, batch_size=10000)
        end_time = time.time()
        print(f"处理时间: {end_time - start_time} 秒")

    def custom_generate():
        # 生成配置文件
        start_time = time.time()
        generate_config_file(
            sub_application_filter=[],
            # attributes_to_delete=["ATTRIBUTE1", "ATTRIBUTE2"],
            attributes_to_update='_G',
            job_filter='JOB_DWD_F_FITB_FIN_PLAN_CS_CD',
            output_file="output_config.xml"
        )
        end_time = time.time()
        print(f"处理时间: {end_time - start_time} 秒")

    custom_generate()

        # # 对比两个配置文件
        # compare_xml_files( './output_config.xml', '../asset/Workspace_256.txt')