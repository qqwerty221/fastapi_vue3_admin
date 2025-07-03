# 导入SQLAlchemy相关组件，用于数据库操作
from sqlalchemy import create_engine, Column, Integer, String, JSON, ForeignKey, Text, select, ARRAY, or_, MetaData, \
    text
from sqlalchemy.orm import relationship, scoped_session, declarative_base, aliased
from sqlalchemy.orm.session import sessionmaker
# 导入lxml的etree模块，用于XML处理
from lxml import etree as ET
import time
import os

# 数据库连接配置
DATABASE_URI = 'postgresql://fastapi:fastapi-root@10.124.160.153/fastapi'
# 创建数据库引擎
engine = create_engine(DATABASE_URI)
# 创建会话工厂，使用scoped_session确保线程安全
Session = scoped_session(sessionmaker(bind=engine))
session = Session()

# 创建declarative基类，用于定义ORM模型
metadata = MetaData(schema="parser")
Base = declarative_base(metadata=metadata)


class ControlMObject(Base):
    """Control-M对象模型，用于存储作业、文件夹等对象的基本信息和层级关系"""
    __tablename__ = 'controlm_objects'
    __table_args__ = {"schema": "parser"}
    # 主键ID
    id = Column(Integer, primary_key=True)
    # 父对象ID，用于构建层级关系
    parent_id = Column(Integer, ForeignKey('controlm_objects.id'))
    # 对象类型，如JOB、FOLDER等
    object_type = Column(String, nullable=False)
    # 对象名称
    name = Column(String, nullable=False)
    # 对象内容
    content = Column(String)
    # 在层级结构中的级别
    hierarchy_level = Column(Integer)
    # 在父对象中的位置
    position_in_parent = Column(Integer)
    # 子对象关系，cascade确保删除父对象时同时删除子对象
    children = relationship("ControlMObject", back_populates="parent", cascade="all, delete-orphan")
    # 父对象关系
    parent = relationship("ControlMObject", back_populates="children", remote_side=[id])
    # 对象属性关系，按position_in_object排序
    attributes = relationship("ControlMAttribute", order_by="ControlMAttribute.position_in_object",
                              back_populates="object")


class ControlMAttribute(Base):
    """Control-M对象的属性模型，用于存储对象的各种属性信息"""
    __tablename__ = 'controlm_attributes'
    __table_args__ = {"schema": "parser"}
    # 主键ID
    id = Column(Integer, primary_key=True)
    # 关联的对象ID
    object_id = Column(Integer, ForeignKey('controlm_objects.id'))
    # 属性名称
    attribute_name = Column(String, nullable=False)
    # 属性值
    attribute_value = Column(String)
    # 属性在对象中的位置顺序
    position_in_object = Column(Integer)
    # 与对象的关联关系
    object = relationship("ControlMObject", back_populates="attributes")


class ControlMObjectWithAttr(Base):
    """Control-M对象及其依赖关系的视图模型，用于存储对象的完整信息及其依赖链"""
    __tablename__ = 'controlm_object_with_attr'
    __table_args__ = {"schema": "parser"}
    # 主键ID
    id = Column(Integer, primary_key=True)
    # 父对象ID
    parent_id = Column(Integer)
    # 应用名称
    sub_application = Column(String, nullable=False)
    # 对象类型
    object_type = Column(String, nullable=False)
    # 对象通用名称
    general_name = Column(String, nullable=False)
    # 依赖使用名称
    dependency_name = Column(String, nullable=False)
    # 层级级别
    hierarchy_level = Column(Integer, nullable=False)
    # 在父对象中的位置
    position_in_parent = Column(Integer, nullable=False)
    # 对象的所有属性聚合为JSON
    json_object_agg = Column(JSON, nullable=False)
    # 上游对象列表
    from_object = Column(ARRAY(String), nullable=False)
    # 下游对象列表
    to_object = Column(ARRAY(String), nullable=False)


class ObjectExtend(Base):
    """CTM文件夹与作业对象 - 物化视图对象映射"""
    __tablename__ = "object_extend"
    __table_args__ = {"schema": "parser"}

    id = Column(Integer, primary_key=True)  # 对象唯一标识符（作业或文件夹）
    parent_id = Column(Integer)  # 父级对象ID（用于构建层级结构）
    sub_application = Column(String)  # 所属子应用名
    object_type = Column(String)  # 对象类型（如：Job, Folder）
    general_name = Column(String)  # 通用名称（用于展示或搜索）
    schedule_time = Column(String)  # 计划调度时间（字符串格式，例如“12:00”）
    cyclic = Column(String)  # 是否为周期性作业
    cyclic_type = Column(String)  # 周期性类型（如 DAILY、HOURLY 等）
    time_from = Column(String)  # 周期性作业起始时间
    rbc_type = Column(String)  # 关联的规则型日历类型（Rule-Based Calendar）
    level = Column(Integer)  # 层级深度（用于树状结构可视化）
    from_object = Column(String)  # 依赖关系的起点对象（如依赖链中的前驱作业）
    to_object = Column(String)  # 依赖关系的目标对象（如后继作业）
    is_schedule = Column(String)  # 是否启用调度（0 表示未调度）
    folder_rbc_type = Column(String)  # 文件夹级别的规则日历类型
    appl_type = Column(String)  # 应用类型（如 Batch、Real-time 等）


class ObjectImpact(Base):
    __tablename__ = 'object_impact'
    __table_args__ = {'schema': 'parser'}

    obj_id = Column(Integer, primary_key=True, comment="对象唯一标识")
    impact_ids = Column(Integer, comment="受影响的id")


def parse_xml_to_db(file_path, batch_size=10000):
    """
    从XML文件解析Control-M配置数据并批量插入数据库。
    采用递归方式处理XML的层级结构，并使用批量插入提高性能。

    :param file_path: XML文件路径
    :param batch_size: 每批插入的行数，默认为10000，用于控制内存使用
    """
    # 解析XML文件
    tree = ET.parse(file_path)
    root = tree.getroot()
    # 初始化待插入的对象和属性列表
    objects_to_add = []
    attributes_to_add = []
    batch_count = 0

    def _parse_element(element, parent_id, hierarchy_level):
        """递归解析XML元素

        :param element: 当前处理的XML元素
        :param parent_id: 父对象的ID
        :param hierarchy_level: 当前层级深度
        """
        nonlocal batch_count

        # 获取下一个对象ID
        query = text("SELECT nextval('parser.seq_controlm_object_id') AS next_value")
        object_id = session.execute(query).scalar()

        # 创建Control-M对象
        obj = ControlMObject(
            id=object_id,
            parent_id=parent_id,
            object_type=element.tag,  # 使用XML标签作为对象类型
            name=element.attrib.get('NAME', ''),  # 获取NAME属性，默认为空字符串
            content=element.text.strip() if element.text else None,  # 处理文本内容
            hierarchy_level=hierarchy_level,  # 记录层级深度
            position_in_parent=element.sourceline  # 使用行号作为位置信息
        )

        objects_to_add.append(obj)

        # 处理元素的所有属性
        for idx, (attr_name, attr_value) in enumerate(element.attrib.items()):
            attr = ControlMAttribute(
                object_id=obj.id,
                attribute_name=attr_name,
                attribute_value=attr_value,
                position_in_object=idx  # 保持属性的原始顺序
            )
            attributes_to_add.append(attr)

        batch_count += 1
        if batch_count >= batch_size:
            # 达到批处理大小时，执行批量插入
            session.bulk_insert_mappings(ControlMObject, [obj.__dict__ for obj in objects_to_add])
            session.bulk_insert_mappings(ControlMAttribute, [attr.__dict__ for attr in attributes_to_add])
            session.commit()
            # 清空列表准备下一批数据
            objects_to_add.clear()
            attributes_to_add.clear()
            batch_count = 0
            print(10000)  # 打印进度指示

        # 递归处理所有子元素
        for child in element:
            _parse_element(child, parent_id=obj.id, hierarchy_level=hierarchy_level + 1)

    # 从根节点开始解析
    _parse_element(root, parent_id=None, hierarchy_level=0)

    # 处理最后一批未满batch_size的数据
    if objects_to_add or attributes_to_add:
        session.bulk_insert_mappings(ControlMObject, [obj.__dict__ for obj in objects_to_add])
        session.bulk_insert_mappings(ControlMAttribute, [attr.__dict__ for attr in attributes_to_add])
        session.commit()

    # 最后刷新物化视图
    query = text("REFRESH MATERIALIZED VIEW parser.object_extend;")
    object_id = session.execute(query).scalar()

    print("解析完成")


# 分析作业影响范围
# def impact_analyse():  --效率太低，卡死
#     query = text("SELECT * FROM parser.impact_analyse(:id, 'ALL' )")
#     object_to_add = []
#     batch_count = 0
#     batch_size = 1000
#
#     # 获取下一个对象ID
#     ids = session.query(ObjectExtend.id).order_by(ObjectExtend.id).all()
#     for row in ids:
#         result = session.execute(query, {"id": row.id, "app_to_skip": ''})
#         for id_list in result:
#             # new_record = ObjectImpact(
#             #     obj_id=row.id,
#             #     impact_ids=id_list.id[0]  # 可以是逗号分隔的字符串
#             # )
#             object_to_add.append({'obj_id': int(row.id), 'impact_ids': int(id_list.id)})
#
#             # print(str(row.id) + ',', end="")
#
#         # 处理最后一批未满batch_size的数据
#         if object_to_add:
#             session.bulk_insert_mappings(ObjectImpact, [obj for obj in object_to_add])
#             session.commit()
#             object_to_add.clear()
#
#     print("done")

#
# def transform_to_json():   -- 效率太低，废弃
#     obj = {}
#
#     name = 'JOB_HQL_P_DWD_CMSK_HD03_FMCS_PROFILE_RUN_MONTH_CS_CD'
#
#     obj[name] = {}
#
#     todo1 = []
#     todo1.append(name)
#
#     def loop_test(input_ame):
#         query = text("SELECT general_name FROM parser.object_extend where :name = any(from_object) ;")
#         result = session.execute(query, {'name': input_ame})
#         output = {}
#         for row in result:
#             todo1.append(row.general_name)
#             output[row.general_name] = {}
#
#         return output
#
#     while len(todo1) > 0:
#         p_name = todo1[0]
#         output_dict = loop_test(p_name)
#         todo1.pop(0)
#         expr = parse('$..' + p_name)
#         for match in expr.find(obj):
#             match.full_path.update(obj, output_dict)

def transform_to_graph():
    # 数据库连接配置
    DATABASE_URI_AGE = 'postgresql://pgage:pgage-root@10.124.160.153:7688/postgres'
    # 创建数据库引擎
    engine_age = create_engine(DATABASE_URI_AGE)
    # 创建会话工厂，使用scoped_session确保线程安全
    Session_age = scoped_session(sessionmaker(bind=engine_age))
    session_age = Session_age()

    # 初始化graph数据库
    session_age.execute(text("LOAD 'age';"))
    session_age.execute(text("SET search_path = ag_catalog, '$user', public;"))

    #1) 将所有的节点添加到age（FOLDER，SUB_FOLDER，SMART_FOLDER，JOB）
    # 查询条件：查找objectextend里的所有对象
    query = (
        select(ObjectExtend.id, ObjectExtend.sub_application, ObjectExtend.object_type, ObjectExtend.general_name)
        .where(ObjectExtend.sub_application.isnot(None))
    )

    object_list = session.execute(query).all()

    for obj in object_list:
        create_stmt = text("select * from cypher('my_graph', $$ CREATE (p:"
                           + obj.object_type + " {obj_app:'" + obj.sub_application
                           + "',obj_id:'" + str(obj.id)
                           + "',obj_name:'" + obj.general_name + "'})$$) as (name agtype)")
        result = session_age.execute(create_stmt)

    #2) 添加FOLDER关系依赖
    # 查询条件：查找objectchain里的所有对象关系
    Parent = aliased(ObjectExtend)

    query = (
        select(
            ObjectExtend.id.label("obj_id"),
            ObjectExtend.object_type.label("child_type"),
            ObjectExtend.parent_id.label("prt_id"),
            Parent.object_type.label("prt_type")
        )
        .join(Parent, ObjectExtend.parent_id == Parent.id, isouter=True)
        .where(Parent.object_type.isnot(None))
        .distinct()
    )

    object_list = session.execute(query).all()

    for obj in object_list:
        create_stmt = text("select * from cypher('my_graph', $$ CREATE (p:"
                           + obj.object_type + " {obj_app:'" + obj.sub_application
                           + "',obj_id:'" + str(obj.id)
                           + "',obj_name:'" + obj.general_name + "'})$$) as (name agtype)")
        result = session_age.execute(create_stmt)

    #3) 添加依赖关系依赖
    print(obj_count, '=', len(object_list))


def generate_config_file(
        sub_application_filter: list[str] | None = None,
        attributes_to_delete: list[str] = [],
        attributes_to_update: dict[str, str] = {},
        job_filter: str | None = None,
        output_file: str = "output_config.xml"
):
    """
    从数据库获取Control-M配置数据并生成新的XML配置文件。
    支持按子应用程序筛选、属性更新和删除、作业依赖链过滤等功能。

    :param sub_application_filter: 按SUB_APPLICATION筛选folder, smart_folder, sub_folder, job
    :param attributes_to_delete: 需要批量删除的属性名列表
    :param attributes_to_update: 需要批量更新的属性值字典，格式为{attribute_name: new_value}
    :param job_filter: 作业名称过滤器，只导出指定作业及其依赖链
    :param output_file: 输出的XML文件路径
    """
    # 初始化参数默认值
    if attributes_to_delete is None:
        attributes_to_delete = []
    if attributes_to_update is None:
        attributes_to_update = {}

    # 构建基础查询
    # 查询条件：选择DEFTABLE/WORKSPACE类型的对象，或者匹配指定子应用程序的对象
    query = (
        select(ControlMObjectWithAttr)
        .where(
            or_(
                ControlMObjectWithAttr.object_type.in_(['DEFTABLE', 'WORKSPACE']),
                ControlMObjectWithAttr.sub_application.in_(sub_application_filter) if sub_application_filter else True,
            )
        )
    )

    # 执行查询获取对象列表
    objects = session.execute(query)

    # 如果指定了作业过滤器，获取该作业的依赖链
    filter_id_list = []
    if job_filter:
        # 调用存储过程获取依赖链ID列表
        query_filter = text("SELECT id FROM parser.get_dependency_chain('" + job_filter + "','" + ",".join(
            sub_application_filter) + "') order by id;")
        filter_list = session.execute(query_filter)
        # 提取ID列表
        filter_id_list = [id[0] for id in filter_list]

    # 初始化XML树结构
    root = ET.Element("ROOT")
    # 用于跟踪已处理的对象ID
    selected_ids = set()
    # 记录处理的对象数量
    selected_count = 0

    def attr_value_check(value):
        """检查并处理属性值长度
        如果属性值长度超过64，则截断前3个字符

        :param value: 原始属性值
        :return: 处理后的属性值
        """
        result = value
        if len(value) >= 64:
            result = value[3:]
        return result

    def process_object(obj, parent_element):
        """
        处理单个Control-M对象，将其转换为XML元素并添加到父元素中。
        包含对象过滤、属性处理和递归处理子对象的逻辑。

        :param obj: Control-M对象实例
        :param parent_element: 父XML元素
        """
        nonlocal selected_count

        # 防止重复处理同一对象
        if obj.id in selected_ids:
            return

        # 确保父对象已经被处理（维护对象层级关系）
        if obj.parent_id and not obj.parent_id in selected_ids:
            return

        # 获取对象的所有属性
        attr_dict = obj.json_object_agg

        # 注：子应用程序过滤已在SQL查询中实现
        # if sub_application_filter and attr_dict.get('SUB_APPLICATION') != sub_application_filter:
        #     if obj.object_type in ['SMART_FOLDER', 'JOB', 'FOLDER', 'SUB_FOLDER']:
        #         return  # SMART_FOLDER,JOB 只提取筛选条件内的范围

        # 作业过滤：只处理指定作业及其依赖链
        if job_filter and obj.object_type in ['JOB'] and obj.id not in filter_id_list:
            return

        # 排除特定类型的作业
        if obj.object_type == 'JOB' and attr_dict.get('APPL_TYPE') == 'DAYUJOB':
            return

        # 创建新的XML元素
        element = ET.SubElement(parent_element, obj.object_type)

        # 处理对象的属性
        for attr, value in attr_dict.items():
            # 跳过需要删除的属性
            if attr in attributes_to_delete:
                continue

            if obj.object_type in ['JOB', 'SMART_FOLDER', 'SUB_FOLDER'] and attr == 'JOBNAME':
                value = attr_value_check(value + attributes_to_update)

            elif obj.object_type == 'FOLDER' and attr == 'FOLDER_NAME':
                value = attr_value_check(value + attributes_to_update)

            elif obj.object_type in ['INCOND', 'OUTCOND'] and attr == 'NAME':
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
        """
        处理XML元素的结构转换，包括文件夹类型转换、属性更新和清理。

        :param parent_element: 父XML元素
        :param element: 当前处理的XML元素
        :param new_root_name: 新的根文件夹名称
        """
        # 移除没有作业的文件夹元素
        if element.tag in ['SUB_FOLDER', 'SMART_FOLDER', 'FOLDER'] and not 'JOB' in [child.tag for child in element]:
            parent_element.remove(element)
            return

        # 处理作业和子文件夹的父文件夹路径
        if element.tag in ['JOB', 'SUB_FOLDER']:
            # 更新父文件夹路径，添加属性更新后缀
            element.attrib['PARENT_FOLDER'] = new_root_name + '/' + '/'.join(
                [path + attributes_to_update for path in element.attrib['PARENT_FOLDER'].split('/')])
            # 删除循环相关的属性
            for attr in element.attrib:
                if attr in ['CYCLIC_TIMES_SEQUENCE', 'CYCLIC_TOLERANCE', 'CYCLIC_TYPE', 'IND_CYCLIC', 'CYCLIC']:
                    del element.attrib[attr]

        # 将SMART_FOLDER和FOLDER转换为SUB_FOLDER
        if element.tag in ['SMART_FOLDER', 'FOLDER']:
            # 修改元素类型
            element.tag = 'SUB_FOLDER'
            # 设置父文件夹路径
            element.attrib['PARENT_FOLDER'] = attr_value_check(new_root_name)

            # 设置创建者标记
            element.set('CREATED_BY', 'inport')

            # 只保留指定的属性，删除其他属性
            allowed_attrs = [
                'JOBISN', 'APPLICATION', 'SUB_APPLICATION', 'JOBNAME', 'CREATED_BY', 'RUN_AS', 'CRITICAL',
                'TASKTYPE', 'CYCLIC', 'INTERVAL', 'CONFIRM', 'RETRO', 'MAXWAIT', 'MAXRERUN', 'AUTOARCH',
                'MAXDAYS', 'MAXRUNS', 'DAYS', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP',
                'OCT', 'NOV', 'DEC', 'DAYS_AND_OR', 'PARENT_FOLDER'
            ]
            # 删除不在允许列表中的属性
            for attr in list(element.attrib.keys()):
                if attr not in allowed_attrs:
                    del element.attrib[attr]

        # 递归处理所有子元素
        for child in element:
            xml_element_process(element, child, new_root_name)

    # 从根节点开始处理所有对象
    for obj in objects:
        if not obj[0].parent_id:  # 只处理根节点，子节点通过递归处理
            process_object(obj[0], root)

    # 创建XML树结构
    tree = ET.ElementTree(root)

    # 获取DEFTABLE节点作为父文件夹
    parent_folder = tree.find('DEFTABLE')
    # 生成带时间戳的唯一标识
    current_unix_time = '_' + str(int(time.time()))

    # 为每个子应用程序创建独立的文件夹
    for sub_application in sub_application_filter:
        # 创建新的智能文件夹
        one_time_folder = ET.Element("SMART_FOLDER")
        # 设置基本属性
        one_time_folder.set('APPLICATION', 'CMSK')
        one_time_folder.set('SUB_APPLICATION', sub_application)
        one_time_folder.set('RUN_AS', sub_application)
        # 设置文件夹标识，使用子应用程序名称加时间戳
        folder_identifier = sub_application + current_unix_time
        one_time_folder.set('PARENT_FOLDER', folder_identifier)
        one_time_folder.set('JOBNAME', folder_identifier)
        one_time_folder.set('FOLDER_NAME', folder_identifier)
        # 设置版本和环境相关属性
        one_time_folder.set('VERSION_HOST', '100.65.250.72, 100.73.29.67, 100.73.56.60')
        one_time_folder.set('IS_CURRENT_VERSION', 'Y')
        one_time_folder.set('DATACENTER', 'controlm')
        one_time_folder.set('VERSION_OPCODE', 'N')
        one_time_folder.set('VERSION', '919')
        # 设置文件夹行为属性
        one_time_folder.set('FOLDER_ORDER_METHOD', 'SYSTEM')
        one_time_folder.set('ENFORCE_VALIDATION', 'N')
        one_time_folder.set('RULE_BASED_CALENDAR_RELATIONSHIP', 'O')
        one_time_folder.set('CREATED_BY', 'inport')

        # 将匹配的子应用程序元素移动到新文件夹
        for folder_like_element in parent_folder:
            if folder_like_element.attrib.get('SUB_APPLICATION') and folder_like_element.attrib[
                'SUB_APPLICATION'] == sub_application:
                one_time_folder.append(folder_like_element)

        # 将新文件夹添加到父文件夹
        parent_folder.append(one_time_folder)

        # 创建并配置基于规则的日历
        RULE_BASED_CALENDAR = ET.SubElement(one_time_folder, 'RULE_BASED_CALENDAR')
        RULE_BASED_CALENDAR.set('NAME', 'none')
        RULE_BASED_CALENDAR.set('MAXWAIT', '0')
        # 设置所有月份的默认值为1
        for attr in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']:
            RULE_BASED_CALENDAR.set(attr, '1')

        # 递归处理文件夹中的所有元素
        for element in one_time_folder:
            xml_element_process(one_time_folder, element, one_time_folder.attrib['JOBNAME'])

    # 将处理后的XML写入文件
    with open(output_file, "wb") as f:
        f.write(ET.tostring(parent_folder, pretty_print=True, xml_declaration=True, encoding="utf-8"))

    # 检查是否有未处理的对象
    all_object_ids = {obj.id for obj in objects}
    missing_ids = all_object_ids - selected_ids
    if missing_ids:
        print(f"Warning: Some objects were not processed. Missing IDs: {missing_ids}")

    # 输出处理统计信息
    print(f"Selected {selected_count} elements with IDs: {selected_ids}")


if __name__ == '__main__':
    def parse_init():
        """初始化函数：解析XML文件并导入数据库"""
        start_time = time.time()
        # 构建XML文件路径
        file_path = os.path.join(os.path.dirname(__file__), "asset", "Workspace_300.xml")
        # 执行解析和导入
        # parse_xml_to_db(file_path=file_path, batch_size=10000)
        impact_analyse()
        # 计算并打印处理时间
        end_time = time.time()
        print(f"处理时间: {end_time - start_time} 秒")


    def custom_generate():
        """自定义生成函数：根据特定条件生成新的配置文件"""
        start_time = time.time()
        # 生成配置文件，指定过滤条件和更新规则
        generate_config_file(
            sub_application_filter=[],  # 不限制子应用程序
            # attributes_to_delete=["ATTRIBUTE1", "ATTRIBUTE2"],  # 可选：指定要删除的属性
            attributes_to_update='_G',  # 属性更新后缀
            job_filter='JOB_DWD_F_FITB_FIN_PLAN_CS_CD',  # 指定作业过滤器
            output_file="output_config.xml"  # 输出文件名
        )
        # 计算并打印处理时间
        end_time = time.time()
        print(f"处理时间: {end_time - start_time} 秒")

    # 取消注释以执行相应功能
    # parse_init()
    # custom_generate()
    transform_to_graph()
    # 用于配置文件对比的功能（已注释）
    # compare_xml_files( './output_config.xml', '../asset/Workspace_256.txt')
