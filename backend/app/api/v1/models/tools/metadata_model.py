# -*- coding: utf-8 -*-

from sqlalchemy import Column, String, Text, Boolean, JSON, Integer, ForeignKey, ARRAY
from app.core.base_model import *


class IndicatorModel(ModelBase, PrimaryKey, TimestampMixin, SoftDeleteMixin, CreatorMixin):
    """指标表"""
    __tablename__ = 't_indicator_info'

    indicator_name = Column(String(100), nullable=False, comment='指标名称')
    indicator_code = Column(String(100), nullable=False, comment='指标编码')
    indicator_type = Column(String(50), nullable=False, comment='指标类型')
    indicator_unit = Column(String(50), nullable=False, comment='计量单位')
    Measure_method = Column(String(100), nullable=False, comment='度量')
    description = Column(Text, nullable=True, comment='描述')
    dimension_sets = Column(JSON, nullable=True, comment='关联维度编码列表')


class AttributeModel(ModelBase, PrimaryKey, TimestampMixin, SoftDeleteMixin, CreatorMixin):
    """属性表"""
    __tablename__ = 't_attribute_info'

    attribute_name = Column(String(100), nullable=False, comment='属性名称')
    attribute_code = Column(String(100), nullable=False, comment='属性编码')
    attribute_type = Column(String(50), nullable=False, comment='属性类型')
    data_type = Column(String(50), nullable=False, comment='数据类型')
    default_value = Column(Text, nullable=True, comment='默认值')
    value_range = Column(JSON, nullable=True, comment='取值范围')
    description = Column(Text, nullable=True, comment='描述')


class DimensionModel(ModelBase, PrimaryKey, TimestampMixin, SoftDeleteMixin, CreatorMixin):
    """维度表"""
    __tablename__ = 't_dimension_info'

    dimension_name = Column(String(100), nullable=False, comment='维度名称')
    dimension_code = Column(String(100), nullable=False, comment='维度编码')
    dimension_type = Column(String(50), nullable=False, comment='维度类型')
    description = Column(Text, nullable=True, comment='描述')
    relate_attribute_code = Column(String(50), nullable=False, comment='维度类型')


class FactDataModel(ModelBase, PrimaryKey, TimestampMixin, SoftDeleteMixin, CreatorMixin):
    """事实表表"""
    __tablename__ = 't_factdata_info'

    data_source_code = Column(String(100), nullable=False, comment='数据源编码')
    schema_name = Column(String(50), nullable=False, comment='schema名称')
    table_name = Column(String(100), nullable=False, comment='事实表名称')
    table_code = Column(String(100), nullable=False, comment='数据表编码')
    dimension_lvl = Column(JSON, nullable=True, comment='维度颗粒度')
    constrain_sets = Column(ARRAY(String), nullable=True, comment='限定条件集')
    description = Column(Text, nullable=True, comment='描述')


class FactDataColumnModel(ModelBase, PrimaryKey, TimestampMixin, SoftDeleteMixin, CreatorMixin):
    """事实表字段表"""
    __tablename__ = 't_factdata_column_detail'

    table_code = Column(String(100), nullable=False, comment='事实表编码')
    column_name = Column(String(50), nullable=False, comment='字段名称')
    column_type = Column(String(50), nullable=False, comment='字段类型')
    meta_type = Column(String(50), nullable=False, comment='元数据类型')
    rela_meta_code = Column(String(100), nullable=False, comment='关联元数据编码')
    constrain_sets = Column(ARRAY(String), nullable=True, comment='限定条件集')


class ConstrainModel(ModelBase, PrimaryKey, TimestampMixin, SoftDeleteMixin, CreatorMixin):
    """约束条件表"""
    __tablename__ = 't_constrain_set_info'

    constrain_code = Column(String(100), nullable=False, comment='限定条件编码')
    constrain_name = Column(String(100), nullable=False, comment='限定条件名称')
    attribute_code = Column(String(100), nullable=False, comment='属性编码')
    compare_operator = Column(String(100), nullable=False, comment='对比条件')
    constrain_value = Column(ARRAY(String), nullable=False, comment='条件值范围')

# TODO

# class DataSourceModel(ModelBase, PrimaryKey, TimestampMixin, SoftDeleteMixin, CreatorMixin):
#     """数据来源表"""
#     __tablename__ = 't_data_source_info'
#
#     data_source_code = Column(String(100), nullable=False, comment='数据源编码')