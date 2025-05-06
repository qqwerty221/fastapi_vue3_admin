# -*- coding: utf-8 -*-

from sqlalchemy import Column, String, Text, Boolean
from app.core.base_model import *


class ScriptModel(ModelBase, PrimaryKey, TimestampMixin, SoftDeleteMixin, CreatorMixin):
    """脚本表"""
    __tablename__ = 't_script_info'

    app_name = Column(String(50), nullable=False, comment='所属应用名称')
    script_name = Column(String(100), nullable=False, comment='脚本名称')
    script_path = Column(String(255), nullable=False, comment='脚本路径')
    script_content = Column(Text, nullable=False, comment='脚本内容')
    is_parsed = Column(Boolean, default=False, comment='是否已解析')
