# -*- coding: utf-8 -*-
from pydantic import Field, ConfigDict
from app.core.base_schema import BaseSchema


class ScriptSchema(BaseSchema):
    """脚本基础模型"""
    app_name: str = Field(default=None, description='所属应用名称')
    script_name: str = Field(default=None, description='脚本名称')
    script_path: str = Field(default=None, description='脚本路径')
    script_content: str = Field(default=None, description='脚本内容')
    is_parsed: bool = Field(default=False, description='是否已解析')
    script_type: str = Field(default=False, description='脚本类型')
    is_deleted: bool = Field(default=False, description='是否已删除')


class ScriptCreateSchema(ScriptSchema):
    """创建脚本模型"""
    id: None


class ScriptUpdateSchema(ScriptSchema):
    """更新脚本模型"""
    id: int = Field(..., gt=0, description='主键ID')


class ScriptOutSchema(ScriptSchema):
    """脚本响应模型"""
    model_config = ConfigDict(from_attributes=True)

class DialogSchema(BaseSchema):
    """语句基础模块"""

    script_id: int = Field(default=None, description='所属脚本id')
    dialog_type: str = Field(default=None, description='语句类型')
    dialog_content: str = Field(default=None, description='语句类型')
    dialog_parsed: str = Field(default=None, description='解析结果')
    dialog_order: int = Field(default=None, description='段落序号')
    target_tables: list = Field(default=None, description='目标表')
    source_tables: list = Field(default=None, description='来源表')


class DialogCreateSchema(DialogSchema):
    """创建脚本模型"""
    id: None


class DialogUpdateSchema(DialogSchema):
    """更新脚本模型"""
    id: int = Field(..., gt=0, description='主键ID')


class DialogOutSchema(DialogSchema):
    """脚本响应模型"""
    model_config = ConfigDict(from_attributes=True)
