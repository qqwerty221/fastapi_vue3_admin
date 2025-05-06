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
