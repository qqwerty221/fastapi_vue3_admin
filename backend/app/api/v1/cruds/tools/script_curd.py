# -*- coding: utf-8 -*-

from app.api.v1.schemas.tools.script_schema import ScriptCreateSchema, ScriptUpdateSchema
from app.core.base_crud import CRUDBase
from app.api.v1.models.tools.script_model import ScriptModel
from app.api.v1.schemas.system.auth_schema import AuthSchema


class ScriptCRUD(CRUDBase[ScriptModel, ScriptCreateSchema, ScriptUpdateSchema]):
    """脚本模块数据层"""

    def __init__(self, auth: AuthSchema) -> None:
        """初始化脚本CRUD"""
        super().__init__(model=ScriptModel, auth=auth)
