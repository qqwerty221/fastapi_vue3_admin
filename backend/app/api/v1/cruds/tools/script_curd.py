# -*- coding: utf-8 -*-
from typing import List

from app.api.v1.schemas.tools.script_schema import *
from app.core.base_crud import CRUDBase
from app.api.v1.models.tools.script_model import ScriptModel, DialogModel
from app.api.v1.schemas.system.auth_schema import AuthSchema


class ScriptCRUD(CRUDBase[ScriptModel, ScriptCreateSchema, ScriptUpdateSchema]):
    """脚本模块数据层"""

    def __init__(self, auth: AuthSchema) -> None:
        """初始化脚本CRUD"""
        super().__init__(model=ScriptModel, auth=auth)


class DialogCRUD(CRUDBase[DialogModel, DialogCreateSchema, DialogUpdateSchema]):
    """脚本模块数据层"""

    def __init__(self, auth: AuthSchema) -> None:
        """初始化脚本CRUD"""
        super().__init__(model=DialogModel, auth=auth)

    async def get_ids_by_script_id(self, script_id: int) -> List[int]:
        """通过脚本id获取语句idlist"""
        dialog_obj_list = await self.partial_list(named_select=['id'], search={"script_id": script_id})
        dialog_ids = [obj.id for obj in dialog_obj_list]
        return dialog_ids