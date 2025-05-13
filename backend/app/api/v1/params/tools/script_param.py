# -*- coding: utf-8 -*-

from typing import Optional
from fastapi import Query


class ScriptQueryParams:
    """部门管理查询参数"""

    def __init__(
            self,
            app_name: Optional[str] = Query(None, description="应用名称", min_length=5, max_length=50),
            script_name: Optional[str] = Query(None, description="脚本名称", min_length=2, max_length=128),
            is_parsed: Optional[bool] = Query(None, description="是否解析(True已解析 False未解析)"),
            script_type: Optional[str] = Query(None, description="脚本类型", min_length=5, max_length=50)
            
    ) -> None:
        super().__init__()

        # 模糊查询字段
        self.app_name = ("like", app_name)
        self.script_name = ("like", script_name)

        # 精确查询字段
        self.is_parsed = is_parsed
        self.script_type = script_type
