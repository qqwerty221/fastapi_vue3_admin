# -*- coding: utf-8 -*-

from fastapi import APIRouter

from app.api.v1.controllers.tools.script_controller import router as ScriptRouter

ToolsApiRouter = APIRouter(prefix="/tools")

ToolsApiRouter.include_router(router=ScriptRouter, prefix="/script", tags=["脚本解析模块"])

