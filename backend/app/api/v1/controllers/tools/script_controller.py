# -*- coding: utf-8 -*-

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import JSONResponse

from app.api.v1.params.tools.script_param import ScriptQueryParams
from app.api.v1.schemas.system.auth_schema import AuthSchema
from app.api.v1.services.tools.script_service import ScriptService
from app.common.request import PaginationService
from app.common.response import SuccessResponse
from app.core.base_params import PaginationQueryParams
from app.core.dependencies import get_current_user, AuthPermission
from app.core.logger import logger
from app.core.router_class import OperationLogRoute

router = APIRouter(route_class=OperationLogRoute)


@router.get('/list', summary='获取脚本列表', description='查询脚本清单接口')
async def get_script_list(
        page: PaginationQueryParams = Depends(),
        search: ScriptQueryParams = Depends(),
        auth: AuthSchema = Depends(AuthPermission(permissions=["tools:sqlparse:query"]))
) -> JSONResponse:
    """获取脚本列表"""
    result_dict_list = await ScriptService.get_script_list(auth=auth, search=search)
    result_dict = await PaginationService.get_page_obj(data_list=result_dict_list, page_no=page.page_no,page_size=page.page_size)
    logger.info(f"{auth.user.name} 获取当前脚本清单成功")
    return SuccessResponse(data=result_dict, msg='获取当前脚本清单成功')


@router.post('/import', summary='导入脚本')
async def import_scripts(
        auth: AuthSchema = Depends(get_current_user)
) -> JSONResponse:
    """导入脚本"""
    await ScriptService.import_scripts(auth=auth)
    return SuccessResponse(msg='导入脚本成功')

@router.post('/parse', summary='脚本解析')
async def parse_scripts(
        auth: AuthSchema = Depends(get_current_user)
) -> JSONResponse:
    """导入脚本"""
    await ScriptService.parse_scripts(auth=auth)
    return SuccessResponse(msg='导入解析成功')
