# -*- coding: utf-8 -*-

from typing import Any, Callable, Coroutine
from fastapi import Request, Response
from fastapi.routing import APIRoute
from user_agents import parse

from app.api.v1.schemas.system.auth_schema import AuthSchema
from app.api.v1.schemas.system.operation_log_schema import OperationLogCreateSchema
from app.api.v1.services.system.operation_log_service import OperationLogService
from app.core.database import session_connect
from app.config.setting import settings

"""
在 FastAPI 中，route_class 参数用于自定义路由的行为。
通过设置 route_class，你可以定义一个自定义的路由类，从而在每个路由处理之前或之后执行特定的操作。
这对于日志记录、权限验证、性能监控等场景非常有用。
"""


class OperationLogRoute(APIRoute):
    """操作日志路由装饰器"""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            # 请求前的处理
            response: Response = await original_route_handler(request)

            # 请求后的处理
            if not settings.OPERATION_LOG_RECORD:
                return response
            if request.method not in settings.OPERATION_RECORD_METHOD:
                return response
            route: APIRoute = request.scope.get("route")
            if route.name in settings.IGNORE_OPERATION_FUNCTION:
                return response

            user_agent = parse(request.headers.get("user-agent"))
            payload = b"{}"
            req_content_type = request.headers.get("Content-Type", "")

            if 'multipart/form-data' in req_content_type or 'application/x-www-form-urlencoded' in req_content_type:
                payload = ', '.join([f'{k}: {v}' for k, v in (await request.form()).items()])
            else:
                payload = await request.body()
                path_params = request.path_params
                oper_param = {}
                if payload:
                    oper_param = payload.decode()
                if path_params:
                    oper_param.update(path_params)
                # payload = json.dumps(oper_param, ensure_ascii=False)
                payload = str(oper_param)

            response_data = response.body if "application/json" in response.headers.get("Content-Type", "") else b"{}"

            async with session_connect() as session:
                async with session.begin():
                    auth = AuthSchema(db=session)
                    # 获取当前用户ID,如果是登录接口则为空
                    current_user_id = request.scope.get("user_id") if "user_id" in request.scope else None

                    await OperationLogService.create_log_service(data=OperationLogCreateSchema(
                        request_path=request.url.path,
                        request_method=request.method,
                        request_payload=payload,
                        request_ip=request.client.host,
                        request_os=user_agent.os.family,
                        request_browser=user_agent.browser.family,
                        response_code=response.status_code,
                        response_json=response_data.decode(),
                        description=route.summary,
                        creator_id=current_user_id
                    ), auth=auth)

            return response

        return custom_route_handler
