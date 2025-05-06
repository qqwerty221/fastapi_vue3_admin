# -*- coding: utf-8 -*-

import os
from datetime import datetime
from typing import List, Dict

from app.api.v1.cruds.tools.script_curd import ScriptCRUD
from app.api.v1.params.tools.script_param import ScriptQueryParams
from app.api.v1.schemas.system.auth_schema import AuthSchema
from app.api.v1.schemas.tools.script_schema import ScriptUpdateSchema, ScriptCreateSchema, ScriptSchema, ScriptOutSchema
from app.config.setting import settings
from app.core.exceptions import CustomException
from app.core.logger import logger


class ScriptService:
    """脚本服务"""

    @classmethod
    async def get_script_list(cls, auth: AuthSchema, search: ScriptQueryParams) -> List[Dict]:
        """获取脚本列表"""
        script_list = await ScriptCRUD(auth).list(search=search.__dict__)
        # 将ScriptModel对象转换为可序列化的字典格式
        result_list = []

        for script in script_list:
            bbb = ScriptOutSchema.model_validate(script).model_dump()
            result_list.append(bbb)

        return [ScriptSchema.model_validate(script).model_dump() for script in script_list]

    @classmethod
    async def create_script_service(cls, auth: AuthSchema, data: ScriptCreateSchema) -> Dict:
        script = await ScriptCRUD(auth).get(app_name=data.app_name, script_name=data.script_name)
        if script:
            new_script = await ScriptCRUD(auth).update(id=script.id,data=data)
        else:
            new_script = await ScriptCRUD(auth).create(data=data)
        new_script_dict = ScriptOutSchema.model_validate(new_script).model_dump()
        return new_script_dict

    @classmethod
    async def import_scripts(cls, auth: AuthSchema) -> None:
        """导入脚本"""
        base_path = os.path.join(settings.BASE_DIR.parent, settings.SCRIPT_BASE)

        # 遍历目录获取所有应用和脚本
        for app_name in os.listdir(base_path):
            app_path = os.path.join(base_path, app_name)
            if not os.path.isdir(app_path) or app_name.startswith('.'):
                continue

            # 递归遍历应用目录下的所有文件
            # 定义需要跳过的文件和目录
            skip_files = {'.git', 'test', 'TEST', 'README.md'}
            skip_dirs = {'.git', 'test', 'TEST'}

            for root, dirs, files in os.walk(app_path):
                # 过滤掉需要跳过的目录
                dirs[:] = [d for d in dirs if d not in skip_dirs]

                for file in files:
                    if files:
                        # 跳过特定文件
                        if file.startswith('.') or file in skip_files:
                            continue

                        file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(file_path, base_path)

                        # 读取文件内容
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                        except Exception as e:
                            logger.info(msg=f'读取文件{file_path}失败: {str(e)}')
                            continue

                        import_script = ScriptCreateSchema(
                            id=None,
                            app_name=app_name,
                            script_name=file,
                            script_path=file_path,
                            script_content=content,
                            is_parsed=False,
                            is_delete=False
                        )

                        try:
                            result = await cls.create_script_service(auth=auth,data=import_script)
                        except Exception as e:
                            logger.info(f'{file}脚本导入异常')
                            continue