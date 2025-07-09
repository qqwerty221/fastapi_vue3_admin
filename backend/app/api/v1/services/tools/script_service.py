# -*- coding: utf-8 -*-
import json
import os
import traceback
from datetime import datetime
from typing import List, Dict

from app.api.v1.cruds.tools.script_curd import ScriptCRUD, DialogCRUD
from app.api.v1.params.tools.script_param import ScriptQueryParams
from app.api.v1.schemas.system.auth_schema import AuthSchema
from app.api.v1.schemas.tools.script_schema import ScriptUpdateSchema, ScriptCreateSchema, ScriptSchema, \
    ScriptOutSchema, DialogCreateSchema, DialogOutSchema
from app.config.setting import settings
from app.core.exceptions import CustomException
from app.core.logger import logger
import sqlglot


class ScriptService:
    """脚本服务"""

    @classmethod
    async def get_script_list(cls, auth: AuthSchema, search: ScriptQueryParams = None) -> List[Dict]:
        """获取脚本列表"""
        if not search:
            search = ScriptQueryParams(
                app_name=None,
                script_name=None,
                is_parsed=None
            )

        script_list = await ScriptCRUD(auth).list(search=search.__dict__)
        # 将ScriptModel对象转换为可序列化的字典格式
        result_list = []

        for script in script_list:
            result_list.append(ScriptOutSchema.model_validate(script).model_dump())

        return [ScriptSchema.model_validate(script).model_dump() for script in script_list]

    @classmethod
    async def create_script_service(cls, auth: AuthSchema, data: ScriptCreateSchema) -> Dict:
        script = await ScriptCRUD(auth).get(app_name=data.app_name, script_name=data.script_name)
        if script:
            new_script = await ScriptCRUD(auth).update(id=script.id, data=data)
        else:
            new_script = await ScriptCRUD(auth).create(data=data)
        new_script_dict = ScriptOutSchema.model_validate(new_script).model_dump()
        return new_script_dict

    @classmethod
    async def create_dialog_service(cls, auth: AuthSchema, script_id: int, data: List[DialogCreateSchema], database_type: str) -> None:
        dialog_ids = await DialogCRUD(auth).get_ids_by_script_id(script_id=script_id)
        try:
            await DialogCRUD(auth).delete(ids=dialog_ids)

            for dialog in data:
                await DialogCRUD(auth).create(data=dialog)

            await ScriptCRUD(auth).set(ids=[script_id], is_parsed=True, script_type=database_type)

        except Exception as e:
            raise CustomException(msg=f'{script_id}语句创建失败')

    @classmethod
    async def import_scripts(cls, auth: AuthSchema) -> None:
        """导入脚本"""
        base_path = os.path.join(settings.BASE_DIR.parent, settings.SCRIPT_BASE)

        # 遍历目录获取所有应用和脚本
        for app_name in os.listdir(base_path):
            logger.info(f'开始处理{app_name}')
            app_path = os.path.join(base_path, app_name)
            if not os.path.isdir(app_path) or app_name.startswith('.'):
                continue

            # 递归遍历应用目录下的所有文件
            # 定义需要跳过的文件和目录
            skip_files = {'.git', 'test', 'TEST', 'README.md', 'Readme.md'}
            skip_dirs = {'.git', 'test', 'TEST', 'TEST_JOB'}

            for root, dirs, files in os.walk(app_path):
                # 过滤掉需要跳过的目录
                dirs[:] = [d for d in dirs if d not in skip_dirs]

                for file in files:
                    if files:
                        # 跳过特定文件
                        if file.startswith('.') or file in skip_files:
                            continue

                        file_path = os.path.join(root, file)

                        if "批计算" in file_path:
                            script_type = 'HIVE'
                        elif "数据集成" in file_path:
                            script_type = 'DI'
                        elif "通用" in file_path:
                            script_type = 'COMMON'
                        elif "流计算" in file_path:
                            script_type = 'FLINK'
                        else:
                            script_type = 'ERROR'

                        # 读取文件内容
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                        except Exception as e:
                            logger.info(msg=f'读取文件{file_path}失败: {str(e)}')
                            continue

                        if script_type == 'DI':
                            content = json.dump(content)


                        import_script = ScriptCreateSchema(
                            id=None,
                            app_name=app_name,
                            script_name=file.replace(".sql", "").lower(),
                            script_path=file_path.replace(base_path, "").replace('/' + app_name , "").replace(".sql", "").lower(),
                            script_content=content,
                            is_parsed=False,
                            script_type=script_type,
                            is_delete=False
                        )

                        try:
                            result = await cls.create_script_service(auth=auth, data=import_script)
                        except Exception as e:
                            logger.info(f'{file}脚本导入异常')
                            continue

    @classmethod
    async def parse_scripts(cls, auth: AuthSchema) -> None:
        """解析脚本"""
        result_dict_list = await cls.get_script_list(auth=auth,
                                                     search=ScriptQueryParams(app_name=None,
                                                                              script_name=None,
                                                                              is_parsed=False,
                                                                              script_type='HIVE'
                                                                              ))
        for script in result_dict_list:
            try:

                for database_type in ['hive', 'postgres', 'mysql']:
                    try:
                        parsed_dialog_list = sqlglot.parse(script['script_content'],dialect=database_type)
                        if parsed_dialog_list:
                            break
                    except Exception as e:
                        logger.info('not' + database_type)
                        continue

                create_dialog_list = []

                for index, dialog in enumerate(parsed_dialog_list, start=1):
                    source_tables = set()
                    cte_to_remove = set()
                    target_tables = set()
                    if dialog:
                        node_list = dialog.walk()
                        for node in node_list:
                            if isinstance(node, sqlglot.exp.Table):
                                source_tables.add(node.name.lower())
                            if isinstance(node, (sqlglot.exp.CTE, sqlglot.exp.Subquery)):
                                cte_to_remove.add(node.alias.lower())
                            if isinstance(node, (sqlglot.exp.Insert, sqlglot.exp.Create)):
                                target_tables.add(node.this.this.name.lower())

                        source_tables = source_tables - target_tables - cte_to_remove
                        create_dialog = DialogCreateSchema(
                            id=None,
                            script_id=script['id'],
                            dialog_type=dialog.__class__.__name__,
                            dialog_content=dialog.sql(),
                            dialog_parsed=dialog.dump(),
                            dialog_order=index,
                            target_tables=target_tables,
                            source_tables=source_tables
                        )
                        create_dialog_list.append(create_dialog)
                await cls.create_dialog_service(auth=auth, script_id=script["id"], data=create_dialog_list,database_type=database_type.upper())


            except Exception as e:
                error_message = traceback.format_exc()
                logger.error(f"异常原因: {error_message}")
                logger.info(f'{script["script_name"]}脚本解析异常')
                continue
