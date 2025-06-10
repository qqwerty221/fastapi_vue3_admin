# -*- coding: utf-8 -*-

import importlib
import uuid

from sqlalchemy.orm import declarative_base
from typing import Any, List, Dict, Sequence, Optional

from app.core.logger import logger
from app.core.exceptions import CustomException

Base = declarative_base()

def import_module(module: str, desc: str) -> Any:
    """
    动态导入模块
    :param module: 模块名称
    :param desc: 模块描述
    :return: 模块对象
    """
    try:
        module_path, module_class = module.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, module_class)
    except ModuleNotFoundError:
        logger.error(f"导入{desc}失败,未找到模块:{module}")
        raise ModuleNotFoundError(f"导入{desc}失败,未找到模块:{module}")
    except AttributeError:
        logger.error(f"导入{desc}失败,未找到模块方法:{module}")
        raise AttributeError(f"导入{desc}失败,未找到模块方法:{module}")

async def import_modules_async(modules: list, desc: str, **kwargs):
    """
    异步导入模块列表
    :param modules: 模块列表
    :param desc: 模块描述
    :param kwargs: 额外参数
    """
    for module in modules:
        if not module:
            continue
        try:
            module_path = module[0:module.rindex(".")]
            module_name = module[module.rindex(".") + 1:]
            module_obj = importlib.import_module(module_path)
            await getattr(module_obj, module_name)(**kwargs)
        except ModuleNotFoundError:
            logger.error(f"导入{desc}失败,未找到模块:{module}")
            raise ModuleNotFoundError(f"导入{desc}失败,未找到模块:{module}")
        except AttributeError:
            logger.error(f"导入{desc}失败,未找到模块方法:{module}")
            raise AttributeError(f"导入{desc}失败,未找到模块方法:{module}")

def get_random_character() -> str:
    """生成随机字符串"""
    return uuid.uuid4().hex

def get_parent_id_map(model_list: Sequence[Base]) -> Dict[int, int]:
    """
    获取父级ID映射字典
    :param model_list: 模型列表
    :return: {id: parent_id} 映射字典
    """
    return {item.id: item.parent_id for item in model_list}

def get_parent_recursion(
        id: int,
        id_map: Dict[int, int],
        ids: Optional[List[int]] = None
) -> List[int]:
    """
    递归获取所有父级ID
    :param id: 当前ID
    :param id_map: ID映射字典
    :param ids: 已收集的ID列表
    :return: 所有父级ID列表
    """
    ids = ids or []
    if id in ids:
        raise CustomException(msg="递归获取父级ID失败,不可以自引用")
    ids.append(id)
    parent_id = id_map.get(id)
    if parent_id:
        get_parent_recursion(parent_id, id_map, ids)
    return ids

def get_child_id_map(model_list: Sequence[Base]) -> Dict[int, List[int]]:
    """
    获取子级ID映射字典
    :param model_list: 模型列表
    :return: {id: [child_ids]} 映射字典
    """
    data_map = {}
    for model in model_list:
        data_map.setdefault(model.id, [])
        if model.parent_id:
            data_map.setdefault(model.parent_id, []).append(model.id)
    return data_map

def get_child_recursion(
        id: int,
        id_map: Dict[int, List[int]],
        ids: Optional[List[int]] = None
) -> List[int]:
    """
    递归获取所有子级ID
    :param id: 当前ID
    :param id_map: ID映射字典
    :param ids: 已收集的ID列表
    :return: 所有子级ID列表
    """
    ids = ids or []
    ids.append(id)
    for child in id_map.get(id, []):
        get_child_recursion(child, id_map, ids)
    return ids

def bytes2human(n: int, format_str: str = '%(value).1f%(symbol)s') -> str:
    """
    字节数转人类可读格式
    :param n: 字节数
    :param format_str: 格式化字符串
    :return: 可读的字节字符串,如 '1.5MB'
    """
    symbols = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    prefix = {s: 1 << (i + 1) * 10 for i, s in enumerate(symbols[1:])}
    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format_str % locals()
    return format_str % dict(symbol=symbols[0], value=n)

def bytes2file_response(bytes_info: bytes):
    """生成文件响应"""
    yield bytes_info

