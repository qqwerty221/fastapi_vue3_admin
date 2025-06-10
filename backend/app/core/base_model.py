# -*- coding: utf-8 -*-
from datetime import datetime

from sqlalchemy import Column, Integer, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, declared_attr
from sqlalchemy.ext.asyncio import AsyncAttrs

Base = declarative_base()


class ModelBase(AsyncAttrs, Base):
    """
    SQLAlchemy 基础模型类
    继承自 AsyncAttrs 和 DeclarativeBase,提供异步操作支持
    """
    __abstract__ = True  # 声明为抽象基类,不会创建实际数据库表


class PrimaryKey:
    """
    主键基础类
    继承自 ModelBase,提供通用的数据库模型方法
    """

    id = Column(Integer, primary_key=True, autoincrement=True, comment='主键ID')


class TimestampMixin:
    """
    时间戳基础类
    继承自 ModelBase,提供通用的数据库模型方法
    """

    created_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')



class CreatorMixin:
    """创建人信息"""
    @declared_attr
    def creator_id(cls):
        return Column(
            Integer,
            ForeignKey("system_users.id", ondelete="SET NULL", onupdate="CASCADE"),
            nullable=True,
            index=True,
            comment="创建人ID"
        )

    @declared_attr
    def creator(cls):
        return relationship(
            "UserModel",
            foreign_keys=[cls.creator_id],
            lazy="joined",
            post_update=True,
            uselist=False
        )


class SoftDeleteMixin:
    """
    软删除基础类
    继承自 ModelBase,提供通用的数据库模型方法
    """

    is_deleted = Column(Boolean, default=False, comment='是否已删除')
