"""1

Revision ID: 1ac102039db2
Revises: 
Create Date: 2025-04-25 15:50:04.014228

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1ac102039db2'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        't_script_info',
        sa.Column('id', sa.Integer(), primary_key=True, comment='主键ID'),
        sa.Column('app_name', sa.String(50), nullable=False, comment='所属应用名称'),
        sa.Column('script_name', sa.String(100), nullable=False, comment='脚本名称'),
        sa.Column('script_path', sa.String(255), nullable=False, comment='脚本路径'),
        sa.Column('script_content', sa.Text(), nullable=False, comment='脚本内容'),
        sa.Column('is_parsed', sa.Boolean(), default=False, comment='是否已解析'),
        sa.Column('is_deleted', sa.Boolean(), default=False, comment='是否已删除'),
        sa.Column('update_time', sa.DateTime, default=False, comment='更新时间'),
        sa.Column('update_by', sa.String(50), default=False, comment='更新人'),
    )


def downgrade() -> None:
    pass
