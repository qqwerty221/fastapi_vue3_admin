import sqlglot


def extract_tables(script):
    # 获取分区表分区字段
    parsed_dialog_list = sqlglot.parse(script)
    for i in parsed_dialog_list:
        if isinstance(i, sqlglot.exp.Insert):
            if 'partition' in i.args['this'].args:
                print(",".join([col_name.name for col_name in i.args['this'].args['partition'].args['expressions']]))
            if isinstance(i.args['expression'], sqlglot.exp.Select):
                print(i.args['expression'].selects[-1].this)

    # 获取



    # # 提取来源数据表（FROM、JOIN等）
    # for table in parsed.find_all(sqlglot.exp.Table):
    #     if not table.alias:  # 过滤掉别名
    #         source_tables.add(table.name)
    #
    # # 提取目标数据表（INSERT、UPDATE、CREATE）
    # for stmt in parsed.find_all(sqlglot.exp.Insert) + parsed.find_all(sqlglot.exp.Update) + parsed.find_all(sqlglot.exp.Create):
    #     if stmt.this:
    #         target_tables.add(stmt.this.name)
    #
    # return list(source_tables), list(target_tables)


# 示例 HQL 语句
script = """

-- 功能：数据入湖历史表stg到ods
-- 作者: modalin
-- 创建日期：2022-08-05
-- 修改日期：
-- 返回值：无
-- 脚本名称: HQL_P_ODS_CMSK_HOEST0165_SYS_LOG_SS

set
  hive.exec.dynamic.partition.mode = nonstrict;
set
  hive.auto.convert.join = true;

create table if not exists  ods_cmsk.HOEST0165_USER_VISIT_HISTORY_SS ( 
  ID    string  COMMENT '主键',
  IP    string  COMMENT '记录访问的ip信息',
  VISIT_TYPE    string  COMMENT '0是pc  1是移动',
  STAFF_USER_ID     string  COMMENT '用户ID 用户ID,sys_staff表的ID字段',
  STAFF_USER_NAME   string ,
  FIRST_CODE    string  COMMENT '第一层级菜单',
  FIRST_NAME    string  COMMENT '第一层级菜单名称',
  MENU_CODE     string  COMMENT '菜单代码 菜单代码',
  MENU_NAME     string  COMMENT '菜单名字',
  CREATE_DAY string ,
  REVISION    string  COMMENT '乐观锁',
  CREATED_BY string  ,
  CREATE_TIME     string  COMMENT '创建时间',
  UPDATED_BY  string ,
  UPDATE_TIME     string  COMMENT '更新时间' ,

datasource string ,
DW_SNSH_DT  string
)     COMMENT   '系统日志' partitioned by(p_day string) row format delimited fields terminated by '\u0001' stored as rcfile;
with aaa as (
    select * from nbbb
    left join ccc on 1=2
)
INSERT overwrite TABLE ods_cmsk.HOEST0165_USER_VISIT_HISTORY_SS PARTITION(p_day)
SELECT
   ID 
  ,IP 
  ,VISIT_TYPE 
  ,STAFF_USER_ID 
  ,STAFF_USER_NAME 
  ,FIRST_CODE 
  ,FIRST_NAME 
  ,MENU_CODE 
  ,MENU_NAME 
  ,CREATE_DAY 
  ,REVISION 
  ,CREATED_BY 
  ,CREATE_TIME 
  ,UPDATED_BY 
  ,UPDATE_TIME 
,
'stg_cmsk.HGEST0165_USER_VISIT_HISTORY' as datasource,
CURRENT_TIMESTAMP as DW_SNSH_DT,
  DATE_FORMAT(CURRENT_DATE,'yyyy-MM') AS P_DAY
FROM(
select * from stg_cmsk.HGEST0165_USER_VISIT_HISTORY where 1=3) t
left join ddd on d=g

"""
sqlglot.pretty = True

aaa = extract_tables(script)
