import psycopg2
import sqlglot

# 连接数据库
conn = psycopg2.connect(
    dbname="fastapi",
    user="fastapi",
    password="fastapi-root",
    host="localhost",
    port="5432"
)
query =
"""
SELECT upper(script_content) 
  FROM public.t_script_info 
 where app_name = 'cmsk_basic'
"""

table_list = [
    'HOEST0106_DATA_WIDE_YS_DYREPORTDATA1_SS'
    ,'HOEST0133_IM_VISITOR_FOLLOW_SS'
    ,'HOEST0111_CARDVOUCHERINFO_NEW_SS'
    ,'HOEST0120_MYWORKFLOWPROCESSENTITY_SS'
    ,'HOEST0161_CUSTOMER_POINT_SUMMARY_SS'
    ,'HOEST0144_XL_ORDER_GOODS_SS'
    ,'HOEST0133_IM_CUSTOMER_SIGN_RECORD_SS'
    ,'HOEST0133_IM_USER_VISITOR_SS'
    ,'HOEST0166_YS_SALEPROFITBYPRODDATA_SS'
    ,'HOEST0134_IM_CUSTOMER_SIGN_RECORD_SS'
    ,'HOEST0133_T_INF_CUSTOMER_POOL_DISTRIBUTION_LOG_SS'
    ,'HOEST0106_DATA_WIDE_YS_SALEPROFITBYPRODDATA_SS'
    ,'HOEST0134_IM_USER_VISITOR_SS'
    ,'HOEST0142_S_GETIN_SS'
    ,'HOEST0110_TB_PLATFORM_FINANCIAL_VOUCHER_ITEM_SS'
    ,'HOEST0166_YS_VERCOSTTARGET_SS'
    ,'HOEST0133_IM_USER_VISITOR_CUSTOMER_INFO_SS'
    ,'HOEST0142_S_VOUCHER_SS'
    ,'HOEST0109_TBB_QUES_RECORD_SS'
    ,'HOEST0167_T_CSL_ITEMDATAENTRY_SS'
]

cur = conn.cursor()
# 查询数据表
cur.execute(query)

rows = cur.fetchall()

# 使用 sqlglot 解析字段内容
for row in rows:
    parsed_sql = sqlglot.parse(row[0])
    print(parsed_sql)

# 关闭数据库连接
cur.close()
conn.close()
