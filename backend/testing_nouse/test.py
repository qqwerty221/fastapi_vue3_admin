import sqlglot

def extract_tables(script):

    parsed_dialog_list = sqlglot.parse(script)
    for i in parsed_dialog_list:
        print(i)
        print(type(i))
        print(i.dump())




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

--------------------------------------------------------------------------------------------
-- 脚本名称:S-HA20_AMH_RENTAL_INDEX_DAILY_SS
-- 脚本功能:伊敦出租指标日报表
-- 分析主题:公寓-出租指标
-- 输入表:    DWD_CMSK.HOEST0160_ORG_ACCOUNT_DIM                组织维表
--           ADS_CMSK_AMH.HOEST0160_R_VACANT_CHECKOUT_DAILY_SS 出租日报表
--           dwd_cmsk.HOEST0160_RENTAL_RATE_PLUS_SS            出租周度数据表
--           DWS_CMSK.HS03_FMCS_TARGET_VALUE_FILLING_PRJ_CS    填报目标值

-- 输出表: ADS_CMSK_AMH.HA20_AMH_RENTAL_INDEX_DAILY_SS   伊敦出租指标日报表
-- 创建时间:2024-06-03
-- 创建人:CPL

-- 修改时间:2024-08-14
-- 填充预留字段值 净租金收入、可出租、已出租目标值字段
-- 创建人:CPL

-- 修改时间：2024-11-27
-- 修改逻辑：1、过滤掉虚拟项目数据  2、取消虚拟房间数据的过滤
-- 创建人：Hulingli
--------------------------------------------------------------------------------------------  

-- 资源参数(复杂逻辑)
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.execution.engine=spark;
set spark.dynamicAllocation.enabled=true;
set spark.dynamicAllocation.maxExecutors=3;
set spark.dynamicAllocation.minExecutors=3;
set spark.executor.memory=4G;
set spark.executor.cores=4;
set spark.app.name = H_HA03_CMSK_MANAGE_COCKPIT_SS:曹鹏磊;
-- set hive.exec.max.dynamic.partitions=1000;  --  这个值需要根据实际情况增加


INSERT OVERWRITE TABLE ADS_CMSK_AMH.HA20_AMH_RENTAL_INDEX_DAILY_SS
SELECT 
 NULL             -- 所属管理主体编码
,ORG_DIM.COMPANY  -- 所属管理主体名称
,GYZX
,公寓中心
,GZGY
,长租公寓
,ORG_DIM.DISTRICT_NAME
,NULL
,ORG_DIM.CITY_NAME
,DATE_DIM.DATE_D
,DATE_DIM.DATE_M
,YEAR(DATE_DIM.DATE_D)
,ORG_DIM.PROJECT_ID
,ORG_DIM.PROJECT_NAME
,DAILY.KCZ_ROOM_NUM
,DAILY.RENTABLE_AREA
,DAILY.RENTED_AREA
,MONTH_DATA.RENTED_AREA
,MONTH_DATA.RENTABLE_AREA
,MONTH_DATA.PRIME_BUSINESS_INCOME
,MONTH_DATA.RENT_AMOUNT
,TARGET_DATA.INCOME_MONTH_TARGET
,TARGET_DATA.NET_RENTAL
,TARGET_DATA.RENTED_AREA
,TARGET_DATA.RENTABLE_AREA
,DATE_DIM.DATE_D
FROM (SELECT DISTINCT COMPANY,DISTRICT_NAME,CITY_NAME,PROJECT_ID,PROJECT_NAME FROM DWD_CMSK.HOEST0160_ORG_ACCOUNT_DIM) ORG_DIM 
JOIN (SELECT M_DATE_CODE AS DATE_D, DATE_FORMAT(M_DATE_CODE, yyyy-MM) AS DATE_M FROM DWD_CMSK.HD00_DATE_DIM_CS WHERE M_DATE_CODE = DATE_SUB(CURRENT_DATE,1)) DATE_DIM -- 维度表方便刷历史数据
LEFT JOIN ( SELECT 
              PER_DAY       -- 数据日期
             ,PROJECT_ID    -- 项目ID
             ,PROJECT_NAME  -- 项目名称
             ,SUM(KCZ_ROOM_NUM)  AS  KCZ_ROOM_NUM  -- 可出租房间数
             ,SUM(RENTED_AREA)   AS  RENTED_AREA   -- 已出租房间面积
             ,SUM(RENTABLE_AREA) AS  RENTABLE_AREA -- 可出租房间面积
            FROM ADS_CMSK_AMH.HOEST0160_R_VACANT_CHECKOUT_DAILY_SS  -- 日报
            WHERE PER_DAY = DATE_SUB(CURRENT_DATE, 1) 
            GROUP BY PER_DAY
                    ,PROJECT_ID
                    ,PROJECT_NAME
          ) DAILY
    ON ORG_DIM.PROJECT_ID = DAILY.PROJECT_ID
   AND DATE_DIM.DATE_D = DAILY.PER_DAY
LEFT JOIN (
            SELECT 
              DATE_FORMAT(ADD_MONTHS(DN, -1), yyyy-MM)  DN   -- 数据日期
             ,PROJECT_ID                                      -- 项目ID
             ,PROJECT_NAME                                    -- 项目名称
             ,SUM(RENTED_AREA)               AS RENTED_AREA             -- 月已出租面积
             ,SUM(RENTABLE_AREA)             AS RENTABLE_AREA           -- 月可出租面积
             ,SUM(PRIME_BUSINESS_INCOME)     AS PRIME_BUSINESS_INCOME   -- 月度营业收入
             ,SUM(RENT_AMOUNT)               AS RENT_AMOUNT             -- 月客房收入
            FROM dwd_cmsk.HOEST0160_RENTAL_RATE_PLUS_SS      
            WHERE DATE_FORMAT(ADD_MONTHS(DN, -1), yyyy-MM) = DATE_FORMAT(DATE_SUB(CURRENT_DATE, 1), yyyy-MM) 
            --  AND house_structure in ('01','03')           -- 2024-11-27修改逻辑，取消虚拟房间的过滤
            GROUP BY DATE_FORMAT(ADD_MONTHS(DN, -1), yyyy-MM)
                    ,PROJECT_ID
                    ,PROJECT_NAME
          ) MONTH_DATA
    ON  ORG_DIM.PROJECT_ID = MONTH_DATA.PROJECT_ID
   AND DATE_DIM.DATE_M = MONTH_DATA.DN
LEFT JOIN (  -- 2024-08-14 补齐 净租金收入、可出租、已出租目标值字段
            SELECT 
             DN 
            ,PROJECT_ID
            ,PROJECT_NAME
            ,INCOME_MONTH_TARGET    -- 收入目标
            ,NET_RENTAL             -- 净租金收入目标
            ,RENTABLE_AREA          -- 可出租目标
            ,RENTED_AREA            -- 已出租目标
            FROM DWS_CMSK.HS03_FMCS_TARGET_VALUE_FILLING_PRJ_CS
            WHERE DN = DATE_FORMAT(DATE_SUB(CURRENT_DATE, 1), yyyy-MM) 
          ) TARGET_DATA
    ON  ORG_DIM.PROJECT_ID = TARGET_DATA.PROJECT_ID
   AND DATE_DIM.DATE_M = TARGET_DATA.DN
where ORG_DIM.COMPANY != '虚拟项目'      -- 2024-11-27新增过滤条件：伊敦日报过滤掉虚拟项目数据
;



"""
sqlglot.pretty = True

aaa = extract_tables(script)

