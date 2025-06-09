import traceback

import sqlglot


def extract_tables(script):
    # 获取分区表分区字段
    try:

        for database_type in ['postgres', 'hive',  'mysql']:
            try:
                parsed_dialog_list = sqlglot.parse(script,dialect=database_type)
                if parsed_dialog_list:
                    break
            except Exception as e:
                print(database_type)
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
                        source_tables.add(node.name)
                    if isinstance(node, (sqlglot.exp.CTE, sqlglot.exp.Subquery)):
                        cte_to_remove.add(node.alias)
                    if isinstance(node, (sqlglot.exp.Insert, sqlglot.exp.Create)):
                        target_tables.add(node.this.this.name)

                source_tables = source_tables - target_tables - cte_to_remove
            print(source_tables,target_tables,cte_to_remove)
    except Exception as e:
        error_message = traceback.format_exc()
        print(f"异常原因: {error_message}")
        print(111)


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
-- 脚本名称:H_HA03_CMSK_MANAGE_COCKPIT_SS
-- 脚本功能:管理驾驶舱数据表
-- 分析主题:公寓-财务
-- 输入表: DWS_CMSK.HS03_MANAGE_COCKPIT_TIME_M_CS       -- 单项目利润表(实时数-月度)

    -- DWS_CMSK.HS03_MANAGE_COCKPIT_TIME_M_CS        单项目利润表(实时数当月数)       
    -- DWS_CMSK.HS03_MANAGE_COCKPIT_TIME_Q_CS        单项目利润表(实时数当季数)
    -- DWS_CMSK.HS03_MANAGE_COCKPIT_TIME_Y_CS        单项目利润表(实时数当年数)
    -- DWS_CMSK.HS03_MANAGE_COCKPIT_FINAL_M_CS       单项目利润表(定版数当月数)
    -- DWS_CMSK.HS03_MANAGE_COCKPIT_FINAL_Q_CS       单项目利润表(定版数当季数)
    -- DWS_CMSK.HS03_MANAGE_COCKPIT_FINAL_Y_CS       单项目利润表(定版数当年数)
-- 输出表: ADS_CMSK_AMH.HA03_CMSK_MANAGE_COCKPIT_SS   管理驾驶舱数据表
-- 创建时间:2024-03-01
-- 创建人:CPL

-- 修改时间 2024-07-17 月的和季度不展示年度预算

-- 修改时间 2024-11-28 季度和年度的预算EBITDA和净利润的预算值展示并且定版月度的数据新增了很多字段
--------------------------------------------------------------------------------------------  

-- 资源参数(复杂逻辑)
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.execution.engine=spark;
set spark.dynamicAllocation.enabled=true;
set spark.dynamicAllocation.maxExecutors=8;
set spark.dynamicAllocation.minExecutors=8;
set spark.executor.memory=4G;
set spark.executor.cores=4;
set hive.support.quoted.identifiers=none; 
set spark.app.name = H_HA03_CMSK_MANAGE_COCKPIT_SS:曹鹏磊;

CREATE TABLE IF NOT EXISTS ADS_CMSK_AMH.HA03_CMSK_MANAGE_COCKPIT_SS(
 date_month             string             COMMENT '年月'
,district_name          string             COMMENT '片区'     
,stat_type              string             COMMENT '统计类型: 1 ,2 ,3 月、季 、年'
,project_id             string             COMMENT '项目编码'
,project_name           string             COMMENT '项目名称'
,city_id                string             COMMENT '城市编码'
,city_name              string             COMMENT '城市名称'
,province_id            string             COMMENT '省份编码'
,province_name          string             COMMENT '省份名称'
,form_type_id           string             COMMENT '业态编码'   
,form_type_name         string             COMMENT '业态名称'   
,operation_pattern      string             COMMENT '资产类型' 
,project_stage          string             COMMENT '项目阶段'

,revenue                DECIMAL(38, 6)     COMMENT '营业收入'   
,revenue_budget         DECIMAL(38, 6)     COMMENT '营业收入预算'
,revenue_yoy            DECIMAL(38, 6)     COMMENT '去年同期营业收入'
,revenue_qoq            DECIMAL(38, 6)     COMMENT '上月营业收入'

,main_revenue           DECIMAL(38, 6)     COMMENT '主营业收入-财务'  
,main_revenue_budget    DECIMAL(38, 6)     COMMENT '主营业收入-财务预算'
,main_revenue_yoy       DECIMAL(38, 6)     COMMENT '去年同期主营业收入-财务'
,main_revenue_qoq       DECIMAL(38, 6)     COMMENT '上月主营业收入-财务'

,ebitda                 DECIMAL(38, 6)     COMMENT 'EBITDA'
,ebitda_budget          DECIMAL(38, 6)     COMMENT 'EBITDA预算'
,ebitda_yoy             DECIMAL(38, 6)     COMMENT '去年同期EBITDA'
,ebitda_qoq             DECIMAL(38, 6)     COMMENT '上月EBITDA'

,profit_net             DECIMAL(38, 6)     COMMENT '净利润'
,profit_net_budget      DECIMAL(38, 6)     COMMENT '净利润预算'

,account_receivable     DECIMAL(38, 6)     COMMENT '应收账款'

,gop                    DECIMAL(38, 6)     COMMENT 'GOP'
,gop_budget             DECIMAL(38, 6)     COMMENT 'GOP预算'
,gop_yoy                DECIMAL(38, 6)     COMMENT '去年同期GOP'

,cost_operation         DECIMAL(38, 6)     COMMENT '营业成本'
,cost_operation_budget  DECIMAL(38, 6)     COMMENT '营业成本预算'

,room_area_ava          DECIMAL(38, 6)     COMMENT '可出租面积'
,room_num_ava           DECIMAL(38, 6)     COMMENT '可出租房间数量'
,room_area_ava_yoy      DECIMAL(38, 6)     COMMENT '去年同期可出租面积'
,room_num_ava_yoy       DECIMAL(38, 6)     COMMENT '去年同期可出租房间数量'

,room_area_use          DECIMAL(38, 6)     COMMENT '已出租面积'
,room_num_use           DECIMAL(38, 6)     COMMENT '已出租房间数量'
,room_area_use_budget   DECIMAL(38, 6)     COMMENT '已出租面积预算'
,room_num_use_budget    DECIMAL(38, 6)     COMMENT '已出租房间数量预算'
,room_area_use_yoy      DECIMAL(38, 6)     COMMENT '去年同期已出租面积'
,room_num_use_yoy       DECIMAL(38, 6)     COMMENT '去年同期已出租房间数量'
,asset_original_value   DECIMAL(38, 6)     COMMENT '运营规模（资产原值）'
,rent_income            DECIMAL(38, 6)     COMMENT '客房收入'
,rent_income_yoy        DECIMAL(38, 6)     COMMENT '去年同期客房收入'
,rent_income_budget     DECIMAL(38, 6)     COMMENT '客房收入预算'
,data_source            STRING             COMMENT '数据来源'
,etl_ts                 STRING             COMMENT '数据入表时间'
)partitioned by (P_MONTH STRING COMMENT '按月分区');

-- ALTER TABLE DWD_CMSK.HA03_CMSK_MANAGE_COCKPIT_SS ADD COLUMNS(
--      profit_net_qoq              DECIMAL(18,2)   COMMENT '净利润上月'  
--     ,profit_net_yoy              DECIMAL(18,2)   COMMENT '净利润去年同期'  
-- );

-- 2024-11-26新增字段
-- ALTER TABLE ADS_CMSK_AMH.HA03_CMSK_MANAGE_COCKPIT_SS ADD COLUMNS(
--       room_area_ava_budget              DECIMAL(38, 6)   COMMENT '可出租面积预算'
--      ) cascade;



-- 先插入实时数
INSERT OVERWRITE TABLE ADS_CMSK_AMH.HA03_CMSK_MANAGE_COCKPIT_SS
SELECT
 date_month
,district_name
,stat_type
,project_id           
,project_name         
,city_id              
,city_name            
,province_id          
,province_name        
,form_type_id         
,form_type_name       
,asset_type_name
,asset_phase_name    
,revenue              
,revenue_budget       
,revenue_yoy          
,revenue_qoq          
,main_revenue         
,main_revenue_budget  
,main_revenue_yoy     
,main_revenue_qoq     
,ebitda               
-- ,IF(stat_type = '3',ebitda_budget,0)
,ebitda_budget    
,ebitda_yoy           
,ebitda_qoq           
,profit_net           
-- ,IF(stat_type = '3',profit_net_budget,0)
,profit_net_budget
,account_receivable   
,gop                  
,gop_budget           
,gop_yoy              
,cost_operation       
,cost_operation_budget
,room_area_ava        
,room_num_ava         
,room_area_ava_yoy    
,room_num_ava_yoy     
,room_area_use        
,room_num_use         
,room_area_use_budget 
,room_num_use_budget  
,room_area_use_yoy    
,room_num_use_yoy     
,asset_original_value 
,rent_income          
,rent_income_yoy      
,rent_income_budget  
,'实时数'
,current_timestamp
,null field_1
,room_area_ava_budget
,date_month
FROM ( SELECT  (NET_RENTAL_YEAR_APPROVED|NET_RENTAL_YEAR_INTERNAL|RENTABLE_AREA_YEAR|RENTED_AREA_YEAR|EBITDA_YEAR_APPROVED|EBITDA_YEAR_INTERNAL|EBITDA_NET_PROFIT_APPROVED|EBITDA_NET_PROFIT_INTERNAL|GOP_YEAR_APPROVED|GOP_YEAR_INTERNAL|revenue_qom|main_revenue_qom|ebitda_qom|profit_net_qom)?+.+  FROM DWS_CMSK.HS03_MANAGE_COCKPIT_TIME_M_CS
       UNION ALL 
       SELECT * FROM DWS_CMSK.HS03_MANAGE_COCKPIT_TIME_Q_CS
       UNION ALL 
       SELECT * FROM DWS_CMSK.HS03_MANAGE_COCKPIT_TIME_Y_CS
     ) t_time
;


-- 有定版数使用定版数，用定版数覆盖实时数
INSERT OVERWRITE TABLE ADS_CMSK_AMH.HA03_CMSK_MANAGE_COCKPIT_SS
SELECT
 date_month
,district_name
,stat_type
,project_id           
,project_name         
,city_id              
,city_name            
,province_id          
,province_name        
,form_type_id         
,form_type_name       
,asset_type_name
,asset_phase_name  
,revenue              
,revenue_budget       
,revenue_yoy          
,revenue_qoq          
,main_revenue         
,main_revenue_budget  
,main_revenue_yoy     
,main_revenue_qoq     
,ebitda               
-- ,IF(stat_type = '3',ebitda_budget,0)
,ebitda_budget
,ebitda_yoy           
,ebitda_qoq           
,profit_net           
-- ,IF(stat_type = '3',profit_net_budget,0)
,profit_net_budget
,account_receivable   
,gop                  
,gop_budget           
,gop_yoy              
,cost_operation       
,cost_operation_budget
,room_area_ava        
,room_num_ava         
,room_area_ava_yoy    
,room_num_ava_yoy     
,room_area_use        
,room_num_use         
,room_area_use_budget 
,room_num_use_budget  
,room_area_use_yoy    
,room_num_use_yoy     
,asset_original_value 
,rent_income          
,rent_income_yoy      
,rent_income_budget  
,'定版数'
,current_timestamp
,null field_1
,room_area_ava_budget
,date_month
FROM ( SELECT  (NET_RENTAL_YEAR_APPROVED|NET_RENTAL_YEAR_INTERNAL|RENTABLE_AREA_YEAR|RENTED_AREA_YEAR|EBITDA_YEAR_APPROVED|EBITDA_YEAR_INTERNAL|EBITDA_NET_PROFIT_APPROVED|EBITDA_NET_PROFIT_INTERNAL|GOP_YEAR_APPROVED|GOP_YEAR_INTERNAL|revenue_budget_Q|main_revenue_budget_Q|ebitda_budget_Q|profit_net_budget_Q|gop_budget_Q|rent_income_budget_Q|room_area_ava_budget_Q|revenue_budget_y|main_revenue_budget_y|room_area_use_budget_Q|revenue_qom|main_revenue_qom|ebitda_qom|profit_net_qom)?+.+  
       FROM DWS_CMSK.HS03_MANAGE_COCKPIT_FINAL_M_CS
       UNION ALL 
       SELECT * FROM DWS_CMSK.HS03_MANAGE_COCKPIT_FINAL_Q_CS
       UNION ALL 
       SELECT * FROM DWS_CMSK.HS03_MANAGE_COCKPIT_FINAL_Y_CS
     ) t_final
;
"""
sqlglot.pretty = True

aaa = extract_tables(script)
