-- truncate table parser.controlm_attributes ;
-- truncate table parser.controlm_objects ;
truncate table parser.object_impact ;

-- 关联CTM对象与属性
drop view if exists parser.controlm_object_with_attr cascade;
create view parser.controlm_object_with_attr
            (id, parent_id, sub_application, object_type, general_name, dependency_name, hierarchy_level,
             position_in_parent, json_object_agg, from_object, to_object)
as
WITH frame AS (SELECT t_1.id,
                      t_1.parent_id,
                      t_1.object_type,
                      t_1.name,
                      t_1.hierarchy_level,
                      t_1.position_in_parent,
                      json_object_agg(t1.attribute_name, t1.attribute_value) AS json_object_agg,
                      COALESCE(json_object_agg(t1.attribute_name, t1.attribute_value) ->> 'SUB_APPLICATION'::text,
                               NULL::text)                                   AS sub_application,
                      COALESCE(json_object_agg(t1.attribute_name, t1.attribute_value) ->> 'JOBNAME'::text,
                               NULL::text)                                   AS jobname,
                      COALESCE(json_object_agg(t1.attribute_name, t1.attribute_value) ->> 'FOLDER_NAME'::text,
                               NULL::text)                                   AS foldername,
                      CASE
                          WHEN t_1.object_type = 'INCOND'::text THEN split_part(t_1.name, '-TO-'::text, 1)
                          ELSE NULL::text
                          END                                                AS from_object,
                      CASE
                          WHEN t_1.object_type = 'OUTCOND'::text THEN split_part(t_1.name, '-TO-'::text, 2)
                          ELSE NULL::text
                          END                                                AS to_object
               FROM parser.controlm_objects t_1
                        LEFT JOIN parser.controlm_attributes t1 ON t_1.id = t1.object_id
               GROUP BY t_1.id, t_1.parent_id, t_1.object_type, t_1.name, t_1.hierarchy_level, t_1.position_in_parent)
SELECT id,
       parent_id,
       sub_application,
       object_type,
       CASE
           WHEN name = ''::text THEN COALESCE(foldername, jobname)
           ELSE name
           END                       AS general_name,
       COALESCE(foldername, jobname) AS dependency_name,
       hierarchy_level,
       position_in_parent,
       json_object_agg,
       from_object,
       to_object
FROM frame t;

-- 文件夹表
drop view if exists parser.folders cascade ;
create or replace view parser.folders as
select t.id
     ,t.parent_id
     ,t.sub_application
     ,t.object_type
     ,t.general_name
     ,t.json_object_agg->>'TIMEFROM'              as time_from
     ,t.json_object_agg->>'CYCLIC_TIMES_SEQUENCE' as schedule_time
     ,t.json_object_agg->>'CYCLIC'                as cyclic
     ,t.json_object_agg->>'CYCLIC_TYPE'           as CYCLIC_TYPE
     ,t.json_object_agg
from parser.controlm_object_with_attr t
where t.object_type in ('FOLDER','SUB_FOLDER','SMART_FOLDER') ;
select * from parser.folders
where id = 296475;

-- 作业表
drop view if exists parser.jobs cascade ;
create or replace view parser.jobs as
select t.id
     ,t.parent_id
     ,t.sub_application
     ,t.object_type
     ,t.general_name
     ,t.json_object_agg->>'TIMEFROM'              as time_from
     ,t.json_object_agg->>'CYCLIC_TIMES_SEQUENCE' as schedule_time
     ,t.json_object_agg->>'CYCLIC'                as cyclic
     ,t.json_object_agg->>'CYCLIC_TYPE'           as CYCLIC_TYPE
     ,t.json_object_agg->>'APPL_TYPE'             as APPL_TYPE
     ,concat(t.json_object_agg->>'JAN'
    ,t.json_object_agg->>'FEB'
    ,t.json_object_agg->>'MAR'
    ,t.json_object_agg->>'APR'
    ,t.json_object_agg->>'MAY'
    ,t.json_object_agg->>'JUN'
    ,t.json_object_agg->>'JUL'
    ,t.json_object_agg->>'AUG'
    ,t.json_object_agg->>'SEP'
    ,t.json_object_agg->>'OCT'
    ,t.json_object_agg->>'NOV'
    ,t.json_object_agg->>'DEC') as calender
     ,t.json_object_agg
from parser.controlm_object_with_attr t
where t.object_type in ('JOB') ;
select * from parser.jobs
where parent_id = 248571;

-- 调度日历表
drop view if exists parser.rbc cascade ;
create or replace view parser.rbc as
select t.id
     ,t.parent_id
     ,t.object_type
     ,t.json_object_agg->>'NAME' as rbc_type
     ,t.json_object_agg->>'SHIFT' as SHIFT
     ,t.json_object_agg->>'LEVEL' as LEVEL
     ,concat(t.json_object_agg->>'JAN'
    ,t.json_object_agg->>'FEB'
    ,t.json_object_agg->>'MAR'
    ,t.json_object_agg->>'APR'
    ,t.json_object_agg->>'MAY'
    ,t.json_object_agg->>'JUN'
    ,t.json_object_agg->>'JUL'
    ,t.json_object_agg->>'AUG'
    ,t.json_object_agg->>'SEP'
    ,t.json_object_agg->>'OCT'
    ,t.json_object_agg->>'NOV'
    ,t.json_object_agg->>'DEC') as calender
     ,t.json_object_agg
from parser.controlm_object_with_attr t
where t.object_type in ('RULE_BASED_CALENDAR', 'RULE_BASED_CALENDARS') ;
select * from parser.rbc
where parent_id = 249098;

-- 上下游关系
drop view if exists parser.io_cond cascade;
create or replace view parser.io_cond as
with cond_list as (select t.id
                        , t.parent_id
                        , case
                              when t.object_type = 'INCOND' then split_part(t.json_object_agg ->> 'NAME', '-TO-', 1)
                              else null end as from_object
                        , case
                              when t.object_type = 'OUTCOND' then split_part(t.json_object_agg ->> 'NAME', '-TO-', 2)
                              else null end as to_object
                   from parser.controlm_object_with_attr t
                   where t.object_type in ('INCOND', 'OUTCOND'))
select t.parent_id
     ,string_to_array(string_agg(t.from_object,','),',') as from_object
     ,string_to_array(string_agg(t.to_object,','),',')   as to_object
from cond_list t
left join parser.controlm_object_with_attr t1 on t.parent_id = t1.id
where t1.general_name <> coalesce(t.from_object,'0')
  and t1.general_name <> coalesce(t.to_object,'0')
group by t.parent_id ;
select * from parser.io_cond t where t.parent_id = 349391;


-- 文件夹 详细信息
drop view if exists parser.folder_extend cascade ;
create or replace view parser.folder_extend as
select t.id
     ,t.parent_id
     ,t.sub_application
     ,t.object_type
     ,t.general_name
     ,t.schedule_time
     ,t.cyclic
     ,t.CYCLIC_TYPE
     ,t.time_from
     ,t1.rbc_type
     ,t1.level
     ,t2.from_object
     ,t2.to_object
     ,case when t1.rbc_type in ('None','none','NONE','!DAILY')
             or t1.shift = 'Ignore Job'
             or t1.calender = '000000000000'
           then 0
           else 1
            end as is_schedule
     ,t1.rbc_type as folder_rbc_type
     ,'' as APPL_TYPE
FROM parser.folders t
         left join parser.rbc t1     on t.id = t1.parent_id
         left join parser.io_cond t2 on t.id = t2.parent_id ;
select * from parser.folder_extend t;

-- 作业 详细信息
drop view if exists parser.job_extend cascade ;
create or replace view parser.job_extend as
select t.id
     ,t.parent_id
     ,t.sub_application
     ,t.object_type
     ,t.general_name
     ,t.schedule_time
     ,t.cyclic
     ,t.CYCLIC_TYPE
     ,t.time_from
     ,t1.rbc_type
     ,t1.level
     ,t2.from_object
     ,t2.to_object
     ,case when t1.rbc_type is null
    or t1.rbc_type in ('None','none','NONE','!DAILY')
    or t1.shift = 'Ignore Job'
    or t.calender = '000000000000'
    or t1.calender = '000000000000'
    or(t3.level = 'Y' and t3.is_schedule = 0)
               then 0
           else 1 end as is_schedule
     ,t3.rbc_type as folder_rbc_type
     ,t.APPL_TYPE
from parser.jobs t
         left join parser.rbc t1 on t.id = t1.parent_id
         left join parser.io_cond t2 on t.id = t2.parent_id
         left join parser.folder_extend t3 on t.parent_id = t3.id;
select * from parser.job_extend ;

-- 所有对象 物化
DROP materialized view if exists parser.object_extend cascade;
create materialized view parser.object_extend as
SELECT job_extend.id,
       job_extend.parent_id,
       job_extend.sub_application,
       job_extend.object_type,
       job_extend.general_name,
       job_extend.schedule_time,
       job_extend.cyclic,
       job_extend.cyclic_type,
       job_extend.time_from,
       job_extend.rbc_type,
       job_extend.level,
       job_extend.from_object,
       job_extend.to_object,
       job_extend.is_schedule,
       job_extend.folder_rbc_type,
       job_extend.appl_type
FROM parser.job_extend
UNION ALL
SELECT folder_extend.id,
       folder_extend.parent_id,
       folder_extend.sub_application,
       folder_extend.object_type,
       folder_extend.general_name,
       folder_extend.schedule_time,
       folder_extend.cyclic,
       folder_extend.cyclic_type,
       folder_extend.time_from,
       folder_extend.rbc_type,
       folder_extend.level,
       folder_extend.from_object,
       folder_extend.to_object,
       folder_extend.is_schedule,
       folder_extend.folder_rbc_type,
       folder_extend.appl_type
FROM parser.folder_extend
WITH NO DATA;
alter materialized view parser.object_extend owner to fastapi;
REFRESH MATERIALIZED VIEW parser.object_extend;

-- 对象关系链
drop view if exists parser.object_chain cascade ;
create view parser.object_chain as
WITH RECURSIVE cte AS (
    SELECT t.id,
           t.parent_id,
           t.sub_application,
           t.object_type,
           t.general_name,
           t.id::varchar as ids
    FROM parser.object_extend t
    WHERE t.object_type IN ('FOLDER', 'SUB_FOLDER', 'SMART_FOLDER')
      AND t.parent_id = 248001

    UNION ALL

    SELECT t.id,
           t.parent_id,
           t.sub_application,
           t.object_type,
           t.general_name,
           concat(t1.ids,',',t.id) as ids
    FROM parser.object_extend t
             JOIN cte t1 ON t1.id = t.parent_id
)
select * from cte ;
select * from parser.object_chain ;

-- 分析作业对下游的影响
drop function if exists parser.impact_analyse ;
create function parser.impact_analyse(job_id integer, sub_app text)
    returns TABLE(id integer)
    language plpgsql
as
$$
BEGIN
RETURN QUERY
    WITH RECURSIVE cte AS (
            SELECT t.id,
                   t.parent_id,
                   t.sub_application,
                   t.object_type,
                   t.general_name,
                   t.from_object,
                   t.to_object,
                   t.general_name as depend_chain
            FROM parser.object_extend t
--     WHERE t.from_object is null
            where t.id = job_id
            UNION ALL

            SELECT t.id,
                   t.parent_id,
                   t.sub_application,
                   t.object_type,
                   t.general_name,
                   t.from_object,
                   t.to_object,
                   concat(t1.depend_chain,',',t.general_name) as depend_chain
             FROM parser.object_extend t
             JOIN(select unnest(string_to_array(sub_app,',')) as apps) t2
               ON t.sub_application = case when sub_app = 'ALL' then t.sub_application else t2.apps end
             JOIN cte t1 ON t1.general_name = any(t.from_object)
        )
    select distinct t.id::integer
      from cte t
     order by 1 ;
END;
$$;
alter function parser.impact_analyse(integer, text ) owner to fastapi;
select * from parser.impact_analyse(276004, 'ALL' ) ;

-- 对象影响分析表
DROP TABLE IF EXISTS parser.object_impact CASCADE;

CREATE TABLE parser.object_impact (
                                      obj_id        INTEGER PRIMARY KEY,
                                      decendant_ids INTEGER[]
);

ALTER TABLE parser.object_impact OWNER TO fastapi;





select t1.object_type,count(*)
from parser.folder_extend t
         left join parser.controlm_objects t1
                   on t.parent_id = t1.id
group by t1.object_type ;

select t1.object_type,count(*)
from parser.job_extend t
         left join parser.controlm_objects t1
                   on t.parent_id = t1.id
group by t1.object_type ;

select * from parser.object_extend t where 'JOB_HQL_P_ODS_CMSK_HOEST0106_ATA_WIDE_HZ_DATAPHOTODETAIL_NEW_SS' = any(t.from_object)
select * from parser.object_extend where general_name like '%JOB_HZ_IF_EXISTS_DATA_CD%' ;

select *
from parser.object_extend t
--  where t.general_name = 'JOB_HQL_P_ODS_CMSK_HOEST0141_BASE_PORT_SS_CD'
where 'JOB_HQL_P_ODS_CMSK_HOEST0141_BASE_PORT_SS_CD' =  any(t.from_object) ;

drop view parser.object_decendant ;
create view parser.object_decendant as
with frame as (select t.obj_id, unnest(t.decendant_ids) as decendant_ids
               from parser.object_impact t)
   , all_data as (
    select t.obj_id,t1.sub_application,t1.general_name as obj_name,t1.object_type,t2.general_name as decendant_name,t2.object_type,t2.id as decendant_id
    from frame t
             left join parser.object_extend t1 on t.obj_id = t1.id
             left join parser.object_extend t2 on t.decendant_ids = t2.id
)
select t.obj_id,t.sub_application,t.obj_name,string_agg(t.decendant_name,',' order by t.decendant_id)
from all_data t
group by t.obj_id,t.sub_application, t.obj_name ;
select * from parser.object_decendant ;