create schema metadata ;
alter schema metadata to fastapi ;

CREATE TABLE metadata.column_meta (
                                     id SERIAL PRIMARY KEY,
                                     db_ip VARCHAR(50),
                                     port INTEGER,
                                     dbase_name VARCHAR(255),
                                     dbase_schema VARCHAR(255),
                                     table_name VARCHAR(255),
                                     table_comment VARCHAR(255),
                                     tb_field VARCHAR(255),
                                     field_type VARCHAR(255),
                                     field_comment VARCHAR(255),
                                     is_partitions VARCHAR(255),
                                     is_index VARCHAR(255)
);

drop table if exists metadata.table_meta ;
create table metadata.table_meta (
                                      id SERIAL PRIMARY KEY,
                                      table_name VARCHAR(255),
                                      table_comment VARCHAR(255),
                                      is_partitions VARCHAR(255),
                                      is_index VARCHAR(255)
) ;

drop table if exists metadata.system_meta ;
create table metadata.system_meta (
                                      id SERIAL PRIMARY KEY,
                                      db_type VARCHAR(50),
                                      db_ip VARCHAR(50),
                                      port INTEGER,
                                      dbase_name VARCHAR(255),
                                      dbase_schema VARCHAR(255),
                                      sys_name TEXT
) ;


drop table if exists metadata.module_meta ;
create table metadata.module_meta (
                                      id SERIAL PRIMARY KEY,
                                      system_id INTEGER,
                                      module_name varchar(200),
                                      sub1 varchar(200),
                                      sub2 varchar(200),
                                      sub3 varchar(200),
                                      sub4 varchar(200),
                                      sub5 varchar(200),
                                      table_arr Integer[]
) ;

select * from metadata.column_meta ;
select * from metadata.system_meta ;
select * from metadata.table_meta  ;
select * from metadata.module_meta ;

insert into metadata.table_meta(table_name, table_comment, is_partitions, is_index)
select table_name
      ,table_comment
      ,sum(case when is_partitions = '是' then 1 else 0 end) as is_partitions
      ,sum(case when is_index = '是' then 1 else 0 end)      as is_index
  from metadata.column_meta
 group by table_name
        ,table_comment;
commit;

select *
  from metadata.column_meta t
 where t.table_name = 'zz_sys_perm_code_0609' ;


select *
  from metadata.table_meta
 where lower(table_name) in (
     'ha01_xsrb_qyqs_ss'


     ) ;


with module as (
select t.*,unnest(t.table_arr) as table_name
  from metadata.module_meta t)
select s.sys_name
      ,m.module_name,m.sub1,m.sub2,m.sub3,m.sub4
      ,concat(s.dbase_schema,'.',t.table_name) as table_name
      ,t.table_comment
  from module m
left join metadata.table_meta t on m.table_name = t.id
left join metadata.system_meta s on m.system_id = s.id ;


select *
  from public.t_dialog_table_list ;


   select *
     from parser.object_decendant od
left join public.t_dialog_table_list dtl
       on od.decendant_name = dtl.job_name;