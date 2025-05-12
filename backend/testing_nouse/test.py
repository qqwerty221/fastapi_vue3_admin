import sqlglot

def extract_tables(script):

    parsed_dialog_list = sqlglot.parse(script)
    for dialog in parsed_dialog_list:
        print(dialog.__class__.__name__)




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
insert into aaa as select * from bbb ;
with gcte as (
SELECT a.id, b.name, c.salary
FROM employees a
JOIN departments b ON a.dept_id = b.id
JOIN salaries c ON a.id = c.emp_id
)
   select d.name as new_name
         ,d.dept_id
         ,max(f.name) as ori_name
     from deploy d
left join fired f
       on d.location = f.address
    group by d.name 
         ,d.dept_id
    union all
   select e.first_name
         ,e.new_id
         ,g.name
     from(select * from escolate where 1=2 ) e
     join gcte g
       on e.new_id = g.id
    where exists(
   select t.position
         ,t.id
     from employees t
    where t.id = e.id) ;
"""
sqlglot.pretty = True

aaa = extract_tables(script)

