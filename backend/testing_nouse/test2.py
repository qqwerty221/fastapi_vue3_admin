import sqlglot
from sqlglot.lineage import lineage

# 定义 SQL 语句
sql_query = """
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
     from escolate e
     join gcte g
       on e.new_id = g.id
    where exists(
   select t.position
         ,t.id
     from employees t
    where t.id = e.id)
"""

# 解析 SQL 并获取血缘信息
lineage_info = lineage("new_name", sql_query)

print(lineage_info)
# 输出血缘信息
print("目标表:", lineage_info.targets)
print("来源表:", lineage_info.sources)
print("字段血缘关系:")
for column, sources in lineage_info.columns.items():
    print(f"{column} <- {sources}")
