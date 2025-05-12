import sqlglot
from sqlglot import parse_one

def generate_dot(node, dot=None, parent=None):
    if dot is None:
        dot = ["digraph AST {"]

    if not hasattr(node, "args"):
        dot.append(f'    "{id(node)}" [label="{str(node)}"];')
        return dot  # 避免继续递归非 AST 节点

    node_id = str(id(node))
    dot.append(f'    "{node_id}" [label="{node}"];')

    if parent:
        dot.append(f'    "{parent}" -> "{node_id}";')

    for child in node.args.values():
        if isinstance(child, list):
            for sub_child in child:
                generate_dot(sub_child, dot, node_id)
        elif child:
            generate_dot(child, dot, node_id)

    return dot


# 示例 SQL 语句
sql = """
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

# 解析 SQL 语句
ast = parse_one(sql)

# 生成 DOT 语法
dot_code = generate_dot(ast)
dot_code.append("}")

# 保存为 DOT 文件
dot_filename = "sql_ast.dot"
with open(dot_filename, "w") as f:
    f.write("\n".join(dot_code))

print(f"DOT 文件已保存为 {dot_filename}")


# https://graphviz.christine.website/?engine=dot#digraph%20G%20%7B%0A%0A%20%20subgraph%20cluster_0%20%7B%0A%20%20%20%20style%3Dfilled%3B%0A%20%20%20%20color%3Dlightgrey%3B%0A%20%20%20%20node%20%5Bstyle%3Dfilled%2Ccolor%3Dwhite%5D%3B%0A%20%20%20%20a0%20-%3E%20a1%20-%3E%20a2%20-%3E%20a3%3B%0A%20%20%20%20label%20%3D%20%22process%20%231%22%3B%0A%20%20%7D%0A%0A%20%20subgraph%20cluster_1%20%7B%0A%20%20%20%20node%20%5Bstyle%3Dfilled%5D%3B%0A%20%20%20%20b0%20-%3E%20b1%20-%3E%20b2%20-%3E%20b3%3B%0A%20%20%20%20label%20%3D%20%22process%20%232%22%3B%0A%20%20%20%20color%3Dblue%0A%20%20%7D%0A%20%20start%20-%3E%20a0%3B%0A%20%20start%20-%3E%20b0%3B%0A%20%20a1%20-%3E%20b3%3B%0A%20%20b2%20-%3E%20a3%3B%0A%20%20a3%20-%3E%20a0%3B%0A%20%20a3%20-%3E%20end%3B%0A%20%20b3%20-%3E%20end%3B%0A%0A%20%20start%20%5Bshape%3DMdiamond%5D%3B%0A%20%20end%20%5Bshape%3DMsquare%5D%3B%0A%7D