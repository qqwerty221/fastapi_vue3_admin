from py2neo import Graph, Node, Relationship, Subgraph
from py2neo.matching import NodeMatcher

# 1️⃣ 建立连接
graph = Graph("bolt://10.124.160.153:7687", auth=("neo4j", "password123"))
# 注意：确保 Neo4j 服务已启动，端口和密码正确

# 2️⃣ 创建节点
alice = Node("Person", name="Alice")
bob = Node("Person", name="Bob")
# 标签为 "Person"，附带属性 name

# 3️⃣ 创建关系
friend = Relationship(alice, "KNOWS", bob)
# 表示 Alice -> Bob 的 “KNOWS” 关系

# 4️⃣ 打包为子图并写入数据库
subgraph = Subgraph([alice, bob], [friend])
graph.create(subgraph)

# 5️⃣ 查询节点
matcher = NodeMatcher(graph)
alice_node = matcher.match("Person", name="Alice").first()
print("🔍 查询到的节点：", dict(alice_node))

# 6️⃣ 更新属性
alice_node["age"] = 30
graph.push(alice_node)
print("✅ 已更新 Alice 的年龄属性")

# 7️⃣ 执行自定义 Cypher 查询
result = graph.run("MATCH (p:Person) RETURN p.name AS name").data()
print("📋 所有 Person 节点名：", [r['name'] for r in result])

# 8️⃣ 删除节点（演示：删除 Bob）
bob_node = matcher.match("Person", name="Bob").first()
if bob_node:
    graph.delete(bob_node)
    print("🗑️ 删除了节点 Bob")
