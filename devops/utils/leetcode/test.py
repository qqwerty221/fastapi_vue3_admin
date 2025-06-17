import re

s = "123456"
if re.fullmatch("a", 'aa'):
    print("匹配成功")
else:
    print("匹配失败")
