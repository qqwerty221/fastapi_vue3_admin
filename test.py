from pydantic import BaseModel
from datetime import datetime

class MyModel(BaseModel):
    timestamp: datetime

data = MyModel(timestamp=datetime.now())

print(type(data.timestamp))
print(data.model_dump())  # 输出 ISO 8601 格式
