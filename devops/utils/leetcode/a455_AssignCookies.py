from typing import List

class Solution:
    def findContentChildren(self, g: List[int], s: List[int]) -> int:
        g.sort()  # 将孩子的胃口升序排序
        s.sort()  # 将饼干的尺寸升序排序

        child = 0
        cookie = 0

        # 遍历孩子和饼干，只要还有孩子和饼干可以尝试匹配
        while child < len(g) and cookie < len(s):
            if s[cookie] >= g[child]:  # 如果当前饼干能满足当前孩子
                child += 1  # 满足一个孩子，换下一个孩子
            cookie += 1  # 无论是否满足，尝试下一块饼干

        return child  # 返回满足的小孩数量
