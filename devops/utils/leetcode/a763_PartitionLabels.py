from typing import List

class Solution:
    def partitionLabels(self, s: str) -> List[int]:
        res = []
        group = []

        k = set(s)
        for c in set(s):
            group.append([s.find(c), s.rfind(c)])
        group.sort(key=lambda x: x[0])

        point_b = group[0][1]
        for seg in group[1:]:
            if seg[0] <= point_b:
                point_b = max(point_b, seg[1])
            else:
                res.append(point_b)
                point_b = seg[1]
        res.append(point_b)

        # 计算每段长度
        final = []
        prev = -1
        for end in res:
            final.append(end - prev)
            prev = end
        return final
