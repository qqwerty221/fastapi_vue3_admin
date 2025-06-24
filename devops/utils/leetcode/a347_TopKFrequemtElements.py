from typing import List


class Solution:
    def topKFrequent(self, nums: List[int], k: int) -> List[int]:
        result = {1: []}
        def check(num, time):
            if num in result[time]:
                if time + 1 not in result:
                    result[time + 1] = []
                check(num, time + 1)
            else:
                result[time].append(num)

        for num in nums:
            check(num,1)

        keys = result.keys()

        if k > len(keys):
            k = len(keys)

        bbb = list(keys)[::-1]

        ccc = bbb[k - 1]

        ddd = result[ccc]

        return ddd