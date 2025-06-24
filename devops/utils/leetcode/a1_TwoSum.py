from typing import List


class Solution:
    def twoSum(self, nums: List[int], target: int) -> List[int]:
        compare = []
        for i,v in enumerate(nums):
            if v in compare:
                return [compare.index(v),i]
            minus = target - v
            compare.append(minus)