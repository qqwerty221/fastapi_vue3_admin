from typing import List


class Solution:
    def checkPossibility(self, nums: List[int]) -> bool:
        k = 0
        for i in range(1, len(nums)):
            if nums[i - 1] > nums[i]:
                if i - 2 >= 0:
                    if nums[i - 2] > nums[i]:
                        nums[i] = nums[i - 1]
                else:
                    nums[i - 1] = nums[i]
                k += 1
            if k > 1:
                return False
        return True