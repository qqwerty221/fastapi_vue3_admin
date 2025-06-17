from typing import List


class Solution:
    def maxArea(self, height: List[int]) -> int:
        result = 0
        i = 0
        j = len(height) - 1

        while i < j:
            comp = min(height[i], height[j]) * (j - i)
            result = max(result, comp)

            if height[i] < height[j]:
                i += 1
            else:
                j -= 1

        return result
