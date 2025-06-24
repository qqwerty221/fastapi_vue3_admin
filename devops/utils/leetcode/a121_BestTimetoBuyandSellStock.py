from typing import List


class Solution:
    def maxProfit(self, prices: List[int]) -> int:
        buy = prices[0]
        profit = 0

        for p in prices[1:]:
            if p <= buy:
                buy = p
            else:
                profit = max(profit, p - buy)

        return profit