from typing import List


class Solution:
    def canPlaceFlowers(self, flowerbed: List[int], n: int) -> bool:
        left = 0
        new = 0
        length = len(flowerbed)

        for i in range(length):
            if flowerbed[i] == 1:
                left = 1
                continue
            else:
                if left == 1:
                    left = 0
                    continue
                else:
                    if i == length - 1 or flowerbed[i + 1] == 0:
                        new += 1
                        left = 1
        return new >= n