from typing import List


class Solution:
    def merge(self, nums1: List[int], m: int, nums2: List[int], n: int) -> List:
        """
        Do not return anything, modify nums1 in-place instead.
        """
        i, j, k = m - 1, n - 1, m + n - 1

        if m == 0:
            nums1.pop()
            nums1 = nums1 + nums2

        while i >= 0 and j >= 0:
            if nums1[i] > nums2[j]:
                greater = nums1[i]
                i -= 1
            else:
                greater = nums2[j]
                j -= 1

            nums1[k] = greater
            k -= 1


        return nums1