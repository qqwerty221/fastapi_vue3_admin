from typing import List


class Solution:
    def longestCommonPrefix(self, strs: List[str]) -> str:
        result = ''
        length = len(strs[0])
        for i in range(length):
            result_set = set([ str[:(length - i)] for str in strs])

            if len(result_set) == 1:
                result = result_set.pop()
                break

        return result