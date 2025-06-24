class Solution(object):
    def validPalindrome(self, s):
        def check(i, j):
            while i < j:
                if s[i] != s[j]:
                    return False
                i, j = i + 1, j - 1
            return True

        i, j = 0, len(s) - 1
        while i < j:
            if s[i] != s[j]:
                aaa = check(i + 1, j)
                bbb = check(i, j - 1)
                return aaa or bbb
            i, j = i + 1, j - 1
        return True