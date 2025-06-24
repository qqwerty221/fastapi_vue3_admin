class Solution:
    def isSubsequence(self, s: str, t: str) -> bool:
        for i in t:
            if i == s[0]:
                s = s[1:]

        if s:
            return False
        else:
            return True