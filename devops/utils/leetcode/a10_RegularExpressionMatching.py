class Solution:
    def isMatch(self, s: str, p: str) -> bool:
        import re

        if re.fullmatch(p, s):
            return True
        else:
            return False