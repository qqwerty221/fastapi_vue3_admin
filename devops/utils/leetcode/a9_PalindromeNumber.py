class Solution:
    def isPalindrome(self, x: int) -> bool:
        if x < 0:
            return False
        elif x == 0:
            return True

        str_in = str(x)
        length = len(str_in)

        if length == 1:
            return True
        elif length == 2:
            if x in (11,22,33,44,55,66,77,88,99):
                return True
            else:
                return False

        for i,n in enumerate(range(length)[::-1]):
            if i >= n:
                return True

            if str_in[i] != str_in[n]:
                return False