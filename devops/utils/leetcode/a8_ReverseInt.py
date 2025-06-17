class Solution:
    def reverse(self, x: int) -> int:
        reversed_str = str(abs(x))[::-1]

        if len(reversed_str) < 10:
            res = reversed_str

        elif int(reversed_str[:-1]) > 214748364:
            res = '0'

        elif int(reversed_str[:-1]) == 214748364:
            if int(reversed_str[-1]) > 8 and x < 0:
                res = '0'
            if int(reversed_str[-1]) > 7 and x >= 0:
                res = '0'
        else:
            res = reversed_str

        if x < 0:
            res = int(res) * -1

        return int(res)
