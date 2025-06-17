class Solution:
    def convert(self, s: str, num_rows: int) -> str:
        if num_rows == 1 or num_rows >= len(s):
            return s
        rows = [''] * num_rows
        step = -1
        curr_row = 0

        for char in s:
            rows[curr_row] += char
            if curr_row == 0 or curr_row == num_rows - 1:
                step = -step
            curr_row += step

        return ''.join(rows)
