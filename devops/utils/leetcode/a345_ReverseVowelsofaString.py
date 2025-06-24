class Solution:
    def reverseVowels(self, s: str) -> str:
        vole = 'aeiouAEIOU'
        l = list(s)
        i = 0
        j = len(l) - 1

        while i < j:
            if l[i] in vole:
                if l[j] in vole:
                    l[i], l[j] = l[j], l[i]
                    j -= 1
                    i += 1
                else:
                    j -= 1
            else:
                i += 1

        return ''.join(l)