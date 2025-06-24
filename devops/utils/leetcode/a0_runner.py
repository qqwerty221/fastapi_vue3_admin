import time
start = time.perf_counter()
# from a7_ZigzagConversion import Solution as zigzagConversion
# zigzagConversion1 = zigzagConversion()
# aaa = zigzagConversion1.convert('PAYPALISHIRING',3)
# print(aaa)

# from a8_ReverseInt import Solution as reverseInt1
# reverseInt2 = reverseInt1()
# aaa = reverseInt2.reverse(123)
# print(aaa)


# from a9_PalindromeNumber import Solution as palindromeNumber
# palindromeNumber1 = palindromeNumber().isPalindrome(121)
# print(palindromeNumber1)

# from a10_RegularExpressionMatching import Solution as RegularExpressionMatching
# RegularExpressionMatching1 = RegularExpressionMatching().isMatch('aa','a')
# print(RegularExpressionMatching1)

# from a14_LongestCommonPrefix import Solution as LongestCommonPrefix
# LongestCommonPrefix1 = LongestCommonPrefix().longestCommonPrefix(["flower","flow","flight"])
# print(LongestCommonPrefix1)

# from a345_ReverseVowelsofaString import Solution as ReverseVowelsofaString
# ReverseVowelsofaString1 = ReverseVowelsofaString().reverseVowels('IceCreAm')
# print(ReverseVowelsofaString1)

# from a680_ValidPalindromII import Solution as ValidPalindromII
# ValidPalindromII1 = ValidPalindromII().validPalindrome('eaeadaea')
# print(ValidPalindromII1)

# from a88_MergeSortedArray import Solution as MergeSortedArray
# MergeSortedArray1 = MergeSortedArray().merge(nums1=[0], m=0, nums2=[1], n=1)
# print(MergeSortedArray1)

# from a347_TopKFrequemtElements import Solution as TopKFrequemtElements
# TopKFrequemtElements1 = TopKFrequemtElements().topKFrequent([4,1,-1,2,-1,2,3], 2)
# print(TopKFrequemtElements1)

# from a455_AssignCookies import Solution as AssignCookies
# AssignCookies1 = AssignCookies().findContentChildren([3, 9, 10],  [11, 8])
# print(AssignCookies1)

# from a121_BestTimetoBuyandSellStock import Solution as BestTimetoBuyandSellStock
# BestTimetoBuyandSellStock1 = BestTimetoBuyandSellStock().maxProfit([7,1,5,3,6,4])
# print(BestTimetoBuyandSellStock1)

# from a605_CanPlaceFlowers import Solution as CanPlaceFlowers
# CanPlaceFlowers1 = CanPlaceFlowers().canPlaceFlowers(flowerbed = [1,0,0,0,0,0,1], n = 2)
# print(CanPlaceFlowers1)

# from a392_IsSubsequence import  Solution as IsSubsequence
# IsSubsequence1 = IsSubsequence().isSubsequence(s="acb", t="ahbgdc")
# print(IsSubsequence1)

# from a665_NondecreasingArray import Solution as NondecreasingArray
# NondecreasingArray1 = NondecreasingArray().checkPossibility([3,4,2,3])
# print(NondecreasingArray1)

# from a53_MaximumSubarray import Solution as MaximumSubarray
# MaximumSubarray1 = MaximumSubarray().maxSubArray([-2,1,-3,4,-1,2,1,-5,4])
# print(MaximumSubarray1)

from a763_PartitionLabels import Solution as PartitionLabels
PartitionLabels1 = PartitionLabels().partitionLabels(s='eiurvtyksrusnrvetjenkeurxnleunlsevhlhloiriqropmxiznxtrjnernhjshsnmxrvvzseyenmxhevhtrvtkthynlriykusvkuhresliyuyjtrsrtverkstrinuhosrypmsrtimuhshjxsvsxhjsnjnkhrjzjmxjzjymxjmxrmzzsmzmjzuuoonunyuxynkxnsnjhnjnuhtinsteunkytsyerjytxqeytreytjretyrtertenoretkrtntrnynprpnunoyonyuxmhmnxhnmhxnkxuyyunkxlelkslsrinabcabcabcbacbacbacbcabacbacbacbacbacbacbcabacbacbacbacbacbacbfgfgfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdfgdgfdf')
print(PartitionLabels1)

end = time.perf_counter()
print(f"运行时间：{(end - start) * 1000:.3f} 毫秒")