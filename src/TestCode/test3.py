

class Solution(object):
    def merge(self, nums1, m, nums2, n):
        """
        :type nums1: List[int]
        :type m: int
        :type nums2: List[int]
        :type n: int
        :rtype: None Do not return anything, modify nums1 in-place instead.
        """

        res = []
        index1 = 0
        index2 = 0

        while(index1 < m and index2<n):
            if nums1[index1] < nums2[index2]:
                res.append(nums1[index1])
                index1 = index1 +1
            else:
                res.append(nums2[index2])
                index2 = index2 +1

        if index1 < m:
            res.extend(nums1[index1:m])
     
        if index2 < n:
            res.extend(nums2[index2:n])
        
        for index, v in enumerate(res):
            nums1[index] = v

    def removeElement(self, nums, val):
        """
        :type nums: List[int]
        :type val: int
        :rtype: int
        """
        while (val in nums):
            nums.remove(val)
        
        return len(nums)


if __name__ == "__main__":
    s = Solution()
    #nums1 = [2,0]
    # nums1 = [1,2,3,0,0,0]
    # nums2 = [2,5,6]
    # s.merge(nums1,3,nums2,3)
    # print(nums1)
    nums = [3,2,2,3]
    s.removeElement(nums,3)
    