'''
Created on Jun 3, 2020

@author: aac75
'''
from collections import deque

class WeightedDeque:
    
    weightList = []
    
    def __init__(self, size, weight_spread):
        self.size = size
        self.weight_spread = weight_spread
        self.wDeque = deque([])
    
    def push(self, value):
        self.wDeque.append(value)    
        
    def pop(self):
        return self.wDeque.popleft()
    
    def assignWeights(self, _weightList):
        if (len(self.weightList) == 0):
            
            for i in _weightList:
                self.weightList.append(i)
            
    def getWeightedAverage(self):
        
        sum = 0
        i = 0
        
        for w in self.weightList:
            
            try:
                sum += w * self.wDeque[i]
            except:
                print(len(self.weightList))
                print("An exception occurred")
            
            i += 1
            
        return sum / i
    
    def full(self):
        return len(self.wDeque) >= self.size