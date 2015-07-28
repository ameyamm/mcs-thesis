'''
Created on Jul 26, 2015

@author: ameya
'''
from math import sqrt

def distBetweenVectors(vec1, vec2):
    dist = 0 
    
    if len(vec1) != len(vec2):
        raise Exception("Vectors length not same")
    
    for i in range(len(vec1)):
        dist += pow(vec1[i] - vec2[i], 2)
    
    return sqrt(dist) 