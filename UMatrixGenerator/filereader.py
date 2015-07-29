'''
Created on Jul 26, 2015

@author: ameya
'''

import sys
import util

import numpy as np

from math import sqrt
import matplotlib.pyplot as plt
import matplotlib.cm as cm

def getSomLayerFromFile(filename):
    somLayer = list() # array of array of vectors
    
    with open(filename) as f :
        for rec in f :
            row = rec.split("|")
            rowList = list()
            for cell in row :
                cellVector = [ float(x) for x in cell.split(",") ]
                rowList.append(cellVector)
            somLayer.append(rowList)
    
    return somLayer        

def getUMatrix(somLayer) :
    
    somRows = len(somLayer)
    somCols = len(somLayer[0])
    
    umatrixRows = (somRows * 2) - 1
    umatrixCols = (somCols * 2) - 1
    
    umatrix = [[None for x in range(umatrixCols)] for y in range(umatrixRows)]

    '''
    Fill the cells between the neuron cells
    Reference from SOM Tool Box : http://www.ifs.tuwien.ac.at/dm/download/somtoolbox+src.tar.gz
    '''
    for row in range(umatrixRows) :
        for col in range(umatrixCols) :
            if row % 2 != 0 and col % 2 == 0 : 
                # umatrix intermediate row
                umatrix[row][col] = util.distBetweenVectors(somLayer[(row - 1)/2][col/2], 
                                                            somLayer[(row - 1)/2 + 1][col/2])
                
            elif row % 2 == 0 and col % 2 != 0 : 
                # umatrix intermediate column
                umatrix[row][col] = util.distBetweenVectors(somLayer[row/2][(col - 1)/2], 
                                                            somLayer[row/2][(col - 1)/2 + 1])
                
            elif row % 2 != 0 and col % 2 != 0 :
                dist1 = util.distBetweenVectors(somLayer[(row - 1)/2][(col - 1)/2], 
                                                somLayer[(row - 1)/2 + 1][(col - 1)/2 + 1])
                dist2 = util.distBetweenVectors(somLayer[(row - 1)/2 + 1][(col - 1)/2], 
                                                somLayer[(row - 1)/2][(col - 1)/2 + 1])
                
                umatrix[row][col] = (dist1 + dist2) / (2 * sqrt(2))
    
    for row in range(0, umatrixRows, 2):
        for col in range(0, umatrixCols, 2):
            lst = list()
            if row == 0 and col == 0:
                lst = [umatrix[row+1][col],umatrix[row][col + 1]]
            elif row == 0 and col == umatrixCols - 1:
                lst = [umatrix[row+1][col], umatrix[row][col-1]]
            elif row == umatrixRows - 1 and col == 0:
                lst = [umatrix[row - 1][col], umatrix[row][col + 1]]
            elif row == umatrixRows - 1 and col == umatrixCols - 1:
                lst = [umatrix[row-1][col], umatrix[row][col-1]]
            elif col == 0:
                lst = [umatrix[row - 1][col], umatrix[row + 1][col], umatrix[row][col + 1]]
            elif col == umatrixCols - 1:
                lst = [umatrix[row - 1][col], umatrix[row + 1][col], umatrix[row][col - 1]]
            elif row == 0 :
                lst = [umatrix[row][col - 1], umatrix[row][col + 1], umatrix[row + 1][col]]
            elif row == umatrixRows - 1:
                lst = [umatrix[row][col - 1], umatrix[row][col + 1], umatrix[row - 1][col]]
            else :
                lst = [umatrix[row][col - 1], umatrix[row][col + 1], 
                       umatrix[row - 1][col], umatrix[row + 1][col]]
            umatrix[row][col] = np.mean(a = np.array(lst))
    
    print("SOM LAYER : {} x {}".format(somRows, somCols))
    print("UMatrix : {} x {}".format(umatrixRows, umatrixCols))
    
    '''
    for row in range(somRows):
        for col in range(somCols):
            print("Som[{}][{}]:{}".format(row,col,somLayer[row][col])),# end = "", flush = True)
        print
    '''
    for row in range(umatrixRows):
        for col in range(umatrixCols):
            print("Umatrix[{}][{}]:{}".format(row,col,umatrix[row][col])),# end = "", flush = True)
        print
            
    return np.array(umatrix)

def getVectorFromFile(filename):
    return np.fromfile(filename, sep = ',')

def umatrixImage(filename, somMatrix, umatrix, maxVector, minVector):
    #umatrixNumpy = imresize(np.array(umatrix),(200,200),interp = 'cubic')
    
    #image = Image.fromarray(umatrixNumpy, mode='L')
    
    numberOfAttributes = maxVector.size
    
    plt.figure(1)
    plt.subplot(2, numberOfAttributes, int(numberOfAttributes/2))
    plt.imshow(umatrix, cmap = plt.get_cmap('Greys'))
    plt.colorbar( orientation='horizontal' )
    
    for i in range(numberOfAttributes):
        somLayerForAttribute = np.copy(somMatrix[...,i])
        
        print ("som For Attribute {}-{}:{}".format(i, maxVector[i], minVector[i]))
        print somLayerForAttribute
        for elem in np.nditer(somLayerForAttribute, op_flags = ['readwrite']):
            elem[...] = (elem * (maxVector[i] - minVector[i])) + minVector[i]
        
        plt.subplot(2, numberOfAttributes, numberOfAttributes + i + 1)
        plt.imshow(somLayerForAttribute)
        plt.colorbar( orientation='horizontal')
    
    #image = Image.fromarray(np.array(umatrix), mode = 'RGB')
    #image.save(filename + ".jpeg")
    
    #plt.colorbar()
    plt.show()
    
def main(args):
    if (len(args) != 4):
        print("Usage : " + args[0] + " matrixfile maxVectorFile minVectorFile")
        return  
    
    filename = args[1]
    
    somLayer = getSomLayerFromFile(filename)
    
    umatrix = getUMatrix(somLayer)
    
    maxVectorFile = args[2]
    
    maxVector = getVectorFromFile(maxVectorFile)
    
    minVectorFile = args[3]
    
    minVector = getVectorFromFile(minVectorFile)
    
    umatrixImage(filename, np.array(somLayer), umatrix, maxVector, minVector)
            

if __name__ == '__main__':
    main(sys.argv)