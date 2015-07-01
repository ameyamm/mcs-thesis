package com.ameyamm.mcs_thesis.ghsom

import com.ameyamm.mcs_thesis.utils.Utils
/**
 * @author ameya
 */
class SOMLayer( private val _rowDim : Int, private val _colDim : Int ) {
  
  private val neurons : Vector[Vector[Neuron]] = 
      Vector.tabulate(_rowDim, _colDim)((rowParam,colParam) => 
                                          Neuron(row = rowParam.toString(), 
                                          column = colParam.toString(), 
                                          attributeVector = Utils.generateRandomVector(DoubleDimension.getRandomDimensionValue)))
}

object SOMLayer {
  def apply(rowDim : Int, colDim : Int) = {
    new SOMLayer(rowDim, colDim)
  }
}