package com.ameyamm.mcs_thesis.ghsom

import com.ameyamm.mcs_thesis.utils.Utils

import org.apache.spark.rdd.RDD
/**
 * @author ameya
 */
class SOMLayer( private val _rowDim : Int, private val _colDim : Int ) {
  private val neurons : Vector[Vector[Neuron]] = 
      Vector.tabulate(_rowDim, _colDim)(
          (rowParam,colParam) => 
                Neuron(row = rowParam, 
                       column = colParam, 
                       neuronInstance = Instance("neuron-[" + rowParam.toString() +","+ colParam.toString() + "]",
                                                 Utils.generateRandomArray(DoubleDimension.getRandomDimensionValue)
                                        )
                )
      )
                                          
  def display() {
    neurons.foreach( vectorRow => vectorRow.foreach(neuron => println(neuron))) 
  }
  
  def train(dataset : RDD[Instance]) : SOMLayer = {
    
    for ( iteration <- 0 until SOMLayer.EPOCHS ) {
        val t = SOMLayer.EPOCHS - iteration 
        
        val neuronUpdatesRDD : RDD[(String, (Array[DimensionType], Double))]= dataset.map { instance => 
                                  val bmu : Neuron = findBMU(instance)
                                  
                                  val temp = neurons.flatten
                                  
                                  temp.map { neuron => 
                                    val neighbourhoodFactor = neuron.computeNeighbourhoodFactor(bmu, iteration)
                                    (neuron.index, (neuron.getAttributeVectorWithNeighbourhoodFactor(neighbourhoodFactor)))
                                  }
                               }
        
    }
    
    /* Find the BMU */
    
    
    null
  }
  
  def findBMU( instance : Instance ) : Neuron = {
    null
  }
                             
}

object SOMLayer {
  
  val EPOCHS : Int = 5
  
  def apply(rowDim : Int, colDim : Int) = {
    new SOMLayer(rowDim, colDim)
  }
  
  def main(args : Array[String]) {
    val somlayer = SOMLayer(4,4)
    somlayer.display()
    println(somlayer)
  }
}