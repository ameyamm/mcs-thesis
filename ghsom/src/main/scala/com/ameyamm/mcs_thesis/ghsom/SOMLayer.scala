package com.ameyamm.mcs_thesis.ghsom

import com.ameyamm.mcs_thesis.utils.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions

/**
 * @author ameya
 */
class SOMLayer( private val _rowDim : Int, private val _colDim : Int, attributeVectorSize : Int) extends Serializable {
  
  private var neurons : Array[Array[Neuron]] = Array.tabulate(_rowDim, _colDim)(
          (rowParam,colParam) => 
                Neuron(row = rowParam, 
                       column = colParam, 
                       neuronInstance = Instance("neuron-[" + rowParam.toString() +","+ colParam.toString() + "]",
                                                 Utils.generateRandomArray(DoubleDimension.getRandomDimensionValue, attributeVectorSize)
                                        )
                )
      )
                                          
  def display() {
    neurons.foreach( neuronRow => neuronRow.foreach(neuron => println(neuron))) 
  }
  
  def train(dataset : RDD[Instance]) {
    
    for ( iteration <- 0 until SOMLayer.EPOCHS ) {
        val t = SOMLayer.EPOCHS - iteration 
        
        /*val first = dataset.first()
        
        val temp = neurons.flatten
        
        val neighbourhoodFactor = temp(5).computeNeighbourhoodFactor(temp(4), t)
        
        println(">>>>" + first)
        println(">>>>>" + neighbourhoodFactor)
        println(">>>>>>" + temp(5).getAttributeVectorWithNeighbourhoodFactor(neighbourhoodFactor).mkString(":"))
     */
        
        // neuronUpdatesRDD is a RDD of (numerator, denominator) for the update of neurons at the end of epoch
        val neuronUpdatesRDD = dataset.flatMap { instance => 
                                  val bmu : Neuron = findBMU(instance)
                                  
                                  val temp = neurons.flatten
                                  
                                  temp.map { neuron => 
                                    val neighbourhoodFactor = neuron.computeNeighbourhoodFactor(bmu, t)
                                   
                                    (neuron.index, (neuron.getAttributeVectorWithNeighbourhoodFactor(neighbourhoodFactor), instance.attributeVector))
                                  }
                               }//.asInstanceOf[PairRDDFunctions[String, (Array[DimensionType], Array[DimensionType])]]

        //              neuronid, [(num,den),...] => neuronid, (SUM(num), SUM(den)) => neuronid, num/den
        neuronUpdatesRDD.take(5).foreach(t => println(t._1 + 
                                                       "|" +
                                                       t._2._1.mkString(":") + 
                                                       "|" + 
                                                       t._2._2.mkString(":"))
                                                       )
            
        
        val updatedModel = new PairRDDFunctions[String, (Array[DimensionType],Array[DimensionType])](neuronUpdatesRDD)
                                   .reduceByKey(combineNeuronUpdates _).mapValues(computeUpdatedVector _).collectAsMap()
        
        
                                   
        for (i <- 0 until neurons.size) {
          for (j <- 0 until neurons(0).size) {
            neurons(i)(j).neuronInstance = Instance("neuron-[" + i.toString() +","+ j.toString() + "]",
                                                    updatedModel(i.toString() + "," + j.toString()))
          }
        } 
        
    }
    
  }
  
  private def computeUpdatedVector(
    tup : (Array[DimensionType], Array[DimensionType])
  ) : Array[DimensionType] = {
    tup._1.zip(tup._2).map(t => t._1 / t._2)
  }
  
  private def combineNeuronUpdates(
      a : (Array[DimensionType], Array[DimensionType]), 
      b : (Array[DimensionType], Array[DimensionType])
  ) : (Array[DimensionType], Array[DimensionType]) = {
    
    // for both elements of tuples,
    // zip up the corresponding arrays of a & b and add them
    
    (a._1.zip(b._1).map(t => t._1 + t._2), a._2.zip(b._2).map(t => t._1 + t._2))
  }
  
  def findBMU( instance : Instance ) : Neuron = {
    var bmu : Neuron = neurons(0)(0)
    
    var minDist = instance.getDistanceFrom(bmu.neuronInstance)
    
    for (i <- 0 until neurons.size) {
      for (j <- 0 until neurons(0).size) {
        val dist = neurons(i)(j).neuronInstance.getDistanceFrom(instance)
        if (dist < minDist) {
          minDist = dist
          bmu = neurons(i)(j)
        }
      }
    } 
    bmu
  }
                             
}

object SOMLayer {
  
  val EPOCHS : Int = 15
  
  def apply(rowDim : Int, colDim : Int, vectorSize : Int) = {
    new SOMLayer(rowDim, colDim,vectorSize)
  }
  
  def main(args : Array[String]) {
    val somlayer = SOMLayer(4,4,68)
    somlayer.display()
    println(somlayer)
  }
}