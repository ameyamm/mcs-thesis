package com.ameyamm.mcs_thesis.ghsom

import scala.collection.immutable
import org.apache.spark.rdd.RDD

/**
 * @author ameya
 */
class GHSom(private val _dataset : RDD[Instance]) extends Serializable {
   val somTree : immutable.TreeMap[String,SOMLayer] = immutable.TreeMap()
   
   def dataset : RDD[Instance] = _dataset
   
   def train {
     // Compute m0 - Mean of all the input
     val (sumOfInstances, totalInstances) = dataset.map(instance => 
                              (instance, 1L)) 
                              .reduce(computeMean)
                              
     val m0 = sumOfInstances.attributeVector.map { attribute => attribute / totalInstances }
     
     val meanInstance = Instance("0thlayer", m0)
     
     // Compute mqe0 - mean quantization error for 0th layer
     
     val mqe0 = dataset.map(instance => meanInstance.getDistanceFrom(instance)).reduce(_ + _) / totalInstances
     
     println("<<<<<<<<<<<<<<<<<<<<<<<<<")
     println("m0:" + m0.mkString(":"))
     println("total Instances : " + totalInstances)
     println("mqe0 : " + mqe0)
     println(">>>>>>>>>>>>>>>>>>>>>>>>>")
     
     // Create first som layer of 4 x 4
     
     val somLayer1 = SOMLayer(4,4,dataset.first.attributeVector.size)
     println("BEFORE: RANDOM>>>>>")
     somLayer1.display()
     somLayer1.train(dataset)
     println("AFTER: TRAINED>>>>>")
     somLayer1.display()
   }
   
   private def computeMean( a: (Instance, Long), b : (Instance, Long) ) : (Instance,Long) = {
     (a._1 + b._1, a._2 + b._2)
   }
   
}

object GHSom {
  def apply(dataset : RDD[Instance]) : GHSom = {
    new GHSom(_dataset = dataset)
  }
}