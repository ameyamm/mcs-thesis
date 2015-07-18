package com.ameyamm.mcs_thesis.ghsom

import scala.collection.immutable
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import com.ameyamm.mcs_thesis.globals.GHSomConfig

/**
 * @author ameya
 */
class GHSom() extends Serializable {
   
   //def dataset : RDD[Instance] = _dataset
   
   def train(dataset : RDD[Instance]) {
     
     // Compute m0 - Mean of all the input
     
     // executes on workers
     val (sumOfInstances, totalInstances) = dataset.map(instance => 
                              (instance, 1L)) 
                              .reduce(GHSomFunctions.computeSumAndNumOfInstances) // returns to the driver (Instance[attribSum], total count)
                              
     // runs on driver
     val m0 = sumOfInstances.attributeVector.map { attribute => attribute / totalInstances } 
     
     //val m0 = sumOfInstances.attributeVector.map { attribute => attribute }
     val meanInstance = Instance("0thlayer", m0)
     
     val layer0Neuron = Neuron(0,0,meanInstance)
     // Compute mqe0 - mean quantization error for 0th layer
     
     // map runs on workers, computing distance value of each instance from the meanInstance 
     val mqe0 = dataset.map(instance => meanInstance.getDistanceFrom(instance)).reduce(_ + _) / totalInstances
     //val qe0 = dataset.map(instance => meanInstance.getDistanceFrom(instance)).reduce(_ + _) 
     
     
     println("<<<<<<<<<<<<<<<<<<<<<<<<<")
     println("SumofInstances-" + sumOfInstances)
     println("meanInstance - " + meanInstance)
     println("m0:" + m0.mkString(":"))
     println("total Instances : " + totalInstances)
     println("mqe0 : " + mqe0)
     println(">>>>>>>>>>>>>>>>>>>>>>>>>")
     
     val layer = 0 
     val layer0NeuronID = "0,0"
     val layerNeuronRDD = dataset.map(instance => (layer, layer0NeuronID, instance)).persist()
     
     // layer and neuron is the pared layer and neuron
     case class LayerNeuron(layer : Int, neuronID : String) {
       override def equals( obj : Any ) : Boolean = {
         obj match {
          case o : LayerNeuron => {
            (this.layer.equals(o.layer) && this.neuronID.equals(o.neuronID)) 
          }
          case _ => false 
         }
       }
  
       override def hashCode : Int = layer.hashCode() + neuronID.hashCode()
     }
     
     val layerQueue = new mutable.Queue[LayerNeuron]()
     
     layerQueue.enqueue(LayerNeuron(layer, layer0NeuronID))
     
     var hierarchicalGrowth = true 
     
     // Create first som layer of 2 x 2
     
     while(!layerQueue.isEmpty) {
       
       val currentLayerNeuron = layerQueue.dequeue
       
       // make dataset for this layer

       val currentDataset = layerNeuronRDD.filter( obj => 
                                                     obj._1.equals(currentLayerNeuron.layer) && 
                                                     obj._2.equals(currentLayerNeuron.neuronID)
                                           )
                                           .map(obj => obj._3)

       var isGrown : Boolean = false 
     
       val attribVectorSize = currentDataset.first.attributeVector.size

       val currentLayer = SOMLayer(
                       parentNeuronID = currentLayerNeuron.neuronID, 
                       layer = currentLayerNeuron.layer + 1, 
                       rowDim = 2, 
                       colDim = 2,
                       parentNeuronMQE = layer0Neuron.mqe,
                       vectorSize = attribVectorSize)
     
       println("BEFORE: RANDOM>>>>>")
    
       do {
           // runs on driver
           currentLayer.clearMappedInputs
           // MapReduce : Uses driver and workers returning the updated values to the driver 
           currentLayer.train(currentDataset)
           // MapReduce : Uses driver and workers, updating the neurons at the driver
           // computes the layer's MQE_m and updates the mqe for individual neurons in the layer
           currentLayer.computeMQE_m(currentDataset)
           // Grows the layer, adding row/column. Runs on the driver
           isGrown = currentLayer.grow(GHSomConfig.TAU1, mqe0)
           println("AFTER: TRAINED>>>>>")
           currentLayer.display()
       }while(isGrown)
         
       // Logic for hierarchical growth
       // find neurons in current layer not abiding the condition
       // for the current dataset find the rdd of instances for the neurons to expand
       // add the instances to the dataset RDD - layer, neuronid, instance
     }
     
     /*
     while(!layerQueue.isEmpty) {
       val currentLayer = layerQueue.dequeue()
       //val parentLayerMQE0 = 
       // Grow Layer
       do {
         // runs on driver
         currentLayer.clearMappedInputs
         // MapReduce : Uses driver and workers returning the updated values to the driver 
         currentLayer.train(dataset)
         // MapReduce : Uses driver and workers, updating the neurons at the driver
         // computes the layer's MQE_m and updates the mqe for individual neurons in the layer
         currentLayer.computeMQE_m(dataset)
         // Grows the layer, adding row/column. Runs on the driver
         isGrown = currentLayer.grow(GHSomConfig.TAU1, mqe0)
         println("AFTER: TRAINED>>>>>")
         currentLayer.display()
       }while(isGrown)
         
       
     }
     somLayer1.display()
     * 
     */
     
     
     
   }
   
   
   
}

object GHSomFunctions {
  def computeSumAndNumOfInstances( a: (Instance, Long), b : (Instance, Long) ) : (Instance,Long) = {
     (a._1 + b._1, a._2 + b._2)
  }
  
}

object GHSom {
  def apply() : GHSom = {
    new GHSom()
  }
  
  
}