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
     
     layer0Neuron.mqe = mqe0
     
     println("meanInstance - " + meanInstance)
     println("total Instances : " + totalInstances)
     println("mqe0 : " + mqe0)
     println(">>>>>>>>>>>>>>>>>>>>>>>>>")
     
     // ID of a particular SOMLayer. there can be many layers in the same layer. this is like a PK
     val layer = 0 
     var layerNeuronRDD = dataset.map(instance => (layer, layer0Neuron.id, instance))
     
     // layer and neuron is the pared layer and neuron
     case class LayerNeuron(parentLayer : Int, parentNeuronID : String, parentNeuronMQE : Double) {
       override def equals( obj : Any ) : Boolean = {
         obj match {
          case o : LayerNeuron => {
            (this.parentLayer.equals(o.parentLayer) && this.parentNeuronID.equals(o.parentNeuronID)) 
          }
          case _ => false 
         }
       }
  
       override def hashCode : Int = parentLayer.hashCode() + parentNeuronID.hashCode()
     }
     
     val layerQueue = new mutable.Queue[LayerNeuron]()
     
     layerQueue.enqueue(LayerNeuron(layer, layer0Neuron.id, mqe0))
     
     var hierarchicalGrowth = true 
     
     // Create first som layer of 2 x 2
     
     while(!layerQueue.isEmpty) {
       
       val currentLayerNeuron = layerQueue.dequeue
       
       // make dataset for this layer

       val currentDataset = layerNeuronRDD.filter( obj => 
                                                     obj._1.equals(currentLayerNeuron.parentLayer) && 
                                                     obj._2.equals(currentLayerNeuron.parentNeuronID)
                                           )
                                           .map(obj => obj._3)
                                           
       val instanceCount = currentDataset.count

       var isGrown = false 
     
       val attribVectorSize = currentDataset.first.attributeVector.size

       val currentLayer = SOMLayer(
                       rowDim = 2, 
                       colDim = 2,
                       parentNeuronID = currentLayerNeuron.parentNeuronID, 
                       parentLayer = currentLayerNeuron.parentLayer, 
                       parentNeuronMQE = currentLayerNeuron.parentNeuronMQE,
                       vectorSize = attribVectorSize)
     
       do {
           // runs on driver
           currentLayer.clearMappedInputs
           // MapReduce : Uses driver and workers returning the updated values to the driver 
           currentLayer.train(currentDataset)
           // MapReduce : Uses driver and workers, updating the neurons at the driver
           // computes the layer's MQE_m and updates the mqe for individual neurons in the layer
           currentLayer.computeMQE_m(currentDataset)
           println("Trained Layer")
           currentLayer.gridSize
           currentLayer.display()
           if (currentLayer.totalNeurons < instanceCount * GHSomConfig.GRID_SIZE_FACTOR)
           // Grows the layer, adding row/column. Runs on the driver
             isGrown = currentLayer.grow(GHSomConfig.TAU1)
           else
             isGrown = false 
           println("After growth")
           currentLayer.gridSize
           currentLayer.display()
       }while(isGrown)
         
       // MapReduce : Uses driver and workers, updating the neurons at the driver
       // computes the layer's MQE_m and updates the mqe for individual neurons in the layer
       // currentLayer.computeMQE_m(currentDataset)

       currentLayer.display()
       // Logic for hierarchical growth
       // find neurons in current layer not abiding the condition
       // for the current dataset find the rdd of instances for the neurons to expand
       // add the instances to the dataset RDD - layer, neuronid, instance
         
       // check for mqe_i > TAU2 x mqe_parentNeuron
         
       val neuronsToExpand : mutable.Set[Neuron] = currentLayer.getNeuronsForHierarchicalExpansion(GHSomConfig.TAU2, totalInstances)
       
       
       neuronsToExpand.foreach { neuron =>  
         println("Expand neuron: " + currentLayer.layerID + " : " + neuron.id + " : " + neuron.mqe)  
         layerQueue.enqueue(LayerNeuron(currentLayer.layerID, neuron.id, neuron.mqe))
       }
       
       layerNeuronRDD = currentLayer.populateRDDForHierarchicalExpansion(layerNeuronRDD, currentDataset, neuronsToExpand)
       layerNeuronRDD = layerNeuronRDD.filter( tup => !(tup._1.equals(currentLayerNeuron.parentLayer) &&  
                                                       tup._2.equals(currentLayerNeuron.parentNeuronID))
                                             )
       println("layerNeuronRDD Count : " + layerNeuronRDD.count)
       currentLayer.dumpToFile
     }
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
