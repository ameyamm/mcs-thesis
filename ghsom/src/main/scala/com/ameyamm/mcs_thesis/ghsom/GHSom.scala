package com.ameyamm.mcs_thesis.ghsom

import scala.collection.immutable
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import com.ameyamm.mcs_thesis.globals.GHSomConfig
import scala.math.{abs, sqrt}

/**
 * @author ameya
 */
class GHSom() extends Serializable {
   
   //def dataset : RDD[Instance] = _dataset
   
   def train(dataset : RDD[Instance], attributes : Array[Attribute] = null, epochsValue : Int = GHSomConfig.EPOCHS) {
     
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
     //val mqe0 = dataset.map(instance => meanInstance.getDistanceFrom(instance)).reduce(_ + _) / totalInstances //mqe_change
     val qe0 = dataset.map(instance => meanInstance.getDistanceFrom(instance)).reduce(_ + _) 
     
     //layer0Neuron.mqe = mqe0 //mqe_change
     layer0Neuron.qe = qe0
     
     println("meanInstance - " + meanInstance)
     println("total Instances : " + totalInstances)
     //println("mqe0 : " + mqe0)
     println("qe0 : " + qe0)
     println(">>>>>>>>>>>>>>>>>>>>>>>>>")
     
     // ID of a particular SOMLayer. there can be many layers in the same layer. this is like a PK
     val layer = 0 
     var layerNeuronRDD = dataset.map(instance => (layer, layer0Neuron.id, instance))
     
     // layer and neuron is the pared layer and neuron
     //case class LayerNeuron(parentLayer : Int, parentNeuronID : String, parentNeuronMQE : Double) { // mqe_change
     case class LayerNeuron(parentLayer : Int, parentNeuron : Neuron) {
       override def equals( obj : Any ) : Boolean = {
         obj match {
          case o : LayerNeuron => {
            (this.parentLayer.equals(o.parentLayer) && this.parentNeuron.id.equals(o.parentNeuron.id)) 
          }
          case _ => false 
         }
       }
  
       override def hashCode : Int = parentLayer.hashCode() + parentNeuron.id.hashCode()
     }
     
     val layerQueue = new mutable.Queue[LayerNeuron]()
     
     //layerQueue.enqueue(LayerNeuron(layer, layer0Neuron.id, mqe0)) //mqe_change
     layerQueue.enqueue(LayerNeuron(layer, layer0Neuron))
     
     var hierarchicalGrowth = true 
     
     // Create first som layer of 2 x 2
     
     while(!layerQueue.isEmpty) {
       
       val currentLayerNeuron = layerQueue.dequeue
       
       println("Processing for parentLayer :" + currentLayerNeuron.parentLayer + ", parent neuron : " + currentLayerNeuron.parentNeuron.id)
       // make dataset for this layer

       val currentDataset = layerNeuronRDD.filter( obj => 
                                                     obj._1.equals(currentLayerNeuron.parentLayer) && 
                                                     obj._2.equals(currentLayerNeuron.parentNeuron.id)
                                           )
                                           .map(obj => obj._3)
                                           
       val instanceCount = currentDataset.count

       println("Instance count in dataset for current layer " + instanceCount)
       
       var continueTraining = false 
     
       val attribVectorSize = currentDataset.first.attributeVector.size

       val currentLayer = SOMLayer(
                       rowDim = GHSomConfig.INIT_LAYER_SIZE, 
                       colDim = GHSomConfig.INIT_LAYER_SIZE,
                       parentNeuron = currentLayerNeuron.parentNeuron,
                       parentLayer = currentLayerNeuron.parentLayer, 
                       vectorSize = attribVectorSize)
                       
       if (currentLayerNeuron.parentLayer != 0) {
         println("Initializing")
         currentLayer.initializeLayerWithParentNeuronWeightVectors
       }
     
       var epochs = epochsValue
       var prevMQE_m = 0.0
       do {
           //var epochs = currentLayer.totalNeurons * 2
           // runs on driver
           currentLayer.clearMappedInputs
          
           println("epochs : " + epochs)
           // MapReduce : Uses driver and workers returning the updated values to the driver 
           currentLayer.train(currentDataset, epochs)
           
           // MapReduce : Uses driver and workers, updating the neurons at the driver
           // computes the layer's MQE_m and updates the mqe for individual neurons in the layer
           currentLayer.computeStatsForLayer(currentDataset)
           
           //val (needsTraining, mqe_m, errorNeuron) = currentLayer.checkMQE(GHSomConfig.TAU1) //mqe_change
           val (needsTraining, mqe_m, errorNeuron) = currentLayer.checkQE(GHSomConfig.TAU1)
           
           if (needsTraining && currentLayer.totalNeurons < sqrt(instanceCount)) {
             currentLayer.growMultipleCells(GHSomConfig.TAU1)
             //currentLayer.growSingleRowColumn(errorNeuron)
             continueTraining = true 
             println("Growing")
             currentLayer.gridSize
           }
           else if (needsTraining && abs(mqe_m - prevMQE_m) > 0.1) {
             epochs = epochs * 2
             continueTraining = true
             prevMQE_m = mqe_m
             println("Increasing epochs " + epochs )
           }
           else {
             continueTraining = false
             println("Done training")
           }
           //if (currentLayer.totalNeurons < instanceCount * GHSomConfig.GRID_SIZE_FACTOR)
           // Grows the layer, adding row/column. Runs on the driver
           /*
            * if (currentLayer.totalNeurons < instanceCount)
            
             isGrown = currentLayer.grow(GHSomConfig.TAU1)
           else
           *  
           */
             
           //else
           //isGrown = false 
           
       }while(continueTraining)
         
       // MapReduce : Uses driver and workers, updating the neurons at the driver
       // computes the layer's MQE_m and updates the mqe for individual neurons in the layer
       // currentLayer.computeMQE_m(currentDataset)

       currentLayer.display()
       // Logic for hierarchical growth
       // find neurons in current layer not abiding the condition
       // for the current dataset find the rdd of instances for the neurons to expand
       // add the instances to the dataset RDD - layer, neuronid, instance
         
       // check for mqe_i > TAU2 x mqe_parentNeuron
         
       //val neuronsToExpand : mutable.Set[Neuron] = currentLayer.getNeuronsForHierarchicalExpansion(mqe0 * GHSomConfig.TAU2, totalInstances) // mqe_change
       println("Hierarchical criterion: " + GHSomConfig.TAU2 + "x" + qe0 + "=" + (qe0 * GHSomConfig.TAU2))
       val neuronsToExpand : mutable.Set[Neuron] = currentLayer.getNeuronsForHierarchicalExpansion(qe0 * GHSomConfig.TAU2, totalInstances)
       
       neuronsToExpand.foreach { neuron =>  
         //println("Expand neuron: " + currentLayer.layerID + " : " + neuron.id + " : " + neuron.mqe) 
         println("Expand neuron: " + currentLayer.layerID + " : " + neuron.id + " : " + neuron.qe) 
         //layerQueue.enqueue(LayerNeuron(currentLayer.layerID, neuron.id, neuron.mqe)) // mqe_change
         layerQueue.enqueue(LayerNeuron(currentLayer.layerID, neuron))
       }
       
       layerNeuronRDD = currentLayer.populateRDDForHierarchicalExpansion(layerNeuronRDD, currentDataset, neuronsToExpand)
       layerNeuronRDD = layerNeuronRDD.filter( tup => !(tup._1.equals(currentLayerNeuron.parentLayer) &&  
                                                       tup._2.equals(currentLayerNeuron.parentNeuron.id))
                                             )
       println("layerNeuronRDD Count : " + layerNeuronRDD.count)
       
       if (GHSomConfig.CLASS_LABELS) {
         currentLayer.computeClassLabels(currentDataset)
       }
       
       if (GHSomConfig.LABEL_SOM) {
         currentLayer.computeLabels(currentDataset, attributes.map(attrib => attrib.name))
       }
       currentLayer.dumpToFile(attributes)
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
