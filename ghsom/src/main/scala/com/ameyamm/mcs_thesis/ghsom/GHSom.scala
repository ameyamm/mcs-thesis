package com.ameyamm.mcs_thesis.ghsom

import scala.collection.immutable
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import com.ameyamm.mcs_thesis.globals.GHSomConfig
import scala.math.{abs, sqrt}
import scala.concurrent.duration.{FiniteDuration,Duration}
import java.util.concurrent.TimeUnit._
import java.io.File
import org.apache.commons.io.FileUtils

/**
 * @author ameya
 */
class GHSom() extends Serializable {
   
   def train(dataset : RDD[Instance], attributes : Array[Attribute] = null, epochsValue : Int = GHSomConfig.EPOCHS) {
     
     val startLearningTime = System.currentTimeMillis()
     
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
     layer0Neuron.mappedInstanceCount = totalInstances
     
     println("total Instances : " + totalInstances)
     //println("mqe0 : " + mqe0)
     println("qe0 : " + qe0)
     
     // ID of a particular SOMLayer. there can be many layers in the same layer. this is like a PK
     val layer = 0 
     var layerNeuronRDD = dataset.map(instance => GHSom.LayerNeuronRDDRecord(layer, layer0Neuron.id, instance))
     
     val layerQueue = new mutable.Queue[GHSom.LayerNeuron]()
     
     //layerQueue.enqueue(LayerNeuron(layer, layer0Neuron.id, mqe0)) //mqe_change
     layerQueue.enqueue(GHSom.LayerNeuron(layer, layer0Neuron))
     
     var hierarchicalGrowth = true 
     
     // Create first som layer of 2 x 2
     
     val attribVectorSize = layer0Neuron.neuronInstance.attributeVector.size

     dumpAttributes(attributes)
     while(!layerQueue.isEmpty) {
       val layerLearningStartTime = System.currentTimeMillis()
       val currentLayerNeuron = layerQueue.dequeue
       
       println("Processing for parentLayer :" + currentLayerNeuron.parentLayer + ", parent neuron : " + currentLayerNeuron.parentNeuron.id)
       // make dataset for this layer

       val currentDataset = layerNeuronRDD.filter( obj => 
                                                     obj.parentLayerID.equals(currentLayerNeuron.parentLayer) && 
                                                     obj.parentNeuronID.equals(currentLayerNeuron.parentNeuron.id)
                                           )
                                          .map(obj => obj.instance)
                                           
       val instanceCount = currentLayerNeuron.parentNeuron.mappedInstanceCount

       println("Instance count in dataset for current layer " + instanceCount)
       
       var continueTraining = false 
     

       val currentLayer = SOMLayer(
                       rowDim = GHSomConfig.INIT_LAYER_SIZE, 
                       colDim = GHSomConfig.INIT_LAYER_SIZE,
                       parentNeuron = currentLayerNeuron.parentNeuron,
                       parentLayer = currentLayerNeuron.parentLayer, 
                       vectorSize = attribVectorSize)
                       
       if (currentLayerNeuron.parentLayer != 0) {
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
           else if (needsTraining && prevMQE_m - mqe_m > 0.1) {
             epochs = epochs * 2
             continueTraining = true
             prevMQE_m = mqe_m
             println("Increasing epochs " + epochs )
           }
           else {
             continueTraining = false
             println("Done training")
           }
       }while(continueTraining)
       
       println("Layer " + currentLayer.layerID + " Training time : " + Duration.create(System.currentTimeMillis() - layerLearningStartTime, MILLISECONDS))  

       if (GHSomConfig.CLASS_LABELS) {
         currentLayer.computeClassLabels(currentDataset)
       }
       
       if (GHSomConfig.LABEL_SOM) {
         currentLayer.computeLabels(currentDataset, attributes.map(attrib => attrib.name))
       }

       currentLayer.dumpToFile(attributes)
       // MapReduce : Uses driver and workers, updating the neurons at the driver
       // computes the layer's MQE_m and updates the mqe for individual neurons in the layer
       // currentLayer.computeMQE_m(currentDataset)

       //currentLayer.display()
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
         layerQueue.enqueue(GHSom.LayerNeuron(currentLayer.layerID, neuron))
       }
       
       layerNeuronRDD = layerNeuronRDD ++ currentLayer.getRDDForHierarchicalExpansion(currentDataset, neuronsToExpand)
       
       layerNeuronRDD = layerNeuronRDD.filter( record => !(record.parentLayerID.equals(currentLayerNeuron.parentLayer) &&  
                                                           record.parentNeuronID.equals(currentLayerNeuron.parentNeuron.id))
                                             )
       
    }

     println("Training time : " + Duration.create(System.currentTimeMillis() - startLearningTime, MILLISECONDS))
   }
   
   private def dumpAttributes(attributes : Array[Attribute]) {
     val encoding : String = null
     val attributeFileName = "attributes.txt"
     
     val attributeString = attributes.map(attrib => attrib.name + "|" + attrib.minValue + "|" + attrib.maxValue)
                                     .mkString("\n")
     
     FileUtils.writeStringToFile(new File(attributeFileName), attributeString, encoding)
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
  
  // layer and neuron is the pared layer and neuron
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
  
  case class LayerNeuronRDDRecord(parentLayerID : Int, parentNeuronID : String, instance : Instance) {
    override def equals( obj : Any ) : Boolean = {
			  obj match {
			  case o : LayerNeuronRDDRecord => {
				  (this.parentLayerID.equals(o.parentLayerID) && 
           this.parentNeuronID.equals(o.parentNeuronID) && 
           this.instance.equals(o.instance)) 
			  }
			  case _ => false 
			  }
	  }

	  override def hashCode : Int = parentLayerID.hashCode() + parentNeuronID.hashCode() + instance.hashCode()
  }
}
