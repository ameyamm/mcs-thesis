package com.ameyamm.mcs_thesis.ghsom

import com.ameyamm.mcs_thesis.utils.Utils
import com.ameyamm.mcs_thesis.globals.GHSomConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import scala.collection.mutable
import scala.collection.immutable
import scala.collection.Set
import scala.math.{abs,max}
import org.apache.commons
import org.apache.commons.io.FileUtils
import java.io.File


/**
 * @author ameya
 */
class SOMLayer private (
    private val _layerID : Int, 
    private var _rowDim : Int, 
    private var _colDim : Int, 
    private val _parentNeuronID : String,
    private val _parentLayer : Int,
    private val parentNeuronMQE : Double, 
    attributeVectorSize : Int
) extends Serializable {
  
  private var neurons : Array[Array[Neuron]] = Array.tabulate(_rowDim, _colDim)(
          (rowParam,colParam) => 
                Neuron(row = rowParam, 
                       column = colParam, 
                       neuronInstance = Instance("neuron-[" + rowParam.toString() +","+ colParam.toString() + "]",
                                                 Utils.generateRandomArray(DoubleDimension.getRandomDimensionValue, attributeVectorSize)
                                        )
                )
      )
                                          
  def layerID : Int = _layerID

  def parentLayer : Int = _parentLayer

  def gridSize() {
    println("LAYER SIZE: " + _rowDim + "x" + _colDim)
  }
  
  def totalNeurons : Long = _rowDim * _colDim
  
  def display() {
    println("Display Layer")
    println("Layer Details -> id : " + 
        this._layerID + 
        ";parent Layer : " + 
        this.parentLayer + 
        ";parent Neuron" +
        this._parentNeuronID)
    println("Layer:")
    neurons.foreach( neuronRow => neuronRow.foreach(neuron => println(neuron))) 
  }
  
  def clearMappedInputs {
    for (neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        neuron.clearMappedInputs()
      }
    }  
  }
  
  def train(dataset : RDD[Instance]) {
    
    // TODO : neurons could be made broadcast variable
    val neurons = this.neurons
    
    val maxIterations = max(max(this._rowDim, this._colDim),GHSomConfig.EPOCHS)
    
    for ( iteration <- 0 until maxIterations ) {
      val t = maxIterations - iteration 
        
        /**** MapReduce Begins ****/
      // neuronUpdatesRDD is a RDD of (numerator, denominator) for the update of neurons at the end of epoch
      // runs on workers
      val neuronUpdatesRDD = dataset.flatMap { instance => 
                                  val bmu : Neuron = SOMLayerFunctions.findBMU(neurons, instance)
                                  
                                  val temp = neurons.flatten
                                  
                                  temp.map { neuron => 
                                    val neighbourhoodFactor = neuron.computeNeighbourhoodFactor(bmu, t)
                                    (
                                        neuron.id, 
                                        (
                                          NeuronFunctions.getAttributeVectorWithNeighbourhoodFactor(neuron, neighbourhoodFactor), 
                                          neighbourhoodFactor
                                        )
                                    )
                                  }
                               }

        //              neuronid, [(num,den),...] => neuronid, (SUM(num), SUM(den)) => neuronid, num/den
       
        val updatedModel = new PairRDDFunctions[String, (Array[DimensionType],Double)](neuronUpdatesRDD) // create a pairrdd
                                   .reduceByKey(SOMLayerFunctions.combineNeuronUpdates) // combines/shuffles updates from all workers
                                   .mapValues(SOMLayerFunctions.computeUpdatedNeuronVector) // updates all values (neuron ID -> wt. vector)
                                   .collectAsMap()  // converts to map of neuronID -> wt. vector // returns to driver
        
        /**** MapReduce Ends ****/
                                   
        // Running on driver                                   
        /* 
         * At the driver:
         * Perform update to neurons 
         */
        
        for (i <- 0 until neurons.size) {
          for (j <- 0 until neurons(0).size) {
            neurons(i)(j).neuronInstance = null
            neurons(i)(j).neuronInstance = Instance("neuron-[" + i.toString() +","+ j.toString() + "]",
                                                    updatedModel(i.toString() + "," + j.toString()))
          }
        } 
    }
  }
  
  def computeMQE_m(dataset : RDD[Instance]) {
    
    val neurons = this.neurons
    
    /***** MapReduce Begins *****/
    // runs on workers 
    // creates a map of (neuron id -> (quantization error, instance count = 1)) 
    val neuronsQE = new PairRDDFunctions[String, (Double, Long)](
                      dataset.map { instance => 
                        val bmu = SOMLayerFunctions.findBMU(neurons, instance)
                        
                        val qe = bmu.neuronInstance.getDistanceFrom(instance)
                        (bmu.id,(qe,1L))         
                      }
                    )
    
    // combines / reduces all the quantization errors
    val neuronMQEs = neuronsQE.reduceByKey(SOMLayerFunctions.combineNeuronsQE)
                              .mapValues(SOMLayerFunctions.computeMQEForNeuron)

    neuronMQEs.collectAsMap
              .map(updateNeuronMQEs) // return to driver
    /***** MapReduce Ends *****/
  }
  
  /**
   * Grows the layer adding rows/columns. 
   * This method runs on the driver completely
   * @param tau1 parameter controlling the horizontal growth of a layer
   * @param mqe_u mean quantization error of the parent neuron of the layer 
   */
  def grow(tau1 : Double) : Boolean = {
    // compute MQEm of the layer and neuron with max mqe
    var mqe_m : Double = 0
    var mappedNeuronsCnt : Int = 0 
    var maxMqeNeuron = neurons(0)(0)
    var maxMqe : Double = 0 
    
    for (neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        if (neuron.mappedInstanceCount != 0) {
          mqe_m += neuron.mqe
          mappedNeuronsCnt += 1
          if (neuron.mqe > maxMqe) {
            maxMqeNeuron = neuron
            maxMqe = neuron.mqe
          }
        }     
      }
    }
    
    if (mqe_m / mappedNeuronsCnt > tau1 * this.parentNeuronMQE) {
      //var neuronPair : NeuronPair = getNeuronAndNeighbourForGrowing(maxMqeNeuron)
      var neuronPairSet = getNeuronAndNeighbourSetForGrowing(tau1 * parentNeuronMQE)
      
      for (pair <- neuronPairSet) {
        println("Neurons to Expand")  
        println(pair)
      }
      
      neurons = getGrownLayer(neuronPairSet)
      true
      /*
      val growNeurons : mutable.Set[NeuronPair] = new mutable.HashSet[NeuronPair]()
      var newRows = _rowDim 
      var newCols = _colDim
    
      growNeurons.foreach { neuronPair => {
                            if (neuronPair.neuron1.row == neuronPair.neuron2.row)
                              newCols += 1
                            else 
                              newRows += 1
                          } 
                        }
      */
    }
    else 
      false 
  }
  /*
  private def combineNeuronsQE( t1: (Double, Long), 
                                t2: (Double, Long) 
                              ) : (Double, Long) = {
    (t1._1 + t2._1, t1._2 + t2._2)
  }
  */
  
  def getNeuronsForHierarchicalExpansion(tau2 : Double, instanceCount : Long) : mutable.Set[Neuron] = {
      val neuronSet = new mutable.HashSet[Neuron]()
      
      neurons.foreach { 
        neuronRow => 
          neuronRow.foreach { 
            neuron => 
              if (neuron.mappedInstanceCount > GHSomConfig.HIERARCHICAL_COUNT_FACTOR * instanceCount &&
                  neuron.mqe > tau2 * parentNeuronMQE) { 
                neuronSet += neuron
              }
          } 
      }
      
      neuronSet
  }
  
  def populateRDDForHierarchicalExpansion(
      addToDataset : RDD[(Int, String, Instance)], 
      dataset : RDD[Instance], 
      neuronsToExpand : mutable.Set[Neuron]
      ) : RDD[(Int, String, Instance)] = {
    
    val context = dataset.context
    
    val rddRecordList = new mutable.ListBuffer[(Int, String, Instance)]
    
    val neurons = this.neurons
    
    val layerID = this.layerID
    
    val neuronIdsToExpand = neuronsToExpand.map(neuron => neuron.id)
    var origDataset = addToDataset
    
    val datasetToAdd = 
          new PairRDDFunctions[String, List[(Int, String, Instance)]](dataset.map{
          instance => 
                   val bmu = SOMLayerFunctions.findBMU(neurons, instance) 
                   if (neuronIdsToExpand.contains(bmu.id)) {
                    val tup = (layerID, bmu.id, instance)
                    (layerID.toString + ":" + bmu.id, List(tup))
                    //val list = List[(Int, String, Instance)]()
                   }
                   else 
                    ("-1", List((-1, "-1", null)))
          })
    
    val newDataset = datasetToAdd.reduceByKey(SOMLayerFunctions.mergeDatasetsForHierarchicalExpansion)
                                  .filter(tup => !tup._1.equals("-1"))
                                  .flatMap(tup => tup._2)
    
    origDataset = origDataset ++ newDataset
    //rddRecordList.foreach(tup => println("RDD TO APPEND:" + tup._1 + ":" + tup._2 + ":" + tup._3.label))
    //context.parallelize(rddRecordList)
    //println("populateRDDForHierarchicalExpansion size:" + origDataset.count)
    origDataset
  }
  
  def dumpToFile {
    
    val strNeurons = neurons.map(row => {
                                  row.map(neuron => neuron.neuronInstance.attributeVector.mkString(",")).mkString("|")
                               }
                            )
                            .mkString("\n")
    
    val filename = "SOMLayer_" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuronID + ".data"

    val encoding : String = null
    
    FileUtils.writeStringToFile(new File(filename), strNeurons, encoding)
  }
  
  private def updateNeuronMQEs(
      tuple : (String, (Double, Double, Long))) : Unit = {

    val neuronRowCol = tuple._1.split(",")

    val (neuronRow, neuronCol) = (neuronRowCol(0).toInt, neuronRowCol(1).toInt)
    
    println("Neuron - " + tuple._1 + "; MQE : " + tuple._2._1 + ";mappedInstance Count : " + tuple._2._3) 
    
    neurons(neuronRow)(neuronCol).mqe = tuple._2._1
    neurons(neuronRow)(neuronCol).qe = tuple._2._2
    neurons(neuronRow)(neuronCol).mappedInstanceCount = tuple._2._3
    neurons(neuronRow)(neuronCol).clearMappedInputs()

  }
  
  private case class NeuronPair ( neuron1 : Neuron, neuron2 : Neuron ) {
       override def equals( obj : Any ) : Boolean = {
         obj match {
          case o : NeuronPair => {
            (this.neuron1.equals(o.neuron1) && this.neuron2.equals(o.neuron2)) || 
            (this.neuron1.equals(o.neuron2) && this.neuron2.equals(o.neuron1))
          }
          case _ => false 
         }
       }
  
       override def hashCode : Int = neuron1.hashCode() + neuron2.hashCode()
       
       def isSameRow : Boolean = {
         if (neuron1.row == neuron2.row)
           true
         else 
           false
       }
       
       def isSameCol : Boolean = {
         if (neuron1.column == neuron2.column) 
           true
         else
           false
       }
       
       override def toString : String = {
         "Neuron1: " + neuron1.id + ", Neuron2: " + neuron2.id
       }
  }
  
  private def getNeuronAndNeighbourSetForGrowing(
      criterion : Double
      ) : Set[NeuronPair] = {
    val neuronNeighbourSet = new mutable.HashSet[NeuronPair]()
    
    for(neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        if (neuron.mqe > criterion) {
          val dissimilarNeighbour = getMostDissimilarNeighbour(neuron)
          neuronNeighbourSet += NeuronPair(neuron, dissimilarNeighbour)
        }
      }
    }
    neuronNeighbourSet
  }
 
  private def getMostDissimilarNeighbour(refNeuron : Neuron) = {
    val neighbours = getNeighbourNeurons(refNeuron)
    
    var dissimilarNeighbour : Neuron = null
    var maxDist = 0.0
    // find the dissimilar neighbour
    for (neighbour <- neighbours) {
      val dist = refNeuron.neuronInstance.getDistanceFrom(neighbour.neuronInstance)
      if (dist > maxDist) {
        dissimilarNeighbour = neighbour
        maxDist = dist
      }
    }
    
    dissimilarNeighbour
  }
  
  //private def getNeuronAndNeighbourForGrowing(tau1 : Double, ghsomQE : Double) : NeuronPair = {
  private def getNeuronAndNeighbourForGrowing(errorNeuron : Neuron) : NeuronPair = {  
    var neuronPair : NeuronPair = null 
    val neighbours = getNeighbourNeurons(errorNeuron)
    var dissimilarNeighbour : Neuron = null
    var maxDist = 0.0
    
    // find the dissimilar neighbour
    for (neighbour <- neighbours) {
      val dist = errorNeuron.neuronInstance.getDistanceFrom(neighbour.neuronInstance)
      if (dist > maxDist) {
        dissimilarNeighbour = neighbour
        maxDist = dist
      }
    }
          
    NeuronPair(errorNeuron, dissimilarNeighbour)
    /*
    // find the neurons for growing
    for ( i <- 0 until _rowDim ) {
      for ( j <- 0 until _colDim ) {
        val neuron = neurons(i)(j)
        
        if (neuron.mqe > tau1 * ghsomQE) {
          val neighbours = getNeighbourNeurons(neuron)
          var dissimilarNeighbour : Neuron = null
          var maxDist = 0.0
          // find the dissimilar neighbour
          for (neighbour <- neighbours) {
            val dist = neuron.neuronInstance.getDistanceFrom(neighbour.neuronInstance)
            if (dist > maxDist) {
              dissimilarNeighbour = neighbour
              maxDist = dist
            }
          }
          
          neuronPair = NeuronPair(neuron, dissimilarNeighbour)
         // growNeurons += NeuronPair(neuron, dissimilarNeighbour)
        }
      }
    }*/
    
  }

  private def getGrownLayer(neuronNeighbourSet : Set[NeuronPair]) 
  : Array[Array[Neuron]] = {
    
    // get count of rows, columns to be added
    
    val currentNeuronLayer = neurons 
    
    var rowsToAdd = 0 
    var colsToAdd = 0 
    
    for (neuronPair <- neuronNeighbourSet) {
      if (neuronPair.isSameRow) 
        colsToAdd += 1
      else if (neuronPair.isSameCol)
        rowsToAdd += 1
      else {
        throw new IllegalArgumentException("neighbour set contains improper neighbour pair")
      }
    }
    
    val newNeurons = Array.ofDim[Neuron](_rowDim + rowsToAdd, _colDim + colsToAdd)
    
    // copy original array as it is
    for (i <- 0 until neurons.size) {
      for ( j <- 0 until neurons(0).size) {
        newNeurons(i)(j) = neurons(i)(j)
      }
    }
    
    _rowDim += rowsToAdd
    _colDim += colsToAdd
    
    // update the new array for each neuron pair
    for (neuronPair <- neuronNeighbourSet) {
      val (rowIdxNeuron1, colIdxNeuron1) : (Int, Int)= getNeuronRowColIdxInLayer(neuronPair.neuron1, newNeurons)
      
      if (neuronPair.isSameRow) {
        if (neuronPair.neuron1.column < neuronPair.neuron2.column) {
          // update and shift the values after this column
          insertInNextCol(newNeurons, colIdxNeuron1)
        }
        else 
          // update and shift the values after previous column
          insertInNextCol(newNeurons, colIdxNeuron1 - 1)
      }
      else {
        if (neuronPair.neuron1.row < neuronPair.neuron2.row) {
          // update and shift the values after this row 
          insertInNextRow(newNeurons, rowIdxNeuron1)
        }
        else 
          // update and shift the values after previous row 
          insertInNextRow(newNeurons, rowIdxNeuron1 - 1)
      }
    }

    for (i <- 0 until newNeurons.size) {
      for (j <- 0 until newNeurons(0).size) {
        newNeurons(i)(j).updateRowCol(i, j)
      }
    }
    
    newNeurons
  }
  
  private def insertInNextRow(neuronArray : Array[Array[Neuron]], row : Int) {
   
    if (row + 1 >= neuronArray.size || row + 2 >= neuronArray.size)
      throw new IllegalArgumentException("row value improper")
    
    for ( i <- neuronArray.size - 1 until row + 1 by -1 ) {
      for (j <- 0 until neuronArray(0).size) {
        neuronArray(i)(j) = neuronArray(i-1)(j)
      }
    } 

    // update with the average instance 
    for (j <- 0 until neuronArray(0).size ) {
      if (neuronArray(row)(j) != null && neuronArray(row + 2)(j) != null) {
        neuronArray(row + 1)(j) = Neuron(
                                    row + 1, 
                                    j, 
                                    Instance.averageInstance(
                                        neuronArray(row)(j).neuronInstance, 
                                        neuronArray(row + 2)(j).neuronInstance 
                                        )
                                    )
        neuronArray(row + 1)(j).id = neuronArray(row)(j).id + "+" + neuronArray(row + 2)(j).id
      }
    }
  }
  
  private def insertInNextCol(neuronArray : Array[Array[Neuron]], col : Int) {
   
    if (col + 1 >= neuronArray(0).size || col + 2 >= neuronArray(0).size)
      throw new IllegalArgumentException("row value improper")
    
    for (j <- neuronArray(0).size - 1 until col + 1 by -1 ) {
      for ( i <- 0 until neuronArray.size ) {
        neuronArray(i)(j) = neuronArray(i)(j - 1)
      }
    } 

    // update with the average instance 
    for (i <- 0 until neuronArray.size ) {
      if (neuronArray(i)(col) != null && neuronArray(i)(col + 2) != null) {
        neuronArray(i)(col + 1) = Neuron(
                                    i, 
                                    col + 1, 
                                    Instance.averageInstance(
                                        neuronArray(i)(col).neuronInstance, 
                                        neuronArray(i)(col + 2).neuronInstance 
                                        )
                                    )
        neuronArray(i)(col + 1).id = neuronArray(i)(col).id + "+" + neuronArray(i)(col + 2).id
      }
    }
  }
  
  private def getNeuronRowColIdxInLayer(neuron : Neuron, neuronArray : Array[Array[Neuron]]) : (Int, Int) = {

    var rowColTuple : (Int, Int) = (0,0)
    
    var found = false 
    var i = 0
    while ( i < neuronArray.size && !found ) {
      var j = 0
      while ( j < neuronArray(0).size && !found ) {
        if ( neuronArray(i)(j) != null && neuron.id.equals(neuronArray(i)(j).id) ) {
          rowColTuple = (i,j)
          found = true
        }
        j += 1
      }
      i += 1
    }
    
    if (found == false)
      throw new IllegalStateException("Improper state of neuron Array")

    rowColTuple
  }
  
  
  /*
  private def getGrownLayer(neuronPair : NeuronPair) : Array[Array[Neuron]] = {
    
    var newNeurons : Array[Array[Neuron]] = null
    // add a row
    if (neuronPair.neuron1.row != neuronPair.neuron2.row) { 
      newNeurons = getRowAddedLayer(neuronPair)
      _rowDim += 1
    }
    else { // add a column
      newNeurons = getColumnAddedLayer(neuronPair)
      _colDim += 1
    }
    
    newNeurons
  }
  
  private def getRowAddedLayer(neuronPair : NeuronPair) : Array[Array[Neuron]] = {
    var newNeurons = Array.ofDim[Neuron](_rowDim + 1, _colDim) // add a row
    
    val minRow = (neuronPair.neuron1.row < neuronPair.neuron2.row) match { 
                  case true => neuronPair.neuron1.row 
                  case false => neuronPair.neuron2.row
                  }
    
    for ( i <- 0 to minRow ) { // 0 to minRow included
      for ( j <- 0 until _colDim) {
        newNeurons(i)(j) = neurons(i)(j)
      }
    }
    
    // average values for new row
    for ( j <- 0 until _colDim ) {
      newNeurons(minRow + 1)(j) = 
        Neuron(minRow + 1, 
               j, 
               Instance.averageInstance(neurons(minRow)(j).neuronInstance, 
                                        neurons(minRow+1)(j).neuronInstance))
    }
    
    // copy remaining
    for (i <- minRow + 1 until _rowDim) {
      for ( j <- 0 until _colDim ) {
        newNeurons(i + 1)(j) = Neuron(i+1, j, neurons(i)(j).neuronInstance)
      }
    }
    
    newNeurons
  }
  * 
  */
  
  private def getColumnAddedLayer(neuronPair : NeuronPair) : Array[Array[Neuron]] = {
    var newNeurons = Array.ofDim[Neuron](_rowDim, _colDim + 1) // add a column
    
    val minCol = (neuronPair.neuron1.column < neuronPair.neuron2.column) match { 
                  case true => neuronPair.neuron1.column 
                  case false => neuronPair.neuron2.column
                  }
    
    for ( j <- 0 to minCol ) { // 0 to minCol included
      for ( i <- 0 until _rowDim) {
        newNeurons(i)(j) = neurons(i)(j)
      }
    }
    
    // average values for new row
    for ( i <- 0 until _rowDim ) {
      newNeurons(i)(minCol + 1) = 
        Neuron(i, 
               minCol + 1, 
               Instance.averageInstance(neurons(i)(minCol).neuronInstance, 
                                        neurons(i)(minCol+1).neuronInstance))
    }
    
    // copy remaining
    for (j <- minCol + 1 until _colDim) {
      for ( i <- 0 until _rowDim ) {
        newNeurons(i)(j + 1) = Neuron(i, j+1, neurons(i)(j).neuronInstance)
      }
    }
    
    newNeurons
  }
  
  private def getNeighbourNeurons(neuron : Neuron) : Iterable[Neuron] = {
      
    val row = neuron.row
      
    val col = neuron.column
      
    val neighbours : mutable.ListBuffer[Neuron] = new mutable.ListBuffer[Neuron]()
      
    if (row - 1 >= 0)
      neighbours += neurons(row - 1)(col)
      
    if (row + 1 < _rowDim)
      neighbours += neurons(row + 1)(col)
      
    if (col - 1 >= 0)
      neighbours += neurons(row)(col - 1)
      
    if (col + 1 < _colDim)
      neighbours += neurons(row)(col + 1)
        
    neighbours
  }
  
  /*
  private def computeUpdatedVector(
    tup : (Array[DimensionType], Double)
  ) : Array[DimensionType] = {
    tup._1.map(t => t / tup._2)
  }

  private def combineNeuronUpdates(
      a : (Array[DimensionType], Double), 
      b : (Array[DimensionType], Double)
  ) : (Array[DimensionType], Double) = {
    
    // for both elements of tuples,
    // zip up the corresponding arrays of a & b and add them
    
    (a._1.zip(b._1).map(t => t._1 + t._2), a._2 + b._2)
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
  * 
  */
                             
}

object SOMLayerFunctions {
  def findBMU(neurons : Array[Array[Neuron]], instance : Instance) : Neuron = {
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
  
  def combineNeuronUpdates(
      a : (Array[DimensionType], Double), 
      b : (Array[DimensionType], Double)
  ) : (Array[DimensionType], Double) = {
    
    // for both elements of tuples,
    // zip up the corresponding arrays of a & b and add them
    
    (a._1.zip(b._1).map(t => t._1 + t._2), a._2 + b._2)
  }
  
  def computeUpdatedNeuronVector(
    tup : (Array[DimensionType], Double)
  ) : Array[DimensionType] = {
    tup._1.map(t => t / tup._2)
  }
  
  def combineNeuronsQE( t1: (Double, Long), 
                                t2: (Double, Long) 
                              ) : (Double, Long) = {
    (t1._1 + t2._1, t1._2 + t2._2)
  }
  
  /**
   * input : tuple of (qe, num_of_instances)
   * output : tuple of (mqe , qe, num_of_instances)
   */
  def computeMQEForNeuron(
      tup : (Double, Long) 
  ) : (Double, Double, Long) = {
    (tup._1/tup._2, tup._1, tup._2)
  }
  
  def mergeDatasetsForHierarchicalExpansion(
      tup1List : List[(Int, String, Instance)], 
      tup2List : List[(Int, String, Instance)] 
  ) : List[(Int, String, Instance)] = {
    tup1List ++ tup2List
  }
}

object SOMLayer {
  
  private var layerId = 0
  
  def apply(parentNeuronID : String, parentLayer : Int, rowDim : Int, colDim : Int, parentNeuronMQE : Double, vectorSize : Int) = {
    layerId += 1
    new SOMLayer(layerId, rowDim, colDim, parentNeuronID, parentLayer, parentNeuronMQE, vectorSize )
  }
  
  def main(args : Array[String]) {
  }
}
