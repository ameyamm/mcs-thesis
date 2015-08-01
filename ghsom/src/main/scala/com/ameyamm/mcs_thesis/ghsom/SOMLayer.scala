package com.ameyamm.mcs_thesis.ghsom

import com.ameyamm.mcs_thesis.utils.Utils
import com.ameyamm.mcs_thesis.globals.GHSomConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import scala.collection.mutable
import scala.collection.immutable
import scala.collection.Set
import scala.math.{abs,max,min,log,exp,pow,sqrt,ceil}
import org.apache.commons
import org.apache.commons.io.FileUtils
import java.io.File
import scala.compat.Platform


/**
 * @author ameya
 */
class SOMLayer private (
  private val _layerID : Int, 
  private var _rowDim : Int, 
  private var _colDim : Int, 
  private val _parentNeuronID : String,
  private val _parentLayer : Int,
  private val parentNeuronQE : Double, 
  //private val parentNeuronMQE : Double,  // mqe_change
  attributeVectorSize : Int
) extends Serializable {

  private var neurons : Array[Array[Neuron]] = {
    Array.tabulate(_rowDim, _colDim)(
      (rowParam,colParam) => {
        val neuron = Neuron(row = rowParam, 
          column = colParam, 
          neuronInstance = Instance("neuron-[" + rowParam.toString() +","+ colParam.toString() + "]",
            Utils.generateRandomArray(DoubleDimension.getRandomDimensionValue, attributeVectorSize)
            //Utils.generateArrayOfGivenValue(value, attributeVectorSize)
            )
        )
        neuron
      }
    )
  }

  def layerID : Int = _layerID

  def parentLayer : Int = _parentLayer

  def gridSize() {
    println("LAYER SIZE: " + _rowDim + "x" + _colDim)
  }

  def totalNeurons : Long = _rowDim * _colDim

  private case class NeuronPair ( var neuron1 : Neuron, var neuron2 : Neuron ) {

    if (isSameRow) {
      if (neuron1.column > neuron2.column) {
        val temp = neuron1
        neuron1 = neuron2
        neuron2 = temp
      }
    }
    else {
      if (neuron1.row > neuron2.row) {
        val temp = neuron1
        neuron1 = neuron2
        neuron2 = temp
      }
    }

    override def equals( obj : Any ) : Boolean = {
      obj match {
        case o : NeuronPair => {
          (this.neuron1.equals(o.neuron1) && this.neuron2.equals(o.neuron2)) || 
          (this.neuron1.equals(o.neuron2) && this.neuron2.equals(o.neuron1))  
          /*
          ((this.isSameRow) && (o.isSameRow) && (this.neuron1.column == o.neuron1.column) && (this.neuron2.column == o.neuron2.column)) || 
          ((this.isSameCol) && (o.isSameCol) && (this.neuron1.row == o.neuron1.row) && (this.neuron2.row == o.neuron2.row)) 
          */

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

  def train(dataset : RDD[Instance], maxIterations : Long) {

    // TODO : neurons could be made broadcast variable
    val neurons = this.neurons

    //val maxIterations = max(max(this._rowDim, this._colDim),GHSomConfig.EPOCHS)
    //val sampledDataset = dataset.sample(withReplacement = false, fraction = 1.0, seed = Platform.currentTime)

    var iteration = 0

    val radius = ceil(sqrt(pow(this._rowDim, 2) + pow(this._colDim,2))).asInstanceOf[Int] //max(this._rowDim, this._colDim)
    
    while( iteration < maxIterations ) {
      //println("Sigma = " + (radius * (exp(( -1 * iteration) / (maxIterations / log(radius)))))) 
      /**** MapReduce Begins ****/
      // neuronUpdatesRDD is a RDD of (numerator, denominator) for the update of neurons at the end of epoch
      // runs on workers
      val neuronUpdatesRDD = 
        //dataset.sample(withReplacement = false, fraction = 1.0, seed = Platform.currentTime)
      dataset.flatMap { instance => 
        val bmu : Neuron = SOMLayerFunctions.findBMU(neurons, instance)

        val temp = neurons.flatten
        temp.map { neuron => 
          val neighbourhoodFactor = neuron.computeNeighbourhoodFactor(bmu, iteration, maxIterations, radius)
          (
            neuron.id, 
            (
              InstanceFunctions.getAttributeVectorWithNeighbourhoodFactor(instance, neighbourhoodFactor), 
              neighbourhoodFactor
            )
          )
        }
      }

      // neuronid, [(num,den),...] => neuronid, (SUM(num), SUM(den)) => neuronid, num/den

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

        iteration += 1
    }
  }

  def computeMQE_m(dataset : RDD[Instance]) {

    val neurons = this.neurons

    /***** MapReduce Begins *****/
    // runs on workers 
    // creates a map of (neuron id -> (quantization error, instance count = 1)) 
    //val neuronsQE = new PairRDDFunctions[String, (Double, Long)]( 
    val neuronsQE = new PairRDDFunctions[String, (Double, Long, Set[String])]( // label_map
      dataset.map { instance => 
        val bmu = SOMLayerFunctions.findBMU(neurons, instance)

        val qe = bmu.neuronInstance.getDistanceFrom(instance)
        //(bmu.id,(qe,1L))
        (bmu.id,(qe,1L,Set(instance.label))) // label_map
      }
    )

    // combines / reduces (adds) all quantization errors and instance counts from each mapper
    val neuronMQEs = neuronsQE.reduceByKey(SOMLayerFunctions.combineNeuronsQE)
                              .mapValues(SOMLayerFunctions.computeMQEForNeuron)

    neuronMQEs.collectAsMap
              .map(updateNeuronMQEs) // return to driver
    /***** MapReduce Ends *****/
  }

  //def checkMQE(tau1 : Double): (Boolean, Double, Neuron) = { //mqe_change
  def checkQE(tau1 : Double): (Boolean, Double, Neuron) = {
    //var sum_mqe_m : Double = 0 // mqe_change
    var sum_qe_m : Double = 0
    var mappedNeuronsCnt : Int = 0 
    //var maxMqeNeuron = neurons(0)(0) //mqe_change
    var maxQeNeuron = neurons(0)(0)
    //var maxMqe : Double = 0 //mqe_change
    var maxQe : Double = 0

    for (neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        if (neuron.mappedInstanceCount != 0) {
          sum_qe_m += neuron.qe
          mappedNeuronsCnt += 1
          if (neuron.qe > maxQe) {
            maxQeNeuron = neuron
            maxQe = neuron.qe
          }
          /*
           * mqe_change
           sum_mqe_m += neuron.mqe
           mappedNeuronsCnt += 1

           if (neuron.mqe > maxMqe) {
             maxMqeNeuron = neuron
             maxMqe = neuron.mqe
           }
           */
        }     
      }
    }

    //val MQE_m = sum_mqe_m / mappedNeuronsCnt // mqe_change
    val MQE_m = sum_qe_m / mappedNeuronsCnt
    println("Criterion : ")
    //println("sum_qe_m / mappedNeuronsCnt : " + sum_mqe_m + "/" + mappedNeuronsCnt + "=" + MQE_m) //mqe_change
    println("sum_qe_m / mappedNeuronsCnt : " + sum_qe_m + "/" + mappedNeuronsCnt + "=" + MQE_m)
    println("tau1 x parentNeuronQE : " + tau1 + "x" + this.parentNeuronQE + "=" + tau1 * this.parentNeuronQE)
    //println("tau1 x parentNeuronMQE : " + tau1 + "x" + this.parentNeuronMQE) //mqe_change

    if (MQE_m > tau1 * this.parentNeuronQE) {  
      (true, MQE_m , maxQeNeuron)
    }
    else {
      (false, MQE_m , maxQeNeuron)
    }

    /*
    if (MQE_m > tau1 * this.parentNeuronMQE) {  
      (true, MQE_m , maxMqeNeuron)
    }
    else {
      (false, MQE_m , maxMqeNeuron)
    }
     * 
     */
  }

  def growSingleRowColumn(errorNeuron : Neuron) {

    val dissimilarNeighbour = getMostDissimilarNeighbour(errorNeuron)

    neurons = getGrownLayer(NeuronPair(errorNeuron, dissimilarNeighbour))

  }

  /**
   * Grows the layer adding rows/columns. 
   * This method runs on the driver completely
   * @param tau1 parameter controlling the horizontal growth of a layer
   * @param mqe_u mean quantization error of the parent neuron of the layer 
   */
  def growMultipleCells(tau1 : Double) {
    //var neuronPairSet = getNeuronAndNeighbourSetForGrowing(tau1 * parentNeuronMQE)// mqe_change
    var neuronPairSet = getNeuronAndNeighbourSetForGrowing(tau1 * parentNeuronQE)

    for (pair <- neuronPairSet) {
      println("Neurons to Expand")  
      println(pair)
    }

    neurons = getGrownLayer(neuronPairSet)
  }

  def getNeuronsForHierarchicalExpansion(criterion : Double, instanceCount : Long) : mutable.Set[Neuron] = {
    val neuronSet = new mutable.HashSet[Neuron]()

    neurons.foreach { 
      neuronRow => 
        neuronRow.foreach { 
          neuron => 
            if (neuron.mappedInstanceCount > GHSomConfig.HIERARCHICAL_COUNT_FACTOR * instanceCount &&
              neuron.qe > criterion /* mqe_change neuron.mqe > tau2 * parentNeuronMQE */) { 
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
      new PairRDDFunctions[String, List[(Int, String, Instance)]](
        dataset.map{
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

    val strNeurons = 
      neurons.map(row => {
                    row.map(neuron => neuron.neuronInstance.attributeVector.mkString(","))
                       .mkString("|")
                    }
             )
             .mkString("\n")

      val filename = "SOMLayer_CodebookVectors" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuronID + ".data"

      val encoding : String = null

      FileUtils.writeStringToFile(new File(filename), strNeurons, encoding)

      val mappedInstances = 
        neurons.map(row => 
                       row.map(neuron => neuron.mappedInstanceCount.toString())
                          .mkString("|")
                   )
                   .mkString("\n")

      val mappedInstanceFileName = "SOMLayer_MappedInstance_" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuronID + ".data"

      FileUtils.writeStringToFile(new File(mappedInstanceFileName), mappedInstances, encoding)
              
      val mappedInstanceLabels = neurons.map(row => 
          row.map(neuron => neuron.mappedInstanceLabels.mkString(","))
            .mkString("|")
            )
            .mkString("\n")

      val mappedInstanceLabelsFileName = "SOMLayer_MappedInstanceLabels_" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuronID + ".data"

      FileUtils.writeStringToFile(new File(mappedInstanceLabelsFileName), mappedInstanceLabels, encoding)
  }

  private def updateNeuronMQEs(
    tuple : (String, (Double, Double, Long, Set[String]))) : Unit = {

      val neuronRowCol = tuple._1.split(",")

      val (neuronRow, neuronCol) = (neuronRowCol(0).toInt, neuronRowCol(1).toInt)

      //println("Neuron - " + tuple._1 + "; MQE : " + tuple._2._1 + ";mappedInstance Count : " + tuple._2._3) 

      neurons(neuronRow)(neuronCol).mqe = tuple._2._1
      neurons(neuronRow)(neuronCol).qe = tuple._2._2
      neurons(neuronRow)(neuronCol).mappedInstanceCount = tuple._2._3
      neurons(neuronRow)(neuronCol).mappedInstanceLabels = tuple._2._4
      neurons(neuronRow)(neuronCol).clearMappedInputs()

  }
  
  private def updateNeuronMQEs(
    tuple : (String, (Double, Double, Long))) : Unit = {

      val neuronRowCol = tuple._1.split(",")

      val (neuronRow, neuronCol) = (neuronRowCol(0).toInt, neuronRowCol(1).toInt)

      //println("Neuron - " + tuple._1 + "; MQE : " + tuple._2._1 + ";mappedInstance Count : " + tuple._2._3) 

      neurons(neuronRow)(neuronCol).mqe = tuple._2._1
      neurons(neuronRow)(neuronCol).qe = tuple._2._2
      neurons(neuronRow)(neuronCol).mappedInstanceCount = tuple._2._3
      neurons(neuronRow)(neuronCol).clearMappedInputs()

  }


  private def getNeuronAndNeighbourSetForGrowing(
    criterion : Double
  ) : Set[NeuronPair] = {

    // find neurons which have high qe
    var neuronsToExpandList = new mutable.ListBuffer[Neuron]()

    for(neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        //if (neuron.mqe > criterion) { //mqe_change
        if (neuron.qe > criterion) {
          neuronsToExpandList += neuron
        }
        }
      }

      //neuronsToExpandList.sortWith(_.mqe > _.mqe) //mqe_change
      neuronsToExpandList = neuronsToExpandList.sortWith(_.qe > _.qe)

      val neuronNeighbourSet = mutable.Set[NeuronPair]()

      for (neuron <- neuronsToExpandList) {
        val dissimilarNeighbour = getMostDissimilarNeighbour(neuron)
        val neuronPair = NeuronPair(neuron, dissimilarNeighbour)

        val rowColNotExistsInSet = neuronNeighbourSet.forall { 
          setPair =>
            (setPair.isSameCol && neuronPair.isSameCol && 
              setPair.neuron1.row != neuronPair.neuron1.row) || 
            //setPair.neuron2.row != neuronPair.neuron2.row)) ||
            (setPair.isSameRow && neuronPair.isSameRow && 
              setPair.neuron1.column != neuronPair.neuron1.column)  
            //setPair.neuron2.column != neuronPair.neuron2.column))
        }

        if (rowColNotExistsInSet || neuronNeighbourSet.isEmpty ) {
          println("Adding neuron pair to set : " + neuronPair)
          neuronNeighbourSet += neuronPair
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

    }

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

      /* 
       * label_som
       * Input : (QE , Count, Set[String])
       * Output : (QE_sum, Count_sum, Set[String] Union)
       */
      def combineNeuronsQE( t1: (Double, Long, Set[String]), 
        t2: (Double, Long, Set[String]) 
        ) : (Double, Long, Set[String]) = {
          (t1._1 + t2._1, t1._2 + t2._2, t1._3.union(t2._3))
        }

      /* Input : (QE , Count)
       * Output : (QE_sum, Count_sum)
       */
      def combineNeuronsQE( t1: (Double, Long), 
        t2: (Double, Long) 
        ) : (Double, Long) = {
          (t1._1 + t2._1, t1._2 + t2._2)
        }

        /**
         * input : tuple of (qe, num_of_instances, Set[String])
         * output : tuple of (mqe , qe, num_of_instances, Set[String])
         */
        def computeMQEForNeuron(
          tup : (Double, Long, Set[String]) 
        ) : (Double, Double, Long, Set[String]) = {
          (tup._1/tup._2, tup._1, tup._2, tup._3)
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

      //def apply(parentNeuronID : String, parentLayer : Int, rowDim : Int, colDim : Int, parentNeuronMQE /*mqe_change*/ : Double, vectorSize : Int) = {
      def apply(parentNeuronID : String, parentLayer : Int, rowDim : Int, colDim : Int, parentNeuronQE : Double, vectorSize : Int) = {
        layerId += 1
        //new SOMLayer(layerId, rowDim, colDim, parentNeuronID, parentLayer, parentNeuronMQE /*mqe_change*/ , vectorSize )
        new SOMLayer(layerId, rowDim, colDim, parentNeuronID, parentLayer, parentNeuronQE, vectorSize )
      }

      def main(args : Array[String]) {
      }
      }
