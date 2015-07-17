package com.ameyamm.mcs_thesis.ghsom

import com.ameyamm.mcs_thesis.utils.Utils
import com.ameyamm.mcs_thesis.globals.GHSomConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import scala.collection.mutable

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
    
    for ( iteration <- 0 until GHSomConfig.EPOCHS ) {
        val t = GHSomConfig.EPOCHS - iteration 
        
        // neuronUpdatesRDD is a RDD of (numerator, denominator) for the update of neurons at the end of epoch
        val neuronUpdatesRDD = dataset.flatMap { instance => 
                                  val bmu : Neuron = this.findBMU(instance)
                                  
                                  val temp = neurons.flatten
                                  
                                  temp.map { neuron => 
                                    val neighbourhoodFactor = neuron.computeNeighbourhoodFactor(bmu, t)
                                   
                                    (neuron.id, (neuron.getAttributeVectorWithNeighbourhoodFactor(neighbourhoodFactor), neighbourhoodFactor))
                                  }
                               }

        //              neuronid, [(num,den),...] => neuronid, (SUM(num), SUM(den)) => neuronid, num/den
       
        val updatedModel = new PairRDDFunctions[String, (Array[DimensionType],Double)](neuronUpdatesRDD)
                                   .reduceByKey(combineNeuronUpdates _).mapValues(computeUpdatedVector _).collectAsMap()
                                   
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
    val neuronsQE = new PairRDDFunctions[String, (Double, Long)](
                    dataset.map { instance => 
                        val bmu = findBMU(instance)
                        //bmu.addToMappedInputs(instance)
                        val qe = bmu.neuronInstance.getDistanceFrom(instance)
                        (bmu.id,(qe,1L))         
                        //bmu.qe = bmu.qe + bmu.neuronInstance.getDistanceFrom(instance)
                    })
    
    val neuronMQEs = neuronsQE.reduceByKey(combineNeuronsQE _).mapValues(computeMQENeuron _)
    println("AMU neuronMQE")
    neuronMQEs.take(5).foreach(t => println("ID : " + t._1 + "\nMQE : " + t._2._1 + "\n QE:" + t._2._2 +"\nMapped Count:" + t._2._3))
    neuronMQEs.collectAsMap.map(updateNeuronMQEs)
  }
  
  private def combineNeuronsQE( t1: (Double, Long), 
                                t2: (Double, Long) 
                              ) : (Double, Long) = {
    (t1._1 + t2._1, t1._2 + t2._2)
  }
  
  /**
   * input : tuple of (qe, num_of_instances)
   * output : tuple of (mqe , qe, num_of_instances)
   */
  private def computeMQENeuron(
      tup : (Double, Long) 
      ) : (Double, Double, Long) = {
    (tup._1/tup._2, tup._1, tup._2)
  }
  
  private def updateNeuronMQEs(
      tuple : (String, (Double, Double, Long))) : Unit = {
    val neuronRowCol = tuple._1.split(",")
    val (neuronRow, neuronCol) = (neuronRowCol(0).toInt, neuronRowCol(1).toInt)
    
    neurons(neuronRow)(neuronCol).mqe = tuple._2._1
    neurons(neuronRow)(neuronCol).qe = tuple._2._2
    neurons(neuronRow)(neuronCol).mappedInstanceCount = tuple._2._3
    neurons(neuronRow)(neuronCol).clearMappedInputs()
  }

  def clearMappedInputs {
    for (neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        neuron.clearMappedInputs()
      }
    }  
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
  }
  
  def grow(tau1 : Double, qe_u : Double) : Boolean = {

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
    
    println("GROWTH >>>> " + mqe_m + ":" + mappedNeuronsCnt)
    if (mqe_m / mappedNeuronsCnt > tau1 * qe_u) {
      var neuronPair : NeuronPair = getNeuronAndNeighbourForGrowing(maxMqeNeuron)
      neurons = getGrownLayer(neuronPair)
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
  
  private def getGrownLayer(neuronPair : NeuronPair) : Array[Array[Neuron]] = {
    
    var newNeurons : Array[Array[Neuron]] = null
    // add a row
    if (neuronPair.neuron1.row != neuronPair.neuron2.row) { 
      newNeurons = getRowAddedLayer(neuronPair)
    }
    else { // add a column
      newNeurons = getColumnAddedLayer(neuronPair)
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
                             
}

object SOMLayer {
  
  val EPOCHS : Int = GHSomConfig.EPOCHS
  
  def apply(rowDim : Int, colDim : Int, vectorSize : Int) = {
    new SOMLayer(rowDim, colDim,vectorSize)
  }
  
  def main(args : Array[String]) {
    val somlayer = SOMLayer(4,4,68)
    somlayer.display()
    println(somlayer)
  }
}