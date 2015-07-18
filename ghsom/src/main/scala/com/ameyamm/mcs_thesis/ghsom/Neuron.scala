package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */

import scala.collection.mutable
import scala.collection.immutable
import scala.math.{exp,pow}

class Neuron (
    private val _row : Int, 
    private val _column : Int, 
    private var _neuronInstance : Instance = null
  ) extends Serializable {
  private val _mappedInputs : mutable.ListBuffer[Instance] = mutable.ListBuffer()
  
  private var _qe : Double = 0
  
  private var _mqe : Double = 0
  
  private var _mappedInstanceCount : Long = 0
  
  /* Getters and setters */
  def row : Int = _row 
  
  def column : Int = _column
  
  def neuronInstance : Instance = _neuronInstance 
  
  def neuronInstance_= (instance : Instance) : Unit = _neuronInstance = instance 
  
  def id : String = row.toString() + "," + column.toString() 
  
  def mappedInputs : mutable.ListBuffer[Instance] = _mappedInputs
  
  def mqe : Double = _mqe
  
  def mqe_= (value : Double) : Unit = _mqe = value
  
  def qe : Double = _qe
  
  def qe_= (value : Double) : Unit = _qe = value
  
  def mappedInstanceCount : Long = _mappedInstanceCount

  def mappedInstanceCount_= (value : Long): Unit = _mappedInstanceCount = value
  
  /* Methods */
  def addToMappedInputs(instance : Instance) {
    _mappedInputs += instance
  }
  
  def addToMappedInputs(instances : mutable.ListBuffer[Instance]) {
    _mappedInputs ++= instances
  }
  
  def clearMappedInputs() {
    _mappedInputs.clear()
  }
  
  def computeNeighbourhoodFactor(neuron : Neuron, iteration : Long) : Double = {
    exp(-(pow(this.row - neuron.row, 2) + pow(this.column - neuron.column , 2)/pow(iteration,2)))    
  }
  
  
  
  override def toString() : String = {
    var neuronString = "[" + row + "," + column + "]" + "(" + qe + ":" + mqe + ")" + ":" + "["
    
    neuronInstance.attributeVector.foreach( attrib => neuronString += (attrib.toString() + ",") )
    
    neuronString += "]"
    
    neuronString
  }
  
  override def equals( obj : Any ) : Boolean = {
    obj match {
      case o : Neuron => o.id.equals(this.id)
      case _ => false 
    }
  }
  
  override def hashCode = id.hashCode()
  
}

object NeuronFunctions {
  def getAttributeVectorWithNeighbourhoodFactor(neuron : Neuron, factor : Double) : Array[DimensionType] = {
    neuron.neuronInstance.attributeVector.map { attrib => attrib * factor }  
  }
}

object Neuron {
  def apply(row : Int, column: Int, neuronInstance : Instance) : Neuron = {
    new Neuron(row, column, neuronInstance)
  }
}