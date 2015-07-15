package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */

import scala.collection.mutable
import scala.collection.immutable
import scala.math.{exp,pow}

class Neuron (private val _row : Long, private val _column : Long, private val _neuronInstance : Instance = null){
  private val mappedInputs : mutable.ListBuffer[Instance] = mutable.ListBuffer()
  
  def row : Long = _row 
  
  def column : Long = _column
  
  def neuronInstance : Instance = _neuronInstance
  
  def index : String = "[" + row.toString() + "," + column.toString() + "]"
  
  def addToMappedInputs(instance : Instance) {
    mappedInputs += instance
  }
  
  def computeNeighbourhoodFactor(neuron : Neuron, iteration : Long) : Double = {
    exp(-(pow(this.row - neuron.row, 2) + pow(this.column - neuron.column , 2)/pow(iteration,2)))    
  }
  
  def getAttributeVectorWithNeighbourhoodFactor(factor : Double) : Array[DimensionType] = {
    neuronInstance.attributeVector.map { attrib => attrib * factor }  
  }
  
  override def toString() : String = {
    var neuronString = "[" + row + "," + column + "]" + "(" + neuronInstance.attributeVector.size + ")" + ":" + "["
    
    neuronInstance.attributeVector.foreach( attrib => neuronString += (attrib.toString() + ",") )
    
    neuronString += "]"
    
    neuronString
  }
  
}

object Neuron {
  def apply(row : Long, column: Long, neuronInstance : Instance) : Neuron = {
    new Neuron(row, column, neuronInstance)
  }
}