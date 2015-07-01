package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */

import scala.collection.mutable
import scala.collection.immutable

class Neuron (private val _row : String, private val _column : String, private val _attributeVector : immutable.Vector[DimensionType] = null){
  private val mappedInputs : mutable.ListBuffer[Instance] = mutable.ListBuffer()
  
  def row : String = _row 
  
  def column : String = _column
  
  def attributeVector : Vector[DimensionType] = _attributeVector
  
  def addToMappedInputs(instance : Instance) {
    mappedInputs += instance
  }
  
}

object Neuron {
  def apply(row : String, column: String, attributeVector : immutable.Vector[DimensionType]) : Neuron = {
    new Neuron(row, column, attributeVector)
  }
}