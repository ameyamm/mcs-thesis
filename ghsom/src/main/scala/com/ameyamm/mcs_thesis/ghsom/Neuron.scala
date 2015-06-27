package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */

import scala.collection.mutable

class Neuron (private val _row : String, private val _column : String, private val _weightVector : Vector[Dimension]){
  private val mappedInputs : mutable.ListBuffer[Instance] = mutable.ListBuffer()
  
  def row : String = _row 
  
  def column : String = _column
  
  def weightVector : Vector[Dimension] = _weightVector
  
  def addToMappedInputs(instance : Instance) {
    mappedInputs += instance
  }
  
}

object Neuron {
  def apply(row : String, column: String, weightVector : Vector[Dimension]) : Neuron = {
    new Neuron(row, column, weightVector)
  }
}