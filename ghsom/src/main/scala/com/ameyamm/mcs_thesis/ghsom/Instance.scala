package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */

import jdk.nashorn.internal.ir.annotations.Immutable

class Instance( private val _label : String, private val _attributeVector : Array[_ <: DimensionType] ) extends Serializable {
  
  def label = _label 
  
  def attributeVector = _attributeVector
  
  override def toString() : String = {
    return _label + "::>" + attributeVector.mkString(":")
  }
  
}

object Instance extends Serializable {
  def apply(label : String, attributeVector : Array[_ <: DimensionType]) = {
    new Instance (label, attributeVector)
  }
}
