package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */

import scala.collection.immutable
import jdk.nashorn.internal.ir.annotations.Immutable

class Instance( private val _label : String, private val _attributeVector : immutable.Vector[DimensionType] ) {
  
  def label = _label 
  
  def attributeVector = _attributeVector
  
}

object Instance {
  def apply(label : String, attributeVector : immutable.Vector[DimensionType]) = {
    new Instance (label, attributeVector)
  }
}