package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */

import scala.collection.immutable

class Instance(label : String, attributeVector : immutable.Vector[Dimension] ) {
  
  def getLabel = label 
  
  def getAttributeVector = attributeVector
  
}