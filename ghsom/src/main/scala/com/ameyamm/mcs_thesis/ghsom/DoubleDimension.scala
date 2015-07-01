package com.ameyamm.mcs_thesis.ghsom

import scala.util.Random

/**
 * @author ameya
 */

class DoubleDimension(_value : Double) extends Dimension[Double](_value) {
  
  override def equals( d2 : DimensionType ) : Boolean = {
     d2 match {
       case otherDoubleDimension : DoubleDimension => otherDoubleDimension.value == value
       case _ => false 
     } 
  }
  
  def getRandomDimensionValue() : DoubleDimension = {
    new DoubleDimension(Random.nextDouble())
  }
}

object DoubleDimension {
  def apply(value : Double) : DoubleDimension = {
    new DoubleDimension(value)
  }
  
  def getRandomDimensionValue() : DoubleDimension = {
    new DoubleDimension(Random.nextDouble())
  }
}