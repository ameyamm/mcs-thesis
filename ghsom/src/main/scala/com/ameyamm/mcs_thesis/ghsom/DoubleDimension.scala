package com.ameyamm.mcs_thesis.ghsom

import scala.util.Random
import org.apache.commons.lang.IllegalClassException

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

  override def compare( that : DimensionType ) : Int = {
    that match {
      case thatObj : DoubleDimension => {
                                          val x = this.value - thatObj.value
                                          if (x == 0) return 0
                                          else if (x > 0) return 1
                                          else return -1
                                        }
      case _ => throw new IllegalClassException("Illegal class in DoubleDimension")
    }
  }
}

object DoubleDimension {
  
  val MinValue = DoubleDimension(Double.MinValue)
  val MaxValue = DoubleDimension(Double.MaxValue)
  
  def apply(value : Double) : DoubleDimension = {
    new DoubleDimension(value)
  }
  
  def apply() : DoubleDimension = {
    new DoubleDimension(0)
  }
  
  def getRandomDimensionValue() : DoubleDimension = {
    new DoubleDimension(Random.nextDouble())
  }
}
