package com.ameyamm.mcs_thesis.ghsom

import scala.util.Random
import org.apache.commons.lang.IllegalClassException
import scala.math.sqrt
import scala.compat.Platform

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
  
  def !=( that : Double ) : Boolean = {
    this.value != that
  }
  
  def -( that : DoubleDimension ) = {
    DoubleDimension(this.value - that.value)
  }
  
  override def -( that : DimensionType ) = {
    that match {
      case d : DoubleDimension =>DoubleDimension(this.value - d.value) 
      case _ => throw new IllegalClassException("Illegal class in DoubleDimension")
    }
  }
  
  override def /( that : DimensionType ) = {
    that match {
      case d : DoubleDimension =>DoubleDimension(this.value / d.value) 
      case _ => throw new IllegalClassException("Illegal class in DoubleDimension")
    }
  }
  
  def /( that : DoubleDimension ) = {
    DoubleDimension(this.value / that.value) 
  }
  
  override def /( num : Long ) : DimensionType = {
    DoubleDimension(this.value / num)
  } 

  override def /( num : Double) : DimensionType = {
    DoubleDimension(this.value / num)
  } 
  
  override def toString : String = {
    this.value.toString
  }
  
  override def + (that : DimensionType) : DimensionType = {
    that match {
      case d : DoubleDimension => new DoubleDimension(this.value + d.value)
      case _ => throw new IllegalClassException("Illegal class in DoubleDimension")
    }
  }
  
  override def * (multFactor : Double) : DimensionType = {
    new DoubleDimension(this.value * multFactor)
  }

  override def getDistanceFrom(that : DimensionType) : Double = {
    that match {
      case d : DoubleDimension => (this.value - d.value) * (this.value - d.value)
      case _ => throw new IllegalClassException("Illegal class in DoubleDimension")
    }
  }
  
  override def cloneMe : DimensionType = {
    new DoubleDimension(this.value)
  }
}

object DoubleDimension {
  
  val MinValue = DoubleDimension(Double.MinValue)
  val MaxValue = DoubleDimension(Double.MaxValue)
  val randomGenerator = new Random(Platform.currentTime)
  
  def apply(value : Double) : DoubleDimension = {
    new DoubleDimension(value)
  }
  
  def apply() : DoubleDimension = {
    new DoubleDimension(0)
  }
  
  def getRandomDimensionValue() : DoubleDimension = {
    new DoubleDimension(randomGenerator.nextDouble())
  }
  
  def getMax( a : DoubleDimension, b : DoubleDimension ) : DoubleDimension = {
    if (a > b) a 
    else b 
    }
  
  def getMin( a : DoubleDimension, b : DoubleDimension) : DoubleDimension = {
    if (a < b) a 
    else b 
    }

}
