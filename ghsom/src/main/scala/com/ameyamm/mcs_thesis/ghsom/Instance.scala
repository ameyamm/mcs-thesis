package com.ameyamm.mcs_thesis.ghsom

import scala.math.sqrt

/**
 * @author ameya
 */

class Instance( private val _label : String, private val _attributeVector : Array[DimensionType] ) extends Serializable {
  
  def label = _label 
  
  def attributeVector = _attributeVector
  
  override def toString() : String = {
    return _label + "::>" + attributeVector.mkString(":")
  }
  
  def +(that : Instance) : Instance = {
    new Instance("+", 
                  this.attributeVector.zip(that.attributeVector).map( t => t._1 + t._2 )               
        )
  }
  
  def -(that : Instance) : Instance = {
    new Instance ("-",
                  this.attributeVector.zip(that.attributeVector).map(t => t._1 - t._2))
  }

  def getDistanceFrom( other : Instance ) : Double = {
    sqrt(this.attributeVector.zip(other.attributeVector).map(t => t._1.getDistanceFrom(t._2)).reduce(_ + _))
  }
  
  def /(num : Double) : Instance = {
    new Instance(this.label, this.attributeVector.map(value => value/num))
  }
  
}

object InstanceFunctions {
  def getAttributeVectorWithNeighbourhoodFactor(instance : Instance, factor : Double) : Array[DimensionType] = {
    instance.attributeVector.map { attrib => attrib * factor }  
  }
  
  def getAverageInstance(instances : Instance*) : Instance = {
    
    var avgAttributeVector : Array[DimensionType] = instances(0).attributeVector.map(elem => elem.cloneMe)
    
    var skippedFirst = false
    
    for (instance <- instances) {
      if (!skippedFirst)
        skippedFirst = true 
      else {
        avgAttributeVector = avgAttributeVector.zip(instance.attributeVector).map(t => t._1 + t._2)
      }
    } 
    
    avgAttributeVector = avgAttributeVector.map(elem => elem / instances.size)
    
    new Instance( 
        "average instance",
        avgAttributeVector
        )
  }
  
  def getQEInstance(instance1 : Instance, instance2 : Instance) : Instance = {
    new Instance("qe", instance1.attributeVector.zip(instance2.attributeVector).map(tup => (tup._1 - tup._2).getAbs))
  }
}

object Instance extends Serializable {
  def apply(label : String, attributeVector : Array[DimensionType]) = {
    new Instance (label, attributeVector)
  }
  
  def averageInstance(instance1 : Instance, instance2 : Instance) : Instance = {
    Instance("avgInstance", instance1.attributeVector.zip(instance2.attributeVector).map(t => (t._1 + t._2) / 2))
  }
}
