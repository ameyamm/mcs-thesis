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
    new Instance( this.label + "," + that.label, 
                  this.attributeVector.zip(that.attributeVector).map( t => t._1 + t._2 )               
        )
  }
  
  def getDistanceFrom( other : Instance ) : Double = {
    sqrt(this.attributeVector.zip(other.attributeVector).map(t => t._1.getDistanceFrom(t._2)).reduce(_ + _))
  }
  
}

object InstanceFunctions {
  def getAttributeVectorWithNeighbourhoodFactor(instance : Instance, factor : Double) : Array[DimensionType] = {
    instance.attributeVector.map { attrib => attrib * factor }  
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
