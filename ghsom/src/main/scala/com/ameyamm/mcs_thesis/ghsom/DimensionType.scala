package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */

/**
 * Trait for individual dimension of an attribute vector
 */
trait DimensionType extends Serializable with Ordered[DimensionType] {
  def getDistanceFrom( that : DimensionType ) : Double 
  def getRandomDimensionValue() : DimensionType
  def equals( d2 : DimensionType ) : Boolean
  override def toString() : String
  def +(d2 : DimensionType) : DimensionType
  def -(d2 : DimensionType) : DimensionType
  def /(d2 : DimensionType) : DimensionType
  def *(d2 : DimensionType) : DimensionType 
  def !=(d2 : DimensionType) : Boolean
  def ==(d2 : DimensionType) : Boolean
  def <(d2 : DimensionType) : Boolean
  def <=(d2 : DimensionType) : Boolean
  def >(d2 : DimensionType) : Boolean
  def >=(d2 : DimensionType) : Boolean
  def /(num : Long) : DimensionType
  def /(num : Double) : DimensionType
  def *(num : Double) : DimensionType
  def cloneMe : DimensionType 
  def getAbs : DimensionType
  def isZero : Boolean
  def getValue : Double
}

