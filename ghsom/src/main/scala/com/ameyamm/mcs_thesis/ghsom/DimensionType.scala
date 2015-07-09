package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */

/**
 * Trait for individual dimension of an attribute vector
 */
trait DimensionType extends Serializable with Ordered[DimensionType] {
  def getRandomDimensionValue() : DimensionType
  def equals( d2 : DimensionType ) : Boolean
  override def toString() : String
}