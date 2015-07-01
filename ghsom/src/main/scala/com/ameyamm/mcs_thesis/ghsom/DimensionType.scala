package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */

/**
 * Trait for individual dimension of an attribute vector
 */
trait DimensionType {
  def getRandomDimensionValue() : DimensionType
  def equals( d2 : DimensionType ) : Boolean
}