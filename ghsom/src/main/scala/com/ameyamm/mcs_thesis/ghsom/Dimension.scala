package com.ameyamm.mcs_thesis.ghsom

/**
 * @author ameya
 */
abstract class Dimension[T](protected var _value : T) extends DimensionType { 
  def value : T = _value
  def value_=(value : T) : Unit = { _value = value  }
}