package com.ameyamm.mcs_thesis.utils

import scala.collection.immutable
import com.ameyamm.mcs_thesis.ghsom.DimensionType
import com.ameyamm.mcs_thesis.ghsom.DoubleDimension

/**
 * @author ameya
 */
object Utils {
 def generateRandomVector( randomDimensionFunction : () => DimensionType) : Vector[DimensionType] = {
    immutable.Vector.tabulate(Constants.DIMENSION_VECTOR_SIZE)( i => randomDimensionFunction() )
 } 
 
 def generateRandomArray( randomDimensionFunction : () => DimensionType, vectorSize : Int) : Array[DimensionType] = {
    Array.tabulate(vectorSize)( i => randomDimensionFunction() )
 } 
 
 def generateArrayOfGivenValue(value : Double, vectorSize : Int ) : Array[DimensionType] = {
   Array.tabulate(vectorSize)(i => DoubleDimension(value))
 }
}