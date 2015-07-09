package com.ameyamm.mcs_thesis.utils

import scala.collection.immutable
import com.ameyamm.mcs_thesis.ghsom.DimensionType

/**
 * @author ameya
 */
object Utils {
 def generateRandomVector( randomDimensionFunction : () => DimensionType) : Vector[DimensionType] = {
    immutable.Vector.tabulate(Constants.DIMENSION_VECTOR_SIZE)( i => randomDimensionFunction() )
 } 
}