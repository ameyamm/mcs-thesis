package com.ameyamm.mcs_thesis.input_generator

import org.apache.spark.rdd.RDD

/**
 * @author ameya
 */
abstract class DatasetReader extends Serializable {
  
  def vectorSize() : Int
 
}
