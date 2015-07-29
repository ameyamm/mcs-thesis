package com.ameyamm.mcs_thesis.input_generator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.LogManager
import org.apache.commons
import org.apache.commons.io.FileUtils
import java.io.File


import com.ameyamm.mcs_thesis.ghsom.Instance
import com.ameyamm.mcs_thesis.ghsom.DoubleDimension
import com.ameyamm.mcs_thesis.ghsom.DimensionType
import com.ameyamm.mcs_thesis.ghsom.GHSom

/**
 * @author ameya
 */
class IrisDatasetReader (val dataset : RDD[String]) extends Serializable{
  
  private val datasetOfInstanceObjs = instanizeDataset(dataset)
  
  case class Iris( 
      sepalLength : DoubleDimension, 
      sepalWidth : DoubleDimension,
      petalLength : DoubleDimension,
      petalWidth : DoubleDimension,
      classValue : String 
      ) {
    def getInstanceObj = {
      val attributeVector : Array[DimensionType] = Array(sepalLength, sepalWidth, petalLength, petalWidth)
      Instance(classValue, attributeVector)
    }
  }
  
  def getDataset : RDD[Instance] = datasetOfInstanceObjs
  
  def printDataset() {
    // println(datasetRDD.count)
    println("AMU Dataset")
    println(datasetOfInstanceObjs.take(9).mkString("\n"))
  }
      
  def instanizeDataset(dataset : RDD[String]) : RDD[Instance] = {
    
    val irisDataset = convertToIrisRDD(dataset)
    
    val obj = irisDataset.first()
    
    println(">>>>>> " + obj.classValue + ":" + obj.petalLength)
    
    val attribMap : RDD[(String,DoubleDimension)] = 
      irisDataset.flatMap( 
                    iris => { 
                          List(
                                ("sepalLength",iris.sepalLength),
                                ("sepalWidth", iris.sepalWidth),
                                ("petalLength", iris.petalLength),
                                ("petalWidth", iris.petalWidth)
                              )
                    }
                  )
    
    val maxVector = attribMap.reduceByKey( DoubleDimension.getMax _).collectAsMap()
    val minVector = attribMap.reduceByKey( DoubleDimension.getMin _).collectAsMap()
    
    val maxfilename = "maxVector.data"
    val encoding : String = null
    val maxVectorString = maxVector.toList
                                   .map(tup => (tup._1, tup._2))
                                   .sortWith(_._1 < _._1)
                                   .map(tup => tup._2)
                                   .mkString(",")

    FileUtils.writeStringToFile(new File(maxfilename), maxVectorString, encoding)
    
    val minfilename = "minVector.data"
    val minVectorString = minVector.toList
                                   .map(tup => (tup._1, tup._2))
                                   .sortWith(_._1 < _._1)
                                   .map(tup => tup._2)
                                   .mkString(",")
    
    irisDataset.map( iris =>
                  Iris(
                        {
                          if (maxVector("sepalLength") - minVector("sepalLength") != DoubleDimension(0)) 
                            (iris.sepalLength - minVector("sepalLength")) / (maxVector("sepalLength") - minVector("sepalLength"))
                          else 
                            DoubleDimension(0) 
                        },
                        {
                          if (maxVector("sepalWidth") - minVector("sepalWidth") != DoubleDimension(0)) 
                            (iris.sepalWidth - minVector("sepalWidth")) / (maxVector("sepalWidth") - minVector("sepalWidth"))
                          else 
                            DoubleDimension(0)
                        },
                        {
                          if (maxVector("petalLength") - minVector("petalLength") != DoubleDimension(0)) 
                            (iris.petalLength - minVector("petalLength")) / (maxVector("petalLength") - minVector("petalLength"))
                          else 
                            DoubleDimension(0)
                        },
                        {
                          if (maxVector("petalWidth") - minVector("petalWidth") != DoubleDimension(0)) 
                            (iris.petalWidth - minVector("petalWidth")) / (maxVector("petalWidth") - minVector("petalWidth"))
                          else 
                            DoubleDimension(0)
                        },
                        iris.classValue
                      ).getInstanceObj
                )
  }
  
  private def convertToIrisRDD( dataset : RDD[String] ) : RDD[Iris] = {
    
    dataset.map( line => {
      val array = line.split(',')
      Iris(
          DoubleDimension(array(0).toDouble),
          DoubleDimension(array(1).toDouble),
          DoubleDimension(array(2).toDouble),
          DoubleDimension(array(3).toDouble),
          array(4)
          )
    })
  }
  
}


object IrisDatasetReader {
  
  val logger = LogManager.getLogger("Iris")
  
  def main(args : Array[String]) {
    val conf = new SparkConf(true)
               .setAppName("Iris Dataset Reader")
               .set("spark.storage.memoryFraction","0")
               .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .set("spark.default.parallelism","8")
               .set("spark.kryoserializer.buffer.max","400")
               .setMaster("spark://192.168.101.13:7077")

    val sc = new SparkContext(conf) 

    /*
    val maxVector = Array.fill(10)(DoubleDimension.MinValue)
    val attribVector = Array.fill(10)(DoubleDimension.getRandomDimensionValue)
    println(maxVector.mkString)
    println(attribVector.mkString)

    for ( i <- 0 until attribVector.size ) { 
        maxVector(i) = if (attribVector(i) > maxVector(i)) attribVector(i) else maxVector(i)
    } 
    println(maxVector.mkString)
    */
    val dataset = sc.textFile("hdfs://192.168.101.13:9000/user/ameya/datasets/iris/iris.data")
    println(dataset.first())
    val datasetReader = new IrisDatasetReader(dataset) 
    datasetReader.printDataset()
    val irisDataset = datasetReader.getDataset
    
    val ghsom = GHSom()
    
    ghsom.train(irisDataset)
  }
}
