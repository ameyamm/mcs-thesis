package com.ameyamm.mcs_thesis.input_generator

/**
 * @author ameya
 */
import com.ameyamm.mcs_thesis.ghsom.Instance
import com.ameyamm.mcs_thesis.ghsom.DoubleDimension
import com.ameyamm.mcs_thesis.ghsom.DimensionType
import com.ameyamm.mcs_thesis.ghsom.GHSom
import com.ameyamm.mcs_thesis.globals.GHSomConfig
import com.ameyamm.mcs_thesis.ghsom.Attribute
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.LogManager
import org.apache.commons
import org.apache.commons.io.FileUtils
import java.io.File

class AnimalDataset (val dataset : RDD[String]) extends Serializable{
  
  val attributeHeaderNames = Array("hair","feathers", "eggs", "milk", 
      "airborne", "aquatic", "predator", "toothed", "backbone", 
      "breathes", "venomous", "fins", "legs", "tail","domestic", "catsize", "type" )
  
  val attributes : Array[Attribute] = Array.ofDim(attributeHeaderNames.size)
  
  private val datasetOfInstanceObjs = instanizeDataset(dataset)
  
  def printDataset() {
    // println(datasetRDD.count)
  }
  
  def getDataset : RDD[Instance] = datasetOfInstanceObjs
  
  case class Data ( className : String, attributeVector : Array[DoubleDimension] ) {
    def getInstanceObj = {
      Instance(className, attributeVector.asInstanceOf[Array[DimensionType]])
    }
  }
  
  def instanizeDataset(dataset : RDD[String]) : RDD[Instance] = {
    
    val data = convertToDataRDD(dataset)
    
    val obj = data.first()
    
    val attribMap : RDD[(String,DoubleDimension)] = 
      data.flatMap( 
                    rec => { 
                          val indexElem = rec.attributeVector.zipWithIndex
                          indexElem.map(tup => (tup._2.toString(), tup._1)).toList
                    }
                  )
    
    val maxVector = attribMap.reduceByKey( DoubleDimension.getMax _).collectAsMap()
    val minVector = attribMap.reduceByKey( DoubleDimension.getMin _).collectAsMap()
    
    for (i <- 0 until attributes.size) {
      attributes(i) = Attribute(attributeHeaderNames(i), 
                                maxVector(i.toString()), 
                                minVector(i.toString()))
    }
    
    /*
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

    FileUtils.writeStringToFile(new File(minfilename), minVectorString, encoding)
    */
    
    data.map( rec => 
                  Data( 
                        rec.className,
                        rec.attributeVector.zipWithIndex
                                           .map( tup => (tup._1 - minVector(tup._2.toString)) / (maxVector(tup._2.toString) - minVector(tup._2.toString))) 
                      ).getInstanceObj
        )
  }
  
  def convertToDataRDD(dataset : RDD[String]) : RDD[Data] = {
    dataset.map( line => {
      val array = line.split(',')
      Data(
          array(0),
          array.slice(from = 1, until = array.length).map { x => DoubleDimension(x.toDouble) }          
          )
    })
  }
}

object AnimalDataset {
  
  val logger = LogManager.getLogger("Iris")
  
  def main(args : Array[String]) {
    val conf = new SparkConf(true)
               .setAppName("Animal")
               .set("spark.storage.memoryFraction","0")
               .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .set("spark.default.parallelism","8")
               .set("spark.kryoserializer.buffer.max","400")
               .setMaster("spark://192.168.101.13:7077")

    val sc = new SparkContext(conf) 
    
    var epochs = GHSomConfig.EPOCHS
    if(args.length >= 1) {
      epochs = args(0).toInt  
    }
    
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
    val dataset = sc.textFile("hdfs://192.168.101.13:9000/user/ameya/datasets/animal/animal.data")
    val datasetReader = new AnimalDataset(dataset) 
    datasetReader.printDataset()
    val processedDataset = datasetReader.getDataset
    
    val ghsom = GHSom()
    
    ghsom.train(processedDataset, datasetReader.attributes, epochs)
  }
}