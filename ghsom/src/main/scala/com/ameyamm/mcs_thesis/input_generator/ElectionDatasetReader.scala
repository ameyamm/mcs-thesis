package com.ameyamm.mcs_thesis.input_generator

/**
 * @author ameya
 */

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.rdd.RDD

import com.ameyamm.mcs_thesis.ghsom.Instance
import com.ameyamm.mcs_thesis.ghsom.DimensionType
import com.ameyamm.mcs_thesis.ghsom.DoubleDimension

class ElectionDatasetReader(private val datasetRDD : CassandraRDD[CassandraRow]) extends DatasetReader {
  
  //private val datasetRDD = sc.cassandraTable("uscensus1990", "dataset")

  private val _vectorSize = datasetRDD.first.size 
  private val datasetOfInstanceObjs = instanizeDataset(datasetRDD)
  
  def printDataset() {
   // println(datasetRDD.count)
    println(datasetRDD.first)
  }
  
  def vectorSize : Int = _vectorSize 
  
  def instanizeDataset(dataset : CassandraRDD[CassandraRow]) : RDD[Instance]= {
    var maxVector : Array[DoubleDimension] = Array.fill(vectorSize)(DoubleDimension.MinValue)
    var minVector : Array[DoubleDimension] = Array.fill(vectorSize)(DoubleDimension.MaxValue)
    
    var instanceDataset = dataset.map( row => convertToInstance(row) )
    
   // instanceDataset = 
    instanceDataset.foreach( 
                              instance => { 
                                val attribVector = instance.attributeVector.map( 
                                                      x => x match { 
                                                        case dd : DoubleDimension => dd 
                                                        case _ => throw new ClassCastException  
                                                      }  
                                                   ) 
                                for ( i <- 0 until attribVector.size ) { 
                                    maxVector(i) = if (attribVector(i) > maxVector(i)) attribVector(i) else maxVector(i)
                                    minVector(i) = if (attribVector(i) < minVector(i)) attribVector(i) else minVector(i) 
                                } 
    //                            instance
                              } 
                      )
                   
     val maxstr = maxVector.mkString(",")               
     val minstr = minVector.mkString(",")      
     println(">>>>>First instance")
     instanceDataset.first().attributeVector.foreach { x => print(x + ",")}
     println(">>>>>>>MAXSTR")
     println(maxstr)
     println(">>>>>>>MINSTR")
     println(minstr)
     instanceDataset

  }
  
  def convertToInstance(row : CassandraRow) : Instance = {
    
    val label = row.getString("caseid")
    val attribVector : Array[DoubleDimension] = Array.fill(vectorSize - 1)(DoubleDimension())
    
    val dage = row.getLongOption("dage") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dage
    val dancstry1 = row.getLongOption("dancstry1") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dancstry1
    val dancstry2 = row.getLongOption("dancstry2") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dancstry2
    val ddepart = row.getLongOption("ddepart") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ ddepart
    val dhispanic = row.getLongOption("dhispanic") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dhispanic
    val dhour89 = row.getLongOption("dhour89") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dhour89
    val dhours = row.getLongOption("dhours") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dhours
    val dincome1 = row.getLongOption("dincome1") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dincome1
    val dincome2 = row.getLongOption("dincome2") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dincome2
    val dincome3 = row.getLongOption("dincome3") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dincome3
    val dincome4 = row.getLongOption("dincome4") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dincome4
    val dincome5 = row.getLongOption("dincome5") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dincome5
    val dincome6 = row.getLongOption("dincome6") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dincome6
    val dincome7 = row.getLongOption("dincome7") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dincome7
    val dincome8 = row.getLongOption("dincome8") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dincome8
    val dindustry = row.getLongOption("dindustry") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dindustry
    val doccup = row.getLongOption("doccup") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ doccup
    val dpob = row.getLongOption("dpob") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dpob
    val dpoverty = row.getLongOption("dpoverty") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dpoverty
    val dpwgt1 = row.getLongOption("dpwgt1") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dpwgt1
    val drearning = row.getLongOption("drearning") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ drearning
    val drpincome = row.getLongOption("drpincome") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ drpincome
    val dtravtime = row.getLongOption("dtravtime") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dtravtime
    val dweek89 = row.getLongOption("dweek89") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dweek89
    val dyrsserv = row.getLongOption("dyrsserv") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ dyrsserv
    val iavail = row.getLongOption("iavail") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iavail
    val icitizen = row.getLongOption("icitizen") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ icitizen
    val iclass = row.getLongOption("iclass") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iclass
    val idisabl1 = row.getLongOption("idisabl1") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ idisabl1
    val idisabl2 = row.getLongOption("idisabl2") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ idisabl2
    val ienglish = row.getLongOption("ienglish") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ ienglish
    val ifeb55 = row.getLongOption("ifeb55") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ ifeb55
    val ifertil = row.getLongOption("ifertil") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ ifertil
    val iimmigr = row.getLongOption("iimmigr") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iimmigr
    val ikorean = row.getLongOption("ikorean") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ ikorean
    val ilang1 = row.getLongOption("ilang1") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ ilang1
    val ilooking = row.getLongOption("ilooking") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ ilooking
    val imarital = row.getLongOption("imarital") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ imarital
    val imay75880 = row.getLongOption("imay75880") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ imay75880
    val imeans = row.getLongOption("imeans") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ imeans
    val imilitary = row.getLongOption("imilitary") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ imilitary
    val imobility = row.getLongOption("imobility") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ imobility
    val imobillim = row.getLongOption("imobillim") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ imobillim
    val iothrserv = row.getLongOption("iothrserv") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iothrserv
    val iperscare = row.getLongOption("iperscare") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iperscare
    val iragechld = row.getLongOption("iragechld") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iragechld
    val irelat1 = row.getLongOption("irelat1") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ irelat1
    val irelat2 = row.getLongOption("irelat2") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ irelat2
    val iremplpar = row.getLongOption("iremplpar") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iremplpar
    val iriders = row.getLongOption("iriders") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iriders
    val irlabor = row.getLongOption("irlabor") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ irlabor
    val irownchld = row.getLongOption("irownchld") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ irownchld
    val irpob = row.getLongOption("irpob") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ irpob
    val irrelchld = row.getLongOption("irrelchld") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ irrelchld
    val irspouse = row.getLongOption("irspouse") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ irspouse
    val irvetserv = row.getLongOption("irvetserv") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ irvetserv
    val ischool = row.getLongOption("ischool") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ ischool
    val isept80 = row.getLongOption("isept80") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ isept80
    val isex = row.getLongOption("isex") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ isex
    val isubfam1 = row.getLongOption("isubfam1") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ isubfam1
    val isubfam2 = row.getLongOption("isubfam2") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ isubfam2
    val itmpabsnt = row.getLongOption("itmpabsnt") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ itmpabsnt
    val ivietnam = row.getLongOption("ivietnam") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ ivietnam
    val iwork89 = row.getLongOption("iwork89") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iwork89
    val iworklwk = row.getLongOption("iworklwk") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iworklwk
    val iwwii = row.getLongOption("iwwii") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iwwii
    val iyearsch = row.getLongOption("iyearsch") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iyearsch
    val iyearwrk = row.getLongOption("iyearwrk") match { case Some(value) => DoubleDimension(value) ; case None => null}
    attribVector :+ iyearwrk
    
    Instance(label, attribVector)
  }
}

object ElectionDatasetReader {
  def main(args : Array[String]) {
    val conf = new SparkConf(true)
              .setAppName("GHSOM Election Dataset")
              .setMaster("local[4]")
              .set("spark.cassandra.connection.host", "localhost")
              .set("spark.cassandra.auth.username","ameya")
              .set("spark.cassandra.auth.password","amu5886")
              
              
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
    val dataset = sc.cassandraTable("uscensus1990", "dataset") 
    println("CREATING DATASET >>>>>>")
    val datasetReader : ElectionDatasetReader = new ElectionDatasetReader(dataset) 
    println("VECTOR SIZE:>>>>>>>> " )
    println(datasetReader.vectorSize)
    println("DATASET PRINT:>>>>>>>> " )
    println(datasetReader.printDataset())
              
  }
}
