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
import com.ameyamm.mcs_thesis.model.Contact

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
    
    var contactDataset = dataset.map( row => convertToContact(row) )
    
   // instanceDataset = 
    val attribMap = contactDataset.flatMap( 
                              contact => { 
                                List(
                                  ("dage",contact.dage match { case Some(value) => value ; case None => null }),
                                  ("dancstry1",contact.dancstry1 match { case Some(value) => value ; case None => null }),
                                  ("dancstry2",contact.dancstry2 match { case Some(value) => value ; case None => null }),
                                  ("ddepart",contact.ddepart match { case Some(value) => value ; case None => null }),
                                  ("dhispanic",contact.dhispanic match { case Some(value) => value ; case None => null }),
                                  ("dhour89",contact.dhour89 match { case Some(value) => value ; case None => null }),
                                  ("dhours",contact.dhours match { case Some(value) => value ; case None => null }),
                                  ("dincome1",contact.dincome1 match { case Some(value) => value ; case None => null }),
                                  ("dincome2",contact.dincome2 match { case Some(value) => value ; case None => null }),
                                  ("dincome3",contact.dincome3 match { case Some(value) => value ; case None => null }),
                                  ("dincome4",contact.dincome4 match { case Some(value) => value ; case None => null }),
                                  ("dincome5",contact.dincome5 match { case Some(value) => value ; case None => null }),
                                  ("dincome6",contact.dincome6 match { case Some(value) => value ; case None => null }),
                                  ("dincome7",contact.dincome7 match { case Some(value) => value ; case None => null }),
                                  ("dincome8",contact.dincome8 match { case Some(value) => value ; case None => null }),
                                  ("dindustry",contact.dindustry match { case Some(value) => value ; case None => null }),
                                  ("doccup",contact.doccup match { case Some(value) => value ; case None => null }),
                                  ("dpob",contact.dpob match { case Some(value) => value ; case None => null }),
                                  ("dpoverty",contact.dpoverty match { case Some(value) => value ; case None => null }),
                                  ("dpwgt1",contact.dpwgt1 match { case Some(value) => value ; case None => null }),
                                  ("drearning",contact.drearning match { case Some(value) => value ; case None => null }),
                                  ("drpincome",contact.drpincome match { case Some(value) => value ; case None => null }),
                                  ("dtravtime",contact.dtravtime match { case Some(value) => value ; case None => null }),
                                  ("dweek89",contact.dweek89 match { case Some(value) => value ; case None => null }),
                                  ("dyrsserv",contact.dyrsserv match { case Some(value) => value ; case None => null }),
                                  ("iavail",contact.iavail match { case Some(value) => value ; case None => null }),
                                  ("icitizen",contact.icitizen match { case Some(value) => value ; case None => null }),
                                  ("iclass",contact.iclass match { case Some(value) => value ; case None => null }),
                                  ("idisabl1",contact.idisabl1 match { case Some(value) => value ; case None => null }),
                                  ("idisabl2",contact.idisabl2 match { case Some(value) => value ; case None => null }),
                                  ("ienglish",contact.ienglish match { case Some(value) => value ; case None => null }),
                                  ("ifeb55",contact.ifeb55 match { case Some(value) => value ; case None => null }),
                                  ("ifertil",contact.ifertil match { case Some(value) => value ; case None => null }),
                                  ("iimmigr",contact.iimmigr match { case Some(value) => value ; case None => null }),
                                  ("ikorean",contact.ikorean match { case Some(value) => value ; case None => null }),
                                  ("ilang1",contact.ilang1 match { case Some(value) => value ; case None => null }),
                                  ("ilooking",contact.ilooking match { case Some(value) => value ; case None => null }),
                                  ("imarital",contact.imarital match { case Some(value) => value ; case None => null }),
                                  ("imay75880",contact.imay75880 match { case Some(value) => value ; case None => null }),
                                  ("imeans",contact.imeans match { case Some(value) => value ; case None => null }),
                                  ("imilitary",contact.imilitary match { case Some(value) => value ; case None => null }),
                                  ("imobility",contact.imobility match { case Some(value) => value ; case None => null }),
                                  ("imobillim",contact.imobillim match { case Some(value) => value ; case None => null }),
                                  ("iothrserv",contact.iothrserv match { case Some(value) => value ; case None => null }),
                                  ("iperscare",contact.iperscare match { case Some(value) => value ; case None => null }),
                                  ("iragechld",contact.iragechld match { case Some(value) => value ; case None => null }),
                                  ("irelat1",contact.irelat1 match { case Some(value) => value ; case None => null }),
                                  ("irelat2",contact.irelat2 match { case Some(value) => value ; case None => null }),
                                  ("iremplpar",contact.iremplpar match { case Some(value) => value ; case None => null }),
                                  ("iriders",contact.iriders match { case Some(value) => value ; case None => null }),
                                  ("irlabor",contact.irlabor match { case Some(value) => value ; case None => null }),
                                  ("irownchld",contact.irownchld match { case Some(value) => value ; case None => null }),
                                  ("irpob",contact.irpob match { case Some(value) => value ; case None => null }),
                                  ("irrelchld",contact.irrelchld match { case Some(value) => value ; case None => null }),
                                  ("irspouse",contact.irspouse match { case Some(value) => value ; case None => null }),
                                  ("irvetserv",contact.irvetserv match { case Some(value) => value ; case None => null }),
                                  ("ischool",contact.ischool match { case Some(value) => value ; case None => null }),
                                  ("isept80",contact.isept80 match { case Some(value) => value ; case None => null }),
                                  ("isex",contact.isex match { case Some(value) => value ; case None => null }),
                                  ("isubfam1",contact.isubfam1 match { case Some(value) => value ; case None => null }),
                                  ("isubfam2",contact.isubfam2 match { case Some(value) => value ; case None => null }),
                                  ("itmpabsnt",contact.itmpabsnt match { case Some(value) => value ; case None => null }),
                                  ("ivietnam",contact.ivietnam match { case Some(value) => value ; case None => null }),
                                  ("iwork89",contact.iwork89 match { case Some(value) => value ; case None => null }),
                                  ("iworklwk",contact.iworklwk match { case Some(value) => value ; case None => null }),
                                  ("iwwii",contact.iwwii match { case Some(value) => value ; case None => null }),
                                  ("iyearsch",contact.iyearsch match { case Some(value) => value ; case None => null }),
                                  ("iyearwrk",contact.iyearwrk match { case Some(value) => value ; case None => null })
                                )
                              } 
                      )
                   
     println(attribMap.take(10).mkString(","))
     /*
     val maxstr = maxVector.mkString(",")               
     val minstr = minVector.mkString(",")      
     println(">>>>>First instance")
     instanceDataset.first().attributeVector.foreach { x => print(x + ",")}
     println(">>>>>>>MAXSTR")
     println(maxstr)
     println(">>>>>>>MINSTR")
     println(minstr)*/
    contactDataset.map(contact => contact.getInstanceObj)
  }
  
  def convertToContact(row : CassandraRow) : Contact= {
    new Contact(
      caseid = row.getString("caseid"),
      dage = row.getLongOption("dage") ,
      dancstry1 = row.getLongOption("dancstry1") ,
      dancstry2 = row.getLongOption("dancstry2") ,
      ddepart = row.getLongOption("ddepart") ,
      dhispanic = row.getLongOption("dhispanic") ,
      dhour89 = row.getLongOption("dhour89") ,
      dhours = row.getLongOption("dhours") ,
      dincome1 = row.getLongOption("dincome1") ,
      dincome2 = row.getLongOption("dincome2") ,
      dincome3 = row.getLongOption("dincome3") ,
      dincome4 = row.getLongOption("dincome4") ,
      dincome5 = row.getLongOption("dincome5") ,
      dincome6 = row.getLongOption("dincome6") ,
      dincome7 = row.getLongOption("dincome7") ,
      dincome8 = row.getLongOption("dincome8") ,
      dindustry = row.getLongOption("dindustry") ,
      doccup = row.getLongOption("doccup") ,
      dpob = row.getLongOption("dpob") ,
      dpoverty = row.getLongOption("dpoverty") ,
      dpwgt1 = row.getLongOption("dpwgt1") ,
      drearning = row.getLongOption("drearning") ,
      drpincome = row.getLongOption("drpincome") ,
      dtravtime = row.getLongOption("dtravtime") ,
      dweek89 = row.getLongOption("dweek89") ,
      dyrsserv = row.getLongOption("dyrsserv") ,
      iavail = row.getLongOption("iavail") ,
      icitizen = row.getLongOption("icitizen") ,
      iclass = row.getLongOption("iclass") ,
      idisabl1 = row.getLongOption("idisabl1") ,
      idisabl2 = row.getLongOption("idisabl2") ,
      ienglish = row.getLongOption("ienglish") ,
      ifeb55 = row.getLongOption("ifeb55") ,
      ifertil = row.getLongOption("ifertil") ,
      iimmigr = row.getLongOption("iimmigr") ,
      ikorean = row.getLongOption("ikorean") ,
      ilang1 = row.getLongOption("ilang1") ,
      ilooking = row.getLongOption("ilooking") ,
      imarital = row.getLongOption("imarital") ,
      imay75880 = row.getLongOption("imay75880") ,
      imeans = row.getLongOption("imeans") ,
      imilitary = row.getLongOption("imilitary") ,
      imobility = row.getLongOption("imobility") ,
      imobillim = row.getLongOption("imobillim") ,
      iothrserv = row.getLongOption("iothrserv") ,
      iperscare = row.getLongOption("iperscare") ,
      iragechld = row.getLongOption("iragechld") ,
      irelat1 = row.getLongOption("irelat1") ,
      irelat2 = row.getLongOption("irelat2") ,
      iremplpar = row.getLongOption("iremplpar") ,
      iriders = row.getLongOption("iriders") ,
      irlabor = row.getLongOption("irlabor") ,
      irownchld = row.getLongOption("irownchld") ,
      irpob = row.getLongOption("irpob") ,
      irrelchld = row.getLongOption("irrelchld") ,
      irspouse = row.getLongOption("irspouse") ,
      irvetserv = row.getLongOption("irvetserv") ,
      ischool = row.getLongOption("ischool") ,
      isept80 = row.getLongOption("isept80") ,
      isex = row.getLongOption("isex") ,
      isubfam1 = row.getLongOption("isubfam1") ,
      isubfam2 = row.getLongOption("isubfam2") ,
      itmpabsnt = row.getLongOption("itmpabsnt") ,
      ivietnam = row.getLongOption("ivietnam") ,
      iwork89 = row.getLongOption("iwork89") ,
      iworklwk = row.getLongOption("iworklwk") ,
      iwwii = row.getLongOption("iwwii") ,
      iyearsch = row.getLongOption("iyearsch") ,
      iyearwrk = row.getLongOption("iyearwrk") 
    )
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
