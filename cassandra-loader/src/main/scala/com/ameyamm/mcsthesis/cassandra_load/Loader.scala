package com.ameyamm.mcsthesis.cassandra_load

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

import com.ameyamm.mcsthesis.utils.Utils

/**
 * @author ameya
 */

class Loader(val conf : SparkConf) {
  private var sc = new SparkContext(conf)

  def loadCassandra()
  {
    val contactRDD = getContactRDD()
    contactRDD.take(3).foreach(map => println(map.mkString(":")))
  }
  
  def getContactRDD() =
  {
    // source of t_contact
    val tContactDataSrc = sc.textFile("hdfs://192.168.101.13:9000/user/hduser/Cassandra/MigrationFiles/t_contact/t_contact_search_fast_AB.txt")
    val tContactHeader = sc.textFile(path = "hdfs://192.168.101.13:9000/user/hduser/Cassandra/MigrationFiles/t_contact/srcHeader.txt")
    val tContactHeaderArray = tContactHeader.flatMap(rec => rec.split('|')).toArray()
    
    // src file contains pipe delimited records. split them to create a String array RDD
    val tContactDataRDD = tContactDataSrc.map(rec => rec.split('|'))
                              .map(rec => Utils.removeAtIdx(Utils.removeAtIdx(rec.toList,27),25).toArray) // removes the problematic columns containing 
                                                                                                  // pipes in their values.
                      
    val tContactMap = tContactDataRDD.map(rec => tContactHeaderArray.zip(rec).toMap)
    tContactMap

  }
  
}

object Loader {
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Cassandra Loading")
      .set("spark.cassandra.connection.host","192.168.101.11")
      .set("spark.cassandra.auth.username","netfore")
      .set("spark.cassandra.auth.password","netforePWD")
      .set("spark.storage.memoryFraction","0")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.default.parallelism","8")
      .set("spark.kryoserializer.buffer.max.mb","400")

    val cassandraLoader = new Loader(conf);
    cassandraLoader.loadCassandra();
  }
}