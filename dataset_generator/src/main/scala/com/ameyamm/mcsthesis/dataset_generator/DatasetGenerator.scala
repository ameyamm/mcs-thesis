package com.ameyamm.mcsthesis.dataset_generator

/**
 * @author ameya
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

class DatasetGenerator {

	def migrateData()
	{
		//DatasetGenerator.sc
	}
}

object DatasetGenerator {

	val conf = new SparkConf().setAppName("Cassandra Loading")
			.set("spark.cassandra.connection.host","192.168.101.11")
			//      .set("spark.cassandra.connection.native.port","9042")
			//      .set("spark.cassandra.connection.rpc.port","9160")
			.set("spark.cassandra.auth.username","netfore")
			.set("spark.cassandra.auth.password","netforePWD")
			.set("spark.storage.memoryFraction","0")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.set("spark.default.parallelism","8")
			.set("spark.kryoserializer.buffer.max","400")
			.setMaster("spark://192.168.101.13:7077")

	private val sc = new SparkContext(conf)

	def main(args : Array[String]) {
  
    new DatasetGenerator()




	}
}