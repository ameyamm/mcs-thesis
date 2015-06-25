package com.ameyamm.mcsthesis.cassandra_load

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

import scala.collection
import scala.collection.mutable

import com.ameyamm.mcsthesis.utils.Utils
import com.ameyamm.mcsthesis.model.Contact
/**
 * @author ameya
 */

//class Loader(val conf : SparkConf) extends Serializable{
class Loader(/*val sc : SparkContext*/) extends Serializable{

//	private val sc = new SparkContext(conf)
	def loadCassandra(sc : SparkContext)
	{
		val contactRDD = getContactRDD(sc)
    println(contactRDD.first())
    contactRDD.saveToCassandra("vote4db", "contact", 
        SomeColumns("id",
                    "first_name",
                    "last_name",
                    "middle_name",
                    "honorific",
                    "suffix",
                    "organization_name",
                    "contact_type",
                    "date_of_birth",
                    "dob",
                    "deceased",
                    "employer",
                    "occupation",
                    "gender",
                    "language",
                    "contact_methods",
                    "civic_address_id",
                    "civic_address_type",
                    "civic_address_building_number",
                    "civic_address_apartment_number",
                    "civic_address_city",
                    "civic_address_country",
                    "civic_address_line1", 
                    "civic_address_line2", 
                    "civic_address_meridian", 
                    "civic_address_number_suffix", 
                    "civic_address_postal_code", 
                    "civic_address_province", 
                    "civic_address_range", 
                    "civic_address_quarter", 
                    "civic_address_reserve",
                    "civic_address_section")
            )
	}

	private def getContactObjFromMap(mapRec : Map[String,String]) = {
		new Contact(
				id = mapRec.get("id") match {
				case Some(id) => id.toLong
				case None => -1 
				},
				first_name = mapRec.get("first_name"),
				last_name = mapRec.get("last_name"),
				middle_name = mapRec.get("middle_name"),
				honorific = mapRec.get("honorific"),
				suffix = mapRec.get("suffix"),
				organization_name = mapRec.get("organization_name"),
				organization_name2 = mapRec.get("organization_name2"),
				contact_type = mapRec.get("contact_type"),
				date_of_birth = mapRec.get("date_of_birth") match {
				case Some(dob) => Some(Utils.convertToDate(dob))
				case None      => None
				},
				deceased = mapRec.get("deceased") match {
				case Some(isDeceased) => isDeceased.equals("t")
				case None => false
				},
				employer = mapRec.get("employer"),
				gender = mapRec.get("gender"),
				industry = mapRec.get("industry"),
				language = mapRec.get("language"),
				occupation = mapRec.get("occupation"),
				contact_methods = computeContactMethodSet
				(
						bulkEmail = mapRec.get("allow_bulk_email"),
						bulkMail = mapRec.get("allow_bulk_mail"),
						call = mapRec.get("allow_call"),
						canvas = mapRec.get("allow_canvas"),
						email = mapRec.get("allow_email"),
						mail = mapRec.get("allow_mail"),
						voiceBroadcast = mapRec.get("allow_voice_broadcast"),
						sms = mapRec.get("allowsms")
				),
        civic_address_type = mapRec.get("civic_address_type"),
        civic_address_id = mapRec.get("civic_address_id") match {
                case Some(id) => id.toLong
                case None => -1 
                },
        civic_address_building_number = mapRec.get("civic_address_building_number"), 
        civic_address_apartment_number = mapRec.get("civic_address_apartment_number"),
        civic_address_city = mapRec.get("civic_address_city"), 
        civic_address_country = mapRec.get("civic_address_country"), 
        civic_address_line1 = mapRec.get("civic_address_line1"), 
        civic_address_line2 = mapRec.get("civic_address_line2"), 
        civic_address_meridian = mapRec.get("civic_address_meridian"), 
        civic_address_number_suffix = mapRec.get("civic_address_number_suffix"), 
        civic_address_postal_code = mapRec.get("civic_address_postal_code"), 
        civic_address_province = mapRec.get("civic_address_province"), 
        civic_address_range = mapRec.get("civic_address_range"), 
        civic_address_quarter = mapRec.get("civic_address_quarter"), 
        civic_address_reserve = mapRec.get("civic_address_reserve"),
        civic_address_section = mapRec.get("civic_address_section")
     )
	}

	private def getContactRDD(sc : SparkContext) = {
		// source of t_contact
        val tContactDataSrc = sc.textFile("hdfs://192.168.101.13:9000/user/hduser/Cassandra/MigrationFiles/t_contact/t_contact_search_fast_AB.txt")
				val tContactHeader = sc.textFile(path = "hdfs://192.168.101.13:9000/user/hduser/Cassandra/MigrationFiles/t_contact/srcHeader.txt")
				val tContactHeaderArray = sc.broadcast(tContactHeader.flatMap(rec => rec.split('|')).collect())//.toArray()

				// src file contains pipe delimited records. split them to create a String array RDD
				val tContactDataRDD = tContactDataSrc.map(rec => removeNulls(rec.split('|'))) 
				.map(rec => 
				Utils.removeAtIdx(Utils.removeAtIdx(Utils.removeAtIdx(Utils.removeAtIdx(rec.toList,72),71),27),25).toArray) // removes the problematic columns containing 
				// pipes in their values.

				// create a map of t_contact column name and value for each record in tContactDataRDD
				val tContactRDDMap = tContactDataRDD.map(rec => tContactHeaderArray.value.zip(rec).toMap)
        //tContactRDDMap.take(3).foreach(rec => println(rec.mkString(";")))

				val tContactRDD = tContactRDDMap.map( mapRec => getContactObjFromMap(mapRec) ) 
        tContactRDD
		}

  private def removeNulls(recArray : Array[String]) : Array[String] = {
			recArray.map( value => if (value.equals("\\N")) null else value )
	}

	private def computeContactMethodSet(
			bulkEmail : Option[String],
			bulkMail : Option[String],
			call : Option[String],
			canvas : Option[String],
			email : Option[String],
			mail : Option[String],
			voiceBroadcast : Option[String],
			sms : Option[String]
			) : Option[Set[String]] = {
			var contactMethods = new mutable.HashSet[String]() 
      
      /*val temp = collection.immutable.HashSet[Option[String]](bulkEmail,bulkMail,call,canvas,email,sms)
      temp.take(2).foreach { x => println(">>>>>>>>>>>>" + x) }*/
      

			bulkEmail match { 
					case Some(bulkEmailVal) => if (bulkEmailVal.contains("t")) contactMethods += Contact.CONTACT_METHOD_BULK_EMAIL else contactMethods 
					case None => contactMethods 
			} 

			bulkMail match { 
			case Some(bulkMailVal) => if (bulkMailVal.equals("t")) contactMethods += Contact.CONTACT_METHOD_BULK_MAIL else contactMethods 
			case None => contactMethods 
			} 

			call match { 
			case Some(callVal) => if (callVal.equals("t")) contactMethods += Contact.CONTACT_METHOD_CALL else contactMethods 
			case None => contactMethods 
			} 

			canvas match {
			case Some(canvasVal) => if (canvasVal.equals("t")) contactMethods += Contact.CONTACT_METHOD_CANVAS else contactMethods 
			case None => contactMethods 
			} 

			email match {
			case Some(emailVal) => if (emailVal.equals("t")) contactMethods += Contact.CONTACT_METHOD_EMAIL else contactMethods 
			case None => contactMethods 
			} 

			mail match {
			case Some(mailVal) => if (mailVal.equals("t")) contactMethods += Contact.CONTACT_METHOD_MAIL else contactMethods 
			case None => contactMethods 
			}

			voiceBroadcast match {
			case Some(voiceBroadcastVal) => if (voiceBroadcastVal.equals("t")) contactMethods += Contact.CONTACT_METHOD_VOICE_BROADCAST else contactMethods 
			case None => contactMethods 
			}

			sms match {
			case Some(smsVal) => if (smsVal.equals("t")) contactMethods += Contact.CONTACT_METHOD_SMS else contactMethods 
			case None => contactMethods 
			}

			if (contactMethods.size != 0)
				Some(Set() ++ contactMethods)
			else 
				None
	}


}

object Loader extends Serializable{

	def main(args : Array[String]) {
		val conf = new SparkConf().setAppName("Cassandra Loading")
        .set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
        .set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
				.set("spark.cassandra.connection.host","192.168.101.12")
	//			.set("spark.cassandra.connection.native.port","9042")
	//			.set("spark.cassandra.connection.rpc.port","9160")
				.set("spark.cassandra.auth.username","netfore")
				.set("spark.cassandra.auth.password","netforePWD")
				.set("spark.storage.memoryFraction","0")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.default.parallelism","8")
				.set("spark.kryoserializer.buffer.max","400")
        .setMaster("spark://192.168.101.13:7077")
        //.set("spark.driver.host","172.16.0.6")
        

    val sc = new SparkContext(conf)
    //val cassandraLoader = new Loader(new SparkContext(conf));
    val cassandraLoader = new Loader();
		cassandraLoader.loadCassandra(sc);
	}
}