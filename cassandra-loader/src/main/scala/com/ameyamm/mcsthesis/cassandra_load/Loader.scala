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
		contactRDD.take(3).foreach(contact => println(contact))
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

	private def getContactObjFromMap(mapRec : Map[String,String]) = {
    //println(mapRec.get("date_of_birth"))
		new Contact(
				id = mapRec.get("id") match {
				case Some(id) => id.toLong
				case None => -1 
				},
				_firstName = mapRec.get("first_name"),
				_lastName = mapRec.get("last_name"),
				_middleName = mapRec.get("middle_name"),
				_honorific = mapRec.get("honorific"),
				_suffix = mapRec.get("suffix"),
				_organizationName = mapRec.get("organization_name"),
				_organizationName2 = mapRec.get("organization_name2"),
				_contactType = mapRec.get("contact_type"),
				_dateOfBirth = mapRec.get("date_of_birth") match {
				case Some(dob) => Some(Utils.convertToDate(dob))
				case None      => None
				},
				_deceased = mapRec.get("deceased") match {
				case Some(isDeceased) => isDeceased.equals("t")
				case None => false
				},
				_employer = mapRec.get("employer"),
				_gender = mapRec.get("gender"),
				_industry = mapRec.get("industry"),
				_language = mapRec.get("language"),
				_occupation = mapRec.get("occupation"),
				_contactMethod = computeContactMethodSet
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
        _civicAddrType = mapRec.get("civic_address_type"),
        _civicAddrId = mapRec.get("civic_address_id") match {
                case Some(id) => id.toLong
                case None => -1 
                },
        _civicAddrBuildingNum = mapRec.get("civic_address_building_number"), 
        _civicAddrApartmentNum = mapRec.get("civic_address_apartment_number"),
        _civicAddrCity = mapRec.get("civic_address_city"), 
        _civicAddrCountry = mapRec.get("civic_address_country"), 
        _civicAddrLine1 = mapRec.get("civic_address_line1"), 
        _civicAddrLine2 = mapRec.get("civic_address_line2"), 
        _civicAddrMeridian = mapRec.get("civic_address_meridian"), 
        _civicAddrNumSuffix = mapRec.get("civic_address_number_suffix"), 
        _civicAddrPostalCode = mapRec.get("civic_address_postal_code"), 
        _civicAddrProvince = mapRec.get("civic_address_province"), 
        _civicAddrRange = mapRec.get("civic_address_range"), 
        _civicAddrQuarter = mapRec.get("civic_address_quarter"), 
        _civicAddrReserve = mapRec.get("civic_address_reserve"),
        _civicAddrSection = mapRec.get("civic_address_section")
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
}

object Loader extends Serializable{

	def main(args : Array[String]) {
		val conf = new SparkConf().setAppName("Cassandra Loading")
				.set("spark.cassandra.connection.host","192.168.101.11")
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