package com.ameyamm.mcsthesis.dataset_generator

/**
 * @author ameya
 */
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

import org.joda.time.DateTime

//import com.typesafe.config.ConfigFactory

import com.ameyamm.mcsthesis.utils.Util
import com.ameyamm.mcsthesis.model._

object DatasetGenerator {
  /*private val config = ConfigFactory.load()
  
  private val cassandraHost = config.getString("cassandra.host")
  private val cassandraUsername = config.getString("cassandra.username")
  private val cassandraPassword = config.getString("cassandra.password")
  * 
  */
  private val cassandraHost = CassandraConfig.cassandraHost 
  private val cassandraUsername = CassandraConfig.cassandraUsername 
  private val cassandraPassword = CassandraConfig.cassandraPassword
	private val conf = new SparkConf().setAppName("Cassandra Loading")
			.set("spark.cassandra.connection.host",cassandraHost)
			.set("spark.cassandra.auth.username",cassandraUsername)
			.set("spark.cassandra.auth.password",cassandraPassword)
			.set("spark.storage.memoryFraction","0")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.set("spark.default.parallelism","8")
			.set("spark.kryoserializer.buffer.max","400")
			.setMaster("spark://192.168.101.13:7077")
	private val sc = new SparkContext(conf)

  def convertToVoter( analyticsContact : AnalyticsContact) : VoterContact = {
    new VoterContact(
                        contact_id = analyticsContact.contact_id,
                        campaign_id = analyticsContact.campaign_id,
                        region_id = analyticsContact.region_id,
                        first_name = analyticsContact.first_name,
                        middle_name = analyticsContact.middle_name,
                        last_name = analyticsContact.last_name,
                        contact_type = analyticsContact.contact_type,
                        organization_name = analyticsContact.organization_name,
                        date_of_birth = Util.getDateFromString(analyticsContact.date_of_birth),
                        deceased = Util.getBooleanFromString(analyticsContact.deceased),
                        employer = analyticsContact.employer,
                        gender = analyticsContact.gender,
                        industry = analyticsContact.industry,
                        language = analyticsContact.language,
                        occupation = analyticsContact.occupation,
                        contact_methods = Util.getSetFromString(analyticsContact.contact_methods),
                        mark = analyticsContact.mark,
                        leaning = analyticsContact.leaning,
                        cases_filed = analyticsContact.cases_filed,
                        cases_closed = analyticsContact.cases_closed,
                        etags = Util.getSetFromString(analyticsContact.etags),
                        survey_responses = Util.getSetFromString(analyticsContact.survey_responses),
                        tags = Util.getSetFromString(analyticsContact.tags),
                        federal_elector_id = analyticsContact.federal_elector_id,
                        federal_poll = analyticsContact.federal_poll,
                        federal_riding = analyticsContact.federal_riding,
                        federal_seq_number = analyticsContact.federal_seq_number,
                        has_email = Util.getBooleanFromString(analyticsContact.has_email),
                        townhouse_or_apartment = analyticsContact.townhouse_or_apartment,
                        civic_address_type = analyticsContact.civic_address_type,
                        civic_address_city = analyticsContact.civic_address_city,
                        civic_address_province = analyticsContact.civic_address_province,
                        civic_address_country = analyticsContact.civic_address_country,
                        civic_address_postal_code = analyticsContact.civic_address_postal_code,
                        civic_address_street_type = analyticsContact.civic_address_street_type,
                        civic_address_township = analyticsContact.civic_address_township,
                        province_city = analyticsContact.civic_address_province + "|" + analyticsContact.civic_address_city
                    )
  } 
  
  def loadData() {
      
    val analyticsContactRDD = sc.cassandraTable[AnalyticsContact]("vote4db", "t_analytics_contact")
      
    val voterRDD = analyticsContactRDD.map( analyticsContact => convertToVoter(analyticsContact))
    println(voterRDD.first().toString())
      
  }
  
	def main(args : Array[String]) {
      loadData()   
	}

}