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

  def convertToVoter( analyticsContact : CassandraRow) : VoterContact = {
    new VoterContact(
                        contact_id = analyticsContact.getLong("contact_id"),
                        campaign_id = analyticsContact.getLongOption("campaign_id"),
                        region_id = analyticsContact.getLongOption("region_id"),
                        first_name = analyticsContact.getStringOption("first_name"),
                        middle_name = analyticsContact.getStringOption("middle_name"),
                        last_name = analyticsContact.getStringOption("last_name"),
                        contact_type = analyticsContact.getStringOption("contact_type"),
                        organization_name = analyticsContact.getStringOption("organization_name"),
                        date_of_birth = Util.getDateFromString(analyticsContact.getStringOption("date_of_birth")),
                        deceased = Util.getBooleanFromString(analyticsContact.getStringOption("deceased")),
                        employer = analyticsContact.getStringOption("employer"),
                        gender = analyticsContact.getStringOption("gender"),
                        industry = analyticsContact.getStringOption("industry"),
                        language = analyticsContact.getStringOption("language"),
                        occupation = analyticsContact.getStringOption("occupation"),
                        contact_methods = Util.getSetFromString(analyticsContact.getStringOption("contact_methods")),
                        mark = analyticsContact.getStringOption("mark"),
                        leaning = analyticsContact.getStringOption("leaning"),
                        cases_filed = analyticsContact.getIntOption("cases_filed"),
                        cases_closed = analyticsContact.getIntOption("cases_closed"),
                        etags = Util.getSetFromString(analyticsContact.getStringOption("etags")),
                        survey_responses = Util.getSetFromString(analyticsContact.getStringOption("survey_responses")),
                        tags = Util.getSetFromString(analyticsContact.getStringOption("tags")),
                        federal_elector_id = analyticsContact.getLongOption("federal_elector_id"),
                        federal_poll = analyticsContact.getLongOption("federal_poll"),
                        federal_riding = analyticsContact.getLongOption("federal_riding"),
                        federal_seq_number = analyticsContact.getLongOption("federal_seq_number"),
                        has_email = Util.getBooleanFromString(analyticsContact.getStringOption("has_email")),
                        townhouse_or_apartment = analyticsContact.getStringOption("townhouse_or_apartment"),
                        civic_address_type = analyticsContact.getStringOption("civic_address_type"),
                        civic_address_city = analyticsContact.getStringOption("civic_address_city"),
                        civic_address_province = analyticsContact.getStringOption("civic_address_province"),
                        civic_address_country = analyticsContact.getStringOption("civic_address_country"),
                        civic_address_postal_code = analyticsContact.getStringOption("civic_address_postal_code"),
                        civic_address_street_type = analyticsContact.getStringOption("civic_address_street_type"),
                        civic_address_township = analyticsContact.getStringOption("civic_address_township"),
                        province_city = Some( Util.getStringFromOptionString(analyticsContact.getStringOption("civic_address_province"))
                            + "|" + Util.getStringFromOptionString(analyticsContact.getStringOption("civic_address_city"))
                            )
                    )
  } 
  
  def loadData() {
      
    val analyticsContactRDD = sc.cassandraTable("vote4db", "t_analytics_contact")
    val analyticsContact  = analyticsContactRDD.first()
    println(analyticsContact.getStringOption("civic_address_province") + ":" + analyticsContact.getStringOption("civic_address_city") + ":" + analyticsContact.getLongOption("contact_id"))
    val voterRDD = analyticsContactRDD.map( analyticsContact => convertToVoter(analyticsContact))
    println("Saving to Cassandra")
    voterRDD.saveToCassandra("vote4db", "voters", SomeColumns(
                                                      "contact_id",
                                                        "campaign_id",
                                                        "region_id",
                                                        "first_name",
                                                        "middle_name",
                                                        "last_name",
                                                        "contact_type",
                                                        "organization_name",
                                                        "date_of_birth",
                                                        "deceased",
                                                        "employer",
                                                        "gender",
                                                        "industry",
                                                        "language",
                                                        "occupation",
                                                        "contact_methods",
                                                        "mark",
                                                        "leaning",
                                                        "cases_filed",
                                                        "cases_closed",
                                                        "etags",
                                                        "survey_responses",
                                                        "tags",
                                                        "federal_elector_id",
                                                        "federal_poll",
                                                        "federal_riding",
                                                        "federal_seq_number",
                                                        "has_email",
                                                        "townhouse_or_apartment",
                                                        "civic_address_type",
                                                        "civic_address_city",
                                                        "civic_address_province",
                                                        "civic_address_country",
                                                        "civic_address_postal_code",
                                                        "civic_address_street_type",
                                                        "civic_address_township", 
                                                        "province_city"))
    println("Saving complete")
  }
  
	def main(args : Array[String]) {
      loadData()   
	}

}