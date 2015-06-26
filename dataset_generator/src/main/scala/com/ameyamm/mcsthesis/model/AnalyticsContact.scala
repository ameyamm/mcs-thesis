package com.ameyamm.mcsthesis.model

/**
 * @author ameya
 */
class AnalyticsContact extends Serializable {
  
		val contact_id : Long = 0
    val campaign_id : Option[Long] = None
    val region_id : Option[Long] = None
    val activity_id : Option[Long] = None
    val first_name : Option[String] = None
    val middle_name : Option[String] = None
    val last_name : Option[String] = None
    val contact_type : Option[String] = None
    val organization_name : Option[String] = None
    val date_of_birth : Option[String] = None
    val deceased : Option[String] = None
    val employer : Option[String] = None
    val gender : Option[String] = None
    val industry : Option[String] = None
    val language : Option[String] = None
    val occupation : Option[String] = None
    val contact_methods : Option[String] = None
    val mark : Option[String] = None
    val leaning : Option[String] = None
    val cases_filed : Option[Int] = None
    val cases_closed : Option[Int] = None
    val etags : Option[String] = None
    val survey_responses : Option[String] = None
    val tags : Option[String] = None
    val federal_elector_id : Option[Long] = None
    val federal_poll : Option[Long] = None
    val federal_riding : Option[Long] = None
    val federal_seq_number : Option[Long] = None
    val has_email : Option[String] = None
    val townhouse_or_apartment : Option[String] = None
    val civic_address_type : Option[String] = None
    val civic_address_city : Option[String] = None
    val civic_address_province : Option[String] = None
    val civic_address_country : Option[String] = None
    val civic_address_postal_code : Option[String] = None
    val civic_address_street_type : Option[String] = None
    val civic_address_township : Option[String] = None
}
