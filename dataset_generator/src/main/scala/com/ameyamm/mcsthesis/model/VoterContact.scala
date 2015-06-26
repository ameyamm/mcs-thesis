package com.ameyamm.mcsthesis.model

import org.joda.time.DateTime
/**
 * @author ameya
 */
class VoterContact(
        val contact_id : Long ,
        val campaign_id : Option[Long] ,
        val region_id : Option[Long] ,
        val first_name : Option[String] ,
        val middle_name : Option[String] ,
        val last_name : Option[String] ,
        val contact_type : Option[String] ,
        val organization_name : Option[String] ,
        val date_of_birth : Option[DateTime] ,
        val deceased : Option[Boolean] ,
        val employer : Option[String] ,
        val gender : Option[String] ,
        val industry : Option[String] ,
        val language : Option[String] ,
        val occupation : Option[String] ,
        val contact_methods : Option[Set[String]] ,
        val mark : Option[String] ,
        val leaning : Option[String] ,
        val cases_filed : Option[Int] ,
        val cases_closed : Option[Int] ,
        val etags : Option[Set[String]] ,
        val survey_responses : Option[Set[String]] ,
        val tags : Option[Set[String]] ,
        val federal_elector_id : Option[Long] ,
        val federal_poll : Option[Long] ,
        val federal_riding : Option[Long] ,
        val federal_seq_number : Option[Long] ,
        val has_email : Option[Boolean] ,
        val townhouse_or_apartment : Option[String] ,
        val civic_address_type : Option[String] ,
        val civic_address_city : Option[String] ,
        val civic_address_province : Option[String] ,
        val civic_address_country : Option[String] ,
        val civic_address_postal_code : Option[String] ,
        val civic_address_street_type : Option[String] ,
        val civic_address_township : Option[String] , 
        val province_city : Option[String]
        ) 