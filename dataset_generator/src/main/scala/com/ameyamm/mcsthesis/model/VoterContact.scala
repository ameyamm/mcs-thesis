package com.ameyamm.mcsthesis.model

import org.joda.time.DateTime
/**
 * @author ameya
 */
class VoterContact(
        val contact_id : Long,
        val campaign_id : Long,
        val region_id : Long,
        val first_name : String,
        val middle_name : String,
        val last_name : String,
        val contact_type : String,
        val organization_name : String,
        val date_of_birth : DateTime,
        val deceased : Boolean,
        val employer : String,
        val gender : String,
        val industry : String,
        val language : String,
        val occupation : String,
        val contact_methods : Set[String],
        val mark : String,
        val leaning : String,
        val cases_filed : Int,
        val cases_closed : Int,
        val etags : Set[String],
        val survey_responses : Set[String],
        val tags : Set[String],
        val federal_elector_id : Long,
        val federal_poll : Long,
        val federal_riding : Long,
        val federal_seq_number : Long,
        val has_email : Boolean,
        val townhouse_or_apartment : String,
        val civic_address_type : String,
        val civic_address_city : String,
        val civic_address_province : String,
        val civic_address_country : String,
        val civic_address_postal_code : String,
        val civic_address_street_type : String,
        val civic_address_township : String, 
        val province_city : String
        ) 