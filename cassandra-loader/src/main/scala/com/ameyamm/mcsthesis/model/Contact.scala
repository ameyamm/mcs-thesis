package com.ameyamm.mcsthesis.model

import java.util.Date
/**
 * @author ameya
 */
class Contact (
    
     val id : Long,

    // Contact Details
    // Person
     val first_name : Option[String] = None,
     val last_name : Option[String] = None,
     val middle_name : Option[String] = None,
     val honorific : Option[String] = None,
     val suffix : Option[String] = None,
    
    // Organization
     val organization_name : Option[String] = None,
     val organization_name2 : Option[String] = None,
     val contact_type : Option[String] = None,

    // Personal details
     val date_of_birth : Option[Date] = None,
     val deceased : Boolean = false,
     val employer : Option[String] = None,
     val gender : Option[String] = None,
     val industry : Option[String] = None,
     val language : Option[String] = None,
     val occupation : Option[String] = None,
    
    // Communication 
     val contact_methods : Option[Set[String]] = None,
     val email : Option[Set[String]] = None,
     val phone_number : Option[String] = None,

    // Civic Address
     val civic_address_id : Long = -1,
     val civic_address_type : Option[String] = None,
     val civic_address_building_number : Option[String] = None, 
     val civic_address_apartment_number : Option[String] = None, 
     val civic_address_city : Option[String] = None,
     val civic_address_country : Option[String] = None,
     val civic_address_line1 : Option[String] = None,
     val civic_address_line2 : Option[String] = None,
     val civic_address_meridian : Option[String] = None, 
     val civic_address_number_suffix : Option[String] = None,
     val civic_address_postal_code : Option[String] = None,
     val civic_address_province : Option[String] = None,
     val civic_address_range : Option[String] = None,
     val civic_address_quarter : Option[String] = None,
     val civic_address_reserve : Option[String] = None, 
     val civic_address_section : Option[String] = None,

    // Electoral details 
     val federal_electoral_id : Option[String] = None,
     val federal_poll : Option[String] = None,
     val federal_riding : Option[String] = None, 
     val federal_sequence_number : Option[String] = None,

    // Misc Properties
     val marks : Int = 0,
     val leaning : Char = '\0',
     val etag_name : Option[String] = None,
     val etag_desc : Option[String] = None,
     val etag_enum_name : Option[String] = None,
     val etag_enum_desc : Option[String] = None,
     val etag_string : Option[String] = None,
     val tag_description : Option[String] = None,
     val tag_name : Option[String] = None,
     val tag_type : Option[String] = None,
     val survey_response : Option[Map[String, String]] = None,
     val activity_list_name : Option[String] = None,
     val activity_list_status : Option[String] = None,
     val activity_type : Option[String] = None,
     val activity_method : Option[String] = None,
     val activity_name : Option[String] = None,
     val activity_notes : Option[String] = None,
     val activity_purpose : Option[String] = None,
     val activity_status : Option[String] = None,
     val activity_target_name : Option[String] = None,
     val activity_target_type : Option[String] = None,

    // Other Ids
     val mailing_address_id : Long = -1,
     val contact_pool_id : Long = -1) extends Serializable {

  override def toString() = {
    id.toString() + " : " + first_name.get  + " : " + date_of_birth.get + " : " + civic_address_city.get + " : " + contact_methods.get.mkString(":")
  }
}

object Contact extends Serializable {
  val CONTACT_METHOD_BULK_EMAIL = "bulk_email" 
  val CONTACT_METHOD_BULK_MAIL = "bulk_mail"
  val CONTACT_METHOD_EMAIL = "email"
  val CONTACT_METHOD_MAIL = "mail"
  val CONTACT_METHOD_CANVAS = "canvas"
  val CONTACT_METHOD_CALL = "call"
  val CONTACT_METHOD_SMS = "sms"
  val CONTACT_METHOD_VOICE_BROADCAST = "voice_broadcast"
}
