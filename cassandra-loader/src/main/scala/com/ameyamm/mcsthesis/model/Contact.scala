package com.ameyamm.mcsthesis.model

import java.util.Date
/**
 * @author ameya
 */
class Contact (
    
    private val id : Long,

    // Contact Details
    // Person
    private val _firstName : Option[String] = None,
    private val _lastName : Option[String] = None,
    private val _middleName : Option[String] = None,
    private val _honorific : Option[String] = None,
    private val _suffix : Option[String] = None,
    
    // Organization
    private val _organizationName : Option[String] = None,
    private val _organizationName2 : Option[String] = None,
    private val _contactType : Option[String] = None,

    // Personal details
    private val _dateOfBirth : Option[Date] = None,
    private val _deceased : Boolean = false,
    private val _employer : Option[String] = None,
    private val _gender : Option[String] = None,
    private val _industry : Option[String] = None,
    private val _language : Option[String] = None,
    private val _occupation : Option[String] = None,
    
    // Communication 
    private val _contactMethod : Option[Set[String]] = None,
    private val _email : Option[Set[String]] = None,
    private val _phoneNumber : Option[String] = None,

    // Civic Address
    private val _civicAddrId : Long = -1,
    private val _civicAddrType : Option[String] = None,
    private val _civicAddrBuildingNum : Option[String] = None, 
    private val _civicAddrApartmentNum : Option[String] = None, 
    private val _civicAddrCity : Option[String] = None,
    private val _civicAddrCountry : Option[String] = None,
    private val _civicAddrLine1 : Option[String] = None,
    private val _civicAddrLine2 : Option[String] = None,
    private val _civicAddrMeridian : Option[String] = None, 
    private val _civicAddrNumSuffix : Option[String] = None,
    private val _civicAddrPostalCode : Option[String] = None,
    private val _civicAddrProvince : Option[String] = None,
    private val _civicAddrRange : Option[String] = None,
    private val _civicAddrQuarter : Option[String] = None,
    private val _civicAddrReserve : Option[String] = None, 
    private val _civicAddrSection : Option[String] = None,

    // Electoral details 
    private val _federalElectorId : Option[String] = None,
    private val _federalPoll : Option[String] = None,
    private val _federalRiding : Option[String] = None, 
    private val _federalSeqNum : Option[String] = None,

    // Misc Properties
    private val _marks : Int = 0,
    private val _leaning : Char = '\0',
    private val _etagName : Option[String] = None,
    private val _etagDesc : Option[String] = None,
    private val _etagEnumName : Option[String] = None,
    private val _etagEnumDesc : Option[String] = None,
    private val _etagStr : Option[String] = None,
    private val _tagDesc : Option[String] = None,
    private val _tagName : Option[String] = None,
    private val _tagType : Option[String] = None,
    private val _surveyResponse : Option[Map[String, String]] = None,
    private val _activityListName : Option[String] = None,
    private val _activityListStatus : Option[String] = None,
    private val _activityType : Option[String] = None,
    private val _activityMethod : Option[String] = None,
    private val _activityName : Option[String] = None,
    private val _activityNotes : Option[String] = None,
    private val _activityPurpose : Option[String] = None,
    private val _activityStatus : Option[String] = None,
    private val _activityTargetName : Option[String] = None,
    private val _activityTargetType : Option[String] = None,

    // Other Ids
    private val _mailingAddressId : Long = -1,
    private val _contactPoolId : Long = -1) extends Serializable {

  override def toString() = {
    id.toString() + " : " + _firstName.get  + " : " + _dateOfBirth.get + " : " + _civicAddrCity.get + " : " +_contactMethod.get.mkString(":")
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