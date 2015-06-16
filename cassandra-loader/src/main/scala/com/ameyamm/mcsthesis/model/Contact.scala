package com.ameyamm.mcsthesis.model

import java.util.Date
/**
 * @author ameya
 */
class Contact(
    private val id : Long,

    // Contact Details
    // Person
    private val _firstName : String = null,
    private val _lastName : String = null,
    private val _middleName : String = null,
    private val _honorific : String = null,
    private val _suffix : String = null,
    
    // Organization
    private val _organizationName : String = null,
    private val _organizationName2 : String = null,
    private val _contactType : String = null,

    // Personal details
    private val _dateOfBirth : Date = null,
    private val _deceased : Boolean = false,
    private val _employer : String = null,
    private val _gender : String = null,
    private val _industry : String = null,
    private val _language : String = null,
    private val _occupation : String = null,
    
    // Communication 
    private val _contactMethod : Set[String] = null,
    private val _email : Set[String] = null,
    private val _phoneNumber : String = null,

    // Civic Address
    private val _civicAddressId : Long = -1,
    private val _civicAddrType : String = null,
    private val _civicAddrBuildingNum : String = null, 
    private val _civicAddrApartmentNum : String = null, 
    private val _civicAddrCity : String = null,
    private val _civicAddrCountry : String = null,
    private val _civicAddrLine1 : String = null,
    private val _civicAddrLine2 : String = null,
    private val _civicAddrMeridian : String = null, 
    private val _civicAddrNumSuffix : String = null,
    private val _civicAddrPostalCode : String = null,
    private val _civicAddrProvince : String = null,
    private val _civicAddrRange : String = null,
    private val _civicAddrQuarter : String = null,
    private val _civicAddrReserve : String = null, 
    private val _civicAddrSection : String = null,

    // Electoral details 
    private val _federalElectorId : String = null,
    private val _federalPoll : String = null,
    private val _federalRiding : String = null, 
    private val _federalSeqNum : String = null,

    // Misc Properties
    private val _marks : Int = 0,
    private val _leaning : Char = '\0',
    private val _etagName : String = null,
    private val _etagDesc : String = null,
    private val _etagEnumName : String = null,
    private val _etagEnumDesc : String = null,
    private val _etagString : String = null,
    private val _tagDesc : String = null,
    private val _tagName : String = null,
    private val _tagType : String = null,
    private val _surveyResponse : Map[String, String] = null,
    private val _activityListName : String = null,
    private val _activityListStatus : String = null,
    private val _activityType : String = null,
    private val _activityMethod : String = null,
    private val _activityName : String = null,
    private val _activityNotes : String = null,
    private val _activityPurpose : String = null,
    private val _activityStatus : String = null,
    private val _activityTargetName : String = null,
    private val _activityTargetType : String = null,

    // Other Ids
    private val _mailingAddressId : Long = -1,
    private val _contactPoolId : Long = -1) {
  
}