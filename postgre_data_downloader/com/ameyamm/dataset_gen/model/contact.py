'''
Created on Jun 18, 2015

@author: ameya
'''

import copy
import string

class Contact:
    '''
    classdocs
    '''
    CONTACT_METHODS_BULK_EMAIL = "bulk_email"
    CONTACT_METHODS_BULK_MAIL  = "bulk_mail"
    CONTACT_METHODS_EMAIL      = "email"
    CONTACT_METHODS_MAIL       = "mail"
    CONTACT_METHODS_CALL       = "call"
    CONTACT_METHODS_CANVAS     = "canvas"
    CONTACT_METHODS_SMS        = "sms"
    CONTACT_METHODS_VOICE_BROADCAST = "voice_broadcast"
    
    CONTACT_TOWNHOUSE = "townhouse"
    CONTACT_APARTMENT = "apartment"
    
    T_ANALYTICS_CONTACT_COLUMNS = {
                                        "contact_id": "%r",
                                        "campaign_id": "%r",
                                        "region_id": "%r",
                                        "activity_id": "%r",
                                        "first_name": "%s",
                                        "middle_name": "%s",
                                        "last_name": "%s",
                                        "contact_type": "%s",
                                        "organization_name": "%s",
                                        "date_of_birth": "%s",
                                        "deceased": "%r",
                                        "employer": "%s",
                                        "gender": "%s",
                                        "industry": "%s",
                                        "language": "%s",
                                        "occupation": "%s",
                                        "contact_methods": "%s",
                                        "mark": "%s",
                                        "leaning": "%s",
                                        "cases_filed": "%d",
                                        "cases_closed": "%d",
                                        "etags": "%s",
                                        "survey_responses": "%s",
                                        "tags": "%s",
                                        "federal_elector_id": "%r",
                                        "federal_poll": "%r",
                                        "federal_riding": "%r",
                                        "federal_seq_number": "%r",
                                        "has_email": "%r",
                                        "townhouse_or_apartment": "%s",
                                        "civic_address_type": "%s",
                                        "civic_address_city": "%s",
                                        "civic_address_province": "%s",
                                        "civic_address_country": "%s",
                                        "civic_address_postal_code": "%s",
                                        "civic_address_street_type": "%s",
                                        "civic_address_township": "%s" 
                                    }

    def __init__(self, 
            contact_id,
            first_name = None,
            middle_name = None,
            last_name = None,
            contact_type = None,
            organization_name = None,
            date_of_birth = None,
            deceased = False,
            employer = None,
            gender = None ,
            industry = None,
            language = None,
            occupation = None ,
            contact_methods = None, 
            federal_elector_id = None,
            federal_poll = None,
            federal_riding = None,
            federal_seq_number = None,
            has_email = False,
            civic_address_type = None ,
            civic_address_city = None,
            civic_address_province = None,
            civic_address_country = None ,
            civic_address_postal_code = None,
            civic_address_street_type = None,
            civic_address_township = None,
            mark = None,
            leaning = None,
            cases_filed = 0,
            cases_closed = 0,
            etags = None
            ):
        '''
        Constructor
        '''
        # identification attributes
        self.contact_id = contact_id 
        self.campaign_id = None
        self.region_id = None
        self.activity_id = None
        self.first_name = string.replace(first_name,"'","") if first_name is not None else None
        self.middle_name = string.replace(middle_name,"'","") if middle_name is not None else None
        self.last_name = string.replace(last_name,"'","") if last_name is not None else None
        self.contact_type = string.replace(contact_type,"'","") if contact_type is not None else None
        self.organization_name = string.replace(organization_name,"'","") if organization_name is not None else None

        # demographic attributes
        self.date_of_birth = date_of_birth
        self.deceased = False
        self.employer = string.replace(employer,"'","") if employer is not None else None
        self.gender = string.replace(gender ,"'","") if gender  is not None else None
        self.industry = string.replace(industry,"'","") if industry is not None else None
        self.language = string.replace(language,"'","") if language is not None else None
        self.occupation = string.replace(occupation,"'","") if occupation is not None else None 

        # behavorial attributes
        self.contact_methods = None # set
        self.mark = None
        self.leaning = None
        self.cases_filed = 0 
        self.cases_closed = 0 
        self.etags = None #set
        self.survey_responses = None # dictionary[Survey Ques, Survey Resp]
        self.tags = None # set

        # electoral attributes
        self.federal_elector_id = federal_elector_id
        self.federal_poll = federal_poll
        self.federal_riding = federal_riding
        self.federal_seq_number = federal_seq_number

        # demographic attributes
        self.has_email = has_email

        # location attributes
        self.townhouse_or_apartment = None
        self.civic_address_type = string.replace(civic_address_type ,"'","") if civic_address_type  is not None else None
        self.civic_address_city = string.replace(civic_address_city,"'","") if civic_address_city is not None else None
        self.civic_address_province = string.replace(civic_address_province,"'","") if civic_address_province is not None else None
        self.civic_address_country = string.replace(civic_address_country ,"'","") if civic_address_country  is not None else None
        self.civic_address_postal_code = string.replace(civic_address_postal_code,"'","") if civic_address_postal_code is not None else None
        self.civic_address_street_type = string.replace(civic_address_street_type,"'","") if civic_address_street_type is not None else None
        self.civic_address_township = string.replace(civic_address_township,"'","") if civic_address_township is not None else None
        
    def copy(self):
        return copy.deepcopy(self)
    
    def setContactMethods(self, 
                          allow_bulk_email, 
                          allow_bulk_mail,
                          allow_email,
                          allow_call,
                          allow_mail,
                          allow_sms,
                          allow_voice_broadcast,
                          allow_canvas):
        self.contact_methods = set()
        if allow_bulk_email is True:
            self.contact_methods.add(Contact.CONTACT_METHODS_BULK_EMAIL)   
        if allow_bulk_mail is True:
            self.contact_methods.add(Contact.CONTACT_METHODS_BULK_MAIL)   
        if allow_email is True:
            self.contact_methods.add(Contact.CONTACT_METHODS_EMAIL)   
        if allow_mail is True:
            self.contact_methods.add(Contact.CONTACT_METHODS_MAIL)   
        if allow_call is True:
            self.contact_methods.add(Contact.CONTACT_METHODS_CALL)   
        if allow_sms is True:
            self.contact_methods.add(Contact.CONTACT_METHODS_SMS)   
        if allow_canvas is True:
            self.contact_methods.add(Contact.CONTACT_METHODS_CANVAS)   
        if allow_voice_broadcast is True:
            self.contact_methods.add(Contact.CONTACT_METHODS_VOICE_BROADCAST)   

    def addTags(self, tag_name):
        if tag_name is None:
            return 
        
        if self.tags is None:
            self.tags = set()

        self.tags.add(string.replace(tag_name,"'",""))
        
    def addEtags(self, etag_name, etag_enum):
        if etag_name is None or etag_name == "":
            return 
        
        if self.etags is None:
            self.etags = set()
            
        self.etags.add(string.replace(etag_name,"'","") + ':' + (etag_enum if etag_enum is not None else ""))
        
    def getInsertQueryString(self):
        queryColumnStr = ' '.join([
                    "insert into t_analytics_contact (", 
                    "contact_id,",
                    "campaign_id,",
                    "region_id,",
                    "activity_id,",
                    "first_name,",
                    "middle_name,",
                    "last_name,",
                    "contact_type,",
                    "organization_name,",
                    "date_of_birth,",
                    "deceased,",
                    "employer,",
                    "gender,",
                    "industry,",
                    "language,",
                    "occupation,",
                    "contact_methods,",
                    "mark,",
                    "leaning,",
                    "cases_filed,",
                    "cases_closed,",
                    "etags,",
                    "survey_responses,",
                    "tags,",
                    "federal_elector_id,",
                    "federal_poll,",
                    "federal_riding,",
                    "federal_seq_number,",
                    "has_email,",
                    "townhouse_or_apartment,",
                    "civic_address_type,",
                    "civic_address_city,",
                    "civic_address_province,",
                    "civic_address_country,",
                    "civic_address_postal_code,",
                    "civic_address_street_type,",
                    "civic_address_township", 
                    ")",
                    "values (",
                    ""])

        
        # create a string of,%r ... for as many attributes in the class
        queryValueString = ",".join(['%s' for attrib in self.__dict__.keys() if not attrib.startswith("__")]) + ")"    
        
        queryValue = (
                        self.contact_id,
                        self.campaign_id,
                        self.region_id,
                        self.activity_id,
                        self.first_name,
                        self.middle_name,
                        self.last_name,
                        self.contact_type,
                        self.organization_name,
                        self.date_of_birth,
                        self.deceased,
                        self.employer,
                        self.gender,
                        self.industry,
                        self.language,
                        self.occupation,
                        "'[" + "|".join(self.contact_methods) + "]'" if self.contact_methods is not None else None, 
                        self.mark,
                        self.leaning,
                        self.cases_filed,
                        self.cases_closed,
                        "'[" + "|".join(self.etags) + "]'" if self.etags is not None else None, 
                        self.survey_responses,
                        "'[" + "|".join(self.tags) + "]'" if self.tags is not None else None,
                        self.federal_elector_id,
                        self.federal_poll,
                        self.federal_riding,
                        self.federal_seq_number,
                        self.has_email,
                        self.townhouse_or_apartment,
                        self.civic_address_type,
                        self.civic_address_city,
                        self.civic_address_province,
                        self.civic_address_country,
                        self.civic_address_postal_code,
                        self.civic_address_street_type,
                        self.civic_address_township 
                    )
                        
        return (queryColumnStr + queryValueString, queryValue)

        '''
                return queryColumnStr + queryValueString.format(
                                                    self.contact_id, 
                                                    self.campaign_id if self.campaign_id is not None else "null", 
                                                    self.region_id if self.region_id is not None else "null", 
                                                    self.activity_id if self.activity_id is not None else "null", 
                                                    "'" + self.first_name + "'" if self.first_name is not None else "null", 
                                                    "'" + self.middle_name + "'" if self.middle_name is not None else "null", 
                                                    "'" + self.last_name + "'" if self.last_name is not None else "null", 
                                                    "'" + self.contact_type + "'" if self.contact_type is not None else "null", 
                                                    "'" + self.organization_name + "'" if self.organization_name is not None else "null", 
                                                    "'" + self.date_of_birth + "'" if self.date_of_birth is not None else "null", 
                                                    self.deceased,
                                                    "'" + self.employer + "'" if self.employer is not None else "null", 
                                                    "'" + self.gender + "'" if self.gender is not None else "null", 
                                                    "'" + self.industry + "'" if self.industry is not None else "null", 
                                                    "'" + self.language + "'" if self.language is not None else "null", 
                                                    "'" + self.occupation + "'" if self.occupation is not None else "null", 
                                                    "'[" + "|".join(self.contact_methods) + "]'" if self.contact_methods is not None else None, 
                                                    self.mark, 
                                                    "'" + self.leaning + "'" if self.occupation is not None else None, 
                                                    self.cases_filed, 
                                                    self.cases_closed, 
                                                    "'[" + "|".join(self.etags) + "]'" if self.etags is not None else None, 
                                                    self.survey_responses, 
                                                    "'[" + "|".join(self.tags) + "]'" if self.tags is not None else None, 
                                                    self.federal_elector_id, 
                                                    self.federal_poll, 
                                                    self.federal_riding, 
                                                    self.federal_seq_number, 
                                                    self.has_email, 
                                                    "'" + self.townhouse_or_apartment + "'" if self.civic_address_type is not None else None, 
                                                    "'" + self.civic_address_type + "'" if self.civic_address_type is not None else None, 
                                                    "'" + self.civic_address_city + "'" if self.civic_address_city is not None else None, 
                                                    "'" + self.civic_address_province + "'" if self.civic_address_province is not None else None, 
                                                    "'" + self.civic_address_country + "'" if self.civic_address_country is not None else None, 
                                                    "'" + self.civic_address_postal_code + "'" if self.civic_address_postal_code is not None else None, 
                                                    "'" + self.civic_address_street_type + "'" if self.civic_address_street_type is not None else None, 
                                                    "'" + self.civic_address_township + "'" if self.civic_address_township is not None else None) + ")"
        '''



