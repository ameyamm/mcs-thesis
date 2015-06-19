'''
Created on Jun 18, 2015

@author: ameya
'''

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

    def __init__(self, 
            id,
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
            federal_sequence_number = None,
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
            etags = None,
            townhouse_or_apartment = None   
            ):
        '''
        Constructor
        '''
        # identification attributes
        self.id = id 
        self.first_name = first_name 
        self.middle_name = middle_name
        self.last_name = last_name
        self.contact_type = contact_type
        self.organization_name = organization_name

        # demographic attributes
        self.date_of_birth = date_of_birth
        deceased = False
        self.employer = employer
        self.gender = gender 
        self.industry = industry
        self.language = language
        self.occupation = occupation 

        # behavorial attributes
        self.contact_methods = contact_methods # set
        self.mark = None
        self.leaning = None
        self.cases_filed = 0 
        self.cases_closed = 0 
        self.etags = None # set
        self.survey_responses = None # dictionary[Survey Ques, Survey Resp]

        # electoral attributes
        self.federal_elector_id = federal_elector_id
        self.federal_poll = federal_poll
        self.federal_riding = federal_riding
        self.federal_sequence_number = federal_sequence_number

        # demographic attributes
        self.has_email = has_email

        # location attributes
        self.townhouse_or_apartment = townhouse_or_apartment
        self.civic_address_type = civic_address_type 
        self.civic_address_city = civic_address_city
        self.civic_address_province = civic_address_province
        self.civic_address_country = civic_address_country 
        self.civic_address_postal_code = civic_address_postal_code
        self.civic_address_street_type = civic_address_street_type
        self.civic_address_township = civic_address_township
        
        







