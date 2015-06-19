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
    
    CONTACT_TOWNHOUSE = "townhouse"
    CONTACT_APARTMENT = "apartment"

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
        self.id = id 
        self.first_name = first_name 
        self.middle_name = middle_name
        self.last_name = last_name
        self.contact_type = contact_type
        self.organization_name = organization_name

        # demographic attributes
        self.date_of_birth = date_of_birth
        self.deceased = False
        self.employer = employer
        self.gender = gender 
        self.industry = industry
        self.language = language
        self.occupation = occupation 

        # behavorial attributes
        self.contact_methods = None # set
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
        self.federal_seq_number = federal_seq_number

        # demographic attributes
        self.has_email = has_email

        # location attributes
        self.townhouse_or_apartment = None
        self.civic_address_type = civic_address_type 
        self.civic_address_city = civic_address_city
        self.civic_address_province = civic_address_province
        self.civic_address_country = civic_address_country 
        self.civic_address_postal_code = civic_address_postal_code
        self.civic_address_street_type = civic_address_street_type
        self.civic_address_township = civic_address_township
        
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







