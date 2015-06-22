'''
Created on Jun 18, 2015

@author: ameya
'''



import psycopg2.extras

from com.ameyamm.dataset_gen.runner import queries
from com.ameyamm.dataset_gen.model.contact import Contact

def getDBConnection():
    try :
        return psycopg2.connect(host = "192.168.100.74",
                                database = "vote4db",
                                user = 'netfore',
                                password ='netforePWD')
    except:
        print("Unable to connect to database")
        return None

def loadTarget():
    t_contact_conn = getDBConnection()        
    t_marks_conn = getDBConnection()     
    t_case_file_conn = getDBConnection()   
    t_etag_conn = getDBConnection()
    t_tag_conn = getDBConnection()
    t_contact_campaign_region_conn = getDBConnection()
    t_analytics_contact_conn = getDBConnection()

    t_contact_cursor = t_contact_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    t_marks_cursor = t_marks_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    t_case_file_cursor = t_case_file_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    t_etag_cursor = t_etag_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    t_tag_cursor = t_tag_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    t_contact_campaign_region_cursor = t_contact_campaign_region_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    t_analytics_contact_cursor = t_analytics_contact_conn.cursor()
   
    t_contact_cursor.execute(queries.SELECT_T_CONTACT)
    for contactRec in t_contact_cursor:
        contact = Contact(
                          contact_id = contactRec['contact_id'],
                          first_name= contactRec['first_name'],
                          middle_name=contactRec['middle_name'],
                          last_name=contactRec['last_name'],
                          contact_type=contactRec['contact_type'],
                          organization_name = contactRec['organization_name'],
                          date_of_birth = contactRec['date_of_birth'],
                          deceased = contactRec['deceased'],
                          employer = contactRec['employer'],
                          gender = contactRec['gender'],
                          industry = contactRec['industry'],
                          language = contactRec['language'],
                          occupation = contactRec['occupation'],
                          federal_elector_id = contactRec['federal_elector_id'],
                          federal_poll = contactRec['federal_poll'],
                          federal_riding = contactRec['federal_riding'],
                          federal_seq_number = contactRec['federal_seq_number'],
                          civic_address_type = contactRec['civic_address_type'],
                          civic_address_city = contactRec['civic_address_city'],
                          civic_address_province = contactRec['civic_address_province'],
                          civic_address_country = contactRec['civic_address_country'],
                          civic_address_postal_code = contactRec['civic_address_postal_code'],
                          civic_address_street_type = contactRec['civic_address_street_type'],
                          civic_address_township = contactRec['civic_address_street_type']
                        )

        
        contact.setContactMethods(allow_bulk_email = contactRec['allow_bulk_email'],
                                  allow_bulk_mail = contactRec['allow_bulk_mail'],
                                  allow_email = contactRec['allow_email'],
                                  allow_mail = contactRec['allow_mail'],
                                  allow_canvas = contactRec['allow_canvas'],
                                  allow_sms = contactRec['allowsms'],
                                  allow_call = contactRec['allow_call'],
                                  allow_voice_broadcast = contactRec['allow_voice_broadcast']
                                  )
        
        if contactRec['email_preferred'] is not None:
            contact.has_email = True

        if contactRec['civic_address_apartment_number'] is not None : 
            contact.townhouse_or_apartment = Contact.CONTACT_APARTMENT
        else:
            contact.townhouse_or_apartment = Contact.CONTACT_TOWNHOUSE
        
        t_case_file_cursor = t_case_file_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        t_case_file_cursor.execute(queries.SELECT_T_CASE_FILE_FILED.format(contact.contact_id))
        caseFiledRow = t_case_file_cursor.fetchone()
        contact.cases_filed = caseFiledRow['cases_filed']
        
        t_case_file_cursor.execute(queries.SELECT_T_CASE_FILE_CLOSED.format(contact.contact_id))
        caseClosedRow = t_case_file_cursor.fetchone()
        contact.cases_closed = caseClosedRow['cases_closed']
        
        # find all associated tags
        t_tag_cursor.execute(queries.SELECT_T_TAG_QUERY.format(contact.contact_id))
        for tagRow in t_tag_cursor:
            contact.addTags(tagRow['name'])
        
        # find the campaigns and regions associated to create duplicate contacts
        t_contact_campaign_region_cursor.execute(
                            queries.SELECT_DISTINCT_CONTACT_CAMPAIGN_REGION.format(contact.contact_id, contact.contact_id))
        
        print(contact.first_name)
        copyContact = None
        for distinctContactRow in t_contact_campaign_region_cursor:
            copyContact = contact.copy()
            copyContact.campaign_id = distinctContactRow['campaign_id']
            copyContact.region_id = distinctContactRow['region_id']
            
            # get marks 
            if copyContact.campaign_id is not None and copyContact.region_id is not None :  
                t_marks_cursor.execute(queries.SELECT_T_CONTACT_MARKS.format(
                                                                         copyContact.contact_id, 
                                                                         copyContact.campaign_id,
                                                                         copyContact.region_id))
                marksRow = t_marks_cursor.fetchone()
                if marksRow is not None : 
                    copyContact.mark = marksRow['mark']
                    copyContact.leaning = marksRow['leaning']
                
            t_etag_cursor.execute(queries.SELECT_T_ENUM_TAG.format(
                                                                   copyContact.contact_id,
                                                                   ' = ' + str(copyContact.campaign_id) if copyContact.campaign_id is not None else 'is null',
                                                                   ' = ' + str(copyContact.region_id) if copyContact.region_id is not None else 'is null'))
            
            for etagRow in t_etag_cursor:
                copyContact.addEtags(etagRow['etag_name'],etagRow['etag_enum'])
            
            insertQuery = copyContact.getInsertQueryString()
            t_analytics_contact_cursor.execute(insertQuery[0],insertQuery[1])    
            #print("{}".format(copyContact.getInsertQueryString()))
                
            
            
        if copyContact is None:
            insertQuery = contact.getInsertQueryString()
            t_analytics_contact_cursor.execute(insertQuery[0],insertQuery[1])

    t_analytics_contact_conn.commit()
    
    t_contact_cursor.close()
    t_marks_cursor.close()
    t_case_file_cursor.close()
    t_etag_cursor.close()
    t_tag_cursor.close()
    t_contact_campaign_region_cursor.close()
    t_analytics_contact_cursor.close()
    
    t_contact_conn.close()
    t_marks_conn.close()
    t_case_file_conn.close()
    t_etag_conn.close()
    t_tag_conn.close()
    t_contact_campaign_region_conn.close()
    t_analytics_contact_conn.close()
    
    return
    
def main():
    loadTarget()
    

    return
    
if __name__ == '__main__':
    main()