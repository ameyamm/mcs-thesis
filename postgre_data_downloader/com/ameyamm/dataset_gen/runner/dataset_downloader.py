'''
Created on Jun 18, 2015

@author: ameya
'''

from __future__ import generators 
import threading
import logging

import psycopg2.extras

from com.ameyamm.dataset_gen.runner import queries
from com.ameyamm.dataset_gen.model.contact import Contact

logging.basicConfig(filename = "datadownload-upload.log",
        level=logging.DEBUG,
        format='%(threadName)-10s : %(message)s',
        )

def resultSetGenerator(cursor, size = 1000):
    while True:
        results = cursor.fetchmany(size)
        if not results:
            break 
        for result in results:
            yield result

class dbthread(threading.Thread):
    def __init__(self, threadProvince):
        threading.Thread.__init__(self)
        self.name = 'Thread-{}'.format(threadProvince) 
        self.threadProvince = threadProvince

    def run(self):
        logging.debug('Starting thread for {}'.format(self.name))
        loadTarget(threadName = self.name, threadProvince = self.threadProvince)
        

def getDBConnection():
    try :
        return psycopg2.connect(host = "XXX.XXX.XXX.XX",
                                database = "XXXXXXX",
                                user = 'XXXXXXX',
                                password ='XXXXXXXXXX')
    except:
        print("Unable to connect to database")
        return None

def loadTarget(threadProvince, threadName = None):
    t_contact_conn = getDBConnection()        
    rec_conn = getDBConnection()   
    t_analytics_contact_conn = getDBConnection()

    t_contact_cursor = t_contact_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    record_cursor = rec_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    t_analytics_contact_cursor = t_analytics_contact_conn.cursor()
   
    t_contact_cursor.execute(queries.SELECT_T_CONTACT.format(threadProvince))
    for contactRec in resultSetGenerator(t_contact_cursor):
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
        
        record_cursor.execute(queries.SELECT_T_CASE_FILE_FILED.format(contact.contact_id))
        caseFiledRow = record_cursor.fetchone()
        contact.cases_filed = caseFiledRow['cases_filed']

        record_cursor.execute(queries.SELECT_T_CASE_FILE_CLOSED.format(contact.contact_id))
        caseClosedRow = record_cursor.fetchone()

        contact.cases_closed = caseClosedRow['cases_closed']
        
        # find all associated tags
        record_cursor.execute(queries.SELECT_T_TAG_QUERY.format(contact.contact_id))
        tagRows = record_cursor.fetchall()
        for tagRow in tagRows:
            contact.addTags(tagRow['name'])
        
        # find the campaigns and regions associated to create duplicate contacts
        record_cursor.execute(
                            queries.SELECT_DISTINCT_CONTACT_CAMPAIGN_REGION.format(contact.contact_id, contact.contact_id))

        logging.debug(contact.contact_id)
        copyContact = None
        distinctContactRows = record_cursor.fetchall()
        for distinctContactRow in distinctContactRows:
            copyContact = contact.copy()
            copyContact.campaign_id = distinctContactRow['campaign_id']
            copyContact.region_id = distinctContactRow['region_id']
            
            # get marks 
            if copyContact.campaign_id is not None and copyContact.region_id is not None :  
                record_cursor.execute(queries.SELECT_T_CONTACT_MARKS.format(
                                                                         copyContact.contact_id, 
                                                                         copyContact.campaign_id,
                                                                         copyContact.region_id))
                marksRow = record_cursor.fetchone()
                if marksRow is not None : 
                    copyContact.mark = marksRow['mark']
                    copyContact.leaning = marksRow['leaning']
                
            record_cursor.execute(queries.SELECT_T_ENUM_TAG.format(
                                                                   copyContact.contact_id,
                                                                   ' = ' + str(copyContact.campaign_id) if copyContact.campaign_id is not None else 'is null',
                                                                   ' = ' + str(copyContact.region_id) if copyContact.region_id is not None else 'is null'))
            
            etagRows = record_cursor.fetchall()
            for etagRow in etagRows:
                copyContact.addEtags(etagRow['etag_name'],etagRow['etag_enum'])
            
            insertQuery = copyContact.getInsertQueryString()

            try : 
                t_analytics_contact_cursor.execute(insertQuery[0],insertQuery[1])    
            except Exception as e :
                logging.debug("Exception CopyContact : {} : {}".format(
                                                    copyContact.contact_id,e))
                pass

            #print("{}".format(copyContact.getInsertQueryString()))
                
        if copyContact is None:
            insertQuery = contact.getInsertQueryString()
            try : 
                t_analytics_contact_cursor.execute(insertQuery[0],insertQuery[1])    
            except Exception as e:
                logging.debug("Exception Contact: {} : {}".format(
                                                    contact.contact_id,e))
                pass

        t_analytics_contact_conn.commit()
    
    record_cursor.close()
    t_contact_cursor.close()
    t_analytics_contact_cursor.close()
    
    t_contact_conn.close()
    rec_conn.close()
    t_analytics_contact_conn.close()
    
    return
    
def main():
    provinces_set1 = [ 'AB', 'NU', 'MB' ]
    provinces_set2 = [ 'NB', 'NL', 'ON' ]
    provinces_set3 = [ 'NT', 'BC', 'NS']
    provinces_set4 = [ 'PE', 'QC', 'SK', 'YT' ]

    threads = [dbthread(province) for province in provinces_set1]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    threads = [dbthread(province) for province in provinces_set2]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    threads = [dbthread(province) for province in provinces_set3]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    threads = [dbthread(province) for province in provinces_set4]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    return
    
if __name__ == '__main__':
    main()
