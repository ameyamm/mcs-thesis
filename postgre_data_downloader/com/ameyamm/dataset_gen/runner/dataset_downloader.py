'''
Created on Jun 18, 2015

@author: ameya
'''



import psycopg2
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

def loadTContact(conn):
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(queries.SELECT_TCONTACT)
    for contactRec in cursor:
        contact = Contact(
                          contact_id = contactRec['id'],
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
        
        print("{}::{}::{}::{}::{}".format(contactRec["first_name"], 
                                      contactRec["id"],
                                      contactRec['email_preferred'],
                                      contactRec['civic_address_building_number'],
                                      contactRec['civic_address_apartment_number']))
    return
    
def main():
    conn = getDBConnection()        
    loadTContact(conn)

    return
    
if __name__ == '__main__':
    main()