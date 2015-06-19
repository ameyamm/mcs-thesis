'''
Created on Jun 18, 2015

@author: ameya
'''
import psycopg2

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
    return
    
def main():
    conn = getDBConnection()        
    loadTContact(conn)

    return
    
if __name__ == '__main__':
    main()