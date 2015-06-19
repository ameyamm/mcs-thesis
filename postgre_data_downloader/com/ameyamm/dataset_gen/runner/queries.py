'''
Created on Jun 19, 2015

@author: ameya
'''
SELECT_TCONTACT = """
                    select 
                        id, 
                        first_name,
                        middle_name,
                        last_name, 
                        contact_type,
                        organization_name, 
                        date_of_birth,
                        deceased,
                        employer,
                        gender,
                        industry,
                        language,
                        occupation,
                        federal_elector_id,
                        federal_poll,
                        federal_riding,
                        federal_seq_number,
                        email_preferred,
                        allow_bulk_mail,
                        allow_bulk_email,
                        allow_email,
                        allow_mail,
                        allowsms,
                        allow_call,
                        allow_voice_broadcast,
                        allow_canvas,
                        civic_address_type,
                        civic_address_building_number,
                        civic_address_apartment_number,
                        civic_address_city,
                        civic_address_province,
                        civic_address_postal_code,
                        civic_address_country,
                        civic_address_township,
                        civic_address_street_type
                    from public.t_contact_search_fast limit 10;
                    """
                        
                        
                        