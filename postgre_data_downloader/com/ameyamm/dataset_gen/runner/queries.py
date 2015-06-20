'''
Created on Jun 19, 2015

@author: ameya
'''

SELECT_TCONTACT = """
                    select 
                        id as contact_id, 
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
SELECT_T_MARKS = """select 
                        mark,
                        leaning,
                        campaign_id,
                        region_id
                    from 
                        t_contact_marks
                    where 
                        contact_id = {}"""
'''
SELECT_TCONTACT = """
            select 
            distinct
            contact.id contact_id, 
            contact.first_name first_name,
            contact.middle_name middle_name,
            contact.last_name last_name, 
            contact.contact_type contact_type,
            contact.organization_name organization_name, 
            contact.date_of_birth date_of_birth,
            contact.deceased deceased,
            contact.employer employer,
            contact.gender gender,
            contact.industry industry,
            contact.language as language,
            contact.occupation occupation,
            contact.federal_elector_id federal_elector_id,
            contact.federal_poll federal_poll,
            contact.federal_riding federal_riding,
            contact.federal_seq_number federal_seq_number,
            contact.email_preferred email_preferred,
            contact.allow_bulk_mail allow_bulk_mail,
            contact.allow_bulk_email allow_bulk_email,
            contact.allow_email allow_email,
            contact.allow_mail allow_mail,
            contact.allowsms allowsms,
            contact.allow_call allow_call,
            contact.allow_voice_broadcast allow_voice_broadcast,
            contact.allow_canvas allow_canvas,
            contact.civic_address_type civic_address_type,
            contact.civic_address_building_number civic_address_building_number,
            contact.civic_address_apartment_number civic_address_apartment_number,
            contact.civic_address_city civic_address_city,
            contact.civic_address_province civic_address_province,
            contact.civic_address_postal_code civic_address_postal_code,
            contact.civic_address_country civic_address_country,
            contact.civic_address_township civic_address_township,
            contact.civic_address_street_type civic_address_street_type,
            marks.mark mark,
            marks.leaning leaning,
            marks.campaign_id campaign_id,
            campaign.name campaign_name,
            campaign.description campaign_description,
            region.id region_id,
            region.name region_name
            --activity.id,
            --activity.activity_type,
            --activity.name
            from 
            public.t_contact_search_fast contact JOIN public.t_contact_marks marks ON (contact.id = marks.contact_id)
            JOIN public.t_campaign campaign ON (marks.campaign_id = campaign.id) 
            JOIN public.t_region region ON (region.id = marks.region_id)
            where marks.deletedtime is NULL 
            limit 10;
            """
'''

                        
                        
                        