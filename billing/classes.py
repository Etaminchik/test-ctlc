from datetime import timedelta,datetime
import os
from psycopg2 import DatabaseError, errors

class Selects():
    def __init__(self,cur_,task_create_date_):
        self.cur = cur_
        self.task_create_date = task_create_date_
        


    def get_nums_tests(self):
        self.cur.execute("""
            select substring(task_body,25,2)::int as num,
            task_creation_date 
            from oimm.tasks
            where task_body like '<!--Comment:/load_tests/%'
            group by num,task_creation_date 
            order by num""")
        return self.cur.fetchall()

class Abonents():
    def __init__(self,cur_):
        self.cur = cur_
        self.end_users = """
            (select subs_id
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on sgh.sgnh_subs_id = s.subs_id 
            where sgh.sgnh_identity in 
            (select sgnh_identity  
            from oims.subs_generic_history
            where sgnh_subs_id in 
            (select subs_id 
            from oims.subscribers
            where subs_sbtp_id = 2))
            and s.subs_sbtp_id = 1)
        """

    def count_abonents(self,oper_id):
        result = []
        self.cur.execute(f"""
            select count(*)
            from oims.subscribers s 
            where subs_sbtp_id = 1
            and subs_id not in 
            {self.end_users}
            and subs_contract_close_date > now()
            and subs_oper_id = {oper_id}""")
        result.append(self.cur.fetchall()[0][0])
        self.cur.execute(f"""
            select count(*)
            from oims.subscribers s 
            where subs_sbtp_id = 1
            and subs_id not in 
            {self.end_users}
            and subs_contract_close_date < now()
            and subs_oper_id = {oper_id}""")
        result.append(self.cur.fetchall()[0][0])
        self.cur.execute(f"""
            select count(*)
            from oims.subscribers 
            where subs_sbtp_id = 2
            and subs_contract_close_date > now()
            and subs_oper_id = {oper_id}""")
        result.append(self.cur.fetchall()[0][0])
        self.cur.execute(f"""
            select count(*)
            from oims.subscribers 
            where subs_sbtp_id = 2
            and subs_contract_close_date < now()
            and subs_oper_id = {oper_id}""")
        result.append(self.cur.fetchall()[0][0])   
        self.cur.execute(f"""
            select count(*)
            from oims.subscribers s 
            where subs_sbtp_id = 1
            and subs_id in 
            {self.end_users}
            and subs_contract_close_date > now()
            and subs_oper_id = {oper_id}""")
        result.append(self.cur.fetchall()[0][0])
        self.cur.execute(f"""
            select count(*)
            from oims.subscribers s 
            where subs_sbtp_id = 1
            and subs_id in 
            {self.end_users}
            and subs_contract_close_date < now()
            and subs_oper_id = {oper_id}""")     
        result.append(self.cur.fetchall()[0][0])
        return result

    def fiz_fio_masks(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where (sprh_given_name = 'Имя'
            or sprh_patronymic_name = 'Отчество'
            or sprh_surname = 'Фамилия')
            and s.subs_id not in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and sph.sprh_end_date > now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where (sprh_given_name = 'Имя'
            or sprh_patronymic_name = 'Отчество'
            or sprh_surname = 'Фамилия')
            and s.subs_id not in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and sph.sprh_end_date < now()
            """)
        result.append(self.cur.fetchall())
        return result
    
    def ur_fio_masks(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where (sprh_given_name = 'Имя'
            or sprh_patronymic_name = 'Отчество'
            or sprh_surname = 'Фамилия')
            and s.subs_id in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where (sprh_given_name = 'Имя'
            or sprh_patronymic_name = 'Отчество'
            or sprh_surname = 'Фамилия')
            and s.subs_id in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result
    

    def fiz_bd_masks(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where sph.sprh_birth_date = '2000-01-01'
            and s.subs_id not in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and sph.sprh_end_date > now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where sph.sprh_birth_date = '2000-01-01'
            and s.subs_id not in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and sph.sprh_end_date < now()
            """)
        result.append(self.cur.fetchall())
        return result
    
    def ur_bd_masks(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where sph.sprh_birth_date = '2000-01-01'
            and s.subs_id in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where sph.sprh_birth_date = '2000-01-01'
            and s.subs_id in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result


    def fiz_doc_masks(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where (sprh_document_serial_number = '0000'
            or sprh_document_number = '000000'
            or sprh_document_issuer = 'Данные не предоставлены оператором'
            or sprh_document_issue_date = '2000-01-01')
            and sph.sprh_doct_id = 1
            and s.subs_id not in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and sph.sprh_end_date > now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where (sprh_document_serial_number = '0000'
            or sprh_document_number = '000000'
            or sprh_document_issuer = 'Данные не предоставлены оператором'
            or sprh_document_issue_date = '2000-01-01')
            and s.subs_id not in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and sph.sprh_end_date < now()
            """)
        result.append(self.cur.fetchall())
        return result
    
    def ur_doc_masks(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where (sprh_document_serial_number = '0000'
            or sprh_document_serial_number = '000000'
            or sprh_document_issuer = 'Данные не предоставлены оператором'
            or sprh_document_issue_date = '2000-01-01')
            and sph.sprh_doct_id = 1
            and s.subs_id in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where (sprh_document_serial_number = '0000'
            or sprh_document_serial_number = '000000'
            or sprh_document_issuer = 'Данные не предоставлены оператором'
            or sprh_document_issue_date = '2000-01-01')
            and s.subs_id in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result 


    def fiz_adr_masks(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 0 --Адрес регистрации
            and s.subs_sbtp_id = 1
            and s.subs_oper_id = {oper_id}
            and s.subs_id not in {self.end_users}
            and sah.sadh_end_date > now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 0 --Адрес регистрации
            and s.subs_sbtp_id = 1
            and s.subs_oper_id = {oper_id}
            and s.subs_id not in {self.end_users}
            and sah.sadh_end_date < now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 3 --Адрес установки оборудования
            and s.subs_sbtp_id = 1
            and s.subs_oper_id = {oper_id}
            and s.subs_id not in {self.end_users}
            and sah.sadh_end_date > now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 3 --Адрес установки оборудования
            and s.subs_sbtp_id = 1
            and s.subs_oper_id = {oper_id}
            and s.subs_id not in {self.end_users}
            and sah.sadh_end_date < now()
            """)
        result.append(self.cur.fetchall())
        return result
    
    def ur_ok_adr_masks(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 0 -- Регистрации
            and s.subs_sbtp_id = 1
            and s.subs_oper_id = {oper_id}
            and s.subs_id in {self.end_users}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 0 -- Регистрации
            and s.subs_sbtp_id = 1
            and s.subs_oper_id = {oper_id}
            and s.subs_id in {self.end_users}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 3 --Адрес установки оборудования
            and s.subs_sbtp_id = 1
            and s.subs_oper_id = {oper_id}
            and s.subs_id in {self.end_users}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 3 --Адрес установки оборудования
            and s.subs_sbtp_id = 1
            and s.subs_oper_id = {oper_id}
            and s.subs_id in {self.end_users}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result


    def ur_adr_masks(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 0 
            and s.subs_sbtp_id = 2
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 0 
            and s.subs_sbtp_id = 2
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 1
            and s.subs_sbtp_id = 2
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 1 
            and s.subs_sbtp_id = 2
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 2 
            and s.subs_sbtp_id = 2
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 2 
            and s.subs_sbtp_id = 2
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())

        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 3 
            and s.subs_sbtp_id = 2
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier 
            from oims.subscribers s 
            join oims.subs_addresses_history sah 
            on sah.sadh_subs_id = s.subs_id 
            where sah.sadh_building = 'НетДанных'
            and sah.sadh_addt_id = 3 
            and s.subs_sbtp_id = 2
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result

    def fiz_number(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier  
            from oims.subscribers s 
            full outer join oims.subs_phones_history sph 
            on sph.sphh_subs_id = s.subs_id 
            where s.subs_sbtp_id = 1
            and sph.sphh_phone is null
            and s.subs_id not in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier  
            from oims.subscribers s 
            full outer join oims.subs_phones_history sph 
            on sph.sphh_subs_id = s.subs_id 
            where s.subs_sbtp_id = 1
            and sph.sphh_phone is null
            and s.subs_id not in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result

    def fiz_number_630(self,oper_id):#check
        result = []
        try:
            self.cur.execute(f"""
                select distinct s.subs_external_identifier  
                from oims.subscribers s 
                join oims.subs_person_history sph
                on sph.sprh_subs_id = s.subs_id 
                where s.subs_sbtp_id = 1
                and sph.sprh_contact_phone is not null
                and s.subs_id not in {self.end_users}
                and s.subs_oper_id = {oper_id}
                and s.subs_contract_close_date > now()
            """)
            result.append(self.cur.fetchall())
        except DatabaseError as e:
            self.conn.rollback()
            result.append([])
        try:
            self.cur.execute(f"""
                select distinct s.subs_external_identifier  
                from oims.subscribers s 
                join oims.subs_person_history sph
                on sph.sprh_subs_id = s.subs_id 
                where s.subs_sbtp_id = 1
                and sph.sprh_contact_phone is not null
                and s.subs_id not in {self.end_users}
                and s.subs_oper_id = {oper_id}
                and s.subs_contract_close_date < now()
            """)
            result.append(self.cur.fetchall())
        except DatabaseError as e:
            self.conn.rollback()
            result.append([])
        return result

    def ur_number(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_organization_history soh 
            on soh.sorh_subs_id = s.subs_id 
            where soh.sorh_phone_fax = 'Данные не предоставлены оператором'
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_organization_history soh 
            on soh.sorh_subs_id = s.subs_id 
            where soh.sorh_phone_fax = 'Данные не предоставлены оператором'
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result

    def ur_name(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_organization_history soh 
            on soh.sorh_subs_id = s.subs_id 
            where soh.sorh_official_name = 'Данные не предоставлены оператором'
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_organization_history soh 
            on soh.sorh_subs_id = s.subs_id 
            where soh.sorh_official_name = 'Данные не предоставлены оператором'
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result

    def ur_bank(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on sgh.sgnh_subs_id = s.subs_id 
            where subs_sbtp_id = 2
            and (sgh.sgnh_bank_id in 
            (select bank_id
            from oims.banks
            where bank_name = 'Данные Не Предоставлены Оператором')
            or sgnh_bank_account = '-')
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on sgh.sgnh_subs_id = s.subs_id 
            where subs_sbtp_id = 2
            and (sgh.sgnh_bank_id in 
            (select bank_id
            from oims.banks
            where bank_name = 'Данные Не Предоставлены Оператором')
            or sgnh_bank_account = '-')
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result

    def ur_contact(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on sgh.sgnh_subs_id = s.subs_id 
            where subs_sbtp_id = 2
            and sgnh_contact_info = 'Фамилия Имя Отчество'
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on sgh.sgnh_subs_id = s.subs_id 
            where subs_sbtp_id = 2
            and sgnh_contact_info = 'Фамилия Имя Отчество'
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result


    def ur_inn(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on sgh.sgnh_subs_id = s.subs_id 
            where subs_sbtp_id = 2
            and sgnh_tax_reference_number = '0000000000'
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on sgh.sgnh_subs_id = s.subs_id 
            where subs_sbtp_id = 2
            and sgnh_tax_reference_number = '0000000000'
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result

    def fiz_minor(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where sph.sprh_birth_date > now() - interval ' 18 years'
            and s.subs_id not in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where sph.sprh_birth_date > now() - interval ' 18 years'
            and s.subs_id not in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result

    def ur_minor(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where sph.sprh_birth_date > now() - interval ' 18 years'
            and s.subs_id in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier
            from oims.subscribers s 
            join oims.subs_person_history sph 
            on sph.sprh_subs_id = s.subs_id 
            where sph.sprh_birth_date > now() - interval ' 18 years'
            and s.subs_id in {self.end_users}
            and s.subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result
    
    def ur_bez_okon(self,oper_id):#check
        result = []
        self.cur.execute(f"""
            select distinct s.subs_external_identifier, sgh.sgnh_identity
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on s.subs_id = sgh.sgnh_subs_id 
            where subs_sbtp_id = 2
            and sgh.sgnh_identity not in 
            (select sgh.sgnh_identity
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on s.subs_id = sgh.sgnh_subs_id 
            where subs_sbtp_id = 1
            and subs_oper_id = {oper_id})
            and subs_oper_id = {oper_id}
            and sgh.sgnh_end_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select distinct s.subs_external_identifier, sgh.sgnh_identity
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on s.subs_id = sgh.sgnh_subs_id 
            where subs_sbtp_id = 2
            and sgh.sgnh_identity not in 
            (select sgh.sgnh_identity
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on s.subs_id = sgh.sgnh_subs_id 
            where subs_sbtp_id = 1
            and subs_oper_id = {oper_id})
            and subs_oper_id = {oper_id}
            and s.subs_contract_close_date < now()
            """)
        result.append(self.cur.fetchall())
        return result    

    def subs_payments(self,oper_id):#check
        self.cur.execute(f"""
            select pp.pymp_name, count(1) 
            from oims.subscribers s 
            join oims.subs_payments sp 
            on sp.spym_subs_id = s.subs_id 
            join oims.payment_providers pp 
            on pp.pymp_id = sp.spym_pymp_id
            where s.subs_oper_id = {oper_id}
            group by pp.pymp_name
            order by count(1) desc
            """)
        return self.cur.fetchall()

    def check_login(self,oper_id,login):#check
        result = []
        self.cur.execute(f"""
            select * from oims.subscribers s 
            join oims.subs_generic_history sgh on s.subs_id = sgh.sgnh_subs_id 
            full outer join oims.subs_person_history sph on s.subs_id = sph.sprh_subs_id 
            full outer join oims.subs_organization_history soh on s.subs_id = soh.sorh_subs_id 
            full outer join oims.subs_addresses_history sah on s.subs_id = sah.sadh_subs_id 
            where s.subs_oper_id = {oper_id}
            and sgnh_identity = '{login}'
            and s.subs_sbtp_id = 2
            and sah.sadh_addt_id = 0
            and sgh.sgnh_end_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select * from oims.subscribers s 
            join oims.subs_generic_history sgh on s.subs_id = sgh.sgnh_subs_id 
            full outer join oims.subs_person_history sph on s.subs_id = sph.sprh_subs_id 
            full outer join oims.subs_organization_history soh on s.subs_id = soh.sorh_subs_id 
            full outer join oims.subs_addresses_history sah on s.subs_id = sah.sadh_subs_id 
            where s.subs_oper_id = {oper_id}
            and sgnh_identity = '{login}'
            and s.subs_sbtp_id = 2
            and sah.sadh_addt_id = 3
            and sgh.sgnh_end_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select * from oims.subscribers s 
            join oims.subs_generic_history sgh on s.subs_id = sgh.sgnh_subs_id 
            full outer join oims.subs_person_history sph on s.subs_id = sph.sprh_subs_id 
            full outer join oims.subs_organization_history soh on s.subs_id = soh.sorh_subs_id 
            full outer join oims.subs_addresses_history sah on s.subs_id = sah.sadh_subs_id 
            where s.subs_oper_id = {oper_id}
            and sgnh_identity = '{login}'
            and s.subs_sbtp_id = 2
            and sah.sadh_addt_id = 1
            and sgh.sgnh_end_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select * from oims.subscribers s 
            join oims.subs_generic_history sgh on s.subs_id = sgh.sgnh_subs_id 
            full outer join oims.subs_person_history sph on s.subs_id = sph.sprh_subs_id 
            full outer join oims.subs_organization_history soh on s.subs_id = soh.sorh_subs_id 
            full outer join oims.subs_addresses_history sah on s.subs_id = sah.sadh_subs_id 
            where s.subs_oper_id = {oper_id}
            and sgnh_identity = '{login}'
            and s.subs_sbtp_id = 2
            and sah.sadh_addt_id = 2
            and sgh.sgnh_end_date > now()
            """)
        result.append(self.cur.fetchall())
        #fizik
        self.cur.execute(f"""
            select * from oims.subscribers s 
            join oims.subs_generic_history sgh on s.subs_id = sgh.sgnh_subs_id 
            full outer join oims.subs_person_history sph on s.subs_id = sph.sprh_subs_id 
            full outer join oims.subs_organization_history soh on s.subs_id = soh.sorh_subs_id 
            full outer join oims.subs_addresses_history sah on s.subs_id = sah.sadh_subs_id 
            where s.subs_oper_id = {oper_id}
            and sgnh_identity = '{login}'
            and s.subs_sbtp_id = 1
            and sah.sadh_addt_id = 0
            and sgh.sgnh_end_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select * from oims.subscribers s 
            join oims.subs_generic_history sgh on s.subs_id = sgh.sgnh_subs_id 
            full outer join oims.subs_person_history sph on s.subs_id = sph.sprh_subs_id 
            full outer join oims.subs_organization_history soh on s.subs_id = soh.sorh_subs_id 
            full outer join oims.subs_addresses_history sah on s.subs_id = sah.sadh_subs_id 
            where s.subs_oper_id = {oper_id}
            and sgnh_identity = '{login}'
            and s.subs_sbtp_id = 1
            and sah.sadh_addt_id = 3
            and sgh.sgnh_end_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select * from oims.subscribers s 
            join oims.subs_generic_history sgh on s.subs_id = sgh.sgnh_subs_id 
            full outer join oims.subs_person_history sph on s.subs_id = sph.sprh_subs_id 
            full outer join oims.subs_organization_history soh on s.subs_id = soh.sorh_subs_id 
            full outer join oims.subs_addresses_history sah on s.subs_id = sah.sadh_subs_id 
            where s.subs_oper_id = {oper_id}
            and sgnh_identity = '{login}'
            and s.subs_sbtp_id = 1
            and sah.sadh_addt_id = 1
            and sgh.sgnh_end_date > now()
            """)
        result.append(self.cur.fetchall())
        self.cur.execute(f"""
            select * from oims.subscribers s 
            join oims.subs_generic_history sgh on s.subs_id = sgh.sgnh_subs_id 
            full outer join oims.subs_person_history sph on s.subs_id = sph.sprh_subs_id 
            full outer join oims.subs_organization_history soh on s.subs_id = soh.sorh_subs_id 
            full outer join oims.subs_addresses_history sah on s.subs_id = sah.sadh_subs_id 
            where s.subs_oper_id = {oper_id}
            and sgnh_identity = '{login}'
            and s.subs_sbtp_id = 1
            and sah.sadh_addt_id = 2
            and sgh.sgnh_end_date > now()
            """)
        result.append(self.cur.fetchall())

        return result

class Critical_errors():
    def __init__(self,cur_):
        self.cur = cur_

    def len_phones(self,oper_id):
        self.cur.execute(f"""
            select subs_external_identifier, 
            sgh.sgnh_identity 
            from oims.subscribers s 
            join oims.subs_generic_history sgh 
            on s.subs_id = sgh.sgnh_subs_id 
            join oims.subs_phones_history sph 
            on s.subs_id = sph.sphh_subs_id 
            where (length(sphh_phone) < 2
            or length(sphh_phone) > 32)
            and s.subs_oper_id = {oper_id}
        """)
        return self.cur.fetchall()

class Dictionary():
    def __init__(self,cur_):
        self.cur = cur_
    def operators(self):
        self.cur.execute("select * from oims.operators order by oper_id")
        return self.cur.fetchall()
    
    def telcos(self):
        self.cur.execute("select * from oims.operators order by oper_id")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            status.append('[ OK  ]')
        #    if item[4] == datetime(2037, 1, 1, 0, 0):
        #        status.append('[ OK  ]')
        #    else:
        #        status.append('[ ERR ]')
        return result, status

    def doc_types(self,operator_):
        self.cur.execute("select * from oims.document_types order by doct_id")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[2] >= operator_[3] and item[3] <= operator_[4]:
                status.append('[ OK  ]')
            else:
                status.append('[ ERR ]')

            
          #  if item[2] >= datetime(2000, 1, 1, 0, 0) \
          #      and item[3] <= datetime(2037, 1, 1, 0, 0):
          #      status.append('[ OK  ]')
          #  else:
          #      status.append('[ ERR ]')
        return result, status

    def ip_numbering_plan(self, operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""select * from oims.oper_ip_numbering_plan_history where oinp_oper_id = {oper_id}""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[4] >= operator_[3]\
                and item[5] <= operator_[4]:
                status.append('[ OK  ]')
            else:
                status.append('[ ERR ]')
        return result, status

    def termination_causes(self,operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""select * from oims.termination_causes where terc_oper_id = {oper_id} order by terc_code""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[5] >= operator_[3]\
                and item[6] <= operator_[4]\
                and item[4] != 'Код завершения неизвестен':
                status.append('[ OK  ]')
            elif item[5] >= operator_[3]\
                and item[6] <= operator_[4]\
                and item[4] == 'Код завершения неизвестен':
                status.append('[ WRN ]')
            else:
                status.append('[ ERR ]')
        return result, status

    def call_types(self,operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""select * from oims.call_types where cllt_oper_id = {oper_id} order by cllt_external_identifier""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[4] >= operator_[3]\
                and item[5] <= operator_[4]\
                and item[3] != 'Неизвестный тип вызова':
                status.append('[ OK  ]')
            elif item[4] >= operator_[3]\
                and item[5] <= operator_[4]\
                and item[3] == 'Неизвестный тип вызова':
                status.append('[ WRN ]')
            else:
                status.append('[ ERR ]')
        return result, status

    def ip_data_points(self,operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""select * from oims.oper_ip_data_point_history where odph_oper_id = {oper_id} order by odph_id""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[4] >= operator_[3]\
                and item[5] <= operator_[4]:
                status.append('[ OK  ]')
            else:
                status.append('[ ERR ]')
        return result, status

    def special_numbers(self,operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""select * from oims.special_numbers_history where spcn_oper_id = {oper_id}""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[5] >= operator_[3]\
                and item[6] <= operator_[4]:
                status.append('[ OK  ]')
            else:
                status.append('[ ERR ]')
        return result, status

    def phone_numbering_plan(self, operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""select * from oims.oper_phone_numbering_plan_history where opnp_oper_id = {oper_id} order by opnp_id""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[22] >= operator_[3]\
                and item[23] <= operator_[4]:
                status.append('[ OK  ]')
            else:
                status.append('[ ERR ]')
        return result, status

    def switches(self, operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""select * from oims.switches join oims.switch_types st on st.swtt_id = swit_swtt_id where swit_oper_id = {oper_id} order by swit_id""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[8] >= operator_[3]\
                and item[9] <= operator_[4]:
                status.append('[ OK  ]')
            else:
                status.append('[ ERR ]')
        return result, status

    def gates(self, operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""select opgt_identifier, gttp_name, opgt_description, opgt_address, ogia_address, ogia_port, opgt_start_date,opgt_end_date
                            from oims.oper_gate_history ogh 
                            join oims.opgt_ip_addresses oia 
                            on ogh.opgt_id = oia.ogia_opgt_id 
                            join oims.gate_types gt 
                            on gt.gttp_id = ogh.opgt_gttp_id 
                            where ogh.opgt_oper_id = {oper_id}""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[6] >= operator_[3]\
                and item[7] <= operator_[4]:
                status.append('[ OK  ]')
            else:
                status.append('[ ERR ]')
        return result, status
    
    def supplement_services(self,operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""select * from oims.services where serv_oper_id = {oper_id} order by serv_external_identifier""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[5] >= operator_[3]\
                and item[6] <= operator_[4]:
                status.append('[ OK  ]')
            else:
                status.append('[ ERR ]')
        return result, status


    def bunches_map(self,operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""
            select b1.bnch_external_identifier,b1.bnch_description,
            b2.bnch_external_identifier,b2.bnch_description,
            bnmp_start_date,bnmp_end_date
            from oims.bunches_map bm
            join oims.bunches b1
            on b1.bnch_id = bm.bnmp_bnch_first_id 
            join oims.bunches b2 
            on b2.bnch_id = bm.bnmp_bnch_second_id 
            where b1.bnch_oper_id = {oper_id}""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[4] >= operator_[3]\
                and item[5] <= operator_[4]:
                status.append('[ OK  ]')
            else:
                status.append('[ ERR ]')
        return result, status

    def bunches(self,operator_):
        oper_id = operator_[0]
        self.cur.execute(f"""
            select bnch_external_identifier,
            bnct_name,swit_description,
            bnch_mac_address,bnch_atm_vpi,
            bnch_atm_vci,bnch_description,
            bnch_start_date,bnch_end_date
            from oims.bunches b 
            join oims.switches s 
            on s.swit_id = b.bnch_swit_id 
            join oims.bunch_types bt 
            on bt.bnct_id = b.bnch_bnct_id 
            where bnch_oper_id = {oper_id}
            order by bnch_external_identifier""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            if item[7] >= operator_[3]\
                and item[8] <= operator_[4]:
                status.append('[ OK  ]')
            else:
                status.append('[ ERR ]')
        return result, status

    def pay_types(self, operator_):
        self.cur.execute(f"""select * from oims.payment_providers""")
        result = self.cur.fetchall()
        status = list()
        for item in result:
            status.append('[ OK  ]')
        return result, status






class Export():
    def __init__(self):
        pass

    def list_of_id(self,operators_,list_,file_name_,export_type=0):
        path = operators_[5].replace(' ', '_')
        result = ''
        if not os.path.isdir(path + 'abonents/'):
            os.mkdir(path + 'abonents/')
        if len(list_) != 0:
            file = open(path + 'abonents/'+ file_name_ + '.txt', 'w', encoding="utf-8")
            for l in list_:
                result += (str(l[export_type]) + '\n')
            file.write(result)    
    
    def psi(self, operators_, text_):
        path = operators_[5].replace(' ', '_')
        file = open(path + 'statistics.txt', 'a', encoding="utf-8")
        file.write(str(text_) +  '\n')
        file.close
    
    def dictionary(self, operators_, text_):
        path = operators_[5].replace(' ', '_')
        file = open(path + 'dictionaries.txt', 'a', encoding="utf-8")
        file.write(str(text_) +  '\n')
        file.close
