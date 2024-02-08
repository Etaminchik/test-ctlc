# -*- coding: utf-8 -*-

from .logger import logging
import psycopg2
import json
import time
import os
import ast

from .scenariuos import general
from .scenariuos.oimc_broadband import bundles_aaa
from .scenariuos.oimc_broadband import bundles_nat
from .scenariuos.oimc_broadband import bundles_logins


def run(config_):
    telco_codes     = json.loads(config_['main']['telco_codes'])
    log_path        = config_['main']['log_path']
    if not os.path.isdir(log_path):os.mkdir(log_path)
    tmp_files_path  = config_['main']['tmp_files_path']
    if not os.path.isdir(tmp_files_path):os.mkdir(tmp_files_path)
#    ingener_mode    = config_['main']['ingener_mode']

    db_host         = config_['database']['host']
    db_port         = config_['database']['port']
    db_user         = config_['database']['user']
    db_password     = config_['database']['password']
    db_name         = config_['database']['database']

    nat_check       = config_['scenarious']['nat_check']
    aaa_check       = config_['scenarious']['aaa_check']
    logins_check    = config_['scenarious']['logins_check']

    nat_range_hours                                 = int(config_['NAT']['nat_range_hours'])
    nat_exclude_client_address                      = config_['NAT']['nat_exclude_client_address']
    nat_exclude_server_address                      = config_['NAT']['nat_exclude_server_address']
    nat_exclude_dict_ip_numbering                   = config_['NAT']['nat_exclude_dict_ip_numbering']
    nat_threshold_for_analysis_as_percentage        = int(config_['NAT']['nat_threshold_for_analysis_as_percentage'])
    nat_threshold_for_error_sampling_as_percentage  = int(config_['NAT']['nat_threshold_for_error_sampling_as_percentage'])

    aaa_range_hours                                 = int(config_['AAA']['aaa_range_hours'])
    aaa_exclude_client_address                      = ast.literal_eval(config_['AAA']['aaa_exclude_client_address'])
    aaa_exclude_server_address                      = ast.literal_eval(config_['AAA']['aaa_exclude_server_address'])
    aaa_threshold_for_analysis_as_percentage        = int(config_['AAA']['aaa_threshold_for_analysis_as_percentage'])
    aaa_threshold_for_error_sampling_as_percentage  = int(config_['AAA']['aaa_threshold_for_error_sampling_as_percentage'])

    logins_range_hours = int(config_['logins']['logins_range_hours'])
    logins_check_telco_in_generic_history = config_['logins']['logins_check_telco_in_generic_history']


    


    scenarios = []
    matrix_nat=matrix_aaa=matrix_logins = []
    emulation_id = int(time.time())
    logging.info("{0} {1:08x} Starting  {0}".format("=" * 35, emulation_id))
    logging.info(f""">>>>>>> Checking the file configuration""")
    
    if nat_check == 'True':
        scenarios.append('check_nat')
        logging.info(f"""[NAT] A check of the NAT bundle is planned. Range: {nat_range_hours} hours.""")
        logging.info(f"""[NAT] Excludes: client cidr: {nat_exclude_client_address}, server cidr: {nat_exclude_server_address}, oims.oper_ip_numbering_plan_history from server cidr: {nat_exclude_dict_ip_numbering}.""")

    if aaa_check == 'True':
        scenarios.append('check_aaa')
        logging.info(f"""[AAA] A check of the AAA bundle is planned. Range: {aaa_range_hours} hours.""")
        logging.info(f"""[AAA] Excludes: client cidr: {aaa_exclude_client_address}.""")
    
    if logins_check == 'True':
        scenarios.append('check_logins')
        logging.info(f"""[LGN] A check of the logins bundle is planned. Range: {logins_range_hours} hours.""")

    logging.info("{0} {1:08x} Finishing {0}".format("=" * 35, emulation_id))
    logging.info("Use to cut out log records: sed -n '/{0:08x} Starting/,/{0:08x} Finishing/p' {1}".format(emulation_id, log_path+'/test-ctlc.txt'))
    print()

    emulation_id = int(time.time())
    logging.info("{0} {1:08x} Starting  {0}".format("=" * 35, emulation_id))
    logging.info(f""">>>>>>> Checking general settings""")

    try:
        con = psycopg2.connect(
        database    = db_name, 
        user        = db_user, 
        password    = db_password, 
        host        = db_host, 
        port        = db_port)
        cur = con.cursor()  
        logging.info(f"""Successful database login: {db_host}:{db_port}/{db_name}""")
    except psycopg2.OperationalError as e:
        print(f"Error{e}")
        return
    operators = general.check_telco_codes(cur, telco_codes)
    logging.info(f"""{len(operators)} telco codes found""")
    schema_version = general.checking_schema_version(cur)
    if schema_version >= 8:
        logging.info(f"""Schema version {schema_version}. There are native partitions.""")
        native_partitions = True
    else:
        logging.info(f"""Schema version {schema_version}. There are no native partitions.""")
        native_partitions = False

    logging.info("{0} {1:08x} Finishing {0}".format("=" * 35, emulation_id))
    logging.info("Use to cut out log records: sed -n '/{0:08x} Starting/,/{0:08x} Finishing/p' {1}".format(emulation_id, log_path+'/test-ctlc.txt'))
    print()

    if 'check_nat' in scenarios:
        emulation_id = int(time.time())
        logging.info("{0} {1:08x} Starting  {0}".format("=" * 35, emulation_id))
        logging.info(f""">>>>>>> Checking the percentage of the NAT bundle""")

        matrix_nat = bundles_nat.run(cur,
                        operators,
                        native_partitions,
                        nat_range_hours,
                        nat_exclude_dict_ip_numbering,
                        nat_exclude_client_address,
                        nat_exclude_server_address,
                        [nat_threshold_for_analysis_as_percentage,nat_threshold_for_error_sampling_as_percentage],
                        tmp_files_path
                        )

        logging.info("{0} {1:08x} Finishing {0}".format("=" * 35, emulation_id))
        logging.info("Use to cut out log records: sed -n '/{0:08x} Starting/,/{0:08x} Finishing/p' {1}".format(emulation_id, log_path+'/test-ctlc.txt'))
        print()

    if 'check_aaa' in scenarios:
        emulation_id = int(time.time())
        logging.info("{0} {1:08x} Starting  {0}".format("=" * 35, emulation_id))
        logging.info(f""">>>>>>> Checking the percentage of the AAA bundle""")

        matrix_aaa = bundles_aaa.run(cur,
                        operators,
                        native_partitions,
                        aaa_range_hours,
                        aaa_exclude_client_address,
                        aaa_exclude_server_address,
                        [aaa_threshold_for_analysis_as_percentage,aaa_threshold_for_error_sampling_as_percentage],
                        tmp_files_path)

        logging.info("{0} {1:08x} Finishing {0}".format("=" * 35, emulation_id))
        logging.info("Use to cut out log records: sed -n '/{0:08x} Starting/,/{0:08x} Finishing/p' {1}".format(emulation_id, log_path+'/test-ctlc.txt'))
        print()

    if 'check_logins' in scenarios:
        emulation_id = int(time.time())
        logging.info("{0} {1:08x} Starting  {0}".format("=" * 35, emulation_id))
        logging.info(f""">>>>>>> Checking the percentage of the logins bundle""")

        matrix_logins = bundles_logins.run(cur,
                        operators,
                        native_partitions,
                        logins_range_hours,
                        logins_check_telco_in_generic_history,
                        tmp_files_path
                        )

        logging.info("{0} {1:08x} Finishing {0}".format("=" * 35, emulation_id))
        logging.info("Use to cut out log records: sed -n '/{0:08x} Starting/,/{0:08x} Finishing/p' {1}".format(emulation_id, log_path+'/test-ctlc.txt'))
        print()


    emulation_id = int(time.time())
    logging.info("{0} {1:08x} Starting  {0}".format("=" * 35, emulation_id))
    logging.info(f""">>>>>>> Checking the percentage of the logins bundle""")
    general.combining_bundles_reports(operators,matrix_nat,matrix_aaa,matrix_logins)
    logging.info("{0} {1:08x} Finishing {0}".format("=" * 35, emulation_id))
    logging.info("Use to cut out log records: sed -n '/{0:08x} Starting/,/{0:08x} Finishing/p' {1}".format(emulation_id, log_path+'/test-ctlc.txt'))
    print()
