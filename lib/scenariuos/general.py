from ..logger import logging
import numpy as np


def info():
    ...


def check_telco_codes(cur_, telco_codes_: list):
    #check telco_codes
    if len(telco_codes_) == 0:
        __select = f"""select * from oims.operators order by oper_telco_code"""
        logging.debug(f"""SELECT: {__select}""")
        cur_.execute(__select)
        __result = cur_.fetchall()
        logging.debug(f"""RESULT: {__result}""")
    else:
        __select = f"""select * from oims.operators where oper_telco_code in ({','.join(str(x) for x in telco_codes_)}) order by oper_telco_code """
        logging.debug(f"""SELECT: {__select}""")
        cur_.execute(__select)
        __result = cur_.fetchall()
        logging.debug(f"""RESULT: {__result}""")
    for i in range(len(__result)):
        __result[i] = list(__result[i])
    return __result


def checking_schema_version(cur_):
    __select = f"""select * from oimm.component_versions order by cmvr_name"""
    logging.debug(f"""SELECT: {__select}""")
    cur_.execute(__select)
    __result = cur_.fetchall()
    logging.debug(f"""RESULT: {__result}""")
    return int(''.join(__result[0][1].split('.')[0]))




def combining_bundles_reports(operators_,matrix_nat_,matrix_aaa_,matrix_logins_):
    telco_codes = np.array(operators_)
    matrix_nat_np       = np.array(matrix_nat_)
    matrix_aaa_np       = np.array(matrix_aaa_)
    matrix_logins_np    = np.array(matrix_logins_)
    type_part = {
         'rawf'     :'Передача данных (закрытые протоколы)',
         'htrq'     :'Интернет-посещения HTTP',
         'emlc'     :'Email-сообщения',
         'imcn'     :'IM-сообщения',
         'vipc'     :'VoIP-соединения',
         'trmc'     :'Терминальный доступ',
         'ftpc'     :'FTP-соединения'                           
         }

    for telco in telco_codes:
        logging.info(f"""Statistics: {telco[2]}, telco: {telco[1]}""")
        logging.info("{:<40} {:<9} {:<11} {:<9} {:<11} {:<9} {:<11}".format('Type','NAT', 'NAT count','AAA','AAA count','Logins','Logins count'))
        nat=nat_count=aaa=aaa_count=logins=logins_count=0
        for type_p in type_part:
            if matrix_nat_np.size > 0:
                data_nat    = matrix_nat_np[(matrix_nat_np[:,0] == str(telco[1])) & (matrix_nat_np[:,1] == type_p)]

                if data_nat.size > 0:
                    nat = f"{np.mean(data_nat[:,2].astype(float), dtype=float):.3f}"
                    nat_count = np.sum((data_nat[:,5]).astype(int))
                else: nat=nat_count= 0

            if matrix_aaa_np.size > 0:
                data_aaa    = matrix_aaa_np[(matrix_aaa_np[:,0] == str(telco[1])) & (matrix_aaa_np[:,1] == type_p)]
                if data_aaa.size > 0:
                    aaa = f"{np.mean(data_aaa[:,2].astype(float), dtype=float):.3f}"
                    aaa_count = np.sum((data_aaa[:,5]).astype(int))
                else: aaa=aaa_count= 0

            if matrix_logins_np.size > 0:
                data_logins = matrix_logins_np[(matrix_logins_np[:,0] == str(telco[1])) & (matrix_logins_np[:,1] == type_p)]
                if data_logins.size > 0:
                    logins = f"{np.mean(data_logins[:,2].astype(float), dtype=float):.3f}"
                    logins_count = np.sum((data_logins[:,5]).astype(int))
                else: logins=logins_count= 0


            logging.info("{:<40} {:<7} {:<9} {:<7} {:<9} {:<7} {:<9}".format(type_part[type_p],nat,nat_count,aaa,aaa_count,logins,logins_count))
        logging.info("="*80)


