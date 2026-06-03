from ..logger import logging


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




def _filter_matrix(matrix, telco, type_p):
    return [row for row in matrix if str(row[0]) == str(telco) and row[1] == type_p]


def _mean_pct(rows):
    vals = [float(row[2]) for row in rows]
    return f"{sum(vals) / len(vals):.3f}"


def _sum_count(rows):
    return sum(int(row[5]) for row in rows)


def combining_bundles_reports(operators_, matrix_nat_, matrix_aaa_, matrix_logins_):
    type_part = {
        'rawf': 'Передача данных (закрытые протоколы)',
        'htrq': 'Интернет-посещения HTTP',
        'emlc': 'Email-сообщения',
        'imcn': 'IM-сообщения',
        'vipc': 'VoIP-соединения',
        'trmc': 'Терминальный доступ',
        'ftpc': 'FTP-соединения',
    }

    for telco in operators_:
        logging.info(f"""Statistics: {telco[2]}, telco: {telco[1]}""")
        logging.info("{:<40} {:<10} {:<15} {:<10} {:<15} {:<10} {:<15}".format('Type', 'NAT', 'NAT count', 'AAA', 'AAA count', 'Logins', 'Logins count'))
        for type_p in type_part:
            nat = nat_count = aaa = aaa_count = logins = logins_count = 0

            if matrix_nat_:
                data_nat = _filter_matrix(matrix_nat_, telco[1], type_p)
                if data_nat:
                    nat = _mean_pct(data_nat)
                    nat_count = _sum_count(data_nat)

            if matrix_aaa_:
                data_aaa = _filter_matrix(matrix_aaa_, telco[1], type_p)
                if data_aaa:
                    aaa = _mean_pct(data_aaa)
                    aaa_count = _sum_count(data_aaa)

            if matrix_logins_:
                data_logins = _filter_matrix(matrix_logins_, telco[1], type_p)
                if data_logins:
                    logins = _mean_pct(data_logins)
                    logins_count = _sum_count(data_logins)

            logging.info("{:<40} {:<10} {:<15} {:<10} {:<15} {:<10} {:<15}".format(type_part[type_p], nat, nat_count, aaa, aaa_count, logins, logins_count))
        logging.info("=" * 80)


