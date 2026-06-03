optional = """and not htrq_client_address <<inet'0.0.0.0/0'"""

from ...logger import logging
from string import Template
from datetime import datetime, timedelta
today = datetime.today()

template_aaa = Template("""
select ${type_}_telco_code, count(*),
count(case when ${type_}_subscriber_id = 'operator' ${optional} then 1 else null end)
from ${partition}
where ${type_}_telco_code in (${telco_codes}) ${scope}
group by ${type_}_telco_code
order by ${type_}_telco_code""")



template_aaa_clients = Template("""
select ${addr_field}, count(1)
from ${partition}
where ${type_}_subscriber_id = 'operator'
and ${type_}_telco_code = ${telco} ${optional} ${scope}
group by ${addr_field}
having count(1) > ${having_count}
order by count(1) desc
""")




template_partitions_old = Template("""
SELECT "partition",parent,range_min,range_max
FROM public.pathman_partition_list
where (parent = 'oimc.http_requests'::regclass
or parent = 'oimc.raw_flows'::regclass
or parent = 'oimc.voip_connections'::regclass
or parent = 'oimc.mail_connections'::regclass
or parent = 'oimc.im_connections'::regclass
or parent = 'oimc.ftp_connections'::regclass
or parent = 'oimc.terminal_connections'::regclass)
and range_min >= '${date_l}'                        
and range_max <= '${date_h}' 
""")

template_partitions_new = Template("""
select rngp_partition,rngp_base_table,rngp_from_value,rngp_to_value
from oimc.range_partitions rp 
where (rngp_base_table = 'oimc.http_requests'::regclass  
or rngp_base_table = 'oimc.raw_flows'::regclass   
or rngp_base_table = 'oimc.voip_connections'::regclass       
or rngp_base_table = 'oimc.mail_connections'::regclass      
or rngp_base_table = 'oimc.im_connections'::regclass       
or rngp_base_table = 'oimc.ftp_connections'::regclass    
or rngp_base_table = 'oimc.terminal_connections'::regclass)                    
and rngp_from_value >= '${date_l}'                     
and rngp_to_value <= '${date_h}' 
""")

type_part = {
     'oimc.raw_flows':              'rawf',
     'oimc.http_requests':          'htrq',
     'oimc.mail_connections':       'emlc',
     'oimc.im_connections':         'imcn',
     'oimc.voip_connections':       'vipc',
     'oimc.terminal_connections':   'trmc',
     'oimc.ftp_connections':        'ftpc'
     }

def subnet_scope(type_,field_,from_oinp_,subnet_list_,oper_ids_):
    # Subnet membership predicate for one address field (client/server).
    # Returns "" when no scoping is configured, else a parenthesised OR of the
    # active sources (oims.oper_ip_numbering_plan_history and/or an explicit list).
    parts = []
    if from_oinp_ == 'True':
        parts.append(f"{type_}_{field_}_address <<= any(select oinp_subnet from oims.oper_ip_numbering_plan_history where oinp_oper_id in ({oper_ids_}))")
    if subnet_list_:
        parts.append(f"{type_}_{field_}_address <<= any (array{subnet_list_}::inet[])")
    return "(" + " or ".join(parts) + ")" if parts else ""

def optional(type_,exclude_,telco_codes_):
    result = ""
    if exclude_[0] != '[]' or exclude_[0] != '':result += f"""\n and not {type_}_client_address <<= any (array{exclude_[0]}::inet[])"""
    if exclude_[1] != '[]' or exclude_[1] != '':result += f"""\n and not {type_}_server_address <<= any (array{exclude_[1]}::inet[])"""
    if exclude_[2] == 'True':result+= f"""\n and not {type_}_client_address <<= any(select oinp_subnet from oims.oper_ip_numbering_plan_history where oinp_oper_id in ({telco_codes_}) and oinp_description ilike '%Служебн%')"""
    if exclude_[2] == 'True':result+= f"""\n and not {type_}_server_address <<= any(select oinp_subnet from oims.oper_ip_numbering_plan_history where oinp_oper_id in ({telco_codes_}) and oinp_description ilike '%Служебн%')"""
    return result

def info():
    ...
def run(cur_,telco_codes_,native_partitions_,range_,exclude_client_address_,exclude_server_address_,threshold_,tmp_files_path_,aaa_exclude_services_subnets_from_ip_numbering_,subnets_from_oper_ip_numbers_only_,subnets_only_from_the_list_):
    oper_ids = ','.join(str(r[0]) for r in telco_codes_)
    scope_active = subnets_from_oper_ip_numbers_only_ == 'True' or bool(subnets_only_from_the_list_)
    results_matrix = []
    if range_ % 24 == 0:
        date_l = (datetime.today() - timedelta(days=range_ // 24)).strftime("%Y-%m-%d 00:00:00")
        date_h = (datetime.today()).strftime("%Y-%m-%d 00:00:00")
    else:
        date_l = (datetime.today() - timedelta(hours=range_)).strftime("%Y-%m-%d %H:%M:%S")
        date_h = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")


    if native_partitions_:template_partitions = template_partitions_new
    else:template_partitions = template_partitions_old

    __select = template_partitions.substitute(date_l=date_l, date_h=date_h)
    logging.debug(f"""SELECT: {__select}""")
    cur_.execute(__select)
    __result = cur_.fetchall()
    logging.debug(f"""RESULT: {__result}""")
    part_list = __result
    logging.info(f"""[AAA] {len(part_list)} partitions found.""")

    for part in part_list:
        type_p = type_part[part[1]]
        if scope_active:
            scope_clause = "and ({0} or {1})".format(
                subnet_scope(type_p,'client',subnets_from_oper_ip_numbers_only_,subnets_only_from_the_list_,oper_ids),
                subnet_scope(type_p,'server',subnets_from_oper_ip_numbers_only_,subnets_only_from_the_list_,oper_ids))
        else:
            scope_clause = ""
        __select = template_aaa.substitute(partition=part[0],
                                           type_=type_p,
                                           telco_codes=','.join(str(r[1]) for r in telco_codes_),
                                           scope=scope_clause,
                                           optional=optional(type_p,
                                                             [exclude_client_address_,exclude_server_address_,aaa_exclude_services_subnets_from_ip_numbering_],
                                                             oper_ids))
        
        logging.debug(f"""SELECT: {__select}""")
        cur_.execute(__select)
        __result = cur_.fetchall()
        results = __result
        logging.debug(f"""RESULT: {__result}""")
        text_for_log = """[AAA] {:<35} """.format(part[0])
        for result in results:
            procent = 100 - result[2] / result[1] * 100
            results_matrix.append([str(result[0]), type_part[part[1]], procent, part[2],part[3],result[1]])
            text_for_log += """{:<10} | """.format(f"""{result[0]}:{procent:.3f}""")

            if procent < threshold_[0]:
                # When subnet scoping is active, dump offending addresses with two
                # separate queries (client- and server-side), mirroring aaachecker.
                fields = ['client','server'] if scope_active else ['client']
                dump_rows = []
                for fld in fields:
                    scope_pred = "and " + subnet_scope(type_p,fld,subnets_from_oper_ip_numbers_only_,subnets_only_from_the_list_,oper_ids) if scope_active else ""
                    __select = template_aaa_clients.substitute(type_=type_p,
                                                           addr_field=f"{type_p}_{fld}_address",
                                                           partition=part[0],
                                                           telco=result[0],
                                                           having_count=result[2]*threshold_[1]/100,
                                                           scope=scope_pred,
                                                           optional=optional(
                                                                type_p,
                                                                [exclude_client_address_,exclude_server_address_,aaa_exclude_services_subnets_from_ip_numbering_],
                                                                oper_ids))
                    logging.debug(f"""SELECT: {__select}""")
                    cur_.execute(__select)
                    __result = cur_.fetchall()
                    logging.debug(f"""RESULT: {__result}""")
                    dump_rows += __result

                file = open(f"""{tmp_files_path_}/aaa_{result[0]}_{type_p}.{today.strftime("%y%m%d_%H%M%S")}.txt""",'a', encoding="utf-8")
                file.write(str(dump_rows).replace('[(','').replace(')]','').replace('), (','\n') +  '\n')
                file.close


        logging.info(text_for_log)



    if not results_matrix:
        logging.error("[AAA] ДАННЫХ ЗА ПЕРИОД НЕТ")
        return []

    for telco in sorted(set(row[0] for row in results_matrix)):
        logging.info("=" * 30 + f": AAA statistics [telco:{telco}]:" + "=" * 30)
        for type_p in sorted(set(row[1] for row in results_matrix if row[0] == telco)):
            data = [row for row in results_matrix if row[0] == telco and row[1] == type_p]
            table_name = next(k for k, v in type_part.items() if v == type_p)
            mean_pct = sum(float(row[2]) for row in data) / len(data)
            logging.info("{:<35} {:<8} {} {} {}".format(
                table_name, f"{mean_pct:.3f}",
                min(row[3] for row in data), max(row[4] for row in data),
                sum(float(row[5]) for row in data)))




    return results_matrix

