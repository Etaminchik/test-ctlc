optional = """and not htrq_client_address <<inet'0.0.0.0/0'"""

from ...logger import logging
from string import Template
from datetime import datetime, timedelta
today = datetime.today()

template_logins_wo_telco = Template("""
select ${type_}_telco_code,
count(*),
count(case when 
${type_}_subscriber_id not in 
(select sgh.sgnh_identity 
from oims.subscribers s 
join oims.subs_generic_history sgh 
on s.subs_id = sgh.sgnh_subs_id)
then 1 else null end)
from ${partition}
where ${type_}_telco_code in (${telco_codes}) ${scope}
group by ${type_}_telco_code
order by ${type_}_telco_code
""")

template_logins_w_telco = Template("""
select ${type_}_telco_code,
count(*),
count(case when 
${type_}_subscriber_id not in
(select sgh.sgnh_identity
from oims.subscribers s
join oims.subs_generic_history sgh
on s.subs_id = sgh.sgnh_subs_id
where s.subs_oper_id in (${oper_ids}))
then 1 else null end)
from ${partition}
where ${type_}_telco_code in (${telco_codes}) ${scope}
group by ${type_}_telco_code
order by ${type_}_telco_code
""")

template_logins_list_w_telco = Template("""
select ${type_}_subscriber_id
from ${partition}
where ${type_}_subscriber_id not in
(select sgh.sgnh_identity
from oims.subscribers s
join oims.subs_generic_history sgh
on s.subs_id = sgh.sgnh_subs_id
where s.subs_oper_id = ${oper_id})
and ${type_}_telco_code = ${telco} ${scope}
""")

template_logins_list_wo_telco = Template("""
select ${type_}_subscriber_id
from ${partition}
where ${type_}_subscriber_id not in
(select sgh.sgnh_identity
from oims.subscribers s
join oims.subs_generic_history sgh
on s.subs_id = sgh.sgnh_subs_id)
and ${type_}_telco_code = ${telco} ${scope}
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

def optional(type_,exclude_):
    result = ""
    if exclude_ != '[]' or exclude_ != '':result += f""" and not {type_}_client_address <<= any (array{exclude_}::inet[])"""
    return result

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

def info():
    ...
def run(cur_,telco_codes_,native_partitions_,range_,check_telco_in_generic_history_,tmp_files_path_,subnets_from_oper_ip_numbers_only_,subnets_only_from_the_list_):
    oper_ids = ','.join(str(r[0]) for r in telco_codes_)
    code2id  = {str(r[1]): str(r[0]) for r in telco_codes_}   # telco_code -> oper_id
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

    if check_telco_in_generic_history_ == 'True': template_logins,template_logins_list = template_logins_w_telco,template_logins_list_w_telco
    else: template_logins,template_logins_list = template_logins_wo_telco,template_logins_list_wo_telco

    __select = template_partitions.substitute(date_l=date_l, date_h=date_h)
    logging.debug(f"""SELECT: {__select}""")
    cur_.execute(__select)
    __result = cur_.fetchall()
    logging.debug(f"""RESULT: {__result}""")
    part_list = __result
    logging.info(f"""[LGN] {len(part_list)} partitions found.""")



    for part in part_list:
        type_p = type_part[part[1]]
        if scope_active:
            scope_clause = "and ({0} or {1})".format(
                subnet_scope(type_p,'client',subnets_from_oper_ip_numbers_only_,subnets_only_from_the_list_,oper_ids),
                subnet_scope(type_p,'server',subnets_from_oper_ip_numbers_only_,subnets_only_from_the_list_,oper_ids))
        else:
            scope_clause = ""
        __select = template_logins.substitute(partition=part[0],
                                           type_=type_p,
                                           telco_codes=','.join(str(r[1]) for r in telco_codes_),
                                           oper_ids=oper_ids,
                                           scope=scope_clause)

        logging.debug(f"""SELECT: {__select}""")
        cur_.execute(__select)
        __result = cur_.fetchall()
        results = __result
        logging.debug(f"""RESULT: {__result}""")
        text_for_log = """[LGN] {:<35} """.format(part[0])

        logins_for_file = []
        for result in results:
            procent = 100 - result[2] / result[1] * 100
            results_matrix.append([str(result[0]), type_p, procent, part[2],part[3],result[1]])
            text_for_log += """{:<10} | """.format(f"""{result[0]}:{procent:.3f}""")

            if result[2] != 0:
                # When subnet scoping is active, collect offending logins with two
                # separate queries (client- and server-side), mirroring aaachecker.
                fields = ['client','server'] if scope_active else ['client']
                for fld in fields:
                    scope_pred = "and " + subnet_scope(type_p,fld,subnets_from_oper_ip_numbers_only_,subnets_only_from_the_list_,oper_ids) if scope_active else ""
                    __select = template_logins_list.substitute(type_=type_p,partition=part[0],telco=result[0],oper_id=code2id[str(result[0])],scope=scope_pred)
                    cur_.execute(__select)
                    __result = [item[0] for item in cur_.fetchall()]
                    logins_for_file = list(set(logins_for_file) | set(__result))
                    logging.debug(f"""RESULT: {__result}""")

        logging.info(text_for_log)


    file = open(f"""{tmp_files_path_}/logins_{result[0]}.{today.strftime("%y%m%d_%H%M%S")}.txt""",'a', encoding="utf-8")
    file.write(str(logins_for_file).replace('[(','').replace(')]','').replace('), (','\n') +  '\n')
    file.close


    if not results_matrix:
        logging.error("[LGN] ДАННЫХ ЗА ПЕРИОД НЕТ")
        return []

    for telco in sorted(set(row[0] for row in results_matrix)):
        logging.info("=" * 30 + f": Logins statistics [telco:{telco}]:" + "=" * 30)
        for type_p in sorted(set(row[1] for row in results_matrix if row[0] == telco)):
            data = [row for row in results_matrix if row[0] == telco and row[1] == type_p]
            table_name = next(k for k, v in type_part.items() if v == type_p)
            mean_pct = sum(float(row[2]) for row in data) / len(data)
            logging.info("{:<35} {:<8} {} {} {}".format(
                table_name, f"{mean_pct:.3f}",
                min(row[3] for row in data), max(row[4] for row in data),
                sum(float(row[5]) for row in data)))

    return results_matrix

