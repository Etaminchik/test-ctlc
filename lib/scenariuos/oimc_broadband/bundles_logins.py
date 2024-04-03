optional = """and not htrq_client_address <<inet'0.0.0.0/0'"""

from ...logger import logging
from string import Template
from datetime import datetime, timedelta
import numpy as np

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
where trmc_telco_code in (${telco_codes})
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
where s.subs_oper_id in (${telco_codes}))
then 1 else null end)
from ${partition}
where ${type_}_telco_code in (${telco_codes})
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
where s.subs_oper_id = ${telco})
and ${type_}_telco_code = ${telco}
""")

template_logins_list_wo_telco = Template("""
select ${type_}_subscriber_id 
from ${partition}
where ${type_}_subscriber_id not in 
(select sgh.sgnh_identity 
from oims.subscribers s 
join oims.subs_generic_history sgh 
on s.subs_id = sgh.sgnh_subs_id)
and ${type_}_telco_code = ${telco}
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
--and range_max <= '${date_h}' 
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
--and rngp_to_value <= '${date_h}' 
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

def info():
    ...
def run(cur_,telco_codes_,native_partitions_,range_,check_telco_in_generic_history_,tmp_files_path_):
    telco_codes_ = np.array(telco_codes_)
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
        __select = template_logins.substitute(partition=part[0], 
                                           type_=type_part[part[1]], 
                                           telco_codes=np.array2string(telco_codes_[:,1]).replace('[','').replace(']','').replace(' ',','))
        
        logging.debug(f"""SELECT: {__select}""")
        cur_.execute(__select)
        __result = cur_.fetchall()
        results = __result
        logging.debug(f"""RESULT: {__result}""")
        text_for_log = """[LGN] {:<35} """.format(part[0])

        logins_for_file = []
        for result in results:
            procent = 100 - result[2] / result[1] * 100
            results_matrix.append([str(result[0]), type_part[part[1]], procent, part[2],part[3],result[1]])
            text_for_log += """{:<10} | """.format(f"""{result[0]}:{procent:.3f}""")

            if result[2] != 0:
                __select = template_logins_list.substitute(type_=type_part[part[1]],partition=part[0],telco=result[0])
                cur_.execute(__select)
                __result = cur_.fetchall()
                results = __result
                logging.debug(f"""RESULT: {__result}""")
                if __result[0] not in logins_for_file:
                    logins_for_file.append(__result[0])

        logging.info(text_for_log)


    file = open(f"""{tmp_files_path_}/logins.{today.strftime("%y%m%d_%H%M%S")}.txt""",'a', encoding="utf-8")
    file.write(str(logins_for_file).replace('[(','').replace(')]','').replace('), (','\n') +  '\n')
    file.close


    results_matrix_np = np.array(results_matrix)

            
    for telco in (np.unique(results_matrix_np[:,0])):
        logging.info("="*30 +f": Logins statistics [telco:{telco}]:"+"="*30)
         

        for type_p in np.unique(results_matrix_np[results_matrix_np[:,0] == telco][:,1]):
            data = results_matrix_np[(results_matrix_np[:,0] == telco) & (results_matrix_np[:,1] == type_p)]
            logging.info("{:<35} {:<8} {} {} {}".format(list(filter(lambda x: type_part[x] == data[:,1][0], type_part))[0], 
                                               f"{np.mean(np.asarray(data[:,2], dtype=float)):.3f}",
                                                np.min(np.asarray(data[:,3],dtype=np.datetime64)),
                                                np.max(np.asarray(data[:,4],dtype=np.datetime64)),
                                                np.sum(np.asarray(data[:,5], dtype=float))))

    return results_matrix

