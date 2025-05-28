#launch in the cron once every 3 hours 
#0 */3 * * * /bin/python3 /opt/vasexperts/etc/test-ctlc/aaachecker.py > /dev/null 2>&1

REPORT_PATH = './'
INTERVAL_IN_SEC = 60*60*3 #3 hours

SUBNETS = [
    "195.128.120.0/21",
    "45.143.136.0/22",
    "46.173.208.0/20",
    "91.203.192.0/22",
    "141.101.245.0/24",
    "195.22.148.0/23"
]


import psycopg2
from datetime import datetime, timedelta
from collections import Counter

conn = psycopg2.connect(
    dbname="vasexperts",
    user="oim_admin",
    password="admin",
    host="localhost",
    port="54321")

subnet_conditions = (" or ".join([f"t.%(tbl)s_%(type)s_address << inet '{subnet}'\n" for subnet in SUBNETS]))


pattern_get_addresses = f"""
WITH 
service_numbers AS (
  SELECT oinp_subnet 
  FROM oims.oper_ip_numbering_plan_history
  WHERE oinp_description ILIKE '%%Служебн%%'
  GROUP BY oinp_subnet 
)
SELECT t.%(tbl)s_%(type)s_address, count(1)
FROM %(table)s t
WHERE t.%(tbl)s_subscriber_id = 'operator'
and t.%(tbl)s_begin_connection_time > '%(date)s'
AND NOT EXISTS (SELECT 1 FROM service_numbers WHERE oinp_subnet = t.%(tbl)s_client_address)
AND NOT EXISTS (SELECT 1 FROM service_numbers WHERE oinp_subnet = t.%(tbl)s_server_address)
and ({subnet_conditions})
group by t.%(tbl)s_%(type)s_address
order by count(1) desc
"""

pattern_get_count_operator = """
WITH 
service_numbers AS (
  SELECT oinp_subnet 
  FROM oims.oper_ip_numbering_plan_history
  WHERE oinp_description ILIKE '%%Служебн%%'
  GROUP BY oinp_subnet
)
SELECT COUNT(*)
FROM %(table)s t
WHERE t.%(tbl)s_subscriber_id = 'operator'
and t.%(tbl)s_begin_connection_time > '%(date)s'
AND NOT EXISTS (SELECT 1 FROM service_numbers WHERE oinp_subnet = t.%(tbl)s_client_address)
AND NOT EXISTS (SELECT 1 FROM service_numbers WHERE oinp_subnet = t.%(tbl)s_server_address)
"""

pattern_get_count_all = """
select count(*) FROM %(table)s t
where t.%(tbl)s_begin_connection_time > '%(date)s'
"""

types = {
    'RAW FLOWS(HTTPS)':         ['oimc.raw_flows',              'rawf'],
    'HTTP':                     ['oimc.http_requests',          'htrq'],
    'MAIL':                     ['oimc.mail_connections',       'emlc'],
    'VoIP':                     ['oimc.voip_connections',       'vipc'],
    'IM message':               ['oimc.im_connections',         'imcn'],
    'FTP session':              ['oimc.ftp_connections',        'ftpc'],
    'Terminal connection':      ['oimc.terminal_connections',   'trmc']
}

class AAAChecker:
    
    def __init__(self):
        self.path = REPORT_PATH
        self.interval = INTERVAL_IN_SEC
       
    def create_file(self):
        file_name = f"""aaa_report.{datetime.now().strftime("%y%m%d_%H%M%S")}.txt"""
        print(f"File created: {self.path + file_name}")
        return self.path + file_name

    def create_select(self,pattern_,table_,tbl_,type_=None):
        date_ = (datetime.now() - timedelta(seconds=self.interval)).strftime("%Y-%m-%d %H:%M:%S")
        select = pattern_ % {"table": table_, "tbl":tbl_, "type": type_, "date": date_}
        return select

    def script(self):   
        addresses = {}
        text_for_file = "Checking the connection with logins\n\n=============================================\n"
        for name,type in types.items():
            with conn.cursor() as cur:

                sql = self.create_select(pattern_   = pattern_get_count_operator,
                                         table_     = type[0],
                                         tbl_       = type[1])
                cur.execute(sql)
                count_operator = cur.fetchall()[0][0]

                sql = self.create_select(pattern_   = pattern_get_count_all,
                                         table_     = type[0],
                                         tbl_       = type[1])
                cur.execute(sql)
                count_all = cur.fetchall()[0][0]
                bundle = 100 - count_operator/count_all*100
                text_for_file += f"{name:<25}: {bundle:>8.2f}%\n"

                if bundle < 95:
                    sql = self.create_select(pattern_   = pattern_get_addresses,
                                             table_     = type[0],
                                             tbl_       = type[1],
                                             type_      ='client')
                    cur.execute(sql)
                    result = {ip: count for ip, count in cur.fetchall()}
                    addresses = Counter(addresses) + Counter(result)
                    sql = self.create_select(pattern_   = pattern_get_addresses,
                                             table_     = type[0],
                                             tbl_       = type[1],
                                             type_      ='server')
                    cur.execute(sql)
                    result = {ip: count for ip, count in cur.fetchall()}
                    addresses = Counter(addresses) + Counter(result)
        print(text_for_file)
        text_for_file += "=============================================\n\nUnrelated addresses:\n"
        for ip, count in addresses.most_common():
            text_for_file += f"{ip}: {count}\n"
        
        with open(self.create_file(), "w", encoding="utf-8") as file:
            file.write(text_for_file)
        
                


if SUBNETS:    
    AAAChecker().script()
else:
    print('Fill the SUBNETS array with the operator's subnets')
