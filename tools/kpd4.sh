#!/usr/bin/env bash

mail_=$(find /opt/vasexperts/var/dump/oim_data/1/304 -maxdepth 1 -type f -name EMAIL_\*.dump.gz   -mmin -86401 |wc -l)
im_=$(find /opt/vasexperts/var/dump/oim_data/1/306 -maxdepth 1 -type f -name IM_\*.dump.gz      -mmin -86401 |wc -l)
ftp_=$(find /opt/vasexperts/var/dump/oim_data/1/307 -maxdepth 1 -type f -name FTP_\*.dump.gz     -mmin -86401 |wc -l)
term_=$(find /opt/vasexperts/var/dump/oim_data/1/308 -maxdepth 1 -type f -name TERM_\*.dump.gz    -mmin -86401 |wc -l)
raw_=$(find /opt/vasexperts/var/dump/oim_data/1/309 -maxdepth 1 -type f -name RAW_\*.dump.gz     -mmin -86401 |wc -l)
http_=$(find /opt/vasexperts/var/dump/oim_data/1/310 -maxdepth 1 -type f -name HTTP_\*.dump.gz    -mmin -86401 |wc -l)
voip_=$(find /opt/vasexperts/var/dump/oim_data/1/311 -maxdepth 1 -type f -name VOIP_\*.dump.gz    -mmin -86401 |wc -l)
nat_=$(find /opt/vasexperts/var/dump/oim_data/1/312 -maxdepth 1 -type f -name NAT_\*.dump.gz     -mmin -86401 |wc -l)

echo "email: $mail_"
echo "im: $im_"
echo "ftp: $ftp_"
echo "term: $term_"
echo "raw: $raw_"
echo "http: $http_"
echo "voip: $voip_"
echo "nat: $nat_"
