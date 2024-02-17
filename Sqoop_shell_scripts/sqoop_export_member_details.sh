#!/bin/sh
# Shell script loading card_transactions data to MySQL!
# Call command - ./sqoop_export_card_txns.sh quickstart.cloudera:3306 project root stg_card_transactions

sqoop export \
--connect jdbc:mysql://quickstart.cloudera:3306/project \
--username root \
--password <> \
--table stg_member_details \
--export-dir project_datasets/card_members.csv \
--verbose \
--fields-terminated-by ',' 
