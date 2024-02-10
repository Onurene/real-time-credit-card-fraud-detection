#!/bin/sh
# Shell script loading card_transactions data to MySQL!
# Call command - ./sqoop_export_card_txns.sh quickstart.cloudera:3306 fraud_detection root card_transactions

sqoop export \
--connect jdbc:mysql://quickstart.cloudera:3306/fraud_detection \
--username root \
--password <> \
--table card_transactions \
--staging-table stg_card_transactions \
--export-dir /user/cloudera/fraud_detection/card_transactions.csv \
--verbose \
--fields-terminated-by ',' 
