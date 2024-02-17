#!/bin/sh
# Shell script loading card_transactions data from MySQL to Hive!
# Call command - ./sqoop_import_card_txns.sh quickstart.cloudera:3306 bigdataproject root card_transactions

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/fraud_detection \
--username root \
--password cloudera \
--table transactions \
--target-dir /user/cloudera/fraud_detection/transactions \
--delete-target-dir
