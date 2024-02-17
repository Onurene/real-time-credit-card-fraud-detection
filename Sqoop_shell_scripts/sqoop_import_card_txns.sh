#!/bin/sh
# Shell script loading card_transactions data from MySQL to Hive!
# Call command - ./sqoop_import_card_txns.sh quickstart.cloudera:3306 bigdataproject root card_transactions

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/project \
--username root \
--password <> \
--table transactions \
--target-dir /user/cloudera/project_datasets/card_transactions/ \
--delete-target-dir
