Connect to mysql
mysql -u root -p
Password :

> mysql 

create database project;


use project;

create table stg_card_transactions 
(card_id bigint,
member_id bigint,
amount int,
postcode int,
pos_id bigint,
transaction_dt varchar(255),
status varchar(50)
);

Alter ignore table stg_card_transactions add unique index idx_card_txns(card_id,transaction_dt);

create table card_transactions 
(card_id bigint,
member_id bigint,
amount int,
postcode int,
pos_id bigint,
transaction_dt datetime,
status varchar(50),
PRIMARY KEY(card_id, transaction_dt)
);

insert into card_transactions
select card_id, member_id, amount, postcode, pos_id,
str_to_date(transaction_dt, '%d-%m-%Y%H:%i:%s'),
status
from stg_card_transactions
;

commit;

>hive

SET HIVE.ENFORCE.BUCKETING=TRUE;


create external table if not exists member_score
(
    member_id bigint,
    score float
)
row format delimited fields terminated by ','
stored as textfile
location '/project_datasets/member_score/';

load data inpath 'project_datasets/member_score/member_score.csv' overwrite into table member_score;

# running the load command moves the csv files from HDFS location

create external table if not exists member_details
(
    card_id bigint,
    member_id bigint,
    member_joining_dt string ,
    card_purchase_dt string ,
    country string,
    city string,
    score float
)
row format delimited fields terminated by ','
stored as textfile
location '/project_datasets/member_detail/';

load data inpath 'project_datasets/member_detail/card_members.csv' overwrite into table member_details;


--Member score bucketed table(8 buckets)
create table if not exists member_score_bucketed
(
    member_id bigint,
    score float
)
CLUSTERED BY (member_id) into 8 buckets;

--Member details bucketed table(8 buckets)

create table if not exists member_details_bucketed
(
    card_id bigint,
    member_id bigint,
    member_joining_dt string ,
    card_purchase_dt string ,
    country string,
    city string,
    score float
)
CLUSTERED BY (card_id) into 8 buckets;

Hive-Hbase card_transactions table creation : External & Bucketed tables

create external table if not exists card_transactions 
(
    card_id bigint,
    member_id bigint,
    amount float,
    postcode int,
    pos_id bigint,
    transaction_dt string,
    status string
)row format delimited fields terminated by ','
stored as textfile
location '/project_datasets/card_transactions/';

load data inpath 'project_datasets/card_transactions/card_transactions.csv' overwrite into table card_transactions;


Select count(*) from member_score;
select count(*) from member_details;

create table card_transactions_bucketed
(
    cardid_txnts string, 
    card_id bigint, 
    member_id bigint,
    amount float,
    postcode int,
    pos_id bigint,
    transaction_dt string,
    status string
)
CLUSTERED by (card_id) into 8 buckets
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH
SERDEPROPERTIES("hbase.columns.mapping"=":key,trans_data:card_id,trans_data:member_id,trans_data:amount,trans_data:postcode,trans_data:pos_id,trans_data:transaction_dt,trans_data:status") 
TBLPROPERTIES ("hbase.table.name" = "card_transactions");

insert into table card_transactions_bucketed
select concat_ws('~',cast(card_id as string),cast(transaction_dt as string)) as cardid_txnts,
        card_id,
        member_id,
        amount,
        postcode,
        pos_id,
        transaction_dt,
        status
from card_transactions;


create table card_lookup
(
    member_id bigint,
    card_id bigint ,
    ucl float ,
    score float,
    last_txn_time timestamp,
    last_txn_zip string
)
CLUSTERED by (card_id) into 8 buckets
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH
SERDEPROPERTIES("hbase.columns.mapping"=":key,lkp_data:member_id,lkp_data:ucl,lkp_data:score, lkp_data:last_txn_time,lkp_data:last_txn_zip")
TBLPROPERTIES ("hbase.table.name" = "card_lookup");

loading data into hive bucketed tables

insert into table member_score_bucketed
select * from member_score;

insert into table member_details_bucketed
select * from member_details;

