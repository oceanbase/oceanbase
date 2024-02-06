#!/bin/bash -x

HOST=${1:-11.166.82.163}
PORT=${2:-46905}
RPCPORT=${3:-46904}

# HOST=${1:-11.158.97.240}
# PORT=${2:-41101}
# RPCPORT=${3:-41100}

THREAD=500
ROWS=10000
IO_THREAD=100
VAL_LEN=1024
user_name=root
tenant_name=sys
table_name=batch_execute_test
passwd=''
user="$user_name@$tenant_name"
db=test
# ~/myWorkspace/dooba/dooba -d -h 100.88.11.96 -P 50803 -u root@sys --table-api=1
echo run test...
rm -f libobtable.log
# table api
mysql -h $HOST -P $PORT -u $user -e "alter system set _enable_defensive_check = true;"
mysql -h $HOST -P $PORT -u $user -e "drop table if exists batch_execute_test; create table if not exists batch_execute_test (C1 bigint primary key, C2 bigint, C3 varchar(100)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists complex_batch_execute_test; create table if not exists complex_batch_execute_test (C1 bigint primary key, C2 bigint, C3 varchar(100)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists all_single_operation_test; create table if not exists all_single_operation_test (C1 bigint primary key, C2 bigint, C3 varchar(100)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists type_check_test; create table type_check_test (pk1 bigint, pk2 varchar(10), ctinyint tinyint, csmallint smallint, cmediumint mediumint, cint int, cbigint bigint, utinyint tinyint unsigned, usmallint smallint unsigned, umediumint mediumint unsigned, uint int unsigned, ubigint bigint unsigned, cfloat float, cdouble double, ufloat float unsigned, udouble double unsigned, cnumber decimal(10, 2), unumber decimal(10,2) unsigned, cvarchar varchar(10), cchar char(10), cbinary binary(10), cvarbinary varbinary(10), ctimestamp timestamp, cdatetime datetime, cyear year, cdate date, ctime time, ctext text, cblob blob, cbit bit(64), cnotnull bigint not null default 111, PRIMARY KEY(pk1, pk2));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists column_default_value; create table column_default_value (C1 bigint primary key, C2 bigint default 1, C3 varchar(100) default 'abc') PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists partial_update_test; create table if not exists partial_update_test (C1 bigint primary key, C2 bigint, C3 varchar(100)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists append_lob_test; create table if not exists append_lob_test (C1 bigint primary key, C2 bigint, C3 mediumtext)" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists all_lob_test; create table if not exists all_lob_test (C1 bigint primary key, C2 bigint, C3 mediumtext)" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists large_scan_test; create table if not exists large_scan_test (C1 bigint primary key, C2 bigint, C3 varchar(100))" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists uniq_replace_test; create table if not exists uniq_replace_test (C1 bigint primary key, C2 bigint, C3 varchar(100), unique key C2_uniq(C2))" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists varchar_rowkey_update_test; create table if not exists varchar_rowkey_update_test (K varchar(512) primary key, T bigint, KEY idx_T(T) LOCAL);" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_increment_test; create table if not exists multi_increment_test (C1 bigint primary key, C2 bigint, C3 varchar(100)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_increment_test; create table if not exists single_increment_test (C1 bigint primary key, C2 bigint, C3 varchar(100)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_get_test; create table if not exists single_get_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world') PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_get_test; create table if not exists multi_get_test (C1 bigint primary key, C2 bigint, C3 varchar(100) default 'hello world') PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_insert_test; create table if not exists single_insert_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world') PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists insert_generate_test; create table if not exists insert_generate_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world', C3_PREFIX varchar(10) GENERATED ALWAYS AS (substr(C3,1,2))) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_update_test; create table if not exists single_update_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world') PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_update_test; create table if not exists multi_update_test (C1 bigint primary key, C2 bigint, C3 varchar(100) default 'hello world') PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists update_generate_test; create table if not exists update_generate_test (C1 bigint primary key, C2 varchar(100), C3 varchar(100), GEN varchar(100) GENERATED ALWAYS AS (concat(C2,c3)) stored) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_delete_test; create table if not exists single_delete_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world') PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_replace_test; create table if not exists single_replace_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world') PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists replace_unique_key_test; create table if not exists replace_unique_key_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world', unique index i1(c2) local)" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_replace_test; create table if not exists multi_replace_test (C1 bigint primary key, C2 bigint, C3 varchar(100) default 'hello world', unique index i1(c2) local)" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_insert_up_test; create table if not exists single_insert_up_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world', UNIQUE KEY idx_c2 (C2))" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_insert_or_update_test; create table if not exists multi_insert_or_update_test (C1 bigint primary key, C2 bigint, C3 varchar(100) default 'hello world') PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists kv_query_test; create table if not exists kv_query_test (C1 bigint, C2 bigint, C3 bigint, PRIMARY KEY(C1, C2), KEY idx_c2 (C2), KEY idx_c3 (C3), KEY idx_c2c3(C2, C3));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists virtual_generate_col_test; create table if not exists virtual_generate_col_test (C1 bigint primary key, C2 bigint, C3 varchar(100), C3_PREFIX varchar(10) GENERATED ALWAYS AS (substr(C3,1,2)))" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists store_generate_col_test; create table if not exists store_generate_col_test (C1 bigint primary key, C2 varchar(10), C3 varchar(10), GEN varchar(30) generated always as (concat(C2,C3)) stored)" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists check_scan_range_test; create table if not exists check_scan_range_test (C1 bigint, C2 varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, C3 bigint, PRIMARY KEY(C1, C2), KEY idx_c3 (C3));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_insert_test; create table if not exists multi_insert_test (C1 bigint primary key, C2 bigint, C3 varchar(100)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_delete_test; create table if not exists multi_delete_test (C1 bigint primary key, C2 bigint, C3 varchar(100)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists query_sync_multi_batch_test; create table if not exists query_sync_multi_batch_test (PK1 bigint, PK2 bigint, C1 bigint, C2 varchar(100), C3 bigint, PRIMARY KEY(PK1, PK2), INDEX idx1(C1, C2));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists large_scan_query_sync_test; create table if not exists large_scan_query_sync_test (C1 bigint primary key, C2 bigint, C3 varchar(100));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists query_sync_with_index_test; create table if not exists query_sync_with_index_test (C1 bigint, C2 bigint, C3 bigint, primary key(C1, C2), KEY idx_c2 (C2), KEY idx_c3 (C3), KEY idx_c2c3(C2, C3));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists query_sync_multi_task_test; create table if not exists query_sync_multi_task_test (C1 bigint primary key, C2 bigint, C3 varchar(100));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists query_with_filter; create table if not exists query_with_filter (C1 bigint primary key, C2 bigint, C3 varchar(100), C4 double default 0);" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists query_and_mutate; create table if not exists query_and_mutate (C1 bigint primary key, C2 bigint, C3 varchar(100), C4 double default 0);" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists atomic_batch_ops; create table if not exists atomic_batch_ops (C1 bigint, C2 varchar(128), C3 varbinary(1024) default null, C4 bigint not null default -1, primary key(C1), UNIQUE KEY idx_c2c4 (C2, C4));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists auto_increment_defensive_test; create table if not exists auto_increment_defensive_test (C1 bigint AUTO_INCREMENT primary key) PARTITION BY KEY(C1) PARTITIONS 16;" $db

# INDEX idx1(C1, C2)
mysql -h $HOST -P $PORT -u $user -e "drop table if exists execute_query_test; create table if not exists execute_query_test (PK1 bigint, PK2 bigint, C1 bigint, C2 varchar(100), C3 bigint, PRIMARY KEY(PK1, PK2));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists secondary_index_test; create table if not exists secondary_index_test (C1 bigint primary key, C2 bigint, C3 varchar(100), index i1(c2) local, index i2(c3) local, index i3(c2, c3) local)" $db
# hbase api
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1; create table if not exists htable1_cf1 (K varbinary(1024), Q varbinary(256), T bigint, V varbinary(1024), K_PREFIX varbinary(1024) GENERATED ALWAYS AS (substr(K,1,32)) STORED, primary key(K, Q, T));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_reverse; create table if not exists htable1_cf1_reverse like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_ttl; create table if not exists htable1_cf1_ttl (K varbinary(1024), Q varbinary(256), T bigint, V varbinary(1024), primary key(K, Q, T)) kv_attributes='{\"Hbase\": {\"TimeToLive\": 5}}'" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_filter; create table if not exists htable1_cf1_filter like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_delete; create table if not exists htable1_cf1_delete like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_put; create table if not exists htable1_cf1_put like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_mutate; create table if not exists htable1_cf1_mutate like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_query_and_mutate; create table if not exists htable1_cf1_query_and_mutate like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_increment; create table if not exists htable1_cf1_increment like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_increment_empty; create table if not exists htable1_cf1_increment_empty like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_append; create table if not exists htable1_cf1_append like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_empty_cq; create table if not exists htable1_cf1_empty_cq (K varbinary(1024), Q varbinary(256), T bigint, V varbinary(1024), primary key(K, Q, T));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_query_sync; create table if not exists htable1_query_sync (K varbinary(1024), Q varbinary(256), T bigint, V varbinary(1024), primary key(K, Q, T));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_check_and_put; create table if not exists htable1_cf1_check_and_put like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_check_and_put_put; create table if not exists htable1_cf1_check_and_put_put like htable1_cf1" $db

# run
./test_table_api "$HOST" "$PORT" "$tenant_name" "$user_name" "$passwd" "$db" "$table_name" $RPCPORT #--gtest_filter=TestBatchExecute.
# round2 with index
mysql -h $HOST -P $PORT -u $user -e "drop table if exists batch_execute_test; create table if not exists batch_execute_test (C1 bigint primary key, C2 bigint, C3 varchar(100), index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists complex_batch_execute_test; create table if not exists complex_batch_execute_test (C1 bigint primary key, C2 bigint, C3 varchar(100), index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists all_single_operation_test; create table if not exists all_single_operation_test (C1 bigint primary key, C2 bigint, C3 varchar(100)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists type_check_test; create table type_check_test (pk1 bigint, pk2 varchar(10), ctinyint tinyint, csmallint smallint, cmediumint mediumint, cint int, cbigint bigint, utinyint tinyint unsigned, usmallint smallint unsigned, umediumint mediumint unsigned, uint int unsigned, ubigint bigint unsigned, cfloat float, cdouble double, ufloat float unsigned, udouble double unsigned, cnumber decimal(10, 2), unumber decimal(10,2) unsigned, cvarchar varchar(10), cchar char(10), cbinary binary(10), cvarbinary varbinary(10), ctimestamp timestamp, cdatetime datetime, cyear year, cdate date, ctime time, ctext text, cblob blob, cbit bit(64), cnotnull bigint not null default 111, PRIMARY KEY(pk1, pk2));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists column_default_value; create table column_default_value (C1 bigint primary key, C2 bigint default 1, C3 varchar(100) default 'abc', index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists partial_update_test; create table if not exists partial_update_test (C1 bigint primary key, C2 bigint, C3 varchar(100), index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists append_lob_test; create table if not exists append_lob_test (C1 bigint primary key, C2 bigint, C3 mediumtext, index i1(c2) local)" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists all_lob_test; create table if not exists all_lob_test (C1 bigint primary key, C2 bigint, C3 mediumtext, index i1(c2) local)" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists large_scan_test; create table if not exists large_scan_test (C1 bigint primary key, C2 bigint, C3 varchar(100), index i1(c2) local, index i2(c3) local, index i3(c2, c3))" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists uniq_replace_test; create table if not exists uniq_replace_test (C1 bigint primary key, C2 bigint, C3 varchar(100), unique key C2_uniq(C2), index i2(c3) local)" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists varchar_rowkey_update_test; create table if not exists varchar_rowkey_update_test (K varchar(512) primary key, T bigint, KEY idx_T(T) LOCAL);" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_increment_test; create table if not exists multi_increment_test (C1 bigint primary key, C2 bigint, C3 varchar(100), index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_get_test; create table if not exists single_get_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world', index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_get_test; create table if not exists multi_get_test (C1 bigint primary key, C2 bigint, C3 varchar(100) default 'hello world', index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_increment_test; create table if not exists single_increment_test (C1 bigint primary key, C2 bigint, C3 varchar(100), index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_insert_test; create table if not exists single_insert_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world', index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_update_test; create table if not exists single_update_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world', index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_update_test; create table if not exists multi_update_test (C1 bigint primary key, C2 bigint, C3 varchar(100) default 'hello world', index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists insert_generate_test; create table if not exists insert_generate_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world', C3_PREFIX varchar(10) GENERATED ALWAYS AS (substr(C3,1,2))) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists update_generate_test; create table if not exists update_generate_test (C1 bigint primary key, C2 varchar(100), C3 varchar(100), GEN varchar(100) GENERATED ALWAYS AS (concat(C2,c3)) stored) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_delete_test; create table if not exists single_delete_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world', index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_replace_test; create table if not exists single_replace_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world',index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists replace_unique_key_test; create table if not exists replace_unique_key_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world', unique index i1(c2) local)" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists single_insert_up_test; create table if not exists single_insert_up_test (C1 bigint primary key, C2 double, C3 varchar(100) default 'hello world', UNIQUE KEY idx_c2 (C2))" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_insert_or_update_test; create table if not exists multi_insert_or_update_test (C1 bigint primary key, C2 bigint, C3 varchar(100) default 'hello world', index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists kv_query_test; create table if not exists kv_query_test (C1 bigint, C2 bigint, C3 bigint, PRIMARY KEY(C1, C2), KEY idx_c2 (C2), KEY idx_c3 (C3), KEY idx_c2c3(C2, C3));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists check_scan_range_test; create table if not exists check_scan_range_test (C1 bigint, C2 varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, C3 bigint, PRIMARY KEY(C1, C2), KEY idx_c3 (C3));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists virtual_generate_col_test; create table if not exists virtual_generate_col_test (C1 bigint primary key, C2 bigint, C3 varchar(100), C3_PREFIX varchar(10) GENERATED ALWAYS AS (substr(C3,1,2)))" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists store_generate_col_test; create table if not exists store_generate_col_test (C1 bigint primary key, C2 varchar(10), C3 varchar(10), GEN varchar(30) generated always as (concat(C2,C3)) stored)" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_insert_test; create table if not exists multi_insert_test (C1 bigint primary key, C2 bigint, C3 varchar(100), index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists multi_delete_test; create table if not exists multi_delete_test (C1 bigint primary key, C2 bigint, C3 varchar(100), index i1(c2) local, index i2(c3) local, index i3(c2, c3)) PARTITION BY KEY(C1) PARTITIONS 16" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists query_sync_multi_batch_test; create table if not exists query_sync_multi_batch_test (PK1 bigint, PK2 bigint, C1 bigint, C2 varchar(100), C3 bigint, PRIMARY KEY(PK1, PK2), INDEX idx1(C1, C2));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists large_scan_query_sync_test; create table if not exists large_scan_query_sync_test (C1 bigint primary key, C2 bigint, C3 varchar(100));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists query_sync_with_index_test; create table if not exists query_sync_with_index_test (C1 bigint, C2 bigint, C3 bigint, primary key(C1, C2), KEY idx_c2 (C2), KEY idx_c3 (C3), KEY idx_c2c3(C2, C3));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists query_sync_multi_task_test; create table if not exists query_sync_multi_task_test (C1 bigint primary key, C2 bigint, C3 varchar(100));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists query_with_filter; create table if not exists query_with_filter (C1 bigint primary key, C2 bigint, C3 varchar(100), C4 double default 0);" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists query_and_mutate; create table if not exists query_and_mutate (C1 bigint primary key, C2 bigint, C3 varchar(100), C4 double default 0);" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists atomic_batch_ops; create table if not exists atomic_batch_ops (C1 bigint, C2 varchar(128), C3 varbinary(1024) default null, C4 bigint not null default -1, primary key(C1), UNIQUE KEY idx_c2c4 (C2, C4));" $db

# INDEX idx1(C1, C2)
mysql -h $HOST -P $PORT -u $user -e "drop table if exists execute_query_test; create table if not exists execute_query_test (PK1 bigint, PK2 bigint, C1 bigint, C2 varchar(100), C3 bigint, PRIMARY KEY(PK1, PK2));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists secondary_index_test; create table if not exists secondary_index_test (C1 bigint primary key, C2 bigint, C3 varchar(100), index i1(c2) local, index i2(c3) local, index i3(c2, c3) local)" $db
# hbase api
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1; create table if not exists htable1_cf1 (K varbinary(1024), Q varbinary(256), T bigint, V varbinary(1024), K_PREFIX varbinary(1024) GENERATED ALWAYS AS (substr(K,1,32)) STORED, primary key(K, Q, T));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_filter; create table if not exists htable1_cf1_filter like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_delete; create table if not exists htable1_cf1_delete like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_put; create table if not exists htable1_cf1_put like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_mutate; create table if not exists htable1_cf1_mutate like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_query_and_mutate; create table if not exists htable1_cf1_query_and_mutate like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_increment; create table if not exists htable1_cf1_increment like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_increment_empty; create table if not exists htable1_cf1_increment_empty like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_append; create table if not exists htable1_cf1_append like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_reverse; create table if not exists htable1_cf1_reverse like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_ttl; create table if not exists htable1_cf1_ttl (K varbinary(1024), Q varbinary(256), T bigint, V varbinary(1024), primary key(K, Q, T)) kv_attributes='{\"Hbase\": {\"TimeToLive\": 5}}'" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_empty_cq; create table if not exists htable1_cf1_empty_cq (K varbinary(1024), Q varbinary(256), T bigint, V varbinary(1024), primary key(K, Q, T));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_query_sync; create table if not exists htable1_query_sync (K varbinary(1024), Q varbinary(256), T bigint, V varbinary(1024), primary key(K, Q, T));" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_check_and_put; create table if not exists htable1_cf1_check_and_put like htable1_cf1" $db
mysql -h $HOST -P $PORT -u $user -e "drop table if exists htable1_cf1_check_and_put_put; create table if not exists htable1_cf1_check_and_put_put like htable1_cf1" $db

# run
./test_table_api "$HOST" "$PORT" "$tenant_name" "$user_name" "$passwd" "$db" "$table_name" $RPCPORT
