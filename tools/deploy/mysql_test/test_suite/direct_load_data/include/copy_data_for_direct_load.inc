connect (obsys_for_load_data,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_for_load_data;

--disable_query_log
let $HOST_IP = query_get_value('select host_ip()',host_ip(), 1);
let $DATA_FOLDER_NAME = data_for_mysqltest_direct_loaddata;
let $SCP_CMD = ssh $HOST_IP 'mkdir -p "$OBSERVER_DIR/$DATA_FOLDER_NAME"' && scp -r $PWD/mysql_test/test_suite/direct_load_data/data/* $HOST_IP:"$OBSERVER_DIR/$DATA_FOLDER_NAME";
exec $SCP_CMD;
--enable_query_log

disconnect obsys_for_load_data;
connection default;

