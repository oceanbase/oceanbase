#--disable_query_log
#--disable_result_log
#
#let $index_status_expect = 2;
#let $result = 2;
#
#connect (obsys_caio,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
#connection obsys_caio;
#let $i=210;
#while($i>0)
#{
#    sleep 1;
#    #--echo $index_name
#    ##    let $result = query_get_value(select count(*) from oceanbase.__all_virtual_table where table_name like '__idx_%' and index_status!=2,count(*),1);
#    let $result = query_get_value(select count(*) from oceanbase.__all_virtual_table as t join oceanbase.__all_virtual_database as d where t.tenant_id = d.tenant_id and t.database_id = d.database_id and d.in_recyclebin = false and d.database_name != "__recyclebin" and t.table_name like '__idx_%' and t.index_status!=2,count(*),1);
#    if($result == 0)
#    {
#        let $i = -4;
#    }
#    dec $i;
#}
#sleep 2;
#if($i != -5)
#{
#    let $table_id = query_get_value(select table_id from oceanbase.__all_virtual_table as t join oceanbase.__all_virtual_database as d where t.tenant_id = d.tenant_id and t.database_id = d.database_id and d.in_recyclebin = false and d.database_name != "__recyclebin" and t.table_name like '__idx_%' and t.index_status!=2 limit 1,table_id,1);
#    --echo $result idx is build failed, first table id is $table_id
#}
#disconnect obsys_caio;
#connection default;
#--enable_query_log
#--enable_result_log
