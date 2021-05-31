--disable_query_log
--disable_result_log
connection default;
let $tenant_id = query_get_value(select effective_tenant_id() as ID from dual, ID, 1);
connect (obsys_ydm,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_ydm;
let $__i__= 100;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;

## 检查有没有分区的列级统计信息还没有汇报上来
    let $__num__ = query_get_value(select count(*) as num from oceanbase.__all_virtual_meta_table a where a.tenant_id = $tenant_id and  not exists (select 1 from oceanbase.__all_virtual_column_statistic b where a.table_id = b.table_id and a.partition_id = b.partition_id and b.tenant_id = $tenant_id), num, 1);
    if($__num__ == 0)
    {
        let $__i__ = -5;
    }
}

connection default;
disconnect obsys_ydm;

--enable_query_log
--enable_result_log
