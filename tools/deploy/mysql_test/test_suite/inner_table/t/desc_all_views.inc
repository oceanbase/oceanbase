# set $check_tenant_id
# set connection_sys
# set connection_tenant
# set query_type_2_str
# set query_type_3_str
# set query_type_4_str
connection connection_sys;
let $cnt = query_get_value(select count(*) as cnt from oceanbase.__all_virtual_table where tenant_id = $check_tenant_id and table_type = 1, cnt, 1);

let $idx = 1;
while ($idx <= $cnt)
{
connection connection_sys;
let $database_name = query_get_value(select d.database_name from oceanbase.__all_virtual_table as t join oceanbase.__all_virtual_database as d on t.tenant_id = d.tenant_id and t.database_id = d.database_id where t.tenant_id = $check_tenant_id and table_type = 1 order by t.table_id, database_name, $idx);
let $table_name = query_get_value(select table_name from oceanbase.__all_virtual_table where tenant_id = $check_tenant_id and table_type = 1 order by table_id, table_name, $idx);

connection connection_tenant;
eval desc $database_name.$table_name;
eval set @query_type = (select 
  case 
  when ($query_type_2_str like '%|$table_name|%') then 2
  when ($query_type_3_str like '%|$table_name|%') then 3
  when ($query_type_4_str like '%|$table_name|%') then 4
  else 1
  end a);
let $query_type=query_get_value(select @query_type a, a, 1);

if ($query_type == 1)
{
--replace_column 1 1
eval select /*+QUERY_TIMEOUT(60000000)*/ count(*) as cnt from (select * from $database_name.$table_name limit 1);
}

if ($query_type == 2)
{
eval select /*+QUERY_TIMEOUT(60000000)*/ IF(count(*) >= 0, 1, 0) from $database_name.$table_name;
}

if ($query_type == 3)
{
--replace_column 1 1
--disable_warnings
eval select /*+QUERY_TIMEOUT(60000000)*/ count(*) as cnt from (select * from $database_name.$table_name limit 1);
--enable_warnings
}

if ($query_type == 4)
{
--disable_warnings
# do nothing
--enable_warnings
}


inc $idx;
}


