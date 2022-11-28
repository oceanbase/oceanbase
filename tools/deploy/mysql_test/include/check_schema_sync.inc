
connect (ob_sys_check_schema,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection ob_sys_check_schema;

let $i = 10;
while($i > 0)
{
  sleep 1;
  let $cnt = query_get_value(select count(*) as cnt from oceanbase.__all_virtual_server_schema_info as a join oceanbase.__all_virtual_server_schema_info as b on a.tenant_id = b.tenant_id  where a.refreshed_schema_version != b.refreshed_schema_version, cnt, 1);
  dec $i;
  if ($cnt == 0)
  {
    let $i = -4;
  }
}

if ($i != -4)
{
  --echo check schema sync timeout
  eval select * from oceanbase.__all_virtual_server_schema_info;
}
disconnect ob_sys_check_schema;
