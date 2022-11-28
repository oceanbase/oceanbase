
let $server_cnt = query_get_value(select count(*) as cnt from oceanbase.__all_server, cnt, 1);

let $schema_version = query_get_value(select max(schema_version) as max_schema_version from oceanbase.__all_tenant_history, max_schema_version, 1);

let $sync_cnt = 0;

let $i = 10;
while($i > 0)
{
  sleep 1;
  dec $i;
  let $sync_cnt = query_get_value(select count(*) as cnt from oceanbase.__all_virtual_server_schema_info where refreshed_schema_version >= $schema_version and tenant_id = 1, cnt, 1);
  if ($sync_cnt == $server_cnt)
  {
    let $i = -4;
  }
}

if ($i != -4)
{
  --echo check tenant sync timeout, sync_cnt:$sync_cnt, server_cnt:$server_cnt
  eval select * from oceanbase.__all_virtual_server_schema_info where refreshed_schema_version >= $schema_version and tenant_id = 1;
}
