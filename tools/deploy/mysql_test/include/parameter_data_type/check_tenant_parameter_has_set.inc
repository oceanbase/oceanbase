let $done = 0;
while($done == 0)
{
  let $done = query_get_value(select count(name) cnt from oceanbase.__all_virtual_tenant_parameter where name = '$para_name' and tenant_id = 1, cnt, 1);
  if ($done == 0)
  {
    sleep 1;
  }
}
let $done = 0;
while($done == 0)
{
  let $done = query_get_value(select count(name) cnt from oceanbase.__all_virtual_tenant_parameter where name = '$para_name' and tenant_id = $mysql_tenant_id, cnt, 1);
  if ($done == 0)
  {
    sleep 1;
  }
}
