let $i = 10;
while($i > 0)
{
  dec $i;
  let $no_leader_ls_cnt = query_get_value(select count(*) as cnt from oceanbase.__all_virtual_log_stat where tenant_id = $check_can_write_tenant_id  and ls_id not in (select ls_id from oceanbase.__all_virtual_log_stat where tenant_id = $check_can_write_tenant_id and role = 'LEADER'), cnt, 1);

  if ( $no_leader_ls_cnt == 0)
  {
    let $i = 0;
  }
  sleep 1;
}
