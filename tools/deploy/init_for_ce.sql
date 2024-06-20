system sleep 5;
alter system set balancer_idle_time = '10s';
create user if not exists 'admin' IDENTIFIED BY 'admin';
use oceanbase;
create database if not exists test;

use test;
grant all on *.* to 'admin' WITH GRANT OPTION;



alter system set enable_syslog_wf=false;
set @@session.ob_query_timeout = 200000000;

source init_create_tenant_routines.sql;

call adjust_sys_resource();
call create_tenant_with_arg('mysql', 'mysql', '2c2g', '');

/****************************** ATTENTION ******************************/
/* The tenant=all will be deprecated. If you want all tenants to be    */
/* modified, use tenant=sys & tenant=all_user & tenant=all_meta.       */
/***********************************************************************/
set @@session.ob_query_timeout = 10000000;
system sleep 5;
alter tenant sys set variables recyclebin = 'on';
alter tenant sys set variables ob_enable_truncate_flashback = 'on';
alter tenant mysql set variables ob_tcp_invited_nodes='%';
alter tenant mysql set variables recyclebin = 'on';
alter tenant mysql set variables ob_enable_truncate_flashback = 'on';
alter tenant sys set variables _nlj_batching_enabled = true;
alter tenant mysql set variables _nlj_batching_enabled = true;
alter system set ob_compaction_schedule_interval = '10s' tenant sys;
alter system set ob_compaction_schedule_interval = '10s' tenant all_user;
alter system set ob_compaction_schedule_interval = '10s' tenant all_meta;
alter system set merger_check_interval = '10s' tenant sys;
alter system set merger_check_interval = '10s' tenant all_user;
alter system set merger_check_interval = '10s' tenant all_meta;
alter system set enable_sql_extension=true tenant sys;
alter system set enable_sql_extension=true tenant all_user;
alter system set enable_sql_extension=true tenant all_meta;
alter system set _enable_adaptive_compaction = false tenant sys;
alter system set _enable_adaptive_compaction = false tenant all_user;
alter system set _enable_adaptive_compaction = false tenant all_meta;
alter system set_tp tp_no = 1200, error_code = 4001, frequency = 1;
alter system set_tp tp_no = 509, error_code = 4016, frequency = 1;
alter system set_tp tp_no = 368, error_code = 4016, frequency = 1;
alter system set_tp tp_no = 551, error_code = 5434, frequency = 1;

alter system set _enable_var_assign_use_das = true tenant = sys;
alter system set _enable_var_assign_use_das = true tenant = all_user;
alter system set _enable_var_assign_use_das = true tenant = all_meta;
alter system set _enable_spf_batch_rescan = true tenant = sys;
alter system set _enable_spf_batch_rescan = true tenant = all_user;
alter system set _enable_spf_batch_rescan = true tenant = all_meta;