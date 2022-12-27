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
call create_tenant_by_memory_resource('mysql', 'mysql');

set @@session.ob_query_timeout = 10000000;
system sleep 5;
alter tenant sys set variables recyclebin = 'on';
alter tenant sys set variables ob_enable_truncate_flashback = 'on';
alter tenant mysql set variables ob_tcp_invited_nodes='%';
alter tenant mysql set variables recyclebin = 'on';
alter tenant mysql set variables ob_enable_truncate_flashback = 'on';
alter system set ob_compaction_schedule_interval = '10s' tenant all;
alter system set merger_check_interval = '10s' tenant all;
alter system set enable_sql_extension=true tenant all;
alter system set _enable_adaptive_compaction = false tenant all;
