system sleep 5;
alter system set balancer_idle_time = '10s';
alter system set enable_auto_refresh_location_cache = "False";
alter system set merger_warm_up_duration_time = '0s';
alter system set zone_merge_concurrency = 2;
alter system set merger_check_interval = '10s';
alter system set enable_syslog_wf=False;
alter system set freeze_trigger_percentage = '70';
create user if not exists 'admin' IDENTIFIED BY 'admin';
use oceanbase;
create database if not exists test;

use test;
grant all on *.* to 'admin' WITH GRANT OPTION;



#FIXME: schema拆分模式建租户耗时增加，这里先加大语句超时时间先绕过
set @@session.ob_query_timeout = 40000000;
create resource unit box1 max_cpu 2, max_memory 805306368, max_iops 128, max_disk_size '5G', max_session_num 64, MIN_CPU=1, MIN_MEMORY=805306368, MIN_IOPS=128;
create resource pool pool2 unit = 'box1', unit_num = 1;
create tenant mysql replica_num = 1, resource_pool_list=('pool2') set ob_tcp_invited_nodes='%', ob_compatibility_mode='mysql', parallel_max_servers=10, parallel_servers_target=10, ob_sql_work_area_percentage=20, secure_file_priv = "";
alter resource unit sys_unit_config min_memory=1073741824,max_memory=1073741824;
set @@session.ob_query_timeout = 10000000;
system sleep 5;
alter tenant sys set variables recyclebin = 'on';
alter tenant sys set variables ob_enable_truncate_flashback = 'on';
alter tenant mysql set variables ob_tcp_invited_nodes='%';
alter tenant mysql set variables recyclebin = 'on';
alter tenant mysql set variables ob_enable_truncate_flashback = 'on';

select count(*) from oceanbase.__all_server group by zone limit 1 into @num;
set @sql_text = concat('alter resource pool pool2', ' unit_num = ', @num);
prepare stmt from @sql_text;
execute stmt;
deallocate prepare stmt;

select primary_zone from oceanbase.__all_tenant where tenant_id = 1 into @zone_name;
alter tenant mysql primary_zone = @zone_name;

