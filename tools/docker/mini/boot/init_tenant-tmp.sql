CREATE RESOURCE UNIT IF NOT EXISTS @OB_TENANT_NAME@ max_cpu = 9, max_memory = 2684354560, min_memory = 2684354560, max_iops = 10000, min_iops = 1280, max_session_num = 3000, max_disk_size = 5153960755;
CREATE RESOURCE POOL IF NOT EXISTS @OB_TENANT_NAME@ UNIT = '@OB_TENANT_NAME@', UNIT_NUM = 1, ZONE_LIST = ('zone1');
CREATE TENANT IF NOT EXISTS @OB_TENANT_NAME@ charset='utf8mb4', replica_num=1, zone_list=('zone1'), primary_zone='RANDOM', resource_pool_list=('@OB_TENANT_NAME@');

ALTER SYSTEM SET _clog_aggregation_buffer_amount = 4;
ALTER SYSTEM SET _flush_clog_aggregation_buffer_timeout = '1ms';
ALTER TENANT @OB_TENANT_NAME@ SET VARIABLES ob_timestamp_service = 'GTS';
ALTER TENANT @OB_TENANT_NAME@ SET VARIABLES autocommit = ON;
ALTER TENANT @OB_TENANT_NAME@ SET VARIABLES ob_query_timeout = 36000000000;
ALTER TENANT @OB_TENANT_NAME@ SET VARIABLES ob_trx_timeout = 36000000000;
ALTER TENANT @OB_TENANT_NAME@ SET VARIABLES max_allowed_packet = 67108864;
ALTER TENANT @OB_TENANT_NAME@ SET VARIABLES ob_sql_work_area_percentage = 100;
ALTER TENANT @OB_TENANT_NAME@ SET VARIABLES parallel_max_servers = 20;
ALTER TENANT @OB_TENANT_NAME@ SET VARIABLES parallel_servers_target = 48;
ALTER TENANT @OB_TENANT_NAME@ SET VARIABLES ob_tcp_invited_nodes='%'
