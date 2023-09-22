-- =========================================================================================
-- author: wanhong.wwh
-- 本文件提供了创建和删除租户相关的procedure，简化操作流程，不再需要了解租户资源规格、资源池概念，提供集成化的接口
-- * 创建租户: 集成创建租户规格、创建资源池、创建租户
-- * 删除租户: 删除租户、删除资源池、删除资源规格
-- 使用方法示例：
--    call oceanbase.create_mysql_tenant('租户名称');
--    call oceanbase.drop_tenant();
-- 注意事项：
--    * create_xx_tenant()和drop_tenant()需要成对调用，保证删除所有资源
--    * procedure创建在oceanbase库下，需要增加oceanbase前缀
--    * 如果放在mysqltest中使用，建议增加 --disable_query_log和--enable_query_log，避免输出影响结果，例如：
--      --disable_query_log
--      call oceanbase.create_mysql_tenant('test');
--      --enable_query_log
-- *  mysql租户系列procedure
--    - create_mysql_tenant：创建一个默认配置的mysql租户，目前是2c4g
--    - create_mysql_tenant_mini：创建一个默认配置的mysql租户
set @@session.ob_query_timeout = 200000000;
alter system set __min_full_resource_pool_memory=1073741824;

use oceanbase;

-- 1C1G 规格
create resource unit if not exists 1c1g max_cpu 1, memory_size '1G';

-- 1C2G 规格
create resource unit if not exists 1c2g max_cpu 1, memory_size '2G';

-- 2C2G 规格
create resource unit if not exists 2c2g max_cpu 2, memory_size '2G';

-- 2C4G 规格
create resource unit if not exists 2c4g max_cpu 2, memory_size '4G';

-- =================================== create_tenant 模板 ===================================
-- create_tenant: 指定规格创建租户
-- @param  tenant_name  租户名
-- @param  compat_mode  兼容模式，'mysql' or 'oracle'
-- @param  unit_config  unit规格名，要求规格名已存在，例如：我们预创建了一批规格：1c1g, 1c2g, 2c2g, 2c4g
delimiter /
drop procedure if exists create_tenant;/
create procedure create_tenant(tenant_name varchar(64), compat_mode varchar(10), unit_config varchar(64))
begin
  call oceanbase.create_tenant_with_arg(tenant_name, compat_mode, unit_config, '');
end /

-- create_tenant_with_arg: 指定规格、以及参数列表创建租户
-- @param arg_list    创建租户的参数列表，例如: charset=gb18030
drop procedure if exists create_tenant_with_arg;/
create procedure create_tenant_with_arg(tenant_name varchar(64), compat_mode varchar(10), unit_config varchar(64), arg_list varchar(64))
begin
  declare num int;
  declare zone_name varchar(20);

  select count(*) from oceanbase.DBA_OB_SERVERS group by zone limit 1 into num;
  select zone from (select zone, count(*) as a from oceanbase.DBA_OB_ZONES group by region order by a desc limit 1) into zone_name;
  -- resource pool名称默认为：pool_for_tenant_xxx
  set @pool_name = concat("pool_for_tenant_", tenant_name);
  set @sql_text = concat("create resource pool if not exists ", @pool_name, " unit = '", unit_config, "', unit_num = ", num, ";");
  prepare stmt from @sql_text;
  execute stmt;

  if (arg_list = '') then set @str = ''; else set @str = ','; end if;

  set @sql_text = concat("create tenant ", tenant_name, " primary_zone='", zone_name, "', resource_pool_list=('", @pool_name, "') ", @str, arg_list, " set ob_compatibility_mode='", compat_mode, "', ob_tcp_invited_nodes='%', parallel_servers_target=10, secure_file_priv = '/';");
  prepare stmt from @sql_text;
  execute stmt;
  deallocate prepare stmt;
end /

-- =================================== drop_tenant ===================================
-- 保证删除掉租户，清理资源
drop procedure if exists drop_tenant;/
create procedure drop_tenant(tenant_name varchar(64))
begin
  declare recyclebin_value int;
  select value from oceanbase.CDB_OB_SYS_VARIABLES where name = 'recyclebin' and tenant_id=1 into recyclebin_value;
  set recyclebin = off;

  -- 首先删除租户
  set @sql_text = concat("drop tenant if exists ", tenant_name, ";");
  prepare stmt from @sql_text;
  execute stmt;

  -- 清理resource pool
  set @pool_name = concat("pool_for_tenant_", tenant_name);
  set @sql_text = concat("drop resource pool if exists ", @pool_name, ";");
  prepare stmt from @sql_text;
  execute stmt;

  deallocate prepare stmt;
  set recyclebin = recyclebin_value;
end /

-- =================================== drop_tenant_force ===================================
-- 快速删除租户，资源可能还保留
drop procedure if exists drop_tenant_force;/
create procedure drop_tenant_force(tenant_name varchar(64))
begin
  declare recyclebin_value int;
  select value from oceanbase.CDB_OB_SYS_VARIABLES where name = 'recyclebin' and tenant_id=1 into recyclebin_value;
  set recyclebin = off;

  -- 首先删除租户
  set @sql_text = concat("drop tenant if exists ", tenant_name, " force;");
  prepare stmt from @sql_text;
  execute stmt;

  -- 清理resource pool
  set @pool_name = concat("pool_for_tenant_", tenant_name);
  set @sql_text = concat("drop resource pool if exists ", @pool_name, ";");
  prepare stmt from @sql_text;
  execute stmt;

  deallocate prepare stmt;
  set recyclebin = recyclebin_value;
end /

-- =================================== create_mysql_tenant ===================================
-- create_mysql_tenant / create_mysql_tenant_with_arg: 默认创建一个2c4g的mysql租户
drop procedure if exists create_mysql_tenant;/
create procedure create_mysql_tenant(tenant_name varchar(64))
begin
  call oceanbase.create_mysql_tenant_with_arg(tenant_name, '');
end /

drop procedure if exists create_mysql_tenant_with_arg;/
create procedure create_mysql_tenant_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_mysql_tenant_2c4g(tenant_name);
end /

-- create_mysql_tenant_mini / create_mysql_tenant_mini_with_arg: 默认创建一个1c1g的mysql租户
drop procedure if exists create_mysql_tenant_mini;/
create procedure create_mysql_tenant_mini(tenant_name varchar(64))
begin
  call oceanbase.create_mysql_tenant_1c1g(tenant_name);
end /

drop procedure if exists create_mysql_tenant_mini_with_arg;/
create procedure create_mysql_tenant_mini_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_mysql_tenant_1c1g_with_arg(tenant_name, arg_list);
end /

-- create_mysql_tenant_1c1g / create_mysql_tenant_1c1g_with_arg: 创建一个1c1g的mysql租户
drop procedure if exists create_mysql_tenant_1c1g;/
create procedure create_mysql_tenant_1c1g(tenant_name varchar(64))
begin
  call oceanbase.create_tenant(tenant_name, 'mysql', '1c1g');
end /

drop procedure if exists create_mysql_tenant_1c1g_with_arg;/
create procedure create_mysql_tenant_1c1g_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_tenant_with_arg(tenant_name, 'mysql', '1c1g', arg_list);
end /

-- create_mysql_tenant_1c2g / create_mysql_tenant_1c2g_with_arg: 创建一个1c2g的mysql租户
drop procedure if exists create_mysql_tenant_1c2g;/
create procedure create_mysql_tenant_1c2g(tenant_name varchar(64))
begin
  call oceanbase.create_tenant(tenant_name, 'mysql', '1c2g');
end /

drop procedure if exists create_mysql_tenant_1c2g_with_arg;/
create procedure create_mysql_tenant_1c2g_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_tenant_with_arg(tenant_name, 'mysql', '1c2g', arg_list);
end /

-- create_mysql_tenant_2c2g / create_mysql_tenant_2c2g_with_arg: 创建一个2c2g的mysql租户
drop procedure if exists create_mysql_tenant_2c2g;/
create procedure create_mysql_tenant_2c2g(tenant_name varchar(64))
begin
  call oceanbase.create_tenant(tenant_name, 'mysql', '2c2g');
end /

drop procedure if exists create_mysql_tenant_2c2g_with_arg;/
create procedure create_mysql_tenant_2c2g_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_tenant_with_arg(tenant_name, 'mysql', '2c2g', arg_list);
end /

-- create_mysql_tenant_2c4g / create_mysql_tenant_2c4g_with_arg: 创建一个2c4g的mysql租户
drop procedure if exists create_mysql_tenant_2c4g;/
create procedure create_mysql_tenant_2c4g(tenant_name varchar(64))
begin
  call oceanbase.create_tenant(tenant_name, 'mysql', '2c4g');
end /

drop procedure if exists create_mysql_tenant_2c4g_with_arg;/
create procedure create_mysql_tenant_2c4g_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_tenant_with_arg(tenant_name, 'mysql', '2c4g', arg_list);
end /

-- =================================== create_oracle_tenant ===================================
-- create_oracle_tenant / create_oracle_tenant_with_arg: 默认创建一个2c4g的oracle租户
drop procedure if exists create_oracle_tenant;/
create procedure create_oracle_tenant(tenant_name varchar(64))
begin
  call oceanbase.create_oracle_tenant_2c4g(tenant_name);
end /

drop procedure if exists create_oracle_tenant_with_arg;/
create procedure create_oracle_tenant_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_oracle_tenant_2c4g_with_arg(tenant_name, arg_list);
end /

-- create_oracle_tenant_mini / create_oracle_tenant_mini_with_arg: 默认创建一个1c1g的oracle租户
drop procedure if exists create_oracle_tenant_mini;/
create procedure create_oracle_tenant_mini(tenant_name varchar(64))
begin
  call oceanbase.create_oracle_tenant_1c1g(tenant_name);
end /

drop procedure if exists create_oracle_tenant_mini_with_arg;/
create procedure create_oracle_tenant_mini_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_oracle_tenant_1c1g_with_arg(tenant_name, arg_list);
end /

-- create_oracle_tenant_1c1g / create_oracle_tenant_1c1g_with_arg: 创建一个1c1g的oracle租户
drop procedure if exists create_oracle_tenant_1c1g;/
create procedure create_oracle_tenant_1c1g(tenant_name varchar(64))
begin
  call oceanbase.create_tenant(tenant_name, 'oracle', '1c1g');
end /

drop procedure if exists create_oracle_tenant_1c1g_with_arg;/
create procedure create_oracle_tenant_1c1g_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_tenant_with_arg(tenant_name, 'oracle', '1c1g', arg_list);
end /

-- create_oracle_tenant_1c2g / create_oracle_tenant_1c2g_with_arg: 创建一个1c2g的oracle租户
drop procedure if exists create_oracle_tenant_1c2g;/
create procedure create_oracle_tenant_1c2g(tenant_name varchar(64))
begin
  call oceanbase.create_tenant(tenant_name, 'oracle', '1c2g');
end /

drop procedure if exists create_oracle_tenant_1c2g_with_arg;/
create procedure create_oracle_tenant_1c2g_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_tenant_with_arg(tenant_name, 'oracle', '1c2g', arg_list);
end /

-- create_oracle_tenant_2c2g / create_oracle_tenant_2c2g_with_arg: 创建一个2c2g的oracle租户
drop procedure if exists create_oracle_tenant_2c2g;/
create procedure create_oracle_tenant_2c2g(tenant_name varchar(64))
begin
  call oceanbase.create_tenant(tenant_name, 'oracle', '2c2g');
end /

drop procedure if exists create_oracle_tenant_2c2g_with_arg;/
create procedure create_oracle_tenant_2c2g_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_tenant_with_arg(tenant_name, 'oracle', '2c2g', arg_list);
end /

-- create_oracle_tenant_2c4g: 创建一个2c4g的oracle租户
drop procedure if exists create_oracle_tenant_2c4g;/
create procedure create_oracle_tenant_2c4g(tenant_name varchar(64))
begin
  call oceanbase.create_tenant(tenant_name, 'oracle', '2c4g');
end /

drop procedure if exists create_oracle_tenant_2c4g_with_arg;/
create procedure create_oracle_tenant_2c4g_with_arg(tenant_name varchar(64), arg_list varchar(64))
begin
  call oceanbase.create_tenant_with_arg(tenant_name, 'oracle', '2c4g', arg_list);
end /

-- create_tenant_by_memory_limit: 根据memory_limit创建租户
drop procedure if exists create_tenant_by_memory_resource_with_arg;/
create procedure create_tenant_by_memory_resource_with_arg(tenant_name varchar(64), compat_mode varchar(10), arg_list varchar(64))
begin
  declare mem bigint;
  select memory_limit from GV$OB_SERVERS limit 1 into mem;
  if (mem < 8589934592) then
    call oceanbase.create_tenant_with_arg(tenant_name, compat_mode, '1c1g', arg_list);
  elseif (mem < 17179869184) then
    call oceanbase.create_tenant_with_arg(tenant_name, compat_mode, '2c2g', arg_list);
  else
    call oceanbase.create_tenant_with_arg(tenant_name, compat_mode, '2c4g', arg_list);
  end if;
end /

drop procedure if exists create_tenant_by_memory_resource;/
create procedure create_tenant_by_memory_resource(tenant_name varchar(64), compat_mode varchar(10))
begin
  call create_tenant_by_memory_resource_with_arg(tenant_name, compat_mode, '');
end /

-- adjust_sys_resource: 根据memory_limit调整sys租户规格
drop procedure if exists adjust_sys_resource;/
create procedure adjust_sys_resource()
begin
  declare mem bigint;
  select memory_limit from GV$OB_SERVERS limit 1 into mem;
  set @sql_text = "alter resource unit sys_unit_config memory_size = 1073741824;";
  if (mem < 17179869184) then
    prepare stmt from @sql_text;
    execute stmt;
    deallocate prepare stmt;
  end if;
end /

-- end of procedure
delimiter ;
