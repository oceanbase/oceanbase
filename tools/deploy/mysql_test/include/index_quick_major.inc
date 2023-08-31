connect (obsys_idm,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_idm;
set ob_enable_index_direct_select=1;
alter system set merger_check_interval = '10s' tenant sys;
alter system set merger_check_interval = '10s' tenant all_user;
alter system set merger_check_interval = '10s' tenant all_meta;
disconnect obsys_idm;
connection default;
