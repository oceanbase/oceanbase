connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys;
# major freeze all tenant
--disable_warnings
alter system major freeze tenant = sys;
alter system major freeze tenant = all_user;
alter system major freeze tenant = all_meta;
--enable_warnings
disconnect obsys;
connection default;
