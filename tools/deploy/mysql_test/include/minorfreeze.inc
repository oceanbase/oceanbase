connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys;
alter system minor freeze tenant = all;
disconnect obsys;
connection default;
