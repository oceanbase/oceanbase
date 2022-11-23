use oceanbase;
create user if not exists 'admin' IDENTIFIED BY 'admin';
grant all on *.* to 'admin' WITH GRANT OPTION;
create database obproxy;

