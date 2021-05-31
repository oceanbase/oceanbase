SET SQL_SAFE_UPDATES = 0;
create database if not exists zhan;
use zhan;
#register operators here
drop table if exists oper;
create table oper (name varchar(10),literal varchar(10)) engine = MEMORY;
insert into oper values('add','+');
#insert into oper values('minus','-');
#insert into oper values('mul','*');
#insert into oper values('div','/');
#insert into oper values('mod','%');
#register datatypes and their cases

#register data types below
drop table if exists datatype;
create table datatype (name varchar(100)) engine = MEMORY;

insert into datatype values('tinyint');
drop table if exists tinyint_case;
create table tinyint_case (id int primary key auto_increment,v tinyint) engine = MEMORY;
insert into tinyint_case(v) values(-128);
insert into tinyint_case(v) values(-64);
insert into tinyint_case(v) values(-1);
insert into tinyint_case(v) values(0);
insert into tinyint_case(v) values(1);
insert into tinyint_case(v) values(64);
insert into tinyint_case(v) values(127);
insert into tinyint_case(v) values(null);

insert into datatype values('smallint');
drop table if exists smallint_case;
create table smallint_case (id int primary key auto_increment,v smallint) engine = MEMORY;
insert into smallint_case(v) values(-32768);
insert into smallint_case(v) values(-64);
insert into smallint_case(v) values(-1);
insert into smallint_case(v) values(0);
insert into smallint_case(v) values(1);
insert into smallint_case(v) values(64);
insert into smallint_case(v) values(32767);
insert into smallint_case(v) values(null);


insert into datatype values('mediumint');
drop table if exists mediumint_case;
create table mediumint_case (id int primary key auto_increment,v mediumint) engine = MEMORY;
insert into mediumint_case(v) values(-8388608);
insert into mediumint_case(v) values(-64);
insert into mediumint_case(v) values(-1);
insert into mediumint_case(v) values(0);
insert into mediumint_case(v) values(1);
insert into mediumint_case(v) values(64);
insert into mediumint_case(v) values(8388607);
insert into mediumint_case(v) values(null);

insert into datatype values('int32');
drop table if exists int32_case;
create table int32_case (id int primary key auto_increment,v int) engine = MEMORY;
insert into int32_case(v) values(-2147483648);
insert into int32_case(v) values(-64);
insert into int32_case(v) values(-1);
insert into int32_case(v) values(0);
insert into int32_case(v) values(1);
insert into int32_case(v) values(64);
insert into int32_case(v) values(2147483647);
insert into int32_case(v) values(null);

insert into datatype values('int');
drop table if exists int_case;
create table int_case (id int primary key auto_increment,v bigint) engine = MEMORY;
insert into int_case(v) values(-9223372036854775808);
insert into int_case(v) values(-2147483648);
insert into int_case(v) values(-64);
insert into int_case(v) values(-1);
insert into int_case(v) values(0);
insert into int_case(v) values(1);
insert into int_case(v) values(64);
insert into int_case(v) values(2147483647);
insert into int_case(v) values(9223372036854775807);
insert into int_case(v) values(null);


insert into datatype values('utinyint');
drop table if exists utinyint_case;
create table utinyint_case (id int primary key auto_increment,v tinyint unsigned) engine = MEMORY;
insert into utinyint_case(v) values(0);
insert into utinyint_case(v) values(1);
insert into utinyint_case(v) values(64);
insert into utinyint_case(v) values(255);
insert into utinyint_case(v) values(null);


insert into datatype values('usmallint');
drop table if exists usmallint_case;
create table usmallint_case (id int primary key auto_increment,v smallint unsigned) engine = MEMORY;
insert into usmallint_case(v) values(0);
insert into usmallint_case(v) values(1);
insert into usmallint_case(v) values(64);
insert into usmallint_case(v) values(65535);
insert into usmallint_case(v) values(null);

insert into datatype values('umediumint');
drop table if exists umediumint_case;
create table umediumint_case (id int primary key auto_increment,v mediumint unsigned) engine = MEMORY;
insert into umediumint_case(v) values(0);
insert into umediumint_case(v) values(1);
insert into umediumint_case(v) values(64);
insert into umediumint_case(v) values(16777215);
insert into umediumint_case(v) values(null);

insert into datatype values('uint32');
drop table if exists uint32_case;
create table uint32_case (id int primary key auto_increment,v int unsigned) engine = MEMORY;
insert into uint32_case(v) values(0);
insert into uint32_case(v) values(1);
insert into uint32_case(v) values(64);
insert into uint32_case(v) values(4294967295);
insert into uint32_case(v) values(null);

insert into datatype values('uint64');
drop table if exists uint64_case;
create table uint64_case (id int primary key auto_increment,v bigint unsigned) engine = MEMORY;
insert into uint64_case(v) values(0);
insert into uint64_case(v) values(1);
insert into uint64_case(v) values(64);
insert into uint64_case(v) values(4294967295);
insert into uint64_case(v) values(18446744073709551615);
insert into uint64_case(v) values(null);

insert into datatype values('float');
drop table if exists float_case;
create table float_case (id int primary key auto_increment,v float) engine = MEMORY;
insert into float_case(v) values(-3.4e+38);
insert into float_case(v) values(-64.5);
insert into float_case(v) values(-1.5);
insert into float_case(v) values(0);
insert into float_case(v) values(1.5);
insert into float_case(v) values(64.5);
insert into float_case(v) values(3.4e+38);
insert into float_case(v) values(null);

insert into datatype values('double');
drop table if exists double_case;
create table double_case (id int primary key auto_increment,v double) engine = MEMORY;
insert into double_case(v) values(-3.4e+38);
insert into double_case(v) values(-64.5);
insert into double_case(v) values(-1.5);
insert into double_case(v) values(0);
insert into double_case(v) values(1.5);
insert into double_case(v) values(64.5);
insert into double_case(v) values(3.4e+38);
insert into double_case(v) values(null);


/*
insert into datatype values('number');
drop table if exists number_case;
create table number_case (id int primary key auto_increment,v numeric(30,4)) engine = MEMORY;
insert into number_case(v) values(-32768.1234);
insert into number_case(v) values(-64.1234);
insert into number_case(v) values(-1.1234);
insert into number_case(v) values(0);
insert into number_case(v) values(1.1234);
insert into number_case(v) values(64.1234);
insert into number_case(v) values(32768.1234);
insert into number_case(v) values(null);
*/

insert into datatype values('varchar');
drop table if exists varchar_case;
create table varchar_case (id int primary key auto_increment,v varchar(255)) engine = MEMORY;
insert into varchar_case(v) values(-32768);
insert into varchar_case(v) values(-64);
insert into varchar_case(v) values(-1);
insert into varchar_case(v) values(0);
insert into varchar_case(v) values(1);
insert into varchar_case(v) values(64);
insert into varchar_case(v) values(32768);
insert into varchar_case(v) values(null);


insert into datatype values('char');
drop table if exists char_case;
create table char_case (id int primary key auto_increment,v char(255)) engine = MEMORY;
insert into char_case(v) values(-32768);
insert into char_case(v) values(-64);
insert into char_case(v) values(-1);
insert into char_case(v) values(0);
insert into char_case(v) values(1);
insert into char_case(v) values(64);
insert into char_case(v) values(32768);
insert into char_case(v) values(null);


drop table if exists case_combination;
create table case_combination as select A.name as oper, A.literal as literal,B.name as type1,C.name as type2 from oper A join datatype B join datatype C ;

drop table if exists output;
create table output (v varchar(255)) engine = MEMORY ;

drop procedure if exists init_temp_tables;
delimiter //
create procedure init_temp_tables(in oper_literal varchar(255),in case_table_name_1 varchar(255),in case_table_name_2 varchar(255)) 
	begin
        declare is_overflow bool default false;
        declare continue handler for 1690 set is_overflow = true;
        drop table if exists temp_table_1;
        drop table if exists temp_table_2;
        drop table if exists temp_table_res;
        #create temp table following the original structure 
        set @stmt_str = concat('create temporary table temp_table_1 as select * from ',case_table_name_1,' where 1 = 2');
        prepare stmt from @stmt_str;
        execute stmt;
        deallocate prepare stmt;
        set @stmt_str = concat('create temporary table temp_table_2 as select * from ',case_table_name_2,' where 1 = 2');
        prepare stmt from @stmt_str;
        execute stmt;
        deallocate prepare stmt;
        #deduce type. 0 can be inserted to any type
        insert into temp_table_1(v) values(0);
        insert into temp_table_2(v) values(0);
        set @stmt_str = concat('create temporary table temp_table_res select A.v ',oper_literal,' B.v as vres from temp_table_1 A join temp_table_2 B');
        prepare stmt from @stmt_str;
        execute stmt;
        deallocate prepare stmt;
        set @stmt_str = concat('select column_type from information_schema.columns where table_name = \'temp_table_res\' and column_name = \'vres\' into @type_res');
        prepare stmt from @stmt_str;
        execute stmt;
        deallocate prepare stmt;
        drop table temp_table_1;
        drop table temp_table_2;
        drop table temp_table_res;
        #get type name for both arg tables
        set @stmt_str = concat('select column_type from information_schema.columns where table_name = \'',case_table_name_1,'\' and column_name = \'v\' into @type_t1');
        prepare stmt from @stmt_str;
        execute stmt;
        deallocate prepare stmt;
        set @stmt_str = concat('select column_type from information_schema.columns where table_name = \'',case_table_name_2,'\' and column_name = \'v\' into @type_t2');
        prepare stmt from @stmt_str;
        execute stmt;
        deallocate prepare stmt;
        drop table if exists value_combination;
        set @stmt_str = concat('create temporary table value_combination (id int primary key auto_increment, v1 ',@type_t1,', v2 ',@type_t2,') engine = memory');
        prepare stmt from @stmt_str;
        execute stmt;
        deallocate prepare stmt;
        set @stmt_str = concat('insert into value_combination (v1,v2) select A.v , B.v from ',case_table_name_1,' as A join ',case_table_name_2,' as B');
        prepare stmt from @stmt_str;
        execute stmt;
        deallocate prepare stmt;
        #we are good to go
    end
	//
delimiter ;

drop procedure if exists cleanup_temp_tables;
delimiter //
create procedure cleanup_temp_tables() 
	begin
        drop table if exists temp_table_1;
        drop table if exists temp_table_2;
        drop table if exists temp_table_res;
        drop table if exists value_combination;
    end
	//
delimiter ;


drop procedure if exists do_generate_for_type_pair;
delimiter //
create procedure do_generate_for_type_pair(in oper varchar(255),in oper_literal varchar(255),in type1 varchar(255),in type2 varchar(255)) 
	begin
        declare is_overflow bool default false;
        declare value_index int default 1;
        declare value_count int default 1;
        select count(*) from value_combination into value_count;
        begin
        declare continue handler for 1690 set is_overflow = true;
        while value_index <= value_count do
            set is_overflow = false;
            set @stmt_str = concat('insert into output select concat(\'',oper,'\',\'|\',\'',type1,'\',\'|\',\'',type2,'\',\'|\',\'',@type_res,'\',\'|\',ifnull(v1,\'null\'),\'|\',ifnull(v2,\'null\'),\'|\',ifnull(v1 ',oper_literal,' v2,\'null\')) from value_combination where id = ',value_index);
            prepare stmt from @stmt_str;
            execute stmt;
            deallocate prepare stmt;
            if is_overflow = true
            then
                set @stmt_str = concat('insert into output select concat(\'',oper,'\',\'|\',\'',type1,'\',\'|\',\'',type2,'\',\'|\',\'',@type_res,'\',\'|\',ifnull(v1,\'null\'),\'|\',ifnull(v2,\'null\'),\'|\',\'overflow\') from value_combination where id = ',value_index);
                prepare stmt from @stmt_str;
                execute stmt;
                deallocate prepare stmt;
            end if;
            set value_index = value_index + 1;
        end while;
      end;  
    end;
	//
delimiter ;

drop procedure if exists generate; 
delimiter //
create procedure generate()
    begin
    declare oper varchar(255);
    declare oper_literal varchar(255);
    declare typecase_table_name1 varchar(255);
    declare typecase_table_name2 varchar(255);
    declare type1 varchar(255);
    declare type2 varchar(255);
    declare done int default 0;
    declare cursor_name_1 varchar(255);
    declare cursor_name_2 varchar(255);
    declare value_name_1 varchar(255);
    declare value_name_2 varchar(255);
    declare case_combination_iter cursor for select * from case_combination;
    declare continue handler for not found set done = 1;
    open case_combination_iter;
    fetch case_combination_iter into oper,oper_literal,type1,type2;
    repeat
        set typecase_table_name1 = concat(type1,'_case');
        set typecase_table_name2 = concat(type2,'_case');
        call init_temp_tables(oper_literal,typecase_table_name1,typecase_table_name2);
        call do_generate_for_type_pair(oper,oper_literal,type1,type2);
        call cleanup_temp_tables();
        fetch case_combination_iter into oper,oper_literal,type1,type2;
    until done end repeat;
    end
    //
delimiter ;

call generate();

select * from output;

drop table if exists tinyint_case;
drop table if exists smallint_case;
drop table if exists mediumint_case;
drop table if exists int32_case;
drop table if exists int_case;

drop table if exists utinyint_case;
drop table if exists usmallint_case;
drop table if exists umediumint_case;
drop table if exists uint32_case;
drop table if exists uint64_case;

drop table if exists float_case;
drop table if exists double_case;
drop table if exists number_case;
drop table if exists varchar_case;
drop table if exists char_case;

drop table if exists output;
drop table if exists case_combination;
drop table if exists oper;
drop table if exists datatype;

drop procedure if exists init_temp_tables;
drop procedure if exists cleanup_temp_tables;
drop procedure if exists do_generate_for_type_pair;
drop procedure if exists generate; 
