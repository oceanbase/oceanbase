############################## test for start with #######################################
### 单表测试 ###
select c1 from t0 start with pk = 1 connect by prior 1 = 1;
select c1 from t0 start with c1 = 1 connect by prior c1 = c2;
select c1 from t0 start with pk > 1 connect by prior c1 = c2;
select c1 from t0 start with c1 > 1 or c1 < -1 connect by prior c1 = c2;
select c1 from t0 start with c1 > 1 or 1 = 2 connect by prior c1 = c2;
select c1 from (select * from t0) as tt start with c1 > 1  connect by prior c1 = c2;

### 两表测试 ###

select t0.c1 from t0, t4 start with t0.c1 = 1 connect by prior t0.c1 = t0.c2;
select t0.c1 from t0, t4 start with t4.c1 = 1 connect by prior t0.c1 = t0.c2;
select t0.c1 from t0, t4 start with t0.c1 + t4.c1 = 1 connect by prior t0.c1 = t0.c2;
select t0.c1 from t0 left join t4 on t0.c1 = t4.c1  connect by prior t0.c1 = t0.c2;
select t0.c1 from t0 left join t4 on t0.c1 = t4.c1 and t0.c1 > 1 start with t4.c1 is NULL connect by prior t0.c1 = t0.c2;
select t0.c1 from t0 left join t4 on t0.c1 = t4.c1 start with t0.pk != 0 connect by prior t0.c1 = t0.c2;
select t0.c1 from t0 left join t4 on t0.c1 > t4.c1 start with t0.pk != 0 and t4.c1 is NULL connect by prior t0.c1 = t0.c2;

###  三表测试 ###
select t0.c1 from t0, t4 , t7 start with t0.c1 = 1 connect by prior t0.c1 = t0.c2;
select t0.c1 from t0, t4 , t7 start with t0.pk = 1 connect by prior t0.c1 = t0.c2;

### 测试将start with expression 下降到不同位置 ###

# single table #
select t0.c1 from t0 start with t0.c1 = 1 connect by  1 = 1;
select t0.c1 from t0 start with t0.pk = 1 connect by  1 = 1;
select t0.c1 from t0 start with t0.c1  connect by  1 = 1;
select t0.c1 from t0 start with t0.c1 + t0.c2 connect by  1 = 1;
select t0.c1 from t0 start with (select c1 from t7) connect by prior 1 = 1;
select t0.c1 from t0 start with t0.c1 > (select c1 from t7) connect by prior 1 = 1;
## select t0.c1 from t0 start with t0.c1 > (select c1 from t7 where t0.c1 = t7.c1) connect by prior 1 = 1;

# subquery #
select c1 from (select * from t0) as tt start with c1 = 1 connect by 1 = 1;
select c1 from (select * from t0) as tt start with pk = 1 connect by 1 = 1;
select c1 from (select * from t0) as tt start with c1 + c2 connect by 1 = 1;

# multi table #
select t0.c1 from t0 join t7 start with t0.c1 = 1 connect by 1 = 1;
select t0.c1 from t0 join t7 start with t7.c1 = 1 connect by 1 = 1;
select t0.c1 from t0 join t7 start with t0.c1 + t7.c1 connect by 1 = 1;
## select t0.c1 from t0 join t7 start with t0.c1 > (select c1 from t8 where t7.c1 = t8.c1) connect by 1 = 1;
select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 start with t0.c1 = 1 connect by 1 = 1;
select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 start with t7.c1 = 1 connect by 1 = 1;
select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 start with t0.c1 + t7.c1 connect by 1 = 1;
## select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 start with t0.c1 > (select c1 from t8 where t7.c1 = t8.c1) connect by 1 = 1;

###################### test for connect by expression ####################################

### simple ###
select * from t7 connect by prior 1 = 1;
select * from t7 connect by prior c1 = c2;
select * from t7 connect by prior c1 + c2;

### column expr from on relation ###
select * from t7 connect by prior c1;
select * from t7 connect by prior c1 > 1;
select * from t7 connect by prior c1 = 1;
select * from t7 connect by c1;
select * from t7 connect by c1 = 1;

### column expr from multi relation ###
select * from t7 connect by c1 + c2;
select * from t7 connect by prior c1 + c2;
select * from t7 connect by prior c1 > prior c2;
select * from t7 connect by prior c1 = c2;

### test for connect by with subuqery ###
## not support now ##
## select * from t7 connect by (select c1 from t0);
## select * from t7 connect by c1 = (select c1 from t0);
## select * from t7 connect by prior c1 = (select c1 from t0);
## select * from t7 connect by c1 > (select c1 from t0 where c1 > t7.c1);
## select * from t7 connect by c1 > (select c1 from t0 where c1 = prior t7.c1);

### test for multi table ###

# single column #
select t0.c1 from t0 join t7 connect by t7.c1 > 1;
select t0.c1 from t0 join t7 connect by t0.c1 > 1;
select t0.c1 from t0 join t7 connect by prior t7.c1 > 1;
select t0.c1 from t0 join t7 connect by prior t0.c1 > 1;
select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 connect by t7.c1 = 1;
select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 connect by t0.c1 = 1;
select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 connect by prior t7.c1 = 1;
select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 connect by prior t0.c1 = 1;

# multi column #
select t0.c1 from t0 join t7 connect by t0.c1 = t7.c1;
select t0.c1 from t0 join t7 connect by prior t0.c1 = prior t7.c1;
select t0.c1 from t0 join t7 connect by prior t0.c1 = t7.c1;
select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 connect by t0.c1 = t7.c1;
select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 connect by prior t0.c1 = prior t7.c1;
select t0.c1 from t0 left join t7 on t0.c1 = t7.c1 connect by prior t0.c1 = t7.c1;


############################ test for where conditoin ####################################

### 单表 ###
# simple #
select * from t7 where t7.c1 connect by prior c1 = c2;
select * from t7 where t7.c1 + t7.c2 connect by prior c1 = c2;
select * from t7 where t7.c1 > t7.c2 connect by prior c1 = c2;

# prior column #
select * from t7 where prior t7.c1 connect by prior c1 = c2;
select * from t7 where prior t7.c1 + t7.c2 connect by prior c1 = c2;
select * from t7 where t7.c1 > prior t7.c2 connect by prior c1 = c2;
select * from t7 where prior (t7.c1 + t7.c2) connect by prior c1 = c2;

# subquery filter #
select * from t7 where (select c1 from t0) connect by prior c1 = c2;
select * from t7 where c1 > (select c1 from t0) connect by prior c1 = c2;
select c1 + prior a3 > (select c1 from t7) from t0 connect by prior c1 = c2;
select prior c1 + 1 > (select c1 from t7) from t0 connect by prior c1 = c2;
## select * from t7 where c1 > (select c1 from t0 where t0.c1 > t7.c1) connect by prior c1 = c2;

### 多表 ###
# simpile #
select t0.c1 from t0 join t7 where t0.c1 connect by prior t0.c1 = t7.c1;
select t0.c1 from t0 join t7 where t0.c1 + t7.c1 connect by prior t0.c1 = t7.c1;
select t0.c1 from t7 left join t0 on t7.c1 = t0.c1 where t0.c1 is NULL connect by prior t0.c1 = t7.c1;

# prior #
select t0.c1 from t0 join t7 where prior (t0.c1 + t7.c1) connect by prior t0.c1 = t7.c1;
select t0.c1 from t7 left join t0 on t7.c1 = t0.c1 where prior t0.c1 is NULL connect by prior t0.c1 = t7.c1;

############################# test for output expr #######################################

### 单表测试 ###
# same output #
select prior c2, c1 from t0 start with c1 = 1 connect by prior c1 = c2;
select prior c2, c2 from t0 connect by prior (c1 + 1) = (c1 + 1);
select prior (c2 + 1), (c2 + 1) from t0 connect by prior c1 = c1;
select prior 1 from t0 where prior (c2 + 1) = (c2 + 1) connect by prior c1 = c1;
select prior 1 from t0 where prior (c2 + 1) = (c2 + c1) connect by prior c1 = c1;

# overlap output #
select prior pk from t0 where c1 = 0 start with c1 = 1 connect by prior c1 = c2;

# different output #
select prior a3, pk from t0 start with c1 = 1 connect by prior c1 = c2;

# complex output #
select c1 + c2 from t0 where prior c2 = c1 connect by prior c1 = c2;
select prior(c1 + c2) from t0 where prior c2 = c1 connect by prior c1 = c2;
select abs(a3) + a3 from t0 where prior c2 = c1 connect by prior c1 = c2;
select prior (abs(a3) + a3) from t0 where prior c2 = c1 connect by prior c1 = c2;

select c1 + c2 from t0 where prior c2 < c1 connect by prior c1 > c2;
select prior(c1 + c2) from t0 where prior c2 < c1 connect by prior c1 > c2;
select prior(c1 + c2), (c1 + c2) from t0 where prior c2 < c1 connect by prior c1 > c2;
select abs(a3) + a3 from t0 where prior c2 < c1 connect by prior c1 > c2;
select prior (abs(a3) + a3) from t0 where prior c2 < c1 connect by prior c1 > c2;

### 多表测试 ###

# same output #
select prior t9.c3, t0.pk from t0 join t9 connect by prior t0.pk = t9.c3;
select prior t0.a3, t0.pk from t0 join t9 connect by prior t0.pk = t0.a3;

# overlap output #
select prior t9.c3, t0.pk, prior t0.c2  from t0 join t9 where t9.c1 > 1 connect by prior t0.pk = t9.c3;
select t9.c1 from t0 join t9 where prior t9.c3 + t0.pk + prior t0.c2 connect by prior t0.pk = t9.c3;

# different output #
select prior t0.c1, t9.c2 from t0 join t9 connect by prior t0.pk = t9.c3;
select prior t0.c1 from t0 join t9 where t9.c2 > 1 connect by prior t0.pk = t9.c3;
select t0.c1, prior t9.c2 from t0 join t9 connect by prior t0.pk = t9.c3;
select t0.c1 from t0 join t9 where prior t9.c2 > 1 connect by prior t0.pk = t9.c3;

# complex output #
select prior (t0.c1 + t0.c2) from t0 join t9 where t0.pk > prior t9.c3 connect by prior t0.pk = t9.c3;
select prior (t0.c1 + t9.c2) from t0 join t9 where t0.pk > prior t9.c3 connect by prior t0.pk = t9.c3;
select t0.c1 + t0.c2 from t0 join t9 where t0.pk > prior t9.c3 connect by prior t0.pk = t9.c3;
select t0.c1 + t9.c2 from t0 join t9 where t0.pk > prior t9.c3 connect by prior t0.pk = t9.c3;

############################# test for pseudo column #######################################
select c1, level from t0 start with pk = 1 connect by prior 1 = 1;
select c1, level + 1 from t0 start with pk = 1 connect by prior 1 = 1;
select c1 from t0 where level < 1 start with pk = 1 connect by prior 1 = 1;
select c1, level from t0 where level < 1 start with pk = 1 connect by prior 1 = 1;
select c1, level from t0 where level < 1 start with pk = 1 connect by prior 1 = 1;
select c1, level from t0 where abs(level) != level start with pk = 1 connect by prior 1 = 1;
select max(level) from t0 start with pk = 1 connect by prior 1 = 1;
select max(level + 1) from t0 start with pk = 1 connect by prior 1 = 1;
select c1 from t0 where level connect by prior c1 = c2;
select c1 from t0 where level > 1 and level < 2 connect by prior c1 = c2;
select c1 from t0 where level > 1 or level < 2 connect by prior c1 = c2;
select c1 from t0 where level > 1 or level < 2 and level < 2*level connect by prior c1 = c2;
select c1 from t0 where level > level * c2 or level < c1 connect by prior c1 = c2;
## bug : select max(c1 + 1) from t0 start with pk = 1 connect by prior 1 = 1;
## bug : select max(c1) from t0 connect by prior 1 = 1;

select c1, connect_by_isleaf from t0 start with pk = 1 connect by prior 1 = 1;
select c1, connect_by_isleaf + 1 from t0 start with pk = 1 connect by prior 1 = 1;
select c1 from t0 where connect_by_isleaf < 1 start with pk = 1 connect by prior 1 = 1;
select c1, connect_by_isleaf from t0 where connect_by_isleaf < 1 start with pk = 1 connect by prior 1 = 1;
select c1, connect_by_isleaf from t0 where connect_by_isleaf < 1 start with pk = 1 connect by prior 1 = 1;
select c1, connect_by_isleaf from t0 where abs(connect_by_isleaf) != connect_by_isleaf start with pk = 1 connect by prior 1 = 1;
select max(connect_by_isleaf) from t0 start with pk = 1 connect by prior 1 = 1;
select max(connect_by_isleaf + 1) from t0 start with pk = 1 connect by prior 1 = 1;
select c1 from t0 where connect_by_isleaf connect by prior c1 = c2;
select c1 from t0 where connect_by_isleaf > 1 and connect_by_isleaf < 2 connect by prior c1 = c2;
select c1 from t0 where connect_by_isleaf > 1 or connect_by_isleaf < 2 connect by prior c1 = c2;
select c1 from t0 where connect_by_isleaf > 1 or connect_by_isleaf < 2 and connect_by_isleaf < 2*connect_by_isleaf connect by prior c1 = c2;
select c1 from t0 where connect_by_isleaf > connect_by_isleaf * c2 or connect_by_isleaf < c1 connect by prior c1 = c2;

select c1, connect_by_iscycle from t0 start with pk = 1 connect by nocycle prior 1 = 1;
select c1, connect_by_iscycle + 1 from t0 start with pk = 1 connect by nocycle prior 1 = 1;
select c1 from t0 where connect_by_iscycle < 1 start with pk = 1 connect by nocycle prior 1 = 1;
select c1, connect_by_iscycle from t0 where connect_by_iscycle < 1 start with pk = 1 connect by nocycle prior 1 = 1;
select c1, connect_by_iscycle from t0 where connect_by_iscycle < 1 start with pk = 1 connect by nocycle prior 1 = 1;
select c1, connect_by_iscycle from t0 where abs(connect_by_iscycle) != connect_by_iscycle start with pk = 1 connect by nocycle prior 1 = 1;
select max(connect_by_iscycle) from t0 start with pk = 1 connect by nocycle prior 1 = 1;
select max(connect_by_iscycle + 1) from t0 start with pk = 1 connect by nocycle prior 1 = 1;
select c1 from t0 where connect_by_iscycle connect by nocycle prior c1 = c2;
select c1 from t0 where connect_by_iscycle > 1 and connect_by_iscycle < 2 connect by nocycle prior c1 = c2;
select c1 from t0 where connect_by_iscycle > 1 or connect_by_iscycle < 2 connect by nocycle prior c1 = c2;
select c1 from t0 where connect_by_iscycle > 1 or connect_by_iscycle < 2 and connect_by_iscycle < 2*connect_by_iscycle connect by nocycle prior c1 = c2;
select c1 from t0 where connect_by_iscycle > connect_by_iscycle * c2 or connect_by_iscycle < c1 connect by nocycle prior c1 = c2;

############################## test for order siblings by #######################################
select c1, c2 from t0 connect by prior c1 = c2 order by c1 + 1;
select c1, c2 from t0 connect by prior c1 = c2 order by c1 + c2;
select c1, c2 from t0 connect by prior c1 = c2 order siblings by c1 + 1;
select c1, c2 from t0 connect by prior c1 = c2 order siblings by c1 + c2;

############################## test for order connect_by_root #######################################
select connect_by_root(c1) from t0 connect by prior c1 = c2;
select connect_by_root(c1 + 1) from t0 connect by prior c1 = c2;
select 1 + connect_by_root(c1) from t0 connect by prior c1 = c2;
select connect_by_root(1) from t0 connect by prior c1 = c2;
select connect_by_root(level + c1) from t0 connect by prior c1 = c2;

select c1 from t0 where connect_by_root(c1) > 1 connect by prior c1 = c2;
select c1 from t0 where connect_by_root(level) > c2 connect by prior c1 = c2;
select c1 from t0 where connect_by_root(1) > c2 connect by prior c1 = c2;
select c1 from t0 connect by prior c1 = c2 order by connect_by_root c1;

############################## test for order sys_connect_by_path #######################################
select sys_connect_by_path(c1, 1) from t0 connect by prior c1 = c2;
select sys_connect_by_path(c1 + 1, 1) from t0 connect by prior c1 = c2;
select 1 + sys_connect_by_path(c1, 'a') from t0 connect by prior c1 = c2;
select sys_connect_by_path(1, 'a') from t0 connect by prior c1 = c2;
select sys_connect_by_path(level + c1, 'abc') from t0 connect by prior c1 = c2;

select c1 from t0 where sys_connect_by_path(c1, 1) > 1 connect by prior c1 = c2;
select c1 from t0 where sys_connect_by_path(level, 'b') > c2 connect by prior c1 = c2;
select c1 from t0 where sys_connect_by_path(1, 'c') > c2 connect by prior c1 = c2;
select c1 from t0 connect by prior c1 = c2 order by sys_connect_by_path(c1, 'd');

############################## test for subquery #######################################
select * from t7 where c1 in (select c1 from t0 start with c1 = 0 connect by nocycle prior c1 = c2 order siblings by c1);
select * from t7 where c2 in (select c1 from t0 start with c1 = 0 and t7.c1 > 0 connect by nocycle prior c1 = c2);
select * from t7 where c2 in (select c1 from t0 start with c1 = 0 connect by nocycle prior c1 = c2);
select * from t7 where c2 in (select t7.c1 > 0 from t1 start with c1 = 0 and t7.c1 > 0 connect by nocycle prior c1 = c2);
select * from t0 where c2 in (select prior t0.c1 + 99 from t4 start with c1 = 0 connect by nocycle prior c1 = c2);
select * from t0 where c2 in (select t0.c1 + 99 from t4 start with c1 = 0 connect by nocycle prior c1 = c2);
select * from t0 where c2 in (select t0.c1 > t4.c1 from t4 start with c1 = 0 connect by nocycle prior c1 = c2);
