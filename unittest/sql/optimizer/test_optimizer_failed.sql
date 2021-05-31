#No match partitions
insert into t1 partition (p0) values(3, 2);
update set t1 partition(p0) set c2 = 1 where c1 = 3;
#Unkonw partition
insert into t1 partition (p9) values(3, 2);
insert into t1 partition (p3, p10) values(3, 2);
insert into t1 values (1,2) on duplicate key update c1 = c1+1;

#update can not cross partition
#update t1 partition(p0) set c1 = 1;
update t1 set c1 = 2 where c1 = 3;
update t5 set c2 = 2 where c2 = 2 and c3 = 3;
update t1 set c1 = c2 where c1 = 3;
insert into t10 values (1,2, 3) on duplicate key update c2 = c2+1;
select * from ts partition(p1sp0, p0spp0);
select * from ts partition(p1s0, p0sp0);
