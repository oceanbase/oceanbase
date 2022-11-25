## 测试点：
## 0.计划层级结构是否正确
## 1.columnv expr是否正确
## 2.output expr是否正确

######### normal test ###############
## test for update clause
merge into targetTable using sourceTable on (targetTable.id = sourceTable.id) when matched then update set targetTable.sales = sourceTable.sales;

## test for insert clause
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales);

######### test condition ###############
## test for match condition
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id and t1.id != t2.sales) when matched then update set t1.sales = t2.sales;

## test for update condition
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id and t1.id != t2.sales) when matched then update set t1.sales = t2.sales where t1.sales > 1;

## test for delete condition
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id and t1.id != t2.sales) when matched then update set t1.sales = t2.sales where t1.sales > 88 delete where t1.id < 99;

## test for insert condition
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales) where t2.id > 0;

## test for full condition
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when matched then update set t1.sales = t2.sales where t1.id = 999 delete where t1.sales =888 when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales) where t2.id = 777;

######### test for column conv ###############

### test update column conv #####
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id and t1.id != t2.sales) when matched then update set t1.sales = 1;

### test insert column conv ######
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when not matched then insert(t1.id, t1.sales) values(1, 1);
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when not matched then insert(t1.id) values(t2.id);

######### test for hidden rowkey ###############
merge into target1 using sourceTable on (target1.id = sourceTable.id) when matched then update set target1.c2 = sourceTable.sales;
merge into target2 using sourceTable on (target2.id = sourceTable.id) when matched then update set target2.c2 = sourceTable.sales;

################## test for subquery  ##################
merge into targetTable t1 using (select * from sourceTable) t2 on (t1.id = t2.id) when matched then update set t1.sales = t2.sales where t1.id = 999 delete where t1.sales =888 when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales) where t2.id = 777;
