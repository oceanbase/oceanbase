#!/bin/sh
SRC_DIR=`pwd`/../../../../
count=50
resolve_cost=0
ddl_cost=0
local_host="`hostname --fqdn`"
local_ip=`host $local_host 2>/dev/null | awk '{print $NF}'`
function getTiming(){
  start=$1
  end=$2
  start_s=`echo $start | cut -d '.' -f 1`
  start_ns=`echo $start | cut -d '.' -f 2`
  end_s=`echo $end | cut -d '.' -f 1`
  end_ns=`echo $end | cut -d '.' -f 2`
  time_micro=$(( (10#$end_s-10#$start_s)*1000000 + (10#$end_ns/1000 - 10#$start_ns/1000) ))
  time_ms=`expr $time_micro/1000  | bc `
  #echo "$time_micro microseconds"
  echo $time_ms
}

function build(){
  cd $SRC_DIR/rpm
  source /etc/profile
  dep_create oceanbase
  cd $SRC_DIR
  make distclean
  ./build.sh init
  ./configure --with-release --with-coverage=no --enable-buildtime=no --enable-strip-ut=no --enable-silent-rules --enable-dlink-observer=yes || return 2
  cd $SRC_DIR/src
  make -j20 || return 2
  cd $SRC_DIR/unittest/sql
  make resolver/ddl_resolver || return 2
}

function genTestsql(){
  cd $SRC_DIR/unittest/share/schema/gen_ddl_sql
  rm -rf trade.sql
  for ((i=1;i<=$count;i++))
  do
    sed "s/000/$i/" $1 >>  trade.sql
  done
  mv -f trade.sql $SRC_DIR/unittest/sql/resolver/sql/test_resolver_trade.test
}

function genDmlTestsql(){
  cd $SRC_DIR/unittest/share/schema/gen_ddl_sql
  rm -rf dml.sql
  cat ddl.test >> dml.sql
  for ((i=1;i<=$count;i++))
  do
    cat dml.test >> dml.sql
  done
  mv -f dml.sql $SRC_DIR/unittest/sql/resolver/sql/test_resolver_dml.test
}
function testResolve(){
  cd $SRC_DIR/unittest/sql/resolver
  resolve_cost=`./ddl_resolver -c $1 | grep -r 'use time' | awk -F ':' '{print $2}'`
  echo $resolve_cost
}

function testDDL(){
  cd $SRC_DIR/tools/deploy
  ./copy.sh
  echo -e "data_dir = '~/data'\ngcfg['_verbose_'] = False\nObCfg.init_config['trace_log_slow_query_watermark']='100ms'\nob2 = OBI(server_spec='$local_ip@zone1')" > config2.py
  ./deploy.py ob2.reboot
  begin_time=`date +%s.%N`
  ./deploy.py ob2.obmysql < $SRC_DIR/unittest/sql/resolver/sql/test_resolver_trade.test
  end_time=`date +%s.%N`
  ddl_cost=`getTiming $begin_time $end_time`
  ./deploy.py ob2.force_stop
  echo $ddl_cost
}
genTestsql trade10.sql
case "$1" in
build)
  build
  ;;
resolve)
  #resolve_cost=`testResolve`
  testResolve trade
  all_tables=`expr $count*59  | bc`
  ave_time=`echo "scale=4;$resolve_cost/$all_tables" | bc`
  echo -e "\033[31m ===================================== \033[0m"
  echo -e "\033[31m all_tables   cost_all_time   ave_time \033[0m"
  echo -e "\033[31m $all_tables   $resolve_cost us   $ave_time us \033[0m"
  echo -e "\033[31m ===================================== \033[0m"
  genDmlTestsql
  testResolve dml
  all_dml_sqls=`expr $count*591 | bc`
  all_sqls=`expr $all_dml_sqls+69 | bc`
  ave_time=`echo "scale=4;$resolve_cost/$all_sqls" | bc`
  echo -e "\033[31m ===================================== \033[0m"
  echo -e "\033[31m all_sqls   cost_all_time   ave_time \033[0m"
  echo -e "\033[31m $all_sqls    $resolve_cost us   $ave_time us \033[0m"
  ;;
ddl)
  testDDL
  all_tables=`expr $count*59  | bc`
  ave_time=`echo "scale=4;$ddl_cost$all_tables" | bc`
  echo -e "\033[31m ===================================== \033[0m"
  echo -e "\033[31m all_tables   cost_all_time   ave_time \033[0m"
  echo -e "\033[31m $all_tables   $resolve_cost ms   $ave_time ms \033[0m"
  echo -e "\033[31m ===================================== \033[0m"
  ;;
*)
  build
  count=600
  genTestsql create.sql
  all_time=`testResolve trade`
  all_create_tables=`expr $count*30  | bc`
  ave_create_time=`echo "scale=4;$all_time/$all_create_tables" | bc`
  count=400
  genTestsql trade10.sql
  testResolve trade
  all_tables_resolve=`expr $count*59  | bc`
  ave_time_resolve=`echo "scale=4;$resolve_cost/$all_tables_resolve" | bc`
  count=100
  genDmlTestsql
  all_dml_time=`testResolve dml`
  all_dml_sqls=`expr $count*591 | bc`
  all_sqls=`expr $all_dml_sqls+69 | bc`
  ave_dml_time=`echo "scale=4;$all_dml_time/$all_sqls" | bc`
  count=50
  genTestsql trade10.sql
  testDDL
  all_tables_ddl=`expr $count*59  | bc`
  ave_time_ddl=`echo "scale=4;$ddl_cost/$all_tables_ddl" | bc`
  echo  "========" `date '+%Y-%m-%d %H:%M:%S'` "=========" >> ~/result
  echo "$all_create_tables    $all_time us    $ave_create_time us" >> ~/result
  echo "$all_tables_resolve    $resolve_cost us    $ave_time_resolve us" >> ~/result
  echo "$all_sqls    $all_dml_time us    $ave_dml_time us" >> ~/result
  echo "$all_tables_ddl    $ddl_cost ms    $ave_time_ddl ms" >> ~/result
  echo -e "\033[31m ===========create cost============== \033[0m"
  echo -e "\033[31m all_tables   cost_all_time   ave_time \033[0m"
  echo -e "\033[31m $all_create_tables    $all_time us    $ave_create_time us \033[0m"
  echo -e "\033[31m ===========resolve cost============== \033[0m"
  echo -e "\033[31m all_tables   cost_all_time   ave_time \033[0m"
  echo -e "\033[31m $all_tables_resolve    $resolve_cost us    $ave_time_resolve us \033[0m"
  echo -e "\033[31m ===================================== \033[0m"
  echo -e "\033[31m ===========dml resolve cost============== \033[0m"
  echo -e "\033[31m all_sqls   cost_all_time   ave_time \033[0m"
  echo -e "\033[31m $all_sqls    $all_dml_time us    $ave_dml_time us \033[0m"
  echo -e "\033[31m =============ddl cost================ \033[0m"
  echo -e "\033[31m all_tables   cost_all_time   ave_time \033[0m"
  echo -e "\033[31m $all_tables_ddl    $ddl_cost ms    $ave_time_ddl ms \033[0m"
  echo -e "\033[31m ===================================== \033[0m"
  ;;
esac
