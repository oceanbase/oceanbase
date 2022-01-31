#!/bin/bash
curr_version=$1
dest_version=$2
path=$3

cd $path
# modify CMakeList.txt
sed -i "s/VERSION $curr_version/VERSION $dest_version/g" CMakeLists.txt

# modify min_observer_version
sed -i 's/min_observer_version, OB_CLUSTER_PARAMETER, '\"$curr_version\"'/min_observer_version, OB_CLUSTER_PARAMETER, '\"$dest_version\"'/g' src/share/parameter/ob_parameter_seed.ipp

# modifycluster_vesion
curr_version_temp=`echo $curr_version | sed 's/\.//g'`
curr_version_temp1=`echo $curr_version| sed 's/\./, /g'`
dest_version_temp=`echo $dest_version | sed 's/\.//g'`
dest_version_temp1=`echo $dest_version| sed 's/\./, /g'`
sed -i "/#define CLUSTER_VERSION_$curr_version_temp (oceanbase::common::cal_version($curr_version_temp1))/a\#define CLUSTER_VERSION_$dest_version_temp (oceanbase::common::cal_version($dest_version_temp1))" src/share/ob_cluster_version.h
sed -i "s/define CLUSTER_CURRENT_VERSION CLUSTER_VERSION_$curr_version_temp/define CLUSTER_CURRENT_VERSION CLUSTER_VERSION_$dest_version_temp/g" src/share/ob_cluster_version.h

# modify upgrade script
cd tools/upgrade
python reset_upgrade_scripts.py
cd -
sed -i "s/new_version = '$curr_version'/new_version = '$dest_version'/g" tools/upgrade/upgrade_post_checker.py

# modify mysqltest test set
sed -i "s/$curr_version/$dest_version/g" tools/deploy/mysql_test/r//mysql/system_variable.result


if [[ "$curr_version" > "2.2.70" ]]
then 
  # modify share/ob_upgrade_utils.h
  curr_version_temp1=`echo $curr_version| sed 's/\./, /g'`
  oldNum=`cat src/share/ob_upgrade_utils.h |grep 'static const int64_t CLUTER_VERSION_NUM' | awk -F ' = ' '{print $2}' | awk -F';' '{print $1}'`
  let newNum=oldNum+1
  sed -i "s/static const int64_t CLUTER_VERSION_NUM = $oldNum/static const int64_t CLUTER_VERSION_NUM = $newNum/g" src/share/ob_upgrade_utils.h
  sed -i "/DEF_SIMPLE_UPGRARD_PROCESSER($curr_version_temp1)/a\\DEF_SIMPLE_UPGRARD_PROCESSER($dest_version_temp1);" src/share/ob_upgrade_utils.h

  # modify share/ob_upgrade_utils.cpp
  temp=`echo $curr_version | sed 's/\./UL, /g'`
  curr_version_temp2=${temp}UL
  temp=`echo $dest_version | sed 's/\./UL, /g'`
  dest_version_temp2=${temp}UL
  sed -i "s/CALC_CLUSTER_VERSION($curr_version_temp2)/CALC_CLUSTER_VERSION($curr_version_temp2),/g" src/share/ob_upgrade_utils.cpp
  sed -i "/CALC_CLUSTER_VERSION($curr_version_temp2)/a\\  CALC_CLUSTER_VERSION($dest_version_temp2)   //$dest_version" src/share/ob_upgrade_utils.cpp
  sed -i "/INIT_PROCESSOR_BY_VERSION($curr_version_temp1)/a\\    INIT_PROCESSOR_BY_VERSION($dest_version_temp1);" src/share/ob_upgrade_utils.cpp

  ## modify tools/upgrade/special_upgrade_action_post.py
  sed -i "/= actions begin =/a\\  run_upgrade_job(conn, cur, \"$dest_version\")" tools/upgrade/special_upgrade_action_post.py
  cd tools/upgrade/
  python ./gen_upgrade_scripts.py
  cd -

  ##modify yml file
  num1=`echo $dest_version | awk -F'.' '{print $3}'`
  let num2=num1+1
  last_version=`echo $dest_version | sed "s/\(.*\)$num1/\1$num2/g"`
  sed -i "/$dest_version/a\\- version: $dest_version\n  can_be_upgraded_to:\n      - $last_version" tools/upgrade/oceanbase_upgrade_dep.yml
fi

# modify upgrade case
curr_version_temp1=`echo $curr_version | sed 's/\./_/g'`
echo $curr_version_temp1 >> tools/obtest/observer.list
num=`ls -l tools/obtest/t/compat_farm/*.test | wc -l`
if [[ $num = 1 ]]
then
  old_version=`ls tools/obtest/t/compat_farm/upgrade_from_*_lite.test | sed 's/tools\/obtest\/t\/compat_farm\/upgrade_from_\(.*\)_lite.test/\1/g'`
  mv tools/obtest/t/compat_farm/upgrade_from_${old_version}_lite.test tools/obtest/t/compat_farm/upgrade_from_${curr_version_temp1}_lite.test
  mv tools/obtest/r/compat_farm/upgrade_from_${old_version}_lite.result tools/obtest/r/compat_farm/upgrade_from_${curr_version_temp1}_lite.result
  sed -i s/version=$old_version/version=$curr_version_temp1/g tools/obtest/t/compat_farm/upgrade_from_${curr_version_temp1}_lite.test
  sed -i s/version=$old_version/version=$curr_version_temp1/g tools/obtest/r/compat_farm/upgrade_from_${curr_version_temp1}_lite.result
else
  echo "compat_farm has more than one case, please confirm!"
  exit 1 
fi 
