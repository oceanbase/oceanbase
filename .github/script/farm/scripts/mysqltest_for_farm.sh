#!/bin/sh

set -x

export HOME="${_CONDOR_JOB_IWD:-${HOME:-${GITHUB_WORKSPACE}/farm_home_${SLICE_IDX:-0}}}"
mkdir -p "$HOME"
if [[ -n "${GITHUB_WORKSPACE:-}" ]]; then
    rm -rf "$HOME/oceanbase"
    ln -s "$GITHUB_WORKSPACE" "$HOME/oceanbase"
    [[ -x "$GITHUB_WORKSPACE/observer" && ! -e "$HOME/observer" ]] && ln -sfn "$GITHUB_WORKSPACE/observer" "$HOME/observer"
    [[ -x "$GITHUB_WORKSPACE/obproxy" && ! -e "$HOME/obproxy" ]] && ln -sfn "$GITHUB_WORKSPACE/obproxy" "$HOME/obproxy"
fi
export PATH="${PATH:-/bin:/usr/bin}:/usr/local/bin"
export USER=$(whoami)

HOST=`hostname -i`
DOWNLOAD_DIR="${DOWNLOAD_DIR:-$HOME/downloads}"
SLOT_ID="${SLOT_ID:-`echo ${_CONDOR_SLOT:-slot${SLICE_IDX:-0}} | cut -c5-`}"

function prepare_config {
    if [[ "$MINI" == "1" ]] || ([[ "$MINI" == "-1" ]] && [[ -f $HOME/oceanbase/tools/deploy/enable_mini_mode ]])
    then
        [[ -f $HOME/oceanbase/tools/deploy/enable_mini_mode ]] && MINI_SIZE=$(cat $HOME/oceanbase/tools/deploy/enable_mini_mode)
        [[ $MINI_SIZE =~ ^[0-9]+G ]] || MINI_SIZE="8G"
        MINI_CONFIG_ITEM="ObCfg.init_config['memory_limit']='$MINI_SIZE'"
    fi


    cd $HOME/oceanbase/tools/deploy
    if [ "$WITH_PROXY" ] && [ "$WITH_PROXY" != "0" ]
    then
        if [ "$CLUSTER_SPEC" == '2x1' ]
        then
            SPEC="[$HOST,proxy@$HOST]@zone1 [$HOST]@zone2"
        else
            SPEC="[$HOST,proxy@$HOST]@zone1"
        fi
    else
        if [[ "$CLUSTER_SPEC" == '2x1' ]] || [[ "$SLAVE" == "1" ]]
        then
            SPEC="[$HOST]@zone1 [$HOST]@zone2"
        else
            SPEC="[$HOST]@zone1"
        fi
    fi
    # farm中由于机器变小了，需要设置下
    CPU_COUNT_CONFIG="ObCfg.init_config['cpu_count']='24'"
    cat >config7.py <<EOF
home='$HOME'
data_dir='$HOME/data'
port_gen = itertools.count(5000 + $SLOT_ID * 100)
$USER = OBI(
    server_spec='$SPEC',
    is_local=True,
    proxy_cfg_key='$USER_\${local_ip}_proxy_$SLOT_ID',
    cfg_key='$USER_\${local_ip}_rslist_$SLOT_ID')
$MINI_CONFIG_ITEM
$CPU_COUNT_CONFIG
EOF
}

function prepare_bin {
    mkdir -p $DOWNLOAD_DIR || return 1

    cd $HOME
    if [ "$WITH_PROXY" ] && [ "$WITH_PROXY" != "0" ]
    then
        # 先从$HOME目录获取
        if [[ -x $HOME/obproxy ]]
        then
            echo "从根目录读取obproxy..."
            mv obproxy $DOWNLOAD_DIR/ || return 3
        elif [[ -x $HOME/oceanbase/tools/obproxy/obproxy ]]
        then
            echo "从ob代码库获取obproxy..."
            cp $HOME/oceanbase/tools/obproxy/obproxy $DOWNLOAD_DIR/ || return 1
        else
            (
                cd $DOWNLOAD_DIR &&
                    wget http://11.166.86.153:8877/obproxy.release -O obproxy &&
                    chmod +x obproxy
            ) || return 2
            echo "从8877获取obproxy..."
        fi
    fi

    if [ ! -x observer ]
    then
        (
            cd $DOWNLOAD_DIR &&
                wget http://11.166.86.153:8877/observer -O observer &&
                chmod +x observer
        ) || return 1
    else
    cp observer $DOWNLOAD_DIR/ || return 2
    fi

    if [ "$WITH_DEPS" ] && [ "$WITH_DEPS" != "0" ]
    then
        cd $HOME/oceanbase && ./build.sh init || return 3
    fi
    # ignore copy.sh failed
    cd $HOME/oceanbase/tools/deploy && [[ -f copy.sh ]] && (sh copy.sh || return 0)
}

function prepare_obs {
    cd $HOME
    if [ "$WITH_PROXY" ] && [ "$WITH_RPOXY" != "0" ]
    then
       mkdir -p $USER.proxy0/bin &&
           ln -st $USER.proxy0/bin $DOWNLOAD_DIR/obproxy || return 1
    fi

    OBS_CNT=2
    for ((i=0; i<$OBS_CNT; i++))
    do
        mkdir -p $HOME/$USER.obs$i/bin &&
            ln -st $HOME/$USER.obs$i/bin $DOWNLOAD_DIR/observer || return 1
    done
}

function prepare_env {
    prepare_bin || return 1
    prepare_config || return 2
    prepare_obs || return 3
}

function run_mysqltest {
    set -x
    slb=""
    [[ "$SLB" != "" ]] && slb="slb=$SLB,$EXECID"
    cd $HOME/oceanbase/tools/deploy
    mkfifo test.fifo
    if [ "$WITH_PROXY" ] && [ "$WITH_PROXY" != "0" ]
    then
        cat test.fifo | tee result.out & ./hap.py $USER.proxy0.mysqltest collect_all slices=$SLICES slice_idx=$SLICE_IDX $slb `echo $ARGV`  1> test.fifo 2>&1 
    elif [[ "$SLAVE" == "1" ]]
    then
	    cat test.fifo | tee result.out & ./hap.py $USER.obs1.mysqltest collect_all slices=$SLICES slice_idx=$SLICE_IDX $slb `echo $ARGV`  1> test.fifo 2>&1        
    else
        cat test.fifo | tee result.out & ./hap.py $USER.mysqltest collect_all slices=$SLICES slice_idx=$SLICE_IDX $slb `echo $ARGV`  1> test.fifo 2>&1 
    fi

    # check if there is error
    if [ "$?" == "0" ]
    then
       grep -E 'PASSED' $HOME/oceanbase/tools/deploy/result.out  > /dev/null && ! grep -E "FAIL LST" $HOME/oceanbase/tools/deploy/result.out  > /dev/null || return 1
    else
        return 1
    fi
}

function collect_log {
    cd $HOME/oceanbase/tools/deploy || return 1
    if [ -d "collected_log" ]
    then
        mv mysql_test/var/log/* collected_log
        mv $HOME/$USER.*/core[.-]* collected_log
        mv collected_log collected_log_$SLICE_IDX || return 1
        tar cvfz $HOME/collected_log_$SLICE_IDX.tar.gz collected_log_$SLICE_IDX
    fi
}

export -f run_mysqltest

function obd_prepare_obd {
    if [[ -f $HOME/oceanbase/deps/init/dep_create.sh ]]
    then
        if [[ "$IS_CE" == "1" ]]
        then
          cd $HOME/oceanbase && ./build.sh init --ce || return 3
        else  
          cd $HOME/oceanbase && ./build.sh init || return 3
        fi
    else
        if grep 'dep_create.sh' $HOME/oceanbase/build.sh
        then
            cd $HOME/oceanbase/deps/3rd && bash dep_create.sh all || return 4
        else
            cd $HOME/oceanbase && ./build.sh init || return 3
        fi
    fi
    
    $obd devmode enable
    $obd env set OBD_DEPLOY_BASE_DIR $HOME/oceanbase/tools/deploy
    $obd --version
    if [[ "$IS_CE" == "1" && ! -f $OBD_HOME/.obd/mirror/remote/taobao.repo ]]
    then
        mkdir -p $OBD_HOME/.obd/mirror/remote && echo -e "[taobao]\nname=taobao\nbaseurl=http://yum.tbsite.net/taobao/7/x86_64/current\nenabled=1\n\n[taobao-test]\nname=taobao-test\nbaseurl=http://yum.tbsite.net/taobao/7/x86_64/test\nenabled=0" > $OBD_HOME/.obd/mirror/remote/taobao.repo
    fi
}

export -f obd_prepare_obd

function obd_prepare_global {
    export LANG=en_US.UTF-8
    HOST=`hostname -i`
    DOWNLOAD_DIR=$HOME/downloads
    SLOT_ID="${SLOT_ID:-$(echo "${_CONDOR_SLOT:-slot${SLICE_IDX:-0}}" | cut -c5-)}"
    PORT_NUM=`expr 5000 + $SLOT_ID \* 100`

    # 根据传入的observer binary判断是否开源版
    if [[ `$HOME/observer -V 2>&1 | grep -E '(OceanBase CE|OceanBase_CE)'` ]]
    then
        COMPONENT="oceanbase-ce"
        export IS_CE=1
    else
        COMPONENT="oceanbase"
        export IS_CE=0
    fi
    # 根据build.sh中的内容判断依赖安装路径
    if grep 'dep_create.sh' $HOME/oceanbase/build.sh
    then
        DEP_PATH=$HOME/oceanbase/deps/3rd
    else
        DEP_PATH=$HOME/oceanbase/rpm/.dep_create/var
    fi

    if [[ -f "$HOME/oceanbase/tools/deploy/mysqltest_config.yaml" ]]
    then
        export WITH_MYSQLTEST_CONFIG_YAML=1
    else
        export WITH_MYSQLTEST_CONFIG_YAML=0
    fi

    ob_name=obcluster$SLOT_ID
    app_name=$ob_name.$USER.$HOST
    ocp_config_server='http://ocp-cfg.alibaba.net:8080/services?User_ID=alibaba&UID=test'
    proxy_cfg_url=${ocp_config_server}\&Action=GetObProxyConfig\&ObRegionGroup=$app_name
    cfg_url=${ocp_config_server}

    export obd=$DEP_PATH/usr/bin/obd
    export OBD_HOME=$HOME
    export OBD_INSTALL_PRE=$DEP_PATH
    export DATA_PATH=$HOME/data

}

function obd_prepare_config {
    
    MINI_SIZE="10G"
    if [[ "$MINI" == "1" ]] || ([[ "$MINI" == "-1" ]] && [[ -f $HOME/oceanbase/tools/deploy/enable_mini_mode ]])
    then
        [[ -f $HOME/oceanbase/tools/deploy/enable_mini_mode ]] && MINI_SIZE=$(cat $HOME/oceanbase/tools/deploy/enable_mini_mode)
        [[ $MINI_SIZE =~ ^[0-9]+G ]] || MINI_SIZE="8G"
    fi
    if [[ "$CLUSTER_SPEC" == '2x1' ]] || [[ "$SLAVE" == "1" ]]
    then
        mysql_port=$PORT_NUM && PORT_NUM=`expr $PORT_NUM + 1`
        rpc_port1=$PORT_NUM && PORT_NUM=`expr $PORT_NUM + 1`
        mysql_port2=$PORT_NUM && PORT_NUM=`expr $PORT_NUM + 1`
        rpc_port2=$PORT_NUM && PORT_NUM=`expr $PORT_NUM + 1`
        SERVERS=$(cat <<-EOF
  servers:
    - name: server1
      ip: 127.0.0.1
    - name: server2
      ip: 127.0.0.1
  server1:
    mysql_port: $mysql_port
    rpc_port: $rpc_port1
    home_path: $DATA_PATH/observer1
    zone: zone1
  server2:
    mysql_port: $mysql_port2
    rpc_port: $rpc_port2
    home_path: $DATA_PATH/observer2
    zone: zone2
EOF
)
    else
        mysql_port=$PORT_NUM && PORT_NUM=`expr $PORT_NUM + 1`
        rpc_port=$PORT_NUM && PORT_NUM=`expr $PORT_NUM + 1`
        SERVERS=$(cat <<-EOF
  servers:
    - name: server1
      ip: 127.0.0.1
  server1:
    mysql_port: $mysql_port
    rpc_port: $rpc_port
    home_path: $DATA_PATH/observer1
    zone: zone1
EOF
)
    fi
    export MYSQL_PORT=$mysql_port
    proxy_conf=""
    if [ "$WITH_PROXY" ] && [ "$WITH_PROXY" != "0" ]
    then
        listen_port=$PORT_NUM && PORT_NUM=`expr $PORT_NUM + 1`
        prometheus_listen_port=$PORT_NUM && PORT_NUM=`expr $PORT_NUM + 1`
        
        proxy_conf=$(cat $HOME/oceanbase/tools/deploy/obd/obproxy.yaml.template)
        if [[ -f $HOME/oceanbase/test/obproxy_test_config ]]
        then
            obproxy_switch=`sed '/^obproxy_switch=/!d;s/.*=//' $HOME/oceanbase/test/obproxy_test_config`

            # 当且仅当 obproxy 指定模式为wget时，影响mysqltest obproxy配置，否则mysqltest依旧沿用原有obproxy获取逻辑
                mkdir $HOME/bin
                if [[ -f $HOME/obproxy ]]
                then
                    cp $HOME/obproxy $HOME/bin
                else
                    wget_url=`sed '/^wget_url=/!d;s/.*=//' $HOME/oceanbase/test/obproxy_test_config`
                    wget $wget_url -O $HOME/bin/obproxy
                    if [[ -f $HOME/bin/obproxy ]]
                    then
                        echo "obproxy 文件下载成功"
                        chmod +x $HOME/bin/obproxy
                        
                    else
                        # 下载失败，沿用原有逻辑
                        echo "obproxy 文件下载失败"
                        return 1
                    fi
                fi
                
                echo "obrpxoy版本:"
                echo `$HOME/bin/obproxy -V 2>&1`

                obproxy_version=$("$HOME"/bin/obproxy --version  2>&1  | grep -E 'obproxy \(OceanBase [\.0-9]+ \S+\)' | grep -Eo '\s[.0-9]+\s')
                
                $obd mirror create -n obproxy -p $HOME -t "latest" -V $obproxy_version
                echo "执行结果: $?"

                proxy_conf=$(echo "$proxy_conf" | sed '/package_hash: [0-9a-z]*/d')
                proxy_conf="""$proxy_conf
  tag: latest
"""
        fi
        proxy_conf=${proxy_conf//'{{%% COMPONENT %%}}'/$COMPONENT}
        proxy_conf=${proxy_conf//'{{%% LISTEN_PORT %%}}'/$listen_port}
        proxy_conf=${proxy_conf//'{{%% PROMETHEUS_LISTEN_PORT %%}}'/$prometheus_listen_port}
        proxy_conf=${proxy_conf//'{{%% OBPORXY_HOME_PATH %%}}'/$DATA_PATH\/obproxy}
        proxy_conf=${proxy_conf//'{{%% OBPROXY_CONFIG_SERVER_URL %%}}'/$proxy_cfg_url}
    fi

    if [[ "$MINI" == "1" && -f $HOME/oceanbase/tools/deploy/obd/core-test.yaml.template ]]
    then
        conf=$(cat $HOME/oceanbase/tools/deploy/obd/core-test.yaml.template)
    else
        conf=$(cat $HOME/oceanbase/tools/deploy/obd/config.yaml.template)
    fi
    # cgroup support
    if [[ -f $HOME/oceanbase/tools/deploy/obd/.use_cgroup || $(echo " $ARGV " | grep " use-cgroup ") ]] 
    then
        CGROUP_CONFIG="cgroup_dir: /sys/fs/cgroup/cpu/$USER"
    fi
    # MYSQLRTEST_ARGS contains changes for special run
    proxy_conf="""$MYSQLRTEST_ARGS
    $CGROUP_CONFIG
$proxy_conf"""
    conf=${conf//'{{%% PROXY_CONF %%}}'/"$proxy_conf"}
    conf=${conf//'{{%% DEPLOY_PATH %%}}'/"$HOME/oceanbase/tools/deploy"}
    conf=${conf//'{{%% COMPONENT %%}}'/$COMPONENT}
    conf=${conf//'{{%% SERVERS %%}}'/$SERVERS}
    conf=${conf//'{{%% TAG %%}}'/'latest'}
    conf=${conf//'{{%% MINI_SIZE %%}}'/$MINI_SIZE}
    conf=${conf//'{{%% OBCONFIG_URL %%}}'/$cfg_url}
    conf=${conf//'{{%% APPNAME %%}}'/$app_name}
    conf=${conf//'{{%% EXTRA_PARAM %%}}'/$MYSQLRTEST_ARGS}
    if [[ "$IS_CE" == "1" ]]
    then
      conf=$(echo "$conf" | sed 's/oceanbase:/oceanbase\-ce:/g' | sed 's/\- oceanbase$/- oceanbase-ce/g')
    fi
    echo "$conf" > $HOME/oceanbase/tools/deploy/config.yaml

    if [[ $WITH_SHARED_STORAGE == "1" ]]
    then
        # shared_storage 需要在global修改相关的mode
        share_storage_workdir="$(date +%s)-$(uuidgen)"
        first_global_line_count=`sed -n '/global:/=' $HOME/oceanbase/tools/deploy/config.yaml | head -n 1`
        set +x
        sed -i "${first_global_line_count}a\    mode: 'shared_storage'\n\
    shared_storage: \"oss://oss-436751-0701-all-test/$FARM2_RUN_USER/$share_storage_workdir?host=oss-cn-hangzhou.aliyuncs.com&access_id=${SENSITIVE_TEST_OSS_ID_FOR_OBJECT_STORAGE_FOR_TOTAL}&access_key=${SENSITIVE_TEST_OSS_KEY_FOR_OBJECT_STORAGE_FOR_TOTAL}&max_iops=0&max_bandwidth=0B&scope=region\""  $HOME/oceanbase/tools/deploy/config.yaml
        sed -i "s/datafile_size: '20G'/datafile_size: '200G'/g" $HOME/oceanbase/tools/deploy/config.yaml
        set -x
    fi

    if [[ $SHARED_STORAGE_MODE == "1" ]]
    then
        # shared_storage 需要在global修改相关的mode
        MINIO_IP="100.88.99.213"
        if [[ $TEST_NAME == "farm2" ]]
        then
            MINIO_IP="100.88.99.213"
        fi
        share_storage_workdir="$(date +%s)-$(uuidgen)"
        first_global_line_count=`sed -n '/global:/=' $HOME/oceanbase/tools/deploy/config.yaml | head -n 1`
        sed -i "${first_global_line_count}a\    mode: 'shared_storage'\n\
    shared_storage: \"s3://farm-test/mysqltest/$share_storage_workdir/clog_and_data?host=http://$MINIO_IP:9000&access_id=minioadmin&access_key=minioadmin&s3_region=us-east-1&max_iops=10000&max_bandwidth=1GB&scope=region\""  $HOME/oceanbase/tools/deploy/config.yaml
    fi

    if [[ ! $WITH_SHARED_STORAGE == "1" ]]
    then
        cat $HOME/oceanbase/tools/deploy/config.yaml
    fi
}
function get_baseurl {
    repo_file=$OBD_HOME/.obd/mirror/remote/taobao.repo
    start=0
    end=$(cat $repo_file | wc -l )
    for line in $(grep -nE "^\[.*\]$" $repo_file)
    do
      num=$(echo "$line" | awk -F ":" '{print $1}')
      [ "$find_section" == "1" ] && end=$num
      is_target_section=$(echo $(echo "$line" | grep -E "\[\s*$repository\s*\]"))
      [ "$is_target_section" != "" ] && start=$num && find_section='1'
    done
    repo_section=$(awk "NR>=$start && NR<=$end" $repo_file)
    baseurl=$(echo "$repo_section" | grep baseurl | awk -F '=' '{print $2}')
}

function get_version_and_release {
    start=0
    end=$(cat $config_yaml | wc -l)
    for line in $(grep -nE '^\S+:' $config_yaml)
    do
      num=$(echo "$line" | awk -F ":" '{print $1}')
      [ "$find_obproxy" == "1" ] && end=$num
      [ "$(echo "$line" | grep obproxy)" != "" ] && start=$num && find_obproxy='1'
    done
    odp_conf=$(awk "NR>=$start && NR<=$end" $config_yaml)
    version=$(echo "$odp_conf" | grep version | awk '{print $2}')
    release=$(echo "$odp_conf" | grep release | awk '{print $2}')
    include_file=$DEPLOY_PATH/$(echo "$odp_conf"| grep 'include:'| awk '{print $2}')
    if [[ -f $include_file ]]
    then
      _repo=$(echo $(grep '__mirror_repository_section_name:'  $include_file | awk '{print $2}'))
      [ "$_repo" != "" ] && repository=$_repo 
      version=$(echo $(grep 'version:' $include_file | awk '{print $2}'))
      release=$(echo $(grep 'release:' $include_file | awk '{print $2}'))
    fi
}

function get_obproxy {
    repository="taobao"
    config_yaml=$HOME/oceanbase/tools/deploy/config.yaml
    DEPLOY_PATH=$HOME/oceanbase/tools/deploy
    obproxy_mirror_repository=$(echo $(grep '__mirror_repository_section_name' $config_yaml | awk -F':' '{print $2}'))
    [ "$obproxy_mirror_repository" != "" ] && repository=$obproxy_mirror_repository

    get_version_and_release
    get_baseurl
    $obd mirror disable remote

    if [[ "$baseurl" != "" && "$version" != "" && "$release" != "" ]]
    then
      OS_ARCH="$(uname -m)"
      if [[ $OS_ARCH == "aarch64" ]]
      then
          pkg_name="obproxy-$version-$release.aarch64.rpm"
      else
          pkg_name="obproxy-$version-$release.x86_64.rpm"
      fi
      if [ "$(find $OBD_HOME/.obd/mirror/local -name $pkg_name)" == "" ]
      then
        download_dir=$OBD_HOME/.obd_download
        mkdir -p $download_dir
        wget $baseurl/obproxy/$pkg_name -O "$download_dir/$pkg_name" -o $download_dir/obproxy.down
        $obd mirror clone "$download_dir/$pkg_name" && rm -rf $download_dir && return 0
      else
        return 0
      fi
    fi
    echo "Download pkg failed. Try to get obproxy from repository $repository"
    count=0
    interval=5
    retry_limit=100
    while (( $count < $retry_limit ))
    do
    $obd mirror disable remote
    $obd mirror enable $repository
    $obd mirror update && $obd mirror list $repository > /dev/null && return 0
    count=`expr $count + 1`
    (( $count <= $retry_limit )) && sleep $interval
    done 
    return 1
}

function obd_prepare_bin {
    cd $HOME
    mkdir -p $DOWNLOAD_DIR/{bin,etc,admin} || return 1

    if [ "$WITH_PROXY" ] && [ "$WITH_PROXY" != "0" ]
    then
        get_obproxy || (echo "failed to get obdproxy" || return 1)
    fi

    if [ ! -x observer ]
    then
        (
            cd $DOWNLOAD_DIR/bin &&
                wget http://11.166.86.153:8877/observer -O observer &&
                chmod +x observer
        ) || return 1
    else
        cp observer $DOWNLOAD_DIR/bin/ || return 2
    fi
    
    cd $HOME/oceanbase/tools/deploy
    cp $DEP_PATH/u01/obclient/bin/obclient ./obclient && chmod 777 obclient
    cp $DEP_PATH/u01/obclient/bin/mysqltest ./mysqltest && chmod 777 mysqltest
    
    if [ "$WITH_DEPS" ] && [ "$WITH_DEPS" != "0" ]
    then
        cd $HOME/oceanbase/tools/deploy && [[ -f copy.sh ]] && sh copy.sh
    fi
    if [[ -f "$HOME/oceanbase/tools/deploy/obd/.observer_obd_plugin_version" ]]
    then
        obs_version=$(cat $HOME/oceanbase/tools/deploy/obd/.observer_obd_plugin_version)
    else
        $DOWNLOAD_DIR/bin/observer -V
        obs_version=`$DOWNLOAD_DIR/bin/observer -V 2>&1 | grep -E "observer \(OceanBase([ \_]CE)? ([.0-9]+)\)" | grep -Eo '([.0-9]+)'`
    fi

    mkdir $HOME/oceanbase/tools/deploy/admin
    if [[ -d $HOME/oceanbase/src/share/inner_table/sys_package ]]
    then 
        cp $HOME/oceanbase/src/share/inner_table/sys_package/*.sql $HOME/oceanbase/tools/deploy/admin/
    fi

    $obd mirror create -n $COMPONENT -p $DOWNLOAD_DIR -t latest -V $obs_version || return 2

}

function obd_init_cluster {
    retries=$reboot_retries
    while (( $retries > 0 ))
    do
    if [[ "$retries" == "$reboot_retries" ]]
    then
        $obd cluster deploy $ob_name -c $HOME/oceanbase/tools/deploy/config.yaml -f && $obd cluster start $ob_name -f && $obd cluster display $ob_name
    else
        $obd cluster redeploy $ob_name -f
    fi
    retries=`expr $retries - 1`
    ./obclient -h 127.1 -P $MYSQL_PORT -u root -A -e "alter system set_tp tp_no = 509, error_code = 4016, frequency = 1;"
    ret=$?
    if [[ $ret == 0 ]]
    then
        init_files=('init.sql' 'init_mini.sql' 'init_for_ce.sql')
        for init_file in ${init_files[*]}
        do
            if [[ -f $HOME/oceanbase/tools/deploy/$init_file ]]
            then
                if ! grep "alter system set_tp tp_no = 509, error_code = 4016, frequency = 1;" $HOME/oceanbase/tools/deploy/$init_file
                then
                    echo -e "\nalter system set_tp tp_no = 509, error_code = 4016, frequency = 1;" >> $HOME/oceanbase/tools/deploy/$init_file
                fi
            fi
        done
    fi
    $obd test mysqltest $ob_name $SERVER_ARGS --mysqltest-bin=./mysqltest --obclient-bin=./obclient --init-only --init-sql-dir=$HOME/oceanbase/tools/deploy $INIT_FLIES --log-dir=$HOME/init_case $EXTRA_ARGS -v > ./init_sql_log && init_seccess=1 && break
    done
}

function is_case_selector_arg()
{
    local arg
    arg=$1
    case_selector_patterns=('test-set=*' 'testset=*' 'suite=*' 'tag=*' 'regress_suite=*' 'all' 'psmall')
    for p in ${case_selector_patterns[@]}
    do
      [[ "$(echo $arg | grep -Eo $p)" != "" ]] && return 0
    done
    return 1
}

function obd_run_mysqltest {
    set -x
    obd_prepare_global

    cd $HOME/oceanbase/tools/deploy
    if [[ "$SLB" != "" ]] && [[ "$EXECID" != "" ]]
    then
        SCHE_ARGS="--slb-host=$SLB --exec-id=$EXECID "
    else
        SCHE_ARGS="--slices=$SLICES --slice-idx=$SLICE_IDX "
    fi
    
    
    if [[ "$MINI" == "1" && -f core-test.init_mini.sql ]]
    then
        INIT_FLIES="--init-sql-files=core-test.init_mini.sql,init_user.sql|root@mysql|test"
        [ -f init_user_oracle.sql ] && INIT_FLIES="${INIT_FLIES},init_user_oracle.sql|SYS@oracle|SYS"
    else
        if [[ "$IS_CE" == "1" && -f $HOME/oceanbase/.ce ]]
        then
            INIT_FLIES="--init-sql-files=init_mini.sql,init_user.sql|root@mysql|test "
        fi
    fi
    CLUSTER_MODE="c"
    if [[ "$SLAVE" == "1" ]]
    then
    SERVER_ARGS="--test-server=server2"
    CLUSTER_MODE="slave"
    else
    SERVER_ARGS=""
    fi
    if [ "$WITH_PROXY" ] && [ "$WITH_PROXY" != "0" ]
    then
    CLUSTER_MODE="proxy"
    fi
    reboot_retries=2
    if [[ "$WITH_MYSQLTEST_CONFIG_YAML" == "1" ]]
    then
        EXTRA_ARGS="--test-mode=$CLUSTER_MODE "
    else
        EXTRA_ARGS="--cluster-mode=$CLUSTER_MODE "
    fi
    if [[  -f $HOME/oceanbase/tools/deploy/obd/.fast-reboot ]]
    then
        EXTRA_ARGS="${EXTRA_ARGS}--fast-reboot "
    fi 

    if [[ "$IS_CE" == "1" ]]
    then
        EXTRA_ARGS="${EXTRA_ARGS}--mode=mysql "
    fi

    EXTRA_ARGS_WITHOUT_CASE=$EXTRA_ARGS
    
    if [[ "$ARGV" != "" ]]
    then
        for arg in $ARGV
        do  
            [[ "${arg%%=*}" == "testset" ]] && arg="${arg/testset=/test-set=}"
            [[ "${arg%%=*}" == "reboot-timeout" ]] && has_reboot_timeout=1
            [[ "${arg%%=*}" == "reboot-retries" ]] && reboot_retries=${arg#*=}
            [[ "${arg%%=*}" == "disable-collect" ]] && disable_collect=1 && continue
            [[ "${arg%%=*}" == "use-cgroup" ]] && continue
            EXTRA_ARGS="${EXTRA_ARGS}--${arg} "
            if ! is_case_selector_arg "${arg}"
            then
              EXTRA_ARGS_WITHOUT_CASE="${EXTRA_ARGS_WITHOUT_CASE}--${arg} "
            fi
        done
    fi

    if [[ "$has_reboot_timeout" != "1" ]]
    then
        REBOOT_TIMEOUT="--reboot-timeout=600"
    fi
    if [[ "$disable_collect" != "1" ]]
    then
        COLLECT_ARG="--collect-all"
        if [[ "$WITH_MYSQLTEST_CONFIG_YAML" == "0" ]] && [ "$WITH_PROXY" ] && [ "$WITH_PROXY" != "0" ] && [[ -f "$HOME/oceanbase/tools/deploy/obd/.collect_proxy_log" ]]
        then
            COLLECT_ARG="$COLLECT_ARG --collect-components=$COMPONENT,obproxy"
        fi
    fi
    obd_init_cluster
    if [[ "$init_seccess" != "1" ]]
    then
        cat ./init_sql_log
        echo "Failed to init sql, see more log at ./collected_log/_test_init"
        mkdir -p $HOME/collected_log/_test_init/
        [[ -d "$DATA_PATH/observer1/log" ]] && mkdir -p $HOME/collected_log/_test_init/observer1 && mv $DATA_PATH/observer1/log/* $HOME/collected_log/_test_init/observer1
        [[ -d "$DATA_PATH/observer2/log" ]] && mkdir -p $HOME/collected_log/_test_init/observer2 && mv $DATA_PATH/observer2/log/* $HOME/collected_log/_test_init/observer2
        [[ -d "$DATA_PATH/obproxy/log" ]] && mkdir -p $HOME/collected_log/_test_init/obproxy && mv $DATA_PATH/obproxy/log/* $HOME/collected_log/_test_init/obproxy
        exit 1
    fi 
    if [[ "$SPECIAL_RUN" == "1" ]]
    then
        ret=255
        result_dir=$HOME/record
        mkdir -p $result_dir
        RECORD_ARGS="--record-dir=$result_dir --record --log-dir=./var/rec_log"
        RESULT_ARGS="--result-dir=$result_dir --result-file-suffix=.record --log-dir=./var/log"
        mysqltest_cmd="$obd test mysqltest $ob_name $SERVER_ARGS --mysqltest-bin=./mysqltest --obclient-bin=./obclient --init-sql-dir=$HOME/oceanbase/tools/deploy --special-run --sort-result $COLLECT_ARG $REBOOT_TIMEOUT $VERBOSE_ARG"
        # $mysqltest_cmd --all $INIT_FLIES $SCHE_ARGS $RECORD_ARGS 2>&1 > record.out
        $mysqltest_cmd $INIT_FLIES $SCHE_ARGS $RECORD_ARGS $EXTRA_ARGS 2>&1 | tee record.out  && ( exit ${PIPESTATUS[0]})
        rec_ret=$?
        cat record.out | grep -E '\|\s+(PASSED|FAILED)|s+\|'  > record_result
        TEST_CASES=`cat record_result | grep PASSED | awk '{print \$2}' | tr '\n' ',' | head -c -1`
        ret=0
        if [[ "$TEST_CASES" != "" ]]
        then
            if [[ "$SP_CHANGE" != "" ]]
            then
                arr=(${SP_CHANGE//,/ })
                for line in ${arr[@]}
                do
                MYSQLRTEST_ARGS=$MYSQLRTEST_ARGS$'\n'"    ${line%%=*}: ${line##*=}"
                done
                obd_prepare_config
                echo "config after change:"
                cat $HOME/oceanbase/tools/deploy/config.yaml
                $obd cluster destroy $ob_name && $obd cluster deploy $ob_name -c $HOME/oceanbase/tools/deploy/config.yaml && $obd cluster start $ob_name -f && $obd cluster display $ob_name
            else
                $obd cluster redeploy $ob_name
            fi
            obd_init_cluster
            $mysqltest_cmd $INIT_FLIES --test-set=$TEST_CASES --sp-hint="$SP_HINT" $RESULT_ARGS $EXTRA_ARGS_WITHOUT_CASE 2>&1 | tee compare.out && ( exit ${PIPESTATUS[0]})
            ret=$?
        else
            echo 'No case succeed?!'
        fi
        if [[ $JOBNAME == 'mysqltest_opensource' ]]
        then
            submarker="_opensource"
        else
            submarker=""
        fi
        mv record.out $HOME/mysqltest${submarker}_record_output.$SLICE_IDX
        mv compare.out $HOME/mysqltest${submarker}_compare_output${JOB_SUFFIX}.$SLICE_IDX
        echo "finish!"
        [[ "$rec_ret" != "0" ]] && return $rec_ret
        [[ "$ret" != "0" ]] && return $ret
        return 0
    elif [[ $CROSS_VALIDATION == "1"  ]]
    then
       ret=255
        # 先checkout commit
        # todo: 修改为把tools deploy下的init sql都带入过去
        mkdir $HOME/tmp_init_sql
        mkdir $HOME/obd_tmp
        find $HOME/oceanbase/tools/deploy -name 'init_*.sql' | xargs -i cp {} $HOME/tmp_init_sql
        cp $HOME/oceanbase/tools/deploy/obd/* $HOME/obd_tmp
        cd $HOME && tar -cvf tmp_init_sql.tar.gz $HOME/tmp_init_sql
        cd $HOME && tar -cvf origin_obd_file.tar.gz $HOME/obd_tmp

        cd $HOME/oceanbase
        # 针对多个分支进行处理
        ret=0
        for commit in $CROSS_VALIDATION_COMMIT
        do
            git checkout -f $commit || ret=2
            # 然后重新针对obd 进行重新构建路径
            cd $HOME && tar -xvf tmp_init_sql.tar.gz && cp tmp_init_sql/*  $HOME/oceanbase/tools/deploy
            cd $HOME && tar -xvf origin_obd_file.tar.gz && cp obd_tmp/*  $HOME/oceanbase/tools/deploy/obd

            if [[ -f $HOME/oceanbase/deps/init/dep_create.sh ]]
            then
                if [[ "$IS_CE" == "1" ]]
                then
                    cd $HOME/oceanbase && ./build.sh init --ce || ret=3
                else  
                    cd $HOME/oceanbase && ./build.sh init || ret=3
                fi
            else
                if grep 'dep_create.sh' $HOME/oceanbase/build.sh
                then
                    cd $HOME/oceanbase/deps/3rd && bash dep_create.sh all || ret=4
                else
                    cd $HOME/oceanbase && ./build.sh init || ret=4
                fi
            fi
            # 根据build.sh中的内容判断依赖安装路径
            if grep 'dep_create.sh' $HOME/oceanbase/build.sh
            then
                DEP_PATH=$HOME/oceanbase/deps/3rd
            else
                DEP_PATH=$HOME/oceanbase/rpm/.dep_create/var
            fi
            extra_ignore_cmd=""
            if [[ `$DEP_PATH/u01/obclient/bin/mysqltest --help | grep "cmp-ignore-explain"` ]]
            then
                extra_ignore_cmd=" --ignore-explain=enable"
            fi

            if [[ `$DEP_PATH/u01/obclient/bin/mysqltest --help | grep "disable-explain"` ]]
            then
                extra_ignore_cmd=" $extra_ignore_cmd --special-run"
            fi

            cd $HOME/oceanbase/tools/deploy
            mysqltest_cmd="$obd test mysqltest $ob_name $SERVER_ARGS --mysqltest-bin=$DEP_PATH/u01/obclient/bin/mysqltest --obclient-bin=$DEP_PATH/u01/obclient/bin/obclient $COLLECT_ARG --init-sql-dir=$HOME/oceanbase/tools/deploy --log-dir=./var/log $REBOOT_TIMEOUT $VERBOSE_ARG $EXTRA_ARGS_WITHOUT_CASE --suite=cross_validation $extra_ignore_cmd"
            $mysqltest_cmd $INIT_FLIES $SCHE_ARGS 2>&1 | tee compare.out && ( exit ${PIPESTATUS[0]})
            test_suite_ret=$?

            FILTER=`ls -ltr $HOME/oceanbase/tools/deploy/mysql_test/test_suite/cross_validation/t | grep -v total | awk '{gsub(".test","",$9);print "cross_validation."$9}' | tr '\n' ',' | head -c -1`
            if [[ $FILTER == "" ]]
            then
                echo "without testsuite cases"
            fi
            mysqltest_cmd="$obd test mysqltest $ob_name $SERVER_ARGS  --mysqltest-bin=$DEP_PATH/u01/obclient/bin/mysqltest --obclient-bin=$DEP_PATH/u01/obclient/bin/obclient $COLLECT_ARG --init-sql-dir=$HOME/oceanbase/tools/deploy --log-dir=./var/log $REBOOT_TIMEOUT $VERBOSE_ARG $EXTRA_ARGS_WITHOUT_CASE --exclude=$FILTER --tag=cross_validation $extra_ignore_cmd"
            $mysqltest_cmd $INIT_FLIES $SCHE_ARGS 2>&1 | tee compare.out && ( exit ${PIPESTATUS[0]})
            tag_ret=$?
            [[ $test_suite_ret = 0 && $tag_ret = 0 ]] && current_ret=0 || current=1
            [[ $ret = 0 && $current_ret = 0 ]] && ret=0 || ret=1
        done
    
        if [[ $JOBNAME == 'mysqltest_opensource' ]]
        then
            submarker="_opensource"
        else
            submarker=""
        fi 

        mv compare.out $HOME/mysqltest${submarker}_compare_output.$SLICE_IDX
        echo "finish!"
        return $ret
    elif [[ $WITH_SHARED_STORAGE == "1" ]]
    then
        # 把sensitive_test目录下的测试集合移动到tools/deploy/mysql_test 
        # 并且创建一个新的目录用来表示单独的测试
        if [[ ! -d $HOME/oceanbase/sensitive_test/mysql_test ]]
        then
            return 0
        fi
        need_mv_dirs=`ls -1 $HOME/oceanbase/sensitive_test/mysql_test/test_suite|xargs`
        run_suites=""
        for need_mv_dir in $need_mv_dirs
        do
            new_dir=$HOME/oceanbase/tools/deploy/mysql_test/test_suite/shared_storage__${need_mv_dir}
            run_suites="${run_suites}shared_storage__${need_mv_dir},"
            mkdir -p $new_dir
            cp -r $HOME/oceanbase/sensitive_test/mysql_test/test_suite/$need_mv_dir/* $new_dir
        done
        run_suites=`echo $run_suites | awk '{print substr($0, 1, length($0)-1)}'`
        mysqltest_cmd="$obd test mysqltest $ob_name $SERVER_ARGS --mysqltest-bin=./mysqltest --obclient-bin=./obclient $COLLECT_ARG --init-sql-dir=$HOME/oceanbase/tools/deploy --log-dir=./var/log $REBOOT_TIMEOUT $VERBOSE_ARG $EXTRA_ARGS_WITHOUT_CASE --suite=$run_suites"
        $mysqltest_cmd $INIT_FLIES $SCHE_ARGS 2>&1 | tee compare.out && ( exit ${PIPESTATUS[0]})
    else
        if [[ -f $HOME/oceanbase/tools/deploy/error_log_filter.json && ( $BRANCH == 'master' || $BRANCH == "4_2_x_release" ) && "$FROM_FARM" == '1' ]]
        then
            echo "python $HOME/common/analyse_the_observer_log.py collect -p ${DATA_PATH}/observer1/log  -p ${DATA_PATH}/observer2/log -i $GID -j ${JOBNAME}.${SLICE_IDX} -o $HOME -F $HOME/oceanbase/tools/deploy/error_log_filter.json" > $HOME/oceanbase/tools/deploy/analyse_the_observer_log.sh
            chmod a+x $HOME/oceanbase/tools/deploy/analyse_the_observer_log.sh
            RUN_EXTRA_CHECK_CMD='--extra-script-after-case="$HOME/oceanbase/tools/deploy/./analyse_the_observer_log.sh"'
        fi
        mysqltest_cmd="$obd test mysqltest $ob_name $SERVER_ARGS --mysqltest-bin=./mysqltest --obclient-bin=./obclient $COLLECT_ARG --init-sql-dir=$HOME/oceanbase/tools/deploy --log-dir=./var/log $REBOOT_TIMEOUT $VERBOSE_ARG $EXTRA_ARGS"
        $mysqltest_cmd $RUN_EXTRA_CHECK_CMD $INIT_FLIES $SCHE_ARGS 2>&1 | tee compare.out && ( exit ${PIPESTATUS[0]})
        ret=$?
        if [[ $JOBNAME == 'mysqltest_opensource' ]]
        then
            submarker="_opensource"
        else
            submarker=""
        fi
        mv compare.out $HOME/mysqltest${submarker}_compare_output.$SLICE_IDX
        echo "finish!"
        return $ret
    fi
}

function obd_prepare_env {
    obd_prepare_global
    obd_prepare_obd
    obd_prepare_config
    obd_prepare_bin
}

function obd_collect_log {
    
    echo "collect log"
    cd $HOME/
    mkdir -p collected_log
    mkdir -p collected_log/obd_log
    mkdir -p collected_log/mysqltest_log
    mkdir -p collected_log/mysqltest_rec_log
    find $DATA_PATH -name 'core[.-]*' | xargs -i cp {} collected_log
    [[ "$OBD_HOME" != "" ]] && mv $OBD_HOME/.obd/log/*  collected_log/obd_log/
    set +x
    if [[ $WITH_SHARED_STORAGE == "1" ]]
    then
        sed -i  "s/${SENSITIVE_TEST_OSS_KEY_FOR_OBJECT_STORAGE_FOR_TOTAL}//" collected_log/obd_log/obd
    fi
    set -x
    mv oceanbase/tools/deploy/var/log/* collected_log/mysqltest_log/
    mv oceanbase/tools/deploy/var/rec_log/* collected_log/mysqltest_rec_log/
    mv collected_log collected_log_$SLICE_IDX
    tar zcvf collected_log_${SLICE_IDX}${JOB_SUFFIX}.tar.gz collected_log_$SLICE_IDX
}

function collect_obd_case_log {
    
    echo "collect obd case log"
    cd $HOME/
    if [[ $JOBNAME == 'mysqltest_opensource' ]]
    then
        submarker="_opensource"
    else
        submarker=""
    fi
    [[ "$OBD_HOME" != "" ]] && cat $OBD_HOME/.obd/log/* | grep '\[INFO\]' > mysqltest_cases_log${submarker}_$SLICE_IDX.output

}


export -f obd_prepare_global
export -f obd_prepare_config
export -f obd_run_mysqltest
export -f obd_init_cluster
export -f is_case_selector_arg

function run {
    if [[ -f /root/tmp/.ssh/id_rsa ]]
    then
        mv /root/tmp/.ssh/id_rsa /root/.ssh/
    fi
    if ([[ -f $HOME/oceanbase/rpm/oceanbase.deps ]] && [[ "$(grep 'ob-deploy' $HOME/oceanbase/rpm/oceanbase.deps )" != "" ]]) || 
      ([[ -f $HOME/oceanbase/deps/3rd/oceanbase.el7.x86_64.deps ]] && [[ "$(grep 'ob-deploy' $HOME/oceanbase/deps/3rd/oceanbase.el7.x86_64.deps )" != "" ]]) ||
      ([[ -f $HOME/oceanbase/deps/init/oceanbase.el7.x86_64.deps ]] && [[ "$(grep 'ob-deploy' $HOME/oceanbase/deps/init/oceanbase.el7.x86_64.deps )" != "" ]])
    then
        if [[ $WITH_SHARED_STORAGE == "1" ]]
        then
            # 单独判断下是否有对应的case集合
            if [[ ! -d $HOME/oceanbase/sensitive_test/mysql_test ]]
            then
                return 0
            fi
        fi
        timeout=18000
        [[ "$SPECIAL_RUN" == "1" ]] && timeout=72000
        obd_prepare_env &&
        timeout $timeout bash -c "obd_run_mysqltest" 
        test_ret=$?
        error_log_ret=0
        if [[ -f $HOME/oceanbase/tools/deploy/error_log_filter.json && $BRANCH == 'master' ]]
        then
            # notice the core
            python $HOME/common/analyse_the_observer_core.py collect -p ${DATA_PATH} -j ${JOBNAME}.${SLICE_IDX} -o $HOME
            error_log_ret=0
            collect_obd_case_log
        fi
        if [[ $test_ret -ne 0 || $error_log_ret -ne 0  ]]
        then
            obd_collect_log
        fi
        return `[[ $test_ret = 0 && $error_log_ret = 0 ]]` 
    else
        prepare_env &&
        timeout 18000 bash -c "run_mysqltest"   
        test_ret=$?      
        error_log_ret=0
        if [[ -f $HOME/oceanbase/tools/deploy/error_log_filter.json && ( $BRANCH == 'master' || $BRANCH == "4_2_x_release" ) ]]
        then
            python $HOME/common/analyse_the_observer_log.py collect -p ${DATA_PATH}/observer1/log  -p ${DATA_PATH}/observer2/log -i $GID -j ${JOBNAME}.${SLICE_IDX} -o $HOME -F $HOME/oceanbase/tools/deploy/error_log_filter.json
            error_log_ret=0
        fi
        if [[ $test_ret -ne 0 || $error_log_ret -ne 0  ]]
        then
            collect_log
        fi
        return `[[ $test_ret = 0 && $error_log_ret = 0 ]]`
    fi
    
}

run "$@"