#!/bin/bash 

BASE_DIR=$(readlink -f "$(dirname ${BASH_SOURCE[0]})/../..")
DEPLOY_PATH="$BASE_DIR/tools/deploy"
OBSERVER_BIN="$BASE_DIR/tools/deploy/bin/observer"
OBD_CLUSTER_PATH="$DEPLOY_PATH"/.obd/cluster
OBD_LOCAL_VERSION_PATH="$DEPLOY_PATH"/.obd/version
shopt -s expand_aliases
source $DEPLOY_PATH/activate_obd.sh
tag="latest"
export TELEMETRY_REPORTER="dev"

current_path=$(pwd)
if [[ "$current_path" != "$BASE_DIR/tools/deploy" ]]
then
  echo "Switching basedir to [$BASE_DIR/tools/deploy]..."
  cd $BASE_DIR/tools/deploy || exit 1
fi

function absolute_path {
  if [[ "$1" != "" ]]
  then
    if [[ $(echo "$1" | grep -E '^/') == "" ]]
    then
      echo ${current_path}/$1
    else
      echo $1
    fi
  fi
}

function obd_exec {
  echo -e "\033[32m>>> obd $* \033[0m"
  $OBD_BIN $* $VERBOSE_FLAG
}
alias obd="obd_exec"

function variables_prepare {
  if [[ "$(readlink -f "$BASE_DIR"/..)" == "$OB_FLOW_WORK_DIR" ]]
  then
  path=$(readlink -f "$BASE_DIR")
  export task="${path##*/}"
  else
  export task="default"
  fi
  port_gen=$((100*($(id -u)%500)+10000))
  HOST=$(hostname -i)
  DATA_PATH="/data/$(whoami)"
  IPADDRESS="127.0.0.1"
  if [[ -f "$BASE_DIR/.ce" ]]
  then
    export IS_CE=1
    COMPONENT="oceanbase-ce"
  else
    COMPONENT="oceanbase"
  fi
  if grep 'dep_create.sh' $BASE_DIR/build.sh 2>&1 >/dev/null
  then
    DEP_PATH=$BASE_DIR/deps/3rd
  else
    DEP_PATH=$BASE_DIR/rpm/.dep_create/var
  fi

  export LD_LIBRARY_PATH=$DEP_PATH/u01/obclient/lib:$LD_LIBRARY_PATH

  OBCLIENT_BIN=$DEP_PATH/u01/obclient/bin/obclient
  OBCLIENT_BIN_ARGS="--obclient-bin=$OBCLIENT_BIN"
  MYSQLTEST_BIN=$DEP_PATH/u01/obclient/bin/mysqltest
  MYSQLTEST_BIN_ARGS="--mysqltest-bin=$MYSQLTEST_BIN"
  CLIENT_BIN_ARGS="$OBCLIENT_BIN_ARGS $MYSQLTEST_BIN_ARGS"

  DEFAULT_DEPLOY_NAME_FILE=$OBD_HOME/.obd/.default_deploy
}

function copy_sh {
  if [[ -f copy.sh ]]
  then
  bash copy.sh $BUILD_PATH
  else
  echo 'can not find copy.sh'
  fi
}

function mirror_create {
  # obproxy  mirror create
  if [[ "$WITH_LOCAL_PROXY" != "" ]] && [[ -x $DEPLOY_PATH/bin/obproxy ]]
  then
  odp_version=$("$DEPLOY_PATH"/bin/obproxy --version  2>&1  | grep -E 'obproxy \(OceanBase [\.0-9]+ \S+\)' | grep -Eo '\s[.0-9]+\s')
  obd mirror create -n obproxy -p "$DEPLOY_PATH" -t $tag -V $odp_version -f
  fi

  # observer mirror create
  if [[ "$OBSERVER_PATH" != "" ]]
  then
    mkdir -p $BASE_DIR/tools/deploy/{bin,etc,admin}
    cp -f $OBSERVER_PATH $OBSERVER_BIN || exit 1
  fi
  obs_version_info=`$OBSERVER_BIN -V 2>&1`
  if [[ $? != 0 ]]
  then
    echo $obs_version_info
    return 1
  fi
  obs_version=$(echo "$obs_version_info" | grep -E "observer \(OceanBase([ \_]CE)? ([.0-9]+)\)" | grep -Eo '([.0-9]+)')
  if [[ "$obs_version" == "" ]]
  then
    echo "can not check observer version"
    echo $obs_version_info
    return 1
  fi
  if [ "x$IS_CE" == "x" ]
  then
    export IS_CE="0"
    [[ $($OBSERVER_BIN -V 2>&1 | grep -E 'OceanBase[_ ]CE') ]] && COMPONENT="oceanbase-ce" && export IS_CE="1"
  fi
  $OBSERVER_BIN -V
  [[ -f "$BASE_DIR/tools/deploy/obd/.observer_obd_plugin_version" ]] && obs_version=$(cat $BASE_DIR/tools/deploy/obd/.observer_obd_plugin_version)
  obs_mirror_info=$(obd_exec mirror create -n $COMPONENT -p "$DEPLOY_PATH" -V "$obs_version"  -t $tag -f) && success=1
  if [[ "$success" != "1" ]]
  then
  echo "$obs_mirror_info"
  echo "create observer mirror failed"
  return 2
  else
  echo "$obs_mirror_info"
  fi
  obs_package_hash=$(echo "$obs_mirror_info"| grep -E '^md5:' | awk '{print $2}')
}



## generate config
function generate_config {
  app_name=$task.$USER.$HOST

  port_num=$port_gen
  mysql_port=$port_num && port_num=$((port_num+1))
  rpc_port=$port_num && port_num=$((port_num+1))
  mysql_port2=$port_num && port_num=$((port_num+1))
  rpc_port2=$port_num && port_num=$((port_num+1))
  listen_port=$port_num && port_num=$((port_num+1))
  prometheus_listen_port=$port_num && port_num=$((port_num+1))

  base_template=$(cat obd/config.yaml.template)
  proxy_conf=$(cat obd/obproxy.yaml.template)
  if [[ "$WITH_LOCAL_PROXY" != "" ]]
  then
  proxy_conf=$(echo "$proxy_conf" | sed 's/package_hash: [0-9a-z]*/tag: {{%% TAG %%}}/g')
  fi
  proxy_conf=${proxy_conf//'{{%% TAG %%}}'/$tag}
  proxy_conf=${proxy_conf//'{{%% COMPONENT %%}}'/$COMPONENT}
  proxy_conf=${proxy_conf//'{{%% LISTEN_PORT %%}}'/$listen_port}
  proxy_conf=${proxy_conf//'{{%% PROMETHEUS_LISTEN_PORT %%}}'/$prometheus_listen_port}
  proxy_conf=${proxy_conf//'{{%% OBPORXY_HOME_PATH %%}}'/"$DATA_PATH"/obproxy}


  base_template=${base_template//"{{%% COMPONENT %%}}"/$COMPONENT}
  base_template=${base_template//"{{%% TAG %%}}"/$tag}
  base_template=${base_template//"{{%% DEPLOY_PATH %%}}"/$DEPLOY_PATH}
  base_template=${base_template//"{{%% TOOLS_PATH %%}}"/$BASE_DIR/tools}
  base_template=${base_template//"{{%% MINI_SIZE %%}}"/$mem}
  base_template=${base_template//"{{%% APPNAME %%}}"/$app_name}
  base_template=${base_template//"{{%% EXTRA_PARAM %%}}"/}

  # simple.yaml
  SERVERS=$(cat <<-EOF
  servers:
    - name: server1
      ip: $IPADDRESS
  server1:
    mysql_port: $mysql_port
    rpc_port: $rpc_port
    home_path: $DATA_PATH/observer1
    zone: zone1
    # The directory for data storage. The default value is home_path/store.
    # data_dir: /data
    # The directory for clog, ilog, and slog. The default value is the same as the data_dir value.
    # redo_dir: /redo
EOF
)
  single_conf=${base_template}
  single_conf=${single_conf//"{{%% SERVERS %%}}"/$SERVERS}
  single_without_proxy_conf=${single_conf//"{{%% PROXY_CONF %%}}"/}
  single_with_proxy_conf=${single_conf//"{{%% PROXY_CONF %%}}"/$proxy_conf}

  [ ! -f ./single.yaml ] && echo "$single_without_proxy_conf" > ./single.yaml && echo "generate yaml config file: $(readlink -f ./single.yaml)"
  [ ! -f ./single-with-proxy.yaml ] && echo "$single_with_proxy_conf" > ./single-with-proxy.yaml && echo "generate yaml config file: $(readlink -f ./single-with-proxy.yaml)"

  # 2x1
  SERVERS=$(cat <<-EOF
  servers:
    - name: server1
      ip: $IPADDRESS
    - name: server2
      ip: $IPADDRESS
  server1:
    mysql_port: $mysql_port
    rpc_port: $rpc_port
    home_path: $DATA_PATH/observer1
    # The directory for data storage. The default value is home_path/store.
    # data_dir: /data
    # The directory for clog, ilog, and slog. The default value is the same as the data_dir value.
    # redo_dir: /redo
    zone: zone1
  server2:
    mysql_port: $mysql_port2
    rpc_port: $rpc_port2
    home_path: $DATA_PATH/observer2
    # The directory for data storage. The default value is home_path/store.
    # data_dir: /data
    # The directory for clog, ilog, and slog. The default value is the same as the data_dir value.
    # redo_dir: /redo
    zone: zone2
EOF
)

  distributed_conf=${base_template}
  distributed_conf=${distributed_conf//"{{%% SERVERS %%}}"/$SERVERS}
  distributed_without_proxy_conf=${distributed_conf//"{{%% PROXY_CONF %%}}"/}
  distributed_with_proxy_conf=${distributed_conf//"{{%% PROXY_CONF %%}}"/$proxy_conf}
  [ ! -f ./distributed.yaml ] && echo "$distributed_without_proxy_conf" > ./distributed.yaml && echo "generate yaml config file: $(readlink -f ./distributed.yaml)"
  [ ! -f ./distributed-with-proxy.yaml ] && echo "$distributed_with_proxy_conf" > ./distributed-with-proxy.yaml && echo "generate yaml config file: $(readlink -f ./distributed-with-proxy.yaml)"
}

function show_deploy_name {
  echo -e "\e[1m\033[32mDeploy name: $deploy_name \033[0m"
}

function get_deploy_name {
  [ "$HELP" == "1" ] && return
  if [[ "$deploy_name" != "" ]]
  then
  return
  fi 
  if [[ "$DEPLOY_NAME" == "" ]]
  then
    [[ "$YAML_CONF" != "" ]] && yaml_name=${YAML_CONF##*/} && deploy_name=${yaml_name%.yaml} && show_deploy_name && return
    mkdir -p $OBD_CLUSTER_PATH
    cluster_dirs=$(ls -1 $OBD_CLUSTER_PATH)
    cluster_num=$(echo "$cluster_dirs" | wc -l )
    if (( ${cluster_num} == 1 )) && [[ -f $DEFAULT_DEPLOY_NAME_FILE ]]
    then
      deploy_name=$(cat "$DEFAULT_DEPLOY_NAME_FILE") && show_deploy_name && return
    elif [[ "$OB_DO_DEFAULT_DEPLOY_NAME" ]]
    then
      deploy_name=$OB_DO_DEFAULT_DEPLOY_NAME && show_deploy_name && return
    fi
    echo """
Deploy name is required. Use -n <deploy-name> to set the deploy name.
Available deployments:
$cluster_dirs
""" && exit 1
  else
    deploy_name="$DEPLOY_NAME" && show_deploy_name && return
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
    version=$(echo "$odp_conf" | grep 'version:' | awk '{print $2}')
    release=$(echo "$odp_conf" | grep 'release:' | awk '{print $2}')
    include_file=$(echo "$odp_conf"| grep 'include:'| awk '{print $2}')
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
    obproxy_mirror_repository=$(echo $(grep '__mirror_repository_section_name' $config_yaml | awk -F':' '{print $2}'))
    [ "$obproxy_mirror_repository" != "" ] && repository=$obproxy_mirror_repository

    get_version_and_release
    get_baseurl
    if [[ "$baseurl" != "" && "$version" != "" && "$release" != "" ]]
    then
      pkg_name="obproxy-$version-$release.x86_64.rpm"
      if [ "$(find $OBD_HOME/.obd/mirror/local -name $pkg_name)" == "" ]
      then
        download_dir=$OBD_HOME/.obd_download
        mkdir -p $download_dir
        wget $baseurl/obproxy/$pkg_name -O "$download_dir/$pkg_name" -o $download_dir/obproxy.down
        obd mirror clone "$download_dir/$pkg_name" && rm -rf $download_dir && return 0
      else
        return 0
      fi
    fi
    obd mirror enable $repository
}

function deploy_cluster {
  get_deploy_name
  if [[ "$YAML_CONF" != "" ]] 
  then
    config_yaml=$YAML_CONF
  else
    if [[ -f $OBD_CLUSTER_PATH/$deploy_name/tmp_config.yaml && "$(grep config_status $OBD_CLUSTER_PATH/$deploy_name/.data | awk '{print $2}')" != "UNCHNAGE" ]]
    then
      config_yaml=$OBD_CLUSTER_PATH/$deploy_name/tmp_config.yaml
    else
      config_yaml=$OBD_CLUSTER_PATH/$deploy_name/config.yaml
    fi
  fi
  [[ "$(grep -E "^obproxy:" $config_yaml)" != "" ]] && ( get_obproxy || exit 1 )

  echo "$deploy_name" > "$DEFAULT_DEPLOY_NAME_FILE"
  if [[ "$HIDE_DESTROY" == "1" ]]
  then
    obd cluster destroy "$deploy_name" -f > /dev/null 2>&1
  else
    obd cluster destroy "$deploy_name" -f
  fi
  if [[ -f $OBD_CLUSTER_PATH/$deploy_name/inner_config.yaml ]]
  then
    sed -i '/$_deploy_/d' $OBD_CLUSTER_PATH/$deploy_name/inner_config.yaml
  fi
  [[ "$YAML_CONF" == "" ]] || yaml_config_args="-c $YAML_CONF"
  obd cluster deploy "$deploy_name" -C $yaml_config_args || exit 1
  if ! obd cluster start "$deploy_name" -f;
  then
    while [[ "$NO_CONFIRM" != "1" && "$(grep 'config_status: NEED_REDEPLOY' $OBD_CLUSTER_PATH/$deploy_name/.data)" != "" ]] 
    do
      read -r -p "Start $deploy_name failed, do you want to edit config and continue?[Y/n]" input
      case $input in
        [Yy]);&
        "")
        obd cluster edit-config "$deploy_name"
        if obd cluster start "$deploy_name" -f;
        then
          break
        else
          continue
        fi
        ;;
        [Nn])
        exit 1
      esac
    done
  fi
  get_init_sql
  if [[ "$NEED_FAST_REBOOT" == "1" ]]
  then
  obd test mysqltest "$deploy_name" $INIT_FLIES --init-only $CLIENT_BIN_ARGS --fast-reboot
  else
  obd test mysqltest "$deploy_name" $INIT_FLIES --init-only $CLIENT_BIN_ARGS
  fi
}

function get_init_sql {
  [[ "$INIT_FLIES" != "" ]] && return
  if [[ "$MINI" == "1" && -f $BASE_DIR/tools/deploy/init.sql ]]
  then
    INIT_FLIES="--init-sql-files=init.sql,init_user.sql|root@mysql|test"
    [ -f init_user_oracle.sql ] && INIT_FLIES="${INIT_FLIES},init_user_oracle.sql|SYS@oracle|SYS"
  fi
}

function start_cluster {
  get_deploy_name
  obd cluster start "$deploy_name" -f
}

function stop_cluster {
  get_deploy_name
  obd cluster stop "$deploy_name"
}

function restart_cluster {
  get_deploy_name
  obd cluster restart "$deploy_name"
}

function upgreade_cluster {
  get_deploy_name
  obd cluster upgrade "$deploy_name"
}

function destroy_cluster {
  get_deploy_name
  if [[ ! -e  "$OBD_CLUSTER_PATH/$deploy_name" ]]
  then
    exit 1
  fi
  if [[ "$(grep 'status: STATUS_DESTROYED' $OBD_CLUSTER_PATH/$deploy_name/.data)" == "" ]]
  then
    obd cluster destroy "$deploy_name"  -f
  fi
  if [[ "$RM_CLUSTER" == "1" ]]
  then
    rm -rf $OBD_CLUSTER_PATH/$deploy_name
  else
    echo "Use --rm to remove deploy $deploy_name dir"
  fi
}

function reinstall_cluster {
  get_deploy_name
  obd cluster reinstall "$deploy_name" -c $COMPONENT --hash=$obs_package_hash
}

function tool_cmd {
  cmd=$1
  get_deploy_name
  if [[ $HELP == "1" ]]
  then
    obd tool command --help
  return
  fi
  obd tool command "$deploy_name" $cmd $extra_args
}

function connect {
  get_deploy_name
  if [[ $HELP == "1" ]]
  then
    obd tool db_connect --help
  return
  fi
  obd tool db_connect "$deploy_name" $OBCLIENT_BIN_ARGS $extra_args
}

function mysqltest {
  if [[ $HELP == "1" ]]
  then
  obd test mysqltest --help
  return
  fi
  get_deploy_name

  if [[ "$NEED_REBOOT" == "1" || "$YAML_CONF" != "" ]]
  then
  mirror_create || return 1
  deploy_cluster
  fi
  get_init_sql
  obd test mysqltest "$deploy_name" $CLIENT_BIN_ARGS $extra_args $INIT_FLIES
}

function deploy {
  if [[ "$YAML_CONF" != *.yaml && "$DEPLOY_NAME" == "" ]]
  then
    echo "Use -n to set deploy name, or -c to set the deploy config"
    exit 1
  fi
  get_deploy_name
  if [[ "$YAML_CONF" != "" && -d "$OBD_CLUSTER_PATH"/"$deploy_name" ]]
  then
    while true
    do
      read -r -p "Deploy name: $deploy_name already exists. Do you want to OVERWRITE it?[y/n]" input
      case $input in
        [Yy])
        break
        ;;
        [Nn])
        exit 1
      esac
    done
  fi
  HIDE_DESTROY=1
  [[ "$EXEC_CP" == "1" ]] && copy_sh
  mirror_create || exit 1
  deploy_cluster
}

function edit {
  get_deploy_name
  obd cluster edit-config $deploy_name
  if [[ "$(grep 'config_status: NEED_REDEPLOY' $OBD_CLUSTER_PATH/$deploy_name/.data)" != "" ]]
  then
    echo -e "\e[33mif you need redeploy, please use '$entrance redeploy -n $deploy_name'\e[0m"
  fi
}

function display {
  get_deploy_name
  obd cluster display $deploy_name
}

function create_tenant {
  get_deploy_name
  obd cluster tenant create $deploy_name $extra_args
}

function drop_tenant {
  get_deploy_name
  obd cluster tenant drop $deploy_name $extra_args
}

function sysbench {
  get_deploy_name
  obd test sysbench $deploy_name $OBCLIENT_BIN_ARGS $extra_args
}

function tpch {
  get_deploy_name
  obd test tpch $deploy_name $OBCLIENT_BIN_ARGS $extra_args
}

function tpcc {
  get_deploy_name
  obd test tpcc $deploy_name $OBCLIENT_BIN_ARGS $extra_args
}

function tpcds {
  get_deploy_name
  obd test tpcds $deploy_name $OBCLIENT_BIN_ARGS $extra_args
}

function graph {
  get_deploy_name
  obd tool graph $deploy_name $extra_args
}

function set-config {
  OB_DO_GLOBAL_CONFIG=${OB_DO_GLOBAL_CONFIG:-~/.ob_do_global}
  touch $OB_DO_GLOBAL_CONFIG
  if [[ "$1" != "" ]]; then
    key="$1"
    value="$2"
    if [[ $(grep -E "^$key=" $OB_DO_GLOBAL_CONFIG) ]]; then
      sed -i "s/^$key=.*/$key=$value/g" $OB_DO_GLOBAL_CONFIG
    else
      echo "$key=$value" >> $OB_DO_GLOBAL_CONFIG
    fi
  fi
}


function help_info {
  echo """
Usage: $entrance <command> [options]

Available commands:

prepare  [-b BUILD_PATH -p DATA_PATH -h HOST]    Prepare for deployment.
deploy -c YAML_CONF [-n DEPLOY_NAME]     Deploy a cluster by a deploy yaml file. Default deploy name will be the name of yaml file.
redeploy [-c YAML_CONF -n DEPLOY_NAME]   Redeploy cluster.
reinstall [-n DEPLOY_NAME]               Reinstall cluster. (Change bin file, sync libs and restart)
start [-n DEPLOY_NAME]                   Start cluster.
stop [-n DEPLOY_NAME]                    Stop a started cluster.
restart [-n DEPLOY_NAME]                 Restart cluster.
destroy [-n DEPLOY_NAME]                 Destroy cluster.
upgrade [-n DEPLOY_NAME]                 Upgrade cluster.
list [-n DEPLOY_NAME]                    List cluster.
display [-n DEPLOY_NAME]                 Display cluster info.
create_tenant [-n DEPLOY_NAME]           Create tenant.
drop_tenant [-n DEPLOY_NAME]             Drop tenant.
sysbench [-n DEPLOY_NAME]                Run sysbench, use '--help' for more details.
tpch [-n DEPLOY_NAME]                    Run tpch test, use '--help' for more details.
tpcc [-n DEPLOY_NAME]                    Run tpcc test, use '--help' for more details.
tpcds [-n DEPLOY_NAME]                   Run tpcds test, use '--help' for more details.
mysqltest [-n DEPLOY_NAME]               Run mysqltest, use '--help' for more details.
pid [-n DEPLOY_NAME]                     Get pid list for servers, use '--help' for more details.
ssh [-n DEPLOY_NAME]                     Ssh to target server and change directory to log path, use '--help' for more details.
less [-n DEPLOY_NAME]                    Use command less to the observer.log, use '--help' for more details.
gdb [-n DEPLOY_NAME]                     Use gdb to attch target server, use '--help' for more details.
sql [-n DEPLOY_NAME]                     Connect to target server by root@sys, use '--help' for more details.
sys [-n DEPLOY_NAME]                     Connect to target server by root@sys, use '--help' for more details.
mysql [-n DEPLOY_NAME]                   Connect to target server by root@mysql, use '--help' for more details.
oracle [-n DEPLOY_NAME]                  Connect to target server by SYS@oracle, use '--help' for more details.
graph [-n DEPLOY_NAME]

Options:
-V, --version                            Show version of obd.
-c YAML_CONF, --config YAML_CONF         The deploy yaml file.
-n DEPLOY_NAME, --deploy-name DEPLOY_NAME
                                         The name of the deployment.
-v VERBOSE                               Activate verbose output.
-b BUILD_PATH, --build-path BUILD_PATH   The build path of oceanbase. If not specified, it will be chosen in alphabetical order.
-p DATA_PATH, --data-path DATA_PATH      The data path for server deployment, it can be changed in the yaml file.
--ip IPADDRESS                           The ipaddress for server deployment, it can be changed in the yaml file.
--port PORT_BEGIN                        The port starting point. All the ports can be changed in the yaml file.
--with-local-obproxy                     Use local obproxy.
--skip-copy                              Skip copy.sh.
--cp                                     Exec copy.sh.
--reboot                                 Redeploy cluster before mysqltest.

"""
}

function main() {
  entrance=${OBD_SH_ENTRANCE:-obd.sh}
  variables_prepare
  command="$1"
  shift
  extra_args=""
  while true; do
    case "$1" in
      -v ) VERBOSE_FLAG='-v'; set -x; shift ;;
      --with-local-obproxy) WITH_LOCAL_PROXY="1";SKIP_COPY="1"; shift ;;
      -c | --config )
        if [[ $command == "deploy" || $command == "redeploy" || $command == "mysqltest" ]]
        then
          YAML_CONF="$2"
          shift 2
        else
          extra_args="$extra_args $1"
          shift
        fi
        ;;
      -n | --deploy-name ) DEPLOY_NAME="$2"; shift 2 ;;
      -b | --build-path ) BUILD_PATH="$2"; shift 2 ;;
      -p | --data-path ) DATA_PATH="$2"; shift 2 ;;
      -N ) NO_CONFIRM="1"; shift ;;
      --ip ) IPADDRESS="$2"; shift 2 ;;
      --disable-reboot ) DISABLE_REBOOT="1"; extra_args="$extra_args $1"; shift ;;
      --reboot ) NEED_REBOOT="1"; shift ;;
      --fast-reboot ) NEED_FAST_REBOOT="1"; extra_args="$extra_args $1"; shift ;;
      --cp ) EXEC_CP="1"; shift ;;
      --skip-copy ) SKIP_COPY="1"; shift ;;
      --mini) MINI="1"; shift ;;
      --port ) export port_gen="$2"; extra_args="$extra_args $1"; shift ;;
      --observer ) OBSERVER_PATH="$2"; shift 2 ;;
      --rm ) RM_CLUSTER="1"; shift ;;
      -- ) shift ;;
      "" ) break ;;
      * ) extra_args="$extra_args $1"; [[ "$1" == "--help" || "$1" == "-h" ]] && HELP="1" ; shift ;;
    esac
  done

  YAML_CONF=$(absolute_path ${YAML_CONF})
  DATA_PATH=$(absolute_path ${DATA_PATH})
  BUILD_PATH=$(absolute_path ${BUILD_PATH})
  OBSERVER_PATH=$(absolute_path ${OBSERVER_PATH})

  if [[ "$MINI" == "1" && "$DISABLE_REBOOT" != "1" ]]
  then 
    NEED_REBOOT="1"
  fi
  if [[ ! -f $OBD_HOME/.obd/.obd_environ || "$(grep '"OBD_DEV_MODE": "1"' $OBD_HOME/.obd/.obd_environ)" == "" ]]
  then
  obd devmode enable || (echo "Exec obd cmd failed. If your branch is based on 3.1_opensource_release, please go to the deps/3rd directory and execute 'bash dep_create.sh all' to install obd." && exit 1)
  [[ "$OBD_LOCK_MODE" ]] || obd env set OBD_LOCK_MODE 0
  fi
  if [[  "$(grep '"OBD_DEPLOY_BASE_DIR":' $DEPLOY_PATH/.obd/.obd_environ)" == "" ]]
  then
  obd env set OBD_DEPLOY_BASE_DIR "$DEPLOY_PATH"
  fi
  OBD_DEPLOY_BASE_DIR=$(grep -Po '"OBD_DEPLOY_BASE_DIR": "(.*?)"[,}]' ./.obd/.obd_environ  | sed 's/"OBD_DEPLOY_BASE_DIR": "\(.*\)"[,}]/\1/g')
  if [[ ! -d $OBD_DEPLOY_BASE_DIR ]]
  then
  obd env set OBD_DEPLOY_BASE_DIR "$DEPLOY_PATH"
  fi
  case $command in
    -V | --version)
    obd --version
    ;;
    -h | --help)
    help_info
    ;;
    prepare)
    [[ "$SKIP_COPY" == "" ]] && copy_sh
    generate_config
    ;;
    deploy)
    deploy
    ;;
    redeploy)
    deploy
    ;;
    start)
    start_cluster
    ;;
    stop)
    stop_cluster
    ;;
    restart)
    restart_cluster
    ;;
    destroy)
    destroy_cluster
    ;;
    upgrade)
    [[ "$EXEC_CP" == "1" ]] && copy_sh
    mirror_create || exit 1
    upgreade_cluster
    ;;
    mysqltest)
    [[ "$EXEC_CP" == "1" ]] && copy_sh
    mysqltest
    ;;
    sql);&
    sys)
    connect 
    ;;
    mysql)
    extra_args="--tenant mysql $extra_args"
    connect
    ;;
    oracle)
    extra_args="--user SYS -m OB_ORACLE --tenant oracle $extra_args"
    connect
    ;;
    pid)
    tool_cmd pid
    ;;
    ssh)
    tool_cmd ssh
    ;;
    less)
    tool_cmd less
    ;;
    gdb)
    tool_cmd gdb
    ;;
    reinstall)
    get_deploy_name
    [[ "$EXEC_CP" == "1" ]] && copy_sh
    mirror_create && reinstall_cluster
    ;;
    list)
    obd cluster list
    ;;
    edit)
    edit
    ;;
    display)
    display
    ;;
    create_tenant)
    create_tenant
    ;;
    drop_tenant)
    drop_tenant
    ;;
    sysbench)
    sysbench
    ;;
    tpch)
    tpch
    ;;
    tpcc)
    tpcc
    ;;
    tpcds)
    tpcds
    ;;
    graph)
    graph
    ;;
    display-trace)
    obd display-trace ${extra_args}
    ;;
    set-config)
    set-config ${extra_args}
    ;;
    *)
    echo "Unknown command: $command"
    help_info
    exit 1
    ;;
  esac
}

main "$@"
