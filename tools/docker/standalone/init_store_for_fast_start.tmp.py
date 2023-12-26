import pymysql as mysql
import argparse
import time
import datetime
import subprocess
import os
import logging

def kill_server():
    kill_observer_cmd = "ps -ef | grep observer | grep -v grep | grep -v init_store_for_fast_start.py | awk '{print $2}' | xargs kill -9"
    kill_res = subprocess.call(kill_observer_cmd, shell=True)
    if kill_res != 0:
        logging.warn("kill observer failed")
        exit(-1)
    logging.info("kill observer ok")

def check_file_or_path_exist(bin_abs_path, home_abs_path, store_tar_file_path, etc_dest_dir):
    if not os.path.isfile(bin_abs_path):
        logging.warn("invalid bin path")
        return False
    if not os.path.isdir(home_abs_path):
        logging.warn("invalid home path")
        return False
    if not os.path.isdir(store_tar_file_path):
        logging.warn("invalid store tar file path")
        return False
    if not os.path.isdir(etc_dest_dir):
        logging.warn(etc_dest_dir)
        logging.warn("invalid etc dest dir")
        return False
    return True

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    cur_path = os.curdir
    cur_path = os.path.abspath(cur_path)
    logging.info("=================== cur_path: %s ==============" % cur_path)
    parser = argparse.ArgumentParser()
    parser.add_argument("observer_bin_path", type=str, help="the path of observer binary file")
    parser.add_argument("observer_home_path", type=str, help="the path of sys log / config file / sql.sock / audit info")
    parser.add_argument("store_tar_file_dir", type=str, help="store dir zip target dir")
    parser.add_argument("etc_dest_dir", type=str, help="the dest dir to save etc config files")
    parser.add_argument("--only_build_env", action='store_true', help="build env & start observer without bootstrap and basic check")
    parser.add_argument("-p", dest="mysql_port", type=str, default="@OB_MYSQL_PORT@")
    parser.add_argument("-P", dest="rpc_port", type=str, default="@OB_RPC_PORT@")
    parser.add_argument("-z", dest="zone", type=str, default="zone1")
    parser.add_argument("-c", dest="cluster_id", type=str, default="1")
    parser.add_argument("-d", dest="data_path", type=str, default="/data/store")
    parser.add_argument("-r", dest="rootservice", type=str, default="@OB_SERVER_IP@:@OB_RPC_PORT@")
    parser.add_argument("-I", dest="ip", type=str, default="@OB_SERVER_IP@")
    parser.add_argument("-l", dest="log_level", type=str, default="INFO")
    parser.add_argument("-o", dest="opt_str", type=str, default="__min_full_resource_pool_memory=2147483648,memory_limit=6G,system_memory=1G,datafile_size=256M,log_disk_size=5G,cpu_count=16")
    parser.add_argument("-N", dest="daemon", type=str, default="1")
    parser.add_argument("--tenant_name", type=str, default="@OB_TENANT_NAME@")
    parser.add_argument("--tenant_lower_case_table_names", type=int, default="@OB_TENANT_LOWER_CASE_TABLE_NAMES@")
    parser.add_argument("--max_cpu", type=float, default=7.0)
    parser.add_argument("--min_cpu", type=float, default=7.0)
    parser.add_argument("--memory_size", type=int, default=3221225472)
    parser.add_argument("--log_disk_size", type=int, default=3221225472)
    args = parser.parse_args()

    bin_abs_path = os.path.abspath(args.observer_bin_path)
    home_abs_path = os.path.abspath(args.observer_home_path)
    data_abs_path = os.path.abspath(args.data_path)
    store_tar_file_path = os.path.abspath(args.store_tar_file_dir)
    etc_dest_dir = os.path.abspath(args.etc_dest_dir)

    if not check_file_or_path_exist(bin_abs_path, home_abs_path, store_tar_file_path, etc_dest_dir):
        logging.warn("check file / path exist failed")
        exit(-1)

    rebuild_env_cmd = "sh ./env.sh %s %s %s -C true && sh ./env.sh %s %s %s -B true" % (bin_abs_path, home_abs_path, data_abs_path, \
                                                                                       bin_abs_path, home_abs_path, data_abs_path) if bin_abs_path != home_abs_path + "/observer" else \
                      "sh ./env.sh %s %s %s -C && sh ./env.sh %s %s %s -B" % (bin_abs_path, home_abs_path, data_abs_path, \
                                                                              bin_abs_path, home_abs_path, data_abs_path)

    # prepare environment for observer
    env_prepare = subprocess.call(rebuild_env_cmd, shell=True)
    if env_prepare != 0:
        logging.warn("prepare env failed")
        exit(-1)

    # prepare observer start parameters
    daemon_option = "-N" if args.daemon=="1" else ""
    observer_args = "-p %s -P %s -z %s -c %s -d %s -r %s -I %s -l %s -o %s %s" % (args.mysql_port, args.rpc_port, args.zone, \
                                                                                  args.cluster_id, data_abs_path, \
                                                                                  args.rootservice, args.ip, args.log_level, args.opt_str, \
                                                                                  daemon_option)
    os.chdir(home_abs_path)
    observer_cmd = "./observer %s" % (observer_args)
    subprocess.Popen(observer_cmd, shell=True)

    # bootstrap observer
    time.sleep(4)
    try:
        db = mysql.connect(host=args.ip, user="root", port=int(args.mysql_port), passwd="")
        cursor = db.cursor(cursor=mysql.cursors.DictCursor)
        logging.info('connection success!')
        if not args.only_build_env:
            logging.info("waiting for bootstrap...")
            bootstrap_begin = datetime.datetime.now()
            cursor.execute("ALTER SYSTEM BOOTSTRAP ZONE '%s' SERVER '%s'" % (args.zone, args.rootservice))
            bootstrap_end = datetime.datetime.now()
            logging.info('bootstrap success: %s ms' % ((bootstrap_end - bootstrap_begin).total_seconds() * 1000))
            # checkout server status
            cursor.execute("select * from oceanbase.__all_server")
            server_status = cursor.fetchall()
            if len(server_status) != 1 or server_status[0]['status'] != 'ACTIVE':
                logging.warn("get server status failed")
                exit(-1)
            logging.info('check server status ok')
            # create test tenant
            cursor.execute("create resource unit %s_unit max_cpu %s, memory_size %s, min_cpu %s, log_disk_size %s" % ( \
                            args.tenant_name, args.max_cpu, args.memory_size, args.min_cpu, args.log_disk_size))
            cursor.execute("create resource pool %s_pool unit='%s_unit', unit_num=1, zone_list=('%s')" % ( \
                            args.tenant_name, args.tenant_name, args.zone))
            logging.info("waiting for create tenant...")
            create_tenant_begin = datetime.datetime.now()
            cursor.execute("create tenant %s replica_num=1,zone_list=('%s'),primary_zone='RANDOM',resource_pool_list=('%s_pool') set ob_tcp_invited_nodes='%%', ob_compatibility_mode = 'mysql', lower_case_table_names=%d" % ( \
                            args.tenant_name, args.zone, args.tenant_name, args.tenant_lower_case_table_names))
            create_tenant_end = datetime.datetime.now()
            logging.info('create tenant success: %s ms' % ((create_tenant_end - create_tenant_begin).total_seconds() * 1000))
        db.close()
    except mysql.err.Error as e:
        logging.warn("deploy observer failed")
        kill_server()
        exit(-1)

    # grant privilege
    try:
        db = mysql.connect(host=args.ip, user="root@%s" % (args.tenant_name), port=int(args.mysql_port), passwd="")
        cursor = db.cursor(cursor=mysql.cursors.DictCursor)
        logging.info('connect by common tenant success!')
        cursor.execute("CREATE USER '%s'@'%%'" % (args.tenant_name))
        cursor.execute("GRANT ALL ON *.* TO '%s'@'%%'" % (args.tenant_name))
        logging.info("grant privilege success!")
    except mysql.err.Error as e:
        logging.warn("grant privilege for common tenant failed")
        kill_server()
        exit(-1)

    # stop observer
    kill_server()

    # record block cnt
    record_block_cnt_cmd = "cd %s/clog/log_pool && echo $[ `ls | wc -l` - 1 ] > %s/block_cnt" % (data_abs_path, store_tar_file_path)
    record_res = subprocess.call(record_block_cnt_cmd, shell=True)
    if record_res != 0:
        logging.warn("record block cnt failed")
        exit(-1)
    logging.info("record block cnt ok")

    # build store tar file
    build_store_tar_cmd = "cd %s/clog/log_pool && ls | grep '[0-9]' | xargs rm && cd %s/.. && \
                           tar -Sczvf %s/store.tar.gz ./store" % (data_abs_path, data_abs_path, store_tar_file_path)
    build_res = subprocess.call(build_store_tar_cmd, shell=True)
    if build_res != 0:
        logging.warn("build store tar file failed")
        exit(-1)
    logging.info("build store tar file ok")

    # copy config files to etc_dest_dir
    cp_config_cmd = "cp -r %s/etc/* %s" % (home_abs_path, etc_dest_dir)
    cp_config_res = subprocess.call(cp_config_cmd, shell=True)
    if cp_config_res != 0:
        logging.warn("cp config failed")
        exit(-1)
    logging.info("cp config ok")

    # clean env
    os.chdir(cur_path)
    clean_env_cmd = "sh ./env.sh %s %s %s -C true" % (bin_abs_path, home_abs_path, data_abs_path) if bin_abs_path != home_abs_path + "/observer" else \
                    "sh ./env.sh %s %s %s -C" % (bin_abs_path, home_abs_path, data_abs_path)
    clean_res = subprocess.call(clean_env_cmd, shell=True)
    if clean_res != 0:
        logging.warn("clean env failed")
        exit(-1)
    logging.info("clean all env ok")
