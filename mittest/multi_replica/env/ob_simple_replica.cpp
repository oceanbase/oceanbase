/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include<dirent.h>
#include <memory>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#define private public
#define protected public

#include "lib/oblog/ob_log.h"
#include "observer/ob_server.h"
#include "storage/tx_storage/ob_ls_service.h"

#undef private
#undef protected

#include "ob_simple_replica.h"
#include "lib/allocator/ob_libeasy_mem_pool.h"
#include "ob_mittest_utils.h"



namespace oceanbase
{
namespace observer
{

uint32_t get_local_addr(const char *dev_name)
{
  int fd, intrface;
  struct ifreq buf[16];
  struct ifconf ifc;

  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    return 0;
  }

  ifc.ifc_len = sizeof(buf);
  ifc.ifc_buf = (caddr_t)buf;
  if (ioctl(fd, SIOCGIFCONF, (char *)&ifc) != 0) {
    close(fd);
    return 0;
  }

  intrface = static_cast<int>(ifc.ifc_len / sizeof(struct ifreq));
  while (intrface-- > 0) {
    if (ioctl(fd, SIOCGIFFLAGS, (char *)&buf[intrface]) != 0) {
      continue;
    }
    if ((buf[intrface].ifr_flags & IFF_LOOPBACK) != 0)
      continue;
    if (!(buf[intrface].ifr_flags & IFF_UP))
      continue;
    if (dev_name != NULL && strcmp(dev_name, buf[intrface].ifr_name))
      continue;
    if (!(ioctl(fd, SIOCGIFADDR, (char *)&buf[intrface]))) {
      close(fd);
      return ((struct sockaddr_in *)(&buf[intrface].ifr_addr))->sin_addr.s_addr;
    }
  }
  close(fd);
  return 0;
}

int64_t ObSimpleServerReplica::get_rpc_port(int &server_fd)
{
  return unittest::get_rpc_port(server_fd);
}

ObSimpleServerReplica::ObSimpleServerReplica(const std::string &app_name,
                                             const std::string &env_prefix,
                                             const int zone_id,
                                             const int rpc_port,
                                             const string &rs_list,
                                             const ObServerInfoList &server_list,
                                             bool is_restart,
                                             ObServer &server,
                                             const std::string &dir_prefix,
                                             const char *log_disk_size,
                                             const char *memory_limit)
    : server_(server), zone_id_(zone_id), rpc_port_(rpc_port), rs_list_(rs_list),
      server_info_list_(server_list), app_name_(app_name), data_dir_(dir_prefix),
      run_dir_(env_prefix), log_disk_size_(log_disk_size), memory_limit_(memory_limit),
      is_restart_(is_restart)
{
  // if (ObSimpleServerReplicaRestartHelper::is_restart_) {
  //   std::string port_file_name = run_dir_ + std::string("/port.txt");
  //   FILE *infile = nullptr;
  //   if (nullptr == (infile = fopen(port_file_name.c_str(), "r"))) {
  //     ob_abort();
  //   }
  //   fscanf(infile, "%d\n", &rpc_port_);
  // } else {
  // rpc_port_ = unittest::get_rpc_port(server_fd_);
  // }
  mysql_port_ = rpc_port_ + 1;
}

std::string ObSimpleServerReplica::get_local_ip()
{
  uint32_t ip = get_local_addr("bond0");
  if (ip == 0) {
    ip = get_local_addr("eth0");
  }
  if (ip == 0) {
    return "";
  }
  return inet_ntoa(*(struct in_addr *)(&ip));
}

int ObSimpleServerReplica::simple_init()
{
  int ret = OB_SUCCESS;

  local_ip_ = get_local_ip();
  if (local_ip_ == "") {
    SERVER_LOG(WARN, "get_local_ip failed");
    return -666666666;
  }

  easy_pool_set_allocator(ob_easy_realloc);
  ev_set_allocator(ob_easy_realloc);

  std::string zone_str = "zone" + std::to_string(zone_id_);

  ObServerOptions opts;
  opts.cluster_id_ = 1;
  opts.rpc_port_ = rpc_port_;
  opts.mysql_port_ = mysql_port_;
  opts.data_dir_ = data_dir_.c_str();
  opts.zone_ = zone_str.c_str();
  opts.appname_ = "test_ob";
  opts.rs_list_ = rs_list_.c_str();
  // NOTE: memory_limit must keep same with log_disk_size
  optstr_ = std::string();
  optstr_ = optstr_ + "log_disk_size=" + std::string(log_disk_size_)
            + ",memory_limit=" + std::string(memory_limit_)
            + ",cache_wash_threshold=1G,net_thread_count=4,cpu_count=16,schema_history_expire_time="
              "1d,workers_per_cpu_quota=10,datafile_disk_percentage=2,__min_full_resource_pool_"
              "memory=2147483648,system_memory=5G,trace_log_slow_query_watermark=100ms,datafile_"
              "size=10G,stack_size=512K";
  opts.optstr_ = optstr_.c_str();
  // opts.devname_ = "eth0";
  opts.use_ipv6_ = false;

  char *curr_dir = get_current_dir_name();

  if (OB_FAIL(chdir(run_dir_.c_str()))) {
    SERVER_LOG(WARN, "change dir failed.", KR(ret), K(curr_dir), K(run_dir_.c_str()), K(errno));
  } else {
    SERVER_LOG(INFO, "change dir done.", K(curr_dir), K(run_dir_.c_str()));
  }
  fprintf(stdout,
          "[PID:%d] init opt : zone_id = %d, rpc_port = %d, mysql_port = %d, zone = %s, "
          "all_server_count = "
          "%ld, rs_list = %s\n",
          getpid(), zone_id_, rpc_port_, mysql_port_, zone_str.c_str(), server_info_list_.count(),
          rs_list_.c_str());


  // 因为改变了工作目录，设置为绝对路径
  for (int i = 0; i < MAX_FD_FILE; i++) {
    int len = strlen(OB_LOGGER.log_file_[i].filename_);
    if (len > 0) {
      std::string cur_file_name = OB_LOGGER.log_file_[i].filename_;
      cur_file_name = cur_file_name.substr(cur_file_name.find_last_of("/\\") + 1);
      std::string ab_file = std::string(curr_dir) + "/" + run_dir_ + "/"
                            + cur_file_name;
      SERVER_LOG(INFO, "convert ab file", K(ab_file.c_str()));
      MEMCPY(OB_LOGGER.log_file_[i].filename_, ab_file.c_str(), ab_file.size());
    }
  }
  // std::string ab_file = std::string(curr_dir) + "/" + run_dir_ + "/" + app_name_;
  //
  // std::string app_log_name = ab_file + ".log";
  // std::string app_rs_log_name = ab_file + "_rs.log";
  // std::string app_ele_log_name = ab_file + "_election.log";
  // std::string app_trace_log_name = ab_file + "_trace.log";
  // OB_LOGGER.set_file_name(app_log_name.c_str(),
  //                         true,
  //                         false,
  //                         app_rs_log_name.c_str(),
  //                         app_ele_log_name.c_str(),
  //                         app_trace_log_name.c_str());

  ObPLogWriterCfg log_cfg;
  ret = server_.init(opts, log_cfg);
  if (OB_FAIL(ret)) {
    return ret;
  }
  ret = init_sql_proxy();

  if (OB_SUCC(ret)) {
    if (OB_FAIL(bootstrap_client_.init())) {
      SERVER_LOG(WARN, "client init failed", K(ret));
    } else if (OB_FAIL(bootstrap_client_.get_proxy(bootstrap_srv_proxy_))) {
      SERVER_LOG(WARN, "get_proxy failed", K(ret));
    }
  }
  return ret;
}

int ObSimpleServerReplica::init_sql_proxy()
{
  int ret = OB_SUCCESS;
  sql_conn_pool_.set_db_param("root@sys", "", "test");
  common::ObAddr db_addr;
  db_addr.set_ip_addr(local_ip_.c_str(), mysql_port_);

  ObConnPoolConfigParam param;
  //param.sqlclient_wait_timeout_ = 10; // 10s
  // turn up it, make unittest pass
  param.sqlclient_wait_timeout_ = 1000; // 300s
  param.long_query_timeout_ = 300*1000*1000; // 120s
  param.connection_refresh_interval_ = 200*1000; // 200ms
  param.connection_pool_warn_time_ = 10*1000*1000; // 1s
  param.sqlclient_per_observer_conn_limit_ = 1000;
  ret = sql_conn_pool_.init(db_addr, param);
  if (OB_SUCC(ret)) {
    sql_conn_pool_.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
    ret = sql_proxy_.init(&sql_conn_pool_);
  }

  return ret;
}

int ObSimpleServerReplica::init_sql_proxy2(const char *tenant_name, const char *db_name, const bool oracle_mode)
{
  int ret = OB_SUCCESS;
  std::string user = oracle_mode ? "sys@" : "root@";
  sql_conn_pool2_.set_db_param((user + std::string(tenant_name)).c_str(), "", db_name);
  common::ObAddr db_addr;
  db_addr.set_ip_addr(local_ip_.c_str(), mysql_port_);

  ObConnPoolConfigParam param;
  //param.sqlclient_wait_timeout_ = 10; // 10s
  // turn up it, make unittest pass
  param.sqlclient_wait_timeout_ = 1000; // 100s
  param.long_query_timeout_ = 300*1000*1000; // 120s
  param.connection_refresh_interval_ = 200*1000; // 200ms
  param.connection_pool_warn_time_ = 10*1000*1000; // 1s
  param.sqlclient_per_observer_conn_limit_ = 1000;
  ret = sql_conn_pool2_.init(db_addr, param);
  if (OB_SUCC(ret)) {
    sql_conn_pool2_.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
    ret = sql_proxy2_.init(&sql_conn_pool2_);
  }

  return ret;
}

int ObSimpleServerReplica::init_sql_proxy_with_short_wait()
{
  int ret = OB_SUCCESS;
  sql_conn_pool_with_short_wait_.set_db_param("root@sys", "", "test");
  common::ObAddr db_addr;
  db_addr.set_ip_addr(local_ip_.c_str(), mysql_port_);

  ObConnPoolConfigParam param;
  //param.sqlclient_wait_timeout_ = 10; // 10s
  // turn up it, make unittest pass
  param.sqlclient_wait_timeout_ = 3; // 3s
  param.long_query_timeout_ = 3*1000*1000; // 3s
  param.connection_refresh_interval_ = 200*1000; // 200ms
  param.connection_pool_warn_time_ = 10*1000*1000; // 1s
  param.sqlclient_per_observer_conn_limit_ = 1000;
  ret = sql_conn_pool_with_short_wait_.init(db_addr, param);
  if (OB_SUCC(ret)) {
    sql_conn_pool_with_short_wait_.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
    ret = sql_proxy_with_short_wait_.init(&sql_conn_pool_with_short_wait_);
  }

  return ret;
}

int ObSimpleServerReplica::simple_start()
{
  int ret = OB_SUCCESS;
  // bootstrap
  if (zone_id_ == 1 && !is_restart_) {
    std::thread th([this]() {
      int64_t start_time = ObTimeUtility::current_time();
      int ret = OB_SUCCESS;
      int64_t curr_time = ObTimeUtility::current_time();
      while (curr_time - start_time < 5 * 60 * 1000 * 1000) {
        ret = this->bootstrap();
        if (OB_SUCC(ret)) {
          break;
        }
        ::usleep(200 * 1000);
        curr_time = ObTimeUtility::current_time();
      }
      SERVER_LOG(INFO, "ObSimpleServerReplica bootstrap th exit", K(ret), K(zone_id_), K(rpc_port_),
                 K(mysql_port_));
    });
    th_ = std::move(th);
  }
  SERVER_LOG(INFO, "ObSimpleServerReplica init succ prepare to start...", K(zone_id_), K(rpc_port_),
             K(mysql_port_));
  ret = server_.start();
  if (zone_id_ == 1 && !is_restart_) {
    th_.join();
    fprintf(stdout, "[BOOTSTRAP SUCC] zone_id = %d, rpc_port = %d, mysql_port = %d\n", zone_id_,
            rpc_port_, mysql_port_);
  }
  if (OB_SUCC(ret)) {
    SERVER_LOG(INFO, "ObSimpleServerReplica start succ", K(zone_id_), K(rpc_port_), K(mysql_port_));
    fprintf(stdout, "[START OBSERVER SUCC] zone_id = %d, rpc_port = %d, mysql_port = %d\n",
            zone_id_, rpc_port_, mysql_port_);
  } else {
    SERVER_LOG(WARN, "ObSimpleServerReplica start failed", K(ret), K(zone_id_), K(rpc_port_),
               K(mysql_port_));
    // fprintf(stdout, "start failed. ret = %d\n", ret);
    ob_abort();
  }
  return ret;
}

int ObSimpleServerReplica::bootstrap()
{
  SERVER_LOG(INFO, "ObSimpleServerReplica::bootstrap start", K(zone_id_), K(rpc_port_), K(mysql_port_));
  int ret = OB_SUCCESS;
  /*
  if (server_.get_gctx().ob_service_ == nullptr) {
    ret = -66666666;
    SERVER_LOG(INFO, "observice is nullptr");
  } else {
    // observer内部有线程的检查, 这里在新建线程下调用会有问题
    obrpc::ObServerInfo server_info;
    server_info.zone_ = "zone1";
    server_info.server_ = common::ObAddr(common::ObAddr::IPV4, local_ip_.c_str(), rpc_port_);
    server_info.region_ = "sys_region";
    obrpc::ObBootstrapArg arg;
    arg.cluster_role_ = common::PRIMARY_CLUSTER;
    arg.server_list_.push_back(server_info);
    SERVER_LOG(INFO, "observice.bootstrap call", K(arg), K(ret));
    ret = server_.get_gctx().ob_service_->bootstrap(arg);
    SERVER_LOG(INFO, "observice.bootstrap return", K(arg), K(ret));
  }
  */

  // obrpc::ObNetClient client;
  // obrpc::ObSrvRpcProxy srv_proxy;

  // } else {
    const int64_t timeout = 180 * 1000 * 1000; //180s
    common::ObAddr dst_server(common::ObAddr::IPV4, local_ip_.c_str(), rpc_port_);
    bootstrap_srv_proxy_.set_server(dst_server);
    bootstrap_srv_proxy_.set_timeout(timeout);
    // obrpc::ObServerInfo server_info;
    // std::string zone_str = "zone" +std::tostrin
    // server_info.zone_ = "";
    // server_info.server_ = common::ObAddr(common::ObAddr::IPV4, local_ip_.c_str(), rpc_port_);
    // server_info.region_ = "sys_region";
    obrpc::ObBootstrapArg arg;
    arg.cluster_role_ = common::PRIMARY_CLUSTER;
    arg.server_list_.assign(server_info_list_);
    if (OB_FAIL(bootstrap_srv_proxy_.bootstrap(arg))) {
      SERVER_LOG(WARN, "bootstrap failed", K(arg), K(ret));
    }
  // }
  SERVER_LOG(INFO, "ObSimpleServerReplica::bootstrap end", K(ret), K(zone_id_), K(rpc_port_), K(mysql_port_));
  return ret;
}

int ObSimpleServerReplica::simple_close()
{
  SERVER_LOG(INFO, "ObSimpleServerReplica::simple_close start");
  int ret = OB_SUCCESS;

  // remove ls for exit
  /*
  ObSEArray<uint64_t, 16> tenant_ids;
  GCTX.omt_->get_mtl_tenant_ids(tenant_ids);

  auto do_remove_ls = [] (uint64_t tenant_id) {
    int ret = OB_SUCCESS;
    share::ObTenantSwitchGuard guard;
    ObLS *ls;
    if (OB_SUCC(guard.switch_to(tenant_id))) {
      ObSEArray<share::ObLSID, 10> ls_ids;
      common::ObSharedGuard<storage::ObLSIterator> ls_iter;
      if (OB_SUCC(MTL(ObLSService*)->get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
        while (true) {
          if (OB_SUCC(ls_iter->get_next(ls))) {
            ls_ids.push_back(ls->get_ls_id());
          } else {
            break;
          }
        }
      }
      ls_iter.reset();
      SERVER_LOG(INFO, "safe quit need remove ls", K(MTL_ID()), K(ls_ids));
      for (int i = 0; i < ls_ids.count(); i++) {
        if (ls_ids.at(i).id() > share::ObLSID::SYS_LS_ID) {
          MTL(ObLSService*)->remove_ls(ls_ids.at(i));
        }
      }
      MTL(ObLSService*)->remove_ls(share::ObLSID{share::ObLSID::SYS_LS_ID});
    }

  };
  for (int64_t i = 0; i < tenant_ids.count(); i++) {
    if (tenant_ids.at(i) != OB_SYS_TENANT_ID) {
      do_remove_ls(tenant_ids.at(i));
    }
  }
  do_remove_ls(OB_SYS_TENANT_ID);
  */

  sql_conn_pool_.stop();
  sql_conn_pool_.close_all_connection();
  sql_conn_pool2_.stop();
  sql_conn_pool2_.close_all_connection();

  SERVER_LOG(INFO, "ObSimpleServerReplica::simple_close set_stop");
  server_.set_stop();

  SERVER_LOG(INFO, "ObSimpleServerReplica::simple_close wait");
  ret = server_.wait();

  SERVER_LOG(INFO, "ObSimpleServerReplica::simple_close destroy");
  server_.destroy();
  SERVER_LOG(INFO, "ObSimpleServerReplica::simple_close destroy");
  ObKVGlobalCache::get_instance().destroy();
  SERVER_LOG(INFO, "ObSimpleServerReplica::simple_close destroy");
  ObVirtualTenantManager::get_instance().destroy();
  SERVER_LOG(INFO, "ObSimpleServerReplica::simple_close end", K(ret));

  SERVER_LOG(INFO, "ObSimpleServerReplica::simple_close end", K(ret));
  return ret;
}

void ObSimpleServerReplica::reset()
{
}

} // end observer
} // end oceanbase
