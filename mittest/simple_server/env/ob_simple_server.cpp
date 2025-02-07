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
#include "ob_simple_server.h"
#include "ob_simple_server_restart_helper.h"
#include "observer/ob_server.h"
#include "storage/tx_storage/ob_ls_service.h"

#undef private
#undef protected

#include "lib/allocator/ob_libeasy_mem_pool.h"
#include "ob_mittest_utils.h"



namespace oceanbase
{
const char *shared_storage_info __attribute__((weak)) = NULL;

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

ObSimpleServer::ObSimpleServer(const std::string &env_prefix,
                               const char *log_disk_size,
                               const char *memory_limit,
                               const char *datafile_size,
                               ObServer &server,
                               const std::string &dir_prefix)
  : server_(server),
    log_disk_size_(log_disk_size),
    memory_limit_(memory_limit),
    datafile_size_(datafile_size),
    data_dir_(dir_prefix),
    run_dir_(env_prefix)
{
  if (ObSimpleServerRestartHelper::is_restart_) {
    std::string port_file_name = run_dir_ + std::string("/port.txt");
    FILE *infile = nullptr;
    if (nullptr == (infile = fopen(port_file_name.c_str(), "r"))) {
      ob_abort();
    }
    fscanf(infile, "%d\n", &rpc_port_);
  } else {
    rpc_port_ = unittest::get_rpc_port(server_fd_);
  }
  mysql_port_ = rpc_port_ + 1;
}

std::string ObSimpleServer::get_local_ip()
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

int ObSimpleServer::simple_init()
{
  int ret = OB_SUCCESS;

  local_ip_ = get_local_ip();
  if (local_ip_ == "") {
    SERVER_LOG(WARN, "get_local_ip failed");
    return -666666666;
  }

  easy_pool_set_allocator(ob_easy_realloc);
  ev_set_allocator(ob_easy_realloc);

  ObServerOptions opts;
  opts.cluster_id_ = 1;
  opts.rpc_port_ = rpc_port_;
  opts.mysql_port_ = mysql_port_;
  opts.data_dir_ = data_dir_.c_str();
  opts.zone_ = "zone1";
  opts.appname_ = "test_ob";
  rs_list_ = local_ip_ + ":" + std::to_string(opts.rpc_port_) + ":" + std::to_string(opts.mysql_port_);
  opts.rs_list_ = rs_list_.c_str();
  // NOTE: memory_limit must keep same with log_disk_size
  optstr_ = std::string();
  optstr_ = optstr_ + "log_disk_size=" + std::string(log_disk_size_) + ",memory_limit=" + std::string(memory_limit_) + ",cache_wash_threshold=1G,net_thread_count=4,cpu_count=16,schema_history_expire_time=1d,workers_per_cpu_quota=10,datafile_disk_percentage=2,__min_full_resource_pool_memory=1073741824,system_memory=5G,trace_log_slow_query_watermark=100ms,datafile_size=" + std::string(datafile_size_) +",stack_size=512K";
  opts.optstr_ = optstr_.c_str();
  //opts.devname_ = "eth0";
  opts.use_ipv6_ = false;

  char *curr_dir = get_current_dir_name();
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);


  if (ObSimpleServerRestartHelper::is_restart_) {
    if (OB_FAIL(chdir(run_dir_.c_str()))) {
      SERVER_LOG(WARN, "change dir failed.", KR(ret), K(run_dir_.c_str()));
    } else {
      SERVER_LOG(INFO, "change dir done.", K(run_dir_.c_str()));
    }
  } else {
    // first
    system(("rm -rf " + run_dir_).c_str());
    system(("rm -f *born*log*"));
    system(("rm -f *restart*log*"));

    //check admin sys package exist
    DIR *admin_dir = opendir("admin");
    SERVER_LOG(INFO, "create dir and change work dir start.", K(run_dir_.c_str()));
    if (OB_FAIL(mkdir(run_dir_.c_str(), 0777))) {
    } else if (OB_FAIL(chdir(run_dir_.c_str()))) {
    } else {
      const char *current_dir = run_dir_.c_str();
      SERVER_LOG(INFO, "create dir and change work dir done.", K(current_dir));
    }
    // mkdir
    std::string data_dir = opts.data_dir_;
    std::vector<std::string> dirs;
    dirs.push_back(opts.data_dir_);
    dirs.push_back("run");
    dirs.push_back("etc");
    dirs.push_back("log");
    dirs.push_back("wallet");
    dirs.push_back("admin");

    dirs.push_back(data_dir + "/clog");
    dirs.push_back(data_dir + "/slog");
    dirs.push_back(data_dir + "/sstable");
    for (auto &dir : dirs) {
      ret = mkdir(dir.c_str(), 0777);
      if (OB_FAIL(ret)) {
        SERVER_LOG(ERROR, "ObSimpleServer mkdir", K(ret), K(dir.c_str()));
        return ret;
      }
    }
    if (admin_dir != NULL) {
      system(("cp ../admin/* admin/"));
    }

    std::string port_file_name = std::string("port.txt");
    FILE *outfile = nullptr;
    if (nullptr == (outfile = fopen(port_file_name.c_str(), "w"))) {
      ob_abort();
    }
    fprintf(outfile, "%d\n", rpc_port_);
    fclose(outfile);
    fprintf(stdout, "rpc_port = %d mysql_port = %d\n", rpc_port_, rpc_port_+1);
  }

  // 因为改变了工作目录，设置为绝对路径
  for (int i=0;i<MAX_FD_FILE;i++) {
    int len = strlen(OB_LOGGER.log_file_[i].filename_);
    if (len > 0) {
      std::string ab_file = std::string(curr_dir) + "/" + std::string(OB_LOGGER.log_file_[i].filename_);
      SERVER_LOG(INFO, "convert ab file", K(ab_file.c_str()));
      MEMCPY(OB_LOGGER.log_file_[i].filename_, ab_file.c_str(), ab_file.size());
    }
  }

  ObPLogWriterCfg log_cfg;

  ret = server_.init(opts, log_cfg);
  if (OB_FAIL(ret)) {
    return ret;
  }
  ret = init_sql_proxy();

  return ret;
}

int ObSimpleServer::init_sql_proxy()
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

int ObSimpleServer::init_sql_proxy2(const char *tenant_name, const char *db_name, const bool oracle_mode)
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
  param.sqlclient_per_observer_conn_limit_ = 10000;
  ret = sql_conn_pool2_.init(db_addr, param);
  if (OB_SUCC(ret)) {
    sql_conn_pool2_.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
    ret = sql_proxy2_.init(&sql_conn_pool2_);
  }

  return ret;
}

int ObSimpleServer::init_sql_proxy_with_short_wait()
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

int ObSimpleServer::simple_start()
{
  int ret = simple_init();
  if (OB_FAIL(ret)) {
    return ret;
  }
  // bootstrap
  if (!ObSimpleServerRestartHelper::is_restart_) {
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
      SERVER_LOG(INFO, "ObSimpleServer bootstrap th exit");
    });
    th_ = std::move(th);
  }
  SERVER_LOG(INFO, "ObSimpleServer init succ prepare to start...");
  ret = server_.start();
  if (!ObSimpleServerRestartHelper::is_restart_) {
    th_.join();
  }
  if (OB_SUCC(ret)) {
    SERVER_LOG(INFO, "ObSimpleServer start succ");
  } else {
    SERVER_LOG(WARN, "ObSimpleServer start failed", K(ret));
    // fprintf(stdout, "start failed. ret = %d\n", ret);
    ob_abort();
  }
  return ret;
}

int ObSimpleServer::bootstrap()
{
  SERVER_LOG(INFO, "ObSimpleServer::bootstrap start");
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

  obrpc::ObNetClient client;
  obrpc::ObSrvRpcProxy srv_proxy;

  if (OB_FAIL(client.init())) {
    SERVER_LOG(WARN, "client init failed", K(ret));
  } else if (OB_FAIL(client.get_proxy(srv_proxy))) {
    SERVER_LOG(WARN, "get_proxy failed", K(ret));
  } else {
    const int64_t timeout = 180 * 1000 * 1000; //180s
    common::ObAddr dst_server(common::ObAddr::IPV4, local_ip_.c_str(), rpc_port_);
    srv_proxy.set_server(dst_server);
    srv_proxy.set_timeout(timeout);
    obrpc::ObServerInfo server_info;
    server_info.zone_ = "zone1";
    server_info.server_ = common::ObAddr(common::ObAddr::IPV4, local_ip_.c_str(), rpc_port_);
    server_info.region_ = "sys_region";
    obrpc::ObBootstrapArg arg;
    arg.cluster_role_ = common::PRIMARY_CLUSTER;
    arg.server_list_.push_back(server_info);
#ifdef OB_BUILD_SHARED_STORAGE
    if (NULL != shared_storage_info) {
      ObString shared_storage_info_str(strlen(shared_storage_info), shared_storage_info);
      arg.shared_storage_info_ = shared_storage_info_str;
    }
#endif
    if (OB_FAIL(srv_proxy.bootstrap(arg))) {
      SERVER_LOG(WARN, "bootstrap failed", K(arg), K(ret));
    }
  }
  SERVER_LOG(INFO, "ObSimpleServer::bootstrap end", K(ret));
  return ret;
}

int ObSimpleServer::simple_close()
{
  SERVER_LOG(INFO, "ObSimpleServer::simple_close start");
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

  SERVER_LOG(INFO, "ObSimpleServer::simple_close set_stop");
  server_.set_stop();

  SERVER_LOG(INFO, "ObSimpleServer::simple_close wait");
  ret = server_.wait();

  SERVER_LOG(INFO, "ObSimpleServer::simple_close destroy");
  server_.destroy();
  SERVER_LOG(INFO, "ObSimpleServer::simple_close destroy");
  ObKVGlobalCache::get_instance().destroy();
  SERVER_LOG(INFO, "ObSimpleServer::simple_close destroy");
  ObVirtualTenantManager::get_instance().destroy();
  SERVER_LOG(INFO, "ObSimpleServer::simple_close end", K(ret));

  SERVER_LOG(INFO, "ObSimpleServer::simple_close end", K(ret));
  return ret;
}

void ObSimpleServer::reset()
{
}

} // end observer
} // end oceanbase
