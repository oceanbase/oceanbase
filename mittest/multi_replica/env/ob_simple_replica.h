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

#pragma once

#include "observer/ob_server.h"
#include "lib/net/ob_addr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "rpc/obrpc/ob_net_client.h"
#include "share/ob_rpc_struct.h"
#include "lib/mysqlclient/ob_single_mysql_connection_pool.h"

namespace oceanbase
{
namespace observer
{

#define DEFAULT_TEST_TENANT_NAME "tt1"

class ObSimpleServerReplica
{
public:
  static const int64_t MAX_WAIT_TENANT_SCHEMA_TIME = 20_s;

  static int64_t get_rpc_port(int &server_fd);
public:
  ObSimpleServerReplica(const std::string &app_name,
                        const std::string &env_prefix,
                        const int zone_id,
                        const int rpc_port,
                        const string &rs_list,
                        const ObServerInfoList &server_list,
                        bool is_restart,
                        ObServer &server = ObServer::get_instance(),
                        const std::string &dir_prefix = "./store",
                        const char *log_disk_size = "10G",
                        const char *memory_limit = "10G");
  ~ObSimpleServerReplica() { reset(); }
  ObServer &get_observer() { return server_; }
  int simple_init();
  int simple_start();
  int simple_close();
  std::string get_local_ip();
  int bootstrap();
  void reset();
  common::ObMySQLProxy &get_sql_proxy() { return sql_proxy_; }
  common::ObMySQLProxy &get_sql_proxy2() { return sql_proxy2_; }
  common::ObMySQLProxy &get_sql_proxy_with_short_wait() { return sql_proxy_with_short_wait_; }
  common::ObAddr get_addr()
  {
    common::ObAddr addr;
    addr.set_ip_addr(local_ip_.c_str(), rpc_port_);
    return addr;
  }

  int init_sql_proxy2(const char *tenant_name = DEFAULT_TEST_TENANT_NAME,
                      const char *db_name = "test",
                      const bool oracle_mode = false);
  int init_sql_proxy_with_short_wait();

protected:
  int init_sql_proxy();

private:
  ObServer &server_;
  std::thread th_;
  std::string local_ip_;
    int zone_id_;
  int rpc_port_;
  int mysql_port_;
  std::string rs_list_;
  ObServerInfoList server_info_list_;
  std::string app_name_;
  std::string data_dir_;
  std::string optstr_;
  std::string run_dir_;
  const char *log_disk_size_;
  const char *memory_limit_;
  common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool_;
  common::ObMySQLProxy sql_proxy_;
  common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool2_;
  common::ObMySQLProxy sql_proxy2_;
  common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool_with_short_wait_;
  common::ObMySQLProxy sql_proxy_with_short_wait_;
  int server_fd_;
  bool set_bootstrap_warn_log_;

  bool is_restart_;

  obrpc::ObNetClient bootstrap_client_;
  obrpc::ObSrvRpcProxy bootstrap_srv_proxy_;
};

} // namespace observer
} // namespace oceanbase
