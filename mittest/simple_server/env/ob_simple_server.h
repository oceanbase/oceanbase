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

class ObSimpleServer
{
public:
  static const int64_t MAX_WAIT_TENANT_SCHEMA_TIME = 20_s;

public:
  ObSimpleServer(const std::string &env_prefix,
                 const char *log_disk_size = "10G",
                 const char *memory_limit = "10G",
                 ObServer &server = ObServer::get_instance(),
                 const std::string &dir_prefix = "./store_");
  ~ObSimpleServer() { reset(); }
  ObServer& get_observer() { return server_; }
  int simple_init();
  int simple_start();
  int simple_close();
  std::string get_local_ip();
  int get_mysql_port() { return mysql_port_; }
  int bootstrap();
  void reset();
  common::ObMySQLProxy &get_sql_proxy() { return sql_proxy_; }
  common::ObMySQLProxy &get_sql_proxy2() { return sql_proxy2_; }
  common::ObMySQLProxy &get_sql_proxy_with_short_wait() { return sql_proxy_with_short_wait_; }
  common::ObAddr get_addr() {
    common::ObAddr addr;
    addr.set_ip_addr(local_ip_.c_str(), rpc_port_);
    return addr;
  }

  int init_sql_proxy2(const char *tenant_name = "tt1", const char *db_name="test", const bool oracle_mode = false);
  int init_sql_proxy_with_short_wait();
protected:
  int init_sql_proxy();

private:
  ObServer &server_;
  std::thread th_;
  std::string local_ip_;
  int rpc_port_;
  int mysql_port_;
  const char *log_disk_size_;
  const char *memory_limit_;
  std::string data_dir_;
  std::string rs_list_;
  std::string optstr_;
  std::string run_dir_;
  common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool_;
  common::ObMySQLProxy sql_proxy_;
  common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool2_;
  common::ObMySQLProxy sql_proxy2_;
  common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool_with_short_wait_;
  common::ObMySQLProxy sql_proxy_with_short_wait_;
  int server_fd_;
  bool set_bootstrap_warn_log_;
};


} // end observer
} // end oceanbase
