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
 *
 * ObCDCRSSQLServerProvider define
 * Get the RootsServer list from ConfigURL or RSLIST
 */

#ifndef OCEANBASE_LIBOBCDC_SQL_SERVER_PROVIDER_H__
#define OCEANBASE_LIBOBCDC_SQL_SERVER_PROVIDER_H__

#include "lib/mysqlclient/ob_mysql_server_provider.h"   // ObMySQLServerProvider
#include "lib/container/ob_se_array.h"                  // ObSEArray
#include "lib/container/ob_array.h"                     // ObArray
#include "share/ob_root_addr_agent.h"                   // ObRootAddr
#include "lib/lock/ob_spin_rwlock.h"                    // SpinRWLock
#include "lib/lock/ob_small_spin_lock.h"                // ObByteLock
#include "ob_log_svr_blacklist.h"                       // ObLogSvrBlacklist
#include "ob_log_config.h"                              // TCONF, ObLogConfig

namespace oceanbase
{
namespace libobcdc
{
class ObLogSQLServerProvider : public common::sqlclient::ObMySQLServerProvider
{
  static const int64_t MAX_CONFIG_LENGTH = 1 << 10; // for config_url and rootservice_list
  static const int64_t DEFAULT_ROOT_SERVER_NUM = 16;
  static const int64_t REFRESH_SERVER_LIST_TIMEOUT_MS = 60 * 1000 * 1000;
  static const int64_t FETCH_RS_RETRY_INTERVAL_ON_INIT_FAIL = 100 * 1000;  // 100ms Retry to retrieve RS list
  static const char *DEFAULT_VALUE_OF_RS_CONF; // default value of config rootserver_list and config_url

  typedef common::ObSEArray<share::ObRootAddr, DEFAULT_ROOT_SERVER_NUM> ServerList;

public:
  ObLogSQLServerProvider();
  virtual ~ObLogSQLServerProvider();

public:
  virtual int get_server(const int64_t svr_idx, common::ObAddr &server);
  virtual int64_t get_server_count() const;
  virtual int get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids) override;
  virtual int get_tenant_servers(const uint64_t tenant_id, common::ObIArray<ObAddr> &tenant_servers) override;
  // Aone:
  // Call when ObMySQLConnectionPool background timing task refresh, in the process of refreshing the connection pool of read and write locks, when the config server exception, parsing does not return, resulting in write locks can not be released, thus affecting the Formatter refresh schema to get server list, resulting in Delay
  // Optimisation options.
  // (1) Timed task do nothing in the background, relying on ObLog active refresh calls
  // (2) Concurrent correctness guarantee: rely on ObLogSQLServerProvider's refresh_lock_
  // (3) Set libcurl connection timeout
  virtual int refresh_server_list(void);
  virtual int prepare_refresh() override;
  virtual int end_refresh() override;

  // Called by ObLog active refresh
  int call_refresh_server_list(void);

public:
  int init(const char *config_url, const char *rs_list);
  void destroy();
  void configure(const ObLogConfig &cfg);

private:
  // str_len of rs_list should be greater than 1(default valud is ';') and less than MAX_CONFIG_LENGTH
  int check_rs_list_valid_(const char *rs_list);
  int init_by_rs_list_conf_(const char *rs_list);
  // rs list is valid or not(multi svr split by `;`), format: ip:rpc_port:sql_port
  int parse_rs_list_(const char *rs_list, ServerList &server_list);
  // get copy of rs_lislt_ (conf str), caller should guaratee call this function when is_using_rs_list_ = true
  int get_copy_of_rs_list_conf_(char *rs_list_copy);
  int refresh_by_rs_list_conf_(const char *rs_list, ServerList &server_list);
  int init_by_cluster_url_(const char *config_url);
  int refresh_until_success_(const char *url, ServerList &server_list);
  int refresh_by_cluster_url_(const char *url, ServerList &server_list);
  // get valid rs server list after filter by server blacklist
  int get_svr_list_based_on_blacklist_(ServerList &server_list);
  int filter_by_svr_blacklist_(ServerList &server_list,
      common::ObArray<common::ObAddr> &remove_svrs);

private:
  bool        inited_;
  bool        is_using_rs_list_;
  char        rs_list_[MAX_CONFIG_LENGTH];
  char        config_url_[MAX_CONFIG_LENGTH];
  ServerList  server_list_;

  mutable common::SpinRWLock refresh_lock_; // lock to protect refresh ServerList server_list_
  mutable common::ObByteLock rs_conf_lock_; // lock to protect refresh conf(char*) rs_list_
  ObLogSvrBlacklist          svr_blacklist_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSQLServerProvider);
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_SQL_SERVER_PROVIDER_H__ */
