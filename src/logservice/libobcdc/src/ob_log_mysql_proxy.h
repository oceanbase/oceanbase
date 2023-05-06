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
 * OBCDCMySQLProxy define
 */

#ifndef OCEANBASE_LIBOBCDC_MYSQL_PROXY_H__
#define OCEANBASE_LIBOBCDC_MYSQL_PROXY_H__

#include "lib/task/ob_timer.h"                            // ObTimer
#include "lib/mysqlclient/ob_mysql_proxy.h"               // ObMySQLProxy
#include "lib/mysqlclient/ob_mysql_connection_pool.h"     // ObMySQLConnectionPool

namespace oceanbase
{
namespace common
{
class ObCommonConfig;

namespace sqlclient
{
class ObMySQLServerProvider;
} // namespace sqlclient
} // namespace common

namespace libobcdc
{

///////////////////////////////////// ObLogMysqlProxy /////////////////////////////////
typedef common::sqlclient::ObMySQLServerProvider ServerProviderType;
typedef common::sqlclient::ObMySQLConnectionPool ConnectionPoolType;

class ObLogMysqlProxy
{
public:
  ObLogMysqlProxy();
  virtual ~ObLogMysqlProxy();

public:
  int init(
      const char *cluster_user,
      const char *cluster_password,
      const int64_t sql_conn_timeout_us,
      const int64_t sql_query_timeout_us,
      const bool enable_ssl_client_authentication,
      ServerProviderType *server_provider,
      bool is_tenant_server_provider = false);
  void destroy();
  void stop();

  common::ObMySQLProxy &get_ob_mysql_proxy() { return mysql_proxy_; }
  void refresh_conn_pool() { connection_pool_.signal_refresh(); }
  bool is_oracle_mode() const { return is_oracle_mode_; }

private:
  int detect_tenant_mode_(ServerProviderType *server_provider);

private:
  bool                  inited_;

  char                  cluster_user_[common::OB_MAX_USER_NAME_BUF_LENGTH];
  char                  cluster_password_[common::OB_MAX_PASSWORD_LENGTH + 1];
  bool                  is_oracle_mode_;

  ConnectionPoolType    connection_pool_;
  // Thread-safe proxies, getting connections and locking
  common::ObMySQLProxy  mysql_proxy_;
  int                   tg_id_; // timer tg id

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogMysqlProxy);
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_MYSQL_PROXY_H__ */
