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

#ifndef OCEANBASE_LIBOBLOG_MYSQL_PROXY_H__
#define OCEANBASE_LIBOBLOG_MYSQL_PROXY_H__

#include "lib/task/ob_timer.h"                            // ObTimer
#include "lib/mysqlclient/ob_mysql_connection_pool.h"     // ObMySQLConnectionPool
#include "lib/mysqlclient/ob_mysql_proxy.h"               // ObMySQLProxy

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

namespace liboblog
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
  int init(ServerProviderType *server_provider,
      const char *cluster_user,
      const char *cluster_password,
      const char *cluster_db_name,
      const int64_t sql_conn_timeout_us,
      const int64_t sql_query_timeout_us,
      const bool enable_ssl_client_authentication);
  void destroy();

  common::ObMySQLProxy &get_ob_mysql_proxy() { return mysql_proxy_; }

private:
  bool                  inited_;

  char                  cluster_user_[common::OB_MAX_USER_NAME_BUF_LENGTH];
  char                  cluster_password_[common::OB_MAX_PASSWORD_LENGTH + 1];
  char                  cluster_db_name_[common::OB_MAX_DATABASE_NAME_BUF_LENGTH];

  ConnectionPoolType    connection_pool_;
  // Thread-safe proxies, getting connections and locking
  common::ObMySQLProxy  mysql_proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogMysqlProxy);
};
} // namespace liboblog
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBLOG_MYSQL_PROXY_H__ */
