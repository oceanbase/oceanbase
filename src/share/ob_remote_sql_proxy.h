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

#ifndef OCEANBASE_SHARE_OB_REMOTE_SQL_PROXY_H
#define OCEANBASE_SHARE_OB_REMOTE_SQL_PROXY_H
#include "lib/task/ob_timer.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/ob_remote_server_provider.h"

namespace oceanbase {
namespace share {
class ObRemoteSqlProxy : public ObISQLClient {
public:
  ObRemoteSqlProxy();
  ~ObRemoteSqlProxy();
  int init(ObRemoteServerProvider* server_provider);
  int set_login_info(const char* usr_info, const char* passwd);
  void destroy();
  virtual int escape(
      const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size) override;
  void signal_refresh()
  {
    connection_pool_.signal_refresh();
  }
  virtual int read(ReadResult& res, const uint64_t tenant_id, const char* sql) override;
  virtual int read(ReadResult& res, const int64_t cluster_id, const uint64_t tenant_id, const char* sql) override;
  virtual int write(const uint64_t tenant_id, const char* sql, int64_t& affected_rows) override;
  virtual sqlclient::ObISQLConnectionPool* get_pool() override;

  virtual bool is_oracle_mode() const override
  {
    return mysql_proxy_.is_oracle_mode();
  }

private:
  bool inited_;
  ObMySQLProxy mysql_proxy_;
  common::sqlclient::ObMySQLConnectionPool connection_pool_;
  ObRemoteServerProvider* server_provider_;
};
}  // namespace share
}  // namespace oceanbase
#endif
