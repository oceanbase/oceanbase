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

#ifndef OCEANBASE_SINGLE_MYSQL_CONNECTION_POOL_H_
#define OCEANBASE_SINGLE_MYSQL_CONNECTION_POOL_H_
#include <mariadb/mysql.h>
#include "lib/container/ob_se_array.h"
#include "lib/list/ob_list.h"
#include "lib/allocator/ob_cached_allocator.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_server_provider.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
class ObSingleMySQLServerProvider : public ObMySQLServerProvider {
public:
  ObSingleMySQLServerProvider();
  void init(const ObAddr& server);
  virtual int get_cluster_list(common::ObIArray<int64_t>& cluster_list) override;
  virtual int get_server(const int64_t cluster_id, const int64_t svr_idx, common::ObAddr& server) override;
  virtual int64_t get_cluster_count() const override;
  virtual int64_t get_server_count() const override;
  virtual int64_t get_server_count(const int64_t cluster_id) const override;
  int refresh_server_list(void) override;
  int prepare_refresh() override;

private:
  ObAddr server_;
};

class ObSingleMySQLConnectionPool : public ObMySQLConnectionPool {
public:
  ObSingleMySQLConnectionPool();
  ~ObSingleMySQLConnectionPool();
  int init(const ObAddr& server, const ObConnPoolConfigParam& config);

private:
  ObSingleMySQLServerProvider provider_;
};
}  // namespace sqlclient
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_SINGLE_MYSQL_CONNECTION_POOL_H_
