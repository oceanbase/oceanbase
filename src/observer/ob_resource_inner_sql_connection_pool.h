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

#ifndef OCEANBASE_OBSERVER_OB_RESOURCE_INNER_SQL_CONNECTION_POOL_H_
#define OCEANBASE_OBSERVER_OB_RESOURCE_INNER_SQL_CONNECTION_POOL_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_thread_cond.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "lib/mysqlclient/ob_isql_connection.h"

namespace oceanbase
{
namespace observer
{

class ObResourceInnerSQLConnectionPool
{
public:
  ObResourceInnerSQLConnectionPool();
  virtual ~ObResourceInnerSQLConnectionPool();
  ObInnerSQLConnectionPool &get_inner_sql_conn_pool() { return inner_sql_conn_pool_; };
  int init(share::schema::ObMultiVersionSchemaService *schema_service,
           sql::ObSql *ob_sql,
           ObVTIterCreator *vt_iter_creator_,
           common::ObServerConfig *config = NULL,
           const bool is_ddl = false);
  bool is_inited() { return is_inited_; }
  int acquire(const uint64_t conn_id, const bool is_oracle_mode, const bool kill_using_conn,
              common::sqlclient::ObISQLConnection *&conn, sql::ObSQLSessionInfo *session_info);
  int release(const bool reuse_conn, common::sqlclient::ObISQLConnection *&conn);

private:
  int fetch_max_conn_id(uint64_t &max_conn_id);
  int inner_release(common::sqlclient::ObISQLConnection *conn);
  typedef common::hash::ObHashMap<uint64_t /* key is conn id */,
                                  common::sqlclient::ObISQLConnection* /* value is inner sql connection */,
                                  common::hash::NoPthreadDefendMode> IdConnMap;
  bool is_inited_;
  uint64_t max_conn_id_;
  ObInnerSQLConnectionPool inner_sql_conn_pool_;
  common::ObLatch lock_;
  IdConnMap id_conn_map_;
};

} // end of namespace observer
} // end of namespace oceanbase
#endif // OCEANBASE_OBSERVER_OB_RESOURCE_INNER_SQL_CONNECTION_POOL_H_
