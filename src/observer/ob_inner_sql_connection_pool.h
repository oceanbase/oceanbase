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

#ifndef OCEANBASE_OBSERVER_OB_INNER_SQL_CONNECTION_POOL_H_
#define OCEANBASE_OBSERVER_OB_INNER_SQL_CONNECTION_POOL_H_

#include "lib/allocator/page_arena.h"
#include "lib/list/ob_dlist.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/lock/ob_thread_cond.h"
#include "ob_inner_sql_connection.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnection;
}
class ObServerConfig;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace sql
{
class ObSql;
}
namespace observer
{
class ObVTIterCreator;

class ObInnerSQLConnectionPool : public common::sqlclient::ObISQLConnectionPool
{
public:
  friend class ObInnerSQLConnection;
  friend class ObResourceInnerSQLConnectionPool;
  static constexpr int64_t RP_MAX_FREE_LIST_NUM = 1024;
  class LinkNode : public common::ObDLinkBase<LinkNode>
  {
  };
  static_assert(sizeof(LinkNode) <= sizeof(ObInnerSQLConnection),
      "inner sql connection size is too small");

  ObInnerSQLConnectionPool();
  virtual ~ObInnerSQLConnectionPool();

  int init(share::schema::ObMultiVersionSchemaService *schema_service,
           sql::ObSql *ob_sql,
           ObVTIterCreator *vt_iter_creator_,
           common::ObServerConfig *config = NULL,
           const bool is_ddl = false);

  virtual void stop() { stop_ = true; }
  // wait all connection been released
  virtual int wait();

  // sql string escape
  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size);

  // acquired connection must be released
  virtual int acquire(const uint64_t tenant_id, common::sqlclient::ObISQLConnection *&conn, ObISQLClient *client_addr, const int32_t group_id) override;
  virtual int release(common::sqlclient::ObISQLConnection *conn, const bool success);
  int acquire_spi_conn(sql::ObSQLSessionInfo *session_info, observer::ObInnerSQLConnection *&conn);
  int acquire(sql::ObSQLSessionInfo *session_info,
      common::sqlclient::ObISQLConnection *&conn,
      const bool is_oracle_mode = false);

  virtual int on_client_inactive(common::ObISQLClient *client_addr) override;
  virtual common::sqlclient::ObSQLConnPoolType get_type() override { return common::sqlclient::INNER_POOL; }
  virtual common::sqlclient::DblinkDriverProto get_pool_link_driver_proto() override { return common::sqlclient::DBLINK_DRV_OB; }
  // for dblink
  virtual int create_dblink_pool(const common::sqlclient::dblink_param_ctx &param_ctx, const ObAddr &server,
                                 const ObString &db_tenant, const ObString &db_user,
                                 const ObString &db_pass, const ObString &db_name,
                                 const common::ObString &conn_str,
                                 const common::ObString &cluster_str) override
  { UNUSEDx(param_ctx, server, db_tenant, db_user, db_pass, db_name, conn_str); return OB_SUCCESS; }
  virtual int acquire_dblink(const sqlclient::dblink_param_ctx &param_ctx, common::sqlclient::ObISQLConnection *&dblink_conn)
  { UNUSEDx(param_ctx, dblink_conn); return OB_SUCCESS; }
  virtual int release_dblink(common::sqlclient::ObISQLConnection *dblink_conn)
  { UNUSEDx(dblink_conn); return OB_SUCCESS; }
  virtual int do_acquire_dblink(const sqlclient::dblink_param_ctx &param_ctx, common::sqlclient::ObISQLConnection *&dblink_conn)
  { UNUSEDx(param_ctx, dblink_conn); return OB_SUCCESS; }
  virtual int try_connect_dblink(common::sqlclient::ObISQLConnection *dblink_conn, int64_t timeout_sec = 0) { UNUSEDx(dblink_conn, timeout_sec); return OB_SUCCESS; }
  virtual int clean_dblink_connection(uint64_t tenant_id)
  { UNUSED(tenant_id); return OB_SUCCESS; }
  void dump_used_conn_list();

  // Dozens of connections may acquired by one worker in oracle mode, because all sys tables
  // implemented by agent virtual table which need inner connection. Too many connections
  // warning may be triggered by parallel execution complicate sys table queries.
  //
  // 100000 = 50 connections * 2000 workers.
  const static int64_t WARNNING_CONNECTION_CNT = 100000;
  const static int64_t MAX_DUMP_SIZE = 20;

private:
  // alloc connection from %free_conn_list_
  int alloc_conn(ObInnerSQLConnection *&conn);
  // revert connection to %free_conn_list_
  int free_conn(ObInnerSQLConnection *conn);
  // revert connection, called by ObInnerSQLConnection::unref()
  int revert(ObInnerSQLConnection *conn);

  int add_to_used_conn_list(ObInnerSQLConnection *conn);
  int remove_from_used_conn_list(ObInnerSQLConnection *conn);

  bool inited_;
  volatile bool stop_;
  common::ObThreadCond cond_;
  int64_t total_conn_cnt_;
  common::ObDList<LinkNode> free_conn_list_;
  common::ObDList<ObInnerSQLConnection> used_conn_list_;
  common::ObArenaAllocator allocator_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  sql::ObSql *ob_sql_;
  ObVTIterCreator *vt_iter_creator_;
  common::ObServerConfig *config_;
  bool is_ddl_;

  DISALLOW_COPY_AND_ASSIGN(ObInnerSQLConnectionPool);
};

} // end namespace observer
} // end namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_INNER_SQL_CONNECTION_POOL_H_
