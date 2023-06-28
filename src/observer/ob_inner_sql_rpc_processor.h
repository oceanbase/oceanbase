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

#ifndef OBDEV_SRC_OBSERVER_INNER_SQL_RPC_PROCESSOR_H_
#define OBDEV_SRC_OBSERVER_INNER_SQL_RPC_PROCESSOR_H_

#include "share/ob_scanner.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_inner_sql_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase
{

namespace observer
{
class ObGlobalContext;
}

namespace obrpc
{

class ObInnerSqlRpcP : public obrpc::ObRpcProcessor< obrpc::ObInnerSQLRpcProxy::ObRpc<obrpc::OB_INNER_SQL_SYNC_TRANSMIT> >
{
public:
  ObInnerSqlRpcP(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObInnerSqlRpcP() {}

public:
  virtual int process();
private:
  int create_tmp_session(
      uint64_t tenant_id,
      sql::ObSQLSessionInfo *&tmp_session,
      sql::ObFreeSessionCtx &free_session_ctx,
      const bool is_oracle_mode);
  void cleanup_tmp_session(
      sql::ObSQLSessionInfo *&tmp_session,
      sql::ObFreeSessionCtx &free_session_ctx);

  int process_start_transaction(
      sqlclient::ObISQLConnection *conn,
      const ObSqlString &start_trans_sql,
      const ObInnerSQLTransmitArg &transmit_arg,
      ObInnerSQLTransmitResult &transmit_result);
  int process_register_mds(sqlclient::ObISQLConnection *con,
                           const ObInnerSQLTransmitArg &arg);
  int process_rollback(sqlclient::ObISQLConnection *conn);
  int process_commit(sqlclient::ObISQLConnection *conn);
  int process_write(
      sqlclient::ObISQLConnection *conn,
      const ObSqlString &write_sql,
      const ObInnerSQLTransmitArg &transmit_arg,
      ObInnerSQLTransmitResult &transmit_result);
  int process_read(
      sqlclient::ObISQLConnection *conn,
      const ObSqlString &read_sql,
      const ObInnerSQLTransmitArg &transmit_arg,
      ObInnerSQLTransmitResult &transmit_result);
  int set_session_param_to_conn(
      sqlclient::ObISQLConnection *conn,
      const ObInnerSQLTransmitArg &transmit_arg);
  const observer::ObGlobalContext &gctx_;
  DISALLOW_COPY_AND_ASSIGN(ObInnerSqlRpcP);
};

class ResourceGroupGuard
{
  //todo qilu:revert after ddl_back_threads are split under tenants
public:
  ResourceGroupGuard(const int32_t group_id);
  ~ResourceGroupGuard();
public:
  bool group_change_;
  int32_t old_group_id_;

};

}
}
#endif /* OBDEV_SRC_OBSERVER_INNER_SQL_RPC_PROCESSOR_H_ */
