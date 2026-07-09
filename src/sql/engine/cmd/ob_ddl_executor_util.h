/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DDL_EXECUTOR_UTIL_
#define OCEANBASE_SQL_OB_DDL_EXECUTOR_UTIL_
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObPartition;
struct ObSubPartition;
struct ObBasePartition;
class ObMultiVersionSchemaService;
}
}
namespace obrpc
{
struct ObAlterTableArg;
}
namespace common
{
class ObIAllocator;
struct ObExprCtx;
class ObNewRow;
class ObMySQLProxy;
}
namespace sql
{
class ObExecContext;
class ObRawExpr;
class ObCreateTableStmt;
class ObTableStmt;

class ObDDLExecutorUtil final
{
public:
  ObDDLExecutorUtil() {}
  virtual ~ObDDLExecutorUtil() {}
  static int wait_ddl_finish(
      const uint64_t tenant_id,
      const int64_t task_id,
      const bool ddl_need_retry_at_executor,
      ObSQLSessionInfo *session,
      obrpc::ObCommonRpcProxy *common_rpc_proxy,
      const bool is_support_cancel = true,
      const int64_t wait_timeout_us = common::OB_MAX_USER_SPECIFIED_TIMEOUT);
  static int wait_ddl_retry_task_finish(
      const uint64_t tenant_id,
      const int64_t task_id,
      ObSQLSessionInfo &session,
      obrpc::ObCommonRpcProxy *common_rpc_proxy,
      int64_t &affected_rows);
  static int wait_build_index_finish(const uint64_t tenant_id, const int64_t task_id, bool &is_finish);
  static int handle_session_exception(ObSQLSessionInfo &session);
  static int cancel_ddl_task(const int64_t tenant_id, obrpc::ObCommonRpcProxy *common_rpc_proxy);
  template<class ARG, class RES>
  static int execute_pcreate_table(obrpc::ObCommonRpcProxy &common_rpc_proxy, ObSQLSessionInfo *my_session, const char* parallel_ddl_type,
                                  int (ObCommonRpcProxy::*rpc_func)(const ARG&, RES&, const ObRpcOpts&), const ARG &arg, RES &res,
                                  const uint64_t tenant_id);
private:
  static inline bool is_server_stopped() { return observer::ObServer::get_instance().is_stopped(); }
private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLExecutorUtil);
};

} //end namespace sql
} //end namespace oceanbase


#endif //OCEANBASE_SQL_OB_DDL_EXECUTOR_UTIL_
