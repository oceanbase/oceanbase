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
      ObSQLSessionInfo *session,
      obrpc::ObCommonRpcProxy *common_rpc_proxy,
      const bool is_support_cancel = true);
  static int wait_ddl_retry_task_finish(
      const uint64_t tenant_id,
      const int64_t task_id,
      ObSQLSessionInfo &session,
      obrpc::ObCommonRpcProxy *common_rpc_proxy,
      int64_t &affected_rows);
  static int wait_build_index_finish(const uint64_t tenant_id, const int64_t task_id, bool &is_finish);
  static int handle_session_exception(ObSQLSessionInfo &session);
  static int cancel_ddl_task(const int64_t tenant_id, obrpc::ObCommonRpcProxy *common_rpc_proxy);
private:
  static inline bool is_server_stopped() { return observer::ObServer::get_instance().is_stopped(); }
private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLExecutorUtil);
};

} //end namespace sql
} //end namespace oceanbase


#endif //OCEANBASE_SQL_OB_DDL_EXECUTOR_UTIL_
