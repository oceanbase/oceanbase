/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OB_MYSQL_LOCK_TABLE_EXECUTOR_H_
#define OCEANBASE_OB_MYSQL_LOCK_TABLE_EXECUTOR_H_

#include "storage/tablelock/ob_lock_executor.h"
#include "sql/session/ob_basic_session_info.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
class ObExecContext;
struct ObMySQLLockNode;
}

namespace transaction
{

namespace tablelock
{
class ObMySQLLockTableExecutor : public ObLockExecutor
{
public:
  int execute(sql::ObExecContext &ctx,
              const ObIArray<sql::ObMySQLLockNode> &lock_node_list);
private:
  int lock_tables_(sql::ObSQLSessionInfo *session,
                   const transaction::ObTxParam &tx_param,
                   const uint32_t client_session_id,
                   const uint64_t client_session_create_ts,
                   const ObIArray<sql::ObMySQLLockNode> &lock_node_list,
                   const int64_t timeout_us);
  int lock_table_(sql::ObSQLSessionInfo *session,
                  const transaction::ObTxParam &tx_param,
                  const uint32_t client_session_id,
                  const uint64_t client_session_create_ts,
                  const uint64_t table_id,
                  const int64_t lock_mode,
                  const int64_t timeout_us);
};

class ObMySQLUnlockTableExecutor : public ObUnLockExecutor
{
public:
  int execute(sql::ObExecContext &ctx);
};

} // tablelock
} // transaction
} // oceanbase
#endif
