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

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_INDEX_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_INDEX_EXECUTOR_H_
#include "lib/allocator/ob_allocator.h"
namespace oceanbase
{
namespace obrpc
{
struct ObCreateIndexArg;
struct ObDropIndexArg;
struct ObAlterTableRes;
class ObCommonRpcProxy;
}

namespace sql
{
class ObExecContext;

class ObCreateIndexStmt;
class ObSQLSessionInfo;

class ObCreateIndexExecutor
{
public:
  friend class ObAlterTableExecutor;
  ObCreateIndexExecutor();
  virtual ~ObCreateIndexExecutor();
  int execute(ObExecContext &ctx, ObCreateIndexStmt &stmt);
private:
  int set_drop_index_stmt_str(
      obrpc::ObDropIndexArg &drop_index_arg,
      common::ObIAllocator &allocator);
  int sync_check_index_status(sql::ObSQLSessionInfo &my_session,
        obrpc::ObCommonRpcProxy &common_rpc_proxy,
        const obrpc::ObCreateIndexArg &create_index_arg,
        const obrpc::ObAlterTableRes &res,
        common::ObIAllocator &allocator,
        bool is_update_global_indexes = false);
  int handle_session_exception(ObSQLSessionInfo &session);
  int handle_switchover();
};

class ObDropIndexStmt;
class ObDropIndexExecutor
{
public:
  ObDropIndexExecutor();
  virtual ~ObDropIndexExecutor();

  int execute(ObExecContext &ctx, ObDropIndexStmt &stmt);
  static int wait_drop_index_finish(
      const uint64_t tenant_id,
      const int64_t task_id,
      sql::ObSQLSessionInfo &session);
};

class ObFlashBackIndexStmt;
class ObFlashBackIndexExecutor {
public:
  ObFlashBackIndexExecutor() {}
  virtual ~ObFlashBackIndexExecutor() {}
  int execute(ObExecContext &ctx, ObFlashBackIndexStmt &stmt);
private:
};

class ObPurgeIndexStmt;
class ObPurgeIndexExecutor {
public:
  ObPurgeIndexExecutor() {}
  virtual ~ObPurgeIndexExecutor() {}
  int execute(ObExecContext &ctx, ObPurgeIndexStmt &stmt);
private:
};

}
}
#endif /* OCEANBASE_SRC_SQL_ENGINE_CMD_OB_INDEX_EXECUTOR_H_ */

