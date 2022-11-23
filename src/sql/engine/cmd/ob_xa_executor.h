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

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_XA_CMD_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_OB_XA_CMD_EXECUTOR_

#include "lib/utility/ob_macro_utils.h"
#include "lib/string/ob_string.h"
#include "sql/resolver/xa/ob_xa_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObXaStartStmt;
class ObXaStartExecutor
{
public:
  ObXaStartExecutor() {}
  ~ObXaStartExecutor() {}
  int execute(ObExecContext &ctx, ObXaStartStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObXaStartExecutor);
};

class ObPlXaStartExecutor
{
public:
  ObPlXaStartExecutor() {}
  ~ObPlXaStartExecutor() {}
  int execute(ObExecContext &ctx, ObXaStartStmt &stmt);
private:
  int get_org_cluster_id_(ObSQLSessionInfo *session, int64_t &org_cluster_id);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlXaStartExecutor);
};

class ObXaEndStmt;
class ObXaEndExecutor
{
public:
  ObXaEndExecutor() {}
  ~ObXaEndExecutor() {}
  int execute(ObExecContext &ctx, ObXaEndStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObXaEndExecutor);
};

class ObPlXaEndExecutor
{
public:
  ObPlXaEndExecutor() {}
  ~ObPlXaEndExecutor() {}
  int execute(ObExecContext &ctx, ObXaEndStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlXaEndExecutor);
};

class ObXaPrepareStmt;
class ObXaPrepareExecutor
{
public:
  ObXaPrepareExecutor() {}
  ~ObXaPrepareExecutor() {}
  int execute(ObExecContext &ctx, ObXaPrepareStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObXaPrepareExecutor);
};

class ObPlXaPrepareExecutor
{
public:
  ObPlXaPrepareExecutor() {}
  ~ObPlXaPrepareExecutor() {}
  int execute(ObExecContext &ctx, ObXaPrepareStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlXaPrepareExecutor);
};

class ObXaCommitStmt;
class ObXaRollBackStmt;
class ObXaEndTransExecutor
{
public:
  ObXaEndTransExecutor() {}
  ~ObXaEndTransExecutor() {}
  int execute(ObExecContext &ctx, ObXaCommitStmt &stmt)
  {
    return execute_(stmt.get_xa_string(), false, ctx);
  }
  int execute(ObExecContext &ctx, ObXaRollBackStmt &stmt)
  {
    return execute_(stmt.get_xa_string(), true, ctx);
  }
private:
  int execute_(const common::ObString& xid, const bool is_rollback,
      ObExecContext &ctx);
  DISALLOW_COPY_AND_ASSIGN(ObXaEndTransExecutor);
};

class ObPlXaEndTransExecutor
{
public:
  ObPlXaEndTransExecutor() {}
  ~ObPlXaEndTransExecutor() {}
  int execute(ObExecContext &ctx, ObXaCommitStmt &stmt);
  int execute(ObExecContext &ctx, ObXaRollBackStmt &stmt);
private:
  int execute_(const common::ObString &gtrid_str,
               const common::ObString &bqual_str,
               const int64_t format_id,
               const bool is_rollback,
               const int64_t flags,
               ObExecContext &ctx);
  DISALLOW_COPY_AND_ASSIGN(ObPlXaEndTransExecutor);
};

class ObPlXaCommitExecutor
{
public:
  ObPlXaCommitExecutor() {}
  ~ObPlXaCommitExecutor() {}
  int execute(ObExecContext &ctx, ObXaCommitStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlXaCommitExecutor);
};

class ObPlXaRollbackExecutor
{
public:
  ObPlXaRollbackExecutor() {}
  ~ObPlXaRollbackExecutor() {}
  int execute(ObExecContext &ctx, ObXaRollBackStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlXaRollbackExecutor);
};

} // end namespace sql
} // end namespace oceanbase


#endif
