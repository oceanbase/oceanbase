/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_DCL_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_OB_DCL_EXECUTOR_

#include "share/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace obrpc
{
class ObCommonRpcProxy;
}

namespace sql
{
class ObExecContext;
class ObGrantStmt;
class ObGrantExecutor
{
public:
  ObGrantExecutor() {}
  virtual ~ObGrantExecutor() {}
  int execute(ObExecContext &ctx, ObGrantStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObGrantExecutor);
};

class ObRevokeStmt;
class ObRevokeExecutor
{
public:
  ObRevokeExecutor() {}
  virtual ~ObRevokeExecutor() {}
  int execute(ObExecContext &ctx, ObRevokeStmt &stmt);
private:
  int revoke_user(obrpc::ObCommonRpcProxy *rpc_proxy,
                  ObRevokeStmt &stmt);
  int revoke_catalog(obrpc::ObCommonRpcProxy *rpc_proxy,
                     ObRevokeStmt &stmt);
  int revoke_db(obrpc::ObCommonRpcProxy *rpc_proxy,
                ObRevokeStmt &stmt);
  int revoke_table(obrpc::ObCommonRpcProxy *rpc_proxy,
                   ObRevokeStmt &stmt,
                   ObExecContext &ctx);

  int revoke_routine(obrpc::ObCommonRpcProxy *rpc_proxy,
                     ObRevokeStmt &stmt,
                     ObExecContext &ctx);
  int revoke_sys_priv(obrpc::ObCommonRpcProxy *rpc_proxy,
                   ObRevokeStmt &stmt);
  int revoke_object(obrpc::ObCommonRpcProxy *rpc_proxy,
                    ObRevokeStmt &stmt,
                    ObExecContext &ctx);
  int revoke_sensitive_rule(obrpc::ObCommonRpcProxy *rpc_proxy,
                            ObRevokeStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObRevokeExecutor);
};
}
}
#endif //OCEANBASE_SQL_ENGINE_CMD_OB_DCL_EXECUTOR_
