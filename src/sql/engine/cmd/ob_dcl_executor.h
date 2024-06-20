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
  int revoke_db(obrpc::ObCommonRpcProxy *rpc_proxy,
                ObRevokeStmt &stmt);
  int revoke_table(obrpc::ObCommonRpcProxy *rpc_proxy,
                   ObRevokeStmt &stmt);

  int revoke_routine(obrpc::ObCommonRpcProxy *rpc_proxy, ObRevokeStmt &stmt);
  int revoke_sys_priv(obrpc::ObCommonRpcProxy *rpc_proxy,
                   ObRevokeStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObRevokeExecutor);
};
}
}
#endif //OCEANBASE_SQL_ENGINE_CMD_OB_DCL_EXECUTOR_
