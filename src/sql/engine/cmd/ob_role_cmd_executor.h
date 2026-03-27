/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_ROLE_CMD_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_ROLE_CMD_EXECUTOR_
#include "lib/string/ob_string.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObCommonRpcProxy;
struct ObCreateRoleArg;
class ObDropUserArg;
struct ObAlterRoleArg;
}
namespace sql
{
class ObExecContext;
class ObCreateRoleStmt;
class ObDropRoleStmt;
class ObAlterRoleStmt;
class ObCreateRoleExecutor
{
public:
  ObCreateRoleExecutor() {}
  virtual ~ObCreateRoleExecutor() {}
  int execute(ObExecContext &ctx, ObCreateRoleStmt &stmt);
private:
  int create_role(obrpc::ObCommonRpcProxy *rpc_proxy,
                  const obrpc::ObCreateRoleArg &arg) const;
  int drop_role(obrpc::ObCommonRpcProxy *rpc_proxy,
                  const obrpc::ObDropUserArg &arg) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateRoleExecutor);
};

class ObDropRoleExecutor
{
public:
  ObDropRoleExecutor() {}
  virtual ~ObDropRoleExecutor() {}
  int execute(ObExecContext &ctx, ObDropRoleStmt &stmt);
private:
  int drop_role(obrpc::ObCommonRpcProxy *rpc_proxy,
                  const obrpc::ObDropUserArg &arg) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropRoleExecutor);
};

class ObAlterRoleExecutor
{
public:
  ObAlterRoleExecutor() {}
  virtual ~ObAlterRoleExecutor() {}
  int execute(ObExecContext &ctx, ObAlterRoleStmt &stmt);
private:
  int alter_role(obrpc::ObCommonRpcProxy *rpc_proxy,
                 const obrpc::ObAlterRoleArg &arg) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterRoleExecutor);
};

}
}
#endif //OCEANBASE_SQL_ENGINE_CMD_USER_CMD_EXECUTOR_
