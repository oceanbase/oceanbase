/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_PROFILE_CMD_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_PROFILE_CMD_EXECUTOR_
#include "lib/string/ob_string.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObCommonRpcProxy;
class ObUserProfileArg;
class ObDropUserArg;
}
namespace sql
{
class ObExecContext;
class ObUserProfileStmt;
class ObProfileDDLExecutor
{
public:
  ObProfileDDLExecutor() {}
  virtual ~ObProfileDDLExecutor() {}
  int execute(ObExecContext &ctx, ObUserProfileStmt &stmt);
private:
private:
  DISALLOW_COPY_AND_ASSIGN(ObProfileDDLExecutor);
};

}
}
#endif //OCEANBASE_SQL_ENGINE_CMD_PROFILE_CMD_EXECUTOR_
