/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_SENSITIVE_RULE_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_SENSITIVE_RULE_EXECUTOR_H_
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObSensitiveRuleStmt;

class ObSensitiveRuleExecutor
{
public:
  ObSensitiveRuleExecutor() {}
  virtual ~ObSensitiveRuleExecutor() {}
  int execute(ObExecContext &ctx, ObSensitiveRuleStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSensitiveRuleExecutor);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SRC_SQL_ENGINE_CMD_OB_SENSITIVE_RULE_EXECUTOR_H_