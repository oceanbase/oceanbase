/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_CCL_RULE_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_CCL_RULE_EXECUTOR_H_
#include "share/ob_define.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreateCCLRuleStmt;
class ObDropCCLRuleStmt;

class ObCreateCCLRuleExecutor
{
public:
  ObCreateCCLRuleExecutor();
  virtual ~ObCreateCCLRuleExecutor();
  int execute(ObExecContext &ctx, ObCreateCCLRuleStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateCCLRuleExecutor);
};

class ObDropCCLRuleExecutor
{
public:
  ObDropCCLRuleExecutor();
  virtual ~ObDropCCLRuleExecutor();
  int execute(ObExecContext &ctx, ObDropCCLRuleStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropCCLRuleExecutor);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_CMD_OB_CCL_RULE_EXECUTOR_H_ */
