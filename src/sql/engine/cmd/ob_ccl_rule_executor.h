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
