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

#ifndef OCEANBASE_SQL_OB_BASELINE_EXECUTOR_H_
#define OCEANBASE_SQL_OB_BASELINE_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"
namespace oceanbase {
namespace common {
class ObString;
}
namespace share {
namespace schema {}
}  // namespace share
namespace sql {
class ObExecContext;
class ObAlterBaselineStmt;
class ObLogPlan;
class ObDMLStmt;
class ObOptimizerContext;

class ObAlterBaselineExecutor {
public:
  ObAlterBaselineExecutor()
  {}
  virtual ~ObAlterBaselineExecutor()
  {}
  int execute(ObExecContext& ctx, ObAlterBaselineStmt& stmt);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterBaselineExecutor);
};

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_BASELINE_EXECUTOR_H_
