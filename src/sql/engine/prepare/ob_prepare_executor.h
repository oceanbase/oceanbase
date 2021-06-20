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

#ifndef OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_PREPARE_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_PREPARE_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObPrepareStmt;

class ObPrepareExecutor {
public:
  ObPrepareExecutor()
  {}
  virtual ~ObPrepareExecutor()
  {}
  int execute(ObExecContext& ctx, ObPrepareStmt& stmt);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPrepareExecutor);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_PREPARE_EXECUTOR_H_ */
