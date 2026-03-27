/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_DEALLOCATE_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_DEALLOCATE_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"


namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObDeallocateStmt;

class ObDeallocateExecutor
{
public:
  ObDeallocateExecutor() {}
  virtual ~ObDeallocateExecutor() {}
  int execute(ObExecContext &ctx, ObDeallocateStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDeallocateExecutor);
};

}
}



#endif /* OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_DEALLOCATE_EXECUTOR_H_ */
