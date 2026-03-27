/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_PREPARE_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_PREPARE_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/session/ob_sql_session_info.h"


namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObPrepareStmt;

class ObPrepareExecutor
{
public:
  ObPrepareExecutor() {}
  virtual ~ObPrepareExecutor() {}
  int execute(ObExecContext &ctx, ObPrepareStmt &stmt);
private:
  int multiple_query_check(const ObSQLSessionInfo &session,
                           const common::ObString &sql,
                           common::ObIAllocator &allocator);
  DISALLOW_COPY_AND_ASSIGN(ObPrepareExecutor);
};

}
}

#endif /* OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_PREPARE_EXECUTOR_H_ */
