/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_INTERVAL_PARTITION_UTILS_H
#define OCEANBASE_SQL_ENGINE_CMD_OB_INTERVAL_PARTITION_UTILS_H

#include <stdint.h>

#include "lib/utility/ob_macro_utils.h"
#include "sql/resolver/ob_stmt_type.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObTableSchema;
}
}
namespace sql
{

class ObExecContext;
class ObRawExpr;
class ObAlterTableStmt;

class ObIntervalPartitionUtils
{
public:
  static int check_transition_interval_valid(const stmt::StmtType stmt_type,
                                             ObExecContext &ctx,
                                             ObRawExpr *transition_expr,
                                             ObRawExpr *interval_expr);
  static int check_transition_interval_consistent(const share::schema::ObTableSchema &table_schema,
                                                  ObExecContext &ctx,
                                                  ObAlterTableStmt &stmt);

  static int set_interval_value(ObExecContext &ctx,
                                const stmt::StmtType stmt_type,
                                share::schema::ObTableSchema &table_schema,
                                ObRawExpr *interval_expr);
};

}
}

#endif /* OCEANBASE_SQL_ENGINE_CMD_OB_INTERVAL_PARTITION_UTILS_H */
