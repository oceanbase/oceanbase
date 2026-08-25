/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_LAKE_TABLE_OPTIMIZER_UTILS_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_LAKE_TABLE_OPTIMIZER_UTILS_H

#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObLakeTableOptimizerUtils
{
public:
  static int get_enable_lake_table_parallel_resolving(const ObDMLStmt &stmt,
                                                      bool &enable);
};

} // namespace sql
} // namespace oceanbase
#endif
