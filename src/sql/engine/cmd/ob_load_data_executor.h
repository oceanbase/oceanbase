/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOAD_DATA_EXECUTOR_H_
#define OCEANBASE_LOAD_DATA_EXECUTOR_H_
#include "sql/resolver/cmd/ob_load_data_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObLoadDataStmt;
class ObLoadDataExecutor
{
public:
  ObLoadDataExecutor() {}
  virtual ~ObLoadDataExecutor() {}
  int execute(ObExecContext &ctx, ObLoadDataStmt &stmt);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLoadDataExecutor);
  // function members
private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_LOAD_DATA_EXECUTOR_H_ */
