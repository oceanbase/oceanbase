/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_EXECUTOR_TASK_SPLITER_FACTORY_
#define OCEANBASE_SQL_EXECUTOR_TASK_SPLITER_FACTORY_

#include "sql/executor/ob_task_spliter.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace sql
{
class ObTaskSpliterFactory
{
public:
  ObTaskSpliterFactory();
  virtual ~ObTaskSpliterFactory();
  void reset();
  int create(ObExecContext &exec_ctx, ObJob &job, int spliter_type, ObTaskSpliter *&spliter);
private:
  common::ObSEArray<ObTaskSpliter*, 4> store_;
private:
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObTaskSpliterFactory);
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_TASK_SPLITER_FACTORY_ */
//// end of header file

