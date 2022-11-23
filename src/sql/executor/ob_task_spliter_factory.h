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

