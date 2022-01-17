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

#ifndef OCEANBASE_SQL_EXECUTOR_INTERM_TASK_SPLITER_
#define OCEANBASE_SQL_EXECUTOR_INTERM_TASK_SPLITER_

#include "sql/executor/ob_task_spliter.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase {
namespace sql {
class ObIntermTaskSpliter : public ObTaskSpliter {
public:
  ObIntermTaskSpliter();
  virtual ~ObIntermTaskSpliter();
  virtual int get_next_task(ObTaskInfo*& task);
  virtual TaskSplitType get_type() const
  {
    return ObTaskSpliter::INTERM_SPLIT;
  }

private:
  /* functions */
  int prepare();

private:
  /* variables */
  bool prepare_done_flag_;
  int64_t next_task_idx_;
  int64_t total_task_count_;
  common::ObSEArray<ObTaskInfo*, 8> store_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIntermTaskSpliter);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_INTERM_TASK_SPLITER_ */
//// end of header file
