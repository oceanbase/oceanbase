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

#ifndef OCEANBASE_SQL_EXECUTOR_MULTIINSERT_TASK_SPLITER_
#define OCEANBASE_SQL_EXECUTOR_MULTIINSERT_TASK_SPLITER_

#include "sql/executor/ob_multiscan_task_spliter.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "lib/hash/ob_iteratable_hashset.h"

namespace oceanbase {
namespace sql {

class ObMultiInsertTaskSpliter : public ObTaskSpliter {
public:
  ObMultiInsertTaskSpliter();
  virtual ~ObMultiInsertTaskSpliter();
  virtual int get_next_task(ObTaskInfo*& task);
  virtual TaskSplitType get_type() const
  {
    return ObTaskSpliter::INSERT_SPLIT;
  }

private:
  /* functions */
  int prepare();
  int get_next_range_location(ObTaskInfo::ObRangeLocation& range_loc);

private:
  /* variables */
  const ObPhyTableLocation* phy_table_loc_;
  bool prepare_done_flag_;
  common::ObSEArray<ObTaskInfo*, 2> store_;
  int64_t next_task_idx_;

private:
  /* other */
  DISALLOW_COPY_AND_ASSIGN(ObMultiInsertTaskSpliter);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_MULTIINSERT_TASK_SPLITER_ */
