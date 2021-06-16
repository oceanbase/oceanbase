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

#ifndef OCEANBASE_EXECUTOR_OB_DETERMINATE_TASK_SPLITER_H_
#define OCEANBASE_EXECUTOR_OB_DETERMINATE_TASK_SPLITER_H_

#include "sql/executor/ob_task_spliter.h"
namespace oceanbase {
namespace sql {

class ObDeterminateTaskTransmit;

class ObDeterminateTaskSpliter : public ObTaskSpliter {
public:
  ObDeterminateTaskSpliter();
  virtual ~ObDeterminateTaskSpliter();
  virtual int get_next_task(ObTaskInfo*& task) override;
  virtual TaskSplitType get_type() const override
  {
    return ObTaskSpliter::DETERMINATE_TASK_SPLIT;
  }

private:
  struct SliceIDCompare;

private:
  int fetch_child_result(const ObTaskID& task_id, ObTaskInfo& task);
  int set_task_destination(const ObDeterminateTaskTransmit& transmit, const ObTaskID& task_id, ObTaskInfo& task);

  int record_task(const int64_t sys_job_id, const ObTaskID& task_id, const ObTaskInfo& task, const int64_t slice_count);
  int task_executed_servers(
      const int64_t sys_job_id, const ObTaskID& task_id, common::ObIArray<common::ObAddr>& servers);

private:
  int64_t task_idx_;
  common::ObAddr pre_addr_;

  common::ObArray<const ObSliceEvent*> child_slices_;
  bool child_slices_fetched_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_EXECUTOR_OB_DETERMINATE_TASK_SPLITER_H_
