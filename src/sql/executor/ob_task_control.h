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

#ifndef OCEANBASE_SQL_EXECUTOR_TASK_CONTROL_
#define OCEANBASE_SQL_EXECUTOR_TASK_CONTROL_

#include "sql/executor/ob_task_info.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace sql
{
class ObTaskEvent;

class ObTaskResult
{
public:
  ObTaskResult() : slice_events_(NULL) {}
  virtual ~ObTaskResult() {}
  void reset() { task_location_.reset(); slice_events_ = NULL; }
  int init(const ObTaskLocation &task_location,
           const common::ObIArray<ObSliceEvent> &slice_events);
  bool is_valid() const { return task_location_.is_valid() && NULL != slice_events_; }
  const ObTaskLocation &get_task_location() const { return task_location_; }
  uint64_t get_task_id() const { return task_location_.get_task_id(); }
  bool has_empty_data() const;
  int64_t get_found_rows() const;
  int64_t get_affected_rows() const;
  int64_t get_matched_rows() const;
  int64_t get_duplicated_rows() const;

  TO_STRING_KV(K_(task_location), K_(slice_events));
protected:
  ObTaskLocation task_location_;
  const common::ObIArray<ObSliceEvent> *slice_events_;
};


// Base class of all kinds of job control
class ObTaskControl
{
public:
  ObTaskControl();
  virtual ~ObTaskControl();
  void reset();
  int add_task(ObTaskInfo *task) { return tasks_.push_back(task); }
  int64_t get_task_count() const { return tasks_.count(); }
  int find_task(uint64_t task_id, ObTaskInfo *&task) const;
  int get_task_location(uint64_t task_id, ObTaskLocation &task_location) const;
  int prepare(int64_t job_parallel_degree);
  int get_ready_tasks(common::ObIArray<ObTaskInfo *> &tasks) const;
  int get_running_tasks(common::ObIArray<ObTaskInfo *> &tasks) const;
  int get_finished_tasks(common::ObIArray<ObTaskInfo *> &tasks) const;
  int get_skipped_tasks(common::ObIArray<ObTaskInfo *> &tasks) const;
  int get_failed_tasks(common::ObIArray<ObTaskInfo *> &tasks) const;
  int get_begin_running_tasks(common::ObIArray<ObTaskInfo *> &tasks) const;
  int get_all_tasks(common::ObIArray<ObTaskInfo *> &tasks) const;
  void set_is_select_plan(bool is_select_plan) { is_select_plan_ = is_select_plan; }
  inline void set_root_job() { is_root_job_ = true; }
  inline bool is_root_job() const { return is_root_job_; }
  TO_STRING_KV("tasks", tasks_);
private:
  int get_task_by_state(common::ObIArray<ObTaskInfo *> &tasks, int state) const;
  // 数组下标转换为可以存进queue中的void*指针
  inline void *id_to_ptr(uint64_t id)
  {
    // 由于queue中push一个NULL指针会报错，而数组下标一般从0开始，因此这里要改为从1开始
    return reinterpret_cast<void*>(id + 1);
  }
  // queue中取出的void*指针转换为数组下标
  inline uint64_t ptr_to_id(void *ptr)
  {
    // 指针转回来之后从1开始，而数组下标从0开始，因此减去1
    return reinterpret_cast<uint64_t>(ptr) - 1;
  }
  DISALLOW_COPY_AND_ASSIGN(ObTaskControl);
private:
  common::ObSEArray<ObTaskInfo *, 2> tasks_;
  //  bool is_scan_job_;
  bool is_root_job_;
  // 用于底层scan_task的发送
  // 计划中一定不包含insert update delete等数据修改操作
  // 仅仅是select/select for update。这种情况下可以考虑做分区级重试
  //
  bool is_select_plan_;
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_TASK_CONTROL_ */
//// end of header file
