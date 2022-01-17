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

namespace oceanbase {
namespace sql {
class ObAddrsProvider;
class ObTaskEvent;

class ObTaskResult {
public:
  ObTaskResult() : slice_events_(NULL)
  {}
  virtual ~ObTaskResult()
  {}
  void reset()
  {
    task_location_.reset();
    slice_events_ = NULL;
  }
  int init(const ObTaskLocation& task_location, const common::ObIArray<ObSliceEvent>& slice_events);
  bool is_valid() const
  {
    return task_location_.is_valid() && NULL != slice_events_;
  }
  const ObTaskLocation& get_task_location() const
  {
    return task_location_;
  }
  const common::ObIArray<ObSliceEvent>* get_slice_events() const
  {
    return slice_events_;
  }
  uint64_t get_task_id() const
  {
    return task_location_.get_task_id();
  }
  bool has_empty_data() const;
  int64_t get_found_rows() const;
  int64_t get_affected_rows() const;
  int64_t get_matched_rows() const;
  int64_t get_duplicated_rows() const;

  TO_STRING_KV(K_(task_location), K_(slice_events));

protected:
  ObTaskLocation task_location_;
  const common::ObIArray<ObSliceEvent>* slice_events_;
};

// Base class of all kinds of job control
class ObTaskControl {
public:
  ObTaskControl();
  virtual ~ObTaskControl();
  int init_finish_queue(int64_t dop);
  void reset();
  int add_task(ObTaskInfo* task)
  {
    return tasks_.push_back(task);
  }
  int64_t get_task_count() const
  {
    return tasks_.count();
  }
  int find_task(uint64_t task_id, ObTaskInfo*& task) const;
  int get_task_result(uint64_t task_id, ObTaskResult& task_result) const;
  int get_task_location(uint64_t task_id, ObTaskLocation& task_location) const;
  int prepare(int64_t job_parallel_degree);
  int update_task_state(ObExecContext& ctx, ObTaskEvent& evt);
  int signal_schedule_error(int64_t nop_event)
  {
    return finish_queue_.push(id_to_ptr(nop_event));
  }
  bool all_tasks_finished_or_skipped_or_failed() const;
  bool all_tasks_run() const;
  int get_ready_tasks(common::ObIArray<ObTaskInfo*>& tasks) const;
  int get_running_tasks(common::ObIArray<ObTaskInfo*>& tasks) const;
  int get_finished_tasks(common::ObIArray<ObTaskInfo*>& tasks) const;
  int get_skipped_tasks(common::ObIArray<ObTaskInfo*>& tasks) const;
  int get_failed_tasks(common::ObIArray<ObTaskInfo*>& tasks) const;
  int get_begin_running_tasks(common::ObIArray<ObTaskInfo*>& tasks) const;
  int get_all_tasks(common::ObIArray<ObTaskInfo*>& tasks) const;
  int get_finished_slice_events(common::ObIArray<const ObSliceEvent*>& slice_events);
  int append_finished_slice_events(common::ObIArray<const ObSliceEvent*>& slice_events, bool skip_empty);
  //  int pop_task_loc(ObTaskLocation &task_loc, int64_t timeout);
  common::ObLightyQueue& get_finish_queue()
  {
    return finish_queue_;
  }
  int sort_finished_slice_events(bool skip_empty, bool repart_part, bool repart_subpart);
  void set_is_select_plan(bool is_select_plan)
  {
    is_select_plan_ = is_select_plan;
  }

  //  inline void set_scan_job() { is_scan_job_ = true; }
  //  inline bool is_scan_job() const { return is_scan_job_; }
  inline void set_root_job()
  {
    is_root_job_ = true;
  }
  inline bool is_root_job() const
  {
    return is_root_job_;
  }
  TO_STRING_KV("tasks", tasks_);

private:
  int get_task_by_state(common::ObIArray<ObTaskInfo*>& tasks, int state) const;
  inline void* id_to_ptr(uint64_t id)
  {
    return reinterpret_cast<void*>(id + 1);
  }
  inline uint64_t ptr_to_id(void* ptr)
  {
    return reinterpret_cast<uint64_t>(ptr) - 1;
  }
  int update_location_cache_and_retry_task(ObExecContext& exec_ctx, ObTaskInfo& task_info, ObTaskEvent& evt);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTaskControl);

private:
  common::ObSEArray<ObTaskInfo*, 4> tasks_;
  //  bool is_scan_job_;
  bool is_root_job_;
  common::ObLightyQueue finish_queue_;
  bool is_select_plan_;

  // private:
  //  struct ObSliceComparer
  //  {
  //  public:
  //    ObSliceComparer(bool cmp_part, bool cmp_subpart);
  //    virtual ~ObSliceComparer();
  //    bool operator()(const ObSliceEvent *slice1, const ObSliceEvent *slice2);
  //    int get_ret() const { return ret_; }
  //  private:
  //    bool cmp_part_;
  //    bool cmp_subpart_;
  //    int ret_;
  //  };
  //  common::ObSEArray<const ObSliceEvent *, 16> finished_slice_events_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_TASK_CONTROL_ */
//// end of header file
