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

#ifndef OCEANBASE_SQL_EXECUTOR_JOB_
#define OCEANBASE_SQL_EXECUTOR_JOB_

#include "lib/queue/ob_lighty_queue.h"
#include "sql/executor/ob_job_id.h"
#include "sql/executor/ob_task_id.h"
#include "sql/executor/ob_slice_id.h"
#include "sql/executor/ob_task_info.h"
#include "sql/executor/ob_task_executor.h"
#include "sql/executor/ob_task_control.h"
#include "sql/engine/ob_physical_plan.h"

namespace oceanbase {
namespace sql {
enum ObJobState {
  OB_JOB_STATE_NOT_INIT,
  OB_JOB_STATE_INITED,
  OB_JOB_STATE_RUNNING,
  OB_JOB_STATE_FINISHED,
};
class ObPhyOperator;
class ObTaskEvent;
class ObTaskSpliter;
class ObPhysicalPlanCtx;
class ObAddrsProvider;
class ObExecContext;
class ObOpSpec;
class ObMiniJob {
public:
  ObMiniJob() : phy_plan_(NULL), root_op_(NULL), extend_op_(NULL), root_spec_(NULL), extend_spec_(NULL)
  {}

  inline const ObPhysicalPlan* get_phy_plan() const
  {
    return phy_plan_;
  }
  inline void set_phy_plan(const ObPhysicalPlan* phy_plan)
  {
    phy_plan_ = phy_plan;
  }
  inline const ObPhyOperator* get_root_op() const
  {
    return root_op_;
  }
  inline void set_root_op(const ObPhyOperator* root_op)
  {
    root_op_ = root_op;
  }
  inline const ObPhyOperator* get_extend_op() const
  {
    return extend_op_;
  }
  inline void set_extend_op(const ObPhyOperator* extend_op)
  {
    extend_op_ = extend_op;
  }

  inline const ObOpSpec* get_root_spec() const
  {
    return root_spec_;
  }
  inline void set_root_spec(const ObOpSpec* root_spec)
  {
    root_spec_ = root_spec;
  }
  inline const ObOpSpec* get_extend_spec() const
  {
    return extend_spec_;
  }
  inline void set_extend_spec(const ObOpSpec* extend_op)
  {
    extend_spec_ = extend_op;
  }

  DECLARE_TO_STRING;

private:
  const ObPhysicalPlan* phy_plan_;
  const ObPhyOperator* root_op_;
  const ObPhyOperator* extend_op_;
  // for new engine
  const ObOpSpec* root_spec_;
  const ObOpSpec* extend_spec_;
};

class ObJob {
public:
  ObJob();
  virtual ~ObJob();
  // different task needs different spliter
  void set_task_spliter(ObTaskSpliter* spliter)
  {
    task_spliter_ = spliter;
  }
  int init_finish_queue(const ObExecContext& exec_ctx);
  // @deprecated
  int add_depending_job(const ObJob* job);

  int sort_scan_partition_locations(ObExecContext& ctx);
  int get_task_control(const ObExecContext& ctx, ObTaskControl*& task_control);
  int get_finished_task_locations(common::ObSArray<ObTaskLocation>& task_locs) const;

  ObJobState get_state()
  {
    return state_;
  }
  void set_state(ObJobState state)
  {
    state_ = state;
  }
  int update_job_state(ObExecContext& ctx, ObTaskEvent& evt, bool& job_finished);
  int signal_schedule_error(int64_t nop_event)
  {
    return task_control_.signal_schedule_error(nop_event);
  }

  int get_task_result(uint64_t task_id, ObTaskResult& task_result) const;
  common::ObLightyQueue& get_finish_queue()
  {
    return task_control_.get_finish_queue();
  }

  int append_to_last_failed_task_infos(common::ObIArray<ObTaskInfo*>& last_failed_task_infos) const;
  int print_status(char* buf, int64_t buf_len, int64_t& pos, bool ignore_normal_state = false) const;
  int find_child_job(uint64_t root_op_id, ObJob*& job) const;

  void reset();  // TODO 2014-11-18, need reset ds,

  inline void set_root_job()
  {
    is_root_job_ = true;
    task_control_.set_root_job();
  }
  inline bool is_root_job() const
  {
    return is_root_job_;
  }
  inline void set_phy_plan(ObPhysicalPlan* phy_plan)
  {
    phy_plan_ = phy_plan;
    if (phy_plan) {
      // statement without select will not do partition level retry
      task_control_.set_is_select_plan(phy_plan->is_select_plan());
    }
  }
  inline const ObPhysicalPlan* get_phy_plan() const
  {
    return phy_plan_;
  }
  inline void set_root_op(ObPhyOperator* op)
  {
    root_op_ = op;
  }
  inline ObPhyOperator* get_root_op()
  {
    return root_op_;
  }
  inline void set_ob_job_id(const ObJobID& ob_job_id)
  {
    ob_job_id_ = ob_job_id;
  }
  inline const ObJobID& get_ob_job_id()
  {
    return ob_job_id_;
  }
  inline uint64_t get_execution_id() const
  {
    return ob_job_id_.get_execution_id();
  }
  inline uint64_t get_job_id() const
  {
    return ob_job_id_.get_job_id();
  }
  inline uint64_t get_root_op_id() const
  {
    return ob_job_id_.get_root_op_id();
  }
  inline bool all_tasks_run() const
  {
    return task_control_.all_tasks_run();
  }

  inline void set_parent_job(ObJob* parent_job);
  inline int append_child_job(ObJob* job)
  {
    return child_jobs_.push_back(job);
  }
  inline int get_child_job(int64_t idx, ObJob*& job) const
  {
    return child_jobs_.at(idx, job);
  }
  inline int64_t get_child_count() const
  {
    return child_jobs_.count();
  }
  inline bool parent_can_exec()
  {
    return OB_JOB_STATE_FINISHED == state_;
  }
  int job_can_exec(bool& can_exec);
  int append_finished_slice_events(common::ObIArray<const ObSliceEvent*>& slice_events, bool skip_empty);
  int need_skip_empty_result(bool& skip_empty) const;
  int child_need_repart(bool& with_part, bool& with_subpart) const;
  inline void set_outer_join_child_job(bool outer_join_child_job)
  {
    is_outer_join_child_job_ = outer_join_child_job;
  }
  inline void set_outer_join_child_scan(bool outer_join_child_scan)
  {
    has_outer_join_child_scan_ = outer_join_child_scan;
  }
  inline void set_scan(bool has_scan)
  {
    has_scan_ = has_scan;
  }
  inline bool is_outer_join_child_job() const
  {
    return is_outer_join_child_job_;
  }
  inline bool has_outer_join_child_job() const;
  inline bool has_outer_join_child_scan() const
  {
    return has_outer_join_child_scan_;
  }
  inline bool has_child_job() const
  {
    return get_child_count() > 0;
  }
  inline bool has_scan_op() const
  {
    return has_scan_;
  }

  DECLARE_TO_STRING;

private:
  int prepare_task_control(const ObExecContext& exec_ctx);
  int get_parallel_degree(const ObExecContext& exec_ctx, int64_t& stmt_parallel_degree);
  int is_valid_finished_task_infos(const common::ObIArray<ObTaskInfo*>& task_infos, bool& is_valid) const;
  void print_plan_tree(char* buf, const int64_t buf_len, int64_t& pos, const ObPhyOperator* phy_op) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObJob);

private:
  static const int64_t MAX_CHILD_JOB_NUM = 16;
  ObJobID ob_job_id_;
  bool is_root_job_;
  ObPhysicalPlan* phy_plan_;
  ObPhyOperator* root_op_;
  ObJobState state_;
  // control task split and task's status
  ObTaskSpliter* task_spliter_;
  bool task_splited_;
  ObTaskControl task_control_;

  common::ObSEArray<ObJob*, 4> child_jobs_;
  bool is_outer_join_child_job_;
  bool has_outer_join_child_scan_;
  bool has_scan_;
};

inline bool ObJob::has_outer_join_child_job() const
{
  bool ret = false;
  for (int64_t i = 0; i < child_jobs_.count(); i++) {
    if (!OB_ISNULL(child_jobs_.at(i)) && child_jobs_.at(i)->is_outer_join_child_job()) {
      ret = true;
      break;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_JOB_ */
//// end of header file
