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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_JOB_CONTROL_
#define OCEANBASE_SQL_EXECUTOR_OB_JOB_CONTROL_

#include "lib/container/ob_se_array.h"
#include "sql/executor/ob_job.h"

namespace oceanbase
{
namespace sql
{
class ObTaskEvent;
class ObJobControl
{
public:
  explicit ObJobControl();
  virtual ~ObJobControl();

  void reset();
  virtual int sort_job_scan_part_locs(ObExecContext &ctx);
  virtual int init_job_finish_queue(ObExecContext &ctx);
//  int arrange_jobs();
  /*
   * @input ob_execution_id 用于在IRM中区分不同execution的结果，
   * 仅对Distributed模式有效，其余模式均为INVALID_ID
   */
  int create_job(common::ObIAllocator &alloc,
                 const ObExecutionID &ob_execution_id,
                 uint64_t root_op_id,
                 ObJob *&job) const;
  int find_job_by_job_id(uint64_t job_id, ObJob *&job) const;
  int find_job_by_root_op_id(uint64_t root_op_id, ObJob *&job) const;
  virtual int get_ready_jobs(common::ObIArray<ObJob *> &jobs, bool serial_sched = false) const = 0;
  virtual int get_running_jobs(common::ObIArray<ObJob *> &jobs) const;
  int get_all_jobs(common::ObIArray<ObJob *> &jobs) const;
  int get_all_jobs_except_root_job(common::ObIArray<ObJob *> &jobs) const;
  int all_jobs_finished(bool &is_finished) const;
  int all_jobs_finished_except_root_job(bool &is_finished) const;
  // build op input of the current job
  int print_status(char *buf, int64_t buf_len, bool ignore_normal_state = false) const;

  inline int add_job(ObJob *job) { return jobs_.push_back(job); }
  inline int64_t get_job_count() const { return jobs_.count(); }

  DECLARE_TO_STRING;
private:
  int build_job_ctx(ObExecContext &ctx, ObJob &job);
  int build_job_op_input(ObExecContext &ctx, ObJob &job);
//  int jobs_quick_sort(common::ObIArray<ObJob *> &jobs,
//                      int64_t low,
//                      int64_t high);
  void print_job_tree(char *buf, const int64_t buf_len, int64_t &pos, ObJob *job) const;
protected:
  common::ObSEArray<ObJob *, 2> jobs_;  // remote plan has two jobs
private:
  static volatile uint64_t global_job_id_;
  DISALLOW_COPY_AND_ASSIGN(ObJobControl);
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_JOB_CONTROL_ */
