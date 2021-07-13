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

#ifndef OCEANBASE_SQL_EXECUTOR_DISTRIBUTED_JOB_CONTROL_
#define OCEANBASE_SQL_EXECUTOR_DISTRIBUTED_JOB_CONTROL_

#include "sql/executor/ob_job_control.h"

namespace oceanbase {
namespace sql {
class ObDistributedJobControl : public ObJobControl {
public:
  ObDistributedJobControl();
  virtual ~ObDistributedJobControl();

  virtual int get_ready_jobs(common::ObIArray<ObJob*>& jobs, bool serial_sched = false) const;
  virtual int sort_job_scan_part_locs(ObExecContext& ctx) override;
  virtual int init_job_finish_queue(ObExecContext& ctx) override;
  int get_root_job(ObJob*& root_job) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistributedJobControl);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_DISTRIBUTED_JOB_CONTROL_ */
