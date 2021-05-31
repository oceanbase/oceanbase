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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_distributed_job_control.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObDistributedJobControl::ObDistributedJobControl()
{}

ObDistributedJobControl::~ObDistributedJobControl()
{}

int ObDistributedJobControl::get_ready_jobs(ObIArray<ObJob*>& jobs, bool serial_sched) const
{
  int ret = OB_SUCCESS;
  UNUSED(serial_sched);
  bool all_finish = true;
  jobs.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob* job = jobs_.at(i);
    bool can_exec = true;
    if (OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_EXE_LOG(WARN, "job is NULL", K(ret), K(i), K(jobs_.count()));
    } else if (OB_JOB_STATE_FINISHED != job->get_state()) {
      all_finish = false;
      if (OB_FAIL(job->job_can_exec(can_exec))) {
        LOG_WARN("fail to get job can exec", K(ret));
      } else if (!can_exec) {
        // nothing.
      } else if (OB_FAIL(OB_I(t1) jobs.push_back(job))) {
        LOG_WARN("fail to push job into array", K(ret));
      } else if (serial_sched) {
        break;
      }
    }
  }
  if (all_finish && OB_SUCCESS == ret) {
    ret = OB_ITER_END;
  }
  return ret;
}
int ObDistributedJobControl::sort_job_scan_part_locs(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob* job = jobs_.at(i);
    if (OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("job is NULL", K(ret), K(i));
    } else if (OB_FAIL(job->sort_scan_partition_locations(ctx))) {
      LOG_WARN("fail to sort scan partition locations", K(ret), K(i), K(*job));
    }
  }
  return ret;
}

int ObDistributedJobControl::init_job_finish_queue(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob* job = jobs_.at(i);
    if (OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("job is NULL", K(ret), K(i));
    } else if (OB_FAIL(job->init_finish_queue(ctx))) {
      LOG_WARN("fail to sort scan partition locations", K(ret), K(i), K(*job));
    }
  }
  return ret;
}

int ObDistributedJobControl::get_root_job(ObJob*& root_job) const
{
  int ret = OB_SUCCESS;
  root_job = jobs_.at(0);
  if (OB_ISNULL(root_job) || OB_UNLIKELY(!root_job->is_root_job())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_EXE_LOG(WARN, "root job is NULL or invalid", K(ret), K(jobs_.count()));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
