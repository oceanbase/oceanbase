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

#include "sql/executor/ob_remote_job_control.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObRemoteJobControl::ObRemoteJobControl()
{
}

ObRemoteJobControl::~ObRemoteJobControl()
{
}

int ObRemoteJobControl::get_ready_jobs(ObIArray<ObJob*> &jobs, bool serial_sched) const
{
  int ret = OB_SUCCESS;
  UNUSED(serial_sched);
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob *job = jobs_.at(i);
    if (OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job is NULL", K(ret));
    } else if (OB_JOB_STATE_INITED == job->get_state()) {
      if (OB_FAIL(jobs.push_back(job))) {
        LOG_WARN("fail to push back job", K(ret), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (2 != jobs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the count of ready jobs is not 2", K(jobs.count()));
  }
  return ret;
}
}/* ns sql*/
}/* ns oceanbase */
