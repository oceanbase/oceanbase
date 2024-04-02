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

#include "log_shared_task.h"
#include "palf_env_impl.h"                    // PalfEnvImpl
#include "share/ob_errno.h"                   // errno...

namespace oceanbase
{
namespace palf
{
LogSharedTask::LogSharedTask(const int64_t palf_id,const int64_t palf_epoch)
  : palf_id_(palf_id), palf_epoch_(palf_epoch)
{}

LogSharedTask::~LogSharedTask()
{
  destroy();
}

void LogSharedTask::destroy()
{
  reset();
}

void LogSharedTask::reset()
{
  palf_id_ = INVALID_PALF_ID;
  palf_epoch_ = -1;
}

LogHandleSubmitTask::LogHandleSubmitTask(const int64_t palf_id,const int64_t palf_epoch)
  : LogSharedTask(palf_id, palf_epoch)
{}

LogHandleSubmitTask::~LogHandleSubmitTask()
{}

void LogHandleSubmitTask::free_this(IPalfEnvImpl *palf_env_impl)
{
  palf_env_impl->get_log_allocator()->free_log_handle_submit_task(this);
}

int LogHandleSubmitTask::do_task(IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  int64_t palf_epoch = -1;
  IPalfHandleImplGuard guard;
  common::ObTimeGuard time_guard("handle submit task", 100 * 1000);
  if (OB_FAIL(palf_env_impl->get_palf_handle_impl(palf_id_, guard))) {
    PALF_LOG(WARN, "IPalfEnvImpl get_palf_handle_impl failed", K(ret), KPC(this));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->get_palf_epoch(palf_epoch))) {
    PALF_LOG(WARN, "IPalfEnvImpl get_palf_epoch failed", K(ret), KPC(this));
  } else if (palf_epoch != palf_epoch_) {
    PALF_LOG(WARN, "palf_epoch has changed, drop task", K(ret), K(palf_epoch), KPC(this));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->try_handle_next_submit_log())) {
    PALF_LOG(WARN, "PalfHandleImpl try_handle_next_submit_log failed", K(ret), KPC(this));
  } else {
    PALF_LOG(TRACE, "LogHandleSubmitTask handle_task success", K(time_guard), KPC(this));
  }
  return ret;
}

} // end namespace palf
} // end namespace oceanbase
