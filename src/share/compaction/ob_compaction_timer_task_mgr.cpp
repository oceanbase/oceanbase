//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "share/compaction/ob_compaction_timer_task_mgr.h"
#include "deps/oblib/src/lib/thread/thread_mgr.h"
namespace oceanbase
{
namespace compaction
{
int ObCompactionTimerTask::restart_schedule_timer_task(
  const int64_t schedule_interval,
  const int64_t tg_id,
  common::ObTimerTask &timer_task)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_FAIL(TG_TASK_EXIST(tg_id, timer_task, is_exist))) {
    LOG_ERROR("failed to check merge schedule task exist", K(ret));
  } else if (is_exist && OB_FAIL(TG_CANCEL_R(tg_id, timer_task))) {
    LOG_WARN("failed to cancel task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, timer_task, schedule_interval, true/*repeat*/))) {
    LOG_WARN("Fail to schedule timer task", K(ret));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
