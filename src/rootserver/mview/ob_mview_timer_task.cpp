/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RS

#include "rootserver/mview/ob_mview_timer_task.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;

int ObMViewTimerTask::schedule_task(const int64_t delay, bool repeate, bool immediate)
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer *);
  if (OB_ISNULL(timer)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("shared timer is NULL", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, delay, repeate, immediate))) {
    LOG_WARN("fail to schedule mview timer task", KR(ret), KP(this), K(delay), K(repeate),
             K(immediate));
  }
  return ret;
}

void ObMViewTimerTask::cancel_task()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer *);
  if (OB_ISNULL(timer)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("shared timer is NULL", KR(ret));
  } else {
    TG_CANCEL_TASK(timer->get_tg_id(), *this);
  }
}

void ObMViewTimerTask::wait_task()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer *);
  if (OB_ISNULL(timer)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("shared timer is NULL", KR(ret));
  } else {
    TG_WAIT_TASK(timer->get_tg_id(), *this);
  }
}

} // namespace rootserver
} // namespace oceanbase
