/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_device_credential_task.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_thread_mgr.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;

ObDeviceCredentialTask::ObDeviceCredentialTask() : is_inited_(false), schedule_interval_us_(0)
{}

ObDeviceCredentialTask::~ObDeviceCredentialTask()
{}

int ObDeviceCredentialTask::init(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObDeviceCredentialTask has already been inited", K(ret));
  } else if (interval_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(interval_us));
  } else {
    schedule_interval_us_ = interval_us;
    if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, *this, schedule_interval_us_, true /*schedule repeatly*/))) {
      LOG_ERROR("fail to schedule task ObDeviceCredentialTask", K(ret), K(interval_us), KPC(this));
    } else {
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

void ObDeviceCredentialTask::reset()
{
  is_inited_ = false;
  schedule_interval_us_ = 0;
}

void ObDeviceCredentialTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = common::ObTimeUtility::fast_current_time();
  LOG_INFO("device credential task start", K(start_us));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("device credential task not init", K(ret));
  } else if (OB_FAIL(do_work_())) {
    LOG_WARN("fail to do work", K(ret));
  }
  const int64_t cost_us = common::ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("device credential task finish", K(cost_us));
}

int ObDeviceCredentialTask::do_work_()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(ObDeviceCredentialMgr::get_instance().refresh())) {
    OB_LOG(WARN, "failed to refresh device credentials", K(ret));
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase