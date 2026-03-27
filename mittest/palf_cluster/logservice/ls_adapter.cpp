/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ls_adapter.h"
#include "logservice/replayservice/ob_replay_status.h"

namespace oceanbase
{
namespace palfcluster
{
MockLSAdapter::MockLSAdapter() :
    is_inited_(false)
  {}

MockLSAdapter::~MockLSAdapter()
{
  destroy();
}

int MockLSAdapter::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "MockLSAdapter init twice", K(ret));
  } else {
    is_inited_ = true;
    CLOG_LOG(INFO, "MockLSAdapter init success", K(ret));
  }
  return ret;
}

void MockLSAdapter::destroy()
{
  is_inited_ = false;
}

int MockLSAdapter::replay(logservice::ObLogReplayTask *replay_task)
{
  int ret = OB_SUCCESS;
  return ret;
}

int MockLSAdapter::wait_append_sync(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  return ret;
}

} // end namespace palfcluster
} // end namespace oceanbase
