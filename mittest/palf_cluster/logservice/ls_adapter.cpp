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
