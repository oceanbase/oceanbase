// Copyright (c) 2021 OceanBase
// SPDX-License-Identifier: Apache-2.0

#define USING_LOG_PREFIX CLOG

#include "lib/thread/thread_mgr.h"
#include "ob_active_keep_alive.h"

namespace oceanbase
{
namespace logservice
{

ObActiveKeepAlive::ObActiveKeepAlive()
  : is_inited_(false),
    tg_id_(-1),
    active_keep_alive_worker_(),
    self_addr_()
{
}

ObActiveKeepAlive::~ObActiveKeepAlive()
{
  destroy_();
}

void ObActiveKeepAlive::destroy_()
{
  if (IS_INIT) {
    CLOG_LOG(INFO, "ObActiveKeepAlive destroy", KPC(this));
    is_inited_ = false;
    tg_id_ = -1;
    active_keep_alive_worker_.destroy();
    self_addr_.reset();
  }
}

int ObActiveKeepAlive::init(const int tg_id, const common::ObAddr &self_addr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObActiveKeepAlive init twice", KR(ret), KPC(this), K(tg_id), K(self_addr));
  } else if (0 > tg_id || !self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(tg_id), K(self_addr));
  } else if (OB_FAIL(active_keep_alive_worker_.init(self_addr))) {
    CLOG_LOG(WARN, "active keep alive worker init failed", KR(ret), K(self_addr), K_(active_keep_alive_worker));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, ACTIVE_KEEP_ALIVE_ROUND_INTERVAL, true))) {
    CLOG_LOG(WARN, "TG_SCHEDULE failed", KR(ret), K(tg_id));
  } else {
    tg_id_ = tg_id;
    self_addr_ = self_addr;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObActiveKeepAlive init success", KR(ret), KPC(this));
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    CLOG_LOG(WARN, "ObActiveKeepAlive init failed", KR(ret), KPC(this));
    active_keep_alive_worker_.destroy();
  }
  return ret;
}

void ObActiveKeepAlive::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObActiveKeepAlive not init", KR(ret), KPC(this));
  } else {
    active_keep_alive_worker_.do_work();
  }
}

} // namespace logservice
} // namespace oceanbase
