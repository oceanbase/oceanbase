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

#include "log_updater.h"
#include "lib/ob_errno.h"                     // ERRNO
#include "lib/time/ob_time_utility.h"         // ObTimeUtility
#include "share/ob_thread_define.h"           // TGDefIDs
#include "share/ob_thread_mgr.h"              // TG_CREATE
#include "palf_env_impl.h"                    // IPalfEnvImpl
namespace oceanbase
{
namespace palf
{
LogUpdater::LogUpdater() : palf_env_impl_(NULL), tg_id_(-1), is_inited_(false) {}

LogUpdater::~LogUpdater()
{
  destroy();
}

int LogUpdater::init(IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  int tg_id = lib::TGDefIDs::LogUpdater;
  if (NULL == palf_env_impl) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_id, tg_id_))) {
    PALF_LOG(ERROR, "LogUpdater create failed", K(ret));
  } else {
    palf_env_impl_ = palf_env_impl;
    is_inited_ = true;
    PALF_LOG(INFO, "LogUpdater init success", KPC(palf_env_impl), K(tg_id_), K(tg_id));
  }
  return ret;
}

int LogUpdater::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(TG_START(tg_id_))) {
    PALF_LOG(WARN, "LogUpdater TG_START failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, PALF_UPDATE_CACHED_STAT_INTERVAL_US, true))) {
    PALF_LOG(WARN, "LogUpdater TG_SCHEDULE failed", K(ret));
  } else {
    PALF_LOG(INFO, "LogUpdater start success", K(tg_id_), KPC(palf_env_impl_));
  }
  return ret;
}

void LogUpdater::stop()
{
  if (-1 != tg_id_) {
    PALF_LOG(INFO, "LogUpdater stop start", K(tg_id_), KPC(palf_env_impl_));
    TG_STOP(tg_id_);
    PALF_LOG(INFO, "LogUpdater stop finished", K(tg_id_), KPC(palf_env_impl_));
  }
}

void LogUpdater::wait()
{
  if (-1 != tg_id_) {
    PALF_LOG(INFO, "LogUpdater wait start", K(tg_id_), KPC(palf_env_impl_));
    TG_WAIT(tg_id_);
    PALF_LOG(INFO, "LogUpdater wait finished", K(tg_id_), KPC(palf_env_impl_));
  }
}

void LogUpdater::destroy()
{
  PALF_LOG(INFO, "LogUpdater destroy start", K(tg_id_), KPC(palf_env_impl_));
  is_inited_ = false;
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
  }
  tg_id_ = -1;
  palf_env_impl_ = NULL;
  PALF_LOG(INFO, "LogUpdater destroy finish", K(tg_id_), KPC(palf_env_impl_));
}

void LogUpdater::runTimerTask()
{
  int64_t start_time_us = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  if (NULL == palf_env_impl_) {
    PALF_LOG(ERROR, "palf_env_impl_ is NULL, unexpected error");
  } else {
    auto update_func = [](IPalfHandleImpl *ipalf_handle_impl) {
      return ipalf_handle_impl->update_palf_stat();
    };
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = palf_env_impl_->for_each(update_func))) {
      PALF_LOG(WARN, "for_each try_freeze_log_func failed", K(tmp_ret));
    }
    int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us;
    if (cost_time_us >= PALF_UPDATE_CACHED_STAT_INTERVAL_US) {
      PALF_LOG(WARN, "update_palf cost too much time", K(ret), K(cost_time_us), KPC(palf_env_impl_));
    }
  }
}
} // end namespace palf
} // end namespace oceanbase
