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

#include "block_gc_timer_task.h"
#include "lib/ob_errno.h"                     // ERRNO
#include "lib/time/ob_time_utility.h"         // ObTimeUtility
#include "share/ob_thread_define.h"           // TGDefIDs
#include "share/ob_thread_mgr.h"              // TG_CREATE
#include "palf_env_impl.h"                    // PalfEnvImpl
namespace oceanbase
{
namespace palf
{
BlockGCTimerTask::BlockGCTimerTask() : palf_env_impl_(NULL), tg_id_(-1), is_inited_(false) {}

BlockGCTimerTask::~BlockGCTimerTask() { palf_env_impl_ = NULL; tg_id_ = -1; is_inited_ = false; }

int BlockGCTimerTask::init(PalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  int tg_id = lib::TGDefIDs::PalfBlockGC;
  if (NULL == palf_env_impl) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_id, tg_id_))) {
    PALF_LOG(ERROR, "BlockGCTimerTask create failed", K(ret));
  } else {
    palf_env_impl_ = palf_env_impl;
    PALF_LOG(INFO, "BlockGCTimerTask init success", KPC(palf_env_impl), K(tg_id_), K(tg_id));
    is_inited_ = true;
  }
  return ret;
}

int BlockGCTimerTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(TG_START(tg_id_))) {
    PALF_LOG(WARN, "BlockGCTimerTask TG_START failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, BLOCK_GC_TIMER_INTERVAL_MS, true))) {
    PALF_LOG(WARN, "BlockGCTimerTask TG_SCHEDULE failed", K(ret));
  } else {
    PALF_LOG(INFO, "BlockGCTimerTask start success", K(tg_id_), KPC(palf_env_impl_));
  }
  return ret;
}

void BlockGCTimerTask::stop()
{
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    PALF_LOG(INFO, "BlockGCTimerTask stop finished", K(tg_id_), KPC(palf_env_impl_));
  }
}

void BlockGCTimerTask::wait()
{
  if (-1 != tg_id_) {
    TG_WAIT(tg_id_);
    PALF_LOG(INFO, "BlockGCTimerTask wait finished", K(tg_id_), KPC(palf_env_impl_));
  }
}

void BlockGCTimerTask::destroy()
{
  PALF_LOG(INFO, "BlockGCTimerTask destroy finished", K(tg_id_), KPC(palf_env_impl_));
  is_inited_ = false;
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
  }
  tg_id_ = -1;
  palf_env_impl_ = NULL;
}

void BlockGCTimerTask::runTimerTask()
{
  int64_t start_time_us = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  if (NULL == palf_env_impl_) {
    PALF_LOG(ERROR, "palf_env_impl_ is NULL, unexpected error");
  } else if (OB_FAIL(palf_env_impl_->try_recycle_blocks())) {
    PALF_LOG(WARN, "PalfEnvImpl try_recycle_blocks failed");
  } else {
    int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us;
    if (cost_time_us >= 1 * 1000) {
      PALF_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "try_recycle_blocks cost too much time", K(ret), K(cost_time_us), KPC(palf_env_impl_));
    }
    if (palf_reach_time_interval(10 * 1000 * 1000, warn_time_)) {
      PALF_LOG(INFO, "BlockGCTimerTask success", K(ret), K(cost_time_us), KPC(palf_env_impl_));
    }
  }
}
} // end namespace palf
} // end namespace oceanbase
