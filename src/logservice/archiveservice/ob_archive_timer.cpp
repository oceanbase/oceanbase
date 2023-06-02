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

#include "ob_archive_timer.h"
#include "lib/ob_define.h"
#include "lib/thread/ob_thread_name.h"        // lib::set_thread_name
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "share/config/ob_server_config.h"    // GCONF
#include "ob_archive_define.h"
#include "ob_archive_round_mgr.h"             // ObArchiveRoundMgr
#include "share/rc/ob_tenant_base.h"          // MTL
#include "ob_ls_meta_recorder.h"              // ObLSMetaRecorder

namespace oceanbase
{
namespace archive
{
using namespace oceanbase::share;

// =========================== ObArchiveTimer ========================= //
ObArchiveTimer::ObArchiveTimer() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  record_task_(this),
  recorder_(NULL),
  round_mgr_(NULL)
{}

ObArchiveTimer::~ObArchiveTimer()
{
  destroy();
}

int ObArchiveTimer::init(const uint64_t tenant_id,
    ObLSMetaRecorder *recorder,
    ObArchiveRoundMgr *round_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(ERROR, "ObArchiveTimer has been initialized", K(ret), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(recorder_ = recorder)
      || OB_ISNULL(round_mgr_ = round_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(recorder), K(round_mgr));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
    ARCHIVE_LOG(INFO, "ObArchiveTimer init succ");
  }
  return ret;
}

void ObArchiveTimer::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  recorder_ = NULL;
  round_mgr_ = NULL;
}

int ObArchiveTimer::start()
{
  int ret = OB_SUCCESS;
  ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(ERROR, "ObArchiveTimer not init", K(ret), K(inited_), K(tenant_id_));
  } else if (OB_FAIL(ObThreadPool::start())) {
    ARCHIVE_LOG(WARN, "ObArchiveTimer start fail", K(ret), K(tenant_id_));
  } else {
    ARCHIVE_LOG(INFO, "ObArchiveTimer start succ", K(tenant_id_));
  }
  return ret;
}

void ObArchiveTimer::stop()
{
  ARCHIVE_LOG(INFO, "ObArchiveTimer stop", K(tenant_id_));
  ObThreadPool::stop();
}

void ObArchiveTimer::wait()
{
  ARCHIVE_LOG(INFO, "ObArchiveTimer wait", K(tenant_id_));
  ObThreadPool::wait();
}

void ObArchiveTimer::run1()
{
  ARCHIVE_LOG(INFO, "ObArchiveTimer thread start", K(tenant_id_));
  lib::set_thread_name("ArcTimer");
  common::ObCurTraceId::init(GCONF.self_addr_);

  if (OB_UNLIKELY(! inited_)) {
    ARCHIVE_LOG_RET(ERROR, OB_NOT_INIT, "ObArchiveTimer not init", K(tenant_id_));
  } else {
    while (!has_set_stop()) {
      int64_t begin_tstamp = ObTimeUtility::current_time();
      do_thread_task_();
      int64_t end_tstamp = ObTimeUtility::current_time();
      int64_t wait_interval = THREAD_RUN_INTERVAL - (end_tstamp - begin_tstamp);
      if (wait_interval > 0) {
        ob_usleep(wait_interval);
      }
    }
  }
  ARCHIVE_LOG(INFO, "ObArchiveTimer thread end", K(tenant_id_));
}

void ObArchiveTimer::do_thread_task_()
{
  ArchiveKey key;
  share::ObArchiveRoundState state;
  round_mgr_->get_archive_round_info(key, state);
  if (! state.is_doing()) {
    // just skip
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      ARCHIVE_LOG(INFO, "archive round not in doing status, just skip", K(key), K(state));
    }
  } else {
    common::ObTimeGuard time_guard("do_thread_task", THREAD_RUN_INTERVAL);
    record_task_.handle();
    time_guard.click();
  }
}

// ============================== LSMetaRecordTask ============================ //
ObArchiveTimer::LSMetaRecordTask::LSMetaRecordTask(ObArchiveTimer *timer) :
  timer_(timer)
{}

ObArchiveTimer::LSMetaRecordTask::~LSMetaRecordTask()
{
  timer_ = NULL;
}

void ObArchiveTimer::LSMetaRecordTask::handle()
{
  if (NULL == timer_ || NULL == timer_->recorder_) {
    ARCHIVE_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid timer", K(timer_), K(timer_->recorder_));
  } else {
    timer_->recorder_->handle();
  }
}

} // namespace archive
} // namespace oceanbase
