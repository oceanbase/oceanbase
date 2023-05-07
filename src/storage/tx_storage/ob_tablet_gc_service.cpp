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

#define USING_LOG_PREFIX STORAGE

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_thread_mgr.h"
#include "storage/tx_storage/ob_tablet_gc_service.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tx_storage/ob_ls_map.h"     // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "logservice/ob_log_service.h"
#include "logservice/palf/log_define.h"
#include "storage/tablet/ob_tablet.h" // ObTablet
#include "rootserver/ob_tenant_info_loader.h"
#include "share/ob_tenant_info_proxy.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
namespace checkpoint
{

// The time interval for waiting persist tablet successful after freezing is 5s
const int64_t ObTabletGCHandler::FLUSH_CHECK_INTERVAL = 5 * 1000 * 1000L;

// The time interval for waiting persist tablet successful after freezing in total is 72 * 5s = 6m
const int64_t ObTabletGCHandler::FLUSH_CHECK_MAX_TIMES = 72;

// The time interval for checking tablet_persist_trigger_ is 5s
const int64_t ObTabletGCService::GC_CHECK_INTERVAL = 5 * 1000 * 1000L;

// The time interval for checking deleted tablet trigger is 5s
const int64_t ObTabletGCService::GC_CHECK_DELETE_INTERVAL = 30 * 1000 * 1000L;

// The time interval for gc tablet and persist tablet whether the tablet_persist_trigger_ is 24 * 720 * 5s = 1d
const int64_t ObTabletGCService::GLOBAL_GC_CHECK_INTERVAL_TIMES = 24 * 720;

int ObTabletGCService::mtl_init(ObTabletGCService* &m)
{
  return m->init();
}

int ObTabletGCService::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTabletGCService init twice.", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTabletGCService::start()
{
  int ret = OB_SUCCESS;
  timer_for_tablet_change_.set_run_wrapper(MTL_CTX());
  timer_for_tablet_gc_.set_run_wrapper(MTL_CTX());
  if (OB_FAIL(timer_for_tablet_change_.init())) {
    STORAGE_LOG(ERROR, "fail to init timer", KR(ret));
  } else if (OB_FAIL(timer_for_tablet_gc_.init())) {
    STORAGE_LOG(ERROR, "fail to init timer", KR(ret));
  } else if (OB_FAIL(timer_for_tablet_change_.schedule(tablet_change_task_, GC_CHECK_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule task", KR(ret));
  } else if (OB_FAIL(timer_for_tablet_gc_.schedule(tablet_gc_task_, GC_CHECK_DELETE_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule task", KR(ret));
  }
  return ret;
}

int ObTabletGCService::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTabletGCService is not initialized", KR(ret));
  } else {
    STORAGE_LOG(INFO, "ObTabletGCService stoped");
  }
  timer_for_tablet_change_.stop();
  timer_for_tablet_gc_.stop();
  return ret;
}

void ObTabletGCService::wait()
{
  timer_for_tablet_change_.wait();
  timer_for_tablet_gc_.wait();
}

void ObTabletGCService::destroy()
{
  is_inited_ = false;
  timer_for_tablet_change_.destroy();
  timer_for_tablet_gc_.destroy();
}

void ObTabletGCService::ObTabletChangeTask::runTimerTask()
{
  STORAGE_LOG(INFO, "====== [tabletchange] timer task ======", K(GC_CHECK_INTERVAL));
  static int64_t times = 0;
  times = (times + 1) % GLOBAL_GC_CHECK_INTERVAL_TIMES;
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  bool skip_gc_task = false;

  skip_gc_task = (OB_SUCCESS != (OB_E(EventTable::EN_TABLET_GC_TASK_FAILED) OB_SUCCESS));

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "mtl ObLSService should not be null", KR(ret));
  } else if (OB_UNLIKELY(skip_gc_task)) {
    // do nothing
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    STORAGE_LOG(WARN, "get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is NULL", KR(ret));
  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      ObTabletGCHandler *tablet_gc_handler = NULL;
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "ls is NULL", KR(ret));
      } else if (FALSE_IT(tablet_gc_handler = ls->get_tablet_gc_handler())) {
      } else if (tablet_gc_handler->check_stop()) {
        STORAGE_LOG(INFO, "[tabletgc] tablet_gc_handler is offline", K(ls->get_ls_id()));
      } else {
        uint8_t tablet_persist_trigger = tablet_gc_handler->get_tablet_persist_trigger_and_reset();
        STORAGE_LOG(INFO, "[tabletchange] task check ls", K(ls->get_ls_id()), K(tablet_persist_trigger));
        if (times == 0
            || ObTabletGCHandler::is_set_tablet_persist_trigger(tablet_persist_trigger)
            || ObTabletGCHandler::is_tablet_gc_trigger(tablet_persist_trigger)) {
          obsys::ObRLockGuard lock(tablet_gc_handler->wait_lock_);
          bool need_retry = false;
          SCN checkpoint_scn;
          ObFreezer *freezer = ls->get_freezer();
          common::ObTabletIDArray unpersist_tablet_ids;
          const bool is_deleted = true;
          const bool only_deleted = true;
          bool is_gc = false;

          if (OB_ISNULL(freezer)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "freezer should not null", K(ls->get_ls_id()), KR(ret));
          }
          // 1. get minor merge point
          else if (OB_FAIL(freezer->decide_max_decided_scn(checkpoint_scn))) {
            need_retry = true;
            STORAGE_LOG(WARN, "decide_max_decided_scn failed", KR(ret), K(freezer->get_ls_id()));
          } else if (!checkpoint_scn.is_valid()
                     || SCN::min_scn() == checkpoint_scn
                     || checkpoint_scn < ls->get_tablet_change_checkpoint_scn()) {
            STORAGE_LOG(INFO, "no any log callback and no need to update clog checkpoint",
              K(freezer->get_ls_id()), K(checkpoint_scn), KPC(ls), K(ls->get_ls_meta()));
          }
          // 2. get gc tablet. tablet_change_ts cannot update when gc tablet failed.
          else if (OB_FAIL(tablet_gc_handler->gc_tablets(is_gc, need_retry))) {
            need_retry = true;
            STORAGE_LOG(WARN, "failed to gc tablet", KR(ret));
          }
          // 3. get unpersist_tablet_ids
          else if (OB_FAIL(tablet_gc_handler->get_unpersist_tablet_ids(unpersist_tablet_ids, need_retry, !only_deleted))) {
            need_retry = true;
            STORAGE_LOG(WARN, "failed to get_unpersist_tablet_ids", KPC(ls), KR(ret));
          }
          // 4. flush unpersit_tablet_ids
          else if (OB_FAIL(tablet_gc_handler->flush_unpersist_tablet_ids(unpersist_tablet_ids, checkpoint_scn))) {
            need_retry = true;
            STORAGE_LOG(WARN, "failed to flush_unpersist_tablet_ids", KPC(ls), KR(ret), K(unpersist_tablet_ids));
          }
          // 5. update tablet_change_checkpoint in log meta
          else if (checkpoint_scn > ls->get_tablet_change_checkpoint_scn()
                   && OB_FAIL(ls->set_tablet_change_checkpoint_scn(checkpoint_scn))) {
            need_retry = true;
            STORAGE_LOG(WARN, "failed to set_tablet_change_checkpoint_scn", KPC(ls), KR(ret), K(checkpoint_scn));
          }
          STORAGE_LOG(INFO, "[tabletchange] tablet in a ls persist and gc process end", KR(ret), KPC(ls), K(checkpoint_scn), K(unpersist_tablet_ids));
          if (need_retry) {
            STORAGE_LOG(INFO, "[tabletchange] persist or gc error, need try", KR(ret), KPC(ls), K(checkpoint_scn), K(tablet_persist_trigger));
            if (ObTabletGCHandler::is_set_tablet_persist_trigger(tablet_persist_trigger)) {
              tablet_gc_handler->set_tablet_persist_trigger();
            }
            if (ObTabletGCHandler::is_tablet_gc_trigger(tablet_persist_trigger)) {
              tablet_gc_handler->set_tablet_gc_trigger();
            }
          }
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {
        STORAGE_LOG(INFO, "[tabletchange] succeed to gc_tablet", KR(ret), K(ls_cnt), K(times));
      } else {
        STORAGE_LOG(INFO, "[tabletchange] no logstream", KR(ret), K(ls_cnt), K(times));
      }
    }
  }
}

void ObTabletGCService::ObTabletGCTask::runTimerTask()
{
  STORAGE_LOG(INFO, "====== [tabletgc] timer task ======", K(GC_CHECK_INTERVAL));
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  bool skip_gc_task = false;

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "mtl ObLSService should not be null", KR(ret));
  } else if (OB_UNLIKELY(skip_gc_task)) {
    // do nothing
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    STORAGE_LOG(WARN, "get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is NULL", KR(ret));
  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      ObTabletGCHandler *tablet_gc_handler = NULL;
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "ls is NULL", KR(ret));
      } else if (FALSE_IT(tablet_gc_handler = ls->get_tablet_gc_handler())) {
      } else if (tablet_gc_handler->check_stop()) {
        STORAGE_LOG(INFO, "[tabletgc] tablet_gc_handler is stop", K(ls->get_ls_id()));
      } else {
        bool is_tablet_gc = tablet_gc_handler->is_tablet_gc_trigger_and_reset();
        STORAGE_LOG(INFO, "[tabletgc] task check ls", K(ls->get_ls_id()), K(is_tablet_gc));
        if (is_tablet_gc) {
          obsys::ObRLockGuard lock(tablet_gc_handler->wait_lock_);
          bool need_retry = false;
          SCN checkpoint_scn;
          const bool is_deleted = true;
          const bool only_deleted = true;
          bool is_gc = false;

          if (OB_FAIL(tablet_gc_handler->gc_tablets(is_gc, need_retry))) {
            need_retry = true;
            STORAGE_LOG(WARN, "failed to gc tablet", KR(ret));
          } else if (is_gc) {
            tablet_gc_handler->set_tablet_persist_trigger();
          }
          STORAGE_LOG(INFO, "[tabletgc] tablet in a ls gc process end", KR(ret), KPC(ls), K(checkpoint_scn));
          if (need_retry) {
            STORAGE_LOG(INFO, "[tabletgc] persist or gc error, need try", KR(ret), KPC(ls), K(checkpoint_scn));
            if (is_tablet_gc) {
              tablet_gc_handler->set_tablet_gc_trigger();
            }
          }
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {
        STORAGE_LOG(INFO, "[tabletgc] succeed to gc_tablet", KR(ret), K(ls_cnt));
      } else {
        STORAGE_LOG(INFO, "[tabletgc] no logstream", KR(ret), K(ls_cnt));
      }
    }
  }
}

int ObTabletGCHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTabletGCHandler init twice", KR(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret));
  } else {
    ls_ = ls;
    is_inited_ = true;
  }
  return ret;
}

void ObTabletGCHandler::set_tablet_persist_trigger()
{
  uint8_t old_v = 0;
  uint8_t new_v = 0;
  do {
    old_v = ATOMIC_LOAD(&tablet_persist_trigger_);
    new_v = old_v | 1;
    if (old_v == new_v) {
      break;
    }
  } while (ATOMIC_CAS(&tablet_persist_trigger_, old_v, new_v) != old_v);
}

void ObTabletGCHandler::set_tablet_gc_trigger()
{
  uint8_t old_v = 0;
  uint8_t new_v = 0;
  do {
    old_v = ATOMIC_LOAD(&tablet_persist_trigger_);
    new_v = old_v | 2;
    if (old_v == new_v) {
      break;
    }
  } while (ATOMIC_CAS(&tablet_persist_trigger_, old_v, new_v) != old_v);
}

uint8_t ObTabletGCHandler::get_tablet_persist_trigger_and_reset()
{
  uint8_t old_v = 0;
  uint8_t new_v = 0;
  do {
    old_v = ATOMIC_LOAD(&tablet_persist_trigger_);
    if (old_v == new_v) {
      break;
    }
  } while (ATOMIC_CAS(&tablet_persist_trigger_, old_v, new_v) != old_v);
  return old_v;
}

int ObTabletGCHandler::check_tablet_gc_for_standby_(bool &cannot_gc, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  cannot_gc = false;
  SCN readable_scn;
  ObTablet *tablet = NULL;
  rootserver::ObTenantInfoLoader *info = MTL(rootserver::ObTenantInfoLoader*);
  ObTabletTxMultiSourceDataUnit tx_data;
  share::ObAllTenantInfo tenant_info;
  if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "mtl ObTenantRecoveryReportor should not be null", KR(ret));
  } else if (OB_FAIL(info->get_tenant_info(tenant_info))) {
    LOG_WARN("fail to get_tennat_info", KPC(info));
  } else if (tenant_info.is_primary()) {
  } else if (FALSE_IT(readable_scn = tenant_info.get_standby_scn())) {
  } else if (!readable_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "readable scn is invalid", KR(ret), KPC(this->ls_), K(tenant_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet is NULL", KR(ret), KPC(this->ls_), K(tablet_handle));
  } else if (OB_FAIL(tablet->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(this->ls_), KPC(tablet));
  } else if (tx_data.tx_scn_ >= readable_scn) {
    LOG_INFO("[tabletgc] tablet cannot gc for standby", K(ret), KPC(this->ls_), KPC(tablet), K(tx_data), K(readable_scn));
    cannot_gc = true;
  }
  return ret;
}

int ObTabletGCHandler::check_tablet_gc_(bool &cannot_gc, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  cannot_gc = false;
  if (OB_FAIL(check_tablet_gc_for_standby_(cannot_gc, tablet_handle))) {
    LOG_WARN("failed to check_tablet_gc_for_standby", K(ret), KPC(this->ls_));
  }
  return ret;
}

bool ObTabletGCHandler::is_tablet_gc_trigger_and_reset()
{
  uint8_t old_v = 0;
  uint8_t new_v = 0;
  do {
    old_v = ATOMIC_LOAD(&tablet_persist_trigger_);
    new_v = old_v & (~2);
    if (old_v == new_v) {
      break;
    }
  } while (ATOMIC_CAS(&tablet_persist_trigger_, old_v, new_v) != old_v);
  return 0 != (old_v & 2);
}

int ObTabletGCHandler::get_unpersist_tablet_ids(common::ObTabletIDArray &unpersist_tablet_ids,
                                                bool &need_retry,
                                                bool only_deleted /* = false */)
{
  int64_t ret = OB_SUCCESS;
  ObLSTabletIterator tablet_iter(ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US);
  ObTabletTxMultiSourceDataUnit tx_data;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handle is not inited", KR(ret));
  } else if (OB_FAIL(ls_->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    STORAGE_LOG(WARN, "failed to build ls tablet iter", KR(ret), KPC(this));
  } else {
    ObTabletHandle tablet_handle;
    while (OB_SUCC(ret)) {
      tx_data.reset();
      if (check_stop()) {
        ret = OB_EAGAIN;
        STORAGE_LOG(INFO, "tablet gc handler stop", KR(ret), KPC(this), K(tablet_handle), KPC(ls_), K(ls_->get_ls_meta()));
      } else if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "failed to get tablet", KR(ret), KPC(this), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid tablet handle", KR(ret), KPC(this), K(tablet_handle));
      } else if (tablet_handle.get_obj()->is_ls_inner_tablet()) {
        // skip ls inner tablet
      } else {
        const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
        const ObTabletID &tablet_id = tablet_meta.tablet_id_;
        SCN tmp_scn;
        if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
          LOG_WARN("failed to get tx data", K(ret), K(tablet_id));
        } else if (only_deleted) {
          bool cannot_gc = false;
          if (OB_FAIL(check_tablet_gc_(cannot_gc, tablet_handle))) {
            LOG_WARN("failed to check tablet gc", KR(ret), KPC(tablet_handle.get_obj()));
          } else if (cannot_gc) {
            need_retry = true;
          } else if (ObTabletStatus::DELETED == tx_data.tablet_status_) {
            STORAGE_LOG(INFO, "[tabletgc] get tx_data for gc", K(tx_data), K(tablet_meta));
            if (OB_FAIL(unpersist_tablet_ids.push_back(tablet_id))) {
              STORAGE_LOG(WARN, "failed to push_back deleted tablet", KR(ret));
            }
          }
        } else if (tx_data.tx_scn_ > tablet_meta.clog_checkpoint_scn_) {
          STORAGE_LOG(INFO, "[tabletgc] get tx_data for persist", K(tx_data), K(tablet_meta));
          if (OB_FAIL(unpersist_tablet_ids.push_back(tablet_id))) {
            STORAGE_LOG(WARN, "failed to push_back", KR(ret));
          }
        }
      }
    }
  }
  STORAGE_LOG(INFO, "[tabletgc] get unpersist_tablet_ids", KR(ret), K(unpersist_tablet_ids), K(only_deleted));
  return ret;
}

int ObTabletGCHandler::flush_unpersist_tablet_ids(const common::ObTabletIDArray &unpersist_tablet_ids,
                                                  const SCN checkpoint_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handle is not inited", KR(ret));
  } else if (OB_FAIL(freeze_unpersist_tablet_ids(unpersist_tablet_ids))) {
    STORAGE_LOG(WARN, "fail to freeze unpersist tablet", KR(ret), KPC(this->ls_), K(unpersist_tablet_ids));
  } else if (OB_FAIL(wait_unpersist_tablet_ids_flushed(unpersist_tablet_ids, checkpoint_scn))) {
    STORAGE_LOG(WARN, "fail to wait unpersist tablet ids flushed", KR(ret), KPC(this->ls_), K(unpersist_tablet_ids));
  }
  return ret;
}

int ObTabletGCHandler::freeze_unpersist_tablet_ids(const common::ObTabletIDArray &unpersist_tablet_ids)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handle is not inited", KR(ret));
  } else if (OB_FAIL(ls_->batch_tablet_freeze(unpersist_tablet_ids, true/*is_sync*/))){
    STORAGE_LOG(WARN, "fail to batch freeze tablet", KR(ret), KPC(this->ls_), K(unpersist_tablet_ids));
  }
  const int64_t end_ts = ObTimeUtility::fast_current_time();
  const int64_t cost = end_ts - start_ts;
  STORAGE_LOG(INFO, "[tabletgc] freeze unpersist_tablet_ids", KR(ret), K(unpersist_tablet_ids), K(unpersist_tablet_ids.count()), K(cost));
  return ret;
}

int ObTabletGCHandler::wait_unpersist_tablet_ids_flushed(const common::ObTabletIDArray &unpersist_tablet_ids,
                                                         const SCN checkpoint_scn)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  int64_t retry_times = FLUSH_CHECK_MAX_TIMES;
  int64_t i = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handle is not inited", KR(ret));
  } else if (check_stop()) {
    ret = OB_EAGAIN;
    STORAGE_LOG(INFO, "tablet gc handler stop", KR(ret), KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
  }
  // wait all tablet flushed
  while (unpersist_tablet_ids.count() > i && retry_times > 0 && OB_SUCC(ret)) {
    ob_usleep(FLUSH_CHECK_INTERVAL);
    while (unpersist_tablet_ids.count() > i && OB_SUCC(ret)) {
      ObTabletHandle handle;
      ObTablet *tablet = nullptr;
      SCN rec_log_scn = SCN::min_scn();
      if (check_stop()) {
        ret = OB_EAGAIN;
        STORAGE_LOG(INFO, "tablet gc handler stop", KR(ret), KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
      } else if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(unpersist_tablet_ids.at(i), handle, ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
        STORAGE_LOG(WARN, "fail to get tablet", KR(ret), K(i), KPC(this->ls_), K(unpersist_tablet_ids));
      } else if (OB_ISNULL(tablet = handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet is NULL", KR(ret), K(i), KPC(this->ls_), K(unpersist_tablet_ids));
      } else if (OB_FAIL(tablet->get_rec_log_scn(rec_log_scn))) {
        STORAGE_LOG(WARN, "fail to get rec log ts", KR(ret), K(handle));
      } else if (rec_log_scn > checkpoint_scn) {
        STORAGE_LOG(INFO, "[tabletgc] finish tablet flush", K(rec_log_scn), K(checkpoint_scn),
                    K(retry_times), K(i), K(ls_->get_ls_id()), KPC(tablet));
        i++;
      } else {
        STORAGE_LOG(INFO, "[tabletgc] wait tablet flush", K(rec_log_scn), K(checkpoint_scn),
                    K(retry_times), K(i), K(ls_->get_ls_id()), KPC(tablet));
        break;
      }
    }
    retry_times--;
  }

  if (OB_SUCC(ret) && i != unpersist_tablet_ids.count()) {
    ret = OB_TIMEOUT;
    STORAGE_LOG(WARN, "flush tablet timeout", KR(ret), K(retry_times), K(i), KPC(ls_), K(unpersist_tablet_ids));
  }
  const int64_t end_ts = ObTimeUtility::fast_current_time();
  const int64_t cost = end_ts - start_ts;
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "[tabletgc] flush tablet finish", KR(ret), KPC(ls_), K(unpersist_tablet_ids), K(unpersist_tablet_ids.count()), K(cost));
  }
  return ret;
}

int ObTabletGCHandler::gc_tablets(bool &is_gc, bool &need_retry)
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard lock(gc_lock_);
  const bool only_deleted = true;
  common::ObTabletIDArray deleted_tablet_ids;
  is_gc = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handler is not inited", KR(ret));
  } else if (check_stop()) {
    ret = OB_EAGAIN;
    STORAGE_LOG(INFO, "tablet gc handler stop", KR(ret), KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
  } else if (OB_FAIL(get_unpersist_tablet_ids(deleted_tablet_ids, need_retry, only_deleted))) {
    STORAGE_LOG(WARN, "failed to get_unpersist_tablet_ids", KPC(ls_), KR(ret));
  } else if (!deleted_tablet_ids.empty() && OB_FAIL(ls_->get_tablet_svr()->remove_tablets(deleted_tablet_ids))) {
    STORAGE_LOG(WARN, "failed to remove tablets", K(ret), K(deleted_tablet_ids));
  } else {
    is_gc = !deleted_tablet_ids.empty();
    STORAGE_LOG(INFO, "gc tablet finish", K(ret), K(deleted_tablet_ids));
  }
  return ret;
}

int ObTabletGCHandler::offline()
{
  int ret = OB_SUCCESS;
  set_stop();
  if (!is_finish()) {
    ret = OB_EAGAIN;
    STORAGE_LOG(INFO, "tablet gc handler not finish, retry", KR(ret), KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
  } else {
    STORAGE_LOG(INFO, "tablet gc handler offline", KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
  }
  return ret;
}

void ObTabletGCHandler::online()
{
  set_tablet_persist_trigger();
  set_start();
  STORAGE_LOG(INFO, "tablet gc handler online", KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
}

} // checkpoint
} // storage
} // oceanbase
