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
#include "share/ob_thread_mgr.h"
#include "share/ob_errno.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "storage/tx_storage/ob_checkpoint_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_map.h"     // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "logservice/ob_log_service.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/lsn.h"
#include "logservice/archiveservice/ob_archive_service.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
using namespace share;
using namespace palf;
namespace storage
{
namespace checkpoint
{

int64_t ObCheckPointService::CHECK_CLOG_USAGE_INTERVAL = 2000 * 1000L;
int64_t ObCheckPointService::CHECKPOINT_INTERVAL = 5000 * 1000L;
int64_t ObCheckPointService::TRAVERSAL_FLUSH_INTERVAL = 5000 * 1000L;

int ObCheckPointService::mtl_init(ObCheckPointService* &m)
{
  return m->init(MTL_ID());
}

int ObCheckPointService::init(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCheckPointService init twice.", K(ret));
  } else if (OB_FAIL(freeze_thread_.init(tenant_id, lib::TGDefIDs::LSFreeze))) {
    LOG_WARN("fail to initialize freeze thread", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObCheckPointService::start()
{
  int ret = OB_SUCCESS;
  checkpoint_timer_.set_run_wrapper(MTL_CTX());
  traversal_flush_timer_.set_run_wrapper(MTL_CTX());
  check_clog_disk_usage_timer_.set_run_wrapper(MTL_CTX());

  if (OB_FAIL(checkpoint_timer_.init("TxCkpt", ObMemAttr(MTL_ID(), "CheckPointTimer")))) {
    STORAGE_LOG(ERROR, "fail to init checkpoint_timer", K(ret));
  } else if (OB_FAIL(checkpoint_timer_.schedule(checkpoint_task_, CHECKPOINT_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule checkpoint task", K(ret));
  } else if (OB_FAIL(traversal_flush_timer_.init("Flush", ObMemAttr(MTL_ID(), "FlushTimer")))) {
    STORAGE_LOG(ERROR, "fail to init traversal_timer", K(ret));
  } else if (OB_FAIL(traversal_flush_timer_.schedule(traversal_flush_task_, TRAVERSAL_FLUSH_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule traversal_flush task", K(ret));
  } else if (OB_FAIL(check_clog_disk_usage_timer_.init("CKClogDisk", ObMemAttr(MTL_ID(), "DiskUsageTimer")))) {
    STORAGE_LOG(ERROR, "fail to init check_clog_disk_usage_timer", K(ret));
  } else if (OB_FAIL(check_clog_disk_usage_timer_.schedule(check_clog_disk_usage_task_, CHECK_CLOG_USAGE_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule check_clog_disk_usage task", K(ret));
  }
  return ret;
}

int ObCheckPointService::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckPointService is not initialized", K(ret));
  } else {
    TG_STOP(freeze_thread_.get_tg_id());
    LOG_INFO("ObCheckPointService stoped");
  }
  checkpoint_timer_.stop();
  traversal_flush_timer_.stop();
  check_clog_disk_usage_timer_.stop();
  return ret;
}

void ObCheckPointService::wait()
{
  checkpoint_timer_.wait();
  traversal_flush_timer_.wait();
  check_clog_disk_usage_timer_.wait();
  TG_WAIT(freeze_thread_.get_tg_id());
}

int ObCheckPointService::add_ls_freeze_task(
    ObDataCheckpoint *data_checkpoint,
    SCN rec_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(freeze_thread_.add_task(data_checkpoint, rec_scn))) {
    STORAGE_LOG(WARN, "logstream freeze task failed", K(ret));
  }
  return ret;
}

void ObCheckPointService::destroy()
{
  TG_DESTROY(freeze_thread_.get_tg_id());
  is_inited_ = false;
  checkpoint_timer_.destroy();
  traversal_flush_timer_.destroy();
  check_clog_disk_usage_timer_.destroy();
}

void ObCheckPointService::ObCheckpointTask::runTimerTask()
{
  STORAGE_LOG(INFO, "====== checkpoint timer task ======");
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "mtl ObLSService should not be null", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    STORAGE_LOG(WARN, "get log stream iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is NULL", K(ret));
  } else {
    DEBUG_SYNC(BEFORE_CHECKPOINT_TASK);
    ObLS *ls = nullptr;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      ObLSHandle ls_handle;
      ObCheckpointExecutor *checkpoint_executor = nullptr;
      ObDataCheckpoint *data_checkpoint = nullptr;
      palf::LSN checkpoint_lsn;
      palf::LSN archive_lsn;
      SCN unused_archive_scn;
      bool archive_force_wait = false;
      bool archive_ignore = false;
      if (OB_FAIL(ls_svr->get_ls(ls->get_ls_id(), ls_handle, ObLSGetMod::APPLY_MOD))) {
        STORAGE_LOG(WARN, "get log stream failed", K(ret), K(ls->get_ls_id()));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "log stream not exist", K(ret), K(ls->get_ls_id()));
      } else if (OB_ISNULL(data_checkpoint = ls->get_data_checkpoint())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "data_checkpoint should not be null", K(ret), K(ls->get_ls_id()));
      } else if (OB_FAIL(data_checkpoint->check_can_move_to_active_in_newcreate())) {
        STORAGE_LOG(WARN, "check can move to active failed", K(ret), K(ls->get_ls_id()));
      } else if (OB_ISNULL(checkpoint_executor = ls->get_checkpoint_executor())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "checkpoint_executor should not be null", K(ls->get_ls_id()));
      } else if (OB_FAIL(checkpoint_executor->update_clog_checkpoint())) {
        STORAGE_LOG(WARN, "update_clog_checkpoint failed", K(ret), K(ls->get_ls_id()));
      } else if (OB_FAIL(MTL(archive::ObArchiveService*)->get_ls_archive_progress(ls->get_ls_id(),
              archive_lsn, unused_archive_scn, archive_force_wait, archive_ignore))) {
        STORAGE_LOG(WARN, "get ls archive progress failed", K(ret), K(ls->get_ls_id()));
      } else {
        checkpoint_lsn = ls->get_clog_base_lsn();
        if (! archive_force_wait || archive_ignore || archive_lsn >= checkpoint_lsn) {
          // do nothing
        } else {
          STORAGE_LOG(TRACE, "archive_lsn small than checkpoint_lsn, set base_lsn with archive_lsn",
              K(archive_lsn), K(checkpoint_lsn), KPC(ls));
          checkpoint_lsn = archive_lsn;
        }
        if (OB_FAIL(ls->get_log_handler()->advance_base_lsn(checkpoint_lsn))) {
          ARCHIVE_LOG(WARN, "advance base lsn failed", K(ret), K(checkpoint_lsn));
        } else {
          FLOG_INFO("[CHECKPOINT] advance palf base lsn successfully",
              K(checkpoint_lsn), K(ls->get_ls_id()));
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {
        STORAGE_LOG(INFO, "succeed to update_clog_checkpoint", K(ret), K(ls_cnt));
      } else {
        STORAGE_LOG(INFO, "no logstream", K(ret), K(ls_cnt));
      }
    }
  }
}

bool ObCheckPointService::get_disk_usage_threshold_(int64_t &threshold)
{
  int ret = OB_SUCCESS;
  bool get_disk_usage_threshold_success = false;
  // avod clog disk full
  logservice::ObLogService *log_service = nullptr;
  if (OB_ISNULL(log_service = MTL(logservice::ObLogService *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get_log_service failed", K(ret));
  } else {
    int64_t used_size = 0;
    int64_t total_size = 0;
    if (OB_FAIL(log_service->get_palf_disk_usage(used_size, total_size))) {
      STORAGE_LOG(WARN, "get_disk_usage failed", K(ret), K(used_size), K(total_size));
    } else {
      threshold = total_size * NEED_FLUSH_CLOG_DISK_PERCENT / 100;
      get_disk_usage_threshold_success = true;
    }
  }

  return get_disk_usage_threshold_success;
}

bool ObCheckPointService::cannot_recycle_log_over_threshold_(const int64_t threshold, const bool need_update_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  bool cannot_recycle_log_over_threshold = false;
  int64_t cannot_recycle_log_size = 0;
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "mtl ObLSService should not be null", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    STORAGE_LOG(WARN, "get log stream iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is NULL", K(ret));
  } else {
    ObLS *ls = nullptr;
    int ls_cnt = 0;
    for (; OB_SUCC(ret) && OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      ObLSHandle ls_handle;
      ObCheckpointExecutor *checkpoint_executor = nullptr;
      if (OB_FAIL(ls_svr->get_ls(ls->get_ls_id(), ls_handle, ObLSGetMod::APPLY_MOD))) {
        STORAGE_LOG(WARN, "get log stream failed", K(ret), K(ls->get_ls_id()));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "log stream not exist", K(ret), K(ls->get_ls_id()));
      } else if (OB_ISNULL(checkpoint_executor = ls->get_checkpoint_executor())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "checkpoint_executor should not be null", K(ls->get_ls_id()));
      } else if (need_update_checkpoint_scn && OB_FAIL(checkpoint_executor->update_clog_checkpoint())) {
        STORAGE_LOG(WARN, "update_clog_checkpoint failed", K(ret), K(ls->get_ls_id()));
      } else {
        cannot_recycle_log_size += checkpoint_executor->get_cannot_recycle_log_size();
      }
    }
    if (ret == OB_ITER_END) {
      if (cannot_recycle_log_size > threshold) {
        cannot_recycle_log_over_threshold = true;
      }
      STORAGE_LOG(INFO, "cannot_recycle_log_size statistics",
                  K(cannot_recycle_log_size), K(threshold), K(need_update_checkpoint_scn));
    }
  }

  return cannot_recycle_log_over_threshold;
}

int ObCheckPointService::flush_to_recycle_clog_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "mtl ObLSService should not be null", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    STORAGE_LOG(WARN, "get log stream iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is NULL", K(ret));
  } else {
    ObLS *ls = nullptr;
    int64_t ls_cnt = 0;
    int64_t succ_ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      if (OB_TMP_FAIL(ls->flush_to_recycle_clog())) {
        STORAGE_LOG(WARN, "flush ls to recycle clog failed", KR(tmp_ret), KPC(ls));
        tmp_ret = OB_SUCCESS;
      } else {
        ++succ_ls_cnt;
      }
    }
    STORAGE_LOG(DEBUG, "finish flush if need", KR(ret), K(ls_cnt), K(succ_ls_cnt));

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

void ObCheckPointService::ObTraversalFlushTask::runTimerTask()
{
  STORAGE_LOG(INFO, "====== traversal_flush timer task ======");
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "mtl ObLSService should not be null", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    STORAGE_LOG(WARN, "get log stream iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is NULL", K(ret));
  } else {
    ObLS *ls = nullptr;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      ObLSHandle ls_handle;
      ObCheckpointExecutor *checkpoint_executor = nullptr;
      if (OB_FAIL(ls_svr->get_ls(ls->get_ls_id(), ls_handle, ObLSGetMod::APPLY_MOD))) {
        STORAGE_LOG(WARN, "get log stream failed", K(ret), K(ls->get_ls_id()));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "log stream not exist", K(ret), K(ls->get_ls_id()));
      } else if (OB_ISNULL(checkpoint_executor = ls->get_checkpoint_executor())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "checkpoint_executor should not be null", K(ls->get_ls_id()));
      } else if (OB_FAIL(checkpoint_executor->traversal_flush())) {
        STORAGE_LOG(WARN, "traversal_flush failed", K(ret), K(ls->get_ls_id()));
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {
        STORAGE_LOG(INFO, "succeed to traversal_flush", K(ret), K(ls_cnt));
      } else {
        STORAGE_LOG(INFO, "no logstream", K(ret), K(ls_cnt));
      }
    }
  }
  ObCurTraceId::reset();
}

void ObCheckPointService::ObCheckClogDiskUsageTask::runTimerTask()
{
  STORAGE_LOG(INFO, "====== check clog disk timer task ======");
  int ret = OB_SUCCESS;
  int64_t threshold_size = INT64_MAX;
  bool need_flush = false;
  if (checkpoint_service_.get_disk_usage_threshold_(threshold_size)) {
    if (checkpoint_service_.cannot_recycle_log_over_threshold_(threshold_size,
        false /* not udpate clog_checkpoint_scn */ )) {
      // update clog_checkpoint_scn, double check
      if (checkpoint_service_.cannot_recycle_log_over_threshold_(threshold_size,
          true /* update clog_checkpoint_scn */ )) {
        need_flush = true;
      }
    }
  }

  if (need_flush && OB_FAIL(checkpoint_service_.flush_to_recycle_clog_())) {
    STORAGE_LOG(ERROR, "flush to recycle clog failed", K(ret), K(need_flush));
  }
}

} // checkpoint
} // storage
} // oceanbase
