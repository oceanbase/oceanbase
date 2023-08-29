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

#include "ob_tablet_ddl_kv_mgr.h"
#include "share/scn.h"
#include "share/ob_force_print_log.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/blocksstable/ob_sstable_sec_meta_iterator.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::storage;

ObTabletDDLKvMgr::ObTabletDDLKvMgr()
  : is_inited_(false), ls_id_(), tablet_id_(), success_start_scn_(SCN::min_scn()), table_key_(), data_format_version_(0),
    start_scn_(SCN::min_scn()), commit_scn_(SCN::min_scn()), execution_id_(-1), state_lock_(),
    max_freeze_scn_(SCN::min_scn()), head_(0), tail_(0), lock_(), ref_cnt_(0)
{
}

ObTabletDDLKvMgr::~ObTabletDDLKvMgr()
{
  destroy();
}

void ObTabletDDLKvMgr::destroy()
{
  if (is_started()) {
    LOG_INFO("start destroy ddl kv manager", K(ls_id_), K(tablet_id_), K(start_scn_), K(head_), K(tail_), K(lbt()));
  }
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  ATOMIC_STORE(&ref_cnt_, 0);
  for (int64_t pos = head_; pos < tail_; ++pos) {
    const int64_t idx = get_idx(pos);
    free_ddl_kv(idx);
  }
  head_ = 0;
  tail_ = 0;
  for (int64_t i = 0; i < MAX_DDL_KV_CNT_IN_STORAGE; ++i) {
    ddl_kv_handles_[i].reset();
  }
  ls_id_.reset();
  tablet_id_.reset();
  table_key_.reset();
  data_format_version_ = 0;
  start_scn_.set_min();
  commit_scn_.set_min();
  max_freeze_scn_.set_min();
  execution_id_ = -1;
  success_start_scn_.set_min();
  is_inited_ = false;
}

int ObTabletDDLKvMgr::init(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletDDLKvMgr is already inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletDDLKvMgr::ddl_start_nolock(const ObITable::TableKey &table_key,
                                       const SCN &start_scn,
                                       const int64_t data_format_version,
                                       const int64_t execution_id,
                                       const SCN &checkpoint_scn)
{
  int ret = OB_SUCCESS;
  bool is_brand_new = false;
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!table_key.is_valid() || !start_scn.is_valid_and_not_min() || execution_id < 0 || data_format_version < 0
        || (checkpoint_scn.is_valid_and_not_min() && checkpoint_scn < start_scn))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), K(start_scn), K(execution_id), K(data_format_version), K(checkpoint_scn));
  } else if (table_key.get_tablet_id() != tablet_id_) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet id not same", K(ret), K(table_key), K(tablet_id_));
  } else {
    if (start_scn_.is_valid_and_not_min()) {
      if (execution_id >= execution_id_ && start_scn >= start_scn_) {
        LOG_INFO("execution id changed, need cleanup", K(ls_id_), K(tablet_id_), K(execution_id_), K(execution_id), K(start_scn_), K(start_scn));
        cleanup_unlock();
        is_brand_new = true;
      } else {
        if (!checkpoint_scn.is_valid_and_not_min()) {
          // only return error code when not start from checkpoint.
          ret = OB_TASK_EXPIRED;
        }
        LOG_INFO("ddl start ignored", K(ls_id_), K(tablet_id_), K(execution_id_), K(execution_id), K(start_scn_), K(start_scn));
      }
    } else {
      is_brand_new = true;
    }
    if (OB_SUCC(ret) && is_brand_new) {
      table_key_ = table_key;
      data_format_version_ = data_format_version;
      execution_id_ = execution_id;
      start_scn_.atomic_store(start_scn);
      max_freeze_scn_ = SCN::max(start_scn, checkpoint_scn);
    }
  }
  return ret;
}

// ddl start from log
//    cleanup ddl sstable
// ddl start from checkpoint
//    keep ddl sstable table

int ObTabletDDLKvMgr::ddl_start(ObLS &ls,
                                ObTablet &tablet,
                                const ObITable::TableKey &table_key,
                                const SCN &start_scn,
                                const int64_t data_format_version,
                                const int64_t execution_id,
                                const SCN &checkpoint_scn)
{
  int ret = OB_SUCCESS;
  SCN saved_start_scn;
  int64_t saved_snapshot_version = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    ObLatchWGuard state_guard(state_lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    if (OB_FAIL(ddl_start_nolock(table_key, start_scn, data_format_version, execution_id, checkpoint_scn))) {
      LOG_WARN("failed to ddl start", K(ret));
    } else if (OB_FAIL(ls.get_ddl_log_handler()->add_tablet(tablet_id_))) {
      LOG_WARN("add tablet failed", K(ret));
    } else {
      // save variables under lock
      saved_start_scn = start_scn_;
      saved_snapshot_version = table_key_.get_snapshot_version();
      commit_scn_.atomic_store(get_commit_scn(tablet.get_tablet_meta()));
      if (checkpoint_scn.is_valid_and_not_min()) {
        if (tablet.get_tablet_meta().table_store_flag_.with_major_sstable() && tablet.get_tablet_meta().ddl_commit_scn_.is_valid_and_not_min()) {
          success_start_scn_.atomic_store(tablet.get_tablet_meta().ddl_start_scn_);
        }
      }
    }
  }
  if (OB_SUCC(ret) && !checkpoint_scn.is_valid_and_not_min()) {
    // remove ddl sstable if exists and flush ddl start log ts and snapshot version into tablet meta
    if (OB_FAIL(update_tablet(tablet, saved_start_scn, saved_snapshot_version, data_format_version, execution_id, saved_start_scn))) {
      LOG_WARN("clean up ddl sstable failed", K(ret), K(ls_id_), K(tablet_id_));
    }
  }
  FLOG_INFO("start ddl kv mgr finished", K(ret), K(start_scn), K(execution_id), K(checkpoint_scn), K(*this));
  return ret;
}

int ObTabletDDLKvMgr::ddl_commit(ObTablet &tablet, const SCN &start_scn, const SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (!is_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl not started", K(ret));
  } else if (start_scn < get_start_scn()) {
    ret = OB_TASK_EXPIRED;
    LOG_INFO("skip ddl commit log", K(start_scn), K(*this));
  } else if (OB_FAIL(set_commit_scn(tablet.get_tablet_meta(), commit_scn))) {
    LOG_WARN("failed to set commit scn", K(ret));
  } else if (OB_FAIL(freeze_ddl_kv(tablet, commit_scn))) {
    LOG_WARN("freeze ddl kv failed", K(ret), K(commit_scn));
  } else {
    ret = OB_EAGAIN;
    while (OB_EAGAIN == ret) {
      if (OB_FAIL(update_ddl_major_sstable(tablet))) {
        LOG_WARN("update ddl major sstable failed", K(ret));
      }
      if (OB_EAGAIN == ret) {
        usleep(1000L);
      }
    }

    ObDDLTableMergeDagParam param;
    param.ls_id_ = ls_id_;
    param.tablet_id_ = tablet_id_;
    param.rec_scn_ = commit_scn;
    param.is_commit_ = true;
    param.start_scn_ = start_scn;
    param.compat_mode_ = tablet.get_tablet_meta().compat_mode_;
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet.get_ddl_kv_mgr(param.ddl_kv_mgr_handle_))) {
      LOG_WARN("failed to get ddl kv mgr", K(ret));
    } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
      } else {
        ret = OB_SUCCESS; // the backgroud scheduler will reschedule again
        LOG_INFO("schedule ddl merge task need retry",
            K(start_scn), K(commit_scn), K(*this),
            "wait_elpased_s", (ObTimeUtility::fast_current_time() - start_ts) / 1000000L);
      }
    } else {
      LOG_INFO("schedule ddl commit task success", K(start_scn), K(commit_scn), K(*this));
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::schedule_ddl_dump_task(ObTablet &tablet, const SCN &start_scn, const SCN &rec_scn)
{
  int ret = OB_SUCCESS;
  ObDDLTableMergeDagParam param;
  param.ls_id_ = ls_id_;
  param.tablet_id_ = tablet_id_;
  param.rec_scn_ = rec_scn;
  param.is_commit_ = false;
  param.start_scn_ = start_scn;
  param.compat_mode_ = tablet.get_tablet_meta().compat_mode_;
  LOG_INFO("schedule ddl dump task", K(param));
  if (OB_UNLIKELY(tablet.get_tablet_meta().tablet_id_ != tablet_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet id mismatched", K(ret), K(tablet), KPC(this));
  } else if (OB_FAIL(tablet.get_ddl_kv_mgr(param.ddl_kv_mgr_handle_))) {
    LOG_WARN("failed to get ddl kv mgr", K(ret));
  } else if (OB_FAIL(freeze_ddl_kv(tablet))) {
    LOG_WARN("ddl kv manager try freeze failed", K(ret), K(param));
  } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::schedule_ddl_merge_task(ObTablet &tablet, const SCN &start_scn, const SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (is_commit_success()) {
    FLOG_INFO("ddl commit already succeed", K(start_scn), K(commit_scn), K(*this));
  } else if (start_scn < get_start_scn()) {
    ret = OB_TASK_EXPIRED;
    LOG_INFO("skip ddl commit log", K(start_scn), K(commit_scn), K(*this));
  } else {
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    ObDDLTableMergeDagParam param;
    param.ls_id_ = ls_id_;
    param.tablet_id_ = tablet_id_;
    param.rec_scn_ = commit_scn;
    param.is_commit_ = true;
    param.start_scn_ = start_scn;
    param.compat_mode_ = tablet.get_tablet_meta().compat_mode_;
    // check ls/tablet state by get_ls/ddl_get_tablet, and retry submit dag in case of the previous dag failed
    if (OB_FAIL(tablet.get_ddl_kv_mgr(param.ddl_kv_mgr_handle_))) {
      LOG_WARN("failed to get ddl kv", K(ret), K(param));
    } else if (OB_FAIL(freeze_ddl_kv(tablet))) {
      LOG_WARN("ddl kv manager try freeze failed", K(ret), K(param));
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(param));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                                 param.tablet_id_,
                                                 tablet_handle,
                                                 ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("failed to get tablet", K(ret), K(param));
    } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      if (OB_SIZE_OVERFLOW == ret || OB_EAGAIN == ret) {
        ret = OB_EAGAIN;
      } else {
        LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
      }
    } else {
      ret = OB_EAGAIN; // until major sstable is ready
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::wait_ddl_merge_success(ObTablet &tablet, const SCN &start_scn, const SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!start_scn.is_valid_and_not_min() || !commit_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_scn), K(commit_scn));
  } else if (!is_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl not started", K(ret));
  } else if (start_scn > get_start_scn()) {
    ret = OB_ERR_SYS;
    LOG_WARN("start log ts not match", K(ret), K(start_scn), K(start_scn_), K(ls_id_), K(tablet_id_));
  } else {
    const int64_t wait_start_ts = ObTimeUtility::fast_current_time();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(schedule_ddl_merge_task(tablet, start_scn, commit_scn))) {
        if (OB_EAGAIN == ret) {
          ob_usleep(100L); // 100us.
          ret = OB_SUCCESS; // retry
        } else {
          LOG_WARN("commit ddl log failed", K(ret), K(start_scn), K(commit_scn), K(ls_id_), K(tablet_id_));
        }
      } else {
        break;
      }
      if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
        LOG_INFO("wait build ddl sstable", K(ret), K(ls_id_), K(tablet_id_), K(start_scn_), K(commit_scn), K(max_freeze_scn_),
            "wait_elpased_s", (ObTimeUtility::fast_current_time() - wait_start_ts) / 1000000L);
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_major_merge_param(ObTablet &tablet, ObDDLTableMergeDagParam &param)
{
  int ret = OB_SUCCESS;
  uint32_t lock_tid = 0;
  if (OB_FAIL(tablet.get_ddl_kv_mgr(param.ddl_kv_mgr_handle_))) {
    LOG_WARN("failed to get ddl kv mgr", K(ret));
  } else if (OB_FAIL(rdlock(TRY_LOCK_TIMEOUT, lock_tid))) {
    LOG_WARN("failed to rdlock", K(ret), KPC(this));
  } else if (can_schedule_major_compaction_nolock(tablet.get_tablet_meta())) {
    param.ls_id_ = ls_id_;
    param.tablet_id_ = tablet_id_;
    param.rec_scn_ = get_commit_scn(tablet.get_tablet_meta());
    param.is_commit_ = true;
    param.start_scn_ = start_scn_;
    param.compat_mode_ = tablet.get_tablet_meta().compat_mode_;
  } else {
    ret = OB_EAGAIN;
  }
  if (0 != lock_tid) {
    unlock(lock_tid);
  }
  return ret;
}

int ObTabletDDLKvMgr::get_rec_scn(SCN &rec_scn)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  const bool is_commit_succ = is_commit_success();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  }
  if (OB_SUCC(ret) && !is_commit_succ) {
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_,
                                                      tablet_handle,
                                                      ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
                                                      ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
      LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
    }

    // rec scn of ddl start log
    if (OB_SUCC(ret)) {
      const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
      const SCN start_scn = get_start_scn();
      if (start_scn.is_valid_and_not_min() && start_scn > tablet_meta.ddl_start_scn_) {
        // has a latest start log and not flushed to tablet meta, keep it
        rec_scn = SCN::min(rec_scn, start_scn);
      }
    }

    // rec scn of ddl commit log
    if (OB_SUCC(ret)) {
      const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
      if (tablet_meta.ddl_commit_scn_.is_valid_and_not_min()) {
        // has commit log and already dumped to tablet meta, skip
      } else {
        const SCN commit_scn = get_commit_scn(tablet_meta);
        if (commit_scn.is_valid_and_not_min()) {
          // has commit log and not yet dumped to tablet meta
          rec_scn = SCN::min(rec_scn, commit_scn);
        } else {
          // no commit log
        }
      }
    }
  }

  // rec scn of ddl redo
  if (OB_SUCC(ret)) {
    bool has_ddl_kv = false;
    if (OB_FAIL(check_has_effective_ddl_kv(has_ddl_kv))) {
      LOG_WARN("failed to check ddl kv", K(ret));
    } else if (has_ddl_kv) {
      SCN min_scn;
      if (OB_FAIL(get_ddl_kv_min_scn(min_scn))) {
        LOG_WARN("fail to get ddl kv min log ts", K(ret));
      } else {
        rec_scn = SCN::min(rec_scn, min_scn);
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::set_commit_scn(const ObTabletMeta &tablet_meta, const SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(commit_scn <= SCN::min_scn())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(commit_scn));
  } else {
    ObLatchWGuard state_guard(state_lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    const SCN old_commit_scn = get_commit_scn(tablet_meta);
    if (old_commit_scn.is_valid_and_not_min() && old_commit_scn != commit_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("already committed by others", K(ret), K(commit_scn), K(*this));
    } else {
      commit_scn_.atomic_store(commit_scn);
    }
  }
  return ret;
}

SCN ObTabletDDLKvMgr::get_commit_scn(const ObTabletMeta &tablet_meta)
{
  SCN mgr_commit_scn = commit_scn_.atomic_load();
  SCN commit_scn = SCN::min_scn();
  if (tablet_meta.ddl_commit_scn_.is_valid_and_not_min() || mgr_commit_scn.is_valid_and_not_min()) {
    if (tablet_meta.ddl_commit_scn_.is_valid_and_not_min()) {
      commit_scn = tablet_meta.ddl_commit_scn_;
    } else {
      commit_scn = mgr_commit_scn;
    }
  } else {
    commit_scn = SCN::min_scn();
  }
  return commit_scn;
}

int ObTabletDDLKvMgr::set_commit_success(const SCN &start_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(start_scn <= SCN::min_scn())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_scn));
  } else {
    ObLatchWGuard state_guard(state_lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    if (start_scn < start_scn_) {
      ret = OB_TASK_EXPIRED;
      LOG_WARN("ddl task expired", K(ret), K(start_scn), K(*this));
    } else if (OB_UNLIKELY(start_scn > start_scn_)) {
      if (start_scn_.is_valid_and_not_min()) {
        ret = OB_ERR_SYS;
        LOG_WARN("sucess start log ts too large", K(ret), K(start_scn), K(*this));
      } else {
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(1000L * 1000L * 60L)) {
          LOG_INFO("ddl start scn is invalid, maybe migration has offlined the logstream", K(*this));
        }
      }
    } else {
      success_start_scn_.atomic_store(start_scn);
    }
  }
  return ret;
}

bool ObTabletDDLKvMgr::is_commit_success()
{
  const SCN success_start_scn = success_start_scn_.atomic_load();
  const SCN start_scn = start_scn_.atomic_load();
  return success_start_scn > SCN::min_scn() && success_start_scn == start_scn;
}

void ObTabletDDLKvMgr::reset_commit_success()
{
  ObLatchWGuard state_guard(state_lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  success_start_scn_.atomic_store(SCN::min_scn());
}

bool ObTabletDDLKvMgr::can_schedule_major_compaction_nolock(const ObTabletMeta &tablet_meta)
{
  return get_commit_scn(tablet_meta).is_valid_and_not_min() && !is_commit_success();
}

int ObTabletDDLKvMgr::cleanup()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObLatchWGuard state_guard(state_lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    cleanup_unlock();
  }
  return ret;
}

void ObTabletDDLKvMgr::cleanup_unlock()
{
  LOG_INFO("cleanup ddl kv mgr", K(*this));
  for (int64_t pos = head_; pos < tail_; ++pos) {
    const int64_t idx = get_idx(pos);
    free_ddl_kv(idx);
  }
  head_ = 0;
  tail_ = 0;
  for (int64_t i = 0; i < MAX_DDL_KV_CNT_IN_STORAGE; ++i) {
    ddl_kv_handles_[i].reset();
  }
  table_key_.reset();
  data_format_version_ = 0;
  start_scn_.atomic_store(SCN::min_scn());
  commit_scn_.atomic_store(SCN::min_scn());
  max_freeze_scn_.set_min();
  execution_id_ = -1;
  success_start_scn_.atomic_store(SCN::min_scn());
}

bool ObTabletDDLKvMgr::is_execution_id_older(const int64_t execution_id)
{
  return execution_id < execution_id_;
}

int ObTabletDDLKvMgr::online()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_,
                                                    tablet_handle,
                                                    0,
                                                    ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(cleanup())) {
    LOG_WARN("failed to cleanup ddl kv mgr", K(ret), KPC(tablet_handle.get_obj()));
  } else if (!tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_.is_valid_and_not_min()) {
    LOG_DEBUG("no need to start ddl kv manager", K(ret), "tablet_meta", tablet_handle.get_obj()->get_tablet_meta());
  } else {
    const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    table_key.tablet_id_ = tablet_meta.tablet_id_;
    table_key.version_range_.base_version_ = 0;
    table_key.version_range_.snapshot_version_ = tablet_meta.ddl_snapshot_version_;
    const SCN &start_scn = tablet_meta.ddl_start_scn_;
    if (OB_FAIL(ddl_start(*ls_handle.get_ls(),
                          *tablet_handle.get_obj(),
                          table_key,
                          start_scn,
                          tablet_meta.ddl_data_format_version_,
                          tablet_meta.ddl_execution_id_,
                          tablet_meta.ddl_checkpoint_scn_))) {
      if (OB_TASK_EXPIRED == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("start ddl kv manager failed", K(ret), K(tablet_meta));
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::register_to_tablet(const SCN &ddl_start_scn, ObDDLKvMgrHandle &kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ddl_start_scn.is_valid_and_not_min() || kv_mgr_handle.get_obj() != this)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_start_scn), KP(kv_mgr_handle.get_obj()), KP(this));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_,
                                                    tablet_handle,
                                                    0,
                                                    ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    ObLatchWGuard state_guard(state_lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    if (ddl_start_scn < start_scn_) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl task expired", K(ret), K(ls_id_), K(tablet_id_), K(start_scn_), K(ddl_start_scn));
    } else if (ddl_start_scn > start_scn_) {
      if (SCN::min_scn() == start_scn_) {
        // maybe ls offline
        ret = OB_EAGAIN;
      } else {
        ret = OB_ERR_SYS;
      }
      LOG_WARN("ddl kv mgr register before start", K(ret), K(ls_id_), K(tablet_id_), K(start_scn_), K(ddl_start_scn));
    } else {
      if (OB_FAIL(tablet_handle.get_obj()->set_ddl_kv_mgr(kv_mgr_handle))) {
        LOG_WARN("set ddl kv mgr into tablet failed", K(ret), K(ls_id_), K(tablet_id_), K(start_scn_));
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::unregister_from_tablet(const SCN &ddl_start_scn, ObDDLKvMgrHandle &kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ddl_start_scn.is_valid_and_not_min() || kv_mgr_handle.get_obj() != this)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_start_scn), KP(kv_mgr_handle.get_obj()), KP(this));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_,
                                                    tablet_handle,
                                                    0,
                                                    ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    ObLatchWGuard state_guard(state_lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    if (ddl_start_scn < start_scn_) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl task expired", K(ret), K(ls_id_), K(tablet_id_), K(start_scn_), K(ddl_start_scn));
    } else if (ddl_start_scn > start_scn_) {
      if (SCN::min_scn() == start_scn_) {
        // maybe ls offline
        ret = OB_EAGAIN;
      } else {
        ret = OB_ERR_SYS;
      }
      LOG_WARN("ddl kv mgr register before start", K(ret), K(ls_id_), K(tablet_id_), K(start_scn_), K(ddl_start_scn));
    } else {
      if (OB_FAIL(tablet_handle.get_obj()->remove_ddl_kv_mgr(kv_mgr_handle))) {
        LOG_WARN("remove ddl kv mgr from tablet failed", K(ret), K(ls_id_), K(tablet_id_), K(start_scn_));
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::rdlock(const int64_t timeout_us, uint32_t &tid)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = timeout_us + ObTimeUtility::current_time();
  if (OB_SUCC(state_lock_.rdlock(ObLatchIds::TABLET_DDL_KV_MGR_LOCK, abs_timeout_us))) {
    tid = static_cast<uint32_t>(GETTID());
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTabletDDLKvMgr::wrlock(const int64_t timeout_us, uint32_t &tid)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = timeout_us + ObTimeUtility::current_time();
  if (OB_SUCC(state_lock_.wrlock(ObLatchIds::TABLET_DDL_KV_MGR_LOCK, abs_timeout_us))) {
    tid = static_cast<uint32_t>(GETTID());
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_EAGAIN;
  }
  return ret;
}

void ObTabletDDLKvMgr::unlock(const uint32_t tid)
{
  if (OB_SUCCESS != state_lock_.unlock(&tid)) {
    ob_abort();
  }
}

int ObTabletDDLKvMgr::update_tablet(ObTablet &tablet,
                                    const SCN &start_scn,
                                    const int64_t snapshot_version,
                                    const int64_t data_format_version,
                                    const int64_t execution_id,
                                    const SCN &ddl_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObArenaAllocator tmp_arena("DDLUpdateTblTmp");
  const ObStorageSchema *storage_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!start_scn.is_valid_and_not_min() || snapshot_version <= 0 || !ddl_checkpoint_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_scn), K(snapshot_version), K(ddl_checkpoint_scn));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(tablet.load_storage_schema(tmp_arena, storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), K(tablet));
  } else {
    ObSSTable sstable;
    const int64_t rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    ObTabletHandle new_tablet_handle;
    ObUpdateTableStoreParam param(tablet.get_snapshot_version(),
                                  ObVersionRange::MIN_VERSION, // multi_version_start
                                  storage_schema,
                                  rebuild_seq);
    param.ddl_info_.keep_old_ddl_sstable_ = false;
    param.ddl_info_.ddl_start_scn_ = start_scn;
    param.ddl_info_.ddl_snapshot_version_ = snapshot_version;
    param.ddl_info_.ddl_checkpoint_scn_ = ddl_checkpoint_scn;
    param.ddl_info_.ddl_execution_id_ = execution_id;
    param.ddl_info_.data_format_version_ = data_format_version;
    if (OB_FAIL(create_empty_ddl_sstable(tablet, tmp_arena, sstable))) {
      LOG_WARN("create empty ddl sstable failed", K(ret));
    } else if (FALSE_IT(param.sstable_ = &sstable)) {
    } else if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(ls_id_), K(tablet_id_), K(param));
    } else {
      LOG_INFO("update tablet success", K(ls_id_), K(tablet_id_), K(param), K(start_scn), K(snapshot_version), K(ddl_checkpoint_scn));
    }
  }
  ObTablet::free_storage_schema(tmp_arena, storage_schema);
  return ret;
}

int ObTabletDDLKvMgr::create_empty_ddl_sstable(ObTablet &tablet, common::ObArenaAllocator &allocator, blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObTabletDDLParam ddl_param;
  if (OB_FAIL(get_ddl_param(ddl_param))) {
    LOG_WARN("get ddl param failed", K(ret));
  } else {
    ddl_param.table_key_.table_type_ = ObITable::DDL_DUMP_SSTABLE;
    ddl_param.table_key_.scn_range_.start_scn_ = SCN::scn_dec(start_scn_);
    ddl_param.table_key_.scn_range_.end_scn_ = start_scn_;
    ObArray<const ObDataMacroBlockMeta *> empty_meta_array;
    if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(tablet, ddl_param, empty_meta_array, nullptr/*first_ddl_sstable*/, allocator, sstable))) {
      LOG_WARN("create empty ddl sstable failed", K(ret));
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::update_ddl_major_sstable(ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObArenaAllocator allocator;
  const ObStorageSchema *storage_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(tablet.load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("load storage schema failed", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    ObTabletHandle new_tablet_handle;
    ObUpdateTableStoreParam param(tablet.get_snapshot_version(),
                                  ObVersionRange::MIN_VERSION, // multi_version_start
                                  storage_schema,
                                  ls_handle.get_ls()->get_rebuild_seq());
    param.ddl_info_.keep_old_ddl_sstable_ = true;
    param.ddl_info_.ddl_commit_scn_ = get_commit_scn(tablet.get_tablet_meta());
    if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(ls_id_), K(tablet_id_), K(param));
    }
  }
  ObTablet::free_storage_schema(allocator, storage_schema);
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_param(ObTabletDDLParam &ddl_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (!is_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl not started", K(ret));
  } else {
    ObLatchRGuard state_guard(state_lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    ddl_param.tenant_id_ = MTL_ID();
    ddl_param.ls_id_ = ls_id_;
    ddl_param.table_key_ = table_key_;
    ddl_param.start_scn_ = start_scn_;
    ddl_param.commit_scn_ = commit_scn_;
    ddl_param.snapshot_version_ = table_key_.get_snapshot_version();
    ddl_param.data_format_version_ = data_format_version_;
  }

  return ret;
}

int ObTabletDDLKvMgr::get_freezed_ddl_kv(const SCN &freeze_scn, ObTableHandleV2 &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    bool found = false;
    ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    for (int64_t i = head_; OB_SUCC(ret) && !found && i < tail_; ++i) {
      const int64_t idx = get_idx(i);
      ObTableHandleV2 &cur_kv_handle = ddl_kv_handles_[idx];
      ObDDLKV *cur_kv = static_cast<ObDDLKV *>(cur_kv_handle.get_table());
      if (OB_ISNULL(cur_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(cur_kv), K(i), K(head_), K(tail_));
      } else if (freeze_scn == cur_kv->get_freeze_scn()) {
        found = true;
        kv_handle = cur_kv_handle;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("freezed ddl kv with given freeze log scn is not found", K(ret), K(freeze_scn));
    }
  }
  return ret;
}
int64_t ObTabletDDLKvMgr::get_count()
{
  int64_t ddl_kv_count = 0;
  {
    ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    ddl_kv_count = tail_ - head_;
  }
  return ddl_kv_count;
}

int64_t ObTabletDDLKvMgr::get_count_nolock() const
{
  return tail_ - head_;
}

int64_t ObTabletDDLKvMgr::get_idx(const int64_t pos) const
{
  return pos & (MAX_DDL_KV_CNT_IN_STORAGE - 1);
}

int ObTabletDDLKvMgr::get_active_ddl_kv_impl(ObTableHandleV2 &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  if (get_count_nolock() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ObTableHandleV2 &tail_kv_handle = ddl_kv_handles_[get_idx(tail_ - 1)];
    ObDDLKV *kv = static_cast<ObDDLKV *>(tail_kv_handle.get_table());
    if (nullptr == kv) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, kv must not be nullptr", K(ret));
    } else if (kv->is_freezed()) {
      kv = nullptr;
      ret = OB_SUCCESS;
    } else {
      kv_handle = tail_kv_handle;
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_or_create_ddl_kv(ObTablet &tablet, const SCN &start_scn, const SCN &scn, ObTableHandleV2 &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (!scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(scn));
  } else {
    uint32_t lock_tid = 0; // try lock to avoid hang in clog callback
    if (OB_FAIL(rdlock(TRY_LOCK_TIMEOUT, lock_tid))) {
      LOG_WARN("failed to rdlock", K(ret), K(start_scn), KPC(this));
    } else if (start_scn != start_scn_) {
      ret = OB_TASK_EXPIRED;
      LOG_WARN("ddl task expired", K(ret), K(start_scn), KPC(this));
    } else {
      ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
      try_get_ddl_kv_unlock(scn, kv_handle);
    }
    if (lock_tid != 0) {
      unlock(lock_tid);
    }
  }
  if (OB_SUCC(ret) && !kv_handle.is_valid()) {
    uint32_t lock_tid = 0; // try lock to avoid hang in clog callback
    if (OB_FAIL(wrlock(TRY_LOCK_TIMEOUT, lock_tid))) {
      LOG_WARN("failed to wrlock", K(ret), K(start_scn), KPC(this));
    } else if (start_scn != start_scn_) {
      ret = OB_TASK_EXPIRED;
      LOG_WARN("ddl task expired", K(ret), K(start_scn), KPC(this));
    } else {
      ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
      try_get_ddl_kv_unlock(scn, kv_handle);
      if (kv_handle.is_valid()) {
        // do nothing
      } else if (OB_FAIL(alloc_ddl_kv(tablet, kv_handle))) {
        LOG_WARN("create ddl kv failed", K(ret));
      }
    }
    if (lock_tid != 0) {
      unlock(lock_tid);
    }
  }
  return ret;
}

void ObTabletDDLKvMgr::try_get_ddl_kv_unlock(const SCN &scn, ObTableHandleV2 &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  if (get_count_nolock() > 0) {
    for (int64_t i = tail_ - 1; OB_SUCC(ret) && i >= head_ && !kv_handle.is_valid(); --i) {
      ObTableHandleV2 &tmp_kv_handle = ddl_kv_handles_[get_idx(i)];
      ObDDLKV *tmp_kv = static_cast<ObDDLKV *>(tmp_kv_handle.get_table());
      if (OB_ISNULL(tmp_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(tmp_kv), K(i), K(head_), K(tail_));
      } else if (scn > tmp_kv->get_start_scn() && scn <= tmp_kv->get_freeze_scn()) {
        kv_handle = tmp_kv_handle;
        break;
      }
    }
  }
}

int ObTabletDDLKvMgr::freeze_ddl_kv(ObTablet &tablet, const SCN &freeze_scn)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 kv_handle;
  ObLatchWGuard state_guard(state_lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (0 == get_count_nolock()) {
    // do nothing
  } else if (OB_FAIL(get_active_ddl_kv_impl(kv_handle))) {
    LOG_WARN("fail to get active ddl kv", K(ret));
  }
  if (OB_SUCC(ret) && !kv_handle.is_valid() && freeze_scn > max_freeze_scn_) {
    // freeze_scn > 0 only occured when ddl commit
    // assure there is an alive ddl kv, for waiting pre-logs
    if (OB_FAIL(alloc_ddl_kv(tablet, kv_handle))) {
      LOG_WARN("create ddl kv failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && kv_handle.is_valid()) {
    ObDDLKV *kv = static_cast<ObDDLKV *>(kv_handle.get_table());
    if (OB_ISNULL(kv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl kv is null", K(ret), KP(kv), K(kv_handle));
    } else if (OB_FAIL(kv->freeze(freeze_scn))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail to freeze active ddl kv", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      max_freeze_scn_ = SCN::max(max_freeze_scn_, kv->get_freeze_scn());
      LOG_INFO("freeze ddl kv", "kv", *kv);
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::release_ddl_kvs(const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_RELEASE_DDL_KV);
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else {
    for (int64_t i = head_; OB_SUCC(ret) && i < tail_; ++i) {
      const int64_t idx = get_idx(head_);
      ObDDLKV *kv = static_cast<ObDDLKV *>(ddl_kv_handles_[idx].get_table());
      LOG_INFO("try release ddl kv", K(end_scn), KPC(kv));
#ifdef ERRSIM
          if (OB_SUCC(ret)) {
            ret = OB_E(EventTable::EN_DDL_RELEASE_DDL_KV_FAIL) OB_SUCCESS;
            if (OB_FAIL(ret)) {
              LOG_WARN("errsim release ddl kv failed", KR(ret));
            }
          }
#endif
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(kv), K(i), K(head_), K(tail_));
      } else if (kv->is_closed() && kv->get_freeze_scn() <= end_scn) {
        const SCN &freeze_scn = kv->get_freeze_scn();
        free_ddl_kv(idx);
        ++head_;
        LOG_INFO("succeed to release ddl kv", K(ls_id_), K(tablet_id_), K(freeze_scn));
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_kv_min_scn(SCN &min_scn)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  min_scn = SCN::max_scn();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else {
    for (int64_t i = head_; OB_SUCC(ret) && i < tail_; ++i) {
      const int64_t idx = get_idx(head_);
      ObDDLKV *kv = static_cast<ObDDLKV *>(ddl_kv_handles_[idx].get_table());
      if (OB_ISNULL(kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(kv), K(i), K(head_), K(tail_));
      } else {
        min_scn = SCN::min(min_scn, kv->get_min_scn());
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_kvs_unlock(const bool frozen_only, ObTablesHandleArray &kv_handle_array)
{
  int ret = OB_SUCCESS;
  kv_handle_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else {
    for (int64_t pos = head_; OB_SUCC(ret) && pos < tail_; ++pos) {
      const int64_t idx = get_idx(pos);
      ObTableHandleV2 &cur_kv_handle = ddl_kv_handles_[idx];
      ObDDLKV *cur_kv = static_cast<ObDDLKV *>(cur_kv_handle.get_table());
      if (OB_ISNULL(cur_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(cur_kv), K(pos), K(head_), K(tail_));
      } else if (!frozen_only || cur_kv->is_freezed()) {
        if (OB_FAIL(kv_handle_array.add_table(cur_kv_handle))) {
          LOG_WARN("fail to push back ddl kv", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_kvs(const bool frozen_only, ObTablesHandleArray &kv_handle_array)
{
  int ret = OB_SUCCESS;
  kv_handle_array.reset();
  ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_FAIL(get_ddl_kvs_unlock(frozen_only, kv_handle_array))) {
    LOG_WARN("get ddl kv unlock failed", K(ret));
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_kvs_for_query(ObTablet &tablet, ObTablesHandleArray &kv_handle_array)
{
  int ret = OB_SUCCESS;
  kv_handle_array.reset();
  ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (!tablet.get_tablet_meta().ddl_commit_scn_.is_valid_and_not_min()) {
    // do nothing
  } else if (OB_FAIL(get_ddl_kvs_unlock(true/*frozen_only*/, kv_handle_array))) {
    LOG_WARN("get ddl kv unlock failed", K(ret));
  }
  return ret;
}

int ObTabletDDLKvMgr::check_has_effective_ddl_kv(bool &has_ddl_kv)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else {
    has_ddl_kv = 0 != get_count_nolock();
  }
  return ret;
}

int ObTabletDDLKvMgr::alloc_ddl_kv(ObTablet &tablet, ObTableHandleV2 &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ObTableHandleV2 tmp_kv_handle;
  ObDDLKV *kv = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv manager not init", K(ret));
  } else if (OB_UNLIKELY(!is_started())) {
    ret = OB_ERR_SYS;
    LOG_WARN("ddl kv manager not started", K(ret));
  } else if (get_count_nolock() == MAX_DDL_KV_CNT_IN_STORAGE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, too much ddl kv count", K(ret));
  } else if (OB_FAIL(t3m->acquire_ddl_kv(tmp_kv_handle))) {
    LOG_WARN("acquire ddl kv failed", K(ret));
  } else if (OB_ISNULL(kv = static_cast<ObDDLKV *>(tmp_kv_handle.get_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl kv is null", K(ret));
  } else if (OB_FAIL(kv->init(tablet,
                              start_scn_,
                              table_key_.get_snapshot_version(),
                              max_freeze_scn_,
                              data_format_version_))) {
    LOG_WARN("fail to init ddl kv", K(ret), K(ls_id_), K(tablet_id_),
        K(start_scn_), K(table_key_), K(max_freeze_scn_), K(data_format_version_));
  } else {
    const int64_t idx = get_idx(tail_);
    tail_++;
    ddl_kv_handles_[idx] = tmp_kv_handle;
    kv_handle = tmp_kv_handle;
    FLOG_INFO("succeed to add ddl kv", K(ls_id_), K(tablet_id_), K(head_), K(tail_), "ddl_kv_cnt", get_count_nolock(), KP(kv));
  }
  return ret;
}

void ObTabletDDLKvMgr::free_ddl_kv(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= MAX_DDL_KV_CNT_IN_STORAGE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx));
  } else {
    FLOG_INFO("free ddl kv", K(ls_id_), K(tablet_id_), KPC(ddl_kv_handles_[idx].get_table()));
    ddl_kv_handles_[idx].reset();
  }
}

