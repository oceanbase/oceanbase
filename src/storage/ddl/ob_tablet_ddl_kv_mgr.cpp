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
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::storage;

ObTabletDDLKvMgr::ObTabletDDLKvMgr()
  : is_inited_(false),
    ls_id_(), tablet_id_(),
    max_freeze_scn_(SCN::min_scn()),
    head_(0), tail_(0), lock_(), ref_cnt_(0)
{
}

ObTabletDDLKvMgr::~ObTabletDDLKvMgr()
{
  destroy();
}

void ObTabletDDLKvMgr::destroy()
{
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
  max_freeze_scn_.set_min();
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
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletDDLKvMgr::set_max_freeze_scn(const share::SCN &checkpoint_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!checkpoint_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(checkpoint_scn));
  } else {
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    max_freeze_scn_ = checkpoint_scn;
  }
  return ret;
}

int ObTabletDDLKvMgr::get_rec_scn(SCN &rec_scn)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletFullDirectLoadMgr *tablet_mgr = nullptr;
  ObTabletDirectLoadMgrHandle direct_load_mgr_hdl;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  bool is_major_sstable_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
          ls_id_,
          tablet_id_,
          true/* is_full_direct_load */,
          direct_load_mgr_hdl,
          is_major_sstable_exist))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get tablet mgr failed", K(ret), K(tablet_id_));
    }
  } else if (OB_ISNULL(tablet_mgr = direct_load_mgr_hdl.get_full_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(ls_id_), K(tablet_id_));
  }
  if (OB_SUCC(ret) && nullptr != tablet_mgr) {
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
      const share::SCN start_scn_in_mem = tablet_mgr->get_start_scn();
      const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
      if (start_scn_in_mem.is_valid_and_not_min() && start_scn_in_mem > tablet_meta.ddl_start_scn_) {
        // has a latest start log and not flushed to tablet meta, keep it
        rec_scn = SCN::min(rec_scn, start_scn_in_mem);
      }
    }

    // rec scn of ddl commit log
    if (OB_SUCC(ret)) {
      const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
      if (tablet_meta.ddl_commit_scn_.is_valid_and_not_min()) {
        // has commit log and already dumped to tablet meta, skip
      } else {
        const SCN commit_scn = tablet_mgr->get_commit_scn(tablet_meta);
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

int ObTabletDDLKvMgr::cleanup()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
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
  max_freeze_scn_.set_min();
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
                                                    ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(cleanup())) {
    LOG_WARN("failed to cleanup ddl kv mgr", K(ret), KPC(tablet_handle.get_obj()));
  }
  return ret;
}

int ObTabletDDLKvMgr::rdlock(const int64_t timeout_us, uint32_t &tid)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = timeout_us + ObTimeUtility::current_time();
  if (OB_SUCC(lock_.rdlock(ObLatchIds::TABLET_DDL_KV_MGR_LOCK, abs_timeout_us))) {
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
  if (OB_SUCC(lock_.wrlock(ObLatchIds::TABLET_DDL_KV_MGR_LOCK, abs_timeout_us))) {
    tid = static_cast<uint32_t>(GETTID());
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_EAGAIN;
  }
  return ret;
}

void ObTabletDDLKvMgr::unlock(const uint32_t tid)
{
  if (OB_SUCCESS != lock_.unlock(&tid)) {
    ob_abort();
  }
}

int ObTabletDDLKvMgr::get_freezed_ddl_kv(const SCN &freeze_scn, ObDDLKVHandle &kv_handle)
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
      ObDDLKVHandle &cur_kv_handle = ddl_kv_handles_[idx];
      ObDDLKV *cur_kv = cur_kv_handle.get_obj();
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

bool ObTabletDDLKvMgr::can_freeze()
{
  int64_t ddl_kv_count = get_count();
  return ddl_kv_count < MAX_DDL_KV_CNT_IN_STORAGE;
}

int64_t ObTabletDDLKvMgr::get_count_nolock() const
{
  return tail_ - head_;
}

int64_t ObTabletDDLKvMgr::get_idx(const int64_t pos) const
{
  return pos & (MAX_DDL_KV_CNT_IN_STORAGE - 1);
}

int ObTabletDDLKvMgr::get_active_ddl_kv_impl(ObDDLKVHandle &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  if (get_count_nolock() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ObDDLKVHandle &tail_kv_handle = ddl_kv_handles_[get_idx(tail_ - 1)];
    ObDDLKV *kv = tail_kv_handle.get_obj();
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

int ObTabletDDLKvMgr::get_or_create_ddl_kv(
    const share::SCN &macro_redo_scn,
    const share::SCN &macro_redo_start_scn,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    ObDDLKVHandle &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  uint32_t direct_load_lock_tid = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_redo_scn.is_valid_and_not_min() || !macro_redo_start_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_redo_scn), K(macro_redo_start_scn));
  } else if (OB_FAIL(direct_load_mgr_handle.get_obj()->rdlock(TRY_LOCK_TIMEOUT/*10s*/, direct_load_lock_tid))) {
    // usually use the latest start scn to allocate kv.
    LOG_WARN("lock failed", K(ret));
  } else if (OB_UNLIKELY(macro_redo_start_scn < direct_load_mgr_handle.get_obj()->get_start_scn())) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("ddl task expired", K(ret), K(macro_redo_start_scn), "start_scn", direct_load_mgr_handle.get_obj()->get_start_scn());
  } else if (OB_UNLIKELY(macro_redo_start_scn > direct_load_mgr_handle.get_obj()->get_start_scn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected start scn in memory", K(ret), K(macro_redo_start_scn), "start_scn", direct_load_mgr_handle.get_obj()->get_start_scn());
  } else {
    uint32_t lock_tid = 0; // try lock to avoid hang in clog callback
    if (OB_FAIL(rdlock(TRY_LOCK_TIMEOUT, lock_tid))) {
      LOG_WARN("failed to rdlock", K(ret), KPC(this));
    } else {
      try_get_ddl_kv_unlock(macro_redo_scn, kv_handle);
    }
    if (lock_tid != 0) {
      unlock(lock_tid);
    }
  }
  if (OB_SUCC(ret) && !kv_handle.is_valid()) {
    uint32_t lock_tid = 0; // try lock to avoid hang in clog callback
    if (OB_FAIL(wrlock(TRY_LOCK_TIMEOUT, lock_tid))) {
      LOG_WARN("failed to wrlock", K(ret), KPC(this));
    } else {
      try_get_ddl_kv_unlock(macro_redo_scn, kv_handle);
      if (kv_handle.is_valid()) {
        // do nothing
      } else if (OB_FAIL(alloc_ddl_kv(direct_load_mgr_handle.get_obj()->get_start_scn(),
        direct_load_mgr_handle.get_obj()->get_table_key().get_snapshot_version(),
        direct_load_mgr_handle.get_obj()->get_data_format_version(),
        kv_handle))) {
        LOG_WARN("create ddl kv failed", K(ret), KPC(direct_load_mgr_handle.get_obj()));
      }
    }
    if (lock_tid != 0) {
      unlock(lock_tid);
    }
  }
  if (direct_load_lock_tid != 0) {
    direct_load_mgr_handle.get_obj()->unlock(direct_load_lock_tid);
  }
  return ret;
}

void ObTabletDDLKvMgr::try_get_ddl_kv_unlock(const SCN &scn, ObDDLKVHandle &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  if (get_count_nolock() > 0) {
    for (int64_t i = tail_ - 1; OB_SUCC(ret) && i >= head_ && !kv_handle.is_valid(); --i) {
      ObDDLKVHandle &tmp_kv_handle = ddl_kv_handles_[get_idx(i)];
      ObDDLKV *tmp_kv = tmp_kv_handle.get_obj();
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

int ObTabletDDLKvMgr::freeze_ddl_kv(
  const share::SCN &start_scn,
  const int64_t snapshot_version,
  const uint64_t data_format_version,
  const SCN &freeze_scn)
{
  int ret = OB_SUCCESS;
  ObDDLKVHandle kv_handle;
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
    if (OB_FAIL(alloc_ddl_kv(start_scn, snapshot_version, data_format_version, kv_handle))) {
      LOG_WARN("create ddl kv failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && kv_handle.is_valid()) {
    ObDDLKV *kv = kv_handle.get_obj();
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
      ObDDLKV *kv = ddl_kv_handles_[idx].get_obj();
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
      ObDDLKV *kv = ddl_kv_handles_[idx].get_obj();
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

int ObTabletDDLKvMgr::get_ddl_kvs_unlock(const bool frozen_only, ObIArray<ObDDLKVHandle> &kv_handle_array)
{
  int ret = OB_SUCCESS;
  kv_handle_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else {
    for (int64_t pos = head_; OB_SUCC(ret) && pos < tail_; ++pos) {
      const int64_t idx = get_idx(pos);
      ObDDLKVHandle &cur_kv_handle = ddl_kv_handles_[idx];
      ObDDLKV *cur_kv = cur_kv_handle.get_obj();
      if (OB_ISNULL(cur_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(cur_kv), K(pos), K(head_), K(tail_));
      } else if (!frozen_only || cur_kv->is_freezed()) {
        if (OB_FAIL(kv_handle_array.push_back(cur_kv_handle))) {
          LOG_WARN("fail to push back ddl kv", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_kvs(const bool frozen_only, ObIArray<ObDDLKVHandle> &kv_handle_array)
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

int ObTabletDDLKvMgr::get_ddl_kvs_for_query(ObTablet &tablet, ObIArray<ObDDLKVHandle> &kv_handle_array)
{
  int ret = OB_SUCCESS;
  kv_handle_array.reset();
  ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_FAIL(get_ddl_kvs_unlock(true/*frozen_only*/, kv_handle_array))) {
    LOG_WARN("get ddl kv unlock failed", K(ret));
  }
  return ret;
}

// when ddl commit scn is only in memory, try flush it, need wait log replay point elapsed the ddl commit scn
int ObTabletDDLKvMgr::try_flush_ddl_commit_scn(
    ObLSHandle &ls_handle,
    const ObTabletHandle &tablet_handle,
    const ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    const share::SCN &commit_scn)
{
  int ret = OB_SUCCESS;
   ObTabletFullDirectLoadMgr *direct_load_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_handle.is_valid() || OB_ISNULL(direct_load_mgr = direct_load_mgr_handle.get_full_obj()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_handle), K(tablet_handle));
  } else if (commit_scn.is_valid_and_not_min() // already committed
      && tablet_handle.get_obj()->get_tablet_meta().ddl_checkpoint_scn_ != commit_scn) {// only exist in memory
    SCN max_decided_scn;
    bool already_freezed = true;
    {
      ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
      already_freezed = max_freeze_scn_ >= commit_scn;
    }
    if (already_freezed) {
      // do nothing
    } else if (OB_FAIL(ls_handle.get_ls()->get_max_decided_scn(max_decided_scn))) {
      LOG_WARN("get max decided log ts failed", K(ret), K(ls_handle.get_ls()->get_ls_id()));
    } else if (SCN::plus(max_decided_scn, 1) >= commit_scn) { // commit_scn elapsed, means the prev clog already replayed or applied
      // max_decided_scn is the left border scn - 1
      // the min deciding(replay or apply) scn (aka left border) is max_decided_scn + 1
      if (OB_FAIL(freeze_ddl_kv(direct_load_mgr->get_start_scn(),
              direct_load_mgr->get_table_key().get_snapshot_version(),
              direct_load_mgr->get_data_format_version(),
              commit_scn))) {
        LOG_WARN("freeze ddl kv failed", K(ret));
      }
    }
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

int ObTabletDDLKvMgr::check_has_freezed_ddl_kv(bool &has_freezed_ddl_kv)
{
  int ret = OB_SUCCESS;
  has_freezed_ddl_kv = false;
  ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else {
    for (int64_t pos = head_; !has_freezed_ddl_kv && OB_SUCC(ret) && pos < tail_; ++pos) {
      const int64_t idx = get_idx(pos);
      ObDDLKVHandle &cur_kv_handle = ddl_kv_handles_[idx];
      ObDDLKV *cur_kv = cur_kv_handle.get_obj();
      if (OB_ISNULL(cur_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(cur_kv), K(pos), K(head_), K(tail_));
      } else if (cur_kv->is_freezed()) {
        has_freezed_ddl_kv = true;
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::alloc_ddl_kv(
    const share::SCN &start_scn,
    const int64_t snapshot_version,
    const uint64_t data_format_version,
    ObDDLKVHandle &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ObDDLKVHandle tmp_kv_handle;
  ObDDLKV *kv = nullptr;
  ObDDLMemtable *ddl_memtable = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv manager not init", K(ret));
  } else if (get_count_nolock() == MAX_DDL_KV_CNT_IN_STORAGE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, too much ddl kv count", K(ret));
  } else if (OB_FAIL(t3m->acquire_ddl_kv(tmp_kv_handle))) {
    LOG_WARN("acquire ddl kv failed", K(ret));
  } else if (OB_ISNULL(kv = tmp_kv_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl kv is null", K(ret));
  } else if (OB_FAIL(kv->init(ls_id_,
                              tablet_id_,
                              start_scn,
                              snapshot_version,
                              max_freeze_scn_,
                              data_format_version))) {
    LOG_WARN("fail to init ddl kv", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    const int64_t idx = get_idx(tail_);
    tail_++;
    ddl_kv_handles_[idx] = tmp_kv_handle;
    kv_handle = tmp_kv_handle;
    FLOG_INFO("succeed to add ddl kv", K(ls_id_), K(tablet_id_), K(head_), K(tail_), "ddl_kv_cnt", get_count_nolock(), KP(kv));
  }
  return ret;
}

void ObTabletDDLKvMgr::set_ddl_kv(const int64_t idx, ObDDLKVHandle &kv_handle)
{
  //only for unittest
  ddl_kv_handles_[idx] = kv_handle;
  tail_++;
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
    FLOG_INFO("free ddl kv", K(ls_id_), K(tablet_id_), KPC(ddl_kv_handles_[idx].get_obj()));
    ddl_kv_handles_[idx].reset();
  }
}

