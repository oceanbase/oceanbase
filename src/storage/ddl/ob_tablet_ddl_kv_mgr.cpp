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
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ddl/ob_ddl_merge_schedule.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::transaction;

// invalid ddl_kv_type, trans_id, seq_no means all
bool ObDDLKVQueryParam::match_ddl_kv(const ObDDLKV &ddl_kv) const
{
  return (!is_valid_ddl_kv(ddl_kv_type_)
           || (ddl_kv_type_ == ddl_kv.get_ddl_kv_type()))
         && (!trans_id_.is_valid()
           || (trans_id_ == ddl_kv.get_trans_id()))
         && (!seq_no_.is_valid()
           || (seq_no_ == ddl_kv.get_seq_no()));
}

ObTabletDDLKvMgr::ObTabletDDLKvMgr()
  : is_inited_(false),
    ls_id_(), tablet_id_(),
    max_freeze_scn_(SCN::min_scn()),
    head_(0), tail_(0), lock_(), idem_checker_(), ref_cnt_(0)
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
  (void)del_tablet_from_ddl_log_handler(ls_id_, tablet_id_);
  for (int64_t i = 0; i < MAX_DDL_KV_CNT_IN_STORAGE; ++i) {
    ddl_kv_handles_[i].reset();
  }
  ls_id_.reset();
  tablet_id_.reset();
  max_freeze_scn_.set_min();
  idem_checker_.destroy();
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
  } else if (OB_FAIL(add_tablet_to_ddl_log_handler(ls_id, tablet_id))) {
    LOG_WARN("failed to add tablet to ddl log handler", KR(ret), K(ls_id), K(tablet_id));
  }

  if (OB_FAIL(ret)) {
  } else {
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
    if (OB_FAIL(add_idempotence_checker_nolock())) {
      LOG_WARN("failed to init idem checker", K(ret));
    } else {
      ls_id_ = ls_id;
      tablet_id_ = tablet_id;
      is_inited_ = true;
    }

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
    if (OB_ENTRY_NOT_EXIST == ret ||
        OB_TASK_EXPIRED == ret) {
      ret = OB_SUCCESS;
      tablet_mgr = nullptr;
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
  (void)del_tablet_from_ddl_log_handler(ls_id_, tablet_id_);
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

int ObTabletDDLKvMgr::add_idempotence_checker()
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  return add_idempotence_checker_nolock();
}


int ObTabletDDLKvMgr::add_idempotence_checker_nolock()
{
  int ret = OB_SUCCESS;
  if (idem_checker_.is_inited()) {
    /* skip */
  } else if (OB_FAIL(idem_checker_.init())) {
    LOG_WARN("failed to init idem checker", K(ret));
  }
  return ret;
}

int ObTabletDDLKvMgr::calc_idem_block_checksum(const ObDDLMacroBlockType block_type,
                                               const ObDirectLoadType direct_load_type,
                                               const char *buf,
                                               const int64_t buf_size,
                                               int64_t &checksum)
{
  return ObDDLMacroIdemChecker::calc_block_checksum(block_type, direct_load_type, buf, buf_size, checksum);
}
/*
* check macro block already exist in ddl kv
* parameters check logic are set in IdemChker
*/
int ObTabletDDLKvMgr::check_idem_block_exist(const ObDDLMacroBlockType block_type,
                                             const ObDirectLoadType direct_load_type,
                                             const blocksstable::MacroBlockId &macro_block_id,
                                             const blocksstable::ObLogicMacroBlockId &logic_id,
                                             const int64_t checksum,
                                             const ObITable::TableType table_type,
                                             bool &is_marco_block_already_exist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_idempotence_checker())) {
    LOG_WARN("failed to add idempotence checker", K(ret));
  } else if (OB_FAIL(idem_checker_.check_block_exist(block_type, direct_load_type, macro_block_id, logic_id, checksum, table_type, is_marco_block_already_exist))) {
    LOG_WARN("failed to check block exist", K(ret));
  }
  return ret;
}

int ObTabletDDLKvMgr::set_idem_block_checksum(const ObDDLMacroBlockType block_type,
                                              const ObDirectLoadType direct_load_type,
                                              const blocksstable::MacroBlockId &block_id,
                                              const blocksstable::ObLogicMacroBlockId &logic_id,
                                              const int64_t checksum,
                                              const ObITable::TableType table_type)
{
  return idem_checker_.set_block_checksum(block_type, direct_load_type, block_id, logic_id, checksum, table_type);
}

int ObTabletDDLKvMgr::remove_idempotence_checker()
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  idem_checker_.destroy();
  return ret;
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

int ObTabletDDLKvMgr::get_or_create_shared_nothing_ddl_kv(
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
  } else if (OB_UNLIKELY(!macro_redo_scn.is_valid_and_not_min()
                      || !macro_redo_start_scn.is_valid_and_not_min()
                      || !direct_load_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_redo_scn), K(macro_redo_start_scn), K(direct_load_mgr_handle));
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
        direct_load_mgr_handle.get_obj()->get_tenant_data_version(),
        kv_handle,
        ObDDLKVType::DDL_KV_FULL))) {
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

int ObTabletDDLKvMgr::get_or_create_idem_ddl_kv(
    const share::SCN &macro_redo_scn,
    const share::SCN &macro_redo_start_scn,
    const int64_t snapshot_version,
    const uint64_t data_format_version,
    ObDDLKVHandle &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_redo_scn.is_valid_and_not_min()
                      || !macro_redo_start_scn.is_valid_and_not_min()
                      || snapshot_version <= 0
                      || data_format_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_redo_scn), K(macro_redo_start_scn), K(snapshot_version), K(data_format_version));
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
      } else if (OB_FAIL(alloc_ddl_kv(macro_redo_start_scn, snapshot_version, data_format_version, kv_handle, ObDDLKVType::DDL_KV_FULL))) {
        LOG_WARN("create ddl kv failed", K(ret), K(macro_redo_start_scn), K(snapshot_version), K(data_format_version));
      }
    }
    if (lock_tid != 0) {
      unlock(lock_tid);
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_or_create_inc_major_ddl_kv(
    const share::SCN &macro_redo_scn,
    const share::SCN &macro_redo_start_scn,
    const int64_t snapshot_version,
    const uint64_t data_format_version,
    const ObTransID &trans_id,
    const ObTxSEQ &seq_no,
    const ObITable::TableType table_type,
    ObDDLKVHandle &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_redo_scn.is_valid_and_not_min()
                         || !macro_redo_start_scn.is_valid_and_not_min()
                         || snapshot_version <= 0
                         || data_format_version <= 0
                         || !trans_id.is_valid()
                         || !seq_no.is_valid()
                         || !ObITable::is_table_type_valid(table_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_redo_scn), K(macro_redo_start_scn),
        K(snapshot_version), K(data_format_version), K(trans_id), K(seq_no), K(table_type));
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
      } else if (OB_FAIL(alloc_ddl_kv(macro_redo_start_scn,
                                      snapshot_version,
                                      data_format_version,
                                      kv_handle,
                                      ObDDLKVType::DDL_KV_INC_MAJOR,
                                      trans_id,
                                      seq_no,
                                      table_type))) {
        LOG_WARN("create ddl kv failed", K(ret), K(trans_id), K(seq_no), K(table_type));
      }
    }
    if (lock_tid != 0) {
      unlock(lock_tid);
    }
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
    const SCN &freeze_scn,
    const ObDDLKVType ddl_kv_type,
    const transaction::ObTransID &trans_id,
    const transaction::ObTxSEQ &seq_no,
    const ObITable::TableType table_type)
{
  int ret = OB_SUCCESS;
  ObDDLKVHandle kv_handle;
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_full_ddl_kv(ddl_kv_type) && !is_inc_major_ddl_kv(ddl_kv_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support full and inc_major ddl kv", K(ret), K(ddl_kv_type));
  } else if (0 == get_count_nolock()) {
    // do nothing
  } else if (OB_FAIL(get_active_ddl_kv_impl(kv_handle))) {
    LOG_WARN("fail to get active ddl kv", K(ret));
  }
  if (OB_SUCC(ret) && !kv_handle.is_valid() && freeze_scn > max_freeze_scn_) {
    // freeze_scn > 0 only occured when ddl commit
    // assure there is an alive ddl kv, for waiting pre-logs
    if (OB_FAIL(alloc_ddl_kv(start_scn, snapshot_version, data_format_version, kv_handle, ddl_kv_type, trans_id, seq_no, table_type))) {
      LOG_WARN("create ddl kv failed", K(ret), K(start_scn), K(snapshot_version),
          K(data_format_version), K(ddl_kv_type), K(trans_id), K(seq_no), K(table_type));
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
      } else if (!is_inc_major_ddl_kv(ddl_kv_type)) {
        ret = OB_SUCCESS;
      }
    } else {
      max_freeze_scn_ = SCN::max(max_freeze_scn_, kv->get_freeze_scn());
      FLOG_INFO("freeze ddl kv", K(max_freeze_scn_), "kv", *kv);
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::release_ddl_kvs(const ObDDLKVType ddl_kv_type, const SCN &end_scn)
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
      } else if (kv->is_closed() && kv->get_freeze_scn() <= end_scn && kv->get_ddl_kv_type() == ddl_kv_type) {
        const SCN &freeze_scn = kv->get_freeze_scn();
        const uint64_t data_format_version = kv->get_data_format_version();
        free_ddl_kv(idx);
        ++head_;
        LOG_INFO("succeed to release ddl kv", K(ls_id_), K(tablet_id_), K(freeze_scn));

        if ((get_count_nolock() == 0) && (data_format_version >= DATA_VERSION_4_5_0_0)) {
          if (OB_FAIL(del_tablet_from_ddl_log_handler(ls_id_, tablet_id_))) {
            LOG_WARN("failed to del tablet from ddl log handler", KR(ret), K_(ls_id), K_(tablet_id));
          }
        }
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
    // head_ is the oldest ddl kv, so the min scn is the min of the min scn and freeze scn
    const int64_t idx = get_idx(head_);
    ObDDLKV *kv = ddl_kv_handles_[idx].get_obj();
    if (OB_ISNULL(kv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(kv), K(head_), K(tail_));
    } else {
      // may be empty ddl kv, so use the freeze scn as the min scn
      min_scn = SCN::min(kv->get_min_scn(), kv->get_freeze_scn());
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_kvs_unlock(
    const bool frozen_only,
    ObIArray<ObDDLKVHandle> &kv_handle_array,
    const ObDDLKVQueryParam &ddl_kv_query_param)
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
      } else if ((!frozen_only || cur_kv->is_freezed())
                 && ddl_kv_query_param.match_ddl_kv(*cur_kv)) {
        if (OB_FAIL(kv_handle_array.push_back(cur_kv_handle))) {
          LOG_WARN("fail to push back ddl kv", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_kvs(
    const bool frozen_only,
    ObIArray<ObDDLKVHandle> &kv_handle_array,
    const ObDDLKVQueryParam &ddl_kv_query_param)
{
  int ret = OB_SUCCESS;
  kv_handle_array.reset();
  ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DDL_KV_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_FAIL(get_ddl_kvs_unlock(frozen_only,
                                        kv_handle_array,
                                        ddl_kv_query_param))) {
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
  return try_flush_ddl_commit_scn(ls_handle.get_ls(), tablet_handle, direct_load_mgr_handle, commit_scn);
}
int ObTabletDDLKvMgr::try_flush_ddl_commit_scn(
    ObLS *ls,
    const ObTabletHandle &tablet_handle,
    const ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    const share::SCN &commit_scn)
{
  int ret = OB_SUCCESS;
   ObTabletFullDirectLoadMgr *direct_load_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == ls || !tablet_handle.is_valid() || OB_ISNULL(direct_load_mgr = direct_load_mgr_handle.get_full_obj()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(ls), K(tablet_handle));
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
    } else if (OB_FAIL(ls->get_max_decided_scn(max_decided_scn))) {
      LOG_WARN("get max decided log ts failed", K(ret), K(ls->get_ls_id()));
    } else if (SCN::plus(max_decided_scn, 1) >= commit_scn) { // commit_scn elapsed, means the prev clog already replayed or applied
      // max_decided_scn is the left border scn - 1
      // the min deciding(replay or apply) scn (aka left border) is max_decided_scn + 1
      if (OB_FAIL(freeze_ddl_kv(direct_load_mgr->get_start_scn(),
              direct_load_mgr->get_table_key().get_snapshot_version(),
              direct_load_mgr->get_tenant_data_version(),
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
    ObDDLKVHandle &kv_handle,
    const ObDDLKVType ddl_kv_type,
    const transaction::ObTransID &trans_id,
    const transaction::ObTxSEQ &seq_no,
    const ObITable::TableType table_type)
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
  } else if (OB_UNLIKELY(!(storage::is_full_ddl_kv(ddl_kv_type)
                          || storage::is_inc_major_ddl_kv(ddl_kv_type)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support full or inc_major ddl kv", KR(ret), K(ddl_kv_type));
  } else if (OB_FAIL(handle_ddl_kv_queue_overflow(ddl_kv_type))) {
    if (OB_EAGAIN == ret) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) { // 10s
        LOG_INFO("too much ddl kv count, need retry", KR(ret), K(ddl_kv_type));
      }
    } else {
      LOG_WARN("error unexpected, too much ddl kv count", KR(ret), K(ddl_kv_type));
    }
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
                              data_format_version,
                              ddl_kv_type,
                              trans_id,
                              seq_no,
                              table_type))) {
    LOG_WARN("fail to init ddl kv", K(ret), K(ls_id_), K(tablet_id_), K(ddl_kv_type), K(trans_id), K(seq_no), K(table_type));
  } else {
    const int64_t idx = get_idx(tail_);
    tail_++;
    ddl_kv_handles_[idx] = tmp_kv_handle;
    kv_handle = tmp_kv_handle;
    FLOG_INFO("succeed to add ddl kv", K(ls_id_), K(tablet_id_), K(head_), K(tail_), K(max_freeze_scn_), "ddl_kv_cnt", get_count_nolock(), KP(kv));

    if (data_format_version >= DATA_VERSION_4_5_0_0) {
      if (OB_FAIL(add_tablet_to_ddl_log_handler(ls_id_, tablet_id_))) {
        LOG_WARN("failed to add tablet to ddl log handler", KR(ret), K_(ls_id), K_(tablet_id));
      }
    }
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

int ObTabletDDLKvMgr::handle_ddl_kv_queue_overflow(const ObDDLKVType ddl_kv_type)
{
  int ret = OB_SUCCESS;
  int64_t ddl_kv_count = get_count_nolock();
  if (is_inc_major_ddl_kv(ddl_kv_type)) {
    // TODO @yuya.yu There is deadlock here. Need to fix it
    // if (ddl_kv_count >= MAX_DDL_KV_CNT_TO_DUMP) {
    //   int tmp_ret = OB_SUCCESS;
    //   if (OB_TMP_FAIL(ObDDLMergeScheduler::schedule_tablet_ddl_inc_major_merge(ls_id_, tablet_id_))) {
    //     LOG_WARN("failed to schedule tablet ddl inc major merge", KR(tmp_ret), K_(ls_id), K_(tablet_id));
    //   }
    // }
    if (ddl_kv_count == MAX_DDL_KV_CNT_IN_STORAGE) {
      ret = OB_EAGAIN;
    }
  } else {
    if (ddl_kv_count == MAX_DDL_KV_CNT_IN_STORAGE) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::add_tablet_to_ddl_log_handler(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService *);
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("ls service should not be null", KR(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_ddl_log_handler()->add_tablet(tablet_id))) {
    LOG_WARN("failed to add tablet", KR(ret), K(ls_id), K(tablet_id), K(common::lbt()));
  }
  return ret;
}

int ObTabletDDLKvMgr::del_tablet_from_ddl_log_handler(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService *);
  ObSEArray<ObTabletID, 1> tablet_ids;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("ls service should not be null", KR(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    LOG_WARN("failed to push back tablet id", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_ddl_log_handler()->del_tablets(tablet_ids))) {
    LOG_WARN("failed to del tablets", KR(ret), K(ls_id), K(tablet_ids), K(common::lbt()));
  }
  return ret;
}

ObDDLIdemKey::ObDDLIdemKey():
key_(), table_type_(ObITable::TableType::MAX_TABLE_TYPE)
{}

ObDDLIdemKey::~ObDDLIdemKey()
{}

ObDDLIdemKey::ObDDLIdemKey(const ObDDLIdemKey &other)
{
  if (GCTX.is_shared_storage_mode()) {
    key_.macro_block_id_ = other.key_.macro_block_id_;
  } else {
    key_.logic_block_id_ = other.key_.logic_block_id_;
  }
  table_type_ = other.table_type_;
}


int ObDDLIdemKey::init(const MacroBlockId &macro_block_id,
                       const ObLogicMacroBlockId &logic_block_id,
                       const ObITable::TableType table_type)
{
  int ret = OB_SUCCESS;
  table_type_ = table_type;
  if (GCTX.is_shared_storage_mode()) {
    if (!macro_block_id.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro block id", K(ret), K(macro_block_id));
    } else {
      key_.macro_block_id_ = macro_block_id;
    }
  } else {
    if (!logic_block_id.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid logic block id", K(ret), K(logic_block_id));
    } else {
      key_.logic_block_id_ = logic_block_id;
    }
  }
  return ret;
}

uint64_t ObDDLIdemKey::hash() const
{
  uint64_t hash_val = 0;
  uint64_t idem_type = table_type_;
  if (GCTX.is_shared_storage_mode()) {
    hash_val = key_.macro_block_id_.hash();
  } else{
    hash_val = key_.logic_block_id_.hash();
  }
  hash_val = murmurhash(&idem_type, sizeof(table_type_), hash_val);
  return hash_val;
}

int ObDDLIdemKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = hash();
  return ret;
}

bool ObDDLIdemKey::operator==(const ObDDLIdemKey &other) const
{
  bool ret = false;
  if (GCTX.is_shared_storage_mode()) {
    ret = key_.macro_block_id_ == other.key_.macro_block_id_ && table_type_ == other.table_type_;
  } else {
    ret = key_.logic_block_id_ == other.key_.logic_block_id_ && table_type_ == other.table_type_;
  }
  return ret;
}

ObDDLIdemKey& ObDDLIdemKey::operator=(const ObDDLIdemKey &other)
{
  if (GCTX.is_shared_storage_mode()) {
    key_.macro_block_id_ = other.key_.macro_block_id_;
  } else {
    key_.logic_block_id_ = other.key_.logic_block_id_;
  }
  table_type_ = other.table_type_;
  return *this;
}

ObDDLMacroIdemChecker::ObDDLMacroIdemChecker():
 checksum_map_(), allocator_(ObMemAttr(MTL_ID(), "DDL_IDEM_CHECK"))
{}

ObDDLMacroIdemChecker::~ObDDLMacroIdemChecker()
{
  destroy();
}

int ObDDLMacroIdemChecker::init()
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("idem chekcer has already been inited", K(ret));
  } else if (OB_FAIL(checksum_map_.create(997, ObMemAttr(MTL_ID(), "idem_checker")))) {
    LOG_WARN("failed create macro block checksum map", K(ret), K(lbt()));
  }
  return ret;
}

bool ObDDLMacroIdemChecker::is_inited()
{
  return checksum_map_.created();
}

/*
 * only idem type need check idempotence
 * but if it's empty block type,skip it
*/
bool ObDDLMacroIdemChecker::need_check_block_checksum(const ObDDLMacroBlockType block_type, const ObDirectLoadType direct_load_type)
{
  return is_idem_type(direct_load_type) &&
         ObDDLMacroBlockType::DDL_MB_SS_EMPTY_DATA_TYPE != block_type;
}

int ObDDLMacroIdemChecker::calc_block_checksum(const ObDDLMacroBlockType block_type,
                                               const ObDirectLoadType direct_load_type,
                                               const char *buf,
                                               const int64_t buf_size,
                                               int64_t &checksum)
{
  int ret = OB_SUCCESS;
  checksum = 0;
  if (!ObDDLMacroIdemChecker::need_check_block_checksum(block_type, direct_load_type)) {
    checksum = 0;
  } else {
    if (nullptr == buf || buf_size <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid value, buf & buf_size should not be empty", K(ret), K(block_type), K(direct_load_type), KP(buf), K(buf_size));
    } else if (ObDDLMacroBlockType::DDL_MB_DATA_TYPE == block_type || ObDDLMacroBlockType::DDL_MB_INDEX_TYPE == block_type) {
      const ObMacroBlockCommonHeader *common_header = reinterpret_cast<const ObMacroBlockCommonHeader *>(buf);
      if (OB_FAIL(common_header->check_integrity())) {
        LOG_WARN("macro block common header check integrity failed", K(ret), KPC(common_header));
      } else {
        checksum = common_header->get_payload_checksum();
      }
    } else {
      checksum = ob_crc64(buf, buf_size);
    }
  }
  return ret;
}


int ObDDLMacroIdemChecker::check_block_exist(const ObDDLMacroBlockType block_type,
                                             const ObDirectLoadType direct_load_type,
                                             const blocksstable::MacroBlockId &block_id,
                                             const blocksstable::ObLogicMacroBlockId &logic_id,
                                             const int64_t checksum,
                                             ObITable::TableType table_type,
                                             bool &is_marco_block_already_exist)
{
  int ret = OB_SUCCESS;
  ObDDLIdemKey key;
  is_marco_block_already_exist = false;
  if (!ObDDLMacroIdemChecker::need_check_block_checksum(block_type, direct_load_type)) {
    /* skip */
  } else if (OB_FAIL(key.init(block_id, logic_id, table_type))) {
    LOG_WARN("failed to init key value", K(ret), K(block_id), K(logic_id), K(table_type));
  } else if (!checksum_map_.created())  {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("macro block checksum map not created", K(ret), K(checksum_map_.created()));
  } else {
    int64_t prev_checksum = 0;
    if (OB_FAIL(checksum_map_.get_refactored(key, prev_checksum))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get refactored", K(ret), K(block_id), K(logic_id));
      }
    } else if (prev_checksum == checksum) {
      is_marco_block_already_exist = true;
      LOG_INFO("macro block already exist, skip replay it", K(block_id), K(logic_id), K(checksum));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("checksum not match", K(ret), K(block_id), K(logic_id), K(prev_checksum), K(checksum));
    }
  }
  return ret;
}

int ObDDLMacroIdemChecker::set_block_checksum(const ObDDLMacroBlockType block_type,
                                              const ObDirectLoadType direct_load_type,
                                              const blocksstable::MacroBlockId &block_id,
                                              const blocksstable::ObLogicMacroBlockId &logic_id,
                                              const int64_t checksum,
                                              const ObITable::TableType table_type)

{
  int ret = OB_SUCCESS;
  ObDDLIdemKey key;
  bool is_block_exist = false;
  /* check block exist */
  if (!ObDDLMacroIdemChecker::need_check_block_checksum(block_type, direct_load_type)) {
    /* skip */
  } else if (OB_FAIL(check_block_exist(block_type, direct_load_type, block_id, logic_id, checksum, table_type, is_block_exist))) {
    LOG_WARN("failed to check block exist", K(ret), K(block_id), K(logic_id), K(checksum), K(table_type));
  } else if (is_block_exist) {
    LOG_INFO("block already exist, skip set checksum", K(block_id), K(logic_id), K(checksum), K(table_type));
  } else if (OB_FAIL(key.init(block_id, logic_id, table_type))) {
    LOG_WARN("failed to init key value", K(ret), K(block_id), K(logic_id));
  } else if (OB_FAIL(checksum_map_.set_refactored(key, checksum))) {
    LOG_WARN("failed to set refactored", K(ret), K(block_id), K(logic_id), K(checksum));
  }
  return ret;
}
void ObDDLMacroIdemChecker::destroy()
{
  if (is_inited()) {
    checksum_map_.destroy();
  }
}