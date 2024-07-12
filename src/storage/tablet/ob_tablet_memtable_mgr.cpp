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
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_multi_source_data.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ls/ob_freezer.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase
{
using namespace common;
using namespace rootserver;
using namespace blocksstable;
using namespace memtable;
using namespace transaction;
using namespace clog;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace storage
{
using namespace mds;

ObTabletMemtableMgr::ObTabletMemtableMgr()
  : ls_(nullptr),
    lock_def_(common::ObLatchIds::TABLET_MEMTABLE_LOCK),
    retry_times_(0),
    schema_recorder_(),
    medium_info_recorder_()
{
  lock_.lock_type_ = LockType::OB_SPIN_RWLOCK;
  lock_.lock_ = &lock_def_;

#if defined(__x86_64__)
  static_assert(sizeof(ObTabletMemtableMgr) <= 480, "The size of ObTabletMemtableMgr will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
#endif
}

ObTabletMemtableMgr::~ObTabletMemtableMgr()
{
  destroy();
}

void ObTabletMemtableMgr::destroy()
{
  STORAGE_LOG(DEBUG, "destroy tablet memtable mgr", KP(this), KPC(this));
  MemMgrWLockGuard lock_guard(lock_);
  // release memtable
  ObIMemtable *imemtable = nullptr;
  int ret = OB_SUCCESS;
  for (int64_t pos = memtable_head_; pos < memtable_tail_; ++pos) {
    imemtable = tables_[get_memtable_idx(pos)];
    if (OB_ISNULL(imemtable)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "memtable is nullptr", K(ret), KP(imemtable), K(pos));
    } else if (imemtable->is_tablet_memtable()) {
      ObITabletMemtable *memtable = static_cast<ObITabletMemtable *>(imemtable);
      memtable->remove_from_data_checkpoint();
      memtable->set_frozen();
    }
  }

  reset_tables();
  tablet_id_ = 0;
  ls_ = NULL;
  freezer_ = nullptr;
  schema_recorder_.destroy();
  medium_info_recorder_.destroy();
  retry_times_ = 0;
  is_inited_ = false;
}

int ObTabletMemtableMgr::init(const common::ObTabletID &tablet_id,
                              const ObLSID &ls_id,
                              ObFreezer *freezer,
                              ObTenantMetaMemMgr *t3m)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(storage::ObLSService *);
  ObLSHandle ls_handle;
  ObMdsTableMgr *mds_table_mgr = nullptr;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("this has been initialized, not init again", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())
             || OB_ISNULL(t3m)
             || OB_ISNULL(freezer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), KP(freezer), KP(t3m));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id,
                                        ls_handle,
                                        ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(MTL_ID()));
  } else if (OB_ISNULL(ls_ = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ls should not be NULL", K(ret), KP(ls_));
  } else {
    tablet_id_ = tablet_id;
    t3m_ = t3m;
    freezer_ = freezer;
    retry_times_ = 0;
    is_inited_ = true;
    TRANS_LOG(DEBUG, "succeeded to init tablet memtable mgr", K(ret), K(ls_id), K(tablet_id), KP(this), KPC(this));
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObTabletMemtableMgr::init_storage_recorder(
    const ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    const int64_t max_saved_schema_version,
    const int64_t max_saved_medium_scn,
    const lib::Worker::CompatMode compat_mode,
    logservice::ObLogHandler *log_handler)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_recorder_.init(ls_id, tablet_id, max_saved_schema_version, compat_mode, log_handler))) {
    TRANS_LOG(WARN, "failed to init schema recorder", K(ret), K(max_saved_schema_version), KP(log_handler));
  } else if (OB_FAIL(medium_info_recorder_.init(ls_id, tablet_id, max_saved_medium_scn, log_handler))) {
    TRANS_LOG(WARN, "failed to init medium info recorder", K(ret), K(max_saved_medium_scn), KP(log_handler));
  } else {
    TRANS_LOG(INFO, "success to init storage recorder", K(ret), K(ls_id), K(tablet_id), K(max_saved_schema_version),
      K(max_saved_medium_scn), K(compat_mode));
  }
  return ret;
}

int ObTabletMemtableMgr::try_resolve_boundary_on_create_memtable_for_leader_(
    ObITabletMemtable *last_frozen_tablet_memtable,
    ObITabletMemtable *new_tablet_memtable)
{
  int ret = OB_SUCCESS;
  bool double_check = false;
  bool can_resolve = false;
  int64_t write_ref = 0;
  int64_t unsubmitted_cnt = 0;
  do {
    write_ref = last_frozen_tablet_memtable->get_write_ref();
    unsubmitted_cnt = last_frozen_tablet_memtable->get_unsubmitted_cnt();
    if (0 == write_ref && 0 == unsubmitted_cnt) {
      share::SCN max_decided_scn;
      if (OB_FAIL(freezer_->get_max_consequent_callbacked_scn(max_decided_scn))) {
        TRANS_LOG(WARN, "get max decided scn fail", K(ret), K(freezer_->get_ls_id()));
      } else if (max_decided_scn >= last_frozen_tablet_memtable->get_end_scn()) {
        // logstream's continous apply has pass frozen memtable's right boundary
        can_resolve = true;
      }
    }
    if (!can_resolve && !double_check) {
      last_frozen_tablet_memtable->clear_resolved_active_memtable_left_boundary();
    }
    double_check = !double_check;
  } while (!can_resolve && double_check);

  if (write_ref > 0) {
    // NB: for the leader, if the write ref on the frozen memtable is greater
    // than 0, we cannot create a new memtable. Otherwise we may finish the
    // write on the new memtable before finishing the write on the frozen
    // memtable and cause the writes and callbacks on memtable_ctx out of order.
    ret = OB_EAGAIN;
    TRANS_LOG(INFO,
              "last frozen's write flag is not 0 during create new memtable",
              KPC(last_frozen_tablet_memtable),
              KPC(new_tablet_memtable));
  } else if (can_resolve) {
    SCN new_start_scn;
    last_frozen_tablet_memtable->resolve_right_boundary();
    last_frozen_tablet_memtable->set_resolved_active_memtable_left_boundary();
    if (new_tablet_memtable != last_frozen_tablet_memtable) {
      new_start_scn = MAX(last_frozen_tablet_memtable->get_end_scn(),
                                     last_frozen_tablet_memtable->get_migration_clog_checkpoint_scn());
      int tmp_ret = new_tablet_memtable->resolve_left_boundary(new_start_scn);
      if (OB_SUCCESS != tmp_ret) {
        TRANS_LOG(ERROR, "resolve left boundary failed", KR(tmp_ret), K(new_start_scn), KPC(new_tablet_memtable));
      }
    }
    TRANS_LOG(INFO,
              "[resolve_right_boundary] in create_memtable on leader",
              K(new_start_scn),
              KPC(last_frozen_tablet_memtable),
              KPC(new_tablet_memtable));
  } else if (unsubmitted_cnt > 0) {
    new_tablet_memtable->set_logging_blocked();
    TRANS_LOG(INFO, "set new memtable logging blocked", KPC(last_frozen_tablet_memtable), KPC(new_tablet_memtable));
  }
  return ret;
}

// There are two cases:
// 1. create the first memtable for tablet
// 2. create the new memtable after freezing the old memtable
int ObTabletMemtableMgr::create_memtable(const CreateMemtableArg &arg)
{
  // Write lock
  ObTimeGuard time_guard("ObTabletMemtableMgr::create_memtable", 10 * 1000);
  MemMgrWLockGuard lock_guard(lock_);
  time_guard.click("lock");

  int ret = OB_SUCCESS;
  ObLSID ls_id;
  const uint32_t logstream_freeze_clock = freezer_->get_freeze_clock();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
  } else if (FALSE_IT(ls_id = ls_->get_ls_id())) {
  } else if (has_memtable_() && OB_FAIL(check_boundary_memtable_(logstream_freeze_clock))) {
    STORAGE_LOG(DEBUG, "check boundary memtable failed", KR(ret), K(arg), K(ls_id));
  } else if (get_memtable_count_() >= MAX_MEMSTORE_CNT) {
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    ob_usleep(1 * 1000);
    if ((++retry_times_ % (60 * 1000)) == 0) {  // 1 min
      ObTableHandleV2 first_frozen_memtable;
      get_first_frozen_memtable_(first_frozen_memtable);
      LOG_ERROR("cannot create more memtable",
                K(ret),
                K(ls_id),
                K(tablet_id_),
                K(MAX_MEMSTORE_CNT),
                K(get_memtable_count_()),
                KPC(first_frozen_memtable.get_table()));
    }
  } else if (arg.for_replay_ && arg.clog_checkpoint_scn_ != arg.new_clog_checkpoint_scn_) {
    ret = OB_EAGAIN;
    LOG_INFO("clog_checkpoint_scn changed, need retry to replay", K(ls_id), K(tablet_id_), K(arg));
  } else if (MAX_MEMSTORE_CNT - 1 == get_memtable_count_() && arg.for_inc_direct_load_) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1LL * 1000LL * 1000LL)) {
      STORAGE_LOG(INFO, "only data memtable can use the last slot in memtable mgr", K(ret), K(arg), KPC(this));
    }
  } else if (OB_FAIL(create_memtable_(arg, logstream_freeze_clock, time_guard))) {
    STORAGE_LOG(WARN, "create memtable failed", KR(ret), K(ls_id));
  }

  return ret;
}

int ObTabletMemtableMgr::check_boundary_memtable_(const uint32_t logstream_freeze_clock)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  ObITabletMemtable *tablet_memtable = nullptr;
  if (OB_FAIL(get_boundary_memtable_(table_handle))) {
    STORAGE_LOG(WARN, "get boundary memtable failed", KR(ret), K(table_handle));
  } else if (OB_FAIL(table_handle.get_tablet_memtable(tablet_memtable))) {
    STORAGE_LOG(WARN, "get boundary memtable failed", KR(ret), K(table_handle));
  } else if (tablet_memtable->is_direct_load_memtable()) {
    if (tablet_memtable->get_end_scn().is_max()) {
      PAUSE();
      // if end_scn of direct load memtable has not decided, return OB_ENTRY_EXIST and Tablet will reset it to
      // OB_SUCCESS. Then refresh_and_protect_table(refresh_and_protect_memtable) in StorageTableGuard will retry create
      // memtable
      ret = OB_ENTRY_EXIST;
    }
  } else if (tablet_memtable->is_data_memtable()) {
    if (tablet_memtable->is_active_memtable() && tablet_memtable->get_freeze_clock() == logstream_freeze_clock) {
      ret = OB_ENTRY_EXIST;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "Invalid table type", KR(ret), KPC(tablet_memtable));
  }

  if (OB_FAIL(ret)) {
    STORAGE_LOG(DEBUG, "check boundary memtable failed", KR(ret), K(logstream_freeze_clock), KPC(tablet_memtable));
  }
  return ret;
}

int ObTabletMemtableMgr::create_memtable_(const CreateMemtableArg &arg,
                                          const uint32_t logstream_freeze_clock,
                                          ObTimeGuard &tg)
{
  int ret = OB_SUCCESS;

  ObTableHandleV2 memtable_handle;
  ObITable::TableKey table_key;
  const ObLSID ls_id = ls_->get_ls_id();
  table_key.table_type_ = arg.for_inc_direct_load_ ? ObITable::DIRECT_LOAD_MEMTABLE : ObITable::DATA_MEMTABLE;
  table_key.tablet_id_ = tablet_id_;
  table_key.scn_range_.start_scn_ = arg.clog_checkpoint_scn_;
  table_key.scn_range_.end_scn_.set_max();
  ObITabletMemtable *new_tablet_memtable = NULL;
  ObLSHandle ls_handle;
  retry_times_ = 0;

  if (OB_FAIL(acquire_tablet_memtable_(arg.for_inc_direct_load_, memtable_handle))) {
    LOG_WARN("failed to create memtable", K(ret), K(ls_id), K(tablet_id_));
  } else if (FALSE_IT(tg.click("acquire_memtable"))) {
  } else if (OB_ISNULL(new_tablet_memtable = static_cast<ObITabletMemtable *>(memtable_handle.get_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get memtable", K(ret), K(ls_id), K(tablet_id_), K(memtable_handle));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DATA_MEMTABLE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id), K(tablet_id_));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, invalid ls handle", K(ret), K(ls_handle), K(ls_id), K(tablet_id_));
  } else if (OB_FAIL(new_tablet_memtable->init(
                 table_key, ls_handle, freezer_, this, arg.schema_version_, logstream_freeze_clock))) {
    LOG_WARN("failed to init memtable",
             K(ret),
             K(ls_id),
             K(table_key),
             KP(freezer_),
             KP(this),
             K(arg),
             K(logstream_freeze_clock));
  } else if (OB_FAIL(resolve_boundary_(new_tablet_memtable, arg))) {
    LOG_WARN("failed to add memtable", K(ret), K(ls_id), K(tablet_id_), K(memtable_handle));
  } else if (FALSE_IT(tg.click("init memtable"))) {
  } else if (FALSE_IT(block_freeze_if_memstore_full_(new_tablet_memtable))) {
  } else if (OB_FAIL(add_memtable_(memtable_handle))) {
    LOG_WARN("failed to add memtable", K(ret), K(ls_id), K(tablet_id_), K(memtable_handle));
  } else if (FALSE_IT(tg.click("add memtable"))) {
  } else if (OB_FAIL(new_tablet_memtable->add_to_data_checkpoint(freezer_->get_ls_data_checkpoint()))) {
    LOG_WARN("add to data_checkpoint failed", K(ret), K(ls_id), KPC(new_tablet_memtable));
    clean_tail_memtable_();
  } else if (FALSE_IT(tg.click("add to data_checkpoint"))) {
  } else {
    LOG_INFO("succeed to create memtable", K(arg), K(ret), K(ls_id), KPC(new_tablet_memtable), KPC(this));
  }

  return ret;
}

int ObTabletMemtableMgr::resolve_boundary_(ObITabletMemtable *new_tablet_memtable, const CreateMemtableArg &arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTableHandleV2 last_frozen_memtable_handle;
  ObITabletMemtable *last_frozen_tablet_memtable = nullptr;
  // step 1 : get frozen memtable if exist
  if (OB_FAIL(get_last_frozen_memtable_(last_frozen_memtable_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get last frozen memtable", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(last_frozen_memtable_handle.get_tablet_memtable(last_frozen_tablet_memtable))) {
    LOG_WARN("fail to get memtable", K(ret));
  }

  // step 2 : if frozen memtable exist, try deciding end_scn for frozen memtable
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(last_frozen_tablet_memtable)) {
    if (last_frozen_tablet_memtable->is_data_memtable()) {
      (void)resolve_data_memtable_boundary_(last_frozen_tablet_memtable, new_tablet_memtable, arg);
    } else if (last_frozen_tablet_memtable->is_direct_load_memtable()) {
      (void)resolve_direct_load_memtable_boundary_(last_frozen_tablet_memtable, new_tablet_memtable, arg);
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "Invalid table type", KR(ret), KPC(last_frozen_tablet_memtable));
    }
  } else {
    // use new clog_checkpoint_scn as start_scn for new memtable
    (void)new_tablet_memtable->resolve_left_boundary(arg.new_clog_checkpoint_scn_);
  }

  return ret;
}

void ObTabletMemtableMgr::resolve_data_memtable_boundary_(ObITabletMemtable *frozen_tablet_memtable,
                                                          ObITabletMemtable *new_tablet_memtable,
                                                          const CreateMemtableArg &arg)
{
  int ret = OB_SUCCESS;
  if (arg.for_replay_) {
    SCN new_memtable_start_scn;
    frozen_tablet_memtable->resolve_right_boundary();
    if (new_tablet_memtable != frozen_tablet_memtable) {
      new_memtable_start_scn =
          MAX(frozen_tablet_memtable->get_end_scn(), frozen_tablet_memtable->get_migration_clog_checkpoint_scn());
      (void)new_tablet_memtable->resolve_left_boundary(new_memtable_start_scn);
    }
    TRANS_LOG(INFO, "[resolve_right_boundary] in create_memtable on replay", KPC(frozen_tablet_memtable));
  }
  // for leader, decide the right boundary of frozen memtable
  else if (OB_FAIL(try_resolve_boundary_on_create_memtable_for_leader_(frozen_tablet_memtable, new_tablet_memtable))) {
    TRANS_LOG(WARN, "try resolve boundary fail", K(ret));
  }
}

void ObTabletMemtableMgr::resolve_direct_load_memtable_boundary_(ObITabletMemtable *frozen_tablet_memtable,
                                                                 ObITabletMemtable *active_tablet_memtable,
                                                                 const CreateMemtableArg &arg)
{
  int64_t start_ts = ObClockGenerator::getClock();

  SCN new_memtable_start_scn;
  if (frozen_tablet_memtable->get_end_scn().is_max()) {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "frozen direct load memtable must have a valid end_scn", KPC(frozen_tablet_memtable));
  } else if (active_tablet_memtable != frozen_tablet_memtable) {
    new_memtable_start_scn =
        MAX(frozen_tablet_memtable->get_end_scn(), frozen_tablet_memtable->get_migration_clog_checkpoint_scn());
    (void)active_tablet_memtable->resolve_left_boundary(new_memtable_start_scn);
    (void)frozen_tablet_memtable->set_resolved_active_memtable_left_boundary();
  }
}

void ObTabletMemtableMgr::block_freeze_if_memstore_full_(ObITabletMemtable *new_tablet_memtable)
{
  if (MAX_MEMSTORE_CNT - 1 == get_memtable_count_()) {
    // if the new memtable is last one in memtable_array
    // not allow it to be freezed
    // otherwise the number of memtables will be out of limit
    new_tablet_memtable->set_allow_freeze(false);
    FLOG_INFO(
        "not allow memtable to be freezed", K(get_memtable_count_()), K(MAX_MEMSTORE_CNT), KPC(new_tablet_memtable));
  }
}

bool ObTabletMemtableMgr::has_active_memtable()
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    MemMgrRLockGuard lock_guard(lock_);
    if (NULL != get_active_memtable_()) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObTabletMemtableMgr::get_boundary_memtable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);
  handle.reset();
  ret = get_boundary_memtable_(handle);
  return ret;
}

int ObTabletMemtableMgr::get_boundary_memtable_(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  ObITabletMemtable *tablet_memtable = nullptr;
  if (OB_UNLIKELY(get_memtable_count_() == 0)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (memtable_tail_ > memtable_head_) {
    if (OB_FAIL(get_ith_memtable(memtable_tail_ - 1, handle))) {
      STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(memtable_tail_));
    } else if (OB_FAIL(handle.get_tablet_memtable(tablet_memtable))) {
      handle.reset();
      LOG_WARN("there is no boundary memtable", K(ret));
    }
  }
  return ret;
}


int ObTabletMemtableMgr::get_active_memtable(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_active_memtable_(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get active memtable", K(ret));
    }
  }
  return ret;
}

int ObTabletMemtableMgr::get_active_memtable_(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(get_memtable_count_() == 0)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("no memtable exists", K(ret));
  } else if (memtable_tail_ > memtable_head_) {
    ObITabletMemtable *tablet_memtable = nullptr;
    if (OB_FAIL(get_ith_memtable(memtable_tail_ - 1, handle))) {
      STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(memtable_tail_));
    } else if (OB_FAIL(handle.get_tablet_memtable(tablet_memtable))) {
      LOG_WARN("fail to get memtable", K(ret));
    } else if (!tablet_memtable->is_active_memtable()) {
      handle.reset();
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObTabletMemtableMgr::get_last_frozen_memtable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_last_frozen_memtable_(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get last frozen memtable", K(ret));
    }
  }

  return ret;
}

int ObTabletMemtableMgr::get_last_frozen_memtable_(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();

  if (OB_UNLIKELY(get_memtable_count_() == 0)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (memtable_tail_ > memtable_head_) {
    for (int64_t i = memtable_tail_ - 1; OB_SUCC(ret) && i >= memtable_head_; --i) {
      ObTableHandleV2 m_handle;
      ObITabletMemtable *tablet_memtable = nullptr;
      if (OB_FAIL(get_ith_memtable(i, m_handle))) {
        STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(i));
      } else if (OB_UNLIKELY(!m_handle.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("memtable handle is invalid", K(ret), K(m_handle));
      } else if (OB_FAIL(m_handle.get_tablet_memtable(tablet_memtable))) {
        LOG_WARN("fail to get memtable", K(ret), K(m_handle));
      } else if (OB_ISNULL(tablet_memtable)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("memtable must not null", K(ret), K(m_handle));
      } else if (tablet_memtable->is_frozen_memtable()) {
        handle = m_handle;
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!handle.is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObTabletMemtableMgr::resolve_left_boundary_for_active_memtable(ObITabletMemtable *tablet_memtable,
                                                                   const SCN start_scn)
{
  ObTableHandleV2 handle;
  ObITabletMemtable *active_tablet_memtable = nullptr;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_active_memtable(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get active memtable", K(ret));
    }
  } else if (OB_FAIL(handle.get_tablet_memtable(active_tablet_memtable))) {
    LOG_WARN("fail to get active memtable", K(ret));
  } else {
    // set the start_scn of the new memtable
    int tmp_ret = active_tablet_memtable->resolve_left_boundary(start_scn);
    if (OB_SUCCESS != tmp_ret) {
      TRANS_LOG(ERROR, "resolve left boundary failed", K(start_scn), KPC(active_tablet_memtable), KPC(tablet_memtable));
    }
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    tablet_memtable->set_resolved_active_memtable_left_boundary();
  }

  return ret;
}

int ObTabletMemtableMgr::unset_logging_blocked_for_active_memtable(ObITabletMemtable *tablet_memtable)
{
  ObTableHandleV2 handle;
  ObITabletMemtable *active_tablet_memtable = nullptr;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_active_memtable(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get active memtable", K(ret));
    }
  } else if (OB_FAIL(handle.get_tablet_memtable(active_tablet_memtable))) {
    LOG_WARN("fail to get active memtable", K(ret));
  } else {
    // allow the new memtable to submit log
    active_tablet_memtable->clear_logging_blocked();
  }
  if (OB_ENTRY_NOT_EXIST== ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    tablet_memtable->set_unset_active_memtable_logging_blocked();
  }

  return ret;
}

int ObTabletMemtableMgr::set_is_tablet_freeze_for_active_memtable(
    ObTableHandleV2 &handle,
    const int64_t trace_id)
{
  handle.reset();
  ObITabletMemtable *active_tablet_memtable = nullptr;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_active_memtable(handle))) {
    LOG_DEBUG("fail to get active memtable", K(ret));
  } else if (OB_FAIL(handle.get_tablet_memtable(active_tablet_memtable))) {
    LOG_WARN("fail to get active memtable", K(ret));
    if (ret == OB_NOT_INIT) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("active memtable is null", K(ret));
    }
  } else if (active_tablet_memtable->allow_freeze()) {
    active_tablet_memtable->set_is_tablet_freeze();
    if (checkpoint::INVALID_TRACE_ID != trace_id) {
      active_tablet_memtable->set_trace_id(trace_id);
    }
  } else {
    handle.reset();
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    TRANS_LOG(INFO, "not set is_tablet_freeze because the memtable cannot be freezed", KPC(active_tablet_memtable));
  }

  return ret;
}

int ObTabletMemtableMgr::get_memtable_for_replay(const SCN &replay_scn, ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  ObITabletMemtable *tablet_memtable = nullptr;
  handle.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret));
  } else {
    const share::ObLSID &ls_id = ls_->get_ls_id();
    MemMgrRLockGuard lock_guard(lock_);
    int64_t i = 0;
    for (i = memtable_tail_ - 1; OB_SUCC(ret) && i >= memtable_head_; --i) {
      if (OB_FAIL(get_ith_memtable(i, handle))) {
        STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(i));
      } else if (OB_FAIL(handle.get_tablet_memtable(tablet_memtable))) {
        handle.reset();
        LOG_WARN("fail to get data memtable", K(ret));
      } else {
        if (replay_scn > tablet_memtable->get_start_scn() && replay_scn <= tablet_memtable->get_end_scn()) {
          break;
        } else {
          handle.reset();
        }
      }
    }

    if (OB_SUCC(ret) && !handle.is_valid() && i < memtable_head_) {
      SCN clog_checkpoint_scn;
      if (OB_FAIL(get_newest_clog_checkpoint_scn(clog_checkpoint_scn))) {
      } else if (replay_scn <= clog_checkpoint_scn) {
        // no need to replay the log
        ret = OB_NO_NEED_UPDATE;
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("fail to get memtable for replay", K(ret), K(ls_id), K(tablet_id_), K(replay_scn), K(clog_checkpoint_scn), K(memtable_tail_ - memtable_head_));
      }
    }
  }

  return ret;
}

int ObTabletMemtableMgr::get_memtables_nolock(ObTableHdlArray &handle)
{
  int ret = OB_SUCCESS;
  const int64_t start_point = -1;
  const bool include_active_memtable = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_memtables_(handle, start_point, include_active_memtable))) {
    LOG_WARN("fail to get memtables", K(ret), K(start_point), K(include_active_memtable));
  }
  return ret;
}

int ObTabletMemtableMgr::get_all_memtables(ObTableHdlArray &handle)
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_memtables_nolock(handle))) {
    LOG_WARN("failed to get all memtables", K(ret));
  }
  return ret;
}

DEF_REPORT_CHEKCPOINT_DIAGNOSE_INFO(UpdateReleaseTime, update_release_time)
int ObTabletMemtableMgr::release_head_memtable_(ObIMemtable *imemtable,
                                                const bool force)
{
  int ret = OB_SUCCESS;
  ObITabletMemtable *memtable = static_cast<ObITabletMemtable *>(imemtable);

  if (OB_UNLIKELY(get_memtable_count_() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret));
  } else {
    const share::ObLSID &ls_id = ls_->get_ls_id();
    const int64_t idx = get_memtable_idx(memtable_head_);
    int64_t occupy_size = 0;
    if (nullptr != tables_[idx] && memtable == tables_[idx]) {
      ObMtStat& mt_stat = memtable->get_mt_stat();
      occupy_size = memtable->get_occupied_size();
      if (0 == mt_stat.release_time_) {
        mt_stat.release_time_ = ObTimeUtility::current_time();
      } else {
        LOG_WARN("cannot set release_time twice", K(ls_id), KPC(memtable));
      }
      if (!memtable->is_empty()) {
        memtable->set_read_barrier();
      }
      memtable->remove_from_data_checkpoint();
      memtable->set_is_flushed();
      if (force) {
        memtable->set_freeze_state(TabletMemtableFreezeState::FORCE_RELEASED);
      } else {
        memtable->set_freeze_state(TabletMemtableFreezeState::RELEASED);
      }
      memtable->set_frozen();
      memtable->report_memtable_diagnose_info(UpdateReleaseTime());
      release_head_memtable();
      ObITabletMemtable *active_memtable = get_active_memtable_();
      if (OB_NOT_NULL(active_memtable) && !active_memtable->allow_freeze()) {
        active_memtable->set_allow_freeze(true);
        FLOG_INFO("allow active memtable to be freezed", K(ls_id), KPC(active_memtable));
      }

      FLOG_INFO("succeed to release head data memtable", K(ret), K(occupy_size), KPC(memtable));
    }
  }

  return ret;
}

int ObTabletMemtableMgr::get_first_frozen_memtable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_first_frozen_memtable_(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get first frozen memtable", K(ret));
    }
  }

  return ret;
}

ObITabletMemtable *ObTabletMemtableMgr::get_active_memtable_()
{
  int ret = OB_SUCCESS;
  ObITabletMemtable *memtable = nullptr;
  if (memtable_tail_ > memtable_head_) {
    memtable = static_cast<ObITabletMemtable *>(tables_[get_memtable_idx(memtable_tail_ - 1)]);
    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "memtable is nullptr", K(ret), KP(memtable), K(memtable_tail_));
    } else if (!memtable->is_active_memtable()) {
      memtable = NULL;
    }
  }

  return memtable;
}

ObITabletMemtable *ObTabletMemtableMgr::get_memtable_(const int64_t pos) const
{
  int ret = OB_SUCCESS;
  ObITabletMemtable *memtable = nullptr;
  ObITabletMemtable *table = static_cast<ObITabletMemtable *>(tables_[get_memtable_idx(pos)]);
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("table is nullptr", K(ret), KP(table), K(pos));
  } else if (!table->is_tablet_memtable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not data memtable", K(ret), K(pos), K(table->get_key()));
  } else {
    memtable = table;
  }
  return memtable;
}

void ObTabletMemtableMgr::clean_tail_memtable_()
{
  if (memtable_tail_ > memtable_head_) {
    ObITabletMemtable *memtable = get_memtable_(memtable_tail_ - 1);
    if (OB_NOT_NULL(memtable)) {
      memtable->set_frozen();
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "memtable is null when clean_tail_memtable_", KPC(this));
    }
    ObIMemtableMgr::release_tail_memtable();
  }
}

int ObTabletMemtableMgr::get_memtables_(ObTableHdlArray &handle, const int64_t start_point,
    const bool include_active_memtable)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = memtable_head_;
  if (-1 != start_point) {
    if (OB_FAIL(find_start_pos_(start_point, start_pos))) {
      LOG_WARN("failed to find_start_pos_", K(ret), K(start_point));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(add_tables_(start_pos, include_active_memtable, handle))) {
    LOG_WARN("failed to add tables", K(ret), K(start_point), K(include_active_memtable));
  }
  return ret;
}

int ObTabletMemtableMgr::add_tables_(
    const int64_t start_pos,
    const bool include_active_memtable,
    ObTableHdlArray &handle)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(start_pos > -1)) {
    const int64_t last_pos = memtable_tail_ - 1;
    for (int64_t pos = start_pos; OB_SUCC(ret) && pos < last_pos; ++pos) {
      ObTableHandleV2 memtable_handle;
      if (OB_FAIL(get_ith_memtable(pos, memtable_handle))) {
        STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(pos));
      } else if (OB_UNLIKELY(!memtable_handle.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid memtable handle", K(ret), K(memtable_handle));
      } else if (OB_FAIL(handle.push_back(memtable_handle))) {
        LOG_WARN("failed to add memtable", K(ret), K(memtable_handle));
      }
    }
    if (OB_SUCC(ret) && memtable_tail_ > memtable_head_) {
      ObTableHandleV2 last_memtable_handle;
      if (OB_FAIL(get_ith_memtable(last_pos, last_memtable_handle))) {
        STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(last_pos));
      } else if (OB_UNLIKELY(!last_memtable_handle.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid memtable handle", K(ret), K(last_memtable_handle));
      } else if (include_active_memtable || last_memtable_handle.get_table()->is_frozen_memtable()) {
        if (OB_FAIL(handle.push_back(last_memtable_handle))) {
          LOG_WARN("failed to add last memtable to handle", K(ret), K(start_pos),
                   K(include_active_memtable), K(last_memtable_handle));
        }
      }
    }
  }
  return ret;
}

int ObTabletMemtableMgr::find_start_pos_(const int64_t start_point, int64_t &start_pos)
{
  int ret = OB_SUCCESS;
  start_pos = -1;
  if (OB_UNLIKELY(start_point < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_point", K(ret), K(start_point));
  }
  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
    ObITabletMemtable *memtable = get_memtable_(i);
    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable must not null", K(ret));
    } else if (memtable->get_snapshot_version() > start_point) {
      start_pos = i;
      break;
    }
  }
  return ret;
}

int64_t ObTabletMemtableMgr::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_NAME("ObIMemtableMgr");
    J_COLON();
    pos += ObIMemtableMgr::to_string(buf + pos, buf_len - pos);
    J_COMMA();
    J_KV(K_(schema_recorder));
    J_OBJ_END();
  }
  return pos;
}

int ObTabletMemtableMgr::get_first_frozen_memtable_(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;

  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; i++) {
    ObTableHandleV2 m_handle;
    ObITabletMemtable *memtable = nullptr;
    if (OB_FAIL(get_ith_memtable(i, m_handle))) {
      STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(i));
    } else if (OB_UNLIKELY(!m_handle.is_valid())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable handle is invalid", K(ret), K(m_handle));
    } else if (OB_FAIL(m_handle.get_tablet_memtable(memtable))) {
      LOG_WARN("fail to get memtable", K(ret), K(m_handle));
    } else if (OB_ISNULL(memtable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memtable is nullptr", K(ret), K(m_handle));
    } else if (memtable->is_frozen_memtable()) {
      handle = m_handle;
      break;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!handle.is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObTabletMemtableMgr::set_frozen_for_all_memtables()
{
  int ret = OB_SUCCESS;
  MemMgrWLockGuard lock_guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret));
  } else {
    const share::ObLSID &ls_id = ls_->get_ls_id();
    for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
      // memtable that cannot be released will block memtables behind it
      ObITabletMemtable *memtable = static_cast<ObITabletMemtable *>(tables_[get_memtable_idx(i)]);
      if (OB_ISNULL(memtable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "memtable is nullptr", K(ret), K(ls_id), KP(memtable), K(i));
      } else {
        STORAGE_LOG(INFO, "set frozen for offline", K(ls_id), K(i), KPC(memtable));
        memtable->set_offlined();
        memtable->set_frozen();
      }
    }
  }
  return ret;
}

int ObTabletMemtableMgr::acquire_tablet_memtable_(const bool for_inc_direct_load, ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  if (for_inc_direct_load) {
    ret = t3m_->acquire_direct_load_memtable(handle);
  } else {
    ret = t3m_->acquire_data_memtable(handle);
  }
  return ret;
}

int ObTabletMemtableMgr::freeze_direct_load_memtable(ObITabletMemtable *tablet_memtable)
{
  int ret = OB_SUCCESS;
  MemMgrWLockGuard lock_guard(lock_);
  ObTableHandleV2 boundary_memtable_handle;
  ObITabletMemtable *boundary_memtable = nullptr;
  if (OB_FAIL(get_boundary_memtable_(boundary_memtable_handle))) {
    STORAGE_LOG(WARN, "get boundary memtable failed", KR(ret));
  } else if (!boundary_memtable_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "last memtable handle is unexpected invalid", KR(ret), K(boundary_memtable_handle));
  } else if (OB_FAIL(boundary_memtable_handle.get_tablet_memtable(boundary_memtable))) {
    STORAGE_LOG(WARN, "get active tablet memtable failed", KR(ret), K(boundary_memtable_handle));
  } else if (OB_ISNULL(tablet_memtable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tablet memtable is unexpected null", KR(ret));
  } else if (tablet_memtable != boundary_memtable || !(tablet_memtable->get_end_scn().is_max())) {
    STORAGE_LOG(INFO, "this direct load memtable already freezed", KPC(tablet_memtable), KPC(boundary_memtable));
  } else if (!tablet_memtable->is_direct_load_memtable()) {
    STORAGE_LOG(WARN, "not direct load memtable", KR(ret), KPC(tablet_memtable));
  } else if (!tablet_memtable->allow_freeze()) {
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    STORAGE_LOG(WARN,
                "active direct load memtable is not allowd freeze",
                K(get_memtable_count_()),
                K(tables_),
                KPC(tablet_memtable));
  } else {
    const int64_t FREEZE_DIRECT_LOAD_MEMTABLE_WARN_INTERVAL = 100LL * 1000LL;
    int64_t start_ts = ObClockGenerator::getClock();
    while (tablet_memtable->get_write_ref() > 0) {
      // waiting for all write operation done.
      if (TC_REACH_TIME_INTERVAL(FREEZE_DIRECT_LOAD_MEMTABLE_WARN_INTERVAL)) {
        int64_t freeze_wait_time_ms = (ObClockGenerator::getClock() - start_ts) / 1000;
        STORAGE_LOG_RET(WARN,
                        OB_ERR_TOO_MUCH_TIME,
                        "freeze direct load memtable cost too much time. has wait for(ms) : ",
                        K(freeze_wait_time_ms),
                        KPC(tablet_memtable));
      }
      PAUSE();
    }
    (void)tablet_memtable->resolve_right_boundary();
    (void)tablet_memtable->set_freeze_state(TabletMemtableFreezeState::FREEZING);
    (void)tablet_memtable->set_frozen_time(ObClockGenerator::getClock());

    STORAGE_LOG(INFO, "finish freeze direct load memtable", KP(this), KPC(tablet_memtable));
  }

  return ret;
}


int ObTabletMemtableMgr::get_direct_load_memtables_for_write(ObTableHdlArray &handles)
{
  int ret  = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);

  int64_t last_data_memtable_pos = 0;
  if (OB_FAIL(find_last_data_memtable_pos_(last_data_memtable_pos))) {
    LOG_WARN("find last data memtable pos failed", KR(ret), KP(this));
  } else if (OB_FAIL(add_tables_(last_data_memtable_pos + 1, true/*include_active_memtable*/, handles))) {
    LOG_WARN("add tables failed", KR(ret), KP(this));
  } else {
    ObITabletMemtable *memtable = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < handles.count(); i++) {
      if (OB_ISNULL(memtable = static_cast<ObITabletMemtable*>(handles.at(i).get_table()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected nullptr", KR(ret), KP(this), KP(memtable));
      } else if (!memtable->is_direct_load_memtable()) {
        if (1 == handles.count()) {
          // The active memtable is data memtable, reset it
          handles.reset();
        } else {
          LOG_ERROR("invalid memtable", KR(ret), K(i), K(handles), KPC(memtable));
        }
      } else {
        memtable->inc_write_ref();
      }
    }
  }

  return ret;
}

int ObTabletMemtableMgr::find_last_data_memtable_pos_(int64_t &last_pos)
{
  int ret = OB_SUCCESS;
  last_pos = memtable_head_ - 1;
  for (int64_t i = memtable_tail_-1; OB_SUCC(ret) && i >= memtable_head_; i--) {
    ObITabletMemtable *memtable = get_memtable_(i);
    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable must not null", K(ret));
    } else if (memtable->is_data_memtable()) {
      last_pos = i;
      break;
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
