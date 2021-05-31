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
#include "storage/ob_pg_memtable_mgr.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/ob_partition_key.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_partition_service.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ob_table_mgr.h"

namespace oceanbase {
using namespace common;
using namespace rootserver;
using namespace blocksstable;
using namespace memtable;
using namespace transaction;
using namespace clog;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace storage {

ObPGMemtableMgr::ObPGMemtableMgr() : memtable_head_(0), memtable_tail_(0), last_print_readable_info_ts_(0)
{
  MEMSET(memtables_, 0, sizeof(memtables_));
}

void ObPGMemtableMgr::reset()
{
  memtable_head_ = 0;
  memtable_tail_ = 0;
  MEMSET(memtables_, 0, sizeof(memtables_));
  new_active_memstore_handle_.reset();
  last_print_readable_info_ts_ = 0;
}

void ObPGMemtableMgr::destroy()
{
  TCWLockGuard lock_guard(lock_);
  // release memtable
  for (int64_t pos = memtable_head_; pos < memtable_tail_; ++pos) {
    if (OB_ISNULL(memtables_[get_memtable_idx_(pos)])) {
      STORAGE_LOG(ERROR, "memtable must not null", K(pos), K(get_memtable_idx_(pos)));
    } else {
      ObTableMgr::get_instance().release_table(memtables_[get_memtable_idx_(pos)]);
      memtables_[get_memtable_idx_(pos)] = NULL;
    }
  }
  memtable_head_ = 0;
  memtable_tail_ = 0;
  MEMSET(memtables_, 0, sizeof(memtables_));
  new_active_memstore_handle_.reset();
}

void ObPGMemtableMgr::invalidate_readable_info_()
{
  LOG_INFO("invalid save safe slave read timestamp",
      K_(pkey),
      K(cur_readable_info_),
      K(tmp_readable_info_),
      K(last_readable_info_));
  tmp_readable_info_.reset();
  last_readable_info_.reset();
  cur_readable_info_.reset();
}

// create the first memtable of partition
int ObPGMemtableMgr::create_memtable(const ObDataStorageInfo& info, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObTableHandle memtable_handle;

  if (!info.is_valid() || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(info), K(pkey));
  } else {
    // Write lock
    TCWLockGuard lock_guard(lock_);
    const ObDataStorageInfo& data_info = info;
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::MEMTABLE;
    table_key.pkey_ = pkey;
    table_key.table_id_ = pkey.table_id_;
    table_key.trans_version_range_.base_version_ =
        data_info.get_publish_version() == 0 ? 1 : data_info.get_publish_version();
    table_key.trans_version_range_.multi_version_start_ = table_key.trans_version_range_.base_version_;
    table_key.trans_version_range_.snapshot_version_ = ObVersionRange::MAX_VERSION;
    table_key.log_ts_range_.start_log_ts_ = data_info.get_last_replay_log_ts();
    table_key.log_ts_range_.end_log_ts_ = ObLogTsRange::MAX_TS;
    table_key.log_ts_range_.max_log_ts_ = 0;
    memtable::ObMemtable* memtable = NULL;

    if (OB_FAIL(ObTableMgr::get_instance().create_memtable(table_key, memtable_handle))) {
      STORAGE_LOG(WARN, "failed to create memtable", K(ret), K(pkey));
    } else if (OB_FAIL(memtable_handle.get_memtable(memtable))) {
      STORAGE_LOG(WARN, "failed to get memtable", K(ret), K(info), K(pkey));
    } else if (FALSE_IT(memtable->set_max_schema_version(data_info.get_schema_version()))) {
      // set initial schema version for memtable
    } else if (OB_FAIL(add_memtable_(memtable))) {
      STORAGE_LOG(WARN, "add memtable error", K(ret), K(info), K(pkey));
    } else {
      memtable->set_with_accurate_log_ts_range(info.is_created_by_new_minor_freeze());
      invalidate_readable_info_();
    }
  }

  return ret;
}

int ObPGMemtableMgr::update_memtable_schema_version(const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  ObMemtable* active_memtable = get_active_memtable_();
  if (NULL != active_memtable) {
    int64_t old_schema_version = active_memtable->get_max_schema_version();
    active_memtable->set_max_schema_version(schema_version);
    TRANS_LOG(INFO, "update memtable schema version", K(*active_memtable), K(old_schema_version), K(schema_version));
  } else {
    TRANS_LOG(INFO, "none active memtable", K(schema_version));
  }

  return ret;
}

int ObPGMemtableMgr::new_active_memstore(const common::ObPartitionKey& pkey, int64_t& protection_clock)
{
  int ret = OB_SUCCESS;
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::MEMTABLE;
  table_key.pkey_ = pkey;
  table_key.table_id_ = pkey.table_id_;
  table_key.trans_version_range_.snapshot_version_ = ObVersionRange::MAX_VERSION;
  table_key.log_ts_range_.end_log_ts_ = ObLogTsRange::MAX_TS;
  table_key.log_ts_range_.max_log_ts_ = 0;
  memtable::ObMemtable* memtable = NULL;

  TCWLockGuard lock_guard(lock_);

  if (OB_ISNULL(get_active_memtable_())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "no active memstore, unexpected error", K(ret), K(pkey));
  } else if (get_unmerged_memtable_count_() >= MAX_FROZEN_MEMSTORE_CNT_IN_STORAGE ||
             get_memtable_count_() >= MAX_MEMSTORE_CNT) {
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    STORAGE_LOG(WARN,
        "Too many frozen memtables",
        K(ret),
        K(get_unmerged_memtable_count_()),
        K(get_memtable_count_()),
        K(pkey));
    new_active_memstore_handle_.reset();
  } else if (NULL != new_active_memstore_handle_.get_table()) {
    ret = OB_ENTRY_EXIST;
    STORAGE_LOG(WARN, "new active memstore already exist", K(ret), K(get_memtable_count_()), K(pkey));
  } else if (OB_FAIL(ObTableMgr::get_instance().create_memtable(table_key, new_active_memstore_handle_))) {
    STORAGE_LOG(WARN, "failed to create active memtable", K(ret), K(pkey));
  } else if (OB_FAIL(new_active_memstore_handle_.get_memtable(memtable))) {
    STORAGE_LOG(WARN, "new active memstore handle get memtable error", K(ret), K_(new_active_memstore_handle));
  } else if (OB_ISNULL(memtable)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "new_active_memstore must not null", K(ret), K(pkey));
  } else {
    protection_clock = memtable->get_retire_clock();
    STORAGE_LOG(INFO, "new active memstore success", K(pkey), K(*memtable), K(protection_clock));
  }

  return ret;
}

int ObPGMemtableMgr::effect_new_active_memstore(const ObSavedStorageInfoV2& info, const common::ObPartitionKey& pkey,
    const common::ObReplicaType& replica_type, const bool emergency)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable* active_mt = NULL;
  ObTimeGuard timeguard("effect new active memstore", 100L * 1000L);

  {
    TCWLockGuard lock_guard(lock_);

    if (OB_FAIL(new_active_memstore_handle_.get_memtable(active_mt))) {
      STORAGE_LOG(WARN, "no prepared memstore exist", K(ret), K(pkey));
    } else if (OB_ISNULL(active_mt)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "active_mt must not null", K(ret), K(pkey));
    } else if (OB_FAIL(active_mt->set_base_version(info.get_data_info().get_publish_version()))) {
      STORAGE_LOG(ERROR, "fail to set base version", K(ret), K(pkey));
    } else if (OB_FAIL(active_mt->set_start_log_ts(info.get_data_info().get_last_replay_log_ts()))) {
      STORAGE_LOG(ERROR, "fail to set base version", K(ret), K(pkey));
    } else if (OB_FAIL(save_frozen_base_storage_info_(info))) {
      STORAGE_LOG(ERROR, "fail to set frozen info", K(ret), K(pkey));
    } else {
      active_mt->set_with_accurate_log_ts_range(info.get_data_info().is_created_by_new_minor_freeze());

      timeguard.click();

      if (OB_SUCC(ret) && OB_FAIL(freeze_and_add_memtable_(replica_type, active_mt, emergency))) {
        STORAGE_LOG(WARN, "effect new active memstore failed", K(ret), K(pkey));
      }

      timeguard.click();

      if (OB_SUCC(ret)) {
        new_active_memstore_handle_.reset();
      }
    }
  }
  if (OB_FAIL(ret)) {
    // private method should not call publich method, otherwise K(*this) maybe deadlock;
    STORAGE_LOG(WARN, "effect new active memstore error", K(info), K(pkey), K(replica_type), K(*this));
  }
  return ret;
}

int ObPGMemtableMgr::clean_new_active_memstore()
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);
  new_active_memstore_handle_.reset();

  return ret;
}

int ObPGMemtableMgr::complete_active_memstore(const ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable* active_memstore = NULL;

  TCWLockGuard lock_guard(lock_);

  if (OB_UNLIKELY(NULL == (active_memstore = get_active_memtable_()))) {
    ret = OB_SUCCESS;
    STORAGE_LOG(WARN, "active memstore is NULL", K(ret), K(info));
  } else if (OB_UNLIKELY(!active_memstore->is_active_memtable())) {
    ret = OB_SUCCESS;
    STORAGE_LOG(WARN, "can not freeze twice", K(ret), K(info));
  } else if (OB_FAIL(save_frozen_base_storage_info_(info))) {
    STORAGE_LOG(ERROR, "fail to set frozen info", K(ret), K(info));
  } else if (OB_FAIL(freeze_active_memtable_(
                 info.get_data_info().get_last_replay_log_ts(), info.get_data_info().get_publish_version(), false))) {
    STORAGE_LOG(WARN, "freeze memstore failed", K(ret), K(info));
  } else {
    active_memstore->mark_for_split();
    new_active_memstore_handle_.reset();
  }

  return ret;
}

bool ObPGMemtableMgr::has_active_memtable()
{
  bool bool_ret = false;
  TCRLockGuard lock_guard(lock_);

  if (NULL != get_active_memtable_()) {
    bool_ret = true;
  }

  return bool_ret;
}

int ObPGMemtableMgr::get_active_memtable(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable* memtable = NULL;
  TCRLockGuard lock_guard(lock_);

  if (get_memtable_count_() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_ISNULL(memtable = get_active_memtable_())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(handle.set_table(memtable))) {
    STORAGE_LOG(WARN, "failed to set memtable", K(ret));
  }
  return ret;
}

int ObPGMemtableMgr::get_memtables(
    ObTablesHandle& handle, const bool reset_handle, const int64_t start_point, const bool include_active_memtable)
{
  TCRLockGuard lock_guard(lock_);
  if (reset_handle) {
    handle.reset();
  }
  return get_memtables_(handle, start_point, include_active_memtable);
}

int ObPGMemtableMgr::get_memtables_nolock(ObTablesHandle& handle)
{
  const int64_t start_point = -1;
  const bool include_active_memtable = true;
  return get_memtables_(handle, start_point, include_active_memtable);
}

int ObPGMemtableMgr::get_all_memtables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  if (OB_FAIL(get_memtables_nolock(handle))) {
    LOG_WARN("failed to get all memtables", K(ret));
  }
  return ret;
}

void ObPGMemtableMgr::clean_memtables()
{
  int tmp_ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);

  for (int64_t pos = memtable_head_; pos < memtable_tail_; ++pos) {
    const int64_t idx = get_memtable_idx_(pos);

    STORAGE_LOG(INFO,
        "succeed clear non reused memtable",
        K(idx),
        K(pos),
        K(memtable_head_),
        K(memtable_tail_),
        K(*memtables_[idx]));

    if (OB_ISNULL(memtables_[idx])) {
      tmp_ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "memtable must not null", K(tmp_ret), K(idx));
    } else if (OB_SUCCESS != (tmp_ret = ObTableMgr::get_instance().release_table(memtables_[idx]))) {
      STORAGE_LOG(WARN, "failed to release table", K(tmp_ret));
    }
    memtables_[idx] = NULL;
  }
  memtable_head_ = memtable_tail_;
  invalidate_readable_info_();
}

int ObPGMemtableMgr::release_head_memtable(memtable::ObMemtable* memtable)
{
  int tmp_ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);

  if (get_memtable_count_() > 0) {
    const int64_t idx = get_memtable_idx_(memtable_head_);
    if (NULL != memtables_[idx] && memtable == memtables_[idx]) {
      memtable->set_read_barrier();
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ObTableMgr::get_instance().release_table(memtable)))) {
        STORAGE_LOG(WARN, "failed to release memtable", K(tmp_ret), K(memtable->get_key()));
        // defensive code : check slave_read_timestamp and the right bounded
        // of frozen_memstore when releasing memstore (20660486)
      } else if (cur_readable_info_.max_readable_ts_ <= memtable->get_snapshot_version()) {
        // rebuild scenario maybe exists
        // ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "unexpected slave read timestamp or frozen memstore snapshot version",
            K_(cur_readable_info),
            K(*memtable));
      } else {
        // do nothing
      }
      memtables_[idx] = NULL;
      ++memtable_head_;
    }
  }
  return OB_SUCCESS;
}

bool ObPGMemtableMgr::has_memtable()
{
  TCRLockGuard lock_guard(lock_);
  return get_memtable_count_() > 0;
}

int64_t ObPGMemtableMgr::get_memtable_count() const
{
  TCRLockGuard lock_guard(lock_);
  return get_memtable_count_();
}

int ObPGMemtableMgr::get_first_frozen_memtable(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable* memtable = NULL;
  TCRLockGuard guard(lock_);
  if (OB_ISNULL(memtable = get_first_frozen_memtable_())) {
    // do nothing
  } else if (OB_FAIL(handle.set_table(memtable))) {
    STORAGE_LOG(WARN, "failed to set table", K(ret));
  }
  return ret;
}

int ObPGMemtableMgr::freeze_and_add_memtable_(
    const common::ObReplicaType& replica_type, memtable::ObMemtable* memtable, const bool emergency)
{
  int ret = OB_SUCCESS;
  ObMemtable* old_memtable = NULL;
  ObTimeGuard timeguard("freeze and add memtable", 100L * 1000L);

  if (OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(memtable));
  } else if (!ObReplicaTypeCheck::is_replica_with_memstore(replica_type)) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "replica without memstore, cannot freeze memtable", K(ret));
  } else if (get_memtable_count_() >= MAX_MEMSTORE_CNT) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "memtable count is too much, cannot add more", K(ret), K(get_memtable_count_()));
  } else if (OB_UNLIKELY(NULL == (old_memtable = get_active_memtable_()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "no active memtable found", KP(old_memtable));
  } else {
    if (OB_FAIL(wait_old_memtable_release_())) {
      STORAGE_LOG(WARN, "failed to wait_old_memtable_release", K(ret));
    }

    timeguard.click();

    if (OB_SUCC(ret) &&
        OB_FAIL(freeze_active_memtable_(memtable->get_start_log_ts(), memtable->get_base_version(), emergency))) {
      STORAGE_LOG(WARN, "failed to freeze active memtable", K(ret));
    }

    timeguard.click();

    if (OB_SUCC(ret)) {
      memtable->inc_timestamp(old_memtable->get_timestamp());
      memtable->set_max_schema_version(old_memtable->get_max_schema_version());

      if (OB_FAIL(add_memtable_(memtable))) {
        STORAGE_LOG(ERROR, "failed to do add memtable", K(ret), K(*memtable));
        ob_abort();
      }
    }
  }

  return ret;
}

int ObPGMemtableMgr::freeze_active_memtable_(int64_t freeze_log_ts, int64_t snapshot_version, const bool emergency)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable* frozen_memtable = get_active_memtable_();

  if (OB_UNLIKELY(NULL == frozen_memtable || !frozen_memtable->is_active_memtable())) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "frozen memtable not found", K(ret));
  } else if (OB_FAIL(frozen_memtable->minor_freeze(emergency))) {
    STORAGE_LOG(WARN, "memtable minor freeze failed", K(ret), K(*frozen_memtable));
  } else if (ObVersionRange::MAX_VERSION != frozen_memtable->get_snapshot_version()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("memtable cannot frozen twice", K(ret), K(snapshot_version), K(*frozen_memtable));
  } else if (OB_FAIL(frozen_memtable->set_snapshot_version(snapshot_version))) {
    LOG_ERROR("failed to set snapshot version", K(ret), K(*frozen_memtable));
  } else if (OB_FAIL(frozen_memtable->set_end_log_ts(freeze_log_ts))) {
    LOG_ERROR("failed to set end_log_ts", K(ret), K(*frozen_memtable), K(freeze_log_ts));
  } else if (OB_FAIL(frozen_memtable->update_max_log_ts(freeze_log_ts))) {
    LOG_ERROR("failed to update max log id", K(ret), K(*frozen_memtable), K(freeze_log_ts));
  }

  return ret;
}

int ObPGMemtableMgr::wait_old_memtable_release_()
{
  int ret = OB_SUCCESS;

  const int64_t SLEEP_US = 100;
  int64_t begin_time = common::ObTimeUtility::current_time();
  memtable::ObMemtable* old_memtable = get_active_memtable_();

  if (OB_UNLIKELY(NULL == old_memtable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "no active memtable found", KP(old_memtable));
  } else {
    old_memtable->set_write_barrier();

    MEM_BARRIER();  // need sequential consistency here

    while (old_memtable->get_write_ref() > 0) {
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        ObTaskController::get().allow_next_syslog();
        STORAGE_LOG(
            WARN, "wait old memstore release cost too long", "time", ObTimeUtility::current_time() - begin_time);
      }

      usleep(SLEEP_US);
    }
  }

  return ret;
}

int ObPGMemtableMgr::save_frozen_base_storage_info_(const ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;
  ObTableHandle handle;
  memtable::ObMemtable* memtable = NULL;

  if (OB_ISNULL(memtable = get_active_memtable_())) {
    ret = OB_ACTIVE_MEMTBALE_NOT_EXSIT;
    STORAGE_LOG(WARN, "active memtable must not null", K(ret), K(get_memtable_count_()));
  } else if (OB_FAIL(memtable->save_base_storage_info(info))) {
    STORAGE_LOG(WARN, "failed to save base store info", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to save base store info", K(info));
  }
  return ret;
}

memtable::ObMemtable* ObPGMemtableMgr::get_active_memtable_()
{
  memtable::ObMemtable* memtable = NULL;

  if (memtable_tail_ > memtable_head_) {
    memtable = memtables_[get_memtable_idx_(memtable_tail_ - 1)];
    if (NULL != memtable && !memtable->is_active_memtable()) {
      memtable = NULL;
    }
  }

  return memtable;
}

memtable::ObMemtable* ObPGMemtableMgr::get_memtable_(const int64_t pos) const
{
  return memtables_[get_memtable_idx_(pos)];
}

OB_INLINE int64_t ObPGMemtableMgr::get_memtable_idx_(const int64_t pos) const
{
  return pos & (MAX_MEMSTORE_CNT - 1);
}

int64_t ObPGMemtableMgr::get_memtable_count_() const
{
  return memtable_tail_ - memtable_head_;
}

int64_t ObPGMemtableMgr::get_unmerged_memtable_count_() const
{
  int64_t cnt = 0;

  for (int64_t i = memtable_head_; i < memtable_tail_; i++) {
    ObMemtable* memtable = get_memtable_(i);
    if (NULL == memtable) {
      STORAGE_LOG(ERROR, "memtable must not null");
    } else if (0 == memtable->get_minor_merged_time()) {
      cnt++;
    }
  }

  return cnt;
}

int ObPGMemtableMgr::add_memtable_(memtable::ObMemtable* memtable)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(memtable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "memtable is null, unexpected error", K(ret), KP(memtable));
  } else {
    const int64_t idx = get_memtable_idx_(memtable_tail_);
    memtable_tail_++;
    memtables_[idx] = memtable;
    memtable->inc_ref();
    ObTaskController::get().allow_next_syslog();
    // TODO check if table continues and if has duplicates
    STORAGE_LOG(INFO,
        "succeed to add memtable",
        K(memtable_head_),
        K(memtable_tail_),
        K(get_memtable_count_()),
        K(*memtable),
        KP(memtable),
        K(lbt()));
  }

  return ret;
}

int ObPGMemtableMgr::get_memtables_(
    ObTablesHandle& handle, const int64_t start_point, const bool include_active_memtable)
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

int ObPGMemtableMgr::add_tables_(const int64_t start_pos, const bool include_active_memtable, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(start_pos > -1)) {
    const int64_t last_pos = memtable_tail_ - 1;
    for (int64_t pos = start_pos; OB_SUCC(ret) && pos < last_pos; ++pos) {
      memtable::ObMemtable* memtable = get_memtable_(pos);
      if (OB_ISNULL(memtable)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "memtable must not null", K(ret));
      } else if (OB_FAIL(handle.add_table(memtable))) {
        STORAGE_LOG(WARN, "failed to add memtable", K(ret));
      }
    }
    if (OB_SUCC(ret) && memtable_tail_ > memtable_head_) {
      ObMemtable* last_memtable = get_memtable_(last_pos);
      if (include_active_memtable || last_memtable->is_frozen_memtable()) {
        if (OB_FAIL(handle.add_table(last_memtable))) {
          LOG_WARN("failed to add last memtable to handle",
              K(ret),
              K(start_pos),
              K(include_active_memtable),
              K(*last_memtable));
        }
      }
    }
  }
  return ret;
}

int ObPGMemtableMgr::push_reference_tables(const ObTablesHandle& ref_memtables)
{
  int ret = OB_SUCCESS;
  ObTablesHandle current_memtables;
  {
    TCWLockGuard lock_guard(lock_);
    if (OB_FAIL(get_memtables_nolock(current_memtables))) {
      LOG_WARN("failed to get memtables nolock", K(ret));
    } else if (OB_FAIL(current_memtables.add_tables(ref_memtables))) {
      LOG_WARN("failed to add tables", K(ret));
    } else {
      for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
        ObMemtable* memtable = get_memtable_(i);
        memtable->dec_ref();
      }
      memtable_tail_ = memtable_head_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < current_memtables.get_count(); ++i) {
        ObMemtable* memtable = static_cast<ObMemtable*>(current_memtables.get_table(i));
        if (OB_FAIL(add_memtable_(memtable))) {
          LOG_WARN("failed to add memtable", K(ret), K(current_memtables), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (current_memtables.get_count() == 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition should have at least one memtable", K(ret));
        } else {
          ObTableStore::ObITableSnapshotVersionCompare cmp(ret);
          std::sort(&memtables_[0], &memtables_[memtable_tail_], cmp);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to sort memtables", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    // private method should not call publich method, otherwise K(*this) maybe deadlock;
    LOG_WARN("push refrence tables error", K(ret), K(*this));
  }
  return ret;
}

int ObPGMemtableMgr::find_start_pos_(const int64_t start_point, int64_t& start_pos)
{
  int ret = OB_SUCCESS;
  start_pos = -1;
  if (OB_UNLIKELY(start_point < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_point", K(ret), K(start_point));
  }
  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
    ObMemtable* memtable = get_memtable_(i);
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

int64_t ObPGMemtableMgr::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    TCRLockGuard lock_guard(lock_);
    J_OBJ_START();
    J_ARRAY_START();
    for (int64_t i = memtable_head_; i < memtable_tail_; ++i) {
      ObMemtable* memtable = get_memtable_(i);
      if (nullptr != memtable) {
        J_OBJ_START();
        J_KV(K(i), "table_key", memtable->get_key(), "version", memtable->get_version(), "ref", memtable->get_ref());
        J_OBJ_END();
        J_COMMA();
      }
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

int ObPGMemtableMgr::update_readable_info(const ObPartitionReadableInfo& readable_info)
{
  int ret = OB_SUCCESS;

  if (readable_info.force_ || readable_info.max_readable_ts_ >= cur_readable_info_.max_readable_ts_) {
    last_readable_info_ = cur_readable_info_;
    cur_readable_info_ = readable_info;
    const int64_t cur_time = ObTimeUtility::current_time();
    if (cur_time > last_print_readable_info_ts_ + PRINT_READABLE_INFO_DURATION_US) {
      STORAGE_LOG(TRACE, "update_readable_info", K_(pkey), K(readable_info));
      last_print_readable_info_ts_ = cur_time;
    }
  } else {
    STORAGE_LOG(ERROR,
        "min slave read timestamp is smaller than the last, unexpected error",
        K_(pkey),
        K(cur_readable_info_),
        K(readable_info),
        K(last_readable_info_));
  }

  return ret;
}

int ObPGMemtableMgr::find_start_pos_(
    const int64_t start_log_ts, const int64_t start_snapshot_version, int64_t& start_pos)
{
  int ret = OB_SUCCESS;
  start_pos = -1;
  if (OB_UNLIKELY(start_log_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_snapshot_version", K(ret), K(start_snapshot_version), K(start_log_ts));
  }
  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
    ObMemtable* memtable = get_memtable_(i);
    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable must not null", K(ret));
    } else if (memtable->get_end_log_ts() == start_log_ts) {
      if (memtable->get_snapshot_version() > start_snapshot_version) {
        start_pos = i;
        break;
      }
    } else if (memtable->get_end_log_ts() > start_log_ts) {
      start_pos = i;
      break;
    }
  }
  return ret;
}

int ObPGMemtableMgr::get_memtables_v2(ObTablesHandle& handle, const int64_t start_log_ts,
    const int64_t start_snapshot_version, const bool reset_handle, const bool include_active_memtable)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  if (reset_handle) {
    handle.reset();
  }
  int64_t start_pos = memtable_head_;
  if (0 < start_log_ts) {
    if (OB_FAIL(find_start_pos_(start_log_ts, start_snapshot_version, start_pos))) {
      LOG_WARN("failed to find_start_pos_", K(ret), K(start_log_ts), K(start_snapshot_version));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_tables_(start_pos, include_active_memtable, handle))) {
    LOG_WARN("failed to add_tables",
        K(ret),
        K(start_log_ts),
        K(start_snapshot_version),
        K(include_active_memtable),
        K(reset_handle));
  }
  return ret;
}

memtable::ObMemtable* ObPGMemtableMgr::get_first_frozen_memtable_()
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable* memtable = NULL;
  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; i++) {
    memtable = get_memtable_(i);
    if (NULL == memtable) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "memtable must not null", K(ret));
    } else {
      if (memtable->is_frozen_memtable()) {
        break;
      } else {
        memtable = NULL;
      }
    }
  }
  return memtable;
}

}  // namespace storage
}  // namespace oceanbase
