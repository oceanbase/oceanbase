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
#include "ob_table_store.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_array_iterator.h"
#include "ob_table_mgr.h"
#include "share/ob_task_define.h"
#include "storage/ob_pg_memtable_mgr.h"
#include "share/ob_tenant_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_force_print_log.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;
using namespace memtable;
using namespace share::schema;

ObTableStore::ObTableStore()
    : is_inited_(false),
      freeze_info_mgr_(nullptr),
      table_count_(0),
      is_ready_for_read_(false),
      pg_memtable_mgr_(nullptr),
      complement_minor_sstable_(nullptr),
      pkey_(),
      table_id_(OB_INVALID_ID),
      uptime_(0),
      start_pos_(-1),
      inc_pos_(-1),
      multi_version_start_(-1)
{
  MEMSET(tables_, 0, sizeof(tables_));
}

ObTableStore::~ObTableStore()
{
  reset();
}

void ObTableStore::reset()
{
  for (int64_t i = 0; i < table_count_; ++i) {
    ObITable* table = tables_[i];
    if (nullptr != table) {
      table->dec_ref();
    }
  }
  if (nullptr != complement_minor_sstable_) {
    complement_minor_sstable_->dec_ref();
    complement_minor_sstable_ = nullptr;
  }
  is_inited_ = false;
  freeze_info_mgr_ = nullptr;
  MEMSET(tables_, 0, sizeof(tables_));
  table_count_ = 0;
  pkey_.reset();
  table_id_ = OB_INVALID_ID;
  uptime_ = 0;
  start_pos_ = -1;
  inc_pos_ = -1;
  is_ready_for_read_ = false;
  replay_tables_.reset();
  pg_memtable_mgr_ = nullptr;
  multi_version_start_ = -1;
}

OB_SERIALIZE_MEMBER(
    ObTableStore, pkey_, table_id_, uptime_, start_pos_, inc_pos_, replay_tables_, multi_version_start_);

int ObTableStore::init(const common::ObPartitionKey& pkey, const uint64_t table_id,
    ObFreezeInfoSnapshotMgr* freeze_info_mgr, ObPGMemtableMgr* pg_memtable_mgr)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableStore init twice", K(ret), K(PRETTY_TS(*this)));
  } else if (!pkey.is_valid() || OB_INVALID_ID == table_id || OB_ISNULL(freeze_info_mgr) ||
             OB_ISNULL(pg_memtable_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to init ObTableStore",
        K(ret),
        K(pkey),
        K(table_id),
        KP(freeze_info_mgr),
        KP(pg_memtable_mgr));
  } else {
    is_inited_ = true;
    freeze_info_mgr_ = freeze_info_mgr;
    is_ready_for_read_ = false;
    table_count_ = 0;
    pkey_ = pkey;
    table_id_ = table_id;
    uptime_ = ObTimeUtility::current_time();
    start_pos_ = -1;
    inc_pos_ = -1;
    pg_memtable_mgr_ = pg_memtable_mgr;
    complement_minor_sstable_ = nullptr;
    multi_version_start_ = -1;
  }

  return ret;
}

int ObTableStore::init(
    const ObTableStore& table_store, ObFreezeInfoSnapshotMgr* freeze_info_mgr, ObPGMemtableMgr* pg_memtable_mgr)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableStore init twice", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_ISNULL(freeze_info_mgr) || OB_ISNULL(pg_memtable_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to init ObTableStore", K(ret), KP(freeze_info_mgr), KP(pg_memtable_mgr));
  } else if (OB_FAIL(replay_tables_.assign(table_store.replay_tables_))) {
    LOG_WARN("failed to copy replay tables", K(ret));
  } else {
    is_inited_ = true;
    freeze_info_mgr_ = freeze_info_mgr;
    table_count_ = 0;
    is_ready_for_read_ = false;
    pkey_ = table_store.pkey_;
    table_id_ = table_store.table_id_;
    uptime_ = table_store.uptime_;
    start_pos_ = table_store.start_pos_;
    inc_pos_ = table_store.inc_pos_;
    pg_memtable_mgr_ = pg_memtable_mgr;
    complement_minor_sstable_ = table_store.complement_minor_sstable_;
    multi_version_start_ = table_store.multi_version_start_;
  }

  return ret;
}

// TODO  get_all_tables no need return active memtable, and should always return complement
int ObTableStore::get_all_tables(
    const bool include_active_memtable, const bool include_complement_minor_sstable, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();

  // allow get all tables when not valid
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_count_; ++i) {
    if (OB_FAIL(handle.add_table(tables_[i]))) {
      LOG_WARN("failed to add table", K(ret), K(PRETTY_TS(*this)));
    }
  }

  if (OB_SUCC(ret) && include_complement_minor_sstable && OB_NOT_NULL(complement_minor_sstable_)) {
    if (OB_FAIL(handle.add_table(complement_minor_sstable_))) {
      LOG_WARN("failed to add complement table", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_memtables(include_active_memtable, handle))) {
      LOG_WARN("failed to get memtables", K(ret), K(PRETTY_TS(*this)));
    }
  }
  return ret;
}

int ObTableStore::get_read_tables(const int64_t snapshot_version, ObTablesHandle& handle, const bool allow_not_ready)
{
  int ret = OB_SUCCESS;
  bool can_read = false;
  bool contain_snapshot_version = false;

  if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(snapshot_version));
  } else if (OB_UNLIKELY(0 == table_count_ && allow_not_ready)) {
    if (OB_FAIL(pg_memtable_mgr_->get_memtables(handle))) {
      LOG_WARN("failed to get memtables", K(ret));
    }
  } else if (OB_UNLIKELY(!allow_not_ready && !is_ready_for_read_)) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("table store not ready for read", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(snapshot_version), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(get_read_base_tables(snapshot_version, handle, contain_snapshot_version))) {
    LOG_WARN("failed to get major read tables", K(ret), K(allow_not_ready), K(snapshot_version), K(PRETTY_TS(*this)));
  } else if (OB_LIKELY(!handle.empty())) {
    if (OB_FAIL(get_inc_read_tables(snapshot_version, handle, contain_snapshot_version))) {
      LOG_WARN("failed to get_inc_read_tables", K(ret), K(snapshot_version), K(PRETTY_TS(*this)));
    }
  } else {
    // allow_not_ready will be set before the index table takes effect.
    // 1) When there is no major sstable, return all incremental sstables.
    // Does not check whether the snapshot_version is included.
    // 2) When there is a corresponding major sstable, an error needs to be reported.
    // This is because indexing will wait for the end of the transaction.
    // If there are old queries, the indexing logic must have error.
    if (!allow_not_ready) {
      // handle must be empty, no need to reset
      if (OB_FAIL(get_hist_major_table(snapshot_version, handle))) {
        LOG_WARN("Failed to get hist major table", K(ret), K(snapshot_version), K(PRETTY_TS(*this)));
      } else if (handle.empty()) {
        ret = OB_SNAPSHOT_DISCARDED;
        LOG_WARN("cannot found specified version", K(ret), K(snapshot_version), K(PRETTY_TS(*this)));
      }
    } else if (start_pos_ != inc_pos_) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("major sstable exists, but cannot found match major version",
          K(ret),
          K(snapshot_version),
          K(PRETTY_TS(*this)));
    } else if (OB_FAIL(get_minor_sstables(handle))) {
      STORAGE_LOG(WARN, "Failed to get all minor sstables", K(ret));
    } else if (OB_NOT_NULL(complement_minor_sstable_) && OB_FAIL(handle.add_table(complement_minor_sstable_))) {
      LOG_WARN("failed to add complement table", K(ret), K(handle), K(*complement_minor_sstable_));
    } else if (OB_FAIL(get_memtables(true /*include_active_memtable*/, handle))) {
      LOG_WARN("Failed to get memtables", K(ret), K(PRETTY_TS(*this)));
    }
  }

  return ret;
}

int ObTableStore::get_read_base_tables(
    const int64_t snapshot_version, ObTablesHandle& handle, bool& contain_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObITable* table = nullptr;

  for (int64_t i = inc_pos_ - 1; OB_SUCC(ret) && i >= start_pos_; --i) {
    if (OB_ISNULL(tables_[i])) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Unexpected null table", K(ret), K(i), K(PRETTY_TS(*this)));
    } else if (tables_[i]->is_major_sstable() && tables_[i]->get_snapshot_version() <= snapshot_version) {
      table = tables_[i];
      contain_snapshot_version = table->get_snapshot_version() == snapshot_version;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(table)) {
    if (OB_FAIL(handle.add_table(table))) {
      LOG_WARN("failed to add read base sstable", K(ret), KPC(table));
    }
  }
  LOG_DEBUG("get read base tables", K(handle), K(snapshot_version), K(PRETTY_TS(*this)));

  return ret;
}

int ObTableStore::get_inc_read_tables(
    const int64_t snapshot_version, ObTablesHandle& handle, bool& contain_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObSSTable* base_table = nullptr;
  ObITable* inc_table = nullptr;

  if (OB_ISNULL(base_table = static_cast<ObSSTable*>(handle.get_last_table()))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Unexpected base table handle", K(ret), K(handle));
  } else {
    bool found_inc = false;
    int64_t base_table_count = handle.get_count();
    uint64_t last_log_ts = base_table->is_major_sstable() ? INT64_MAX : base_table->get_end_log_ts();
    for (int64_t i = inc_pos_; OB_SUCC(ret) && i < table_count_; i++) {
      inc_table = tables_[i];
      LOG_DEBUG("table", K(i), KPC(static_cast<ObSSTable*>(inc_table)), K(last_log_ts), K(handle));
      if (!found_inc) {
        if ((base_table->is_major_sstable() &&
                inc_table->get_upper_trans_version() > base_table->get_snapshot_version()) /*major*/
            || inc_table->get_end_log_ts() > last_log_ts /*buf minor*/) {
          found_inc = true;
        }
      }
      if (OB_SUCC(ret) && found_inc) {
        if (OB_UNLIKELY(inc_table->get_start_log_ts() > last_log_ts)) {
          ret = OB_REPLICA_NOT_READABLE;
          LOG_WARN("table's log ts range is not continuous", K(ret), K(last_log_ts), KPC(inc_table));
        } else if (OB_FAIL(handle.add_table(inc_table))) {
          LOG_WARN("failed to add table", K(ret), KPC(inc_table), K(handle));
        } else {
          last_log_ts = inc_table->get_end_log_ts();
        }
      }
    }  // end for
    if (OB_SUCC(ret)) {
      // add complement sstable
      if (OB_NOT_NULL(complement_minor_sstable_) && OB_FAIL(handle.add_table(complement_minor_sstable_))) {
        LOG_WARN("failed to add complement table", K(ret), K(handle), K(*complement_minor_sstable_));
      } else {
        // add memtable
        const int64_t sstable_count = handle.get_count();
        const int64_t get_memtable_snapshot_version = handle.get_table(sstable_count - 1)->get_snapshot_version();
        const uint64_t get_memtable_log_ts =
            tables_[table_count_ - 1]->is_major_sstable() ? 0 : tables_[table_count_ - 1]->get_end_log_ts();
        if (OB_FAIL(pg_memtable_mgr_->get_memtables_v2(
                handle, get_memtable_log_ts, get_memtable_snapshot_version, false /*reset_handle*/))) {
          LOG_WARN("failed to get memtables",
              K(ret),
              K(get_memtable_log_ts),
              K(get_memtable_snapshot_version),
              KPC(PRETTY_TS_P(this)));
        } else if (OB_UNLIKELY(handle.get_count() <= base_table_count)) {
          ret = OB_REPLICA_NOT_READABLE;
          LOG_WARN("cannot find match memtable", K(ret), K(handle), K(PRETTY_TS(*this)));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // read snapshot version alwasy >= snapshot version if base table
      if (OB_LIKELY(!contain_snapshot_version && snapshot_version >= multi_version_start_ &&
                    snapshot_version <= handle.get_table(handle.get_count() - 1)->get_snapshot_version())) {
        // most case should satisfy this path
        contain_snapshot_version = true;
      }
      if (OB_UNLIKELY(!contain_snapshot_version)) {
        ret = OB_SNAPSHOT_DISCARDED;
        LOG_WARN("no table found for specified version", K(ret), K(handle), K(snapshot_version));
      } else {
        LOG_DEBUG("get_inc_read_tables", K(*base_table), K(handle));
      }
    }
  }

  return ret;
}

int ObTableStore::get_hist_major_table(const int64_t snapshot_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;

  ObITable* table = nullptr;
  for (int64_t i = start_pos_ - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_ISNULL(table = tables_[i])) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Unexpected null table", K(ret), K(i), K(PRETTY_TS(*this)));
    } else if (table->get_snapshot_version() == snapshot_version) {
      if (OB_FAIL(handle.add_table(table))) {
        LOG_WARN("failed to add major sstable", K(ret));
      } else {
        break;
      }
    }
  }

  return ret;
}

int ObTableStore::get_major_sstable(const common::ObVersion& version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else if (!version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(version));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_pos_; ++i) {
      ObITable* table = tables_[i];
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret), K(i), K(PRETTY_TS(*this)));
      } else if (table->get_version() > version) {
        break;
      } else if (table->get_version() == version && table->is_major_sstable()) {
        if (OB_FAIL(handle.add_table(table))) {
          LOG_WARN("failed to add table", K(ret), K(PRETTY_TS(*this)));
        }
        break;
      }
    }
  }

  if (OB_SUCC(ret) && handle.empty()) {
    ret = OB_VERSION_NOT_MATCH;
    LOG_WARN("Cannot find major sstable with specified version", K(ret), K(version), K(PRETTY_TS(*this)));
  }

  return ret;
}

int ObTableStore::get_sample_read_tables(const common::SampleInfo& sample_info, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tmp_handle;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(get_read_tables(INT64_MAX, tmp_handle))) {
    LOG_WARN("failed to get read tables", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_handle.get_count(); ++i) {
      ObITable* table = tmp_handle.get_table(i);
      bool need_table = false;
      if (SampleInfo::SAMPLE_ALL_DATA == sample_info.scope_) {
        need_table = true;
      } else if (SampleInfo::SAMPLE_BASE_DATA == sample_info.scope_) {
        need_table = table->is_major_sstable();
      } else if (SampleInfo::SAMPLE_INCR_DATA == sample_info.scope_) {
        need_table = !table->is_major_sstable();
      }
      if (need_table) {
        if (OB_FAIL(handle.add_table(table))) {
          LOG_WARN("failed to add table to handle", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && handle.get_count() == 0) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("cannot found specified version", K(ret), K(PRETTY_TS(*this)));
    }
  }
  return ret;
}

int ObTableStore::get_latest_major_sstable(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();

  if (table_count_ <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = inc_pos_ - 1; OB_SUCC(ret) && i >= start_pos_; i--) {
      if (OB_ISNULL(tables_[i])) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret), K(PRETTY_TS(*this)));
      } else if (tables_[i]->is_major_sstable()) {
        if (OB_FAIL(handle.set_table(tables_[i]))) {
          LOG_WARN("failed to add table", K(ret));
        }
        break;
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!handle.is_valid())) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}

// merged_table allowed to be null
int ObTableStore::build_new_merge_store(const AddTableParam& param, ObTablesHandle& old_handle)
{
  int ret = OB_SUCCESS;
  // The version range of major sstables will not cross.
  // The version range in other tables will not cross.
  // The construction steps are as follows:
  // 1. The merged_table needs to find the corresponding memtable and replace it after MINI MERGE.
  // 2. The merged_table will be added to the end of major_tables after MAJOR MERGE;
  // 3. Find the required minor and major sstable according to multi_version_start
  // 4. According to keep_major_version_num supplement, the major sstable may needs to be retained
  // (if the partition is split, the upper layer should pass 0. If it is not 0,
  //  the baseline before and after the split will be retained at the same time)
  ObArray<ObITable*> major_tables;
  ObArray<ObITable*> inc_tables;  // memtable or minor sstable
  int64_t temp_kept_major_version_num = param.max_kept_major_version_number_;
  int64_t multi_version_start = param.multi_version_start_;
  bool need_safe_check = table_count_ >= MAX_SSTABLE_CNT_IN_STORAGE - RESERVED_STORE_CNT_IN_STORAGE;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableStore is not inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to build new merge store", K(ret), K(param));
  } else if (table_count_ != 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot rebuild table store", K(ret), K(PRETTY_TS(*this)));
  } else if (old_handle.get_count() > MAX_TABLE_CNT_IN_STORAGE) {
    ret = OB_ERR_SYS;
    LOG_ERROR("too many tables in old_handle", K(ret), K(old_handle.get_count()));
  } else if (OB_FAIL(classify_tables(old_handle, major_tables, inc_tables))) {
    LOG_WARN("failed to classify tables", K(ret));
  } else if (OB_NOT_NULL(param.table_)) {
    if (param.table_->is_trans_sstable()) {
      if (!pkey_.is_trans_table()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("trans table should only appear in trans sstable", K(ret), K(param));
      } else if (OB_FAIL(add_trans_sstable(param.table_, major_tables))) {
        LOG_WARN("failed to build trans sstables", K(ret));
      }
    } else if (pkey_.is_trans_table()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trans table should only have trans sstable", K(ret), K(param));
    } else if (param.table_->is_minor_sstable()) {
      if (OB_FAIL(add_minor_sstable(need_safe_check, inc_tables, param.table_))) {
        LOG_WARN("failed to replace_memtable_with_minor_sstable", K(ret), K(param), K(old_handle));
      }
    } else if (param.table_->is_major_sstable()) {
      if (OB_FAIL(add_major_sstable(param.table_, major_tables))) {
        LOG_WARN("failed to build major merge store", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected table type", K(ret), KPC(param.table_));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_tables(temp_kept_major_version_num,
            multi_version_start,
            major_tables,
            inc_tables,
            param.backup_snapshot_version_))) {
      LOG_WARN("failed to build_tables", K(ret), K(multi_version_start), KPC(param.table_), K(old_handle));
    } else {
      if (OB_NOT_NULL(param.complement_minor_sstable_)) {
        param.complement_minor_sstable_->inc_ref();
        complement_minor_sstable_ = param.complement_minor_sstable_;
      }
      if (OB_FAIL(update_replay_tables())) {
        LOG_WARN("failed to update_replay_tables", K(ret));
      } else if (OB_FAIL(update_multi_version_start())) {
        LOG_WARN("Failed to update multi version start of table store", K(ret));
      } else {
        FLOG_INFO("succeed to build new merge store", K(major_tables), K(inc_tables), K(PRETTY_TS(*this)));
      }
    }
  }
  return ret;
}

bool ObTableStore::is_multi_version_break(const ObVersionRange& new_version_range, const int64_t last_snapshot_vesion)
{
  return new_version_range.base_version_ != new_version_range.multi_version_start_ &&
         new_version_range.multi_version_start_ > last_snapshot_vesion;
}

int ObTableStore::classify_tables(
    const ObTablesHandle& old_handle, ObArray<ObITable*>& major_tables, ObArray<ObITable*>& inc_tables)
{
  int ret = OB_SUCCESS;
  major_tables.reset();
  inc_tables.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store not is_inited_", K(ret), K(PRETTY_TS(*this)));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < old_handle.get_count(); ++i) {
    ObITable* table = old_handle.get_table(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret));
    } else if (table->is_memtable()) {
      // new minor merge do not need pass memtable to judge active trx count
      continue;
    } else if (table->is_complement_minor_sstable()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Unexpected complement sstable in old handle", K(ret), K(i), K(old_handle));
    } else if (table->is_major_sstable()) {
      if (OB_FAIL(major_tables.push_back(table))) {
        LOG_WARN("failed to add major tables", K(ret));
      }
    } else if (OB_FAIL(inc_tables.push_back(table))) {
      LOG_WARN("failed to add other tables", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sort_major_tables(major_tables))) {
      LOG_WARN("failed to sort major tables", K(ret));
    } else if (OB_FAIL(sort_minor_tables(inc_tables))) {
      LOG_WARN("failed to sort inc tables", K(ret));
    } else {
      LOG_DEBUG("classify table result", K(major_tables), K(inc_tables));
    }
  }

  return ret;
}

int ObTableStore::add_trans_sstable(ObSSTable* new_table, ObIArray<ObITable*>& trans_tables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == new_table || trans_tables.count() > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to add trans sstable", K(ret), KP(new_table), K(trans_tables));
  } else if (trans_tables.empty()) {
  } else if (OB_ISNULL(trans_tables.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null trans table", K(ret), K(trans_tables));
  } else if (trans_tables.at(0)->get_snapshot_version() >= new_table->get_snapshot_version()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new table is not newer than old table, not expected",
        K(ret),
        "new table key",
        new_table->get_key(),
        "old table key",
        trans_tables.at(0)->get_key());
  }
  if (OB_SUCC(ret)) {
    trans_tables.reuse();
    if (OB_FAIL(trans_tables.push_back(new_table))) {
      LOG_WARN("failed to push back new trans sstable", K(ret), KPC(new_table));
    }
  }
  return ret;
}

int ObTableStore::add_major_sstable(ObSSTable* new_table, ObArray<ObITable*>& major_tables)
{
  int ret = OB_SUCCESS;
  bool need_add = true;

  if (OB_ISNULL(new_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(new_table));
  }
  for (int64_t i = major_tables.count() - 1; OB_SUCC(ret) && need_add && i >= 0; --i) {
    ObITable* table = major_tables.at(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret));
    } else if (table->get_key().pkey_ == new_table->get_key().pkey_ &&
               table->get_key().version_.major_ == new_table->get_key().version_.major_) {
      LOG_DEBUG("add major sstables", K(table->get_key()), K(new_table->get_key()));
      if (table->get_key() != new_table->get_key()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("major version same but table key not match", K(ret), K(*table), K(*new_table));
      }
      need_add = false;
      break;
    }
  }

  if (OB_SUCC(ret) && need_add) {
    if (OB_FAIL(major_tables.push_back(new_table))) {
      LOG_WARN("failed to add major tables", K(ret));
    } else if (OB_FAIL(sort_major_tables(major_tables))) {
      LOG_WARN("failed to sort major tables", K(ret));
    }
  }
  return ret;
}

int ObTableStore::add_minor_sstable(const bool need_safe_check, ObIArray<ObITable*>& inc_tables, ObSSTable* new_table)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable*> tmp_tables;

  if (OB_ISNULL(new_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("merged_table maybe not null", K(ret));
  } else if (OB_FAIL(check_need_add_minor_sstable(inc_tables, new_table, tmp_tables))) {
    LOG_WARN("failed to check_need_add_minor_sstable", K(ret), K(inc_tables));
  } else if (need_safe_check && tmp_tables.count() == inc_tables.count()) {
    ret = OB_MINOR_MERGE_NOT_ALLOW;
    LOG_WARN("too many sstables, no pos for fresh new minor sstable", K(ret), K(*new_table), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(tmp_tables.push_back(new_table))) {
    LOG_WARN("failed to add merged table", K(ret));
  } else if (OB_FAIL(sort_minor_tables(tmp_tables))) {
    LOG_WARN("failed to sort tables", K(ret));
  } else if (OB_FAIL(inc_tables.assign(tmp_tables))) {
    LOG_WARN("failed to assign inc tables", K(ret));
  } else {
    LOG_DEBUG("succeed to add minor sstable", "new_table_key", new_table->get_key(), K(inc_tables));
  }
  return ret;
}

bool ObTableStore::check_include_by_log_ts_range(ObITable& a, ObITable& b)
{
  bool bret = false;
  if (a.get_end_log_ts() >= b.get_end_log_ts() && a.get_start_log_ts() <= b.get_start_log_ts()) {
    bret = true;
  }
  return bret;
}

bool ObTableStore::check_intersect_by_log_ts_range(ObITable& a, ObITable& b)
{
  bool bret = false;
  if (!(a.get_end_log_ts() <= b.get_start_log_ts() || a.get_start_log_ts() >= b.get_end_log_ts())) {
    bret = true;
  }
  return bret;
}

int ObTableStore::check_need_add_minor_sstable(
    ObIArray<ObITable*>& inc_tables, ObITable* new_table, ObIArray<ObITable*>& tmp_tables)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(new_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to check minor sstable need to add", K(ret), KP(new_table));
  } else if (!new_table->get_key().log_ts_range_.is_valid() || new_table->get_key().log_ts_range_.is_empty()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("meet invalid or empty log_ts_range sstable", KPC(new_table));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_tables.count(); ++i) {
      ObITable* table = inc_tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret));
      } else if (table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected memtable in inc tables", K(ret), KPC(table), K(i), K(inc_tables));
      } else if (check_include_by_log_ts_range(*new_table, *table)) {
        // existing table contained by new table
        LOG_DEBUG("table purged", K(*new_table), K(*table));
      } else if (check_include_by_log_ts_range(*table, *new_table)) {
        ret = OB_MINOR_SSTABLE_RANGE_CROSS;
        LOG_WARN("new_table is contained by existing table", K(ret), KPC(new_table), KPC(table));
      } else if (check_intersect_by_log_ts_range(*table, *new_table)) {
        ret = OB_MINOR_SSTABLE_RANGE_CROSS;
        LOG_WARN("new table's log_ts range is crossed with existing table", K(*new_table), K(*table));
      } else if (OB_FAIL(tmp_tables.push_back(table))) {
        LOG_WARN("failed to add table", K(ret));
      }
    }
  }

  return ret;
}

int ObTableStore::build_tables(const int64_t kept_major_version_num, const int64_t expect_multi_version_start,
    ObIArray<ObITable*>& major_tables, ObIArray<ObITable*>& inc_tables, const int64_t backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  int64_t inc_pos = -1;
  int64_t need_major_pos = -1;
  int64_t backup_major_pos = -1;
  ObITable* major_table = NULL;
  table_count_ = 0;
  start_pos_ = 0;
  is_ready_for_read_ = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store not is_inited_", K(ret), K(PRETTY_TS(*this)));
  } else if (expect_multi_version_start <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", K(ret), K(expect_multi_version_start));
  } else {
    if (major_tables.empty()) {
      // no major sstable exists
      need_major_pos = 0;
    } else if (OB_FAIL(
                   find_need_major_sstable(major_tables, expect_multi_version_start, need_major_pos, major_table))) {
      LOG_WARN("failed to find_need_major_sstable", K(ret));
    } else if (OB_FAIL(find_need_backup_major_sstable(major_tables, backup_snapshot_version, backup_major_pos))) {
      LOG_WARN("failed to find need backup major sstable", K(ret), K(backup_snapshot_version));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(major_table)) {
        // keep all inc table when major table not exists
        inc_pos = 0;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < inc_tables.count(); ++i) {
          if (inc_tables.at(i)->get_upper_trans_version() > major_table->get_snapshot_version()) {
            inc_pos = i;
            break;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t copy_start_pos = 0;
      if (kept_major_version_num >= major_tables.count()) {
        copy_start_pos = 0;
      } else {
        copy_start_pos = std::min(need_major_pos, major_tables.count() - kept_major_version_num);
      }
      start_pos_ = need_major_pos - copy_start_pos;
      inc_pos_ = major_tables.count() - copy_start_pos;
      if (OB_FAIL(add_backup_table(major_tables, backup_major_pos, copy_start_pos))) {
        LOG_WARN("failed to add_backup_table", K(ret));
      } else if (OB_FAIL(inner_add_table(major_tables, copy_start_pos))) {
        LOG_WARN("failed to add major table", K(ret));
      } else if (inc_pos >= 0 && OB_FAIL(inner_add_table(inc_tables, inc_pos))) {
        LOG_WARN("failed to add inc table", K(ret));
      } else {
        LOG_INFO("succ to add table for build_tables",
            K(backup_major_pos),
            K(need_major_pos),
            K(inc_pos),
            K(copy_start_pos));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_valid()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid table store", K(ret), K(PRETTY_TS(*this)));
    } else if (OB_FAIL(check_ready_for_read())) {
      LOG_WARN("failed check_read_for_read", K(ret), K(PRETTY_TS(*this)));
    }
  }

  return ret;
}

int ObTableStore::add_backup_table(
    ObIArray<ObITable*>& major_tables, const int64_t backup_major_pos, const int64_t copy_start_pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store not is_inited_", K(ret), K(PRETTY_TS(*this)));
  } else if (backup_major_pos < 0 || backup_major_pos >= copy_start_pos) {
    // do nothing
  } else if (backup_major_pos >= major_tables.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(backup_major_pos), K(major_tables));
  } else {
    ObITable* major_table = major_tables.at(backup_major_pos);

    if (OB_SUCC(ret)) {
      if (table_count_ >= MAX_SSTABLE_CNT_IN_STORAGE) {
        ret = OB_ERR_SYS;
        LOG_ERROR("cannot build store with too many tables", K(ret), K(table_count_));
      } else if (!major_table->is_sstable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("major sstable must be sstable", K(ret), K(*major_table));
      } else {
        tables_[table_count_++] = major_table;
        major_table->inc_ref();
        ++start_pos_;
        ++inc_pos_;
        LOG_INFO("add backup major table", K(*major_table));
      }
    }
  }
  return ret;
}

int ObTableStore::inner_add_table(ObIArray<ObITable*>& tables, const int64_t start)
{
  int ret = OB_SUCCESS;

  if (tables.empty()) {
    // do nothing
  } else if (start < 0 || start >= tables.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(start), K(tables));
  } else {
    for (int64_t i = start; OB_SUCC(ret) && i < tables.count(); ++i) {
      if (table_count_ >= MAX_SSTABLE_CNT_IN_STORAGE) {
        ret = OB_ERR_SYS;
        LOG_ERROR("cannot build store with too many tables", K(ret), K(table_count_));
      } else if (tables.at(i)->is_sstable()) {
        tables_[table_count_++] = tables.at(i);
        tables.at(i)->inc_ref();
      }
    }
  }
  return ret;
}

int ObTableStore::check_ready_for_read()
{
  int ret = OB_SUCCESS;
  int64_t last_snapshot_version = 0;
  is_ready_for_read_ = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store not is_inited_", K(ret), K(PRETTY_TS(*this)));
  } else if (inc_pos_ > table_count_ || table_count_ < 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Invalid pos or table_count", K(ret), K(PRETTY_TS(*this)));
  } else if (start_pos_ < 0 || inc_pos_ - start_pos_ <= 0) {
    is_ready_for_read_ = false;
    LOG_INFO("no valid major sstable, not ready for read", K(PRETTY_TS(*this)));
  } else {
    // check major tables
    for (int64_t i = start_pos_; OB_SUCC(ret) && is_ready_for_read_ && i < inc_pos_; ++i) {
      ObITable* table = tables_[i];
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unexpected null table", K(ret), KP(table), K(i), K(PRETTY_TS(*this)));
      } else if (!table->is_major_sstable() && !table->is_trans_sstable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must be major sstable", K(ret), K(i), K(PRETTY_TS(*this)));
      } else if (table->get_base_version() != 0) {
        is_ready_for_read_ = false;
        LOG_INFO("major table not ready for read", K(ret), K(i), K(table->get_base_version()), K(PRETTY_TS(*this)));
      }
    }

    // check if LogTsRanges are consecutive between all minor sstables
    ObITable* last_table = nullptr;
    for (int64_t i = inc_pos_; is_ready_for_read_ && OB_SUCC(ret) && i < table_count_; ++i) {
      ObITable* table = tables_[i];
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unexpected null table", K(ret), KP(table), K(i), K(PRETTY_TS(*this)));
      } else if (!table->is_minor_sstable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table type not valid", K(ret), K(i), K(*table), K(PRETTY_TS(*this)));
      } else if (OB_ISNULL(last_table)) {
      } else if (table->get_start_log_ts() > last_table->get_end_log_ts()) {
        is_ready_for_read_ = false;
        LOG_INFO("table log_id range not continuous", K(i), KPC(table), KPC(last_table), K(PRETTY_TS(*this)));
      } else if (table->get_end_log_ts() < last_table->get_end_log_ts()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("inc table end_log_ts sequence not right", K(ret), K(i), K(PRETTY_TS(*this)));
      } else if (table->get_end_log_ts() == last_table->get_end_log_ts()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("inc table snapshot_version sequence not right", K(ret), K(i), K(PRETTY_TS(*this)));
      }
      last_table = table;
    }
    if (OB_FAIL(ret)) {
      is_ready_for_read_ = false;
    }
  }
  return ret;
}

int ObTableStore::get_next_major_merge_info(const int merge_version, int64_t& need_base_table_pos,
    int64_t& next_major_version, int64_t& next_major_freeze_ts, int64_t& base_schema_version, int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  ObFreezeInfoSnapshotMgr::FreezeInfo freeze_info;
  need_base_table_pos = -1;
  next_major_version = -1;
  next_major_freeze_ts = -1;

  if (inc_pos_ <= start_pos_ || start_pos_ < 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("major sstable not exist", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = inc_pos_ - 1; OB_SUCC(ret) && i >= start_pos_; --i) {
      ObITable* table = tables_[i];
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unexpected null table", K(ret), KP(table), K(i), K(PRETTY_TS(*this)));
      } else if (table->is_major_sstable()) {
        if (merge_version <= table->get_version()) {
          ret = OB_NO_NEED_MERGE;
          FLOG_INFO("major merge already finished", K(ret), KPC(table), K(merge_version));
        } else {
          need_base_table_pos = i;
          next_major_version = table->get_version().major_ + 1;
          if (OB_FAIL(table->get_frozen_schema_version(base_schema_version))) {
            LOG_WARN("Failed to get frozen schema version", K(ret));
          }
        }
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (-1 == need_base_table_pos) {
        ret = OB_ENTRY_NOT_EXIST;
        ObTaskController::get().allow_next_syslog();
        LOG_WARN("major sstable not exist", K(ret), K(PRETTY_TS(*this)));
      } else if (OB_FAIL(freeze_info_mgr_->get_freeze_info_by_major_version(
                     get_table_id(), next_major_version, freeze_info))) {
        LOG_WARN("failed to get freeze info", K(ret), K(next_major_version));
      } else {
        next_major_freeze_ts = freeze_info.freeze_ts;
        schema_version = freeze_info.schema_version;
      }
    }
  }
  return ret;
}

int ObTableStore::get_major_merge_tables(const ObGetMergeTablesParam& param, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  int64_t next_major_version = 0;
  int64_t next_major_freeze_ts = 0;
  int64_t need_base_table_pos = -1;
  ObSSTable* base_table = NULL;
  result.reset();
  result.merge_version_ = param.merge_version_;
  result.suggest_merge_type_ = MAJOR_MERGE;
  DEBUG_SYNC(BEFORE_GET_MAJOR_MGERGE_TABLES);

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else if (!param.is_major_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get major merge tables", K(ret), K(param));
  } else if (OB_FAIL(get_next_major_merge_info(result.merge_version_,
                 need_base_table_pos,
                 next_major_version,
                 next_major_freeze_ts,
                 result.base_schema_version_,
                 result.schema_version_))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get latest major sstable for merge", K(ret));
    }
  } else {
    if (OB_ISNULL(base_table = static_cast<ObSSTable*>(tables_[need_base_table_pos]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null table", K(ret), KP(base_table), K(need_base_table_pos), K(PRETTY_TS(*this)));
    } else if (next_major_version > param.merge_version_) {
      ret = OB_EAGAIN;
      LOG_INFO("major version cannot merge not", K(ret), K(next_major_version), K(param), K(PRETTY_TS(*this)));
    } else if (OB_FAIL(result.handle_.add_table(base_table))) {
      LOG_WARN("failed to add table", K(ret));
    } else if (OB_FAIL(result.base_handle_.set_table(base_table))) {
      LOG_WARN("failed to set base table", K(ret));
    } else if (base_table->get_snapshot_version() >= next_major_freeze_ts) {
    } else if (OB_FAIL(find_major_merge_inc_tables(*base_table, next_major_freeze_ts, result.handle_))) {
      LOG_WARN("failed to find_major_merge_inc_tables", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // TODO  simplify large snapshot major sstable logic, reduce code
    if (result.handle_.get_count() < 2) {
      if (base_table->get_snapshot_version() < next_major_freeze_ts) {
        ret = OB_NO_NEED_MERGE;
        LOG_WARN("cannot major merge tables",
            K(ret),
            K(param),
            K(next_major_version),
            K(next_major_freeze_ts),
            K(result),
            K(PRETTY_TS(*this)));
      } else if (!base_table->is_major_sstable() || base_table->get_total_row_count() != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable with snapshot_version larger than next_major_freeze_ts should be empty",
            K(ret),
            K(*base_table),
            K(next_major_freeze_ts));
      } else {
        result.create_sstable_for_large_snapshot_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t major_snapshot = std::max(base_table->get_snapshot_version(), next_major_freeze_ts);
      result.read_base_version_ = base_table->get_snapshot_version();
      result.version_range_.multi_version_start_ = major_snapshot;
      result.version_range_.base_version_ = base_table->get_base_version();
      result.version_range_.snapshot_version_ = major_snapshot;
      result.create_snapshot_version_ = base_table->get_meta().create_snapshot_version_;
      result.checksum_method_ = base_table->get_meta().checksum_method_;
      result.suggest_merge_type_ = MAJOR_MERGE;
    }
  }

  if (OB_SUCC(ret)) {
    if (result.handle_.get_count() > 1 && OB_FAIL(result.handle_.check_continues(nullptr))) {
      LOG_WARN("failed to check continues for major merge",
          K(ret),
          K(param),
          K(next_major_version),
          K(next_major_freeze_ts),
          K(result),
          K(PRETTY_TS(*this)));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("succeed to get_major_merge_tables",
          K(param),
          K(next_major_version),
          K(next_major_freeze_ts),
          K(result),
          K(PRETTY_TS(*this)));
    }
  }

  return ret;
}

int ObTableStore::find_major_merge_inc_tables(
    const ObSSTable& base_table, const int64_t next_major_freeze_ts, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  bool start_add_table_flag = false;
  bool need_add_complement_table_flag = true;
  for (int64_t i = inc_pos_; OB_SUCC(ret) && i < table_count_; ++i) {
    ObITable* table = tables_[i];
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null table", K(ret), KP(table), K(i), K(PRETTY_TS(*this)));
    } else if (base_table.get_partition_key() != table->get_partition_key()) {
      // When the split target partition merges the sstable of the source partition, it cannot bring its own sstables
      continue;
    } else if (table->get_base_version() >= next_major_freeze_ts) {
      need_add_complement_table_flag = false;  // no need to add complement sstable
      break;
    } else if (!start_add_table_flag && table->get_upper_trans_version() >= base_table.get_snapshot_version()) {
      // Since the backfill of upper_trans_version is asynchronous,
      // it may appear that upper_trans_version of a table which contains uncommitted transactions is MAX
      // Some subsequent tables do not contain uncommitted transactions,
      // and the tables after MAX are not recycled when the Major SSTable overwrites the table
      // All tables after MAX must be added to the handle, otherwise there may be a log_ts_range hole
      start_add_table_flag = true;
    }
    if (OB_SUCC(ret) && start_add_table_flag) {
      if (OB_FAIL(handle.add_table(table))) {
        LOG_WARN("failed to add table", K(ret));
      }
    }
  }                                                      // end for
  if (OB_SUCC(ret) && need_add_complement_table_flag) {  // add complement SSTable
    if (OB_NOT_NULL(complement_minor_sstable_) && OB_FAIL(handle.add_table(complement_minor_sstable_))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }
  FLOG_INFO("find_major_merge_inc_tables_", K(base_table), K(next_major_freeze_ts), K(handle));
  return ret;
}

int ObTableStore::add_minor_merge_result(ObITable* table, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to add minor merge result", KP(table), K(ret));
  } else if (result.handle_.empty()) {
    result.version_range_.base_version_ = table->get_base_version();
    result.version_range_.multi_version_start_ = table->get_multi_version_start();
    result.log_ts_range_.start_log_ts_ = table->get_start_log_ts();
  } else if (is_multi_version_break(table->get_version_range(), result.version_range_.snapshot_version_)) {
    // multi version not continuous, refer to update_multi_version_start function
    FLOG_INFO("Push up multi version start due to uncontinuous range", K(result.version_range_), KPC(table));
    result.version_range_.multi_version_start_ = table->get_multi_version_start();
  }
  if (OB_SUCC(ret)) {
    result.version_range_.snapshot_version_ =
        std::max(table->get_snapshot_version(), result.version_range_.snapshot_version_);
    result.log_ts_range_.end_log_ts_ = table->get_end_log_ts();
    result.log_ts_range_.max_log_ts_ = std::max(table->get_end_log_ts(), table->get_max_log_ts());
    if (OB_FAIL(result.handle_.add_table(table))) {
      LOG_WARN("Failed to add table", K(*table), K(ret));
    } else {
      result.dump_memtable_timestamp_ = MAX(result.dump_memtable_timestamp_, table->get_timestamp());
    }
  }
  return ret;
}

// Used to adjust whether to do L0 Minor merge or L1 Minor merge
int ObTableStore::refine_mini_minor_merge_result(ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;

  if (result.handle_.empty()) {
  } else if (MINI_MINOR_MERGE != result.suggest_merge_type_ && HISTORY_MINI_MINOR_MERGE != result.suggest_merge_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected merge type to refine merge tables", K(result), K(ret));
  } else {
    common::ObSEArray<ObITable*, MAX_SSTABLE_CNT_IN_STORAGE> mini_tables;
    ObITable* table = NULL;
    ObSSTable* sstable = NULL;
    int64_t mini_sstable_size = 1;
    int64_t minor_sstable_size = 1;
    int64_t minor_sstable_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_tables().count(); i++) {
      if (OB_ISNULL(table = result.handle_.get_tables().at(i))) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unexpected null table", KP(table), K(ret));
      } else if (table->is_memtable() || table->is_major_sstable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unexpected table type", K(*table), K(ret));
      } else if (FALSE_IT(sstable = reinterpret_cast<ObSSTable*>(table))) {
      } else if (table->is_mini_minor_sstable()) {
        mini_sstable_size += sstable->get_total_row_count();
        if (OB_FAIL(mini_tables.push_back(table))) {
          LOG_WARN("Failed to push mini minor table into array", K(ret));
        }
      } else if (table->is_multi_version_minor_sstable()) {  // not include buf minor sstable
        if (mini_tables.count() > 0) {
          mini_tables.reset();
          LOG_INFO("minor refine, minor merge sstable refine to minor merge due to chaos table order", K(result));
          break;
        } else {
          minor_sstable_size += sstable->get_total_row_count();
          minor_sstable_count++;
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t size_amplification_factor = GCONF._minor_compaction_amplification_factor;
      int64_t min_minor_sstable_row_count = OB_MIN_MINOR_SSTABLE_ROW_COUNT;
      int64_t minor_compact_trigger = GCONF.minor_compact_trigger;

      if (0 == size_amplification_factor) {
        size_amplification_factor = OB_DEFAULT_COMPACTION_AMPLIFICATION_FACTOR;
      }
      if (1 == result.handle_.get_count()) {
        // only one minor sstable, no need mini minor merge
        LOG_INFO("minor refine, only one mini sstable, no need to do mini minor merge", K(result));
        result.handle_.reset();
      } else if (HISTORY_MINI_MINOR_MERGE == result.suggest_merge_type_) {
        // use minor merge to do history mini minor merge and skip other checks
      } else if (0 == minor_compact_trigger) {
        // produce L1 minor sstable when minor_compact_tigger is 0
        result.suggest_merge_type_ = MINOR_MERGE;
      } else if (mini_tables.empty() || minor_sstable_count > 1) {
        // no mini sstable or exist chaos sstable order or 2 more L1 sstable
        result.suggest_merge_type_ = MINOR_MERGE;
        LOG_INFO("minor refine, no mini sstables, mini minor merge sstable refine to minor merge",
            K(result),
            K(minor_sstable_count));
      } else if (1 == mini_tables.count()) {
        // only one mini sstable, and 1 L1 sstable at last, no need mini minor merge
        LOG_INFO("minor refine, only one mini sstable, no need to do mini minor merge", K(result));
        result.handle_.reset();
      } else if (minor_sstable_count == 0 && mini_sstable_size > min_minor_sstable_row_count) {
        result.suggest_merge_type_ = MINOR_MERGE;
        LOG_INFO("minor refine, mini minor merge sstable refine to minor merge",
            K(minor_sstable_size),
            K(mini_sstable_size),
            K(min_minor_sstable_row_count),
            K(result));
      } else if (minor_sstable_count == 1 &&
                 mini_sstable_size > (minor_sstable_size * size_amplification_factor / 100)) {
        result.suggest_merge_type_ = MINOR_MERGE;
        LOG_INFO("minor refine, mini minor merge sstable refine to minor merge",
            K(minor_sstable_size),
            K(mini_sstable_size),
            K(size_amplification_factor),
            K(result));
      } else {
        // reset the merge result, mini sstable merge into a new mini sstable
        result.handle_.reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < mini_tables.count(); i++) {
          if (OB_ISNULL(table = mini_tables.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("Unexpected null table", KP(table), K(ret));
          } else if (OB_FAIL(add_minor_merge_result(table, result))) {
            LOG_WARN("Failed to add table to minor merge result", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("minor refine, mini minor merge sstable refine info",
              K(minor_sstable_size),
              K(mini_sstable_size),
              K(result));
        }
      }
    }
  }

  return ret;
}

int ObTableStore::get_mini_merge_tables(
    const ObGetMergeTablesParam& param, const int64_t demand_multi_version_start, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  int64_t merge_inc_base_version = 0;
  const ObMergeType merge_type = param.merge_type_;
  ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite freeze_info;
  result.reset();
  DEBUG_SYNC(BEFORE_GET_MINOR_MGERGE_TABLES);

  if (OB_UNLIKELY(!is_valid()) &&
      OB_UNLIKELY(nullptr != pg_memtable_mgr_ && 0 == pg_memtable_mgr_->get_memtable_count())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else if (MINI_MERGE != merge_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(merge_type));
  } else if (OB_FAIL(cal_mini_merge_inc_base_version(merge_inc_base_version))) {
    LOG_WARN("failed to cal_minor_merge_base_version", K(ret), K(table_id_));
  } else if (merge_inc_base_version <= 0) {
    ret = OB_NO_NEED_MERGE;
    STORAGE_LOG(WARN, "Unexpected empty table store to minor merge", K(ret), K(*this));
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version, freeze_info))) {
    LOG_WARN("failed to get next major freeze", K(ret), K(merge_inc_base_version), K(*this));
  } else if (OB_FAIL(find_mini_merge_tables(param, freeze_info, result))) {
    LOG_WARN("Failed to find minor merge tables", K(ret), K(freeze_info));
  } else if (OB_FAIL(deal_with_minor_result(merge_type, demand_multi_version_start, result))) {
    LOG_WARN("Failed to deal with minor merge result", K(ret), K(merge_type), K(demand_multi_version_start), K(result));
  } else {
    FLOG_INFO("succeed to get_mini_merge_tables",
        K(demand_multi_version_start),
        K(merge_inc_base_version),
        K(freeze_info),
        K(result),
        K(PRETTY_TS(*this)));
  }
  return ret;
}

int ObTableStore::deal_with_minor_result(
    const ObMergeType& merge_type, const int64_t demand_multi_version_start, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  if (result.handle_.empty()) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("no need to minor merge", K(ret), K(merge_type), K(result), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(check_continues_and_get_schema_version(merge_type, result))) {
    LOG_WARN("failed to check continues and get schema version", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(cal_multi_version_start(demand_multi_version_start, result.version_range_))) {
    LOG_WARN("Failed to calculate minor merge multi version start", K(ret), K(demand_multi_version_start), K(result));
  }
  return ret;
}

int ObTableStore::get_mini_minor_merge_tables(
    const ObGetMergeTablesParam& param, const int64_t multi_version_start, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = param.merge_type_;
  int64_t merge_inc_base_version = 0;
  ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite freeze_info;
  result.reset();
  DEBUG_SYNC(BEFORE_GET_MINOR_MGERGE_TABLES);

  if (OB_UNLIKELY(!is_valid()) &&
      OB_UNLIKELY(nullptr != pg_memtable_mgr_ && 0 == pg_memtable_mgr_->get_memtable_count())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else if (MINI_MINOR_MERGE != merge_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(merge_type));
  } else if (OB_FAIL(cal_minor_merge_inc_base_version(merge_inc_base_version))) {
    LOG_WARN("failed to cal_minor_merge_base_version", K(ret), K(table_id_));
  } else if (merge_inc_base_version <= 0) {
    ret = OB_NO_NEED_MERGE;
    STORAGE_LOG(WARN, "Unexpected empty table store to minor merge", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version, freeze_info))) {  // get freeze info
    LOG_WARN("failed to get freeze info", K(ret), K(merge_inc_base_version), K(PRETTY_TS(*this)));
  } else {
    const int64_t min_snapshot_version = freeze_info.prev.freeze_ts;
    // In the primary and standby database scenarios,
    // the freeze_info of the standby library may not be refreshed for a long time,
    // but then drag a minor from the primary database to update the major version.
    // At this time, the multi_version is not continuous.
    // If directly do mini minor merge, it will cause the loss of the previous major data,
    // so the mini minor merge should also use snapshot_gc_ts as the boundary
    // Unless the multi_version exceeding snapshot_gc_ts is continuous
    const int64_t max_snapshot_version = freeze_info.next.freeze_version > 0 ? freeze_info.next.freeze_ts : INT64_MAX;
    const int64_t expect_multi_version = MIN(freeze_info.next.freeze_ts, multi_version_start);
    if (OB_FAIL(find_mini_minor_merge_tables(
            param, min_snapshot_version, max_snapshot_version, expect_multi_version, result))) {
      LOG_WARN("Failed to find mini minor merge tables",
          K(ret),
          K(merge_type),
          K(min_snapshot_version),
          K(max_snapshot_version),
          K(freeze_info));
    } else if (OB_FAIL(deal_with_minor_result(merge_type, expect_multi_version, result))) {
      LOG_WARN("Failed to deal with minor merge result",
          K(ret),
          K(merge_type),
          K(expect_multi_version),
          K(multi_version_start),
          K(result));
    } else {
      FLOG_INFO("succeed to get mini minor merge tables",
          K(merge_type),
          K(expect_multi_version),
          K(multi_version_start),
          K(merge_inc_base_version),
          K(freeze_info),
          K(result),
          K(PRETTY_TS(*this)));
    }
  }

  return ret;
}

int ObTableStore::get_hist_minor_range(const ObIArray<ObFreezeInfoSnapshotMgr::FreezeInfoLite>& freeze_infos,
    const bool is_optimize, int64_t& min_snapshot_version, int64_t& max_snapshot_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(freeze_infos.count() <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get hist minor range", K(ret), K(freeze_infos));
  } else {
    int64_t cnt = 0;
    int64_t last_freeze_ts = freeze_infos.at(0).freeze_ts;
    int64_t idx = 1;
    int64_t max_cnt = 1;
    int64_t max_idx = 0;
    ObITable* table = nullptr;
    for (int64_t pos = inc_pos_; OB_SUCC(ret) && idx < freeze_infos.count() && pos < table_count_; pos++) {
      int64_t freeze_ts = freeze_infos.at(idx).freeze_ts;
      if (OB_ISNULL(table = tables_[pos])) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unexpected null table", K(ret), K(pos), K(PRETTY_TS(*this)));
      } else if (table->get_base_version() < last_freeze_ts) {
        // skip small minor sstable
      } else if (table->get_max_merged_trans_version() > freeze_ts) {
        if (cnt > max_cnt) {
          max_cnt = cnt;
          max_idx = idx;
          if (!is_optimize) {
            break;
          }
        }
        // change new major freeze info
        last_freeze_ts = freeze_ts;
        idx++;
        cnt = 0;
      } else {
        cnt++;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (max_idx > 0) {
      min_snapshot_version = freeze_infos.at(max_idx - 1).freeze_ts;
      max_snapshot_version = freeze_infos.at(max_idx).freeze_ts;
    } else {
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("cannot find suitable hist minor merge range", K(ret));
    }
  }

  return ret;
}

int ObTableStore::get_hist_minor_merge_tables(
    const ObGetMergeTablesParam& param, const int64_t multi_version_start, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = param.merge_type_;
  const int64_t hist_threashold = cal_hist_minor_merge_threshold();
  int64_t min_snapshot_version = 0;
  int64_t max_snapshot_version = 0;
  int64_t expect_multi_version = 0;
  ObITable* first_major_table = nullptr;
  result.reset();

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else if (HISTORY_MINI_MINOR_MERGE != merge_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(merge_type));
  } else if (table_count_ - inc_pos_ < hist_threashold) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("No need to do hist mini minor merge", K(ret), K_(table_count), K_(inc_pos), K(hist_threashold));
  } else if (OB_FAIL(get_boundary_major_sstable(false, first_major_table))) {
    LOG_WARN("Failed to get first major sstable", K(ret));
  } else if (OB_ISNULL(first_major_table)) {
    ObITable* table = nullptr;
    // index table during building, need compat with continuous multi version
    if (0 == (max_snapshot_version = freeze_info_mgr_->get_latest_frozen_timestamp())) {
      // no freeze info found, wait normal mini minor to free sstable
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No freeze range to do hist minor merge for buiding index", K(ret), K(PRETTY_TS(*this)));
    } else if (OB_FAIL(get_boundary_minor_sstable(false, table))) {
      LOG_WARN("Failed to get first minor sstble", K(ret));
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null minor sstable", K(ret), K(PRETTY_TS(*this)));
    } else {
      // index without major should keep all multi version
      min_snapshot_version = 0;
      expect_multi_version = MIN(multi_version_start, table->get_multi_version_start());
    }
  } else {
    ObSEArray<ObFreezeInfoSnapshotMgr::FreezeInfoLite, 8> freeze_infos;
    if (OB_FAIL(freeze_info_mgr_->get_freeze_info_behind_major_version(
            first_major_table->get_version().major_, freeze_infos))) {
      LOG_WARN("Failed to get freeze infos behind major version", K(ret), KPC(first_major_table));
    } else if (freeze_infos.count() <= 1) {
      // only one major freeze found, wait normal mini minor to reduce table count
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No enough freeze range to do hist minor merge", K(ret), K(freeze_infos));
    } else if (OB_FAIL(get_hist_minor_range(freeze_infos, true, min_snapshot_version, max_snapshot_version))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("Failed to get hist minor range", K(ret), K(freeze_infos));
      }
    } else {
      expect_multi_version = MIN(max_snapshot_version, multi_version_start);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(find_mini_minor_merge_tables(
            param, min_snapshot_version, max_snapshot_version, expect_multi_version, result))) {
      LOG_WARN("Failed to find mini minor merge tables",
          K(ret),
          K(merge_type),
          K(min_snapshot_version),
          K(max_snapshot_version),
          K(expect_multi_version));
    } else if (OB_FAIL(deal_with_minor_result(merge_type, expect_multi_version, result))) {
      LOG_WARN("Failed to deal with minor merge result",
          K(ret),
          K(merge_type),
          K(expect_multi_version),
          K(multi_version_start),
          K(result));
    } else {
      FLOG_INFO("succeed to get hist minor merge table",
          K(merge_type),
          K(expect_multi_version),
          K(multi_version_start),
          K(min_snapshot_version),
          K(max_snapshot_version),
          K(result),
          K(PRETTY_TS(*this)));
    }
  }
  return ret;
}

int ObTableStore::check_continues_and_get_schema_version(const ObMergeType& merge_type, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  ObITable* table = nullptr;

  if (OB_UNLIKELY(!result.log_ts_range_.is_valid() || result.handle_.empty())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Invalid argument to check result", K(ret), K(result));
  } else if (OB_FAIL(result.handle_.check_continues(&result.log_ts_range_))) {
    // check continues for log_ts_range and version_range
    LOG_WARN("failed to check continues", K(ret), K(result), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(get_minor_schema_version(result.schema_version_))) {
    LOG_WARN("failed to get minor sschema version", K(ret), K(PRETTY_TS(*this)));
  } else if (merge_type == MINI_MERGE) {
    result.base_schema_version_ = result.schema_version_;
  } else if (OB_ISNULL(table = result.handle_.get_table(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null table", K(ret), K(result.handle_));
  } else if (OB_FAIL(table->get_frozen_schema_version(result.base_schema_version_))) {
    LOG_WARN("failed to get frozen schema version", K(ret), KPC(table));
  }
  return ret;
}

int ObTableStore::is_memtable_need_merge(const ObMemtable& memtable, bool& need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  // when big transaction feature becomes stable, we could just use end_log_ts to judge need_merge
  if (table_count_ > start_pos_ && start_pos_ >= 0) {
    ObITable* table = nullptr;
    if (OB_ISNULL(table = tables_[table_count_ - 1])) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Unexpected null sstable", K(ret), K(PRETTY_TS(*this)));
    } else if (table->is_major_sstable()) {
      if (OB_NOT_NULL(complement_minor_sstable_)) {
        // major sstable may cover all the mini sstable, so we need check complement_minor_sstable_
        if (complement_minor_sstable_->get_end_log_ts() >= memtable.get_end_log_ts() &&
            complement_minor_sstable_->get_timestamp() >= memtable.get_timestamp()) {
          need_merge = false;
        } else {
          need_merge = true;
        }
      } else if (memtable.get_upper_trans_version() <= table->get_snapshot_version()) {
        need_merge = false;
      } else {
        need_merge = true;
      }
    } else if (memtable.get_end_log_ts() > table->get_end_log_ts()) {
      need_merge = true;
      // all the defend code below will be useless
    } else if (memtable.get_max_log_ts() <= table->get_end_log_ts()) {
      // memtable not released
    } else if (memtable.get_end_log_ts() == table->get_end_log_ts()) {
      // there are two situations for memtable log ts
      // 1. during reboot max_log_ts == end_log_ts
      // 2. max_log_ts >= end_log_ts >= last_minor.max_log_ts
      if (memtable.get_max_log_ts() == memtable.get_end_log_ts()) {
      } else if (memtable.get_max_log_ts() != table->get_max_log_ts() ||
                 memtable.get_timestamp() > table->get_timestamp()) {
        ret = OB_ERR_UNEXPECTED;
        ;
        LOG_ERROR("Unexpected memtable cross with sstable", K(ret), K(memtable), KPC(table), K(PRETTY_TS(*this)));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      ;
      LOG_ERROR("Unexpected memtable cross with sstable", K(ret), K(memtable), KPC(table), K(PRETTY_TS(*this)));
    }
  } else {
    need_merge = true;
  }
  LOG_DEBUG("check memtable need merge", K(memtable), K(need_merge), K(PRETTY_TS(*this)));

  return ret;
}

int ObTableStore::find_mini_merge_tables(const ObGetMergeTablesParam& param,
    const ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite& freeze_info, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  int64_t max_snapshot_version = INT64_MAX;
  ObTablesHandle frozen_memtables;
  result.handle_.reset();
  result.version_range_.reset();
  const ObMergeType merge_type = param.merge_type_;
  int64_t reserve_snapshot_for_major = INT64_MAX;

  // Freezing in the restart phase may not satisfy end >= last_max_sstable,
  // so the memtable cannot be filtered by log_ts
  // can only take out all frozen memtable
  if (OB_UNLIKELY(MINI_MERGE != merge_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected merge type to find mini merge tables", K(ret), K(merge_type), K(param));
  } else if (OB_FAIL(get_all_memtables(false /*include_active_memtable*/, frozen_memtables))) {
    LOG_WARN("failed to get all tables", K(ret), K(PRETTY_TS(*this)));
  } else if (freeze_info.next.freeze_version > 0) {
    max_snapshot_version = freeze_info.next.freeze_ts;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < frozen_memtables.get_count(); ++i) {
    memtable::ObMemtable* memtable = static_cast<memtable::ObMemtable*>(frozen_memtables.get_table(i));
    bool need_merge = false;
    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret), K(i), K(PRETTY_TS(*this)));
    } else if (!is_own_table(memtable) && memtable->get_partition_key() != pg_memtable_mgr_->get_pkey()) {
      continue;
    } else if (memtable->is_active_memtable()) {
      // only frozen memtable can do mini merge
      LOG_INFO("skip active memtable", K(i), KPC(memtable));
      break;
    } else if (!memtable->can_be_minor_merged()) {
      FLOG_INFO("memtable cannot mini merge now", K(ret), K(i), KPC(memtable), K(max_snapshot_version), K(param));
      break;
    } else if (memtable->get_end_log_ts() > param.trans_table_end_log_ts_ ||
               memtable->get_timestamp() > param.trans_table_timestamp_) {
      // mini merge of trans_table need be finished before
      ObPartitionKey trans_table_pkey;
      IGNORE_RETURN trans_table_pkey.generate_trans_table_pkey(pkey_);
      LOG_WARN("wait trans sstable merge",
          K(ret),
          K(i),
          K(trans_table_pkey),
          K(max_snapshot_version),
          K(param),
          KPC(memtable));
      break;
    } else if ((OB_FAIL(is_memtable_need_merge(*memtable, need_merge)))) {
      LOG_WARN("Failed to check memtable need merge", K(i), K(need_merge), KPC(memtable));
    } else if (!need_merge) {
      LOG_DEBUG("memtable wait to release", K(i), K(param), KPC(memtable));
      continue;
    } else if (result.handle_.get_count() > 0) {
      if (result.log_ts_range_.end_log_ts_ < memtable->get_start_log_ts() ||
          result.log_ts_range_.max_log_ts_ > memtable->get_end_log_ts()) {
        FLOG_INFO("log id not continues, reset previous minor merge tables",
            K(i),
            "last_end_log_ts",
            result.log_ts_range_.end_log_ts_,
            "last_max_log_ts",
            result.log_ts_range_.max_log_ts_,
            KPC(memtable));
        // mini merge always use the oldest memtable to dump
        break;
      } else if (memtable->get_snapshot_version() > max_snapshot_version) {
        // This judgment is only to try to prevent cross-major mini merge,
        // but when the result is empty, it can still be added
        LOG_INFO("max_snapshot_version is reached, no need find more tables",
            K(i),
            K(merge_type),
            K(max_snapshot_version),
            KPC(memtable));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_minor_merge_result(memtable, result))) {
        LOG_WARN("Failed to add table to minor merge result", K(ret));
      } else {
        LOG_INFO("add candidate table", KPC(memtable));
      }
    }
  }

  if (OB_SUCC(ret)) {
    result.suggest_merge_type_ = merge_type;
    if (OB_FAIL(refine_mini_merge_result(result))) {
      LOG_WARN("failed to refine_minor_merge_result", K(ret), K(frozen_memtables));
    }
  }

  return ret;
}

int ObTableStore::find_mini_minor_merge_tables(const ObGetMergeTablesParam& param, const int64_t min_snapshot_version,
    const int64_t max_snapshot_version, const int64_t expect_multi_version, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  ObTablesHandle all_tables;
  result.handle_.reset();
  result.version_range_.reset();
  const int64_t inc_pos = inc_pos_ >= 0 ? inc_pos_ : 0;
  const ObMergeType merge_type = param.merge_type_;
  int64_t reserve_snapshot_for_major = INT64_MAX;
  LOG_INFO("find_mini_minor_merge_tables",
      K(ret),
      K(min_snapshot_version),
      K(max_snapshot_version),
      K(expect_multi_version),
      K(PRETTY_TS(*this)));
  if (OB_FAIL(get_all_sstables(false /*need_complent*/, all_tables))) {
    LOG_WARN("failed to get all tables", K(ret), K(PRETTY_TS(*this)));
  }
  for (int64_t i = inc_pos; OB_SUCC(ret) && i < all_tables.get_count(); ++i) {
    ObITable* table = all_tables.get_table(i);
    if (OB_UNLIKELY(nullptr == table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret), K(i), K(PRETTY_TS(*this)));
    } else if (OB_UNLIKELY(table->is_memtable())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("meet memtable in sstable handles", K(ret), K(i), K(all_tables), KPC(PRETTY_TS_P(this)));
    } else if (!is_own_table(table) && table->get_partition_key() != pg_memtable_mgr_->get_pkey()) {
      continue;
    } else if (table->get_base_version() < min_snapshot_version) {
      continue;
    } else if (is_multi_version_break(table->get_version_range(), result.version_range_.snapshot_version_) &&
               table->get_multi_version_start() >= expect_multi_version) {
      if (result.handle_.get_count() > 1) {
        // do not involve sstable with bigger uncontinue multi version than max_snapshot_version
        FLOG_INFO("Multi version start larger than max snapshot, stop find more minor sstables",
            K(i),
            K(expect_multi_version),
            KPC(table));
        break;
      } else {
        FLOG_INFO("Multi version start larger than max snapshot, reset and find sstables behind",
            K(i),
            K(expect_multi_version),
            KPC(table));
        result.handle_.reset();
        result.version_range_.reset();
        result.log_ts_range_.reset();
      }
    }
    if (OB_SUCC(ret)) {
      if (result.handle_.get_count() > 0) {
        if (result.log_ts_range_.end_log_ts_ < table->get_start_log_ts()) {
          LOG_INFO("log id not continues, reset previous minor merge tables",
              K(i),
              "last_end_log_ts",
              result.log_ts_range_.end_log_ts_,
              K(*table));
          result.handle_.reset();
          result.version_range_.reset();
          result.log_ts_range_.reset();
        } else if (table->get_max_merged_trans_version() > max_snapshot_version) {
          // snapshot_version <= max_merged_trans_version <= upper_trans_version
          // upper_trans_version is more safe to keep the minor sstable do not
          // crossing the major freeze, but it's not always properly filled.
          // so max_merged_trans_version is the only safe choice
          LOG_INFO("max_snapshot_version is reached, no need find more tables",
              K(i),
              K(merge_type),
              K(max_snapshot_version),
              K(*table));
          break;
        }
      }
      LOG_INFO("add candidate table", K(*table));
      if (OB_FAIL(add_minor_merge_result(table, result))) {
        LOG_WARN("Failed to add table to minor merge result", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    result.suggest_merge_type_ = merge_type;
    if (OB_FAIL(refine_mini_minor_merge_result(result))) {
      LOG_WARN("failed to refine_minor_merge_result", K(ret));
    }
  }

  return ret;
}

int ObTableStore::refine_mini_merge_result(ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Table store not inited", K(ret));
  } else if (result.suggest_merge_type_ != MINI_MERGE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to refine ", K(ret), K(result));
  } else if (result.handle_.empty()) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("no need minor merge", K(ret), K(result), K(PRETTY_TS(*this)));
  } else if (table_count_ > 0 && tables_[table_count_ - 1]->is_minor_sstable()) {
    ObITable* last_table = tables_[table_count_ - 1];
    if (result.log_ts_range_.start_log_ts_ > last_table->get_end_log_ts()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpected uncontinuous log_ts_range in mini merge", K(ret), K(result), KPC(last_table));
    } else if (result.log_ts_range_.start_log_ts_ < last_table->get_end_log_ts()) {
      // reboot phase
      if (OB_NOT_NULL(complement_minor_sstable_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("Unexpected complement minor sstable during reboot", K(ret), K(PRETTY_TS(*this)));
      } else if (OB_FAIL(refine_mini_merge_result_in_reboot_phase(*last_table, result))) {
        LOG_WARN(
            "failed to refine mini merge in reboot phase", K(ret), K(result), KPC(last_table), K(PRETTY_TS(*this)));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_complement_to_mini_merge_result(result))) {
      LOG_WARN("failed to refine mini merge with complement", K(ret), K(result), K(PRETTY_TS(*this)));
    }
  }

  return ret;
}

int ObTableStore::refine_mini_merge_result_in_reboot_phase(ObITable& last_table, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  const int64_t last_end_log_ts = last_table.get_end_log_ts();
  const int64_t last_max_log_ts = last_table.get_max_log_ts();
  const int64_t last_snapshot_version = last_table.get_snapshot_version();
  if (result.log_ts_range_.end_log_ts_ < last_max_log_ts &&
      result.log_ts_range_.end_log_ts_ != result.log_ts_range_.max_log_ts_) {
    // When the end_log_ts >= last_max_log_ts constraint is not met,
    // all memtables in this mini merge are considered to be in the reboot phase
    // In the reboot phase, frozen memtable must satisfy end_log_ts == max_log_ts
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("in reboot phase, end must equal to max", K(ret), K(result), K(PRETTY_TS(*this)));
  } else if (result.log_ts_range_.end_log_ts_ <= last_end_log_ts) {
    if (result.version_range_.snapshot_version_ > last_snapshot_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR(
          "Unexpected chaos snapshot_version and log_ts", K(ret), K(result), K(last_end_log_ts), K(PRETTY_TS(*this)));
    } else {
      ret = OB_NEED_REMOVE_UNNEED_TABLE;
      LOG_WARN("No need mini merge memtable which is covered by existing sstable",
          K(ret),
          K(result),
          K(last_end_log_ts),
          K(PRETTY_TS(*this)));
    }
  } else if (result.version_range_.snapshot_version_ <= last_snapshot_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR(
        "Unexpected chaos snapshot_version and log_ts", K(ret), K(result), K(last_end_log_ts), K(PRETTY_TS(*this)));
  } else {
    // fix start_log_ts to make log_ts_range continuous
    result.log_ts_range_.start_log_ts_ = MAX(last_end_log_ts, result.log_ts_range_.start_log_ts_);
    // fix base_version to make version_range continuous
    result.version_range_.base_version_ = MAX(last_snapshot_version, result.version_range_.base_version_);
    result.version_range_.multi_version_start_ =
        MAX(result.version_range_.base_version_, result.version_range_.multi_version_start_);
    FLOG_INFO("Fix mini merge result log ts range", K(ret), K(last_table), K(result));
  }
  return ret;
}

int ObTableStore::add_complement_to_mini_merge_result(ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(complement_minor_sstable_)) {
    ObTablesHandle tmp_handle;
    // keep complement minor sstable before memtable
    if (complement_minor_sstable_->get_timestamp() == 0) {
      // the complement_minor_sstable is replayed sstable, skip data
      FLOG_INFO("Ignore data of complement sstable during replay stage", KPC(complement_minor_sstable_));
    } else if (OB_FAIL(tmp_handle.add_table(complement_minor_sstable_))) {
      LOG_WARN("failed to add table", K(ret), K(result), K(*complement_minor_sstable_));
    } else if (OB_FAIL(tmp_handle.add_tables(result.handle_))) {
      LOG_WARN("failed to add table", K(ret), K(result), K(*complement_minor_sstable_));
    } else if (OB_FAIL(result.handle_.assign(tmp_handle))) {
      LOG_WARN("failed to assign result handle", K(ret), K(tmp_handle));
    }
  }
  return ret;
}

int ObTableStore::get_minor_schema_version(int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  ObTablesHandle all_tables;
  const uint64_t tenant_id = is_inner_table(table_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id_);
  int64_t tenant_schema_version = 0;
  schema_version = -1;

  if (OB_FAIL(get_all_tables(true /*include_active_memtable*/, true /*include_complement*/, all_tables))) {
    LOG_WARN("failed to get all tables", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = all_tables.get_count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      int64_t tmp_schema_version = 0;
      ObITable* table = all_tables.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret));
      } else if (table->is_memtable()) {
        tmp_schema_version = static_cast<ObMemtable*>(table)->get_max_schema_version();
      } else {
#ifdef ERRSIM
        ret = E(EventTable::EN_SKIP_GLOBAL_SSTABLE_SCHEMA_VERSION) ret;
        if (OB_FAIL(ret)) {
          ret = OB_SUCCESS;
          if (table_id_ == pkey_.get_table_id() &&
              static_cast<ObSSTable*>(table)->get_meta().create_snapshot_version_ == table->get_snapshot_version()) {
            LOG_INFO("skip global index major sstable", K(*table));
            continue;
          }
        }
#endif
        tmp_schema_version = static_cast<ObSSTable*>(table)->get_meta().schema_version_;
      }
      if (OB_SUCC(ret) && tmp_schema_version > schema_version) {
        schema_version = tmp_schema_version;
        LOG_INFO("update minor schema_version", K(schema_version), K(*table));
      }
    }
    if (OB_SUCC(ret)) {
      // schema_version will be recorded on sstable,
      // so use tenant_schema_version will make schema_version on sstable is increasing
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_refreshed_schema_version(
              tenant_id, tenant_schema_version))) {
        LOG_WARN("failed to get tenant full schema guard", K(ret), K(tenant_id));
      } else if (tenant_schema_version > schema_version) {
        schema_version = tenant_schema_version;
      }
    }
    if (OB_SUCC(ret)) {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("got minor schema version", K(schema_version), K(table_id_), K(ret));
    }
  }
  return ret;
}

int ObTableStore::find_need_major_sstable(const common::ObIArray<ObITable*>& major_tables,
    const int64_t multi_version_start, int64_t& major_pos, ObITable*& major_table)
{
  int ret = OB_SUCCESS;
  major_pos = -1;
  major_table = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store not is_inited_", K(ret), K(PRETTY_TS(*this)));
  }

  for (int64_t i = major_tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (major_tables.at(i)->get_snapshot_version() <= multi_version_start) {
      major_pos = i;
      major_table = major_tables.at(i);
      LOG_INFO("found first need major table", K(major_pos), K(multi_version_start), K(i), K(*major_table));
      break;
    }
  }

  if (OB_SUCC(ret) && NULL == major_table && major_tables.count() > 0) {
    major_pos = 0;
    major_table = major_tables.at(0);
    if (!major_table->is_trans_sstable()) {
      LOG_WARN("cannot find need major table, use oldest major table",
          K(major_pos),
          K(multi_version_start),
          K(*major_table));
    }
  }
  return ret;
}

int ObTableStore::find_need_backup_major_sstable(
    const common::ObIArray<ObITable*>& major_tables, const int64_t backup_snapshot_version, int64_t& major_pos)
{
  int ret = OB_SUCCESS;
  major_pos = -1;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store not is_inited_", K(ret), K(PRETTY_TS(*this)));
  }

  for (int64_t i = major_tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (major_tables.at(i)->get_snapshot_version() <= backup_snapshot_version &&
        major_tables.at(i)->is_major_sstable()) {
      major_pos = i;
      LOG_INFO("found first need backup major table",
          K(major_pos),
          K(backup_snapshot_version),
          K(i),
          "major_table",
          *major_tables.at(i));
      break;
    }
  }
  return ret;
}

int ObTableStore::cal_multi_version_start(const int64_t demand_multi_version_start, ObVersionRange& version_range)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!version_range.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Invalid version range of minor merge", K(version_range));
  } else if (demand_multi_version_start < version_range.multi_version_start_) {
    LOG_WARN("cannot reserve multi_version_start", K(ret), K(demand_multi_version_start), K(version_range));
  } else if (demand_multi_version_start < version_range.snapshot_version_) {
    version_range.multi_version_start_ = demand_multi_version_start;
    FLOG_INFO("succ reserve multi_version_start", K(demand_multi_version_start), K(version_range));
  } else {
    version_range.multi_version_start_ = version_range.snapshot_version_;
    FLOG_INFO("no need keep multi version", K(demand_multi_version_start), K(version_range));
  }

  return ret;
}

int ObTableStore::cal_minor_merge_inc_base_version(int64_t& inc_base_version)
{
  int ret = OB_SUCCESS;
  inc_base_version = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store not is_inited_", K(ret), K(PRETTY_TS(*this)));
  } else if (0 == table_count_) {
    // do nothing
  } else {
    if (inc_pos_ < table_count_) {  // get last minor sstable
      inc_base_version = tables_[table_count_ - 1]->get_max_merged_trans_version();
    }
    if (start_pos_ >= 0 && inc_pos_ > start_pos_) {  // get last major sstable after start_pos_
      inc_base_version = MAX(tables_[inc_pos_ - 1]->get_snapshot_version(), inc_base_version);
    }
  }
  return ret;
}

int ObTableStore::cal_mini_merge_inc_base_version(int64_t& inc_base_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cal_minor_merge_inc_base_version(inc_base_version))) {
    LOG_WARN("failed to cal_minor_merge_base_version", K(ret), K(table_id_));
  } else {
    ObTablesHandle memtables;
    ObMemtable* first_memtable = nullptr;
    const bool include_active_memtable = false;
    if (OB_FAIL(get_memtables(include_active_memtable, memtables))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("Partition in split may not have memtable", K(ret));
      } else {
        LOG_WARN("Failed to get first memtable", K(ret));
      }
    } else if (memtables.empty()) {
      // no memtable
    } else if (OB_FAIL(memtables.get_first_memtable(first_memtable))) {
      LOG_WARN("failed to get_first_memtable", K(ret));
    } else if (!first_memtable->is_frozen_memtable()) {
    } else if (first_memtable->get_base_version() > inc_base_version) {
      LOG_INFO("update inc_base_version",
          K(inc_base_version),
          "new_inc_base_version",
          first_memtable->get_base_version(),
          K(*first_memtable));
      inc_base_version = first_memtable->get_base_version();
    } else if (first_memtable->get_snapshot_version() > inc_base_version) {
      // use inc_base_version
    } else {  // first_memtable->get_snapshot_version() <= inc_base_version
      LOG_INFO("Unexpected empty vertion range memtable",
          K(ret),
          K(inc_base_version),
          K(*first_memtable),
          K(PRETTY_TS(*this)));
    }
  }
  return ret;
}

int ObTableStore::get_boundary_major_sstable(const bool last, ObITable*& major_table)
{
  int ret = OB_SUCCESS;
  major_table = nullptr;
  for (int64_t i = inc_pos_ - 1; OB_SUCC(ret) && i >= start_pos_; --i) {
    if (OB_ISNULL(tables_[i])) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Unexpected null table", K(i), K(PRETTY_TS(*this)));
    } else if (!tables_[i]->is_major_sstable()) {
    } else if (FALSE_IT(major_table = tables_[i])) {
    } else if (last) {
      break;
    }
  }

  return ret;
}

int ObTableStore::get_boundary_minor_sstable(const bool last, ObITable*& minor_table)
{
  int ret = OB_SUCCESS;
  minor_table = nullptr;
  for (int64_t i = table_count_ - 1; OB_SUCC(ret) && i >= inc_pos_; --i) {
    if (OB_ISNULL(tables_[i])) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Unexpected null table", K(i), K(PRETTY_TS(*this)));
    } else if (FALSE_IT(minor_table = tables_[i])) {
    } else if (last) {
      break;
    }
  }

  return ret;
}

int ObTableStore::get_table_count(int64_t& table_count) const
{
  int ret = OB_SUCCESS;
  table_count = 0;

  if (OB_UNLIKELY(table_count_ < 0)) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else {
    table_count = table_count_;
  }
  return ret;
}

int ObTableStore::check_table_count_safe(bool& is_safe) const
{
  int ret = OB_SUCCESS;
  is_safe = false;

  if (OB_UNLIKELY(table_count_ < 0)) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else if (table_count_ > MAX_SSTABLE_CNT_IN_STORAGE - RESERVED_STORE_CNT_IN_STORAGE) {
    is_safe = false;
    LOG_INFO("too many sstable in table store, need wait compaction", K_(table_count), K(PRETTY_TS(*this)));
  } else {
    is_safe = true;
  }
  return ret;
}

int ObTableStore::has_major_sstable(bool& has_major)
{
  int ret = OB_SUCCESS;
  has_major = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_pos_ && !has_major; ++i) {
    if (OB_ISNULL(tables_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, table must not be NULL", K(ret), K(i), K(PRETTY_TS(*this)));
    } else {
      has_major = tables_[i]->is_major_sstable();
    }
  }
  return ret;
}

int64_t ObTableStore::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObTableStore");
    J_COLON();
    J_KV(KP(this),
        K_(pkey),
        K_(table_id),
        K_(uptime),
        K_(table_count),
        K_(start_pos),
        K_(inc_pos),
        K_(is_ready_for_read),
        K_(replay_tables),
        KP_(pg_memtable_mgr));
    J_COMMA();
    J_ARRAY_START();
    for (int64_t i = 0; i < table_count_; ++i) {
      ObITable* table = tables_[i];
      if (NULL != table && table->is_sstable()) {
        J_OBJ_START();
        J_KV(K(i),
            "type",
            table->get_key().table_type_,
            "partition_id",
            table->get_key().pkey_.get_partition_id(),
            "version",
            table->get_version(),
            "version_range",
            table->get_key().trans_version_range_,
            "log_ts_range",
            table->get_key().log_ts_range_,
            "ref",
            table->get_ref());
        J_OBJ_END();
        J_COMMA();
      }
    }
    if (OB_NOT_NULL(complement_minor_sstable_)) {
      J_OBJ_START();
      J_KV("type",
          complement_minor_sstable_->get_key().table_type_,
          "partition_id",
          complement_minor_sstable_->get_key().pkey_.get_partition_id(),
          "version",
          complement_minor_sstable_->get_version(),
          "version_range",
          complement_minor_sstable_->get_key().trans_version_range_,
          "log_ts_range",
          complement_minor_sstable_->get_key().log_ts_range_,
          "ref",
          complement_minor_sstable_->get_ref());
      J_OBJ_END();
      J_COMMA();
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

int ObTableStore::finish_replay(const int64_t multi_version_start)
{
  int ret = OB_SUCCESS;

  FLOG_INFO("start finish replay table store", K(multi_version_start), K(PRETTY_TS(*this)));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Unexpected uninited table store", K(ret), K(PRETTY_TS(*this)));
  } else if (multi_version_start < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(multi_version_start));
  } else if (replay_tables_.count() > MAX_SSTABLE_CNT_IN_STORAGE) {
    ret = OB_ERR_SYS;
    LOG_ERROR("replay tables are too many", K(ret), K(replay_tables_.count()), LITERAL_K(MAX_SSTABLE_CNT_IN_STORAGE));
  } else if (OB_FAIL(check_ready_for_read())) {
    LOG_WARN("failed to multi_version_start", K(ret), K(PRETTY_TS(*this)));
  } else {
    FLOG_INFO("succeed finish replay table store", K(multi_version_start), K(PRETTY_TS(*this)));
  }
  return ret;
}

bool ObTableStore::ObITableSnapshotVersionCompare::operator()(const ObITable* lstore, const ObITable* rstore) const
{
  bool bret = false;
  int tmp_ret = OB_SUCCESS;
  if (NULL == lstore) {
    bret = false;
    tmp_ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "left store must not null", K(tmp_ret));
  } else if (NULL == rstore) {
    bret = true;
    tmp_ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "right store must not null", K(tmp_ret));
  } else {
    bret = lstore->get_snapshot_version() < rstore->get_snapshot_version();
  }

  if (OB_SUCCESS == result_code_ && OB_SUCCESS != tmp_ret) {
    result_code_ = tmp_ret;
  }
  return bret;
}

bool ObTableStore::ObITableIDCompare::operator()(const ObITable* ltable, const ObITable* rtable) const
{
  bool bret = false;
  int tmp_ret = OB_SUCCESS;
  if (NULL == ltable) {
    bret = false;
    tmp_ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "left store must not null", K(tmp_ret));
  } else if (NULL == rtable) {
    bret = true;
    tmp_ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "right store must not null", K(tmp_ret));
  } else if (ltable->get_key().table_id_ != rtable->get_key().table_id_) {
    bret = ltable->get_key().table_id_ < rtable->get_key().table_id_;
  }
  return bret;
}

bool ObTableStore::ObITableLogTsRangeCompare::operator()(const ObITable* ltable, const ObITable* rtable) const
{
  bool bret = false;
  int tmp_ret = OB_SUCCESS;
  if (NULL == ltable) {
    bret = false;
    tmp_ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "left store must not null", K(tmp_ret));
  } else if (NULL == rtable) {
    bret = true;
    tmp_ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "right store must not null", K(tmp_ret));
  } else if (ltable->is_major_sstable() || rtable->is_major_sstable()) {
    tmp_ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid table type", K(tmp_ret), KPC(ltable), KPC(rtable));
  } else if (ltable->get_end_log_ts() == rtable->get_end_log_ts()) {
    // TODO: WARNING!!!!  (end log ts is equal)
    if (ltable->get_snapshot_version() == rtable->get_snapshot_version()) {  // equal snapshot version
      if (ltable->is_complement_minor_sstable() != rtable->is_complement_minor_sstable()) {
        bret = !ltable->is_complement_minor_sstable();
      } else {
        bret = false;
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table type not expected", K(tmp_ret), K(*ltable), K(*rtable));
      }
    } else {
      bret = ltable->get_snapshot_version() < rtable->get_snapshot_version();
    }
  } else {  // log ts not equal
    bret = ltable->get_end_log_ts() < rtable->get_end_log_ts();
  }

  if (OB_SUCCESS == result_code_ && OB_SUCCESS != tmp_ret) {
    result_code_ = tmp_ret;
  }
  return bret;
}

int ObTableStore::sort_minor_tables(ObArray<ObITable*>& tables)
{
  int ret = OB_SUCCESS;

  if (tables.empty()) {
    // no need sort
  } else {
    // There is an assumption: either all tables are with log_ts range, or none
    ObITableLogTsRangeCompare comp(ret);
    std::sort(tables.begin(), tables.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort tables", K(ret), K(tables), K(PRETTY_TS(*this)));
    }
  }
  return ret;
}

int ObTableStore::sort_major_tables(ObArray<ObITable*>& tables)
{
  int ret = OB_SUCCESS;

  if (tables.empty()) {
    // no need sort
  } else {
    // There is an assumption: either all tables are with log_ts range, or none
    ObITableSnapshotVersionCompare comp(ret);
    std::sort(tables.begin(), tables.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort tables", K(ret), K(tables), K(PRETTY_TS(*this)));
    }
  }
  return ret;
}

// delete old major sstable and minor sstable
int ObTableStore::need_remove_old_table(const ObVersion& kept_min_version, const int64_t multi_version_start,
    const int64_t backup_snapshot_version, int64_t& real_kept_major_num, bool& need_remove)
{
  int ret = OB_SUCCESS;
  int64_t need_backup_version = -1;
  need_remove = false;

  if (inc_pos_ < 1) {
    // no major sstable
    need_remove = false;
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not valid", K(ret), K(PRETTY_TS(*this)));
  } else if (!kept_min_version.is_valid() || multi_version_start <= 0 || backup_snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(kept_min_version), K(multi_version_start), K(backup_snapshot_version));
  } else {
    // TODO  one loop is enough
    for (int64_t i = inc_pos_ - 1; i >= 0; --i) {
      ObITable* major_sstable = tables_[i];
      if (major_sstable->is_major_sstable() && major_sstable->get_snapshot_version() <= backup_snapshot_version) {
        need_backup_version = major_sstable->get_version().major_;
        break;
      }
    }
    // check old major sstable before start_pos
    for (int64_t i = 0; OB_SUCC(ret) && i < start_pos_ && !need_remove; ++i) {
      ObITable* major_sstable = tables_[i];
      if (major_sstable->is_major_sstable()) {
        if (major_sstable->get_version().major_ < kept_min_version &&
            major_sstable->get_version().major_ != need_backup_version) {
          need_remove = true;
        }
      }
    }

    if (OB_SUCC(ret) && need_remove) {
      ObITable* latest_major_sstable = nullptr;
      if (OB_FAIL(get_boundary_major_sstable(true, latest_major_sstable))) {
        LOG_WARN("Failed to get last major sstable", K(ret));
      } else if (OB_ISNULL(latest_major_sstable)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("latest_major_sstable must not null", K(ret));
      } else {
        real_kept_major_num = latest_major_sstable->get_version().major_ - kept_min_version.major_;
        if (real_kept_major_num <= 0) {
          real_kept_major_num = 1;
        }
        LOG_INFO("need remove old table", K(real_kept_major_num), K(kept_min_version), K(*latest_major_sstable));
      }
    }

    // use max_kept_version and multi_version to decide the need_remove value
    if (OB_SUCC(ret) && !need_remove) {
      // check major sstable after start_pos
      const int64_t next_pos = start_pos_ + 1;
      if (next_pos < inc_pos_) {
        ObITable* major_sstable = tables_[next_pos];
        if (major_sstable->is_major_sstable() && major_sstable->get_snapshot_version() < multi_version_start) {
          need_remove = true;
          LOG_INFO("need remove unneed major sstable for multi_version_start",
              K(multi_version_start),
              K(next_pos),
              K(PRETTY_TS(*this)));
        }
      }
    }

    // we fill upper trans version of minor sstable after transaction status determined
    // now we also should check if there is some minor need to be recycled
    if (OB_SUCC(ret) && !need_remove && inc_pos_ < table_count_) {
      ObITable* first_minor_table = nullptr;
      if (OB_ISNULL(first_minor_table = tables_[inc_pos_])) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "Unexpected null sstable", K(ret), K(PRETTY_TS(*this)));
      } else if (first_minor_table->get_upper_trans_version() == INT64_MAX) {
      } else {
        ObITable* need_major_table = nullptr;
        for (int64_t i = inc_pos_ - 1; OB_SUCC(ret) && i >= start_pos_; --i) {
          if (OB_ISNULL(need_major_table = tables_[i])) {
            ret = OB_ERR_SYS;
            STORAGE_LOG(ERROR, "Unexpected null sstable", K(ret), K(i), K(PRETTY_TS(*this)));
          } else if (need_major_table->get_snapshot_version() <= multi_version_start) {
            break;
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(need_major_table)) {
          if (first_minor_table->get_upper_trans_version() <= need_major_table->get_snapshot_version()) {
            need_remove = true;
            LOG_INFO("need remove unneed minor sstable for old version", KPC(first_minor_table), KPC(need_major_table));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableStore::update_replay_tables()
{
  int ret = OB_SUCCESS;
  replay_tables_.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_count_; ++i) {
      ObITable* table = tables_[i];
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret), K(i), K(PRETTY_TS(*this)));
      } else if (table->is_memtable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unexpected memtable in table store", K(ret), K(i), KPC(table));
      } else if (OB_FAIL(replay_tables_.push_back(table->get_key()))) {
        LOG_WARN("Failed to add table key", K(ret), K(*table));
      }
    }
  }
  return ret;
}

int ObTableStore::update_multi_version_start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObTableStore is not inited", K(ret), K(PRETTY_TS(*this)));
  } else if (is_valid()) {
    multi_version_start_ = -1;
    ObITable* table = nullptr;
    ObITable* last_table = nullptr;
    for (int64_t i = inc_pos_; OB_SUCC(ret) && i < table_count_; i++) {
      if (OB_ISNULL(table = tables_[i])) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unexpected null table", K(ret), K(PRETTY_TS(*this)));
      } else if (!is_own_table(table)) {
        continue;
      } else if (nullptr == last_table) {
        multi_version_start_ = table->get_multi_version_start();
      } else if (is_multi_version_break(table->get_version_range(), last_table->get_snapshot_version())) {
        // 1. table->get_multi_version_start() <= last_table->get_snapshot_version()
        //    multi_version_start < snapshot_version of last sstable, means multi_version range is continuous, no need
        //    to increase
        // 2. table->get_multi_version_start() == table->get_base_version()
        //    multi_version_start of table is equal to base_version, means multi_version range is continuous
        // 3. other situation shows need to set multi_version_start with last_table
        multi_version_start_ = table->get_multi_version_start();
      }
      if (OB_SUCC(ret)) {
        last_table = table;
      }
    }
    // no minor and complement, use major snapshot instead
    if (OB_FAIL(ret)) {
    } else if (-1 == multi_version_start_) {
      last_table = nullptr;
      if (OB_FAIL(get_boundary_major_sstable(true, last_table))) {
        LOG_WARN("Failed to get last major sstable", K(ret));
      } else if (OB_ISNULL(last_table)) {
        // major sstable not found
      } else {
        multi_version_start_ = last_table->get_snapshot_version();
      }
    }
  }

  return ret;
}

int ObTableStore::equals(const ObTableStore& other, bool& is_equal)
{
  int ret = OB_SUCCESS;

  // ignore uptime_,freeze_info_mgr_,replay_tables_
  is_equal = true;

  if (!is_inited_ || !other.is_inited_) {
    ret = OB_NOT_INIT;
    is_equal = false;
    LOG_ERROR("not inited", K(ret), K(other), K(PRETTY_TS(*this)));
  } else if (pkey_ != other.pkey_ || table_id_ != other.table_id_ || table_count_ != other.table_count_ ||
             start_pos_ != other.start_pos_ || inc_pos_ != other.inc_pos_ ||
             is_ready_for_read_ != other.is_ready_for_read_) {
    is_equal = false;
  } else {
    for (int64_t i = 0; is_equal && i < table_count_; ++i) {
      if (tables_[i] != other.tables_[i]) {
        is_equal = false;
      }
    }
    if (is_equal && complement_minor_sstable_ != other.complement_minor_sstable_) {
      is_equal = false;
    }
  }
  return ret;
}

int ObTableStore::get_replay_tables(ObIArray<ObITable::TableKey>& replay_tables)
{
  int ret = OB_SUCCESS;
  replay_tables.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(replay_tables.assign(replay_tables_))) {
    LOG_WARN("Failed to copy replay tables", K(ret));
  }
  return ret;
}

int ObTableStore::get_all_memtables(const bool include_active_memtable, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  const bool reset_handle = false;
  const int64_t start_snapshot_version = -1;
  if (OB_FAIL(pg_memtable_mgr_->get_memtables(handle, reset_handle, start_snapshot_version, include_active_memtable))) {
    LOG_WARN("failed to get memtables", K(ret), K(PRETTY_TS(*this)));
  }
  return ret;
}

int ObTableStore::get_memtables(const bool include_active_memtable, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  const bool reset_handle = false;
  int64_t start_log_ts = 0;
  int64_t start_snapshot_version = -1;
  if (OB_LIKELY(table_count_ > 0)) {
    ObITable* last_table = tables_[table_count_ - 1];
    if (last_table->is_table_with_log_ts_range()) {
      start_log_ts = last_table->get_end_log_ts();
      start_snapshot_version = last_table->get_snapshot_version();
    }
  }
  if (OB_FAIL(pg_memtable_mgr_->get_memtables_v2(
          handle, start_log_ts, start_snapshot_version, reset_handle, include_active_memtable))) {
    LOG_WARN("failed to get memtables", K(ret), K(PRETTY_TS(*this)));
  }
  return ret;
}

int ObTableStore::get_all_sstables(const bool need_complemnt, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();

  // allow get all tables when not valid
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_count_; ++i) {
    if (OB_FAIL(handle.add_table(tables_[i]))) {
      LOG_WARN("failed to add table", K(ret), K(PRETTY_TS(*this)));
    }
  }
  // no need to add buffer minor sstable
  if (OB_SUCC(ret) && need_complemnt && nullptr != complement_minor_sstable_) {
    if (OB_FAIL(handle.add_table(complement_minor_sstable_))) {
      LOG_WARN("failed to add complemnt table", K(ret), KPC(complement_minor_sstable_));
    }
  }

  return ret;
}

int ObTableStore::get_neighbour_freeze_info(
    const int64_t snapshot_version, ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite& freeze_info)
{
  int ret = OB_SUCCESS;
  ObITable* last_major_sstable = nullptr;
  if (OB_FAIL(get_boundary_major_sstable(true, last_major_sstable))) {
    LOG_WARN("Failed to get last major sstable", K(ret));
  } else if (OB_SUCC(freeze_info_mgr_->get_neighbour_major_freeze(snapshot_version, freeze_info))) {
    if (OB_NOT_NULL(last_major_sstable)) {
      // special path for restore major sstable with small snapshot version
      if (last_major_sstable->get_version().major_ >= freeze_info.prev.freeze_version) {
        if (last_major_sstable->get_snapshot_version() < freeze_info.prev.freeze_ts) {
          FLOG_INFO(
              "Restore major sstable with small snapshot version, need reset", K(freeze_info), KPC(last_major_sstable));
          freeze_info.prev.freeze_ts = last_major_sstable->get_snapshot_version();
        }
      }
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    LOG_WARN("Failed to get freeze info, use snapshot_gc_ts instead", K(ret), K(snapshot_version));
    ret = OB_SUCCESS;
    freeze_info.reset();
    freeze_info.next.freeze_version = 0;
    freeze_info.next.freeze_ts = freeze_info_mgr_->get_snapshot_gc_ts();
    if (OB_NOT_NULL(last_major_sstable)) {
      freeze_info.prev.freeze_version = last_major_sstable->get_version().major_;
      freeze_info.prev.freeze_ts = last_major_sstable->get_snapshot_version();
    }
  } else {
    LOG_WARN("Failed to get neighbour major freeze info", K(ret), K(snapshot_version));
  }
  return ret;
}

int ObTableStore::check_need_mini_minor_merge(const bool using_remote_memstore, bool& need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite freeze_info;
  const int64_t mini_minor_threshold = GCONF.minor_compact_trigger;
  int64_t merge_inc_base_version = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret), K(PRETTY_TS(*this)));
  } else if (table_count_ - inc_pos_ <= mini_minor_threshold) {
    // total number of mini sstable is less than threshold + 1
  } else if (OB_FAIL(cal_minor_merge_inc_base_version(merge_inc_base_version))) {
    LOG_WARN("failed to cal_minor_merge_base_version", K(ret), K(table_id_));
  } else if (merge_inc_base_version <= 0) {
    ret = OB_NO_NEED_MERGE;
    STORAGE_LOG(WARN, "Unexpected empty table store to minor merge", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version, freeze_info))) {  // 1: get freeze info
    LOG_WARN("failed to get freeze info", K(ret), K(merge_inc_base_version), K(PRETTY_TS(*this)));
  } else {
    int64_t minor_sstable_count = 0;
    int64_t need_merge_mini_count = 0;
    int64_t minor_check_snapshot_version = 0;
    for (int64_t i = inc_pos_; OB_SUCC(ret) && i < table_count_; ++i) {
      if (!is_own_table(tables_[i])) {
        continue;
      } else if (tables_[i]->get_base_version() >= freeze_info.prev.freeze_ts) {
        if (freeze_info.next.freeze_version > 0 &&
            tables_[i]->get_max_merged_trans_version() > freeze_info.next.freeze_ts) {
          break;
        }
        minor_sstable_count++;
        if (tables_[i]->is_mini_minor_sstable()) {
          if (mini_minor_threshold == need_merge_mini_count++) {
            minor_check_snapshot_version = tables_[i]->get_snapshot_version();
          }
        } else if (need_merge_mini_count > 0 || minor_sstable_count - need_merge_mini_count > 1) {
          // chaos order with mini and minor sstable OR more than one minor sstable
          // need compaction except data replica
          need_merge_mini_count = mini_minor_threshold + 1;
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      // GCONF.minor_compact_trigger means the maximum number of the current L0 sstable,
      // the compaction will be scheduled when it be exceeded
      // If minor_compact_trigger = 0, it means that all L0 sstables should be merged into L1 as soon as possible
      if (minor_sstable_count <= 1) {
        // only one minor sstable exist, no need to do mini minor merge
      } else if (table_count_ >= MAX_SSTABLE_CNT_IN_STORAGE - RESERVED_STORE_CNT_IN_STORAGE) {
        need_merge = true;
        LOG_INFO("table store has too many sstables, need to compaction", K(PRETTY_TS(*this)));
      } else if (need_merge_mini_count <= mini_minor_threshold) {
        // no need merge
      } else if (OB_FAIL(
                     judge_need_mini_minor_merge(using_remote_memstore, minor_check_snapshot_version, need_merge))) {
        LOG_WARN("failed to judge need mini minor merge",
            K(using_remote_memstore),
            K(minor_check_snapshot_version),
            K(need_merge),
            K(PRETTY_TS(*this)));
      }
    }
  }
  LOG_DEBUG("check_need_mini_minor_merge", K(need_merge), K(freeze_info), K(PRETTY_TS(*this)));
  return ret;
}

int ObTableStore::judge_need_mini_minor_merge(
    const bool using_remote_memstore, const int64_t minor_check_snapshot_version, bool& need_merge)
{
  int ret = OB_SUCCESS;
  const int64_t delay_merge_schedule_interval = GCONF._minor_compaction_interval;
  const int64_t follower_replica_merge_level = GCONF._follower_replica_merge_level;
  if (using_remote_memstore) {
    if (is_follower_d_minor_merge(follower_replica_merge_level)) {
      // The leader delays the scheduling of minor compaction, and the D-replica also needs local compaction
      need_merge = true;
    }
  } else if (delay_merge_schedule_interval > 0 && minor_check_snapshot_version > 0) {
    // delays the compaction scheduling
    int64_t current_time = ObTimeUtility::current_time();
    if (minor_check_snapshot_version + delay_merge_schedule_interval < current_time) {
      // need merge
      need_merge = true;
    }
  } else {
    need_merge = true;
  }
  LOG_DEBUG("judge_need_mini_minor_merge", K(using_remote_memstore), K(minor_check_snapshot_version), K(need_merge));
  return ret;
}

int64_t ObTableStore::cal_hist_minor_merge_threshold() const
{
  int64_t hist_threashold = GCONF.minor_compact_trigger;

  return MIN((1 + hist_threashold) * OB_HIST_MINOR_FACTOR, MAX_TABLE_CNT_IN_STORAGE / 2);
}

int ObTableStore::check_need_hist_minor_merge(bool& need_merge)
{
  int ret = OB_SUCCESS;
  const int64_t hist_threashold = cal_hist_minor_merge_threshold();
  ObITable* first_major_table = nullptr;
  need_merge = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObTableStore is not inited", K(ret));
  } else if (table_count_ - inc_pos_ < hist_threashold) {
    // total number of mini sstable is less than hist_threashold, no need merge
    // reduce schedule frequency of hist minor merge
  } else if (OB_FAIL(get_boundary_major_sstable(false, first_major_table))) {
    LOG_WARN("Failed to get first major sstable", K(ret));
  } else if (OB_ISNULL(first_major_table)) {
    // index table during building, need compat with continuous multi version
    need_merge = freeze_info_mgr_->get_latest_frozen_timestamp() > 0;
  } else {
    ObSEArray<ObFreezeInfoSnapshotMgr::FreezeInfoLite, 8> freeze_infos;
    int64_t max_snapshot_version;
    int64_t min_snapshot_version;
    if (OB_FAIL(freeze_info_mgr_->get_freeze_info_behind_major_version(
            first_major_table->get_version().major_, freeze_infos))) {
      LOG_WARN("Failed to get freeze infos behind major version", K(ret), KPC(first_major_table));
    } else if (freeze_infos.count() <= 1) {
      // only one major freeze found, wait normal mini minor to reduce table count
    } else if (OB_FAIL(get_hist_minor_range(freeze_infos, false, min_snapshot_version, max_snapshot_version))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("Failed to get hist minor range", K(ret), K(freeze_infos));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      need_merge = true;
    }
    LOG_DEBUG("succ to check hist mino merge", K(need_merge), K(hist_threashold), K(freeze_infos));
  }
  if (OB_SUCC(ret) && need_merge) {
    if (REACH_TIME_INTERVAL(30 * 1000 * 1000)) {
      FLOG_INFO("Table store need to do hist minor merge to reduce sstables",
          K(need_merge),
          K(hist_threashold),
          K(PRETTY_TS(*this)));
    }
  }

  return ret;
}

int ObTableStore::set_replay_sstables(const bool is_replay_old, const common::ObIArray<ObSSTable*>& sstables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableStore has not been inited", K(ret));
  } else {
    int64_t sstable_index = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < replay_tables_.count(); ++i) {
      if (OB_ISNULL(sstables.at(sstable_index))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret), K(sstable_index), K(sstables.at(sstable_index)));
      } else if (replay_tables_.at(i) != sstables.at(sstable_index)->get_key()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table key is not equal",
            K(ret),
            K(is_replay_old),
            K(replay_tables_.at(i)),
            K(*sstables.at(sstable_index)));
      } else {
        sstable_index++;
      }
    }
    if (OB_SUCC(ret) && replay_tables_.count() != sstables.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table count is not equal", K(ret), K(sstables), K(replay_tables_));
    } else {  // push right sstables into tables_ & replay_tables_
      replay_tables_.reuse();
      ObSSTable* sstable = nullptr;
      table_count_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); ++i) {
        if (OB_ISNULL(sstable = sstables.at(i))) {
          ret = OB_ERR_SYS;
          LOG_ERROR("Unexpected null table", K(ret), K(i), K(sstables));
        } else if (sstable->is_complement_minor_sstable()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Unexpecte complement minor sstable to replay", K(ret), KPC(sstable));
        } else {
          if (sstable->is_minor_sstable() && sstable->get_multi_version_start() != sstable->get_snapshot_version()) {
            multi_version_start_ = sstable->get_multi_version_start();
          }
          sstable->inc_ref();
          tables_[table_count_++] = sstable;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(replay_tables_.push_back(sstable->get_key()))) {
          LOG_WARN("push into replay_tables_ error", K(ret), K(i), KPC(sstable));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {  // reset table_count & inc_pos_
    if (OB_FAIL(update_multi_version_start())) {
      LOG_WARN("Failed to update multi version start of table store", K(ret));
    }
  }
  return ret;
}

bool ObTableStore::is_own_table(const ObITable* table)
{
  return table->get_partition_key() == pkey_;
}

int ObTableStore::check_complete(bool& is_complete)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  is_complete = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited, fatal error", K(ret));
  } else if (OB_FAIL(get_all_tables_from_start(handle))) {
    LOG_WARN("failed to get_all_tables", K(ret), K(pkey_), K(table_id_));
  } else {
    is_complete = check_complete_(handle);
  }
  return ret;
}

bool ObTableStore::check_complete_(const ObTablesHandle& handle)
{
  bool is_complete = true;
  bool is_major_exist = false;
  bool is_memtable_exist = false;
  for (int64_t i = 0; i < handle.get_count(); i++) {
    if (handle.get_table(i)->is_major_sstable()) {
      is_major_exist = true;
    }
    if (handle.get_table(i)->is_memtable()) {
      is_memtable_exist = true;
    }
  }

  if (!is_major_exist || !is_memtable_exist) {
    is_complete = false;
  }

  for (int64_t i = 0; is_complete && i < handle.get_count() - 1; i++) {
    ObITable* cur_table = handle.get_table(i);
    ObITable* next_table = handle.get_table(i + 1);
    if (cur_table->is_table_with_log_ts_range() && cur_table->get_end_log_ts() < next_table->get_start_log_ts()) {
      is_complete = false;
    }
  }
  if (!is_complete) {
    LOG_DEBUG("failed to check_complete", K(handle));
  }
  return is_complete;
}

// TODO  merge into get_all_tables
int ObTableStore::get_all_tables_from_start(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();

  // allow get all tables when not valid
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }

  int64_t start = (start_pos_ >= 0 ? start_pos_ : 0);
  for (int64_t i = start; OB_SUCC(ret) && i < table_count_; ++i) {
    if (OB_FAIL(handle.add_table(tables_[i]))) {
      LOG_WARN("failed to add table", K(ret), K(PRETTY_TS(*this)));
    }
  }

  if (OB_SUCC(ret)) {
    const bool include_active_memtable = true;
    if (OB_FAIL(get_memtables(include_active_memtable, handle))) {
      LOG_WARN("failed to get_memtables", K(ret), K(PRETTY_TS(*this)));
    }
  }
  return ret;
}

// TODO  after buffer minor move to inc_pos_ - 1, we also should add buffer minor
int ObTableStore::get_minor_sstables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    for (int64_t i = inc_pos_; OB_SUCC(ret) && i < table_count_; ++i) {
      if (OB_FAIL(handle.add_table(tables_[i]))) {
        LOG_WARN("failed to add_table", K(ret));
      }
    }
  }
  return ret;
}

int ObTableStore::get_min_schema_version(int64_t& min_schema_version)
{
  int ret = OB_SUCCESS;
  ObITable* table = nullptr;
  min_schema_version = INT64_MAX;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_count_; i++) {
    if (OB_ISNULL(table = tables_[i])) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Unexpected null sstable", K(ret), K(i), K(PRETTY_TS(*this)));
    } else {
      min_schema_version = min(min_schema_version, static_cast<ObSSTable*>(table)->get_meta().schema_version_);
    }
  }
  return ret;
}

int ObTableStore::get_mark_deletion_tables(
    const int64_t end_log_ts, const int64_t snapshot_version, ObTablesHandle& handle)
{
  UNUSED(snapshot_version);
  int ret = OB_SUCCESS;
  ObTablesHandle tmp_handle;
  const bool include_active_memtable = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableStore is not inited", K(ret));
  } else if (OB_FAIL(get_read_tables(INT64_MAX, tmp_handle))) {
    LOG_WARN("failed to get_all_tables", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_handle.get_count(); ++i) {
      ObITable* table = tmp_handle.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null", K(ret), K(i), K(PRETTY_TS(*this)));
      } else if (table->get_end_log_ts() <= end_log_ts) {
        if (OB_FAIL(handle.add_table(table))) {
          LOG_WARN("failed to add table", K(ret));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObTableStore::clear_complement_minor_sstable()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store is not inited", K(ret));
  } else if (OB_NOT_NULL(complement_minor_sstable_)) {
    complement_minor_sstable_->dec_ref();
    FLOG_INFO("succeed to clear complement_minor_sstable",
        K_(pkey),
        K_(table_id),
        "table key",
        complement_minor_sstable_->get_key());
    complement_minor_sstable_ = nullptr;
  }
  return ret;
}

int ObTableStore::get_schema_version(int64_t& schema_version) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table_store is not inited", K(ret));
  } else if (table_count_ <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("table store is empty", K(ret), K(table_id_));
  } else {
    schema_version = static_cast<const ObSSTable*>(tables_[0])->get_meta().schema_version_;
  }
  return ret;
}

// Split Section
// TODO  try to split split code from table_store
/*
 * set_reference_tables is required in two cases
 * 0. When doing logic split, the table store is empty at this time
 * 1. The observer is down and restarted, and the target partition also depends on the source partition
 */

int ObTableStore::set_reference_tables(
    ObTablesHandle& new_handle, ObTablesHandle& old_handle, const int64_t memtable_base_version, bool& need_update)
{
  int ret = OB_SUCCESS;
  need_update = true;
  ObTablesHandle handle_without_complement;
  ObTablesHandle old_handle_without_complement;
  ObSSTable* complement_sstable = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited, fatal error", K(ret));
  } else if (new_handle.get_count() >= MAX_SSTABLE_CNT_IN_STORAGE ||
             old_handle.get_count() >= MAX_SSTABLE_CNT_IN_STORAGE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN(
        "too many tables", K(ret), K(new_handle.get_count()), K(old_handle.get_count()), K(MAX_SSTABLE_CNT_IN_STORAGE));
  } else if (0 != table_count_ || -1 != start_pos_) {
    ret = OB_ERR_SYS;
    LOG_WARN("It should be an empty table store", K(ret));
  } else if (OB_FAIL(check_need_reference_tables(old_handle, memtable_base_version, need_update))) {
    LOG_WARN("failed to check need reference tables", K(ret), K(old_handle), K(memtable_base_version));
  } else if (!need_update) {
    LOG_WARN("no need to set reference tables", K(PRETTY_TS(*this)));
  } else {
    start_pos_ = 0;
    int64_t pos = 0;

    for (int64_t i = 0; OB_SUCC(ret) && i < new_handle.get_count(); i++) {
      if (!new_handle.get_table(i)->is_complement_minor_sstable()) {
        if (OB_FAIL(handle_without_complement.add_table(new_handle.get_table(i)))) {
          LOG_WARN("failed to push back table", K(ret), K(PRETTY_TS(*this)));
        }
      } else {
        complement_sstable = static_cast<ObSSTable*>(new_handle.get_table(i));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < old_handle.get_count(); i++) {
      if (!old_handle.get_table(i)->is_complement_minor_sstable()) {
        if (OB_FAIL(old_handle_without_complement.add_table(old_handle.get_table(i)))) {
          LOG_WARN("failed to push back table", K(ret), K(PRETTY_TS(*this)));
        }
      }
    }

    while (OB_SUCC(ret) && pos < handle_without_complement.get_count() &&
           pos < old_handle_without_complement.get_count()) {
      ObITable* new_table = handle_without_complement.get_table(pos);
      ObITable* old_table = old_handle_without_complement.get_table(pos);
      if (old_table->get_key() == new_table->get_key()) {
        tables_[table_count_++] = new_table;
        new_table->inc_ref();
        pos++;
      } else {
        break;
      }
    }

    for (int64_t i = pos; OB_SUCC(ret) && i < handle_without_complement.get_count(); i++) {
      ObITable* new_table = handle_without_complement.get_table(i);
      if (new_table->is_sstable()) {
        tables_[table_count_++] = new_table;
        new_table->inc_ref();
      }
    }

    for (int64_t i = pos; OB_SUCC(ret) && i < old_handle_without_complement.get_count(); i++) {
      ObITable* old_table = old_handle_without_complement.get_table(i);
      if (OB_ISNULL(old_table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret), K(pkey_), K(table_id_));
      } else if (!is_own_table(old_table)) {
        // do nothing
      } else {
        if (table_count_ + 1 >= MAX_SSTABLE_CNT_IN_STORAGE) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("too many tables", K(ret), K(table_count_), K(MAX_SSTABLE_CNT_IN_STORAGE));
        } else if (old_table->is_sstable()) {
          tables_[table_count_++] = old_table;
          old_table->inc_ref();
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < table_count_; ++i) {
        if (!tables_[i]->is_major_sstable()) {
          inc_pos_ = i;
          break;
        }
      }
      if (-1 == inc_pos_) {
        inc_pos_ = table_count_;
      }
      LOG_INFO("succ to set reference tables", K(handle_without_complement), K(old_handle), K(PRETTY_TS(*this)));
      if (OB_FAIL(check_ready_for_read())) {
        LOG_WARN("failed to check ready for read", K(ret), K(PRETTY_TS(*this)));
      }
    }
  }

  // no need set complement_minor_sstable
  complement_minor_sstable_ = complement_sstable;

  if (OB_SUCC(ret)) {
    if (need_update && OB_FAIL(update_replay_tables())) {
      LOG_WARN("failed to update_replay_tables", K(ret));
    }
  }
  return ret;
}

/*
 * If p1 refers to the minor sstable of p0, then minor_split is required
 * If p1 refers to the major sstable of p0, then major_split is required
 * It is possible that p0 has been split, but p1 has not updated the sstable reference, so judgment is needed
 * Check the old_handle, not this table_store
 *  */
int ObTableStore::check_need_split(
    const ObTablesHandle& old_handle, common::ObVersion& split_version, bool& need_split, bool& need_minor_split)
{
  int ret = OB_SUCCESS;
  bool can_split = true;
  need_split = false;
  need_minor_split = false;

  int64_t first_reference_pos = -1;
  int64_t last_reference_pos = -1;

  int64_t major_split_pos = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited, fatal error", K(ret));
  } else if (old_handle.empty()) {
    // do nothing
  } else if (OB_FAIL(check_can_split(old_handle, can_split))) {
    LOG_WARN("failed to check can split, ", K(ret), K(old_handle));
  } else if (!can_split) {
    need_split = false;
  } else if (OB_FAIL(get_minor_split_table_pos(old_handle, first_reference_pos, last_reference_pos))) {
    LOG_WARN("failed to get last reference position, ", K(ret), K(old_handle));
  } else {
    // Not only inherited the major sstable of p0, but also minor sstable + memtable
    if (-1 != last_reference_pos) {
      need_split = true;
      need_minor_split = true;
    } else if (OB_FAIL(get_major_split_table_pos(old_handle, major_split_pos))) {
      LOG_WARN("failed to get major split table pos, ", K(ret), K(old_handle));
    } else if (-1 == major_split_pos) {
      // no need to split
    } else if (OB_ISNULL(old_handle.get_table(major_split_pos))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null, ", K(ret), K(old_handle));
    } else if (-1 != major_split_pos) {
      need_split = true;
      if (need_split) {
        split_version.major_ = old_handle.get_table(major_split_pos)->get_key().version_.major_;
      }
    }
  }
  return ret;
}

int ObTableStore::is_physical_split_finished(bool& is_physical_split_finish)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  is_physical_split_finish = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited, fatal error", K(ret));
  } else if (0 == table_count_) {
    is_physical_split_finish = false;
  } else {
    for (int64_t i = 0; i < table_count_; i++) {
      if (!is_own_table(tables_[i])) {
        is_physical_split_finish = false;
        break;
      }
    }
  }
  return ret;
}

int ObTableStore::is_physical_split_finished(ObTablesHandle& old_handle, bool& is_physical_split_finish)
{
  int ret = OB_SUCCESS;
  int64_t major_split_pos = -1;
  is_physical_split_finish = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited, fatal error", K(ret));
  } else if (OB_FAIL(get_major_split_table_pos(old_handle, major_split_pos))) {
    LOG_WARN("failed to get major split table pos", K(ret), K(old_handle));
  } else if (-1 != major_split_pos) {
    is_physical_split_finish = false;
  }
  return ret;
}

int ObTableStore::get_physical_split_info(ObVirtualPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  ObSSTable* table = NULL;
  ObTablesHandle old_handle;
  int64_t major_split_pos = -1;
  int64_t first_reference_pos = -1;
  int64_t last_reference_pos = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited, fatal error", K(ret));
  } else if (OB_FAIL(get_all_tables(false /*include_active_memtable*/, false /*include_complement*/, old_handle))) {
    LOG_WARN("failed to get_all_tables", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(get_minor_split_table_pos(old_handle, first_reference_pos, last_reference_pos))) {
    LOG_WARN("failed to get minor split table pos", K(ret), K(pkey_), K(old_handle));
  } else if (-1 != first_reference_pos) {
    split_info.is_major_split_ = false;
  } else if (OB_FAIL(get_major_split_table_pos(old_handle, major_split_pos))) {
    LOG_WARN("failed to get major split table pos", K(ret), K(pkey_), K(old_handle));
  } else if (-1 != major_split_pos) {
    split_info.is_major_split_ = true;
  }
  return ret;
}

int ObTableStore::get_reference_tables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;

  ObITable* latest_major_sstable = NULL;
  handle.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = start_pos_; i < table_count_; ++i) {
      if (tables_[i]->is_major_sstable()) {
        latest_major_sstable = tables_[i];
      }
    }
    if (NULL == latest_major_sstable) {
      ret = OB_ERR_SYS;
      LOG_WARN("there should be at least one major sstable", K(ret), K(PRETTY_TS(*this)));
    } else if (OB_FAIL(handle.add_table(latest_major_sstable))) {
      LOG_WARN("failed to add table", K(ret), K(PRETTY_TS(*this)));
    } else {
      for (int64_t i = inc_pos_; OB_SUCC(ret) && i < table_count_; ++i) {
        if (tables_[i]->get_snapshot_version() > latest_major_sstable->get_snapshot_version()) {
          if (OB_FAIL(handle.add_table(tables_[i]))) {
            LOG_WARN("failed to add table", K(ret), K(PRETTY_TS(*this)));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (NULL != complement_minor_sstable_ && OB_FAIL(handle.add_table(complement_minor_sstable_))) {
          LOG_WARN("failed to add table", K(ret), K(*this));
        }
      }
    }
  }
  return ret;
}

// After p0 minor sstable+memtable are spilted, p1 need to update table_store and take split sstables
int ObTableStore::build_minor_split_store(const ObTablesHandle& old_handle, ObTablesHandle& handle, bool& need_update)
{
  int ret = OB_SUCCESS;
  bool has_fetch_split_table = false;
  int64_t first_reference_pos = -1;
  int64_t last_reference_pos = -1;
  bool can_split = false;
  need_update = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (table_count_ != 0 || start_pos_ != -1) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot rebuild table store", K(ret), K(PRETTY_TS(*this)));
  } else if (old_handle.get_count() > MAX_TABLE_CNT_IN_STORAGE) {
    ret = OB_ERR_SYS;
    LOG_ERROR("too many tables in old_handle", K(ret), K(old_handle));
  } else if (old_handle.get_count() <= 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("it should not be empty table", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(check_can_split(old_handle, can_split))) {
    LOG_WARN("failed to check if need split", K(ret), K(old_handle));
  } else if (!can_split) {
    need_update = false;
    LOG_INFO("no need to build minor split table store", K(pkey_), K(old_handle));
  } else if (OB_FAIL(get_minor_split_table_pos(old_handle, first_reference_pos, last_reference_pos))) {
    LOG_WARN("failed to get last reference position", K(ret), K(old_handle));
  } else if (-1 == last_reference_pos) {
    need_update = false;
    LOG_INFO("no need to build minor split table store", K(pkey_), K(old_handle));
  } else {
    start_pos_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < old_handle.get_count(); i++) {
      ObITable* table = old_handle.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret), K(pkey_), K(table_id_), K(old_handle), K(i));
      } else if (!is_own_table(table) && !table->is_major_sstable()) {
        ObITable::TableKey table_key = table->get_key();

        if (!has_fetch_split_table) {
          has_fetch_split_table = true;

          if (OB_SUCC(ret)) {
            for (int64_t j = 0; j < handle.get_count(); j++) {
              tables_[table_count_] = handle.get_table(j);
              tables_[table_count_]->inc_ref();
              table_count_++;
            }
          }
        }
      } else if (table->is_sstable()) {
        tables_[table_count_++] = table;
        table->inc_ref();
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < table_count_; ++i) {
        if (!tables_[i]->is_major_sstable()) {
          inc_pos_ = i;
          break;
        }
      }
      if (-1 == inc_pos_) {
        inc_pos_ = table_count_;
      }
    }

    if (OB_SUCC(ret)) {
      if (table_count_ >= MAX_SSTABLE_CNT_IN_STORAGE) {
        ret = OB_ERR_SYS;
        LOG_ERROR("tables is too much, cannot add more", K(ret), K(table_count_));
      } else {
        LOG_INFO("succeed to build minor split store", K(PRETTY_TS(*this)));
        if (OB_FAIL(check_ready_for_read())) {
          LOG_WARN("failed to check ready for read", K(ret), K(PRETTY_TS(*this)));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_replay_tables())) {
      LOG_WARN("failed to update_replay_tables", K(ret));
    }
  }
  return ret;
}

// After the p0 major sstable is split, p1 updates table_store,
// taking the sstable belonging to p1 after the split and the major sstable of the unsplit part of p0
int ObTableStore::build_major_split_store(const ObTablesHandle& old_handle, ObTablesHandle& handle, bool& need_update)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t major_split_pos = -1;
  need_update = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (table_count_ != 0 || start_pos_ != -1) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot rebuild table store", K(ret), K(PRETTY_TS(*this)));
  } else if (old_handle.get_count() > MAX_TABLE_CNT_IN_STORAGE) {
    ret = OB_ERR_SYS;
    LOG_ERROR("too many tables in old_handle", K(ret), K(old_handle.get_count()), K(PRETTY_TS(*this)));
  } else if (old_handle.get_count() <= 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("it should not be empty table", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(get_major_split_table_pos(old_handle, major_split_pos))) {
    LOG_WARN("failed to get major split table pos", K(ret), K(old_handle));
  } else if (-1 == major_split_pos) {
    need_update = false;
    LOG_INFO("no need to build major split table store", K(ret), K(old_handle));
  } else {
    ObITable* table = old_handle.get_table(major_split_pos);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret), K(pkey_), K(table_id_));
    } else {
      start_pos_ = table_count_;

      tables_[table_count_] = handle.get_table(0);
      tables_[table_count_]->inc_ref();
      table_count_++;

      for (int64_t i = 0; OB_SUCC(ret) && i < old_handle.get_count(); i++) {
        ObITable* table = old_handle.get_table(i);
        if (OB_ISNULL(table)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("table must not null", K(ret), K(pkey_), K(table_id_));
        } else if (!table->is_major_sstable()) {
          tables_[table_count_++] = table;
          table->inc_ref();
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < table_count_; ++i) {
        if (!tables_[i]->is_major_sstable()) {
          inc_pos_ = i;
          break;
        }
      }
      if (-1 == inc_pos_) {
        inc_pos_ = table_count_;
      }
    }

    if (OB_SUCC(ret)) {
      if (table_count_ >= MAX_SSTABLE_CNT_IN_STORAGE) {
        ret = OB_ERR_SYS;
        LOG_ERROR("tables is too much, cannot add more", K(ret), K(table_count_));
      } else {
        LOG_INFO("succeed to build major split store", K(PRETTY_TS(*this)));
        if (OB_FAIL(check_ready_for_read())) {
          LOG_WARN("failed to check ready for read", K(ret), K(PRETTY_TS(*this)));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_replay_tables())) {
      LOG_WARN("failed to update_replay_tables", K(ret));
    }
  }

  return ret;
}

bool ObTableStore::is_spliting()
{
  bool is_spliting = false;

  if (is_valid()) {
    if (pkey_.get_partition_id() != tables_[start_pos_]->get_partition_key().get_partition_id()) {
      is_spliting = true;
    }
  }
  return is_spliting;
}

int ObTableStore::check_can_split(bool& can_split)
{
  int ret = OB_SUCCESS;
  bool is_complete = false;
  can_split = true;
  ObTablesHandle handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited, fatal error", K(ret));
  } else if (OB_FAIL(get_all_tables_from_start(handle))) {
    LOG_WARN("failed to get_all_tables", K(ret), K(pkey_), K(table_id_));
  } else {
    is_complete = check_complete_(handle);
    if (!is_complete) {
      can_split = false;
    } else {
      if (OB_FAIL(check_can_split(handle, can_split))) {
        LOG_WARN("failed to check can split", K(ret), K(pkey_), K(table_id_));
      }
    }
  }
  return ret;
}

int ObTableStore::check_can_split(const ObTablesHandle& old_handle, bool& can_split)
{
  int ret = OB_SUCCESS;
  ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite freeze_info;
  ObITable* minor_sstable = NULL;
  can_split = true;
  const ObPartitionKey& memtable_pkey = pg_memtable_mgr_->get_pkey();

  // A memtable without a source partition can be physically split
  for (int64_t i = old_handle.get_count() - 1; can_split && i >= 0; i--) {
    ObITable* table = old_handle.get_table(i);
    if (table->get_partition_key() != memtable_pkey && table->is_memtable()) {
      can_split = false;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_INFO("has memtable, cannot split now", K(*table), K_(pkey));
      }
      break;
    }
  }

  // When merging and splitting concurrently, make sure that multiple copies are split under the same major version
  for (int64_t i = old_handle.get_count() - 1; OB_SUCC(ret) && can_split && i >= 0; i--) {
    ObITable* table = old_handle.get_table(i);
    if (!is_own_table(table) && table->is_minor_sstable() && NULL == minor_sstable) {
      minor_sstable = table;
    } else if (!is_own_table(table) && table->is_major_sstable() && NULL != minor_sstable) {
      if (OB_FAIL(freeze_info_mgr_->get_neighbour_major_freeze(table->get_snapshot_version(), freeze_info))) {
        LOG_WARN("failed to get neighbour major freeze", K(ret), K(old_handle));
      } else if (minor_sstable->get_upper_trans_version() >= freeze_info.next.freeze_ts) {
        can_split = false;
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("has not merged minor sstable, cannot split now", K(freeze_info), K(*minor_sstable));
        }
      }
    }
  }
  return ret;
}

// 1. When just splitting, the table store of the target partition is empty, it must be needed
// 2. After the machine is restarted, it is necessary to determine whether
// there is a hole between the table store and the active memtable
int ObTableStore::check_need_reference_tables(
    ObTablesHandle& old_handle, const int64_t memtable_base_version, bool& need_reference)
{
  int ret = OB_SUCCESS;
  int64_t base_version = 0;
  int64_t latest_major_pos = -1;
  need_reference = false;
  if (old_handle.empty()) {
    need_reference = true;
  } else {
    for (int64_t i = 0; i < old_handle.get_count(); i++) {
      if (old_handle.get_table(i)->is_major_sstable()) {
        latest_major_pos = i;
      }
    }
    if (-1 == latest_major_pos) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should have a major sstable", K(ret), K(PRETTY_TS(*this)));
    } else {
      for (int64_t i = latest_major_pos; i < old_handle.get_count(); i++) {
        if (i + 1 < old_handle.get_count()) {
          base_version = old_handle.get_table(i + 1)->get_base_version();
        } else {
          base_version = memtable_base_version;
        }
        if (old_handle.get_table(i)->get_snapshot_version() < base_version) {
          need_reference = true;
          break;
        }
      }
      if (need_reference && is_own_table(old_handle.get_table(latest_major_pos))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the partition has finished physical split, should not need reference", K(ret), K(PRETTY_TS(*this)));
      }
    }
  }
  return ret;
}

int ObTableStore::get_minor_split_table_pos(
    const ObTablesHandle& old_handle, int64_t& first_reference_pos, int64_t& last_reference_pos)
{
  int ret = OB_SUCCESS;
  first_reference_pos = -1;
  last_reference_pos = -1;
  const ObPartitionKey& pgkey = pg_memtable_mgr_->get_pkey();

  for (int64_t i = old_handle.get_count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    ObITable* table = old_handle.get_table(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret), K(pkey_), K(table_id_));
    } else {
      const ObITable::TableKey& table_key = table->get_key();
      if ((table_key.is_minor_sstable() && table_key.pkey_ != pkey_) ||
          (table_key.is_memtable() && table_key.pkey_ != pgkey)) {
        if (-1 == last_reference_pos) {
          last_reference_pos = i;
        }
        first_reference_pos = i;
      }
    }
  }
  return ret;
}

int ObTableStore::get_major_split_table_pos(const ObTablesHandle& old_handle, int64_t& pos)
{
  int ret = OB_SUCCESS;
  pos = -1;
  for (int64_t i = old_handle.get_count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    ObITable* table = old_handle.get_table(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret), K(pkey_), K(table_id_));
    } else {
      if (!is_own_table(table) && table->is_major_sstable()) {
        pos = i;
      }
    }
  }
  return ret;
}

int ObTableStore::get_major_split_tables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  int64_t pos = -1;
  ObTablesHandle old_handle;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(get_all_tables(false /*include_active_memtable*/, false /*include_complement*/, old_handle))) {
    LOG_WARN("failed to get all tables", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(get_major_split_table_pos(old_handle, pos))) {
    LOG_WARN("failed to get major split table pos", K(ret), K(PRETTY_TS(*this)));
  } else if (-1 == pos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no need to do major split", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(handle.add_table(tables_[pos]))) {
    LOG_WARN("failed to add table", K(ret), K(PRETTY_TS(*this)));
  }

  return ret;
}

int ObTableStore::get_minor_split_tables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  int64_t first_reference_pos = -1;
  int64_t last_reference_pos = -1;
  ObTablesHandle old_handle;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(get_all_tables(false /*include_active_memtable*/, false /*include_complement*/, old_handle))) {
    LOG_WARN("failed to get all tables", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_FAIL(get_minor_split_table_pos(old_handle, first_reference_pos, last_reference_pos))) {
    LOG_WARN("failed to get minor split table position", K(ret), K(old_handle));
  } else if (-1 == last_reference_pos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no need to do minor split", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = first_reference_pos; i <= last_reference_pos; i++) {
      if (OB_FAIL(handle.add_table(old_handle.get_table(i)))) {
        LOG_WARN("failed to add table for minor split", K(ret), K(i), K(PRETTY_TS(*this)));
      }
    }
  }
  return ret;
}

// Migrate section
int ObTableStore::check_can_migrate(bool& can_migrate)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  can_migrate = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited, fatal error", K(ret));
  } else if (OB_FAIL(get_all_tables_from_start(handle))) {
    LOG_WARN("failed to get_all_tables", K(ret), K(pkey_), K(table_id_));
  } else {
    can_migrate = check_complete_(handle);

    for (int64_t i = handle.get_count() - 1; can_migrate && i >= 0; i--) {
      ObITable* table = handle.get_table(i);
      if (table->is_memtable() && table->get_partition_key() != pg_memtable_mgr_->get_pkey()) {
        can_migrate = false;
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("has not finished minor split, cannot migrate", K(*table), K_(pkey));
        }
        break;
      }
    }
  }

  return ret;
}

// TODO check table store continuous with end_log_ts of major sstable
int ObTableStore::get_continue_tables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  bool has_major = false;
  // TODO to be replaced by major_end_log_ts
  int64_t last_major_snapshot = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < start_pos_; ++i) {
      if (OB_ISNULL(tables_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, table must not be NULL", K(ret), K(i), K(PRETTY_TS(*this)));
      } else if (OB_FAIL(handle.add_table(tables_[i]))) {
        LOG_WARN("failed to push table into handle", K(ret), K(i), KPC(tables_[i]));
      }
    }

    for (int64_t i = start_pos_; i < inc_pos_ && OB_SUCC(ret); ++i) {
      ObSSTable* sstable = static_cast<ObSSTable*>(tables_[i]);
      if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, table must not be NULL", K(ret), K(i), K(PRETTY_TS(*this)));
      } else if (sstable->is_major_sstable()) {
        has_major = true;
        last_major_snapshot = sstable->get_key().trans_version_range_.snapshot_version_;
        if (OB_FAIL(handle.add_table(tables_[i]))) {
          LOG_WARN("failed to add tables into handle", K(ret), KPC(tables_[i]));
        }
      }
    }

    if (OB_SUCC(ret) && has_major) {
      ObITable* last_table = nullptr;
      ObITable* curr_table = nullptr;
      for (int64_t i = inc_pos_; OB_SUCC(ret) && i < table_count_; ++i) {
        if (OB_ISNULL(curr_table = tables_[i])) {
          ret = OB_ERR_SYS;
          LOG_WARN("Unexpected null table", K(ret), K(i), K(PRETTY_TS(*this)));
        } else if (OB_NOT_NULL(last_table) && curr_table->get_start_log_ts() > last_table->get_end_log_ts()) {
          break;
        } else if (OB_FAIL(handle.add_table(curr_table))) {
          LOG_WARN("failed to add table into handle", K(ret), KPC(curr_table));
        } else {
          last_table = curr_table;
        }
      }
    }
  }
  return ret;
}

int ObTableStore::get_needed_local_tables_for_migrate(
    const ObMigrateRemoteTableInfo& remote_table_info, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store is not inited", K(ret));
  } else if (remote_table_info.need_reuse_local_minor_) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(ERROR, "Unexpected remote table info to reuse local minor", K(ret), K(remote_table_info));
  } else if (table_count_ <= 0) {
    // empty table store, do nothing
  } else {
    bool need_add = !remote_table_info.has_major();
    // Keep the major sstable older than the source
    for (int64_t i = inc_pos_ - 1; OB_SUCC(ret) && i >= 0; --i) {
      ObITable* table = tables_[i];
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_WARN("table should not be null", K(ret));
      } else if (need_add || table->get_key().version_.major_ + 1 == remote_table_info.remote_min_major_version_) {
        need_add = true;
        if (OB_FAIL(handle.add_table(table))) {
          LOG_WARN("failed to add table", K(ret), KPC(table));
        }
      }
    }
  }
  return ret;
}

int ObTableStore::get_multi_version_start(int64_t& multi_version_start)
{
  int ret = OB_SUCCESS;
  multi_version_start = INT64_MAX;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = table_count_ - 1; i >= 0; --i) {
      if (tables_[i]->get_multi_version_start() != tables_[i]->get_base_version()) {
        multi_version_start = tables_[i]->get_multi_version_start();
        break;
      }
    }
  }

  return ret;
}

int ObTableStore::get_min_max_major_version(int64_t& min_version, int64_t& max_version)
{
  int ret = OB_SUCCESS;
  min_version = INT64_MAX;
  max_version = INT64_MIN;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = table_count_ - 1; i >= 0; --i) {
      if (tables_[i]->is_major_sstable()) {
        const int64_t major_version = tables_[i]->get_version();
        min_version = min(min_version, major_version);
        max_version = max(max_version, major_version);
      }
    }
  }
  return ret;
}

int ObTableStore::get_flashback_major_sstable(const int64_t flashback_scn, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObITable* major_sstable = NULL;
  handle.reset();

  if (table_count_ <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = table_count_ - 1; OB_SUCC(ret) && i >= start_pos_; --i) {
      if (OB_ISNULL(tables_[i])) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret), K(PRETTY_TS(*this)));
      } else if (tables_[i]->is_major_sstable() && tables_[i]->get_snapshot_version() <= flashback_scn) {
        major_sstable = tables_[i];
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(major_sstable)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_FAIL(handle.add_table(major_sstable))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }

  return ret;
}

int ObLogTsCompater::set_log_ts(const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_ts_range_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "Unexpected null log ts range", K(ret), K(*this));
  } else if (OB_UNLIKELY(log_ts < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to set log ts", K(ret), K(log_ts));
  } else if (is_base_) {
    log_ts_range_->start_log_ts_ = log_ts;
  } else {
    log_ts_range_->end_log_ts_ = log_ts;
    log_ts_range_->max_log_ts_ = log_ts;
  }
  return ret;
}

int ObTableCompater::add_tables(ObIArray<ObSSTable*>& sstables)
{
  int ret = OB_SUCCESS;
  if (sstables.empty()) {
  } else {
    ObSSTable* sstable = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); i++) {
      if (OB_ISNULL(sstable = sstables.at(i))) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "Unexpected null sstable", K(ret), K(i), K(sstables));
      } else if (sstable->is_major_sstable()) {
        sstable->get_log_ts_range().reset();
      } else {
        ObLogTsCompater base_log_ts(sstable->get_base_version(), sstable->get_log_ts_range(), true);
        ObLogTsCompater snapshot_log_ts(sstable->get_snapshot_version(), sstable->get_log_ts_range(), false);
        if (OB_FAIL(compaters_.push_back(base_log_ts))) {
          STORAGE_LOG(WARN, "Failed to push back major sstableb base log ts", K(ret), K(i), KPC(sstable));
        } else if (OB_FAIL(compaters_.push_back(snapshot_log_ts))) {
          STORAGE_LOG(WARN, "Failed to push back major sstableb snapshot log ts", K(ret), K(i), KPC(sstable));
        }
      }
    }
  }
  return ret;
}

int ObTableCompater::add_tables(ObIArray<ObMigrateTableInfo::SSTableInfo>& sstables)
{
  int ret = OB_SUCCESS;

  if (sstables.empty()) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); i++) {
      ObMigrateTableInfo::SSTableInfo& sstable_info = sstables.at(i);
      if (sstable_info.src_table_key_.is_major_sstable()) {
        sstable_info.dest_log_ts_range_.reset();
      } else {
        ObITable::TableKey& table_key = sstable_info.src_table_key_;
        ObLogTsCompater base_log_ts(table_key.get_base_version(), sstable_info.dest_log_ts_range_, true);
        ObLogTsCompater snapshot_log_ts(table_key.get_snapshot_version(), sstable_info.dest_log_ts_range_, false);
        if (OB_FAIL(compaters_.push_back(base_log_ts))) {
          STORAGE_LOG(WARN, "Failed to push back minor sstable base log ts", K(ret), K(i), K(sstable_info));
        } else if (OB_FAIL(compaters_.push_back(snapshot_log_ts))) {
          STORAGE_LOG(WARN, "Failed to push back minor sstable snapshot log ts", K(ret), K(i), K(sstable_info));
        }
      }
    }
  }

  return ret;
}

int ObTableCompater::fill_log_ts()
{
  int ret = OB_SUCCESS;

  if (compaters_.empty()) {
    STORAGE_LOG(INFO, "Nothing to fill log ts", K(*this));
  } else {
    std::sort(compaters_.begin(), compaters_.end());
    int64_t new_log_ts = ObLogTsRange::MIN_TS;
    int64_t last_version = INT64_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < compaters_.count(); i++) {
      ObLogTsCompater& compater = compaters_.at(i);
      if (compater.version_ != last_version) {
        new_log_ts++;
        last_version = compater.version_;
      }
      if (OB_FAIL(compater.set_log_ts(new_log_ts))) {
        STORAGE_LOG(WARN, "Failed to set new log ts", K(ret), K(new_log_ts));
      }
    }
    if (OB_SUCC(ret)) {
      // compat pg always use last replay log ts from OB_MAX_COMPAT_LOG_TS
      last_version = compaters_.at(compaters_.count() - 1).version_;
      for (int64_t i = compaters_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
        if (compaters_.at(i).version_ == last_version) {
          if (OB_FAIL(compaters_.at(i).set_log_ts(OB_MAX_COMPAT_LOG_TS))) {
            STORAGE_LOG(WARN, "Failed to set new log ts", K(ret), K(new_log_ts));
          }
        } else {
          break;
        }
      }
      if (OB_SUCC(ret)) {
        STORAGE_LOG(INFO, "Succ to set new log ts range for compat", K_(compaters));
      }
    }
  }

  return ret;
}

void ObPrintableTableStore::table_to_string(
    ObITable* table, const char* table_type, char* buf, const int64_t buf_len, int64_t& pos) const
{
  if (table != nullptr && table->is_sstable()) {
    ObCurTraceId::TraceId* trace_id = ObCurTraceId::get_trace_id();
    BUF_PRINTF("[%ld] [ ", GETTID());
    BUF_PRINTO(PC(trace_id));
    BUF_PRINTF(" ] ");
    BUF_PRINTF(" %-8s %-10s %-8d %-16lu %-16lu %-16lu %-16lu %-19lu %-16lu %-16lu %-16lu %-4ld %-4d ",
        table_type,
        ObITable::get_table_type_name(table->get_key().table_type_),
        table->get_version().major_,
        table->get_base_version(),
        table->get_multi_version_start(),
        table->get_snapshot_version(),
        table->get_max_merged_trans_version(),
        table->get_upper_trans_version(),
        table->get_start_log_ts(),
        table->get_end_log_ts(),
        table->get_max_log_ts(),
        table->get_ref(),
        static_cast<ObSSTable*>(table)->has_compact_row());
  }
}

int64_t ObPrintableTableStore::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObTableStore_Pretty");
    J_COLON();
    J_KV(KP(this),
        K_(pkey),
        K_(table_id),
        K_(uptime),
        K_(table_count),
        K_(start_pos),
        K_(inc_pos),
        K_(is_ready_for_read),
        K_(replay_tables),
        K_(multi_version_start),
        KPC_(pg_memtable_mgr),
        KPC_(complement_minor_sstable));
    J_COMMA();
    BUF_PRINTF("table_array");
    J_COLON();
    J_OBJ_START();
    if (table_count_ > 0 || complement_minor_sstable_ != nullptr) {
      ObCurTraceId::TraceId* trace_id = ObCurTraceId::get_trace_id();
      J_NEWLINE();
      // table_type| partition_id|version| base_version|multi_version|snapshot_version
      //      |upper_trans_version|start_log_ts|end_log_ts|max_log_ts|ref
      BUF_PRINTF("[%ld] [ ", GETTID());
      BUF_PRINTO(PC(trace_id));
      BUF_PRINTF(" ] ");
      BUF_PRINTF(" %-8s %-10s %-8s %-16s %-16s %-16s %-16s %-19s %-16s %-16s %-16s %-4s %-4s \n",
          "pos",
          "table_type",
          "version",
          "base_version",
          "multi_ver_start",
          "snapshot_version",
          "max_merge_ver",
          "upper_trans_ver",
          "start_log_ts",
          "end_log_ts",
          "max_log_ts",
          "ref",
          "cpat");
      for (int64_t i = 0; i < table_count_; ++i) {
        table_to_string(tables_[i], i == start_pos_ ? "start" : (i == inc_pos_ ? "inc" : " "), buf, buf_len, pos);
        if (i < table_count_ - 1) {
          J_NEWLINE();
        }
      }
      if (nullptr != complement_minor_sstable_) {
        if (table_count_ > 0) {
          J_NEWLINE();
        }
        table_to_string(complement_minor_sstable_, "comple", buf, buf_len, pos);
      }
    } else {
      J_EMPTY_OBJ();
    }
    J_OBJ_END();
    J_OBJ_END();
  }
  return pos;
}

int ObTableStore::get_recovery_point_tables(const int64_t snapshot_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  bool can_read = false;
  bool contain_snapshot_version = false;
  int64_t inc_base_pos = -1;
  ObArray<ObITable*> major_tables;
  int64_t need_major_pos = 0;
  ObITable* major_table = NULL;
  handle.reset();
  int64_t major_snapshot_version = 0;
  int64_t needed_inc_pos = -1;
  if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(snapshot_version));
  } else if (0 == table_count_ || !is_ready_for_read_) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("table store not ready for read", K(ret), K(PRETTY_TS(*this)));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(snapshot_version), K(PRETTY_TS(*this)));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_pos_; ++i) {
      ObITable* tmp_major_table = tables_[i];
      if (OB_FAIL(major_tables.push_back(tmp_major_table))) {
        LOG_WARN("failed to push major table into array", K(ret), KP(tmp_major_table));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(find_need_major_sstable(major_tables, snapshot_version, need_major_pos, major_table))) {
        LOG_WARN("failed to find_need_major_sstable", K(ret));
      } else if (need_major_pos < 0 || OB_ISNULL(major_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get need major sstable", K(ret), K(need_major_pos), KP(major_table), K(snapshot_version));
      } else if (OB_FAIL(handle.add_table(major_table))) {
        LOG_WARN("failed to add major table into handle", K(ret), KPC(major_table));
      } else {
        major_snapshot_version = major_table->get_snapshot_version();
      }
    }

    if (OB_SUCC(ret)) {
      // find first suitable inc pos
      for (int64_t i = inc_pos_; i < table_count_; ++i) {
        ObITable* table = tables_[i];
        if (table->get_upper_trans_version() > major_snapshot_version) {
          needed_inc_pos = i;
          break;
        }
      }

      if (needed_inc_pos < 0) {
      } else {
        for (int64_t i = needed_inc_pos; OB_SUCC(ret) && i < table_count_; ++i) {
          ObITable* table = tables_[i];
          if (OB_FAIL(handle.add_table(table))) {
            LOG_WARN("failed to add table into handle", K(ret), KPC(table));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (nullptr != complement_minor_sstable_) {
        if (OB_FAIL(handle.add_table(complement_minor_sstable_))) {
          LOG_WARN("failed to add sstable into handle", K(ret));
        }
      }
    }
  }
  return ret;
}
