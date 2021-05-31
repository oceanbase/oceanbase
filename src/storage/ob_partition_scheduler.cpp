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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/ob_partition_scheduler.h"
#include "storage/ob_partition_merge_task.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_build_index_task.h"
#include "lib/time/ob_time_utility.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/stat/ob_stat_manager.h"
#include "share/ob_thread_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/ob_sstable_merge_info_mgr.h"
#include "share/ob_task_define.h"
#include "share/ob_index_status_table_operator.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"
#include "storage/ob_sstable_row_whole_scanner.h"
#include "storage/ob_partition_storage.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/ob_i_store.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server.h"
#include "storage/ob_file_system_util.h"
#include "storage/ob_pg_storage.h"
#include <algorithm>

namespace oceanbase {
namespace storage {
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::blocksstable;
using namespace oceanbase::compaction;
using namespace share::schema;
using namespace share;
using namespace memtable;
static const int64_t TENANT_BUCKET_NUM = 128;
/*
 * -----------------------------------------------ObBloomfilterBuildTask---------------------------------------------------
 */
ObBloomFilterBuildTask::ObBloomFilterBuildTask(const uint64_t table_id, const blocksstable::MacroBlockId& macro_id,
    const int64_t prefix_len, const ObITable::TableKey& table_key)
    : IObDedupTask(T_BLOOMFILTER),
      table_id_(table_id),
      macro_id_(macro_id),
      prefix_len_(prefix_len),
      table_key_(table_key),
      access_param_(),
      access_context_(),
      allocator_(ObModIds::OB_BLOOM_FILTER)
{}

ObBloomFilterBuildTask::~ObBloomFilterBuildTask()
{}

int64_t ObBloomFilterBuildTask::hash() const
{
  uint64_t hash_val = macro_id_.hash();
  hash_val = murmurhash(&table_id_, sizeof(uint64_t), hash_val);
  return hash_val;
}

bool ObBloomFilterBuildTask::operator==(const IObDedupTask& other) const
{
  bool is_equal = false;
  if (this == &other) {
    is_equal = true;
  } else {
    if (get_type() == other.get_type()) {
      // it's safe to do this transformation, we have checked the task's type
      const ObBloomFilterBuildTask& o = static_cast<const ObBloomFilterBuildTask&>(other);
      is_equal = (o.table_id_ == table_id_) && (o.macro_id_ == macro_id_);
    }
  }
  return is_equal;
}

int64_t ObBloomFilterBuildTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

IObDedupTask* ObBloomFilterBuildTask::deep_copy(char* buffer, const int64_t buf_size) const
{
  ObBloomFilterBuildTask* task = NULL;
  if (NULL != buffer && buf_size >= get_deep_copy_size()) {
    task = new (buffer) ObBloomFilterBuildTask(table_id_, macro_id_, prefix_len_, table_key_);
  }
  return task;
}

int ObBloomFilterBuildTask::process()
{
  int ret = OB_SUCCESS;
  ObBloomFilterCacheValue bfcache_value;

  ObTenantStatEstGuard stat_est_guard(OB_SYS_TENANT_ID);
  if (OB_UNLIKELY(OB_INVALID_ID == table_id_) || OB_UNLIKELY(!macro_id_.is_valid()) || OB_UNLIKELY(prefix_len_ <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("The bloom filter build task is not valid, ", K_(table_id), K_(macro_id), K_(prefix_len), K(ret));
  } else if (OB_FAIL(build_bloom_filter())) {
    LOG_WARN("Fail to build bloom filter, ", K(ret));
  } else {
    LOG_INFO("Success to build bloom filter, ", K_(table_id), K_(macro_id));
  }

  return ret;
}

int ObBloomFilterBuildTask::build_bloom_filter()
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObSSTableRowWholeScanner* scanner = NULL;
  const ObStoreRow* row = NULL;
  ObStoreRowkey rowkey;
  share::schema::ObColDesc col_desc;
  ObExtStoreRange range;
  ObStoreCtx store_ctx;
  const uint64_t tenant_id = extract_tenant_id(table_id_);
  ObBlockCacheWorkingSet block_cache_ws;
  bool need_build = false;
  ObTableHandle table_handle;
  ObSSTable* sstable = NULL;
  ObFullMacroBlockMeta meta;
  ObIPartitionGroupGuard pg_guard;
  ObStorageFile* pg_file = nullptr;
  ObIMemtableCtxFactory* memctx_factory = ObPartitionService::get_instance().get_mem_ctx_factory();
  CREATE_WITH_TEMP_ENTITY(TABLE_SPACE, table_id_)
  {
    if (OB_FAIL(ObFileSystemUtil::get_pg_file_with_guard(table_key_.get_partition_key(), pg_guard, pg_file))) {
      LOG_WARN("fail to get pg_file", K(ret), K(table_key_));
    } else if (OB_FAIL(OB_STORE_CACHE.get_bf_cache().check_need_build(
                   ObBloomFilterCacheKey(table_id_, macro_id_, pg_file->get_file_id(), prefix_len_), need_build))) {
      STORAGE_LOG(WARN, "Fail to check need build, ", K(ret));
    } else if (!need_build) {
      // already in cache,do nothing
    } else if (OB_ISNULL(memctx_factory)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memtable ctx factory is null", K(ret));
    } else if (OB_FAIL(block_cache_ws.init(tenant_id))) {
      LOG_WARN("block_cache_ws init failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(table_key_, table_handle))) {
      LOG_WARN("fail to acquire table", K(ret));
    } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
      LOG_WARN("failed to get table", K_(table_key), K(ret));
    } else if (OB_ISNULL(sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable is null, unexpected ", K(ret));
    } else if (OB_FAIL(sstable->get_meta(macro_id_, meta))) {
      LOG_WARN("fail to get meta", K(ret), K(macro_id_));
    } else if (!meta.is_valid()) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, meta must not be null", K(ret), K(meta));
    } else {
      if (OB_ISNULL(buf = ob_malloc(sizeof(ObSSTableRowWholeScanner), ObModIds::OB_BLOOM_FILTER))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory, ", "size", sizeof(ObSSTableRowWholeScanner), K(ret));
      } else {
        scanner = new (buf) ObSSTableRowWholeScanner();
      }

      // prepare scan param
      if (OB_SUCC(ret)) {
        access_param_.reset();
        if (OB_FAIL(access_param_.out_col_desc_param_.init())) {
          LOG_WARN("init out cols fail", K(ret));
        } else {
          access_param_.iter_param_.table_id_ = table_id_;
          access_param_.iter_param_.schema_version_ = meta.meta_->schema_version_;
          access_param_.iter_param_.rowkey_cnt_ = meta.schema_->rowkey_column_number_;
          access_param_.iter_param_.out_cols_ = &access_param_.out_col_desc_param_.get_col_descs();
          scanner = new (buf) ObSSTableRowWholeScanner();
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < prefix_len_; ++i) {
          col_desc.col_id_ = meta.schema_->column_id_array_[i];
          col_desc.col_type_ = meta.schema_->column_type_array_[i];
          if (OB_FAIL(access_param_.out_col_desc_param_.push_back(col_desc))) {
            LOG_WARN("Fail to push the col to param columns, ", K(ret));
          }
        }
      }
      // prepare scan param
      if (OB_SUCC(ret)) {
        access_param_.reset();
        if (OB_FAIL(access_param_.out_col_desc_param_.init())) {
          LOG_WARN("init out cols fail", K(ret));
        } else {
          access_param_.iter_param_.table_id_ = table_id_;
          access_param_.iter_param_.schema_version_ = meta.meta_->schema_version_;
          access_param_.iter_param_.rowkey_cnt_ = meta.meta_->rowkey_column_number_;
          access_param_.iter_param_.out_cols_ = &access_param_.out_col_desc_param_.get_col_descs();
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < prefix_len_; ++i) {
          col_desc.col_id_ = meta.schema_->column_id_array_[i];
          col_desc.col_type_ = meta.schema_->column_type_array_[i];
          if (OB_FAIL(access_param_.out_col_desc_param_.push_back(col_desc))) {
            LOG_WARN("Fail to push the col to param columns, ", K(ret));
          }
        }
      }

      // prepare scan context
      if (OB_SUCC(ret)) {
        access_context_.reset();
        common::ObQueryFlag query_flag;
        common::ObVersionRange trans_version_range;
        ObPartitionKey pg_key;
        query_flag.set_ignore_trans_stat();

        range.get_range().set_whole_range();
        trans_version_range.base_version_ = 0;
        trans_version_range.multi_version_start_ = 0;
        trans_version_range.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;

        if (OB_FAIL(ObPartitionService::get_instance().get_pg_key(table_key_.pkey_, pg_key))) {
          LOG_WARN("failed to get_pg_key", K(ret), K(table_key_));
        } else if (OB_ISNULL(store_ctx.mem_ctx_ = memctx_factory->alloc())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memtable ctx failed", K(ret));
        } else if (OB_FAIL(store_ctx.init_trans_ctx_mgr(pg_key))) {
          LOG_WARN("failed to init_trans_ctx_mgr", K(ret), K(pg_key));
        } else if (OB_FAIL(access_context_.init(
                       query_flag, store_ctx, allocator_, allocator_, block_cache_ws, trans_version_range))) {
          LOG_WARN("failed to init accesss_context", K(ret));
        }
      }

      // scan and build
      if (OB_SUCC(ret)) {
        ObIPartitionGroupGuard guard;
        ObBloomFilterCacheValue bfcache_value;
        blocksstable::ObMacroBlockCtx macro_block_ctx;

        if (OB_FAIL(bfcache_value.init(prefix_len_, meta.meta_->row_count_))) {
          LOG_WARN("Fail to init bloom filter, ", K(ret));
        } else if (OB_FAIL(sstable->get_macro_block_ctx(macro_id_, macro_block_ctx))) {
          LOG_WARN("fail to get macro block ctx", K(ret));
        } else if (OB_FAIL(
                       scanner->open(access_param_.iter_param_, access_context_, &range, macro_block_ctx, sstable))) {
          LOG_WARN("fail to set context", K(ret));
        } else {
          while (OB_SUCC(scanner->get_next_row(row))) {
            rowkey.assign(row->row_val_.cells_, row->row_val_.count_);
            if (OB_FAIL(bfcache_value.insert(rowkey))) {
              LOG_WARN("Fail to insert rowkey to bfcache, ", K(ret));
              break;
            }
          }

          if (OB_ITER_END == ret) {
            ObStorageCacheContext context;
            context.set(block_cache_ws);
            if (OB_FAIL(context.bf_cache_->put_bloom_filter(
                    table_id_, macro_id_, pg_file->get_file_id(), bfcache_value, true))) {
              LOG_WARN("Fail to put value to bloom_filter_cache, ", K(ret));
            } else {
              FLOG_INFO("Succ to put a new bloomfilter", K(macro_id_), K_(table_id), K(bfcache_value));
            }
          }
        }
      }
      if (NULL != scanner) {
        scanner->~ObSSTableRowWholeScanner();
        ob_free(scanner);
      }
      if (nullptr != store_ctx.mem_ctx_) {
        memctx_factory->free(store_ctx.mem_ctx_);
        store_ctx.mem_ctx_ = nullptr;
      }
    }
  }

  return ret;
}

/*
 * -----------------------------------------------ObBloomfilterLoadTask---------------------------------------------------
 */
ObBloomFilterLoadTask::ObBloomFilterLoadTask(
    const uint64_t table_id, const blocksstable::MacroBlockId& macro_id, const ObITable::TableKey& table_key)
    : IObDedupTask(T_BLOOMFILTER_LOAD), table_id_(table_id), macro_id_(macro_id), table_key_(table_key)
{}

ObBloomFilterLoadTask::~ObBloomFilterLoadTask()
{}

int64_t ObBloomFilterLoadTask::hash() const
{
  uint64_t hash_val = macro_id_.hash();
  hash_val = murmurhash(&table_id_, sizeof(uint64_t), hash_val);
  return hash_val;
}

bool ObBloomFilterLoadTask::operator==(const IObDedupTask& other) const
{
  bool is_equal = false;
  if (this == &other) {
    is_equal = true;
  } else {
    if (get_type() == other.get_type()) {
      // it's safe to do this transformation, we have checked the task's type
      const ObBloomFilterLoadTask& o = static_cast<const ObBloomFilterLoadTask&>(other);
      is_equal = (o.table_id_ == table_id_) && (o.macro_id_ == macro_id_);
    }
  }
  return is_equal;
}

int64_t ObBloomFilterLoadTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

IObDedupTask* ObBloomFilterLoadTask::deep_copy(char* buffer, const int64_t buf_size) const
{
  ObBloomFilterLoadTask* task = NULL;
  if (NULL != buffer && buf_size >= get_deep_copy_size()) {
    task = new (buffer) ObBloomFilterLoadTask(table_id_, macro_id_, table_key_);
  }
  return task;
}

int ObBloomFilterLoadTask::process()
{
  int ret = OB_SUCCESS;

  ObTenantStatEstGuard stat_est_guard(OB_SYS_TENANT_ID);
  if (OB_UNLIKELY(OB_INVALID_ID == table_id_) || OB_UNLIKELY(!macro_id_.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("The bloom filter build task is not valid, ", K_(table_id), K_(macro_id), K(ret));
  } else {
    ObBloomFilterCacheValue bf_cache_value;
    ObBloomFilterDataReader bf_macro_reader;
    blocksstable::ObMacroBlockCtx macro_block_ctx;  // TODO(): fix build bloom filter later
    ObTableHandle table_handle;
    ObSSTable* sstable = NULL;
    blocksstable::ObStorageFileHandle file_handle;
    ObStorageFile* file = NULL;

    if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(table_key_, table_handle))) {
      LOG_WARN("failed to get table handle", K_(table_key), K(ret));
    } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
      LOG_WARN("failed to get table", K_(table_key), K(ret));
    } else if (OB_ISNULL(sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable is null, unexpected, ", K(ret));
    } else if (OB_FAIL(sstable->get_bf_macro_block_ctx(macro_block_ctx))) {
      LOG_WARN("fail to get macro block ctx", K(ret));
    } else if (OB_UNLIKELY(macro_id_ != macro_block_ctx.get_macro_block_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro_id not same with macro_block_ctx", K_(macro_id), K(macro_block_ctx.get_macro_block_id()), K(ret));
    } else if (OB_FAIL(file_handle.assign(sstable->get_storage_file_handle()))) {
      LOG_WARN("fail to get file handle", K(ret), K(sstable->get_storage_file_handle()));
    } else if (OB_ISNULL(file = file_handle.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(file_handle));
    } else if (OB_FAIL(bf_macro_reader.read_bloom_filter(macro_block_ctx, file, bf_cache_value))) {
      LOG_WARN("Failed to read bloomfilter cache", K_(table_id), K_(macro_id), K(ret));
    } else if (OB_UNLIKELY(!bf_cache_value.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected bloomfilter cache value", K_(table_id), K_(macro_id), K(bf_cache_value), K(ret));
    } else if (OB_FAIL(ObStorageCacheSuite::get_instance().get_bf_cache().put_bloom_filter(
                   table_id_, macro_id_, file->get_file_id(), bf_cache_value))) {
      LOG_WARN("Fail to put value to bloom filter cache", K_(table_id), K_(macro_id), K(bf_cache_value), K(ret));
    } else {
      LOG_INFO("Success to load bloom filter", K_(table_id), K_(macro_id));
    }
  }

  return ret;
}

/**
 * -----------------------------------------------ObMergeStatistic--------------------------------------------------------
 */

ObMergeStatEntry::ObMergeStatEntry() : frozen_version_(0), start_time_(0), finish_time_(0)
{}

void ObMergeStatEntry::reset()
{
  frozen_version_ = 0;
  start_time_ = 0;
  finish_time_ = 0;
}

ObMergeStatistic::ObMergeStatistic() : lock_()
{}

ObMergeStatistic::~ObMergeStatistic()
{}

int ObMergeStatistic::notify_merge_start(const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  ObMergeStatEntry* pentry = NULL;
  if (OB_UNLIKELY(frozen_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(frozen_version), K(ret));
  } else {
    obsys::CWLockGuard guard(lock_);
    if (OB_FAIL(search_entry(frozen_version, pentry))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        pentry = &(stats_[frozen_version % MAX_KEPT_HISTORY]);
        pentry->reset();
        pentry->frozen_version_ = frozen_version;
        pentry->start_time_ = ::oceanbase::common::ObTimeUtility::current_time();
      } else {
        LOG_WARN("Fail to search entry, ", K(ret));
      }
    } else if (OB_ISNULL(pentry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected error, the pentry is NULL, ", K(ret));
    }
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMergeStatistic::notify_merge_finish(const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  ObMergeStatEntry* pentry = NULL;
  if (OB_UNLIKELY(frozen_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(frozen_version), K(ret));
  } else {
    obsys::CWLockGuard guard(lock_);
    if (OB_FAIL(search_entry(frozen_version, pentry))) {
      LOG_WARN("Fail to search entry, ", K(ret));
    } else if (OB_ISNULL(pentry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected error, the pentry is NULL, ", K(ret));
    } else {
      if (0 == pentry->finish_time_) {
        pentry->finish_time_ = ::oceanbase::common::ObTimeUtility::current_time();
        LOG_INFO("set merge finish time",
            K(frozen_version),
            "cost_time",
            (pentry->finish_time_ - pentry->start_time_) / 1000000,
            "start_time",
            time2str(pentry->start_time_),
            "finish_time",
            time2str(pentry->finish_time_));
      }
    }
  }
  return ret;
}

int ObMergeStatistic::get_entry(const int64_t frozen_version, ObMergeStatEntry& entry)
{
  int ret = OB_SUCCESS;
  ObMergeStatEntry* pentry = NULL;
  if (OB_UNLIKELY(frozen_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(frozen_version), K(ret));
  } else {
    obsys::CRLockGuard guard(lock_);
    if (OB_FAIL(search_entry(frozen_version, pentry))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        entry.reset();
      } else {
        LOG_WARN("Fail to search entry, ", K(frozen_version), K(ret));
      }
    } else {
      entry = *pentry;
    }
  }
  return ret;
}

int ObMergeStatistic::search_entry(const int64_t frozen_version, ObMergeStatEntry*& pentry)
{
  int ret = OB_SUCCESS;
  pentry = NULL;
  if (OB_UNLIKELY(frozen_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(frozen_version), K(ret));
  } else if (stats_[frozen_version % MAX_KEPT_HISTORY].frozen_version_ != frozen_version) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    pentry = &(stats_[frozen_version % MAX_KEPT_HISTORY]);
  }
  return ret;
}

/*
 *  ----------------------------------------------ObMinorMergeHistory--------------------------------------------------
 */

ObMinorMergeHistory::ObMinorMergeHistory(const uint64_t tenant_id) : count_(0), tenant_id_(tenant_id)
{}

ObMinorMergeHistory::~ObMinorMergeHistory()
{}

int ObMinorMergeHistory::notify_minor_merge_start(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (MAX_MINOR_HISTORY == count_) {
    MEMMOVE(snapshot_history_, &snapshot_history_[1], sizeof(int64_t) * (count_ - 1));
    --count_;
  }
  int64_t insert_pos = 0;
  for (int64_t i = count_ - 1; - 1 == insert_pos && i >= 0; --i) {
    if (snapshot_history_[i] < snapshot_version) {
      insert_pos = i + 1;
    }
  }
  MEMMOVE(&snapshot_history_[insert_pos + 1], &snapshot_history_[insert_pos], sizeof(int64_t) * (count_ - insert_pos));
  snapshot_history_[insert_pos] = snapshot_version;
  ++count_;
  SERVER_EVENT_ADD("minor_merge",
      "minor merge start",
      "tenant_id",
      tenant_id_,
      "snapshot_version",
      snapshot_version,
      "checkpoint_type",
      "DATA_CKPT",
      "checkpoint_cluster_version",
      GET_MIN_CLUSTER_VERSION());

  FLOG_INFO("[MINOR_MERGE_HISTORY] minor merge start", K(tenant_id_), K(snapshot_version), K_(count));
  return ret;
}

int ObMinorMergeHistory::notify_minor_merge_finish(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (count_ > 0) {
    int64_t i = 0;
    bool is_found = false;
    for (; i < count_; ++i) {
      if (snapshot_version >= snapshot_history_[i]) {
        SERVER_EVENT_ADD("minor_merge",
            "minor merge finish",
            "tenant_id",
            tenant_id_,
            "snapshot_version",
            snapshot_history_[i],
            "checkpoint_type",
            "DATA_CKPT",
            "checkpoint_cluster_version",
            GET_MIN_CLUSTER_VERSION());
        FLOG_INFO("[MINOR_MERGE_HISTORY] minor merge finish", K(tenant_id_), K(snapshot_version), K_(count));
        is_found = true;
      } else {
        break;
      }
    }
    if (is_found) {
      if (i < count_) {
        MEMMOVE(snapshot_history_, &snapshot_history_[i], sizeof(int64_t) * (count_ - i));
      }
      count_ -= i;
      LOG_INFO("[MINOR_MERGE_HISTORY] notify minor merge finish", K(tenant_id_), K_(count));
    }
  }
  return ret;
}

/*
 * -----------------------------------------------ObPartitionScheduler---------------------------------------------------
 */
ObPartitionScheduler::MergeRetryTask::MergeRetryTask()
{}

ObPartitionScheduler::MergeRetryTask::~MergeRetryTask()
{}

void ObPartitionScheduler::MergeRetryTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::current_time();
  if (OB_FAIL(ObPartitionScheduler::get_instance().merge_all())) {
    LOG_WARN("Fail to merge all partition, ", K(ret));
  }
  cost_ts = ObTimeUtility::current_time() - cost_ts;
  LOG_INFO("MergeRetryTask", K(cost_ts));
}

void ObPartitionScheduler::WriteCheckpointTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (is_enable_) {
    int64_t cost_ts = ObTimeUtility::current_time();
    if (OB_FAIL(ObPartitionScheduler::get_instance().write_checkpoint())) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to write_checkpoint", K(ret));
      }
    }
    cost_ts = ObTimeUtility::current_time() - cost_ts;
    LOG_INFO("WriteCheckpointTask", K(cost_ts));
  } else {
    LOG_INFO("WriteCheckpointTask is disabled");
  }
}

ObPartitionScheduler::MinorMergeScanTask::MinorMergeScanTask()
{}

ObPartitionScheduler::MinorMergeScanTask::~MinorMergeScanTask()
{}

void ObPartitionScheduler::MinorMergeScanTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPartitionScheduler::get_instance().schedule_minor_merge_all())) {
    LOG_WARN("Fail to scan minor merge task", K(ret));
  }
  if (OB_FAIL(ObPartitionScheduler::get_instance().check_release_memtable())) {
    LOG_WARN("Fail to check release memtable task", K(ret));
  }
}

void ObPartitionScheduler::MergeCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_MERGE_CHECK_TASK);
  int64_t cost_ts = ObTimeUtility::current_time();
  if (OB_FAIL(ObPartitionScheduler::get_instance().check_all())) {
    LOG_WARN("fail to check all", K(ret));
  }
  cost_ts = ObTimeUtility::current_time() - cost_ts;
  LOG_INFO("MergeCheckTask", K(cost_ts));
}

void ObPartitionScheduler::UpdateCacheInfoTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPartitionScheduler::get_instance().update_min_sstable_schema_info())) {
    LOG_WARN("Fail to scan minor merge task", K(ret));
  }
}

ObPartitionScheduler::ObPartitionScheduler()
    : frozen_version_(0),
      merged_version_(0),
      queue_(),
      bf_queue_(),
      partition_service_(NULL),
      schema_service_(NULL),
      report_(NULL),
      write_ckpt_task_(),
      inner_task_(),
      is_major_scan_to_be_continued_(false),
      is_minor_scan_to_be_continued_(false),
      failure_fast_retry_interval_us_(DEFAULT_FAILURE_FAST_RETRY_INTERVAL_US),
      minor_merge_schedule_interval_(DEFAULT_MINOR_MERGE_SCAN_INTERVAL_US),
      merge_statistic_(),
      frozen_version_lock_(),
      timer_lock_(),
      first_most_merged_(false),
      inited_(false),
      merge_check_task_(),
      is_stop_(false),
      clear_unused_trans_status_task_(),
      report_version_(0),
      update_cache_info_task_()
{}

ObPartitionScheduler::~ObPartitionScheduler()
{
  destroy();
}

ObPartitionScheduler& ObPartitionScheduler::get_instance()
{
  static ObPartitionScheduler instance_;
  return instance_;
}

int ObPartitionScheduler::init(
    ObPartitionService& partition_service, ObMultiVersionSchemaService& schema_service, ObIPartitionReport& report)
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  int32_t merge_work_thread_num = get_default_merge_thread_cnt();
  const int64_t minor_schedule_interval = GCONF._ob_minor_merge_schedule_interval;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObPartitionScheduler has been inited.");
  } else if (OB_UNLIKELY(minor_schedule_interval < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(minor_schedule_interval), K(ret));
  } else if (OB_FAIL(queue_.init(get_max_merge_thread_cnt(), "ParSchedule", PS_TASK_QUEUE_SIZE, PS_TASK_MAP_SIZE))) {
    LOG_WARN("Fail to init queue, ", K(ret));
  } else if (OB_FAIL(queue_.set_work_thread_num(merge_work_thread_num))) {
    LOG_WARN("Fail to set work_thread_num.", K(ret), K(merge_work_thread_num));
  } else if (OB_FAIL(queue_.set_thread_dead_threshold(DEFAULT_THREAD_DEAD_THRESHOLD))) {
    LOG_WARN("Fail to set thread dead threshold", K(ret));
  } else if (OB_FAIL(bf_queue_.init(BLOOM_FILTER_BUILD_THREAD_CNT, "BFBuildTask"))) {
    LOG_WARN("Fail to init bloom filter queue, ", K(ret));
  } else if (OB_FAIL(tenant_snapshot_map_.create(TENANT_BUCKET_NUM, common::ObModIds::OB_PARTITION_SCHEDULER))) {
    LOG_WARN("failed to create tenant_snapshot_map_", K(ret));
  } else if (OB_FAIL(
                 current_tenant_snapshot_map_.create(TENANT_BUCKET_NUM, common::ObModIds::OB_PARTITION_SCHEDULER))) {
    LOG_WARN("failed to create tenant_snapshot_map_", K(ret));
  } else if (OB_FAIL(minor_merge_his_map_.create(TENANT_BUCKET_NUM, common::ObModIds::OB_PARTITION_SCHEDULER))) {
    LOG_WARN("failed to create minor_merge_his_map_", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::MinorScan))) {
    LOG_WARN("Fail to init timer, ", K(ret));
  } else if (OB_FAIL(min_sstable_schema_version_map_.create(TENANT_BUCKET_NUM, "min_sch_version"))) {
    LOG_WARN("failed to create min_sstable_schema_version_map_", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::MinorScan, minor_scan_task_, minor_schedule_interval, true))) {
    LOG_WARN("Fail to schedule minor merge scan task, ", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::MajorScan))) {
    LOG_WARN("Fail to init timer, ", K(ret));
  } else if (OB_FAIL(
                 TG_SCHEDULE(lib::TGDefIDs::MajorScan, inner_task_, DEFAULT_MAJOR_MERGE_RETRY_INTERVAL_US, repeat))) {
    LOG_WARN("failed to schedule retry task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(
                 lib::TGDefIDs::MajorScan, write_ckpt_task_, DEFAULT_WRITE_CHECKPOINT_INTERVAL_US, repeat))) {
    LOG_WARN("Fail to schedule inner task, ", K(ret));
  } else if (OB_FAIL(
                 TG_SCHEDULE(lib::TGDefIDs::MajorScan, merge_check_task_, DEFAULT_MERGE_CHECK_INTERVAL_US, repeat))) {
    LOG_WARN("fail to schedule merge check task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::MajorScan,
                 clear_unused_trans_status_task_,
                 DEFAULT_CLEAR_UNUSED_TRANS_INTERVAL_US,
                 repeat))) {
    LOG_WARN("fail to schedule clear_unused_trans_status_task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::MajorScan,
                 update_cache_info_task_,
                 DEFAULT_MIN_SCHEMA_VERSION_UPDATE_INTERVAL_US,
                 repeat))) {
    LOG_WARN("fail to schedule update_cache_info_task", K(ret));
  } else {
    queue_.set_label("queue");
    bf_queue_.set_label("bf_queue");
    partition_service_ = &partition_service;
    report_ = &report;
    schema_service_ = &schema_service;
    inited_ = true;
    minor_merge_schedule_interval_ = minor_schedule_interval;
  }

  if (!inited_) {
    destroy();
  }

  return ret;
}

void ObPartitionScheduler::destroy()
{
  stop();
  wait();
  TG_DESTROY(lib::TGDefIDs::MinorScan);
  TG_DESTROY(lib::TGDefIDs::MajorScan);
  queue_.destroy();
  bf_queue_.destroy();
  frozen_version_ = 0;
  merged_version_ = 0;
  report_version_ = 0;
  partition_service_ = NULL;
  schema_service_ = NULL;
  report_ = NULL;
  inited_ = false;
  LOG_INFO("The ObPartitionScheduler destroy.");
}

int ObPartitionScheduler::schedule_merge(const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  first_most_merged_ = true;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(frozen_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(frozen_version), K(ret));
  } else {
    obsys::CWLockGuard frozen_version_guard(frozen_version_lock_);
    lib::ObMutexGuard merge_guard(timer_lock_);
    if (frozen_version_ < frozen_version) {
      TG_CANCEL(lib::TGDefIDs::MinorScan, minor_task_for_major_);
      // schedule minor scan task immediately for faster merge schedule
      if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::MinorScan, minor_task_for_major_, 0))) {
        LOG_WARN("Fail to schedule minor scan task", K(frozen_version), K(ret));
      } else {
        frozen_version_ = frozen_version;
        LOG_INFO("succeed to schedule merge task with new frozen version", K(frozen_version));
        int tmp_ret = OB_SUCCESS;
        static const int64_t FAST_MAJOR_TASK_DELAY = 8 * 1000 * 1000L;  // 8s
        if (OB_UNLIKELY(OB_SUCCESS !=
                        (tmp_ret = TG_SCHEDULE(lib::TGDefIDs::MajorScan, fast_major_task_, FAST_MAJOR_TASK_DELAY)))) {
          LOG_WARN("failed to schedule fast major task", K(frozen_version), K(tmp_ret));
        }
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = merge_statistic_.notify_merge_start(frozen_version)))) {
          LOG_WARN("Fail to notify merge statistic start, ", K(frozen_version), K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

// pkey should be passed
int ObPartitionScheduler::schedule_merge(const ObPartitionKey& partition_key, bool& is_merged)
{
  int ret = OB_SUCCESS;
  int64_t frozen_version = 0;
  ObVersion base_version;
  is_merged = false;

  DEBUG_SYNC(DELAY_SCHEDULE_MERGE);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!partition_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(partition_key), K(ret));
  } else {
    {
      obsys::CRLockGuard frozen_version_guard(frozen_version_lock_);
      frozen_version = frozen_version_;
    }

    storage::ObIPartitionGroupGuard guard;
    ObPGPartitionGuard pg_part_guard;
    ObIPartitionGroup* pg = nullptr;
    if (OB_FAIL(partition_service_->get_partition(partition_key, guard))) {
      LOG_WARN("Fail to get partition, ", K(partition_key), K(ret));
    } else if (OB_ISNULL(pg = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to get partition, ", K(partition_key), K(ret));
    } else if (OB_FAIL(schedule_pg(MINI_MINOR_MERGE, *pg, static_cast<ObVersion>(frozen_version), is_merged))) {
      LOG_WARN("failed to schedule partition storage", K(ret), K(partition_key));
    } else if (OB_FAIL(schedule_pg(MAJOR_MERGE, *pg, static_cast<ObVersion>(frozen_version), is_merged))) {
      LOG_WARN("failed to schedule partition storage", K(ret), K(partition_key));
    }
  }
  return ret;
}

int ObPartitionScheduler::schedule_major_merge()
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_FAIL(TG_TASK_EXIST(lib::TGDefIDs::MajorScan, fast_major_task_, is_exist))) {
  } else {
    if (!is_exist) {
      if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::MajorScan, fast_major_task_, 0))) {
        LOG_WARN("failed to fast major task");
      }
    }
  }
  return ret;
}

int ObPartitionScheduler::schedule_minor_merge_all()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_merged = false;
  ObIPartitionArrayGuard partitions;
  int64_t cost_ts = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (!schema_service_->is_sys_full_schema()) {
    // wait schema_service ready
    ret = OB_EAGAIN;
    LOG_WARN("schema is not ready", K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Partition service is NULL.");
  } else if (OB_FAIL(partition_service_->get_all_partitions(partitions))) {
    LOG_WARN("Fail to get all partitions, ", K(ret));
  } else if (!schema_service_->is_sys_full_schema()) {
    // wait schema_service ready
    ret = OB_EAGAIN;
    LOG_WARN("schema is not ready", K(ret));
  } else {
    ObIPartitionGroup* partition = NULL;
    common::ObArray<memtable::ObMergePriorityInfo> merge_priority_infos;
    ObVersion invalid_version = ObVersion::MIN_VERSION;

    DEBUG_SYNC(MINOR_MERGE_TIMER_TASK);
    LOG_INFO("start try minor merge all");
    is_minor_scan_to_be_continued_ = false;

    if (OB_FAIL(sort_for_minor_merge(merge_priority_infos, partitions))) {
      STORAGE_LOG(WARN, "failed to sort for minor merge", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < merge_priority_infos.count(); i++) {
      partition = merge_priority_infos.at(i).partition_;
      if (OB_UNLIKELY(NULL == partition)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get partition failed", K(ret));
      } else {
        ObPartitionState state = partition->get_partition_state();
        const ObPartitionKey& pkey = partition->get_partition_key();
        if (is_leader_state(state) || is_follower_state(state)) {
          if (OB_SUCCESS != (tmp_ret = schedule_pg(MINI_MERGE, *partition, invalid_version, is_merged))) {
            if (OB_EAGAIN != tmp_ret && OB_SIZE_OVERFLOW != tmp_ret) {
              LOG_WARN("Fail to add merge task, ", K(pkey), K(tmp_ret));
            } else if (OB_SIZE_OVERFLOW == tmp_ret) {
              break;
            }
          }
          if (OB_SUCCESS != (tmp_ret = schedule_pg(MINI_MINOR_MERGE, *partition, invalid_version, is_merged))) {
            if (OB_EAGAIN != tmp_ret && OB_SIZE_OVERFLOW != tmp_ret) {
              LOG_WARN("Fail to add merge task, ", K(pkey), K(tmp_ret));
            } else if (OB_SIZE_OVERFLOW == tmp_ret) {
              break;
            }
          }
          if (OB_SUCCESS != (tmp_ret = schedule_pg(HISTORY_MINI_MINOR_MERGE, *partition, invalid_version, is_merged))) {
            if (OB_EAGAIN != tmp_ret && OB_SIZE_OVERFLOW != tmp_ret) {
              LOG_WARN("Fail to add merge task, ", K(pkey), K(tmp_ret));
            } else if (OB_SIZE_OVERFLOW == tmp_ret) {
              break;
            }
          }
        } else {
          LOG_DEBUG("skip partition no need minor merge", K(state), K(pkey));
        }
      }
    }
  }

  cost_ts = ObTimeUtility::current_time() - cost_ts;
  LOG_INFO("finish try minor merge all", K(cost_ts));
  return ret;
}

int ObPartitionScheduler::sort_for_minor_merge(
    common::ObArray<memtable::ObMergePriorityInfo>& merge_priority_infos, ObIPartitionArrayGuard& partitions)
{
  int ret = OB_SUCCESS;
  memtable::ObMergePriorityInfo merge_priority_info;
  ObIPartitionGroup* partition = NULL;
  ObMergePriorityCompare compare;
  if (OB_FAIL(merge_priority_infos.reserve(partitions.count()))) {
    STORAGE_LOG(WARN, "failed to reserve for merge_priority_infos", K(ret), K(partitions.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < partitions.count(); i++) {
    partition = partitions.at(i);
    if (OB_UNLIKELY(NULL == partition)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(ret));
    } else if (OB_FAIL(partition->get_merge_priority_info(merge_priority_info))) {
      STORAGE_LOG(WARN, "failed to get merge priority info", K(ret));
    } else {
      merge_priority_info.partition_ = partition;
      if (OB_FAIL(merge_priority_infos.push_back(merge_priority_info))) {
        STORAGE_LOG(WARN, "failed to push back merge priority info", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    std::sort(merge_priority_infos.begin(), merge_priority_infos.end(), compare);
  }

  return ret;
}

int ObPartitionScheduler::schedule_build_bloomfilter(
    const uint64_t table_id, const MacroBlockId macro_id, const int64_t prefix_len, const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(!macro_id.is_valid()) ||
             OB_UNLIKELY(prefix_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(table_id), K(macro_id), K(prefix_len), K(ret));
  } else {
    ObBloomFilterBuildTask task(table_id, macro_id, prefix_len, table_key);
    if (OB_FAIL(bf_queue_.add_task(task))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to add bloomfilter build task, ", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionScheduler::schedule_load_bloomfilter(
    const uint64_t table_id, const MacroBlockId& macro_id, const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(table_id), K(macro_id), K(ret));
  } else {
    ObBloomFilterLoadTask task(table_id, macro_id, table_key);
    if (OB_FAIL(bf_queue_.add_task(task))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Failed to add bloomfilter load task", K(ret));
      }
    }
  }

  return ret;
}

int64_t ObPartitionScheduler::get_frozen_version() const
{
  obsys::CRLockGuard frozen_version_guard(frozen_version_lock_);
  return frozen_version_;
}

int64_t ObPartitionScheduler::get_merged_version() const
{
  return merged_version_;
}

int64_t ObPartitionScheduler::get_merged_schema_version(const uint64_t tenant_id) const
{
  int64_t schema_version = OB_INVALID_VERSION;

  if (0 != merged_version_) {
    ObFreezeInfoSnapshotMgr::FreezeInfo freeze_info;
    if (OB_LIKELY(OB_SUCCESS == ObFreezeInfoMgrWrapper::get_instance().get_tenant_freeze_info_by_major_version(
                                    tenant_id, merged_version_, freeze_info))) {
      schema_version = freeze_info.schema_version;
    }
  }

  return schema_version;
}

void ObPartitionScheduler::set_fast_retry_interval(const int64_t failure_fast_retry_interval_us)
{
  if (failure_fast_retry_interval_us >= 0) {
    failure_fast_retry_interval_us_ = failure_fast_retry_interval_us;
  }
}

int ObPartitionScheduler::reload_config()
{
  int ret = OB_SUCCESS;
  int32_t merge_thread_cnt = get_default_merge_thread_cnt();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (0 == common::ObServerConfig::get_instance().merge_thread_count) {
    if (OB_FAIL(queue_.set_work_thread_num(merge_thread_cnt))) {
      LOG_WARN("Fail to set default set default work_thread_num, ", K(ret), K(merge_thread_cnt));
    }
  } else if (OB_FAIL(queue_.set_work_thread_num(
                 static_cast<int32_t>(common::ObServerConfig::get_instance().merge_thread_count)))) {
    LOG_WARN("Fail to reload ObPartitionScheduler config, ", K(ret));
  }
  return ret;
}

int ObPartitionScheduler::schedule_pg_mini_merge(const ObPartitionKey& pg_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to schedule pg mini merge", K(ret), K(pg_key));
  } else {
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = nullptr;
    bool is_merged = false;
    if (OB_FAIL(partition_service_->get_partition(pg_key, guard))) {
      LOG_WARN("Failed to get pg partition", K(ret), K(pg_key));
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Failed to get partition group", K(ret), K(pg_key));
    } else if (OB_FAIL(schedule_pg(MINI_MERGE, *partition, ObVersion::MIN_VERSION, is_merged))) {
      LOG_WARN("Failed to schedule pg mini merge", K(ret));
    }
  }

  return ret;
}

int ObPartitionScheduler::schedule_pg(
    const ObMergeType merge_type, ObIPartitionGroup& partition, const common::ObVersion& merge_version, bool& is_merged)
{
  int ret = E(EventTable::EN_SCHEDULE_MERGE) OB_SUCCESS;
  bool can_merge = false;
  bool need_merge = true;
  ObPartitionReplicaState stat = OB_UNKNOWN_REPLICA;
  ObPartitionKey pg_key = partition.get_partition_key();
  const bool is_major_merge = storage::is_major_merge(merge_type);
  const bool is_restore = partition.get_pg_storage().is_restore();
  ObPartitionArray pkeys;
  is_merged = false;
  bool need_merge_trans_table = MINI_MERGE == merge_type;
  bool need_fast_freeze = false;
  int64_t merge_log_ts = 0;
  int64_t trans_table_seq = -1;
  int64_t trans_end_log_ts = 0;
  int64_t trans_timestamp = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(is_major_merge && merge_version <= 0) || OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(is_major_merge), K(pg_key), K(partition), K(merge_version), K(ret));
  } else if (OB_FAIL(partition.get_replica_state(stat))) {
    LOG_WARN("Fail to get replica stat, ", K(ret), K(pg_key));
  } else if (OB_PERMANENT_OFFLINE_REPLICA == stat) {
    is_merged = true;
    LOG_INFO("Partition has been offline, no need to merge partition", K(pg_key));
  } else if (OB_FAIL(partition.check_can_do_merge(can_merge, need_merge))) {
    LOG_WARN("Fail to check can do merge, ", K(ret), K(partition), K(merge_version));
  } else if (!can_merge) {
    if (need_merge) {
      ret = OB_EAGAIN;
      LOG_INFO("partition can't merge now, will retry later", K(pg_key));
    } else {
      is_merged = true;
    }
  } else if (OB_FAIL(partition.get_all_pg_partition_keys(pkeys))) {
    LOG_WARN("failed to get_all_pg_partition_keys", K(ret), K(pg_key));
  } else if (need_merge_trans_table &&
             OB_FAIL(partition.get_pg_storage().check_need_merge_trans_table(
                 need_merge_trans_table, merge_log_ts, trans_table_seq, trans_end_log_ts, trans_timestamp))) {
    LOG_WARN("failed to check_need_merge_trans_table", K(ret), K(pg_key));
  } else if (need_merge_trans_table && OB_FAIL(schedule_trans_table_merge_dag(pg_key, merge_log_ts, trans_table_seq))) {
    LOG_WARN("failed to schedule_trans_table_merge_dag", K(ret), K(pg_key));
  }
#ifdef ERRSIM
  else if (OB_FAIL(E(EventTable::EN_SCHEDULE_DATA_MINOR_MERGE) OB_SUCCESS)) {
    // skip schedule minor merge data
    ret = OB_SUCCESS;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      STORAGE_LOG(ERROR, "skip schedule minor merge data");
    }
  }
#endif
  else {
    bool tmp_is_merged = !need_merge_trans_table;
    for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < pkeys.count(); ++i) {
      bool part_is_merged = false;
      ObPGPartitionGuard guard;
      ObPGPartition* pg_partition = nullptr;
      const ObPartitionKey& part_key = pkeys.at(i);
      if (OB_FAIL(partition.get_pg_partition(part_key, guard))) {
        LOG_WARN("failed to get pg partition", K(ret), K(pg_key), K(part_key));
      } else if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pg partition is null", K(ret), K(pg_key), K(part_key));
      } else if (OB_FAIL(schedule_partition(merge_type,
                     *pg_partition,
                     partition,
                     merge_version,
                     trans_end_log_ts,
                     trans_timestamp,
                     is_restore,
                     part_is_merged))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("failed to scheduler partition", K(ret), K(pg_key), K(part_key));
        } else if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_WARN("failed to schedule partition", K(ret), K(pg_key), K(part_key));
        }
      } else {
        tmp_is_merged &= part_is_merged;
      }
    }

    if (OB_SUCC(ret)) {
      is_merged = tmp_is_merged;
      if (is_merged && need_check_fast_freeze(merge_type, pg_key)) {
        ObFastFreezeChecker fast_freeze_checker(pg_key.get_tenant_id());
        if (fast_freeze_checker.need_check()) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS !=
              (tmp_ret = fast_freeze_checker.check_hotspot_need_fast_freeze(partition, need_fast_freeze))) {
            if (OB_ENTRY_NOT_EXIST != tmp_ret) {
              LOG_WARN("Failed to check need fast freeze for hotspot table", K(tmp_ret), K(pg_key));
            }
          } else if (!need_fast_freeze) {

          } else if (OB_SUCCESS != (tmp_ret = partition_service_->minor_freeze(pg_key))) {
            LOG_WARN("Failed to schedule fast freeze", K(tmp_ret), K(pg_key));
          } else {
            FLOG_INFO("Fast freeze success", K(pg_key), K(fast_freeze_checker));
          }
        }
      }
    }
  }
  LOG_TRACE("finish schedule_pg",
      K(pg_key),
      K(merge_type),
      K(merge_version),
      K(is_merged),
      K(need_merge),
      K(can_merge),
      K(need_fast_freeze));
  return ret;
}

int ObPartitionScheduler::schedule_partition(const ObMergeType merge_type, ObPGPartition& partition,
    ObIPartitionGroup& pg, const common::ObVersion& merge_version, const int64_t trans_table_end_log_ts,
    const int64_t trans_table_timestamp, const bool is_restore, bool& is_merged)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_partition_exist = false;
  ObPartitionKey pkey = partition.get_partition_key();
  const uint64_t fetch_tenant_id = is_inner_table(pkey.get_table_id()) ? OB_SYS_TENANT_ID : pkey.get_tenant_id();
  const bool is_major_merge = MAJOR_MERGE == merge_type;
  bool check_dropped_partition = true;
  bool is_finish = true;
  ObPartitionStorage* storage = static_cast<ObPartitionStorage*>(partition.get_storage());
  is_merged = false;
  if (OB_ISNULL(storage)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition storage is null", K(ret));
  } else if (is_major_merge && OB_FAIL(schema_service_->get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
    STORAGE_LOG(WARN, "fail to get schema guard", K(ret));
  } else if (is_major_merge &&
             OB_FAIL(schema_guard.check_partition_exist(
                 pkey.table_id_, pkey.get_partition_id(), check_dropped_partition, is_partition_exist))) {
    STORAGE_LOG(WARN, "fail to check partition exist", K(ret), K(pkey), "pg_key", pg.get_partition_key());
    // the partiton need to do minor merge, if its source partition cannot be found in schema
  } else if (is_major_merge && OB_UNLIKELY(!is_partition_exist) && !pg.is_in_dest_split()) {
    is_merged = true;
    LOG_INFO("The partition does not exist, no need to merge partition, ",
        K(pkey),
        "pg_key",
        pg.get_partition_key(),
        "is_in_dest_split",
        pg.is_in_dest_split());
  } else if (OB_FAIL(pg.check_physical_split(is_finish))) {
    LOG_WARN("failed to check physical split", K(ret), K(pkey), "pg_key", pg.get_partition_key());
  } else if (is_restore && MAJOR_MERGE == merge_type) {
    is_merged = true;
  } else {
    if (!is_finish) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = schedule_partition_split(partition, pg.get_partition_key()))) {
        if (OB_EAGAIN != tmp_ret && OB_SIZE_OVERFLOW != tmp_ret) {
          LOG_WARN("Fail to schedule partition split",
              K(tmp_ret),
              "pkey",
              partition.get_partition_key(),
              "split_info",
              partition.get_split_info(),
              "pg_key",
              pg.get_partition_key());
        }
      }
    }

    if (OB_FAIL(schedule_partition_merge(
            merge_type, partition, pg, merge_version, trans_table_end_log_ts, trans_table_timestamp, is_merged))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("Fail to schedule partition merge",
            K(ret),
            K(partition.get_partition_key()),
            "pg_key",
            pg.get_partition_key());
      }
    }
  }

  LOG_DEBUG("schedule_partition", K(pkey), K(pg.get_partition_key()), K(merge_type), K(ret));
  return ret;
}

// split of p0 is driven by p1, one time split base sstable
// one time split trans+memtable
int ObPartitionScheduler::schedule_partition_split(
    const bool is_major_split, ObPGPartition& partition, const ObPartitionKey& dest_pg_key)
{
  int ret = OB_SUCCESS;
  ObPartitionStorage* storage = static_cast<ObPartitionStorage*>(partition.get_storage());
  const ObPartitionKey dest_pkey = partition.get_partition_key();
  const ObPartitionKey src_pkey = partition.get_split_info().get_src_partition();
  ObPartitionSplitInfo split_info;
  ObSEArray<uint64_t, OB_MAX_SSTABLE_PER_TABLE> need_split_table_ids;
  common::ObVersion split_version;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_ISNULL(storage) || OB_UNLIKELY(!dest_pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(partition), K(dest_pkey));
  } else {
    if (OB_FAIL(
            storage->get_partition_store().get_split_table_ids(split_version, is_major_split, need_split_table_ids))) {
      LOG_WARN("failed to get split table ids", K(ret), K(src_pkey), K(dest_pkey));
    } else {
      if (!need_split_table_ids.empty()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < need_split_table_ids.count(); ++i) {
          uint64_t table_id = need_split_table_ids[i];
          if (OB_FAIL(schedule_split_sstable_dag(
                  is_major_split, split_version, src_pkey, dest_pkey, dest_pg_key, table_id))) {
            if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
              LOG_WARN("failed to schedule_split_sstable", K(ret), K(src_pkey), K(dest_pkey));
            }
          } else {
            STORAGE_LOG(INFO,
                "schedule split partition finish",
                K(src_pkey),
                K(dest_pkey),
                K(is_major_split),
                K(split_version));
          }
        }
        // dest partition cannot split before the source partition
      }
    }
  }
  return ret;
}

int ObPartitionScheduler::schedule_partition_split(ObPGPartition& partition, const ObPartitionKey& dest_pg_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schedule_partition_split(false, partition, dest_pg_key))) {
    STORAGE_LOG(WARN, "failed to schedule minor split", K(ret), K(dest_pg_key));
  } else if (OB_FAIL(schedule_partition_split(true, partition, dest_pg_key))) {
    STORAGE_LOG(WARN, "failed to schedule minor split", K(ret), K(dest_pg_key));
  }

  return ret;
}

int ObPartitionScheduler::schedule_partition_merge(const ObMergeType merge_type, ObPGPartition& partition,
    ObIPartitionGroup& pg, const common::ObVersion& frozen_version, const int64_t trans_table_end_log_ts,
    const int64_t trans_table_timestamp, bool& is_merged)
{
  int ret = E(EventTable::EN_SCHEDULE_MERGE) OB_SUCCESS;
  ObPartitionStorage* storage = static_cast<ObPartitionStorage*>(partition.get_storage());
  ObPartitionKey pkey = partition.get_partition_key();
  ObSEArray<uint64_t, OB_MAX_SSTABLE_PER_TABLE> need_merge_table_ids;
  common::ObVersion merge_version = frozen_version;
  bool need_merge = false;
  const bool is_major_merge = storage::is_major_merge(merge_type);
  const bool using_remote_memstore = pg.is_replica_using_remote_memstore();

  DEBUG_SYNC(AFTER_FREEZE_BEFORE_MINI_MERGE);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_ISNULL(storage) || OB_UNLIKELY(is_major_merge && merge_version <= 0) || OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(partition), K(frozen_version), K(ret));
  } else if (!partition.can_schedule_merge()) {
    // no nothing
  } else if (OB_FAIL(storage->get_partition_store().get_merge_table_ids(merge_type,
                 using_remote_memstore,
                 pg.is_in_dest_split(),
                 trans_table_end_log_ts,
                 trans_table_timestamp,
                 merge_version,
                 need_merge_table_ids,
                 need_merge))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN(
          "failed to get major merge table ids", K(ret), K(merge_version), K(pkey), "pg_key", pg.get_partition_key());
    }
  } else if (!need_merge) {
    is_merged = true;
  }

  if (OB_SUCC(ret) && need_merge_table_ids.count() > 0) {
    bool can_schedule = true;
    if (OB_FAIL(can_schedule_partition(merge_type, can_schedule))) {
      LOG_WARN("failed to check can schedule partition", K(ret), K(merge_type));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < need_merge_table_ids.count() && can_schedule; ++i) {
      const uint64_t table_id = need_merge_table_ids[i];
      if (OB_FAIL(check_need_merge_table(table_id, need_merge))) {
        LOG_WARN("failed to check need merge table", K(ret), K(table_id), "pg_key", pg.get_partition_key());
      } else if (!need_merge) {
        // no need to merge
      } else if (OB_FAIL(schedule_merge_sstable_dag(pg, merge_type, merge_version, pkey, table_id))) {
        if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
          LOG_WARN("failed to schedule_major_merge_sstable",
              K(ret),
              K(merge_version),
              K(table_id),
              "pg_key",
              pg.get_partition_key());
        }
      }
    }
  }
  LOG_DEBUG("schedule_partition",
      K(pkey),
      K(is_merged),
      K(need_merge_table_ids),
      K(merge_type),
      K(merge_version),
      K(need_merge));

  return ret;
}

int ObPartitionScheduler::check_need_merge_table(const uint64_t table_id, bool& need_merge)
{
  int ret = OB_SUCCESS;
  const share::schema::ObSimpleTableSchemaV2* table_schema = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  share::schema::ObMultiVersionSchemaService& schema_service =
      share::schema::ObMultiVersionSchemaService::get_instance();
  const uint64_t tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);

  need_merge = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_full_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_id));
  } else if (NULL == table_schema) {
    need_merge = false;
  }
  return ret;
}

int ObPartitionScheduler::alloc_merge_dag(const ObMergeType merge_type, ObSSTableMergeDag*& dag)
{
  int ret = OB_SUCCESS;
  int64_t mini_merge_thread = GCONF._mini_merge_concurrency;
  dag = nullptr;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (MAJOR_MERGE == merge_type) {
    ObSSTableMajorMergeDag* major_merge_dag = nullptr;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(major_merge_dag))) {
      LOG_WARN("failed to alloc major merge dag", K(ret));
    } else {
      dag = major_merge_dag;
    }
  } else if (mini_merge_thread > 0 && MINI_MERGE == merge_type) {
    ObSSTableMiniMergeDag* mini_merge_dag = nullptr;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(mini_merge_dag))) {
      LOG_WARN("failed to alloc mini merge dag", K(ret));
    } else {
      dag = mini_merge_dag;
    }
  } else {
    ObSSTableMinorMergeDag* minor_merge_dag = nullptr;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(minor_merge_dag))) {
      LOG_WARN("failed to alloc minor merge dag", K(ret));
    } else {
      dag = minor_merge_dag;
    }
  }

  return ret;
}

int ObPartitionScheduler::alloc_split_dag(ObSSTableSplitDag*& dag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else {
    ObSSTableSplitDag* split_dag = NULL;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(split_dag))) {
      LOG_WARN("failed to alloc split dag", K(ret));
    } else {
      dag = split_dag;
    }
  }
  return ret;
}

int ObPartitionScheduler::alloc_write_checkpoint_dag(ObWriteCheckpointDag*& dag)
{
  int ret = OB_SUCCESS;
  ObWriteCheckpointDag* write_checkpoint_dag = nullptr;
  dag = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(write_checkpoint_dag))) {
    LOG_WARN("failed to alloc write checkpoint dag", K(ret));
  } else {
    dag = write_checkpoint_dag;
  }
  return ret;
}

int ObPartitionScheduler::write_checkpoint()
{
  int ret = OB_SUCCESS;
  const int64_t frozen_version = get_frozen_version();
  ObWriteCheckpointDag* dag = NULL;
  ObWriteCheckpointTask* finish_task = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_FAIL(alloc_write_checkpoint_dag(dag))) {
    LOG_WARN("Fail to alloc dag", K(ret));
  } else if (OB_FAIL(dag->init(frozen_version))) {
    STORAGE_LOG(WARN, "Fail to init task", K(ret), K(frozen_version));
  } else if (OB_FAIL(dag->alloc_task(finish_task))) {
    STORAGE_LOG(WARN, "Fail to alloc task, ", K(ret));
  } else if (OB_FAIL(finish_task->init(frozen_version))) {
    STORAGE_LOG(WARN, "failed to init finish_task", K(ret));
  } else if (OB_FAIL(dag->add_task(*finish_task))) {
    STORAGE_LOG(WARN, "Fail to add task", K(ret), K(frozen_version));
  } else {
    if (OB_FAIL(ObDagScheduler::get_instance().add_dag(dag))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task, ", K(ret));
      }
    } else {
      STORAGE_LOG(INFO, "schedule write checkpoint DAG finish", K(frozen_version));
    }
  }

  if (OB_FAIL(ret) && NULL != dag) {
    ObDagScheduler::get_instance().free_dag(*dag);
  }
  return ret;
}

int ObPartitionScheduler::schedule_merge_sstable_dag(const ObIPartitionGroup& pg, const ObMergeType merge_type,
    const common::ObVersion& frozen_version, const common::ObPartitionKey& pkey, const uint64_t index_id)
{
  int ret = OB_SUCCESS;
  ObSSTableMergeDag* dag = NULL;
  ObSSTableMergePrepareTask* prepare_task = NULL;
  ObSSTableScheduleMergeParam param;
  ObIPartitionGroupGuard guard;
  memtable::ObMergePriorityInfo info;
  blocksstable::ObStorageFile* storage_file = NULL;
  const bool is_major_merge = storage::is_major_merge(merge_type);
  param.merge_type_ = merge_type;
  param.merge_version_ = frozen_version;
  param.pkey_ = pkey;
  param.index_id_ = index_id;
  param.schedule_merge_type_ = merge_type;
  param.pg_key_ = pg.get_partition_key();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (!is_major_merge && OB_FAIL(pg.get_merge_priority_info(info))) {
    LOG_WARN("fail to get merge priority, ", K(ret), K(pkey), "pg_key", pg.get_partition_key());
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(param.pg_key_, guard))) {
    LOG_WARN("fail to get partition", K(ret), K(param.pg_key_));
  } else if (OB_ISNULL(storage_file = guard.get_partition_group()->get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the storage file is null", K(ret), K(storage_file), K(pkey));
  } else if (is_major_merge && FALSE_IT(storage_file->reserve_ref_cnt_map_for_merge())) {
  } else if (OB_FAIL(alloc_merge_dag(merge_type, dag))) {
    LOG_WARN("Fail to alloc dag", K(ret), K(index_id));
  } else if (OB_FAIL(dag->init(param, report_))) {
    STORAGE_LOG(WARN, "Fail to init task", K(ret), K(pkey), K(frozen_version));
  } else if (OB_FAIL(dag->alloc_task(prepare_task))) {
    STORAGE_LOG(WARN, "Fail to alloc task, ", K(ret));
  } else if (OB_FAIL(prepare_task->init())) {
    STORAGE_LOG(WARN, "failed to init prepare_task", K(ret));
  } else if (OB_FAIL(dag->add_task(*prepare_task))) {
    STORAGE_LOG(WARN, "Fail to add task", K(ret), K(pkey), K(frozen_version));
  } else {
    if (OB_FAIL(ObDagScheduler::get_instance().add_dag(dag, is_major_merge ? false : info.emergency_))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task, ", K(ret));
      }
    } else {
      STORAGE_LOG(INFO,
          "schedule merge sstable dag finish",
          K(pkey),
          "pg_key",
          pg.get_partition_key(),
          K(index_id),
          K(merge_type),
          K(frozen_version),
          K(info),
          K(dag));
    }
  }

  if (OB_FAIL(ret) && NULL != dag) {
    ObDagScheduler::get_instance().free_dag(*dag);
  }
  return ret;
}

int ObPartitionScheduler::schedule_split_sstable_dag(const bool is_major_split, const common::ObVersion& split_version,
    const common::ObPartitionKey& src_pkey, const common::ObPartitionKey& dest_pkey,
    const common::ObPartitionKey& dest_pg_key, const uint64_t index_id)
{
  UNUSED(split_version);
  int ret = OB_SUCCESS;
  ObSSTableScheduleSplitParam param;

  UNUSED(split_version);
  param.is_major_split_ = is_major_split;
  param.merge_version_ = split_version;
  param.src_pkey_ = src_pkey;
  param.dest_pkey_ = dest_pkey;
  param.dest_pg_key_ = dest_pg_key;
  param.index_id_ = index_id;

  ObSSTableSplitDag* dag = NULL;
  ObSSTableSplitPrepareTask* prepare_task = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (!src_pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(ret), K(src_pkey));
  } else if (OB_FAIL(alloc_split_dag(dag))) {
    LOG_WARN("Fail to alloc dag", K(ret), K(src_pkey), K(dest_pkey), K(index_id));
  } else if (OB_ISNULL(partition_service_->get_mem_ctx_factory())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid mem ctx factory", K(ret));
  } else if (OB_FAIL(dag->init(param, partition_service_->get_mem_ctx_factory(), report_))) {
    STORAGE_LOG(WARN, "Fail to init task", K(ret), K(src_pkey), K(dest_pkey), K(index_id));
  } else if (OB_FAIL(dag->alloc_task(prepare_task))) {
    STORAGE_LOG(WARN, "Fail to alloc task, ", K(ret), K(src_pkey), K(dest_pkey), K(index_id));
  } else if (OB_FAIL(prepare_task->init())) {
    STORAGE_LOG(WARN, "Fail to init prepare task, ", K(ret), K(src_pkey), K(dest_pkey), K(index_id));
  } else if (OB_FAIL(dag->add_task(*prepare_task))) {
    STORAGE_LOG(WARN, "Fail to add task, ", K(ret), K(src_pkey), K(dest_pkey), K(index_id));
  } else {
    if (OB_FAIL(ObDagScheduler::get_instance().add_dag(dag))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task, ", K(ret));
      } else {
        LOG_INFO("need to split again", K(is_major_split), K(src_pkey), K(dest_pkey), K(index_id));
      }
    } else {
      STORAGE_LOG(INFO, "schedule split partition finish", K(src_pkey), K(dest_pkey));
    }
  }

  if (OB_FAIL(ret) && NULL != dag) {
    ObDagScheduler::get_instance().free_dag(*dag);
  }
  return ret;
}

int ObPartitionScheduler::get_all_dependent_tables(
    ObSchemaGetterGuard& schema_guard, ObPGPartitionArrayGuard& partitions, ObHashSet<uint64_t>& dependent_table_set)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (0 > partitions.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partitions.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      ObPGPartition* partition = partitions.at(i);
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is NULL", K(ret), K(i));
      } else {
        const uint64_t table_id = partition->get_partition_key().table_id_;
        const ObTableSchema* schema = NULL;
        const uint64_t fetch_tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
        if (OB_FAIL(schema_guard.get_table_schema(table_id, schema))) {
          if (OB_TENANT_NOT_EXIST == ret) {  // tenant is deleted, partitions wait to gc
            ret = OB_SUCCESS;
            if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
              FLOG_WARN("partition is wait to gc", K(fetch_tenant_id), K(table_id));
            }
          } else {
            LOG_WARN("get table schema failed", K(ret), K(table_id), K(*partition));
          }
        } else if (!schema_service_->is_tenant_full_schema(fetch_tenant_id)) {
          ret = OB_TENANT_SCHEMA_NOT_FULL;
          LOG_WARN("failed to check is full schema", K(ret), K(fetch_tenant_id));
        } else if (OB_ISNULL(schema)) {
          // do nothing
        } else {
          const ObTableSchema* mv_schema = NULL;
          for (int64_t j = 0; OB_SUCC(ret) && j < schema->get_mv_count(); ++j) {
            const uint64_t mv_tid = schema->get_mv_tid(j);
            if (OB_FAIL(schema_guard.get_table_schema(mv_tid, mv_schema))) {
              LOG_WARN("get table schema failed", K(ret), K(mv_tid));
            } else if (OB_ISNULL(mv_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("mv schema is NULL", K(ret), K(mv_tid));
            } else {
              FOREACH_CNT_X(dtid, mv_schema->get_depend_table_ids(), OB_SUCC(ret))
              {
                if (OB_FAIL(dependent_table_set.set_refactored(*dtid))) {
                  LOG_WARN("set dependent table set failed", K(ret), K(*dtid));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionScheduler::check_all_partitions(bool& check_finished, common::ObVersion& frozen_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionArrayGuard partitions;
  check_finished = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionScheduler has not been inited", K(ret));
  } else if (OB_FAIL(partition_service_->get_all_partitions(partitions))) {
    LOG_WARN("fail to get all partitions", K(ret));
  } else if (!schema_service_->is_sys_full_schema()) {
    ret = OB_EAGAIN;
  } else {
    {
      obsys::CRLockGuard frozen_version_guard(frozen_version_lock_);
      frozen_version = frozen_version_;
    }
    // skip the partition which check failed
    for (int64_t i = 0; !is_stop_ && i < partitions.count(); ++i) {
      bool check_finished_tmp = false;
      ObIPartitionGroup* partition = partitions.at(i);
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is NULL", K(ret), K(i));
      } else if (OB_FAIL(check_partition(frozen_version, partition, check_finished_tmp))) {
        LOG_WARN("fail to check partition", K(ret), "pkey", partition->get_partition_key());
        tmp_ret = OB_SUCCESS == tmp_ret ? ret : tmp_ret;
      } else if (!check_finished_tmp) {
        check_finished = false;
      }
    }
    ret = tmp_ret;
  }
  return ret;
}

int ObPartitionScheduler::check_restore_point(ObPGStorage& pg_storage)
{
  int ret = OB_SUCCESS;
  if (is_inner_table(pg_storage.get_partition_key().get_table_id())) {
    // do nothing
  } else {
    int64_t snapshot_gc_ts = 0;
    const ObPartitionKey pg_key = pg_storage.get_partition_key();
    ObSEArray<share::ObSnapshotInfo, 1> restore_points;
    ObSEArray<int64_t, 1> snapshot_versions;
    ObSEArray<int64_t, 1> schema_versions;
    if (OB_FAIL(ObFreezeInfoMgrWrapper::get_instance().get_reserve_points(
            pg_key.get_tenant_id(), ObSnapShotType::SNAPSHOT_FOR_RESTORE_POINT, restore_points, snapshot_gc_ts))) {
      LOG_WARN("failed to get restore points", K(ret), K(pg_key));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < restore_points.count(); i++) {
        if (OB_FAIL(snapshot_versions.push_back(restore_points.at(i).snapshot_ts_))) {
          LOG_WARN("failed to push back snapshot ts", K(ret), K(i));
        } else if (OB_FAIL(schema_versions.push_back(restore_points.at(i).schema_version_))) {
          LOG_WARN("failed to push back schema version", K(ret), K(i));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(pg_storage.update_restore_points(snapshot_versions, schema_versions, snapshot_gc_ts))) {
        LOG_WARN("failed to add restore points", K(ret), K(snapshot_versions), K(snapshot_gc_ts));
      }
    }
  }
  return ret;
}

int ObPartitionScheduler::check_backup_point(ObPGStorage& pg_storage)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_gc_ts = 0;
  const ObPartitionKey pg_key = pg_storage.get_partition_key();
  ObSEArray<share::ObSnapshotInfo, 1> backup_points;
  ObSEArray<int64_t, 1> snapshot_versions;
  ObSEArray<int64_t, 1> schema_versions;
  int64_t last_minor_freeze_ts = 0;
  int64_t publish_version = 0;
  if (OB_FAIL(ObFreezeInfoMgrWrapper::get_instance().get_reserve_points(
          pg_key.get_tenant_id(), ObSnapShotType::SNAPSHOT_FOR_BACKUP_POINT, backup_points, snapshot_gc_ts))) {
    LOG_WARN("failed to get restore points", K(ret), K(pg_key));
  } else if (OB_FAIL(pg_storage.get_active_memtable_base_version(publish_version))) {
    LOG_WARN("failed to get active memtable base version", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_points.count(); i++) {
      if (OB_FAIL(snapshot_versions.push_back(backup_points.at(i).snapshot_ts_))) {
        LOG_WARN("failed to push back snapshot ts", K(ret), K(i), K(pg_key));
      } else if (OB_FAIL(schema_versions.push_back(backup_points.at(i).schema_version_))) {
        LOG_WARN("failed to push back schema version", K(ret), K(i));
      } else {
        last_minor_freeze_ts = std::max(backup_points.at(i).snapshot_ts_, last_minor_freeze_ts);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (last_minor_freeze_ts > publish_version) {
      const bool emergency = false;
      const bool force = true;
      if (OB_FAIL(ObPartitionService::get_instance().minor_freeze(pg_key, emergency, force))) {
        LOG_WARN("failed to minor freeze", K(ret), K(pg_key));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pg_storage.update_backup_points(snapshot_versions, schema_versions, snapshot_gc_ts))) {
      LOG_WARN("failed to add restore points", K(ret), K(snapshot_versions), K(snapshot_gc_ts));
    }
  }

  return ret;
}

int ObPartitionScheduler::check_partition(
    const common::ObVersion& version, ObIPartitionGroup* partition, bool& check_finished)
{
  int ret = OB_SUCCESS;
  bool need_report = false;
  bool is_removed = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionScheduler has not been inited", K(ret));
  } else if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(partition));
  } else if (OB_FAIL(partition->try_clear_split_info())) {
    LOG_WARN("failed to clear split info", K(ret), "pkey", partition->get_partition_key());
  } else if (OB_FAIL(remove_split_dest_partition(partition, is_removed))) {
    LOG_WARN("failed to remove split dest partition", K(ret), "pkey", partition->get_partition_key());
  } else if (is_removed) {
    // do nothing
  } else {
    int tmp_ret = OB_SUCCESS;

    if (OB_SUCCESS != (tmp_ret = check_restore_point(partition->get_pg_storage()))) {
      LOG_WARN("failed to check restore point", K(tmp_ret), "pkey", partition->get_partition_key());
    }

    if (OB_SUCCESS != (tmp_ret = check_backup_point(partition->get_pg_storage()))) {
      LOG_WARN("failed to check backup point", K(tmp_ret), "pkey", partition->get_partition_key());
    }

    if (partition->get_pg_storage().is_restore()) {
      // skip restore partition
      // do nothing
    } else if (OB_FAIL(partition->get_pg_storage().try_update_report_status(
                   *report_, version, check_finished, need_report))) {
      LOG_WARN("fail to try update report status", K(ret), "pkey", partition->get_partition_key());
      if (OB_CHECKSUM_ERROR == ret || OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        if (OB_SUCCESS != (tmp_ret = report_->report_merge_error(partition->get_partition_key(), ret))) {
          STORAGE_LOG(ERROR, "Fail to submit checksum error task", K(tmp_ret));
        }
      }
    } else if (check_finished && need_report) {
      ObPartitionArray pkeys;
      if (OB_SUCCESS != (tmp_ret = partition->get_all_pg_partition_keys(pkeys))) {
        STORAGE_LOG(WARN, "get all pg partition keys error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = report_->submit_pt_update_task(partition->get_partition_key()))) {
        if (OB_PARTITION_NOT_EXIST == tmp_ret) {
          STORAGE_LOG(INFO, "partition has been dropped", "pkey", partition->get_partition_key());
        } else {
          STORAGE_LOG(ERROR, "fail to submit pt update task", K(tmp_ret));
        }
      }
      report_->submit_pg_pt_update_task(pkeys);
    }
    if (OB_SUCCESS != (tmp_ret = partition->get_pg_storage().try_drop_unneed_index())) {
      LOG_WARN("failed to try_drop_unneed_index", K(tmp_ret));
    }
  }
  return ret;
}

int ObPartitionScheduler::schedule_all_partitions(bool& merge_finished, common::ObVersion& frozen_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int inc_task_cnt = 0;
  int64_t finish_partition_cnt = 0;
  bool most_merge_finished = 0;
  ObIPartitionArrayGuard pgs;
  ObPGPartitionArrayGuard partitions(partition_service_->get_partition_map());
  ObSchemaGetterGuard schema_guard;
  const int64_t follower_replica_merge_level = GCONF._follower_replica_merge_level;
  merge_finished = false;
  is_major_scan_to_be_continued_ = false;

  const int64_t dependent_table_bucket_cnt = 1024;
  ObHashSet<uint64_t> dependent_table_set;
  bool mv_merged = true;
  bool can_schedule = true;

  DEBUG_SYNC(BEFORE_SCHEDULE_ALL_PARTITIONS);
  LOG_INFO("start schedule_all_partitions", K(frozen_version_));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (!schema_service_->is_sys_full_schema()) {
    // wait schema_service ready
    ret = OB_EAGAIN;
    LOG_WARN("schema is not ready", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret));
  } else if (OB_FAIL(partition_service_->get_all_partitions(pgs))) {
    LOG_WARN("Fail to get all partitions, ", K(ret));
  } else if (OB_FAIL(get_all_pg_partitions(pgs, partitions))) {
    LOG_WARN("failed to get all partitions", K(ret));
  } else if (OB_FAIL(dependent_table_set.create(dependent_table_bucket_cnt))) {
    LOG_WARN("create hashmap failed", K(ret));
  } else {
    // get frozen_version
    {
      obsys::CRLockGuard frozen_version_guard(frozen_version_lock_);
      frozen_version = frozen_version_;
    }

    merge_finished = true;
    if (frozen_version > 0) {
      ObFreezeInfoSnapshotMgr::FreezeInfoLite freeze_info;
      if (OB_FAIL(ObFreezeInfoMgrWrapper::get_instance().get_freeze_info_by_major_version(
              frozen_version.major_, freeze_info))) {
        LOG_WARN("failed to get_freeze_info_by_major_version", K(ret), K(frozen_version));
      } else if (OB_FAIL(get_all_dependent_tables(schema_guard, partitions, dependent_table_set))) {
        LOG_WARN("find all dependent schemas failed", K(ret));
      } else {
        // generate merge tasks
        for (int64_t round = 0; !is_stop_ && OB_SIZE_OVERFLOW != tmp_ret && round < 2; ++round) {
          // first time schedule all partitions except dependent tables,
          // when all mv are merged, then schedule the dependent tables
          for (int64_t i = 0; !is_stop_ && OB_SIZE_OVERFLOW != tmp_ret && i < partitions.count(); ++i) {
            // allow schedule fail, if failed, continue to schedule other partitions
            ObPGPartition* partition = partitions.at(i);
            ObIPartitionGroup* pg = nullptr;
            if (OB_ISNULL(partition)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("partition is NULL", K(ret), K(i));
            } else if (OB_ISNULL(pg = partition->get_pg())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("pg is null", K(ret), K(i), K(partition->get_partition_key()));
            } else {
              const ObPartitionKey& pkey = partition->get_partition_key();
              const uint64_t table_id = pkey.get_table_id();
              const bool is_dep_table = is_dependent_table(dependent_table_set, table_id);
              const bool first_time = (0 == round);
              const bool can_schedule = (first_time && !is_dep_table) || (!first_time && is_dep_table);
              const int64_t trans_table_end_log_ts = 0;
              const int64_t trans_table_timestamp = 0;
              const bool is_restore = pg->get_pg_storage().is_restore();
              if (can_schedule) {
                const ObTableSchema* schema = NULL;
                if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = schema_guard.get_table_schema(table_id, schema)))) {
                  LOG_WARN("failed to get table schema", K(tmp_ret), K(table_id));
                } else if (OB_ISNULL(schema)) {
                  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
                    LOG_INFO("The table does not exist, no need to merge partition, ", K(tmp_ret), K(pkey));
                  } else {
                    LOG_DEBUG("The table does not exist, no need to merge partition, ", K(tmp_ret), K(pkey));
                  }
                } else {
                  const bool is_mv_left = (0 < schema->get_mv_count());
                  bool is_merged = false;
                  // D replica should not major merge locally
                  if (!partition->can_schedule_merge() ||
                      (pg->is_replica_using_remote_memstore() &&
                          !is_follower_d_major_merge(follower_replica_merge_level)) ||
                      pg->is_share_major_in_zone()) {
                    // skip schedule
                    mv_merged = is_mv_left ? false : mv_merged;
                    merge_finished = false;
                  } else if (OB_SUCCESS != (tmp_ret = schedule_partition(MAJOR_MERGE,
                                                *partition,
                                                *pg,
                                                static_cast<ObVersion>(frozen_version),
                                                trans_table_end_log_ts,
                                                trans_table_timestamp,
                                                is_restore,
                                                is_merged))) {
                    mv_merged = is_mv_left ? false : mv_merged;
                    merge_finished = false;
                    if (OB_SIZE_OVERFLOW == tmp_ret) {
                      is_major_scan_to_be_continued_ = true;
                    }
                    if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
                      LOG_WARN("Fail to schedule partition major merge", K(pkey), K(frozen_version), K(tmp_ret));
                    }
                  } else {
                    if (!is_merged) {
                      mv_merged = is_mv_left ? false : mv_merged;
                      merge_finished = false;
                      ++inc_task_cnt;
                      if (OB_SUCCESS != (tmp_ret = check_and_freeze_for_major_(freeze_info.freeze_ts, *pg))) {
                        LOG_WARN("failed to check_and_freeze_for_major", K(tmp_ret), K(pkey));
                      }
                    } else if (is_restore) {
                      // do nothing
                    } else {
                      ++finish_partition_cnt;
                    }
                  }
                }
              }
            }
          }  // end for

          if (!mv_merged) {
            break;
          }
        }  // end for
      }

      if (OB_UNLIKELY(OB_SUCCESS != ret || OB_SUCCESS != tmp_ret)) {
        merge_finished = false;
      }

      is_major_scan_to_be_continued_ |= inc_task_cnt > 0;
      if (0 != partitions.count()) {
        most_merge_finished = ((finish_partition_cnt * 100 / partitions.count()) >=
                                  (ObServerConfig::get_instance().merger_completion_percentage))
                                  ? true
                                  : false;
      }

      const int64_t merge_task_count =
          ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE) +
          ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MAJOR_MERGE);
      LOG_INFO("Schedule finished, finish schedule_all_partitions",
          K(frozen_version),
          K(finish_partition_cnt),
          K(merge_task_count),
          K(inc_task_cnt),
          K_(is_major_scan_to_be_continued),
          K(most_merge_finished),
          K(merge_finished));

      if (merge_finished) {
        merged_version_ = frozen_version;
      }

      if (merge_finished || (first_most_merged_ && most_merge_finished)) {
        first_most_merged_ = false;
        if (report_version_ < frozen_version) {
          if (OB_FAIL(report_->report_merge_finished(frozen_version))) {
            LOG_WARN("Fail to report merge finished, ", K(frozen_version), K(ret));
          } else {
            report_version_ = frozen_version;
            if (merge_finished) {
              LOG_INFO("Success to report merge finished, ", K(frozen_version));
              if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = merge_statistic_.notify_merge_finish(frozen_version)))) {
                LOG_WARN("Fail to notify merge statistic finish, ", K(frozen_version), K(tmp_ret));
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObPartitionScheduler::check_all()
{
  int ret = OB_SUCCESS;
  common::ObVersion frozen_version;
  bool check_finished = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ObPartitionScheduler::get_instance().could_merge_start())) {
    ret = OB_CANCELED;
    LOG_INFO("Merge has been paused.");
  } else if (OB_FAIL(check_all_partitions(check_finished, frozen_version))) {
    LOG_WARN("fail to check all partitions", K(ret));
  }
  return ret;
}

int ObPartitionScheduler::merge_all()
{
  int ret = OB_SUCCESS;
  bool merge_finished = false;
  common::ObVersion frozen_version;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!ObPartitionScheduler::get_instance().could_merge_start())) {
    ret = OB_CANCELED;
    LOG_INFO("Merge has been paused.");
  } else if (OB_FAIL(schedule_all_partitions(merge_finished, frozen_version))) {
    LOG_WARN("Fail to schedule all partitions, ", K(ret));
  }
  return ret;
}

int32_t ObPartitionScheduler::get_max_merge_thread_cnt()
{
  int64_t cpu_cnt = common::ObServerConfig::get_instance().cpu_count;
  if (0 == cpu_cnt) {
    cpu_cnt = common::get_cpu_num();
  }
  return (int32_t)min(cpu_cnt, MAX_MERGE_WORK_THREAD_NUM);
}

int32_t ObPartitionScheduler::get_default_merge_thread_cnt()
{
  return (int32_t)min(
      DEFAULT_MERGE_WORK_THREAD_NUM, std::max(1, get_max_merge_thread_cnt() * MERGE_THREAD_CNT_PERCENT / 100));
}

int ObPartitionScheduler::check_index_compact_to_latest(
    const common::ObPartitionKey& pkey, const uint64_t index_id, bool& is_finished)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  ObIPartitionGroup* partition = NULL;
  ObPartitionStorage* storage = NULL;
  ObTableHandle index_table_handle;
  ObSSTable* sstable = NULL;
  bool table_store_exist = false;
  is_finished = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || OB_INVALID_ID == index_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pkey), K(index_id));
  } else if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
  } else if (OB_FAIL(partition->get_pg_partition(pkey, pg_partition_guard)) ||
             OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get pg partition error", K(ret), K(pkey), K(index_id));
  } else if (OB_ISNULL(
                 storage = static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, storage must not be NULL", K(ret));
  } else if (OB_FAIL(storage->get_partition_store().check_table_store_exist(index_id, table_store_exist))) {
    STORAGE_LOG(WARN, "fail to check table exist", K(ret));
  } else if (!table_store_exist) {
    is_finished = true;
    ret = OB_TABLE_IS_DELETED;
  } else if (OB_FAIL(storage->get_partition_store().get_last_major_sstable(index_id, index_table_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get last major sstable", K(ret), K(index_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(index_table_handle.get_sstable(sstable))) {
    LOG_WARN("Failed to get latest major sstable", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, index table must not be NULL", K(ret), K(index_id));
  } else {
    bool is_all_checked = false;
    const int64_t merged_version = sstable->get_version().major_;
    common::ObSEArray<share::schema::ObIndexTableStat, OB_MAX_INDEX_PER_TABLE> index_stats;
    is_finished = merged_version >= frozen_version_;
    if (is_finished) {
      if (OB_FAIL(index_stats.push_back(
              ObIndexTableStat(index_id, INDEX_STATUS_UNAVAILABLE, true /*check index is not dropped*/)))) {
        LOG_WARN("fail to push back index stats", K(ret));
      } else if (OB_FAIL(storage->validate_sstables(index_stats, is_all_checked))) {
        LOG_WARN("fail to validate sstable", K(ret));
      } else {
        is_finished = is_all_checked;
      }
    }
    STORAGE_LOG(INFO, "check_index_compact", K(index_id), K(merged_version), K(frozen_version_));
  }
  return ret;
}

int ObPartitionScheduler::check_release_memtable()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionArrayGuard partitions;
  const int64_t start_ts = ObTimeUtility::current_time();

  lib::ObMutexGuard guard(check_release_lock_);
  LOG_INFO("start check_release_memtable", K(frozen_version_));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_FAIL(partition_service_->get_all_partitions(partitions))) {
    LOG_WARN("Fail to get all partitions, ", K(ret));
  } else if (!schema_service_->is_sys_full_schema()) {
    // wait schema_service ready
    ret = OB_EAGAIN;
    LOG_WARN("schema is not ready", K(ret));
  }

  bool has_error = false;
  current_tenant_snapshot_map_.clear();
  for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < partitions.count(); ++i) {
    ObIPartitionGroup* partition = partitions.at(i);
    int64_t snapshot_version = INT64_MAX;
    if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is NULL", K(ret), K(i));
    } else if (OB_SUCCESS != (tmp_ret = partition->get_pg_storage().check_release_memtable(*report_))) {
      if (OB_EAGAIN != tmp_ret && OB_TABLE_IS_DELETED != tmp_ret) {
        LOG_WARN("failed to check release memtable", K(tmp_ret), "pkey", partition->get_partition_key());
      }
    }

    if (!has_error) {
      if (OB_SUCCESS !=
          (tmp_ret = partition->get_pg_storage().get_min_frozen_memtable_base_version(snapshot_version))) {
        LOG_WARN("failed to get_min_frozen_memtable_base_version", K(tmp_ret), K(partition->get_partition_key()));
        has_error = true;
      } else {
        uint64_t tenant_id = extract_tenant_id(partition->get_partition_key().get_table_id());
        int64_t t_snapshot_version = INT64_MAX;
        if (OB_UNLIKELY(
                OB_SUCCESS != (tmp_ret = current_tenant_snapshot_map_.get_refactored(tenant_id, t_snapshot_version)))) {
          if (OB_HASH_NOT_EXIST == tmp_ret) {
            t_snapshot_version = INT64_MAX;
            tmp_ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get tenant_snapshot_map", K(tmp_ret), K(tenant_id));
            has_error = true;
          }
        }
        if (OB_SUCCESS == tmp_ret) {
          t_snapshot_version = std::min(t_snapshot_version, snapshot_version);
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = current_tenant_snapshot_map_.set_refactored(
                                             tenant_id, t_snapshot_version, true)))) {
            LOG_WARN("failed to set tenant_snapshot_map", K(tmp_ret), K(tenant_id));
            has_error = true;
          }
        }
      }
    }
  }

  if (!has_error) {
    common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode>::iterator iter;
    for (iter = current_tenant_snapshot_map_.begin(); iter != current_tenant_snapshot_map_.end(); ++iter) {
      const uint64_t tenant_id = iter->first;
      const int64_t min_base_version = iter->second;
      int64_t t_snapshot_version = -1;
      if (OB_SUCCESS != (tmp_ret = tenant_snapshot_map_.get_refactored(tenant_id, t_snapshot_version))) {
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          tmp_ret = OB_SUCCESS;
          t_snapshot_version = -1;
        } else {
          LOG_WARN("failed to get from tenant_snapshot_map", K(tmp_ret), K(tenant_id));
        }
      }
      if (OB_SUCCESS == tmp_ret) {
        if (min_base_version > t_snapshot_version || INT64_MAX == t_snapshot_version) {
          if (OB_SUCCESS != (tmp_ret = tenant_snapshot_map_.set_refactored(tenant_id, min_base_version, true))) {
            LOG_WARN("failed to set tenant_snapshot_map", K(tmp_ret), K(tenant_id), K(min_base_version));
          }
          if (OB_SUCCESS != (tmp_ret = notify_minor_merge_finish(tenant_id, min_base_version))) {
            LOG_WARN("failed to notify_minor_merge_finish", K(tmp_ret), K(tenant_id), K(min_base_version));
          }
          if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
            FLOG_INFO("try notify minor merge finish", K(tenant_id), K(min_base_version));
          }
        }
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("check_release_memtable", K(ret), "partition_count", partitions.count(), K(cost_ts));
  return ret;
}

int ObPartitionScheduler::get_all_pg_partitions(
    ObIPartitionArrayGuard& pg_arr_guard, ObPGPartitionArrayGuard& part_arr_guard)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < pg_arr_guard.count(); ++i) {
    ObIPartitionGroup* pg = nullptr;
    if (OB_ISNULL(pg = pg_arr_guard.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pg is null", K(ret));
    } else if (OB_FAIL(pg->get_pg_storage().get_all_pg_partitions(part_arr_guard))) {
      LOG_WARN("failed to get all pg partitions", K(ret), K(pg->get_partition_key()));
    }
  }
  return ret;
}

int ObPartitionScheduler::notify_minor_merge_start(const uint64_t tenant_id, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObMinorMergeHistory* history = nullptr;
  {
    obsys::CRLockGuard lock_guard(frozen_version_lock_);
    if (OB_FAIL(minor_merge_his_map_.get_refactored(tenant_id, history))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get minor merge history", K(ret), K(tenant_id), K(snapshot_version));
      }
      history = nullptr;
    }
  }
  if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) {
    obsys::CWLockGuard lock_guard(frozen_version_lock_);
    if (OB_FAIL(minor_merge_his_map_.get_refactored(tenant_id, history))) {
      if (OB_HASH_NOT_EXIST == ret) {
        void* buf = nullptr;
        if (OB_ISNULL(buf = ob_malloc(sizeof(ObMinorMergeHistory), ObModIds::OB_PARTITION_SCHEDULER))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate minor merge history", K(ret), K(tenant_id));
        } else {
          history = new (buf) ObMinorMergeHistory(tenant_id);
          if (OB_FAIL(minor_merge_his_map_.set_refactored(tenant_id, history))) {
            ob_free(history);
            history = nullptr;
            LOG_WARN("failed to set minor_merge_his_map_", K(ret), K(tenant_id));
          }
        }
      } else {
        LOG_WARN("failed to get minor merge his map", K(ret), K(tenant_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(history)) {
      if (OB_FAIL(history->notify_minor_merge_start(snapshot_version))) {
        LOG_WARN("failed to notify minor merge start", K(ret), K(tenant_id), K(snapshot_version));
      }
    }
  }
  return ret;
}

int ObPartitionScheduler::notify_minor_merge_finish(const uint64_t tenant_id, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObMinorMergeHistory* history = nullptr;
  {
    obsys::CRLockGuard lock_guard(frozen_version_lock_);
    if (OB_FAIL(minor_merge_his_map_.get_refactored(tenant_id, history))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get minor merge history", K(ret), K(tenant_id), K(snapshot_version));
      } else {
        ret = OB_SUCCESS;
      }
      history = nullptr;
    }
  }
  if (OB_NOT_NULL(history)) {
    if (OB_FAIL(history->notify_minor_merge_finish(snapshot_version))) {
      LOG_WARN("failed to notify_minor_merge_finish", K(ret), K(tenant_id), K(snapshot_version));
    }
  }
  return ret;
}

void ObPartitionScheduler::stop()
{
  is_stop_ = true;
  TG_STOP(lib::TGDefIDs::MinorScan);
  TG_STOP(lib::TGDefIDs::MajorScan);
  stop_merge();
}

void ObPartitionScheduler::wait()
{
  TG_WAIT(lib::TGDefIDs::MinorScan);
  TG_WAIT(lib::TGDefIDs::MajorScan);
}

int ObPartitionScheduler::remove_split_dest_partition(ObIPartitionGroup* partition, bool& is_removed)
{
  int ret = OB_SUCCESS;
  bool can_migrate = true;
  is_removed = false;
  if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    common::ObReplicaMember dst(observer::ObServer::get_instance().get_self(), ObTimeUtility::current_time());
    ObIPartitionGroupGuard guard;
    if (partition->is_dest_logical_split_finish() &&
        ObReplicaTypeCheck::is_readonly_replica(partition->get_replica_type())) {
      const ObPartitionSplitInfo& split_info = partition->get_split_info();
      const ObPartitionKey src_pkey = split_info.get_src_partition();
      if (OB_FAIL(ObPartitionService::get_instance().get_partition(src_pkey, guard))) {
        if (OB_PARTITION_NOT_EXIST != ret) {
          LOG_WARN("failed to get partition", K(ret));
        }
      }

      if (OB_PARTITION_NOT_EXIST == ret) {
        dst.set_replica_type(partition->get_replica_type());
        if (OB_FAIL(partition->check_can_migrate(can_migrate))) {
          LOG_WARN("failed to check can migrate", K(ret));
        } else if (!can_migrate) {
          if (OB_FAIL(ObPartitionService::get_instance().remove_replica(partition->get_partition_key(), dst))) {
            LOG_WARN("failed to remove replica", K(ret), "pkey", partition->get_partition_key());
          } else {
            is_removed = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionScheduler::reload_minor_merge_schedule_interval()
{
  int ret = OB_SUCCESS;
  const int64_t minor_merge_schedule_interval = GCONF._ob_minor_merge_schedule_interval;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (minor_merge_schedule_interval_ != minor_merge_schedule_interval) {
    bool is_exist = false;
    if (OB_FAIL(TG_TASK_EXIST(lib::TGDefIDs::MinorScan, minor_scan_task_, is_exist))) {
      LOG_ERROR("failed to check minor merge schedule task exist", K(ret));
    } else if (is_exist) {
      TG_CANCEL(lib::TGDefIDs::MinorScan, minor_scan_task_);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::MinorScan, minor_scan_task_, minor_merge_schedule_interval, true))) {
      LOG_ERROR("failed to schedule minor merge schedule task", K(ret));
    } else {
      minor_merge_schedule_interval_ = minor_merge_schedule_interval;
      FLOG_INFO("success to reload new minor merge schedule interval", K(minor_merge_schedule_interval));
    }
  }
  return ret;
}

int ObPartitionScheduler::can_schedule_partition(const ObMergeType merge_type, bool& can_schedule)
{
  int ret = OB_SUCCESS;
  const float PAUSE_MERGE_DISK_RADIO = 0.95;
  const int64_t PRINT_INTERVAL = 5 * 1000 * 1000;  // 5s
  bool is_backup_started = false;
  can_schedule = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (ObMergeType::MAJOR_MERGE != merge_type) {
    can_schedule = true;
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().is_base_backup_start(is_backup_started))) {
    LOG_WARN("failed to check is base backup started", K(ret));
  } else {
    // TODO() fix ofs
    //    const float use_disk_space_radio = OB_FILE_SYSTEM.get_used_macro_block_count() /
    //        OB_FILE_SYSTEM.get_total_macro_block_count();
    //    can_schedule = !(use_disk_space_radio >= PAUSE_MERGE_DISK_RADIO && is_backup_started);
    //    if (!can_schedule) {
    //      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
    //        FLOG_INFO("can not schedule merge", K(is_backup_started), K(use_disk_space_radio));
    //      }
    //    }
  }
  return ret;
}

ObFastFreezeChecker::ObFastFreezeChecker(const int64_t tenant_id)
{
  reset();
  if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID)) {
    LOG_WARN("Invalid tenant id to init fast freeze checker", K(tenant_id));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    // fast freeze only trigger for user tenant
    tenant_id_ = tenant_id;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (tenant_config.is_valid()) {
      enable_fast_freeze_ = tenant_config->_ob_enable_fast_freeze;
    }
  }
}

ObFastFreezeChecker::~ObFastFreezeChecker()
{
  reset();
}

void ObFastFreezeChecker::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  enable_fast_freeze_ = false;
}

int ObFastFreezeChecker::check_hotspot_need_fast_freeze(ObIPartitionGroup& pg, bool& need_fast_freeze)
{
  int ret = OB_SUCCESS;
  need_fast_freeze = false;

  if (OB_UNLIKELY(!need_check())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected fast freeze checker status", K(ret), K(*this));
  } else if (OB_FAIL(
                 pg.get_pg_storage().check_active_mt_hotspot_row_exist(need_fast_freeze, FAST_FREEZE_INTERVAL_US))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("check active memstore hotspot row exist error", K(ret), "pkey", pg.get_partition_key());
    }
  } else {
    if (need_fast_freeze) {
      LOG_INFO("current partition need fast freeze because of hotspot row", "pkey", pg.get_partition_key());
    }
  }
  return ret;
}

ObClearTransTableTask::ObClearTransTableTask()
{}

ObClearTransTableTask::~ObClearTransTableTask()
{}

void ObClearTransTableTask::runTimerTask()
{
  ObPartitionScheduler::get_instance().clear_unused_trans_status();
}

int ObPartitionScheduler::schedule_trans_table_merge_dag(
    const ObPartitionKey& pg_key, const int64_t end_log_ts, const int64_t trans_table_seq)
{
  int ret = OB_SUCCESS;
  ObTransTableMergeDag* dag = NULL;
  ObTransTableMergeTask* trans_merge_task = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(dag))) {
    LOG_WARN("Fail to alloc dag", K(ret));
  } else if (OB_FAIL(dag->init(pg_key))) {
    STORAGE_LOG(WARN, "Fail to init task", K(ret));
  } else if (OB_FAIL(dag->alloc_task(trans_merge_task))) {
    STORAGE_LOG(WARN, "Fail to alloc task, ", K(ret));
  } else if (OB_FAIL(trans_merge_task->init(pg_key, end_log_ts, trans_table_seq))) {
    STORAGE_LOG(WARN, "failed to init trans_merge_task", K(ret));
  } else if (OB_FAIL(dag->add_task(*trans_merge_task))) {
    STORAGE_LOG(WARN, "Fail to add task", K(ret));
  } else {
    if (OB_FAIL(ObDagScheduler::get_instance().add_dag(dag))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task, ", K(ret));
      }
    } else {
      STORAGE_LOG(INFO, "schedule merge trans table dag finish", K(pg_key), K(dag));
    }
  }

  if (OB_FAIL(ret) && NULL != dag) {
    ObDagScheduler::get_instance().free_dag(*dag);
  }
  return ret;
}

int ObPartitionScheduler::check_and_freeze_for_major_(const int64_t freeze_ts, ObIPartitionGroup& pg)
{
  int ret = OB_SUCCESS;
  int64_t base_version = 0;
  const bool emergency = false;
  const bool force = true;  // force to freeze memtable before major merge
  const ObPartitionKey& pg_key = pg.get_partition_key();
  int64_t slave_read_ts = 0;
  if (OB_FAIL(pg.get_pg_storage().get_active_memtable_base_version(base_version))) {
    LOG_WARN("failed to get_active_memtable_base_version", K(ret), K(freeze_ts), K(pg_key));
  } else if (base_version < freeze_ts) {
    if (OB_FAIL(pg.get_pg_storage().get_weak_read_timestamp(slave_read_ts))) {
      LOG_WARN("failed to get weak_read_timestamp", K(ret), K(pg_key));
    } else if (slave_read_ts < freeze_ts) {
      // slave_read_ts is smaller than freeze_ts, try later
    } else if (OB_FAIL(ObPartitionService::get_instance().minor_freeze(pg_key, emergency, force))) {
      LOG_WARN("failed to minor_freeze", K(ret), K(pg_key));
    } else {
      LOG_INFO("check_and_freeze_for_major success", K(ret), K(pg_key), K(freeze_ts));
    }
  }
  return ret;
}

int ObPartitionScheduler::clear_unused_trans_status()
{
  int ret = OB_SUCCESS;
  ObIPartitionArrayGuard partitions;
  const int64_t start_ts = ObTimeUtility::current_time();

  LOG_INFO("start clear trans table", K(frozen_version_));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_FAIL(partition_service_->get_all_partitions(partitions))) {
    LOG_WARN("Fail to get all partitions, ", K(ret));
  } else {
    for (int64_t i = 0; !is_stop_ && i < partitions.count(); ++i) {
      ObIPartitionGroup* partition = partitions.at(i);
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is NULL", K(ret), K(i));
      } else if (OB_FAIL(partition->get_pg_storage().clear_unused_trans_status())) {
        LOG_WARN("failed to clear unused trans status", K(ret));
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("finish clear trans table", K(ret), "partition_count", partitions.count(), K(cost_ts));
  return ret;
}

int ObPartitionScheduler::update_min_sstable_schema_info()
{
  int ret = OB_SUCCESS;
  ObIPartitionArrayGuard partitions;
  int64_t min_schema_version = INT64_MAX;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionScheduler has not been inited, ", K(ret));
  } else if (OB_FAIL(partition_service_->get_all_partitions(partitions))) {
    LOG_WARN("Fail to get all partitions, ", K(ret));
  } else {
    TenantMinSSTableSchemaVersionMap tmp_map;
    if (OB_FAIL(tmp_map.create(TENANT_BUCKET_NUM, "tmp_map"))) {
      LOG_WARN("failed to create tmp map", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < partitions.count(); ++i) {
        ObIPartitionGroup* partition = partitions.at(i);
        ObReplicaType replica_type;
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is NULL", K(ret), K(i));
        } else if (OB_FAIL(partition->get_pg_storage().get_replica_type(replica_type))) {
          LOG_WARN("failed to get replica type", K(ret), K(partition->get_partition_key()));
        } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(replica_type)) {
          continue;
        } else if (OB_FAIL(partition->get_pg_storage().get_min_schema_version(min_schema_version))) {
          LOG_WARN("failed to get max cleanout log id", K(ret));
        } else if (OB_FAIL(update_min_schema_version(
                       tmp_map, partition->get_pg_storage().get_partition_key().table_id_, min_schema_version))) {
          LOG_WARN("failed to add min schema version",
              K(ret),
              "table_id",
              partition->get_pg_storage().get_partition_key().table_id_,
              K(min_schema_version));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(update_to_map(tmp_map))) {
        LOG_WARN("failed to update infomation to map", K(ret));
      }
      if (OB_FAIL(tmp_map.destroy())) {
        LOG_WARN("failed to destory map", K(ret));
      }
    }
  }
  LOG_INFO("finish update min schema version",
      K(ret),
      "partition_cnt",
      partitions.count(),
      "cost_ts",
      ObTimeUtility::fast_current_time() - start_ts);
  return ret;
}

int ObPartitionScheduler::update_to_map(TenantMinSSTableSchemaVersionMap& tmp_map)
{
  int ret = OB_SUCCESS;
  int64_t tmp_schema_version = 0;
  const int erase_flag = 1; /*erase old one*/
  lib::ObMutexGuard guard(min_sstable_schema_version_lock_);
  for (TenantMinSSTableSchemaVersionMapIterator iter = tmp_map.begin(); OB_SUCC(ret) && iter != tmp_map.end(); ++iter) {
    if (OB_FAIL(min_sstable_schema_version_map_.get_refactored(iter->first, tmp_schema_version))) {
      if (OB_HASH_NOT_EXIST == ret) {  // add pair
        ret = OB_SUCCESS;
        if (OB_FAIL(min_sstable_schema_version_map_.set_refactored(iter->first, iter->second))) {
          LOG_WARN("failed to add min schema version", K(ret), "tenant_id", iter->first, K(iter->second));
        }
      } else {
        LOG_WARN("failed to get min schema version", K(ret), "tenant_id", iter->first, K(iter->second));
      }
    } else if (tmp_schema_version > iter->second) {  // update pair
      if (REACH_TIME_INTERVAL(1000 * 1000 * 10)) {
        LOG_INFO("min schema version is getting smaller",
            "tenant_id",
            iter->first,
            "old min_schema_version",
            tmp_schema_version,
            "new min_schema_version",
            iter->second);
      }
    } else if (OB_FAIL(min_sstable_schema_version_map_.set_refactored(iter->first, iter->second, erase_flag))) {
      LOG_WARN(
          "failed to add min schema version", K(ret), "tenant_id", iter->first, "min_schema_version", iter->second);
    } else {
      LOG_INFO("succeed to update tenant min schema version",
          "tenant_id",
          iter->first,
          "old tenant_schema_version",
          tmp_schema_version,
          "new tenant_schema_version",
          iter->second);
    }
  }
  return ret;
}

int ObPartitionScheduler::update_min_schema_version(
    TenantMinSSTableSchemaVersionMap& tmp_map, const int64_t table_id, const int64_t min_schema_version)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(table_id);
  int64_t tmp_schema_version = 0;
  if (is_inner_table(table_id)) {
    tenant_id = OB_SYS_TENANT_ID;  // sys_tenant's table OR inner_tables
  }
  if (OB_FAIL(tmp_map.get_refactored(tenant_id, tmp_schema_version))) {
    if (OB_HASH_NOT_EXIST == ret) {  // add pair
      ret = OB_SUCCESS;
      if (OB_FAIL(tmp_map.set_refactored(tenant_id, min_schema_version))) {
        LOG_WARN("failed to add min schema version", K(ret), K(tenant_id), K(min_schema_version));
      }
    }
  } else if (tmp_schema_version > min_schema_version) {  // update pair
    const int flag = 1;                                  /*erase old one*/
    if (OB_FAIL(tmp_map.set_refactored(tenant_id, min_schema_version, flag))) {
      LOG_WARN("failed to update min schema version", K(ret), K(tenant_id), K(min_schema_version));
    }
  }
  return ret;
}

int64_t ObPartitionScheduler::get_min_schema_version(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t min_schema_version = OB_INVALID_VERSION;
  lib::ObMutexGuard guard(min_sstable_schema_version_lock_);
  if (OB_FAIL(min_sstable_schema_version_map_.get_refactored(tenant_id, min_schema_version))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get min schema version failed", K(ret), K(tenant_id));
    }
  }
  return min_schema_version;
}

} /* namespace storage */
} /* namespace oceanbase */
