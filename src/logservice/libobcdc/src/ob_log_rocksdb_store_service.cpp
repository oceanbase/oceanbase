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
 *
 * OBCDC Storage based on rocksdb
 */

#define USING_LOG_PREFIX OBLOG_STORAGER

#include "ob_log_rocksdb_store_service.h"
#include "ob_log_utils.h"
#include "ob_log_config.h"
#include "lib/utility/ob_print_utils.h"     // databuff_printf
#include "lib/file/file_directory_utils.h"  // FileDirectoryUtils
#include "lib/oblog/ob_log.h"               // ObLogger
#include "lib/oblog/ob_log_module.h"        // LOG_*
#include "lib/ob_errno.h"

#include <algorithm>

#include "lib/utility/utility.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/utilities/memory_util.h"

#define RETRY_FUNC_ON_IO_ERROR_WITH_USLEEP_MS(stop_flag, sleep_ms, var, func, args...) \
  do {\
    if (OB_SUCC(ret)) \
    { \
      int64_t _retry_func_on_error_last_print_time = common::ObClockGenerator::getClock();\
      int64_t _retry_func_on_error_cur_print_time = 0;\
      const int64_t _PRINT_RETRY_FUNC_INTERVAL = 10 * _SEC_;\
      s = rocksdb::Status::IOError();\
      while (s.IsIOError() && ! (stop_flag)) \
      { \
        s = (var).func(args); \
        if (s.IsIOError()) { \
          ob_usleep(sleep_ms); \
        }\
        _retry_func_on_error_cur_print_time = common::ObClockGenerator::getClock();\
        if (_retry_func_on_error_cur_print_time - _retry_func_on_error_last_print_time >= _PRINT_RETRY_FUNC_INTERVAL) {\
          LOG_DBA_WARN(OB_IO_ERROR, \
              "msg", "put value into rocksdb failed", \
              "error", s.ToString().c_str(), \
              "last_print_time", _retry_func_on_error_last_print_time); \
          _retry_func_on_error_last_print_time = _retry_func_on_error_cur_print_time; \
        }\
      } \
      if ((stop_flag)) \
      { \
        ret = OB_IN_STOP_STATE; \
      } \
    } \
  } while (0)

namespace oceanbase
{
namespace libobcdc
{
RocksDbStoreService::RocksDbStoreService()
{
  is_inited_ = false;
  is_stopped_ = true;
  is_enable_compress_ = false;
  m_db_ = NULL;
  m_db_path_.clear();
}

RocksDbStoreService::~RocksDbStoreService()
{
  destroy();
}

int RocksDbStoreService::init(const std::string &path)
{
  int ret = OB_SUCCESS;
  UNUSED(path);

  if (OB_UNLIKELY(is_inited_)) {
    LOG_ERROR("RocksDbStoreService has inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(init_dir_(path.c_str()))) {
    LOG_ERROR("init_dir_ fail", K(ret));
  } else if (OB_FAIL(init_database_(path))) {
    LOG_ERROR("init_database_ fail", K(ret));
  } else {
    is_stopped_ = false;
    is_inited_ = true;
  }

  return ret;
}

int RocksDbStoreService::init_database_(const std::string &path)
{
  int ret = OB_SUCCESS;

  const int64_t sliding_window_size = 10000;
  const int64_t deletion_trigger = 1000;
  const double deletion_ratio = 0.1;

  const int cpu_count = static_cast<int>(common::get_cpu_num());
  const int flush_threads = std::max(2, std::min(cpu_count / 4, 8));
  const int compaction_threads = std::max(4, std::min(cpu_count / 2, 24));

  is_enable_compress_ = (TCONF.rocksdb_enable_compress == 1);
  m_db_path_ = path;
  m_options_.create_if_missing = true;
  m_options_.unordered_write = true;
  m_options_.allow_concurrent_memtable_write = true;
  m_options_.advise_random_on_open = false;
  // Keep buffered reads so that recently-flushed data benefits from OS page
  // cache and prepopulate_block_cache; Direct IO only for background ops.
  m_options_.use_direct_reads = false;
  m_options_.use_direct_io_for_flush_and_compaction = true;
  m_options_.bytes_per_sync = 0;

  m_options_.max_background_jobs = flush_threads + compaction_threads;
  m_options_.max_open_files = -1;
  m_options_.max_subcompactions = static_cast<uint32_t>(std::max(4, std::min(cpu_count / 4, 16)));
  m_options_.env->SetBackgroundThreads(flush_threads, rocksdb::Env::HIGH);
  m_options_.env->SetBackgroundThreads(compaction_threads, rocksdb::Env::LOW);

  // Shared block cache across all column families for unified memory control.
  // Created once here and reused in every create_column_family call.
  // std::shared_ptr is unavoidable: RocksDB API (BlockBasedTableOptions::block_cache,
  // Options::write_buffer_manager) requires shared ownership.
  size_t block_cache_size = TCONF.rocksdb_block_cache_size.get();
  shared_block_cache_ = rocksdb::NewLRUCache(block_cache_size, 16);

  // WriteBufferManager caps total memtable memory across all CFs.
  // Only charge memtable to block cache when the cache is large enough to
  // absorb it; otherwise the dummy entries would evict all real cache data.
  const int64_t rocksdb_write_buffer_size = TCONF.rocksdb_write_buffer_size;
  size_t total_write_buffer_limit =
      static_cast<size_t>(rocksdb_write_buffer_size) * _M_ * 9;
  bool charge_to_cache = (block_cache_size > total_write_buffer_limit);
  m_options_.write_buffer_manager.reset(
      new rocksdb::WriteBufferManager(
          total_write_buffer_limit,
          charge_to_cache ? shared_block_cache_ : nullptr));

  m_options_.table_properties_collector_factories.emplace_back(
    rocksdb::NewCompactOnDeletionCollectorFactory(sliding_window_size, deletion_trigger, deletion_ratio));

  m_options_.statistics = rocksdb::CreateDBStatistics();
  m_options_.statistics->set_stats_level(rocksdb::StatsLevel::kExceptDetailedTimers);
  m_options_.stats_dump_period_sec = 10;

  rocksdb::Status s = rocksdb::DB::Open(m_options_, m_db_path_, &m_db_);
  if (!s.ok()) {
    _LOG_ERROR("first open rocks db failed, path is %s, status is %s",
        m_db_path_.c_str(), s.ToString().c_str());
    ret = OB_ERR_UNEXPECTED;
  } else {
    _LOG_INFO("RocksDbStoreService database init success, path:%s, num_cpus=%d, "
        "flush_threads=%d, compaction_threads=%d, max_subcompactions=%u, "
        "block_cache_size=%zu, write_buffer_limit=%zu, charge_memtable_to_cache=%d",
        m_db_path_.c_str(), cpu_count,
        flush_threads, compaction_threads, m_options_.max_subcompactions,
        block_cache_size, total_write_buffer_limit, charge_to_cache);
  }

  return ret;
}

int RocksDbStoreService::close()
{
  int ret = OB_SUCCESS;

  if (NULL != m_db_) {
    LOG_INFO("closing rocksdb ...");
    mark_stop_flag();
    ob_usleep(5 * _SEC_);

    rocksdb::Status status = m_db_->Close();

    if (! status.ok()) {
      ret = OB_ERR_UNEXPECTED;
      _LOG_ERROR("rocksdb close failed, error %s", status.ToString().c_str());
    } else {
      LOG_INFO("rocksdb close succ");
    }
  }

  return ret;
}

int RocksDbStoreService::init_dir_(const char *dir_path)
{
  int ret = OB_SUCCESS;
  const static int64_t CMD_BUF_SIZE = 1024;
  static char cmd_buf[CMD_BUF_SIZE];
  int64_t cmd_pos = 0;

  if (OB_FAIL(common::databuff_printf(cmd_buf, CMD_BUF_SIZE, cmd_pos, "rm -rf %s", dir_path))) {
    LOG_ERROR("databuff_printf fail", K(ret), K(cmd_buf), K(cmd_pos), K(dir_path));
  } else {
    (void)system(cmd_buf);
    LOG_INFO("system succ", K(cmd_buf), K(cmd_pos), K(dir_path));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(common::FileDirectoryUtils::create_full_path(dir_path))) {
      LOG_ERROR("FileDirectoryUtils create_full_path fail", K(ret), K(dir_path));
    } else {
      // succ
    }
  }

  return ret;
}

void RocksDbStoreService::destroy()
{
  if (is_inited_) {
    LOG_INFO("rocksdb service destroy begin");
    close();

    if (OB_NOT_NULL(m_db_)) {
      delete m_db_;
      m_db_ = NULL;
    }
    m_options_.write_buffer_manager.reset();
    shared_block_cache_.reset();
    is_inited_ = false;
    LOG_INFO("rocksdb service destroy end");
  }
}

int RocksDbStoreService::put(const std::string &key, const ObSlice &value)
{
  int ret = OB_SUCCESS;
  rocksdb::WriteOptions writer_options;
  writer_options.disableWAL = true;

  if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else {
    // find column family handle for cf
    rocksdb::Status s;
    RETRY_FUNC_ON_IO_ERROR_WITH_USLEEP_MS(is_stopped(), 1 * _SEC_, (*m_db_), Put, writer_options,
        rocksdb::Slice(key.c_str(), key.size()),
        rocksdb::Slice(value.buf_, value.buf_len_));

    if (!s.ok()) {
      _LOG_ERROR("RocksDbStoreService put value into rocksdb failed, error %s", s.ToString().c_str());
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

int RocksDbStoreService::put(void *cf_handle, const std::string &key, const ObSlice &value)
{
  int ret = OB_SUCCESS;
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);
  rocksdb::WriteOptions writer_options;
  writer_options.disableWAL = true;

  if (OB_ISNULL(column_family_handle)) {
    LOG_ERROR("column_family_handle is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else {
    rocksdb::Status s;
    RETRY_FUNC_ON_IO_ERROR_WITH_USLEEP_MS(is_stopped(), 1 * _SEC_, (*m_db_), Put, writer_options, column_family_handle,
        rocksdb::Slice(key),
        rocksdb::Slice(value.buf_, value.buf_len_));

    if (!s.ok()) {
      _LOG_ERROR("RocksDbStoreService put value into rocksdb failed, error %s", s.ToString().c_str());
      ret = OB_IO_ERROR;
    }
  }

  return ret;
}

int RocksDbStoreService::batch_write(void *cf_handle,
    const common::ObArray<ObLogStoreKey> &keys,
    const common::ObArray<ObSlice> &values)
{
  int ret = OB_SUCCESS;
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);
  rocksdb::WriteOptions writer_options;
  writer_options.disableWAL = true;

  if (OB_ISNULL(column_family_handle)) {
    LOG_ERROR("column_family_handle is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (keys.count() != values.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("keys and values count mismatch", "key_count", keys.count(), "value_count", values.count());
  } else {
    rocksdb::WriteBatch batch;

    // Convert ObLogStoreKey to string and add to WriteBatch
    for (int64_t idx = 0; OB_SUCC(ret) && !is_stopped() && idx < keys.count(); ++idx) {
      std::string key_str;
      const ObLogStoreKey &key = keys.at(idx);
      if (OB_FAIL(key.get_key(key_str))) {
        LOG_ERROR("get_key from ObLogStoreKey fail", KR(ret), K(idx), "key_count", keys.count());
      } else {
        rocksdb::Status s;
        RETRY_FUNC_ON_IO_ERROR_WITH_USLEEP_MS(is_stopped(), 1 * _SEC_, batch, Put, column_family_handle,
            rocksdb::Slice(key_str),
            rocksdb::Slice(values.at(idx).buf_, values.at(idx).buf_len_));

        if (!s.ok()) {
          ret = OB_IO_ERROR;
          _LOG_ERROR("RocksDbStoreService build batch failed, error %s", s.ToString().c_str());
        }
      }
    }

    if (OB_SUCC(ret) && !is_stopped()) {
      rocksdb::Status s;
      RETRY_FUNC_ON_IO_ERROR_WITH_USLEEP_MS(is_stopped(), 1 * _SEC_, (*m_db_), Write, writer_options, &batch);

      if (!s.ok()) {
        _LOG_ERROR("RocksDbStoreService WriteBatch put value into rocksdb failed, error %s", s.ToString().c_str());
        ret = OB_IO_ERROR;
      }
    }

    if (is_stopped()) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

int RocksDbStoreService::get(const std::string &key, std::string &value)
{
  int ret = OB_SUCCESS;

  if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else {
    rocksdb::ReadOptions read_options;
    read_options.verify_checksums = (TCONF.rocksdb_read_verify_checksums != 0);
    rocksdb::Status s = m_db_->Get(read_options, key, &value);

    if (!s.ok()) {
      _LOG_ERROR("RocksDbStoreService get value from rocksdb failed, error %s, key:%s",
          s.ToString().c_str(), key.c_str());
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

int RocksDbStoreService::get(void *cf_handle, const std::string &key, std::string &value)
{
  int ret = OB_SUCCESS;
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);

  if (OB_ISNULL(column_family_handle)) {
    LOG_ERROR("column_family_handle is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else {
    rocksdb::ReadOptions read_options;
    read_options.verify_checksums = (TCONF.rocksdb_read_verify_checksums != 0);
    rocksdb::Status s = m_db_->Get(read_options, column_family_handle, key, &value);

    if (s.IsNotFound()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (!s.ok()) {
      _LOG_ERROR("RocksDbStoreService get value from rocksdb failed, error %s, key:%s",
          s.ToString().c_str(), key.c_str());
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

int RocksDbStoreService::del(const std::string &key)
{
  int ret = OB_SUCCESS;

  if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else {
    // find column family handle for cf
    rocksdb::WriteOptions writer_options;
    writer_options.disableWAL = true;
    rocksdb::Status s;
    RETRY_FUNC_ON_IO_ERROR_WITH_USLEEP_MS(is_stopped(), 1 * _SEC_, (*m_db_), Delete, writer_options, key);

    if (!s.ok()) {
      ret = OB_IO_ERROR;
      _LOG_ERROR("delete %s from rocksdb failed, error %s", key.c_str(), s.ToString().c_str());
    }
  }

  return ret;
}

int RocksDbStoreService::del(void *cf_handle, const std::string &key)
{
  int ret = OB_SUCCESS;
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);
  rocksdb::WriteOptions writer_options;
  writer_options.disableWAL = true;

  if (OB_ISNULL(column_family_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column_family_handle is NULL", KR(ret));
  } else if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else {
    rocksdb::Status s;
    RETRY_FUNC_ON_IO_ERROR_WITH_USLEEP_MS(is_stopped(), 1 * _SEC_, (*m_db_), Delete, writer_options,
        column_family_handle, key);

    if (!s.ok()) {
      LOG_ERROR("delete %s from rocksdb failed, error %s", key.c_str(), s.ToString().c_str());
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

int RocksDbStoreService::batch_delete(void *cf_handle, const common::ObArray<ObLogStoreKey> &keys)
{
  int ret = OB_SUCCESS;
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);
  rocksdb::WriteOptions writer_options;
  writer_options.disableWAL = true;

  if (OB_ISNULL(column_family_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column_family_handle is NULL", KR(ret));
  } else if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else if (keys.empty()) {
    // empty keys, nothing to delete
  } else {
    rocksdb::WriteBatch batch;

    // Convert ObLogStoreKey to string and add all delete operations to WriteBatch
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      std::string key_str;
      const ObLogStoreKey &key = keys.at(i);
      if (OB_FAIL(key.get_key(key_str))) {
        LOG_ERROR("get_key from ObLogStoreKey fail", KR(ret), K(i), "key_count", keys.count());
      } else {
        rocksdb::Status s;
        RETRY_FUNC_ON_IO_ERROR_WITH_USLEEP_MS(is_stopped(), 1 * _SEC_, batch, Delete, column_family_handle, key_str);
        if (!s.ok()) {
          ret = OB_IO_ERROR;
          LOG_ERROR("batch delete add key failed", KR(ret), K(i), "key", key_str.c_str(), "error", s.ToString().c_str());
        }
      }
    }

    // Execute batch delete
    if (OB_SUCC(ret)) {
      rocksdb::Status s;
      RETRY_FUNC_ON_IO_ERROR_WITH_USLEEP_MS(is_stopped(), 1 * _SEC_, (*m_db_), Write, writer_options, &batch);

      if (!s.ok()) {
        LOG_ERROR("batch delete write failed", KR(ret), "key_count", keys.count(), "error", s.ToString().c_str());
        ret = OB_ERR_UNEXPECTED;
      } else {
        LOG_TRACE("batch delete succ", "key_count", keys.count());
      }
    }
  }

  return ret;
}

int RocksDbStoreService::del_range(void *cf_handle, const std::string &begin_key, const std::string &end_key)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = get_timestamp();
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);

  if (OB_ISNULL(column_family_handle)) {
    LOG_ERROR("column_family_handle is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else {
    rocksdb::WriteOptions writer_options;
    writer_options.disableWAL = true;
    rocksdb::Status s;
    RETRY_FUNC_ON_IO_ERROR_WITH_USLEEP_MS(is_stopped(), 1 * _SEC_, (*m_db_), DeleteRange, writer_options,
        column_family_handle, begin_key, end_key);

    if (!s.ok()) {
      LOG_ERROR("DeleteRange %s from rocksdb failed, error %s", begin_key.c_str(), s.ToString().c_str());
      ret = OB_ERR_UNEXPECTED;
    } else {
      // NOTICE invoke this interface lob data clean task interval
      double time_cost = (get_timestamp() - start_ts)/1000.0;
      _LOG_INFO("DEL_RANGE time_cost=%.3lfms start_key=%s end_key=%s", time_cost, begin_key.c_str(), end_key.c_str());
    }
  }

  return ret;
}

int RocksDbStoreService::compact_range(
    void *cf_handle,
    const std::string &begin_key,
    const std::string &end_key,
    const bool op_entire_cf)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = get_timestamp();
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);
  rocksdb::CompactRangeOptions compact_option;
  compact_option.change_level = true;

  if (OB_ISNULL(column_family_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column_family_handle is NULL");
  } else if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else {
    rocksdb::Status s;
    if (op_entire_cf) {
      // compact from nullptr to nullptr means compact all data in the column_family
      s = m_db_->CompactRange(compact_option, column_family_handle, nullptr, nullptr);
    } else {
      rocksdb::Slice begin(begin_key);
      rocksdb::Slice end(end_key);
      s = m_db_->CompactRange(compact_option, column_family_handle, &begin, &end);
    }

    if (!s.ok()) {
      _LOG_WARN("COMPACT_RANGE [%s - %s] failed, reason: [%s]", begin_key.c_str(), end_key.c_str(), s.ToString().c_str());
    } else {
      int64_t time_cost = get_timestamp() - start_ts;
      _LOG_INFO("COMPACT_RANGE [%s - %s] time_cost=%s", begin_key.c_str(), end_key.c_str(), TVAL_TO_STR(time_cost));
    }
  }
  return ret;
}

int RocksDbStoreService::flush(void *cf_handle)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = get_timestamp();
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);
  rocksdb::FlushOptions flush_option; // default wait=true, allow_wait_stall=false

  if (OB_ISNULL(column_family_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column_family_handle is NULL");
  } else if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else {
    rocksdb::Status s = m_db_->Flush(flush_option, column_family_handle);

    if (! s.ok()) {
      _LOG_WARN("ROCKSDB FLUSH failed, reason: [%s]", s.ToString().c_str());
    } else {
      int64_t time_cost = get_timestamp() - start_ts;
      _LOG_INFO("ROCKSDB FLUSH SUCC, time_cost=%s", TVAL_TO_STR(time_cost));
    }
  }

  return ret;
}

int RocksDbStoreService::create_column_family(const std::string& column_family_name,
    void *&cf_handle)
{
  int ret = OB_SUCCESS;
  const int64_t rocksdb_write_buffer_size = TCONF.rocksdb_write_buffer_size;
  rocksdb::ColumnFamilyHandle *column_family_handle = NULL;
  rocksdb::ColumnFamilyOptions cf_options;

  cf_options.level0_file_num_compaction_trigger = 4;
  cf_options.level0_slowdown_writes_trigger = 48;
  cf_options.level0_stop_writes_trigger = 64;

  cf_options.max_write_buffer_number = 9;
  cf_options.min_write_buffer_number_to_merge = 2;
  cf_options.write_buffer_size = rocksdb_write_buffer_size * _M_;

  // Enable memtable bloom filter for faster point-lookup negative matches
  cf_options.memtable_prefix_bloom_size_ratio = 0.1;
  cf_options.memtable_whole_key_filtering = true;

  cf_options.num_levels = 7;
  cf_options.max_bytes_for_level_base = 2ULL * 1024 * 1024 * 1024;
  cf_options.target_file_size_base = 64 * 1024 * 1024;
  cf_options.level_compaction_dynamic_level_bytes = true;

  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = shared_block_cache_;

  // Ribbon filter: ~30% smaller than Bloom at same accuracy.
  // bloom_before_level=0: Bloom for flushes (fast build), Ribbon for L1+ (space efficient).
  table_options.filter_policy.reset(
    rocksdb::NewRibbonFilterPolicy(16, 0)
  );

  // Put index/filter in block cache for bounded memory across many CFs
  table_options.cache_index_and_filter_blocks = true;
  table_options.cache_index_and_filter_blocks_with_high_priority = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;

  // Pre-populate block cache on flush to avoid read-after-write disk IO
  // (CDC writes redo, reads it back soon after — this eliminates the read)
  table_options.prepopulate_block_cache =
      rocksdb::BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly;

  table_options.block_size = 32 * 1024;
  table_options.enable_index_compression = false;
  cf_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  cf_options.compaction_pri = rocksdb::CompactionPri::kMinOverlappingRatio;

  // CDC lookups are almost always hits (put → get → delete on same key),
  // so skip building filters on the bottommost level to save CPU and memory.
  cf_options.optimize_filters_for_hits = true;

  if (is_enable_compress_) {
    std::vector<rocksdb::CompressionType> compressions;
    compressions.resize(cf_options.num_levels);

    compressions[0] = rocksdb::kNoCompression;
    compressions[1] = rocksdb::kNoCompression;

    for (int i = 2; i <= 4 && i < cf_options.num_levels; i++) {
        compressions[i] = rocksdb::kLZ4Compression;
    }

    for (int i = 5; i < cf_options.num_levels; i++) {
        compressions[i] = rocksdb::kZSTD;
    }

    cf_options.compression_per_level = compressions;
    cf_options.bottommost_compression = rocksdb::kZSTD;

    cf_options.compression_opts.level = 3;
    // Parallel compression threads per flush/compaction job (RocksDB 10.7+).
    // Beneficial for ZSTD/LZ4 heavy layers; each bg job spawns up to this
    // many threads for block-level parallel compression.
    cf_options.compression_opts.parallel_threads = 4;
    cf_options.bottommost_compression_opts.parallel_threads = 4;
    cf_options.bottommost_compression_opts.enabled = true;
  }

  if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
  } else {
    rocksdb::Status status = m_db_->CreateColumnFamily(cf_options, column_family_name, &column_family_handle);

    if (! status.ok()) {
      _LOG_ERROR("rocksdb CreateColumnFamily [%s] failed, error %s", column_family_name.c_str(), status.ToString().c_str());
      ret = OB_ERR_UNEXPECTED;
    } else {
      cf_handle = reinterpret_cast<void *>(column_family_handle);

      LOG_INFO("rocksdb CreateColumnFamily succ", "column_family_name", column_family_name.c_str(),
          K(column_family_handle), K(cf_handle), K(rocksdb_write_buffer_size));
    }
  }

  return ret;
}

int RocksDbStoreService::drop_column_family(void *cf_handle)
{
  int ret = OB_SUCCESS;
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);

  if (OB_ISNULL(column_family_handle)) {
    LOG_ERROR("column_family_handle is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else {
    rocksdb::Status status = m_db_->DropColumnFamily(column_family_handle);

    if (! status.ok()) {
      _LOG_ERROR("rocksdb DropColumnFamily failed, error %s", status.ToString().c_str());
      ret = OB_ERR_UNEXPECTED;
    } else {
      LOG_INFO("rocksdb DropColumnFamily succ");
    }
  }

  return ret;
}

int RocksDbStoreService::destory_column_family(void *cf_handle)
{
  int ret = OB_SUCCESS;
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);

  if (OB_ISNULL(column_family_handle)) {
    LOG_ERROR("column_family_handle is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else {
    rocksdb::Status status = m_db_->DestroyColumnFamilyHandle(column_family_handle);

    if (! status.ok()) {
      _LOG_ERROR("rocksdb DestroyColumnFamilyHandle failed, error %s", status.ToString().c_str());
      ret = OB_ERR_UNEXPECTED;
    } else {
      LOG_INFO("rocksdb DestroyColumnFamilyHandle succ");
    }
  }

  return ret;
}

void RocksDbStoreService::get_mem_usage(const std::vector<uint64_t> ids,
    const std::vector<void *> cf_handles)
{
  int ret = OB_SUCCESS;
  int64_t total_memtable_usage = 0;
  int64_t total_block_cache_usage = 0;
  int64_t total_table_readers_usage = 0;
  int64_t total_block_cache_pinned_usage = 0;

  for (int64_t idx = 0; OB_SUCC(ret) && !is_stopped() && idx < cf_handles.size(); ++idx) {
    rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handles[idx]);

    if (OB_ISNULL(column_family_handle)) {
      LOG_ERROR("column_family_handle is NULL");
      ret = OB_INVALID_ARGUMENT;
    } else {
      std::string memtable_usage;
      std::string block_cache_usage;
      std::string table_readers_usage;
      std::string block_cache_pinned_usage;
      int64_t int64_memtable_usage = 0;
      int64_t int64_block_cache_usage = 0;
      int64_t int64_table_readers_usage = 0;
      int64_t int64_block_cache_pinned_usage = 0;

      m_db_->GetProperty(column_family_handle, "rocksdb.cur-size-all-mem-tables", &memtable_usage);
      m_db_->GetProperty(column_family_handle, "rocksdb.block-cache-usage", &block_cache_usage);
      m_db_->GetProperty(column_family_handle, "rocksdb.estimate-table-readers-mem", &table_readers_usage);
      m_db_->GetProperty(column_family_handle, "rocksdb.block-cache-pinned-usage", &block_cache_pinned_usage);

      c_str_to_int(memtable_usage.c_str(), int64_memtable_usage);
      c_str_to_int(block_cache_usage.c_str(), int64_block_cache_usage);
      c_str_to_int(table_readers_usage.c_str(), int64_table_readers_usage);
      c_str_to_int(block_cache_pinned_usage.c_str(), int64_block_cache_pinned_usage);

      total_memtable_usage += int64_memtable_usage;
      total_block_cache_usage += int64_block_cache_usage;
      total_table_readers_usage += int64_table_readers_usage;
      total_block_cache_pinned_usage += int64_block_cache_pinned_usage;

      LOG_INFO("[ROCKSDB] [MEM]", "tenant_id", ids[idx],
          "memtable", memtable_usage.c_str(),
          "block_cache", block_cache_usage.c_str(),
          "table_readers", table_readers_usage.c_str(),
          "block_cache_pinned", block_cache_pinned_usage.c_str());
    }
  } // for

  LOG_INFO("[ROCKSDB] [TOTAL_MEM]",
      "memtable", SIZE_TO_STR(total_memtable_usage),
      "block_cache", SIZE_TO_STR(total_block_cache_usage),
      "table_readers", SIZE_TO_STR(total_table_readers_usage),
      "block_cache_pinned", SIZE_TO_STR(total_block_cache_pinned_usage));

  if (m_options_.write_buffer_manager && m_options_.write_buffer_manager->enabled()) {
    LOG_INFO("[ROCKSDB] [WRITE_BUFFER_MGR]",
        "memory_usage", SIZE_TO_STR(m_options_.write_buffer_manager->memory_usage()),
        "mutable_memtable_usage", SIZE_TO_STR(m_options_.write_buffer_manager->mutable_memtable_memory_usage()),
        "buffer_size", SIZE_TO_STR(m_options_.write_buffer_manager->buffer_size()));
  }

  print_db_stats_info_();
}

int RocksDbStoreService::get_mem_usage(void * cf_handle, int64_t &estimate_live_data_size, int64_t &estimate_num_keys)
{
  int ret = OB_SUCCESS;
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);

  if (OB_ISNULL(column_family_handle)) {
    LOG_ERROR("column_family_handle is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else {
    std::string estimate_live_data_size_str;
    std::string estimate_num_keys_str;

    m_db_->GetProperty(column_family_handle, "rocksdb.estimate-live-data-size", &estimate_live_data_size_str);
    m_db_->GetProperty(column_family_handle, "rocksdb.estimate-num-keys", &estimate_num_keys_str);

    c_str_to_int(estimate_live_data_size_str.c_str(), estimate_live_data_size);
    c_str_to_int(estimate_num_keys_str.c_str(), estimate_num_keys);
  }
  return ret;
}

void RocksDbStoreService::print_db_stats_info_() const
{
  int ret = OB_SUCCESS;
  // 使用 MemoryUtil 获取 RocksDB 整体(所有 column_family) 内存占用
  std::map<rocksdb::MemoryUtil::UsageType, uint64_t> usage_by_type;
  std::vector<rocksdb::DB*> dbs = {m_db_};
  std::unordered_set<const rocksdb::Cache*> caches;  // 传空, MemoryUtil 会自动从 DB 获取 block cache

  rocksdb::Status s = rocksdb::MemoryUtil::GetApproximateMemoryUsageByType(dbs, caches, &usage_by_type);
  if (!s.ok()) {
    LOG_WARN("[ROCKSDB] [OVERALL_MEM] GetApproximateMemoryUsageByType failed", "error", s.ToString().c_str());
    return;
  }

  // 从 map 中提取各项内存使用
  int64_t memtable_total = static_cast<int64_t>(usage_by_type[rocksdb::MemoryUtil::kMemTableTotal]);
  int64_t memtable_unflushed = static_cast<int64_t>(usage_by_type[rocksdb::MemoryUtil::kMemTableUnFlushed]);
  int64_t table_readers_total = static_cast<int64_t>(usage_by_type[rocksdb::MemoryUtil::kTableReadersTotal]);
  int64_t cache_total = static_cast<int64_t>(usage_by_type[rocksdb::MemoryUtil::kCacheTotal]);

  // 计算总内存: memtable + table_readers + cache
  int64_t total_memory_usage = memtable_total + table_readers_total + cache_total;

  // 整体内存占用(所有 CF) - 使用 MemoryUtil 获取的精确值
  LOG_INFO("[ROCKSDB] [OVERALL_MEM] [ALL_CF]",
      "total_memory_usage", SIZE_TO_STR(total_memory_usage),
      "memtable_total", SIZE_TO_STR(memtable_total),
      "memtable_unflushed", SIZE_TO_STR(memtable_unflushed),
      "table_readers_total", SIZE_TO_STR(table_readers_total),
      "block_cache_total", SIZE_TO_STR(cache_total));

  // 内存分布(各组件占总内存比例)
  if (total_memory_usage > 0) {
    LOG_INFO("[ROCKSDB] [OVERALL_MEM] [MEM_DISTRIBUTION]",
        "total", SIZE_TO_STR(total_memory_usage),
        "memtable_pct", (100 * memtable_total / total_memory_usage),
        "memtable_unflushed_pct", (100 * memtable_unflushed / total_memory_usage),
        "table_readers_pct", (100 * table_readers_total / total_memory_usage),
        "block_cache_pct", (100 * cache_total / total_memory_usage));
  }
}

}
}
