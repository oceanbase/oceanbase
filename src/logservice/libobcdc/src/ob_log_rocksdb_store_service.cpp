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

namespace oceanbase
{
namespace libobcdc
{
RocksDbStoreService::RocksDbStoreService()
{
  is_inited_ = false;
  is_stopped_ = true;
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
  } else {
    m_db_path_ = path;
    m_options_.create_if_missing = true;
    const int total_threads = 32;
    // By default, RocksDB uses only one background thread for flush and
    // compaction. Calling this function will set it up such that total of
    // `total_threads` is used. Good value for `total_threads` is the number of
    // cores. You almost definitely want to call this function if your system is
    // bottlenecked by RocksDB.
    m_options_.IncreaseParallelism(total_threads);
    // Maximum number of concurrent background compaction jobs, submitted to the default LOW priority thread pool.
    m_options_.max_background_compactions = 16;
    m_options_.max_background_flushes = 16;
    // 2G
    m_options_.db_write_buffer_size = 2 << 30;
    m_options_.max_open_files = 100;

    rocksdb::Status s = rocksdb::DB::Open(m_options_, m_db_path_, &m_db_);
    if (!s.ok()) {
      _LOG_ERROR("first open rocks db failed, path is %s, status is %s",
          m_db_path_.c_str(), s.ToString().c_str());
      ret = OB_ERR_UNEXPECTED;
    } else {
      _LOG_INFO("RocksDbStoreService init success, path:%s, total_threads=%d", m_db_path_.c_str(), total_threads);
      is_stopped_ = false;
      is_inited_ = true;
    }

  }

  return ret;
}

int RocksDbStoreService::close()
{
  int ret = OB_SUCCESS;

  if (NULL != m_db_) {
    LOG_INFO("closing rocksdb ...");
    mark_stop_flag();
    usleep(5 * _SEC_);

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
    rocksdb::Status s = m_db_->Put(
        writer_options,
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
    rocksdb::Status s = m_db_->Put(writer_options, column_family_handle, rocksdb::Slice(key),
        rocksdb::Slice(value.buf_, value.buf_len_));

    if (!s.ok()) {
      _LOG_ERROR("RocksDbStoreService put value into rocksdb failed, error %s", s.ToString().c_str());
      ret = OB_IO_ERROR;
    }
  }

  return ret;
}

int RocksDbStoreService::batch_write(void *cf_handle,
    const std::vector<std::string> &keys,
    const std::vector<ObSlice> &values)
{
  int ret = OB_SUCCESS;
  rocksdb::ColumnFamilyHandle *column_family_handle = static_cast<rocksdb::ColumnFamilyHandle *>(cf_handle);
  rocksdb::WriteOptions writer_options;
  writer_options.disableWAL = true;

  if (OB_ISNULL(column_family_handle)) {
    LOG_ERROR("column_family_handle is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else {
    rocksdb::WriteBatch batch;

    for (int64_t idx = 0; OB_SUCC(ret) && !is_stopped() && idx < keys.size(); ++idx) {
      rocksdb::Status s = batch.Put(
          column_family_handle,
          rocksdb::Slice(keys[idx]),
          rocksdb::Slice(values[idx].buf_, values[idx].buf_len_));
      if (!s.ok()) {
        ret = OB_IO_ERROR;
        _LOG_ERROR("RocksDbStoreService build batch failed, error %s", s.ToString().c_str());
      }
    }

    if (OB_SUCC(ret) && !is_stopped()) {
      rocksdb::Status s = m_db_->Write(writer_options, &batch);

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
    //rocksdb::PinnableSlice slice(&value);
    rocksdb::Status s = m_db_->Get(rocksdb::ReadOptions(), key, &value);

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
    rocksdb::Status s = m_db_->Get(rocksdb::ReadOptions(), column_family_handle, key, &value);

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
    rocksdb::Status s = m_db_->Delete(rocksdb::WriteOptions(), key);
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
    rocksdb::Status s = m_db_->Delete(writer_options, column_family_handle, key);

    if (!s.ok()) {
      LOG_ERROR("delete %s from rocksdb failed, error %s", key.c_str(), s.ToString().c_str());
      ret = OB_ERR_UNEXPECTED;
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
    rocksdb::Status s = m_db_->DeleteRange(rocksdb::WriteOptions(), column_family_handle,
        begin_key, end_key);

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

int RocksDbStoreService::compact_range(void *cf_handle, const std::string &begin_key, const std::string &end_key)
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
    rocksdb::Slice begin(begin_key);
    rocksdb::Slice end(end_key);
    rocksdb::Status s = m_db_->CompactRange(rocksdb::CompactRangeOptions(), column_family_handle,
        &begin, &end);

    if (!s.ok()) {
      LOG_ERROR("CompactRange %s from rocksdb failed, error %s", begin_key.c_str(), s.ToString().c_str());
      ret = OB_ERR_UNEXPECTED;
    } else {
      // NOTICE invoke this interface lob data clean task interval
      double time_cost = (get_timestamp() - start_ts)/1000.0;
      _LOG_INFO("COMPACT_RANGE time_cost=%.3lfms start_key=%s end_key=%s", time_cost, begin_key.c_str(), end_key.c_str());
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

  cf_options.OptimizeLevelStyleCompaction();
  cf_options.level0_slowdown_writes_trigger = 32;
  // The maximum number of write buffers that are built up in memory.
  // The default and the minimum number is 2, so that when 1 write buffer
  // is being flushed to storage, new writes can continue to the other
  // write buffer.
  // If max_write_buffer_number > 3, writing will be slowed down to
  // options.delayed_write_rate if we are writing to the last write buffer
  // allowed.
  //
  // Default: 2
  cf_options.max_write_buffer_number = 9;
  // Column Family's default memtable size is 64M, when the maximum limit is exceeded, memtable -> immutable memtable, increase write_buffer_size, can reduce write amplification
  cf_options.write_buffer_size = rocksdb_write_buffer_size << 20;
  // config rocksdb compression
  // supported compress algorithms will print in LOG file
  // cf_options.compression = rocksdb::CompressionType::kLZ4Compression;
  // cf_options.bottommost_compression = rocksdb::CompressionType::kZSTD;

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
  } else if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
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
  } else if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
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

}
}
