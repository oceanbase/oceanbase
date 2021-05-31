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

#include "storage/ob_partition_storage.h"
#include "share/ob_worker.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/utility/utility.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/word_segment/ob_word_segment.h"
#include "lib/bloom_filter/ob_bloomfilter.h"
#include "lib/file/file_directory_utils.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/row/ob_row_store.h"
#include "storage/ob_base_storage_info.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/ob_debug_sync.h"
#include "share/ob_cluster_version.h"
#include "share/ob_index_trans_status_reporter.h"
#include "share/ob_index_task_table_operator.h"
#include "share/ob_index_status_table_operator.h"
#include "share/ob_index_checksum.h"
#include "share/ob_index_build_stat.h"
#include "share/ob_sstable_checksum_operator.h"
#include "share/ob_unique_index_row_transformer.h"
#include "share/allocator/ob_memstore_allocator_mgr.h"
#include "common/ob_range.h"
#include "share/ob_tenant_mgr.h"
#include "storage/ob_table_scan_iterator.h"
#include "storage/ob_value_row_iterator.h"
#include "storage/ob_i_partition_component_factory.h"
#include "storage/ob_i_partition_report.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "storage/compaction/ob_partition_merge_util.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_saved_storage_info.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/transaction/ob_trans_part_ctx.h"
#include "storage/ob_single_merge.h"
#include "storage/ob_multiple_get_merge.h"
#include "storage/ob_multiple_scan_merge.h"
#include "storage/ob_index_merge.h"
#include "storage/ob_query_iterator_factory.h"
#include "storage/ob_partition_merge_task.h"
#include "storage/ob_partition_split_task.h"
#include "storage/ob_interm_macro_mgr.h"
#include "storage/ob_long_ops_monitor.h"
#include "storage/ob_freeze_info_snapshot_mgr.h"
#include "storage/ob_store_row_comparer.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "ob_warm_up.h"
#include "observer/ob_server_struct.h"
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_define.h"
#include "sql/engine/ob_operator.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "ob_partition_service.h"
#include "storage/ob_sstable_merge_info_mgr.h"
#include "share/ob_task_define.h"
#include <libgen.h>
#include <sys/resource.h>
#include "ob_table_mgr.h"
#include "storage/ob_store_row_filter.h"
#include "storage/ob_partition_split.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "lib/hash/ob_hashmap.h"
#include "ob_partition_range_spliter.h"
#include "storage/ob_sstable_dump_error_info.h"
#include "storage/ob_pg_storage.h"

namespace oceanbase {
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;
using namespace oceanbase::compaction;
using namespace oceanbase::memtable;

namespace storage {

// can only reset pointer of memtable, cannot reset other variables
void ObStorageWriterGuard::reset()
{
  if (NULL != memtable_) {
    memtable_->dec_write_ref();
    memtable_ = NULL;
  }
}

bool ObStorageWriterGuard::need_to_refresh_table(ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;

  ObITable* table = tables_handle.get_last_table();
  if (NULL == table) {
    bool_ret = true;
  } else if (!table->is_memtable()) {
    bool_ret = false;
  } else if (tables_handle.check_store_expire()) {
    bool_ret = true;
  } else if (OB_FAIL(static_cast<ObMemtable*>(table)->inc_write_ref())) {
    bool_ret = true;
  } else {
    bool_ret = false;
    memtable_ = static_cast<ObMemtable*>(table);
  }
  if (bool_ret && check_if_need_log()) {
    LOG_ERROR("refresh table too much times", K(ret), KP(table), K(store_ctx_.cur_pkey_));
  }

  return bool_ret;
}

int ObStorageWriterGuard::refresh_and_protect_table(ObRelativeTable& relative_table)
{
  int ret = OB_SUCCESS;
  ObTablesHandle& tables_handle = relative_table.tables_handle_;

  // cannot reset store_
  reset();
  while (OB_SUCC(ret) && need_to_refresh_table(tables_handle)) {
    if (OB_FAIL(store_->get_read_tables(relative_table.get_table_id(),
            store_ctx_.mem_ctx_->get_read_snapshot(),
            tables_handle,
            relative_table.allow_not_ready()))) {
      STORAGE_LOG(WARN, "fail to get read tables", K(ret), "pkey", relative_table.get_table_id());
    }
  }

  return ret;
}

bool ObStorageWriterGuard::check_if_need_log()
{
  bool need_log = false;
  if ((++retry_count_ % GET_TS_INTERVAL) == 0) {
    const int64_t cur_ts = common::ObTimeUtility::current_time();
    if (0 >= last_ts_) {
      last_ts_ = cur_ts;
    } else if (cur_ts - last_ts_ >= LOG_INTERVAL_US) {
      last_ts_ = cur_ts;
      need_log = true;
    } else {
      // do nothing
    }
  }
  return need_log;
}

// called by pg, cannot reset pg_memtable_mgr_
int ObStorageWriterGuard::refresh_and_protect_pg_memtable(ObPGStorage& pg_storage, ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;

  reset();
  if (OB_ISNULL(pg_memtable_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg memtable mgr is null, unexpected error", K(ret), KP_(pg_memtable_mgr));
  } else {
    static const int64_t MAX_RETRY_CNT = 4000;
    static const int64_t FAST_RETRY_CNT = 1000;

    const bool reset_handle = true;
    const bool include_active_memtable = true;
    while (OB_SUCC(ret) && need_to_refresh_table(tables_handle)) {
      if (retry_count_ > MAX_RETRY_CNT) {
        ret = OB_EAGAIN;
        STORAGE_LOG(ERROR, "refresh memtable retry too many times", K(ret), K(store_ctx_.cur_pkey_));
      } else if (retry_count_ > FAST_RETRY_CNT) {
        usleep(retry_count_);
      }

      if (OB_FAIL(ret)) {
      } else if (pg_storage.is_empty_pg()) {
        // special error code
        ret = OB_EMPTY_PG;
        STORAGE_LOG(WARN, "refresh and protect pg memtable error", K(ret), "pg_key", pg_storage.get_partition_key());
      } else if (OB_FAIL(pg_memtable_mgr_->get_memtables(
                     tables_handle, reset_handle, -1 /*read latest memtable*/, include_active_memtable))) {
        STORAGE_LOG(WARN, "fail to get memtables", K(ret));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

ObStorageWriterGuard::~ObStorageWriterGuard()
{
  uint32_t& interval = get_writing_throttling_sleep_interval();
  if ((!is_replay_) && need_control_mem_ && interval > 0) {
    uint64_t timeout = 10000;  // 10s
    common::ObWaitEventGuard wait_guard(common::ObWaitEventIds::MEMSTORE_MEM_PAGE_ALLOC_WAIT, timeout, 0, 0, interval);
    bool need_sleep = true;
    const int32_t SLEEP_INTERVAL_PER_TIME = 100 * 1000;  // 100ms
    int64_t left_interval = interval;
    if (NULL != store_ctx_.mem_ctx_ && !is_replay_) {
      int64_t query_left_time = store_ctx_.mem_ctx_->get_abs_lock_wait_timeout() - ObTimeUtility::current_time();
      left_interval = min(left_interval, query_left_time);
    }
    if (NULL != memtable_) {
      need_sleep = memtable_->is_active_memtable();
    }

    reset();
    int tmp_ret = OB_SUCCESS;
    // speed of clog replay is not controled by sleep, but by ObReplayEngine's control
    while ((left_interval > 0) && need_sleep) {
      // because left_interval and SLEEP_INTERVAL_PER_TIME both are greater than
      // zero, so it's safe to convert to uint32_t, be careful with comparation between int and uint
      uint32_t sleep_interval = static_cast<uint32_t>(min(left_interval, SLEEP_INTERVAL_PER_TIME));
      usleep(sleep_interval);
      left_interval -= sleep_interval;
      if (NULL != store_ctx_.mem_ctx_) {
        ObGMemstoreAllocator* memstore_allocator = NULL;
        if (OB_SUCCESS != (tmp_ret = ObMemstoreAllocatorMgr::get_instance().get_tenant_memstore_allocator(
                               store_ctx_.mem_ctx_->get_tenant_id(), memstore_allocator))) {
        } else if (OB_ISNULL(memstore_allocator)) {
          OB_LOG(WARN, "get_tenant_mutil_allocator failed", K(store_ctx_.cur_pkey_), K(tmp_ret));
        } else {
          need_sleep = memstore_allocator->need_do_writing_throttle();
        }
      }
    }
  }
  reset();
  if (!is_replay_) {
    interval = 0;
  }
}

bool ObPartitionPrefixAccessStat::AccessStat::is_valid() const
{
  return bf_access_cnt_ > 0;
}

ObPartitionPrefixAccessStat& ObPartitionPrefixAccessStat::operator=(const ObPartitionPrefixAccessStat& other)
{
  if (this != &other) {
    MEMCPY(this, &other, sizeof(ObPartitionPrefixAccessStat));
  }
  return *this;
}

int ObPartitionPrefixAccessStat::add_stat(const ObTableAccessStat& stat)
{
  int ret = OB_SUCCESS;
  const int64_t prefix = stat.rowkey_prefix_;
  if (OB_UNLIKELY(prefix > MAX_ROWKEY_PREFIX_NUM)) {
  } else if (OB_UNLIKELY(prefix < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rowkey prefix should be greater than 0");
  } else {
    AccessStat& rowkey_prefix = rowkey_prefix_[prefix];
    rowkey_prefix.bf_access_cnt_ += stat.bf_access_cnt_;
    rowkey_prefix.bf_filter_cnt_ += stat.bf_filter_cnt_;
    rowkey_prefix.empty_read_cnt_ += stat.empty_read_cnt_;
  }
  return ret;
}

int ObPartitionPrefixAccessStat::add_stat(const ObTableScanStatistic& stat)
{
  int ret = OB_SUCCESS;
  const int64_t prefix = stat.rowkey_prefix_;
  if (OB_UNLIKELY(prefix > MAX_ROWKEY_PREFIX_NUM)) {
  } else if (OB_UNLIKELY(prefix < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rowkey prefix should be greater than 0");
  } else {
    AccessStat& rowkey_prefix = rowkey_prefix_[prefix];
    rowkey_prefix.bf_access_cnt_ += stat.bf_access_cnt_;
    rowkey_prefix.bf_filter_cnt_ += stat.bf_filter_cnt_;
    rowkey_prefix.empty_read_cnt_ += stat.empty_read_cnt_;
  }
  return ret;
}

int ObPartitionPrefixAccessStat::get_optimal_prefix(int64_t& prefix)
{
  int ret = OB_SUCCESS;
  int64_t opt_prefix = 0;
  int64_t filter_cnt = 0;
  for (int i = 1; i <= MAX_ROWKEY_PREFIX_NUM; ++i) {
    if (filter_cnt < rowkey_prefix_[i].bf_filter_cnt_) {
      filter_cnt = rowkey_prefix_[i].bf_filter_cnt_;
      opt_prefix = i;
    }
  }
  if (opt_prefix > 0) {
    prefix = opt_prefix;
  } else if (0 == opt_prefix && prefix <= MAX_ROWKEY_PREFIX_NUM) {
    ret = OB_INVALID_ARGUMENT;
    prefix = 0;
    STORAGE_LOG(DEBUG, "invalid rowkey prefix", K(opt_prefix), K(prefix));
  }
  return ret;
}

// json format, do not quote json names to make string more readable
// example [{prefix:1,filter:0,access:1,empty:1},{prefix:2,filter:2,access:2,empty:0}]
int64_t ObPartitionPrefixAccessStat::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int print_count = 0;
  J_ARRAY_START();
  for (int i = 0; i <= MAX_ROWKEY_PREFIX_NUM; ++i) {
    const AccessStat& stat = rowkey_prefix_[i];
    if (stat.is_valid()) {
      if (print_count > 0) {
        J_COMMA();
      }
      J_OBJ_START();
      J_KV("prefix", i, "filter", stat.bf_filter_cnt_, "access", stat.bf_access_cnt_, "empty", stat.empty_read_cnt_);
      J_OBJ_END();
      print_count++;
    }
  }
  J_ARRAY_END();
  return pos;
}

template <typename T>
int ObPartitionStorage::get_merge_opt(const int64_t merge_version, const int64_t storage_version,
    const int64_t work_version, const int64_t born_version, const T& old_val, const T& new_val, T& opt)
{
  int ret = OB_SUCCESS;
  // TODO(lincang: after merge_version changed to trans_version, needs to check <= 0)
  if (merge_version < 0 || storage_version < 0 || work_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(merge_version), K(storage_version), K(work_version));
  } else {
    if (storage_version == born_version) {
      if (work_version > merge_version) {
        opt = old_val;
      } else {
        opt = new_val;
      }
    } else if (storage_version < born_version) {
      opt = old_val;
    } else if (storage_version > born_version) {
      opt = new_val;
    }
  }
  return ret;
}

ObPartitionStorage::ObPartitionStorage()
    : cluster_version_(CLUSTER_VERSION_140),
      pkey_(),
      schema_service_(NULL),
      cp_fty_(NULL),
      txs_(NULL),
      pg_memtable_mgr_(NULL),
      is_inited_(false),
      merge_successed_(true),
      merge_timestamp_(0),
      merge_failed_cnt_(0),
      store_(),
      replay_replica_type_(REPLICA_TYPE_FULL),
      prefix_access_stat_()
{}

ObPartitionStorage::~ObPartitionStorage()
{
  destroy();
}

int ObPartitionStorage::init(const common::ObPartitionKey& pkey, ObIPartitionComponentFactory* cp_fty,
    share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs,
    ObPGMemtableMgr& pg_memtable_mgr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The storage has been inited, ", K(ret));
  } else if (!pkey.is_valid() || NULL == schema_service || NULL == cp_fty || NULL == txs) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(schema_service), K(cp_fty), K(txs), K(ret));
  } else {
    pkey_ = pkey;
    cp_fty_ = cp_fty;
    schema_service_ = schema_service;
    txs_ = txs;
    pg_memtable_mgr_ = &pg_memtable_mgr;
    is_inited_ = true;
  }

  return ret;
}

void ObPartitionStorage::destroy()
{
  FLOG_INFO("destroy partition storage", K_(pkey), K(lbt()));
  if (OB_UNLIKELY(!is_inited_)) {
    store_.destroy();
    pkey_.reset();
    prefix_access_stat_.reset();
    schema_service_ = NULL;
    cp_fty_ = NULL;
    pg_memtable_mgr_ = NULL;
    is_inited_ = false;
  }
}

bool ObPartitionStorage::is_inited() const
{
  bool bool_ret = true;

  // no lock, caller should control concurrency
  if (!is_inited_) {
    bool_ret = false;
  } else if (!store_.is_inited()) {
    bool_ret = false;
  } else {
    // do nothing
  }
  return bool_ret;
}

int ObPartitionStorage::ObDMLRunningCtx::prepare_column_desc(
    const common::ObIArray<uint64_t>& column_ids, const ObRelativeTable& table, ObColDescIArray& col_descs)
{
  int ret = OB_SUCCESS;
  int64_t count = column_ids.count();
  if (column_ids.count() <= 0 || !table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(column_ids), K(table));
  } else {
    ObColDesc col_desc;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(table.get_col_desc(column_ids.at(i), col_desc))) {
        STORAGE_LOG(WARN, "fail to get column description", "column_id", column_ids.at(i));
      } else if (OB_FAIL(col_descs.push_back(col_desc))) {
        STORAGE_LOG(WARN, "fail to add column description", K(col_desc));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::ObDMLRunningCtx::prepare_column_info(const ObIArray<uint64_t>& column_ids)
{
  int ret = OB_SUCCESS;
  const ObRelativeTable& table = relative_tables_.data_table_;
  if (column_ids.count() <= 0 || !table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(column_ids), K(table));
  } else if (table.use_schema_param()) {
    col_descs_ = &(dml_param_.table_param_->get_col_descs());
    if (col_descs_->count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "col desc is empty", K(ret), K(table));
    } else if (relative_tables_.idx_cnt_ > 0 || T_DML_UPDATE == dml_type_) {
      col_map_ = &(dml_param_.table_param_->get_col_map());
    }
  } else {
    // construct column description from schema
    void* ptr = nullptr;
    ObColDescArray* tmp_col = nullptr;
    if (OB_ISNULL(ptr = allocator_.alloc(static_cast<int64_t>(sizeof(ObColDescArray))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc column description", K(ret));
    } else if (FALSE_IT(tmp_col = new (ptr) ObColDescArray())) {
      // do nothing
    } else if (OB_FAIL(prepare_column_desc(column_ids, table, *tmp_col))) {
      STORAGE_LOG(WARN, "fail to prepare column description", K(ret));
      allocator_.free(ptr);
      ptr = nullptr;
      tmp_col = nullptr;
    } else {
      col_descs_ = tmp_col;
    }

    // construct column map from schema
    ptr = nullptr;
    ColumnMap* tmp_map = nullptr;
    if (OB_SUCC(ret) && (relative_tables_.idx_cnt_ > 0 || T_DML_UPDATE == dml_type_)) {
      if (OB_ISNULL(ptr = allocator_.alloc(static_cast<int64_t>(sizeof(ColumnMap))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "fail to alloc column map", K(ret));
      } else if (FALSE_IT(tmp_map = new (ptr) ColumnMap(allocator_))) {
        // do nothing
      } else if (OB_FAIL(tmp_map->init(column_ids))) {
        STORAGE_LOG(WARN, "init columns map fail", K(ret));
        allocator_.free(tmp_map);
        ptr = nullptr;
        tmp_map = nullptr;
      } else {
        col_map_ = tmp_map;
      }
    }
  }
  return ret;
}

int ObPartitionStorage::write_index_row(
    ObRelativeTable& relative_table, const ObStoreCtx& ctx, const ObColDescIArray& idx_columns, ObStoreRow& index_row)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(DELAY_INDEX_WRITE);
  if (OB_UNLIKELY(relative_table.is_domain_index())) {
    uint64_t domain_column_id = OB_INVALID_ID;
    if (OB_FAIL(relative_table.get_fulltext_column(domain_column_id))) {
      STORAGE_LOG(WARN, "failed to get domain column id", K(ret));
    } else {
      int64_t pos = -1;
      for (int64_t i = 0; OB_SUCC(ret) && -1 == pos && i < idx_columns.count(); ++i) {
        if (domain_column_id == idx_columns.at(i).col_id_) {
          pos = i;
        }
      }
      if (OB_SUCC(ret)) {
        ObSEArray<ObString, 8> words;
        ObObj orig_obj = index_row.row_val_.cells_[pos];
        ObString orig_string;
        if (OB_UNLIKELY(-1 == pos)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "domain index has no domain column", K(domain_column_id), K(idx_columns), K(ret));
        } else if (orig_obj.is_null()) {
          // skip
        } else if (OB_FAIL(orig_obj.get_string(orig_string))) {
          STORAGE_LOG(WARN, "get string from original obj failed", K(ret));
        } else if (OB_FAIL(split_on(orig_string, ',', words))) {
          STORAGE_LOG(WARN, "failed to split string", K(orig_string), K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < words.count(); ++i) {
            index_row.row_val_.cells_[pos].set_string(index_row.row_val_.cells_[pos].get_type(), words.at(i));
            if (OB_FAIL(
                    write_row(relative_table, ctx, relative_table.get_rowkey_column_num(), idx_columns, index_row))) {
              if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
                STORAGE_LOG(WARN,
                    "failed to write index row",
                    K(relative_table.get_table_id()),
                    K(relative_table.get_rowkey_column_num()),
                    K(idx_columns),
                    K(index_row));
              }
            }
          }
          // recover original data
          index_row.row_val_.cells_[pos] = orig_obj;
        }
      }
    }
  } else {
    if (OB_FAIL(write_row(relative_table, ctx, relative_table.get_rowkey_column_num(), idx_columns, index_row))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        STORAGE_LOG(WARN,
            "failed to write index row",
            K(relative_table.get_table_id()),
            K(relative_table.get_rowkey_column_num()),
            K(idx_columns),
            K(index_row));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::ObDMLRunningCtx::prepare_index_row()
{
  int ret = OB_SUCCESS;
  if (!relative_tables_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid relative tables", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "index does not exist, no need to malloc index work member");
    if (OB_FAIL(malloc_store_row(allocator_, relative_tables_.max_col_num_, idx_row_))) {
      STORAGE_LOG(WARN, "fail to malloc temp table row", K(ret));
    } else {
      idx_row_->flag_ = T_DML_DELETE == dml_type_ ? ObActionFlag::OP_DEL_ROW : ObActionFlag::OP_ROW_EXIST;
      idx_row_->set_dml(dml_type_);
    }
  }

  if (OB_FAIL(ret)) {
    idx_row_ = nullptr;
  }
  return ret;
}

int ObPartitionStorage::reshape_delete_row(
    ObDMLRunningCtx& run_ctx, RowReshape*& row_reshape, ObStoreRow& tbl_row, ObStoreRow& new_tbl_row)
{
  int ret = OB_SUCCESS;

  const ObColDescIArray& col_descs = *run_ctx.col_descs_;
  ObSQLMode sql_mode = run_ctx.dml_param_.sql_mode_;
  RowReshape* reshape_ptr = nullptr;
  bool need_reshape = false;

  if (OB_FAIL(need_reshape_table_row(tbl_row.row_val_, tbl_row.row_val_.get_count(), sql_mode, need_reshape))) {
    LOG_WARN("fail to check need reshape new", K(ret), K(tbl_row.row_val_), K(sql_mode));
  } else if (need_reshape && nullptr == row_reshape) {
    if (OB_FAIL(
            malloc_rows_reshape(run_ctx.allocator_, col_descs, 1, run_ctx.relative_tables_.data_table_, reshape_ptr))) {
      LOG_WARN("fail to malloc reshape", K(ret), K(col_descs), K(sql_mode));
    } else {
      row_reshape = reshape_ptr;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(reshape_row(
                 tbl_row.row_val_, tbl_row.row_val_.get_count(), row_reshape, new_tbl_row, need_reshape, sql_mode))) {
    LOG_WARN("fail to reshape new", K(ret), K(new_tbl_row.row_val_), K(need_reshape), K(sql_mode));
  }

  return ret;
}

int ObPartitionStorage::delete_row(ObDMLRunningCtx& run_ctx, RowReshape*& row_reshape, const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  const ObDMLBaseParam& dml_param = run_ctx.dml_param_;
  const ObStoreCtx& ctx = run_ctx.store_ctx_;
  int64_t rowkey_size = run_ctx.relative_tables_.data_table_.get_rowkey_column_num();
  ObStoreRow& tbl_row = run_ctx.tbl_row_;

  ObStoreRow new_tbl_row;
  ObStoreRow del_row;
  ObSEArray<int64_t, 64> update_idx;  // update_idx is a dummy param here
  tbl_row.flag_ = ObActionFlag::OP_DEL_ROW;
  tbl_row.set_dml(T_DML_DELETE);
  tbl_row.row_val_ = row;

  if (OB_FAIL(reshape_delete_row(run_ctx, row_reshape, tbl_row, tbl_row))) {
    LOG_WARN("failed to reshape row", K(ret), K(tbl_row));
  } else if (!dml_param.is_total_quantity_log_) {
    if (OB_FAIL(write_row(run_ctx.relative_tables_.data_table_, ctx, rowkey_size, *run_ctx.col_descs_, tbl_row))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        STORAGE_LOG(WARN, "failed to set row", K_(pkey), K(rowkey_size), K(*run_ctx.col_descs_), K(tbl_row), K(ret));
      }
    }
  } else if (dml_param.is_total_quantity_log_) {
    update_idx.reset();  // update_idx is a dummy param here
    new_tbl_row.reset();
    new_tbl_row.flag_ = ObActionFlag::OP_DEL_ROW;
    new_tbl_row.set_dml(T_DML_DELETE);
    new_tbl_row.row_val_ = tbl_row.row_val_;
    tbl_row.flag_ = ObActionFlag::OP_ROW_EXIST;
    tbl_row.set_dml(T_DML_UNKNOWN);
    if (OB_FAIL(write_row(run_ctx.relative_tables_.data_table_,
            ctx,
            rowkey_size,
            *run_ctx.col_descs_,
            update_idx,
            tbl_row,
            new_tbl_row))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        STORAGE_LOG(WARN,
            "failed to set row",
            K_(pkey),
            K(rowkey_size),
            K(*run_ctx.col_descs_),
            K(tbl_row),
            K(new_tbl_row),
            K(ret));
      }
    } else {
      STORAGE_LOG(DEBUG, "Success to del main table row, ", K(tbl_row), K(new_tbl_row));
    }
  }

  if (OB_SUCC(ret)) {
    bool null_idx_val = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < run_ctx.relative_tables_.idx_cnt_; ++i) {
      ObRelativeTable& relative_table = run_ctx.relative_tables_.index_tables_[i];
      int64_t size = relative_table.get_rowkey_column_num();
      if (OB_ISNULL(run_ctx.idx_row_) || OB_ISNULL(run_ctx.col_map_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "index row or column map is NULL", KP(run_ctx.idx_row_), KP(run_ctx.col_map_), K(ret));
      } else if (OB_FAIL(relative_table.build_index_row(tbl_row.row_val_,
                     *run_ctx.col_map_,
                     true,
                     run_ctx.idx_row_->row_val_,
                     null_idx_val,
                     &run_ctx.idx_col_descs_))) {
        STORAGE_LOG(WARN, "failed to generate index row", K(ret));
      } else if (OB_FAIL(write_index_row(relative_table, ctx, run_ctx.idx_col_descs_, *run_ctx.idx_row_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          STORAGE_LOG(WARN,
              "failed to set index row",
              "index_id",
              relative_table.get_table_id(),
              K(size),
              K(run_ctx.idx_col_descs_),
              "index_row",
              to_cstring(*run_ctx.idx_row_),
              K(ret));
        }
      } else {
        STORAGE_LOG(DEBUG, "Success to del index row, ", K(*run_ctx.idx_row_));
      }
    }
  }

  return ret;
}

int ObPartitionStorage::delete_row(
    const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids, const ObNewRow& row)

{
  int ret = OB_SUCCESS;
  RowReshape* row_reshape = nullptr;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!ctx.is_valid() || !dml_param.is_valid() || column_ids.count() <= 0 || !row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "input pointers can not be NULL", K(dml_param), K(column_ids), K(row), KP(ctx.mem_ctx_), K(ret));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_DELETE);
    if (OB_FAIL(run_ctx.init(&column_ids, NULL, true, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
#ifndef NDEBUG
    } else if (OB_FAIL(check_column_ids_valid(run_ctx.allocator_,
                   run_ctx.relative_tables_,
                   run_ctx.col_map_,
                   column_ids,
                   T_DML_DELETE,
                   column_ids,
                   NO_CHANGE,
                   run_ctx.dml_param_.is_total_quantity_log_))) {
      STORAGE_LOG(WARN, "column_ids illegal", K(ret));
#endif
    } else if (OB_FAIL(delete_row(run_ctx, row_reshape, row))) {
      STORAGE_LOG(WARN, "fail to delete row", K(pkey_), K(row), K(ret));
    } else {
      EVENT_ADD(STORAGE_DELETE_ROW_COUNT, 1);
    }
    free_row_reshape(run_ctx.allocator_, row_reshape, 1);
  }

  return ret;
}

int ObPartitionStorage::delete_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  RowReshape* row_reshape = nullptr;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  int64_t afct_num = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_ISNULL(row_iter) || !ctx.is_valid() || column_ids.count() <= 0 || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "input pointers can not be NULL", K(dml_param), K(column_ids), KP(row_iter), KP(ctx.mem_ctx_), K(ret));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_DELETE);
    ObNewRow* row = NULL;
    if (OB_FAIL(run_ctx.init(&column_ids, NULL, true, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
#ifndef NDEBUG
    } else if (OB_FAIL(check_column_ids_valid(run_ctx.allocator_,
                   run_ctx.relative_tables_,
                   run_ctx.col_map_,
                   column_ids,
                   T_DML_DELETE,
                   column_ids,
                   NO_CHANGE,
                   run_ctx.dml_param_.is_total_quantity_log_))) {
      STORAGE_LOG(WARN, "column_ids illegal", K(ret));
#endif
    }
    // delete table row and index rows
    while (OB_SUCC(ret) && OB_SUCC(row_iter->get_next_row(row))) {
      if (ObTimeUtility::current_time() > run_ctx.dml_param_.timeout_) {
        ret = OB_TIMEOUT;
        int64_t cur_time = ObTimeUtility::current_time();
        STORAGE_LOG(WARN, "query timeout", K(cur_time), K(run_ctx.dml_param_), K(ret));
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get next row from iterator failed", KP(row), K(ret));
      } else if (OB_FAIL(delete_row(run_ctx, row_reshape, *row))) {
        STORAGE_LOG(WARN, "fail to delete row", K(pkey_), K(row), K(ret));
      } else {
        ++afct_num;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      affected_rows = afct_num;
      EVENT_ADD(STORAGE_DELETE_ROW_COUNT, afct_num);
    }
    free_row_reshape(run_ctx.allocator_, row_reshape, 1);
  }

  return ret;
}

int ObPartitionStorage::malloc_rows_reshape(common::ObIAllocator& work_allocator, const ObColDescIArray& col_descs,
    const int64_t row_count, const ObRelativeTable& table, RowReshape*& row_reshape_ins)
{
  int ret = OB_SUCCESS;
  int64_t binary_buffer_len = 0;
  ObSEArray<std::pair<int32_t, int32_t>, common::OB_ROW_MAX_COLUMNS_COUNT> binary_len_array;

  if (col_descs.count() <= 0 || nullptr != row_reshape_ins || !table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(col_descs), K(row_reshape_ins), K(table));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
      const share::schema::ObColDesc& col_desc = col_descs.at(i);
      if (col_desc.col_type_.is_binary()) {
        int32_t data_length = 0;
        if (OB_FAIL(table.get_column_data_length(col_desc.col_id_, data_length))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "get column data length fail", K(ret), K(col_desc));
        } else if (OB_FAIL(binary_len_array.push_back(std::make_pair(i, data_length)))) {
          STORAGE_LOG(WARN, "fail to push element into row_reshape_ins", K(ret), K(i), K(data_length));
        } else {
          if (OB_UNLIKELY(data_length < 0)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "wrong data length", K(col_desc.col_id_), K(data_length), K(ret));
          } else {
            binary_buffer_len += data_length;
          }
          STORAGE_LOG(INFO, "get binary len", K(data_length), K(col_desc), K(i));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    void* ptr = NULL;
    int64_t num = col_descs.count();
    int64_t reshape_size = sizeof(RowReshape);
    int64_t cell_size = sizeof(common::ObObj) * num;
    int64_t total_size = reshape_size + cell_size + binary_buffer_len;
    if (NULL == (ptr = work_allocator.alloc(row_count * total_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "failed to malloc temp row cells", K(ret));
    } else {
      row_reshape_ins = new (ptr) RowReshape[row_count];
      int64_t reshape_sizes = row_count * reshape_size;
      int64_t remain_size = cell_size + binary_buffer_len;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        int64_t offset = reshape_sizes + i * remain_size;
        void* cell_ptr = static_cast<char*>(ptr) + offset;
        row_reshape_ins[i].row_reshape_cells_ = new (cell_ptr) ObObj[num]();
        row_reshape_ins[i].binary_buffer_len_ = binary_buffer_len;
        if (binary_buffer_len > 0) {
          row_reshape_ins[i].binary_buffer_ptr_ = static_cast<char*>(cell_ptr) + cell_size;
        }
        if (binary_len_array.count() > 0) {
          if (OB_FAIL(row_reshape_ins[i].binary_len_array_.assign(binary_len_array))) {
            STORAGE_LOG(WARN, "failed to assign binary_len_array", K(ret), K(pkey_));
          }
        }
        STORAGE_LOG(INFO, "binary_len_array", K(binary_len_array), K(col_descs));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::malloc_rows_reshape_if_need(ObIAllocator& work_allocator, const ObColDescIArray& col_descs,
    const int64_t row_count, const ObRelativeTable& table, const ObSQLMode sql_mode, RowReshape*& row_reshape_ins)
{
  int ret = OB_SUCCESS;
  bool char_binary_exists = false;
  bool has_empty_string = false;
  bool char_only = true;
  int64_t binary_buffer_len = 0;
  ObSEArray<std::pair<int32_t, int32_t>, common::OB_ROW_MAX_COLUMNS_COUNT> binary_len_array;

  if (col_descs.count() <= 0 || NULL != row_reshape_ins || !table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(col_descs), K(row_reshape_ins), K(table));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
    const share::schema::ObColDesc& col_desc = col_descs.at(i);
    if (col_desc.col_type_.is_char() || col_desc.col_type_.is_binary()) {
      char_binary_exists = true;
      if (col_desc.col_type_.is_binary()) {
        char_only = false;
        int32_t data_length = 0;
        if (OB_FAIL(table.get_column_data_length(col_desc.col_id_, data_length))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "get column data length fail", K(ret), K(col_desc));
        } else if (OB_FAIL(binary_len_array.push_back(std::make_pair(i, data_length)))) {
          STORAGE_LOG(WARN, "fail to push element into row_reshape_ins", K(ret), K(i), K(data_length));
        } else {
          if (OB_UNLIKELY(data_length < 0)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "wrong data length", K(col_desc.col_id_), K(data_length), K(ret));
          } else {
            binary_buffer_len += data_length;
          }
        }
      }
    } else if (is_oracle_compatible(sql_mode) && col_desc.col_type_.is_character_type()) {
      has_empty_string = true;
      // STORAGE_LOG(WARN, "Pstr2", K(col_desc), K(lbt()), K(has_empty_string));
    }
  }
  if (OB_SUCCESS == ret && (char_binary_exists || has_empty_string)) {
    void* ptr = NULL;
    int64_t num = col_descs.count();
    int64_t reshape_size = sizeof(RowReshape);
    int64_t cell_size = sizeof(common::ObObj) * num;
    int64_t total_size = reshape_size + cell_size + binary_buffer_len;
    if (NULL == (ptr = work_allocator.alloc(row_count * total_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "failed to malloc temp row cells", K(ret));
    } else {
      row_reshape_ins = new (ptr) RowReshape[row_count];
      int64_t reshape_sizes = row_count * reshape_size;
      int64_t remain_size = cell_size + binary_buffer_len;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        int64_t offset = reshape_sizes + i * remain_size;
        row_reshape_ins[i].char_only_ = char_only;
        void* cell_ptr = static_cast<char*>(ptr) + offset;
        row_reshape_ins[i].row_reshape_cells_ = new (cell_ptr) ObObj[num]();
        row_reshape_ins[i].binary_buffer_len_ = binary_buffer_len;
        if (binary_buffer_len > 0) {
          row_reshape_ins[i].binary_buffer_ptr_ = static_cast<char*>(cell_ptr) + cell_size;
        }
        if (binary_len_array.count() > 0) {
          if (OB_FAIL(row_reshape_ins[i].binary_len_array_.assign(binary_len_array))) {
            STORAGE_LOG(WARN, "failed to assign binary_len_array", K(ret), K(pkey_));
          }
        }
      }
    }
  }
  return ret;
}

void ObPartitionStorage::free_row_reshape(ObIAllocator& work_allocator, RowReshape*& row_reshape_ins, int64_t row_count)
{
  if (NULL != row_reshape_ins) {
    for (int64_t i = 0; i < row_count; i++) {
      row_reshape_ins[i].~RowReshape();
    }
    work_allocator.free(row_reshape_ins);
    row_reshape_ins = NULL;
  }
}

int ObPartitionStorage::need_reshape_table_row(const ObNewRow& row, RowReshape* row_reshape_ins,
    int64_t row_reshape_cells_count, ObSQLMode sql_mode, bool& need_reshape) const
{
  int ret = OB_SUCCESS;
  need_reshape = false;
  if (!row.is_valid() || row_reshape_cells_count != row.get_count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(row), K(row.count_), K(row_reshape_cells_count), K(ret));
  } else {
    if (NULL == row_reshape_ins) {
      // do not need reshape
    } else if (row_reshape_ins->char_only_) {
      ObString space_pattern;
      for (int64_t i = 0; !need_reshape && i < row.get_count(); ++i) {
        const ObObj& cell = row.get_cell(i);
        if (cell.is_fixed_len_char_type()) {
          space_pattern = ObCharsetUtils::get_const_str(cell.get_collation_type(), ' ');
        }
        if (cell.is_fixed_len_char_type() && cell.get_string_len() > 0 &&
            0 == MEMCMP(cell.get_string_ptr() + cell.get_string_len() - space_pattern.length(),
                     space_pattern.ptr(),
                     space_pattern.length())) {
          need_reshape = true;
        } else if (is_oracle_compatible(sql_mode) && cell.is_character_type() && cell.get_string_len() == 0) {
          // Oracle compatibility mode: '' as null
          need_reshape = true;
          STORAGE_LOG(DEBUG, "Pstor2", K(cell), K(cell.get_string()), K(need_reshape));
        }
      }
    } else {
      need_reshape = true;  // with binary, we do not check it
    }
  }
  return ret;
}

int ObPartitionStorage::need_reshape_table_row(
    const common::ObNewRow& row, const int64_t column_cnt, ObSQLMode sql_mode, bool& need_reshape) const
{
  int ret = OB_SUCCESS;
  need_reshape = false;
  if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row), K(row.count_));
  } else {
    ObString space_pattern;
    for (int64_t i = 0; !need_reshape && i < column_cnt; ++i) {
      const ObObj& cell = row.get_cell(i);
      if (cell.is_fixed_len_char_type()) {
        space_pattern = ObCharsetUtils::get_const_str(cell.get_collation_type(), ' ');
      }
      if (cell.is_fixed_len_char_type() && cell.get_string_len() > 0 &&
          0 == MEMCMP(cell.get_string_ptr() + cell.get_string_len() - space_pattern.length(),
                   space_pattern.ptr(),
                   space_pattern.length())) {
        need_reshape = true;
      } else if (is_oracle_compatible(sql_mode) && cell.is_character_type() && cell.get_string_len() == 0) {
        // Oracle compatibility mode: '' as null
        need_reshape = true;
        STORAGE_LOG(DEBUG, "Pstor2", K(cell), K(cell.get_string()), K(need_reshape));
      } else if (cell.is_binary()) {
        need_reshape = true;
      }
    }
  }
  return ret;
}

int ObPartitionStorage::reshape_row(const ObNewRow& row, const int64_t column_cnt, RowReshape* row_reshape_ins,
    ObStoreRow& tbl_row, bool need_reshape, ObSQLMode sql_mode) const
{
  int ret = OB_SUCCESS;
  if (column_cnt > row.get_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column cnt can not be larger than row column cnt", K(ret), K(column_cnt), K(row));
  } else if (!need_reshape) {
    tbl_row.row_val_ = row;
  } else {
    int64_t binary_len_array_count = row_reshape_ins->binary_len_array_.count();
    ObDataBuffer data_buffer(row_reshape_ins->binary_buffer_ptr_, row_reshape_ins->binary_buffer_len_);
    for (int64_t i = 0, j = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      const ObObj& cell = row.get_cell(i);
      if (cell.is_binary()) {
        for (; j < binary_len_array_count; j++) {
          if (row_reshape_ins->binary_len_array_.at(j).first == i) {
            break;
          }
        }
        if (OB_UNLIKELY(binary_len_array_count <= j)) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid argument", K(binary_len_array_count), K(ret));
        } else {
          const char* str = cell.get_string_ptr();
          int32_t len = cell.get_string_len();
          char* dest_str = NULL;
          int32_t binary_len = row_reshape_ins->binary_len_array_.at(j++).second;
          if (binary_len > len) {
            if (NULL == (dest_str = (char*)(data_buffer.alloc(binary_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              STORAGE_LOG(ERROR, "fail to alloc mem to binary", K(ret), K(i), K(j), K(binary_len));
            } else {
              char pad_char = '\0';
              MEMCPY(dest_str, str, len);
              MEMSET(dest_str + len, pad_char, binary_len - len);
            }
          } else if (binary_len == len) {
            dest_str = const_cast<char*>(str);
          } else if (binary_len < len) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "binary_len should be greater than len", K(ret), K(binary_len), K(len));
          }
          if (OB_SUCC(ret)) {
            // set_binary set both type_ and cs_type
            row_reshape_ins->row_reshape_cells_[i].set_binary(ObString(binary_len, dest_str));
          }
        }
      } else if (is_oracle_compatible(sql_mode) && cell.is_character_type() && cell.get_string_len() == 0) {
        // Oracle compatibility mode: '' as null
        LOG_DEBUG("reshape empty string to null", K(cell));
        row_reshape_ins->row_reshape_cells_[i].set_null();
      } else if (cell.is_fixed_len_char_type()) {
        const char* str = cell.get_string_ptr();
        int32_t len = cell.get_string_len();
        ObString space_pattern = ObCharsetUtils::get_const_str(cell.get_collation_type(), ' ');
        for (; len > 0; len -= space_pattern.length()) {
          if (0 != MEMCMP(str + len - space_pattern.length(), space_pattern.ptr(), space_pattern.length())) {
            break;
          }
        }
        // need to set collation type
        row_reshape_ins->row_reshape_cells_[i].set_string(cell.get_type(), ObString(len, str));
        row_reshape_ins->row_reshape_cells_[i].set_collation_type(cell.get_collation_type());
      } else {
        row_reshape_ins->row_reshape_cells_[i] = cell;
      }
    }
    tbl_row.row_val_.cells_ = row_reshape_ins->row_reshape_cells_;
    tbl_row.row_val_.count_ = column_cnt;
  }
  return ret;
}

int ObPartitionStorage::reshape_table_rows(const ObNewRow* rows, RowReshape* row_reshape_ins,
    int64_t row_reshape_cells_count, ObStoreRow* tbl_rows, int64_t row_count, ObSQLMode sql_mode) const
{
  int ret = OB_SUCCESS;
  bool need_reshape = false;
  if (row_count <= 0) {
    ret = OB_ERR_WRONG_VALUE_COUNT_ON_ROW;
    STORAGE_LOG(WARN, "row count should be bigger than 0", K(row_count), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      if (OB_FAIL(need_reshape_table_row(rows[i], row_reshape_ins, row_reshape_cells_count, sql_mode, need_reshape))) {
        STORAGE_LOG(WARN, "fail to check whether reshape is needed");
      } else if (OB_FAIL(reshape_row(
                     rows[i], rows[i].get_count(), &row_reshape_ins[i], tbl_rows[i], need_reshape, sql_mode))) {
        STORAGE_LOG(WARN, "fail to reshape row");
      }
    }
  }
  return ret;
}

int ObPartitionStorage::put_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  int64_t afct_num = 0;
  int64_t dup_num = 0;
  transaction::ObTransSnapInfo snap_info;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!ctx.is_valid() || !dml_param.is_valid() || column_ids.count() <= 0 || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ctx.mem_ctx_), K(dml_param), K(column_ids), KP(row_iter), K(ret));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_UPDATE);
    ObIAllocator& work_allocator = run_ctx.allocator_;
    ObNewRow* row = NULL;
    ObStoreRow& tbl_row = run_ctx.tbl_row_;
    RowReshape* row_reshape_ins = NULL;

    if (OB_FAIL(run_ctx.init(&column_ids, NULL, false, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
    } else if (!run_ctx.relative_tables_.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "data table schema must not null", K(ret));
    } else if (OB_FAIL(malloc_rows_reshape_if_need(work_allocator,
                   *run_ctx.col_descs_,
                   1,
                   run_ctx.relative_tables_.data_table_,
                   dml_param.sql_mode_,
                   row_reshape_ins))) {
      STORAGE_LOG(WARN, "get row reshape instance failed", K(ret));
#ifndef NDEBUG
    } else if (OB_FAIL(check_column_ids_valid(work_allocator,
                   run_ctx.relative_tables_,
                   run_ctx.col_map_,
                   column_ids,
                   T_DML_UPDATE,
                   column_ids,
                   NO_CHANGE,
                   dml_param.is_total_quantity_log_))) {
      STORAGE_LOG(WARN, "column_ids illegal", K(ret));
#endif
    } else {
      tbl_row.flag_ = ObActionFlag::OP_ROW_EXIST;
      tbl_row.dml_ = T_DML_UPDATE;
    }

    while (OB_SUCC(ret) && OB_SUCC(row_iter->get_next_row(row))) {
      if (ObTimeUtility::current_time() > dml_param.timeout_) {
        ret = OB_TIMEOUT;
        int64_t cur_time = ObTimeUtility::current_time();
        STORAGE_LOG(WARN, "query timeout", K(cur_time), K(dml_param), K(ret));
      } else if (OB_FAIL(reshape_table_rows(
                     row, row_reshape_ins, run_ctx.col_descs_->count(), &tbl_row, 1, dml_param.sql_mode_))) {
        STORAGE_LOG(WARN, "fail to get reshape row", K(ret), K(run_ctx.col_descs_->count()));
      } else if (OB_FAIL(direct_insert_row_and_index(run_ctx, tbl_row))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          STORAGE_LOG(WARN, "failed to write row", K(ret));
        }
      }
      ++afct_num;
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    free_row_reshape(work_allocator, row_reshape_ins, 1);

    if (OB_SUCC(ret)) {
      affected_rows = afct_num;
      EVENT_ADD(STORAGE_INSERT_ROW_COUNT, afct_num);
    }
  }

  return ret;
}

int ObPartitionStorage::insert_rows_(ObDMLRunningCtx& run_ctx, const ObNewRow* const rows, const int64_t row_count,
    ObRowsInfo& rows_info, ObStoreRow* tbl_rows, RowReshape* row_reshape_ins, int64_t& afct_num, int64_t& dup_num)
{
  int ret = OB_SUCCESS;

  const ObStoreCtx& ctx = run_ctx.store_ctx_;
  const ObDMLBaseParam& dml_param = run_ctx.dml_param_;
  const bool is_ignore = dml_param.is_ignore_;
  const ObRelativeTable& data_table = run_ctx.relative_tables_.data_table_;

  if (is_ignore) {
    if (OB_UNLIKELY(row_count > 1)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "ignore not support batch insert", K(row_count), K(ret));
    } else {
      ctx.mem_ctx_->sub_stmt_begin();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ObTimeUtility::current_time() > dml_param.timeout_) {
    ret = OB_TIMEOUT;
    int64_t cur_time = ObTimeUtility::current_time();
    STORAGE_LOG(WARN, "query timeout", K(cur_time), K(dml_param), K(ret));
  } else if (OB_FAIL(reshape_table_rows(
                 rows, row_reshape_ins, run_ctx.col_descs_->count(), tbl_rows, row_count, dml_param.sql_mode_))) {
    STORAGE_LOG(WARN, "fail to get reshape rows", K(ret));
  } else if (OB_FAIL(rows_info.check_duplicate(tbl_rows, row_count, data_table))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      char rowkey_buffer[OB_TMP_BUF_SIZE_256];
      if (OB_SUCCESS != extract_rowkey(data_table,
                            rows_info.get_duplicate_rowkey(),
                            rowkey_buffer,
                            OB_TMP_BUF_SIZE_256,
                            run_ctx.dml_param_.tz_info_)) {
        STORAGE_LOG(WARN, "extract rowkey failed");
      } else {
        ObString index_name = "PRIMARY";
        if (data_table.is_index_table()) {
          data_table.get_index_name(index_name);
        }
        LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, index_name.length(), index_name.ptr());
      }
    } else {
      STORAGE_LOG(WARN, "fail to check duplicate", K(ret));
    }
  } else if (OB_FAIL(
                 insert_table_rows(run_ctx, run_ctx.relative_tables_.data_table_, *run_ctx.col_descs_, rows_info))) {
    STORAGE_LOG(WARN, "failed to insert table rows", K(ret));
  } else if (OB_FAIL(insert_index_rows(run_ctx, tbl_rows, row_count))) {
    STORAGE_LOG(WARN, "failed to insert index rows", K(ret));
  } else {
    afct_num = afct_num + row_count;
  }

  if (is_ignore && OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
    ctx.mem_ctx_->sub_stmt_end(false);  // abort row
    ++dup_num;
    ret = OB_SUCCESS;
  }

  return ret;
}

static inline int get_next_rows(ObNewRowIterator* row_iter, bool enable_bulk, ObNewRow*& rows, int64_t& row_count)
{
  row_count = 1;
  return enable_bulk ? row_iter->get_next_rows(rows, row_count) : row_iter->get_next_row(rows);
}

int ObPartitionStorage::insert_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  int64_t afct_num = 0;
  int64_t dup_num = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!ctx.is_valid() || !dml_param.is_valid() || column_ids.count() <= 0 || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ctx.mem_ctx_), K(dml_param), K(column_ids), KP(row_iter), K(ret));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_INSERT);
    ObIAllocator& work_allocator = run_ctx.allocator_;
    void* ptr = NULL;
    ObStoreRow* tbl_rows = NULL;
    RowReshape* row_reshape_ins = NULL;

    if (OB_FAIL(run_ctx.init(&column_ids, NULL, true, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
#ifndef NDEBUG
    } else if (OB_FAIL(check_column_ids_valid(work_allocator,
                   run_ctx.relative_tables_,
                   run_ctx.col_map_,
                   column_ids,
                   T_DML_INSERT,
                   column_ids,
                   NO_CHANGE,
                   dml_param.is_total_quantity_log_))) {
      STORAGE_LOG(WARN, "column_ids illegal", K(ret));
#endif
    }

    const bool is_ignore = dml_param.is_ignore_;
    int64_t row_count = 0;
    // index of row that exists
    int64_t row_count_first_bulk = 0;
    bool first_bulk = true;
    ObNewRow* rows = NULL;
    ObRowsInfo rows_info;
    const ObRelativeTable& data_table = run_ctx.relative_tables_.data_table_;

    DEBUG_SYNC(BEFORE_INSERT_ROWS);

    while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, !is_ignore, rows, row_count))) {
      if (row_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "row_count should be greater than 0");
      } else if (first_bulk) {
        first_bulk = false;
        row_count_first_bulk = row_count;

        if (OB_FAIL(rows_info.init(data_table, ctx, *run_ctx.col_descs_))) {
          STORAGE_LOG(WARN, "Failed to init rows info", K(ret), K(data_table));
        } else if (OB_ISNULL(ptr = work_allocator.alloc(row_count * sizeof(ObStoreRow)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(ERROR, "fail to allocate memory", K(row_count), K(ret));
        } else if (OB_FAIL(malloc_rows_reshape_if_need(work_allocator,
                       *run_ctx.col_descs_,
                       row_count,
                       run_ctx.relative_tables_.data_table_,
                       dml_param.sql_mode_,
                       row_reshape_ins))) {
          STORAGE_LOG(WARN, "get row reshape instance failed", K(ret));
        } else {
          tbl_rows = new (ptr) ObStoreRow[row_count];
          for (int64_t i = 0; i < row_count; i++) {
            tbl_rows[i].flag_ = ObActionFlag::OP_ROW_EXIST;
            tbl_rows[i].set_dml(T_DML_INSERT);
          }
        }
      }

      if (OB_SUCC(ret)) {
        ret = insert_rows_(run_ctx, rows, row_count, rows_info, tbl_rows, row_reshape_ins, afct_num, dup_num);
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (nullptr != ptr) {
      work_allocator.free(ptr);
    }
    free_row_reshape(work_allocator, row_reshape_ins, row_count_first_bulk);
    if (OB_SUCC(ret)) {
      affected_rows = afct_num;
      dml_param.duplicated_rows_ = dup_num;
      EVENT_ADD(STORAGE_INSERT_ROW_COUNT, afct_num);
      const ObTableAccessStat& access_stat = rows_info.exist_helper_.table_access_context_.access_stat_;
      if (access_stat.rowkey_prefix_ > 0 && access_stat.bf_access_cnt_ > 0) {
        prefix_access_stat_.add_stat(access_stat);
      }
    }
  }
  return ret;
}

int ObPartitionStorage::insert_row(
    const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids, const ObNewRow& row)
{
  int ret = OB_SUCCESS;

  ObTimeGuard timeguard(__func__, 100 * 1000);
  int64_t afct_num = 0;
  int64_t dup_num = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!ctx.is_valid() || !dml_param.is_valid() || column_ids.count() <= 0 || !row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ctx.mem_ctx_), K(dml_param), K(column_ids), K(row), K(ret));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_INSERT);
    run_ctx.relative_tables_.set_table_param(dml_param.table_param_);
    ObIAllocator& work_allocator = run_ctx.allocator_;
    ObStoreRow& tbl_row = run_ctx.tbl_row_;
    RowReshape* row_reshape_ins = NULL;

    if (OB_FAIL(run_ctx.init(&column_ids, NULL, true, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
#ifndef NDEBUG
    } else if (OB_FAIL(check_column_ids_valid(work_allocator,
                   run_ctx.relative_tables_,
                   run_ctx.col_map_,
                   column_ids,
                   T_DML_INSERT,
                   column_ids,
                   NO_CHANGE,
                   dml_param.is_total_quantity_log_))) {
      STORAGE_LOG(WARN, "column_ids illegal", K(ret));
#endif
    }

    const bool is_ignore = dml_param.is_ignore_;
    ObRowsInfo rows_info;
    const ObRelativeTable& data_table = run_ctx.relative_tables_.data_table_;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rows_info.init(data_table, ctx, *run_ctx.col_descs_))) {
      STORAGE_LOG(WARN, "Failed to init rows info", K(ret), K(data_table));
    } else if (OB_FAIL(malloc_rows_reshape_if_need(work_allocator,
                   *run_ctx.col_descs_,
                   1,
                   run_ctx.relative_tables_.data_table_,
                   dml_param.sql_mode_,
                   row_reshape_ins))) {
      STORAGE_LOG(WARN, "get row reshape instance failed", K(ret));
    } else {
      tbl_row.flag_ = ObActionFlag::OP_ROW_EXIST;
      tbl_row.set_dml(T_DML_INSERT);
    }

    if (OB_SUCC(ret)) {
      ret = insert_rows_(run_ctx, &row, 1, rows_info, &tbl_row, row_reshape_ins, afct_num, dup_num);
    }

    free_row_reshape(work_allocator, row_reshape_ins, 1);

    if (OB_SUCC(ret)) {
      dml_param.duplicated_rows_ = dup_num;
      EVENT_ADD(STORAGE_INSERT_ROW_COUNT, afct_num);
      const ObTableAccessStat& access_stat = rows_info.exist_helper_.table_access_context_.access_stat_;
      if (access_stat.rowkey_prefix_ > 0 && access_stat.bf_access_cnt_ > 0) {
        prefix_access_stat_.add_stat(access_stat);
      }
    }
  }

  return ret;
}

int ObPartitionStorage::insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, const ObIArray<uint64_t>& duplicated_column_ids, const ObNewRow& row,
    const ObInsertFlag flag, int64_t& affected_rows, ObNewRowIterator*& duplicated_rows)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!ctx.is_valid() || !dml_param.is_valid() || column_ids.count() <= 0 ||
             duplicated_column_ids.count() <= 0 || !row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        KP(ctx.mem_ctx_),
        K(dml_param),
        K(column_ids),
        K(duplicated_column_ids),
        K(row),
        K(flag),
        K(ret));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_INSERT);
    ObIAllocator& work_allocator = run_ctx.allocator_;
    duplicated_rows = NULL;
    ObStoreRow& tbl_row = run_ctx.tbl_row_;
    RowReshape* row_reshape_ins = NULL;
    if (OB_FAIL(run_ctx.init(&column_ids, NULL, true, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
    } else if (OB_FAIL(malloc_rows_reshape_if_need(work_allocator,
                   *run_ctx.col_descs_,
                   1,
                   run_ctx.relative_tables_.data_table_,
                   dml_param.sql_mode_,
                   row_reshape_ins))) {
      STORAGE_LOG(WARN, "get row reshape instance failed", K(ret));
#ifndef NDEBUG
    } else if (OB_FAIL(check_column_ids_valid(work_allocator,
                   run_ctx.relative_tables_,
                   run_ctx.col_map_,
                   column_ids,
                   T_DML_INSERT,
                   column_ids,
                   NO_CHANGE,
                   run_ctx.dml_param_.is_total_quantity_log_))) {
      STORAGE_LOG(WARN, "column_ids illegal", K(ret));
#endif
    } else {
      tbl_row.flag_ = ObActionFlag::OP_ROW_EXIST;
      tbl_row.set_dml(T_DML_INSERT);

      if (OB_FAIL(run_ctx.relative_tables_.data_table_.check_rowkey_in_column_ids(duplicated_column_ids, false))) {
        STORAGE_LOG(WARN, "is not rowkey columns", K(duplicated_column_ids), K(ret));
      } else if (OB_FAIL(reshape_table_rows(
                     &row, row_reshape_ins, run_ctx.col_descs_->count(), &tbl_row, 1, dml_param.sql_mode_))) {
        STORAGE_LOG(WARN, "fail to get reshape row", K(ret), K(run_ctx.col_descs_->count()));
      } else if (OB_FAIL(get_conflict_rows(run_ctx, flag, duplicated_column_ids, tbl_row.row_val_, duplicated_rows))) {
        STORAGE_LOG(WARN, "failed to get conflict row(s)", K(duplicated_column_ids), K(row), K(ret));
      } else if (NULL == duplicated_rows) {
        // insert table row and index row(s)
        if (OB_FAIL(direct_insert_row_and_index(run_ctx, tbl_row))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            STORAGE_LOG(WARN, "failed to write row", K(ret));
          }
        } else {
          affected_rows = 1;
          EVENT_INC(STORAGE_INSERT_ROW_COUNT);
        }
      } else {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
      }
    }
    free_row_reshape(work_allocator, row_reshape_ins, 1);
  }

  return ret;
}

// deprecated it()
int ObPartitionStorage::revert_insert_iter(common::ObNewRowIterator* iter)
{
  int ret = OB_SUCCESS;
  if (iter) {
    ObQueryIteratorFactory::free_insert_dup_iter(iter);
    // iter->~ObNewRowIterator();
    // ob_free(iter);
  }
  return ret;
}

/* arguments: change_type is only for update rows,
 * in other DML interface, just ignore this argument
 */
int ObPartitionStorage::check_other_columns_in_column_ids(const ObRelativeTables& relative_tables,
    const ColumnMap* column_ids_map, const common::ObIArray<uint64_t>& column_ids, const ObRowDml dml_type,
    const ChangeType change_type, const bool is_total_quantity_log)
{
  int ret = OB_SUCCESS;
  if (!relative_tables.is_valid() || OB_ISNULL(column_ids_map) || column_ids.count() <= 0 ||
      !relative_tables.data_table_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "relative tables is invalid", K(ret));
  } else if (is_total_quantity_log || T_DML_INSERT == dml_type ||
             (T_DML_UPDATE == dml_type && ROWKEY_CHANGE == change_type)) {
    if (column_ids.count() != relative_tables.data_table_.get_column_count()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN,
          "invalid column_ids",
          K(ret),
          "column_ids",
          column_ids,
          "column_ids length",
          column_ids.count(),
          "column count",
          relative_tables.data_table_.get_column_count());
    } else if (OB_FAIL(relative_tables.data_table_.check_column_in_map(*column_ids_map))) {
      // check if column_ids contains all columns
      STORAGE_LOG(WARN, "check column in map fail", K(ret), K(relative_tables.data_table_));
    }
  } else if (T_DML_UPDATE == dml_type || T_DML_DELETE == dml_type) {
    // check if column_ids contains all index columns in all related index tables
    const int64_t rowkey_len = relative_tables.data_table_.get_rowkey_column_num();
    for (int64_t i = 0; OB_SUCC(ret) && i < relative_tables.idx_cnt_; ++i) {
      if (OB_FAIL(relative_tables.index_tables_[i].check_index_column_in_map(*column_ids_map, rowkey_len))) {
        STORAGE_LOG(WARN, "check index table column in map fail", K(ret), K(relative_tables.index_tables_[i]));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid dml type", K(dml_type), K(ret));
  }
  return ret;
}

// allocate memory for col_map, remember to free the memory after using
int ObPartitionStorage::construct_column_ids_map(const common::ObIArray<uint64_t>& column_ids,
    const int64_t total_column_count, ObIAllocator& work_allocator, ColumnMap*& col_map)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (column_ids.count() <= 0 || total_column_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(column_ids), K(total_column_count), K(ret));
  } else if (NULL == (ptr = work_allocator.alloc(static_cast<int64_t>(sizeof(ColumnMap))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to allocate memory for column_ids map", K(ret));
  } else {
    col_map = new (ptr) ColumnMap(work_allocator);
    if (OB_FAIL(col_map->init(column_ids))) {
      STORAGE_LOG(WARN, "Init ObColMap failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && NULL != col_map) {
    col_map->clear();
    work_allocator.free(col_map);
    col_map = NULL;
  }
  return ret;
}

// free the memory for col_map after using
void ObPartitionStorage::deconstruct_column_ids_map(ObIAllocator& work_allocator, ColumnMap*& col_map)
{
  if (NULL != col_map) {
    col_map->clear();
    work_allocator.free(col_map);
    col_map = NULL;
  }
}

/* arguments: upd_column_ids and change_type are only for
 * update rows, in other DML interfaces, just ignore these
 * two arguments
 */
int ObPartitionStorage::check_column_ids_valid(ObIAllocator& work_allocator, const ObRelativeTables& relative_tables,
    const ColumnMap* col_map, const common::ObIArray<uint64_t>& column_ids, const ObRowDml dml_type,
    const common::ObIArray<uint64_t>& upd_column_ids, const ChangeType change_type, const bool is_total_quantity_log)
{
  int ret = OB_SUCCESS;
  ColumnMap* column_ids_map = nullptr;
  if (!relative_tables.is_valid() || (NULL != col_map && !col_map->is_inited()) || column_ids.count() <= 0 ||
      upd_column_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        KP(col_map),
        K(column_ids),
        K(dml_type),
        K(upd_column_ids),
        K(change_type),
        K(is_total_quantity_log),
        K(ret));
  } else if (nullptr != col_map) {
    column_ids_map = const_cast<ColumnMap*>(col_map);
  } else if (OB_FAIL(construct_column_ids_map(
                 column_ids, relative_tables.data_table_.get_column_count(), work_allocator, column_ids_map))) {
    STORAGE_LOG(WARN, "fail to construct column_ids map", K(ret), K(column_ids), K(column_ids_map));
  } else if (NULL == column_ids_map) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "column_ids_map should not be NULL by now", K(ret), K(column_ids_map));
  } else {
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(relative_tables.data_table_.check_rowkey_in_column_ids(column_ids, true))) {
      STORAGE_LOG(WARN, "invalid rowkey in column_ids", K(column_ids), K(ret));
    } else if (OB_FAIL(check_other_columns_in_column_ids(
                   relative_tables, column_ids_map, column_ids, dml_type, change_type, is_total_quantity_log))) {
      STORAGE_LOG(WARN, "invalid index column in column_ids", K(column_ids), K(ret));
    } else if (T_DML_UPDATE == dml_type) {
      int32_t idx = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < upd_column_ids.count(); ++i) {
        if (OB_FAIL(column_ids_map->get(upd_column_ids.at(i), idx)) || idx < 0) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN,
              "column_ids lack columns or invalid update column id",
              K(ret),
              K(idx),
              "column id",
              upd_column_ids.at(i),
              "upd_column_ids",
              upd_column_ids,
              "column_ids",
              column_ids);
        }
      }
    } else {
    }
  }
  // free the memory allocated by construct_column_ids_map()
  if (NULL == col_map && NULL != column_ids_map) {
    deconstruct_column_ids_map(work_allocator, column_ids_map);
  }
  return ret;
}

int ObPartitionStorage::fetch_conflict_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& in_column_ids, const ObIArray<uint64_t>& out_column_ids, ObNewRowIterator& check_row_iter,
    ObIArray<ObNewRowIterator*>& dup_row_iters)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_INSERT);
    ObIAllocator& work_allocator = run_ctx.allocator_;
    ObStoreRow& tbl_row = run_ctx.tbl_row_;
    RowReshape* row_reshape_ins = NULL;
    ObNewRow* check_row = NULL;
    if (OB_FAIL(run_ctx.init(&in_column_ids, NULL, false, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
    } else if (OB_FAIL(malloc_rows_reshape_if_need(work_allocator,
                   *run_ctx.col_descs_,
                   1,
                   run_ctx.relative_tables_.data_table_,
                   dml_param.sql_mode_,
                   row_reshape_ins))) {
      STORAGE_LOG(WARN, "get row reshape instance failed", K(ret));
    }
    while (OB_SUCC(ret) && OB_SUCC(check_row_iter.get_next_row(check_row))) {
      tbl_row.flag_ = ObActionFlag::OP_ROW_EXIST;
      tbl_row.set_dml(T_DML_INSERT);
      ObNewRowIterator* dup_row_iter = NULL;
      if (OB_FAIL(reshape_table_rows(
              check_row, row_reshape_ins, run_ctx.col_descs_->count(), &tbl_row, 1, dml_param.sql_mode_))) {
        STORAGE_LOG(WARN, "fail to get reshape row", K(ret), K(run_ctx.col_descs_->count()));
      } else if (OB_FAIL(get_conflict_rows(
                     run_ctx, INSERT_RETURN_ALL_DUP, out_column_ids, tbl_row.row_val_, dup_row_iter))) {
        STORAGE_LOG(WARN, "failed to get conflict row(s)", K(out_column_ids), K(tbl_row), K(ret));
      } else if (OB_FAIL(dup_row_iters.push_back(dup_row_iter))) {
        STORAGE_LOG(WARN, "store duplicate row iterator failed", K(out_column_ids), K(tbl_row), K(ret));
      }
      if (OB_FAIL(ret) && dup_row_iter != NULL) {
        ObQueryIteratorFactory::free_insert_dup_iter(dup_row_iter);
        dup_row_iter = NULL;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    free_row_reshape(work_allocator, row_reshape_ins, 1);
  }
  return ret;
}

int ObPartitionStorage::multi_get_rows(const ObStoreCtx& store_ctx, const ObTableAccessParam& access_param,
    ObTableAccessContext& access_ctx, ObRelativeTable& relative_table, const GetRowkeyArray& rowkeys,
    ObNewRowIterator*& duplicated_rows)
{
  int ret = OB_SUCCESS;
  const ObTablesHandle& tables_handle = relative_table.tables_handle_;
  ObMultipleGetMerge* get_merge = NULL;
  void* buf = NULL;

  // look up the row
  if (OB_UNLIKELY(!is_inited_) || OB_ISNULL(access_ctx.allocator_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret), KP(access_ctx.allocator_));
  } else if (NULL == (buf = access_ctx.allocator_->alloc(sizeof(ObMultipleGetMerge)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory for multi get merge ", K(ret));
  } else {
    {
      ObStorageWriterGuard guard(store_, store_ctx, false);
      if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
        STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_));
      }
    }
    get_merge = new (buf) ObMultipleGetMerge();

    ObGetTableParam get_table_param;
    ObStoreRow* row = NULL;
    get_table_param.tables_handle_ = &tables_handle;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(get_merge->init(access_param, access_ctx, get_table_param))) {
      STORAGE_LOG(WARN, "Fail to init ObSingleMerge, ", K(ret));
    } else if (OB_FAIL(get_merge->open(rowkeys))) {
      STORAGE_LOG(WARN, "Fail to open iter, ", K(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_merge->get_next_row(row))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to get next row", K(ret));
        }
      } else if (ObActionFlag::OP_ROW_EXIST == row->flag_) {
        // store the conflict rowkey
        if (NULL == duplicated_rows) {
          ObValueRowIterator* dup_iter = NULL;
          if (NULL == (dup_iter = ObQueryIteratorFactory::get_insert_dup_iter())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(ERROR, "no memory to alloc ObValueRowIterator", K(ret));
          } else {
            duplicated_rows = dup_iter;
            if (OB_FAIL(dup_iter->init(true))) {
              STORAGE_LOG(WARN, "failed to initialize ObValueRowIterator", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObValueRowIterator* dup_iter = static_cast<ObValueRowIterator*>(duplicated_rows);
          if (OB_NOT_NULL(access_param.output_exprs_)) {
            // static engine need project datum to obj row
            if (OB_ISNULL(access_param.op_)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret), KP(access_param.op_));
            }
            for (int64_t i = 0; OB_SUCC(ret) && i < access_param.output_exprs_->count(); i++) {
              ObDatum* datum = NULL;
              const sql::ObExpr* e = access_param.output_exprs_->at(i);
              if (OB_ISNULL(e)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("expr is NULL", K(ret));
              } else if (OB_FAIL(e->eval(access_param.op_->get_eval_ctx(), datum))) {
                LOG_WARN("evaluate expression failed", K(ret));
              } else if (OB_FAIL(datum->to_obj(row->row_val_.cells_[i], e->obj_meta_, e->obj_datum_map_))) {
                LOG_WARN("convert datum to obj failed", K(ret));
              }
            }
            row->row_val_.count_ = access_param.output_exprs_->count();
            LOG_DEBUG("get conflict row", K_(row->row_val));
          }
          if (OB_FAIL(dup_iter->add_row(row->row_val_))) {
            STORAGE_LOG(WARN, "failed to store conflict row", K(*row));
          } else {
            LOG_DEBUG("get conflict row", K_(row->row_val));
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  // revert iterator
  if (NULL != get_merge) {
    get_merge->~ObMultipleGetMerge();
    get_merge = NULL;
  }
  if (OB_FAIL(ret) && duplicated_rows != NULL) {
    ObQueryIteratorFactory::free_insert_dup_iter(duplicated_rows);
    duplicated_rows = NULL;
  }
  return ret;
}

int ObPartitionStorage::get_index_conflict_row(ObDMLRunningCtx& run_ctx, const ObTableAccessParam& table_access_param,
    ObTableAccessContext& table_access_ctx, ObRelativeTable& relative_table, bool need_index_back, const ObNewRow& row,
    ObNewRowIterator*& duplicated_rows)
{
  int ret = OB_SUCCESS;
  ObStoreRow* idx_row = run_ctx.idx_row_;
  ObNewRowIterator* dup_rowkey_iter = nullptr;
  const ColumnMap* col_map = run_ctx.col_map_;
  bool null_idx_val = false;
  ObColDescArray uk_out_descs;
  ObColDescArray pk_out_descs;
  ObSEArray<int32_t, 8> uk_out_idxs;
  ObStoreRowkey index_rowkey;
  ObNewRowIterator*& tmp_rowkey_iter = need_index_back ? dup_rowkey_iter : duplicated_rows;
  if (OB_FAIL(run_ctx.relative_tables_.data_table_.get_rowkey_column_ids(pk_out_descs))) {
    LOG_WARN("get rowkey column ids failed", K(ret));
  } else if (OB_FAIL(relative_table.build_index_row(
                 row, *col_map, false, idx_row->row_val_, null_idx_val, &uk_out_descs))) {
    STORAGE_LOG(WARN, "failed to generate index row", K(ret));
  } else if (OB_FAIL(get_column_index(pk_out_descs, uk_out_descs, uk_out_idxs))) {
    STORAGE_LOG(WARN,
        "failed to get output column index",
        K(ret),
        K(*run_ctx.relative_tables_.data_table_.get_schema_param()),
        K(*relative_table.get_schema_param()));
  } else {
    GetRowkeyArray rowkeys;
    ObTableAccessParam index_access_param;
    if (relative_table.is_storage_index_table()) {
      table_access_ctx.query_flag_.index_invalid_ = !relative_table.can_read_index();
    } else {
      table_access_ctx.query_flag_.index_invalid_ = false;
    }
    index_rowkey.assign(idx_row->row_val_.cells_, relative_table.get_rowkey_column_num());
    if (OB_FAIL(index_access_param.init_basic_param(relative_table.get_table_id(),
            relative_table.get_schema_version(),
            relative_table.get_rowkey_column_num(),
            uk_out_descs,
            &uk_out_idxs))) {
      LOG_WARN("init basic param failed", K(ret));
    } else if (OB_FAIL(get_conflict_row(
                   run_ctx, index_access_param, table_access_ctx, relative_table, index_rowkey, tmp_rowkey_iter))) {
      LOG_WARN("get conflict row failed", K(relative_table), K(index_rowkey), K(pk_out_descs));
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(need_index_back) && OB_UNLIKELY(tmp_rowkey_iter != NULL)) {
      OZ(convert_row_to_rowkey(*dup_rowkey_iter, rowkeys));
      OZ(multi_get_rows(run_ctx.store_ctx_,
          table_access_param,
          table_access_ctx,
          run_ctx.relative_tables_.data_table_,
          rowkeys,
          duplicated_rows));
    }
  }
  if (dup_rowkey_iter != NULL) {
    ObQueryIteratorFactory::free_insert_dup_iter(dup_rowkey_iter);
    dup_rowkey_iter = NULL;
  }
  return ret;
}

int ObPartitionStorage::get_conflict_row(ObDMLRunningCtx& run_ctx, const ObTableAccessParam& access_param,
    ObTableAccessContext& access_ctx, ObRelativeTable& relative_table, const ObStoreRowkey& rowkey,
    ObNewRowIterator*& duplicated_rows)
{
  int ret = OB_SUCCESS;
  ObExtStoreRowkey ext_rowkey(rowkey);
  GetRowkeyArray rowkeys;
  OZ(rowkeys.push_back(ext_rowkey));
  OZ(multi_get_rows(run_ctx.store_ctx_, access_param, access_ctx, relative_table, rowkeys, duplicated_rows));
  LOG_DEBUG("get conflict row", K(ret), K(rowkey), K(relative_table), K(access_param));
  return ret;
}

int ObPartitionStorage::convert_row_to_rowkey(ObNewRowIterator& rowkey_iter, GetRowkeyArray& rowkeys)
{
  int ret = OB_SUCCESS;
  ObNewRow* rows = NULL;
  int64_t row_count = 0;
  do {
    if (OB_FAIL(rowkey_iter.get_next_rows(rows, row_count))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "get next row from row iterator failed", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      ObExtStoreRowkey ext_rowkey(ObStoreRowkey(rows[i].cells_, rows[i].count_));
      LOG_DEBUG("convert row to rowkey", K(ext_rowkey), K(i), K(row_count));
      if (OB_FAIL(rowkeys.push_back(ext_rowkey))) {
        LOG_WARN("store rowkey failed", K(ret), K(ext_rowkey), K(i), K(row_count));
      }
    }
  } while (OB_SUCC(ret));
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

// primary key of index is short, so get primary key of main table by iteration
// if memtable supports output of any column(not including complete primary key is ok)
// then not necessary to provide index array for merge
int ObPartitionStorage::get_column_index(
    const ObColDescIArray& tbl_col_desc, const ObColDescIArray& idx_col_desc, common::ObIArray<int32_t>& col_idx_array)
{
  int ret = OB_SUCCESS;
  if (&tbl_col_desc == &idx_col_desc) {
    for (int32_t i = 0; OB_SUCC(ret) && i < tbl_col_desc.count(); ++i) {
      if (OB_FAIL(col_idx_array.push_back(i))) {
        STORAGE_LOG(WARN, "failed to choose output column idx", K(i), K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tbl_col_desc.count(); ++i) {
      uint64_t id = tbl_col_desc.at(i).col_id_;
      bool found = false;
      for (int32_t j = 0; OB_SUCC(ret) && j < idx_col_desc.count(); ++j) {
        if (id == idx_col_desc.at(j).col_id_) {
          found = true;
          if (OB_FAIL(col_idx_array.push_back(static_cast<int16_t>(j)))) {
            STORAGE_LOG(WARN, "failed to choose output column idx", K(id), K(ret));
          }
          break;
        }
      }
      if (!found) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to choose output column idx", K(id), K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::init_dml_access_ctx(ObDMLRunningCtx& run_ctx, ObArenaAllocator& allocator,
    ObBlockCacheWorkingSet& block_cache_ws, ObTableAccessContext& table_access_ctx)
{
  int ret = OB_SUCCESS;
  common::ObQueryFlag query_flag;
  common::ObVersionRange trans_version_range;
  query_flag.read_latest_ = ObQueryFlag::OBSF_MASK_READ_LATEST;
  // TODO  trans_version_range will be passed as a parameter
  trans_version_range.snapshot_version_ = run_ctx.store_ctx_.mem_ctx_->get_read_snapshot();
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;

  if (OB_FAIL(table_access_ctx.init(
          query_flag, run_ctx.store_ctx_, allocator, allocator, block_cache_ws, trans_version_range))) {
    LOG_WARN("failed to init table access ctx", K(ret));
  } else {
    table_access_ctx.expr_ctx_ = const_cast<ObExprCtx*>(&run_ctx.dml_param_.expr_ctx_);
  }
  return ret;
}

// this function fetch these conflict rows of one row
// and get_conflict_rows promise that the rowkey in the head of the duplicated row
// so out_col_ids must make sure the rowkey in the head
int ObPartitionStorage::get_conflict_rows(ObDMLRunningCtx& run_ctx, const ObInsertFlag flag,
    const common::ObIArray<uint64_t>& out_col_ids, const common::ObNewRow& row,
    common::ObNewRowIterator*& duplicated_rows)
{
  int ret = OB_SUCCESS;
  ObRelativeTables& relative_tables = run_ctx.relative_tables_;
  ObRelativeTable& data_table = relative_tables.data_table_;
  ObColDescArray tbl_out_descs;
  ObArenaAllocator allocator(ObModIds::OB_TABLE_SCAN_ITER);
  share::schema::ObTableParam table_param(allocator);
  ObTableAccessParam table_access_param;
  ObTableAccessContext table_access_ctx;
  ObBlockCacheWorkingSet block_cache_ws;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  }
  CK(data_table.is_valid());
  OZ(block_cache_ws.init(extract_tenant_id(data_table.get_table_id())));
  OZ(ObDMLRunningCtx::prepare_column_desc(out_col_ids, data_table, tbl_out_descs));
  OZ(data_table.build_table_param(out_col_ids, table_param));
  OZ(table_access_param.init(data_table.get_table_id(),
      data_table.get_schema_version(),
      data_table.get_rowkey_column_num(),
      tbl_out_descs,
      false,
      &table_param,
      false));
  OZ(init_dml_access_ctx(run_ctx, allocator, block_cache_ws, table_access_ctx));
  if (OB_SUCC(ret)) {
    ObStoreRowkey rowkey(row.cells_, data_table.get_rowkey_column_num());
    table_access_param.virtual_column_exprs_ = &(run_ctx.dml_param_.virtual_columns_);
    table_access_param.output_exprs_ = run_ctx.dml_param_.output_exprs_;
    table_access_param.op_ = run_ctx.dml_param_.op_;
    table_access_param.op_filters_ = run_ctx.dml_param_.op_filters_;
    table_access_param.row2exprs_projector_ = run_ctx.dml_param_.row2exprs_projector_;
    OZ(get_conflict_row(run_ctx, table_access_param, table_access_ctx, data_table, rowkey, duplicated_rows));
  }
  // check conflict row(s) of index table
  if (OB_SUCC(ret) && !run_ctx.dml_param_.only_data_table_) {
    // rowkey in the head of out_col_ids
    bool need_index_back = (out_col_ids.count() != data_table.get_rowkey_column_num());
    for (int64_t i = 0;
         OB_SUCC(ret) && (INSERT_RETURN_ALL_DUP == flag || NULL == duplicated_rows) && i < relative_tables.idx_cnt_;
         ++i) {
      if (relative_tables.index_tables_[i].is_unique_index()) {
        OZ(get_index_conflict_row(run_ctx,
            table_access_param,
            table_access_ctx,
            relative_tables.index_tables_[i],
            need_index_back,
            row,
            duplicated_rows));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::extract_rowkey(const ObRelativeTable& table, const common::ObStoreRowkey& rowkey, char* buffer,
    const int64_t buffer_len, const ObTimeZoneInfo* tz_info)
{
  int ret = OB_SUCCESS;

  if (!table.is_valid() || !rowkey.is_valid() || OB_ISNULL(buffer) || buffer_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(table), K(rowkey), K(buffer), K(buffer_len), K(tz_info));
  } else {
    const int64_t rowkey_size = table.get_rowkey_column_num();
    int64_t pos = 0;
    int64_t valid_rowkey_size = 0;
    uint64_t column_id = OB_INVALID_ID;
    MEMSET(buffer, 0, buffer_len);

    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_size; i++) {
      if (OB_FAIL(table.get_rowkey_col_id_by_idx(i, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Failed to get rowkey column description", K(i), K(ret));
      } else if (column_id <= OB_MIN_SHADOW_COLUMN_ID) {
        valid_rowkey_size++;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_rowkey_size; ++i) {
      const ObObj& obj = rowkey.get_obj_ptr()[i];
      if (OB_FAIL(obj.print_plain_str_literal(buffer, buffer_len - 1, pos, tz_info))) {
        STORAGE_LOG(WARN, "fail to print_plain_str_literal", K(ret));
      } else if (i < valid_rowkey_size - 1) {
        if (OB_FAIL(databuff_printf(buffer, buffer_len - 1, pos, "-"))) {
          LOG_WARN("databuff print failed", K(ret));
        }
      }
    }
    if (buffer != nullptr) {
      buffer[pos++] = '\0';
    }
  }

  return ret;
}

int ObPartitionStorage::insert_table_row(
    ObDMLRunningCtx& run_ctx, ObRelativeTable& relative_table, const ObColDescIArray& col_descs, ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  const ObStoreCtx& ctx = run_ctx.store_ctx_;
  bool exists = false;
  ObColDescArray cols;
  if (!ctx.is_valid() || col_descs.count() <= 0 || !row.is_valid() || !relative_table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KP(ctx.mem_ctx_), K(col_descs), K(row), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < relative_table.get_rowkey_column_num(); ++i) {
      ret = cols.push_back(col_descs.at(i));
    }
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "failed to get rowkey columns");
    } else if (((!relative_table.is_storage_index_table()) || relative_table.is_unique_index()) &&
               OB_FAIL(rowkey_exists(relative_table, ctx, cols, row.row_val_, exists))) {
      STORAGE_LOG(WARN, "failed to check whether row exists", K(row), K(ret));
    } else {
      if (OB_UNLIKELY(exists)) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        STORAGE_LOG(WARN, "rowkey already exists", K(relative_table.get_table_id()), K(row), K(ret));
      } else if (OB_UNLIKELY(relative_table.is_storage_index_table())) {
        if (OB_FAIL(write_index_row(relative_table, ctx, col_descs, row))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            STORAGE_LOG(WARN, "failed to set row", K(row), K(ret));
          }
        }
      } else if (OB_FAIL(write_row(relative_table, ctx, relative_table.get_rowkey_column_num(), col_descs, row))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          STORAGE_LOG(WARN, "failed to set row", K(row), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionStorage::insert_table_rows(
    ObDMLRunningCtx& run_ctx, ObRelativeTable& relative_table, const ObColDescIArray& col_descs, ObRowsInfo& rows_info)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  const ObStoreCtx& ctx = run_ctx.store_ctx_;
  if (!ctx.is_valid() || col_descs.count() <= 0 || !relative_table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KP(ctx.mem_ctx_), K(col_descs), K(ret));
  } else {
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "failed to get rowkey columns");
    } else if (((!relative_table.is_storage_index_table()) || relative_table.is_unique_index()) &&
               OB_FAIL(rowkeys_exists(ctx, relative_table, rows_info, exists))) {
      STORAGE_LOG(WARN, "failed to check whether row exists", K(ret));
    } else {
      if (OB_UNLIKELY(exists)) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        STORAGE_LOG(WARN, "rowkey already exists", K(relative_table.get_table_id()), K(ctx), K(ret));
      }

      if (OB_UNLIKELY(relative_table.is_storage_index_table())) {
        for (int64_t i = 0; OB_SUCC(ret) && i < rows_info.row_count_; i++) {
          ObStoreRow& row = rows_info.rows_[i];
          if (OB_FAIL(write_index_row(relative_table, ctx, col_descs, row))) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
              STORAGE_LOG(WARN, "failed to set row", K(row), K(ret));
            }
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < rows_info.row_count_; i++) {
          ObStoreRow& row = rows_info.rows_[i];
          if (OB_FAIL(write_row(relative_table, ctx, relative_table.get_rowkey_column_num(), col_descs, row))) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
              STORAGE_LOG(WARN, "failed to set row", K(row), K(ret));
            }
          }
        }
      }
    }
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret && !run_ctx.dml_param_.is_ignore_) {
      char rowkey_buffer[OB_TMP_BUF_SIZE_256];
      if (OB_SUCCESS != extract_rowkey(relative_table,
                            rows_info.get_duplicate_rowkey(),
                            rowkey_buffer,
                            OB_TMP_BUF_SIZE_256,
                            run_ctx.dml_param_.tz_info_)) {
        STORAGE_LOG(WARN, "extract rowkey failed");
      }
      ObString index_name = "PRIMARY";
      if (relative_table.is_index_table()) {
        if (OB_SUCCESS != relative_table.get_index_name(index_name)) {
          STORAGE_LOG(WARN, "get_index_name failed");
        }
      } else if (share::is_oracle_mode() && OB_SUCCESS != relative_table.get_primary_key_name(index_name)) {
        STORAGE_LOG(WARN, "get_pk_name failed");
      }
      LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, index_name.length(), index_name.ptr());
    }
  }
  return ret;
}

int ObPartitionStorage::insert_index_rows(ObDMLRunningCtx& run_ctx, ObStoreRow* rows, int64_t row_count)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(DELAY_INDEX_WRITE);
  ObString index_name;
  // insert index row(s)
  for (int64_t k = 0; OB_SUCC(ret) && k < row_count; k++) {
    ObStoreRow& tbl_row = rows[k];
    bool null_idx_val = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < run_ctx.relative_tables_.idx_cnt_; ++i) {
      ObRelativeTable& table = run_ctx.relative_tables_.index_tables_[i];
      if (OB_FAIL(table.build_index_row(tbl_row.row_val_,
              *run_ctx.col_map_,
              false,
              run_ctx.idx_row_->row_val_,
              null_idx_val,
              &run_ctx.idx_col_descs_))) {
        STORAGE_LOG(WARN, "failed to generate index row", K(ret));
      } else if (OB_FAIL(insert_table_row(run_ctx, table, run_ctx.idx_col_descs_, *run_ctx.idx_row_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          STORAGE_LOG(WARN, "failed to set row", K(ret), "index_row", to_cstring(*run_ctx.idx_row_));
        }
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret && !run_ctx.dml_param_.is_ignore_) {
          ObStoreRowkey rowkey(run_ctx.idx_row_->row_val_.cells_, table.get_rowkey_column_num());
          char rowkey_buffer[OB_TMP_BUF_SIZE_256];
          if (OB_SUCCESS !=
              extract_rowkey(table, rowkey, rowkey_buffer, OB_TMP_BUF_SIZE_256, run_ctx.dml_param_.tz_info_)) {
            STORAGE_LOG(WARN, "extract rowkey failed", K(rowkey));
          } else if (OB_SUCCESS == table.get_index_name(index_name)) {
            LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, index_name.length(), to_cstring(index_name));
          } else {
            LOG_USER_ERROR(
                OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, static_cast<int>(sizeof("PRIMARY") - 1), "PRIMARY");
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionStorage::direct_insert_row_and_index(ObDMLRunningCtx& run_ctx, const ObStoreRow& tbl_row)
{
  int ret = OB_SUCCESS;
  const ObStoreCtx& store_ctx = run_ctx.store_ctx_;
  ObRelativeTables& relative_tables = run_ctx.relative_tables_;
  const ObColDescIArray& idx_col_descs = run_ctx.idx_col_descs_;
  const ColumnMap* col_map = run_ctx.col_map_;
  ObStoreRow* idx_row = run_ctx.idx_row_;
  if (!store_ctx.is_valid() || !relative_tables.is_valid() || nullptr == run_ctx.col_descs_ ||
      run_ctx.col_descs_->count() <= 0 || !tbl_row.is_valid() ||
      (relative_tables.idx_cnt_ > 0 && (NULL == col_map || NULL == idx_row))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        KP(store_ctx.mem_ctx_),
        K(relative_tables.idx_cnt_),
        KP(run_ctx.col_descs_),
        K(tbl_row),
        KP(col_map),
        KP(idx_row),
        K(ret));
  } else {
    const ObColDescIArray& col_descs = *run_ctx.col_descs_;
    if (OB_FAIL(write_row(relative_tables.data_table_,
            store_ctx,
            relative_tables.data_table_.get_rowkey_column_num(),
            col_descs,
            tbl_row))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        STORAGE_LOG(WARN,
            "failed to write table row",
            K(relative_tables.data_table_.get_table_id()),
            K(relative_tables.data_table_.get_rowkey_column_num()),
            K(col_descs),
            K(tbl_row),
            K(ret));
      }
    }
    bool null_idx_val = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < relative_tables.idx_cnt_; ++i) {
      if (OB_FAIL(relative_tables.index_tables_[i].build_index_row(
              tbl_row.row_val_, *col_map, false, idx_row->row_val_, null_idx_val, &run_ctx.idx_col_descs_))) {
        STORAGE_LOG(WARN, "failed to generate index row", K(ret));
      } else if (OB_FAIL(write_index_row(relative_tables.index_tables_[i], store_ctx, idx_col_descs, *idx_row))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          STORAGE_LOG(WARN,
              "failed to write index row",
              K(i),
              K(relative_tables.index_tables_[i].get_table_id()),
              K(relative_tables.index_tables_[i].get_rowkey_column_num()),
              K(idx_col_descs),
              K(*idx_row));
        }
      }
    }
  }

  return ret;
}
/* this func is an encapsulation of ObNewRowIterator->get_next_row.
 * 1. need_copy_cells is true, perform a cells copy, but not a deep copy.
 *    memory for store_row.row_val.cells_ has already allocated before
 *    this func is invoked, no need to alloc memory in this func.
 * 2. need_copy_cells is false, just perform an assignment, no any copy behavior,
 */
int ObPartitionStorage::get_next_row_from_iter(
    ObNewRowIterator* row_iter, ObStoreRow& store_row, const bool need_copy_cells)
{
  int ret = OB_SUCCESS;
  ObNewRow* row = NULL;
  if (NULL == row_iter) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(row_iter), K(ret));
  } else if (OB_FAIL(row_iter->get_next_row(row))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to iterate a row", K(row), K(ret));
    }
  } else {
    if (need_copy_cells) {
      // in this situation, store_row.row_val has already hold mem for cells_,
      // no need to alloc mem here, we copy cells only.
      store_row.row_val_.count_ = row->count_;
      for (int64_t i = 0; i < row->count_; ++i) {
        store_row.row_val_.cells_[i] = row->cells_[i];
      }
    } else {
      store_row.row_val_ = *row;
    }
  }
  return ret;
}

// It is performance unfriendly modification
// In theory, it's possible that update statement doesn't modify data
// But it is not common in practice
// This is the requirement of DRC, we only check primary key of main table
// don't need to check primary key of index
int ObPartitionStorage::check_rowkey_value_change(const common::ObNewRow& old_row, const common::ObNewRow& new_row,
    const int64_t rowkey_len, bool& rowkey_change) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowkey_len <= 0) || OB_UNLIKELY(old_row.count_ < rowkey_len) ||
      OB_UNLIKELY(new_row.count_ < rowkey_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(old_row), K(new_row), K(rowkey_len), K(ret));
  } else {
    rowkey_change = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_len; ++i) {
      if (old_row.cells_[i] != new_row.cells_[i]) {
        rowkey_change = true;
        break;
      }
    }
  }
  return ret;
}

int ObPartitionStorage::update_row(ObDMLRunningCtx& run_ctx, const ObIArray<ChangeType>& changes,
    const ObIArray<int64_t>& update_idx, const bool delay_new, RowReshape*& old_row_reshape_ins,
    RowReshape*& row_reshape_ins, ObStoreRow& old_tbl_row, ObStoreRow& new_tbl_row, ObRowStore* row_store,
    bool& duplicate)
{
  int ret = OB_SUCCESS;
  const ObDMLBaseParam& dml_param = run_ctx.dml_param_;
  const ObColDescIArray& col_descs = *run_ctx.col_descs_;
  bool data_tbl_rowkey_change = false;
  int64_t data_tbl_rowkey_len = run_ctx.relative_tables_.data_table_.get_rowkey_column_num();
  bool reshape_new_row = false;
  bool reshape_old_row = false;
  RowReshape* reshape_ptr = nullptr;
  ObSQLMode sql_mode = dml_param.sql_mode_;
  duplicate = false;

  const bool is_ignore = dml_param.is_ignore_ && !share::is_oracle_mode();
  if (is_ignore) {
    run_ctx.store_ctx_.mem_ctx_->sub_stmt_begin();
  }

  if (col_descs.count() != old_tbl_row.row_val_.get_count() || col_descs.count() != new_tbl_row.row_val_.get_count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "invalid argument", K(ret), K(col_descs.count()), K(old_tbl_row.row_val_), K(new_tbl_row.row_val_));
  } else if (OB_FAIL(need_reshape_table_row(
                 new_tbl_row.row_val_, new_tbl_row.row_val_.get_count(), sql_mode, reshape_new_row))) {
    LOG_WARN("fail to check need reshape new", K(ret), K(new_tbl_row.row_val_), K(sql_mode));
  } else if (OB_FAIL(need_reshape_table_row(
                 old_tbl_row.row_val_, old_tbl_row.row_val_.get_count(), sql_mode, reshape_old_row))) {
    LOG_WARN("fail to check need reshape old", K(ret), K(old_tbl_row.row_val_), K(sql_mode));
  } else if ((nullptr == row_reshape_ins && nullptr != old_row_reshape_ins) ||
             (nullptr != row_reshape_ins && nullptr == old_row_reshape_ins)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new and old reshape mismatch", K(ret), KP(row_reshape_ins), KP(old_row_reshape_ins));
  } else if ((reshape_new_row || reshape_old_row) && (nullptr == row_reshape_ins || nullptr == old_row_reshape_ins)) {
    if (OB_FAIL(
            malloc_rows_reshape(run_ctx.allocator_, col_descs, 2, run_ctx.relative_tables_.data_table_, reshape_ptr))) {
      LOG_WARN("fail to malloc reshape", K(ret), K(col_descs), K(sql_mode));
    } else {
      row_reshape_ins = &(reshape_ptr[0]);
      old_row_reshape_ins = &(reshape_ptr[1]);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(reshape_row(new_tbl_row.row_val_,
                 new_tbl_row.row_val_.get_count(),
                 row_reshape_ins,
                 new_tbl_row,
                 reshape_new_row,
                 sql_mode))) {
    LOG_WARN("fail to reshape new", K(ret), K(new_tbl_row.row_val_), K(reshape_new_row), K(sql_mode));
  } else if (OB_FAIL(reshape_row(old_tbl_row.row_val_,
                 old_tbl_row.row_val_.get_count(),
                 old_row_reshape_ins,
                 old_tbl_row,
                 reshape_old_row,
                 sql_mode))) {
    LOG_WARN("fail to reshape new", K(ret), K(old_tbl_row.row_val_), K(reshape_old_row), K(sql_mode));
  } else if (ROWKEY_CHANGE == changes.at(0) &&
             OB_FAIL(check_rowkey_value_change(
                 old_tbl_row.row_val_, new_tbl_row.row_val_, data_tbl_rowkey_len, data_tbl_rowkey_change))) {
    STORAGE_LOG(
        WARN, "check data table rowkey change failed", K(old_tbl_row), K(new_tbl_row), K(data_tbl_rowkey_len), K(ret));
  } else if (OB_FAIL(process_old_row(run_ctx, data_tbl_rowkey_change, changes, old_tbl_row))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      STORAGE_LOG(
          WARN, "fail to process old row", K(*run_ctx.col_descs_), K(old_tbl_row), K(data_tbl_rowkey_change), K(ret));
    }
  } else if (delay_new && share::is_oracle_mode()) {
    // if total quantity log is needed, we should cache both new row and old row,
    // and the sequence is new_row1, old_row1, new_row2, old_row2....,
    // if total quantity log isn't needed, just cache new row
    if (OB_ISNULL(row_store)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "row_store is NULL", K(ret));
    } else if (OB_FAIL(row_store->add_row(new_tbl_row.row_val_))) {
      STORAGE_LOG(WARN, "failed to store new row", K(new_tbl_row), K(ret));
    } else if (OB_FAIL(row_store->add_row(old_tbl_row.row_val_))) {
      STORAGE_LOG(WARN, "failed to store old row", K(old_tbl_row), K(ret));
    }
  } else if (OB_FAIL(process_new_row(run_ctx, changes, update_idx, old_tbl_row, new_tbl_row, data_tbl_rowkey_change))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      STORAGE_LOG(WARN, "fail to process new row", K(new_tbl_row), K(ret));
    }
  }

  if (is_ignore && OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
    run_ctx.store_ctx_.mem_ctx_->sub_stmt_end(false);  // abort row
    duplicate = true;
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObPartitionStorage::update_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, const ObIArray<uint64_t>& updated_column_ids, const ObNewRow& old_row,
    const ObNewRow& new_row)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!ctx.is_valid() || !dml_param.is_valid() || column_ids.count() <= 0 || updated_column_ids.count() <= 0 ||
             !old_row.is_valid() || !new_row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ret),
        KP(ctx.mem_ctx_),
        K(dml_param),
        K(column_ids),
        K(updated_column_ids),
        K(old_row),
        K(new_row));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_UPDATE);
    ObIAllocator& work_allocator = run_ctx.allocator_;
    ObStoreRow old_tbl_row;
    ObStoreRow& new_tbl_row = run_ctx.tbl_row_;
    RowReshape* row_reshape_ins = NULL;
    RowReshape* old_row_reshape_ins = NULL;
    ObSEArray<ChangeType, OB_MAX_INDEX_PER_TABLE> changes;
    ObSEArray<int64_t, 64> update_idx;
    bool delay_new = false;  // no use
    bool duplicate = false;

    if (OB_FAIL(run_ctx.init(&column_ids, &updated_column_ids, true, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
    } else if (!run_ctx.relative_tables_.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "data table is not valid", K(ret));
    } else if (OB_FAIL(construct_update_idx(run_ctx.col_map_, updated_column_ids, update_idx))) {
      STORAGE_LOG(WARN, "failed to construct update_idx", K(updated_column_ids), K(ret));
    } else if (OB_FAIL(check_rowkey_change(updated_column_ids, run_ctx.relative_tables_, changes, delay_new))) {
      STORAGE_LOG(WARN, "failed to check rowkey changes", K(ret));
    } else if (changes.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "changes flag should contain at least one element", "changes flag count", changes.count());
#ifndef NDEBUG
    } else if (OB_FAIL(check_column_ids_valid(work_allocator,
                   run_ctx.relative_tables_,
                   run_ctx.col_map_,
                   column_ids,
                   T_DML_UPDATE,
                   updated_column_ids,
                   changes.at(0),
                   dml_param.is_total_quantity_log_))) {
      STORAGE_LOG(WARN, "column_ids illegal", K(ret));
#endif
    } else {
      old_tbl_row.row_val_ = old_row;
      old_tbl_row.flag_ = ObActionFlag::OP_ROW_EXIST;
      old_tbl_row.set_dml(T_DML_UPDATE);
      new_tbl_row.row_val_ = new_row;
      new_tbl_row.flag_ = ObActionFlag::OP_ROW_EXIST;
      new_tbl_row.set_dml(T_DML_UPDATE);
    }

    if (OB_SUCC(ret) && OB_FAIL(update_row(run_ctx,
                            changes,
                            update_idx,
                            false,  // process new row with no delay
                            old_row_reshape_ins,
                            row_reshape_ins,
                            old_tbl_row,
                            new_tbl_row,
                            NULL,
                            duplicate))) {
      STORAGE_LOG(WARN, "failed to update row", K(pkey_), K(ret), K(changes));
    }

    free_row_reshape(work_allocator, row_reshape_ins, 2);
    row_reshape_ins = nullptr;
    old_row_reshape_ins = nullptr;

    if (OB_SUCC(ret)) {
      EVENT_ADD(STORAGE_UPDATE_ROW_COUNT, 1);
    }
  }
  return ret;
}

int ObPartitionStorage::update_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, const ObIArray<uint64_t>& updated_column_ids, ObNewRowIterator* row_iter,
    int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  int64_t afct_num = 0;
  int64_t dup_num = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!ctx.is_valid() || !dml_param.is_valid() || column_ids.count() <= 0 || updated_column_ids.count() <= 0 ||
             OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ret),
        KP(ctx.mem_ctx_),
        K(dml_param),
        K(column_ids),
        K(updated_column_ids),
        KP(row_iter));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_UPDATE);
    ObIAllocator& work_allocator = run_ctx.allocator_;
    ObStoreRow old_tbl_row;
    void* old_row_cells = NULL;
    ObStoreRow& new_tbl_row = run_ctx.tbl_row_;
    RowReshape* row_reshape_ins = NULL;
    RowReshape* old_row_reshape_ins = NULL;
    ObSEArray<ChangeType, OB_MAX_INDEX_PER_TABLE> changes;
    ObSEArray<int64_t, 64> update_idx;
    ObRowStore row_store;
    bool delay_new = false;

    if (OB_FAIL(run_ctx.init(&column_ids, &updated_column_ids, true, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
    } else if (!run_ctx.relative_tables_.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "data table is not prepared", K(ret));
    } else if (OB_FAIL(construct_update_idx(run_ctx.col_map_, updated_column_ids, update_idx))) {
      STORAGE_LOG(WARN, "failed to construct update_idx", K(updated_column_ids), K(ret));
    } else if (OB_FAIL(check_rowkey_change(updated_column_ids, run_ctx.relative_tables_, changes, delay_new))) {
      STORAGE_LOG(WARN, "failed to check rowkey changes", K(ret));
    } else if (changes.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "changes flag should contain at least one element", "changes flag count", changes.count());
#ifndef NDEBUG
    } else if (OB_FAIL(check_column_ids_valid(work_allocator,
                   run_ctx.relative_tables_,
                   run_ctx.col_map_,
                   column_ids,
                   T_DML_UPDATE,
                   updated_column_ids,
                   changes.at(0),
                   dml_param.is_total_quantity_log_))) {
      STORAGE_LOG(WARN, "column_ids illegal", K(ret));
#endif
    } else {
      int64_t num = run_ctx.relative_tables_.data_table_.get_column_count();
      if (NULL == (old_row_cells = work_allocator.alloc(static_cast<int64_t>(sizeof(common::ObObj) * num)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "failed to malloc temp row cells", K(ret));
      } else {
        old_tbl_row.row_val_.cells_ = new (old_row_cells) ObObj[num]();
        old_tbl_row.flag_ = ObActionFlag::OP_ROW_EXIST;
        old_tbl_row.set_dml(T_DML_UPDATE);
        new_tbl_row.flag_ = ObActionFlag::OP_ROW_EXIST;
        new_tbl_row.set_dml(T_DML_UPDATE);
      }
    }
    int64_t got_row_count = 0;
    while (OB_SUCC(ret) && OB_SUCC(get_next_row_from_iter(row_iter, old_tbl_row, true)) &&
           OB_SUCC(get_next_row_from_iter(row_iter, new_tbl_row, false))) {
      bool duplicate = false;
      ++got_row_count;
      if ((0 == (0x1FF & got_row_count)) && (ObTimeUtility::current_time() > dml_param.timeout_)) {
        // checking timeout cost too much, so check every 512 rows
        ret = OB_TIMEOUT;
        int64_t cur_time = ObTimeUtility::current_time();
        STORAGE_LOG(WARN, "query timeout", K(cur_time), K(dml_param), K(ret));
      } else if (OB_FAIL(update_row(run_ctx,
                     changes,
                     update_idx,
                     delay_new,
                     old_row_reshape_ins,
                     row_reshape_ins,
                     old_tbl_row,
                     new_tbl_row,
                     &row_store,
                     duplicate))) {
        STORAGE_LOG(WARN, "failed to update row", K(pkey_), K(ret));
      } else if (duplicate) {
        dup_num++;
      } else {
        afct_num++;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCCESS == ret && row_store.get_row_count() > 0) {
      void* ptr1 = NULL;
      int64_t num = run_ctx.relative_tables_.data_table_.get_column_count();
      if (NULL == (ptr1 = work_allocator.alloc(sizeof(common::ObObj) * num))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "failed to malloc temp row cells", K(ret));
      } else {
        new_tbl_row.row_val_.cells_ = new (ptr1) ObObj[num]();
        ObRowStore::Iterator row_iter2 = row_store.begin();
        // when total_quantity_log is true, we should iterate new_tbl_row and old_tbl_row, and
        // dispose these two rows together, otherwise, when total_quantity_log is false,
        // row_iter2 doesn't contain old rows, and old_tbl_row is a dummy param in process_new_row
        while (OB_SUCC(ret) && OB_SUCC(row_iter2.get_next_row(new_tbl_row.row_val_))) {
          bool rowkey_change = (ROWKEY_CHANGE == changes.at(0));
          int64_t data_tbl_rowkey_len = run_ctx.relative_tables_.data_table_.get_rowkey_column_num();
          if (OB_FAIL(row_iter2.get_next_row(old_tbl_row.row_val_))) {
            STORAGE_LOG(WARN, "fail to get row from row stores", K(ret));
          } else if (rowkey_change &&
                     OB_FAIL(check_rowkey_value_change(
                         old_tbl_row.row_val_, new_tbl_row.row_val_, data_tbl_rowkey_len, rowkey_change))) {
            STORAGE_LOG(WARN,
                "check data table rowkey change failed",
                K(old_tbl_row),
                K(new_tbl_row),
                K(data_tbl_rowkey_len),
                K(ret));
          } else if (OB_FAIL(process_new_row(run_ctx, changes, update_idx, old_tbl_row, new_tbl_row, rowkey_change))) {
            STORAGE_LOG(WARN, "fail to process new row", K(old_tbl_row), K(new_tbl_row), K(ret));
          }
        }
        work_allocator.free(ptr1);
        ptr1 = NULL;
        new_tbl_row.row_val_.cells_ = NULL;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (NULL != old_row_cells) {
      work_allocator.free(old_row_cells);
    }
    free_row_reshape(work_allocator, row_reshape_ins, 2);
    row_reshape_ins = nullptr;
    old_row_reshape_ins = nullptr;
    old_tbl_row.row_val_.cells_ = NULL;
    if (OB_SUCC(ret)) {
      affected_rows = afct_num;
      dml_param.duplicated_rows_ = dup_num;
      EVENT_ADD(STORAGE_UPDATE_ROW_COUNT, afct_num);
    }
  }
  return ret;
}

int ObPartitionStorage::get_change_type(
    const ObIArray<uint64_t>& update_ids, const ObRelativeTable& table, ChangeType& change_type)
{
  int ret = OB_SUCCESS;
  if (update_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(update_ids), K(ret));
  } else {
    const int64_t count = update_ids.count();
    bool is_rowkey = false;
    change_type = NO_CHANGE;
    for (int64_t i = 0; OB_SUCC(ret) && i < count && ROWKEY_CHANGE != change_type; ++i) {
      if (OB_FAIL(table.is_rowkey_column_id(update_ids.at(i), is_rowkey))) {
        LOG_WARN("check is_rowkey fail", K(ret), K(update_ids.at(i)));
      } else {
        change_type = is_rowkey ? ROWKEY_CHANGE : OTHER_CHANGE;
      }
    }
    if (OB_SUCC(ret)) {
      if (table.is_unique_index() && ROWKEY_CHANGE != change_type) {
        uint64_t cid = OB_INVALID_ID;
        bool innullable = true;
        for (int64_t j = 0; OB_SUCC(ret) && j < table.get_rowkey_column_num(); ++j) {
          if (OB_FAIL(table.get_rowkey_col_id_by_idx(j, cid))) {
            LOG_WARN("get rowkey column id fail", K(ret), K(j));
          } else if (cid > OB_MIN_SHADOW_COLUMN_ID) {
            if (innullable) {
              break;  // other_change
            } else {
              cid -= OB_MIN_SHADOW_COLUMN_ID;
              for (int64_t k = 0; OB_SUCC(ret) && k < count; ++k) {
                if (cid == update_ids.at(k)) {
                  change_type = ND_ROWKEY_CHANGE;
                  break;
                }
              }
              if (ND_ROWKEY_CHANGE == change_type) {
                break;
              }
            }
          } else {
            bool is_nullable = false;
            if (OB_FAIL(table.is_column_nullable(cid, is_nullable))) {
              LOG_WARN("check nullable fail", K(ret), K(cid), K(table));
            } else if (is_nullable) {
              innullable = false;
            }
          }
        }
      }
    }
  }
  return ret;
}

// first is table, others are indexes
int ObPartitionStorage::check_rowkey_change(const ObIArray<uint64_t>& update_ids,
    const ObRelativeTables& relative_tables, ObIArray<ChangeType>& changes, bool& delay_new)
{
  int ret = OB_SUCCESS;
  if (update_ids.count() <= 0 || !relative_tables.is_valid() || changes.count() > 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(update_ids), K(changes.count()), K(ret));
  } else {
    changes.reset();
    delay_new = false;
    const ObRelativeTable* relative_table = NULL;
    ChangeType type;
    for (int i = 0; OB_SUCC(ret) && i <= relative_tables.idx_cnt_; ++i) {
      if (0 == i) {
        relative_table = &relative_tables.data_table_;
      } else {
        relative_table = &relative_tables.index_tables_[i - 1];
      }
      if (OB_FAIL(get_change_type(update_ids, *relative_table, type))) {
        STORAGE_LOG(WARN, "failed to get change type");
      } else {
        if ((0 == i || relative_table->is_unique_index()) && (ROWKEY_CHANGE == type || ND_ROWKEY_CHANGE == type)) {
          delay_new = true;
        }
        if (OB_FAIL(changes.push_back(type))) {
          STORAGE_LOG(WARN, "failed to check rowkey change", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionStorage::construct_update_idx(
    const ColumnMap* col_map, const common::ObIArray<uint64_t>& upd_col_ids, common::ObIArray<int64_t>& update_idx)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  if (OB_ISNULL(col_map) || upd_col_ids.count() <= 0 || update_idx.count() > 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(col_map), K(upd_col_ids), K(upd_col_ids.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < upd_col_ids.count(); ++i) {
      int32_t idx = -1;
      if (OB_SUCCESS != (err = col_map->get(upd_col_ids.at(i), idx)) || idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "column id doesn't exist", "col_id", upd_col_ids.at(i), K(err), K(ret));
      } else if (OB_FAIL(update_idx.push_back(idx))) {
        STORAGE_LOG(WARN, "fail to push idx into update_idx", K(idx), K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::process_old_row(ObDMLRunningCtx& run_ctx, const bool data_tbl_rowkey_change,
    const ObIArray<ChangeType>& change_flags, const ObStoreRow& tbl_row)
{
  int ret = OB_SUCCESS;
  const ObStoreCtx& store_ctx = run_ctx.store_ctx_;
  ObRelativeTables& relative_tables = run_ctx.relative_tables_;
  const ColumnMap* col_map = run_ctx.col_map_;
  ObColDescIArray& idx_col_descs = run_ctx.idx_col_descs_;
  ObStoreRow* idx_row = run_ctx.idx_row_;
  bool is_delete_total_quantity_log = run_ctx.dml_param_.is_total_quantity_log_;
  if (!store_ctx.is_valid() || !relative_tables.is_valid() || change_flags.count() <= 0 ||
      nullptr == run_ctx.col_descs_ || run_ctx.col_descs_->count() <= 0 || !tbl_row.is_valid() ||
      (relative_tables.idx_cnt_ > 0 && (NULL == col_map || NULL == idx_row))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        KP(store_ctx.mem_ctx_),
        K(relative_tables.idx_cnt_),
        KP(col_map),
        K(change_flags),
        KP(run_ctx.col_descs_),
        K(tbl_row),
        KP(idx_row),
        K(is_delete_total_quantity_log),
        K(ret));
  } else {
    const ObColDescIArray& col_descs = *run_ctx.col_descs_;
    uint64_t table_id = relative_tables.data_table_.get_table_id();
    int64_t rowkey_size = relative_tables.data_table_.get_rowkey_column_num();
    const bool prelock = run_ctx.dml_param_.prelock_;
    bool locked = false;

    if (OB_UNLIKELY(prelock) && OB_FAIL(check_row_locked_by_myself(relative_tables.data_table_,
                                    store_ctx,
                                    col_descs,
                                    ObStoreRowkey(tbl_row.row_val_.cells_, rowkey_size),
                                    locked))) {
      STORAGE_LOG(WARN, "fail to check row locked", K(ret), K(pkey_), K(tbl_row));
    } else if (OB_UNLIKELY(prelock) && !locked) {
      ret = OB_ERR_ROW_NOT_LOCKED;
      STORAGE_LOG(DEBUG, "row has not been locked", K(ret), K(pkey_), K(tbl_row));
    } else if (data_tbl_rowkey_change) {
      ObStoreRow del_row = tbl_row;
      del_row.set_dml(T_DML_DELETE);
      if (!is_delete_total_quantity_log) {
        if (OB_FAIL(write_row(relative_tables.data_table_, store_ctx, rowkey_size, col_descs, del_row))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            STORAGE_LOG(WARN, "failed to delete row", K(del_row), K(ret));
          }
        }
      } else {
        ObStoreRow new_tbl_row;
        new_tbl_row.flag_ = ObActionFlag::OP_DEL_ROW;
        new_tbl_row.set_dml(T_DML_DELETE);
        new_tbl_row.row_val_ = tbl_row.row_val_;
        del_row.flag_ = ObActionFlag::OP_ROW_EXIST;
        del_row.set_dml(T_DML_UNKNOWN);
        ObSEArray<int64_t, 64> update_idx;
        if (OB_FAIL(write_row(
                relative_tables.data_table_, store_ctx, rowkey_size, col_descs, update_idx, del_row, new_tbl_row))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            STORAGE_LOG(WARN, "failed to delete row", K(del_row), K(ret));
          }
        }
      }
    } else {
      // need to lock main table rows that don't need to be deleted
      if (OB_FAIL(lock_row(relative_tables.data_table_,
              store_ctx,
              col_descs,
              ObStoreRowkey(tbl_row.row_val_.cells_, rowkey_size)))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          STORAGE_LOG(WARN, "lock row failed", K(table_id), K(col_descs), K(tbl_row), K(rowkey_size));
        }
      }
    }
    if (OB_SUCCESS == ret && relative_tables.idx_cnt_ > 0) {
      bool null_idx_val = false;
      idx_row->flag_ = ObActionFlag::OP_DEL_ROW;
      idx_row->set_dml(T_DML_DELETE);
      int64_t i = 0;
      while (OB_SUCCESS == ret && i < relative_tables.idx_cnt_) {
        ChangeType type = change_flags.at(i + 1);
        bool exists = true;
        if (ROWKEY_CHANGE == type || ND_ROWKEY_CHANGE == type) {
          if (OB_FAIL(relative_tables.index_tables_[i].build_index_row(
                  tbl_row.row_val_, *col_map, true, idx_row->row_val_, null_idx_val, &idx_col_descs))) {
            STORAGE_LOG(WARN, "failed to generate index row", K(ret));
          } else if (relative_tables.index_tables_[i].can_read_index() &&
                     relative_tables.index_tables_[i].is_storage_index_table() &&
                     OB_FAIL(rowkey_exists(
                         relative_tables.index_tables_[i], store_ctx, idx_col_descs, idx_row->row_val_, exists))) {
            STORAGE_LOG(WARN, "failed to check rowkey existing", K(*idx_row), K(ret));
          } else if (!exists) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR,
                "DEBUG ATTENTION!!!! update or delete a non exist index row",
                K(ret),
                KPC(idx_row),
                K(relative_tables.data_table_.tables_handle_),
                K(relative_tables.index_tables_[i]),
                K(relative_tables.index_tables_[i].tables_handle_));
          } else {
            // STORAGE_LOG(INFO, "build index row", K(*idx_row), K(null_idx_val), K(type));
            if (ND_ROWKEY_CHANGE == type && !null_idx_val) {
              // ignore delete
            } else if (OB_FAIL(write_index_row(relative_tables.index_tables_[i], store_ctx, idx_col_descs, *idx_row))) {
              if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
                STORAGE_LOG(WARN, "failed to set index row", "index_row", to_cstring(*idx_row), K(ret));
              }
            }
          }
        }
        ++i;
      }
    }
  }
  return ret;
}

int ObPartitionStorage::process_row_of_data_table(ObDMLRunningCtx& run_ctx, const ObIArray<int64_t>& update_idx,
    const ObStoreRow& old_tbl_row, const ObStoreRow& new_tbl_row, const bool rowkey_change)
{
  int ret = OB_SUCCESS;
  const ObStoreCtx& ctx = run_ctx.store_ctx_;
  ObRelativeTables& relative_tables = run_ctx.relative_tables_;
  bool is_update_total_quantity_log = run_ctx.dml_param_.is_total_quantity_log_;
  const common::ObTimeZoneInfo* tz_info = run_ctx.dml_param_.tz_info_;
  if (!ctx.is_valid() || !relative_tables.is_valid() || nullptr == run_ctx.col_descs_ ||
      run_ctx.col_descs_->count() <= 0 || update_idx.count() <= 0 ||
      (is_update_total_quantity_log && !old_tbl_row.is_valid()) || !new_tbl_row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        KP(ctx.mem_ctx_),
        KP(run_ctx.col_descs_),
        K(update_idx),
        K(old_tbl_row),
        K(new_tbl_row),
        K(is_update_total_quantity_log),
        K(rowkey_change),
        K(ret));
  } else {
    const ObColDescIArray& col_descs = *run_ctx.col_descs_;
    bool exists = false;
    int64_t rowkey_len = relative_tables.data_table_.get_rowkey_column_num();
    int64_t table_id = relative_tables.data_table_.get_table_id();
    if (rowkey_change &&
        OB_FAIL(rowkey_exists(relative_tables.data_table_, ctx, col_descs, new_tbl_row.row_val_, exists))) {
      STORAGE_LOG(WARN, "failed to check rowkey exists", K(new_tbl_row), K(ret));
    } else if (exists) {
      char buffer[OB_TMP_BUF_SIZE_256];
      ObStoreRowkey rowkey(new_tbl_row.row_val_.cells_, relative_tables.data_table_.get_rowkey_column_num());
      ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
      if (OB_SUCCESS != extract_rowkey(relative_tables.data_table_, rowkey, buffer, OB_TMP_BUF_SIZE_256, tz_info)) {
        STORAGE_LOG(WARN, "extract rowkey failed", K(rowkey));
      } else {
        ObString index_name = "PRIMARY";
        if (relative_tables.data_table_.is_index_table()) {
          relative_tables.data_table_.get_index_name(index_name);
        }
        LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, buffer, index_name.length(), index_name.ptr());
      }
      STORAGE_LOG(WARN, "rowkey already exists", K(new_tbl_row), K(ret));
    } else {
      ObStoreRow new_row;
      new_row.flag_ = ObActionFlag::OP_ROW_EXIST;
      new_row.set_dml(rowkey_change ? T_DML_INSERT : T_DML_UPDATE);
      new_row.row_val_ = new_tbl_row.row_val_;
      if (is_update_total_quantity_log && !rowkey_change) {
        ObStoreRow old_row;
        old_row.flag_ = ObActionFlag::OP_ROW_EXIST;
        old_row.set_dml(T_DML_UPDATE);
        old_row.row_val_ = old_tbl_row.row_val_;
        if (OB_FAIL(write_row(relative_tables.data_table_, ctx, rowkey_len, col_descs, update_idx, old_row, new_row))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            STORAGE_LOG(WARN, "failed to update to row", K(table_id), K(old_row), K(new_row), K(ret));
          }
        }
      } else {
        if (OB_FAIL(write_row(relative_tables.data_table_, ctx, rowkey_len, col_descs, new_row))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            STORAGE_LOG(WARN, "failed to update to row", K(table_id), K(new_row), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionStorage::process_row_of_index_tables(
    ObDMLRunningCtx& run_ctx, const ObIArray<ChangeType>& change_flags, const ObStoreRow& new_tbl_row)
{
  // index table doesn't need full column clog, so don't need to call set function of memtable full column clog
  int ret = OB_SUCCESS;
  const ObStoreCtx& ctx = run_ctx.store_ctx_;
  ObRelativeTables& relative_tables = run_ctx.relative_tables_;
  const ColumnMap* col_map = run_ctx.col_map_;
  ObColDescIArray& idx_col_descs = run_ctx.idx_col_descs_;
  ObStoreRow* idx_row = run_ctx.idx_row_;
  const common::ObTimeZoneInfo* tz_info = run_ctx.dml_param_.tz_info_;
  if (!ctx.is_valid() || !relative_tables.is_valid() || OB_ISNULL(col_map) || change_flags.count() <= 0 ||
      !new_tbl_row.is_valid() || OB_ISNULL(idx_row)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "invalid argument", KP(ctx.mem_ctx_), K(col_map), K(change_flags), K(new_tbl_row), KP(idx_row), K(ret));
  } else {
    bool null_idx_val = false;
    int64_t i = 0;
    ObString idx_name;
    while (OB_SUCCESS == ret && i < relative_tables.idx_cnt_) {
      ObRelativeTable& relative_table = relative_tables.index_tables_[i];
      if (NO_CHANGE != change_flags.at(i + 1)) {
        idx_row->flag_ = ObActionFlag::OP_ROW_EXIST;
        if (OB_FAIL(relative_table.build_index_row(
                new_tbl_row.row_val_, *col_map, false, idx_row->row_val_, null_idx_val, &idx_col_descs))) {
          STORAGE_LOG(WARN, "failed to generate index row", K(ret));
        } else {
          if (ROWKEY_CHANGE == change_flags.at(i + 1) || (ND_ROWKEY_CHANGE == change_flags.at(i + 1) && null_idx_val)) {
            idx_row->set_dml(T_DML_INSERT);
          } else {
            idx_row->set_dml(T_DML_UPDATE);
          }
          if (T_DML_INSERT == idx_row->get_dml() && relative_table.is_unique_index()) {
            bool exists = false;
            if (OB_FAIL(rowkey_exists(relative_table, ctx, idx_col_descs, idx_row->row_val_, exists))) {
              STORAGE_LOG(WARN, "failed to check rowkey existing", K(*idx_row), K(ret));
            } else if (exists) {
              ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
              char rowkey_buffer[OB_TMP_BUF_SIZE_256];
              ObStoreRowkey rowkey(idx_row->row_val_.cells_, relative_table.get_rowkey_column_num());
              if (OB_SUCCESS != extract_rowkey(relative_table, rowkey, rowkey_buffer, OB_TMP_BUF_SIZE_256, tz_info)) {
                STORAGE_LOG(WARN, "extract rowkey failed", K(rowkey));
              }
              if (OB_SUCCESS == relative_table.get_index_name(idx_name)) {
                LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, idx_name.length(), idx_name.ptr());
              } else {
                LOG_USER_ERROR(
                    OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, static_cast<int>(sizeof("PRIMARY") - 1), "PRIMARY");
              }
              STORAGE_LOG(WARN, "index duplicate", "row", to_cstring(idx_row->row_val_));
            } else {
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(write_index_row(relative_table, ctx, idx_col_descs, *idx_row))) {
              if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
                STORAGE_LOG(WARN, "failed to set index row", K(*idx_row), K(ret));
              }
            }
          }
        }
      }
      ++i;
    }
  }
  return ret;
}

int ObPartitionStorage::process_new_row(ObDMLRunningCtx& run_ctx, const ObIArray<ChangeType>& change_flags,
    const common::ObIArray<int64_t>& update_idx, const ObStoreRow& old_tbl_row, const ObStoreRow& new_tbl_row,
    const bool rowkey_change)
{
  int ret = OB_SUCCESS;
  if (change_flags.count() <= 0 || update_idx.count() <= 0 || !new_tbl_row.is_valid() ||
      change_flags.count() != run_ctx.relative_tables_.idx_cnt_ + 1 ||
      (run_ctx.relative_tables_.idx_cnt_ > 0 && (NULL == run_ctx.col_map_ || NULL == run_ctx.idx_row_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(run_ctx.relative_tables_.idx_cnt_),
        KP(run_ctx.col_map_),
        KP(run_ctx.idx_row_),
        K(change_flags),
        K(update_idx),
        K(new_tbl_row),
        K(rowkey_change),
        K(ret));
  } else {
    // write full column clog needs to construct update_idx and pass to memtable
    if (OB_FAIL(process_row_of_data_table(run_ctx, update_idx, old_tbl_row, new_tbl_row, rowkey_change))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        STORAGE_LOG(WARN,
            "fail to process_row_of_data_table",
            K(update_idx),
            K(old_tbl_row),
            K(new_tbl_row),
            K(rowkey_change),
            K(ret));
      }
    } else if (run_ctx.relative_tables_.idx_cnt_ > 0 &&
               OB_FAIL(process_row_of_index_tables(run_ctx, change_flags, new_tbl_row))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        STORAGE_LOG(WARN, "fail to process_row_of_index_tables", K(change_flags), K(new_tbl_row), K(ret));
      }
    } else {
    }
  }
  return ret;
}

bool ObPartitionStorage::illegal_filter(const ObTableScanParam& param) const
{
  bool bret = true;
  if (!param.scan_flag_.is_index_back() && param.filters_before_index_back_.count() > 0) {
    // index filters exist only when index back query
    bret = false;
    STORAGE_LOG(INFO, "index filter(s) exist(s) only when index back query!");
  }
  return bret;
}

// weak consistency read, check schema version
// requirement: schema version of read operation should be >= schema version of data
//              always read old data with new schema
int ObPartitionStorage::check_schema_version_for_bounded_staleness_read_(const int64_t table_version_for_read,
    const int64_t data_max_schema_version, const uint64_t table_id /* table id of table, not PG */)
{
  int ret = OB_SUCCESS;
  int64_t cur_table_version = OB_INVALID_VERSION;
  int64_t tenant_schema_version = OB_INVALID_VERSION;

  if (table_version_for_read >= data_max_schema_version) {
    // read schema version is biger than max schema version of data, pass
  } else {
    // read schema version is smaller than max schema version of data, two possible cases:
    // 1. max schema version of data is max schema version of table, return schema error, asking for schema refresh
    //
    //    standalone pg is in this case
    //
    // 2. max schema version of data is max schema version of multiple table partitions
    //
    //    It is the case when pg contains multiple partitions, it can only return max schema version of all partitions
    //
    // To differentiate the above two cases, check with the help of local schema version

    const uint64_t tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
    ObSchemaGetterGuard schema_guard;
    // get schema version of this table in schema service
    if (OB_ISNULL(schema_service_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "invalid schema service", K(ret), K(schema_service_));
    } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
      STORAGE_LOG(WARN, "schema service get tenant schema guard fail", K(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema_version(table_id, cur_table_version))) {
      STORAGE_LOG(WARN, "get table schema version fail", K(ret), K(table_id));
    }

    // check whether input table version and schema version of this table in schema service same
    // if not same, refresh schema
    else if (OB_UNLIKELY(table_version_for_read != cur_table_version)) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN,
          "schema version for read mismatch",
          K(ret),
          K(table_id),
          K(table_version_for_read),
          K(cur_table_version),
          K(data_max_schema_version),
          K(pkey_));
    }
    // get max schema version of the tenant
    else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id, tenant_schema_version))) {
      STORAGE_LOG(WARN, "get tenant refreshed schema version fail", K(ret), K(tenant_id));
    } else if (tenant_schema_version >= data_max_schema_version) {
      // if max schema version of the tenant is bigger than data's schema version,
      // then schema of read operation is newer than data's
    } else {
      ret = OB_SCHEMA_NOT_UPTODATE;
      STORAGE_LOG(WARN,
          "schema is not up to date for read, need refresh",
          K(ret),
          K(table_version_for_read),
          K(cur_table_version),
          K(tenant_schema_version),
          K(data_max_schema_version),
          K(table_id),
          K(tenant_id),
          K(pkey_));
    }
  }

  STORAGE_LOG(DEBUG,
      "check schema version for bounded staleness read",
      K(ret),
      K(data_max_schema_version),
      K(table_version_for_read),
      K(cur_table_version),
      K(tenant_schema_version),
      K(table_id),
      K(pkey_));
  return ret;
}

int ObPartitionStorage::erase_stat_cache()
{
  int ret = OB_SUCCESS;
  ObSSTable* sstable = nullptr;
  ObTableHandle table_handle;
  if (OB_FAIL(ObStatManager::get_instance().erase_table_stat(pkey_))) {
    STORAGE_LOG(WARN, "failed to erase table stat", K(ret), K(pkey_));
  } else if (OB_FAIL(store_.get_last_major_sstable(pkey_.get_table_id(), table_handle))) {
    STORAGE_LOG(WARN, "failed to get_last_major_sstable", K(ret), K(pkey_));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    STORAGE_LOG(WARN, "failed to get last major sstable", K(ret), K(pkey_));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable is null", K(ret), K(pkey_));
  } else {
    const ObIArray<ObSSTableColumnMeta>& column_meta = sstable->get_meta().column_metas_;
    ObSSTableColumnMeta col_meta;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_meta.count(); ++i) {
      if (OB_FAIL(column_meta.at(i, col_meta))) {
        STORAGE_LOG(WARN, "failed to get column meta", K(ret), K(i), K(pkey_));
      } else {
        if (OB_FAIL(ObStatManager::get_instance().erase_column_stat(pkey_, col_meta.column_id_))) {
          STORAGE_LOG(WARN, "failed to erase column stat", K(ret), K(pkey_), K(col_meta));
        }
      }
    }
  }
  return ret;
}

int ObPartitionStorage::table_scan(
    ObTableScanParam& param, const int64_t data_max_schema_version, common::ObNewRowIterator*& result)
{
  int ret = OB_SUCCESS;
  ObTableScanIterator* iter = NULL;
  ObStoreCtx ctx;
  ctx.cur_pkey_ = pkey_;

  // STORAGE_LOG(DEBUG, "begin table scan", K(param));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid()) || OB_UNLIKELY(!illegal_filter(param))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret), K(param));
  } else if (param.trans_desc_->is_bounded_staleness_read() &&
             OB_FAIL(check_schema_version_for_bounded_staleness_read_(
                 param.schema_version_, data_max_schema_version, param.ref_table_id_))) {
    // check schema_version with ref_table_id, becuase schema_version of scan_param is from ref table
    STORAGE_LOG(WARN, "check schema version for bounded staleness read fail", K(ret), K(param));
  } else if (OB_FAIL(txs_->get_store_ctx(*param.trans_desc_, param.pg_key_, ctx))) {
    STORAGE_LOG(WARN, "fail to get transaction context", K_(pkey), K(param));
  } else {
    bool ctx_add = false;

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(NULL == (iter = rp_alloc(ObTableScanIterator, ObTableScanIterator::LABEL)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "no memory");
      } else if (OB_FAIL(iter->init(*txs_, ctx, param, store_))) {
        STORAGE_LOG(WARN, "Fail to init table scan iterator, ", K(ret));
      } else {
        ctx_add = true;
        if (param.for_update_) {
          if (OB_FAIL(lock_scan_rows(ctx, param, *iter))) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
              STORAGE_LOG(WARN, "lock_scan_rows failed", K(ret), K(param));
            }
          } else if (OB_FAIL(iter->rescan(param))) {
            STORAGE_LOG(WARN, "Fail to rescan iterator, ", K(ret));
          }
        }
        // save table's schema version in memtable context, for waiting transaction end in building index
        if (OB_SUCC(ret)) {
          ctx.mem_ctx_->set_table_version(param.schema_version_);
        }
      }
    }

    if (OB_SUCC(ret)) {
      result = iter;
    } else {
      if (!ctx_add) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = txs_->revert_store_ctx(*param.trans_desc_, param.pg_key_, ctx))) {
          STORAGE_LOG(WARN, "fail to revert transaction context", K(tmp_ret));
        }
      }
      if (NULL != iter) {
        revert_scan_iter(iter);
        iter = NULL;
      }
    }
  }

  return ret;
}

int ObPartitionStorage::table_scan(
    ObTableScanParam& param, const int64_t data_max_schema_version, common::ObNewIterIterator*& result)
{
  int ret = OB_SUCCESS;
  ObTableScanIterIterator* iter = NULL;
  ObStoreCtx ctx;
  ctx.cur_pkey_ = pkey_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid()) || OB_UNLIKELY(!illegal_filter(param))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret), K(param));
  } else if (param.trans_desc_->is_bounded_staleness_read() &&
             OB_FAIL(check_schema_version_for_bounded_staleness_read_(
                 param.schema_version_, data_max_schema_version, param.ref_table_id_))) {
    // check schema_version with ref_table_id, becuase schema_version of scan_param is from ref table
    STORAGE_LOG(WARN, "check schema version for bounded staleness read fail", K(ret), K(param));
    // need to get store ctx of PG, cur_key_ saves the real partition
  } else if (OB_FAIL(txs_->get_store_ctx(*param.trans_desc_, param.pg_key_, ctx))) {
    STORAGE_LOG(WARN, "fail to get transaction context", K(ret), K(param));
  } else {
    bool ctx_add = false;

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(NULL == (iter = rp_alloc(ObTableScanIterIterator, ObTableScanIterIterator::LABEL)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "no memory");
      } else if (OB_FAIL(iter->init(*txs_, ctx, param, store_))) {
        STORAGE_LOG(WARN, "Fail to init table scan iterator, ", K(ret));
      } else {
        ctx_add = true;
        if (param.for_update_) {
          if (OB_FAIL(lock_scan_rows(ctx, param, *iter))) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
              STORAGE_LOG(WARN, "lock_scan_rows failed", K(ret), K(param));
            }
          } else if (OB_FAIL(iter->rescan(param))) {
            STORAGE_LOG(WARN, "Fail to rescan iterator, ", K(ret));
          }
        }
        // save table's schema version in memtable context, for waiting transaction end in building index
        if (OB_SUCC(ret)) {
          ctx.mem_ctx_->set_table_version(param.schema_version_);
        }
      }
    }

    if (OB_SUCC(ret)) {
      result = iter;
    } else {
      if (!ctx_add) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = txs_->revert_store_ctx(*param.trans_desc_, param.pg_key_, ctx))) {
          STORAGE_LOG(WARN, "fail to revert transaction context", K(tmp_ret));
        }
      }
      if (NULL != iter) {
        iter->reset();
        rp_free(static_cast<ObTableScanIterIterator*>(iter), ObTableScanIterIterator::LABEL);
        iter = NULL;
      }
    }
  }

  return ret;
}

int ObPartitionStorage::join_mv_scan(ObTableScanParam& left_param, ObTableScanParam& right_param,
    const int64_t left_data_max_schema_version, const int64_t right_data_max_schema_version,
    ObIPartitionStorage& right_storage, common::ObNewRowIterator*& result)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(left_param, right_param, left_data_max_schema_version, right_data_max_schema_version, right_storage, result);
  return ret;
}

int ObPartitionStorage::prepare_lock_row(const ObTableScanParam& scan_param, const common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (NULL != scan_param.output_exprs_ && NULL != scan_param.op_) {
    if (row.count_ < scan_param.output_exprs_->count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("row cell count not enough", K(ret), K(row.count_), K(scan_param.output_exprs_->count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_param.output_exprs_->count(); i++) {
        sql::ObExpr* e = scan_param.output_exprs_->at(i);
        ObDatum* datum = NULL;
        if (OB_FAIL(e->eval(scan_param.op_->get_eval_ctx(), datum))) {
          LOG_WARN("expr evaluate failed", K(ret), K(*e));
        } else if (OB_FAIL(datum->to_obj(row.cells_[i], e->obj_meta_, e->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret), K(*datum));
        }
      }
    }
  }
  return ret;
}

int ObPartitionStorage::lock_scan_rows(
    const ObStoreCtx& ctx, const ObTableScanParam& scan_param, ObTableScanIterator& iter)
{
  int ret = OB_SUCCESS;
  RowReshape* row_reshape = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_ISNULL(ctx.mem_ctx_) || OB_ISNULL(scan_param.table_param_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ctx.mem_ctx_), KP(scan_param.table_param_));
  } else {
    ObNewRow* row = nullptr;
    while (OB_SUCC(ret) && OB_SUCC(iter.get_next_row(row))) {
      if (NULL != scan_param.op_) {
        if (OB_FAIL(prepare_lock_row(scan_param, *row))) {
          LOG_WARN("prepare lock row failed", K(ret));
        }
        scan_param.op_->clear_evaluated_flag();
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(lock_rows_(ctx, scan_param, *row, row_reshape))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            STORAGE_LOG(WARN, "failed to lock row", K(row), K(ret));
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
      // do nothing
    } else {
      STORAGE_LOG(WARN, "get_next_row error", K(ret));
    }
    free_row_reshape(ctx.mem_ctx_->get_query_allocator(), row_reshape, 1);
  }
  return ret;
}

int ObPartitionStorage::lock_scan_rows(
    const ObStoreCtx& ctx, const ObTableScanParam& scan_param, ObTableScanIterIterator& iter)
{
  int ret = OB_SUCCESS;
  RowReshape* row_reshape = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_ISNULL(ctx.mem_ctx_) || OB_ISNULL(scan_param.table_param_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ctx.mem_ctx_), KP(scan_param.table_param_));
  } else {
    ObNewRow* row = nullptr;
    ObTableScanNewRowIterator row_iter(iter);
    while (OB_SUCC(ret) && OB_SUCC(row_iter.get_next_row(row))) {
      if (NULL != scan_param.op_) {
        if (OB_FAIL(prepare_lock_row(scan_param, *row))) {
          LOG_WARN("prepare lock row failed", K(ret));
        }
        scan_param.op_->clear_evaluated_flag();
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(lock_rows_(ctx, scan_param, *row, row_reshape))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            STORAGE_LOG(WARN, "fail to lock row", K(ret), K(row));
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
      // do nothing
    } else {
      STORAGE_LOG(WARN, "fail to get next row", K(ret));
    }
    free_row_reshape(ctx.mem_ctx_->get_query_allocator(), row_reshape, 1);
  }
  return ret;
}

int ObPartitionStorage::revert_scan_iter(ObNewRowIterator* iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (iter) {
    switch (iter->get_type()) {
      case ObNewRowIterator::ObTableScanIterator:
        iter->reset();
        rp_free(static_cast<ObTableScanIterator*>(iter), ObTableScanIterator::LABEL);
        break;
      default:
        iter->~ObNewRowIterator();
        break;
    }
    iter = NULL;
  }
  return ret;
}

int ObPartitionStorage::lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const int64_t abs_lock_timeout, common::ObNewRowIterator* row_iter, ObLockFlag lock_flag, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  UNUSED(lock_flag);
  int64_t afct_num = 0;
  ObColDescArray col_desc;
  RowReshape* row_reshape_ins = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!ctx.is_valid() || !dml_param.is_valid() || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ctx.mem_ctx_),
        K(dml_param),
        K(abs_lock_timeout),
        K(row_iter),
        K(lock_flag),
        K(ret));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_LOCK);
    ObNewRow* row = NULL;
    if (OB_FAIL(run_ctx.init(NULL, NULL, false, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
    } else if (OB_FAIL(run_ctx.relative_tables_.data_table_.get_rowkey_column_ids(col_desc))) {
      STORAGE_LOG(WARN, "Fail to get column desc", K(ret));
    } else {
      ctx.mem_ctx_->set_abs_lock_wait_timeout(get_lock_wait_timeout(abs_lock_timeout, dml_param.timeout_));
      while (OB_SUCCESS == ret && OB_SUCC(row_iter->get_next_row(row))) {
        if (ObTimeUtility::current_time() > dml_param.timeout_) {
          ret = OB_TIMEOUT;
          int64_t cur_time = ObTimeUtility::current_time();
          STORAGE_LOG(WARN, "query timeout", K(cur_time), K(dml_param), K(ret));
        } else if (OB_FAIL(lock_row(run_ctx.relative_tables_.data_table_,
                       ctx,
                       col_desc,
                       *row,
                       dml_param.sql_mode_,
                       run_ctx.allocator_,
                       row_reshape_ins))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            STORAGE_LOG(WARN, "failed to lock row", K(*row), K(ret));
          }
        } else {
          ++afct_num;
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        affected_rows = afct_num;
      }
      free_row_reshape(run_ctx.allocator_, row_reshape_ins, 1);
    }
  }
  return ret;
}

int ObPartitionStorage::lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const int64_t abs_lock_timeout, const common::ObNewRow& row, ObLockFlag lock_flag)
{
  int ret = OB_SUCCESS;
  RowReshape* row_reshape = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_FAIL(lock_rows_(ctx, dml_param, abs_lock_timeout, row, lock_flag, row_reshape))) {
    LOG_WARN("failed to lock rows", K(ret), K(row), K(ctx));
  }
  free_row_reshape(ctx.mem_ctx_->get_query_allocator(), row_reshape, 1);
  return ret;
}

int ObPartitionStorage::lock_rows_(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const int64_t abs_lock_timeout, const common::ObNewRow& row, ObLockFlag lock_flag, RowReshape*& row_reshape)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  UNUSED(lock_flag);
  ObColDescArray col_desc;

  if (!ctx.is_valid() || !dml_param.is_valid() || row.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "invalid argument", K(ctx.mem_ctx_), K(dml_param), K(abs_lock_timeout), K(row), K(lock_flag), K(ret));
  } else {
    ObDMLRunningCtx run_ctx(ctx, dml_param, ctx.mem_ctx_->get_query_allocator(), T_DML_LOCK);

    if (OB_FAIL(run_ctx.init(NULL, NULL, false, schema_service_, store_))) {
      STORAGE_LOG(WARN, "init dml running context failed", K(ret));
    } else if (ObTimeUtility::current_time() > dml_param.timeout_) {
      ret = OB_TIMEOUT;
      int64_t cur_time = ObTimeUtility::current_time();
      STORAGE_LOG(WARN, "query timeout", K(cur_time), K(dml_param), K(ret));
    } else if (OB_FAIL(run_ctx.relative_tables_.data_table_.get_rowkey_column_ids(col_desc))) {
      STORAGE_LOG(WARN, "failed to get column desc", K(ret));
    } else {
      ctx.mem_ctx_->set_abs_lock_wait_timeout(get_lock_wait_timeout(abs_lock_timeout, dml_param.timeout_));
      if (OB_FAIL(lock_row(run_ctx.relative_tables_.data_table_,
              ctx,
              col_desc,
              row,
              dml_param.sql_mode_,
              run_ctx.allocator_,
              row_reshape))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          STORAGE_LOG(WARN, "failed to lock row", K(row), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionStorage::lock_rows_(
    const ObStoreCtx& ctx, const ObTableScanParam& scan_param, const common::ObNewRow& row, RowReshape*& row_reshape)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  bool lock_conflict = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!ctx.is_valid() || !scan_param.is_valid() || row.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ctx.mem_ctx_), K(scan_param), K(row), K(ret));
  }
  ObDMLBaseParam dml_param;
  dml_param.timeout_ = scan_param.timeout_;
  dml_param.schema_version_ = scan_param.schema_version_;
  dml_param.only_data_table_ = true;  // only need row lock on main table
  dml_param.sql_mode_ = scan_param.sql_mode_;
  while (OB_SUCC(ret)) {
    if (scan_param.for_update_wait_timeout_ > 0 &&
        ObTimeUtility::current_time() > scan_param.for_update_wait_timeout_) {
      ret = lock_conflict ? OB_ERR_EXCLUSIVE_LOCK_CONFLICT : OB_TIMEOUT;
      int64_t cur_time = ObTimeUtility::current_time();
      STORAGE_LOG(WARN, "wait timeout", K(cur_time), K_(scan_param.for_update_wait_timeout), K(ret));
    } else if (OB_SUCC(lock_rows_(ctx, dml_param, scan_param.for_update_wait_timeout_, row, LF_NONE, row_reshape))) {
      break;
    } else if (0 == scan_param.for_update_wait_timeout_ && OB_EAGAIN == ret) {
      ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    } else {
      if (OB_TIMEOUT == ret) {
        ret = lock_conflict ? OB_ERR_EXCLUSIVE_LOCK_CONFLICT : ret;
        int64_t cur_time = ObTimeUtility::current_time();
        STORAGE_LOG(WARN, "query timeout", K(cur_time), K_(scan_param.timeout), K(ret));
      } else if (OB_EAGAIN == ret) {
        lock_conflict = true;
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "lock row failed", K(row), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        STORAGE_LOG(WARN, "query interrupt, ", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::get_batch_rows(const ObTableScanParam& param, const ObBatch& batch, int64_t& logical_row_count,
    int64_t& physical_row_count, common::ObIArray<common::ObEstRowCountRecord>& est_records)
{
  int ret = OB_SUCCESS;
  ObPartitionEst batch_estimate;
  ObTablesHandle tables_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_UNLIKELY(!param.is_estimate_valid() || !batch.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(param), K(batch), K(ret));
  } else {
    if (-1 == param.frozen_version_) {
      if (OB_FAIL(store_.get_read_tables(param.index_id_, GET_BATCH_ROWS_READ_SNAPSHOT_VERSION, tables_handle))) {
        STORAGE_LOG(WARN, "failed to get read stores", K(ret), K(param));
      }
    } else {
      if (OB_FAIL(store_.get_read_frozen_tables(param.index_id_, param.frozen_version_, tables_handle))) {
        STORAGE_LOG(WARN, "failed to get read stores", K(ret), K(param));
      }
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(DEBUG, "got estimate cost tables", K(tables_handle), K(param));
    if (OB_FAIL(estimate_row_count(param, batch, tables_handle.get_tables(), batch_estimate, est_records))) {
      STORAGE_LOG(WARN, "failed to estimate_cost on stores. ", K(param), K(batch), K(ret));
    } else {
      logical_row_count = batch_estimate.logical_row_count_;
      physical_row_count = batch_estimate.physical_row_count_;
    }
  }
  return ret;
}

// TODO(): fix it for split partition later
int ObPartitionStorage::query_range_to_macros(ObIAllocator& allocator, const ObIArray<common::ObStoreRange>& ranges,
    const int64_t type, uint64_t* macros_count, const int64_t* total_task_count,
    ObIArray<common::ObStoreRange>* splitted_ranges, ObIArray<int64_t>* split_index)
{
  int ret = OB_SUCCESS;
  ObTableHandle table_handle;
  ObSSTable* last_base_table = NULL;
  STORAGE_LOG(DEBUG, "split_macros");
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (ranges.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ranges must not empty", K(ret));
  } else if (OB_FAIL(store_.get_last_major_sstable(ranges.at(0).get_table_id(), table_handle))) {
    STORAGE_LOG(WARN, "failed to get store handle", K(ret));
  } else if (OB_FAIL(table_handle.get_sstable(last_base_table))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get last base store", K(ret));
  } else if (OB_ISNULL(last_base_table)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invlaid last base store", K(ret));
  } else if (!last_base_table->is_major_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid ssstore type", K(ret), "type", last_base_table->is_major_sstable());
  } else if (OB_FAIL(last_base_table->query_range_to_macros(
                 allocator, ranges, type, macros_count, total_task_count, splitted_ranges, split_index))) {
    LOG_WARN("failed to do query range to macros", K(ret));
  }
  return ret;
}

int ObPartitionStorage::get_multi_ranges_cost(const ObIArray<ObStoreRange>& ranges, int64_t& total_size)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition storage is not initialized", K(ret));
  } else if (OB_UNLIKELY(ranges.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get multi range cost", K(ret), K(ranges));
  } else if (OB_FAIL(store_.get_read_tables(ranges.at(0).get_table_id(), ObVersionRange::MAX_VERSION, tables_handle))) {
    STORAGE_LOG(WARN, "Failed to get read tables", K(ret));
  } else {
    ObPartitionMultiRangeSpliter spliter;
    if (OB_FAIL(spliter.get_multi_range_size(tables_handle, ranges, total_size))) {
      STORAGE_LOG(WARN, "Failed to get multi ranges cost", K(ret));
    }
  }

  return ret;
}

int ObPartitionStorage::split_multi_ranges(const ObIArray<ObStoreRange>& ranges, const int64_t expected_task_count,
    ObIAllocator& allocator, ObArrayArray<ObStoreRange>& multi_range_split_array)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition storage is not initialized", K(ret));
  } else if (OB_UNLIKELY(ranges.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get multi range cost", K(ret), K(ranges));
  } else if (OB_FAIL(store_.get_read_tables(ranges.at(0).get_table_id(), ObVersionRange::MAX_VERSION, tables_handle))) {
    STORAGE_LOG(WARN, "Failed to get read tables", K(ret));
  } else {
    ObPartitionMultiRangeSpliter spliter;
    if (OB_FAIL(spliter.get_split_multi_ranges(
            tables_handle, ranges, expected_task_count, allocator, multi_range_split_array))) {
      STORAGE_LOG(WARN, "Failed to get multi ranges split array", K(ret));
    }
  }

  return ret;
}

int ObPartitionStorage::estimate_row_count(const storage::ObTableScanParam& param, const storage::ObBatch& batch,
    const ObIArray<ObITable*>& tables, ObPartitionEst& part_est,
    common::ObIArray<common::ObEstRowCountRecord>& est_records)
{
  int ret = OB_SUCCESS;
  if (!param.is_estimate_valid() || !batch.is_valid() || tables.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(param), K(batch), K(tables.count()), K(ret));
  } else {
    switch (batch.type_) {
      case ObBatch::T_GET:
        if (OB_ISNULL(batch.rowkey_)) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid batch", K(batch), K(ret));
        } else {
          ret = ObSingleMerge::estimate_row_count(param.scan_flag_, param.index_id_, *batch.rowkey_, tables, part_est);
        }
        break;
      case ObBatch::T_MULTI_GET:
        if (OB_ISNULL(batch.rowkeys_)) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid batch", K(batch), K(ret));
        } else {
          ret = ObMultipleGetMerge::estimate_row_count(
              param.scan_flag_, param.index_id_, *batch.rowkeys_, tables, part_est);
        }
        break;
      case ObBatch::T_SCAN:
        if (OB_ISNULL(batch.range_)) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid batch", K(batch), K(ret));
        } else {
          ret = ObMultipleScanMerge::estimate_row_count(
              param.scan_flag_, param.index_id_, *batch.range_, tables, part_est, est_records);
        }
        break;
      case ObBatch::T_MULTI_SCAN:
        if (OB_ISNULL(batch.ranges_)) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid batch", K(batch), K(ret));
        } else {
          ret = ObMultipleMultiScanMerge::estimate_row_count(
              param.scan_flag_, param.index_id_, *batch.ranges_, tables, part_est, est_records);
        }
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "unknown range type", K(batch.type_));
        break;
    }
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "failed to estimate merge cost", K(batch.type_), K(ret));
    }
  }
  return ret;
}

// TODO(): fix it for spilit partition later
int ObPartitionStorage::get_table_stat(const uint64_t table_id, common::ObTableStat& stat)
{
  int ret = OB_SUCCESS;
  ObTableHandle table_handle;
  ObSSTable* base_table = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_FAIL(store_.get_last_major_sstable(table_id, table_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "Fail to get base ssstore, ", K(ret));
    }
  } else if (OB_FAIL(table_handle.get_sstable(base_table))) {
    STORAGE_LOG(WARN, "failed to get_sstable", K(ret));
  } else if (OB_FAIL(base_table->get_table_stat(stat))) {
    STORAGE_LOG(WARN, "fail to get table stat. on ssstore. ", K(ret));
  }
  return ret;
}

// TODO(): merge two get_concurrent_cnt method later
// the schema_version maybe tenant level, but the function
// is only called by get_build_index_param currently, so no problem
int ObPartitionStorage::get_concurrent_cnt(uint64_t table_id, int64_t schema_version, int64_t& concurrent_cnt)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* table_schema = NULL;
  ObTableHandle handle;
  ObSSTable* sstable = NULL;
  concurrent_cnt = 0;
  const uint64_t tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
  const bool check_formal = extract_pure_id(table_id) > OB_MAX_CORE_TABLE_ID;  // to avoid circular dependency

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The partition storage has not been initialized, ", K(ret), K_(pkey));
  } else if (OB_FAIL(store_.get_last_major_sstable(table_id, handle))) {
    STORAGE_LOG(WARN, "failed to get_last_base_sstable", K(ret), K(table_id));
  } else if (OB_FAIL(handle.get_sstable(sstable))) {
    STORAGE_LOG(WARN, "failed to get sstable", K(ret), K(table_id));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "sstable must not null", K(ret), K(table_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
                 tenant_id, schema_guard, schema_version, OB_INVALID_VERSION /*sys_schema_version*/))) {
    STORAGE_LOG(WARN, "Fail to get schema, ", K(schema_version), K(ret));
  } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
    STORAGE_LOG(WARN, "schema_guard is not formal", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    STORAGE_LOG(WARN, "Fail to get main table schema, ", K(table_id), K(ret));
  } else if (NULL == table_schema) {
    ret = OB_SCHEMA_ERROR;
    STORAGE_LOG(ERROR, "Fail to get main table schema, ", K_(pkey), K(ret));
  } else if (OB_FAIL(sstable->get_concurrent_cnt(table_schema->get_tablet_size(), concurrent_cnt))) {
    STORAGE_LOG(WARN, "failed to get_concurrent_cnt", K(ret), K(table_id));
  }
  return ret;
}

int ObPartitionStorage::build_merge_ctx(storage::ObSSTableMergeCtx& ctx)
{
  int ret = OB_SUCCESS;
  ObGetMergeTablesParam get_merge_table_param;
  ObGetMergeTablesResult get_merge_table_result;
  ObVersion merge_version;
  get_merge_table_param.merge_type_ = ctx.param_.merge_type_;
  get_merge_table_param.index_id_ = ctx.param_.index_id_;
  get_merge_table_param.merge_version_ = ctx.param_.merge_version_;
  get_merge_table_param.trans_table_end_log_ts_ = ctx.trans_table_end_log_ts_;
  get_merge_table_param.trans_table_timestamp_ = ctx.trans_table_timestamp_;

  // only ctx.param_ is inited, fill other fields here
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The partition storage has not been initialized, ", K(ret), K_(pkey));
  } else if (!ctx.param_.is_valid() || OB_ISNULL(ctx.partition_guard_.get_pg_partition())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx));
  } else if (ctx.param_.pkey_ != pkey_) {
    ret = OB_ERR_SYS;
    LOG_WARN("pkey not match", K(ret), K(pkey_), K(ctx.param_.pkey_));
  } else if (OB_FAIL(store_.get_merge_tables(get_merge_table_param, get_merge_table_result))) {
    LOG_WARN("failed to get merge tables", K(ret), K(ctx), K(get_merge_table_result));
  } else {
    if (OB_FAIL(ctx.tables_handle_.add_tables(get_merge_table_result.handle_))) {
      LOG_WARN("failed to add tables", K(ret));
    } else if (NULL != get_merge_table_result.base_handle_.get_table() &&
               OB_FAIL(ctx.base_table_handle_.set_table(get_merge_table_result.base_handle_.get_table()))) {
      LOG_WARN("failed to set base table", K(ret));
    } else {
      ctx.sstable_version_range_ = get_merge_table_result.version_range_;
      ctx.read_base_version_ =
          is_major_merge(get_merge_table_param.merge_type_) ? get_merge_table_result.read_base_version_ : 0;
      ctx.log_ts_range_ = get_merge_table_result.log_ts_range_;
      ctx.param_.merge_version_ = get_merge_table_result.merge_version_;
      if (ctx.param_.merge_type_ != get_merge_table_result.suggest_merge_type_) {
        FLOG_INFO(
            "change merge type", "param", ctx.param_, "suggest_merge_type", get_merge_table_result.suggest_merge_type_);
        ctx.param_.merge_type_ = get_merge_table_result.suggest_merge_type_;
      }
      ctx.base_schema_version_ = get_merge_table_result.base_schema_version_;
      ctx.schema_version_ = get_merge_table_result.schema_version_;
      ctx.create_snapshot_version_ = get_merge_table_result.create_snapshot_version_;
      ctx.dump_memtable_timestamp_ = get_merge_table_result.dump_memtable_timestamp_;
      ctx.create_sstable_for_large_snapshot_ = get_merge_table_result.create_sstable_for_large_snapshot_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_schemas_to_merge(ctx))) {
    LOG_WARN("Fail to get schemas to merge, ", K(ret), K_(pkey), K(ctx));
  } else {
    if (OB_SUCC(ret)) {
      if (ctx.param_.is_major_merge()) {
        if (OB_FAIL(cal_major_merge_param(ctx))) {
          LOG_WARN("Fail to cal major merge param, ", K(ret), K_(pkey), K(ctx));
        }
      } else {
        if (OB_FAIL(cal_minor_merge_param(ctx))) {
          LOG_WARN("Fail to cal minor merge param, ", K(ret), K_(pkey), K(ctx));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("succeed to build merge ctx", K(pkey_), K(ctx));
  }

  return ret;
}

int ObPartitionStorage::check_need_update_estimator(
    const ObTableSchema& table_schema, int64_t data_version, int64_t& stat_sampling_ratio)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The partition storage has not been initialized", K(ret), K_(pkey));
  } else if (OB_UNLIKELY(!table_schema.is_valid()) || OB_UNLIKELY(data_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(table_schema), K(data_version));
  } else if (table_schema.is_index_table()) {
    // index no need update estimator
    stat_sampling_ratio = 0;
  } else if (FALSE_IT(stat_sampling_ratio = GCONF.merge_stat_sampling_ratio)) {
  } else if (stat_sampling_ratio < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid stat_sampling_ratio value", K(stat_sampling_ratio), K(ret));
  } else if (0 == stat_sampling_ratio) {
    // ratio == 0 mean disable estimator
  } else {
    int tmp_ret = OB_SUCCESS;
    ObArenaAllocator allocator(ObModIds::OB_CS_MERGER);
    ObArray<ObColumnStat*> column_stats;
    ObArray<ObColDesc, ObIAllocator&> column_ids(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
    uint64_t table_id = table_schema.get_table_id();
    void* ptr = NULL;
    ObColumnStat* stat_ptr = NULL;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = table_schema.get_store_column_ids(column_ids)))) {
      LOG_WARN("Fail to get column ids. ", K(tmp_ret));
    }
    for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < column_ids.count(); ++i) {
      ptr = allocator.alloc(sizeof(ObColumnStat));
      if (OB_ISNULL(ptr) || OB_ISNULL(stat_ptr = new (ptr) ObColumnStat(allocator)) ||
          OB_UNLIKELY(!stat_ptr->is_writable())) {
        tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for ObColumnStat object. ", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = column_stats.push_back(stat_ptr))) {
        LOG_WARN("fail to push back stat_ptr. ", K(tmp_ret));
      } else {
        stat_ptr->set_table_id(table_id);
        stat_ptr->set_partition_id(pkey_.get_partition_id());
        stat_ptr->set_column_id(column_ids.at(i).col_id_);
        ptr = NULL;
        stat_ptr = NULL;
      }
    }
    if (OB_SUCCESS == tmp_ret) {
      if (OB_SUCCESS != (tmp_ret = ObStatManager::get_instance().get_batch_stat(
                             table_schema, pkey_.get_partition_id(), column_stats, allocator))) {
        LOG_WARN("fail to batch get column stat. ", K(tmp_ret), K(table_id), K(pkey_.get_partition_id()));
      } else {
        for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < column_ids.count(); ++i) {
          stat_ptr = column_stats.at(i);
          if (OB_ISNULL(stat_ptr) || column_ids.at(i).col_id_ != stat_ptr->get_key().column_id_) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected error.",
                K(tmp_ret),
                KP(stat_ptr),
                K(table_id),
                K(pkey_.get_partition_id()),
                "expect_column_id",
                column_ids.at(i).col_id_,
                "actual_column_id",
                stat_ptr->get_key().column_id_);
          } else if (stat_ptr->get_version() >= data_version) {
            LOG_INFO("column stat has updated by another replica.", K(*stat_ptr));
            // no need update in this time, put NULL;
            stat_sampling_ratio = 0;
            break;
          }
        }
      }
    }
    if (OB_SUCCESS != tmp_ret) {
      stat_sampling_ratio = 0;
    }
  }

  LOG_INFO("check_estimator_stat_sampling_ratio", K(data_version), K(stat_sampling_ratio));
  return ret;
}

int ObPartitionStorage::update_estimator(const ObTableSchema* base_schema, const bool is_full,
    const ObIArray<ObColumnStat*>& column_stats, ObSSTable* sstable, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  int64_t estimate_start_time = 0;
  int64_t estimate_end_time = 0;
  ObArenaAllocator allocator;
  ObSSTableMergeInfo& sstable_merge_info = sstable->get_sstable_merge_info();
  estimate_start_time = ::oceanbase::common::ObTimeUtility::current_time();
  int tmp_ret = OB_SUCCESS;
  bool need_report = true;
  if (!is_full) {
    ObArray<common::ObColumnStat*> base_column_stats;
    ObArray<ObColDesc, ObIAllocator&> column_ids(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
    ObColumnStat* stat_ptr = NULL;
    void* ptr = NULL;
    int64_t partition_id = pkey.get_partition_id();
    uint64_t table_id = base_schema->get_table_id();
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = base_schema->get_store_column_ids(column_ids)))) {
      LOG_WARN("Fail to get column ids. ", K(tmp_ret));
    }
    for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < column_ids.count(); ++i) {
      ptr = allocator.alloc(sizeof(common::ObColumnStat));
      if (OB_ISNULL(ptr) || OB_ISNULL(stat_ptr = new (ptr) common::ObColumnStat(allocator)) ||
          OB_UNLIKELY(!stat_ptr->is_writable())) {
        tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for ObColumnStat object. ", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = base_column_stats.push_back(stat_ptr))) {
        LOG_WARN("fail to push back stat_ptr. ", K(tmp_ret));
      } else {
        stat_ptr->set_table_id(table_id);
        stat_ptr->set_partition_id(partition_id);
        stat_ptr->set_column_id(column_ids.at(i).col_id_);
        ptr = NULL;
        stat_ptr = NULL;
      }
    }
    if (OB_SUCCESS == tmp_ret) {
      if (OB_SUCCESS != (tmp_ret = ObStatManager::get_instance().get_batch_stat(
                             *base_schema, partition_id, base_column_stats, allocator))) {
        STORAGE_LOG(WARN, "fail to batch get column stat. ", K(tmp_ret), K(partition_id));
      } else if (base_column_stats.count() == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column stat count should not be 0", K(tmp_ret));
      } else if (base_column_stats.at(0)->get_version() >= sstable_merge_info.version_.version_) {
        // already has latest column stat, no need to update
        need_report = false;
      } else {
        for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < column_stats.count(); ++i) {
          for (int64_t j = 0; OB_SUCCESS == tmp_ret && j < base_column_stats.count(); ++j) {
            if (NULL != base_column_stats.at(j) && NULL != column_stats.at(i) &&
                base_column_stats.at(j)->get_column_id() == column_stats.at(i)->get_column_id()) {
              if (OB_SUCCESS != (tmp_ret = column_stats.at(i)->add(*base_column_stats.at(j)))) {
                STORAGE_LOG(WARN, "Fail to add other, ", K(tmp_ret));
              }
            }
          }
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCCESS == tmp_ret && need_report && i < column_stats.count(); ++i) {
    if (NULL != column_stats.at(i)) {
      if (is_full || 0 == column_stats.at(i)->get_version()) {
        column_stats.at(i)->set_last_rebuild_version(sstable_merge_info.version_.version_);
      }
      column_stats.at(i)->set_version(sstable_merge_info.version_.version_);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = column_stats.at(i)->finish()))) {
        STORAGE_LOG(WARN, "Fail to finish column state, ", K(tmp_ret), K(i));
      }
    }
  }
  if (need_report &&
      OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ObStatManager::get_instance().update_column_stats(column_stats)))) {
    STORAGE_LOG(WARN, "Fail to update column stats, ", K(tmp_ret));
  } else {
    STORAGE_LOG(INFO, "finish update column stat completed.", K(need_report));
  }
  estimate_end_time = ::oceanbase::common::ObTimeUtility::current_time();
  sstable_merge_info.estimate_cost_time_ = (estimate_end_time - estimate_start_time);
  return ret;
}

bool ObPartitionStorage::has_memstore()
{
  bool bret = false;
  ObTablesHandle tables_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(ERROR, "partition storage is not initialized");
  } else {
    bret = store_.has_memtable();
  }
  return bret;
}

int ObPartitionStorage::get_replayed_table_version(int64_t& table_version)
{
  int ret = OB_SUCCESS;
  ObTableHandle table_handle;
  ObMemtable* memtable = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_FAIL(store_.get_active_memtable(table_handle))) {
    STORAGE_LOG(WARN, "Fail to get active memtable", K(ret));
  } else if (OB_FAIL(table_handle.get_memtable(memtable))) {
    STORAGE_LOG(WARN, "failed to get memtable", K(ret));
  } else if (OB_ISNULL(memtable)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "memtable must not null", K(ret));
  } else {
    table_version = memtable->get_max_schema_version();
  }
  return ret;
}

int ObPartitionStorage::serialize(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_FAIL(store_.serialize(allocator, new_buf, serialize_size))) {
    STORAGE_LOG(WARN, "failed to serialize store", K(ret));
  }

  return ret;
}

int ObPartitionStorage::deserialize(const ObReplicaType replica_type, const char* buf, const int64_t buf_len,
    ObIPartitionGroup* pg, int64_t& pos, bool& is_old_meta, ObPartitionStoreMeta& old_meta)
{
  int ret = OB_SUCCESS;
  int64_t magic_num = 0;
  int64_t tmp_pos = pos;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0 || pos < 0 || pos >= buf_len || OB_ISNULL(pg)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(buf), K(buf_len), K(pos), K(ret), KP(pg));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, tmp_pos, &magic_num))) {
    STORAGE_LOG(WARN, "deserialize magic_num failed.", K(ret));
  } else if (MAGIC_NUM_1_4_61 != magic_num) {
    if (OB_FAIL(store_.deserialize(pg, pg_memtable_mgr_, replica_type, buf, buf_len, pos, is_old_meta, old_meta))) {
      LOG_WARN("failed to deserialize store_", K(ret), K(buf_len), K(pos), K(magic_num));
    } else {
      pkey_ = store_.get_partition_key();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("MAGIC_NUM_1_4_61 should not be here", K(ret), K(magic_num));
  }
  return ret;
}

int ObPartitionStorage::get_all_tables(ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_FAIL(store_.get_all_tables(tables_handle))) {
    STORAGE_LOG(WARN, "failed to get_all_tables", K(ret));
  }
  return ret;
}

int ObPartitionStorage::get_schemas_to_split(storage::ObSSTableSplitCtx& ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t index_id = ctx.param_.index_id_;
  int64_t& schema_version = ctx.schema_version_;
  ObSchemaGetterGuard& schema_guard = ctx.schema_guard_;
  const ObTableSchema*& table_schema = ctx.table_schema_;

  int64_t& split_schema_version = ctx.split_schema_version_;
  ObSchemaGetterGuard& split_schema_guard = ctx.split_schema_guard_;
  const ObTableSchema*& split_table_schema = ctx.split_table_schema_;

  ObITable* table = NULL;

  const uint64_t tenant_id = is_inner_table(index_id) ? OB_SYS_TENANT_ID : extract_tenant_id(index_id);
  const bool check_formal = extract_pure_id(index_id) > OB_MAX_CORE_TABLE_ID;  // to avoid circular dependency

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!is_valid_id(index_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(index_id));
  } else if (ctx.tables_handle_.get_tables().count() < 1) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table count must not zero", K(ret), K(pkey_));
  } else if (ctx.param_.is_major_split_) {
    if (OB_ISNULL(table = ctx.tables_handle_.get_table(0))) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "failed to get first table", K(ret), K(pkey_));
    } else if (OB_FAIL(table->get_frozen_schema_version(schema_version))) {
      LOG_WARN("failed to get frozen schema version", K(ret), K_(pkey), KPC(table));
    }
  } else if (!ctx.param_.is_major_split_) {
    int64_t tenant_schema_version = 0;
    if (OB_ISNULL(table = ctx.tables_handle_.get_last_table())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "failed to get last table", K(ret));
    } else if (OB_FAIL(table->get_frozen_schema_version(schema_version))) {
      LOG_WARN("failed to get frozen schema version", K(ret), K_(pkey), KPC(table));
    } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id, tenant_schema_version))) {
      LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id), K_(pkey));
    } else if (tenant_schema_version < schema_version) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("local schema is too old", K(ret), K(tenant_schema_version), K(schema_version));
    } else {
      schema_version = tenant_schema_version;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ctx.partition_guard_.get_partition_group())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "partition should not be NULL", K(ret));
    } else {
      split_schema_version = ctx.partition_guard_.get_partition_group()->get_split_info().get_schema_version();
      if (OB_FAIL(schema_service_->get_tenant_schema_guard(
              tenant_id, split_schema_guard, split_schema_version, OB_INVALID_VERSION))) {
        STORAGE_LOG(WARN, "Fail to get schema mgr by version, ", K(ret), K(ctx));
      } else if (check_formal && OB_FAIL(split_schema_guard.check_formal_guard())) {
        STORAGE_LOG(WARN, "schema_guard is not formal", K(ret), K(tenant_id));
      } else if (OB_FAIL(split_schema_guard.get_table_schema(ctx.param_.index_id_, split_table_schema))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_TABLE_IS_DELETED;
          STORAGE_LOG(INFO, "table is deleted", K(index_id), K(schema_version));
        } else {
          STORAGE_LOG(WARN, "Fail to get table schema, ", K(ret), K(index_id));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(
            schema_service_->get_tenant_schema_guard(tenant_id, schema_guard, schema_version, OB_INVALID_VERSION))) {
      STORAGE_LOG(WARN, "Fail to get schema, ", K(schema_version), K(ret));
    } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
      STORAGE_LOG(WARN, "schema_guard is not formal", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_id, table_schema))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLE_IS_DELETED;
        STORAGE_LOG(INFO, "table is deleted", K(index_id), K(schema_version));
      } else {
        STORAGE_LOG(WARN, "Fail to get table schema, ", K(ret), K(index_id));
      }
    } else if (NULL == table_schema) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN, "table_schema must not null", K(ret), K_(pkey), K(index_id), K(schema_version));
    } else {
      STORAGE_LOG(INFO, "table_schema is, ", K(*table_schema));
    }
  }

  return ret;
}

int ObPartitionStorage::get_schemas_to_merge(storage::ObSSTableMergeCtx& ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t index_id = ctx.param_.index_id_;
  int64_t& schema_version = ctx.schema_version_;
  ObSchemaGetterGuard& schema_guard = ctx.schema_guard_;
  const ObTableSchema*& data_table_schema = ctx.data_table_schema_;
  const ObTableSchema*& table_schema = ctx.table_schema_;
  const ObTableSchema*& dep_table_schema = ctx.mv_dep_table_schema_;
  int64_t save_schema_version = schema_version;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!is_valid_id(index_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(index_id));
  } else if (OB_FAIL(schema_service_->retry_get_schema_guard(
                 schema_version, index_id, schema_guard, save_schema_version))) {
    if (OB_TABLE_IS_DELETED != ret) {
      STORAGE_LOG(WARN, "Fail to get schema, ", K(schema_version), K(ret));
    } else {
      STORAGE_LOG(WARN, "table is deleted", K(ret), K(index_id));
    }
  } else if (save_schema_version < schema_version) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN(
        "can not use older schema version", K(ret), K(schema_version), K(save_schema_version), K_(pkey), K(index_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_id, table_schema))) {
    STORAGE_LOG(WARN, "Fail to get table schema, ", K(ret), K(index_id));
  } else if (NULL == table_schema) {
    const uint64_t fetch_tenant_id = is_inner_table(index_id) ? OB_SYS_TENANT_ID : extract_tenant_id(index_id);
    if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
      STORAGE_LOG(WARN, "Fail to get schema, ", K(ret), K(fetch_tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_id, table_schema))) {
      STORAGE_LOG(WARN, "Fail to get table schema, ", K(ret), K(index_id));
    } else if (NULL == table_schema) {
      ret = OB_TABLE_IS_DELETED;
      STORAGE_LOG(WARN, "table is deleted", K(ret), K_(pkey), K(index_id));
    }
  }

  if (OB_SUCC(ret)) {
    const ObPrintableTableSchema* print_table_schema = reinterpret_cast<const ObPrintableTableSchema*>(table_schema);
    FLOG_INFO("Get schema to merge", K(schema_version), K(save_schema_version), K(*print_table_schema));
    schema_version = save_schema_version;
  }
  if (OB_SUCC(ret)) {
    if (pkey_.table_id_ != index_id) {
      if (OB_FAIL(schema_guard.get_table_schema(pkey_.table_id_, data_table_schema))) {
        STORAGE_LOG(WARN, "Fail to get data table schema, ", K(ret), K(schema_version), K(pkey_.table_id_));
      } else if (NULL == data_table_schema) {
        ret = OB_SCHEMA_ERROR;
        LOG_ERROR("data_table_schema must not null", K(ret), K(pkey_), K(schema_version), K(pkey_.table_id_));
      } else {
        STORAGE_LOG(INFO,
            "Get data table schema to merge",
            K(schema_version),
            K(*reinterpret_cast<const ObPrintableTableSchema*>(data_table_schema)));
      }
    } else {
      data_table_schema = table_schema;
    }

    if (OB_SUCC(ret) && NULL != data_table_schema) {
      const bool with_global_index = false;
      if (OB_FAIL(schema_guard.get_all_aux_table_status(
              data_table_schema->get_table_id(), with_global_index, ctx.index_stats_))) {
        LOG_WARN("failed to get index statsu", K(ret), K(pkey_));
      } else {
        LOG_INFO("succeed to get index status", "index_status", ctx.index_stats_);
      }
    }
  }
  // get dependent table schema, only major merge need merge mv
  if (OB_SUCC(ret) && ctx.param_.is_major_merge()) {
    if (OB_FAIL(get_depend_table_schema(table_schema, schema_guard, dep_table_schema))) {
      STORAGE_LOG(WARN, "fail to get depend table schema", K(ret));
    }
  }

  if (OB_SUCC(ret) && NULL != table_schema && table_schema->is_use_bloomfilter()) {
    ctx.bf_rowkey_prefix_ = table_schema->get_rowkey_column_num();
    if (OB_SUCCESS != get_prefix_access_stat().get_optimal_prefix(ctx.bf_rowkey_prefix_)) {
      ctx.bf_rowkey_prefix_ = 0;
    } else if (OB_UNLIKELY(ctx.bf_rowkey_prefix_ > table_schema->get_rowkey_column_num())) {
      STORAGE_LOG(WARN, "unexpected prefix rowkey,", K(ctx.bf_rowkey_prefix_));
      ctx.bf_rowkey_prefix_ = 0;
    } else {
      STORAGE_LOG(INFO, "succeed to get optimal prefix,", K(ctx.bf_rowkey_prefix_));
    }
  }
  return ret;
}

int ObPartitionStorage::get_depend_table_schema(
    const ObTableSchema* table_schema, ObSchemaGetterGuard& schema_guard, const ObTableSchema*& dep_table_schema)
{
  int ret = OB_SUCCESS;
  dep_table_schema = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(table_schema));
  } else {
    const int64_t dep_table_count = table_schema->get_depend_table_ids().count();
    if (0 == dep_table_count) {
      // do nothing
    } else if (1 != dep_table_count) {
      ret = OB_ERR_SYS;
      LOG_ERROR("materialized view only support one depend table", K(ret), K(pkey_));
    } else {
      const uint64_t dep_table_id = table_schema->get_depend_table_ids().at(0);
      if (OB_FAIL(schema_guard.get_table_schema(dep_table_id, dep_table_schema))) {
        LOG_WARN("failed to get table schema", K(ret), K(dep_table_id));
      } else if (NULL == dep_table_schema) {
        ret = OB_SCHEMA_ERROR;
        LOG_ERROR("dep_table_schema must not null", K(ret), K(pkey_), K(dep_table_id));
      } else {
        LOG_INFO("get dependent schema to merge", K(*dep_table_schema));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::check_is_schema_changed(const int64_t column_checksum_method,
    const ObTableSchema& base_table_schema, const ObTableSchema& main_table_schema, bool& is_schema_changed,
    bool& is_column_changed, bool& is_progressive_merge_num_changed)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2* base_column = NULL;
  is_schema_changed = false;
  is_column_changed = false;
  is_progressive_merge_num_changed =
      base_table_schema.get_progressive_merge_num() != main_table_schema.get_progressive_merge_num();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  }

  for (ObTableSchema::const_column_iterator iter = main_table_schema.column_begin();
       OB_SUCC(ret) && iter != main_table_schema.column_end() && !is_column_changed;
       ++iter) {
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "wrong column iterator", K(ret));
    } else if (NULL != (base_column = base_table_schema.get_column_schema((*iter)->get_column_id()))) {
      if (CCM_VALUE_ONLY == column_checksum_method && ob_is_integer_type((*iter)->get_meta_type().get_type()) &&
          ob_is_integer_type(base_column->get_meta_type().get_type())) {
        is_column_changed = (*iter)->get_meta_type().get_type() < base_column->get_meta_type().get_type();
      } else if (CCM_VALUE_ONLY == column_checksum_method && ob_is_string_type((*iter)->get_meta_type().get_type()) &&
                 (ob_is_string_type(base_column->get_meta_type().get_type())) &&
                 (*iter)->get_charset_type() == base_column->get_charset_type() &&
                 (*iter)->get_meta_type().get_collation_type() == base_column->get_collation_type()) {
        is_column_changed = (*iter)->get_accuracy().get_length() < base_column->get_accuracy().get_length();
      } else if ((common::ObStringTC == (*iter)->get_meta_type().get_type_class()
                         ? (*iter)->get_accuracy().get_length() < base_column->get_accuracy().get_length()
                         : (*iter)->get_accuracy() != base_column->get_accuracy()) ||
                 (*iter)->get_charset_type() != base_column->get_charset_type() ||
                 (*iter)->get_meta_type() != base_column->get_meta_type() ||
                 (*iter)->get_orig_default_value() != base_column->get_orig_default_value()) {
        // column is modified
        is_column_changed = true;
      }
    } else {
      // add some column
      is_schema_changed = true;
      if ((*iter)->is_stored_generated_column()) {
        is_column_changed = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (base_table_schema.get_column_count() != main_table_schema.get_column_count() ||
        0 != strcmp(base_table_schema.get_compress_func_name(), main_table_schema.get_compress_func_name()) ||
        base_table_schema.get_row_store_type() != main_table_schema.get_row_store_type()
        //|| base_table_schema.get_store_format() != main_table_schema.get_store_format()
        // enable it when store format represent more storage option
        || base_table_schema.get_pctfree() != main_table_schema.get_pctfree()) {
      is_schema_changed = true;
    }
  }

  STORAGE_LOG(INFO,
      "check schema is changed",
      K(is_schema_changed),
      K(is_column_changed),
      K(is_progressive_merge_num_changed),
      K(reinterpret_cast<const ObPrintableTableSchema&>(base_table_schema)),
      K(reinterpret_cast<const ObPrintableTableSchema&>(main_table_schema)));

  return ret;
}

int ObPartitionStorage::cal_major_merge_param(ObSSTableMergeCtx& ctx)
{
  int ret = OB_SUCCESS;
  bool is_schema_changed = false;
  bool is_column_changed = false;
  bool is_progressive_merge_num_changed = false;
  ObSSTable* last_major_sstable = nullptr;
  int64_t merge_version = ctx.param_.merge_version_.major_;
  ctx.checksum_method_ = 0;

  // some input param check
  CK(OB_NOT_NULL(ctx.table_schema_));

  // check schema change to determine whether use increment/progressive/full merge
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx.base_table_handle_.get_sstable(last_major_sstable))) {
      LOG_WARN("failed to get sstable", K(ret));
    } else if (OB_ISNULL(last_major_sstable)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("last_major_sstable must not null", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const ObTableSchema* base_table_schema = nullptr;
    ObSchemaGetterGuard base_schema_guard;
    uint64_t tenant_id =
        is_inner_table(ctx.table_schema_->get_table_id()) ? OB_SYS_TENANT_ID : ctx.table_schema_->get_tenant_id();
    const uint64_t table_id = pkey_.get_table_id();
    const bool check_formal = extract_pure_id(table_id) > OB_MAX_CORE_TABLE_ID;  // to avoid circular dependency

    OZ(schema_service_->get_tenant_schema_guard(
        tenant_id, base_schema_guard, ctx.base_schema_version_, OB_INVALID_VERSION));
    if (check_formal) {
      OZ(base_schema_guard.check_formal_guard());
    }
    OZ(base_schema_guard.get_table_schema(ctx.param_.index_id_, base_table_schema),
        tenant_id,
        ctx.base_schema_version_,
        ctx.table_schema_->get_table_id());

    if (OB_SUCC(ret) && OB_NOT_NULL(base_table_schema)) {
      // determine whether use increment/progressive/full merge
      OZ(check_is_schema_changed(last_major_sstable->get_meta().checksum_method_,
          *base_table_schema,
          *ctx.table_schema_,
          is_schema_changed,
          is_column_changed,
          is_progressive_merge_num_changed));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t schema_progressive_merge_round = ctx.data_table_schema_->get_progressive_merge_round();
    ctx.is_full_merge_ = false;
    if (is_column_changed) {
      // column has been changed, must do full merge
      ctx.is_full_merge_ = true;
    } else if (1 == ctx.table_schema_->get_progressive_merge_num()) {
      ctx.is_full_merge_ = true;
    }

    if (0 == schema_progressive_merge_round) {
      ctx.use_new_progressive_ = false;
      if (ctx.is_full_merge_) {
        // full merge
        ctx.progressive_merge_num_ = 1;
        ctx.progressive_merge_start_version_ = 0;
      } else {
        const int64_t rewrite_merge_version = 0;
        int64_t progressive_merge_num = ctx.table_schema_->get_progressive_merge_num();
        if (0 == progressive_merge_num) {
          if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2100) {
            // force rewrite
            progressive_merge_num = 0 == rewrite_merge_version ? 0 : OB_AUTO_PROGRESSIVE_MERGE_NUM;
          }
        }
        if (is_schema_changed || (rewrite_merge_version == merge_version)) {
          // new progressive merge
          ctx.progressive_merge_num_ = progressive_merge_num;
          ctx.progressive_merge_start_version_ = merge_version;
        } else if (last_major_sstable->get_meta().progressive_merge_start_version_ < merge_version &&
                   last_major_sstable->get_meta().progressive_merge_end_version_ > merge_version) {
          // in progressive merge progress
          if (is_progressive_merge_num_changed) {
            ctx.progressive_merge_num_ = progressive_merge_num;
            ctx.progressive_merge_start_version_ = merge_version;
          } else {
            ctx.progressive_merge_num_ = last_major_sstable->get_meta().progressive_merge_end_version_ -
                                         last_major_sstable->get_meta().progressive_merge_start_version_;
            ctx.progressive_merge_start_version_ = last_major_sstable->get_meta().progressive_merge_start_version_;
          }
        } else if (rewrite_merge_version > 0 && merge_version >= rewrite_merge_version &&
                   merge_version < rewrite_merge_version + progressive_merge_num) {
          // in online index build, progressive_merge_start_version is not calculated in init, need extra check
          ctx.progressive_merge_start_version_ =
              is_progressive_merge_num_changed ? merge_version : rewrite_merge_version;
          ctx.progressive_merge_num_ = progressive_merge_num;
          STORAGE_LOG(INFO, "build index", K(ctx.progressive_merge_start_version_), K(ctx.progressive_merge_num_));
        } else {
          // increment merge
          ctx.progressive_merge_num_ = 0;
          ctx.progressive_merge_start_version_ = 0;
        }
      }

      if (OB_SUCC(ret)) {
        if (ctx.table_schema_->is_index_table()) {
          // if this is the first round major merge after index created, do full merge
          // to avoid bug 15777811
          // TODO: temporary solution, better way is to read tables using non-intersected version range
          if (last_major_sstable->get_snapshot_version() == ctx.create_snapshot_version_) {
            ctx.is_full_merge_ = true;
            // full merge in index build, progressive_merge_start_version needs to be calculated, keeping consistent
            // with main table
            ctx.backup_progressive_merge_num_ = ctx.progressive_merge_num_;
            ctx.backup_progressive_merge_start_version_ = ctx.progressive_merge_start_version_;
            ctx.progressive_merge_num_ = 1;
            ctx.progressive_merge_start_version_ = 0;
            ctx.is_created_index_first_merge_ = true;
          }
        }
      }

      STORAGE_LOG(INFO,
          "Calc progressive param, ",
          K(is_schema_changed),
          K(is_column_changed),
          K(is_progressive_merge_num_changed),
          K(ctx.progressive_merge_num_),
          K(ctx.progressive_merge_start_version_),
          K(merge_version),
          K(ctx.table_schema_->get_progressive_merge_num()),
          K(ctx.data_table_schema_->get_progressive_merge_num()),
          K(ctx.backup_progressive_merge_num_),
          K(ctx.backup_progressive_merge_start_version_));
    } else {
      const int64_t meta_progressive_merge_round = last_major_sstable->get_meta().progressive_merge_round_;
      const int64_t storage_format_version = ctx.data_table_schema_->get_storage_format_version();
      int64_t progressive_merge_num = 0;
      if (0 == ctx.data_table_schema_->get_progressive_merge_num()) {
        if (storage_format_version < OB_STORAGE_FORMAT_VERSION_V4) {
          progressive_merge_num = OB_AUTO_PROGRESSIVE_MERGE_NUM;
        } else {
          progressive_merge_num = 1 == schema_progressive_merge_round ? 0 : OB_AUTO_PROGRESSIVE_MERGE_NUM;
        }
      } else {
        progressive_merge_num = ctx.data_table_schema_->get_progressive_merge_num();
      }
      ctx.progressive_merge_num_ = progressive_merge_num;
      if (ctx.is_full_merge_) {
        ctx.progressive_merge_round_ = schema_progressive_merge_round;
        ctx.progressive_merge_step_ = progressive_merge_num;
      } else {
        ctx.use_new_progressive_ = true;
        if (meta_progressive_merge_round < schema_progressive_merge_round) {
          ctx.progressive_merge_round_ = schema_progressive_merge_round;
          ctx.progressive_merge_step_ = 0;
          ctx.progressive_merge_start_version_ = merge_version;
        } else if (meta_progressive_merge_round == schema_progressive_merge_round) {
          ctx.progressive_merge_round_ = meta_progressive_merge_round;
          ctx.progressive_merge_step_ = last_major_sstable->get_meta().progressive_merge_step_;
        }
      }
      STORAGE_LOG(INFO,
          "Calc progressive param",
          K(is_schema_changed),
          K(is_column_changed),
          K(is_progressive_merge_num_changed),
          K(ctx.progressive_merge_num_),
          K(ctx.progressive_merge_round_),
          K(schema_progressive_merge_round),
          K(storage_format_version),
          K(ctx.progressive_merge_step_),
          K(ctx.is_full_merge_),
          K(progressive_merge_num),
          K(meta_progressive_merge_round));
    }
  }

  // determine logical data version
  if (OB_SUCC(ret)) {
    if (last_major_sstable->get_logical_data_version() >= merge_version) {
      ctx.logical_data_version_ = last_major_sstable->get_logical_data_version() + 1;
      STORAGE_LOG(INFO,
          "Logical data version of sstable larger than frozen version",
          K(ctx.logical_data_version_),
          K(merge_version));
    } else {
      ctx.logical_data_version_ = merge_version;
    }
  }

  // determine checksum method
  if (OB_SUCC(ret)) {
    const int64_t progressive_start_version = ctx.progressive_merge_start_version_;
    const int64_t progressive_end_version = ctx.progressive_merge_start_version_ + ctx.progressive_merge_num_;
    ctx.checksum_method_ = last_major_sstable->get_meta().checksum_method_;
    ctx.is_in_progressive_new_checksum_ = false;
  }

  // determine merge level and data format
  OZ(get_merge_level(ctx.param_.merge_version_, ctx, ctx.merge_level_));
  OZ(get_store_column_checksum_in_micro(ctx.param_.merge_version_, ctx, ctx.store_column_checksum_in_micro_));
  if (OB_SUCC(ret)) {
    if (ctx.is_full_merge_) {
      ctx.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
    } else {
      const int64_t storage_format_version = ctx.data_table_schema_->get_storage_format_version();
      if (storage_format_version < OB_STORAGE_FORMAT_VERSION_V4) {
        if (ctx.param_.merge_version_ >= ctx.progressive_merge_start_version_ &&
            ctx.param_.merge_version_ < ctx.progressive_merge_start_version_ + ctx.progressive_merge_num_) {
          // when doing progressive merge, should use macro block merge level
          ctx.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
        }
      } else {
        if (ctx.progressive_merge_step_ < ctx.progressive_merge_num_) {
          ctx.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
        }
      }

      if (ctx.merge_level_ != MACRO_BLOCK_MERGE_LEVEL) {
        if (is_column_changed || is_schema_changed) {
          ctx.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
        }
      }
    }
  }

  // determine estimator, parallize, mv and init final merge ctx
  if (OB_SUCC(ret)) {
    OZ(check_need_update_estimator(*ctx.table_schema_, ctx.param_.merge_version_, ctx.stat_sampling_ratio_));
  }
  OZ(prepare_merge_mv_depend_sstable(
         ctx.param_.merge_version_, ctx.table_schema_, ctx.mv_dep_table_schema_, ctx.mv_dep_tables_handle_),
      pkey_,
      ctx.param_.merge_version_);
  OZ(init_merge_context(ctx), ctx);
  return ret;
}

int ObPartitionStorage::cal_minor_merge_param(ObSSTableMergeCtx& ctx)
{
  int ret = OB_SUCCESS;
  const ObMultiVersionRowInfo* multi_version_row_info = NULL;
  ObMultiVersionColDescGenerate multi_version_col_desc_gen;
  ObSSTable* first_minor_sstable = nullptr;

  // some input param check
  CK(!ctx.tables_handle_.empty());
  CK(OB_NOT_NULL(ctx.tables_handle_.get_table(0)));

  if (OB_SUCC(ret)) {
    ctx.progressive_merge_num_ = 0;
    // determine whether to use increment/full merge
    ctx.is_full_merge_ = false;
    ctx.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;

    if (ctx.tables_handle_.get_table(0)->is_minor_sstable()) {
      // if base_version of minor merge is bigger than base_version of minor sstable in the merge, then run full merge
      // 1. to clean outdated data in the multi version sstable
      // 2. to avoid bug, because with incremental minor merge, if multi version rows with same rowkey
      // cross macro blocks, it cannot know the position of last_multi_version_row
      first_minor_sstable = reinterpret_cast<ObSSTable*>(ctx.tables_handle_.get_table(0));
      if (first_minor_sstable->is_multi_version_minor_sstable() &&
          ctx.sstable_version_range_.base_version_ > first_minor_sstable->get_base_version()) {
        ctx.is_full_merge_ = true;
      }
    }

    // determine logical data version
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(first_minor_sstable) &&
          first_minor_sstable->get_logical_data_version() >= ctx.sstable_version_range_.snapshot_version_) {
        ctx.logical_data_version_ = first_minor_sstable->get_logical_data_version() + 1;
        STORAGE_LOG(
            INFO, "Logical data version of sstable larger than snapshot version", K(ctx), KPC(first_minor_sstable));
      } else {
        ctx.logical_data_version_ = ctx.sstable_version_range_.snapshot_version_;
      }
    }
    OZ(multi_version_col_desc_gen.init(ctx.table_schema_));
    OZ(multi_version_col_desc_gen.generate_multi_version_row_info(multi_version_row_info));
    CK(OB_NOT_NULL(multi_version_row_info));
    if (OB_SUCC(ret)) {
      ctx.multi_version_row_info_ = *multi_version_row_info;
    }

    OZ(init_merge_context(ctx), ctx);
  }

  return ret;
}

int ObPartitionStorage::init_merge_context(storage::ObSSTableMergeCtx& ctx)
{
  int ret = OB_SUCCESS;
  bool has_lob = false;
  ObArray<const blocksstable::ObSSTableMeta*> sstable_metas;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_ISNULL(ctx.table_schema_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("table schema must not null", K(ret), K(ctx));
  } else if (OB_FAIL(ctx.init_parallel_merge())) {
    LOG_WARN("Failed to init parallel merge in sstable merge ctx", K(ret));
  } else if (OB_FAIL(ctx.table_schema_->has_lob_column(has_lob, true))) {
    LOG_WARN("Failed to check table has lob column", K(ret));
  } else if (OB_FAIL(ctx.merge_context_.init(ctx.get_concurrent_cnt(), has_lob, &ctx.column_stats_, false))) {
    LOG_WARN("failed to init merge context", K(ret));
  } else if (ctx.param_.is_major_merge()) {
    ObSSTable* old_version_sstable = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.tables_handle_.get_count(); ++i) {
      ObITable* table = ctx.tables_handle_.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret));
      } else if (table->get_version() == ctx.param_.merge_version_ - 1 && table->is_major_sstable()) {
        old_version_sstable = static_cast<ObSSTable*>(table);
        if (OB_FAIL(sstable_metas.push_back(&old_version_sstable->get_meta()))) {
          LOG_WARN("failed to add sstable meta", K(ret), K(ctx));
        }
      }
    }

    if (OB_SUCC(ret) && ctx.need_incremental_checksum()) {
      if (OB_FAIL(ctx.column_checksum_.init(ctx.get_concurrent_cnt(),
              *ctx.table_schema_,
              !ctx.is_full_merge_,
              ctx.checksum_method_,
              sstable_metas))) {
        LOG_WARN("failed to init merge context checksum", K(ret), K(ctx));
      }
    }
  } else if (ctx.param_.is_mini_merge()) {
    if (OB_FAIL(ctx.merge_context_for_complement_minor_sstable_.init(
            ctx.get_concurrent_cnt(), has_lob, &ctx.column_stats_, true))) {
      LOG_WARN("failed to init merge context", K(ret));
    }
  }
  return ret;
}

int ObPartitionStorage::get_concurrent_cnt(ObTablesHandle& tables_handle,
    const share::schema::ObTableSchema& table_schema, const ObMergeType& merge_type, int64_t& concurrent_cnt)
{
  int ret = OB_SUCCESS;
  ObIArray<ObITable*>& tables = tables_handle.get_tables();

  if (!table_schema.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid args", K(ret), K(table_schema), K(tables.count()));
  } else if (MAJOR_MERGE != merge_type) {
    concurrent_cnt = 1;
    LOG_INFO("merge is only support one thread merge", K(merge_type), K(table_schema), K(tables_handle));
  } else if (tables.count() < 1) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid tables_handle", K(ret), K(tables_handle));
  } else if (!tables.at(0)->is_sstable()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("first table must be sstable", K(ret), K(tables));
  } else {
    ObSSTable* first_sstable = static_cast<ObSSTable*>(tables.at(0));
    const int64_t tablet_size = table_schema.get_tablet_size();
    if (OB_FAIL(first_sstable->get_concurrent_cnt(tablet_size, concurrent_cnt))) {
      LOG_WARN("failed to get_concurrent_cnt", K(ret), K(tables));
    }
  }
  return ret;
}

void ObPartitionStorage::check_data_checksum(const common::ObReplicaType& replica_type, const ObPartitionKey& pkey,
    const ObTableSchema& schema, const ObSSTable& data_sstable, common::ObIArray<storage::ObITable*>& base_tables,
    ObIPartitionReport& report)
{
  // check data_checksum between replicas.The check is used to hold the memstore if the data_checksum is not equal
  // between replicas Notes: if two replicas select the __all_meta_table at the same time, they may both can't get the
  // current version replica information. In this case, the check is not useful.
  int ret = OB_SUCCESS;
  ObArray<ObSSTableDataChecksumItem> items;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, sql proxy must not be NULL", K(ret));
  } else if (!data_sstable.is_major_sstable()) {
    // only check major sstable
    // TODO: open later
  } else if (OB_SUCCESS != (ret = ObSSTableDataChecksumOperator::get_checksum(pkey.get_table_id(),
                                schema.get_table_id(),
                                pkey.get_partition_id(),
                                ObITable::MAJOR_SSTABLE,
                                items,
                                *GCTX.sql_proxy_))) {
    STORAGE_LOG(WARN, "fail to get partition checksum.", K(ret), K(pkey));
  } else if (items.count() > 0) {
    const ObSSTableMeta& data_sstable_meta = data_sstable.get_meta();
    if (REPLICA_TYPE_LOGONLY == replica_type) {
      if (0 == data_sstable_meta.data_checksum_) {
      } else {
        ret = OB_CHECKSUM_ERROR;
        STORAGE_LOG(ERROR, "data_checksum is not equal 0.", K(ret), K(replica_type), K(data_sstable_meta));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
        // only compare the replica which data_version equal with current data_sstable
        if (data_sstable.get_snapshot_version() == items.at(i).snapshot_version_) {
          if (REPLICA_TYPE_LOGONLY == items.at(i).replica_type_) {
            if (0 == items.at(i).data_checksum_) {
            } else {
              ret = OB_CHECKSUM_ERROR;
              STORAGE_LOG(ERROR, "data_checksum between replicas is not equal.", K(ret), K(items.at(i)));
            }
          } else if (data_sstable_meta.data_checksum_ != items.at(i).data_checksum_) {
            ret = OB_CHECKSUM_ERROR;
            STORAGE_LOG(
                ERROR, "data_checksum between replicas is not equal.", K(ret), K(items.at(i)), K(data_sstable_meta));
          }
        }
      }
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_MERGE_CHECKSUM) OB_SUCCESS;
    }
#endif
    if (OB_CHECKSUM_ERROR == ret) {
      int report_ret = OB_SUCCESS;
      if (OB_SUCCESS != (report_ret = report.report_merge_error(pkey, ret))) {
        STORAGE_LOG(ERROR, "Fail to submit checksum error task, ", K(report_ret));
      }
      // OB_CHECKSUM_ERROR gold bless you, dump memtable and minor sstable, continue to refresh __all_meta_table
      STORAGE_LOG(INFO, "begin to dump, ");
      dump2text(schema, base_tables, pkey);  // no need to check ret
    }
  } else {
    // first time insert into __all_meta_table, skip check
  }
}

void ObPartitionStorage::dump2text(
    const ObTableSchema& schema, common::ObIArray<storage::ObITable*>& base_tables, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  ObITable* table = NULL;
  ObSSTable* sstable = NULL;
  FILE* fd = NULL;
  int64_t file_size = 0;
  char real_dir[OB_MAX_FILE_NAME_LENGTH];
  char dump_mem_dir[OB_MAX_FILE_NAME_LENGTH];
  char dump_ss_dir[OB_MAX_FILE_NAME_LENGTH];
  char dump_mem_name[OB_MAX_FILE_NAME_LENGTH];
  char dump_ss_name[OB_MAX_FILE_NAME_LENGTH];
  if (OB_FAIL(FileDirectoryUtils::get_file_size("/proc/sys/kernel/core_pattern", file_size)) ||
      file_size > OB_MAX_FILE_NAME_LENGTH) {
    STORAGE_LOG(WARN, "fail to get size", K(ret));
  } else if (NULL == (fd = fopen("/proc/sys/kernel/core_pattern", "r"))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "open file fail:", K(ret));
  } else if (fread(real_dir, sizeof(char), file_size, fd) != file_size) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "read file fail:", K(ret), K(errno), KERRNOMSG(errno));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < base_tables.count(); i++) {
    if (NULL == (table = base_tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "The store not a valid store.", K(ret), K(i));
    } else {
      if (NULL != table && table->is_memtable()) {
        pret = snprintf(dump_mem_dir,
            OB_MAX_FILE_NAME_LENGTH,
            "%s/%s.%s.%ld.%s.%d.%s.%ld.%s.%d.%s.%d",
            dirname(real_dir),
            "dump_memtable",
            "part_id",
            pkey.get_partition_id(),
            "part_cnt",
            pkey.get_partition_cnt(),
            "table_id",
            pkey.get_table_id(),
            "major",
            table->get_version().major_,
            "minor",
            table->get_version().minor_);
        if (pret < 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "name too long", K(ret), K(dump_mem_dir));
        } else if (0 != mkdir(dump_mem_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
          if (EEXIST == errno) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "fail to mkdir.", K(ret), K(dump_ss_name), K(errno), KERRNOMSG(errno));
          }
        } else {
          pret = snprintf(dump_mem_name, OB_MAX_FILE_NAME_LENGTH, "%s/%s", dump_mem_dir, "memtable");
          if (pret < 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN, "name too long", K(ret), K(dump_mem_name));
          } else {
            ((ObMemtable*)table)->dump2text(dump_mem_name);  // no need to check ret
          }
        }
      }
    }
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < base_tables.count(); i++) {  // base sstore don't need to dump
    if (NULL == (table = base_tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "The store not a valid store.", K(ret), K(i));
    } else if (table->is_minor_sstable()) {
      sstable = static_cast<ObSSTable*>(table);
      pret = snprintf(dump_ss_dir,
          OB_MAX_FILE_NAME_LENGTH,
          "%s/%s.%s.%ld.%s.%d.%s.%ld.%s.%d.%s.%d",
          dirname(real_dir),
          "dump_sstable",
          "part_id",
          pkey.get_partition_id(),
          "part_cnt",
          pkey.get_partition_cnt(),
          "table_id",
          schema.get_table_id(),
          "major",
          sstable->get_version().major_,
          "minor",
          sstable->get_version().minor_);
      if (pret < 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "name too long", K(ret), K(dump_ss_dir));
      } else if (0 != mkdir(dump_ss_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
        if (EEXIST == errno) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "fail to mkdir.", K(ret), K(dump_ss_dir), K(errno), KERRNOMSG(errno));
        }
      } else {
        pret = snprintf(dump_ss_name, OB_MAX_FILE_NAME_LENGTH, "%s/%s", dump_ss_dir, "sstable");
        if (pret < 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "name too long", K(ret), K(dump_ss_name));
        } else {
          sstable->dump2text(dump_ss_dir, schema, dump_ss_name);  // no need to check ret
        }
      }
    }
  }
}

int ObPartitionStorage::lock(const ObStoreCtx& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

int ObPartitionStorage::check_index_need_build(const ObTableSchema& index_schema, bool& need_build)
{
  int ret = OB_SUCCESS;
  bool table_exist = false;
  need_build = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (!index_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(index_schema));
  } else if (OB_FAIL(schema_service_->check_table_exist(index_schema.get_table_id(),
                 OB_INVALID_VERSION, /*latest local guard*/
                 table_exist))) {
    STORAGE_LOG(
        WARN, "Fail to check if table exist, ", K_(pkey), "schema_version", index_schema.get_schema_version(), K(ret));
  } else if (OB_UNLIKELY(!table_exist)) {
    need_build = false;
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("The table does not exist, no need to create index, ", K(index_schema.get_table_id()));
  } else {
    need_build = index_schema.is_index_local_storage() && !index_schema.can_read_index() &&
                 INDEX_STATUS_UNAVAILABLE == index_schema.get_index_status();
    if (need_build) {
      ObIndexStatusTableOperator::ObBuildIndexStatus status;
      if (OB_FAIL(ObIndexStatusTableOperator::get_build_index_status(
              index_schema.get_table_id(), pkey_.get_partition_id(), GCTX.self_addr_, *GCTX.sql_proxy_, status))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get build index status", K(ret), K(pkey_), "index_id", index_schema.get_table_id());
        } else {
          ret = OB_SUCCESS;
          need_build = true;
        }
      } else {
        need_build = false;
      }
    }
  }
  return ret;
}

int ObPartitionStorage::get_build_index_stores(
    const ObTenantSchema& tenant_schema, compaction::ObBuildIndexParam& param)
{
  int ret = OB_SUCCESS;
  ObIndexTransStatus status;
  ObSimpleFrozenStatus frozen_status;
  int64_t freeze_version = 0;
  ObFreezeInfoProxy freeze_info_proxy;
  const bool allow_not_ready = false;
  const bool need_safety_check = true;
  ObTablesHandle tables_handle;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(tenant_schema));
  } else if (OB_FAIL(ObIndexTransStatusReporter::get_wait_trans_status(param.index_schema_->get_table_id(),
                 ObIndexTransStatusReporter::ROOT_SERVICE,
                 -1,
                 *GCTX.sql_proxy_,
                 status))) {
    STORAGE_LOG(
        WARN, "fail to get build index version", K(ret), K(pkey_), "index_id", param.index_schema_->get_table_id());
  } else if (OB_UNLIKELY(status.snapshot_version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, snapshot version is invalid", K(ret), K(status.snapshot_version_));
  } else if (OB_FAIL(store_.get_read_tables(param.index_schema_->get_data_table_id(),
                 status.snapshot_version_,
                 tables_handle,
                 allow_not_ready,
                 need_safety_check))) {
    if (OB_REPLICA_NOT_READABLE == ret) {
      ret = OB_EAGAIN;
    } else {
      STORAGE_LOG(WARN, "snapshot version has been discard", K(ret));
    }
  } else if (OB_FAIL(freeze_info_proxy.get_frozen_info_less_than(
                 *GCTX.sql_proxy_, status.snapshot_version_, frozen_status))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get next major freeze", K(ret), K(pkey_));
    } else {
      freeze_version = 1L;
      ret = OB_SUCCESS;
    }
  } else {
    freeze_version = frozen_status.frozen_version_;
  }

  // get local sort task ranges
  if (OB_SUCC(ret)) {
    ObSSTable* sstable = NULL;
    if (OB_FAIL(tables_handle.get_first_sstable(sstable))) {
      STORAGE_LOG(WARN, "fail to get last base store", K(ret));
    } else if (OB_ISNULL(sstable)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "sstable must not be NULL", K(ret));
    } else {
      ObExtStoreRange range;
      for (int64_t i = 0; OB_SUCC(ret) && i < param.concurrent_cnt_; ++i) {
        if (OB_FAIL(sstable->get_range(i, param.concurrent_cnt_, param.allocator_, range))) {
          STORAGE_LOG(WARN, "fail to get range", K(ret));
        } else if (OB_FAIL(param.local_sort_ranges_.push_back(range))) {
          STORAGE_LOG(WARN, "fail to push back local sort range", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    DEBUG_SYNC(BEFORE_UPDATE_INDEX_BUILD_VERSION);
    const ObTableSchema* dep_table_schema = param.dep_table_schema_;
    param.version_ = freeze_version;
    param.snapshot_version_ = status.snapshot_version_;
    param.checksum_method_ = CCM_VALUE_ONLY;
    if (NULL == dep_table_schema) {
      STORAGE_LOG(INFO,
          "succ to get build index param",
          K(param),
          "table_schema",
          *param.table_schema_,
          "index_schema",
          *param.index_schema_);
    } else {
      STORAGE_LOG(INFO,
          "succ to get build index param",
          K(param),
          "table_schema",
          *param.table_schema_,
          "index_schema",
          *param.index_schema_,
          "dep_table_schema",
          *dep_table_schema);
    }
  }

  return ret;
}

int ObPartitionStorage::get_build_index_context(const ObBuildIndexParam& param, ObBuildIndexContext& context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(context.preallocate_local_sorters(param.concurrent_cnt_))) {
    STORAGE_LOG(WARN, "fail to preallocate local sorters", K(ret));
  }
  return ret;
}

int ObPartitionStorage::generate_index_output_param(const ObTableSchema& data_table_schema,
    const ObTableSchema& index_schema, ObArray<ObColDesc>& col_ids, ObArray<ObColDesc>& org_col_ids,
    ObArray<int32_t>& output_projector, int64_t& unique_key_cnt)
{
  int ret = OB_SUCCESS;
  col_ids.reuse();
  output_projector.reuse();
  unique_key_cnt = 0;
  if (OB_UNLIKELY(!data_table_schema.is_valid() || !index_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(data_table_schema), K(index_schema));
  } else {
    // add data table rowkey
    const ObRowkeyInfo& rowkey_info = data_table_schema.get_rowkey_info();
    const ObRowkeyColumn* rowkey_column = NULL;
    ObColDesc col_desc;
    for (int32_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_ISNULL(rowkey_column = rowkey_info.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL", K(ret), K(i), K(rowkey_info));
      } else {
        col_desc.col_id_ = rowkey_column->column_id_;
        col_desc.col_type_ = rowkey_column->type_;
        col_desc.col_order_ = rowkey_column->order_;
        if (OB_FAIL(col_ids.push_back(col_desc))) {
          STORAGE_LOG(WARN, "fail to push back column desc", K(ret));
        }
      }
    }

    // add index table other columns
    ObArray<ObColDesc> index_table_columns;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(index_schema.get_column_ids(index_table_columns))) {
        STORAGE_LOG(WARN, "fail to get column ids", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < index_table_columns.count(); ++i) {
        const ObColDesc& index_col_desc = index_table_columns.at(i);
        int64_t j = 0;
        for (j = 0; OB_SUCC(ret) && j < col_ids.count(); ++j) {
          if (index_col_desc.col_id_ == col_ids.at(j).col_id_) {
            break;
          }
        }
        if (j == col_ids.count() && index_col_desc.col_id_ < OB_MIN_SHADOW_COLUMN_ID) {
          if (OB_FAIL(col_ids.push_back(index_col_desc))) {
            STORAGE_LOG(WARN, "fail to push back index col desc", K(ret));
          }
        }
      }
    }

    for (int64_t k = 0; OB_SUCC(ret) && k < index_schema.get_rowkey_column_num(); ++k) {
      if (index_table_columns.at(k).col_id_ >= OB_MIN_SHADOW_COLUMN_ID) {
        index_table_columns.at(k).col_id_ = index_table_columns.at(k).col_id_ - OB_MIN_SHADOW_COLUMN_ID;
      } else {
        ++unique_key_cnt;
      }
    }

    // generate output projector
    for (int64_t i = 0; OB_SUCC(ret) && i < index_table_columns.count(); ++i) {
      int64_t j = 0;
      for (j = 0; OB_SUCC(ret) && j < col_ids.count(); ++j) {
        if (col_ids.at(j).col_id_ == index_table_columns.at(i).col_id_) {
          break;
        }
      }
      if (j == col_ids.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "error unexpected, output col does not exist in index table columns", K(ret));
      } else if (OB_FAIL(output_projector.push_back(static_cast<int32_t>(j)))) {
        STORAGE_LOG(WARN, "fail to push back output projector", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(org_col_ids.assign(index_table_columns))) {
        STORAGE_LOG(WARN, "fail to assign col ids", K(ret));
      }
    }

    STORAGE_LOG(
        INFO, "output index projector", K(output_projector), K(index_table_columns), K(col_ids), K(unique_key_cnt));
  }

  return ret;
}

int ObPartitionStorage::local_sort_index_by_range(
    const int64_t idx, const ObBuildIndexParam& index_param, const ObBuildIndexContext& index_context)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObExtStoreRange range;
  ObArray<ObColDesc> col_ids;
  ObArray<ObColDesc> org_col_ids;
  ObArray<int32_t> output_projector;
  ObArray<int32_t> projector;
  const ObStoreRow* row = NULL;
  ObArray<int64_t> sort_column_indexes;
  ObTablesHandle tables_handle;
  ObSSTable* sstable = NULL;
  ObArenaAllocator allocator(ObModIds::OB_PARTITION_STORAGE);
  uint64_t tenant_id = OB_INVALID_ID;
  ObColumnChecksumCalculator checksum;
  int64_t row_count = 0;
  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);
  int comp_ret = OB_SUCCESS;
  ObStoreRowComparer comparer(comp_ret, sort_column_indexes);
  const int64_t file_buf_size = ObExternalSortConstant::DEFAULT_FILE_READ_WRITE_BUFFER;
  const int64_t expire_timestamp = 0;  // no time limited
  const ObTableSchema* table_schema = NULL;
  const ObTableSchema* index_schema = NULL;
  const ObTableSchema* dep_table = NULL;
  const int64_t concurrent_cnt = index_param.concurrent_cnt_;
  ObExternalSort<ObStoreRow, ObStoreRowComparer>* local_sort = NULL;
  int64_t unique_key_cnt = 0;
  int64_t macro_block_cnt = 0;
  ObCreateIndexKey key;
  ObCreateIndexScanTaskStat task_stat;
  int tmp_ret = OB_SUCCESS;
  int64_t index_table_size = 0;
  if (OB_UNLIKELY(!index_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(index_param));
  } else {
    table_schema = index_param.table_schema_;
    index_schema = index_param.index_schema_;
    dep_table = index_param.dep_table_schema_;
    local_sort = index_context.sorters_.at(idx);
    tenant_id = extract_tenant_id(table_schema->get_table_id());
    if (index_schema->is_materialized_view()) {
      query_flag.join_type_ = sql::LEFT_OUTER_JOIN;
    }
  }
  DEBUG_SYNC(BEFORE_BUILD_LOCAL_INDEX);

  if (OB_SUCC(ret)) {
    const bool allow_not_ready = false;
    const bool need_safety_check = true;
    if (OB_FAIL(store_.get_read_tables(index_param.index_schema_->get_data_table_id(),
            index_param.snapshot_version_,
            tables_handle,
            allow_not_ready,
            need_safety_check))) {
      if (OB_REPLICA_NOT_READABLE == ret) {
        ret = OB_EAGAIN;
      } else {
        STORAGE_LOG(WARN, "snapshot version has been discard", K(ret));
      }
    } else if (OB_FAIL(tables_handle.get_first_sstable(sstable))) {
      STORAGE_LOG(WARN, "fail to get last base store", K(ret));
    } else if (OB_ISNULL(sstable)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "sstable must not be NULL", K(ret));
    } else {
      if (OB_FAIL(index_param.local_sort_ranges_.at(idx, range))) {
        STORAGE_LOG(WARN, "fail to get range", K(ret));
      } else {
        macro_block_cnt = sstable->get_macro_block_count() / concurrent_cnt;
        key.index_table_id_ = index_schema->get_table_id();
        key.partition_id_ = pkey_.get_partition_id();
        key.tenant_id_ = extract_tenant_id(key.index_table_id_);
        task_stat.type_ = ObILongOpsTaskStat::TaskType::SCAN;
        task_stat.task_id_ = idx;
        task_stat.state_ = ObILongOpsTaskStat::TaskState::RUNNING;
        task_stat.macro_count_ = macro_block_cnt;
        if (OB_SUCCESS != (tmp_ret = key.to_key_string())) {
          LOG_WARN("fail to key string", K(ret));
        } else if (OB_SUCCESS != (tmp_ret = LONG_OPS_MONITOR_INSTANCE.update_task_stat(key, task_stat))) {
          STORAGE_LOG(WARN, "fail to update task stat", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    col_ids.reuse();
    sort_column_indexes.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_schema->get_rowkey_column_num(); ++i) {
      if (OB_FAIL(sort_column_indexes.push_back(i))) {
        STORAGE_LOG(WARN, "Fail to push sort column indexes, ", K(ret), K(i));
      }
    }
  }

  STORAGE_LOG(
      INFO, "start to local_sort_index_by_range", "index_id", index_schema->get_table_id(), K(idx), K(concurrent_cnt));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(SLOGGER.begin(OB_LOG_CS_DAILY_MERGE))) {
    STORAGE_LOG(WARN, "fail to begin transaction", K(ret));
  } else if (OB_FAIL(local_sort->init(
                 index_param.DEFAULT_INDEX_SORT_MEMORY_LIMIT, file_buf_size, expire_timestamp, tenant_id, &comparer))) {
    STORAGE_LOG(WARN, "Fail to init external sort, ", K(ret));
  } else if (OB_FAIL(comp_ret)) {
    STORAGE_LOG(ERROR, "comp_ret must not fail", K(ret));
  } else if (OB_FAIL(generate_index_output_param(
                 *table_schema, *index_schema, col_ids, org_col_ids, projector, unique_key_cnt))) {
    STORAGE_LOG(WARN, "Fail to get column ids, ", K(ret));
  } else {
    ObObj* cells_buf = NULL;
    ObStoreRow default_row;
    ObStoreRow tmp_row;

    if (NULL == (cells_buf = reinterpret_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * org_col_ids.count() * 2)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "failed to alloc cells buf", K(ret), K(org_col_ids.count()));
    } else {
      cells_buf = new (cells_buf) ObObj[org_col_ids.count() * 2];
      default_row.flag_ = ObActionFlag::OP_ROW_EXIST;
      default_row.row_val_.cells_ = cells_buf;
      default_row.row_val_.count_ = org_col_ids.count();
      tmp_row.flag_ = ObActionFlag::OP_ROW_EXIST;
      tmp_row.row_val_.cells_ = cells_buf + org_col_ids.count();
      tmp_row.row_val_.count_ = org_col_ids.count();
    }

    // extend col_ids for generated column
    ObArray<ObColDesc> extended_col_ids;
    ObArray<ObColDesc> org_extended_col_ids;
    ObArray<ObISqlExpression*> dependent_exprs;
    ObExprCtx expr_ctx;
    if (OB_SUCC(ret)) {
      ObArray<ObColDesc> index_table_columns;
      if (OB_FAIL(index_schema->get_orig_default_row(org_col_ids, default_row.row_val_))) {
        STORAGE_LOG(WARN, "Fail to get default row from table schema, ", K(ret));
      } else if (OB_FAIL(append(extended_col_ids, col_ids))) {
        STORAGE_LOG(ERROR, "failed to clone col_ids", K(ret), K(col_ids), K(extended_col_ids));
      } else if (OB_FAIL(append(org_extended_col_ids, org_col_ids))) {
        STORAGE_LOG(WARN, "fail to append col array", K(ret));
      } else if (OB_FAIL(append(output_projector, projector))) {
        STORAGE_LOG(WARN, "fail to clone output projector", K(ret));
      } else {
        const ObTableSchema* data_table_schema = NULL;
        const ObColumnSchemaV2* column_schema = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < org_col_ids.count(); ++i) {
          data_table_schema = NULL;
          column_schema = NULL;
          if (org_col_ids.at(i).col_id_ < OB_APP_MIN_COLUMN_ID) {
            // do nothing
          } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(org_col_ids.at(i).col_id_))) {
            if (NULL == dep_table || !index_schema->is_depend_column(org_col_ids.at(i).col_id_) ||
                OB_ISNULL(
                    column_schema = dep_table->get_column_schema(org_col_ids.at(i).col_id_ - OB_MIN_MV_COLUMN_ID))) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "The column schema is NULL, ", K(ret), K(org_col_ids.at(i).col_id_));
            } else {
              data_table_schema = dep_table;
            }
          } else {
            data_table_schema = table_schema;
          }

          if (OB_SUCC(ret)) {
            if (NULL != column_schema && column_schema->is_generated_column()) {
              ObSEArray<uint64_t, 8> ref_columns;
              if (OB_FAIL(column_schema->get_cascaded_column_ids(ref_columns))) {
                STORAGE_LOG(WARN, "Failed to get cascaded column ids", K(ret));
              } else {
                ObColDesc col;
                const ObColumnSchemaV2* data_column_schema = NULL;
                for (int64_t j = 0; OB_SUCC(ret) && j < ref_columns.count(); ++j) {
                  col.reset();
                  if (OB_ISNULL(data_column_schema = table_schema->get_column_schema(ref_columns.at(j)))) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN, "The column schema is NULL, ", K(ret));
                  } else {
                    col.col_id_ = ref_columns.at(j);
                    col.col_type_ = data_column_schema->get_meta_type();
                    col.col_order_ = data_column_schema->get_order_in_rowkey();
                    if (OB_FAIL(org_extended_col_ids.push_back(col))) {
                      STORAGE_LOG(WARN, "fail to push back col id", K(ret));
                    } else {
                      int64_t k = 0;
                      for (k = 0; OB_SUCC(ret) && k < extended_col_ids.count(); ++k) {
                        if (extended_col_ids.at(k).col_id_ == col.col_id_) {
                          break;
                        }
                      }

                      if (k == extended_col_ids.count()) {
                        if (OB_FAIL(extended_col_ids.push_back(col))) {
                          STORAGE_LOG(WARN, "push back error", K(ret));
                        }
                      }
                      if (OB_SUCC(ret)) {
                        if (OB_FAIL(output_projector.push_back(static_cast<int32_t>(k)))) {
                          STORAGE_LOG(WARN, "fail to push back output projector", K(ret));
                        }
                      }
                    }
                  }
                }
              }
              if (OB_SUCC(ret)) {  // make sql expression for generated column
                ObISqlExpression* expr = NULL;
                if (OB_FAIL(sql::ObSQLUtils::make_generated_expression_from_str(
                        column_schema->get_orig_default_value().get_string(),
                        *data_table_schema,
                        *column_schema,
                        org_extended_col_ids,
                        allocator,
                        expr))) {
                  STORAGE_LOG(WARN,
                      "failed to make sql expression",
                      K(default_row.row_val_.cells_[i].get_string()),
                      K(*data_table_schema),
                      K(*column_schema),
                      K(extended_col_ids),
                      K(ret));
                } else if (OB_FAIL(dependent_exprs.push_back(expr))) {
                  STORAGE_LOG(WARN, "push back error", K(ret));
                } else { /*do nothing*/
                }
              }
            } else {
              if (OB_FAIL(dependent_exprs.push_back(NULL))) {
                STORAGE_LOG(WARN, "push back error", K(ret));
              }
            }
          }
        }
      }
    }
    STORAGE_LOG(INFO, "output projector", K(extended_col_ids), K(output_projector));

    ObArray<ObColumnParam*> col_params;
    for (int64_t i = 0; OB_SUCC(ret) && i < extended_col_ids.count(); i++) {
      const ObColumnSchemaV2* col = index_schema->get_column_schema(extended_col_ids.at(i).col_id_);
      if (NULL == col) {
        // generated column's depend column, get column schema from data table
        col = table_schema->get_column_schema(extended_col_ids.at(i).col_id_);
      }
      if (NULL == col) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column schema failed", K(ret));
      } else {
        void* buf = allocator.alloc(sizeof(ObColumnParam));
        if (NULL == buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret));
        } else {
          ObColumnParam* col_param = new (buf) ObColumnParam(allocator);
          if (OB_FAIL(ObTableParam::convert_column_schema_to_param(*col, *col_param))) {
            LOG_WARN("convert schema column to column parameter failed", K(ret));
          } else if (OB_FAIL(col_params.push_back(col_param))) {
            LOG_WARN("push to array failed", K(ret));
          }
        }
      }
    }

    ObTableAccessParam access_param;
    ObTableAccessContext access_ctx;
    ObBlockCacheWorkingSet block_cache_ws;
    ObStoreCtx ctx;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(access_param.out_col_desc_param_.init())) {
        LOG_WARN("init out cols fail", K(ret));
      } else if (OB_FAIL(access_param.out_col_desc_param_.assign(extended_col_ids))) {
        LOG_WARN("assign out cols fail", K(ret));
      } else {
        access_param.iter_param_.table_id_ = table_schema->get_table_id();
        access_param.iter_param_.rowkey_cnt_ = table_schema->get_rowkey_column_num();
        access_param.iter_param_.schema_version_ = table_schema->get_schema_version();
        access_param.iter_param_.out_cols_project_ = &output_projector;
        access_param.iter_param_.out_cols_ = &access_param.out_col_desc_param_.get_col_descs();
        access_param.out_cols_param_ = &col_params;
        access_param.reserve_cell_cnt_ = output_projector.count();
      }
    }

    if (OB_SUCC(ret)) {
      common::ObVersionRange trans_version_range;
      trans_version_range.snapshot_version_ = index_param.snapshot_version_;
      trans_version_range.multi_version_start_ = index_param.snapshot_version_;
      trans_version_range.base_version_ = 0;
      ObPartitionKey pg_key;

      if (OB_ISNULL(ctx.mem_ctx_ = txs_->get_mem_ctx_factory()->alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "fail to allocate transaction memory context", K(ret));
      } else if (OB_FAIL(ctx.mem_ctx_->trans_begin())) {
        STORAGE_LOG(WARN, "fail to begin transaction", K(ret));
      } else if (OB_FAIL(
                     ctx.mem_ctx_->sub_trans_begin(index_param.snapshot_version_, BUILD_INDEX_READ_SNAPSHOT_VERSION))) {
        STORAGE_LOG(WARN, "fail to begin sub transaction", K(ret), K(index_param));
      } else if (OB_FAIL(block_cache_ws.init(tenant_id))) {
        LOG_WARN("block_cache_ws init failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(checksum.init(org_col_ids.count()))) {
        STORAGE_LOG(WARN, "fail to init checksum calculator", K(ret));
      } else if (OB_FAIL(ObPartitionService::get_instance().get_pg_key(pkey_, pg_key))) {
        LOG_WARN("failed to get_pg_key", K(ret), K(pkey_));
      } else if (OB_FAIL(ctx.init_trans_ctx_mgr(pg_key))) {
        LOG_WARN("failed to init_trans_ctx_mgr", K(ret), K(pg_key));
      } else if (OB_FAIL(access_ctx.init(query_flag, ctx, allocator, allocator, block_cache_ws, trans_version_range))) {
        LOG_WARN("failed to init accesss ctx", K(ret));
      }
    }

    ObStoreCtx rctx;
    ObTablesHandle rtables_handle;
    ObTableAccessParam rta_param;
    ObTableAccessContext rta_ctx;
    ObGetTableParam get_table_param;
    ObIStoreRowIterator* row_iter = NULL;
    ObMultipleScanMerge* scan_merge = NULL;
    void* buf = NULL;
    ObTableParam rt_param(allocator);
    get_table_param.partition_store_ = &store_;

    STORAGE_LOG(INFO, "start do output_store.scan");
    if (1 == idx && !index_param.table_schema_->is_sys_table()) {
      DEBUG_SYNC(BEFORE_BUILD_LOCAL_INDEX_SCAN);
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(range.to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
      STORAGE_LOG(WARN, "fail to get to collation free range", K(ret));
    } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMultipleScanMerge)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory for ObMultipleScanMerge", K(ret));
    } else if (OB_ISNULL(scan_merge = new (buf) ObMultipleScanMerge())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "fail to do placement new", K(ret));
    } else if (OB_FAIL(scan_merge->init(access_param, access_ctx, get_table_param))) {
      STORAGE_LOG(WARN, "fail to init scan merge", K(ret), K(access_param), K(access_ctx));
    } else if (OB_FAIL(scan_merge->open(range))) {
      STORAGE_LOG(WARN, "fail to open scan merge", K(ret));
    } else {
      scan_merge->disable_padding();
      scan_merge->disable_fill_default();
      scan_merge->disable_fill_virtual_column();
      row_iter = scan_merge;
    }

    if (OB_SUCC(ret)) {
      ObArenaAllocator calc_buf(ObModIds::OB_SQL_EXPR_CALC);
      int64_t get_next_row_time = 0;
      int64_t trans_time = 0;
      int64_t add_item_time = 0;
      int64_t t1 = 0;
      int64_t t2 = 0;
      int64_t t3 = 0;
      int64_t t4 = 0;
      if (OB_FAIL(sql::ObSQLUtils::make_default_expr_context(allocator, expr_ctx))) {
        STORAGE_LOG(WARN, "failed to make default expr context ", K(ret));
      }
      tables_handle.reset();
      DEBUG_SYNC(BEFORE_BUILD_LOCAL_INDEX_REFRESH_TABLES);
      STORAGE_LOG(INFO, "start do next row", K(col_ids), K(default_row.row_val_));
      // TODO(): replace this with global function
      ObWorker::CompatMode sql_mode;
      if (OB_SUCC(ret)) {
        sql_mode = THIS_WORKER.get_compatibility_mode();
      }
      while (OB_SUCC(ret)) {
        t1 = ObTimeUtility::current_time();
        calc_buf.reuse();
        dag_yield();
        if (OB_FAIL(row_iter->get_next_row(row))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "Fail to get next row, ", K(ret));
          }
        } else {
          t2 = ObTimeUtility::current_time();
          get_next_row_time += t2 - t1;
          DEBUG_SYNC(BEFORE_BUILD_LOCAL_INDEX_REFRESH_TABLES_MID);
          for (int64_t k = 0; OB_SUCC(ret) && k < org_col_ids.count(); ++k) {
            if (row->row_val_.cells_[k].is_nop_value()) {
              if (NULL != dependent_exprs.at(k)) {  // generated column
                if (OB_FAIL(sql::ObSQLUtils::calc_sql_expression(dependent_exprs.at(k),
                        *table_schema,
                        org_extended_col_ids,
                        row->row_val_,
                        calc_buf,
                        expr_ctx,
                        tmp_row.row_val_.cells_[k]))) {
                  STORAGE_LOG(
                      WARN, "failed to calc expr", K(row->row_val_), K(org_col_ids), K(dependent_exprs.at(k)), K(ret));
                }
              } else {
                tmp_row.row_val_.cells_[k] = default_row.row_val_.cells_[k];
              }
            } else {
              tmp_row.row_val_.cells_[k] = row->row_val_.cells_[k];
            }
          }

          // process unique index shadow columns
          if (OB_SUCC(ret) && index_schema->is_unique_index()) {
            const int64_t shadow_column_cnt = index_schema->get_rowkey_column_num() - unique_key_cnt;
            if (OB_FAIL(ObUniqueIndexRowTransformer::convert_to_unique_index_row(tmp_row.row_val_,
                    static_cast<ObCompatibilityMode>(sql_mode),
                    unique_key_cnt,
                    shadow_column_cnt,
                    NULL,
                    tmp_row.row_val_))) {
              LOG_WARN("fail to convert to unique index row", K(ret));
            }
          }

          // process fulltext
          if (OB_SUCC(ret)) {
            if (index_schema->is_domain_index()) {
              int64_t t3_1 = 0;
              int64_t t3_2 = 0;
              ObSEArray<ObObj, 32> words;
              ObObj orig_obj;
              uint64_t domain_column_id = OB_INVALID_ID;
              if (OB_FAIL(index_schema->get_index_info().get_fulltext_column(domain_column_id))) {
                STORAGE_LOG(WARN, "failed to get domain column id", K(index_schema->get_index_info()), K(ret));
              } else {
                int64_t pos = -1;
                for (int64_t i = 0; OB_SUCC(ret) && -1 == pos && i < org_col_ids.count(); ++i) {
                  if (domain_column_id == org_col_ids.at(i).col_id_) {
                    pos = i;
                  }
                }
                if (OB_SUCC(ret)) {
                  ObSEArray<ObString, 8> words;
                  ObObj orig_obj = tmp_row.row_val_.cells_[pos];
                  ObString orig_string = orig_obj.get_string();
                  if (-1 == pos) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN, "domain index has no domain column", K(domain_column_id), K(org_col_ids), K(ret));
                  } else if (OB_FAIL(split_on(orig_string, ',', words))) {
                    STORAGE_LOG(WARN, "failed to split string", K(orig_string), K(ret));
                  } else {
                    t3 = ObTimeUtility::current_time();
                    trans_time += t3 - t2;
                    for (int64_t i = 0; OB_SUCC(ret) && i < words.count(); ++i) {
                      t3_1 = ObTimeUtility::current_time();
                      tmp_row.row_val_.cells_[pos].set_string(tmp_row.row_val_.cells_[pos].get_type(), words.at(i));
                      t3_2 = ObTimeUtility::current_time();
                      trans_time += t3_2 - t3_1;
                      if (OB_FAIL(local_sort->add_item(tmp_row))) {
                        STORAGE_LOG(WARN, "Fail to add item to sort, ", K(ret));
                      }
                      ++row_count;
                      index_table_size += tmp_row.get_serialize_size();
                      t4 = ObTimeUtility::current_time();
                      add_item_time += t4 - t3_2;
                    }
                    // recover original data
                    tmp_row.row_val_.cells_[pos] = orig_obj;
                  }
                }
              }
            } else {
              t3 = ObTimeUtility::current_time();
              if (OB_FAIL(local_sort->add_item(tmp_row))) {
                STORAGE_LOG(WARN, "Fail to add item to sort, ", K(ret));
              }
              ++row_count;
              t4 = ObTimeUtility::current_time();
              trans_time += t3 - t2;
              add_item_time += t4 - t3;
              index_table_size += tmp_row.get_serialize_size();
            }
          }
          // calculate main table checksum
          if (OB_SUCC(ret)) {
            if (OB_FAIL(checksum.calc_column_checksum(index_param.checksum_method_, &tmp_row, NULL, NULL))) {
              STORAGE_LOG(WARN, "fail to calc column checksum", K(ret));
            }
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      STORAGE_LOG(INFO,
          "print prepare build index cost time",
          K(get_next_row_time),
          K(trans_time),
          K(add_item_time),
          K(org_col_ids),
          K(ObArrayWrap<int64_t>(checksum.get_column_checksum(), org_col_ids.count())));
    }

    task_stat.state_ =
        (OB_SUCCESS == ret) ? ObILongOpsTaskStat::TaskState::SUCCESS : ObILongOpsTaskStat::TaskState::FAIL;
    if (OB_SUCCESS != (tmp_ret = LONG_OPS_MONITOR_INSTANCE.update_task_stat(key, task_stat))) {
      STORAGE_LOG(WARN, "fail to update task stat", K(tmp_ret), K(key));
    }

    if (NULL != scan_merge) {
      scan_merge->~ObMultipleScanMerge();
      scan_merge = NULL;
    }
    if (NULL != rctx.mem_ctx_) {
      rctx.mem_ctx_->trans_end(true, 0);
      rctx.mem_ctx_->trans_clear();
      txs_->get_mem_ctx_factory()->free(rctx.mem_ctx_);
      rctx.mem_ctx_ = NULL;
    }
    if (NULL != ctx.mem_ctx_) {
      ctx.mem_ctx_->trans_end(true, 0);
      ctx.mem_ctx_->trans_clear();
      txs_->get_mem_ctx_factory()->free(ctx.mem_ctx_);
      ctx.mem_ctx_ = NULL;
    }
    sql::ObSQLUtils::destruct_default_expr_context(expr_ctx);

    if (OB_SUCC(ret)) {
      const bool is_final_merge = false;
      int64_t storage_log_seq_num = 0;
      const int64_t index_macro_cnt = index_table_size / OB_FILE_SYSTEM.get_macro_block_size();
      ObCreateIndexSortTaskStat sort_task_stat;
      sort_task_stat.task_id_ = concurrent_cnt + idx;
      sort_task_stat.type_ = ObILongOpsTaskStat::TaskType::SORT;
      sort_task_stat.state_ = ObILongOpsTaskStat::TaskState::RUNNING;
      sort_task_stat.macro_count_ = index_macro_cnt;
      sort_task_stat.run_count_ = MAX(1L, static_cast<int64_t>(std::log(index_macro_cnt) / std::log(31)));
      const_cast<ObBuildIndexContext&>(index_context).add_index_macro_cnt(index_macro_cnt);
      if (OB_SUCCESS != (tmp_ret = LONG_OPS_MONITOR_INSTANCE.update_task_stat(key, sort_task_stat))) {
        STORAGE_LOG(WARN, "fail to update task stat", K(ret));
      }
      if (OB_FAIL(local_sort->do_sort(is_final_merge))) {
        STORAGE_LOG(WARN, "Fail to do sort, ", K(ret));
      } else if (OB_FAIL(
                     const_cast<ObBuildIndexContext&>(index_context)
                         .add_main_table_checksum(checksum.get_column_checksum(), row_count, org_col_ids.count()))) {
        STORAGE_LOG(WARN, "fail to add main table checksum", K(ret));
      } else if (OB_FAIL(SLOGGER.commit(storage_log_seq_num))) {
        STORAGE_LOG(WARN, "fail to commit transaction", K(ret));
      }
      sort_task_stat.state_ =
          (OB_SUCCESS == ret) ? ObILongOpsTaskStat::TaskState::SUCCESS : ObILongOpsTaskStat::TaskState::FAIL;
      if (OB_SUCCESS != (tmp_ret = LONG_OPS_MONITOR_INSTANCE.update_task_stat(key, sort_task_stat))) {
        STORAGE_LOG(WARN, "fail to update task stat", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
      STORAGE_LOG(WARN, "fail to abort transaction", K(tmp_ret));
    }
  }

  int64_t cost_time = ObTimeUtility::current_time() - start_time;
  STORAGE_LOG(INFO,
      "finish local_sort_index_by_range",
      K(ret),
      "table_id",
      index_schema->get_table_id(),
      K(cost_time),
      K(idx),
      K(concurrent_cnt),
      K(row_count));
  return ret;
}

int ObPartitionStorage::prepare_merge_mv_depend_sstable(const common::ObVersion& frozen_version,
    const share::schema::ObTableSchema* schema, const share::schema::ObTableSchema* dep_schema,
    ObTablesHandle& mv_dep_tables_handle)
{
  int ret = OB_SUCCESS;
  bool table_exist = false;
  ObPartitionStorage* dep_storage = NULL;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  const int64_t snapshot_version = ObVersionRange::MAX_VERSION;  // TODO: fill it

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("table schema must not null", K(ret), K(pkey_));
  } else if (!schema->is_materialized_view()) {
    // not mv,  do nothing
  } else if (OB_ISNULL(dep_schema)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("for mv merge, dep_schema must not null", K(ret), K(frozen_version), K(*schema));
  } else if (dep_schema->get_partition_num() != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("partitioned join mv depend table not supported", K(ret), K(*dep_schema));
  } else if (OB_FAIL(schema_service_->check_table_exist(schema->get_table_id(),
                 OB_INVALID_VERSION, /*latest local guard*/
                 table_exist))) {
    STORAGE_LOG(WARN,
        "failed to check table exist",
        K(ret),
        "table_id",
        schema->get_table_id(),
        "schema_version",
        schema->get_schema_version());
  } else if (OB_UNLIKELY(!table_exist)) {
    LOG_INFO("skip merge not exist table", K(ret), K(pkey_), K(schema->get_table_id()));
  } else {
    const uint64_t dep_table_id = dep_schema->get_table_id();
    ObPartitionKey rpkey(dep_table_id, 0, dep_schema->get_partition_cnt());
    if (OB_FAIL(GCTX.par_ser_->get_partition(rpkey, guard)) || OB_ISNULL(guard.get_partition_group())) {
      LOG_WARN("get partition failed", K(ret), K(rpkey));
    } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(rpkey, pg_partition_guard)) ||
               OB_ISNULL(pg_partition_guard.get_pg_partition())) {
      LOG_WARN("get pg partition failed", K(ret), K(rpkey));
    } else if (NULL ==
               (dep_storage = static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get partition storage failed", K(ret));
    } else if (!ObReplicaTypeCheck::is_readable_replica(dep_storage->get_replica_type())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("dependent table of MV is not readable",
          K(ret),
          "table id",
          schema->get_table_id(),
          "dependent table id",
          dep_table_id,
          "dependent table type",
          dep_storage->get_replica_type());
    } else if (OB_FAIL(dep_storage->get_partition_store().get_read_tables(
                   dep_table_id, snapshot_version, mv_dep_tables_handle))) {
      LOG_WARN("failed to get read tables", K(ret), K(frozen_version), K(dep_table_id));
    } else if (mv_dep_tables_handle.get_tables().count() < 1) {
      ret = OB_ERR_SYS;
      LOG_ERROR("dep_tables must not zero", K(ret), K(snapshot_version), K(dep_table_id));
    } else {
      ObITable* end_table = mv_dep_tables_handle.get_tables().at(mv_dep_tables_handle.get_tables().count() - 1);
      if (OB_ISNULL(end_table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("end table must not null", K(ret), K(frozen_version), K(dep_table_id));
      } else if (end_table->is_memtable() && static_cast<ObMemtable*>(end_table)->is_active_memtable()) {
        // TODO: this needs to be modified after daily merge with global version. Including global version is enough.
        ret = OB_EAGAIN;
        LOG_WARN("dep table is not frozen, cannot merge", K(ret), K(frozen_version), K(dep_table_id), K(*end_table));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::join_mv_init_merge_param(const share::schema::ObTableSchema& ltable,
    const share::schema::ObTableSchema& rtable, const ObTableAccessParam& lta_param,
    const ObTableAccessContext& lta_ctx, ObTableParam& rt_param, ObTableAccessParam& rta_param,
    ObTableAccessContext& rta_ctx, ObStoreCtx& rctx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    rctx.mem_ctx_ = txs_->get_mem_ctx_factory()->alloc();
    if (NULL == rctx.mem_ctx_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (OB_FAIL(rctx.mem_ctx_->trans_begin())) {
      LOG_WARN("failed to begin transaction", K(ret));
    } else if (OB_FAIL(rctx.mem_ctx_->sub_trans_begin(
                   MV_RIGHT_MERGE_READ_SNAPSHOT_VERSION, MV_RIGHT_MERGE_READ_SNAPSHOT_VERSION))) {
      LOG_WARN("failed to begin sub transaction", K(ret));
    } else {
      ObArray<uint64_t> col_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < lta_param.out_col_desc_param_.count(); ++i) {
        if (i >= lta_param.iter_param_.out_cols_project_->count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "invalid out col projector idx", K(ret), K(i), "count", lta_param.iter_param_.out_cols_project_->count());
        } else {
          const int64_t idx = lta_param.iter_param_.out_cols_project_->at(i);
          if (OB_FAIL(col_ids.push_back(lta_param.out_col_desc_param_.get_col_descs().at(idx).col_id_))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(rt_param.convert_join_mv_rparam(ltable, rtable, col_ids))) {
        LOG_WARN("convert to join mv param failed", K(ret));
      } else {
        ObColDescArray tmp_rt_out_cols;
        ObColDesc desc;
        FOREACH_CNT_X(c, rt_param.get_columns(), OB_SUCC(ret))
        {
          desc.col_id_ = (*c)->get_column_id();
          desc.col_type_ = (*c)->get_meta_type();
          desc.col_order_ = (*c)->get_column_order();
          if (OB_FAIL(tmp_rt_out_cols.push_back(desc))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }

        // TODO hard code, should be passed as parameters
        ObVersionRange trans_version_range;
        trans_version_range.base_version_ = 0;
        trans_version_range.multi_version_start_ = MV_RIGHT_MERGE_READ_SNAPSHOT_VERSION;
        trans_version_range.snapshot_version_ = MV_RIGHT_MERGE_READ_SNAPSHOT_VERSION;

        ObPartitionKey pg_key;

        if (!trans_version_range.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("trans version range is not valid", K(ret), K(trans_version_range));
        } else if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(lta_ctx.block_cache_ws_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null block cache working set", K(ret));
        } else if (OB_FAIL(ObPartitionService::get_instance().get_pg_key(pkey_, pg_key))) {
          LOG_WARN("failed to get_pg_key", K(ret), K(pkey_));
        } else if (OB_FAIL(rctx.init_trans_ctx_mgr(pg_key))) {
          LOG_WARN("failed to init_trans_ctx_mgr", K(ret), K(pg_key));
        } else if (OB_FAIL(rta_ctx.init(lta_ctx.query_flag_,
                       rctx,
                       *lta_ctx.allocator_,
                       *lta_ctx.stmt_allocator_,
                       *lta_ctx.block_cache_ws_,
                       trans_version_range))) {
          LOG_WARN("init right table access context failed", K(ret));
        } else if (OB_FAIL(rta_param.init(rtable.get_table_id(),
                       rtable.get_schema_version(),
                       rtable.get_rowkey_column_num(),
                       tmp_rt_out_cols,
                       false, /*is_multi_version_minor_merge*/
                       &rt_param,
                       true /* is_mv_right_table */))) {
          LOG_WARN("init right table access param failed", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret) && NULL != rctx.mem_ctx_) {
    rctx.mem_ctx_->trans_end(true, 0);
    rctx.mem_ctx_->trans_clear();
    txs_->get_mem_ctx_factory()->free(rctx.mem_ctx_);
    rctx.mem_ctx_ = NULL;
  }
  return ret;
}

int ObPartitionStorage::retire_warmup_store(const bool is_disk_full)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K_(pkey), K(ret));
  } else if (OB_FAIL(store_.retire_prewarm_store(is_disk_full))) {
    STORAGE_LOG(WARN, "fail to retire sorted store", K_(pkey), K(ret));
  }
  return ret;
}

int ObPartitionStorage::halt_prewarm()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K_(pkey), K(ret));
  } else if (OB_FAIL(store_.halt_prewarm())) {
    STORAGE_LOG(WARN, "fail to halt prewarm", K_(pkey), K(ret));
  }
  return ret;
}

int ObPartitionStorage::get_partition_ss_store_info(common::ObArray<PartitionSSStoreInfo>& partition_ss_store_info_list)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;
  PartitionSSStoreInfo info;
  ObArray<ObSSTable*> sstables;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_FAIL(store_.get_all_tables(tables_handle))) {
    STORAGE_LOG(WARN, "Fail to get all ssstores", K(ret));
  } else if (OB_FAIL(tables_handle.get_all_sstables(sstables))) {
    STORAGE_LOG(WARN, "failed to get all sstables", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); ++i) {
      ObSSTable* sstable = sstables.at(i);
      info.reset();
      if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "The base ssstore is NULL", K(ret));
      } else if (sstable->is_major_sstable()) {
        const ObSSTableMergeInfo& merge_info = sstable->get_sstable_merge_info();

        info.version_ = sstable->get_version();
        info.macro_block_count_ = sstable->get_meta().get_total_macro_block_count();
        info.use_old_macro_block_count_ = sstable->get_meta().get_total_use_old_macro_block_count();
        info.rewrite_macro_old_micro_block_count_ = merge_info.rewrite_macro_old_micro_block_count_;
        info.rewrite_macro_total_micro_block_count_ = merge_info.rewrite_macro_total_micro_block_count_;
        info.is_merged_ = true;
        info.is_modified_ = info.macro_block_count_ != info.use_old_macro_block_count_;

        if (OB_FAIL(partition_ss_store_info_list.push_back(info))) {
          STORAGE_LOG(WARN, "Fail to add info", K(ret));
        }
      }
    }
  }
  return ret;
}

/*
int ObPartitionStorage::get_sstore_min_version(common::ObVersion &min_version)
{
  int ret = OB_SUCCESS;
  ObSSStore *store = NULL;
  ObTablesHandle tables_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_FAIL(get_ssstore_max_version(min_version))) {
    STORAGE_LOG(WARN, "Fail to get max ssstore version", K(ret));
  } else if (OB_FAIL(stores_.get_all_ssstores(stores_handle))) {
    STORAGE_LOG(WARN, "Fail to get all ssstores", K(ret));
  } else {
    const ObIArray<ObIStore*> &ssstores = stores_handle.get_stores();
    for (int64_t i = 0; OB_SUCC(ret) && i < ssstores.count(); ++i) {
      if (is_major_ssstore(ssstores.at(i)->get_store_type())) {
        store = static_cast<ObSSStore *>(ssstores.at(i));
      }
      if (NULL == store) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "The base ssstore is NULL", K(ret));
      } else if (min_version > store->get_version()){
        min_version = store->get_version();
      } else {}
    }
  }
  return ret;
}
*/

int ObPartitionStorage::do_warm_up_request(const ObIWarmUpRequest* request)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;
  const int64_t snapshot_version = WARM_UP_READ_SNAPSHOT_VERSION;
  EVENT_INC(WARM_UP_REQUEST_COUNT);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (NULL == request) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(request));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(store_.get_replica_type())) {
    // without ssstore, skip warm up
  } else if (OB_FAIL(store_.get_read_tables(request->get_index_id(), snapshot_version, tables_handle))) {
    STORAGE_LOG(WARN, "Fail to get read stores, ", K(ret));
  } else if (OB_FAIL(request->warm_up(txs_->get_mem_ctx_factory(), tables_handle.get_tables()))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to warm up request, ", K_(pkey), K(ret));
    }
  } else {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      STORAGE_LOG(INFO, "Success to do warm up request, ", K_(pkey));
    }
  }
  return ret;
}

int ObPartitionStorage::ObDMLRunningCtx::init(const ObIArray<uint64_t>* column_ids,
    const ObIArray<uint64_t>* upd_col_ids, const bool use_table_param, ObMultiVersionSchemaService* schema_service,
    ObPartitionStore& store)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(schema_service) || OB_UNLIKELY(!store_ctx_.is_valid()) || OB_UNLIKELY(!dml_param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(store_ctx_.mem_ctx_), K(dml_param_), KP(schema_service));
  } else if (use_table_param) {
    relative_tables_.set_table_param(dml_param_.table_param_);
  }
  const ObPartitionKey& pkey = store_ctx_.cur_pkey_;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(relative_tables_.prepare_tables(store_ctx_.cur_pkey_.get_table_id(),
                 dml_param_.schema_version_,
                 store_ctx_.mem_ctx_->get_read_snapshot(),
                 dml_param_.tenant_schema_version_,
                 upd_col_ids,
                 schema_service,
                 store))) {
    STORAGE_LOG(WARN, "failed to get relative table", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_pg() &&
                         (extract_pure_id(pkey.get_table_id()) == OB_ALL_TABLE_HISTORY_TID ||
                             extract_pure_id(pkey.get_table_id()) == OB_ALL_TABLE_V2_HISTORY_TID) &&
                         relative_tables_.get_index_tables_buf_count() != 1)) {
    ret = OB_SCHEMA_EAGAIN;
    STORAGE_LOG(WARN,
        "__all_table_history should have one index table",
        K(ret),
        "cnt",
        relative_tables_.get_index_tables_buf_count());
  } else if ((relative_tables_.idx_cnt_ > 0 || T_DML_UPDATE == dml_type_) && OB_FAIL(prepare_index_row())) {
    STORAGE_LOG(WARN, "failed to malloc work members", K(ret));
  } else if (NULL != column_ids && OB_FAIL(prepare_column_info(*column_ids))) {
    STORAGE_LOG(WARN, "fail to get column descriptions and column map", K(ret), K(*column_ids));
  } else {
    store_ctx_.mem_ctx_->set_table_version(dml_param_.schema_version_);
    store_ctx_.mem_ctx_->set_abs_expired_time(dml_param_.timeout_);
    store_ctx_.mem_ctx_->set_abs_lock_wait_timeout(dml_param_.timeout_);
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    // DO NOT change the order, relative_tables_ should reset after free_work_members
    free_work_members();
    relative_tables_.reset();
  }
  return ret;
}

void ObPartitionStorage::ObDMLRunningCtx::free_work_members()
{
  if (nullptr != idx_row_) {
    allocator_.free(idx_row_);
    idx_row_ = nullptr;
  }
  if (nullptr != col_descs_ && !relative_tables_.data_table_.use_schema_param()) {
    ObColDescIArray* tmp_col = const_cast<ObColDescIArray*>(col_descs_);
    tmp_col->reset();
    allocator_.free(tmp_col);
    col_descs_ = nullptr;
  }
  if (nullptr != col_map_ && !relative_tables_.data_table_.use_schema_param()) {
    ColumnMap* tmp_map = const_cast<ColumnMap*>(col_map_);
    tmp_map->clear();
    allocator_.free(tmp_map);
    col_map_ = nullptr;
  }
}

int ObPartitionStorage::do_rowkey_exists(const ObStoreCtx& store_ctx, const ObIArray<ObITable*>& read_tables,
    const int64_t table_id, const ObStoreRowkey& rowkey, const ObQueryFlag& query_flag,
    const ObColDescIArray& col_descs, bool& exists)
{
  int ret = OB_SUCCESS;
  if (!store_ctx.is_valid() || read_tables.count() <= 0 || OB_INVALID_ID == table_id || !rowkey.is_valid() ||
      col_descs.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        KP(store_ctx.mem_ctx_),
        K(read_tables.count()),
        K(table_id),
        K(rowkey),
        K(query_flag),
        K(col_descs),
        K(ret));
  } else {
    bool found = false;
    for (int64_t i = read_tables.count() - 1; (!found) && OB_SUCC(ret) && i >= 0; --i) {
      ObITable* table = read_tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unknown store", K(table), K(ret));
      } else if (OB_FAIL(table->exist(store_ctx, table_id, rowkey, col_descs, exists, found))) {
        STORAGE_LOG(WARN, "Fail to check if exist in store, ", K(table_id), K(i), K(ret));
      } else {
        STORAGE_LOG(DEBUG, "rowkey_exists check::", K(i), K(*table), K(table_id), K(rowkey), K(exists), K(found));
      }
    }

    if (OB_SUCCESS == ret && false == found) {
      exists = false;
    }
  }
  return ret;
}

int ObPartitionStorage::rowkey_exists(ObRelativeTable& relative_table, const ObStoreCtx& store_ctx,
    const ObColDescIArray& col_descs, const common::ObNewRow& row, bool& exists)
{
  int ret = OB_SUCCESS;
  ObTablesHandle& tables_handle = relative_table.tables_handle_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!store_ctx.is_valid() || col_descs.count() <= 0 || !row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KP(store_ctx.mem_ctx_), K(col_descs), K(row), K(ret));
  } else {
    {
      ObStorageWriterGuard guard(store_, store_ctx, false);
      if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
        STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_));
      }
    }

    if (OB_SUCC(ret)) {
      ObStoreRowkey rowkey(row.cells_, relative_table.get_rowkey_column_num());
      bool read_latest = true;
      ObQueryFlag flag;
      flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;
      if (relative_table.is_storage_index_table()) {
        flag.index_invalid_ = !relative_table.can_read_index();
      }

      ret = do_rowkey_exists(
          store_ctx, tables_handle.get_tables(), relative_table.get_table_id(), rowkey, flag, col_descs, exists);
    }
  }

  return ret;
}

int ObPartitionStorage::do_rowkeys_prefix_exist(
    const ObIArray<ObITable*>& read_stores, ObRowsInfo& rows_info, bool& may_exist)
{
  int ret = OB_SUCCESS;
  if (!rows_info.is_valid() || read_stores.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rows_info), K(read_stores.count()), K(ret));
  } else {
    may_exist = false;
    for (int64_t i = read_stores.count() - 1; !may_exist && OB_SUCC(ret) && i >= 0; --i) {
      ObITable* store = read_stores.at(i);
      if (OB_ISNULL(store)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unknown store", K(store), K(ret));
      } else if (OB_FAIL(store->prefix_exist(rows_info, may_exist))) {
        STORAGE_LOG(WARN, "Fail to check if exist in store, ", K(rows_info), K(i), K(ret));
      } else {
        STORAGE_LOG(DEBUG,
            "rowkey prefix exists check",
            K(i),
            K(rows_info.ext_rowkeys_),
            K(may_exist),
            K(rows_info.get_max_prefix_length()));
      }
    }
  }

  return ret;
}

int ObPartitionStorage::do_rowkeys_exists(const ObIArray<ObITable*>& read_stores, ObRowsInfo& rows_info, bool& exists)
{
  int ret = OB_SUCCESS;
  if (!rows_info.is_valid() || read_stores.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rows_info), K(read_stores.count()), K(ret));
  } else {
    bool all_rows_found = false;
    for (int64_t i = read_stores.count() - 1; !exists && !all_rows_found && OB_SUCC(ret) && i >= 0; --i) {
      ObITable* store = read_stores.at(i);
      if (OB_ISNULL(store)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unknown store", K(store), K(ret));
      } else if (OB_FAIL(store->exist(rows_info, exists, all_rows_found))) {
        STORAGE_LOG(WARN, "Fail to check if exist in store, ", K(rows_info), K(i), K(ret));
      } else {
        STORAGE_LOG(DEBUG, "rowkey_exists check::", K(i), K(rows_info.ext_rowkeys_), K(exists), K(all_rows_found));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::rowkeys_exists(
    const ObStoreCtx& store_ctx, ObRelativeTable& relative_table, ObRowsInfo& rows_info, bool& exists)
{
  int ret = OB_SUCCESS;
  ObTablesHandle& tables_handle = relative_table.tables_handle_;
  bool may_exist = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!rows_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(rows_info));
  } else {
    {
      ObStorageWriterGuard guard(store_, store_ctx, false);
      if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
        STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_));
      }
    }
    if (OB_SUCC(ret) && rows_info.get_max_prefix_length() > 0) {
      if (OB_FAIL(do_rowkeys_prefix_exist(tables_handle.get_tables(), rows_info, may_exist))) {
        STORAGE_LOG(WARN, "Failed to do prefix rowkey exist", K(ret));
      }
    }
    STORAGE_LOG(DEBUG, "rows info", K(rows_info), K(may_exist));
    if (OB_SUCC(ret) && may_exist) {
      ret = do_rowkeys_exists(tables_handle.get_tables(), rows_info, exists);
    }
  }
  return ret;
}

int ObPartitionStorage::write_row(ObRelativeTable& relative_table, const ObStoreCtx& store_ctx,
    const int64_t rowkey_len, const common::ObIArray<share::schema::ObColDesc>& col_descs,
    const storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;

  {
    ObStorageWriterGuard guard(store_, store_ctx, true);

    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
    } else if (!store_ctx.is_valid() || col_descs.count() <= 0 || rowkey_len <= 0 || !row.is_valid() ||
               !relative_table.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN,
          "invalid argument",
          KP(store_ctx.mem_ctx_),
          K(relative_table),
          K(rowkey_len),
          K(col_descs),
          K(row),
          K(ret));
    } else if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
      STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_));
    } else {
      ObMemtable* write_memtable = NULL;
      const uint64_t table_id = relative_table.get_table_id();
      store_ctx.tables_ = &relative_table.tables_handle_.get_tables();
      if (OB_FAIL(relative_table.tables_handle_.get_last_memtable(write_memtable))) {
        STORAGE_LOG(WARN, "failed to get_last_memtable", K(ret));
      } else if (OB_FAIL(write_memtable->set(store_ctx, table_id, rowkey_len, col_descs, row))) {
        STORAGE_LOG(WARN, "failed to set memtable", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(store_ctx.mem_ctx_);
    transaction::ObPartTransCtx* part_ctx = static_cast<transaction::ObPartTransCtx*>(mt_ctx->get_trans_ctx());

    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = part_ctx->submit_log_if_neccessary()))) {
      TRANS_LOG(INFO, "submit log if neccesary failed", K(tmp_ret), K(store_ctx), K(relative_table));
    }
  }

  return ret;
}

int ObPartitionStorage::write_row(ObRelativeTable& relative_table, const storage::ObStoreCtx& store_ctx,
    const int64_t rowkey_len, const common::ObIArray<share::schema::ObColDesc>& col_descs,
    const ObIArray<int64_t>& update_idx, const storage::ObStoreRow& old_row, const storage::ObStoreRow& new_row)
{
  int ret = OB_SUCCESS;

  {
    ObStorageWriterGuard guard(store_, store_ctx, true);

    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
    } else if (!store_ctx.is_valid() || col_descs.count() <= 0 || rowkey_len <= 0 || !old_row.is_valid() ||
               !new_row.is_valid() || !relative_table.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN,
          "invalid argument",
          KP(store_ctx.mem_ctx_),
          K(relative_table),
          K(rowkey_len),
          K(col_descs),
          K(update_idx),
          K(old_row),
          K(new_row),
          K(ret));
    } else if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
      STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_));
    } else {
      ObMemtable* write_memtable = NULL;
      const uint64_t table_id = relative_table.get_table_id();
      store_ctx.tables_ = &(relative_table.tables_handle_.get_tables());
      if (OB_FAIL(relative_table.tables_handle_.get_last_memtable(write_memtable))) {
        STORAGE_LOG(WARN, "failed to get write memtable", K(ret));
      } else if (OB_FAIL(
                     write_memtable->set(store_ctx, table_id, rowkey_len, col_descs, update_idx, old_row, new_row))) {
        STORAGE_LOG(WARN, "failed to set write memtable", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(store_ctx.mem_ctx_);
    transaction::ObPartTransCtx* part_ctx = static_cast<transaction::ObPartTransCtx*>(mt_ctx->get_trans_ctx());

    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = part_ctx->submit_log_if_neccessary()))) {
      TRANS_LOG(INFO, "submit log if neccesary failed", K(tmp_ret), K(store_ctx), K(relative_table));
    }
  }

  return ret;
}

int ObPartitionStorage::lock_row(ObRelativeTable& relative_table, const storage::ObStoreCtx& store_ctx,
    const common::ObIArray<share::schema::ObColDesc>& col_descs, const common::ObNewRow& row, const ObSQLMode sql_mode,
    ObIAllocator& allocator, RowReshape*& row_reshape_ins)
{
  int ret = OB_SUCCESS;
  ObStorageWriterGuard guard(store_, store_ctx, true);
  bool need_reshape_row;
  ObStoreRow lock_row;
  RowReshape* reshape_ptr = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!relative_table.is_valid() || !store_ctx.is_valid() || col_descs.count() <= 0 || !row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(relative_table), KP(store_ctx.mem_ctx_), K(col_descs), K(row));
  } else if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
    STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_));
  } else if (OB_FAIL(need_reshape_table_row(row, col_descs.count(), sql_mode, need_reshape_row))) {
    LOG_WARN("fail to check need reshape row", K(ret), K(row), K(sql_mode));
  } else if (need_reshape_row && nullptr == row_reshape_ins) {
    if (OB_FAIL(malloc_rows_reshape(allocator, col_descs, 1, relative_table, reshape_ptr))) {
      LOG_WARN("fail to malloc reshape", K(ret), K(col_descs), K(sql_mode));
    } else {
      row_reshape_ins = reshape_ptr;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(reshape_row(row, col_descs.count(), row_reshape_ins, lock_row, need_reshape_row, sql_mode))) {
      LOG_WARN("failed to reshape row", K(ret), K(row), K(need_reshape_row), K(sql_mode));
    } else {
      memtable::ObMemtable* write_memtable = NULL;
      const uint64_t table_id = relative_table.get_table_id();
      store_ctx.tables_ = &(relative_table.tables_handle_.get_tables());
      if (OB_FAIL(relative_table.tables_handle_.get_last_memtable(write_memtable))) {
        STORAGE_LOG(WARN, "failed to get_last_memtable", K(ret));
      } else if (OB_ISNULL(write_memtable)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "write_memtable must not null", K(ret));
      } else if (OB_FAIL(write_memtable->lock(store_ctx, table_id, col_descs, lock_row.row_val_))) {
        STORAGE_LOG(WARN, "failed to lock write_memtable", K(ret), K(col_descs), K(lock_row));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::lock_row(ObRelativeTable& relative_table, const storage::ObStoreCtx& store_ctx,
    const common::ObIArray<share::schema::ObColDesc>& col_descs, const common::ObStoreRowkey& rowkey)
{
  int ret = OB_SUCCESS;
  ObStorageWriterGuard guard(store_, store_ctx, true);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!relative_table.is_valid() || !store_ctx.is_valid() || col_descs.count() <= 0 || !rowkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(relative_table), KP(store_ctx.mem_ctx_), K(col_descs), K(rowkey));
  } else if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
    STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_));
  } else {
    memtable::ObMemtable* write_memtable = NULL;
    const uint64_t table_id = relative_table.get_table_id();
    store_ctx.tables_ = &(relative_table.tables_handle_.get_tables());
    if (OB_FAIL(relative_table.tables_handle_.get_last_memtable(write_memtable))) {
      STORAGE_LOG(WARN, "failed to get_last_memtable", K(ret));
    } else if (OB_ISNULL(write_memtable)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "write_memtable must not null", K(ret));
    } else if (OB_FAIL(write_memtable->lock(store_ctx, table_id, col_descs, rowkey))) {
      STORAGE_LOG(WARN, "failed to lock write memtable", K(ret), K(table_id), K(col_descs), K(rowkey));
    }
  }
  return ret;
}

int ObPartitionStorage::check_row_locked_by_myself(ObRelativeTable& relative_table, const ObStoreCtx& store_ctx,
    const ObIArray<share::schema::ObColDesc>& col_descs, const ObStoreRowkey& rowkey, bool& locked)
{
  int ret = OB_SUCCESS;
  ObStorageWriterGuard guard(store_, store_ctx, true);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!relative_table.is_valid() || !store_ctx.is_valid() || col_descs.count() <= 0 || !rowkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(relative_table), KP(store_ctx.mem_ctx_), K(col_descs), K(rowkey));
  } else if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
    STORAGE_LOG(WARN, "fail to protect table", K(ret), K(pkey_));
  } else {
    memtable::ObMemtable* write_memtable = NULL;
    const uint64_t table_id = relative_table.get_table_id();
    store_ctx.tables_ = &(relative_table.tables_handle_.get_tables());
    if (OB_FAIL(relative_table.tables_handle_.get_last_memtable(write_memtable))) {
      STORAGE_LOG(WARN, "failed to get_last_memtable", K(ret));
    } else if (OB_ISNULL(write_memtable)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "write_memtable must not null", K(ret));
    } else if (OB_FAIL(write_memtable->check_row_locked_by_myself(store_ctx, table_id, col_descs, rowkey, locked))) {
      STORAGE_LOG(WARN, "failed to lock write memtable", K(ret), K(table_id), K(col_descs), K(rowkey));
    }
  }

  return ret;
}

bool ObPartitionStorage::is_expired_version(
    const common::ObVersion& base_version, const common::ObVersion& version, const int64_t max_kept_number)
{
  return version.major_ > 0 && version.major_ < (base_version.major_ - max(0, max_kept_number - 1));
}

int ObPartitionStorage::get_merge_level(
    const int64_t merge_version, const ObSSTableMergeCtx& ctx, ObMergeLevel& merge_level)
{
  int ret = OB_SUCCESS;
  int64_t storage_format_work_version = 0;
  int64_t storage_format_version = 0;
  if (OB_FAIL(ctx.get_storage_format_version(storage_format_version))) {
    LOG_WARN("fail to get storage format version", K(ret));
  } else if (OB_FAIL(ctx.get_storage_format_work_version(storage_format_work_version))) {
    LOG_WARN("fail to get storage format work version", K(ret));
  } else if (OB_FAIL(get_merge_opt(merge_version,
                 storage_format_version,
                 storage_format_work_version,
                 OB_STORAGE_FORMAT_VERSION_V1,
                 MACRO_BLOCK_MERGE_LEVEL,
                 MICRO_BLOCK_MERGE_LEVEL,
                 merge_level))) {
    LOG_WARN("fail to get merge option", K(ret));
  }
  return ret;
}

int ObPartitionStorage::get_store_column_checksum_in_micro(
    const int64_t merge_version, const ObSSTableMergeCtx& ctx, bool& store_column_checksum)
{
  int ret = OB_SUCCESS;
  int64_t storage_format_work_version = 0;
  int64_t storage_format_version = 0;
  if (OB_FAIL(ctx.get_storage_format_version(storage_format_version))) {
    LOG_WARN("fail to get storage format version", K(ret));
  } else if (OB_FAIL(ctx.get_storage_format_work_version(storage_format_work_version))) {
    LOG_WARN("fail to get storage format work version", K(ret));
  } else if (OB_FAIL(get_merge_opt(merge_version,
                 storage_format_version,
                 storage_format_work_version,
                 OB_STORAGE_FORMAT_VERSION_V3,
                 false,
                 true,
                 store_column_checksum))) {
    LOG_WARN("fail to get merge option", K(ret));
  }
  return ret;
}

int ObPartitionStorage::deep_copy_build_index_schemas(const ObTableSchema* data_table_schema,
    const ObTableSchema* index_schema, const ObTableSchema* dep_table_schema, ObBuildIndexParam& param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(data_table_schema), KP(index_schema));
  } else {
    ObIAllocator& allocator = param.allocator_;
    const int64_t table_schema_cnt = NULL == dep_table_schema ? 2 : 3;
    const int64_t alloc_size = table_schema_cnt * sizeof(ObTableSchema);
    char* buf = NULL;
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory for deep copy schema", K(ret));
    } else {
      ObTableSchema* deep_copy_data_table_schema = NULL;
      ObTableSchema* deep_copy_index_schema = NULL;
      ObTableSchema* deep_copy_dep_table_schema = NULL;
      deep_copy_data_table_schema = new (buf) ObTableSchema(&allocator);
      buf += sizeof(ObTableSchema);
      deep_copy_index_schema = new (buf) ObTableSchema(&allocator);
      buf += sizeof(ObTableSchema);
      if (NULL != deep_copy_dep_table_schema) {
        deep_copy_dep_table_schema = new (buf) ObTableSchema(&allocator);
      }
      if (OB_FAIL(deep_copy_data_table_schema->assign(*data_table_schema))) {
        STORAGE_LOG(WARN, "fail to assign table schema", K(ret));
      } else if (OB_FAIL(deep_copy_index_schema->assign(*index_schema))) {
        STORAGE_LOG(WARN, "fail to assign index schema", K(ret));
      } else if (NULL != dep_table_schema && OB_FAIL(deep_copy_dep_table_schema->assign(*dep_table_schema))) {
        STORAGE_LOG(WARN, "fail to assign dep table schema", K(ret));
      } else {
        param.table_schema_ = deep_copy_data_table_schema;
        param.index_schema_ = deep_copy_index_schema;
        param.dep_table_schema_ = deep_copy_dep_table_schema;
      }
    }
  }
  return ret;
}

// schema_version maybe tenant level
// but it doesn't support index building of tenant level system table
// (index of tenant level virtual table works immediatelly)
int ObPartitionStorage::get_build_index_param(
    const uint64_t index_id, const int64_t schema_version, ObIPartitionReport* report, ObBuildIndexParam& param)
{
  int ret = OB_SUCCESS;
  int64_t concurrent_cnt = 0;
  const uint64_t tenant_id = is_inner_table(index_id) ? OB_SYS_TENANT_ID : extract_tenant_id(index_id);
  const bool check_formal = extract_pure_id(index_id) > OB_MAX_CORE_TABLE_ID;  // to avoid circular dependency
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (OB_INVALID_ID == index_id || OB_INVALID_VERSION == schema_version || NULL == report) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(index_id), K(schema_version), KP(report));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTenantSchema* tenant_schema = NULL;
    const ObTableSchema* data_table_schema = NULL;
    const ObTableSchema* index_schema = NULL;
    const ObTableSchema* dep_table = NULL;
    if (OB_FAIL(
            schema_service_->get_tenant_schema_guard(tenant_id, schema_guard, schema_version, OB_INVALID_VERSION))) {
      STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(schema_version));
    } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
      LOG_WARN("schema_guard is not formal", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_tenant_info(extract_tenant_id(index_id), tenant_schema))) {
      STORAGE_LOG(WARN, "fail to get tenant info", K(ret), K(index_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN, "tenant not exist", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_id, index_schema))) {
      STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(index_id));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN, "index schema not exist", K(ret), K(index_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), data_table_schema))) {
      STORAGE_LOG(
          WARN, "fail to get table schema", K(ret), K(index_id), "data_table_id", index_schema->get_data_table_id());
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN, "data table not exist", K(ret), "data_table_id", index_schema->get_data_table_id());
    } else if (OB_FAIL(get_depend_table_schema(index_schema, schema_guard, dep_table))) {
      STORAGE_LOG(WARN, "fail to get depend table schema", K(ret));
    } else if (OB_FAIL(deep_copy_build_index_schemas(data_table_schema, index_schema, dep_table, param))) {
      STORAGE_LOG(WARN, "fail to deep copy build index schemas", K(ret));
    } else if (OB_FAIL(get_concurrent_cnt(data_table_schema->get_table_id(), schema_version, concurrent_cnt))) {
      STORAGE_LOG(WARN, "fail to get concurrent cnt", K(ret));
    } else {
      param.schema_cnt_ = data_table_schema->get_index_tid_count() + 1;
      param.schema_version_ = schema_version;
      param.report_ = report;
      // control currency level, to finish merge in the last round
      param.concurrent_cnt_ = MIN(concurrent_cnt,
          param.DEFAULT_INDEX_SORT_MEMORY_LIMIT / ObExternalSortConstant::DEFAULT_FILE_READ_WRITE_BUFFER / 2);
      param.report_ = report;
      if (OB_FAIL(get_build_index_stores(*tenant_schema, param))) {
        STORAGE_LOG(WARN, "fail to get build index stores", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::check_table_continuity(const uint64_t table_id, const int64_t version, bool& is_continual)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;
  bool tmp_continual = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_INVALID_ID == table_id || 0 >= version) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(table_id), K(version));
  } else if (OB_FAIL(store_.get_read_tables(table_id, version, tables_handle))) {
    STORAGE_LOG(WARN, "failed to get read stores", K(ret), K(table_id), K(version));
  } else if (0 == tables_handle.get_count()) {
    tmp_continual = false;
  } else {
    tmp_continual = true;
    int64_t last_version = version;
    for (int64_t i = tables_handle.get_count() - 1; tmp_continual && i >= 0; i--) {
      ObITable* table = tables_handle.get_table(i);
      if (table->get_snapshot_version() != last_version) {
        tmp_continual = false;
      } else {
        last_version = table->get_base_version();
      }
    }
  }
  if (OB_SUCCESS == ret) {
    is_continual = tmp_continual;
  }
  return ret;
}

void ObPartitionStorage::set_merge_status(bool merge_success)
{
  ATOMIC_STORE(&merge_successed_, merge_success);
  ATOMIC_STORE(&merge_timestamp_, ObTimeUtility::current_time());
  if (merge_success) {
    ATOMIC_STORE(&merge_failed_cnt_, 0);
  } else {
    ATOMIC_AAF(&merge_failed_cnt_, 1);
  }
}

bool ObPartitionStorage::can_schedule_merge()
{
  int bool_ret = true;
  if (!merge_successed_ &&
      (ObTimeUtility::current_time() < (merge_timestamp_ + merge_failed_cnt_ * DELAY_SCHEDULE_TIME_UNIT))) {
    bool_ret = false;
  }

  return bool_ret;
}

int ObPartitionStorage::append_local_sort_data(const share::ObBuildIndexAppendLocalDataParam& param,
    const common::ObPGKey& pg_key, const blocksstable::ObStorageFileHandle& file_handle, ObNewRowIterator& iter)
{
  int ret = OB_SUCCESS;
  ObFreezeInfoProxy freeze_info_proxy;
  int64_t snapshot_version = 0;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema* tenant_schema = NULL;
  const ObTableSchema* index_schema = NULL;
  const ObTableSchema* data_table_schema = NULL;
  ObColumnChecksumCalculator checksum;
  ObDataStoreDesc data_desc;
  SMART_VAR(ObMacroBlockWriter, writer)
  {
    const uint64_t execution_id = param.execution_id_;
    const uint64_t task_id = param.task_id_;
    const uint64_t index_table_id = param.index_id_;
    const int64_t schema_version = param.schema_version_;
    const bool is_major_merge = true;
    ObTablesHandle table_handle;
    ObIntermMacroKey key;
    ObIntermMacroHandle handle;
    ObSimpleFrozenStatus frozen_status;
    bool need_append = false;
    bool has_major = false;
    int64_t checksum_method = 0;
    key.execution_id_ = execution_id;
    key.task_id_ = task_id;
    const uint64_t tenant_id = is_inner_table(index_table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(index_table_id);
    const bool check_formal = extract_pure_id(index_table_id) > OB_MAX_CORE_TABLE_ID;  // to avoid circular dependency
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
    } else if (OB_UNLIKELY(!param.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), K(param));
    } else if (OB_FAIL(store_.has_major_sstable(index_table_id, has_major))) {
      STORAGE_LOG(WARN, "fail to check has major sstable", K(ret));
    } else {
      need_append = !has_major;
    }

    if (OB_SUCC(ret) && need_append) {
      if (OB_FAIL(INTERM_MACRO_MGR.get(key, handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "fail to get interm macro value", K(ret));
        } else {
          ret = OB_SUCCESS;
          need_append = true;
        }
      } else {
        need_append = false;
      }
    }

    STORAGE_LOG(INFO, "begin append local sort data", K(ret), K(need_append), K(has_major));
    if (OB_SUCC(ret) && need_append) {
      if (OB_FAIL(
              schema_service_->get_tenant_schema_guard(tenant_id, schema_guard, schema_version, OB_INVALID_VERSION))) {
        STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(pkey_), K(index_table_id), K(schema_version));
      } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
        LOG_WARN("schema_guard is not formal", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schema(index_table_id, index_schema))) {
        STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(index_table_id));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        STORAGE_LOG(WARN, "schema error, index schema not exist", K(ret), K(index_table_id), K(schema_version));
      } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), data_table_schema))) {
        STORAGE_LOG(WARN, "fail to get table schema", K(ret), "data_table_id", index_schema->get_data_table_id());
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_SCHEMA_ERROR;
        STORAGE_LOG(
            WARN, "data table schema must not be NULL", K(ret), "data_table_id", index_schema->get_data_table_id());
      } else if (OB_FAIL(ObIndexBuildStatOperator::get_snapshot_version(
                     index_schema->get_data_table_id(), index_table_id, *GCTX.sql_proxy_, snapshot_version))) {
        STORAGE_LOG(WARN, "fail to get snapshot version", K(ret), K(index_table_id));
      } else if (snapshot_version <= 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "error unexpected, snapshot version is invalid", K(ret), K(snapshot_version));
      } else if (OB_FAIL(
                     freeze_info_proxy.get_frozen_info_less_than(*GCTX.sql_proxy_, snapshot_version, frozen_status))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "fail to get next major freeze", K(ret), K(pkey_));
        } else {
          frozen_status.frozen_version_ = 1L;
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(ObIndexChecksumOperator::get_checksum_method(
                     param.execution_id_, index_schema->get_data_table_id(), checksum_method, *GCTX.sql_proxy_))) {
        STORAGE_LOG(WARN, "fail to get checksum method", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_guard.get_tenant_info(pkey_.get_tenant_id(), tenant_schema))) {
        STORAGE_LOG(WARN, "fail to get tenant schema", K(ret), K(pkey_), "tenant_id", pkey_.get_tenant_id());
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_SCHEMA_ERROR;
        STORAGE_LOG(WARN, "tenant schema not exist", K(ret), K(pkey_), "tenant_id", pkey_.get_tenant_id());
      } else if (OB_FAIL(checksum.init(index_schema->get_column_count()))) {
        STORAGE_LOG(WARN, "fail to init checksum", K(ret));
      } else if (OB_FAIL(data_desc.init(*index_schema,
                     frozen_status.frozen_version_,
                     nullptr,
                     pkey_.get_partition_id(),
                     is_major_merge ? storage::MAJOR_MERGE : storage::MINOR_MERGE,
                     blocksstable::CCM_VALUE_ONLY == checksum_method /*calc column checksum*/,
                     true /*store column checksum in micro block*/,
                     pg_key,
                     file_handle))) {
        STORAGE_LOG(WARN, "Fail to init data store desc, ", K(ret));
      } else if (OB_FAIL(writer.open(data_desc, ObMacroDataSeq((INT64_MAX / param.task_cnt_) * param.task_id_)))) {
        STORAGE_LOG(WARN, "Fail to open macro block writer, ", K(ret));
      } else {
        ObStoreRow row;
        ObNewRow* row_val = NULL;
        row.flag_ = ObActionFlag::OP_ROW_EXIST;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(THIS_WORKER.check_status())) {
            STORAGE_LOG(WARN, "failed to check status", K(ret));
          } else if (OB_FAIL(iter.get_next_row(row_val))) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "fail to get next row", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            row.row_val_.assign(row_val->cells_, row_val->count_);
            if (OB_FAIL(writer.append_row(row))) {
              if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret && index_schema->is_unique_index()) {
                LOG_USER_ERROR(
                    OB_ERR_PRIMARY_KEY_DUPLICATE, "", static_cast<int>(sizeof("UNIQUE IDX") - 1), "UNIQUE IDX");
              } else {
                STORAGE_LOG(WARN, "fail to append row to sstable", K(ret));
              }
            } else if (OB_FAIL(checksum.calc_column_checksum(checksum_method, &row, NULL, NULL))) {
              STORAGE_LOG(WARN, "fail to calc column checksum", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(writer.close())) {
            STORAGE_LOG(WARN, "fail to close writer", K(ret));
          } else {
            bool is_added = false;
            if (OB_FAIL(INTERM_MACRO_MGR.put(key, writer.get_macro_block_write_ctx()))) {
              STORAGE_LOG(WARN, "fail to put interm result", K(ret));
            } else {
              is_added = true;
              if (OB_FAIL(report_checksum(
                      execution_id, task_id, *index_schema, checksum_method, checksum.get_column_checksum()))) {
                STORAGE_LOG(WARN, "fail to report checksum", K(ret), K_(pkey), K(index_table_id));
              }
            }

            if (OB_FAIL(ret) && is_added) {
              int tmp_ret = OB_SUCCESS;
              if (OB_SUCCESS != (tmp_ret = INTERM_MACRO_MGR.remove(key))) {
                STORAGE_LOG(WARN, "fail to remove interm macro list", K(tmp_ret), K(pkey_));
              }
            }
          }
        }
      }
    }
    STORAGE_LOG(INFO,
        "finish append local sort data",
        K(ret),
        K_(pkey),
        K(index_table_id),
        K(snapshot_version),
        K(frozen_status));
  }

  return ret;
}

int ObPartitionStorage::report_checksum(const uint64_t execution_id, const uint64_t task_id,
    const ObTableSchema& index_schema, const int64_t checksum_method, int64_t* column_checksum)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> column_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (OB_UNLIKELY(!index_schema.is_valid()) || OB_ISNULL(column_checksum)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(index_schema), KP(column_checksum));
  } else if (OB_FAIL(index_schema.get_column_ids(column_ids))) {
    STORAGE_LOG(WARN, "fail to get column ids", K(ret));
  } else {
    ObArray<ObIndexChecksumItem> checksum_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      ObIndexChecksumItem item;
      item.execution_id_ = execution_id;
      item.tenant_id_ = pkey_.get_tenant_id();
      item.table_id_ = index_schema.get_table_id();
      item.partition_id_ = pkey_.get_partition_id();
      item.column_id_ = column_ids.at(i).col_id_;
      item.task_id_ = task_id;
      item.checksum_ = column_checksum[i];
      item.checksum_method_ = checksum_method;
      if (item.column_id_ >= OB_MIN_SHADOW_COLUMN_ID) {
        continue;
      } else if (OB_FAIL(checksum_items.push_back(item))) {
        STORAGE_LOG(WARN, "fail to push back item", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObIndexChecksumOperator::update_checksum(checksum_items, *GCTX.sql_proxy_))) {
        LOG_WARN("fail to update checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionStorage::create_partition_store(const ObReplicaType& replica_type, const int64_t multi_version_start,
    const uint64_t data_table_id, const int64_t create_schema_version, const int64_t create_timestamp,
    ObIPartitionGroup* pg, ObTablesHandle& sstables_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_INVALID_ID == data_table_id || OB_ISNULL(pg)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(data_table_id), KP(pg));
  } else {
    ObPGPartitionStoreMeta partition_meta;
    const bool write_slog = true;
    const bool need_create_sstable = sstables_handle.get_count() > 0;
    int64_t data_version = 0;
    int64_t snapshot_version = 0;

    if (need_create_sstable) {
      for (int64_t i = 0; i < sstables_handle.get_count() && OB_SUCC(ret); ++i) {
        const ObITable* table = sstables_handle.get_table(i);
        if (OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "table is NULL", K(ret), K(pkey_));
        } else if (table->is_major_sstable()) {
          data_version = MAX(data_version, int64_t(table->get_version().major_));
          snapshot_version = MAX(snapshot_version, table->get_snapshot_version());
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_partition_meta(replica_type,
                   data_version,
                   multi_version_start,
                   data_table_id,
                   create_schema_version,
                   create_timestamp,
                   snapshot_version,
                   partition_meta))) {
      STORAGE_LOG(WARN, "failed to init partition meta", K(ret));
    } else if (OB_FAIL(store_.create_partition_store(
                   partition_meta, write_slog, pg, ObFreezeInfoMgrWrapper::get_instance(), pg_memtable_mgr_))) {
      STORAGE_LOG(WARN, "failed to init partition store", K(ret));
    }
  }
  return ret;
}

int ObPartitionStorage::init_partition_meta(const ObReplicaType& replica_type, const int64_t data_version,
    const int64_t multi_version_start, const uint64_t data_table_id, const int64_t create_schema_version,
    const int64_t create_timestamp, const int64_t snapshot_version, ObPGPartitionStoreMeta& partition_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!ObReplicaTypeCheck::is_replica_type_valid(replica_type) || multi_version_start <= 0 ||
             OB_INVALID_ID == data_table_id || create_schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid args",
        K(ret),
        K(replica_type),
        K(multi_version_start),
        K(data_table_id),
        K(create_schema_version));
  } else {
    partition_meta.pkey_ = pkey_;
    partition_meta.replica_type_ = replica_type;
    partition_meta.multi_version_start_ = multi_version_start;
    partition_meta.data_table_id_ = data_table_id;
    partition_meta.create_schema_version_ = create_schema_version;

    partition_meta.report_status_.reset();
    partition_meta.report_status_.data_version_ = data_version;
    partition_meta.create_timestamp_ = create_timestamp;
    partition_meta.report_status_.snapshot_version_ = snapshot_version;
  }
  return ret;
}

int ObPartitionStorage::update_multi_version_start(const int64_t multi_version_start)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret), K(pkey_));
  } else if (OB_FAIL(store_.update_multi_version_start(multi_version_start))) {
    STORAGE_LOG(WARN, "failed to update_multi_version_start", K(ret));
  }
  return ret;
}

bool ObPartitionStorage::need_check_index_(
    const uint64_t index_id, const ObIArray<share::schema::ObIndexTableStat>& index_stats)
{
  bool need_check = false;

  for (int64_t i = 0; i < index_stats.count(); ++i) {
    const share::schema::ObIndexTableStat& stat = index_stats.at(i);
    if (stat.index_id_ == index_id) {
      need_check = !is_final_invalid_index_status(stat.index_status_, stat.is_drop_schema_);
      break;
    }
  }
  return need_check;
}

int ObPartitionStorage::validate_sstables(
    const ObIArray<share::schema::ObIndexTableStat>& index_stats, bool& is_all_checked)
{
  int ret = OB_SUCCESS;
  ObTableHandle main_table_handle;
  ObTablesHandle handle;
  ObSSTable* main_sstable = NULL;
  is_all_checked = true;
  bool is_physical_split_finish = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionStore has not been inited", K(ret));
  } else if (OB_FAIL(store_.is_physical_split_finished(is_physical_split_finish))) {
    STORAGE_LOG(WARN, "failed to check physical split finish", K(ret));
  } else if (!is_physical_split_finish) {
    is_all_checked = true;
  } else if (OB_FAIL(store_.get_last_major_sstable(pkey_.get_table_id(), main_table_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get last major sstable", K(ret));
    }
  } else if (OB_FAIL(store_.get_last_all_major_sstable(handle))) {
    LOG_WARN("fail to get last all major sstable", K(ret));
  } else if (OB_FAIL(main_table_handle.get_sstable(main_sstable))) {
    LOG_WARN("fail to get major sstable", K(ret));
  } else {
    ObArray<ObSSTable*> index_tables;
    if (OB_FAIL(index_tables.push_back(main_sstable))) {
      LOG_WARN("failed to add main sstable", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
      ObITable* table = handle.get_table(i);
      ObSSTable* sstable = NULL;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, table must not be NULL", K(ret));
      } else if (OB_UNLIKELY(!table->is_major_sstable())) {
        ret = OB_ERR_SYS;
        LOG_WARN("err sys, table must be sstable", K(ret), K(*table));
      } else if (OB_ISNULL(sstable = static_cast<ObSSTable*>(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, sstable must not be NULL", K(ret));
      } else if (table->get_key().table_id_ == pkey_.get_table_id()) {
        // do nothing
      } else if (sstable->get_meta().create_snapshot_version_ == table->get_snapshot_version()) {
        // create index first time
      } else if (!need_check_index_(table->get_key().table_id_, index_stats)) {
        // skip index we do not care
      } else if (OB_FAIL(index_tables.push_back(sstable))) {
        LOG_WARN("fail to push back sstable", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ColumnChecksumMap column_checksum_map;
      const int64_t row_count = main_sstable->get_meta().row_count_;
      if (OB_FAIL(column_checksum_map.create(main_sstable->get_meta().column_cnt_, ObModIds::OB_PARTITION_STORAGE))) {
        LOG_WARN("failed to create column checksum map", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < index_tables.count(); ++i) {
        ObSSTable* index_table = index_tables.at(i);
        if (main_sstable->get_snapshot_version() != index_table->get_snapshot_version()) {
          is_all_checked = false;
          if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
            FLOG_INFO("index table data version is smaller than main table, can not checked all",
                K(*main_sstable),
                K(*index_table));
          }
        } else if (OB_FAIL(validate_sstable(row_count, index_table, column_checksum_map))) {
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            handle_error_index_table(*index_table, index_stats, ret);
            if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
              dump_error_info(*main_sstable, *index_table);
            }
          } else {
            LOG_WARN("fail to validate index sstable", K(ret), "main_sstable_meta", main_sstable->get_meta());
          }
        }
      }
      if (column_checksum_map.created()) {
        column_checksum_map.destroy();
      }
    }
    if (is_all_checked) {
      is_all_checked = OB_SUCCESS == ret;
    }
  }
  return ret;
}

int ObPartitionStorage::dump_error_info(ObSSTable& main_sstable, ObSSTable& index_sstable)
{
  int ret = OB_SUCCESS;
  ObSSTableDumpErrorInfo dumper;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* main_table_schema = NULL;
  const ObTableSchema* index_table_schema = NULL;
  int64_t save_schema_version = 0;
  const int64_t schema_version = main_sstable.get_meta().schema_version_;

  if (OB_FAIL(schema_service_->retry_get_schema_guard(
          schema_version, main_sstable.get_table_id(), schema_guard, save_schema_version))) {
    if (OB_TABLE_IS_DELETED != ret) {
      STORAGE_LOG(WARN, "Fail to get schema, ", K(schema_version), K(ret));
    } else {
      STORAGE_LOG(WARN, "table is deleted", K(ret), "table_id", main_sstable.get_table_id());
    }
  } else if (save_schema_version < schema_version) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("can not use older schema version",
        K(ret),
        K(schema_version),
        K(save_schema_version),
        K_(pkey),
        "table_id",
        main_sstable.get_table_id());
  } else if (OB_FAIL(schema_guard.get_table_schema(main_sstable.get_table_id(), main_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(main_sstable));
  } else if (OB_ISNULL(main_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("main table schema is null", K(ret), K(main_sstable));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_sstable.get_table_id(), index_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(index_sstable));
  } else if (OB_ISNULL(index_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index table schema is null", K(ret), K(index_sstable));
  } else if (OB_FAIL(dumper.main_and_index_row_count_error(
                 main_sstable, *main_table_schema, index_sstable, *index_table_schema))) {
    LOG_WARN("failed to find extras rows", K(ret), K(main_sstable), K(index_sstable));
  }
  return ret;
}

int ObPartitionStorage::try_update_report_status(const common::ObVersion& version, bool& finished, bool& need_report)
{
  int ret = OB_SUCCESS;
  bool major_merge_finished = false;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* main_table_schema = NULL;
  ObArray<ObIndexTableStat> index_stats;
  bool is_all_checked = false;
  finished = false;
  const uint64_t fetch_tenant_id = is_inner_table(pkey_.get_table_id()) ? OB_SYS_TENANT_ID : pkey_.get_tenant_id();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (!version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(version));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
    STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(fetch_tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(pkey_.get_table_id(), main_table_schema))) {
    STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(pkey_));
  } else if (OB_ISNULL(main_table_schema)) {
    // do nothing
    finished = true;
    STORAGE_LOG(INFO, "main table schema has been deleted", K(ret), K(pkey_));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    const bool with_mv = true;
    const ObTableSchema* tmp_schema = NULL;
    if (OB_FAIL(main_table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos, with_mv))) {
      STORAGE_LOG(WARN, "fail to get index tid array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        if (OB_FAIL(schema_guard.get_table_schema(simple_index_infos.at(i).table_id_, tmp_schema))) {
          STORAGE_LOG(WARN, "fail to get table schema", K(ret));
        } else if (OB_ISNULL(tmp_schema)) {
          ret = OB_SCHEMA_ERROR;
          STORAGE_LOG(WARN, "schema error, index schema must be exist", K(ret));
        } else if (OB_FAIL(index_stats.push_back(ObIndexTableStat(simple_index_infos.at(i).table_id_,
                       tmp_schema->get_index_status(),
                       tmp_schema->is_dropped_schema())))) {
          STORAGE_LOG(WARN, "fail to push back index status", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret) || finished) {
  } else if (OB_FAIL(store_.check_need_report(version, need_report))) {
    STORAGE_LOG(WARN, "fail to check need report", K(ret));
  } else if (need_report) {
    if (OB_FAIL(store_.check_major_merge_finished(version, major_merge_finished))) {
      STORAGE_LOG(WARN, "fail to check major merge finished", K(ret));
    } else if (major_merge_finished) {
      if (OB_FAIL(validate_sstables(index_stats, is_all_checked))) {
        STORAGE_LOG(WARN, "fail to validate sstables", K(ret), K(pkey_));
      } else {
        const int64_t data_table_id = main_table_schema->is_global_index_table()
                                          ? main_table_schema->get_data_table_id()
                                          : main_table_schema->get_table_id();
        if (OB_FAIL(store_.try_update_report_status(data_table_id))) {
          STORAGE_LOG(WARN, "fail to advance report status", K(ret));
        } else {
          finished = true;
        }
      }
    }
  } else {
    finished = true;
  }
  return ret;
}

int ObPartitionStorage::fill_checksum(const uint64_t index_id, const int sstable_type, ObSSTableChecksumItem& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == index_id || sstable_type < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(index_id), K(sstable_type));
  } else if (OB_FAIL(store_.fill_checksum(index_id, sstable_type, item))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to fill checksum item", K(ret), K(index_id), K(sstable_type));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < item.column_checksum_.count(); ++i) {
      item.column_checksum_.at(i).server_ = GCTX.self_addr_;
    }
  }
  return ret;
}

int ObPartitionStorage::feedback_scan_access_stat(const ObTableScanParam& param)
{
  int ret = OB_SUCCESS;
  const ObTableScanStatistic& main_scan_stat = param.main_table_scan_stat_;
  if (main_scan_stat.rowkey_prefix_ > 0 && main_scan_stat.bf_access_cnt_ > 0) {
    if (OB_FAIL(prefix_access_stat_.add_stat(main_scan_stat))) {}
  }
  return ret;
}

int ObPartitionStorage::validate_sstable(const int64_t row_count, const ObSSTable* sstable, ColumnChecksumMap& cc_map)
{
  int ret = OB_SUCCESS;
  const ObSSTableMeta& meta = sstable->get_meta();
  if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable is null", K(ret));
  } else if (INDEX_TYPE_DOMAIN_CTXCAT == meta.index_type_) {
    // skip checking domain_ctxcat index
  } else if (row_count != meta.row_count_) {
    ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
    LOG_WARN(
        "sstable row count is not same to main table", K(ret), "main table row_count", row_count, "sstable meta", meta);
  } else {
    const int64_t table_id = sstable->get_table_id();
    ObColumnChecksumEntry entry;
    for (int64_t i = 0; OB_SUCC(ret) && i < meta.column_cnt_; ++i) {
      const ObSSTableColumnMeta& col_meta = meta.column_metas_.at(i);
      if (col_meta.column_id_ > static_cast<int64_t>(OB_MIN_SHADOW_COLUMN_ID)) {
        // skip shadow column
      } else if (OB_FAIL(cc_map.get_refactored(col_meta.column_id_, entry))) {
        if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
          if (OB_FAIL(cc_map.set_refactored(
                  col_meta.column_id_, ObColumnChecksumEntry(col_meta.column_checksum_, table_id)))) {
            LOG_WARN("failed to set column checksum entry", K(ret), K(col_meta), K(table_id));
          }
        } else {
          LOG_WARN("failed to get column checksum", K(ret), K(col_meta), K(table_id));
        }
      } else if (col_meta.column_checksum_ != entry.checksum_) {
        ret = OB_CHECKSUM_ERROR;
        LOG_ERROR("column checksum is not equal",
            K(ret),
            "column_id",
            col_meta.column_id_,
            "table_id",
            entry.table_id_,
            "other table_id",
            table_id,
            "checksum",
            entry.checksum_,
            "other checksum",
            col_meta.column_checksum_,
            K(meta));
      }
    }
  }
  return ret;
}

void ObPartitionStorage::handle_error_index_table(
    const ObSSTable& index_table, const ObIArray<share::schema::ObIndexTableStat>& index_stats, int& result)
{
  int ret = OB_SUCCESS;
  result = OB_ERR_PRIMARY_KEY_DUPLICATE;
  int64_t j = 0;
  for (j = 0; j < index_stats.count(); ++j) {
    if (index_stats.at(j).index_id_ == index_table.get_key().table_id_) {
      break;
    }
  }
  if (j == index_stats.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, fail to find index stats", K(ret), K(index_table), K(index_stats));
  } else {
    // index building, not stopping merge
    const share::schema::ObIndexTableStat& index_stat = index_stats.at(j);
    if (share::schema::INDEX_STATUS_UNAVAILABLE == index_stat.index_status_ ||
        is_error_index_status(index_stat.index_status_, index_stat.is_drop_schema_)) {
      LOG_WARN("fail to build index, row count is not the same", K(ret), K(index_stats.at(j)));
      result = OB_SUCCESS;
    } else {
      LOG_ERROR("fail to merge index, row count is not the same",
          K(result),
          "index_id",
          index_table.get_key().table_id_,
          K(index_stats.at(j)));
    }
  }
}

}  // namespace storage
}  // namespace oceanbase
