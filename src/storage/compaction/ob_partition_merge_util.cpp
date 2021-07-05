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
#include "ob_partition_merge_util.h"
#include "share/stat/ob_stat_manager.h"
#include "storage/ob_row_fuse.h"
#include "storage/ob_i_store.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "share/config/ob_server_config.h"
#include "sql/ob_sql_utils.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_partition_merge_task.h"
#include "storage/ob_store_row_comparer.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"
#include "storage/ob_file_system_util.h"
#include "lib/file/file_directory_utils.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace storage;
using namespace memtable;

namespace compaction {

int ObBuildIndexContext::add_main_table_checksum(
    const int64_t* main_table_checksum, const int64_t row_cnt, const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  if (column_cnt <= 0 || row_cnt < 0 || OB_ISNULL(main_table_checksum)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(column_cnt), K(row_cnt), KP(main_table_checksum));
  } else if (column_cnt_ != 0 && column_cnt_ != column_cnt) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, column count of each task must equal", K(ret), K(column_cnt), K(column_cnt_));
  } else {
    ObSpinLockGuard guard(lock_);
    if (OB_ISNULL(main_table_checksum_)) {
      if (OB_ISNULL(main_table_checksum_ = static_cast<int64_t*>(allocator_.alloc(column_cnt * sizeof(int64_t))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to allocate memory for main table checksum", K(ret));
      } else {
        memset(main_table_checksum_, 0, sizeof(int64_t) * column_cnt);
      }
    }

    if (OB_SUCC(ret)) {
      row_cnt_ += row_cnt;
      column_cnt_ = column_cnt;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
        main_table_checksum_[i] += main_table_checksum[i];
      }
    }
  }
  return ret;
}

int ObBuildIndexContext::check_column_checksum(
    const int64_t* index_table_checksum, const int64_t index_row_cnt, const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_ISNULL(index_table_checksum) || index_row_cnt < 0 || column_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(index_table_checksum), K(index_row_cnt), K(column_cnt));
  } else if (OB_UNLIKELY(column_cnt != column_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "error unexpected, column count of main table is not equal with index table",
        K(ret),
        "main_table_column_cnt",
        column_cnt_,
        "index_table_column_cnt",
        column_cnt);
  } else if (OB_UNLIKELY(row_cnt_ != index_row_cnt)) {
    ret = OB_CHECKSUM_ERROR;
    STORAGE_LOG(WARN,
        "checksum error, index table row count is equal to main table row count",
        K(ret),
        "main_table_row_count",
        row_cnt_,
        "index_table_row_count",
        index_row_cnt);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      if (OB_UNLIKELY(main_table_checksum_[i] != index_table_checksum[i])) {
        ret = OB_CHECKSUM_ERROR;
        STORAGE_LOG(WARN,
            "checksum error, main table checksum is not equal to index table checksum",
            K(ret),
            K(i),
            "main_table_column_check_sum",
            main_table_checksum_[i],
            "index_table_column_checksum",
            index_table_checksum[i]);
      }
    }
  }
  return ret;
}

int ObBuildIndexContext::preallocate_local_sorters(const int64_t concurrent_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(concurrent_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(concurrent_cnt));
  } else {
    void* buf = NULL;
    ObExternalSort<ObStoreRow, ObStoreRowComparer>* local_sort = NULL;
    if (OB_FAIL(sorters_.reserve(concurrent_cnt))) {
      STORAGE_LOG(WARN, "fail to reserve memory for sorters array", K(ret), K(concurrent_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < concurrent_cnt; ++i) {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ExternalSort)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
      } else if (OB_ISNULL(local_sort = new (buf) ExternalSort())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to placement new ExternalSort", K(ret));
      } else if (OB_FAIL(sorters_.push_back(local_sort))) {
        STORAGE_LOG(WARN, "fail to push back local sorter", K(ret));
      }
    }
  }
  return ret;
}

void ObBuildIndexContext::add_index_macro_cnt(const int64_t index_macro_cnt)
{
  ObSpinLockGuard guard(lock_);
  index_macro_cnt_ += index_macro_cnt;
}

void ObBuildIndexContext::destroy()
{
  is_report_succ_ = false;
  update_sstore_snapshot_version_ = 0;
  is_unique_checking_complete_ = false;
  build_index_ret_ = OB_SUCCESS;
  need_build_ = false;
  main_table_checksum_ = NULL;
  column_cnt_ = 0;
  row_cnt_ = 0;
  for (int64_t i = 0; i < sorters_.count(); ++i) {
    sorters_.at(i)->clean_up();
    sorters_.at(i)->~ObExternalSort();
  }
  is_update_sstore_complete_ = true;
  output_sstable_ = NULL;
  allocator_.reset();
}

/**
 * ---------------------------------------------------------ObMacroRowIterator--------------------------------------------------------------
 */

ObMacroRowIterator::Param::Param()
    : memctx_factory_(NULL),
      schema_(NULL),
      column_ids_(NULL),
      table_(NULL),
      is_base_iter_(false),
      is_last_iter_(false),
      is_full_merge_(false),
      merge_level_(MACRO_BLOCK_MERGE_LEVEL),
      row_store_type_(FLAT_ROW_STORE),
      range_(NULL),
      multi_version_row_info_(NULL),
      merge_type_(ObMergeType::INVALID_MERGE_TYPE),
      version_range_(),
      merge_log_ts_(INT64_MAX),
      is_iter_overflow_to_complement_(false),
      is_sstable_cut_(false)
{}

bool ObMacroRowIterator::Param::is_valid() const
{
  return NULL != memctx_factory_ && NULL != schema_ && NULL != column_ids_ && NULL != table_ && NULL != range_ &&
         merge_type_ > ObMergeType::INVALID_MERGE_TYPE && merge_type_ < ObMergeType::MERGE_TYPE_MAX &&
         (!is_multi_version_minor_merge(merge_type_) || NULL != multi_version_row_info_) && log_ts_range_.is_valid();
}

ObMacroRowIterator::ObMacroRowIterator()
    : table_id_(OB_INVALID_ID),
      rowkey_column_cnt_(0),
      column_ids_(NULL),
      schema_version_(0),
      table_(NULL),
      row_iter_(NULL),
      curr_row_(NULL),
      ctx_factory_(NULL),
      ctx_(),
      curr_block_desc_(),
      context_(),
      block_cache_ws_(),
      allocator_(ObModIds::OB_CS_MERGER),
      stmt_allocator_(ObModIds::OB_CS_MERGER),
      is_iter_end_(false),
      use_block_(false),
      is_base_iter_(false),
      is_last_iter_(false),
      is_sstable_iter_(false),
      is_inited_(false),
      merge_range_(),
      macro_range_(),
      curr_range_(),
      micro_block_iter_(),
      micro_scanner_(NULL),
      curr_micro_block_(NULL),
      advice_merge_level_(MACRO_BLOCK_MERGE_LEVEL),
      curr_merge_level_(MACRO_BLOCK_MERGE_LEVEL),
      macro_block_opened_(false),
      micro_block_opened_(false),
      row_store_type_(FLAT_ROW_STORE),
      micro_block_count_(-1),
      reuse_micro_block_count_(-1),
      iter_row_count_(0),
      magic_row_count_(0),
      purged_count_(0),
      multi_version_row_info_(NULL),
      macro_reader_(),
      need_rewrite_dirty_macro_block_(true),
      file_handle_()
{}

ObMacroRowIterator::~ObMacroRowIterator()
{
  destroy();
}

int ObMacroRowIterator::init_iter_mode(const Param& param, ObTableIterParam::ObIterTransNodeMode& iter_mode)
{
  int ret = OB_SUCCESS;
  if (param.is_iter_overflow_to_complement_) {
    if (!param.is_last_iter_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only last memtable can iter to complement", K(ret), K(param));
    } else {
      iter_mode = ObTableIterParam::OIM_ITER_OVERFLOW_TO_COMPLEMENT;
    }
  } else if (param.is_last_iter_) {
    iter_mode = ObTableIterParam::OIM_ITER_NON_OVERFLOW_TO_MINI;
  } else {
    iter_mode = ObTableIterParam::OIM_ITER_FULL;
  }
  return ret;
}

int ObMacroRowIterator::init(const Param& param, const ObPartitionKey& pg_key)
{
  int ret = OB_SUCCESS;
  const bool is_multi_version_minor_merge = storage::is_multi_version_minor_merge(param.merge_type_);
  const ObTableSchema* schema = param.schema_;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments, ", K(ret), K(param));
  } else if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObMacroRowIterator has been inited, ", K(ret));
  } else if (param.table_->is_sstable() &&
             OB_FAIL(file_handle_.assign(static_cast<ObSSTable*>(param.table_)->get_storage_file_handle()))) {
    LOG_WARN("fail to get file handle", K(ret), K(param.table_->get_partition_key()));
  } else {
    const uint64_t tenant_id = extract_tenant_id(schema->get_table_id());
    ObQueryFlag query_flag(ObQueryFlag::Forward,
        true, /*is daily merge scan*/
        true, /*is read multiple macro block*/
        true, /*sys task scan, read one macro block in single io*/
        false /*is full row scan?*/,
        false,
        false);
    query_flag.multi_version_minor_merge_ = is_multi_version_minor_merge;
    query_flag.is_sstable_cut_ = param.is_sstable_cut_;
    multi_version_row_info_ = is_multi_version_minor_merge ? param.multi_version_row_info_ : nullptr;
    ctx_factory_ = param.memctx_factory_;

    if (OB_FAIL(block_cache_ws_.init(tenant_id))) {
      LOG_WARN("block_cache_ws init failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(param_.init(schema->get_table_id(),
                   schema->get_schema_version(),
                   schema->get_rowkey_column_num(),
                   *param.column_ids_,
                   is_multi_version_minor_merge))) {
      LOG_WARN("init table access param failed", K(ret));
    } else if (OB_ISNULL(ctx_.mem_ctx_ = param.memctx_factory_->alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("cannot allocate memory context for scan context, ", K(ret));
    } else if (OB_FAIL(ctx_.mem_ctx_->trans_begin())) {
      LOG_WARN("fail to begin transaction.", K(ret));
    } else if (OB_FAIL(ctx_.mem_ctx_->sub_trans_begin(
                   param.version_range_.snapshot_version_, INT64_MAX - 2, true /*safe read*/))) {
      LOG_WARN("fail to begin sub transaction.", K(ret));
    } else if (OB_FAIL(ctx_.init_trans_ctx_mgr(pg_key))) {
      LOG_WARN("failed to init store ctx", K(ret));
    } else if (OB_FAIL(context_.init(
                   query_flag, ctx_, allocator_, stmt_allocator_, block_cache_ws_, param.version_range_))) {
      LOG_WARN("init table access context failed", K(ret));
    } else if (OB_FAIL(init_iter_mode(param, param_.iter_param_.iter_mode_))) {
      LOG_WARN("failed to init iter mode", K(ret));
    } else {
      ctx_.mem_ctx_->set_multi_version_start(param.version_range_.multi_version_start_);
      ctx_.mem_ctx_->set_base_version(param.version_range_.base_version_);
      context_.merge_log_ts_ = param.merge_log_ts_;
      context_.read_out_type_ =
          (is_multi_version_minor_merge && GCONF._enable_sparse_row) ? SPARSE_ROW_STORE : FLAT_ROW_STORE;
      LOG_DEBUG("cal read out row type", K(ret), K(*this), K(context_.read_out_type_), K(param.merge_type_));
      if (OB_FAIL(inner_init())) {
        LOG_WARN("failed to inner init", K(ret));
      } else {
        if (((param.is_base_iter_ && param.table_->is_sstable()) || param.table_->is_major_sstable()) &&
            !param.is_full_merge_) {
          if (OB_FAIL(init_macro_iter(param, pg_key))) {
            LOG_WARN("failed to init macro iter", K(ret));
          }
        } else {
          if (OB_FAIL(init_row_iter(param))) {
            LOG_WARN("failed to init row iter", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    table_id_ = schema->get_table_id();
    rowkey_column_cnt_ = schema->get_rowkey_column_num();
    column_ids_ = param.column_ids_;
    schema_version_ = schema->get_schema_version();
    table_ = param.table_;
    use_block_ = ((param.is_base_iter_ || table_->is_major_sstable()) && !param.is_full_merge_);
    is_iter_end_ = false;
    is_base_iter_ = param.is_base_iter_;
    is_last_iter_ = param.is_last_iter_;
    is_sstable_iter_ = param.table_->is_sstable();
    merge_range_ = *param.range_;

    curr_range_.reset();
    micro_block_iter_.reset();
    micro_scanner_ = NULL;
    micro_row_scanner_.reset();
    multi_version_micro_row_scanner_.reset();
    curr_micro_block_ = NULL;
    advice_merge_level_ = param.merge_level_;
    curr_merge_level_ = param.merge_level_;
    macro_block_opened_ = false;
    micro_block_opened_ = false;
    row_store_type_ = param.row_store_type_;
    micro_block_count_ = -1;
    reuse_micro_block_count_ = -1;
    iter_row_count_ = 0;

    // choose micro scanner
    if (table_->is_multi_version_minor_sstable() && is_multi_version_minor_merge) {
      // multi version minor merge should NOT use micro-level incremental merge
      micro_scanner_ = NULL;
    } else if (table_->is_multi_version_minor_sstable()) {
      micro_scanner_ = &multi_version_micro_row_scanner_;
    } else {
      micro_scanner_ = &micro_row_scanner_;
    }

    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObMacroRowIterator::init_macro_iter(const Param& param, const ObPartitionKey& pg_key)
{
  int ret = OB_SUCCESS;
  UNUSED(pg_key);
  ObSSTable* sstable = NULL;
  const ObTableSchema* schema = param.schema_;

  if (OB_UNLIKELY(!param.table_->is_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The table type is not sstable, ", K(ret), K(schema->get_table_id()));
  } else {
    if (is_multi_version_minor_merge(param.merge_type_)) {
      need_rewrite_dirty_macro_block_ = true;
    }
    sstable = static_cast<ObSSTable*>(param.table_);
    if (param.range_->get_range().is_whole_range()) {
      if (OB_FAIL(sstable->scan_macro_block(macro_block_iter_))) {
        LOG_WARN("Fail to get macro_block_iter", K(ret), K(schema->get_table_id()));
      }
    } else {
      if (OB_FAIL(sstable->scan_macro_block(*param.range_, macro_block_iter_))) {
        LOG_WARN("Fail to get macro_block_iter", K(ret), "table_id", schema->get_table_id(), K(*param.range_));
      }
    }
  }
  return ret;
}

int ObMacroRowIterator::init_row_iter(const Param& param)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* iter = nullptr;
  ObVPCompactIter* vp_iter = nullptr;
  const ObTableSchema* schema = param.schema_;

  if (OB_FAIL(param.table_->scan(param_.iter_param_, context_, *param.range_, iter))) {
    LOG_WARN("Fail to get row_iter, ", K(ret), "sstable", *param.table_);
  } else {
    row_iter_ = iter;
    iter = nullptr;
  }

  if (nullptr != iter) {
    iter->~ObStoreRowIterator();
    iter = nullptr;
  }

  if (nullptr != vp_iter) {
    vp_iter->~ObVPCompactIter();
    vp_iter = nullptr;
  }
  return ret;
}

void ObMacroRowIterator::destroy()
{
  if (NULL != row_iter_) {
    row_iter_->~ObIStoreRowIterator();
  }

  if (NULL != ctx_.mem_ctx_) {
    ctx_.mem_ctx_->trans_end(true, 0);
    ctx_.mem_ctx_->trans_clear();
    if (NULL != ctx_factory_) {
      ctx_factory_->free(ctx_.mem_ctx_);
      ctx_.mem_ctx_ = NULL;
    }
  }

  table_id_ = OB_INVALID_ID;
  rowkey_column_cnt_ = 0;
  column_ids_ = NULL;
  schema_version_ = 0;
  table_ = NULL;
  row_iter_ = NULL;
  curr_row_ = NULL;
  ctx_factory_ = NULL;
  is_iter_end_ = false;
  is_base_iter_ = false;
  is_sstable_iter_ = false;
  use_block_ = false;
  is_inited_ = false;
  merge_range_.reset();
  macro_range_.reset();
  curr_range_.reset();
  micro_block_iter_.reset();
  micro_scanner_ = NULL;
  micro_row_scanner_.reset();
  multi_version_micro_row_scanner_.reset();
  curr_micro_block_ = NULL;
  advice_merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  curr_merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  macro_block_opened_ = false;
  micro_block_opened_ = false;
  row_store_type_ = FLAT_ROW_STORE;
  iter_row_count_ = 0;
  magic_row_count_ = 0;
  multi_version_row_info_ = NULL;
  allocator_.reuse();
  stmt_allocator_.reuse();
  need_rewrite_dirty_macro_block_ = true;
  file_handle_.reset();
}

int ObMacroRowIterator::next()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroRowIterator has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(is_iter_end_)) {
    ret = OB_ITER_END;
  } else {
    const bool iter_row = (NULL != row_iter_) || (MICRO_BLOCK_MERGE_LEVEL == curr_merge_level_ && micro_block_opened_);
    if (iter_row) {
      if (NULL != row_iter_) {
        if (OB_FAIL(row_iter_->get_next_row(curr_row_))) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            row_iter_->~ObIStoreRowIterator();
            row_iter_ = NULL;
            curr_row_ = NULL;
          }
        } else {
          ++iter_row_count_;
          if (curr_row_->row_pos_flag_.is_micro_first()) {
            ++micro_block_count_;
          }
        }
      } else if (OB_FAIL(micro_scanner_->get_next_row(curr_row_))) {
        if (OB_ITER_END == ret) {
          curr_row_ = NULL;
          micro_block_opened_ = false;
        }
      } else {
        ++iter_row_count_;
      }

      if (OB_FAIL(ret)) {
        if (OB_ITER_END == ret) {
          if (use_block_ && OB_FAIL(next_range())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("next_range failed", K(ret));
            }
          }
        } else {
          LOG_WARN("Fail to get next row, ", K(ret));
        }
      } else if (!curr_row_->is_valid()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid curr_row", K(ret), K(*curr_row_));
        right_to_die_or_duty_to_live();
      }
    } else if (use_block_ && OB_FAIL(next_range())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("next_range failed", K(ret));
      }
    }

    if (OB_ITER_END == ret) {
      is_iter_end_ = true;
    }
  }
  if (NULL != curr_row_) {
    STORAGE_LOG(DEBUG,
        "macro_row_iter next",
        K(ret),
        KP(this),
        K(table_->get_key()),
        KP(table_),
        KP(row_iter_),
        K(curr_row_),
        K(*curr_row_));
  } else {
    STORAGE_LOG(DEBUG, "next", K(ret), KP(this), KP(table_), KP(row_iter_), KP(curr_row_), K(*curr_row_));
  }
  return ret;
}

int ObMacroRowIterator::open_curr_range()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (micro_block_opened_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("micro block opened, can't open it twice", K(ret));
  } else if (MACRO_BLOCK_MERGE_LEVEL == curr_merge_level_ && macro_block_opened_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("macro block opened, can't open it twice", K_(curr_merge_level), K_(macro_block_opened), K(ret));
  } else if (macro_block_opened_) {
    if (OB_FAIL(open_curr_micro_block())) {
      LOG_WARN("open_curr_micro_block failed", K(ret));
    }
  } else {
    if (OB_FAIL(open_curr_macro_block())) {
      LOG_WARN("open_curr_macro_block failed", K(ret));
    }
  }
  return ret;
}
int ObMacroRowIterator::open_curr_macro_block()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(open_curr_macro_block_())) {
    LOG_WARN("Fail to open curr macro block", K(ret));
  } else if (OB_FAIL(next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Fail to get next row, ", K(ret));
    }
  }
  return ret;
}

int ObMacroRowIterator::open_curr_macro_block_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (MACRO_BLOCK_MERGE_LEVEL == curr_merge_level_) {
    macro_range_.reset();
    allocator_.reuse();
    macro_range_.get_range() = curr_range_;
    ObStoreRowIterator* iter = NULL;
    if (OB_UNLIKELY(!use_block_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected error, the ObMacroRowIterator does not use block, ", K(ret));
    } else if (OB_FAIL(macro_range_.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
      STORAGE_LOG(WARN, "fail to get collation free range", K(ret));
    } else if (OB_FAIL(table_->scan(param_.iter_param_, context_, macro_range_, iter))) {
      LOG_WARN("Fail to scan row_iter, ", K_(table_id), K(ret));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The row_iter_ is NULL, ", K(ret));
    } else {
      macro_block_opened_ = true;
      row_iter_ = iter;
    }
  } else {
    micro_block_iter_.reset();
    ObStorageFile* file = NULL;
    if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "fail to get pg file", K(ret), K(file_handle_));
    } else if (OB_FAIL(micro_block_iter_.init(
                   table_id_, *column_ids_, curr_block_desc_.range_, curr_block_desc_.macro_block_ctx_, file))) {
      LOG_WARN("micro_block_iter_ init failed",
          K(ret),
          K_(table_id),
          "column_ids",
          *column_ids_,
          K_(curr_block_desc),
          KP(file));
    } else {
      micro_block_opened_ = false;
      macro_block_opened_ = true;
      micro_block_count_ = -1;
      reuse_micro_block_count_ = -1;
      micro_scanner_->reset();
    }
  }
  return ret;
}

int ObMacroRowIterator::open_curr_micro_block()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(micro_scanner_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_scanner_ is NULL", K(ret));
  } else {
    const bool is_left_border = micro_block_iter_.is_left_border();
    const bool is_right_border = micro_block_iter_.is_right_border();
    ObMicroBlockData decompressed_data;
    bool is_compressed = false;
    micro_scanner_->reset();
    if (OB_UNLIKELY(!curr_micro_block_->range_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("curr micro range is not valid", K(ret));
    } else if (OB_FAIL(micro_scanner_->init(param_.iter_param_, context_, static_cast<ObSSTable*>(table_)))) {
      LOG_WARN("failed to init micro row scanner", K(ret), K(param_), K(context_));
    } else if (OB_FAIL(micro_scanner_->set_range(curr_micro_block_->range_))) {
      LOG_WARN("failed to init micro scanner", K(ret));
    } else if (OB_FAIL(macro_reader_.decompress_data(curr_micro_block_->meta_,
                   curr_micro_block_->data_.get_buf(),
                   curr_micro_block_->data_.get_buf_size(),
                   decompressed_data.get_buf(),
                   decompressed_data.get_buf_size(),
                   is_compressed))) {
      LOG_WARN("fail to decompress data", K(ret));
    } else if (OB_FAIL(micro_scanner_->open(curr_block_desc_.macro_block_ctx_.get_macro_block_id(),
                   curr_block_desc_.full_meta_,
                   decompressed_data,
                   is_left_border,
                   is_right_border))) {
      LOG_WARN("failed to open micro scanner", K(ret));
    } else {
      micro_block_opened_ = true;
      if (OB_FAIL(next())) {
        LOG_WARN("next failed", K(ret));
      }
    }
  }
  return ret;
}

bool ObMacroRowIterator::macro_block_opened() const
{
  bool bret = macro_block_opened_;
  if (!macro_block_opened_ && OB_NOT_NULL(curr_row_)) {
    FLOG_WARN("row is not null and macro is not opened", K(bret), KPC(curr_row_));
  }
  return bret;
}

int ObMacroRowIterator::next_range()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (MICRO_BLOCK_MERGE_LEVEL == curr_merge_level_ && macro_block_opened_) {
    if (OB_FAIL(micro_block_iter_.next(curr_micro_block_))) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(macro_block_iter_.get_next_macro_block(curr_block_desc_))) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            is_iter_end_ = true;
          } else {
            LOG_WARN("Fail to get next macro block, ", K(ret));
          }
        } else {
          curr_range_ = curr_block_desc_.range_;
          macro_block_opened_ = false;
          choose_merge_level();
        }
      } else {
        LOG_WARN("micro_block_iter next failed", K(ret));
      }
    } else {
      curr_range_ = curr_micro_block_->range_;
      micro_block_opened_ = false;
      ++micro_block_count_;
      ++reuse_micro_block_count_;
    }
  } else {
    if (OB_FAIL(macro_block_iter_.get_next_macro_block(curr_block_desc_))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        is_iter_end_ = true;
      } else {
        LOG_WARN("Fail to get next macro block, ", K(ret));
      }
    } else {
      bool need_open = false;
      curr_range_ = curr_block_desc_.range_;
      macro_block_opened_ = false;
      choose_merge_level();

      if (OB_NOT_NULL(multi_version_row_info_) && table_->is_sstable()) {
        // parallel minor merge need consider the range split
        // parallel minor always use macro block level
        if (merge_range_.get_range().is_whole_range()) {
        } else {
          if (merge_range_.get_range().get_start_key().compare(curr_range_.get_start_key()) > 0) {
            curr_range_.get_start_key() = merge_range_.get_range().get_start_key();
            need_open = true;
          }
          if (merge_range_.get_range().get_end_key().compare(curr_range_.get_end_key()) <= 0) {
            curr_range_.get_end_key() = merge_range_.get_range().get_end_key();
            need_open = true;
          }
        }
      }
      if (OB_SUCC(ret) && need_open) {
        // open macro block and next, only use next in base class @attension
        if (OB_FAIL(open_curr_macro_block_())) {
          LOG_WARN("Fail to open curr macro block", K(ret));
        } else if (OB_FAIL(ObMacroRowIterator::next())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to get next row, ", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

void ObMacroRowIterator::choose_merge_level()
{
  if (curr_block_desc_.schema_version_ <= 0 || curr_block_desc_.schema_version_ != schema_version_) {
    curr_merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  } else if (row_store_type_ != curr_block_desc_.row_store_type_) {
    // all micro block should be rewrite if row store type change.
    curr_merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  } else if (table_->is_multi_version_minor_sstable() && context_.query_flag_.is_multi_version_minor_merge()) {
    // multi version minor merge should NOT use micro-level incremental merge
    curr_merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  } else {
    curr_merge_level_ = advice_merge_level_;
  }
}

int ObMacroRowIterator::compare(const ObMacroRowIterator& other, int64_t& cmp_ret)
{
  int ret = OB_SUCCESS;
  ObStoreRowkey left_rowkey;
  ObStoreRowkey right_rowkey;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroRowIterator has not been inited, ", K(ret));
  } else if (NULL != curr_row_) {
    left_rowkey.assign(curr_row_->row_val_.cells_, rowkey_column_cnt_);
    if (NULL != other.curr_row_) {
      right_rowkey.assign(other.curr_row_->row_val_.cells_, rowkey_column_cnt_);
      cmp_ret = left_rowkey.compare(right_rowkey);
      if (cmp_ret > 0) {
        cmp_ret = 1;
      } else if (cmp_ret < 0) {
        cmp_ret = -1;
      }
    } else {
      cmp_ret = compare(left_rowkey, other.curr_range_);
      if (0 == cmp_ret) {
        cmp_ret = CANNOT_COMPARE_RIGHT_IS_RANGE;
      }
    }
  } else {
    if (NULL != other.curr_row_) {
      // when doing minor merge, the only case we need to use trans version column in comparison
      // is when left is a range and right is a row
      right_rowkey.assign(other.curr_row_->row_val_.cells_, get_rowkey_column_cnt());
      cmp_ret = -compare(right_rowkey, curr_range_);
      if (0 == cmp_ret) {
        cmp_ret = CANNOT_COMPARE_LEFT_IS_RANGE;
      }
    } else {
      left_rowkey = curr_range_.get_start_key();
      right_rowkey = curr_range_.get_end_key();
      cmp_ret = compare(left_rowkey, other.curr_range_);
      if (cmp_ret > 0) {
        cmp_ret = 1;
      } else if (cmp_ret == 0) {
        if (left_rowkey.compare(other.curr_range_.get_end_key()) == 0 &&
            !curr_range_.get_border_flag().inclusive_start()) {
          cmp_ret = 1;
        } else if (left_rowkey.compare(other.curr_range_.get_start_key()) == 0) {
          if (curr_range_.get_border_flag().inclusive_start()) {
            cmp_ret = CANNOT_COMPARE_BOTH_ARE_RANGE;
          } else {
            cmp_ret = CANNOT_COMPARE_RIGHT_IS_RANGE;
          }
        } else {
          cmp_ret = CANNOT_COMPARE_RIGHT_IS_RANGE;
        }
      } else {
        cmp_ret = compare(right_rowkey, other.curr_range_);
        if (cmp_ret < 0) {
          cmp_ret = -1;
        } else if (cmp_ret == 0) {
          if (right_rowkey.compare(other.curr_range_.get_start_key()) == 0 &&
              !curr_range_.get_border_flag().inclusive_end()) {
            cmp_ret = -1;
          } else {
            cmp_ret = CANNOT_COMPARE_LEFT_IS_RANGE;
          }
        } else {
          cmp_ret = CANNOT_COMPARE_LEFT_IS_RANGE;
        }
      }
    }
  }

  return ret;
}

void ObMacroRowIterator::get_border_key(const ObStoreRowkey& border_key, const bool is_start_key, ObStoreRowkey& rowkey)
{
  if (NULL == multi_version_row_info_) {
    rowkey.assign(const_cast<ObObj*>(border_key.get_obj_ptr()), border_key.get_obj_cnt());
  } else {
    rowkey.assign(cells_, multi_version_row_info_->multi_version_rowkey_column_cnt_);
    for (int64_t i = 0; i < rowkey_column_cnt_; ++i) {
      cells_[i] = border_key.get_obj_ptr()[i];
    }
    if (is_start_key) {
      cells_[multi_version_row_info_->trans_version_index_].set_min_value();
      cells_[multi_version_row_info_->trans_version_index_ + 1].set_min_value();
    } else {
      cells_[multi_version_row_info_->trans_version_index_].set_max_value();
      cells_[multi_version_row_info_->trans_version_index_ + 1].set_max_value();
    }
  }
}

int64_t ObMacroRowIterator::compare(const ObStoreRowkey& rowkey, const ObStoreRange& range)
{
  int64_t cmp_ret = 0;
  int32_t left_cmp = 0;
  int32_t right_cmp = 0;
  ObStoreRowkey start_key;
  ObStoreRowkey end_key;

  get_border_key(range.get_start_key(), true, start_key);
  left_cmp = rowkey.compare(start_key);
  if (left_cmp < 0) {
    cmp_ret = -1;
  } else if (left_cmp == 0) {
    if (range.get_border_flag().inclusive_start()) {
      cmp_ret = 0;
    } else {
      cmp_ret = -1;
    }
  } else {
    get_border_key(range.get_end_key(), false, end_key);
    right_cmp = rowkey.compare(end_key);
    if (right_cmp > 0) {
      cmp_ret = 1;
    } else if (right_cmp == 0) {
      if (range.get_border_flag().inclusive_end()) {
        cmp_ret = 0;
      } else {
        cmp_ret = 1;
      }
    } else {
      cmp_ret = 0;
    }
  }

  return cmp_ret;
}

int ObMacroRowIterator::multi_version_compare(const ObMacroRowIterator& other, int64_t& cmp_ret)
{
  int ret = OB_SUCCESS;
  int64_t left_trans_version = INT64_MAX;
  int64_t right_trans_version = INT64_MAX;
  int64_t trans_version_index = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroRowIterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(curr_row_) || OB_ISNULL(other.curr_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the cur row and other cur row should not be NULL", K(ret), KP_(curr_row), KP(other.curr_row_));
  } else if (OB_ISNULL(multi_version_row_info_) || !multi_version_row_info_->is_valid()) {
    ret = OB_ERR_SYS;
    LOG_WARN("multi version row info is invalid", K(ret), KP(multi_version_row_info_));
  } else if ((trans_version_index = multi_version_row_info_->trans_version_index_) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the multi version trans version index is invalid", K(ret), K(trans_version_index));
  } else {
    left_trans_version = !table_->is_multi_version_table() ? -table_->get_snapshot_version()
                                                           : curr_row_->row_val_.cells_[trans_version_index].get_int();
    right_trans_version = !other.table_->is_multi_version_table()
                              ? -other.table_->get_snapshot_version()
                              : other.curr_row_->row_val_.cells_[trans_version_index].get_int();
    if (left_trans_version < right_trans_version) {
      cmp_ret = -1;
    } else if (left_trans_version > right_trans_version) {
      cmp_ret = 1;
    } else {
      cmp_ret = 0;
    }
  }
  return ret;
}

int ObMacroRowIterator::compact_multi_version_row(const ObMacroRowIterator& other, memtable::ObNopBitMap& nop_pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroRowIterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(curr_row_) || OB_ISNULL(other.curr_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the cur row and other cur row should not be NULL", K(ret), KP_(curr_row), KP(other.curr_row_));
  } else if (OB_ISNULL(multi_version_row_info_) || !multi_version_row_info_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the multi version row info is invalid", K(ret), KP(multi_version_row_info_));
  } else if (curr_row_->is_delete() || other.curr_row_->is_delete()) {
    // do nothing
  } else {
    int64_t curr_row_index = 0;
    int64_t other_row_index = 0;
    curr_row_index = !table_->is_multi_version_table() ? rowkey_column_cnt_
                                                       : multi_version_row_info_->multi_version_rowkey_column_cnt_;
    other_row_index = !other.table_->is_multi_version_table()
                          ? rowkey_column_cnt_
                          : multi_version_row_info_->multi_version_rowkey_column_cnt_;
    for (; curr_row_index < curr_row_->row_val_.count_; ++curr_row_index, ++other_row_index) {
      if (nop_pos.test(curr_row_index)) {
        if (curr_row_->row_val_.cells_[curr_row_index].is_nop_value() &&
            !other.curr_row_->row_val_.cells_[other_row_index].is_nop_value()) {
          curr_row_->row_val_.cells_[curr_row_index] = other.curr_row_->row_val_.cells_[other_row_index];
          nop_pos.set_false(curr_row_index);
        } else if (!curr_row_->row_val_.cells_[curr_row_index].is_nop_value()) {
          nop_pos.set_false(curr_row_index);
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObMacroRowIterator::compact_multi_version_sparse_row(const ObMacroRowIterator& other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroRowIterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(curr_row_) || OB_ISNULL(other.curr_row_) || !curr_row_->is_sparse_row_ ||
             !other.curr_row_->is_sparse_row_ || OB_ISNULL(curr_row_->column_ids_) ||
             OB_ISNULL(other.curr_row_->column_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rows are not valid", K(ret), K(*this), K(*curr_row_), K(*(other.curr_row_)));
  } else if (OB_ISNULL(multi_version_row_info_) || !multi_version_row_info_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the multi version row info is invalid", K(ret), KP(multi_version_row_info_));
  } else if (curr_row_->is_delete() || other.curr_row_->is_delete()) {
    // do nothing
  } else {
    int64_t curr_row_index = 0;
    int64_t other_row_index = 0;
    ObFixedBitSet<OB_ALL_MAX_COLUMN_ID> bit_set;
    for (int64_t i = 0; OB_SUCC(ret) && i < curr_row_->row_val_.count_; ++i) {
      if (OB_FAIL(bit_set.add_member(curr_row_->column_ids_[i]))) {
        STORAGE_LOG(WARN, "add column id into set failed", K(*curr_row_));
      }
    }
    if (OB_SUCC(ret)) {
      curr_row_index = curr_row_->row_val_.count_;
      other_row_index = !other.table_->is_multi_version_table()
                            ? rowkey_column_cnt_
                            : multi_version_row_info_->multi_version_rowkey_column_cnt_;
      bool found = true;
      for (; OB_SUCC(ret) && other_row_index < other.curr_row_->row_val_.count_; ++other_row_index) {
        found = bit_set.has_member(other.curr_row_->column_ids_[other_row_index]);
        if (!found) {
          if (OB_FAIL(bit_set.add_member(other.curr_row_->column_ids_[other_row_index]))) {
            STORAGE_LOG(WARN,
                "add column id into set failed",
                "index",
                other_row_index,
                "column_id",
                other.curr_row_->column_ids_[other_row_index]);
          } else {
            STORAGE_LOG(DEBUG,
                "add col",
                K(curr_row_index),
                K(other_row_index),
                K(other.curr_row_->column_ids_[other_row_index]),
                K(curr_row_->row_val_.cells_[curr_row_index]));
            curr_row_->row_val_.cells_[curr_row_index] = other.curr_row_->row_val_.cells_[other_row_index];
            curr_row_->column_ids_[curr_row_index] = other.curr_row_->column_ids_[other_row_index];
            ++curr_row_index;
          }
        } else {
          STORAGE_LOG(DEBUG,
              "not need col",
              K(other_row_index),
              K(other.curr_row_->column_ids_[other_row_index]),
              K(other.curr_row_->row_val_.cells_[other_row_index]));
        }
      }
      ObStoreRow* row = const_cast<ObStoreRow*>(curr_row_);
      row->row_val_.count_ = curr_row_index;
      STORAGE_LOG(DEBUG, "compact sparse row", K(*row));
    }
  }
  return ret;
}

int ObMacroRowIterator::get_current_trans_version(int64_t& trans_version)
{
  int ret = OB_SUCCESS;
  trans_version = INT64_MAX;
  int64_t trans_version_index = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroRowIterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(multi_version_row_info_) || !multi_version_row_info_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the multi version row info is invalid", K(ret), KP(multi_version_row_info_));
  } else if (OB_ISNULL(curr_row_)) {
    // do nothing, macro block is not opened
    ret = OB_NOT_OPEN;
    LOG_WARN("macro block not open, cannot get current trans version", K(ret));
  } else if (table_->is_old_minor_sstable()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid old minor sstable", K(ret), K(*table_));
  } else {
    trans_version_index = multi_version_row_info_->trans_version_index_;
    trans_version = !table_->is_multi_version_table() ? table_->get_snapshot_version()
                                                      : -curr_row_->row_val_.cells_[trans_version_index].get_int();
  }
  return ret;
}

int ObMacroRowIterator::exist(const ObStoreRow* row, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = true;
  if (OB_UNLIKELY(NULL == row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", KP(row), K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroRowIterator has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!use_block_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error, the ObMacroRowIterator does not use block, ", K(ret));
  } else {
    bool has_found = false;
    ObStoreRowkey rowkey(row->row_val_.cells_, rowkey_column_cnt_);
    if (OB_FAIL(table_->exist(ctx_, table_id_, rowkey, *column_ids_, is_exist, has_found))) {
      LOG_WARN("Fail to check if exist, ", K(ret));
    }
  }
  return ret;
}

void ObMacroRowIterator::reset_first_multi_version_row_flag()
{
  if (NULL != curr_row_) {
    curr_row_->row_type_flag_.set_first_multi_version_row(false);
  }
}

bool ObMacroRowIterator::need_rewrite_current_macro_block() const
{
  bool bret = false;
  if (need_rewrite_dirty_macro_block_ && curr_block_desc_.contain_uncommitted_row_) {
    bret = true;
    LOG_INFO("need rewrite one dirty macro", K(curr_block_desc_));
  } else if (nullptr != table_ && table_->is_multi_version_minor_sstable() &&
             ObMultiVersionRowkeyHelpper::MVRC_OLD_VERSION ==
                 static_cast<ObSSTable*>(table_)->get_multi_version_rowkey_type()) {
    bret = true;
    LOG_INFO("need rewrite macro block of old minor sstable", K(curr_block_desc_));
  }
  return bret;
}

bool ObMacroRowIterator::is_trans_state_table_valid() const
{
  bool bret = false;
  transaction::ObTransStateTableGuard* trans_table_guard = NULL;
  transaction::ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    LOG_ERROR("The ObMacroRowIterator has not been inited", K(*this));
  } else if (OB_ISNULL(trans_table_guard = ctx_.mem_ctx_->get_trans_table_guard())) {
    LOG_ERROR("The trans state table is invalid", K(*this));
  } else if (OB_ISNULL(ctx_mgr = trans_table_guard->get_trans_state_table().get_partition_trans_ctx_mgr())) {
    LOG_ERROR("The partition ctx mgr is invalid", K(*this));
  } else {
    bret = !ctx_mgr->is_partition_stopped();
  }

  return bret;
}

/*************************ObMinorMergeMacroRowIterator************************************/
ObMinorMergeMacroRowIterator::ObMinorMergeMacroRowIterator()
    : obj_copy_(obj_copy_allocator_), row_queue_(), check_first_row_compacted_(true)

{
  for (int i = 0; i < RNPI_MAX; ++i) {
    nop_pos_[i] = NULL;
    bit_set_[i] = NULL;
  }
}

ObMinorMergeMacroRowIterator::~ObMinorMergeMacroRowIterator()
{
  row_queue_.reset();
  obj_copy_allocator_.reuse();
  reset();
}

void ObMinorMergeMacroRowIterator::reset()
{
  if (FLAT_ROW_STORE == context_.read_out_type_) {
    for (int i = 0; i < RNPI_MAX; ++i) {
      if (OB_NOT_NULL(nop_pos_[i])) {
        nop_pos_[i]->reset();
        nop_pos_[i] = NULL;
      }
    }
  } else {
    for (int i = 0; i < RNPI_MAX; ++i) {
      if (OB_NOT_NULL(bit_set_[i])) {
        bit_set_[i]->reset();
        bit_set_[i] = NULL;
      }
    }
  }
}

int ObMinorMergeMacroRowIterator::next()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMinorMergeMacroRowIterator has not been inited, ", K(ret));
  } else if (row_queue_.has_next()) {  // get row from row_queue
    if (OB_FAIL(row_queue_.get_next_row(curr_row_))) {
      LOG_WARN("Fail to get row from row_queue", K(ret));
    } else {
      LOG_DEBUG("get next row", K(ret), KPC(curr_row_));
    }
  } else if (OB_UNLIKELY(is_iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObMacroRowIterator::next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to call next", K(ret));
    }
  } else {
    row_queue_.reuse();
    obj_copy_allocator_.reuse();
    if (OB_NOT_NULL(curr_row_) && check_first_row_compacted_ && !curr_row_->row_type_flag_.is_uncommitted_row() &&
        !curr_row_->row_type_flag_.is_magic_row()) {
      // the first output row of each rowkey must be compact row
      // skip the uncommited row and magic row(last row)
      check_first_row_compacted_ = false;
      if (curr_row_->row_type_flag_.is_compacted_multi_version_row()) {  // do nothing for compacted row
        // do nothing
      } else if (OB_FAIL(make_first_row_compacted())) {
        LOG_WARN("Fail to compact first row, ", K(ret));
      } else if (OB_FAIL(row_queue_.get_next_row(curr_row_))) {  // return first row in row_queue
        LOG_WARN("Fail to get row from row_queue", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Error to meet iter_end", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(curr_row_)) {
      if (curr_row_->row_type_flag_.is_last_multi_version_row()) {  // meet last row
        check_first_row_compacted_ = true;
      }
      if (curr_row_->row_type_flag_.is_magic_row()) {
        ++magic_row_count_;
      }
      STORAGE_LOG(DEBUG,
          "macro_row_iter next",
          K(ret),
          KP(this),
          K(table_->get_key()),
          KP(table_),
          KP(row_iter_),
          KPC(curr_row_));
    } else if (OB_SUCC(ret) && use_block_ && !macro_block_opened() &&
               need_rewrite_current_macro_block()) {  // open next macro block
      STORAGE_LOG(DEBUG, "call open next block", K(ret), KP(this));
      if (OB_FAIL(ObMacroRowIterator::open_curr_macro_block())) {
        STORAGE_LOG(WARN, "call next in open next macro block", K(ret));
      }
      STORAGE_LOG(DEBUG, "next", K(ret), KP(this), KP(table_), KP(row_iter_), KPC(curr_row_));
    }
  }
  return ret;
}

int ObMinorMergeMacroRowIterator::open_curr_macro_block()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMacroRowIterator::open_curr_macro_block())) {
    LOG_WARN(
        "call open curr macro block failed", K(ret), KP(this), KP(table_), KP(row_iter_), KP(curr_row_), K(*curr_row_));
  } else {
    check_first_row_compacted_ = true;
  }
  return ret;
}

int ObMinorMergeMacroRowIterator::multi_version_compare(const ObMacroRowIterator& other, int64_t& cmp_ret)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMacroRowIterator::multi_version_compare(other, cmp_ret))) {
    LOG_WARN(
        "call open curr macro block failed", K(ret), KP(this), KP(table_), KP(row_iter_), KP(curr_row_), K(*curr_row_));
  } else if (0 == cmp_ret && curr_row_->row_type_flag_.is_uncommitted_row() &&
             other.get_curr_row()->row_type_flag_.is_uncommitted_row()) {  // equal trans version
    int64_t sql_sequence_index = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
        multi_version_row_info_->rowkey_column_cnt_, multi_version_row_info_->multi_version_rowkey_column_cnt_);
    if (sql_sequence_index < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the sql sequence index is invalid", K(ret), K(sql_sequence_index));
    } else {
      int64_t left_sql_sequence = curr_row_->row_val_.cells_[sql_sequence_index].get_int();
      int64_t right_sql_sequence = other.get_curr_row()->row_val_.cells_[sql_sequence_index].get_int();
      if (left_sql_sequence < right_sql_sequence) {
        cmp_ret = -1;
      } else if (left_sql_sequence > right_sql_sequence) {
        cmp_ret = 1;
      } else {
        cmp_ret = 0;
        LOG_DEBUG("may recovery cause memtable and sstable has same row", K(*curr_row_), K(right_sql_sequence));
      }
    }
  }
  return ret;
}

int ObMinorMergeMacroRowIterator::inner_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_queue_.init(context_.read_out_type_, multi_version_row_info_->column_cnt_))) {
    LOG_WARN("failed to init row_queue", K(ret), K(context_.read_out_type_), K(multi_version_row_info_->column_cnt_));
  } else if (FLAT_ROW_STORE == context_.read_out_type_) {  // read flat row
    void* ptr = NULL;
    for (int i = 0; OB_SUCC(ret) && i < RNPI_MAX; ++i) {  // init nop pos
      if (NULL == (ptr = stmt_allocator_.alloc(sizeof(storage::ObNopPos)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "fail to alloc nop pos");
      } else {
        nop_pos_[i] = new (ptr) storage::ObNopPos();
        if (OB_FAIL(nop_pos_[i]->init(stmt_allocator_, OB_ROW_MAX_COLUMNS_COUNT))) {
          STORAGE_LOG(WARN, "failed to init first row nop pos", K(ret));
        }
      }
    }
  } else if (SPARSE_ROW_STORE == context_.read_out_type_) {  // read sparse row
    void* ptr = NULL;
    for (int i = 0; OB_SUCC(ret) && i < RNPI_MAX; ++i) {  // init bit set
      if (NULL == (ptr = stmt_allocator_.alloc(sizeof(ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "fail to alloc nop pos", K(ret));
      } else {
        bit_set_[i] = new (ptr) ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>();
        bit_set_[i]->reset();
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid read out row type", K(ret), K(context_.read_out_type_));
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMinorMergeMacroRowIterator::next_row_with_open_macro_block()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMacroRowIterator::next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret), K(*curr_row_));
    }
  } else if (OB_ISNULL(curr_row_)) {
    if (OB_FAIL(ObMacroRowIterator::open_curr_macro_block_())) {  // open next macro block
      LOG_WARN("failed to open next macro block", K(ret), K(*curr_row_));
    } else if (OB_FAIL(ObMacroRowIterator::next())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret), K(*curr_row_));
      }
    }
  }
  return ret;
}

int ObMinorMergeMacroRowIterator::compact_last_row()
{
  int ret = OB_SUCCESS;
  bool compact_flag = false;
  const int64_t trans_version_col_idx = multi_version_row_info_->trans_version_index_;
  if (curr_row_->row_val_.cells_[trans_version_col_idx].get_int() !=
      row_queue_.get_last()->row_val_.cells_[trans_version_col_idx].get_int()) {
    // switch to a new transaction
    if (OB_FAIL(row_queue_.add_empty_row(obj_copy_allocator_))) {
      LOG_WARN("failed to add empty row into row_queue", K(ret));
    } else {
      compact_flag = true;
      LOG_DEBUG("add row into row_queue", K(ret), KPC(curr_row_));
    }
  } else if (row_queue_.count() > 1) {
    // curr row and first row of rowkey are not int the same trasaction
    // need compact curr rw to the first row of the row queue
    compact_flag = true;
  }
  if (OB_SUCC(ret) && compact_flag) {
    if (OB_FAIL(compact_row(*curr_row_, RNPI_LAST_ROW, *row_queue_.get_last()))) {
      LOG_WARN("failed to compact to last row", K(ret), KPC(curr_row_));
    } else {
      LOG_DEBUG("success to compact to last row", K(ret), KPC(curr_row_), KPC(row_queue_.get_last()));
    }
  }
  return ret;
}

int ObMinorMergeMacroRowIterator::make_first_row_compacted()
{
  int ret = OB_SUCCESS;
  if (row_queue_.is_empty()) {
    if (OB_NOT_NULL(nop_pos_[RNPI_FIRST_ROW])) {
      nop_pos_[RNPI_FIRST_ROW]->reset();
    } else if (OB_NOT_NULL(bit_set_[RNPI_FIRST_ROW])) {
      bit_set_[RNPI_FIRST_ROW]->reset();
    }
    if (OB_FAIL(row_queue_.add_empty_row(obj_copy_allocator_))) {
      LOG_WARN("failed to add empty row into row_queue", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(multi_version_row_info_->trans_version_index_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the multi version trans version index is invalid",
        K(ret),
        "trans_version_idx",
        multi_version_row_info_->trans_version_index_);
  }
  ObStoreRow* first_row = row_queue_.get_first();
  bool is_magic_row_flag = false;
  while (OB_SUCC(ret)) {
    if (OB_UNLIKELY(OB_ISNULL(curr_row_))) {
      LOG_DEBUG("curr row is null", K(ret), K(curr_row_));
      break;
    } else if (OB_FAIL(ObMagicRowManager::is_magic_row(curr_row_->row_type_flag_, is_magic_row_flag))) {
      LOG_WARN("failed to check is magic row", K(ret), KPC(curr_row_));
    } else if (!is_magic_row_flag) {  // not a magic row
      // first row not compacted
      if (!first_row->row_type_flag_.is_compacted_multi_version_row() &&
          OB_FAIL(compact_row(*curr_row_, RNPI_FIRST_ROW, *first_row))) {
        LOG_WARN("failed to compact first row", K(ret), KPC(curr_row_), K_(row_queue));
      } else if (OB_FAIL(compact_last_row())) {
        LOG_WARN("failed to compact last row", K(ret), KPC(curr_row_), K_(row_queue));
      } else {
        LOG_DEBUG("success to compact row", K(ret), KPC(curr_row_), KPC(first_row), KPC(row_queue_.get_last()));
      }
    }
    if (OB_SUCC(ret)) {
      if (curr_row_->row_type_flag_.is_first_multi_version_row()) {
        row_queue_.get_first()->row_type_flag_.set_first_multi_version_row(true);
      }
      if (curr_row_->row_type_flag_.is_last_multi_version_row()) {  // same trans_version & meet Last row
        LOG_DEBUG("meet last row", K(ret), KPC(curr_row_));
        row_queue_.get_last()->row_type_flag_.set_last_multi_version_row(true);
        break;
      }
      if (first_row->row_type_flag_.is_compacted_multi_version_row() && row_queue_.count() > 2) {
        // make first row compacted & meet committed row
        break;
      }
      if (OB_FAIL(next_row_with_open_macro_block())) {
        LOG_WARN("failed to get next row with open macro block", K(ret), KPC(curr_row_));
      }
    }
  }  // end of while
  if (OB_SUCC(ret)) {
    LOG_DEBUG("make first row compacted",
        K(ret),
        KPC(curr_row_),
        KPC(row_queue_.get_first()),
        KPC(row_queue_.get_last()),
        K(row_queue_.count()));
  }
  return ret;
}

int ObMinorMergeMacroRowIterator::compact_row(
    const ObStoreRow& former, const int64_t row_compact_info_index, ObStoreRow& result)
{
  int ret = OB_SUCCESS;
  bool final_result = false;
  STORAGE_LOG(DEBUG, "compact row", K(ret), K(former), K(result));
  if (row_compact_info_index < 0 || row_compact_info_index >= RNPI_MAX) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row compact info index", K(ret), K(row_compact_info_index));
  } else if (former.is_sparse_row_) {  // compat sparse row
    if (OB_ISNULL(bit_set_[row_compact_info_index])) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "bit set is null", K(ret), K(row_compact_info_index));
    } else if (OB_FAIL(storage::ObRowFuse::fuse_sparse_row(
                   former, result, *bit_set_[row_compact_info_index], final_result, &obj_copy_))) {
      STORAGE_LOG(WARN, "failed to fuse sparse row", K(ret), K(row_compact_info_index));
    }
  } else {  // compat flat row
    if (OB_ISNULL(nop_pos_[row_compact_info_index])) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "nop pos is null", K(ret), K(row_compact_info_index));
    } else if (OB_FAIL(storage::ObRowFuse::fuse_row(
                   former, result, *nop_pos_[row_compact_info_index], final_result, &obj_copy_))) {
      STORAGE_LOG(WARN, "failed to fuse row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (final_result || former.row_type_flag_.is_last_multi_version_row() ||
        former.row_type_flag_.is_compacted_multi_version_row()) {
      result.row_type_flag_.set_compacted_multi_version_row(true);
    }
  }
  LOG_DEBUG("compact row", K(ret), K(final_result), K(result), K(former));
  return ret;
}

/**
 * ---------------------------------------------------------ObPartitionMergeUtil--------------------------------------------------------------
 */
bool ObPartitionMergeUtil::do_merge_status = true;

ObPartitionMergeUtil::ObPartitionMergeUtil()
{}

ObPartitionMergeUtil::~ObPartitionMergeUtil()
{}

void ObPartitionMergeUtil::stop_merge()
{
  if (do_merge_status) {
    LOG_INFO("Daily merge has been paused!");
  }
  do_merge_status = false;
}

void ObPartitionMergeUtil::resume_merge()
{
  if (!do_merge_status) {
    LOG_INFO("Daily merge has been resumed!");
  }
  do_merge_status = true;
}

bool ObPartitionMergeUtil::could_merge_start()
{
  return do_merge_status;
}

int ObPartitionMergeUtil::merge_partition(memtable::ObIMemtableCtxFactory* memctx_factory,
    storage::ObSSTableMergeCtx& ctx, ObIStoreRowProcessor& processor, const int64_t idx, const bool iter_complement)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_CS_MERGER);
  ObMacroRowIterator* macro_row_iter = NULL;
  ObIPartitionMergeFuser::MERGE_ITER_ARRAY macro_row_iters;
  ObIPartitionMergeFuser::MERGE_ITER_ARRAY minimum_iters;
  ObSSTable* first_sstable = NULL;
  ObExtStoreRange merge_range;
  merge_range.get_range().set_whole_range();
  ObMergeParameter merge_param;
  ObIPartitionMergeFuser* partition_fuser = NULL;

  if (!ctx.is_valid() || OB_ISNULL(memctx_factory)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ctx));
  } else if (OB_FAIL(merge_param.init(ctx, idx, iter_complement))) {
    STORAGE_LOG(WARN, "Failed to assign the merge param", K(ret));
  } else if (OB_FAIL(init_partition_fuser(merge_param, allocator, partition_fuser))) {
    STORAGE_LOG(WARN, "Failed to init partition fuser", K(merge_param), K(ret));
  } else if (OB_ISNULL(partition_fuser)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null partition fuser", K(ret));
  } else if (OB_FAIL(ctx.get_merge_range(idx, merge_range, allocator))) {
    STORAGE_LOG(WARN, "Failed to get merge range from merge context", K(ret));
  } else {
    ObArray<ObMacroBlockInfoPair> lob_blocks;
    if (OB_ISNULL(ctx.tables_handle_.get_tables().at(0))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "Unexpected null first table in merge ctx", K(ret), K(ctx));
    } else if (ctx.is_full_merge_) {
      // full merge no need reuse lob macro block
    } else if (ctx.tables_handle_.get_tables().at(0)->is_sstable()) {
      first_sstable = static_cast<ObSSTable*>(ctx.tables_handle_.get_tables().at(0));
      if (partition_fuser->has_lob_column()) {
        if (OB_FAIL(first_sstable->find_lob_macros(merge_range, lob_blocks))) {
          STORAGE_LOG(WARN, "Failed to add lob merge info", KP(first_sstable), K(merge_range), K(ret));
        } else {
          STORAGE_LOG(
              DEBUG, "[LOB] Success to add lob base sstable for merge builder", K(*first_sstable), K(ctx), K(ret));
        }
      }
    } else if (ctx.param_.is_major_merge()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("first table for major merge must be sstable", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(processor.open(ctx,
                            idx,
                            iter_complement,
                            partition_fuser->get_schema_column_ids(),
                            (lob_blocks.count() > 0) ? &lob_blocks : NULL))) {
      LOG_WARN("fail to open processor.", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // prepare iterators
    // int64_t pre_snapshot_version = 0;
    ObIArray<ObITable*>& tables = ctx.tables_handle_.get_tables();
    const blocksstable::ObDataStoreDesc* data_store_desc = processor.get_data_store_desc();

    if (OB_ISNULL(data_store_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null datastore desc and processor", KP(data_store_desc), K(processor), K(ret));
    }

    int64_t start_idx = 0;
    if (OB_SUCC(ret) && iter_complement && ctx.param_.is_mini_merge()) {
      start_idx = tables.count() - 1;
      ObITable* table = tables.at(start_idx);
      if (OB_ISNULL(table) || !table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last table of mini merge handles should be memtable", K(start_idx), KPC(table));
      }
    }

    for (int64_t i = start_idx; OB_SUCC(ret) && i < tables.count(); ++i) {
      ObITable* table = tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The store is NULL, ", K(i), K(ret));
      } else if (table->is_sstable() && static_cast<ObSSTable*>(table)->get_macro_block_count() <= 0) {
        // do nothing. don't need to construct iter for empty sstable
        FLOG_INFO("table is empty, need not create iter",
            K(i),
            KPC(static_cast<ObSSTable*>(table)),
            K(static_cast<ObSSTable*>(table)->get_meta()));
      } else if (OB_FAIL(init_macro_row_iter(ctx,
                     partition_fuser->get_schema_column_ids(),
                     memctx_factory,
                     allocator,
                     table,
                     data_store_desc,
                     merge_range,
                     merge_param,
                     0 == i,
                     tables.count() - 1 == i,
                     macro_row_iter))) {
        LOG_WARN("failed to init iter", K(ret));
      } else if (OB_FAIL(macro_row_iters.push_back(macro_row_iter))) {
        macro_row_iter->~ObMacroRowIterator();
        allocator.free(macro_row_iter);
        LOG_WARN("Fail to add macro_row_iter, ", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_row_iters.count(); ++i) {
      if (OB_FAIL(macro_row_iters.at(i)->next())) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("Fail to get next, ", K(ret), K(i));
        }
      }
    }

    // compute the count of macro block need rewrite
    int64_t need_rewrite_block_cnt = 0;
    if (OB_SUCC(ret) && ctx.param_.is_major_merge() && !ctx.is_full_merge_ && macro_row_iters.count() > 0) {
      if (OB_FAIL(get_macro_block_count_to_rewrite(
              ctx, macro_row_iters, first_sstable, merge_range, need_rewrite_block_cnt))) {
        LOG_WARN("Failed to compute the count of macro block to rewrite.", K(ret));
      }
    }

    // do merge
    if (OB_FAIL(ret)) {
    } else if (macro_row_iters.empty()) {
      ret = OB_ITER_END;
    } else {
      int64_t rewrite_block_cnt = 0;
      while (OB_SUCC(ret)) {
        share::dag_yield();
        // find minimum macro_row_iter
        if (OB_UNLIKELY(ctx.param_.is_major_merge() && !ObPartitionMergeUtil::could_merge_start())) {
          ret = OB_CANCELED;
          LOG_INFO("Merge has been paused,", K(ret));
        } else if (OB_FAIL(partition_fuser->find_minimum_iters(macro_row_iters, minimum_iters))) {
          LOG_WARN("Failed to find minimum iters", K(ret));
        } else {
          if (0 == minimum_iters.count()) {
            ret = OB_ITER_END;
          } else if (1 == minimum_iters.count() && NULL == minimum_iters.at(0)->get_curr_row()) {
            ObMacroRowIterator* iter = minimum_iters.at(0);
            const storage::ObMacroBlockDesc& block_desc = iter->get_curr_macro_block();
            if (!iter->macro_block_opened() &&
                ((rewrite_block_cnt < need_rewrite_block_cnt && ctx.need_rewrite_macro_block(block_desc)) ||
                    (iter->need_rewrite_current_macro_block()))) {
              if (!ctx.param_.is_major_merge()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("only major merge can call rewrite_macro_block", K(ret), K(ctx), KPC(iter));
              } else if (OB_FAIL(rewrite_macro_block(minimum_iters, ctx.merge_level_, partition_fuser, processor))) {
                LOG_WARN("rewrite_macro_block failed", K(ret), K(ctx));
              } else {
                ++rewrite_block_cnt;
              }
            } else {
              if (!iter->macro_block_opened()) {
                if (OB_FAIL(processor.process(iter->get_curr_macro_block().macro_block_ctx_))) {
                  LOG_WARN("Fail to append macro_block_id, ", K(ret));
                }
              } else if (MICRO_BLOCK_MERGE_LEVEL == iter->get_merge_level() && !iter->micro_block_opened()) {
                const ObMicroBlock& micro_block = iter->get_curr_micro_block();
                if (OB_FAIL(processor.process(micro_block))) {
                  LOG_WARN("process micro_block failed", K(micro_block), K(ret));
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("cur row is null, but block not opened", K(ret), K(ctx));
              }

              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(iter->next())) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("next failed", K(ret));
                }
              }
            }
          } else {
            if (OB_FAIL(partition_fuser->fuse_row(minimum_iters))) {
              LOG_WARN("Failed to fuse row", K(*partition_fuser), K(ret));
            } else if (OB_FAIL(processor.process(
                           *partition_fuser->get_result_row(), partition_fuser->get_compact_type()))) {
              LOG_WARN("Failed to process row",
                  K(ret),
                  K(partition_fuser->get_compact_type()),
                  K(*partition_fuser->get_result_row()));
              ObTableDumper::print_error_info(ret, ctx, macro_row_iters);
            } else if (OB_FAIL(partition_fuser->calc_column_checksum(false))) {
              LOG_WARN("Failed to calculate column checksum", K(ret));
            }

            for (int64_t i = 0; OB_SUCC(ret) && i < minimum_iters.count(); ++i) {
              if (OB_FAIL(minimum_iters.at(i)->next())) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("Fail to next macro_row_iter, ", K(i), K(ret), KPC(minimum_iters.at(i)));
                }
              }
            }
          }
        }
      }  // end of while
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      processor.set_purged_count(partition_fuser->get_purged_count());
      if (OB_FAIL(processor.close())) {
        LOG_WARN("Fail to close processor", K(ret), K(ctx));
      }
    }
  }

  // check the validness of the txn table for possible concurrent rebuild or server stop
  if (OB_SUCC(ret) && 0 < macro_row_iters.count()) {
    ObMacroRowIterator* iter = macro_row_iters.at(0);
    if (OB_NOT_NULL(iter)) {
      if (!iter->is_trans_state_table_valid()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("Fail to complete the merge because of broken txn table", K(ret), K(*iter));
      }
    }
  }

  for (int64_t i = 0; i < macro_row_iters.count(); ++i) {
    ObMacroRowIterator* iter = macro_row_iters.at(i);
    FLOG_INFO("iter row count",
        K(i),
        "row_count",
        iter->get_iter_row_count(),
        "magic_row_count",
        iter->get_magic_row_count(),
        "purged_count",
        iter->get_purged_count(),
        "reuse_micro_block_count",
        iter->get_reuse_micro_block_count(),
        "pkey",
        iter->get_table()->get_key(),
        "table",
        *iter->get_table(),
        "need_rewrite_dirty_macro_block",
        iter->need_rewrite_dirty_macro_block());
  }

  for (int64_t i = 0; i < macro_row_iters.count(); ++i) {
    ObMacroRowIterator* iter = macro_row_iters.at(i);
    if (OB_NOT_NULL(iter)) {
      if (OB_SUCC(ret) && !iter->is_end()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("iter not end", K(ret), K(*iter));
      }
      iter->~ObMacroRowIterator();
    }
  }
  if (OB_NOT_NULL(partition_fuser)) {
    partition_fuser->~ObIPartitionMergeFuser();
    partition_fuser = NULL;
  }

  return ret;
}

int ObTableDumper::generate_dump_table_name(const char* dir_name, const ObITable* table, char* file_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else {
    int64_t pret = snprintf(file_name,
        OB_MAX_FILE_NAME_LENGTH,
        "%s/%s.%s.%ld.%s.%ld.%s.%d.%s.%ld.%s.%ld",
        dir_name,
        table->is_memtable() ? "dump_memtable" : "dump_sstable",
        "table_id",
        table->get_key().table_id_,
        "part_id",
        table->get_partition_key().get_partition_id(),
        "table_type",
        table->get_key().table_type_,
        "start_log_ts",
        table->get_start_log_ts(),
        "end_log_ts",
        table->get_end_log_ts());
    if (pret < 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("name too long", K(ret), K(pret), K(file_name));
    }
  }
  return ret;
}

lib::ObMutex ObTableDumper::lock;

int ObTableDumper::check_disk_free_space(const char* dir_name)
{
  int ret = OB_SUCCESS;
  int64_t total_space = 0;
  int64_t free_space = 0;
  if (OB_FAIL(common::FileDirectoryUtils::get_disk_space(dir_name, total_space, free_space))) {
    STORAGE_LOG(WARN, "Failed to get disk space ", K(ret), K(dir_name));
  } else if (free_space < ObTableDumper::DUMP_TABLE_DISK_FREE_PERCENTAGE * total_space) {
    ret = OB_CS_OUTOF_DISK_SPACE;
  }
  return ret;
}

int ObTableDumper::judge_disk_free_space(const char* dir_name, ObITable* table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else {
    int64_t total_space = 0;
    int64_t free_space = 0;
    if (OB_FAIL(common::FileDirectoryUtils::get_disk_space(dir_name, total_space, free_space))) {
      STORAGE_LOG(WARN, "Failed to get disk space ", K(ret), K(dir_name));
    } else if (table->is_sstable()) {
      if (free_space -
              static_cast<ObSSTable*>(table)->get_meta().get_total_macro_block_count() * OB_DEFAULT_MACRO_BLOCK_SIZE <
          ObTableDumper::DUMP_TABLE_DISK_FREE_PERCENTAGE * total_space) {
        ret = OB_CS_OUTOF_DISK_SPACE;
        LOG_WARN("disk space is not enough", K(ret), K(free_space), K(total_space), KPC(table));
      }
    } else if (free_space - static_cast<ObMemtable*>(table)->get_occupied_size() * MEMTABLE_DUMP_SIZE_PERCENTAGE <
               ObTableDumper::DUMP_TABLE_DISK_FREE_PERCENTAGE * total_space) {
      ret = OB_CS_OUTOF_DISK_SPACE;
      LOG_WARN("disk space is not enough", K(ret), K(free_space), K(total_space), KPC(table));
    }
  }
  return ret;
}

bool ObTableDumper::need_dump_table(int err_no)
{
  bool bret = false;
  if (OB_CHECKSUM_ERROR == err_no || OB_ERR_UNEXPECTED == err_no || OB_ERR_SYS == err_no ||
      OB_ROWKEY_ORDER_ERROR == err_no || OB_ERR_PRIMARY_KEY_DUPLICATE == err_no) {
    bret = true;
  }
  return bret;
}

void ObTableDumper::print_error_info(
    int err_no, storage::ObSSTableMergeCtx& ctx, ObIPartitionMergeFuser::MERGE_ITER_ARRAY& macro_row_iters)
{
  int ret = OB_SUCCESS;
  if (need_dump_table(err_no)) {

    const char* dump_table_dir = "/tmp";
    for (int64_t midx = 0; midx < macro_row_iters.count(); ++midx) {
      ObMacroRowIterator* cur_iter = macro_row_iters.at(midx);
      if (NULL != cur_iter->get_curr_row()) {
        LOG_WARN("macro row iter content: ",
            K(midx),
            K(cur_iter->get_table()->get_key()),
            K(cur_iter->get_curr_macro_block()),
            K(*cur_iter->get_curr_row()));
      } else {
        LOG_WARN("macro row iter content: ",
            K(midx),
            K(cur_iter->get_table()->get_key()),
            K(cur_iter->get_curr_macro_block()));
      }
    }
    // dump all sstables in this merge
    ObIArray<ObITable*>& tables = ctx.tables_handle_.get_tables();
    char file_name[OB_MAX_FILE_NAME_LENGTH];
    lib::ObMutexGuard guard(ObTableDumper::lock);
    for (int idx = 0; OB_SUCC(ret) && idx < tables.count(); ++idx) {
      ObITable* table = tables.at(idx);
      if (OB_ISNULL(table)) {
        LOG_WARN("The store is NULL", K(idx), K(tables));
      } else if (OB_FAIL(compaction::ObTableDumper::judge_disk_free_space(dump_table_dir, table))) {
        if (OB_CS_OUTOF_DISK_SPACE != ret) {
          LOG_WARN("failed to judge disk space", K(ret), K(dump_table_dir));
        }
      } else if (OB_FAIL(generate_dump_table_name(dump_table_dir, table, file_name))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("name too long", K(ret), K(file_name));
      } else if (table->is_sstable()) {
        if (OB_FAIL(static_cast<ObSSTable*>(table)->dump2text(dump_table_dir, *ctx.table_schema_, file_name))) {
          if (OB_CS_OUTOF_DISK_SPACE != ret) {
            LOG_WARN("failed to dump sstable", K(ret), K(file_name));
          }
        }
      } else if (table->is_memtable()) {
        LOG_INFO("jump dump memtable", K(ret), K(file_name));
        /*
        if (OB_FAIL(static_cast<ObMemtable *>(table)->dump2text(file_name))) {
          LOG_WARN("failed to dump memtable", K(ret), K(file_name));
        }
        */
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("success to dump table", K(ret), K(file_name));
      }
    }
  }  // end for
}

int ObPartitionMergeUtil::init_macro_row_iter(storage::ObSSTableMergeCtx& ctx,
    const common::ObIArray<share::schema::ObColDesc>& column_ids, memtable::ObIMemtableCtxFactory* memctx_factory,
    ObIAllocator& allocator, ObITable* table, const blocksstable::ObDataStoreDesc* data_store_desc,
    ObExtStoreRange& merge_range, ObMergeParameter& merge_param, const bool is_base_iter, const bool is_last_iter,
    ObMacroRowIterator*& macro_row_iter)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  macro_row_iter = NULL;

  if (OB_ISNULL(memctx_factory)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(memctx_factory));
  } else if (is_major_merge(merge_param.merge_type_)) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMacroRowIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory, ", K(ret));
    } else {
      macro_row_iter = new (buf) ObMacroRowIterator();
      buf = NULL;
    }
  } else if (is_multi_version_minor_merge(merge_param.merge_type_)) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMinorMergeMacroRowIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory, ", K(ret));
    } else {
      macro_row_iter = new (buf) ObMinorMergeMacroRowIterator();
      buf = NULL;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Unsupported merge type", K(ret), K(merge_param));
  }
  if (OB_SUCC(ret)) {
    ObVersionRange whole_version_range;
    whole_version_range.base_version_ = 0;
    whole_version_range.multi_version_start_ = ctx.sstable_version_range_.multi_version_start_;
    whole_version_range.snapshot_version_ = INT64_MAX - 2;
    ObMacroRowIterator::Param param;
    param.memctx_factory_ = memctx_factory;
    param.schema_ = ctx.table_schema_;
    param.column_ids_ = &column_ids;
    param.table_ = table;
    param.is_base_iter_ = is_base_iter;
    param.is_last_iter_ = is_last_iter;
    param.is_full_merge_ = ctx.is_full_merge_;
    param.merge_level_ = ctx.merge_level_;
    param.merge_type_ = ctx.param_.merge_type_;
    param.multi_version_row_info_ =
        is_multi_version_minor_merge(param.merge_type_) ? &ctx.multi_version_row_info_ : nullptr;
    param.row_store_type_ = data_store_desc->row_store_type_;
    param.range_ = &merge_range;
    param.version_range_ =
        is_multi_version_minor_merge(param.merge_type_) ? whole_version_range : ctx.sstable_version_range_;
    param.log_ts_range_ = ctx.log_ts_range_;
    param.is_iter_overflow_to_complement_ = merge_param.is_iter_complement_;
    if (merge_param.is_major_merge() && table->is_multi_version_minor_sstable()) {
      ObSSTable* base_sstable = nullptr;
      if (OB_FAIL(ctx.base_table_handle_.get_sstable(base_sstable))) {
        LOG_WARN("failed to get base sstable", K(ret));
      } else if (OB_ISNULL(base_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("base sstable should not be null", K(ret));
      } else {
        // the minor sstable may cross multi major freeze points
        // the read base version of the major merge should use the snapshot version of base major sstable
        param.version_range_.base_version_ = base_sstable->get_snapshot_version();
      }
    } else if (ObMergeType::MINI_MINOR_MERGE == merge_param.merge_type_ ||
               ObMergeType::MINOR_MERGE == merge_param.merge_type_) {
      param.version_range_.base_version_ = 0;
    }
    if (OB_SUCC(ret) && merge_param.is_multi_version_minor_merge()) {
      param.merge_log_ts_ = ctx.merge_log_ts_;
    }
    FLOG_INFO("init iter",
        KP(macro_row_iter),
        "table_key",
        table->get_key(),
        KP(table),
        K(merge_range),
        "version_range",
        param.version_range_,
        "merge_level",
        param.merge_level_,
        "row_store_type",
        param.row_store_type_,
        "merge_type",
        param.merge_type_,
        "is_base_iter",
        param.is_base_iter_,
        "is_last_iter",
        param.is_last_iter_,
        "is_full_merge",
        param.is_full_merge_,
        "merge_log_ts",
        param.merge_log_ts_,
        "iter_overflow",
        param.is_iter_overflow_to_complement_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_row_iter->init(param, ctx.param_.pg_key_))) {
      LOG_WARN("Fail to init macro_row_iter", K(ret));
    } else {
      LOG_DEBUG("init_macro_row_iter", K(param));
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != macro_row_iter) {
      macro_row_iter->~ObMacroRowIterator();
      allocator.free(macro_row_iter);
    }

    if (NULL != buf) {
      allocator.free(buf);
    }
  }

  return ret;
}

int ObPartitionMergeUtil::init_partition_fuser(
    const ObMergeParameter& merge_param, ObArenaAllocator& allocator, ObIPartitionMergeFuser*& partition_fuser)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  partition_fuser = NULL;

  if (OB_UNLIKELY(!merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init partition fuser", K(merge_param), K(ret));
  } else if (is_multi_version_minor_merge(merge_param.merge_type_)) {
    if (GCONF._enable_sparse_row) {  // use sparse row
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSparseMinorPartitionMergeFuser)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for ObSparseMinorPartitionMergeFuser", K(ret));
      } else {
        partition_fuser = new (buf) ObSparseMinorPartitionMergeFuser();
      }
    } else {  // use flat row
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObFlatMinorPartitionMergeFuser)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for ObFlatMinorPartitionMergeFuser", K(ret));
      } else {
        partition_fuser = new (buf) ObFlatMinorPartitionMergeFuser();
      }
    }
  } else if (!is_major_merge(merge_param.merge_type_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid merge type", K(ret), K(merge_param));
  } else if (merge_param.checksum_method_ == blocksstable::CCM_TYPE_AND_VALUE) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObIncrementMajorPartitionMergeFuser)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory for ObIncrementMajorPartitionMergeFuser", K(ret));
    } else {
      partition_fuser = new (buf) ObIncrementMajorPartitionMergeFuser();
    }
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMajorPartitionMergeFuser)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for ObMajorPartitionMergeFuser", K(ret));
  } else {
    partition_fuser = new (buf) ObMajorPartitionMergeFuser();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(partition_fuser)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null partition fuser", K(ret));
  } else if (OB_FAIL(partition_fuser->init(merge_param))) {
    STORAGE_LOG(WARN, "Failed to init partition fuser", K(ret));
  } else {
    FLOG_INFO("succeed to init merge fuser", "fuser_name", partition_fuser->get_fuser_name());
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(partition_fuser)) {
      partition_fuser->~ObIPartitionMergeFuser();
      partition_fuser = NULL;
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = NULL;
    }
  }

  return ret;
}

int ObPartitionMergeUtil::get_macro_block_count_to_rewrite_by_round(const storage::ObSSTableMergeCtx& ctx,
    const ObIPartitionMergeFuser::MERGE_ITER_ARRAY& macro_row_iters, storage::ObSSTable* first_sstable,
    ObExtStoreRange& merge_range, int64_t& need_rewrite_block_cnt)
{
  int ret = OB_SUCCESS;
  need_rewrite_block_cnt = 0;
  if (!ctx.use_new_progressive_ || macro_row_iters.count() <= 0 || nullptr == first_sstable) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ctx), K(macro_row_iters.count()), KP(first_sstable));
  } else {
    const int64_t progressive_merge_num = ctx.progressive_merge_num_;
    if (ctx.progressive_merge_step_ < progressive_merge_num) {
      ObMacroBlockIterator macro_block_iter;
      ObMacroBlockDesc macro_block_desc;
      merge_range = macro_row_iters.at(0)->get_merge_range();
      if (OB_FAIL(first_sstable->scan_macro_block(merge_range, macro_block_iter))) {
        LOG_WARN("fail to scan macro block", K(ret));
      }

      while (OB_SUCC(ret)) {
        if (OB_FAIL(macro_block_iter.get_next_macro_block(macro_block_desc))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next macro block", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (macro_block_desc.progressive_merge_round_ < ctx.progressive_merge_round_) {
          ++need_rewrite_block_cnt;
        }
      }
      if (OB_SUCC(ret)) {
        need_rewrite_block_cnt =
            std::max(need_rewrite_block_cnt / (progressive_merge_num - ctx.progressive_merge_step_), 1L);
        LOG_INFO("There are some macro block need rewrite, ",
            "pkey",
            ctx.param_.pkey_,
            "index_id",
            ctx.param_.index_id_,
            K(need_rewrite_block_cnt),
            K(ctx.param_.merge_version_),
            K(ctx.progressive_merge_step_),
            K(ctx.progressive_merge_num_));
      }
    }
  }
  return ret;
}

int ObPartitionMergeUtil::get_macro_block_count_to_rewrite_by_version(const storage::ObSSTableMergeCtx& ctx,
    const ObIPartitionMergeFuser::MERGE_ITER_ARRAY& macro_row_iters, storage::ObSSTable* first_sstable,
    ObExtStoreRange& merge_range, int64_t& need_rewrite_block_cnt)
{
  int ret = OB_SUCCESS;

  if (!ctx.param_.is_major_merge() || OB_ISNULL(first_sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid merge type for get_macro_block_count_to_rewrite_by_version", K(ret));
  }

  if (OB_SUCC(ret) && ctx.param_.merge_version_ >= ctx.progressive_merge_start_version_ &&
      ctx.param_.merge_version_ < ctx.progressive_merge_start_version_ + ctx.progressive_merge_num_) {
    ObMacroBlockIterator macro_iter;
    ObMacroBlockDesc block_desc;
    merge_range = macro_row_iters.at(0)->get_merge_range();
    if (OB_FAIL(first_sstable->scan_macro_block(merge_range, macro_iter))) {
      LOG_WARN("Fail to scan macro block, ", K(ret));
    }

    if (OB_SUCC(ret)) {
      while (OB_SUCC(macro_iter.get_next_macro_block(block_desc))) {
        if (block_desc.data_version_ < static_cast<uint64_t>(ctx.progressive_merge_start_version_)) {
          ++need_rewrite_block_cnt;
        }
      }

      if (OB_ITER_END != ret) {
        LOG_WARN("Fail to scan macro block iter, ", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(ret)) {
      need_rewrite_block_cnt =
          need_rewrite_block_cnt /
          (ctx.progressive_merge_num_ - (ctx.param_.merge_version_ - ctx.progressive_merge_start_version_));
      if (need_rewrite_block_cnt <= 0) {
        need_rewrite_block_cnt = 1;
      }
      LOG_INFO("There are some macro block need rewrite, ",
          "pkey",
          ctx.param_.pkey_,
          "index_id",
          ctx.param_.index_id_,
          K(need_rewrite_block_cnt),
          K(ctx.param_.merge_version_),
          K(ctx.progressive_merge_start_version_),
          K(ctx.progressive_merge_num_));
    }
  }

  return ret;
}

int ObPartitionMergeUtil::get_macro_block_count_to_rewrite(const storage::ObSSTableMergeCtx& ctx,
    const ObIPartitionMergeFuser::MERGE_ITER_ARRAY& macro_row_iters, storage::ObSSTable* first_sstable,
    ObExtStoreRange& merge_range, int64_t& need_rewrite_block_cnt)
{
  int ret = OB_SUCCESS;
  if (ctx.use_new_progressive_) {
    if (OB_FAIL(get_macro_block_count_to_rewrite_by_round(
            ctx, macro_row_iters, first_sstable, merge_range, need_rewrite_block_cnt))) {
      LOG_WARN("fail to get macro block count to rewrite by round", K(ret));
    }
  } else {
    if (OB_FAIL(get_macro_block_count_to_rewrite_by_version(
            ctx, macro_row_iters, first_sstable, merge_range, need_rewrite_block_cnt))) {
      LOG_WARN("fail to get macro block count to rewrite by version", K(ret));
    }
  }
  return ret;
}

int ObPartitionMergeUtil::rewrite_macro_block(ObIPartitionMergeFuser::MERGE_ITER_ARRAY& minimum_iters,
    const storage::ObMergeLevel& merge_level, ObIPartitionMergeFuser* partition_fuser, ObIStoreRowProcessor& processor)
{
  int ret = OB_SUCCESS;
  ObMacroRowIterator* iter = static_cast<ObMacroRowIterator*>(minimum_iters.at(0));
  if (OB_ISNULL(partition_fuser)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected NULL partition fuser", K(partition_fuser), K(ret));
  } else if (!partition_fuser->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected partition fuser", K(*partition_fuser), K(ret));
  } else if (OB_FAIL(iter->open_curr_macro_block())) {
    LOG_WARN("Fail to open the curr macro block", K(ret));
  } else {
    while (OB_SUCC(ret) && iter->macro_block_opened()) {
      // open the micro block if needed
      if (MICRO_BLOCK_MERGE_LEVEL == merge_level && !iter->micro_block_opened()) {
        if (OB_FAIL(iter->open_curr_micro_block())) {
          LOG_WARN("open_curr_micro_block failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(partition_fuser->fuse_row(minimum_iters))) {
        LOG_WARN("Failed to fuse row", K(ret));
      } else if (OB_FAIL(processor.process(*partition_fuser->get_result_row(), partition_fuser->get_compact_type()))) {
        LOG_WARN("Failed to process row", K(ret));
      } else if (OB_FAIL(partition_fuser->calc_column_checksum(true))) {
        LOG_WARN("Failed to calculate column checksum", K(ret));
      } else if (OB_FAIL(iter->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Fail to iter next row", K(ret));
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

} /* namespace compaction */
} /* namespace oceanbase */
