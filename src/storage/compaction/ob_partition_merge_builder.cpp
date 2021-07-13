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
#include "storage/ob_sstable.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "share/config/ob_server_config.h"
#include "sql/ob_sql_utils.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_partition_merge_task.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace storage;

namespace compaction {

/**
 * ---------------------------------------------------------ObMacroBlockBuilder--------------------------------------------------------------
 */
ObMacroBlockBuilder::ObMacroBlockBuilder()
    : merge_type_(INVALID_MERGE_TYPE),
      writer_(NULL),
      desc_(),
      sstable_merge_info_(),
      task_idx_(0),
      mark_deletion_maker_(nullptr),
      merge_context_(nullptr),
      allocator_(ObModIds::OB_CS_COMMON),
      need_build_bloom_filter_(false),
      bf_macro_writer_(),
      cols_id_map_(nullptr),
      is_opened_(false)
{}

ObMacroBlockBuilder::~ObMacroBlockBuilder()
{
  reset();
}

int ObMacroBlockBuilder::open(storage::ObSSTableMergeCtx& ctx, const int64_t idx, const bool iter_complement,
    const ObIArray<ObColDesc>& column_ids, const ObIArray<ObMacroBlockInfoPair>* lob_blocks)
{
  UNUSED(column_ids);
  int ret = OB_SUCCESS;
  void* buf = NULL;
  const ObMultiVersionRowInfo* multi_version_row_info =
      ctx.param_.is_multi_version_minor_merge() ? &ctx.multi_version_row_info_ : NULL;

  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The builder has been opened, ", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(ctx));
  } else if (OB_FAIL(desc_.init(*ctx.table_schema_,
                 ctx.param_.merge_version_,
                 multi_version_row_info,
                 ctx.param_.pkey_.get_partition_id(),
                 ctx.param_.merge_type_,
                 ctx.need_full_checksum(),
                 ctx.store_column_checksum_in_micro_,
                 ctx.param_.pg_key_,
                 ctx.pg_guard_.get_partition_group()->get_storage_file_handle(),
                 ctx.sstable_version_range_.snapshot_version_))) {
    STORAGE_LOG(WARN, "data store desc fail to init.", K(ret));
  } else if (NULL == (buf = allocator_.alloc(sizeof(ObMacroBlockWriter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    writer_ = new (buf) ObMacroBlockWriter();
    ObMacroDataSeq macro_start_seq(0);
    task_idx_ = idx;
    desc_.merge_info_ = &sstable_merge_info_;
    mark_deletion_maker_ = NULL;
    if (desc_.need_prebuild_bloomfilter_) {
      ObIPartitionGroup* pg = ctx.pg_guard_.get_partition_group();
      if (OB_ISNULL(pg)) {
        STORAGE_LOG(WARN, "Unexpected null partition group", K(ctx));
        desc_.need_prebuild_bloomfilter_ = false;
      } else if (is_follower_state(pg->get_partition_state())) {
        desc_.need_prebuild_bloomfilter_ = false;
      } else if (ctx.bf_rowkey_prefix_ <= 0 || ctx.bf_rowkey_prefix_ > desc_.schema_rowkey_col_cnt_) {
        desc_.need_prebuild_bloomfilter_ = false;
        ctx.bf_rowkey_prefix_ = 0;
      } else {
        desc_.bloomfilter_rowkey_prefix_ = ctx.bf_rowkey_prefix_;
      }
    }
    if (!ctx.param_.is_major_merge()) {
      if (desc_.need_prebuild_bloomfilter_ && ctx.parallel_merge_ctx_.get_concurrent_cnt() == 1 &&
          OB_FAIL(init_bloomfilter_if_need(ctx))) {
        STORAGE_LOG(WARN, "Failed to init bloom filter writer", K(ret));
        ret = OB_SUCCESS;
      }
      if (ctx.table_schema_->is_dropped_schema()) {
      } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBlockMarkDeletionMaker)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(
            WARN, "alloc memory for block mark deletion maker failed", K(ret), K(sizeof(ObBlockMarkDeletionMaker)));
      } else {
        mark_deletion_maker_ = new (buf) ObBlockMarkDeletionMaker();
        if (OB_FAIL(mark_deletion_maker_->init(*ctx.table_schema_,
                ctx.param_.pkey_,
                ctx.param_.index_id_,
                ctx.sstable_version_range_.snapshot_version_,
                ctx.log_ts_range_.end_log_ts_))) {
          STORAGE_LOG(WARN, "fail to init mark deletion maker, skip maker", K(ret));
          mark_deletion_maker_ = NULL;
          ret = OB_SUCCESS;
        }
        desc_.mark_deletion_maker_ = mark_deletion_maker_;
      }
    } else {
      desc_.mark_deletion_maker_ = NULL;
    }
    if (OB_SUCC(ret)) {
      ObITable* table = NULL;
      ObSSTable* first_sstable = NULL;
      if (OB_ISNULL(table = ctx.tables_handle_.get_tables().at(0))) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "sstable is null", K(ret));
      } else if (!table->is_sstable()) {
      } else {
        first_sstable = reinterpret_cast<ObSSTable*>(table);
        if (first_sstable->get_logical_data_version() >= ctx.logical_data_version_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(
              ERROR, "Unexpected large logical data version", K(ret), K(*first_sstable), K(ctx.logical_data_version_));
        } else if (ctx.param_.is_major_merge()) {
          if (desc_.data_version_ < ctx.logical_data_version_) {
            // incremental major merge need consider logical data_version
            desc_.data_version_ = ctx.logical_data_version_;
            STORAGE_LOG(INFO,
                "Logical data version of sstable larger than frozen version",
                K(ret),
                K(ctx.logical_data_version_),
                K(desc_.data_version_));
          }
        } else if (desc_.snapshot_version_ < ctx.logical_data_version_) {
          // incremental major merge need consider logical data_version
          desc_.snapshot_version_ = ctx.logical_data_version_;
          STORAGE_LOG(INFO,
              "Logical data version of sstable larger than snapshot version",
              K(ret),
              K(ctx.logical_data_version_),
              K(desc_.snapshot_version_));
        }
        if (OB_SUCC(ret)) {
          if (desc_.need_prebuild_bloomfilter_) {
            if (OB_SUCCESS != (first_sstable->get_macro_max_row_count(desc_.bloomfilter_size_))) {
              desc_.bloomfilter_size_ = 0;
            }
          }
          if (first_sstable->get_rowkey_helper().is_valid()) {
            bool is_oracle_mode =
                is_oracle_compatible(static_cast<ObCompatibilityMode>(THIS_WORKER.get_compatibility_mode()));
            if (first_sstable->get_rowkey_helper().is_oracle_mode() != is_oracle_mode) {
              ret = OB_ERR_SYS;
              STORAGE_LOG(ERROR,
                  "sstable oracle compati mode is mismatch",
                  K(ret),
                  K(is_oracle_mode),
                  K(first_sstable->get_rowkey_helper()));
            } else {
              desc_.rowkey_helper_ = &first_sstable->get_rowkey_helper();
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(macro_start_seq.set_parallel_degree(task_idx_))) {
      STORAGE_LOG(WARN, "Failed to set parallel degree to macro start seq", K_(task_idx), K(ret));
    } else if (OB_FAIL(writer_->open(desc_, macro_start_seq, iter_complement ? nullptr : lob_blocks))) {
      STORAGE_LOG(WARN, "macro block writer fail to open.", K(ret));
    } else {
      sstable_merge_info_.total_child_task_ = ctx.get_concurrent_cnt();
      sstable_merge_info_.column_cnt_ = desc_.row_column_count_;
      sstable_merge_info_.table_id_ = ctx.param_.index_id_;
      sstable_merge_info_.partition_id_ = ctx.param_.pkey_.get_partition_id();
      sstable_merge_info_.merge_start_time_ = ObTimeUtility::current_time();
      sstable_merge_info_.table_type_ = ctx.table_schema_->get_table_type();
      sstable_merge_info_.major_table_id_ = ctx.table_schema_->get_data_table_id();
      sstable_merge_info_.merge_level_ = ctx.merge_level_;
      sstable_merge_info_.merge_type_ = ctx.param_.merge_type_;
      sstable_merge_info_.version_ = ctx.param_.merge_version_;
      sstable_merge_info_.merge_status_ = MERGE_RUNNING;
      sstable_merge_info_.table_count_ = ctx.tables_handle_.get_count();
      merge_type_ = ctx.param_.merge_type_;
      merge_context_ = &ctx.get_merge_context(iter_complement);
      is_opened_ = true;
    }

    if (OB_SUCC(ret) && SPARSE_ROW_STORE == desc_.row_store_type_) {
      void* buf = static_cast<share::schema::ColumnMap*>(allocator_.alloc(sizeof(share::schema::ColumnMap)));
      if (OB_ISNULL(buf)) {  // alloc failed
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate col id map", K(ret));
      } else {
        share::schema::ColumnMap* cols_id_map_ptr = new (buf) share::schema::ColumnMap(allocator_);
        ObArray<uint64_t> col_id_array;
        for (int i = 0; OB_SUCC(ret) && i < desc_.row_column_count_; ++i) {
          if (OB_FAIL(col_id_array.push_back(desc_.column_ids_[i]))) {
            STORAGE_LOG(WARN, "Fail to push", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(cols_id_map_ptr->init(col_id_array))) {
            STORAGE_LOG(WARN, "Fail to init col id map", K(ret));
          } else {
            cols_id_map_ = cols_id_map_ptr;
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMacroBlockBuilder::init_bloomfilter_if_need(storage::ObSSTableMergeCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init bloomfilter write", K(ctx), K(ret));
  } else if (!desc_.need_prebuild_bloomfilter_ || ctx.param_.is_major_merge()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Major merge would not build bloomfilter for sstable, or prebuild_bloomfilter is off", K(ret));
  } else if (ctx.table_schema_->get_tenant_id() < OB_MAX_RESERVED_TENANT_ID) {
    // only check user table
  } else if (OB_FAIL(bf_macro_writer_.init(desc_))) {
    STORAGE_LOG(WARN, "Failed to init bloomfilter macro writer", K(ret));
  } else {
    ObBloomFilterDataReader bf_macro_reader;
    ObBloomFilterCacheValue bf_cache_value;
    ObMacroBlockCtx bf_block_ctx;
    ObITable* table = NULL;
    ObSSTable* sstable = NULL;
    ObStorageFile* file = nullptr;
    need_build_bloom_filter_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && need_build_bloom_filter_ && i < ctx.tables_handle_.get_tables().count(); i++) {
      if (OB_ISNULL(table = ctx.tables_handle_.get_tables().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null table", KP(table), K(ret));
      } else if (!table->is_sstable()) {
        break;
      } else if (FALSE_IT(sstable = reinterpret_cast<ObSSTable*>(table))) {
      } else if (0 == sstable->get_total_row_count()) {
        // skip empty sstable
      } else if (!sstable->has_bloom_filter_macro_block()) {
        need_build_bloom_filter_ = false;
      } else if (OB_FAIL(sstable->get_bf_macro_block_ctx(bf_block_ctx))) {
        STORAGE_LOG(WARN, "Failed to get bloomfilter block ctx", K(ret));
      } else if (nullptr == file && OB_ISNULL(file = desc_.file_handle_.get_storage_file())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to get pg_file", K(ret), K(desc_.file_handle_));
      } else {
        ObKVCacheHandle cache_handle;
        const ObBloomFilterCacheValue* bf_cached_cache = NULL;
        if (OB_FAIL(ObStorageCacheSuite::get_instance().get_bf_cache().get_sstable_bloom_filter(sstable->get_table_id(),
                bf_block_ctx.get_macro_block_id(),
                file->get_file_id(),
                sstable->get_rowkey_column_count(),
                bf_cached_cache,
                cache_handle))) {
          if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
            STORAGE_LOG(WARN, "Failed to get bloomfilter cache value from exist bf cache", K(bf_block_ctx), K(ret));
          }
          ret = OB_SUCCESS;
        }
        if (OB_NOT_NULL(bf_cached_cache)) {

        } else if (OB_FAIL(bf_macro_reader.read_bloom_filter(bf_block_ctx, file, bf_cache_value))) {
          if (OB_NOT_SUPPORTED == ret) {
            need_build_bloom_filter_ = false;
          } else {
            STORAGE_LOG(WARN, "Failed to read bloomfilter cache", K(ret));
          }
        } else if (OB_UNLIKELY(!bf_cache_value.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected bloomfilter cache value", K(bf_cache_value), K(ret));
        } else {
          bf_cached_cache = &bf_cache_value;
        }

        if (OB_SUCC(ret) && OB_FAIL(bf_macro_writer_.append(*bf_cached_cache))) {
          if (OB_NOT_SUPPORTED == ret) {
            need_build_bloom_filter_ = false;
            ret = OB_SUCCESS;
          } else {
            STORAGE_LOG(WARN, "Failed to append bloomfilter cache value", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret) || !need_build_bloom_filter_) {
      if (OB_NOT_SUPPORTED == ret) {
        ret = OB_SUCCESS;
      }
      bf_macro_writer_.reset();
    }
  }

  return ret;
}

int ObMacroBlockBuilder::process(const blocksstable::ObMacroBlockCtx& macro_block_ctx)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  int64_t macro_block_builder_errsim_flag = GCONF.macro_block_builder_errsim_flag;
  if (2 == macro_block_builder_errsim_flag) {
    if (writer_->get_macro_block_write_ctx().get_macro_block_count() - sstable_merge_info_.use_old_macro_block_count_ >=
        1) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "fake macro_block_builder_errsim_flag", K(ret), K(macro_block_builder_errsim_flag));
    }
  }
#endif

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(!macro_block_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(macro_block_ctx), K(ret));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The builder has not been opened, ", K(ret));
  } else if (OB_FAIL(writer_->append_macro_block(macro_block_ctx))) {
    STORAGE_LOG(WARN, "macro block writer fail to close.", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "Success to append macro block, ", K(macro_block_ctx));
  }
  return ret;
}

int ObMacroBlockBuilder::process(const ObMicroBlock& micro_block)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The builder has not been opened", K(ret));
  } else if (!micro_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid micro_block", K(micro_block), K(ret));
  } else if (OB_FAIL(writer_->append_micro_block(micro_block))) {
    LOG_WARN("macro block writer append micro_block failed", K(micro_block), K(ret));
  }
  return ret;
}

int ObMacroBlockBuilder::check_row_columns(const ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (row.is_sparse_row_) {
    /* not check for sparse row
    if (OB_FAIL(check_sparse_row_columns(row))) {
      LOG_WARN("check sparse row columns failed", K(row), K(ret));
    }
    */
  } else {
    if (OB_FAIL(check_flat_row_columns(row))) {
      LOG_WARN("check flat row columns failed", K(row), K(ret));
    }
  }
  return ret;
}

int ObMacroBlockBuilder::check_sparse_row_columns(const ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (!row.is_sparse_row_) {
    ret = OB_INVALID_ARGUMENT;
  } else if (ObActionFlag::OP_ROW_EXIST != row.flag_) {
    // do nothing
  } else if (OB_ISNULL(cols_id_map_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("col id map is null", K(row), K_(cols_id_map), K(ret));
  } else {
    int32_t proj_pos = -1;
    for (int i = 0; OB_SUCC(ret) && i < row.row_val_.count_; ++i) {
      if (OB_FAIL(cols_id_map_->get(row.column_ids_[i], proj_pos))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;  // not found is not an error
        } else {
          STORAGE_LOG(WARN, "get projector from ColumnMap failed", K(ret), "column_id", row.column_ids_[i]);
        }
      } else if (0 > proj_pos) {
        // not needed column
      } else if (OB_FAIL(check_sparse_row_column(row.row_val_.cells_[i], proj_pos))) {
        LOG_WARN(
            "failed to check row column", K(ret), K(i), K(proj_pos), K(row.row_val_.cells_[i]), K(row.column_ids_[i]));
      }
    }
  }
  return ret;
}

int ObMacroBlockBuilder::check_flat_row_columns(const ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (ObActionFlag::OP_ROW_EXIST != row.flag_) {
  } else if (row.row_val_.count_ != desc_.row_column_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected column count of store row", K(row), K_(desc), K(ret));
  } else {
    const int64_t interval = 4;
    int64_t i = 0;
    for (i = 0; i + interval < row.row_val_.count_; i += interval) {
      const int tmp0 = check_row_column(row, i + 0);
      const int tmp1 = check_row_column(row, i + 1);
      const int tmp2 = check_row_column(row, i + 2);
      const int tmp3 = check_row_column(row, i + 3);
      if (OB_FAIL(tmp0) || OB_FAIL(tmp1) || OB_FAIL(tmp2) || OB_FAIL(tmp3)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to check row column", K(ret), K(i), K(interval), K(row));
        break;
      }
    }

    for (; OB_SUCC(ret) && i < row.row_val_.count_; ++i) {
      if (OB_FAIL(check_row_column(row, i))) {
        LOG_WARN("failed to check row column", K(ret), K(i));
      }
    }
  }

  return ret;
}

OB_INLINE int ObMacroBlockBuilder::check_row_column(const storage::ObStoreRow& row, const int64_t idx)
{
  int ret = OB_SUCCESS;
  const ObObj& obj = row.row_val_.cells_[idx];

  if (obj.is_nop_value() || obj.is_null()) {
    // pass
  } else if (obj.get_type() != desc_.column_types_[idx].get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected column type of store row",
        K(ret),
        K(idx),
        K(obj),
        "column_type",
        desc_.column_types_[idx],
        "col id",
        desc_.column_ids_[idx],
        K_(desc),
        K(row));
  }

  return ret;
}

OB_INLINE int ObMacroBlockBuilder::check_sparse_row_column(const ObObj& obj, const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (obj.is_nop_value() || obj.is_null()) {
    // pass
  } else if (obj.get_type() != desc_.column_types_[idx].get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected column type of store row",
        K(ret),
        K(idx),
        K(obj),
        "column_type",
        desc_.column_types_[idx],
        "col id",
        desc_.column_ids_[idx],
        K_(desc));
  }

  return ret;
}

int ObMacroBlockBuilder::append_bloom_filter(const ObStoreRow& row)
{
  int ret = OB_SUCCESS;

  if (!is_opened_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The builder has not been opened", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The row is invalid to append", K(row), K(ret));
  } else if (OB_UNLIKELY(is_major_merge(merge_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Major merge should not build bloomfilter for sstable", K(ret));
  } else if (OB_UNLIKELY(!need_build_bloom_filter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected status for append bloomfilter", K_(need_build_bloom_filter), K(ret));
  } else if (OB_FAIL(bf_macro_writer_.append(row))) {
    if (OB_NOT_SUPPORTED == ret) {
      ret = OB_SUCCESS;
      need_build_bloom_filter_ = false;
      bf_macro_writer_.reset();
    } else {
      LOG_WARN("Failed to append row to bloomfilter", K(row), K(ret));
    }
  }

  return ret;
}

int ObMacroBlockBuilder::process(const ObStoreRow& row, const ObCompactRowType::ObCompactRowTypeEnum type)
{
  int ret = OB_SUCCESS;
  bool is_output = true;

#ifdef ERRSIM
  int64_t macro_block_builder_errsim_flag = GCONF.macro_block_builder_errsim_flag;
  if (1 == macro_block_builder_errsim_flag && NULL != writer_) {
    if (writer_->get_macro_block_write_ctx().get_macro_block_count() > 1) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "fake macro_block_builder_errsim_flag", K(ret), K(macro_block_builder_errsim_flag));
    }
  }
#endif

  if (OB_FAIL(ret)) {
    // fake errsim
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The builder_ has not been opened", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "The row is invalid, ", K(row), K(ret));
  } else if (OB_FAIL(check_row_columns(row))) {
    STORAGE_LOG(WARN, "The row is invalid, ", K(row), K_(desc), K(ret));
  } else if (!is_multi_version_minor_merge(merge_type_)) {
    if ((ObActionFlag::OP_ROW_EXIST == row.flag_ || row.row_type_flag_.is_uncommitted_row()) &&
        OB_FAIL(writer_->append_row(row))) {
      STORAGE_LOG(WARN, "Fail to append row to builder_", K(ret), K_(merge_type));
    } else if (ObActionFlag::OP_DEL_ROW == row.flag_) {
      is_output = false;
      if (OB_FAIL(writer_->append_row(row, true /*virtual_append*/))) {
        STORAGE_LOG(WARN, "Fail to append row to builder_", K(ret), K_(merge_type));
      } else {
        STORAGE_LOG(DEBUG, "Success to virtual append row to builder_", K(ret), K_(merge_type), K(row));
      }
    }
  } else {  // minor merge
    if (common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == row.flag_) {
      STORAGE_LOG(ERROR, "Unexpected not exist row to appen", K(ret), K(row), K_(merge_type));
    } else if (OB_FAIL(writer_->append_row(row))) {  // del_row need been stored when minor merge
      STORAGE_LOG(WARN, "Fail to append row to builder_", K(ret), K_(merge_type));
    } else if (need_build_bloom_filter_ && OB_FAIL(append_bloom_filter(row))) {
      STORAGE_LOG(WARN, "Failed to append row to bloomfilter", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "Success to append row to builder_", K(ret), K_(merge_type), K(row));
    }
  }
  if (OB_SUCC(ret)) {
    est_row(type, is_output);
  }
  return ret;
}

int ObMacroBlockBuilder::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The builder_ has not been opened, ", K(ret));
  } else if (NULL == writer_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, the writer is NULL, ", K(ret));
  } else if (OB_FAIL(writer_->close())) {
    STORAGE_LOG(WARN, "macro block fail to close.", K(ret));
  } else {
    sstable_merge_info_.merge_finish_time_ = ::oceanbase::common::ObTimeUtility::current_time();
    sstable_merge_info_.merge_cost_time_ =
        sstable_merge_info_.merge_finish_time_ - sstable_merge_info_.merge_start_time_;
    if (OB_FAIL(merge_context_->add_macro_blocks(task_idx_,
            &(writer_->get_macro_block_write_ctx()),
            desc_.has_lob_column_ ? &(writer_->get_lob_macro_block_write_ctx()) : NULL,
            sstable_merge_info_))) {
      STORAGE_LOG(WARN, "Fail to update macro blocks, ", K(ret));
    } else if (need_build_bloom_filter_ && bf_macro_writer_.get_row_count() > 0) {
      if (OB_FAIL(bf_macro_writer_.flush_bloom_filter())) {
        STORAGE_LOG(WARN, "Failed to flush bloomfilter macro block", K(ret));
      } else if (OB_UNLIKELY(bf_macro_writer_.get_block_write_ctx().is_empty())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected macro block write ctx", K(ret));
      } else if (OB_FAIL(ObStorageCacheSuite::get_instance().get_bf_cache().put_bloom_filter(desc_.table_id_,
                     bf_macro_writer_.get_block_write_ctx().macro_block_list_.at(0),
                     merge_context_->get_file_id(),
                     bf_macro_writer_.get_bloomfilter_cache_value()))) {
        if (OB_ENTRY_EXIST != ret) {
          STORAGE_LOG(WARN, "Fail to put value to bloom_filter_cache", K(desc_.table_id_), K(ret));
        }
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(INFO, "Succ to put value to bloom_filter_cache", K(desc_.table_id_));
      }
      if (OB_SUCC(ret) && OB_FAIL(merge_context_->add_bloom_filter(bf_macro_writer_.get_block_write_ctx()))) {
        STORAGE_LOG(WARN, "Failed to add bloomfilter block ctx to merge context", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_opened_ = false;
      sstable_merge_info_.dump_info("macro block builder close");
    }
  }

  return ret;
}

// TODO reuse macroblocks may change the way of statistics
void ObMacroBlockBuilder::est_row(const ObCompactRowType::ObCompactRowTypeEnum& type, const bool is_output)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The builder is not opened,", K(ret));
  } else {
    if (is_output) {
      ++sstable_merge_info_.output_row_count_;
    }
    switch (type) {
      case ObCompactRowType::T_BASE_ROW:
        ++sstable_merge_info_.base_row_count_;
        ++sstable_merge_info_.use_base_row_count_;
        break;
      case ObCompactRowType::T_INSERT_ROW:
        ++sstable_merge_info_.insert_row_count_;
        ++sstable_merge_info_.memtable_row_count_;
        break;
      case ObCompactRowType::T_UPDATE_ROW:
        ++sstable_merge_info_.base_row_count_;
        ++sstable_merge_info_.update_row_count_;
        ++sstable_merge_info_.memtable_row_count_;
        break;
      case ObCompactRowType::T_DELETE_ROW:
        ++sstable_merge_info_.delete_row_count_;
        ++sstable_merge_info_.base_row_count_;
        ++sstable_merge_info_.memtable_row_count_;
        break;
      default:
        // do nothing
        break;
    }
  }
}

void ObMacroBlockBuilder::reset()
{
  if (NULL != writer_) {
    writer_->~ObMacroBlockWriter();
    writer_ = NULL;
  }
  merge_type_ = INVALID_MERGE_TYPE;
  writer_ = NULL;
  merge_context_ = NULL;
  desc_.reset();
  sstable_merge_info_.reset();
  task_idx_ = 0;
  if (NULL != mark_deletion_maker_) {
    mark_deletion_maker_->~ObBlockMarkDeletionMaker();
    mark_deletion_maker_ = NULL;
  }
  if (OB_NOT_NULL(cols_id_map_)) {
    cols_id_map_->~ColumnMap();
    cols_id_map_ = NULL;
  }
  allocator_.reset();
  need_build_bloom_filter_ = false;
  bf_macro_writer_.reset();
  is_opened_ = false;
}

void ObMacroBlockBuilder::set_purged_count(const int64_t count)
{
  sstable_merge_info_.purged_row_count_ = count;
}

/**
 * ---------------------------------------------------------ObMacroBlockEstimator--------------------------------------------------------------
 */
ObMacroBlockEstimator::ObMacroBlockEstimator()
    : component_(NULL),
      is_opened_(false),
      partition_id_(0),
      stat_sampling_ratio_(0),
      stat_sampling_count_(0),
      merge_context_(nullptr),
      allocator_(ObModIds::OB_CS_COMMON)
{}

ObMacroBlockEstimator::~ObMacroBlockEstimator()
{}

int ObMacroBlockEstimator::open(storage::ObSSTableMergeCtx& ctx, const int64_t idx, const bool iter_complement,
    const ObIArray<ObColDesc>& column_ids, const ObIArray<ObMacroBlockInfoPair>* lob_blocks)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(ctx));
  } else if (OB_ISNULL(component_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The component is NULL, ", K(ret));
  } else if (OB_UNLIKELY(is_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The ObMacroBlockEstimator has been opened, ", K(ret));
  } else if (OB_FAIL(component_->open(ctx, idx, iter_complement, column_ids, lob_blocks))) {
    LOG_WARN("Fail to open component", K(ret), K(ctx));
  } else if (FALSE_IT(stat_sampling_ratio_ = ctx.stat_sampling_ratio_)) {
  } else if (stat_sampling_ratio_ > 0) {
    stat_sampling_count_ = 0;
    partition_id_ = ctx.param_.pkey_.get_partition_id();
    merge_context_ = &(ctx.merge_context_);
    allocator_.reuse();
    int tmp_ret = OB_SUCCESS;
    ObSEArray<ObColDesc, OB_DEFAULT_SE_ARRAY_COUNT> column_ids;
    uint64_t table_id = ctx.param_.index_id_;
    void* ptr = NULL;
    ObColumnStat* stat_ptr = NULL;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ctx.table_schema_->get_store_column_ids(column_ids)))) {
      LOG_WARN("Fail to get column ids. ", K(tmp_ret));
    }
    for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < column_ids.count(); ++i) {
      ptr = allocator_.alloc(sizeof(common::ObColumnStat));
      if (OB_ISNULL(ptr) || OB_ISNULL(stat_ptr = new (ptr) common::ObColumnStat(allocator_)) ||
          OB_UNLIKELY(!stat_ptr->is_writable())) {
        tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for ObColumnStat object. ", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = column_stats_.push_back(stat_ptr))) {
        LOG_WARN("fail to push back stat_ptr. ", K(tmp_ret));
      } else {
        stat_ptr->reset();
        stat_ptr->set_table_id(table_id);
        stat_ptr->set_partition_id(partition_id_);
        stat_ptr->set_column_id(column_ids.at(i).col_id_);
        ptr = NULL;
        stat_ptr = NULL;
      }
    }
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      stat_sampling_ratio_ = 0;
    }
  }

  if (OB_SUCC(ret)) {
    is_opened_ = true;
  }
  return ret;
}

int ObMacroBlockEstimator::process(const blocksstable::ObMacroBlockCtx& macro_block_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The ObStatEstimator has not been opened, ", K(ret));
  } else if (OB_UNLIKELY(!macro_block_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(macro_block_ctx), K(ret));
  } else if (OB_FAIL(component_->process(macro_block_ctx))) {
    LOG_WARN("Fail to process macro block, ", K(macro_block_ctx), K(ret));
  }
  return ret;
}

int ObMacroBlockEstimator::process(const ObMicroBlock& micro_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The ObStatEstimator has not been opened, ", K(ret));
  } else if (OB_UNLIKELY(!micro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(micro_block), K(ret));
  } else if (OB_FAIL(component_->process(micro_block))) {
    LOG_WARN("Fail to process macro block, ", K(micro_block), K(ret));
  }
  return ret;
}

int ObMacroBlockEstimator::process(const storage::ObStoreRow& row, const ObCompactRowType::ObCompactRowTypeEnum type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The ObStatEstimator has not been opened, ", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(row), K(ret));
  } else if (OB_FAIL(component_->process(row, type))) {
    LOG_WARN("Fail to process row, ", K(ret));
  } else if (stat_sampling_ratio_ > 0 && ObActionFlag::OP_ROW_EXIST == row.flag_) {
    if (stat_sampling_count_++ < stat_sampling_ratio_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = update_estimator(row))) {
        stat_sampling_ratio_ = 0;
        LOG_WARN("Fail to estimator row, reset sampling ratio", K_(stat_sampling_ratio), K(tmp_ret));
      }
    }
    if (stat_sampling_count_ >= 100) {
      stat_sampling_count_ = 0;
    }
  }
  return ret;
}

int ObMacroBlockEstimator::update_estimator(const storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row.is_valid()) || stat_sampling_ratio_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(row), K_(stat_sampling_ratio), K(ret));
  } else if (OB_UNLIKELY(column_stats_.count() != row.row_val_.count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column count not equal to row cell count.", K(column_stats_.count()), K(row.row_val_.count_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats_.count(); ++i) {
      if (NULL == column_stats_.at(i)) {
        // skip.
      } else if (row.row_val_.cells_[i].is_lob()) {
        // skip lob with colun stat
      } else if (OB_FAIL(column_stats_.at(i)->add_value(row.row_val_.cells_[i]))) {
        LOG_WARN("fill column stat error.", K(ret), K(i), K(row.row_val_.cells_[i]));
      }
    }
  }
  return ret;
}

int ObMacroBlockEstimator::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroBlockEstimator has not been opened, ", K(ret));
  } else if (OB_FAIL(component_->close())) {
    LOG_WARN("Fail to close component, ", K(ret));
  } else if (nullptr != merge_context_) {
    // ignore ret
    (void)merge_context_->add_column_stats(column_stats_);
  }
  return ret;
}

void ObMacroBlockEstimator::reset()
{
  is_opened_ = false;
  merge_context_ = nullptr;
  stat_sampling_ratio_ = 0;
  stat_sampling_count_ = 0;
  column_stats_.reuse();
  allocator_.reuse();
  if (NULL != component_) {
    component_->reset();
  }
}

void ObMacroBlockEstimator::set_purged_count(const int64_t count)
{
  if (NULL != component_) {
    component_->set_purged_count(count);
  }
}

} /* namespace compaction */
} /* namespace oceanbase */
