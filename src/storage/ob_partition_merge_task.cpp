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
#include "storage/ob_partition_merge_task.h"
#include "lib/time/ob_time_utility.h"
#include "lib/stat/ob_session_stat.h"
#include "share/stat/ob_stat_manager.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_index_task_table_operator.h"
#include "observer/ob_sstable_checksum_updater.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/ob_partition_scheduler.h"
#include "storage/ob_sstable_merge_info_mgr.h"
#include "storage/ob_build_index_task.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_table_mgr.h"
#include "storage/ob_server_checkpoint_writer.h"
#include "storage/ob_partition_split_task.h"
#include "share/ob_task_define.h"
#include "share/ob_force_print_log.h"
#include "ob_sstable.h"
#include "ob_i_store.h"
#include "storage/ob_ms_row_iterator.h"
#include "storage/ob_dag_warning_history_mgr.h"
#include "storage/ob_pg_storage.h"

namespace oceanbase {
namespace storage {
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::compaction;
using namespace share::schema;
using namespace share;
using namespace omt;
using namespace transaction;

bool is_merge_dag(ObIDag::ObIDagType dag_type)
{
  return dag_type == ObIDag::DAG_TYPE_SSTABLE_MAJOR_MERGE || dag_type == ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE ||
         dag_type == ObIDag::DAG_TYPE_SSTABLE_MINI_MERGE;
}

ObMacroBlockMergeTask::ObMacroBlockMergeTask()
    : ObITask(ObITask::TASK_TYPE_MACROMERGE), idx_(0), ctx_(NULL), processor_(NULL), is_inited_(false)
{}

ObMacroBlockMergeTask::~ObMacroBlockMergeTask()
{}

int ObMacroBlockMergeTask::init(const int64_t idx, storage::ObSSTableMergeCtx& ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice, ", K(ret));
  } else if (idx < 0 || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(idx), K(ctx));
  } else {
    if (ctx.stat_sampling_ratio_ > 0) {
      estimator_.set_component(&builder_);
      processor_ = &estimator_;
    } else {
      processor_ = &builder_;
    }
    if (OB_SUCC(ret)) {
      idx_ = idx;
      ctx_ = &ctx;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMacroBlockMergeTask::generate_next_task(ObITask*& next_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, ", K(ret));
  } else if (idx_ + 1 == ctx_->get_concurrent_cnt()) {
    ret = OB_ITER_END;
  } else if (!is_merge_dag(dag_->get_type())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    ObMacroBlockMergeTask* merge_task = NULL;
    ObSSTableMergeDag* merge_dag = static_cast<ObSSTableMergeDag*>(dag_);

    if (OB_FAIL(merge_dag->alloc_task(merge_task))) {
      LOG_WARN("Fail to alloc task, ", K(ret));
    } else if (OB_FAIL(merge_task->init(idx_ + 1, *ctx_))) {
      LOG_WARN("Fail to init task, ", K(ret));
    } else {
      next_task = merge_task;
    }
  }
  return ret;
}

int ObMacroBlockMergeTask::process_iter_to_complement(transaction::ObTransService* txs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(txs) || OB_ISNULL(processor_) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans service or builder or ctx is null", K(ret), K(txs), K(processor_), K(ctx_));
  } else {
    processor_->reset();  // reset macro builder
    if (OB_FAIL(ObPartitionMergeUtil::merge_partition(txs->get_mem_ctx_factory(), *ctx_, *processor_, idx_, true))) {
      LOG_WARN("Fail to merge partition, ", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockMergeTask::process()
{
  int ret = OB_SUCCESS;
  ObTenantStatEstGuard stat_est_guard(OB_SYS_TENANT_ID);
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  transaction::ObTransService* txs = ObPartitionService::get_instance().get_trans_service();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, ", K(ret));
  } else if (OB_ISNULL(txs)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("trans service must not null", K(ret));
  } else if (OB_UNLIKELY(ctx_->param_.is_major_merge() && !ObPartitionScheduler::get_instance().could_merge_start())) {
    ret = OB_CANCELED;
    LOG_INFO("Merge has been paused, ", K(ret));
  } else if (OB_FAIL(
                 ObPartitionMergeUtil::merge_partition(txs->get_mem_ctx_factory(), *ctx_, *processor_, idx_, false))) {
    LOG_WARN("Fail to merge partition, ", K(ret));
  } else if (ctx_->param_.is_mini_merge() && OB_FAIL(process_iter_to_complement(txs))) {
    LOG_WARN("Fail to iter row to complement, ", K(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("merge macro blocks ok", K(idx_), "task", *this);
  }

  if (OB_FAIL(ret)) {
    if (NULL != ctx_) {
      if (OB_CANCELED == ret) {
        STORAGE_LOG(INFO, "merge is canceled, ", K(ret), K(ctx_->param_), K(idx_));
      } else {
        LOG_WARN("Fail to merge, ", K(ret), K(ctx_->param_), K(idx_));
      }
    }
  }

  if (NULL != processor_) {
    processor_->reset();
  }

  return ret;
}

ObSSTableMergeContext::ObSSTableMergeContext()
    : is_inited_(false),
      lock_(),
      block_ctxs_(),
      lob_block_ctxs_(),
      bloom_filter_block_ctx_(nullptr),
      sstable_merge_info_(),
      column_stats_(nullptr),
      allocator_(ObModIds::OB_CS_MERGER, OB_MALLOC_MIDDLE_BLOCK_SIZE),
      finish_count_(0),
      concurrent_cnt_(0),
      merge_complement_(false),
      file_id_(OB_INVALID_DATA_FILE_ID)
{}

ObSSTableMergeContext::~ObSSTableMergeContext()
{
  destroy();
}

void ObSSTableMergeContext::destroy()
{
  is_inited_ = false;
  concurrent_cnt_ = 0;

  for (int64_t i = 0; i < block_ctxs_.count(); ++i) {
    if (NULL != block_ctxs_.at(i)) {
      block_ctxs_.at(i)->~ObMacroBlocksWriteCtx();
    }
  }
  block_ctxs_.reset();

  for (int64_t i = 0; i < lob_block_ctxs_.count(); ++i) {
    if (NULL != lob_block_ctxs_.at(i)) {
      lob_block_ctxs_.at(i)->~ObMacroBlocksWriteCtx();
    }
  }
  lob_block_ctxs_.reset();
  if (OB_NOT_NULL(bloom_filter_block_ctx_)) {
    bloom_filter_block_ctx_->~ObMacroBlocksWriteCtx();
    bloom_filter_block_ctx_ = NULL;
  }
  sstable_merge_info_.reset();
  allocator_.reset();
  finish_count_ = 0;
}

int ObSSTableMergeContext::init(const int64_t concurrent_cnt, const bool has_lob, ObIArray<ObColumnStat*>* column_stats,
    const bool merge_complement)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (concurrent_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(concurrent_cnt));
  } else if (OB_FAIL(block_ctxs_.prepare_allocate(concurrent_cnt))) {
    LOG_WARN("failed to reserve block arrays", K(ret), K(concurrent_cnt));
  } else if (has_lob && OB_FAIL(lob_block_ctxs_.prepare_allocate(concurrent_cnt))) {
    LOG_WARN("failed to reserve lob block arrays", K(ret), K(concurrent_cnt));
  } else {
    for (int64_t i = 0; i < concurrent_cnt; ++i) {
      block_ctxs_[i] = NULL;
      if (has_lob) {
        lob_block_ctxs_[i] = NULL;
      }
    }
    bloom_filter_block_ctx_ = NULL;
    column_stats_ = column_stats;
    concurrent_cnt_ = concurrent_cnt;
    finish_count_ = 0;
    merge_complement_ = merge_complement;
    is_inited_ = true;
  }

  return ret;
}

int ObSSTableMergeContext::add_macro_blocks(const int64_t idx, blocksstable::ObMacroBlocksWriteCtx* write_ctx,
    blocksstable::ObMacroBlocksWriteCtx* lob_blocks_ctx, const ObSSTableMergeInfo& sstable_merge_info)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (idx < 0 || idx >= concurrent_cnt_ || OB_ISNULL(write_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret), K(idx), K(concurrent_cnt_));
  } else if (NULL != block_ctxs_[idx]) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "block ctx is valid, fatal error", K(ret), K(idx));
  } else if (OB_FAIL(sstable_merge_info_.add(sstable_merge_info))) {
    LOG_WARN("failed to add sstable_merge_info", K(ret));
  } else if (OB_FAIL(new_block_write_ctx(block_ctxs_[idx]))) {
    LOG_WARN("failed to new block write ctx", K(ret));
  } else if (OB_FAIL(block_ctxs_[idx]->set(*write_ctx))) {
    LOG_WARN("failed to assign block arrays", K(ret), K(idx), K(write_ctx));
  } else if (need_lob_macro_blocks() && OB_NOT_NULL(lob_blocks_ctx) &&
             OB_FAIL(add_lob_macro_blocks(idx, lob_blocks_ctx))) {
    LOG_WARN("failed to add lob macro blocks", K(ret));
  } else {
    ++finish_count_;
  }
  return ret;
}

int ObSSTableMergeContext::add_column_stats(const common::ObIArray<common::ObColumnStat*>& column_stats)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(column_stats_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The column stats is null, ", K(ret));
  } else if (OB_UNLIKELY(column_stats_->count() != column_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not equal column count, ", K(ret), K(column_stats_->count()), K(column_stats.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats_->count(); ++i) {
      if (OB_FAIL(column_stats_->at(i)->add(*column_stats.at(i)))) {
        LOG_WARN("Fail to add column stat, ", K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableMergeContext::add_lob_macro_blocks(const int64_t idx, blocksstable::ObMacroBlocksWriteCtx* blocks_ctx)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= lob_block_ctxs_.count() || OB_ISNULL(blocks_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid idx", K(ret), K(idx), K_(concurrent_cnt));
  } else if (NULL != lob_block_ctxs_[idx]) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "lob_block_arrays_[idx] is inited, fatal error", K(ret), K(idx));
  } else if (OB_FAIL(new_block_write_ctx(lob_block_ctxs_[idx]))) {
    LOG_WARN("failed to new block write ctx", K(ret));
  } else if (OB_FAIL(lob_block_ctxs_[idx]->set(*blocks_ctx))) {
    LOG_WARN("failed to transfer lob marco blocks ctx", K(ret), K(*blocks_ctx));
  } else {
    STORAGE_LOG(DEBUG, "[LOB] sstable merge context add lob macro blocks", K(idx), K(*blocks_ctx), K(ret));
  }
  return ret;
}

int ObSSTableMergeContext::add_bloom_filter(blocksstable::ObMacroBlocksWriteCtx& bloom_filter_block_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!bloom_filter_block_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to add bloomfilter", K(bloom_filter_block_ctx), K(ret));
  } else if (OB_NOT_NULL(bloom_filter_block_ctx_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "The bloom filter block ctx is inited, fatal error", K(ret));
  } else if (OB_FAIL(new_block_write_ctx(bloom_filter_block_ctx_))) {
    LOG_WARN("failed to new block write ctx", K(ret));
  } else if (OB_FAIL(bloom_filter_block_ctx_->set(bloom_filter_block_ctx))) {
    LOG_WARN("failed to transfer bloomfilter marco blocks ctx", K(ret), K(bloom_filter_block_ctx));
  }

  return ret;
}

int ObSSTableMergeContext::create_sstable(storage::ObCreateSSTableParamWithTable& param,
    storage::ObIPartitionGroupGuard& pg_guard, ObTableHandle& table_handle)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* pg = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableMergeContext has not been inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_ISNULL(pg = pg_guard.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, partition group must not be null", K(ret));
  } else if (finish_count_ != concurrent_cnt_) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, not all sub task finished", K(ret), K(finish_count_), K(concurrent_cnt_));
  } else {
    sstable_merge_info_.is_complement_ = merge_complement_;
    ObPGCreateSSTableParam pg_create_sstable_param;
    pg_create_sstable_param.with_table_param_ = &param;
    pg_create_sstable_param.bloomfilter_block_ = bloom_filter_block_ctx_;
    pg_create_sstable_param.sstable_merge_info_ = &sstable_merge_info_;
    ObArray<ObMacroBlocksWriteCtx*>& data_blocks = pg_create_sstable_param.data_blocks_;
    ObArray<ObMacroBlocksWriteCtx*>& lob_blocks = pg_create_sstable_param.lob_blocks_;
    if (OB_FAIL(data_blocks.reserve(block_ctxs_.count()))) {
      LOG_WARN("fail to reserve data block array", K(ret));
    } else if (OB_FAIL(lob_blocks.reserve(lob_block_ctxs_.count()))) {
      LOG_WARN("fail to reserve lob block array", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < block_ctxs_.count(); ++i) {
      if (OB_ISNULL(block_ctxs_.at(i))) {
        ret = OB_ERR_SYS;
        LOG_WARN("data block ctx must not be null", K(ret));
      } else if (OB_FAIL(data_blocks.push_back(block_ctxs_[i]))) {
        LOG_WARN("fail to push back data block write ctx", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < lob_block_ctxs_.count(); ++i) {
      if (nullptr != lob_block_ctxs_[i]) {
        if (OB_FAIL(lob_blocks.push_back(lob_block_ctxs_[i]))) {
          LOG_WARN("fail to push back lob block", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(pg->create_sstable(pg_create_sstable_param, table_handle))) {
        LOG_WARN("fail to create sstable", K(ret));
      } else {
        file_id_ = pg->get_storage_file()->get_file_id();
      }
    }

    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = ObSSTableMergeInfoMgr::get_instance().add_sstable_merge_info(sstable_merge_info_))) {
        LOG_WARN("failed to add sstable merge info ", K(tmp_ret), K(sstable_merge_info_));
      }
    }
  }
  return ret;
}

int ObSSTableMergeContext::create_sstables(ObIArray<storage::ObCreateSSTableParamWithTable>& params,
    storage::ObIPartitionGroupGuard& pg_guard, ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* pg = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableMergeContext has not been inited", K(ret));
  } else if (OB_ISNULL(pg = pg_guard.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, partition group must not be null", K(ret));
  } else if (finish_count_ != concurrent_cnt_) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, not all sub task finished", K(ret), K(finish_count_), K(concurrent_cnt_));
  } else {
    ObTableHandle table_handle;
    sstable_merge_info_.is_complement_ = merge_complement_;

    for (int64_t i = 0; OB_SUCC(ret) && i < block_ctxs_.count(); i++) {
      ObTableHandle handle;
      ObPGCreateSSTableParam pg_create_sstable_param;
      pg_create_sstable_param.with_table_param_ = &params.at(i);
      pg_create_sstable_param.sstable_merge_info_ = &sstable_merge_info_;
      ObArray<ObMacroBlocksWriteCtx*>& data_blocks = pg_create_sstable_param.data_blocks_;
      ObArray<ObMacroBlocksWriteCtx*>& lob_blocks = pg_create_sstable_param.lob_blocks_;

      if (OB_FAIL(data_blocks.push_back(block_ctxs_[i]))) {
        LOG_WARN("fail to push back data block write ctx", K(ret));
      }

      if (OB_SUCC(ret)) {
        if (0 != lob_block_ctxs_.count()) {
          if (nullptr != lob_block_ctxs_[i]) {
            if (OB_FAIL(lob_blocks.push_back(lob_block_ctxs_[i]))) {
              LOG_WARN("fail to push back lob block", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(pg->create_sstable(pg_create_sstable_param, table_handle))) {
          LOG_WARN("fail to create sstable", K(ret));
        } else {
          file_id_ = pg->get_storage_file()->get_file_id();
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(tables_handle.add_table(table_handle))) {
          LOG_WARN("fail to add table", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS !=
            (tmp_ret = ObSSTableMergeInfoMgr::get_instance().add_sstable_merge_info(sstable_merge_info_))) {
          LOG_WARN("failed to add sstable merge info ", K(tmp_ret), K(sstable_merge_info_));
        }
      }
    }
  }
  return ret;
}

int ObSSTableMergeContext::get_data_macro_block_count(int64_t& macro_block_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableMergeContext has not been inited", K(ret));
  } else {
    macro_block_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_ctxs_.count(); i++) {
      if (OB_NOT_NULL(block_ctxs_.at(i))) {
        macro_block_count += block_ctxs_.at(i)->macro_block_list_.count();
      }
    }
  }

  return ret;
}

ObSSTableScheduleMergeParam::ObSSTableScheduleMergeParam()
    : merge_type_(INVALID_MERGE_TYPE),
      merge_version_(),
      pkey_(),
      index_id_(OB_INVALID_ID),
      schedule_merge_type_(INVALID_MERGE_TYPE)
{}

bool ObSSTableScheduleMergeParam::is_valid() const
{
  return pkey_.is_valid() && index_id_ != OB_INVALID_ID &&
         (merge_type_ > INVALID_MERGE_TYPE && merge_type_ < MERGE_TYPE_MAX) &&
         (!is_major_merge() || merge_version_.is_valid());
}

int ObSSTableMergePrepareTask::init_estimate(storage::ObSSTableMergeCtx& ctx)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (0 == ctx.stat_sampling_ratio_) {
    // pass
  } else {
    int tmp_ret = OB_SUCCESS;
    ObColumnStat* column_stat = NULL;
    ObArray<ObColDesc> column_ids;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ctx.table_schema_->get_store_column_ids(column_ids)))) {
      LOG_WARN("Fail to get column ids. ", K(tmp_ret));
    }
    for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < column_ids.count(); ++i) {
      buf = ctx.allocator_.alloc(sizeof(common::ObColumnStat));
      if (OB_ISNULL(buf) || OB_ISNULL(column_stat = new (buf) common::ObColumnStat(ctx.allocator_)) ||
          OB_UNLIKELY(!column_stat->is_writable())) {
        tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for ObColumnStat object. ", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = ctx.column_stats_.push_back(column_stat))) {
        LOG_WARN("fail to push back column_stat. ", K(tmp_ret));
      } else {
        column_stat->reset();
        column_stat->set_table_id(ctx.param_.pkey_.get_table_id());
        column_stat->set_partition_id(ctx.param_.pkey_.get_partition_id());
        column_stat->set_column_id(column_ids.at(i).col_id_);
        buf = NULL;
        column_stat = NULL;
      }
    }
    if (OB_FAIL(tmp_ret)) {
      ctx.stat_sampling_ratio_ = 0;
      LOG_WARN("failed to init_estimate, skip update estimator", K(ctx.stat_sampling_ratio_), K(ret), K(tmp_ret));
    }
  }
  return ret;
}

ObSSTableMergeCtx::ObSSTableMergeCtx()
    : param_(),
      report_(NULL),
      sstable_version_range_(),
      create_snapshot_version_(0),
      dump_memtable_timestamp_(0),
      tables_handle_(),
      base_table_handle_(),
      base_schema_version_(0),
      mv_dep_table_schema_(NULL),
      data_table_schema_(NULL),
      index_stats_(),
      schema_version_(0),
      table_schema_(NULL),
      schema_guard_(),
      bf_rowkey_prefix_(0),
      is_full_merge_(false),
      stat_sampling_ratio_(0),
      merge_level_(MACRO_BLOCK_MERGE_LEVEL),
      multi_version_row_info_(),
      merge_context_(),
      merge_context_for_complement_minor_sstable_(),
      progressive_merge_num_(0),
      progressive_merge_start_version_(0),
      parallel_merge_ctx_(),
      pg_guard_(),
      partition_guard_(),
      checksum_method_(0),
      mv_dep_tables_handle_(),
      column_stats_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
      merged_table_handle_(),
      merged_complement_minor_table_handle_(),
      allocator_(ObModIds::OB_CS_MERGER),
      result_code_(OB_SUCCESS),
      column_checksum_(),
      is_in_progressive_new_checksum_(false),
      backup_progressive_merge_num_(0),
      backup_progressive_merge_start_version_(0),
      is_created_index_first_merge_(false),
      store_column_checksum_in_micro_(false),
      progressive_merge_round_(0),
      progressive_merge_step_(0),
      use_new_progressive_(false),
      create_sstable_for_large_snapshot_(false),
      logical_data_version_(0),
      merge_log_ts_(INT_MAX),
      trans_table_end_log_ts_(0),
      trans_table_timestamp_(0),
      read_base_version_(0)
{}

ObSSTableMergeCtx::~ObSSTableMergeCtx()
{}

bool ObSSTableMergeCtx::is_valid() const
{
  return param_.is_valid() && NULL != report_ && !tables_handle_.empty() && create_snapshot_version_ >= 0 &&
         dump_memtable_timestamp_ >= 0 && schema_version_ >= 0 && base_schema_version_ >= 0 && NULL != table_schema_ &&
         NULL != data_table_schema_ && stat_sampling_ratio_ >= 0 && stat_sampling_ratio_ <= 100 &&
         progressive_merge_num_ >= 0 && progressive_merge_start_version_ >= 0 &&
         (!param_.is_multi_version_minor_merge() || multi_version_row_info_.is_valid()) &&
         parallel_merge_ctx_.is_valid() && logical_data_version_ > 0 && log_ts_range_.is_valid();
}

bool ObSSTableMergeCtx::need_rewrite_macro_block(const ObMacroBlockDesc& block_desc) const
{
  bool need_rewrite = false;
  if (use_new_progressive_) {
    need_rewrite = progressive_merge_num_ > 1 && block_desc.progressive_merge_round_ < progressive_merge_round_ &&
                   progressive_merge_step_ < progressive_merge_num_;
  } else {
    need_rewrite = progressive_merge_start_version_ > 0 && progressive_merge_num_ > 1 &&
                   block_desc.data_version_ < progressive_merge_start_version_ &&
                   param_.merge_version_ >= progressive_merge_start_version_ &&
                   param_.merge_version_ < progressive_merge_start_version_ + progressive_merge_num_;
  }
  return need_rewrite;
}

int ObSSTableMergeCtx::get_storage_format_version(int64_t& storage_format_version) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, tenant schema or data table schema is NULL", K(ret), KP(data_table_schema_));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2200) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major merge should not happen during upgrade", K(ret));
  } else {
    storage_format_version = data_table_schema_->get_storage_format_version();
  }
  return ret;
}

int ObSSTableMergeCtx::get_storage_format_work_version(int64_t& storage_format_work_version) const
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2200) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major merge should not happen during upgrade", K(ret));
  } else {
    storage_format_work_version = param_.merge_version_;
  }
  return ret;
}

ObITable::TableType ObSSTableMergeCtx::get_merged_table_type() const
{
  ObITable::TableType table_type = ObITable::MAX_TABLE_TYPE;

  if (param_.is_major_merge()) {  // MAJOR_MERGE
    table_type = ObITable::TableType::MAJOR_SSTABLE;
  } else if (MINI_MERGE == param_.merge_type_ || MINI_MINOR_MERGE == param_.merge_type_) {
    table_type = ObITable::TableType::MINI_MINOR_SSTABLE;
  } else {  // MINOR_MERGE || HISTORY_MINI_MINOR_MERGE
    table_type = ObITable::TableType::MULTI_VERSION_MINOR_SSTABLE;
  }
  return table_type;
}

int ObSSTableMergeCtx::init_parallel_merge()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parallel_merge_ctx_.init(*this))) {
    STORAGE_LOG(WARN, "Failed to init parallel merge context", K(ret));
  }
  return ret;
}

int ObSSTableMergeCtx::get_merge_range(int64_t parallel_idx, ObExtStoreRange& merge_range, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!parallel_merge_ctx_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid parallel merge ctx", K(ret), K_(parallel_merge_ctx));
  } else if (OB_FAIL(parallel_merge_ctx_.get_merge_range(parallel_idx, merge_range, allocator))) {
    STORAGE_LOG(WARN, "Failed to get merge range from parallel merge ctx", K(ret));
  }

  return ret;
}

ObMergeParameter::ObMergeParameter()
    : tables_handle_(nullptr),
      is_full_merge_(false),
      merge_type_(INVALID_MERGE_TYPE),
      checksum_method_(blocksstable::CCM_UNKOWN),
      merge_level_(MACRO_BLOCK_MERGE_LEVEL),
      table_schema_(NULL),
      mv_dep_table_schema_(NULL),
      version_range_(),
      checksum_calculator_(NULL),
      is_iter_complement_(false)
{}

bool ObMergeParameter::is_valid() const
{
  return tables_handle_ != nullptr && !tables_handle_->empty() && NULL != table_schema_ &&
         (checksum_method_ != CCM_TYPE_AND_VALUE || OB_NOT_NULL(checksum_calculator_)) &&
         merge_type_ > INVALID_MERGE_TYPE && merge_type_ < MERGE_TYPE_MAX;
}

void ObMergeParameter::reset()
{
  tables_handle_ = nullptr;
  is_full_merge_ = false;
  merge_type_ = INVALID_MERGE_TYPE;
  checksum_method_ = blocksstable::CCM_UNKOWN;
  merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  table_schema_ = NULL;
  mv_dep_table_schema_ = NULL;
  version_range_.reset();
  checksum_calculator_ = NULL;
  is_iter_complement_ = false;
}

int ObMergeParameter::init(ObSSTableMergeCtx& merge_ctx, const int64_t idx, const bool iter_complement)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!merge_ctx.is_valid() || idx < 0 || idx >= merge_ctx.get_concurrent_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to assign merge parameter", K(merge_ctx), K(idx), K(ret));
  } else {
    ObITable* table = NULL;
    tables_handle_ = &merge_ctx.tables_handle_;
    int64_t tables_cnt = (NULL == merge_ctx.mv_dep_table_schema_) ? tables_handle_->get_tables().count() : 1;
    is_full_merge_ = merge_ctx.is_full_merge_;
    merge_type_ = merge_ctx.param_.merge_type_;
    checksum_method_ = merge_ctx.checksum_method_;
    checksum_calculator_ = NULL;
    merge_level_ = merge_ctx.merge_level_;
    table_schema_ = merge_ctx.table_schema_;
    mv_dep_table_schema_ = merge_ctx.mv_dep_table_schema_;
    version_range_ = merge_ctx.sstable_version_range_;
    is_iter_complement_ = iter_complement;
    if (OB_SUCC(ret) && need_checksum() && merge_ctx.need_incremental_checksum()) {
      if (OB_FAIL(merge_ctx.column_checksum_.get_checksum_calculator(idx, checksum_calculator_))) {
        LOG_WARN("fail to get checksum calculator", K(ret));
      } else if (OB_ISNULL(checksum_calculator_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null checksum calculator", K(ret));
      }
    }
  }

  return ret;
}

ObWriteCheckpointDag::ObWriteCheckpointDag()
    : ObIDag(DAG_TYPE_MAJOR_MERGE_FINISH, DAG_PRIO_SSTABLE_MAJOR_MERGE), is_inited_(false), frozen_version_(0)
{}

ObWriteCheckpointDag::~ObWriteCheckpointDag()
{}

bool ObWriteCheckpointDag::operator==(const ObIDag& other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    is_same = true;
  }
  return is_same;
}

int64_t ObWriteCheckpointDag::hash() const
{
  return 0;
}

int ObWriteCheckpointDag::init(int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    frozen_version_ = frozen_version;
    is_inited_ = true;
  }
  return ret;
}

int64_t ObWriteCheckpointDag::get_tenant_id() const
{
  return OB_SYS_TENANT_ID;
}

int ObWriteCheckpointDag::fill_comment(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(databuff_printf(buf, buf_len, "major merge finish task"))) {
    LOG_WARN("failed to fill comment", K(ret), K_(frozen_version));
  }

  return ret;
}

ObSSTableMergeDag::ObSSTableMergeDag(const ObIDagType type, const ObIDagPriority priority)
    : ObIDag(type, priority),
      is_inited_(false),
      compat_mode_(ObWorker::CompatMode::INVALID),
      ctx_(),
      merge_type_(INVALID_MERGE_TYPE),
      pkey_(),
      index_id_(0)
{}

ObSSTableMergeDag::~ObSSTableMergeDag()
{
  int report_ret = OB_SUCCESS;
  if (OB_CHECKSUM_ERROR == ctx_.result_code_ && NULL != ctx_.report_) {
    if (OB_SUCCESS != (report_ret = ctx_.report_->report_merge_error(ctx_.param_.pkey_, ctx_.result_code_))) {
      STORAGE_LOG(ERROR, "Fail to submit checksum error task", K(report_ret), K_(ctx));
    }
  }

  if (ctx_.partition_guard_.get_pg_partition() != NULL) {
    ctx_.partition_guard_.get_pg_partition()->set_merge_status(OB_SUCCESS == get_dag_ret());
  }
}

int ObSSTableMergeDag::inner_init(const ObSSTableScheduleMergeParam& param, ObIPartitionReport* report)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(param));
  } else if (!param.is_valid() || OB_ISNULL(report)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), KP(report));
  } else {
    if (OB_FAIL(get_compat_mode_with_table_id(param.pkey_.table_id_, compat_mode_))) {
      LOG_WARN("failed to get compat mode", K(ret), K(param));
    } else {
      ctx_.param_ = param;
      ctx_.report_ = report;
      merge_type_ = param.merge_type_;
      pkey_ = param.pkey_;
      index_id_ = param.index_id_;
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObSSTableMergeDag::operator==(const ObIDag& other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSTableMergeDag& other_merge_dag = static_cast<const ObSSTableMergeDag&>(other);
    if (merge_type_ != other_merge_dag.merge_type_ || pkey_ != other_merge_dag.pkey_ ||
        index_id_ != other_merge_dag.index_id_) {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObSSTableMergeDag::hash() const
{
  int64_t hash_value = 0;

  hash_value = common::murmurhash(&merge_type_, sizeof(merge_type_), hash_value);
  hash_value += pkey_.hash();
  hash_value = common::murmurhash(&index_id_, sizeof(index_id_), hash_value);
  return hash_value;
}

int ObSSTableMergeDag::fill_comment(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const char* merge_type = merge_type_to_str(merge_type_);

  if (OB_FAIL(
          databuff_printf(buf, buf_len, "%s task: key=%s table_id=%lu", merge_type, to_cstring(pkey_), index_id_))) {
    LOG_WARN("failed to fill comment", K(ret), K(ctx_));
  }

  return ret;
}

ObSSTableMajorMergeDag::ObSSTableMajorMergeDag()
    : ObSSTableMergeDag(DAG_TYPE_SSTABLE_MAJOR_MERGE, DAG_PRIO_SSTABLE_MAJOR_MERGE)
{}

ObSSTableMajorMergeDag::~ObSSTableMajorMergeDag()
{
  if (0 == ObDagScheduler::get_instance().get_dag_count(ObIDagType::DAG_TYPE_SSTABLE_MAJOR_MERGE)) {
    ObPartitionScheduler::get_instance().schedule_major_merge();
  }
}

int ObSSTableMajorMergeDag::init(const ObSSTableScheduleMergeParam& param, ObIPartitionReport* report)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(param));
  } else if (!param.is_major_merge()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("is major merge param not match", K(ret), K(param));
  } else if (OB_FAIL(ObSSTableMergeDag::inner_init(param, report))) {
    LOG_WARN("failed to init ObSSTableMergeDag", K(ret));
  }
  return ret;
}

ObSSTableMiniMergeDag::ObSSTableMiniMergeDag()
    : ObSSTableMergeDag(DAG_TYPE_SSTABLE_MINI_MERGE, DAG_PRIO_SSTABLE_MINI_MERGE)
{}

ObSSTableMiniMergeDag::~ObSSTableMiniMergeDag()
{}

int ObSSTableMiniMergeDag::init(const ObSSTableScheduleMergeParam& param, ObIPartitionReport* report)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(param));
  } else if (!param.is_mini_merge()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("is mini merge param not match", K(ret), K(param));
  } else if (OB_FAIL(ObSSTableMergeDag::inner_init(param, report))) {
    LOG_WARN("failed to init ObSSTableMergeDag", K(ret));
  }
  return ret;
}

ObSSTableMinorMergeDag::ObSSTableMinorMergeDag()
    : ObSSTableMergeDag(DAG_TYPE_SSTABLE_MINOR_MERGE, DAG_PRIO_SSTABLE_MINOR_MERGE)
{}

ObSSTableMinorMergeDag::~ObSSTableMinorMergeDag()
{}

int ObSSTableMinorMergeDag::init(const ObSSTableScheduleMergeParam& param, ObIPartitionReport* report)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(param));
  } else if (param.is_major_merge()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("is major merge param not match", K(ret), K(param));
  } else if (OB_FAIL(ObSSTableMergeDag::inner_init(param, report))) {
    LOG_WARN("failed to init ObSSTableMergeDag", K(ret));
  }
  return ret;
}

ObSSTableMergePrepareTask::ObSSTableMergePrepareTask()
    : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_PREPARE), is_inited_(false), merge_dag_(NULL)
{}

ObSSTableMergePrepareTask::~ObSSTableMergePrepareTask()
{}

int ObSSTableMergePrepareTask::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag must not null", K(ret));
  } else if (!is_merge_dag(dag_->get_type())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    merge_dag_ = static_cast<ObSSTableMergeDag*>(dag_);
    if (OB_UNLIKELY(!merge_dag_->get_ctx().param_.is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("ctx.param_ not valid", K(ret), K(merge_dag_->get_ctx().param_));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObSSTableMergePrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObPartitionStorage* storage = NULL;
  ObTenantStatEstGuard stat_est_guard(OB_SYS_TENANT_ID);
  storage::ObSSTableMergeCtx* ctx = NULL;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  ObIPartitionGroup* pg = nullptr;

  DEBUG_SYNC(MERGE_PARTITION_TASK);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(ctx = &merge_dag_->get_ctx())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("ctx must not null", K(ret));
  } else if (OB_UNLIKELY(ctx->param_.is_major_merge() && !ObPartitionScheduler::get_instance().could_merge_start())) {
    ret = OB_CANCELED;
    LOG_INFO("Merge has been paused", K(ret), K(ctx));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(ctx->param_.pkey_, ctx->pg_guard_))) {
    LOG_WARN("Fail to get partition", K(ret), K(ctx));
  } else if (NULL == (pg = ctx->pg_guard_.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The partition must not null", K(ret), K(ctx));
  } else if (OB_FAIL(pg->get_pg_partition(ctx->param_.pkey_, ctx->partition_guard_)) ||
             OB_ISNULL(ctx->partition_guard_.get_pg_partition())) {
    LOG_WARN("Fail to get pg partition", K(ret), K(ctx));
  } else if (OB_ISNULL(
                 storage = static_cast<ObPartitionStorage*>(ctx->partition_guard_.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The partition storage must not NULL", K(ret), K(ctx));
  } else if (ctx->param_.is_multi_version_minor_merge() &&
             OB_FAIL(pg->get_pg_storage().get_trans_table_end_log_ts_and_timestamp(
                 ctx->trans_table_end_log_ts_, ctx->trans_table_timestamp_))) {
    LOG_WARN("failed to get trans_table end_log_ts and timestamp", K(ret), K(ctx->param_));
  } else if (OB_FAIL(storage->build_merge_ctx(*ctx))) {
    LOG_WARN("failed to build merge ctx", K(ret), K(ctx->param_));
  } else if (ctx->param_.is_multi_version_minor_merge()) {
    if (ctx->log_ts_range_.is_empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpected empty log ts range in minor merge", K(ret), K(ctx->log_ts_range_));
    } else {
      ctx->merge_log_ts_ = ctx->log_ts_range_.end_log_ts_;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_estimate(*ctx))) {
    LOG_WARN("failed to init estimate", K(ret));
  } else if (OB_FAIL(generate_merge_sstable_task())) {
    LOG_WARN("Failed to generate_merge_sstable_task", K(ret));
  } else {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("succeed to init merge ctx", "task", *this);
  }

  return ret;
}

int ObSSTableMergePrepareTask::generate_merge_sstable_task()
{
  int ret = OB_SUCCESS;
  ObMacroBlockMergeTask* macro_merge_task = NULL;
  ObSSTableMergeFinishTask* finish_task = NULL;
  storage::ObSSTableMergeCtx* ctx = NULL;

  // add macro merge task
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(ctx = &merge_dag_->get_ctx())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("ctx must not null", K(ret));
  } else if (ObITable::is_trans_sstable(ctx->get_merged_table_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpecte trans sstable to do normal merge", K(ret), KPC(ctx));
  } else {
    if (ctx->param_.is_major_merge() && ctx->create_sstable_for_large_snapshot_) {
      // if base_table is larger than next_major_freeze_ts, we do not need to merge
      // just create a major sstable with same version range and param.merge_version
      if (OB_FAIL(create_sstable_for_large_snapshot(*ctx))) {
        LOG_WARN("failed to create_sstable_for_large_snapshot", K(ret));
      }
    } else if (OB_FAIL(merge_dag_->alloc_task(macro_merge_task))) {
      LOG_WARN("Fail to alloc task", K(ret));
    } else if (OB_ISNULL(macro_merge_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpecte null macro merge task", K(ret), KPC(ctx));
    } else {
      if (OB_FAIL(macro_merge_task->init(0 /*task_idx*/, *ctx))) {
        LOG_WARN("Fail to init macro merge task", K(ret), K(ctx));
      } else if (OB_FAIL(add_child(*macro_merge_task))) {
        LOG_WARN("Fail to add child", K(ret), K(ctx));
      } else if (OB_FAIL(merge_dag_->add_task(*macro_merge_task))) {
        LOG_WARN("Fail to add task", K(ret), K(ctx));
      }
    }
  }

  // add finish task
  if (OB_SUCC(ret)) {
    if (OB_FAIL(merge_dag_->alloc_task(finish_task))) {
      LOG_WARN("Fail to alloc task", K(ret), K(ctx));
    } else {
      if (OB_FAIL(finish_task->init())) {
        LOG_WARN("Fail to init main table finish task", K(ret), K(ctx));
      } else if (OB_NOT_NULL(macro_merge_task) && OB_FAIL(macro_merge_task->add_child(*finish_task))) {
        LOG_WARN("Fail to add child", K(ret), K(ctx));
      } else if (OB_ISNULL(macro_merge_task) && OB_FAIL(add_child(*finish_task))) {
        LOG_WARN("Fail to add child", K(ret), K(ctx));
      } else if (OB_FAIL(merge_dag_->add_task(*finish_task))) {
        LOG_WARN("Fail to add task", K(ret), K(ctx));
      }
    }
  }
  return ret;
}

int ObSSTableMergePrepareTask::create_sstable_for_large_snapshot(ObSSTableMergeCtx& ctx)
{
  int ret = OB_SUCCESS;
  ObCreateSSTableParamWithTable param;
  ObSSTable* base_sstable = nullptr;
  ObSSTable* new_sstable = nullptr;
  if (OB_FAIL(ctx.base_table_handle_.get_sstable(base_sstable))) {
    LOG_WARN("failed to get base sstable", K(ret));
  } else if (OB_ISNULL(base_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base sstable should not be null", K(ret));
  } else {
    param.table_key_.pkey_ = ctx.param_.pkey_;
    param.table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
    param.table_key_.table_id_ = ctx.param_.index_id_;
    param.table_key_.version_ = ctx.param_.merge_version_;
    param.table_key_.trans_version_range_ = base_sstable->get_version_range();
    param.schema_ = ctx.table_schema_;
    param.schema_version_ = ctx.schema_version_;
    param.progressive_merge_start_version_ = base_sstable->get_meta().progressive_merge_start_version_;
    param.progressive_merge_end_version_ = base_sstable->get_meta().progressive_merge_end_version_;
    param.create_snapshot_version_ = base_sstable->get_meta().create_snapshot_version_;
    param.dump_memtable_timestamp_ = base_sstable->get_timestamp();
    param.checksum_method_ = base_sstable->get_meta().checksum_method_;
    param.progressive_merge_round_ = base_sstable->get_meta().progressive_merge_round_;
    param.progressive_merge_step_ = base_sstable->get_meta().progressive_merge_step_;
    param.logical_data_version_ = ctx.logical_data_version_;
    ObPGCreateSSTableParam pg_create_sstable_param;
    pg_create_sstable_param.with_table_param_ = &param;
    if (OB_SUCC(ret)) {
      ObIPartitionGroup* pg = nullptr;
      if (OB_ISNULL(pg = ctx.pg_guard_.get_partition_group())) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, partition group must not be null", K(ret));
      } else if (OB_FAIL(pg->create_sstable(pg_create_sstable_param, ctx.merged_table_handle_))) {
        LOG_WARN("fail to create sstable", K(ret));
      } else if (OB_FAIL(ctx.merged_table_handle_.get_sstable(new_sstable))) {
        LOG_WARN("fail to get merged sstable", K(ret));
      } else {
        FLOG_INFO("success to create sstable for larger snapshot version than major ts", K(*new_sstable));
      }
    }
  }
  return ret;
}

ObSSTableMergeFinishTask::ObSSTableMergeFinishTask()
    : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_FINISH), is_inited_(false), merge_dag_(NULL)
{}

ObSSTableMergeFinishTask::~ObSSTableMergeFinishTask()
{}

int ObSSTableMergeFinishTask::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag must not null", K(ret));
  } else if (!is_merge_dag(dag_->get_type())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    merge_dag_ = static_cast<ObSSTableMergeDag*>(dag_);
    if (OB_UNLIKELY(!merge_dag_->get_ctx().is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("ctx not valid", K(ret), K(merge_dag_->get_ctx()));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObSSTableMergeFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObITable::TableType type = ObITable::TableType::MAX_TABLE_TYPE;
  ObIPartitionGroupGuard partition_guard;
  ObPGPartitionGuard pg_partition_guard;
  ObPartitionStorage* storage = NULL;
  ObSSTable* sstable = NULL;
  ObSSTable* complement_minor_sstable = NULL;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  transaction::ObTransService* txs = ObPartitionService::get_instance().get_trans_service();
  bool is_all_checked = false;
  const int64_t max_kept_major_version_number = GCONF.max_kept_major_version_number;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", K(ret));
  } else if (OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("txs is null", K(ret));
  } else if (OB_FAIL(merge_dag_->get_ctx().result_code_)) {
    LOG_WARN("result_code is not success, cannot finish merge", K(ret), K(merge_dag_->get_ctx()));
  } else {
    storage::ObSSTableMergeCtx& ctx = merge_dag_->get_ctx();
    ObPartitionKey& pkey = ctx.param_.pkey_;
    type = ctx.get_merged_table_type();

    if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(ctx.partition_guard_.get_pg_partition()->get_storage()))) {
      ret = OB_ERR_SYS;
      LOG_WARN("partition storage must not null", K(ret), K(pkey));
    } else if (ctx.merged_table_handle_.is_valid()) {
      if (OB_UNLIKELY(ctx.param_.is_mini_merge())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unxpected valid merged table handle with mini merge", K(ret), K(ctx));
      } else if (OB_FAIL(ctx.merged_table_handle_.get_sstable(sstable))) {
        LOG_WARN("failed to get sstable", K(ret), KP(sstable));
      } else if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable should not be NULL", K(ret), KP(sstable));
      }
    } else if (OB_FAIL(get_merged_sstable_(
                   type, ctx.param_.index_id_, ctx.merge_context_, ctx.merged_table_handle_, sstable))) {
      LOG_WARN("failed to finish_merge_sstable", K(ret));
    } else if (ctx.param_.is_mini_merge() && OB_FAIL(get_merged_sstable_(ObITable::COMPLEMENT_MINOR_SSTABLE,
                                                 ctx.param_.index_id_,
                                                 ctx.merge_context_for_complement_minor_sstable_,
                                                 ctx.merged_complement_minor_table_handle_,
                                                 complement_minor_sstable))) {
      LOG_WARN("failed to get complement_minor_sstable", K(ret), K(ctx));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx.pg_guard_.get_partition_group()->add_sstable_for_merge(
                   ctx.param_.pkey_, sstable, max_kept_major_version_number, *ctx.report_, complement_minor_sstable))) {
      LOG_WARN("failed to add sstable for merge", K(ret), K(ctx));
    }
    if (OB_SUCC(ret) && ctx.param_.is_major_merge() && !ctx.table_schema_->is_dropped_schema()) {
      storage->get_prefix_access_stat().reset();
      if (OB_FAIL(storage->validate_sstables(ctx.index_stats_, is_all_checked))) {
        LOG_WARN("fail to validate sstables", K(ret));
      } else {
        ObPartitionStorage::check_data_checksum(storage->get_replica_type(),
            pkey,
            *ctx.table_schema_,
            *sstable,
            ctx.tables_handle_.get_tables(),
            *ctx.report_);
        if (OB_SUCCESS != (tmp_ret = ctx.report_->submit_checksum_update_task(ctx.param_.pkey_,
                               ctx.param_.index_id_,
                               ObITable::MAJOR_SSTABLE,
                               observer::ObSSTableChecksumUpdateType::UPDATE_DATA_CHECKSUM))) {
          STORAGE_LOG(ERROR, "fail to submit data checksum update task", K(ret));
        }
      }
    }
  }

  if (NULL != merge_dag_) {
    if (OB_FAIL(ret)) {
      ObTaskController::get().allow_next_syslog();
      storage::ObSSTableMergeCtx& ctx = merge_dag_->get_ctx();
      LOG_WARN(
          "sstable merge finish", K(ret), K(ctx), "task", *(static_cast<ObITask*>(this)), "pg_key", ctx.param_.pg_key_);
      (void)txs->print_all_trans_ctx(ctx.param_.pkey_);
      ATOMIC_VCAS(&merge_dag_->get_ctx().result_code_, OB_SUCCESS, ret);
    } else {
      ObSSTableMergeInfo merge_info =
          OB_NOT_NULL(sstable) ? sstable->get_sstable_merge_info() : complement_minor_sstable->get_sstable_merge_info();
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("sstable merge finish", K(ret), K(merge_info), KPC(sstable), KPC(complement_minor_sstable));
    }
  }

  return ret;
}

int ObSSTableMergeFinishTask::check_macro_cnt_of_merge_table(
    const ObITable::TableType& type, storage::ObSSTableMergeContext& merge_context)
{
  int ret = OB_SUCCESS;
  storage::ObSSTableMergeCtx& ctx = merge_dag_->get_ctx();
  if (ctx.log_ts_range_.end_log_ts_ == ctx.log_ts_range_.max_log_ts_ && ObITable::is_complement_minor_sstable(type)) {
    int64_t macro_block_count = 0;
    if (OB_FAIL(merge_context.get_data_macro_block_count(macro_block_count))) {
      LOG_WARN("Failed to get macro block count from merge context", K(ret), K(merge_context));
    } else if (macro_block_count > 0) {
      // max_log_ts == end_log_ts means that there is no trans node,
      // and there should also be no macro block in complement minor sstable
      ret = OB_ERR_SYS;
      LOG_ERROR("Unexpected empty overflow complement sstable with data blocks",
          K(ret),
          K(type),
          K(macro_block_count),
          K(ctx),
          K(merge_context));
    }
  }
  return ret;
}

int ObSSTableMergeFinishTask::get_merged_sstable_(const ObITable::TableType& type, const uint64_t table_id,
    storage::ObSSTableMergeContext& merge_context, storage::ObTableHandle& table_handle, ObSSTable*& sstable)
{
  int ret = OB_SUCCESS;
  storage::ObSSTableMergeCtx& ctx = merge_dag_->get_ctx();
  const ObTableSchema* table_schema = ctx.table_schema_;
  sstable = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(table_id) || OB_ISNULL(table_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get merged sstable", K(ret), K(table_id), K(ctx));
  } else if (OB_UNLIKELY(table_id != ctx.table_schema_->get_table_id())) {
    LOG_INFO("Need create sstable for aux vp based on main vp's sstable");
    if (ctx.param_.is_major_merge()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Major merge will not create aux vp sstable based on main vp sstable", K(ret));
    } else if (OB_FAIL(ctx.schema_guard_.get_table_schema(table_id, table_schema))) {
      LOG_WARN("Failed to get aux vp table schema", K(ret), K(table_id), KP(table_schema));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Aux vp table schema must not be null", K(ret), K(table_id), KP(table_schema));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_macro_cnt_of_merge_table(type, merge_context))) {
    LOG_ERROR("log_ts_range and macro block count not match", K(ret), K(table_id), KP(table_schema), K(merge_context));
  } else {
    ObPartitionKey& pkey = ctx.param_.pkey_;
    const bool is_index_first_merge = ctx.is_created_index_first_merge_;
    const int64_t max_kept_major_version_number = GCONF.max_kept_major_version_number;
    const int64_t progressive_merge_start_version =
        is_index_first_merge ? ctx.backup_progressive_merge_start_version_ : ctx.progressive_merge_start_version_;
    const int64_t progressive_merge_end_version =
        progressive_merge_start_version +
        (is_index_first_merge ? ctx.backup_progressive_merge_num_ : ctx.progressive_merge_num_);
    const share::schema::ObTableModeFlag table_mode_flag = ctx.table_schema_->get_table_mode_flag();

    LOG_INFO("create new merged sstable",
        "pkey",
        ctx.param_.pkey_,
        K(table_id),
        "snapshot_version",
        ctx.sstable_version_range_.snapshot_version_,
        K(ctx.param_.merge_type_),
        K(ctx.create_snapshot_version_),
        K(ctx.dump_memtable_timestamp_),
        K(is_index_first_merge),
        K(ctx.backup_progressive_merge_num_),
        K(progressive_merge_start_version),
        K(progressive_merge_end_version),
        K(table_mode_flag));

    ObITable::TableKey table_key;
    const bool use_inc_macro_block_slog = true;
    ObCreateSSTableParamWithTable param;

    table_key.table_type_ = type;
    if (ctx.param_.is_major_merge()) {
      table_key.pkey_ = ctx.base_table_handle_.get_table()->get_partition_key();
    } else {
      table_key.pkey_ = ctx.param_.pkey_;
    }
    table_key.table_id_ = table_id;
    table_key.version_ = ctx.param_.merge_version_;
    table_key.trans_version_range_ = ctx.sstable_version_range_;
    // both Mini & Complement SSTable use the same log_ts_range
    table_key.log_ts_range_ = ctx.log_ts_range_;
    param.table_key_ = table_key;
    param.schema_ = table_schema;
    param.schema_version_ = ctx.schema_version_;
    param.progressive_merge_start_version_ = progressive_merge_start_version;
    param.progressive_merge_end_version_ = progressive_merge_end_version;
    param.create_snapshot_version_ = ctx.create_snapshot_version_;
    param.dump_memtable_timestamp_ = ctx.dump_memtable_timestamp_;
    param.checksum_method_ = ctx.checksum_method_;
    param.logical_data_version_ = ctx.logical_data_version_;
    param.has_compact_row_ = true;
    if (ctx.use_new_progressive_) {
      param.progressive_merge_round_ = ctx.progressive_merge_round_;
      param.progressive_merge_step_ =
          std::min(ctx.data_table_schema_->get_progressive_merge_num(), ctx.progressive_merge_step_ + 1);
    }
    merge_context.get_sstable_merge_info().snapshot_version_ = ctx.sstable_version_range_.snapshot_version_;

    if (OB_FAIL(merge_context.create_sstable(param, ctx.pg_guard_, table_handle))) {
      LOG_WARN("fail to create sstable", K(ret), K(ctx.param_), K(table_handle));
    } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
      LOG_WARN("fail to get sstable", K(ret));
    } else {
      FLOG_INFO("succeed to merge sstable",
          K(param),
          "table_key",
          table_handle.get_table()->get_key(),
          "merge_info",
          ctx.merge_context_.get_sstable_merge_info());
    }

    if (OB_SUCC(ret)) {
      if (ctx.stat_sampling_ratio_ > 0 &&
          OB_FAIL(ObPartitionStorage::update_estimator(
              ctx.table_schema_, ctx.is_full_merge_, ctx.column_stats_, sstable, pkey))) {
        STORAGE_LOG(WARN, "failed to update estimator", K(ret), K(pkey));
      }
    }
  }

  return ret;
}

ObWriteCheckpointTask::ObWriteCheckpointTask()
    : ObITask(ObITask::TASK_TYPE_MAJOR_MERGE_FINISH), is_inited_(false), frozen_version_(-1)
{}

ObWriteCheckpointTask::~ObWriteCheckpointTask()
{}

int ObWriteCheckpointTask::init(int64_t frozen_version)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag must not null", K(ret));
  } else if (ObIDag::DAG_TYPE_MAJOR_MERGE_FINISH != dag_->get_type()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    frozen_version_ = frozen_version;
    is_inited_ = true;
  }

  return ret;
}

int ObWriteCheckpointTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  static int64_t last_write_time_ = 0;
  static int64_t checkpoint_version_ = 0;
  static common::ObLogCursor slog_cursor_;

  const int64_t now = ObTimeUtility::current_time();
  int64_t alert_interval = FAIL_WRITE_CHECKPOINT_ALERT_INTERVAL;
  int64_t min_interval = RETRY_WRITE_CHECKPOINT_MIN_INTERVAL;
#ifdef ERRSIM
  alert_interval = GCONF.fail_write_checkpoint_alert_interval;
  min_interval = GCONF.retry_write_checkpoint_min_interval;
  LOG_INFO("use errsim checkpoint config", K(alert_interval), K(min_interval));
#endif
  DEBUG_SYNC(DELAY_WRITE_CHECKPOINT);

  common::ObLogCursor cur_cursor;
  // Get slog cursor every RETRY_WRITE_CHECKPOINT_MIN_INTERVAL
  if (now > last_write_time_ + min_interval || checkpoint_version_ < frozen_version_) {
    if (OB_SUCCESS != (tmp_ret = SLOGGER.get_active_cursor(cur_cursor))) {
      LOG_WARN("get slog current cursor fail", K(tmp_ret));
    }
  } else {
    cur_cursor = slog_cursor_;
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("checkpoint stats",
        K(slog_cursor_),
        K(cur_cursor),
        K(checkpoint_version_),
        K(frozen_version_),
        K(last_write_time_));
  }

  // ignore tmp_ret, cur_cursor will be invalid if previous step failed
  // write checkpoint if 1) just finish major merge
  //                     2) log id increased over MIN_WRITE_CHECKPOINT_LOG_CNT
  if (cur_cursor.is_valid() && ((checkpoint_version_ < frozen_version_) ||
                                   ((cur_cursor.newer_than(slog_cursor_) &&
                                       (cur_cursor.log_id_ - slog_cursor_.log_id_ >= MIN_WRITE_CHECKPOINT_LOG_CNT))))) {
    SERVER_EVENT_ADD("storage",
        "write checkpoint start",
        "tenant_id",
        0,
        "checkpoint_snapshot",
        frozen_version_,
        "checkpoint_type",
        "META_CKPT",
        "checkpoint_cluster_version",
        GET_MIN_CLUSTER_VERSION());
    if (OB_SUCCESS != (tmp_ret = ObServerCheckpointWriter::get_instance().write_checkpoint(cur_cursor))) {
      ObTaskController::get().allow_next_syslog();
      if (0 != last_write_time_ && now > last_write_time_ + alert_interval && OB_EAGAIN != tmp_ret &&
          cur_cursor.log_id_ - slog_cursor_.log_id_ >= MIN_WRITE_CHECKPOINT_LOG_CNT * 2) {
        LOG_ERROR("Fail to write checkpoint in long time",
            K(tmp_ret),
            K(frozen_version_),
            K(last_write_time_),
            K(alert_interval),
            "task",
            *this);
      } else {
        LOG_WARN("Fail to write checkpoint in short time",
            K(tmp_ret),
            K(frozen_version_),
            K(last_write_time_),
            K(alert_interval),
            "task",
            *this);
      }
    } else {
      SERVER_EVENT_ADD("storage",
          "write checkpoint finish",
          "tenant_id",
          0,
          "checkpoint_snapshot",
          frozen_version_,
          "checkpoint_type",
          "META_CKPT",
          "checkpoint_cluster_version",
          GET_MIN_CLUSTER_VERSION());
      checkpoint_version_ = frozen_version_;
      last_write_time_ = now;
      slog_cursor_ = cur_cursor;
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("Success to write checkpoint", K(frozen_version_), K(last_write_time_), K(slog_cursor_), "task", *this);
    }
  }

  return ret;
}

int ObSSTableMergeContext::new_block_write_ctx(blocksstable::ObMacroBlocksWriteCtx*& ctx)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;

  if (OB_NOT_NULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx must not null", K(ret), K(ctx));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(blocksstable::ObMacroBlocksWriteCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObMacroBlocksWriteCtx", K(ret));
  } else {
    ctx = new (buf) blocksstable::ObMacroBlocksWriteCtx();
  }
  return ret;
}

int ObTransTableMergeDag::fill_comment(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "trans table merge task : key=%s", to_cstring(pg_key_)))) {
    LOG_WARN("failed to fill comment", K(ret), K_(pg_key));
  }
  return ret;
}

ObTransTableMergeTask::ObTransTableMergeTask()
    : ObITask(ObITask::TASK_TYPE_MACROMERGE),
      pg_key_(),
      allocator_(ObModIds::OB_CS_MERGER),
      trans_table_pkey_(),
      desc_(),
      end_log_ts_(OB_INVALID_ID),
      tables_handle_(),
      table_schema_(),
      trans_table_seq_(-1),
      is_inited_(false)
{}

// for merge
int ObTransTableMergeTask::init(const ObPartitionKey& pg_key, const int64_t end_log_ts, const int64_t trans_table_seq)
{
  int ret = OB_SUCCESS;
  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(trans_table_pkey_.generate_trans_table_pkey(pg_key))) {
    LOG_WARN("failed to generate trans_table_pkey", K(ret), K_(pg_key));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, pg_guard_))) {
    LOG_WARN("failed to get partition", K(ret), K_(pg_key));
  } else if (OB_ISNULL(pg_guard_.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition group should not be null", K(ret), K(pg_key));
  } else {
    pg_key_ = pg_key;
    end_log_ts_ = end_log_ts;
    trans_table_seq_ = trans_table_seq;
    is_inited_ = true;
  }

  return ret;
}

// for migrate
int ObTransTableMergeTask::init(const ObPartitionKey& pg_key, ObSSTable* sstable)
{
  int ret = OB_SUCCESS;
  storage::ObPGPartitionGuard partition_guard;
  ObIPartitionGroup* pg = nullptr;
  ObSSTable* local_table = nullptr;
  ObTableHandle handle;

  if (!pg_key.is_valid() || OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key), KP(sstable));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable should not be null", K(ret), K(pg_key));
  } else if (OB_FAIL(trans_table_pkey_.generate_trans_table_pkey(pg_key))) {
    LOG_WARN("failed to init pkey", K(ret), K(pg_key));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, pg_guard_))) {
    LOG_WARN("failed to get partition", K(ret), K(pg_key));
  } else if (NULL == (pg = pg_guard_.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The partition must not null", K(ret));
  } else if (OB_FAIL(pg->get_pg_partition(trans_table_pkey_, partition_guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get pg partition", K(ret));
    }
  } else if (OB_FAIL(static_cast<ObPartitionStorage*>(partition_guard.get_pg_partition()->get_storage())
                         ->get_partition_store()
                         .get_last_major_sstable(trans_table_pkey_.get_table_id(), handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get latest sstable", K(ret));
    } else {
      // possible during migrating
      FLOG_INFO("local trans table not exist, may be migrate or add replica", K(pg_key));
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(local_table = static_cast<ObSSTable*>(handle.get_table()))) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to get local trans sstable", K(ret), K(pg_key));
  } else if (local_table->get_end_log_ts() > sstable->get_end_log_ts()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("local trans sstable can not be newer when merge with remote",
        K(ret),
        "local table",
        local_table->get_key(),
        "remote table",
        sstable->get_key());
  } else if (OB_FAIL(tables_handle_.add_table(local_table))) {
    LOG_WARN("failed to add table", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tables_handle_.add_table(sstable))) {
      LOG_WARN("failed to add table", K(ret));
    } else {
      pg_key_ = pg_key;
      end_log_ts_ = sstable->get_end_log_ts();
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTransTableMergeTask::process()
{
  int ret = OB_SUCCESS;
  blocksstable::ObMacroBlockWriter writer;
  ObTableHandle handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("trans table merge task is not inited", K(ret));
  } else if (OB_FAIL(init_schema_and_writer(writer))) {
    LOG_WARN("failed to init merge writer", K(ret), K_(pg_key));
  } else if (OB_FAIL(merge_trans_table(writer))) {
    LOG_WARN("failed to merge partition", K(ret), K_(pg_key));
  } else if (OB_FAIL(get_merged_trans_sstable(handle, writer))) {
    LOG_WARN("failed to get merged trans sstable", K(ret), K_(pg_key));
  } else if (OB_FAIL(update_partition_store(handle))) {
    LOG_WARN("failed to update partitino store", K(ret), K_(trans_table_pkey));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObPartitionScheduler::get_instance().schedule_pg_mini_merge(pg_key_))) {
      LOG_WARN("Failed to schedule pg mini merge", K(tmp_ret));
    }
  }

  return ret;
}

int ObTransTableMergeTask::init_schema_and_writer(blocksstable::ObMacroBlockWriter& writer)
{
  int ret = OB_SUCCESS;
  const int64_t version = 0;
  const ObMultiVersionRowInfo* multi_version_row_info = NULL;
  const int64_t partition_id = trans_table_pkey_.get_partition_id();
  const bool is_major_merge = false;
  const bool need_calc_column_checksum = false;
  const bool store_micro_block_column_checksum = false;
  const int64_t snapshot_version = 1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("trans table merge task is not inited", K(ret));
  } else if (OB_FAIL(table_schema_.generate_kv_schema(
                 trans_table_pkey_.get_tenant_id(), trans_table_pkey_.get_table_id()))) {
    LOG_WARN("Fail to init table schema", K(ret), K_(pg_key));
  } else if (tables_handle_.get_count() != 1) {
    if (OB_FAIL(desc_.init(table_schema_,
            version,
            multi_version_row_info,
            partition_id,
            is_major_merge ? MAJOR_MERGE : MINOR_MERGE,
            need_calc_column_checksum,
            store_micro_block_column_checksum,
            trans_table_pkey_,
            pg_guard_.get_partition_group()->get_storage_file_handle(),
            snapshot_version,
            false /*need_check_order*/))) {
      LOG_WARN("Fail to init data store desc", K(ret));
    } else if (OB_FAIL(writer.open(desc_, 0 /*start_seq*/))) {
      LOG_WARN("Fail to open writer", K(ret));
    }
  }

  return ret;
}

int ObTransTableMergeTask::merge_trans_table(blocksstable::ObMacroBlockWriter& writer)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService* txs = ObPartitionService::get_instance().get_trans_service();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("trans table merge task is not inited", K(ret));
  } else if (0 == tables_handle_.get_count()) {
    if (OB_FAIL(txs->iterate_trans_table(pg_key_, end_log_ts_, writer))) {
      LOG_WARN("failed to iterate trans table", K(ret), K_(pg_key));
    }
  } else if (2 == tables_handle_.get_count()) {
    if (OB_FAIL(merge_remote_with_local(writer))) {
      LOG_WARN("failed to merge remote with local", K(ret), K_(pg_key));
    }
  } else if (1 == tables_handle_.get_count()) {
    // possible during migrating
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should only be 0 or 2 tables", K(ret), K_(pg_key));
  }

  if (OB_SUCC(ret)) {
    if (1 != tables_handle_.get_count() && OB_FAIL(writer.close())) {
      LOG_WARN("Fail to close writer", K(ret), K_(pg_key));
    } else {
      FLOG_INFO("succeed to merge trans table", K_(pg_key), K_(end_log_ts), K_(tables_handle));
    }
  }

  return ret;
}

int ObTransTableMergeTask::get_merged_trans_sstable(ObTableHandle& table_handle, ObMacroBlockWriter& writer)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("trans table merge task is not inited", K(ret));
  } else {
    ObCreateSSTableParamWithTable param;
    ObITable::TableKey table_key;
    ObSSTable* sstable = nullptr;

    table_key.table_type_ = ObITable::TableType::TRANS_SSTABLE;
    table_key.table_id_ = trans_table_pkey_.get_table_id();
    table_key.trans_version_range_.multi_version_start_ = 0;
    table_key.trans_version_range_.base_version_ = 0;
    table_key.trans_version_range_.snapshot_version_ = ObTimeUtility::current_time();
    table_key.log_ts_range_.start_log_ts_ = 0;
    table_key.log_ts_range_.end_log_ts_ = end_log_ts_;
    table_key.log_ts_range_.max_log_ts_ = table_key.log_ts_range_.end_log_ts_;
    table_key.pkey_ = trans_table_pkey_;

    param.table_key_ = table_key;
    param.schema_ = &table_schema_;
    param.schema_version_ = SCHEMA_VERSION;
    param.logical_data_version_ = table_key.version_ + 1;
    ObPGCreateSSTableParam pg_create_sstable_param;
    pg_create_sstable_param.with_table_param_ = &param;
    ObArray<ObMacroBlocksWriteCtx*>& data_blocks = pg_create_sstable_param.data_blocks_;
    if (1 == tables_handle_.get_count()) {
      if (OB_FAIL(tables_handle_.get_first_sstable(sstable))) {
        LOG_WARN("failed to get fist sstable", K(ret), K_(pg_key), K(table_key));
      } else if (OB_FAIL(table_handle.set_table(sstable))) {
        LOG_WARN("failed to set table", K(ret), K_(pg_key), K(table_key));
      }
    } else if (OB_FAIL(data_blocks.push_back(&writer.get_macro_block_write_ctx()))) {
      LOG_WARN("fail to push back data block", K(ret));
    } else if (OB_FAIL(pg_guard_.get_partition_group()->create_sstable(pg_create_sstable_param, table_handle))) {
      LOG_WARN("fail to create sstable", K(ret));
    }
  }
  return ret;
}

int ObTransTableMergeTask::update_partition_store(ObTableHandle& table_handle)
{
  int ret = OB_SUCCESS;
  const int64_t max_kept_major_number = 1;
  ObSSTable* sstable = nullptr;
  if (0 > trans_table_seq_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans table seq should not be negative", K(ret), K_(trans_table_seq));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K_(pg_key));
  } else if (OB_FAIL(pg_guard_.get_partition_group()->get_pg_storage().add_trans_sstable(trans_table_seq_, sstable))) {
    LOG_WARN("fail to add sstable for merge", K(ret), K(pg_key_), K(trans_table_pkey_));
  }

  return ret;
}

int ObTransTableMergeTask::merge_remote_with_local(blocksstable::ObMacroBlockWriter& writer)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* local_row_iter = NULL;
  ObStoreRowIterator* remote_row_iter = NULL;

  ObExtStoreRange whole_range;
  whole_range.get_range().set_whole_range();

  ObStoreCtx ctx;
  ObTableIterParam iter_param;
  ObTableAccessContext access_context;
  common::ObVersionRange trans_version_range;
  common::ObQueryFlag query_flag;
  query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;

  // TODO hard code temporarily
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = common::ObVersionRange::MAX_VERSION - 2;

  common::ObSEArray<share::schema::ObColDesc, 2> columns;
  share::schema::ObColDesc key;
  key.col_id_ = OB_APP_MIN_COLUMN_ID;
  key.col_type_.set_binary();
  key.col_order_ = ObOrderType::ASC;

  share::schema::ObColDesc value;
  value.col_id_ = OB_APP_MIN_COLUMN_ID + 1;
  value.col_type_.set_binary();

  if (2 != tables_handle_.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "table count should be 2", K(ret), K_(trans_table_pkey));
  } else if (OB_FAIL(access_context.init(query_flag, ctx, allocator_, trans_version_range))) {
    STORAGE_LOG(WARN, "failed to init access context", K(ret));
  } else if (OB_FAIL(columns.push_back(key))) {
    STORAGE_LOG(WARN, "failed to push back key", K(ret), K(key), K_(trans_table_pkey));
  } else if (OB_FAIL(columns.push_back(value))) {
    STORAGE_LOG(WARN, "failed to push back value", K(ret), K(value), K_(trans_table_pkey));
  } else if (OB_FAIL(whole_range.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
    STORAGE_LOG(WARN, "failed to transform range to collation free and range cutoff", K(whole_range), K(ret));
  } else {
    iter_param.table_id_ = trans_table_pkey_.get_table_id();
    iter_param.schema_version_ = 0;
    iter_param.rowkey_cnt_ = 1;
    iter_param.out_cols_ = &columns;

    ObITable* local_table = tables_handle_.get_table(0);
    ObITable* remote_table = tables_handle_.get_table(1);

    const ObStoreRow* local_row = NULL;
    const ObStoreRow* remote_row = NULL;
    const ObStoreRow* target_row = NULL;

    bool need_next_local = true;
    bool need_next_remote = true;

    int ret1 = OB_SUCCESS;
    int ret2 = OB_SUCCESS;

    int64_t pos = 0;
    int64_t pos1 = 0;

    SOURCE source = SOURCE::NONE;
    int64_t replace_local_cnt = 0;
    int64_t remote_cnt = 0;
    int64_t local_cnt = 0;
    int64_t delete_cnt = 0;

    if (OB_ISNULL(local_table) || OB_ISNULL(remote_table)) {
      ret = OB_ERR_SYS;
      LOG_WARN("table should not be null", K(ret), KP(local_table), KP(remote_table));
    } else if (OB_FAIL(local_table->scan(iter_param, access_context, whole_range, local_row_iter))) {
      LOG_WARN("failed to scan local table", K(ret), K_(pg_key));
    } else if (OB_FAIL(remote_table->scan(iter_param, access_context, whole_range, remote_row_iter))) {
      LOG_WARN("failed to scan remote table", K(ret), K_(pg_key));
    }

    while (OB_SUCC(ret)) {
      transaction::ObTransID local_trans_id;
      transaction::ObTransID remote_trans_id;

      if (need_next_local) {
        ret1 = local_row_iter->get_next_row(local_row);
      }
      if (need_next_remote) {
        ret2 = remote_row_iter->get_next_row(remote_row);
      }

      if (OB_SUCCESS != ret1 && OB_ITER_END != ret1) {
        ret = ret1;
        STORAGE_LOG(WARN, "failed to get next row", K(ret), K_(trans_table_pkey));
        break;
      }

      if (OB_SUCCESS != ret2 && OB_ITER_END != ret2) {
        ret = ret2;
        STORAGE_LOG(WARN, "failed to get next row", K(ret), K_(trans_table_pkey));
        break;
      }

      if (OB_SUCCESS == ret1 && OB_SUCCESS == ret2) {
        pos = 0;
        pos1 = 0;
        if (OB_FAIL(local_trans_id.deserialize(
                local_row->row_val_.cells_[0].get_string_ptr(), local_row->row_val_.cells_[0].val_len_, pos))) {
          STORAGE_LOG(WARN, "failed to deserialize trans_id", K(ret), K(*local_row));
        } else if (OB_FAIL(remote_trans_id.deserialize(remote_row->row_val_.cells_[0].get_string_ptr(),
                       remote_row->row_val_.cells_[0].val_len_,
                       pos1))) {
          STORAGE_LOG(WARN, "failed to deserialize trans_id", K(ret), K(*remote_row));
        } else {
          ObTransKey local_trans_key(pg_key_, local_trans_id);
          ObTransKey remote_trans_key(pg_key_, remote_trans_id);

          int cmp = local_trans_key.compare_trans_table(remote_trans_key);
          // use remote first
          if (0 == cmp) {
            target_row = remote_row;
            need_next_local = true;
            need_next_remote = true;
            source = SOURCE::FROM_BOTH;
          } else if (cmp < 0) {
            target_row = local_row;
            need_next_local = true;
            need_next_remote = false;
            source = SOURCE::FROM_LOCAL;
          } else if (cmp > 0) {
            target_row = remote_row;
            need_next_local = false;
            need_next_remote = true;
            source = SOURCE::FROM_REMOTE;
          }
        }
      } else if (OB_SUCCESS == ret1) {
        source = SOURCE::FROM_LOCAL;
        target_row = local_row;
        need_next_local = true;
        need_next_remote = false;
      } else if (OB_SUCCESS == ret2) {
        source = SOURCE::FROM_REMOTE;
        target_row = remote_row;
        need_next_remote = true;
        need_next_local = false;
      } else {
        break;
      }

      if (OB_SUCC(ret)) {
        switch (source) {
          case SOURCE::FROM_BOTH:
            replace_local_cnt++;
            break;
          case SOURCE::FROM_REMOTE:
            remote_cnt++;
            break;
          case SOURCE::FROM_LOCAL: {
            transaction::ObTransSSTableDurableCtxInfo status_info;
            pos = 0;
            if (OB_FAIL(status_info.deserialize(
                    local_row->row_val_.cells_[1].v_.string_, local_row->row_val_.cells_[1].val_len_, pos))) {
              STORAGE_LOG(WARN, "failed to deserialize status_info", K(ret), K(status_info));
            } else {
              // only keep trans status of terminated transactions in FROM_LOCAL
              if (!status_info.trans_table_info_.is_terminated()) {
                delete_cnt++;
                target_row = NULL;
                FLOG_INFO("remove local trans status", K(status_info));
              } else {
                local_cnt++;
              }
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (NULL != target_row) {
          if (OB_FAIL(writer.append_row(*target_row))) {
            STORAGE_LOG(WARN, "failed to append row", K(ret), K(*target_row));
          }
        }
      }
    }
    FLOG_INFO("finish merge local and remote trans table",
        K(ret),
        K(replace_local_cnt),
        K(remote_cnt),
        K(local_cnt),
        K(delete_cnt));
  }

  if (NULL != local_row_iter) {
    local_row_iter->~ObStoreRowIterator();
  }

  if (NULL != remote_row_iter) {
    remote_row_iter->~ObStoreRowIterator();
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
