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
#include "ob_partition_merger.h"
#include "lib/file/file_directory_utils.h"
#include "logservice/ob_log_service.h"
#include "ob_tenant_tablet_scheduler.h"
#include "ob_tablet_merge_task.h"
#include "ob_tablet_merge_ctx.h"
#include "ob_i_compaction_filter.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/blocksstable/ob_data_macro_block_merge_writer.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/column_store/ob_column_oriented_sstable.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace blocksstable;

namespace compaction
{

const char * ObMacroBlockOp::block_op_str_[] = {
    "BLOCK_OP_NONE",
    "BLOCK_OP_REORG",
    "BLOCK_OP_REWRITE"
};

const char* ObMacroBlockOp::get_block_op_str() const
{
  STATIC_ASSERT(static_cast<int64_t>(OP_REWRITE) + 1 == ARRAYSIZEOF(block_op_str_), "block op array is mismatch");
  return is_valid() ? block_op_str_[block_op_] : "OP_INVALID";
}

/*
 *ObDataDescHelper
 */
int ObDataDescHelper::build(
    const ObMergeParameter &merge_param,
    ObTabletMergeInfo &input_merge_info,
    blocksstable::ObDataStoreDesc &data_store_desc,
    ObSSTableMergeInfo &output_merge_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_store_desc.shallow_copy(input_merge_info.get_sstable_build_desc().get_desc()))) {
    STORAGE_LOG(WARN, "failed to init data desc", K(ret), K(merge_param));
  } else {
    // init merge info
    const ObStaticMergeParam &static_param = merge_param.static_param_;
    output_merge_info.reset();
    output_merge_info.tenant_id_ = MTL_ID();
    output_merge_info.ls_id_ = static_param.get_ls_id();
    output_merge_info.tablet_id_ = static_param.get_tablet_id();
    output_merge_info.merge_type_ = static_param.get_merge_type();
    output_merge_info.compaction_scn_ = static_param.get_compaction_scn();
    output_merge_info.progressive_merge_round_ = static_param.progressive_merge_round_;
    output_merge_info.progressive_merge_num_ = static_param.progressive_merge_num_;
    output_merge_info.concurrent_cnt_ = static_param.concurrent_cnt_;
    output_merge_info.is_full_merge_ = static_param.is_full_merge_;
    // init desc input data_store_desc
    data_store_desc.sstable_index_builder_ = input_merge_info.get_index_builder();
    data_store_desc.merge_info_ = &output_merge_info;
  }
  return ret;
}

/*
 *ObProgressiveMergeHelper
 */

void ObProgressiveMergeHelper::reset()
{
  progressive_merge_round_ = 0;
  rewrite_block_cnt_ = 0;
  need_rewrite_block_cnt_ = 0;
  data_version_ = 0;
  full_merge_ = false;
  check_macro_need_merge_ = false;
  is_inited_ = false;
}

int ObProgressiveMergeHelper::init(const ObSSTable &sstable, const ObMergeParameter &merge_param, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObStaticMergeParam &static_param = merge_param.static_param_;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObProgressiveMergeHelper init twice", K(ret));
  } else if (FALSE_IT(reset())) {
  } else if (static_param.is_full_merge_) {
    full_merge_ = check_macro_need_merge_ = true;
  } else {
    int64_t rewrite_macro_cnt = 0, reduce_macro_cnt = 0, rewrite_block_cnt_for_progressive = 0;
    bool last_is_small_data_macro = false;
    const bool is_major = is_major_merge_type(static_param.get_merge_type());
    const bool need_calc_progressive_merge = is_major && static_param.progressive_merge_step_ < static_param.progressive_merge_num_;

    progressive_merge_round_ = static_param.progressive_merge_round_;

    ObSSTableSecMetaIterator *sec_meta_iter = nullptr;
    ObDataMacroBlockMeta macro_meta;
    const storage::ObITableReadInfo *index_read_info = nullptr;
    if (sstable.is_normal_cg_sstable()) {
      if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
        STORAGE_LOG(WARN, "failed to get index read info from ObTenantCGReadInfoMgr", KR(ret));
      }
    } else {
      index_read_info = static_param.rowkey_read_info_;
    }
    const ObDatumRange &merge_range = sstable.is_normal_cg_sstable() ? merge_param.merge_rowid_range_ : merge_param.merge_range_;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(index_read_info)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "index read info is unexpected null", KR(ret), KP(index_read_info), K(sstable), K(merge_param));
    } else if (OB_FAIL(sstable.scan_secondary_meta(
            allocator,
            merge_range,
            *index_read_info,
            blocksstable::DATA_BLOCK_META,
            sec_meta_iter))) {
      STORAGE_LOG(WARN, "Fail to scan secondary meta", K(ret), K(merge_range));
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(sec_meta_iter->get_next(macro_meta))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Failed to get next macro block", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (macro_meta.val_.progressive_merge_round_ < progressive_merge_round_) {
        ++rewrite_block_cnt_for_progressive;
      }
      if (macro_meta.val_.data_zsize_ < OB_DEFAULT_MACRO_BLOCK_SIZE *
          ObMacroBlockWriter::DEFAULT_MACRO_BLOCK_REWRTIE_THRESHOLD / 100) {
        rewrite_macro_cnt++;
        if (last_is_small_data_macro) {
          reduce_macro_cnt++;
        }
        last_is_small_data_macro = true;
      } else {
        last_is_small_data_macro = false;
      }
    }
    if (OB_NOT_NULL(sec_meta_iter)) {
      sec_meta_iter->~ObSSTableSecMetaIterator();
      allocator.free(sec_meta_iter);
    }
    if (OB_SUCC(ret)) {
      if (need_calc_progressive_merge) {
        need_rewrite_block_cnt_ = MAX(rewrite_block_cnt_for_progressive /
            (static_param.progressive_merge_num_ - static_param.progressive_merge_step_), 1L);
        STORAGE_LOG(INFO, "There are some macro block need rewrite", "tablet_id", static_param.get_tablet_id(),
            K(need_rewrite_block_cnt_), K(static_param.progressive_merge_step_),
            K(static_param.progressive_merge_num_), K(progressive_merge_round_), K(table_idx_));
      }
      check_macro_need_merge_ = rewrite_macro_cnt <= (reduce_macro_cnt * 2);
      if (static_param.data_version_ < DATA_VERSION_4_3_2_0
          && sstable.is_normal_cg_sstable() && rewrite_macro_cnt < CG_TABLE_CHECK_REWRITE_CNT_) {
        check_macro_need_merge_ = true;
      }
      STORAGE_LOG(INFO, "finish macro block need merge check", K(check_macro_need_merge_), K(rewrite_macro_cnt), K(reduce_macro_cnt), K(table_idx_));
    }

    if (OB_SUCC(ret)) {
      data_version_ = static_param.data_version_;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObProgressiveMergeHelper::check_macro_block_op(const ObMacroBlockDesc &macro_desc,
                                                   ObMacroBlockOp &block_op) const
{
  int ret = OB_SUCCESS;

  block_op.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObProgressiveMergeHelper not init", K(ret));
  } else if (!macro_desc.is_valid_with_macro_meta()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro desc", K(ret), K(macro_desc));
  } else if (full_merge_) {
    block_op.set_rewrite();
  } else {
    if (need_rewrite_block_cnt_ > 0) {
      const int64_t block_merge_round = macro_desc.macro_meta_->val_.progressive_merge_round_;
      if(need_rewrite_block_cnt_ > rewrite_block_cnt_ && block_merge_round < progressive_merge_round_) {
        block_op.set_rewrite();
      }
    }
    if (block_op.is_none()) {
      if (!check_macro_need_merge_) {
      } else if (macro_desc.macro_meta_->val_.data_zsize_
          < OB_SERVER_BLOCK_MGR.get_macro_block_size() * DEFAULT_MACRO_BLOCK_REWRTIE_THRESHOLD / 100) {
        // before 432 we need rewrite theis macro block
        if (data_version_ < DATA_VERSION_4_3_2_0) {
          block_op.set_rewrite();
        } else {
          block_op.set_reorg();
        }
      }
    }
  }

  return ret;

}

/*
 *ObMerger
 */
ObMerger::ObMerger(
  compaction::ObLocalArena &allocator,
  const ObStaticMergeParam &static_param)
  : merger_arena_(allocator),
    merge_ctx_(nullptr),
    task_idx_(0),
    force_flat_format_(false),
    merge_param_(static_param),
    partition_fuser_(nullptr),
    merge_helper_(nullptr),
    base_iter_(nullptr),
    trans_state_mgr_(merger_arena_)
{
}

void ObMerger::reset()
{
  if (OB_NOT_NULL(merge_helper_)) {
    merge_helper_->~ObPartitionMergeHelper();
    merger_arena_.free(merge_helper_);
    merge_helper_ = nullptr;
  }

  if (OB_NOT_NULL(partition_fuser_)) {
    partition_fuser_->~ObIPartitionMergeFuser();
    merger_arena_.free(partition_fuser_);
    partition_fuser_ = nullptr;
  }

  trans_state_mgr_.destroy();
  base_iter_ = nullptr;
  merge_param_.reset();
  force_flat_format_ = false;
  task_idx_ = 0;
  merge_ctx_ = nullptr;
}

int ObMerger::prepare_merge(ObBasicTabletMergeCtx &ctx, const int64_t idx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ctx.is_valid() || idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObMerger", K(ret), K(ctx), K(idx));
  } else {
    merge_ctx_ = &ctx;
    task_idx_ = idx;

    if (OB_FAIL(merge_param_.init(ctx, task_idx_, &merger_arena_))) {
      STORAGE_LOG(WARN, "Failed to assign the merge param", K(ret), KPC(merge_ctx_), K_(task_idx));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans_state_mgr_.init(CACHED_TRANS_STATE_MAX_CNT))) {
        STORAGE_LOG(WARN, "failed to init merge trans state mgr", K(tmp_ret));
      } else {
        merge_param_.trans_state_mgr_ = &trans_state_mgr_;
      }
      if (OB_FAIL(ObMergeFuserBuilder::build(merge_param_, ctx.static_desc_.major_working_cluster_version_ ,merger_arena_, partition_fuser_))) {
        STORAGE_LOG(WARN, "failed to build partition fuser", K(ret), K(merge_param_));
      } else if (OB_FAIL(inner_prepare_merge(ctx, idx))) {
        STORAGE_LOG(WARN, "failed to inner prepare merge", K(ret), K(ctx));
        CTX_SET_DIAGNOSE_LOCATION(ctx);
      }
    }
  }

  return ret;
}

int ObMerger::get_base_iter_curr_macro_block(const blocksstable::ObMacroBlockDesc *&macro_desc)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(base_iter_) || base_iter_->is_iter_end()) {
    macro_desc = nullptr;
  } else if (OB_FAIL(base_iter_->get_curr_macro_block(macro_desc))) {
    STORAGE_LOG(WARN, "Failed to get curr macro block", K(ret), KPC(base_iter_));
  } else if (OB_ISNULL(macro_desc) || OB_UNLIKELY(!macro_desc->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid macro block descriptor", K(ret), KPC(macro_desc), KPC(base_iter_));
  }

  return ret;
}


/*
 *ObPartitionMerger
 */

ObPartitionMerger::ObPartitionMerger(
  compaction::ObLocalArena &allocator,
  const ObStaticMergeParam &static_param)
  : ObMerger(allocator, static_param),
    merge_progress_(nullptr),
    data_store_desc_(),
    merge_info_(),
    macro_writer_(nullptr),
    minimum_iters_(DEFAULT_ITER_ARRAY_SIZE, ModulePageAllocator(allocator)),
    progressive_merge_helper_()
{
}

ObPartitionMerger::~ObPartitionMerger()
{
  reset();
}

void ObPartitionMerger::reset()
{
  minimum_iters_.reset();
  merge_info_.reset();
  data_store_desc_.reset();
  merge_progress_ = nullptr;
  if (OB_NOT_NULL(macro_writer_)) {
    macro_writer_->~ObMacroBlockWriter();
    merger_arena_.free(macro_writer_);
    macro_writer_ = nullptr;
  }
  progressive_merge_helper_.reset();
  ObMerger::reset();
}

int ObPartitionMerger::inner_prepare_merge(ObBasicTabletMergeCtx &ctx, const int64_t idx)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObDataDescHelper::build(merge_param_, static_cast<ObTabletMergeCtx *>(merge_ctx_)->get_merge_info(),
                                      data_store_desc_, merge_info_))) {
    STORAGE_LOG(WARN, "Failed to init data store desc", K(ret));
  } else if (OB_UNLIKELY(!merge_param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid merge param", K(ret), K(merge_param_));
  } else if (OB_FAIL(open_macro_writer(merge_param_))) {
    STORAGE_LOG(WARN, "Failed to open macro writer", K(ret), K(merge_param_));
  } else if (OB_FAIL(inner_init())) {
    STORAGE_LOG(WARN, "Failed to inner init", K(ret));
  } else {
    merge_progress_ = ctx.info_collector_.merge_progress_;
  }

  return ret;
}


int ObPartitionMerger::open_macro_writer(ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  ObMacroDataSeq macro_start_seq(0);
  const ObStaticMergeParam &static_param = merge_ctx_->static_param_;
  if (OB_NOT_NULL(macro_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro_writer_ is not null", K(ret), KPC(macro_writer_));
  } else if (OB_UNLIKELY(!merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid merge parameter", K(ret), K(merge_param));
  } else if (OB_ISNULL(table = static_param.tables_handle_.get_table(0))) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "sstable is null", K(ret));
  } else if (OB_FAIL(macro_start_seq.set_parallel_degree(task_idx_))) {
    STORAGE_LOG(WARN, "Failed to set parallel degree to macro start seq", K(ret), K_(task_idx));
  } else if (OB_FAIL(macro_start_seq.set_sstable_seq(static_param.sstable_logic_seq_))) {
    STORAGE_LOG(WARN, "failed to set sstable seq", K(ret), K(static_param.sstable_logic_seq_));
  } else {
    if (force_flat_format_) {
      data_store_desc_.force_flat_store_type();//temp code
    }
    if ((data_store_desc_.is_major_merge_type() && data_store_desc_.get_major_working_cluster_version() <= DATA_VERSION_4_0_0_0)
      || !data_store_desc_.is_use_pct_free()) {
      const bool is_use_macro_writer_buffer = true;
      macro_writer_ = alloc_helper<ObMacroBlockWriter>(merger_arena_, is_use_macro_writer_buffer);
    } else {
      macro_writer_ = alloc_helper<ObDataMacroBlockMergeWriter>(merger_arena_);
    }

    if (OB_ISNULL(macro_writer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory for macro writer", K(ret), K(merge_param));
    } else if (OB_FAIL(macro_writer_->open(data_store_desc_, macro_start_seq))) {
      STORAGE_LOG(WARN, "Failed to open macro block writer", K(ret));
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(macro_writer_)) {
        macro_writer_->~ObMacroBlockWriter();
        macro_writer_ = nullptr;
      }
    }
  }

  return ret;
}

int ObPartitionMerger::close()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(macro_writer_->close())) {
    STORAGE_LOG(WARN, "Failed to close macro block writer", K(ret));
  } else {
    ObTabletMergeInfo &merge_info = static_cast<ObTabletMergeCtx *>(merge_ctx_)->get_merge_info();
    merge_info_.merge_finish_time_ = common::ObTimeUtility::fast_current_time();
    if (OB_FAIL(merge_info.add_macro_blocks(merge_info_))) {
      STORAGE_LOG(WARN, "Failed to add macro blocks", K(ret));
    } else {
      merge_info_.dump_info("macro block builder close");
    }
  }

  return ret;
}

int ObPartitionMerger::check_row_columns(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (row.row_flag_.is_not_exist() || row.row_flag_.is_delete()) {
  } else if (OB_UNLIKELY(row.count_ != data_store_desc_.get_row_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "Unexpected column count of store row", K(row), K_(data_store_desc), K(ret));
  }
  return ret;
}

int ObPartitionMerger::process(const ObMacroBlockDesc &macro_desc)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  int64_t macro_block_builder_errsim_flag = GCONF.macro_block_builder_errsim_flag;
  if (2 == macro_block_builder_errsim_flag) {
    if (macro_writer_->get_macro_block_write_ctx().get_macro_block_count() -
        merge_info_.multiplexed_macro_block_count_ >= 1) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "fake macro_block_builder_errsim_flag", K(ret),
                  K(macro_block_builder_errsim_flag));
    }
  }
#endif

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(!macro_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to append macro block", K(ret), K(macro_desc));
  } else if (OB_FAIL(macro_writer_->append_macro_block(macro_desc))) {
    LOG_WARN("Failed to append to macro block writer", K(ret));
  } else {
    LOG_DEBUG("Success to append macro block", K(ret), K(macro_desc));
  }
  return ret;
}

int ObPartitionMerger::process(const ObMicroBlock &micro_block)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObMacroBlockDesc *macro_desc;
  if (OB_FAIL(get_base_iter_curr_macro_block(macro_desc))) {
    STORAGE_LOG(WARN, "Failed to get base iter macro", K(ret));
  } else if (!micro_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument to append micro block", K(ret), K(micro_block));
  } else if (OB_FAIL(macro_writer_->append_micro_block(micro_block, macro_desc))) {
    STORAGE_LOG(WARN, "Failed to append micro block to macro block writer", K(ret), K(micro_block));
  } else {
    LOG_DEBUG("append micro block", K(ret), K(micro_block));
  }

  return ret;
}

int ObPartitionMerger::process(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObICompactionFilter::ObFilterRet filter_ret = ObICompactionFilter::FILTER_RET_MAX;
#ifdef ERRSIM
  int64_t macro_block_builder_errsim_flag = GCONF.macro_block_builder_errsim_flag;
  if (1 == macro_block_builder_errsim_flag) {
    if (macro_writer_->get_macro_block_write_ctx().get_macro_block_count() > 1) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "fake macro_block_builder_errsim_flag", K(ret),
                  K(macro_block_builder_errsim_flag));
    }
  }
#endif

  if (OB_FAIL(ret)) {
    // fake errsim
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to append row", K(ret), K(row));
  } else if (OB_UNLIKELY(row.row_flag_.is_not_exist())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "Unexpected not exist row to append", K(ret), K(row));
  } else if (OB_FAIL(try_filter_row(row, filter_ret))) {
    STORAGE_LOG(WARN, "failed to filter row", K(ret), K(row));
  } else if (ObICompactionFilter::FILTER_RET_REMOVE == filter_ret) {
    // drop this row
  } else if (OB_FAIL(check_row_columns(row))) {
    STORAGE_LOG(WARN, "Failed to check row columns", K(ret), K(row));
  } else if (OB_FAIL(inner_process(row))) {
    STORAGE_LOG(WARN, "Failed to inner append row", K(ret));
  } else {
    LOG_DEBUG("append row", K(ret), K(row));
  }
  return ret;
}

int ObPartitionMerger::merge_macro_block_iter(MERGE_ITER_ARRAY &minimum_iters, int64_t &reuse_row_cnt)
{
  int ret = OB_SUCCESS;
  bool rewrite = false;

  ObPartitionMergeIter *iter = nullptr;
  if (OB_UNLIKELY(minimum_iters.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected minimum iters to rewrite macro block", K(ret), K(minimum_iters));
  } else if (OB_ISNULL(iter = minimum_iters.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null iter", K(ret));
  } else if (iter->is_macro_block_opened()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter macro_block_opened", K(ret), KPC(iter));
  } else {
    const ObMacroBlockDesc *macro_desc = nullptr;
    ObMacroBlockOp block_op;
    if (OB_FAIL(iter->get_curr_macro_block(macro_desc))) {
      STORAGE_LOG(WARN, "Failed to get current micro block", K(ret), KPC(iter));
    } else if (OB_ISNULL(macro_desc) || OB_UNLIKELY(!macro_desc->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null macro block", K(ret), KPC(macro_desc), KPC(iter));
    } else if (OB_FAIL(check_macro_block_op(*macro_desc, block_op))) {
      STORAGE_LOG(WARN, "Failed to try_rewrite_macro_block", K(ret));
    } else if (block_op.is_rewrite()) {
      if (OB_FAIL(rewrite_macro_block(minimum_iters))) {
        STORAGE_LOG(WARN, "Failed to rewrite macro block", K(ret));
      }
    } else if (block_op.is_reorg()) {
      if (OB_FAIL(iter->open_curr_range(false /* rewrite */))) {
        STORAGE_LOG(WARN, "Failed to open_curr_range", K(ret));
      }
    } else if (OB_FAIL(process(*macro_desc))) {
      STORAGE_LOG(WARN, "Failed to append macro block", K(ret));
    } else if (FALSE_IT(reuse_row_cnt += macro_desc->row_count_)) {
    } else if (OB_FAIL(iter->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "Failed to get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionMerger::check_macro_block_op(const ObMacroBlockDesc &macro_desc, ObMacroBlockOp &block_op)
{
  int ret = OB_SUCCESS;

  block_op.reset();
  if (!progressive_merge_helper_.is_valid()) {
  } else if (OB_FAIL(progressive_merge_helper_.check_macro_block_op(macro_desc, block_op))) {
    STORAGE_LOG(WARN, "failed to check macro operation", K(ret), K(macro_desc));
  } else if (block_op.is_rewrite()) {
    progressive_merge_helper_.inc_rewrite_block_cnt();
  }

  return ret;
}

int ObPartitionMerger::try_filter_row(
    const ObDatumRow &row,
    ObICompactionFilter::ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  if (merge_ctx_->has_filter()) {
    if (OB_FAIL(merge_ctx_->filter(
        row,
        filter_ret))) {
      STORAGE_LOG(WARN, "failed to filter row", K(ret), K(filter_ret));
    } else if (OB_UNLIKELY(filter_ret >= ObICompactionFilter::FILTER_RET_MAX
        || filter_ret < ObICompactionFilter::FILTER_RET_NOT_CHANGE)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get wrong filter ret", K(filter_ret));
    } else {
      merge_info_.filter_statistics_.inc(filter_ret);
    }
  }
  return ret;
}

/*
 *ObPartitionMajorMerger
 */
ObPartitionMajorMerger::ObPartitionMajorMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param)
  : ObPartitionMerger(allocator, static_param)
{
}

ObPartitionMajorMerger::~ObPartitionMajorMerger()
{
  reset();
}

int ObPartitionMajorMerger::inner_init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_progressive_merge_helper())) {
    STORAGE_LOG(WARN, "Failed to init progressive_merge_helper", K(ret));
  } else {
    merge_helper_ = OB_NEWx(ObPartitionMajorMergeHelper, (&merger_arena_), merge_ctx_->read_info_, merger_arena_);

    if (OB_ISNULL(merge_helper_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory for partition helper", K(ret));
    } else if (OB_FAIL(merge_helper_->init(merge_param_))) {
      STORAGE_LOG(WARN, "Failed to init merge helper", K(ret));
    }
  }

  return ret;
}

int ObPartitionMajorMerger::inner_process(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const bool is_delete = row.row_flag_.is_delete();
  if (is_delete) {
      // drop del row
  } else {
    const blocksstable::ObMacroBlockDesc *macro_desc;
    if (OB_FAIL(get_base_iter_curr_macro_block(macro_desc))) {
      STORAGE_LOG(WARN, "Failed to get base iter macro", K(ret));
    } else if (OB_FAIL(macro_writer_->append_row(row, macro_desc))) {
      STORAGE_LOG(WARN, "Failed to append row to macro writer", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(DEBUG, "Success to virtual append row to major macro writer", K(ret), K(row));
  }
  return ret;
}

int ObPartitionMajorMerger::merge_partition(
    ObBasicTabletMergeCtx &ctx,
    const int64_t idx)
{
  int ret = OB_SUCCESS;
  SET_MEM_CTX(ctx.mem_ctx_);

  if (OB_FAIL(prepare_merge(ctx, idx))) {
    STORAGE_LOG(WARN, "Failed to prepare merge partition", K(ret), K(ctx), K(idx));
  } else if (OB_ISNULL(partition_fuser_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null partition fuser", K(ret));
  } else {
    bool has_incremental_data = false;
    if (merge_helper_->is_iter_end()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(merge_helper_->has_incremental_data(has_incremental_data))) {
      STORAGE_LOG(WARN, "Failed to check has_incremental_data", K(ret), KPC(merge_helper_));
    } else if (progressive_merge_helper_.is_progressive_merge_finish()
            && !has_incremental_data
            && !merge_param_.is_full_merge()) {
      if (OB_FAIL(reuse_base_sstable(*merge_helper_)) && OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Failed to reuse base sstable", K(ret), KPC(merge_helper_));
      } else {
        FLOG_INFO("succeed to reuse base sstable", KPC(merge_helper_));
      }
    } else {
      int64_t reuse_row_cnt = 0;
      int64_t macro_block_count = 0;
      while (OB_SUCC(ret)) {
        macro_block_count = merge_info_.macro_block_count_;
        ctx.mem_ctx_.mem_click();
        if (OB_FAIL(share::dag_yield())) {
          STORAGE_LOG(WARN, "fail to yield dag", KR(ret));
        } else if (OB_UNLIKELY(!MTL(ObTenantTabletScheduler *)->could_major_merge_start())) {
          ret = OB_CANCELED;
          STORAGE_LOG(WARN, "Major merge has been paused", K(ret));
          CTX_SET_DIAGNOSE_LOCATION(ctx);
        } else if (merge_helper_->is_iter_end()) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(merge_helper_->find_rowkey_minimum_iters(minimum_iters_))) {
          STORAGE_LOG(WARN, "Failed to find minimum iters", K(ret), KPC(merge_helper_));
        } else if (0 == minimum_iters_.count()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected minimum_iters_ is null", K(ret));
        } else if (FALSE_IT(set_base_iter(minimum_iters_))) {
        } else if (merge_helper_->is_need_skip()) {
          //move purge iters
          if (OB_FAIL(merge_helper_->move_iters_next(minimum_iters_))) {
            STORAGE_LOG(WARN, "failed to move_iters_next", K(ret), K(minimum_iters_));
          }
        } else if (1 == minimum_iters_.count() && nullptr == minimum_iters_.at(0)->get_curr_row()) {
          ObPartitionMergeIter *iter = minimum_iters_.at(0);
          if (!iter->is_macro_block_opened()) {
            if (OB_FAIL(merge_macro_block_iter(minimum_iters_, reuse_row_cnt))) {
              STORAGE_LOG(WARN, "Failed to merge_macro_block_iter", K(ret), K(minimum_iters_));
            }
          } else if (!iter->is_micro_block_opened()) {
            // only micro_merge_iter will set the micro_block_opened flag
            if (OB_FAIL(merge_micro_block_iter(*iter, reuse_row_cnt))) {
              STORAGE_LOG(WARN, "Failed to merge_micro_block_iter", K(ret), K(minimum_iters_));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "cur row is null, but block opened", K(ret), KPC(iter));
          }
        } else if (OB_FAIL(merge_same_rowkey_iters(minimum_iters_))) {
          STORAGE_LOG(WARN, "failed to merge_same_rowkey_iters", K(ret), K(minimum_iters_));
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(merge_helper_->rebuild_rows_merger())) {
          STORAGE_LOG(WARN, "rebuild rows merge failed", K(ret), KPC(merge_helper_));
        }
        // updating merge progress should not have effect on normal merge process
        if (macro_block_count < merge_info_.macro_block_count_) {
          if (OB_NOT_NULL(merge_progress_) && (OB_SUCC(ret) || ret == OB_ITER_END)) {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = merge_progress_->update_merge_progress(idx,
                reuse_row_cnt + merge_helper_->get_iters_row_count()))) {
              STORAGE_LOG(WARN, "failed to update merge progress", K(tmp_ret));
            }
          }
        }
      } // end of while
    }
    if (OB_ITER_END != ret || OB_FAIL(merge_helper_->check_iter_end())) { //verify merge end
      STORAGE_LOG(WARN, "Partition merge did not end normally", K(ret));
      if (GCONF._enable_compaction_diagnose) {
        ObPartitionMergeDumper::print_error_info(ret, merge_helper_->get_merge_iters(), *merge_ctx_);
        macro_writer_->dump_block_and_writer_buffer();
      }
    } else if (OB_FAIL(close())){
      STORAGE_LOG(WARN, "failed to close partition merger", K(ret));
    }

    FLOG_INFO("get current compaction task's mem peak", K(ret), "mem_peak_total", ctx.mem_ctx_.get_total_mem_peak());
  }
  return ret;
}

int ObPartitionMajorMerger::init_progressive_merge_helper()
{
  int ret = OB_SUCCESS;
  ObSSTable *first_sstable = nullptr;
  const ObTablesHandleArray &tables_handle = merge_ctx_->get_tables_handle();
  if (tables_handle.get_count() == 0) {
  } else if (OB_ISNULL(first_sstable = static_cast<ObSSTable *>(tables_handle.get_table(0)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null first sstable", K(ret), K(tables_handle));
  } else if (OB_FAIL(progressive_merge_helper_.init(*first_sstable, merge_param_, merger_arena_))) {
    STORAGE_LOG(WARN, "failed to init progressive_merge_helper", K(ret));
  }

  return ret;
}

int ObPartitionMajorMerger::merge_same_rowkey_iters(MERGE_ITER_ARRAY &merge_iters)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_fuser_->fuse_row(merge_iters))) {
    STORAGE_LOG(WARN, "Failed to fuse row", KPC_(partition_fuser), K(ret));
  } else if (OB_FAIL(process(partition_fuser_->get_result_row()))) {
    STORAGE_LOG(WARN, "Failed to process row", K(ret), K(partition_fuser_->get_result_row()));
  } else if (OB_FAIL(merge_helper_->move_iters_next(merge_iters))) {
    STORAGE_LOG(WARN, "failed to move iters", K(ret), K(merge_iters));
  }
  return ret;
}

int ObPartitionMajorMerger::merge_micro_block_iter(ObPartitionMergeIter &iter, int64_t &reuse_row_cnt)
{
  int ret = OB_SUCCESS;
  const ObMicroBlock *micro_block;
  if (OB_FAIL(iter.get_curr_micro_block(micro_block))) {
    STORAGE_LOG(WARN, "Failed to get current micro block", K(ret), K(iter));
  } else if (OB_ISNULL(micro_block)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null micro block", K(ret), K(iter));
  } else if (OB_FAIL(process(*micro_block))) {
    STORAGE_LOG(WARN, "Failed to append micro block", K(ret), K(micro_block));
  } else if (FALSE_IT(reuse_row_cnt += micro_block->header_.row_count_)) {
  } else if (OB_FAIL(iter.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "Failed to get next row", K(ret));
    }
  }
  return ret;
}

//TODO this func should be replaced with ObPartitionMinorMerger:::rewrite_macro_block
int ObPartitionMajorMerger::rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters)
{
  int ret = OB_SUCCESS;
  ObPartitionMergeIter *iter = nullptr;
  blocksstable::MacroBlockId curr_macro_id;
  const ObMacroBlockDesc *curr_macro = nullptr;
  const ObMacroBlockDesc *tmp_macro = nullptr;
  if (minimum_iters.count() != 1) {
    ret = OB_INNER_STAT_ERROR;
    STORAGE_LOG(WARN, "Unexpected minimum iters to rewrite macro block", K(ret), K(minimum_iters));
  } else if (!partition_fuser_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected partition fuser", KPC(partition_fuser_), K(ret));
  } else if (FALSE_IT(iter = minimum_iters.at(0))) {
  } else if (OB_FAIL(iter->open_curr_range(true /* rewrite */))) {
    STORAGE_LOG(WARN, "Failed to open the curr macro block", K(ret));
  } else if (OB_FAIL(iter->get_curr_macro_block(curr_macro))) {
    STORAGE_LOG(WARN, "failed to get curr macro block", K(ret), KPC(curr_macro));
  } else if (OB_ISNULL(curr_macro)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "curr macro is null", K(ret), KPC(curr_macro));
  } else {
    STORAGE_LOG(DEBUG, "Rewrite macro block", KPC(iter));
    curr_macro_id = curr_macro->macro_block_id_;
    // TODO maybe we need use macro_block_ctx to decide whether the result row came from the same macro block
    while (OB_SUCC(ret) && !iter->is_iter_end() && iter->is_macro_block_opened()) {
      if (OB_FAIL(merge_same_rowkey_iters(minimum_iters))) {
        STORAGE_LOG(WARN, "failed to merge_same_rowkey_iters", K(ret), K(minimum_iters));
      } else if (OB_FAIL(iter->get_curr_macro_block(tmp_macro))) {
        STORAGE_LOG(WARN, "failed to get curr macro block", K(ret), KPC(tmp_macro));
      } else if (OB_ISNULL(tmp_macro)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tmp macro is null", K(ret), KPC(tmp_macro));
      } else if (tmp_macro->macro_block_id_ != curr_macro_id) {
        LOG_DEBUG("break for different macro", K(ret), KPC(tmp_macro), KPC(curr_macro));
        break;
      }
    }
  }

  return ret;
}

int ObPartitionMajorMerger::reuse_base_sstable(ObPartitionMergeHelper &merge_helper)
{
  int ret = OB_SUCCESS;
  MERGE_ITER_ARRAY minimum_iters;
  ObPartitionMergeIter *base_iter = nullptr;
  const ObMacroBlockDesc *macro_desc = nullptr;

  if (OB_FAIL(merge_helper.find_rowkey_minimum_iters(minimum_iters))) {
    STORAGE_LOG(WARN, "failed to find_rowkey_minimum_iters", K(ret), K(merge_helper));
  } else if (1 != minimum_iters.count() || OB_ISNULL(base_iter = minimum_iters.at(0)) ||
      !base_iter->is_base_sstable_iter() || !base_iter->is_macro_merge_iter()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected minimum_iters", K(ret), KPC(base_iter), K(minimum_iters));
  } else {
    const ObSSTable *base_table = static_cast<const ObSSTable *>(base_iter->get_table());
    while (OB_SUCC(ret)) {
      if (base_iter->is_iter_end()) {
        ret = OB_ITER_END;
      } else if (base_table->is_small_sstable()
          && !base_iter->is_macro_block_opened()
          && merge_info_.concurrent_cnt_ > 1) { // small_sstable should not reuse macro in concurrency
        if (OB_FAIL(base_iter->open_curr_range(true/*for_rewrite*/))) {
          LOG_WARN("failed to open curr range", KR(ret), K(base_iter));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (base_iter->is_macro_block_opened()) { // opend for cross range
        // flush all row in curr macro block
        while (OB_SUCC(ret) && base_iter->is_macro_block_opened()) {
          if (OB_ISNULL(base_iter->get_curr_row())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "curr row is unexpected null", K(ret), KPC(base_iter));
          } else if (OB_FAIL(process(*base_iter->get_curr_row()))) {
            STORAGE_LOG(WARN, "Failed to process row", K(ret), K(partition_fuser_->get_result_row()));
            if (GCONF._enable_compaction_diagnose) {
              ObPartitionMergeDumper::print_error_info(ret, minimum_iters, *merge_ctx_);
            }
          } else if (OB_FAIL(base_iter->next())) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "Failed to get next", K(ret), KPC(base_iter));
            }
          }
        } // end of while
      } else if (OB_FAIL(base_iter->get_curr_macro_block(macro_desc))) {
        STORAGE_LOG(WARN, "Failed to get current macro block", K(ret), KPC(base_iter));
      } else if (OB_ISNULL(macro_desc) || OB_UNLIKELY(!macro_desc->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Invalid macro block descriptor", K(ret), KPC(macro_desc), KPC(base_iter));
      } else if (OB_FAIL(process(*macro_desc))) {
        STORAGE_LOG(WARN, "Fail to append macro block", K(ret), KPC(base_iter));
      } else if (OB_FAIL(base_iter->next())) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Failed to get next", K(ret), KPC(base_iter));
        }
      }
    }
  }

  return ret;
}

/*
 *ObPartitionMinorMergerV2
 */

ObPartitionMinorMerger::ObPartitionMinorMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param)
  : ObPartitionMerger(allocator, static_param),
    minimum_iter_idxs_(DEFAULT_ITER_COUNT * sizeof(int64_t), ModulePageAllocator(allocator))
{
}

ObPartitionMinorMerger::~ObPartitionMinorMerger()
{
  reset();
}

void ObPartitionMinorMerger::reset()
{
  minimum_iter_idxs_.reset();
  ObPartitionMerger::reset();
}

int ObPartitionMinorMerger::inner_init()
{
  int ret = OB_SUCCESS;
  merge_helper_ = OB_NEWx(ObPartitionMinorMergeHelper, (&merger_arena_), merge_ctx_->read_info_, merger_arena_);

  if (OB_ISNULL(merge_helper_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to allocate memory for partition helper", K(ret));
  } else if (OB_FAIL(init_progressive_merge_helper())) {
    STORAGE_LOG(WARN, "Failed to init progressive_merge_helper", K(ret));
  } else if (OB_FAIL(merge_helper_->init(merge_param_))) {
    STORAGE_LOG(WARN, "Failed to init merge helper", K(ret));
  }

  return ret;
}

int ObPartitionMinorMerger::init_progressive_merge_helper()
{
  int ret = OB_SUCCESS;
  ObSSTable *first_sstable = nullptr;
  const ObTablesHandleArray &tables_handle = merge_ctx_->get_tables_handle();
  if (tables_handle.get_count() == 0 || is_mini_merge(merge_param_.static_param_.get_merge_type())) {
  } else if (!tables_handle.get_table(0)->is_sstable()) {
  } else if (OB_ISNULL(first_sstable = static_cast<ObSSTable *>(tables_handle.get_table(0)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null first sstable", K(ret), K(tables_handle));
  } else if (OB_FAIL(progressive_merge_helper_.init(*first_sstable, merge_param_, merger_arena_))) {
    STORAGE_LOG(WARN, "failed to init progressive_merge_helper", K(ret));
  }

  return ret;
}

int ObPartitionMinorMerger::open_macro_writer(ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPartitionMerger::open_macro_writer(merge_param))){
    STORAGE_LOG(WARN, "Failed to open_macro_writer", K(ret), K(merge_param));
  }

  return ret;
}

int ObPartitionMinorMerger::rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters)
{
  int ret = OB_SUCCESS;
  ObPartitionMergeIter *iter = nullptr;
  if (OB_ISNULL(iter = minimum_iters.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null iter", K(ret));
  } else if (OB_FAIL(iter->open_curr_range(true /* rewrite */))) {
    STORAGE_LOG(WARN, "Failed to open the curr macro block", K(ret));
  }
  return ret;
}

int ObPartitionMinorMerger::inner_process(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObMacroBlockDesc *macro_desc;
  if (OB_FAIL(get_base_iter_curr_macro_block(macro_desc))) {
    STORAGE_LOG(WARN, "Failed to get base iter macro", K(ret));
  } else if (OB_FAIL(macro_writer_->append_row(row, macro_desc))) {
    STORAGE_LOG(WARN, "Failed to append row to macro writer", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "Success to append row to minor macro writer", K(ret), K(row));
  }

  return ret;
}

int ObPartitionMinorMerger::merge_partition(
    ObBasicTabletMergeCtx &ctx,
    const int64_t idx)
{
  int ret = OB_SUCCESS;
  SET_MEM_CTX(ctx.mem_ctx_);
  ObLocalArena local_arena("MinimumIters");

  if (OB_FAIL(prepare_merge(ctx, idx))) {
    LOG_WARN("failed to prepare merge", K(ret), K(idx), K(ctx));
  } else {
    int64_t reuse_row_cnt = 0;
    int64_t macro_block_count = 0;
    MERGE_ITER_ARRAY rowkey_minimum_iters(DEFAULT_ITER_ARRAY_SIZE, ModulePageAllocator(local_arena));

    while (OB_SUCC(ret)) {
      macro_block_count = merge_info_.macro_block_count_;
      ctx.mem_ctx_.mem_click();
      if (OB_FAIL(share::dag_yield())) {
        STORAGE_LOG(WARN, "fail to yield dag", KR(ret));
      } else if (merge_helper_->is_iter_end()) { //find minimum merge iter
        ret = OB_ITER_END;
      } else if (OB_FAIL(merge_helper_->find_rowkey_minimum_iters(rowkey_minimum_iters))) {
        STORAGE_LOG(WARN, "Failed to find minimum iters", K(ret), KPC(merge_helper_));
      } else if (rowkey_minimum_iters.empty()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected rowkey_minimum_iters is null", K(ret));
      } else if (FALSE_IT(set_base_iter(rowkey_minimum_iters))) {
      } else if (1 == rowkey_minimum_iters.count()
          && nullptr == rowkey_minimum_iters.at(0)->get_curr_row()) {
        // only one iter, output its' macro block
        if (OB_FAIL(merge_macro_block_iter(rowkey_minimum_iters, reuse_row_cnt))) {
          STORAGE_LOG(WARN, "Failed to merge_macro_block_iter", K(ret), K(rowkey_minimum_iters));
        }
      } else if (OB_FAIL(merge_same_rowkey_iters(rowkey_minimum_iters))) {
        STORAGE_LOG(WARN, "Failed to merge iters with same rowkey", K(ret), K(rowkey_minimum_iters));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(merge_helper_->rebuild_rows_merger())) {
        STORAGE_LOG(WARN, "rebuild rows merge failed", K(ret), KPC(merge_helper_));
      }
      // updating merge progress should not have effect on normal merge process
      if (macro_block_count < merge_info_.macro_block_count_) {
        if (OB_NOT_NULL(merge_progress_) && (OB_SUCC(ret) || ret == OB_ITER_END)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = merge_progress_->update_merge_progress(idx,
              reuse_row_cnt + merge_helper_->get_iters_row_count()))) {
            STORAGE_LOG(WARN, "failed to update merge progress", K(tmp_ret));
          }
        }
      }
    } // end of while

    if (OB_ITER_END != ret || OB_FAIL(merge_helper_->check_iter_end())) {
      STORAGE_LOG(WARN, "Partition merge did not end normally", K(ret));
      if (GCONF._enable_compaction_diagnose) {
        ObPartitionMergeDumper::print_error_info(ret, merge_helper_->get_merge_iters(), *merge_ctx_);
        macro_writer_->dump_block_and_writer_buffer();
      }
    } else if (OB_FAIL(close())){
      STORAGE_LOG(WARN, "failed to close partition merger", K(ret));
    } else if (ctx.get_tablet_id().is_special_merge_tablet()) {
      // do nothing
    } else if (is_mini_merge(merge_param_.static_param_.get_merge_type())) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(collect_merge_stat(merge_param_.static_param_.get_merge_type(),
          *(reinterpret_cast<ObPartitionMinorMergeHelper*>(merge_helper_)), ctx))) {
        STORAGE_LOG(WARN, "failed to collect merge stat", K(tmp_ret), K_(merge_param));
      }
    }

    FLOG_INFO("get current compaction task's mem peak", K(ret), "mem_peak_total", ctx.mem_ctx_.get_total_mem_peak());
  }
  return ret;
}

/*
 * TODO(@DanLing)
 * Add mysql test case after column store branch is merged into master,
 * cause __all_virtual_tablet_stat is on column store.
 */
int ObPartitionMinorMerger::collect_merge_stat(
    const ObMergeType &merge_type,
    ObPartitionMinorMergeHelper &merge_helper,
    ObBasicTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTransNodeDMLStat tnode_stat;

  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(merge_type), K(ctx));
  } else if (OB_UNLIKELY(!is_mini_merge(merge_type))) {
  } else if (ctx.get_tablet_id().is_special_merge_tablet()) {
    // do nothing
  } else if (OB_FAIL(merge_helper.collect_tnode_dml_stat(merge_type, tnode_stat))) {
    STORAGE_LOG(WARN, "failed to get memtable stat", K(ret));
  } else if (tnode_stat.empty()) {
    // do nothing
  } else {
    ctx.collect_tnode_dml_stat(tnode_stat);
  }
  return ret;
}

int ObPartitionMinorMerger::merge_single_iter(ObPartitionMergeIter &merge_iter)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(merge_iter.get_curr_row())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected empty row of merge iter", K(ret), K(merge_iter));
  } else {
    const ObDatumRow *cur_row = nullptr;
    bool finish = false;
    bool rowkey_first_row = !merge_iter.is_rowkey_first_row_already_output();
    bool shadow_already_output = merge_iter.is_rowkey_shadow_row_already_output();
    while (OB_SUCC(ret) && !finish) {
      if (OB_ISNULL(cur_row = merge_iter.get_curr_row())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected empty row of merge iter", K(ret), K(merge_iter));
      } else if (rowkey_first_row && cur_row->is_ghost_row()) {
        // discard ghost row
        finish = true;
      } else if (rowkey_first_row) {
        const_cast<ObDatumRow *>(cur_row)->mvcc_row_flag_.set_first_multi_version_row(true);
        rowkey_first_row = false;
      } else {
        const_cast<ObDatumRow *>(cur_row)->mvcc_row_flag_.set_first_multi_version_row(false);
      }
      if (OB_FAIL(ret) || finish) {
      } else if (shadow_already_output && cur_row->is_shadow_row()) {
      } else if (OB_FAIL(process(*cur_row))) {
        STORAGE_LOG(WARN, "Failed to process row", K(ret), KPC(cur_row), K(merge_iter));
      } else if (cur_row->is_last_multi_version_row()) {
        finish = true;
      } else if (!shadow_already_output && cur_row->is_shadow_row()) {
        shadow_already_output = true;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(merge_iter.next())) {
        if (OB_ITER_END == ret) {
          if (finish) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "meed iter end without Last row", K(ret), K(merge_iter), K(finish));
          }
        } else {
          STORAGE_LOG(WARN, "Fail to next merge iter", K(ret), K(merge_iter), K(finish));
        }
      } else if (!finish && OB_ISNULL(merge_iter.get_curr_row())) {
        if (OB_FAIL(merge_iter.open_curr_range(false /*for_rewrite*/))) {
          STORAGE_LOG(WARN, "Failed to open curr range", K(ret), K(merge_iter));
        }
      }
    }
  }


  return ret;
}

int ObPartitionMinorMerger::find_minimum_iters_with_same_rowkey(MERGE_ITER_ARRAY &merge_iters,
                                                                MERGE_ITER_ARRAY &minimum_iters,
                                                                ObIArray<int64_t> &iter_idxs)
{
  int ret = OB_SUCCESS;
  ObPartitionMergeIter *base_iter = nullptr;
  ObPartitionMergeIter *merge_iter = nullptr;
  minimum_iters.reuse();
  iter_idxs.reuse();
  if (OB_UNLIKELY(merge_iters.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to find minimum iters with same rowkey", K(ret),
                K(merge_iters));
  } else if (OB_ISNULL(base_iter = merge_iters.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null merge iter", K(ret), K(merge_iters));
  } else if (OB_FAIL(minimum_iters.push_back(base_iter))) {
    STORAGE_LOG(WARN, "Failed to push back merge iter", K(ret));
  } else if (OB_FAIL(iter_idxs.push_back(0))) {
    STORAGE_LOG(WARN, "Failed to push back iter idx", K(ret));
  } else {
    for (int64_t i = 1; OB_SUCC(ret) && i < merge_iters.count(); i++) {
      int cmp_ret = 0;
      if (OB_ISNULL(merge_iter = merge_iters.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null merge iter", K(ret), K(i), K(merge_iters));
      } else if (OB_FAIL(merge_iter->multi_version_compare(*base_iter, cmp_ret))) {
        STORAGE_LOG(WARN, "Failed to compare multi version merge iter", K(ret));
      } else if (OB_UNLIKELY(cmp_ret < 0)) {
        minimum_iters.reuse();
        iter_idxs.reuse();
        base_iter = merge_iter;
      } else if (cmp_ret > 0) {
        // skip this merge iter
        continue;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(minimum_iters.push_back(merge_iter))) {
        STORAGE_LOG(WARN, "Failed to push back merge iter", K(ret));
      } else if (OB_FAIL(iter_idxs.push_back(i))) {
        STORAGE_LOG(WARN, "Failed to push back iter idx", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObPartitionMinorMerger::check_first_committed_row(const MERGE_ITER_ARRAY &merge_iters)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merge_iters.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to merge iters with same rowkey", K(ret), K(merge_iters));
  } else {
    ObPartitionMergeIter *merge_iter = nullptr;
    for (int64_t i = merge_iters.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      if (OB_ISNULL(merge_iter = merge_iters.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null merge iter", K(ret), K(merge_iters));
      } else if (merge_iter->is_compact_completed_row()) {
        // do nothing
      } else if (OB_UNLIKELY(!merge_iter->get_curr_row()->is_ghost_row())) {
        ret = OB_INNER_STAT_ERROR;
        STORAGE_LOG(WARN, "Unexpected non compact merge iter", K(ret), KPC(merge_iter));
      }
    }
  }

  return ret;
}

int ObPartitionMinorMerger::set_result_flag(MERGE_ITER_ARRAY &fuse_iters,
                                            const bool rowkey_first_row,
                                            const bool add_shadow_row,
                                            const bool need_check_last)
{
  int ret = OB_SUCCESS;
  ObPartitionMergeIter *base_iter = nullptr;
  const ObDatumRow *base_row = nullptr;

  if (OB_UNLIKELY(fuse_iters.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid empty fuse iters", K(ret), K(fuse_iters));
  } else if (OB_ISNULL(base_iter = fuse_iters.at(0))) {
    ret = OB_INNER_STAT_ERROR;
    STORAGE_LOG(WARN, "Unexpected null fuse iter", K(ret), K(fuse_iters));
  } else if (OB_ISNULL(base_row = base_iter->get_curr_row())) {
    ret = OB_INNER_STAT_ERROR;
    STORAGE_LOG(WARN, "Unexpected null curr row for base iter", K(ret), KPC(base_iter));
  } else {
    const bool is_result_compact = partition_fuser_->get_result_row().is_compacted_multi_version_row();
    ObMultiVersionRowFlag row_flag = base_row->mvcc_row_flag_;
    if (!base_row->is_uncommitted_row() && !base_row->is_ghost_row()) {
      row_flag.set_compacted_multi_version_row(is_result_compact);
    }
    if (rowkey_first_row) {
      row_flag.set_first_multi_version_row(true);
    } else {
      row_flag.set_first_multi_version_row(false);
    }
    if (base_row->is_ghost_row()) {
    } else if (!base_row->is_last_multi_version_row()) {
    } else if (!need_check_last) {
      row_flag.set_last_multi_version_row(false);
    } else {
      for (int64_t i = 1; OB_SUCC(ret) && i < fuse_iters.count(); i++) {
        if (OB_UNLIKELY(nullptr == fuse_iters.at(i) || nullptr == fuse_iters.at(i)->get_curr_row())) {
          ret = OB_INNER_STAT_ERROR;
          STORAGE_LOG(WARN, "Unexpected null fuse iter or curr row", K(ret), K(i), KPC(fuse_iters.at(i)));
        } else if (!fuse_iters.at(i)->get_curr_row()->is_last_multi_version_row()) {
          row_flag.set_last_multi_version_row(false);
          break;
        }
      }
    }
    if (FAILEDx(partition_fuser_->set_multi_version_flag(row_flag))) {
      STORAGE_LOG(WARN, "Failed to set multi version row flag and dml", K(ret));
    } else if (add_shadow_row && OB_FAIL(partition_fuser_->make_result_row_shadow(
          data_store_desc_.get_schema_rowkey_col_cnt() + 1 /*sql_sequence_col_idx*/))) {
        LOG_WARN("failed to make shadow row", K(ret),
          "result_row", partition_fuser_->get_result_row(),
          "sql_seq_col_idx", data_store_desc_.get_schema_rowkey_col_cnt() + 1);
    } else {
      STORAGE_LOG(DEBUG, "succ to set multi version row flag and dml", K(partition_fuser_->get_result_row()),
                  K(row_flag), KPC(base_row));
    }
  }

  return ret;
}

int ObPartitionMinorMerger::try_remove_ghost_iters(MERGE_ITER_ARRAY &merge_iters,
                                                   const bool rowkey_first_row,
                                                   MERGE_ITER_ARRAY &minimum_iters,
                                                   ObIArray<int64_t> &iter_idxs)
{
  int ret = OB_SUCCESS;
  // if new iter iters ghost row, old row may have smalled trans version
  // now we can just ignore all the ghost row since we have one normal row at least

  if (OB_UNLIKELY(merge_iters.count() < 1 || (!rowkey_first_row && merge_iters.count() == 1))) {
  } else {
    bool found_ghost = false;
    ObPartitionMergeIter *merge_iter = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters.count(); i++) {
      if (OB_ISNULL(merge_iter = merge_iters.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null merge iter", K(ret), K(i), K(merge_iters));
      } else if (merge_iter->get_curr_row()->is_ghost_row()) {
        if (!found_ghost) {
          found_ghost = true;
          minimum_iters.reuse();
          iter_idxs.reuse();
        }
        if (OB_FAIL(minimum_iters.push_back(merge_iter))) {
          STORAGE_LOG(WARN, "Failed to push back merge iter", K(ret));
        } else if (OB_FAIL(iter_idxs.push_back(i))) {
          STORAGE_LOG(WARN, "Failed to push back iter idx", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && found_ghost) {
      // not the first row, we need keep at least one ghost row for last row flag
      if (minimum_iters.count() == merge_iters.count() && !rowkey_first_row) {
      } else {
        LOG_TRACE("try to remove useless row which consists of ghost rows only",
            KPC(minimum_iters.at(0)), K(rowkey_first_row), K(iter_idxs));
        if (OB_FAIL(move_and_remove_unused_iters(merge_iters, minimum_iters, iter_idxs))) {
          STORAGE_LOG(WARN, "Failed to move and remove iters", K(ret));
        }
      }
    }
  }


  return ret;
}

int ObPartitionMinorMerger::merge_same_rowkey_iters(MERGE_ITER_ARRAY &merge_iters)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(merge_iters.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to merge iters with same rowkey", K(ret), K(merge_iters));
  } else if (OB_LIKELY(merge_iters.count() == 1)) {
    if (OB_FAIL(merge_single_iter(*merge_iters.at(0)))) {
      STORAGE_LOG(WARN, "Failed to merge single merge iter", K(ret));
    }
  } else {
    bool rowkey_first_row = true;
    bool shadow_already_output = false;
    ObPartitionMergeIter *base_iter = nullptr;
    // base iter always iters the row with newer version
    while (OB_SUCC(ret) && !merge_iters.empty()) {
      bool add_shadow_row = false;
      MERGE_ITER_ARRAY *fuse_iters = &minimum_iters_;
      if (OB_FAIL(try_remove_ghost_iters(merge_iters, rowkey_first_row, minimum_iters_, minimum_iter_idxs_))) {
        STORAGE_LOG(WARN, "Failed to check and remove ghost iters", K(ret));
      } else if (OB_UNLIKELY(merge_iters.empty())) {
        // all the iters are ghost row iter
        break;
      } else if (OB_FAIL(find_minimum_iters_with_same_rowkey(merge_iters, minimum_iters_, minimum_iter_idxs_))) {
        STORAGE_LOG(WARN, "Failed to find minimum iters with same rowkey", K(ret));
      } else if (OB_UNLIKELY(minimum_iters_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected empty minimum iters", K(ret), K(merge_iters));
      } else if (OB_ISNULL(base_iter = minimum_iters_.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null merge iter", K(ret), K_(minimum_iters));
      } else if (!shadow_already_output && base_iter->is_compact_completed_row()) {
        if (OB_FAIL(check_add_shadow_row(merge_iters,
                                         minimum_iters_.count() != merge_iters.count(),
                                         add_shadow_row))) {
          LOG_WARN("Failed to merge shadow row", K(ret), K(merge_iters));
        } else {
          fuse_iters = &merge_iters;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(base_iter)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null base iter", K(ret), K(merge_iters));
      } else if (shadow_already_output && base_iter->get_curr_row()->is_shadow_row()) {
        if (OB_UNLIKELY(1 != minimum_iters_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected minimum shadow row iters", K(ret), K(minimum_iters_));
        }
      } else if (OB_FAIL(partition_fuser_->fuse_row(*fuse_iters))) {
        STORAGE_LOG(WARN, "Failed to fuse rowkey minimum iters", K(ret), KPC(fuse_iters));
      } else if (OB_FAIL(set_result_flag(*fuse_iters, rowkey_first_row, add_shadow_row,
                                         minimum_iters_.count() == merge_iters.count()))) {
        STORAGE_LOG(WARN, "Failed to calc multi version row flag", K(ret), K(add_shadow_row),
                    K(shadow_already_output), KPC(fuse_iters));
      } else if (OB_FAIL(process(partition_fuser_->get_result_row()))) {
        STORAGE_LOG(WARN, "Failed to process row", K(ret), K(partition_fuser_->get_result_row()), KPC(fuse_iters));
      } else if (!shadow_already_output && base_iter->is_compact_completed_row()) {
        shadow_already_output = true;
      }

      if (OB_SUCC(ret)) {
        rowkey_first_row = false;
        if (add_shadow_row) {
          if (OB_FAIL(skip_shadow_row(*fuse_iters))) {
            LOG_WARN("Failed to skip shadow row", K(ret), K(merge_iters));
          }
        } else if (OB_FAIL(move_and_remove_unused_iters(merge_iters, minimum_iters_, minimum_iter_idxs_))) {
          LOG_WARN("Failed to move and remove iters", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObPartitionMinorMerger::check_add_shadow_row(MERGE_ITER_ARRAY &merge_iters, const bool contain_multi_trans, bool& add_shadow_row)
{
  int ret = OB_SUCCESS;
  add_shadow_row = false;
  if (OB_FAIL(check_first_committed_row(merge_iters))) {
    LOG_WARN("Failed to check compact first multi version row", K(ret));
  } else {
    if (contain_multi_trans) {
      add_shadow_row = true;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters.count(); i++) {
        if (OB_UNLIKELY(nullptr == merge_iters.at(i) || nullptr == merge_iters.at(i)->get_curr_row())) {
          ret = OB_INNER_STAT_ERROR;
          LOG_WARN("Unexpected null fuse iter or curr row", K(ret), K(i), KPC(merge_iters.at(i)));
        } else if (merge_iters.at(i)->get_curr_row()->is_shadow_row()) {
          add_shadow_row = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObPartitionMinorMerger::move_and_remove_unused_iters(MERGE_ITER_ARRAY &merge_iters,
                                                         MERGE_ITER_ARRAY &minimum_iters,
                                                         ObIArray<int64_t> &iter_idxs)
{
  int ret = OB_SUCCESS;
  ObPartitionMergeIter *merge_iter = nullptr;

  for (int64_t i = minimum_iters.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    bool need_remove = false;
    if (OB_ISNULL(merge_iter = minimum_iters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null merge iter", K(ret), K(i), K(minimum_iters));
    } else if (FALSE_IT(need_remove = (merge_iter->get_curr_row()->is_last_multi_version_row()))) {
    } else if (OB_FAIL(merge_iter->next())) {
      if (OB_ITER_END == ret && need_remove) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "Failed to next merge iter", K(ret), KPC(merge_iter));
      }
    } else if (!need_remove && nullptr == merge_iter->get_curr_row()) {
      if (OB_FAIL(merge_iter->open_curr_range(false /*for_rewrite*/))) {
        STORAGE_LOG(WARN, "Failed to open curr range", K(ret));
      }
    }
    if (OB_SUCC(ret) && need_remove) {
      if (OB_FAIL(merge_iters.remove(iter_idxs.at(i)))) {
        STORAGE_LOG(WARN, "Failed to remove merge iter", K(ret), K(i), K(iter_idxs),
                    K(merge_iters));
      }
    }
  }

  return ret;
}

int ObPartitionMinorMerger::skip_shadow_row(MERGE_ITER_ARRAY &merge_iters)
{
  int ret = OB_SUCCESS;
  ObPartitionMergeIter *merge_iter = nullptr;
  const ObDatumRow *merge_row = nullptr;
  for (int64_t i = merge_iters.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    if (OB_ISNULL(merge_iter = merge_iters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null merge iter", K(ret), K(i), K(merge_iters));
    } else if (OB_ISNULL(merge_row = merge_iter->get_curr_row())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null curr row", K(ret), KPC(merge_iter));
    } else if (merge_row->is_shadow_row()) {
      if (OB_FAIL(merge_iter->next())) {
        LOG_WARN("Failed to next merge iter", K(ret), KPC(merge_iter));
      } else if (nullptr == merge_iter->get_curr_row()) {
        if (OB_FAIL(merge_iter->open_curr_range(false /*for_rewrite*/))) {
          LOG_WARN("Failed to open curr range", K(ret));
        }
      }
    } // else continue
  }

  return ret;
}

/*
 *ObPartitionMergeDumper
 */
int ObPartitionMergeDumper::generate_dump_table_name(const char *dir_name,
                                                     const ObITable *table,
                                                     char *file_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table is null", K(ret));
  } else {
    int64_t pret = snprintf(
                       file_name, OB_MAX_FILE_NAME_LENGTH, "%s/%s.%s.%ld.%s.%d.%s.%ld.%s.%ld",
                       dir_name,
                       table->is_memtable() ? "dump_memtable" : "dump_sstable",
                       "tablet_id", table->get_key().tablet_id_.id(),
                       "table_type", table->get_key().table_type_,
                       "start_scn", table->get_start_scn().get_val_for_tx(),
                       "end_scn", table->get_end_scn().get_val_for_tx());
    if (pret < 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "name too long", K(ret), K(pret), K(file_name));
    }
  }
  return ret;
}

lib::ObMutex ObPartitionMergeDumper::lock(common::ObLatchIds::MERGER_DUMP_LOCK);

int ObPartitionMergeDumper::check_disk_free_space(const char *dir_name)
{
  int ret = OB_SUCCESS;
  int64_t total_space = 0;
  int64_t free_space = 0;
  if (OB_FAIL(FileDirectoryUtils::get_disk_space(dir_name, total_space, free_space))) {
    STORAGE_LOG(WARN, "Failed to get disk space ", K(ret), K(dir_name));
  } else if (free_space < ObPartitionMergeDumper::DUMP_TABLE_DISK_FREE_PERCENTAGE * total_space) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
  }
  return ret;
}

int ObPartitionMergeDumper::judge_disk_free_space(const char *dir_name, ObITable *table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table is null", K(ret));
  } else {
    int64_t total_space = 0;
    int64_t free_space = 0;
    if (OB_FAIL(FileDirectoryUtils::get_disk_space(dir_name, total_space, free_space))) {
      STORAGE_LOG(WARN, "Failed to get disk space ", K(ret), K(dir_name));
    } else if (table->is_sstable()) {
      if (free_space
          - static_cast<ObSSTable *>(table)->get_total_macro_block_count() *
          OB_DEFAULT_MACRO_BLOCK_SIZE
          < ObPartitionMergeDumper::DUMP_TABLE_DISK_FREE_PERCENTAGE * total_space) {
        ret = OB_SERVER_OUTOF_DISK_SPACE;
        STORAGE_LOG(WARN, "disk space is not enough", K(ret), K(free_space), K(total_space), KPC(table));
      }
    } else if (free_space
               - static_cast<ObMemtable *>(table)->get_occupied_size() * MEMTABLE_DUMP_SIZE_PERCENTAGE
               < ObPartitionMergeDumper::DUMP_TABLE_DISK_FREE_PERCENTAGE * total_space) {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      STORAGE_LOG(WARN, "disk space is not enough", K(ret), K(free_space), K(total_space), KPC(table));
    }
  }
  return ret;
}

bool ObPartitionMergeDumper::need_dump_table(int err_no)
{
  bool bret = false;
  if (OB_CHECKSUM_ERROR == err_no
      || OB_ERR_UNEXPECTED == err_no
      || OB_ERR_SYS == err_no
      || OB_ROWKEY_ORDER_ERROR == err_no
      || OB_ERR_PRIMARY_KEY_DUPLICATE == err_no) {
    bret = true;
  }
  return bret;
}

void ObPartitionMergeDumper::print_error_info(const int err_no,
                                              const MERGE_ITER_ARRAY &merge_iters,
                                              ObBasicTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  const char *dump_table_dir = "/tmp";
  if (need_dump_table(err_no)) {
    for (int64_t midx = 0; midx < merge_iters.count(); ++midx) {
      const ObPartitionMergeIter *cur_iter = merge_iters.at(midx);
      const ObMacroBlockDesc *macro_desc = nullptr;
      const ObDatumRow *curr_row = cur_iter->get_curr_row();
      if (!cur_iter->is_macro_merge_iter()) {
        if (OB_NOT_NULL(curr_row)) {
          STORAGE_LOG(WARN, "merge iter content: ", K(midx), K(cur_iter->get_table()->get_key()),
              KPC(cur_iter->get_curr_row()));
        }
      } else if (OB_FAIL(cur_iter->get_curr_macro_block(macro_desc))) {
        STORAGE_LOG(WARN, "Failed to get current micro block", K(ret), KPC(cur_iter));
      } else if (OB_ISNULL(macro_desc)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null macro block", K(ret), KPC(macro_desc), KPC(cur_iter));
      } else if (OB_ISNULL(curr_row)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "merge iter content: ", K(midx), K(cur_iter->get_table()->get_key()),
                    KPC(macro_desc));
      } else {
        STORAGE_LOG(WARN, "merge iter content: ", K(midx), K(cur_iter->get_table()->get_key()),
                    KPC(macro_desc), KPC(cur_iter->get_curr_row()));
      }
    }
    // dump all sstables in this merge
    char file_name[OB_MAX_FILE_NAME_LENGTH];
    lib::ObMutexGuard guard(ObPartitionMergeDumper::lock);
    const ObTablesHandleArray &tables_handle = ctx.get_tables_handle();
    for (int idx = 0; OB_SUCC(ret) && idx < tables_handle.get_count(); ++idx) {
      ObITable *table = tables_handle.get_table(idx);
      ObITable *dump_table = nullptr;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "The store is NULL", K(idx), K(tables_handle));
      } else if (OB_FAIL(compaction::ObPartitionMergeDumper::judge_disk_free_space(dump_table_dir,
                         table))) {
        if (OB_SERVER_OUTOF_DISK_SPACE != ret) {
          STORAGE_LOG(WARN, "failed to judge disk space", K(ret), K(dump_table_dir));
        }
      } else {
        dump_table = table;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_dump_table_name(dump_table_dir, dump_table, file_name))) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "name too long", K(ret), K(file_name));
      } else if (dump_table->is_sstable()) {
        if (OB_FAIL(static_cast<ObSSTable *>(dump_table)->dump2text(dump_table_dir, *ctx.static_param_.schema_,
                                                               file_name))) {
          if (OB_SERVER_OUTOF_DISK_SPACE != ret) {
            STORAGE_LOG(WARN, "failed to dump sstable", K(ret), K(file_name));
          }
        } else {
          STORAGE_LOG(INFO, "success to dump sstable", K(ret), K(file_name));
        }
      } else if (dump_table->is_memtable()) {
        STORAGE_LOG(INFO, "skip dump memtable", K(ret), K(file_name));
        /*
         *if (OB_FAIL(static_cast<ObMemtable *>(dump_table)->dump2text(file_name))) {
         *  STORAGE_LOG(WARN, "failed to dump memtable", K(ret), K(file_name));
         *}
         */
      }
    } // end for
  }
}

} //compaction
} //oceanbase
