//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_tablet_merge_info.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/tx_table/ob_tx_data_memtable.h"

namespace oceanbase
{
using namespace blocksstable;
namespace compaction
{
/*
 *  ----------------------------------------------ObTabletMergeInfo--------------------------------------------------
 */

ObTabletMergeInfo::ObTabletMergeInfo()
  :  is_inited_(false),
     lock_(common::ObLatchIds::TABLET_MERGE_INFO_LOCK),
     bloomfilter_block_id_(),
     sstable_merge_info_(),
     sstable_builder_()
{
}

ObTabletMergeInfo::~ObTabletMergeInfo()
{
  destroy();
}


void ObTabletMergeInfo::destroy()
{
  is_inited_ = false;
  bloomfilter_block_id_.reset();

  sstable_builder_.reset();
  sstable_merge_info_.reset();
}

int ObTabletMergeInfo::init(const ObBasicTabletMergeCtx &ctx, bool need_check/*true*/, bool merge_start/*true*/)
{
  int ret = OB_SUCCESS;
  const int64_t concurrent_cnt = ctx.get_concurrent_cnt();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_UNLIKELY(need_check && concurrent_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(concurrent_cnt));
  } else {
    bloomfilter_block_id_.reset();
    build_sstable_merge_info(ctx);
    if (merge_start) {
      sstable_merge_info_.merge_start_time_ = ctx.static_param_.start_time_;
    }
    is_inited_ = true;
  }

  return ret;
}

void ObTabletMergeInfo::build_sstable_merge_info(const ObBasicTabletMergeCtx &ctx)
{
  const ObStaticMergeParam &static_param = ctx.static_param_;
  sstable_merge_info_.tenant_id_ = MTL_ID();
  sstable_merge_info_.ls_id_ = ctx.get_ls_id();
  sstable_merge_info_.tablet_id_ = ctx.get_tablet_id();
  sstable_merge_info_.compaction_scn_ = static_param.get_compaction_scn();
  sstable_merge_info_.merge_type_ = ctx.get_inner_table_merge_type();
  sstable_merge_info_.progressive_merge_round_ = static_param.progressive_merge_round_;
  sstable_merge_info_.progressive_merge_num_ = static_param.progressive_merge_num_;
  sstable_merge_info_.concurrent_cnt_ = static_param.concurrent_cnt_;
  sstable_merge_info_.is_full_merge_ = static_param.is_full_merge_;
}

int ObTabletMergeInfo::add_macro_blocks(const ObSSTableMergeInfo &sstable_merge_info)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(sstable_merge_info_.add(sstable_merge_info))) {
    LOG_WARN("failed to add sstable_merge_info", K(ret));
  }
  return ret;
}

int ObTabletMergeInfo::prepare_sstable_builder(const ObITableReadInfo *index_read_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(sstable_builder_.set_index_read_info(index_read_info))) {
    LOG_WARN("failed to init sstable builder", K(ret), KPC(index_read_info));
  }
  return ret;
}

int ObTabletMergeInfo::prepare_index_builder()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(sstable_builder_.prepare_index_builder())) {
    LOG_WARN("failed to init index builder", K(ret));
  }
  return ret;
}

int ObTabletMergeInfo::build_create_sstable_param(const ObBasicTabletMergeCtx &ctx,
                                                  const ObSSTableMergeRes &res,
                                                  const MacroBlockId &bf_macro_id,
                                                  ObTabletCreateSSTableParam &param,
                                                  const ObStorageColumnGroupSchema *cg_schema,
                                                  const int64_t column_group_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx.is_valid() || !res.is_valid() || (nullptr != cg_schema && (!cg_schema->is_valid() || column_group_idx < 0)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge ctx", K(ret), K(ctx), K(res), K(column_group_idx), KPC(cg_schema));
  } else if (OB_UNLIKELY(nullptr != cg_schema && cg_schema->column_cnt_ != res.data_column_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table column cnt is unexpected mismatched!", K(ret), KPC(cg_schema), K(res));
  } else if (OB_FAIL(param.init_for_merge(ctx, res, cg_schema, column_group_idx))) {
    LOG_WARN("fail to init create sstable param for merge",
        K(ret), K(ctx), K(res), KPC(cg_schema), K(column_group_idx));
  } else if (ctx.get_tablet_id().is_ls_tx_data_tablet()) {
      ret = record_start_tx_scn_for_tx_data(ctx, param);
  }
  return ret;
}

int ObTabletMergeInfo::record_start_tx_scn_for_tx_data(const ObBasicTabletMergeCtx &ctx, ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  // set INT64_MAX for invalid check
  param.filled_tx_scn_.set_max();
  const ObTablesHandleArray &tables_handle = ctx.get_tables_handle();
  if (is_mini_merge(ctx.get_merge_type())) {
    // when this merge is MINI_MERGE, use the start_scn of the oldest tx data memtable as start_tx_scn
    ObTxDataMemtable *tx_data_memtable = nullptr;
    if (tables_handle.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tables handle is unexpected empty", KR(ret), K(ctx));
    } else if (OB_ISNULL(tx_data_memtable = (ObTxDataMemtable*)tables_handle.get_table(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table ptr is unexpected nullptr", KR(ret), K(ctx));
    } else {
      param.filled_tx_scn_ = tx_data_memtable->get_start_scn();
    }
  } else if (is_minor_merge(ctx.get_merge_type())) {
    // when this merge is MINOR_MERGE, use max_filtered_end_scn in filter if filtered some tx data
    ObTransStatusFilter *compaction_filter_ = (ObTransStatusFilter*)ctx.info_collector_.compaction_filter_;
    ObSSTableMetaHandle sstable_meta_hdl;
    ObSSTable *oldest_tx_data_sstable = static_cast<ObSSTable *>(tables_handle.get_table(0));
    if (OB_ISNULL(oldest_tx_data_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tx data sstable is unexpected nullptr", KR(ret));
    } else if (OB_FAIL(oldest_tx_data_sstable->get_meta(sstable_meta_hdl))) {
      LOG_WARN("fail to get sstable meta handle", K(ret));
    } else {
      param.filled_tx_scn_ = sstable_meta_hdl.get_sstable_meta().get_filled_tx_scn();

      if (OB_NOT_NULL(compaction_filter_)) {
        // if compaction_filter is valid, update filled_tx_log_ts if recycled some tx data
        SCN recycled_scn;
        if (compaction_filter_->get_max_filtered_end_scn() > SCN::min_scn()) {
          recycled_scn = compaction_filter_->get_max_filtered_end_scn();
        } else {
          recycled_scn = compaction_filter_->get_recycle_scn();
        }
        if (recycled_scn > param.filled_tx_scn_) {
          param.filled_tx_scn_ = recycled_scn;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected merge type when merge tx data table", KR(ret), K(ctx));
  }

  return ret;
}

int ObTabletMergeInfo::create_sstable(
    ObBasicTabletMergeCtx &ctx,
    ObTableHandleV2 &merge_table_handle,
    bool &skip_to_create_empty_cg,
    const ObStorageColumnGroupSchema *cg_schema /* = nullptr */,
    const int64_t column_group_idx /* = 0*/)
{
  int ret = OB_SUCCESS;
  skip_to_create_empty_cg = false;
  bool is_main_table = false;
  const ObTablesHandleArray &tables_handle = ctx.get_tables_handle();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet merge info is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ctx.is_valid() || (nullptr != cg_schema && (!cg_schema->is_valid() || column_group_idx < 0)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge ctx", K(ret), K(ctx), KPC(cg_schema), K(column_group_idx));
  } else if (NULL == cg_schema) {
    // row store mode, do nothing
  } else {
    is_main_table = cg_schema->is_all_column_group() || cg_schema->is_rowkey_column_group(); //"rowkey_cg" and "all_cg" are not allowed to coexist
  }

  if (OB_SUCC(ret)) {
    // if base sstable is small sstable and was reused, we disable the small sstable optimization
    const ObSSTable *sstable = static_cast<const ObSSTable*>(tables_handle.get_table(0));
    const bool is_reused_small_sst = is_major_or_meta_merge_type(ctx.get_merge_type())
                                   && nullptr == cg_schema //row store mode
                                   && sstable->is_small_sstable()
                                   && 1 == sstable_merge_info_.macro_block_count_
                                   && 1 == sstable_merge_info_.multiplexed_macro_block_count_;

    SMART_VARS_2((ObSSTableMergeRes, res), (ObTabletCreateSSTableParam, param)) {
      if (!is_reused_small_sst
          && OB_FAIL(sstable_builder_.build_sstable_merge_res(ctx.static_param_, sstable_merge_info_, res))) {
        LOG_WARN("fail to close index builder", K(ret), KPC(sstable), "is_small_sst", sstable->is_small_sstable());
        CTX_SET_DIAGNOSE_LOCATION(ctx);
      } else if (is_reused_small_sst && OB_FAIL(sstable_builder_.build_reused_small_sst_merge_res(sstable->get_macro_read_size(),
                        sstable->get_macro_offset(), res))) {
        LOG_WARN("fail to close index builder for reused small sstable", K(ret), KPC(sstable));
      } else if (OB_FAIL(build_create_sstable_param(ctx, res, bloomfilter_block_id_, param, cg_schema, column_group_idx))) {
        LOG_WARN("fail to build create sstable param", K(ret));
      } else if (is_main_table) { // should build co sstable
        if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, ctx.mem_ctx_.get_allocator(), merge_table_handle))) {
          LOG_WARN("fail to create sstable", K(ret), K(param));
          CTX_SET_DIAGNOSE_LOCATION(ctx);
        }
      } else if (NULL == cg_schema) { // not co major merge, only need to create one sstable
        if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, ctx.mem_ctx_.get_allocator(), merge_table_handle))) {
          LOG_WARN("fail to create sstable", K(ret), K(param));
          CTX_SET_DIAGNOSE_LOCATION(ctx);
        }
      } else if (NULL != cg_schema && 0 == param.data_blocks_cnt_) { // skip to create normal cg sstable that is empty
        skip_to_create_empty_cg = true;
        FLOG_INFO("skip to create empty cg sstable!", K(ret), K(param), KPC(cg_schema));
      } else { // use tmp allocator to create normal cg sstable due to the concurrent problem
        ObArenaAllocator tmp_allocator("TmpCGSSTable", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
        ObTableHandleV2 tmp_handle;
        ObSSTable *sstable = nullptr;
        ObSSTable *new_sstable = nullptr;
        if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, tmp_allocator, tmp_handle))) {
          LOG_WARN("fail to create sstable", K(ret), K(param));
          CTX_SET_DIAGNOSE_LOCATION(ctx);
        } else if (OB_FAIL(tmp_handle.get_sstable(sstable))) {
          STORAGE_LOG(WARN, "Failed to get sstable", K(ret));
        } else if (OB_FAIL(sstable->deep_copy(ctx.mem_ctx_.get_safe_arena(), new_sstable, true/*transfer macro ref*/))) {
          STORAGE_LOG(WARN, "Failed to deep copy sstable", K(ret));
        } else if (OB_FAIL(ctx.try_set_upper_trans_version(*sstable))) {
          LOG_WARN("failed to set upper trans version", K(ret), K(param));
        } else if (OB_FAIL(merge_table_handle.set_sstable(new_sstable, &ctx.mem_ctx_.get_safe_arena()))) {
          STORAGE_LOG(WARN, "Failed to set sstable", K(ret));
        }
      }

      if (OB_SUCC(ret) && !skip_to_create_empty_cg) {
        FLOG_INFO("succeed to merge sstable", K(param), "table_key", merge_table_handle.get_table()->get_key(),
            KPC(cg_schema), KPC(this));
      }
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
