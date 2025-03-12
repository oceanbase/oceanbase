//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_progressive_merge_helper.h"
#include "storage/compaction/ob_partition_merger.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
namespace compaction
{
ERRSIM_POINT_DEF(EN_CO_MERGE_REUSE_MICRO);

ObProgressiveMergeMgr::ObProgressiveMergeMgr()
  : progressive_merge_round_(0),
    progressive_merge_num_(0),
    progressive_merge_step_(0),
    data_version_(0),
    finish_cur_round_(false),
    is_inited_(false)
{}

void ObProgressiveMergeMgr::reset()
{
  is_inited_ = false;
  progressive_merge_round_ = 0;
  progressive_merge_num_ = 0;
  progressive_merge_step_ = 0;
  data_version_ = 0;
  finish_cur_round_ = false;
}

int ObProgressiveMergeMgr::init(
  const ObTabletID &tablet_id, // for print log
  const bool is_full_merge,
  const ObSSTableBasicMeta &base_meta,
  const ObStorageSchema &schema,
  const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObProgressiveMergeMgr is inited before", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!base_meta.is_valid() || !schema.is_valid() || 0 == data_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(base_meta), K(schema), K(data_version));
  } else {
    const int64_t meta_progressive_merge_round = base_meta.progressive_merge_round_;
    const int64_t schema_progressive_merge_round = schema.get_progressive_merge_round();

    if (OB_UNLIKELY(meta_progressive_merge_round > schema_progressive_merge_round)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("progressive round on schema is less than meta", KR(ret), K(meta_progressive_merge_round),
        K(schema_progressive_merge_round));
    } else if (INIT_PROGRESSIVE_MERGE_ROUND == schema_progressive_merge_round) { // no progressive
      progressive_merge_num_ = 0;
      progressive_merge_step_ = 0;
    } else if (FALSE_IT(progressive_merge_num_ = (0 == schema.get_progressive_merge_num() ? OB_AUTO_PROGRESSIVE_MERGE_NUM : schema.get_progressive_merge_num()))) {
    } else if (is_full_merge) { // end cur round
      progressive_merge_step_ = progressive_merge_num_;
    } else if (meta_progressive_merge_round < schema_progressive_merge_round) { // new round
      progressive_merge_step_ = 0;
    } else if (meta_progressive_merge_round == schema_progressive_merge_round) {
      // when create new major, will set step+1 on sstable meta, means one step forward
      progressive_merge_step_ = base_meta.progressive_merge_step_;
    }
    if (OB_SUCC(ret)) {
      data_version_ = data_version;
      progressive_merge_round_ = schema_progressive_merge_round;
      finish_cur_round_ = true; // init cur progressive round finish, until some job report unfinish
      is_inited_ = true;
      // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
      FLOG_INFO("Calc progressive param", K(tablet_id), KPC(this));
    }
  }
  return ret;
}

int64_t ObProgressiveMergeMgr::get_result_progressive_merge_step(
    const ObTabletID &tablet_id,
    const int64_t column_group_idx) const // parameters are used for print log
{
  int64_t result_step = MIN(progressive_merge_num_, progressive_merge_step_ + 1);

  if (data_version_ >= DATA_VERSION_4_3_3_0 && finish_cur_round_ && result_step < progressive_merge_num_) {
    if (0 == column_group_idx) { // only print once
      FLOG_INFO("finish cur progressive_merge_round", K(tablet_id), K(result_step), K_(progressive_merge_round),
        K_(progressive_merge_step), K_(progressive_merge_num));
    }
    result_step = progressive_merge_num_; // this result will be used to update pregressive merge step on sstable meta
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("merge_errsim", "progressive_merge_finish",
                          "tablet_id", tablet_id.id(),
                          "progressive_merge_round", progressive_merge_round_,
                          "progressive_merge_step", result_step,
                          "progressive_merge_num", progressive_merge_num_);
#endif
  }
  return result_step;
}

void ObProgressiveMergeMgr::mark_progressive_round_unfinish()
{
  ATOMIC_SET(&finish_cur_round_, false);
}
/*
 *ObProgressiveMergeHelper
 */

void ObProgressiveMergeHelper::reset()
{
  is_inited_ = false;
  mgr_ = NULL;
  rewrite_block_cnt_ = 0;
  need_rewrite_block_cnt_ = 0;
  data_version_ = 0;
  full_merge_ = false;
  check_macro_need_merge_ = false;
}

int ObProgressiveMergeHelper::init(
  const ObSSTable &sstable,
  const ObMergeParameter &merge_param,
  ObProgressiveMergeMgr *mgr)
{
  int ret = OB_SUCCESS;
  const ObStaticMergeParam &static_param = merge_param.static_param_;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObProgressiveMergeHelper init twice", K(ret));
  } else if (OB_UNLIKELY(NULL != mgr && !mgr->is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input progressive mgr is invalid", KR(ret), KPC(mgr));
  } else if (FALSE_IT(reset())) {
  } else if (static_param.for_unittest_) {
  } else if (static_param.is_full_merge_) {
    full_merge_ = check_macro_need_merge_ = true;
  } else if (merge_param.is_mv_merge()) {
    STORAGE_LOG(INFO, "mv merge, not init progressive merge", K(ret));
  } else {
    mgr_ = mgr; // init mgr first
    int64_t rewrite_macro_cnt = 0, reduce_macro_cnt = 0, rewrite_block_cnt_for_progressive = 0;

    if (OB_FAIL(collect_macro_info(sstable, merge_param, rewrite_macro_cnt, reduce_macro_cnt, rewrite_block_cnt_for_progressive))) {
      LOG_WARN("Fail to scan secondary meta", K(ret), K(merge_param));
    } else if (need_calc_progressive_merge()) {
      if (rewrite_block_cnt_for_progressive > 0) {
        need_rewrite_block_cnt_ = MAX(rewrite_block_cnt_for_progressive /
            (mgr->get_progressive_merge_num() - mgr->get_progressive_merge_step()), 1L);
        FLOG_INFO("There are some macro block need rewrite", "tablet_id", static_param.get_tablet_id(),
            K(rewrite_block_cnt_for_progressive), K(need_rewrite_block_cnt_), KPC(mgr), K(table_idx_));
      } else {
        need_rewrite_block_cnt_ = 0;
      }
    }
    if (OB_FAIL(ret)) {
      reset();
    } else {
      check_macro_need_merge_ = rewrite_macro_cnt <= (reduce_macro_cnt * 2);
      if (static_param.data_version_ < DATA_VERSION_4_3_2_0
          && sstable.is_normal_cg_sstable() && rewrite_macro_cnt < CG_TABLE_CHECK_REWRITE_CNT_) {
        check_macro_need_merge_ = true;
      }
      FLOG_INFO("finish macro block need merge check", "tablet_id", static_param.get_tablet_id(), K(check_macro_need_merge_), K(rewrite_macro_cnt), K(reduce_macro_cnt), K(table_idx_));
    }
  }

  if (OB_SUCC(ret)) {
    data_version_ = static_param.data_version_;
    is_inited_ = true;
  }
  return ret;
}

int ObProgressiveMergeHelper::collect_macro_info(
    const blocksstable::ObSSTable &sstable,
    const ObMergeParameter &merge_param,
    int64_t &rewrite_macro_cnt,
    int64_t &reduce_macro_cnt,
    int64_t &rewrite_block_cnt_for_progressive)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  tmp_allocator.set_attr(ObMemAttr(MTL_ID(), "ProgMacroIter"));
  ObSSTableSecMetaIterator *sec_meta_iter = nullptr;
  ObDataMacroBlockMeta macro_meta;
  bool last_is_small_data_macro = false;
  const int64_t compare_progressive_merge_round = get_compare_progressive_round();
  if (OB_FAIL(open_macro_iter(sstable, merge_param, tmp_allocator, sec_meta_iter))) {
    LOG_WARN("Fail to scan secondary meta", K(ret), K(merge_param));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sec_meta_iter->get_next(macro_meta))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Failed to get next macro block", K(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (macro_meta.val_.progressive_merge_round_ < compare_progressive_merge_round) {
      ++rewrite_block_cnt_for_progressive;
    }
    if (macro_meta.val_.data_zsize_ < REWRITE_MACRO_SIZE_THRESHOLD) {
      rewrite_macro_cnt++;
      if (last_is_small_data_macro) {
        reduce_macro_cnt++;
      }
      last_is_small_data_macro = true;
    } else {
      last_is_small_data_macro = false;
    }
  } // while
  if (OB_NOT_NULL(sec_meta_iter)) {
    sec_meta_iter->~ObSSTableSecMetaIterator();
    tmp_allocator.free(sec_meta_iter);
  }
  return ret;
}

int ObProgressiveMergeHelper::open_macro_iter(
    const blocksstable::ObSSTable &sstable,
    const ObMergeParameter &merge_param,
    ObIAllocator &allocator,
    ObSSTableSecMetaIterator *&sec_meta_iter)
{
  int ret = OB_SUCCESS;
  const ObStaticMergeParam &static_param = merge_param.static_param_;
  const storage::ObITableReadInfo *index_read_info = nullptr;
  if (sstable.is_normal_cg_sstable()) {
    if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
      LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", KR(ret));
    }
  } else {
    index_read_info = static_param.rowkey_read_info_;
  }
  const ObDatumRange &merge_range = sstable.is_normal_cg_sstable() ? merge_param.merge_rowid_range_ : merge_param.merge_range_;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(index_read_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index read info is unexpected null", KR(ret), KP(index_read_info), K(sstable), K(merge_param));
  } else if (OB_FAIL(sstable.scan_secondary_meta(
          allocator,
          merge_range,
          *index_read_info,
          DATA_BLOCK_META,
          sec_meta_iter))) {
    LOG_WARN("Fail to scan secondary meta", K(ret), K(merge_range));
  }
  return ret;
}

int ObProgressiveMergeHelper::check_macro_block_op(const ObMacroBlockDesc &macro_desc,
                                                   ObMacroBlockOp &block_op)
{
  int ret = OB_SUCCESS;

  block_op.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObProgressiveMergeHelper not init", K(ret));
  } else if (!macro_desc.is_valid_with_macro_meta()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid macro desc", K(ret), K(macro_desc));
  } else if (full_merge_) {
    block_op.set_rewrite();
  } else {
    const int64_t block_merge_round = macro_desc.macro_meta_->val_.progressive_merge_round_;
    const int64_t compare_progressive_merge_round = get_compare_progressive_round();
    if (need_rewrite_block_cnt_ > 0) {
      if (need_rewrite_block_cnt_ > rewrite_block_cnt_ && block_merge_round < compare_progressive_merge_round) {
        block_op.set_rewrite();
        rewrite_block_cnt_++;
      }
    }
    if (block_op.is_none() && check_macro_need_merge_) {
      bool need_set_block_op = macro_desc.macro_meta_->val_.data_zsize_ < REWRITE_MACRO_SIZE_THRESHOLD;
#ifdef ERRSIM
      if (OB_UNLIKELY(EN_CO_MERGE_REUSE_MICRO)) {
        ret = OB_SUCCESS;
        need_set_block_op = true;
        FLOG_INFO("ERRSIM EN_CO_MERGE_REUSE_MICRO", KR(ret), K(need_set_block_op));
      }
#endif
      if (!need_set_block_op) {
      } else if (data_version_ < DATA_VERSION_4_3_2_0) {
        block_op.set_rewrite();
      } else {
        block_op.set_reorg();
      }
    }
  }
  return ret;
}

void ObProgressiveMergeHelper::end() const
{
  if (need_calc_progressive_merge() && need_rewrite_block_cnt_ > 0) { // have check mgr_ ptr in need_calc_progressive_merge
    mgr_->mark_progressive_round_unfinish();
  }
}

} // namespace compaction
} // namespace oceanbase
