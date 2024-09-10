/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/iter/ob_das_vid_merge_iter.h"
#include "sql/das/iter/ob_das_iter_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_attach_define.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObDASVIdMergeIterParam::ObDASVIdMergeIterParam()
  : ObDASIterParam(DAS_ITER_VEC_VID_MERGE),
    rowkey_vid_tablet_id_(),
    rowkey_vid_ls_id_(),
    rowkey_vid_iter_(nullptr),
    data_table_iter_(nullptr),
    rowkey_vid_ctdef_(nullptr),
    data_table_ctdef_(nullptr),
    rowkey_vid_rtdef_(nullptr),
    data_table_rtdef_(nullptr),
    trans_desc_(nullptr),
    snapshot_(nullptr)
{}

ObDASVIdMergeIterParam::~ObDASVIdMergeIterParam()
{
}

ObDASVIdMergeIter::ObDASVIdMergeIter()
  : ObDASIter(),
    need_filter_rowkey_vid_(true),
    rowkey_vid_scan_param_(),
    rowkey_vid_iter_(nullptr),
    data_table_iter_(nullptr),
    rowkey_vid_ctdef_(nullptr),
    data_table_ctdef_(nullptr),
    rowkey_vid_rtdef_(nullptr),
    data_table_rtdef_(nullptr),
    rowkey_vid_tablet_id_(),
    rowkey_vid_ls_id_(),
    merge_memctx_()
{
}

ObDASVIdMergeIter::~ObDASVIdMergeIter()
{
}

int ObDASVIdMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkey_vid_iter_) || OB_ISNULL(data_table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, rowkey vid or data table iter is nullptr", K(ret), KP(rowkey_vid_iter_),
        KP(data_table_iter_));
  } else if (OB_FAIL(build_rowkey_vid_range())) {
    LOG_WARN("fail to build rowkey vid range", K(ret));
  } else if (OB_FAIL(data_table_iter_->do_table_scan())) {
    LOG_WARN("fail to do table scan for data table", K(ret), KPC(data_table_iter_));
  } else if (OB_FAIL(rowkey_vid_iter_->do_table_scan())) {
    LOG_WARN("fail to do table scan for rowkey vid", K(ret), KPC(rowkey_vid_iter_));
  }
  LOG_INFO("do table scan", K(ret), K(data_table_iter_->get_scan_param()), K(rowkey_vid_iter_->get_scan_param()));
  return ret;
}

int ObDASVIdMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkey_vid_iter_) || OB_ISNULL(data_table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, rowkey vid or data table iter is nullptr", K(ret), KP(rowkey_vid_iter_),
        KP(data_table_iter_));
  } else if (OB_FAIL(build_rowkey_vid_range())) {
    LOG_WARN("fail to build rowkey vid range", K(ret));
  } else if (OB_FAIL(data_table_iter_->rescan())) {
    LOG_WARN("fail to rescan data table iter", K(ret), KPC(data_table_iter_));
  } else {
    rowkey_vid_scan_param_.tablet_id_ = rowkey_vid_tablet_id_;
    rowkey_vid_scan_param_.ls_id_     = rowkey_vid_ls_id_;
    if (OB_FAIL(rowkey_vid_iter_->rescan())) {
      LOG_WARN("fail to rescan rowkey doc iter", K(ret), KPC(rowkey_vid_iter_));
    }
  }
  LOG_INFO("rescan", K(ret), K(data_table_iter_->get_scan_param()), K(rowkey_vid_iter_->get_scan_param()));
  return ret;
}

void ObDASVIdMergeIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(rowkey_vid_iter_)) {
    rowkey_vid_iter_->clear_evaluated_flag();
  }
  if (OB_NOT_NULL(data_table_iter_)) {
    data_table_iter_->clear_evaluated_flag();
  }
}

int ObDASVIdMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_VEC_VID_MERGE != param.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(ret), K(param));
  } else {
    ObDASVIdMergeIterParam &merge_param = static_cast<ObDASVIdMergeIterParam &>(param);
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), "DocIdMerge", ObCtxIds::DEFAULT_CTX_ID).set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(merge_memctx_, param))) {
      LOG_WARN("failed to create merge memctx", K(ret));
    } else if (OB_FAIL(init_rowkey_vid_scan_param(merge_param.rowkey_vid_tablet_id_, merge_param.rowkey_vid_ls_id_,
            merge_param.rowkey_vid_ctdef_, merge_param.rowkey_vid_rtdef_, merge_param.trans_desc_,
            merge_param.snapshot_))) {
      LOG_WARN("fail to init rowkey vid scan param", K(ret), K(merge_param));
    } else {
      data_table_iter_  = merge_param.data_table_iter_;
      rowkey_vid_iter_  = merge_param.rowkey_vid_iter_;
      data_table_ctdef_ = merge_param.data_table_ctdef_;
      rowkey_vid_ctdef_ = merge_param.rowkey_vid_ctdef_;
      data_table_rtdef_ = merge_param.data_table_rtdef_;
      rowkey_vid_rtdef_ = merge_param.rowkey_vid_rtdef_;
      rowkey_vid_tablet_id_ = merge_param.rowkey_vid_tablet_id_;
      rowkey_vid_ls_id_     = merge_param.rowkey_vid_ls_id_;
      need_filter_rowkey_vid_ = true;
    }
  }
  return ret;
}

int ObDASVIdMergeIter::set_vid_merge_related_ids(
    const ObDASRelatedTabletID &tablet_ids,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_ids.rowkey_vid_tablet_id_.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey doc tablet id or ls id", K(ret), K(tablet_ids.rowkey_vid_tablet_id_), K(ls_id));
  } else {
    rowkey_vid_tablet_id_ = tablet_ids.rowkey_vid_tablet_id_;
    rowkey_vid_ls_id_     = ls_id;
  }
  return ret;
}

int ObDASVIdMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(data_table_iter_) && OB_FAIL(data_table_iter_->reuse())) {
    LOG_WARN("fail to reuse data table iter", K(ret));
  } else if (OB_NOT_NULL(rowkey_vid_iter_)) {
    const ObTabletID old_tablet_id = rowkey_vid_scan_param_.tablet_id_;
    const bool tablet_id_changed = old_tablet_id.is_valid() && old_tablet_id != rowkey_vid_tablet_id_;
    rowkey_vid_scan_param_.need_switch_param_ = rowkey_vid_scan_param_.need_switch_param_ || (tablet_id_changed ? true : false);
    if (OB_FAIL(rowkey_vid_iter_->reuse())) {
      LOG_WARN("fail to reuse rowkey vid iter", K(ret));
    }
  }
  if (OB_NOT_NULL(merge_memctx_)) {
    merge_memctx_->reset_remain_one_page();
  }
  return ret;
}

int ObDASVIdMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(merge_memctx_)) {
    DESTROY_CONTEXT(merge_memctx_);
    merge_memctx_ = nullptr;
  }
  rowkey_vid_scan_param_.destroy_schema_guard();
  rowkey_vid_scan_param_.snapshot_.reset();
  rowkey_vid_scan_param_.destroy();
  data_table_iter_ = nullptr;
  rowkey_vid_iter_ = nullptr;
  need_filter_rowkey_vid_ = true;
  return ret;
}

int ObDASVIdMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_iter_) || OB_ISNULL(rowkey_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data table or rowkey vid iter is nullptr", K(ret), KP(data_table_iter_),
        K(rowkey_vid_iter_));
  } else if (!need_filter_rowkey_vid_) {
    if (OB_FAIL(concat_row())) {
      LOG_WARN("fail to concat data table and rowkey vid row", K(ret));
    }
  } else if (OB_FAIL(sorted_merge_join_row())) {
    LOG_WARN("fail to sorted merge join data table and rowkey vid row", K(ret));
  }
  LOG_TRACE("inner get next row", K(ret));
  return ret;
}

int ObDASVIdMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_iter_) || OB_ISNULL(rowkey_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data table or rowkey vid iter is nullptr", K(ret), KP(data_table_iter_),
        K(rowkey_vid_iter_));
  } else if (!need_filter_rowkey_vid_) {
    if (OB_FAIL(concat_rows(count, capacity))) {
      LOG_WARN("fail to concat data table and rowkey vid rows", K(ret));
    }
  } else if (OB_FAIL(sorted_merge_join_rows(count, capacity))) {
    LOG_WARN("fail to sorted merge join data table and rowkey vid rows", K(ret));
  }
  LOG_TRACE("inner get next rows", K(ret), K(count), K(capacity));
  return ret;
}

int ObDASVIdMergeIter::init_rowkey_vid_scan_param(
    const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    transaction::ObTxDesc *trans_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  rowkey_vid_scan_param_.tenant_id_ = tenant_id;
  rowkey_vid_scan_param_.key_ranges_.set_attr(ObMemAttr(tenant_id, "SParamKR"));
  rowkey_vid_scan_param_.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "SParamSSKR"));
  if (OB_UNLIKELY(!tablet_id.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef or rtdef", K(ret), KPC(ctdef), KPC(rtdef));
  } else {
    rowkey_vid_scan_param_.tablet_id_ = tablet_id;
    rowkey_vid_scan_param_.ls_id_ = ls_id;
    rowkey_vid_scan_param_.scan_allocator_ = &get_arena_allocator();
    rowkey_vid_scan_param_.allocator_ = &rtdef->stmt_allocator_;
    rowkey_vid_scan_param_.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    rowkey_vid_scan_param_.index_id_ = ctdef->ref_table_id_;
    rowkey_vid_scan_param_.is_get_ = ctdef->is_get_;
    rowkey_vid_scan_param_.is_for_foreign_check_ = rtdef->is_for_foreign_check_;
    rowkey_vid_scan_param_.timeout_ = rtdef->timeout_ts_;
    rowkey_vid_scan_param_.scan_flag_ = rtdef->scan_flag_;
    rowkey_vid_scan_param_.reserved_cell_count_ = ctdef->access_column_ids_.count();
    rowkey_vid_scan_param_.sql_mode_ = rtdef->sql_mode_;
    rowkey_vid_scan_param_.frozen_version_ = rtdef->frozen_version_;
    rowkey_vid_scan_param_.force_refresh_lc_ = rtdef->force_refresh_lc_;
    rowkey_vid_scan_param_.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    rowkey_vid_scan_param_.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    rowkey_vid_scan_param_.ext_file_column_exprs_ = &(ctdef->pd_expr_spec_.ext_file_column_exprs_);
    rowkey_vid_scan_param_.ext_column_convert_exprs_ = &(ctdef->pd_expr_spec_.ext_column_convert_exprs_);
    rowkey_vid_scan_param_.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    rowkey_vid_scan_param_.table_param_ = &(ctdef->table_param_);
    rowkey_vid_scan_param_.op_ = rtdef->p_pd_expr_op_;
    rowkey_vid_scan_param_.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    rowkey_vid_scan_param_.schema_version_ = ctdef->schema_version_;
    rowkey_vid_scan_param_.tenant_schema_version_ = rtdef->tenant_schema_version_;
    rowkey_vid_scan_param_.limit_param_ = rtdef->limit_param_;
    rowkey_vid_scan_param_.need_scn_ = rtdef->need_scn_;
    rowkey_vid_scan_param_.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    rowkey_vid_scan_param_.fb_snapshot_ = rtdef->fb_snapshot_;
    rowkey_vid_scan_param_.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    if (rtdef->is_for_foreign_check_) {
      rowkey_vid_scan_param_.trans_desc_ = trans_desc;
    }
    if (OB_NOT_NULL(snapshot)) {
      rowkey_vid_scan_param_.snapshot_ = *snapshot;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null snapshot", K(ret), KPC(ctdef), KPC(rtdef));
    }
    if (OB_NOT_NULL(trans_desc)) {
      rowkey_vid_scan_param_.tx_id_ = trans_desc->get_tx_id();
    } else {
      rowkey_vid_scan_param_.tx_id_.reset();
    }
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      rowkey_vid_scan_param_.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    rowkey_vid_scan_param_.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_FAIL(rowkey_vid_scan_param_.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to assign column ids", K(ret));
    }
    if (rtdef->sample_info_ != nullptr) {
      rowkey_vid_scan_param_.sample_info_ = *rtdef->sample_info_;
    }
  }

  LOG_INFO("init rowkey vid table scan param finished", K(rowkey_vid_scan_param_), K(ret));
  return ret;
}

int ObDASVIdMergeIter::build_rowkey_vid_range()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_iter_) || OB_ISNULL(data_table_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, data table iter or ctdef is nullptr", K(ret), KP(data_table_iter_), KP(data_table_ctdef_));
  } else {
    const common::ObIArray<common::ObNewRange> &key_ranges = data_table_iter_->get_scan_param().key_ranges_;
    const common::ObIArray<common::ObNewRange> &ss_key_ranges = data_table_iter_->get_scan_param().ss_key_ranges_;
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
      ObNewRange key_range = key_ranges.at(i);
      key_range.table_id_ = rowkey_vid_scan_param_.index_id_;
      if (OB_FAIL(rowkey_vid_scan_param_.key_ranges_.push_back(key_range))) {
        LOG_WARN("fail to push back key range for rowkey vid scan param", K(ret), K(key_range));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ss_key_ranges.count(); ++i) {
      ObNewRange ss_key_range = ss_key_ranges.at(i);
      ss_key_range.table_id_ = rowkey_vid_scan_param_.index_id_;
      if (OB_FAIL(rowkey_vid_scan_param_.ss_key_ranges_.push_back(ss_key_range))) {
        LOG_WARN("fail to push back ss key range for rowkey vid scan param", K(ret), K(ss_key_range));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const ObExprPtrIArray *op_filters = data_table_iter_->get_scan_param().op_filters_;
    if (OB_ISNULL(op_filters) || (OB_NOT_NULL(op_filters) && op_filters->empty())) {
      need_filter_rowkey_vid_ = false;
    } else {
      need_filter_rowkey_vid_ = true;
    }
    rowkey_vid_scan_param_.sample_info_ = data_table_iter_->get_scan_param().sample_info_;
  }
  LOG_INFO("build rowkey vid range", K(ret), K(need_filter_rowkey_vid_), K(rowkey_vid_scan_param_.key_ranges_),
      K(rowkey_vid_scan_param_.ss_key_ranges_), K(rowkey_vid_scan_param_.sample_info_));
  return ret;
}

int ObDASVIdMergeIter::concat_row()
{
  int ret = OB_SUCCESS;
  int64_t vid_id;
  if (OB_FAIL(data_table_iter_->get_next_row())) {
    if (OB_ITER_END == ret) {
      if (OB_FAIL(rowkey_vid_iter_->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next rows", K(ret));
        }
      } else {
        ObArenaAllocator allocator("RowkeyVid");
        common::ObRowkey rowkey;
        if (OB_FAIL(get_rowkey(allocator, rowkey_vid_ctdef_, rowkey_vid_rtdef_, rowkey))) {
          LOG_WARN("fail to process_data_table_rowkey", K(ret));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row count isn't equal between data table and rowkey doc", K(ret), K(rowkey),
              K(rowkey_vid_iter_->get_scan_param()), K(data_table_iter_->get_scan_param()));
        }
      }
    } else {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_FAIL(rowkey_vid_iter_->get_next_row())) {
    LOG_WARN("fail to get next row", K(ret));
  } else if (OB_FAIL(get_vid_id(rowkey_vid_ctdef_, rowkey_vid_rtdef_, vid_id))) {
    LOG_WARN("fail to get vid id", K(ret));
  } else if (OB_FAIL(fill_vid_id_in_data_table(vid_id))) {
    LOG_WARN("fail to fill vid id in data table", K(ret), K(vid_id));
  }
  return ret;
}

int ObDASVIdMergeIter::concat_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t data_row_cnt = 0;
  int64_t rowkey_vid_row_cnt = 0;
  common::ObArray<int64_t> vid_ids;
  bool need_fill_vids = false;
  if (OB_FAIL(data_table_iter_->get_next_rows(data_row_cnt, capacity))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  if (OB_FAIL(ret) && ret != OB_ITER_END) {
  } else { // whatever succ or iter_end, we should get from rowkey_vid_iter
    bool expect_iter_end = (ret == OB_ITER_END);
    int real_cap = (data_row_cnt > 0 && !expect_iter_end) ? data_row_cnt : capacity;
    ret = OB_SUCCESS; // recover ret from iter end
    while (OB_SUCC(ret) && (real_cap > 0 || expect_iter_end)) {
      rowkey_vid_row_cnt = 0;
      if (OB_FAIL(rowkey_vid_iter_->get_next_rows(rowkey_vid_row_cnt, real_cap))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get next row", K(ret), K(data_row_cnt), K(real_cap), K(vid_ids));
        }
      }
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
      } else if (rowkey_vid_row_cnt > 0) {
        const int tmp_ret = ret;
        if (OB_FAIL(get_vid_ids(rowkey_vid_row_cnt, rowkey_vid_ctdef_, rowkey_vid_rtdef_, vid_ids))) {
          LOG_WARN("fail to get vid ids", K(ret), K(count));
        } else {
          ret = tmp_ret;
        }
      }
      real_cap -= rowkey_vid_row_cnt;
    }
    if (expect_iter_end && ret != OB_ITER_END) {
      int tmp_ret = ret;
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row count isn't equal between data table and rowkey vid",
                K(ret), K(tmp_ret), K(capacity), K(vid_ids.count()), K(data_row_cnt));
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
  } else if (OB_UNLIKELY(data_row_cnt != vid_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The row count of data table isn't equal to rowkey vid", K(ret), K(data_row_cnt), K(vid_ids),
        K(data_table_iter_->get_scan_param()), K(rowkey_vid_iter_->get_scan_param()));
  } else {
    count = data_row_cnt;
    if (count > 0 && data_table_ctdef_->vec_vid_idx_ != -1) {
      const int tmp_ret = ret;
      if (OB_FAIL(fill_vid_ids_in_data_table(vid_ids))) {
        LOG_WARN("fail to fill vid ids in data table", K(ret), K(tmp_ret), K(vid_ids));
      } else {
        ret = tmp_ret;
      }
    }
  }
  LOG_TRACE("concat rows in data table and rowkey vid", K(ret), K(data_row_cnt), K(vid_ids), K(count),
      K(capacity));
  return ret;
}

int ObDASVIdMergeIter::sorted_merge_join_row()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("DocIdMergeRow");
  common::ObRowkey data_table_rowkey;
  if (OB_FAIL(data_table_iter_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next data table row", K(ret));
  } else if (OB_ITER_END == ret) {
    while (OB_SUCC(rowkey_vid_iter_->get_next_row()));
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next rowkey vid row", K(ret));
    }
  } else if (OB_FAIL(get_rowkey(allocator, data_table_ctdef_, data_table_rtdef_, data_table_rowkey))) {
    LOG_WARN("fail to get data table rowkey", K(ret));
  } else {
    int64_t vid_id;
    bool is_found = false;
    while (OB_SUCC(ret) && !is_found) {
      common::ObRowkey rowkey_vid_rowkey;
      if (OB_FAIL(rowkey_vid_iter_->get_next_row())) {
        LOG_WARN("fail to get next rowkey vid row", K(ret));
      } else if (OB_FAIL(get_rowkey(allocator, rowkey_vid_ctdef_, rowkey_vid_rtdef_, rowkey_vid_rowkey))) {
        LOG_WARN("fail to get rowkey vid rowkey");
      } else if (rowkey_vid_rowkey.equal(data_table_rowkey, is_found)) {
        LOG_WARN("fail to equal rowkey between data table and rowkey", K(ret));
      }
      LOG_TRACE("compare one row in rowkey vid", K(ret), "need_skip=", !is_found, K(data_table_rowkey),
          K(rowkey_vid_rowkey));
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, The row count of data table isn't equal to rowkey vid", K(ret));
      }
    } else if (OB_FAIL(get_vid_id(rowkey_vid_ctdef_, rowkey_vid_rtdef_, vid_id))) {
      LOG_WARN("fail to get vid id", K(ret));
    } else if (OB_FAIL(fill_vid_id_in_data_table(vid_id))) {
      LOG_WARN("fail to fill vid id in data table", K(ret), K(vid_id));
    }
  }
  return ret;
}

int ObDASVIdMergeIter::sorted_merge_join_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("DocIdMergeRows");
  common::ObArray<common::ObRowkey> rowkeys_in_data_table;
  common::ObArray<int64_t> vid_ids;
  bool is_iter_end = false;
  int64_t data_table_cnt = 0;
  if (OB_FAIL(data_table_iter_->get_next_rows(data_table_cnt, capacity)) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next data table rows", K(ret), K(data_table_cnt), K(capacity), KPC(data_table_iter_));
  } else if (0 == data_table_cnt && OB_ITER_END == ret) {
    count = 0;
  } else if (OB_UNLIKELY(0 == data_table_cnt && OB_SUCCESS == ret)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data table row count is 0, but ret code is success", K(ret), KPC(data_table_iter_));
  } else if (OB_ITER_END == ret && FALSE_IT(is_iter_end = true)) {
  } else if (OB_FAIL(get_rowkeys(data_table_cnt, allocator, data_table_ctdef_, data_table_rtdef_,
          rowkeys_in_data_table))) {
    LOG_WARN("fail to get data table rowkeys", K(ret), K(data_table_cnt));
  } else {
    const int64_t batch_size = is_iter_end ? capacity : data_table_cnt;
    int64_t remain_cnt = data_table_cnt;
    int64_t rowkey_vid_cnt = 0;
    while (OB_SUCC(ret) && remain_cnt > 0) {
      common::ObArray<common::ObRowkey> rowkeys_in_rowkey_vid;
      common::ObArray<int64_t> vid_ids_in_rowkey_vid;
      if (OB_FAIL(rowkey_vid_iter_->get_next_rows(rowkey_vid_cnt, batch_size)) && OB_ITER_END != ret) {
        LOG_WARN("fail to get next rowkey vid rows", K(ret), K(remain_cnt),  K(batch_size), K(rowkey_vid_iter_));
      } else if (OB_UNLIKELY(OB_ITER_END == ret && (!is_iter_end || 0 == rowkey_vid_cnt))){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, iter end is reached at rowkey vid, but not at data table", K(ret), K(is_iter_end),
            K(rowkey_vid_cnt));
      } else if (OB_FAIL(get_rowkeys_and_vid_ids(rowkey_vid_cnt, allocator, rowkey_vid_ctdef_, rowkey_vid_rtdef_,
              rowkeys_in_rowkey_vid, vid_ids_in_rowkey_vid))) {
        LOG_WARN("fail to get rowkey vid rowkeys", K(ret), K(rowkey_vid_cnt));
      } else {
        for (int64_t i = data_table_cnt - remain_cnt, j = 0;
             OB_SUCC(ret) && i < data_table_cnt && j < rowkeys_in_rowkey_vid.count();
             ++j) {
          bool is_equal = false;
          LOG_INFO("compare one row in rowkey vid", K(ret), K(i), K(j), K(rowkeys_in_data_table.at(i)),
              K(rowkeys_in_rowkey_vid.at(j)));
          if (rowkeys_in_rowkey_vid.at(j).equal(rowkeys_in_data_table.at(i), is_equal)) {
            LOG_WARN("fail to equal rowkey between data table and rowkey", K(ret));
          } else if (is_equal) {
            if (OB_FAIL(vid_ids.push_back(vid_ids_in_rowkey_vid.at(j)))) {
              LOG_WARN("fail to push back vid id", K(ret), K(j), K(vid_ids_in_rowkey_vid));
            } else {
              --remain_cnt;
              ++i;
              LOG_INFO("find vid id in rowkey vid", K(vid_ids_in_rowkey_vid.at(j)), K(remain_cnt), K(i), K(data_table_cnt));
            }
          }
        }
      }
    }
    if (FAILEDx(fill_vid_ids_in_data_table(vid_ids))) {
      LOG_WARN("fail to fill vid ids in data table", K(ret), K(vid_ids));
    } else {
      count = data_table_cnt;
      ret = is_iter_end ? OB_ITER_END : ret;
    }
  }
  return ret;
}

int ObDASVIdMergeIter::get_rowkey(
    common::ObIAllocator &allocator,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ctdef), KP(rtdef));
  } else {
    const int64_t rowkey_cnt = ctdef->table_param_.get_read_info().get_schema_rowkey_count();
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate rowkey obj buffer", K(ret), K(rowkey_cnt));
    } else {
      ObObj *obj_ptr = new (buf) ObObj[rowkey_cnt];
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
        ObExpr *expr = ctdef->result_output_.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, expr is nullptr", K(ret), K(i), KPC(ctdef));
        } else if (T_PSEUDO_GROUP_ID == expr->type_) {
          // nothing to do.
        } else {
          ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
          if (OB_FAIL(datum.to_obj(obj_ptr[i], expr->obj_meta_, expr->obj_datum_map_))) {
            LOG_WARN("fail to convert datum to obj", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        rowkey.assign(obj_ptr, rowkey_cnt);
      }
    }
  }
  return ret;
}

int ObDASVIdMergeIter::get_vid_id(
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    int64_t &vid_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ctdef), KP(rtdef));
  } else {
    const int64_t rowkey_cnt = ctdef->table_param_.get_read_info().get_schema_rowkey_count();
    const int64_t extern_size = ctdef->trans_info_expr_ != nullptr ? 1 : 0;
    ObExpr *expr = nullptr;
    if (GCONF.enable_strict_defensive_check()) {
      if (OB_UNLIKELY(ctdef->result_output_.count() != rowkey_cnt + 1 + extern_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected result output column count", K(ret), K(rowkey_cnt), K(ctdef->result_output_.count()));
      }
    } else if (OB_UNLIKELY(ctdef->result_output_.count() != rowkey_cnt + 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected result output column count", K(ret), K(rowkey_cnt),
          K(ctdef->result_output_.count()));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(expr = ctdef->result_output_.at(rowkey_cnt))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, vid id expr is nullptr", K(ret), K(rowkey_cnt), K(ctdef->result_output_));
    } else {
      ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
      vid_id = datum.get_int();
      // if (OB_FAIL(vid_id.from_string(datum.get_string()))) {
      //   LOG_WARN("fail to get vid id", K(ret), K(datum));
      // }
    }
  }
  return ret;
}

int ObDASVIdMergeIter::get_rowkeys(
    const int64_t size,
    common::ObIAllocator &allocator,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObIArray<common::ObRowkey> &rowkeys)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*rtdef->eval_ctx_);
  batch_info_guard.set_batch_size(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    batch_info_guard.set_batch_idx(i);
    common::ObRowkey rowkey;
    if (OB_FAIL(get_rowkey(allocator, ctdef, rtdef, rowkey))) {
      LOG_WARN("fail to process_data_table_rowkey", K(ret), K(i));
    } else if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("fail to push back rowkey", K(ret), K(rowkey));
    }
  }
  return ret;
}

int ObDASVIdMergeIter::get_vid_ids(
    const int64_t size,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObIArray<int64_t> &vid_ids)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*rtdef->eval_ctx_);
  batch_info_guard.set_batch_size(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    batch_info_guard.set_batch_idx(i);
    int64_t vid_id;
    if (OB_FAIL(get_vid_id(ctdef, rtdef, vid_id))) {
      LOG_WARN("fail to get vid id", K(ret), K(i));
    } else if (OB_FAIL(vid_ids.push_back(vid_id))) {
      LOG_WARN("fail to push back vid id", K(ret), K(vid_id));
    } else {
      LOG_INFO("[vec index debug]get one vid ", K(vid_id));
    }
  }
  return ret;
}

int ObDASVIdMergeIter::get_rowkeys_and_vid_ids(
    const int64_t size,
    common::ObIAllocator &allocator,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObIArray<common::ObRowkey> &rowkeys,
    common::ObIArray<int64_t> &vid_ids)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*rtdef->eval_ctx_);
  batch_info_guard.set_batch_size(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    batch_info_guard.set_batch_idx(i);
    common::ObRowkey rowkey;
    int64_t vid_id;
    if (OB_FAIL(get_rowkey(allocator, ctdef, rtdef, rowkey))) {
      LOG_WARN("fail to process_data_table_rowkey", K(ret), K(i));
    } else if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("fail to push back rowkey", K(ret), K(rowkey));
    } else if (OB_FAIL(get_vid_id(ctdef, rtdef, vid_id))) {
      LOG_WARN("fail to get vid id", K(ret), K(i));
    } else if (OB_FAIL(vid_ids.push_back(vid_id))) {
      LOG_WARN("fail to push back vid id", K(ret), K(vid_id));
    }
  }
  return ret;
}

int ObDASVIdMergeIter::fill_vid_id_in_data_table(const int64_t &vid_id)
{
  int ret = OB_SUCCESS;
  // if (OB_UNLIKELY(!vid_id.is_valid())) {
  //   ret = OB_INVALID_ARGUMENT;
  //   LOG_WARN("invalid arguments", K(ret), K(vid_id));
  // } else
  if (OB_ISNULL(data_table_ctdef_) || OB_ISNULL(data_table_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, data table ctdef is nullptr", K(ret), KP(data_table_ctdef_), KP(data_table_rtdef_));
  } else {
    const int64_t vid_id_idx = data_table_ctdef_->vec_vid_idx_;
    ObExpr *vid_id_expr = nullptr;
    if (OB_UNLIKELY(vid_id_idx >= data_table_ctdef_->result_output_.count() || vid_id_idx < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid vid id idx", K(ret), K(vid_id_idx), K(data_table_ctdef_->result_output_.count()));
    } else if (OB_ISNULL(vid_id_expr = data_table_ctdef_->result_output_.at(vid_id_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpeted error, vid id expr is nullptr", K(ret), K(vid_id_idx), KPC(data_table_ctdef_));
    } else {
      const uint64_t buf_len = sizeof(int64_t);
      ObDatum &datum = vid_id_expr->locate_datum_for_write(*data_table_rtdef_->eval_ctx_);
      void *buf = static_cast<void *>(vid_id_expr->get_str_res_mem(*data_table_rtdef_->eval_ctx_, buf_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), KP(buf));
      } else {
        // ObDocId *vid_id_ptr = new (buf) ObDocId(vid_id);
        datum.set_int(vid_id);
        vid_id_expr->set_evaluated_projected(*data_table_rtdef_->eval_ctx_);
        LOG_INFO("Doc id merge fill a vidument id", K(vid_id));
      }
      if (OB_SUCC(ret)) {
      }
    }
  }
  return ret;

}

int ObDASVIdMergeIter::fill_vid_ids_in_data_table(const common::ObIArray<int64_t> &vid_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == vid_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(vid_ids));
  } else if (OB_ISNULL(data_table_ctdef_) || OB_ISNULL(data_table_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, data table ctdef is nullptr", K(ret), KP(data_table_ctdef_), KP(data_table_rtdef_));
  } else {
    const uint64_t len_of_vid_id = sizeof(int64_t);
    const uint64_t len_of_buf = len_of_vid_id * vid_ids.count();
    const int64_t vid_id_idx = data_table_ctdef_->vec_vid_idx_;
    ObExpr *vid_id_expr = nullptr;
    ObDatum *datums = nullptr;
    char *buf = nullptr;
    if (OB_UNLIKELY(vid_id_idx >= data_table_ctdef_->result_output_.count() || vid_id_idx < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid vid id idx", K(ret), K(vid_id_idx), K(data_table_ctdef_->result_output_.count()));
    } else if (OB_ISNULL(vid_id_expr = data_table_ctdef_->result_output_.at(vid_id_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, vid id expr is nullptr", K(ret), K(vid_id_idx), KPC(data_table_ctdef_));
    } else if (OB_ISNULL(datums = vid_id_expr->locate_datums_for_update(*data_table_rtdef_->eval_ctx_, vid_ids.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(vid_id_expr));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < vid_ids.count(); ++i) {
        datums[i].set_int(vid_ids.at(i));
      }
      if (OB_SUCC(ret)) {
        vid_id_expr->set_evaluated_projected(*data_table_rtdef_->eval_ctx_);
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase