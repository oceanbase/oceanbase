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

#include "sql/das/iter/ob_das_doc_id_merge_iter.h"
#include "sql/das/iter/ob_das_iter_define.h"
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_scan_op.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObDASDocIdMergeIterParam::ObDASDocIdMergeIterParam()
  : ObDASIterParam(DAS_ITER_DOC_ID_MERGE),
    rowkey_doc_tablet_id_(),
    rowkey_doc_ls_id_(),
    rowkey_doc_iter_(nullptr),
    data_table_iter_(nullptr),
    rowkey_doc_ctdef_(nullptr),
    data_table_ctdef_(nullptr),
    rowkey_doc_rtdef_(nullptr),
    data_table_rtdef_(nullptr),
    trans_desc_(nullptr),
    snapshot_(nullptr)
{}

ObDASDocIdMergeIterParam::~ObDASDocIdMergeIterParam()
{
}

ObDASDocIdMergeIter::ObDASDocIdMergeIter()
  : ObDASIter(),
    need_filter_rowkey_doc_(true),
    rowkey_doc_scan_param_(),
    rowkey_doc_iter_(nullptr),
    data_table_iter_(nullptr),
    rowkey_doc_ctdef_(nullptr),
    data_table_ctdef_(nullptr),
    rowkey_doc_rtdef_(nullptr),
    data_table_rtdef_(nullptr),
    rowkey_doc_tablet_id_(),
    rowkey_doc_ls_id_(),
    merge_memctx_()
{
}

ObDASDocIdMergeIter::~ObDASDocIdMergeIter()
{
}

int ObDASDocIdMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkey_doc_iter_) || OB_ISNULL(data_table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, rowkey doc or data table iter is nullptr", K(ret), KP(rowkey_doc_iter_),
        KP(data_table_iter_));
  } else if (OB_FAIL(build_rowkey_doc_range())) {
    LOG_WARN("fail to build rowkey doc range", K(ret));
  } else if (OB_FAIL(data_table_iter_->do_table_scan())) {
    LOG_WARN("fail to do table scan for data table", K(ret), KPC(data_table_iter_));
  } else if (OB_FAIL(rowkey_doc_iter_->do_table_scan())) {
    LOG_WARN("fail to do table scan for rowkey doc", K(ret), KPC(rowkey_doc_iter_));
  }
  LOG_INFO("do table scan", K(ret), K(data_table_iter_->get_scan_param()), K(rowkey_doc_iter_->get_scan_param()));
  return ret;
}

int ObDASDocIdMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkey_doc_iter_) || OB_ISNULL(data_table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, rowkey doc or data table iter is nullptr", K(ret), KP(rowkey_doc_iter_),
        KP(data_table_iter_));
  } else if (OB_FAIL(build_rowkey_doc_range())) {
    LOG_WARN("fail to build rowkey doc range", K(ret));
  } else if (OB_FAIL(data_table_iter_->rescan())) {
    LOG_WARN("fail to rescan data table iter", K(ret), KPC(data_table_iter_));
  } else {
    if (OB_FAIL(rowkey_doc_iter_->rescan())) {
      LOG_WARN("fail to rescan rowkey doc iter", K(ret), KPC(rowkey_doc_iter_));
    }
  }
  LOG_INFO("rescan", K(ret), K(data_table_iter_->get_scan_param()), K(rowkey_doc_iter_->get_scan_param()));
  return ret;
}

void ObDASDocIdMergeIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(rowkey_doc_iter_)) {
    rowkey_doc_iter_->clear_evaluated_flag();
  }
  if (OB_NOT_NULL(data_table_iter_)) {
    data_table_iter_->clear_evaluated_flag();
  }
}

int ObDASDocIdMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_DOC_ID_MERGE != param.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(ret), K(param));
  } else {
    ObDASDocIdMergeIterParam &merge_param = static_cast<ObDASDocIdMergeIterParam &>(param);
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), "DocIdMerge", ObCtxIds::DEFAULT_CTX_ID).set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(merge_memctx_, param))) {
      LOG_WARN("failed to create merge memctx", K(ret));
    } else if (OB_FAIL(init_rowkey_doc_scan_param(merge_param.rowkey_doc_tablet_id_, merge_param.rowkey_doc_ls_id_,
            merge_param.rowkey_doc_ctdef_, merge_param.rowkey_doc_rtdef_, merge_param.trans_desc_,
            merge_param.snapshot_))) {
      LOG_WARN("fail to init rowkey doc scan param", K(ret), K(merge_param));
    } else {
      data_table_iter_  = merge_param.data_table_iter_;
      rowkey_doc_iter_  = merge_param.rowkey_doc_iter_;
      data_table_ctdef_ = merge_param.data_table_ctdef_;
      rowkey_doc_ctdef_ = merge_param.rowkey_doc_ctdef_;
      data_table_rtdef_ = merge_param.data_table_rtdef_;
      rowkey_doc_rtdef_ = merge_param.rowkey_doc_rtdef_;
      rowkey_doc_tablet_id_ = merge_param.rowkey_doc_tablet_id_;
      rowkey_doc_ls_id_     = merge_param.rowkey_doc_ls_id_;
      need_filter_rowkey_doc_ = true;
    }
  }
  return ret;
}

int ObDASDocIdMergeIter::set_doc_id_merge_related_ids(
    const ObDASRelatedTabletID &tablet_ids,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_ids.rowkey_doc_tablet_id_.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey doc tablet id or ls id", K(ret), K(tablet_ids.rowkey_doc_tablet_id_), K(ls_id));
  } else {
    rowkey_doc_tablet_id_ = tablet_ids.rowkey_doc_tablet_id_;
    rowkey_doc_ls_id_     = ls_id;
  }
  return ret;
}

int ObDASDocIdMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(data_table_iter_) && OB_FAIL(data_table_iter_->reuse())) {
    LOG_WARN("fail to reuse data table iter", K(ret));
  } else if (OB_NOT_NULL(rowkey_doc_iter_)) {
    const ObTabletID old_tablet_id = rowkey_doc_scan_param_.tablet_id_;
    const bool tablet_id_changed = old_tablet_id.is_valid() && old_tablet_id != rowkey_doc_tablet_id_;
    rowkey_doc_scan_param_.need_switch_param_ = rowkey_doc_scan_param_.need_switch_param_ || (tablet_id_changed ? true : false);
    if (OB_FAIL(rowkey_doc_iter_->reuse())) {
      LOG_WARN("fail to reuse rowkey doc iter", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(merge_memctx_)) {
    merge_memctx_->reset_remain_one_page();
  }
  return ret;
}

int ObDASDocIdMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(merge_memctx_)) {
    DESTROY_CONTEXT(merge_memctx_);
    merge_memctx_ = nullptr;
  }
  rowkey_doc_scan_param_.destroy_schema_guard();
  rowkey_doc_scan_param_.snapshot_.reset();
  rowkey_doc_scan_param_.destroy();
  data_table_iter_ = nullptr;
  rowkey_doc_iter_ = nullptr;
  need_filter_rowkey_doc_ = true;
  return ret;
}

int ObDASDocIdMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_iter_) || OB_ISNULL(rowkey_doc_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data table or rowkey doc iter is nullptr", K(ret), KP(data_table_iter_),
        K(rowkey_doc_iter_));
  } else if (!need_filter_rowkey_doc_) {
    if (OB_FAIL(concat_row())) {
      LOG_WARN("fail to concat data table and rowkey doc row", K(ret));
    }
  } else if (OB_FAIL(sorted_merge_join_row())) {
    LOG_WARN("fail to sorted merge join data table and rowkey doc row", K(ret));
  }
  LOG_TRACE("inner get next row", K(ret));
  return ret;
}

int ObDASDocIdMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_iter_) || OB_ISNULL(rowkey_doc_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data table or rowkey doc iter is nullptr", K(ret), KP(data_table_iter_),
        K(rowkey_doc_iter_));
  } else if (!need_filter_rowkey_doc_) {
    if (OB_FAIL(concat_rows(count, capacity))) {
      LOG_WARN("fail to concat data table and rowkey doc rows", K(ret));
    }
  } else if (OB_FAIL(sorted_merge_join_rows(count, capacity))) {
    LOG_WARN("fail to sorted merge join data table and rowkey doc rows", K(ret));
  }
  LOG_TRACE("inner get next rows", K(ret), K(count), K(capacity));
  return ret;
}

int ObDASDocIdMergeIter::init_rowkey_doc_scan_param(
    const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    transaction::ObTxDesc *trans_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  rowkey_doc_scan_param_.tenant_id_ = tenant_id;
  rowkey_doc_scan_param_.key_ranges_.set_attr(ObMemAttr(tenant_id, "SParamKR"));
  rowkey_doc_scan_param_.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "SParamSSKR"));
  if (OB_UNLIKELY(!tablet_id.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef or rtdef", K(ret), KPC(ctdef), KPC(rtdef));
  } else {
    rowkey_doc_scan_param_.tablet_id_ = tablet_id;
    rowkey_doc_scan_param_.ls_id_ = ls_id;
    rowkey_doc_scan_param_.scan_allocator_ = &get_arena_allocator();
    rowkey_doc_scan_param_.allocator_ = &rtdef->stmt_allocator_;
    rowkey_doc_scan_param_.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    rowkey_doc_scan_param_.index_id_ = ctdef->ref_table_id_;
    rowkey_doc_scan_param_.is_get_ = ctdef->is_get_;
    rowkey_doc_scan_param_.is_for_foreign_check_ = rtdef->is_for_foreign_check_;
    rowkey_doc_scan_param_.timeout_ = rtdef->timeout_ts_;
    rowkey_doc_scan_param_.scan_flag_ = rtdef->scan_flag_;
    rowkey_doc_scan_param_.reserved_cell_count_ = ctdef->access_column_ids_.count();
    rowkey_doc_scan_param_.sql_mode_ = rtdef->sql_mode_;
    rowkey_doc_scan_param_.frozen_version_ = rtdef->frozen_version_;
    rowkey_doc_scan_param_.force_refresh_lc_ = rtdef->force_refresh_lc_;
    rowkey_doc_scan_param_.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    rowkey_doc_scan_param_.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    rowkey_doc_scan_param_.ext_file_column_exprs_ = &(ctdef->pd_expr_spec_.ext_file_column_exprs_);
    rowkey_doc_scan_param_.ext_column_convert_exprs_ = &(ctdef->pd_expr_spec_.ext_column_convert_exprs_);
    rowkey_doc_scan_param_.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    rowkey_doc_scan_param_.table_param_ = &(ctdef->table_param_);
    rowkey_doc_scan_param_.op_ = rtdef->p_pd_expr_op_;
    rowkey_doc_scan_param_.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    rowkey_doc_scan_param_.schema_version_ = ctdef->schema_version_;
    rowkey_doc_scan_param_.tenant_schema_version_ = rtdef->tenant_schema_version_;
    rowkey_doc_scan_param_.limit_param_ = rtdef->limit_param_;
    rowkey_doc_scan_param_.need_scn_ = rtdef->need_scn_;
    rowkey_doc_scan_param_.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    rowkey_doc_scan_param_.fb_snapshot_ = rtdef->fb_snapshot_;
    rowkey_doc_scan_param_.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    if (rtdef->is_for_foreign_check_) {
      rowkey_doc_scan_param_.trans_desc_ = trans_desc;
    }
    if (OB_NOT_NULL(snapshot)) {
      if (OB_FAIL(rowkey_doc_scan_param_.snapshot_.assign(*snapshot))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null snapshot", K(ret), KPC(ctdef), KPC(rtdef));
    }
    if (OB_NOT_NULL(trans_desc)) {
      rowkey_doc_scan_param_.tx_id_ = trans_desc->get_tx_id();
    } else {
      rowkey_doc_scan_param_.tx_id_.reset();
    }
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      rowkey_doc_scan_param_.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    rowkey_doc_scan_param_.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_FAIL(rowkey_doc_scan_param_.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to assign column ids", K(ret));
    }
    if (rtdef->sample_info_ != nullptr) {
      rowkey_doc_scan_param_.sample_info_ = *rtdef->sample_info_;
    }
  }

  LOG_INFO("init rowkey doc table scan param finished", K(rowkey_doc_scan_param_), K(ret));
  return ret;
}

int ObDASDocIdMergeIter::build_rowkey_doc_range()
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
      key_range.table_id_ = rowkey_doc_scan_param_.index_id_;
      if (OB_FAIL(rowkey_doc_scan_param_.key_ranges_.push_back(key_range))) {
        LOG_WARN("fail to push back key range for rowkey doc scan param", K(ret), K(key_range));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ss_key_ranges.count(); ++i) {
      ObNewRange ss_key_range = ss_key_ranges.at(i);
      ss_key_range.table_id_ = rowkey_doc_scan_param_.index_id_;
      if (OB_FAIL(rowkey_doc_scan_param_.ss_key_ranges_.push_back(ss_key_range))) {
        LOG_WARN("fail to push back ss key range for rowkey doc scan param", K(ret), K(ss_key_range));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const ObExprPtrIArray *op_filters = data_table_iter_->get_scan_param().op_filters_;
    if (OB_ISNULL(op_filters) || (OB_NOT_NULL(op_filters) && op_filters->empty())) {
      need_filter_rowkey_doc_ = false;
    } else {
      need_filter_rowkey_doc_ = true;
    }
    rowkey_doc_scan_param_.tablet_id_ = rowkey_doc_tablet_id_;
    rowkey_doc_scan_param_.ls_id_     = rowkey_doc_ls_id_;
    rowkey_doc_scan_param_.sample_info_ = data_table_iter_->get_scan_param().sample_info_;
    rowkey_doc_scan_param_.scan_flag_.scan_order_ = data_table_iter_->get_scan_param().scan_flag_.scan_order_;
    if (!data_table_iter_->get_scan_param().need_switch_param_) {
      rowkey_doc_scan_param_.need_switch_param_ = false;
    }
  }
  LOG_INFO("build rowkey doc range", K(ret), K(need_filter_rowkey_doc_), K(rowkey_doc_scan_param_.key_ranges_),
      K(rowkey_doc_scan_param_.ss_key_ranges_), K(rowkey_doc_scan_param_.sample_info_));
  return ret;
}

int ObDASDocIdMergeIter::concat_row()
{
  int ret = OB_SUCCESS;
  common::ObDocId doc_id;
  if (OB_FAIL(data_table_iter_->get_next_row())) {
    if (OB_ITER_END == ret) {
      if (OB_FAIL(rowkey_doc_iter_->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next rows", K(ret));
        }
      } else {
        ObArenaAllocator allocator("RowkeyDoc");
        common::ObRowkey rowkey;
        if (OB_FAIL(get_rowkey(allocator, rowkey_doc_ctdef_, rowkey_doc_rtdef_, rowkey))) {
          LOG_WARN("fail to process_data_table_rowkey", K(ret));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row count isn't equal between data table and rowkey doc", K(ret), K(rowkey),
              K(rowkey_doc_iter_->get_scan_param()), K(data_table_iter_->get_scan_param()));
        }
      }
    } else {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_FAIL(rowkey_doc_iter_->get_next_row())) {
    LOG_WARN("fail to get next row", K(ret));
    int tmp_ret = OB_SUCCESS;
    ObArenaAllocator allocator("RowkeyDoc");
    common::ObRowkey rowkey;
    if (OB_TMP_FAIL(get_rowkey(allocator, data_table_ctdef_, data_table_rtdef_, rowkey))) {
      LOG_WARN("fail to process_data_table_rowkey", K(ret), K(tmp_ret));
    } else {
      LOG_WARN("data table rowkey", K(ret), K(rowkey), K(rowkey_doc_iter_->get_scan_param()),
          K(data_table_iter_->get_scan_param()));
    }
  } else if (OB_FAIL(get_doc_id(rowkey_doc_ctdef_, rowkey_doc_rtdef_, doc_id))) {
    LOG_WARN("fail to get doc id", K(ret));
  } else if (OB_FAIL(fill_doc_id_in_data_table(doc_id))) {
    LOG_WARN("fail to fill doc id in data table", K(ret), K(doc_id));
  }
  return ret;
}

int ObDASDocIdMergeIter::concat_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t data_row_cnt = 0;
  int64_t rowkey_doc_row_cnt = 0;
  common::ObArray<common::ObDocId> doc_ids;
  if (OB_FAIL(data_table_iter_->get_next_rows(data_row_cnt, capacity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
  } else { // whatever succ or iter_end, we should get next rows from rowkey doc.
    const bool expect_iter_end = (OB_ITER_END == ret);
    int64_t real_cap = (data_row_cnt > 0 && !expect_iter_end) ? data_row_cnt : capacity;
    ret = OB_SUCCESS; // recover ret from iter end
    while (OB_SUCC(ret) && (real_cap > 0 || expect_iter_end)) {
      rowkey_doc_row_cnt = 0;
      if (OB_FAIL(rowkey_doc_iter_->get_next_rows(rowkey_doc_row_cnt, real_cap))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get next row", K(ret), K(data_row_cnt), K(real_cap), K(doc_ids));
        }
      }
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
      } else if (rowkey_doc_row_cnt > 0) {
        const int tmp_ret = ret;
        if (OB_FAIL(get_doc_ids(rowkey_doc_row_cnt, rowkey_doc_ctdef_, rowkey_doc_rtdef_, doc_ids))) {
          LOG_WARN("fail to get doc ids", K(ret), K(count));
        } else {
          ret = tmp_ret;
        }
      }
      real_cap -= rowkey_doc_row_cnt;
    }
    if (OB_FAIL(ret) && OB_ITER_END != ret) {
    } else if (expect_iter_end && OB_ITER_END != ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row count isn't equal between data table and rowkey doc", K(ret), K(capacity), K(rowkey_doc_row_cnt),
          K(data_row_cnt));
    } else if (OB_UNLIKELY(data_row_cnt != doc_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The row count of data table isn't equal to rowkey doc", K(ret), K(data_row_cnt), K(doc_ids),
        K(data_table_iter_->get_scan_param()), K(rowkey_doc_iter_->get_scan_param()));
    } else {
      count = data_row_cnt;
      if (count > 0) {
        const int tmp_ret = ret;
        if (OB_FAIL(fill_doc_ids_in_data_table(doc_ids))) {
          LOG_WARN("fail to fill doc ids in data table", K(ret), K(tmp_ret), K(doc_ids));
        } else {
          ret = tmp_ret;
        }
      }
    }
  }
  LOG_TRACE("concat rows in data table and rowkey doc", K(ret), K(data_row_cnt), K(doc_ids.count()), K(count),
      K(capacity));
  return ret;
}

int ObDASDocIdMergeIter::sorted_merge_join_row()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("DocIdMergeRow");
  common::ObRowkey data_table_rowkey;
  if (OB_FAIL(data_table_iter_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next data table row", K(ret));
  } else if (OB_ITER_END == ret) {
    while (OB_SUCC(rowkey_doc_iter_->get_next_row()));
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next rowkey doc row", K(ret));
    }
  } else if (OB_FAIL(get_rowkey(allocator, data_table_ctdef_, data_table_rtdef_, data_table_rowkey))) {
    LOG_WARN("fail to get data table rowkey", K(ret));
  } else {
    common::ObDocId doc_id;
    bool is_found = false;
    while (OB_SUCC(ret) && !is_found) {
      common::ObRowkey rowkey_doc_rowkey;
      if (OB_FAIL(rowkey_doc_iter_->get_next_row())) {
        LOG_WARN("fail to get next rowkey doc row", K(ret));
      } else if (OB_FAIL(get_rowkey(allocator, rowkey_doc_ctdef_, rowkey_doc_rtdef_, rowkey_doc_rowkey))) {
        LOG_WARN("fail to get rowkey doc rowkey");
      } else if (rowkey_doc_rowkey.equal(data_table_rowkey, is_found)) {
        LOG_WARN("fail to equal rowkey between data table and rowkey", K(ret));
      }
      LOG_TRACE("compare one row in rowkey doc", K(ret), "need_skip=", !is_found, K(data_table_rowkey),
          K(rowkey_doc_rowkey));
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, The row count of data table isn't equal to rowkey doc", K(ret));
      }
    } else if (OB_FAIL(get_doc_id(rowkey_doc_ctdef_, rowkey_doc_rtdef_, doc_id))) {
      LOG_WARN("fail to get doc id", K(ret));
    } else if (OB_FAIL(fill_doc_id_in_data_table(doc_id))) {
      LOG_WARN("fail to fill doc id in data table", K(ret), K(doc_id));
    }
  }
  return ret;
}

int ObDASDocIdMergeIter::sorted_merge_join_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("DocIdMergeRows");
  common::ObArray<common::ObRowkey> rowkeys_in_data_table;
  common::ObArray<common::ObDocId> doc_ids;
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
    int64_t remain_cnt = data_table_cnt;
    int64_t rowkey_doc_cnt = 0;
    while (OB_SUCC(ret) && remain_cnt > 0) {
      common::ObArray<common::ObRowkey> rowkeys_in_rowkey_doc;
      common::ObArray<common::ObDocId> doc_ids_in_rowkey_doc;
      const int64_t batch_size = is_iter_end ? capacity : remain_cnt;
      if (OB_FAIL(rowkey_doc_iter_->get_next_rows(rowkey_doc_cnt, batch_size)) && OB_ITER_END != ret) {
        LOG_WARN("fail to get next rowkey doc rows", K(ret), K(remain_cnt),  K(batch_size), K(rowkey_doc_iter_));
      } else if (OB_UNLIKELY(OB_ITER_END == ret && (!is_iter_end || 0 == rowkey_doc_cnt))){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, iter end is reached at rowkey doc, but not at data table", K(ret), K(is_iter_end),
            K(rowkey_doc_cnt));
      } else if (OB_FAIL(get_rowkeys_and_doc_ids(rowkey_doc_cnt, allocator, rowkey_doc_ctdef_, rowkey_doc_rtdef_,
              rowkeys_in_rowkey_doc, doc_ids_in_rowkey_doc))) {
        LOG_WARN("fail to get rowkey doc rowkeys", K(ret), K(rowkey_doc_cnt));
      } else {
        for (int64_t i = data_table_cnt - remain_cnt, j = 0;
             OB_SUCC(ret) && i < data_table_cnt && j < rowkeys_in_rowkey_doc.count();
             ++j) {
          bool is_equal = false;
          LOG_TRACE("compare one row in rowkey doc", K(ret), K(i), K(j), K(rowkeys_in_data_table.at(i)),
              K(rowkeys_in_rowkey_doc.at(j)));
          if (rowkeys_in_rowkey_doc.at(j).equal(rowkeys_in_data_table.at(i), is_equal)) {
            LOG_WARN("fail to equal rowkey between data table and rowkey", K(ret));
          } else if (is_equal) {
            if (OB_FAIL(doc_ids.push_back(doc_ids_in_rowkey_doc.at(j)))) {
              LOG_WARN("fail to push back doc id", K(ret), K(j), K(doc_ids_in_rowkey_doc));
            } else {
              --remain_cnt;
              ++i;
              LOG_TRACE("find doc id in rowkey doc", K(doc_ids_in_rowkey_doc.at(j)), K(remain_cnt), K(i), K(data_table_cnt));
            }
          }
        }
      }
    }
    if (FAILEDx(fill_doc_ids_in_data_table(doc_ids))) {
      LOG_WARN("fail to fill doc ids in data table", K(ret), K(doc_ids));
    } else {
      count = data_table_cnt;
      ret = is_iter_end ? OB_ITER_END : ret;
    }
  }
  return ret;
}

int ObDASDocIdMergeIter::get_rowkey(
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
        LOG_TRACE("get one rowkey", K(rowkey));
      }
    }
  }
  return ret;
}

int ObDASDocIdMergeIter::get_doc_id(
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObDocId &doc_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ctdef), KP(rtdef));
  } else {
    const int64_t rowkey_cnt = ctdef->table_param_.get_read_info().get_schema_rowkey_count();
    ObExpr *expr = nullptr;
    // When the defensive check level is set to 2 (strict defensive check), the transaction information of the current
    // row is recorded for 4377 diagnosis. Then, it will add pseudo_trans_info_expr into result output of das scan.
    //
    // just skip it if trans info expr in ctdef isn't nullptr.
    if (OB_NOT_NULL(ctdef->trans_info_expr_)) {
      if (OB_UNLIKELY(ctdef->result_output_.count() != rowkey_cnt + 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected result output column count", K(ret), K(rowkey_cnt), K(ctdef->result_output_.count()));
      }
    } else if (OB_UNLIKELY(ctdef->result_output_.count() != rowkey_cnt + 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected result output column count", K(ret), K(rowkey_cnt), K(ctdef->result_output_.count()));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(expr = ctdef->result_output_.at(rowkey_cnt))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, doc id expr is nullptr", K(ret), K(rowkey_cnt), K(ctdef->result_output_));
    } else {
      ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
      if (OB_FAIL(doc_id.from_string(datum.get_string()))) {
        LOG_WARN("fail to get doc id", K(ret), K(datum));
      }
    }
  }
  return ret;
}

int ObDASDocIdMergeIter::get_rowkeys(
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

int ObDASDocIdMergeIter::get_doc_ids(
    const int64_t size,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObIArray<common::ObDocId> &doc_ids)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*rtdef->eval_ctx_);
  batch_info_guard.set_batch_size(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    batch_info_guard.set_batch_idx(i);
    common::ObDocId doc_id;
    if (OB_FAIL(get_doc_id(ctdef, rtdef, doc_id))) {
      LOG_WARN("fail to get doc id", K(ret), K(i));
    } else if (OB_FAIL(doc_ids.push_back(doc_id))) {
      LOG_WARN("fail to push back doc id", K(ret), K(doc_id));
    }
  }
  return ret;
}

int ObDASDocIdMergeIter::get_rowkeys_and_doc_ids(
    const int64_t size,
    common::ObIAllocator &allocator,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    common::ObIArray<common::ObRowkey> &rowkeys,
    common::ObIArray<common::ObDocId> &doc_ids)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*rtdef->eval_ctx_);
  batch_info_guard.set_batch_size(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    batch_info_guard.set_batch_idx(i);
    common::ObRowkey rowkey;
    common::ObDocId doc_id;
    if (OB_FAIL(get_rowkey(allocator, ctdef, rtdef, rowkey))) {
      LOG_WARN("fail to process_data_table_rowkey", K(ret), K(i));
    } else if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("fail to push back rowkey", K(ret), K(rowkey));
    } else if (OB_FAIL(get_doc_id(ctdef, rtdef, doc_id))) {
      LOG_WARN("fail to get doc id", K(ret), K(i));
    } else if (OB_FAIL(doc_ids.push_back(doc_id))) {
      LOG_WARN("fail to push back doc id", K(ret), K(doc_id));
    }
  }
  return ret;
}

int ObDASDocIdMergeIter::fill_doc_id_in_data_table(const common::ObDocId &doc_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!doc_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(doc_id));
  } else if (OB_ISNULL(data_table_ctdef_) || OB_ISNULL(data_table_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, data table ctdef is nullptr", K(ret), KP(data_table_ctdef_), KP(data_table_rtdef_));
  } else {
    const int64_t doc_id_idx = data_table_ctdef_->doc_id_idx_;
    ObExpr *doc_id_expr = nullptr;
    if (OB_UNLIKELY(doc_id_idx >= data_table_ctdef_->result_output_.count() || doc_id_idx < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid doc id idx", K(ret), K(doc_id_idx), K(data_table_ctdef_->result_output_.count()));
    } else if (OB_ISNULL(doc_id_expr = data_table_ctdef_->result_output_.at(doc_id_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpeted error, doc id expr is nullptr", K(ret), K(doc_id_idx), KPC(data_table_ctdef_));
    } else {
      const uint64_t buf_len = sizeof(ObDocId);
      ObDatum &datum = doc_id_expr->locate_datum_for_write(*data_table_rtdef_->eval_ctx_);
      void *buf = static_cast<void *>(doc_id_expr->get_str_res_mem(*data_table_rtdef_->eval_ctx_, buf_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), KP(buf));
      } else {
        ObDocId *doc_id_ptr = new (buf) ObDocId(doc_id);
        datum.set_string(doc_id_ptr->get_string());
        doc_id_expr->set_evaluated_projected(*data_table_rtdef_->eval_ctx_);
        LOG_TRACE("Doc id merge fill a document id", K(doc_id), KP(doc_id_expr), KPC(doc_id_expr));
      }
      if (OB_SUCC(ret)) {
      }
    }
  }
  return ret;

}

int ObDASDocIdMergeIter::fill_doc_ids_in_data_table(const common::ObIArray<common::ObDocId> &doc_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == doc_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(doc_ids));
  } else if (OB_ISNULL(data_table_ctdef_) || OB_ISNULL(data_table_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, data table ctdef is nullptr", K(ret), KP(data_table_ctdef_), KP(data_table_rtdef_));
  } else {
    const uint64_t len_of_doc_id = sizeof(ObDocId);
    const uint64_t len_of_buf = len_of_doc_id * doc_ids.count();
    const int64_t doc_id_idx = data_table_ctdef_->doc_id_idx_;
    ObExpr *doc_id_expr = nullptr;
    ObDatum *datums = nullptr;
    char *buf = nullptr;
    if (OB_UNLIKELY(doc_id_idx >= data_table_ctdef_->result_output_.count() || doc_id_idx < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid doc id idx", K(ret), K(doc_id_idx), K(data_table_ctdef_->result_output_.count()));
    } else if (OB_ISNULL(doc_id_expr = data_table_ctdef_->result_output_.at(doc_id_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, doc id expr is nullptr", K(ret), K(doc_id_idx), KPC(data_table_ctdef_));
    } else if (OB_ISNULL(datums = doc_id_expr->locate_datums_for_update(*data_table_rtdef_->eval_ctx_, doc_ids.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(doc_id_expr));
    } else if (OB_ISNULL(buf = static_cast<char *>(doc_id_expr->get_str_res_mem(*data_table_rtdef_->eval_ctx_, len_of_buf)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), KP(buf));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < doc_ids.count(); ++i) {
        ObDocId *doc_id = new (buf + len_of_doc_id * i) ObDocId(doc_ids.at(i));
        datums[i].set_string(doc_id->get_string());
        LOG_TRACE("Doc id merge fill a document id", KPC(doc_id), K(i));
      }
      if (OB_SUCC(ret)) {
        doc_id_expr->set_evaluated_projected(*data_table_rtdef_->eval_ctx_);
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
