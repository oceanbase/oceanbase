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

#include "sql/das/iter/ob_das_func_data_iter.h"
#include "sql/das/iter/ob_das_iter_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/das/iter/ob_das_text_retrieval_iter.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObDASFuncDataIterParam::ObDASFuncDataIterParam()
  : ObDASIterParam(ObDASIterType::DAS_ITER_FUNC_DATA),
    tr_merge_iters_(nullptr),
    iter_count_(0),
    main_lookup_ctdef_(nullptr),
    main_lookup_rtdef_(nullptr),
    main_lookup_iter_(nullptr),
    trans_desc_(nullptr),
    snapshot_(nullptr)
{}

ObDASFuncDataIterParam::~ObDASFuncDataIterParam()
{
}

ObDASFuncDataIter::ObDASFuncDataIter()
   :ObDASIter(),
    tr_merge_iters_(nullptr),
    iter_count_(0),
    main_lookup_ctdef_(nullptr),
    main_lookup_rtdef_(nullptr),
    main_lookup_iter_(nullptr),
    main_lookup_tablet_id_(0),
    main_lookup_ls_id_(0),
    main_lookup_param_(),
    merge_memctx_(),
    doc_ids_(),
    read_count_(0)
  {}

ObDASFuncDataIter::~ObDASFuncDataIter()
{
}

int ObDASFuncDataIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tr_merge_iters_) || OB_UNLIKELY(iter_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, tr merge iter is nullptr", K(ret));
  } else if (OB_FAIL(build_tr_merge_iters_rangekey())) {
    LOG_WARN("fail to build rowkey doc range", K(ret));
  } else {
    if (nullptr != main_lookup_iter_) {
      if (OB_UNLIKELY(!main_lookup_tablet_id_.is_valid() || !main_lookup_ls_id_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, main lookup tablet id or ls id is invalid", K(ret), K(main_lookup_tablet_id_), K(main_lookup_ls_id_));
      } else {
        main_lookup_param_.tablet_id_ = main_lookup_tablet_id_;
        main_lookup_param_.ls_id_ = main_lookup_ls_id_;
        if (OB_FAIL(main_lookup_iter_->do_table_scan())) {
          LOG_WARN("fail to do table scan for main lookup table", K(ret), KPC(main_lookup_iter_));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_count_; i++) {
      if (OB_FAIL(tr_merge_iters_[i]->do_table_scan())) {
        LOG_WARN("fail to do table scan for tr merge iter", K(ret), K(i), KPC(tr_merge_iters_[i]));
      }
    }
  }
  return ret;
}

int ObDASFuncDataIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tr_merge_iters_) || OB_UNLIKELY(iter_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, tr merge iter is nullptr", K(ret));
  } else if (OB_FAIL(build_tr_merge_iters_rangekey())) {
    LOG_WARN("fail to build rowkey doc range", K(ret));
  } else if (nullptr != main_lookup_iter_ && OB_FAIL(main_lookup_iter_->rescan())) {
    LOG_WARN("fail to do table scan for main lookup table", K(ret), KPC(main_lookup_iter_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_count_; i++) {
      if (OB_FAIL(tr_merge_iters_[i]->rescan())) {
        LOG_WARN("fail to do table scan for tr merge iter", K(ret), K(i), KPC(tr_merge_iters_[i]));
      }
    }
  }
  return ret;
}

void ObDASFuncDataIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(main_lookup_iter_)) {
    main_lookup_iter_->clear_evaluated_flag();
  }
  for (int64_t i = 0; i < iter_count_; i++) {
    if (OB_NOT_NULL(tr_merge_iters_[i])) {
      tr_merge_iters_[i]->clear_evaluated_flag();
    }
  }
}

int ObDASFuncDataIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_FUNC_DATA != param.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(ret), K(param));
  } else {
    ObDASFuncDataIterParam &merge_param = static_cast<ObDASFuncDataIterParam &>(param);
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), "FTSMerge", ObCtxIds::DEFAULT_CTX_ID).set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(merge_memctx_, param))) {
      LOG_WARN("failed to create merge memctx", K(ret));
    } else {
      tr_merge_iters_ = merge_param.tr_merge_iters_;
      iter_count_ = merge_param.iter_count_;
      main_lookup_ctdef_ = merge_param.main_lookup_ctdef_;
      main_lookup_rtdef_ = merge_param.main_lookup_rtdef_;
      main_lookup_iter_ = merge_param.main_lookup_iter_;
      read_count_ = 0;
      sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(ObVarcharType, CS_TYPE_BINARY);
      cmp_func_ = lib::is_oracle_mode() ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
      if (main_lookup_iter_ && OB_FAIL(init_main_lookup_scan_param(main_lookup_param_,
                                                                   main_lookup_ctdef_,
                                                                   main_lookup_rtdef_,
                                                                   merge_param.trans_desc_,
                                                                   merge_param.snapshot_))) {
        LOG_WARN("fail to init rowkey doc scan param", K(ret), K(merge_param));
      }
    }
  }
  return ret;
}

int ObDASFuncDataIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  doc_ids_.reuse();
  read_count_ = 0;
  if (OB_NOT_NULL(main_lookup_iter_)) {
    ObDASScanIter *main_lookup_iter = static_cast<ObDASScanIter *>(main_lookup_iter_);
    storage::ObTableScanParam &main_lookup_scan_param = main_lookup_iter->get_scan_param();
    if (OB_UNLIKELY(&main_lookup_param_ != &main_lookup_iter->get_scan_param())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, main lookup param is nullptr", K(ret));
    } else {
      const ObTabletID &old_tablet_id = main_lookup_param_.tablet_id_;
      main_lookup_param_.need_switch_param_ = main_lookup_param_.need_switch_param_ ||
          ((old_tablet_id.is_valid() && old_tablet_id != main_lookup_tablet_id_) ? true : false);
      main_lookup_param_.tablet_id_ = main_lookup_tablet_id_;
      main_lookup_param_.ls_id_ = main_lookup_ls_id_;
      if (!main_lookup_param_.key_ranges_.empty()) {
        main_lookup_param_.key_ranges_.reuse();
      }
      if (OB_FAIL(main_lookup_iter_->reuse())) {
        LOG_WARN("fail to reuse data table iter", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(merge_memctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_memctx_ is nullptr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_count_; i++) {
      if (OB_NOT_NULL(tr_merge_iters_[i])) {
        tr_merge_iters_[i]->reuse();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tr merge iter is nullptr", K(ret), K(i));
      }
    }
    merge_memctx_->reset_remain_one_page();
  }
  return ret;
}

int ObDASFuncDataIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(merge_memctx_)) {
    DESTROY_CONTEXT(merge_memctx_);
    merge_memctx_ = nullptr;
  }
  if (OB_NOT_NULL(main_lookup_iter_)) {
    main_lookup_iter_ = nullptr;
  }
  for (int64_t i = 0; i < iter_count_; i++) {
    if (OB_NOT_NULL(tr_merge_iters_[i])) {
      tr_merge_iters_[i] = nullptr;
    }
  }
  doc_ids_.reset();
  main_lookup_param_.destroy_schema_guard();
  main_lookup_param_.snapshot_.reset();
  main_lookup_param_.destroy();
  read_count_ = 0;
  return ret;
}

int ObDASFuncDataIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  int64_t default_size = doc_ids_.count();
  bool iter_end = false;
  if (OB_ISNULL(tr_merge_iters_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, tr merge iter is nullptr", K(ret));
  } else if (OB_UNLIKELY(1 != default_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, default size is not 1", K(ret), K(default_size));
  } else if (main_lookup_iter_ && OB_FAIL(main_lookup_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row for main lookup table", K(ret), KPC(main_lookup_iter_));
    } else {
      ret = OB_SUCCESS;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < iter_count_; i++) {
    if (OB_FAIL(tr_merge_iters_[i]->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row for tr merge iter", K(ret), K(i), KPC(tr_merge_iters_[i]));
      } else {
        ret = OB_SUCCESS;
        iter_end = true;
      }
    }
  }
  if (OB_SUCC(ret) && iter_end) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObDASFuncDataIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t main_lookup_count = 0;
  int64_t tr_merge_count = 0;
  int64_t default_size = doc_ids_.count();

  if (OB_ISNULL(tr_merge_iters_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, tr merge iter is nullptr", K(ret));
  } else if (OB_NOT_NULL(main_lookup_iter_)) {
    while (OB_SUCC(ret) && main_lookup_count == 0) {
      if (OB_FAIL(main_lookup_iter_->get_next_rows(main_lookup_count, capacity))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row for main lookup table", K(ret), KPC(main_lookup_iter_));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(main_lookup_iter_ && default_size < main_lookup_count + read_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, main lookup count is not equal to capacity", K(ret), K(default_size), K(main_lookup_count), K_(read_count));
  }

  int tmp_count = 0;
  int tr_merge_capacity = main_lookup_count != 0 ? OB_MIN(capacity, main_lookup_count) : capacity;
  for (int64_t i = 0; OB_SUCC(ret) && i < iter_count_; i++) {
    tr_merge_count = 0;
    if (OB_FAIL(tr_merge_iters_[i]->get_next_rows(tr_merge_count, tr_merge_capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next rows for tr merge iter", K(ret), K(i), KPC(tr_merge_iters_[i]));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(tmp_count != 0 && tmp_count != tr_merge_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, tr merge count is not equal to tmp count", K(ret), K(tr_merge_count), K(tmp_count), K(i));
    } else if (OB_UNLIKELY(0 != tr_merge_count &&
                           tr_merge_count != tr_merge_capacity &&
                           tr_merge_count + read_count_ != default_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, tr merge count is not equal to capacity",
        K(ret), K(tr_merge_count), K(capacity), K(i), K_(read_count), K(default_size));
    } else {
      tmp_count = tr_merge_count;
    }
  }
  if (OB_SUCC(ret) && main_lookup_iter_ && tr_merge_count != main_lookup_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, tr merge count is not equal to main lookup count", K(ret), K(tr_merge_count), K(main_lookup_count));
  }
  if (OB_SUCC(ret)) {
    count = tr_merge_count;
    if (0 == tr_merge_count) {
      ret = OB_ITER_END;
    } else {
      read_count_ = read_count_ + count;
    }
  }
  return ret;
}

int ObDASFuncDataIter::build_tr_merge_iters_rangekey()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tr_merge_iters_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, tr merge iters is nullptr", K(ret));
  } else {
    lib::ob_sort(doc_ids_.begin(), doc_ids_.end(), FtsDocIdCmp(cmp_func_, &ret));
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to sort doc id", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_count_; i++) {
      ObDASTextRetrievalMergeIter *tr_merge_iter = static_cast<ObDASTextRetrievalMergeIter *>(tr_merge_iters_[i]);
      if (OB_FAIL(tr_merge_iter->set_rangkey_and_selector(doc_ids_))) {
        LOG_WARN("fail to add doc id", K(ret));
      }
    }
  }
  return ret;
}

int ObDASFuncDataIter::init_main_lookup_scan_param(
    ObTableScanParam &param,
    const ObDASScanCtDef *ctdef,
    ObDASScanRtDef *rtdef,
    transaction::ObTxDesc *trans_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  param.tenant_id_ = tenant_id;
  param.key_ranges_.set_attr(ObMemAttr(tenant_id, "SParamKR"));
  param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "SParamSSKR"));
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef or rtdef", K(ret), KPC(ctdef), KPC(rtdef));
  } else {
    param.scan_allocator_ = &get_arena_allocator();
    param.allocator_ = &rtdef->stmt_allocator_;
    param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    param.index_id_ = ctdef->ref_table_id_;
    param.is_get_ = ctdef->is_get_;
    param.is_for_foreign_check_ = rtdef->is_for_foreign_check_;
    param.timeout_ = rtdef->timeout_ts_;
    param.scan_flag_ = rtdef->scan_flag_;
    param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    param.sql_mode_ = rtdef->sql_mode_;
    param.frozen_version_ = rtdef->frozen_version_;
    param.force_refresh_lc_ = rtdef->force_refresh_lc_;
    param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    param.ext_file_column_exprs_ = &(ctdef->pd_expr_spec_.ext_file_column_exprs_);
    param.ext_mapping_column_exprs_ = &(ctdef->pd_expr_spec_.ext_mapping_column_exprs_);
    param.ext_mapping_column_ids_ = &(ctdef->pd_expr_spec_.ext_mapping_column_ids_);
    param.ext_column_dependent_exprs_ = &(ctdef->pd_expr_spec_.ext_column_convert_exprs_);
    param.ext_enable_late_materialization_ = ctdef->pd_expr_spec_.ext_enable_late_materialization_;
    param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    param.table_param_ = &(ctdef->table_param_);
    param.op_ = rtdef->p_pd_expr_op_;
    param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    param.schema_version_ = ctdef->schema_version_;
    param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    param.limit_param_ = rtdef->limit_param_;
    param.need_scn_ = rtdef->need_scn_;
    param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    param.fb_snapshot_ = rtdef->fb_snapshot_;
    param.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    if (rtdef->is_for_foreign_check_) {
      param.trans_desc_ = trans_desc;
    }
    if (OB_NOT_NULL(snapshot)) {
      if (OB_FAIL(param.snapshot_.assign(*snapshot))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null snapshot", K(ret), KPC(ctdef), KPC(rtdef));
    }
    if (OB_NOT_NULL(trans_desc)) {
      param.tx_id_ = trans_desc->get_tx_id();
    } else {
      param.tx_id_.reset();
    }
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_FAIL(param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to assign column ids", K(ret));
    }
    if (rtdef->sample_info_ != nullptr) {
      param.sample_info_ = *rtdef->sample_info_;
    }
  }

  LOG_DEBUG("init rowkey doc table scan param finished", K(param), K(ret));
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
