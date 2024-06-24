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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_local_lookup_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_ir_define.h"
#include "storage/concurrency_control/ob_data_validation_service.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASLocalLookupIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASLookupIter::inner_init(param))) {
    LOG_WARN("failed to init das lookup iter", K(ret));
  } else if (param.type_ != ObDASIterType::DAS_ITER_LOCAL_LOOKUP) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    ObDASLocalLookupIterParam &lookup_param = static_cast<ObDASLocalLookupIterParam&>(param);
    trans_desc_ = lookup_param.trans_desc_;
    snapshot_ = lookup_param.snapshot_;
    if (lookup_param.rowkey_exprs_->empty()) {
      // for compatibility
      if (OB_FAIL(init_rowkey_exprs_for_compat())) {
        LOG_WARN("failed eto init rowkeys exprs for compat", K(ret));
      }
    } else if (OB_FAIL(rowkey_exprs_.assign(*lookup_param.rowkey_exprs_))) {
      LOG_WARN("failed to assign rowkey exprs", K(ret));
    }
  }

  return ret;
}

int ObDASLocalLookupIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  // index_scan_param is maintained by das scan op.
  if (OB_FAIL(index_table_iter_->reuse())) {
    LOG_WARN("failed to reuse index table iter", K(ret));
  } else {
    const ObTabletID &old_tablet_id = lookup_param_.tablet_id_;
    lookup_param_.need_switch_param_ = lookup_param_.need_switch_param_ ||
      ((old_tablet_id.is_valid() && old_tablet_id != lookup_tablet_id_) ? true : false);
    if (OB_FAIL(data_table_iter_->reuse())) {
      LOG_WARN("failed to reuse data table iter", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(ObDASLookupIter::inner_reuse())) {
    LOG_WARN("failed to reuse das lookup iter", K(ret));
  }
  trans_info_array_.reuse();
  return ret;
}

int ObDASLocalLookupIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASLookupIter::inner_release())) {
    LOG_WARN("failed to release lookup iter", K(ret));
  }
  lookup_param_.destroy_schema_guard();
  lookup_param_.snapshot_.reset();
  lookup_param_.destroy();
  trans_info_array_.reset();
  return ret;
}

int ObDASLocalLookupIter::rescan()
{
  int ret = OB_SUCCESS;
  // only rescan index table, data table will be rescan in do_lookup.
  if (OB_FAIL(index_table_iter_->rescan())) {
    LOG_WARN("failed to rescan index table iter", K(ret));
  }
  return ret;
}

int ObDASLocalLookupIter::init_scan_param(ObTableScanParam &param, const ObDASScanCtDef *ctdef, ObDASScanRtDef *rtdef)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  param.tenant_id_ = tenant_id;
  param.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
  param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef or rtdef", K(ctdef), K(rtdef));
  } else {
    param.tablet_id_ = lookup_tablet_id_;
    param.ls_id_ = lookup_ls_id_;
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
    param.ext_column_convert_exprs_ = &(ctdef->pd_expr_spec_.ext_column_convert_exprs_);
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
      param.trans_desc_ = trans_desc_;
    }
    if (OB_NOT_NULL(snapshot_)) {
      param.snapshot_ = *snapshot_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null snapshot", K(ret), KPC(this));
    }
    if (OB_NOT_NULL(trans_desc_)) {
      param.tx_id_ = trans_desc_->get_tx_id();
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

  LOG_DEBUG("init local index lookup param finished", K(param), K(ret));
  return ret;
}

void ObDASLocalLookupIter::reset_lookup_state()
{
  ObDASLookupIter::reset_lookup_state();
  trans_info_array_.reuse();
}

int ObDASLocalLookupIter::add_rowkey()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(data_table_iter_->get_type() == DAS_ITER_SCAN);
  ObDASScanIter *scan_iter = static_cast<ObDASScanIter *>(data_table_iter_);
  storage::ObTableScanParam &scan_param = scan_iter->get_scan_param();
  ObNewRange lookup_range;
  int64 group_id = 0;

  if (DAS_OP_TABLE_SCAN == index_ctdef_->op_type_) {
    const ObDASScanCtDef *index_ctdef = static_cast<const ObDASScanCtDef*>(index_ctdef_);
    if (nullptr != index_ctdef->group_id_expr_) {
      group_id = index_ctdef->group_id_expr_->locate_expr_datum(*eval_ctx_).get_int();
    }
    if (nullptr != index_ctdef->trans_info_expr_) {
      ObDatum *datum_ptr = nullptr;
      if (OB_FAIL(build_trans_info_datum(index_ctdef->trans_info_expr_, datum_ptr))) {
        LOG_WARN("failed to build trans info datum", K(ret));
      } else if (OB_ISNULL(datum_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(trans_info_array_.push_back(datum_ptr))) {
        LOG_WARN("failed to push back trans info array", K(ret), KPC(datum_ptr));
      }
    }
  }

  int64_t group_idx = ObNewRange::get_group_idx(group_id);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_lookup_range(lookup_range))) {
    LOG_WARN("failed to build lookup range", K(ret));
  } else if (FALSE_IT(lookup_range.group_idx_ = group_idx)) {
  } else if (OB_FAIL(scan_param.key_ranges_.push_back(lookup_range))) {
    LOG_WARN("failed to push back lookup range", K(ret));
  } else {
    scan_param.is_get_ = true;
  }
  LOG_DEBUG("build local lookup range", K(lookup_range), K(ret));

  return ret;
}

int ObDASLocalLookupIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(index_table_iter_->do_table_scan())) {
    LOG_WARN("failed to scan index table", K(ret));
  }
  return ret;
}

int ObDASLocalLookupIter::add_rowkeys(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K_(eval_ctx));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_size(count);
    for(int i = 0; OB_SUCC(ret) && i < count; i++) {
      batch_info_guard.set_batch_idx(i);
      if(OB_FAIL(add_rowkey())) {
        LOG_WARN("failed to add rowkey", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObDASLocalLookupIter::do_index_lookup()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(data_table_iter_->get_type() == DAS_ITER_SCAN);
  if (is_first_lookup_) {
    is_first_lookup_ = false;
    if (OB_FAIL(init_scan_param(lookup_param_, lookup_ctdef_, lookup_rtdef_))) {
      LOG_WARN("failed to init scan param", K(ret));
    } else if (OB_FAIL(data_table_iter_->do_table_scan())) {
      if (OB_SNAPSHOT_DISCARDED == ret && lookup_param_.fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to do partition scan", K(lookup_param_), K(ret));
      }
    }
  } else {
    // reuse -> real rescan
    // reuse: store next tablet_id, ls_id and reuse storage iter;
    // rescan: bind tablet_id, ls_id to scan_param and rescan;
    lookup_param_.tablet_id_ = lookup_tablet_id_;
    lookup_param_.ls_id_ = lookup_ls_id_;
    if (OB_FAIL(data_table_iter_->rescan())) {
      LOG_WARN("failed to rescan data table", K(ret));
    }
  }
  return ret;
}

int ObDASLocalLookupIter::check_index_lookup()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(data_table_iter_->get_type() == DAS_ITER_SCAN);
  ObDASScanIter *scan_iter = static_cast<ObDASScanIter*>(data_table_iter_);
  if (GCONF.enable_defensive_check() &&
      lookup_ctdef_->pd_expr_spec_.pushdown_filters_.empty()) {
    if (OB_UNLIKELY(lookup_rowkey_cnt_ != lookup_row_cnt_)) {
      ret = OB_ERR_DEFENSIVE_CHECK;
      ObString func_name = ObString::make_string("check_lookup_row_cnt");
      LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
      LOG_ERROR("Fatal Error!!! Catch a defensive error!",
          K(ret), K_(lookup_rowkey_cnt), K_(lookup_row_cnt),
          "main table id", lookup_ctdef_->ref_table_id_,
          "main tablet id", lookup_tablet_id_,
          KPC_(trans_desc), KPC_(snapshot));
      concurrency_control::ObDataValidationService::set_delay_resource_recycle(lookup_ls_id_);
      const ObTableScanParam &scan_param = scan_iter->get_scan_param();
      if (trans_info_array_.count() == scan_param.key_ranges_.count()) {
        for (int64_t i = 0; i < trans_info_array_.count(); i++) {
          ObDatum *datum = trans_info_array_.at(i);
          LOG_ERROR("dump lookup range and trans info of local lookup das task",
              K(i), KPC(trans_info_array_.at(i)), K(scan_param.key_ranges_.at(i)));
        }
      } else {
        for (int64_t i = 0; i < scan_param.key_ranges_.count(); i++) {
          LOG_ERROR("dump lookup range of local lookup das task",
              K(i), K(scan_param.key_ranges_.at(i)));
        }
      }
    }
  }

  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_DUMP_WRITE_BUFFER);
  if (0 != simulate_error) {
    for (int64_t i = 0; i < trans_info_array_.count(); i++) {
      LOG_INFO("dump trans info of local lookup das task", K(i), KPC(trans_info_array_.at(i)));
    }
  }

  return ret;
}

int ObDASLocalLookupIter::init_rowkey_exprs_for_compat()
{
  int ret = OB_SUCCESS;
  if (ObDASOpType::DAS_OP_TABLE_SCAN == index_ctdef_->op_type_
      || ObDASOpType::DAS_OP_IR_AUX_LOOKUP == index_ctdef_->op_type_) {
    const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(index_ctdef_);
    int64_t rowkey_cnt = scan_ctdef->result_output_.count();
    if (nullptr != scan_ctdef->group_id_expr_) {
      rowkey_cnt -= 1;
    }
    if (nullptr != scan_ctdef->trans_info_expr_) {
      rowkey_cnt -= 1;
    }
    if (OB_FAIL(rowkey_exprs_.reserve(rowkey_cnt))) {
      LOG_WARN("failed to reserve rowkey exprs cnt", K(rowkey_cnt), K(ret));
    } else {
      for (int64_t i = 0; i < scan_ctdef->result_output_.count(); i++) {
        ObExpr* const expr = scan_ctdef->result_output_.at(i);
        if (T_PSEUDO_GROUP_ID == expr->type_ || T_PSEUDO_ROW_TRANS_INFO_COLUMN  == expr->type_) {
          // skip
        } else if (OB_FAIL(rowkey_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
  } else if (ObDASOpType::DAS_OP_IR_SCAN == index_ctdef_->op_type_
      || ObDASOpType::DAS_OP_SORT == index_ctdef_->op_type_) {
    // only doc_id as rowkey for text retrieval index back
    const ObDASIRScanCtDef *ir_ctdef = nullptr;
    if (ObDASOpType::DAS_OP_SORT == index_ctdef_->op_type_) {
      if (OB_UNLIKELY(ObDASOpType::DAS_OP_IR_SCAN != index_ctdef_->children_[0]->op_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected child of sort iter is not an ir scan iter for compatible scan", K(ret));
      } else {
        ir_ctdef = static_cast<const ObDASIRScanCtDef *>(index_ctdef_->children_[0]);
      }
    } else {
      ir_ctdef = static_cast<const ObDASIRScanCtDef *>(index_ctdef_);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rowkey_exprs_.push_back(ir_ctdef->inv_scan_doc_id_col_))) {
        LOG_WARN("gailed to add rowkey exprs", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected compatible das scan op type", K(ret), K(index_ctdef_->op_type_));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
