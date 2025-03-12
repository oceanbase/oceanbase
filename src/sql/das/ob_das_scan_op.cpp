/**ob_das_scan_op.cpp
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
#include "ob_das_scan_op.h"
#include "sql/das/ob_das_extra_data.h"
#include "storage/concurrency_control/ob_data_validation_service.h"
#include "sql/das/iter/ob_das_iter_utils.h"
#include "storage/tx_storage/ob_access_service.h"


namespace oceanbase
{
namespace common
{
namespace serialization
{
template <>
struct EnumEncoder<false, const sql::ObDASScanCtDef *> : sql::DASCtEncoder<sql::ObDASScanCtDef>
{
};

template <>
struct EnumEncoder<false, sql::ObDASScanRtDef *> : sql::DASRtEncoder<sql::ObDASScanRtDef>
{
};
} // end namespace serialization
} // end namespace common

using namespace storage;
using namespace transaction;
namespace sql
{

OB_SERIALIZE_MEMBER(ObDASTCBMemProfileKey, fake_unique_id_, timestamp_);
OB_SERIALIZE_MEMBER(ObDASScanCtDef,
                    ref_table_id_,
                    access_column_ids_,
                    schema_version_,
                    table_param_,
                    pd_expr_spec_,
                    aggregate_column_ids_,
                    group_id_expr_,
                    result_output_,
                    is_get_,
                    is_external_table_,
                    external_file_location_,
                    external_file_access_info_,
                    external_files_,
                    external_file_format_str_,
                    trans_info_expr_,
                    group_by_column_ids_,
                    ir_scan_type_,
                    rowkey_exprs_,
                    table_scan_opt_,
                    doc_id_idx_,
                    vec_vid_idx_,
                    multivalue_idx_,
                    multivalue_type_,
                    index_merge_idx_,
                    flags_,
                    partition_infos_,
                    domain_id_idxs_,
                    domain_types_,
                    domain_tids_,
                    pre_query_range_,
                    pre_range_graph_);

OB_DEF_SERIALIZE(ObDASScanRtDef)
{
  int ret = OB_SUCCESS;
    LST_DO_CODE(OB_UNIS_ENCODE,
    tenant_schema_version_,
    limit_param_,
    need_scn_,
    force_refresh_lc_,
    frozen_version_,
    fb_snapshot_,
    timeout_ts_,
    tx_lock_timeout_,
    sql_mode_,
    scan_flag_,
    pd_storage_flag_,
    need_check_output_datum_,
    is_for_foreign_check_,
    fb_read_tx_uncommitted_,
    key_ranges_,
    ss_key_ranges_,
    task_count_,
    scan_op_id_,
    scan_rows_size_,
    row_width_,
    das_tasks_key_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDASScanRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
    tenant_schema_version_,
    limit_param_,
    need_scn_,
    force_refresh_lc_,
    frozen_version_,
    fb_snapshot_,
    timeout_ts_,
    tx_lock_timeout_,
    sql_mode_,
    scan_flag_,
    pd_storage_flag_,
    need_check_output_datum_,
    is_for_foreign_check_,
    fb_read_tx_uncommitted_,
    key_ranges_,
    ss_key_ranges_,
    task_count_,
    scan_op_id_,
    scan_rows_size_,
    row_width_,
    das_tasks_key_);
  if (OB_SUCC(ret)) {
    (void)ObSQLUtils::adjust_time_by_ntp_offset(timeout_ts_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASScanRtDef)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
    tenant_schema_version_,
    limit_param_,
    need_scn_,
    force_refresh_lc_,
    frozen_version_,
    fb_snapshot_,
    timeout_ts_,
    tx_lock_timeout_,
    sql_mode_,
    scan_flag_,
    pd_storage_flag_,
    need_check_output_datum_,
    is_for_foreign_check_,
    fb_read_tx_uncommitted_,
    key_ranges_,
    ss_key_ranges_,
    task_count_,
    scan_op_id_,
    scan_rows_size_,
    row_width_,
    das_tasks_key_);
  return len;
}

ObDASScanRtDef::~ObDASScanRtDef()
{
  if (p_row2exprs_projector_ != nullptr) {
    p_row2exprs_projector_->~ObRow2ExprsProjector();
    p_row2exprs_projector_ = nullptr;
  }
  if (p_pd_expr_op_ != nullptr) {
    p_pd_expr_op_->~ObPushdownOperator();
    p_pd_expr_op_ = nullptr;
  }
}

int ObDASScanRtDef::init_pd_op(ObExecContext &exec_ctx,
                               const ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(p_row2exprs_projector_)) {
    p_row2exprs_projector_ =
        new(&row2exprs_projector_) ObRow2ExprsProjector(exec_ctx.get_allocator());
  }
  if (nullptr == p_pd_expr_op_) {
    if (OB_ISNULL(eval_ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(eval_ctx_));
    } else if (FALSE_IT(p_pd_expr_op_ = new(&pd_expr_op_) ObPushdownOperator(*eval_ctx_,
                                                                             scan_ctdef.pd_expr_spec_,
                                                                             enable_rich_format()))) {
    } else if (OB_FAIL(pd_expr_op_.init_pushdown_storage_filter())) {
      LOG_WARN("init pushdown storage filter failed", K(ret));
    } else if (OB_NOT_NULL(scan_ctdef.trans_info_expr_)) {
      //do nothing
    }
  }
  return ret;
}

ObDASScanOp::ObDASScanOp(ObIAllocator &op_alloc)
  : ObIDASTaskOp(op_alloc),
    scan_param_(),
    scan_ctdef_(nullptr),
    scan_rtdef_(nullptr),
    result_(nullptr),
    remain_row_cnt_(0),
    tablet_ids_(op_alloc),
    retry_alloc_(nullptr),
    ir_param_()
{
}

ObDASScanOp::~ObDASScanOp()
{
  scan_param_.destroy();
  trans_info_array_.destroy();

  if (retry_alloc_ != nullptr) {
    retry_alloc_->reset();
    retry_alloc_ = nullptr;
  }
}

int ObDASScanOp::swizzling_remote_task(ObDASRemoteInfo *remote_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIDASTaskOp::swizzling_remote_task(remote_info))) {
    LOG_WARN("fail to swizzling remote task", K(ret));
  } else if (remote_info != nullptr) {
    //DAS scan is executed remotely
    trans_desc_ = remote_info->trans_desc_;
    scan_rtdef_->stmt_allocator_.set_alloc(&CURRENT_CONTEXT->get_arena_allocator());
    scan_rtdef_->scan_allocator_.set_alloc(&CURRENT_CONTEXT->get_arena_allocator());
    scan_rtdef_->tsc_monitor_info_ = remote_info->tsc_monitor_info_;
    if (OB_FAIL(scan_rtdef_->init_pd_op(*remote_info->exec_ctx_, *scan_ctdef_))) {
      LOG_WARN("init scan pushdown operator failed", K(ret));
    } else {
      scan_rtdef_->p_pd_expr_op_->get_eval_ctx()
            .set_max_batch_size(scan_ctdef_->pd_expr_spec_.max_batch_size_);
      bool is_vectorized = scan_rtdef_->p_pd_expr_op_->is_vectorized();
      if (!scan_rtdef_->p_pd_expr_op_->is_vectorized()) {
        scan_rtdef_->p_pd_expr_op_->get_eval_ctx().set_batch_size(1);
        scan_rtdef_->p_pd_expr_op_->get_eval_ctx().set_batch_idx(0);
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < related_rtdefs_.count(); ++i) {
      if (OB_NOT_NULL(related_rtdefs_.at(i)) &&
         (DAS_OP_TABLE_SCAN == related_rtdefs_.at(i)->op_type_ ||
          DAS_OP_TABLE_BATCH_SCAN == related_rtdefs_.at(i)->op_type_)) {
        const ObDASScanCtDef *related_ctdef = static_cast<const ObDASScanCtDef*>(related_ctdefs_.at(i));
        ObDASScanRtDef *related_rtdef = static_cast<ObDASScanRtDef*>(related_rtdefs_.at(i));
        related_rtdef->stmt_allocator_.set_alloc(&CURRENT_CONTEXT->get_arena_allocator());
        related_rtdef->scan_allocator_.set_alloc(&CURRENT_CONTEXT->get_arena_allocator());
        related_rtdef->tsc_monitor_info_ = remote_info->tsc_monitor_info_;
        if (OB_FAIL(related_rtdef->init_pd_op(*remote_info->exec_ctx_, *related_ctdef))) {
          LOG_WARN("init related rtdef pushdown operator failed", K(ret));
        } else {
          related_rtdef->p_pd_expr_op_->get_eval_ctx()
              .set_max_batch_size(related_ctdef->pd_expr_spec_.max_batch_size_);
          if (!related_rtdef->p_pd_expr_op_->is_vectorized()) {
            related_rtdef->p_pd_expr_op_->get_eval_ctx().set_batch_size(1);
            related_rtdef->p_pd_expr_op_->get_eval_ctx().set_batch_idx(0);
          }
        }
      }
    }
  }
  return ret;
}

int ObDASScanOp::init_scan_param()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  scan_param_.tenant_id_ = tenant_id;
  scan_param_.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
  scan_param_.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
  scan_param_.tx_lock_timeout_ = scan_rtdef_->tx_lock_timeout_;
  scan_param_.index_id_ = scan_ctdef_->ref_table_id_;
  scan_param_.is_get_ = scan_ctdef_->is_get_;
  scan_param_.is_for_foreign_check_ = scan_rtdef_->is_for_foreign_check_;
  scan_param_.timeout_ = scan_rtdef_->timeout_ts_;
  scan_param_.scan_flag_ = scan_rtdef_->scan_flag_;
  scan_param_.reserved_cell_count_ = scan_ctdef_->access_column_ids_.count();
  if (in_part_retry_ && retry_alloc_ != nullptr) {
    scan_param_.allocator_      = retry_alloc_;
    scan_param_.scan_allocator_ = retry_alloc_;
  } else {
    scan_param_.allocator_ = &scan_rtdef_->stmt_allocator_;
    scan_param_.scan_allocator_ = &scan_rtdef_->scan_allocator_;
  }
  scan_param_.sql_mode_ = scan_rtdef_->sql_mode_;
  scan_param_.frozen_version_ = scan_rtdef_->frozen_version_;
  scan_param_.force_refresh_lc_ = scan_rtdef_->force_refresh_lc_;
  scan_param_.output_exprs_ = &(scan_ctdef_->pd_expr_spec_.access_exprs_);
  scan_param_.ext_file_column_exprs_ = &(scan_ctdef_->pd_expr_spec_.ext_file_column_exprs_);
  scan_param_.ext_column_convert_exprs_ = &(scan_ctdef_->pd_expr_spec_.ext_column_convert_exprs_);
  scan_param_.calc_exprs_ = &(scan_ctdef_->pd_expr_spec_.calc_exprs_);
  scan_param_.aggregate_exprs_ = &(scan_ctdef_->pd_expr_spec_.pd_storage_aggregate_output_);
  scan_param_.table_param_ = &(scan_ctdef_->table_param_);
  scan_param_.op_ = scan_rtdef_->p_pd_expr_op_;
  scan_param_.row2exprs_projector_ = scan_rtdef_->p_row2exprs_projector_;
  scan_param_.schema_version_ = scan_ctdef_->schema_version_;
  scan_param_.tenant_schema_version_ = scan_rtdef_->tenant_schema_version_;
  scan_param_.limit_param_ = scan_rtdef_->limit_param_;
  scan_param_.need_scn_ = scan_rtdef_->need_scn_;
  scan_param_.pd_storage_flag_ = scan_ctdef_->pd_expr_spec_.pd_storage_flag_.pd_flag_;
  scan_param_.table_scan_opt_ = scan_ctdef_->table_scan_opt_;
  scan_param_.fb_snapshot_ = scan_rtdef_->fb_snapshot_;
  scan_param_.fb_read_tx_uncommitted_ = scan_rtdef_->fb_read_tx_uncommitted_;
  scan_param_.auto_split_filter_type_ = scan_ctdef_->pd_expr_spec_.auto_split_filter_type_;
  scan_param_.auto_split_filter_ = scan_ctdef_->pd_expr_spec_.auto_split_expr_;
  scan_param_.auto_split_params_ = const_cast<ExprFixedArray *>(&(scan_ctdef_->pd_expr_spec_.auto_split_params_));

  scan_param_.is_mds_query_ = false;
  scan_param_.main_table_scan_stat_.tsc_monitor_info_ = scan_rtdef_->tsc_monitor_info_;
  scan_param_.in_row_cache_threshold_ = scan_rtdef_->in_row_cache_threshold_;
  if (scan_rtdef_->is_for_foreign_check_) {
    scan_param_.trans_desc_ = trans_desc_;
  }
  scan_param_.ls_id_ = ls_id_;
  scan_param_.tablet_id_ = tablet_id_;
  if (scan_rtdef_->sample_info_ != nullptr) {
    scan_param_.sample_info_ = *scan_rtdef_->sample_info_;
  }
  if (OB_NOT_NULL(snapshot_)) {
    if (OB_FAIL(scan_param_.snapshot_.assign(*snapshot_))) {
      LOG_WARN("assign snapshot fail", K(ret));
    } else if (snapshot_->read_elr() && !scan_param_.trans_desc_) {
      scan_param_.trans_desc_ = trans_desc_;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("snapshot is null", K(ret), KPC(this));
  }
  // set tx_id for read-latest,
  // TODO: add if(query_flag.is_read_latest) ..
  if (OB_NOT_NULL(trans_desc_)) {
    scan_param_.tx_id_ = trans_desc_->get_tx_id();
    scan_param_.tx_seq_base_ = trans_desc_->get_seq_base();
  } else {
    scan_param_.tx_id_.reset();
  }
  if (!scan_ctdef_->pd_expr_spec_.pushdown_filters_.empty()) {
    scan_param_.op_filters_ = &scan_ctdef_->pd_expr_spec_.pushdown_filters_;
  }
  scan_param_.pd_storage_filters_ = scan_rtdef_->p_pd_expr_op_->pd_storage_filters_;
  if (FAILEDx(scan_param_.column_ids_.assign(scan_ctdef_->access_column_ids_))) {
    LOG_WARN("init column ids failed", K(ret));
  }
  //external table scan params
  if (OB_SUCC(ret) && scan_ctdef_->is_external_table_) {
    scan_param_.partition_infos_ = &(scan_ctdef_->partition_infos_);
    scan_param_.external_file_access_info_ = scan_ctdef_->external_file_access_info_.str_;
    scan_param_.external_file_location_ = scan_ctdef_->external_file_location_.str_;
    if (OB_FAIL(scan_param_.external_file_format_.load_from_string(scan_ctdef_->external_file_format_str_.str_, *scan_param_.allocator_))) {
      LOG_WARN("fail to load from string", K(ret));
    } else {
      uint64_t max_idx = 0;
      for (int i = 0; i < scan_param_.ext_file_column_exprs_->count(); i++) {
        if (scan_param_.ext_file_column_exprs_->at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
          max_idx = std::max(max_idx, scan_param_.ext_file_column_exprs_->at(i)->extra_);
        }
      }
      scan_param_.external_file_format_.csv_format_.file_column_nums_ = static_cast<int64_t>(max_idx);
      scan_param_.external_file_format_.csv_format_.ignore_extra_fields_ = true;
    }
  }
  LOG_DEBUG("init scan param", K(ret), K(scan_param_));
  return ret;
}

ObITabletScan &ObDASScanOp::get_tsc_service()
{
  return is_virtual_table(scan_ctdef_->ref_table_id_) ? *GCTX.vt_par_ser_
                                                      : scan_ctdef_->is_external_table_ ? *GCTX.et_access_service_
                                                                                        : *(MTL(ObAccessService *));
}

ObDASIterTreeType ObDASScanOp::get_iter_tree_type() const
{
  ObDASIterTreeType tree_type = ObDASIterTreeType::ITER_TREE_INVALID;
  bool is_fts_index = scan_param_.table_param_->is_fts_index() && attach_ctdef_ != nullptr;
  bool is_vector_index = is_vec_idx_scan(attach_ctdef_);
  bool is_spatial_index = scan_param_.table_param_->is_spatial_index() && !is_vector_index;
  bool is_multivalue_index = scan_param_.table_param_->is_multivalue_index();

  if (is_func_lookup(attach_ctdef_)) {
    tree_type = ObDASIterTreeType::ITER_TREE_FUNC_LOOKUP;
  } else if (is_fts_index) {
    tree_type = ObDASIterTreeType::ITER_TREE_TEXT_RETRIEVAL;
  } else if (is_spatial_index) {
    tree_type = ObDASIterTreeType::ITER_TREE_GIS_LOOKUP;
  } else if (is_multivalue_index) {
    tree_type = ObDASIterTreeType::ITER_TREE_MVI_LOOKUP;
  } else if (is_vector_index) {
    tree_type = ObDASIterTreeType::ITER_TREE_VEC_LOOKUP;
  } else if (OB_UNLIKELY(is_index_merge(attach_ctdef_))) {
    tree_type = ObDASIterTreeType::ITER_TREE_INDEX_MERGE;
  } else {
    tree_type = OB_ISNULL(get_lookup_ctdef()) ? ObDASIterTreeType::ITER_TREE_PARTITION_SCAN
                                              : ObDASIterTreeType::ITER_TREE_LOCAL_LOOKUP;
  }
  LOG_TRACE("get iter tree type", K(tree_type), K(scan_param_.tablet_id_), KPC(attach_ctdef_));
  return tree_type;
}

bool ObDASScanOp::is_index_merge(const ObDASBaseCtDef *attach_ctdef) const
{
  bool bret = false;
  if (attach_ctdef != nullptr) {
    const ObDASBaseCtDef *rowkey_scan_ctdef = nullptr;
    if (attach_ctdef->op_type_ == DAS_OP_INDEX_MERGE) {
      bret = true;
    } else if (attach_ctdef->op_type_ == DAS_OP_TABLE_LOOKUP &&
               FALSE_IT(rowkey_scan_ctdef = static_cast<const ObDASTableLookupCtDef*>(attach_ctdef)->get_rowkey_scan_ctdef())) {
    } else if (attach_ctdef->op_type_ == DAS_OP_INDEX_PROJ_LOOKUP &&
               FALSE_IT(rowkey_scan_ctdef = static_cast<const ObDASIndexProjLookupCtDef*>(attach_ctdef)->get_rowkey_scan_ctdef())) {
    } else if (rowkey_scan_ctdef != nullptr) {
      bret = rowkey_scan_ctdef->op_type_ == DAS_OP_INDEX_MERGE;
    }
  }
  return bret;
}

bool ObDASScanOp::is_func_lookup(const ObDASBaseCtDef *attach_ctdef) const
{
  bool bret = false;
  if (nullptr != attach_ctdef && attach_ctdef->op_type_ == ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP) {
    const ObDASBaseCtDef *lookup_ctdef = static_cast<const ObDASIndexProjLookupCtDef *>(attach_ctdef)->get_lookup_ctdef();
    if (OB_NOT_NULL(lookup_ctdef)) {
      bret = lookup_ctdef->op_type_ == ObDASOpType::DAS_OP_FUNC_LOOKUP;
    }
  }
  return bret;
}

bool ObDASScanOp::is_vec_idx_scan(const ObDASBaseCtDef *attach_ctdef) const
{
  int ret = OB_SUCCESS;

  bool bret = false;
  if (attach_ctdef != nullptr) {
    const ObDASBaseCtDef *vir_scan_ctdef = nullptr;
    ObDASBaseRtDef *vir_scan_rtdef = nullptr;
    if (OB_FAIL(ObDASUtils::find_child_das_def(attach_ctdef_, attach_rtdef_, DAS_OP_VEC_SCAN, vir_scan_ctdef, vir_scan_rtdef))) {
      SQL_DAS_LOG(WARN, "find chld das def failed", K(ret));
    } else {
      bret = (nullptr != vir_scan_ctdef) && (nullptr != vir_scan_rtdef);
    }
  }

  return bret;
}

storage::ObTableScanParam* ObDASScanOp::get_local_lookup_param()
{
  storage::ObTableScanParam* lookup_param = nullptr;
  if (is_local_task() && ITER_TREE_LOCAL_LOOKUP == get_iter_tree_type() && OB_NOT_NULL(result_)) {
    ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter*>(result_);
    lookup_param = &lookup_iter->get_lookup_param();
  }
  return lookup_param;
}

int ObDASScanOp::init_related_tablet_ids(ObDASRelatedTabletID &related_tablet_ids)
{
  int ret = OB_SUCCESS;
  related_tablet_ids.reset();

  bool is_vec_index_query = is_vec_idx_scan(attach_ctdef_);
  if (OB_FAIL(get_table_lookup_tablet_id(related_tablet_ids.lookup_tablet_id_))) {
    LOG_WARN("failed to get table lookup tablet id", K(ret));
  } else if (OB_ISNULL(attach_ctdef_) || OB_ISNULL(attach_rtdef_)) { // no attached task.
  } else if (OB_UNLIKELY(related_ctdefs_.count() != related_tablet_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected related scan array not match", K(ret), K_(related_ctdefs), K_(related_tablet_ids));
  } else if (OB_FAIL(get_rowkey_domain_tablet_id(related_tablet_ids))) {
    LOG_WARN("fail to get rowkey doc tablet id", K(ret));
  } else if (OB_FAIL(get_doc_rowkey_tablet_id(related_tablet_ids.doc_rowkey_tablet_id_))) {
    LOG_WARN("failed to get doc rowkey tablet id", K(ret));
  } else if (OB_FAIL(get_index_merge_tablet_ids(related_tablet_ids.index_merge_tablet_ids_))) {
    LOG_WARN("failed to get index merge tablet ids", K(ret));
  } else if (OB_FAIL(get_fts_tablet_ids(related_tablet_ids.fts_tablet_ids_, attach_rtdef_))) {
    LOG_WARN("failed to get fts tablet ids", K(ret));
  } else if (is_vec_index_query && OB_FAIL(get_vec_ir_tablet_ids(related_tablet_ids))) {
    LOG_WARN("failed to get vec aux tablet id", K(ret), K_(related_tablet_ids));
  }
  return ret;
}

int ObDASScanOp::open_op()
{
  int ret = OB_SUCCESS;
  //Retry may be called many times.
  //Only for DASScanOp now, we add a retry alloc to avoid
  //memory expansion.
  if (in_part_retry_) {
    init_retry_alloc();
  }
  reset_access_datums_ptr();
  ObDASIterTreeType tree_type = ITER_TREE_INVALID;
  if (OB_FAIL(init_scan_param())) {
    LOG_WARN("init scan param failed", K(ret));
  } else if (FALSE_IT(tree_type = get_iter_tree_type())) {
  } else if (SUPPORTED_DAS_ITER_TREE(tree_type)) {
    ObDASIter *result = nullptr;
    if (OB_FAIL(init_related_tablet_ids(tablet_ids_))) {
    LOG_WARN("failed to init related tablet ids", K(ret));
    } else if (OB_FAIL(ObDASIterUtils::create_das_scan_iter_tree(tree_type,
                                                                 scan_param_,
                                                                 scan_ctdef_,
                                                                 scan_rtdef_,
                                                                 get_lookup_ctdef(),
                                                                 get_lookup_rtdef(),
                                                                 attach_ctdef_,
                                                                 attach_rtdef_,
                                                                 tablet_ids_,
                                                                 trans_desc_,
                                                                 snapshot_,
                                                                 op_alloc_,
                                                                 result))) {
      LOG_WARN("failed to create das scan iter tree", K(get_iter_tree_type()), K(ret));
    } else {
      result_ = result;
      if (OB_FAIL(result->do_table_scan())) {
        LOG_WARN("iter tree failed to do table scan", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter tree type", K(ret), K(tree_type));
  }

  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_LOOKUPOP_INIT_ERROR);
  if (OB_UNLIKELY(OB_SUCCESS != simulate_error)) {
    ret = simulate_error;
  }
  return ret;
}

int ObDASScanOp::release_op()
{
  int ret = OB_SUCCESS;
  ObDASIterTreeType tree_type = get_iter_tree_type();
  if (SUPPORTED_DAS_ITER_TREE(tree_type)) {
    if (OB_NOT_NULL(result_)) {
      ObDASIter *result = static_cast<ObDASIter*>(result_);
      if (OB_FAIL(result->release())) {
        LOG_WARN("failed to release das iter tree", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter tree type", K(ret), K(tree_type));
  }
  //need to clear the flag:need_switch_param_
  //otherwise table_rescan will jump to the switch iterator path in retry
  scan_param_.need_switch_param_ = false;
  scan_param_.destroy_schema_guard();

  if (retry_alloc_ != nullptr) {
    retry_alloc_->reset();
    retry_alloc_ = nullptr;
  }
  tablet_ids_.reset();

  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_LOOKUPOP_INIT_ERROR);
  if (OB_UNLIKELY(OB_SUCCESS != simulate_error)) {
    if (result_ != nullptr) {
      if (ObNewRowIterator::IterType::ObLocalIndexLookupIterator == result_->get_type() ||
          ObNewRowIterator::IterType::ObGroupLookupOp == result_->get_type()) {
        ObLocalIndexLookupOp *lookup_op = static_cast<ObLocalIndexLookupOp*>(result_);
        if (lookup_op->get_rowkey_iter() != nullptr && lookup_op->get_rowkey_iter()->get_type() == ObNewRowIterator::ObTableScanIterator) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("after executing release_op(), the rowkey_iter_ is still not null", K(ret));
        }
      }
    }
  }
  return ret;
}

//output row from remote DAS task
//maybe change the expr datum ptr to its RPC datum store
//if we need to fetch next row from the next das task,
//must reset the datum ptr to expr preallocate frame buffer
//otherwise, get_next_row in the local das task maybe has a wrong status
void ObDASScanOp::reset_access_datums_ptr(int64_t capacity)
{
  if (scan_rtdef_->p_pd_expr_op_->is_vectorized() && OB_NOT_NULL(scan_rtdef_->eval_ctx_)) {
    int64_t reset_batch_size = capacity > 0 ? capacity : scan_rtdef_->eval_ctx_->max_batch_size_;
    reset_batch_size = min(reset_batch_size, scan_rtdef_->eval_ctx_->max_batch_size_);
    if (attach_ctdef_ != nullptr) {
      reset_access_datums_ptr(attach_ctdef_, *scan_rtdef_->eval_ctx_, reset_batch_size);
    } else {
      reset_access_datums_ptr(scan_ctdef_, *scan_rtdef_->eval_ctx_, reset_batch_size);
      if (get_lookup_ctdef() != nullptr) {
        reset_access_datums_ptr(get_lookup_ctdef(), *scan_rtdef_->eval_ctx_, reset_batch_size);
      }
    }
  }
}

void ObDASScanOp::reset_access_datums_ptr(const ObDASBaseCtDef *ctdef, ObEvalCtx &eval_ctx, int64_t capacity)
{
  if (ctdef != nullptr) {
    if (ctdef->op_type_== DAS_OP_TABLE_SCAN) {
      const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(ctdef);
      FOREACH_CNT(e, scan_ctdef->pd_expr_spec_.access_exprs_) {
        (*e)->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = (*e)->get_eval_info(eval_ctx);
        info.point_to_frame_ = true;
      }
      FOREACH_CNT(e, scan_ctdef->pd_expr_spec_.pd_storage_aggregate_output_) {
        (*e)->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = (*e)->get_eval_info(eval_ctx);
        info.point_to_frame_ = true;
      }
      FOREACH_CNT(e, scan_ctdef->pd_expr_spec_.ext_file_column_exprs_) {
        (*e)->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = (*e)->get_eval_info(eval_ctx);
        info.point_to_frame_ = true;
      }
      if (scan_ctdef->trans_info_expr_ != nullptr) {
        ObExpr *trans_expr = scan_ctdef->trans_info_expr_;
        trans_expr->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = trans_expr->get_eval_info(eval_ctx);
        info.point_to_frame_ = true;
      }
    } else {
      for (int64_t i = 0; i < ctdef->children_cnt_; i++) {
        reset_access_datums_ptr(ctdef->children_[i], eval_ctx, capacity);
      }
    }
  }
}

//对于远程执行返回的TSC result，DASScanOp解析ObDASTaskResult,
//并将result iterator link到DASScan Task上，输出给对应的TableScan Operator
int ObDASScanOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(*task_result) == typeid(ObDASScanResult));
  CK(task_id_ == task_result->get_task_id());
  CK(OB_NOT_NULL(scan_rtdef_));
  CK(OB_NOT_NULL(scan_rtdef_->tsc_monitor_info_));
#endif
  if (need_check_output_datum()) {
    reset_access_datums_ptr();
  }
  ObDASScanResult *scan_result = static_cast<ObDASScanResult*>(task_result);
  if (OB_FAIL(scan_result->init_result_iter(&get_result_outputs(),
                                            &(scan_rtdef_->p_pd_expr_op_->get_eval_ctx())))) {
    LOG_WARN("init scan result iterator failed", K(ret));
  } else {
    result_ = scan_result;
    LOG_DEBUG("decode task result", K(*scan_result));
    // Aggregate the remote TSC statistical information into the local monitor node.
    if (OB_NOT_NULL(scan_rtdef_->tsc_monitor_info_)) {
      ObTSCMonitorInfo &tsc_monitor_info = *scan_rtdef_->tsc_monitor_info_;
      tsc_monitor_info.add_io_read_bytes(scan_result->get_io_read_bytes());
      tsc_monitor_info.add_ssstore_read_bytes(scan_result->get_ssstore_read_bytes());
      tsc_monitor_info.add_ssstore_read_row_cnt(scan_result->get_ssstore_read_row_cnt());
      tsc_monitor_info.add_memstore_read_row_cnt(scan_result->get_memstore_read_row_cnt());
    }
  }
  return ret;
}

//远程执行返回的TSC result,通过RPC回包带回给DAS Scheduler，
//如果结果集超过一个RPC，标记RPC包为has_more，剩余结果集通过DTL传输回DAS Scheduler
int ObDASScanOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
  bool added = false;
  int64_t simulate_row_cnt = - EVENT_CALL(EventTable::EN_DAS_SCAN_RESULT_OVERFLOW);
  has_more = false;
  ObDASScanResult &scan_result = static_cast<ObDASScanResult&>(task_result);
  ObChunkDatumStore &datum_store = scan_result.get_datum_store();
  ObTempRowStore &vec_row_store = scan_result.get_vec_row_store();
  bool iter_end = false;
  int64_t loop_times = 0;
  while (OB_SUCC(ret) && !has_more) {
    const ExprFixedArray &result_output = get_result_outputs();
    ObEvalCtx &eval_ctx = scan_rtdef_->p_pd_expr_op_->get_eval_ctx();
    if (loop_times % 16 == 0 && OB_UNLIKELY(IS_INTERRUPTED())) {
      ObInterruptCode code = GET_INTERRUPT_CODE();
      ret = code.code_;
      LOG_WARN("received a interrupt", K(code), K(ret));
    } else {
      if (!scan_rtdef_->p_pd_expr_op_->is_vectorized()) {
        scan_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
        if (OB_FAIL(get_output_result_iter()->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row from result failed", K(ret));
          }
        } else if (OB_UNLIKELY(simulate_row_cnt > 0
                  && datum_store.get_row_cnt() >= simulate_row_cnt)) {
          // simulate a datum store overflow error, send the remaining result through RPC
          has_more = true;
          remain_row_cnt_ = 1;
        } else if (OB_FAIL(datum_store.try_add_row(result_output,
                                                   &eval_ctx,
                                                   das::OB_DAS_MAX_PACKET_SIZE,
                                                   added))) {
          LOG_WARN("try add row to datum store failed", K(ret));
        } else if (!added) {
          has_more = true;
          remain_row_cnt_ = 1;
        }
        if (OB_SUCC(ret) && has_more) {
          LOG_DEBUG("try fill task result", K(simulate_row_cnt),
                    K(datum_store.get_row_cnt()), K(has_more),
                    "output_row", ROWEXPR2STR(eval_ctx, result_output));
        }
      } else {
        int64_t max_batch_size = scan_ctdef_->pd_expr_spec_.max_batch_size_;
        scan_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
        remain_row_cnt_ = 0;
        if (iter_end) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(get_output_result_iter()->get_next_rows(remain_row_cnt_, max_batch_size))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next batch from result failed", K(ret));
          } else {
            iter_end = true;
            ret = OB_SUCCESS;
          }
        }
        if (enable_rich_format()) {
          LOG_DEBUG("rich format fill task result", K(remain_row_cnt_), K(memory_limit), K(ret), K(scan_result));
          if (OB_FAIL(ret) || 0 == remain_row_cnt_) {
          } else if (OB_UNLIKELY(simulate_row_cnt > 0
                    && vec_row_store.get_row_cnt() >= simulate_row_cnt)) {
            // simulate a datum store overflow error, send the remaining result through RPC
            has_more = true;
          } else if (OB_UNLIKELY(OB_FAIL(vec_row_store.try_add_batch(result_output, &eval_ctx,
                                                          remain_row_cnt_, memory_limit,
                                                          added)))) {
            LOG_WARN("try add row to datum store failed", K(ret));
          } else if (!added) {
            has_more = true;
          }
        } else {
          if (OB_FAIL(ret) || 0 == remain_row_cnt_) {
          } else if (OB_UNLIKELY(simulate_row_cnt > 0
                    && datum_store.get_row_cnt() >= simulate_row_cnt)) {
            // simulate a datum store overflow error, send the remaining result through RPC
            has_more = true;
          } else if (OB_UNLIKELY(OB_FAIL(datum_store.try_add_batch(result_output, &eval_ctx,
                                                          remain_row_cnt_, memory_limit,
                                                          added)))) {
            LOG_WARN("try add row to datum store failed", K(ret));
          } else if (!added) {
            has_more = true;
          }
        }
        if (OB_SUCC(ret) && has_more) {
          const ObBitVector *skip = NULL;
          PRINT_VECTORIZED_ROWS(SQL, DEBUG, eval_ctx, result_output, remain_row_cnt_, skip,
                                K(simulate_row_cnt), K(datum_store.get_row_cnt()),
                                K(has_more));
        }
      }
    }
    ++ loop_times;
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (enable_rich_format()) {
    memory_limit -= vec_row_store.get_mem_used();
  } else {
    memory_limit -= datum_store.get_mem_used();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(scan_rtdef_->tsc_monitor_info_)) {
    scan_result.add_io_read_bytes(*scan_rtdef_->tsc_monitor_info_->io_read_bytes_);
    scan_result.add_ssstore_read_bytes(*scan_rtdef_->tsc_monitor_info_->ssstore_read_bytes_);
    scan_result.add_ssstore_read_row_cnt(*scan_rtdef_->tsc_monitor_info_->ssstore_read_row_cnt_);
    scan_result.add_memstore_read_row_cnt(*scan_rtdef_->tsc_monitor_info_->memstore_read_row_cnt_);
    scan_rtdef_->tsc_monitor_info_->reset_stat();
  }
  return ret;
}

int ObDASScanOp::fill_extra_result(const ObDASTCBInterruptInfo &interrupt_info)
{
  int ret = OB_SUCCESS;
  ObDASTaskResultMgr &result_mgr = MTL(ObDataAccessService *)->get_task_res_mgr();
  const ExprFixedArray &result_output = get_result_outputs();
  ObEvalCtx &eval_ctx = scan_rtdef_->p_pd_expr_op_->get_eval_ctx();
  ObNewRowIterator *output_result_iter = get_output_result_iter();
  if (OB_ISNULL(output_result_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("output result iter is null", K(ret));
  } else if (OB_FAIL(result_mgr.save_task_result(task_id_,
                                                 interrupt_info,
                                                 &result_output,
                                                 &eval_ctx,
                                                 *output_result_iter,
                                                 remain_row_cnt_,
                                                 scan_ctdef_,
                                                 scan_rtdef_,
                                                 *this))) {
    LOG_WARN("save task result failed", KR(ret), K(task_id_), K(interrupt_info));
  }
  return ret;
}

int ObDASScanOp::init_task_info(uint32_t row_extend_size)
{
  UNUSED(row_extend_size);
  return OB_SUCCESS;
}

int ObDASScanOp::rescan()
{
  int &ret = errcode_;
  reset_access_datums_ptr();
  scan_param_.tablet_id_ = tablet_id_;
  scan_param_.ls_id_ = ls_id_;

  LOG_DEBUG("begin to das table rescan",
            "ls_id", scan_param_.ls_id_,
            "tablet_id", scan_param_.tablet_id_,
            "scan_range", scan_param_.key_ranges_,
            "range_pos", scan_param_.range_array_pos_);
  ObDASIterTreeType tree_type = get_iter_tree_type();
  if (SUPPORTED_DAS_ITER_TREE(tree_type)) {
    if (OB_ISNULL(result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr das iter tree", K(ret));
    } else {
      ObDASIter *result = static_cast<ObDASIter*>(result_);
      if (OB_FAIL(result->rescan())) {
        LOG_WARN("failed to rescan das iter", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter tree type", K(ret), K(tree_type));
  }

  return ret;
}

int ObDASScanOp::reuse_iter()
{
  int &ret = errcode_;
  //May be retry change to retry alloc.
  //Change back.
  scan_param_.scan_allocator_ = &scan_rtdef_->scan_allocator_;
  const ObTabletID &storage_tablet_id = scan_param_.tablet_id_;
  scan_param_.need_switch_param_ = (storage_tablet_id.is_valid() && storage_tablet_id != tablet_id_ ? true : false);
  scan_param_.key_ranges_.reuse();
  scan_param_.ss_key_ranges_.reuse();
  scan_param_.mbr_filters_.reuse();
  ObDASIterTreeType tree_type = get_iter_tree_type();
  if (SUPPORTED_DAS_ITER_TREE(tree_type)) {
    if (OB_ISNULL(result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr das iter tree", K(ret));
    } else {
      ObDASIter *result = static_cast<ObDASIter*>(result_);
      ObDASDomainIdMergeIter *domain_id_merge_iter = nullptr;
      if (OB_FAIL(init_related_tablet_ids(tablet_ids_))) {
        LOG_WARN("fail to init related tablet ids", K(ret));
      } else {
        switch (get_iter_tree_type()) {
          case ITER_TREE_PARTITION_SCAN: {
            break;
          }
          case ITER_TREE_LOCAL_LOOKUP: {
            ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter*>(result_);
            lookup_iter->set_tablet_id(tablet_ids_.lookup_tablet_id_);
            lookup_iter->set_ls_id(ls_id_);
            break;
          }
          case ITER_TREE_TEXT_RETRIEVAL: {
            ObDASIter *result_iter = static_cast<ObDASIter *>(result_);
            if (OB_FAIL(ObDASIterUtils::set_text_retrieval_related_ids(
                attach_ctdef_, attach_rtdef_, tablet_ids_, ls_id_, result_iter))) {
              LOG_WARN("failed to set text retrieval related ids", K(ret));
            }
            break;
          }
          case ITER_TREE_INDEX_MERGE: {
            ObDASIter *result_iter = static_cast<ObDASIter *>(result_);
            if (OB_FAIL(ObDASIterUtils::set_index_merge_related_ids(
                attach_ctdef_, attach_rtdef_, tablet_ids_, ls_id_, result_iter))) {
              LOG_WARN("failed to set index merge related ids", K(ret));
            }
            break;
          }
          case ITER_TREE_FUNC_LOOKUP: {
            ObDASIter *result_iter = static_cast<ObDASIter *>(result_);
            if (OB_FAIL(ObDASIterUtils::set_func_lookup_iter_related_ids(
                attach_ctdef_, attach_rtdef_, tablet_ids_, ls_id_, result_iter))) {
              LOG_WARN("failed to set text retrieval related ids", K(ret));
            }
            break;
          }
          case ITER_TREE_MVI_LOOKUP: {
            if (OB_NOT_NULL(get_lookup_ctdef())) {
              ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter*>(result_);
              lookup_iter->set_tablet_id(tablet_ids_.lookup_tablet_id_);
              lookup_iter->set_ls_id(ls_id_);
            }
            break;
          }
          case ITER_TREE_GIS_LOOKUP: {
            ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter*>(result_);
            lookup_iter->set_tablet_id(tablet_ids_.lookup_tablet_id_);
            lookup_iter->set_ls_id(ls_id_);
            break;
          }
          case ITER_TREE_VEC_LOOKUP: {
            ObDASIter *result_iter = static_cast<ObDASIter *>(result_);
            if (OB_FAIL(ObDASIterUtils::set_vec_lookup_related_ids(
                attach_ctdef_, attach_rtdef_, tablet_ids_, ls_id_, result_iter))) {
              LOG_WARN("failed to set index merge related ids", K(ret));
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      if (FAILEDx(result->get_domain_id_merge_iter(domain_id_merge_iter))) {
        LOG_WARN("fail to get domain id merge iter", K(ret));
      } else if (OB_NOT_NULL(domain_id_merge_iter) && OB_FAIL(domain_id_merge_iter->set_domain_id_merge_related_ids(tablet_ids_, ls_id_))) {
        LOG_WARN("fail to set domain id merge related ids", K(ret));
      } else if (OB_FAIL(result->reuse())) {
        LOG_WARN("failed to reuse das iter tree", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter tree type", K(ret), K(tree_type));
  }

  return ret;
}

const ExprFixedArray &ObDASScanOp::get_result_outputs() const
{
  const ExprFixedArray *result_output = nullptr;
  if (attach_ctdef_ != nullptr) {
    OB_ASSERT(ObDASTaskFactory::is_attached(attach_ctdef_->op_type_));
    result_output = &(static_cast<const ObDASAttachCtDef*>(attach_ctdef_)->result_output_);
  } else if (get_lookup_ctdef() != nullptr) {
    result_output = &get_lookup_ctdef()->result_output_;
  } else {
    result_output = &scan_ctdef_->result_output_;
  }
  return *result_output;
}

// get main table lookup ctdef
const ObDASScanCtDef *ObDASScanOp::get_lookup_ctdef() const
{
  const ObDASScanCtDef *lookup_ctdef = nullptr;
  const ObDASBaseCtDef *attach_ctdef = attach_ctdef_;
  if (OB_NOT_NULL(attach_ctdef) && DAS_OP_DOC_ID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    attach_ctdef = attach_ctdef->children_[0];
  } else if (OB_NOT_NULL(attach_ctdef) && DAS_OP_VID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    attach_ctdef = attach_ctdef->children_[0];
  }
  if (nullptr == attach_ctdef) {
    if (!related_ctdefs_.empty()) {
      OB_ASSERT(related_ctdefs_.count() == 1);
      OB_ASSERT(related_ctdefs_.at(0)->op_type_ == DAS_OP_TABLE_SCAN);
      lookup_ctdef = static_cast<const ObDASScanCtDef*>(related_ctdefs_.at(0));
    }
  } else {
    if (DAS_OP_TABLE_LOOKUP == attach_ctdef->op_type_) {
      const ObDASTableLookupCtDef *table_lookup_ctdef = static_cast<const ObDASTableLookupCtDef*>(attach_ctdef);
      lookup_ctdef = table_lookup_ctdef->get_lookup_scan_ctdef();
    } else if (DAS_OP_INDEX_PROJ_LOOKUP == attach_ctdef->op_type_) {
      // func lookup may involve main table lookup
      OB_ASSERT(2 == attach_ctdef->children_cnt_  && attach_ctdef->children_ != nullptr);
      if (DAS_OP_FUNC_LOOKUP == attach_ctdef->children_[1]->op_type_) {
        const ObDASFuncLookupCtDef *func_ctdef = static_cast<const ObDASFuncLookupCtDef*>(attach_ctdef->children_[1]);
        lookup_ctdef = static_cast<const ObDASScanCtDef*>(func_ctdef->get_main_lookup_scan_ctdef());
      } else {
        const ObDASIndexProjLookupCtDef *table_lookup_ctdef = static_cast<const ObDASIndexProjLookupCtDef*>(attach_ctdef);
        lookup_ctdef = table_lookup_ctdef->get_lookup_scan_ctdef();
      }
    }
  }
  return lookup_ctdef;
}

ObDASScanRtDef *ObDASScanOp::get_lookup_rtdef()
{
  ObDASScanRtDef *lookup_rtdef = nullptr;
  ObDASBaseRtDef *attach_rtdef = attach_rtdef_;
  if (OB_NOT_NULL(attach_rtdef) && DAS_OP_DOC_ID_MERGE == attach_rtdef->op_type_) {
    attach_rtdef = attach_rtdef->children_[0];
  } else if (OB_NOT_NULL(attach_rtdef) && DAS_OP_VID_MERGE == attach_rtdef->op_type_) {
    OB_ASSERT(2 == attach_rtdef->children_cnt_ && attach_rtdef->children_ != nullptr);
    attach_rtdef = attach_rtdef->children_[0];
  }
  if (nullptr == attach_rtdef) {
    if (!related_rtdefs_.empty()) {
      OB_ASSERT(related_rtdefs_.count() == 1);
      OB_ASSERT(related_rtdefs_.at(0)->op_type_ == DAS_OP_TABLE_SCAN);
      lookup_rtdef = static_cast<ObDASScanRtDef*>(related_rtdefs_.at(0));
    }
  } else {
    if (DAS_OP_TABLE_LOOKUP == attach_rtdef->op_type_) {
      ObDASTableLookupRtDef *table_lookup_rtdef = static_cast<ObDASTableLookupRtDef*>(attach_rtdef);
      lookup_rtdef = table_lookup_rtdef->get_lookup_scan_rtdef();
    } else if (DAS_OP_INDEX_PROJ_LOOKUP == attach_rtdef->op_type_) {
      OB_ASSERT(2 == attach_rtdef->children_cnt_  && attach_rtdef->children_ != nullptr);
      if (DAS_OP_FUNC_LOOKUP == attach_rtdef->children_[1]->op_type_) {
        const ObDASFuncLookupRtDef *func_rtdef = static_cast<const ObDASFuncLookupRtDef*>(attach_rtdef->children_[1]);
        lookup_rtdef = static_cast<ObDASScanRtDef*>(func_rtdef->get_main_lookup_scan_rtdef());
      } else {
        ObDASIndexProjLookupRtDef *table_lookup_rtdef = static_cast<ObDASIndexProjLookupRtDef*>(attach_rtdef);
        lookup_rtdef = table_lookup_rtdef->get_lookup_scan_rtdef();
      }
    }
  }
  return lookup_rtdef;
}

int ObDASScanOp::reserve_related_buffer(const int64_t related_scan_cnt)
{
  int ret = OB_SUCCESS;
  related_ctdefs_.clear();
  related_rtdefs_.clear();
  related_tablet_ids_.clear();
  OZ(related_ctdefs_.reserve(related_scan_cnt));
  OZ(related_rtdefs_.reserve(related_scan_cnt));
  OZ(related_tablet_ids_.reserve(related_scan_cnt));
  return ret;
}

int ObDASScanOp::set_related_task_info(const ObDASBaseCtDef *lookup_ctdef,
                                       ObDASBaseRtDef *lookup_rtdef,
                                       const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  OZ(related_ctdefs_.push_back(lookup_ctdef));
  OZ(related_rtdefs_.push_back(lookup_rtdef));
  OZ(related_tablet_ids_.push_back(tablet_id));
  return ret;
}

int ObDASScanOp::get_rowkey_domain_tablet_id(ObDASRelatedTabletID &related_tablet_ids) const
{
  int ret = OB_SUCCESS;
  if (nullptr != attach_ctdef_) {
    const ObDASBaseCtDef *ctdef = nullptr;
    if (ObDASOpType::DAS_OP_DOMAIN_ID_MERGE == attach_ctdef_->op_type_ ||
        ObDASOpType::DAS_OP_DOC_ID_MERGE == attach_ctdef_->op_type_ ||
        ObDASOpType::DAS_OP_VID_MERGE == attach_ctdef_->op_type_) {
      ctdef = attach_ctdef_;
    } else if (ObDASOpType::DAS_OP_TABLE_LOOKUP == attach_ctdef_->op_type_ || ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP == attach_ctdef_->op_type_) {
      if (OB_UNLIKELY(2 != attach_ctdef_->children_cnt_) || OB_ISNULL(attach_ctdef_->children_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected error, children of doc id merge ctdef is invalid", K(ret), KPC(attach_ctdef_));
      } else if (ObDASOpType::DAS_OP_DOMAIN_ID_MERGE == attach_ctdef_->children_[1]->op_type_ ||
                 ObDASOpType::DAS_OP_DOC_ID_MERGE == attach_ctdef_->children_[1]->op_type_ ||
                 ObDASOpType::DAS_OP_VID_MERGE == attach_ctdef_->children_[1]->op_type_) {
        ctdef = attach_ctdef_->children_[1];
      } else if (ObDASOpType::DAS_OP_FUNC_LOOKUP == attach_ctdef_->children_[1]->op_type_) {
        // func lookup may involve rowkey doc lookup
        const ObDASBaseCtDef *func_ctdef = static_cast<const ObDASIndexProjLookupCtDef *>(attach_ctdef_)->get_lookup_ctdef();
        ctdef = static_cast<const ObDASScanCtDef *>(static_cast<const ObDASFuncLookupCtDef *>(func_ctdef)->get_doc_id_lookup_scan_ctdef());
        related_tablet_ids.rowkey_doc_tablet_id_.reset();
        for (int64_t i = 0; OB_NOT_NULL(ctdef) && !related_tablet_ids.rowkey_doc_tablet_id_.is_valid() && i < related_ctdefs_.count(); ++i) {
          if (ctdef == related_ctdefs_.at(i)) {
            related_tablet_ids.rowkey_doc_tablet_id_ = related_tablet_ids_.at(i);
          }
        }
        if (OB_NOT_NULL(ctdef)) {
          if (ObDASOpType::DAS_OP_DOMAIN_ID_MERGE != ctdef->op_type_
              && ObDASOpType::DAS_OP_DOC_ID_MERGE != ctdef->op_type_
              && ObDASOpType::DAS_OP_VID_MERGE    != ctdef->op_type_) {
            ctdef = nullptr;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(ctdef)) {
      if (OB_UNLIKELY(2 > ctdef->children_cnt_) || OB_ISNULL(ctdef->children_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected error, children of domain id merge ctdef is invalid", K(ret), KPC(ctdef));
      } else if (OB_FAIL(related_tablet_ids.domain_tablet_ids_.prepare_allocate(ctdef->children_cnt_ - 1))) {
        LOG_WARN("failed to prepare allocate array", K(ret), K(ctdef->children_cnt_));
      } else {
        for (int64_t i = 1; OB_SUCC(ret) && i < ctdef->children_cnt_; i++) {
          const ObDASScanCtDef *rowkey_domain_ctdef = static_cast<const ObDASScanCtDef *>(ctdef->children_[i]);
          common::ObTabletID tablet_id;
          tablet_id.reset();
          for (int64_t j = 0; !tablet_id.is_valid() && j < related_ctdefs_.count(); ++j) {
            if (rowkey_domain_ctdef == related_ctdefs_.at(j)) {
              tablet_id = related_tablet_ids_.at(j);
            }
          }
          if (tablet_id.is_valid()) {
            related_tablet_ids.domain_tablet_ids_.at(i - 1) = tablet_id;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to find valid tablet id", K(ret), K(i), KPC(rowkey_domain_ctdef));
          }
        }
      }
    }
  }
  LOG_DEBUG("get rowkey doc tablet id", K(ret), K(related_tablet_ids.domain_tablet_ids_), K(related_ctdefs_), KPC(attach_ctdef_));
  return ret;
}

int ObDASScanOp::get_doc_rowkey_tablet_id(common::ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  const ObDASBaseCtDef *ctdef = nullptr;
  ObDASBaseRtDef *rtdef = nullptr;
  if (nullptr != attach_ctdef_) {
    if (OB_FAIL(ObDASUtils::find_child_das_def(attach_ctdef_,
                                               attach_rtdef_,
                                               DAS_OP_IR_AUX_LOOKUP,
                                               ctdef,
                                               rtdef))) {
      LOG_WARN("failed to find aux lookup ctdef", K(ret));
    } else if (nullptr != ctdef) {
      const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = static_cast<const ObDASIRAuxLookupCtDef*>(ctdef);
      for (int64_t i = 0; OB_SUCC(ret) && !tablet_id.is_valid() && i < related_ctdefs_.count(); ++i) {
        if (aux_lookup_ctdef->get_lookup_scan_ctdef() == related_ctdefs_.at(i)) {
          tablet_id = related_tablet_ids_.at(i);
        }
      }
    }
  }
  return ret;
}

int ObDASScanOp::get_table_lookup_tablet_id(common::ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  if (get_lookup_ctdef() != nullptr) {
    for (int i = 0; !tablet_id.is_valid() && i < related_ctdefs_.count(); ++i) {
      if (get_lookup_ctdef() == related_ctdefs_.at(i)) {
        tablet_id = related_tablet_ids_.at(i);
      }
    }
  } else if (attach_ctdef_ != nullptr && ObDASOpType::DAS_OP_FUNC_LOOKUP == attach_ctdef_->op_type_) {
    // func lookup may involve main table lookup
    const ObDASBaseCtDef *main_ctdef = static_cast<const ObDASFuncLookupCtDef*>(attach_ctdef_)->get_main_lookup_scan_ctdef();
    if (main_ctdef != nullptr) {
      for (int i = 0; !tablet_id.is_valid() && i < related_ctdefs_.count(); ++i) {
        if (main_ctdef == related_ctdefs_.at(i)) {
          tablet_id = related_tablet_ids_.at(i);
        }
      }
    }
  }
  return ret;
}

int ObDASScanOp::get_vec_ir_tablet_ids(ObDASRelatedTabletID &related_tablet_ids)
{
  int ret = OB_SUCCESS;
  bool is_ivf = ObDASIterUtils::is_vec_ivf_scan(attach_ctdef_, attach_rtdef_);
  if (!is_ivf) {
    if (OB_FAIL(get_hnsw_ir_tablet_ids(
            related_tablet_ids.inv_idx_tablet_id_,
            related_tablet_ids.delta_buf_tablet_id_,
            related_tablet_ids.index_id_tablet_id_,
            related_tablet_ids.snapshot_tablet_id_,
            related_tablet_ids.lookup_tablet_id_,
            related_tablet_ids.rowkey_vid_tablet_id_))) {
      LOG_WARN("failed to get hnsw ir tablet ids", K(ret));
    }
  } else {
    if (OB_FAIL(get_ivf_ir_tablet_ids(
            related_tablet_ids.inv_idx_tablet_id_,
            related_tablet_ids.centroid_tablet_id_,
            related_tablet_ids.cid_vec_tablet_id_,
            related_tablet_ids.rowkey_cid_tablet_id_,
            related_tablet_ids.special_aux_tablet_id_,
            related_tablet_ids.lookup_tablet_id_))) {
      LOG_WARN("failed to get ivf ir tablet ids", K(ret));
    }
  }
  return ret;
}

int ObDASScanOp::get_ivf_ir_tablet_ids(
    common::ObTabletID &vec_row_tid,
    common::ObTabletID &centroid_tid_,
    common::ObTabletID &cid_vec_tid,
    common::ObTabletID &rowkey_cid_tid,
    common::ObTabletID &special_aux_tid,
    common::ObTabletID &com_aux_vec_tid)
{
  int ret = OB_SUCCESS;
  centroid_tid_.reset();
  cid_vec_tid.reset();
  rowkey_cid_tid.reset();
  special_aux_tid.reset();
  if (OB_UNLIKELY(related_ctdefs_.count() != related_tablet_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected related scan array not match", K(ret), K_(related_ctdefs), K_(related_tablet_ids));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_.count(); ++i) {
    const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef *>(related_ctdefs_.at(i));
    if (OB_ISNULL(ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ctdef", K(ret), K(i), K(ctdef));
    } else {
      switch (ctdef->ir_scan_type_) {
        case ObTSCIRScanType::OB_NOT_A_SPEC_SCAN: {
          break;
        }
        case ObTSCIRScanType::OB_IR_INV_IDX_SCAN: {
          vec_row_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN: {
          centroid_tid_ = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN: {
          cid_vec_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN: {
          rowkey_cid_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN: {
          special_aux_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_COM_AUX_SCAN: {
          com_aux_vec_tid = related_tablet_ids_.at(i);
          break;
        }
        default: {
          break;
        }
      }
    }
  }
  return ret;
}

int ObDASScanOp::get_hnsw_ir_tablet_ids(
    common::ObTabletID &vec_row_tid,
    common::ObTabletID &delta_buf_tid,
    common::ObTabletID &index_id_tid,
    common::ObTabletID &snapshot_tid,
    common::ObTabletID &com_aux_vec_tid,
    common::ObTabletID &rowkey_vid_tid)
{
  int ret = OB_SUCCESS;
  vec_row_tid.reset();
  delta_buf_tid.reset();
  index_id_tid.reset();
  snapshot_tid.reset();
  com_aux_vec_tid.reset();
  rowkey_vid_tid.reset();
  if (OB_UNLIKELY(related_ctdefs_.count() != related_tablet_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected related scan array not match", K(ret), K_(related_ctdefs), K_(related_tablet_ids));
  }
  for (int64_t i= 0; OB_SUCC(ret) && i < related_ctdefs_.count(); ++i) {
    const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef *>(related_ctdefs_.at(i));
    switch (ctdef->ir_scan_type_) {
      case ObTSCIRScanType::OB_NOT_A_SPEC_SCAN: {
        break;
      }
      case ObTSCIRScanType::OB_IR_INV_IDX_SCAN:
      case ObTSCIRScanType::OB_VEC_FILTER_SCAN: {
        vec_row_tid = related_tablet_ids_.at(i);
        break;
      }
      case ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN: {
        delta_buf_tid = related_tablet_ids_.at(i);
        break;
      }
      case ObTSCIRScanType::OB_VEC_IDX_ID_SCAN: {
        index_id_tid = related_tablet_ids_.at(i);
        break;
      }
      case ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN: {
        snapshot_tid = related_tablet_ids_.at(i);
        break;
      }
      case ObTSCIRScanType::OB_VEC_COM_AUX_SCAN: {
        com_aux_vec_tid = related_tablet_ids_.at(i);
        break;
      }
      case ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN: {
        rowkey_vid_tid = related_tablet_ids_.at(i);
        break;
      }
      default: {
        break;
      }
    }
  }
  return ret;
}

int ObDASScanOp::get_fts_tablet_ids(common::ObIArray<ObDASFTSTabletID> &fts_tablet_ids,
                                    ObDASBaseRtDef *rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr rtdef", K(ret));
  } else if (DAS_OP_IR_SCAN == rtdef->op_type_) {
    ObDASIRScanRtDef *ir_rtdef = static_cast<ObDASIRScanRtDef *>(rtdef);
    ObDASFTSTabletID fts_tablet_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < related_rtdefs_.count(); ++i) {
      if (ir_rtdef->get_inv_idx_agg_rtdef() == related_rtdefs_.at(i) ||
          ir_rtdef->get_inv_idx_scan_rtdef() == related_rtdefs_.at(i)) {
        fts_tablet_id.inv_idx_tablet_id_ = related_tablet_ids_.at(i);
      } else if (ir_rtdef->get_doc_id_idx_agg_rtdef() == related_rtdefs_.at(i)) {
        fts_tablet_id.doc_id_idx_tablet_id_ = related_tablet_ids_.at(i);
      } else if (ir_rtdef->get_fwd_idx_agg_rtdef() == related_rtdefs_.at(i)) {
        fts_tablet_id.fwd_idx_tablet_id_ = related_tablet_ids_.at(i);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(fts_tablet_ids.push_back(fts_tablet_id))) {
        LOG_WARN("failed to push back fts tablet id", K(ret));
      } else {
        ir_rtdef->fts_idx_ = fts_tablet_ids.count() - 1;
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rtdef->children_cnt_; ++i) {
      if (OB_FAIL(SMART_CALL(get_fts_tablet_ids(fts_tablet_ids, rtdef->children_[i])))) {
        LOG_WARN("failed to get fts tablet ids", K(ret));
      }
    }
  }

  return ret;
}

int ObDASScanOp::get_index_merge_tablet_ids(common::ObIArray<common::ObTabletID> &index_merge_tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t N = related_tablet_ids_.count();
  int64_t index_merge_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef*>(related_ctdefs_.at(i));
    if (OB_ISNULL(ctdef)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ctdef), K(ret));
    } else if (ctdef->is_index_merge_) {
      index_merge_cnt += 1;
    }
  }
  if (OB_SUCC(ret) && index_merge_cnt > 0) {
    if (OB_FAIL(index_merge_tablet_ids.prepare_allocate(index_merge_cnt))) {
      LOG_WARN("failed to reserve index merge tablet ids", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef*>(related_ctdefs_.at(i));
      if (OB_ISNULL(ctdef)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ctdef), K(ret));
      } else if (ctdef->is_index_merge_) {
        OB_ASSERT(ctdef->index_merge_idx_ >= 0 && ctdef->index_merge_idx_ < index_merge_cnt);
        index_merge_tablet_ids.at(ctdef->index_merge_idx_) = related_tablet_ids_.at(i);
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASScanOp, ObIDASTaskOp),
                    scan_param_.key_ranges_,
                    scan_ctdef_,
                    scan_rtdef_,
                    scan_param_.ss_key_ranges_,
                    ir_param_);

OB_SERIALIZE_MEMBER(ObDASObsoletedObj, flag_);

ObDASGroupScanOp::ObDASGroupScanOp(ObIAllocator &op_alloc)
  : ObDASScanOp(op_alloc),
    iter_(),
    cur_group_idx_(0),
    group_size_(0)
{

}

void ObDASGroupScanOp::init_group_range(int64_t cur_group_idx, int64_t group_size)
{
  cur_group_idx_ = cur_group_idx;
  group_size_ = group_size;
}

ObDASGroupScanOp::~ObDASGroupScanOp()
{
}

OB_SERIALIZE_MEMBER((ObDASGroupScanOp, ObDASScanOp), iter_, cur_group_idx_, group_size_);

ObDASScanResult::ObDASScanResult()
  : ObIDASTaskResult(),
    ObNewRowIterator(),
    datum_store_("DASScanResult"),
    result_iter_(),
    output_exprs_(nullptr),
    eval_ctx_(nullptr),
    extra_result_(nullptr),
    need_check_output_datum_(false),
    enable_rich_format_(false),
    io_read_bytes_(0),
    ssstore_read_bytes_(0),
    ssstore_read_row_cnt_(0),
    memstore_read_row_cnt_(0)
{
}

ObDASScanResult::~ObDASScanResult()
{
}

int ObDASScanResult::get_next_row(ObNewRow *&row)
{
  UNUSED(row);
  return OB_NOT_IMPLEMENT;
}

int ObDASScanResult::get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(result_iter_.get_next_row<false>(*eval_ctx_, *output_exprs_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from result iterator failed", K(ret));
    } else if (extra_result_ != nullptr) {
      if (OB_FAIL(extra_result_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from extra result failed", K(ret));
        }
      }
    }
  } else {
    LOG_DEBUG("get next row from result iter", K(ret),
              "output", ROWEXPR2STR(*eval_ctx_, *output_exprs_));
  }
  return ret;
}

int ObDASScanResult::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (enable_rich_format_) {
    ret = vec_result_iter_.get_next_batch(*output_exprs_, *eval_ctx_, capacity, count);
  } else if (OB_UNLIKELY(need_check_output_datum_)) {
    ret = result_iter_.get_next_batch<true>(*output_exprs_, *eval_ctx_,
                                                       capacity, count);
  } else {
    ret = result_iter_.get_next_batch<false>(*output_exprs_, *eval_ctx_,
                                                       capacity, count);
  }
  if (OB_FAIL(ret)) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from result iterator failed", K(ret));
    } else if (extra_result_ != nullptr) {
      if (OB_FAIL(extra_result_->get_next_rows(count, capacity))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next batch from extra result failed", K(ret));
        }
      }
    }
  } else {
    const ObBitVector *skip = NULL;
    PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *output_exprs_, count, skip);
  }
  LOG_DEBUG("das result next rows", K(enable_rich_format_), K(count), K(capacity), K(ret));
  return ret;
}

void ObDASScanResult::reset()
{
  result_iter_.reset();
  datum_store_.reset();
  vec_result_iter_.reset();
  vec_row_store_.reset();
  output_exprs_ = nullptr;
  eval_ctx_ = nullptr;
  enable_rich_format_ = false;
}

int ObDASScanResult::init_result_iter(const ExprFixedArray *output_exprs, ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  output_exprs_ = output_exprs;
  eval_ctx_ = eval_ctx;
  LOG_DEBUG("init result iter", K(enable_rich_format_), K(vec_row_store_), K(datum_store_));
  if (enable_rich_format_) {
    if (OB_FAIL(vec_row_store_.begin(vec_result_iter_))) {
      LOG_WARN("begin vec row result iterator failed", K(ret));
    }
  } else {
    if (OB_FAIL(datum_store_.begin(result_iter_))) {
      LOG_WARN("begin datum result iterator failed", K(ret));
    }
  }

  return ret;
}

int ObDASScanResult::init(const ObIDASTaskOp &op, common::ObIAllocator &alloc)
{
  UNUSED(alloc);
  int ret = OB_SUCCESS;
  const ObDASScanOp &scan_op = static_cast<const ObDASScanOp&>(op);
  uint64_t tenant_id = MTL_ID();
  need_check_output_datum_ = scan_op.need_check_output_datum();
  enable_rich_format_ = scan_op.enable_rich_format();
  LOG_DEBUG("das scan result init", K(enable_rich_format_));
  //if (!enable_rich_format_) {
  if (OB_FAIL(datum_store_.init(UINT64_MAX,
                               tenant_id,
                               ObCtxIds::DEFAULT_CTX_ID,
                               "DASScanResult",
                               false/*enable_dump*/))) {
    LOG_WARN("init datum store failed", K(ret));
  } else {
    ObMemAttr mem_attr(tenant_id, "DASScanResult", ObCtxIds::DEFAULT_CTX_ID);
    const ExprFixedArray &result_output = scan_op.get_result_outputs();
    int64_t max_batch_size = static_cast<const ObDASScanCtDef *>(scan_op.get_ctdef())
                             ->pd_expr_spec_.max_batch_size_;
    if (OB_FAIL(vec_row_store_.init(result_output,
                                    max_batch_size,
                                    mem_attr,
                                    UINT64_MAX, /*mem_limit*/
                                    false, /*enable_dump*/
                                    0, /*row_extra_size*/
                                    NONE_COMPRESSOR,
                                    false /*reorder_fixed_expr*/))) {
      LOG_WARN("init vec row store failed", K(ret));
    }
  }

  return ret;
}

int ObDASScanResult::reuse()
{
  int ret = OB_SUCCESS;
  result_iter_.reset();
  datum_store_.reset();
  vec_result_iter_.reset();
  vec_row_store_.reset();
  return ret;
}

int ObDASScanResult::link_extra_result(ObDASExtraData &extra_result, ObIDASTaskOp *task_op)
{
  extra_result.set_output_info(output_exprs_, eval_ctx_);
  extra_result.set_need_check_output_datum(need_check_output_datum_);
  extra_result_ = &extra_result;
  ObTSCMonitorInfo *tsc_monitor_info =
    static_cast<ObDASScanRtDef *>(task_op->get_rtdef())->tsc_monitor_info_;
  extra_result.set_tsc_monitor_info(tsc_monitor_info);
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER((ObDASScanResult, ObIDASTaskResult),
                    datum_store_,
                    enable_rich_format_,
                    vec_row_store_,
                    io_read_bytes_,
                    ssstore_read_bytes_,
                    ssstore_read_row_cnt_,
                    memstore_read_row_cnt_);

ObLocalIndexLookupOp::~ObLocalIndexLookupOp()
{

}

int ObLocalIndexLookupOp::init(const ObDASScanCtDef *lookup_ctdef,
                               ObDASScanRtDef *lookup_rtdef,
                               const ObDASScanCtDef *index_ctdef,
                               ObDASScanRtDef *index_rtdef,
                               ObTxDesc *tx_desc,
                               ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  lookup_ctdef_ = lookup_ctdef;
  lookup_rtdef_ = lookup_rtdef;
  index_ctdef_ = index_ctdef;
  index_rtdef_ = index_rtdef;
  tx_desc_ = tx_desc;
  snapshot_ = snapshot;
  state_ = INDEX_SCAN;
  if (OB_ISNULL(lookup_memctx_)) {
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), ObModIds::OB_SQL_TABLE_LOOKUP, ObCtxIds::DEFAULT_CTX_ID)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(lookup_memctx_, param))) {
      LOG_WARN("create lookup mem context entity failed", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_row(ObNewRow *&row)
{
  UNUSED(row);
  return OB_NOT_IMPLEMENT;
}

void ObLocalIndexLookupOp::print_trans_info_and_key_range_()
{
  int ret = OB_ERR_DEFENSIVE_CHECK;
  if (trans_info_array_.count() == scan_param_.key_ranges_.count()) {
    for (int64_t i = 0; i < trans_info_array_.count(); i++) {
      LOG_ERROR("dump TableLookup DAS Task trans_info and key_ranges", K(i),
                KPC(trans_info_array_.at(i)), K(scan_param_.key_ranges_.at(i)));
    }
  } else {
    for (int64_t i = 0; i < scan_param_.key_ranges_.count(); i++) {
      LOG_ERROR("dump TableLookup DAS Task key_ranges",
                K(i), K(scan_param_.key_ranges_.at(i)));
    }
  }
}

int ObLocalIndexLookupOp::get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIndexLookupOpImpl::get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from index table lookup failed", K(ret));
      if (OB_ERR_DEFENSIVE_CHECK == ret) {
        (void)print_trans_info_and_key_range_();
      }
    } else {
      LOG_DEBUG("get next row from index table lookup", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIndexLookupOpImpl::get_next_rows(count, capacity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next rows from index table lookup failed", K(ret));
      if (OB_ERR_DEFENSIVE_CHECK == ret) {
        (void)print_trans_info_and_key_range_();
      }
    } else {
      LOG_DEBUG("get next rows from index table lookup ", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_row_from_index_table()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rowkey_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from index scan failed", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_rows_from_index_table(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rowkey_iter_->get_next_rows(count, capacity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from index scan failed", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::process_data_table_rowkey()
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = index_ctdef_->result_output_.count();
  if (nullptr != index_ctdef_->group_id_expr_) {
    rowkey_cnt -= 1;
  }
  if (nullptr != index_ctdef_->trans_info_expr_) {
    rowkey_cnt -= 1;
  }
  int64_t group_idx = 0;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;
  common::ObArenaAllocator& lookup_alloc = lookup_memctx_->get_arena_allocator();
  ObNewRange lookup_range;
  if (OB_ISNULL(buf = lookup_alloc.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new (buf) ObObj[rowkey_cnt];
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_ctdef_->result_output_.count(); ++i) {
    ObObj tmp_obj;
    ObExpr *expr = index_ctdef_->result_output_.at(i);
    if (T_PSEUDO_GROUP_ID == expr->type_) {
      group_idx = ObNewRange::get_group_idx(expr->locate_expr_datum(*lookup_rtdef_->eval_ctx_).get_int());
    } else if (T_PSEUDO_ROW_TRANS_INFO_COLUMN == expr->type_) {
      // do nothing
    } else {
      ObDatum &col_datum = expr->locate_expr_datum(*lookup_rtdef_->eval_ctx_);
      if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      } else if (OB_FAIL(ob_write_obj(lookup_alloc, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
      }
    }
  }

  if (OB_SUCC(ret) && nullptr != index_ctdef_->trans_info_expr_) {
    void *buf = nullptr;
    ObDatum *datum_ptr = nullptr;
    if (OB_FAIL(build_trans_datum(index_ctdef_->trans_info_expr_,
                                  lookup_rtdef_->eval_ctx_,
                                  lookup_memctx_->get_arena_allocator(),
                                  datum_ptr))) {

    } else if (OB_ISNULL(datum_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(trans_info_array_.push_back(datum_ptr))) {
      LOG_WARN("fail to push back trans info array", K(ret), KPC(datum_ptr));
    }
  }

  if (OB_SUCC(ret)) {
    ObRowkey table_rowkey(obj_ptr, rowkey_cnt);
    uint64_t ref_table_id = lookup_ctdef_->ref_table_id_;
    if (OB_FAIL(lookup_range.build_range(ref_table_id, table_rowkey))) {
      LOG_WARN("build lookup range failed", K(ret), K(ref_table_id), K(table_rowkey));
    } else if (FALSE_IT(lookup_range.group_idx_ = group_idx)) {
    } else if (OB_FAIL(scan_param_.key_ranges_.push_back(lookup_range))) {
      LOG_WARN("store lookup key range failed", K(ret), K(scan_param_));
    }
    LOG_DEBUG("build data table range", K(ret), K(table_rowkey), K(lookup_range), K(scan_param_.key_ranges_.count()));
  }
  return ret;
}

int ObLocalIndexLookupOp::process_data_table_rowkeys(const int64_t size, const ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  UNUSED(skip);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*index_rtdef_->eval_ctx_);
  batch_info_guard.set_batch_size(size);
  for (auto i = 0; OB_SUCC(ret) && i < size; i++) {
    batch_info_guard.set_batch_idx(i);
    if (OB_FAIL(process_data_table_rowkey())) {
      LOG_WARN("Failed to process_data_table_rowkey", K(ret), K(i));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::do_index_lookup()
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  ObNewRowIterator *&storage_iter = get_lookup_storage_iter();
  if (scan_param_.key_ranges_.empty()) {
    //do nothing
  } else if (storage_iter == nullptr) {
    //first index lookup, init scan param and do table scan
    if (OB_FAIL(init_scan_param())) {
      LOG_WARN("init scan param failed", K(ret));
    } else if (OB_FAIL(tsc_service.table_scan(scan_param_,
                       storage_iter))) {
      if (OB_SNAPSHOT_DISCARDED == ret && scan_param_.fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to scan table", K(scan_param_), K(ret));
      }
    }
  } else {
    const ObTabletID &storage_tablet_id = scan_param_.tablet_id_;
    scan_param_.need_switch_param_ = (storage_tablet_id.is_valid() && storage_tablet_id != tablet_id_ ? true : false);
    scan_param_.tablet_id_ = tablet_id_;
    scan_param_.ls_id_ = ls_id_;
    if (OB_FAIL(reuse_iter())) {
      LOG_WARN("failed to reuse iter", K(ret));
    } else if (OB_FAIL(tsc_service.table_rescan(scan_param_, storage_iter))) {
      LOG_WARN("table_rescan scan iter failed", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_row_from_data_table()
{
  int ret = OB_SUCCESS;
  do_clear_evaluated_flag();
  if (scan_param_.key_ranges_.empty()) {
    ret= OB_ITER_END;
    state_ = FINISHED;
  } else if (OB_FAIL(lookup_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from data table failed", K(ret));
    } else {
      LOG_DEBUG("get next row from data table ", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_rows_from_data_table(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  lookup_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
  if (scan_param_.key_ranges_.empty()) {
    ret = OB_ITER_END;
    state_ = FINISHED;
  } else {
    ret = lookup_iter_->get_next_rows(count, capacity);
    if (OB_ITER_END == ret && count > 0) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::check_lookup_row_cnt()
{
  int ret = OB_SUCCESS;
  if (GCONF.enable_defensive_check()
      && !is_group_scan_
      && lookup_ctdef_->pd_expr_spec_.pushdown_filters_.empty()) {
    if (OB_UNLIKELY(lookup_rowkey_cnt_ != lookup_row_cnt_)) {
      ret = OB_ERR_DEFENSIVE_CHECK;
      ObString func_name = ObString::make_string("check_local_lookup_row_cnt");
      LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
      LOG_ERROR("Fatal Error!!! Catch a defensive error!",
                      K(ret), K_(lookup_rowkey_cnt), K_(lookup_row_cnt),
                      "index_table_id", index_ctdef_->ref_table_id_ ,
                      "data_table_tablet_id", tablet_id_ ,
                      KPC_(snapshot),
                      KPC_(tx_desc));
      concurrency_control::ObDataValidationService::set_delay_resource_recycle(ls_id_);
      (void)print_trans_info_and_key_range_();
    }
  }

  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_DUMP_WRITE_BUFFER);
  if (0 != simulate_error) {
    for (int64_t i = 0; i < trans_info_array_.count(); i++) {
      LOG_INFO("dump TableLookup DAS Task trans info", K(i), KPC(trans_info_array_.at(i)));
    }
  }

  return ret;
}

OB_INLINE ObITabletScan &ObLocalIndexLookupOp::get_tsc_service()
{
  return is_virtual_table(lookup_ctdef_->ref_table_id_) ?
      *GCTX.vt_par_ser_ : *(MTL(ObAccessService *));
}

OB_INLINE int ObLocalIndexLookupOp::init_scan_param()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  scan_param_.tenant_id_ = tenant_id;
  scan_param_.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
  scan_param_.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
  scan_param_.tx_lock_timeout_ = lookup_rtdef_->tx_lock_timeout_;
  scan_param_.index_id_ = lookup_ctdef_->ref_table_id_;
  scan_param_.is_get_ = lookup_ctdef_->is_get_;
  scan_param_.is_for_foreign_check_ = lookup_rtdef_->is_for_foreign_check_;
  scan_param_.timeout_ = lookup_rtdef_->timeout_ts_;
  scan_param_.scan_flag_ = lookup_rtdef_->scan_flag_;
  scan_param_.reserved_cell_count_ = lookup_ctdef_->access_column_ids_.count();
  scan_param_.allocator_ = &lookup_rtdef_->stmt_allocator_;
  scan_param_.sql_mode_ = lookup_rtdef_->sql_mode_;
  scan_param_.scan_allocator_ = &lookup_memctx_->get_arena_allocator();
  scan_param_.frozen_version_ = lookup_rtdef_->frozen_version_;
  scan_param_.force_refresh_lc_ = lookup_rtdef_->force_refresh_lc_;
  scan_param_.output_exprs_ = &(lookup_ctdef_->pd_expr_spec_.access_exprs_);
  scan_param_.aggregate_exprs_ = &(lookup_ctdef_->pd_expr_spec_.pd_storage_aggregate_output_);
  scan_param_.table_param_ = &(lookup_ctdef_->table_param_);
  scan_param_.op_ = lookup_rtdef_->p_pd_expr_op_;
  scan_param_.row2exprs_projector_ = lookup_rtdef_->p_row2exprs_projector_;
  scan_param_.schema_version_ = lookup_ctdef_->schema_version_;
  scan_param_.tenant_schema_version_ = lookup_rtdef_->tenant_schema_version_;
  scan_param_.limit_param_ = lookup_rtdef_->limit_param_;
  scan_param_.need_scn_ = lookup_rtdef_->need_scn_;
  scan_param_.pd_storage_flag_ = lookup_ctdef_->pd_expr_spec_.pd_storage_flag_.pd_flag_;
  scan_param_.table_scan_opt_ = lookup_ctdef_->table_scan_opt_;
  scan_param_.fb_snapshot_ = lookup_rtdef_->fb_snapshot_;
  scan_param_.fb_read_tx_uncommitted_ = lookup_rtdef_->fb_read_tx_uncommitted_;
  scan_param_.ls_id_ = ls_id_;
  scan_param_.tablet_id_ = tablet_id_;
  scan_param_.main_table_scan_stat_.tsc_monitor_info_ = lookup_rtdef_->tsc_monitor_info_;
  if (lookup_rtdef_->is_for_foreign_check_) {
    scan_param_.trans_desc_ = tx_desc_;
  }
  // lookup to main table should invoke multi get
  scan_param_.is_get_ = true;
  scan_param_.is_for_foreign_check_ = lookup_rtdef_->is_for_foreign_check_;
  if (lookup_rtdef_->is_for_foreign_check_) {
    scan_param_.trans_desc_ = tx_desc_;
  }
  if (OB_NOT_NULL(snapshot_)) {
    if (OB_FAIL(scan_param_.snapshot_.assign(*snapshot_))) {
      LOG_WARN("assign snapshot fail", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("snapshot is null", K(ret), KPC(this));
  }
  // set tx_id for read-latest,
  // TODO: add if(query_flag.is_read_latest) ..
  if (OB_NOT_NULL(tx_desc_)) {
    scan_param_.tx_id_ = tx_desc_->get_tx_id();
    scan_param_.tx_seq_base_ = tx_desc_->get_seq_base();
  } else {
    scan_param_.tx_id_.reset();
  }
  if (!lookup_ctdef_->pd_expr_spec_.pushdown_filters_.empty()) {
    scan_param_.op_filters_ = &lookup_ctdef_->pd_expr_spec_.pushdown_filters_;
  }
  scan_param_.pd_storage_filters_ = lookup_rtdef_->p_pd_expr_op_->pd_storage_filters_;
  if (FAILEDx(scan_param_.column_ids_.assign(lookup_ctdef_->access_column_ids_))) {
    LOG_WARN("init column ids failed", K(ret));
  }
  LOG_DEBUG("init local index lookup scan_param", K(scan_param_));
  return ret;
}
int ObLocalIndexLookupOp::reuse_iter()
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  if (OB_FAIL(tsc_service.reuse_scan_iter(scan_param_.need_switch_param_, get_lookup_storage_iter()))) {
    LOG_WARN("reuse scan iterator failed", K(ret));
  }
  return ret;
}
int ObLocalIndexLookupOp::reset_lookup_state()
{
  int ret = OB_SUCCESS;
  state_ = INDEX_SCAN;
  index_end_ = false;
  lookup_row_cnt_ = 0;
  lookup_rowkey_cnt_ = 0;
  trans_info_array_.reuse();
  lookup_rtdef_->stmt_allocator_.set_alloc(index_rtdef_->stmt_allocator_.get_alloc());
  // Keep lookup_rtdef_->stmt_allocator_.alloc_ consistent with index_rtdef_->stmt_allocator_.alloc_
  // to avoid memory expansion
  if (lookup_iter_ != nullptr) {
    scan_param_.key_ranges_.reuse();
    scan_param_.ss_key_ranges_.reuse();
  }
  if (OB_SUCC(ret) && lookup_memctx_ != nullptr) {
    lookup_memctx_->reset_remain_one_page();
  }

  return ret;
}

int ObLocalIndexLookupOp::revert_iter()
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  if (OB_FAIL(tsc_service.revert_scan_iter(get_lookup_storage_iter()))) {
    LOG_WARN("revert scan iterator failed", K(ret));
  }
  //release the memory hold by local index lookup op

  rowkey_iter_ = NULL;
  lookup_iter_ = NULL;
  scan_param_.destroy_schema_guard();
  scan_param_.~ObTableScanParam();
  trans_info_array_.destroy();
  if (lookup_memctx_ != nullptr) {
    lookup_memctx_->reset_remain_one_page();
    DESTROY_CONTEXT(lookup_memctx_);
    lookup_memctx_ = nullptr;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
