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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/pdml/static/ob_px_sstable_insert_op.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "storage/ddl/ob_direct_load_mgr_agent.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::common::serialization;


OB_SERIALIZE_MEMBER((ObPxMultiPartSSTableInsertOpInput, ObPxMultiPartModifyOpInput));

OB_SERIALIZE_MEMBER((ObPxMultiPartSSTableInsertSpec, ObPxMultiPartInsertSpec), flashback_query_expr_,
                     regenerate_heap_table_pk_);

int ObPxMultiPartSSTableInsertSpec::get_snapshot_version(ObEvalCtx &eval_ctx, int64_t &snapshot_version) const
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  snapshot_version = 0;
  if (OB_FAIL(flashback_query_expr_->eval(eval_ctx, datum))) {
    LOG_WARN("expr evaluate failed", K(ret));
  } else if (datum->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL value", K(ret));
  } else {
    snapshot_version = datum->get_int();
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::get_tablet_info_from_row(
    const ObExprPtrIArray &row,
    common::ObTabletID &tablet_id,
    storage::ObTabletSliceParam *tablet_slice_param)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  if (nullptr != tablet_slice_param) {
    tablet_slice_param->reset();
  }

  // 1. get tablet_id
  const int64_t part_id_idx = get_spec().row_desc_.get_part_id_index();
  if (NO_PARTITION_ID_FLAG == part_id_idx) {
    ObDASTableLoc *table_loc = ins_rtdef_.das_rtdef_.table_loc_;
    if (OB_ISNULL(table_loc) || table_loc->get_tablet_locs().size() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert table location is invalid", K(ret), KPC(table_loc));
    } else {
      tablet_id = table_loc->get_first_tablet_loc()->tablet_id_;
    }
  } else if (part_id_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, part_id_idx is not valid", K(ret), K(part_id_idx));
  } else if (row.count() > part_id_idx) {
    ObExpr *expr = row.at(part_id_idx);
    ObDatum &expr_datum = expr->locate_expr_datum(get_eval_ctx());
    tablet_id = expr_datum.get_int();
  }

  // 2. get slice param
  if (OB_SUCC(ret) && nullptr != tablet_slice_param) {
    bool found_slice_expr = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found_slice_expr && i < row.count(); ++i) {
      if (row.at(i)->type_ == ObItemType::T_PSEUDO_DDL_SLICE_ID) {
        ObDatum &expr_datum = row.at(i)->locate_expr_datum(get_eval_ctx());
        tablet_slice_param->slice_id_ = expr_datum.get_int();
        found_slice_expr = true;
      }
    }
    if (OB_SUCC(ret) && !found_slice_expr) {
      tablet_slice_param->slice_idx_ = ctx_.get_px_task_id();
    }
    if (OB_SUCC(ret) && tablet_slice_param->slice_idx_ >= ObTabletSliceParam::MAX_TABLET_SLICE_COUNT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ddl_slice_id", K(ret), KPC(tablet_slice_param));
    }
  }

  return ret;
}

const ObPxMultiPartSSTableInsertSpec &ObPxMultiPartSSTableInsertOp::get_spec() const
{
  return static_cast<const ObPxMultiPartSSTableInsertSpec &>(spec_);
}

int ObPxMultiPartSSTableInsertOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  if (OB_FAIL(ObPxMultiPartInsertOp::inner_open())) {
    LOG_WARN("inner open failed", K(ret));
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(tablet_store_map_.create(MAP_HASH_BUCKET_NUM, "SSTABLE_INS"))) {
    LOG_WARN("fail to create row cnt map", K(ret));
  } else {
    const int64_t ddl_table_id = MY_SPEC.plan_->get_ddl_table_id();
    if (OB_FAIL(ctx_.get_sqc_handler()->get_sub_coord().get_participants(
          ctx_.get_sqc_handler()->get_sqc_init_arg().sqc_,
          ddl_table_id,
          participants_))) {
      LOG_WARN("get participants failed", K(ret));
    } else if (OB_FAIL(MY_SPEC.get_snapshot_version(eval_ctx_, snapshot_version_))) {
      LOG_WARN("get snapshot version failed", K(ret));
    } else {
      // sort in ASC order by tablet id.
      lib::ob_sort(participants_.begin(), participants_.end(), ObLSTabletIDPairCmp());
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SSTABLE_INSERT_CG_ROW_COUNT;
      op_monitor_info_.otherstat_1_value_ = 0;
      op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::SSTABLE_INSERT_ROW_COUNT;
      op_monitor_info_.otherstat_2_value_ = 0;
      op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::DDL_TASK_ID;
      op_monitor_info_.otherstat_5_value_ = MY_SPEC.plan_->get_ddl_task_id();
      LOG_INFO("update table context", K(snapshot_version_),
               K(MY_SPEC.ins_ctdef_.das_ctdef_.table_id_), K(MY_SPEC.ins_ctdef_.das_ctdef_.index_tid_));
    }
  }
  return ret;
}

void ObPxMultiPartSSTableInsertOp::destroy()
{
  participants_.reset();
  curr_tablet_store_iter_.reset();
  tablet_seq_caches_.reset();
  if (tablet_store_map_.created()) {
    TabletStoreMap::iterator iter = tablet_store_map_.begin();
    for (; iter != tablet_store_map_.end(); ++iter) {
      iter->second->~ObChunkDatumStore();
    }
  }
  tablet_store_map_.destroy();
  tablet_pre_slice_count_map_.destroy();
  allocator_.reset();
}

int ObPxMultiPartSSTableInsertOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = common::OB_INVALID_ID;
  ObSQLSessionInfo *my_session = nullptr;
  const ObTableSchema *table_schema = nullptr; // TODO(shuangcan): remove this
  ObSqlCtx *sql_ctx = NULL;
  int64_t notify_idx = 0;
  int64_t unused_row_scan_cnt = 0;
  int64_t ddl_task_id = MY_SPEC.plan_->get_ddl_task_id();
  ObInsertMonitor insert_monitor(unused_row_scan_cnt, op_monitor_info_.otherstat_2_value_, op_monitor_info_.otherstat_1_value_);
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_DDL_EXECUTE_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("errsim ddl execute get next row failed", KR(ret));
      }
    }
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the child op is null", K(ret), K(MTL_ID()), KP(child_));
  } else if (get_spec().is_returning_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable insert op should not return rows", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, session must not be nullptr", K(ret));
  } else if (OB_ISNULL(sql_ctx = ctx_.get_sql_ctx()) || OB_ISNULL(sql_ctx->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, schema guard not be nullptr", K(ret));
  } else if (OB_FALSE_IT(tenant_id = GET_MY_SESSION(ctx_)->get_effective_tenant_id())) {
  } else if (OB_FAIL(sql_ctx->schema_guard_->get_table_schema(tenant_id,
             MY_SPEC.plan_->get_ddl_table_id(), table_schema))) {
    LOG_WARN("fail to get table schema", K(tenant_id), K(MY_SPEC.plan_->get_ddl_table_id()), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("Table not exist", K(MY_SPEC.plan_->get_ddl_table_id()), K(ret));
  } else if (OB_FALSE_IT(is_vec_gen_vid_ = table_schema->is_vec_rowkey_vid_type())) {
  } else if (OB_FALSE_IT(is_vec_data_complement_ = (table_schema->is_vec_index_snapshot_data_type() ||
                                                    table_schema->is_vec_ivfflat_index() ||
                                                    table_schema->is_vec_ivfsq8_index() ||
                                                    table_schema->is_vec_ivfpq_index()))) {
  } else if (need_count_rows() && OB_FAIL(get_all_rows_and_count())) {
    LOG_WARN("fail to get all rows and count", K(ret));
  } else {
    const ObPhysicalPlan *phy_plan = NULL;
    ObMacroDataSeq block_start_seq;
    int64_t schema_version = 0;
    bool all_slices_empty = false; // all slices empty.
    const uint64_t index_tid = MY_SPEC.plan_->get_ddl_table_id();
    const ObDDLCtrl &ddl_ctrl = ctx_.get_sqc_handler()->get_ddl_control();
    bool need_idempotent_autoinc_val = false;
    if (OB_ISNULL(ctx_.get_physical_plan_ctx()) || OB_ISNULL(phy_plan = ctx_.get_physical_plan_ctx()->get_phy_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get phy_plan failed", K(ret), KP(ctx_.get_physical_plan_ctx()), KP(phy_plan));
    } else if (table_schema->is_vec_delta_buffer_type() || table_schema->is_vec_index_id_type()) {
      all_slices_empty = true;
      is_all_partition_finished_ = true;
    } else if (OB_FAIL(get_next_row_with_cache())) {// get one row first for calc part_id
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail get next row from child", K(ret));
      } else {
        all_slices_empty = true;
        ret = OB_SUCCESS;
      }
    } else if (FALSE_IT(need_idempotent_autoinc_val =
                            (is_shared_storage_dempotent_mode(ddl_ctrl.direct_load_type_) &&
                             !is_incremental_direct_load(ddl_ctrl.direct_load_type_)) &&
                            !MY_SPEC.regenerate_heap_table_pk_)) {

    } else if (need_idempotent_autoinc_val && OB_FAIL(build_table_slice_info())) {
      LOG_WARN("fail to build table slice info", K(ret));
    }

    /* get tablet slice info */
    bool is_partition_table  = true;
    common::hash::ObHashMap<int64_t, int64_t> tablet_slice_cnt_map;
    if (OB_FAIL(ret)) {
    } else if (!is_shared_storage_dempotent_mode(ddl_ctrl.direct_load_type_)) {
    } else if (OB_FAIL(tablet_slice_cnt_map.create(participants_.count(), "SliceInfoM",
                                                        ObModIds::OB_HASH_NODE, tenant_id))) {
      LOG_WARN("fail to create hash table", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_task_tablet_slice_count(MTL_ID(), ddl_task_id, is_partition_table, tablet_slice_cnt_map))) {
      LOG_WARN("failed to get tablet slice count", K(ret), K(ddl_task_id), K(MTL_ID()));
    }

    for (notify_idx = 0; OB_SUCC(ret) && notify_idx < participants_.count();) {
      clear_evaluated_flag();
      bool is_current_slice_empty = false;
      const share::ObLSID &notify_ls_id = participants_.at(notify_idx).first;
      const ObTabletID &notify_tablet_id = participants_.at(notify_idx).second;
      ObDirectLoadSliceInfo slice_info;
      slice_info.is_full_direct_load_ = true;
      slice_info.is_lob_slice_ = false;
      slice_info.ls_id_ = notify_ls_id;
      slice_info.data_tablet_id_ = notify_tablet_id;
      slice_info.context_id_ = ddl_ctrl.context_id_;
      int64_t affected_rows = 0;
      ObTabletID row_tablet_id;
      ObTabletSliceParam tablet_slice_param;
      bool is_tablet_finish = false;
      const ObTabletCacheInterval *curr_tablet_seq_cache =
        count_rows_finish_ && curr_tablet_idx_ < tablet_seq_caches_.count() && curr_tablet_idx_ >= 0 ?
          &tablet_seq_caches_.at(curr_tablet_idx_) : nullptr;
      ObDirectLoadMgrAgent ddl_agent;

      if (!is_shared_storage_dempotent_mode(ddl_ctrl.direct_load_type_)) {
      } else if (MY_SPEC.regenerate_heap_table_pk_) {
        slice_info.total_slice_cnt_ = ctx_.get_sqc_handler()->get_sqc_ctx().get_task_count();
      } else if (OB_FAIL(tablet_slice_cnt_map.get_refactored(is_partition_table ? slice_info.data_tablet_id_.id() : 0, slice_info.total_slice_cnt_))) {
        LOG_WARN("failed to get tablet slice cnt", K(ret), K(is_partition_table), K(slice_info.data_tablet_id_));
      } else if (slice_info.total_slice_cnt_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("slice cnt should not less than 0", K(ret), K(is_partition_table), K(slice_info.data_tablet_id_));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ddl_agent.init(slice_info.context_id_, slice_info.ls_id_, slice_info.data_tablet_id_, ddl_ctrl.direct_load_type_))) {
        LOG_WARN("init agent failed", K(ret), K(slice_info));
      } else if (all_slices_empty || is_all_partition_finished_) {
        is_current_slice_empty = true;
      } else {
        const ObExprPtrIArray *row = &child_->get_spec().output_;
        if (OB_FAIL(get_tablet_info_from_row(*row, row_tablet_id, &tablet_slice_param))) {
          LOG_WARN("get tablet info failed", K(ret));
        } else if (notify_tablet_id != row_tablet_id) {
          is_current_slice_empty = true;
        }
      }
      int64_t parallel_idx = curr_tablet_seq_cache ? curr_tablet_seq_cache->task_id_ : MAX(tablet_slice_param.slice_idx_, 0);
      FLOG_INFO("update ddl parallel id", K(ret), K(notify_tablet_id), K(slice_info), K(parallel_idx), K(tablet_slice_param), K(ctx_.get_px_task_id()), K(is_current_slice_empty),
          K(row_tablet_id), K(is_all_partition_finished_), K(count_rows_finish_), K(curr_tablet_idx_), K(tablet_seq_caches_.count()), KPC(curr_tablet_seq_cache));

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDDLUtil::init_macro_block_seq(parallel_idx, block_start_seq))) {
        LOG_WARN("set parallel index failed", K(ret), K(parallel_idx));
      } else if (OB_FAIL(ddl_agent.open_sstable_slice(block_start_seq, slice_info))) {
        LOG_WARN("create sstable slice writer failed", K(ret), K(block_start_seq), K(slice_info));
      } else {
        int64_t tablet_id = notify_tablet_id.id();
        int64_t table_level_slice_idx = 0;
        if (!need_idempotent_autoinc_val) {

        } else if (is_partitioned_table_) {
          int64_t pre_slice_count = 0;
          if (OB_FAIL(tablet_pre_slice_count_map_.get_refactored(tablet_id, pre_slice_count))) {
            LOG_WARN("get pre slice count failed", K(ret), K(tablet_id));
          } else {
            table_level_slice_idx = pre_slice_count + tablet_slice_param.slice_idx_;
          }
        } else {
          table_level_slice_idx = tablet_slice_param.slice_idx_;
        }
        if (OB_SUCC(ret)) {
          ObDDLSliceRowIterator slice_row_iter(
              this, notify_tablet_id, is_current_slice_empty, table_schema->get_rowkey_column_num(),
              snapshot_version_, tablet_slice_param, need_idempotent_autoinc_val, table_all_slice_count_,
              table_level_slice_idx, autoinc_range_interval_);
          ObDDLInsertRowIterator row_iter;
          if (OB_FAIL(row_iter.init(tenant_id, ddl_agent, &slice_row_iter, notify_ls_id, notify_tablet_id,
                                    slice_info.context_id_, tablet_slice_param,
                                    table_schema->get_lob_columns_count(), slice_info.total_slice_cnt_, is_vec_data_complement_))) {
            LOG_WARN("init ddl insert rot iterator failed", K(ret));
          } else if (OB_FAIL(ddl_agent.fill_sstable_slice(slice_info, &row_iter, affected_rows,
                                                          &insert_monitor))) {
            LOG_WARN("fill data into sstable slice failed", K(ret), K(slice_info));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObTabletID next_row_tablet_id;
        if (is_all_partition_finished_) {
          is_tablet_finish = true;
        } else if (OB_FAIL(get_tablet_info_from_row(child_->get_spec().output_, next_row_tablet_id))) {
          LOG_WARN("get next row tablet id failed", K(ret));
        } else {
          is_tablet_finish = next_row_tablet_id != notify_tablet_id;
        }
        if (OB_SUCC(ret) && is_tablet_finish) {
          notify_idx++;
        }
      }
      if (OB_SUCC(ret)) {
        blocksstable::ObMacroDataSeq unused_seq;
        slice_info.is_task_finish_ = is_tablet_finish;
        if (OB_FAIL(ddl_agent.close_sstable_slice(slice_info, &insert_monitor, unused_seq))) {
          LOG_WARN("close sstable slice failed", K(ret), K(slice_info));
        }
        ctx_.get_physical_plan_ctx()->add_affected_rows(affected_rows);
      }
    }
    if (OB_SUCC(ret) && need_idempotent_autoinc_val) {
      if (OB_FAIL(update_sqc_global_autoinc_value())) {
        LOG_WARN("sync global autoinc value failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (notify_idx < participants_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), K(notify_idx), K(participants_));
    } else {
      ret = OB_ITER_END;
      LOG_INFO("all partitions is end", K(notify_idx), K(participants_));
    }
  }
  if (OB_FAIL(ret) && OB_EAGAIN == ret) {
    LOG_WARN("replace eagain into error unexpected, px not allow eagain", K(ret));
    ret = OB_TIMEOUT;
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::get_next_row_with_cache()
{
  int ret = OB_SUCCESS;
  if (count_rows_finish_) {
    clear_evaluated_flag();
    if (!curr_tablet_store_iter_.is_valid() || !curr_tablet_store_iter_.has_next()) {
      if (curr_tablet_idx_ >= tablet_seq_caches_.count() - 1) {
        ret = OB_ITER_END;
      } else {
        ObTabletID tablet_id = tablet_seq_caches_.at(++curr_tablet_idx_).tablet_id_;
        ObChunkDatumStore *tablet_store = nullptr;
        if (OB_FAIL(tablet_store_map_.get_refactored(tablet_id, tablet_store))) {
          // expect curr_tablet_idx to be moved when remove tablet store from map
          // so if we cannot find the corresponding tablet store using curr tablet id
          // there is something wrong.
          LOG_WARN("fail to get tablet store", K(ret), K(tablet_id));
        } else if (OB_ISNULL(tablet_store)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet store is expected null", K(ret));
        } else if (OB_FAIL(tablet_store->begin(curr_tablet_store_iter_))) {
          LOG_WARN("fail to start tablet store iterator", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(curr_tablet_store_iter_.get_next_row(child_->get_spec().output_, eval_ctx_))) {
      LOG_WARN("fail to get next row from chunk store", K(ret));
    } else {
      const ObIArray<ObExpr*> &child_expr = child_->get_spec().output_;
      ObExpr *auto_inc_expr = nullptr;
      uint64_t next_autoinc_val = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < child_expr.count(); ++i) {
        if (child_expr.at(i)->type_ == T_TABLET_AUTOINC_NEXTVAL) {
          auto_inc_expr = child_expr.at(i);
          break;
        }
      }
      if (OB_SUCC(ret) && is_vec_gen_vid_ && child_expr.count() > 0) {
        auto_inc_expr = child_expr.at(child_expr.count() - 1);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(auto_inc_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cannot find tablet autoinc expr", K(child_->get_spec().output_));
      } else if (curr_tablet_idx_ < 0 || curr_tablet_idx_ >= tablet_seq_caches_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected curr tablet idx", K(ret), K(curr_tablet_idx_));
      } else if (OB_FAIL(tablet_seq_caches_.at(curr_tablet_idx_).next_value(next_autoinc_val))) {
        LOG_WARN("fail to get next autoinc val", K(ret), K(tablet_seq_caches_.at(curr_tablet_idx_)));
        // because this op is single-thread, next value should never fail, otherwise this ddl will fail
        ret = OB_ERR_UNEXPECTED;
      } else if (tablet_seq_caches_.at(curr_tablet_idx_).task_id_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet seq interval doesn't have start seq", K(ret), K(tablet_seq_caches_));
      }

      if (OB_SUCC(ret)) {
        ObDatum &datum = auto_inc_expr->locate_datum_for_write(eval_ctx_);
        ObTabletID tablet_id = tablet_seq_caches_.at(curr_tablet_idx_).tablet_id_;
        if (is_vec_gen_vid_) {
          // TODO @lhd make vid into struct
          datum.set_uint(next_autoinc_val);
        } else {
          datum.set_uint(next_autoinc_val);
        }
        auto_inc_expr->set_evaluated_projected(eval_ctx_);
      }
    }
  } else {
    ret = child_->get_next_row();
  }
  if (OB_SUCC(ret)) {
    op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SSTABLE_INSERT_ROW_COUNT;
  }
  if (OB_ITER_END == ret) {
    is_all_partition_finished_ = true;
    LOG_INFO("scan all partition finished");
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::get_all_rows_and_count()
{
  int ret = OB_SUCCESS;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  if (OB_UNLIKELY(!tablet_store_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache row store or row_cnt_map is not inited", K(ret));
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else {
    int64_t curr_tablet_row_cnt = 0;
    const ObDDLCtrl &ddl_ctrl = ctx_.get_sqc_handler()->get_ddl_control();
    while (OB_SUCC(ret)) {
      const ObExprPtrIArray *row = &child_->get_spec().output_;
      ObTabletID row_tablet_id;
      ObChunkDatumStore *tablet_store = nullptr;
      clear_evaluated_flag();
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail get next row from child", K(ret));
        }
      } else if (OB_FAIL(get_tablet_info_from_row(*row, row_tablet_id))) {
        LOG_WARN("failed to get tablet id", K(ret));
      } else if (OB_FAIL(tablet_store_map_.get_refactored(row_tablet_id, tablet_store))) {
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(create_tablet_store(row_tablet_id, tablet_store))) {
            LOG_WARN("failed to create tablet store", K(ret), K(row_tablet_id));
          }
        } else {
          LOG_WARN("failed to get tablet store from map", K(ret), K(row_tablet_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(tablet_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unepxected tablet store", K(ret));
      } else if (OB_FAIL(tablet_store->add_row(*row, &eval_ctx_))) {
        LOG_WARN("failed to add row to tablet store", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      TabletStoreMap::const_iterator iter;
      for (iter = tablet_store_map_.begin(); OB_SUCC(ret) && iter != tablet_store_map_.end(); ++iter) {
        ObTabletCacheInterval interval(iter->first, iter->second->get_row_cnt());
        if (OB_FAIL(tenant_direct_load_mgr->get_tablet_cache_interval(ddl_ctrl.context_id_, iter->first, interval))) {
          LOG_WARN("failed to get tablet cache intervals", K(ret), "tablet_id", iter->first);
        } else if (OB_FAIL(tablet_seq_caches_.push_back(interval))) {
          LOG_WARN("failed to add tablet cache interval", K(ret), K(interval));
        }
      }
      if (OB_SUCC(ret)) {
        lib::ob_sort(tablet_seq_caches_.begin(), tablet_seq_caches_.end());
        count_rows_finish_ = true;
      }
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::create_tablet_store(ObTabletID &tablet_id, ObChunkDatumStore *&tablet_store)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_store_map_.created() || nullptr != tablet_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected input", K(ret));
  } else {
    void *buf = allocator_.alloc(sizeof(ObChunkDatumStore));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for tablet store", K(ret));
    } else if (OB_ISNULL(tablet_store = new (buf) ObChunkDatumStore("SSTABLE_INS"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new ObChunkDatumStore", K(ret));
    } else if (OB_FAIL(tablet_store->init(TABLET_STORE_MEM_LIMIT,
                                          ctx_.get_my_session()->get_effective_tenant_id(),
                                          ObCtxIds::DEFAULT_CTX_ID,
                                          "SSTABLE_INS",
                                          true/*enable_dump*/))) {
      LOG_WARN("failed to init chunk datum store", K(ret));
    } else if (OB_FAIL(tablet_store->alloc_dir_id())) {
      LOG_WARN("failed to alloc dir id", K(ret));
    } else if (OB_FAIL(tablet_store_map_.set_refactored(tablet_id, tablet_store))) {
      LOG_WARN("failed to add tablet store to map", K(ret), K(tablet_id));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != tablet_store) {
        tablet_store->~ObChunkDatumStore();
      }
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::build_table_slice_info()
{
  int ret = OB_SUCCESS;
  int64_t ddl_task_id = MY_SPEC.plan_->get_ddl_task_id();
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  uint64_t tenant_id = MTL_ID();
  ObString schedule_info;
  ObSqlString sql_string;
  rootserver::ObDDLSliceInfo ddl_slice_info;
  ObArenaAllocator arena(ObMemAttr(tenant_id, "ddl_slice_info"));
  bool use_idempotent_mode = false;

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(tablet_pre_slice_count_map_.create(MAP_HASH_BUCKET_NUM, "SliceInfoM",
                                                        ObModIds::OB_HASH_NODE, tenant_id))) {
    LOG_WARN("fail to create hash table", K(ret));
  } else if (OB_FAIL(rootserver::ObDDLTaskRecordOperator::get_schedule_info_for_update(
                 *sql_proxy, tenant_id, ddl_task_id, arena, ddl_slice_info, use_idempotent_mode))) {
    LOG_WARN("fail to get schedule info", K(ret), K(tenant_id), K(ddl_task_id));
  } else {
    int64_t table_slice_count = 0;
    const common::Ob2DArray<sql::ObPxTabletRange> &part_ranges = ddl_slice_info.part_ranges_;
    if (0 == part_ranges.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no partition range", K(ret), K(ddl_slice_info));
    } else if (1 == part_ranges.count() && 0 == part_ranges.at(0).tablet_id_) {
      // for unpartitioned table, there is only one tablet and its tablet id is 0
      is_partitioned_table_ = false;
      table_slice_count = part_ranges.at(0).range_cut_.count() + 1;
    } else {
      is_partitioned_table_ = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_ranges.count(); ++i) {
        const ObPxTabletRange &cur_part_range = part_ranges.at(i);
        const int64_t tablet_id = cur_part_range.tablet_id_;
        if (OB_FAIL(tablet_pre_slice_count_map_.set_refactored(tablet_id, table_slice_count))) {
          LOG_WARN("fail to set map", K(ret), K(tablet_id), K(table_slice_count));
        } else {
          table_slice_count += cur_part_range.range_cut_.count() + 1;
        }
      }
    }
    if (OB_SUCC(ret)) {
      table_all_slice_count_ = table_slice_count;
      autoinc_range_interval_ = ddl_slice_info.autoinc_range_interval_;
    }
  }

  return ret;
}

int ObPxMultiPartSSTableInsertOp::update_sqc_global_autoinc_value()
{
  int ret = OB_SUCCESS;
  ObAutoincrementService &auto_service = ObAutoincrementService::get_instance();
  ObEvalCtx &eval_ctx = get_eval_ctx();
  ObPhysicalPlanCtx *plan_ctx = eval_ctx.exec_ctx_.get_physical_plan_ctx();
  ObPhysicalPlanCtx *sqc_plan_ctx =
      eval_ctx.exec_ctx_.get_sqc_handler()->get_exec_ctx().get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(sqc_plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context is null", K(ret), K(plan_ctx), K(sqc_plan_ctx));
  } else {
    const ObIArray<AutoincParam> &autoinc_params = plan_ctx->get_autoinc_params();
    ObIArray<AutoincParam> &sqc_autoinc_params = sqc_plan_ctx->get_autoinc_params();
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      const AutoincParam &autoinc_param = autoinc_params.at(i);
      AutoincParam *sqc_autoinc_param = NULL;
      for (int64_t j = 0; OB_SUCC(ret) && j < sqc_autoinc_params.count(); ++j) {
        AutoincParam &iter = sqc_autoinc_params.at(j);
        if (autoinc_param.autoinc_table_id_ == iter.autoinc_table_id_ &&
            autoinc_param.autoinc_col_id_ == iter.autoinc_col_id_) {
          sqc_autoinc_param = &iter;
          break;
        }
      }
      if (OB_ISNULL(sqc_autoinc_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can't find sqc autoinc param", K(ret), K(autoinc_params), K(sqc_autoinc_params));
      } else {
        while (OB_SUCC(ret)) {
          uint64_t sqc_global_autoinc_val = ATOMIC_LOAD(&sqc_autoinc_param->global_value_to_sync_);
          if (sqc_global_autoinc_val >= autoinc_param.global_value_to_sync_) {
            break;
          } else {
            if (ATOMIC_BCAS(&sqc_autoinc_param->global_value_to_sync_, sqc_global_autoinc_val,
                            autoinc_param.global_value_to_sync_)) {
              break;
            }
          }
        }
      }
    }
  }

  return ret;
}
