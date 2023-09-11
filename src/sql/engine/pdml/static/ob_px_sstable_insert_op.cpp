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
#include "common/ob_tablet_id.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx.h"

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

int ObPxMultiPartSSTableInsertOp::get_tablet_id_from_row(const ObExprPtrIArray &row,
                                                         const int64_t part_id_idx,
                                                         ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
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
  return ret;
}

const ObPxMultiPartSSTableInsertSpec &ObPxMultiPartSSTableInsertOp::get_spec() const
{
  return static_cast<const ObPxMultiPartSSTableInsertSpec &>(spec_);
}

int ObPxMultiPartSSTableInsertOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxMultiPartInsertOp::inner_open())) {
    LOG_WARN("inner open failed", K(ret));
  } else {
    int64_t snapshot_version = 0;
    const int64_t context_id = ctx_.get_sqc_handler()->get_ddl_context_id();
    if (OB_FAIL(MY_SPEC.get_snapshot_version(eval_ctx_, snapshot_version))) {
      LOG_WARN("get snapshot version failed", K(ret));
    } else if (OB_FAIL(ObSSTableInsertManager::get_instance().update_table_context(
        context_id, snapshot_version))) {
      LOG_WARN("update table context failed", K(ret));
    } else if (OB_FAIL(tablet_store_map_.create(MAP_HASH_BUCKET_NUM, "SSTABLE_INS"))) {
      LOG_WARN("fail to create row cnt map", K(ret));
    } else {
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SSTABLE_INSERT_ROW_COUNT;
      op_monitor_info_.otherstat_1_value_ = 0;
      op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::DDL_TASK_ID;
      op_monitor_info_.otherstat_5_value_ = MY_SPEC.plan_->get_ddl_task_id();
      LOG_INFO("update table context", K(context_id), K(snapshot_version),
               K(MY_SPEC.ins_ctdef_.das_ctdef_.table_id_), K(MY_SPEC.ins_ctdef_.das_ctdef_.index_tid_));
    }
  }
  return ret;
}

void ObPxMultiPartSSTableInsertOp::destroy()
{
  curr_tablet_store_iter_.reset();
  tablet_seq_caches_.reset();
  if (tablet_store_map_.created()) {
    TabletStoreMap::iterator iter = tablet_store_map_.begin();
    for (; iter != tablet_store_map_.end(); ++iter) {
      iter->second->~ObChunkDatumStore();
    }
  }
  tablet_store_map_.destroy();
  allocator_.reset();
}

static int notify_tablet_end(const int64_t context_id, const ObTabletID &tablet_id, const int64_t tablets_count, int64_t &notify_idx, bool emergent_finish = false)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertManager &sstable_context_mgr = ObSSTableInsertManager::get_instance();
  if (OB_UNLIKELY(context_id < 0 || !tablet_id.is_valid() || tablets_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context_id), K(tablet_id), K(tablets_count));
  } else if (OB_FAIL(sstable_context_mgr.notify_tablet_end(context_id, tablet_id))) {
    LOG_WARN("notify partition end failed", K(ret), K(context_id), K(tablet_id));
  }
  ++notify_idx; // ignore ret
  if (0 == notify_idx % 1000 || tablets_count == notify_idx || emergent_finish) { // batch 1000 or reach the end
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = sstable_context_mgr.finish_ready_tablets(context_id, notify_idx))) {
      LOG_WARN("finsh ready partitions failed", K(tmp_ret), K(context_id), K(notify_idx));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = nullptr;
  const ObTableSchema *table_schema = nullptr; // TODO(shuangcan): remove this
  ObSqlCtx *sql_ctx = NULL;
  ObArray<ObTabletID> tablet_ids;
  int64_t notify_idx = 0;
  int64_t context_id = -1;
  ObSSTableInsertManager &sstable_context_mgr = ObSSTableInsertManager::get_instance();
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the child op is null", K(ret));
  } else if (get_spec().is_returning_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable insert op should not return rows", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, session must not be nullptr", K(ret));
  } else if (OB_ISNULL(ctx_.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc handler is null", K(ret));
  } else if (FALSE_IT(context_id = ctx_.get_sqc_handler()->get_ddl_context_id())) {
  } else if (OB_FAIL(sstable_context_mgr.get_tablet_ids(context_id, tablet_ids))) {
    LOG_WARN("get tablet ids failed", K(ret), K(context_id));
  } else if (OB_ISNULL(sql_ctx = ctx_.get_sql_ctx()) || OB_ISNULL(sql_ctx->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, schema guard not be nullptr", K(ret));
  } else if (OB_FAIL(sql_ctx->schema_guard_->get_table_schema(
             GET_MY_SESSION(ctx_)->get_effective_tenant_id(),
             MY_SPEC.plan_->get_ddl_table_id(), table_schema))) {
    LOG_WARN("fail to get table schema", K(MY_SPEC.plan_->get_ddl_table_id()), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("Table not exist", K(MY_SPEC.plan_->get_ddl_table_id()), K(ret));
  } else {
    std::sort(tablet_ids.begin(), tablet_ids.end()); // sort in ASC order
  }

  if (OB_FAIL(ret)) {
  } else if (need_count_rows()) {
    if (OB_FAIL(get_all_rows_and_count())) {
      LOG_WARN("fail to get all rows and count", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_next_row_with_cache())) {// get one row first for calc part_id
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row from child", K(ret));
    }
  } else {
    const ObPhysicalPlan *phy_plan = NULL;
    ObSSTableInsertTabletParam write_sstable_param;
    ObMacroDataSeq block_start_seq;
    int64_t schema_version = 0;
    const uint64_t index_tid = MY_SPEC.plan_->get_ddl_table_id();
    if (OB_ISNULL(ctx_.get_physical_plan_ctx()) || OB_ISNULL(phy_plan = ctx_.get_physical_plan_ctx()->get_phy_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get phy_plan failed", K(ret), KP(ctx_.get_physical_plan_ctx()), KP(phy_plan));
    } else {
      write_sstable_param.context_id_ = context_id;
      write_sstable_param.table_id_ = index_tid;
      write_sstable_param.write_major_ = true;
      write_sstable_param.task_cnt_ = ctx_.get_sqc_handler()->get_sqc_ctx().get_task_count();
      write_sstable_param.schema_version_ = MY_SPEC.plan_->get_ddl_schema_version();
      write_sstable_param.execution_id_ = MY_SPEC.plan_->get_ddl_execution_id();
    }
    while (OB_SUCC(ret) && notify_idx < tablet_ids.count()) {
      ObTabletID &notify_tablet_id = tablet_ids.at(notify_idx);
      clear_evaluated_flag();
      const ObExprPtrIArray *row = &child_->get_spec().output_;
      ObTabletID row_tablet_id;
      if (OB_FAIL(get_tablet_id_from_row(*row, get_spec().row_desc_.get_part_id_index(), row_tablet_id))) {
        LOG_WARN("get part id failed", K(ret));
      } else if (row_tablet_id != notify_tablet_id) {
        notify_tablet_end(context_id, notify_tablet_id, tablet_ids.count(), notify_idx);
      } else {
        write_sstable_param.tablet_id_ = row_tablet_id;
        int64_t affected_rows = 0;
        ObSSTableInsertRowIterator row_iter(ctx_, this);
        const ObTabletCacheInterval *curr_tablet_seq_cache =
          count_rows_finish_ && curr_tablet_idx_ < tablet_seq_caches_.count() ? &tablet_seq_caches_.at(curr_tablet_idx_) : nullptr;
        int64_t parallel_idx = curr_tablet_seq_cache ? curr_tablet_seq_cache->task_id_ : ctx_.get_px_task_id();
        FLOG_INFO("update ddl parallel id", K(ret), K(parallel_idx), K(ctx_.get_px_task_id()),
            K(count_rows_finish_), K(curr_tablet_idx_), K(tablet_seq_caches_.count()), KPC(curr_tablet_seq_cache));
        if (OB_FAIL(block_start_seq.set_parallel_degree(parallel_idx))) {
          LOG_WARN("set parallel index failed", K(ret), K(parallel_idx));
        } else if (OB_FAIL(ObSSTableInsertManager::get_instance().add_sstable_slice(
                write_sstable_param, block_start_seq, row_iter, affected_rows))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to write sstable rows to storage layer", K(ret),
                K(row_tablet_id), K(block_start_seq), K(write_sstable_param));
          }
        }
        if (OB_SUCC(ret) || OB_ITER_END == ret) {
          ctx_.get_physical_plan_ctx()->add_affected_rows(affected_rows);
          notify_tablet_end(context_id, row_tablet_id, tablet_ids.count(), notify_idx, affected_rows > 0);
          if (row_iter.get_current_tablet_id() == row_tablet_id) {
            ret = OB_ITER_END; // continue to next partition
          }
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    // try flush sstable, ignore ret
    while (OB_ITER_END == ret && notify_idx < tablet_ids.count()) {
      ObTabletID &notify_tablet_id = tablet_ids.at(notify_idx);
      notify_tablet_end(context_id, notify_tablet_id, tablet_ids.count(), notify_idx);
    }
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
      if (OB_ISNULL(auto_inc_expr)) {
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
        datum.set_uint(next_autoinc_val);
        auto_inc_expr->set_evaluated_projected(eval_ctx_);
      }
    }
  } else {
    ret = child_->get_next_row();
  }
  if (OB_SUCC(ret)) {
    op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SSTABLE_INSERT_ROW_COUNT;
    op_monitor_info_.otherstat_1_value_++;
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::get_all_rows_and_count()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_store_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache row store or row_cnt_map is not inited", K(ret));
  } else {
    int64_t curr_tablet_row_cnt = 0;
    while (OB_SUCC(ret)) {
      const ObExprPtrIArray *row = &child_->get_spec().output_;
      ObTabletID row_tablet_id;
      ObChunkDatumStore *tablet_store = nullptr;
      clear_evaluated_flag();
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail get next row from child", K(ret));
        }
      } else if (OB_FAIL(get_tablet_id_from_row(*row,
                                                get_spec().row_desc_.get_part_id_index(),
                                                row_tablet_id))) {
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
        if (OB_FAIL(ObSSTableInsertManager::get_instance().get_tablet_cache_interval(
                                                                 ctx_.get_sqc_handler()->get_ddl_context_id(),
                                                                 iter->first,
                                                                 interval))) {
          LOG_WARN("failed to get tablet cache intervals", K(ret));
        } else if (OB_FAIL(tablet_seq_caches_.push_back(interval))) {
          LOG_WARN("failed to add tablet cache interval", K(ret), K(interval));
        }
      }
      if (OB_SUCC(ret)) {
        std::sort(tablet_seq_caches_.begin(), tablet_seq_caches_.end());
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
