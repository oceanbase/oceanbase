/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_modify_executor.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/das/ob_das_insert_op.h"

using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{
int ObTableApiModifyExecutor::open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_das_ref())) {
    LOG_WARN("fail to init das dml ctx", K(ret));
  } else {
    is_opened_ = true;
  }
  return ret;
}

OB_INLINE int ObTableApiModifyExecutor::init_das_ref()
{
  int ret = OB_SUCCESS;
  ObDASRef &das_ref = dml_rtctx_.das_ref_;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx_);

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
  } else {
    // todo@dazhi: use different labels for each dml type
    const char *label = "DmlDASCtx";
    const bool use_dist_das = false;
    ObMemAttr memattr(session->get_effective_tenant_id(), label, ObCtxIds::EXECUTE_CTX_ID);
    // das_ref.set_expr_frame_info(expr_frame_info_);
    das_ref.set_mem_attr(memattr);
    das_ref.set_execute_directly(!use_dist_das);
  }

  return OB_SUCCESS;
}

int ObTableApiModifyExecutor::submit_all_dml_task()
{
  int ret = OB_SUCCESS;
  ObDASRef &das_ref = dml_rtctx_.das_ref_;

  if (das_ref.has_task()) {
    if (OB_FAIL(das_ref.execute_all_task())) {
      LOG_WARN("fail to execute all dml das tasks", K(ret));
    } else if (OB_FAIL(das_ref.close_all_task())) {
      LOG_WARN("fail to close all dml tasks", K(ret));
    } else {
      das_ref.reuse();
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::close()
{
  int ret = OB_SUCCESS;
  ObDASRef &das_ref = dml_rtctx_.das_ref_;

  if (!is_opened_) {
    // do nothing
  } else if (das_ref.has_task()) {
    if (OB_FAIL(das_ref.close_all_task())) {
      LOG_WARN("fail to close all insert das task", K(ret));
    } else {
      das_ref.reset();
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::init_das_dml_rtdef(const ObDASDMLBaseCtDef &das_ctdef,
                                                 ObDASDMLBaseRtDef &das_rtdef,
                                                 const ObDASTableLocMeta *loc_meta)
{
  int ret = OB_SUCCESS;

  ObDASRef &das_ref = dml_rtctx_.das_ref_;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(das_ref.get_exec_ctx());
  ObDASCtx &das_ctx = das_ref.get_exec_ctx().get_das_ctx();
  uint64_t table_loc_id = das_ctdef.table_id_;
  uint64_t ref_table_id = das_ctdef.index_tid_;
  das_rtdef.timeout_ts_ = tb_ctx_.get_timeout_ts();
  das_rtdef.prelock_ = my_session->get_prelock();
  das_rtdef.tenant_schema_version_ = tb_ctx_.get_tenant_schema_version();
  das_rtdef.sql_mode_ = my_session->get_sql_mode();
  das_rtdef.table_loc_ = das_ctx.get_table_loc_by_id(table_loc_id, ref_table_id);
  if (OB_ISNULL(das_rtdef.table_loc_)) {
    if (OB_ISNULL(loc_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("loc meta is null", K(ret), K(table_loc_id),
          K(ref_table_id), K(das_ctx.get_table_loc_list()));
    } else if (OB_FAIL(das_ctx.extended_table_loc(*loc_meta, das_rtdef.table_loc_))) {
      LOG_WARN("extended table location failed", K(ret), KPC(loc_meta));
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::init_related_das_rtdef(const DASDMLCtDefArray &das_ctdefs,
                                                     DASDMLRtDefArray &das_rtdefs)
{
  int ret = OB_SUCCESS;
  ObDASRef &das_ref = dml_rtctx_.das_ref_;
  int64_t ct_count = das_ctdefs.count();

  if (!das_ctdefs.empty()) {
    ObIAllocator &allocator = das_ref.get_exec_ctx().get_allocator();
    if (OB_FAIL(das_rtdefs.allocate_array(allocator, ct_count))) {
      SQL_DAS_LOG(WARN, "fail to create das insert rtdef array", K(ret), K(ct_count));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ct_count; ++i) {
    ObDASTaskFactory &das_factory = das_ref.get_exec_ctx().get_das_ctx().get_das_factory();
    ObDASBaseRtDef *das_rtdef = nullptr;
    if (OB_FAIL(das_factory.create_das_rtdef(das_ctdefs.at(i)->op_type_, das_rtdef))) {
      SQL_DAS_LOG(WARN, "fail to create das insert rtdef", K(ret));
    } else if (OB_FAIL(init_das_dml_rtdef(*das_ctdefs.at(i),
                                          static_cast<ObDASDMLBaseRtDef&>(*das_rtdef),
                                          nullptr))) {
      SQL_DAS_LOG(WARN, "fail to init das dml rtdef", K(ret));
    } else {
      das_rtdefs.at(i) = static_cast<ObDASDMLBaseRtDef*>(das_rtdef);
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::calc_tablet_loc(ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  ObTableID table_loc_id = get_table_ctx().get_ref_table_id();
  ObTableID ref_table_id = get_table_ctx().get_ref_table_id();;
  ObDASCtx &das_ctx = exec_ctx_.get_das_ctx();
  ObDASTableLoc *table_loc = nullptr;

  if (OB_ISNULL(table_loc = das_ctx.get_table_loc_by_id(table_loc_id, ref_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table location by table id failed", K(ret),
              K(table_loc_id), K(ref_table_id), K(das_ctx.get_table_loc_list()));
  } else {
    tablet_loc = table_loc->get_first_tablet_loc();
  }

  return ret;
}

int ObTableApiModifyExecutor::generate_ins_rtdef(const ObTableInsCtDef &ins_ctdef,
                                                 ObTableInsRtDef &ins_rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_das_dml_rtdef(ins_ctdef.das_ctdef_,
                                 ins_rtdef.das_rtdef_,
                                 nullptr))) {
    LOG_WARN("fail to init das dml rtdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(ins_ctdef.related_ctdefs_,
                                            ins_rtdef.related_rtdefs_))) {
    LOG_WARN("fail to init related das ctdef", K(ret));
  } else {
    ins_rtdef.das_rtdef_.related_ctdefs_ = &ins_ctdef.related_ctdefs_;
    ins_rtdef.das_rtdef_.related_rtdefs_ = &ins_rtdef.related_rtdefs_;
  }

  return ret;
}

int ObTableApiModifyExecutor::generate_del_rtdef(const ObTableDelCtDef &del_ctdef,
                                                 ObTableDelRtDef &del_rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_das_dml_rtdef(del_ctdef.das_ctdef_,
                                 del_rtdef.das_rtdef_,
                                 nullptr))) {
    LOG_WARN("fail to init das dml rtdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(del_ctdef.related_ctdefs_,
                                            del_rtdef.related_rtdefs_))) {
    LOG_WARN("fail to init related das ctdef", K(ret));
  } else {
    del_rtdef.das_rtdef_.related_ctdefs_ = &del_ctdef.related_ctdefs_;
    del_rtdef.das_rtdef_.related_rtdefs_ = &del_rtdef.related_rtdefs_;
  }

  return ret;
}

int ObTableApiModifyExecutor::generate_upd_rtdef(const ObTableUpdCtDef &upd_ctdef,
                                                 ObTableUpdRtDef &upd_rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_das_dml_rtdef(upd_ctdef.das_ctdef_,
                                 upd_rtdef.das_rtdef_,
                                 nullptr))) {
    LOG_WARN("fail to init das dml rtdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(upd_ctdef.related_ctdefs_,
                                            upd_rtdef.related_rtdefs_))) {
    LOG_WARN("fail to init related das ctdef", K(ret));
  } else {
    upd_rtdef.das_rtdef_.related_ctdefs_ = &upd_ctdef.related_ctdefs_;
    upd_rtdef.das_rtdef_.related_rtdefs_ = &upd_rtdef.related_rtdefs_;
    dml_rtctx_.get_exec_ctx().set_update_columns(&upd_ctdef.assign_columns_);
  }

  return ret;
}

int ObTableApiModifyExecutor::insert_row_to_das(const ObTableInsCtDef &ins_ctdef,
                                                ObTableInsRtDef &ins_rtdef)
{
  int ret = OB_SUCCESS;
  ObDASTabletLoc *tablet_loc = nullptr;
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
    LOG_WARN("fail to calc partition key", K(ret));
  } else if (OB_FAIL(ObDMLService::insert_row(ins_ctdef.das_ctdef_,
                                              ins_rtdef.das_rtdef_,
                                              tablet_loc,
                                              dml_rtctx_,
                                              ins_ctdef.new_row_,
                                              stored_row))) {
    LOG_WARN("fail to insert row by dml service", K(ret));
  }

  return ret;
}

int ObTableApiModifyExecutor::delete_row_to_das(const ObTableDelCtDef &del_ctdef,
                                                ObTableDelRtDef &del_rtdef)
{
  int ret = OB_SUCCESS;
  ObDASTabletLoc *tablet_loc = nullptr;
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  // todo:linjing check rowkey null and skip
  if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
    LOG_WARN("fail tp calc tablet location", K(ret));
  } else if (OB_FAIL(ObDMLService::delete_row(del_ctdef.das_ctdef_,
                                              del_rtdef.das_rtdef_,
                                              tablet_loc,
                                              dml_rtctx_,
                                              del_ctdef.old_row_,
                                              stored_row))) {
    LOG_WARN("fail to delete row to das op", K(ret), K(del_ctdef), K(del_rtdef));
  }

  return ret;
}

int ObTableApiModifyExecutor::get_next_conflict_rowkey(DASTaskIter &task_iter,
                                                       const ObConflictChecker &conflict_checker)
{
  int ret = OB_SUCCESS;
  bool got_row = false;

  while (OB_SUCC(ret) && !got_row) {
    ObNewRow *dup_row = nullptr;
    ObChunkDatumStore::StoredRow *stored_row = nullptr;
    ObDASWriteBuffer::DmlShadowRow ssr;
    ObDASInsertOp *ins_op = static_cast<ObDASInsertOp*>(*task_iter);
    ObNewRowIterator *conflict_result = ins_op->get_duplicated_result();
    const ObDASInsCtDef *ins_ctdef = static_cast<const ObDASInsCtDef*>(ins_op->get_ctdef());
    if (OB_ISNULL(conflict_result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("duplicted key result is null", K(ret));
    } else if (OB_FAIL(conflict_result->get_next_row(dup_row))) {
      if (OB_ITER_END == ret) {
        ++task_iter;
        if (!task_iter.is_end()) {
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("fail to get next row from das result", K(ret));
      }
    } else if (OB_FAIL(ssr.init(dml_rtctx_.get_das_alloc(), ins_ctdef->table_rowkey_types_, false))) {
      LOG_WARN("fail to init shadow stored row", K(ret), K(ins_ctdef->table_rowkey_types_));
    } else if (OB_FAIL(ssr.shadow_copy(*dup_row))) {
      LOG_WARN("fail to shadow copy ob new row", K(ret));
    } else if (OB_ISNULL(stored_row = ssr.get_store_row())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("stored row is null", K(ret));
    } else if (OB_FAIL(stored_row->to_expr(conflict_checker.checker_ctdef_.data_table_rowkey_expr_,
        conflict_checker.eval_ctx_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row from result iterator", K(ret));
      }
    } else {
      got_row = true;
    }
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase