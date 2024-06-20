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
#include "ob_table_scan_executor.h"
#include "ob_table_context.h"
#include "sql/das/ob_das_utils.h"
#include "share/index_usage/ob_index_usage_info_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace table
{

int ObTableApiScanExecutor::init_das_scan_rtdef(const ObDASScanCtDef &das_ctdef,
                                                ObDASScanRtDef &das_rtdef,
                                                const ObDASTableLocMeta *loc_meta)
{
  int ret = OB_SUCCESS;
  const ObTableCtx &tb_ctx = get_table_ctx();
  const ObTableApiScanCtDef &tsc_ctdef = scan_spec_.get_ctdef();
  bool is_lookup = (&das_ctdef == tsc_ctdef.lookup_ctdef_);
  das_rtdef.timeout_ts_ = tb_ctx.get_timeout_ts();
  das_rtdef.scan_flag_.scan_order_ = is_lookup ? ObQueryFlag::KeepOrder : tb_ctx.get_scan_order();
  das_rtdef.scan_flag_.index_back_ = tb_ctx.is_index_back();
  das_rtdef.scan_flag_.read_latest_ = tb_ctx.is_read_latest();
  das_rtdef.need_check_output_datum_ = false;
  das_rtdef.sql_mode_ = SMO_DEFAULT;
  das_rtdef.stmt_allocator_.set_alloc(&das_ref_.get_das_alloc());
  das_rtdef.scan_allocator_.set_alloc(&das_ref_.get_das_alloc());
  das_rtdef.eval_ctx_ = &get_eval_ctx();
  if (!is_lookup) {
    das_rtdef.limit_param_.limit_ = tb_ctx.get_limit();
    das_rtdef.limit_param_.offset_ = tb_ctx.get_offset();
  }
  if (OB_FAIL(das_rtdef.init_pd_op(exec_ctx_, das_ctdef))) {
    LOG_WARN("fail to init pushdown storage filter", K(ret));
  } else {
    das_rtdef.tenant_schema_version_ = tb_ctx.get_tenant_schema_version();
    ObTableID table_loc_id = tb_ctx.get_ref_table_id();
    das_rtdef.table_loc_ = exec_ctx_.get_das_ctx().get_table_loc_by_id(table_loc_id, das_ctdef.ref_table_id_);
    if (OB_ISNULL(das_rtdef.table_loc_)) {
      ObDASTabletLoc *tablet_loc = nullptr;
      if (OB_ISNULL(loc_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get table loc by id", K(ret), K(table_loc_id), K(das_ctdef.ref_table_id_));
      } else if (OB_FAIL(exec_ctx_.get_das_ctx().extended_table_loc(*loc_meta, das_rtdef.table_loc_))) {
        LOG_WARN("fail to extend table loc", K(ret), KPC(loc_meta));
      } else if (OB_FAIL(exec_ctx_.get_das_ctx().extended_tablet_loc(*das_rtdef.table_loc_,
                                                                     tb_ctx.get_tablet_id(),
                                                                     tablet_loc))) {
        LOG_WARN("fail to extend tablet loc", K(ret), K(tb_ctx.get_tablet_id()));
      }
    }
  }
  return ret;
}

int ObTableApiScanExecutor::init_tsc_rtdef()
{
  int ret = OB_SUCCESS;
  // init das_ref_
  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = MTL_ID();
  mem_attr.label_ = "ScanDASCtx";
  das_ref_.set_mem_attr(mem_attr);
  das_ref_.set_expr_frame_info(scan_spec_.get_expr_frame_info());
  das_ref_.set_execute_directly(true);
  // init rtdef
  const ObDASScanCtDef &scan_ctdef = scan_spec_.get_ctdef().scan_ctdef_;
  ObDASScanRtDef &scan_rtdef = tsc_rtdef_.scan_rtdef_;
  if (OB_FAIL(init_das_scan_rtdef(scan_ctdef, scan_rtdef, NULL))) {
    LOG_WARN("fail to init das scan rtdef", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (scan_spec_.get_ctdef().lookup_ctdef_ != NULL) {
      const ObDASScanCtDef &lookup_ctdef = *scan_spec_.get_ctdef().lookup_ctdef_;
      ObDASBaseRtDef *das_rtdef = NULL;
      ObDASTaskFactory &das_factory = exec_ctx_.get_das_ctx().get_das_factory();
      if (OB_FAIL(das_factory.create_das_rtdef(DAS_OP_TABLE_SCAN, das_rtdef))) {
        LOG_WARN("fail to create das rtdef", K(ret));
      } else {
        tsc_rtdef_.lookup_rtdef_ = static_cast<ObDASScanRtDef*>(das_rtdef);
        if (OB_FAIL(init_das_scan_rtdef(lookup_ctdef,
                                        *tsc_rtdef_.lookup_rtdef_,
                                        scan_spec_.get_ctdef().lookup_loc_meta_))) {
          LOG_WARN("fail to init das scan rtdef", K(ret), K(lookup_ctdef));
        }
      }
    }
  }

  return ret;
}

int ObTableApiScanExecutor::prepare_das_task()
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *task_op = nullptr;
  ObDASScanOp *scan_op = nullptr;
  ObDASTabletLoc *tablet_loc = tsc_rtdef_.scan_rtdef_.table_loc_->get_first_tablet_loc();
  if (OB_FAIL(das_ref_.create_das_task(tablet_loc,
                                       DAS_OP_TABLE_SCAN,
                                       task_op))) {
    LOG_WARN("fail to prepare das task", K(ret));
  } else {
    scan_op = static_cast<ObDASScanOp*>(task_op);
    scan_op->set_scan_ctdef(&scan_spec_.get_ctdef().scan_ctdef_);
    scan_op->set_scan_rtdef(&tsc_rtdef_.scan_rtdef_);
    scan_op->set_can_part_retry(false);
    tsc_rtdef_.scan_rtdef_.table_loc_->is_reading_ = true;
    if (scan_spec_.get_ctdef().lookup_ctdef_ != nullptr) {
      //is local index lookup, need to set the lookup ctdef to the das scan op
      ObDASTableLoc *lookup_table_loc = tsc_rtdef_.lookup_rtdef_->table_loc_;
      ObDASTabletLoc *tablet_loc_list = tsc_rtdef_.lookup_rtdef_->table_loc_->get_first_tablet_loc();
      ObDASTabletLoc *lookup_tablet_loc = ObDASUtils::get_related_tablet_loc(
          *tablet_loc_list, lookup_table_loc->loc_meta_->ref_table_id_);
      if (OB_ISNULL(lookup_tablet_loc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lookup tablet loc is nullptr", K(ret), KPC(lookup_table_loc->loc_meta_));
      } else if (OB_FAIL(scan_op->reserve_related_buffer(1))) {
        LOG_WARN("failed to set related scan cnt", K(ret));
      } else if (OB_FAIL(scan_op->set_related_task_info(scan_spec_.get_ctdef().lookup_ctdef_,
                                                        tsc_rtdef_.lookup_rtdef_,
                                                        lookup_tablet_loc->tablet_id_))) {
        LOG_WARN("set related task info failed", K(ret));
      } else {
        lookup_table_loc->is_reading_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      // set scan range
      ObIArray<ObNewRange> &scan_ranges = scan_op->get_scan_param().key_ranges_;
      if (OB_FAIL(scan_ranges.assign(get_table_ctx().get_key_ranges()))) {
        LOG_WARN("fail to assign scan ranges", K(ret));
      }
    }
  }

  return ret;
}

int ObTableApiScanExecutor::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (das_ref_.has_task()) {
    if (OB_FAIL(das_ref_.execute_all_task())) {
      LOG_WARN("fail to execute all das scan task", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    //prepare to output row
    scan_result_ = das_ref_.begin_result_iter();
  }
  return ret;
}

int ObTableApiScanExecutor::do_init_before_get_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare_das_task())) {
    LOG_WARN("fail to prepare das task", K(ret));
  } else if (OB_FAIL(do_table_scan())) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to do table scan", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    need_do_init_ = false;
  }

  return ret;
}

int ObTableApiScanExecutor::get_next_row_with_das()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  while (OB_SUCC(ret) && !got_row) {
    bool filter = false;
    clear_evaluated_flag();
    if (OB_FAIL(scan_result_.get_next_row())) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(scan_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task failed", K(ret));
          }
        }
      } else {
        LOG_WARN("get next row from das result failed", K(ret));
      }
    } else if (OB_FAIL(check_filter(filter))) {
      LOG_WARN("fail to check row filtered", K(ret));
    } else if (filter) {
      LOG_DEBUG("the row is filtered", K(ret));
    } else {
      ++input_row_cnt_;
      ++output_row_cnt_;
      got_row = true;
    }
  }
  return ret;
}

int ObTableApiScanExecutor::check_filter(bool &filter)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &exprs = scan_spec_.get_ctdef().filter_exprs_;
  ObDatum *datum = NULL;
  filter = false;

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count() && !filter; i++) {
    if (OB_FAIL(exprs.at(i)->eval(eval_ctx_, datum))) {
      LOG_WARN("fail to eval filter expr", K(ret), K(*exprs.at(i)));
    } else if (tb_ctx_.is_ttl_table()) {
      filter = (!datum->is_null() && datum->get_bool()); // ttl场景下，过期表达式不过滤is_null
    } else {
      filter = datum->get_bool();
    }
  }

  return ret;
}

int ObTableApiScanExecutor::open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_tsc_rtdef())) {
    LOG_WARN("fail to init table scan rtdef", K(ret));
  } else {
    is_opened_ = true;
  }
  return ret;
}

int ObTableApiScanExecutor::get_next_row()
{
  int ret = OB_SUCCESS;
  if (0 == get_table_ctx().get_limit()) {
    // limit 0，直接返回iter end
    ret = OB_ITER_END;
  } else if (need_do_init_ && OB_FAIL(do_init_before_get_row())) {
    LOG_WARN("fail to do init before get row", K(ret));
  } else if (OB_FAIL(get_next_row_with_das())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row from das", K(ret));
    }
  }
  return ret;
}

int ObTableApiScanExecutor::close()
{
  int ret = OB_SUCCESS;

  if (!is_opened_) {
    // do nothing
  } else if (das_ref_.has_task()) {
    if (OB_FAIL(das_ref_.close_all_task())) {
      LOG_WARN("fail to close all das task", K(ret));
    } else {
      reset();
    }
  }

  if (OB_SUCC(ret)) {
    oceanbase::share::ObIndexUsageInfoMgr *mgr = MTL(oceanbase::share::ObIndexUsageInfoMgr *);
    if (tb_ctx_.get_table_id() == tb_ctx_.get_ref_table_id()) {
      // skip // use primary key, do nothing
    } else if (OB_NOT_NULL(mgr)) {
      mgr->update(tb_ctx_.get_tenant_id(), tb_ctx_.get_index_table_id());
    }
  }

  return ret;
}

int ObTableApiScanRowIterator::open(ObTableApiScanExecutor *executor)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(executor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan executor is null", K(ret));
  } else if (OB_FAIL(executor->open())) {
    LOG_WARN("fail to open scan executor", K(ret));
  } else {
    scan_executor_ = executor;
  }

  return ret;
}

// Memory of row is owned by iterator, and row cannot be used after iterator close
// or get_next_row next time unless you use deep copy.
int ObTableApiScanRowIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObNewRow *tmp_row = nullptr;
  char *row_buf = nullptr;
  ObObj *cells = nullptr;
  const ObTableCtx &tb_ctx = scan_executor_->get_table_ctx();
  const ExprFixedArray &output_exprs = scan_executor_->get_spec().get_ctdef().output_exprs_;
  const ObIArray<uint64_t> &query_col_ids = tb_ctx.get_query_col_ids();
  const int64_t cells_cnt = tb_ctx.is_scan() ? query_col_ids.count() : output_exprs.count();
  row_allocator_.reuse();

  if (OB_ISNULL(scan_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan executor is null", K(ret));
  } else if (OB_FAIL(scan_executor_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row by scan executor", K(ret));
    }
  } else if (OB_ISNULL(row_buf = static_cast<char*>(row_allocator_.alloc(sizeof(ObNewRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObNewRow buffer", K(ret));
  } else if (OB_ISNULL(cells = static_cast<ObObj*>(row_allocator_.alloc(sizeof(ObObj) * cells_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cells buffer", K(ret), K(cells_cnt));
  } else {
    // 循环select_exprs,eval获取datum，并将datum转ObObj，最后组成ObNewRow
    tmp_row = new(row_buf)ObNewRow(cells, cells_cnt);
    ObObj tmp_obj;
    ObDatum *datum = nullptr;
    ObEvalCtx &eval_ctx = scan_executor_->get_eval_ctx();
    if (tb_ctx.is_scan()) { // 转为用户select的顺序
      const ObIArray<uint64_t> &select_col_ids = tb_ctx.get_select_col_ids();
      for (int64_t i = 0; OB_SUCC(ret) && i < query_col_ids.count(); i++) {
        uint64_t col_id = query_col_ids.at(i);
        int64_t idx = -1;
        if (!has_exist_in_array(select_col_ids, col_id, &idx)) {
          ret = OB_ERR_COLUMN_NOT_FOUND;
          LOG_WARN("query column id not found", K(ret), K(select_col_ids), K(col_id), K(query_col_ids));
        } else if (OB_FAIL(output_exprs.at(idx)->eval(eval_ctx, datum))) {
          LOG_WARN("fail to eval datum", K(ret));
        } else if (OB_FAIL(datum->to_obj(tmp_obj, output_exprs.at(idx)->obj_meta_))) {
          LOG_WARN("fail to datum to obj", K(ret), K(output_exprs.at(idx)->obj_meta_), K(i), K(idx));
        } else {
          cells[i] = tmp_obj;
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cells_cnt; i++) {
        if (OB_FAIL(output_exprs.at(i)->eval(eval_ctx, datum))) {
          LOG_WARN("fail to eval datum", K(ret));
        } else if (OB_FAIL(datum->to_obj(tmp_obj, output_exprs.at(i)->obj_meta_))) {
          LOG_WARN("fail to datum to obj", K(ret), K(output_exprs.at(i)->obj_meta_));
        } else {
          cells[i] = tmp_obj;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = tmp_row;
  }

  return ret;
}

// deep copy the new row using given allocator
int ObTableApiScanRowIterator::get_next_row(ObNewRow *&row, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObNewRow *inner_row = nullptr;
  if (OB_FAIL(get_next_row(inner_row))) {
    LOG_WARN("fail to get next row", KR(ret));
  } else if (OB_ISNULL(inner_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new row is null", KR(ret));
  } else {
    ObNewRow *tmp_row = nullptr;
    int64_t buf_size = inner_row->get_deep_copy_size() + sizeof(ObNewRow);
    char *tmp_row_buf = static_cast<char *>(allocator.alloc(buf_size));
    if (OB_ISNULL(tmp_row_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new row", KR(ret));
    } else {
      tmp_row = new(tmp_row_buf)ObNewRow();
      int64_t pos = sizeof(ObNewRow);
      if (OB_FAIL(tmp_row->deep_copy(*inner_row, tmp_row_buf, buf_size, pos))) {
        LOG_WARN("fail to deep copy new row", KR(ret));
      } else {
        row = tmp_row;
      }
    }
  }
  return ret;
}

int ObTableApiScanRowIterator::close()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(scan_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan executor is null", K(ret));
  } else if (OB_FAIL(scan_executor_->close())) {
    LOG_WARN("fail to close scan executor", K(ret));
  } else {
    row_allocator_.reset();
  }

  return ret;
}

void ObTableApiScanExecutor::clear_evaluated_flag()
{
  const ExprFixedArray &filter_exprs = get_spec().get_ctdef().filter_exprs_;
  for (int64_t i = 0; i < filter_exprs.count(); i++) {
    ObSQLUtils::clear_expr_eval_flags(*filter_exprs.at(i), eval_ctx_);
  }
  ObTableApiExecutor::clear_evaluated_flag();
}

}  // namespace table
}  // namespace oceanbase
