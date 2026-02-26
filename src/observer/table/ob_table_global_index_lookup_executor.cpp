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
#include "ob_table_global_index_lookup_executor.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace table
{
ObTableGlobalIndexLookupExecutor::ObTableGlobalIndexLookupExecutor(ObTableApiScanExecutor *scan_executor)
  : ObIndexLookupOpImpl(GLOBAL_INDEX, DEFAULT_BATCH_ROW_COUNT /*default_batch_row_count*/),
    allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    scan_executor_(scan_executor),
    tb_ctx_(scan_executor_->get_table_ctx()),
    das_ref_(scan_executor_->get_eval_ctx(), scan_executor_->get_exec_ctx()),
    lookup_result_(),
    lookup_range_()
{}

void ObTableGlobalIndexLookupExecutor::do_clear_evaluated_flag()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan executor is NULL", K(ret));
  } else {
    scan_executor_->clear_evaluated_flag();
  }
}

int ObTableGlobalIndexLookupExecutor::check_lookup_row_cnt()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo &my_session = tb_ctx_.get_session_info();
  if (OB_ISNULL(scan_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan executor is NULL", K(ret));
  } else if (GCONF.enable_defensive_check() && OB_UNLIKELY(lookup_rowkey_cnt_ != lookup_row_cnt_)) {
    ret = OB_ERR_DEFENSIVE_CHECK;
    ObString func_name = ObString::make_string("check_lookup_row_cnt");
    LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
    LOG_ERROR("Fatal Error!!! Catch a defensive error!",
              K(ret), K_(lookup_rowkey_cnt), K_(lookup_row_cnt),
              K(DAS_CTX(scan_executor_->get_exec_ctx()).get_snapshot()),
              K_(lookup_range),
              KPC(my_session.get_tx_desc()));
  }
  return ret;
}

int ObTableGlobalIndexLookupExecutor::open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo &sess_info = tb_ctx_.get_session_info();
  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = sess_info.get_effective_tenant_id();
  mem_attr.label_ = ObModIds::OB_SQL_TABLE_LOOKUP;
  das_ref_.set_mem_attr(mem_attr);
  das_ref_.set_expr_frame_info(get_spec().get_expr_frame_info());
  if (OB_ISNULL(scan_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan executor is NULL", K(ret));
  } else {
    scan_executor_->das_ref_.set_lookup_iter(&lookup_result_);
    scan_executor_->tsc_rtdef_.lookup_rtdef_->scan_allocator_.set_alloc(&allocator_);
    scan_executor_->tsc_rtdef_.lookup_rtdef_->stmt_allocator_.set_alloc(&allocator_);
  }
  return ret;
}

int ObTableGlobalIndexLookupExecutor::get_next_row_from_index_table()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = tb_ctx_.get_tenant_id();
  if (allocator_.used() > get_memory_limit(tenant_id)) {
    // if the memory used is out of memory limit, force to stop the index table scan
    // and the reserved index rows will be scanned in next batch
    ret = OB_ITER_END;
    LOG_WARN("allocator use reach limit", K(ret), K(allocator_.used()), K(get_memory_limit(tenant_id)));
  } else if (OB_FAIL(scan_executor_->get_next_row_for_tsc())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from child failed", K(ret));
    }
  }
  return ret;
}

bool ObTableGlobalIndexLookupExecutor::has_das_scan_task(const ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_op)
{
  das_op = static_cast<ObDASScanOp*>(das_ref_.find_das_task(
                                      tablet_loc, DAS_OP_TABLE_SCAN));
  return das_op != nullptr;
}

int ObTableGlobalIndexLookupExecutor::process_data_table_rowkey()
{
  int ret = OB_SUCCESS;
  ObObjectID partition_id = ObExprCalcPartitionId::NONE_PARTITION_ID;
  ObTabletID tablet_id;
  ObDASScanOp *das_scan_op = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;
  ObDASScanRtDef *lookup_rtdef = nullptr;
  if (OB_ISNULL(scan_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan executor is NULL", K(ret));
  } else if (OB_ISNULL(lookup_rtdef = scan_executor_->tsc_rtdef_.lookup_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lookup_rtdef is NULL", K(ret));
  } else if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(get_calc_part_id_expr(),
                                                               get_eval_ctx(),
                                                               partition_id,
                                                               tablet_id))) {
    LOG_WARN("fail to calc part id", K(ret), KPC(get_calc_part_id_expr()));
  } else if (OB_ISNULL(lookup_rtdef->table_loc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lookup_rtdef table_loc is NULL", K(ret));
  } else if (OB_FAIL(DAS_CTX(scan_executor_->get_exec_ctx()).extended_tablet_loc(*lookup_rtdef->table_loc_,
            tablet_id, tablet_loc))) {
    LOG_WARN("failt to extend tablet loc", K(ret), K(tablet_id));
  } else if (OB_UNLIKELY(!has_das_scan_task(tablet_loc, das_scan_op))) {
    ObIDASTaskOp *tmp_op = nullptr;
    if (OB_FAIL(das_ref_.create_das_task(tablet_loc, DAS_OP_TABLE_SCAN, tmp_op))) {
      LOG_WARN("prepare das task failed", K(ret));
    } else if (OB_ISNULL(tmp_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tmp op is NULL", K(ret));
    } else {
      das_scan_op = static_cast<ObDASScanOp*>(tmp_op);
      das_scan_op->set_scan_ctdef(get_lookup_ctdef());
      das_scan_op->set_scan_rtdef(lookup_rtdef);
      das_scan_op->set_can_part_retry(true);
    }
  }
  if (OB_SUCC(ret)) {
    storage::ObTableScanParam &scan_param = das_scan_op->get_scan_param();
    lookup_range_.reset();
    if (OB_FAIL(build_data_table_range(lookup_range_))) {
      LOG_WARN("build data table range failed", K(ret), KPC(tablet_loc));
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(lookup_range_))) {
      LOG_WARN("store lookup key range failed", K(ret), K(scan_param));
    } else {
      scan_param.is_get_ = true;
    }
    LOG_DEBUG("process data table rowkey: ", K(scan_param.key_ranges_), K(tablet_id));
  }
  return ret;
}

int ObTableGlobalIndexLookupExecutor::build_data_table_range(common::ObNewRange &lookup_range)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = get_ctdef().global_index_rowkey_exprs_.count();
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new(buf) ObObj[rowkey_cnt];
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    ObObj tmp_obj;
    ObExpr *expr = nullptr;
    if (OB_ISNULL(expr = get_ctdef().global_index_rowkey_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey expr is NULL", K(ret), K(i));
    } else {
      ObDatum &col_datum = expr->locate_expr_datum(get_eval_ctx());
      if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      } else if (OB_FAIL(ob_write_obj(allocator_, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObRowkey table_rowkey(obj_ptr, rowkey_cnt);
    uint64_t ref_table_id = get_lookup_ctdef()->ref_table_id_;
    if (OB_FAIL(lookup_range.build_range(ref_table_id, table_rowkey))) {
      LOG_WARN("build lookup range failed", K(ret), K(ref_table_id), K(table_rowkey));
    }
    LOG_DEBUG("build data table range", K(ret), K(table_rowkey), K(lookup_range));
  }
  return ret;
}

int ObTableGlobalIndexLookupExecutor::do_index_lookup()
{
  int ret = OB_SUCCESS;
  const int64_t curr_time = ObTimeUtility::fast_current_time();
  if (OB_FAIL(das_ref_.execute_all_task())) {
    LOG_WARN("fail to execute das task", K(ret), K(curr_time));
  } else {
    lookup_result_ = das_ref_.begin_result_iter();
  }
  return ret;
}

int ObTableGlobalIndexLookupExecutor::get_next_row_from_data_table()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  if (OB_UNLIKELY(lookup_result_.is_end())) {
    ret = OB_ITER_END;
    LOG_DEBUG("lookup task is empty", K(ret));
  }
  do_clear_evaluated_flag();
  while (OB_SUCC(ret) && !got_row) {
    bool filter = false;
    if (OB_FAIL(lookup_result_.get_next_row())) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(lookup_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task result failed", K(ret));
          }
        }
      } else {
        LOG_WARN("get next row from das result failed", K(ret));
      }
    } else if (OB_FAIL(scan_executor_->check_filter(filter))) {
      LOG_WARN("fail to check row filtered", K(ret));
    } else if (filter) {
      lookup_row_cnt_++;
      LOG_DEBUG("the row is filtered", K(ret), K(lookup_row_cnt_), K(lookup_rowkey_cnt_));
    } else {
      got_row = true;
    }
  }
  return ret;
}

int ObTableGlobalIndexLookupExecutor::reset_lookup_state()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(das_ref_.close_all_task())) {
    LOG_WARN("close all das task failed", K(ret));
  } else {
    das_ref_.reuse();
    allocator_.reset_remain_one_page();
    index_end_ = false;
  }
  return ret;
}

int ObTableGlobalIndexLookupExecutor::close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(das_ref_.close_all_task())) {
    LOG_WARN("close all das task failed", K(ret));
  }
  return ret;
}

void ObTableGlobalIndexLookupExecutor::destroy()
{
  state_ = FINISHED;
  index_end_ = true;
  das_ref_.reset();
  allocator_.reset_remain_one_page();
}

int ObTableGlobalIndexLookupExecutor::get_next_rows_from_index_table(int64_t &count, int64_t capacity)
{
  return OB_NOT_IMPLEMENT;
}

int ObTableGlobalIndexLookupExecutor::get_next_rows_from_data_table(int64_t &count, int64_t capacity)
{
  return OB_NOT_IMPLEMENT;
}

int ObTableGlobalIndexLookupExecutor::process_data_table_rowkeys(const int64_t size, const ObBitVector *skip)
{
  return OB_NOT_IMPLEMENT;
}

}  // namespace table
}  // namespace oceanbase