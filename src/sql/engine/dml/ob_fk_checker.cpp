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
#include "sql/engine/dml/ob_fk_checker.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{
namespace sql
{


int ObForeignKeyChecker::reset() {
  int ret = OB_SUCCESS;
  if (se_rowkey_dist_ctx_ != nullptr) {
    se_rowkey_dist_ctx_->destroy();
    se_rowkey_dist_ctx_ = nullptr;
  }
  clear_exprs_.reset();
  table_rowkey_.reset();
  if (das_ref_.has_task()) {
    if (OB_FAIL(das_ref_.close_all_task())) {
      LOG_WARN("close all das task failed", K(ret));
    }
  }
  das_ref_.reset();
  return ret;
}

int ObForeignKeyChecker::reuse() {
  int ret = OB_SUCCESS;
  batch_distinct_fk_cnt_ = 0;
  if (das_ref_.has_task()) {
    if (OB_FAIL(das_ref_.close_all_task())) {
      LOG_WARN("close all das task failed", K(ret));
    }
  }
  das_ref_.reuse();
  return ret;
}

int ObForeignKeyChecker::do_fk_check_batch(bool &all_has_result)
{
  int ret = OB_SUCCESS;

  int64_t get_row_count = 0;
  if (0 == batch_distinct_fk_cnt_) {
    LOG_TRACE("distinct foreign key count is 0 in a batch");
    all_has_result = true;
  } else if (OB_FAIL(das_ref_.execute_all_task())) {
    LOG_WARN("execute all scan das task failed", K(ret));
  } else if (OB_FAIL(get_scan_result_count(get_row_count))) {
    LOG_WARN("failed to check the result count od foreign key scan task", K(ret));
  } else if (get_row_count > batch_distinct_fk_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result row count exceeds the number of unique key", K(ret), K(get_row_count), K(batch_distinct_fk_cnt_));
  } else if (get_row_count == batch_distinct_fk_cnt_) {
    all_has_result = true;
  } else {
    all_has_result = false;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(das_ref_.close_all_task())) {
      LOG_WARN("close all das task failed", K(ret));
    } else {
      reuse();
    }
  }
  return ret;
}

int ObForeignKeyChecker::get_scan_result_count(int64_t &get_row_count)
{
  int ret = OB_SUCCESS;
  get_row_count = 0;
  DASOpResultIter result_iter = das_ref_.begin_result_iter();
  while (OB_SUCC(ret)) {
    if (OB_FAIL(result_iter.get_next_row())) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(result_iter.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task failed", K(ret));
          }
        }
      } else {
        LOG_WARN("get next row from das result failed", K(ret));
      }
    } else {
      get_row_count++;
    }
  }
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

int ObForeignKeyChecker::do_fk_check_single_row(const ObIArray<ObForeignKeyColumn> &columns, const ObExprPtrIArray &row, bool &has_result)
{
  int ret = OB_SUCCESS;
  bool need_check = true;
  bool is_all_null = false;
  bool has_null = false;

  if (OB_FAIL(build_fk_check_das_task(columns, row, need_check))) {
    LOG_WARN("failed to build table look up das task", K(ret));
  } else if (!need_check) {
    has_result = true;
    LOG_TRACE("This value has been checked successfully in previous row", K(row));
  } else if (OB_FAIL(das_ref_.execute_all_task())) {
    LOG_WARN("execute all scan das task failed", K(ret));
  } else {
    // check if result is empty
    DASOpResultIter result_iter = das_ref_.begin_result_iter();
    int64_t result_cnt = 0;
    bool got_row = false;
    if (1 != das_ref_.get_das_task_cnt()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("has more than one das task", K(das_ref_.get_das_task_cnt()), K(ret));
    } else if (OB_FAIL(result_iter.get_next_row())) {
      if (OB_ITER_END == ret) {
         ret = OB_SUCCESS;
      }
      has_result = false;
    } else {
      has_result = true;
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(das_ref_.close_all_task())) {
    LOG_WARN("close all das task failed", K(ret));
  } else {
    reuse();
  }
  return ret;
}

int ObForeignKeyChecker::build_fk_check_das_task(const ObIArray<ObForeignKeyColumn> &columns,
                                    const ObExprPtrIArray &row,
                                    bool &need_check)
{
  int ret = OB_SUCCESS;
  ObDASScanOp *das_scan_op = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;
  ObNewRange lookup_range;
  need_check = true;
  bool is_all_null = false;
  bool has_null = false;
  if (OB_FAIL(check_fk_columns_has_null(columns, row, is_all_null, has_null))) {
    LOG_WARN("failed to check foreign key columns are all null", K(ret));
  } else if (has_null) {
    // Match simple is the ony one match method of OB, if foreign key columns has null, it will pass foreign key check;
    // Note: we need to support match partial and match full method for a more strict foreign key check in MySQL mode
    need_check = false;
    LOG_TRACE("foreign key columns has null, pass foreign key check");
  } else if (OB_FAIL(build_table_range(columns, row, lookup_range, need_check))) {
    LOG_WARN("build data table range failed", K(ret), KPC(tablet_loc));
  } else if (!need_check) {
    LOG_TRACE("The current foreign key has been successfully checked before", K(lookup_range));
  } else if (OB_FAIL(calc_lookup_tablet_loc(tablet_loc))) {
    LOG_WARN("calc lookup pkey fail", K(ret));
  } else if (OB_FAIL(get_das_scan_op(tablet_loc, das_scan_op))) {
    LOG_WARN("get_das_scan_op failed", K(ret), K(tablet_loc));
  } else if (OB_ISNULL(das_scan_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das_scan_op should be not null", K(ret));
  } else {
    storage::ObTableScanParam &scan_param = das_scan_op->get_scan_param();
    if (OB_FAIL(scan_param.key_ranges_.push_back(lookup_range))) {
      LOG_WARN("store lookup key range failed", K(ret), K(lookup_range), K(scan_param));
    } else {
      batch_distinct_fk_cnt_ += 1;
      LOG_TRACE("after build conflict rowkey", K(scan_param.tablet_id_),
                K(scan_param.key_ranges_.count()), K(lookup_range), K(batch_distinct_fk_cnt_));
    }
  }
  return ret;
}

int ObForeignKeyChecker::calc_lookup_tablet_loc(ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  ObExpr *part_id_expr = NULL;
  ObTabletID tablet_id;
  ObObjectID partition_id = OB_INVALID_ID;
  tablet_loc = nullptr;
  if (OB_ISNULL(part_id_expr = checker_ctdef_.calc_part_id_expr_)) {
    tablet_loc = local_tablet_loc_;
    if (OB_ISNULL(tablet_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet loc is null", K(ret));
    }
  }
  else if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(clear_exprs_, eval_ctx_))) {
    LOG_WARN("fail to clear rowkey flag", K(ret), K(checker_ctdef_.part_id_dep_exprs_));
  } else if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(part_id_expr, eval_ctx_, partition_id, tablet_id))) {
    if (OB_NO_PARTITION_FOR_GIVEN_VALUE == ret) {
      //NOTE: no partition means no referenced value in parent table, change the ret_code to OB_ERR_NO_REFERENCED_ROW
      ret = OB_ERR_NO_REFERENCED_ROW;
      LOG_WARN("No referenced value in parent table and no partition for given value", K(ret));
    } else {
      LOG_WARN("fail to calc part id", K(ret), KPC(part_id_expr));
    }
  } else if (OB_FAIL(DAS_CTX(das_ref_.get_exec_ctx()).extended_tablet_loc(*table_loc_, tablet_id, tablet_loc))) {
    LOG_WARN("extended tablet loc failed", K(ret));
  }
  LOG_TRACE("tablet_id and partition id is", K(tablet_id), K(partition_id));
  return ret;
}

int ObForeignKeyChecker::get_das_scan_op(ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_scan_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!das_ref_.has_das_op(tablet_loc, das_scan_op))) {
    if (OB_FAIL(das_ref_.prepare_das_task(tablet_loc, das_scan_op))) {
      LOG_WARN("prepare das task failed", K(ret));
    } else {
      das_scan_op->set_scan_ctdef(&checker_ctdef_.das_scan_ctdef_);
      das_scan_op->set_scan_rtdef(&das_scan_rtdef_);
    }
  }
  return ret;
}

int ObForeignKeyChecker::init_foreign_key_checker(int64_t estimate_row,
                                                  const ObExprFrameInfo *expr_frame_info,
                                                  ObForeignKeyCheckerCtdef &fk_ctdef,
                                                  const ObExprPtrIArray &row,
                                                  common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = eval_ctx_.exec_ctx_.get_my_session();
  const ObDASTableLocMeta &loc_meta = fk_ctdef.loc_meta_;
  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = session->get_effective_tenant_id();
  mem_attr.label_ = "SqlFKeyCkr";
  das_ref_.set_expr_frame_info(expr_frame_info);
  das_ref_.set_mem_attr(mem_attr);
  das_ref_.set_execute_directly(false); // 确认这个参数设置的合理性
  if (OB_FAIL(DAS_CTX(das_ref_.get_exec_ctx()).extended_table_loc(loc_meta, table_loc_))) {
    LOG_WARN("failed to extend table_loc", K(ret), K(loc_meta));
  } else if (OB_ISNULL(table_loc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table location is null", K(ret));
  } else if (OB_FAIL(init_das_scan_rtdef())) {
    LOG_WARN("failed to init das scan rtdef for foreign key check", K(ret));
  } else if (OB_FAIL(ObDMLService::create_rowkey_check_hashset(estimate_row, &das_ref_.get_exec_ctx(), se_rowkey_dist_ctx_))) {
    LOG_WARN("failed to create hash set used for foreign key check", K(ret));
  } else if (OB_FAIL(ObSqlTransControl::set_fk_check_snapshot(das_ref_.get_exec_ctx()))) {
    LOG_WARN("failed to set snapshot for foreign key check", K(ret));
  } else if (OB_FAIL(init_clear_exprs(fk_ctdef, row))) {
    LOG_WARN("failed to init clear exprs", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator used to init foreign key checker is null", K(ret));
  } else {
    allocator_ = allocator;
    table_loc_->is_fk_check_ = true; //mark the table location with fk checking action
  }

  if (OB_SUCC(ret) && !fk_ctdef.is_part_table_) {
    if (OB_FAIL(DAS_CTX(das_ref_.get_exec_ctx()).extended_tablet_loc(*table_loc_, fk_ctdef.tablet_id_, local_tablet_loc_))) {
      LOG_WARN("failed to extended tablet loc for foreign key check", K(ret), K(fk_ctdef.tablet_id_));
    }
  }
  return ret;
}

int ObForeignKeyChecker::init_clear_exprs(ObForeignKeyCheckerCtdef &fk_ctdef, const ObExprPtrIArray &row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < fk_ctdef.part_id_dep_exprs_.count(); ++i) {
    ObExpr* expr =  fk_ctdef.part_id_dep_exprs_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition id calc expr is null", K(ret), K(i));
    } else if (!has_exist_in_array(row, expr)) {
      clear_exprs_.push_back(expr);
    }
  }
  return ret;
}

int ObForeignKeyChecker::init_das_scan_rtdef()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = eval_ctx_.exec_ctx_.get_physical_plan_ctx();
  ObSQLSessionInfo *my_session = eval_ctx_.exec_ctx_.get_my_session();
  ObTaskExecutorCtx &task_exec_ctx = eval_ctx_.exec_ctx_.get_task_exec_ctx();
  das_scan_rtdef_.timeout_ts_ = plan_ctx->get_ps_timeout_timestamp();
  das_scan_rtdef_.sql_mode_ = my_session->get_sql_mode();
  das_scan_rtdef_.stmt_allocator_.set_alloc(&das_ref_.get_das_alloc());
  das_scan_rtdef_.scan_allocator_.set_alloc(&das_ref_.get_das_alloc());
  ObQueryFlag query_flag(ObQueryFlag::Forward, // scan_order
                         false, // daily_merge
                         false, // optimize
                         false, // sys scan
                         false, // full_row
                         false, // index_back
                         false, // query_stat
                         lib::is_mysql_mode() ? ObQueryFlag::MysqlMode : ObQueryFlag::AnsiMode, // sql_mode
                         true // read_latest
                        );
  das_scan_rtdef_.scan_flag_.flag_ = query_flag.flag_;
  int64_t schema_version = task_exec_ctx.get_query_tenant_begin_schema_version();
  das_scan_rtdef_.tenant_schema_version_ = schema_version;
  das_scan_rtdef_.eval_ctx_ = &eval_ctx_;
  das_scan_rtdef_.is_for_foreign_check_ = true;
  das_scan_rtdef_.scan_flag_.set_for_foreign_key_check();
  // No Need to init pushdown op?
  if (OB_FAIL(das_scan_rtdef_.init_pd_op(eval_ctx_.exec_ctx_, checker_ctdef_.das_scan_ctdef_))) {
    LOG_WARN("init pushdown storage filter failed", K(ret));
  }
  return ret;
}

int ObForeignKeyChecker::build_table_range(const ObIArray<ObForeignKeyColumn> &columns,
                                           const ObExprPtrIArray &row,
                                           ObNewRange &lookup_range,
                                           bool &need_check)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = checker_ctdef_.rowkey_count_;
  int64_t fk_cnt = columns.count();
  bool need_shadow_columns = false;
  if (0 == rowkey_cnt || 0 == fk_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey count of foreign key count", K(ret), K(rowkey_cnt), K(fk_cnt));
  } else if (rowkey_cnt == fk_cnt) {
    ret = build_primary_table_range(columns, row, lookup_range, need_check);
  } else if (OB_FAIL(check_need_shadow_columns(columns, row, need_shadow_columns))) {
    LOG_WARN("failed to check need shadow columns", K(ret));
  } else {
    if (need_shadow_columns) {
      ret = build_index_table_range_need_shadow_column(columns, row, lookup_range, need_check);
    } else {
      ret = build_index_table_range(columns, row, lookup_range, need_check);
    }
  }
  return ret;
}

int ObForeignKeyChecker::check_fk_column_type(const ObObjMeta &col_obj_meta,
                                              const ObObjMeta &dst_obj_meta,
                                              const ObPrecision col_precision,
                                              const ObPrecision dst_precision,
                                              bool &need_extra_cast)
{
  int ret = OB_SUCCESS;
  need_extra_cast = false;
  if (col_obj_meta.get_type() != dst_obj_meta.get_type()) {
    if (lib::is_oracle_mode() && ob_is_number_tc(col_obj_meta.get_type()) && ob_is_number_tc(dst_obj_meta.get_type())) {
      // oracle mode, numberfloat and number type are same
    } else if ((ob_is_number_tc(col_obj_meta.get_type()) && ob_is_decimal_int_tc(dst_obj_meta.get_type())) ||
        (ob_is_number_tc(dst_obj_meta.get_type()) && ob_is_decimal_int_tc(col_obj_meta.get_type()))) {
      need_extra_cast = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type to perform foreign key check", K(col_obj_meta), K(dst_obj_meta));
    }
  } else if (ob_is_decimal_int_tc(col_obj_meta.get_type()) &&
      ObRawExprUtils::decimal_int_need_cast(col_precision, col_obj_meta.get_scale(),
                                            dst_precision, dst_obj_meta.get_scale())) {
    need_extra_cast = true;
  }
  return ret;
}

int ObForeignKeyChecker::build_primary_table_range(const ObIArray<ObForeignKeyColumn> &columns,
                                           const ObExprPtrIArray &row,
                                           ObNewRange &lookup_range,
                                           bool &need_check)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;
  int64_t rowkey_cnt = checker_ctdef_.rowkey_count_;
  int64_t fk_cnt = columns.count();
  // check parent table
  if (fk_cnt != checker_ctdef_.rowkey_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid foreign key column count", K(ret), K(fk_cnt), K(checker_ctdef_.rowkey_ids_.count()));
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new(buf) ObObj[rowkey_cnt];
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < fk_cnt; ++i) {
    ObObj tmp_obj;
    ObDatum *col_datum = nullptr;
    ObExpr *column_expr = row.at(columns.at(i).idx_);
    const ObObjMeta &col_obj_meta = column_expr->obj_meta_;
    const ObObjMeta &dst_obj_meta = columns.at(i).obj_meta_;
    const ObObjDatumMapType &obj_datum_map = column_expr->obj_datum_map_;
    int64_t rowkey_index = checker_ctdef_.rowkey_ids_.at(i);
    bool need_extra_cast = false;
    const ObPrecision dst_prec = dst_obj_meta.is_decimal_int() ?
        dst_obj_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
    ObObjMeta to_obj_meta = col_obj_meta;
    if (rowkey_index < 0 || rowkey_index >= rowkey_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid woekey index to build scan range", K(ret), K(rowkey_index));
    } else if (OB_FAIL(check_fk_column_type(col_obj_meta, dst_obj_meta,
        column_expr->datum_meta_.precision_, dst_prec,
        need_extra_cast))) {
      LOG_WARN("failed to perform foreign key column type check", K(ret), K(i));
    } else if (OB_FAIL(column_expr->eval(eval_ctx_, col_datum))) {
      LOG_WARN("evaluate expr failed", K(ret), K(i));
    } else if (!need_extra_cast && FALSE_IT(to_obj_meta = dst_obj_meta)) {
    } else if (OB_FAIL(col_datum->to_obj(tmp_obj, to_obj_meta, obj_datum_map))) {
      LOG_WARN("convert datum to obj failed", K(ret), K(i));
    } else if (need_extra_cast) {
      ObCastMode cm = CM_NONE | CM_CONST_TO_DECIMAL_INT_EQ;
      ObCastCtx cast_ctx(allocator_, NULL, cm, ObCharset::get_system_collation());
      ObAccuracy res_acc;
      if (ObDecimalIntType == dst_obj_meta.get_type()) {
        res_acc.set_precision(dst_obj_meta.get_stored_precision());
        res_acc.set_scale(dst_obj_meta.get_scale());
        cast_ctx.res_accuracy_ = &res_acc;
      }
      ObObj ori_obj = tmp_obj;
      if(OB_FAIL(ObObjCaster::to_type(dst_obj_meta.get_type(), cast_ctx, ori_obj, tmp_obj))) {
        LOG_WARN("fail to cast type", K(ret), K(col_obj_meta), K(dst_obj_meta));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_obj(*allocator_, tmp_obj, obj_ptr[rowkey_index]))) {// 这里需要做深拷贝
      LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
    }
  }

  if (OB_SUCC(ret)) {
    ObRowkey table_rowkey(obj_ptr, rowkey_cnt);
    // check if the foreign key has been checked before
    ret = se_rowkey_dist_ctx_->exist_refactored(table_rowkey);
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      need_check = false;
      LOG_TRACE("This foreign key has been checked before", K(table_rowkey));
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      need_check = true;
      uint64_t ref_table_id = checker_ctdef_.das_scan_ctdef_.ref_table_id_;
      if (OB_FAIL(lookup_range.build_range(ref_table_id, table_rowkey))) {
        LOG_WARN("build lookup range failed", K(ret), K(ref_table_id), K(table_rowkey));
      } else if (OB_FAIL(se_rowkey_dist_ctx_->set_refactored(table_rowkey))) {
        LOG_WARN("failed to add foreign key to cached hash_set", K(ret));
      }
    } else {
      LOG_WARN("check if foreign key item exists failed", K(ret), K(table_rowkey));
    }
  }
  return ret;
}

int ObForeignKeyChecker::build_index_table_range(const ObIArray<ObForeignKeyColumn> &columns,
                                           const ObExprPtrIArray &row,
                                           ObNewRange &lookup_range,
                                           bool &need_check)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;
  int64_t rowkey_cnt = checker_ctdef_.rowkey_count_;
  int64_t fk_cnt = columns.count();
  if (fk_cnt != checker_ctdef_.rowkey_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid foreign key column count", K(ret), K(fk_cnt), K(checker_ctdef_.rowkey_ids_.count()));
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new(buf) ObObj[rowkey_cnt];
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    if (i < fk_cnt) {
      ObObj tmp_obj;
      ObDatum *col_datum = nullptr;
      ObExpr *column_expr = row.at(columns.at(i).idx_);
      const ObObjMeta &col_obj_meta = column_expr->obj_meta_;
      const ObObjMeta &dst_obj_meta = columns.at(i).obj_meta_;
      const ObObjDatumMapType &obj_datum_map = column_expr->obj_datum_map_;
      int64_t rowkey_index = checker_ctdef_.rowkey_ids_.at(i);
      bool need_extra_cast = false;
      const ObPrecision dst_prec = dst_obj_meta.is_decimal_int() ?
        dst_obj_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
      ObObjMeta to_obj_meta = col_obj_meta;
      if (rowkey_index < 0 || rowkey_index >= fk_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid woekey index to build scan range", K(ret), K(rowkey_index));
      } else if (OB_FAIL(check_fk_column_type(col_obj_meta, dst_obj_meta,
          column_expr->datum_meta_.precision_, dst_prec,
          need_extra_cast))) {
        LOG_WARN("failed to perform foreign key column type check", K(ret), K(i));
      } else if (OB_FAIL(column_expr->eval(eval_ctx_, col_datum))) {
        LOG_WARN("evaluate expr failed", K(ret), K(i));
      } else if (!need_extra_cast && FALSE_IT(to_obj_meta = dst_obj_meta)) {
      } else if (OB_FAIL(col_datum->to_obj(tmp_obj, to_obj_meta, obj_datum_map))) {
        LOG_WARN("convert datum to obj failed", K(ret), K(i));
      } else if (need_extra_cast) {
        ObCastMode cm = CM_NONE | CM_CONST_TO_DECIMAL_INT_EQ;
        ObCastCtx cast_ctx(allocator_, NULL, cm, ObCharset::get_system_collation());
        ObAccuracy res_acc;
        if (ObDecimalIntType == dst_obj_meta.get_type()) {
          res_acc.set_precision(dst_obj_meta.get_stored_precision());
          res_acc.set_scale(dst_obj_meta.get_scale());
          cast_ctx.res_accuracy_ = &res_acc;
        }
        ObObj ori_obj = tmp_obj;
        if(OB_FAIL(ObObjCaster::to_type(dst_obj_meta.get_type(), cast_ctx, ori_obj, tmp_obj))) {
          LOG_WARN("fail to cast type", K(ret), K(col_obj_meta), K(dst_obj_meta));
        }
      }
      if (OB_FAIL(ret)) {
      // 这里需要做深拷贝
      } else if (OB_FAIL(ob_write_obj(*allocator_, tmp_obj, obj_ptr[rowkey_index]))) {
        LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
      }
    } else {
      // is parent key is unique key, the store key of the unique index is unique key + optional primary key
      // if unique key is not null, the column of optional primary key is null
      obj_ptr[i].set_null();
    }
  }

  if (OB_SUCC(ret)) {
    ObRowkey table_rowkey(obj_ptr, rowkey_cnt);
    // check if the foreign key has been checked before
    ret = se_rowkey_dist_ctx_->exist_refactored(table_rowkey); // check if the foreign key has been check before
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      need_check = false;
      LOG_TRACE("This foreign key has been checked before", K(table_rowkey));
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      need_check = true;
      uint64_t ref_table_id = checker_ctdef_.das_scan_ctdef_.ref_table_id_;
      if (OB_FAIL(lookup_range.build_range(ref_table_id, table_rowkey))) {
        LOG_WARN("build lookup range failed", K(ret), K(ref_table_id), K(table_rowkey));
      } else if (OB_FAIL(se_rowkey_dist_ctx_->set_refactored(table_rowkey))) { // add the foreign key that has not been checked before to the cached hash-set
        LOG_WARN("failed to add foreign key to cached hash_set", K(ret));
      }
    } else {
      LOG_WARN("check if foreign key item exists failed", K(ret), K(table_rowkey));
    }
  }
  return ret;
}

int ObForeignKeyChecker::build_index_table_range_need_shadow_column(const ObIArray<ObForeignKeyColumn> &columns,
                                                                    const ObExprPtrIArray &row,
                                                                    ObNewRange &lookup_range,
                                                                    bool &need_check)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr_start = nullptr;
  ObObj *obj_ptr_end = nullptr;
  void *buf_start = nullptr;
  void *buf_end = nullptr;
  int64_t rowkey_cnt = checker_ctdef_.rowkey_count_;
  int64_t fk_cnt = columns.count();
  if (fk_cnt != checker_ctdef_.rowkey_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid foreign key column count", K(ret), K(fk_cnt), K(checker_ctdef_.rowkey_ids_.count()));
  } else if (OB_ISNULL(buf_start = allocator_->alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else if (OB_ISNULL(buf_end = allocator_->alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr_start = new(buf_start) ObObj[rowkey_cnt];
    obj_ptr_end = new(buf_end) ObObj[rowkey_cnt];
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    if (i < fk_cnt) {
      ObObj tmp_obj;
      ObDatum *col_datum = nullptr;
      ObExpr *column_expr = row.at(columns.at(i).idx_);
      const ObObjMeta &col_obj_meta = column_expr->obj_meta_;
      const ObObjMeta &dst_obj_meta = columns.at(i).obj_meta_;
      const ObObjDatumMapType &obj_datum_map = column_expr->obj_datum_map_;
      int64_t rowkey_index = checker_ctdef_.rowkey_ids_.at(i);
      bool need_extra_cast = false;
      const ObPrecision dst_prec = dst_obj_meta.is_decimal_int() ?
        dst_obj_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
      ObObjMeta to_obj_meta = col_obj_meta;
      if (rowkey_index < 0 || rowkey_index >= fk_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid woekey index to build scan range", K(ret), K(rowkey_index));
      } else if (OB_FAIL(check_fk_column_type(col_obj_meta, dst_obj_meta,
          column_expr->datum_meta_.precision_, dst_prec,
          need_extra_cast))) {
        LOG_WARN("failed to perform foreign key column type check", K(ret), K(i));
      } else if (OB_FAIL(column_expr->eval(eval_ctx_, col_datum))) {
        LOG_WARN("evaluate expr failed", K(ret), K(i));
      } else if (!need_extra_cast && FALSE_IT(to_obj_meta = dst_obj_meta)) {
      } else if (OB_FAIL(col_datum->to_obj(tmp_obj, to_obj_meta, obj_datum_map))) {
        LOG_WARN("convert datum to obj failed", K(ret), K(i));
      } else if (need_extra_cast) {
        ObCastMode cm = CM_NONE | CM_CONST_TO_DECIMAL_INT_EQ;
        ObCastCtx cast_ctx(allocator_, NULL, cm, ObCharset::get_system_collation());
        ObAccuracy res_acc;
        if (ObDecimalIntType == dst_obj_meta.get_type()) {
          res_acc.set_precision(dst_obj_meta.get_stored_precision());
          res_acc.set_scale(dst_obj_meta.get_scale());
          cast_ctx.res_accuracy_ = &res_acc;
        }
        ObObj ori_obj = tmp_obj;
        if(OB_FAIL(ObObjCaster::to_type(dst_obj_meta.get_type(), cast_ctx, ori_obj, tmp_obj))) {
          LOG_WARN("fail to cast type", K(ret), K(col_obj_meta), K(dst_obj_meta));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ob_write_obj(*allocator_, tmp_obj, obj_ptr_start[rowkey_index]))) {
        LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
      } else if (OB_FAIL(ob_write_obj(*allocator_, tmp_obj, obj_ptr_end[rowkey_index]))) {
        LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
      }
    } else {
      obj_ptr_start[i].set_min_value();
      obj_ptr_end[i].set_max_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObRowkey start_key(obj_ptr_start, rowkey_cnt);
    ObRowkey end_key(obj_ptr_end, rowkey_cnt);
    lookup_range.start_key_ = start_key;
    lookup_range.end_key_ = end_key;
  }
  return ret;
}

int ObForeignKeyChecker::check_need_shadow_columns(const ObIArray<ObForeignKeyColumn> &columns,
                                                   const ObExprPtrIArray &row,
                                                   bool &need_shadow_columns)
{
  int ret = OB_SUCCESS;
  need_shadow_columns = false;
  if (lib::is_mysql_mode()) {
    bool rowkey_has_null = false;
    for (int64_t i = 0; OB_SUCC(ret) && !rowkey_has_null && i < columns.count(); ++i) {
      ObDatum *col_datum = nullptr;
      OZ(row.at(columns.at(i).idx_)->eval(eval_ctx_, col_datum));
      if (col_datum->is_null()) {
        rowkey_has_null = true;
      }
    }
    need_shadow_columns = rowkey_has_null;
  } else {
    bool is_rowkey_all_null = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_rowkey_all_null && i < columns.count(); ++i) {
      ObDatum *col_datum = nullptr;
      OZ(row.at(columns.at(i).idx_)->eval(eval_ctx_, col_datum));
      if (!col_datum->is_null()) {
        is_rowkey_all_null = false;
      }
    }
    need_shadow_columns = is_rowkey_all_null;
  }
  LOG_TRACE("need shadow columns", K(need_shadow_columns));
  return ret;
}

int ObForeignKeyChecker::check_fk_columns_has_null(const ObIArray<ObForeignKeyColumn> &columns,
                                                   const ObExprPtrIArray &row,
                                                   bool &is_all_null,
                                                   bool &has_null)
{
  int ret = OB_SUCCESS;
  is_all_null = true;
  has_null = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ObDatum *col_datum = nullptr;
    OZ(row.at(columns.at(i).idx_)->eval(eval_ctx_, col_datum));
    if (!col_datum->is_null()) {
      is_all_null = false;
    } else {
      has_null = true;
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase