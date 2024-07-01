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
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/das/ob_das_insert_op.h"
#include "sql/das/ob_das_delete_op.h"
#include "sql/das/ob_das_update_op.h"
#include "sql/das/ob_das_lock_op.h"
#include "sql/das/ob_das_def_reg.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/dml/ob_trigger_handler.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/dml/ob_err_log_service.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/tx/ob_trans_service.h"
#include "pl/ob_pl.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/dml/ob_fk_checker.h"
#include "storage/lob/ob_lob_manager.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
namespace sql
{

bool ObDMLService::check_cascaded_reference(const ObExpr *expr, const ObExprPtrIArray &row)
{
  bool bret = false;
  if (OB_ISNULL(expr) || expr->parent_cnt_ <= 0) {
    bret = false;
  } else {
    for (int i = 0; !bret && i < expr->parent_cnt_; ++i) {
      ObExpr *parent_expr = expr->parents_[i];
      if (parent_expr != nullptr) {
        if (parent_expr->type_ == T_FUN_COLUMN_CONV) {
          bret = has_exist_in_array(row, parent_expr);
        }
      }
      if (!bret) {
        bret = check_cascaded_reference(parent_expr, row);
      }
    }
  }
  return bret;
}

int ObDMLService::check_row_null(const ObExprPtrIArray &row,
                                 ObEvalCtx &eval_ctx,
                                 int64_t row_num,
                                 const ColContentIArray &column_infos,
                                 bool is_ignore,
                                 bool is_single_value,
                                 ObTableModifyOp &dml_op)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  CK(row.count() >= column_infos.count());
  if (OB_ISNULL(session = dml_op.get_exec_ctx().get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < column_infos.count(); i++) {
    ObDatum *datum = NULL;
    const bool is_nullable = column_infos.at(i).is_nullable_;
    uint64_t col_idx = column_infos.at(i).projector_index_;
    if (OB_FAIL(row.at(col_idx)->eval(eval_ctx, datum))) {
      common::ObString column_name = column_infos.at(i).column_name_;
      ret = ObDMLService::log_user_error_inner(ret, row_num, column_name, dml_op.get_exec_ctx());
    } else if (!is_nullable && datum->is_null()) {
      if (is_ignore ||
          (lib::is_mysql_mode() && !is_single_value && !is_strict_mode(session->get_sql_mode()))) {
        ObObj zero_obj;
        ObExprStrResAlloc res_alloc(*row.at(col_idx), eval_ctx);
        ObDatum &row_datum = row.at(col_idx)->locate_datum_for_write(eval_ctx);
        bool is_decimal_int = ob_is_decimal_int(row.at(col_idx)->datum_meta_.type_);
        if (is_oracle_mode()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dml with ignore not supported in oracle mode");
        } else if (ob_is_geometry(row.at(col_idx)->obj_meta_.get_type())) {
            ret = OB_BAD_NULL_ERROR;
            LOG_WARN("dml with ignore not supported in geometry type");
            LOG_USER_ERROR(OB_BAD_NULL_ERROR, column_infos.at(i).column_name_.length(), column_infos.at(i).column_name_.ptr());
        } else if (ob_is_roaringbitmap(row.at(col_idx)->obj_meta_.get_type())) {
            ret = OB_BAD_NULL_ERROR;
            LOG_WARN("dml with ignore not supported in roaringbitmap type");
            LOG_USER_ERROR(OB_BAD_NULL_ERROR, column_infos.at(i).column_name_.length(), column_infos.at(i).column_name_.ptr());
        } else if (check_cascaded_reference(row.at(col_idx), row)) {
          //This column is dependent on other columns and cannot be modified again;
          //otherwise, it will necessitate a cascading recalculation of the dependent expression results.
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("dml with ignore not supported with cascaded column", KPC(row.at(col_idx)));
          LOG_USER_ERROR(OB_BAD_NULL_ERROR, column_infos.at(i).column_name_.length(), column_infos.at(i).column_name_.ptr());
        } else if (OB_FAIL(ObObjCaster::get_zero_value(
            row.at(col_idx)->obj_meta_.get_type(),
            row.at(col_idx)->obj_meta_.get_collation_type(),
            zero_obj))) {
          LOG_WARN("get column default zero value failed", K(ret), K(column_infos.at(i)), K(row.at(col_idx)->max_length_));
        } else if (is_decimal_int) {
          ObDecimalIntBuilder dec_val;
          dec_val.set_zero(wide::ObDecimalIntConstValue::get_int_bytes_by_precision(
            row.at(col_idx)->datum_meta_.precision_));
          row_datum.set_decimal_int(dec_val.get_decimal_int(), dec_val.get_int_bytes());
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("get column default zero value failed", K(ret), K(column_infos.at(i)));
        } else if (OB_FAIL(ObDASUtils::padding_fixed_string_value(row.at(col_idx)->max_length_,
                                                                  res_alloc,
                                                                  zero_obj))) {
          LOG_WARN("padding fixed string value failed", K(ret));
        } else if (!is_decimal_int && OB_FAIL(row_datum.from_obj(zero_obj))) {
          LOG_WARN("assign zero obj to datum failed", K(ret), K(zero_obj));
        } else if (zero_obj.is_lob_storage() && zero_obj.has_lob_header() != row.at(col_idx)->obj_meta_.has_lob_header()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("has lob header mark is wrong", K(ret), K(i), K(col_idx),
            K(zero_obj.get_meta()), K(row.at(col_idx)->obj_meta_));
        } else {
          //output warning msg
          const ObString &column_name = column_infos.at(i).column_name_;
          LOG_USER_WARN(OB_BAD_NULL_ERROR, column_name.length(), column_name.ptr());
        }
      } else {
        //output warning msg
        const ObString &column_name = column_infos.at(i).column_name_;
        ret = OB_BAD_NULL_ERROR;
        LOG_USER_ERROR(OB_BAD_NULL_ERROR, column_name.length(), column_name.ptr());
      }
    }
  }
  return ret;
}

int ObDMLService::check_column_type(const ExprFixedArray &dml_row,
                                    int64_t row_num,
                                    const ObIArray<ColumnContent> &column_infos,
                                    ObTableModifyOp &dml_op)
{
  int ret = OB_SUCCESS;
  CK(dml_row.count() >= column_infos.count());
  ObArenaAllocator tmp_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObUserLoggingCtx::Guard logging_ctx_guard(*dml_op.get_exec_ctx().get_user_logging_ctx());
  dml_op.get_exec_ctx().set_cur_rownum(row_num);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_infos.count(); ++i) {
    const ColumnContent &column_info = column_infos.at(i);
    common::ObString column_name = column_infos.at(i).column_name_;
    dml_op.get_exec_ctx().set_cur_column_name(&column_name);
    ObExpr *expr = dml_row.at(column_info.projector_index_);
    ObDatum *datum = nullptr;
    if (OB_FAIL(expr->eval(dml_op.get_eval_ctx(), datum))) {
      ret = ObDMLService::log_user_error_inner(ret, row_num, column_name, dml_op.get_exec_ctx());
    } else if (!datum->is_null() && expr->obj_meta_.is_geometry()) {
      // geo column type
      const uint32_t column_srid = column_info.srs_info_.srid_;
      const ObGeoType column_geo_type = static_cast<ObGeoType>(column_info.srs_info_.geo_type_);
      ObString wkb = datum->get_string();
      uint32_t input_srid = UINT32_MAX;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
          expr->datum_meta_, expr->obj_meta_.has_lob_header(), wkb))) {
        LOG_WARN("fail to get real string data", K(ret), K(wkb));
      } else if (OB_FAIL(ObGeoTypeUtil::check_geo_type(column_geo_type, wkb))) {
        LOG_WARN("check geo type failed", K(ret), K(wkb));
        ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
        LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
      } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(wkb, input_srid))) {
        LOG_WARN("get srid by wkb failed", K(ret), K(wkb));
      } else if (OB_FAIL(ObSqlGeoUtils::check_srid(column_srid, input_srid))) {
        ret = OB_ERR_WRONG_SRID_FOR_COLUMN;
        LOG_USER_ERROR(OB_ERR_WRONG_SRID_FOR_COLUMN, static_cast<uint64_t>(input_srid),
            static_cast<uint64_t>(column_srid));
      }
    }
  }
  return ret;
}

int ObDMLService::check_rowkey_is_null(const ObExprPtrIArray &row,
                                       int64_t rowkey_cnt,
                                       ObEvalCtx &eval_ctx,
                                       bool &is_null)
{
  int ret = OB_SUCCESS;
  is_null = false;
  CK(row.count() >= rowkey_cnt);
  ObDatum *datum = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !is_null && i < rowkey_cnt; ++i) {
    if (OB_FAIL(row.at(i)->eval(eval_ctx, datum))) {
      LOG_WARN("eval expr failed", K(ret), K(i));
    } else {
      is_null = datum->is_null();
    }
  }
  return ret;
}

int ObDMLService::check_rowkey_whether_distinct(const ObExprPtrIArray &row,
                                                DistinctType distinct_algo,
                                                ObEvalCtx &eval_ctx,
                                                ObExecContext &root_ctx,
                                                ObRowkey &tmp_table_rowkey,
                                                SeRowkeyDistCtx *rowkey_dist_ctx,
                                                bool &is_dist)
{
  int ret = OB_SUCCESS;
  is_dist = true;
  if (T_DISTINCT_NONE != distinct_algo) {
    if (T_HASH_DISTINCT == distinct_algo) {
      ObIAllocator &allocator = root_ctx.get_allocator();
      const int64_t rowkey_cnt = tmp_table_rowkey.get_obj_cnt();
      if (OB_ISNULL(rowkey_dist_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("distinct check hash set is null", K(ret));
      } else {
        //step1: Init ObObj of ObTableRowkey
        ObObj *tmp_obj_ptr = tmp_table_rowkey.get_obj_ptr();
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
          ObExpr *expr = row.at(i);
          ObDatum *col_datum = nullptr;
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr in rowkey is nullptr", K(ret), K(i));
          } else if (OB_FAIL(expr->eval(eval_ctx, col_datum))) {
            LOG_WARN("failed to evaluate expr in rowkey", K(ret), K(i));
          } else if (OB_ISNULL(col_datum)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("evaluated column datum in rowkey is nullptr", K(ret), K(i));
          } else if (OB_FAIL(col_datum->to_obj(tmp_obj_ptr[i], expr->obj_meta_, expr->obj_datum_map_))) {
            LOG_WARN("convert datum to obj failed", K(ret), K(i));
          }
        }

        //step2: Perform distinct check use ObRowkey
        {
          ret = rowkey_dist_ctx->exist_refactored(tmp_table_rowkey);
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
            is_dist = false;
          } else if (OB_HASH_NOT_EXIST == ret) {
            //step3: if not exist, deep copy data and add ObRowkey to hash set
            //step3.1: Init the buffer of ObObj Array
            ret = OB_SUCCESS;
            ObObj *obj_ptr = nullptr;
            void *buf = nullptr;
            if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
            } else {
              obj_ptr = new(buf) ObObj[rowkey_cnt];
            }

            //step3.2: deep copy data to ObObj
            for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
              if (OB_FAIL(ob_write_obj(allocator, tmp_obj_ptr[i], obj_ptr[i]))) {
                LOG_WARN("deep copy rowkey value failed", K(ret), K(obj_ptr[i]));
              }
            }
            ObRowkey table_rowkey(obj_ptr, rowkey_cnt);
            //step3.3: add ObRowkey to hash set
            if (OB_SUCC(ret) && OB_FAIL(rowkey_dist_ctx->set_refactored(table_rowkey))) {
              LOG_WARN("set rowkey item failed", K(ret));
            }
          } else {
            LOG_WARN("check if rowkey item exists failed", K(ret));
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Check Update/Delete row with merge distinct");
    }
  }
  return ret;
}

int ObDMLService::create_rowkey_check_hashset(int64_t estimate_row,
                                           ObExecContext *root_ctx,
                                           SeRowkeyDistCtx *&rowkey_dist_ctx)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = root_ctx->get_allocator();
  if (OB_ISNULL(rowkey_dist_ctx)) {
    //create rowkey distinct context
    void *buf = allocator.alloc(sizeof(SeRowkeyDistCtx));
    ObSQLSessionInfo *my_session = root_ctx->get_my_session();
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), "size", sizeof(SeRowkeyDistCtx));
    } else if (OB_ISNULL(my_session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("my session is null", K(ret));
    } else {
      rowkey_dist_ctx = new (buf) SeRowkeyDistCtx();
      int64_t match_rows = estimate_row > ObDMLBaseCtDef::MIN_ROWKEY_DISTINCT_BUCKET_NUM ?
                            estimate_row : ObDMLBaseCtDef::MIN_ROWKEY_DISTINCT_BUCKET_NUM;
      //
      // match_rows是优化器估行的结果，如果这个值很大，
      // 直接创建有这么多bucket的hashmap会申请
      // 不到内存，这里做了限制为64k，防止报内存不足的错误
      const int64_t max_bucket_num = match_rows > ObDMLBaseCtDef::MAX_ROWKEY_DISTINCT_BUCKET_NUM ?
                              ObDMLBaseCtDef::MAX_ROWKEY_DISTINCT_BUCKET_NUM : match_rows;
      if (OB_FAIL(rowkey_dist_ctx->create(max_bucket_num,
                                      ObModIds::OB_DML_CHECK_ROWKEY_DISTINCT_BUCKET,
                                      ObModIds::OB_DML_CHECK_ROWKEY_DISTINCT_NODE,
                                      my_session->get_effective_tenant_id()))) {
        LOG_WARN("create rowkey distinct context failed", K(ret), "rows", estimate_row);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Create hash set on a pointer that is not null", K(ret));
  }
  return ret;
}

int ObDMLService::check_lob_column_changed(ObEvalCtx &eval_ctx,
            const ObExpr& old_expr, ObDatum& old_datum,
            const ObExpr& new_expr, ObDatum& new_datum,
            int64_t& result) {
  INIT_SUCC(ret);
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  int64_t timeout = 0;
  int64_t query_st = eval_ctx.exec_ctx_.get_my_session()->get_query_start_time();
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get lob manager handle null.", K(ret));
  } else if (OB_FAIL(eval_ctx.exec_ctx_.get_my_session()->get_query_timeout(timeout))) {
    LOG_WARN("failed to get session query timeout", K(ret));
  } else {
    timeout += query_st;
    ObString old_str = old_datum.get_string();
    ObString new_str = new_datum.get_string();
    bool old_set_has_lob_header = old_expr.obj_meta_.has_lob_header() && old_str.length() > 0;
    bool new_set_has_lob_header = new_expr.obj_meta_.has_lob_header() && new_str.length() > 0;
    ObLobLocatorV2 old_lob(old_str, old_set_has_lob_header);
    ObLobLocatorV2 new_lob(new_str, new_set_has_lob_header);
    if (old_set_has_lob_header && new_set_has_lob_header) {
      bool is_equal = false;
      ObLobCompareParams cmp_params;
      // binary compare ignore charset
      cmp_params.collation_left_ = CS_TYPE_BINARY;
      cmp_params.collation_right_ = CS_TYPE_BINARY;
      cmp_params.offset_left_ = 0;
      cmp_params.offset_right_ = 0;
      cmp_params.compare_len_ = UINT64_MAX;
      cmp_params.timeout_ = timeout;
      cmp_params.tx_desc_ = eval_ctx.exec_ctx_.get_my_session()->get_tx_desc();
      if (old_lob.is_persist_lob() && new_lob.is_delta_temp_lob()) {
        if (OB_FAIL(ObDeltaLob::has_diff(new_lob, result))) {
          LOG_WARN("delata lob has_diff fail", K(ret), K(old_lob), K(new_lob));
        }
      } else if(OB_FAIL(lob_mngr->equal(old_lob, new_lob, cmp_params, is_equal))) {
        LOG_WARN("fail to compare lob", K(ret), K(old_lob), K(new_lob));
      } else {
        result = is_equal ? 0 : 1;
      }
    } else {
      result = ObDatum::binary_equal(old_datum, new_datum) ? 0 : 1;
    }
  }
  return ret;
}

int ObDMLService::check_row_whether_changed(const ObUpdCtDef &upd_ctdef,
                                            ObUpdRtDef &upd_rtdef,
                                            ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, update_start_check_row);
  if (!upd_ctdef.is_primary_index_) {
    // if in pdml, upd_rtdef.primary_rtdef_ is null
    // in normal dml stmt, upd_rtdef.primary_rtdef_ is not null
    upd_rtdef.is_row_changed_ = false;
    if (OB_NOT_NULL(upd_rtdef.primary_rtdef_)) {
      upd_rtdef.is_row_changed_ = upd_rtdef.primary_rtdef_->is_row_changed_;
    } else if (lib::is_mysql_mode()) {
      // whether the global index row is updated is subject to the result of the primary index
      // pdml e.g.:
      // create table t22(a int primary key,
      //                 b int,
      //                 c int,
      //                 d timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      // ) partition by hash(a) partitions 5;
      // create index idx on t22(c, d) global;
      // insert into t22 values(1, 1, 1, now());
      // set _force_parallel_dml_dop = 3;
      // update t22 set b=2, c=1 where a=1;
      // this case, we must update data_table and index_table
      const ObExprPtrIArray &old_row = upd_ctdef.old_row_;
      const ObExprPtrIArray &new_row = upd_ctdef.new_row_;
      FOREACH_CNT_X(info, upd_ctdef.assign_columns_, OB_SUCC(ret) && !upd_rtdef.is_row_changed_) {
        const uint64_t idx = info->projector_index_;
        ObDatum *old_datum = NULL;
        ObDatum *new_datum = NULL;
        if (OB_FAIL(old_row.at(idx)->eval(eval_ctx, old_datum))
            || OB_FAIL(new_row.at(idx)->eval(eval_ctx, new_datum))) {
          LOG_WARN("evaluate value failed", K(ret));
        } else {
          if(is_lob_storage(old_row.at(idx)->datum_meta_.type_)
              && is_lob_storage(new_row.at(idx)->datum_meta_.type_))
          {
            int64_t cmp_res = 0;
            if(OB_FAIL(check_lob_column_changed(eval_ctx, *old_row.at(idx), *old_datum, *new_row.at(idx), *new_datum, cmp_res))) {
              LOG_WARN("compare lob datum failed", K(ret));
            } else {
              upd_rtdef.is_row_changed_ = (cmp_res != 0);
            }
          } else {
            upd_rtdef.is_row_changed_ = !ObDatum::binary_equal(*old_datum, *new_datum);
          }
        }
      }
    } else {
      //in oracle mode, no matter whether the updated row is changed or not,
      //the row will be updated in the storage
      upd_rtdef.is_row_changed_ = true;
    }
  } else if (lib::is_mysql_mode()) {
    upd_rtdef.is_row_changed_ = false;
    const ObExprPtrIArray &old_row = upd_ctdef.old_row_;
    const ObExprPtrIArray &new_row = upd_ctdef.new_row_;
    FOREACH_CNT_X(info, upd_ctdef.assign_columns_, OB_SUCC(ret) && !upd_rtdef.is_row_changed_) {
      const uint64_t idx = info->projector_index_;
      if (OB_LIKELY(!info->auto_filled_timestamp_)) {
        ObDatum *old_datum = NULL;
        ObDatum *new_datum = NULL;
        if (OB_FAIL(old_row.at(idx)->eval(eval_ctx, old_datum))
            || OB_FAIL(new_row.at(idx)->eval(eval_ctx, new_datum))) {
          LOG_WARN("evaluate value failed", K(ret));
        } else {
          if(is_lob_storage(old_row.at(idx)->datum_meta_.type_)
              && is_lob_storage(new_row.at(idx)->datum_meta_.type_))
          {
            int64_t cmp_res = 0;
            if(OB_FAIL(check_lob_column_changed(eval_ctx, *old_row.at(idx), *old_datum, *new_row.at(idx), *new_datum, cmp_res))) {
              LOG_WARN("compare lob datum failed", K(ret));
            } else {
              upd_rtdef.is_row_changed_ = (cmp_res != 0);
            }
          } else {
            upd_rtdef.is_row_changed_ = !ObDatum::binary_equal(*old_datum, *new_datum);
          }
        }
      }
    }
  } else {
    //in oracle mode, no matter whether the updated row is changed or not,
    //the row will be updated in the storage
    upd_rtdef.is_row_changed_ = true;
  }
  if (OB_SUCC(ret) &&
      upd_rtdef.is_row_changed_ &&
      upd_ctdef.is_primary_index_ &&
      upd_ctdef.dupd_ctdef_.is_batch_stmt_) {
    //check predicate column whether changed in batch stmt execution
    const ObExprPtrIArray &old_row = upd_ctdef.old_row_;
    const ObExprPtrIArray &new_row = upd_ctdef.new_row_;
    for (int64_t i = 0; OB_SUCC(ret) && i < upd_ctdef.assign_columns_.count(); ++i) {
      const ColumnContent &info = upd_ctdef.assign_columns_.at(i);
      uint64_t idx = info.projector_index_;
      ObDatum *old_datum = NULL;
      ObDatum *new_datum = NULL;
      if (info.is_predicate_column_) {
        if (OB_FAIL(old_row.at(idx)->eval(eval_ctx, old_datum))
            || OB_FAIL(new_row.at(idx)->eval(eval_ctx, new_datum))) {
          LOG_WARN("evaluate value failed", K(ret));
        } else if (!ObDatum::binary_equal(*old_datum, *new_datum)) {
          //update the predicate column, will lead to the next stmt result change, need rollback
          ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
          LOG_TRACE("batch stmt update the predicate column, need to rollback", K(ret),
                    K(info), KPC(old_datum), KPC(new_datum));
        }
      }
    }
  }
  LOG_DEBUG("after check update row changed", K(ret), K(upd_rtdef.is_row_changed_),
              "old_row", ROWEXPR2STR(eval_ctx, upd_ctdef.old_row_),
              "new_row", ROWEXPR2STR(eval_ctx, upd_ctdef.new_row_));
  NG_TRACE_TIMES(2, update_end_check_row);
  return ret;
}

int ObDMLService::filter_row_for_check_cst(const ExprFixedArray &cst_exprs,
                                           ObEvalCtx &eval_ctx,
                                           bool &filtered)
{
  int ret = OB_SUCCESS;
  filtered = false;
  for (int64_t i = 0; OB_SUCC(ret) && !filtered && i < cst_exprs.count(); ++i) {
    ObExpr *expr = cst_exprs.at(i);
    ObDatum *datum = nullptr;
    if (OB_FAIL(expr->eval(eval_ctx, datum))) {
      if (is_mysql_mode()) {
        LOG_INFO("cover original errno while calulating check expr in mysql mode", K(ret));
        filtered = true;
        ret = OB_SUCCESS;
      } else { // oracle mode
        LOG_WARN("eval check constraint expr failed", K(ret));
      }
    } else {
      OB_ASSERT(ob_is_int_tc(expr->datum_meta_.type_));
      if (!datum->is_null() && 0 == datum->get_int()) {
        filtered = true;
      }
    }
  }
  return ret;
}

int ObDMLService::filter_row_for_view_check(const ExprFixedArray &cst_exprs,
                                           ObEvalCtx &eval_ctx,
                                           bool &filtered)
{
  int ret = OB_SUCCESS;
  filtered = false;
  for (int64_t i = 0; OB_SUCC(ret) && !filtered && i < cst_exprs.count(); ++i) {
    ObExpr *expr = cst_exprs.at(i);
    ObDatum *datum = nullptr;
    if (OB_FAIL(expr->eval(eval_ctx, datum))) {
      LOG_WARN("eval check constraint expr failed", K(ret));
    } else {
      OB_ASSERT(ob_is_int_tc(expr->datum_meta_.type_));
      if (datum->is_null() || 0 == datum->get_int()) {
        filtered = true;
      }
    }
  }
  return ret;
}

int ObDMLService::process_before_stmt_trigger(const ObDMLBaseCtDef &dml_ctdef,
                                              ObDMLBaseRtDef &dml_rtdef,
                                              ObDMLRtCtx &dml_rtctx,
                                              const ObDmlEventType &dml_event)
{
  int ret = OB_SUCCESS;
  dml_rtctx.get_exec_ctx().set_dml_event(dml_event);
  if (dml_ctdef.is_primary_index_ && !dml_ctdef.trig_ctdef_.tg_args_.empty()) {
    if (!dml_rtctx.op_.get_spec().use_dist_das()
        || dml_rtctx.get_exec_ctx().get_my_session()->is_remote_session()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Do before stmt trigger without DAS");
      LOG_WARN("Do before stmt trigger without DAS not supported", K(ret),
               K(dml_rtctx.op_.get_spec().use_dist_das()),
               K(dml_rtctx.get_exec_ctx().get_my_session()->is_remote_session()));
    } else if (OB_FAIL(TriggerHandle::do_handle_before_stmt(dml_rtctx.op_,
                                                            dml_ctdef.trig_ctdef_,
                                                            dml_rtdef.trig_rtdef_,
                                                            dml_event))) {
      LOG_WARN("failed to handle before stmt trigger", K(ret));
    } else if (OB_FAIL(ObSqlTransControl::stmt_refresh_snapshot(dml_rtctx.get_exec_ctx()))) {
      LOG_WARN("failed to get new snapshot after before stmt trigger evaluated", K(ret));
    }
  }
  return ret;
}

int ObDMLService::process_after_stmt_trigger(const ObDMLBaseCtDef &dml_ctdef,
                                             ObDMLBaseRtDef &dml_rtdef,
                                             ObDMLRtCtx &dml_rtctx,
                                             const ObDmlEventType &dml_event)
{
  int ret = OB_SUCCESS;
  dml_rtctx.get_exec_ctx().set_dml_event(dml_event);
  if (dml_ctdef.is_primary_index_ && !dml_ctdef.trig_ctdef_.tg_args_.empty()) {
    if (!dml_rtctx.op_.get_spec().use_dist_das()
        || dml_rtctx.get_exec_ctx().get_my_session()->is_remote_session()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Do after stmt trigger without DAS");
      LOG_WARN("Do after stmt trigger without DAS not supported", K(ret),
               K(dml_rtctx.op_.get_spec().use_dist_das()),
               K(dml_rtctx.get_exec_ctx().get_my_session()->is_remote_session()));
    } else if (OB_FAIL(TriggerHandle::do_handle_after_stmt(dml_rtctx.op_,
                                                           dml_ctdef.trig_ctdef_,
                                                           dml_rtdef.trig_rtdef_,
                                                           dml_event))) {
      LOG_WARN("failed to handle after stmt trigger", K(ret));
    }
  }
  return ret;
}

int ObDMLService::init_heap_table_pk_for_ins(const ObInsCtDef &ins_ctdef, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (ins_ctdef.is_primary_index_ && ins_ctdef.is_heap_table_ && !ins_ctdef.has_instead_of_trigger_) {
    ObExpr *auto_inc_expr = ins_ctdef.new_row_.at(0);
    if (OB_ISNULL(auto_inc_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (auto_inc_expr->type_ != T_TABLET_AUTOINC_NEXTVAL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("auto_inc_expr type unexpected", K(ret), KPC(auto_inc_expr));
    } else {
      ObDatum &datum = auto_inc_expr->locate_datum_for_write(eval_ctx);
      datum.set_null();
      auto_inc_expr->get_eval_info(eval_ctx).evaluated_ = true;
    }
  }
  return ret;
}

int ObDMLService::process_insert_row(const ObInsCtDef &ins_ctdef,
                                     ObInsRtDef &ins_rtdef,
                                     ObTableModifyOp &dml_op,
                                     bool &is_skipped)
{
  int ret = OB_SUCCESS;
  if (ins_ctdef.is_primary_index_) {
    bool is_filtered = false;
    is_skipped = false;
    ObEvalCtx &eval_ctx = dml_op.get_eval_ctx();
    uint64_t ref_table_id = ins_ctdef.das_base_ctdef_.index_tid_;
    ObSQLSessionInfo *my_session = NULL;
    bool has_instead_of_trg = ins_ctdef.has_instead_of_trigger_;
    //first, check insert value whether matched column type
    if (OB_FAIL(check_column_type(ins_ctdef.new_row_,
                                  ins_rtdef.cur_row_num_,
                                  ins_ctdef.column_infos_,
                                  dml_op))) {
      LOG_WARN("check column type failed", K(ret));
    } else if (OB_ISNULL(my_session = dml_op.get_exec_ctx().get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(check_nested_sql_legality(dml_op.get_exec_ctx(), ins_ctdef.das_ctdef_.index_tid_))) {
      LOG_WARN("failed to check stmt table", K(ret), K(ref_table_id));
    } else if (OB_FAIL(TriggerHandle::init_param_new_row(
        eval_ctx, ins_ctdef.trig_ctdef_, ins_rtdef.trig_rtdef_))) {
      LOG_WARN("failed to handle before trigger", K(ret));
    } else if (OB_FAIL(TriggerHandle::do_handle_before_row(
        dml_op, ins_ctdef.das_base_ctdef_, ins_ctdef.trig_ctdef_, ins_rtdef.trig_rtdef_))) {
      LOG_WARN("failed to handle before trigger", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (has_instead_of_trg) {
      is_skipped = true;
    } else if (OB_FAIL(check_row_null(ins_ctdef.new_row_,
                                      dml_op.get_eval_ctx(),
                                      ins_rtdef.cur_row_num_,
                                      ins_ctdef.column_infos_,
                                      ins_ctdef.das_ctdef_.is_ignore_,
                                      ins_ctdef.is_single_value_,
                                      dml_op))) {
      LOG_WARN("check row null failed", K(ret));
    } else if (OB_FAIL(filter_row_for_view_check(ins_ctdef.view_check_exprs_,
                                                 eval_ctx, is_filtered))) {
      //check column constraint expr
      LOG_WARN("filter row for check cst failed", K(ret));
    } else if (OB_UNLIKELY(is_filtered)) {
      ret = OB_ERR_CHECK_OPTION_VIOLATED;
      LOG_WARN("view check option violated", K(ret));
    } else if (OB_FAIL(filter_row_for_check_cst(ins_ctdef.check_cst_exprs_,
                                                eval_ctx, is_filtered))) {
      //check column constraint expr
      LOG_WARN("filter row for check cst failed", K(ret));
    } else if (OB_UNLIKELY(is_filtered)) {
      if (is_mysql_mode() && ins_ctdef.das_ctdef_.is_ignore_) {
        is_skipped = true;
        LOG_USER_WARN(OB_ERR_CHECK_CONSTRAINT_VIOLATED);
        LOG_WARN("check constraint violated, skip this row", K(ret));
      } else {
        ret = OB_ERR_CHECK_CONSTRAINT_VIOLATED;
        LOG_WARN("column constraint check failed", K(ret));
      }
    }

    if (OB_FAIL(ret) && dml_op.is_error_logging_ && should_catch_err(ret) && !has_instead_of_trg) {
      dml_op.err_log_rt_def_.first_err_ret_ = ret;
      // cover the err_ret  by design
      ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < ins_ctdef.new_row_.count(); ++i) {
        ObExpr *expr = ins_ctdef.new_row_.at(i);
        ObDatum *datum = nullptr;
        if (OB_FAIL(expr->eval(dml_op.get_eval_ctx(), datum))) {
          if (should_catch_err(ret) && !(IS_CONST_TYPE(expr->type_))) {
            expr->locate_datum_for_write(dml_op.get_eval_ctx()).set_null();
            expr->set_evaluated_flag(dml_op.get_eval_ctx());
            ret = OB_SUCCESS;
          }
        }
      }
    }
  }
  ret = (ret == OB_SUCCESS ? dml_op.err_log_rt_def_.first_err_ret_ : ret);
  // If any error occurred before, the error code here is not OB_SUCCESS;
  return ret;
}

int ObDMLService::process_lock_row(const ObLockCtDef &lock_ctdef,
                                   ObLockRtDef &lock_rtdef,
                                   bool &is_skipped,
                                   ObTableModifyOp &dml_op)
{
  int ret = OB_SUCCESS;
  is_skipped = false;
  bool is_null = false;
  UNUSED(dml_op);
  if (lock_ctdef.need_check_filter_null_ &&
      OB_FAIL(check_rowkey_is_null(lock_ctdef.old_row_,
                                   lock_ctdef.das_ctdef_.rowkey_cnt_,
                                   dml_op.get_eval_ctx(),
                                   is_null))) {
    LOG_WARN("failed to check rowkey is null", K(ret));
  } else if (is_null) {
    // no need to lock
    is_skipped = true;
  }
  return ret;
}

int ObDMLService::process_delete_row(const ObDelCtDef &del_ctdef,
                                     ObDelRtDef &del_rtdef,
                                     bool &is_skipped,
                                     ObTableModifyOp &dml_op)
{
  int ret = OB_SUCCESS;
  is_skipped = false;
  if (del_ctdef.is_primary_index_) {
    uint64_t ref_table_id = del_ctdef.das_base_ctdef_.index_tid_;
    ObSQLSessionInfo *my_session = NULL;
    bool has_instead_of_trg = del_ctdef.has_instead_of_trigger_;
    if (OB_ISNULL(my_session = dml_op.get_exec_ctx().get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(check_nested_sql_legality(dml_op.get_exec_ctx(), del_ctdef.das_ctdef_.index_tid_))) {
      LOG_WARN("failed to check stmt table", K(ret), K(ref_table_id));
    }
    if (OB_SUCC(ret) && del_ctdef.need_check_filter_null_ && !has_instead_of_trg) {
      bool is_null = false;
      if (OB_FAIL(check_rowkey_is_null(del_ctdef.old_row_,
                                       del_ctdef.das_ctdef_.rowkey_cnt_,
                                       dml_op.get_eval_ctx(),
                                       is_null))) {
        LOG_WARN("check rowkey is null failed", K(ret), K(del_ctdef), K(del_rtdef));
      } else if (is_null) {
        is_skipped = true;
      }
    }

    if (OB_SUCC(ret) && !is_skipped && OB_NOT_NULL(del_rtdef.se_rowkey_dist_ctx_) && !has_instead_of_trg) {
      bool is_distinct = false;
      ObExecContext *root_ctx = nullptr;
      if (OB_FAIL(get_exec_ctx_for_duplicate_rowkey_check(&dml_op.get_exec_ctx(), root_ctx))) {
        LOG_WARN("get root ExecContext failed", K(ret));
      } else if (OB_ISNULL(root_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the root ctx of foreign key nested session is null", K(ret));
      } else if (OB_FAIL(check_rowkey_whether_distinct(del_ctdef.distinct_key_,
                                                      T_HASH_DISTINCT,
                                                      dml_op.get_eval_ctx(),
                                                      *root_ctx,
                                                      del_rtdef.table_rowkey_,
                                                      del_rtdef.se_rowkey_dist_ctx_,
                                                      is_distinct))) {
        LOG_WARN("check rowkey whether distinct failed", K(ret),
                  K(del_ctdef), K(del_rtdef), K(dml_op.get_spec().rows_));
      } else if (!is_distinct) {
        is_skipped = true;
      }
    }

    if (OB_SUCC(ret) && !is_skipped) {
      if (OB_FAIL(TriggerHandle::init_param_old_row(
        dml_op.get_eval_ctx(), del_ctdef.trig_ctdef_, del_rtdef.trig_rtdef_))) {
        LOG_WARN("failed to handle before trigger", K(ret));
      } else if (OB_FAIL(TriggerHandle::do_handle_before_row(
          dml_op, del_ctdef.das_base_ctdef_, del_ctdef.trig_ctdef_, del_rtdef.trig_rtdef_))) {
        LOG_WARN("failed to handle before trigger", K(ret));
      } else if (has_instead_of_trg) {
        is_skipped = true;
      }
    }
    // here only catch foreign key execption
    if (OB_FAIL(ret) && dml_op.is_error_logging_ && should_catch_err(ret) && !has_instead_of_trg) {
      dml_op.err_log_rt_def_.first_err_ret_ = ret;
    }

    LOG_DEBUG("process delete row", K(ret), K(is_skipped),
               "old_row", ROWEXPR2STR(dml_op.get_eval_ctx(), del_ctdef.old_row_));
  }

  return ret;
}

int ObDMLService::process_update_row(const ObUpdCtDef &upd_ctdef,
                                     ObUpdRtDef &upd_rtdef,
                                     bool &is_skipped,
                                     ObTableModifyOp &dml_op)
{
  int ret = OB_SUCCESS;
  is_skipped = false;
  bool has_instead_of_trg = upd_ctdef.has_instead_of_trigger_;
  if (upd_ctdef.is_primary_index_) {
    uint64_t ref_table_id = upd_ctdef.das_base_ctdef_.index_tid_;
    ObSQLSessionInfo *my_session = NULL;
    if (OB_SUCC(ret) && upd_ctdef.need_check_filter_null_ && !has_instead_of_trg) {
      bool is_null = false;
      if (OB_FAIL(check_rowkey_is_null(upd_ctdef.old_row_,
                                       upd_ctdef.dupd_ctdef_.rowkey_cnt_,
                                       dml_op.get_eval_ctx(),
                                       is_null))) {
        LOG_WARN("check rowkey is null failed", K(ret), K(upd_ctdef), K(upd_rtdef));
      } else if (is_null) {
        is_skipped = true;
      }
    }

    if (OB_SUCC(ret) && !is_skipped) {
      if (upd_ctdef.is_heap_table_ &&
          OB_FAIL(copy_heap_table_hidden_pk(dml_op.get_eval_ctx(), upd_ctdef))) {
        LOG_WARN("fail to copy heap table hidden pk", K(ret), K(upd_ctdef));
      }
    }

    if (OB_SUCC(ret) && !is_skipped && !has_instead_of_trg) {
      bool is_distinct = false;
      if (OB_FAIL(check_rowkey_whether_distinct(upd_ctdef.distinct_key_,
                                                upd_ctdef.distinct_algo_,
                                                dml_op.get_eval_ctx(),
                                                dml_op.get_exec_ctx(),
                                                upd_rtdef.table_rowkey_,
                                                upd_rtdef.se_rowkey_dist_ctx_,
                                                is_distinct))) {
        LOG_WARN("check rowkey whether distinct failed", K(ret),
                 K(upd_ctdef), K(upd_rtdef), K(dml_op.get_spec().rows_));
      } else if (!is_distinct) {
        is_skipped = true;
      }
    }
    if (OB_SUCC(ret) && !is_skipped) {
      bool is_filtered = false;
      //first, check assignment column whether matched column type
      if (OB_FAIL(check_column_type(upd_ctdef.new_row_,
                                    upd_rtdef.cur_row_num_,
                                    upd_ctdef.assign_columns_,
                                    dml_op))) {
        LOG_WARN("check column type failed", K(ret));
      } else if (OB_ISNULL(my_session = dml_op.get_exec_ctx().get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else if (OB_FAIL(check_nested_sql_legality(dml_op.get_exec_ctx(), upd_ctdef.dupd_ctdef_.index_tid_))) {
        LOG_WARN("failed to check stmt table", K(ret), K(ref_table_id));
      } else if (OB_FAIL(TriggerHandle::init_param_rows(
          dml_op.get_eval_ctx(), upd_ctdef.trig_ctdef_, upd_rtdef.trig_rtdef_))) {
        LOG_WARN("failed to handle before trigger", K(ret));
      } else if (OB_FAIL(TriggerHandle::do_handle_before_row(
          dml_op, upd_ctdef.das_base_ctdef_, upd_ctdef.trig_ctdef_, upd_rtdef.trig_rtdef_))) {
        LOG_WARN("failed to handle before trigger", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (has_instead_of_trg) {
        is_skipped = true;
      } else if (OB_FAIL(check_row_null(upd_ctdef.new_row_,
                                        dml_op.get_eval_ctx(),
                                        upd_rtdef.cur_row_num_,
                                        upd_ctdef.assign_columns_,
                                        upd_ctdef.dupd_ctdef_.is_ignore_,
                                        false,
                                        dml_op))) {
        LOG_WARN("check row null failed", K(ret), K(upd_ctdef), K(upd_rtdef));
      } else if (OB_FAIL(check_row_whether_changed(upd_ctdef, upd_rtdef, dml_op.get_eval_ctx()))) {
        LOG_WARN("check row whether changed failed", K(ret), K(upd_ctdef), K(upd_rtdef));
      } else if (OB_UNLIKELY(!upd_rtdef.is_row_changed_)) {
        //do nothing
      } else if (OB_FAIL(filter_row_for_view_check(upd_ctdef.view_check_exprs_, dml_op.get_eval_ctx(), is_filtered))) {
        LOG_WARN("filter row for view check exprs failed", K(ret));
      } else if (OB_UNLIKELY(is_filtered)) {
        ret = OB_ERR_CHECK_OPTION_VIOLATED;
        LOG_WARN("view check option violated", K(ret));
      } else if (OB_FAIL(filter_row_for_check_cst(upd_ctdef.check_cst_exprs_, dml_op.get_eval_ctx(), is_filtered))) {
        LOG_WARN("filter row for check cst failed", K(ret));
      } else if (OB_UNLIKELY(is_filtered)) {
        if (is_mysql_mode() && upd_ctdef.dupd_ctdef_.is_ignore_) {
          is_skipped = true;
          LOG_USER_WARN(OB_ERR_CHECK_CONSTRAINT_VIOLATED);
          LOG_WARN("check constraint violated, skip this row", K(ret));
        } else {
          ret = OB_ERR_CHECK_CONSTRAINT_VIOLATED;
          LOG_WARN("row is filtered by check filters, running is stopped", K(ret));
        }
      }
    }
  } else {
    //for global index, only check whether the updated row is changed
    if (OB_FAIL(check_row_whether_changed(upd_ctdef, upd_rtdef, dml_op.get_eval_ctx()))) {
      LOG_WARN("check row whether changed failed", K(ret), K(upd_ctdef), K(upd_rtdef));
    }
  }

  if (OB_FAIL(ret) && dml_op.is_error_logging_ && should_catch_err(ret) && !has_instead_of_trg) {
    dml_op.err_log_rt_def_.first_err_ret_ = ret;
    // cover the err_ret  by design
    ret = OB_SUCCESS;
    //todo @kaizhan.dkz expr.set_null() must skip const expr
    for (int64_t i = 0; OB_SUCC(ret) && i < upd_ctdef.full_row_.count(); ++i) {
      ObExpr *expr = upd_ctdef.full_row_.at(i);
      ObDatum *datum = nullptr;
      if (OB_FAIL(expr->eval(dml_op.get_eval_ctx(), datum))) {
        if (should_catch_err(ret) && !(IS_CONST_TYPE(expr->type_))) {
          expr->locate_datum_for_write(dml_op.get_eval_ctx()).set_null();
          expr->set_evaluated_flag(dml_op.get_eval_ctx());
          ret = OB_SUCCESS;
        }
      }
    }
  }
  ret = (ret == OB_SUCCESS ? dml_op.err_log_rt_def_.first_err_ret_ : ret);
  if (OB_SUCC(ret) && !is_skipped) {
    LOG_DEBUG("process update row", K(ret), K(is_skipped), K(upd_ctdef), K(upd_rtdef),
                "old_row", ROWEXPR2STR(dml_op.get_eval_ctx(), upd_ctdef.old_row_),
                "new_row", ROWEXPR2STR(dml_op.get_eval_ctx(), upd_ctdef.new_row_));
  }
  return ret;
}

int ObDMLService::insert_row(const ObInsCtDef &ins_ctdef,
                             ObInsRtDef &ins_rtdef,
                             const ObDASTabletLoc *tablet_loc,
                             ObDMLRtCtx &dml_rtctx,
                             ObChunkDatumStore::StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_dml_tablet_validity(dml_rtctx,
                                        *tablet_loc,
                                        ins_ctdef.new_row_,
                                        ins_ctdef,
                                        ins_rtdef))) {
    LOG_WARN("check insert row tablet validity failed", K(ret));
  } else if (OB_FAIL(insert_row(ins_ctdef.das_ctdef_,
                                ins_rtdef.das_rtdef_,
                                tablet_loc,
                                dml_rtctx,
                                ins_ctdef.new_row_,
                                stored_row))) {
    LOG_WARN("insert row to das failed", K(ret));
  }
  return ret;
}

int ObDMLService::insert_row(const ObDASInsCtDef &ins_ctdef,
                             ObDASInsRtDef &ins_rtdef,
                             const ObDASTabletLoc *tablet_loc,
                             ObDMLRtCtx &dml_rtctx,
                             const ExprFixedArray &new_row,
                             ObChunkDatumStore::StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  ret = write_row_to_das_op<DAS_OP_TABLE_INSERT>(ins_ctdef,
                                                 ins_rtdef,
                                                 tablet_loc,
                                                 dml_rtctx,
                                                 new_row,
                                                 nullptr,
                                                 stored_row);
  return ret;
}

int ObDMLService::delete_row(const ObDelCtDef &del_ctdef,
                             ObDelRtDef &del_rtdef,
                             const ObDASTabletLoc *tablet_loc,
                             ObDMLRtCtx &dml_rtctx,
                             ObChunkDatumStore::StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_dml_tablet_validity(dml_rtctx,
                                        *tablet_loc,
                                        del_ctdef.old_row_,
                                        del_ctdef,
                                        del_rtdef))) {
    LOG_WARN("check old row tablet validity failed", K(ret));
  } else if (OB_FAIL(write_row_to_das_op<DAS_OP_TABLE_DELETE>(del_ctdef.das_ctdef_,
                                                              del_rtdef.das_rtdef_,
                                                              tablet_loc,
                                                              dml_rtctx,
                                                              del_ctdef.old_row_,
                                                              del_ctdef.trans_info_expr_,
                                                              stored_row))) {
    LOG_WARN("delete old row from das failed", K(ret));
  }
  return ret;
}
int ObDMLService::lock_row(const ObLockCtDef &lock_ctdef,
                           ObLockRtDef &lock_rtdef,
                           const ObDASTabletLoc *tablet_loc,
                           ObDMLRtCtx &dml_rtctx)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  if (OB_FAIL(check_dml_tablet_validity(dml_rtctx,
                                        *tablet_loc,
                                        lock_ctdef.old_row_,
                                        lock_ctdef,
                                        lock_rtdef))) {
    LOG_WARN("check old row tablet validity failed", K(ret));
  } else if (OB_FAIL(write_row_to_das_op<DAS_OP_TABLE_LOCK>(lock_ctdef.das_ctdef_,
                                                lock_rtdef.das_rtdef_,
                                                tablet_loc,
                                                dml_rtctx,
                                                lock_ctdef.old_row_,
                                                nullptr,
                                                stored_row))) {
    LOG_WARN("lock row to das failed", K(ret));
  }
  return ret;
}

int ObDMLService::lock_row(const ObDASLockCtDef &dlock_ctdef,
                           ObDASLockRtDef &dlock_rtdef,
                           const ObDASTabletLoc *tablet_loc,
                           ObDMLRtCtx &das_rtctx,
                           const ExprFixedArray &old_row)
{
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  return write_row_to_das_op<DAS_OP_TABLE_LOCK>(dlock_ctdef,
                                                dlock_rtdef,
                                                tablet_loc,
                                                das_rtctx,
                                                old_row,
                                                nullptr,
                                                stored_row);
}

/*
 * Note: During the update process,
 * ObDMLService::check_row_whether_changed() and ObDMLService::update_row() must be executed together
 * within a single iteration,
 * because the update_row process relies on check_row_whether_changed to determine
 * whether the new and old values of the row being updated have changed.
 **/
int ObDMLService::update_row(const ObUpdCtDef &upd_ctdef,
                             ObUpdRtDef &upd_rtdef,
                             const ObDASTabletLoc *old_tablet_loc,
                             const ObDASTabletLoc *new_tablet_loc,
                             ObDMLRtCtx &dml_rtctx,
                             ObChunkDatumStore::StoredRow *&old_row,
                             ObChunkDatumStore::StoredRow *&new_row,
                             ObChunkDatumStore::StoredRow *&full_row)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(dml_rtctx.get_exec_ctx());
  if (OB_FAIL(check_dml_tablet_validity(dml_rtctx,
                                        *old_tablet_loc,
                                        upd_ctdef.old_row_,
                                        upd_ctdef,
                                        upd_rtdef))) {
    LOG_WARN("check update old row tablet validity failed", K(ret));
  } else if (OB_FAIL(check_dml_tablet_validity(dml_rtctx,
                                               *new_tablet_loc,
                                               upd_ctdef.new_row_,
                                               upd_ctdef,
                                               upd_rtdef))) {
    LOG_WARN("check update new row tablet validity failed", K(ret));
  } else if (OB_UNLIKELY(!upd_rtdef.is_row_changed_)) {
    //old row is equal to new row, only need to lock row
    if (OB_ISNULL(upd_rtdef.dlock_rtdef_)) {
      ObIAllocator &allocator = dml_rtctx.get_exec_ctx().get_allocator();
      if (OB_FAIL(init_das_lock_rtdef_for_update(dml_rtctx, upd_ctdef, upd_rtdef))) {
        LOG_WARN("init das lock rtdef failed", K(ret), K(upd_ctdef), K(upd_rtdef));
      } else {
        upd_rtdef.dlock_rtdef_->for_upd_wait_time_ = plan_ctx->get_ps_timeout_timestamp();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(write_row_to_das_op<DAS_OP_TABLE_LOCK>(*upd_ctdef.dlock_ctdef_,
                                                         *upd_rtdef.dlock_rtdef_,
                                                         old_tablet_loc,
                                                         dml_rtctx,
                                                         upd_ctdef.old_row_,
                                                         nullptr,
                                                         old_row))) {
        LOG_WARN("write row to das op failed", K(ret), K(upd_ctdef), K(upd_rtdef));
      } else {
        new_row = old_row;
      }
    }
  } else if (OB_UNLIKELY(old_tablet_loc != new_tablet_loc)) {
    //the updated row may be moved across partitions
    if (upd_ctdef.dupd_ctdef_.is_ignore_) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cross-partition update ignore");
      LOG_WARN("update ignore is not supported in across partition update, it will induce lost data error", K(ret));
    } else if (OB_LIKELY(!upd_ctdef.multi_ctdef_->is_enable_row_movement_)) {
      ret = OB_ERR_UPD_CAUSE_PART_CHANGE;
      LOG_WARN("the updated row is moved across partitions", K(ret),
               KPC(old_tablet_loc), KPC(new_tablet_loc));
    } else if (OB_ISNULL(upd_rtdef.ddel_rtdef_)) {
      if (OB_FAIL(init_das_del_rtdef_for_update(dml_rtctx, upd_ctdef, upd_rtdef))) {
        LOG_WARN("init das delete rtdef failed", K(ret), K(upd_ctdef), K(upd_rtdef));
      } else if (OB_FAIL(init_das_ins_rtdef_for_update(dml_rtctx, upd_ctdef, upd_rtdef))) {
        LOG_WARN("init das insert rtdef failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      //because of this bug:
      //if the updated row is moved across partitions, we must delete old row at first
      //and then store new row to a temporary buffer,
      //only when all old rows have been deleted, new rows can be inserted
      if (OB_FAIL(write_row_to_das_op<DAS_OP_TABLE_DELETE>(*upd_ctdef.ddel_ctdef_,
                                                           *upd_rtdef.ddel_rtdef_,
                                                           old_tablet_loc,
                                                           dml_rtctx,
                                                           upd_ctdef.old_row_,
                                                           upd_ctdef.trans_info_expr_,
                                                           old_row))) {
        LOG_WARN("delete row to das op failed", K(ret), K(upd_ctdef), K(upd_rtdef));
      } else if (upd_ctdef.is_heap_table_ &&
          OB_FAIL(set_update_hidden_pk(dml_rtctx.get_eval_ctx(),
                                       upd_ctdef,
                                       new_tablet_loc->tablet_id_))) {
        LOG_WARN("update across partitions fail to set new_hidden_pk", K(ret), KPC(new_tablet_loc));
      } else if (OB_FAIL(write_row_to_das_op<DAS_OP_TABLE_INSERT>(*upd_ctdef.dins_ctdef_,
                                                                  *upd_rtdef.dins_rtdef_,
                                                                  new_tablet_loc,
                                                                  dml_rtctx,
                                                                  upd_ctdef.new_row_,
                                                                  nullptr,
                                                                  new_row))) {
        LOG_WARN("insert row to das op failed", K(ret), K(upd_ctdef), K(upd_rtdef));
      } else {
        LOG_DEBUG("update pkey changed", K(ret), KPC(old_tablet_loc), KPC(new_tablet_loc),
                  "old row", ROWEXPR2STR(dml_rtctx.get_eval_ctx(), upd_ctdef.old_row_),
                  "new row", ROWEXPR2STR(dml_rtctx.get_eval_ctx(), upd_ctdef.new_row_));
      }
    }
  } else if (OB_FAIL(write_row_to_das_op<DAS_OP_TABLE_UPDATE>(upd_ctdef.dupd_ctdef_,
                                                              upd_rtdef.dupd_rtdef_,
                                                              old_tablet_loc,
                                                              dml_rtctx,
                                                              upd_ctdef.full_row_,
                                                              upd_ctdef.trans_info_expr_,
                                                              full_row))) {
    LOG_WARN("write row to das op failed", K(ret), K(upd_ctdef), K(upd_rtdef));
  } else {
    LOG_DEBUG("update pkey not changed", K(ret), KPC(old_tablet_loc),
              "old row", ROWEXPR2STR(dml_rtctx.get_eval_ctx(), upd_ctdef.old_row_),
              "new row", ROWEXPR2STR(dml_rtctx.get_eval_ctx(), upd_ctdef.new_row_));
  }
  return ret;
}

int ObDMLService::update_row(const ObDASUpdCtDef &ctdef,
                             ObDASUpdRtDef &rtdef,
                             const ObDASTabletLoc *tablet_loc,
                             ObDMLRtCtx &dml_rtctx,
                             const ExprFixedArray &full_row)
{
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  return write_row_to_das_op<DAS_OP_TABLE_UPDATE>(ctdef,
                                                  rtdef,
                                                  tablet_loc,
                                                  dml_rtctx,
                                                  full_row,
                                                  nullptr,
                                                  stored_row);
}

int ObDMLService::delete_row(const ObDASDelCtDef &das_del_ctdef,
                             ObDASDelRtDef &das_del_rtdef,
                             const ObDASTabletLoc *tablet_loc,
                             ObDMLRtCtx &das_rtctx,
                             const ExprFixedArray &old_row,
                             ObChunkDatumStore::StoredRow *&stored_row)
{
  return write_row_to_das_op<DAS_OP_TABLE_DELETE>(das_del_ctdef,
                                                  das_del_rtdef,
                                                  tablet_loc,
                                                  das_rtctx,
                                                  old_row,
                                                  nullptr,
                                                  stored_row);
}

int ObDMLService::init_dml_param(const ObDASDMLBaseCtDef &base_ctdef,
                                 ObDASDMLBaseRtDef &base_rtdef,
                                 transaction::ObTxReadSnapshot &snapshot,
                                 const int16_t write_branch_id,
                                 ObIAllocator &das_alloc,
                                 storage::ObStoreCtxGuard &store_ctx_gurad,
                                 storage::ObDMLBaseParam &dml_param)
{
  int ret = OB_SUCCESS;
  dml_param.timeout_ = base_rtdef.timeout_ts_;
  dml_param.schema_version_ = base_ctdef.schema_version_;
  dml_param.is_total_quantity_log_ = base_ctdef.is_total_quantity_log_;
  dml_param.tz_info_ = &base_ctdef.tz_info_;
  dml_param.sql_mode_ = base_rtdef.sql_mode_;
  dml_param.table_param_ = &base_ctdef.table_param_;
  dml_param.tenant_schema_version_ = base_rtdef.tenant_schema_version_;
  dml_param.encrypt_meta_ = &base_ctdef.encrypt_meta_;
  dml_param.prelock_ = base_rtdef.prelock_;
  dml_param.is_batch_stmt_ = base_ctdef.is_batch_stmt_;
  dml_param.dml_allocator_ = &das_alloc;
  dml_param.snapshot_ = snapshot;
  dml_param.branch_id_ = write_branch_id;
  dml_param.store_ctx_guard_ = &store_ctx_gurad;
  if (base_ctdef.is_batch_stmt_) {
    dml_param.write_flag_.set_is_dml_batch_opt();
  }
  if (base_ctdef.is_insert_up_) {
    dml_param.write_flag_.set_is_insert_up();
  }
  if (base_ctdef.is_table_api_) {
    dml_param.write_flag_.set_is_table_api();
  }
  if (dml_param.table_param_->get_data_table().is_storage_index_table()
      && !dml_param.table_param_->get_data_table().can_read_index()) {
    dml_param.write_flag_.set_is_write_only_index();
  }
  if (base_rtdef.is_for_foreign_key_check_) {
    dml_param.write_flag_.set_check_row_locked();
  }
  return ret;
}

int ObDMLService::init_das_dml_rtdef(ObDMLRtCtx &dml_rtctx,
                                     const ObDASDMLBaseCtDef &das_ctdef,
                                     ObDASDMLBaseRtDef &das_rtdef,
                                     const ObDASTableLocMeta *loc_meta)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(dml_rtctx.get_exec_ctx());
  ObSQLSessionInfo *my_session = GET_MY_SESSION(dml_rtctx.get_exec_ctx());
  ObDASCtx &das_ctx = dml_rtctx.get_exec_ctx().get_das_ctx();
  uint64_t table_loc_id = das_ctdef.table_id_;
  uint64_t ref_table_id = das_ctdef.index_tid_;
  das_rtdef.ctdef_ = &das_ctdef;
  das_rtdef.timeout_ts_ = plan_ctx->get_ps_timeout_timestamp();
  das_rtdef.prelock_ = my_session->get_prelock();
  das_rtdef.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
  das_rtdef.sql_mode_ = my_session->get_sql_mode();
  if (OB_ISNULL(das_rtdef.table_loc_ = dml_rtctx.op_.get_input()->get_table_loc())) {
    das_rtdef.table_loc_ = das_ctx.get_table_loc_by_id(table_loc_id, ref_table_id);
    if (OB_ISNULL(das_rtdef.table_loc_)) {
      if (OB_ISNULL(loc_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("loc meta is null", K(ret), K(table_loc_id), K(ref_table_id), K(das_ctx.get_table_loc_list()));
      } else if (OB_FAIL(das_ctx.extended_table_loc(*loc_meta, das_rtdef.table_loc_))) {
        LOG_WARN("extended table location failed", K(ret), KPC(loc_meta));
      }
    }
  }
  if (ObSQLUtils::is_fk_nested_sql(&dml_rtctx.get_exec_ctx())) {
    das_rtdef.is_for_foreign_key_check_ = true;
  }
  return ret;
}

int ObDMLService::init_trigger_for_insert(
  ObDMLRtCtx &dml_rtctx,
  const ObInsCtDef &ins_ctdef,
  ObInsRtDef &ins_rtdef,
  ObIArray<ObExpr*> &clear_exprs)
{
  int ret = OB_SUCCESS;
  if (!ins_ctdef.is_primary_index_ || 0 >= ins_ctdef.trig_ctdef_.tg_args_.count()) {
    // nothing
    LOG_DEBUG("debug non-primary key insert trigger for merge", K(ret));
  } else if (OB_FAIL(TriggerHandle::init_trigger_params(dml_rtctx, ins_ctdef.trig_ctdef_.tg_event_,
                          ins_ctdef.trig_ctdef_, ins_rtdef.trig_rtdef_))) {
    LOG_WARN("failed to init trigger params", K(ret));
  } else {
    append(clear_exprs, ins_ctdef.trig_ctdef_.new_row_exprs_);
    LOG_DEBUG("debug insert trigger for merge", K(ret));
  }
  return ret;
}

int ObDMLService::init_fk_checker_array(ObDMLRtCtx &dml_rtctx,
                                        const ObDMLBaseCtDef &dml_ctdef,
                                        FkCheckerArray &fk_checker_array)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = dml_rtctx.get_exec_ctx().get_allocator();
  const ObForeignKeyArgArray &fk_args = dml_ctdef.fk_args_;
  if (!fk_args.empty()) {
    if (OB_FAIL(fk_checker_array.allocate_array(allocator, fk_args.count()))) {
      LOG_WARN("failed to create foreign key checker array", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < fk_args.count(); ++i) {
        fk_checker_array.at(i) = nullptr;
      }
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < fk_args.count(); ++i) {
    const ObForeignKeyArg &fk_arg = fk_args.at(i);
    ObForeignKeyChecker *fk_checker = nullptr;
    if (fk_arg.use_das_scan_) {
      if (OB_ISNULL(fk_arg.fk_ctdef_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("need to perform foreign key check by das scan task, but scan ctdef is null", K(ret));
      } else {
        // create fk_checker here
        void *checker_buf = allocator.alloc(sizeof(ObForeignKeyChecker));
        if (OB_ISNULL(checker_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("cllocate foreign key checker buffer failed", K(ret));
        } else {
          fk_checker = new(checker_buf) ObForeignKeyChecker(dml_rtctx.get_eval_ctx(), *fk_arg.fk_ctdef_);
          fk_checker_array.at(i) = fk_checker;
          const ObExprFrameInfo *expr_frame_info = NULL;
          expr_frame_info = nullptr != dml_rtctx.op_.get_spec().expr_frame_info_
                               ? dml_rtctx.op_.get_spec().expr_frame_info_
                               : &(dml_rtctx.op_.get_spec().plan_->get_expr_frame_info());
          int64_t estimate_row = dml_rtctx.op_.get_spec().rows_;
          ObIAllocator *allocator = &dml_rtctx.op_.get_exec_ctx().get_allocator();
          if (OB_FAIL(fk_checker->init_foreign_key_checker(estimate_row, expr_frame_info, *fk_arg.fk_ctdef_,
                                                           dml_ctdef.new_row_, allocator))) {
            LOG_WARN("failed to init foreign key checker", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDMLService::init_ins_rtdef(
  ObDMLRtCtx &dml_rtctx,
  ObInsRtDef &ins_rtdef,
  const ObInsCtDef &ins_ctdef,
  ObIArray<ObExpr*> &clear_exprs,
  ObIArray<ObForeignKeyChecker*> &fk_checkers)
{
  int ret = OB_SUCCESS;
  dml_rtctx.get_exec_ctx().set_dml_event(ObDmlEventType::DE_INSERTING);
  const ObDASTableLocMeta *loc_meta = get_table_loc_meta(ins_ctdef.multi_ctdef_);
  if (lib::is_mysql_mode()
      && OB_FAIL(ObDASUtils::check_nested_sql_mutating(ins_ctdef.das_ctdef_.index_tid_, dml_rtctx.get_exec_ctx()))) {
    // MySql returns error, trigger the insert statement through udf,
    // even if there is no data that meets the where condition which in insert-select
    LOG_WARN("failed to check stmt table", K(ret), K(ins_ctdef.das_ctdef_.index_tid_));
  } else if (OB_FAIL(init_das_dml_rtdef(dml_rtctx,
                                       ins_ctdef.das_ctdef_,
                                       ins_rtdef.das_rtdef_,
                                       loc_meta))) {
    LOG_WARN("failed to init das dml rtdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(dml_rtctx, ins_ctdef.related_ctdefs_, ins_rtdef.related_rtdefs_))) {
    LOG_WARN("init related das ctdef failed", K(ret));
  } else if (OB_FAIL(init_trigger_for_insert(dml_rtctx, ins_ctdef, ins_rtdef, clear_exprs))) {
    LOG_WARN("failed to init trigger for insert", K(ret));
  } else if (OB_FAIL(init_fk_checker_array(dml_rtctx, ins_ctdef, ins_rtdef.fk_checker_array_))) {
    LOG_WARN("failed to init foreign key checker array", K(ret));
  } else {
    ins_rtdef.das_rtdef_.related_ctdefs_ = &ins_ctdef.related_ctdefs_;
    ins_rtdef.das_rtdef_.related_rtdefs_ = &ins_rtdef.related_rtdefs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < ins_rtdef.fk_checker_array_.count(); ++i) {
      if (OB_NOT_NULL(ins_rtdef.fk_checker_array_.at(i))) {
        fk_checkers.push_back(ins_rtdef.fk_checker_array_.at(i));
      }
    }
  }
  return ret;
}

int ObDMLService::init_trigger_for_delete(
  ObDMLRtCtx &dml_rtctx, const ObDelCtDef &del_ctdef, ObDelRtDef &del_rtdef)
{
  int ret = OB_SUCCESS;
  if (del_ctdef.is_primary_index_ && 0 < del_ctdef.trig_ctdef_.tg_args_.count()) {
    OZ(TriggerHandle::init_trigger_params(dml_rtctx, del_ctdef.trig_ctdef_.tg_event_,
                                del_ctdef.trig_ctdef_, del_rtdef.trig_rtdef_));
    LOG_DEBUG("debug delete trigger init", K(ret), K(del_ctdef.is_primary_index_),
      K(del_ctdef.trig_ctdef_.tg_args_.count()));
  }
  return ret;
}

int ObDMLService::init_del_rtdef(ObDMLRtCtx &dml_rtctx,
                                 ObDelRtDef &del_rtdef,
                                 const ObDelCtDef &del_ctdef)
{
  int ret = OB_SUCCESS;
  dml_rtctx.get_exec_ctx().set_dml_event(ObDmlEventType::DE_DELETING);
  const ObDASTableLocMeta *loc_meta = get_table_loc_meta(del_ctdef.multi_ctdef_);
    if (lib::is_mysql_mode()
      && OB_FAIL(ObDASUtils::check_nested_sql_mutating(del_ctdef.das_ctdef_.index_tid_, dml_rtctx.get_exec_ctx()))) {
    // MySql returns error, trigger the delete statement through udf,
    // even if there is no data that meets the where condition
    LOG_WARN("failed to check stmt table", K(ret), K(del_ctdef.das_ctdef_.index_tid_));
  } else if (OB_FAIL(init_das_dml_rtdef(dml_rtctx,
                                       del_ctdef.das_ctdef_,
                                       del_rtdef.das_rtdef_,
                                       loc_meta))) {
    LOG_WARN("failed to init das dml rfdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(dml_rtctx, del_ctdef.related_ctdefs_, del_rtdef.related_rtdefs_))) {
    LOG_WARN("init related das ctdef failed", K(ret));
  } else if (OB_FAIL(init_trigger_for_delete(dml_rtctx, del_ctdef, del_rtdef))) {
    LOG_WARN("failed to init trigger for delete", K(ret));
  } else if (OB_FAIL(init_fk_checker_array(dml_rtctx, del_ctdef, del_rtdef.fk_checker_array_))) {
    LOG_WARN("failed to init foreign key checker array", K(ret));
  } else {
    del_rtdef.das_rtdef_.related_ctdefs_ = &del_ctdef.related_ctdefs_;
    del_rtdef.das_rtdef_.related_rtdefs_ = &del_rtdef.related_rtdefs_;
  }

  if (OB_SUCC(ret)) {
    ObTableModifyOp &dml_op = dml_rtctx.op_;
    const uint64_t del_table_id = del_ctdef.das_base_ctdef_.index_tid_;
    ObExecContext *root_ctx = nullptr;
    if (T_DISTINCT_NONE != del_ctdef.distinct_algo_) {
      if (dml_op.is_fk_nested_session()) {
        // for delete distinct check that has foreign key, perform global distinct check between nested session,
        // to avoid delete same row mutiple times between different nested sqls
        if (OB_FAIL(get_exec_ctx_for_duplicate_rowkey_check(&dml_op.get_exec_ctx(), root_ctx))) {
          LOG_WARN("failed to get root exec ctx", K(ret));
        } else if (OB_ISNULL(root_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the root exec ctx is nullptr", K(ret));
        } else {
          DASDelCtxList& del_ctx_list = root_ctx->get_das_ctx().get_das_del_ctx_list();
          if (ObDMLService::is_nested_dup_table(del_table_id, del_ctx_list)) {
            // for table deleted at parent session too, no need to create a new hash set
            if (OB_FAIL(ObDMLService::get_nested_dup_table_ctx(del_table_id, del_ctx_list, del_rtdef.se_rowkey_dist_ctx_))) {
              LOG_WARN("failed to get nested duplicate delete table ctx for fk nested session", K(ret));
            }
          } else {
            // for table not deleted at parent session, create a new hash set and add to the list at root ctx
            DmlRowkeyDistCtx del_ctx;
            del_ctx.table_id_ = del_table_id;
            LOG_TRACE("[FOREIGN KEY] create hash set used for checking duplicate rowkey due to cascade delete", K(del_table_id));
            if (OB_FAIL(ObDMLService::create_rowkey_check_hashset(dml_op.get_spec().rows_, root_ctx, del_ctx.deleted_rows_))) {
              LOG_WARN("failed to create hash set", K(ret));
            } else if (OB_FAIL(del_ctx_list.push_back(del_ctx))) {
              if (nullptr != del_ctx.deleted_rows_) {
                del_ctx.deleted_rows_->destroy();
                del_ctx.deleted_rows_ = nullptr;
              }
              LOG_WARN("failed to push del ctx to list", K(ret));
            } else {
              del_rtdef.se_rowkey_dist_ctx_ = del_ctx.deleted_rows_;
            }
          }
        }
      } else {
        // for delete distinct check without foreign key, perform distinct check at current sql
        DASDelCtxList& del_ctx_list = dml_op.get_exec_ctx().get_das_ctx().get_das_del_ctx_list();
        DmlRowkeyDistCtx del_ctx;
        del_ctx.table_id_ = del_table_id;
        LOG_TRACE("[FOREIGN KEY] create hash set used for checking duplicate rowkey due to cascade delete", K(del_table_id));
        if (OB_FAIL(get_exec_ctx_for_duplicate_rowkey_check(&dml_op.get_exec_ctx(), root_ctx))) {
          LOG_WARN("failed to get root exec ctx", K(ret));
        } else if (OB_ISNULL(root_ctx) || root_ctx != &dml_op.get_exec_ctx()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid root exec ctx for delete distinct check", K(ret));
        } else if (OB_FAIL(ObDMLService::create_rowkey_check_hashset(dml_op.get_spec().rows_, root_ctx, del_ctx.deleted_rows_))) {
          LOG_WARN("failed to create hash set", K(ret));
        } else if (OB_FAIL(del_ctx_list.push_back(del_ctx))) {
          if (nullptr != del_ctx.deleted_rows_) {
            del_ctx.deleted_rows_->destroy();
            del_ctx.deleted_rows_ = nullptr;
          }
        } else {
          del_rtdef.se_rowkey_dist_ctx_ = del_ctx.deleted_rows_;
        }
      }
    } else { //T_DISTINCT_NONE == del_ctdef.distinct_algo_, means optimizer think don't need to create a new hash set for distinct check
      if (dml_op.is_fk_nested_session()) { //for delete triggered by delete cascade, need to check whether upper nested sqls will delete the same table
        if (OB_FAIL(get_exec_ctx_for_duplicate_rowkey_check(&dml_op.get_exec_ctx(), root_ctx))) {
          LOG_WARN("failed to get root exec ctx", K(ret));
        } else if (OB_ISNULL(root_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the root exec ctx is nullptr", K(ret));
        } else {
          DASDelCtxList& del_ctx_list = root_ctx->get_das_ctx().get_das_del_ctx_list();
          if (ObDMLService::is_nested_dup_table(del_table_id, del_ctx_list)) {
            // A duplicate table was found
            LOG_TRACE("[FOREIGN KEY] get hash set used for checking duplicate rowkey due to cascade delete", K(del_table_id));
            if (OB_FAIL(ObDMLService::get_nested_dup_table_ctx(del_table_id, del_ctx_list, del_rtdef.se_rowkey_dist_ctx_))) {
              LOG_WARN("failed to get nested duplicate delete table ctx for fk nested session", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(del_rtdef.se_rowkey_dist_ctx_)) {
    const int64_t rowkey_cnt = del_ctdef.distinct_key_.count();
    ObIAllocator &allocator = dml_rtctx.get_exec_ctx().get_allocator();
    if (OB_FAIL(init_ob_rowkey(allocator, del_ctdef.distinct_key_.count(), del_rtdef.table_rowkey_))) {
      LOG_WARN("fail to init ObRowkey used for distinct check", K(ret));
    }
  }
  return ret;
}

int ObDMLService::init_trigger_for_update(
  ObDMLRtCtx &dml_rtctx,
  const ObUpdCtDef &upd_ctdef,
  ObUpdRtDef &upd_rtdef,
  ObTableModifyOp &dml_op,
  ObIArray<ObExpr*> &clear_exprs)
{
  UNUSED(dml_op);
  int ret = OB_SUCCESS;
  if (!upd_ctdef.is_primary_index_ || 0 >= upd_ctdef.trig_ctdef_.tg_args_.count()) {
    // nothing
    LOG_DEBUG("debug non-primary key update trigger for merge", K(ret));
  } else if (OB_FAIL(TriggerHandle::init_trigger_params(dml_rtctx, upd_ctdef.trig_ctdef_.tg_event_,
                          upd_ctdef.trig_ctdef_, upd_rtdef.trig_rtdef_))) {
    LOG_WARN("failed to init trigger params", K(ret));
  } else {
    ObSEArray<ObExpr *, 4> new_exprs;
    for (int64_t i = 0; i < upd_ctdef.trig_ctdef_.new_row_exprs_.count() && OB_SUCC(ret); ++i) {
      if (upd_ctdef.trig_ctdef_.new_row_exprs_.at(i) != upd_ctdef.trig_ctdef_.old_row_exprs_.at(i)) {
        OZ(new_exprs.push_back(upd_ctdef.trig_ctdef_.new_row_exprs_.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      append(clear_exprs, new_exprs);
    }
    LOG_DEBUG("debug update trigger for merge", K(ret));
  }
  return ret;
}

int ObDMLService::init_upd_rtdef(
  ObDMLRtCtx &dml_rtctx,
  ObUpdRtDef &upd_rtdef,
  const ObUpdCtDef &upd_ctdef,
  ObIArray<ObExpr*> &clear_exprs,
  ObIArray<ObForeignKeyChecker*> &fk_checkers)
{
  int ret = OB_SUCCESS;
  const ObDASTableLocMeta *loc_meta = get_table_loc_meta(upd_ctdef.multi_ctdef_);
  dml_rtctx.get_exec_ctx().set_dml_event(ObDmlEventType::DE_UPDATING);
  if (lib::is_mysql_mode()
      && OB_FAIL(ObDASUtils::check_nested_sql_mutating(upd_ctdef.dupd_ctdef_.index_tid_, dml_rtctx.get_exec_ctx()))) {
    // MySql returns error, trigger the update statement through udf,
    // even if there is no data that meets the where condition
    LOG_WARN("failed to check stmt table", K(ret), K(upd_ctdef.dupd_ctdef_.index_tid_));
  } else if (OB_FAIL(init_das_dml_rtdef(dml_rtctx,
                                       upd_ctdef.dupd_ctdef_,
                                       upd_rtdef.dupd_rtdef_,
                                       loc_meta))) {
    LOG_WARN("failed to init das dml rfdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(dml_rtctx, upd_ctdef.related_upd_ctdefs_, upd_rtdef.related_upd_rtdefs_))) {
    LOG_WARN("init related das ctdef failed", K(ret));
  } else if (OB_FAIL(init_trigger_for_update(dml_rtctx, upd_ctdef, upd_rtdef, dml_rtctx.op_, clear_exprs))) {
    LOG_WARN("failed to init trigger for update", K(ret));
  } else if (OB_FAIL(init_fk_checker_array(dml_rtctx, upd_ctdef, upd_rtdef.fk_checker_array_))) {
    LOG_WARN("failed to init foreign key checker array", K(ret));
  } else {
    upd_rtdef.dupd_rtdef_.related_ctdefs_ = &upd_ctdef.related_upd_ctdefs_;
    upd_rtdef.dupd_rtdef_.related_rtdefs_ = &upd_rtdef.related_upd_rtdefs_;
    dml_rtctx.get_exec_ctx().set_update_columns(&upd_ctdef.assign_columns_);
    for (int64_t i = 0; OB_SUCC(ret) && i < upd_rtdef.fk_checker_array_.count(); ++i) {
      if (OB_NOT_NULL(upd_rtdef.fk_checker_array_.at(i))) {
        fk_checkers.push_back(upd_rtdef.fk_checker_array_.at(i));
      }
    }
  }

  if (OB_SUCC(ret) && T_DISTINCT_NONE != upd_ctdef.distinct_algo_) {
    ObTableModifyOp &dml_op = dml_rtctx.op_;
    if (OB_FAIL(create_rowkey_check_hashset(dml_op.get_spec().rows_,
                                            &dml_op.get_exec_ctx(),
                                            upd_rtdef.se_rowkey_dist_ctx_))) {
      LOG_WARN("failed to create distinct check hash set", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(upd_rtdef.se_rowkey_dist_ctx_)) {
    const int64_t rowkey_cnt = upd_ctdef.distinct_key_.count();
    ObIAllocator &allocator = dml_rtctx.get_exec_ctx().get_allocator();
    if (OB_FAIL(init_ob_rowkey(allocator, upd_ctdef.distinct_key_.count(), upd_rtdef.table_rowkey_))) {
      LOG_WARN("fail to init ObRowkey used for distinct check", K(ret));
    }
  }
  return ret;
}

int ObDMLService::init_ob_rowkey( ObIAllocator &allocator, const int64_t rowkey_cnt, ObRowkey &table_rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new(buf) ObObj[rowkey_cnt];
  }
  table_rowkey.assign(obj_ptr, rowkey_cnt);
  return ret;
}

int ObDMLService::add_trans_info_datum(ObExpr *trans_info_expr,
                                       ObEvalCtx &eval_ctx,
                                       ObChunkDatumStore::StoredRow *stored_row)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;

  if (OB_ISNULL(trans_info_expr) || OB_ISNULL(stored_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), K(trans_info_expr), K(stored_row));
  } else if (OB_FAIL(trans_info_expr->eval(eval_ctx, datum))) {
    LOG_WARN("failed to evaluate expr datum", K(ret));
  } else if (OB_ISNULL(datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    char *buf = static_cast<char *>(stored_row->get_extra_payload());
    *static_cast<int32_t *>(stored_row->get_extra_payload()) = datum->len_;
    int64_t pos = sizeof(int32_t);
    MEMCPY(buf + pos, datum->ptr_, datum->len_);
  }

  return ret;
}

int ObDMLService::init_das_ins_rtdef_for_update(ObDMLRtCtx &dml_rtctx,
                                                const ObUpdCtDef &upd_ctdef,
                                                ObUpdRtDef &upd_rtdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = dml_rtctx.get_exec_ctx().get_allocator();
  const ObDASTableLocMeta *loc_meta = get_table_loc_meta(upd_ctdef.multi_ctdef_);
  if (OB_FAIL(ObDASTaskFactory::alloc_das_rtdef(DAS_OP_TABLE_INSERT,
                                                allocator,
                                                upd_rtdef.dins_rtdef_))) {
    LOG_WARN("create das delete rtdef failed", K(ret));
  } else if (OB_FAIL(init_das_dml_rtdef(dml_rtctx,
                                        *upd_ctdef.dins_ctdef_,
                                        *upd_rtdef.dins_rtdef_,
                                        loc_meta))) {
    LOG_WARN("init das insert rtdef failed", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(dml_rtctx, upd_ctdef.related_ins_ctdefs_, upd_rtdef.related_ins_rtdefs_))) {
    LOG_WARN("init related das insert ctdef failed", K(ret));
  } else {
    upd_rtdef.dins_rtdef_->related_ctdefs_ = &upd_ctdef.related_ins_ctdefs_;
    upd_rtdef.dins_rtdef_->related_rtdefs_ = &upd_rtdef.related_ins_rtdefs_;
  }
  return ret;
}

int ObDMLService::init_das_del_rtdef_for_update(ObDMLRtCtx &dml_rtctx,
                                                const ObUpdCtDef &upd_ctdef,
                                                ObUpdRtDef &upd_rtdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = dml_rtctx.get_exec_ctx().get_allocator();
  const ObDASTableLocMeta *loc_meta = get_table_loc_meta(upd_ctdef.multi_ctdef_);
  if (OB_FAIL(ObDASTaskFactory::alloc_das_rtdef(DAS_OP_TABLE_DELETE,
                                                allocator,
                                                upd_rtdef.ddel_rtdef_))) {
    LOG_WARN("create das delete rtdef failed", K(ret));
  } else if (OB_FAIL(init_das_dml_rtdef(dml_rtctx,
                                        *upd_ctdef.ddel_ctdef_,
                                        *upd_rtdef.ddel_rtdef_,
                                        loc_meta))) {
    LOG_WARN("init das dml rtdef failed", K(ret), K(upd_ctdef), K(upd_rtdef));
  } else if (OB_FAIL(init_related_das_rtdef(dml_rtctx, upd_ctdef.related_del_ctdefs_, upd_rtdef.related_del_rtdefs_))) {
    LOG_WARN("init related das ctdef failed", K(ret));
  } else {
    upd_rtdef.ddel_rtdef_->related_ctdefs_ = &upd_ctdef.related_del_ctdefs_;
    upd_rtdef.ddel_rtdef_->related_rtdefs_ = &upd_rtdef.related_del_rtdefs_;
  }
  return ret;
}

int ObDMLService::init_das_lock_rtdef_for_update(ObDMLRtCtx &dml_rtctx,
                                                 const ObUpdCtDef &upd_ctdef,
                                                 ObUpdRtDef &upd_rtdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = dml_rtctx.get_exec_ctx().get_allocator();
  const ObDASTableLocMeta *loc_meta = get_table_loc_meta(upd_ctdef.multi_ctdef_);
  if (OB_FAIL(ObDASTaskFactory::alloc_das_rtdef(DAS_OP_TABLE_LOCK,
                                                allocator,
                                                upd_rtdef.dlock_rtdef_))) {
    LOG_WARN("create das lock rtdef failed", K(ret));
  } else if (OB_FAIL(init_das_dml_rtdef(dml_rtctx,
                                        *upd_ctdef.dlock_ctdef_,
                                        *upd_rtdef.dlock_rtdef_,
                                        loc_meta))) {
    LOG_WARN("init das dml rtdef failed", K(ret), K(upd_ctdef), K(upd_rtdef));
  }
  return ret;
}
int ObDMLService::init_lock_rtdef(ObDMLRtCtx &dml_rtctx,
                                  const ObLockCtDef &lock_ctdef,
                                  ObLockRtDef &lock_rtdef,
                                  int64_t wait_ts)
{
  lock_rtdef.das_rtdef_.for_upd_wait_time_ = wait_ts;
  const ObDASTableLocMeta *loc_meta = get_table_loc_meta(lock_ctdef.multi_ctdef_);
  return init_das_dml_rtdef(dml_rtctx, lock_ctdef.das_ctdef_, lock_rtdef.das_rtdef_, loc_meta);
}

template <int N>
int ObDMLService::write_row_to_das_op(const ObDASDMLBaseCtDef &ctdef,
                                      ObDASDMLBaseRtDef &rtdef,
                                      const ObDASTabletLoc *tablet_loc,
                                      ObDMLRtCtx &dml_rtctx,
                                      const ExprFixedArray &row,
                                      ObExpr *trans_info_expr,
                                      ObChunkDatumStore::StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  bool need_retry = false;
  bool is_strict_defensive_check = trans_info_expr == nullptr ? false : true;
  typedef typename das_reg::ObDASOpTypeTraits<N>::DASCtDef CtDefType;
  typedef typename das_reg::ObDASOpTypeTraits<N>::DASRtDef RtDefType;
  typedef typename das_reg::ObDASOpTypeTraits<N>::DASOp OpType;
  OB_ASSERT(typeid(ctdef) == typeid(CtDefType));
  OB_ASSERT(typeid(rtdef) == typeid(RtDefType));
  int64_t extend_size = is_strict_defensive_check ?
      ObDASWriteBuffer::DAS_WITH_TRANS_INFO_EXTEND_SIZE :
        ObDASWriteBuffer::DAS_ROW_DEFAULT_EXTEND_SIZE;
  do {
    bool buffer_full = false;
    need_retry = false;
    //1. find das dml op
    OpType *dml_op = nullptr;
    if (OB_UNLIKELY(!dml_rtctx.das_ref_.has_das_op(tablet_loc, dml_op))) {
      if (OB_FAIL(dml_rtctx.das_ref_.prepare_das_task(tablet_loc, dml_op))) {
        LOG_WARN("prepare das task failed", K(ret), K(N));
      } else if (OB_FAIL(dml_op->init_task_info(extend_size))) {
        LOG_WARN("fail to init das write buff", K(ret), K(extend_size));
      } else {
        dml_op->set_das_ctdef(static_cast<const CtDefType*>(&ctdef));
        dml_op->set_das_rtdef(static_cast<RtDefType*>(&rtdef));
        rtdef.table_loc_->is_writing_ = true;
      }
      if (OB_SUCC(ret) &&
          rtdef.related_ctdefs_ != nullptr && !rtdef.related_ctdefs_->empty()) {
        if (OB_FAIL(add_related_index_info(*tablet_loc,
                                           *rtdef.related_ctdefs_,
                                           *rtdef.related_rtdefs_,
                                           *dml_op))) {
          LOG_WARN("add related index info failed", K(ret));
        }
      }
    }
    //2. try add row to das dml buffer
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml_op->write_row(row, dml_rtctx.get_eval_ctx(), stored_row, buffer_full))) {
        LOG_WARN("insert row to das dml op buffer failed", K(ret), K(ctdef), K(rtdef));
      } else if (!buffer_full &&
          OB_NOT_NULL(trans_info_expr) &&
          OB_FAIL(ObDMLService::add_trans_info_datum(trans_info_expr, dml_rtctx.get_eval_ctx(), stored_row))) {
        LOG_WARN("fail to add trans info datum", K(ret));
      } else if (OB_NOT_NULL(stored_row)) {
        dml_rtctx.add_cached_row_size(stored_row->row_size_);
        LOG_DEBUG("write row to das op", K(ret), K(buffer_full),
            "op_type", N, "table_id", ctdef.table_id_, "index_tid", ctdef.index_tid_,
            "row", ROWEXPR2STR(dml_rtctx.get_eval_ctx(), row), "row_size", stored_row->row_size_);
      }
    }
    //3. if buffer is full, frozen node, create a new das op to add row
    if (OB_SUCC(ret) && buffer_full) {
      need_retry = true;
      if (REACH_COUNT_INTERVAL(10)) { // print log per 10 times.
        LOG_INFO("DAS write buffer full, ", K(dml_op->get_row_cnt()), K(dml_rtctx.get_row_buffer_size()));
      }
      dml_rtctx.das_ref_.set_frozen_node();
    }
  } while (OB_SUCC(ret) && need_retry);
  return ret;
}

int ObDMLService::add_related_index_info(const ObDASTabletLoc &tablet_loc,
                                         const DASDMLCtDefArray &related_ctdefs,
                                         DASDMLRtDefArray &related_rtdefs,
                                         ObIDASTaskOp &das_op)
{
  int ret = OB_SUCCESS;
  das_op.get_related_ctdefs().set_capacity(related_ctdefs.count());
  das_op.get_related_rtdefs().set_capacity(related_rtdefs.count());
  das_op.get_related_tablet_ids().set_capacity(related_ctdefs.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs.count(); ++i) {
    ObDASTabletLoc *index_tablet_loc = ObDASUtils::get_related_tablet_loc(
        const_cast<ObDASTabletLoc&>(tablet_loc), related_ctdefs.at(i)->index_tid_);
    if (OB_ISNULL(index_tablet_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index tablet loc is null", K(ret), K(i));
    } else if (OB_FAIL(das_op.get_related_ctdefs().push_back(related_ctdefs.at(i)))) {
      LOG_WARN("store related ctdef failed", K(ret), K(related_ctdefs), K(i));
    } else if (OB_FAIL(das_op.get_related_rtdefs().push_back(related_rtdefs.at(i)))) {
      LOG_WARN("store related rtdef failed", K(ret));
    } else if (OB_FAIL(das_op.get_related_tablet_ids().push_back(index_tablet_loc->tablet_id_))) {
      LOG_WARN("store related index tablet id failed", K(ret));
    }
  }
  return ret;
}

int ObDMLService::convert_exprs_to_stored_row(ObIAllocator &allocator,
                                              ObEvalCtx &eval_ctx,
                                              const ObExprPtrIArray &exprs,
                                              ObChunkDatumStore::StoredRow *&new_row)
{
  return ObChunkDatumStore::StoredRow::build(new_row, exprs, eval_ctx, allocator);
}

int ObDMLService::catch_violate_error(int err_ret,
                                      transaction::ObTxSEQ savepoint_no,
                                      ObDMLRtCtx &dml_rtctx,
                                      ObErrLogRtDef &err_log_rt_def,
                                      ObErrLogCtDef &error_logging_ctdef,
                                      ObErrLogService &err_log_service,
                                      ObDASOpType type)
{
  int ret = OB_SUCCESS;
  int rollback_ret = OB_SUCCESS;
  // 1. if there is no exception in the previous processing, then write this row to storage layer
  if (OB_SUCC(err_ret) &&
      err_log_rt_def.first_err_ret_ == OB_SUCCESS) {
    if (OB_FAIL(write_one_row_post_proc(dml_rtctx))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        err_log_rt_def.first_err_ret_ = ret;
        if (OB_SUCCESS != (rollback_ret = ObSqlTransControl::rollback_savepoint(dml_rtctx.get_exec_ctx(), savepoint_no))) {
          ret = rollback_ret;
          LOG_WARN("fail to rollback save point", K(rollback_ret));
        }
      } else {
        LOG_WARN("fail to insert row ", K(ret));
      }
    }
  }

  // 2. if cache some err, must write err info to error logging table
  // if need write error logging table, will cover error_code
  // 3. should_catch_err(ret) == true -> write_one_row_post_proc throw some err
  // 4. should_catch_err(err_ret) == true -> some error occur before write storage
  if (OB_SUCCESS == rollback_ret &&
      OB_SUCCESS != err_log_rt_def.first_err_ret_ &&
      (should_catch_err(ret) || should_catch_err(err_ret))) {
    if (OB_FAIL(err_log_service.insert_err_log_record(GET_MY_SESSION(dml_rtctx.get_exec_ctx()),
                                                      error_logging_ctdef,
                                                      err_log_rt_def,
                                                      type))) {
      LOG_WARN("fail to insert_err_log_record", K(ret));
    }
  }
  return ret;
}

int ObDMLService::write_one_row_post_proc(ObDMLRtCtx &dml_rtctx)
{
  int ret = OB_SUCCESS;
  int close_ret = OB_SUCCESS;
  if (dml_rtctx.das_ref_.has_task()) {
    if (OB_FAIL(dml_rtctx.das_ref_.execute_all_task())) {
      LOG_WARN("execute all update das task failed", K(ret));
    }

    // whether execute result is success or fail , must close all task
    if (OB_SUCCESS != (close_ret = (dml_rtctx.das_ref_.close_all_task()))) {
      LOG_WARN("close all das task failed", K(ret));
    } else {
      dml_rtctx.reuse();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("must have das task");
  }

  ret = (ret == OB_SUCCESS ? close_ret : ret);
  return ret;
}

int ObDMLService::copy_heap_table_hidden_pk(ObEvalCtx &eval_ctx,
                                            const ObUpdCtDef &upd_ctdef)
{
  int ret = OB_SUCCESS;
  ObExpr *old_hidden_pk = upd_ctdef.old_row_.at(0);
  ObExpr *new_hidden_pk = upd_ctdef.new_row_.at(0);
  ObDatum *hidden_pk_datum = NULL;
  if (OB_ISNULL(old_hidden_pk) || OB_ISNULL(new_hidden_pk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is unexpected null", K(ret), KPC(old_hidden_pk), KPC(new_hidden_pk));
  } else if (!upd_ctdef.is_heap_table_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not heap_table", K(ret), K(upd_ctdef));
  } else if (new_hidden_pk->type_ != T_TABLET_AUTOINC_NEXTVAL) {
    LOG_TRACE("heap table not update the part_key", K(ret));
  } else if (OB_FAIL(old_hidden_pk->eval(eval_ctx, hidden_pk_datum))) {
    LOG_WARN("eval old_hidden_pk failed", K(ret), KPC(old_hidden_pk));
  } else if (OB_ISNULL(hidden_pk_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hidden_pk_datum is null", K(ret), KPC(old_hidden_pk));
  } else if (!old_hidden_pk->obj_meta_.is_uint64()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hidden_pk must be uint64 type", K(ret), KPC(old_hidden_pk));
  } else {
    ObDatum &datum = new_hidden_pk->locate_datum_for_write(eval_ctx);
    datum.set_uint(hidden_pk_datum->get_uint());
    new_hidden_pk->get_eval_info(eval_ctx).evaluated_ = true;
  }
  return ret;
}

int ObDMLService::set_update_hidden_pk(ObEvalCtx &eval_ctx,
                                       const ObUpdCtDef &upd_ctdef,
                                       const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  uint64_t autoinc_seq = 0;
  if (upd_ctdef.is_heap_table_ && upd_ctdef.is_primary_index_) {
    ObExpr *auto_inc_expr = upd_ctdef.new_row_.at(0);
    ObSQLSessionInfo *my_session = eval_ctx.exec_ctx_.get_my_session();
    uint64_t tenant_id = my_session->get_effective_tenant_id();
    if (OB_FAIL(get_heap_table_hidden_pk(tenant_id, tablet_id, autoinc_seq))) {
      LOG_WARN("fail to het hidden pk", K(ret), K(tablet_id), K(tenant_id));
    } else if (OB_ISNULL(auto_inc_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new_hidden_pk_expr is null", K(ret), K(upd_ctdef));
    } else if (auto_inc_expr->type_ != T_TABLET_AUTOINC_NEXTVAL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the first expr is not tablet_auto_inc column", K(ret), KPC(auto_inc_expr));
    } else {
      ObDatum &datum = auto_inc_expr->locate_datum_for_write(eval_ctx);
      datum.set_uint(autoinc_seq);
      auto_inc_expr->get_eval_info(eval_ctx).evaluated_ = true;
    }
  }
  return ret;
}

int ObDMLService::get_heap_table_hidden_pk(uint64_t tenant_id,
                                           const ObTabletID &tablet_id,
                                           uint64_t &pk)
{
  int ret = OB_SUCCESS;
  uint64_t autoinc_seq = 0;
  ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
  if (OB_FAIL(auto_inc.get_autoinc_seq(tenant_id, tablet_id, autoinc_seq))) {
    LOG_WARN("get_autoinc_seq fail", K(ret), K(tenant_id), K(tablet_id));
  } else {
    pk = autoinc_seq;
    LOG_TRACE("after get autoinc_seq", K(pk), K(tenant_id), K(tablet_id));
  }
  return ret;
}

int ObDMLService::set_heap_table_hidden_pk(const ObInsCtDef &ins_ctdef,
                                           const ObTabletID &tablet_id,
                                           ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  uint64_t autoinc_seq = 0;
  if (ins_ctdef.is_heap_table_ && ins_ctdef.is_primary_index_) {
    ObSQLSessionInfo *my_session = eval_ctx.exec_ctx_.get_my_session();
    uint64_t tenant_id = my_session->get_effective_tenant_id();
    if (OB_FAIL(ObDMLService::get_heap_table_hidden_pk(tenant_id,
                                                       tablet_id,
                                                       autoinc_seq))) {
      LOG_WARN("fail to het hidden pk", K(ret), K(tablet_id), K(tenant_id));
    } else {
      ObExpr *auto_inc_expr = ins_ctdef.new_row_.at(0);
      if (auto_inc_expr->type_ != T_TABLET_AUTOINC_NEXTVAL) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the first expr is not tablet_auto_inc column", K(ret), KPC(auto_inc_expr));
      } else {
        ObDatum &datum = auto_inc_expr->locate_datum_for_write(eval_ctx);
        datum.set_uint(autoinc_seq);
        auto_inc_expr->get_eval_info(eval_ctx).evaluated_ = true;
      }
    }
  }
  return ret;
}

int ObDMLService::check_nested_sql_legality(ObExecContext &ctx, common::ObTableID ref_table_id)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.get_my_session();
  if (session->is_remote_session() && ctx.get_parent_ctx() != nullptr) {
    //in nested sql, and the sql is remote or distributed
    pl::ObPLContext *pl_ctx = ctx.get_parent_ctx()->get_pl_stack_ctx();
    if (pl_ctx == nullptr || !pl_ctx->in_autonomous()) {
      //this nested sql require transaction scheduler control
      //but the session is remote, means this sql executing without transaction scheduler control
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Perform a DML operation inside a query or remote/distributed sql");
      LOG_WARN("check nested sql legality failed", K(ret), K(pl_ctx));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDASUtils::check_nested_sql_mutating(ref_table_id, ctx))) {
      LOG_WARN("check nested sql mutating failed", K(ret));
    }
  }
  return ret;
}

int ObDMLService::create_anonymous_savepoint(ObTxDesc &tx_desc, transaction::ObTxSEQ &savepoint)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSqlTransControl::create_anonymous_savepoint(tx_desc, savepoint))) {
    LOG_WARN("create savepoint failed", K(ret));
  }
  return ret;
}

int ObDMLService::rollback_local_savepoint(ObTxDesc &tx_desc, const transaction::ObTxSEQ savepoint, int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  ObTransService *tx = MTL(transaction::ObTransService*);
  if (OB_FAIL(tx->rollback_to_implicit_savepoint(tx_desc, savepoint, expire_ts, nullptr))) {
    LOG_WARN("rollback to implicit local savepoint failed", K(ret));
  }
  return ret;
}

int ObDMLService::check_local_index_affected_rows(int64_t table_affected_rows,
                                                  int64_t index_affected_rows,
                                                  const ObDASDMLBaseCtDef &ctdef,
                                                  ObDASDMLBaseRtDef &rtdef,
                                                  const ObDASDMLBaseCtDef &related_ctdef,
                                                  ObDASDMLBaseRtDef &related_rtdef)
{
  int ret = OB_SUCCESS;
  if (GCONF.enable_defensive_check()) {
    if (table_affected_rows != index_affected_rows
        && !related_ctdef.table_param_.get_data_table().is_domain_index()
        && !related_ctdef.table_param_.get_data_table().is_mlog_table()) {
      ret = OB_ERR_DEFENSIVE_CHECK;
      ObString func_name = ObString::make_string("check_local_index_affected_rows");
      LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
      SQL_DAS_LOG(ERROR, "Fatal Error!!! data table affected row is not match with index table", K(ret),
                  "table_affected_rows", table_affected_rows,
                  "index_affected_rows", index_affected_rows,
                  "table_ctdef", ctdef,
                  "index_ctdef", related_ctdef,
                  "table_rtdef", rtdef,
                  "index_rtdef", related_rtdef);
    }
  }
  return ret;
}

template <typename T>
const ObDASTableLocMeta *ObDMLService::get_table_loc_meta(const T *multi_ctdef)
{
  const ObDASTableLocMeta *loc_meta = nullptr;
  if (multi_ctdef != nullptr) {
    loc_meta = &(multi_ctdef->loc_meta_);
  }
  return loc_meta;
}

int ObDMLService::init_related_das_rtdef(ObDMLRtCtx &dml_rtctx,
                                         const DASDMLCtDefArray &das_ctdefs,
                                         DASDMLRtDefArray &das_rtdefs)
{
  int ret = OB_SUCCESS;
  if (!das_ctdefs.empty()) {
    ObIAllocator &allocator = dml_rtctx.get_exec_ctx().get_allocator();
    if (OB_FAIL(das_rtdefs.allocate_array(allocator, das_ctdefs.count()))) {
      SQL_DAS_LOG(WARN, "create das insert rtdef array failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < das_ctdefs.count(); ++i) {
    ObDASTaskFactory &das_factory = dml_rtctx.get_exec_ctx().get_das_ctx().get_das_factory();
    ObDASBaseRtDef *das_rtdef = nullptr;
    if (OB_FAIL(das_factory.create_das_rtdef(das_ctdefs.at(i)->op_type_, das_rtdef))) {
      SQL_DAS_LOG(WARN, "create das insert rtdef failed", K(ret));
    } else if (OB_FAIL(init_das_dml_rtdef(dml_rtctx,
                                          *das_ctdefs.at(i),
                                          static_cast<ObDASDMLBaseRtDef&>(*das_rtdef),
                                          nullptr))) {
      SQL_DAS_LOG(WARN, "init das dml rtdef failed", K(ret));
    } else {
      das_rtdefs.at(i) = static_cast<ObDASDMLBaseRtDef*>(das_rtdef);
    }
  }
  return ret;
}

int ObDMLService::check_dml_tablet_validity(ObDMLRtCtx &dml_rtctx,
                                            const ObDASTabletLoc &tablet_loc,
                                            const ExprFixedArray &row,
                                            const ObDMLBaseCtDef &dml_ctdef,
                                            ObDMLBaseRtDef &dml_rtdef)
{
  int ret = OB_SUCCESS;
  if (GCONF.enable_strict_defensive_check()) {
    //only strict defensive check need to check tablet validity, otherwise ignore it
    ObTableLocation *tmp_location = nullptr;
    ObSEArray<ObTabletID, 1> tablet_ids;
    ObSEArray<ObObjectID, 1> partition_ids;
    uint64_t table_id = tablet_loc.loc_meta_->ref_table_id_;
    ObIAllocator &allocator = dml_rtctx.get_exec_ctx().get_allocator();
    if (OB_ISNULL(dml_rtdef.check_location_)) {
      void *location_buf = allocator.alloc(sizeof(ObTableLocation));
      if (OB_ISNULL(location_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate location buffer failed", K(ret));
      } else {
        dml_rtdef.check_location_ = new(location_buf) ObTableLocation(allocator);
        tmp_location = dml_rtdef.check_location_;
        ObSchemaGetterGuard *schema_guard = dml_rtctx.get_exec_ctx().get_sql_ctx()->schema_guard_;
        ObSQLSessionInfo *session = dml_rtctx.get_exec_ctx().get_my_session();
        ObSqlSchemaGuard sql_schema_guard;
        sql_schema_guard.set_schema_guard(schema_guard);
        // Here, judge the schema version at the check table level.
        // If the table-level schema_version is not equal, directly report an error schema_again
        if (OB_ISNULL(schema_guard) || OB_ISNULL(session)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(schema_guard), K(session));
        } else if (OB_FAIL(tmp_location->init_table_location_with_column_ids(
            sql_schema_guard, table_id, dml_ctdef.column_ids_, dml_rtctx.get_exec_ctx()))) {
          LOG_WARN("init table location with column ids failed", K(ret), K(dml_ctdef), K(table_id));
        }
      }
    } else {
      tmp_location = dml_rtdef.check_location_;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(convert_exprs_to_row(row,
                                       dml_rtctx.get_eval_ctx(),
                                       dml_rtdef,
                                       dml_rtctx.get_exec_ctx().get_allocator()))) {
        LOG_WARN("check dml tablet validity", K(ret));
      } else if (OB_FAIL(tmp_location->calculate_tablet_id_by_row(dml_rtctx.get_exec_ctx(),
                                                                  table_id,
                                                                  dml_ctdef.column_ids_,
                                                                  *dml_rtdef.check_row_,
                                                                  tablet_ids,
                                                                  partition_ids))) {
        LOG_WARN("calculate tablet id by row failed", K(ret));
      } else if (tablet_ids.count() != 1 || tablet_loc.tablet_id_ != tablet_ids.at(0)) {
        ObSchemaGetterGuard *schema_guard = dml_rtctx.get_exec_ctx().get_sql_ctx()->schema_guard_;
        ObSQLSessionInfo *session = dml_rtctx.get_exec_ctx().get_my_session();
        const ObTableSchema *table_schema = nullptr;
        // Here, judge the schema version at the check table level.
        // If the table-level schema_version is not equal, directly report an error schema_again
        if (OB_ISNULL(schema_guard) || OB_ISNULL(session)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(schema_guard), K(session));
        } else if (OB_FAIL(schema_guard->get_table_schema(
            session->get_effective_tenant_id(), table_id, table_schema))) {
          LOG_WARN("failed to get table schema", K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("failed to get schema", K(ret));
        } else if (table_schema->get_schema_version() != dml_ctdef.das_base_ctdef_.schema_version_) {
          ret = OB_SCHEMA_EAGAIN;
          LOG_WARN("table version mismatch", K(ret), K(table_id),
              K(table_schema->get_schema_version()), K(dml_ctdef.das_base_ctdef_.schema_version_));
        } else {
          ret = OB_ERR_DEFENSIVE_CHECK;
          ObString func_name = ObString::make_string("check_dml_tablet_validity");
          LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
          LOG_ERROR("Fatal Error!!! Catch a defensive error!", K(ret),
                    K(tablet_loc), K(tablet_ids),
                    KPC(dml_rtdef.check_row_), KPC(dml_rtdef.check_location_));
          LOG_ERROR("Fatal Error!!! Catch a defensive error!", K(ret), K(table_schema->get_schema_version()), K(dml_ctdef.das_base_ctdef_.schema_version_),
                    KPC(tmp_location), KPC(table_schema));
        }
      }
    }
  }
  return ret;
}

int ObDMLService::convert_exprs_to_row(const ExprFixedArray &exprs,
                                       ObEvalCtx &eval_ctx,
                                       ObDMLBaseRtDef &dml_rtdef,
                                       ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dml_rtdef.check_row_)) {
    if (OB_FAIL(ob_create_row(allocator, exprs.count(), dml_rtdef.check_row_))) {
      LOG_WARN("create current row failed", K(ret));
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObDatum *datum = nullptr;
    if (OB_FAIL(exprs.at(i)->eval(eval_ctx, datum))) {
      LOG_WARN("get column datum failed", K(ret));
    } else if (OB_FAIL(datum->to_obj(dml_rtdef.check_row_->cells_[i], exprs.at(i)->obj_meta_))) {
      LOG_WARN("expr datum to current row failed", K(ret));
    }
  }
  return ret;
}

bool ObDMLService::is_nested_dup_table(const uint64_t table_id,  DASDelCtxList& del_ctx_list)
{
  bool ret = false;
  DASDelCtxList::iterator iter = del_ctx_list.begin();
  for (; !ret && iter != del_ctx_list.end(); iter++) {
    DmlRowkeyDistCtx del_ctx = *iter;
    if (del_ctx.table_id_ == table_id) {
      ret = true;
    }
  }
  return ret;
}

int ObDMLService::get_nested_dup_table_ctx(const uint64_t table_id,  DASDelCtxList& del_ctx_list, SeRowkeyDistCtx *&rowkey_dist_ctx)
{
  int ret = OB_SUCCESS;
  bool find = false;
  DASDelCtxList::iterator iter = del_ctx_list.begin();
  for (; !find && iter != del_ctx_list.end(); iter++) {
    DmlRowkeyDistCtx del_ctx = *iter;
    if (del_ctx.table_id_ == table_id) {
      find = true;
      rowkey_dist_ctx = del_ctx.deleted_rows_;
    }
  }
  return ret;
}

int ObDMLService::handle_after_processing_multi_row(ObDMLModifyRowsList *dml_modify_rows, ObTableModifyOp *dml_op)
{
  int ret = OB_SUCCESS;
  const ObDmlEventType t_insert = ObDmlEventType::DE_INSERTING;
  const ObDmlEventType t_update = ObDmlEventType::DE_UPDATING;
  const ObDmlEventType t_delete = ObDmlEventType::DE_DELETING;
  if (OB_ISNULL(dml_modify_rows) || OB_ISNULL(dml_op) || OB_ISNULL(dml_op->get_child())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml operator or modify rows list is null", K(dml_modify_rows), K(dml_op));
  } else if (OB_ISNULL(dml_op->last_store_row_.get_store_row()) &&
    OB_FAIL(dml_op->last_store_row_.init(dml_op->get_exec_ctx().get_allocator(), dml_op->get_child()->get_spec().output_.count()))) {
    LOG_WARN("failed to init shadow stored row", K(ret));
  } else if (OB_FAIL(dml_op->last_store_row_.shadow_copy(dml_op->get_child()->get_spec().output_, dml_op->get_eval_ctx()))) {
    LOG_WARN("failed to backup the datum ptr of child operator", K(ret));
  } else {
    ObDMLModifyRowsList::iterator row_iter = dml_modify_rows->begin();
    for (; OB_SUCC(ret) && row_iter != dml_modify_rows->end(); row_iter++) {
      ObDMLModifyRowNode &modify_row = *row_iter;
      if (OB_ISNULL(modify_row.full_row_) && OB_ISNULL(modify_row.new_row_) && OB_ISNULL(modify_row.old_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parameter for batch post row processing", K(ret));
      } else if (OB_ISNULL(modify_row.dml_op_) || OB_ISNULL(modify_row.dml_ctdef_) || OB_ISNULL(modify_row.dml_rtdef_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parameter for batch post row processing", K(ret));
      } else {
        ObTableModifyOp &op = *modify_row.dml_op_;
        const ObDMLBaseCtDef &dml_ctdef = *modify_row.dml_ctdef_;
        ObDMLBaseRtDef &dml_rtdef = *modify_row.dml_rtdef_;
        const ObDmlEventType dml_event = modify_row.dml_event_;
        // process foreign key
        if (OB_NOT_NULL(modify_row.full_row_) && OB_FAIL(modify_row.full_row_->to_expr(modify_row.dml_ctdef_->full_row_, op.get_eval_ctx()))) {
          LOG_WARN("failed to covert stored full row to expr", K(ret));
        } else if (OB_NOT_NULL(modify_row.old_row_) && OB_FAIL(modify_row.old_row_->to_expr(dml_ctdef.old_row_, op.get_eval_ctx()))) {
          LOG_WARN("failed to covert stored old row to expr", K(ret));
        } else if (OB_NOT_NULL(modify_row.new_row_) && OB_FAIL(modify_row.new_row_->to_expr(dml_ctdef.new_row_, op.get_eval_ctx()))) {
          LOG_WARN("failed to covert stored new row to expr", K(ret));
        } else if (op.need_foreign_key_checks()) {
          if (t_update == dml_event && !reinterpret_cast<ObUpdRtDef &>(dml_rtdef).is_row_changed_) {
            LOG_DEBUG("update operation don't change any value, no need to perform foreign key check");
          } else {
            // if check foreign key in batch, build fk check tasks here
            if (dml_op->get_spec().check_fk_batch_) {
              if (OB_FAIL(build_batch_fk_check_tasks(dml_ctdef, dml_rtdef))) {
                LOG_WARN("failed to build batch check foreign key checks", K(ret));
              }
            } else if (OB_FAIL(ForeignKeyHandle::do_handle(op, dml_ctdef, dml_rtdef))) {
              LOG_WARN("failed to handle foreign key constraints", K(ret));
            }
          }
        }
        // process after row trigger
        if (OB_SUCC(ret) && dml_ctdef.trig_ctdef_.all_tm_points_.has_after_row()) {
          ObEvalCtx &eval_ctx = op.get_eval_ctx();
          if (dml_event != t_insert && dml_event != t_update && dml_event != t_delete) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid trigger event", K(ret));
          } else if (t_insert == dml_event && OB_FAIL(TriggerHandle::init_param_new_row(
              eval_ctx, dml_ctdef.trig_ctdef_, dml_rtdef.trig_rtdef_))) {
            LOG_WARN("failed to init trigger parameter for new row", K(ret));
          } else if (t_delete == dml_event && OB_FAIL(TriggerHandle::init_param_old_row(
              eval_ctx, dml_ctdef.trig_ctdef_, dml_rtdef.trig_rtdef_))) {
            LOG_WARN("failed to init trigger parameter for old row", K(ret));
          } else if (t_update == dml_event && OB_FAIL(TriggerHandle::init_param_rows(
              eval_ctx, dml_ctdef.trig_ctdef_, dml_rtdef.trig_rtdef_))) {
            LOG_WARN("failed to init trigger parameter for old row and new row", K(ret));
          } else if (OB_FAIL(TriggerHandle::do_handle_after_row(op, dml_ctdef.trig_ctdef_, dml_rtdef.trig_rtdef_, dml_event))) {
            LOG_WARN("failed to handle after trigger", K(ret));
          }
        }
      }
    }

    // check the result of batch foreign key check results
    if (OB_SUCC(ret)) {
      if (dml_op->get_spec().check_fk_batch_ && OB_FAIL(dml_op->perform_batch_fk_check())) {
        LOG_WARN("failed to perform batch foreign key check", K(ret));
      } else if (OB_FAIL(dml_op->last_store_row_.restore(dml_op->get_child()->get_spec().output_, dml_op->get_eval_ctx()))) {
        LOG_WARN("failed to restore the datum ptr", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLService::handle_after_processing_single_row(ObDMLModifyRowsList *dml_modify_rows)
{
  int ret = OB_SUCCESS;
  // for single-row processing, the expr defined in ctdef and trig parameters haven't been refreshed
  // case1: only one row added to the list of dml_modify_rows;
  // case2: for multi table dml stmt, each table adds a row to the list;
  // In these cases, there is no need to re-convert the rows to be modified into an expression and init trig parameters;
  ObDMLModifyRowsList::iterator row_iter = dml_modify_rows->begin();
  for (; OB_SUCC(ret) && row_iter != dml_modify_rows->end(); row_iter++) {
    ObDMLModifyRowNode &modify_row = *row_iter;
    if (OB_ISNULL(modify_row.full_row_) && OB_ISNULL(modify_row.new_row_) && OB_ISNULL(modify_row.old_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter for batch post row processing", K(ret));
    } else if (OB_ISNULL(modify_row.dml_op_) || OB_ISNULL(modify_row.dml_ctdef_) || OB_ISNULL(modify_row.dml_rtdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter for batch post row processing", K(ret));
    } else {
      ObTableModifyOp &op = *modify_row.dml_op_;
      const ObDMLBaseCtDef &dml_ctdef = *modify_row.dml_ctdef_;
      ObDMLBaseRtDef &dml_rtdef = *modify_row.dml_rtdef_;
      const ObDmlEventType t_insert = ObDmlEventType::DE_INSERTING;
      const ObDmlEventType t_update = ObDmlEventType::DE_UPDATING;
      const ObDmlEventType t_delete = ObDmlEventType::DE_DELETING;
      const ObDmlEventType dml_event = modify_row.dml_event_;
      if (dml_event != t_insert && dml_event != t_update && dml_event != t_delete) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid trigger event", K(ret));
      } else {
        if (t_update == dml_event) {
          // for update op, Foreign key checks need to be performed only if the value has changed
          if (reinterpret_cast<ObUpdRtDef &>(dml_rtdef).is_row_changed_ && OB_FAIL(ForeignKeyHandle::do_handle(op, dml_ctdef, dml_rtdef))) {
            LOG_WARN("failed to handle foreign key constraints", K(ret));
          }
        } else if (OB_FAIL(ForeignKeyHandle::do_handle(op, dml_ctdef, dml_rtdef))) {
          LOG_WARN("failed to handle foreign key constraints", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(TriggerHandle::do_handle_after_row(op, dml_ctdef.trig_ctdef_, dml_rtdef.trig_rtdef_, dml_event))) {
        LOG_WARN("failed to handle after trigger", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLService::handle_after_row_processing(ObTableModifyOp *op,
                                              ObDMLModifyRowsList *dml_modify_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(dml_modify_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table modify operator or list of the modify rows is null", K(ret), K(op), K(dml_modify_rows));
  } else if (1 > dml_modify_rows->size()) {
    // after row processing list is empty, nothing to do
    #ifndef NDEBUG
      LOG_INFO("No row need to perform foreign key check or after row trigger");
    #endif
  } else if (op->execute_single_row_) {
    ret = handle_after_processing_single_row(dml_modify_rows);
  } else {
    ret = handle_after_processing_multi_row(dml_modify_rows, op);
  }
  return ret;
}

int ObDMLService::build_batch_fk_check_tasks(const ObDMLBaseCtDef &dml_ctdef,
                                             ObDMLBaseRtDef &dml_rtdef)
{
  int ret = OB_SUCCESS;
  if (dml_rtdef.fk_checker_array_.count() != dml_ctdef.fk_args_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("foreign key aruments count is not equal with foreign key checker count",
              K(ret),K(dml_rtdef.fk_checker_array_.count()), K(dml_ctdef.fk_args_.count()));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < dml_rtdef.fk_checker_array_.count(); ++i) {
      ObForeignKeyChecker *fk_checker = dml_rtdef.fk_checker_array_.at(i);
      const ObForeignKeyArg &fk_arg = dml_ctdef.fk_args_.at(i);
      bool need_check = true;
      if (OB_ISNULL(fk_checker)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("foreign key checker is nullptr", K(ret), K(i));
      } else if (!fk_arg.use_das_scan_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("foreign key check can not use das scan", K(ret), K(i));
      } else if (OB_FAIL(fk_checker->build_fk_check_das_task(fk_arg.columns_, dml_ctdef.new_row_, need_check))) {
        LOG_WARN("failed to build batch foreign key check scan task", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLService::log_user_error_inner(
    int ret,
    int64_t row_num,
    common::ObString &column_name,
    ObExecContext &ctx)
{
  if (OB_DATA_OUT_OF_RANGE == ret) {
    ObSQLUtils::copy_and_convert_string_charset(
        ctx.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx.get_my_session()->get_local_collation_connection());
    LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, column_name.length(), column_name.ptr(), row_num);
  } else if (OB_ERR_DATA_TOO_LONG == ret && lib::is_mysql_mode()) {
    ObSQLUtils::copy_and_convert_string_charset(
        ctx.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx.get_my_session()->get_local_collation_connection());
    LOG_USER_ERROR(OB_ERR_DATA_TOO_LONG, column_name.length(), column_name.ptr(), row_num);
  } else if (OB_ERR_VALUE_LARGER_THAN_ALLOWED == ret) {
    LOG_USER_ERROR(OB_ERR_VALUE_LARGER_THAN_ALLOWED);
  } else if (OB_INVALID_DATE_VALUE == ret || OB_INVALID_DATE_FORMAT == ret) {
    ret = OB_INVALID_DATE_VALUE;
    ObSQLUtils::copy_and_convert_string_charset(
        ctx.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx.get_my_session()->get_local_collation_connection());
    LOG_USER_ERROR(
        OB_ERR_INVALID_DATE_MSG_FMT_V2,
        column_name.length(),
        column_name.ptr(),
        row_num);
  } else if ((OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == ret || OB_INVALID_NUMERIC == ret)
      && lib::is_mysql_mode()) {
    ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD;
    ObSQLUtils::copy_and_convert_string_charset(
        ctx.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx.get_my_session()->get_local_collation_connection());
    LOG_USER_ERROR(
        OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD,
        column_name.length(),
        column_name.ptr(),
        row_num);
  } else if (OB_ERR_WARN_DATA_OUT_OF_RANGE == ret && lib::is_mysql_mode()) {
    ObSQLUtils::copy_and_convert_string_charset(
        ctx.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx.get_my_session()->get_local_collation_connection());
    LOG_USER_ERROR(OB_ERR_WARN_DATA_OUT_OF_RANGE, column_name.length(), column_name.ptr(), row_num);
  } else if ((OB_ERR_DATA_TRUNCATED == ret || OB_ERR_DOUBLE_TRUNCATED == ret)
      && lib::is_mysql_mode()) {
    ret = OB_ERR_DATA_TRUNCATED;
    ob_reset_tsi_warning_buffer();
    ObSQLUtils::copy_and_convert_string_charset(
        ctx.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx.get_my_session()->get_local_collation_connection());
    LOG_USER_ERROR(OB_ERR_DATA_TRUNCATED, column_name.length(), column_name.ptr(), row_num);
  } else {
    LOG_WARN("fail to operate row", K(ret));
  }
  return ret;
}

// get the exec_ctx to create hash set to perform duplicate rowkey check,
// which is used to avoid delete or update same row mutiple times
int ObDMLService::get_exec_ctx_for_duplicate_rowkey_check(ObExecContext *ctx, ObExecContext* &needed_ctx)
{
  int ret = OB_SUCCESS;
  ObExecContext *parent_ctx = nullptr;
  needed_ctx = nullptr;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (OB_ISNULL(parent_ctx = ctx->get_parent_ctx())) {
    // case1: current ctx is the root ctx, means current stmt is the root stmt,
    // create hash set at current ctx to perform duplicate rowkey check
    needed_ctx = ctx;
  } else if (!(parent_ctx->get_das_ctx().is_fk_cascading_)) {
    // case2: current stmt is not the root stmt, is not triggered by foreign key cascade operations
    // may be trigger or PL instead, create hash set at current ctx to perform duplicate rowkey check
    needed_ctx = ctx;
  } else if (OB_FAIL(SMART_CALL(get_exec_ctx_for_duplicate_rowkey_check(parent_ctx, needed_ctx)))) {
    // case3: current stmt is a nested stmt, and is triggered by foreign key cascade operations
    // need to find it's ancestor ctx which is not triggered by cascade operations
    LOG_WARN("failed to get the exec_ctx to perform duplicate rowkey check between nested sqls", K(ret), K(ctx->get_nested_level()));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
