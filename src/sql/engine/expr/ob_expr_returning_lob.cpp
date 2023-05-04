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
#include "ob_expr_returning_lob.h"
#include "ob_expr_lob_utils.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprReturningLob::ObExprReturningLob(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_RETURNING_LOB, "returning_lob", 4, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                        false, INTERNAL_IN_ORACLE_MODE)
{
}
int ObExprReturningLob::calc_result_type1(ObExprResType &type,
                                          ObExprResType &arg,
                                          common::ObExprTypeCtx &) const
{
  int ret = OB_SUCCESS;
  type = arg;
  if (ObLongTextType == arg.get_type() || ObLobType == arg.get_type()) {
    type.set_type(ObLobType);
  }
  return ret;
}
int ObExprReturningLob::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types,
                                          int64_t param_num,
                                          common::ObExprTypeCtx &type_ctx) const
{
  //objs[0] value
  //objs[1] table_id
  //objs[2] column_id
  //objs[3] rowid
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  CK(param_num == 4)
  if (OB_SUCC(ret)) {
    type = types[0];
    if (ObLongTextType == types[0].get_type() || ObLobType == types[0].get_type()) {
      type.set_type(ObLobType);
    }
  }
  return ret;
}

int ObExprReturningLob::cg_expr(ObExprCGCtx &,
                                const ObRawExpr &,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(4 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprReturningLob::eval_lob;
  return ret;
}
// This expression is only used when dml returning lob_locator under the new engine,
// expr_datum will be calculated in the insert/update operator,
// so no calculation is needed here, as long as expr_datum is not cleaned up
int ObExprReturningLob::eval_lob(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  //objs[0] value
  //objs[1] table_id
  //objs[2] column_id
  //objs[3] rowid
  ObString rowid_str;
  ObExpr *rowid_expr = NULL;
  ObExpr *value_expr = NULL;
  ObExpr *table_id_expr = NULL;
  ObExpr *column_id_expr = NULL;
  uint64_t column_id = 0;
  uint64_t table_id = 0;
  if (expr.arg_cnt_ != 4) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("returning lob expr arg_cnt is not equal 4", K(ret));
  } else if (OB_ISNULL(rowid_expr = expr.args_[3])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowid expr is null", K(ret));
  } else {
    ObObj rowid_obj;
    const ObObjMeta &rowid_obj_meta = rowid_expr->obj_meta_;
    ObDatum *rowid_col_datum = NULL;
    if (OB_FAIL(rowid_expr->eval(ctx, rowid_col_datum))) {
      LOG_WARN("rowid expr eval fail", K(ret));
    } else if (OB_ISNULL(rowid_col_datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowid_col_datum is null", K(ret));
    } else if (OB_FAIL(rowid_col_datum->to_obj(rowid_obj, rowid_obj_meta))) {
      LOG_WARN("rowid_col_datum to obj fail", K(ret));
    } else {
      int64_t rowid_buf_size = rowid_obj.get_urowid().needed_base64_buffer_size();
      int64_t rowid_str_size = 0;
      char *rowid_buf = nullptr;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &alloc = alloc_guard.get_allocator();
      if (OB_ISNULL(rowid_buf = reinterpret_cast<char *>(alloc.alloc(rowid_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for rowid base64 str", K(ret), K(rowid_buf_size));
      } else if (OB_FAIL(rowid_obj.get_urowid().get_base64_str(rowid_buf, rowid_buf_size, rowid_str_size))) {
        STORAGE_LOG(WARN, "Failed to get rowid base64 string", K(ret));
      } else {
        rowid_str.assign_ptr(rowid_buf, rowid_str_size);
        LOG_DEBUG("rowid_str is", K(rowid_str));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObDatum *table_id_datum = NULL;
    ObDatum *column_id_datum = NULL;
    if (OB_ISNULL(table_id_expr = expr.args_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_id expr is null", K(ret));
    } else if (OB_FAIL(table_id_expr->eval(ctx, table_id_datum))) {
      LOG_WARN("table id expr eval fail", K(ret));
    } else if (OB_ISNULL(column_id_expr = expr.args_[2])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column id expr is null", K(ret));
    } else if (OB_FAIL(column_id_expr->eval(ctx, column_id_datum))) {
      LOG_WARN("column id expr eval fail ", K(ret));
    } else {
      column_id = column_id_datum->get_uint64();
      table_id = table_id_datum->get_uint64();
    }
  }
  if (OB_SUCC(ret)) {
    const ObObjMeta &lob_obj_meta = expr.obj_meta_;
    int64_t locator_size = 0;
    char *buf = nullptr;
    ObString values_string;
    ObLobLocator *locator = nullptr;
    ObDatum *value_col_datum = NULL;
    ObObj value_lob_obj;
    if (OB_ISNULL(value_expr = expr.args_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value expr is null", K(ret));
    } else if (OB_FAIL(value_expr->eval(ctx, value_col_datum))) {
      LOG_WARN("fail to eval value_expr", K(ret));
    } else if (OB_ISNULL(value_col_datum)) {
      LOG_WARN("src_col_datum is null", K(ret));
    } else if (!value_expr->obj_meta_.has_lob_header()) {
      if (OB_FAIL(value_col_datum->to_obj(value_lob_obj, value_expr->obj_meta_))) {
        LOG_WARN("value_col_datum to_obj failed", K(ret));
      } else {
        value_lob_obj.get_string(values_string);
        locator_size = sizeof(ObLobLocator) + values_string.length() + rowid_str.length();
        LOG_DEBUG("src_lob_obj values_string is", K(values_string.length()),
                                                  K(locator_size),
                                                  K(rowid_str.length()),
                                                  K(values_string),
                                                  K(rowid_str));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, locator_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (FALSE_IT(MEMSET(buf, 0, locator_size))) {
      } else if (FALSE_IT(locator = reinterpret_cast<ObLobLocator *>(buf))) {
      } else if (OB_FAIL(locator->init(table_id,
          column_id,
          100, // use a default snapshot_version, now is unused
          /*trans_desc.get_snapshot_version(),*/
          // some bug: get_snapshot_version() is -1
          LOB_DEFAULT_FLAGS, rowid_str, values_string))) {
        LOG_WARN("Failed to init lob locator", K(ret), K(rowid_str));
      } else {
        expr_datum.set_lob_locator(*locator);
      }
    } else {
      // use lob locator v2
      // if rowid_str is empty use result is a temp lob, or it is a persist lob
      // referer to build_returning_lob_expr
      // do nothing? assert must be locator?
      if (!value_col_datum->is_null()) {
        ObLobLocatorV2 loc(value_col_datum->get_string(), value_expr->obj_meta_.has_lob_header());
        if (!loc.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid lob value", K(ret), K(value_col_datum), K(loc));
        }
      }
      expr_datum.set_datum(*value_col_datum);
    }
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
