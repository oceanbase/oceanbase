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
#include "sql/engine/expr/ob_expr_orahash.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "objit/common/ob_item_type.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "ob_expr_func_part_hash.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_schema_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

ObExprOrahash::ObExprOrahash(ObIAllocator &alloc)
  :ObFuncExprOperator(alloc, T_FUN_SYS_ORAHASH, N_ORAHASH, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprOrahash::~ObExprOrahash()
{
}

int ObExprOrahash::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types,
                                     int64_t param_num,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (param_num < 1) {
    ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
    LOG_WARN("not enough arguments.", K(ret));
  } else if (param_num > 3) {
    ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
    LOG_WARN("Too many arguments.", K(ret));
  } else if (OB_UNLIKELY(types[0].is_lob() || types[0].is_ext())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("inconsistent type", K(ret));
  } else {

    if (types[0].is_decimal_int()) {
      // if param types are decimal_int and ob_number but represent the same thing
      // need to get the same results
      types[0].set_calc_type(ObNumberType);
    }

    type.set_number();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].precision_);
    type.set_calc_type(common::ObNumberType);
  }
  if (OB_SUCC(ret)) {
    if (1 < param_num && !types[1].is_null()) {
      types[1].set_calc_type(ObIntType);
    }
    if (2 < param_num && !types[2].is_null()) {
      types[2].set_calc_type(ObIntType);
    }
  }
  return ret;
}

//算法详见
uint64_t ObExprOrahash::hash_mod_oracle(uint64_t val, uint64_t buckets) const
{
  uint64_t N = 1;
  // caller ensure buckets > 0
  // log2(buckets) result will be here: N
  uint64_t mask = buckets >> 1;
  while (mask) {
    N = N << 1;
    mask = mask >> 1;
  }

  uint64_t part_id= val % N;
  if (part_id + N < buckets && (val & N) == N){
    part_id += N;
  }
  return part_id;
}

bool ObExprOrahash::is_applicable_type(const ObObj& input) const
{
  bool ret = true;
  if (input.is_lob() || input.is_ext()) {
    return false;
  }

  return ret;
}

bool ObExprOrahash::is_valid_number(const int64_t& input)
{
  bool ret = true;
  if (input < 0 || input > MAX_BUCKETS) {
    ret = false;
  }

  return ret;
}

int ObExprOrahash::get_int64_value(const ObObj &obj, ObExprCtx &expr_ctx, int64_t &val) const
{
  int ret = OB_SUCCESS;
  EXPR_DEFINE_CAST_CTX(expr_ctx,  CM_NONE);
  EXPR_GET_INT64_V2(obj, val);
  if (OB_FAIL(ret)) {
    LOG_WARN("get int64 failed.", K(ret), K(val), K(obj));
    // 为了和oralce兼容，ora_hash(expr, 1e33)这种场景下，ob报的是OB_DATA_OUT_OF_RANGE，oracle是illegal argument
    if (ret == OB_DATA_OUT_OF_RANGE){
      ret = OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION;
      LOG_WARN("error code covered for compatiable with oracle", K(ret));
    }
  } else {
    if (!is_valid_number(val)) {
      ret = OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION;
      LOG_WARN("illegal argument for ora_hash function", K(ret), K(val), K(obj));
    }
  }
  return ret;
}

bool ObExprOrahash::is_any_null(const ObObj *objs, const int64_t num) const
{
  bool ret = false;
  for (int i = 0; i < num; i++) {
    ret = ret || objs[i].is_null();
  }
  return ret;
}

int ObExprOrahash::eval_orahash(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  // ora_hash(expr, bucket, seed);
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval param faield", K(ret));
  } else {
    bool has_null = false;
    for (int64_t i = 0; !has_null && i < expr.arg_cnt_; ++i) {
      if (expr.locate_param_datum(ctx, i).is_null()) {
        has_null = true;
      }
    }
    if (has_null) {
      res.set_null();
    } else {
      int64_t bval = MAX_BUCKETS;
      int64_t sval = 0;
      if (1 < expr.arg_cnt_) {
        bval = expr.locate_param_datum(ctx, 1).get_int();
      }
      if (2 < expr.arg_cnt_) {
        sval = expr.locate_param_datum(ctx, 2).get_int();
      }
      if (!is_valid_number(bval) || !is_valid_number(sval)) {
        ret = OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION;
        LOG_WARN("illegal argument for ora_hash function", K(ret), K(bval), K(sval));
      }
      OV(!ObExprFuncPartHash::is_virtual_part_for_oracle(
         ctx.exec_ctx_.get_task_executor_ctx()), OB_NOT_SUPPORTED);
      if (OB_SUCC(ret)) {
        uint64_t hval = 0;
        bval = bval + 1; // consistent with oracle
        ObExpr mock_expr = expr;
        mock_expr.arg_cnt_ = 1;
        ObDatum mock_res;
        int64_t hval_int = 0;
        mock_res.pack_ = sizeof(hval_int);
        mock_res.int_ = &hval_int;
        if (OB_FAIL(ObExprFuncPartHash::eval_oracle_part_hash(mock_expr, ctx, mock_res, sval))) {
          LOG_WARN("eval_hash_val failed", K(ret));
        }
        if(OB_SUCC(ret)) {
          if (0 == sval) {
            // seed 为0的时候，需要和partition by的结果一致
            hval_int = std::abs(hval_int);
            int64_t tmp_hval = 0;
            if (OB_FAIL(share::schema::ObPartitionUtils::calc_hash_part_idx(hval_int, bval, tmp_hval))) {
              LOG_WARN("failed to calc hash part index.", K(ret));
            } else {
              hval = static_cast<uint64_t>(tmp_hval);
            }
          } else {
            // 老引擎下调用了ObExprFuncPartHash::calc_value_for_oracle()由于没有做static_cast
            // 所以结果跟这里直接调用ObExprFuncPartHash::eval_oracle_part_hash()结果有出入
            // 应该是老引擎的bug
            hval = static_cast<uint64_t>(hval_int);
            hval = hval % bval;
          }
        }
        if (OB_SUCC(ret)) {
          number::ObNumber res_nmb;
          ObNumStackOnceAlloc res_alloc;
          if (OB_FAIL(res_nmb.from(hval, res_alloc))) {
            LOG_WARN("set result number failed.", K(ret));
          } else {
            res.set_number(res_nmb);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprOrahash::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                           ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  expr.eval_func_ = eval_orahash;
  return ret;
}
