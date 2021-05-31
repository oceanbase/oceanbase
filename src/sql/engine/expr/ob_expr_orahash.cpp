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
#include "sql/parser/ob_item_type.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "ob_expr_func_part_hash.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_schema_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

ObExprOrahash::ObExprOrahash(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ORAHASH, N_ORAHASH, PARAM_NUM_UNKNOWN, NOT_ROW_DIMENSION)
{}

ObExprOrahash::~ObExprOrahash()
{}

int ObExprOrahash::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  uint64_t hval = 0;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init.", K(ret));
  } else {
    number::ObNumber numhash;
    int64_t bval = MAX_BUCKETS, sval = 0;
    if (is_any_null(objs, param_num)) {
      result.set_null();
    } else {
      switch (param_num) {
        case 1:
          break;
        case 2:  // buckets
          ret = get_int64_value(objs[1], expr_ctx, bval);
          break;
        case 3:  // buckets and seed
          if (OB_SUCC(get_int64_value(objs[1], expr_ctx, bval))) {
            if (OB_FAIL(get_int64_value(objs[2], expr_ctx, sval))) {
              LOG_WARN("parse seed value failed.", K(ret));
            }
          } else {
            LOG_WARN("parse bucket value failed.", K(ret));
          }
          break;
        default:
          if (param_num < 1) {
            ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
            LOG_WARN("not enough arguments", K(ret));
          } else {
            ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
            LOG_WARN("Too many arguments.", K(ret));
          }
          break;
      }
      if (OB_SUCC(ret)) {
        if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3000) {
          if (ObExprFuncPartOldHash::is_virtual_part_for_oracle(expr_ctx.exec_ctx_->get_task_executor_ctx())) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support for virtual tables.", K(ret));
          } else {
            bval = bval + 1;  // consistent with oracle
            if (0 == sval) {
              ObObj tmpres;
              if (OB_FAIL(ObExprFuncPartOldHash::calc_value_for_oracle(objs, 1, tmpres))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("Failed to calc hash value", K(ret));
              } else {
                int64_t val = tmpres.get_int();
                // hval = hash_mod_oracle(val, static_cast<uint64_t>(bval));
                int64_t tmphval = 0;
                if (OB_FAIL(schema::ObPartitionUtils::calc_hash_part_idx(val, bval, tmphval))) {
                  LOG_WARN("failed to calc hash part index.", K(ret));
                }
                hval = static_cast<uint64_t>(tmphval);
              }
            } else {
              hval = ObExprFuncPartOldHash::calc_hash_value_with_seed(objs[0], sval);
              hval = hval % bval;
            }
            if (OB_SUCC(ret)) {
              if (OB_SUCC(numhash.from(hval, *(expr_ctx.calc_buf_)))) {
                result.set_number(numhash);
              } else {
                LOG_WARN("set result number failed.", K(ret));
              }
            } else {
              // do nothing;
            }
          }
        } else if (ObExprFuncPartHash::is_virtual_part_for_oracle(expr_ctx.exec_ctx_->get_task_executor_ctx())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support for virtual tables.", K(ret));
        } else {
          bval = bval + 1;  // consistent with oracle
          if (0 == sval) {  // When seed is 0, it needs to be consistent with the result of partition by
            ObObj tmpres;
            if (OB_FAIL(ObExprFuncPartHash::calc_value_for_oracle(objs, 1, tmpres))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Failed to calc hash value", K(ret));
            } else {
              int64_t val = tmpres.get_int();
              // hval = hash_mod_oracle(val, static_cast<uint64_t>(bval));
              int64_t tmphval = 0;
              if (OB_FAIL(schema::ObPartitionUtils::calc_hash_part_idx(val, bval, tmphval))) {
                LOG_WARN("failed to calc hash part index.", K(ret));
              }
              hval = static_cast<uint64_t>(tmphval);
            }
          } else {
            hval = ObExprFuncPartHash::calc_hash_value_with_seed(objs[0], sval);
            hval = hval % bval;
          }
          if (OB_SUCC(ret)) {
            if (OB_SUCC(numhash.from(hval, *(expr_ctx.calc_buf_)))) {
              result.set_number(numhash);
            } else {
              LOG_WARN("set result number failed.", K(ret));
            }
          } else {
            // do nothing;
          }
        }
      } else {
        // do nothing;
      }
    }
  }
  return ret;
}

int ObExprOrahash::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  switch (param_num) {
    case 1:
      if (OB_UNLIKELY(types[0].is_lob() || types[0].is_ext())) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("inconsistent type", K(ret));
      }
      break;
    case 2:
      if (!(types[1].is_numeric_type() || types[1].is_null())) {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("inconsistent type", K(ret), K(param_num));
      }
      break;
    case 3:
      if (!((types[1].is_numeric_type() || types[1].is_null()) && (types[2].is_numeric_type() || types[2].is_null()))) {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("inconsistent type", K(ret), K(param_num));
      }
      break;
    default:
      if (param_num < 1) {
        ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
        LOG_WARN("not enough arguments.", K(ret));
      } else {
        ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
        LOG_WARN("Too many arguments.", K(ret));
      }
      break;
  }
  if (OB_SUCC(ret)) {
    type.set_number();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].precision_);
    type.set_calc_type(common::ObNumberType);
  }
  const ObSQLSessionInfo* session = type_ctx.get_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret) && session->use_static_typing_engine()) {
    if (1 < param_num && !types[1].is_null()) {
      types[1].set_calc_type(ObIntType);
    }
    if (2 < param_num && !types[2].is_null()) {
      types[2].set_calc_type(ObIntType);
    }
  }
  return ret;
}

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

  uint64_t part_id = val % N;
  if (part_id + N < buckets && (val & N) == N) {
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

int ObExprOrahash::get_int64_value(const ObObj& obj, ObExprCtx& expr_ctx, int64_t& val) const
{
  int ret = OB_SUCCESS;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  EXPR_GET_INT64_V2(obj, val);
  if (OB_FAIL(ret)) {
    LOG_WARN("get int64 failed.", K(ret), K(val), K(obj));
    if (ret == OB_DATA_OUT_OF_RANGE) {
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

bool ObExprOrahash::is_any_null(const ObObj* objs, const int64_t num) const
{
  bool ret = false;
  for (int i = 0; i < num; i++) {
    ret = ret || objs[i].is_null();
  }
  return ret;
}

int ObExprOrahash::eval_orahash(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
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
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3000) {
        OV(!ObExprFuncPartOldHash::is_virtual_part_for_oracle(ctx.exec_ctx_.get_task_executor_ctx()), OB_NOT_SUPPORTED);
      } else {
        OV(!ObExprFuncPartHash::is_virtual_part_for_oracle(ctx.exec_ctx_.get_task_executor_ctx()), OB_NOT_SUPPORTED);
      }
      if (OB_SUCC(ret)) {
        uint64_t hval = 0;
        bval = bval + 1;  // consistent with oracle
        ObExpr mock_expr = expr;
        mock_expr.arg_cnt_ = 1;
        ObDatum mock_res;
        int64_t hval_int = 0;
        mock_res.pack_ = sizeof(hval_int);
        mock_res.int_ = &hval_int;
        if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3000) {
          if (OB_FAIL(ObExprFuncPartOldHash::eval_oracle_old_part_hash(mock_expr, ctx, mock_res, sval))) {
            LOG_WARN("eval_hash_val failed", K(ret));
          }
        } else if (OB_FAIL(ObExprFuncPartHash::eval_oracle_part_hash(mock_expr, ctx, mock_res, sval))) {
          LOG_WARN("eval_hash_val failed", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (0 == sval) {
            hval_int = std::abs(hval_int);
            int64_t tmp_hval = 0;
            if (OB_FAIL(schema::ObPartitionUtils::calc_hash_part_idx(hval_int, bval, tmp_hval))) {
              LOG_WARN("failed to calc hash part index.", K(ret));
            } else {
              hval = static_cast<uint64_t>(tmp_hval);
            }
          } else {
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

int ObExprOrahash::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  expr.eval_func_ = eval_orahash;
  return ret;
}
