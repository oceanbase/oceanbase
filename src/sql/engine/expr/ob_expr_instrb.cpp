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
#include "sql/engine/expr/ob_expr_instrb.h"
#include "sql/engine/expr/ob_expr_instr.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{

ObExprInstrb::ObExprInstrb(ObIAllocator &alloc)
    : ObLocationExprOperator(alloc, T_FUN_SYS_INSTRB, N_INSTRB, MORE_THAN_ONE, NOT_ROW_DIMENSION) {}

ObExprInstrb::~ObExprInstrb() {}

int ObExprInstrb::calc_result_type2(ObExprResType &type,
                                    ObExprResType &type1,
                                    ObExprResType &type2,
                                    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObExprOperator::calc_result_flag2(type, type1, type2);
    type.set_number();
    type.set_precision(PRECISION_UNKNOWN_YET);
    type.set_scale(NUMBER_SCALE_UNKNOWN_YET);

    ObSEArray<ObExprResType*, 2, ObNullAllocator> params;
    OZ(params.push_back(&type1));
    ObExprResType tmp_type;
    OZ(aggregate_string_type_and_charset_oracle(*session, params, tmp_type));
    OZ(params.push_back(&type2));
    OZ(deduce_string_param_calc_type_and_charset(*session, tmp_type, params));
  }
  return ret;
}

int ObExprInstrb::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *type_array,
                                    int64_t param_num,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 2 || param_num > 4)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param num is invalid", K(ret), K(param_num));
  } else if (OB_ISNULL(type_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type array is null", K(ret));
  } else if (OB_FAIL(calc_result_type2(type, type_array[0], type_array[1], type_ctx))) {
    LOG_WARN("fail calc result type", K(param_num), K(ret));
  } else {
    if (3 == param_num) {
      type_array[2].set_calc_type(ObNumberType); // position
    } else if (4 == param_num) {
      type_array[2].set_calc_type(ObNumberType); // position
      type_array[3].set_calc_type(ObNumberType); // occurrence
    }
  }
  return ret;
}

static int calc_instrb_text(ObTextStringIter &haystack_iter,
                            ObTextStringIter &needle_iter,
                            ObIAllocator &calc_alloc,
                            ObExprKMPSearchCtx *kmp_ctx,
                            ObEvalCtx &ctx,
                            int64_t &pos_int,
                            int64_t &occ_int,
                            ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObString haystack_data;
  ObString needle_data;
  int64_t ret_idx = -1;
  if (OB_FAIL(haystack_iter.init(0, NULL, &calc_alloc))) {
    LOG_WARN("init haystack_iter failed ", K(ret), K(haystack_iter));
  } else if (OB_FAIL(needle_iter.init(0, NULL, &calc_alloc))) {
    LOG_WARN("init needle_iter failed ", K(ret), K(needle_iter));
  } else if (OB_FAIL(needle_iter.get_full_data(needle_data))) {
    LOG_WARN("init lob str iter failed ", K(ret), K(needle_iter));
  } else if (OB_FAIL(kmp_ctx->init(needle_data, pos_int < 0, ctx.exec_ctx_.get_allocator()))) {
    LOG_WARN("init kmp ctx failed", K(ret), K(needle_data));
  } else {
    ObTextStringIterState state;
    size_t needle_byte_len = needle_data.length();
    if (haystack_iter.is_outrow_lob()) {
      haystack_iter.set_reserved_byte_len(needle_byte_len - 1);
      if (pos_int > 0) {
        haystack_iter.set_start_offset(pos_int - 1); // start char len
        pos_int = 1; // start pos is handled by lob mngr for out row lobs
      } else {
        haystack_iter.set_backward(); // search backward if pos < 0
      }
    }
    // search one block each loop
    bool not_first_search = false;
    if (pos_int > 0) {
      int64_t count = 0;
      while (OB_SUCC(ret) && count < occ_int &&
             (state = haystack_iter.get_next_block(haystack_data)) == TEXTSTRING_ITER_NEXT) {
        if (not_first_search) {
          ret_idx = -1;
          pos_int = 1;
        }
        for (; count < occ_int && OB_SUCC(ret); ++count) {
          if (OB_FAIL(kmp_ctx->instrb_search(haystack_data, pos_int, 1, ret_idx))) {
            LOG_WARN("search needle in haystack failed", K(ret), K(haystack_data), K(needle_data));
          } else {
            if (ret_idx <= 0) {
              break;
            } else {
              pos_int = ret_idx + 1;
            }
          }
        }
        not_first_search = true;
      }
      if (OB_FAIL(ret)) {
      } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
        ret = (haystack_iter.get_inner_ret() != OB_SUCCESS) ?
              haystack_iter.get_inner_ret() : OB_INVALID_DATA;
        LOG_WARN("iter state invalid", K(ret), K(state), K(haystack_iter));
      } else {
        if (ret_idx != 0) { // add accessed length by get_next_block()
          ret_idx += haystack_iter.get_last_accessed_byte_len() + haystack_iter.get_start_offset();
          if (haystack_iter.get_iter_count() > 1) { // minus reserved length
            OB_ASSERT(ret_idx > haystack_iter.get_reserved_byte_len());
            ret_idx -= haystack_iter.get_reserved_byte_len();
          }
        }
      }
    } else {
      int64_t total_byte_len = 0;
      int64_t max_access_byte_len = 0;
      if (OB_FAIL(haystack_iter.get_byte_len(total_byte_len))) {
        LOG_WARN("get haystack char len failed", K(ret), K(state));
      } else if (haystack_iter.is_outrow_lob()) {
        max_access_byte_len = total_byte_len + pos_int + 1;
        haystack_iter.set_start_offset(-pos_int - 1); // start char len
        pos_int = -1;
      } else {
        max_access_byte_len = total_byte_len;
      }
      int64_t count = 0;
      while (OB_SUCC(ret)
              && count < occ_int
              && (state = haystack_iter.get_next_block(haystack_data)) == TEXTSTRING_ITER_NEXT) {
        if (not_first_search) {
          ret_idx = -1;
          pos_int = -1;
        }
        for (; count < occ_int && OB_SUCC(ret); ++count) {
          if (OB_FAIL(kmp_ctx->instrb_search(haystack_data, pos_int, 1, ret_idx))) {
            LOG_WARN("search needle in haystack failed", K(ret), K(haystack_data), K(needle_data));
          } else {
            if (ret_idx <= 0) {
              break;
            } else { // Notice: negative pos
              pos_int = -1 -(static_cast<int64_t>(haystack_iter.get_accessed_len()) - ret_idx + 1);
            }
          }
        }
        not_first_search = true;
      }
      if (OB_FAIL(ret)) {
      } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
        ret = (haystack_iter.get_inner_ret() != OB_SUCCESS) ?
              haystack_iter.get_inner_ret() : OB_INVALID_DATA;
        LOG_WARN("iter state invalid", K(ret), K(state), K(haystack_iter));
      } else {
        if (ret_idx != 0) { // add accessed length by get_next_block()
          ret_idx += (max_access_byte_len - static_cast<int64_t>(haystack_iter.get_accessed_byte_len()));
        }
      }
    }
    number::ObNumber res_nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(res_nmb.from(ret_idx, tmp_alloc))) {
      LOG_WARN("int64_t to ObNumber failed", K(ret), K(ret_idx));
    } else {
      res_datum.set_number(res_nmb);
    }
  }
  return ret;
}

int calc_instrb_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  bool is_null = false;
  ObDatum *haystack = NULL;
  ObDatum *needle = NULL;
  int64_t pos_int = 0;
  int64_t occ_int = 0;
  number::ObNumber res_nmb;
  ObCollationType calc_cs_type = CS_TYPE_INVALID;
  const ObObjType haystack_type = expr.args_[0]->datum_meta_.type_;
  const ObObjType needle_type = expr.args_[1]->datum_meta_.type_;
  bool haystack_has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
  bool needle_has_lob_header = expr.args_[1]->obj_meta_.has_lob_header();
  if (OB_FAIL(ObExprOracleInstr::calc_oracle_instr_arg(expr, ctx, is_null,
                                                       haystack, needle,
                                                       pos_int, occ_int, calc_cs_type))) {
    LOG_WARN("calc_instrb_arg failed", K(ret));
  } else if (is_null) {
    res_datum.set_null();
  } else if (OB_UNLIKELY(0 == haystack->get_string().length() || ob_is_empty_lob(haystack_type, *haystack, haystack_has_lob_header))) {
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(res_nmb.from((uint64_t)(0), tmp_alloc))) {
      LOG_WARN("get number from int failed", K(ret));
    } else {
      res_datum.set_number(res_nmb);
    }
  } else if (OB_ISNULL(haystack) || OB_ISNULL(needle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("haystack or needle is NULL", K(ret), KP(haystack), KP(needle));
  } else if (OB_UNLIKELY(0 >= occ_int)) {
    ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
    LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, occ_int);
  } else if (OB_UNLIKELY(0 == pos_int)) {
    res_nmb.set_zero();
    res_datum.set_number(res_nmb);
  } else {
    const ObString& haystack_str = haystack->get_string();
    const ObString& needle_str = needle->get_string();
    int64_t ret_idx = -1;
    ObNumStackOnceAlloc tmp_alloc;
    ObExprKMPSearchCtx *kmp_ctx = NULL;
    const uint64_t op_id = static_cast<uint64_t>(expr.expr_ctx_id_);
    if (OB_ISNULL(needle_str.ptr()) || ob_is_empty_lob(needle_type, *needle, needle_has_lob_header)) {
      ret = OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN;
      LOG_WARN("ptr is null", K(needle));
    } else if (OB_UNLIKELY(0 == needle_str.length() || ob_is_empty_lob(needle_type, *needle, needle_has_lob_header))) {
      if (OB_FAIL(res_nmb.from((uint64_t)(0), tmp_alloc))) {
        LOG_WARN("get number from int failed", K(ret));
      } else {
        res_datum.set_number(res_nmb);
      }
    } else if (OB_FAIL(ObExprKMPSearchCtx::get_kmp_ctx_from_exec_ctx(ctx.exec_ctx_, op_id, kmp_ctx))) {
      LOG_WARN("get kmp ctx failed", K(ret));
    } else if (!ob_is_text_tc(haystack_type) && !ob_is_text_tc(needle_type)) {
      if (OB_FAIL(kmp_ctx->init(needle_str, pos_int < 0, ctx.exec_ctx_.get_allocator()))) {
        LOG_WARN("init kmp ctx failed", K(ret), K(needle_str));
      } else if (OB_FAIL(kmp_ctx->instrb_search(haystack_str, pos_int, occ_int, ret_idx))) {
        LOG_WARN("search needle in haystack failed", K(ret), K(haystack_str), K(needle_str));
      } else if (OB_FAIL(res_nmb.from(ret_idx, tmp_alloc))) {
        LOG_WARN("int64_t to ObNumber failed", K(ret), K(ret_idx));
      } else {
        res_datum.set_number(res_nmb);
      }
    } else { // text types
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      // similar to instr, but reserve length is byte len, use binary cs type for byte access
      ObTextStringIter haystack_iter(haystack_type, CS_TYPE_BINARY, haystack->get_string(), haystack_has_lob_header);
      ObTextStringIter needle_iter(needle_type, calc_cs_type, needle->get_string(), needle_has_lob_header);

      if (OB_FAIL(calc_instrb_text(haystack_iter, needle_iter, calc_alloc,
                                   kmp_ctx, ctx, pos_int, occ_int, res_datum))) {
        LOG_WARN("failed to calc_instrb_text", K(ret));
      }
    }
  }
  return ret;
}

int ObExprInstrb::calc_instrb_expr_batch(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObExprKMPSearchCtx *kmp_ctx = NULL;
  const uint64_t op_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  ObCollationType calc_cs_type = CS_TYPE_INVALID;
  const ObObjType haystack_type = expr.args_[0]->datum_meta_.type_;
  const ObObjType needle_type = expr.args_[1]->datum_meta_.type_;
  bool haystack_has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
  bool needle_has_lob_header = expr.args_[1]->obj_meta_.has_lob_header();
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_0 failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_1 failed", K(ret));
  } else if (expr.arg_cnt_ > 2 && OB_FAIL(expr.args_[2]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_2 failed", K(ret));
  } else if (expr.arg_cnt_ == 4 && OB_FAIL(expr.args_[3]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_3 failed", K(ret));
  } else if (OB_FAIL(ObLocationExprOperator::get_calc_cs_type(expr, calc_cs_type))) {
    LOG_WARN("get_calc_cs_type failed", K(ret));
  } else if (OB_FAIL(ObExprKMPSearchCtx::get_kmp_ctx_from_exec_ctx(ctx.exec_ctx_,
                                                                   op_id, kmp_ctx))) {
    LOG_WARN("get kmp ctx failed", K(ret));
  } else {
    ObDatum *res_datum = expr.locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;;
      }
      ObDatum &haystack = expr.args_[0]->locate_expr_datum(ctx, i);
      ObDatum &needle = expr.args_[1]->locate_expr_datum(ctx, i);
      bool is_null = false;
      int64_t pos_int = 1;
      int64_t occ_int = 1;
      number::ObNumber res_nmb;
      if (haystack.is_null() || needle.is_null()) {
        is_null = true;
      }
      if (!is_null && (3 == expr.arg_cnt_ || 4 == expr.arg_cnt_)) {
        ObDatum &pos = expr.args_[2]->locate_expr_datum(ctx, i);
        if (pos.is_null()) {
          is_null = true;
        } else {
          number::ObNumber pos_nmb(pos.get_number());
          if (OB_FAIL(ObExprUtil::trunc_num2int64(pos_nmb, pos_int))) {
            LOG_WARN("trunc_num2int64 failed", K(ret));
          } else if (INT64_MIN == pos_int) {
            pos_int = INT64_MAX;
          }
        }
      }
      if (OB_SUCC(ret) && !is_null && 4 == expr.arg_cnt_) {
        ObDatum &occ = expr.args_[3]->locate_expr_datum(ctx, i);
        if (occ.is_null()) {
          is_null = true;
        } else {
          number::ObNumber occ_nmb(occ.get_number());
          if (OB_FAIL(ObExprUtil::trunc_num2int64(occ_nmb, occ_int))) {
            LOG_WARN("trunc_num2int64 failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(is_null)) {
        res_datum[i].set_null();
      } else if (OB_UNLIKELY(0 == haystack.get_string().length() || ob_is_empty_lob(haystack_type, haystack, haystack_has_lob_header))) {
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(res_nmb.from((uint64_t)(0), tmp_alloc))) {
          LOG_WARN("get number from int failed", K(ret));
        } else {
          res_datum[i].set_number(res_nmb);
        }
      } else if (OB_UNLIKELY(0 >= occ_int)) {
        ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
        LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, occ_int);
      } else if (OB_UNLIKELY(0 == pos_int)) {
        res_nmb.set_zero();
        res_datum[i].set_number(res_nmb);
      } else {
        const ObString& haystack_str = haystack.get_string();
        const ObString& needle_str = needle.get_string();
        int64_t ret_idx = -1;
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_ISNULL(needle_str.ptr()) || ob_is_empty_lob(needle_type, needle, needle_has_lob_header)) {
          ret = OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN;
          LOG_WARN("ptr is null", K(needle_str));
        } else if (OB_UNLIKELY(0 == needle_str.length() || ob_is_empty_lob(needle_type, needle, needle_has_lob_header))) {
          if (OB_FAIL(res_nmb.from((uint64_t)(0), tmp_alloc))) {
            LOG_WARN("get number from int failed", K(ret));
          } else {
            res_datum[i].set_number(res_nmb);
          }
        } else if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
          if (OB_FAIL(kmp_ctx->init(needle_str, pos_int < 0, ctx.exec_ctx_.get_allocator()))) {
            LOG_WARN("init kmp ctx failed", K(ret), K(needle_str));
          } else if (OB_FAIL(kmp_ctx->instrb_search(haystack_str, pos_int, occ_int, ret_idx))) {
            LOG_WARN("search needle in haystack failed", K(ret), K(haystack_str), K(needle_str));
          } else if (OB_FAIL(res_nmb.from(ret_idx, tmp_alloc))) {
            LOG_WARN("int64_t to ObNumber failed", K(ret), K(ret_idx));
          } else {
            res_datum[i].set_number(res_nmb);
          }
        } else { // text types
          // similar to instr, but reserve length is byte len, use binary cs type for byte access
          ObString haystack_data;
          ObString needle_data;
          ObEvalCtx::TempAllocGuard alloc_guard(ctx);
          ObIAllocator &calc_alloc = alloc_guard.get_allocator();
          ObTextStringIter haystack_iter(haystack_type, CS_TYPE_BINARY, haystack.get_string(), haystack_has_lob_header);
          ObTextStringIter needle_iter(needle_type, calc_cs_type, needle.get_string(), needle_has_lob_header);

          if (OB_FAIL(calc_instrb_text(haystack_iter, needle_iter, calc_alloc,
                                       kmp_ctx, ctx, pos_int, occ_int, res_datum[i]))) {
            LOG_WARN("failed to calc_instrb_text", K(ret));
          }
        }
      }
      eval_flags.set(i);
    }
  }
  return ret;
}

int ObExprInstrb::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 2 || rt_expr.arg_cnt_ > 4)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret), K(rt_expr.arg_cnt_));
  }
  rt_expr.eval_func_ = calc_instrb_expr;
  rt_expr.eval_batch_func_ = ObExprInstrb::calc_instrb_expr_batch;
  return ret;
}

} //namespace sql
} //namespace oceanbase
