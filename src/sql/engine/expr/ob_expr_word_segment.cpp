/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "sql/engine/expr/ob_expr_word_segment.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprWordSegment::ObExprWordSegment(ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_WORD_SEGMENT, N_WORD_SEGMENT, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprWordSegment::calc_result_typeN(ObExprResType &type,
                                         ObExprResType *types,
                                         int64_t param_num,
                                         ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 1) || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for fulltext expr", K(ret), K(param_num), KP(types));
  } else {
    ObLength max_len = 0;
    for (int64_t i = 0; i < param_num; ++i) {
      max_len += types[i].get_length();
    }
    type.set_varchar();
    type.set_length(max_len);
    type.set_collation_type(types[0].get_collation_type());
  }
  return ret;
}

int ObExprWordSegment::calc_resultN(ObObj &result,
                                    const ObObj *objs_array,
                                    int64_t param_num,
                                    ObExprCtx &expr_ctx) const
{
  return OB_NOT_SUPPORTED;
}

int ObExprWordSegment::cg_expr(
    ObExprCGCtx &cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(cg_ctx);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 1) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(rt_expr.arg_cnt_), KP(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = generate_fulltext_column;
  }
  return ret;
}

/*static*/ int ObExprWordSegment::generate_fulltext_column(
    const ObExpr &raw_ctx,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  const ObCharsetInfo *cs = nullptr;
  if (OB_UNLIKELY(raw_ctx.arg_cnt_ <= 0) || OB_ISNULL(raw_ctx.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(raw_ctx), KP(raw_ctx.args_));
  } else if (OB_ISNULL(cs = ObCharset::get_charset(raw_ctx.args_[0]->obj_meta_.get_collation_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, charset info is nullptr", K(ret), KP(cs), K(raw_ctx.args_[0]->obj_meta_));
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(eval_ctx);
    int64_t res_str_len = 0;
    ObSEArray<ObString, 1> ft_parts;
    const int64_t mb_max_len = cs->mbmaxlen;
    char mb_separator[mb_max_len];
    int32_t length_of_separator = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_ctx.arg_cnt_; ++i) {
      ObString res;
      common::ObDatum *datum = nullptr;
      if (OB_ISNULL(raw_ctx.args_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, nullptr", K(ret), KP(raw_ctx.args_[i]), K(i), K(raw_ctx.arg_cnt_));
      } else if (OB_FAIL(raw_ctx.args_[i]->eval(eval_ctx, datum))) {
        LOG_WARN("fail to eval expr", K(ret), K(raw_ctx), K(eval_ctx));
      } else if (OB_ISNULL(datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, datum is nullptr", K(ret), KP(datum));
      } else if (FALSE_IT(res = datum->get_string())) {
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(alloc_guard.get_allocator(), *datum, raw_ctx.args_[i]->datum_meta_,
              raw_ctx.args_[i]->obj_meta_.has_lob_header(), res))) {
        LOG_WARN("fail to get real data.", K(ret), K(res));
      } else if (OB_FAIL(ft_parts.push_back(res))) {
        LOG_WARN("fail to push back ft part array", K(ret), K(res));
      } else {
        res_str_len += ft_parts.at(ft_parts.count() - 1).length();
      }
    }
    if (OB_SUCC(ret)) {
      wchar_t wide_char = L' ';
      if (OB_FAIL(ObCharset::wc_mb(raw_ctx.args_[0]->obj_meta_.get_collation_type(), wide_char, mb_separator,
              mb_max_len, length_of_separator))) {
        LOG_WARN("fail to wc_mb", K(ret), K(mb_max_len), KPHEX(mb_separator, mb_max_len));
      } else {
        res_str_len = res_str_len + length_of_separator * (ft_parts.count() - 1);
      }
    }
    if (OB_SUCC(ret)) {
      ObExprStrResAlloc res_alloc(raw_ctx, eval_ctx);
      char *ptr = static_cast<char*>(res_alloc.alloc(res_str_len));
      if (OB_UNLIKELY(NULL == ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(res_str_len));
      } else {
        char* cur_ptr = ptr;
        for (int64_t i = 0; OB_SUCC(ret) && i < ft_parts.count(); ++i) {
          if (0 != i) {
            MEMCPY(cur_ptr, mb_separator, length_of_separator);
            cur_ptr += length_of_separator;
          }
          MEMCPY(cur_ptr, ft_parts.at(i).ptr(), ft_parts.at(i).length());
          cur_ptr += ft_parts.at(i).length();
        }
        if (OB_SUCC(ret)) {
          ObString str(res_str_len, ptr);
          expr_datum.set_string(str);
          LOG_INFO("generate fulltext column", K(str), K(raw_ctx), K(eval_ctx), K(expr_datum));
        }
      }
    }
  }
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
