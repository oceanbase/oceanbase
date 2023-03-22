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
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_fixed_array_iterator.h"
#include "lib/oblog/ob_log.h"
#include "ob_expr_instr.h"
#include "ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{

ObExprInstr::ObExprInstr(ObIAllocator &alloc)
    : ObLocationExprOperator(alloc, T_FUN_SYS_INSTR, N_INSTR, 2, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

ObExprInstr::~ObExprInstr() {}

int ObExprInstr::calc_mysql_instr_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != expr.arg_cnt_) || OB_ISNULL(expr.args_) ||
      OB_ISNULL(expr.args_[0]) || OB_ISNULL(expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expr", K(ret), K(expr));
  } else if (OB_FAIL(ObLocationExprOperator::calc_(expr, *expr.args_[1], *expr.args_[0],
                                                   ctx, res_datum))) {
    LOG_WARN("ObLocationExprOperator::calc_ faied", K(ret));
  }
  return ret;
}

int ObExprInstr::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_mysql_instr_expr;
  return OB_SUCCESS;
}

/***** oracle ******/

ObExprOracleInstr::ObExprOracleInstr(ObIAllocator &alloc)
    : ObLocationExprOperator(alloc, T_FUN_SYS_INSTR, N_INSTR, MORE_THAN_ONE, NOT_ROW_DIMENSION) {}

ObExprOracleInstr::~ObExprOracleInstr() {}

int ObExprOracleInstr::calc_result_type2(ObExprResType &type,
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
    // 如果两个参数的cs type不一致，计算的结果是有问题的
    // 虽然这里会对参数的cs type进行转换，但是instr返回的结果是以字符为单位的，cs type的转换
    // 不会影响instr的结果
    OZ(params.push_back(&type2));
    OZ(deduce_string_param_calc_type_and_charset(*session, tmp_type, params));
  }
  return ret;
}

int ObExprOracleInstr::calc_result_typeN(ObExprResType &type,
                                         ObExprResType *type_array,
                                         int64_t param_num,
                                         ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 2 || param_num > 4)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param num is invalid", K(ret), K(param_num));
  } else if (OB_ISNULL(type_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type array is null", K(ret), K(type_array));
  } else if (OB_FAIL(calc_result_type2(type, type_array[0], type_array[1], type_ctx))) {
    LOG_WARN("fail calc result type", K(param_num), K(ret));
  } else {
    // 如果用户指定了 position、occurrence，则转换成 Number
    // 如果没有指定，则在运行时用一个 int 指定为默认值 1
    if (3 == param_num) {
      type_array[2].set_calc_type(ObNumberType); // position
    } else if (4 == param_num) {
      type_array[2].set_calc_type(ObNumberType); // position
      type_array[3].set_calc_type(ObNumberType); // occurrence
    }
  }
  return ret;
}

static int calc_oracle_instr_text(ObTextStringIter &haystack_iter,
                                  ObTextStringIter &needle_iter,
                                  ObIAllocator &calc_alloc,
                                  const ObCollationType &calc_cs_type,
                                  int64_t &pos_int,
                                  int64_t &occ_int,
                                  uint32_t &idx)
{
  int ret = OB_SUCCESS;
  ObString haystack_data;
  ObString needle_data;
  int64_t needle_char_len = 0;
  int64_t haystack_char_len = 0;
  int64_t abs_pos_int = (pos_int > 0) ? (pos_int) : (-pos_int);
  if (OB_FAIL(haystack_iter.init(0, NULL, &calc_alloc))) {
    LOG_WARN("init haystack_iter failed ", K(ret), K(haystack_iter));
  } else if (OB_FAIL(haystack_iter.get_char_len(haystack_char_len))) {
    LOG_WARN("get haystack char len failed ", K(ret), K(haystack_iter));
  } else if (OB_FAIL(needle_iter.init(0, NULL, &calc_alloc))) {
    LOG_WARN("init needle_iter failed ", K(ret), K(needle_iter));
  } else if (OB_FAIL(needle_iter.get_full_data(needle_data))) {
    LOG_WARN("get needle data failed ", K(ret), K(needle_iter));
  } else if (OB_FAIL(needle_iter.get_char_len(needle_char_len))) {
    LOG_WARN("get needle char len failed ", K(ret), K(needle_iter));
  } else if ((needle_char_len < 1) ||
             (haystack_char_len - abs_pos_int + 1 < needle_char_len)) {
    // pattern is empty string, just return zero
    // pattern length is bigger than content, just return zero
    idx = 0;
  } else {
    if (haystack_iter.is_outrow_lob()) {
      haystack_iter.set_reserved_len(static_cast<size_t>(needle_char_len) - 1);
      if (pos_int > 0) {
        haystack_iter.set_start_offset(pos_int - 1); // start char len
        pos_int = 1; // start pos is handled by lob mngr for out row lobs
      } else {
        haystack_iter.set_backward();
      }
    }

    idx = 0;
    bool not_first_search = false;
    ObTextStringIterState state;
    if (pos_int > 0) {
      int64_t count = 0;
      while (count < occ_int && (state = haystack_iter.get_next_block(haystack_data)) == TEXTSTRING_ITER_NEXT) {
        if (not_first_search) {
          idx = 0;
          pos_int = 1;
        }
        for (; count < occ_int; ++count) {
          idx = ObCharset::locate(calc_cs_type, haystack_data.ptr(), haystack_data.length(),
                                  needle_data.ptr(), needle_data.length(), pos_int);
          if (idx <= 0) {
            break;
          } else {
            pos_int = idx + 1;
          }
        }
        not_first_search = true;
      }
      if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
        ret = (haystack_iter.get_inner_ret() != OB_SUCCESS) ?
              haystack_iter.get_inner_ret() : OB_INVALID_DATA;
        LOG_WARN("iter state invalid", K(ret), K(state), K(haystack_iter));
      } else {
        if (idx != 0) {
          // 需要加上get next block实际访问过的长度
          idx += haystack_iter.get_last_accessed_len() + haystack_iter.get_start_offset();
          if (haystack_iter.get_iter_count() > 1) { // minus reserved length
            OB_ASSERT(idx > haystack_iter.get_reserved_char_len());
            idx -= haystack_iter.get_reserved_char_len();
          }
        }
      }
    } else { // case pos_int < 0; pos_int == 0 handled by the outside caller
      int count = 0;
      int64_t total_char_len = 0;
      int64_t max_access_len = 0;
      bool access_inrow_lob_prefix = false; // only access prefix of an inrow lob
      if (OB_FAIL(haystack_iter.get_char_len(total_char_len))) {
        LOG_WARN("get haystack char len failed", K(ret), K(state));
      } else if (haystack_iter.is_outrow_lob()) {
        max_access_len = total_char_len + pos_int + 1;
        haystack_iter.set_start_offset(-pos_int - 1); // start char len
        pos_int = -1;
      } else { // inrow case
        max_access_len = total_char_len + pos_int + 1;
        if (pos_int < -1) {
          pos_int = -1;
          access_inrow_lob_prefix = true;
        }
      }
      while (OB_SUCC(ret)
              && count < occ_int
              && (state = haystack_iter.get_next_block(haystack_data)) == TEXTSTRING_ITER_NEXT) {
        if (not_first_search) {
          idx = 0;
          pos_int = -1;
        } else {
          // compat with oracle, same content has different result in varchar or clob
          // example:
          // create table test_clob(v1 clob);
          // insert into test_clob values(rpad('a',4000,'r'));
          // select instr(v1,'rrrrrrr',-1,1000) from test_clob; // 2995
          // select instr(v1,'rrrrrrr',-6,1000) from test_clob; // 2990
          // However, if v1 type is varchar, result of both select stmt is 2995
          if (access_inrow_lob_prefix) {
            uint32 accessed_byte_len = ObCharset::charpos(calc_cs_type,
                                                          haystack_data.ptr(),
                                                          haystack_data.length(),
                                                          max_access_len);
            haystack_data.assign_ptr(haystack_data.ptr(), accessed_byte_len);
          }
        }
        for (; count < occ_int && OB_SUCC(ret); ++count) {
          if (OB_FAIL(ObExprOracleInstr::slow_reverse_search(calc_alloc, calc_cs_type, haystack_data,
                                                             needle_data, pos_int, 1, idx))) {
            LOG_WARN("slow_reverse_search failed", K(ret), K(calc_cs_type),
                      K(haystack_data), K(needle_data), K(pos_int), K(occ_int));
          } else if (idx <= 0) {
            break;
          } else {
            // Notice: negative pos
            if (access_inrow_lob_prefix) {
              pos_int = -1 -(max_access_len - idx + 1);
            } else {
              pos_int = -1 -(static_cast<int64_t>(haystack_iter.get_accessed_len()) - idx + 1);
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
        if (idx != 0 && !access_inrow_lob_prefix) {
          // need to count accessed length by get_next_block
          idx += (max_access_len - static_cast<int64_t>(haystack_iter.get_accessed_len()));
        }
      }
    }
  }
  return ret;
}

int ObExprOracleInstr::calc(ObObj &result,
                            const ObObj &haystack,
                            const ObObj &needle,
                            const ObObj &position,
                            const ObObj &occurrence,
                            ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expr_ctx.calc_buf_ is NULL", K(ret));
  } else if (OB_UNLIKELY(haystack.is_null() ||
                  needle.is_null() ||
                  position.is_null() ||
                  occurrence.is_null())) {
    result.set_null();
  } else if (OB_UNLIKELY(!is_type_valid(haystack.get_type())
                         || !is_type_valid(needle.get_type())
                         || !is_type_valid(position.get_type())
                         || !is_type_valid(occurrence.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the param is not castable", K(haystack), K(needle), K(position),
                                          K(occurrence), K(ret));
  } else {
    int64_t pos = 0;
    int64_t occ = 0;
    const ObCollationType &calc_cs_type = (haystack.get_collation_type() ==
                                           needle.get_collation_type()) ?
                                    haystack.get_collation_type() : CS_TYPE_INVALID;
    if (OB_UNLIKELY(!ObCharset::is_valid_collation(static_cast<int64_t>(calc_cs_type)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("calc cs_type is invalid", K(ret), K(calc_cs_type));
    } else if (OB_FAIL(ObExprUtil::get_trunc_int64(position, expr_ctx, pos))) {
      LOG_WARN("fail get position", K(position), K(ret));
    } else if (OB_FAIL(ObExprUtil::get_trunc_int64(occurrence, expr_ctx, occ))) {
      LOG_WARN("fail get occurrence", K(occurrence), K(ret));
    } else if (OB_UNLIKELY(occ <= 0)) {
      ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, occ);
    } else if (OB_UNLIKELY(pos == 0)) {
      // 这里是为了兼容 Oracle 行为，Oracle 对于 pos = 0，总返回 0
      // 例外：select instr('abd','b',0,0) from dual;
      // 这种情况下因为 acc=0 ，上面已经报错，不返回 0
      number::ObNumber num;
      num.from(static_cast<int64_t>(0), *(expr_ctx.calc_buf_));
      result.set_number(num);
    } else {
      uint32_t idx = 0;
      ObString str1;
      ObString str2 = needle.get_string();
      if (haystack.is_lob_locator()) {
        if (OB_ISNULL(haystack.get_lob_locator())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("lob locator is null", K(ret));
        } else {
          const ObString lob_str(
                static_cast<int64_t>(haystack.get_lob_locator()->get_payload_length()),
                haystack.get_lob_locator()->get_payload_ptr());
          str1 = lob_str;
        }
      } else {
        str1 = haystack.get_string();
      }

      const ObObjType haystack_type = haystack.get_type();
      const ObObjType needle_type = needle.get_type();
      if (OB_FAIL(ret)) {
      } else if (!ob_is_text_tc(haystack_type) && !ob_is_text_tc(needle_type)) {
        if (pos > 0) {
          for (int64_t i = 0; i < occ; ++i) {
            idx = ObCharset::locate(calc_cs_type, str1.ptr(), str1.length(), str2.ptr(),
                                    str2.length(), pos);
            if (idx <= 0) {
              break;
            } else {
              pos = idx + 1;
            }
          }
        } else {
          if (OB_FAIL(slow_reverse_search(*(expr_ctx.calc_buf_), calc_cs_type, str1,
                                          str2, pos, occ, idx))) {
            LOG_WARN("slow_reverse_search failed", K(ret), K(str1), K(str2), K(occ));
          }
        }
      } else { // at least one of the inputs is text tc
        ObTextStringIter haystack_iter(haystack_type, calc_cs_type, str1, haystack.has_lob_header());
        ObTextStringIter needle_iter(needle_type, calc_cs_type, str2, needle.has_lob_header());
        if (OB_FAIL(calc_oracle_instr_text(haystack_iter, needle_iter,
                                           *expr_ctx.calc_buf_, calc_cs_type, pos, occ, idx))) {
          LOG_WARN("calc oracle instr for text types failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        number::ObNumber num;
        if (OB_FAIL(num.from((uint64_t)idx, *(expr_ctx.calc_buf_)))) {
          LOG_WARN("get nmb from int failed", K(ret), K(idx));
        } else {
          result.set_number(num);
        }
      }
    }
  }
  return ret;
}

int ObExprOracleInstr::slow_reverse_search(ObIAllocator &alloc,
                                           const ObCollationType &cs_type,
                                           const ObString &str1,
                                           const ObString &str2,
                                           int64_t neg_start,
                                           int64_t occ,
                                           uint32_t &idx)
{
  /* TODO: 应该采用更加高效的 kmp 算法。
   * 但是，同时考虑到要支持各种字符集 (pos表示字符偏移，而不是字节偏移）
   * 所以必须使用支持字符集的 kmp 算法，有一定工作量
   *
   * kmp 算法可以参考 ObExprSubstringIndex 类中的实现，它不需要考虑
   * 字符集合collation，所以用 byte compare 的算法就足够
   *
   * 下面采用的算法是从后到前逐个比较
   * 这个算法比较低效，复杂度为 O(str2.length() * str1.length())
   */
  int ret = OB_SUCCESS;
  size_t str1_len_in_char  = ObCharset::strlen_char(cs_type, str1.ptr(), str1.length());
  size_t str2_len_in_char  = ObCharset::strlen_char(cs_type, str2.ptr(), str2.length());
  // return 0 if start_idx_in_char less than 0.
  int64_t start_idx_in_char = static_cast<int64_t>(str1_len_in_char) + neg_start;
  ObFixedArray<size_t, ObIAllocator> str1_byte_num(&alloc,  str1_len_in_char);
  ObFixedArray<size_t, ObIAllocator> str1_byte_offset(&alloc,  str1_len_in_char+1);

  if (0 <= neg_start) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("neg_start must be negative", K(ret), K(neg_start));
  } else if (0 > start_idx_in_char) {
    idx = 0;
    LOG_DEBUG("neg_start is too large, return 0", K(ret), K(idx));
  } else if (OB_FAIL(ObExprUtil::get_mb_str_info(str1, cs_type,
                                                 str1_byte_num, str1_byte_offset))) {
    LOG_WARN("get_mb_str_info failed", K(ret), K(str1), K(cs_type), K(str1_len_in_char));
  } else if (str1_byte_num.count()+1 != str1_byte_offset.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size of str1_byte_num and size of str1_byte_offset should be same",
             K(ret), K(str1_byte_num), K(str1_byte_offset));
  } else if (start_idx_in_char >= str1_byte_num.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start_idx_in_char should less than size of str1_byte_num", K(ret),
                                                                         K(start_idx_in_char),
                                                                         K(str1_byte_num));
  } else {
    // 尽可能定位到最合适的地方
    start_idx_in_char = std::min(start_idx_in_char, std::max(0L,
                                 static_cast<int64_t>(str1_len_in_char) -
                                 static_cast<int64_t>(str2_len_in_char)));
    for ( ; start_idx_in_char >= 0; --start_idx_in_char) {
      size_t cur_byte_offset = str1_byte_offset[start_idx_in_char];
      int64_t compare_len = std::min(static_cast<int64_t>(str1.length()) -
                                     static_cast<int64_t>(cur_byte_offset),
                                     static_cast<int64_t>(str2.length()));
      idx = ObCharset::instr(cs_type,
                             str1.ptr() + cur_byte_offset,
                             compare_len, /* only compare limited bytes */
                             str2.ptr(),
                             str2.length());
      if (idx > 0 && (--occ == 0)) {
        // 调整成相对于 str1 起点的偏移值
        idx = idx + static_cast<uint32_t>(cur_byte_offset);
        break;
      }
    }
    if (OB_SUCCESS == ret) {
      if (0 < occ) { // 匹配次数不足
        idx = 0;
      } else {
        if (0 != idx) {
          // ObCharset::instr()返回的idx是以1开始的
          --idx;
          typename ObFixedArray<size_t, ObIAllocator>::iterator iter =
                   std::find(str1_byte_offset.begin(), str1_byte_offset.end(), idx);
          if (iter == str1_byte_offset.end()) {
            // @shaoge 表明遇到了非法的字符
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cannot find char idx by byte idx", K(ret), K(idx), K(str1_byte_offset),
                                                         K(str1), K(str2));
          } else {
            // 最终结果要以1开始
            idx = iter - str1_byte_offset.begin() + 1;
          }
        }
      }
    }
  }
  return ret;
}

// haystack/needle没有直接传递ObString作为参数，因为想保证ObDatum结果的const属性
int ObExprOracleInstr::calc_oracle_instr_arg(const ObExpr &expr, ObEvalCtx &ctx,
                                             bool &is_null,
                                             ObDatum *&haystack, ObDatum *&needle, int64_t &pos_int,
                                             int64_t &occ_int, ObCollationType &calc_cs_type)
{
  int ret = OB_SUCCESS;
  ObDatum *pos = NULL;
  ObDatum *occ = NULL;
  is_null = false;
  pos_int = 1;
  occ_int = 1;
  if (OB_FAIL(expr.args_[0]->eval(ctx, haystack))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (haystack->is_null()) {
    is_null = true;
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, needle))) {
    LOG_WARN("eval arg 1 failed", K(ret));
  } else if (needle->is_null()) {
    is_null = true;
  }

  if (OB_SUCC(ret) && !is_null) {
    if (3 == expr.arg_cnt_ || 4 == expr.arg_cnt_) {
      if (OB_FAIL(expr.args_[2]->eval(ctx, pos))) {
        LOG_WARN("eval pos arg failed", K(ret));
      } else if (pos->is_null()) {
        is_null = true;
      } else {
        number::ObNumber pos_nmb(pos->get_number());
        if (OB_FAIL(ObExprUtil::trunc_num2int64(pos_nmb, pos_int))) {
          LOG_WARN("trunc_num2int64 failed", K(ret));
        } else if (INT64_MIN == pos_int) {
          pos_int = INT64_MAX;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !is_null) {
    if (4 == expr.arg_cnt_) {
      if (OB_FAIL(expr.args_[3]->eval(ctx, occ))) {
        LOG_WARN("eval occ arg failed", K(ret));
      } else if (occ->is_null()) {
        is_null = true;
      } else {
        number::ObNumber occ_nmb(occ->get_number());
        if (OB_FAIL(ObExprUtil::trunc_num2int64(occ_nmb, occ_int))) {
          LOG_WARN("trunc_num2int64 failed", K(ret));
        }
      }
    }
  }

  calc_cs_type = CS_TYPE_INVALID;
  if (OB_SUCC(ret) && !is_null) {
    if (OB_FAIL(ObLocationExprOperator::get_calc_cs_type(expr, calc_cs_type))) {
      LOG_WARN("get_calc_cs_type failed", K(ret));
    }
  }
  return ret;
}

int ObExprOracleInstr::calc_oracle_instr_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                              ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // instr(haystack, needle, pos, occ);
  ObDatum *haystack = NULL;
  ObDatum *needle = NULL;
  bool is_null = false;
  int64_t pos_int = 1;
  int64_t occ_int = 1;
  ObCollationType calc_cs_type = CS_TYPE_INVALID;
  const ObObjType needle_type = expr.args_[1]->datum_meta_.type_;
  const ObObjType haystack_type = expr.args_[0]->datum_meta_.type_;
  bool haystack_has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
  bool needle_has_lob_header = expr.args_[1]->obj_meta_.has_lob_header();
  if (OB_FAIL(ObExprOracleInstr::calc_oracle_instr_arg(expr, ctx, is_null, haystack,
              needle, pos_int, occ_int, calc_cs_type))) {
    LOG_WARN("calc_oracle_instr_arg failed", K(ret));
  } else if (is_null) {
    res_datum.set_null();
  } else if (0 == haystack->get_string().length() || ob_is_empty_lob(haystack_type, *haystack, haystack_has_lob_header)) {
    number::ObNumber res_nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(res_nmb.from((uint64_t)(0), tmp_alloc))) {
      LOG_WARN("get number from int failed", K(ret));
    } else {
      res_datum.set_number(res_nmb);
    }
  } else {
    if (OB_UNLIKELY(occ_int <= 0)) {
      ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, occ_int);
    } else if (OB_ISNULL(haystack) || OB_ISNULL(needle)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum is NULL", K(ret), KP(haystack), KP(needle));
    } else if (OB_UNLIKELY(pos_int == 0)) {
      number::ObNumber res_nmb;
      ObNumStackOnceAlloc tmp_alloc;
      if (OB_FAIL(res_nmb.from(static_cast<int64_t>(0), tmp_alloc))) {
        LOG_WARN("get number from 0 failed", K(ret));
      } else {
        res_datum.set_number(res_nmb);
      }
    } else {
      uint32_t idx = 0;
      if (!ob_is_text_tc(haystack_type) && !ob_is_text_tc(needle_type)) {
        const ObString &str1 = haystack->get_string();
        const ObString &str2 = needle->get_string();
        if (pos_int > 0) {
          for (int64_t i = 0; i < occ_int; ++i) {
            idx = ObCharset::locate(calc_cs_type, str1.ptr(), str1.length(),
                                    str2.ptr(), str2.length(),pos_int);
            if (idx <= 0) {
              break;
            } else {
              pos_int = idx + 1;
            }
          }
        } else {
          ObEvalCtx::TempAllocGuard alloc_guard(ctx);
          ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
          if (OB_FAIL(ObExprOracleInstr::slow_reverse_search(tmp_alloc, calc_cs_type, str1,
                                                            str2, pos_int, occ_int, idx))) {
            LOG_WARN("slow_reverse_search failed", K(ret), K(calc_cs_type), K(str1), K(str2),
                      K(pos_int), K(occ_int));
          }
        }
      } else { // at least one of the inputs is text tc
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        ObIAllocator &calc_alloc = alloc_guard.get_allocator();
        ObTextStringIter haystack_iter(haystack_type, calc_cs_type, haystack->get_string(), haystack_has_lob_header);
        ObTextStringIter needle_iter(needle_type, calc_cs_type, needle->get_string(), needle_has_lob_header);

        if (OB_FAIL(calc_oracle_instr_text(haystack_iter, needle_iter,
                                          calc_alloc, calc_cs_type, pos_int, occ_int, idx))) {
          LOG_WARN("calc oracle instr for text types failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        number::ObNumber res_nmb;
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(res_nmb.from((uint64_t)idx, tmp_alloc))) {
          LOG_WARN("get number from int failed", K(ret), K(idx));
        } else {
          res_datum.set_number(res_nmb);
        }
      }
    }
  }
  return ret;
}

int ObExprOracleInstr::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 2 || rt_expr.arg_cnt_ > 4)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret), K(rt_expr.arg_cnt_));
  }
  rt_expr.eval_func_ = calc_oracle_instr_expr;
  return ret;
}

} //namespace sql
} //namespace oceanbase
