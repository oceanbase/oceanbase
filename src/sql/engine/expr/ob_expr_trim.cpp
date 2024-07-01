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
#include "sql/engine/expr/ob_expr_trim.h"
#include <string.h>
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObExprTrim::ObExprTrim(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_TRIM, N_TRIM, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}

ObExprTrim::ObExprTrim(ObIAllocator &alloc,
                       ObExprOperatorType type,
                       const char *name,
                       int32_t param_num)
    : ObStringExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL)
{
}

ObExprTrim::~ObExprTrim()
{
}

int ObExprTrim::trim(ObString &result,
                     const int64_t trim_type,
                     const ObString &pattern,
                     const ObString &text)
{
  int ret = OB_SUCCESS;
  int32_t start = 0;
  int32_t end = 0;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    start = 0;
    end = text.length();
  } else {
    switch (trim_type) {
    case TYPE_LRTRIM: {
        ret = lrtrim(text, pattern, start, end);
        break;
      }
    case TYPE_LTRIM: {
        end = text.length();
        ret = ltrim(text, pattern, start);
        break;
      }
    case TYPE_RTRIM: {
        start = 0;
        ret = rtrim(text, pattern, end);
        break;
      }
    default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid type", K(trim_type), K(ret));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    result = ObString(end - start, end - start,
                      const_cast<char *>(text.ptr() + start));
  }
  return ret;
}

// for func Ltrim/Rtrim which has 2 string param
int ObExprTrim::trim2(common::ObString &result,
                  const int64_t trim_type,
                  const common::ObString &pattern,
                  const common::ObString &text,
                  const ObCollationType &cs_type,
                  const ObFixedArray<size_t, ObIAllocator> &pattern_byte_num,
                  const ObFixedArray<size_t, ObIAllocator> &pattern_byte_offset)
{
  int ret = OB_SUCCESS;
  int32_t start = 0;
  int32_t end = 0;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    start = 0;
    end = text.length();
  } else {
    switch (trim_type) {
    case TYPE_LTRIM: {
        end = text.length();
        ret = ltrim2(text, pattern, start, cs_type, pattern_byte_num, pattern_byte_offset);
        break;
      }
    case TYPE_RTRIM: {
        start = 0;
        ret = rtrim2(text, pattern, end, cs_type, pattern_byte_num, pattern_byte_offset);
        break;
      }
    default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid type", K(trim_type), K(ret));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    result = ObString(end - start, end - start,
                      const_cast<char *>(text.ptr() + start));
  }
  return ret;
}

int ObExprTrim::lrtrim(const ObString src, const ObString pattern, int32_t &start, int32_t &end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(pattern.length()));
  } else {
    int32_t i = 0;
    start = 0;
    end = src.length();
    int32_t src_len = src.length();
    int32_t pattern_len = pattern.length();
    for (i = 0;
        OB_SUCC(ret) && i <=  src_len - pattern_len;
        i += pattern_len) {
      if (0 == MEMCMP(src.ptr() + i, pattern.ptr(), pattern_len)) {
        start += pattern_len;
      } else {
        break;
      }
    } //end for
    for (i = src_len - pattern_len;
        OB_SUCC(ret) && i >= start;
        i -= pattern_len) {
      if (0 == MEMCMP(src.ptr() + i, pattern.ptr(), pattern_len)) {
        end -= pattern_len;
      } else {
        break;
      }
    } //end for
  }
  return ret;
}

// NOTE: pattern.length() MUST > 0
int ObExprTrim::ltrim(const ObString src, const ObString pattern, int32_t &start)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(pattern.length()));
  } else {
    start = 0;
    int32_t src_len = src.length();
    int32_t pattern_len = pattern.length();
    for (int32_t i = 0;
        OB_SUCC(ret) && i <= src_len - pattern_len;
        i += pattern_len) {
      if (0 == MEMCMP(src.ptr() + i, pattern.ptr(), pattern_len)) {
        start += pattern_len;
      } else {
        break;
      }
    }//end for
  }
  return ret;
}

// NOTE: pattern.length() MUST > 0
int ObExprTrim::rtrim(const ObString src, const ObString pattern, int32_t &end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(pattern.length()));
  } else {
    end = src.length();
    int32_t src_len = src.length();
    int32_t pattern_len = pattern.length();
    for (int32_t i = src_len - pattern_len;
        OB_SUCC(ret) && i >= 0;
        i -= pattern_len) {
      if (0 == MEMCMP(src.ptr() + i, pattern.ptr(), pattern_len)) {
        end -= pattern_len;
      } else {
        break;
      }
    }
  }
  return ret;
}


int ObExprTrim::ltrim2(const ObString src,
                       const ObString pattern,
                       int32_t &start,
                       const ObCollationType &cs_type,
                       const ObFixedArray<size_t, ObIAllocator> &pattern_byte_num,
                       const ObFixedArray<size_t, ObIAllocator> &pattern_byte_offset)
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invaild arguemnt", K(ret), K(pattern.length()));
  } else {
    int32_t i = 0;
    start = 0;
    int32_t src_len = src.length();


    //对于pattern中的每个字符，首先检查src_len - start是否有足够的长度，如果有则，在src左端进行MEMCMP，match后
    //start+=char_len进入下一轮，遍历pattern无法满足则ret
    bool is_match = false;
    do {
      is_match = false;
      for (i=0; OB_SUCC(ret) && i < pattern_byte_num.count() && !is_match; ++i) {
        int32_t char_len = pattern_byte_num[i];
        int32_t pattern_offset = pattern_byte_offset[i];
        if (src_len - start < char_len) {
        //src字符串当前长度不到一个字符，不进行此次匹配，与下一个pattern字符进行比较，下个字符字节长度可能满足需求
        } else if (0 == MEMCMP(src.ptr() + start, pattern.ptr() + pattern_offset, char_len)) {
          start += char_len;
          is_match = true;
        } else {
          continue;
        }
      }
    } while (is_match);
  }
    return ret;
}

int ObExprTrim::rtrim2(const ObString src,
                       const ObString pattern,
                       int32_t &end,
                       const ObCollationType &cs_type,
                       const ObFixedArray<size_t, ObIAllocator> &pattern_byte_num,
                       const ObFixedArray<size_t, ObIAllocator> &pattern_byte_offset)
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invaild arguemnt", K(ret), K(pattern.length()));
  } else {
    int32_t i = 0;
    int32_t src_len = src.length();
    end = src_len;

    //对于pattern中的每个字符，首先检查end是否有足够的长度，如果有则在src右端进行MEMCMP，match后
    //end-=char_len进入下轮，遍历pattern无法满足则ret
    bool is_match = false;
    do {
      is_match = false;
      for (i=0; OB_SUCC(ret) && i < pattern_byte_num.count() && !is_match; ++i) {
        int32_t char_len = pattern_byte_num[i];
        int32_t pattern_offset = pattern_byte_offset[i];
        if (end  < char_len) {
        //src字符串当前长度不到一个字符，不进行此次匹配，与下一个pattern字符进行比较，下个字符字节长度可能满足需求
        } else if (0 ==
                   MEMCMP(src.ptr() + end - char_len, pattern.ptr() + pattern_offset, char_len)) {
          end -= char_len;
          is_match = true;
        } else {
          continue;
        }
      }
    } while (is_match);

  }
  return ret;
}


inline int ObExprTrim::deduce_result_type(ObExprResType &type,
                                          ObExprResType *types,
                                          int64_t param_num,
                                          ObExprTypeCtx &type_ctx)
{
  int ret = OB_SUCCESS;
  CK(NULL != types);
  CK(NULL != type_ctx.get_session());
  CK(param_num >= 1 && param_num <= 3);

  ObExprResType *str_type = &types[param_num - 1];
  type.set_length(types[param_num - 1].get_length());
  ObExprResType *pattern_type = (3 == param_num) ? &types[1] : NULL;
  if (param_num > 2) {
    types[0].set_calc_type(ObIntType);
  }

  if (lib::is_oracle_mode()) {//这里Oracle模式步骤是否有区别
    ObSEArray<ObExprResType*, 2, ObNullAllocator> str_params;
    OZ(str_params.push_back(str_type));
    LOG_DEBUG("str_type is", K(str_type));
    OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), str_params,
                                                type, PREFER_VAR_LEN_CHAR)); // prefer varchar
    if (NULL != pattern_type) {
      OZ(str_params.push_back(pattern_type));
    }
    OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, str_params));
    OX(type.set_length(str_type->get_calc_length()));
  } else {
    if (str_type->is_lob()) {
      type.set_type(str_type->get_type());
      str_type->set_calc_type(str_type->get_type());
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(str_type->get_type());
      }
    } else if (str_type->is_nstring()) {
      type.set_meta(*str_type);
      //兼容Oracle行为
      type.set_type_simple(ObNVarchar2Type);
      type.set_length_semantics(LS_CHAR);
      str_type->set_calc_type(ObVarcharType);
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(ObVarcharType);
      }
    } else {
      const common::ObLengthSemantics default_length_semantics =
                   (OB_NOT_NULL(type_ctx.get_session())
                   ? type_ctx.get_session()->get_actual_nls_length_semantics()
                   : common::LS_BYTE);
      type.set_varchar();
      type.set_length_semantics(types[0].is_varchar_or_char()
                               ? types[0].get_length_semantics() : default_length_semantics);
      str_type->set_calc_type(ObVarcharType);
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(ObVarcharType);
      }
    }
    // deduce charset
    ObSEArray<ObExprResType, 2> tmp_types;
    OZ(tmp_types.push_back(*str_type));
    //if (NULL != pattern_type && !lib::is_oracle_mode()) {
    //  OZ(tmp_types.push_back(*pattern_type));
    //}
    OZ(aggregate_charsets_for_string_result_with_comparison(
            type, &tmp_types.at(0), tmp_types.count(), type_ctx.get_coll_type()));
    str_type->set_calc_collation_type(type.get_collation_type());
    str_type->set_calc_collation_level(type.get_collation_level());
    if (NULL != pattern_type) {
      pattern_type->set_calc_collation_type(type.get_collation_type());
      pattern_type->set_calc_collation_level(type.get_collation_level());
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      ObLength res_length = 0;
      OZ(ObExprResultTypeUtil::deduce_max_string_length_oracle(
        type_ctx.get_session()->get_dtc_params(), *str_type, type, res_length));
      OX(type.set_length(res_length));
    }
  }

  return ret;
}

int ObExprTrim::fill_default_pattern(char *buf, const int64_t in_len,
                                     ObCollationType cs_type, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  char default_pattern = ' ';
  const ObCharsetInfo *cs = ObCharset::get_charset(cs_type);
  if (OB_ISNULL(cs) || OB_ISNULL(cs->cset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected collation type", K(ret), K(cs_type), K(cs));
  } else if (NULL == buf || in_len < cs->mbminlen) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(ret), KP(buf), K(in_len), K(cs->mbminlen));
  } else if (1 == cs->mbminlen) {
    *buf = default_pattern;
    out_len = 1;
  } else {
    // mbminlen is always enough
    cs->cset->fill(cs, buf, cs->mbminlen, default_pattern);
    out_len = cs->mbminlen;
  }
  return ret;
}

int ObExprTrim::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 <= rt_expr.arg_cnt_ && rt_expr.arg_cnt_ <= 3);
  rt_expr.eval_func_ = eval_trim;
  return ret;
}

static int text_trim2(ObTextStringIter &str_iter,
                      ObTextStringDatumResult &output_result,
                      int64_t trim_type,
                      const common::ObString &pattern,
                      const size_t &pattern_len_in_char,
                      const ObCollationType &cs_type,
                      const ObFixedArray<size_t, ObIAllocator> &pattern_byte_num,
                      const ObFixedArray<size_t, ObIAllocator> &pattern_byte_offset)
{
  int ret = OB_SUCCESS;
  ObString output;
  int64_t total_byte_len = 0;
  if (OB_FAIL(str_iter.get_byte_len(total_byte_len))) {
    LOG_WARN("get str_iter byte len failed", K(ret));
  } else {
    ObTextStringIterState state;
    ObString str_data;
    str_iter.set_reserved_len(pattern_len_in_char);
    if (trim_type == ObExprTrim::TYPE_LTRIM) {
      bool found_start = false;
      while (OB_SUCC(ret) && (state = str_iter.get_next_block(str_data)) == TEXTSTRING_ITER_NEXT) {
        if (!found_start) {
          if (OB_FAIL(ObExprTrim::trim2(output, trim_type, pattern,
                                        str_data, cs_type, pattern_byte_num, pattern_byte_offset))) {
            LOG_WARN("do trim2 failed", K(ret));
          } else if (output.length() != 0) {
            found_start = true;
            str_iter.reset_reserve_len();
            int64_t result_len = output.length() + (total_byte_len - str_iter.get_accessed_byte_len());
            if (OB_FAIL(output_result.init(result_len))) {
              LOG_WARN("init stringtext result failed", K(ret), K(result_len));
            } else if (OB_FAIL(output_result.append(output))) {
              LOG_WARN("fail to append to result", K(ret), K(output.length()), K(output_result));
            }
          }
        } else if (OB_FAIL(output_result.append(str_data))) {
          LOG_WARN("fail to append to result", K(ret), K(str_data.length()), K(output_result));
        }
      }
    } else if (trim_type == ObExprTrim::TYPE_RTRIM) {
      bool found_end = false;
      str_iter.set_backward();
      char *buf = NULL;
      int64_t buf_size = 0;
      int64_t buf_pos = 0;
      while (OB_SUCC(ret) && (state = str_iter.get_next_block(str_data)) == TEXTSTRING_ITER_NEXT) {
        if (!found_end) {
          if (OB_FAIL(ObExprTrim::trim2(output, trim_type, pattern,
                                        str_data, cs_type, pattern_byte_num, pattern_byte_offset))) {
            LOG_WARN("do trim2 failed", K(ret));
          } else if (output.length() != 0) {
            found_end = true;
            str_iter.reset_reserve_len();
            int64_t result_len = output.length() + (total_byte_len - str_iter.get_accessed_byte_len());
            if (OB_FAIL(output_result.init(result_len))) {
              LOG_WARN("init stringtext result failed", K(ret), K(result_len));
            } else if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_size))) {
              LOG_WARN("stringtext result reserve buffer failed", K(ret));
            } else if (OB_FAIL(output_result.lseek(result_len, 0))) {
              LOG_WARN("result lseek failed", K(ret));
            } else {
              buf_pos = buf_size - output.length();
              MEMCPY(buf + buf_pos, output.ptr(), output.length());
            }
          }
        } else {
          buf_pos -= str_data.length();
          MEMCPY(buf + buf_pos, str_data.ptr(), str_data.length());
        }
      }
      if (OB_SUCC(ret)) {
        OB_ASSERT(buf_pos == 0);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", K(trim_type), K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
      ret = (str_iter.get_inner_ret() != OB_SUCCESS) ?
            str_iter.get_inner_ret() : OB_INVALID_DATA;
      LOG_WARN("iter state invalid", K(ret), K(state), K(str_iter));
    } else if (!output_result.is_init() && OB_FAIL(output_result.init(0))) { // nothing found build empty lob
      LOG_WARN("init stringtext result for empty lob failed", K(ret));
    } else {
      output_result.set_result();
    }
  }
  return ret;
}

static int text_trim(ObTextStringIter &str_iter,
                     ObTextStringIter &str_backward_iter,
                     ObIAllocator &calc_alloc,
                     ObTextStringDatumResult &output_result,
                     int64_t trim_type,
                     const common::ObString &pattern)
{
  int ret = OB_SUCCESS;
  ObString output;
  int64_t total_byte_len = 0;
  if (OB_FAIL(str_iter.init(0, NULL, &calc_alloc))) {
    LOG_WARN("init str_iter failed ", K(ret), K(str_iter));
  } else if (OB_FAIL(str_iter.get_byte_len(total_byte_len))) {
    LOG_WARN("get str_iter byte len failed", K(ret));
  } else {
    ObTextStringIterState state;
    ObString str_data;
    str_iter.set_reserved_byte_len(pattern.length());
    if (trim_type == ObExprTrim::TYPE_LTRIM) {
      bool found_start = false;
      while (OB_SUCC(ret) && (state = str_iter.get_next_block(str_data)) == TEXTSTRING_ITER_NEXT) {
        if (!found_start) {
          if (OB_FAIL(ObExprTrim::trim(output, trim_type, pattern, str_data))) {
            LOG_WARN("do trim failed", K(ret));
          } else if (output.length() != 0) {
            found_start = true;
            str_iter.reset_reserve_len();
            int64_t result_len = output.length() + (total_byte_len - str_iter.get_accessed_byte_len());
            if (OB_FAIL(output_result.init(result_len))) {
              LOG_WARN("init stringtext result failed", K(ret), K(result_len));
            } else if (OB_FAIL(output_result.append(output))) {
              LOG_WARN("fail to append to result", K(ret), K(output.length()), K(output_result));
            }
          }
        } else if (OB_FAIL(output_result.append(str_data))) {
          LOG_WARN("fail to append to result", K(ret), K(str_data.length()), K(output_result));
        }
      }
    } else if (trim_type == ObExprTrim::TYPE_RTRIM) {
      bool found_end = false;
      str_iter.set_backward();
      char *buf = NULL;
      int64_t buf_size = 0;
      int64_t buf_pos = 0;
      while (OB_SUCC(ret) && (state = str_iter.get_next_block(str_data)) == TEXTSTRING_ITER_NEXT) {
        if (!found_end) {
          if (OB_FAIL(ObExprTrim::trim(output, trim_type, pattern, str_data))) {
            LOG_WARN("do trim failed", K(ret));
          } else if (output.length() != 0) {
            found_end = true;
            str_iter.reset_reserve_len();
            int64_t result_len = output.length() + (total_byte_len - str_iter.get_accessed_byte_len());
            if (OB_FAIL(output_result.init(result_len))) {
              LOG_WARN("init stringtext result failed", K(ret), K(result_len));
            } else if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_size))) {
              LOG_WARN("stringtext result reserve buffer failed", K(ret));
            } else if (OB_FAIL(output_result.lseek(result_len, 0))) {
              LOG_WARN("result lseek failed", K(ret));
            } else if (buf_size < output.length()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("buf size is wrong with data length", K(ret), K(buf_size), K(output.length()));
            } else {
              buf_pos = buf_size - output.length();
              MEMCPY(buf + buf_pos, output.ptr(), output.length());
            }
          }
        } else {
          if (buf_pos < str_data.length()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("buf pos is wrong with data length", K(ret), K(buf_pos), K(str_data.length()));
          } else {
            buf_pos -= str_data.length();
            MEMCPY(buf + buf_pos, str_data.ptr(), str_data.length());
          }
        }
      }
      if (OB_SUCC(ret)) {
        OB_ASSERT(buf_pos == 0);
      }
    } else if (trim_type == ObExprTrim::TYPE_LRTRIM) {
      // find start, if access total length, end
      bool found_start = false;
      bool is_finished = false;
      int64_t start_pos = 0;
      int64_t end_pos = 0;
      while (OB_SUCC(ret)
             && found_start == false
             && is_finished == false
             && (state = str_iter.get_next_block(str_data)) == TEXTSTRING_ITER_NEXT) {
        if (OB_FAIL(ObExprTrim::trim(output, ObExprTrim::TYPE_LTRIM, pattern, str_data))) {
          LOG_WARN("do front ltrim failed", K(ret), K(pattern), K(str_data));
        } else if (output.length() != 0) {
          found_start = true;
          str_iter.reset_reserve_len();
          start_pos = str_iter.get_accessed_byte_len() - output.length();
          // output not copied
        } else if (str_iter.get_accessed_byte_len() == total_byte_len) { // search to end and all output is zero
          is_finished = true;
          if (OB_FAIL(output_result.init(output.length()))) {
            LOG_WARN("init stringtext result failed", K(ret), K(output.length()));
          } else if (OB_FAIL(output_result.append(output))) {
            LOG_WARN("fail to append to result", K(ret), K(output.length()), K(output_result));
          }
        }
      }
      if (OB_FAIL(ret) || is_finished) {
      } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
        ret = (str_iter.get_inner_ret() != OB_SUCCESS) ?
              str_iter.get_inner_ret() : OB_INVALID_DATA;
        LOG_WARN("iter state invalid", K(ret), K(state), K(str_iter));
      } else {
        OB_ASSERT(found_start);
        // find end
        ObTextStringIterState back_state;
        if (OB_FAIL(str_backward_iter.init(0, NULL, &calc_alloc))) {
          LOG_WARN("init str_iter failed ", K(ret), K(str_backward_iter));
        } else {
          ObString backward_str_data;
          ObString backward_output;
          str_backward_iter.set_backward();
          str_backward_iter.set_reserved_byte_len(pattern.length());
          bool found_end = false;
          while (OB_SUCC(ret)
             && found_end == false
             && (back_state = str_backward_iter.get_next_block(backward_str_data)) == TEXTSTRING_ITER_NEXT) {
            if (OB_FAIL(ObExprTrim::trim(backward_output, ObExprTrim::TYPE_RTRIM, pattern, backward_str_data))) {
              LOG_WARN("do backward rtrim failed", K(ret), K(pattern), K(backward_str_data));
            } else if (backward_output.length() != 0) {
              found_end = true;
              end_pos = total_byte_len - str_backward_iter.get_accessed_byte_len() + backward_output.length();
            }
          }
          // copy from start to end
          int result_len = end_pos - start_pos;
          if (OB_FAIL(ret)) {
          } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
            ret = (str_backward_iter.get_inner_ret() != OB_SUCCESS) ?
                  str_backward_iter.get_inner_ret() : OB_INVALID_DATA;
            LOG_WARN("str_backward_iter state invalid", K(ret), K(state), K(str_backward_iter));
          } else if (result_len < 0) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("init stringtext result failed", K(ret), K(start_pos), K(end_pos));
          // } else if (result_len == total_byte_len) {
          // the same as input if it is a temp lob?
          } else if (OB_FAIL(output_result.init(result_len))) {
            LOG_WARN("init stringtext result failed", K(ret), K(output.length()));
          } else {
            str_data = output;
            do {
              if (str_data.length() > result_len) {
                if (OB_FAIL(output_result.append(str_data.ptr(), result_len))) {
                  LOG_WARN("fail to append to result", K(ret), K(result_len), K(output_result));
                } else {
                  result_len = 0;
                }
              } else if (OB_FAIL(output_result.append(str_data))) {
                LOG_WARN("fail to append to result", K(ret), K(str_data.length()), K(output_result));
              } else {
                result_len -= str_data.length();
              }
            } while (OB_SUCC(ret) && result_len > 0 && (state = str_iter.get_next_block(str_data)) == TEXTSTRING_ITER_NEXT);
            if (OB_SUCC(ret)) {
              OB_ASSERT(result_len == 0);
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
      ret = (str_iter.get_inner_ret() != OB_SUCCESS) ?
            str_iter.get_inner_ret() : OB_INVALID_DATA;
      LOG_WARN("iter state invalid", K(ret), K(state), K(str_iter));
    } else if (!output_result.is_init() && OB_FAIL(output_result.init(0))) { // nothing found build empty lob
      LOG_WARN("init stringtext result for empty lob failed", K(ret));
    } else {
      output_result.set_result();
    }
  }
  return ret;
}

static int eval_trim_inner(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum,
                           const int64_t &trim_type, ObString &pattern, bool &res_is_clob,
                           const ObDatumMeta &str_meta, const bool str_has_lob_header,
                           const ObDatum &str_datum)
{
  int ret = OB_SUCCESS;
  ObString output;
  if (2 == expr.arg_cnt_ && (T_FUN_SYS_LTRIM == expr.type_ || T_FUN_SYS_RTRIM == expr.type_)) {
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    ObCollationType cs_type = expr.datum_meta_.cs_type_;
    size_t pattern_len_in_char  =
                  ObCharset::strlen_char(cs_type, pattern.ptr(), pattern.length());
    ObFixedArray<size_t, ObIAllocator> pattern_byte_num(calc_alloc,  pattern_len_in_char);
    ObFixedArray<size_t, ObIAllocator> pattern_byte_offset(calc_alloc,  pattern_len_in_char +1);
    if (OB_FAIL(ObExprUtil::get_mb_str_info(pattern, cs_type,
                                            pattern_byte_num, pattern_byte_offset))) {
      LOG_WARN("get_mb_str_info failed", K(ret), K(pattern), K(cs_type), K(pattern_len_in_char));
    } else if (!res_is_clob && (pattern_byte_num.count() + 1 != pattern_byte_offset.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("size of pattern_byte_num and size of pattern_byte_offset should be same",
                K(ret), K(pattern_byte_num), K(pattern_byte_offset));
    }
    if (OB_FAIL(ret)) {
    } else if (!ob_is_text_tc(str_meta.type_)) {
      if (OB_FAIL(ObExprTrim::trim2(output, trim_type, pattern,
                                    str_datum.get_string(), cs_type,
                                    pattern_byte_num, pattern_byte_offset))) {
        LOG_WARN("do trim2 failed", K(ret));
      } else {
        if (output.empty() && lib::is_oracle_mode() && !res_is_clob) {
          expr_datum.set_null();
        } else {
          expr_datum.set_string(output);
        }
      }
    } else { // is text tc, trim left or right
      ObTextStringIter str_iter(str_meta.type_, str_meta.cs_type_, str_datum.get_string(), str_has_lob_header);
      ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
      if (OB_FAIL(str_iter.init(0, NULL, &calc_alloc))) {
        LOG_WARN("init str_iter failed ", K(ret), K(str_iter));
      } else if (OB_FAIL(text_trim2(str_iter, output_result, trim_type, pattern, pattern_len_in_char,
                                    cs_type, pattern_byte_num,  pattern_byte_offset))) {
        LOG_WARN("text_trim2 failed", K(ret));
      }
    }
  } else {
    if (!ob_is_text_tc(str_meta.type_)) {
      if (OB_FAIL(ObExprTrim::trim(output, trim_type, pattern, str_datum.get_string()))) {
        LOG_WARN("do trim failed", K(ret));
      } else {
        if (output.empty() && lib::is_oracle_mode() && !res_is_clob) {
          expr_datum.set_null();
        } else {
          expr_datum.set_string(output);
        }
      }
    } else { // is text tc, trim left or right or both
      // Notice: need to access with byte length
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      ObTextStringIter str_forward_iter(str_meta.type_, CS_TYPE_BINARY, str_datum.get_string(), str_has_lob_header);
      ObTextStringIter str_backward_iter(str_meta.type_, CS_TYPE_BINARY, str_datum.get_string(), str_has_lob_header);
      ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
      if (OB_FAIL(text_trim(str_forward_iter, str_backward_iter, calc_alloc, output_result,
                            trim_type, pattern))) {
        LOG_WARN("text_trim failed", K(ret));
      }
    }
  }
  return ret;
}

int ObExprTrim::eval_trim(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  bool has_null = false;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else {
    for (int64_t i = 0; i < expr.arg_cnt_; i++) {
      if (expr.locate_param_datum(ctx, i).is_null()) {
        has_null = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (has_null) {
    expr_datum.set_null();
  } else {
    int64_t trim_type = TYPE_LRTRIM;
    // may be called by ltrim() or rtrim()
    if (1 == expr.arg_cnt_) {
      if (T_FUN_SYS_LTRIM == expr.type_) {
        trim_type = TYPE_LTRIM;
      } else if (T_FUN_SYS_RTRIM == expr.type_) {
        trim_type = TYPE_RTRIM;
      }
    } else if (2 == expr.arg_cnt_) {
      if (T_FUN_SYS_LTRIM == expr.type_) {
        LOG_WARN("trim_type is ltrim");
        trim_type = TYPE_LTRIM;
      } else if (T_FUN_SYS_RTRIM == expr.type_) {
        LOG_WARN("trim type is rtrim");
        trim_type = TYPE_RTRIM;
      } else {
        trim_type = expr.locate_param_datum(ctx, 0).get_int();
        LOG_WARN("trim type is ", K(trim_type));
      }
    } else {
      trim_type = expr.locate_param_datum(ctx, 0).get_int();
    }

    char default_pattern_buffer[8];
    ObString pattern;
    bool res_is_clob = false;
    if (2 == expr.arg_cnt_ && (T_FUN_SYS_LTRIM == expr.type_ || T_FUN_SYS_RTRIM == expr.type_)) {
      const ObDatumMeta &str_meta = expr.args_[expr.arg_cnt_ - 2]->datum_meta_;
      const ObDatum &str_datum = expr.locate_param_datum(ctx, expr.arg_cnt_ - 2);
      const bool str_has_lob_header = expr.args_[expr.arg_cnt_ - 2]->obj_meta_.has_lob_header();
      pattern = expr.locate_param_datum(ctx, expr.arg_cnt_ - 1).get_string();
      res_is_clob = lib::is_oracle_mode()
                    && ob_is_text_tc(expr.args_[expr.arg_cnt_ - 2]->datum_meta_.type_)
                    && (CS_TYPE_BINARY != expr.args_[expr.arg_cnt_ - 2]->datum_meta_.cs_type_);
      const ObDatumMeta &pattern_meta = expr.args_[expr.arg_cnt_ - 1]->datum_meta_;
      const bool pattern_has_lob_header = expr.args_[expr.arg_cnt_ - 1]->obj_meta_.has_lob_header();
      if (!ob_is_text_tc(pattern_meta.type_)) { // pattern not text tc
        if (OB_FAIL(eval_trim_inner(expr, ctx, expr_datum, trim_type, pattern,
                                    res_is_clob, str_meta, str_has_lob_header, str_datum))) {
          LOG_WARN("failed to eval trim case 1", K(ret));
        }
      } else { // pattern is text tc
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        ObIAllocator &calc_alloc = alloc_guard.get_allocator();
        const ObDatum &pattern_datum = expr.locate_param_datum(ctx, expr.arg_cnt_ - 1);
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(calc_alloc, pattern_datum,
                                                              pattern_meta, pattern_has_lob_header, pattern))) {
          LOG_WARN("failed to read real pattern", K(ret), K(pattern));
        } else if (OB_FAIL(eval_trim_inner(expr, ctx, expr_datum, trim_type, pattern,
                                           res_is_clob, str_meta, str_has_lob_header, str_datum))) {
          LOG_WARN("failed to eval trim case 1 for text tc pattern", K(ret));
        }
      }
    } else {
      const ObDatumMeta &str_meta = expr.args_[expr.arg_cnt_ - 1]->datum_meta_;
      const ObDatum &str_datum = expr.locate_param_datum(ctx, expr.arg_cnt_ - 1);
      const bool str_has_lob_header = expr.args_[expr.arg_cnt_ - 1]->obj_meta_.has_lob_header();
      res_is_clob = lib::is_oracle_mode()
                    && ob_is_text_tc(expr.args_[expr.arg_cnt_ - 1]->datum_meta_.type_)
                    && (CS_TYPE_BINARY != expr.args_[expr.arg_cnt_ - 1]->datum_meta_.cs_type_);
      if (3 == expr.arg_cnt_) {
        const ObDatumMeta &pattern_meta = expr.args_[1]->datum_meta_;
        const bool pattern_has_lob_header = expr.args_[1]->obj_meta_.has_lob_header();
        if (!ob_is_text_tc(pattern_meta.type_)) { // pattern not text tc
          pattern = expr.locate_param_datum(ctx, 1).get_string();
          if (lib::is_oracle_mode() && 1 < ObCharset::strlen_char(
                  expr.datum_meta_.cs_type_, pattern.ptr(), pattern.length())) {
            ret = OB_ERR_IN_TRIM_SET;
            LOG_USER_ERROR(OB_ERR_IN_TRIM_SET);
          } else if (OB_FAIL(eval_trim_inner(expr, ctx, expr_datum, trim_type, pattern,
                                             res_is_clob, str_meta, str_has_lob_header, str_datum))) {
            LOG_WARN("failed to eval trim case 2", K(ret));
          }
        } else {
          ObEvalCtx::TempAllocGuard alloc_guard(ctx);
          ObIAllocator &calc_alloc = alloc_guard.get_allocator();
          const ObDatum &pattern_datum = expr.locate_param_datum(ctx, 1);
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(calc_alloc, pattern_datum,
                                                                pattern_meta, pattern_has_lob_header, pattern))) {
            LOG_WARN("failed to read real pattern", K(ret), K(pattern));
          } else if (lib::is_oracle_mode() && 1 < ObCharset::strlen_char(
                  expr.datum_meta_.cs_type_, pattern.ptr(), pattern.length())) {
            ret = OB_ERR_IN_TRIM_SET;
            LOG_USER_ERROR(OB_ERR_IN_TRIM_SET);
          } else if (OB_FAIL(eval_trim_inner(expr, ctx, expr_datum, trim_type, pattern,
                                             res_is_clob, str_meta, str_has_lob_header, str_datum))) {
            LOG_WARN("failed to eval trim case 2 for text tc pattern", K(ret));
          }
        }
      } else {
        int64_t out_len = 0;
        if (OB_FAIL(fill_default_pattern(default_pattern_buffer,
                                         sizeof(default_pattern_buffer),
                                         expr.datum_meta_.cs_type_,
                                         out_len))) {
        } else if (out_len <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected out length", K(ret), K(out_len));
        } else {
          pattern.assign_ptr(default_pattern_buffer, static_cast<int32_t>(out_len));
        }
        if (OB_SUCC(ret)
            && OB_FAIL(eval_trim_inner(expr, ctx, expr_datum, trim_type, pattern,
                                       res_is_clob, str_meta, str_has_lob_header, str_datum))) {
          LOG_WARN("failed to eval trim case 3", K(ret));
        }
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprTrim, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

// Ltrim start
ObExprLtrim::ObExprLtrim(ObIAllocator &alloc)
    : ObExprTrim(alloc, T_FUN_SYS_LTRIM, N_LTRIM, (lib::is_oracle_mode()) ? ONE_OR_TWO : 1)
{
}

ObExprLtrim::ObExprLtrim(ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name,
                         int32_t param_num)
    : ObExprTrim(alloc, type, name, param_num)
{
}

ObExprLtrim::~ObExprLtrim()
{
}

inline int ObExprLtrim::calc_result_type1(ObExprResType &res_type,
                                          ObExprResType &type1,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    OZ(ObExprTrim::deduce_result_type(res_type, &type1, 1, type_ctx));
  }
  return ret;
}

inline int ObExprLtrim::deduce_result_type(ObExprResType &type,
                                          ObExprResType *types,
                                          int64_t param_num,
                                          ObExprTypeCtx &type_ctx)
{
  int ret = OB_SUCCESS;
  CK(NULL != types);
  CK(NULL != type_ctx.get_session());
  CK(param_num >= 1 && param_num <= 2);

  ObExprResType *str_type = &types[0];
  type.set_length(types[0].get_length());
  ObExprResType *pattern_type = (2 == param_num) ? &types[1] : NULL;

  if (lib::is_oracle_mode()) {
    ObSEArray<ObExprResType*, 2, ObNullAllocator> str_params;
    OZ(str_params.push_back(str_type));
    OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), str_params,
                                                type, PREFER_VAR_LEN_CHAR)); // prefer varchar
    if (NULL != pattern_type) {
      OZ(str_params.push_back(pattern_type));
    }
    OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, str_params));
    OX(type.set_length(str_type->get_calc_length()));
  } else {
    if (str_type->is_lob()) {
      type.set_type(str_type->get_type());
      str_type->set_calc_type(str_type->get_type());
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(pattern_type->get_type());
      }
    } else if (str_type->is_nstring()) {
      type.set_meta(*str_type);
      type.set_type_simple(ObNVarchar2Type);
      type.set_length_semantics(LS_CHAR);
      str_type->set_calc_type(ObVarcharType);
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(ObVarcharType);
      }
    } else {
      const common::ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session())
                                                                  ? type_ctx.get_session()->get_actual_nls_length_semantics()
                                                                  : common::LS_BYTE);
      type.set_varchar();
      type.set_length_semantics(types[0].is_varchar_or_char()
                                ? types[0].get_length_semantics() : default_length_semantics);
      str_type->set_calc_type(ObVarcharType);
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(ObVarcharType);
      }
    }
    // deduce charset
    ObSEArray<ObExprResType, 2> tmp_types;
    OZ(tmp_types.push_back(*str_type));
    if (NULL != pattern_type && !lib::is_oracle_mode()) {
      OZ(tmp_types.push_back(*pattern_type));
    }
    OZ(aggregate_charsets_for_string_result_with_comparison(
            type, &tmp_types.at(0), tmp_types.count(), type_ctx.get_coll_type()));
    str_type->set_calc_collation_type(type.get_collation_type());
    str_type->set_calc_collation_level(type.get_collation_level());
    if (NULL != pattern_type) {
      pattern_type->set_calc_collation_type(type.get_collation_type());
      pattern_type->set_calc_collation_level(type.get_collation_level());
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      ObLength res_length = 0;
      OZ(ObExprResultTypeUtil::deduce_max_string_length_oracle(
        type_ctx.get_session()->get_dtc_params(), *str_type, type, res_length));
      OX(type.set_length(res_length));
    }
  }
  return ret;
}


int ObExprLtrim::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_ || 2 == rt_expr.arg_cnt_);
  // trim type is detected by expr type in ObExprTrim::eval_trim
  rt_expr.eval_func_ = &ObExprTrim::eval_trim;
  return ret;
}

// Rtrim start
ObExprRtrim::ObExprRtrim(ObIAllocator &alloc)
    : ObExprLtrim(alloc, T_FUN_SYS_RTRIM, N_RTRIM, (lib::is_oracle_mode()) ? ONE_OR_TWO :1)
{
}

ObExprRtrim::~ObExprRtrim()
{
}

inline int ObExprRtrim::calc_result_type1(ObExprResType &res_type,
                                          ObExprResType &type1,
                                          ObExprTypeCtx &type_ctx) const
{
  return ObExprLtrim::calc_result_type1(res_type, type1, type_ctx);
}


} // sql
} // oceanbase
