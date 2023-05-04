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
#include "sql/engine/expr/ob_expr_translate.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "objit/common/ob_item_type.h"
#include "lib/oblog/ob_log.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_oracle_to_char.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprTranslate::ObExprTranslate(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_TRANSLATE, N_TRANSLATE, MORE_THAN_ONE, VALID_FOR_GENERATED_COL)
{
}

ObExprTranslate::~ObExprTranslate()
{
}

int ObExprTranslate::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t mbmaxlen = 0;
  const ObBasicSessionInfo *session = type_ctx.get_session();
  CK(OB_NOT_NULL(type_ctx.get_session()));
  if (OB_FAIL(ret)) {
  } else if (2 == param_num) {  // translate(c1 using char_cs/nchar_cs)
    int64_t char_or_nchar_cs = -1;
    if (OB_FAIL(types[1].get_param().get_int(char_or_nchar_cs))) {
      LOG_WARN("expected const int value for param1", K(ret), K(types[1].get_param()));
    } else if (OB_UNLIKELY(0 != char_or_nchar_cs && 1 != char_or_nchar_cs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("second param expected 0(char_cs) or 1(nchar_cs)", K(ret), K(char_or_nchar_cs));
    } else {
      ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();
      bool is_char_cs = 0 == char_or_nchar_cs;
      auto params = make_const_carray(&types[0]);
      ObLength result_len = 0;
      if (is_char_cs) {
        type.set_type(ObVarcharType);
        if (types[0].is_literal()) {
          type.set_length_semantics(nls_param.nls_length_semantics_);
        } else if (types[0].is_string_or_lob_locator_type()) {
          if (types[0].is_clob() || types[0].is_clob_locator()) {
            type.set_length_semantics(LS_BYTE);
          } else {
            type.set_length_semantics(types[0].get_length_semantics());
          }
        } else {
          type.set_length_semantics(nls_param.nls_length_semantics_);
        }
        type.set_collation_level(CS_LEVEL_IMPLICIT);
        type.set_collation_type(nls_param.nls_collation_);
      } else {
        type.set_type(ObNVarchar2Type);
        type.set_length_semantics(LS_CHAR);
        type.set_collation_level(CS_LEVEL_IMPLICIT);
        type.set_collation_type(nls_param.nls_nation_collation_);
      }
      OZ(deduce_string_param_calc_type_and_charset(*session, type, params, LS_BYTE));
      // the following code does not make sense, only to make result length identical with oracle,
      // but it make the calc_result_type not reentrant
      // if (OB_SUCC(ret)) {
      //   if (type.is_nvarchar2() && types[0].is_varchar_or_char()) {
      //     result_len = types[0].get_calc_length();
      //   } else {
      //     result_len = MIN(types[0].get_calc_length() * ObCharset::MAX_MB_LEN,
      //                     OB_MAX_ORACLE_VARCHAR_LENGTH);
      //   }
      //   if (type.is_nvarchar2() && !ob_is_string_tc(types[0].get_type())) {
      //     const ObCharsetInfo *cs = ObCharset::get_charset(type.get_collation_type());
      //     result_len = result_len / cs->mbminlen;
      //   }
      //   type.set_length(result_len);
      // }
      OX(type.set_length(types[0].get_calc_length() * ObCharset::MAX_MB_LEN));

    }
  } else if (OB_LIKELY(3 == param_num)) {  //translate(c1, c2, c3)
    // 这里只使用第一个参数推导结果类型，实验了下Oracle，结果类型是跟第一个参数挂钩的
    ObExprResType &ori_str_type = types[0];
    ObExprResType &from_str_type = types[1];
    ObExprResType &to_str_type = types[2];
    ObSEArray<ObExprResType*, 3, ObNullAllocator> params;
    OZ(params.push_back(&ori_str_type));
    OZ(aggregate_string_type_and_charset_oracle(*session, params, type, PREFER_VAR_LEN_CHAR));
    if (OB_SUCC(ret) && type.is_clob()) {
      type.set_type(ObVarcharType);
      type.set_length_semantics(LS_BYTE);
    }
    // 使用第一个参数推导结果的类型后，再使用结果类型推导剩余所有参数的类型
    OZ(params.push_back(&from_str_type));
    OZ(params.push_back(&to_str_type));
    OZ(deduce_string_param_calc_type_and_charset(*session, type, params));
    CK((ori_str_type.get_calc_meta() == to_str_type.get_calc_meta()));
    CK((ori_str_type.get_calc_meta() == from_str_type.get_calc_meta()));
    // deduce result length
    if (OB_SUCC(ret)) {
      ObLength result_len = types[0].get_calc_length();
      OZ(ObExprResultTypeUtil::deduce_max_string_length_oracle(session->get_dtc_params(),
        types[0], type, result_len, LS_CHAR));
      if (OB_SUCC(ret) && LS_BYTE == type.get_length_semantics()) {
        OZ(ObCharset::get_mbmaxlen_by_coll(type.get_collation_type(), mbmaxlen));
        OX(result_len *= mbmaxlen);
      }
      OX(type.set_length(result_len));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  }
  LOG_DEBUG("check type info", K(type.get_collation_type()), K(type.get_length()),
                               K(type.get_length_semantics()));
  return ret;
}

// 使用hashmap存储from_str和to_str的每个字符, key是from_str中的字符, val是to_str的字符
// 循环ori_str的每个字符，从map中找key，如果存在，则替换为val
// 支持多字节字符
int ObExprTranslate::translate_hashmap(const ObString &ori_str,
                                       const ObString &from_str,
                                       const ObString &to_str,
                                       ObCollationType cs_type,
                                       ObIAllocator &tmp_alloc,
                                       ObIAllocator &res_alloc,
                                       ObString &res_str,
                                       bool &is_null)
{
  int ret = OB_SUCCESS;
  is_null = false;
  res_str.reset();
  ObIAllocator *allocator = &tmp_alloc;
  if (OB_ISNULL(ori_str.ptr()) || OB_ISNULL(from_str.ptr()) || OB_ISNULL(to_str.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("string pointer is null", KP(ori_str.ptr()), KP(from_str.ptr()), KP(to_str.ptr()));
  } else if ((0 == ori_str.length()) || (0 == from_str.length()) || (0 == to_str.length())) {
    is_null = true;
  } else {
    size_t ori_str_len_in_char  = ObCharset::strlen_char(cs_type, ori_str.ptr(), ori_str.length());
    size_t from_str_len_in_char = ObCharset::strlen_char(cs_type, from_str.ptr(), from_str.length());
    size_t to_str_len_in_char   = ObCharset::strlen_char(cs_type, to_str.ptr(), to_str.length());
    // 记录ori_str/from_str/to_str中每个字符的字节长度
    ObFixedArray<size_t, ObIAllocator> ori_str_byte_num(allocator,  ori_str_len_in_char);
    ObFixedArray<size_t, ObIAllocator> from_str_byte_num(allocator, from_str_len_in_char);
    ObFixedArray<size_t, ObIAllocator> to_str_byte_num(allocator,   to_str_len_in_char);
    ObFixedArray<size_t, ObIAllocator> ori_str_byte_offset(allocator, ori_str_len_in_char+1);
    ObFixedArray<size_t, ObIAllocator> from_str_byte_offset(allocator, from_str_len_in_char+1);
    ObFixedArray<size_t, ObIAllocator> to_str_byte_offset(allocator, to_str_len_in_char+1);
    char  *ret_buf = NULL;
    size_t ret_buf_size = 0;
    int64_t mbmaxlen = 0;
    if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(cs_type, mbmaxlen))) {
      LOG_WARN("get_mbmaxlen_by_coll failed", K(cs_type), K(ret));
    } else {
      ret_buf_size = ori_str_len_in_char * mbmaxlen;
      ret_buf = static_cast<char*>(res_alloc.alloc(ret_buf_size));
      if (OB_ISNULL(ret_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ori_str.length()));
      } else {
        if (OB_FAIL(ObExprUtil::get_mb_str_info(ori_str, cs_type,
                                                ori_str_byte_num, ori_str_byte_offset)) ||
            OB_FAIL(ObExprUtil::get_mb_str_info(from_str, cs_type,
                                                from_str_byte_num, from_str_byte_offset)) ||
            OB_FAIL(ObExprUtil::get_mb_str_info(to_str, cs_type,
                                                to_str_byte_num, to_str_byte_offset))) {
          LOG_WARN("get_mb_str_info fail", K(cs_type), K(ori_str), K(from_str), K(to_str));
        } else {
          StringHashMap from_to_str_map;
          // TODO: PostgreSQL的实现是使用两个循环，外层循环迭代ori_str，内层循环迭代from_str。比较简单
          // 但是在字符串比较长时，没有这里使用hash的方法好。但是使用hash的方式需要付出空间和构建hash的时间的代价
          // 后续可以分析在什么时候用那种方法比较好
          // TODO: ori_str是列，from_str以及to_str是常量时，哈希表只需要构建一次，不需要每次都进行哈希表的构建
          if (OB_FAIL(from_to_str_map.create(from_str_byte_num.count(), ObModIds::OB_SQL_EXPR))) {
            LOG_WARN("from_to_str_map init failed", K(ret));
          } else if (OB_FAIL(insert_map(from_str, to_str, *(allocator), 
                  from_str_byte_num, to_str_byte_num, from_to_str_map))) {
            LOG_WARN("construct StringHashMap failed", K(ret), K(from_str), K(to_str), 
                K(from_str_byte_num), K(to_str_byte_num));
          } else {
            ObString key_iter_str;
            ObString val_iter_str;
            size_t ori_str_byte_pos = 0;
            size_t ret_buf_byte_pos = 0;
            for (size_t i = 0; OB_SUCC(ret) && (i < ori_str_byte_num.count()); ++i) {
              key_iter_str.assign_ptr(ori_str.ptr() + ori_str_byte_pos, ori_str_byte_num[i]);
              ori_str_byte_pos += ori_str_byte_num[i];
              if (OB_SUCCESS == (ret = from_to_str_map.get_refactored(key_iter_str, val_iter_str))) {
                MEMCPY(ret_buf + ret_buf_byte_pos, val_iter_str.ptr(), val_iter_str.length());
                ret_buf_byte_pos += val_iter_str.length();
                LOG_DEBUG("entry found", K(key_iter_str), K(ret_buf_byte_pos), K(val_iter_str));
              } else if (OB_HASH_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                MEMCPY(ret_buf + ret_buf_byte_pos, key_iter_str.ptr(), key_iter_str.length());
                ret_buf_byte_pos += key_iter_str.length();
                LOG_DEBUG("entry not exists", K(key_iter_str), K(ret_buf_byte_pos));
              } else if (OB_FAIL(ret)) {
                LOG_WARN("get item from hash map failed", K(ret), K(key_iter_str));
              }
            } // for
            res_str.assign_ptr(ret_buf, ret_buf_byte_pos);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprTranslate::insert_map(const ObString &key_str, const ObString &val_str, ObIAllocator &allocator,
                                const ObFixedArray<size_t, ObIAllocator> &key_str_byte_num,
                                const ObFixedArray<size_t, ObIAllocator> &val_str_byte_num,
                                StringHashMap &ret_map)
{
  int ret = OB_SUCCESS;
  ret_map.clear();
  if ((0 == key_str.length()) || (0 == val_str.length())) {
    LOG_WARN("input str length is zero", K(key_str), K(val_str));
  } else if (OB_ISNULL(key_str.ptr()) || OB_ISNULL(val_str.ptr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input_str.ptr() is NULL", K(ret), KP(key_str.ptr()), KP(val_str.ptr()));
  } else {
    char *buf = NULL;
    size_t buf_len = 0;
    ObString key_iter_str;
    ObString val_iter_str;
    size_t key_str_byte_pos = 0;
    size_t val_str_byte_pos = 0;
    for (size_t i = 0; OB_SUCC(ret) && (i < key_str_byte_num.count()); ++i) {
      key_iter_str.assign_ptr(key_str.ptr() + key_str_byte_pos, key_str_byte_num[i]);
      if (OB_HASH_NOT_EXIST == (ret = ret_map.get_refactored(key_iter_str, val_iter_str))) {
        ret = OB_SUCCESS;
        buf_len = key_str_byte_num[i];
        buf = static_cast<char*>(allocator.alloc(buf_len));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(key_str.length()));
        } else {
          MEMCPY(buf, key_str.ptr() + key_str_byte_pos, buf_len);
          key_iter_str.assign_ptr(buf, buf_len);
          key_str_byte_pos += buf_len;
        }

        if (OB_SUCC(ret)) {
          if (i >= val_str_byte_num.count()) {
            val_iter_str.reset();
          } else {
            buf_len = val_str_byte_num[i];
            buf = static_cast<char*>(allocator.alloc(buf_len));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed", K(key_str.length()));
            } else {
              MEMCPY(buf, val_str.ptr() + val_str_byte_pos, buf_len);
              val_iter_str.assign_ptr(buf, buf_len);
              val_str_byte_pos += buf_len;
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_SUCCESS != (ret = ret_map.set_refactored(key_iter_str, val_iter_str))) {
            LOG_WARN("ret_map.set_refactored failed", K(key_iter_str), K(val_iter_str), K(ret));
          }
          LOG_DEBUG("insert done", K(ret), K(key_iter_str), K(val_iter_str));
        }
      } else if (OB_SUCC(ret)) {
        LOG_DEBUG("entry exist", K(ret), K(key_iter_str), K(val_iter_str));
        key_str_byte_pos += key_str_byte_num[i];
        if (i < val_str_byte_num.count()) {
          val_str_byte_pos += val_str_byte_num[i];
        }
      } else {
        LOG_WARN("get key from hash map filed", K(ret), K(key_iter_str));
      }
    }
  }
  return ret;
}

int calc_translate_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // translate(ori_str, from_str, to_str);
  ObDatum *ori_datum = NULL;
  ObDatum *from_datum = NULL;
  ObDatum *to_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, ori_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, from_datum)) ||
      OB_FAIL(expr.args_[2]->eval(ctx, to_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(ori_datum), KP(from_datum), KP(to_datum), K(expr));
  } else if (ori_datum->is_null() || from_datum->is_null() || to_datum->is_null()) {
    res_datum.set_null();
  } else {
    ObString ori_str = ori_datum->get_string();
    ObString from_str = from_datum->get_string();
    ObString to_str = to_datum->get_string();
    ObString res_str;
    bool is_null = false;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObCollationType cs_type = expr.datum_meta_.cs_type_;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_FAIL(ObExprTranslate::translate_hashmap(ori_str, from_str, to_str, cs_type,
            alloc_guard.get_allocator(), res_alloc, res_str, is_null))) {
      LOG_WARN("translate_hashmap failed", K(ret), K(ori_str), K(from_str), K(to_str));
    } else if (is_null || 0 == res_str.length()) {
      // oracle模式下结果长度为0，需要把结果设置为null
      res_datum.set_null();
    } else {
      res_datum.set_string(res_str);
    }
  }
  return ret;
}

int calc_translate_using_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *ori_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, ori_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(ori_datum), K(expr));
  } else if (ori_datum->is_null()) {
    res_datum.set_null();
  } else {
    res_datum.set_string(ori_datum->get_string());
  }
  return ret;
}

int ObExprTranslate::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (2 == rt_expr.arg_cnt_) {
    CK(rt_expr.args_[0]->datum_meta_.type_ == rt_expr.datum_meta_.type_);
    CK(rt_expr.args_[0]->datum_meta_.cs_type_ == rt_expr.datum_meta_.cs_type_);
    rt_expr.eval_func_ = calc_translate_using_expr;
  } else if (3 == rt_expr.arg_cnt_) {
    rt_expr.eval_func_ = calc_translate_expr;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid number of argument", K(ret), K(rt_expr.arg_cnt_), K(raw_expr));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
