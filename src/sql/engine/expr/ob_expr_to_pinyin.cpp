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
#include "sql/engine/expr/ob_expr_to_pinyin.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/charset/ob_charset_string_helper.h"
#include "sql/engine/expr/ob_expr_to_pinyin_tab.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprToPinyin::ObExprToPinyin(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TO_PINYIN, N_TO_PINYIN, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprToPinyin::~ObExprToPinyin()
{
}

int ObExprToPinyin::calc_result_type1(ObExprResType &type,
                                      ObExprResType &type1,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
  type.set_varchar();
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  const sql::ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null",K(ret));
  } else {
    type.set_collation_type(lib::is_oracle_mode() ?
      session->get_nls_collation() :
      session->get_local_collation_connection());
  }
  return ret;
}

uint64_t convert_to_sortkey(ObIAllocator &alloc, ObString input) {
  const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
  char *buf = NULL;
  size_t buf_len = cs->coll->strnxfrmlen(cs, cs->mbmaxlen*input.length());
  bool is_valid_unicode_tmp = 1;
  size_t result_len = 0;

  uint64_t sortkey = -1;
  if (OB_ISNULL(buf = static_cast<char*>(alloc.alloc(buf_len)))) {
    int ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret), K(buf_len), K(input));
  } else {
    result_len = cs->coll->strnxfrm(cs,
                                    reinterpret_cast<uchar *>(buf),
                                    buf_len,
                                    buf_len,
                                    reinterpret_cast<const uchar *>(input.ptr()),
                                    input.length(),
                                    0,
                                    &is_valid_unicode_tmp);
    uint64_t res = *reinterpret_cast<uint64_t *>(buf);
    alloc.free(buf);
    sortkey = (res % 256) * 256 + (res / 256 % 256);
  }
  return sortkey;
}

bool compare_end(const PinyinPair& a, const PinyinPair& b) {
  return a.end < b.end;
}

ObString convert_word_to_pinyin(ObIAllocator &alloc, ObString input) {
  int ret = OB_SUCCESS;
  uint64_t input_sortkey = convert_to_sortkey(alloc, input);
  ObString result;
  // 根据sortkey转换为拼音
  // 二分查找
  PinyinPair target = {0, input_sortkey, ""};
  PinyinPair *it = std::lower_bound(PINYIN_TABLE, PINYIN_TABLE + PINYIN_COUNT, target, compare_end);
  if(it != PINYIN_TABLE + PINYIN_COUNT &&
    input_sortkey >= it->begin && input_sortkey <= it->end) {
    result = it->pinyin;
  } else {
    result = input;
  }
  return result;
}

int ObExprToPinyin::eval_to_pinyin(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;

  ObDatum *input = NULL;
  ObString input_str;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  ObIAllocator &res_alloc = ctx.get_expr_res_alloc();
  const sql::ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();

  if (OB_FAIL(expr.args_[0]->eval(ctx, input))) {
    LOG_WARN("fail to eval", K(ret), KPC(expr.args_[0]));
  } else if (input->is_null()) {
    expr_datum.set_null();
    return ret;
  } else {
    input_str = input->get_string();
  }

  const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
  size_t buf_len = cs->mbmaxlen*input_str.length();
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char*>(calc_alloc.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret), K(buf_len), K(input_str));
  } else {
    struct Functor {
      Functor(char *buf, int64_t &off, ObIAllocator &alloc) : buf(buf), off(off), calc_alloc(alloc) {}
      char *buf;
      int64_t &off;
      ObIAllocator &calc_alloc;

      int operator() (const ObString &str, ob_wc_t wchar) {
        int ret = OB_SUCCESS;
        ObString pinyin = convert_word_to_pinyin(calc_alloc, str);
        if(!pinyin.empty()) {
          MEMCPY(buf + off, pinyin.ptr(), pinyin.length());
          off += pinyin.length();
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
        return ret;
      }
    };
    int64_t off = 0;
    Functor temp_handler(buf, off, calc_alloc);
    ObCharsetType charset_type = ObCharset::charset_type_by_coll(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
    ObFastStringScanner::foreach_char(input_str, charset_type, temp_handler);
    ObString converted_result;
    OZ(ObExprUtil::convert_string_collation(ObString(off, buf),
                                        CS_TYPE_UTF8MB4_ZH_0900_AS_CS,
                                        converted_result,
                                        lib::is_oracle_mode() ? session->get_nls_collation() : session->get_local_collation_connection(),
                                        res_alloc));
    expr_datum.set_string(converted_result);
  }
  return ret;
}

int ObExprToPinyin::eval_to_pinyin_batch(
  const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  LOG_DEBUG("eval to_pinyin in batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);
  const sql::ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();

  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("failed to eval batch result input", K(ret));
    } else {
      ObDatum *datum_array = expr.args_[0]->locate_batch_datums(ctx);
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      ObIAllocator &res_alloc = ctx.get_expr_res_alloc();
      const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);

      for(int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (skip.at(j) || eval_flags.at(j)) {
          continue;
        } else if (datum_array[j].is_null()) {
          results[j].set_null();
          eval_flags.set(j);
        } else {
          ObString input_str = datum_array[j].get_string();
          int64_t off = 0;
          char *buf = NULL;
          size_t buf_len = cs->mbmaxlen*input_str.length();
          if (OB_ISNULL(buf = static_cast<char *>(calc_alloc.alloc(buf_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc buf", K(ret), K(buf_len), K(input_str));
          } else {
            struct Functor {
              Functor(char *buf, int64_t &off, ObIAllocator &alloc) : buf(buf), off(off), calc_alloc(alloc) {}
              char *buf;
              int64_t &off;
              ObIAllocator &calc_alloc;
              int operator() (const ObString &str, ob_wc_t wchar) {
                int ret = OB_SUCCESS;
                ObString pinyin = convert_word_to_pinyin(calc_alloc, str);
                if(!pinyin.empty()) {
                  MEMCPY(buf + off, pinyin.ptr(), pinyin.length());
                  off += pinyin.length();
                } else {
                  ret = OB_ERR_UNEXPECTED;
                }
                return ret;
              }
            };
            int64_t off = 0;
            Functor temp_handler(buf, off, calc_alloc);
            ObCharsetType charset_type = ObCharset::charset_type_by_coll(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
            ObFastStringScanner::foreach_char(input_str, charset_type, temp_handler);
            ObString converted_result;
            OZ(ObExprUtil::convert_string_collation(ObString(off, buf),
                                                CS_TYPE_UTF8MB4_ZH_0900_AS_CS,
                                                converted_result,
                                                lib::is_oracle_mode() ? session->get_nls_collation() : session->get_local_collation_connection(),
                                                res_alloc));
            results[j].set_string(converted_result);
            eval_flags.set(j);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprToPinyin::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprToPinyin::eval_to_pinyin;
  if (rt_expr.args_[0]->is_batch_result()) {
    rt_expr.eval_batch_func_ = ObExprToPinyin::eval_to_pinyin_batch;
  }
  return OB_SUCCESS;
}

} //namespace sql
} //namespace oceanbase
