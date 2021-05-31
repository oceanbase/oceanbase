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
#include "sql/engine/expr/ob_expr_ctxcat_analyzer.h"
#include "sql/engine/expr/ob_iter_expr_set_operation.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"

#define DEFAULT_FTB_SYNTAX "+ -><()~*:\"\"&|";
const char* ft_boolean_syntax = DEFAULT_FTB_SYNTAX;
/* Boolean search operators */
#define FTB_YES (ft_boolean_syntax[0])
#define FTB_EGAL (ft_boolean_syntax[1])
#define FTB_NO (ft_boolean_syntax[2])
#define FTB_INC (ft_boolean_syntax[3])
#define FTB_DEC (ft_boolean_syntax[4])
#define FTB_LBR (ft_boolean_syntax[5])
#define FTB_RBR (ft_boolean_syntax[6])
#define FTB_NEG (ft_boolean_syntax[7])
#define FTB_TRUNC (ft_boolean_syntax[8])
#define FTB_LQUOT (ft_boolean_syntax[10])
#define FTB_RQUOT (ft_boolean_syntax[11])
#define true_word_char(ctype, c) (!(ctype & (_MY_PNT | _MY_SPC | _MY_CTR | _MY_B)))
namespace oceanbase {
using namespace common;
namespace sql {
int ObExprCtxCatAnalyzer::parse_search_keywords(
    ObObj& keyword_text, ObMatchAgainstMode mode_flag, ObIterExprOperator*& search_tree, ObIArray<ObObj>& keywords)
{
  int ret = OB_SUCCESS;

  search_tree = NULL;
  keywords.reset();
  if (OB_LIKELY(NATURAL_LANGUAGE_MODE == mode_flag)) {
    if (OB_FAIL(ft_get_simple_words(keyword_text, search_tree, keywords))) {
      LOG_WARN("fulltext get simple words failed", K(ret));
    }
  } else if (OB_FAIL(ft_get_boolean_words(keyword_text, search_tree, keywords))) {
    LOG_WARN("fulltext get simple words failed", K(ret));
  }

  return ret;
}

int ObExprCtxCatAnalyzer::create_set_op_tree(
    ObIArray<ObIndexScanIterExpr*>& set_keys, ObItemType set_op_type, ObIterExprOperator*& search_tree)
{
  int ret = OB_SUCCESS;
  ObSetOpIterExpr* set_iter = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < set_keys.count(); ++i) {
    if (OB_ISNULL(search_tree)) {
      search_tree = set_keys.at(i);
    } else if (OB_FAIL(expr_factory_.alloc(set_op_type, set_iter))) {
      LOG_WARN("create union row iterator expr failed", K(ret));
    } else {
      set_iter->set_left_iter(search_tree);
      set_iter->set_right_iter(set_keys.at(i));
      search_tree = set_iter;
    }
  }
  return ret;
}

int ObExprCtxCatAnalyzer::ft_get_simple_words(
    ObObj& keyword_text, ObIterExprOperator*& search_tree, ObIArray<ObObj>& keywords)
{
  int ret = OB_SUCCESS;
  ObString text;
  if (OB_FAIL(keyword_text.get_string(text))) {
    LOG_WARN("get string from keyword text failed", K(ret));
  } else {
    uchar* start = reinterpret_cast<uchar*>(text.ptr());
    uchar* end = start + text.length();
    uchar* curr = start;
    ObCollationType cs_type = keyword_text.get_collation_type();
    ObSEArray<ObIndexScanIterExpr*, 4> union_keys;
    const ObCharsetHandler* cset_handle = NULL;
    const ObCharsetInfo* cset_info = NULL;
    int32_t ctype = 0;
    if (OB_ISNULL(cset_info = ObCharset::get_charset(cs_type)) || OB_ISNULL(cset_handle = cset_info->cset)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cs_type is invalid", K(cs_type), K(cset_info), K(cset_handle));
    }
    while (OB_SUCC(ret) && curr < end) {
      // skip the non-true-word at the head fo the string
      bool is_break = false;
      int32_t mbl = 0;
      while (curr < end && !is_break) {
        mbl = cset_handle->ctype(cset_info, &ctype, curr, end);
        if (true_word_char(ctype, *curr)) {
          is_break = true;
        } else {
          curr += mbl > 0 ? mbl : 1;
        }
      }
      start = curr;
      if (is_break) {
        curr += mbl > 0 ? mbl : 1;
      }
      // to find the delimiter
      is_break = false;
      while (curr < end && !is_break) {
        mbl = cset_handle->ctype(cset_info, &ctype, curr, end);
        if (!true_word_char(ctype, *curr)) {
          is_break = true;
        } else {
          curr += mbl > 0 ? mbl : 1;
        }
      }
      ObString keyword(curr - start, reinterpret_cast<char*>(start));
      if (is_break) {
        curr += mbl > 0 ? mbl : 1;
      }

      if (!keyword.empty()) {
        ObIndexScanIterExpr* keyword_iter = NULL;
        ObObj result_obj;
        result_obj.set_varchar(keyword);
        result_obj.set_collation_type(cs_type);
        if (OB_FAIL(keywords.push_back(result_obj))) {
          LOG_WARN("store keyword failed", K(ret));
        } else if (OB_FAIL(expr_factory_.alloc(T_ROW_ITER_EXPR, keyword_iter))) {
          LOG_WARN("create index scan iterator expr failed", K(ret));
        } else if (OB_ISNULL(keyword_iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("keyword iterator is null");
        } else if (FALSE_IT(keyword_iter->set_iter_idx(keywords.count() - 1))) {
          // do nothing
        } else if (OB_FAIL(union_keys.push_back(keyword_iter))) {
          LOG_WARN("push back union keys failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_set_op_tree(union_keys, T_OP_UNION, search_tree))) {
        LOG_WARN("create union op tree failed", K(ret));
      }
    }
  }

  return ret;
}

int ObExprCtxCatAnalyzer::ft_get_boolean_words(
    ObObj& keyword_text, ObIterExprOperator*& search_tree, ObIArray<ObObj>& keywords)
{
  int ret = OB_SUCCESS;
  ObString text;
  if (OB_FAIL(keyword_text.get_string(text))) {
    LOG_WARN("get string from keyword text failed", K(ret));
  } else {
    uchar* start = reinterpret_cast<uchar*>(text.ptr());
    uchar* end = start + text.length();
    uchar* curr = start;
    ObCollationType cs_type = keyword_text.get_collation_type();
    ObSEArray<ObIndexScanIterExpr*, 4> union_keys;
    ObSEArray<ObIndexScanIterExpr*, 4> intersect_keys;
    ObSEArray<ObIndexScanIterExpr*, 4> except_keys;
    const ObCharsetHandler* cset_handle = NULL;
    const ObCharsetInfo* cset_info = NULL;
    int32_t ctype = 0;

    if (OB_ISNULL(cset_info = ObCharset::get_charset(cs_type)) || OB_ISNULL(cset_handle = cset_info->cset)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cs_type is invalid", K(cs_type), K(cset_info), K(cset_handle));
    }
    while (OB_SUCC(ret) && curr < end) {
      bool is_break = false;
      int32_t mbl = 0;
      ObItemType set_op_type = T_OP_UNION;
      // skip the non-true-word at the head fo the string
      while (curr < end && !is_break) {
        mbl = cset_handle->ctype(cset_info, &ctype, curr, end);
        if (true_word_char(ctype, *curr)) {
          is_break = true;
          if (curr > start) {
            if (FTB_YES == *(curr - 1)) {
              set_op_type = T_OP_INTERSECT;
            } else if (FTB_NO == *(curr - 1)) {
              set_op_type = T_OP_EXCEPT;
            }
          }
        } else {
          curr += mbl > 0 ? mbl : 1;
        }
      }
      start = curr;
      if (is_break) {
        curr += mbl > 0 ? mbl : 1;
      }
      // to find the delimiter
      is_break = false;
      while (curr < end && !is_break) {
        mbl = cset_handle->ctype(cset_info, &ctype, curr, end);
        if (!true_word_char(ctype, *curr)) {
          is_break = true;
        } else {
          curr += mbl > 0 ? mbl : 1;
        }
      }
      ObString keyword(curr - start, reinterpret_cast<char*>(start));
      if (is_break) {
        curr += mbl > 0 ? mbl : 1;
      }
      if (!keyword.empty()) {
        ObIndexScanIterExpr* keyword_iter = NULL;
        ObObj result_obj;
        result_obj.set_varchar(keyword);
        result_obj.set_collation_type(cs_type);
        if (OB_FAIL(keywords.push_back(result_obj))) {
          LOG_WARN("store keyword failed", K(ret));
        } else if (OB_FAIL(expr_factory_.alloc(T_ROW_ITER_EXPR, keyword_iter))) {
          LOG_WARN("create index scan iterator expr failed", K(ret));
        } else if (OB_ISNULL(keyword_iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("keyword iterator is null");
        } else if (FALSE_IT(keyword_iter->set_iter_idx(keywords.count() - 1))) {
          // do nothing
        }
        if (OB_SUCC(ret)) {
          switch (set_op_type) {
            case T_OP_UNION:
              ret = union_keys.push_back(keyword_iter);
              break;
            case T_OP_INTERSECT:
              ret = intersect_keys.push_back(keyword_iter);
              break;
            case T_OP_EXCEPT:
              ret = except_keys.push_back(keyword_iter);
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected set op type", K(set_op_type));
              break;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (union_keys.count() <= 0 && intersect_keys.count() <= 0) {
        // match(b, c) against('-keyword')>0 means empty set
        keywords.reset();
      } else if (OB_FAIL(create_set_op_tree(union_keys, T_OP_UNION, search_tree))) {
        LOG_WARN("create union op tree failed", K(ret));
      } else if (OB_FAIL(create_set_op_tree(intersect_keys, T_OP_INTERSECT, search_tree))) {
        LOG_WARN("create intersect op tree failed", K(ret));
      } else if (OB_FAIL(create_set_op_tree(except_keys, T_OP_EXCEPT, search_tree))) {
        LOG_WARN("create except op tree failed", K(ret));
      }
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
