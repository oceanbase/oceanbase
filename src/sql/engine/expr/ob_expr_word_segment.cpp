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

#include "sql/engine/expr/ob_expr_word_segment.h"
#include "lib/word_segment/ob_word_segment.h"

namespace oceanbase {
using namespace common;
namespace sql {
ObExprWordSegment::ObExprWordSegment(ObIAllocator& allocator)
    : ObFuncExprOperator(allocator, T_FUN_SYS_WORD_SEGMENT, N_WORD_SEGMENT, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprWordSegment::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  ObLength max_len = 0;
  for (int64_t i = 0; /*OB_SUCCESS == ret && */ i < param_num; ++i) {
    types[i].set_calc_type(ObVarcharType);
    max_len += types[i].get_length();
  }
  type.set_varchar();
  type.set_length(max_len);
  ret = aggregate_charsets_for_string_result(type, types, param_num, type_ctx.get_coll_type());
  return ret;
}

int ObExprWordSegment::calc_resultN(
    ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 32> words;
  ObWordSegment ws;
  common::hash::ObHashSet<ObObj, common::hash::NoPthreadDefendMode> hashset;
  const int64_t BUCKET_SIZE = 64;
  if (OB_ISNULL(objs_array) || OB_ISNULL(expr_ctx.calc_buf_) || param_num < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument passed in", K(objs_array), K(expr_ctx.calc_buf_), K(param_num), K(ret));
  } else if (OB_FAIL(ws.init(tokenizer_))) {
    LOG_WARN("failed to init ObWordSegment", K(ret));
  } else if (OB_FAIL(hashset.create(BUCKET_SIZE))) {
    LOG_ERROR("failed to create hashset", K(ret));
  } else {
    ObSEArray<ObObj, 8> tmp_result;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      tmp_result.reset();
      if (objs_array[i].is_null()) {
      } else if (!objs_array[i].is_string_type()) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("Unsupported type to word segment", K(objs_array[i].get_type()), K(ret));
      } else if (OB_FAIL(ws.segment(objs_array[i], tmp_result))) {
        LOG_WARN("failed to segment string in index cell", K(i), K(objs_array[i]), K(ret));
      } else {
        ObObj word;
        for (int64_t j = 0; OB_SUCC(ret) && j < tmp_result.count(); j++) {
          word.reset();
          int hash_ret = hashset.exist_refactored(tmp_result.at(j));
          if (OB_HASH_EXIST == hash_ret) {
            // do nothing
          } else if (OB_HASH_NOT_EXIST != hash_ret) {
            ret = hash_ret;
            LOG_WARN("failed to check exist in hashset", K(ret));
          } else if (OB_FAIL(ob_write_obj(*expr_ctx.calc_buf_, tmp_result.at(j), word))) {
            LOG_WARN("failed to copy string", K(i), K(tmp_result.at(j).get_string()), K(ret));
          } else if (OB_FAIL(hashset.set_refactored(word))) {
            LOG_WARN("failed to set item", K(word), K(ret));
          } else if (OB_FAIL(words.push_back(word.get_string()))) {
            LOG_WARN("failed to push back word", K(ret));
          } else { /*do nothing*/
          }
        }
        // recover original data and free memory allocated by ws regardless of any error
        int tmp_ret = ws.reset();
        if (OB_SUCCESS != tmp_ret) {
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
          LOG_WARN("Fail to reset ws", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !words.empty()) {
      int64_t len = words.count();  // for delimiters
      for (int64_t i = 0; OB_SUCC(ret) && i < words.count(); ++i) {
        len += words.at(i).length();
      }
      if (OB_SUCC(ret)) {
        char* ptr = static_cast<char*>(expr_ctx.calc_buf_->alloc(len));
        if (OB_UNLIKELY(NULL == ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(expr_ctx.calc_buf_), K(len), K(ret));
        } else {
          char* cur_ptr = ptr;
          for (int64_t i = 0; OB_SUCC(ret) && i < words.count(); ++i) {
            MEMCPY(cur_ptr, words.at(i).ptr(), words.at(i).length());
            cur_ptr += words.at(i).length();
            cur_ptr++[0] = ',';
          }
          if (OB_SUCC(ret)) {
            ObString str(len, ptr);
            result.set_varchar(str);
            if (!result.is_null()) {
              result.set_collation(result_type_);
            }
          }
        }
      }
    }
    // must destory hash anyway and free unused memory
    hashset.destroy();
    for (int64_t i = 0; OB_SUCC(ret) && i < words.count(); ++i) {
      expr_ctx.calc_buf_->free(words.at(i).ptr());
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
