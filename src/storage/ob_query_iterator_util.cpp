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

#include "share/ob_worker.h"
#include "storage/ob_query_iterator_util.h"

namespace oceanbase {
using namespace common;
namespace storage {

int pad_column(const ObAccuracy accuracy, common::ObIAllocator& padding_alloc, common::ObObj& cell)
{
  int ret = OB_SUCCESS;
  if (cell.is_fixed_len_char_type()) {
    char* buf = NULL;
    ObLength length = accuracy.get_length();  // byte or char length
    int32_t cell_strlen = 0;                  // byte or char length
    int32_t pad_whitespace_length = 0;        // pad whitespace length
    ObString space_pattern = ObCharsetUtils::get_const_str(cell.get_collation_type(), OB_PADDING_CHAR);

    if (OB_FAIL(cell.get_char_length(accuracy, cell_strlen, share::is_oracle_mode()))) {
      STORAGE_LOG(WARN, "Fail to get char length, ", K(ret));
    } else {
      if (cell_strlen < length) {
        pad_whitespace_length = length - cell_strlen;
        if (ObCharset::is_cs_nonascii(cell.get_collation_type())) {
          pad_whitespace_length *= space_pattern.length();
        }
      }
      if (pad_whitespace_length > 0) {
        int32_t buf_len = cell.get_val_len() + pad_whitespace_length;
        if (NULL == (buf = (char*)padding_alloc.alloc(buf_len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(ERROR, "no memory", K(ret));
        } else {
          MEMMOVE(buf, cell.get_string_ptr(), cell.get_val_len());
          if (ObCharset::is_cs_nonascii(cell.get_collation_type())) {
            for (int32_t len_i = cell.get_val_len(); len_i + space_pattern.length() <= buf_len;
                 len_i += space_pattern.length()) {
              MEMCPY(buf + len_i, space_pattern.ptr(), space_pattern.length());
            }
            STORAGE_LOG(DEBUG, "multi-byte padding", K(pad_whitespace_length), K(cell));
          } else {
            MEMSET(buf + cell.get_val_len(), OB_PADDING_CHAR, pad_whitespace_length);
          }
          // watch out !!! in order to deep copy an ObObj instance whose type is char or varchar,
          // set_collation_type() should be revoked. But here no need to set collation type
          cell.set_string(cell.get_type(), ObString(buf_len, buf_len, buf));
        }
      }
    }
  }
  return ret;
}

int pad_column(const common::ObAccuracy accuracy, sql::ObEvalCtx& ctx, sql::ObExpr* expr)
{
  int ret = OB_SUCCESS;
  sql::ObDatum* datum = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if ((datum = &expr->locate_expr_datum(ctx))->is_null()) {
    // do nothing.
  } else if (expr->obj_meta_.is_fixed_len_char_type()) {
    ObCollationType cs_type = expr->datum_meta_.cs_type_;
    ObLength length = accuracy.get_length();  // byte or char length
    int32_t cur_len = 0;                      // byte or char length
    int32_t pad_whitespace_length = 0;        // pad whitespace length
    ObString space_pattern = ObCharsetUtils::get_const_str(cs_type, OB_PADDING_CHAR);

    if (is_oracle_byte_length(share::is_oracle_mode(), accuracy.get_length_semantics())) {
      cur_len = datum->len_;
    } else {
      cur_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, datum->ptr_, datum->len_));
    }
    if (cur_len < length) {
      pad_whitespace_length = length - cur_len;
      if (ObCharset::is_cs_nonascii(cs_type)) {
        pad_whitespace_length *= space_pattern.length();
      }
    }
    if (pad_whitespace_length > 0) {
      const char* old = datum->ptr_;
      const int32_t buf_len = datum->len_ + pad_whitespace_length;
      char* buf = expr->get_str_res_mem(ctx, buf_len);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "no memory", K(ret));
      } else {
        MEMMOVE(buf, old, datum->len_);
        if (ObCharset::is_cs_nonascii(cs_type)) {
          for (int32_t len_i = datum->len_; len_i + space_pattern.length() <= buf_len;
               len_i += space_pattern.length()) {
            MEMCPY(buf + len_i, space_pattern.ptr(), space_pattern.length());
          }
        } else {
          MEMSET(buf + datum->len_, OB_PADDING_CHAR, pad_whitespace_length);
        }
        datum->ptr_ = buf;
        datum->len_ = buf_len;
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
