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

#include "ob_storage_util.h"
#include "lib/worker.h"
#include "share/datum/ob_datum.h"
#include "share/object/ob_obj_cast.h"
#include "share/vector/ob_discrete_format.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
static const ObString OB_DEFAULT_PADDING_STRING(1, 1, &OB_PADDING_CHAR);

OB_INLINE static const ObString get_padding_str(ObCollationType coll_type)
{
  if (!ObCharset::is_cs_nonascii(coll_type)) {
    return OB_DEFAULT_PADDING_STRING;
  } else {
    return ObCharsetUtils::get_const_str(coll_type, OB_PADDING_CHAR);
  }
}

OB_INLINE static void append_padding_pattern(const ObString &space_pattern,
                                             const int32_t offset,
                                             const int32_t buf_len,
                                             char *&buf,
                                             int32_t &true_len)
{
  true_len = offset;
  if (OB_UNLIKELY((buf_len - offset) < space_pattern.length())) {
  } else if (1 == space_pattern.length()) {
    MEMSET(buf + offset, space_pattern[0], buf_len - offset);
    true_len = buf_len;
  } else {
    for (int32_t i = offset; i <= (buf_len - space_pattern.length()); i += space_pattern.length()) {
      MEMCPY(buf + i, space_pattern.ptr(), space_pattern.length());
      true_len += space_pattern.length();
    }
  }
}

OB_INLINE static int pad_on_local_buf(const ObString &space_pattern,
                                      int32_t pad_whitespace_length,
                                      common::ObIAllocator &padding_alloc,
                                      const char *&ptr,
                                      uint32_t &length)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const int32_t pad_len = length + pad_whitespace_length * space_pattern.length();
  const int64_t buf_len = lib::is_oracle_mode() ? MIN(pad_len, OB_MAX_ORACLE_CHAR_LENGTH_BYTE) : pad_len;
  if (OB_ISNULL((buf = (char*) padding_alloc.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "no memory", K(ret));
  } else {
    int32_t true_len = 0;
    MEMCPY(buf, ptr, length);
    append_padding_pattern(space_pattern, length, buf_len, buf, true_len);
    ptr = buf;
    length = true_len;
  }
  return ret;
}

int pad_column(const ObAccuracy accuracy, common::ObIAllocator &padding_alloc, common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  if (cell.is_fixed_len_char_type()) {
    ObLength length = accuracy.get_length(); // byte or char length
    int32_t cell_strlen = 0; // byte or char length
    const ObString space_pattern = get_padding_str(cell.get_collation_type());
    if (OB_FAIL(cell.get_char_length(accuracy, *(reinterpret_cast<int32_t *>(&cell_strlen)), lib::is_oracle_mode()))) {
      STORAGE_LOG(WARN, "Fail to get char length, ", K(ret));
    } else {
      if (cell_strlen < length) {
        uint32_t cell_len = cell.get_val_len();
        const char *ptr = cell.get_string_ptr();
        if (OB_FAIL(pad_on_local_buf(space_pattern, (length - cell_strlen), padding_alloc,
                                     ptr, cell_len))) {
          STORAGE_LOG(WARN, "Fail to pad on local buf, ", K(ret), K(cell), K(length), K(cell_strlen));
        } else {
          // watch out !!! in order to deep copy an ObObj instance whose type is char or varchar,
          // set_collation_type() should be revoked. But here no need to set collation type
          cell.set_string(cell.get_type(), ObString(cell_len, cell_len, ptr));
        }
      }
    }
  }
  return ret;
}

int pad_column(const ObObjMeta &obj_meta, const ObAccuracy accuracy, common::ObIAllocator &padding_alloc, blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    // do nothing.
  } else if (obj_meta.is_fixed_len_char_type()) {
    ObLength length = accuracy.get_length(); // byte or char length
    const common::ObCollationType cs_type = obj_meta.get_collation_type();
    const ObString space_pattern = get_padding_str(cs_type);
    int32_t cur_len = 0; // byte or char length
    bool is_ascii = can_do_ascii_optimize(cs_type) && is_ascii_str(datum.ptr_, datum.pack_);
    if (is_ascii || is_oracle_byte_length(lib::is_oracle_mode(), accuracy.get_length_semantics())) {
      cur_len = datum.pack_;
    } else {
      cur_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, datum.ptr_, datum.pack_));
    }
    if (cur_len < length &&
        OB_FAIL(pad_on_local_buf(space_pattern, length - cur_len, padding_alloc, datum.ptr_, datum.pack_))) {
      STORAGE_LOG(WARN, "fail to pad on padding allocator", K(ret), K(length), K(cur_len), K(datum));
    }
  }
  return ret;
}

int pad_column(const common::ObAccuracy accuracy, sql::ObEvalCtx &ctx, sql::ObExpr &expr)
{
  int ret = OB_SUCCESS;
  sql::ObDatum &datum = expr.locate_expr_datum(ctx);
  if (datum.is_null()) {
    // do nothing.
  } else if (expr.obj_meta_.is_fixed_len_char_type()) {
    ObLength length = accuracy.get_length(); // byte or char length
    const common::ObCollationType cs_type = expr.datum_meta_.cs_type_;
    const ObString space_pattern = get_padding_str(cs_type);
    int32_t cur_len = 0; // byte or char length
    bool is_ascii = can_do_ascii_optimize(cs_type) && is_ascii_str(datum.ptr_, datum.pack_);
    if (is_ascii || is_oracle_byte_length(lib::is_oracle_mode(), accuracy.get_length_semantics())) {
      cur_len = datum.pack_;
    } else {
      cur_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, datum.ptr_, datum.pack_));
    }
    if (cur_len < length) {
      char *ptr = nullptr;
      const int32_t pad_len = datum.pack_ + (length - cur_len) * space_pattern.length();
      const int64_t buf_len = lib::is_oracle_mode() ? MIN(pad_len, OB_MAX_ORACLE_CHAR_LENGTH_BYTE) : pad_len;
      if (OB_ISNULL(ptr = expr.get_str_res_mem(ctx, buf_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "no memory", K(ret));
      } else {
        int32_t true_len = 0;
        MEMMOVE(ptr, datum.ptr_, datum.pack_);
        append_padding_pattern(space_pattern, datum.pack_, buf_len, ptr, true_len);
        datum.ptr_ = ptr;
        datum.pack_ = true_len;
      }
    }
  }
  return ret;
}

int pad_on_datums(const common::ObAccuracy accuracy,
                  const common::ObCollationType cs_type,
                  common::ObIAllocator &padding_alloc,
                  int64_t row_count,
                  common::ObDatum *&datums)
{
  int ret = OB_SUCCESS;
  ObLength length = accuracy.get_length(); // byte or char length
  const ObString space_pattern = get_padding_str(cs_type);
  bool is_oracle_byte = is_oracle_byte_length(lib::is_oracle_mode(), accuracy.get_length_semantics());
  char *buf = nullptr;
  if (1 == length) {
    int32_t buf_len = space_pattern.length();
    if (OB_ISNULL((buf = (char*) padding_alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "no memory", K(ret));
    } else {
      int32_t true_len = 0;
      append_padding_pattern(space_pattern, 0, buf_len, buf, true_len);
      for (int64_t i = 0; i < row_count; i++) {
        common::ObDatum &datum = datums[i];
        if (datum.is_null()) {
          // do nothing
        } else if (0 == datum.pack_){
          datum.ptr_ = buf;
          datum.pack_ = true_len;
        }
      }
    }
  } else if (can_do_ascii_optimize(cs_type)) {
    int32_t buf_len = length * space_pattern.length() * row_count;
    if (OB_ISNULL(buf = (char*) padding_alloc.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "no memory", K(ret));
    } else {
      char *ptr = buf;
      MEMSET(buf, OB_PADDING_CHAR, buf_len);
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        common::ObDatum &datum = datums[i];
        if (datum.is_null()) {
          // do nothing
        } else {
          if (is_oracle_byte || is_ascii_str(datum.ptr_, datum.pack_)) {
            if (datum.pack_ < length) {
              MEMCPY(ptr, datum.ptr_, datum.pack_);
              datum.ptr_ = ptr;
              datum.pack_ = length;
              ptr = ptr + length;
            }
          } else {
            int32_t cur_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, datum.ptr_, datum.pack_));
            if (cur_len < length &&
                OB_FAIL(pad_on_local_buf(space_pattern, length - cur_len, padding_alloc, datum.ptr_, datum.pack_))) {
              STORAGE_LOG(WARN, "fail to pad on padding allocator", K(ret), K(length), K(cur_len), K(datum));
            }
          }
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      common::ObDatum &datum = datums[i];
      if (datum.is_null()) {
        // do nothing
      } else {
        int32_t cur_len = 0; // byte or char length
        if (is_oracle_byte) {
          cur_len = datum.pack_;
        } else {
          cur_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, datum.ptr_, datum.pack_));
        }
        if (cur_len < length &&
            OB_FAIL(pad_on_local_buf(space_pattern, length - cur_len, padding_alloc, datum.ptr_, datum.pack_))) {
          STORAGE_LOG(WARN, "fail to pad on padding allocator", K(ret), K(length), K(cur_len), K(datum));
        }
      }
    }
  }
  return ret;
}

int pad_on_rich_format_columns(const common::ObAccuracy accuracy,
                               const common::ObCollationType cs_type,
                               const int64_t row_count,
                               const int64_t vec_offset,
                               common::ObIAllocator &padding_alloc,
                               sql::ObExpr &expr,
                               sql::ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(VectorFormat::VEC_DISCRETE != expr.get_format(eval_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected vector format for padding column", K(expr.get_format(eval_ctx)));
  } else {
    ObDiscreteFormat *discrete_format = static_cast<ObDiscreteFormat *>(expr.get_vector(eval_ctx));
    ObLength *lens = discrete_format->get_lens();
    char **ptrs = discrete_format->get_ptrs();
    sql::ObBitVector *nulls = discrete_format->get_nulls();
    ObLength length = accuracy.get_length(); // byte or char length
    const ObString space_pattern = get_padding_str(cs_type);
    bool is_oracle_byte = is_oracle_byte_length(lib::is_oracle_mode(), accuracy.get_length_semantics());
    char *buf = nullptr;
    if (1 == length) {
      int32_t buf_len = space_pattern.length();
      if (OB_ISNULL((buf = (char*) padding_alloc.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "no memory", K(ret));
      } else {
        int32_t true_len = 0;
        append_padding_pattern(space_pattern, 0, buf_len, buf, true_len);
        for (int64_t i = vec_offset; i < vec_offset + row_count; i++) {
          if (nulls->at(i)) {
            // do nothing
          } else if (0 == lens[i]){
            ptrs[i] = buf;
            lens[i] = true_len;
          }
        }
      }
    } else if (can_do_ascii_optimize(cs_type)) {
      int32_t buf_len = length * space_pattern.length() * row_count;
      if (OB_ISNULL(buf = (char*) padding_alloc.alloc(buf_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "no memory", K(ret));
      } else {
        char *ptr = buf;
        MEMSET(buf, OB_PADDING_CHAR, buf_len);
        for (int64_t i = vec_offset; OB_SUCC(ret) && i < vec_offset + row_count; i++) {
          if (nulls->at(i)) {
            // do nothing
          } else {
            if (is_oracle_byte || is_ascii_str(ptrs[i], lens[i])) {
              if (lens[i] < length) {
                MEMCPY(ptr, ptrs[i], lens[i]);
                ptrs[i] = ptr;
                lens[i] = length;
                ptr = ptr + length;
              }
            } else {
              int32_t cur_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, ptrs[i], lens[i]));
              if (cur_len < length &&
                  OB_FAIL(pad_on_local_buf(space_pattern, length - cur_len, padding_alloc, (const char *&)ptrs[i], (uint32_t &)lens[i]))) {
                STORAGE_LOG(WARN, "fail to pad on padding allocator", K(ret), K(length), K(cur_len));
              }
            }
          }
        }
      }
    } else {
      for (int64_t i = vec_offset; OB_SUCC(ret) && i < vec_offset + row_count; i++) {
        if (nulls->at(i)) {
          // do nothing
        } else {
          int32_t cur_len = 0; // byte or char length
          if (is_oracle_byte) {
            cur_len = lens[i];
          } else {
            cur_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, ptrs[i], lens[i]));
          }
          if (cur_len < length &&
              OB_FAIL(pad_on_local_buf(space_pattern, length - cur_len, padding_alloc, (const char *&)ptrs[i], (uint32_t &)lens[i]))) {
            STORAGE_LOG(WARN, "fail to pad on padding allocator", K(ret), K(length), K(cur_len));
          }
        }
      }
    }
  }
  return ret;
}

int distribute_attrs_on_rich_format_columns(const int64_t row_count, const int64_t vec_offset,
                                            sql::ObExpr &expr, sql::ObEvalCtx &eval_ctx)
{
  return ObArrayExprUtils::batch_dispatch_array_attrs(eval_ctx, expr, vec_offset, row_count);
}

int cast_obj(const common::ObObjMeta &src_meta,
             common::ObIAllocator &cast_allocator,
             common::ObObj &obj)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass type_class = src_meta.get_type_class();
  bool need_cast = true;
  bool empty_str = false;

  switch (type_class) {
    case ObIntTC: {
      need_cast = src_meta.get_type() < obj.get_type();
      break;
    }
    case ObUIntTC: {
      need_cast = (obj.get_int() < 0
                   || static_cast<uint64_t>(obj.get_int()) > UINT_MAX_VAL[src_meta.get_type()]);
      break;
    }
    case ObFloatTC: {
      need_cast = (obj.get_float() < 0.0);
      break;
    }
    case ObDoubleTC: {
      need_cast = (obj.get_double() < 0.0);
      break;
    }
    case ObNumberTC: {
      need_cast = obj.is_negative_number();
      break;
    }
    case ObStringTC: {
      need_cast = (obj.get_collation_type() != src_meta.get_collation_type());
      empty_str = 0 == obj.get_val_len();
      break;
    }
    case ObTextTC: {
      need_cast = obj.is_inrow() && (obj.get_collation_type() != src_meta.get_collation_type());
      empty_str = 0 == obj.get_val_len();
      break;
    }
    default: {
      need_cast = false;
    }
  }

  if (ObNullType == obj.get_type() || ObExtendType == obj.get_type()) {
  //ignore src_meta type, do nothing, just return obj
  } else if ((obj.get_type() == src_meta.get_type() && !need_cast) || empty_str) {//just change the type
    obj.set_type(src_meta.get_type());
    if (empty_str && ObTextTC == type_class) {
      obj.set_inrow();
    }
  } else {
    //not support data alteration
    ObObj ori_obj = obj;
    int64_t cm_mode = CM_NONE;
    //int to uint not check range bug:
    if(ObIntTC == ori_obj.get_type_class() && ObUIntTC == type_class) {
      obj.set_uint(src_meta.get_type(), static_cast<uint64_t>(ori_obj.get_int()));
    } else if (ObIntTC == ori_obj.get_type_class() && ObBitTC == type_class) {
      obj.set_bit(static_cast<uint64_t>(ori_obj.get_int()));
    } else {
      ObCastCtx cast_ctx(&cast_allocator, NULL, cm_mode, src_meta.get_collation_type());
      if(OB_FAIL(ObObjCaster::to_type(src_meta.get_type(), cast_ctx, ori_obj, obj))) {
        STORAGE_LOG(WARN, "fail to cast obj",
            K(ret), K(ori_obj), K(obj), K(ori_obj.get_type()),
            K(ob_obj_type_str(ori_obj.get_type())),
            K(src_meta.get_type()), K(ob_obj_type_str(src_meta.get_type())));
      }
    }
  }
  return ret;
}

int fill_datums_lob_locator(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const share::schema::ObColumnParam &col_param,
    const int64_t row_cap,
    ObDatum *datums,
    bool reuse_lob_locator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!col_param.get_meta_type().is_lob_storage() ||
                  nullptr == context.lob_locator_helper_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected param", K(ret), K(col_param.get_meta_type()), K(context.lob_locator_helper_));
  } else {
    if (reuse_lob_locator) {
      context.lob_locator_helper_->reuse();
    }
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_cap; ++row_idx) {
      ObDatum &datum = datums[row_idx];
      if (!datum.is_null() && !datum.get_lob_data().in_row_) {
        if (OB_FAIL(context.lob_locator_helper_->fill_lob_locator_v2(datum, col_param, iter_param, context))) {
          STORAGE_LOG(WARN, "Failed to fill lob loactor", K(ret), K(row_idx), K(datum), K(context), K(iter_param));
        }
      }
    }
  }
  return ret;
}

int fill_exprs_lob_locator(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const share::schema::ObColumnParam &col_param,
    sql::ObExpr &expr,
    sql::ObEvalCtx &eval_ctx,
    const int64_t vector_offset,
    const int64_t row_cap)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!col_param.get_meta_type().is_lob_storage() ||
                  nullptr == context.lob_locator_helper_ ||
                  VectorFormat::VEC_DISCRETE != expr.get_format(eval_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected param", K(ret), K(col_param.get_meta_type()),
                K(context.lob_locator_helper_), K(expr.get_format(eval_ctx)));
  } else {
    ObDiscreteFormat *discrete_format = static_cast<ObDiscreteFormat *>(expr.get_vector(eval_ctx));
    ObDatum datum;
    ObLength length;
    for (int64_t row_idx = vector_offset; OB_SUCC(ret) && (row_idx < row_cap + vector_offset); ++row_idx) {
      if (!discrete_format->is_null(row_idx)) {
        discrete_format->get_payload(row_idx, datum.ptr_, length);
        datum.len_ = static_cast<uint32_t>(length);
        if (!datum.get_lob_data().in_row_) {
          if (OB_FAIL(context.lob_locator_helper_->fill_lob_locator_v2(datum, col_param, iter_param, context))) {
            STORAGE_LOG(WARN, "Failed to fill lob loactor", K(ret), K(row_idx), K(datum), K(context), K(iter_param));
          } else {
            discrete_format->set_datum(row_idx, datum);
          }
        }
      }
    }
    if (OB_SUCC(ret) && col_param.get_meta_type().is_collection_sql_type() &&
        OB_FAIL(storage::distribute_attrs_on_rich_format_columns(row_cap, vector_offset, expr, eval_ctx))) {
      STORAGE_LOG(WARN, "failed to dispatch collection cells", K(ret), K(row_cap), K(vector_offset));
    }
  }
  return ret;
}

// Monotonic black filter only support ">", ">=", "<", "<=", "=" five types.
// All of these monotonic black filters will return false if the input is null.
// When has_null is true, we can not set_always_true() for bool_mask but can judge always false.
int check_skip_by_monotonicity(
    sql::ObBlackFilterExecutor &filter,
    blocksstable::ObStorageDatum &min_datum,
    blocksstable::ObStorageDatum &max_datum,
    const sql::ObBitVector &skip_bit,
    const bool has_null,
    ObBitmap *result_bitmap,
    sql::ObBoolMask &bool_mask)
{
  int ret = OB_SUCCESS;
  bool_mask.set_uncertain();
  if (min_datum.is_null() || max_datum.is_null()) {
    // uncertain
  } else {
    const sql::PushdownFilterMonotonicity mono = filter.get_monotonicity();
    bool is_asc = false;
    switch (mono) {
      case sql::PushdownFilterMonotonicity::MON_ASC: {
        is_asc = true;
      }
      case sql::PushdownFilterMonotonicity::MON_DESC: {
        bool filtered = false;
        ObStorageDatum &false_datum = is_asc ? max_datum : min_datum;
        ObStorageDatum &true_datum = is_asc ? min_datum : max_datum;
        if (OB_FAIL(filter.filter(false_datum, skip_bit, filtered))) {
          STORAGE_LOG(WARN, "Failed to compare with false_datum", K(ret), K(false_datum), K(is_asc));
        } else if (filtered) {
          bool_mask.set_always_false();
        } else if (!has_null) {
          if (OB_FAIL(filter.filter(true_datum, skip_bit, filtered))) {
            STORAGE_LOG(WARN, "Failed to compare with true_datum", K(ret), K(true_datum), K(is_asc));
          } else if (!filtered) {
            bool_mask.set_always_true();
          }
        }
        break;
      }
      case sql::PushdownFilterMonotonicity::MON_EQ_ASC: {
        is_asc = true;
      }
      case sql::PushdownFilterMonotonicity::MON_EQ_DESC: {
        bool min_cmp_res = false;
        bool max_cmp_res = false;
        if (OB_FAIL(filter.judge_greater_or_less(min_datum, skip_bit, is_asc, min_cmp_res))) {
          STORAGE_LOG(WARN, "Failed to judge min_datum", K(ret), K(min_datum));
        } else if (min_cmp_res) {
          bool_mask.set_always_false();
        } else if (OB_FAIL(filter.judge_greater_or_less(max_datum, skip_bit, !is_asc, max_cmp_res))) {
          STORAGE_LOG(WARN, "Failed to judge max_datum", K(ret), K(max_datum));
        } else if (max_cmp_res) {
          bool_mask.set_always_false();
        } else if (!has_null) {
          if (OB_FAIL(filter.filter(min_datum, skip_bit, min_cmp_res))) {
            STORAGE_LOG(WARN, "Failed to compare with min_datum", K(ret), K(min_datum));
          } else if (min_cmp_res) {
            // min datum is filtered
          } else if (OB_FAIL(filter.filter(max_datum, skip_bit, max_cmp_res))) {
            STORAGE_LOG(WARN, "Failed to compare with max_datum", K(ret), K(max_datum));
          } else if (!max_cmp_res) {
            // min datum and max datum are both not filtered
            bool_mask.set_always_true();
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected monotonicity", K(ret), K(mono));
      }
    }
  }
  if (OB_SUCC(ret) && nullptr != result_bitmap){
    if (bool_mask.is_always_false()) {
      result_bitmap->reuse(false);
    } else if (bool_mask.is_always_true()) {
      result_bitmap->reuse(true);
    }
  }
  return ret;
}

int reverse_trans_version_val(common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datums || count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(datums), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      common::ObDatum &datum = datums[i];
      if (OB_UNLIKELY(datum.is_nop() || datum.is_null() || datum.get_int() > 0)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected datum value", K(ret), K(datum));
      } else {
        datum.set_int(-datum.get_int());
      }
    }
  }
  return ret;
}

int reverse_trans_version_val(ObIVector *vector, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == vector || count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(vector), K(count));
  } else if (OB_UNLIKELY(vector->get_format() != VectorFormat::VEC_FIXED)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected vector format for trans version col", K(ret), K(vector->get_format()));
  } else {
    ObFixedLengthBase *fixed_length_base = static_cast<ObFixedLengthBase *>(vector);
    if (OB_UNLIKELY(fixed_length_base->has_null() || fixed_length_base->get_length() != sizeof(int64_t))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected vector", K(ret), K(fixed_length_base->has_null()), K(fixed_length_base->get_length()));
    } else {
      int64_t *ver_ptr = reinterpret_cast<int64_t *>(fixed_length_base->get_data());
      for (int64_t i = 0; i < count; ++i) {
        ver_ptr[i] = -ver_ptr[i];
      }
    }
  }
  return ret;
}

const char *ObMviewScanInfo::OLD_ROW = "O";
const char *ObMviewScanInfo::NEW_ROW = "N";
const char *ObMviewScanInfo::FINAL_ROW = "F";
int ObMviewScanInfo::init(
    const bool is_mv_refresh_query,
    const StorageScanType scan_type,
    const int64_t begin_version,
    const int64_t end_version,
    const common::ObIArray<sql::ObExpr *> &non_mview_filters)
{
  int ret = OB_SUCCESS;
  if (non_mview_filters.count() > 0 && OB_FAIL(op_filters_.reserve(non_mview_filters.count()))) {
    STORAGE_LOG(WARN, "Failed to reserve op filters", K(ret));
  } else {
    is_mv_refresh_query_ = is_mv_refresh_query;
    scan_type_ = scan_type;
    begin_version_ = begin_version;
    end_version_ = end_version;
    for (int64_t i = 0; OB_SUCC(ret) && i < non_mview_filters.count(); ++i) {
      if (OB_FAIL(op_filters_.push_back(non_mview_filters.at(i)))) {
        STORAGE_LOG(WARN, "Failed to push back", K(ret));
      }
    }
  }
  return ret;
}
int ObMviewScanInfo::check_and_update_version_range(const int64_t multi_version_start, common::ObVersionRange &origin_range)
{
  int ret = OB_SUCCESS;
  bool is_verion_valid = 0 == origin_range.base_version_ &&
                         (!is_end_valid() || end_version_ == origin_range.snapshot_version_) &&
                         (!is_begin_valid() || begin_version_ < origin_range.snapshot_version_);
  if (OB_UNLIKELY(!is_verion_valid)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid mview query version", K(ret), K(multi_version_start), K(origin_range), K(*this));
  } else if (OB_UNLIKELY(is_begin_valid() && begin_version_ < multi_version_start)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "begin version is oldder than tablet's multi version start", K(ret), K(multi_version_start), K(origin_range), K(*this));
  } else {
    if (is_begin_valid()) {
      origin_range.base_version_ = begin_version_;
    }
  }
  return ret;
}

int decimal_or_number_to_int64(const ObDatum &datum,
                               const ObDatumMeta &datum_meta,
                               int64_t &res)
{
  int ret = OB_SUCCESS;
  ObObjType ob_type = datum_meta.get_type();
  if (ObNumberType == ob_type) {
    const number::ObNumber nmb(datum.get_number());
    if (OB_FAIL(nmb.extract_valid_int64_with_trunc(res))) {
      STORAGE_LOG(WARN, "failed to cast number to int64", K(ret));
    }
  } else if (ObDecimalIntType == ob_type) {
    int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);
    bool is_valid;
    if (OB_FAIL(wide::check_range_valid_int64(datum.get_decimal_int(), int_bytes, is_valid, res))) {
      STORAGE_LOG(WARN, "failed to check decimal int", K(int_bytes), K(ret));
    } else if (!is_valid) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "decimal int is not valid int64", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected type", K(ob_type), K(ret));
  }
  return ret;
}

// extract mview info from filter: ora_rowscn > V0 and ora_rowscn <= V1 and $OLD_NEW='O|N|F'
int build_mview_scan_info_if_need(
    const common::ObQueryFlag query_flag,
    const sql::ObExprPtrIArray *op_filters,
    sql::ObEvalCtx &eval_ctx,
    common::ObIAllocator *alloc,
    ObMviewScanInfo *&mview_scan_info)
{
  int ret = OB_SUCCESS;
  StorageScanType scan_type = StorageScanType::NORMAL;
  int64_t begin_version = -1;
  int64_t end_version = -1;
  common::ObSEArray<sql::ObExpr *, 4> non_mview_filters;
  if (OB_UNLIKELY(nullptr == alloc || nullptr != mview_scan_info ||
                  nullptr == op_filters || op_filters->count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(alloc), KP(mview_scan_info), KP(op_filters));
  } else {
    sql::ObExpr *e = nullptr;
    ObDatum *datum = NULL;
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
    batch_info_guard.set_batch_size(1);
    for (int64_t i = 0; OB_SUCC(ret) && i < op_filters->count(); ++i) {
      if (OB_ISNULL(e = op_filters->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "expr is null", K(ret), K(i));
      } else if (T_OP_EQ == e->type_  && 2 == e->arg_cnt_ &&
                 T_PSEUDO_OLD_NEW_COL == e->args_[0]->type_ && e->args_[1]->is_static_const_) {
        if (OB_FAIL(e->args_[1]->eval(eval_ctx, datum))) {
          STORAGE_LOG(WARN, "Failed to eval const expr", K(ret));
        } else if (0 == ObString::make_string(ObMviewScanInfo::OLD_ROW).case_compare(datum->get_string())) {
          scan_type = StorageScanType::MVIEW_FIRST_DELETE;
        } else if (0 == ObString::make_string(ObMviewScanInfo::NEW_ROW).case_compare(datum->get_string())) {
          scan_type = StorageScanType::MVIEW_LAST_INSERT;
        } else if (0 == ObString::make_string(ObMviewScanInfo::FINAL_ROW).case_compare(datum->get_string())) {
          scan_type = StorageScanType::MVIEW_FINAL_ROW;
        } else {
          ret = OB_NOT_SUPPORTED;
          STORAGE_LOG(WARN, "Not supported OLD_NEW type", K(ret), KPC(datum));
        }
      } else if (OB_FAIL(non_mview_filters.push_back(e))) {
        STORAGE_LOG(WARN, "Failed to push back non mview filter", K(ret), K(i));
      } else if ((T_OP_GT == e->type_ || T_OP_LE == e->type_) && 2 == e->arg_cnt_) {
        sql::ObExpr *left = e->args_[0];
        sql::ObExpr *right = e->args_[1];
        int64_t rowscn = -1;
        if (T_ORA_ROWSCN != left->type_ && lib::is_oracle_mode()) {
          if (T_FUN_SYS_CAST == left->type_ &&
              2 == left->arg_cnt_ &&
              T_ORA_ROWSCN == left->args_[0]->type_ ) {
            left = left->args_[0];
          } else {
            left = nullptr;
          }
        }
        if (nullptr != left &&
            T_ORA_ROWSCN == left->type_ && (right->is_static_const_ || T_FUN_SYS_LAST_REFRESH_SCN == right->type_)) {
          if (OB_FAIL(right->eval(eval_ctx, datum))) {
            STORAGE_LOG(WARN, "Failed to eval const expr", K(ret));
          } else if (lib::is_oracle_mode()) {
            if (OB_FAIL(decimal_or_number_to_int64(*datum, right->datum_meta_, rowscn))) {
              STORAGE_LOG(WARN, "Failed to get rowscn", K(ret));
            }
          } else {
            rowscn = datum->get_int();
          }
          if (OB_FAIL(ret)) {
          } else if (T_OP_GT == e->type_) {
            begin_version = rowscn;
          } else {
            end_version = rowscn;
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (StorageScanType::NORMAL == scan_type) {
    STORAGE_LOG(INFO, "no need use mview scan", K(scan_type), K(begin_version), K(end_version));
  } else if (OB_ISNULL(mview_scan_info = OB_NEWx(ObMviewScanInfo, alloc, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for mview scan info", K(ret));
  } else if (OB_FAIL(mview_scan_info->init(query_flag.is_mr_mview_refresh_base_scan(), scan_type, begin_version, end_version, non_mview_filters))) {
    STORAGE_LOG(WARN, "Failed to init mview scan info", K(ret));
  }
  STORAGE_LOG(TRACE, "[MVIEW QUERY]: build mview scan info", K(ret), KPC(mview_scan_info));
  return ret;
}

void release_mview_scan_info(common::ObIAllocator *alloc, ObMviewScanInfo *&mview_scan_info)
{
  if (nullptr != mview_scan_info) {
    mview_scan_info->~ObMviewScanInfo();
    if (nullptr != alloc) {
      alloc->free(mview_scan_info);
    }
    mview_scan_info = nullptr;
  }
}

}
}

