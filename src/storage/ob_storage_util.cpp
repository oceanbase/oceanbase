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
#include "share/object/ob_obj_cast.h"
#include "sql/engine/ob_exec_context.h"
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
                                             char *&buf)
{
  if (1 == space_pattern.length()) {
    MEMSET(buf + offset, space_pattern[0], buf_len - offset);
  } else {
    for (int32_t i = offset; i < buf_len; i += space_pattern.length()) {
      MEMCPY(buf + i, space_pattern.ptr(), space_pattern.length());
    }
  }
}

OB_INLINE static int pad_datum_on_local_buf(const ObString &space_pattern,
                                            int32_t pad_whitespace_length,
                                            common::ObIAllocator &padding_alloc,
                                            common::ObDatum &datum)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const int32_t buf_len = datum.pack_ + pad_whitespace_length * space_pattern.length();
  if (OB_ISNULL((buf = (char*) padding_alloc.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "no memory", K(ret));
  } else {
    MEMCPY(buf, datum.ptr_, datum.pack_);
    append_padding_pattern(space_pattern, datum.pack_, buf_len, buf);
    datum.ptr_ = buf;
    datum.pack_ = buf_len;
  }
  return ret;
}

OB_INLINE static int pad_on_local_buf(const ObString &space_pattern,
                                      int32_t pad_whitespace_length,
                                      common::ObIAllocator &padding_alloc,
                                      const char *&ptr,
                                      uint32_t &length)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const int32_t buf_len = length + pad_whitespace_length * space_pattern.length();
  if (OB_ISNULL((buf = (char*) padding_alloc.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "no memory", K(ret));
  } else {
    MEMCPY(buf, ptr, length);
    append_padding_pattern(space_pattern, length, buf_len, buf);
    ptr = buf;
    length = buf_len;
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
        OB_FAIL(pad_datum_on_local_buf(space_pattern, length - cur_len, padding_alloc, datum))) {
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
      const int32_t buf_len = datum.pack_ + (length - cur_len) * space_pattern.length();
      if (OB_ISNULL(ptr = expr.get_str_res_mem(ctx, buf_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "no memory", K(ret));
      } else {
        MEMMOVE(ptr, datum.ptr_, datum.pack_);
        append_padding_pattern(space_pattern, datum.pack_, buf_len, ptr);
        datum.ptr_ = ptr;
        datum.pack_ = buf_len;
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
      append_padding_pattern(space_pattern, 0, buf_len, buf);
      for (int64_t i = 0; i < row_count; i++) {
        common::ObDatum &datum = datums[i];
        if (datum.is_null()) {
          // do nothing
        } else if (0 == datum.pack_){
          datum.ptr_ = buf;
          datum.pack_ = buf_len;
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
                OB_FAIL(pad_datum_on_local_buf(space_pattern, length - cur_len, padding_alloc, datum))) {
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
            OB_FAIL(pad_datum_on_local_buf(space_pattern, length - cur_len, padding_alloc, datum))) {
          STORAGE_LOG(WARN, "fail to pad on padding allocator", K(ret), K(length), K(cur_len), K(datum));
        }
      }
    }
  }
  return ret;
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

}
}

