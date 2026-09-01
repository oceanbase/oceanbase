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
 * This file contains interface support for the JSON path abstraction.
 */

#define USING_LOG_PREFIX LIB

#include "ob_json_bin_view.h"
#include "ob_json_wrapper.h"
#include "ob_json_path.h"
#include "ob_json_base.h"
#include "lib/utility/serialization.h"
#include "common/object/ob_object.h"

namespace oceanbase {
namespace common {

static OB_INLINE bool is_valid_range(int64_t offset, int64_t len, int64_t limit)
{
  return offset >= 0 && len >= 0 && len <= limit - offset;
}

int ObJsonBinView::init(const char *data, int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json binary data", K(ret), KP(data), K(len));
  } else {
    use_lex_order_ = false;
    int64_t offset = 0;
    uint8_t first_byte = static_cast<uint8_t>(data[0]);
    // Full documents start with a doc header. Raw node slices start directly with node type.
    if (first_byte == J_DOC_HEADER_V0) {
      if (OB_UNLIKELY(len < static_cast<int64_t>(sizeof(ObJsonBinDocHeader)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("json binary too short for doc header", K(ret), K(len));
      } else {
        const ObJsonBinDocHeader *doc_header =
            reinterpret_cast<const ObJsonBinDocHeader*>(data);
        use_lex_order_ = doc_header->use_lexicographical_order_;
        offset = sizeof(ObJsonBinDocHeader);
        // Check for extended segment (from partial update).
        if (OB_UNLIKELY(static_cast<int64_t>(doc_header->extend_seg_offset_) != len)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("json binary with extended segment not supported", K(ret),
                   K(doc_header->extend_seg_offset_), K(len));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(parse_node(data, len, offset))) {
        LOG_WARN("parse root node fail", K(ret), K(offset), K(len));
      }
    }
  }
  return ret;
}

int ObJsonBinView::parse_node(const char *buf, int64_t buf_len, int64_t offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_range(offset, sizeof(uint8_t), buf_len))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset out of range", K(ret), K(offset), K(buf_len));
  } else {
    type_ = static_cast<uint8_t>(buf[offset]);
    const int64_t value_offset = offset + static_cast<int64_t>(sizeof(uint8_t));
    ObJBVerType vt = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(type_));
    ObJsonNodeType nt = ObJsonVerType::get_json_type(vt);

    // Containers keep data_ at the type byte so entry offsets stay relative to data_.
    // Scalars set data_ in parse_scalar() to the raw payload (or type byte for string/opaque).
    if (is_container()) {
      data_ = buf + offset;
      ret = parse_container(buf, buf_len, offset, nt == ObJsonNodeType::J_OBJECT);
    } else {
      ret = parse_scalar(buf, buf_len, value_offset);
    }
  }
  return ret;
}

int ObJsonBinView::parse_container(const char *buf, int64_t buf_len, int64_t offset, bool is_object)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_range(offset, OB_JSON_BIN_HEADER_LEN, buf_len))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer too short for container header", K(ret), K(offset), K(buf_len));
  } else {
    const ObJsonBinHeader *header = reinterpret_cast<const ObJsonBinHeader*>(buf + offset);
    entry_var_type_ = header->entry_size_;
    const uint8_t count_var_type = header->count_size_;
    const uint8_t obj_size_var_type = header->obj_size_size_;

    // *_var_type come from 2-bit bitfields so are structurally in [0, JBLS_UINT64].
    // Cache var sizes once.
    const int64_t count_var_sz = static_cast<int64_t>(ObJsonVar::get_var_size(count_var_type));
    const int64_t obj_size_var_sz = static_cast<int64_t>(ObJsonVar::get_var_size(obj_size_var_type));

    int64_t pos = offset + OB_JSON_BIN_HEADER_LEN;
    uint64_t elem_count = 0;
    uint64_t obj_size = 0;
    // Single combined bounds check covering both count and obj_size reads.
    if (OB_UNLIKELY(!is_valid_range(pos, count_var_sz + obj_size_var_sz, buf_len))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer too short for container size header", K(ret),
               K(pos), K(count_var_sz), K(obj_size_var_sz), K(buf_len));
    } else if (OB_FAIL(ObJsonVar::read_var(buf + pos, count_var_type, &elem_count))) {
      LOG_WARN("read element count fail", K(ret));
    } else if (FALSE_IT(pos += count_var_sz)) {
    } else if (OB_FAIL(ObJsonVar::read_var(buf + pos, obj_size_var_type, &obj_size))) {
      LOG_WARN("read obj size fail", K(ret));
    } else {
      pos += obj_size_var_sz;
      // ObJsonBinView uses uint32_t for size fields to keep struct small (32B).
      // If values exceed uint32 range (>4GB container), fall back to ObJsonBin which uses uint64_t.
      if (OB_UNLIKELY(elem_count > UINT32_MAX || obj_size > UINT32_MAX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_TRACE("container too large for ObJsonBinView", K(elem_count), K(obj_size));
      } else if (OB_UNLIKELY(!is_valid_range(offset, static_cast<int64_t>(obj_size), buf_len))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("container data exceeds buffer", K(ret), K(offset), K(obj_size), K(buf_len));
      } else {
        element_count_ = static_cast<uint32_t>(elem_count);
        length_ = static_cast<uint32_t>(obj_size);

        const uint64_t key_off = OB_JSON_BIN_HEADER_LEN + count_var_sz + obj_size_var_sz;
        const uint64_t entry_sz = entry_var_size();
        const uint64_t key_table_sz = is_object ? elem_count * (entry_sz * 2) : 0;
        const uint64_t header_size =
            key_off + key_table_sz + elem_count * (entry_sz + OB_JSON_BIN_VALUE_TYPE_LEN);
        if (OB_UNLIKELY(header_size > obj_size)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("container entry table exceeds obj_size", K(ret),
                   K(key_off), K(key_table_sz), K(header_size), K(obj_size));
        } else {
          key_offset_start_ = static_cast<uint32_t>(key_off);
          value_offset_start_ = static_cast<uint32_t>(key_off + key_table_sz);
        }
      }
    }
  }
  return ret;
}

int ObJsonBinView::parse_scalar(const char *buf, int64_t buf_len, int64_t data_offset)
{
  int ret = OB_SUCCESS;
  const bool is_inlined = is_inline();
  const ObJsonNodeType nt = json_type();
  data_ = nullptr;
  element_count_ = 1;
  length_ = 0;
  switch (nt) {
    case ObJsonNodeType::J_NULL: {
      if (!is_inlined) {
        if (OB_UNLIKELY(!is_valid_range(data_offset, sizeof(char), buf_len))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("buffer too short for null", K(ret), K(data_offset), K(buf_len));
        } else {
          data_ = buf + data_offset;
          length_ = sizeof(char);
        }
      }
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      if (!is_inlined) {
        int64_t val = 0;
        int64_t pos = data_offset;
        if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &val))) {
          LOG_WARN("decode int val failed", K(ret));
        } else {
          data_ = buf + data_offset;
          int_val_ = val;
          length_ = static_cast<uint32_t>(pos - data_offset);
        }
      }
      break;
    }
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      if (!is_inlined) {
        int64_t val = 0;
        int64_t pos = data_offset;
        if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &val))) {
          LOG_WARN("decode uint val failed", K(ret));
        } else {
          data_ = buf + data_offset;
          uint_val_ = static_cast<uint64_t>(val);
          length_ = static_cast<uint32_t>(pos - data_offset);
        }
      }
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      if (OB_UNLIKELY(!is_valid_range(data_offset, sizeof(double), buf_len))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("buffer too short for double", K(ret), K(data_offset), K(buf_len));
      } else {
        data_ = buf + data_offset;
        double_val_ = *reinterpret_cast<const double*>(buf + data_offset);
        length_ = sizeof(double);
      }
      break;
    }
    case ObJsonNodeType::J_OFLOAT: {
      if (OB_UNLIKELY(!is_valid_range(data_offset, sizeof(float), buf_len))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("buffer too short for float", K(ret), K(data_offset), K(buf_len));
      } else {
        data_ = buf + data_offset;
        float_val_ = *reinterpret_cast<const float*>(buf + data_offset);
        length_ = sizeof(float);
      }
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      if (!is_inlined) {
        if (OB_UNLIKELY(!is_valid_range(data_offset, sizeof(bool), buf_len))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("buffer too short for boolean", K(ret), K(data_offset), K(buf_len));
        } else {
          data_ = buf + data_offset;
          uint_val_ = static_cast<uint64_t>(*reinterpret_cast<const bool*>(buf + data_offset));
          length_ = sizeof(bool);
        }
      }
      break;
    }
    case ObJsonNodeType::J_STRING:
    case ObJsonNodeType::J_OBINARY:
    case ObJsonNodeType::J_OOID:
    case ObJsonNodeType::J_ORAWHEX:
    case ObJsonNodeType::J_ORAWID:
    case ObJsonNodeType::J_ODAYSECOND:
    case ObJsonNodeType::J_OYEARMONTH: {
      int64_t str_len = 0;
      const int64_t node_offset = data_offset - static_cast<int64_t>(sizeof(uint8_t));
      int64_t pos = data_offset;
      if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &str_len))) {
        LOG_WARN("decode string length fail", K(ret), K(data_offset), K(pos));
      } else if (OB_UNLIKELY(!is_valid_range(pos, str_len, buf_len))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("string data out of bounds", K(ret), K(pos), K(str_len), K(buf_len));
      } else {
        data_ = buf + node_offset;
        // length_ = total node size including type byte + varint prefix + string data.
        length_ = static_cast<uint32_t>(pos - node_offset + str_len);
      }
      break;
    }
    case ObJsonNodeType::J_OPAQUE: {
      const int64_t node_offset = data_offset - static_cast<int64_t>(sizeof(uint8_t));
      int64_t pos = data_offset;
      if (OB_UNLIKELY(!is_valid_range(pos, sizeof(uint16_t) + sizeof(uint64_t), buf_len))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("buffer too short for opaque", K(ret), K(pos), K(buf_len));
      } else {
        // field_type read on-demand via field_type() accessor.
        pos += sizeof(uint16_t);
        int64_t data_len = *reinterpret_cast<const int64_t*>(buf + pos);
        pos += sizeof(uint64_t);
        const uint64_t opaque_data_offset = static_cast<uint64_t>(pos - node_offset);
        if (OB_UNLIKELY(data_len > UINT32_MAX - opaque_data_offset)) {
          ret = OB_NOT_SUPPORTED;
          LOG_TRACE("opaque data too large for ObJsonBinView", K(data_len));
        } else if (OB_UNLIKELY(!is_valid_range(pos, data_len, buf_len))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("opaque data exceeds buffer", K(ret), K(pos), K(data_len), K(buf_len));
        } else {
          data_ = buf + node_offset;
          // length_ = total node size including type byte + field_type + data_len + data.
          length_ = static_cast<uint32_t>(pos - node_offset + data_len);
        }
      }
      break;
    }
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_MYSQL_DATE:
    case ObJsonNodeType::J_ORACLEDATE: {
      if (OB_UNLIKELY(!is_valid_range(data_offset, sizeof(int32_t), buf_len))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("buffer too short for date", K(ret), K(data_offset), K(buf_len));
      } else {
        data_ = buf + data_offset;
        int_val_ = *reinterpret_cast<const int32_t*>(buf + data_offset);
        length_ = sizeof(int32_t);
      }
      break;
    }
    case ObJsonNodeType::J_TIME:
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_MYSQL_DATETIME:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ:
    case ObJsonNodeType::J_TIMESTAMP: {
      if (OB_UNLIKELY(!is_valid_range(data_offset, sizeof(int64_t), buf_len))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("buffer too short for time/datetime", K(ret), K(data_offset), K(buf_len));
      } else {
        data_ = buf + data_offset;
        int_val_ = *reinterpret_cast<const int64_t*>(buf + data_offset);
        length_ = sizeof(int64_t);
      }
      break;
    }
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      // Decimal payload layout (no type byte in data): prec(i16) + scale(i16) + ObNumber data.
      // data_offset points to prec[0] (whether called from parse_node or populate_child).
      // data_ is set here to buf+data_offset so both code paths are consistent.
      int64_t pos = data_offset;
      // Skip prec(i16) + scale(i16).
      if (OB_UNLIKELY(pos + 2 * static_cast<int64_t>(sizeof(int16_t)) > buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("buffer too short for decimal header", K(ret), K(pos), K(buf_len));
      } else {
        pos += 2 * static_cast<int64_t>(sizeof(int16_t));
        // Use decode_number_type (lighter than full deserialize) to skip ObNumber data.
        number::ObNumber::Desc desc;
        uint32_t *digits = nullptr;
        if (OB_FAIL(serialization::decode_number_type(buf, buf_len, pos, desc, digits))) {
          LOG_WARN("failed to skip decimal number data", K(ret), K(pos));
        } else if (OB_UNLIKELY(pos - data_offset > UINT32_MAX)) {
          ret = OB_NOT_SUPPORTED;
          LOG_TRACE("decimal data too large for ObJsonBinView", K(pos), K(data_offset));
        } else {
          data_ = buf + data_offset;
          // data_ points to prec[0]; length_ is prec+scale+number size (no type byte).
          length_ = static_cast<uint32_t>(pos - data_offset);
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid node type", K(ret), K(nt));
      break;
    }
  }
  return ret;
}

// Shared helper: populate a child view from a non-inline value entry
int ObJsonBinView::populate_child(uint64_t value_offset, uint8_t value_type, ObJsonBinView &result) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(value_offset >= length_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child value_offset out of bounds", K(ret), K(value_offset), K(length_));
  } else {
    result.use_lex_order_ = use_lex_order_;
    if (ObJsonBin::need_type_prefix(value_type)) {
      if (OB_FAIL(result.parse_node(data_, static_cast<int64_t>(length_), static_cast<int64_t>(value_offset)))) {
        LOG_WARN("parse child node fail", K(ret), K(value_offset));
      }
    } else {
      result.type_ = value_type;
      if (OB_FAIL(result.parse_scalar(data_, static_cast<int64_t>(length_), static_cast<int64_t>(value_offset)))) {
        LOG_WARN("parse scalar data fail", K(ret), K(value_offset));
      }
    }
  }
  return ret;
}

// Shared helper: populate a child view from an inline value entry
int ObJsonBinView::populate_inline_child(uint64_t value_offset, uint8_t value_type,
                                         int entry_index, ObJsonBinView &result) const
{
  result.use_lex_order_ = use_lex_order_;
  result.type_ = value_type;
  result.length_ = 0;
  result.element_count_ = 1;
  // Inline scalars have no payload area; use the value entry address as node identity.
  const int64_t var_size = ObJsonVar::get_var_size(entry_var_type_);
  result.data_ = data_ + value_offset_start_
      + entry_index * (var_size + OB_JSON_BIN_VALUE_TYPE_LEN);
  ObJBVerType vt = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(value_type));
  ObJsonNodeType ntype = ObJsonVerType::get_json_type(vt);
  if (ntype == ObJsonNodeType::J_INT || ntype == ObJsonNodeType::J_OINT) {
    int64_t signed_val = ObJsonVar::var_uint2int(value_offset, entry_var_type_);
    result.int_val_ = signed_val;
  } else {
    result.uint_val_ = value_offset;
  }
  return OB_SUCCESS;
}

number::ObNumber ObJsonBinView::get_decimal_data() const
{
  number::ObNumber nmb;
  // data_ points to prec[0]. Decimal payload: prec(2) + scale(2) + ObNumber data.
  // Skip prec(2) + scale(2) = 4 bytes, then deserialize ObNumber.
  // digits_ in the returned ObNumber will point into the original document buffer (zero-copy).
  int64_t pos = 2 * static_cast<int64_t>(sizeof(int16_t));  // = 4
  int ret = nmb.deserialize(data_, static_cast<int64_t>(length_), pos);
  if (OB_FAIL(ret)) {
    LOG_WARN("on-the-fly decimal deserialize fail", K(ret));
  }
  return nmb;
}

ObObjType ObJsonBinView::field_type() const
{
  ObObjType ret_type = ObObjType::ObNullType;
  ObJsonNodeType nt = json_type();
  if (nt == ObJsonNodeType::J_OPAQUE) {
    // Opaque layout from data_: type_byte(1) + field_type(uint16, 2) + data_len(int64, 8) + data.
    // Read field_type from data_[1..2].
    ret_type = static_cast<ObObjType>(*reinterpret_cast<const uint16_t*>(data_ + 1));
  } else {
    // Time types: derive from json_type.
    switch (nt) {
      case ObJsonNodeType::J_DATE:
      case ObJsonNodeType::J_MYSQL_DATE:
      case ObJsonNodeType::J_ORACLEDATE:
      case ObJsonNodeType::J_TIME:
      case ObJsonNodeType::J_DATETIME:
      case ObJsonNodeType::J_ODATE:
      case ObJsonNodeType::J_MYSQL_DATETIME:
      case ObJsonNodeType::J_OTIMESTAMP:
      case ObJsonNodeType::J_OTIMESTAMPTZ:
      case ObJsonNodeType::J_TIMESTAMP:
        ret_type = ObJsonBaseUtil::get_time_type(nt);
        break;
      default:
        break;
    }
  }
  return ret_type;
}

// ============ Template-ized lookup implementation ============
template<typename VarSizeType>
int ObJsonBinView::lookup_impl(const ObString &key, ObJsonBinView &result) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  int64_t low = 0;
  int64_t high = static_cast<int64_t>(element_count_) - 1;

  while (low <= high) {
    int64_t mid = low + (high - low) / 2;
    uint64_t key_offset;
    uint64_t key_len;
    read_key_entry<VarSizeType>(static_cast<int>(mid), key_offset, key_len);

    int cmp;
    if (use_lex_order_) {
      // Lexicographic order: compare directly
      int min_len = MIN(key_len, key.length());
      cmp = MEMCMP(data_ + key_offset, key.ptr(), min_len);
      if (cmp == 0) {
        cmp = static_cast<int>(key_len) - key.length();
      }
    } else {
      // MySQL default: compare length first (key-length shortcut), then content
      if (key_len != static_cast<uint64_t>(key.length())) {
        cmp = static_cast<int>(key_len) - key.length();
      } else {
        cmp = MEMCMP(data_ + key_offset, key.ptr(), static_cast<int>(key_len));
      }
    }
    if (cmp == 0) {
      is_found = true;
      if (OB_FAIL(element_impl<VarSizeType>(static_cast<uint64_t>(mid), result))) {
        LOG_WARN("element_impl fail", K(ret), K(mid));
      }
      break;
    } else if (cmp > 0) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }

  if (OB_SUCC(ret) && !is_found) {
    ret = OB_SEARCH_NOT_FOUND;
  }
  return ret;
}

// Outer lookup: single switch dispatch to template
int ObJsonBinView::lookup(const ObString &key, ObJsonBinView &result) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(json_type() != ObJsonNodeType::J_OBJECT)) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("not an object", K(ret));
  } else {
    switch (entry_var_type_) {
      case JBLS_UINT8:  ret = lookup_impl<uint8_t>(key, result); break;
      case JBLS_UINT16: ret = lookup_impl<uint16_t>(key, result); break;
      case JBLS_UINT32: ret = lookup_impl<uint32_t>(key, result); break;
      case JBLS_UINT64: ret = lookup_impl<uint64_t>(key, result); break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected entry_var_type_ in lookup", K(ret), K(entry_var_type_));
        break;
    }
  }
  return ret;
}

// ============ Template-ized element implementation ============
template<typename VarSizeType>
int ObJsonBinView::element_impl(uint64_t pos, ObJsonBinView &result) const
{
  int ret = OB_SUCCESS;
  uint64_t value_offset;
  uint8_t value_type;
  read_value_entry<VarSizeType>(static_cast<int>(pos), value_offset, value_type);

  if (OB_JSON_TYPE_IS_INLINE(value_type)) {
    if (OB_FAIL(populate_inline_child(value_offset, value_type, static_cast<int>(pos), result))) {
      LOG_WARN("populate_inline_child fail", K(ret), K(pos), K(value_type));
    }
  } else {
    if (OB_FAIL(populate_child(value_offset, value_type, result))) {
      LOG_WARN("populate_child fail", K(ret), K(pos), K(value_offset), K(value_type));
    }
  }
  return ret;
}

int ObJsonBinView::element(uint64_t pos, ObJsonBinView &result) const
{
  int ret = OB_SUCCESS;
  ObJsonNodeType nt = json_type();
  if (OB_UNLIKELY(!is_container())) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("not a container", K(ret), K(nt));
  } else if (OB_UNLIKELY(pos >= element_count_)) {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("out of element", K(ret), K(pos), K(element_count_));
  } else {
    switch (entry_var_type_) {
      case JBLS_UINT8:  ret = element_impl<uint8_t>(pos, result); break;
      case JBLS_UINT16: ret = element_impl<uint16_t>(pos, result); break;
      case JBLS_UINT32: ret = element_impl<uint32_t>(pos, result); break;
      case JBLS_UINT64: ret = element_impl<uint64_t>(pos, result); break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected entry_var_type_ in element", K(ret), K(entry_var_type_));
        break;
    }
  }
  return ret;
}

// ============ get_key / get_key_value ============

template<typename VarSizeType>
int ObJsonBinView::get_key_impl(uint64_t index, ObString &key) const
{
  int ret = OB_SUCCESS;
  uint64_t key_offset;
  uint64_t key_len;
  read_key_entry<VarSizeType>(static_cast<int>(index), key_offset, key_len);
  key.assign_ptr(data_ + key_offset, static_cast<int32_t>(key_len));
  return ret;
}

template<typename VarSizeType>
int ObJsonBinView::get_key_value_impl(uint64_t index, ObString &key, ObJsonBinView &value) const
{
  int ret = OB_SUCCESS;
  uint64_t key_offset;
  uint64_t key_len;
  read_key_entry<VarSizeType>(static_cast<int>(index), key_offset, key_len);
  key.assign_ptr(data_ + key_offset, static_cast<int32_t>(key_len));

  uint64_t value_offset;
  uint8_t value_type;
  read_value_entry<VarSizeType>(static_cast<int>(index), value_offset, value_type);

  if (OB_JSON_TYPE_IS_INLINE(value_type)) {
    if (OB_FAIL(populate_inline_child(value_offset, value_type, static_cast<int>(index), value))) {
      LOG_WARN("populate_inline_child fail", K(ret), K(index), K(value_type));
    }
  } else {
    if (OB_FAIL(populate_child(value_offset, value_type, value))) {
      LOG_WARN("populate_child fail", K(ret), K(index), K(value_offset), K(value_type));
    }
  }
  return ret;
}

int ObJsonBinView::get_key(uint64_t index, ObString &key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_object())) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("not an object", K(ret));
  } else if (OB_UNLIKELY(index >= element_count_)) {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("index out of range", K(ret), K(index), K(element_count_));
  } else {
    switch (entry_var_type_) {
      case JBLS_UINT8:  ret = get_key_impl<uint8_t>(index, key); break;
      case JBLS_UINT16: ret = get_key_impl<uint16_t>(index, key); break;
      case JBLS_UINT32: ret = get_key_impl<uint32_t>(index, key); break;
      case JBLS_UINT64: ret = get_key_impl<uint64_t>(index, key); break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected entry_var_type_", K(ret), K(entry_var_type_));
        break;
    }
  }
  return ret;
}

int ObJsonBinView::get_key_value(uint64_t index, ObString &key, ObJsonBinView &value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_object())) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("not an object", K(ret));
  } else if (OB_UNLIKELY(index >= element_count_)) {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("index out of range", K(ret), K(index), K(element_count_));
  } else {
    switch (entry_var_type_) {
      case JBLS_UINT8:  ret = get_key_value_impl<uint8_t>(index, key, value); break;
      case JBLS_UINT16: ret = get_key_value_impl<uint16_t>(index, key, value); break;
      case JBLS_UINT32: ret = get_key_value_impl<uint32_t>(index, key, value); break;
      case JBLS_UINT64: ret = get_key_value_impl<uint64_t>(index, key, value); break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected entry_var_type_", K(ret), K(entry_var_type_));
        break;
    }
  }
  return ret;
}

int ObJsonBinView::get_obtime(ObTime &t) const
{
  int ret = OB_SUCCESS;
  switch (json_type()) {
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_ORACLEDATE: {
      ret = ObTimeConverter::date_to_ob_time(static_cast<int32_t>(int_val_), t);
      break;
    }
    case ObJsonNodeType::J_MYSQL_DATE: {
      t.mode_ |= DT_TYPE_MYSQL_DATE;
      ret = ObTimeConverter::mdate_to_ob_time(static_cast<int32_t>(int_val_), t);
      break;
    }
    case ObJsonNodeType::J_TIME: {
      ret = ObTimeConverter::time_to_ob_time(int_val_, t);
      break;
    }
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      ret = ObTimeConverter::datetime_to_ob_time(int_val_, NULL, t);
      break;
    }
    case ObJsonNodeType::J_MYSQL_DATETIME: {
      t.mode_ |= DT_TYPE_MYSQL_DATETIME;
      ret = ObTimeConverter::mdatetime_to_ob_time(int_val_, t);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json type for datetime decode", K(ret), K(json_type()));
      break;
    }
  }
  return ret;
}

// ============ get_raw_binary ============

int ObJsonBinView::encode_scalar_payload(char *buf, int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObJsonNodeType nt = json_type();
  // Standalone scalar binary stores a normal type byte; inline flag is only meaningful in value
  // entries.
  buf[pos++] = static_cast<char>(OB_JSON_TYPE_GET_INLINE(type_));
  switch (nt) {
    case ObJsonNodeType::J_NULL:
      buf[pos++] = '\0';
      break;
    case ObJsonNodeType::J_BOOLEAN: {
      bool val = get_boolean();
      MEMCPY(buf + pos, &val, sizeof(bool));
      pos += sizeof(bool);
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT:
      ret = serialization::encode_vi64(buf, buf_size, pos, int_val_);
      break;
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG:
      ret = serialization::encode_vi64(buf, buf_size, pos, static_cast<int64_t>(uint_val_));
      break;
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE:
      MEMCPY(buf + pos, &double_val_, sizeof(double));
      pos += sizeof(double);
      break;
    case ObJsonNodeType::J_OFLOAT:
      MEMCPY(buf + pos, &float_val_, sizeof(float));
      pos += sizeof(float);
      break;
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_MYSQL_DATE:
    case ObJsonNodeType::J_ORACLEDATE: {
      int32_t v = static_cast<int32_t>(int_val_);
      MEMCPY(buf + pos, &v, sizeof(int32_t));
      pos += sizeof(int32_t);
      break;
    }
    case ObJsonNodeType::J_TIME:
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_MYSQL_DATETIME:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ:
    case ObJsonNodeType::J_TIMESTAMP:
      MEMCPY(buf + pos, &int_val_, sizeof(int64_t));
      pos += sizeof(int64_t);
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported scalar type in encode_scalar_payload", K(ret), K(nt));
      break;
  }
  return ret;
}

int ObJsonBinView::get_raw_binary(ObIAllocator &alloc, ObString &out, bool has_lob_header) const
{
  int ret = OB_SUCCESS;
  const bool is_container_node = is_container();
  const int64_t lob_hdr_size = has_lob_header ? static_cast<int64_t>(sizeof(common::ObLobCommon)) : 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json bin view is invalid", K(ret));
  } else if (is_container_node) {
    // Container: allocate and copy with doc header.
    const int64_t doc_hdr_size = sizeof(ObJsonBinDocHeader);
    const int64_t json_total = doc_hdr_size + length_;
    const int64_t total = lob_hdr_size + json_total;
    char *buf = static_cast<char*>(alloc.alloc(total));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(total));
    } else {
      if (has_lob_header) {
        static const common::ObLobCommon LOB_HDR;
        MEMCPY(buf, &LOB_HDR, lob_hdr_size);
      }
      char *json_buf = buf + lob_hdr_size;
      ObJsonBinDocHeader *hdr = reinterpret_cast<ObJsonBinDocHeader*>(json_buf);
      new (hdr) ObJsonBinDocHeader();
      hdr->use_lexicographical_order_ = use_lex_order_ ? 1 : 0;
      hdr->extend_seg_offset_ = json_total;
      MEMCPY(json_buf + doc_hdr_size, data_, length_);
      out.assign_ptr(buf, static_cast<int32_t>(total));
    }
  } else if (OB_NOT_NULL(data_) && length_ > 0) {
    // Non-inline values keep their original payload in data_. Container/string/opaque already
    // include a type byte; scalar values and decimal need one prepended for standalone binary.
    const bool has_type_prefix = ObJsonBin::need_type_prefix(static_cast<uint8_t>(vertype()));
    const int64_t type_byte_size = has_type_prefix ? 0 : 1;
    const int64_t total = lob_hdr_size + type_byte_size + length_;
    char *buf = static_cast<char*>(alloc.alloc(total));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(total));
    } else {
      if (has_lob_header) {
        static const common::ObLobCommon LOB_HDR;
        MEMCPY(buf, &LOB_HDR, lob_hdr_size);
      }
      char *json_buf = buf + lob_hdr_size;
      if (!has_type_prefix) {
        json_buf[0] = static_cast<char>(OB_JSON_TYPE_GET_INLINE(type_));
      }
      MEMCPY(json_buf + type_byte_size, data_, length_);
      out.assign_ptr(buf, static_cast<int32_t>(total));
    }
  } else {
    // Inline scalar: synthesize type byte + payload from the decoded union value.
    const int64_t MAX_SCALAR_BUF_SIZE = 16;  // type(1) + max_vi64(10) or double(8) + padding
    char *buf = static_cast<char*>(alloc.alloc(lob_hdr_size + MAX_SCALAR_BUF_SIZE));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed for scalar", K(ret));
    } else {
      if (has_lob_header) {
        static const common::ObLobCommon LOB_HDR;
        MEMCPY(buf, &LOB_HDR, lob_hdr_size);
      }
      char *json_buf = buf + lob_hdr_size;
      int64_t pos = 0;
      if (OB_FAIL(encode_scalar_payload(json_buf, MAX_SCALAR_BUF_SIZE, pos))) {
        LOG_WARN("encode_scalar_payload fail", K(ret));
      } else {
        out.assign_ptr(buf, static_cast<int32_t>(lob_hdr_size + pos));
      }
    }
  }
  return ret;
}

int ObJsonBinViewAdapter::get_raw_binary(common::ObString &out, ObIAllocator *allocator)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_FAIL(view_.get_raw_binary(*allocator, out))) {
    LOG_WARN("get raw binary fail", K(ret));
  }
  return ret;
}

// ============ Seek Implementation ============

bool ObJsonBinView::is_same_view(const ObJsonBinView &other) const
{
  bool is_same = false;
  if (json_type() != other.json_type() || length() != other.length()) {
  } else {
    is_same = (data() != nullptr && data() == other.data());
  }
  return is_same;
}

int ObJsonBinView::push_unique_hit(const ObJsonBinView &view,
                                   ObIArray<ObJsonWrapper> &hits)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; i < hits.count() && !found; ++i) {
    const ObJsonWrapper &hit = hits.at(i);
    if (hit.is_bin() && hit.get_bin_view().is_same_view(view)) {
      found = true;
    }
  }
  if (!found && OB_FAIL(hits.push_back(ObJsonWrapper(view)))) {
    LOG_WARN("push_back fail", K(ret));
  }
  return ret;
}

int ObJsonBinView::seek(const ObJsonPath &path,
                        ObIArray<ObJsonWrapper> &hits,
                        bool auto_wrap,
                        bool only_need_one) const
{
  int ret = OB_SUCCESS;
  if (path.is_simple_path()) {
    ret = seek_simple_path(path, hits, auto_wrap);
  } else {
    ret = seek_complex_path(path, 0, auto_wrap, only_need_one, hits);
  }
  return ret;
}

int ObJsonBinView::seek_simple_path(const ObJsonPath &path,
                                    ObIArray<ObJsonWrapper> &hits,
                                    bool auto_wrap) const
{
  int ret = OB_SUCCESS;
  JsonPathIterator begin = path.begin();
  JsonPathIterator end = path.end();

  // Fast path: single JPN_MEMBER step (e.g., $.key) — the dominant case.
  // Avoids copying *this to cur, loop overhead, and the matched flag.
  if (OB_LIKELY(begin + 1 == end && (*begin)->get_node_type() == JPN_MEMBER)) {
    if (json_type() == ObJsonNodeType::J_OBJECT) {
      ObJsonBinView child;
      const ObJsonPathBasicNode *path_node = static_cast<const ObJsonPathBasicNode*>(*begin);
      ObString key_name(path_node->get_object().len_, path_node->get_object().object_name_);
      ret = lookup(key_name, child);
      if (OB_LIKELY(ret == OB_SUCCESS)) {
        ret = push_unique_hit(child, hits);
      } else if (OB_LIKELY(ret == OB_SEARCH_NOT_FOUND)) {
        ret = OB_SUCCESS;
      }
    }
  } else {
    // General multi-step path: stop scanning when a step cannot match.
    bool matched = true;
    ObJsonBinView cur = *this;
    for (JsonPathIterator it = begin; OB_SUCC(ret) && matched && it != end; ++it) {
      ObJsonPathNodeType node_type = (*it)->get_node_type();
      if (node_type == JPN_MEMBER) {
        if (cur.json_type() != ObJsonNodeType::J_OBJECT) {
          matched = false;
        } else {
          const ObJsonPathBasicNode *path_node = static_cast<const ObJsonPathBasicNode*>(*it);
          ObString key_name(path_node->get_object().len_, path_node->get_object().object_name_);
          ObJsonBinView child;
          ret = cur.lookup(key_name, child);
          if (ret == OB_SEARCH_NOT_FOUND) {
            ret = OB_SUCCESS;
            matched = false;
          } else if (OB_FAIL(ret)) {
            LOG_WARN("lookup fail", K(ret), K(key_name));
          } else {
            cur = child;
          }
        }
      } else if (node_type == JPN_ARRAY_CELL) {
        const ObJsonPathBasicNode *path_node = static_cast<const ObJsonPathBasicNode*>(*it);
        if (cur.is_array_like()) {
          ObJsonArrayIndex array_index;
          if (OB_FAIL(path_node->get_first_array_index(cur.element_count(), array_index))) {
            LOG_WARN("get array index fail", K(ret));
          } else if (!array_index.is_within_bounds()) {
            matched = false;
          } else {
            ObJsonBinView child;
            if (OB_FAIL(cur.element(array_index.get_array_index(), child))) {
              LOG_WARN("element access fail", K(ret));
            } else {
              cur = child;
            }
          }
        } else if (auto_wrap && path_node->is_autowrap()) {
          ObJsonArrayIndex array_index;
          if (OB_FAIL(path_node->get_first_array_index(1, array_index))) {
            LOG_WARN("get array index fail", K(ret));
          } else if (!array_index.is_within_bounds() || array_index.get_array_index() != 0) {
            matched = false;
          }
        } else {
          matched = false;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected path node type in seek_simple_path", K(ret), K(node_type));
      }
    }

    if (OB_SUCC(ret) && matched) {
      ret = push_unique_hit(cur, hits);
    }
  }
  return ret;
}

int ObJsonBinView::seek_complex_path(const ObJsonPath &path,
                                     int64_t node_idx,
                                     bool auto_wrap,
                                     bool only_need_one,
                                     ObIArray<ObJsonWrapper> &hits) const
{
  int ret = OB_SUCCESS;
  int64_t node_count = path.path_node_cnt();
  if (node_idx >= node_count) {
    ret = push_unique_hit(*this, hits);
  } else {
    JsonPathIterator it = path.begin() + node_idx;
    ObJsonPathNodeType node_type = (*it)->get_node_type();

    switch (node_type) {
      case JPN_MEMBER:
        ret = seek_member(path, node_idx, auto_wrap, only_need_one, hits);
        break;
      case JPN_MEMBER_WILDCARD:
        ret = seek_member_wildcard(path, node_idx, auto_wrap, only_need_one, hits);
        break;
      case JPN_ARRAY_CELL:
        ret = seek_array_cell(path, node_idx, auto_wrap, only_need_one, hits);
        break;
      case JPN_ARRAY_RANGE:
        ret = seek_array_range(path, node_idx, auto_wrap, only_need_one, hits);
        break;
      case JPN_ARRAY_CELL_WILDCARD:
        ret = seek_array_wildcard(path, node_idx, auto_wrap, only_need_one, hits);
        break;
      case JPN_WILDCARD_ELLIPSIS:
        ret = seek_ellipsis(path, node_idx, auto_wrap, only_need_one, hits);
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported path node type in seek_complex_path", K(ret), K(node_type));
        break;
    }
  }
  return ret;
}

int ObJsonBinView::seek_member(const ObJsonPath &path, int64_t node_idx,
                               bool auto_wrap, bool only_need_one,
                               ObIArray<ObJsonWrapper> &hits) const
{
  int ret = OB_SUCCESS;
  JsonPathIterator it = path.begin() + node_idx;
  if (json_type() == ObJsonNodeType::J_OBJECT) {
    const ObJsonPathBasicNode *path_node = static_cast<const ObJsonPathBasicNode*>(*it);
    ObString key_name(path_node->get_object().len_, path_node->get_object().object_name_);
    ObJsonBinView child;
    ret = lookup(key_name, child);
    if (ret == OB_SEARCH_NOT_FOUND) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("lookup fail", K(ret));
    } else {
      ret = child.seek_complex_path(path, node_idx + 1, auto_wrap, only_need_one, hits);
    }
  } else if (auto_wrap && lib::is_oracle_mode() && is_array_like()) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < element_count_; ++i) {
      if (only_need_one && hits.count() > 0) break;
      ObJsonBinView child;
      if (OB_FAIL(element(i, child))) {
        LOG_WARN("element fail", K(ret), K(i));
      } else {
        ret = child.seek_complex_path(path, node_idx, false, only_need_one, hits);
      }
    }
  }
  return ret;
}

int ObJsonBinView::seek_member_wildcard(const ObJsonPath &path, int64_t node_idx,
                                        bool auto_wrap, bool only_need_one,
                                        ObIArray<ObJsonWrapper> &hits) const
{
  int ret = OB_SUCCESS;
  if (json_type() == ObJsonNodeType::J_OBJECT) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < element_count_; ++i) {
      if (only_need_one && hits.count() > 0) break;
      ObJsonBinView child;
      if (OB_FAIL(element(i, child))) {
        LOG_WARN("element fail", K(ret), K(i));
      } else {
        ret = child.seek_complex_path(path, node_idx + 1, auto_wrap, only_need_one, hits);
      }
    }
  } else if (auto_wrap && lib::is_oracle_mode() && is_array_like()) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < element_count_; ++i) {
      if (only_need_one && hits.count() > 0) break;
      ObJsonBinView child;
      if (OB_FAIL(element(i, child))) {
        LOG_WARN("element fail", K(ret), K(i));
      } else {
        ret = child.seek_complex_path(path, node_idx, false, only_need_one, hits);
      }
    }
  }
  return ret;
}

int ObJsonBinView::seek_array_cell(const ObJsonPath &path, int64_t node_idx,
                                   bool auto_wrap, bool only_need_one,
                                   ObIArray<ObJsonWrapper> &hits) const
{
  int ret = OB_SUCCESS;
  JsonPathIterator it = path.begin() + node_idx;
  const ObJsonPathBasicNode *path_node = static_cast<const ObJsonPathBasicNode*>(*it);
  if (is_array_like()) {
    ObJsonArrayIndex array_index;
    if (OB_FAIL(path_node->get_first_array_index(element_count(), array_index))) {
      LOG_WARN("get array index fail", K(ret));
    } else if (array_index.is_within_bounds()) {
      ObJsonBinView child;
      if (OB_FAIL(element(array_index.get_array_index(), child))) {
        LOG_WARN("element fail", K(ret));
      } else {
        ret = child.seek_complex_path(path, node_idx + 1, auto_wrap, only_need_one, hits);
      }
    }
  } else if (auto_wrap && path_node->is_autowrap()) {
    ObJsonArrayIndex array_index;
    if (OB_FAIL(path_node->get_first_array_index(1, array_index))) {
      LOG_WARN("get array index fail", K(ret));
    } else if (array_index.is_within_bounds() && array_index.get_array_index() == 0) {
      ret = seek_complex_path(path, node_idx + 1, auto_wrap, only_need_one, hits);
    }
  }
  return ret;
}

int ObJsonBinView::seek_array_range(const ObJsonPath &path, int64_t node_idx,
                                    bool auto_wrap, bool only_need_one,
                                    ObIArray<ObJsonWrapper> &hits) const
{
  int ret = OB_SUCCESS;
  JsonPathIterator it = path.begin() + node_idx;
  const ObJsonPathBasicNode *path_node = static_cast<const ObJsonPathBasicNode*>(*it);
  if (is_array_like()) {
    ObArrayRange range;
    if (OB_FAIL(path_node->get_array_range(element_count(), range))) {
      LOG_WARN("get array range fail", K(ret));
    } else {
      for (uint64_t i = range.array_begin_; OB_SUCC(ret) && i < range.array_end_ && i < element_count_; ++i) {
        if (only_need_one && hits.count() > 0) break;
        ObJsonBinView child;
        if (OB_FAIL(element(i, child))) {
          LOG_WARN("element fail", K(ret), K(i));
        } else {
          ret = child.seek_complex_path(path, node_idx + 1, auto_wrap, only_need_one, hits);
        }
      }
    }
  } else if (auto_wrap && path_node->is_autowrap()) {
    ret = seek_complex_path(path, node_idx + 1, auto_wrap, only_need_one, hits);
  }
  return ret;
}

int ObJsonBinView::seek_array_wildcard(const ObJsonPath &path, int64_t node_idx,
                                       bool auto_wrap, bool only_need_one,
                                       ObIArray<ObJsonWrapper> &hits) const
{
  int ret = OB_SUCCESS;
  JsonPathIterator it = path.begin() + node_idx;
  const ObJsonPathBasicNode *path_node = static_cast<const ObJsonPathBasicNode*>(*it);
  if (is_array_like()) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < element_count_; ++i) {
      if (only_need_one && hits.count() > 0) break;
      ObJsonBinView child;
      if (OB_FAIL(element(i, child))) {
        LOG_WARN("element fail", K(ret), K(i));
      } else {
        ret = child.seek_complex_path(path, node_idx + 1, auto_wrap, only_need_one, hits);
      }
    }
  } else if (auto_wrap && (lib::is_oracle_mode() ? path_node->is_multi_array_autowrap()
                                                  : path_node->is_autowrap())) {
    ret = seek_complex_path(path, node_idx + 1, auto_wrap, only_need_one, hits);
  }
  return ret;
}

int ObJsonBinView::seek_ellipsis(const ObJsonPath &path, int64_t node_idx,
                                 bool auto_wrap, bool only_need_one,
                                 ObIArray<ObJsonWrapper> &hits) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(seek_complex_path(path, node_idx + 1, auto_wrap, only_need_one, hits))) {
    LOG_WARN("seek after ellipsis fail", K(ret));
  }

  if (OB_SUCC(ret) && !(only_need_one && hits.count() > 0)) {
    if (is_container()) {
      for (uint64_t i = 0; OB_SUCC(ret) && i < element_count_; ++i) {
        if (only_need_one && hits.count() > 0) break;
        ObJsonBinView child;
        if (OB_FAIL(element(i, child))) {
          LOG_WARN("element fail", K(ret), K(i));
        } else {
          ret = child.seek_ellipsis(path, node_idx, auto_wrap, only_need_one, hits);
        }
      }
    }
  }
  return ret;
}

uint32_t ObJsonBinView::depth() const
{
  int ret = OB_SUCCESS;
  uint32_t depth = 1;
  if (is_container()) {
    uint32_t max_child = 0;
    for (uint64_t i = 0; OB_SUCC(ret) && i < element_count_; i++) {
      ObJsonBinView child;
      if (OB_FAIL(element(i, child))) {
        LOG_WARN("get element failed in depth", K(ret), K(i));
      } else {
        max_child = max(max_child, child.depth());
      }
    }
    depth = max_child + 1;
  }
  return depth;
}

} // namespace common
} // namespace oceanbase
