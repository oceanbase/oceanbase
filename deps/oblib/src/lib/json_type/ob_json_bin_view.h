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

#ifndef OCEANBASE_SQL_OB_JSON_BIN_VIEW
#define OCEANBASE_SQL_OB_JSON_BIN_VIEW

#include "ob_json_common.h"
#include "ob_json_bin.h"
#include "ob_json_compare.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
namespace common {

class ObJsonPath;
class ObJsonWrapper;

// Template helper: read variable-length integer at compile-time known type.
// Eliminates switch overhead in hot loops.
template<typename VarSizeType>
struct ReadVar {
  static OB_INLINE uint64_t read(const char *data) {
    return static_cast<uint64_t>(*reinterpret_cast<const VarSizeType*>(data));
  }
};

// ObJsonBinView: lightweight, zero-virtual-function binary JSON reader.
// ~32 bytes on x86-64, stack-allocated, passed by value.
// No base class, no allocator, no memory allocation.
// Storage semantics:
//   - data_ points to the binary location identifying this node.
//     Container/String/Opaque keep the type byte in data_; non-inline scalars
//     keep the payload start in data_; inline scalars keep the value entry address.
//   - Container offsets share storage with inline/scalar decoded values.
class ObJsonBinView
{
public:
  ObJsonBinView()
    : data_(nullptr),
      uint_val_(0),
      element_count_(0),
      length_(0),
      type_(0xFF),
      entry_var_type_(0),
      use_lex_order_(false)
  {}

  // Initialize view from a complete JSON binary document (with doc header)
  int init(const char *data, int64_t len);

  // Type accessors - all inline, zero overhead
  OB_INLINE ObJsonNodeType json_type() const
  {
    return ObJsonVerType::get_json_type(vertype());
  }

  OB_INLINE ObJBVerType vertype() const
  {
    return static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(type_));
  }

  OB_INLINE bool is_object() const
  {
    return json_type() == ObJsonNodeType::J_OBJECT;
  }

  OB_INLINE bool is_array() const
  {
    return json_type() == ObJsonNodeType::J_ARRAY;
  }

  OB_INLINE bool is_semi_hete_col() const
  {
    return json_type() == ObJsonNodeType::J_SEMI_HETE_COL;
  }

  OB_INLINE bool is_array_like() const
  {
    return is_array() || is_semi_hete_col();
  }

  OB_INLINE bool is_container() const
  {
    return is_object() || is_array_like();
  }

  OB_INLINE uint32_t element_count() const { return element_count_; }
  OB_INLINE uint64_t member_count() const { return element_count_; }

  // Scalar value getters
  OB_INLINE int64_t get_int() const { return int_val_; }
  OB_INLINE uint64_t get_uint() const { return uint_val_; }
  OB_INLINE double get_double() const { return double_val_; }
  OB_INLINE bool get_boolean() const { return static_cast<bool>(uint_val_); }
  OB_INLINE float get_float() const { return float_val_; }
  int get_obtime(ObTime &t) const;

  // Get string data. For STRING and OPAQUE types only.
  // STRING: data_ points to type byte, skip type(1) + varint length prefix
  // OPAQUE: data_ points to type byte, skip type(1) + field_type(2) + data_len(8) = 11 bytes
  OB_INLINE ObString get_string() const;

  // Decimal: on-the-fly deserialization from raw binary.
  // data_ points to prec[0]. Decimal payload: prec(2) + scale(2) + ObNumber data.
  // digits_ in the returned ObNumber points into the original document buffer (zero-copy).
  number::ObNumber get_decimal_data() const;
  OB_INLINE ObPrecision get_decimal_precision() const;
  OB_INLINE ObScale get_decimal_scale() const;

  // Opaque: read field_type from data_ + 1. Time types: derive from json_type().
  ObObjType field_type() const;

  // Get raw binary slice for current node (without doc header).
  // Only returns slices that already include a type prefix (container/string/opaque).
  // Scalars and decimal use get_raw_binary(), which prepends the type byte when needed.
  OB_INLINE ObString raw_binary() const
  {
    ObString res;
    if (OB_NOT_NULL(data_) && ObJsonBin::need_type_prefix(static_cast<uint8_t>(vertype())) && length_ > 0) {
      res.assign_ptr(data_, static_cast<int32_t>(length_));
    }
    return res;
  }

  // Get raw binary with doc header prepended (needs allocator for non-root).
  // has_lob_header: if true, prepend ObLobCommon header before the JSON binary.
  int get_raw_binary(ObIAllocator &alloc, ObString &out, bool has_lob_header = false) const;
  int seek(const ObJsonPath &path,
           ObIArray<ObJsonWrapper> &hits,
           bool auto_wrap = false,
           bool only_need_one = false) const;

  uint32_t depth() const;

  // Check if this view has been successfully initialized.
  // Default-constructed views have type_ = 0xFF.
  OB_INLINE bool is_valid() const { return type_ != 0xFF; }

  // Compare two bin views. Returns 0 if equal, <0 if a<b, >0 if a>b.
  static OB_INLINE int compare(const ObJsonBinView &a, const ObJsonBinView &b, int &result, bool is_path = false)
  { return ObJsonCompare::compare(a, b, result, is_path); }

  // Required by ObSEArray/ObIArray for printing
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "{type:%u, element_count:%u, length:%u}",
                    type_, element_count_, length_);
    return pos;
  }

  // Object: O(log n) lookup by key with key-length shortcut
  int lookup(const ObString &key, ObJsonBinView &result) const;

  // Object/Array: O(1) element access by index
  int element(uint64_t pos, ObJsonBinView &result) const;

  // Object: get key string at given index (zero-copy, points into data_)
  int get_key(uint64_t index, ObString &key) const;

  // Object/Array: get val jsonbinview at given index (zero-copy, points into data_)
  OB_INLINE int get_value(uint64_t index, ObJsonBinView &value) const
  { return element(index, value); }

  // Object: get key and value at given index in one call
  int get_key_value(uint64_t index, ObString &key, ObJsonBinView &value) const;

private:
  OB_INLINE bool is_inline() const
  {
    return OB_JSON_TYPE_IS_INLINE(type_);
  }

  OB_INLINE uint32_t length() const { return length_; }

  // Get the binary location identifying this node. For non-inline scalar nodes this is the
  // payload start; for inline scalar nodes this is the containing value entry address.
  OB_INLINE const char *data() const { return data_; }

  // Parse node at given offset within the buffer.
  // Container offset points to the type byte; scalar/string/opaque offsets point to the payload after it.
  int parse_node(const char *buf, int64_t buf_len, int64_t offset);
  int parse_container(const char *buf, int64_t buf_len, int64_t offset, bool is_object);
  int parse_scalar(const char *buf, int64_t buf_len, int64_t value_offset);

  // Internal helpers for entry access
  OB_INLINE uint32_t entry_var_size() const
  { return static_cast<uint32_t>(ObJsonVar::get_var_size(entry_var_type_)); }

  // Template-ized entry accessors (no switch in typed path)
  template<typename VarSizeType>
  OB_INLINE void read_key_entry(int index, uint64_t &key_offset, uint64_t &key_len) const
  {
    const int64_t var_size = sizeof(VarSizeType);
    uint64_t offset = key_offset_start_ + index * (var_size * 2);
    key_offset = ReadVar<VarSizeType>::read(data_ + offset);
    key_len = ReadVar<VarSizeType>::read(data_ + offset + var_size);
  }

  template<typename VarSizeType>
  OB_INLINE void read_value_entry(int index, uint64_t &value_offset, uint8_t &value_type) const
  {
    const int64_t var_size = sizeof(VarSizeType);
    uint64_t offset = value_offset_start_ + index * (var_size + OB_JSON_BIN_VALUE_TYPE_LEN);
    value_offset = ReadVar<VarSizeType>::read(data_ + offset);
    value_type = static_cast<uint8_t>(*(data_ + offset + var_size));
  }

  // Template-ized lookup/element implementations
  template<typename VarSizeType>
  int lookup_impl(const ObString &key, ObJsonBinView &result) const;
  template<typename VarSizeType>
  int element_impl(uint64_t pos, ObJsonBinView &result) const;

  template<typename VarSizeType>
  int get_key_impl(uint64_t index, ObString &key) const;
  template<typename VarSizeType>
  int get_key_value_impl(uint64_t index, ObString &key, ObJsonBinView &value) const;

  // Helper to populate result from a value entry
  int populate_child(uint64_t value_offset, uint8_t value_type, ObJsonBinView &result) const;
  int populate_inline_child(uint64_t value_offset, uint8_t value_type,
                            int entry_index, ObJsonBinView &result) const;

  // Encode scalar payload (type byte + value) into buffer.
  // Used by get_raw_binary() for inline scalar types.
  int encode_scalar_payload(char *buf, int64_t buf_size, int64_t &pos) const;

  // Seek helpers
  int seek_simple_path(const ObJsonPath &path,
                       ObIArray<ObJsonWrapper> &hits,
                       bool auto_wrap) const;
  int seek_complex_path(const ObJsonPath &path,
                        int64_t node_idx,
                        bool auto_wrap, bool only_need_one,
                        ObIArray<ObJsonWrapper> &hits) const;
  int seek_member(const ObJsonPath &path, int64_t node_idx,
                  bool auto_wrap, bool only_need_one,
                  ObIArray<ObJsonWrapper> &hits) const;
  int seek_member_wildcard(const ObJsonPath &path, int64_t node_idx,
                           bool auto_wrap, bool only_need_one,
                           ObIArray<ObJsonWrapper> &hits) const;
  int seek_array_cell(const ObJsonPath &path, int64_t node_idx,
                      bool auto_wrap, bool only_need_one,
                      ObIArray<ObJsonWrapper> &hits) const;
  int seek_array_range(const ObJsonPath &path, int64_t node_idx,
                       bool auto_wrap, bool only_need_one,
                       ObIArray<ObJsonWrapper> &hits) const;
  int seek_array_wildcard(const ObJsonPath &path, int64_t node_idx,
                          bool auto_wrap, bool only_need_one,
                          ObIArray<ObJsonWrapper> &hits) const;
  int seek_ellipsis(const ObJsonPath &path, int64_t node_idx,
                    bool auto_wrap, bool only_need_one,
                    ObIArray<ObJsonWrapper> &hits) const;
  bool is_same_view(const ObJsonBinView &other) const;
  static int push_unique_hit(const ObJsonBinView &view,
                             ObIArray<ObJsonWrapper> &hits);

private:
  const char *data_;        // Binary location of this node (see Storage semantics above)
  union {
    struct {
      uint32_t key_offset_start_;   // For containers: key entry start offset relative to data_
      uint32_t value_offset_start_; // For containers: value entry start offset relative to data_
    };
    int64_t int_val_;
    uint64_t uint_val_;
    double double_val_;
    float float_val_;
  };
  uint32_t element_count_;  // element count for containers
  uint32_t length_;         // bytes available at data_; includes type byte only for prefixed types
  uint8_t type_;            // raw type byte (may include inline flag)
  uint8_t entry_var_type_;  // var size type for entries (JBLS_UINT8..64)
  bool use_lex_order_;      // for object key comparison
};

static_assert(sizeof(ObJsonBinView) == 32, "ObJsonBinView should be 32 bytes");

// Lightweight ObIJsonBase adapter wrapping ObJsonBinView.
// Stack-allocated, zero memory allocation.
// Supports scalar types (J_NULL/J_BOOLEAN/J_INT/J_UINT/J_DOUBLE/J_FLOAT/J_STRING/
// J_DECIMAL and their Oracle aliases), all time types, and J_OPAQUE — by delegating
// to the underlying ObJsonBinView, which already decodes them. For containers
// (J_ARRAY/J_OBJECT/J_SEMI_HETE_COL), use ObJsonWrapper::to_json_base() instead.
class ObJsonBinViewAdapter : public ObIJsonBase
{
public:
  explicit ObJsonBinViewAdapter(const ObJsonBinView &view, ObIAllocator *alloc = nullptr)
    : ObIJsonBase(alloc), view_(view) {}

  // True for the types this adapter fully supports (see class doc above).
  // Containers (J_ARRAY/J_OBJECT/J_SEMI_HETE_COL) must still be materialized via
  // ObJsonWrapper::to_json_base(), since the adapter returns OB_NOT_SUPPORTED for
  // container navigation.
  static bool is_supported_type(ObJsonNodeType type)
  {
    bool result = false;
    switch (type) {
      case ObJsonNodeType::J_NULL:
      case ObJsonNodeType::J_BOOLEAN:
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_STRING:
      // Oracle aliases
      case ObJsonNodeType::J_OINT:
      case ObJsonNodeType::J_OLONG:
      case ObJsonNodeType::J_ODOUBLE:
      case ObJsonNodeType::J_OFLOAT:
      case ObJsonNodeType::J_ODECIMAL:
      case ObJsonNodeType::J_OBINARY:
      case ObJsonNodeType::J_OOID:
      case ObJsonNodeType::J_ORAWHEX:
      case ObJsonNodeType::J_ORAWID:
      case ObJsonNodeType::J_ODAYSECOND:
      case ObJsonNodeType::J_OYEARMONTH:
      // Time types
      case ObJsonNodeType::J_DATE:
      case ObJsonNodeType::J_MYSQL_DATE:
      case ObJsonNodeType::J_ORACLEDATE:
      case ObJsonNodeType::J_TIME:
      case ObJsonNodeType::J_DATETIME:
      case ObJsonNodeType::J_MYSQL_DATETIME:
      case ObJsonNodeType::J_TIMESTAMP:
      case ObJsonNodeType::J_ODATE:
      case ObJsonNodeType::J_OTIMESTAMP:
      case ObJsonNodeType::J_OTIMESTAMPTZ:
      // Opaque
      case ObJsonNodeType::J_OPAQUE:
        result = true;
        break;
      default:  // containers
        result = false;
        break;
    }
    return result;
  }

  ObJsonInType get_internal_type() const override { return ObJsonInType::JSON_BIN; }
  ObJsonNodeType json_type() const override { return view_.json_type(); }
  ObObjType field_type() const override { return view_.field_type(); }
  uint64_t element_count() const override { return view_.element_count(); }
  uint32_t depth() const override { return 0; }
  uint64_t member_count() const override { return 1; }

  bool get_boolean() const override { return view_.get_boolean(); }
  double get_double() const override { return view_.get_double(); }
  float get_float() const override { return view_.get_float(); }
  int64_t get_int() const override { return view_.get_int(); }
  uint64_t get_uint() const override { return view_.get_uint(); }
  const char *get_data() const override { return view_.get_string().ptr(); }
  uint64_t get_data_length() const override
  {
    return static_cast<uint64_t>(view_.get_string().length());
  }

  number::ObNumber get_decimal_data() const override { return view_.get_decimal_data(); }
  ObPrecision get_decimal_precision() const override { return view_.get_decimal_precision(); }
  ObScale get_decimal_scale() const override { return view_.get_decimal_scale(); }

  int get_raw_binary(common::ObString &out, ObIAllocator *allocator = nullptr) override;

  // Time - delegate to the underlying view, which decodes all time types.
  int get_obtime(ObTime &t) const override { return view_.get_obtime(t); }

  // Container / parent navigation - not needed for scalar cast.
  int get_parent(ObIJsonBase *& /*parent*/) const override { return OB_NOT_SUPPORTED; }
  int get_key(uint64_t /*index*/, common::ObString & /*key*/) const override { return OB_NOT_SUPPORTED; }
  int get_array_element(uint64_t /*index*/, ObIJsonBase *& /*value*/) const override { return OB_NOT_SUPPORTED; }
  int get_object_value(uint64_t /*index*/, ObIJsonBase *& /*value*/) const override { return OB_NOT_SUPPORTED; }
  int get_object_value(uint64_t /*index*/, common::ObString & /*key*/, ObIJsonBase *& /*value*/) const override
  {
    return OB_NOT_SUPPORTED;
  }
  int get_object_value(const common::ObString & /*key*/, ObIJsonBase *& /*value*/) const override
  {
    return OB_NOT_SUPPORTED;
  }

  // Write operations - not supported.
  int array_append(ObIJsonBase * /*value*/) override { return OB_NOT_SUPPORTED; }
  int array_insert(uint64_t /*index*/, ObIJsonBase * /*value*/) override { return OB_NOT_SUPPORTED; }
  int array_remove(uint64_t /*index*/) override { return OB_NOT_SUPPORTED; }
  int object_add(const common::ObString & /*key*/, ObIJsonBase * /*value*/) override { return OB_NOT_SUPPORTED; }
  int object_remove(const common::ObString & /*key*/) override { return OB_NOT_SUPPORTED; }
  int replace(const ObIJsonBase * /*old_node*/, ObIJsonBase * /*new_node*/) override { return OB_NOT_SUPPORTED; }

private:
  const ObJsonBinView &view_;
};

// ============ Inline method implementations ============
OB_INLINE ObString ObJsonBinView::get_string() const
{
  ObString res;
  ObJsonNodeType nt = json_type();
  // J_STRING and the Oracle string-like / interval types all use the same
  // [type][varint len][data] layout, so they share this read path.
  if (nt == ObJsonNodeType::J_STRING ||
      nt == ObJsonNodeType::J_OBINARY || nt == ObJsonNodeType::J_OOID ||
      nt == ObJsonNodeType::J_ORAWHEX || nt == ObJsonNodeType::J_ORAWID ||
      nt == ObJsonNodeType::J_ODAYSECOND || nt == ObJsonNodeType::J_OYEARMONTH) {
    // data_ points to type byte. Skip type(1) + varint length prefix to reach string data.
    // decode_vi64 advances pos past the varint.
    int64_t pos = 1;  // skip type byte
    int64_t len = 0;
    serialization::decode_vi64(data_, static_cast<int64_t>(length_), pos, &len);
    res.assign_ptr(data_ + pos, static_cast<int32_t>(len));
  } else if (nt == ObJsonNodeType::J_OPAQUE) {
    // Opaque binary layout from type byte: type(1) + field_type(2) + data_len(8) + data
    static const int OPAQUE_DATA_OFFSET = 1 + sizeof(uint16_t) + sizeof(uint64_t);  // = 11
    // data_ points to type byte. Opaque data starts at fixed offset 11.
    const int64_t data_len =
      *reinterpret_cast<const int64_t*>(data_ + sizeof(uint8_t) + sizeof(uint16_t));
    res.assign_ptr(data_ + OPAQUE_DATA_OFFSET, static_cast<int32_t>(data_len));
  }
  return res;
}

OB_INLINE ObPrecision ObJsonBinView::get_decimal_precision() const
{
  // data_ points to prec[0]. Decimal payload: prec(2) + scale(2) + number_data.
  ObPrecision prec = -1;
  int64_t pos = 0;
  serialization::decode_i16(data_, static_cast<int64_t>(length_), pos, &prec);
  return prec;
}

OB_INLINE ObScale ObJsonBinView::get_decimal_scale() const
{
  // scale is at data_[2] (after prec).
  ObScale scale = -1;
  int64_t pos = static_cast<int64_t>(sizeof(int16_t));  // skip prec
  serialization::decode_i16(data_, static_cast<int64_t>(length_), pos, &scale);
  return scale;
}

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_JSON_BIN_VIEW
