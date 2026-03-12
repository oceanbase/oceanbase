/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_SEARCH_INDEX_ENCODER_H_
#define OCEANBASE_SHARE_OB_SEARCH_INDEX_ENCODER_H_

#include "lib/ob_define.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_common.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_order_perserving_encoder.h"
#include "share/datum/ob_datum.h"

namespace oceanbase
{
namespace common
{
class ObIJsonBase;
}

namespace share
{

enum ObSearchIndexKeyIndex
{
  SEARCH_INDEX_COL_IDX = 0,
  SEARCH_INDEX_PATH    = 1,
  SEARCH_INDEX_VALUE   = 2,
  SEARCH_INDEX_DOC_ID  = 3,
  SEARCH_INDEX_KEY_CNT
};

// Total length of rowkey of search index is 16KB (PATH + VALUE = 2KB + 14KB)
//
// Maximum length for the encoded JSON path in search index.
// This includes the encoded path expression used to locate values in JSON documents.
// If the encoded path length exceeds this limit (2KB), DML operations (INSERT/UPDATE/DELETE)
// or index creation will fail with an error.
static const int64_t SEARCH_INDEX_PATH_LENGTH = 2 * 1024; // 2KB
//
// Maximum length for the indexed value in search index.
// This is the actual content extracted from JSON documents at the specified path.
// If the value length exceeds this limit (14KB), the value will be silently truncated
// to fit within the limit. No error will be reported in this case.
static const int64_t SEARCH_INDEX_VALUE_LENGTH = 14 * 1024; // 14KB

// Search index path encoder
class ObSearchIndexPathEncoder
{
public:
  enum JsonEncodeType : uint8_t {
    ESCAPE_MARKER       = 0x00,
    LOWER_BOUND         = 0x01,

    // mysql types
    TYPE_NULL           = 0x10,  // J_NULL
    TYPE_DECIMAL        = 0x14,  // J_DECIMAL/ J_INT/ J_UINT/ J_DOUBLE
    TYPE_STRING         = 0x18,  // J_STRING
    TYPE_OBJECT         = 0x1c,  // J_OBJECT
    TYPE_ARRAY          = 0x20,  // J_ARRAY
    TYPE_BOOLEAN        = 0x24,  // J_BOOLEAN
    TYPE_DATE           = 0x28,  // J_DATE
    TYPE_TIME           = 0x2c,  // J_TIME
    TYPE_DATETIME       = 0x30,  // J_DATETIME/ J_TIMESTAMP
    TYPE_OPAQUE         = 0x34,  // J_OPAQUE
    TYPE_MYSQL_DATE     = 0x38,  // J_MYSQL_DATE
    TYPE_MYSQL_DATETIME = 0x3c,  // J_MYSQL_DATETIME

    UPPER_BOUND         = 0xE0,

    ESCAPED_NULL        = 0xF0   // ESCAPED_NULL is the escaped version of ESCAPE_MARKER
  };

  struct JsonPathItem
  {
    JsonPathItem() : type(ESCAPE_MARKER), path() {}
    JsonPathItem(JsonEncodeType type, ObString path) : type(type), path(path) {}
    bool is_array_path() const { return type == TYPE_ARRAY; }

    TO_STRING_KV(K(type), K(path));

    JsonEncodeType type;
    ObString path;
  };

  static JsonPathItem make_object_path(ObString path) { return JsonPathItem(TYPE_OBJECT, path); }
  static JsonPathItem make_array_path() { return JsonPathItem(TYPE_ARRAY, ObString()); }

private:
  static int64_t calc_prefix_path_size(const ObIArray<JsonPathItem> &path_items,
                                       bool &has_escaped_null);
  static int encode_prefix_path(const ObIArray<JsonPathItem> &path_items,
                                const bool has_escaped_null,
                                uint8_t *mem, int64_t &pos);
  static int build_path(ObIAllocator &allocator,
                        const ObString &prefix,
                        const ObString &suffix,
                        ObString &path);

public:
  // encode json type as a part of path
  static int encode_type(common::ObJsonNodeType json_type, uint8_t &encode_type);
  // path = basic_path + type
  static int encode_path_prefix(ObIAllocator &allocator, const ObIArray<JsonPathItem> &path_items,
                                ObString &path);
  static int encode_path(ObIAllocator &allocator, const ObIArray<JsonPathItem> &path_items,
                         const ObJsonNodeType scalar_type, ObString &path);
  static int encode_pick_path(const ObItemType pick_type, uint8_t &enc_type);

  // build path for query range.
  static int generate_upper_path(ObIAllocator &allocator, const ObString &prefix, ObString &path);
  static int generate_lower_path(ObIAllocator &allocator, const ObString &prefix, ObString &path);
  static int generate_array_path(ObIAllocator &allocator, const ObString &prefix, ObString &path);
  static int generate_multi_array_path(ObIAllocator &allocator, const ObString &prefix,
                                       ObString &path);
  static int generate_pick_path(ObIAllocator &allocator, const ObString &prefix,
                                const ObItemType pick_type, ObString &path);
  static int generate_enc_path(ObIAllocator &allocator, const ObString &prefix,
                               const uint8_t enc_type, ObString &path);

  // encode scalar path.
  // Encode search index path from path_prefix and json_base
  // Result format: path_prefix + ESCAPE_MARKER + encode_type
  static int encode_path(common::ObIAllocator &allocator,
                         const common::ObString &path_prefix,
                         const common::ObIJsonBase *j_base,
                         common::ObString &result);

  // Enocde search index path with array type (for matching array elements)
  static int encode_array_path(common::ObIAllocator &allocator,
                               const common::ObString &path_prefix,
                               const common::ObIJsonBase *j_base,
                               common::ObString &result);

  // Encode search index path with multi-dimension array type
  // (for matching multi-dimension array elements)
  static int encode_multi_array_path(common::ObIAllocator &allocator,
                                     const common::ObString &path_prefix,
                                     const common::ObIJsonBase *j_base,
                                     common::ObString &result);
};

// Search index value encoder
class ObSearchIndexValueEncoder {
public:
  template<typename T>
  static int encode_int(ObIAllocator &allocator, T int_value, ObString &value);
  template<typename T>
  static int encode_uint(ObIAllocator &allocator, T uint_value, ObString &value);
  static int encode_double(ObIAllocator &allocator, double double_value, ObString &value);
  static int encode_float(ObIAllocator &allocator, float float_value, ObString &value);
  static int encode_number(ObIAllocator &allocator, const ObNumber &number, ObString &value);
  static int encode_decimal_int(ObIAllocator &allocator,
                                const ObDecimalInt *decint,
                                const int32_t int_bytes,
                                ObString &value);
  static int encode_int32(ObIAllocator &allocator, int32_t int32_value, ObString &value);
  static int encode_uint8(ObIAllocator &allocator, uint8_t uint8_value, ObString &value);
  static int encode_string(ObIAllocator &allocator, const ObString &string_value,
                           ObString &value, ObEncParam &enc_param);
  static int init_string_enc_param(const ObObjType obj_type,
                                   const ObCollationType cs_type,
                                   const ObSQLMode sql_mode,
                                   const bool is_null_first,
                                   const bool is_asc,
                                   ObEncParam &enc_param);
  static int encode_json_scalar(ObIAllocator &allocator, const ObIJsonBase *j_base,
                                ObString &value, ObEncParam &enc_param);
  static bool is_json_scalar(const ObIJsonBase *j_base) {
    return OB_ISNULL(j_base) ? false : j_base->is_json_scalar(j_base->json_type());
  }
  static int init_json_string_enc_param(ObEncParam &enc_param);

  // Encode search index value from scalar datum (non-JSON type)
  static int encode_value(common::ObIAllocator &allocator, const ObDatum &datum,
                          const common::ObObjMeta &obj_meta, ObString &value);

  // Encode search index value from scalar object (non-JSON type)
  static int encode_value(common::ObIAllocator &allocator,
                          const common::ObObj &obj,
                          common::ObString &value);

  // Encode search index value from JSON scalar
  static int encode_value(common::ObIAllocator &allocator,
                          const common::ObIJsonBase *j_base,
                          common::ObString &value);

  static bool string_safety_to_compare(const ObString &str);
  static bool string_column_may_truncate(const ObObjMeta &column_meta,
                                         const ObLength column_length);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SEARCH_INDEX_ENCODER_H_
