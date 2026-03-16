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

#define USING_LOG_PREFIX SHARE

#include "share/search_index/ob_search_index_encoder.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/number/ob_number_v2.h"
#include "share/datum/ob_datum.h"
#include "share/ob_order_perserving_encoder.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/json_type/ob_json_bin.h"
#include "common/sql_mode/ob_sql_mode_utils.h"

namespace oceanbase
{
namespace share
{


int ObSearchIndexPathEncoder::encode_type(ObJsonNodeType json_type, uint8_t &enc_type)
{
  int ret = OB_SUCCESS;
  switch (json_type) {
    case ObJsonNodeType::J_NULL: {
      enc_type = TYPE_NULL;
      break;
    }
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_DOUBLE: {
      enc_type = TYPE_DECIMAL;
      break;
    }
    case ObJsonNodeType::J_STRING: {
      enc_type = TYPE_STRING;
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      enc_type = TYPE_BOOLEAN;
      break;
    }
    case ObJsonNodeType::J_DATE: {
      enc_type = TYPE_DATE;
      break;
    }
    case ObJsonNodeType::J_TIME: {
      enc_type = TYPE_TIME;
      break;
    }
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP: {
      enc_type = TYPE_DATETIME;
      break;
    }
    case ObJsonNodeType::J_OPAQUE: {
      enc_type = TYPE_OPAQUE;
      break;
    }
    case ObJsonNodeType::J_MYSQL_DATE: {
      enc_type = TYPE_MYSQL_DATE;
      break;
    }
    case ObJsonNodeType::J_MYSQL_DATETIME: {
      enc_type = TYPE_MYSQL_DATETIME;
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported json node type", K(ret), K(json_type));
    }
  }
  return ret;
}

static inline int64_t count_null_bytes(const ObString &str) {
  int64_t count = 0;
  const char *ptr = str.ptr();
  const int32_t len = str.length();
  for (int32_t i = 0; i < len; ++i) {
    if (static_cast<uint8_t>(ptr[i]) == ObSearchIndexPathEncoder::JsonEncodeType::ESCAPE_MARKER) {
      ++count;
    }
  }
  return count;
}

/*
  calculate the size of the encoded path
  $ => 0x00 type
  $.path => 0x00 0x1c path 0x00 type
  $.path.subpath => 0x00 0x1c path 0x00 0x1c subpath 0x00 type
  $.path[] => 0x00 0x1c path 0x00 0x20 0x00 type
  $.path[][][] => 0x00 0x1c path 0x00 0x20 0x00 0x20 0x00 type
  $.path[].subpath => 0x00 0x1c path 0x00 0x20 0x00 0x1c subpath 0x00 type
*/
int64_t ObSearchIndexPathEncoder::calc_prefix_path_size(const ObIArray<JsonPathItem> &path_items,
                                                        bool &has_escaped_null) {
  int64_t size = 0;
  const static int64_t ESCAPE_SIZE = 2; // escape marker + escape value
  int64_t null_bytes = 0;
  int64_t continue_array_cnt = 0;
  for (int64_t i = 0; i < path_items.count(); ++i) {
    const JsonPathItem &item = path_items.at(i);
    if (item.is_array_path()) {
      ++continue_array_cnt;
      if (continue_array_cnt <= 2) {
        // 0x00 0x20
        size += ESCAPE_SIZE;
      }
    } else {
      continue_array_cnt = 0;
      // 0x00 0x1c
      size += ESCAPE_SIZE;
      null_bytes += count_null_bytes(item.path);
      size += item.path.length();
    }
  }
  size += null_bytes;
  has_escaped_null = null_bytes > 0;
  return size;
}

int ObSearchIndexPathEncoder::encode_prefix_path(const ObIArray<JsonPathItem> &path_items,
                                                  const bool has_escaped_null,
                                                  uint8_t *mem, int64_t &pos) {
  int ret = OB_SUCCESS;
  int64_t continue_array_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < path_items.count(); ++i) {
    const JsonPathItem &item = path_items.at(i);
    if (item.is_array_path()) {
      ++continue_array_cnt;
      if (continue_array_cnt <= 2) {
        mem[pos++] = ESCAPE_MARKER;
        mem[pos++] = TYPE_ARRAY;
      }
    } else {
      continue_array_cnt = 0;
      mem[pos++] = ESCAPE_MARKER;
      mem[pos++] = TYPE_OBJECT;
      if (OB_UNLIKELY(has_escaped_null)) {
        const uint8_t *path_ptr = reinterpret_cast<const uint8_t*>(item.path.ptr());
        int64_t path_len = item.path.length();
        for (int64_t j = 0; j < path_len; ++j) {
          if (path_ptr[j] == ESCAPE_MARKER) {
            mem[pos++] = ESCAPE_MARKER;
            mem[pos++] = ESCAPED_NULL;
          } else {
            mem[pos++] = path_ptr[j];
          }
        }
      } else {
        MEMCPY(mem + pos, item.path.ptr(), item.path.length());
        pos += item.path.length();
      }
    }
  }
  return ret;
}

int ObSearchIndexPathEncoder::encode_path_prefix(ObIAllocator &allocator,
                                                 const ObIArray<JsonPathItem> &path_items,
                                                 ObString &path) {
  int ret = OB_SUCCESS;
  bool has_escaped_null = false;
  int64_t size = calc_prefix_path_size(path_items, has_escaped_null);
  int64_t pos = 0;
  uint8_t *mem = static_cast<uint8_t*>(allocator.alloc(size));
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(size));
  } else if (OB_FAIL(encode_prefix_path(path_items, has_escaped_null, mem, pos))) {
    LOG_WARN("failed to encode prefix path", K(ret));
  } else if (pos > size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected size", K(ret), K(pos), K(size));
  } else {
    path.assign_ptr(reinterpret_cast<char*>(mem), pos);
  }
  return ret;
}

int ObSearchIndexPathEncoder::encode_path(ObIAllocator &allocator,
                                          const ObIArray<JsonPathItem> &path_items,
                                          const ObJsonNodeType scalar_type,
                                          ObString &path)
{
  int ret = OB_SUCCESS;
  bool has_escaped_null = false;
  int64_t size = calc_prefix_path_size(path_items, has_escaped_null) + 2; // 2 for type
  if (OB_UNLIKELY(size > SEARCH_INDEX_PATH_LENGTH)) {
    ret = OB_ERR_TOO_LONG_KEY_LENGTH;
    LOG_WARN("path size over limit", K(ret), K(size), K(SEARCH_INDEX_PATH_LENGTH));
    LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, SEARCH_INDEX_PATH_LENGTH);
  } else {
    int64_t pos = 0;
    uint8_t *mem = static_cast<uint8_t*>(allocator.alloc(size));
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(size));
    } else if (OB_FAIL(encode_prefix_path(path_items, has_escaped_null, mem, pos))) {
      LOG_WARN("failed to encode prefix path", K(ret));
    } else {
      uint8_t enc_type = 0;
      if (OB_FAIL(encode_type(scalar_type, enc_type))) {
        LOG_WARN("fail to encode type", K(ret), K(scalar_type));
      } else if (pos + 2 > size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected size", K(ret), K(pos), K(size));
      } else {
        mem[pos++] = ESCAPE_MARKER;
        mem[pos++] = enc_type;
        path.assign_ptr(reinterpret_cast<char*>(mem), pos);
      }
    }
  }
  return ret;
}

int ObSearchIndexPathEncoder::encode_pick_path(const ObItemType pick_type, uint8_t &enc_type)
{
  int ret = OB_SUCCESS;
  switch (pick_type) {
    case T_JSON_NUMBER: {
      enc_type = TYPE_DECIMAL;
      break;
    }
    case T_JSON_STRING: {
      enc_type = TYPE_STRING;
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported pick type", K(ret), K(pick_type));
    }
  }
  return ret;
}

int ObSearchIndexPathEncoder::build_path(ObIAllocator &allocator,
                                         const ObString &prefix,
                                         const ObString &suffix,
                                         ObString &path)
{
  int ret = OB_SUCCESS;
  const int64_t total_size = prefix.length() + suffix.length();
  char *mem = static_cast<char*>(allocator.alloc(total_size));
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for path", K(ret), K(total_size));
  } else {
    int64_t pos = 0;
    if (prefix.length() > 0) {
      MEMCPY(mem + pos, prefix.ptr(), prefix.length());
      pos += prefix.length();
    }
    if (suffix.length() > 0) {
      MEMCPY(mem + pos, suffix.ptr(), suffix.length());
      pos += suffix.length();
    }
    path.assign_ptr(mem, pos);
  }
  return ret;
}

int ObSearchIndexPathEncoder::generate_upper_path(ObIAllocator &allocator,
                                                       const ObString &prefix,
                                                       ObString &path)
{
  char suffix[2] = { static_cast<char>(ESCAPE_MARKER), static_cast<char>(UPPER_BOUND) };
  return build_path(allocator, prefix, ObString(sizeof(suffix), suffix), path);
}

int ObSearchIndexPathEncoder::generate_lower_path(ObIAllocator &allocator,
                                                       const ObString &prefix,
                                                       ObString &path)
{
  char suffix[2] = { static_cast<char>(ESCAPE_MARKER), static_cast<char>(LOWER_BOUND) };
  return build_path(allocator, prefix, ObString(sizeof(suffix), suffix), path);
}

int ObSearchIndexPathEncoder::generate_array_path(ObIAllocator &allocator,
                                                       const ObString &prefix,
                                                       ObString &path)
{
  char suffix[2] = { static_cast<char>(ESCAPE_MARKER), static_cast<char>(TYPE_ARRAY) };
  return build_path(allocator, prefix, ObString(sizeof(suffix), suffix), path);
}

int ObSearchIndexPathEncoder::generate_multi_array_path(ObIAllocator &allocator,
                                                        const ObString &prefix,
                                                        ObString &path)
{
  char suffix[4] = { static_cast<char>(ESCAPE_MARKER), static_cast<char>(TYPE_ARRAY),
                     static_cast<char>(ESCAPE_MARKER), static_cast<char>(TYPE_ARRAY) };
  return build_path(allocator, prefix, ObString(sizeof(suffix), suffix), path);
}

int ObSearchIndexPathEncoder::generate_pick_path(ObIAllocator &allocator,
                                                 const ObString &prefix,
                                                 const ObItemType pick_type,
                                                 ObString &path)
{
  int ret = OB_SUCCESS;
  uint8_t enc_type = 0;
  if (OB_FAIL(encode_pick_path(pick_type, enc_type))) {
    LOG_WARN("failed to encode pick path", K(ret));
  } else if (OB_FAIL(generate_enc_path(allocator, prefix, enc_type, path))) {
    LOG_WARN("failed to generate enc path", K(ret));
  }
  return ret;
}

int ObSearchIndexPathEncoder::generate_enc_path(ObIAllocator &allocator,
                                                const ObString &prefix,
                                                const uint8_t enc_type,
                                                ObString &path)
{
  char suffix[2] = { static_cast<char>(ESCAPE_MARKER), static_cast<char>(enc_type) };
  return build_path(allocator, prefix, ObString(sizeof(suffix), suffix), path);
}

int ObSearchIndexPathEncoder::encode_path(common::ObIAllocator &allocator,
                                          const common::ObString &path_prefix,
                                          const common::ObIJsonBase *j_base,
                                          common::ObString &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json base is null", K(ret));
  } else {
    uint8_t encode_type = 0;
    ObJsonNodeType j_type = j_base->json_type();
    if (OB_FAIL(ObSearchIndexPathEncoder::encode_type(j_type, encode_type))) {
      LOG_WARN("fail to encode type", K(ret), K(j_type));
    } else {
      int64_t buf_len = path_prefix.length() + 2;
      char *buf = static_cast<char *>(allocator.alloc(buf_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("get memory failed", K(ret), K(buf_len));
      } else {
        MEMCPY(buf, path_prefix.ptr(), path_prefix.length());
        buf[path_prefix.length()] = ESCAPE_MARKER;
        buf[path_prefix.length() + 1] = encode_type;
        result.assign_ptr(buf, static_cast<int32_t>(buf_len));
      }
    }
  }
  return ret;
}

int ObSearchIndexPathEncoder::encode_array_path(common::ObIAllocator &allocator,
                                                const common::ObString &path_prefix,
                                                const common::ObIJsonBase *j_base,
                                                common::ObString &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json base is null", K(ret));
  } else {
    // Always use TYPE_ARRAY for array path to match elements in arrays
    uint8_t encode_type;
    int64_t buf_len = path_prefix.length() + 4;
    char *buf = static_cast<char *>(allocator.alloc(buf_len));
    ObJsonNodeType j_type = j_base->json_type();
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get memory failed", K(ret), K(buf_len));
    } else if (OB_FAIL(ObSearchIndexPathEncoder::encode_type(j_type, encode_type))) {
      LOG_WARN("fail to encode type", K(ret), K(j_type));
    } else {
      MEMCPY(buf, path_prefix.ptr(), path_prefix.length());
      buf[path_prefix.length()] = ESCAPE_MARKER;
      buf[path_prefix.length() + 1] = TYPE_ARRAY;
      buf[path_prefix.length() + 2] = ESCAPE_MARKER;
      buf[path_prefix.length() + 3] = encode_type;
      result.assign_ptr(buf, static_cast<int32_t>(buf_len));
    }
  }
  return ret;
}

int ObSearchIndexPathEncoder::encode_multi_array_path(common::ObIAllocator &allocator,
                                                      const common::ObString &path_prefix,
                                                      const common::ObIJsonBase *j_base,
                                                      common::ObString &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json base is null", K(ret));
  } else {
    // Always use TYPE_ARRAY for multi-dimension array path to match multi-dimension array elements
    uint8_t encode_type;
    int64_t buf_len = path_prefix.length() + 6;
    char *buf = static_cast<char *>(allocator.alloc(buf_len));
    ObJsonNodeType j_type = j_base->json_type();
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get memory failed", K(ret), K(buf_len));
    } else if (OB_FAIL(ObSearchIndexPathEncoder::encode_type(j_type, encode_type))) {
      LOG_WARN("fail to encode type", K(ret), K(j_type));
    } else {
      MEMCPY(buf, path_prefix.ptr(), path_prefix.length());
      buf[path_prefix.length()] = ESCAPE_MARKER;
      buf[path_prefix.length() + 1] = TYPE_ARRAY;
      buf[path_prefix.length() + 2] = ESCAPE_MARKER;
      buf[path_prefix.length() + 3] = TYPE_ARRAY;
      buf[path_prefix.length() + 4] = ESCAPE_MARKER;
      buf[path_prefix.length() + 5] = encode_type;
      result.assign_ptr(buf, static_cast<int32_t>(buf_len));
    }
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_value(ObIAllocator &allocator,
                                            const ObDatum &datum,
                                            const common::ObObjMeta &obj_meta,
                                            ObString &value)
{
  int ret = OB_SUCCESS;
  const ObObjType obj_type = obj_meta.get_type();
  switch (obj_type) {
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      ret = encode_int(allocator, datum.get_int(), value);
      break;
    }
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type: {
      ret = encode_uint(allocator, datum.get_uint64(), value);
      break;
    }
    case ObFloatType:
    case ObUFloatType: {
      ret = encode_float(allocator, datum.get_float(), value);
      break;
    }
    case ObDoubleType:
    case ObUDoubleType: {
      ret = encode_double(allocator, datum.get_double(), value);
      break;
    }
    case ObVarcharType: {
      ObEncParam enc_param;
      if (OB_FAIL(init_string_enc_param(obj_type, obj_meta.get_collation_type(), 0, true, true,
                                        enc_param))) {
        LOG_WARN("fail to init string enc param", K(ret));
      } else if (OB_FAIL(encode_string(allocator, datum.get_string(), value, enc_param))) {
        LOG_WARN("fail to encode string", K(ret));
      }
      break;
    }
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      ObEncParam enc_param;
      ObString str_value = datum.get_string();
      bool has_lob_header = obj_meta.has_lob_header();
      sql::ObDatumMeta datum_meta(obj_meta.get_type(), obj_meta.get_collation_type(),
                                  obj_meta.get_scale());
      if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(allocator, datum, datum_meta,
                                                                 has_lob_header, str_value))) {
        LOG_WARN("fail to get real data", K(ret), K(str_value));
      } else if (OB_FAIL(init_string_enc_param(ObVarcharType, obj_meta.get_collation_type(), 0,
                                               true, true, enc_param))) {
        LOG_WARN("fail to init string enc param", K(ret));
      } else if (OB_FAIL(encode_string(allocator, str_value, value, enc_param))) {
        LOG_WARN("fail to encode string", K(ret));
      }
      break;
    }
    case ObDateType:
    case ObMySQLDateType: {
      ret = encode_int32(allocator, datum.get_date(), value);
      break;
    }
    case ObDateTimeType:
    case ObMySQLDateTimeType: {
      ret = encode_int(allocator, datum.get_datetime(), value);
      break;
    }
    case ObTimeType: {
      ret = encode_int(allocator, datum.get_time(), value);
      break;
    }
    case ObTimestampType: {
      ret = encode_int(allocator, datum.get_timestamp(), value);
      break;
    }
    case ObYearType: {
      ret = encode_uint8(allocator, datum.get_year(), value);
      break;
    }
    case ObNumberType:
    case ObUNumberType: {
      ret = encode_number(allocator, datum.get_number(), value);
      break;
    }
    case ObDecimalIntType: {
      ret = encode_decimal_int(allocator, datum.get_decimal_int(), datum.get_int_bytes(), value);
      break;
    }
    case ObJsonType: {
      ObString j_bin_str = datum.get_string();
      ObIJsonBase *j_base = nullptr;
      bool has_lob_header = obj_meta.has_lob_header();
      sql::ObDatumMeta datum_meta(obj_meta.get_type(), obj_meta.get_collation_type(),
                                  obj_meta.get_scale());
      if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(allocator, datum, datum_meta,
                                                                 has_lob_header, j_bin_str))) {
        LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
      } else if (OB_FAIL(common::ObJsonBaseFactory::get_json_base(&allocator,
                                                                  j_bin_str,
                                                                  ObJsonInType::JSON_BIN,
                                                                  ObJsonInType::JSON_BIN,
                                                                  j_base))) {
        LOG_WARN("fail to get json base", K(ret));
      } else {
        ObEncParam enc_param;
        int64_t type = -1;
        if (OB_FAIL(init_json_string_enc_param(enc_param))) {
          LOG_WARN("fail to init json string enc param", K(ret));
        } else if (OB_FAIL(encode_json_scalar(allocator, j_base, value, enc_param))) {
          LOG_WARN("fail to encode json scalar", K(ret));
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported object type", K(ret), K(obj_type));
    }
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_json_scalar(ObIAllocator &allocator,
                                                  const ObIJsonBase *j_base,
                                                  ObString &value,
                                                  ObEncParam &enc_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(j_base)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json base is null", K(ret));
  } else if (OB_UNLIKELY(!is_json_scalar(j_base))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json base is not a scalar", K(ret));
  } else {
    const ObJsonNodeType node_type = j_base->json_type();
    switch (node_type) {
      case ObJsonNodeType::J_NULL: {
        if (!j_base->is_real_json_null(j_base)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("json base is not a real json null in mysql mode", K(ret));
        } else {
          uint8_t null_val = 0;
          if (OB_FAIL(encode_uint8(allocator, null_val, value))) {
            LOG_WARN("fail to encode null", K(ret), K(null_val));
          }
        }
        break;
      }
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_DOUBLE: {
        ObNumber number;
        if (OB_FAIL(j_base->to_number(&allocator, number))) {
          LOG_WARN("fail to convert json base to number", K(ret));
        } else if (OB_FAIL(encode_number(allocator, number, value))) {
          LOG_WARN("fail to encode number", K(ret));
        }
        break;
      }
      case ObJsonNodeType::J_STRING: {
        ObString string = ObString(j_base->get_data_length(), j_base->get_data());
        if (OB_FAIL(encode_string(allocator, string, value, enc_param))) {
          LOG_WARN("fail to encode string", K(ret));
        }
        break;
      }
      case ObJsonNodeType::J_BOOLEAN: {
        uint8_t bool_val = j_base->get_boolean() ? 1 : 0;
        if (OB_FAIL(encode_uint8(allocator, bool_val, value))) {
          LOG_WARN("fail to encode boolean", K(ret), K(bool_val));
        }
        break;
      }
      case ObJsonNodeType::J_DATE: {
        ObTime ob_time;
        if (OB_FAIL(j_base->get_obtime(ob_time))) {
          LOG_WARN("fail to get obtime from json", K(ret));
        } else {
          int32_t date_val = ObTimeConverter::ob_time_to_date(ob_time);
          if (OB_FAIL(encode_int32(allocator, date_val, value))) {
            LOG_WARN("fail to encode date", K(ret), K(date_val));
          }
        }
        break;
      }
      case ObJsonNodeType::J_TIME: {
        ObTime ob_time;
        if (OB_FAIL(j_base->get_obtime(ob_time))) {
          LOG_WARN("fail to get obtime from json", K(ret));
        } else {
          int64_t time_val = ObTimeConverter::ob_time_to_time(ob_time);
          if (OB_FAIL(encode_int(allocator, time_val, value))) {
            LOG_WARN("fail to encode time", K(ret), K(time_val));
          }
        }
        break;
      }
      case ObJsonNodeType::J_DATETIME:
      case ObJsonNodeType::J_TIMESTAMP: {
        ObTime ob_time;
        ObTimeConvertCtx cvrt_ctx(NULL, false);
        int64_t datetime_val = 0;
        if (OB_FAIL(j_base->get_obtime(ob_time))) {
          LOG_WARN("fail to get obtime from json", K(ret));
        } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, datetime_val))) {
          LOG_WARN("fail to convert obtime to datetime", K(ret));
        } else if (OB_FAIL(encode_int(allocator, datetime_val, value))) {
          LOG_WARN("fail to encode datetime", K(ret), K(datetime_val));
        }
        break;
      }
      case ObJsonNodeType::J_OPAQUE: {
        ObString opaque_data = ObString(j_base->get_data_length(), j_base->get_data());
        if (OB_FAIL(encode_string(allocator, opaque_data, value, enc_param))) {
          LOG_WARN("fail to encode opaque", K(ret));
        }
        break;
      }
      case ObJsonNodeType::J_MYSQL_DATE: {
        ObTime ob_time;
        if (OB_FAIL(j_base->get_obtime(ob_time))) {
          LOG_WARN("fail to get obtime from json", K(ret));
        } else {
          int32_t date_val = ObTimeConverter::ob_time_to_mdate(ob_time).date_;
          if (OB_FAIL(encode_int32(allocator, date_val, value))) {
            LOG_WARN("fail to encode mysql date", K(ret), K(date_val));
          }
        }
        break;
      }
      case ObJsonNodeType::J_MYSQL_DATETIME: {
        ObTime ob_time;
        ObMySQLDateTime datetime_val = 0;
        if (OB_FAIL(j_base->get_obtime(ob_time))) {
          LOG_WARN("fail to get obtime from json", K(ret));
        } else if (OB_FAIL(ObTimeConverter::ob_time_to_mdatetime(ob_time, datetime_val))) {
          LOG_WARN("fail to convert obtime to datetime", K(ret));
        } else if (OB_FAIL(encode_int(allocator, datetime_val.datetime_, value))) {
          LOG_WARN("fail to encode mysql datetime", K(ret), K(datetime_val));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported json node type", K(ret), K(node_type));
      }
    }
  }
  return ret;
}

int ObSearchIndexValueEncoder::init_json_string_enc_param(ObEncParam &enc_param)
{
  int ret = OB_SUCCESS;
  enc_param.type_ = ObVarcharType;
  enc_param.cs_type_ = CS_TYPE_UTF8MB4_BIN;
  enc_param.is_var_len_ = true;
  enc_param.is_memcmp_ = true;
  enc_param.is_nullable_ = false;
  return ret;
}

int ObSearchIndexValueEncoder::init_string_enc_param(const ObObjType obj_type,
                                                     const ObCollationType cs_type,
                                                     const ObSQLMode sql_mode,
                                                     const bool is_null_first,
                                                     const bool is_asc,
                                                     ObEncParam &enc_param)
{
  int ret = OB_SUCCESS;
  enc_param.type_ = obj_type;
  enc_param.cs_type_ = cs_type;
  enc_param.is_var_len_ = !lib::is_oracle_mode() && is_pad_char_to_full_length(sql_mode);
  enc_param.is_memcmp_ = lib::is_oracle_mode();
  enc_param.is_nullable_ = true;
  enc_param.is_null_first_ = is_null_first;
  enc_param.is_asc_ = is_asc;
  return ret;
}

template<typename T>
int ObSearchIndexValueEncoder::encode_int(ObIAllocator &allocator, T int_value, ObString &value)
{
  int ret = OB_SUCCESS;
  int64_t val = int_value;
  unsigned char *mem = static_cast<unsigned char*>(allocator.alloc(sizeof(int64_t)));
  int64_t to_len = 0;
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(ObOrderPerservingEncoder::encode_from_int(val, mem, to_len))) {
    LOG_WARN("failed to encode from int", K(ret));
  } else {
    value.assign_ptr(reinterpret_cast<char*>(mem), to_len);
  }
  return ret;
}

template<typename T>
int ObSearchIndexValueEncoder::encode_uint(ObIAllocator &allocator, T uint_value, ObString &value)
{
  int ret = OB_SUCCESS;
  uint64_t val = uint_value;
  unsigned char *mem = static_cast<unsigned char*>(allocator.alloc(sizeof(uint64_t)));
  int64_t to_len = 0;
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(ObOrderPerservingEncoder::encode_from_uint(val, mem, to_len))) {
    LOG_WARN("failed to encode from uint", K(ret));
  } else {
    value.assign_ptr(reinterpret_cast<char*>(mem), to_len);
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_double(ObIAllocator &allocator, double double_value,
                                             ObString &value)
{
  int ret = OB_SUCCESS;
  unsigned char *mem = static_cast<unsigned char*>(allocator.alloc(sizeof(double)));
  int64_t to_len = 0;
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(ObOrderPerservingEncoder::encode_from_double(double_value, mem, to_len))) {
    LOG_WARN("failed to encode from double", K(ret));
  } else {
    value.assign_ptr(reinterpret_cast<char*>(mem), to_len);
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_float(ObIAllocator &allocator, float float_value,
                                            ObString &value)
{
  int ret = OB_SUCCESS;
  unsigned char *mem = static_cast<unsigned char*>(allocator.alloc(sizeof(float)));
  int64_t to_len = 0;
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(ObOrderPerservingEncoder::encode_from_float(float_value, mem, to_len))) {
    LOG_WARN("failed to encode from float", K(ret));
  } else {
    value.assign_ptr(reinterpret_cast<char*>(mem), to_len);
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_number(ObIAllocator &allocator, const ObNumber &number,
                                             ObString &value)
{
  int ret = OB_SUCCESS;
  ObNumberDesc desc = number.d_;
  int64_t max_buf_len = sizeof(int8_t) + desc.len_ * sizeof(uint32_t) + 2 * sizeof(int32_t);
  unsigned char *mem = static_cast<unsigned char*>(allocator.alloc(max_buf_len));
  int64_t to_len = 0;
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(ObOrderPerservingEncoder::encode_from_number(number, mem, max_buf_len, to_len))) {
    LOG_WARN("failed to encode from number", K(ret));
  } else {
    value.assign_ptr(reinterpret_cast<char*>(mem), to_len);
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_decimal_int(ObIAllocator &allocator,
                                                  const ObDecimalInt *decint,
                                                  const int32_t int_bytes,
                                                  ObString &value)
{
  int ret = OB_SUCCESS;
  unsigned char *mem = static_cast<unsigned char*>(allocator.alloc(int_bytes));
  int64_t to_len = 0;
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(ObOrderPerservingEncoder::encode_from_decint(decint, int_bytes, mem, to_len))) {
    LOG_WARN("failed to encode decimal int", K(ret), K(int_bytes));
  } else {
    value.assign_ptr(reinterpret_cast<char*>(mem), to_len);
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_int32(ObIAllocator &allocator, int32_t int32_value,
                                            ObString &value)
{
  int ret = OB_SUCCESS;
  unsigned char *mem = static_cast<unsigned char*>(allocator.alloc(sizeof(int32_t)));
  int64_t to_len = 0;
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(ObOrderPerservingEncoder::encode_from_int32(int32_value, mem, to_len))) {
    LOG_WARN("failed to encode from int32", K(ret));
  } else {
    value.assign_ptr(reinterpret_cast<char*>(mem), to_len);
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_uint8(ObIAllocator &allocator, uint8_t uint8_value,
                                            ObString &value)
{
  int ret = OB_SUCCESS;
  unsigned char *mem = static_cast<unsigned char*>(allocator.alloc(sizeof(uint8_t)));
  int64_t to_len = 0;
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(ObOrderPerservingEncoder::encode_from_uint8(uint8_value, mem, to_len))) {
    LOG_WARN("failed to encode from uint8", K(ret));
  } else {
    value.assign_ptr(reinterpret_cast<char*>(mem), to_len);
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_string(ObIAllocator &allocator,
                                             const ObString &string_value,
                                             ObString &value,
                                             ObEncParam &enc_param)
{
  int ret = OB_SUCCESS;
  int64_t safty_buf_size = 20;
  // TODO: opt max buf len and limit max value length
  int64_t max_buf_len = 7 * string_value.length() + safty_buf_size;
  unsigned char *mem = static_cast<unsigned char*>(allocator.alloc(max_buf_len));
  int64_t to_len = 0;
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(ObOrderPerservingEncoder::encode_from_string_varlen(string_value, mem,
      max_buf_len, to_len, enc_param))) {
    LOG_WARN("failed to encode from string varlen", K(ret));
  } else {
    // truncate encoded value length to SEARCH_INDEX_VALUE_LENGTH
    to_len = MIN(to_len, SEARCH_INDEX_VALUE_LENGTH);
    value.assign_ptr(reinterpret_cast<char*>(mem), to_len);
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_value(common::ObIAllocator &allocator,
                                            const ObObj &obj,
                                            ObString &result)
{
  int ret = OB_SUCCESS;
  ObDatum datum;
  char elem_datum_buf[common::OBJ_DATUM_MAX_RES_SIZE] = {0};
  datum.ptr_ = elem_datum_buf;
  if (OB_FAIL(datum.from_obj(obj))) {
    LOG_WARN("fail to convert obj to datum", K(ret), K(obj));
  } else if (OB_FAIL(encode_value(allocator, datum, obj.get_meta(), result))) {
    LOG_WARN("fail to encode value", K(ret));
  }
  return ret;
}

int ObSearchIndexValueEncoder::encode_value(common::ObIAllocator &allocator,
                                            const common::ObIJsonBase *j_base,
                                            common::ObString &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  ObEncParam enc_param;
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json base is null", K(ret));
  } else if (!is_json_scalar(j_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json base is not a scalar", K(ret), K(j_base->json_type()));
  } else if (OB_FAIL(init_json_string_enc_param(enc_param))) {
    LOG_WARN("fail to init json string enc param", K(ret));
  } else if (OB_FAIL(encode_json_scalar(allocator, j_base, result, enc_param))) {
    LOG_WARN("fail to encode json scalar", K(ret));
  }
  return ret;
}

bool ObSearchIndexValueEncoder::string_safety_to_compare(const ObString &str)
{
  // TODO: optimize this by collation type.
  // value encoded length worst case is 7x expansion + 20 bytes safety buffer
  return str.length() < (SEARCH_INDEX_VALUE_LENGTH - 20) / 7;
}

bool ObSearchIndexValueEncoder::string_column_may_truncate(const ObObjMeta &column_meta,
                                                           const ObLength column_length)
{
  bool truncated = false;
  ObObjType obj_type = column_meta.get_type();
  if (obj_type == ObVarcharType) {
    // TODO: optimize this by collation type.
    // value encoded length worst case is 7x expansion + 20 bytes safety buffer
    truncated = column_length > (SEARCH_INDEX_VALUE_LENGTH - 20) / 7;
  } else if (ob_is_text_tc(obj_type)) {
    truncated = true;
  }
  return truncated;
}

} // namespace share
} // namespace oceanbase
