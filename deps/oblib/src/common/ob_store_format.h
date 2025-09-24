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

#ifndef OCEANBASE_COMMON_OB_STORE_FORMAT_H_
#define OCEANBASE_COMMON_OB_STORE_FORMAT_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{

enum ObRowStoreType : uint8_t
{
  FLAT_ROW_STORE = 0,
  ENCODING_ROW_STORE = 1,
  SELECTIVE_ENCODING_ROW_STORE = 2,
  CS_ENCODING_ROW_STORE = 3,
  FLAT_OPT_ROW_STORE = 4,
  MAX_ROW_STORE,
  DUMMY_ROW_STORE = UINT8_MAX, // invalid dummy row store type for compatibility
};

enum ObStoreFormatType
{
  OB_STORE_FORMAT_INVALID  = 0,
  OB_STORE_FORMAT_REDUNDANT_MYSQL = 1,
  OB_STORE_FORMAT_COMPACT_MYSQL = 2,
  OB_STORE_FORMAT_DYNAMIC_MYSQL = 3,
  OB_STORE_FORMAT_COMPRESSED_MYSQL = 4,
  OB_STORE_FORMAT_CONDENSED_MYSQL = 5,
  OB_STORE_FORMAT_MAX_MYSQL,
  // 5- 10 reserved for mysql store mode furture
  OB_STORE_FORMAT_NOCOMPRESS_ORACLE = 11,
  OB_STORE_FORMAT_BASIC_ORACLE = 12,
  OB_STORE_FORMAT_OLTP_ORACLE = 13,
  OB_STORE_FORMAT_QUERY_ORACLE = 14,
  OB_STORE_FORMAT_ARCHIVE_ORACLE = 15,
  OB_STORE_FORMAT_QUERY_LOW_ORACLE = 16,
  OB_STORE_FORMAT_ARCHIVE_HIGH_ORACLE = 17,
  OB_STORE_FORMAT_MAX
};

enum ObTableStoreType : uint8_t
{
  OB_TABLE_STORE_INVALID = 0,
  OB_TABLE_STORE_ROW = 1,
  OB_TABLE_STORE_COLUMN = 2,
  OB_TABLE_STORE_ROW_WITH_COLUMN= 3,
  OB_TABLE_STORE_MAX
};

enum class ObMergeEngineType : uint8_t
{
  OB_MERGE_ENGINE_PARTIAL_UPDATE = 0,
  OB_MERGE_ENGINE_DELETE_INSERT = 1,
  OB_MERGE_ENGINE_INSERT_ONLY = 2,
  OB_MERGE_ENGINE_MAX
};

enum ObSkipIndexLevel : uint8_t
{
  OB_SKIP_INDEX_LEVEL_BASE_ONLY = 0,
  OB_SKIP_INDEX_LEVEL_BASE_AND_DELTA_SSTABLE = 1,
  OB_SKIP_INDEX_LEVEL_MAX
};

struct ObStoreFormatItem
{
  const char* format_name_;
  const char* format_print_str_;
  const char* format_compress_name_;
  const ObRowStoreType row_store_type_;
};

class ObStoreFormat{
public:
  static const ObStoreFormatType STORE_FORMAT_MYSQL_START = OB_STORE_FORMAT_REDUNDANT_MYSQL;
  static const ObStoreFormatType STORE_FORMAT_MYSQL_DEFAULT = OB_STORE_FORMAT_DYNAMIC_MYSQL;
  static const ObStoreFormatType STORE_FORMAT_ORACLE_START = OB_STORE_FORMAT_NOCOMPRESS_ORACLE;
  static const ObStoreFormatType STORE_FORMAT_ORACLE_DEFAULT = OB_STORE_FORMAT_ARCHIVE_ORACLE;
private:
  ObStoreFormat() {};
  virtual ~ObStoreFormat() {};
public:
  static inline bool is_row_store_type_valid(const ObRowStoreType type)
  {
    return type >= FLAT_ROW_STORE && type < MAX_ROW_STORE;
  }
  static inline const char *get_row_store_name(const ObRowStoreType type)
  {
    return is_row_store_type_valid(type) ? row_store_name[type] : NULL;
  }
  static inline ObRowStoreType get_default_row_store_type(const bool is_major = true)
  {
    return is_major ? ENCODING_ROW_STORE : FLAT_ROW_STORE;
  }
  static int find_row_store_type(const ObString &row_store, ObRowStoreType &row_store_type);
  static inline bool is_store_format_mysql(const ObStoreFormatType store_format)
  {
    return store_format >= STORE_FORMAT_MYSQL_START && store_format < OB_STORE_FORMAT_MAX_MYSQL;
  }
  static inline bool is_store_format_oracle(const ObStoreFormatType store_format)
  {
    return store_format >= STORE_FORMAT_ORACLE_START && store_format < OB_STORE_FORMAT_MAX;
  }
  static inline bool is_store_format_valid(const ObStoreFormatType store_format)
  {
    return is_store_format_mysql(store_format) || is_store_format_oracle(store_format);
  }
  static inline bool is_store_format_valid(const ObStoreFormatType store_format, bool is_oracle_mode)
  {
    return is_oracle_mode ? is_store_format_oracle(store_format) : is_store_format_mysql(store_format);
  }
  static inline const char* get_store_format_name(const ObStoreFormatType store_format)
  {
    return is_store_format_valid(store_format) ? store_format_items[store_format].format_name_ : NULL;
  }
  static inline const char* get_store_format_print_str(const ObStoreFormatType store_format)
  {
    return is_store_format_valid(store_format) ? store_format_items[store_format].format_print_str_ : NULL;
  }
  static inline const char* get_store_format_compress_name(const ObStoreFormatType store_format)
  {
    return is_store_format_valid(store_format) ? store_format_items[store_format].format_compress_name_: NULL;
  }
  static inline ObRowStoreType get_row_store_type(const ObStoreFormatType store_format)
  {
    return is_store_format_valid(store_format) ? store_format_items[store_format].row_store_type_: MAX_ROW_STORE;
  }
  static inline bool is_row_store_type_with_flat(const ObRowStoreType type)
  {
    return FLAT_ROW_STORE == type || FLAT_OPT_ROW_STORE == type;
  }
  static inline bool is_row_store_type_with_encoding(const ObRowStoreType type)
  {
    return ENCODING_ROW_STORE == type || SELECTIVE_ENCODING_ROW_STORE == type || CS_ENCODING_ROW_STORE == type;
  }
  static inline bool is_row_store_type_with_pax_encoding(const ObRowStoreType type)
  {
    return ENCODING_ROW_STORE == type || SELECTIVE_ENCODING_ROW_STORE == type;
  }
  static inline bool is_row_store_type_with_cs_encoding(const ObRowStoreType type)
  {
    return CS_ENCODING_ROW_STORE == type;
  }

  static int find_store_format_type(const ObString &store_format,
                                    const ObStoreFormatType start,
                                    const ObStoreFormatType end,
                                    ObStoreFormatType &store_format_type);
  static int find_store_format_type_mysql(const ObString &store_format, ObStoreFormatType &store_format_type);
  static int find_store_format_type_oracle(const ObString &store_format, ObStoreFormatType &store_format_type);
  static int find_store_format_type(const ObString &store_format, ObStoreFormatType &store_format_type);
  static int find_store_format_type(const ObString &store_format,
                                    const bool is_oracle_mode,
                                    ObStoreFormatType &store_format_type);
private:
  static const ObStoreFormatItem store_format_items[OB_STORE_FORMAT_MAX];
  static const char *row_store_name[MAX_ROW_STORE];
};

class ObTableStoreFormat {
public:
  static inline bool is_row_store(const ObTableStoreType type)
  {
    return OB_TABLE_STORE_ROW == type;
  }
  static inline bool is_column_store(const ObTableStoreType type)
  {
    return OB_TABLE_STORE_COLUMN == type;
  }
  static inline bool is_row_with_column_store(const ObTableStoreType type)
  {
    return OB_TABLE_STORE_ROW_WITH_COLUMN == type;
  }
  static inline bool is_with_column (const ObTableStoreType type)
  {
    return type > OB_TABLE_STORE_ROW && type < OB_TABLE_STORE_MAX;
  }
  static int find_table_store_type(const ObString &store_format, ObTableStoreType &table_store_type);
};

// store type of sstable of LS replica
enum ObLSStoreType : uint8_t
{
  OB_LS_STORE_NORMAL = 1,
  OB_LS_STORE_COLUMN_ONLY = 2,
  OB_LS_STORE_MAX
};

// this class is used to describe the format of sstable of LS replica
class ObLSStoreFormat
{
  OB_UNIS_VERSION(1);
public:
  ObLSStoreFormat() { reset(); }
  ObLSStoreFormat(const ObLSStoreType &store_type) : store_type_(store_type) {};
  ObLSStoreFormat(const ObLSStoreFormat &other) { store_type_ = other.store_type_; }
  ObLSStoreFormat &operator=(const ObLSStoreFormat &rhs);
  void reset() { store_type_ = OB_LS_STORE_NORMAL; } // default type is NORMAL
  void set(ObLSStoreType store_type) { store_type_ = store_type; }
  bool is_valid() const;
  OB_INLINE bool is_columnstore() const { return OB_LS_STORE_COLUMN_ONLY == store_type_; }
  const char *to_str() const;
  TO_STRING_KV(K_(store_type), "store_type_str", to_str());
private:
  ObLSStoreType store_type_;
};

static const char *MergeEngineTypeStr[] = { "PARTIAL_UPDATE",
                                            "DELETE_INSERT",
                                            "MAX" };
class ObMergeEngineStoreFormat
{
public:
  static inline bool is_merge_engine_valid(const ObMergeEngineType type)
  {
    return type >= ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE && type < ObMergeEngineType::OB_MERGE_ENGINE_MAX;
  }
  static inline const char *get_merge_engine_type_name(const ObMergeEngineType merge_engine_type)
  {
    const int64_t merge_engine_type_idx = static_cast<int64_t>(merge_engine_type);
    const char *str = "INVALID";
    switch (merge_engine_type) {
      case ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE: {
        str = "PARTIAL_UPDATE";
        break;
      }
      case ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT: {
        str = "DELETE_INSERT";
        break;
      }
      case ObMergeEngineType::OB_MERGE_ENGINE_INSERT_ONLY: {
        str = "INSERT_ONLY";
        break;
      }
      case ObMergeEngineType::OB_MERGE_ENGINE_MAX:
      default: {
        str = "INVALID";
        break;
      }
    }
    return str;
  }
};

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_STORE_FORMAT_H_
