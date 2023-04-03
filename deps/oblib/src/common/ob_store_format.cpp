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

#define USING_LOG_PREFIX COMMON

#include "common/ob_store_format.h"

namespace oceanbase
{
namespace common
{

const char *ObStoreFormat::row_store_name[MAX_ROW_STORE] =
{
  "flat_row_store",
  "encoding_row_store",
  "selective_encoding_row_store",
};

const ObStoreFormatItem ObStoreFormat::store_format_items[OB_STORE_FORMAT_MAX] =
{
  {"", "", "", ENCODING_ROW_STORE},   //OB_STORE_FORMAT_INVALID
  // mysql mode
  {"REDUNDANT", "ROW_FORMAT = REDUNDANT", "", FLAT_ROW_STORE},
  {"COMPACT", "ROW_FORMAT = COMPACT", "", FLAT_ROW_STORE},
  {"DYNAMIC", "ROW_FORMAT = DYNAMIC", "", ENCODING_ROW_STORE},
  {"COMPRESSED", "ROW_FORMAT = COMPRESSED", "", ENCODING_ROW_STORE},
  {"CONDENSED", "ROW_FORMAT = CONDENSED", "", SELECTIVE_ENCODING_ROW_STORE},
  {"", "", "", MAX_ROW_STORE},   //reserved for mysql furture
  {"", "", "", MAX_ROW_STORE},   //reserved for mysql furture
  {"", "", "", MAX_ROW_STORE},   //reserved for mysql furture
  {"", "", "", MAX_ROW_STORE},   //reserved for mysql furture
  {"", "", "", MAX_ROW_STORE},   //reserved for mysql furture
  //oracle mode
  {"NOCOMPRESS", "NOCOMPRESS", "none", FLAT_ROW_STORE},
  {"BASIC", "COMPRESS BASIC", "lz4_1.0", FLAT_ROW_STORE},
  {"OLTP", "COMPRESS FOR OLTP", "zstd_1.3.8", FLAT_ROW_STORE},
  {"QUERY", "COMPRESS FOR QUERY", "lz4_1.0", ENCODING_ROW_STORE},
  {"ARCHIVE", "COMPRESS FOR ARCHIVE", "zstd_1.3.8", ENCODING_ROW_STORE},
  {"QUERY LOW", "COMPRESS FOR QUERY LOW", "lz4_1.0", SELECTIVE_ENCODING_ROW_STORE},
};

int ObStoreFormat::find_row_store_type(const ObString &row_store, ObRowStoreType &row_store_type)
{
  int ret = OB_SUCCESS;

  if (row_store.empty()) {
    LOG_WARN("Replace empty rowstore with default row store type", K(row_store_type), K(row_store), K(ret));
    row_store_type = get_default_row_store_type();
  } else {
    row_store_type = MAX_ROW_STORE;
    for (int64_t i = FLAT_ROW_STORE; i < MAX_ROW_STORE && !is_row_store_type_valid(row_store_type); i++) {
      if (0 == row_store.case_compare(get_row_store_name(static_cast<ObRowStoreType> (i)))) {
        row_store_type = static_cast<ObRowStoreType> (i);
      }
    }
    if (!is_row_store_type_valid(row_store_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected row store type", K(row_store_type), K(row_store), K(ret));
    }
  }

  return ret;
}

int ObStoreFormat::find_store_format_type(const ObString &store_format,
                                          const ObStoreFormatType start,
                                          const ObStoreFormatType end,
                                          ObStoreFormatType &store_format_type)
{
  int ret = OB_SUCCESS;

  store_format_type = OB_STORE_FORMAT_INVALID;
  if (store_format.empty()) {
    LOG_WARN("Empty store format str keep invalid type", K(store_format), K(store_format_type), K(ret));
  } else if (!(OB_STORE_FORMAT_INVALID < start && start <= OB_STORE_FORMAT_MAX)
              || !(OB_STORE_FORMAT_INVALID < end && end <= OB_STORE_FORMAT_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected store format type", K(start), K(end), K(ret));
  } else {
    for (int64_t i = start; i < end && !is_store_format_valid(store_format_type); i++) {
      if (0 == store_format.case_compare(get_store_format_name(static_cast<ObStoreFormatType> (i)))) {
        store_format_type = static_cast<ObStoreFormatType> (i);
      }
    }
    if (!is_store_format_valid(store_format_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected store format type", K(store_format), K(store_format_type), K(ret));
    }
  }

  return ret;
}
int ObStoreFormat::find_store_format_type(const ObString &store_format, ObStoreFormatType &store_format_type)
{
  return find_store_format_type(store_format, STORE_FORMAT_MYSQL_START, OB_STORE_FORMAT_MAX, store_format_type);
}
int ObStoreFormat::find_store_format_type_mysql(const ObString &store_format, ObStoreFormatType &store_format_type)
{
  return find_store_format_type(store_format, STORE_FORMAT_MYSQL_START, OB_STORE_FORMAT_MAX_MYSQL, store_format_type);
}
int ObStoreFormat::find_store_format_type_oracle(const ObString &store_format, ObStoreFormatType &store_format_type)
{
  return find_store_format_type(store_format, STORE_FORMAT_ORACLE_START, OB_STORE_FORMAT_MAX, store_format_type);
}
int ObStoreFormat::find_store_format_type(const ObString &store_format, const bool is_oracle_mode, ObStoreFormatType &store_format_type)
{
  return is_oracle_mode ? find_store_format_type_oracle(store_format, store_format_type) : find_store_format_type_mysql(store_format, store_format_type);
}
}//end namespace common
}//end namespace oceanbase

