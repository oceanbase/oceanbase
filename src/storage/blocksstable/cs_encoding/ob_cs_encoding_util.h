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

#ifndef OCEANBASE_ENCODING_OB_CS_ENCODING_UTIL_H_
#define OCEANBASE_ENCODING_OB_CS_ENCODING_UTIL_H_

#include "ob_stream_encoding_struct.h"
#include "storage/blocksstable/encoding/ob_encoding_hash_util.h"
#include "storage/blocksstable/encoding/ob_encoding_util.h"
#include "ob_cs_micro_block_transformer.h"
#include "ob_icolumn_cs_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
class ObCSEncodingUtil
{
public:
  // if row count less than this value, use raw encoding
  static const int64_t ENCODING_ROW_COUNT_THRESHOLD;
  static const int64_t MAX_MICRO_BLOCK_ROW_CNT;
  static const int64_t DEFAULT_DATA_BUFFER_SIZE;
  static const int64_t MAX_BLOCK_ENCODING_STORE_SIZE;
  static const int64_t MAX_COLUMN_ENCODING_STORE_SIZE;

  static int64_t get_bit_size(const uint64_t v);
  static OB_INLINE int64_t get_bitmap_byte_size(const int64_t bit_cnt)
  {
    return (bit_cnt + CHAR_BIT - 1) / CHAR_BIT;
  }
  static OB_INLINE bool is_variable_len_store_class(const ObObjTypeStoreClass sc)
  {
    // ObOTimestampSC and ObIntervalSC are fixed len string
    return ObNumberSC == sc || ObStringSC == sc || ObTextSC == sc ||
        ObLobSC == sc || ObJsonSC == sc || ObGeometrySC == sc || ObRoaringBitmapSC == sc;
  }
  static OB_INLINE bool is_integer_store_class(const ObObjTypeStoreClass sc)
  {
    return ObIntSC == sc || ObUIntSC == sc;
  }
  static OB_INLINE bool is_string_store_class(const ObObjTypeStoreClass sc)
  {
    return ObNumberSC == sc || ObStringSC == sc || ObTextSC == sc ||
        sc == ObLobSC || ObJsonSC == sc || ObGeometrySC == sc ||
        ObOTimestampSC == sc ||  ObIntervalSC == sc || ObRoaringBitmapSC == sc;
  }

  // the sql layer does not allow to modify ptr of datums of these store class,
  // so it need copy when decoding these types even if they use string encoding.
  static OB_INLINE bool is_store_class_need_copy(const ObObjTypeStoreClass sc)
  {
    return ObNumberSC == sc || ObOTimestampSC == sc || ObIntervalSC == sc || ObDecimalIntSC == sc;
  }
  static OB_INLINE bool is_no_need_sort_lob(const ObObjTypeStoreClass sc)
  {
    // ObTextSC type sorting can speeds up filter, ObLobSC/ObJsonSC/ObGeometrySC sorting is meaningless
    return sc == ObLobSC || sc == ObJsonSC || sc == ObGeometrySC || ObRoaringBitmapSC == sc;
  }

  static int build_cs_column_encoding_ctx(ObEncodingHashTable *ht,
    const ObObjTypeStoreClass store_class, const int64_t type_store_size, ObColumnCSEncodingCtx &ctx);

};


}  // end namespace blocksstable
}  // end namespace oceanbase

#endif
