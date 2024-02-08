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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_KEY_VEC_OP_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_KEY_VEC_OP_H_

#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase
{
namespace sql
{
// sort key with addon fields
template <bool has_addon>
class ObSortKeyStore : public ObCompactRow
{
public:
  ObSortKeyStore() : ObCompactRow()
  {}
  ~ObSortKeyStore()
  {}
  ObSortKeyStore *get_addon_ptr(const RowMeta &row_meta)
  {
    return reinterpret_cast<ObSortKeyStore *>(
      *reinterpret_cast<int64_t *>(this->get_extra_payload(row_meta)));
  }
  const ObSortKeyStore *get_addon_ptr(const RowMeta &row_meta) const
  {
    return reinterpret_cast<ObSortKeyStore *>(
      *reinterpret_cast<int64_t *>(this->get_extra_payload(row_meta)));
  }
  ObSortKeyStore *get_addon_ptr(const int32_t extra_offset)
  {
    return reinterpret_cast<ObSortKeyStore *>(
      *reinterpret_cast<int64_t *>(this->get_extra_payload(extra_offset)));
  }
  const ObSortKeyStore *get_addon_ptr(const int32_t extra_offset) const
  {
    return reinterpret_cast<ObSortKeyStore *>(
      *reinterpret_cast<int64_t *>(this->get_extra_payload(extra_offset)));
  }
  void set_addon_ptr(const ObSortKeyStore *addon_ptr, const RowMeta &row_meta)
  {
    *reinterpret_cast<int64_t *>(this->get_extra_payload(row_meta)) =
      reinterpret_cast<int64_t>(addon_ptr);
  }
  void set_max_size(const uint64_t max_size, const RowMeta &row_meta)
  {
    UNUSEDx(max_size, row_meta);
  }
  uint64_t get_max_size(const RowMeta &row_meta) const
  {
    UNUSED(row_meta);
    return 0;
  }
  static int64_t get_extra_size(bool is_sort_key)
  {
    if (is_sort_key) {
      return has_addon ? sizeof(ObSortKeyStore *) : 0;
    } else {
      return 0;
    }
  }
  TO_STRING_KV(KP(this));
};

// Optimize mem usage/performance of top-n sort:
//
// Record buf_len of each allocated row. When old row pop-ed out of the heap
// and has enough space for new row, use the space of old row to store new row
// instead of allocating space for new row.
// Note that this is not perfect solution, it cannot handle the case that row
// size keeps going up. However, this can cover most cases.
template <bool has_addon>
struct ObTopNSortKey : public ObCompactRow
{
  ObTopNSortKey *get_addon_ptr(const RowMeta &row_meta)
  {
    return reinterpret_cast<ObTopNSortKey *>(*reinterpret_cast<int64_t *>(
      (static_cast<char *>(this->get_extra_payload(row_meta)) + sizeof(uint64_t))));
  }
  const ObTopNSortKey *get_addon_ptr(const RowMeta &row_meta) const
  {
    return reinterpret_cast<ObTopNSortKey *>(*reinterpret_cast<int64_t *>(
      (static_cast<char *>(this->get_extra_payload(row_meta)) + sizeof(uint64_t))));
  }
  ObTopNSortKey *get_addon_ptr(const int32_t extra_offset)
  {
    return reinterpret_cast<ObTopNSortKey *>(*reinterpret_cast<int64_t *>(
      (static_cast<char *>(this->get_extra_payload(extra_offset)) + sizeof(uint64_t))));
  }
  const ObTopNSortKey *get_addon_ptr(const int32_t extra_offset) const
  {
    return reinterpret_cast<ObTopNSortKey *>(*reinterpret_cast<int64_t *>(
      (static_cast<char *>(this->get_extra_payload(extra_offset)) + sizeof(uint64_t))));
  }
  void set_addon_ptr(const ObTopNSortKey *addon_ptr, const RowMeta &row_meta)
  {
    *reinterpret_cast<int64_t *>(static_cast<char *>(this->get_extra_payload(row_meta))
                                 + sizeof(uint64_t)) = reinterpret_cast<int64_t>(addon_ptr);
  }
  uint64_t get_max_size(const RowMeta &row_meta) const
  {
    return *reinterpret_cast<uint64_t *>(this->get_extra_payload(row_meta));
  }
  void set_max_size(const uint64_t max_size, const RowMeta &row_meta)
  {
    *reinterpret_cast<uint64_t *>(this->get_extra_payload(row_meta)) = max_size;
  }
  static int64_t get_extra_size(bool is_sort_key)
  {
    if (is_sort_key) {
      return has_addon ? sizeof(ObTopNSortKey *) + sizeof(uint64_t) : sizeof(uint64_t);
    } else {
      return sizeof(uint64_t);
    }
  }
  TO_STRING_KV(KP(this));
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_KEY_VEC_OP_H_ */
