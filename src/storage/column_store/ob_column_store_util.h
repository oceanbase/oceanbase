/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef _OB_COLUMN_STORE_UTIL_H
#define _OB_COLUMN_STORE_UTIL_H

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_iarray.h"
#include "storage/blocksstable/ob_datum_range.h"

namespace oceanbase
{
namespace storage
{
class ObITable;
struct ObTableAccessParam;

typedef int64_t ObCSRowId;
const ObCSRowId OB_INVALID_CS_ROW_ID = -1;
const uint32_t OB_CS_INVALID_CG_IDX = INT32_MAX;
const uint32_t OB_CS_VIRTUAL_CG_IDX = INT32_MAX - 1;

OB_INLINE bool is_virtual_cg(const uint32_t cg_idx)
{
  return OB_CS_VIRTUAL_CG_IDX == cg_idx;
}

enum BlockScanState
{
  BLOCKSCAN_RANGE = 0,
  SWITCH_RANGE,
  BLOCKSCAN_FINISH,
  SCAN_FINISH,
  MAX_STATE,
};

struct ObCSRange
{
  ObCSRange() { reset(); }
  ~ObCSRange() = default;
  ObCSRange(const ObCSRange &range)
  {
    start_row_id_ = range.start_row_id_;
    end_row_id_ = range.end_row_id_;
  }
  ObCSRange(const ObCSRowId start, const uint64_t count)
  {
    start_row_id_ = start;
    end_row_id_ = start + count - 1;
  }
  OB_INLINE void set(const ObCSRowId start, const ObCSRowId end)
  {
    start_row_id_ = start;
    end_row_id_ = end;
  }
  OB_INLINE ObCSRowId begin() const { return start_row_id_; }
  OB_INLINE ObCSRowId end() const { return end_row_id_; }
  OB_INLINE void reset() { start_row_id_ = OB_INVALID_CS_ROW_ID; end_row_id_ = OB_INVALID_CS_ROW_ID; }
  OB_INLINE bool is_valid() const { return start_row_id_ >= 0 && end_row_id_ >= start_row_id_; }
  OB_INLINE int64_t get_row_count() const { return end_row_id_ - start_row_id_ + 1; }
  int compare(const ObCSRowId idx) const
  {
    int cmp_ret = 0;
    if (idx < start_row_id_) {
      cmp_ret = 1;
    } else if (idx > end_row_id_) {
      cmp_ret = -1;
    }
    return cmp_ret;
  }

  TO_STRING_KV(K_(start_row_id), K_(end_row_id));
  ObCSRowId start_row_id_;
  ObCSRowId end_row_id_;
};

class ObCSDatumRange
{
public:
  ObCSDatumRange()
    : cs_datum_range_(),
      datums_()
  {}
  ~ObCSDatumRange() = default;
  ObCSDatumRange(const ObCSRange &range)
  {
    set_datum_range(range.start_row_id_, range.end_row_id_);
  }
  ObCSDatumRange(const int64_t start, const int64_t end)
  {
    set_datum_range(start, end);
  }
  void set_datum_range(const int64_t start, const int64_t end);
  const blocksstable::ObDatumRange& get_cs_datum_range() const { return cs_datum_range_; }

  TO_STRING_KV(K_(cs_datum_range));
private:
  blocksstable::ObDatumRange cs_datum_range_;
  blocksstable::ObStorageDatum datums_[2];
};

template<typename T>
OB_INLINE int init_fixed_array_param(common::ObFixedArray<T, common::ObIAllocator> &param, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (FALSE_IT(param.clear())) {
  } else if (OB_FAIL(param.reserve(size))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      STORAGE_LOG(WARN, "Failed to init params", K(ret));
    } else {
      param.reset();
      if (OB_FAIL(param.init(size))) {
        STORAGE_LOG(WARN, "Failed to init params", K(ret), K(size));
      }
    }
  }
  return ret;
}

} /* storage */
} /* oceanbase */
#endif
