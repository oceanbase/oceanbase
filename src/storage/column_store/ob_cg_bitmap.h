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
#ifndef OB_STORAGE_COLUMN_STORE_ROW_BITMAP__
#define OB_STORAGE_COLUMN_STORE_ROW_BITMAP__

#include "lib/container/ob_bitmap.h"
#include "storage/column_store/ob_column_store_util.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

class ObCGBitmap
{
public:
  ObCGBitmap(ObIAllocator &allocator) :
      bitmap_(allocator),
      start_row_id_(OB_INVALID_CS_ROW_ID)
  {}
  virtual ~ObCGBitmap()
  {}
  bool is_valid() const
  {
    return start_row_id_ != OB_INVALID_CS_ROW_ID;
  }
  int copy_from(const ObCGBitmap &bitmap)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!bitmap.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument", K(ret), K(bitmap));
    } else if (OB_FAIL(bitmap_.reserve(bitmap.bitmap_.size()))) {
      STORAGE_LOG(WARN, "Fail to expand bitmap", K(ret), K(bitmap.bitmap_.size()));
    } else if (OB_FAIL(bitmap_.copy_from(bitmap.bitmap_, 0, bitmap.bitmap_.size()))) {
      STORAGE_LOG(WARN, "Fail to set bitmap", K(ret), K(bitmap));
    } else {
      start_row_id_ = bitmap.start_row_id_;
    }
    return ret;
  }

  int init(
      const uint64_t count,
      const ObCSRowId start_row_id = OB_INVALID_CS_ROW_ID,
      const bool is_all_true = false)
  {
    start_row_id_ = start_row_id;
    return  bitmap_.init(count, is_all_true);
  }

  void reuse(const ObCSRowId start_row_id, const bool is_all_true = false)
  {
    start_row_id_ = start_row_id;
    bitmap_.reuse(is_all_true);
  }

  OB_INLINE ObBitmap* get_inner_bitmap()
  {
    return &bitmap_;
  }

  OB_INLINE ObCSRowId get_start_id() const
  {
    return start_row_id_;
  }

  OB_INLINE int reserve(uint64_t count)
  {
    return bitmap_.reserve(count);
  }

  OB_INLINE int set(const ObCSRowId row_idx, const bool value = true)
  {
    OB_ASSERT(row_idx >= start_row_id_);
    return bitmap_.set(row_idx - start_row_id_, value);
  }

  OB_INLINE int wipe(const ObCSRowId row_idx)
  {
    OB_ASSERT(row_idx >= start_row_id_);
    return bitmap_.wipe(row_idx - start_row_id_);
  }

  OB_INLINE bool test(const ObCSRowId row_idx) const
  {
    OB_ASSERT(row_idx >= start_row_id_);
    return bitmap_.test(row_idx - start_row_id_);
  }

  OB_INLINE bool operator[](const ObCSRowId row_idx) const
  {
    OB_ASSERT(row_idx >= start_row_id_);
    return bitmap_.test(row_idx - start_row_id_);
  }

  OB_INLINE int64_t capacity() const
  {
    return bitmap_.capacity();
  }

  OB_INLINE uint64_t size() const
  {
    return bitmap_.size();
  }

  OB_INLINE uint64_t popcnt() const
  {
    return bitmap_.popcnt();
  }

  OB_INLINE bool is_all_true() const
  {
    return bitmap_.is_all_true();
  }

  OB_INLINE bool is_all_false() const
  {
    return bitmap_.is_all_false();
  }

  OB_INLINE int bit_and(const ObCGBitmap &right)
  {
    int ret = OB_SUCCESS;
    if (start_row_id_ != right.start_row_id_) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument", K(ret), K(start_row_id_), K(right.start_row_id_));
    } else if (OB_FAIL(bitmap_.bit_and(right.bitmap_))) {
      STORAGE_LOG(WARN, "Fail to bit and", K(ret), KPC(this), K(right));
    }
    return ret;
  }

  OB_INLINE int bit_or(const ObCGBitmap &right)
  {
    int ret = OB_SUCCESS;
    if (start_row_id_ != right.start_row_id_) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument", K(ret), K(start_row_id_), K(right.start_row_id_));
    } else if (OB_FAIL(bitmap_.bit_or(right.bitmap_))) {
      STORAGE_LOG(WARN, "Fail to bit or", K(ret), KPC(this), K(right));
    }
    return ret;
  }

  OB_INLINE int bit_not()
  {
    return bitmap_.bit_not();
  }

  int append_bitmap(const ObBitmap &bitmap, const uint32_t offset, const bool is_reverse)
  {
    return bitmap_.append_bitmap(bitmap, offset, is_reverse);
  }

  OB_INLINE bool is_all_true(const ObCSRange &range) const
  {
    OB_ASSERT(range.is_valid() && range.end_row_id_ >= start_row_id_);
    return bitmap_.is_all_true(MAX(range.start_row_id_ - start_row_id_, 0),
                               MIN(range.end_row_id_ - start_row_id_, bitmap_.size() - 1));

  }

  OB_INLINE bool is_all_false(const ObCSRange &range) const
  {
    OB_ASSERT(range.is_valid() && range.end_row_id_ >= start_row_id_);
    return bitmap_.is_all_false(MAX(range.start_row_id_ - start_row_id_, 0),
                                MIN(range.end_row_id_ - start_row_id_, bitmap_.size() - 1));
  }

  OB_INLINE void set_all_true()
  {
    bitmap_.reuse(true);
  }

  OB_INLINE void set_all_false()
  {
    bitmap_.reuse(false);
  }
  OB_INLINE int set_bitmap_batch(ObCSRowId start, ObCSRowId end, const bool value)
  {
    int64_t offset = MAX(start - start_row_id_,  0);
    int64_t count = MIN(end - start + 1, bitmap_.size() - offset);
    return bitmap_.set_bitmap_batch(offset, count, value);
  }

  int set_bitmap(const ObCSRowId start, const int64_t row_count, const bool is_reverse, ObBitmap &bitmap) const;
  int get_first_valid_idx(const ObCSRange &range, const bool is_reverse_scan, ObCSRowId &row_idx) const;
  int get_row_ids(int32_t *row_ids,
                  int64_t &row_cap,
                  ObCSRowId &current,
                  const ObCSRange &query_range,
                  const ObCSRange &data_range,
                  const int64_t batch_size,
                  const bool is_reverse);
  TO_STRING_KV(K_(bitmap), K_(start_row_id));

private:
  common::ObBitmap bitmap_;
  ObCSRowId start_row_id_;
};

int convert_bitmap_to_cs_index(int32_t *row_ids,
                               int64_t &row_cap,
                               ObCSRowId &current,
                               const ObCSRange &query_range,
                               const ObCSRange &data_range,
                               const ObCGBitmap *filter_bitmap,
                               const int64_t batch_size,
                               const bool is_reverse);

}
}
#endif //OB_STORAGE_COLUMN_STORE_ROW_BITMAP__
