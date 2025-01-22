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
#include "src/sql/engine/basic/ob_pushdown_filter.h"
namespace oceanbase
{
using namespace common;
namespace storage
{

class ObCGBitmap
{
public:
  ObCGBitmap(ObIAllocator &allocator) :
      is_reverse_scan_(false),
      bitmap_(allocator),
      start_row_id_(OB_INVALID_CS_ROW_ID),
      max_filter_constant_id_(OB_INVALID_CS_ROW_ID),
      filter_constant_type_()
  {}
  virtual ~ObCGBitmap()
  {}
  bool is_valid() const
  {
    return filter_constant_type_.is_constant() || start_row_id_ != OB_INVALID_CS_ROW_ID;
  }
  int copy_from(const ObCGBitmap &bitmap)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!bitmap.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument", K(ret), K(bitmap));
    } else if (OB_FAIL(bitmap_.reserve(bitmap.bitmap_.size()))) {
      STORAGE_LOG(WARN, "Fail to expand bitmap", K(ret), K(bitmap.bitmap_.size()));
    } else if (bitmap.filter_constant_type_.is_constant()) {
      max_filter_constant_id_ = bitmap.max_filter_constant_id_;
      filter_constant_type_ = bitmap.filter_constant_type_;
      bitmap_.reuse(filter_constant_type_.is_always_true());
    } else {
      if (OB_FAIL(bitmap_.copy_from(bitmap.bitmap_, 0, bitmap.bitmap_.size()))) {
        STORAGE_LOG(WARN, "Fail to set bitmap", K(ret), K(bitmap));
      } else {
        max_filter_constant_id_ = OB_INVALID_CS_ROW_ID;
        filter_constant_type_.set_uncertain();
      }
    }

    if (OB_SUCC(ret)) {
      is_reverse_scan_ = bitmap.is_reverse_scan_;
      start_row_id_ = bitmap.start_row_id_;
    }
    return ret;
  }

  int init(const uint64_t count, const bool is_reverse)
  {
    is_reverse_scan_ = is_reverse;
    return  bitmap_.init(count);
  }

  void reuse(
      const ObCSRowId start_row_id,
      const bool is_all_true = false)
  {
    start_row_id_ = start_row_id;
    if (is_all_true) {
      filter_constant_type_.set_always_true();
      max_filter_constant_id_ = is_reverse_scan_ ? start_row_id : start_row_id + bitmap_.size() - 1;
    } else {
      filter_constant_type_.set_always_false();
      max_filter_constant_id_ = is_reverse_scan_ ? start_row_id : start_row_id + bitmap_.size() - 1;
    }
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

  OB_INLINE int switch_context(const uint64_t count, const bool is_reverse)
  {
    is_reverse_scan_ = is_reverse;
    filter_constant_type_.set_uncertain();
    max_filter_constant_id_ = OB_INVALID_CS_ROW_ID;
    start_row_id_ = OB_INVALID_CS_ROW_ID;
    return bitmap_.reserve(count);
  }

  OB_INLINE int set(const ObCSRowId row_idx, const bool value = true)
  {
    OB_ASSERT(row_idx >= start_row_id_);
    filter_constant_type_.set_uncertain();
    return bitmap_.set(row_idx - start_row_id_, value);
  }

  OB_INLINE int wipe(const ObCSRowId row_idx)
  {
    OB_ASSERT(row_idx >= start_row_id_);
    filter_constant_type_.set_uncertain();
    return bitmap_.wipe(row_idx - start_row_id_);
  }

  OB_INLINE bool test(const ObCSRowId row_idx) const
  {
    OB_ASSERT(row_idx >= start_row_id_);
    bool res;
    if (filter_constant_type_.is_constant()) {
      res = filter_constant_type_.is_always_true();
    } else {
      res = bitmap_.test(row_idx - start_row_id_);
    }
    return res;
  }

  OB_INLINE bool operator[](const ObCSRowId row_idx) const
  {
    OB_ASSERT(row_idx >= start_row_id_);
    bool res;
    if (filter_constant_type_.is_constant()) {
      res = filter_constant_type_.is_always_true();
    } else {
      res = bitmap_.test(row_idx - start_row_id_);
    }
    return res;
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
    uint64_t cnt = 0;
    if (filter_constant_type_.is_constant()) {
      cnt = filter_constant_type_.is_always_true() ? bitmap_.size() : 0;
    } else {
      cnt = bitmap_.popcnt();
    }
    return cnt;
  }

  OB_INLINE bool is_all_true() const
  {
    return filter_constant_type_.is_always_true() ||
        (filter_constant_type_.is_uncertain() && bitmap_.is_all_true());
  }

  OB_INLINE bool is_all_false() const
  {
    return filter_constant_type_.is_always_false() ||
        (filter_constant_type_.is_uncertain() && bitmap_.is_all_false());
  }

  int bit_and(const ObCGBitmap &right);

  int bit_or(const ObCGBitmap &right);

  OB_INLINE int bit_not()
  {
    int ret = OB_SUCCESS;
    if (filter_constant_type_.is_always_true()) {
      filter_constant_type_.set_always_false();
    } else if (filter_constant_type_.is_always_false()) {
      filter_constant_type_.set_always_true();
    } else {
      ret = bitmap_.bit_not();
    }
    return ret;
  }

  int append_bitmap(const ObBitmap &bitmap, const uint32_t offset, const bool is_reverse)
  {
    filter_constant_type_.set_uncertain();
    return bitmap_.append_bitmap(bitmap, offset, is_reverse);
  }

  OB_INLINE bool is_all_true(const ObCSRange &range) const
  {
    OB_ASSERT(range.is_valid() && range.end_row_id_ >= start_row_id_);
    return filter_constant_type_.is_always_true() ||
        (filter_constant_type_.is_uncertain() &&
         bitmap_.is_all_true(MAX(range.start_row_id_ - start_row_id_, 0),
                             MIN(range.end_row_id_ - start_row_id_, bitmap_.size() - 1)));
  }

  OB_INLINE bool is_all_false(const ObCSRange &range) const
  {
    OB_ASSERT(range.is_valid() && range.end_row_id_ >= start_row_id_);
    return filter_constant_type_.is_always_false() ||
        (filter_constant_type_.is_uncertain() &&
         bitmap_.is_all_false(MAX(range.start_row_id_ - start_row_id_, 0),
                              MIN(range.end_row_id_ - start_row_id_, bitmap_.size() - 1)));
  }

  OB_INLINE void set_all_true()
  {
    filter_constant_type_.set_always_true();
    max_filter_constant_id_ = is_reverse_scan_ ? start_row_id_ : start_row_id_ + bitmap_.size() - 1;
  }

  OB_INLINE void set_all_false()
  {
    filter_constant_type_.set_always_false();
    max_filter_constant_id_ = is_reverse_scan_ ? start_row_id_ : start_row_id_ + bitmap_.size() - 1;
  }

  OB_INLINE int set_bitmap_batch(ObCSRowId start, ObCSRowId end, const bool value, int64_t &count)
  {
    int64_t offset = MAX(start - start_row_id_,  0);
    count = MIN(end - start + 1, bitmap_.size() - offset);
    filter_constant_type_.set_uncertain();
    return bitmap_.set_bitmap_batch(offset, count, value);
  }

  OB_INLINE void set_constant_filter_info(const sql::ObBoolMask filter_constant_type, const ObCSRowId filter_constant_id)
  {
    filter_constant_type_ = filter_constant_type;
    max_filter_constant_id_ = filter_constant_id;
  }

  OB_INLINE sql::ObBoolMask get_filter_constant_type() const
  {
    return filter_constant_type_;
  }

  OB_INLINE ObCSRowId get_filter_constant_id() const
  {
    return max_filter_constant_id_;
  }

  OB_INLINE void set_filter_uncertain()
  {
    return filter_constant_type_.set_uncertain();
  }

  int set_bitmap(const ObCSRowId start, const int64_t row_count, const bool is_reverse, ObBitmap &bitmap) const;
  int get_first_valid_idx(const ObCSRange &range, ObCSRowId &row_idx) const;
  int get_row_ids(int32_t *row_ids,
                  int64_t &row_cap,
                  ObCSRowId &current,
                  const ObCSRange &query_range,
                  const ObCSRange &data_range,
                  const int64_t batch_size,
                  const bool is_reverse);
  TO_STRING_KV(K_(is_reverse_scan), K_(start_row_id), K_(max_filter_constant_id),
               K_(filter_constant_type), K_(bitmap));
private:
  bool is_reverse_scan_;
  common::ObBitmap bitmap_;
  ObCSRowId start_row_id_;
  // max_filter_constant_id_ may greater/less than offset+size
  ObCSRowId max_filter_constant_id_;
  sql::ObBoolMask filter_constant_type_;
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
