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

#ifndef SRC_STORAGE_MEMTABLE_OB_NOP_BITMAP_H_
#define SRC_STORAGE_MEMTABLE_OB_NOP_BITMAP_H_

#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace memtable
{
#define MEMSET_(ptr, m, n)                                 \
  {                                                        \
    if (n == 8 && m == 0xFF) {                             \
      *static_cast<uint64_t *>(ptr) = 0xFFFFFFFFFFFFFFFF;  \
    } else {                                               \
      MEMSET(ptr, m, n);                                   \
    }                                                      \
  }

class ObNopBitMap
{
public:
  ObNopBitMap():
    size_(0), cnt_(0), nop_cnt_(0), rowkey_cnt_(0) {}

  int init(int64_t size, int64_t rowkey_cnt)
  {
    size_ = round_up(size);
    cnt_ = size;
    nop_cnt_ = size - rowkey_cnt;
    rowkey_cnt_ = rowkey_cnt;
    MEMSET_(static_cast<void*>(bitmap_), 0xFF, size_ >> 3);
    return common::OB_SUCCESS;
  }

  inline void set_false(int64_t pos)
  {
    bitmap_[pos>>shift_bits] &= ~(1LLU << (pos & mask));
    nop_cnt_ -= (pos >= rowkey_cnt_);
  }

  inline int64_t get_nop_cnt()
  {
    return nop_cnt_;
  }

  inline bool test(int64_t pos)
  {
    return 0 != (bitmap_[pos>>shift_bits] & (1LLU << (pos & mask)));
  }

  inline bool is_empty()
  {
    return 0 == nop_cnt_;
  }

  inline void reuse()
  {
    MEMSET_(static_cast<void*>(bitmap_), 0xFF, size_ >> 3);
    nop_cnt_ = cnt_ - rowkey_cnt_;
  }

  inline int set_nop_obj(common::ObObj *obj_ptr)
  {
    const int64_t block_cnt = size_ >> shift_bits;
    int64_t start_block = rowkey_cnt_ >> shift_bits;
    int64_t offset = rowkey_cnt_ & mask;
    bitmap_[start_block] &= ~0LU << offset;
    int64_t left = (64 - (cnt_ & mask)) & mask;
    bitmap_[block_cnt - 1] &= ~0LU >> left;
    for (int64_t i = start_block; i < block_cnt; ++i) {
      uint64_t tmp = bitmap_[i];
      const int64_t base = i << shift_bits;
      int pos = 0;
      while (0 != tmp) {
        pos = __builtin_ctzl(tmp);
        obj_ptr[pos + base].set_nop_value();
        tmp ^= (1LU << pos);
      }
    }
    return common::OB_SUCCESS;
  }

  inline int set_nop_datums(blocksstable::ObStorageDatum *datum_ptr)
  {
    const int64_t block_cnt = size_ >> shift_bits;
    int64_t start_block = rowkey_cnt_ >> shift_bits;
    int64_t offset = rowkey_cnt_ & mask;
    bitmap_[start_block] &= ~0LU << offset;
    int64_t left = (64 - (cnt_ & mask)) & mask;
    bitmap_[block_cnt - 1] &= ~0LU >> left;
    for (int64_t i = start_block; i < block_cnt; ++i) {
      uint64_t tmp = bitmap_[i];
      const int64_t base = i << shift_bits;
      int pos = 0;
      while (0 != tmp) {
        pos = __builtin_ctzl(tmp);
        datum_ptr[pos + base].set_nop();
        tmp ^= (1LU << pos);
      }
    }
    return common::OB_SUCCESS;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObNopBitMap);
  inline int64_t round_up(int64_t n)
  {
    return (n + mask) & ~mask;
  }
private:
  static const int64_t mask = 63;
  static const int64_t shift_bits = 6;
  int64_t size_;
  int64_t cnt_;
  int64_t nop_cnt_;
  int64_t rowkey_cnt_;
  uint64_t bitmap_[(common::OB_ROW_MAX_COLUMNS_COUNT >> shift_bits) + 1];
};

}
}

#endif /* SRC_STORAGE_MEMTABLE_OB_NOP_BITMAP_H_ */
