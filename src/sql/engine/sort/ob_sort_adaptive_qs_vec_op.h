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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_ADAPTIVE_QS_VEC_OP_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_ADAPTIVE_QS_VEC_OP_H_

#include "lib/container/ob_array.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"

namespace oceanbase
{
namespace sql
{
extern int fast_compare_simd(const unsigned char *s, const unsigned char *t, int64_t length,
                             int64_t &differ_at, int64_t cache_ends);

template <typename Store_Row>
class ObAdaptiveQS
{
public:
  struct AQSItem
  {
    unsigned char *key_ptr_;
    Store_Row *row_ptr_;
    uint32_t len_;
    unsigned char sub_cache_[2];
    AQSItem() : key_ptr_(nullptr), row_ptr_(nullptr), len_(0), sub_cache_()
    {}
    TO_STRING_KV(K_(len), K_(sub_cache), K_(key_ptr), KP(row_ptr_));
  };

  ObAdaptiveQS(common::ObIArray<Store_Row *> &sort_rows, const RowMeta &row_meta,
               common::ObIAllocator &alloc);
  int init(common::ObIArray<Store_Row *> &sort_rows, common::ObIAllocator &alloc,
           int64_t rows_begin, int64_t rows_end, bool &can_encode);
  ~ObAdaptiveQS()
  {
    reset();
  }
  void sort(int64_t rows_begin, int64_t rows_end)
  {
    aqs_cps_qs(0, sort_rows_.count(), 0, 0, 0);
    for (int64_t i = 0; i < sort_rows_.count() && i < (rows_end - rows_begin); ++i) {
      orig_sort_rows_.at(i + rows_begin) = sort_rows_.at(i).row_ptr_;
    }
  }
  void reset()
  {
    sort_rows_.reset();
  }
  void aqs_cps_qs(int64_t l, int64_t r, int64_t common_prefix, int64_t depth_limit,
                  int64_t cache_offset);
  void aqs_radix(int64_t l, int64_t r, int64_t common_prefix, int64_t offset, int64_t depth_limit);
  void inplace_radixsort_more_bucket(int64_t l, int64_t r, int64_t div_val, int64_t common_prefix,
                                     int64_t depth_limit, int64_t cache_offset, bool update);
  void insertion_sort(int64_t l, int64_t r, int64_t common_prefix, int64_t cache_offset);
  inline void swap(int64_t l, int64_t r)
  {
    std::swap(sort_rows_[r], sort_rows_[l]);
  }
  inline int compare_vals(int64_t l, int64_t r, int64_t &differ_at, int64_t common_prefix,
                          int64_t cache_offset);
  inline int compare_cache(AQSItem &l, AQSItem &r, int64_t &differ_at, int64_t common_prefix,
                           int64_t cache_offset);
  static int fast_cmp_normal(const unsigned char *s, const unsigned char *t, int64_t length,
                             int64_t &differ_at, int64_t cache_ends);
  using CmpByteFunc = std::function<int(const unsigned char *s, const unsigned char *t,
                                        int64_t length, int64_t &differ_at, int64_t cache_ends)>;
  CmpByteFunc get_fast_cmp_func()
  {
    return blocksstable::is_avx512_valid() ? fast_compare_simd : fast_cmp_normal;
  }
  CmpByteFunc cmp_byte_func = get_fast_cmp_func();

public:
  unsigned char masks[8]{0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};
  const RowMeta &row_meta_;
  common::ObIArray<Store_Row *> &orig_sort_rows_;
  common::ObFixedArray<AQSItem, common::ObIAllocator> sort_rows_;
  common::ObIAllocator &alloc_;
};

} // end namespace sql
} // end namespace oceanbase

#include "ob_sort_adaptive_qs_vec_op.ipp"

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_ADAPTIVE_QS_VEC_OP_H_ */
