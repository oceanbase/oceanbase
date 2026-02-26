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

OB_INLINE int64_t *get_type_encoding_sortkey_size_map()
{
  static int64_t type_size_map[] = {
    -1, // ObNullType
    2, // ObTinyIntType
    3, // ObSmallIntType
    5, // ObMediumIntType
    5, // ObInt32Type
    9, // ObIntType
    2, // ObUTinyIntType
    3, // ObUSmallIntType
    5, // ObUMediumIntType
    5, // ObUInt32Type
    9, // ObUInt64Type
    5, // ObFloatType
    9, // ObDoubleType
    5, // ObUFloatType
    9, // ObUDoubleType
    -1, // ObNumberType
    -1, // ObUNumberType
    9, // ObDateTimeType
    9, // ObTimestampType
    5, // ObDateType=19,
    9, // ObTimeType
    2, // ObYearType=21,
    -1, // ObVarcharType=22
    -1, // ObCharType=23
    -1, // ObHexStringType
    -1, // ObExtendType
    -1, // ObUnknownType
    -1, // ObTinyTextType
    -1, // ObTextType
    -1, // ObMediumTextType
    -1, // ObLongTextType
    -1, // ObBitType
    -1, // ObEnumType
    -1, // ObSetType
    -1, //ObEnumInnerType
    -1, //ObSetInnerType
    -1, //ObTimestampTZType
    -1, //ObTimestampLTZType
    -1, //ObTimestampNanoType
    -1, //ObRawType
    -1, //ObIntervalYMType
    -1, //ObIntervalDSType
    -1, //ObNumberFloatType
    -1, //ObNVarchar2Type
    -1, //ObNChar
    -1, // ObURowID
    -1, //Lob
    -1, //Json
    -1, //Geometry
    -1, //ObUserDefinedSQLType
    -1, //ObDecimalIntType
    -1, //ObCollectionSQLType
    -1, // ObMySQLDateType
    -1, // ObMySQLDateTimeType
    -1, //RoaringBitmap
    -1 // ObMaxType
  };
  STATIC_ASSERT(ARRAYSIZEOF(type_size_map) == common::ObMaxType + 1,
      "type size map count mismatch with type count");
  return type_size_map;
}
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

template <typename StoreRow, uint32_t SortKeyLen>
struct SortKey {
  static int64_t get_key_size()
  {
    return SortKeyLen;
  }
  OB_INLINE int64_t key_cmp(const SortKey &other_key) const
  {
    return MEMCMP(key_, other_key.key_, SortKeyLen);
  }
  void init(const StoreRow *row, const RowMeta &row_meta)
  {
    int64_t len = row->get_length(row_meta, 0);
    const char *encode_sortkey_ptr = row->get_cell_payload(row_meta, 0);
    MEMCPY(key_, encode_sortkey_ptr, len);
  }

  char key_[SortKeyLen];
  TO_STRING_KV(KP(key_), KPHEX(key_, SortKeyLen));
};

template <typename StoreRow, typename SortKey>
struct SortingItem {
  using KeyType = SortKey;
  static int64_t get_item_size()
  {
    return sizeof(SortingItem);
  }
  void init(const StoreRow *row, const RowMeta &row_meta)
  {
    key_.init(row, row_meta);
    row_ptr_ = const_cast<StoreRow *>(row);
  }

  SortKey key_;
  StoreRow *row_ptr_;
  TO_STRING_KV(K(key_), KP(row_ptr_));
}__attribute__ ((packed));

template <typename StoreRow, typename SortingItem>
class ObFixedKeySort {
  using DataPtr = uint8_t *;

public:
  ObFixedKeySort(common::ObIArray<StoreRow *> &sort_rows, const RowMeta &row_meta,
      common::ObIAllocator &alloc);
  ~ObFixedKeySort()
  {
    reset();
  }

  int init(common::ObIArray<StoreRow *> &sort_rows, common::ObIAllocator &alloc,
      int64_t rows_begin, int64_t rows_end, bool &can_encode);
  void sort(int64_t rows_begin, int64_t rows_end)
  {
    radix_sort(reinterpret_cast<DataPtr>(sorting_items_),
        reinterpret_cast<DataPtr>(tmp_items_),
        item_cnt_,
        0,
        buckets_,
        false);
    for (int64_t i = 0; i < item_cnt_ && i < (rows_end - rows_begin); i++) {
      orig_sort_rows_.at(i + rows_begin) =
          reinterpret_cast<StoreRow *>(sorting_items_[i].row_ptr_);
    }
  }
  void reset();

private:
  int prepare_sorting_items(int64_t rows_begin, int64_t rows_end);
  void insertion_sort(const DataPtr orig_ptr, const int64_t count);
  void radix_sort(const DataPtr orig_ptr, const DataPtr tmp_ptr, const int64_t count,
      const int64_t offset, int64_t *locations, bool swap);

public:
  static constexpr int64_t INSERTION_SORT_THRESHOLD = 16;
  static constexpr int64_t VALUES_PER_RADIX = 256;
  static constexpr int64_t RADIX_LOCATIONS = VALUES_PER_RADIX + 1;

public:
  const RowMeta &row_meta_;
  common::ObIArray<StoreRow *> &orig_sort_rows_;
  common::ObFixedArray<SortingItem, common::ObIAllocator> items_;
  common::ObIAllocator &alloc_;
  DataPtr buf_;
  SortingItem *sorting_items_;
  SortingItem *tmp_items_;
  int64_t item_cnt_;
  int64_t key_size_;
  int64_t item_size_;
  int64_t *buckets_;
};

template <typename StoreRow, uint32_t SortKeyLen>
using FixedKeySort = ObFixedKeySort<StoreRow, SortingItem<StoreRow, SortKey<StoreRow, SortKeyLen>>>;

} // end namespace sql
} // end namespace oceanbase

#include "ob_sort_adaptive_qs_vec_op.ipp"

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_ADAPTIVE_QS_VEC_OP_H_ */
