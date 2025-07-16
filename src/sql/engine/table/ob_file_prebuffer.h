/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_FILE_PREBUFFER_H
#define OB_FILE_PREBUFFER_H

#include "lib/file/ob_file.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_vector.h"
#include "lib/list/ob_list.h"
#include "sql/engine/table/ob_external_file_access.h"

namespace oceanbase {
namespace sql {
class ObFilePreBuffer
{
private:
  class ColumnRange;
  class RangeCacheEntry;

public:
  class ReadRange;
  class ColumnRangeSlices;
  static constexpr int64_t DEFAULT_HOLE_SIZE_LIMIT = 1 * 1024 * 1024;
  static constexpr int64_t DEFAULT_RANGE_SIZE_LIMIT = 8 * 1024 * 1024;
  typedef common::ObSEArray<ReadRange, 16> ReadRangeList;
  typedef common::ObSEArray<ColumnRangeSlices*, 16> ColumnRangeSlicesList;
  typedef common::ObSEArray<ColumnRange, 16> ColumnRangeList;

  typedef common::ObSEArray<ReadRange, 16> CoalesceColumnRange;

  typedef common::ObSEArray<CoalesceColumnRange, 16> CoalesceColumnRangesList;
  typedef common::ObList<RangeCacheEntry*, ObIAllocator> ColumnRangeCacheEntry;
  typedef common::ObSEArray<ColumnRangeCacheEntry*, 16> ColumnRangeCacheEntryList;
  typedef common::ObSortedVector<RangeCacheEntry*> RangeCacheEntryList;

  struct CacheOptions {
    CacheOptions() : hole_size_limit_(0), range_size_limit_(0), lazy_(false), prefetch_limit_(0)
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        hole_size_limit_ = tenant_config->external_table_io_hole_size;
        range_size_limit_ = tenant_config->external_table_io_range_size;
      }
    }
    CacheOptions(const int64_t hole_size_limit, const int64_t range_size_limit, const bool lazy,
                 const int64_t prefetch_limit) :
      hole_size_limit_(hole_size_limit),
      range_size_limit_(range_size_limit), lazy_(lazy), prefetch_limit_(prefetch_limit)
    {}
    static CacheOptions defaults();
    static CacheOptions lazy_defaults();
    TO_STRING_KV(K_(hole_size_limit),
                 K_(range_size_limit),
                 K_(lazy),
                 K_(prefetch_limit));

    int64_t hole_size_limit_;
    int64_t range_size_limit_;
    bool lazy_;
    int64_t prefetch_limit_;
  };

  struct ReadRange {
    ReadRange() : offset_(0), length_(0)
    {}
    ReadRange(int64_t offset, int64_t length) : offset_(offset), length_(length)
    {}
    friend bool operator==(const ReadRange& left, const ReadRange& right) {
      return (left.offset_ == right.offset_ && left.length_ == right.length_);
    }
    friend bool operator!=(const ReadRange& left, const ReadRange& right) {
      return !(left == right);
    }
    OB_INLINE ReadRange &operator=(const ReadRange &other)
    {
      if (this != &other) {
        offset_ = other.offset_;
        length_ = other.length_;
      }
      return *this;
    }
    bool contains(const ReadRange& other) const {
      return (offset_ <= other.offset_ && offset_ + length_ >= other.offset_ + other.length_);
    }
    static bool compare(const ReadRange &l, const ReadRange &r)
    {
      return l.offset_ < r.offset_;
    }
    TO_STRING_KV(K_(offset), K_(length));

    int64_t offset_;
    int64_t length_;
  };

  struct ColumnRangeSlices {
    ColumnRangeSlices() : range_list_()
    {}
    ColumnRangeSlices &operator=(const ColumnRangeSlices &) = delete;
    TO_STRING_KV(K_(range_list));
    ReadRangeList range_list_;
  };

private:
  struct ColumnRangeMeta {
    ColumnRangeMeta() : offset_(-1), length_(0)
    {}
    OB_INLINE ColumnRangeMeta &operator=(const ColumnRangeMeta &other)
    {
      if (this != &other) {
        offset_ = other.offset_;
        length_ = other.length_;
      }
      return *this;
    }
    TO_STRING_KV(K_(offset), K_(length));
    /// offset_: the smallest offset in the range list
    /// length_: describes the maximum range length of the current column range.
    /// that is, the length from the smallest offset to the maximum offset + length
    /// eg: range_list_: {{1, 10}, {20, 100}}, offset_: 1, length_: (20 + 100) - 1 = 119
    int64_t offset_;
    int64_t length_;
  };

  struct ColumnRange {
    ColumnRange() : order_(0), need_merge_(true), meta_(), range_slice_set_(nullptr)
    {}
    ColumnRange(ColumnRangeSlices *range_set) :
      order_(0), need_merge_(true), meta_(), range_slice_set_(range_set)
    {}
    OB_INLINE ColumnRange &operator=(const ColumnRange &other)
    {
      if (this != &other) {
        order_ = other.order_;
        need_merge_ = other.need_merge_;
        meta_ = other.meta_;
        range_slice_set_ = other.range_slice_set_;
      }
      return *this;
    }
    void extract_meta();
    static ColumnRange make_merge_column_range(int64_t offset, int64_t length);
    bool is_valid_range();
    static bool compare(const ColumnRange &l, const ColumnRange &r)
    {
      return l.meta_.offset_ < r.meta_.offset_;
    }
    TO_STRING_KV(K_(order), K_(need_merge), K_(meta), KPC_(range_slice_set));
    int64_t order_;
    bool need_merge_;
    ColumnRangeMeta meta_;
    ColumnRangeSlices *range_slice_set_;
  };

  struct RangeCombiner {
    RangeCombiner(int64_t hole_size_limit, int64_t range_size_limit) :
      hole_size_limit_(hole_size_limit), range_size_limit_(range_size_limit)
    {}
    RangeCombiner &operator=(const RangeCombiner &other) = delete;
    int coalesce(ColumnRangeList &range_list,
                 CoalesceColumnRangesList &range_entries);
    int coalesce_column_ranges(ColumnRangeList &range_list,
                               ColumnRangeList &merge_range_list);
    int coalesce_each_column_ranges(const ColumnRange &range_info,
                                    CoalesceColumnRange &range_entry);
    TO_STRING_KV(K_(hole_size_limit),
                 K_(range_size_limit));
    int64_t hole_size_limit_;
    int64_t range_size_limit_;
  };

  struct RangeCacheEntry {
    RangeCacheEntry() :
      range_(), is_waited_(false), buf_(nullptr), col_cache_entry_(nullptr), io_handle_()
    {}
    RangeCacheEntry &operator=(const RangeCacheEntry &other) = delete;
    bool is_read_cache_range_end(const ReadRange &range) const;
    int wait();
    static bool compare(const RangeCacheEntry *l, const RangeCacheEntry *r)
    {
      return l->range_.offset_ < r->range_.offset_;
    }
    TO_STRING_KV(K_(range), K_(buf), K_(col_cache_entry));
    ReadRange range_;
    bool is_waited_;
    void *buf_;
    ColumnRangeCacheEntry *col_cache_entry_;
    ObExternalFileReadHandle io_handle_;
  };

  struct RangeEntryCmp
  {
    bool operator()(const RangeCacheEntry *entry, const ReadRange &range)
    {
      return entry->range_.offset_ + entry->range_.length_ < range.offset_ + range.length_;
    }
  };

public:
  ObFilePreBuffer(ObExternalFileAccess &file_reader) :
    alloc_(common::ObMemAttr(MTL_ID(), "PreBuffer")), options_(),
    timeout_ts_(INT64_MAX), file_reader_(file_reader), cache_entries_(),
    column_range_cache_entries_()
  {}
  ObFilePreBuffer(const int64_t tenant_id, ObExternalFileAccess &file_reader) :
    alloc_(common::ObMemAttr(tenant_id, "PreBuffer")), options_(),
    timeout_ts_(INT64_MAX), file_reader_(file_reader), cache_entries_(),
    column_range_cache_entries_()
  {}
  ~ObFilePreBuffer();
  int destroy();
  int reset();
  int init(const CacheOptions &cache_options);
  int init(const CacheOptions &cache_options, const int64_t ts_timeout);
  void set_timeout_timestamp(const int64_t ts_timeout_us);
  /// NOTE: The range lists of each column do not overlap, and the order of prefetching is based on
  /// the order of the range lists of the column range.
  /// when pre_buffer is called for the first time in lazy mode, the first range io read of
  /// each column is triggered first, and then prefetched according to the prefetch limit
  int pre_buffer(const ColumnRangeSlicesList &range_list);
  int read(int64_t position, int64_t length, void* out);

private:
  int prefetch_first_range_of_each_column(ColumnRangeCacheEntryList &range_entries);
  int prefetch_column_ranges(ColumnRangeCacheEntryList &range_entries);
  int async_read_range(RangeCacheEntry &cache_range);
  int coalesce_ranges(const ColumnRangeSlicesList &range_list,
                      CoalesceColumnRangesList &range_entries);
  int convert_coalesce_column_ranges(const CoalesceColumnRangesList &coalesce_col_ranges,
                                     ColumnRangeCacheEntryList &column_range_cache_entries);
  int convert_coalesce_col_range_to_cache_entry(const CoalesceColumnRange &column_range,
                                                ColumnRangeCacheEntry *&column_cache_entry);
  int build_cache_entries(const ColumnRangeCacheEntryList &column_range_cache_entries,
                          RangeCacheEntryList &cache_entries);
  // only for debugging
  void dump_raw_ranges(ColumnRangeSlicesList &range_list);
  void dump_coalesce_ranges(CoalesceColumnRangesList &range_entries);
  template<typename ContainerT>
  void dump_list(ContainerT &container)
  {
    int64_t idx = 0;
    FOREACH(iter, container) {
      SQL_ENG_LOG(TRACE, "dump list", K(idx), K(*iter));
      ++idx;
    }
  }

private:
  common::ObMalloc alloc_;
  CacheOptions options_;
  int64_t timeout_ts_;
  ObExternalFileAccess &file_reader_;
  // need to delete randomly, so a list is better, but we also need to sort
  // and do a binary search, so we will use ObSortedVector for now.
  RangeCacheEntryList cache_entries_;
  ColumnRangeCacheEntryList column_range_cache_entries_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFilePreBuffer);
};

}
}

#endif // OB_FILE_PREBUFFER_H
