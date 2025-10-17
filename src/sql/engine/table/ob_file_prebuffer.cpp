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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_file_prebuffer.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql {
#define SAFE_DELETE(p)                                                                             \
  do {                                                                                             \
    alloc_.free(p);                                                                                \
    p = nullptr;                                                                                   \
  } while (0)

ObFilePreBuffer::CacheOptions ObFilePreBuffer::CacheOptions::defaults()
{
  ObFilePreBuffer::CacheOptions options;
  options.lazy_ = false;
  options.prefetch_limit_ = 0;
  return options;
}

ObFilePreBuffer::CacheOptions ObFilePreBuffer::CacheOptions::lazy_defaults()
{
  ObFilePreBuffer::CacheOptions options;
  options.lazy_ = true;
  options.prefetch_limit_ = 2;
  return options;
}

void ObFilePreBuffer::ColumnRange::extract_meta()
{
  int64_t min_offset = INT64_MAX;
  int64_t max_offset = -1;
  for (int64_t i = 0; i < range_slice_set_->range_list_.count(); i++) {
    const ReadRange &range = range_slice_set_->range_list_.at(i);
    min_offset = min_offset > range.offset_ ? range.offset_ : min_offset;
    max_offset =
      range.offset_ + range.length_ > max_offset ? range.offset_ + range.length_ : max_offset;
  }
  if (max_offset > min_offset) {
    meta_.offset_ = min_offset;
    meta_.length_ = max_offset - min_offset;
  }
}

ObFilePreBuffer::ColumnRange
ObFilePreBuffer::ColumnRange::make_merge_column_range(int64_t offset, int64_t length)
{
  ObFilePreBuffer::ColumnRange range_info;
  range_info.order_ = 0;
  range_info.range_slice_set_ = nullptr;
  range_info.need_merge_ = false;
  range_info.meta_.offset_ = offset;
  range_info.meta_.length_ = length;
  return range_info;
}

bool ObFilePreBuffer::ColumnRange::is_valid_range()
{
  return (INT64_MAX != meta_.offset_) && (meta_.length_ > 0);
}

int ObFilePreBuffer::RangeCombiner::coalesce(ColumnRangeList &range_list,
                                             CoalesceColumnRangesList &range_entries)
{
  int ret = OB_SUCCESS;
  ColumnRangeList merge_range_list;
  // first merge the different column ranges
  if (OB_FAIL(coalesce_column_ranges(range_list, merge_range_list))) {
    LOG_WARN("failed to coalesce column ranges", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_range_list.count(); ++i) {
      CoalesceColumnRange column_range;
      const ColumnRange &range = merge_range_list.at(i);
      if (!range.need_merge_) {
        // multiple columns are merged into one large IO, no need to merge again
        if (OB_FAIL(column_range.push_back(
              ReadRange(range.meta_.offset_, range.meta_.length_)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else if (OB_FAIL(coalesce_each_column_ranges(range, column_range))) {
        LOG_WARN("failed to coalesce each column ranges", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(range_entries.push_back(column_range))) {
        LOG_WARN("failed to push range entry", K(ret));
      }
    }
  }
  return ret;
}

int ObFilePreBuffer::RangeCombiner::coalesce_each_column_ranges(const ColumnRange &range,
                                                                CoalesceColumnRange &column_range)
{
  int ret = OB_SUCCESS;
  ReadRangeList no_overlap_range_list;
  ColumnRangeSlices *range_slice_set = range.range_slice_set_;
  if (!range.need_merge_ || nullptr == range_slice_set || range_slice_set->range_list_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected state", K(range.need_merge_), K(range_slice_set),
             K(range_slice_set->range_list_.count()), K(ret));
  } else if (1 == range_slice_set->range_list_.count()) {
    if (OB_FAIL(column_range.push_back(range_slice_set->range_list_.at(0)))) {
      LOG_WARN("failed to push range", K(ret));
    }
  } else if (OB_FAIL(no_overlap_range_list.reserve(range_slice_set->range_list_.count()))) {
    LOG_WARN("failed to reserve", K(ret), K(range_slice_set->range_list_.count()));
  } else {
    // sort in offset
    lib::ob_sort(range_slice_set->range_list_.begin(), range_slice_set->range_list_.end(),
                 ReadRange::compare);
    // remove ranges that overlap 100%
    int64_t pre_offset = 0;
    int64_t pre_length = 0;
    ReadRangeList::iterator iter = range_slice_set->range_list_.begin();
    for (; OB_SUCC(ret) && iter != range_slice_set->range_list_.end(); iter++) {
      if (iter->offset_ >= pre_offset && iter->offset_ + iter->length_ <= pre_offset + pre_length) {
      } else if (OB_FAIL(no_overlap_range_list.push_back(*iter))) {
        LOG_WARN("failed to add range info", K(ret));
      }
      pre_offset = iter->offset_;
      pre_length = iter->length_;
    }
    if (OB_FAIL(ret)) {
    } else if (1 == no_overlap_range_list.count()) {
      if (OB_FAIL(column_range.push_back(no_overlap_range_list.at(0)))) {
        LOG_WARN("failed to push range", K(ret));
      }
    } else {
      int64_t idx = 1;
      int64_t coalesced_start = no_overlap_range_list.at(0).offset_;
      int64_t prev_range_end = coalesced_start + no_overlap_range_list.at(0).length_;
      for (; OB_SUCC(ret) && idx < no_overlap_range_list.count(); idx++) {
        const ReadRange &read_range = no_overlap_range_list.at(idx);
        const int64_t current_range_start = read_range.offset_;
        const int64_t current_range_end = current_range_start + read_range.length_;
        if (current_range_end - coalesced_start > range_size_limit_
            || current_range_start - prev_range_end > hole_size_limit_) {
          if (OB_FAIL(column_range.push_back(
                ReadRange(coalesced_start, prev_range_end - coalesced_start)))) {
            LOG_WARN("failed to push range", K(ret));
          }
          coalesced_start = current_range_start;
        }
        prev_range_end = current_range_end;
      }
      if (prev_range_end > coalesced_start) {
        if (OB_FAIL(column_range.push_back(
              ReadRange(coalesced_start, prev_range_end - coalesced_start)))) {
          LOG_WARN("failed to push range", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObFilePreBuffer::RangeCombiner::coalesce_column_ranges(ColumnRangeList &col_range_list,
                                                           ColumnRangeList &merge_range_list)
{
  int ret = OB_SUCCESS;
  ColumnRangeList no_overlap_range_list;
  if (col_range_list.count() <= 1) {
    if(OB_FAIL(merge_range_list.assign(col_range_list))) {
      LOG_WARN("failed to assign", K(ret));
    }
  } else if (OB_FAIL(no_overlap_range_list.reserve(col_range_list.count()))) {
    LOG_WARN("failed to reserve", K(ret), K(col_range_list.count()));
  } else if (OB_FAIL(merge_range_list.reserve(col_range_list.count()))) {
    LOG_WARN("failed to reserve", K(ret), K(col_range_list.count()));
  } else {
    // sort in offset
    lib::ob_sort(col_range_list.begin(), col_range_list.end(), ColumnRange::compare);
    // remove ranges that overlap 100%
    int64_t pre_offset = 0;
    int64_t pre_length = 0;
    ColumnRangeList::iterator iter = col_range_list.begin();
    for (; OB_SUCC(ret) && iter != col_range_list.end(); iter++) {
      if (iter->meta_.offset_ >= pre_offset
          && iter->meta_.offset_ + iter->meta_.length_ <= pre_offset + pre_length) {
      } else if (OB_FAIL(no_overlap_range_list.push_back(*iter))) {
        LOG_WARN("failed to add range info", K(ret));
      }
      pre_offset = iter->meta_.offset_;
      pre_length = iter->meta_.length_;
    }
    if (OB_FAIL(ret)) {
    } else if (no_overlap_range_list.count() <= 1) {
      if (OB_FAIL(merge_range_list.assign(col_range_list))) {
        LOG_WARN("failed to assign", K(ret));
      }
    } else {
      int64_t coalesced_start_idx = 0;
      int64_t idx = 1;
      int64_t coalesced_start = no_overlap_range_list.at(0).meta_.offset_;
      int64_t prev_range_end = coalesced_start + no_overlap_range_list.at(0).meta_.length_;
      for (; OB_SUCC(ret) && idx < no_overlap_range_list.count(); idx++) {
        const ColumnRange &range_info = no_overlap_range_list.at(idx);
        const int64_t current_range_start = range_info.meta_.offset_;
        const int64_t current_range_end = current_range_start + range_info.meta_.length_;
        if (current_range_end - coalesced_start > range_size_limit_
            || current_range_start - prev_range_end > hole_size_limit_) {
          // contains multiple columns
          if (idx - coalesced_start_idx > 1) {
            if (OB_FAIL(merge_range_list.push_back(ColumnRange::make_merge_column_range(
                  coalesced_start, prev_range_end - coalesced_start)))) {
              LOG_WARN("failed to add merge range info", K(ret));
            }
          } else {
            // only one column
            const ColumnRange &pre_range_info = no_overlap_range_list.at(coalesced_start_idx);
            if (OB_FAIL(merge_range_list.push_back(pre_range_info))) {
              LOG_WARN("failed to add merge range info", K(ret));
            }
          }
          coalesced_start = current_range_start;
          coalesced_start_idx = idx;
        }
        prev_range_end = current_range_end;
      }
      if (prev_range_end > coalesced_start) {
        // contains multiple columns
        if (idx - coalesced_start_idx > 1) {
          if (OB_FAIL(merge_range_list.push_back(ColumnRange::make_merge_column_range(
                coalesced_start, prev_range_end - coalesced_start)))) {
            LOG_WARN("failed to add merge range info", K(ret));
          }
        } else {
          // only one column
          const ColumnRange &pre_range_info = no_overlap_range_list.at(coalesced_start_idx);
          if (OB_FAIL(merge_range_list.push_back(pre_range_info))) {
            LOG_WARN("failed to add merge range info", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

bool ObFilePreBuffer::RangeCacheEntry::is_read_cache_range_end(const ReadRange &range) const
{
  return range.offset_ + range.length_ >= range_.offset_ + range_.length_;
}

int ObFilePreBuffer::RangeCacheEntry::wait(ObLakeTablePreBufferMetrics &metrics)
{
  int ret = OB_SUCCESS;
  if (is_waited_) {
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    if (OB_FAIL(io_handle_.wait())) {
      LOG_WARN("failed to wait io handle", K(ret));
    } else {
      is_waited_ = true;
      const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      metrics.total_io_wait_time_us_ += cost_ts;
      metrics.max_io_wait_time_us_ = MAX(metrics.max_io_wait_time_us_, cost_ts);
    }
  }
  return ret;
}


ObFilePreBuffer::~ObFilePreBuffer()
{
  destroy();
}

int ObFilePreBuffer::init(const CacheOptions &cache_options)
{
  int ret = OB_SUCCESS;
  options_ = cache_options;
  return ret;
}

int ObFilePreBuffer::init(const CacheOptions &cache_options, const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  options_ = cache_options;
  timeout_ts_ = timeout_ts;
  return ret;
}

void ObFilePreBuffer::set_timeout_timestamp(const int64_t timeout_ts)
{
  timeout_ts_ = timeout_ts;
}

int ObFilePreBuffer::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reset())) {
    LOG_WARN("failed to reset", K(ret));
  } else {
    cache_entries_.reset();
    column_range_cache_entries_.reset();
  }
  return ret;
}

int ObFilePreBuffer::reset()
{
  int ret = OB_SUCCESS;
  RangeCacheEntry *cache_entry = nullptr;
  ColumnRangeCacheEntry *column_entry = nullptr;
  for (int64_t i = 0; i < cache_entries_.count(); i++) {
    cache_entry = cache_entries_.at(i);
    // Asynchronous execution requires calling wait to wait for the execution to end.
    // Directly releasing the memory will cause asynchronous execution to access invalid
    // buf_ pointers
    if (nullptr == cache_entry) {
    } else if (nullptr != cache_entry->buf_) {
      // ignore ret
      cache_entry->wait(metrics_);
      SAFE_DELETE(cache_entry->buf_);
      cache_entry->~RangeCacheEntry();
      cache_entry = nullptr;
    }
  }
  for (int64_t i = 0; i < column_range_cache_entries_.count(); i++) {
    column_entry = column_range_cache_entries_.at(i);
    if (nullptr != column_entry) {
      column_entry->~ColumnRangeCacheEntry();
      SAFE_DELETE(column_entry);
    }
  }
  cache_entries_.reset();
  column_range_cache_entries_.reuse();
  return ret;
}

int ObFilePreBuffer::async_read_range(RangeCacheEntry &cache_range)
{
  int ret = OB_SUCCESS;
  if (nullptr != cache_range.buf_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected nullptr", K(ret));
  } else if (OB_ISNULL(cache_range.buf_ = (char *)alloc_.alloc(cache_range.range_.length_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", KR(ret));
  } else {
    const int64_t io_timeout_ms = MAX(0, (timeout_ts_ - ObTimeUtility::current_time()) / 1000);
    ObExternalReadInfo read_info(cache_range.range_.offset_, cache_range.buf_,
                                 cache_range.range_.length_, io_timeout_ms);
    if (OB_FAIL(file_reader_.async_read(read_info, cache_range.io_handle_))) {
      LOG_WARN("fail to read file", K(ret), K(cache_range));
    } else {
      ++metrics_.async_io_count_;
      metrics_.async_io_size_ += cache_range.range_.length_;
      LOG_TRACE("async read range", K(cache_range.range_));
    }
    if (OB_FAIL(ret) && nullptr != cache_range.buf_ ) {
      SAFE_DELETE(cache_range.buf_);
    }
  }
  return ret;
}

int ObFilePreBuffer::prefetch_first_range_of_each_column(ColumnRangeCacheEntryList &range_entries)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < range_entries.count(); i++) {
    RangeCacheEntry *cache_entry = nullptr;
    ColumnRangeCacheEntry *col_range_entry = range_entries.at(i);
    if (OB_ISNULL(col_range_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is null", K(ret));
    } else if (OB_FAIL(col_range_entry->pop_front(cache_entry))) {
      LOG_WARN("failed to pop front", K(ret));
    } else if (OB_FAIL(async_read_range(*cache_entry))) {
      LOG_WARN("failed to async read range", K(ret), KPC(cache_entry));
    }
  }
  return ret;
}

int ObFilePreBuffer::prefetch_column_ranges(ColumnRangeCacheEntryList &range_entries)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < range_entries.count(); i++) {
    RangeCacheEntry *cache_entry = nullptr;
    ColumnRangeCacheEntry *col_range_entry = range_entries.at(i);
    if (OB_ISNULL(col_range_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is null", K(ret));
    } else {
      while (OB_SUCC(ret) && !col_range_entry->empty()) {
        if (OB_FAIL(col_range_entry->pop_front(cache_entry))) {
          LOG_WARN("failed to pop front", K(ret));
        } else if (OB_FAIL(async_read_range(*cache_entry))) {
          LOG_WARN("failed to async read range", K(ret), KPC(cache_entry));
        }
      }
    }
  }
  return ret;
}

int ObFilePreBuffer::convert_coalesce_col_range_to_cache_entry(
  const CoalesceColumnRange &column_range, ColumnRangeCacheEntry *&column_cache_entry)
{
  int ret = OB_SUCCESS;
  RangeCacheEntry *entry = nullptr;
  int64_t range_count = column_range.count();
  char *buf = nullptr;
  if (range_count < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid range count", K(range_count), K(ret));
  } else if (OB_ISNULL(buf = (char *)alloc_.alloc(sizeof(ColumnRangeCacheEntry)
                                                  + sizeof(RangeCacheEntry) * range_count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", KR(ret));
  } else {
    column_cache_entry = new (buf) ColumnRangeCacheEntry(alloc_);
    buf += sizeof(ColumnRangeCacheEntry);
    for (int64_t i = 0; OB_SUCC(ret) && i < range_count; ++i) {
      const ReadRange &read_range = column_range.at(i);
      entry = new (buf) RangeCacheEntry();
      buf += sizeof(RangeCacheEntry);
      entry->range_ = read_range;
      entry->col_cache_entry_ = column_cache_entry;
      if (OB_FAIL(column_cache_entry->push_back(entry))) {
        LOG_WARN("failed to push entry", K(ret));
      }
    }
    if (OB_FAIL(ret) && nullptr != column_cache_entry) {
      column_cache_entry->~ColumnRangeCacheEntry();
      SAFE_DELETE(column_cache_entry);
    }
  }
  return ret;
}

int ObFilePreBuffer::convert_coalesce_column_ranges(
  const CoalesceColumnRangesList &coalesce_col_ranges,
  ColumnRangeCacheEntryList &column_range_cache_entries)
{
  int ret = OB_SUCCESS;
  int64_t N = coalesce_col_ranges.count();
  if (OB_FAIL(column_range_cache_entries.reserve(N))) {
    LOG_WARN("failed to reserve", K(ret), K(N));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      const CoalesceColumnRange &column_range = coalesce_col_ranges.at(i);
      ColumnRangeCacheEntry *column_cache_entry = nullptr;
      if (OB_FAIL(convert_coalesce_col_range_to_cache_entry(column_range, column_cache_entry))) {
        LOG_WARN("failed to convert coalesce column range to cache entry", K(ret));
      } else if (OB_FAIL(column_range_cache_entries.push_back(column_cache_entry))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObFilePreBuffer::build_cache_entries(
  const ColumnRangeCacheEntryList &column_range_cache_entries, RangeCacheEntryList &cache_entries)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_range_cache_entries.count(); ++i) {
    const ColumnRangeCacheEntry *column_cache_entry = column_range_cache_entries.at(i);
    FOREACH_X(entry, *column_cache_entry, OB_SUCC(ret))
    {
      if (OB_FAIL(cache_entries.push_back(*entry))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    cache_entries.sort(RangeCacheEntry::compare);
  }
  return ret;
}

int ObFilePreBuffer::pre_buffer(const ColumnRangeSlicesList &range_list)
{
  int ret = OB_SUCCESS;
  CoalesceColumnRangesList coalesce_ranges_list;
  if (OB_FAIL(reset())) {
    LOG_WARN("failed to reset", K(ret));
  } else if (OB_FAIL(coalesce_ranges(range_list, coalesce_ranges_list))) {
    LOG_WARN("failed to coalesce ranges", K(ret));
  } else if (OB_FAIL(
               convert_coalesce_column_ranges(coalesce_ranges_list, column_range_cache_entries_))) {
    LOG_WARN("failed to convert to column range cache entries", K(ret));
  } else if (OB_FAIL(build_cache_entries(column_range_cache_entries_, cache_entries_))) {
    LOG_WARN("failed to build cache entries", K(ret));
  } else if (options_.lazy_) {
    if (OB_FAIL(prefetch_first_range_of_each_column(column_range_cache_entries_))) {
      LOG_WARN("failed to read first range of each column", K(ret));
    }
  } else {
    if (OB_FAIL(prefetch_column_ranges(column_range_cache_entries_))) {
      LOG_WARN("failed to read all column ranges", K(ret));
    }
  }
  ++metrics_.prebuffer_count_;
  EVENT_INC(EXTERNAL_TABLE_PREBUFFER_CNT);
  return ret;
}

int ObFilePreBuffer::read(int64_t position, int64_t length, void* out)
{
  int ret = OB_SUCCESS;
  ReadRange read_range(position, length);
  // The order is guaranteed when inserting
  const RangeCacheEntryList::iterator iter =
    std::lower_bound(cache_entries_.begin(), cache_entries_.end(), read_range, RangeEntryCmp());
  if (iter != cache_entries_.end() && (*iter)->range_.contains(read_range)
      && nullptr != (*iter)->buf_) {
    ++metrics_.hit_count_;
    RangeCacheEntry *entry = (*iter);
    if (OB_FAIL(entry->wait(metrics_))) {
      LOG_WARN("failed to wait io handle", K(ret));
    } else {
      metrics_.total_read_size_ += length;
      ColumnRangeCacheEntry *column_cache_entry = entry->col_cache_entry_;
      MEMCPY(out, (char *)entry->buf_ + (position - entry->range_.offset_), length);
      if (entry->is_read_cache_range_end(read_range)) {
        if (OB_FAIL(cache_entries_.remove(iter))) {
          LOG_WARN("failed to remove");
        } else {
          alloc_.free(entry->buf_);
          entry->buf_ = nullptr;
          entry->~RangeCacheEntry();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (nullptr != column_cache_entry && options_.lazy_ && options_.prefetch_limit_ > 0) {
        int64_t num_prefetched = 0;
        while (OB_SUCC(ret) && !column_cache_entry->empty()
              && num_prefetched < options_.prefetch_limit_) {
          RangeCacheEntry *cache_entry = nullptr;
          if (OB_FAIL(column_cache_entry->pop_front(cache_entry))) {
            LOG_WARN("failed to pop fromt", K(ret));
          } else if (OB_FAIL(async_read_range(*cache_entry))) {
            LOG_WARN("failed to async read range", K(ret));
          } else {
            ++num_prefetched;
          }
        }
      }
    }
  } else {
    ++metrics_.miss_count_;
    EVENT_INC(EXTERNAL_TABLE_PREBUFFER_MISS_CNT);
    ret = OB_ENTRY_NOT_EXIST;
    LOG_TRACE("cache did not find matching cache entry", K(read_range));
  }
  LOG_TRACE("read range", K(read_range), K(ret));
  return ret;
}

int ObFilePreBuffer::coalesce_ranges(const ColumnRangeSlicesList &range_list,
                                     CoalesceColumnRangesList &range_entries)
{
  int ret = OB_SUCCESS;
  ColumnRangeList column_range_list;
  if (OB_FAIL(column_range_list.reserve(range_list.count()))) {
    LOG_WARN("failed to reserve", K(ret), K(range_list.count()));
  } else {
    // extract metadata for each column
    for (int64_t i = 0; OB_SUCC(ret) && i < range_list.count(); i++) {
      ColumnRangeSlices *slice_list = range_list.at(i);
      ColumnRange col_range(slice_list);
      col_range.extract_meta();
      if (col_range.is_valid_range() && OB_FAIL(column_range_list.push_back(col_range))) {
        LOG_WARN("failed to add column range meta", K(ret));
      }
    }
    dump_raw_ranges(const_cast<ColumnRangeSlicesList &>(range_list));
    RangeCombiner combiner(options_.hole_size_limit_, options_.range_size_limit_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(combiner.coalesce(column_range_list, range_entries))) {
      LOG_WARN("failed to coalesce column ranges", K(ret));
    } else {
      dump_coalesce_ranges(range_entries);
    }
  }
  return ret;
}

int ObFilePreBuffer::register_metrics(ObLakeTableReaderProfile &reader_profile,
                                      const ObString &label)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader_profile.register_metrics(&metrics_, label))) {
    LOG_WARN("failed to register metrics", K(ret));
  }
  return ret;
}

void ObFilePreBuffer::dump_raw_ranges(ColumnRangeSlicesList &range_list)
{
  LOG_TRACE("dump raw ranges");
  dump_list<ColumnRangeSlicesList>(range_list);
}

void ObFilePreBuffer::dump_coalesce_ranges(CoalesceColumnRangesList &range_entries)
{
  LOG_TRACE("dump coalesce ranges");
  dump_list<CoalesceColumnRangesList>(range_entries);
}

#undef SAFE_DELETE

}
}
