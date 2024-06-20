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

#ifndef OCEANBASE_STORAGE_OB_SAMPLE_FILTER_
#define OCEANBASE_STORAGE_OB_SAMPLE_FILTER_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_i_tablet_scan.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
namespace oceanbase
{
namespace storage
{
const int64_t OB_INVALID_ROW_ID = -1;
struct ObRowIdRange
{
public:
  ObRowIdRange() { reset(); }
  ObRowIdRange(const int64_t start, const int64_t end)
  {
    start_row_id_ = start;
    end_row_id_ = end;
  }
  OB_INLINE void set(const int64_t start, int64_t end)
  {
    start_row_id_ = start;
    end_row_id_ = end;
  }
  OB_INLINE void set_begin(const int64_t start) { start_row_id_ = start; }
  OB_INLINE void set_end(const int64_t end) { end_row_id_ = end; }
  OB_INLINE int64_t begin() const { return start_row_id_; }
  OB_INLINE int64_t end() const { return end_row_id_; }
  OB_INLINE void reset() { start_row_id_ = OB_INVALID_ROW_ID; end_row_id_ = OB_INVALID_ROW_ID; }
  OB_INLINE bool is_valid() const { return OB_INVALID_ROW_ID != start_row_id_ && end_row_id_ >= start_row_id_; }
  OB_INLINE int64_t get_row_count() const { return end_row_id_ - start_row_id_ + 1; }
  TO_STRING_KV(K_(start_row_id), K_(end_row_id));
  int64_t start_row_id_;
  int64_t end_row_id_;
};

struct ObIndexRowIdHandle {
public:
  ObIndexRowIdHandle()
    : start_(0),
      offset_(0)
  {}
  ObIndexRowIdHandle(const int64_t start, const int64_t offset)
    : start_(start),
      offset_(offset)
  {}
  TO_STRING_KV(K_(start), K_(offset));
public:
  int64_t start_;
  int64_t offset_;
};

struct ObSampleIntervalInfo
{
public:
  ObSampleIntervalInfo()
    : start_(0),
      interval_length_(0)
  {}
  ObSampleIntervalInfo(const int64_t start, const int64_t interval_length)
    : start_(start),
      interval_length_(interval_length)
  {}
  TO_STRING_KV(K_(start), K_(interval_length));
public:
  int64_t start_;
  int64_t interval_length_;
};

struct ObSampleIntervalParser
{
public:
  ObSampleIntervalParser(const double p, const int64_t seed)
    : interval_id_(0),
      start_(0),
      interval_length_(0),
      left_(0),
      right_(0),
      count_(0),
      percent_(p),
      seed_(seed)
  {}
  OB_INLINE void parse(const int64_t row_num, const ObSampleIntervalInfo &interval_info)
  {
    start_ = interval_info.start_;
    interval_length_ = interval_info.interval_length_;
    interval_id_ = (row_num - start_) / interval_length_;
    count_ = std::floor(static_cast<double>((interval_id_ + 1) * interval_length_) * percent_ / 100)
              - std::floor(static_cast<double>(interval_id_ * interval_length_) * percent_ / 100);
    uint64_t hash_value = murmurhash(&interval_id_, sizeof(interval_id_), static_cast<uint64_t>(seed_));
    uint64_t offset = interval_length_ <= count_ ? 0 : hash_value % (interval_length_ - count_ + 1);
    left_ = static_cast<int64_t>(interval_length_ * interval_id_ + offset) + start_;
    right_ = left_ + count_ - 1;
  }
  OB_INLINE int64_t next_interval()
  {
    return (interval_id_ + 1) * interval_length_ + start_;
  }
  TO_STRING_KV(K_(interval_id),
               K_(start),
               K_(interval_length),
               K_(left),
               K_(right),
               K_(count),
               K_(percent),
               K_(seed));
public:
  int64_t interval_id_;
  int64_t start_;
  int64_t interval_length_;
  int64_t left_;        // sample left point in current interval
  int64_t right_;       // sample right point in current interval
  int64_t count_;       // sample row count in current interval
  double percent_;
  int64_t seed_;
};

struct ObSampleBlockStatistic
{
public:
  ObSampleBlockStatistic()
    : block_count_(0),
      can_skip_block_(true),
      has_filtered_(false)
  {}
  static const int CHECK_COUNT_THRESHOLD = 100;
  OB_INLINE void reset()
  {
    if (block_count_ > 0) {
      block_count_ = 0;
      can_skip_block_ = true;
      has_filtered_ = false;
    }
  }
  OB_INLINE void inc_block_count(blocksstable::ObMicroIndexInfo &index_info, const bool need_reset)
  {
    if (index_info.is_data_block()) {
      if (need_reset) { // has crossed the interval expansion point
        reset();
      } else if (block_count_ == CHECK_COUNT_THRESHOLD) {
        can_skip_block_ = has_filtered_;
      }
      block_count_++;
    }
  }
  OB_INLINE void update_filter_status()
  {
    can_skip_block_ = true;
    has_filtered_ = true;
  }
  TO_STRING_KV(K_(block_count), K_(can_skip_block), K_(has_filtered));
public:
  int64_t block_count_;
  bool can_skip_block_;
  bool has_filtered_;
};

class ObPushdownSampleFilterNode : public sql::ObPushdownFilterNode
{
public:
  ObPushdownSampleFilterNode(common::ObIAllocator &alloc)
      : sql::ObPushdownFilterNode(alloc, sql::PushdownFilterType::SAMPLE_FILTER)
  {}
};

class ObSampleFilterExecutor : public sql::ObPushdownFilterExecutor
{
public:
  ObSampleFilterExecutor(
      common::ObIAllocator &alloc,
      ObPushdownSampleFilterNode &filter,
      sql::ObPushdownOperator &op);
  virtual ~ObSampleFilterExecutor();
  void reset();
  void reuse();
  int init (
      const common::SampleInfo &sample_info,
      const bool is_reverse_scan,
      common::ObIAllocator *allocator);
  int build_row_id_handle(
      const int16_t height,
      const int32_t index_handle_cnt,
      const int32_t data_handle_cnt);
  int increase_row_num(const int64_t count);
  int update_row_num_after_blockscan();
  int check_filtered_after_fuse(bool &filtered);
  int check_sample_block(
      blocksstable::ObMicroIndexInfo &index_info,
      const int64_t level,
      const int64_t parent_fetch_idx,
      const int64_t child_prefetch_idx,
      const bool has_lob_out);
  int apply_sample_filter( // for row store
      sql::PushdownFilterInfo &filter_info,
      const bool is_major,
      common::ObBitmap &result_bitmap);
  int apply_sample_filter( // for column store
      const ObCSRange &range,
      common::ObBitmap &result_bitmap);
  int64_t get_row_num() const { return row_num_; } // for debug
  OB_INLINE virtual common::ObIArray<uint64_t> &get_col_ids() override
  {
    return filter_.get_col_ids();
  }
  OB_INLINE sql::ObPushdownOperator & get_op() override
  {
    OB_ASSERT(false); // op_ maybe null, can not get_op in sample filter
    return op_;
  }
  OB_INLINE void set_block_row_range(
      const int64_t fetch_idx,
      const int64_t current,
      const int64_t last)
  {
    if (!is_reverse_scan_) {
      block_row_range_.set(get_data_start_id(fetch_idx) + current, get_data_start_id(fetch_idx) + last);
    } else {
      block_row_range_.set(get_data_start_id(fetch_idx) + last, get_data_start_id(fetch_idx) + current);
    }
  }
  OB_INLINE common::ObIAllocator *get_allocator() { return allocator_; }
  OB_INLINE int init_evaluated_datums() { return OB_SUCCESS; }
  TO_STRING_KV(K_(is_inited), K_(is_reverse_scan), K_(row_num), K_(interval_infos),
               K_(percent), K_(seed), K_(pd_row_range), K_(block_row_range),
               K_(index_tree_height), K_(index_prefetch_depth), K_(data_prefetch_depth),
               K_(boundary_point), K_(filter_state), K_(filter),
               KP_(data_row_id_handle), KP_(index_row_id_handle), KP_(allocator));
private:
  int init_sample_segment_length(const double percent);
  int update_pd_row_range(const int64_t start, const int64_t end, const bool in_cg = false);
  void reset_row_id_handle();
  int parse_interval_info(const int64_t row_num, ObSampleIntervalParser &interval_parser) const;
  int check_single_row_filtered(const int64_t row_num, bool &filtered);
  int64_t get_range_sample_count(const int64_t start_row_num, const int64_t end_row_num, int64_t &sample_count) const;
  int check_range_filtered(blocksstable::ObMicroIndexInfo &index_info, const int64_t start_row_num);
  int update_row_id_handle(
      const int64_t level,
      int32_t parent_fetch_idx,
      int64_t child_prefetch_idx,
      const int64_t row_count);
  int set_sample_bitmap(
      const int64_t start_row_num,
      const int64_t row_count,
      ObBitmap &result_bitmap);
  OB_INLINE bool can_sample_skip(blocksstable::ObMicroIndexInfo &index_info, const bool has_lob_out)
  {
    return block_statistic_.can_skip_block_
            && !index_info.is_left_border() && !index_info.is_right_border()
            && !index_info.is_filter_always_false();
  }
  OB_INLINE void reset_pushdown_ranges()
  {
    pd_row_range_.reset();
    block_row_range_.reset();
  }
  OB_INLINE int64_t get_data_start_id(const int64_t micro_data_idx) const
  {
    return data_row_id_handle_[micro_data_idx % data_prefetch_depth_];
  }
  OB_INLINE int64_t get_sample_count(const int64_t start_iid, const int64_t end_iid, const int64_t interval_length) const
  {
    return std::floor(static_cast<double>((end_iid + 1) * interval_length) * percent_ / 100.0)
              - std::floor(static_cast<double>(start_iid * interval_length) * percent_ / 100.0);
  }
  OB_INLINE bool is_valid(const int64_t row_id) const
  {
    return row_id != OB_INVALID_ROW_ID;
  }
  static const int64_t EXPAND_INTERVAL_START = 1000000;
  static const int64_t EXPAND_INTERVAL_LENGTH = 1024;
  static const int64_t EXPAND_INTERVAL_INDEX = 1;
private:
  int64_t row_num_;
  int64_t seed_;
  double percent_;
  int64_t boundary_point_;          // fast to judge whether one single row should be filtered
  ObRowIdRange pd_row_range_;       // the row id range in one pushdown process
  ObRowIdRange block_row_range_;    // current micro block range to be judged when sample filter pushdown on major sstable
  ObSEArray<ObSampleIntervalInfo, 2> interval_infos_;
  int64_t *data_row_id_handle_;
  ObIndexRowIdHandle *index_row_id_handle_;
  ObPushdownSampleFilterNode &filter_;
  common::ObIAllocator *allocator_;
  ObSampleBlockStatistic block_statistic_;
  int32_t index_prefetch_depth_;
  int32_t data_prefetch_depth_;
  int16_t index_tree_height_;
  bool filter_state_;               // fast to judge whether one single row should be filtered
  bool is_reverse_scan_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSampleFilterExecutor);
};

class ObRowSampleFilter {
public:
  ObRowSampleFilter();
  virtual ~ObRowSampleFilter();
  void reuse()
  {
    if (OB_NOT_NULL(sample_filter_)) {
      static_cast<ObSampleFilterExecutor*>(sample_filter_)->reuse();
    }
  }
  int init(
      const common::SampleInfo &sample_info,
      sql::ObPushdownOperator *op,
      const bool is_reverse_scan,
      common::ObIAllocator *allocator);
  int combine_to_filter_tree(sql::ObPushdownFilterExecutor *&root_filter);
  OB_INLINE sql::ObPushdownFilterExecutor *get_sample_executor()
  {
    return sample_filter_;
  }
  OB_INLINE common::ObIAllocator *get_allocator()
  {
    return allocator_;
  }
  TO_STRING_KV(K_(is_inited),
               KP_(sample_filter),
               KP_(sample_node),
               KP_(filter),
               KP(filter_node_),
               KP_(allocator));
private:
  sql::ObPushdownFilterExecutor *sample_filter_;
  sql::ObPushdownFilterNode *sample_node_;
  sql::ObPushdownFilterExecutor *filter_;
  sql::ObPushdownFilterNode *filter_node_;
  common::ObIAllocator *allocator_;
  bool is_inited_;
};

class ObRowSampleFilterFactory {
public:
  static int build_sample_filter(
      const common::SampleInfo &sample_info,
      ObRowSampleFilter *&sample_filter,
      sql::ObPushdownOperator *op,
      const bool is_reverse_scan,
      common::ObIAllocator *allocator);
  static void destroy_sample_filter(ObRowSampleFilter *&sample_filter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SAMPLE_FILTER_
