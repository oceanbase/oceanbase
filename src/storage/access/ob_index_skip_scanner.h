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

#ifndef OB_ACCESS_ROWKEY_PREFIX_FILTER_H
#define OB_ACCESS_ROWKEY_PREFIX_FILTER_H

#include "storage/blocksstable/ob_datum_range.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace blocksstable
{
class ObIMicroBlockRowScanner;
class ObIndexBlockRowScanner;
struct ObMicroIndexInfo;
}

namespace memtable
{
class ObMemtableSkipScanIterator;
}

namespace storage
{
class ObITableReadInfo;
class ObIndexSkipScanFactory;
enum class ObIndexSkipNodeState : int8_t
{
  INVALID_STATE = 0,
  PREFIX_UNCERTAIN = 1,
  // the following two state means endkey can be skipped
  // but as there is no startkey, the entire node cannot be determined,
  // will be determined later
  PREFIX_PENDDING_LEFT = 2,
  PREFIX_PENDDING_RIGHT = 3,
  // the following two state means the entire node can be skipped
  PREFIX_SKIPPED_LEFT = 4,
  PREFIX_SKIPPED_RIGHT = 5,
};

struct ObIndexSkipState
{
  ObIndexSkipState() : state_(0) {}
  OB_INLINE void reset()
  {
    state_ = 0;
  }
  OB_INLINE bool is_invalid() const
  {
    return ObIndexSkipNodeState::INVALID_STATE == static_cast<ObIndexSkipNodeState>(node_state_);
  }
  OB_INLINE bool is_skipped() const
  {
    return ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT == static_cast<ObIndexSkipNodeState>(node_state_) ||
           ObIndexSkipNodeState::PREFIX_SKIPPED_RIGHT == static_cast<ObIndexSkipNodeState>(node_state_);
  }
  OB_INLINE int64_t range_idx() const
  {
    return range_idx_;
  }
  OB_INLINE void set_state(const int64_t range_idx, const ObIndexSkipNodeState state)
  {
    range_idx_ = range_idx;
    node_state_ = static_cast<int8_t>(state);
  }
  OB_INLINE void inc_range_idx()
  {
    range_idx_++;
  }
  OB_INLINE void set_range_finished(const bool finish)
  {
    range_finished_ = finish;
  }
  OB_INLINE bool is_range_finished() const
  {
    return range_finished_;
  }
  OB_INLINE bool is_skipped_right() const
  {
    return ObIndexSkipNodeState::PREFIX_SKIPPED_RIGHT == static_cast<ObIndexSkipNodeState>(node_state_);
  }
  OB_INLINE bool is_skipped_left() const
  {
    return ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT == static_cast<ObIndexSkipNodeState>(node_state_);
  }
  OB_INLINE bool is_pendding_right() const
  {
    return ObIndexSkipNodeState::PREFIX_PENDDING_RIGHT == static_cast<ObIndexSkipNodeState>(node_state_);
  }
  OB_INLINE bool is_pendding_left() const
  {
    return ObIndexSkipNodeState::PREFIX_PENDDING_LEFT == static_cast<ObIndexSkipNodeState>(node_state_);
  }
  TO_STRING_KV(K_(range_idx), K_(node_state), K_(range_finished), K_(state));
  union {
    struct {
      int64_t range_idx_: 32;
      int64_t node_state_ : 8;
      int64_t range_finished_: 1;
      int64_t reserved_: 23;
    };
    int64_t state_;
  };
};

enum class ObIndexSkipCtlStrategyType : int8_t
{
  RATIO_BASED = 0,
  COUNT_BASED = 1,
  MAX_STATE
};

class ObIndexSkipCtlStrategy
{
public:
  ObIndexSkipCtlStrategy(const ObIndexSkipCtlStrategyType type);
  ~ObIndexSkipCtlStrategy() = default;
  void reset();
  int check_disabled(const ObIndexSkipState &state, bool &disabled) const;
  OB_INLINE void add_total_row_count(const int64_t row_count)
  {
    total_row_count_ += row_count;
  }
  OB_INLINE void add_skipped_row_count(const int64_t row_count)
  {
    skipped_row_count_ += row_count;
  }
  TO_STRING_KV(K_(strategy_type), K_(total_row_count), K_(skipped_row_count),
               K_(final_count_threshold), K_(final_ratio_threshold));
private:
  static constexpr double SKIP_RATIO_THRESHOLD = 0.8;
  static const int64_t RATIO_BASED_MIN_CHECK_COUNT = 1 << 6;
  static const int64_t RATIO_BASED_MAX_DISABLE_COUNT = 1 << 18;
  static const int64_t COUNT_BASED_MAX_DISABLE_COUNT = 1 << 11;
  ObIndexSkipCtlStrategyType strategy_type_;
  int64_t total_row_count_;
  int64_t skipped_row_count_;
  int64_t final_count_threshold_;
  double final_ratio_threshold_;
};

class ObIndexSkipScanner
{
public:
  ObIndexSkipScanner(const bool is_for_memtable, const blocksstable::ObStorageDatumUtils &datum_utils);
  ~ObIndexSkipScanner();
  void reuse();
  void reset();
  int init(
      const bool is_reverse_scan,
      const int64_t prefix_cnt,
      const blocksstable::ObDatumRange &scan_range,
      const blocksstable::ObDatumRange &skip_range,
      const ObITableReadInfo &read_info,
      common::ObIAllocator &stmt_allocator);
  int switch_info(
      const bool is_reverse_scan,
      const int64_t prefix_cnt,
      const blocksstable::ObDatumRange &scan_range,
      const blocksstable::ObDatumRange &skip_range,
      const ObITableReadInfo &read_info,
      common::ObIAllocator &stmt_allocator);
  int skip(
      blocksstable::ObMicroIndexInfo &index_info,
      ObIndexSkipState &prev_state,
      ObIndexSkipState &state);
  // for reverse scan
  int skip(
      blocksstable::ObMicroIndexInfo &index_info,
      const ObIndexSkipState &next_state,
      ObIndexSkipState &state);
  int skip(
      blocksstable::ObIMicroBlockRowScanner &micro_scanner,
      blocksstable::ObMicroIndexInfo &index_info,
      const bool first = false);
  int skip(
      memtable::ObMemtableSkipScanIterator &mem_iter,
      bool &is_end,
      const bool first = false);
  OB_INLINE common::ObIAllocator *get_stmt_alloc()
  {
    return stmt_alloc_;
  }
  OB_INLINE bool is_opened() const
  {
    return nullptr != skip_range_;
  }
  OB_INLINE bool is_disabled() const
  {
    return is_disabled_;
  }
  OB_INLINE bool is_prefix_filled() const
  {
    return is_prefix_filled_;
  }
  OB_INLINE const blocksstable::ObDatumRange &get_complete_range() const
  {
    return complete_range_;
  }
  OB_INLINE void set_skip_scan_factory(ObIndexSkipScanFactory *skip_scan_factory)
  {
    skip_scan_factory_ = skip_scan_factory;
  }
  TO_STRING_KV(K_(is_inited),
               K_(is_disabled),
               K_(is_reverse_scan),
               K_(is_prefix_filled),
               K_(is_scan_range_complete),
               K_(prefix_cnt),
               K_(micro_start),
               K_(micro_last),
               K_(micro_current),
               K_(prefix_range_idx),
               K_(index_skip_state),
               K_(index_skip_strategy),
               KP_(scan_range),
               KP_(skip_range),
               KP_(range_datums),
               KP_(read_info),
               KP_(skip_scan_factory),
               KP_(stmt_alloc));
private:
  int check_and_preprocess(
      const bool first,
      blocksstable::ObIMicroBlockRowScanner &micro_scanner,
      ObIndexSkipState &state);
  int prepare_ranges(
      const bool is_reverse_scan,
      const int64_t prefix_cnt,
      const blocksstable::ObDatumRange &scan_range,
      const blocksstable::ObDatumRange &skip_range,
      const int64_t schema_rowkey_cnt,
      ObIAllocator &alloc);
  int prepare_range_datums(
      const bool is_reverse_scan,
      const int64_t prefix_cnt,
      const blocksstable::ObDatumRange &scan_range,
      const blocksstable::ObDatumRange &skip_range,
      const int64_t schema_rowkey_cnt,
      ObIAllocator &alloc);
  int check_prefix_changed(const blocksstable::ObDatumRow *row, bool &changed);
  int check_after_range_updated(
      const blocksstable::ObDatumRange &range,
      const blocksstable::ObIMicroBlockRowScanner &micro_scanner,
      bool &skipped);
  int check_scan_range(
      const blocksstable::ObDatumRow *row,
      bool &is_start_equal,
      bool &is_end_equal);
  int update_complete_range(
      const blocksstable::ObDatumRow *row,
      const bool is_for_memtable = false,
      const bool is_start_equal = false,
      const bool is_end_equal = false);
  int update_prefix_range(const bool is_for_memtable = false);
  int skip_in_micro(
      blocksstable::ObIMicroBlockRowScanner &micro_scanner,
      blocksstable::ObMicroIndexInfo &index_info,
      const blocksstable::ObDatumRange &range,
      const bool is_prefix_range,
      bool &has_data);
  int update_state_and_strategy(
       blocksstable::ObIMicroBlockRowScanner &micro_scanner,
       const bool is_prefix_range,
       const bool has_data,
       const bool range_covered,
      ObIndexSkipState &state);
  int check_disabled();
  OB_INLINE ObIndexSkipNodeState get_default_skip_state() const
  {
    return is_reverse_scan_ ? ObIndexSkipNodeState::PREFIX_SKIPPED_RIGHT : ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT;
  }
  const static int64_t CHECK_INTERRUPT_RANGE_CNT = 100;
  common::ObArenaAllocator range_alloc_;
  common::ObArenaAllocator prefix_alloc_;
  bool is_inited_;
  bool is_disabled_;
  bool is_reverse_scan_;
  bool is_prefix_filled_;
  bool is_scan_range_complete_;
  int64_t prefix_cnt_;
  int64_t micro_start_;
  int64_t micro_last_;
  int64_t micro_current_;
  int64_t prefix_range_idx_;
  ObIndexSkipState index_skip_state_;
  ObIndexSkipCtlStrategy index_skip_strategy_;
  const blocksstable::ObStorageDatumUtils &datum_utils_;
  const blocksstable::ObDatumRange *scan_range_;
  const blocksstable::ObDatumRange *skip_range_;
  blocksstable::ObDatumRange complete_range_;
  blocksstable::ObDatumRange prefix_range_;
  blocksstable::ObStorageDatum *range_datums_;
  const ObITableReadInfo *read_info_;
  ObIndexSkipScanFactory *skip_scan_factory_;
  common::ObIAllocator *stmt_alloc_;
};

class ObIndexSkipScanFactory
{
public:
  static int build_index_skip_scanner(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const blocksstable::ObDatumRange *range,
      ObIndexSkipScanner *&skip_scanner,
      const bool is_for_memtable = false);
  static void destroy_index_skip_scanner(ObIndexSkipScanner *&skip_scanner);

  ObIndexSkipScanFactory();
  ~ObIndexSkipScanFactory();
  void reuse();
  void reset();
  int add_skip_scanner(ObIndexSkipScanner *skip_scanner);
  int set_pending_disabled(
      const bool is_reverse_scan,
      const int64_t prefix_cnt,
      const blocksstable::ObStorageDatumUtils &datum_utils);
  int get_newest_prefix_key(const blocksstable::ObDatumRowkey *&newest_prefix_key) const;
  OB_INLINE bool is_pending_disabled() const
  {
    return is_pending_disabled_;
  }
  TO_STRING_KV(K_(is_pending_disabled), K_(newest_prefix_key), K_(skip_scanners));
private:
  common::ObArenaAllocator alloc_;
  bool is_pending_disabled_;
  blocksstable::ObDatumRowkey newest_prefix_key_;
  common::ObSEArray<ObIndexSkipScanner*, 8> skip_scanners_;
};

} // namespace storage
} // namespace oceanbase

#endif // OB_ACCESS_ROWKEY_PREFIX_FILTER_H