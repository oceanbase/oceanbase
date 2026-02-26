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

#ifndef OB_PARTITION_RANGE_SPLITER_H
#define OB_PARTITION_RANGE_SPLITER_H

#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_access_context.h"
#include "ob_storage_struct.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_heap.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/container/ob_array_array.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "access/ob_index_sstable_estimator.h"

namespace oceanbase
{
namespace compaction
{
class ObTabletMergeCtx;
}
namespace memtable
{
class ObIMemtableCtxFactory;
}
namespace blocksstable
{
class ObDataMacroBlockMeta;
class ObSSTableSecMetaIterator;
}
namespace storage
{

struct ObRangePrecision
{
  static constexpr int64_t DEFAULT_DYNAMIC_RANGE_PRECISION_HINT = 0;

  ObRangePrecision() : range_precision_(DEFAULT_DYNAMIC_RANGE_PRECISION_HINT) {}
  ObRangePrecision(const int64_t range_precision) : range_precision_(range_precision) {}

  OB_INLINE bool is_valid() const
  {
    return (range_precision_ > 0 && range_precision_ <= 10000)
           || range_precision_ == DEFAULT_DYNAMIC_RANGE_PRECISION_HINT;
  }

  OB_INLINE int recalc_range_precision(const int64_t range_count)
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(!is_valid() || range_count <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid range precision", KR(ret), K(range_precision_), K(range_count));
    } else {
      if (range_precision_ == DEFAULT_DYNAMIC_RANGE_PRECISION_HINT) {
        const int64_t need_sample_range_limit = 20;
        range_precision_ = range_count > need_sample_range_limit
                               ? need_sample_range_limit * 10000 / range_count
                               : 10000;
      }

      if (range_precision_ <= 0) {
        range_precision_ = 1;
      }

      if (range_precision_ > 10000) {
        range_precision_ = 10000;
      }
    }

    return ret;
  }

  OB_INLINE int64_t get_sample_step() const { return max(1, 10000 / max(1, range_precision_)); }

  TO_STRING_KV(K_(range_precision));

  int64_t range_precision_;
};

class ObIMultiRangeEstimateContext
{
public:
  /**
   * @brief Operation types that can be performed during the traversal of an index block tree.
   */
  enum ObTraverserOperationType : uint8_t
  {
    NOTHING,          ///< No operation is needed
    GOTO_NEXT_LEVEL,  ///< Continue traversal by going to next level
  };

  virtual ~ObIMultiRangeEstimateContext() = default;

  virtual bool is_valid() const = 0;

  virtual bool is_ended() const = 0;

  virtual const ObDatumRange &get_curr_range() const = 0;

  virtual int64_t get_curr_range_idx() const = 0;

  virtual int64_t get_ranges_count() const = 0;

  /**
   * @brief Switch to next range. This method is called by tree traverser.
   */
  virtual int next_range() = 0;

  /**
   * @brief Callback function when a leaf node is during traversal
   */
  virtual int on_leaf_node(const ObMicroIndexInfo &index_row, const ObPartitionEst &est) = 0;

  /**
   * @brief Callback function when a inner node is during traversal
   */
  virtual int on_inner_node(const ObMicroIndexInfo &index_row,
                            const bool is_coverd_by_range,
                            ObTraverserOperationType &opertion) = 0;

  /**
   * @brief Callback function when we cann't goto next level because of performance
   */
  virtual int on_node_estimate(const ObMicroIndexInfo &index_row, const bool is_coverd_by_range) = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObIndexBlockTreeTraverser
{
public:
  // If the row count of data micro block is larger than below limit,
  // we should open this micro block to get more precise estimate result
  static constexpr int64_t OPEN_DATA_MICRO_BLOCK_ROW_LIMIT = 65536;

  static constexpr int64_t OPEN_INDEX_MICRO_BLOCK_LIMIT = 100;

  ObIndexBlockTreeTraverser() : is_inited_(false) {}

  int init(ObSSTable &sstable, const ObITableReadInfo &index_read_info);

  void reuse();

  int traverse(ObIMultiRangeEstimateContext &context,
               const int64_t open_index_micro_block_limit = OPEN_INDEX_MICRO_BLOCK_LIMIT);

  OB_INLINE bool is_inited() const { return is_inited_; }

  OB_INLINE const ObSSTable *get_sstable() const { return sstable_; }

  OB_INLINE const ObITableReadInfo *get_read_info() const { return read_info_; }

  OB_INLINE ObIAllocator &get_allocator() { return allocator_; }

  TO_STRING_KV(KPC_(read_info), KPC_(context), KPC_(sstable));

private:
  class TreeNodeContext
  {
  public:
    int init(const ObMicroBlockData &block_data,
             const ObMicroIndexInfo *micro_index_info,
             ObIndexBlockTreeTraverser &traverser);

    int open(const ObMicroBlockData &block_data, const ObMicroIndexInfo *micro_index_info);

    void reuse();

    int locate_range(const ObDatumRange &range, int64_t &index_row_count);

    int get_next_index_row(ObMicroIndexInfo &idx_block_row);
  private:
    ObArenaAllocator allocator_;
    ObIndexBlockRowScanner scanner_;
  };

  struct PathInfo
  {
    PathInfo() : path_count_(0), left_most_(false), right_most_(false), in_middle_(false) {}
    PathInfo(const int64_t path_count, const int64_t path_idx, const PathInfo &father_path_info)
        : path_count_(path_count), left_most_(path_idx == 0 && (!father_path_info.valid() || father_path_info.left_most_)),
          right_most_(path_idx == path_count - 1 && (!father_path_info.valid() || father_path_info.right_most_)),
          in_middle_(!left_most_ && !right_most_)
    {
    }

    OB_INLINE bool valid() const { return path_count_ != 0; }
    OB_INLINE void reset() { *this = PathInfo(); }

    TO_STRING_KV(K_(path_count), K_(left_most), K_(right_most), K_(in_middle));

    int64_t path_count_;
    bool left_most_;
    bool right_most_;
    bool in_middle_;
  };

  class PathNodeCaches
  {
  public:
    constexpr static int64_t MAX_CACHE_LEVEL = 8;

    struct CacheNode
    {
      CacheNode() : is_inited_(false) {}

      void reuse()
      {
        context_.reuse();
      }

      TO_STRING_KV(K_(key), K_(micro_handle), K_(micro_data), K_(is_inited));

      ObMicroBlockCacheKey key_;
      ObMicroBlockDataHandle micro_handle_;
      ObMicroBlockData micro_data_;
      ObMacroBlockReader macro_block_reader_;
      TreeNodeContext context_;
      bool is_inited_;
    };

    int load_root(const ObMicroBlockData &block_data, ObIndexBlockTreeTraverser &traverser);

    int find(const int64_t level,
            const ObMicroIndexInfo *micro_index_info,
            bool &is_in_cache);

    int get(const int64_t level,
            const ObMicroIndexInfo *micro_index_info,
            ObIndexBlockTreeTraverser &traverser,
            CacheNode *&cache_node,
            bool &is_in_cache);

    int prefetch(const ObMicroIndexInfo &micro_index_info, ObMicroBlockDataHandle &micro_handle);

  private:
    ObArenaAllocator allocator_;
    CacheNode caches_[MAX_CACHE_LEVEL];
  };

  int inner_node_traverse(const ObMicroIndexInfo *micro_index_info,
                          const PathInfo &path_info,
                          const int64_t level,
                          const double open_index_micro_block_limit);

  int leaf_node_traverse(const ObMicroIndexInfo &micro_index_info,
                         const PathInfo &path_info,
                         const int64_t level);

  int goto_next_level_node(const ObMicroIndexInfo &micro_index_info,
                           const PathInfo &path_info,
                           const int64_t level,
                           double open_index_micro_block_limit);

  int handle_overflow_ranges();

  int is_range_cover_node_end_key(const ObDatumRange &range,
                                  const ObMicroIndexInfo &micro_index_info,
                                  bool &is_over);

  ObArenaAllocator allocator_;
  ObSSTable *sstable_;
  const ObITableReadInfo *read_info_;
  ObIMultiRangeEstimateContext *context_;

  PathNodeCaches path_caches_;

  // statistics
  int64_t visited_node_count_;
  int64_t visited_macro_node_count_;
  double avg_range_visited_node_cnt_;
  double remain_can_visited_node_cnt_;

  bool is_inited_;
};

/**
 * @brief Pair the origin store range and datum range.
 * used for sort and find corresponding store range
 */
struct ObPairStoreAndDatumRange
{
  ObPairStoreAndDatumRange() : origin_store_range_(nullptr) {}

  TO_STRING_KV(KPC_(origin_store_range), K_(datum_range));

  const ObStoreRange *origin_store_range_;
  ObDatumRange datum_range_;
};

/**
 * @brief Pair the datum_range and row_count.
 */
struct ObRangeInfo
{
  ObRangeInfo() = default;
  ObRangeInfo(const ObDatumRange *range) : range_(range), row_count_(0), macro_block_count_(0) {}

  TO_STRING_KV(KPC_(range), K_(row_count), K_(macro_block_count));

  const ObDatumRange *range_;
  int64_t row_count_;
  int64_t macro_block_count_;
};

/**
 * @brief Compare range with only startkey.
 * The ranges do not overlap.
 * Therefore, sorting the ranges from smallest to largest only requires sorting by the start key.
 */
struct ObRangeCompartor
{
  ObRangeCompartor(const ObITableReadInfo &read_info)
      : read_info_(&read_info), sort_ret_(OB_SUCCESS)
  {
  }

  bool operator()(const ObDatumRange &a, const ObDatumRange &b) const
  {
    int ret = 0, cmp_ret = 0;
    if (OB_FAIL(
            a.get_start_key().compare(b.get_start_key(), read_info_->get_datum_utils(), cmp_ret))) {
      sort_ret_ = ret;
      STORAGE_LOG(WARN, "Fail to compare range start key", KR(ret));
    }
    return cmp_ret < 0;
  }

  bool operator()(const ObRangeInfo &a, const ObRangeInfo &b) const
  {
    return operator()(*a.range_, *b.range_);
  }

  bool operator()(const ObPairStoreAndDatumRange &a, const ObPairStoreAndDatumRange &b) const
  {
    return operator()(a.datum_range_, b.datum_range_);
  }

  template <typename Range>
  static int is_sorted(const ObIArray<Range> &ranges, ObRangeCompartor &compartor, bool &is_sorted);

  const ObITableReadInfo *read_info_;
  mutable int sort_ret_;
};

/**
 * @brief The context for multi range row estimate
 */
class ObMultiRangeRowEstimateContext : public ObIMultiRangeEstimateContext
{
public:
  // if a node row count is less than 1/8 of the current range row count
  // we can just estimate the node row count instead of open the next level index micro block
  static constexpr int64_t SHOULD_ESTIMATE_RATIO = 8;

  ObMultiRangeRowEstimateContext() : curr_range_idx_(0), is_inited_(false) {}

  virtual ~ObMultiRangeRowEstimateContext() = default;

  // it will create an new array which reference ranges and sort it by start key
  int init(const ObIArray<ObPairStoreAndDatumRange> &ranges,
           const ObITableReadInfo &read_info,
           const bool need_sort = true,
           const ObRangePrecision &range_precision = ObRangePrecision(10000),
           const bool can_goto_micro_level = false);

  bool is_valid() const override { return is_inited_; }

  bool is_ended() const override { return curr_range_idx_ >= ranges_.count(); }

  const ObDatumRange &get_curr_range() const override
  {
    return *ranges_.at(curr_range_idx_).range_;
  }

  int64_t get_curr_range_idx() const override { return curr_range_idx_; }

  int64_t get_ranges_count() const override { return ranges_.count(); };

  int next_range() override;

  int on_node_estimate(const ObMicroIndexInfo &index_row, const bool is_coverd_by_range) override;

  int on_leaf_node(const ObMicroIndexInfo &index_row, const ObPartitionEst &est) override;

  int on_inner_node(const ObMicroIndexInfo &index_row,
                    const bool is_coverd_by_range,
                    ObTraverserOperationType &operation) override;

  int64_t get_row_count_sum() const;

  int64_t get_macro_block_count_sum() const;

  const ObIArray<ObRangeInfo> &get_ranges() const { return ranges_; }

  void reuse();

  OB_INLINE void set_can_goto_micro_level(const bool can_goto_micro_level)
  {
    can_goto_micro_level_ = can_goto_micro_level;
  }

  VIRTUAL_TO_STRING_KV(K_(ranges), K_(curr_range_idx), K_(is_inited));

protected:
  ObSEArray<ObRangeInfo, 4> ranges_;
  uint64_t curr_range_idx_;
  ObRangePrecision range_precision_;
  int64_t sample_step_;
  bool can_goto_micro_level_;
  bool is_inited_;
};

/**
 * @brief The context for multi range split
 */
struct ObSplitRangeInfo
{
  ObSplitRangeInfo() : origin_range_(nullptr), row_count_(0), is_range_end_split_(false) {}

  const ObDatumRange *origin_range_;
  ObDatumRowkey split_rowkey_;
  int64_t row_count_;
  bool is_range_end_split_;

  TO_STRING_KV(KPC_(origin_range), K_(split_rowkey), K_(row_count), K(is_range_end_split_));
};

class ObMultiRangeSplitContext : public ObMultiRangeRowEstimateContext
{
public:
  static constexpr double SPLIT_ROW_UPPER_RATIO = 1.25;

  ObMultiRangeSplitContext()
      : ObMultiRangeRowEstimateContext(), allocator_(nullptr), split_ranges_(nullptr),
        read_info_(nullptr), split_row_limit_(0), split_row_upper_limit_(0)
  {
  }

  int init(const ObIArray<ObPairStoreAndDatumRange> &ranges,
           const ObITableReadInfo &read_info,
           const int64_t split_row_limit,
           ObIAllocator &allocator,
           const ObIArray<ObRangeInfo> &range_infos,
           ObIArray<ObSplitRangeInfo> &split_ranges,
           const bool need_sort = true,
           const ObRangePrecision &range_precision = ObRangePrecision(10000),
           const bool can_goto_micro_level = false);

  virtual ~ObMultiRangeSplitContext() = default;

  int on_node_estimate(const ObMicroIndexInfo &index_row, const bool is_coverd_by_range) override;

  int on_leaf_node(const ObMicroIndexInfo &index_row, const ObPartitionEst &est) override;

  int on_inner_node(const ObMicroIndexInfo &index_row,
                    const bool is_coverd_by_range,
                    ObTraverserOperationType &operation) override;

  int next_range() override;

  VIRTUAL_TO_STRING_KV(K_(ranges), K_(curr_range_idx), K_(split_row_limit), KPC_(split_ranges));

private:
  int try_to_add_new_split_range(int64_t &curr_row_count,
                                 const int64_t add_row_count,
                                 const ObCommonDatumRowkey &rowkey);

protected:
  ObIAllocator *allocator_;
  const ObIArray<ObRangeInfo> *range_infos_;
  ObIArray<ObSplitRangeInfo> *split_ranges_;
  const ObITableReadInfo *read_info_;
  int64_t split_row_limit_;
  int64_t split_row_upper_limit_;
};

class ObMultiRangeInfoEstimateContext : public ObMultiRangeRowEstimateContext
{
public:
  ObMultiRangeInfoEstimateContext()
      : ObMultiRangeRowEstimateContext(), last_macro_block_id_(),
        last_index_row_in_block_offset_(0), total_row_count_(0), total_macro_block_count_(0),
        total_size_(0), avg_micro_block_size_(0)
  {
  }

  virtual ~ObMultiRangeInfoEstimateContext() = default;

  int on_node_estimate(const ObMicroIndexInfo &index_row, const bool is_coverd_by_range) override;

  int on_leaf_node(const ObMicroIndexInfo &index_row, const ObPartitionEst &est) override;

  int on_inner_node(const ObMicroIndexInfo &index_row,
                    const bool is_coverd_by_range,
                    ObTraverserOperationType &operation) override;

  int64_t get_total_row_count() const { return total_row_count_; }
  int64_t get_total_macro_block_count() const { return total_macro_block_count_; }
  int64_t get_total_size() const { return total_size_; }

  VIRTUAL_TO_STRING_KV(K_(ranges), K_(curr_range_idx), K_(avg_micro_block_size));

private:
  OB_INLINE int64_t calc_estimate_size(const ObMicroIndexInfo &index_row,
                                       const bool is_coverd_by_range)
  {
    const int64_t divisor = is_coverd_by_range ? 1 : 2;
    return index_row.get_macro_block_count() >= 1
               ? index_row.get_macro_block_count() * OB_DEFAULT_MACRO_BLOCK_SIZE / divisor
               : index_row.get_micro_block_count() * avg_micro_block_size_ / divisor;
  }

  MacroBlockId last_macro_block_id_;
  int64_t last_index_row_in_block_offset_;
  int64_t total_row_count_;
  int64_t total_macro_block_count_;
  int64_t total_size_;
  int64_t avg_micro_block_size_;
};

// =============================== Heap Related ====================================

class ObISplitRangeHeapElementIter;
class ObSplitRangeHeapElement
{
public:
  TO_STRING_KV(KPC_(split_range_info), KPC_(iter));

  const ObSplitRangeInfo *split_range_info_;
  ObISplitRangeHeapElementIter *iter_;
};

class ObISplitRangeHeapElementIter
{
public:
  virtual ~ObISplitRangeHeapElementIter() = default;
  virtual int get_next(ObSplitRangeHeapElement &element) = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObSplitRangeHeapElementCompator
{
public:
  ObSplitRangeHeapElementCompator(const ObITableReadInfo &index_read_info)
      : index_read_info_(index_read_info), error_ret_(OB_SUCCESS)
  {
  }

  OB_INLINE void reset() { error_ret_ = OB_SUCCESS; }

  OB_INLINE int get_error_code() const { return error_ret_; }

  bool operator()(const ObSplitRangeHeapElement &a, const ObSplitRangeHeapElement &b) const
  {
    int ret = OB_SUCCESS, cmp_ret = 0;

    if (OB_ISNULL(a.split_range_info_) || OB_ISNULL(b.split_range_info_)) {
      error_ret_ = ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Fail to compare range split key", KR(ret));
    } else if (OB_FAIL(a.split_range_info_->split_rowkey_.compare(
            b.split_range_info_->split_rowkey_, index_read_info_.get_datum_utils(), cmp_ret))) {
      error_ret_ = ret;
      STORAGE_LOG(WARN, "Fail to compare range split key", KR(ret));
    }

    return cmp_ret > 0;
  }

private:
  const ObITableReadInfo &index_read_info_;
  mutable int error_ret_;
};

class ObSplitRangeHeapElementIter : public ObISplitRangeHeapElementIter
{
public:
  ObSplitRangeHeapElementIter() : cursor_(0) {}

  ObArray<ObSplitRangeInfo> &get_split_ranges() { return ranges_; }

  virtual int get_next(ObSplitRangeHeapElement &element) override
  {
    int ret = OB_SUCCESS;

    if (cursor_ >= ranges_.count()) {
      ret = OB_ITER_END;
    } else {
      element.split_range_info_ = &ranges_.at(cursor_);
      element.iter_ = this;
      cursor_++;
    }

    return ret;
  }

  TO_STRING_KV(K(ranges_.count()), K_(ranges), K_(cursor));

private:
  ObArray<ObSplitRangeInfo> ranges_;
  int64_t cursor_;
};

class ObPartitionMultiRangeSpliterHelper
{
public:
  ObPartitionMultiRangeSpliterHelper()
      : allocator_(nullptr), read_info_(nullptr), table_id_(0), is_inited_(false)
  {
  }

  int init(ObIAllocator &allocator,
           const ObITableReadInfo &read_info,
           const ObIArray<ObStoreRange> &ranges);

  template <typename ObRange>
  int construct_range(const ObDatumRowkey &start_key,
                      const ObDatumRowkey &end_key,
                      const ObBorderFlag &flag,
                      const bool for_compaction,
                      ObRange &range);

  template <typename ObRange>
  int construct_and_push_range(const ObDatumRowkey *start_key,
                               const ObDatumRowkey *end_key,
                               const ObBorderFlag &flag,
                               const bool for_compaction,
                               ObIArray<ObRange> &ranges);

private:
  template <typename ObTemplateRowkey>
  int deepcopy_rowkey(const ObDatumRowkey &rowkey,
                      const bool for_compaction,
                      ObTemplateRowkey &target);

  ObIAllocator *allocator_;
  const ObITableReadInfo *read_info_;
  int64_t table_id_;

  bool is_inited_;
};

class ObPartitionMultiRangeSpliter
{
public:
  static constexpr int64_t MIN_SPLIT_TABLE_SIZE = 16 << 10; // 16KB
  static constexpr int64_t MIN_SPLIT_TABLE_ROW_COUNT = 65535; // 64K rows
  static constexpr int64_t TOO_MARY_RANGES_THRESHOLD = 20;
  static constexpr int64_t SPLIT_RANGE_FACTOR = 2;
  static constexpr int64_t DEFAULT_MAX_SPLIT_TIME_COST = 10; // ms
  static constexpr int64_t INDEX_BLOCK_PER_TIME = 3; // access 3 index block / ms

  /**
   * @brief Get the multi ranges row count
   *
   * @param ranges
   * @param index_read_info
   * @param table_iter
   * @param row_count the row count of the multi ranges
   * @param macro_block_count the different macro block count of the multi ranges, if all ranges is
   *                          in the same macro block, the macro block count will be set to 1
   * @param max_time
   * @param range_precision
   * @return int
   */
  int get_multi_ranges_row_count(const ObIArray<ObStoreRange> &ranges,
                                 const ObITableReadInfo &index_read_info,
                                 ObTableStoreIterator &table_iter,
                                 int64_t &row_count,
                                 int64_t &macro_block_count,
                                 const int64_t max_time = DEFAULT_MAX_SPLIT_TIME_COST,
                                 const int64_t range_precision = ObRangePrecision::DEFAULT_DYNAMIC_RANGE_PRECISION_HINT);

  int get_multi_range_size(const ObIArray<ObStoreRange> &ranges,
                           const ObITableReadInfo &index_read_info,
                           ObTableStoreIterator &table_iter,
                           int64_t &total_size,
                           const int64_t max_time = DEFAULT_MAX_SPLIT_TIME_COST,
                           const int64_t range_precision = ObRangePrecision::DEFAULT_DYNAMIC_RANGE_PRECISION_HINT);

  int get_multi_range_size(const ObIArray<ObStoreRange> &ranges,
                           const ObITableReadInfo &index_read_info,
                           const ObIArray<ObITable *> &tables,
                           int64_t &total_size,
                           const int64_t max_time = DEFAULT_MAX_SPLIT_TIME_COST,
                           const int64_t range_precision = ObRangePrecision::DEFAULT_DYNAMIC_RANGE_PRECISION_HINT);

  template <typename ObRange>
  int get_split_multi_ranges(const ObIArray<ObStoreRange> &ranges,
                             const int64_t expected_task_count,
                             const ObITableReadInfo &index_read_info,
                             ObTableStoreIterator &table_iter,
                             ObIAllocator &allocator,
                             ObArrayArray<ObRange> &multi_range_split_array,
                             const bool for_compaction = false,
                             const int64_t max_time = DEFAULT_MAX_SPLIT_TIME_COST,
                             const int64_t range_precision = ObRangePrecision::DEFAULT_DYNAMIC_RANGE_PRECISION_HINT);

  template <typename ObRange>
  int get_split_multi_ranges(const ObIArray<ObStoreRange> &ranges,
                             const int64_t expected_task_count,
                             const ObITableReadInfo &index_read_info,
                             const ObIArray<ObITable *> &tables,
                             ObIAllocator &allocator,
                             ObArrayArray<ObRange> &multi_range_split_array,
                             const bool for_compaction = false,
                             const int64_t max_time = DEFAULT_MAX_SPLIT_TIME_COST,
                             const int64_t range_precision = ObRangePrecision::DEFAULT_DYNAMIC_RANGE_PRECISION_HINT);

private:
  int estimate_ranges_info(const ObIArray<ObStoreRange> &ranges,
                           const ObITableReadInfo &read_info,
                           const ObIArray<ObITable *> &tables,
                           int64_t &total_size,
                           int64_t &total_row_count,
                           int64_t &total_macro_block_count,
                           const int64_t max_time = DEFAULT_MAX_SPLIT_TIME_COST,
                           const ObRangePrecision &range_precision = ObRangePrecision());

  bool all_range_is_single_rowkey(const ObIArray<ObStoreRange> &ranges);

  template <typename ObRange>
  int fast_build_range_array(const ObIArray<ObStoreRange> &ranges,
                             const int64_t expected_task_cnt,
                             ObIAllocator &allocator,
                             ObArrayArray<ObRange> &multi_range_split_array);

  template <typename ObRange>
  int build_range_array(const ObIArray<ObStoreRange> &ranges,
                        const int64_t expected_task_count,
                        const ObITableReadInfo &index_read_info,
                        const ObIArray<ObITable *> &tables,
                        ObIAllocator &allocator,
                        ObArrayArray<ObRange> &multi_range_split_array,
                        const bool for_compaction = false,
                        const int64_t max_time = DEFAULT_MAX_SPLIT_TIME_COST,
                        const ObRangePrecision &range_precision = ObRangePrecision());

  int get_tables(ObTableStoreIterator &table_iter, ObIArray<ObITable *> &tables);

  int transform_to_datum_range_and_sort(const ObIArray<ObStoreRange> &ranges,
                                        const ObITableReadInfo &read_info,
                                        ObIAllocator &allocator,
                                        ObIArray<ObPairStoreAndDatumRange> &sorted_ranges);

  int64_t calc_row_split_limit(const int64_t total_row_count, const int64_t expected_task_count, const int64_t ranges_count)
  {
    const int64_t factor = (ranges_count > TOO_MARY_RANGES_THRESHOLD ? 1 : SPLIT_RANGE_FACTOR);
    return max(1, total_row_count / expected_task_count / factor);
  }

  int estimate_ranges_row_and_split(const ObIArray<ObPairStoreAndDatumRange> &ranges,
                                    const int64_t expected_task_count,
                                    const ObITableReadInfo &read_info,
                                    const ObIArray<ObITable *> &tables,
                                    ObIAllocator &allocator,
                                    ObIArray<ObSplitRangeHeapElementIter> &heap_element_iters,
                                    int64_t &estimate_rows_sum,
                                    const int64_t max_time,
                                    const ObRangePrecision &range_precision);

  int split_ranges_for_memtable(const ObIArray<ObPairStoreAndDatumRange> &ranges,
                                const int64_t expected_task_count,
                                memtable::ObMemtable &memtable,
                                ObIAllocator &allocator,
                                ObIArray<ObSplitRangeHeapElementIter> &heap_element_iters,
                                int64_t &estimate_rows_sum,
                                const ObRangePrecision &range_precision);

  int split_ranges_for_sstable(const ObIArray<ObPairStoreAndDatumRange> &sorted_ranges,
                               const ObITableReadInfo &read_info,
                               const int64_t expected_task_count,
                               const int64_t open_index_block_limit,
                               ObSSTable &sstable,
                               ObIAllocator &allocator,
                               ObIArray<ObSplitRangeHeapElementIter> &heap_element_iters,
                               int64_t &estimate_rows_sum,
                               const ObRangePrecision &range_precision);

  int build_heap(ObIArray<ObSplitRangeHeapElementIter> &heap_element_iters,
                 ObBinaryHeap<ObSplitRangeHeapElement, ObSplitRangeHeapElementCompator> &heap);

  template <typename ObRange>
  int do_task_split_algorithm(
      const ObIArray<ObStoreRange> &ranges,
      const ObITableReadInfo &read_info,
      const int64_t expected_task_count,
      const int64_t avg_task_row_count,
      ObIAllocator &allocator,
      ObBinaryHeap<ObSplitRangeHeapElement, ObSplitRangeHeapElementCompator> &heap,
      ObArrayArray<ObRange> &multi_range_split_array,
      const bool for_compaction);
};

class ObPartitionMajorSSTableRangeSpliter
{
public:
  ObPartitionMajorSSTableRangeSpliter();
  virtual ~ObPartitionMajorSSTableRangeSpliter();
  int init(const ObITableReadInfo &index_read_info, blocksstable::ObSSTable *major_sstable,
           int64_t tablet_size, common::ObIAllocator &allocator);
  int split_ranges(common::ObIArray<ObStoreRange> &ranges);
private:
  virtual int scan_major_sstable_secondary_meta(const blocksstable::ObDatumRange &scan_range,
                                                blocksstable::ObSSTableSecMetaIterator *&meta_iter);
  int generate_ranges_by_macro_block(int64_t parallel_degree,
                                     common::ObIArray<ObStoreRange> &ranges);
private:
  blocksstable::ObSSTable *major_sstable_;
  const storage::ObITableReadInfo *index_read_info_;
  int64_t tablet_size_;
  common::ObIAllocator *allocator_;
  bool is_inited_;
};

class ObPartitionIncrementalRangeSpliter
{
  static const int64_t DEFAULT_NOISY_ROW_NUM_SKIPPED = 1000;
  static const int64_t DEFAULT_ROW_NUM_PER_RANGE = 1000000;
  typedef common::ObIArray<blocksstable::ObDatumRange> ObDatumRangeArray;
public:
  ObPartitionIncrementalRangeSpliter();
  virtual ~ObPartitionIncrementalRangeSpliter();
  int init(compaction::ObTabletMergeCtx &merge_ctx, ObIAllocator &allocator);
  int check_is_incremental(bool &is_incremental);
  int split_ranges(ObDatumRangeArray &ranges);
private:
  int alloc_ranges();
  virtual int init_incremental_iter();
  virtual int get_major_sstable_end_rowkey(blocksstable::ObDatumRowkey &rowkey);
  virtual int scan_major_sstable_secondary_meta(const blocksstable::ObDatumRange &scan_range,
                                                blocksstable::ObSSTableSecMetaIterator *&meta_iter);
  int get_ranges_by_inc_data(ObDatumRangeArray &ranges);
  int get_ranges_by_base_sstable(ObDatumRangeArray &ranges);
  // generate ranges according to both <base_ranges> and <inc_ranges>
  int combine_ranges(const ObDatumRangeArray &base_ranges, const ObDatumRangeArray &inc_ranges,
                     ObDatumRangeArray &result_ranges);
  // Combine multiple contiguous ranges (one thread should handle those ranges)
  // according to number of available threads.
  int merge_ranges(const ObDatumRangeArray &ranges, ObDatumRangeArray &result_ranges);
  static int check_continuous(const blocksstable::ObStorageDatumUtils &datum_utils,
                              ObDatumRangeArray &ranges);
protected:
  class ObIncrementalIterator : public storage::ObIStoreRowIterator
  {
  public:
    ObIncrementalIterator(compaction::ObTabletMergeCtx &merge_ctx, ObIAllocator &allocator);
    virtual ~ObIncrementalIterator();

    void reset();
    int init();
    int get_next_row(const blocksstable::ObDatumRow *&row) override;

  private:
    int prepare_table_access_param();
    int prepare_store_ctx();
    int prepare_table_access_context();
    int prepare_get_table_param();

  private:
    compaction::ObTabletMergeCtx &merge_ctx_;
    ObIAllocator &allocator_;
    ObSEArray<share::schema::ObColDesc, OB_MAX_ROWKEY_COLUMN_NUMBER> rowkey_col_ids_;
    ObSEArray<int32_t, OB_MAX_ROWKEY_COLUMN_NUMBER> out_cols_project_;
    ObRowkeyReadInfo tbl_read_info_;
    ObTableAccessParam tbl_xs_param_;
    ObStoreCtx store_ctx_;
    ObTableAccessContext tbl_xs_ctx_;
    ObTableStoreIterator tbls_iter_;
    ObGetTableParam get_tbl_param_;
    blocksstable::ObDatumRange range_to_scan_;
    ObIStoreRowIterator *iter_;
    bool is_inited_;
    DISALLOW_COPY_AND_ASSIGN(ObIncrementalIterator);
  };

private:
  compaction::ObTabletMergeCtx *merge_ctx_;
  ObIAllocator *allocator_;
  blocksstable::ObSSTable *major_sstable_;
  int64_t tablet_size_;
  ObIncrementalIterator *iter_;
  // for unittest
  int64_t default_noisy_row_num_skipped_;
  int64_t default_row_num_per_range_;
  // ranges
  ObDatumRangeArray *inc_ranges_;
  ObDatumRangeArray *base_ranges_;
  ObDatumRangeArray *combined_ranges_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionIncrementalRangeSpliter);
};

}
}

#endif
