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

struct ObMacroEndkey
{
  ObMacroEndkey() : rowkey_(nullptr), iter_idx_(-1) {}
  ~ObMacroEndkey() = default;
  OB_INLINE bool is_valid() const { return OB_NOT_NULL(rowkey_) && iter_idx_ >= 0; }
  OB_INLINE void reset()
  {
    rowkey_ = nullptr;
    iter_idx_ = -1;
  }
  TO_STRING_KV(K_(rowkey), K_(iter_idx));
  ObStoreRowkey *rowkey_;
  int64_t iter_idx_;
};

struct ObMacroEndkeyComparor
{
  ObMacroEndkeyComparor() : ret_(OB_SUCCESS) {}
  ~ObMacroEndkeyComparor() = default;
  OB_INLINE void reset() { ret_ = OB_SUCCESS; }
  OB_INLINE int get_error_code() const { return ret_; }
  OB_INLINE bool operator()(const ObMacroEndkey &left, const ObMacroEndkey &right)
  {
    bool bret = false;
    int cmp = 0;
    if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    } else if (OB_ISNULL(left.rowkey_) || OB_ISNULL(right.rowkey_)
        || OB_UNLIKELY(!left.rowkey_->is_valid() || !right.rowkey_->is_valid())) {
      ret_ = OB_INVALID_ARGUMENT;
      STORAGE_LOG_RET(WARN, ret_, "Invalid argument to compare macro endkey", K_(ret), K(left), K(right));
    } else {
      ret_ = left.rowkey_->compare(*right.rowkey_, cmp);
    }
    return cmp > 0;
  }
  TO_STRING_KV(K_(ret));
  int ret_;
};

struct ObRangeSplitInfo
{
  ObRangeSplitInfo();
  ~ObRangeSplitInfo();
  void reset();
  OB_INLINE bool is_valid() const
  {
    return OB_NOT_NULL(store_range_)
        && OB_NOT_NULL(index_read_info_)
        && OB_NOT_NULL(tables_)
        && store_range_->is_valid()
        && index_read_info_->is_valid()
        && parallel_target_count_ > 0
        && (tables_->count() > 0 || parallel_target_count_ == 1) ;
  }
  OB_INLINE void set_parallel_target(const int64_t parallel_target_count)
  {
    parallel_target_count_ = parallel_target_count;
  }
  OB_INLINE bool is_sstable() const { return is_sstable_; }
  OB_INLINE bool empty() const { return OB_NOT_NULL(tables_) && tables_->empty(); }
  TO_STRING_KV(K_(store_range), KPC_(tables), K_(total_size), K_(max_macro_block_count),
               K_(max_estimate_micro_block_cnt), K_(parallel_target_count), K_(is_sstable));
  const common::ObStoreRange *store_range_;
  const ObITableReadInfo *index_read_info_;
  common::ObIArray<ObITable *> *tables_;
  int64_t total_size_;
  int64_t parallel_target_count_;
  int64_t max_macro_block_count_;
  int64_t max_estimate_micro_block_cnt_;
  ObIAllocator *key_allocator_;
  bool is_sstable_;
};

class ObEndkeyIterator
{
public:
  ObEndkeyIterator();
  virtual ~ObEndkeyIterator() = default;
  OB_INLINE bool is_valid() const
  {
    return is_inited_;
  }
  void reset();
  int open(
      const int64_t skip_cnt,
      const int64_t iter_idx,
      blocksstable::ObSSTable &sstable,
      ObRangeSplitInfo &range_info);
  bool is_empty() { return endkeys_.empty(); }
  int get_endkey_cnt(int64_t &endkey_cnt) const;
  int get_next_macro_block_endkey(ObMacroEndkey &endkey);
  int push_rowkey(
      const int64_t rowkey_col_cnt,
      const common::ObIArray<share::schema::ObColDesc> &col_descs,
      const blocksstable::ObDatumRowkey &end_key,
      ObIAllocator &allocator);
  VIRTUAL_TO_STRING_KV(K_(endkeys), K_(cur_idx), K_(iter_idx), K_(is_inited));
private:
  virtual int init_endkeys(
        const int64_t skip_cnt,
        const ObRangeSplitInfo &range_info,
        const blocksstable::ObDatumRange &datum_range,
        blocksstable::ObSSTable &sstable) = 0;

public:
  static const int64_t DEFAULT_ENDKEY_ARRAY_COUNT = 16;
  ObSEArray<ObStoreRowkey, DEFAULT_ENDKEY_ARRAY_COUNT> endkeys_;
  int64_t cur_idx_;
  int64_t iter_idx_;
  bool is_inited_;
};

class ObMacroEndkeyIterator : public ObEndkeyIterator
{
public:
  ObMacroEndkeyIterator() {}
  virtual ~ObMacroEndkeyIterator() = default;
  INHERIT_TO_STRING_KV("ObEndkeyIterator", ObEndkeyIterator, "cur_iter", "ObMacroEndkeyIterator");

private:
  virtual int init_endkeys(
        const int64_t skip_cnt,
        const ObRangeSplitInfo &range_info,
        const blocksstable::ObDatumRange &datum_range,
        blocksstable::ObSSTable &sstable) override;
};

class ObMicroEndkeyIterator : public ObEndkeyIterator
{
public:
  ObMicroEndkeyIterator() {}
  virtual ~ObMicroEndkeyIterator() = default;
  INHERIT_TO_STRING_KV("ObEndkeyIterator", ObEndkeyIterator, "cur_iter", "ObMicroEndkeyIterator");

private:
  virtual int init_endkeys(
        const int64_t skip_cnt,
        const ObRangeSplitInfo &range_info,
        const blocksstable::ObDatumRange &datum_range,
        blocksstable::ObSSTable &sstable) override;
};

class ObPartitionParallelRanger
{
public:
  explicit ObPartitionParallelRanger(common::ObArenaAllocator &allocator);
  ~ObPartitionParallelRanger();
  ObPartitionParallelRanger() = delete;
  void reset();
  int init(ObRangeSplitInfo &range_info, const bool for_compaction);
  int split_ranges(const bool for_compaction,
                   common::ObIAllocator &allocator,
                   common::ObIArray<common::ObStoreRange> &range_array);
  int construct_single_range(common::ObIAllocator &allocator,
                             const common::ObStoreRowkey &start_key,
                             const common::ObStoreRowkey &end_key,
                             const common::ObBorderFlag  &border_flag,
                             const bool for_compaction,
                             ObStoreRange &range);
  void set_col_cnt(int64_t col_cnt) { col_cnt_ = col_cnt; }
  TO_STRING_KV(KPC(store_range_), K_(endkey_iters), KP_(last_macro_endkey), K_(total_endkey_cnt),
               K_(sample_cnt), K_(parallel_target_count), K_(is_inited));
private:
  int calc_sample_count(const bool for_compaction, ObRangeSplitInfo &range_info);
  int init_macro_iters(ObRangeSplitInfo &range_info);
  int build_parallel_range_heap();
  int get_next_macro_endkey(ObStoreRowkey &rowkey);
  int build_new_rowkey(const common::ObStoreRowkey &rowkey,
                       const bool for_compaction,
                       common::ObIAllocator &allocator,
                       common::ObStoreRowkey &new_rowkey);
  int build_bound_rowkey(const bool is_max, common::ObIAllocator &allocator, common::ObStoreRowkey &new_rowkey);
  int check_rowkey_equal(const common::ObStoreRowkey &rowkey1, const common::ObStoreRowkey &rowkey2, bool &equal);
  int check_continuous(common::ObIArray<common::ObStoreRange> &range_array);
private:
  const common::ObStoreRange *store_range_;
  common::ObSEArray<ObEndkeyIterator *, DEFAULT_STORE_CNT_IN_STORAGE> endkey_iters_;
  common::ObArenaAllocator &allocator_;
  ObMacroEndkeyComparor comparor_;
  common::ObBinaryHeap<ObMacroEndkey, ObMacroEndkeyComparor> range_heap_;
  //Since merge ctx keep the sstable handle, safe to keep the pointer to the endkey in macro meta
  const ObMacroEndkey *last_macro_endkey_;
  int64_t total_endkey_cnt_;
  int64_t sample_cnt_;
  int64_t parallel_target_count_;
  bool is_micro_level_;
  int64_t col_cnt_;
  bool is_inited_;
};

class ObPartitionRangeSpliter
{
public:
  ObPartitionRangeSpliter();
  ~ObPartitionRangeSpliter();
  void reset();
  int get_range_split_info(common::ObIArray<ObITable *> &tables,
                           const ObITableReadInfo &index_read_info,
                           const common::ObStoreRange &store_range,
                           ObRangeSplitInfo &range_info);
  int split_ranges(ObRangeSplitInfo &range_info,
                   common::ObIAllocator &allocator,
                   const bool for_compaction,
                   common::ObIArray<ObStoreRange> &range_array);
  TO_STRING_KV(K_(parallel_ranger));
private:
  int build_single_range(const bool for_compaction,
                         ObRangeSplitInfo &range_info,
                         common::ObIAllocator &allocator,
                         common::ObIArray<ObStoreRange> &range_array);
  int get_single_range_info(const ObStoreRange &store_range,
                            const ObITableReadInfo &index_read_info,
                            ObITable *table,
                            int64_t &total_size,
                            int64_t &macro_block_cnt,
                            int64_t &estimate_micro_block_cnt);
  int split_ranges_memtable(ObRangeSplitInfo &range_info,
                            common::ObIAllocator &allocator,
                            common::ObIArray<ObStoreRange> &range_array);
private:
  common::ObArenaAllocator allocator_;
  ObPartitionParallelRanger parallel_ranger_;
};

class ObPartitionMultiRangeSpliter
{
public:
  // return total size(byte) of all sstables in read_tables
  int get_multi_range_size(
      const common::ObIArray<common::ObStoreRange> &range_array,
      const ObITableReadInfo &index_read_info,
      ObTableStoreIterator &table_iter,
      int64_t &total_size);
  int get_split_multi_ranges(
      const common::ObIArray<common::ObStoreRange> &range_array,
      const int64_t expected_task_count,
      const ObITableReadInfo &index_read_info,
      ObTableStoreIterator &table_iter,
      common::ObIAllocator &allocator,
      common::ObArrayArray<common::ObStoreRange> &multi_range_split_array);
private:
  static const int64_t MIN_SPLIT_TASK_SIZE = 16 << 10;
  static const int64_t MIN_SPLIT_TARGET_SSTABLE_SIZE = MIN_SPLIT_TASK_SIZE * 3;
  static const int64_t SPLIT_TASK_SIZE_HIGH_WATER_MARK_FACTOR = 125;
  static const int64_t SPLIT_TASK_SIZE_LOW_WATER_MARK_FACTOR = 75;
  static const int64_t DEFAULT_STORE_RANGE_ARRAY_SIZE = 8;
  static const int64_t MEMTABLE_SIZE_AMPLIFICATION_FACTOR = 3;
  typedef common::ObSEArray<common::ObStoreRange, DEFAULT_STORE_RANGE_ARRAY_SIZE> RangeSplitArray;
  typedef common::ObSEArray<ObRangeSplitInfo, DEFAULT_STORE_RANGE_ARRAY_SIZE> RangeSplitInfoArray;
private:
  int get_split_tables(ObTableStoreIterator &table_iter, common::ObIArray<ObITable *> &tables);
  int split_multi_ranges(RangeSplitInfoArray &range_info_array,
                         const int64_t expected_task_count,
                         const int64_t total_size,
                         common::ObIAllocator &allocator,
                         common::ObArrayArray<common::ObStoreRange> &multi_range_split_array);
  int get_range_split_infos(common::ObIArray<ObITable *> &tables,
                            const ObITableReadInfo &index_read_info,
                            const common::ObIArray<common::ObStoreRange> &range_array,
                            RangeSplitInfoArray &range_info_array,
                            int64_t &total_size);
  int merge_and_push_range_array(const RangeSplitArray &src_range_split_array,
                                 common::ObIAllocator &allocator,
                                 common::ObArrayArray<common::ObStoreRange> &multi_range_split_array);
  int build_single_range_array(const common::ObIArray<common::ObStoreRange> &range_array,
                               common::ObIAllocator &allocator,
                               common::ObArrayArray<common::ObStoreRange> &multi_range_split_array);
private:
  ObPartitionRangeSpliter range_spliter_;
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
