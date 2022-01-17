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

#include "ob_storage_struct.h"
#include "ob_sstable.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_heap.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/container/ob_array_array.h"

namespace oceanbase {
namespace storage {

struct ObMacroEndkey {
  ObMacroEndkey() : obj_ptr_(nullptr), obj_cnt_(0), iter_idx_(-1)
  {}
  ~ObMacroEndkey() = default;
  OB_INLINE bool is_valid() const
  {
    return OB_NOT_NULL(obj_ptr_) && obj_cnt_ > 0 && iter_idx_ >= 0;
  }
  OB_INLINE void reset()
  {
    obj_ptr_ = nullptr;
    obj_cnt_ = 0;
    iter_idx_ = -1;
  }
  TO_STRING_KV(K_(obj_ptr), K_(obj_cnt), K_(iter_idx));
  common::ObObj* obj_ptr_;
  int64_t obj_cnt_;
  int64_t iter_idx_;
};

struct ObMacroEndkeyComparor {
  ObMacroEndkeyComparor() : ret_(OB_SUCCESS)
  {}
  ~ObMacroEndkeyComparor() = default;
  OB_INLINE void reset()
  {
    ret_ = OB_SUCCESS;
  }
  OB_INLINE int get_error_code() const
  {
    return ret_;
  }
  OB_INLINE bool operator()(const ObMacroEndkey& left, const ObMacroEndkey& right)
  {
    bool bret = false;
    ObRowkey left_rowkey(left.obj_ptr_, left.obj_cnt_);
    ObRowkey right_rowkey(right.obj_ptr_, right.obj_cnt_);
    if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    } else if (OB_UNLIKELY(!left_rowkey.is_valid() || !right_rowkey.is_valid())) {
      ret_ = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument to compare macro end key", K_(ret), K(left_rowkey), K(right_rowkey));
    } else {
      bret = (left_rowkey.compare(right_rowkey)) > 0;
    }
    return bret;
  }
  TO_STRING_KV(K_(ret));
  int ret_;
};

struct ObSSTableRangePara {
  ObSSTableRangePara() : sstable_(nullptr), start_(0), end_(0)
  {}
  ~ObSSTableRangePara() = default;
  OB_INLINE void reset()
  {
    memset(this, 0, sizeof(ObSSTableRangePara));
  }
  OB_INLINE bool is_valid() const
  {
    return OB_NOT_NULL(sstable_) && start_ >= 0 && end_ >= start_ && end_ <= sstable_->get_macro_block_count();
  }
  OB_INLINE int64_t get_macro_count() const
  {
    return end_ - start_;
  }
  TO_STRING_KV(KPC_(sstable), K_(start), K_(end));
  ObSSTable* sstable_;
  int64_t start_;
  int64_t end_;
};

class ObMacroEndkeyIterator {
public:
  ObMacroEndkeyIterator();
  ~ObMacroEndkeyIterator() = default;
  OB_INLINE bool is_valid() const
  {
    return is_inited_;
  }
  void reset();
  OB_INLINE void reuse()
  {
    cur_idx_ = (range_para_.end_ - range_para_.start_) / skip_cnt_;
  }
  int open(const ObSSTableRangePara& range_para, const int64_t skip_cnt, const int64_t iter_idx);
  int get_endkey_cnt(int64_t& endkey_cnt) const;
  int get_next_macro_block_endkey(ObMacroEndkey& endkey);
  TO_STRING_KV(K_(range_para), K_(cur_idx), K_(skip_cnt), K_(iter_idx), K_(is_inited));
  ObSSTableRangePara range_para_;
  int64_t cur_idx_;
  int64_t skip_cnt_;
  int64_t iter_idx_;
  bool is_inited_;
};

class ObPartitionParallelRanger {
public:
  explicit ObPartitionParallelRanger(common::ObArenaAllocator& allocator);
  ~ObPartitionParallelRanger();
  ObPartitionParallelRanger() = delete;
  void reset();
  int init(const common::ObStoreRange& range, common::ObIArray<ObSSTableRangePara>& range_paras,
      const int64_t paralell_target_count);
  int split_ranges(
      common::ObIAllocator& allocator, const bool for_compaction, common::ObIArray<common::ObStoreRange>& range_array);
  int construct_single_range(common::ObIAllocator& allocator, const common::ObStoreRowkey& start_key,
      const common::ObStoreRowkey& end_key, const common::ObBorderFlag& border_flag, const bool for_compaction,
      ObStoreRange& range);
  TO_STRING_KV(KPC(store_range_), K_(endkey_iters), KP_(last_macro_endkey), K_(total_endkey_cnt), K_(sample_cnt),
      K_(parallel_target_count), K_(is_inited));

private:
  int calc_sample_count(common::ObIArray<ObSSTableRangePara>& range_paras, const int64_t paralell_target_count);
  int init_macro_iters(common::ObIArray<ObSSTableRangePara>& range_paras);
  int build_parallel_range_heap();
  int get_next_macro_endkey(ObStoreRowkey& rowkey);
  int build_new_rowkey(const common::ObStoreRowkey& rowkey, common::ObIAllocator& allocator, const bool for_compaction,
      common::ObStoreRowkey& new_rowkey);
  int check_rowkey_equal(const common::ObStoreRowkey& rowkey1, const common::ObStoreRowkey& rowkey2, bool& equal);
  int check_continuous(common::ObIArray<common::ObStoreRange>& range_array);

private:
  const common::ObStoreRange* store_range_;
  common::ObArray<ObMacroEndkeyIterator*> endkey_iters_;
  common::ObArenaAllocator& allocator_;
  ObMacroEndkeyComparor comparor_;
  common::ObBinaryHeap<ObMacroEndkey, ObMacroEndkeyComparor> range_heap_;
  // Since merge ctx keep the sstable handle, safe to keep the pointer to the endkey in macro meta
  const ObMacroEndkey* last_macro_endkey_;
  int64_t total_endkey_cnt_;
  int64_t sample_cnt_;
  int64_t parallel_target_count_;
  bool is_inited_;
};

struct ObRangeSplitInfo {
  ObRangeSplitInfo();
  ~ObRangeSplitInfo();
  void reset();
  OB_INLINE bool is_valid() const
  {
    return nullptr != store_range_ && store_range_->is_valid() && parallel_target_count_ > 0 &&
           (range_paras_.count() > 0 || parallel_target_count_ == 1);
  }
  OB_INLINE void set_parallel_target(const int64_t parallel_target_count)
  {
    parallel_target_count_ = parallel_target_count;
  }
  OB_INLINE bool empty() const
  {
    return range_paras_.empty();
  }
  TO_STRING_KV(K_(store_range), K_(range_paras), K_(total_row_count), K_(total_size), K_(parallel_target_count));
  const common::ObStoreRange* store_range_;
  common::ObArray<ObSSTableRangePara> range_paras_;
  int64_t total_row_count_;
  int64_t total_size_;
  int64_t parallel_target_count_;
};

class ObPartitionRangeSpliter {
public:
  ObPartitionRangeSpliter();
  ~ObPartitionRangeSpliter();
  void reset();
  int get_range_split_info(
      common::ObIArray<ObSSTable*>& sstables, const common::ObStoreRange& store_range, ObRangeSplitInfo& range_info);
  int split_ranges(ObRangeSplitInfo& range_info, common::ObIAllocator& allocator, const bool for_compaction,
      common::ObIArray<ObStoreRange>& range_array);
  TO_STRING_KV(K_(parallel_ranger));

private:
  int build_range_paras(common::ObIArray<ObSSTable*>& sstables, const common::ObStoreRange& store_range,
      common::ObIArray<ObSSTableRangePara>& range_paras);
  int build_range_para(ObSSTable* sstable, const ObExtStoreRange& ext_range, ObSSTableRangePara& range_para);
  int get_size_info(common::ObIArray<ObSSTableRangePara>& range_paras, int64_t& total_row_count, int64_t& total_size);
  int get_single_range_info(const ObSSTableRangePara& range_para, int64_t& total_row_count, int64_t& total_size);

private:
  common::ObArenaAllocator allocator_;
  ObPartitionParallelRanger parallel_ranger_;
};

class ObPartitionMultiRangeSpliter {
public:
  // return total size(byte) of all sstables in read_tables
  int get_multi_range_size(
      ObTablesHandle& read_tables, const common::ObIArray<common::ObStoreRange>& range_array, int64_t& total_size);
  int get_split_multi_ranges(ObTablesHandle& read_tables, const common::ObIArray<common::ObStoreRange>& range_array,
      const int64_t expected_task_count, common::ObIAllocator& allocator,
      common::ObArrayArray<common::ObStoreRange>& multi_range_split_array);

private:
  static const int64_t MIN_SPLIT_TASK_SIZE = 2 << 20;
  static const int64_t MIN_SPLIT_TAGET_SSTABLE_SIZE = MIN_SPLIT_TASK_SIZE * 3;
  static const int64_t SPLIT_TASK_SIZE_HIGH_WATER_MARK_FACTOR = 125;
  static const int64_t SPLIT_TASK_SIZE_LOW_WATER_MARK_FACTOR = 75;
  static const int64_t DEFAULT_STORE_RANGE_ARRAY_SIZE = 8;
  typedef common::ObSEArray<common::ObStoreRange, DEFAULT_STORE_RANGE_ARRAY_SIZE> RangeSplitArray;
  typedef common::ObSEArray<ObRangeSplitInfo, DEFAULT_STORE_RANGE_ARRAY_SIZE> RangeSplitInfoArray;

private:
  int get_split_sstables(ObTablesHandle& read_tables, common::ObIArray<ObSSTable*>& sstables);
  int split_multi_ranges(RangeSplitInfoArray& range_info_array, const int64_t expected_task_count,
      const int64_t total_size, common::ObIAllocator& allocator,
      common::ObArrayArray<common::ObStoreRange>& multi_range_split_array);
  int get_range_split_infos(common::ObIArray<ObSSTable*>& sstables,
      const common::ObIArray<common::ObStoreRange>& range_array, RangeSplitInfoArray& range_info_array,
      int64_t& total_size);
  int merge_and_push_range_array(const RangeSplitArray& src_range_split_array, common::ObIAllocator& allocator,
      common::ObArrayArray<common::ObStoreRange>& multi_range_split_array);
  int build_single_range_array(const common::ObIArray<common::ObStoreRange>& range_array,
      common::ObIAllocator& allocator, common::ObArrayArray<common::ObStoreRange>& multi_range_split_array);

private:
  ObPartitionRangeSpliter range_spliter_;
};

}  // namespace storage
}  // namespace oceanbase

#endif
