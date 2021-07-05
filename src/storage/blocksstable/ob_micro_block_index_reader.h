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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_MICRO_BLOCK_INDEX_READER_H
#define OCEANBASE_BLOCKSSTABLE_OB_MICRO_BLOCK_INDEX_READER_H

#include "ob_row_reader.h"
#include "ob_micro_block_index_mgr.h"

namespace oceanbase {
namespace blocksstable {

struct ObBlockIndexIterator {
public:
  typedef ObBlockIndexIterator self_t;
  typedef std::random_access_iterator_tag iterator_category;
  typedef uint64_t value_type;
  typedef int64_t difference_type;
  typedef uint64_t* pointer;
  typedef uint64_t& reference;

  static const self_t& invalid_iterator()
  {
    static self_t invalid_iter(UINT64_MAX);
    return invalid_iter;
  }

  ObBlockIndexIterator() : block_idx_(0)
  {}
  explicit ObBlockIndexIterator(const uint64_t id) : block_idx_(id)
  {}

  uint64_t operator*() const
  {
    return block_idx_;
  }
  bool operator==(const self_t& r) const
  {
    return block_idx_ == r.block_idx_;
  }
  bool operator!=(const self_t& r) const
  {
    return block_idx_ != r.block_idx_;
  }
  bool operator<(const self_t& r) const
  {
    return block_idx_ < r.block_idx_;
  }
  bool operator>(const self_t& r) const
  {
    return block_idx_ > r.block_idx_;
  }
  bool operator>=(const self_t& r) const
  {
    return block_idx_ >= r.block_idx_;
  }
  bool operator<=(const self_t& r) const
  {
    return block_idx_ <= r.block_idx_;
  }
  difference_type operator-(const self_t& r) const
  {
    return block_idx_ - r.block_idx_;
  }
  self_t operator-(difference_type step) const
  {
    return self_t(block_idx_ - step);
  }
  self_t operator+(difference_type step) const
  {
    return self_t(block_idx_ + step);
  }
  self_t& operator-=(difference_type step)
  {
    block_idx_ -= step;
    return *this;
  }
  self_t& operator+=(difference_type step)
  {
    block_idx_ += step;
    return *this;
  }
  self_t& operator++()
  {
    block_idx_++;
    return *this;
  }
  self_t operator++(int)
  {
    return self_t(block_idx_++);
  }
  self_t& operator--()
  {
    block_idx_--;
    return *this;
  }
  self_t operator--(int)
  {
    return self_t(block_idx_--);
  }

  TO_STRING_KV(K_(block_idx));

  uint64_t block_idx_;
};

class ObMicroBlockIndexReader {
public:
  typedef ObBlockIndexIterator Iterator;

public:
  ObMicroBlockIndexReader();
  ~ObMicroBlockIndexReader();
  void reset();
  int init(const ObFullMacroBlockMeta& meta, const char* index_buf);
  int init(const ObSSTableMacroBlockHeader& header, const char* index_buf, const common::ObObjMeta* column_type_array);
  int get_end_key(const uint64_t index, common::ObObj* objs);
  int get_end_keys(const common::ObStoreRange& range, common::ObIAllocator& allocator,
      common::ObIArray<common::ObStoreRowkey>& end_keys);
  int get_all_end_keys(common::ObIAllocator& allocator, common::ObIArray<common::ObStoreRowkey>& end_keys);
  int get_all_mem_micro_index(ObMicroBlockIndexMgr::MemMicroIndexItem* indexes);
  int get_micro_block_infos(const common::ObStoreRange& range, common::ObIArray<ObMicroBlockInfo>& micro_block_infos);
  inline const Iterator& begin() const
  {
    return begin_;
  }
  inline const Iterator& end() const
  {
    return end_;
  }
  inline int64_t block_count() const
  {
    return block_count_;
  }
  int get_all_mark_deletions_flags(bool* mark_deletion_flags);
  inline int64_t get_mark_deletion_flags_size() const
  {
    return NULL == mark_deletion_stream_ ? 0 : sizeof(char) * block_count_;
  }
  int get_all_deltas(int32_t* delta_array);
  inline int64_t get_delta_size() const
  {
    return nullptr == delta_array_ ? 0 : sizeof(int32_t) * block_count_;
  }

private:
  int init(const char* index_buf, const common::ObObjMeta* column_type_array, const int32_t row_key_column_cnt,
      const int32_t micro_block_cnt, const int32_t index_buf_size, const int32_t endkey_buf_size,
      const int32_t mark_deletion_buf_size, const int32_t delta_buf_size, const int32_t data_base_offset,
      const common::ObRowStoreType row_store_type);
  class ObBlockIndexCompare {
  public:
    ObBlockIndexCompare(ObMicroBlockIndexReader& index_reader, common::ObObj* objs, const int64_t row_key_column_number)
        : index_reader_(index_reader),
          objs_(objs),
          row_key_column_number_(row_key_column_number),
          ret_(common::OB_SUCCESS)
    {}
    inline bool operator()(const uint64_t index, const common::ObStoreRowkey& key)
    {
      int bret = false;
      int ret = common::OB_SUCCESS;
      if (OB_LIKELY(common::OB_SUCCESS == ret_)) {
        if (OB_FAIL(index_reader_.get_end_key(index, objs_))) {
          ret_ = ret;
          STORAGE_LOG(WARN, "failed to get next rowkey from transformer", K(ret), K(index));
        } else {
          common::ObStoreRowkey cur_key(objs_, row_key_column_number_);
          bret = cur_key.compare(key) < 0;
        }
      }
      return bret;
    }
    inline int get_ret()
    {
      return ret_;
    }

  private:
    ObMicroBlockIndexReader& index_reader_;
    common::ObObj* objs_;
    const int64_t row_key_column_number_;
    int ret_;
  };

private:
  int get_end_keys(const Iterator& begin, const Iterator& end, common::ObIAllocator& allocator,
      common::ObIArray<common::ObStoreRowkey>& end_keys);
  int get_micro_block_infos(
      const Iterator& begin, const Iterator& end, common::ObIArray<ObMicroBlockInfo>& micro_block_infos);
  int locate_start_index(const common::ObStoreRange& range, Iterator& start_index);
  int locate_end_index(const common::ObStoreRange& range, Iterator& end_index);
  int lower_bound(const common::ObStoreRowkey& key, common::ObObj* objs, Iterator& index);
  int init_row_reader(const ObRowStoreType row_store_type);

private:
  ObIRowReader* row_reader_;
  ObFlatRowReader flat_row_reader_;
  ObSparseRowReader sparse_row_reader_;
  const common::ObObjMeta* column_type_array_;
  const ObMicroBlockIndex* micro_indexes_;  // address of the micro block index
  const char* endkey_stream_;               // address of the endkey stream
  const char* mark_deletion_stream_;        // address of the mark deletion stream
  const int32_t* delta_array_;
  int32_t block_count_;  // the count of the micro blocks
  int32_t row_key_column_cnt_;
  int32_t data_base_offset_;
  Iterator begin_;  // begin iterator of the index
  Iterator end_;    // end iterator of the index
  bool is_inited_;
};

}  // namespace blocksstable
}  // namespace oceanbase
#endif /* OCEANBASE_BLOCKSSTABLE_OB_MICRO_BLOCK_INDEX_READER_H */
