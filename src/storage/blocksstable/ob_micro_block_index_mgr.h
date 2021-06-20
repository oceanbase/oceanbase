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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_INDEX_MGR_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_INDEX_MGR_H_
#include "lib/container/ob_se_array.h"
#include "common/object/ob_object.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
namespace storage {
class ObRowkeyObjComparer;
}
namespace blocksstable {
struct ObMicroBlockInfo {
  static const int64_t SF_BIT_OFFSET = 24;
  static const int64_t MAX_OFFSET = (1 << (SF_BIT_OFFSET - 1)) - 1;
  static const int64_t SF_BIT_SIZE = 23;
  static const int64_t MAX_SIZE = (1 << (SF_BIT_SIZE - 1)) - 1;
  static const int64_t SF_BIT_INDEX = 16;
  static const uint64_t MAX_INDEX = (1 << (SF_BIT_INDEX)) - 1;
  static const int64_t SF_BIT_MARK_DELETION = 1;
  static const uint64_t MAX_MARK_DELETION = (1 << (SF_BIT_MARK_DELETION)) - 1;
  struct {
    int32_t offset_ : SF_BIT_OFFSET;
    int32_t size_ : SF_BIT_SIZE;
    uint16_t index_ : SF_BIT_INDEX;
    bool mark_deletion_ : SF_BIT_MARK_DELETION;
  };
  ObMicroBlockInfo() : offset_(0), size_(0), index_(0), mark_deletion_(false)
  {}

  void reset()
  {
    offset_ = 0;
    size_ = 0;
    index_ = 0;
    mark_deletion_ = false;
  }
  int set(const int32_t offset, const int32_t size, const int64_t index, bool mark_deletion = false)
  {
    int ret = common::OB_SUCCESS;
    if (!is_offset_valid(offset) || !is_size_valid(size) || !is_index_valid(index)) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Setting invalid value, it may be caused by overflow when converting type");
    } else {
      offset_ = offset & MAX_OFFSET;
      size_ = size & MAX_SIZE;
      index_ = index & MAX_INDEX;
      mark_deletion_ = mark_deletion & MAX_MARK_DELETION;
    }
    return ret;
  }
  bool is_offset_valid(const int32_t offset) const
  {
    return offset >= 0 && offset <= MAX_OFFSET;
  }
  bool is_size_valid(const int32_t size) const
  {
    return size > 0 && size <= MAX_SIZE;
  }
  bool is_index_valid(const int64_t index) const
  {
    return index >= 0 && index <= MAX_INDEX;
  }
  bool is_valid() const
  {
    return offset_ >= 0 && size_ > 0;
  }
  TO_STRING_KV(K_(offset), K_(size), K_(index), K_(mark_deletion));
};

struct ObMicroBlockIndex {
  int32_t data_offset_;
  int32_t endkey_offset_;
  ObMicroBlockIndex() : data_offset_(0), endkey_offset_(0)
  {}

  bool operator==(const ObMicroBlockIndex& other) const;
  bool operator!=(const ObMicroBlockIndex& other) const;
  bool operator<(const ObMicroBlockIndex& other) const;
  inline bool is_valid() const
  {
    return data_offset_ >= 0 && endkey_offset_ >= 0;
  }
  TO_STRING_KV(K_(data_offset), K_(endkey_offset));

private:
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockIndex);
};

struct ObMicroIndexNode {
  common::ObObj obj_;
  int32_t first_micro_index_;
  int32_t first_child_index_;
  int32_t child_num_;
  ObMicroIndexNode() : obj_(), first_micro_index_(0), first_child_index_(0), child_num_(0)
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(obj), K_(first_micro_index), K_(first_child_index), K_(child_num));

private:
  DISALLOW_COPY_AND_ASSIGN(ObMicroIndexNode);
};

class ObMicroBlockIndexMgr : public common::ObIKVCacheValue {
public:
  struct MemMicroIndexItem {
    int32_t data_offset_;
    MemMicroIndexItem() : data_offset_(0)
    {}
    TO_STRING_KV(K_(data_offset));

  private:
    DISALLOW_COPY_AND_ASSIGN(MemMicroIndexItem);
  };
  typedef const MemMicroIndexItem* const_cursor;

  struct Bound {
    const_cursor start_;
    const_cursor end_;
    Bound() : start_(NULL), end_(NULL)
    {}
    bool is_valid() const;
    TO_STRING_KV(K_(start), K_(end));

  private:
    DISALLOW_COPY_AND_ASSIGN(Bound);
  };

private:
  class Compare {
  public:
    OB_INLINE bool operator()(const ObMicroIndexNode& node, const int64_t micro_block_index);
  };
  struct SearchContext {
    const ObMicroIndexNode* begin_node_;
    const ObMicroIndexNode* end_node_;
    const ObMicroIndexNode* father_node_;
    const ObMicroIndexNode* next_node_;
    int64_t column_idx_;
    TO_STRING_KV(K_(begin_node), K_(end_node), K_(father_node), K_(next_node), K_(column_idx));
  };

public:
  ObMicroBlockIndexMgr();
  virtual ~ObMicroBlockIndexMgr()
  {}

  int init(const ObFullMacroBlockMeta& meta, const int64_t node_array_size, const int64_t extra_space_size);
  int search_blocks(const common::ObStoreRange& range, const bool is_left_border, const bool is_right_border,
      common::ObIArray<ObMicroBlockInfo>& infos,
      const common::ObIArray<storage::ObRowkeyObjComparer*>* cmp_funcs = nullptr) const;
  int search_blocks(const common::ObStoreRowkey& rowkey, ObMicroBlockInfo& info,
      const common::ObIArray<storage::ObRowkeyObjComparer*>* cmp_funcs = nullptr) const;
  int search_blocks(const int64_t micro_block_index, common::ObIArray<ObMicroBlockInfo>& infos) const;
  int get_block_count(const common::ObStoreRange& range, const bool is_left_border, const bool is_right_border,
      int64_t& block_count, const common::ObIArray<storage::ObRowkeyObjComparer*>* cmp_funcs = nullptr) const;

  int compare_endkey(const int32_t micro_block_index, const common::ObStoreRowkey& rowkey, int& cmp_ret) const;
  int get_endkey(const int32_t micro_block_index, common::ObIAllocator& allocator, common::ObStoreRowkey& endkey) const;
  int get_endkeys(common::ObIAllocator& allocator, common::ObIArray<common::ObStoreRowkey>& endkeys) const;
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, common::ObIKVCacheValue*& value) const;
  int cal_border_row_count(const common::ObStoreRange& range, const bool is_left_border, const bool is_right_border,
      int64_t& logical_row_count, int64_t& physical_row_count, bool& need_check_micro_block) const;
  // calculate row count can be purged in this macro block
  int cal_macro_purged_row_count(int64_t& purged_row_count) const;

private:
  void get_bound(Bound& bound) const;
  int get_iterator_bound(const Bound& bound, const common::ObStoreRange& range, const bool is_left_border,
      const bool is_right_border, const_cursor& start, const_cursor& end,
      const common::ObIArray<storage::ObRowkeyObjComparer*>* cmp_funcs) const;
  int find_by_rowkey(const common::ObStoreRowkey& rowkey, const_cursor& cursor, bool& is_equal,
      const common::ObIArray<storage::ObRowkeyObjComparer*>* cmp_funcs) const;
  int find_by_range(const common::ObStoreRange& range, const_cursor& left_cursor, bool& is_left_equal,
      const_cursor& right_cursor, bool& is_right_equal,
      const common::ObIArray<storage::ObRowkeyObjComparer*>* cmp_funcs) const;
  int get_micro_info(const_cursor cursor, ObMicroBlockInfo& micro_info) const;
  int store_micro_infos(const_cursor start, const_cursor end, common::ObIArray<ObMicroBlockInfo>& micro_array) const;
  bool is_multi_version() const
  {
    return schema_rowkey_col_cnt_ != rowkey_column_count_;
  }

  int inner_find_rowkey(const common::ObStoreRowkey& rowkey, const SearchContext* search_ctx, const_cursor& cursor,
      bool& is_equal, const common::ObIArray<storage::ObRowkeyObjComparer*>* cmp_funcs,
      int64_t* first_column_node_offset = nullptr) const;

private:
  MemMicroIndexItem* index_array_;
  ObMicroIndexNode* node_array_;
  char* extra_space_base_;  // reserved space for deep copy string and number
  bool* mark_deletion_array_;
  int32_t* delta_array_;

  int32_t micro_index_size_;
  int32_t node_array_size_;
  int32_t extra_space_size_;
  int32_t mark_deletion_flags_size_;
  int32_t delta_size_;

  int32_t micro_count_;
  int32_t rowkey_column_count_;
  int32_t schema_rowkey_col_cnt_;
  int32_t data_offset_;  // data offset of macro block
  bool is_inited_;
  int32_t row_count_;        // macro block row count
  int32_t row_count_delta_;  // row_count_delta of this macro block
private:
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockIndexMgr);
};
}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
