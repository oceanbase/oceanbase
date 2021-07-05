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

#include "ob_micro_block_index_mgr.h"
#include "common/ob_range.h"
#include "storage/ob_sstable_rowkey_helper.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {
bool ObMicroBlockIndex::operator==(const ObMicroBlockIndex& other) const
{
  return data_offset_ == other.data_offset_;
}

bool ObMicroBlockIndex::operator!=(const ObMicroBlockIndex& other) const
{
  return data_offset_ != other.data_offset_;
}

bool ObMicroBlockIndex::operator<(const ObMicroBlockIndex& other) const
{
  return data_offset_ < other.data_offset_;
}

bool ObMicroIndexNode::is_valid() const
{
  return obj_.is_valid_type() && first_micro_index_ >= 0 && first_child_index_ >= 0 && child_num_ >= 0;
}

bool ObMicroBlockIndexMgr::Bound::is_valid() const
{
  return NULL != start_ && NULL != end_ && start_ < end_;
}

OB_INLINE bool ObMicroBlockIndexMgr::Compare::operator()(const ObMicroIndexNode& node, const int64_t micro_block_index)
{
  return node.first_micro_index_ < micro_block_index;
}

ObMicroBlockIndexMgr::ObMicroBlockIndexMgr()
    : index_array_(NULL),
      node_array_(NULL),
      extra_space_base_(NULL),
      mark_deletion_array_(NULL),
      delta_array_(NULL),
      micro_index_size_(0),
      node_array_size_(0),
      extra_space_size_(0),
      mark_deletion_flags_size_(0),
      delta_size_(0),
      micro_count_(0),
      rowkey_column_count_(0),
      schema_rowkey_col_cnt_(0),
      data_offset_(0),
      is_inited_(false),
      row_count_(0),
      row_count_delta_(0)
{}

int ObMicroBlockIndexMgr::init(
    const ObFullMacroBlockMeta& meta, const int64_t node_array_size, const int64_t extra_space_size)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMicroBlockIndexMgr has inited, ", K(ret));
  } else if (!meta.is_valid() || node_array_size <= 0 || extra_space_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "invalid micro block index mgr input param,", K(ret), K(meta), K(node_array_size), K(extra_space_size));
  } else {
    const ObMacroBlockMetaV2& macro_meta = *meta.meta_;
    const int64_t block_count = macro_meta.micro_block_count_;
    const int64_t micro_index_size = (block_count + 1) * sizeof(ObMicroBlockIndexMgr::MemMicroIndexItem);
    const int64_t mark_deletion_flags_size = macro_meta.get_micro_block_mark_deletion_size();
    const int64_t delta_size = macro_meta.get_micro_block_delta_size();
    const int64_t data_offset = macro_meta.micro_block_data_offset_;
    if ((0 == mark_deletion_flags_size && 0 < delta_size) || (0 == delta_size && 0 < mark_deletion_flags_size)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN,
          "mark_deletion_flags_size and delta_size should both be 0 or both greater than 0",
          K(ret),
          K(mark_deletion_flags_size),
          K(delta_size));
    } else {
      index_array_ = reinterpret_cast<MemMicroIndexItem*>(reinterpret_cast<char*>(this) + sizeof(ObMicroBlockIndexMgr));
      node_array_ = reinterpret_cast<ObMicroIndexNode*>(reinterpret_cast<char*>(index_array_) + micro_index_size);
      extra_space_base_ = reinterpret_cast<char*>(reinterpret_cast<char*>(node_array_) + node_array_size);

      mark_deletion_array_ =
          0 == mark_deletion_flags_size
              ? NULL
              : reinterpret_cast<bool*>(reinterpret_cast<char*>(node_array_) + node_array_size + extra_space_size);
      delta_array_ = 0 == delta_size ? NULL
                                     : reinterpret_cast<int32_t*>(reinterpret_cast<char*>(extra_space_base_) +
                                                                  extra_space_size + mark_deletion_flags_size);
      micro_index_size_ = static_cast<int32_t>(micro_index_size);
      node_array_size_ = static_cast<int32_t>(node_array_size);
      extra_space_size_ = static_cast<int32_t>(extra_space_size);
      mark_deletion_flags_size_ = static_cast<int32_t>(mark_deletion_flags_size);
      delta_size_ = static_cast<int32_t>(delta_size);
      micro_count_ = static_cast<int32_t>(block_count);
      rowkey_column_count_ = static_cast<int32_t>(macro_meta.rowkey_column_number_);
      schema_rowkey_col_cnt_ = static_cast<int32_t>(meta.schema_->schema_rowkey_col_cnt_);
      data_offset_ = static_cast<int32_t>(data_offset);
      row_count_ = macro_meta.row_count_;
      row_count_delta_ = macro_meta.row_count_delta_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMicroBlockIndexMgr::search_blocks(const ObStoreRange& range, const bool is_left_border,
    const bool is_right_border, ObIArray<ObMicroBlockInfo>& infos,
    const ObIArray<ObRowkeyObjComparer*>* cmp_funcs) const
{
  int ret = OB_SUCCESS;
  infos.reuse();
  Bound bound;
  const_cursor start = NULL;
  const_cursor end = NULL;
  get_bound(bound);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexMgr should be inited first, ", K(ret));
  } else if (OB_FAIL(get_iterator_bound(bound, range, is_left_border, is_right_border, start, end, cmp_funcs))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "micro block index mgr fail to get iterator bound.", K(ret), K(bound), K(range));
    }
  } else if (OB_FAIL(store_micro_infos(start, end, infos))) {
    STORAGE_LOG(WARN, "micro block index mgr fail to store micro infos.", K(ret), K(OB_P(start)), K(OB_P(end)));
  }
  return ret;
}

int ObMicroBlockIndexMgr::get_block_count(const common::ObStoreRange& range, const bool is_left_border,
    const bool is_right_border, int64_t& block_count,
    const common::ObIArray<storage::ObRowkeyObjComparer*>* cmp_funcs) const
{
  int ret = OB_SUCCESS;
  block_count = 0;
  Bound bound;
  const_cursor start = NULL;
  const_cursor end = NULL;
  get_bound(bound);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexMgr should be inited first, ", K(ret));
  } else if (OB_FAIL(get_iterator_bound(bound, range, is_left_border, is_right_border, start, end, cmp_funcs))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "micro block index mgr fail to get iterator bound.", K(ret), K(bound), K(range));
    }
  } else {
    block_count = end - start + 1;
  }
  return ret;
}

int ObMicroBlockIndexMgr::search_blocks(
    const ObStoreRowkey& rowkey, ObMicroBlockInfo& info, const ObIArray<ObRowkeyObjComparer*>* cmp_funcs) const
{
  int ret = OB_SUCCESS;
  info.reset();
  bool is_equal = false;
  Bound bound;
  get_bound(bound);
  const_cursor cursor = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexMgr should be inited first, ", K(ret));
  } else if (OB_FAIL(find_by_rowkey(rowkey, cursor, is_equal, cmp_funcs))) {
    STORAGE_LOG(WARN, "micro block index mgr fail to find by rowkey.", K(ret), K(rowkey), K(is_equal));
  } else if (OB_UNLIKELY(cursor == bound.end_)) {
    ret = OB_BEYOND_THE_RANGE;
  } else if (OB_FAIL(get_micro_info(cursor, info))) {
    STORAGE_LOG(WARN, "micro block index mgr fail to store micro infos.", K(ret), K(cursor));
  }
  return ret;
}

int ObMicroBlockIndexMgr::search_blocks(const int64_t micro_block_index, ObIArray<ObMicroBlockInfo>& infos) const
{
  int ret = OB_SUCCESS;
  ObMicroBlockInfo info;
  int32_t offset = -1;
  int32_t size = 0;
  bool mark_deletion = false;
  infos.reuse();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexMgr should be inited first, ", K(ret));
  } else if (OB_UNLIKELY(micro_block_index >= micro_count_)) {
    ret = OB_ERR_WRONG_ROWID;
    STORAGE_LOG(ERROR,
        "unexpected rowid error, micro_block_index in rowid is wrong!",
        K(ret),
        K(micro_block_index),
        K_(micro_count));
  } else {
    const_cursor cursor = index_array_ + micro_block_index;
    offset = cursor->data_offset_ + data_offset_;
    size = (cursor + 1)->data_offset_ - cursor->data_offset_;

    if (OB_NOT_NULL(mark_deletion_array_) && micro_block_index < micro_count_) {
      mark_deletion = mark_deletion_array_[micro_block_index];
    }

    if (OB_FAIL(info.set(offset, size, micro_block_index, mark_deletion))) {
      STORAGE_LOG(WARN, "fail to set info", K(ret), K(offset), K(size), K(micro_block_index));
    } else if (OB_FAIL(infos.push_back(info))) {
      STORAGE_LOG(WARN, "micro block info array fail to push back micro info.", K(ret));
    }
  }
  return ret;
}

int64_t ObMicroBlockIndexMgr::size() const
{
  return sizeof(ObMicroBlockIndexMgr) + micro_index_size_ + node_array_size_ + extra_space_size_ +
         mark_deletion_flags_size_ + delta_size_;
}

int ObMicroBlockIndexMgr::deep_copy(char* buf, const int64_t buf_len, common::ObIKVCacheValue*& value) const
{
  int ret = OB_SUCCESS;
  ObMicroBlockIndexMgr* mgr = new (buf) ObMicroBlockIndexMgr();
  int64_t pos = sizeof(ObMicroBlockIndexMgr);

  if (NULL == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret), KP(buf), K(buf_len), "size:", size());
  } else {
    MEMCPY(buf + pos, index_array_, micro_index_size_);
    mgr->index_array_ = reinterpret_cast<MemMicroIndexItem*>(buf + pos);
    mgr->micro_index_size_ = micro_index_size_;
    pos += micro_index_size_;

    MEMCPY(buf + pos, node_array_, node_array_size_);
    ObMicroIndexNode* node_array = reinterpret_cast<ObMicroIndexNode*>(buf + pos);
    mgr->node_array_ = reinterpret_cast<ObMicroIndexNode*>(buf + pos);
    mgr->node_array_size_ = node_array_size_;
    pos += node_array_size_;

    mgr->extra_space_base_ = buf + pos;
    mgr->extra_space_size_ = extra_space_size_;
    for (int64_t i = 0; OB_SUCC(ret) && (i < static_cast<int64_t>(node_array_size_ / sizeof(ObMicroIndexNode))); ++i) {
      if (OB_FAIL(node_array[i].obj_.deep_copy(node_array_[i].obj_, buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "Fail to deep copy obj, ", K(ret), K(i), K(node_array_[i].obj_), K(buf_len), K(pos));
      }
    }
    // mark deletion
    if (OB_SUCC(ret)) {
      if (NULL != mark_deletion_array_) {
        MEMCPY(buf + pos, mark_deletion_array_, mark_deletion_flags_size_);
        mgr->mark_deletion_array_ = reinterpret_cast<bool*>(buf + pos);
        mgr->mark_deletion_flags_size_ = mark_deletion_flags_size_;
        pos += mark_deletion_flags_size_;
      } else {
        mgr->mark_deletion_array_ = NULL;
        mgr->mark_deletion_flags_size_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (NULL != delta_array_) {
        MEMCPY(buf + pos, delta_array_, delta_size_);
        mgr->delta_array_ = reinterpret_cast<int32_t*>(buf + pos);
        mgr->delta_size_ = delta_size_;
        pos += delta_size_;
      } else {
        mgr->delta_array_ = NULL;
        mgr->delta_size_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      mgr->micro_count_ = micro_count_;
      mgr->rowkey_column_count_ = rowkey_column_count_;
      mgr->data_offset_ = data_offset_;
      mgr->row_count_ = row_count_;
      mgr->row_count_delta_ = row_count_delta_;
      mgr->is_inited_ = is_inited_;
      value = mgr;
    }
  }

  return ret;
}

int ObMicroBlockIndexMgr::cal_border_row_count(const ObStoreRange& range, const bool is_left_border,
    const bool is_right_border, int64_t& logical_row_count, int64_t& physical_row_count,
    bool& need_check_micro_block) const
{
  int ret = OB_SUCCESS;
  Bound bound;
  const_cursor start = NULL;
  const_cursor end = NULL;
  need_check_micro_block = false;
  get_bound(bound);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == delta_array_ || NULL == mark_deletion_array_)) {
    // this block index does not have row count delta or mark deletion array
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "range is not valid", K(ret), K(range));
  } else if (OB_FAIL(get_iterator_bound(bound, range, is_left_border, is_right_border, start, end, nullptr))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "failed to get iterator bound", K(ret), K(range));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    const int64_t count = end - start + 1;
    need_check_micro_block = count == 1;
    logical_row_count += (row_count_delta_ * count) / micro_count_;
    physical_row_count += (row_count_ * count) / micro_count_;
    if (row_count_delta_ <= 0) {
      const int64_t gap_ratio = 1 + (row_count_delta_ < 0);
      const int32_t avg_micro_row_count = row_count_ / micro_count_;
      int64_t index = start - index_array_;
      const int64_t end_index = end - index_array_;
      int64_t gap_size = 0;
      for (; OB_SUCC(ret) && index <= end_index; ++index) {
        if (mark_deletion_array_[index]) {
          gap_size += avg_micro_row_count;
        } else {
          if (gap_size >= common::OB_SKIP_RANGE_LIMIT) {
            physical_row_count -= gap_size * gap_ratio;
          }
          gap_size = 0;
        }
      }
      if (gap_size >= common::OB_SKIP_RANGE_LIMIT) {
        physical_row_count -= gap_size * gap_ratio;
      }
    }
  }
  return ret;
}

int ObMicroBlockIndexMgr::cal_macro_purged_row_count(int64_t& purged_row_count) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexMgr is not inited", K(ret));
  } else if (NULL == mark_deletion_array_) {
  } else {
    const int64_t avg_micro_row_count = row_count_ / micro_count_;
    const int64_t gap_ratio = 1 + (row_count_delta_ < 0);
    int64_t gap_size = 0;
    purged_row_count = 0;
    for (int i = 0; OB_SUCC(ret) && i < micro_count_; ++i) {
      if (mark_deletion_array_[i]) {
        gap_size += avg_micro_row_count;
      } else {
        if (gap_size >= common::OB_SKIP_RANGE_LIMIT) {
          purged_row_count += gap_size * gap_ratio;
        }
        gap_size = 0;
      }
    }
    if (gap_size >= common::OB_SKIP_RANGE_LIMIT) {
      purged_row_count += gap_size * gap_ratio;
    }
  }
  return ret;
}

OB_INLINE void ObMicroBlockIndexMgr::get_bound(Bound& bound) const
{
  bound.start_ = index_array_;
  bound.end_ = bound.start_ + micro_count_;
}

int ObMicroBlockIndexMgr::get_iterator_bound(const Bound& bound, const ObStoreRange& range, const bool is_left_border,
    const bool is_right_border, const_cursor& start, const_cursor& end,
    const ObIArray<ObRowkeyObjComparer*>* cmp_funcs) const
{
  int ret = OB_SUCCESS;
  const bool need_search_left = is_left_border && !range.get_start_key().is_min();
  const bool need_search_right = is_right_border && !range.get_end_key().is_max();
  bool is_left_equal = false;
  bool is_right_equal = false;
  if (!need_search_left && !need_search_right) {
    start = bound.start_;
    end = bound.end_ - 1;
  } else if (need_search_left && need_search_right) {
    if (OB_FAIL(find_by_range(range, start, is_left_equal, end, is_right_equal, cmp_funcs))) {
      STORAGE_LOG(WARN, "fail to find by range", K(ret), K(range));
    }
  } else if (need_search_left) {
    end = bound.end_ - 1;
    if (OB_FAIL(find_by_rowkey(range.get_start_key(), start, is_left_equal, cmp_funcs))) {
      STORAGE_LOG(WARN,
          "micro block index mgr fail to find by rowkey.",
          K(ret),
          "rowkey",
          range.get_start_key(),
          K(is_left_equal));
    }
  } else if (need_search_right) {
    start = bound.start_;
    if (OB_FAIL(find_by_rowkey(range.get_end_key(), end, is_right_equal, cmp_funcs))) {
      STORAGE_LOG(WARN,
          "micro block index mgr fail to find by rowkey.",
          K(ret),
          "endkey",
          range.get_end_key(),
          K(is_right_equal));
    }
  }

  if (OB_SUCC(ret) && need_search_left) {
    if (OB_UNLIKELY(start == bound.end_)) {
      ret = OB_BEYOND_THE_RANGE;
    } else if (is_left_equal) {
      if (!range.get_border_flag().inclusive_start()) {
        start++;
        if (start >= bound.end_) {
          ret = OB_BEYOND_THE_RANGE;
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_search_right) {
    if (OB_UNLIKELY(end >= bound.end_)) {
      end--;
    }
    if (OB_UNLIKELY(end < bound.start_)) {
      ret = OB_BEYOND_THE_RANGE;
    }
  }
  return ret;
}

OB_INLINE static const ObMicroIndexNode* obj_lower_bound(
    const ObMicroIndexNode* begin, const ObMicroIndexNode* end, const ObObj& rowkey, bool& is_equal)
{
  is_equal = false;
  const ObMicroIndexNode* middle = nullptr;
  int64_t count = end - begin;
  int64_t step = 0;
  int cmp = 0;
  while (count > 0) {
    step = count >> 1;
    middle = begin + step;
    cmp = middle->obj_.compare(rowkey);
    if (0 == cmp) {
      begin = middle;
      is_equal = true;
      break;
    } else if (cmp < 0) {
      begin = middle + 1;
      count -= step + 1;
    } else {
      count = step;
    }
  }
  return begin;
}

OB_INLINE static const ObMicroIndexNode* obj_lower_bound_with_func(const ObMicroIndexNode* begin,
    const ObMicroIndexNode* end, const ObObj& rowkey, bool& is_equal, ObRowkeyObjComparer& cmp_func)
{
  is_equal = false;
  const ObMicroIndexNode* middle = nullptr;
  int64_t count = end - begin;
  int64_t step = 0;
  int cmp = 0;
  while (count > 0) {
    step = count >> 1;
    middle = begin + step;
    // rowkey may be max/min, but micro block index never
    cmp = cmp_func.compare_semi_safe(middle->obj_, rowkey);
    if (0 == cmp) {
      begin = middle;
      is_equal = true;
      break;
    } else if (cmp < 0) {
      begin = middle + 1;
      count -= step + 1;
    } else {
      count = step;
    }
  }
  return begin;
}

int ObMicroBlockIndexMgr::find_by_rowkey(const ObStoreRowkey& rowkey, const_cursor& cursor, bool& is_equal,
    const ObIArray<ObRowkeyObjComparer*>* cmp_funcs) const
{

  return inner_find_rowkey(rowkey, nullptr, cursor, is_equal, cmp_funcs);
}

OB_INLINE static void init_search_context(const ObMicroIndexNode* node_array, const ObMicroIndexNode*& begin,
    const ObMicroIndexNode*& end, const ObMicroIndexNode*& father, const ObMicroIndexNode*& next)
{
  begin = node_array + node_array[0].first_child_index_;
  end = begin + node_array[0].child_num_;
  father = node_array;
  next = &node_array[1];
}

int ObMicroBlockIndexMgr::find_by_range(const ObStoreRange& range, const_cursor& left_cursor, bool& is_left_equal,
    const_cursor& right_cursor, bool& is_right_equal, const ObIArray<ObRowkeyObjComparer*>* cmp_funcs) const
{
  int ret = OB_SUCCESS;
  left_cursor = nullptr;
  right_cursor = nullptr;
  is_left_equal = false;
  is_right_equal = false;

  const ObObj* rowkey_objs = range.get_start_key().get_obj_ptr();
  const int64_t rowkey_column_cnt = min(range.get_start_key().get_obj_cnt(), rowkey_column_count_);
  int64_t common_prefix_column_cnt = 0;
  ObStoreRowkey::get_common_prefix_length(range.get_start_key(), range.get_end_key(), common_prefix_column_cnt);
  if (OB_UNLIKELY(common_prefix_column_cnt == rowkey_column_cnt)) {  // range start is equal to range end
    if (OB_FAIL(inner_find_rowkey(range.get_start_key(), nullptr, left_cursor, is_left_equal, cmp_funcs))) {
      STORAGE_LOG(WARN, "fail to inner find rowkey", K(ret), "rowkey", range.get_start_key());
    } else {
      right_cursor = left_cursor;
      is_right_equal = is_left_equal;
    }
  } else if (common_prefix_column_cnt == 0) {
    int64_t node_offset = 0;
    if (OB_FAIL(
            inner_find_rowkey(range.get_start_key(), nullptr, left_cursor, is_left_equal, cmp_funcs, &node_offset))) {
      STORAGE_LOG(WARN, "fail to inner find row key", K(ret), "rowkey", range.get_start_key());
    } else {
      SearchContext right_search_ctx;
      init_search_context(node_array_,
          right_search_ctx.begin_node_,
          right_search_ctx.end_node_,
          right_search_ctx.father_node_,
          right_search_ctx.next_node_);
      right_search_ctx.begin_node_ += node_offset;
      right_search_ctx.column_idx_ = 0;
      if (OB_FAIL(inner_find_rowkey(range.get_end_key(), &right_search_ctx, right_cursor, is_right_equal, cmp_funcs))) {
        STORAGE_LOG(WARN, "fail to inner find row key", K(ret), "rowkey", range.get_end_key(), K(right_search_ctx));
      }
    }
  } else {  // common_prefix_column_cnt > 0 && common_prefix_column_cnt < rowkey_column_cnt
    const ObMicroIndexNode* begin = nullptr;
    const ObMicroIndexNode* end = nullptr;
    const ObMicroIndexNode* father = nullptr;
    const ObMicroIndexNode* next = nullptr;
    init_search_context(node_array_, begin, end, father, next);
    const ObMicroIndexNode* target = nullptr;
    bool obj_equal = false;
    bool need_compare_postfix = true;
    for (int64_t i = 0; i < common_prefix_column_cnt; ++i) {
      if (OB_ISNULL(cmp_funcs) || OB_ISNULL(cmp_funcs->at(i))) {
        target = obj_lower_bound(begin, end, rowkey_objs[i], obj_equal);
      } else {
        target = obj_lower_bound_with_func(begin, end, rowkey_objs[i], obj_equal, *cmp_funcs->at(i));
      }
      if (target == end) {  // not found
        target = next;
        need_compare_postfix = false;
        break;
      } else if (!obj_equal) {  // found bigger
        need_compare_postfix = false;
        break;
      } else {  // found equal
        if (target < node_array_ + father->first_child_index_ + father->child_num_ - 1) {
          next = target + 1;
        }
        begin = node_array_ + target->first_child_index_;
        end = begin + target->child_num_;
        father = target;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!need_compare_postfix) {
      left_cursor = index_array_ + target->first_micro_index_;
      is_left_equal = false;
      right_cursor = left_cursor;
      is_right_equal = is_left_equal;
    } else {
      SearchContext search_ctx;
      search_ctx.begin_node_ = begin;
      search_ctx.end_node_ = end;
      search_ctx.father_node_ = father;
      search_ctx.next_node_ = next;
      search_ctx.column_idx_ = common_prefix_column_cnt;
      if (OB_FAIL(inner_find_rowkey(range.get_start_key(), &search_ctx, left_cursor, is_left_equal, cmp_funcs))) {
        STORAGE_LOG(WARN, "fail to inner find row key", K(ret), "rowkey", range.get_start_key(), K(search_ctx));
      } else if (OB_FAIL(
                     inner_find_rowkey(range.get_end_key(), &search_ctx, right_cursor, is_right_equal, cmp_funcs))) {
        STORAGE_LOG(WARN, "fail to inner find row key", K(ret), "rowkey", range.get_end_key(), K(search_ctx));
      }
    }
  }

  return ret;
}

int ObMicroBlockIndexMgr::inner_find_rowkey(const ObStoreRowkey& rowkey, const SearchContext* search_ctx,
    const_cursor& cursor, bool& is_equal, const ObIArray<ObRowkeyObjComparer*>* cmp_funcs,
    int64_t* first_column_node_offset) const
{
  int ret = OB_SUCCESS;
  cursor = nullptr;
  is_equal = false;
  const ObMicroIndexNode* begin = nullptr;
  const ObMicroIndexNode* end = nullptr;
  const ObMicroIndexNode* father = nullptr;
  const ObMicroIndexNode* next = nullptr;
  const ObMicroIndexNode* target = nullptr;
  int64_t begin_column_idx = 0;
  if (OB_ISNULL(search_ctx)) {
    init_search_context(node_array_, begin, end, father, next);
  } else {
    begin = search_ctx->begin_node_;
    end = search_ctx->end_node_;
    father = search_ctx->father_node_;
    next = search_ctx->next_node_;
    begin_column_idx = search_ctx->column_idx_;
  }

  const int64_t rowkey_column_cnt = min(rowkey_column_count_, rowkey.get_obj_cnt());
  const ObObj* rowkey_objs = rowkey.get_obj_ptr();
  bool obj_equal = false;
  for (int64_t i = begin_column_idx; i < rowkey_column_cnt; ++i) {
    if (OB_ISNULL(cmp_funcs) || OB_ISNULL(cmp_funcs->at(i))) {
      target = obj_lower_bound(begin, end, rowkey_objs[i], obj_equal);
    } else {
      target = obj_lower_bound_with_func(begin, end, rowkey_objs[i], obj_equal, *cmp_funcs->at(i));
    }
    if (OB_UNLIKELY(nullptr != first_column_node_offset && 0 == i)) {
      *first_column_node_offset = target - begin;
    }
    if (target == end) {  // not found
      target = next;
      break;
    } else if (!obj_equal) {  // found bigger
      break;
    } else {  // found equal
      if (OB_UNLIKELY(rowkey_column_cnt - 1 == i)) {
        is_equal = rowkey_column_count_ == rowkey_column_cnt && 0 == target->child_num_;
        break;
      }
      if (target < node_array_ + father->first_child_index_ + father->child_num_ - 1) {
        next = target + 1;
      }
      begin = node_array_ + target->first_child_index_;
      end = begin + target->child_num_;
      father = target;
    }
  }

  if (OB_SUCC(ret)) {
    cursor = index_array_ + target->first_micro_index_;
  }
  return ret;
}

int ObMicroBlockIndexMgr::get_micro_info(const_cursor cursor, ObMicroBlockInfo& micro_info) const
{
  int ret = OB_SUCCESS;
  micro_info.reset();

  if (OB_UNLIKELY(NULL == cursor)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret));
  } else {
    const int32_t offset = cursor->data_offset_ + data_offset_;
    const int32_t size = (cursor + 1)->data_offset_ - cursor->data_offset_;
    const int64_t index = cursor - index_array_;
    bool mark_deletion = false;

    if (OB_NOT_NULL(mark_deletion_array_) && index < micro_count_) {
      mark_deletion = mark_deletion_array_[index];
    }

    if (OB_FAIL(micro_info.set(offset, size, index, mark_deletion))) {
      STORAGE_LOG(WARN, "fail to set info", K(ret), K(offset), K(size), K(index));
    }
  }
  return ret;
}

int ObMicroBlockIndexMgr::store_micro_infos(
    const_cursor start, const_cursor end, common::ObIArray<ObMicroBlockInfo>& micro_array) const
{
  int ret = OB_SUCCESS;
  ObMicroBlockInfo info;
  const_cursor cursor = NULL;
  micro_array.reuse();

  if (OB_UNLIKELY(NULL == start || NULL == end || start > end)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret));
  } else {
    for (cursor = start; OB_SUCC(ret) && cursor <= end; ++cursor) {
      if (OB_FAIL(get_micro_info(cursor, info))) {
        STORAGE_LOG(WARN, "fail to get micro info", K(ret), KP(cursor));
      } else if (OB_FAIL(micro_array.push_back(info))) {
        STORAGE_LOG(WARN, "micro block info array fail to push back micro info.", K(ret));
      }
    }
  }
  return ret;
}

int ObMicroBlockIndexMgr::compare_endkey(
    const int32_t micro_block_index, const common::ObStoreRowkey& rowkey, int& cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexMgr should be inited first, ", K(ret));
  } else if (OB_UNLIKELY(micro_block_index >= micro_count_ || !rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR,
        "Invalid argument to compare endkey in micro block index",
        K(ret),
        K(micro_block_index),
        K_(micro_count),
        K(rowkey));
  } else if (OB_ISNULL(node_array_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "node array should not be NULL", KP(node_array_));
  } else {
    ObObj obj[rowkey_column_count_];
    int32_t first_child_index = 2;
    int32_t child_num = node_array_[0].child_num_;
    ObMicroIndexNode* node_ptr = NULL;
    Compare compare;
    for (int32_t i = 0; OB_SUCC(ret) && i < rowkey_column_count_; ++i) {
      node_ptr = std::lower_bound(
          node_array_ + first_child_index, node_array_ + first_child_index + child_num, micro_block_index, compare);
      if (OB_ISNULL(node_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "can not get the node ptr from node array", K(ret), K(micro_block_index), KP(node_ptr));
      } else {
        if (node_ptr->first_micro_index_ != micro_block_index) {
          STORAGE_LOG(TRACE, "find lower bound node", K(*node_ptr));
          if (node_ptr != node_array_) {
            node_ptr = node_ptr - 1;
          }
        }
        obj[i] = node_ptr->obj_;
        first_child_index = node_ptr->first_child_index_;
        child_num = node_ptr->child_num_;
        STORAGE_LOG(TRACE, "find node", K(*node_ptr), K(first_child_index), K(child_num));
      }
    }
    if (OB_SUCC(ret)) {
      ObStoreRowkey endkey;
      endkey.assign(obj, rowkey_column_count_);
      if (!endkey.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "rowkey is invalid", K(ret), K(endkey));
      } else {
        cmp_ret = rowkey.compare(endkey);
      }
      STORAGE_LOG(DEBUG, "check micro endkey", K(cmp_ret), K(endkey), K(rowkey));
    }
  }
  return ret;
}

int ObMicroBlockIndexMgr::get_endkey(
    const int32_t micro_block_index, ObIAllocator& allocator, ObStoreRowkey& endkey) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexMgr should be inited first, ", K(ret));
  } else if (OB_UNLIKELY(micro_block_index >= micro_count_)) {
    ret = OB_ERR_WRONG_ROWID;
    STORAGE_LOG(ERROR,
        "unexpected rowid error, micro_block_index in rowid is wrong!",
        K(ret),
        K(micro_block_index),
        K_(micro_count));
  } else if (OB_ISNULL(node_array_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "node array should not be NULL", KP(node_array_));
  } else {
    ObStoreRowkey rowkey;
    ObObj obj[rowkey_column_count_];
    int32_t first_child_index = 2;
    int32_t child_num = node_array_[0].child_num_;
    ObMicroIndexNode* node_ptr = NULL;
    Compare compare;
    for (int32_t i = 0; OB_SUCC(ret) && i < rowkey_column_count_; ++i) {
      node_ptr = std::lower_bound(
          node_array_ + first_child_index, node_array_ + first_child_index + child_num, micro_block_index, compare);
      if (OB_ISNULL(node_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "can not get the node ptr from node array", K(ret), K(micro_block_index), KP(node_ptr));
      } else {
        if (node_ptr->first_micro_index_ != micro_block_index) {
          STORAGE_LOG(TRACE, "find lower bound node", K(*node_ptr));
          if (node_ptr != node_array_) {
            node_ptr = node_ptr - 1;
          }
        }
        obj[i] = node_ptr->obj_;
        first_child_index = node_ptr->first_child_index_;
        child_num = node_ptr->child_num_;
        STORAGE_LOG(TRACE, "find node", K(*node_ptr), K(first_child_index), K(child_num));
      }
    }

    if (OB_SUCC(ret)) {
      rowkey.assign(obj, rowkey_column_count_);
      if (!rowkey.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "rowkey is invalid", K(ret), K(rowkey));
      } else if (OB_FAIL(rowkey.deep_copy(endkey, allocator))) {
        STORAGE_LOG(WARN, "fail to deep copy endkey", K(ret), K(rowkey));
      } else {
        STORAGE_LOG(TRACE, "get_endkey", K(rowkey));
      }
    }
  }
  return ret;
}

int ObMicroBlockIndexMgr::get_endkeys(ObIAllocator& allocator, ObIArray<ObStoreRowkey>& endkeys) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexMgr should be inited first, ", K(ret));
  } else if (OB_ISNULL(node_array_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "node array should not be NULL", KP(node_array_));
  } else {
    int32_t micro_block_num = node_array_[1].first_micro_index_;
    for (int32_t i = 0; OB_SUCC(ret) && i < micro_block_num; ++i) {
      ObStoreRowkey endkey;
      if (OB_FAIL(get_endkey(i, allocator, endkey))) {
        STORAGE_LOG(WARN, "fail to get endkey", K(ret), K(i));
      } else if (OB_FAIL(endkeys.push_back(endkey))) {
        STORAGE_LOG(WARN, "fail to push endkey to endkeys", K(ret));
      }
    }
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
