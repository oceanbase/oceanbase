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

#include <algorithm>
#include "ob_micro_block_index_transformer.h"
#include "common/row/ob_row.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
using namespace common;

namespace blocksstable {
bool MediumNode::is_valid()
{
  return obj_.is_valid_type() && first_block_index_ >= 0 && first_child_index_ >= 0 && child_num_ >= 0;
}

ObMicroBlockIndexTransformer::NodeArray::NodeArray() : base_(NULL), total_count_(0), cur_count_(0), extra_space_size_(0)
{}

void ObMicroBlockIndexTransformer::NodeArray::reset()
{
  base_ = NULL;
  total_count_ = 0;
  cur_count_ = 0;
  extra_space_size_ = 0;
}

int ObMicroBlockIndexTransformer::NodeArray::ensure_space(common::ObIAllocator& allocator, int64_t count)
{
  int ret = OB_SUCCESS;
  char* buffer = NULL;
  int64_t size = count * sizeof(ObMicroIndexNode);
  if (count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret));
  } else if (NULL == (buffer = static_cast<char*>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "micro block index transformer fail to malloc memory.", K(ret), K(size));
  } else {
    base_ = reinterpret_cast<ObMicroIndexNode*>(buffer);
    total_count_ = count;
  }
  return ret;
}

ObMicroBlockIndexTransformer::ObMicroBlockIndexTransformer()
    : block_count_(0),
      rowkey_column_count_(0),
      data_offset_(0),
      node_vector_(),
      node_vector_count_(0),
      node_array_(),
      micro_index_mgr_(NULL),
      allocator_(ObModIds::OB_SSTABLE_GET_SCAN)
{
  for (int64_t i = 0; i < common::OB_MAX_ROWKEY_COLUMN_NUMBER; ++i) {
    node_vector_[i].set_label(ObModIds::OB_MICRO_INDEX_TRANSFORMER);
  }
}

int ObMicroBlockIndexTransformer::transform(
    const char* index_buf, const ObFullMacroBlockMeta& meta, const ObMicroBlockIndexMgr*& idx_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transform(index_buf, meta))) {
    STORAGE_LOG(WARN, "failed to transform", K(ret));
  } else {
    char* buf = nullptr;
    const int64_t size = get_transformer_size();
    if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate buf for index mgr", K(ret), K(size));
    } else if (OB_FAIL(create_block_index_mgr(meta, buf, size, idx_mgr))) {
      STORAGE_LOG(WARN, "failed to create_block_index_mgr", K(ret));
    }
  }
  return ret;
}

int ObMicroBlockIndexTransformer::transform(const char* index_buf, const ObFullMacroBlockMeta& full_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == index_buf || !full_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), KP(index_buf), K(full_meta));
  } else {
    reset();
    const ObMacroBlockMetaV2& meta = *full_meta.meta_;
    block_count_ = meta.micro_block_count_;
    rowkey_column_count_ = meta.rowkey_column_number_;
    node_vector_count_ = rowkey_column_count_;
    data_offset_ = meta.micro_block_data_offset_;

    if (OB_FAIL(index_reader_.init(full_meta, index_buf))) {
      STORAGE_LOG(WARN, "failed to init micro index reader", K(ret));
    } else if (OB_FAIL(block_index_to_node_vector())) {
      STORAGE_LOG(WARN, "transformer fail to convert block index to node vector.", K(ret));
    } else if (OB_FAIL(node_vector_to_node_array())) {
      STORAGE_LOG(WARN, "transformer fail to convert node vector to node array.", K(ret));
    }
  }
  return ret;
}

void ObMicroBlockIndexTransformer::reset()
{
  index_reader_.reset();
  block_count_ = 0;
  rowkey_column_count_ = 0;
  data_offset_ = 0;
  for (int i = 0; i < node_vector_count_; i++) {
    node_vector_[i].reset();
  }
  node_vector_count_ = 0;
  node_array_.reset();
  allocator_.reset();
  // column_map_
  // micro_index_mgr_
}

int ObMicroBlockIndexTransformer::block_index_to_node_vector()
{
  int ret = OB_SUCCESS;
  const int64_t ROWKEY_ARRAY_SIZE = 2;
  ObObj rowkey_array[ROWKEY_ARRAY_SIZE][OB_MAX_ROWKEY_COLUMN_NUMBER];
  int pre_rowkey_array_index = 0;
  int cur_rowkey_array_index = 0;
  int cur_first_diff_index = 0;

  MediumNode cur_node;

  for (int64_t i = 0; OB_SUCC(ret) && i < block_count_; ++i) {
    if (OB_FAIL(index_reader_.get_end_key(i, rowkey_array[cur_rowkey_array_index]))) {
      STORAGE_LOG(WARN, "transformer fail to get next rowkey.", K(ret), K(i));
    }

    if (OB_SUCC(ret)) {
      if (0 == i) {
        for (int32_t j = 0; OB_SUCC(ret) && j < rowkey_column_count_; ++j) {
          cur_node.obj_ = rowkey_array[cur_rowkey_array_index][j];
          cur_node.first_block_index_ = static_cast<int32_t>(i);
          cur_node.first_child_index_ = 0;
          cur_node.child_num_ = 0;
          if (OB_FAIL(add_node_to_vector(cur_node, j))) {
            STORAGE_LOG(WARN, "transformer fail to add node to vector.", K(ret), K(cur_node), K(j));
          }
        }
      } else {
        if (OB_FAIL(get_first_diff_index(rowkey_array[pre_rowkey_array_index],
                rowkey_array[cur_rowkey_array_index],
                rowkey_column_count_,
                cur_first_diff_index))) {
          STORAGE_LOG(WARN, "fail to get first diff index, ", K(ret));
        } else if (cur_first_diff_index < 0) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN,
              "index transformer fail to get first diff index.",
              K(ret),
              "pre_rowkey",
              rowkey_array[pre_rowkey_array_index],
              "cur_rowkey",
              rowkey_array[cur_rowkey_array_index],
              K(rowkey_column_count_));
        } else if (cur_first_diff_index < rowkey_column_count_) {
          for (int32_t j = cur_first_diff_index; OB_SUCC(ret) && j < rowkey_column_count_; ++j) {
            cur_node.obj_ = rowkey_array[cur_rowkey_array_index][j];
            cur_node.first_block_index_ = static_cast<int32_t>(i);
            cur_node.first_child_index_ = 0;
            cur_node.child_num_ = 0;
            if (OB_FAIL(add_node_to_vector(cur_node, j))) {
              STORAGE_LOG(WARN, "transformer fail to add node to vector.", K(ret), K(cur_node), K(j));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(
              WARN, "invalid transformer diff offset, ", K(ret), K(cur_first_diff_index), K(rowkey_column_count_));
        }
      }
    }

    if (OB_SUCC(ret)) {
      pre_rowkey_array_index = cur_rowkey_array_index;
      cur_rowkey_array_index = (cur_rowkey_array_index + 1) % ROWKEY_ARRAY_SIZE;
      cur_first_diff_index = 0;
    }
  }
  return ret;
}

int ObMicroBlockIndexTransformer::node_vector_to_node_array()
{
  int ret = OB_SUCCESS;
  int64_t node_num = 2;
  int32_t cur_node_num = 0;
  int32_t node_index = 2;

  for (int64_t i = 0; i < rowkey_column_count_; ++i) {
    node_num += node_vector_[i].size();
  }
  if (OB_FAIL(node_array_.ensure_space(allocator_, node_num))) {
    STORAGE_LOG(WARN, "node array fail to ensure space.", K(ret), K(node_num));
  } else {
    MediumNode tmp_node;
    node_array_[0].obj_.reset();
    node_array_[0].first_child_index_ = 2;
    node_array_[0].first_micro_index_ = 0;
    node_array_[0].child_num_ = node_vector_[0].size();
    node_array_.cur_count_++;
    node_array_[1].obj_.reset();
    node_array_[1].first_child_index_ = 0;
    node_array_[1].first_micro_index_ = static_cast<int32_t>(block_count_);
    node_array_[1].child_num_ = 0;
    node_array_.cur_count_++;
    for (int32_t i = 0; i < rowkey_column_count_; ++i) {
      cur_node_num = node_vector_[i].size();
      for (int32_t j = 0; j < cur_node_num; ++j) {
        tmp_node = node_vector_[i].at(j);
        node_array_[node_index + j].obj_ = tmp_node.obj_;
        if (0 != tmp_node.child_num_) {
          node_array_[node_index + j].first_child_index_ = node_index + cur_node_num + tmp_node.first_child_index_;
        } else {
          node_array_[node_index + j].first_child_index_ = 0;
        }

        node_array_[node_index + j].first_micro_index_ = tmp_node.first_block_index_;
        node_array_[node_index + j].child_num_ = tmp_node.child_num_;
        node_array_.cur_count_++;
      }
      node_index += cur_node_num;
    }
  }
  return ret;
}

inline int ObMicroBlockIndexTransformer::add_node_to_vector(MediumNode& cur_node, int index)
{
  int ret = OB_SUCCESS;
  int32_t parent_index = 0;
  MediumNode* parent_node = NULL;

  if (!cur_node.is_valid() || index < 0 || index >= rowkey_column_count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret), K(cur_node), K(index), K_(rowkey_column_count));
  } else if (OB_FAIL(node_vector_[index].push_back(cur_node))) {
    STORAGE_LOG(WARN, "node vector fail to push back cur node.", K(ret), K(index), K(cur_node));
  } else if (OB_FAIL(add_extra_space_size(cur_node.obj_))) {
    STORAGE_LOG(WARN, "transformer fail to add extra space size.", K(ret), "obj", cur_node.obj_);
  } else if (index > 0) {
    parent_index = node_vector_[index - 1].size() - 1;
    parent_node = &node_vector_[index - 1].at(parent_index);
    if (0 == parent_node->child_num_) {
      parent_node->first_child_index_ = node_vector_[index].size() - 1;
    }
    parent_node->child_num_++;
  }
  return ret;
}

int ObMicroBlockIndexTransformer::add_extra_space_size(ObObj& obj)
{
  int ret = OB_SUCCESS;
  if (!obj.is_valid_type()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret));
  } else {
    if (ob_is_string_type(obj.get_type()) || ob_is_raw(obj.get_type()) || ob_is_number_tc(obj.get_type()) ||
        ob_is_rowid_tc(obj.get_type())) {
      node_array_.extra_space_size_ += obj.get_data_length();
    }
  }
  return ret;
}

int ObMicroBlockIndexTransformer::get_first_diff_index(
    const ObObj* pre_rowkey, const ObObj* cur_rowkey, const int64_t rowkey_obj_count, int& cur_first_diff_index) const
{
  int ret = OB_SUCCESS;
  if (NULL == pre_rowkey || NULL == cur_rowkey || rowkey_obj_count <= 0 ||
      rowkey_obj_count > OB_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret), KP(pre_rowkey), KP(cur_rowkey), K(rowkey_obj_count));
  } else {
    for (int i = 0; i < rowkey_obj_count; ++i) {
      if (pre_rowkey[i] != cur_rowkey[i]) {
        cur_first_diff_index = i;
        break;
      }
    }
  }
  return ret;
}

int ObMicroBlockIndexTransformer::create_block_index_mgr(
    const ObFullMacroBlockMeta& meta, char* buffer, const int64_t size, const ObMicroBlockIndexMgr*& idx_mgr)
{
  int ret = OB_SUCCESS;

  idx_mgr = nullptr;
  if (OB_ISNULL(micro_index_mgr_ = new (buffer) ObMicroBlockIndexMgr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "transformer fail to new block index mgr, ", K(ret));
  } else if (OB_FAIL(
                 micro_index_mgr_->init(meta, node_array_.get_node_array_size(), node_array_.get_extra_space_size()))) {
    STORAGE_LOG(WARN,
        "micro index mgr fail to init.",
        K(ret),
        K(block_count_),
        "node_array_size",
        node_array_.get_node_array_size(),
        "extra_space_size",
        node_array_.get_extra_space_size(),
        "mark_deletion_flag size",
        index_reader_.get_mark_deletion_flags_size(),
        "delta size",
        index_reader_.get_delta_size(),
        K(rowkey_column_count_),
        K(data_offset_),
        K(meta));
  } else if (OB_FAIL(fill_block_index_mgr(buffer, size))) {
    STORAGE_LOG(WARN, "transformer fail to fill block index mgr.", K(ret));
  } else {
    idx_mgr = micro_index_mgr_;
  }
  return ret;
}

int ObMicroBlockIndexTransformer::fill_block_index_mgr(char* buffer, int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t pos = sizeof(ObMicroBlockIndexMgr);
  ObMicroIndexNode* node_array = nullptr;

  if (OB_UNLIKELY(NULL == buffer) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret));
  } else {
    if (OB_UNLIKELY((pos + (block_count_ + 1) * sizeof(ObMicroBlockIndexMgr::MemMicroIndexItem)) >
                    static_cast<uint64_t>(size))) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer is not enough, ", K(ret), K(pos), K(size), K_(block_count));
    } else if (OB_FAIL(index_reader_.get_all_mem_micro_index(
                   reinterpret_cast<ObMicroBlockIndexMgr::MemMicroIndexItem*>(buffer + pos)))) {
      STORAGE_LOG(WARN, "failed to get all mem micro indexes", K(ret));
    } else {
      pos += (block_count_ + 1) * sizeof(ObMicroBlockIndexMgr::MemMicroIndexItem);
    }

    // node array
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY((pos + node_array_.get_node_array_size()) > size)) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN,
            "buffer is not enough, ",
            K(ret),
            K(pos),
            K(size),
            "node_array_size:",
            node_array_.get_node_array_size());
      } else {
        MEMCPY(buffer + pos, node_array_.base_, node_array_.get_node_array_size());
        node_array = reinterpret_cast<ObMicroIndexNode*>(buffer + pos);
        pos += node_array_.get_node_array_size();
      }
    }

    // extra size
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY((pos + node_array_.get_extra_space_size()) > size)) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN,
            "buffer is not enough, ",
            K(ret),
            K(pos),
            K(size),
            "extra_space_size:",
            node_array_.get_extra_space_size());
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < node_array_.cur_count_; i++) {
          common::ObObj& tmp_obj = node_array_[i].obj_;
          if (OB_UNLIKELY(ob_is_text_tc(tmp_obj.get_type()))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "Unexpected large lob type in micro block index", K(tmp_obj), K(ret));
          } else if (OB_FAIL(node_array[i].obj_.deep_copy(tmp_obj, buffer, size, pos))) {
            STORAGE_LOG(WARN, "fail to deep_copy", K(tmp_obj), K(size), K(pos), K(ret));
          }
        }
      }
    }

    // mark deletion
    if (OB_SUCC(ret)) {
      if (index_reader_.get_mark_deletion_flags_size() > 0) {
        if ((pos + index_reader_.get_mark_deletion_flags_size()) > size) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN,
              "buffer is not enough, ",
              K(ret),
              K(pos),
              K(size),
              "mark deletion flags size:",
              index_reader_.get_mark_deletion_flags_size());
        } else if (OB_FAIL(index_reader_.get_all_mark_deletions_flags(reinterpret_cast<bool*>(buffer + pos)))) {
          STORAGE_LOG(WARN, "fail to get all mark deletion flags", K(ret));
        } else {
          pos += index_reader_.get_mark_deletion_flags_size();
        }
      }
    }

    // delta array
    if (OB_SUCC(ret)) {
      if (index_reader_.get_delta_size() > 0) {
        if (pos + index_reader_.get_delta_size() > size) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN,
              "buffer is not enough for delta",
              K(ret),
              K(pos),
              K(size),
              "delta size",
              index_reader_.get_delta_size());
        } else if (OB_FAIL(index_reader_.get_all_deltas(reinterpret_cast<int32_t*>(buffer + pos)))) {
          STORAGE_LOG(WARN, "failed to get all deltas", K(ret));
        } else {
          pos += index_reader_.get_delta_size();
        }
      }
    }
  }
  return ret;
}

int64_t ObMicroBlockIndexTransformer::get_transformer_size() const
{
  return sizeof(ObMicroBlockIndexMgr) + (block_count_ + 1) * sizeof(ObMicroBlockIndex) +
         node_array_.get_node_array_size() + node_array_.get_extra_space_size() +
         index_reader_.get_mark_deletion_flags_size() + index_reader_.get_delta_size();
}

}  // end namespace blocksstable
}  // end namespace oceanbase
