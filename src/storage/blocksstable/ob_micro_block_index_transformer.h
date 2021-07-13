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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_INDEX_TRANFFORMER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_INDEX_TRANFFORMER_H_
#include "lib/container/ob_vector.h"
#include "share/schema/ob_table_schema.h"
#include "ob_micro_block_index_mgr.h"
#include "ob_column_map.h"
#include "ob_row_reader.h"
#include "ob_micro_block_index_reader.h"

namespace oceanbase {
namespace blocksstable {
struct MediumNode;
struct ObFullMacroBlockMeta;
class ObMicroBlockIndexMgr;
}  // namespace blocksstable
namespace common {
template <>
struct ob_vector_traits<blocksstable::MediumNode> {
  typedef blocksstable::MediumNode* pointee_type;
  typedef blocksstable::MediumNode value_type;
  typedef const blocksstable::MediumNode const_value_type;
  typedef value_type* iterator;
  typedef const value_type* const_iterator;
  typedef int32_t difference_type;
};
}  // namespace common
}  // namespace oceanbase
namespace oceanbase {
namespace blocksstable {

struct MediumNode {
  common::ObObj obj_;
  int32_t first_block_index_;
  int32_t first_child_index_;
  int32_t child_num_;
  bool is_valid();
  TO_STRING_KV(K_(obj), K_(first_block_index), K_(first_child_index), K_(child_num));
};

class ObMicroBlockIndexTransformer {
public:
  ObMicroBlockIndexTransformer();
  ~ObMicroBlockIndexTransformer()
  {}

  int transform(const char* index_buf, const ObFullMacroBlockMeta& meta, const ObMicroBlockIndexMgr*& idx_mgr);
  int transform(const char* index_buf, const ObFullMacroBlockMeta& meta);
  int create_block_index_mgr(
      const ObFullMacroBlockMeta& meta, char* buffer, const int64_t size, const ObMicroBlockIndexMgr*& idx_mgr);
  int64_t get_transformer_size() const;

private:
  struct NodeArray;

private:
  void reset();
  int block_index_to_node_vector();
  int node_vector_to_node_array();
  int add_node_to_vector(MediumNode& cur_node, int index);
  int add_extra_space_size(common::ObObj& obj);

  int get_first_diff_index(const common::ObObj* pre_rowkey, const common::ObObj* cur_rowkey,
      const int64_t rowkey_column_count, int& cur_first_diff_index) const;

  int fill_block_index_mgr(char* buffer, int64_t size);

private:
  struct NodeArray {
    ObMicroIndexNode* base_;
    int64_t total_count_;
    int64_t cur_count_;
    int64_t extra_space_size_;

    NodeArray();
    ~NodeArray()
    {}

    void reset();
    int ensure_space(common::ObIAllocator& allocator, int64_t count);

    inline int64_t get_node_array_size() const
    {
      return cur_count_ * sizeof(ObMicroIndexNode);
    }
    inline int64_t get_extra_space_size() const
    {
      return extra_space_size_;
    }
    inline ObMicroIndexNode& operator[](const int64_t index)
    {
      return base_[index];
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(NodeArray);
  };

private:
  ObMicroBlockIndexReader index_reader_;
  int64_t block_count_;  // count of micro blocks
  int64_t rowkey_column_count_;
  int64_t data_offset_;

  common::ObVector<MediumNode> node_vector_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
  int64_t node_vector_count_;
  NodeArray node_array_;

  ObMicroBlockIndexMgr* micro_index_mgr_;
  common::ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockIndexTransformer);
};
}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
