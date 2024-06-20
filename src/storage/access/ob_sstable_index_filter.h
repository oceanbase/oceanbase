/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_SSTABLE_INDEX_FILTER_H
#define OB_STORAGE_SSTABLE_INDEX_FILTER_H

#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"
#include "storage/blocksstable/index_block/ob_skip_index_filter_executor.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObMicroIndexInfo;
}
namespace storage
{

struct ObSkippingFilterNode
{
  ObSkippingFilterNode()
   : is_already_determinate_(false),
      skip_index_type_(blocksstable::ObSkipIndexType::MAX_TYPE),
      filter_(nullptr) {}

  OB_INLINE bool is_useful() const
  {
    return blocksstable::ObSkipIndexType::MAX_TYPE != skip_index_type_;
  }
  OB_INLINE void set_useless()
  {
    skip_index_type_ = blocksstable::ObSkipIndexType::MAX_TYPE;
  }
  TO_STRING_KV(K_(is_already_determinate), K_(skip_index_type), KP_(filter));

  bool is_already_determinate_;
  blocksstable::ObSkipIndexType skip_index_type_;
  sql::ObPhysicalFilterExecutor *filter_;
};

class ObSSTableIndexFilter
{
public:
  using ObSkippingFilterNodes = common::ObSEArray<ObSkippingFilterNode, 4>;
  using IndexList = common::ObSEArray<blocksstable::ObSkipIndexType, 4>;
  ObSSTableIndexFilter()
      : is_inited_(false),
      is_cg_(false),
      pushdown_filter_(nullptr),
      allocator_(nullptr),
      skipping_filter_nodes_(),
      skip_filter_executor_()
  {
    skipping_filter_nodes_.set_attr(ObMemAttr(MTL_ID(), "IndexFilters"));
  }
  ~ObSSTableIndexFilter() = default;
  int init(
      const bool is_cg,
      const ObITableReadInfo* read_info,
      sql::ObPushdownFilterExecutor &pushdown_filter,
      common::ObIAllocator *allocator);
  /// Check whether we can skip filtering.
  int check_range(
      const ObITableReadInfo *read_info,
      blocksstable::ObMicroIndexInfo &index_info,
      common::ObIAllocator &allocator,
      const bool use_vectorize);
  /// Check whether we can use skipping index.
  bool can_use_skipping_index() const
  {
    return !skipping_filter_nodes_.empty();
  }
  common::ObIAllocator *get_allocator()
  {
    return allocator_;
  }
  const sql::ObPushdownFilterExecutor *get_pushdown_filter() { return pushdown_filter_; }
  TO_STRING_KV(K_(is_inited), K_(skipping_filter_nodes));
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableIndexFilter);
  int is_filtered_by_skipping_index(
      const ObITableReadInfo *read_info,
      blocksstable::ObMicroIndexInfo &index_info,
      ObSkippingFilterNode &node,
      common::ObIAllocator &allocator,
      const bool use_vectorize);
  int build_skipping_filter_nodes(
      const ObITableReadInfo* read_info,
      sql::ObPushdownFilterExecutor &filter);
  int extract_skipping_filter_from_tree(
    const ObITableReadInfo* read_info,
    sql::ObPushdownFilterExecutor &filter);
  int find_skipping_index(
      const ObITableReadInfo* read_info,
      sql::ObPhysicalFilterExecutor &filter,
      IndexList &index_list) const;
  int find_useful_skipping_filter(
      const IndexList &index_list,
      sql::ObPhysicalFilterExecutor &filter);
private:
  bool is_inited_;
  bool is_cg_;
  sql::ObPushdownFilterExecutor *pushdown_filter_;
  common::ObIAllocator *allocator_;
  ObSkippingFilterNodes skipping_filter_nodes_;
  blocksstable::ObSkipIndexFilterExecutor skip_filter_executor_;
};

class ObSSTableIndexFilterFactory {
public:
  static int build_sstable_index_filter(
      const bool is_cg,
      const ObITableReadInfo* read_info,
      sql::ObPushdownFilterExecutor &pushdown_filter,
      common::ObIAllocator *allocator,
      ObSSTableIndexFilter *&index_filter);

  static void destroy_sstable_index_filter(ObSSTableIndexFilter *&index_filter);
};

struct ObSSTableIndexFilterExtracter
{
public:
  static int extract_skipping_filter(
      const sql::ObPhysicalFilterExecutor &filter,
      const blocksstable::ObSkipIndexType skip_index_type,
      ObSkippingFilterNode &node);
};
} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_SSTABLE_INDEX_FILTER_H
