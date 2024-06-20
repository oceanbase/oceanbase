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

#define USING_LOG_PREFIX STORAGE

#include "ob_sstable_index_filter.h"
#include "ob_table_access_param.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"

namespace oceanbase
{
namespace storage
{

//////////////////////////////////////// ObSSTableIndexFilter //////////////////////////////////////////////

int ObSSTableIndexFilter::init(
    const bool is_cg,
    const ObITableReadInfo* read_info,
    sql::ObPushdownFilterExecutor &pushdown_filter,
    common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  is_cg_ = is_cg;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init ObSSTableIndexFilter twice", K(ret), K_(is_inited));
  } else if (OB_ISNULL(read_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected nullptr read_info", K(ret), KP(read_info), K(is_cg));
  } else if (OB_FAIL(build_skipping_filter_nodes(read_info, pushdown_filter))) {
    LOG_WARN("Fail to build skipping filter node", K(ret));
  } else if (OB_FAIL(skip_filter_executor_.init(MAX(1, pushdown_filter.get_op().get_batch_size()), allocator))) {
    LOG_WARN("Failed to init skip filter executor", K(ret));
  } else {
    pushdown_filter_ = &pushdown_filter;
    allocator_ = allocator;
    is_inited_ = true;
  }
  return ret;
}

// The allocator for building ObSSTableIndexFilter is sql statement level.
// The allocator for filtering in ObSkipIndexFilterExecutor is storage scan/rescan interface level.
int ObSSTableIndexFilter::check_range(
    const ObITableReadInfo *read_info,
    blocksstable::ObMicroIndexInfo &index_info,
    common::ObIAllocator &allocator,
    const bool use_vectorize)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableIndexFilter is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(skipping_filter_nodes_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected empty skipping filter nodes", K(ret), K(skipping_filter_nodes_.count()));
  } else if (OB_UNLIKELY(!index_info.is_filter_uncertain())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected ObMicroIndexInfo", K(ret), K(index_info.get_filter_constant_type()));
  } else {
    sql::ObBoolMask bm;
    bool can_use_skipping_index_filter = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < skipping_filter_nodes_.count(); ++i) {
      ObSkippingFilterNode &node = skipping_filter_nodes_[i];
      if (OB_FAIL(is_filtered_by_skipping_index(read_info, index_info, node, allocator, use_vectorize))) {
        LOG_WARN("Fail to do filter by skipping index", K(ret), K(index_info));
      } else {
        can_use_skipping_index_filter =
          (can_use_skipping_index_filter || node.filter_->is_filter_constant());
      }
    }
    if (OB_SUCC(ret) && can_use_skipping_index_filter) {
      if (OB_FAIL(pushdown_filter_->execute_skipping_filter(bm))) {
        LOG_WARN("Fail to execute skipping filter", K(ret), KP_(pushdown_filter));
      } else {
        index_info.set_filter_constant_type(bm.bmt_);
        // Recover ObBoolMask of the filter.
        for (int64_t i = 0; OB_SUCC(ret) && i < skipping_filter_nodes_.count(); ++i) {
          ObSkippingFilterNode &node = skipping_filter_nodes_[i];
          if (node.filter_->is_filter_constant()) {
            if (!node.is_already_determinate_ &&
                OB_FAIL(index_info.add_skipping_filter_result(node.filter_))) {
              LOG_WARN("Fail to add skipping filter result", K(ret), K(index_info));
            }
            node.filter_->set_filter_uncertain();
          }
        }
      }
    }
  }
  return ret;
}

int ObSSTableIndexFilter::is_filtered_by_skipping_index(
    const ObITableReadInfo *read_info,
    blocksstable::ObMicroIndexInfo &index_info,
    ObSkippingFilterNode &node,
    common::ObIAllocator &allocator,
    const bool use_vectorize)
{
  int ret = OB_SUCCESS;
  node.is_already_determinate_ = false;
  if (OB_UNLIKELY(nullptr == node.filter_ || 1 != node.filter_->get_col_offsets().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter in skipping filter node", K(ret), KPC_(node.filter));
  } else if (index_info.apply_skipping_filter_result(node.filter_)) {
    // There is no need to check skipping index because filter result is contant already.
    node.is_already_determinate_ = true;
  } else {
    const uint32_t col_offset = node.filter_->get_col_offsets(is_cg_).at(0);
    const uint32_t col_idx = static_cast<uint32_t>(read_info->get_columns_index().at(col_offset));
    const ObObjMeta obj_meta = read_info->get_columns_desc().at(col_offset).col_type_;
    if (OB_FAIL(skip_filter_executor_.falsifiable_pushdown_filter(col_idx,
                                                                  obj_meta,
                                                                  node.skip_index_type_,
                                                                  index_info,
                                                                  *node.filter_,
                                                                  allocator,
                                                                  use_vectorize))) {
      LOG_WARN("Fail to falsifiable pushdown filter", K(ret), K(node.filter_));
    }
  }
  return ret;
}

int ObSSTableIndexFilter::build_skipping_filter_nodes(
    const ObITableReadInfo* read_info,
    sql::ObPushdownFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  if (filter.is_logic_op_node()) {
    sql::ObPushdownFilterExecutor **children = filter.get_childs();
    for (uint32_t i = 0; OB_SUCC(ret) && i < filter.get_child_count(); ++i) {
      if (OB_ISNULL(children[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexecpected nullptr filter", K(ret));
      } else if (OB_FAIL(build_skipping_filter_nodes(read_info, *children[i]))) {
        LOG_WARN("Fail to traverse filter tree", K(ret), K(i), KP(children[i]));
      }
    }
  } else if (OB_FAIL(extract_skipping_filter_from_tree(read_info, filter))) {
    LOG_WARN("Fail to extract physical operator from tree", K(ret));
  }
  return ret;
}

int ObSSTableIndexFilter::extract_skipping_filter_from_tree(
    const ObITableReadInfo* read_info,
    sql::ObPushdownFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  sql::ObPhysicalFilterExecutor &physical_filter = static_cast<sql::ObPhysicalFilterExecutor &>(filter);
  if (physical_filter.is_filter_white_node() || static_cast<sql::ObBlackFilterExecutor &>(physical_filter).is_monotonic()) {
    IndexList index_list;
    if (OB_FAIL(find_skipping_index(read_info, physical_filter, index_list))) {
      LOG_WARN("Fail to find useful skipping index", K(ret));
    } else if (OB_FAIL(find_useful_skipping_filter(index_list, physical_filter))) {
      LOG_WARN("Fail to find useful skipping filter", K(ret));
    }
  }
  return ret;
}

int ObSSTableIndexFilter::find_skipping_index(
    const ObITableReadInfo* read_info,
    sql::ObPhysicalFilterExecutor &filter,
    IndexList &index_list) const
{
  int ret = OB_SUCCESS;
  const uint64_t column_id = filter.get_col_ids().at(0);
  const common::ObIArray<ObColumnParam *> *column_params = read_info->get_columns();
  const common::ObIArray<ObColExtend> *column_extend = read_info->get_columns_extend();
  if ((!is_cg_ && OB_ISNULL(column_params)) || OB_ISNULL(column_extend)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected nullptr column_params", K(ret), K_(is_cg), KP(column_params), KP(column_extend));
  } else if (column_extend->empty()) {
  } else {
    int64_t index = -1;
    if (!is_cg_) {
      for (int64_t i = 0; i < column_params->count(); ++i) {
        if (column_id == column_params->at(i)->get_column_id()) {
          index = i;
          break;
        }
      }
    } else {
      index = 0;
    }
    if (OB_UNLIKELY(index < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column not found", K(column_id), KPC(read_info));
    } else if (OB_UNLIKELY(column_extend->count() <= index)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected column meta", K(column_id), K(index), KPC(read_info));
    } else {
      const bool has_min_max = column_extend->at(index).skip_index_attr_.has_min_max();
      if (has_min_max && OB_FAIL(index_list.push_back(blocksstable::ObSkipIndexType::MIN_MAX))) {
        LOG_WARN("Fail to push back skip index type", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableIndexFilter::find_useful_skipping_filter(
    const IndexList &index_list,
    sql::ObPhysicalFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const blocksstable::ObSkipIndexType skip_index_type = index_list[i];
    ObSkippingFilterNode node;
    if (ObSSTableIndexFilterExtracter::extract_skipping_filter(filter, skip_index_type, node)) {
      LOG_WARN("Fail to extract index skipping filter", K(ret), K(skip_index_type));
    } else if (node.is_useful()) {
      node.filter_ = &filter;
      if (OB_FAIL(skipping_filter_nodes_.push_back(node))) {
        LOG_WARN("Fail to push back skipping filter node", K(ret));
      }
    }
  }
  return ret;
}

//////////////////////////////////////// ObSSTableIndexFilterFactory //////////////////////////////////////////////

int ObSSTableIndexFilterFactory::build_sstable_index_filter(
    const bool is_cg,
    const ObITableReadInfo* read_info,
    sql::ObPushdownFilterExecutor &pushdown_filter,
    common::ObIAllocator *allocator,
    ObSSTableIndexFilter *&index_filter)
{
  int ret = OB_SUCCESS;
  ObSSTableIndexFilter *tmp_index_filter = nullptr;
  index_filter = nullptr;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected nullptr allocator", K(ret), KP(allocator));
  } else if (OB_ISNULL(tmp_index_filter = OB_NEWx(ObSSTableIndexFilter, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to new ObSSTableIndexFilter", K(ret));
  } else if (OB_FAIL(tmp_index_filter->init(is_cg, read_info, pushdown_filter, allocator))) {
    LOG_WARN("Fail to init ObSSTableIndexFilter", K(ret), K(is_cg));
  }
  if (OB_SUCC(ret) && tmp_index_filter->can_use_skipping_index()) {
    index_filter = tmp_index_filter;
  } else if (nullptr != tmp_index_filter) {
    tmp_index_filter->~ObSSTableIndexFilter();
    allocator->free(tmp_index_filter);
  }
  return ret;
}

void ObSSTableIndexFilterFactory::destroy_sstable_index_filter(ObSSTableIndexFilter *&index_filter)
{
  if (index_filter) {
    common::ObIAllocator *allocator = index_filter->get_allocator();
    index_filter->~ObSSTableIndexFilter();
    if (allocator) {
      allocator->free(index_filter);
    }
    index_filter = nullptr;
  }
}

//////////////////////////////////////// ObSSTableIndexFilterExtracter //////////////////////////////////////////////

int ObSSTableIndexFilterExtracter::extract_skipping_filter(
    const sql::ObPhysicalFilterExecutor &filter,
    const blocksstable::ObSkipIndexType skip_index_type,
    ObSkippingFilterNode &node)
{
  int ret = OB_SUCCESS;
  switch (skip_index_type) {
    case blocksstable::ObSkipIndexType::MIN_MAX:
      node.skip_index_type_ = blocksstable::ObSkipIndexType::MIN_MAX;
      break;
    default:
      // There are more skipping index types in the future.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unepected skip index type", K(ret), K(skip_index_type));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
