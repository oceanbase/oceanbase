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

#define USING_LOG_PREFIX SQL

#include "ob_external_table_pushdown_filter.h"
#include "share/external_table/ob_external_table_utils.h"

namespace oceanbase
{

using namespace common;
using namespace share;
namespace sql
{

int ObExternalTablePushdownFilter::init(const ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  CK (scan_param != nullptr);
  CK (scan_param->op_ != nullptr);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(scan_param->pd_storage_filters_)) {
    // no pushdown filters
  } else {
    param_ = scan_param;
    ObEvalCtx &eval_ctx = scan_param->op_->get_eval_ctx();
    allocator_.set_attr(ObMemAttr(MTL_ID(), "ExtTblFtPD"));
    if (OB_FAIL(skip_filter_executor_.init(MAX(1, eval_ctx.max_batch_size_), &allocator_))) {
      LOG_WARN("Failed to init skip filter executor", K(ret));
    } else if (OB_FAIL(build_skipping_filter_nodes(*scan_param->pd_storage_filters_))) {
      LOG_WARN("failed to build skip index", K(ret));
    } else if (OB_FAIL(file_filter_col_ids_.prepare_allocate(skipping_filter_nodes_.count()))) {
      LOG_WARN("fail to prepare allocate array", K(ret));
    } else if (OB_FAIL(file_filter_exprs_.prepare_allocate(skipping_filter_nodes_.count()))) {
      LOG_WARN("fail to prepare allocate array", K(ret));
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::prepare_filter_col_meta(
    const common::ObArrayWrap<int> &file_col_index,
    const common::ObIArray<uint64_t> &col_ids,
    const common::ObIArray<ObExpr*> &col_exprs)
{
  int ret = OB_SUCCESS;
  CK (file_col_index.count() == col_exprs.count());
  CK (col_ids.count() == col_exprs.count());
  filter_enabled_ = true;
  int64_t real_filter_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && filter_enabled_ && i < file_col_index.count(); ++i) {
    const int file_col_id = file_col_index.at(i);
    for (int64_t f_idx = 0; OB_SUCC(ret) && f_idx < skipping_filter_nodes_.count(); ++f_idx) {
      const ObSkippingFilterNode &node = skipping_filter_nodes_.at(f_idx);
      if (OB_ISNULL(node.filter_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null filter", K(ret));
      } else if (OB_INVALID_ID == col_ids.at(i) || nullptr == col_exprs.at(i)) {
        filter_enabled_ = false;
      } else if (node.filter_->get_col_ids().at(0) == col_ids.at(i)) {
        file_filter_col_ids_.at(f_idx) = file_col_id;
        file_filter_exprs_.at(f_idx) = col_exprs.at(i);
        ++real_filter_cnt;
      }
    }
  }
  if (0 == real_filter_cnt) {
    filter_enabled_ = false;
  }
  return ret;
}

int ObExternalTablePushdownFilter::apply_skipping_index_filter(
    const PushdownLevel filter_level, MinMaxFilterParamBuilder &param_builder, bool &skipped)
{
  int ret = OB_SUCCESS;
  skipped = false;
  if (filter_enabled_
      && nullptr != param_
      && can_apply_filters(filter_level)) {
    blocksstable::ObMinMaxFilterParam filter_param;
    for (int64_t i = 0; OB_SUCC(ret) && i < skipping_filter_nodes_.count(); ++i) {
      filter_param.set_uncertain();
      ObSkippingFilterNode &node = skipping_filter_nodes_.at(i);
      const int ext_tbl_col_id = file_filter_col_ids_.at(i);
      if (nullptr == file_filter_exprs_.at(i)) {
      } else if (OB_FAIL(param_builder.build(ext_tbl_col_id, file_filter_exprs_.at(i), filter_param))) {
        LOG_WARN("fail to build param", K(ret), K(i));
      } else if (!filter_param.is_uncertain()) {
        bool filter_valid = true;
        const uint64_t ob_col_id = node.filter_->get_col_ids().at(0);
        if (OB_FAIL(node.filter_->init_evaluated_datums(filter_valid))) {
          LOG_WARN("failed to init filter", K(ret));
        } else if (!filter_valid) {
        } else if (OB_FAIL(skip_filter_executor_.falsifiable_pushdown_filter(
            ob_col_id, node.skip_index_type_, MOCK_ROW_COUNT, filter_param, *node.filter_, allocator_,
            true))) {
          LOG_WARN("failed to apply skip index", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObBoolMask bm;
      if (OB_FAIL(param_->pd_storage_filters_->execute_skipping_filter(bm))) {
        LOG_WARN("fail to execute skipping filter", K(ret));
      } else {
        skipped = bm.is_always_false();
      }
    }
    // reset filters
    for (int64_t i = 0; OB_SUCC(ret) && i < skipping_filter_nodes_.count(); ++i) {
      skipping_filter_nodes_[i].filter_->set_filter_uncertain();
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::build_skipping_filter_nodes(sql::ObPushdownFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  if (filter.is_logic_op_node()) {
    sql::ObPushdownFilterExecutor **children = filter.get_childs();
    for (int64_t i = 0; OB_SUCC(ret) && i < filter.get_child_count(); ++i) {
      if (OB_ISNULL(children[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr filter", K(ret));
      } else if (OB_FAIL(build_skipping_filter_nodes(*children[i]))) {
        LOG_WARN("Fail to traverse filter tree", K(ret), K(i), KP(children[i]));
      }
    }
  } else if (OB_FAIL(extract_skipping_filter_from_tree(filter))) {
    LOG_WARN("Fail to extract physical operator from tree", K(ret));
  }
  return ret;
}

int ObExternalTablePushdownFilter::extract_skipping_filter_from_tree(
    sql::ObPushdownFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  sql::ObPhysicalFilterExecutor &physical_filter =
    static_cast<sql::ObPhysicalFilterExecutor &>(filter);
  if (physical_filter.is_filter_white_node() ||
      static_cast<sql::ObBlackFilterExecutor &>(physical_filter).is_monotonic()) {
    const uint64_t column_id = physical_filter.get_col_ids().at(0);
    int64_t index = -1;
    for (int64_t i = 0; i < param_->column_ids_.count() ; ++i) {
      if (column_id == param_->column_ids_.at(i)) {
        index = i;
        break;
      }
    }
    if (OB_UNLIKELY(index < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column not found", K(column_id));
    } else {
      ObSkippingFilterNode node;
      if (OB_FAIL(ObSSTableIndexFilterExtracter::extract_skipping_filter(
          physical_filter, blocksstable::ObSkipIndexType::MIN_MAX, node))) {
        LOG_WARN("Fail to extract index skipping filter", K(ret));
      } else if (node.is_useful()) {
        node.filter_ = &physical_filter;
        if (OB_FAIL(skipping_filter_nodes_.push_back(node))) {
          LOG_WARN("Fail to push back skipping filter node", K(ret));
        }
      }
    }
  }
  return ret;
}

}
}
