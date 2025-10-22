/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_where_optimizer.h"
#include "storage/access/ob_table_access_param.h"

namespace oceanbase
{
namespace storage
{
#define REORDER_FILTER_INTERVAL 32
ObWhereOptimizer::ObWhereOptimizer()
  : iter_param_(nullptr)
  , filter_(nullptr)
  , filter_iters_(nullptr)
  , iter_filter_node_(nullptr)
  , batch_num_(0)
  , reorder_filter_times_(0)
  , reorder_filter_interval_(1)
  , disable_bypass_(false)
  , is_inited_(false)
{
}

int ObWhereOptimizer::init(
  const ObTableIterParam *iter_param,
  sql::ObPushdownFilterExecutor *filter,
  ObSEArray<ObICGIterator*, 4> *filter_iters,
  ObSEArray<sql::ObPushdownFilterExecutor*, 4> *iter_filter_node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObWhereOptimizer init twice", K(ret));
  } else if (OB_ISNULL(iter_param_ = iter_param) || OB_ISNULL(filter_ = filter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("iter param or filter is null", K(ret), K(iter_param_), K(filter_));
  } else {
    if (filter_iters != nullptr) {
      filter_->inc_ref();
    }
    filter_iters_ = filter_iters;
    iter_filter_node_ = iter_filter_node;
    filter_conditions_.reset();
    batch_num_ = 0;
    reorder_filter_times_ = 0;
    reorder_filter_interval_ = 1;
    disable_bypass_ = false;
    judge_filter_whether_enable_reorder(filter);
    is_inited_ = true;
  }
  return ret;
}

void ObWhereOptimizer::reset()
{
  if (filter_iters_ != nullptr) {
    filter_->dec_ref();
  }
  iter_param_ = nullptr;
  filter_ = nullptr;
  filter_iters_ = nullptr;
  iter_filter_node_ = nullptr;
  filter_conditions_.reset();
  batch_num_ = 0;
  reorder_filter_times_ = 0;
  reorder_filter_interval_ = 1;
  disable_bypass_ = false;
  is_inited_ = false;
}

void ObWhereOptimizer::reuse()
{
  iter_param_ = nullptr;
  filter_ = nullptr;
  filter_iters_ = nullptr;
  iter_filter_node_ = nullptr;
  filter_conditions_.reuse();
  batch_num_ = 0;
  reorder_filter_times_ = 0;
  reorder_filter_interval_ = 1;
  disable_bypass_ = false;
  is_inited_ = false;
}

int ObWhereOptimizer::analyze(bool &reordered)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret = analyze_impl(*filter_, reordered);
  }
  return ret;
}

int ObWhereOptimizer::analyze_impl(sql::ObPushdownFilterExecutor &filter, bool &reordered)
{
  int ret = OB_SUCCESS;
  sql::ObPushdownFilterExecutor **children = filter.get_childs();
  const int64_t child_cnt = filter.get_child_count();

  if (filter.is_enable_reorder()) {
    if (OB_FAIL(filter_conditions_.prepare_allocate(child_cnt))) {
      LOG_WARN("Failed to prepare allocate filter conditions", K(ret), K(child_cnt));
    } else {
      for (int64_t i = 0; i < child_cnt; ++i) {
        filter_conditions_.at(i).idx_ = i;
        collect_filter_info(*children[i], filter_conditions_.at(i));
      }

      lib::ob_sort(&filter_conditions_.at(0), &filter_conditions_.at(0) + child_cnt);
      bool need_reorder = false;
      for (int64_t i = 0; i < child_cnt; ++i) {
        if (i != filter_conditions_.at(i).idx_) {
          need_reorder = true;
          break;
        }
      }
      if (need_reorder) {
        int cg_iter_idxs[child_cnt];
        for (int64_t i = 0; i < child_cnt; ++i) {
          cg_iter_idxs[i] = children[i]->get_cg_iter_idx();
        }
        for (int64_t i = 0; i < child_cnt; ++i) {
          children[i] = filter_conditions_.at(i).filter_;
          children[i]->set_cg_iter_idx(cg_iter_idxs[i]);
          if (filter_iters_ != nullptr && iter_filter_node_ != nullptr && cg_iter_idxs[i] != -1) {
            (*filter_iters_).at(cg_iter_idxs[i]) = filter_conditions_.at(i).filter_iter_;
            (*iter_filter_node_).at(cg_iter_idxs[i]) = filter_conditions_.at(i).filter_node_;
          }
        }
        reordered = true;
      }
    }
  } else if (filter.is_logic_op_node()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
      if (OB_FAIL(analyze_impl(*children[i], reordered))) {
        LOG_WARN("Failed to analyze filter tree", K(ret), K(i), KP(children[i]));
      }
    }
  }

  return ret;
}

void ObWhereOptimizer::collect_filter_info(
    sql::ObPushdownFilterExecutor &filter,
    ObFilterCondition &filter_condition)
{
  filter_condition.filter_cost_time_ = filter.get_filter_realtime_statistics().get_filter_cost_time();
  filter_condition.filtered_row_cnt_ = filter.get_filter_realtime_statistics().get_filtered_row_cnt();
  filter_condition.skip_index_skip_block_cnt_ = filter.get_filter_realtime_statistics().get_skip_index_skip_block_cnt();
  filter.get_filter_realtime_statistics().reset();
  filter_condition.filter_ = &filter;
  if (filter_iters_ != nullptr && iter_filter_node_ != nullptr && filter.get_cg_iter_idx() != -1) {
    filter_condition.filter_iter_ = (*filter_iters_).at(filter.get_cg_iter_idx());
    filter_condition.filter_node_ = (*iter_filter_node_).at(filter.get_cg_iter_idx());
  }
}

void ObWhereOptimizer::judge_filter_whether_enable_reorder(sql::ObPushdownFilterExecutor *filter) {
  if (filter == nullptr) {
    // do nothing
  } else if (filter->is_logic_op_node()) {
    bool enable_reorder = true;
    for (int64_t i = 0; i < filter->get_child_count(); ++i) { // enable reorder of this filter if all childs are not logic op nodes
      sql::ObPushdownFilterExecutor *child = filter->get_childs()[i];
      if (child->is_logic_op_node()) {
        enable_reorder = false;
        judge_filter_whether_enable_reorder(child);
      } else if (child->is_sample_node()) {
        enable_reorder = false;
      }
    }
    filter->set_enable_reorder(enable_reorder);
  }
}

int ObWhereOptimizer::reorder_co_filter()
{
  int ret = OB_SUCCESS;
  bool reordered = false;
  ++batch_num_;
  if (filter_->get_ref() != 1 || !filter_->is_logic_op_node()) {
    /* If there is only one node in the filter tree, do nothing. */
  } else if (reorder_filter_times_ >= reorder_filter_interval_) {
    if (OB_FAIL(analyze(reordered))) {  // reordered is used to ajust the reorder interval adaptively in the future.
      LOG_WARN("Failed to analyze in co where optimzier", K(ret));
    } else {
      reorder_filter_times_ = 0;
      reorder_filter_interval_ = REORDER_FILTER_INTERVAL;
    }
  } else {
    ++reorder_filter_times_;
  }
  if (reordered) {
    LOG_TRACE("Reorder co filter tree", K(ret), KP(this), K(batch_num_), KP(filter_), K(filter_->get_ref()), K(filter_->get_type()),
      K(reorder_filter_times_), K(reorder_filter_interval_), K(reordered), K(filter_->get_filter_realtime_statistics()));
  }
  return ret;
}

int ObWhereOptimizer::reorder_row_filter() {
  int ret = OB_SUCCESS;
  bool reordered = false;
  ++batch_num_;
  if (!filter_->is_logic_op_node() || iter_param_->is_use_column_store()) {
    /* If there is only one node in the filter tree, do nothing. If the column store filter reaches here, do nothing. */
  } else if (reorder_filter_times_ >= reorder_filter_interval_) {
    if (OB_FAIL(analyze(reordered))) {  // reordered is used to ajust the reorder interval adaptively in the future.
      LOG_WARN("Failed to analyze in row where optimzier", K(ret));
    } else {
      reorder_filter_times_ = 0;
      reorder_filter_interval_ = REORDER_FILTER_INTERVAL;
    }
  } else {
    ++reorder_filter_times_;
  }
  if (reordered) {
    LOG_TRACE("Reorder row filter tree", K(ret), KP(this), K(batch_num_), KP(filter_), K(filter_->get_type()), K(iter_param_->is_use_column_store()),
      K(reorder_filter_times_), K(reorder_filter_interval_), K(reordered), K(filter_->get_filter_realtime_statistics()));
  }
  return ret;
}

}
}
