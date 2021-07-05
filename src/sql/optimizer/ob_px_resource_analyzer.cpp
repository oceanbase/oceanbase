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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_px_resource_analyzer.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase {
namespace sql {

template <class T>
class DfoTreeNormalizer {
public:
  static int normalize(T& root);
};

template <class T>
int DfoTreeNormalizer<T>::normalize(T& root)
{
  int ret = OB_SUCCESS;
  int64_t non_leaf_cnt = 0;
  int64_t non_leaf_pos = -1;
  ARRAY_FOREACH_X(root.child_dfos_, idx, cnt, OB_SUCC(ret))
  {
    DfoInfo* dfo = root.child_dfos_.at(idx);
    if (0 < dfo->get_child_count()) {
      non_leaf_cnt++;
      if (-1 == non_leaf_pos) {
        non_leaf_pos = idx;
      }
    }
  }
  if (non_leaf_cnt > 1) {
    // sched as the tree shape in bushy tree scenario
  } else if (1 == non_leaf_pos) {
    /*
     * swap dfos to reorder schedule seq
     *
     * simple mode:
     *
     *      inode                 inode
     *      /   \       ===>      /   \
     *    leaf  inode           inode  leaf
     *
     * [*] inode is not leaf
     *
     * complicate mode:
     *
     *      root  --------+-----+
     *      |      |      |     |
     *      leaf0  leaf1  inode leaf2
     *
     *
     *  after transformation:
     *
     *     root  --------+-----+
     *      |     |      |     |
     *      inode leaf0  leaf1 leaf2
     */

    // (1) build dependence
    T* inode = root.child_dfos_.at(non_leaf_pos);
    inode->set_depend_sibling(root.child_dfos_.at(0));

    // (2) transform
    for (int64_t i = non_leaf_pos; i > 0; --i) {
      root.child_dfos_.at(i) = root.child_dfos_.at(i - 1);
    }
    root.child_dfos_.at(0) = inode;
  }
  if (OB_SUCC(ret)) {
    ARRAY_FOREACH_X(root.child_dfos_, idx, cnt, OB_SUCC(ret))
    {
      if (OB_ISNULL(root.child_dfos_.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(idx), K(cnt), K(ret));
      } else if (OB_FAIL(normalize(*root.child_dfos_.at(idx)))) {
        LOG_WARN("fail normalize dfo", K(idx), K(cnt), K(ret));
      }
    }
  }
  return ret;
}

class SchedOrderGenerator {
public:
  SchedOrderGenerator() = default;
  ~SchedOrderGenerator() = default;
  int generate(DfoInfo& root, ObIArray<DfoInfo*>& edges);
};
}  // namespace sql
}  // namespace oceanbase

// post order iterate dfo tree, the result is the sched order
// use edges to represent the order
int SchedOrderGenerator::generate(DfoInfo& root, ObIArray<DfoInfo*>& edges)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < root.get_child_count(); ++i) {
    DfoInfo* child = NULL;
    if (OB_FAIL(root.get_child(i, child))) {
      LOG_WARN("fail get child dfo", K(i), K(root), K(ret));
    } else if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(generate(*child, edges))) {
      LOG_WARN("fail do generate edge", K(*child), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(edges.push_back(&root))) {
      LOG_WARN("fail add edge to array", K(ret));
    }
  }
  return ret;
}

// ===================================================================================
// ===================================================================================
// ===================================================================================
// ===================================================================================

int DfoInfo::add_child(DfoInfo* child)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(child_dfos_.push_back(child))) {
    LOG_WARN("fail push back child to array", K(ret));
  }
  return ret;
}

int DfoInfo::get_child(int64_t idx, DfoInfo*& child)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= child_dfos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child idx unexpected", K(idx), "cnt", child_dfos_.count(), K(ret));
  } else if (OB_FAIL(child_dfos_.at(idx, child))) {
    LOG_WARN("fail get element", K(idx), "cnt", child_dfos_.count(), K(ret));
  }
  return ret;
}

// ===================================================================================
// ===================================================================================
// ===================================================================================
// ===================================================================================

ObPxResourceAnalyzer::ObPxResourceAnalyzer() : dfo_allocator_(CURRENT_CONTEXT.get_malloc_allocator())
{
  dfo_allocator_.set_label("PxResourceAnaly");
}

// entry function
int ObPxResourceAnalyzer::analyze(ObLogicalOperator& root_op, int64_t& max_parallel_thread_group_count)
{
  int ret = OB_SUCCESS;
  // calc thread reservation
  //
  // consider scenarios:
  //  1. multiple px
  //  2. subplan filter rescan (rescanable exchange are QCs)
  //  3. bushy tree scheduling
  ObArray<PxInfo> px_trees;
  if (OB_FAIL(convert_log_plan_to_nested_px_tree(px_trees, root_op))) {
    LOG_WARN("fail convert log plan to nested px tree", K(ret));
  } else if (OB_FAIL(walk_through_px_trees(px_trees, max_parallel_thread_group_count))) {
    LOG_WARN("fail calc max parallel thread group count for resource reservation", K(ret));
  }
  return ret;
}

int ObPxResourceAnalyzer::convert_log_plan_to_nested_px_tree(ObIArray<PxInfo>& px_trees, ObLogicalOperator& root_op)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow, maybe too deep recursive", K(ret));
  } else if (log_op_def::LOG_EXCHANGE == root_op.get_type() &&
             static_cast<const ObLogExchange*>(&root_op)->is_px_consumer()) {
    if (OB_FAIL(create_dfo_tree(px_trees, static_cast<ObLogExchange&>(root_op)))) {
      LOG_WARN("fail create dfo tree", K(ret));
    }
  } else {
    int64_t num = root_op.get_num_of_child();
    for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < num; ++child_idx) {
      DfoInfo* root_dfo = nullptr;
      if (nullptr == root_op.get_child(child_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(child_idx), K(num), K(ret));
      } else if (OB_FAIL(SMART_CALL(convert_log_plan_to_nested_px_tree(px_trees, *root_op.get_child(child_idx))))) {
        LOG_WARN("fail split px tree", K(child_idx), K(num), K(ret));
      }
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::create_dfo_tree(ObIArray<PxInfo>& px_trees, ObLogExchange& root_op)
{
  int ret = OB_SUCCESS;
  PxInfo px_info;
  DfoInfo* root_dfo = nullptr;
  px_info.root_op_ = &root_op;
  ObLogicalOperator* child = root_op.get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exchange out op should always has a child", "type", root_op.get_type(), KP(child), K(ret));

  } else if (log_op_def::LOG_EXCHANGE != child->get_type() ||
             static_cast<const ObLogExchange*>(child)->is_px_consumer()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect a px producer below qc op", "type", root_op.get_type(), K(ret));
  } else if (OB_FAIL(do_split(px_trees, px_info, *child, root_dfo))) {
    LOG_WARN("fail split dfo for current dfo tree", K(ret));
  } else if (OB_FAIL(px_trees.push_back(px_info))) {
    LOG_WARN("fail push back root dfo to dfo tree collector", K(ret));
  }
  return ret;
}

int ObPxResourceAnalyzer::do_split(
    ObIArray<PxInfo>& px_trees, PxInfo& px_info, ObLogicalOperator& root_op, DfoInfo* parent_dfo)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow, maybe too deep recursive", K(ret));
  } else if (log_op_def::LOG_EXCHANGE == root_op.get_type() &&
             static_cast<const ObLogExchange&>(root_op).is_px_consumer() &&
             static_cast<const ObLogExchange&>(root_op).is_rescanable()) {
    if (OB_FAIL(convert_log_plan_to_nested_px_tree(px_trees, root_op))) {
      LOG_WARN("fail create qc for rescan op", K(ret));
    }
  } else {
    if (log_op_def::LOG_EXCHANGE == root_op.get_type() && static_cast<const ObLogExchange&>(root_op).is_px_producer()) {
      DfoInfo* dfo = nullptr;
      if (OB_FAIL(create_dfo(dfo, static_cast<const ObLogExchange&>(root_op).get_px_dop()))) {
        LOG_WARN("fail create dfo", K(ret));
      } else {
        if (nullptr == parent_dfo) {
          px_info.root_dfo_ = dfo;
        } else {
          parent_dfo->add_child(dfo);
        }
        dfo->set_parent(parent_dfo);
        parent_dfo = dfo;
      }
    }

    if (OB_SUCC(ret)) {
      int64_t num = root_op.get_num_of_child();
      for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < num; ++child_idx) {
        DfoInfo* root_dfo = nullptr;
        if (OB_ISNULL(root_op.get_child(child_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null ptr", K(child_idx), K(num), K(ret));
        } else if (OB_FAIL(SMART_CALL(do_split(px_trees, px_info, *root_op.get_child(child_idx), parent_dfo)))) {
          LOG_WARN("fail split px tree", K(child_idx), K(num), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::create_dfo(DfoInfo*& dfo, int64_t dop)
{
  int ret = OB_SUCCESS;
  void* mem_ptr = dfo_allocator_.alloc(sizeof(DfoInfo));
  if (OB_ISNULL(mem_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail allocate memory", K(ret));
  } else if (nullptr == (dfo = new (mem_ptr) DfoInfo())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Null ptr unexpected", KP(mem_ptr), K(ret));
  } else {
    dfo->set_dop(dop);
  }
  return ret;
}

int ObPxResourceAnalyzer::walk_through_px_trees(ObIArray<PxInfo>& px_trees, int64_t& max_parallel_thread_group_count)
{
  int ret = OB_SUCCESS;
  max_parallel_thread_group_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < px_trees.count(); ++i) {
    int64_t count = 0;
    PxInfo& px_info = px_trees.at(i);
    if (OB_FAIL(walk_through_dfo_tree(px_info, count))) {
      LOG_WARN("fail calc px thread group count", K(i), "total", px_trees.count(), K(ret));
    } else if (OB_ISNULL(px_info.root_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("QC op not set in px_info struct", K(ret));
    } else {
      px_info.threads_ = count;
      px_info.root_op_->set_expected_worker_count(count);
    }
    max_parallel_thread_group_count += count;
  }
  return ret;
}

int ObPxResourceAnalyzer::walk_through_dfo_tree(PxInfo& px_root, int64_t& max_parallel_thread_group_count)
{
  int ret = OB_SUCCESS;
  // simulate sched
  ObArray<DfoInfo*> edges;
  SchedOrderGenerator sched_order_gen;

  if (OB_ISNULL(px_root.root_dfo_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_FAIL(DfoTreeNormalizer<DfoInfo>::normalize(*px_root.root_dfo_))) {
    LOG_WARN("fail normalize px tree", K(ret));
  } else if (OB_FAIL(sched_order_gen.generate(*px_root.root_dfo_, edges))) {
    LOG_WARN("fail generate sched order", K(ret));
  }

#ifndef NDEBUG
  for (int x = 0; x < edges.count(); ++x) {
    LOG_DEBUG("dump dfo", K(x), K(*edges.at(x)));
  }
#endif

  int64_t threads = 0;
  int64_t max_threads = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < edges.count(); ++i) {
    DfoInfo& child = *edges.at(i);
    threads += child.set_sched();
    threads += child.set_parent_sched();
    if (child.has_sibling() && child.depend_sibling_->is_leaf_node()) {
      threads += child.depend_sibling_->set_sched();
    }
    if (threads > max_threads) {
      max_threads = threads;
    }
#ifndef NDEBUG
    for (int x = 0; x < edges.count(); ++x) {
      LOG_DEBUG("dump dfo step.sched", K(i), K(x), K(*edges.at(x)), K(threads), K(max_threads));
    }
#endif
    if (child.is_leaf_node()) {
      threads -= child.set_finish();
    } else if (child.is_all_child_finish()) {
      threads -= child.set_finish();
    }
    if (child.has_sibling() && child.depend_sibling_->is_leaf_node()) {
      threads -= child.depend_sibling_->set_finish();
    }
#ifndef NDEBUG
    for (int x = 0; x < edges.count(); ++x) {
      LOG_DEBUG("dump dfo step.finish", K(i), K(x), K(*edges.at(x)), K(threads), K(max_threads));
    }
#endif
  }
  max_parallel_thread_group_count = max_threads;
  return ret;
}
