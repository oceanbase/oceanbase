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
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_del_upd.h"
#include "sql/optimizer/ob_log_join_filter.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase
{
namespace sql
{
class SchedOrderGenerator {
public:
  SchedOrderGenerator() = default;
  ~SchedOrderGenerator() = default;
  int generate(DfoInfo &root, ObIArray<DfoInfo *> &edges);
};
}
}

// 后序遍历 dfo tree 即为调度顺序
// 用 edges 数组表示这种顺序
// 注意：
// 1. root 节点本身不会记录到 edge 中
// 2. 不需要做 normalize，因为我们这个文件只负责估计线程数，不做也可以得到相同结果
int SchedOrderGenerator::generate(
    DfoInfo &root,
    ObIArray<DfoInfo *> &edges)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < root.get_child_count(); ++i) {
    DfoInfo *child = NULL;
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


int LogRuntimeFilterDependencyInfo::describe_dependency(DfoInfo *root_dfo)
{
  int ret = OB_SUCCESS;
  // for each rf create op, find its pair rf use op,
  // then get the lowest common ancestor of them, mark force_bushy of the dfo which the ancestor belongs to.
  for (int64_t i = 0; i < rf_create_ops_.count() && OB_SUCC(ret); ++i) {
    const ObLogJoinFilter *create_op = static_cast<const ObLogJoinFilter *>(rf_create_ops_.at(i));
    const ObLogicalOperator *use_op = create_op->get_paired_join_filter();
    if (OB_ISNULL(use_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("use_op is null");
    } else {
      const ObLogicalOperator *ancestor_op = nullptr;
      DfoInfo *op_dfo = nullptr;;
      if (OB_FAIL(LogLowestCommonAncestorFinder::find_op_common_ancestor(create_op, use_op, ancestor_op))) {
        LOG_WARN("failed to find op common ancestor");
      } else if (OB_ISNULL(ancestor_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op common ancestor not found");
      } else if (OB_FAIL(LogLowestCommonAncestorFinder::get_op_dfo(ancestor_op, root_dfo, op_dfo))) {
        LOG_WARN("failed to find op common ancestor");
      } else if (OB_ISNULL(op_dfo)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the dfo of ancestor_op not found");
      } else {
        // Once the DFO which the ancestor belongs to has set the flag "force_bushy",
        // the DfoTreeNormalizer will not attempt to transform a right-deep DFO tree
        // into a left-deep DFO tree. Consequently, the "join filter create" operator
        // can be scheduled earlier than the "join filter use" operator.
        op_dfo->set_force_bushy(true);
      }
    }
  }
  return ret;
}


int DfoInfo::add_child(DfoInfo *child)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(child_dfos_.push_back(child))) {
    LOG_WARN("fail push back child to array", K(ret));
  }
  return ret;
}

int DfoInfo::get_child(int64_t idx, DfoInfo *&child)
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



ObPxResourceAnalyzer::ObPxResourceAnalyzer()
  : allocator_(CURRENT_CONTEXT->get_malloc_allocator())
{
  allocator_.set_label("PxResourceAnaly");
}


// entry function
int ObPxResourceAnalyzer::analyze(
    ObLogicalOperator &root_op,
    int64_t &max_parallel_thread_count,
    int64_t &max_parallel_group_count,
    ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
    ObHashMap<ObAddr, int64_t> &max_parallel_group_map)
{
  int ret = OB_SUCCESS;
  // 本函数用于分析一个 PX 计划至少需要预留多少组线程才能被调度成功
  //
  // 需要考虑如下场景：
  //  1. multiple px
  //  2. subplan filter rescan (所有被标记为 rescanable 的 exchange 节点都是 QC)
  //  3. bushy tree scheduling
  //
  // 算法：
  // 1. 按照 dfo 调度算法生成调度顺序
  // 2. 然后模拟调度，每调度一对 dfo，就将 child 设置为 done，然后统计当前时刻多少个未完成 dfo
  // 3. 如此继续调度，直至所有 dfo 调度完成
  //
  // ref:
  if (log_op_def::LOG_EXCHANGE == root_op.get_type() &&
      static_cast<const ObLogExchange *>(&root_op)->get_is_remote()) {
    max_parallel_thread_count = 0;
    max_parallel_group_count = 0;
  } else if (OB_FAIL(convert_log_plan_to_nested_px_tree(root_op))) {
    LOG_WARN("fail convert log plan to nested px tree", K(ret));
  } else if (OB_FAIL(walk_through_logical_plan(root_op, max_parallel_thread_count,
                                                max_parallel_group_count,
                                                max_parallel_thread_map,
                                                max_parallel_group_map))) {
    LOG_WARN("walk through logical plan failed", K(ret));
  }
  for (int64_t i = 0; i < px_trees_.count(); ++i) {
    if (OB_NOT_NULL(px_trees_.at(i))) {
      px_trees_.at(i)->~PxInfo();
      px_trees_.at(i) = NULL;
    }
  }
  return ret;
}

// append thread usage of px_info to current thread usage stored in ObPxResourceAnalyzer and update max usage.
int ObPxResourceAnalyzer::append_px(OPEN_PX_RESOURCE_ANALYZE_DECLARE_ARG, PxInfo &px_info)
{
  int ret = OB_SUCCESS;
  cur_parallel_thread_count += px_info.threads_cnt_;
  cur_parallel_group_count += px_info.group_cnt_;
  if (!append_map) {
    // only increase current parallel count.
  } else if (OB_FAIL(px_tree_append<true>(cur_parallel_thread_map, px_info.thread_map_))) {
    LOG_WARN("px tree append failed", K(ret));
  } else if (OB_FAIL(px_tree_append<true>(cur_parallel_group_map, px_info.group_map_))) {
    LOG_WARN("px tree append failed", K(ret));
  } else {
    max_parallel_thread_count = max(max_parallel_thread_count, cur_parallel_thread_count);
    max_parallel_group_count = max(max_parallel_group_count, cur_parallel_group_count);
    FOREACH_X(iter, cur_parallel_thread_map, OB_SUCC(ret)) {
      if (OB_FAIL(update_parallel_map_one_addr(max_parallel_thread_map, iter->first, iter->second, false /*append*/))) {
        LOG_WARN("update parallel map one addr failed", K(ret));
      }
    }
    FOREACH_X(iter, cur_parallel_group_map, OB_SUCC(ret)) {
      if (OB_FAIL(update_parallel_map_one_addr(max_parallel_group_map, iter->first, iter->second, false /*append*/))) {
        LOG_WARN("update parallel map one addr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::remove_px(CLOSE_PX_RESOURCE_ANALYZE_DECLARE_ARG, PxInfo &px_info)
{
  int ret = OB_SUCCESS;
  cur_parallel_thread_count -= px_info.threads_cnt_;
  cur_parallel_group_count -= px_info.group_cnt_;
  if (!append_map) {
    // only decrease current parallel count.
  } else if (OB_FAIL(px_tree_append<false>(cur_parallel_thread_map, px_info.thread_map_))) {
    LOG_WARN("px tree append failed", K(ret));
  } else if (OB_FAIL(px_tree_append<false>(cur_parallel_group_map, px_info.group_map_))) {
    LOG_WARN("px tree append failed", K(ret));
  }
  return ret;

}

// either root_op is a px coordinator or there is no px coordinator above root_op.
int ObPxResourceAnalyzer::convert_log_plan_to_nested_px_tree(ObLogicalOperator &root_op)
{
  int ret = OB_SUCCESS;
  // 算法逻辑上分为两步走：
  // 1. qc 切分：顶层 qc ，subplan filter  右侧 qc
  // 2. 各个 qc 分别算并行度（zigzag, left-deep, right-deep, bushy）
  //
  // 具体实现上，两件事情并在一个流程里做，更简单些
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow, maybe too deep recursive", K(ret));
  } else if (log_op_def::LOG_EXCHANGE == root_op.get_type() &&
      static_cast<const ObLogExchange *>(&root_op)->is_px_consumer()) {
    // 当前 exchange 是一个 QC，将下面的所有子计划抽象成一个 dfo tree
    if (OB_FAIL(create_dfo_tree(static_cast<ObLogExchange &>(root_op)))) {
      LOG_WARN("fail create dfo tree", K(ret));
    }
  } else {
    int64_t num = root_op.get_num_of_child();
    for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < num; ++child_idx) {
      if (nullptr == root_op.get_child(child_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(child_idx), K(num), K(ret));
      } else if (OB_FAIL(SMART_CALL(convert_log_plan_to_nested_px_tree(
                  *root_op.get_child(child_idx))))) {
        LOG_WARN("fail split px tree", K(child_idx), K(num), K(ret));
      }
    }
  }
  return ret;
}

// root_op is a PX COORDINATOR
int ObPxResourceAnalyzer::create_dfo_tree(ObLogExchange &root_op)
{
  int ret = OB_SUCCESS;
  // 以 root_op 为根节点创建一个 dfo tree
  // root_op 的类型一定是 EXCHANGE IN DIST

  // 在向下遍历构造 dfo tree 时，如果遇到 subplan filter 右侧的 exchange，
  // 则将其也转化成一个独立的 dfo tree
  PxInfo *px_info = NULL;
  ObLogicalOperator *child = root_op.get_child(ObLogicalOperator::first_child);
  void *mem_ptr = allocator_.alloc(sizeof(PxInfo));
  if (OB_ISNULL(mem_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail allocate memory", K(ret));
  } else {
    px_info = new(mem_ptr) PxInfo();
    px_info->root_op_ = &root_op;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(px_trees_.push_back(px_info))) {
    LOG_WARN("push back failed", K(ret));
  } else if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exchange out op should always has a child",
             "type", root_op.get_type(), KP(child), K(ret));
  } else if (log_op_def::LOG_EXCHANGE != child->get_type() ||
             static_cast<const ObLogExchange *>(child)->is_px_consumer()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect a px producer below qc op", "type", root_op.get_type(), K(ret));
  } else if (OB_FAIL(do_split(*px_info, *child, NULL /*root_dfo*/))) {
    LOG_WARN("fail split dfo for current dfo tree", K(ret));
  } else {
    (static_cast<ObLogExchange &>(root_op)).set_px_info(px_info);
  }
  return ret;
}


int ObPxResourceAnalyzer::do_split(
    PxInfo &px_info,
    ObLogicalOperator &root_op,
    DfoInfo *parent_dfo)
{
  int ret = OB_SUCCESS;
  // 遇到 subplan filter 右边的 exchange，都将它转成独立的 px tree，并终止向下遍历
  // 算法：
  //  1. 如果当前节点不是 TRANSMIT 算子，则递归遍历它的每一个 child
  //  2. 如果当前是 TRANSMIT 算子，则建立一个 dfo，同时记录它的父 dfo，并继续向下遍历
  //     如果没有父 dfo，说明它是 px root，记录到 px trees 中
  //  3. TODO: 对于 subplan filter rescan 的考虑

  // 算法分为两步走：
  // 1. qc 切分：顶层 qc ，subplan filter  右侧 qc
  // 2. 各个 qc 分别算并行度（zigzag, left-deep, right-deep, bushy）
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow, maybe too deep recursive", K(ret));
  } else if (log_op_def::LOG_EXCHANGE == root_op.get_type() &&
      static_cast<const ObLogExchange&>(root_op).is_px_consumer() &&
      static_cast<const ObLogExchange&>(root_op).is_rescanable()) {
    if (OB_NOT_NULL(parent_dfo)) {
      parent_dfo->has_nested_px_ = true;
    }
    if (OB_FAIL(convert_log_plan_to_nested_px_tree(root_op))) {
      LOG_WARN("fail create qc for rescan op", K(ret));
    }
  } else {
    if (OB_FAIL(ret)) {
    } else if (log_op_def::LOG_JOIN_FILTER == root_op.get_type()) {
      ObLogJoinFilter &log_join_filter = static_cast<ObLogJoinFilter &>(root_op);
      if (log_join_filter.is_create_filter()
          && OB_FAIL(px_info.rf_dpd_info_.rf_create_ops_.push_back(&root_op))) {
        LOG_WARN("failed to push_back log join filter create", K(ret));
      }
    } else if (log_op_def::LOG_EXCHANGE == root_op.get_type()
               && static_cast<const ObLogExchange &>(root_op).is_px_producer()) {
      DfoInfo *dfo = nullptr;
      if (OB_FAIL(create_dfo(dfo, root_op))) {
        LOG_WARN("fail create dfo", K(ret));
      } else {
        if (OB_FAIL(dfo->location_addr_.create(hash::cal_next_prime(10), "PxResourceBucket", "PxResourceNode"))) {
          LOG_WARN("fail to create hash set", K(ret));
        } else if (OB_FAIL(get_dfo_addr_set(root_op, dfo->location_addr_))) {
          LOG_WARN("get addr_set failed", K(ret));
          dfo->destroy();
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
    }

    if (OB_SUCC(ret)) {
      int64_t num = root_op.get_num_of_child();
      for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < num; ++child_idx) {
        if (OB_ISNULL(root_op.get_child(child_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null ptr", K(child_idx), K(num), K(ret));
        } else if (OB_FAIL(SMART_CALL(do_split(
                    px_info,
                    *root_op.get_child(child_idx),
                    parent_dfo)))) {
          LOG_WARN("fail split px tree", K(child_idx), K(num), K(ret));
        }
      }
      if (parent_dfo->location_addr_.size() == 0) {
        if (parent_dfo->has_child()) {
          DfoInfo *child_dfo = nullptr;
          if (OB_FAIL(parent_dfo->get_child(0, child_dfo))) {
            LOG_WARN("get child dfo failed", K(ret));
          } else {
            for (ObHashSet<ObAddr>::const_iterator it = child_dfo->location_addr_.begin();
                OB_SUCC(ret) && it != child_dfo->location_addr_.end(); ++it) {
              if (OB_FAIL(parent_dfo->location_addr_.set_refactored(it->first))){
                LOG_WARN("set refactored failed", K(ret), K(it->first));
              }
            }
          }
        } else if (OB_FAIL(parent_dfo->location_addr_.set_refactored(GCTX.self_addr()))){
          LOG_WARN("set refactored failed", K(ret), K(GCTX.self_addr()));
        }
      }
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::create_dfo(DfoInfo *&dfo, ObLogicalOperator &root_op)
{
  int ret = OB_SUCCESS;
  int64_t dop = static_cast<const ObLogExchange&>(root_op).get_parallel();
  void *mem_ptr = allocator_.alloc(sizeof(DfoInfo));
  if (OB_ISNULL(mem_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail allocate memory", K(ret));
  } else if (nullptr == (dfo = new(mem_ptr) DfoInfo())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Null ptr unexpected", KP(mem_ptr), K(ret));
  } else {
    dfo->set_root_op(&root_op);
    dfo->set_dop(dop);
  }
  return ret;
}

int ObPxResourceAnalyzer::get_dfo_addr_set(const ObLogicalOperator &root_op, ObHashSet<ObAddr> &addr_set)
{
  int ret = OB_SUCCESS;
  if ((root_op.is_table_scan() && !root_op.get_contains_fake_cte()) ||
      (root_op.is_dml_operator() && (static_cast<const ObLogDelUpd&>(root_op)).is_pdml())) {
    const ObTablePartitionInfo *tbl_part_info = nullptr;
    if (root_op.is_table_scan()) {
      const ObLogTableScan &tsc = static_cast<const ObLogTableScan&>(root_op);
      tbl_part_info = tsc.get_table_partition_info();
    } else {
      const ObLogDelUpd &dml_op = static_cast<const ObLogDelUpd&>(root_op);
      tbl_part_info = dml_op.get_table_partition_info();
    }
    if (OB_ISNULL(tbl_part_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get table partition info failed", K(ret),
                                                  K(root_op.get_type()),
                                                  K(root_op.get_operator_id()));
    } else {
      const ObCandiTableLoc &phy_tbl_loc_info = tbl_part_info->get_phy_tbl_location_info();
      const ObCandiTabletLocIArray &phy_part_loc_info_arr = phy_tbl_loc_info.get_phy_part_loc_info_list();
      for (int64_t i = 0; i < phy_part_loc_info_arr.count(); ++i) {
        share::ObLSReplicaLocation replica_loc;
        if (OB_FAIL(phy_part_loc_info_arr.at(i).get_selected_replica(replica_loc))) {
          LOG_WARN("get selected replica failed", K(ret));
        } else if (OB_FAIL(addr_set.set_refactored(replica_loc.get_server(), 1))) {
          LOG_WARN("addr set refactored failed");
        } else {
          LOG_DEBUG("resource analyzer", K(root_op.get_type()),
                                         K(root_op.get_operator_id()),
                                         K(replica_loc.get_server()));
        }
      }
    }
  } else {
    int64_t num = root_op.get_num_of_child();
    for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < num; ++child_idx) {
      ObLogicalOperator *child_op = nullptr;
      if (OB_ISNULL(child_op = root_op.get_child(child_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(child_idx), K(num), K(ret));
      } else if (log_op_def::LOG_EXCHANGE == child_op->get_type() &&
                 static_cast<const ObLogExchange*>(child_op)->is_px_consumer()) {
        // do nothing
      } else if (OB_FAIL(SMART_CALL(get_dfo_addr_set(*child_op, addr_set)))) {
        LOG_WARN("get addr_set failed", K(ret));
      }
    }
  }

  return ret;
}

// root_op is the root operator of the plan or a dfo with nested px coord.
// This function calculates the px usage of the plan or the dfo.
int ObPxResourceAnalyzer::walk_through_logical_plan(
    ObLogicalOperator &root_op,
    int64_t &max_parallel_thread_count,
    int64_t &max_parallel_group_count,
    ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
    ObHashMap<ObAddr, int64_t> &max_parallel_group_map)
{
  int ret = OB_SUCCESS;
  int64_t bucket_size = cal_next_prime(10);
  max_parallel_thread_count = 0;
  max_parallel_group_count = 0;
  int64_t cur_parallel_thread_count = 0;
  int64_t cur_parallel_group_count = 0;
  ObHashMap<ObAddr, int64_t> cur_parallel_thread_map;
  ObHashMap<ObAddr, int64_t> cur_parallel_group_map;
  if (OB_FAIL(cur_parallel_thread_map.create(bucket_size, ObModIds::OB_SQL_PX,
                                              ObModIds::OB_SQL_PX))){
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(cur_parallel_group_map.create(bucket_size, ObModIds::OB_SQL_PX,
                                                    ObModIds::OB_SQL_PX))){
    LOG_WARN("create hash map failed", K(ret));
  } else if (max_parallel_thread_map.created()) {
    max_parallel_thread_map.clear();
    max_parallel_group_map.clear();
  } else if (OB_FAIL(max_parallel_thread_map.create(bucket_size,
                                                    ObModIds::OB_SQL_PX,
                                                    ObModIds::OB_SQL_PX))){
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(max_parallel_group_map.create(bucket_size,
                                                   ObModIds::OB_SQL_PX,
                                                   ObModIds::OB_SQL_PX))){
    LOG_WARN("create hash map failed", K(ret));
  }
  ObPxResourceAnalyzer &px_res_analyzer = *this;
  bool append_map = true;
  if (OB_SUCC(ret) && OB_FAIL(root_op.open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_ARG))) {
    LOG_WARN("open px resource analyze failed", K(ret));
  }
  return ret;
}

// walk through the px_tree and all its children px_trees recursively to init thread/group usage of px_tree.
int ObPxResourceAnalyzer::recursive_walk_through_px_tree(PxInfo &px_tree)
{
  int ret = OB_SUCCESS;
  if (!px_tree.inited_) {
    px_tree.threads_cnt_ = 0;
    px_tree.group_cnt_ = 0;
    int64_t bucket_size = cal_next_prime(10);
    if (OB_FAIL(px_tree.thread_map_.create(bucket_size, ObModIds::OB_SQL_PX, ObModIds::OB_SQL_PX))){
      LOG_WARN("create hash map failed", K(ret));
    } else if (OB_FAIL(px_tree.group_map_.create(bucket_size, ObModIds::OB_SQL_PX, ObModIds::OB_SQL_PX))){
      LOG_WARN("create hash map failed", K(ret));
    } else if (OB_FAIL(px_tree.rf_dpd_info_.describe_dependency(px_tree.root_dfo_))) {
      LOG_WARN("failed to describe dependency");
    } else if (OB_FAIL(walk_through_dfo_tree(px_tree, px_tree.threads_cnt_, px_tree.group_cnt_,
                                            px_tree.thread_map_, px_tree.group_map_))) {
      LOG_WARN("fail calc px thread group count", K(ret));
    } else {
      int64_t op_id = OB_ISNULL(px_tree.root_op_) ? OB_INVALID_ID : px_tree.root_op_->get_op_id();
      LOG_TRACE("after walk_through_dfo_tree", K(op_id), K(px_tree));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(px_tree.root_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("QC op not set in px_info struct", K(ret));
    } else {
      // 将当前 px 的 expected 线程数设置到 QC 算子中
      px_tree.root_op_->set_expected_worker_count(px_tree.threads_cnt_);
    }
    px_tree.inited_ = true;
  }
  return ret;
}

/* A dfo tree is scheduled in post order generally.
 * First, we traversal the tree in post order and generate edges.
 * Then for each edge:
 * 1. Schedule this edge if not scheduled.
 * 2. Schedule parent of this edge if not scheduled.
 * 3. Schedule depend siblings of this edge one by one. One by one means finish (i-1)th sibling before schedule i-th sibling.
 * 4. Finish this edge if it is a leaf node or all children are finished.
*/

/* This function also generate a ObHashMap<ObAddr, int64_t> max_parallel_thread_map.
 * Key is observer. Value is max sum of dops of dfos that are scheduled at same time on this observer.
 * Value means expected thread number on this server.
 * Once a dfo is scheduled or finished, we update(increase or decrease) the current_thread_map.
 * Then compare current thead count with max thread count, and update max_parallel_thread_map if necessary.
*/
int ObPxResourceAnalyzer::walk_through_dfo_tree(
    PxInfo &px_root,
    int64_t &max_parallel_thread_count,
    int64_t &max_parallel_group_count,
    ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
    ObHashMap<ObAddr, int64_t> &max_parallel_group_map)
{
  int ret = OB_SUCCESS;
  // 模拟调度过程
  ObArray<DfoInfo *> edges;
  SchedOrderGenerator sched_order_gen;
  int64_t bucket_size = cal_next_prime(10);
  ObHashMap<ObAddr, int64_t> current_thread_map;
  ObHashMap<ObAddr, int64_t> current_group_map;

  if (OB_ISNULL(px_root.root_dfo_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_FAIL(DfoTreeNormalizer<DfoInfo>::normalize(*px_root.root_dfo_))) {
    LOG_WARN("fail normalize px tree", K(ret));
  } else if (OB_FAIL(sched_order_gen.generate(*px_root.root_dfo_, edges))) {
    LOG_WARN("fail generate sched order", K(ret));
  } else if (OB_FAIL(current_thread_map.create(bucket_size,
                                               ObModIds::OB_SQL_PX,
                                               ObModIds::OB_SQL_PX))){
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(current_group_map.create(bucket_size,
                                              ObModIds::OB_SQL_PX,
                                              ObModIds::OB_SQL_PX))){
  }
#ifndef NDEBUG
  for (int x = 0; x < edges.count(); ++x) {
    LOG_DEBUG("dump dfo", K(x), K(*edges.at(x)));
  }
#endif

  int64_t threads = 0;
  int64_t groups = 0;
  int64_t max_threads = 0;
  int64_t max_groups = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < edges.count(); ++i) {
    DfoInfo &child = *edges.at(i);
    // schedule child if not scheduled.
    if (OB_FAIL(schedule_dfo(child, threads, groups, current_thread_map, current_group_map))) {
      LOG_WARN("schedule dfo failed", K(ret));
    } else if (child.has_parent() && OB_FAIL(schedule_dfo(*child.parent_, threads, groups,
                                                          current_thread_map, current_group_map))) {
      LOG_WARN("schedule parent dfo failed", K(ret));
    } else if (child.has_sibling() && child.depend_sibling_->not_scheduled()) {
      DfoInfo *sibling = child.depend_sibling_;
      while (NULL != sibling && OB_SUCC(ret)) {
        if (OB_UNLIKELY(!sibling->is_leaf_node())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sibling must be leaf node", K(ret));
        } else if (OB_FAIL(schedule_dfo(*sibling, threads, groups, current_thread_map,
                                        current_group_map))) {
          LOG_WARN("schedule sibling failed", K(ret));
        } else if (OB_FAIL(update_max_thead_group_info(threads, groups,
                    current_thread_map, current_group_map,
                    max_threads, max_groups,
                    max_parallel_thread_map, max_parallel_group_map))) {
          LOG_WARN("update max_thead group info failed", K(ret));
        } else if (OB_FAIL(finish_dfo(*sibling, threads, groups, current_thread_map,
                                        current_group_map))) {
          LOG_WARN("finish sibling failed", K(ret));
        } else {
          sibling = sibling->depend_sibling_;
        }
      }
    } else {
      if (OB_FAIL(update_max_thead_group_info(threads, groups,
                    current_thread_map, current_group_map,
                    max_threads, max_groups,
                    max_parallel_thread_map, max_parallel_group_map))) {
        LOG_WARN("update max_thead group info failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(finish_dfo(child, threads, groups, current_thread_map,
                                        current_group_map))) {
        LOG_WARN("finish sibling failed", K(ret));
      }
    }

#ifndef NDEBUG
      for (int x = 0; x < edges.count(); ++x) {
        LOG_DEBUG("dump dfo step.finish",
                  K(i), K(x), K(*edges.at(x)), K(threads), K(max_threads), K(groups), K(max_groups));
      }
#endif
  }
  max_parallel_thread_count = max_threads;
  max_parallel_group_count = max_groups;
  LOG_TRACE("end walk_through_dfo_tree", K(max_parallel_thread_count), K(max_parallel_group_count));
  return ret;
}

template <bool append>
int ObPxResourceAnalyzer::px_tree_append(ObHashMap<ObAddr, int64_t> &max_parallel_count,
                                         ObHashMap<ObAddr, int64_t> &parallel_count)
{
  int ret = OB_SUCCESS;
  for (ObHashMap<ObAddr, int64_t>::const_iterator it = parallel_count.begin();
      OB_SUCC(ret) && it != parallel_count.end(); ++it) {
    bool is_exist = true;
    int64_t dop = 0;
    if (OB_FAIL(max_parallel_count.get_refactored(it->first, dop))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("get refactored failed", K(ret), K(it->first));
      } else {
        is_exist = false;
        if (append) {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret)) {
      dop += append ? it->second : -(it->second);
      if (OB_FAIL(max_parallel_count.set_refactored(it->first, dop, is_exist))){
        LOG_WARN("set refactored failed", K(ret), K(it->first), K(dop), K(is_exist));
      }
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::schedule_dfo(
    DfoInfo &dfo,
    int64_t &threads,
    int64_t &groups,
    ObHashMap<ObAddr, int64_t> &current_thread_map,
    ObHashMap<ObAddr, int64_t> &current_group_map)
{
  int ret = OB_SUCCESS;
  if (dfo.not_scheduled()) {
    dfo.set_scheduled();
    threads += dfo.get_dop();
    const int64_t group = 1;
    groups += group;
    ObHashSet<ObAddr> &addr_set = dfo.location_addr_;
    // we assume that should allocate same thread count for each sqc in the dfo.
    // this may not true. but we can't decide the real count for each sqc. just let it be for now
    const int64_t dop_per_addr = 0 == addr_set.size() ? dfo.get_dop() : (dfo.get_dop() + addr_set.size() - 1) / addr_set.size();
    if (OB_FAIL(update_parallel_map(current_thread_map, addr_set, dop_per_addr))) {
      LOG_WARN("increase current thread map failed", K(ret));
    } else if (OB_FAIL(update_parallel_map(current_group_map, addr_set, group))) {
      LOG_WARN("increase current group map failed", K(ret));
    } else if (dfo.has_nested_px_) {
      ObLogicalOperator *root_op = NULL;
      ObLogicalOperator *child = NULL;
      if (OB_ISNULL(root_op = dfo.get_root_op()) || log_op_def::LOG_EXCHANGE != root_op->get_type()
          || !static_cast<const ObLogExchange *>(root_op)->is_px_producer()
          || OB_ISNULL(child = root_op->get_child(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected root op", K(ret), K(root_op));
      // calculate px usage of nested px coord.
      } else if (OB_FAIL(walk_through_logical_plan(*child, dfo.nested_px_thread_cnt_, dfo.nested_px_group_cnt_,
                                                  dfo.nested_px_thread_map_, dfo.nested_px_group_map_))) {
        LOG_WARN("walk through logical plan", K(ret));
      } else {
        // append px usage of nested px coord to the dfo.
        threads += dfo.nested_px_thread_cnt_;
        groups += dfo.nested_px_group_cnt_;
        if (OB_FAIL(px_tree_append<true>(current_thread_map, dfo.nested_px_thread_map_))) {
          LOG_WARN("px tree append failed", K(ret));
        } else if (OB_FAIL(px_tree_append<true>(current_group_map, dfo.nested_px_group_map_))) {
          LOG_WARN("px tree append failed", K(ret));
        }
      }
    }
    LOG_TRACE("[PxResAnaly] schedule dfo", K(dfo.dop_), K(dfo.has_nested_px_),
              K(dfo.nested_px_thread_cnt_), K(dfo.nested_px_group_cnt_), K(threads), K(groups),
              K(OB_ISNULL(dfo.root_op_) ? OB_INVALID_ID : dfo.root_op_->get_op_id()));
  }
  return ret;
}

int ObPxResourceAnalyzer::finish_dfo(
    DfoInfo &dfo,
    int64_t &threads,
    int64_t &groups,
    ObHashMap<ObAddr, int64_t> &current_thread_map,
    ObHashMap<ObAddr, int64_t> &current_group_map)
{
  int ret = OB_SUCCESS;
  if (dfo.is_scheduling() && (dfo.is_leaf_node() || dfo.is_all_child_finish())) {
    dfo.set_finished();
    threads -= dfo.get_dop();
    const int64_t group = 1;
    groups -= group;
    ObHashSet<ObAddr> &addr_set = dfo.location_addr_;
    const int64_t dop_per_addr = 0 == addr_set.size() ? dfo.get_dop() : (dfo.get_dop() + addr_set.size() - 1) / addr_set.size();
    if (OB_FAIL(update_parallel_map(current_thread_map, addr_set, -dop_per_addr))) {
      LOG_WARN("decrease current thread map failed", K(ret));
    } else if (OB_FAIL(update_parallel_map(current_group_map, addr_set, -group))) {
      LOG_WARN("decrease current group map failed", K(ret));
    } else if (dfo.has_nested_px_) {
      threads -= dfo.nested_px_thread_cnt_;
      groups -= dfo.nested_px_group_cnt_;
      if (OB_FAIL(px_tree_append<false>(current_thread_map, dfo.nested_px_thread_map_))) {
        LOG_WARN("px tree append failed", K(ret));
      } else if (OB_FAIL(px_tree_append<false>(current_group_map, dfo.nested_px_group_map_))) {
        LOG_WARN("px tree append failed", K(ret));
      }
    }
    LOG_TRACE("[PxResAnaly] finish dfo", K(dfo.dop_), K(dfo.has_nested_px_),
              K(dfo.nested_px_thread_cnt_), K(dfo.nested_px_group_cnt_), K(threads), K(groups),
              K(OB_ISNULL(dfo.root_op_) ? OB_INVALID_ID : dfo.root_op_->get_op_id()));
  }
  return ret;
}

int ObPxResourceAnalyzer::update_parallel_map(
    ObHashMap<ObAddr, int64_t> &parallel_map,
    const ObHashSet<ObAddr> &addr_set,
    int64_t count)
{
  int ret = OB_SUCCESS;
  for (hash::ObHashSet<ObAddr>::const_iterator it = addr_set.begin();
        OB_SUCC(ret) && it != addr_set.end(); it++) {
    if (OB_FAIL(update_parallel_map_one_addr(parallel_map, it->first, count, true))) {
      LOG_WARN("update parallel map one addr failed", K(ret));
    }
  }
  return ret;
}

// Update current_parallel_map or max_parallel_map.
// When update current_parallel_map, append is true, because we are increasing or decreasing count.
// When update max_parallel_map, append is false.
int ObPxResourceAnalyzer::update_parallel_map_one_addr(
    ObHashMap<ObAddr, int64_t> &parallel_map,
    const ObAddr &addr,
    int64_t count,
    bool append)
{
  int ret = OB_SUCCESS;
  bool is_exist = true;
  int64_t origin_count = 0;
  if (OB_FAIL(parallel_map.get_refactored(addr, origin_count))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("get refactored failed", K(ret), K(addr));
    } else {
      is_exist = false;
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (append) {
      origin_count += count;
    } else {
      origin_count = max(origin_count, count);
    }
    if (OB_FAIL(parallel_map.set_refactored(addr, origin_count, is_exist))){
      LOG_WARN("set refactored failed", K(ret), K(addr), K(origin_count), K(is_exist));
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::update_max_thead_group_info(
    const int64_t threads,
    const int64_t groups,
    const ObHashMap<ObAddr, int64_t> &current_thread_map,
    const ObHashMap<ObAddr, int64_t> &current_group_map,
    int64_t &max_threads,
    int64_t &max_groups,
    ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
    ObHashMap<ObAddr, int64_t> &max_parallel_group_map)
{
  int ret = OB_SUCCESS;
  max_threads = max(threads, max_threads);
  max_groups = max(groups, max_groups);
  for (ObHashMap<ObAddr, int64_t>::const_iterator it = current_thread_map.begin();
      OB_SUCC(ret) && it != current_thread_map.end(); ++it) {
    if (OB_FAIL(update_parallel_map_one_addr(max_parallel_thread_map, it->first, it->second, false))) {
      LOG_WARN("update parallel map one addr failed", K(ret));
    }
  }
  for (ObHashMap<ObAddr, int64_t>::const_iterator it = current_group_map.begin();
      OB_SUCC(ret) && it != current_group_map.end(); ++it) {
    if (OB_FAIL(update_parallel_map_one_addr(max_parallel_group_map, it->first, it->second, false))) {
      LOG_WARN("update parallel map one addr failed", K(ret));
    }
  }
  return ret;
}

int LogLowestCommonAncestorFinder::find_op_common_ancestor(
    const ObLogicalOperator *left, const ObLogicalOperator *right, const ObLogicalOperator *&ancestor)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObLogicalOperator *, 32> ancestors;

  const ObLogicalOperator *parent = left;
  while (OB_NOT_NULL(parent) && OB_SUCC(ret)) {
    if (OB_FAIL(ancestors.push_back(parent))) {
      LOG_WARN("failed to push back");
    } else {
      parent = parent->get_parent();
    }
  }

  parent = right;
  bool find = false;
  while (OB_NOT_NULL(parent) && OB_SUCC(ret) && !find) {
    for (int64_t i = 0; i < ancestors.count() && OB_SUCC(ret); ++i) {
      if (parent == ancestors.at(i)) {
        find = true;
        ancestor = parent;
        break;
      }
    }
    parent = parent->get_parent();
  }
  return ret;
}
int LogLowestCommonAncestorFinder::get_op_dfo(const ObLogicalOperator *op, DfoInfo *root_dfo, DfoInfo *&op_dfo)
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator *parent = op;
  const ObLogicalOperator *dfo_root_op = nullptr;
  while (OB_NOT_NULL(parent) && OB_SUCC(ret)) {
    if (log_op_def::LOG_EXCHANGE == parent->get_type() &&
        static_cast<const ObLogExchange&>(*parent).is_px_producer()) {
      dfo_root_op = parent;
      break;
    } else {
      parent = parent->get_parent();
    }
  }
  DfoInfo *dfo = nullptr;
  bool find = false;

  ObSEArray<DfoInfo *, 16> dfo_queue;
  int64_t cur_que_front = 0;
  if (OB_FAIL(dfo_queue.push_back(root_dfo))) {
    LOG_WARN("failed to push back");
  }

  while (cur_que_front < dfo_queue.count() && !find && OB_SUCC(ret)) {
    int64_t cur_que_size = dfo_queue.count() - cur_que_front;
    for (int64_t i = 0; i < cur_que_size && OB_SUCC(ret); ++i) {
      dfo = dfo_queue.at(cur_que_front);
      if (dfo->get_root_op() == dfo_root_op) {
        op_dfo = dfo;
        find = true;
        break;
      } else {
        // push child into the queue
        for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < dfo->get_child_count(); ++child_idx) {
          if (OB_FAIL(dfo_queue.push_back(dfo->child_dfos_.at(child_idx)))) {
            LOG_WARN("failed to push back child dfo");
          }
        }
      }
      if (OB_SUCC(ret)) {
        cur_que_front++;
      }
    }
  }
  return ret;
}
