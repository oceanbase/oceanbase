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

#ifndef _OB_PX_RESOURCE_ANALYZER_H
#define _OB_PX_RESOURCE_ANALYZER_H 1
#include "sql/resolver/dml/ob_select_stmt.h"
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
using namespace common::hash;
namespace sql
{

#define OPEN_PX_RESOURCE_ANALYZE_ARG  \
        cur_parallel_thread_count, cur_parallel_group_count,              \
        cur_parallel_thread_map, cur_parallel_group_map,                  \
        max_parallel_thread_count, max_parallel_group_count,              \
        max_parallel_thread_map, max_parallel_group_map,                  \
        px_res_analyzer, append_map

#define OPEN_PX_RESOURCE_ANALYZE_DECLARE_ARG  \
          int64_t &cur_parallel_thread_count,                             \
          int64_t &cur_parallel_group_count,                              \
          hash::ObHashMap<ObAddr, int64_t> &cur_parallel_thread_map,      \
          hash::ObHashMap<ObAddr, int64_t> &cur_parallel_group_map,       \
          int64_t &max_parallel_thread_count,                             \
          int64_t &max_parallel_group_count,                              \
          hash::ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,      \
          hash::ObHashMap<ObAddr, int64_t> &max_parallel_group_map,       \
          ObPxResourceAnalyzer &px_res_analyzer, bool append_map

#define CLOSE_PX_RESOURCE_ANALYZE_ARG  \
        cur_parallel_thread_count, cur_parallel_group_count,              \
        cur_parallel_thread_map, cur_parallel_group_map,                  \
        px_res_analyzer, append_map

#define CLOSE_PX_RESOURCE_ANALYZE_DECLARE_ARG  \
          int64_t &cur_parallel_thread_count,                             \
          int64_t &cur_parallel_group_count,                              \
          hash::ObHashMap<ObAddr, int64_t> &cur_parallel_thread_map,      \
          hash::ObHashMap<ObAddr, int64_t> &cur_parallel_group_map,       \
          ObPxResourceAnalyzer &px_res_analyzer, bool append_map

enum DfoStatus {
  INIT,  // 未调度，不占用线程资源
  SCHED, // 执行中，占用线程资源
  FINISH // 执行完成，释放线程资源
};

template <class T>
class DfoTreeNormalizer
{
public:
  // 将叶子节点旋转至右边，确保中间节点在左边。
  // 同时检测 bushy tree 的情形，报错退出
  static int normalize(T &root);
};

struct DfoInfo {
  DfoInfo() : parent_(nullptr),
    depend_sibling_(nullptr),
    child_dfos_(),
    status_(DfoStatus::INIT),
    dop_(0),
    location_addr_(),
    force_bushy_(false),
    root_op_(nullptr),
    has_nested_px_(false),
    nested_px_thread_cnt_(0),
    nested_px_group_cnt_(0)
  {}
  DfoInfo *parent_;
  DfoInfo *depend_sibling_;
  common::ObSEArray<DfoInfo *, 3> child_dfos_;
  DfoStatus status_;
  int64_t dop_;
  ObHashSet<ObAddr> location_addr_;
  bool force_bushy_;
  ObLogicalOperator *root_op_;
  bool has_nested_px_;
  int64_t nested_px_thread_cnt_;
  int64_t nested_px_group_cnt_;
  ObHashMap<ObAddr, int64_t> nested_px_thread_map_;
  ObHashMap<ObAddr, int64_t> nested_px_group_map_;

  void destroy()
  {
    for (int64_t i = 0; i < child_dfos_.count(); i++) {
      child_dfos_.at(i)->destroy();
    }
    child_dfos_.reset();
    location_addr_.destroy();
    nested_px_thread_map_.destroy();
    nested_px_group_map_.destroy();
  }

  inline void set_root_op(ObLogicalOperator *root_op) { root_op_ = root_op;}
  inline ObLogicalOperator *get_root_op() { return root_op_;}
  inline void set_force_bushy(bool flag) { force_bushy_ = flag; }
  inline bool force_bushy() { return force_bushy_; }
  bool has_sibling() const { return nullptr != depend_sibling_; }
  void set_depend_sibling(DfoInfo *sibling) { depend_sibling_ = sibling; }
  inline bool has_child() const { return child_dfos_.count() > 0; }
  inline bool has_parent() const { return nullptr != parent_; }
  inline bool is_leaf_node() const { return !has_child(); }
  int add_child(DfoInfo *child);
  int get_child(int64_t idx, DfoInfo *&child);
  int64_t get_child_count() const { return child_dfos_.count(); }
  inline void set_parent(DfoInfo *p) { parent_ = p; }
  void set_dop(int64_t dop) { dop_ = dop; }
  int64_t get_dop() const { return dop_; }
  bool not_scheduled() { return DfoStatus::INIT == status_; }
  bool is_scheduling() { return DfoStatus::SCHED == status_; }
  void set_scheduled() { status_ = DfoStatus::SCHED; }
  void set_finished() { status_ = DfoStatus::FINISH; }
  void set_has_depend_sibling(bool has_depend_sibling) { UNUSED(has_depend_sibling); }
  bool is_finish() const
  {
    return DfoStatus::FINISH == status_;
  }
  bool is_all_child_finish() const
  {
    bool f = true;
    for (int64_t i = 0; i < child_dfos_.count(); ++i) {
      if (false == child_dfos_.at(i)->is_finish()) {
        f = false;
        break;
      }
    }
    return f;
  }
  TO_STRING_KV(K_(status), K_(dop), K_(has_nested_px));
};

struct LogRuntimeFilterDependencyInfo
{
public:
  LogRuntimeFilterDependencyInfo() : rf_create_ops_() {}
  ~LogRuntimeFilterDependencyInfo() = default;
  inline bool is_empty() const {
    return rf_create_ops_.empty();
  }
  int describe_dependency(DfoInfo *root_dfo);
public:
  ObTMArray<const ObLogicalOperator *> rf_create_ops_;
};

class ObLogExchange;

/*
 * 计算逻辑计划需要预约多少组线程才能调度成功
 */
class ObPxResourceAnalyzer
{
public:
struct PxInfo {
  PxInfo() : inited_(false), root_op_(nullptr), root_dfo_(nullptr), threads_cnt_(0), group_cnt_(0),
             rf_dpd_info_() {}
  PxInfo(ObLogExchange *root_op, DfoInfo *root_dfo)
      : inited_(false), root_op_(root_op), root_dfo_(root_dfo),
        threads_cnt_(0), group_cnt_(0), rf_dpd_info_()
  {}
  ~PxInfo() {
    destroy();
  }
  void destroy()
  {
    if (OB_NOT_NULL(root_dfo_)) {
      root_dfo_->destroy();
      root_dfo_ = NULL;
    }
    thread_map_.destroy();
    group_map_.destroy();
  }
  bool inited_;
  ObLogExchange *root_op_;
  DfoInfo *root_dfo_;
  // count of required threads for scheduling this px.
  int64_t threads_cnt_;
  int64_t group_cnt_;
  ObHashMap<ObAddr, int64_t> thread_map_;
  ObHashMap<ObAddr, int64_t> group_map_;
  LogRuntimeFilterDependencyInfo rf_dpd_info_;
  TO_STRING_KV(K_(threads_cnt), K_(group_cnt));
};
public:
  ObPxResourceAnalyzer();
  ~ObPxResourceAnalyzer() = default;
  int analyze(
      ObLogicalOperator &root_op,
      int64_t &max_parallel_thread_count,
      int64_t &max_parallel_group_count,
      ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
      ObHashMap<ObAddr, int64_t> &max_parallel_group_map);
  int append_px(OPEN_PX_RESOURCE_ANALYZE_DECLARE_ARG, PxInfo &px_info);
  int remove_px(CLOSE_PX_RESOURCE_ANALYZE_DECLARE_ARG, PxInfo &px_info);
  int recursive_walk_through_px_tree(PxInfo &px_tree);

private:
  int convert_log_plan_to_nested_px_tree(ObLogicalOperator &root_op);
  int create_dfo_tree(ObLogExchange &root_op);
  int do_split(
      PxInfo &px_info,
      ObLogicalOperator &root_op,
      DfoInfo *parent_dfo);
  int walk_through_logical_plan(
    ObLogicalOperator &root_op,
    int64_t &max_parallel_thread_count,
    int64_t &max_parallel_group_count,
    ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
    ObHashMap<ObAddr, int64_t> &max_parallel_group_map);
  int walk_through_dfo_tree(
      PxInfo &px_root,
      int64_t &max_parallel_thread_count,
      int64_t &max_parallel_group_count,
      ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
      ObHashMap<ObAddr, int64_t> &max_parallel_group_map);
  int create_dfo(DfoInfo *&dfo, int64_t dop);
  int create_dfo(DfoInfo *&dfo, ObLogicalOperator &root_op);
  int get_dfo_addr_set(const ObLogicalOperator &root_op, ObHashSet<ObAddr> &addr_set);
  template <bool append>
  int px_tree_append(ObHashMap<ObAddr, int64_t> &max_parallel_count,
                     ObHashMap<ObAddr, int64_t> &parallel_count);
int schedule_dfo(
    DfoInfo &dfo,
    int64_t &threads,
    int64_t &groups,
    ObHashMap<ObAddr, int64_t> &current_thread_map,
    ObHashMap<ObAddr, int64_t> &current_group_map);
int finish_dfo(
    DfoInfo &dfo,
    int64_t &threads,
    int64_t &groups,
    ObHashMap<ObAddr, int64_t> &current_thread_map,
    ObHashMap<ObAddr, int64_t> &current_group_map);
int update_parallel_map(
    ObHashMap<ObAddr, int64_t> &parallel_map,
    const ObHashSet<ObAddr> &addr_set,
    int64_t count);
int update_parallel_map_one_addr(
    ObHashMap<ObAddr, int64_t> &parallel_map,
    const ObAddr &addr,
    int64_t count,
    bool append);
int update_max_thead_group_info(
    const int64_t threads,
    const int64_t groups,
    const ObHashMap<ObAddr, int64_t> &current_thread_map,
    const ObHashMap<ObAddr, int64_t> &current_group_map,
    int64_t &max_threads,
    int64_t &max_groups,
    ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
    ObHashMap<ObAddr, int64_t> &max_parallel_group_map);
private:
  void print_px_usage(const ObHashMap<ObAddr, int64_t> &max_map);
private:
  /* variables */
  common::ObArenaAllocator allocator_;
  ObArray<PxInfo *> px_trees_;
  DISALLOW_COPY_AND_ASSIGN(ObPxResourceAnalyzer);
};

template <class T>
int DfoTreeNormalizer<T>::normalize(T &root)
{
  int ret = OB_SUCCESS;
  int64_t non_leaf_cnt = 0;
  int64_t non_leaf_pos = -1;
  bool need_force_bushy = root.force_bushy();
  ARRAY_FOREACH_X(root.child_dfos_, idx, cnt, OB_SUCC(ret)) {
    T *dfo = root.child_dfos_.at(idx);
    if (0 < dfo->get_child_count()) {
      non_leaf_cnt++;
      if (-1 == non_leaf_pos) {
        non_leaf_pos = idx;
      }
    }
  }
  if (non_leaf_cnt > 1 || need_force_bushy) {
    // UPDATE:
    // 考虑到这种场景很少见，对于 bushy tree 不做右深树变左深树的优化，
    // 直接按照树本来形态调度
  } else if (0 < non_leaf_pos) {
    /*
     * swap dfos to reorder schedule seq
     *
     * 最简单的模式：
     *
     *      inode                 inode
     *      /   \       ===>      /   \
     *    leaf  inode           inode  leaf
     *
     * [*] inode 表示非叶子节点
     *
     * 复杂一些的模式：
     *
     * root 节点有 4 个 孩子，其中第三个是中间，其余是叶子节点
     *
     *      root  --------+-----+
     *      |      |      |     |
     *      leaf0  leaf1  inode leaf2
     *
     * dependence 关系为：inode 依赖 leaf0 和 leaf1，且期待先调度 leaf0，再调度 leaf1
     *
     *  变换后：
     *
     *     root  --------+-----+
     *      |     |      |     |
     *      inode leaf0  leaf1 leaf2
     */

    // (1) build dependence
    // 特别说明：逻辑上，inode 节点拥有一个数组，上面依次记录了它依赖
    // 的叶子节点。为了避免维护数组的开销，让这些依赖的叶子节点形成一个
    // 依赖链条，其效果就等价于在 inode 上设置一个数组了。如上图。
    T *inode = root.child_dfos_.at(non_leaf_pos);
    for (int64_t i = 1; i < non_leaf_pos; ++i) {
      root.child_dfos_.at(i - 1)->set_depend_sibling(root.child_dfos_.at(i));
    }
    inode->set_depend_sibling(root.child_dfos_.at(0));
    inode->set_has_depend_sibling(true);

    // (2) transform
    // 将 inode 节点"荡"到最开始的位置
    for (int64_t i = non_leaf_pos; i > 0; --i) {
      root.child_dfos_.at(i) = root.child_dfos_.at(i-1);
    }
    root.child_dfos_.at(0) = inode;
  }
  if (OB_SUCC(ret)) {
    ARRAY_FOREACH_X(root.child_dfos_, idx, cnt, OB_SUCC(ret)) {
      if (OB_ISNULL(root.child_dfos_.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "NULL ptr", K(idx), K(cnt), K(ret));
      } else if (OB_FAIL(normalize(*root.child_dfos_.at(idx)))) {
        SQL_LOG(WARN, "fail normalize dfo", K(idx), K(cnt), K(ret));
      }
    }
  }
  return ret;
}

class LogLowestCommonAncestorFinder
{
public:
  // for optimizer
  static int find_op_common_ancestor(
      const ObLogicalOperator *left, const ObLogicalOperator *right, const ObLogicalOperator *&ancestor);
  static int get_op_dfo(const ObLogicalOperator *op, DfoInfo *root_dfo, DfoInfo *&op_dfo);
};

}/* ns sql */
}/* ns oceanbase */









#endif
