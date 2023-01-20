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
namespace oceanbase {
namespace sql {

enum DfoStatus { INIT, SCHED, FINISH };

template <class T>
class DfoTreeNormalizer
{
public:
  static int normalize(T &root);
};

struct DfoInfo {
  DfoInfo() : parent_(nullptr), depend_sibling_(nullptr), child_dfos_(), status_(DfoStatus::INIT), dop_(0)
  {}
  DfoInfo* parent_;
  DfoInfo* depend_sibling_;
  common::ObSEArray<DfoInfo*, 3> child_dfos_;
  DfoStatus status_;
  int64_t dop_;

  bool has_sibling() const
  {
    return nullptr != depend_sibling_;
  }
  void set_depend_sibling(DfoInfo* sibling)
  {
    depend_sibling_ = sibling;
  }
  inline bool has_child() const
  {
    return child_dfos_.count() > 0;
  }
  inline bool has_parent() const
  {
    return nullptr != parent_;
  }
  inline bool is_leaf_node() const
  {
    return !has_child();
  }
  int add_child(DfoInfo* child);
  int get_child(int64_t idx, DfoInfo*& child);
  int64_t get_child_count() const
  {
    return child_dfos_.count();
  }
  inline void set_parent(DfoInfo* p)
  {
    parent_ = p;
  }
  void set_dop(int64_t dop)
  {
    dop_ = dop;
  }
  int64_t get_dop() const
  {
    return dop_;
  }

  void set_sched(int64_t &thread)
	{
    if (DfoStatus::INIT == status_) {
      thread += dop_;
      status_ = DfoStatus::SCHED;
    }
  }
  void set_parent_sched(int64_t &thread)
  {
    if (has_parent()) {
      parent_->set_sched(thread);
    }
  }
  void set_finish(int64_t &thread)
  {
    if (DfoStatus::SCHED == status_) {
      thread -= dop_;
      status_ = DfoStatus::FINISH;
    }
  }
  void set_has_depend_sibling(bool has_depend_sibling)
  {
    UNUSED(has_depend_sibling);
  }
  bool is_init() const
  {
    return DfoStatus::INIT == status_;
  }
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
  TO_STRING_KV(K_(status), K_(dop));
};

class ObLogExchange;
struct PxInfo {
  PxInfo() : root_op_(nullptr), root_dfo_(nullptr), threads_(0), acc_threads_(0)
  {}
  PxInfo(ObLogExchange* root_op, DfoInfo* root_dfo)
      : root_op_(root_op), root_dfo_(root_dfo), threads_(0), acc_threads_(0)
  {}
  ObLogExchange* root_op_;
  DfoInfo* root_dfo_;
  int64_t threads_;
  int64_t acc_threads_;
  TO_STRING_KV(K_(threads), K_(acc_threads));
};

class ObPxResourceAnalyzer {
public:
  ObPxResourceAnalyzer();
  ~ObPxResourceAnalyzer() = default;
  int analyze(ObLogicalOperator& root_op, int64_t& max_parallel_thread_group_count);

private:
  int convert_log_plan_to_nested_px_tree(common::ObIArray<PxInfo>& px_trees, ObLogicalOperator& root_op);
  int create_dfo_tree(ObIArray<PxInfo>& px_trees, ObLogExchange& root_op);
  int do_split(common::ObIArray<PxInfo>& px_trees, PxInfo& px_info, ObLogicalOperator& root_op, DfoInfo* parent_dfo);
  int walk_through_px_trees(common::ObIArray<PxInfo>& px_trees, int64_t& max_parallel_thread_group_count);
  int walk_through_dfo_tree(PxInfo& px_root, int64_t& max_parallel_thread_group_count);
  int create_dfo(DfoInfo*& dfo, int64_t dop);
  void release();

private:
  /* variables */
  common::ObArenaAllocator dfo_allocator_;
  common::ObArray<DfoInfo*> dfos_;
  DISALLOW_COPY_AND_ASSIGN(ObPxResourceAnalyzer);
};

template <class T>
int DfoTreeNormalizer<T>::normalize(T &root)
{
  int ret = OB_SUCCESS;
  int64_t non_leaf_cnt = 0;
  int64_t non_leaf_pos = -1;
  ARRAY_FOREACH_X(root.child_dfos_, idx, cnt, OB_SUCC(ret)) {
    T *dfo = root.child_dfos_.at(idx);
    if (0 < dfo->get_child_count()) {
      non_leaf_cnt++;
      if (-1 == non_leaf_pos) {
        non_leaf_pos = idx;
      }
    }
  }
  if (non_leaf_cnt > 1) {
    // sched as the tree shape in bushy tree scenario
  } else if (0 < non_leaf_pos) {
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
    T *inode = root.child_dfos_.at(non_leaf_pos);
    for (int64_t i = 1; i < non_leaf_pos; ++i) {
      root.child_dfos_.at(i - 1)->set_depend_sibling(root.child_dfos_.at(i));
    }
    inode->set_depend_sibling(root.child_dfos_.at(0));
    inode->set_has_depend_sibling(true);
    // (2) transform
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

}  // namespace sql
}  // namespace oceanbase

#endif
