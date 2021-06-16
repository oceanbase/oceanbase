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

  int64_t set_sched()
  {
    int64_t set = 0;
    if (DfoStatus::INIT == status_) {
      set = dop_;
      status_ = DfoStatus::SCHED;
    }
    return set;
  }
  int64_t set_parent_sched()
  {
    int64_t set = 0;
    if (has_parent()) {
      set = parent_->set_sched();
    }
    return set;
  }
  int64_t set_finish()
  {
    int64_t set = 0;
    if (DfoStatus::SCHED == status_) {
      set = dop_;
      status_ = DfoStatus::FINISH;
    }
    return set;
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

private:
  /* variables */
  common::ObArenaAllocator dfo_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPxResourceAnalyzer);
};

}  // namespace sql
}  // namespace oceanbase

#endif
