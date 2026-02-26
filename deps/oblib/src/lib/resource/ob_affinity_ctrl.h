/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_AFFINITY_CTRL_H
#define OB_AFFINITY_CTRL_H

#include <sched.h>
#include <stdint.h>
#include <sys/types.h>
#include <linux/mempolicy.h>
#include "lib/ob_define.h"

#define AFFINITY_CTRL (oceanbase::lib::ObAffinityCtrl::get_instance())

namespace oceanbase
{
namespace lib
{

class ObAffinityCtrl
{
public:
  ObAffinityCtrl();
  ~ObAffinityCtrl() {}
  int init(const bool strict_check_os_params);

  int run_on_node(const int node);
  int thread_bind_to_node(const int node_hint);
  int memory_move_to_node(void *addr, const size_t len, const int node);
  int memory_bind_to_node(void *addr, const size_t len, const int node);
  int memory_set_interleave(void *addr, const size_t len);
  int get_num_nodes() { return inited_ ? num_nodes_ : 1; }
  static int &get_tls_node() {
    static thread_local int node_ = OB_NUMA_SHARED_INDEX;

    return node_;
  }

  int32_t get_numa_id()
  {
    int32_t numa_id = 0;
    if (inited_) {
      numa_id = get_tls_node();
      if (OB_NUMA_SHARED_INDEX == numa_id) {
        numa_id = GETTID() % num_nodes_;
      }
    }
    return numa_id;
  }

  // global single instance ObAffinityCtrl
  static ObAffinityCtrl &get_instance();

private:
  int num_nodes_;
  int inited_;

  struct ObNumaNode {
    cpu_set_t cpu_set_mask;
  };

  ObNumaNode nodes_[OB_MAX_NUMA_NUM];
};

class ObNumaNodeGuard
{
public:
  explicit ObNumaNodeGuard(int numa_node): prev_numa_node_(ObAffinityCtrl::get_tls_node())
  {
    ObAffinityCtrl::get_tls_node() = numa_node;
  }
  ~ObNumaNodeGuard()
  {
    ObAffinityCtrl::get_tls_node() = prev_numa_node_;
  }
private:
  int prev_numa_node_;
};

}  // lib
}  // oceanbase

#endif  // OB_AFFINITY_CTRL_H
