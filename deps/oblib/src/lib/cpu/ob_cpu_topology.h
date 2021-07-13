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

#ifndef OCEANBASE_LIB_OB_CPU_TOPOLOGY_
#define OCEANBASE_LIB_OB_CPU_TOPOLOGY_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"

namespace oceanbase {
namespace common {
class ObCpuTopology {
public:
  static const int64_t MAX_CORE_NUMBER_PER_NODE = 1024;
  static const int64_t MAX_NODE_NUMBER = 64;
  static const int64_t MAX_CORE_NUMBER = MAX_CORE_NUMBER_PER_NODE * MAX_NODE_NUMBER;
  struct CoreInfo {
    int64_t core_number_;
    int64_t cores_[MAX_CORE_NUMBER_PER_NODE];
  };

public:
  ObCpuTopology();

public:
  int init();
  int64_t get_core_num();
  int64_t get_node_num();
  CoreInfo* get_cores_by_node(int64_t node_id);
  int64_t get_thread_node_id();
  int64_t get_thread_core_id();
  void bind_cpu(uint64_t core_id);

private:
  struct ThreadLocalInfo {
    int64_t core_id_;
    int64_t node_id_;
    int64_t valid_;
  };
  ThreadLocalInfo& get_tl_info();
  DISALLOW_COPY_AND_ASSIGN(ObCpuTopology);

private:
  typedef CoreInfo NodeArray[MAX_NODE_NUMBER];
  typedef int64_t CoreNodeMapArray[MAX_CORE_NUMBER];

  int64_t core_number_;
  int64_t node_number_;
  NodeArray nodes_;
  CoreNodeMapArray cores_map_;
  bool init_;

};  // ObCpuTopology

int64_t get_cpu_count();
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_LIB_OB_CPU_TOPOLOGY_
