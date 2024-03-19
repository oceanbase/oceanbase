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

#ifndef OCEANBASE_SQL_MEM_LEAK_CHECKER_H_
#define OCEANBASE_SQL_MEM_LEAK_CHECKER_H_

#include "lib/lock/ob_mutex.h"
#include "lib/alloc/alloc_struct.h"
namespace oceanbase
{
namespace common
{

class ObSqlMemoryLeakChecker
{
public:
  ObSqlMemoryLeakChecker()
    : lock_(), dlist_()
  {}
  static ObSqlMemoryLeakChecker &get_instance();
  void add(lib::ObMemVersionNode &node);
  void remove(lib::ObMemVersionNode &node);
  uint32_t get_min_version();
  void update_check_range(const bool need_delay,
                          uint32_t &min_check_version,
                          uint32_t &max_check_version);
private:
  uint32_t min_check_version_;
  uint32_t max_check_version_;
  lib::ObMutex lock_;
  ObDList<lib::ObMemVersionNode> dlist_;
};

struct ObMemVersionNodeGuard
{
public:
  ObMemVersionNodeGuard()
  {
    lib::ObMemVersionNode::tl_node = &node_;
    ObSqlMemoryLeakChecker::get_instance().add(node_);
  }
  ~ObMemVersionNodeGuard()
  {
    lib::ObMemVersionNode::tl_node = NULL;
    ObSqlMemoryLeakChecker::get_instance().remove(node_);
  }
private:
  lib::ObMemVersionNode node_;
};

struct ObSqlMemoryLeakGuard
{
public:
  ObSqlMemoryLeakGuard(const bool enable_memory_leak)
    : enable_memory_leak_(enable_memory_leak && NULL != lib::ObMemVersionNode::tl_node),
      ignore_node_(lib::ObMemVersionNode::tl_ignore_node)
  {
    lib::ObMemVersionNode::tl_ignore_node = !enable_memory_leak_;
    if (enable_memory_leak) {
      version_ = lib::ObMemVersionNode::tl_node->version_;
      lib::ObMemVersionNode::tl_node->version_ = ATOMIC_LOAD(&lib::ObMemVersionNode::global_version);
    }
  }
  ~ObSqlMemoryLeakGuard()
  {
    lib::ObMemVersionNode::tl_ignore_node = ignore_node_;
    if (enable_memory_leak_) {
      lib::ObMemVersionNode::tl_node->version_ = version_;
    }
  }
private:
  bool enable_memory_leak_;
  bool ignore_node_;
  uint32_t version_;
};
#define DISABLE_SQL_MEMLEAK_GUARD ::oceanbase::common::ObSqlMemoryLeakGuard sql_memory_leak_guard(false);
#define ENABLE_SQL_MEMLEAK_GUARD ::oceanbase::common::ObSqlMemoryLeakGuard sql_memory_leak_guard(true);
} // end namespace common
} // end namespace oceanbase
#endif