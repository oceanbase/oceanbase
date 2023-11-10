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

#include "ob_sql_mem_leak_checker.h"

namespace oceanbase
{
using namespace lib;
namespace common
{
ObSqlMemoryLeakChecker &ObSqlMemoryLeakChecker::get_instance()
{
  static ObSqlMemoryLeakChecker instance;
  return instance;
}

void ObSqlMemoryLeakChecker::add(ObMemVersionNode &node)
{
  lib::ObMutexGuard guard(lock_);
  dlist_.add_first(&node);
}

void ObSqlMemoryLeakChecker::remove(ObMemVersionNode &node)
{
  lib::ObMutexGuard guard(lock_);
  dlist_.remove(&node);
}

uint32_t ObSqlMemoryLeakChecker::get_min_version()
{
  uint32_t version = UINT32_MAX;
  ObMutexGuard guard(lock_);
  DLIST_FOREACH_NORET(node, dlist_) {
    if (node->version_ < version) {
      version = node->version_;
    }
  }
  return version;
}

void ObSqlMemoryLeakChecker::update_check_range(const bool need_delay,
                                                uint32_t &min_check_version,
                                                uint32_t &max_check_version)
{
  uint32_t global_version = ATOMIC_AAF(&ObMemVersionNode::global_version, 1);
  min_check_version = min_check_version_;
  max_check_version = max_check_version_;
  max_check_version_ = std::min(get_min_version(), global_version);
  if (!need_delay) {
    max_check_version = max_check_version_;
  }
  min_check_version_ = max_check_version;
  COMMON_LOG(INFO, "update_check_range", K(min_check_version), K(max_check_version), K(global_version));
}

} // end namespace common
} // end namespace oceanbase