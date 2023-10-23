/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_RUNTIME_UTILITY_MDS_LOCK_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_RUNTIME_UTILITY_MDS_LOCK_H
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_clock_generator.h"
#include "lib/literals/ob_literals.h"
#include "lib/utility/utility.h"
#include "common_define.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

using MdsLock = common::SpinRWLock;

struct MdsRLockGuard {// RAII
  MdsRLockGuard() : p_guard_(nullptr) {}
  MdsRLockGuard(const MdsLock &lock) : p_guard_(reinterpret_cast<common::SpinRLockGuard *>(guard_buffer_)) { new (p_guard_) common::SpinRLockGuard(lock); }
  MdsRLockGuard(const MdsRLockGuard&) = delete;
  MdsRLockGuard &operator=(const MdsRLockGuard&) = delete;
  ~MdsRLockGuard() { if (OB_NOT_NULL(p_guard_)) { p_guard_->~SpinRLockGuard(); } }
  char guard_buffer_[sizeof(common::SpinRLockGuard)];
  common::SpinRLockGuard *p_guard_;
};

struct MdsWLockGuard {// RAII
  MdsWLockGuard() : p_guard_(nullptr) {}
  MdsWLockGuard(const MdsLock &lock) : p_guard_(reinterpret_cast<common::SpinWLockGuard *>(guard_buffer_)) { new (p_guard_) common::SpinWLockGuard(lock); }
  MdsWLockGuard(const MdsWLockGuard&) = delete;
  MdsWLockGuard &operator=(const MdsWLockGuard&) = delete;
  ~MdsWLockGuard() { if (OB_NOT_NULL(p_guard_)) { p_guard_->~SpinWLockGuard(); } }
  int64_t to_string(char *buf, const int64_t buf_len) const { return 0; }// to put it in ObSEArray
  char guard_buffer_[sizeof(common::SpinRLockGuard)];
  common::SpinWLockGuard *p_guard_;
};

}
}
}

#endif
