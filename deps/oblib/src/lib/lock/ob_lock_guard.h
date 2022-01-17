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

#ifndef _OB_LOCK_GUARD_H_
#define _OB_LOCK_GUARD_H_

#include <new>
#include "lib/oblog/ob_log.h"

namespace oceanbase {
namespace lib {

template <typename LockT>
class ObLockGuard {
public:
  explicit ObLockGuard(LockT& lock);
  ~ObLockGuard();
  inline int get_ret() const
  {
    return ret_;
  }

private:
  // disallow copy
  ObLockGuard(const ObLockGuard& other);
  ObLockGuard& operator=(const ObLockGuard& other);
  // disallow new
  void* operator new(std::size_t size);
  void* operator new(std::size_t size, const std::nothrow_t& nothrow_constant) throw();
  void* operator new(std::size_t size, void* ptr) throw();

private:
  // data members
  LockT& lock_;
  int ret_;
};

template <typename LockT>
inline ObLockGuard<LockT>::ObLockGuard(LockT& lock) : lock_(lock), ret_(common::OB_SUCCESS)
{
  if (OB_UNLIKELY(common::OB_SUCCESS != (ret_ = lock_.lock()))) {
    COMMON_LOG(ERROR, "Fail to lock, ", K_(ret));
  }
}

template <typename LockT>
inline ObLockGuard<LockT>::~ObLockGuard()
{
  if (OB_LIKELY(common::OB_SUCCESS == ret_)) {
    if (OB_UNLIKELY(common::OB_SUCCESS != (ret_ = lock_.unlock()))) {
      COMMON_LOG(ERROR, "Fail to unlock, ", K_(ret));
    }
  }
}

}  // end of namespace lib
}  // end of namespace oceanbase

#endif /* _OB_LOCK_GUARD_H_ */
