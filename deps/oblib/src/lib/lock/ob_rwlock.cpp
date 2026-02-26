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

#include "ob_rwlock.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/stat/ob_diagnose_info.h"

using namespace oceanbase;
using namespace obsys;

template <class T>
int ObRLock<T>::lock() const
{
  return rlock_->rdlock(latch_id_);
}

template <>
int ObRLock<pthread_rwlock_t>::lock() const
{
  oceanbase::common::ObWaitEventGuard guard(latch_id_);
  return pthread_rwlock_rdlock(rlock_);
}

template <class T>
int ObRLock<T>::trylock() const
{
  return rlock_->try_rdlock(latch_id_);
}

template<>
int ObRLock<pthread_rwlock_t>::trylock() const
{
  return pthread_rwlock_tryrdlock(rlock_);
}

template <class T>
int ObRLock<T>::unlock() const
{
  return rlock_->unlock();
}

template<>
int ObRLock<pthread_rwlock_t>::unlock() const
{
  return pthread_rwlock_unlock(rlock_);
}

template <class T>
void ObRLock<T>::set_latch_id(uint32_t latch_id)
{
  latch_id_ = latch_id;
}

template <class T>
int ObWLock<T>::lock() const
{
  return wlock_->wrlock(latch_id_);
}

template<>
int ObWLock<pthread_rwlock_t>::lock() const
{
  oceanbase::common::ObWaitEventGuard guard(latch_id_);
  return pthread_rwlock_wrlock(wlock_);
}

template <class T>
int ObWLock<T>::trylock() const
{
  return wlock_->try_wrlock(latch_id_);
}

template<>
int ObWLock<pthread_rwlock_t>::trylock() const
{
  return pthread_rwlock_trywrlock(wlock_);
}

template<class T>
int ObWLock<T>::unlock() const
{
  return wlock_->unlock();
}

template<>
int ObWLock<pthread_rwlock_t>::unlock() const
{
  return pthread_rwlock_unlock(wlock_);
}


template<class T>
void ObWLock<T>::set_latch_id(uint32_t latch_id)
{
  latch_id_ = latch_id;
}


template <LockMode lockMode>
void ObRWLock<lockMode>::set_latch_id(uint32_t latch_id)
{
  rlock_.set_latch_id(latch_id);
  wlock_.set_latch_id(latch_id);
}

template <class T>
inline ObLockGuardBase<T>::ObLockGuardBase(const T& lock, bool block) : lock_(lock)
{
    acquired_ = !(block ? lock_.lock() : lock_.trylock());
}

template<class T>
inline ObLockGuardBase<T>::~ObLockGuardBase()
{
  if (acquired_) {
    lock_.unlock();
  }
}

template class obsys::ObRWLock<NO_PRIORITY>;
template class obsys::ObRWLock<READ_PRIORITY>;
template class obsys::ObRLock<ObLatch>;
template class obsys::ObRLock<pthread_rwlock_t>;
template class obsys::ObWLock<ObLatch>;
template class obsys::ObWLock<pthread_rwlock_t>;

template class obsys::ObLockGuardBase<obsys::ObRLock<ObLatch>>;
template class obsys::ObLockGuardBase<obsys::ObRLock<pthread_rwlock_t>>;
template class obsys::ObLockGuardBase<obsys::ObWLock<ObLatch>>;
template class obsys::ObLockGuardBase<obsys::ObWLock<pthread_rwlock_t>>;
