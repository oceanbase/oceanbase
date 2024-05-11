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

#ifndef OB_STAT_TEMPLATE_H_
#define OB_STAT_TEMPLATE_H_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{

class DIRWLock
{
public:
  DIRWLock();
  ~DIRWLock();
  int try_rdlock();
  int try_wrlock();
  void rdlock();
  void wrlock();
  void wr2rdlock();
  void unlock();
  inline uint32_t get_lock() const {return lock_;}
private:
  static const uint32_t WRITE_MASK = 1<<30;
  volatile uint32_t lock_;
#ifndef NDEBUG
  int64_t wlock_tid_;  // record tid for thread that holds the lock.
#endif
};
/**
 * ----------------------------------------------------template define---------------------------------------------------------
 */

//NOTE the class T must be a POD type
template<class T, int64_t N>
class ObStatArray
{
public:
  ObStatArray();
  int add(const ObStatArray &other);
  T *get(const int64_t idx);
  void reset();
private:
  T items_[N];
};

/**
 * ----------------------------------------------------template implementation---------------------------------------------------------
 */
//ObStatArray
template<class T, int64_t N>
ObStatArray<T, N>::ObStatArray()
{
  memset(items_, 0, sizeof(items_));
}

template<class T, int64_t N>
int ObStatArray<T, N>::add(const ObStatArray<T, N> &other)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; i < N && common::OB_SUCCESS == ret; ++i) {
    if (OB_FAIL(items_[i].add(other.items_[i]))) {
      COMMON_LOG(WARN, "Fail to add other, ", K(ret));
    }
  }
  return ret;
}

template<class T, int64_t N>
T *ObStatArray<T, N>::get(const int64_t idx)
{
  T *item = NULL;
  if (idx >= 0 && idx < N) {
    item = &items_[idx];
  }
  return item;
}

template<class T, int64_t N>
void ObStatArray<T, N>::reset()
{
  memset(items_, 0, sizeof(items_));
}

}//common
}//oceanbase

#endif /* OB_STAT_TEMPLATE_H_ */
