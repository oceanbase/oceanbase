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

#ifndef OCEANBASE_LIB_OB_QSYNC_
#define OCEANBASE_LIB_OB_QSYNC_

#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/cpu/ob_cpu_topology.h"

namespace oceanbase
{
namespace common
{

class ObQSyncImpl
{
public:
  struct Ref
  {
    Ref(): ref_(0) {}
    ~Ref() {}
    int64_t ref_ CACHE_ALIGNED;
  };
  ObQSyncImpl() : ref_array_(NULL), ref_num_(0) {}
  ObQSyncImpl(Ref* ref_array, const int64_t ref_num) : ref_array_(ref_array), ref_num_(ref_num) {}
  ~ObQSyncImpl() {}

  int64_t acquire_ref()
  {
    int64_t idx = get_id() % ref_num_;
    const int64_t new_ref = add_ref(idx, 1);
    if (OB_UNLIKELY(0 >= new_ref)) {
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "unexpected ref", K(new_ref), K(idx));
    }
    return idx;
  }
  void release_ref()
  {
    release_ref(get_id() % ref_num_);
  }
  void release_ref(int64_t idx)
  {
    const int64_t new_ref = add_ref(idx, -1);
    if (OB_UNLIKELY(0 > new_ref)) {
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "unexpected ref", K(new_ref), K(idx));
    }
  }
  void sync()
  {
    for(int64_t i = 0; i < ref_num_; i++) {
      Ref* ref = ref_array_ + i;
      if (NULL != ref) {
        while(ATOMIC_LOAD(&ref->ref_) != 0)
          ;
      }
    }
  }
  bool try_sync()
  {
    bool bool_ret = true;
    for(int64_t i = 0; i < ref_num_; i++) {
      Ref* ref = ref_array_ + i;
      if (NULL != ref) {
        if (ATOMIC_LOAD(&ref->ref_) != 0) {
          bool_ret = false;
          break;
        }
      }
    }
    return bool_ret;
  }

private:
  int64_t add_ref(int64_t idx, int64_t x)
  {
    int64_t ret = 0;
    if (idx < ref_num_) {
      Ref* ref = ref_array_ + idx;
      if (NULL != ref) {
        ret = ATOMIC_AAF(&ref->ref_, x);
      }
    }
    return ret;
  }
  int64_t get_id() const
  {
    return get_itid();
  }
protected:
  Ref* ref_array_;
  int64_t ref_num_;
};

class ObQSync : public ObQSyncImpl
{
public:
  enum { MAX_REF_CNT = 256 };
  ObQSync() : ObQSyncImpl(local_ref_array_, MAX_REF_CNT) {}
  virtual ~ObQSync() {}
private:
  Ref local_ref_array_[MAX_REF_CNT];
};

struct QSyncCriticalGuard
{
  explicit QSyncCriticalGuard(ObQSync& qsync): qsync_(qsync), ref_(qsync.acquire_ref()) {}
  ~QSyncCriticalGuard() { qsync_.release_ref(ref_); }
  ObQSync& qsync_;
  int64_t ref_;
};

#define CriticalGuard(qs) oceanbase::common::QSyncCriticalGuard critical_guard((qs))
#define WaitQuiescent(qs) (qs).sync()

class ObDynamicQSync : public ObQSyncImpl
{
public:
  enum { MAX_REF_CNT = 48 };
  ObDynamicQSync(): is_inited_(false) {}
  virtual ~ObDynamicQSync() { destroy(); }
  bool is_inited() const { return is_inited_; }

  int init(const lib::ObMemAttr& mem_attr)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "ObDynamicQSync init twice, ", K(ret));
    } else {
      int64_t cpu_count = get_cpu_count();
      if (cpu_count > MAX_REF_CNT || cpu_count <= 0) {
        ref_num_ = MAX_REF_CNT;
      } else {
        ref_num_ = cpu_count;
      }
      if (OB_ISNULL(ref_array_ = (Ref*)ob_malloc(sizeof(Ref) * ref_num_, mem_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(ERROR, "Fail to allocate Ref memory, ", K(ret), LITERAL_K(ref_num_));
      } else {
        for (int64_t i = 0 ; i < ref_num_; ++i) {
          new(ref_array_ + i) Ref();
        }
        is_inited_ = true;
      }
    }
    return ret;
  }

  void destroy()
  {
    is_inited_ = false;
    if (OB_NOT_NULL(ref_array_)) {
      for (int64_t i = 0 ; i < ref_num_; ++i) {
        ref_array_[i].~Ref();
      }
      ob_free(ref_array_);
      ref_num_ = 0;
      ref_array_ = NULL;
    }
  }
private:
  bool is_inited_;
};

struct DynamicQSyncCriticalGuard
{
  explicit DynamicQSyncCriticalGuard(ObDynamicQSync& qsync): qsync_(qsync), ref_(qsync.acquire_ref()) {}
  ~DynamicQSyncCriticalGuard() { qsync_.release_ref(ref_); }
  ObDynamicQSync& qsync_;
  int64_t ref_;
};

#define CriticalGuardDynamic(qs) oceanbase::common::DynamicQSyncCriticalGuard critical_guard((qs))
#define WaitQuiescentDynamic(qs) (qs).sync()

}; // end namespace common
}; // end namespace oceanbase

#endif
