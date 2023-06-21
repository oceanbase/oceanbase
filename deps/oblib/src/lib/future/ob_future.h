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

/**
 * ObFuture/ObPromise
 *
 * ObPromise is a promise of "the result(type is specified by template arg T) will be written and
 * only be written once".
 * ObFuture is a read-only handle of ObPromise, once result is written, user could read the result
 * from ObFuture any number of times, before that, all read operation will hung until the written
 * done. User could use ObFuture check the result is written or not at any time.
 *
 *
 * When to use:
 *   - you want to synchronize between threads, usually could be described that thread A waiting for
 *     a operation which will be done by thread B in the future.
 *
 * When not to use:
 *   - you are crazy about performance, value copy of result is not accepted and condition variable
 *     is too heavy to you.
 *
 * Memory usage:
 *   - ObFuture/ObPromise has constant size: 16 Bytes.
 *   - the result is stored in heap, wrapped in ObSharedGuard.
 *   - the memory allocator can be specified, if not, the default memory allocation strategy just s-
 *     imply using ob_malloc.
 *
 * Manual:
 *   - Interface
 * ObPromise
 *     1. int init(ObIAllocator &alloc)
 *        will allock memory and construct inner obj using alloc.
 *        if alloc is not specified, using ob_malloc by default.
 *        err:
 *        - OB_INIT_TWICE : call init more than one time.
 *        - OB_ALLOCATE_MEMORY_FAILED : alloc memory failed.
 *     2. int set(T &&data)
 *        set the result by data, will call data's copy/move construction method.
 *        this method could only be called successed once.
 *        err:
 *        - OB_OP_NOT_ALLOW : set data more than one time.
 *        - OTHERS : forward from ObThreadCond
 *     3. ObFuture<T> get_future() const
 *        return a future object.
 * ObFuture
 *     1. int get(T *&ptr) const
 *        get the promised result, will hung if result is not ready.
 *        if OB_SUCCESS returned, ptr will pointer to the result, notice that ObFuture is also a
 *        life time guard of the result, is you do not hold the ObFuture object, do not use ptr get
 *        from this method, althrough it was returned OB_SUCCESS.
 *        err:
 *        - OTHERS : forward from ObThreadCond
 *     2. int wait() const
 *        same as get() method, but not tell the result, just wait it finished.
 *        err:
 *        - OTHERS : forward from ObThreadCond
 *     3. int wait_for(int64_t time_span_us) const
 *        Similar with wait() method, but hung for at most time_span_us us time.
 *        err:
 *        - OB_TIMEOUT : not finish in time_span_us us time.
 *        - OB_NOT_RUNNING : ObProimise won't write result anymore.
 *        - OTHERS : forward from ObThreadCond
 *     4. int is_valid() const
 *        check if this ObFuture object is inited.
 *     5. bool is_ready() const
 *        check if the result is ready.
 *
 *  - CAUTION:
 *      + DO check is_valid() before using ObPromise/ObFuture, or may CRASH.
 *      + MAKE SURE you visit the result with holding a ObFuture object, ObFuture is a life time
 *        guard of the result.
 *
 *  - Contact  for help.
 */
#ifndef OCEANBASE_LIB_FUTURE_OB_FUTURE_H
#define OCEANBASE_LIB_FUTURE_OB_FUTURE_H

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/guard/ob_shared_guard.h"

#define CHECK_INITED() \
do {\
  if (OB_UNLIKELY(!is_valid())) {\
    OCCAM_LOG_RET(WARN, common::OB_NOT_INIT, "not init", K(lbt()));\
    return OB_NOT_INIT;\
  }\
} while (0)

namespace oceanbase
{
namespace common
{
namespace future
{

struct DefaultFutureAllocator : public ObIAllocator {
  void *alloc(const int64_t size) override {
#ifdef UNITTEST_DEBUG
    total_alive_num++;
#endif
    return ob_malloc(size, SET_USE_500("ObFuture"));
  }
  void* alloc(const int64_t size, const ObMemAttr &attr) override {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override {
#ifdef UNITTEST_DEBUG
    total_alive_num--;
#endif
    ob_free(ptr);
  }
#ifdef UNITTEST_DEBUG
  int total_alive_num = 0;
#endif
  static DefaultFutureAllocator &get_default_allocator() {
    static DefaultFutureAllocator default_allocator;
    return default_allocator;
  }
};

class ObFutureBaseBase
{
protected:
  struct FutureBlockBase
  {
    bool ready_;
    bool stop_;
    ObThreadCond cv_;
  };
protected:
  ObFutureBaseBase() : p_future_base_(nullptr) {};
  ObFutureBaseBase(FutureBlockBase *p_future_base) : p_future_base_(p_future_base) {};
  int init(FutureBlockBase *p_future_base)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(p_future_base)) {
      ret = OB_INVALID_ARGUMENT;
      OCCAM_LOG(WARN, "invalid argument", K(ret), K(lbt()));
    } else {
      p_future_base_ = p_future_base;
      p_future_base_->ready_ = false;
      p_future_base_->stop_ = false;
      new(&(p_future_base_->cv_)) ObThreadCond();
      if (OB_FAIL(p_future_base_->cv_.init(ObWaitEventIds::DYNAMIC_THREAD_POOL_COND_WAIT))) {
        OCCAM_LOG(WARN, "condition var init failed", K(ret));
      }
    }
    return ret;
  }
  int wait() const
  {
    int ret = OB_SUCCESS;
    if (!OB_ISNULL(p_future_base_)) {
      {
        ObThreadCondGuard guard(p_future_base_->cv_);
        while(!p_future_base_->ready_ && !p_future_base_->stop_) {// surpurise awaken
          p_future_base_->cv_.wait();
        }
        if (p_future_base_->stop_) {
          ret = OB_NOT_RUNNING;
        } else if (!p_future_base_->ready_) {
          ret = OB_TIMEOUT;
        }
      }
    } else {
      ret = OB_NOT_INIT;
      OCCAM_LOG(WARN, "p_future_base_ not init yet", K(ret), K(lbt()));
    }
    return ret;
  }
  int wait_for(const int64_t time_span_ms) const
  {
    int ret = OB_SUCCESS;
    if (!OB_ISNULL(p_future_base_)) {
      {
        ObThreadCondGuard guard(p_future_base_->cv_);
        if (!p_future_base_->ready_ && !p_future_base_->stop_) {
          p_future_base_->cv_.wait(time_span_ms);
        }
        if (p_future_base_->stop_) {
          ret = OB_NOT_RUNNING;
        } else if (!p_future_base_->ready_) {
          ret = OB_TIMEOUT;
        } else {
          OCCAM_LOG(WARN, "data is ready");
        }
      }
    } else {
      ret = OB_NOT_INIT;
      OCCAM_LOG(WARN, "p_future_base_ not init yet", K(ret), K(lbt()));
    }
    return ret;
  }
  bool is_valid() const
  {
    bool ret = false;
    if (OB_ISNULL(p_future_base_)) {
      ret = false;
    } else {
      ObThreadCondGuard guard(p_future_base_->cv_);
      ret = !p_future_base_->stop_;
    }
    return ret;
  }
  bool is_ready() const
  {
    bool ret = false;
    if (!OB_ISNULL(p_future_base_)) {
      ObThreadCondGuard guard(p_future_base_->cv_);
      ret = p_future_base_->ready_;
    } else {
      OCCAM_LOG(WARN, "p_future_base_ not init yet", K(lbt()));
    }
    return ret;
  }
public:
  void stop_and_notify_all()
  {
    if (OB_NOT_NULL(future::ObFutureBaseBase::p_future_base_)) {
      {
        ObThreadCondGuard guard(future::ObFutureBaseBase::p_future_base_->cv_);
        future::ObFutureBaseBase::p_future_base_->stop_ = true;
      }
      future::ObFutureBaseBase::p_future_base_->cv_.broadcast();
    }
  }
  FutureBlockBase *p_future_base_;
};

template <typename T>
class ObFutureBase : public ObFutureBaseBase
{
protected:
  struct FutureBlock
  {
    T data_;
    ObFutureBaseBase::FutureBlockBase future_base_block_;
  };
protected:
  ObFutureBase() = default;
  ObFutureBase(ObSharedGuard<FutureBlock> shared_ptr) :
    ObFutureBaseBase(shared_ptr.is_valid() ? &(shared_ptr->future_base_block_) : nullptr),
    data_shared_ptr_(shared_ptr) {}
  int init(ObIAllocator &);
  template <typename T2>
  int set(T2 &&data)
  {
    CHECK_INITED();
    int ret = OB_SUCCESS;
    bool need_notify = false;
    {
      ObThreadCondGuard guard(p_future_base_->cv_);
      if (p_future_base_->ready_) {
        ret = OB_OP_NOT_ALLOW;
        OCCAM_LOG(WARN, "data in promise should only be written once", K(ret), K(lbt()));
      } else {
        new(&(data_shared_ptr_->data_)) T(std::move(data));
        p_future_base_->ready_ = true;
        need_notify = true;
      }
    }
    if (need_notify) {
      p_future_base_->cv_.broadcast();
    }
    return ret;
  }
  int get(T *&ptr) const
  {
    CHECK_INITED();
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObFutureBaseBase::wait())) {
      OCCAM_LOG(WARN, "something wrong happend while waiting", K(ret));
    } else {
      ptr = &(data_shared_ptr_->data_);
    }
    return ret;
  }
  bool is_valid() const { return data_shared_ptr_.is_valid() && ObFutureBaseBase::is_valid(); }
protected:
  ObSharedGuard<FutureBlock> data_shared_ptr_;
};

template <typename T>
int ObFutureBase<T>::init(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObSharedGuard<FutureBlock> temp_shared_ptr;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    OCCAM_LOG(WARN, "init twice", K(ret), K(lbt()));
  } else if (OB_FAIL(ob_alloc_shared<FutureBlock>(temp_shared_ptr, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OCCAM_LOG(WARN, "alloc shared failed", K(ret));
  } else if (OB_FAIL(ObFutureBaseBase::init(&(temp_shared_ptr->future_base_block_)))) {
    // do nothing, temp_shared_ptr is the last guard who hold the ref of FutureBlock
    // allocated memory will auto free when this method return through RAII
    OCCAM_LOG(WARN, "init ObFutureBaseBase failed", K(ret));
  } else {
    data_shared_ptr_ = temp_shared_ptr;
  }
  return ret;
}

}// namespace future

#define DEFAULT_ALLOCATOR future::DefaultFutureAllocator::get_default_allocator()

template <typename T>
class ObFuture : public future::ObFutureBase<T>
{
public:
  ObFuture() = default;
  ObFuture(ObSharedGuard<typename future::ObFutureBase<T>::FutureBlock> shared_data) :
    future::ObFutureBase<T>(shared_data) {}
  int get(T *&ptr) const { return future::ObFutureBase<T>::get(ptr); }
  int wait() const { return future::ObFutureBase<T>::wait(); }
  int wait_for(int64_t time_span_ms) const { return future::ObFutureBase<T>::wait_for(time_span_ms); }
  bool is_valid() const { return future::ObFutureBase<T>::is_valid(); }
  bool is_ready() const { return future::ObFutureBase<T>::is_ready(); }
};

template <typename T>
class ObPromise : public future::ObFutureBase<T>
{
public:
  int init(ObIAllocator &alloc = DEFAULT_ALLOCATOR) { return future::ObFutureBase<T>::init(alloc); }
  template <typename T2>
  int set(T2 &&data) { return future::ObFutureBase<T>::set(std::move(data)); }
  ObFuture<T> get_future() const { return ObFuture<T>(future::ObFutureBase<T>::data_shared_ptr_); }
  bool is_valid() const { return future::ObFutureBase<T>::is_valid(); }
};

/****************************[ specialization for void type ]****************************/

namespace future
{
template <>
class ObFutureBase<void> : public ObFutureBaseBase
{
protected:
  struct FutureBlock
  {
    ObFutureBaseBase::FutureBlockBase future_base_block_;
  };
protected:
  ObFutureBase() = default;
  ObFutureBase(ObSharedGuard<FutureBlock> shared_ptr) :
    ObFutureBaseBase(shared_ptr.is_valid() ? &(shared_ptr->future_base_block_) : nullptr),
    data_shared_ptr_(shared_ptr) {}
    int init(ObIAllocator &alloc) {
        int ret = OB_SUCCESS;
    ObSharedGuard<FutureBlock> temp_shared_ptr;
    if (OB_UNLIKELY(is_valid())) {
      ret = OB_INIT_TWICE;
      OCCAM_LOG(WARN, "init twice", K(ret), K(lbt()));
    } else if (OB_FAIL(ob_alloc_shared<FutureBlock>(temp_shared_ptr, alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OCCAM_LOG(WARN, "alloc shared failed", K(ret));
    } else if (OB_FAIL(ObFutureBaseBase::init(&(temp_shared_ptr->future_base_block_)))) {
      // do nothing, temp_shared_ptr is the last guard who hold the ref of FutureBlock
      // allocated memory will auto free when this method return through RAII
      OCCAM_LOG(WARN, "init ObFutureBaseBase failed", K(ret));
    } else {
      data_shared_ptr_ = temp_shared_ptr;
    }
    return ret;
  }
  int set() {
    CHECK_INITED();
    int ret = OB_SUCCESS;
    bool need_notify = false;
    {
      ObThreadCondGuard guard(p_future_base_->cv_);
      if (p_future_base_->ready_) {
        ret = OB_OP_NOT_ALLOW;
        OCCAM_LOG(WARN, "data in promise should only be written once", K(ret), K(lbt()));
      } else {
        p_future_base_->ready_ = true;
        need_notify = true;
      }
    }
    if (need_notify) {
      p_future_base_->cv_.broadcast();
    }
    return ret;
  }
  int get() const {
    CHECK_INITED();
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObFutureBaseBase::wait())) {
      OCCAM_LOG(WARN, "something wrong happend while waiting", K(ret));
    }
    return ret;
  }
  bool is_valid() const { return data_shared_ptr_.is_valid() && ObFutureBaseBase::is_valid(); }
public:
  ObSharedGuard<FutureBlock> data_shared_ptr_;
};

}// namespace future

template <>
class ObFuture<void> : public future::ObFutureBase<void>
{
public:
  ObFuture() = default;
  ObFuture(ObSharedGuard<typename future::ObFutureBase<void>::FutureBlock> shared_data) :
    future::ObFutureBase<void>(shared_data) {}
  int get() const { return future::ObFutureBase<void>::get(); }
  int wait() const { return future::ObFutureBase<void>::wait(); }
  int wait_for(int64_t time_span_ms) const { return future::ObFutureBase<void>::wait_for(time_span_ms); }
  bool is_valid() const { return future::ObFutureBase<void>::is_valid(); }
  bool is_ready() const { return future::ObFutureBase<void>::is_ready(); }
};

template <>
class ObPromise<void> : public future::ObFutureBase<void>
{
public:
  int init(ObIAllocator &alloc = DEFAULT_ALLOCATOR) { return future::ObFutureBase<void>::init(alloc); }
  int set() { return future::ObFutureBase<void>::set(); }
  ObFuture<void> get_future() const { return ObFuture<void>(future::ObFutureBase<void>::data_shared_ptr_); }
  bool is_valid() const { return future::ObFutureBase<void>::is_valid(); }
};

}// namespace oceanbase
}// namespace common
#undef CHECK_INITED
#undef DEFAULT_ALLOCATOR
#endif
