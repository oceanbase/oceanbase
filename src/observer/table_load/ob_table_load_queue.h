// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_utility.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace observer
{

template<class T>
class ObTableLoadQueue
{
public:
  ObTableLoadQueue();
  ~ObTableLoadQueue();
  int init(uint64_t tenant_id, uint64_t capacity);
  int push(T *obj, int64_t timeout_us = 0);
  int pop(T *&obj, int64_t timeout_us = 0);
  int push_nowait(T *obj);
  int pop_nowait(T *&obj);
  int64_t size() const;
private:
  T **queue_;
  uint64_t capacity_;
  uint64_t push_;
  uint64_t pop_;
  int64_t size_;
  mutable pthread_mutex_t mutex_;
  pthread_cond_t push_cond_;
  pthread_cond_t pop_cond_;
  bool is_inited_;
};

template <class T>
ObTableLoadQueue<T>::ObTableLoadQueue()
  : queue_(nullptr), capacity_(0), push_(0), pop_(0), size_(0), is_inited_(false)
{
  OB_ASSERT(0 == pthread_mutex_init(&mutex_, nullptr));
  OB_ASSERT(0 == pthread_cond_init(&push_cond_, nullptr));
  OB_ASSERT(0 == pthread_cond_init(&pop_cond_, nullptr));
}

template<class T>
ObTableLoadQueue<T>::~ObTableLoadQueue()
{
  if (nullptr != queue_) {
    common::ob_free(queue_);
    queue_ = nullptr;
  }
  OB_ASSERT(0 == pthread_mutex_destroy(&mutex_));
  OB_ASSERT(0 == pthread_cond_destroy(&push_cond_));
  OB_ASSERT(0 == pthread_cond_destroy(&pop_cond_));
}

template<class T>
int ObTableLoadQueue<T>::init(uint64_t tenant_id, uint64_t capacity)
{
  int ret = common::OB_SUCCESS;
  return ret;
}

template<class T>
int64_t ObTableLoadQueue<T>::size() const
{
  int64_t size = 0;
  pthread_mutex_lock(&mutex_);
  size = size_;
  pthread_mutex_unlock(&mutex_);
  return size;
}

template<class T>
int ObTableLoadQueue<T>::push(T *obj, int64_t timeout_us)
{
  int ret = common::OB_SUCCESS;
  return ret;
}

template<class T>
int ObTableLoadQueue<T>::pop(T *&obj, int64_t timeout_us)
{
  int ret = common::OB_SUCCESS;
  return ret;
}

template<class T>
int ObTableLoadQueue<T>::push_nowait(T *obj)
{
  int ret = common::OB_SUCCESS;
  return ret;
}

template<class T>
int ObTableLoadQueue<T>::pop_nowait(T *&obj)
{
  int ret = common::OB_SUCCESS;
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
