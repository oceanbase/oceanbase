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
 *
 * Multi-thread queue based on hash map.
 */

#ifndef OCEANBASE_MAP_QUEUE_H__
#define OCEANBASE_MAP_QUEUE_H__

#include "lib/atomic/ob_atomic.h"           // ATOMIC_*
#include "lib/hash/ob_linear_hash_map.h"    // ObLinearHashMap
#include "lib/oblog/ob_log_module.h"        // LIB_LOG
#include "lib/utility/ob_macro_utils.h"     // OB_FAIL

namespace oceanbase
{
namespace common
{

template <typename T>
class ObMapQueue
{
public:
  ObMapQueue() : inited_(false), map_(), head_(0), tail_(0), dummy_tail_(0) {}
  virtual ~ObMapQueue() { destroy(); }
  int init(const char *label);
  void destroy();
  bool is_inited() const { return inited_; }
  int64_t count() const { return map_.count(); }
public:
  /// A non-blocking push operation
  /// Should definitely succeed unless memory problems are encountered
  int push(const T &val);

  ///  non-blocking pop operation
  ///  error code：
  ///   - OB_EAGAIN: empty queue
  int pop(T &val);

   /// reset
   /// non-thread safe
  int reset();

private:
  // Key.
  struct Key
  {
    int64_t idx_;
    void reset(const int64_t idx) { idx_ = idx; }
    uint64_t hash() const { return static_cast<uint64_t >(idx_); }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return common::OB_SUCCESS;
    }
    bool operator==(const Key &other) const { return idx_ == other.idx_; }
  };
  // Pop contidion.
  class PopCond
  {
  public:
    explicit PopCond(T &val) : val_(val) {}
    ~PopCond() {}
    bool operator()(const Key &key, const T &val)
    {
      UNUSED(key);
      val_ = val;
      return true;
    }
  private:
    T &val_;
  };

private:
  bool inited_;
  // Map.
  common::ObLinearHashMap<Key, T> map_;
  // Sn.
  int64_t head_ CACHE_ALIGNED;
  int64_t tail_ CACHE_ALIGNED;
  int64_t dummy_tail_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMapQueue);
};

//////////////////////////////////////////////////////////////////////////////////

template <typename T>
int ObMapQueue<T>::init(const char *label)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LIB_LOG(ERROR, "init twice");
    ret = common::OB_INIT_TWICE;
  } else if (OB_FAIL(map_.init(label))) {
    LIB_LOG(ERROR, "init map fail", K(ret), K(label));
  } else {
    head_ = 0;
    tail_ = 0;
    dummy_tail_ = 0;
		inited_ = true;
  }

  return ret;
}

template <typename T>
void ObMapQueue<T>::destroy()
{
  inited_ = false;
  (void)map_.destroy();
  head_ = 0;
  tail_ = 0;
  dummy_tail_ = 0;
}

template <typename T>
int ObMapQueue<T>::push(const T &val)
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "not init");
    ret = common::OB_NOT_INIT;
  } else {
    // Get sn.
    int64_t sn = ATOMIC_LOAD(&dummy_tail_);
    while (!ATOMIC_BCAS(&dummy_tail_, sn, sn + 1)) {
      sn = ATOMIC_LOAD(&dummy_tail_);
    }

    // Save val.
    Key key;
    key.reset(sn);
    if (OB_FAIL(map_.insert(key, val))) {
      LIB_LOG(ERROR, "err insert map", K(ret), K(sn));
    }

    // Update tail.
    if (OB_SUCCESS == ret) {
      while (!ATOMIC_BCAS(&tail_, sn, sn + 1)) { PAUSE(); }
    }
  }

  return ret;
}

template <typename T>
int ObMapQueue<T>::pop(T &val)
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "not init");
    ret = common::OB_NOT_INIT;
  } else {
    int64_t head = 0;
    int64_t tail = 0;
    bool done = false;
    while (OB_SUCCESS == ret
        && !done
        && (head = ATOMIC_LOAD(&head_)) < (tail = ATOMIC_LOAD(&tail_))) {
      int64_t sn = head;
      if (ATOMIC_BCAS(&head_, sn, sn + 1)) {
        Key key;
        key.reset(sn);
        PopCond cond(val);
        if (OB_FAIL(map_.erase_if(key, cond))) {
          LIB_LOG(ERROR, "err erase map", K(ret), K(sn));
        }
        else {
          done = true;
        }
      }
    }
    // Empty queue.
    if (OB_SUCCESS == ret && (head == tail)) {
      ret = common::OB_EAGAIN;
    }
  }

  return ret;
}

template <typename T>
int ObMapQueue<T>::reset()
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "not init");
    ret = common::OB_NOT_INIT;
  } else {
    // non-thread safe
    head_ = 0;
    tail_ = 0;
    dummy_tail_ = 0;
    if (OB_FAIL(map_.reset())) {
      LIB_LOG(ERROR, "err reset map", K(ret));
    }
  }

  return ret;
}

}
}

#endif
