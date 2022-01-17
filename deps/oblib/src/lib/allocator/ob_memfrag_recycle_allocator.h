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

#ifndef OB_MEMFRAG_RECYCLE_ALLOCATOR_H_
#define OB_MEMFRAG_RECYCLE_ALLOCATOR_H_

#include "lib/allocator/ob_delay_free_allocator.h"
#include "lib/task/ob_timer.h"
#include "lib/utility/utility.h"

namespace oceanbase {
namespace common {

// The interface of rewritable classes, which provides rewrite_switch methods to rewrite itself to
// new memory.
class ObIRewritable {
public:
  ObIRewritable();

  virtual ~ObIRewritable();

  // In this function, the ObIRewritable need to rewrite itself. Invoke the alloc function of
  // ObMemfragRecycleAllocator to copy all dynamic memory in itself to new memory. The
  // old memory will be freed after safe expire duration time.
  // This function does NOT need to be invoked by upper levels. There will be a timer
  // which will invoke it periodically.
  //@return OB_SUCCESS or other error code
  virtual int rewrite_switch() = 0;
};

// An allocator which is used for allocating memory of meta data, which provides memory allocate
// and free methods.  It can be thread safe or not, which depends on the initialize parameters.
// If the allocator is NOT thread safe after initialization, then the behavior of this allocator will be
// similar with ObFIFOAllocator. It should be used as a local variable. The memory will not be freed
// until destroy or destructor function be invoked. Make sure do not access it by multi threads.
// If the allocator is thread safe after initialization, it will provide a memory fragment clean routine.
// There are two sub allocators in it. Initially, an allocator
// is used for allocating memory and the other is only standby. A timer will periodically check
// the state of it. If there are too much memory fragments in the working allocator, it will switch
// the role of working allocator and standby allocator, then callback the rewrite_switch function of
// registered rewritable object to rewrite itself to current working allocator, at last free memory of
// standby allocator after expire duration time.
class ObMemfragRecycleAllocator : public ObIAllocator {
public:
  ObMemfragRecycleAllocator();
  virtual ~ObMemfragRecycleAllocator();

  // NOT thread safe.
  //@param label: the memory module id of this allocator
  //@param rewritable: NULL != rewritable provides thread safe allocator
  //                                        NULL == rewritable provides thread unsafe allocator
  //@param expire_duation_us: the memory will be freed delayed by at least expire_duration_us time
  //@param memory_rewrite_threshold: it will callback the rewrite function if used memory exceed
  //       memory_rewrite_threshold
  int init(const lib::ObLabel& label, ObIRewritable* rewritable, const int64_t expire_duration_us,
      const int64_t memory_rewrite_threshold = DEFAULT_TOTAL_MEMORY_REWRITE_THRESHOLD);

  int set_recycle_param(const int64_t expire_duration_us, const int64_t memory_rewrite_threshold);

  // NOT thread safe.
  void destroy();

  // Thread safe depends on init parameters.
  // If rewritable is set when init, make sure the allocated memory can be rewrote by it.
  // Allocate memory. If failed, it will return NULL.
  virtual void* alloc(const int64_t size);
  virtual void* alloc(int64_t size, const ObMemAttr& attr)
  {
    UNUSED(attr);
    return alloc(size);
  }

  // Thread safe depends on init parameters.
  // The memory will NOT be freed immediately, it will be freed after safe expire duration (10 minutes
  // or longer), which ensures there is not any reference to this memory.
  // It's better to invoke this method if the memory has been useless, then the memory fragment
  // recycle routine will estimate the size of memory fragment and recycle the memory timely.
  // If you forget invoke this method, it is also OK. The memory fragment recycle routine will force the
  // rewritable interface to rewrite itself if the total memory cost is big enough.
  virtual void free(void* ptr);

  // Thread safe.
  // Should NOT be invoked if this allocator is NOT thread safe.
  // Sometimes there is some memory that does not need to be rewritten. Check whether
  // the memory need to be rewritten.
  //@param ptr
  //@param is_need out param, is true if need rewrite
  //@return OB_SUCCESS or other ERROR CODE
  int need_rewrite(const void* ptr, bool& is_need);

  virtual void set_label(const lib::ObLabel& label);

  // Switch the inner state. Do NOT invoked by upper levels.
  int switch_state();

  // used for stats of memory usage for allocator
  int64_t get_all_alloc_size() const;
  int64_t get_cur_alloc_size() const;

private:
  enum ObMemfragRecycleAllocatorState { INIT, SWITCHING_PREPARE, SWITCHING, SWITCHING_OVER };

  static const int64_t META_MAGIC_NUM = 0x4D455441;
#if 0  // for dbg
  static const int64_t DEFAULT_EXPIRE_DURATION_US = 1000L * 1000L * 6L;
  static const int64_t DEFAULT_TOTAL_MEMORY_REWRITE_THRESHOLD = 4L * 1024L * 1024L;
#else
  static const int64_t DEFAULT_EXPIRE_DURATION_US = 1000L * 1000L * 600L;
  static const int64_t DEFAULT_TOTAL_MEMORY_REWRITE_THRESHOLD = 512L * 1024L * 1024L;
#endif

  static const int64_t EXPIRE_DURATION_FACTOR = 5;
  static const int64_t MAX_TOTAL_MEMORY_REWRITE_THRESHOLD = 5 * DEFAULT_TOTAL_MEMORY_REWRITE_THRESHOLD;
  static constexpr double TOTAL_MEMORY_REWRITE_THRESHOLD_FACTOR = 1.1;
  static const int64_t FRAGMENT_MEMORY_REWRITE_THRESHOLD = 32L * 1024L * 1024L;

  bool need_switch(ObDelayFreeAllocator& allocator);

private:
  ObDelayFreeAllocator allocator_[2];
  int64_t cur_allocator_pos_;
  enum ObMemfragRecycleAllocatorState state_;
  ObIRewritable* rewritable_;
  int64_t state_switch_time_;
  int64_t expire_duration_us_;
  int64_t total_memory_rewrite_threshold_;
  lib::ObMutex mutex_;
  bool inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMemfragRecycleAllocator);
};

// A singleton which is used for recycling memory of all ObMemfragRecycleAllocator periodically.
class ObMRAllocatorRecycler {
public:
  static ObMRAllocatorRecycler& get_instance();
  int register_allocator(ObMemfragRecycleAllocator* allocator);
  int deregister_allocator(ObMemfragRecycleAllocator* allocator);
  void recycle();
  void set_schedule_interval_us(const int64_t interval_us);

private:
  class SelfCreator {
  public:
    SelfCreator();
    virtual ~SelfCreator();
  };
  class ObMRAllocatorRecycleTask : public ObTimerTask {
  public:
    ObMRAllocatorRecycleTask();
    virtual ~ObMRAllocatorRecycleTask();
    void runTimerTask();
  };

private:
  ObMRAllocatorRecycler();
  virtual ~ObMRAllocatorRecycler();
  static const int64_t MAX_META_MEMORY_ALLOCATOR_COUNT = 8;
  static const int64_t TIMER_SCHEDULE_INTERVAL_US = 1000L * 1000L * 600L;
  static SelfCreator self_creator_;
  lib::ObMutex mutex_;
  ObMRAllocatorRecycleTask task_;
  ObTimer timer_;
  ObMemfragRecycleAllocator* allocators_[MAX_META_MEMORY_ALLOCATOR_COUNT];
  bool is_scheduled_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMRAllocatorRecycler);
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_MEMFRAG_RECYCLE_ALLOCATOR_H_ */
