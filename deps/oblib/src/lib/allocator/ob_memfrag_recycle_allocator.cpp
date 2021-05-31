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

#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_memfrag_recycle_allocator.h"

namespace oceanbase {
namespace common {

ObIRewritable::ObIRewritable()
{}

ObIRewritable::~ObIRewritable()
{}

ObMemfragRecycleAllocator::ObMemfragRecycleAllocator()
    : cur_allocator_pos_(0),
      state_(INIT),
      rewritable_(NULL),
      state_switch_time_(0),
      expire_duration_us_(DEFAULT_EXPIRE_DURATION_US),
      total_memory_rewrite_threshold_(DEFAULT_TOTAL_MEMORY_REWRITE_THRESHOLD),
      inited_(false)
{}

ObMemfragRecycleAllocator::~ObMemfragRecycleAllocator()
{
  destroy();
}

int ObMemfragRecycleAllocator::init(const lib::ObLabel& label, ObIRewritable* rewritable,
    const int64_t expire_duration_us, const int64_t memory_rewrite_threshold)
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
    LIB_ALLOC_LOG(WARN, "This ObMemfragRecycleAllocator has been inited.");
  } else if (label != nullptr || expire_duration_us < 0 || memory_rewrite_threshold <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_ALLOC_LOG(
        WARN, "Invalid arguments, ", K(label), K(rewritable), K(expire_duration_us), K(memory_rewrite_threshold));
  } else {
    if (NULL == rewritable) {
      // NOT thread safe allocator
      if (OB_FAIL(allocator_[0].init(label, false, expire_duration_us))) {
        LIB_ALLOC_LOG(WARN, "fail to init the allocator0, ", "ret", ret);
      } else if (OB_FAIL(allocator_[1].init(label, false, expire_duration_us))) {
        LIB_ALLOC_LOG(WARN, "fail to init the allocator1, ", "ret", ret);
      } else {
        rewritable_ = NULL;
        expire_duration_us_ = expire_duration_us;
        inited_ = true;
      }
    } else {
      // Thread safe allocator
      if (OB_FAIL(allocator_[0].init(label, true, expire_duration_us))) {
        LIB_ALLOC_LOG(WARN, "fail to init the allocator0, ", "ret", ret);
      } else if (OB_FAIL(allocator_[1].init(label, true, expire_duration_us))) {
        LIB_ALLOC_LOG(WARN, "fail to init the allocator1, ", "ret", ret);
      } else if (OB_FAIL(ObMRAllocatorRecycler::get_instance().register_allocator(this))) {
        LIB_ALLOC_LOG(WARN, "fail to register memory allocator, ", "ret", ret);
      } else {
        rewritable_ = rewritable;
        expire_duration_us_ = expire_duration_us;
        total_memory_rewrite_threshold_ = memory_rewrite_threshold;
        inited_ = true;
      }
    }
  }

  return ret;
}

int ObMemfragRecycleAllocator::set_recycle_param(
    const int64_t expire_duration_us, const int64_t memory_rewrite_threshold)
{
  int ret = OB_SUCCESS;
  if (expire_duration_us < 0 || memory_rewrite_threshold <= 0) {
    LIB_ALLOC_LOG(WARN, "Invalid arguments, ", K(expire_duration_us), K(memory_rewrite_threshold));
    ret = OB_INVALID_ARGUMENT;
  } else {
    expire_duration_us_ = expire_duration_us;
    total_memory_rewrite_threshold_ = memory_rewrite_threshold;
  }
  return ret;
}

void ObMemfragRecycleAllocator::destroy()
{
  if (inited_) {
    if (NULL != rewritable_) {
      ObMRAllocatorRecycler::get_instance().deregister_allocator(this);
    }
    allocator_[0].destroy();
    allocator_[1].destroy();
    state_ = INIT;
    cur_allocator_pos_ = 0;
    rewritable_ = NULL;
    inited_ = false;
  }
}

void* ObMemfragRecycleAllocator::alloc(const int64_t size)
{
  int64_t pos = ATOMIC_FAA(&cur_allocator_pos_, 0);
  void* ptr_ret = NULL;
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LIB_ALLOC_LOG(WARN, "The ObMemfragRecycleAllocator has not been inited.");
  } else {
    ptr_ret = allocator_[pos].alloc(size + sizeof(int64_t) * 2);

    if (NULL != ptr_ret) {
      *(static_cast<int64_t*>(ptr_ret)) = META_MAGIC_NUM;
      *(static_cast<int64_t*>(ptr_ret) + 1) = pos;
      ptr_ret = static_cast<int64_t*>(ptr_ret) + 2;
    }
  }
  UNUSED(ret);
  return ptr_ret;
}

void ObMemfragRecycleAllocator::free(void* ptr)
{
  if (!inited_) {
    LIB_ALLOC_LOG(WARN, "The ObMemfragRecycleAllocator has not been inited.");
  } else if (NULL == static_cast<int64_t*>(ptr) - 2 || NULL == static_cast<int64_t*>(ptr) - 1) {
    LIB_ALLOC_LOG(ERROR, "wrong pointer", "ptr", ptr);
  } else {
    int64_t magic_num = *(static_cast<int64_t*>(ptr) - 2);
    int64_t pos = *(static_cast<int64_t*>(ptr) - 1);

    if (magic_num != META_MAGIC_NUM || (pos != 0 && pos != 1)) {
      LIB_ALLOC_LOG(ERROR, "wrong data with ", "magic_num", magic_num, "pos", pos);
    } else {
      void* tmp = static_cast<int64_t*>(ptr) - 2;
      allocator_[pos].free(tmp);
      tmp = NULL;
    }
  }
}

int ObMemfragRecycleAllocator::need_rewrite(const void* ptr, bool& is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LIB_ALLOC_LOG(WARN, "The ObMemfragRecycleAllocator has not been inited.");
  } else if (NULL == rewritable_) {
    ret = OB_ERR_UNEXPECTED;
    LIB_ALLOC_LOG(WARN, "The need_rewrite func should not be invoked for thread unsafe allocator.");
  } else if (NULL != ptr) {
    int64_t magic_num = *(static_cast<const int64_t*>(ptr) - 2);
    int64_t pos = *(static_cast<const int64_t*>(ptr) - 1);

    if (META_MAGIC_NUM != magic_num || (0 != pos && 1 != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_ALLOC_LOG(ERROR, "wrong data with ", "magic_num", magic_num, "pos", pos);
    } else {
      is_need = (pos != cur_allocator_pos_);
    }
  }

  return ret;
}

void ObMemfragRecycleAllocator::set_label(const lib::ObLabel& label)
{
  allocator_[0].set_label(label);
  allocator_[1].set_label(label);
}

bool ObMemfragRecycleAllocator::need_switch(ObDelayFreeAllocator& allocator)
{
  return allocator.get_total_size() >= total_memory_rewrite_threshold_ ||
         allocator.get_memory_fragment_size() >= FRAGMENT_MEMORY_REWRITE_THRESHOLD ||
         (::oceanbase::common::ObTimeUtility::current_time() - state_switch_time_ >
             EXPIRE_DURATION_FACTOR * expire_duration_us_);
}

int ObMemfragRecycleAllocator::switch_state()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  enum ObMemfragRecycleAllocatorState old_state = state_;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LIB_ALLOC_LOG(WARN, "The ObMemfragRecycleAllocator has not been inited.");
  } else if (NULL == rewritable_) {
    ret = OB_ERR_UNEXPECTED;
    LIB_ALLOC_LOG(WARN, "The switch_state func should not be invoked for thread unsafe allocator.");
  } else {
    switch (state_) {
      case INIT: {
        if (need_switch(allocator_[cur_allocator_pos_])) {
          (void)ATOMIC_SET(&cur_allocator_pos_, 1 - cur_allocator_pos_);
          state_ = SWITCHING_PREPARE;
          state_switch_time_ = ::oceanbase::common::ObTimeUtility::current_time();
        }
        break;
      }
      case SWITCHING_PREPARE: {
        if (::oceanbase::common::ObTimeUtility::current_time() - state_switch_time_ > expire_duration_us_) {
          state_ = SWITCHING;
          state_switch_time_ = ::oceanbase::common::ObTimeUtility::current_time();
        }
        break;
      }
      case SWITCHING: {
        if (OB_SUCCESS == (ret = rewritable_->rewrite_switch())) {
          state_ = SWITCHING_OVER;
          total_memory_rewrite_threshold_ = max(DEFAULT_TOTAL_MEMORY_REWRITE_THRESHOLD,
              static_cast<int64_t>(TOTAL_MEMORY_REWRITE_THRESHOLD_FACTOR *
                                   static_cast<double>(allocator_[cur_allocator_pos_].get_total_size())));
          if (total_memory_rewrite_threshold_ > MAX_TOTAL_MEMORY_REWRITE_THRESHOLD) {
            total_memory_rewrite_threshold_ = MAX_TOTAL_MEMORY_REWRITE_THRESHOLD;
          }
          state_switch_time_ = ::oceanbase::common::ObTimeUtility::current_time();
        } else {
          LIB_ALLOC_LOG(WARN, "fail to execute rewrite switch function, ", "ret", ret);
        }
        break;
      }
      case SWITCHING_OVER: {
        if (::oceanbase::common::ObTimeUtility::current_time() - state_switch_time_ > expire_duration_us_) {
          allocator_[1 - cur_allocator_pos_].reset();
          state_ = INIT;
          state_switch_time_ = ::oceanbase::common::ObTimeUtility::current_time();
        }
        break;
      }
      default: {
        LIB_ALLOC_LOG(ERROR, "invalid state,  ", "state", static_cast<int32_t>(state_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LIB_ALLOC_LOG(INFO,
        "switch state: ",
        "old_state",
        static_cast<int32_t>(old_state),
        "new_state",
        static_cast<int32_t>(state_),
        "current allocator total_size",
        allocator_[cur_allocator_pos_].get_total_size(),
        "current allocator memory_fragment_size",
        allocator_[cur_allocator_pos_].get_memory_fragment_size(),
        "backup allocator total size",
        allocator_[1 - cur_allocator_pos_].get_total_size(),
        "backup allocator fragment size",
        allocator_[1 - cur_allocator_pos_].get_memory_fragment_size(),
        K(total_memory_rewrite_threshold_));
  }

  return ret;
}

int64_t ObMemfragRecycleAllocator::get_all_alloc_size() const
{
  int64_t total_size = 0;
  total_size += allocator_[0].get_total_size();
  total_size += allocator_[1].get_total_size();
  return total_size;
}

int64_t ObMemfragRecycleAllocator::get_cur_alloc_size() const
{
  return allocator_[cur_allocator_pos_].get_total_size();
}

ObMRAllocatorRecycler::SelfCreator ObMRAllocatorRecycler::self_creator_;

ObMRAllocatorRecycler::SelfCreator::SelfCreator()
{
  ObMRAllocatorRecycler::get_instance();
}

ObMRAllocatorRecycler::SelfCreator::SelfCreator::~SelfCreator()
{}

ObMRAllocatorRecycler::ObMRAllocatorRecycleTask::ObMRAllocatorRecycleTask()
{}

ObMRAllocatorRecycler::ObMRAllocatorRecycleTask::~ObMRAllocatorRecycleTask()
{}

void ObMRAllocatorRecycler::ObMRAllocatorRecycleTask::runTimerTask()
{
  ObMRAllocatorRecycler::get_instance().recycle();
}

ObMRAllocatorRecycler::ObMRAllocatorRecycler() : is_scheduled_(false)
{
  memset(allocators_, 0, sizeof(allocators_));
}

ObMRAllocatorRecycler::~ObMRAllocatorRecycler()
{
  timer_.destroy();
}

ObMRAllocatorRecycler& ObMRAllocatorRecycler::get_instance()
{
  static ObMRAllocatorRecycler instance_;
  return instance_;
}

int ObMRAllocatorRecycler::register_allocator(ObMemfragRecycleAllocator* allocator)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);

  if (NULL == allocator) {
    LIB_ALLOC_LOG(WARN, "The allocator is NULL.");
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (!is_scheduled_) {
      if (OB_FAIL(timer_.init())) {
        LIB_ALLOC_LOG(WARN, "fail to init the timer, ", "ret", ret);
      } else if (OB_FAIL(timer_.schedule(task_, TIMER_SCHEDULE_INTERVAL_US, true))) {
        LIB_ALLOC_LOG(WARN, "fail to schedule the timer, ", "ret", ret);
      } else {
        is_scheduled_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      int64_t i = 0;
      int64_t pos = -1;
      // find a position to register allocator and check if the allocator has been registered.
      for (i = 0; OB_SUCC(ret) && i < MAX_META_MEMORY_ALLOCATOR_COUNT; ++i) {
        if (NULL == allocators_[i] && -1 == pos) {
          pos = i;
        } else if (allocator == allocators_[i]) {
          ret = OB_ERR_UNEXPECTED;
          LIB_ALLOC_LOG(WARN, "The allocator has been registered.");
        }
      }

      if (OB_SUCC(ret)) {
        if (-1 == pos) {
          ret = OB_SIZE_OVERFLOW;
          LIB_ALLOC_LOG(WARN, "Can not register new allocator any more.");
        } else {
          allocators_[pos] = allocator;
        }
      }
    }
  }

  return ret;
}

int ObMRAllocatorRecycler::deregister_allocator(ObMemfragRecycleAllocator* allocator)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);

  if (NULL == allocator) {
    ret = OB_INVALID_ARGUMENT;
    LIB_ALLOC_LOG(WARN, "The allocator is NULL.");
  } else {
    int64_t i = 0;
    bool found = false;
    for (i = 0; !found && i < MAX_META_MEMORY_ALLOCATOR_COUNT; ++i) {
      if (allocator == allocators_[i]) {
        allocators_[i] = NULL;
        // has found the allocator, so break
        found = true;
      }
    }
    if (MAX_META_MEMORY_ALLOCATOR_COUNT == i) {
      ret = OB_ERR_UNEXPECTED;
      LIB_ALLOC_LOG(WARN, "Can not find ", "allocator", allocator);
    }
  }

  return ret;
}

void ObMRAllocatorRecycler::recycle()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);

  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_META_MEMORY_ALLOCATOR_COUNT; ++i) {
    if (NULL != allocators_[i]) {
      if (OB_FAIL(allocators_[i]->switch_state())) {
        LIB_ALLOC_LOG(WARN, "fail to recycle, ", "allocator", allocators_[i], "ret", ret);
      }
    }
  }
}

void ObMRAllocatorRecycler::set_schedule_interval_us(int64_t schedule_interval_us)
{
  int ret = OB_SUCCESS;
  if (timer_.inited() && schedule_interval_us > 0) {
    timer_.cancel_all();
    if (OB_FAIL(timer_.schedule(task_, schedule_interval_us, true))) {
      LIB_ALLOC_LOG(WARN, "fail to schedule the timer, ", "ret", ret);
    }
  }
}

} /* namespace common */
} /* namespace oceanbase */
