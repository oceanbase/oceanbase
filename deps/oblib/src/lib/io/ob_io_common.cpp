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

#define USING_LOG_PREFIX COMMON
#include "lib/io/ob_io_common.h"
#include "lib/io/ob_io_request.h"
#include "lib/io/ob_io_disk.h"
#include "ob_io_resource.h"

namespace oceanbase {
using namespace lib;
namespace common {

int ob_io_setup(int maxevents, io_context_t* ctxp)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = E(EventTable::EN_IO_SETUP) OB_SUCCESS;
  if (OB_SUCC(ret)) {
    ret = io_setup(maxevents, ctxp);
  }
#else
  ret = io_setup(maxevents, ctxp);
#endif
  return ret;
}

int ob_io_destroy(io_context_t ctx)
{
  return io_destroy(ctx);
}

int ob_io_submit(io_context_t ctx, long nr, struct iocb** iocbpp)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = E(EventTable::EN_IO_SUBMIT) OB_SUCCESS;
  if (OB_SUCC(ret)) {
    ret = io_submit(ctx, nr, iocbpp);
  } else {
    if (OB_NEED_RETRY == ret) {
      // for test partial io complete
      for (int64_t i = 0; i < nr; ++i) {
        if (iocbpp[i]->u.c.nbytes > DIO_READ_ALIGN_SIZE) {
          iocbpp[i]->u.c.nbytes -= DIO_READ_ALIGN_SIZE;
        }
      }
      ret = io_submit(ctx, nr, iocbpp);
    } else if (OB_EAGAIN == ret) {
      // each time submit 1 event
      ret = io_submit(ctx, 1, iocbpp);
    }
  }
#else
  ret = io_submit(ctx, nr, iocbpp);
#endif
  return ret;
}

int ob_io_cancel(io_context_t ctx, struct iocb* iocb, struct io_event* evt)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = io_cancel(ctx, iocb, evt);
  COMMON_LOG(INFO, "real ret of io_cancel, ", K(ret));
  int tmp_ret = E(EventTable::EN_IO_CANCEL) OB_SUCCESS;
  if (OB_SUCCESS != tmp_ret) {
    if (OB_NOT_SUPPORTED == tmp_ret) {
      ret = OB_SUCCESS;  // Manually map OB_NOT_SUPPORTED to OB_SUCCESS to simu io_cancel success.
    } else {             // inject other error code
      ret = tmp_ret;
    }
  } else {
    // not inject error_code, return the ret returned by io_cancel
  }
#else
  ret = io_cancel(ctx, iocb, evt);
#endif
  return ret;
}

int ob_io_getevents(io_context_t ctx_id, long min_nr, long nr, struct io_event* events, struct timespec* timeout)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = E(EventTable::EN_IO_GETEVENTS) OB_SUCCESS;
  if (OB_SUCC(ret)) {
    ret = io_getevents(ctx_id, min_nr, nr, events, timeout);
  } else {
    if (OB_IO_ERROR == ret) {
      ret = io_getevents(ctx_id, min_nr, nr, events, timeout);
      for (int64_t i = 0; i < ret; ++i) {
        events[i].res = 1;
      }
    } else if (OB_TIMEOUT == ret) {
      sleep(1);
      ret = io_getevents(ctx_id, min_nr, nr, events, timeout);
    } else if (OB_AIO_TIMEOUT == ret) {
      // ignore real return
      io_getevents(ctx_id, min_nr, nr, events, timeout);
      ret = 0;
    } else if (OB_EAGAIN == ret) {
      ret = io_getevents(ctx_id, min_nr, nr, events, timeout);
      if (ret >= 1) {
        // random make 1 event fail
        int64_t idx = common::ObRandom::rand(1, ret) - 1;
        events[idx].res2 = -EIO;
      }
    } else if (OB_RESOURCE_OUT == ret) {
      // OB_CS_OUTOF_DISK_SPACE is not defined in lib/ob_errno.h, use OB_RESOURCE_OUT to represent
      ret = io_getevents(ctx_id, min_nr, nr, events, timeout);
      for (int64_t i = 0; i < ret; ++i) {
        events[i].res2 = -ENOSPC;
      }
    }
  }
#else
  // ignore EINTR and retry
  while ((ret = io_getevents(ctx_id, min_nr, nr, events, timeout)) < 0 && -EINTR == ret)
    ;
#endif
  return ret;
}

void align_offset_size(const int64_t offset, const int64_t size, int64_t& align_offset, int64_t& align_size)
{
  align_offset = lower_align(offset, DIO_READ_ALIGN_SIZE);
  align_size = upper_align(size + offset - align_offset, DIO_READ_ALIGN_SIZE);
}

/**
 * ------------------------------------ ObIOConfig ----------------------------------
 */

void ObIOConfig::set_default_value()
{
  sys_io_low_percent_ = DEFAULT_SYS_IO_LOW_PERCENT;
  sys_io_high_percent_ = DEFAULT_SYS_IO_HIGH_PERCENT;
  user_iort_up_percent_ = DEFAULT_USER_IO_UP_PERCENT;
  cpu_high_water_level_ = DEFAULT_CPU_HIGH_WATER_LEVEL;
  write_failure_detect_interval_ = DEFAULT_WRITE_FAILURE_DETECT_INTERVAL;
  read_failure_black_list_interval_ = DEFAULT_READ_FAILURE_IN_BLACK_LIST_INTERVAL;
  retry_warn_limit_ = DEFAULT_RETRY_WARN_LIMIT;
  retry_error_limit_ = DEFAULT_RETRY_ERROR_LIMIT;
  disk_io_thread_count_ = DEFAULT_DISK_IO_THREAD_COUNT;
  callback_thread_count_ = DEFAULT_IO_CALLBACK_THREAD_COUNT;
  large_query_io_percent_ = DEFAULT_LARGE_QUERY_IO_PERCENT;
  data_storage_io_timeout_ms_ = DEFAULT_DATA_STORAGE_IO_TIMEOUT_MS;
}

bool ObIOConfig::is_valid() const
{
  return sys_io_low_percent_ >= 0 && sys_io_low_percent_ <= 100 && sys_io_high_percent_ > 0 &&
         sys_io_high_percent_ <= 100 && sys_io_low_percent_ <= sys_io_high_percent_ && user_iort_up_percent_ >= 0 &&
         cpu_high_water_level_ > 0 && write_failure_detect_interval_ > 0 && read_failure_black_list_interval_ > 0 &&
         retry_warn_limit_ > 0 && retry_error_limit_ > retry_warn_limit_ && disk_io_thread_count_ > 0 &&
         disk_io_thread_count_ <= ObDisk::MAX_DISK_CHANNEL_CNT * 2 && disk_io_thread_count_ % 2 == 0 &&
         callback_thread_count_ > 0 && large_query_io_percent_ >= 0 && large_query_io_percent_ <= 100 &&
         data_storage_io_timeout_ms_ > 0;
}

void ObIOConfig::reset()
{
  sys_io_low_percent_ = 0;
  sys_io_high_percent_ = 0;
  user_iort_up_percent_ = 0;
  cpu_high_water_level_ = 0;
  write_failure_detect_interval_ = 0;
  read_failure_black_list_interval_ = 0;
  retry_warn_limit_ = 0;
  retry_error_limit_ = 0;
  disk_io_thread_count_ = 0;
  callback_thread_count_ = 0;
  large_query_io_percent_ = 0;
  data_storage_io_timeout_ms_ = 0;
}

/**
 * ------------------------------------ ObIOStat & Diff ----------------------------------
 */

void ObIOStat::update_io_stat(const uint64_t io_bytes, const uint64_t io_rt_us)
{
  ATOMIC_INC(&io_cnt_);
  ATOMIC_AAF(&io_bytes_, io_bytes);
  ATOMIC_AAF(&io_rt_us_, io_rt_us);
}

ObIOStatDiff::ObIOStatDiff() : average_size_(0), average_rt_us_(0), old_stat_(), new_stat_(), io_stat_(NULL)
{}

void ObIOStatDiff::estimate()
{
  if (OB_ISNULL(io_stat_)) {
    COMMON_LOG(WARN, "io stat is null");
  } else {
    old_stat_ = new_stat_;
    new_stat_ = *io_stat_;
    if (new_stat_.io_cnt_ > old_stat_.io_cnt_ && new_stat_.io_bytes_ > old_stat_.io_bytes_ &&
        new_stat_.io_rt_us_ > old_stat_.io_rt_us_) {
      uint64_t io_cnt = new_stat_.io_cnt_ - old_stat_.io_cnt_;
      uint64_t io_bytes = new_stat_.io_bytes_ - old_stat_.io_bytes_;
      uint64_t io_rt_us = new_stat_.io_rt_us_ - old_stat_.io_rt_us_;
      average_size_ = io_bytes / io_cnt;
      average_rt_us_ = (double)io_rt_us / (double)io_cnt;
    } else {
      average_size_ = 0;
      average_rt_us_ = 0;
    }
  }
}

ObCpuStatDiff::ObCpuStatDiff()
{
  MEMSET(&old_usage_, 0, sizeof(old_usage_));
  MEMSET(&new_usage_, 0, sizeof(new_usage_));
  old_time_ = ObTimeUtility::current_time();
  new_time_ = ObTimeUtility::current_time();
  MEMSET(&avg_usage_, 0, sizeof(avg_usage_));
  getrusage(RUSAGE_SELF, &old_usage_);
}

void ObCpuStatDiff::estimate()
{
  int64_t cpu_time = 0;
  int64_t sched_period_us = 0;
  new_time_ = ObTimeUtility::current_time();
  sched_period_us = new_time_ - old_time_;
  if (sched_period_us > 0) {
    if (0 == getrusage(RUSAGE_SELF, &new_usage_)) {
      cpu_time = (new_usage_.ru_utime.tv_sec - old_usage_.ru_utime.tv_sec) * 1000000 +
                 (new_usage_.ru_utime.tv_usec - old_usage_.ru_utime.tv_usec) +
                 (new_usage_.ru_stime.tv_sec - old_usage_.ru_stime.tv_sec) * 1000000 +
                 (new_usage_.ru_stime.tv_usec - old_usage_.ru_stime.tv_usec);
      avg_usage_ = (cpu_time * 100) / sched_period_us;
      old_usage_ = new_usage_;
      old_time_ = new_time_;
    }
  }
}

/**
 * ------------------------------------------ ObIOQueue ---------------------------------------
 */
ObIOQueue::ObIOQueue()
    : req_array_(NULL),
      queue_depth_(0),
      req_cnt_(0),
      allocator_(ObModIds::OB_IO_QUEUE, common::OB_MALLOC_BIG_BLOCK_SIZE),
      inited_(false)
{}

ObIOQueue::~ObIOQueue()
{}

int ObIOQueue::init(const int32_t queue_depth)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObIOQueue has been inited, ", K(ret));
  } else if (OB_UNLIKELY(queue_depth <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(queue_depth), K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObIORequest*) * queue_depth))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "Fail to allocate memory, ", K(queue_depth), K(ret));
  } else {
    req_array_ = new (buf) ObIORequest*[queue_depth];
    MEMSET(req_array_, 0, sizeof(ObIORequest*) * queue_depth);
    queue_depth_ = queue_depth;
    req_cnt_ = 0;
    inited_ = true;
  }

  if (!inited_) {
    destroy();
  }
  return ret;
}

void ObIOQueue::destroy()
{
  req_array_ = NULL;
  queue_depth_ = 0;
  req_cnt_ = 0;
  allocator_.reset();
  inited_ = false;
}

int ObIOQueue::push(ObIORequest& req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOQueue has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(req_cnt_ >= queue_depth_)) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "The io queue is full, ", K(ret));
  } else if (OB_UNLIKELY(PREWARM_IO == req.desc_.category_ && req_cnt_ >= queue_depth_ / 2)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    req.io_time_.enqueue_time_ = ObTimeUtility::current_time();
    req_array_[req_cnt_++] = &req;
    std::push_heap(&req_array_[0], &req_array_[req_cnt_], cmp_);
  }
  return ret;
}

int ObIOQueue::pop(ObIORequest*& req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOQueue has not been inited, ", K(ret));
  } else if (0 == req_cnt_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (req_array_[0]->deadline_time_ > ObTimeUtility::current_time()) {
    ret = OB_EAGAIN;
  } else {
    std::pop_heap(&req_array_[0], &req_array_[req_cnt_], cmp_);
    --req_cnt_;
    req = req_array_[req_cnt_];
    req->io_time_.dequeue_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

int64_t ObIOQueue::get_deadline()
{
  int64_t deadline = 0;
  if (inited_ && req_cnt_ > 0 && OB_NOT_NULL(req_array_[0])) {
    deadline = req_array_[0]->deadline_time_;
  }
  return deadline;
}

int ObIOQueue::direct_pop(ObIORequest*& req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOQueue has not been inited, ", K(ret));
  } else if (0 == req_cnt_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    --req_cnt_;
    req = req_array_[req_cnt_];
    req->io_time_.dequeue_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

bool ObIOQueue::ObIORequestCmp::operator()(const ObIORequest* a, const ObIORequest* b) const
{
  bool bret = true;
  if (OB_ISNULL(a) || OB_ISNULL(b)) {
    bret = false;
    COMMON_LOG(ERROR, "Comparing request is NULL", K(a), K(b));
  } else {
    bret = a->deadline_time_ > b->deadline_time_;
  }
  return bret;
}

/**
 * ------------------------------------- ObIOChannel ------------------------------------
 */
ObIOChannel::ObIOChannel() : inited_(false), context_(), submit_cnt_(0), can_submit_request_(true)
{}

ObIOChannel::~ObIOChannel()
{
  destroy();
}

int ObIOChannel::init(const int32_t queue_depth)
{
  int ret = OB_SUCCESS;
  int io_ret = 0;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObIOChannel has been inited, ", K(ret));
  } else if (queue_depth <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(queue_depth));
  } else if (OB_FAIL(queue_cond_.init(ObWaitEventIds::IO_QUEUE_LOCK_WAIT))) {
    COMMON_LOG(WARN, "Fail to init queue condition, ", K(ret));
  } else if (OB_FAIL(queue_.init(queue_depth))) {
    COMMON_LOG(WARN, "Fail to init io queue, ", K(ret));
  } else {
    MEMSET(&context_, 0, sizeof(context_));
    if (0 != (io_ret = ob_io_setup(MAX_AIO_EVENT_CNT, &context_))) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "Fail to setup io context, check config aio-max-nr of operating system", K(ret), K(io_ret));
    } else {
      submit_cnt_ = 0;
      can_submit_request_ = true;
      inited_ = true;
    }
  }

  if (!inited_) {
    destroy();
  }
  return ret;
}

void ObIOChannel::destroy()
{
  ob_io_destroy(context_);
  MEMSET(&context_, 0, sizeof(context_));
  submit_cnt_ = 0;
  can_submit_request_ = false;
  queue_cond_.destroy();
  queue_.destroy();
  inited_ = false;
}

int ObIOChannel::enqueue_request(ObIORequest& req)
{
  int ret = OB_SUCCESS;
  bool is_req_in_queue = false;
#ifdef ERRSIM
  ret = EVENT_CALL(EventTable::EN_IO_CHANNEL_QUEUE_ERROR);
  if (OB_FAIL(ret)) {
    COMMON_LOG(WARN, "Errsim enqueue failure, ", K(ret));
    return ret;
  }
#endif
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "Not init", K(ret));
  } else {
    ObThreadCondGuard cond_guard(queue_cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      COMMON_LOG(ERROR, "Fail to guard queue condition", K(ret));
    } else if (OB_FAIL(queue_.push(req))) {
      COMMON_LOG(WARN, "Fail to enqueue io request, ", K(ret));
    } else {
      is_req_in_queue = true;
      if (OB_FAIL(queue_cond_.signal())) {
        COMMON_LOG(ERROR, "Fail to signal queue condition", K(ret));
      }
    }
  }
  if (!is_req_in_queue) {
    req.finish(ret, 0);  // thread-safe
  }

  return ret;
}

int ObIOChannel::dequeue_request(ObIORequest*& req)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    ObThreadCondGuard cond_guard(queue_cond_);
    const int64_t timeout_us = get_pop_wait_timeout(queue_.get_deadline());
    if (OB_FAIL(cond_guard.get_ret())) {
      COMMON_LOG(ERROR, "Fail to guard queue condition", K(ret));
    } else if (timeout_us > 0 && OB_FAIL(queue_cond_.wait_us(timeout_us))) {
      if (OB_TIMEOUT == ret) {
        ret = OB_SUCCESS;
      } else {
        COMMON_LOG(ERROR, "fail to wait queue condition", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (ATOMIC_LOAD(&submit_cnt_) >= MAX_AIO_EVENT_CNT) {
        ret = OB_EAGAIN;
        if (TC_REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          COMMON_LOG(
              INFO, "There are too many submit io request!", K_(submit_cnt), "queue_size", queue_.get_req_count());
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = queue_.pop(req);
    }
  }
  return ret;
}

int64_t ObIOChannel::get_pop_wait_timeout(const int64_t queue_deadline)
{
  const int64_t current_time = ObTimeUtility::current_time();
  int64_t wait_us = 0;
  if (submit_cnt_ >= MAX_AIO_EVENT_CNT) {
    wait_us = DEFAULT_SUBMIT_WAIT_US;
  } else if (queue_deadline <= 0) {
    wait_us = DEFAULT_SUBMIT_WAIT_US;
  } else if (queue_deadline <= current_time) {
    wait_us = 0;
  } else if (queue_deadline > current_time) {
    wait_us = queue_deadline - current_time;
    if (wait_us > DEFAULT_SUBMIT_WAIT_US) {
      wait_us = DEFAULT_SUBMIT_WAIT_US;
    }
  }
  return wait_us;
}

int ObIOChannel::clear_all_requests()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    ObIORequest* req = NULL;
    while (OB_SUCC(ret) && !queue_.empty()) {
      if (OB_FAIL(queue_.direct_pop(req))) {
        if (ret != OB_ENTRY_NOT_EXIST) {
          COMMON_LOG(WARN, "fail to pop request from queue", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(req)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "req is null", K(ret), KP(req));
      } else {
        req->finish(OB_CANCELED, 0);
      }
    }
  }
  return ret;
}

void ObIOChannel::submit()
{
  int ret = OB_SUCCESS;
  ObIORequest* req = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOChannel has not been inited, ", K(ret));
  } else if (!can_submit_request_) {
    ObThreadCondGuard cond_guard(queue_cond_);
    const int64_t empty_queue_wait_time_ms = 1000;
    if (queue_.empty() && OB_FAIL(queue_cond_.wait(empty_queue_wait_time_ms))) {
      if (OB_TIMEOUT != ret) {
        COMMON_LOG(WARN, "fail to wait queue cond", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret) && !can_submit_request_) {
      clear_all_requests();
    }
  } else {
    if (OB_FAIL(dequeue_request(req))) {
      if (OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        COMMON_LOG(WARN, "Fail to pop io request from disk, ", K(ret));
      }
    } else if (OB_ISNULL(req)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "req is null", K(ret));
    } else {
      MasterHolder master_holder(req->master_);
      DiskHolder disk_holder(req->get_disk());
      ObCurTraceId::TraceId saved_trace_id = *ObCurTraceId::get_trace_id();
      ObCurTraceId::set(req->master_->get_trace_id());
      req->channel_ = this;
      int sys_ret = 0;
      if (OB_FAIL(inner_submit(*req, sys_ret))) {
        if (OB_CANCELED != ret) {
          COMMON_LOG(WARN, "fail to inner submit req", K(ret), K(sys_ret));
        }
        req->finish(ret, sys_ret);
      } else {
        req->get_disk()->inc_ref();  // safe only under disk holder
      }
      ObCurTraceId::set(saved_trace_id);
    }
  }
}

int ObIOChannel::inner_submit(ObIORequest& req, int& sys_ret)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  int hang_fd = E(EventTable::EN_IO_HANG_ERROR) OB_SUCCESS;
  if (hang_fd > 0 && req.fd_.fd_ == hang_fd) {
    sleep(1);
  }
#endif

  sys_ret = 0;
  {  // guard submit to prevent canceling halfway
    ObThreadCondGuard guard(req.master_->cond_);
    if (OB_FAIL(guard.get_ret())) {
      COMMON_LOG(ERROR, "fail to guard master condition", K(ret));
    } else if (!req.need_submit_) {
      // the io request has been canceled
      ret = OB_CANCELED;
    } else {
      req.io_time_.os_submit_time_ = ObTimeUtility::current_time();
      ATOMIC_INC(&submit_cnt_);

      struct iocb* iocbp = &(req.iocb_);
      if (1 != (sys_ret = ob_io_submit(context_, 1, &iocbp))) {
        ret = OB_IO_ERROR;
      }

      if (OB_FAIL(ret)) {
        ATOMIC_DEC(&submit_cnt_);
        COMMON_LOG(WARN, "Fail to submit io request, ", K(ret), K(submit_cnt_), K(sys_ret), K(req.desc_));
      } else {
        COMMON_LOG(DEBUG, "Success to submit io request, ", K(ret), K(sys_ret), K(req));
      }
    }
  }
  return ret;
}

void ObIOChannel::get_events()
{
  int ret = OB_SUCCESS;
  static __thread io_event events[MAX_AIO_EVENT_CNT];
  int32_t event_cnt = 0;
  int32_t finish_cnt = 0;
  ObIORequest* req = NULL;
  int64_t io_finish_time = 0;
  MEMSET(events, 0, sizeof(events));
  struct timespec timeout;
  timeout.tv_sec = 0;
  timeout.tv_nsec = AIO_TIMEOUT_NS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOChannel has not been inited, ", K(ret));
  } else {
    event_cnt = ob_io_getevents(context_, 1, MAX_AIO_EVENT_CNT, events, &timeout);
  }
  if (OB_SUCC(ret) && event_cnt > 0) {
    io_finish_time = ObTimeUtility::current_time();
    finish_cnt = event_cnt;
    for (int64_t i = 0; i < event_cnt; ++i) {
      req = reinterpret_cast<ObIORequest*>(events[i].data);
      if (OB_ISNULL(req)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "req is null", K(ret));
        continue;
      }
      const int system_errno = -static_cast<int>(events[i].res);
      const int32_t complete_size = static_cast<int32_t>(events[i].res);
      const int32_t res2 = static_cast<int32_t>(events[i].res2);

      req->io_time_.os_return_time_ = io_finish_time;
      if (0 == res2 && req->io_size_ == complete_size) {  // io full complete
        COMMON_LOG(DEBUG, "Success to get io event, ", K(*req), K(complete_size), K(res2));
        finish_flying_req(*req, OB_SUCCESS, 0);
      } else if (0 == res2 && complete_size > 0 && complete_size < req->io_size_ &&
                 (0 == complete_size % DIO_READ_ALIGN_SIZE)) {  // io partial complete, retry the left part
        COMMON_LOG(WARN, "Partial execute io request, ", K(*req), K(complete_size), K(res2));
        req->io_buf_ = req->io_buf_ + complete_size;
        req->io_size_ -= complete_size;
        req->io_offset_ += complete_size;

        if (IO_CMD_PREAD == req->iocb_.aio_lio_opcode) {
          io_prep_pread(&req->iocb_, req->fd_.fd_, req->io_buf_, req->io_size_, req->io_offset_);
        } else {
          io_prep_pwrite(&req->iocb_, req->fd_.fd_, req->io_buf_, req->io_size_, req->io_offset_);
        }
        req->iocb_.data = req;

        int sys_ret = 0;
        if (OB_SUCC(inner_submit(*req, sys_ret))) {
          --finish_cnt;
        } else {
          finish_flying_req(*req, OB_IO_ERROR, sys_ret);
        }
      } else {  // io failed
        // first print error log
        COMMON_LOG(ERROR, "Fail to execute io request, ", K(*req), K(complete_size), K(res2));
        // then notify
        finish_flying_req(*req, OB_IO_ERROR, system_errno);
      }
      ATOMIC_DEC(&submit_cnt_);
    }
  }
  // ignore failure
  ret = OB_SUCCESS;
  if (event_cnt < 0) {  // get event failed
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      COMMON_LOG(ERROR, "Fail to get io events, ", "errno", event_cnt);
    }
  }
}

void ObIOChannel::cancel(ObIORequest& req)
{
  int ret = OB_SUCCESS;
  UNUSED(ret);
  int sys_ret = 0;
  bool is_cancel = false;

  if (0 != req.io_time_.os_submit_time_ && 0 == req.io_time_.os_return_time_) {
    // Note: here if ob_io_cancel failed (possibly due to kernel not supporting io_cancel),
    // neither we or the get_events thread would call control.callback_->process(),
    // as we previously set need_callback to false.
    struct io_event event;
    MEMSET(&event, 0, sizeof(event));
    if (0 != (sys_ret = ob_io_cancel(context_, &req.iocb_, &event))) {
      COMMON_LOG(DEBUG, "Fail to cancel io request, ", KP(&req), K(context_), K(sys_ret));
    }

    if (0 == sys_ret) {
      ATOMIC_DEC(&submit_cnt_);
      req.finish(OB_CANCELED, sys_ret);
      is_cancel = true;
    }
  }

  // not support until kernel-aio support io_cancel
  if (is_cancel) {
    COMMON_LOG(WARN, "Shouldn't go here, io cancel not supported, ", KP(&req), K(context_), K(sys_ret));
    // the io request has been canceled
    COMMON_LOG(DEBUG, "The IO Request has been canceled!");
  }
}

void ObIOChannel::finish_flying_req(ObIORequest& req, int io_ret, int system_errno)
{
  ObDisk* disk = req.get_disk();
  if (OB_ISNULL(disk)) {
    COMMON_LOG(ERROR, "disk of finishing req is null", KP(&req), K(req));
  } else {
    req.finish(io_ret, system_errno);
    disk->dec_ref();
  }
}

/**
 * ------------------------------------ ObIOInfo ----------------------------------
 */

ObIOInfo::ObIOInfo()
    : size_(0),
      io_desc_(),
      batch_count_(0),
      fail_disk_count_(0),
      offset_(0),
      io_error_handler_(nullptr),
      finish_callback_(nullptr),
      finish_id_(-1)
{}

void ObIOInfo::operator=(const ObIOInfo& r)
{
  size_ = r.size_;
  io_desc_ = r.io_desc_;
  batch_count_ = r.batch_count_;
  if (batch_count_ > 0 && batch_count_ <= MAX_IO_BATCH_NUM) {
    MEMCPY(io_points_, r.io_points_, sizeof(io_points_[0]) * batch_count_);
    MEMCPY(fail_disk_ids_, r.fail_disk_ids_, sizeof(fail_disk_ids_[0]) * batch_count_);
  }
  fail_disk_count_ = r.fail_disk_count_;
  offset_ = r.offset_;
  io_error_handler_ = r.io_error_handler_;
  finish_callback_ = r.finish_callback_;
  finish_id_ = r.finish_id_;
}

bool ObIOInfo::is_valid() const
{
  bool bret = true;
  if (size_ <= 0 || !io_desc_.is_valid() || batch_count_ <= 0 || batch_count_ > MAX_IO_BATCH_NUM || offset_ < 0) {
    bret = false;
    LOG_WARN("not valid", K_(batch_count), K_(size), K_(io_desc), K_(offset));
  } else if (1 == batch_count_) {  // batch mode, only one io point, no need to be aligned
    bret = io_points_[0].is_valid() && size_ == io_points_[0].size_;
    if (!bret) {
      LOG_WARN("not valid", K_(size), K_(batch_count), "io point", io_points_[0]);
    }
  } else {                 // at least 2 io point
    int64_t sum_size = 0;  // for accumulate size in each io point.
    for (int64_t i = 0; bret && i < batch_count_; ++i) {
      const ObIOPoint& point = io_points_[i];
      bool aligned = false;
      if (0 == i) {  // first point, (offset + size) must reach the align border
        aligned = (point.offset_ + point.size_) % DIO_READ_ALIGN_SIZE == 0;
      } else if (batch_count_ - 1 == i) {  // last point, offset must be the align border
        aligned = point.offset_ % DIO_READ_ALIGN_SIZE == 0;
      } else {  // other point, both offset and size must be aligned;
        aligned = point.is_aligned(DIO_READ_ALIGN_SIZE);
      }
      // all io points must be valid and aligned
      if (point.is_valid() && aligned) {
        sum_size += io_points_[i].size_;
      } else {
        bret = false;
        LOG_WARN("not valid", K_(batch_count), K(point));
      }
    }
    if (bret) {
      bret = (size_ == sum_size);  // check size valid
      if (!bret) {
        LOG_WARN("not valid", K_(batch_count), K(size_), K(sum_size));
      }
    }
  }
  return bret;
}

int ObIOInfo::add_fail_disk(const int64_t disk_id)
{
  int ret = OB_SUCCESS;

  if (disk_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(disk_id));
  } else if (fail_disk_count_ >= MAX_IO_BATCH_NUM) {
    ret = OB_ERR_SYS;
    LOG_ERROR("fail disk count must not larger than MAX_IO_BATCH_NUM", K(ret), K(disk_id), K(*this));
  } else {
    fail_disk_ids_[fail_disk_count_] = disk_id;
    ++fail_disk_count_;
  }

  return ret;
}
void ObIOInfo::reset()
{
  size_ = 0;
  io_desc_ = ObIODesc();
  fail_disk_count_ = 0;
  offset_ = 0;
  for (int i = 0; i < batch_count_; ++i) {
    io_points_[i].reset();
    fail_disk_ids_[i] = 0;
  }
  batch_count_ = 0;
  io_error_handler_ = nullptr;
  finish_callback_ = nullptr;
  finish_id_ = -1;
}

int ObIOInfo::notice_finish() const
{
  int ret = OB_SUCCESS;

  if (NULL != finish_callback_) {
    if (OB_FAIL(finish_callback_->notice_finish(finish_id_))) {
      LOG_WARN("failed to notice finish", K(ret), K(*this));
    }
  }
  return ret;
}

ObDefaultIOCallback::ObDefaultIOCallback()
    : is_inited_(false), allocator_(NULL), offset_(-1), buf_size_(-1), io_buf_(NULL), io_buf_size_(0), data_buf_(NULL)
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObDefaultIOCallback::~ObDefaultIOCallback()
{
  if (NULL != allocator_ && NULL != io_buf_) {
    allocator_->free(io_buf_);
    io_buf_ = NULL;
  }
}

int ObDefaultIOCallback::init(common::ObIOAllocator* allocator, const int64_t offset, const int64_t buf_size)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(allocator) || offset < 0 || buf_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(allocator), K(offset), K(buf_size));
  } else {
    allocator_ = allocator;
    offset_ = offset;
    buf_size_ = buf_size;
    io_buf_ = NULL;
    data_buf_ = NULL;
    io_buf_size_ = 0;
    is_inited_ = true;
  }

  return ret;
}

int64_t ObDefaultIOCallback::size() const
{
  return sizeof(*this);
}

int ObDefaultIOCallback::alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset)
{
  int ret = OB_SUCCESS;
  io_buf_size = 0;
  aligned_offset = 0;
  common::align_offset_size(offset_, buf_size_, aligned_offset, io_buf_size);
  io_buf_size_ = io_buf_size + DIO_READ_ALIGN_SIZE;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == (io_buf_ = reinterpret_cast<char*>(allocator_->alloc(io_buf_size_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(io_buf_size), KP(io_buf_));
  } else {
    io_buf = upper_align_buf(io_buf_, DIO_READ_ALIGN_SIZE);
    data_buf_ = io_buf + (offset_ - aligned_offset);
  }
  LOG_DEBUG("alloc default io buf",
      KP(allocator_),
      K(io_buf_size),
      K(buf_size_),
      K(offset_),
      K(aligned_offset),
      KP(data_buf_),
      KP(io_buf_),
      K(io_buf_size_));

  return ret;
}

int ObDefaultIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!is_success) {
    if (nullptr != io_buf_) {
      allocator_->free(io_buf_);
      io_buf_ = nullptr;
      data_buf_ = nullptr;
    }
  }

  return ret;
}

int ObDefaultIOCallback::inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const
{
  int ret = OB_SUCCESS;
  callback = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (NULL != io_buf_ || NULL != data_buf_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot deep copy when io_buf or data buf not null", K(ret), KP(io_buf_), KP(data_buf_));
  } else if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else {
    ObDefaultIOCallback* new_callback = new (buf) ObDefaultIOCallback();
    new_callback->is_inited_ = is_inited_;
    new_callback->allocator_ = allocator_;
    new_callback->offset_ = offset_;
    new_callback->buf_size_ = buf_size_;
    new_callback->io_buf_ = NULL;
    new_callback->data_buf_ = NULL;
    callback = new_callback;
  }
  return ret;
}

const char* ObDefaultIOCallback::get_data()
{
  return data_buf_;
}

ObDiskID::ObDiskID() : disk_idx_(OB_INVALID_DISK_ID), install_seq_(-1)
{}

bool ObDiskID::is_valid() const
{
  return disk_idx_ >= 0 && disk_idx_ < common::OB_MAX_DISK_NUMBER && install_seq_ >= 0;
}

void ObDiskID::reset()
{
  disk_idx_ = -1;
  install_seq_ = -1;
}

bool ObDiskID::operator==(const ObDiskID& other) const
{
  return disk_idx_ == other.disk_idx_ && install_seq_ == other.install_seq_;
}
OB_SERIALIZE_MEMBER(ObDiskID, disk_idx_, install_seq_);

ObDiskFd::ObDiskFd() : fd_(-1), disk_id_()
{}

bool ObDiskFd::is_valid() const
{
  return fd_ >= 0 && disk_id_.is_valid();
}

void ObDiskFd::reset()
{
  fd_ = -1;
  disk_id_.reset();
}

bool ObDiskFd::operator==(const ObDiskFd& other) const
{
  return fd_ == other.fd_ && disk_id_ == other.disk_id_;
}

} /* namespace common */
} /* namespace oceanbase */
