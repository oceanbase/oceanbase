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
#include "lib/io/ob_io_manager.h"
#include "lib/io/ob_io_benchmark.h"
#include "lib/wait_event/ob_wait_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread_mgr.h"

namespace oceanbase {
using namespace lib;
namespace common {

/**
 * ----------------------------------- ObIOHandle ------------------------------
 */

ObIOHandle::ObIOHandle() : master_(NULL)
{}

ObIOHandle::~ObIOHandle()
{
  reset();
}

ObIOHandle::ObIOHandle(const ObIOHandle& other)
{
  *this = other;
}

ObIOHandle& ObIOHandle::operator=(const ObIOHandle& other)
{
  if (&other != this) {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(other.master_)) {
      if (OB_FAIL(set_master(*other.master_))) {
        COMMON_LOG(ERROR, "Fail to set ctrl, ", K(ret));
      }
    }
  }
  return *this;
}

int ObIOHandle::set_master(ObIOMaster& master)
{
  int ret = OB_SUCCESS;
  reset();
  master.inc_ref();
  master.inc_out_ref();
  master_ = &master;
  return ret;
}

int ObIOHandle::wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(master_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOHandle has not been inited, ", K(ret));
  } else if (timeout_ms < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(timeout_ms), K(ret));
  } else if (!master_->has_finished_) {
    ObWaitEventGuard wait_guard(master_->io_info_.io_desc_.wait_event_no_, timeout_ms, master_->io_info_.size_);
    int real_wait_timeout = min(OB_IO_MANAGER.get_io_config().data_storage_io_timeout_ms_, timeout_ms);

    if (real_wait_timeout > 0) {
      ObThreadCondGuard guard(master_->cond_);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(ERROR, "fail to guard master condition", K(ret));
      } else if (!master_->has_finished_ && OB_FAIL(master_->cond_.wait(real_wait_timeout))) {
        COMMON_LOG(WARN, "fail to wait master condition", K(ret), K(real_wait_timeout), K(*master_));
      }
    } else {
      ret = OB_TIMEOUT;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(master_->io_ret_)) {
      COMMON_LOG(WARN, "IO error, ", K(ret), K(*master_));
    }
  } else if (OB_TIMEOUT == ret) {
    COMMON_LOG(WARN, "IO wait timeout, ", K(timeout_ms), K(ret), K(*master_));
  }
  estimate();

  return ret;
}

OB_INLINE static int64_t get_io_interval(const int64_t& begin_time, const int64_t& end_time)
{
  int64_t ret_time = end_time - begin_time;
  static const int64_t max_io_time = 600L * 1000L * 1000L;  // 600s
  ret_time = max(ret_time, 0);
  ret_time = min(ret_time, max_io_time);
  return ret_time;
}

void ObIOHandle::estimate()
{
  if (OB_NOT_NULL(master_) && master_->has_finished_ && !master_->has_estimated_) {
    ObIOMaster::TimeLog& time = master_->time_;
    const int64_t prepare_delay = time.prepare_delay_;
    const int64_t request_delay = get_io_interval(time.send_time_, time.recv_time_);
    const int64_t cb_queue_delay = get_io_interval(time.callback_enqueue_time_, time.callback_dequeue_time_);
    const int64_t cb_process_delay = time.callback_delay_;
    if (IO_MODE_READ == master_->mode_) {
      EVENT_INC(ObStatEventIds::IO_READ_COUNT);
      EVENT_ADD(ObStatEventIds::IO_READ_BYTES, master_->io_info_.size_);
      EVENT_ADD(ObStatEventIds::IO_READ_DELAY, request_delay);
      EVENT_ADD(ObStatEventIds::IO_READ_CB_QUEUE_DELAY, cb_queue_delay);
      EVENT_ADD(ObStatEventIds::IO_READ_CB_PROCESS_DELAY, cb_process_delay);
    } else {
      EVENT_INC(ObStatEventIds::IO_WRITE_COUNT);
      EVENT_ADD(ObStatEventIds::IO_WRITE_BYTES, master_->io_info_.size_);
      EVENT_ADD(ObStatEventIds::IO_WRITE_DELAY, request_delay);
    }

    master_->has_estimated_ = true;
    if (prepare_delay + request_delay + cb_queue_delay + cb_process_delay > LONG_IO_PRINT_TRIGGER_US) {
      COMMON_LOG(WARN,
          "IO Wait too long, ",
          K(prepare_delay),
          K(request_delay),
          K(cb_queue_delay),
          K(cb_process_delay),
          K(*master_));
    }
  }
}

int ObIOHandle::get_io_errno(int& io_errno)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(master_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOHandle has not been inited, ", K(ret));
  } else {
    io_errno = master_->io_ret_;  // todo
  }
  return ret;
}

const char* ObIOHandle::get_buffer()
{
  const char* buf = NULL;
  if (OB_NOT_NULL(master_) && master_->has_finished_ && OB_SUCCESS == master_->io_ret_) {
    if (OB_ISNULL(master_->callback_)) {
      LOG_ERROR("callback must not null", K(*master_));
    } else {
      buf = master_->callback_->get_data();
    }
  }
  return buf;
}

int64_t ObIOHandle::get_data_size()
{
  return OB_NOT_NULL(master_) ? master_->io_info_.size_ : 0;
}

int64_t ObIOHandle::get_rt() const
{
  int64_t rt = -1;
  if (OB_NOT_NULL(master_)) {
    if (master_->time_.end_time_ > master_->time_.begin_time_ && master_->time_.begin_time_ > 0) {
      rt = master_->time_.end_time_ - master_->time_.begin_time_;
    }
  }
  return rt;
}

void ObIOHandle::reset()
{
  if (OB_NOT_NULL(master_)) {
    master_->dec_out_ref();
    master_->dec_ref();
    master_ = NULL;
  }
}

void ObIOHandle::cancel()
{
  if (OB_NOT_NULL(master_)) {
    master_->cancel();
  }
}

int64_t ObIOHandle::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(master_)) {
    databuff_printf(buf, buf_len, pos, "WARN The ObIOHandle has not been inited, ");
  } else {
    J_KV("master", *master_);
  }
  return pos;
}

/**
 * --------------------------------------- ObIOCallbackRunner ---------------------------
 */

ObIOCallbackRunner::ObIOCallbackRunner()
    : inited_(false), callback_thread_cnt_(0), callback_queue_(nullptr), callback_cond_(nullptr)
{}

ObIOCallbackRunner::~ObIOCallbackRunner()
{}

int ObIOCallbackRunner::init(const int32_t queue_depth)
{
  int ret = OB_SUCCESS;
  void* buf = nullptr;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (queue_depth <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(queue_depth));
  } else {
    callback_thread_cnt_ =
        !lib::is_mini_mode() ? ObIOManager::MAX_CALLBACK_THREAD_CNT : ObIOManager::MINI_MODE_MAX_CALLBACK_THREAD_CNT;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(*callback_queue_) * callback_thread_cnt_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "fail to alloc memory for callback queue", K(ret), K(callback_thread_cnt_));
    } else {
      callback_queue_ = new (buf) ObFixedQueue<ObIOMaster>[callback_thread_cnt_];
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(*callback_cond_) * callback_thread_cnt_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "fail to alloc memory for callback queue", K(ret), K(callback_thread_cnt_));
    } else {
      callback_cond_ = new (buf) ObThreadCond[callback_thread_cnt_];
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < callback_thread_cnt_; ++i) {
    if (OB_FAIL(callback_queue_[i].init(queue_depth))) {
      COMMON_LOG(WARN, "Fail to init callback queue", K(ret));
    } else if (OB_FAIL(callback_cond_[i].init(ObWaitEventIds::IO_CALLBACK_QUEUE_LOCK_WAIT))) {
      COMMON_LOG(WARN, "Fail to init callback thread condition", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  if (!inited_) {
    destroy();
  }
  return ret;
}

void clear_callback(ObFixedQueue<ObIOMaster>& cb_queue)
{
  int ret = OB_SUCCESS;
  ObIOMaster* io_master = nullptr;
  while (OB_SUCC(ret) && cb_queue.get_total() > 0) {
    if (OB_SUCC(cb_queue.pop(io_master)) && OB_NOT_NULL(io_master) && io_master->need_callback_ &&
        io_master->can_callback()) {
      io_master->callback_->~ObIOCallback();
    }
  }
}

void ObIOCallbackRunner::destroy()
{
  inited_ = false;
  for (int64_t i = 0; i < callback_thread_cnt_; ++i) {
    if (OB_NOT_NULL(callback_queue_)) {
      ObFixedQueue<ObIOMaster>& cur_queue = callback_queue_[i];
      clear_callback(cur_queue);
      cur_queue.destroy();
    }
    if (OB_NOT_NULL(callback_cond_)) {
      callback_cond_[i].destroy();
    }
  }
  callback_thread_cnt_ = 0;
  callback_queue_ = nullptr;
  callback_cond_ = nullptr;
}

int ObIOCallbackRunner::enqueue_callback(ObIOMaster& master)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "Not init", K(ret));
  } else if (!master.need_callback_) {
    ret = OB_CANCELED;
  } else if (!master.can_callback()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(master));
  } else {
    const int64_t callback_thread_cnt =
        MIN(ObIOManager::get_instance().get_io_config().callback_thread_count_, callback_thread_cnt_);
    const int64_t queue_idx = ObRandom::rand(0, callback_thread_cnt - 1);
    ObFixedQueue<ObIOMaster>& cur_queue = callback_queue_[queue_idx];
    ObThreadCond& cur_condition = callback_cond_[queue_idx];
    if (OB_FAIL(cur_queue.push(&master))) {
      COMMON_LOG(WARN, "Fail to enqueue callback", K(ret));
    } else {
      ObThreadCondGuard guard(cur_condition);
      // after the request has been pushed in the queue,
      // the return code is not allowed to change anymore
      if (OB_SUCCESS != (tmp_ret = guard.get_ret())) {
        COMMON_LOG(ERROR, "fail to guard callback condition", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = cur_condition.signal())) {
        COMMON_LOG(ERROR, "fail to signal callback condition", K(tmp_ret), K(master));
      }
    }
  }
  return ret;
}

int ObIOCallbackRunner::dequeue_callback(int64_t queue_idx, ObIOMaster*& master)
{
  int ret = OB_SUCCESS;
  master = NULL;
  ObFixedQueue<ObIOMaster>& cur_queue = callback_queue_[queue_idx];
  ObThreadCond& cur_condition = callback_cond_[queue_idx];
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "Not init", K(ret));
  } else if (cur_queue.get_total() <= 0) {
    ObThreadCondGuard guard(cur_condition);
    if (OB_FAIL(guard.get_ret())) {
      COMMON_LOG(ERROR, "fail to guard callback condition", K(ret));
    } else if (cur_queue.get_total() <= 0) {
      if (OB_FAIL(cur_condition.wait_us(CALLBACK_WAIT_PERIOD_US)) && OB_TIMEOUT != ret) {
        COMMON_LOG(ERROR, "fail to wait callback condition", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cur_queue.pop(master)) && OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "fail to pop master", K(ret));
    }
  }

  return ret;
}

void ObIOCallbackRunner::do_callback(const int64_t queue_idx)
{
  int ret = OB_SUCCESS;
  ObIOMaster* master = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(queue_idx < 0 || queue_idx >= callback_thread_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(queue_idx));
  } else if (OB_FAIL(dequeue_callback(queue_idx, master))) {
    if (OB_TIMEOUT == ret || OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      COMMON_LOG(WARN, "fail to dequeue callback", K(ret));
    }
  } else if (OB_ISNULL(master)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "master is null", K(ret));
  } else {
    const int64_t begin_time = ObTimeUtility::current_time();
    master->time_.callback_dequeue_time_ = begin_time;
    ObCurTraceId::TraceId saved_trace_id = *ObCurTraceId::get_trace_id();
    ObCurTraceId::set(master->get_trace_id());
    {  // callback must execute in guard, in case of cancel halfway
      ObThreadCondGuard guard(master->cond_);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(ERROR, "Fail to lock master condition, ", K(ret));
      } else if (!master->need_callback_) {
        // canceled, do nothing
      } else if (!master->can_callback()) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "master can not do callback", K(ret), K(master));
      } else if (OB_FAIL(master->callback_->process(OB_SUCCESS == master->io_ret_))) {
        COMMON_LOG(WARN, "fail to callback", K(ret));
      }
      master->io_ret_ = OB_SUCCESS == master->io_ret_ ? ret : master->io_ret_;
      master->time_.callback_delay_ = ObTimeUtility::current_time() - begin_time;
    }
    master->notify_finished();
    master->dec_ref();
    ObCurTraceId::set(saved_trace_id);
  }
}

/**
 * ---------------------------------------- ObIOManager ----------------------------------------
 */

ObIOManager::ObIOManager() : inited_(false), is_working_(false), is_disk_error_(false), is_disk_error_definite_(false)
{}

ObIOManager::~ObIOManager()
{
  destroy();
}

ObIOManager& ObIOManager::get_instance()
{
  static ObIOManager instance;
  return instance;
}

int ObIOManager::init(const int64_t mem_limit, const int32_t disk_number_limit, const int32_t queue_depth)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (mem_limit < OB_MALLOC_BIG_BLOCK_SIZE || disk_number_limit <= 0 ||
             disk_number_limit > ObDiskManager::MAX_DISK_NUM ||
             disk_number_limit > ObIOResourceManager::MAX_CHANNEL_CNT || queue_depth <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(mem_limit), K(disk_number_limit), K(queue_depth));
  } else if (OB_FAIL(resource_mgr_.init(mem_limit))) {
    COMMON_LOG(WARN, "fail to init resource manager", K(ret));
  } else if (OB_FAIL(disk_mgr_.init(disk_number_limit, &resource_mgr_))) {
    COMMON_LOG(WARN, "fail to init disk manager", K(ret));
  } else if (OB_FAIL(callback_mgr_.init(queue_depth))) {
    COMMON_LOG(WARN, "fail to init callback runner", K(ret));
  } else if (OB_FAIL(master_allocator_.init(mem_limit, OB_MALLOC_BIG_BLOCK_SIZE, OB_MALLOC_MIDDLE_BLOCK_SIZE))) {
    COMMON_LOG(WARN, "failed to init allocator", K(ret));
  } else {
    master_allocator_.set_label(ObModIds::OB_IO_MASTER);
    io_conf_.set_default_value();
    inited_ = true;
    if (OB_FAIL(TG_SET_RUNNABLE_AND_START(TGDefIDs::IOMGR, *this))) {
      inited_ = false;
      COMMON_LOG(WARN, "Fail to create io thread, ", K(ret));
    } else {
      is_working_ = true;
      LOG_INFO("succeed to init io manager", K(disk_number_limit), K(queue_depth));
    }
  }
  if (!inited_) {
    destroy();
  }
  return ret;
}

void ObIOManager::destroy()
{
  LOG_INFO("start destroy io manager");
  disk_mgr_.destroy();
  is_working_ = false;

  TG_STOP(TGDefIDs::IOMGR);
  TG_WAIT(TGDefIDs::IOMGR);

  callback_mgr_.destroy();
  resource_mgr_.destroy();
  io_conf_.reset();
  master_allocator_.destroy();
  inited_ = false;
  LOG_INFO("finish destroy io manager");
}

int ObIOManager::read(const ObIOInfo& info, ObIOHandle& handle, const uint64_t timeout_ms)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(aio_read(info, handle))) {
    COMMON_LOG(WARN, "fail to aio_read", K(ret), K(info));
  } else if (OB_FAIL(handle.wait(timeout_ms))) {
    COMMON_LOG(WARN, "fail to wait", K(ret), K(timeout_ms));
  }
  return ret;
}

int ObIOManager::write(const ObIOInfo& info, const uint64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObIOHandle handle;
  if (OB_FAIL(aio_write(info, handle))) {
    COMMON_LOG(WARN, "fail to aio write", K(ret), K(info));
  } else if (OB_FAIL(handle.wait(timeout_ms))) {
    COMMON_LOG(WARN, "fail to wait", K(ret), K(timeout_ms));
  }
  return ret;
}

int ObIOManager::aio_read(const ObIOInfo& info, ObIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObIOMaster* master = NULL;
  ObDefaultIOCallback callback;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_IN_STOP_STATE;
    COMMON_LOG(WARN, "io manager is stopped", K(ret));
  } else if (!info.is_valid() || !info.io_desc_.is_read_mode()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(info));
  } else if (OB_FAIL(callback.init(resource_mgr_.get_allocator(), info.offset_, info.size_))) {
    LOG_WARN("failed to init callback", K(ret), K(info));
  } else if (OB_FAIL(resource_mgr_.alloc_master(info.batch_count_, master))) {  // thread-safe
    COMMON_LOG(WARN, "fail to alloc master", K(ret), KP(master));
  } else if (OB_ISNULL(master)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "master is null", K(ret));
  } else {
    MasterHolder holder(master);
    master->time_.begin_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(inner_aio(info.io_desc_.mode_, info, &callback, master, handle))) {
      COMMON_LOG(WARN, "inner_aio failed", K(ret));
    }
  }
  return ret;
}

int ObIOManager::aio_read(const ObIOInfo& info, ObIOCallback& callback, ObIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObIOMaster* master = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_IN_STOP_STATE;
    COMMON_LOG(WARN, "io manager is stoped", K(ret));
  } else if (!info.is_valid() || !info.io_desc_.is_read_mode()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(info));
  } else if (OB_FAIL(resource_mgr_.alloc_master(info.batch_count_, master))) {  // thread-safe
    COMMON_LOG(WARN, "fail to alloc master", K(ret), KP(master));
  } else if (OB_ISNULL(master)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "master is null", K(ret));
  } else {
    MasterHolder holder(master);
    master->time_.begin_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(inner_aio(info.io_desc_.mode_, info, &callback, master, handle))) {
      COMMON_LOG(WARN, "inner_aio failed", K(ret));
    }
  }
  return ret;
}

int ObIOManager::aio_write(const ObIOInfo& info, ObIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObIOMaster* master = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_IN_STOP_STATE;
    COMMON_LOG(WARN, "io manager is stoped", K(ret));
  } else if (!info.is_valid() || !info.io_desc_.is_write_mode()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(info));
  } else if (OB_FAIL(resource_mgr_.alloc_master(info.batch_count_, master))) {  // thread-safe
    COMMON_LOG(WARN, "fail to alloc master", K(ret), KP(master));
  } else if (OB_ISNULL(master)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "master is null", K(ret));
  } else {
    MasterHolder holder(master);
    master->time_.begin_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(inner_aio(info.io_desc_.mode_, info, NULL /*callback*/, master, handle))) {
      COMMON_LOG(WARN, "inner_aio failed", K(ret));
    }
  }
  return ret;
}

int ObIOManager::aio_write(const ObIOInfo& info, ObIOCallback& callback, ObIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObIOMaster* master = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_IN_STOP_STATE;
    COMMON_LOG(WARN, "io manager is stoped", K(ret));
  } else if (!info.is_valid() || !info.io_desc_.is_write_mode()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(info));
  } else if (OB_FAIL(resource_mgr_.alloc_master(info.batch_count_, master))) {  // thread-safe
    COMMON_LOG(WARN, "fail to alloc master", K(ret), KP(master));
  } else if (OB_ISNULL(master)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "master is null", K(ret));
  } else {
    MasterHolder holder(master);
    master->time_.begin_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(inner_aio(info.io_desc_.mode_, info, &callback, master, handle))) {
      COMMON_LOG(WARN, "inner_aio failed", K(ret));
    }
  }
  return ret;
}

int ObIOManager::inner_aio(
    const ObIOMode mode, const ObIOInfo& info, ObIOCallback* callback, ObIOMaster* master, ObIOHandle& handle)
{
  // weak check, cause this function is only called by new aio_read or aio_write
  int ret = OB_SUCCESS;
  if (OB_ISNULL(master) || mode >= IO_MODE_MAX) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(master), K(mode));
  } else if (OB_FAIL(handle.set_master(*master))) {  // not safe, unless handle is only used after this function.
    COMMON_LOG(WARN, "fail to set master to handle", K(ret));
  } else if (OB_FAIL(master->open(mode, info, callback, &resource_mgr_, &master_allocator_))) {
    COMMON_LOG(WARN, "fail to init master", K(ret));
  } else if (OB_FAIL(master->send_request())) {  // thread-safe
    COMMON_LOG(WARN, "fail to enqueue master", K(ret), KP(master));
  }
  if (OB_FAIL(ret)) {
    handle.reset();
  }
  // COMMON_LOG(INFO, "inner aio finished", K(ret), K(*master));
  return ret;
}

int ObIOManager::set_io_config(const ObIOConfig& conf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOManager has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!conf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(conf), K(ret));
  } else {
    ObMutexGuard guard(conf_mutex_);
    io_conf_ = conf;
    COMMON_LOG(INFO, "Success to config io manager, ", K(conf));
  }
  return ret;
}

void ObIOManager::run1()
{
  int ret = OB_SUCCESS;
  int64_t thread_id = get_thread_idx();
  lib::set_thread_name("IOMGR", thread_id);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOManager has not been inited, ", K(ret));
  } else {
    const int callback_thread_cnt =
        !lib::is_mini_mode() ? ObIOManager::MAX_CALLBACK_THREAD_CNT : ObIOManager::MINI_MODE_MAX_CALLBACK_THREAD_CNT;
    if (thread_id < callback_thread_cnt) {  // callback thread pool
      while (!has_set_stop()) {
        callback_mgr_.do_callback(thread_id % callback_thread_cnt);
      }
      // COMMON_LOG(INFO, "callback stopped", K(thread_id));
    } else {  // schedule thread, dynamic adjust io paramter of each disk
      while (!has_set_stop()) {
        static const int64_t sched_period_us = 100000;
        usleep(sched_period_us);
        disk_mgr_.schedule_all_disks();
        (void)check_disk_error();
      }  // end while-loop
      // COMMON_LOG(INFO, "scheduler stopped", K(thread_id));
    }
  }
}

int ObIOManager::check_disk_error()
{
  int ret = OB_SUCCESS;
  ObArray<ObDiskFd> warning_disks;
  ObArray<ObDiskFd> error_disks;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(disk_mgr_.get_warning_disks(warning_disks))) {
    COMMON_LOG(WARN, "fail to get warning disks", K(ret));
  } else if (OB_FAIL(disk_mgr_.get_error_disks(error_disks))) {
    COMMON_LOG(WARN, "fail to get error disks", K(ret));
  } else {
    is_disk_error_ = warning_disks.size() > 0;
    is_disk_error_definite_ = error_disks.size() > 0;
  }
  return ret;
}

int ObIOManager::is_disk_error(bool& disk_error)
{
  disk_error = is_disk_error_;
  return OB_SUCCESS;
}

int ObIOManager::is_disk_error_definite(bool& disk_error)
{
  disk_error = is_disk_error_definite_;
  return OB_SUCCESS;
}

int ObIOManager::reset_disk_error()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(disk_mgr_.reset_all_disks_health())) {
    COMMON_LOG(WARN, "fail to reset all disk health", K(ret));
  } else {
    is_disk_error_ = false;
    is_disk_error_definite_ = false;
  }
  return ret;
}

} /* namespace common */
} /* namespace oceanbase */
