/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOGSERVICE_LOG_ASYNC_PALF_CTX_INTERFACE_
#define OCEANBASE_LOGSERVICE_LOG_ASYNC_PALF_CTX_INTERFACE_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "log_async_io_struct.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace palf
{
class LogIOTask;
enum class LogIOTaskType;

// Decouples a per-PALF ctx from the concrete async worker that schedules it.
class IAsyncDriveWaker
{
public:
  IAsyncDriveWaker() {}
  virtual ~IAsyncDriveWaker() {}
  // Coalesce concurrent wake requests and schedule at least one future drive.
  // Success does not mean any data has already been persisted.
  virtual int wake_up_for_drive() = 0;
};

// LogAsyncIOWorker 与单个 PALF 异步写状态机之间的边界。worker 负责全局任务
// 接入、ctx 注册和调度；ctx 负责本 PALF 的任务队列、AIO 状态及连续位点发布。
class IAsyncPalfIOCtx
{
public:
  IAsyncPalfIOCtx() {}
  virtual ~IAsyncPalfIOCtx() {}

  // Return the PALF id permanently bound to this context.
  virtual int64_t get_palf_id() const = 0;
  // task 进入 async worker 前预留一个 credit. publish 或 barrier 完成后由 ctx
  // 释放; 只有在所有权尚未进入 ctx 就 dispatch 失败时才由 worker 释放.
  virtual int try_reserve_task_slot(const LogIOTaskType task_type) = 0;
  // Release a reservation that did not enter, or has left, the ctx pipeline.
  virtual void release_task_slot(const LogIOTaskType task_type) = 0;
  // 将 PALF IO task 交给 ctx 的 worker 线程处理. 只有 enqueue 成功才转移所有权.
  virtual int enqueue_task(LogIOTask *task) = 0;
  // Run one non-blocking drive round. next_drive_interval_us limits how long
  // the worker may sleep before the next round: zero means immediately and
  // INT64_MAX means that this ctx has no timer requirement.
  virtual int drive_write(int64_t &next_drive_interval_us) = 0;
  // 在 IOManager callback 线程关闭一次物理 AIO, 并通过出参通知调用方是否
  // 需要唤醒所属 worker 继续 drive.
  virtual int on_aio_complete(const AsyncIOCompletionEvent &event,
                              bool &need_wake_worker) = 0;
  // Request the owning worker to run another drive round from callback context.
  virtual int request_drive() = 0;
  // unregister 排空判据. 返回值覆盖 task credit、planner/fragment、AIO 和
  // 额外生命周期 pin, 只保证零值表示可销毁, 不表示去重后的 task 数量.
  virtual int64_t get_inflight_count() const = 0;
  // Return the oldest positive pending IO start time, or a non-positive value
  // when no timestamp is available.
  virtual int64_t get_oldest_pending_io_start_ts() const = 0;
  // Return an absolute fast_current_time() throttle deadline, or zero if none.
  virtual int64_t get_throttle_next_admit_ts() const = 0;
  // AIO 提交前增加 ctx 引用，callback 销毁时归还；worker 和 producer 访问 map
  // 中的 ctx 时由 AsyncPalfIOCtxEntryGuard 保护，不使用这里的 pin。
  virtual void pin() = 0;
  virtual void unpin() = 0;
  virtual int64_t get_active_ref() const = 0;
  // Report whether all accepted tasks and AIO callbacks have drained.
  virtual bool is_drained() const { return 0 == get_inflight_count(); }
  // Destroy this allocator-owned ctx after unregister confirms it is drained
  // and no callback pin remains.
  virtual void free_this() = 0;
};

} // namespace palf
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_ASYNC_PALF_CTX_INTERFACE_
