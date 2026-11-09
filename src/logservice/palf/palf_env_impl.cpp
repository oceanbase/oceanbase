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

#define USING_LOG_PREFIX PALF
#include "palf_env_impl.h"
#include <stdlib.h>
#include <string.h>
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/oblog/ob_log_module.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/config/ob_server_config.h"
#include "share/ob_errno.h"
#include "share/ob_occam_time_guard.h"
#include "share/ob_debug_sync.h"
#include "share/ob_occam_thread_pool.h"
#include "log_define.h"
#include "palf_handle_impl_guard.h"             // IPalfHandleImplGuard
#include "palf_handle.h"
#include "log_loop_thread.h"
#include "log_rpc.h"
#include "log_block_pool_interface.h"
#include "log_io_utils.h"
#include "share/resource_manager/ob_resource_manager.h"       // ObResourceManager
#include "share/io/ob_io_manager.h"                           // ObIOManager
#include "observer/omt/ob_tenant_config_mgr.h"                // TENANT_CONF

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
namespace
{
int64_t calculate_disk_space_threshold(
    const int64_t total_disk_space,
    const int64_t threshold_percentage)
{
  return total_disk_space / 100 * threshold_percentage
      + total_disk_space % 100 * threshold_percentage / 100;
}
} // namespace

PalfHandleImpl *PalfHandleImplFactory::alloc()
{
  return MTL_NEW(PalfHandleImpl, "palf_env");
}

void PalfHandleImplFactory::free(IPalfHandleImpl *palf_handle_impl)
{
  MTL_DELETE(IPalfHandleImpl, "palf_env", palf_handle_impl);
}

PalfHandleImpl *PalfHandleImplAlloc::alloc_value()
{
  return NULL;
}

void PalfHandleImplAlloc::free_value(IPalfHandleImpl *palf_handle_impl)
{
  PalfHandleImplFactory::free(palf_handle_impl);
  palf_handle_impl = NULL;
}

PalfHandleImplAlloc::Node *PalfHandleImplAlloc::alloc_node(IPalfHandleImpl *palf_handle_impl)
{
  UNUSED(palf_handle_impl);
  return op_reclaim_alloc(Node);
}

void PalfHandleImplAlloc::free_node(PalfHandleImplAlloc::Node *node)
{
  op_reclaim_free(node);
  node = NULL;
}

int PalfDiskOptionsWrapper::init(const PalfDiskOptions &disk_opts)
{
  int ret = OB_SUCCESS;
  if (false == disk_opts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    disk_opts_for_recycling_blocks_ = disk_opts_for_stopping_writing_ = disk_opts;
    status_ = Status::NORMAL_STATUS;
    cur_unrecyclable_log_disk_size_ = 0;
    sequence_ = 0;
  }
  return ret;
}

void PalfDiskOptionsWrapper::reset()
{
  ObSpinLockGuard guard(disk_opts_lock_);
  disk_opts_for_recycling_blocks_.reset();
  disk_opts_for_stopping_writing_.reset();
  status_ = Status::INVALID_STATUS;
  cur_unrecyclable_log_disk_size_ = -1;
  ATOMIC_STORE(&gc_stalled_, false);
  sequence_ = -1;
}

int PalfDiskOptionsWrapper::update_disk_options(const PalfDiskOptions &disk_opts_for_recycling_blocks)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(disk_opts_lock_);
  return update_disk_options_not_guarded_by_lock_(disk_opts_for_recycling_blocks);
}

void PalfDiskOptionsWrapper::set_cur_unrecyclable_log_disk_size(const int64_t unrecyclable_log_disk_size)
{
  ObSpinLockGuard guard(disk_opts_lock_);
  cur_unrecyclable_log_disk_size_ = unrecyclable_log_disk_size;
  ATOMIC_STORE(&gc_stalled_, false);
}

// 算法概览：U 为真实不可回收量，d 为超过 GC 宽限期后的卡顿时长。
// 无有效卡顿时限流压力 E=U；卡顿时按日志盘额度计算限流线 A 和停写线 B，
// 取 U0=min(B, max(U, A+1))，再按 E=U0+(B-U0)*min(d,T)/T 线性增加压力。
// E>A 时复用现有 IO 限流，E>=B 或 d>=最大限流时长 T 时停止接收新日志。
// GC 恢复后 E 回到最新 U；E 只影响限流，真实容量判断仍使用实际磁盘用量。
int PalfDiskOptionsWrapper::try_update_throttling_disk_usage(
    const int64_t unrecyclable_size,
    const int64_t stalled_duration_us,
    bool &need_throttling,
    bool &gc_stall_stopped)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(disk_opts_lock_.trylock())) {
    PALF_LOG(TRACE, "failed to lock disk options", K(ret));
  } else {
    const PalfDiskOptions &opts = disk_opts_for_stopping_writing_;
    const int64_t trigger = opts.log_disk_throttling_percentage_;
    const int64_t stop = opts.log_disk_utilization_limit_threshold_;
    // 默认使用真实不可回收量 U；GC 恢复或不满足卡顿限流条件时，撤销附加压力。
    cur_unrecyclable_log_disk_size_ = unrecyclable_size;
    gc_stall_stopped = false;
    // 配置有效、限流阈值低于停写阈值，且 GC 无进展已达到宽限期后，才启用卡顿限流。
    const bool gc_stalled = unrecyclable_size >= 0 && opts.is_valid() && opts.log_disk_usage_limit_size_ > 0
        && trigger < stop && stalled_duration_us >= 0;
    if (gc_stalled) {
      // 用租户日志盘额度乘以限流/停写百分比，得到以字节为单位的两条阈值线。
      const int64_t trigger_size = opts.log_disk_usage_limit_size_ * trigger / 100;
      const int64_t stop_size = opts.log_disk_usage_limit_size_ * stop / 100;
      // 每轮用最新 U 计算起点：start_size = min(stop_size, max(U, trigger_size + 1))。
      // +1 保证 U 较低时也能越过限流触发线；卡顿期间新增的不可回收日志也会计入起点。
      const int64_t start_size = MIN(stop_size, MAX(unrecyclable_size, trigger_size + 1));
      const int64_t maximum_duration_us = opts.log_disk_throttling_maximum_duration_;
      // 令 d = stalled_duration_us，T = maximum_duration_us，单位均为微秒。
      // 限流用量 E = start_size + (stop_size - start_size) * min(d, T) / T。
      // 固定本轮 U 时，E 随卡顿时长线性逼近停写线，再交给现有 IO 限流公式计算等待时间。
      // E 仅用于限流，实际磁盘用量和容量判断仍使用真实值。
      // Use a floating-point ratio to avoid overflowing the capacity-duration product.
      const double stalled_ratio = static_cast<double>(MIN(stalled_duration_us, maximum_duration_us))
          / static_cast<double>(maximum_duration_us);
      cur_unrecyclable_log_disk_size_ = start_size
          + static_cast<int64_t>(static_cast<double>(stop_size - start_size) * stalled_ratio);
      // E 到达停写线或卡顿满 T 时，关闭租户新日志入口，防止只限速仍持续积压。
      gc_stall_stopped = cur_unrecyclable_log_disk_size_ >= stop_size
          || stalled_duration_us >= maximum_duration_us;
    }
    ATOMIC_STORE(&gc_stalled_, gc_stalled);
    need_throttling = opts.is_valid()
        && cur_unrecyclable_log_disk_size_ > opts.log_disk_usage_limit_size_ * trigger / 100;
    disk_opts_lock_.unlock();
  }
  return ret;
}

bool PalfDiskOptionsWrapper::need_throttling() const
{
  bool is_need = false;
  ObSpinLockGuard guard(disk_opts_lock_);
  const int64_t trigger_size = disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ * disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ / 100;
  return disk_opts_for_stopping_writing_.is_valid() && cur_unrecyclable_log_disk_size_ > trigger_size;
}

int PalfDiskOptionsWrapper::check_log_disk_under_pressure(
    bool &log_disk_under_pressure) const
{
  int ret = OB_SUCCESS;
  log_disk_under_pressure = false;
  ObSpinLockGuard guard(disk_opts_lock_);
  const PalfDiskOptions &disk_options = disk_opts_for_recycling_blocks_;
  if (OB_UNLIKELY(!disk_options.is_valid()
      || cur_unrecyclable_log_disk_size_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "invalid palf disk pressure options", K(ret),
        K(disk_options), K(cur_unrecyclable_log_disk_size_));
  } else {
    const int64_t total_disk_space = disk_options.log_disk_usage_limit_size_;
    const int64_t gc_trigger_percentage = disk_options.log_disk_utilization_threshold_;
    // Match the floor division and strict comparison used by try_recycle_blocks().
    const int64_t gc_trigger_size = calculate_disk_space_threshold(
        total_disk_space, gc_trigger_percentage);
    log_disk_under_pressure = cur_unrecyclable_log_disk_size_ > gc_trigger_size;
  }
  return ret;
}

// Concurrent analysis
// BlockGCThread                                                                     ConfigChangeThread
// T1  get_disk_options
//                                                                                   T2  shrink log_disk when status is SHRINKING_STATUS,
//                                                                                       make disk_opts_for_recycling_blocks to new PalfDiskOptions.
// T3  change disk_opts_for_stopping_writing for disk_opts_for_recycling_blocks
//     and make status to NORMAL_STATUS
// This will cause write-stop, therefore, we only change status to NORMAL when sequence is same.
// And we only update sequence when PalfDiskOptions has change.
void PalfDiskOptionsWrapper::change_to_normal(const int64_t sequence)
{
  ObSpinLockGuard guard(disk_opts_lock_);
  if (sequence_ == sequence && Status::SHRINKING_STATUS == status_)  {
    status_ = Status::NORMAL_STATUS;
    disk_opts_for_stopping_writing_ = disk_opts_for_recycling_blocks_;
    PALF_LOG(INFO, "change_to_normal", KPC(this));
  } else {
    PALF_LOG(INFO, "sequence has changed or status not match", KPC(this), K(sequence));
  }
}

int PalfDiskOptionsWrapper::update_disk_options_not_guarded_by_lock_(const PalfDiskOptions &disk_opts_for_recycling_blocks)
{
  int ret = OB_SUCCESS;
  int64_t curr_stop_write_limit_size =
    disk_opts_for_stopping_writing_.log_disk_usage_limit_size_;
  int64_t next_stop_write_limit_size =
    disk_opts_for_recycling_blocks.log_disk_usage_limit_size_;
  if (false == disk_opts_for_recycling_blocks.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (disk_opts_for_recycling_blocks_ == disk_opts_for_recycling_blocks) {
    PALF_LOG(INFO, "no need update disk options", K(ret), K(disk_opts_for_recycling_blocks_), K(disk_opts_for_recycling_blocks));
  } else {
    if (curr_stop_write_limit_size > next_stop_write_limit_size) {
      status_ = Status::SHRINKING_STATUS;
      // In process of shrinking, to avoid stopping writing,
      // 'disk_opts_for_stopping_writing_' is still an original value, update it
      // with 'disk_opts_for_recycling_blocks' until there is no possibility
      // caused stopping writing.
      disk_opts_for_recycling_blocks_ = disk_opts_for_recycling_blocks;
      PALF_LOG(INFO, "shrink log disk success", K(curr_stop_write_limit_size), K(next_stop_write_limit_size),
               KPC(this));
    } else {
      status_ = Status::NORMAL_STATUS;
      disk_opts_for_recycling_blocks_ = disk_opts_for_stopping_writing_ = disk_opts_for_recycling_blocks;
      PALF_LOG(INFO, "expand log disk success", K(curr_stop_write_limit_size), K(next_stop_write_limit_size),
               KPC(this));
    }
    //always update writing_throttling_trigger_percentage_
    const int64_t new_trigger_percentage = disk_opts_for_recycling_blocks.log_disk_throttling_percentage_;
    const int64_t new_maximum_duration = disk_opts_for_recycling_blocks.log_disk_throttling_maximum_duration_;
    disk_opts_for_recycling_blocks_.log_disk_throttling_percentage_ = new_trigger_percentage;
    disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = new_trigger_percentage;
    disk_opts_for_recycling_blocks_.log_disk_throttling_maximum_duration_ = new_maximum_duration;
    disk_opts_for_stopping_writing_.log_disk_throttling_maximum_duration_ = new_maximum_duration;
    sequence_++;
  }
  return ret;
}

PalfEnvImpl::PalfEnvImpl() : palf_meta_lock_(common::ObLatchIds::PALF_ENV_LOCK),
                             log_alloc_mgr_(NULL),
                             log_block_pool_(NULL),
                             log_block_reclaim_coordinator_(NULL),
                             fetch_log_engine_(),
                             log_rpc_(),
                             cb_thread_pool_(),
                             log_io_worker_wrapper_(),
                             log_shared_queue_th_(),
                             block_gc_timer_task_(),
                             log_updater_(),
                             monitor_(NULL),
                             disk_options_wrapper_(),
                             tenant_reclaim_service_(),
                             throttling_update_lock_(common::ObLatchIds::PALF_ENV_LOCK),
                             disk_not_enough_print_interval_in_gc_thread_(OB_INVALID_TIMESTAMP),
                             disk_not_enough_print_interval_in_loop_thread_(OB_INVALID_TIMESTAMP),
                             self_(),
                             palf_handle_impl_map_(64),  // 指定min_size=64
                             last_palf_epoch_(0),
                             rebuild_replica_log_lag_threshold_(0),
                             diskspace_enough_(true),
                             gc_stall_stopped_(false),
                             tenant_id_(0),
                             io_adapter_(),
                             is_inited_(false),
                             is_running_(false)
{
  log_dir_[0] = '\0';
  tmp_log_dir_[0] = '\0';
}

PalfEnvImpl::~PalfEnvImpl()
{
  destroy();
}

int PalfEnvImpl::init(
    const PalfOptions &options,
    const char *base_dir, const ObAddr &self,
    const int64_t cluster_id,
    const int64_t tenant_id,
    rpc::frame::ObReqTransport *transport,
    obrpc::ObBatchRpc *batch_rpc,
    common::ObILogAllocator *log_alloc_mgr,
    ILogBlockPool *log_block_pool,
    ILogBlockReclaimCoordinator *log_block_reclaim_coordinator,
    PalfMonitorCb *monitor,
    ObResourceManager *resource_manager,
    ObIOManager *io_manager)
{
  int ret = OB_SUCCESS;
  const int64_t io_cb_num = PALF_SLIDING_WINDOW_SIZE * 128;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "PalfEnvImpl is inited twiced", K(ret));
  } else if (OB_ISNULL(base_dir) || !self.is_valid() || NULL == transport || NULL == batch_rpc
             || OB_ISNULL(log_alloc_mgr) || OB_ISNULL(log_block_pool)
             || OB_ISNULL(log_block_reclaim_coordinator) || OB_ISNULL(monitor)
             || OB_ISNULL(resource_manager) || OB_ISNULL(io_manager)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid arguments", K(ret), KP(transport), KP(batch_rpc), K(base_dir), K(self), KP(transport),
             KP(log_alloc_mgr), KP(log_block_pool), KP(log_block_reclaim_coordinator), KP(monitor),
             KP(resource_manager), KP(io_manager));
  } else if (OB_FAIL(init_log_io_worker_config_(options.disk_options_.log_writer_parallelism_,
                                                tenant_id,
                                                log_io_worker_config_))) {
    PALF_LOG(WARN, "init_log_io_worker_config_ failed", K(options));
  } else if (OB_FAIL(fetch_log_engine_.init(this, log_alloc_mgr))) {
    PALF_LOG(ERROR, "FetchLogEngine init failed", K(ret));
  } else if (OB_FAIL(log_rpc_.init(self, cluster_id, tenant_id, transport, batch_rpc))) {
    PALF_LOG(ERROR, "LogRpc init failed", K(ret));
  } else if (OB_FAIL(cb_thread_pool_.init(io_cb_num, this))) {
    PALF_LOG(ERROR, "LogIOTaskThreadPool init failed", K(ret));
  } else if (OB_FAIL(log_io_worker_wrapper_.init(log_io_worker_config_,
                                                 tenant_id,
                                                 cb_thread_pool_.get_tg_id(),
                                                 log_alloc_mgr, this))) {
    PALF_LOG(ERROR, "LogIOWorker init failed", K(ret));
  } else if (OB_FAIL(log_shared_queue_th_.init(this))) {
    PALF_LOG(ERROR, "LogSharedQueueTh init failed", K(ret));
  } else if (OB_FAIL(block_gc_timer_task_.init(this))) {
    PALF_LOG(ERROR, "ObCheckLogBlockCollectTask init failed", K(ret));
  } else if (OB_FAIL(databuff_printf(log_dir_, MAX_PATH_SIZE, "%s", base_dir))) {
    PALF_LOG(ERROR, "construct log directory failed", KR(ret), K(base_dir));
  } else if (OB_FAIL(databuff_printf(tmp_log_dir_, MAX_PATH_SIZE, "%s/tmp_dir", log_dir_))) {
    PALF_LOG(ERROR, "construct temporary log directory failed", KR(ret), K(log_dir_));
  } else if (OB_FAIL(palf_handle_impl_map_.init("LOG_HASH_MAP", tenant_id))) {
    PALF_LOG(ERROR, "palf_handle_impl_map_ init failed", K(ret));
  } else if (OB_FAIL(log_loop_thread_.init(this, self))) {
    PALF_LOG(ERROR, "log_loop_thread_ init failed", K(ret));
  } else if (OB_FAIL(
                 election_timer_.init_and_start(1, 10_ms, "ElectTimer"))) { // just one worker thread
    PALF_LOG(ERROR, "election_timer_ init failed", K(ret));
  } else if (OB_FAIL(election::GLOBAL_INIT_ELECTION_MODULE())) {
    PALF_LOG(ERROR, "global init election module failed", K(ret));
  } else if (OB_FAIL(disk_options_wrapper_.init(options.disk_options_))) {
    PALF_LOG(ERROR, "disk_options_wrapper_ init failed", K(ret));
  } else if (OB_FAIL(log_updater_.init(this))) {
    PALF_LOG(ERROR, "LogUpdater init failed", K(ret));
  } else if (OB_FAIL(io_adapter_.init(tenant_id, resource_manager, io_manager))) {
    PALF_LOG(ERROR, "LogIOAdapter init failed", K(ret));
  } else if (OB_FAIL(tenant_reclaim_service_.init(/* shared_reclaim_enabled */
                 LogReclaimState::DISABLED != log_block_reclaim_coordinator->get_reclaim_state()))) {
    PALF_LOG(ERROR, "TenantLogBlockReclaimService init failed", KR(ret), K(tenant_id));
  } else {
    log_alloc_mgr_ = log_alloc_mgr;
    log_block_pool_ = log_block_pool;
    log_block_reclaim_coordinator_ = log_block_reclaim_coordinator;
    monitor_ = monitor;
    self_ = self;
    tenant_id_ = tenant_id;
    is_inited_ = true;
    is_running_ = true;
    PALF_LOG(INFO, "PalfEnvImpl init success", K(ret), K(self_), KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int PalfEnvImpl::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(scan_all_palf_handle_impl_director_())) {
    PALF_LOG(WARN, "scan_all_palf_handle_impl_director_ failed", K(ret));
  } else if (OB_FAIL(cb_thread_pool_.start())) {
    PALF_LOG(ERROR, "LogIOTaskThreadPool start failed", K(ret));
  } else if (OB_FAIL(log_io_worker_wrapper_.start())) {
    PALF_LOG(ERROR, "LogIOWorker start failed", K(ret));
  } else if (OB_FAIL(log_shared_queue_th_.start())) {
    PALF_LOG(ERROR, "LogIOWorker start failed", K(ret));
  } else if (OB_FAIL(block_gc_timer_task_.start())) {
    PALF_LOG(ERROR, "FileCollectTimerTask start failed", K(ret));
	} else if (OB_FAIL(fetch_log_engine_.start())) {
    PALF_LOG(ERROR, "FetchLogEngine start failed", K(ret));
  } else if (OB_FAIL(log_loop_thread_.start())) {
    PALF_LOG(ERROR, "log_loop_thread_ start failed", K(ret));
  } else if (OB_FAIL(log_updater_.start())) {
    PALF_LOG(ERROR, "LogUpdater start failed", K(ret));
  } else {
    is_running_ = true;
    PALF_LOG(INFO, "PalfEnv start success", K(ret));
  }
  return ret;
}

void PalfEnvImpl::stop()
{
  if (is_running_) {
    PALF_LOG(INFO, "PalfEnvImpl begin stop", KPC(this));
    is_running_ = false;
    log_io_worker_wrapper_.stop();
    log_shared_queue_th_.stop();
    cb_thread_pool_.stop();
    block_gc_timer_task_.stop();
    fetch_log_engine_.stop();
    log_loop_thread_.stop();
    log_updater_.stop();
    PALF_LOG(INFO, "PalfEnvImpl stop success", KPC(this));
  }
}

void PalfEnvImpl::wait()
{
  PALF_LOG(INFO, "PalfEnvImpl begin wait", KPC(this));
  log_io_worker_wrapper_.wait();
  log_shared_queue_th_.wait();
  cb_thread_pool_.wait();
  block_gc_timer_task_.wait();
  fetch_log_engine_.wait();
  log_loop_thread_.wait();
  log_updater_.wait();
  PALF_LOG(INFO, "PalfEnvImpl wait success", KPC(this));
}

void PalfEnvImpl::destroy()
{
  PALF_LOG_RET(WARN, OB_SUCCESS, "PalfEnvImpl destroy", KPC(this));
  is_running_ = false;
  is_inited_ = false;
  palf_handle_impl_map_.destroy();
  log_io_worker_wrapper_.destroy();
  log_shared_queue_th_.destroy();
  cb_thread_pool_.destroy();
  log_loop_thread_.destroy();
  block_gc_timer_task_.destroy();
  fetch_log_engine_.destroy();
  log_updater_.destroy();
  log_rpc_.destroy();
  election_timer_.destroy();
  tenant_reclaim_service_.reset();
  log_alloc_mgr_ = NULL;
  monitor_ = NULL;
  disk_not_enough_print_interval_in_gc_thread_ = OB_INVALID_TIMESTAMP;
  disk_not_enough_print_interval_in_loop_thread_ = OB_INVALID_TIMESTAMP;
  self_.reset();
  log_dir_[0] = '\0';
  tmp_log_dir_[0] = '\0';
  disk_options_wrapper_.reset();
  gc_stall_stopped_ = false;
  rebuild_replica_log_lag_threshold_ = 0;
  io_adapter_.destroy();
}

int PalfEnvImpl::create_palf_handle_impl(const int64_t palf_id,
                                         const AccessMode &access_mode,
                                         const PalfBaseInfo &palf_base_info,
                                         const LogReplicaType replica_type,
                                         IPalfHandleImpl *&palf_handle_impl)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(palf_meta_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfEnvImpl is not inited", K(ret));
  } else if (OB_FAIL(create_palf_handle_impl_(palf_id, access_mode, palf_base_info,
          replica_type, palf_handle_impl))) {
    palf_handle_impl = NULL;
  } else {
    PALF_LOG(INFO, "PalfEnvImpl create_palf_handle_impl finished", K(ret), K(palf_id), K(access_mode),
        K(palf_base_info), K(replica_type), KPC(this));
  }
  return ret;
}

int PalfEnvImpl::create_palf_handle_impl_(const int64_t palf_id,
                                          const AccessMode &access_mode,
                                          const PalfBaseInfo &palf_base_info,
                                          const LogReplicaType replica_type,
                                          IPalfHandleImpl *&ipalf_handle_impl)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char base_dir[MAX_PATH_SIZE] = {'\0'};
  PalfHandleImpl *palf_handle_impl = NULL;
  LSKey hash_map_key(palf_id);
  const int64_t palf_epoch = ATOMIC_AAF(&last_palf_epoch_, 1);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfEnvImpl is not running", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    PALF_LOG(WARN, "PalfEnvImpl is not running", K(ret));
  } else if (false == is_valid_palf_id(palf_id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K(palf_id));
  } else if (OB_ENTRY_EXIST == palf_handle_impl_map_.contains_key(hash_map_key)) {
    ret = OB_ENTRY_EXIST;
    PALF_LOG(WARN, "palf_handle has exist, ignore this request", K(ret), K(palf_id));
  } else if (false == check_can_create_palf_handle_impl_()) {
    ret = OB_LOG_OUTOF_DISK_SPACE;
    PALF_LOG(WARN, "PalfEnv can not hold more instance", K(ret), KPC(this), K(palf_id));
  } else if (0 > (pret = snprintf(base_dir, MAX_PATH_SIZE, "%s/%ld", log_dir_, palf_id))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprinf failed", K(pret), K(palf_id));
  // Note:: order is vital, allocate memory may be fail
  } else if (NULL == (palf_handle_impl = PalfHandleImplFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "alloc palf_handle_impl failed", K(ret));
  } else if (OB_FAIL(create_directory(base_dir))) {
    PALF_LOG(WARN, "prepare_directory_for_creating_ls failed!!!", K(ret), K(palf_id));
  } else if (OB_FAIL(palf_handle_impl->init(palf_id, access_mode, palf_base_info, replica_type,
      &fetch_log_engine_, base_dir, log_alloc_mgr_, log_block_pool_, &log_rpc_,
      log_io_worker_wrapper_.get_log_io_worker(palf_id), &log_shared_queue_th_, this,
      self_, &election_timer_, palf_epoch, &io_adapter_))) {
    PALF_LOG(ERROR, "IPalfHandleImpl init failed", K(ret), K(palf_id));
    // NB: always insert value into hash map finally.
  } else if (OB_FAIL(palf_handle_impl_map_.insert_and_get(hash_map_key, palf_handle_impl))) {
    PALF_LOG(WARN, "palf_handle_impl_map_ insert_and_get failed", K(ret), K(palf_id));
  } else {
    (void) palf_handle_impl->set_monitor_cb(monitor_);
    palf_handle_impl->set_scan_disk_log_finished();
    ipalf_handle_impl = palf_handle_impl;
  }

  if (OB_FAIL(ret) && NULL != palf_handle_impl) {
    // if 'palf_handle_impl' has not been inserted into hash map,
    // need reclaim manually.
    PalfHandleImplFactory::free(palf_handle_impl);
    palf_handle_impl = NULL;
    if (OB_ENTRY_NOT_EXIST == palf_handle_impl_map_.contains_key(hash_map_key)) {
      remove_directory_while_exist_(base_dir);
    }
  }

  PALF_LOG(INFO, "PalfEnvImpl create_palf_handle_impl_ finished", K(ret), K(palf_id),
      K(access_mode), K(palf_base_info), K(replica_type), KPC(this));

  return ret;
}

int PalfEnvImpl::remove_palf_handle_impl(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(palf_meta_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfEnvImpl is not inited", K(ret));
  } else if (OB_FAIL(remove_palf_handle_impl_from_map_not_guarded_by_lock_(palf_id))) {
    PALF_LOG(WARN, "palf instance not exist", K(ret), KPC(this), K(palf_id));
  } else if (OB_FAIL(wait_until_reference_count_to_zero_(palf_id))) {
    PALF_LOG(WARN, "wait_until_reference_count_to_zero_ failed", K(ret), KPC(this), K(palf_id));
  } else {
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int PalfEnvImpl::get_palf_handle_impl(const int64_t palf_id,
                                      IPalfHandleImplGuard &palf_handle_impl_guard)
{
  int ret = OB_SUCCESS;
  IPalfHandleImpl *palf_handle_impl = NULL;
  if (OB_FAIL(get_palf_handle_impl(palf_id, palf_handle_impl))) {
    PALF_LOG(TRACE, "get_palf_handle_impl failed", K(ret), K(palf_id));
  } else {
    palf_handle_impl_guard.palf_env_impl_ = this;
    palf_handle_impl_guard.palf_handle_impl_ = palf_handle_impl;
    palf_handle_impl_guard.palf_id_ = palf_id;
    PALF_LOG(TRACE, "get_palf_handle_impl success", K(palf_id), K(palf_handle_impl_guard));
    // do nothing
  }
  return ret;
}

int PalfEnvImpl::get_palf_handle_impl(const int64_t palf_id,
                                      IPalfHandleImpl *&ipalf_handle_impl)
{
  int ret = OB_SUCCESS;
  LSKey hash_map_key(palf_id);
  IPalfHandleImpl *palf_handle_impl = NULL;
  if (false == is_valid_palf_id(palf_id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id));
  } else if (OB_FAIL(palf_handle_impl_map_.get(hash_map_key, palf_handle_impl))) {
    PALF_LOG(TRACE, "get from map failed", K(ret), K(palf_id), K(palf_handle_impl));
  } else if (false == palf_handle_impl->check_can_be_used()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ipalf_handle_impl = palf_handle_impl;
  }

  if (OB_FAIL(ret) && NULL != palf_handle_impl) {
    revert_palf_handle_impl(palf_handle_impl);
  }
  return ret;
}

void PalfEnvImpl::revert_palf_handle_impl(IPalfHandleImpl *ipalf_handle_impl)
{
  if (NULL != ipalf_handle_impl) {
    palf_handle_impl_map_.revert(ipalf_handle_impl);
  }
}

int PalfEnvImpl::scan_all_palf_handle_impl_director_()
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("PalfEnvImplStart", 0);
  ReloadPalfHandleImplFunctor functor(this);
  if (OB_FAIL(scan_dir(log_dir_, functor))) {
    PALF_LOG(WARN, "scan_dir failed", K(ret));
  } else {
    guard.click("scan_dir");
    PALF_LOG(INFO, "scan_all_palf_handle_impl_director_ success", K(ret), K(log_dir_), K(guard));
  }
  return ret;
}

int PalfEnvImpl::create_directory(const char *base_dir)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char tmp_base_dir[MAX_PATH_SIZE] = {'\0'};
  char log_dir[MAX_PATH_SIZE] = {'\0'};
  char meta_dir[MAX_PATH_SIZE] = {'\0'};
  if (0 > (pret = snprintf(tmp_base_dir, MAX_PATH_SIZE, "%s%s", base_dir, TMP_SUFFIX))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprinf failed", K(pret), K(base_dir));
  } else if (0 > (pret = snprintf(log_dir, MAX_PATH_SIZE, "%s/log", tmp_base_dir))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprinf failed", K(pret), K(base_dir));
  } else if (0 > (pret = snprintf(meta_dir, MAX_PATH_SIZE, "%s/meta", tmp_base_dir))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprinf failed", K(pret), K(base_dir));
  } else if (OB_FAIL(mkdir(tmp_base_dir))) {
    PALF_LOG(WARN, "mkdir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (OB_FAIL(mkdir(log_dir))) {
    PALF_LOG(WARN, "mkdir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (OB_FAIL(mkdir(meta_dir))) {
    PALF_LOG(WARN, "mkdir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (OB_FAIL(rename_with_retry(tmp_base_dir, base_dir))) {
    PALF_LOG(ERROR, "rename tmp dir to normal dir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (OB_FAIL(fsync_with_retry(log_dir_))) {
    PALF_LOG(ERROR, "fsync_dir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else {
    PALF_LOG(INFO, "prepare_directory_for_creating_ls success", K(ret), K(base_dir));
  }
  if (OB_FAIL(ret)) {
    remove_directory_while_exist_(tmp_base_dir);
    remove_directory_while_exist_(base_dir);
  }
  return ret;
}

// step:
// 1. rename log directory to tmp directory.
// 2. delete tmp directory.
// NB: '%s.tmp' is invalid block or invalid directory, before the restart phase of PalfEnvImpl,
//     need delete these tmp block or directory.
int PalfEnvImpl::remove_directory(const char *log_dir)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char tmp_log_dir[MAX_PATH_SIZE] = {'\0'};
  if (0 > (pret = snprintf(tmp_log_dir, MAX_PATH_SIZE, "%s%s", log_dir, TMP_SUFFIX))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprintf failed", K(ret), K(pret), K(log_dir), K(tmp_log_dir));
  } else if (OB_FAIL(rename_with_retry(log_dir, tmp_log_dir))) {
    PALF_LOG(WARN, "rename log dir to tmp dir failed", K(ret), K(errno), K(tmp_log_dir), K(log_dir));
  } else {
    bool result = true;
    do {
      if (OB_FAIL(is_exists(tmp_log_dir, result))) {
        PALF_LOG(WARN, "check directory exists failed", KPC(this), K(log_dir));
      } else if (!result) {
        PALF_LOG(WARN, "directory not exists", KPC(this), K(log_dir));
        break;
      } else if (OB_FAIL(remove_directory_rec(tmp_log_dir, log_block_pool_))) {
        PALF_LOG(WARN, "remove_directory_rec failed", K(tmp_log_dir), KP(log_block_pool_));
      } else {
      }
      if (OB_FAIL(ret) && true == result) {
        PALF_LOG(WARN, "remove directory failed, may be physical disk full", K(ret), KPC(this));
        ob_usleep(100*1000);
      }
    } while (OB_FAIL(ret));
  }
  (void)fsync_with_retry(log_dir_);
  PALF_LOG(WARN, "remove_directory finished", KR(ret), K(log_dir), KP(this));
  return ret;
}

PalfEnvImpl::LogGetRecycableFileCandidate::LogGetRecycableFileCandidate()
  : id_(-1),
    min_block_id_(LOG_INVALID_BLOCK_ID),
    min_block_max_scn_(),
    min_using_block_id_(LOG_INVALID_BLOCK_ID),
    oldest_palf_id_(INVALID_PALF_ID),
    oldest_block_scn_(),
    retention_scn_(),
    retention_expired_blocks_(0),
    retention_unexpired_blocks_(0),
    ret_code_(OB_SUCCESS)
{}

PalfEnvImpl::LogGetRecycableFileCandidate::LogGetRecycableFileCandidate(
    const SCN &retention_scn)
  : LogGetRecycableFileCandidate()
{
  retention_scn_ = retention_scn;
}

PalfEnvImpl::LogGetRecycableFileCandidate::~LogGetRecycableFileCandidate()
{
  ret_code_ = OB_SUCCESS;
  min_using_block_id_ = LOG_INVALID_BLOCK_ID;
  min_block_max_scn_.reset();
  min_block_id_ = LOG_INVALID_BLOCK_ID;
  oldest_palf_id_ = INVALID_PALF_ID;
  oldest_block_scn_.reset();
  retention_scn_.reset();
  retention_expired_blocks_ = 0;
  retention_unexpired_blocks_ = 0;
  id_ = -1;
}

bool PalfEnvImpl::LogGetRecycableFileCandidate::operator()(const LSKey &palf_id, IPalfHandleImpl *palf_handle_impl)
{
  bool bool_ret = true;
  if (NULL == palf_handle_impl) {
    ret_code_ = OB_ERR_UNEXPECTED;
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "the value in hashmap is NULL, unexpected error", K(palf_id), KP(palf_handle_impl));
    bool_ret = false;
  } else {
    int ret = OB_SUCCESS;
    const LSN base_lsn = palf_handle_impl->get_base_lsn_used_for_block_gc();
    const block_id_t min_using_block_id = lsn_2_block(base_lsn, PALF_BLOCK_SIZE);
    block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
    SCN min_block_max_scn;
    // OB_ENTRY_NOT_EXIST means there is not any block;
    // OB_NO_SUCH_FILE_OR_DIRECTORY means there is concurrently with rebuild.
    // OB_ERR_OUT_OF_UPPER_BOUND means there is one block
    auto need_skip_by_ret = [](const int ret ){
      return OB_ENTRY_NOT_EXIST == ret  || OB_NO_SUCH_FILE_OR_DIRECTORY == ret
          || OB_ERR_OUT_OF_UPPER_BOUND == ret;
    };
    if (false == base_lsn.is_valid()) {
      PALF_LOG(WARN, "base_lsn is invalid", K(base_lsn), KPC(palf_handle_impl));
    } else if (OB_FAIL(palf_handle_impl->get_min_block_info_for_gc(min_block_id, min_block_max_scn))
               && !need_skip_by_ret(ret)) {
      ret_code_ = ret;
      bool_ret = false;
      PALF_LOG(WARN, "LogGetRecycableFileCandidate get_min_block_info_for_gc failed", K(ret), K(palf_id));
      // recycable conditions:
      // 1. current palf_handle_impl must have some block can be recycable;
      // 2. current palf_handle_impl must have older blocks(at least two blocks).
      // Always keep there are at least two blocks in range [begin_lsn, base_lsn], because for restart, we will read
      // first uncommitted log before base_lsn.
    } else if (need_skip_by_ret(ret)
               || min_using_block_id < min_block_id
               || min_using_block_id - min_block_id < 2) {
      PALF_LOG(TRACE, "can not recycle blocks, need keep at least two blocks or has been concurrently"
          " with rebuild, skip it",
          K(ret), KPC(palf_handle_impl), K(min_block_id), K(min_using_block_id));
    } else {
      if (!min_block_max_scn_.is_valid() || min_block_max_scn_ >= min_block_max_scn) {
        id_ = palf_id.id_;
        min_block_id_ = min_block_id;
        min_block_max_scn_ = min_block_max_scn;
        min_using_block_id_ = min_using_block_id;
        PALF_LOG(TRACE, "can be recycable palf_handle_impl", K(id_), K(min_block_id_), K(min_using_block_id_),
            K(min_block_max_scn_), K(base_lsn));
      }
      if (retention_scn_.is_valid()
          && OB_FAIL(collect_reclaim_blocks_(*palf_handle_impl, min_block_id, min_using_block_id))) {
        ret_code_ = ret;
        bool_ret = false;
        PALF_LOG(WARN, "collect retention reclaim blocks failed", KR(ret), K(palf_id));
      }
    }
    if (min_block_max_scn.is_valid() && (!oldest_block_scn_.is_valid() || oldest_block_scn_ > min_block_max_scn)) {
      oldest_block_scn_ = min_block_max_scn;
      oldest_palf_id_ = palf_id.id_;
    }
  }
  return bool_ret;
}

ERRSIM_POINT_DEF(ERRSIM_PALF_RETENTION_LOCATE_ERROR,
                 "Inject an error while locating the log retention boundary.");
int PalfEnvImpl::LogGetRecycableFileCandidate::collect_reclaim_blocks_(
    IPalfHandleImpl &palf_handle_impl,
    const block_id_t min_block_id,
    const block_id_t min_using_block_id)
{
  int ret = OB_SUCCESS;
  int64_t candidate_blocks = min_using_block_id - min_block_id - 1;
  int64_t retention_expired_blocks = 0;
  LSN retention_lsn;
  const int errsim_ret = ERRSIM_PALF_RETENTION_LOCATE_ERROR;
  if (OB_UNLIKELY(OB_SUCCESS != errsim_ret)) {
    ret = errsim_ret;
  } else {
    ret = palf_handle_impl.locate_by_scn_coarsely(retention_scn_, retention_lsn);
  }
  if (OB_SUCCESS != ret) {
    if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      // The retention boundary is older than all remaining logs.
      ret = OB_SUCCESS;
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // The LS no longer has logs; discard the earlier candidate count.
      candidate_blocks = 0;
      ret = OB_SUCCESS;
    } else {
      PALF_LOG(WARN, "locate retention boundary failed", KR(ret), K(retention_scn_),
               K(min_block_id), K(min_using_block_id));
    }
  } else if (!retention_lsn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "invalid retention lsn", KR(ret), K(retention_scn_), K(retention_lsn));
  } else {
    const block_id_t retention_block_id = lsn_2_block(retention_lsn, PALF_BLOCK_SIZE);
    const block_id_t candidate_end_block_id = min_using_block_id - 1;
    const block_id_t retention_expired_end_block_id =
        common::min(candidate_end_block_id, retention_block_id);
    // Keep the block containing the retention SCN; the end block is exclusive.
    retention_expired_blocks = retention_expired_end_block_id > min_block_id
        ? retention_expired_end_block_id - min_block_id : 0;
  }
  if (OB_SUCC(ret)) {
    retention_expired_blocks_ += retention_expired_blocks;
    retention_unexpired_blocks_ += candidate_blocks - retention_expired_blocks;
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_PALF_RECLAIM_BY_MODE_PAUSE, "Pause reclaim for the tenant selected by the absolute error code.");

int PalfEnvImpl::try_reclaim_blocks_by_mode_(
    const bool local_reclaim_needed,
    bool &has_recycled,
    int64_t &oldest_palf_id,
    SCN &oldest_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(ERRSIM_PALF_RECLAIM_BY_MODE_PAUSE == -tenant_id_)) {
    // Pause before checking demand so tests can stop selected tenants before resizing.
    PALF_LOG(INFO, "tenant reclaim enters debug sync", K_(tenant_id));
    DEBUG_SYNC(BEFORE_PALF_RECLAIM_BY_MODE);
  }
  if (OB_ISNULL(log_block_reclaim_coordinator_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "log block reclaim coordinator is null", KR(ret), KPC(this));
  } else {
    const LogReclaimState reclaim_state = log_block_reclaim_coordinator_->get_reclaim_state();
    const bool server_reclaim_needed = log_block_reclaim_coordinator_->need_reclaim();
    tenant_reclaim_service_.sync_reclaim_mode(
        reclaim_state, server_reclaim_needed,
        /* local_reclaim_done */ !local_reclaim_needed);
    if (LogReclaimState::ENABLED == reclaim_state
        && OB_SUCCESS != (tmp_ret = try_update_reclaim_snapshot_())) {
      PALF_LOG_RET(WARN, tmp_ret, "update tenant reclaim snapshot failed", KPC(this));
    }
    const bool reclaim_by_shared_quota = LogReclaimState::ENABLED == reclaim_state
        && server_reclaim_needed && tenant_reclaim_service_.can_recycle();
    const bool reclaim_by_local_watermark = LogReclaimState::ENABLED != reclaim_state
        && local_reclaim_needed;
    if (reclaim_by_shared_quota || reclaim_by_local_watermark) {
      if (OB_FAIL(recycle_blocks_(has_recycled, oldest_palf_id, oldest_scn))) {
        PALF_LOG(WARN, "recycle blocks by reclaim mode failed", KR(ret), K(reclaim_state),
                 K(server_reclaim_needed), K(local_reclaim_needed), KPC(this));
      }
    }
  }
  return ret;
}

void PalfEnvImpl::report_disk_full_(
    const PalfDiskOptions &disk_options,
    const int64_t total_used_size_byte,
    const int64_t total_unrecyclable_size_byte,
    const int64_t maximum_used_size,
    const int64_t maximum_palf_id,
    const int64_t oldest_palf_id,
    const SCN &oldest_scn,
    const bool in_shrinking) const
{
  constexpr int64_t MB = 1024 * 1024LL;
  int tmp_ret = OB_LOG_OUTOF_DISK_SPACE;
  const int64_t log_disk_usage_limit_size = disk_options.log_disk_usage_limit_size_;
  const int64_t log_disk_warn_percent = disk_options.log_disk_utilization_threshold_;
  const int64_t log_disk_limit_percent = disk_options.log_disk_utilization_limit_threshold_;
  const bool shared_reclaim_enabled = tenant_reclaim_service_.is_shared_reclaim_enabled();
  const int64_t used_size_for_tenant_limit =
      shared_reclaim_enabled ? total_unrecyclable_size_byte : total_used_size_byte;
  const bool tenant_write_stopped = !ATOMIC_LOAD(&diskspace_enough_);
  const bool pool_write_stopped = OB_NOT_NULL(log_block_reclaim_coordinator_)
      && log_block_reclaim_coordinator_->is_pool_write_stopped();
  LOG_DBA_ERROR(OB_LOG_OUTOF_DISK_SPACE, "msg",
      "[SHARED_LOG_RECLAIM] log disk space is almost full", "ret", tmp_ret,
      "total_size(MB)", log_disk_usage_limit_size/MB,
      "used_size(MB)", used_size_for_tenant_limit/MB,
      "used_percent(%)", (used_size_for_tenant_limit*100) / (log_disk_usage_limit_size+1),
      "physical_used_size(MB)", total_used_size_byte/MB,
      K(shared_reclaim_enabled), K(tenant_write_stopped), K(pool_write_stopped),
      "warn_size(MB)", (log_disk_usage_limit_size*log_disk_warn_percent)/100/MB,
      "warn_percent(%)", log_disk_warn_percent,
      "limit_size(MB)", (log_disk_usage_limit_size*log_disk_limit_percent)/100/MB,
      "limit_percent(%)", log_disk_limit_percent,
      "total_unrecyclable_size_byte(MB)", total_unrecyclable_size_byte/MB,
      "maximum_used_size(MB)", maximum_used_size/MB,
      "maximum_log_stream", maximum_palf_id,
      "oldest_log_stream", oldest_palf_id,
      "oldest_scn", oldest_scn,
      "in_shrinking", in_shrinking);
  LOG_DBA_ERROR_(OB_LOG_DISK_SPACE_ALMOST_FULL, tmp_ret,
      "[SHARED_LOG_RECLAIM] log disk space is almost full",
      ", total_size(MB)=", log_disk_usage_limit_size/MB,
      ", used_size(MB)=", used_size_for_tenant_limit/MB,
      ", used_percent(%)=", (used_size_for_tenant_limit*100) / (log_disk_usage_limit_size+1),
      ", physical_used_size(MB)=", total_used_size_byte/MB,
      ", warn_size(MB)=", (log_disk_usage_limit_size*log_disk_warn_percent)/100/MB,
      ", warn_percent(%)=", log_disk_warn_percent,
      ", limit_size(MB)=", (log_disk_usage_limit_size*log_disk_limit_percent)/100/MB,
      ", limit_percent(%)=", log_disk_limit_percent,
      ", total_unrecyclable_size_byte(MB)=", total_unrecyclable_size_byte/MB,
      ", maximum_used_size(MB)=", maximum_used_size/MB,
      ", maximum_log_stream=", maximum_palf_id,
      ", oldest_log_stream=", oldest_palf_id,
      ", oldest_scn=", oldest_scn,
      ", in_shrinking=", in_shrinking);
}

int PalfEnvImpl::try_recycle_blocks()
{
  // Warn after 100 ms and detect hangs after one second.
  TIMEGUARD_INIT(PALF, 100_ms, 1_s);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  PalfDiskOptions disk_opts_for_stopping_writing;
  PalfDiskOptions disk_opts_for_recycling_blocks;
  PalfDiskOptionsWrapper::Status status = PalfDiskOptionsWrapper::Status::INVALID_STATUS;
  int64_t sequence = -1;
  int64_t total_used_size_byte = 0;
  int64_t total_unrecyclable_size_byte = 0;
  int64_t maximum_palf_id = INVALID_PALF_ID;
  int64_t maximum_used_size = 0;
  const int64_t now_us = ObTimeUtility::current_monotonic_time();
  disk_options_wrapper_.get_disk_opts(disk_opts_for_stopping_writing,
                                      disk_opts_for_recycling_blocks,
                                      status,
                                      sequence);
  if (IS_INIT && OB_NOT_NULL(log_block_reclaim_coordinator_)
      && OB_TMP_FAIL(log_block_reclaim_coordinator_->try_advance_reclaim(now_us))) {
    PALF_LOG_RET(WARN, tmp_ret, "advance server log reclaim failed", KPC(this));
  }
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfEnvImpl is not initialized", KR(ret), KPC(this));
  } else if (OB_FAIL(get_disk_usage_(total_used_size_byte, total_unrecyclable_size_byte,
                                     maximum_palf_id, maximum_used_size))) {
    PALF_LOG(WARN, "get disk usage failed", KR(ret), KPC(this));
  } else if (FALSE_IT(update_log_throttling_(total_unrecyclable_size_byte))) {
  } else {
    const int64_t local_reclaim_watermark =
        calculate_disk_space_threshold(disk_opts_for_recycling_blocks.log_disk_usage_limit_size_,
            disk_opts_for_recycling_blocks.log_disk_utilization_threshold_);
    const bool local_reclaim_needed = total_used_size_byte > local_reclaim_watermark;
    const bool in_shrinking = PalfDiskOptionsWrapper::Status::SHRINKING_STATUS == status;
    bool has_recycled = false;
    int64_t oldest_palf_id = INVALID_PALF_ID;
    SCN oldest_scn;

    if (OB_FAIL(try_reclaim_blocks_by_mode_(
            local_reclaim_needed, has_recycled, oldest_palf_id, oldest_scn))) {
      PALF_LOG(WARN, "try reclaim blocks by mode failed", KR(ret), K(local_reclaim_watermark),
               K(total_used_size_byte), KPC(this));
    }

    const int64_t tenant_warn_watermark =
        disk_opts_for_stopping_writing.log_disk_usage_limit_size_
        * disk_opts_for_stopping_writing.log_disk_utilization_threshold_ / 100LL;
    const int64_t used_size_for_tenant_limit =
        tenant_reclaim_service_.is_shared_reclaim_enabled()
        ? total_unrecyclable_size_byte : total_used_size_byte;
    // Use the current tenant write-limit basis after syncing the reclaim mode.
    // Shared history may stay above the local watermark without server pressure.
    if (in_shrinking && used_size_for_tenant_limit <= local_reclaim_watermark) {
      disk_options_wrapper_.change_to_normal(sequence);
      PALF_LOG(INFO, "change_to_normal success", K(disk_options_wrapper_),
               K(total_used_size_byte), K(used_size_for_tenant_limit), K(local_reclaim_watermark));
    }
    const bool tenant_disk_almost_full = used_size_for_tenant_limit > tenant_warn_watermark;
    // Report either the global hard stop, or a tenant above its warning watermark
    // that could not return a block in this turn.
    if (!check_disk_space_enough() || (tenant_disk_almost_full && !has_recycled)) {
      constexpr int64_t REPORT_INTERVAL_US = 1 * 1000 * 1000;
      if (palf_reach_time_interval(
              REPORT_INTERVAL_US, disk_not_enough_print_interval_in_gc_thread_)) {
        report_disk_full_(disk_opts_for_stopping_writing, total_used_size_byte,
                          total_unrecyclable_size_byte, maximum_used_size,
                          maximum_palf_id, oldest_palf_id, oldest_scn,
                          in_shrinking);
      }
    } else if (REACH_TIME_INTERVAL(2 * 1000 * 1000L)) {
      PALF_LOG(INFO, "[SHARED_LOG_RECLAIM] LOG_DISK_OPTION", K(disk_options_wrapper_));
    }
    (void)remove_stale_incomplete_palf_();
  }
  // Assigned work and its stalled time survive this call until the block count reaches zero.
  return ret;
}

int PalfEnvImpl::get_reclaim_snapshot(const int64_t now_us,
                                     const bool server_need_reclaim,
                                     TenantReclaimSnapshot &snapshot)
{
  return tenant_reclaim_service_.get_reclaim_snapshot(now_us, server_need_reclaim, snapshot);
}

int PalfEnvImpl::apply_reclaim_quota(const int64_t reclaim_blocks)
{
  return tenant_reclaim_service_.apply_reclaim_quota(reclaim_blocks);
}

bool PalfEnvImpl::check_disk_space_enough()
{
  return true == ATOMIC_LOAD(&diskspace_enough_)
         && !ATOMIC_LOAD(&gc_stall_stopped_)
         && (OB_ISNULL(log_block_reclaim_coordinator_)
             || !log_block_reclaim_coordinator_->is_pool_write_stopped());
}

PalfEnvImpl::GetTotalUsedDiskSpace::GetTotalUsedDiskSpace()
  : total_used_disk_space_(0), total_unrecyclable_disk_space_(0), maximum_used_size_(0), palf_id_(INVALID_PALF_ID) {}
PalfEnvImpl::GetTotalUsedDiskSpace::~GetTotalUsedDiskSpace() {}

bool PalfEnvImpl::GetTotalUsedDiskSpace::operator() (const LSKey &ls_key, IPalfHandleImpl *palf_handle_impl)
{
  bool bool_ret = true;
  if (NULL == palf_handle_impl) {
    ret_code_ = OB_ERR_UNEXPECTED;
    bool_ret = false;
  } else {
    constexpr int64_t MB = 1024 * 1024;
    int ret = OB_SUCCESS;
    int64_t used_size = 0;
    int64_t unrecyclable_size = 0;
    if (OB_FAIL(palf_handle_impl->get_total_used_disk_space(used_size, unrecyclable_size))) {
      PALF_LOG(WARN, "failed to get_total_used_disk_space", K(ls_key));
      ret_code_ = ret;
      bool_ret = false;
    } else {
      if (used_size >= maximum_used_size_) {
        maximum_used_size_ = used_size;
        palf_id_ = ls_key.id_;
      }
      total_used_disk_space_ += used_size;
      total_unrecyclable_disk_space_ += unrecyclable_size;
      PALF_LOG(TRACE, "get_total_used_disk_space success", K(ls_key),
             "total_used_disk_space(MB):", total_used_disk_space_/MB,
             "total_unrecyclable_disk_space(MB):", total_unrecyclable_disk_space_/MB,
             "end_lsn", palf_handle_impl->get_end_lsn());
    }
  }
  return bool_ret;
}

PalfEnvImpl::RemoveStaleIncompletePalfFunctor::RemoveStaleIncompletePalfFunctor(PalfEnvImpl *palf_env_impl)
  : palf_env_impl_(palf_env_impl)
{}

 PalfEnvImpl::RemoveStaleIncompletePalfFunctor::~RemoveStaleIncompletePalfFunctor()
{
  palf_env_impl_ = NULL;
}

int PalfEnvImpl::RemoveStaleIncompletePalfFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char *saveptr = NULL;
  char file_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  const char *d_name = entry->d_name;
  MEMCPY(file_name, d_name, strlen(d_name));
  char *tmp = strtok_r(file_name, "_", &saveptr);
  char *timestamp_str = NULL;
  if (NULL == tmp || NULL == (timestamp_str = strtok_r(NULL, "_", &saveptr))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "unexpected format", K(ret), K(tmp), K(file_name));
  } else {
    int64_t timestamp = atol(timestamp_str);
    int64_t current_timestamp = ObTimeUtility::current_time();
    int64_t delta = current_timestamp - timestamp;
    constexpr int64_t week_us = 7 * 24 * 60 * 60 * 1000 * 1000ll;
    if (delta <= week_us) {
      PALF_LOG(TRACE, "no need remove this incomplet dir", K(d_name), K(delta),
          K(timestamp), K(timestamp_str), K(current_timestamp));
    } else {
      char path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      int pret = OB_SUCCESS;
      if (0 > (pret = snprintf(path, MAX_PATH_SIZE, "%s/%s", palf_env_impl_->tmp_log_dir_, d_name))) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "snprintf failed", K(ret), K(file_name), K(d_name));
        // TODO by runlin
      } else if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(path))) {
        PALF_LOG(WARN, "delete_directory_rec failed", K(ret), K(file_name), K(path), K(entry->d_name), K(timestamp_str));
      } else {
        PALF_LOG(WARN, "current incomplete palf has bee staled, delete it", K(timestamp), K(current_timestamp), K(path));
      }
    }
  }
  return ret;
}

int PalfEnvImpl::get_disk_usage(int64_t &used_size_byte, int64_t &total_usable_size_byte)
{
  int ret = OB_SUCCESS;
  constexpr int64_t MB = 1024 * 1024;
  PalfDiskOptions disk_options = disk_options_wrapper_.get_disk_opts_for_recycling_blocks();
  if (OB_FAIL(get_disk_usage_(used_size_byte))) {
    PALF_LOG(WARN, "get_disk_usage_ failed", K(ret));
  } else {
    total_usable_size_byte = disk_options.log_disk_usage_limit_size_;
    PALF_LOG(INFO, "get_disk_usage", K(ret), "capacity(MB):", total_usable_size_byte/MB, "used(MB):", used_size_byte/MB);
  }
  return ret;
}

int PalfEnvImpl::check_log_disk_usage_reached_threshold(
    const int64_t threshold_percentage, bool &reached)
{
  int ret = OB_SUCCESS;
  int64_t physical_size = 0;
  int64_t unrecyclable_size = 0;
  int64_t unused_palf_id = 0;
  int64_t unused_maximum_size = 0;
  const PalfDiskOptions disk_options = disk_options_wrapper_.get_disk_opts_for_recycling_blocks();
  const int64_t limit_size = disk_options.log_disk_usage_limit_size_;
  reached = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf env is not inited", K(ret));
  } else if (threshold_percentage < 0 || threshold_percentage > 100) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid disk usage threshold", K(ret), K(threshold_percentage));
  } else if (limit_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "invalid log disk limit", K(ret), K(limit_size));
  } else if (OB_FAIL(get_disk_usage_(physical_size, unrecyclable_size,
                                    unused_palf_id, unused_maximum_size))) {
    PALF_LOG(WARN, "get disk usage failed", K(ret));
  } else {
    const int64_t used_size = tenant_reclaim_service_.is_shared_reclaim_enabled()
        ? unrecyclable_size : physical_size;
    if (used_size > INT64_MAX / 100 || limit_size > INT64_MAX / 100) {
      ret = OB_SIZE_OVERFLOW;
      PALF_LOG(ERROR, "log disk size is too large for threshold comparison",
          K(ret), K(used_size), K(limit_size), K(threshold_percentage));
    } else {
      reached = used_size * 100 >= threshold_percentage * limit_size;
    }
  }
  return ret;
}

int PalfEnvImpl::get_stable_disk_usage(int64_t &used_size_byte, int64_t &total_usable_size_byte)
{
  int ret = OB_SUCCESS;
  constexpr int64_t MB = 1024 * 1024;
  PalfDiskOptions disk_options = disk_options_wrapper_.get_disk_opts_for_stopping_writing();
  if (OB_FAIL(get_disk_usage_(used_size_byte))) {
    PALF_LOG(WARN, "get_disk_usage_ failed", K(ret));
  } else {
    total_usable_size_byte = disk_options.log_disk_usage_limit_size_;
    PALF_LOG(INFO, "get_stable_disk_usage", K(ret), "capacity(MB):", total_usable_size_byte/MB, "used(MB):", used_size_byte/MB);
  }
  return ret;
}

int PalfEnvImpl::check_log_disk_under_pressure(bool &log_disk_under_pressure)
{
  int ret = OB_SUCCESS;
  log_disk_under_pressure = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf env is not inited", K(ret));
  } else if (OB_FAIL(disk_options_wrapper_.check_log_disk_under_pressure(
      log_disk_under_pressure))) {
    PALF_LOG(WARN, "failed to check whether log disk is under pressure", K(ret));
  }
  return ret;
}

int PalfEnvImpl::update_options(const PalfOptions &options)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(palf_meta_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == options.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(options));
  } else if (OB_FAIL(log_rpc_.update_transport_compress_options(options.compress_options_))) {
    PALF_LOG(WARN, "update_transport_compress_options failed", K(ret), K(options));
  } else if (FALSE_IT(rebuild_replica_log_lag_threshold_ = options.rebuild_replica_log_lag_threshold_)) {
  } else if (OB_FAIL(check_can_update_log_disk_options_(options.disk_options_))) {
    PALF_LOG(WARN, "check_can_update_log_disk_options_ failed", K(options));
  } else if (OB_FAIL(disk_options_wrapper_.update_disk_options(options.disk_options_))) {
    PALF_LOG(WARN, "update_disk_options failed", K(ret), K(options));
  } else {
    PALF_LOG(INFO, "update_options successs", K(options), KPC(this));
  }
  return ret;
}

int PalfEnvImpl::get_options(PalfOptions &options)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    options.disk_options_ = disk_options_wrapper_.get_disk_opts_for_recycling_blocks();
    options.compress_options_ = log_rpc_.get_compress_opts();
    options.rebuild_replica_log_lag_threshold_ = rebuild_replica_log_lag_threshold_;
  }
  return ret;
}

int PalfEnvImpl::for_each(const common::ObFunction<int (IPalfHandleImpl *)> &func)
{
  auto func_impl = [&func](const LSKey &ls_key, IPalfHandleImpl *ipalf_handle_impl) -> bool {
    bool bool_ret = true;
    int ret = OB_SUCCESS;
    if (OB_FAIL(func(ipalf_handle_impl))) {
      PALF_LOG(WARN, "execute func failed", K(ret), K(ls_key));
    }
    if (OB_FAIL(ret)) {
     bool_ret = false;
    }
    return bool_ret;
  };
  int ret = OB_SUCCESS;
  if (!func.is_valid()) {
    // ObFunction will be invalid when allocating memory failed.
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(palf_handle_impl_map_.for_each(func_impl))) {
    PALF_LOG(WARN, "iterate palf_handle_impl_map_ failed", K(ret));
  } else {
  }
  return ret;
}

int PalfEnvImpl::for_each(const common::ObFunction<int (const PalfHandle &)> &func)
{
  auto func_impl = [&func](const LSKey &ls_key, IPalfHandleImpl *ipalf_handle_impl) -> bool {
    bool bool_ret = true;
    int ret = OB_SUCCESS;
    PalfHandle palf_handle;
    palf_handle.palf_handle_impl_ = ipalf_handle_impl;
    if (OB_FAIL(func(palf_handle))) {
      PALF_LOG(WARN, "execute func failed", K(ret), K(ls_key));
    } else {
    }
    if (OB_FAIL(ret)) {
     bool_ret = false;
    }
    return bool_ret;
  };
  int ret = OB_SUCCESS;
  if (!func.is_valid()) {
    // ObFunction will be invalid when allocating memory failed.
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(palf_handle_impl_map_.for_each(func_impl))) {
    PALF_LOG(WARN, "iterate palf_handle_impl_map_ failed", K(ret));
  } else {
  }
  return ret;
}

common::ObILogAllocator* PalfEnvImpl::get_log_allocator()
{
  return log_alloc_mgr_;
}

PalfEnvImpl::ReloadPalfHandleImplFunctor::ReloadPalfHandleImplFunctor(PalfEnvImpl *palf_env_impl) : palf_env_impl_(palf_env_impl)
{
}

int PalfEnvImpl::ReloadPalfHandleImplFunctor::func(const struct dirent *entry)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  ObTimeGuard guard("ReloadFunctor");
  bool result = false;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid args", K(ret), KP(entry));
  } else if (0 > (pret = snprintf(log_dir, MAX_PATH_SIZE, "%s/%s", palf_env_impl_->log_dir_, entry->d_name))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "snprint failed", K(ret), K(pret), K(entry->d_name));
  } else if (OB_FAIL(palf::is_directory(log_dir,result))) {
    PALF_LOG(WARN, "is_directory failed", K(ret), K(log_dir));
  } else if (!result) {
    PALF_LOG(WARN, "path is not a directory, ignore it", K(ret), K(log_dir));
  } else {
    const char *path = entry->d_name;
    bool is_number = true;
    const size_t path_len = strlen(path);
    for (size_t i = 0; is_number && i < path_len; ++i) {
      if ('\0' == path[i]) {
        break;
      } else if (!isdigit(path[i])) {
        is_number = false;
      }
    }
    if (!is_number) {
      // do nothing, skip invalid block like tmp
    } else {
      int64_t id = strtol(path, nullptr, 10);
      if (OB_FAIL(palf_env_impl_->reload_palf_handle_impl_(id))) {
        PALF_LOG(WARN, "reload_palf_handle_impl failed", K(ret));
      }
      guard.click("reload_palf_handle_impl");
      PALF_LOG(INFO, "reload_palf_handle_impl_", K(ret), K(id), K(guard));
    }
  }
  return ret;
}

int PalfEnvImpl::reload_palf_handle_impl_(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  PalfHandleImpl *tmp_palf_handle_impl;
  char base_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  int64_t start_ts = ObTimeUtility::current_time();
  LSKey hash_map_key(palf_id);
  bool is_integrity = true;
  const int64_t palf_epoch = ATOMIC_AAF(&last_palf_epoch_, 1);
  if (0 > (pret = snprintf(base_dir, MAX_PATH_SIZE, "%s/%ld", log_dir_, palf_id))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "snprint failed", K(ret), K(pret), K(palf_id));
  } else if (NULL == (tmp_palf_handle_impl = PalfHandleImplFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "alloc ipalf_handle_impl failed", K(ret));
  } else if (OB_FAIL(tmp_palf_handle_impl->load(palf_id, &fetch_log_engine_, base_dir, log_alloc_mgr_,
          log_block_pool_, &log_rpc_, log_io_worker_wrapper_.get_log_io_worker(palf_id), &log_shared_queue_th_,
          this, self_, &election_timer_, palf_epoch, &io_adapter_, is_integrity))) {
    PALF_LOG(ERROR, "PalfHandleImpl init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(palf_handle_impl_map_.insert_and_get(hash_map_key, tmp_palf_handle_impl))) {
    PALF_LOG(WARN, "palf_handle_impl_map_ insert_and_get failed", K(ret), K(palf_id), K(tmp_palf_handle_impl));
  } else {
    (void) tmp_palf_handle_impl->set_monitor_cb(monitor_);
    (void) tmp_palf_handle_impl->set_scan_disk_log_finished();
    palf_handle_impl_map_.revert(tmp_palf_handle_impl);
    int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    PALF_LOG(INFO, "reload_palf_handle_impl success", K(ret), K(palf_id), K(cost_ts), KP(this),
        KP(&palf_handle_impl_map_));
  }

  if (OB_FAIL(ret) && NULL != tmp_palf_handle_impl) {
    // if 'tmp_palf_handle_impl' has not been inserted into hash map,
    // need reclaim manually.
    PALF_LOG(ERROR, "reload_palf_handle_impl_ failed, need free tmp_palf_handle_impl", K(ret), K(tmp_palf_handle_impl));
    if (OB_ENTRY_NOT_EXIST == palf_handle_impl_map_.contains_key(hash_map_key)) {
      PalfHandleImplFactory::free(tmp_palf_handle_impl);
      tmp_palf_handle_impl = NULL;
    }
  } else if (false == is_integrity) {
    PALF_LOG(WARN, "palf instance is not integrity, remove it", K(palf_id));
    ret = move_incomplete_palf_into_tmp_dir_(palf_id);
  }
  return ret;
}

int PalfEnvImpl::get_total_used_disk_space_(int64_t &total_used_disk_space,
                                            int64_t &total_unrecyclable_disk_space,
                                            int64_t &palf_id,
                                            int64_t &maximum_used_size)
{
  int ret = OB_SUCCESS;
  GetTotalUsedDiskSpace functor;
  if (OB_FAIL(palf_handle_impl_map_.for_each(functor))) {
    ret = functor.ret_code_;
    PALF_LOG(WARN, "get_total_used_disk_space", K(ret), K(functor));
  } else {
    palf_id = functor.palf_id_;
    maximum_used_size = functor.maximum_used_size_;
    total_used_disk_space = functor.total_used_disk_space_;
    total_unrecyclable_disk_space = functor.total_unrecyclable_disk_space_;
  }
  return ret;
}

int PalfEnvImpl::get_disk_usage_(int64_t &used_size_byte,
                                 int64_t &unrecyclable_disk_space,
                                 int64_t &palf_id,
                                 int64_t &maximum_used_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_total_used_disk_space_(used_size_byte, unrecyclable_disk_space, palf_id, maximum_used_size))) {
    PALF_LOG(WARN, "get_total_used_disk_space failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfEnvImpl::get_disk_usage_(int64_t &used_size_byte)
{
  int ret = OB_SUCCESS;
  int64_t unused_unrecyclable_size = 0;
  int64_t unused_palf_id = 0;
  int64_t unused_size = 0;
  if (OB_FAIL(get_disk_usage_(used_size_byte, unused_unrecyclable_size, unused_palf_id, unused_size))) {
    PALF_LOG(WARN, "get_total_used_disk_space failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfEnvImpl::get_reclaim_summary_(int64_t &retention_expired_blocks,
                                      int64_t &retention_unexpired_blocks)
{
  int ret = OB_SUCCESS;
  int64_t retention_us = 0;
  // Copy the setting and release the config reference before reading log blocks.
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (!tenant_config.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "tenant config is invalid", KR(ret), K(tenant_id_));
    } else {
      retention_us = tenant_config->log_retention_time;
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t now_us = ObTimeUtility::current_time();
    SCN retention_scn;
    if (OB_FAIL(calc_retention_scn_(now_us, retention_us, retention_scn))) {
      PALF_LOG(WARN, "calculate retention scn failed", KR(ret), K(now_us),
               K(retention_us), K(tenant_id_));
    } else {
      LogGetRecycableFileCandidate functor(retention_scn);
      if (OB_FAIL(palf_handle_impl_map_.for_each(functor))) {
        if (OB_SUCCESS != functor.ret_code_) {
          ret = functor.ret_code_;
        }
        PALF_LOG(WARN, "collect tenant reclaim summary failed", KR(ret), K(tenant_id_));
      } else {
        retention_expired_blocks = functor.retention_expired_blocks_;
        retention_unexpired_blocks = functor.retention_unexpired_blocks_;
      }
    }
  }
  return ret;
}

int PalfEnvImpl::calc_retention_scn_(
    const int64_t now_us,
    const int64_t retention_us,
    SCN &retention_scn)
{
  int ret = OB_SUCCESS;
  retention_scn.reset();
  if (OB_UNLIKELY(now_us < 0 || retention_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid retention boundary argument", KR(ret), K(now_us), K(retention_us));
  } else {
    const int64_t retention_ts = now_us > retention_us ? now_us - retention_us : 0;
    if (OB_FAIL(retention_scn.convert_from_ts(retention_ts))) {
      PALF_LOG(WARN, "convert retention timestamp failed", KR(ret), K(retention_ts));
    }
  }
  return ret;
}

int PalfEnvImpl::try_update_reclaim_snapshot_()
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  if (OB_FAIL(tenant_reclaim_service_.check_need_update_snapshot(need_update))) {
    PALF_LOG(WARN, "check tenant reclaim snapshot update failed", KR(ret), K(tenant_id_));
  } else if (need_update) {
    int64_t retention_expired_blocks = 0;
    int64_t retention_unexpired_blocks = 0;
    if (OB_FAIL(get_reclaim_summary_(retention_expired_blocks,
                                     retention_unexpired_blocks))) {
      // Keep the last successful counts, less blocks already recycled, until the next refresh.
      PALF_LOG(WARN, "get reclaim summary failed", KR(ret), K(tenant_id_));
    } else {
      tenant_reclaim_service_.update_reclaim_snapshot(
          retention_expired_blocks, retention_unexpired_blocks);
    }
  }
  return ret;
}

int PalfEnvImpl::recycle_blocks_(bool &has_recycled, int64_t &oldest_palf_id, SCN &oldest_scn)
{
  int ret = OB_SUCCESS;
  has_recycled = false;
  // TODO by runlin:
  //  1. only execute unlink blocks when the disk usage watemark reaches the threshold.
  //  2. batch unlink in each round.
  LogGetRecycableFileCandidate functor;
  if (OB_FAIL(palf_handle_impl_map_.for_each(functor))) {
    PALF_LOG(WARN, "palf_handle_impl_map_ for_each failed", K(ret), K(functor));
  } else {
    IPalfHandleImplGuard guard;
    int64_t palf_id(functor.id_);
    const block_id_t min_block_id = functor.min_block_id_;
    if (false == is_valid_block_id(min_block_id)) {
      PALF_LOG(WARN, "there is not any block can be recycled, need verify the base"
          "lsn of PalfHandleImpl whether has been advanced", K(ret), KPC(this));
    } else if (OB_FAIL(get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (FALSE_IT(inject_gc_stall_(palf_id, min_block_id))) {
    } else if (OB_FAIL(guard.get_palf_handle_impl()->delete_block(min_block_id))) {
      PALF_LOG(WARN, "delete block failed", K(ret), K(min_block_id), K(functor));
    } else {
      has_recycled = true;
      PALF_LOG(INFO, "recycle_blocks success", K(functor));
    }
    oldest_palf_id = functor.oldest_palf_id_;
    oldest_scn = functor.oldest_block_scn_;
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_PALF_BLOCK_GC_STALL);
ERRSIM_POINT_DEF(ERRSIM_PALF_BLOCK_GC_DELAY, "Absolute error code is the block GC delay in milliseconds.");
void PalfEnvImpl::inject_gc_stall_(const int64_t palf_id, const block_id_t min_block_id)
{
  if (OB_UNLIKELY(ERRSIM_PALF_BLOCK_GC_STALL == -tenant_id_)) {
    PALF_LOG(INFO, "block GC enters debug sync", K_(tenant_id), K(palf_id), K(min_block_id));
    DEBUG_SYNC(BEFORE_PALF_RECYCLE_BLOCK);
    PALF_LOG(INFO, "block GC leaves debug sync", K_(tenant_id), K(palf_id), K(min_block_id));
  }
  const int64_t sleep_ms = abs(static_cast<int64_t>(ERRSIM_PALF_BLOCK_GC_DELAY));
  if (OB_UNLIKELY(sleep_ms > 0)) {
    PALF_LOG(INFO, "block GC sleeps", K_(tenant_id), K(palf_id), K(min_block_id), K(sleep_ms));
    const int64_t end_ts = ObTimeUtility::current_monotonic_time() + sleep_ms * 1000;
    int64_t left_us = sleep_ms * 1000;
    while (left_us > 0) {
      ob_usleep(MIN(left_us, 1000 * 1000L));
      left_us = end_ts - ObTimeUtility::current_monotonic_time();
    }
  }
}

int PalfEnvImpl::wait_until_reference_count_to_zero_(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char base_dir[MAX_PATH_SIZE] = {'\0'};
  char tmp_base_dir[MAX_PATH_SIZE] = {'\0'};
  if (false == is_valid_palf_id(palf_id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid arguments", K(ret), K(palf_id));
  } else if (0 > (pret = snprintf(base_dir, MAX_PATH_SIZE, "%s/%ld", log_dir_, palf_id))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprinf failed", K(ret), K(pret), K(palf_id));
  } else if (0 > (pret = snprintf(tmp_base_dir, MAX_PATH_SIZE, "%s/%ld%s", log_dir_, palf_id, TMP_SUFFIX))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprinf failed", K(ret), K(pret), K(palf_id));
  } else {
    bool normal_dir_exist = true;
    bool tmp_dir_exist = true;
    while (OB_SUCC(is_exists(base_dir, normal_dir_exist))
           && OB_SUCC(is_exists(tmp_base_dir, tmp_dir_exist))) {
      if (!normal_dir_exist && !tmp_dir_exist) {
        break;
      }
      PALF_LOG(INFO, "wait_until_reference_count_to_zero_ failed, may be reference count has leaked", K(palf_id),
          K(normal_dir_exist), K(tmp_dir_exist), K(base_dir), K(tmp_base_dir));
      ob_usleep(1000);
    }
  }
  return ret;
}

bool PalfEnvImpl::check_can_create_palf_handle_impl_() const
{
  bool bool_ret = true;
  int64_t count = palf_handle_impl_map_.count();
  // NB: avoid concurrent with expand and shrink, need guard by palf_meta_lock_.
  const PalfDiskOptions disk_opts = disk_options_wrapper_.get_disk_opts_for_recycling_blocks();
  bool_ret = (count + 1) * MIN_DISK_SIZE_PER_PALF_INSTANCE <= disk_opts.log_disk_usage_limit_size_;
  return bool_ret;
}

int PalfEnvImpl::remove_palf_handle_impl_from_map_not_guarded_by_lock_(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  LSKey hash_map_key(palf_id);
  auto set_delete_func = [](const LSKey &key, IPalfHandleImpl *value) {
    UNUSED(key);
    value->set_deleted();
  };
  if (OB_FAIL(palf_handle_impl_map_.operate(hash_map_key, set_delete_func))) {
    PALF_LOG(WARN, "operate palf_handle_impl_map_ failed", K(ret), K(palf_id), KPC(this));
  } else if (OB_FAIL(palf_handle_impl_map_.del(hash_map_key))) {
    PALF_LOG(WARN, "palf_handle_impl_map_ del failed", K(ret), K(palf_id));
  } else {
    PALF_LOG(INFO, "remove_palf_handle_impl success", K(ret), K(palf_id));
  }
  return ret;
}

int PalfEnvImpl::move_incomplete_palf_into_tmp_dir_(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  const mode_t mode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;
  char src_log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char dest_log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  bool tmp_dir_exist = false;
  LSKey hash_map_key(palf_id);
  int64_t timestamp = ObTimeUtility::current_time();
  if (OB_FAIL(palf_handle_impl_map_.del(hash_map_key))) {
    PALF_LOG(WARN, "del palf from map failed, unexpected", K(ret),
        K(palf_id), KPC(this));
  } else if (OB_FAIL(check_tmp_log_dir_exist_(tmp_dir_exist))) {
    PALF_LOG(WARN, "check_tmp_log_dir_exist_ failed", K(ret), KPC(this), K(tmp_log_dir_));
  } else if (false == tmp_dir_exist && (OB_FAIL(mkdir(tmp_log_dir_)))) {
    PALF_LOG(ERROR, "mkdir tmp log dir failed", K(ret), KPC(this), K(tmp_log_dir_));
  } else if (0 > (pret = snprintf(src_log_dir, MAX_PATH_SIZE, "%s/%ld", log_dir_, palf_id))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprintf failed, unexpected error", K(ret));
  } else if (0 > (pret = snprintf(dest_log_dir, MAX_PATH_SIZE, "%s/%ld_%ld", tmp_log_dir_, palf_id, timestamp))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprintf failed, unexpected error", K(ret));
  } else if (OB_FAIL(rename_with_retry(src_log_dir, dest_log_dir))) {
    PALF_LOG(ERROR, "rename failed", K(ret), KPC(this), K(src_log_dir), K(dest_log_dir));
  } else if (OB_FAIL(fsync_with_retry(log_dir_))) {
    PALF_LOG(ERROR, "fsync_dir failed", K(ret), KPC(this), K(src_log_dir), K(dest_log_dir));
  } else {
  }
  return ret;
}

int PalfEnvImpl::check_tmp_log_dir_exist_(bool &exist) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(is_exists(tmp_log_dir_, exist))) {
    PALF_LOG(WARN, "check dir exist failed", K(ret), KPC(this), K(tmp_log_dir_));
  } else {
  }
  return ret;
}

int PalfEnvImpl::remove_stale_incomplete_palf_()
{
  int ret = OB_SUCCESS;
  bool exist = false;
  RemoveStaleIncompletePalfFunctor functor(this);
  if (OB_FAIL(check_tmp_log_dir_exist_(exist))) {
    PALF_LOG(WARN, "check_tmp_log_dir_exist_ failed", K(ret), KPC(this));
  } else if (false == exist) {
  } else if (OB_FAIL(scan_dir(tmp_log_dir_, functor))){
    PALF_LOG(WARN, "remove_stale_incomplete_palf_ failed", K(ret), KPC(this), K(tmp_log_dir_));
  } else {
  }
  return ret;
}

int PalfEnvImpl::get_io_statistic_info(int64_t &last_working_time,
                                       int64_t &pending_write_size,
                                       int64_t &pending_write_count,
                                       int64_t &pending_write_rt,
                                       int64_t &accum_write_size,
                                       int64_t &accum_write_count,
                                       int64_t &accum_write_rt)
{
  int ret = OB_SUCCESS;
  GetIOStatistic functor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(palf_handle_impl_map_.for_each(functor))) {
    PALF_LOG(WARN, "for_each failed", K(ret), K(functor));
  } else {
    // use last_working_time of IOWorker to detect
    // more IO operations including: pwrite, rename, ...
    last_working_time = log_io_worker_wrapper_.get_last_working_time();
    pending_write_size = functor.pending_write_size_;
    pending_write_count = functor.pending_write_count_;
    pending_write_rt = functor.pending_write_rt_;
    accum_write_size = functor.accum_write_size_;
    accum_write_count = functor.accum_write_count_;
    accum_write_rt = functor.accum_write_rt_;
  }
  return ret;
}

int64_t PalfEnvImpl::get_tenant_id()
{
  return tenant_id_;
}
int PalfEnvImpl::update_replayable_point(const SCN &replayable_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(fetch_log_engine_.update_replayable_point(replayable_scn))) {
    PALF_LOG(WARN, "update_replayable_point failed", KPC(this), K(replayable_scn));
  }
  return ret;
}

int PalfEnvImpl::get_throttling_options(PalfThrottleOptions &options)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    (void)disk_options_wrapper_.get_throttling_options(options);
  }
  return ret;
}

void PalfEnvImpl::update_log_throttling_(const int64_t unrecyclable_size)
{
  int ret = OB_SUCCESS;
  bool state_changed = false;
  bool gc_stalled = false;
  bool gc_stall_stopped = false;
  int64_t stalled_duration_us = OB_INVALID_TIMESTAMP;
  // GC 和 LogLoop 按同一顺序发布压力、停写状态和 IO 通知；锁忙时保留旧值，下一轮再更新。
  // 两层锁均使用 trylock，避免 LogLoop 等待另一个发布者或磁盘配置更新。
  if (OB_FAIL(throttling_update_lock_.trylock())) {
    PALF_LOG(TRACE, "failed to lock throttling update", K(ret));
  } else {
    const bool prev_gc_stalled = disk_options_wrapper_.is_gc_stalled();
    const bool prev_gc_stall_stopped = ATOMIC_LOAD(&gc_stall_stopped_);
    const int64_t gc_stall_start_ts = tenant_reclaim_service_.get_gc_stall_start_ts();
    const int64_t now_us = ObTimeUtility::current_monotonic_time();
    // 卡顿起点已包含 1 秒无进展宽限期，d 只累计宽限期之后的时间，使用单调时钟。
    // 起点无效或仍在未来时，d 保持无效值，本轮按真实 U 限流并清除旧卡顿状态。
    if (OB_INVALID_TIMESTAMP != gc_stall_start_ts && now_us >= gc_stall_start_ts) {
      stalled_duration_us = now_us - gc_stall_start_ts;
    }
    bool need_throttling = false;
    // 只在取得新 U 样本后发布 E，保持“本轮采样基值 + IO 新增量”的记账方式。
    // 不用旧 U 单独推进时间项，避免刷新 E 时清掉尚未被采样覆盖的 IO 增量。
    if (OB_FAIL(disk_options_wrapper_.try_update_throttling_disk_usage(
        unrecyclable_size, stalled_duration_us, need_throttling, gc_stall_stopped))) {
      PALF_LOG(TRACE, "try_update_throttling_disk_usage failed", K(ret));
    } else {
      gc_stalled = disk_options_wrapper_.is_gc_stalled();
      ATOMIC_STORE(&gc_stall_stopped_, gc_stall_stopped);
      state_changed = prev_gc_stalled != gc_stalled || prev_gc_stall_stopped != gc_stall_stopped;
      ret = log_io_worker_wrapper_.notify_need_writing_throttling(need_throttling, gc_stalled);
    }
    throttling_update_lock_.unlock();
  }
  if (state_changed) {
    PALF_LOG(INFO, "[LOG DISK THROTTLING] GC state changed", K(gc_stalled), K(gc_stall_stopped),
             K(stalled_duration_us), K(unrecyclable_size));
  }
  if (OB_SUCCESS == ret && gc_stalled && REACH_TIME_INTERVAL(2 * 1000 * 1000L)) {
    PALF_LOG(INFO, "[LOG DISK THROTTLING] [GC STALLED]", K(stalled_duration_us),
             K(unrecyclable_size), K(gc_stall_stopped), K(disk_options_wrapper_));
  }
  if (OB_SUCCESS != ret && OB_EAGAIN != ret) {
    PALF_LOG(WARN, "update log writing throttling failed", K(ret));
  }
}

void PalfEnvImpl::period_calc_disk_usage()
{
  int ret = OB_SUCCESS;
  constexpr int64_t MB = 1024 * 1024;
  PalfDiskOptions disk_options = disk_options_wrapper_.get_disk_opts_for_stopping_writing();
  int64_t used_size_byte = 0;
  int64_t unrecyclable_size_byte = 0;
  int64_t unused_palf_id = 0;
  int64_t unused_maximum_used_size = 0;
  if (OB_FAIL(get_disk_usage_(used_size_byte, unrecyclable_size_byte,
                             unused_palf_id, unused_maximum_used_size))) {
    PALF_LOG(WARN, "get_disk_usage_ failed", K(ret));
  } else {
    // GC normally publishes pressure. LogLoop only handles stall expiry and recovery.
    const int64_t gc_stall_start_ts = tenant_reclaim_service_.get_gc_stall_start_ts();
    if (disk_options_wrapper_.is_gc_stalled()
        || (OB_INVALID_TIMESTAMP != gc_stall_start_ts
            && ObTimeUtility::current_monotonic_time() >= gc_stall_start_ts)) {
      update_log_throttling_(unrecyclable_size_byte);
    }
    const int64_t used_size_for_tenant_limit =
        tenant_reclaim_service_.is_shared_reclaim_enabled()
        ? unrecyclable_size_byte : used_size_byte;
    const int64_t log_disk_usage_limit_size =  disk_options.log_disk_usage_limit_size_;
    const int64_t log_disk_limit_percent = disk_options.log_disk_utilization_limit_threshold_;
    const int64_t log_disk_warn_percent = disk_options.log_disk_utilization_threshold_;
    const int64_t usable_disk_limit_size_to_stop_writing =
      log_disk_usage_limit_size * log_disk_limit_percent / 100LL;
    const bool curr_diskspace_enough =
        usable_disk_limit_size_to_stop_writing >= used_size_for_tenant_limit ? true : false;
    const int64_t warn_siz =
      log_disk_usage_limit_size * log_disk_warn_percent / 100LL;
    if (diskspace_enough_ != curr_diskspace_enough) {
      ATOMIC_STORE(&diskspace_enough_, curr_diskspace_enough);
    }
    // NB: print error log when:
    // 1. write-stop.
    if (!curr_diskspace_enough) {
      constexpr int64_t INTERVAL = 1*1000*1000;
      if (palf_reach_time_interval(INTERVAL, disk_not_enough_print_interval_in_loop_thread_)) {
        int tmp_ret = OB_LOG_OUTOF_DISK_SPACE;
        LOG_DBA_ERROR(OB_LOG_OUTOF_DISK_SPACE, "msg",
            "[SHARED_LOG_RECLAIM] log disk space is almost full", "ret", tmp_ret,
            "total_size(MB)", log_disk_usage_limit_size/MB,
            "used_size(MB)", used_size_for_tenant_limit/MB,
            "used_percent(%)", (used_size_for_tenant_limit*100) / (log_disk_usage_limit_size + 1),
            "warn_size(MB)", warn_siz/MB,
            "warn_percent(%)", log_disk_warn_percent,
            "limit_size(MB)", usable_disk_limit_size_to_stop_writing/MB,
            "limit_percent(%)", log_disk_limit_percent);
        LOG_DBA_ERROR_(OB_LOG_DISK_SPACE_ALMOST_FULL, tmp_ret,
            "[SHARED_LOG_RECLAIM] log disk space is almost full",
            ", total_size(MB)=", log_disk_usage_limit_size/MB,
            ", used_size(MB)=", used_size_for_tenant_limit/MB,
            ", used_percent(%)=", (used_size_for_tenant_limit*100) / (log_disk_usage_limit_size + 1),
            ", warn_size(MB)=", warn_siz/MB,
            ", warn_percent(%)=", log_disk_warn_percent,
            ", limit_size(MB)=", usable_disk_limit_size_to_stop_writing/MB,
            ", limit_percent(%)=", log_disk_limit_percent);
        }
      }
  }

}

int PalfEnvImpl::init_log_io_worker_config_(const int log_writer_parallelism,
                                            const int64_t tenant_id,
                                            LogIOWorkerConfig &config)
{
  int ret = OB_SUCCESS;
  // log_writer_parallelism only valid when it's user tenant.
  // to support writing throttling, sys log stream must has dependent LogIOWorker.
  const int64_t real_log_writer_parallelism = is_user_tenant(tenant_id) ? (log_writer_parallelism  + 1) : 1;
  auto tmp_upper_align_div = [](const int64_t num, const int64_t align) -> int64_t {
    return (num + align - 1) / align;
  };

  constexpr int64_t default_io_queue_cap = 100 * 1024;
  constexpr int64_t default_io_batch_width = 8;
  // Due to the uniqueness of the log stream ID, using a hash distribution method can
  // naturally balance the load in a single-unit environment, therefore, we set the
  // min io queue capacity to PALF_SLIDING_WINDOW_SIZE * 2, and set default_io_batch_width
  // to 1.
  // TODO by zjf225077:
  // to support load balance in a multi-unit environment, LogIOWorker needs to use
  // a load factor to ensure that the number of log streams on each LogIOWorker is in
  // a balanced state.
  constexpr int64_t default_min_io_queue_cap = PALF_SLIDING_WINDOW_SIZE * 2;
  constexpr int64_t default_min_batch_width = 1;
  // Assume that a maximum of 100 * 1024 I/O tasks exist simultaneously in single PalfEnvImpl
  config.io_worker_num_ = real_log_writer_parallelism;
  config.io_queue_capcity_ = MAX(default_min_io_queue_cap,
                                 tmp_upper_align_div(default_io_queue_cap, real_log_writer_parallelism));
  // for sys tenant, batch_width is 1
  config.batch_width_ = is_user_tenant(tenant_id) ? default_io_batch_width : 1;
  config.batch_depth_ = PALF_SLIDING_WINDOW_SIZE;
  PALF_LOG(INFO, "init_log_io_worker_config_ success", K(config), K(tenant_id), K(log_writer_parallelism));
  return ret;
}

int PalfEnvImpl::check_can_update_log_disk_options_(const PalfDiskOptions &disk_opts)
{
  int ret = OB_SUCCESS;
  const int64_t curr_palf_instance_num = palf_handle_impl_map_.count();
  const int64_t curr_min_log_disk_size = curr_palf_instance_num * MIN_DISK_SIZE_PER_PALF_INSTANCE;
  if (disk_opts.log_disk_usage_limit_size_ < curr_min_log_disk_size) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "can not hold current palf instance", K(curr_palf_instance_num),
             K(curr_min_log_disk_size), K(disk_opts));
  }
  return ret;
}

int PalfEnvImpl::remove_directory_while_exist_(const char *log_dir)
{
  int ret = OB_SUCCESS;
  bool result = true;
  if (OB_FAIL(is_exists(log_dir, result))) {
    PALF_LOG(WARN, "check directory exists failed", KPC(this), K(log_dir));
  } else if (!result) {
    PALF_LOG(WARN, "directory not exist, remove_directory success!", K(log_dir), K(result));
  } else if (OB_FAIL(remove_directory(log_dir))) {
    PALF_LOG(WARN, "remove_directory failed", K(log_dir), K(result));
  } else {}
  return ret;
}

int PalfEnvImpl::remove_stale_incomplete_palf_(const char *dir_name)
{
  int ret = OB_SUCCESS;
  char *saveptr = NULL;
  char file_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  MEMCPY(file_name, dir_name, strlen(dir_name));
  char *tmp = strtok_r(file_name, "_", &saveptr);
  char *timestamp_str = NULL;
  if (NULL == tmp || NULL == (timestamp_str = strtok_r(NULL, "_", &saveptr))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "unexpected format", K(ret), K(tmp), K(file_name));
  } else {
    int64_t timestamp = atol(timestamp_str);
    int64_t current_timestamp = ObTimeUtility::current_time();
    int64_t delta = current_timestamp - timestamp;
    constexpr int64_t week_us = 7 * 24 * 60 * 60 * 1000 * 1000ll;
    if (delta <= week_us) {
      PALF_LOG(TRACE, "no need remove this incomplet dir", K(dir_name), K(delta),
          K(timestamp), K(timestamp_str), K(current_timestamp));
    } else {
      char path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      int pret = OB_SUCCESS;
      if (0 > (pret = snprintf(path, MAX_PATH_SIZE, "%s/%s", tmp_log_dir_, dir_name))) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "snprintf failed", K(ret), K(file_name), K(path));
        // TODO by runlin
      } else if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(path))) {
        PALF_LOG(WARN, "delete_directory_rec failed", K(ret), K(file_name), K(path), K(dir_name), K(timestamp_str));
      } else {
        PALF_LOG(WARN, "current incomplete palf has bee staled, delete it", K(timestamp), K(current_timestamp), K(path));
      }
    }
  }
  return ret;
}

PalfEnvImpl::GetIOStatistic::GetIOStatistic() :
    last_working_time_(OB_INVALID_TIMESTAMP),
    accum_write_size_(0),
    accum_write_count_(0),
    accum_write_rt_(0),
    pending_write_size_(0),
    pending_write_count_(0),
    pending_write_rt_(0)
{ }

PalfEnvImpl::GetIOStatistic::~GetIOStatistic()
{
  last_working_time_ = OB_INVALID_TIMESTAMP;
  accum_write_size_ = 0;
  accum_write_count_ = 0;
  accum_write_rt_ = 0;
  pending_write_size_ = 0;
  pending_write_count_ = 0;
  pending_write_rt_ = 0;
}

bool PalfEnvImpl::GetIOStatistic::operator()(const LSKey &palf_id,
                                             IPalfHandleImpl *palf_handle_impl)
{
  bool bool_ret = true;
  int ret = OB_SUCCESS;
  int64_t last_working_time = OB_INVALID_TIMESTAMP;
  int64_t last_write_size = 0;
  int64_t accum_write_size = 0;
  int64_t accum_write_count = 0;
  int64_t accum_write_rt = 0;

  if (OB_NOT_NULL(palf_handle_impl)) {
    if (OB_FAIL(palf_handle_impl->get_io_statistic_info(last_working_time,
        last_write_size, accum_write_size, accum_write_count, accum_write_rt))) {
      PALF_LOG(WARN, "failed to get_io_statistic_info", K(palf_id));
    } else {
      // record last_working_time_
      if (OB_INVALID_TIMESTAMP == last_working_time) {
        last_working_time_ = last_working_time_;
      } else if (OB_INVALID_TIMESTAMP == last_working_time_) {
        last_working_time_ = last_working_time;
      } else {
        last_working_time_ = MIN(last_working_time_, last_working_time);
      }
      // record pending write info
      if (OB_INVALID_TIMESTAMP != last_working_time && last_write_size > 0) {
        pending_write_size_ += last_write_size;
        pending_write_count_++;
        pending_write_rt_ += (common::ObTimeUtility::fast_current_time() - last_working_time);
      }
      accum_write_size_ += accum_write_size;
      accum_write_count_ += accum_write_count;
      accum_write_rt_ += accum_write_rt;
    }
  }
  return bool_ret;
}

} // end namespace palf
} // end namespace oceanbase
