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

#include "palf_env_impl.h"
#include <string.h>
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/config/ob_server_config.h"
#include "share/ob_errno.h"
#include "share/ob_occam_thread_pool.h"
#include "log_define.h"
#include "palf_handle_impl_guard.h"             // IPalfHandleImplGuard
#include "palf_handle.h"
#include "log_loop_thread.h"
#include "log_rpc.h"
#include "log_block_pool_interface.h"
#include "log_io_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
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
}

bool PalfDiskOptionsWrapper::need_throttling() const
{
  bool is_need = false;
  ObSpinLockGuard guard(disk_opts_lock_);
  const int64_t trigger_size = disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ * disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ / 100;
  return disk_opts_for_stopping_writing_.is_valid() && cur_unrecyclable_log_disk_size_ > trigger_size;
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
                             fetch_log_engine_(),
                             log_rpc_(),
                             cb_thread_pool_(),
                             log_io_worker_wrapper_(),
                             block_gc_timer_task_(),
                             log_updater_(),
                             monitor_(NULL),
                             disk_options_wrapper_(),
                             disk_not_enough_print_interval_(OB_INVALID_TIMESTAMP),
                             self_(),
                             palf_handle_impl_map_(64),  // 指定min_size=64
                             last_palf_epoch_(0),
                             rebuild_replica_log_lag_threshold_(0),
                             diskspace_enough_(true),
                             tenant_id_(0),
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
    common::ObILogAllocator *log_alloc_mgr,
    ILogBlockPool *log_block_pool,
    PalfMonitorCb *monitor)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  const int64_t io_cb_num = PALF_SLIDING_WINDOW_SIZE * 128;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "PalfEnvImpl is inited twiced", K(ret));
  } else if (OB_ISNULL(base_dir) || !self.is_valid() || NULL == transport
             || OB_ISNULL(log_alloc_mgr) || OB_ISNULL(log_block_pool) || OB_ISNULL(monitor)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid arguments", K(ret), KP(transport), K(base_dir), K(self), KP(transport),
             KP(log_alloc_mgr), KP(log_block_pool), KP(monitor));
  } else if (OB_FAIL(init_log_io_worker_config_(options.disk_options_.log_writer_parallelism_,
                                                tenant_id,
                                                log_io_worker_config_))) {
    PALF_LOG(WARN, "init_log_io_worker_config_ failed", K(options));
  } else if (OB_FAIL(fetch_log_engine_.init(this, log_alloc_mgr))) {
    PALF_LOG(ERROR, "FetchLogEngine init failed", K(ret));
  } else if (OB_FAIL(log_rpc_.init(self, cluster_id, tenant_id, transport))) {
    PALF_LOG(ERROR, "LogRpc init failed", K(ret));
  } else if (OB_FAIL(cb_thread_pool_.init(io_cb_num, this))) {
    PALF_LOG(ERROR, "LogIOTaskThreadPool init failed", K(ret));
  } else if (OB_FAIL(log_io_worker_wrapper_.init(log_io_worker_config_,
                                                 tenant_id,
                                                 cb_thread_pool_.get_tg_id(),
                                                 log_alloc_mgr, this))) {
    PALF_LOG(ERROR, "LogIOWorker init failed", K(ret));
  } else if (OB_FAIL(block_gc_timer_task_.init(this))) {
    PALF_LOG(ERROR, "ObCheckLogBlockCollectTask init failed", K(ret));
  } else if ((pret = snprintf(log_dir_, MAX_PATH_SIZE, "%s", base_dir)) && false) {
    ret = OB_ERR_UNEXPECTED;
  } else if ((pret = snprintf(tmp_log_dir_, MAX_PATH_SIZE, "%s/tmp_dir", log_dir_)) && false) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "error unexpected", K(ret));
  } else if (pret < 0 || pret >= MAX_PATH_SIZE) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(ERROR, "construct log path failed", K(ret), K(pret));
  } else if (OB_FAIL(palf_handle_impl_map_.init("LOG_HASH_MAP", tenant_id))) {
    PALF_LOG(ERROR, "palf_handle_impl_map_ init failed", K(ret));
  } else if (OB_FAIL(log_loop_thread_.init(this))) {
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
  } else {
    log_alloc_mgr_ = log_alloc_mgr;
    log_block_pool_ = log_block_pool;
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
  cb_thread_pool_.destroy();
  log_loop_thread_.destroy();
  block_gc_timer_task_.destroy();
  fetch_log_engine_.destroy();
  log_updater_.destroy();
  log_rpc_.destroy();
  election_timer_.destroy();
  log_alloc_mgr_ = NULL;
  monitor_ = NULL;
  disk_not_enough_print_interval_ = OB_INVALID_TIMESTAMP;
  self_.reset();
  log_dir_[0] = '\0';
  tmp_log_dir_[0] = '\0';
  disk_options_wrapper_.reset();
  rebuild_replica_log_lag_threshold_ = 0;
}

// NB: not thread safe
int PalfEnvImpl::create_palf_handle_impl(const int64_t palf_id,
                                         const AccessMode &access_mode,
                                         const PalfBaseInfo &palf_base_info,
                                         IPalfHandleImpl *&palf_handle_impl)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(palf_meta_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfEnvImpl is not inited", K(ret));
  } else if (OB_FAIL(create_palf_handle_impl_(palf_id, access_mode, palf_base_info,
          NORMAL_REPLICA, palf_handle_impl))) {
    palf_handle_impl = NULL;
  } else {
    PALF_LOG(INFO, "PalfEnvImpl create_palf_handle_impl finished", K(ret), K(palf_id), K(access_mode),
        K(palf_base_info), KPC(this));
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
  } else if (OB_FAIL(create_directory(base_dir))) {
    PALF_LOG(WARN, "prepare_directory_for_creating_ls failed!!!", K(ret), K(palf_id));
  } else if (NULL == (palf_handle_impl = PalfHandleImplFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "alloc palf_handle_impl failed", K(ret));
  } else if (OB_FAIL(palf_handle_impl->init(palf_id, access_mode, palf_base_info, replica_type,
      &fetch_log_engine_, base_dir, log_alloc_mgr_, log_block_pool_, &log_rpc_,
      log_io_worker_wrapper_.get_log_io_worker(palf_id), this, self_, &election_timer_, palf_epoch))) {
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
      remove_directory(base_dir);
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
  const mode_t mode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;
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
  } else if (-1 == (::mkdir(tmp_base_dir, mode))) {
    ret = convert_sys_errno();
    PALF_LOG(WARN, "mkdir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (-1 == (::mkdir(log_dir, mode))) {
    ret = convert_sys_errno();
    PALF_LOG(WARN, "mkdir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (-1 == (::mkdir(meta_dir, mode))) {
    ret = convert_sys_errno();
    PALF_LOG(WARN, "mkdir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (OB_FAIL(rename_with_retry(tmp_base_dir, base_dir))) {
    PALF_LOG(ERROR, "rename tmp dir to normal dir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (OB_FAIL(FileDirectoryUtils::fsync_dir(log_dir_))) {
    PALF_LOG(ERROR, "fsync_dir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else {
    PALF_LOG(INFO, "prepare_directory_for_creating_ls success", K(ret), K(base_dir));
  }
  if (OB_FAIL(ret)) {
    remove_directory(tmp_base_dir);
    remove_directory(base_dir);
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
      if (OB_FAIL(FileDirectoryUtils::is_exists(tmp_log_dir, result))) {
        CLOG_LOG(WARN, "check directory exists failed", KPC(this), K(log_dir));
      } else if (!result) {
        CLOG_LOG(WARN, "directory not exists", KPC(this), K(log_dir));
        break;
      } else if (OB_FAIL(remove_directory_rec(tmp_log_dir, log_block_pool_))) {
        PALF_LOG(WARN, "remove_directory_rec failed", K(tmp_log_dir), KP(log_block_pool_));
      } else {
      }
      if (OB_FAIL(ret) && true == result) {
        PALF_LOG(WARN, "remove directory failed, may be physical disk full", K(ret), KPC(this));
        usleep(100*1000);
      }
    } while (OB_FAIL(ret));
  }
  (void)FileDirectoryUtils::fsync_dir(log_dir_);
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
    ret_code_(OB_SUCCESS)
{}

PalfEnvImpl::LogGetRecycableFileCandidate::~LogGetRecycableFileCandidate()
{
  ret_code_ = OB_SUCCESS;
  min_using_block_id_ = LOG_INVALID_BLOCK_ID;
  min_block_max_scn_.reset();
  min_block_id_ = LOG_INVALID_BLOCK_ID;
  oldest_palf_id_ = INVALID_PALF_ID;
  oldest_block_scn_.reset();
  id_ = -1;
}

bool PalfEnvImpl::LogGetRecycableFileCandidate::operator()(const LSKey &palf_id, IPalfHandleImpl *palf_handle_impl)
{
  bool bool_ret = true;
  if (NULL == palf_handle_impl) {
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
    } else if (min_block_max_scn_.is_valid() && min_block_max_scn_ < min_block_max_scn) {
      PALF_LOG(TRACE, "current palf_handle_impl is not older than previous, skip it", K(min_block_max_scn),
          K(min_block_max_scn_), KPC(palf_handle_impl), K(min_block_id));
    } else {
      id_ = palf_id.id_;
      min_block_id_ = min_block_id;
      min_block_max_scn_ = min_block_max_scn;
      min_using_block_id_ = min_using_block_id;
      PALF_LOG(TRACE, "can be recycable palf_handle_impl", K(id_), K(min_block_id_), K(min_using_block_id_),
          K(min_block_max_scn_), K(base_lsn));
    }
    if (min_block_max_scn.is_valid() && (!oldest_block_scn_.is_valid() || oldest_block_scn_ > min_block_max_scn)) {
      oldest_block_scn_ = min_block_max_scn;
      oldest_palf_id_ = palf_id.id_;
    }
  }
  return bool_ret;
}

int PalfEnvImpl::try_recycle_blocks()
{
  int ret = OB_SUCCESS;
  PalfDiskOptions disk_opts_for_stopping_writing;
  PalfDiskOptions disk_opts_for_recycling_blocks;
  PalfDiskOptionsWrapper::Status status = PalfDiskOptionsWrapper::Status::INVALID_STATUS;
  int64_t sequence = -1;
  disk_options_wrapper_.get_disk_opts(disk_opts_for_stopping_writing,
                                      disk_opts_for_recycling_blocks,
                                      status,
                                      sequence);
  int64_t total_used_size_byte = 0;
  int64_t total_unrecyclable_size_byte = 0;
  int64_t total_size_to_recycle_blocks = disk_opts_for_recycling_blocks.log_disk_usage_limit_size_;
  int64_t total_size_to_stop_write = disk_opts_for_stopping_writing.log_disk_usage_limit_size_;
  int64_t palf_id = 0;
  int64_t maximum_used_size = 0;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_disk_usage_(total_used_size_byte, total_unrecyclable_size_byte,
                                     palf_id, maximum_used_size))) {
    PALF_LOG(WARN, "get_disk_usage_ failed", K(ret), KPC(this));
  } else if (FALSE_IT(disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(total_unrecyclable_size_byte))) {
  } else if (OB_SUCCESS != (tmp_ret = log_io_worker_wrapper_.notify_need_writing_throttling(disk_options_wrapper_.need_throttling()))) {
    PALF_LOG_RET(WARN, tmp_ret, "failed to update_disk_info", K(disk_options_wrapper_));
  } else {
    const int64_t usable_disk_size_to_recycle_blocks =
        total_size_to_recycle_blocks
        * disk_opts_for_recycling_blocks.log_disk_utilization_threshold_ / 100LL;
    const int64_t usable_disk_limit_size_to_stop_writing =
        total_size_to_stop_write
        * disk_opts_for_stopping_writing.log_disk_utilization_limit_threshold_ / 100LL;
    const bool need_recycle =
        usable_disk_size_to_recycle_blocks >= total_used_size_byte ? false : true;
    const bool is_shrinking = disk_options_wrapper_.is_shrinking();
    // Assume that, recycle speed is higher than write speed, therefor, the abnormal case
    // is that, after each 'recycle_blocks_', the 'total_used_size_byte' is one PALF_BLOCK_SIZE
    // more than 'usable_disk_size'.
    const bool curr_diskspace_enough =
        usable_disk_limit_size_to_stop_writing >= total_used_size_byte ? true : false;
    constexpr int64_t MB = 1024 * 1024LL;
    const int64_t print_error_log_disk_size =
        disk_opts_for_stopping_writing.log_disk_usage_limit_size_
        * disk_opts_for_stopping_writing.log_disk_utilization_threshold_ / 100LL;
    const bool need_print_error_log =
        print_error_log_disk_size >= total_used_size_byte ? false : true;

    // step1. change SHRINKING_STATUS to normal
    // 1. when there is no possibility to stop writing,
    // 2. the snapshot of status is SHRINKING_STATUS.
    bool has_recycled = false;
    int64_t oldest_palf_id = INVALID_PALF_ID;
    if (OB_SUCC(ret) && PalfDiskOptionsWrapper::Status::SHRINKING_STATUS == status) {
      if (total_used_size_byte <= usable_disk_size_to_recycle_blocks) {
        disk_options_wrapper_.change_to_normal(sequence);
        PALF_LOG(INFO, "change_to_normal success", K(disk_options_wrapper_),
                 K(total_used_size_byte), K(usable_disk_size_to_recycle_blocks));
      }
    }

    SCN oldest_scn;
    // step2. try recycle blocks
    if (true == need_recycle) {
      if (OB_FAIL(recycle_blocks_(has_recycled, oldest_palf_id, oldest_scn))) {
        PALF_LOG(WARN, "recycle_blocks_ failed", K(usable_disk_size_to_recycle_blocks),
                 K(total_used_size_byte), KPC(this));
      }
    }

    // step3. reset diskspace_enough_.
    if (diskspace_enough_ != curr_diskspace_enough) {
      ATOMIC_STORE(&diskspace_enough_, curr_diskspace_enough);
    }

    // NB: print error log when:
    // 1. write-stop.
    // 2. the used log disk space exceeded the log disk recycle threshold(stop-write PalfDiskOptions) and there is no recycable block.
    if ((false == diskspace_enough_) || (true == need_print_error_log && false == has_recycled)) {
      constexpr int64_t INTERVAL = 1*1000*1000;
      if (palf_reach_time_interval(INTERVAL, disk_not_enough_print_interval_)) {
        int tmp_ret = OB_LOG_OUTOF_DISK_SPACE;
        LOG_DBA_ERROR(OB_LOG_OUTOF_DISK_SPACE, "msg", "log disk space is almost full", "ret", tmp_ret,
            "total_size(MB)", disk_opts_for_recycling_blocks.log_disk_usage_limit_size_/MB,
            "used_size(MB)", total_used_size_byte/MB,
            "used_percent(%)", (total_used_size_byte* 100) / (disk_opts_for_stopping_writing.log_disk_usage_limit_size_ + 1),
            "warn_size(MB)", (total_size_to_recycle_blocks*disk_opts_for_recycling_blocks.log_disk_utilization_threshold_)/100/MB,
            "warn_percent(%)", disk_opts_for_recycling_blocks.log_disk_utilization_threshold_,
            "limit_size(MB)", (total_size_to_recycle_blocks*disk_opts_for_recycling_blocks.log_disk_utilization_limit_threshold_)/100/MB,
            "limit_percent(%)", disk_opts_for_recycling_blocks.log_disk_utilization_limit_threshold_,
            "total_unrecyclable_size_byte(MB)", total_unrecyclable_size_byte/MB,
            "maximum_used_size(MB)", maximum_used_size/MB,
            "maximum_log_stream", palf_id,
            "oldest_log_stream", oldest_palf_id,
            "oldest_scn", oldest_scn);
      }
    } else {
       if (REACH_TIME_INTERVAL(2 * 1000 * 1000L)) {
         PALF_LOG(INFO, "LOG_DISK_OPTION", K(disk_options_wrapper_));
       }
    }

    (void)remove_stale_incomplete_palf_();
  }
  return ret;
}

bool PalfEnvImpl::check_disk_space_enough()
{
  return true == ATOMIC_LOAD(&diskspace_enough_);
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
  char file_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  const char *d_name = entry->d_name;
  MEMCPY(file_name, d_name, strlen(d_name));
  char *tmp = strtok(file_name, "_");
  char *timestamp_str = NULL;
  if (NULL == tmp || NULL == (timestamp_str = strtok(NULL, "_"))) {
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
  if (OB_FAIL(palf_handle_impl_map_.for_each(func_impl))) {
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
  if (OB_FAIL(palf_handle_impl_map_.for_each(func_impl))) {
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
  struct stat st;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid args", K(ret), KP(entry));
  } else if (0 > (pret = snprintf(log_dir, MAX_PATH_SIZE, "%s/%s", palf_env_impl_->log_dir_, entry->d_name))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "snprint failed", K(ret), K(pret), K(entry->d_name));
  } else if (0 != stat(log_dir, &st)) {
    PALF_LOG(INFO, "this entry is not a block", K(ret), K(log_dir), K(errno));
  } else if (false == S_ISDIR(st.st_mode)) {
    PALF_LOG(WARN, "path is not a directory, ignore it", K(ret), K(log_dir), K(st.st_mode));
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
          log_block_pool_, &log_rpc_, log_io_worker_wrapper_.get_log_io_worker(palf_id), this, self_,
          &election_timer_, palf_epoch, is_integrity))) {
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
    while (OB_SUCC(FileDirectoryUtils::is_exists(base_dir, normal_dir_exist))
           && OB_SUCC(FileDirectoryUtils::is_exists(tmp_base_dir, tmp_dir_exist))) {
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
        K(palf_id), KPC(this));;
  } else if (OB_FAIL(check_tmp_log_dir_exist_(tmp_dir_exist))) {
  } else if (false == tmp_dir_exist && (-1 == ::mkdir(tmp_log_dir_, mode))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "mkdir tmp log dir failed", K(ret), KPC(this), K(tmp_log_dir_));
  } else if (0 > (pret = snprintf(src_log_dir, MAX_PATH_SIZE, "%s/%ld", log_dir_, palf_id))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprintf failed, unexpected error", K(ret));
  } else if (0 > (pret = snprintf(dest_log_dir, MAX_PATH_SIZE, "%s/%ld_%ld", tmp_log_dir_, palf_id, timestamp))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "snprintf failed, unexpected error", K(ret));
  } else if (OB_FAIL(rename_with_retry(src_log_dir, dest_log_dir))) {
    PALF_LOG(ERROR, "rename failed", K(ret), KPC(this), K(src_log_dir), K(dest_log_dir));
  } else if (OB_FAIL(FileDirectoryUtils::fsync_dir(log_dir_))) {
    PALF_LOG(ERROR, "fsync_dir failed", K(ret), KPC(this), K(src_log_dir), K(dest_log_dir));
  } else {
  }
  return ret;
}

int PalfEnvImpl::check_tmp_log_dir_exist_(bool &exist) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(FileDirectoryUtils::is_exists(tmp_log_dir_, exist))) {
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

int PalfEnvImpl::get_io_start_time(int64_t &last_working_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    last_working_time = log_io_worker_wrapper_.get_last_working_time();
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
  config.batch_width_ = MAX(default_min_batch_width,
                            tmp_upper_align_div(default_io_batch_width, real_log_writer_parallelism));
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

} // end namespace palf
} // end namespace oceanbase
