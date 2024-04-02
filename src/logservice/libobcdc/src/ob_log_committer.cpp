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
 * Committer
 */

#define USING_LOG_PREFIX OBLOG_COMMITTER

#include "ob_log_committer.h"

#include "lib/string/ob_string.h"            // ObString
#include "storage/tx/ob_trans_define.h"      // ObTransID

#include "ob_log_binlog_record_queue.h" // BRQueue
#include "ob_log_instance.h"            // IObLogErrHandler
#include "ob_log_binlog_record.h"       // ObLogBR
#include "ob_log_part_mgr.h"            // IObLogPartMgr
#include "ob_log_trans_ctx_mgr.h"       // IObLogTransCtxMgr
#include "ob_log_trans_stat_mgr.h"      // IObLogTransStatMgr
#include "ob_log_resource_collector.h"  // IObLogResourceCollector
#include "ob_log_binlog_record_pool.h"  // IObLogBRPool
#include "ob_log_config.h"              // ObLogConfig
#include "ob_log_tenant_mgr.h"          // IObLogTenantMgr
#include "ob_log_trace_id.h"            // ObLogTraceIdGuard

#define _STAT(level, fmt, args...) _OBLOG_COMMITTER_LOG(level, "[STAT] [COMMITTER] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_COMMITTER_LOG(level, "[STAT] [COMMITTER] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)
#define DSTAT(fmt, args...) STAT(DEBUG, fmt, ##args)

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace libobcdc
{

/////////////////////////////////////// ObLogCommitter::CheckpointTask ///////////////////////////////////////

ObLogCommitter::CheckpointTask::CheckpointTask(PartTransTask &task)
{
  tenant_ls_id_ = task.get_tls_id();
  task_type_ = task.get_type();
  timestamp_ = task.get_trans_commit_version();
}

ObLogCommitter::CheckpointTask::~CheckpointTask()
{
  tenant_ls_id_.reset();
  task_type_ = PartTransTask::TASK_TYPE_UNKNOWN;
  timestamp_ = 0;
}

/////////////////////////////////////// ObLogCommitter ///////////////////////////////////////

int64_t ObLogCommitter::g_output_heartbeat_interval =
    ObLogConfig::default_output_heartbeat_interval_msec * _MSEC_;

ObLogCommitter::ObLogCommitter() :
    inited_(false),
    br_queue_(NULL),
    tag_br_alloc_(NULL),
    err_handler_(NULL),
    trans_ctx_mgr_(NULL),
    trans_stat_mgr_(NULL),
    resource_collector_(NULL),
    commit_pid_(0),
    heartbeat_pid_(0),
    stop_flag_(true),
    trans_committer_queue_(),
    trans_committer_queue_cond_(),
    checkpoint_queue_(),
    checkpoint_queue_cond_(),
    checkpoint_queue_allocator_(),
    last_output_checkpoint_(OB_INVALID_VERSION),
    global_heartbeat_seq_(0),
    global_heartbeat_info_queue_(),
    dml_part_trans_task_count_(0),
    ddl_part_trans_task_count_(0),
    dml_trans_count_(0)
{
}

ObLogCommitter::~ObLogCommitter()
{
  destroy();
}

int ObLogCommitter::init(const int64_t start_seq,
    BRQueue *br_queue,
    IObLogResourceCollector *resource_collector,
    IObLogBRPool *tag_br_alloc,
    IObLogTransCtxMgr *trans_ctx_mgr,
    IObLogTransStatMgr *trans_stat_mgr,
    IObLogErrHandler *err_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("committer has been initialized", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(start_seq < 0)
      || OB_ISNULL(br_queue_ = br_queue)
      || OB_ISNULL(resource_collector_ = resource_collector)
      || OB_ISNULL(tag_br_alloc_ = tag_br_alloc)
      || OB_ISNULL(trans_ctx_mgr_ = trans_ctx_mgr)
      || OB_ISNULL(trans_stat_mgr_ = trans_stat_mgr)
      || OB_ISNULL(err_handler_ = err_handler)) {
    LOG_ERROR("invalid arguments", K(start_seq), K(br_queue),
        K(resource_collector), K(tag_br_alloc), K(trans_ctx_mgr), K(trans_stat_mgr), K(err_handler));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(trans_committer_queue_.init(start_seq, OB_MALLOC_MIDDLE_BLOCK_SIZE))) {
    LOG_ERROR("init trans_committer_queue_ fail", KR(ret), K(start_seq));
  } else if (OB_FAIL(checkpoint_queue_.init(start_seq, OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    LOG_ERROR("init checkpoint_queue fail", KR(ret), K(start_seq));
  } else if (OB_FAIL(global_heartbeat_info_queue_.init(start_seq, OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    LOG_ERROR("init global_heartbeat_info_queue fail", KR(ret), K(start_seq));
  } else if (OB_FAIL(checkpoint_queue_allocator_.init(CHECKPOINT_QUEUE_ALLOCATOR_TOTAL_LIMIT,
      CHECKPOINT_QUEUE_ALLOCATOR_HOLD_LIMIT,
      CHECKPOINT_QUEUE_ALLOCATOR_PAGE_SIZE))) {
    LOG_ERROR("init checkpoint_queue_allocator_ fail", KR(ret));
  } else {
    checkpoint_queue_allocator_.set_label(ObModIds::OB_LOG_COMMITTER_CHECKPOINT_QUEUE);
    last_output_checkpoint_ = OB_INVALID_VERSION;
    global_heartbeat_seq_ = start_seq;
    commit_pid_ = 0;
    heartbeat_pid_ = 0;
    dml_part_trans_task_count_ = 0;
    ddl_part_trans_task_count_ = 0;
    dml_trans_count_ = 0;
    stop_flag_ = true;
    inited_ = true;

    LOG_INFO("init committer succ", K(start_seq));
  }

  return ret;
}

void ObLogCommitter::destroy()
{
  stop();

  inited_ = false;
  commit_pid_ = 0;
  heartbeat_pid_ = 0;
  stop_flag_ = true;

  br_queue_ = NULL;
  tag_br_alloc_ = NULL;
  err_handler_ = NULL;
  trans_ctx_mgr_ = NULL;
  trans_stat_mgr_ = NULL;
  resource_collector_ = NULL;

  (void)trans_committer_queue_.destroy();
  (void)checkpoint_queue_.destroy();
  checkpoint_queue_allocator_.destroy();

  last_output_checkpoint_ = OB_INVALID_VERSION;
  global_heartbeat_seq_ = 0;
  (void)global_heartbeat_info_queue_.destroy();

  dml_part_trans_task_count_ = 0;
  ddl_part_trans_task_count_ = 0;
  dml_trans_count_ = 0;
}

int ObLogCommitter::start()
{
  int ret = OB_SUCCESS;
  int pthread_ret = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("committer has not been initialized");
    ret = OB_NOT_INIT;
  } else if (stop_flag_) {
    stop_flag_ = false;


    if (0 != (pthread_ret = pthread_create(&commit_pid_, NULL,
        commit_thread_func_, this))){
      LOG_ERROR("create commit thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
      ret = OB_ERR_UNEXPECTED;
    } else if (0 != (pthread_ret = pthread_create(&heartbeat_pid_, NULL,
        heartbeat_thread_func_, this))){
      LOG_ERROR("create HEARTBEAT thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
      ret = OB_ERR_UNEXPECTED;
    } else {
      LOG_INFO("start Committer commit and HEARTBEAT thread succ");
    }

    if (OB_FAIL(ret)) {
      stop_flag_ = true;
    }
  }

  return ret;
}

void ObLogCommitter::stop()
{
  if (inited_) {
    stop_flag_ = true;

    if (0 != commit_pid_) {
      int pthread_ret = pthread_join(commit_pid_, NULL);

      if (0 != pthread_ret) {
        LOG_ERROR_RET(OB_ERR_SYS, "join Committer commit thread fail", K(commit_pid_), KERRNOMSG(pthread_ret));
      } else {
        LOG_INFO("stop Committer commit thread succ");
      }

      commit_pid_ = 0;
    }

    if (0 != heartbeat_pid_) {
      int pthread_ret = pthread_join(heartbeat_pid_, NULL);

      if (0 != pthread_ret) {
        LOG_ERROR_RET(OB_ERR_SYS, "join Committer HEARTBEAT thread fail", K(heartbeat_pid_), KERRNOMSG(pthread_ret));
      } else {
        LOG_INFO("stop Committer HEARTBEAT thread succ");
      }

      heartbeat_pid_ = 0;
    }
  }
}

void ObLogCommitter::mark_stop_flag()
{
  stop_flag_ = true;
}

int ObLogCommitter::push(PartTransTask *task,
    const int64_t task_count,
    const int64_t timeout,
    ObLogTenant *tenant /* = NULL*/)
{
  UNUSED(timeout);

  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("committer has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)
      || OB_UNLIKELY(! task->is_task_info_valid())
      || OB_UNLIKELY(task_count <= 0)) {
    LOG_ERROR("invalid task", KPC(task), K(task_count));
    ret = OB_INVALID_ARGUMENT;
  }
  // DDL tasks
  // Note: The is_ddl_offline_task() task is an offline task and is not specially handled here
  else if (task->is_ddl_trans()) {
   const int64_t seq = task->get_global_trans_seq();

   if (OB_FAIL(trans_committer_queue_.set(seq, task))) {
    LOG_ERROR("trans_committer_queue_ set fail", KR(ret), K(seq), KPC(task),
        "begin_sn", trans_committer_queue_.begin_sn(),
        "end_sn", trans_committer_queue_.end_sn(),
        KPC(tenant));
   } else {
     trans_committer_queue_cond_.signal();
   }
   // Increase the number of DDL transactions
   (void)ATOMIC_AAF(&ddl_part_trans_task_count_, 1);
  }
  // DML task
  else if (task->is_dml_trans()) {
    (void)ATOMIC_AAF(&dml_part_trans_task_count_, task_count);
    (void)ATOMIC_AAF(&dml_trans_count_, 1);
    // DML does not allow tenant to be invalid
    const int64_t seq = task->get_global_trans_seq();

   if (OB_FAIL(trans_committer_queue_.set(seq, task))) {
    LOG_ERROR("trans_committer_queue_ set fail", KR(ret), K(seq), KPC(task),
        "begin_sn", trans_committer_queue_.begin_sn(),
        "end_sn", trans_committer_queue_.end_sn(),
        KPC(tenant));
   } else {
     trans_committer_queue_cond_.signal();
   }
  }
  // push heartbeat task
  else if (task->is_global_heartbeat() || task->is_ls_heartbeat()) {
    if (OB_FAIL(push_heartbeat_(*task))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push_heartbeat_ fail", KR(ret), K(*task));
      }
    } else {}
  }
  // push partitin offline task
  else if (task->is_offline_ls_task()) {
    if (OB_FAIL(push_offline_ls_task_(*task))) {
      LOG_ERROR("push_offline_ls_task_ fail", KR(ret), KPC(task));
    }
  }
  // Processing of unserviced service tasks
  else if (task->is_not_served_trans()) {
    if (OB_FAIL(handle_not_served_trans_(*task))) {
      LOG_ERROR("handle_not_served_trans_ fail", KR(ret), KPC(task));
    }
  } else if (task->is_ls_op_trans()) {
    if (OB_FAIL(push_ls_table_task_(*task))) {
      LOG_ERROR("push_ls_table_task_ fail", KR(ret), KPC(task));
    }
  } else {
    LOG_ERROR("unknown part trans task", K(*task));
    ret = OB_NOT_SUPPORTED;
  }

  return ret;
}

int ObLogCommitter::alloc_checkpoint_task_(PartTransTask &task, CheckpointTask *&checkpoint_task)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  int64_t size = 0;
  checkpoint_task = NULL;

  size = sizeof(CheckpointTask);

  if (OB_ISNULL(ptr = checkpoint_queue_allocator_.alloc(size))) {
    LOG_ERROR("alloc memory for CheckpointTask fail", K(size));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    checkpoint_task = new (ptr) CheckpointTask(task);
  }
  return ret;
}

void ObLogCommitter::free_checkpoint_task_(CheckpointTask *checkpoint_task)
{
  if (NULL != checkpoint_task) {
    checkpoint_task->~CheckpointTask();
    checkpoint_queue_allocator_.free(checkpoint_task);
    checkpoint_task = NULL;
  }
}

int ObLogCommitter::update_checkpoint_info_(PartTransTask &task)
{
  int ret = OB_SUCCESS;
  CheckpointTask *checkpoint_task = NULL;
  int64_t checkpoint_seq = task.get_checkpoint_seq();

  if (OB_UNLIKELY(checkpoint_seq < 0)) {
    LOG_ERROR("task checkpoint sequence is invalid", K(checkpoint_seq), K(task));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(alloc_checkpoint_task_(task, checkpoint_task))) {
    LOG_ERROR("alloc_checkpoint_task_ fail", KR(ret), K(task));
  } else if (OB_ISNULL(checkpoint_task)) {
    LOG_ERROR("invalid checkpoint_task", K(checkpoint_task));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(checkpoint_queue_.set(checkpoint_seq, checkpoint_task))) {
    LOG_ERROR("set checkpoint_queue_ fail", KR(ret), K(checkpoint_seq), K(checkpoint_task));
    // 释放内存
    free_checkpoint_task_(checkpoint_task);
    checkpoint_task = NULL;
  } else {
    checkpoint_queue_cond_.signal();
  }

  return ret;
}

// Handles both GLOBAL and PART types of heartbeats
int ObLogCommitter::push_heartbeat_(PartTransTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! task.is_task_info_valid())) {
    LOG_ERROR("invalid task", K(task));
    ret = OB_INVALID_DATA;
  } else {
    ret = recycle_task_directly_(task);
  }
  return ret;
}

int ObLogCommitter::handle_not_served_trans_(PartTransTask &task)
{
  const bool can_async_recycle = task.is_contain_empty_redo_log();

  return recycle_task_directly_(task, can_async_recycle);
}

// recycle task directly
int ObLogCommitter::recycle_task_directly_(PartTransTask &task, const bool can_async_recycle)
{
  int ret = OB_SUCCESS;
  int revert_ret = OB_SUCCESS;

  // Only single-threaded calls can be made here, and the GLOBAL HEARTBEAT distribution must be single-threaded
  if (OB_FAIL(record_global_heartbeat_info_(task))) {
    LOG_ERROR("record_global_heartbeat_info_ fail", KR(ret), K(task));
  }
  // upadte checkpoint info
  else if (OB_FAIL(update_checkpoint_info_(task))) {
    LOG_ERROR("update_checkpoint_info_ fail", KR(ret), K(task));
  }

  if (can_async_recycle) {
    if (OB_NOT_NULL(resource_collector_)
        && OB_SUCCESS != (revert_ret = resource_collector_->revert(&task))) {
      if (OB_IN_STOP_STATE != revert_ret) {
        LOG_ERROR("revert PartTransTask fail", K(revert_ret), K(task));
      }
      ret = OB_SUCCESS == ret ? revert_ret : ret;
    }
  }

  return ret;
}

int ObLogCommitter::record_global_heartbeat_info_(PartTransTask &task)
{
  int ret = OB_SUCCESS;

  // 1. checkpoint_seq of the global heartbeat logging task
  // 2. checkpoint_seq is uniformly +1, then shifted 1 bit left
  // (1) +1: to avoid the global heartbeat sequence number being exactly 0, which makes it impossible to pop
  // (2) Shift one bit left: avoid checkpoint_seq is odd, set successfully aligned with default address, get will be minus 1
  if (task.is_global_heartbeat()) {
    int64_t checkpoint_seq = (task.get_checkpoint_seq() + 1) << 1;

    if (OB_FAIL(global_heartbeat_info_queue_.set(global_heartbeat_seq_, reinterpret_cast<int64_t *>(checkpoint_seq)))) {
      LOG_ERROR("set global_heartbeat_info_queue_ fail", KR(ret), K(global_heartbeat_seq_), K(checkpoint_seq));
    } else {
      ++global_heartbeat_seq_;
    }
  }

  return ret;
}

int ObLogCommitter::push_offline_ls_task_(PartTransTask &task)
{
  int ret = OB_SUCCESS;

  // logservice::TenantLSID should be valid
  if (OB_UNLIKELY(! task.get_tls_id().is_valid())) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid offline ls task", KR(ret), K(task));
  } else {
    ret = recycle_task_directly_(task);
  }
  return ret;
}

int ObLogCommitter::push_ls_table_task_(PartTransTask &task)
{
  int ret = OB_SUCCESS;

  // logservice::TenantLSID should be valid
  if (OB_UNLIKELY(! task.get_tls_id().is_valid())) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid ls_table task", KR(ret), K(task));
  } else {
    ret = recycle_task_directly_(task);
  }

  return ret;
}

void *ObLogCommitter::commit_thread_func_(void *arg)
{
  if (NULL != arg) {
    ObLogCommitter *committer = static_cast<ObLogCommitter *>(arg);
    committer->commit_routine();
  }

  return NULL;
}

void *ObLogCommitter::heartbeat_thread_func_(void *arg)
{
  if (NULL != arg) {
    ObLogCommitter *committer = static_cast<ObLogCommitter *>(arg);
    committer->heartbeat_routine();
  }

  return NULL;
}

int ObLogCommitter::next_checkpoint_task_(CheckpointTask *&task)
{
  int ret = OB_SUCCESS;
  bool popped = false;
  CheckpointQueuePopFunc pop_func;

  task = NULL;
  if (OB_FAIL(checkpoint_queue_.pop(pop_func, task, popped))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("pop from CheckpointQueue fail", KR(ret), K(popped));
    } else {
      // not element, normal
      ret = OB_SUCCESS;
      task = NULL;
    }
  } else if (! popped) {
    // No pop out element
    task = NULL;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid task", K(task));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // success
  }

  return ret;
}

int ObLogCommitter::handle_checkpoint_task_(CheckpointTask &task)
{
  int ret = OB_SUCCESS;
  int64_t cur_checkpoint_seq = checkpoint_queue_.begin_sn() - 1;
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;


  DSTAT("[HEARTBEAT] [POP_TASK]", K(task),  "seq", cur_checkpoint_seq);

  // If it is a heartbeat task, update the checkpoint timestamp, and pop the corresponding checkpoint information
  if (task.is_global_heartbeat()) {
    const int64_t global_heartbeat_ts = task.timestamp_;

    if (OB_ISNULL(tenant_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant_mgr should not be null", KR(ret), K(tenant_mgr));
    } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == global_heartbeat_ts)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("global_heartbeat_ts should be valid");
    } else if (OB_FAIL(tenant_mgr->update_committer_global_heartbeat(global_heartbeat_ts))) {
      LOG_ERROR("tenant_mgr update_committer_global_heartbeat failed", KR(ret), K(global_heartbeat_ts));
    } else if (OB_FAIL(next_global_heartbeat_info_(cur_checkpoint_seq))) {
      LOG_ERROR("next_global_heartbeat_info_ fail", KR(ret), K(cur_checkpoint_seq), K(task));
    }
  }
  // If it is a delete partition task, then notify PartMgr to reclaim the partition
  else if (task.is_offline_ls_task()) {
    if (OB_FAIL(handle_offline_checkpoint_task_(task))) {
      LOG_ERROR("handle_offline_checkpoint_task_ fail", KR(ret), K(task));
    }
  } else {
    // Other tasks are not processed
  }

  return ret;
}

int ObLogCommitter::next_global_heartbeat_info_(const int64_t cur_checkpoint_seq)
{
  int ret = OB_SUCCESS;
  bool popped = false;
  GHeartbeatInfoQueuePopFunc pop_func;
  const int64_t *checkpoint_seq = NULL;

  // For the global heartbeat, the checkpoint_seq is first put into the GlobalHeartbeatInfoQueue
  // So the global heartbeat that is processed to the CheckpointQueue, must exist here
  if (OB_FAIL(global_heartbeat_info_queue_.pop(pop_func, checkpoint_seq, popped))) {
    LOG_ERROR("pop from GlobalHeartbeatInfoQueue fail", KR(ret), K(popped));
  } else if (! popped) {
    // No pop out element
    LOG_ERROR("pop from GlobalHeartbeatInfoQueue fail", KR(ret), K(popped));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(checkpoint_seq)) {
    LOG_ERROR("checkpoint_seq is NULL", K(checkpoint_seq));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // succ
    const int64_t next_checkpoint_seq = (reinterpret_cast<const int64_t>(checkpoint_seq)) / 2 - 1;

    if (OB_UNLIKELY(cur_checkpoint_seq != next_checkpoint_seq)) {
      LOG_ERROR("global heartbeat cur_checkpoint_seq is not equal to next_checkpoint_seq", K(cur_checkpoint_seq),
          K(next_checkpoint_seq), "hb_begin_sn", global_heartbeat_info_queue_.begin_sn(),
          "hb_end_sn", global_heartbeat_info_queue_.end_sn());
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

void ObLogCommitter::print_global_heartbeat_info_()
{
  int ret = OB_SUCCESS;
  const int64_t *checkpoint_seq = NULL;
  int64_t next_checkpoint_seq = 0;
  int64_t next_seq = global_heartbeat_info_queue_.begin_sn();
  int64_t end_seq = global_heartbeat_info_queue_.end_sn();
  int64_t checkpoint_queue_begin_sn = checkpoint_queue_.begin_sn();
  int64_t checkpoint_queue_end_sn = checkpoint_queue_.end_sn();

  ret = global_heartbeat_info_queue_.get(next_seq, checkpoint_seq);

  // The next one is not ready, invalid value
  if (OB_ERR_OUT_OF_UPPER_BOUND == ret || (OB_SUCC(ret) && NULL == checkpoint_seq)) {
    next_checkpoint_seq = -1;
  } else {
    // Refer to generation_rules of record_global_heartbeat_info_
    next_checkpoint_seq = (reinterpret_cast<const int64_t>(checkpoint_seq)) / 2 - 1;
  }
  int64_t delta = -1;
  if (-1 == next_checkpoint_seq) {
    delta = -1;
  } else {
    delta = next_checkpoint_seq - checkpoint_queue_begin_sn;
  }

  _ISTAT("[CHECKPOINT_QUEUE] NEXT_SEQ=%ld NEXT_HEARTBEAT=%ld DELAT=%ld "
      "QUEUE(HB=%ld,TOTAL=%ld)",
      checkpoint_queue_begin_sn, next_checkpoint_seq, delta,
      end_seq - next_seq,
      checkpoint_queue_end_sn - checkpoint_queue_begin_sn);
}

int ObLogCommitter::handle_offline_checkpoint_task_(CheckpointTask &task)
{
  int ret = OB_SUCCESS;
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  if (OB_UNLIKELY(! task.is_offline_ls_task())) {
    LOG_ERROR("invalid argument which is not offline partition task", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("tenant mgr is NULL", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // See alloc_checkpoint_task_allocate_memory for details
    const logservice::TenantLSID &tls_id = task.tenant_ls_id_;

    // Go offline and reclaim the partition resources, requiring a successful recovery
    // Since the previous data has been exported, there are no transaction dependencies, so the recovery must be successful
    if (OB_FAIL(tenant_mgr->recycle_ls(tls_id))) {
      LOG_ERROR("recycle_partition fail", KR(ret), K(tls_id), K(task));
    } else {
      // success
    }

    LOG_INFO("handle ls offline task", KR(ret), K(tls_id), K(task));
  }

  return ret;
}

int ObLogCommitter::dispatch_heartbeat_binlog_record_(const int64_t heartbeat_timestamp)
{
  int ret = OB_SUCCESS;
  ObLogBR *br = NULL;
  // heartbeat ObLogBR does not require cluster_id, tenant_id
  const uint64_t cluster_id = 1;
  const uint64_t tenant_id = 1;
  const uint64_t row_index = 0;
  ObString trace_id;
  ObString trace_info;
  ObString unique_id;
  const int64_t ddl_schema_version = 0;
  if (REACH_TIME_INTERVAL(3 * _SEC_)) {
    ISTAT("[HEARTBEAT]", "DELAY", NTS_TO_DELAY(heartbeat_timestamp), "heartbeat", NTS_TO_STR(heartbeat_timestamp));
  }

  if (OB_ISNULL(tag_br_alloc_)) {
    LOG_ERROR("invalid tag_br_alloc_ fail", KR(ret), K(tag_br_alloc_));
    ret = OB_INVALID_ERROR;
  } else if (OB_FAIL(tag_br_alloc_->alloc(br, NULL))) {
    LOG_ERROR("alloc binlog record for HEARTBEAT fail", KR(ret));
  } else if (OB_ISNULL(br)) {
    LOG_ERROR("alloc binlog record for HEARTBEAT fail", KR(ret), K(br));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(br->init_data(HEARTBEAT, cluster_id, tenant_id, row_index, trace_id, trace_info, unique_id,
          ddl_schema_version, heartbeat_timestamp))) {
    LOG_ERROR("init HEARTBEAT binlog record fail", KR(ret), K(heartbeat_timestamp),
        K(cluster_id), K(tenant_id), K(row_index), K(ddl_schema_version), K(trace_id), K(trace_info),
        K(unique_id));
  } else if (OB_FAIL(push_br_queue_(br))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push_br_queue_ fail", KR(ret));
    }
  } else {
    br = NULL;
  }

  if (OB_FAIL(ret)) {
    if (NULL != br) {
      tag_br_alloc_->free(br);
      br = NULL;
    }
  }
  return ret;
}

void ObLogCommitter::heartbeat_routine()
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("committer has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    // Heartbeat thread that periodically generates heartbeat messages
    while (! stop_flag_ && OB_SUCCESS == ret) {
      ObLogTraceIdGuard trace_guard;
      CheckpointTask *task = NULL;
      bool need_continue = false;

      // fetch next task
      if (OB_FAIL(next_checkpoint_task_(task))) {
        LOG_ERROR("next_checkpoint_task_ fail", KR(ret));
      } else if (NULL == task) {
        // next task is not ready
        need_continue = false;
      } else {
        need_continue = true;

        // Process checkpoint tasks
        if (OB_FAIL(handle_checkpoint_task_(*task))) {
          LOG_ERROR("handle_checkpoint_task_ fail", KR(ret), KPC(task));
        } else {
          // Free task memory when processing task is complete
          free_checkpoint_task_(task);
          task = NULL;
        }
      }

      // periodically send a heartbeat binlog record
      // checkpoint timestamp is invalid for the first time, here ensure that the heartbeat is sent as soon as the checkpoint timestamp is valid
      if (OB_SUCC(ret)) {
        if (REACH_TIME_INTERVAL(g_output_heartbeat_interval)) {
          int64_t output_checkpoint = OB_INVALID_TIMESTAMP;

          if (OB_FAIL(calculate_output_checkpoint_(output_checkpoint))) {
            LOG_ERROR("calculate_output_checkpoint_ failed", KR(ret), K(output_checkpoint));
          } else if (OB_INVALID_TIMESTAMP != output_checkpoint) {
            if (OB_FAIL(dispatch_heartbeat_binlog_record_(output_checkpoint))) {
              if (OB_IN_STOP_STATE != ret) {
                LOG_ERROR("dispatch_heartbeat_binlog_record_ fail", KR(ret), K(output_checkpoint));
              }
            }
          }
        }
      }

      if (REACH_TIME_INTERVAL(PRINT_GLOBAL_HEARTBEAT_CHECKPOINT_INTERVAL)) {
        print_global_heartbeat_info_();
      }

      // If there is no need to continue processing the task, wait for a while
      if (OB_SUCCESS == ret && ! need_continue) {
        checkpoint_queue_cond_.timedwait(g_output_heartbeat_interval);
      }
    } // while

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
      err_handler_->handle_error(ret, "committer HEARTBEAT thread exits, err=%d", ret);
      stop_flag_ = true;
    }
  }

  LOG_INFO("committer HEARTBEAT thread exits", KR(ret), K_(stop_flag));
}

void ObLogCommitter::commit_routine()
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("committer has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    int64_t commit_trans_count = 0;

    while (OB_SUCC(ret) && ! stop_flag_) {
      ObLogTraceIdGuard trace_guard;
      PartTransTask *part_trans_task = NULL;
      int64_t next_seq = trans_committer_queue_.begin_sn();
      ret = trans_committer_queue_.get(next_seq, part_trans_task);

      if (OB_ERR_OUT_OF_UPPER_BOUND == ret || (OB_SUCCESS == ret && NULL == part_trans_task)) {
        // data not ready
        ret = OB_SUCCESS;
        trans_committer_queue_cond_.timedwait(DATA_OP_TIMEOUT);
      } else if (OB_FAIL(ret)) {
        LOG_ERROR("get task from commit queue fail", KR(ret), KPC(part_trans_task),
            "begin_sn", trans_committer_queue_.begin_sn(), "end_sn", trans_committer_queue_.end_sn());
      } else {
        // get a valid & ready trans
        if (OB_FAIL(handle_when_trans_ready_(part_trans_task, commit_trans_count))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("handle_when_trans_ready_ fail", KR(ret), KPC(part_trans_task),
                K(commit_trans_count));
          }
        } else {
          bool popped = false;
          bool use_lock = true;
          PartTransTask *pop_task = NULL;
          CommitQueuePopFunc pop_func;

          // trans can definitely pop out
          if (OB_FAIL(trans_committer_queue_.pop(pop_func, pop_task, popped, use_lock))) {
            LOG_ERROR("pop task from commit queue fail", KR(ret), KPC(pop_task), K(popped), K(use_lock),
                "begin_sn", trans_committer_queue_.begin_sn(), "end_sn", trans_committer_queue_.end_sn());
          } else if (OB_UNLIKELY(! popped)) {
            LOG_ERROR("pop task from commit queue fail", "tenant_id", part_trans_task->get_tenant_id(),
                "begin_sn", trans_committer_queue_.begin_sn(), "end_sn", trans_committer_queue_.end_sn());
            ret = OB_ERR_UNEXPECTED;
          } else {
            // succ
          }
        }
      }
    } // while
  }

  if (stop_flag_) {
    ret = OB_IN_STOP_STATE;
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "Committer commit thread exits, err=%d", ret);
    stop_flag_ = true;
  }

  if (OB_FAIL(ret)) {
    LOG_INFO("Committer commit thread exits", KR(ret), K_(stop_flag));
  }
}

int ObLogCommitter::handle_when_trans_ready_(PartTransTask *task,
    int64_t &commit_trans_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("committer has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("task is null", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
    const uint64_t tenant_id = task->get_tenant_id();

    if (OB_FAIL(get_tenant_compat_mode(tenant_id, compat_mode, stop_flag_))) {
      LOG_ERROR("get_tenant_compat_mode fail", KR(ret), "tenant_id", tenant_id,
          "compat_mode", print_compat_mode(compat_mode), KPC(task));
    } else {
      lib::CompatModeGuard g(compat_mode);

      // handle ready task
      if (OB_FAIL(handle_task_(task))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("handle_task_ fail", KR(ret), "compat_mode", print_compat_mode(compat_mode));
        }
      } else {
        ++commit_trans_count;
      }
    }
  }

  return ret;
}

int ObLogCommitter::handle_ddl_task_(PartTransTask *ddl_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ddl_task)
      || (OB_UNLIKELY(! ddl_task->is_ddl_trans()))) {
    LOG_ERROR("invalid ddl task", KPC(ddl_task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Subtract the number of DDL transactions
    ATOMIC_DEC(&ddl_part_trans_task_count_);
    TransCtx *trans_ctx = NULL;
    const uint64_t tenant_id = ddl_task->get_tenant_id();
    const ObTransID &trans_id = ddl_task->get_trans_id();
    int64_t local_schema_version = OB_INVALID_TIMESTAMP;

    // Advance the transaction context state to COMMITTED
    if (OB_FAIL(trans_ctx_mgr_->get_trans_ctx(tenant_id, trans_id, trans_ctx, false))) {
      LOG_ERROR("get_trans_ctx fail", KR(ret), K(tenant_id), K(trans_id), KPC(trans_ctx), KPC(ddl_task));
    } else if (OB_FAIL(trans_ctx->commit())) {
      LOG_ERROR("TransCtx::commit fail", KR(ret), K(trans_id), KPC(trans_ctx), KPC(ddl_task));
    } else {}

    if (OB_SUCC(ret) && ddl_task->is_ddl_trans()) {
      // Set the reference count to: number of statements + 1
      ddl_task->set_ref_cnt(ddl_task->get_stmt_num() + 1);
      local_schema_version = ddl_task->get_local_schema_version();

      // Iterate through each statement of the DDL
      DdlStmtTask *stmt_task = static_cast<DdlStmtTask *>(ddl_task->get_stmt_list().head_);
      while (NULL != stmt_task && OB_SUCCESS == ret) {
        if (OB_FAIL(handle_ddl_stmt_(*stmt_task))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("handle_ddl_stmt_ fail", KR(ret), KPC(stmt_task));
          }
        } else {
          stmt_task = static_cast<DdlStmtTask *>(stmt_task->get_next());
        }
      }
    } // is_ddl_trans

    // update local cur_schema_version
    // host.update_committer_cur_schema_version(local_schema_version);
    // LOG_DEBUG("update_committer_cur_schema_version", K(local_schema_version), KPC(ddl_task));

    // revert TransCtx
    if (NULL != trans_ctx) {
      int revert_ret = OB_SUCCESS;
      if (OB_SUCCESS != (revert_ret = trans_ctx_mgr_->revert_trans_ctx(trans_ctx))) {
        LOG_ERROR("revert_trans_ctx fail", K(revert_ret), K(trans_ctx));
        ret = OB_SUCCESS == ret ? revert_ret : ret;
      } else {
        trans_ctx = NULL;
      }
    }
  }

  return ret;
}

int ObLogCommitter::handle_ddl_stmt_(DdlStmtTask &stmt_task)
{
  int ret = OB_SUCCESS;
  ObLogBR *br = stmt_task.get_binlog_record();

  if (OB_ISNULL(br)) {
    LOG_ERROR("invalid DDL binlog record", K(stmt_task));
    ret = OB_ERR_UNEXPECTED;
  }
  // If the binlog record is invalid, the binlog record resource is recycled
  else if (! br->is_valid()) {
    if (OB_FAIL(revert_binlog_record_(br))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("revert_binlog_record_ fail", KR(ret), K(br), K(stmt_task));
      }
    } else {
      br = NULL;
    }
  } else {
    // If the binlog record is valid, output
    // DDL push to the next element in the BRQueue, the next element in the chain is empty
    br->set_next(NULL);

    if (OB_FAIL(push_br_queue_(br))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push_br_queue_ fail", KR(ret), K(br));
      }
    } else {
      br = NULL;
    }
  }

  return ret;
}

int ObLogCommitter::revert_binlog_record_(ObLogBR *br)
{
  int ret = OB_SUCCESS;
  IBinlogRecord *br_data = NULL;

  if (OB_ISNULL(resource_collector_)) {
    LOG_ERROR("invalid resource collector", K(resource_collector_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(br)) {
    LOG_ERROR("binlog record is invalid", K(br));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(br_data = br->get_data())) {
    LOG_ERROR("binlog record data is invalid", K(br));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int record_type = br_data->recordType();

    if (OB_FAIL(resource_collector_->revert(record_type, br))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("revert binlog record fail", KR(ret), K(br),
            "record_type", print_record_type(record_type));
      }
    } else {
      br = NULL;
    }
  }

  return ret;
}

int ObLogCommitter::handle_task_(PartTransTask *participants)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObLogCommitter handle_task", KPC(participants));

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(participants)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (participants->is_ddl_trans()) {
    if (OB_FAIL(handle_ddl_task_(participants))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle_ddl_task_ fail", KR(ret), KPC(participants));
      }
    }
  } else if (participants->is_dml_trans()) {
    if (OB_FAIL(handle_dml_task_(participants))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle_dml_task_ fail", KR(ret), KPC(participants));
      }
    }
  } else {
    LOG_ERROR("not supported task", KPC(participants));
    ret = OB_NOT_SUPPORTED;
  }
  if (OB_SUCC(ret)) {
    // MUST update tenant_trans_commit_version first and then update checkpoint_queue_(in after_trans_handled_)
    if (OB_FAIL(update_tenant_trans_commit_version_(*participants))) {
      LOG_ERROR("update_tenant_trans_commit_version_ failed", KR(ret), KPC(participants));
    } else if (OB_FAIL(after_trans_handled_(participants))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("after_trans_handled_ failed", KR(ret));
      }
    } else {
      // no more access to participants
    }
  }

  return ret;
}

int ObLogCommitter::handle_dml_task_(PartTransTask *participants)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(participants)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const uint64_t cluster_id = participants->get_cluster_id();
    int64_t trans_commit_version = participants->get_trans_commit_version();
    const uint64_t tenant_id = participants->get_tenant_id();
    TransCtx *trans_ctx = NULL;
    const ObTransID &trans_id = participants->get_trans_id();
    int64_t valid_br_num = 0;
    PartTransTask *part = participants;
    int64_t part_trans_task_count = 0;
    int64_t valid_part_trans_task_count = 0;

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }

    // After processing all participants, update the transaction context information before pushing to the user queue
    if (OB_SUCC(ret)) {
      // Advance the transaction context state to COMMITTED
      if (OB_FAIL(trans_ctx_mgr_->get_trans_ctx(tenant_id, trans_id, trans_ctx, false))) {
        LOG_ERROR("get_trans_ctx fail", K(ret), K(tenant_id), K(trans_id), KPC(trans_ctx), KPC(participants));
      } else if (OB_FAIL(trans_ctx->commit())) {
        LOG_ERROR("TransCtx::commit fail", K(ret), K(trans_id), KPC(trans_ctx), KPC(participants));
      } else {}
    }

    if (OB_SUCC(ret)) {
      valid_br_num = trans_ctx->get_total_br_count();
      part_trans_task_count = trans_ctx->get_ready_participant_count();
      valid_part_trans_task_count = trans_ctx->get_ready_participant_count();
    }

    // Statistical Information
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_trans_stat_(participants->get_tls_id(), valid_br_num))) {
        LOG_ERROR("do trans stat fail", KR(ret), K(valid_br_num));
      }
    }

    // Place the Binlog Record chain in the user queue
    // Binlog Record may be recycled at any time
    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_binlog_record_list_(*trans_ctx, cluster_id, valid_part_trans_task_count,
              tenant_id, trans_commit_version))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("commit_binlog_record_list_ fail", KR(ret), KPC(trans_ctx),
              K(valid_br_num), K(valid_part_trans_task_count),
              K(tenant_id), K(trans_commit_version));
        }
      } else {
        // succ
      }
    }

    // Counting the number of partitioned tasks, reducing the number of participants
    (void)ATOMIC_AAF(&dml_part_trans_task_count_, -part_trans_task_count);
    (void)ATOMIC_AAF(&dml_trans_count_, -1);

    // revert TransCtx
    if (NULL != trans_ctx) {
      int revert_ret = OB_SUCCESS;
      if (OB_SUCCESS != (revert_ret = trans_ctx_mgr_->revert_trans_ctx(trans_ctx))) {
        LOG_ERROR("revert_trans_ctx fail", K(revert_ret), K(trans_ctx));
        ret = OB_SUCCESS == ret ? revert_ret : ret;
      } else {
        trans_ctx = NULL;
      }
    }
  }

  return ret;
}

int ObLogCommitter::update_tenant_trans_commit_version_(const PartTransTask &participants)
{
  int ret = OB_SUCCESS;
  const logservice::TenantLSID &tls_id = participants.get_tls_id();
  const int64_t trans_commit_version = participants.get_trans_commit_version();
  const transaction::ObTransID &tx_id = participants.get_trans_id();

  // expect valid tenant_ls_id and commit_version for all part_trans_task expect global_heartbeat_tasl/offline_ls_task
  if (OB_UNLIKELY(! tls_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant_ls_id for checkpoint_task", KR(ret), K(participants));
//  } else if (OB_UNLIKELY(OB_INVALID_VERSION == commit_version)) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_ERROR("invalid commit_version", KR(ret), K(participants));
  } else {
    ObLogTenantGuard guard;
    ObLogTenant *tenant = NULL;
    const uint64_t tenant_id = tls_id.get_tenant_id();

    // TODO not use tenant_guard if has performance issue.
    if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("get tenant_guard failed", KR(ret));
      } else {
        LOG_WARN("tenant not exist while update_tenant_trans_commit_version, may already dropped",
            KR(ret), K(tenant_id), K(trans_commit_version));
      }
    } else if (OB_ISNULL(tenant = guard.get_tenant())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant is null", KR(ret), K(participants));
    } else if (OB_UNLIKELY(! tenant->is_serving())) {
      LOG_INFO("ignore update tenant_trans_commit_version for not serving tenant", K(tenant_id));
    } else if (OB_FAIL(tenant->update_committer_trans_commit_version(trans_commit_version))) {
      LOG_ERROR("update_tenant_trans_commit_version for tenant failed", KR(ret),
          K(trans_commit_version), K_(last_output_checkpoint), K(participants), KPC(tenant));
    } else {
      // success
      LOG_DEBUG("update tenant_output_checkpoint by trans succ", K(tenant_id), K(tx_id), K(trans_commit_version));
    }
  }

  return ret;
}

int ObLogCommitter::after_trans_handled_(PartTransTask *participants)
{
  int ret = OB_SUCCESS;
  // Update Commit information
  // NOTE: Since the above guarantees that the reference count is greater than the number of Binlog Records, the list of participants here must be valid
  PartTransTask *part_trans_task = participants;
  const bool is_ddl_trans = part_trans_task->is_ddl_trans();

  while (OB_SUCC(ret) && ! stop_flag_ && OB_NOT_NULL(part_trans_task)) {
    PartTransTask *next = part_trans_task->next_task();

    // update checkpint info
    if (OB_FAIL(update_checkpoint_info_(*part_trans_task))) {
      LOG_ERROR("update_checkpoint_info_ fail", KR(ret), KPC(part_trans_task));
    }
    // Decrement the reference count after the Commit message is updated
    // If the reference count is 0, the partition transaction is recycled
    else if (0 == part_trans_task->dec_ref_cnt()) {
      if (OB_FAIL(resource_collector_->revert(part_trans_task))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("revert PartTransTask fail", KR(ret), K(part_trans_task));
        }
      } else {
        part_trans_task = NULL;
      }
    }

    part_trans_task = next;
  }

  if (stop_flag_) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogCommitter::do_trans_stat_(const logservice::TenantLSID &tls_id,
    const int64_t total_stmt_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(trans_stat_mgr_)) {
    LOG_ERROR("trans_stat_mgr_ is null", K(trans_stat_mgr_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(! tls_id.is_valid()) || OB_UNLIKELY(total_stmt_cnt < 0)) {
    LOG_ERROR("invalid argument", K(tls_id), K(total_stmt_cnt));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // A transaction must belong to only one tenant, distributed transactions can cross databases, but not cross tenants
    const uint64_t tenant_id = tls_id.get_tenant_id();
    trans_stat_mgr_->do_rps_stat_after_filter(total_stmt_cnt);
    if (OB_FAIL(trans_stat_mgr_->do_tenant_rps_stat_after_filter(tenant_id, total_stmt_cnt))) {
      LOG_ERROR("do tenant rps stat after filter fail", KR(ret), K(tenant_id), K(total_stmt_cnt));
    }
  }

  return ret;
}

int ObLogCommitter::commit_binlog_record_list_(TransCtx &trans_ctx,
    const uint64_t cluster_id,
    const int64_t part_trans_task_count,
    const uint64_t tenant_id,
    const int64_t trans_commit_version)
{
  int ret = OB_SUCCESS;
  // COMMIT does not require trace id trace_info unique_id
  // BEGIN does not require trace_id, trace_info where unique_id records the transaction ID, as a transaction-level unique ID
  // Purpose: Support Oracle smooth migration, use transaction table in OB to Oracle link to achieve idempotent control
  ObString trace_id;
  ObString trace_info;
  ObString unique_id ;
  const ObTransID trans_id = trans_ctx.get_trans_id();
  const ObString &trans_id_str = trans_ctx.get_trans_id_str();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("committer has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(trans_commit_version <= 0)) {
    LOG_ERROR("invalid argument", K(trans_commit_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(trans_ctx.has_valid_br(stop_flag_))) {
    if (OB_EMPTY_RESULT == ret) {
      if (0 < trans_ctx.get_total_br_count()) {
        // unexpected
        LOG_ERROR("unexpected skiping trans with valid br", KR(ret), K(trans_ctx));
      } else {
        LOG_DEBUG("trans has no valid br to output, skip this trans", KR(ret), K(trans_ctx));
        ret = OB_SUCCESS;
      }
    } else {
      LOG_ERROR("failed to wait for valid br", KR(ret), K(trans_ctx));
    }
  } else {
    ObLogBR *begin_br = NULL;
    ObLogBR *commit_br = NULL;
    const uint64_t row_index = 0;
    const int64_t ddl_schema_version = 0;

    // Assign BEGIN and COMMIT, place them at the beginning and end
    // BEGIN/COMMIT does not need to set host information
    if (OB_FAIL(tag_br_alloc_->alloc(begin_br, NULL))) {
      LOG_ERROR("alloc begin binlog record fail", KR(ret));
    } else if (OB_ISNULL(begin_br)) {
      LOG_ERROR("alloc begin binlog record fail", KR(ret), K(begin_br));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(tag_br_alloc_->alloc(commit_br, NULL))) {
      LOG_ERROR("alloc commit binlog record fail", KR(ret));
    } else if (OB_ISNULL(commit_br)) {
      LOG_ERROR("alloc commit binlog record fail", KR(ret), K(commit_br));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(begin_br->init_data(EBEGIN, cluster_id, tenant_id, row_index, trace_id, trace_info, trans_id_str,
            ddl_schema_version, trans_commit_version, part_trans_task_count, &trans_ctx.get_major_version_str()))) {
      LOG_ERROR("init begin binlog record fail", KR(ret), K(trans_commit_version), K(cluster_id),
          K(tenant_id), K(row_index), K(trace_id), K(trace_info), K(trans_id_str),
          K(ddl_schema_version), K(part_trans_task_count), "major_version:", trans_ctx.get_major_version_str());
    } else if (OB_FAIL(commit_br->init_data(ECOMMIT, cluster_id, tenant_id, row_index, trace_id, trace_info, unique_id,
            ddl_schema_version, trans_commit_version, part_trans_task_count))) {
      LOG_ERROR("init commit binlog record fail", KR(ret), K(trans_commit_version), K(cluster_id),
          K(tenant_id), K(row_index), K(trace_id), K(trace_info), K(unique_id),
          K(ddl_schema_version), K(part_trans_task_count));
    } else {
      LOG_DEBUG("commit trans begin", K(trans_ctx));
      // push begin br to queue
      if (OB_FAIL(push_br_queue_(begin_br))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("push_br_queue_ fail", KR(ret), K(begin_br));
        }
      }

      // push data
      while (! stop_flag_ && OB_SUCC(ret) && ! trans_ctx.is_all_br_committed()) {
        ObLogBR *br_task = NULL;
        uint64_t retry_count = 0;

        if (OB_FAIL(next_ready_br_task_(trans_ctx, br_task))) {
          if (OB_EAGAIN == ret) {
            ob_usleep(10*1000);
            ret = OB_SUCCESS;
            if (OB_UNLIKELY(0 == (++retry_count) % 100)) {
              LOG_DEBUG("waiting for next ready br", KR(ret), K(trans_ctx));
            }
          } else {
            LOG_ERROR("next_ready_br_task_ fail", KR(ret), KPC(br_task));
          }
        } else {
          // Single br down, next reset to NULL
          br_task->set_next(NULL);
          if (OB_FAIL(push_br_queue_(br_task))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("push_br_queue_ fail", KR(ret), K(br_task));
            }
          } else {
            trans_ctx.inc_committed_br_count();
          }
        }
      } // while

      // push commit br to commit
      if (OB_SUCC(ret)) {
        if (OB_FAIL(push_br_queue_(commit_br))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("push_br_queue_ fail", KR(ret), K(commit_br));
          }
        } else if (trans_ctx.get_total_br_count() != trans_ctx.get_committed_br_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("expected all br commit but not", KR(ret), K(trans_ctx));
        }
      }
    }

    if (OB_FAIL(ret)) {
      if (NULL != begin_br) {
        tag_br_alloc_->free(begin_br);
        begin_br = NULL;
      }

      if (NULL != commit_br) {
        tag_br_alloc_->free(commit_br);
        commit_br = NULL;
      }
    }

    LOG_DEBUG("commit_binlog_record_list", KR(ret), K(trans_id), K(trans_id_str), K(trans_commit_version), K(cluster_id),
        K(tenant_id), K(ddl_schema_version), K(trace_id), K(unique_id),
        K(row_index), K(part_trans_task_count), K(trans_ctx));
  }

  return ret;
}

int ObLogCommitter::next_ready_br_task_(TransCtx &trans_ctx, ObLogBR *&br_task)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(trans_ctx.pop_br_for_committer(br_task))) {
    // ERROR will handle by caller, note: OB_EAGIN means waiting sorter append br to trans_ctx or no more br
  } else if (OB_ISNULL(br_task)) {
    LOG_ERROR("invalid task", K(br_task));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(br_task->get_data())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get invalid br_task from trans_br_queue", KR(ret), KPC(br_task), KP(br_task), KP(br_task->get_data()));
  } else {
    // success
  }

  return ret;
}

int ObLogCommitter::push_br_queue_(ObLogBR *br)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(br)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    RETRY_FUNC(stop_flag_, (*br_queue_), push, br, DATA_OP_TIMEOUT);
  }

  return ret;
}

void ObLogCommitter::get_part_trans_task_count(int64_t &ddl_part_trans_task_count,
    int64_t &dml_part_trans_task_count) const
{
  dml_part_trans_task_count = ATOMIC_LOAD(&dml_part_trans_task_count_);
  ddl_part_trans_task_count = ATOMIC_LOAD(&ddl_part_trans_task_count_);
}

int ObLogCommitter::calculate_output_checkpoint_(int64_t &output_checkpoint)
{
  int ret = OB_SUCCESS;
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("obcdc committer is not inited", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(stop_flag_)) {
    ret = OB_IN_STOP_STATE;
    LOG_INFO("obcdc committer is in stop state", KR(ret), K_(inited), K_(stop_flag));
  } else if (OB_ISNULL(tenant_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_mgr is NULL", KR(ret));
  } else if (OB_FAIL(tenant_mgr->get_min_output_checkpoint_for_all_tenant(output_checkpoint))) {
    LOG_ERROR("get_min_output_checkpoint_for_all_tenant failed", KR(ret), K(output_checkpoint));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP >= output_checkpoint)) {
    LOG_INFO("IGNORE INVALID CHECKPOINT", K(output_checkpoint));
    output_checkpoint = OB_INVALID_TIMESTAMP;
  } else if (OB_UNLIKELY(output_checkpoint < last_output_checkpoint_)) {
    // TODO: need handle new_tenant by create or restore.
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("output_checkpoint rollbacked", KR(ret), K_(last_output_checkpoint), K(output_checkpoint));
  } else if (output_checkpoint > last_output_checkpoint_) {
    LOG_DEBUG("next output_checkpoint", K_(last_output_checkpoint), K(output_checkpoint),
        "delay", NTS_TO_DELAY(output_checkpoint));
    last_output_checkpoint_ = output_checkpoint;
  }

  return ret;
}

void ObLogCommitter::configure(const ObLogConfig &cfg)
{
  int64_t output_heartbeat_interval_msec = cfg.output_heartbeat_interval_msec;

  ATOMIC_STORE(&g_output_heartbeat_interval, output_heartbeat_interval_msec * _MSEC_);
  LOG_INFO("[CONFIG]", K(output_heartbeat_interval_msec));
}

} // namespace libobcdc
} // namespace oceanbase

#undef _STAT
#undef STAT
#undef _ISTAT
#undef ISTAT
#undef _DSTAT
#undef DSTAT
