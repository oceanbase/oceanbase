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

#define USING_LOG_PREFIX OBLOG_SEQUENCER

#include "ob_log_sequencer1.h"

#include "lib/string/ob_string.h"       // ObString
#include "lib/atomic/ob_atomic.h"
#include "lib/thread/ob_thread_name.h"
#include "ob_log_instance.h"            // IObLogErrHandler, TCTX
#include "ob_log_tenant.h"              // ObLogTenantGuard, ObLogTenant
#include "ob_log_config.h"              // TCONF
#include "ob_log_trans_ctx_mgr.h"       // IObLogTransCtxMgr
#include "ob_log_trans_stat_mgr.h"      // IObLogTransStatMgr
#include "ob_log_committer.h"           // IObLogCommitter
#include "ob_log_formatter.h"           // IObLogFormatter
#include "ob_log_meta_data_struct.h"    // ObDictTenantInfo
#include "ob_log_ddl_processor.h"       // ObLogDDLProcessor
#include "ob_log_meta_data_service.h"   // GLOGMETADATASERVICE
#include "ob_log_trace_id.h"            // ObLogTraceIdGuard

#define _STAT(level, tag_str, args...) _OBLOG_SEQUENCER_LOG(level, "[STAT] [SEQ] " tag_str, ##args)
#define STAT(level, tag_str, args...) OBLOG_SEQUENCER_LOG(level, "[STAT] [SEQ] " tag_str, ##args)
#define _ISTAT(tag_str, args...) _STAT(INFO, tag_str, ##args)
#define ISTAT(tag_str, args...) STAT(INFO, tag_str, ##args)
#define _DSTAT(tag_str, args...) _STAT(DEBUG, tag_str, ##args)
#define DSTAT(tag_str, args...) STAT(DEBUG, tag_str, ##args)

#define REVERT_TRANS_CTX(trans_ctx) \
    do { \
      if (NULL != trans_ctx) { \
        int err = trans_ctx_mgr_->revert_trans_ctx(trans_ctx); \
        if (OB_SUCCESS != err) { \
          LOG_ERROR("revert_trans_ctx fail", K(err), K(trans_ctx)); \
          ret = OB_SUCCESS == ret ? err : ret; \
        } \
        trans_ctx = NULL; \
      } \
    } while (0)

using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace libobcdc
{
bool ObLogSequencer::g_print_participant_not_serve_info = ObLogConfig::default_print_participant_not_serve_info;

ObLogSequencer::ObLogSequencer()
  : inited_(false),
    round_value_(0),
    heartbeat_round_value_(0),
    trans_ctx_mgr_(NULL),
    trans_stat_mgr_(NULL),
    trans_committer_(NULL),
    redo_dispatcher_(NULL),
    msg_sorter_(NULL),
    err_handler_(NULL),
    schema_inc_replay_(),
    global_checkpoint_(OB_INVALID_TIMESTAMP),
    last_global_checkpoint_(OB_INVALID_TIMESTAMP),
    global_seq_(0),
    br_committer_queue_seq_(0),
    ready_trans_queue_(),
    trans_queue_lock_(),
    seq_trans_queue_(),
    checkpoint_cond_(),
    ready_queue_cond_(),
    seq_queue_cond_(),
    total_part_trans_task_count_(0),
    ddl_part_trans_task_count_(0),
    dml_part_trans_task_count_(0),
    hb_part_trans_task_count_(0),
    queue_part_trans_task_count_(0)
{
}

ObLogSequencer::~ObLogSequencer()
{
  destroy();
}

void ObLogSequencer::configure(const ObLogConfig &config)
{
  bool print_participant_not_serve_info = config.print_participant_not_serve_info;

  ATOMIC_STORE(&g_print_participant_not_serve_info, print_participant_not_serve_info);

  LOG_INFO("[CONFIG]", K(print_participant_not_serve_info));
}

int ObLogSequencer::init(
    const int64_t thread_num,
    const int64_t queue_size,
    IObLogTransCtxMgr &trans_ctx_mgr,
    IObLogTransStatMgr &trans_stat_mgr,
    IObLogCommitter &trans_committer,
    IObLogTransRedoDispatcher &redo_dispatcher,
    IObLogTransMsgSorter &br_sorter,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;
  static const int64_t seq_thread_num = 2;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogSequencer has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)) {
    LOG_ERROR("invalid arguments", K(thread_num), K(queue_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(SequencerThread::init(thread_num, queue_size))) {
    LOG_ERROR("init sequencer queue thread fail", KR(ret), K(thread_num), K(queue_size));
  } else if (OB_FAIL(lib::ThreadPool::set_thread_count(seq_thread_num))) {
    LOG_ERROR("set sequence thread num failed", KR(ret), K(seq_thread_num));
  } else if (OB_FAIL(seq_trans_queue_.init(queue_size))) {
    LOG_ERROR("init sequenced_trans_queue failed", KR(ret), K(queue_size));
  } else if (OB_FAIL(schema_inc_replay_.init(false/*is_start_progress*/))) {
    LOG_ERROR("schema_inc_replay_ init failed", KR(ret));
  } else {
    round_value_ = 0;
    heartbeat_round_value_ = 0;
    trans_ctx_mgr_ = &trans_ctx_mgr;
    trans_committer_ = &trans_committer;
    trans_stat_mgr_ = &trans_stat_mgr;
    redo_dispatcher_ = &redo_dispatcher;
    msg_sorter_ = &br_sorter;
    err_handler_ = &err_handler;
    global_checkpoint_ = OB_INVALID_TIMESTAMP;
    last_global_checkpoint_ = OB_INVALID_TIMESTAMP;
    global_seq_ = 0;
    br_committer_queue_seq_ = 0;
    total_part_trans_task_count_ = 0;
    ddl_part_trans_task_count_ = 0;
    dml_part_trans_task_count_ = 0;
    hb_part_trans_task_count_ = 0;
    queue_part_trans_task_count_ = 0;
    LOG_INFO("init sequencer succ", K(thread_num), K(queue_size));
    inited_ = true;
  }

  return ret;
}

void ObLogSequencer::destroy()
{
  SequencerThread::destroy();

  lib::ThreadPool::wait();
  lib::ThreadPool::destroy();

  inited_ = false;
  round_value_ = 0;
  heartbeat_round_value_ = 0;
  trans_ctx_mgr_ = NULL;
  trans_stat_mgr_ = NULL;
  trans_committer_ = NULL;
  redo_dispatcher_ = NULL;
  msg_sorter_ = NULL;
  err_handler_ = NULL;
  schema_inc_replay_.destroy();
  global_checkpoint_ = OB_INVALID_TIMESTAMP;
  last_global_checkpoint_ = OB_INVALID_TIMESTAMP;
  global_seq_ = 0;
  total_part_trans_task_count_ = 0;
  ddl_part_trans_task_count_ = 0;
  dml_part_trans_task_count_ = 0;
  hb_part_trans_task_count_ = 0;
  queue_part_trans_task_count_ = 0;
}

int ObLogSequencer::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSequencer has not been initialized", KR(ret));
  } else if (OB_FAIL(lib::ThreadPool::start())) {
    LOG_ERROR("ThreadPool start fail" , KR(ret));
  } else if (OB_FAIL(SequencerThread::start())) {
    LOG_ERROR("start sequencer thread fail", KR(ret), "thread_num", get_thread_num());
  } else {
    LOG_INFO("start sequencer threads succ", "thread_num", get_thread_num());
  }

  return ret;
}

void ObLogSequencer::stop()
{
  if (inited_) {
    mark_stop_flag();
    SequencerThread::stop();
    LOG_INFO("stop threads succ", "thread_num", get_thread_num());
  }
}

int ObLogSequencer::push(PartTransTask *part_trans_task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSequencer has not been initialized", KR(ret));
  } else if (OB_ISNULL(part_trans_task)) {
    LOG_ERROR("invalid arguments", K(part_trans_task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const bool is_global_heartbeat = part_trans_task->is_global_heartbeat();
    uint64_t hash_value = 0;

    if (is_global_heartbeat) {
      hash_value = ATOMIC_FAA(&heartbeat_round_value_, 1);
    } else {
      hash_value = ATOMIC_FAA(&round_value_, 1);
    }
    void *push_task = static_cast<void *>(part_trans_task);
    RETRY_FUNC(stop_flag, *(static_cast<ObMQThread *>(this)), push, push_task, hash_value, DATA_OP_TIMEOUT);

    if (OB_SUCC(ret)) {
      (void)ATOMIC_AAF(&queue_part_trans_task_count_, 1);
      do_stat_for_part_trans_task_count_(*part_trans_task, 1, false/*is_sub_stat*/);
    }

    if (OB_FAIL(ret)) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push task into sequencer fail", KR(ret), K(push_task), K(hash_value));
      }
    }
  }

  return ret;
}

void ObLogSequencer::get_task_count(SeqStatInfo &stat_info)
{
  stat_info.total_part_trans_task_count_ = ATOMIC_LOAD(&total_part_trans_task_count_);
  stat_info.ddl_part_trans_task_count_ = ATOMIC_LOAD(&ddl_part_trans_task_count_);
  stat_info.dml_part_trans_task_count_ = ATOMIC_LOAD(&dml_part_trans_task_count_);
  stat_info.hb_part_trans_task_count_ = ATOMIC_LOAD(&hb_part_trans_task_count_);
  stat_info.queue_part_trans_task_count_ = ATOMIC_LOAD(&queue_part_trans_task_count_);
  stat_info.ready_trans_count_ = ready_trans_queue_.size();
  stat_info.sequenced_trans_count_ = seq_trans_queue_.get_curr_total();
}

// A thread is responsible for continually rotating the sequence of transactions that need sequence
void ObLogSequencer::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_idx = lib::ThreadPool::get_thread_idx();
  const int64_t thread_count = lib::ThreadPool::get_thread_count();

  if (thread_count > 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("only expected two thread to handle ready trans",
        KR(ret), K(thread_count), K(thread_idx));
  } else if (0 == thread_idx) {
    lib::set_thread_name("CDC-READY-TX-HANDLER");
    if (OB_FAIL(push_ready_trans_to_seq_queue_())) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push_ready_trans_to_seq_queue_ failed", KR(ret));
      }
    }
  } else if (1 == thread_idx) {
    lib::set_thread_name("CDC-SEQ-TX-HANDLER");
    if (OB_FAIL(handle_trans_in_seq_queue_())) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle_trans_in_seq_queue_ failed", KR(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpect sequencer thread", KR(ret), K(thread_count), K(thread_idx));
  }
  if (OB_SUCC(ret) && lib::ThreadPool::has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }

  // exit on fail
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "sequencer thread(idx=%ld) exits, err=%d", get_thread_idx(), ret);
    mark_stop_flag();
  }
}

int ObLogSequencer::push_ready_trans_to_seq_queue_()
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  while (OB_SUCC(ret) && ! lib::ThreadPool::has_set_stop()) {
    bool is_trans_can_be_output = true;
    if (! ready_trans_queue_.empty()) {
      // notice the lock scope
      ObByteLockGuard guard(trans_queue_lock_);
      TrxSortElem top_trx_sort_elem = ready_trans_queue_.top();
      const int64_t global_trans_version = top_trx_sort_elem.get_trans_commit_version();
      TransCtx *trans_ctx = top_trx_sort_elem.get_trans_ctx_host();
      is_trans_can_be_output = (global_trans_version <= ATOMIC_LOAD(&global_checkpoint_));
      if (is_trans_can_be_output) {
        if (OB_FAIL(seq_trans_queue_.push(trans_ctx))) {
          if (OB_SIZE_OVERFLOW != ret) {
            LOG_ERROR("push trans_ctx into seq_trans_queue failed", KR(ret));
            // push failed, signal consumer wakeup
            seq_queue_cond_.signal();
          } else {
            // seq_trans_queue is full, will retry in next round
            ret = OB_SUCCESS;
            // wait seq_trans_queue not full
            seq_queue_cond_.timedwait(DATA_OP_TIMEOUT);
          }
        } else {
          ready_trans_queue_.pop();
          // push success, signal handle_trans_in_seq_queue_ to consume
          seq_queue_cond_.signal();
        }
      }
    } else {
      // wait trans assembled and push into ready_trans_queue
      ready_queue_cond_.timedwait(DATA_OP_TIMEOUT);
    }
    if (!is_trans_can_be_output) {
      // wait checkpoint advance
      // can't put into ObByteLockGuard(trans_queue_lock_) in case of deak lock of trans_queue_lock_
      checkpoint_cond_.timedwait(DATA_OP_TIMEOUT);
    }
    if (REACH_TIME_INTERVAL(PRINT_SEQ_INFO_INTERVAL)) {
      ISTAT("[OUTPUT]", "DELAY", NTS_TO_DELAY(global_checkpoint_),
          K_(global_checkpoint), K_(last_global_checkpoint), K_(global_seq),
          "ready_trans_count", ready_trans_queue_.size(),
          "sequenced_trans_count", seq_trans_queue_.get_curr_total());
    }
  } // end while

  return ret;
}

int ObLogSequencer::handle_trans_in_seq_queue_()
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  while (OB_SUCC(ret) && ! lib::ThreadPool::has_set_stop()) {
    bool seq_queue_is_empty = false;
    TransCtx *trans_ctx = nullptr;
    ObLogTraceIdGuard trace_guard;
    if (OB_FAIL(seq_trans_queue_.pop(trans_ctx))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        seq_queue_is_empty = true;
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("pop TransCtx from seq_trans_queue failed", KR(ret));
      }
    } else if (OB_FAIL(handle_sequenced_trans_(trans_ctx, lib::ThreadPool::has_set_stop()))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle_sequenced_trans_ failed", KR(ret));
      }
    }
    if (seq_queue_is_empty) {
      // wait data push into seq_trans_queue
      seq_queue_cond_.timedwait(DATA_OP_TIMEOUT);
    } else {
      // consume data in seq_trans_queue, signal producer to push data
      // or consume failed, signal producer wakeup
      seq_queue_cond_.signal();
    }
  } // end while

  return ret;
}

int ObLogSequencer::handle_sequenced_trans_(
    TransCtx *trans_ctx,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool enable_monitor = false;
  ObLogTimeMonitor monitor("Sequencer::handle_tobe_sequenced_trans", enable_monitor);

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSequencer has not been initialized", KR(ret));
  } else if (OB_ISNULL(trans_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("trans_ctx is NULL", KR(ret));
  } else {
    const int64_t new_seq = ATOMIC_FAA(&global_seq_, 1);
    int64_t new_schema_version = 0;
    const int64_t participant_count = trans_ctx->get_ready_participant_count();
    PartTransTask *participant_list = trans_ctx->get_participant_objs();
    const bool is_dml_trans = participant_list->is_dml_trans();
    const bool is_ddl_trans = participant_list->is_ddl_trans();
    const int64_t local_schema_version = participant_list->get_local_schema_version();
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObLogTenantGuard guard;
    ObLogTenant *tenant = NULL;
    const ObTransID trans_id = trans_ctx->get_trans_id();

    if (OB_FAIL(trans_ctx->get_tenant_id(tenant_id))) {
      LOG_ERROR("trans_ctx get_tenant_id fail", KR(ret), K(tenant_id));
    } else if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
      // There is no need to deal with the tenant not existing here, it must exist, and there is a bug if it doesn’t exist
      LOG_ERROR("get_tenant fail", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant = guard.get_tenant())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant is NULL, unexpected error", KR(ret), K(guard));
    } else if (OB_FAIL(tenant->alloc_global_trans_schema_version(! is_dml_trans, local_schema_version, new_schema_version))) {
      LOG_ERROR("tenant alloc_global_trans_schema_version fail", KR(ret), KPC(tenant), KPC(trans_ctx),
          K(is_dml_trans), K(local_schema_version), K(new_schema_version));
    // sequence
    } else if (OB_FAIL(trans_ctx->sequence(new_seq, new_schema_version))) {
      LOG_ERROR("trans_ctx sequence fail", KR(ret), K(new_seq), K(new_schema_version));
    } else {
      monitor.mark_and_get_cost("sequence_done", true);
      if (OB_FAIL(trans_ctx->wait_data_ready(WAIT_TIMEOUT, stop_flag))) {
        if (OB_IN_STOP_STATE != ret && OB_TIMEOUT != ret) {
          LOG_ERROR("trans_ctx wait_data_ready fail", KR(ret));
        }
      }
      monitor.mark_and_get_cost("data_ready", true);

      if (OB_SUCC(ret)) {
        if (is_dml_trans) {
          if (OB_UNLIKELY(! trans_ctx->is_data_ready())) {
            LOG_ERROR("trans_ctx is not date ready", KPC(trans_ctx));
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(handle_dml_trans_(*tenant, *trans_ctx, stop_flag))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("handle_dml_trans_ fail", KR(ret), K(tenant_id), KPC(trans_ctx));
            }
          } else {
            // succ
            monitor.mark_and_get_cost("dml-done", true);
          } // while
        } else if (is_ddl_trans){
          trans_ctx->set_trans_redo_dispatched();
          // need sort_participants = on, which make sure the first PartTransTask of
          // participant_list is DDL_TRANS.
          // TODO: consider more idea to handle this.
          if (OB_FAIL(handle_ddl_trans_(*tenant, *trans_ctx, stop_flag))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("handle_ddl_trans_ fail", KR(ret), K(tenant), KPC(trans_ctx));
            }
          } else {
            monitor.mark_and_get_cost("ddl-done", true);
            // No further operation is possible after that and may be recalled at any time
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected trans type handled by sequencer", KR(ret), KPC(tenant), KPC(trans_ctx), KPC(participant_list));
        }
      }
    }

    LOG_TRACE("handle_sequenced_trans_ end", KR(ret), K(trans_id), KPC(trans_ctx));
  }

  return ret;
}

int ObLogSequencer::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;
  PartTransTask *part_trans_task = static_cast<PartTransTask *>(data);
  (void)ATOMIC_AAF(&queue_part_trans_task_count_, -1);

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSequencer has not been initialized", KR(ret));
  } else if (OB_ISNULL(part_trans_task)) {
    LOG_ERROR("invalid arguments", KPC(part_trans_task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (! part_trans_task->is_global_heartbeat()) {
      LOG_DEBUG("ObLogSequencer handle", KPC(part_trans_task), K(thread_index));
    }

    if (part_trans_task->is_global_heartbeat()) {
      if (OB_FAIL(handle_global_hb_part_trans_task_(*part_trans_task, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("handle_global_hb_part_trans_task_ fail", KR(ret), K(thread_index), KPC(part_trans_task));
        }
      }
    } else if (part_trans_task->is_dml_trans() || part_trans_task->is_ddl_trans()) {
      if (OB_FAIL(handle_part_trans_task_(*part_trans_task, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("handle_part_trans_task_ fail", KR(ret), K(thread_index), KPC(part_trans_task));
        }
      }
    } else {
      LOG_ERROR("not supported task", KPC(part_trans_task));
      ret = OB_NOT_SUPPORTED;
    }
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  // exit on fail
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "Sequencer thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}

int ObLogSequencer::handle_global_hb_part_trans_task_(PartTransTask &part_trans_task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("sequencer has not been intiaizlied", KR(ret));
  } else if (OB_UNLIKELY(! part_trans_task.is_global_heartbeat())) {
    LOG_ERROR("part_trans_task is not ddl_trans, unexpected", K(part_trans_task));
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (0 == part_trans_task.dec_ref_cnt()) {
      const int64_t global_checkpoint = part_trans_task.get_trans_commit_version();
      const int64_t cur_global_checkpoint = ATOMIC_LOAD(&global_checkpoint_);

      // If global checkpoint rollback, unexpected
      if (global_checkpoint < cur_global_checkpoint) {
        LOG_ERROR("global_checkpoint is less than cur_global_checkpoint, unexpected", K(global_checkpoint),
            K(cur_global_checkpoint), K(last_global_checkpoint_), K(part_trans_task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // record last checkpoint
        last_global_checkpoint_ = cur_global_checkpoint;
        // udpate current checkpoint
        ATOMIC_STORE(&global_checkpoint_, global_checkpoint);
        if (global_checkpoint > cur_global_checkpoint) {
          // signal push_ready_trans_to_seq_queue_
          checkpoint_cond_.signal();
        }

        LOG_DEBUG("handle_global_hb_part_trans_task_", K(part_trans_task),
            K(last_global_checkpoint_), K(global_checkpoint_), "delay", NTS_TO_DELAY(global_checkpoint_));
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(push_task_into_committer_(&part_trans_task, 1, stop_flag, NULL/*tenant*/))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("push_task_into_committer_ fail", KR(ret), K(part_trans_task));
          }
        } else {
          // No further operation is possible after that and may be recalled at any time
        }
      }
    }
  }

  return ret;
}

int ObLogSequencer::handle_part_trans_task_(PartTransTask &part_trans_task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("sequencer has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    const bool is_dml_trans = part_trans_task.is_dml_trans();
    TransCtx *trans_ctx = NULL;
    bool is_part_trans_served = true; // default serve
    bool is_all_participants_ready = false;

    if (OB_FAIL(prepare_trans_ctx_(part_trans_task, is_part_trans_served, trans_ctx, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("prepare_trans_ctx_ fail", KR(ret), K(part_trans_task));
      }
    } else {
      // Attempt to add to the participant list, if the partition does not exist in the participant list, the partition transaction must not be served
      if (is_part_trans_served) {
        if (OB_ISNULL(trans_ctx)) {
          LOG_ERROR("prepare trans ctx fail", K(part_trans_task), K(is_part_trans_served));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(trans_ctx->add_participant(part_trans_task,
            is_part_trans_served,
            is_all_participants_ready))) {
          LOG_ERROR("add participant fail", KR(ret), K(part_trans_task), K(*trans_ctx));
        } else {
          // handle success
        }
      }

      // So far it has been confirmed that the partition serve or not, move on to the next step
      if (OB_SUCC(ret)) {
        if (! is_part_trans_served) {
          // Handling partitioned transactions not serve
          if (OB_FAIL(handle_not_served_trans_(part_trans_task, stop_flag))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("handle_not_served_trans_ fail", KR(ret), K(part_trans_task));
            }
          } else {
            // handle success
          }
        } else {
          // Handle if partitioned transaction serve
          if (is_all_participants_ready) {
            // If all participants are gathered, start the next step
            if (OB_FAIL(handle_participants_ready_trans_(is_dml_trans, trans_ctx, stop_flag))) {
              if (OB_IN_STOP_STATE != ret) {
                LOG_ERROR("handle_participants_ready_trans_ fail", KR(ret), K(is_dml_trans), K(*trans_ctx));
              }
            } else {
              // handle success
            }
          } else {
            // Participants have not yet finished gathering, no processing will be done
          }
        }
      } // OB_SUCC(ret)
    }

    REVERT_TRANS_CTX(trans_ctx);
  }


  return ret;
}

int ObLogSequencer::prepare_trans_ctx_(PartTransTask &part_trans_task,
    bool &is_part_trans_served,
    TransCtx *&trans_ctx,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = part_trans_task.get_tenant_id();
  ObLogTenantGuard guard;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("sequencer has not been initialized", KR(ret));
  } else if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    // It is not possible for a tenant not to exist during the processing of data, and here a direct error is reported
    LOG_ERROR("get_tenant fail", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(guard.get_tenant())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant is NULL, unexpected", KR(ret), K(guard), K(part_trans_task));
  } else {
    bool enable_create = true;
    bool trans_ctx_need_discard = false;
    const ObTransID &trans_id = part_trans_task.get_trans_id();
    ObLogTenant *tenant = guard.get_tenant();

    trans_ctx = NULL;
    is_part_trans_served = true; // default to serve

    // get a valid TransCtx
    while (OB_SUCCESS == ret && ! stop_flag) {
      // Get the transaction context, or create one if it doesn't exist
      trans_ctx = NULL;
      ret = trans_ctx_mgr_->get_trans_ctx(tenant_id, trans_id, trans_ctx, enable_create);

      if (OB_FAIL(ret)) {
        LOG_ERROR("get_trans_ctx fail", KR(ret), K(tenant_id), K(trans_id));
        break;
      }

      trans_ctx_need_discard = false;

      const bool print_participant_not_serve_info = ATOMIC_LOAD(&g_print_participant_not_serve_info);

      // prepare trans context
      ret = trans_ctx->prepare(part_trans_task,
          tenant->get_ls_mgr(),
          print_participant_not_serve_info,
          stop_flag,
          trans_ctx_need_discard);

      if (OB_INVALID_ERROR != ret) {
        break;
      }

      ret = OB_SUCCESS;

      // If the transaction context has been deprecated, change the transaction context next time
      REVERT_TRANS_CTX(trans_ctx);

      PAUSE();
    }

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_FAIL(ret)) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("prepare trans_ctx fail", KR(ret), K(trans_ctx), K(part_trans_task));
      }

      // Reversing the transaction context in the case of error
      REVERT_TRANS_CTX(trans_ctx);
    } else if (trans_ctx_need_discard) {
      // If the transaction context needs to be deprecated, then the partitioned transaction is not being served and the transaction context needs to be deleted
      (void)trans_ctx_mgr_->remove_trans_ctx(tenant_id, trans_id);
      is_part_trans_served = false;

      REVERT_TRANS_CTX(trans_ctx);
    } else {
      // succ
    }
  }
  return ret;
}

int ObLogSequencer::handle_not_served_trans_(PartTransTask &part_trans_task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("sequencer has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(! part_trans_task.is_dml_trans())
      && OB_UNLIKELY(! part_trans_task.is_ddl_trans())) {
    LOG_ERROR("part_trans_task is not DML or DDL trans", K(part_trans_task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // If the partitioned transaction is no longer in service, its resources are reclaimed and passed to the Committer as a "non-serviceable transaction" type task
    //
    // Note that.
    // 1. When reclaiming resources, it is not necessary to decrement the number of transactions on the partition, because
    //   it is when the decrementing of the number of transactions on the partition fails that the partition is known to be unserviced
    // 2. This cannot be converted to a heartbeat type task, the heartbeat type timestamp has a special meaning and can only be generated by the fetcher
    _ISTAT("[PART_NOT_SERVE] TRANS_ID=%s TLS=%s LOG_LSN=%ld LOG_TIMESTAMP=%ld",
        to_cstring(part_trans_task.get_trans_id()),
        to_cstring(part_trans_task.get_tls_id()),
        part_trans_task.get_commit_log_lsn().val_,
        part_trans_task.get_trans_commit_version());

    // Conversion of transaction tasks to "unserviced partitioned transactions"
    if (OB_FAIL(part_trans_task.convert_to_not_served_trans())) {
      LOG_ERROR("convert_to_not_served_trans fail", KR(ret), K(part_trans_task));
    }
    // push to Committer, unserviced transaction tasks do not need to provide tenant structures
    else if (OB_FAIL(push_task_into_committer_(&part_trans_task, 1/*task_count*/, stop_flag, NULL))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push_task_into_committer_ fail", KR(ret), K(part_trans_task));
      }
    }
  }

  return ret;
}

int ObLogSequencer::push_task_into_br_sorter_(TransCtx &trans_ctx, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  RETRY_FUNC_ON_ERROR(OB_NEED_RETRY, stop_flag, (*msg_sorter_), submit, &trans_ctx);

  return ret;
}

int ObLogSequencer::push_task_into_redo_dispatcher_(TransCtx &trans_ctx, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(redo_dispatcher_->dispatch_trans_redo(trans_ctx, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("failed to dispatch trans redo", KR(ret), K(trans_ctx), K(stop_flag));
    }
  }

  return ret;
}

int ObLogSequencer::push_task_into_committer_(PartTransTask *task,
    const int64_t task_count,
    volatile bool &stop_flag,
    ObLogTenant *tenant)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("sequencer has not been initialized", KR(ret), K(tenant));
  } else {
    // Counting the number of partitioned tasks
    do_stat_for_part_trans_task_count_(*task, task_count, true/*is_sub_stat*/);

    RETRY_FUNC(stop_flag, (*trans_committer_), push, task, task_count, DATA_OP_TIMEOUT, tenant);
  }

  return ret;
}

int ObLogSequencer::handle_participants_ready_trans_(const bool is_dml_trans,
    TransCtx *trans_ctx,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  stop_flag = stop_flag;

  if (OB_ISNULL(trans_ctx)) {
    LOG_ERROR("invalid argument", K(trans_ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Avoiding TransCtx recycling
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObLogTenantGuard guard;
    ObLogTenant *tenant = NULL;

    if (OB_FAIL(trans_ctx->get_tenant_id(tenant_id))) {
      LOG_ERROR("trans_ctx get_tenant_id fail", KR(ret), K(tenant_id));
    } else if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
      // There is no need to deal with the tenant not existing here, it must exist, and if it doesn't there is a bug
      LOG_ERROR("get_tenant fail", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant = guard.get_tenant())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant is NULL, unexpected error", KR(ret), K(guard));
    } else {
      if (OB_FAIL(recycle_resources_after_trans_ready_(*trans_ctx, *tenant, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("recycle_resources_after_trans_ready_ fail", KR(ret), KPC(trans_ctx), KPC(tenant));
        }
      }
    }

    if (OB_SUCC(ret)) {
      TrxSortElem &trx_sort_elem = trans_ctx->get_trx_sort_elem();
      {
        ObByteLockGuard guard(trans_queue_lock_);
        ready_trans_queue_.push(trx_sort_elem);
      }
      // signal push_ready_trans_to_seq_queue_
      ready_queue_cond_.signal();

      _DSTAT("[TRANS_QUEUE] TENANT_ID=%lu TRANS_ID=%s QUEUE_SIZE=(%lu/%ld) IS_DML=%d",
          tenant_id,
          to_cstring(trx_sort_elem),
          ready_trans_queue_.size(),
          seq_trans_queue_.get_curr_total(),
          is_dml_trans);
    }
  }

  return ret;
}

int ObLogSequencer::handle_dml_trans_(ObLogTenant &tenant, TransCtx &trans_ctx, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = tenant.get_tenant_id();
  PartTransTask* participant_list = trans_ctx.get_participant_objs();
  const int64_t participant_count = trans_ctx.get_ready_participant_count();

  // tmp TODO remove
  static uint64_t total_tx_cnt;
  total_tx_cnt++;

  LOG_DEBUG("handle_dml_trans_", K(trans_ctx), K(total_tx_cnt));

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSequencer has not been initialized", KR(ret));
  } else if (OB_FAIL(push_task_into_br_sorter_(trans_ctx, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push trans into sorter failed", KR(ret), K(trans_ctx), K(stop_flag));
    }
  } else if (OB_FAIL(push_task_into_committer_(participant_list, participant_count, stop_flag, &tenant))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push_task_into_committer_ fail", KR(ret), K(tenant_id), K(participant_list),
          K(participant_count), K(tenant));
    }
  } else if (OB_FAIL(push_task_into_redo_dispatcher_(trans_ctx, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push trans into redo dispatcher failed", KR(ret), K(trans_ctx), K(stop_flag));
    }
  } else {
    // TODO  statistic
  }

  return ret;
}

int ObLogSequencer::handle_ddl_trans_(ObLogTenant &tenant, TransCtx &trans_ctx, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = tenant.get_tenant_id();
  PartTransTask *participant_list = trans_ctx.get_participant_objs();
  const int64_t participant_count = trans_ctx.get_ready_participant_count();

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSequencer has not been initialized", KR(ret));
  } else if (OB_ISNULL(participant_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("participants of trans shoule not be null", KR(ret), K(trans_ctx));
  } else if (OB_UNLIKELY(! participant_list->is_ddl_trans())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expected to handle ddl_trans", KR(ret), KPC(participant_list), K(trans_ctx));
  } else if (OB_FAIL(handle_multi_data_source_info_(tenant, trans_ctx, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_multi_data_source_info_ failed", KR(ret), K(trans_ctx), K(stop_flag));
    }
  } else if (OB_FAIL(push_task_into_committer_(participant_list, participant_count, stop_flag, &tenant))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push_task_into_committer_ fail", KR(ret), K(tenant_id), K(participant_list),
          K(participant_count), K(tenant));
    }
  } else {
    // succ
  }

  return ret;
}

int ObLogSequencer::handle_multi_data_source_info_(
    ObLogTenant &tenant,
    TransCtx &trans_ctx,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const bool is_using_data_dict = is_data_dict_refresh_mode(TCTX.refresh_mode_);
  PartTransTask *part_trans_task = trans_ctx.get_participant_objs();
  IObLogPartMgr &part_mgr = tenant.get_part_mgr();

  while (OB_SUCC(ret) && OB_NOT_NULL(part_trans_task) && ! stop_flag) {
    if (! part_trans_task->is_sys_ls_part_trans()) {
      // USER_LS part_trans_task in DIST_DDL_TRANS won't into dispatcher, set_ref_cnt to 1 to
      // recycle the part_trans_task.
      part_trans_task->set_ref_cnt(1);
    }
    if (part_trans_task->get_multi_data_source_info().has_tablet_change_op()) {
      const CDCTabletChangeInfoArray &tablet_change_info_arr =
          part_trans_task->get_multi_data_source_info().get_tablet_change_info_arr();
      for (int64_t tablet_change_info_idx = 0;
          OB_SUCC(ret) && ! stop_flag && tablet_change_info_idx < tablet_change_info_arr.count();
          tablet_change_info_idx++) {
        const ObCDCTabletChangeInfo &tablet_change_info = tablet_change_info_arr.at(tablet_change_info_idx);
        if (OB_UNLIKELY(! tablet_change_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("tablet_change_info is not valid", KR(ret), K(tablet_change_info), K(tablet_change_info_idx),
              K(tablet_change_info_arr), KPC(part_trans_task));
        } else if (tablet_change_info.is_create_tablet_op()) {
          // create tablet can directly apply to tablet_to_table info.
          if (OB_FAIL(part_mgr.apply_create_tablet_change(tablet_change_info))) {
            LOG_ERROR("apply_create_tablet_change failed", KR(ret), K(tablet_change_info), K(tenant), KPC(part_trans_task));
          } else {
            LOG_DEBUG("CDC_CREATE_TABLET", KR(ret), K(tablet_change_info), K(part_trans_task), KPC(part_trans_task), K(tenant));
          }
        } else if (tablet_change_info.is_delete_tablet_op()) {
          const bool skip_delete_tablet_op = (1 == TCONF.skip_delete_tablet_op);

          if (OB_UNLIKELY(skip_delete_tablet_op)) {
            LOG_INFO("[IGNORE][CDC_DELETE_TABLET]",
                "tls_id", part_trans_task->get_tls_id(),
                "trans_id", trans_ctx.get_trans_id(),
                K(tablet_change_info),
                "op_tablets", tablet_change_info.get_delete_tablet_op_arr());
          // 1. delete tablet should wait all task in dml_parse done.
          } else if (OB_FAIL(wait_until_parser_done_("delete_tablet_op", stop_flag))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("wait_until_parser_done_ failed", KR(ret), KPC(part_trans_task));
            }
          // 2. apply delete_tablet_op
          } else if (OB_FAIL(part_mgr.apply_delete_tablet_change(tablet_change_info))) {
            LOG_ERROR("apply_delete_tablet_change failed", KR(ret), K(tablet_change_info), K(tenant), KPC(part_trans_task));
          } else {
            LOG_DEBUG("CDC_DELETE_TABLET", KR(ret), K(tablet_change_info), K(part_trans_task), KPC(part_trans_task), K(tenant));
          }
        }
      }
    }

    // handle ddl_operation and inc_data_dict in sequencer
    // only when (1) CDC is using data_dict and (2) part_trans is in SYS_LS
    if (OB_SUCC(ret) && is_using_data_dict && part_trans_task->is_sys_ls_part_trans()) {
      ObLogDDLProcessor *ddl_processor = TCTX.ddl_processor_;
      LOG_DEBUG("handle_ddl_trans and mds for data_dict mode begin", KPC(part_trans_task));

      if (part_trans_task->get_multi_data_source_info().is_empty_dict_info()) {
        _LOG_INFO("[IS_NOT_BARRIER] [EMPTY_DICT] tls_id=%s trans_id=%s is_sp=%d",
            to_cstring(part_trans_task->get_tls_id()),
            to_cstring(part_trans_task->get_trans_id()),
            part_trans_task->is_single_ls_trans());
      } else {
        bool is_not_barrier = false;
        ObSchemaOperationType op_type;

        if (OB_FAIL(part_trans_task->check_for_ddl_trans(is_not_barrier, op_type))) {
          LOG_ERROR("part_trans_task check_for_ddl_trans failed", KR(ret), KPC(part_trans_task), K(is_not_barrier));
        } else if (is_not_barrier) {
          _LOG_INFO("[IS_NOT_BARRIER] [DDL] tls_id=%s trans_id=%s is_dist=%d OP_TYPE=%s(%d)",
              to_cstring(part_trans_task->get_tls_id()),
              to_cstring(part_trans_task->get_trans_id()),
              part_trans_task->is_dist_trans(),
              ObSchemaOperation::type_str(op_type), op_type);
        } else {
          // Barrier transaction: should wait all task in dml_parser/reader/Formatter done.
          if (OB_FAIL(wait_until_formatter_done_(stop_flag))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("wait_until_formatter_done_ failed", KR(ret), KPC(part_trans_task));
            }
          } // wait_until_formatter_done_
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(ddl_processor)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("expect valid ddl_processor", KR(ret));
        } else if (OB_FAIL(ddl_processor->handle_ddl_trans(*part_trans_task, tenant, stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("handle_ddl_trans for data_dict mode failed", KR(ret), K(tenant), KPC(part_trans_task));
          }
        } else if (part_trans_task->get_multi_data_source_info().is_ddl_trans()
            && OB_FAIL(handle_ddl_multi_data_source_info_(*part_trans_task, tenant, trans_ctx))) {
          LOG_ERROR("handle_ddl_multi_data_source_info_ failed", KR(ret), KPC(part_trans_task), K(trans_ctx),
              K(stop_flag));
        } else {
          LOG_DEBUG("handle_ddl_trans and mds for data_dict mode done", KPC(part_trans_task));
        }
      }
    }

    if (OB_SUCC(ret)) {
      part_trans_task = part_trans_task->next_task();
    }
  } // end while

  if (OB_SUCC(ret) && stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogSequencer::handle_ddl_multi_data_source_info_(
    PartTransTask &part_trans_task,
    ObLogTenant &tenant,
    TransCtx &trans_ctx)
{
  int ret = OB_SUCCESS;
  DictTenantArray &tenant_metas = part_trans_task.get_dict_tenant_array();
  DictDatabaseArray &database_metas = part_trans_task.get_dict_database_array();
  DictTableArray &table_metas = part_trans_task.get_dict_table_array();
  const bool need_replay = (tenant_metas.count() > 0)
      || (database_metas.count() > 0)
      || (table_metas.count() > 0);

  if (need_replay) {
    const uint64_t tenant_id = part_trans_task.get_tenant_id();
    ObDictTenantInfoGuard dict_tenant_info_guard;
    ObDictTenantInfo *tenant_info = nullptr;

    if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(tenant_id, dict_tenant_info_guard))) {
      LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant_info is nullptr", K(tenant_id));
    } else if (OB_FAIL(schema_inc_replay_.replay(part_trans_task, tenant_metas, database_metas, table_metas, *tenant_info))) {
      LOG_ERROR("schema_inc_replay_ replay failed", KR(ret), K(part_trans_task), K(tenant_info));
    }
  } else {
    // do nothing
  }

  LOG_DEBUG("handle_ddl_multi_data_source_info_ done", KR(ret), K(need_replay),
      "tenant_meta_cnt", tenant_metas.count(),
      "db_meta_cnt", database_metas.count(),
      "tb_meta_cnt", table_metas.count(),
      K(part_trans_task));

  return ret;
}

int ObLogSequencer::wait_until_parser_done_(
    const char *caller,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(TCTX.dml_parser_) || OB_ISNULL(TCTX.reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dml_parser or reader is null", KR(ret));
  } else {
    while (OB_SUCC(ret) && ! stop_flag) {
      int64_t reader_task_count = 0;
      int64_t dml_parser_task_count = 0;
      TCTX.reader_->get_task_count(reader_task_count);

      if (OB_FAIL(TCTX.dml_parser_->get_log_entry_task_count(dml_parser_task_count))) {
        LOG_ERROR("get_dml_parser_task_count failed", KR(ret), K(dml_parser_task_count));
      } else if (0 < (reader_task_count + dml_parser_task_count)) {
        const static int64_t PRINT_WAIT_PARSER_TIMEOUT = 10 * _SEC_;
        if (REACH_TIME_INTERVAL(PRINT_WAIT_PARSER_TIMEOUT)) {
          LOG_INFO("DDL barrier waiting reader and dml_parser empty",
              K(caller), K(reader_task_count), K(dml_parser_task_count));
        } else {
          LOG_DEBUG("DDL barrier waiting reader and dml_parser empty",
              K(reader_task_count), K(dml_parser_task_count));
        }
        // sleep 100ms and retry
        const static int64_t WAIT_PARSER_EMPTY_TIME = 100 * 1000;
        ob_usleep(WAIT_PARSER_EMPTY_TIME);
      } else {
        break;
      }
    } // end while
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogSequencer::wait_until_formatter_done_(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(TCTX.formatter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("formatter is null", KR(ret));
  } else if (OB_FAIL(wait_until_parser_done_("wait_formatted", stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("wait_until_parser_done_ failed", KR(ret));
    }
  } else {
    while (OB_SUCC(ret) && ! stop_flag) {
      int64_t formatter_br_task_count = 0;
      int64_t formatter_log_entray_task_count = 0;
      // should also wait stmt handling in lob_merger.
      // not get_task_count from lob_merger cause formatter and lob_merger will push task to each
      // other, which may leads to wrong total_task in formatter and lob_merger in concurrent case.
      int64_t lob_merger_task_count = 0;

      if (OB_FAIL(TCTX.formatter_->get_task_count(formatter_br_task_count, formatter_log_entray_task_count, lob_merger_task_count))) {
        LOG_ERROR("get_dml_parser_task_count failed", KR(ret), K(formatter_br_task_count), K(formatter_log_entray_task_count));
      } else if (0 < (formatter_br_task_count + formatter_log_entray_task_count + lob_merger_task_count)) {
        const static int64_t PRINT_WAIT_PARSER_TIMEOUT = 10 * _SEC_;

        if (REACH_TIME_INTERVAL(PRINT_WAIT_PARSER_TIMEOUT)) {
          LOG_INFO("DDL barrier transaction waiting Formatter empty",
              K(formatter_br_task_count), K(formatter_log_entray_task_count), K(lob_merger_task_count));
        } else {
          LOG_DEBUG("DDL barrier transaction waiting Formatter empty",
              K(formatter_br_task_count), K(formatter_log_entray_task_count), K(lob_merger_task_count));
        }
        // sleep 10ms and retry
        const static int64_t WAIT_FORMATTER_EMPTY_TIME = 10 * 1000;
        ob_usleep(WAIT_FORMATTER_EMPTY_TIME);
      } else {
        break;
      }
    }
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

// Consider a scenario that dec_ls_trans_count after sequence
// 1. create table ... pk hash 100
// 2. libobcdc has opened a transaction on the partition
// 3. drop table ... When deleting the partition, because the PartMgr reference count is not 0, mark Offline
// 4. The above partition progress is not advancing, so the global progress is not advancing
// 5. global heartbeat does not advance, sequencer cannot advance based on safety loci and thus cannot result in sequenced transaction output
// 6. The inability to sequence does not decrement the reference count, leading to interdependencies and deadlocks
//
// Therefore, unlike previous implementations, resources are not reclaimed after sequencing, but after the distributed transaction has been assembled
int ObLogSequencer::recycle_resources_after_trans_ready_(TransCtx &trans_ctx, ObLogTenant &tenant, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("sequencer has not been initialized", KR(ret));
  // } else if (OB_UNLIKELY(! trans_ctx.is_sequenced())) {
  } else if (OB_UNLIKELY(! trans_ctx.is_participants_ready())) {
    LOG_ERROR("trans is not sequenced", K(trans_ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    PartTransTask *participant = trans_ctx.get_participant_objs();

    // Iterate over each statement of each partitioned transaction of a distributed transaction
    while (NULL != participant && ! stop_flag) {
      // TODO is_ddl_trans: LS_TABLE的事务如何处理？
      if (participant->is_dml_trans() || participant->is_ddl_trans()) {
        const logservice::TenantLSID &tls_id = participant->get_tls_id();
        // Decrement the count of ongoing transactions on the partition
        if (OB_FAIL(tenant.get_ls_mgr().dec_ls_trans_count(tls_id))) {
          LOG_ERROR("dec_ls_trans_count fail", KR(ret), K(tls_id));
        }
      }

      participant = participant->next_task();
    }

    if (OB_SUCC(ret) && stop_flag) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

void ObLogSequencer::do_stat_for_part_trans_task_count_(
    PartTransTask &part_trans_task,
    const int64_t task_count,
    const bool is_sub_stat)
{
  bool is_hb_sub_stat = false;
  int64_t hb_dec_task_count = 0;
  int64_t op_task_count = task_count;
  if (is_sub_stat) {
    op_task_count = -1 * task_count;
  }

  if (part_trans_task.is_ddl_trans()) {
    if (is_sub_stat) {
      (void)ATOMIC_AAF(&ddl_part_trans_task_count_, -1);
      // dist ddl_task contains dml part_trans_task, should do_stat seperately
      if (task_count > 1) {
        (void)ATOMIC_AAF(&dml_part_trans_task_count_, 1 - task_count);
      }
    } else {
      (void)ATOMIC_AAF(&ddl_part_trans_task_count_, op_task_count);
    }
  } else if (part_trans_task.is_dml_trans()) {
    (void)ATOMIC_AAF(&dml_part_trans_task_count_, op_task_count);
  } else {
    // heartbeat
    if (is_sub_stat) {
      is_hb_sub_stat = true;
      hb_dec_task_count = op_task_count * SequencerThread::get_thread_num();
      (void)ATOMIC_AAF(&hb_part_trans_task_count_, hb_dec_task_count);
    } else {
      (void)ATOMIC_AAF(&hb_part_trans_task_count_, op_task_count);
    }
  }

  if (is_hb_sub_stat) {
    (void)ATOMIC_AAF(&total_part_trans_task_count_, hb_dec_task_count);
  } else {
    (void)ATOMIC_AAF(&total_part_trans_task_count_, op_task_count);
  }
}

int ObLogSequencer::do_trans_stat_(const uint64_t tenant_id,
    const int64_t total_stmt_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(trans_stat_mgr_)) {
    LOG_ERROR("trans_stat_mgr_ is null", K(trans_stat_mgr_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_UNLIKELY(total_stmt_cnt < 0)) {
    LOG_ERROR("invalid argument", K(tenant_id), K(total_stmt_cnt));
    ret = OB_INVALID_ARGUMENT;
  } else {
    trans_stat_mgr_->do_tps_stat();
    trans_stat_mgr_->do_rps_stat_before_filter(total_stmt_cnt);
    if (OB_FAIL(trans_stat_mgr_->do_tenant_tps_rps_stat(tenant_id, total_stmt_cnt))) {
      LOG_ERROR("do tenant rps stat before filter", KR(ret), K(tenant_id), K(total_stmt_cnt));
    }
  }

  return ret;
}

#undef _STAT
#undef STAT
#undef _ISTAT
#undef ISTAT
#undef _DSTAT
#undef DSTAT

} // namespace libobcdc
} // namespace oceanbase
