/**
 * Copyright (c) 2022 OceanBase
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

#include "lib/string/ob_string.h"                   // ObString
#include "lib/atomic/ob_atomic.h"
#include "lib/thread/ob_thread_name.h"
#include "storage/transaction/ob_trans_define.h"    // ObTransID
#include "ob_log_instance.h"            // IObLogErrHandler, TCTX
#include "ob_log_tenant.h"              // ObLogTenantGuard, ObLogTenant
#include "ob_log_config.h"              // TCONF
#include "ob_log_trans_ctx_mgr.h"       // IObLogTransCtxMgr
#include "ob_log_trans_stat_mgr.h"      // IObLogTransStatMgr
#include "ob_log_committer.h"           // IObLogCommitter

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
namespace liboblog
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
    global_checkpoint_(OB_INVALID_TIMESTAMP),
    last_global_checkpoint_(OB_INVALID_TIMESTAMP),
    global_seq_(0),
    br_committer_queue_seq_(0),
    trans_queue_(),
    trans_queue_lock_(),
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

int ObLogSequencer::init(const int64_t thread_num,
    const int64_t queue_size,
    IObLogTransCtxMgr &trans_ctx_mgr,
    IObLogTransStatMgr &trans_stat_mgr,
    IObLogCommitter &trans_committer,
    IObLogTransRedoDispatcher &redo_dispatcher,
    IObLogTransMsgSorter &br_sorter,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogSequencer has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)) {
    LOG_ERROR("invalid arguments", K(thread_num), K(queue_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(SequencerThread::init(thread_num, queue_size))) {
    LOG_ERROR("init sequencer queue thread fail", KR(ret), K(thread_num), K(queue_size));
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
    LOG_ERROR("ObLogSequencer has not been initialized");
    ret = OB_NOT_INIT;
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
    lib::ThreadPool::stop();
    SequencerThread::stop();
    LOG_INFO("stop threads succ", "thread_num", get_thread_num());
  }
}

int ObLogSequencer::push(PartTransTask *part_trans_task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogSequencer has not been initialized");
    ret = OB_NOT_INIT;
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
      do_stat_for_part_trans_task_count_(*part_trans_task, 1);
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
  stat_info.sequenced_trans_count_ = trans_queue_.size();
}

// A thread is responsible for continually rotating the sequence of transactions that need sequence
void ObLogSequencer::run1()
{
  const int64_t SLEEP_US = 1000;
  lib::set_thread_name("ObLogSequencerTrans");
  int ret = OB_SUCCESS;
  bool enable_monitor = false;
  ObLogTimeMonitor monitor("Sequencer-deal-trans", enable_monitor);

  while (OB_SUCC(ret) && ! lib::ThreadPool::has_set_stop()) {
    // Global checkpoint not updated or initial value, do nothing
    if (ATOMIC_LOAD(&global_checkpoint_) == ATOMIC_LOAD(&last_global_checkpoint_)) {
      lib::this_routine::usleep(SLEEP_US);
    } else {
      ObByteLockGuard guard(trans_queue_lock_);

      while (OB_SUCC(ret) && ! trans_queue_.empty()) {
        TrxSortElem top_trx_sort_elem = trans_queue_.top();
        const int64_t global_trans_version = top_trx_sort_elem.get_global_trans_version();
        monitor.mark_and_get_cost("begin", true);

        if (global_trans_version <= ATOMIC_LOAD(&global_checkpoint_)) {
          if (OB_FAIL(handle_to_be_sequenced_trans_(top_trx_sort_elem, lib::ThreadPool::has_set_stop()))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("handle_to_be_sequenced_trans_ fail", KR(ret), K(top_trx_sort_elem));
            }
          } else {
            monitor.mark_and_get_cost("end", true);
            trans_queue_.pop();
          }
        } else {
          break;
        }
      } // empty
    }
    lib::this_routine::usleep(SLEEP_US);

    if (REACH_TIME_INTERVAL(PRINT_SEQ_INFO_INTERVAL)) {
      ISTAT("[OUTPUT]", "DELAY", TS_TO_DELAY(global_checkpoint_),
          K(global_checkpoint_), K(last_global_checkpoint_), K(global_seq_), "size", trans_queue_.size());
    }
  }

  // exit on fail
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "sequencer thread exits, err=%d", ret);
    ObLogSequencer::stop();
  }
}

int ObLogSequencer::handle_to_be_sequenced_trans_(TrxSortElem &trx_sort_elem,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool enable_monitor = false;
  ObLogTimeMonitor monitor("Sequencer::handle_tobe_sequenced_trans", enable_monitor);
  TransCtx *trans_ctx = trx_sort_elem.get_trans_ctx_host();
  const int64_t new_seq = ATOMIC_FAA(&global_seq_, 1);
  int64_t new_schema_version = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogSequencer has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(trans_ctx)) {
    LOG_ERROR("trans_ctx is NULL", K(trx_sort_elem));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t participant_count = trans_ctx->get_ready_participant_count();
    PartTransTask *participant_list = trans_ctx->get_participant_objs();
    const bool is_dml_trans = participant_list->is_dml_trans();
    const bool is_ddl_trans = participant_list->is_ddl_trans();
    const int64_t local_schema_version = participant_list->get_local_schema_version();
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObLogTenantGuard guard;
    ObLogTenant *tenant = NULL;

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
      LOG_ERROR("trans_ctx sequence fail", KR(ret), K(trx_sort_elem), K(new_seq), K(new_schema_version));
    } else {
      monitor.mark_and_get_cost("sequence_done", true);
      if (OB_FAIL(trans_ctx->wait_data_ready(WAIT_TIMEOUT, stop_flag))) {
        if (OB_IN_STOP_STATE != ret && OB_TIMEOUT != ret) {
          LOG_ERROR("trans_ctx wait_data_ready fail", KR(ret));
        }
      }
      monitor.mark_and_get_cost("data_ready", true);

      // TODO non-block
      if (is_dml_trans) {
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(! trans_ctx->is_data_ready())) {
            LOG_ERROR("trans_ctx is not date ready", KPC(trans_ctx));
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(handle_dml_trans_(*tenant, *trans_ctx, stop_flag))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("handle_dml_trans_ fail", KR(ret), K(tenant_id), KPC(trans_ctx));
            }
          } else {
            monitor.mark_and_get_cost("dml-done", true);
            // succ
          }
        } // while
      } else if (is_ddl_trans) {
        trans_ctx->set_trans_redo_dispatched(); // ddl need not dispatch redo.

        if (OB_FAIL(push_task_into_committer_(participant_list, participant_count, stop_flag, tenant))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("push_task_into_committer_ fail", KR(ret), K(tenant_id), K(participant_list),
                K(participant_count), K(tenant));
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

    LOG_DEBUG("handle_to_be_sequenced_trans_ end", KR(ret), KPC(trans_ctx));
  }

  return ret;
}

int ObLogSequencer::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  PartTransTask *part_trans_task = static_cast<PartTransTask *>(data);
  (void)ATOMIC_AAF(&queue_part_trans_task_count_, -1);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogSequencer has not been initialized");
    ret = OB_NOT_INIT;
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
    LOG_ERROR("sequencer has not been intiaizlied");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! part_trans_task.is_global_heartbeat())) {
    LOG_ERROR("part_trans_task is not ddl_trans, unexpected", K(part_trans_task));
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (0 == part_trans_task.dec_ref_cnt()) {
      const int64_t global_checkpoint = part_trans_task.get_timestamp();
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

        LOG_DEBUG("handle_global_hb_part_trans_task_", K(part_trans_task), K(last_global_checkpoint_), K(global_checkpoint_));
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
    LOG_ERROR("sequencer has not been initialized");
    ret = OB_NOT_INIT;
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
      ret = trans_ctx_mgr_->get_trans_ctx(trans_id, trans_ctx, enable_create);

      if (OB_FAIL(ret)) {
        LOG_ERROR("get_trans_ctx fail", KR(ret), K(trans_id));
        break;
      }

      trans_ctx_need_discard = false;

      const bool print_participant_not_serve_info = ATOMIC_LOAD(&g_print_participant_not_serve_info);

      // prepare trans context
      ret = trans_ctx->prepare(part_trans_task,
          tenant->get_part_mgr(),
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
      (void)trans_ctx_mgr_->remove_trans_ctx(trans_id);
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
    LOG_ERROR("sequencer has not been initialized");
    ret = OB_NOT_INIT;
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
    _ISTAT("[PART_NOT_SERVE] TRANS_ID=%s PART=%s LOG_ID=%ld LOG_TIMESTAMP=%ld",
        to_cstring(part_trans_task.get_trans_id()),
        to_cstring(part_trans_task.get_partition()),
        part_trans_task.get_prepare_log_id(),
        part_trans_task.get_timestamp());

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
    LOG_ERROR("failed to dispatch trans redo", KR(ret), K(trans_ctx), K(stop_flag));
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
    LOG_ERROR("sequencer has not been initialized", K(tenant));
    ret = OB_NOT_INIT;
  } else {
    // Counting the number of partitioned tasks
    do_stat_for_part_trans_task_count_(*task, -task_count);

    RETRY_FUNC(stop_flag, (*trans_committer_), push, task, task_count, DATA_OP_TIMEOUT, tenant);
  }

  return ret;
}

int ObLogSequencer::handle_participants_ready_trans_(const bool is_dml_trans,
    TransCtx *trans_ctx,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

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
      if (OB_FAIL(recycle_resources_after_trans_ready_(*trans_ctx, *tenant))) {
        LOG_ERROR("recycle_resources_after_trans_ready_ fail", KR(ret), KPC(trans_ctx), KPC(tenant), K(stop_flag));
      }
    }

    if (OB_SUCC(ret)) {
      TrxSortElem &trx_sort_elem = trans_ctx->get_trx_sort_elem();
      ObByteLockGuard guard(trans_queue_lock_);
      trans_queue_.push(trx_sort_elem);

      _DSTAT("[TRANS_QUEUE] TRANS_ID=%s QUEUE_SIZE=%lu ID_DML=%d",
          to_cstring(trx_sort_elem),
          trans_queue_.size(),
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

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogSequencer has not been initialized");
    ret = OB_NOT_INIT;
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

// Consider a scenario that dec_part_trans_count after sequence
// 1. create table ... pk hash 100
// 2. liboblog has opened a transaction on the partition
// 3. drop table ... When deleting the partition, because the PartMgr reference count is not 0, mark Offline
// 4. The above partition progress is not advancing, so the global progress is not advancing
// 5. global heartbeat does not advance, sequencer cannot advance based on safety loci and thus cannot result in sequenced transaction output
// 6. The inability to sequence does not decrement the reference count, leading to interdependencies and deadlocks
//
// Therefore, unlike previous implementations, resources are not reclaimed after sequencing, but after the distributed transaction has been assembled
int ObLogSequencer::recycle_resources_after_trans_ready_(TransCtx &trans_ctx, ObLogTenant &tenant)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("sequencer has not been initialized");
    ret = OB_NOT_INIT;
  // } else if (OB_UNLIKELY(! trans_ctx.is_sequenced())) {
  } else if (OB_UNLIKELY(! trans_ctx.is_participants_ready())) {
    LOG_ERROR("trans is not sequenced", K(trans_ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    PartTransTask *participant = trans_ctx.get_participant_objs();

    // Iterate over each statement of each partitioned transaction of a distributed transaction
    while (NULL != participant) {
      if (participant->is_dml_trans() || participant->is_ddl_trans()) {
        const ObPartitionKey &pkey = participant->get_partition();
        // Decrement the count of ongoing transactions on the partition
        if (OB_FAIL(tenant.get_part_mgr().dec_part_trans_count(pkey))) {
          LOG_ERROR("dec_part_trans_count fail", KR(ret), K(pkey));
        }
      }

      participant = participant->next_task();
    }
  }

  return ret;
}

void ObLogSequencer::do_stat_for_part_trans_task_count_(PartTransTask &part_trans_task,
    const int64_t task_count)
{
  bool is_hb_sub_stat = false;
  int64_t hb_dec_task_count = 0;

  if (part_trans_task.is_ddl_trans()) {
    (void)ATOMIC_AAF(&ddl_part_trans_task_count_, task_count);
  } else if (part_trans_task.is_dml_trans()) {
    (void)ATOMIC_AAF(&dml_part_trans_task_count_, task_count);
  } else {
    // heartbeat
    if (task_count < 0) {
      is_hb_sub_stat = true;
      hb_dec_task_count = task_count * SequencerThread::get_thread_num();
      (void)ATOMIC_AAF(&hb_part_trans_task_count_, hb_dec_task_count);
    } else {
      (void)ATOMIC_AAF(&hb_part_trans_task_count_, task_count);
    }
  }

  if (is_hb_sub_stat) {
    (void)ATOMIC_AAF(&total_part_trans_task_count_, hb_dec_task_count);
  } else {
    (void)ATOMIC_AAF(&total_part_trans_task_count_, task_count);
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

} // namespace liboblog
} // namespace oceanbase
