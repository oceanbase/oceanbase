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
 * ResourceCollector
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_resource_collector.h"

#include "storage/tx/ob_trans_define.h" // ObTransID

#include "ob_log_trace_id.h"            // ObLogTraceIdGuard
#include "ob_log_part_trans_task.h"     // PartTransTask
#include "ob_log_task_pool.h"           // ObLogTransTaskPool
#include "ob_log_binlog_record_pool.h"  // ObLogBRPool
#include "ob_log_trans_ctx.h"           // TransCtx
#include "ob_log_trans_ctx_mgr.h"       // IObLogTransCtxMgr
#include "ob_log_store_service.h"       // IObStoreService
#include "ob_log_store_key.h"           // ObLogStoreKey
#include "ob_log_binlog_record.h"       // ObLogBR
#include "ob_log_meta_manager.h"        // IObLogMetaManager
#include "ob_log_trace_id.h"            // ObLogTraceIdGuard
#include "ob_log_instance.h"
#include "ob_log_tenant.h"
#include "ob_log_config.h"
#include "ob_log_trans_redo_dispatcher.h"
#include "ob_cdc_lob_aux_meta_storager.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace libobcdc
{

ObLogResourceCollector::ObLogResourceCollector() :
    inited_(false),
    br_pool_(NULL),
    trans_ctx_mgr_(NULL),
    meta_manager_(NULL),
    store_service_(NULL),
    err_handler_(NULL),
    br_thread_num_(0),
    br_count_(0),
    total_part_trans_task_count_(0),
    ddl_part_trans_task_count_(0),
    dml_part_trans_task_count_(0),
    hb_part_trans_task_count_(0),
    other_part_trans_task_count_(0)
{
}

ObLogResourceCollector::~ObLogResourceCollector()
{
  destroy();
}

int ObLogResourceCollector::init(const int64_t thread_num,
    const int64_t thread_num_for_br,
    const int64_t queue_size,
    IObLogBRPool *br_pool,
    IObLogTransCtxMgr *trans_ctx_mgr,
    IObLogMetaManager *meta_manager,
    IObStoreService *store_service,
    IObLogErrHandler *err_handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ResourceCollector init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(thread_num_for_br <= 0)
      || OB_UNLIKELY(thread_num_for_br + 1 >= thread_num)
      || OB_UNLIKELY(queue_size <= 0)
      || OB_ISNULL(br_pool)
      || OB_ISNULL(trans_ctx_mgr)
      || OB_ISNULL(meta_manager)
      || OB_ISNULL(store_service)
      || OB_ISNULL(err_handler)) {
    LOG_ERROR("invalid arguments", K(thread_num), K(thread_num_for_br), K(queue_size),
        K(br_pool), K(trans_ctx_mgr), K(meta_manager), K(store_service), K(err_handler));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(RCThread::init(thread_num, queue_size))) {
    LOG_ERROR("init ResourceCollector threads fail", KR(ret), K(thread_num), K(queue_size));
  } else {
    br_pool_ = br_pool;
    trans_ctx_mgr_ = trans_ctx_mgr;
    meta_manager_ = meta_manager;
    store_service_ = store_service;
    err_handler_ = err_handler;
    br_thread_num_ = thread_num_for_br;
    br_count_ = 0;
    total_part_trans_task_count_ = 0;
    ddl_part_trans_task_count_ = 0;
    dml_part_trans_task_count_ = 0;
    hb_part_trans_task_count_ = 0;
    other_part_trans_task_count_ = 0;
    inited_ = true;

    LOG_INFO("init ResourceCollector succ", K(thread_num), K(thread_num_for_br), K(queue_size));
  }
  return ret;
}

void ObLogResourceCollector::destroy()
{
  LOG_INFO("resource_collector destroy begin");
  RCThread::destroy();
  inited_ = false;
  br_pool_ = NULL;
  trans_ctx_mgr_ = NULL;
  meta_manager_ = NULL;
  store_service_ = NULL;
  err_handler_ = NULL;
  br_thread_num_ = 0;
  br_count_ = 0;
  total_part_trans_task_count_ = 0;
  ddl_part_trans_task_count_ = 0;
  dml_part_trans_task_count_ = 0;
  hb_part_trans_task_count_ = 0;
  other_part_trans_task_count_ = 0;
  LOG_INFO("resource_collector destroy end");
}

int ObLogResourceCollector::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(RCThread::start())) {
    LOG_ERROR("start ResourceCollector threads fail", KR(ret));
  } else {
    LOG_INFO("start ResourceCollector threads succ", KR(ret));
  }
  return ret;
}

void ObLogResourceCollector::stop()
{
  if (inited_) {
    RCThread::stop();
    LOG_INFO("stop ResourceCollector threads succ");
  }
}

void ObLogResourceCollector::mark_stop_flag()
{
  if (inited_) {
    RCThread::mark_stop_flag();
    LOG_INFO("resource_collector mark_stop_flag");
  }
}

int ObLogResourceCollector::revert(PartTransTask *task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(push_task_into_queue_(*task))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push task into queue fail", KR(ret), K(task));
    }
  } else {
    // NOTE: After entering the queue, the task may be recycled at any time and cannot be further referenced
  }

  return ret;
}

int ObLogResourceCollector::revert(const int record_type, ObLogBR *br)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(br)) {
    LOG_ERROR("invalid argument", K(br));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (EDDL == record_type) {
      PartTransTask *part_trans_task = NULL;

      if (OB_ISNULL(part_trans_task = static_cast<PartTransTask *>(br->get_host()))) {
        LOG_ERROR("binlog record host is invalid", K(br), K(br->get_host()));
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(dec_ref_cnt_and_try_to_revert_task_(part_trans_task))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("dec_ref_cnt_and_try_to_revert_task_ fail", KR(ret), KPC(part_trans_task));
        }
      } else {}
    } else {
      // Recycle asynchronously in case of HEARTBEAT、BEGIN、COMMIT、DML
      if (OB_FAIL(push_task_into_queue_(*br))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("push task into queue fail", KR(ret), K(br), "record_type", print_record_type(record_type));
        }
      } else {
        // NOTE: After entering the queue, the task may be recycled at any time and cannot be further referenced
        br = NULL;
      }
    }
  }

  return ret;
}

int ObLogResourceCollector::revert_log_entry_task(ObLogEntryTask *log_entry_task)
{
  int ret = OB_SUCCESS;
  PartTransTask* part_trans_task = NULL;

  if (OB_ISNULL(part_trans_task = static_cast<PartTransTask *>(log_entry_task->get_host()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("host of log_entry_task is invalid, failed cast to PartTransTask" ,KR(ret), KPC(log_entry_task));
  } else if (OB_FAIL(revert_log_entry_task_(log_entry_task))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("revert_log_entry_task_ fail", KR(ret), KPC(log_entry_task));
    }
  } else if (OB_FAIL(dec_ref_cnt_and_try_to_revert_task_(part_trans_task))) {
    LOG_ERROR("dec_ref_cnt_and_try_to_revert_task_ fail", KR(ret), KPC(part_trans_task));
  } else {
    log_entry_task = NULL;
  }

  return ret;
}

int ObLogResourceCollector::dec_ref_cnt_and_try_to_revert_task_(PartTransTask *part_trans_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(part_trans_task)) {
    LOG_ERROR("invalid arguments", K(part_trans_task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Decrement the reference count of partitionk transaction task
    // The partition transaction task needs to be recycled if the reference count becomes 0
    // Cannot continue to reference partition transaction tasks after that time, since partitioned transaction tasks may be recalled at any time
    const bool need_revert_part_trans_task = (part_trans_task->dec_ref_cnt() == 0);

    if (need_revert_part_trans_task) {
      if (OB_FAIL(revert(part_trans_task))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("revert PartTransTask fail", KR(ret), K(part_trans_task));
        }
      } else {
        part_trans_task = NULL;
      }
    } else {
      // Cannot continue to access partition transaction task when do not need to recycle it
    }
  }

  return ret;
}

int ObLogResourceCollector::revert_log_entry_task_(ObLogEntryTask *log_entry_task)
{
  int ret = OB_SUCCESS;
  IObLogEntryTaskPool *log_entry_task_pool = TCTX.log_entry_task_pool_;
  IObLogTransRedoDispatcher *trans_redo_dispatcher = TCTX.trans_redo_dispatcher_;
  int64_t data_len = 0;
  bool is_log_entry_stored = false;

  if (OB_ISNULL(log_entry_task)) {
    LOG_ERROR("log_entry_task is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(log_entry_task_pool)) {
    LOG_ERROR("log_entry_task_pool is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log_entry_task->get_status(is_log_entry_stored))) {
    LOG_ERROR("log_entry_task get_status fail", KR(ret), KPC(log_entry_task), K(is_log_entry_stored));
  } else if (OB_FAIL(log_entry_task->get_data_len(data_len))) {
    LOG_ERROR("ObLogEntryTask get_data_len fail", K(log_entry_task));
  } else if (OB_FAIL(log_entry_task->rc_callback())) {
    LOG_ERROR("ObLogEntryTask rc_callback fail", K(log_entry_task));
  } else {
    trans_redo_dispatcher->dec_dispatched_redo_memory(data_len);

    const bool is_test_mode_on = TCONF.test_mode_on != 0;
    if (is_test_mode_on) {
      LOG_INFO("LogEntryTask-free", "LogEntryTask", *log_entry_task, "addr", log_entry_task, K(data_len));
    }

    if (is_log_entry_stored) {
      const bool skip_recycle_data = TCONF.skip_recycle_data != 0;
      if (! skip_recycle_data) {
        const uint64_t tenant_id = log_entry_task->get_tenant_id();
        std::string key;

        if (OB_FAIL(log_entry_task->get_storage_key(key))) {
          LOG_ERROR("get_storage_key fail", KR(ret), "key", key.c_str());
        } else if (OB_FAIL(del_store_service_data_(tenant_id, key))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("del_store_service_data_ fail", KR(ret), KPC(log_entry_task));
          }
        }
      }
    }

    log_entry_task_pool->free(log_entry_task);
  }

  return ret;
}

int ObLogResourceCollector::del_store_service_data_(const uint64_t tenant_id,
    std::string &key)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;
  void *column_family_handle = NULL;

  if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
  } else {
    tenant = guard.get_tenant();
    column_family_handle = tenant->get_cf();
  }

  if (OB_SUCC(ret) && ! RCThread::is_stoped()) {
    if (OB_FAIL(store_service_->del(column_family_handle, key))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("store_service_ del fail", KR(ret), K(tenant_id), K(key.c_str()));
      }
    } else {
      LOG_DEBUG("store_service_ del succ", K(tenant_id), K(key.c_str()));
    }
  }

  return ret;
}

int ObLogResourceCollector::revert_participants_(const int64_t thread_index,
    PartTransTask *participants)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(participants)) {
    LOG_ERROR("invalid arguments", K(participants));
    ret = OB_INVALID_ARGUMENT;
  } else {
    PartTransTask *task = participants;

    while (OB_SUCC(ret) && OB_NOT_NULL(task) && ! RCThread::is_stoped()) {
      PartTransTask *next = task->next_task();
      task->set_next_task(NULL);

      if (OB_FAIL(recycle_part_trans_task_(thread_index, task))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("recycle_part_trans_task_ fail", KR(ret), K(thread_index), KPC(task));
        }
      } else {
        task = next;
      }
    }

    if (RCThread::is_stoped()) {
      ret = OB_IN_STOP_STATE;
    }

    task = NULL;
  }

  return ret;
}

int ObLogResourceCollector::push_task_into_queue_(ObLogResourceRecycleTask &task)
{
  int ret = OB_SUCCESS;
  const static int64_t PUSH_TASK_TIMEOUT_WAIT_TIME = 1 * _MSEC_;
  const static int64_t PUSH_TASK_TIMEOUT_PRINT_INTERVAL = 10 * _SEC_;
  static uint64_t part_trans_task_push_seq = 0;
  static uint64_t br_push_seq = 0;
  uint64_t hash_value = 0;

  // thread [0] for LOB_DATA_CLEAN_TASK
  // thread [1, br_thread_num] for BR_TASK
  // thread [br_thread_num + 1, thread_num] for PART_TRANS_TASK
  // Do stat before actual push task into RCThread may leads to wraog stat if push failed,
  // thus we will retry RCThread::push until success
  if (task.is_part_trans_task()) {
    hash_value = ATOMIC_FAA(&part_trans_task_push_seq, 1);
    hash_value = (hash_value % (RCThread::get_thread_num() - br_thread_num_ - 1)) + br_thread_num_ + 1;

    PartTransTask *part_trans_task = static_cast<PartTransTask *>(&task);

    if (OB_ISNULL(part_trans_task)) {
      LOG_ERROR("invalid argument", K(part_trans_task));
      ret = OB_ERR_UNEXPECTED;
    } else {
      do_stat_(*part_trans_task, true/*need_accumulate_stat*/);
    }
  } else if (task.is_binlog_record_task()) {
    (void)ATOMIC_AAF(&br_count_, 1);

    hash_value = ATOMIC_FAA(&br_push_seq, 1);
    hash_value = (hash_value % br_thread_num_) + 1;
  } else {
    // LOB_DATA_CLEAN_TASK, use thread 0
    // hash_value = 0
  }

  // push to thread queue, asynchronous recycling
  while (OB_SUCC(ret) && ! RCThread::is_stoped()) {
    ret = RCThread::push(&task, hash_value, DATA_OP_TIMEOUT);

    // retry if OB_TIMEOUT and break for other ret code
    if (OB_UNLIKELY(OB_TIMEOUT == ret)) {
      if (TC_REACH_TIME_INTERVAL(PUSH_TASK_TIMEOUT_PRINT_INTERVAL)) {
        LOG_INFO("push task into RC Thread timeout, retrying", KR(ret));
      }
      usleep(PUSH_TASK_TIMEOUT_WAIT_TIME);
      ret = OB_SUCCESS;
    } else {
      break;
    }
  }
  // Note: After a task is pushed to the queue, it may be recycled quickly and the task cannot be accessed later

  if (RCThread::is_stoped()) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogResourceCollector::recycle_part_trans_task_(const int64_t thread_index,
    PartTransTask *task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (task->is_ddl_trans()) {
      if (OB_FAIL(revert_dll_all_binlog_records_(task))) {
        if (OB_IN_STOP_STATE != ret) {
          // Reclaim all Binlog Records within a DDL partitioned transaction
          LOG_ERROR("revert_dll_all_binlog_records_ fail", KR(ret), K(*task));
        }
      }
    }
    LOG_DEBUG("[ResourceCollector] recycle part trans task", K(thread_index), K(*task));

    do_stat_(*task, false/*need_accumulate_stat*/);

    // recycle resource
    task->revert();
    task = NULL;
  }

  return ret;
}

int ObLogResourceCollector::handle(void *data,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;
  ObLogResourceRecycleTask *recycle_task = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(stop_flag)) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_ISNULL(data)) {
    LOG_ERROR("invalid argument", K(data));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(recycle_task = static_cast<ObLogResourceRecycleTask *>(data))) {
    LOG_ERROR("recycle_task is NULL", K(recycle_task));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObLogResourceRecycleTask::TaskType task_type = recycle_task->get_task_type();

    if (recycle_task->is_part_trans_task()) {
      PartTransTask *task = static_cast<PartTransTask *>(recycle_task);

      if (! task->is_served()) {
        if (OB_FAIL(revert_unserved_part_trans_task_(thread_index, *task))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("revert_unserved_part_trans_task_ fail", KR(ret), K(thread_index), KPC(task));
          }
        }
      // DML/DDL
      } else if (task->is_ddl_trans() || task->is_dml_trans()) {
        // Guaranteed reference count of 0
        if (OB_UNLIKELY(0 != task->get_ref_cnt())) {
          LOG_ERROR("can not revert part trans task, ref_cnt is not zero", K(*task));
          ret = OB_INVALID_ARGUMENT;
        } else {
          bool enable_create = false;
          TransCtx *trans_ctx = NULL;
          bool all_participant_revertable = false;
          // Copy the Trans ID to avoid invalidating the Trans ID when the PartTransTask is recycled
          uint64_t tenant_id = task->get_tenant_id();
          ObTransID trans_id = task->get_trans_id();
          int64_t commit_version = task->get_trans_commit_version();

          if (OB_FAIL(trans_ctx_mgr_->get_trans_ctx(tenant_id, trans_id, trans_ctx, enable_create))) {
            LOG_ERROR("get trans_ctx fail", KR(ret), K(tenant_id), K(trans_id), K(*task));
          }
          // Increase the number of participants that can be recycled
          else if (OB_FAIL(trans_ctx->inc_revertable_participant_count(all_participant_revertable))) {
            LOG_ERROR("trans_ctx.inc_revertable_participant_count fail", KR(ret), K(*trans_ctx));
          }
          // Recycle the distributed transaction if all participants are available for recycling
          else if (all_participant_revertable) {
            PartTransTask *participants = trans_ctx->get_participant_objs();

            if (OB_FAIL(trans_ctx->revert_participants())) {
              LOG_ERROR("trans_ctx.revert_participants fail", KR(ret), K(*trans_ctx));
            } else if (OB_FAIL(trans_ctx_mgr_->remove_trans_ctx(tenant_id, trans_id))) {
              LOG_ERROR("remove trans_ctx fail", KR(ret), K(tenant_id), K(trans_id), K(trans_ctx));
            }
            // recycle all participants
            else if (OB_NOT_NULL(participants) && OB_FAIL(revert_participants_(thread_index, participants))) {
              if (OB_IN_STOP_STATE != ret) {
                LOG_ERROR("revert_participants_ fail", KR(ret), K(thread_index), K(participants), K(trans_id));
              }
            } else {
              participants = NULL;
            }
          } else {
            // do nothing
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(push_lob_data_clean_task_(tenant_id, commit_version))) {
              LOG_ERROR("push_lob_data_clean_task_ fail", KR(ret), K(tenant_id), K(trans_id), K(commit_version));
            }
          }

          if (NULL != trans_ctx) {
            int err = trans_ctx_mgr_->revert_trans_ctx(trans_ctx);
            if (OB_SUCCESS != err) {
              LOG_ERROR("revert_trans_ctx fail", K(err));
              ret = OB_SUCCESS == ret ? err : ret;
            }
          }

          task = NULL;
        }
      } else {
        // All other tasks are recycled directly
        if (OB_FAIL(recycle_part_trans_task_(thread_index, task))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("recycle_part_trans_task_ fail", KR(ret), K(thread_index), KPC(task));
          }
        } else {
          task = NULL;
        }
      }
    } else if (recycle_task->is_binlog_record_task()) {
      // HEARTBEAT、BEGIN、COMMIT、INSERT、DELETE、UPDATE
      ObLogBR *task = static_cast<ObLogBR *>(recycle_task);
      int record_type = RecordType::EUNKNOWN;

      if (OB_ISNULL(task)) {
        LOG_ERROR("ObLogBR task is NULL");
        ret = OB_ERR_UNEXPECTED;
      } else if (task->get_record_type(record_type)) {
        LOG_ERROR("ObLogBR task get_record_type fail", KR(ret));
      } else {
        if (HEARTBEAT == record_type || EBEGIN == record_type || ECOMMIT == record_type) {
          br_pool_->free(task);
        } else {
          if (OB_FAIL(revert_dml_binlog_record_(*task, stop_flag))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("revert_dml_binlog_record_ fail", KR(ret), KPC(task));
            }
          } else {}
        }
        ATOMIC_DEC(&br_count_);
        task = NULL;
      }
    } else if (recycle_task->is_lob_data_clean_task()) {
      ObCDCLobAuxMetaStorager &lob_aux_meta_storager = TCTX.lob_aux_meta_storager_;
      ObCDCLobAuxDataCleanTask *task = static_cast<ObCDCLobAuxDataCleanTask *>(recycle_task);
      if (OB_FAIL(lob_aux_meta_storager.clean_unused_data(task))) {
        LOG_ERROR("clean lob data fail", KR(ret));
      }
    } else {
      LOG_ERROR("task type not supported", K(recycle_task), K(thread_index),
          "task_type", ObLogResourceRecycleTask::print_task_type(task_type));
      ret = OB_NOT_SUPPORTED;
    }
  }

  // Failure to withdraw
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "ResourceCollector thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}

int ObLogResourceCollector::push_lob_data_clean_task_(const uint64_t tenant_id, const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  ObCDCLobAuxMetaStorager &lob_aux_meta_storager = TCTX.lob_aux_meta_storager_;
  ObCDCLobAuxDataCleanTask *clean_task = nullptr;
  if (OB_FAIL(lob_aux_meta_storager.get_clean_task(tenant_id, clean_task))){
    LOG_ERROR("lob_aux_meta_storager get_clean_task failed", KR(ret), K(tenant_id));
  } else {
    const bool is_task_push = ATOMIC_LOAD(&clean_task->is_task_push_);
    const int64_t clean_task_interval = lob_aux_meta_storager.get_clean_task_interval();
    if (is_task_push || ! REACH_TIME_INTERVAL(clean_task_interval)) {
      LOG_DEBUG("no need push clean task", K(is_task_push), K(tenant_id), K(commit_version), K(clean_task_interval));
    // try set flag by cas, oldv is false, newv is ture
    // expect return oldv (false). if return true, means cas fail, just skip
    } else if (ATOMIC_CAS(&clean_task->is_task_push_, false, true)) {
      LOG_DEBUG("no need push clean task", K(is_task_push), K(tenant_id), K(commit_version), K(clean_task_interval));
    } else {
      ATOMIC_STORE(&clean_task->commit_version_, commit_version);
      if (OB_FAIL(push_task_into_queue_(*clean_task))) {
        LOG_ERROR("push_task_into_queue_ failed", KR(ret), KPC(clean_task), K(clean_task_interval));
      } else {
        LOG_INFO("push lob data clean task succ", KPC(clean_task), K(clean_task_interval));
      }
    }
  }
  return ret;
}

int ObLogResourceCollector::revert_dml_binlog_record_(ObLogBR &br, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogEntryTask *log_entry_task = nullptr;
  DmlStmtTask *dml_stmt_task = nullptr;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ResourceCollector has not been initialized", KR(ret));
  } else if (OB_ISNULL(log_entry_task = static_cast<ObLogEntryTask *>(br.get_host()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("log_entry_task is nullptr", KR(ret), K(br));
  } else if (OB_ISNULL(dml_stmt_task = static_cast<DmlStmtTask *>(br.get_stmt_task()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("dml_stmt_task is nullptr", KR(ret), K(dml_stmt_task));
  } else {
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list = dml_stmt_task->get_new_lob_ctx_cols();
    ObCDCLobAuxMetaStorager &lob_aux_meta_storager = TCTX.lob_aux_meta_storager_;

    if (lob_data_out_row_ctx_list.has_out_row_lob()) {
      if (OB_FAIL(lob_aux_meta_storager.del(lob_data_out_row_ctx_list, stop_flag))) {
        LOG_ERROR("lob_aux_meta_storager del fail", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(dec_ref_cnt_and_try_to_recycle_log_entry_task_(br))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("dec_ref_cnt_and_try_to_recycle_log_entry_task_ fail", KR(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(revert_single_binlog_record_(&br))) {
      LOG_ERROR("revert_single_binlog_record_ fail", KR(ret));
    }
  }

  return ret;
}

int ObLogResourceCollector::del_trans_(const uint64_t tenant_id,
    const ObString &trans_id_str)
{
  int ret = OB_SUCCESS;
  std::string begin_key;
  std::string end_key;
  begin_key.append(trans_id_str.ptr());
  begin_key.append("_");
  end_key.append(trans_id_str.ptr());
  end_key.append("_");
  end_key.append("}");

  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;
  void *column_family_handle = NULL;

  if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
  } else {
    tenant = guard.get_tenant();
    column_family_handle = tenant->get_cf();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(store_service_->del_range(column_family_handle, begin_key, end_key))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("store_service_ del fail", KR(ret), "begin_key", begin_key.c_str(),
            "end_key", end_key.c_str());
      }
    } else {
      LOG_INFO("store_service_ del succ", KR(ret), "begin_key", begin_key.c_str(),
          "end_key", end_key.c_str());
    }
  }

  return ret;
}
int ObLogResourceCollector::dec_ref_cnt_and_try_to_recycle_log_entry_task_(ObLogBR &br)
{
  int ret = OB_SUCCESS;
  ObLogEntryTask *log_entry_task = static_cast<ObLogEntryTask *>(br.get_host());
  PartTransTask *part_trans_task = NULL;

  if (OB_ISNULL(log_entry_task)) {
    LOG_ERROR("log_entry_task is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(part_trans_task = static_cast<PartTransTask *>(log_entry_task->get_host()))) {
    LOG_ERROR("part_trans_task is NULL", KPC(log_entry_task));
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (TCONF.test_mode_on) {
      LOG_INFO("revert_dml_binlog_record", KP(&br), K(br), KP(log_entry_task), KPC(log_entry_task));
    }
    const bool need_revert_log_entry_task = (log_entry_task->dec_row_ref_cnt() == 0);

    if (need_revert_log_entry_task) {
      if (OB_FAIL(revert_log_entry_task_(log_entry_task))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("revert_log_entry_task_ fail", KR(ret), KPC(log_entry_task));
        }
      } else {
        log_entry_task = NULL;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(dec_ref_cnt_and_try_to_revert_task_(part_trans_task))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("dec_ref_cnt_and_try_to_revert_task_ fail", KR(ret), KPC(part_trans_task));
          }
        }
      }
    }
  }

  return ret;
}

int ObLogResourceCollector::revert_dll_all_binlog_records_(PartTransTask *task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! task->is_ddl_trans())) {
    LOG_ERROR("is not ddl trans, unexpected", KPC(task));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const StmtList &stmt_list = task->get_stmt_list();
    DdlStmtTask *stmt_task = static_cast<DdlStmtTask *>(stmt_list.head_);

    // Iterate through all statements, get all Binlog Records and reclaim them
    // FIXME: the Binlog Record contains references to memory allocated by the PartTransTask.
    // They should be actively freed here, but as PartTransTask will release the memory uniformly when it is reclaimed
    // memory in the Binlog Record is not actively freed here
    while (OB_SUCC(ret) && OB_NOT_NULL(stmt_task) && ! RCThread::is_stoped()) {
      DdlStmtTask *next = static_cast<DdlStmtTask *>(stmt_task->get_next());
      ObLogBR *br = stmt_task->get_binlog_record();
      stmt_task->set_binlog_record(NULL);

      if (OB_FAIL(revert_single_binlog_record_(br))) {
        LOG_ERROR("revert_single_binlog_record_ fail", KR(ret));
      }

      stmt_task = next;
    }
    if (RCThread::is_stoped()) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

int ObLogResourceCollector::revert_single_binlog_record_(ObLogBR *br)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(br)) {
    LOG_ERROR("br is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else {
    IBinlogRecord *br_data = NULL;

    if (OB_ISNULL(br_data = br->get_data())) {
      LOG_ERROR("binlog record data is invalid", K(br));
      ret = OB_INVALID_ARGUMENT;
    } else {
      ITableMeta *tblMeta = NULL;
      // recycle Table Meta of binlog record
      if (0 != br_data->getTableMeta(tblMeta)) {
        LOG_ERROR("getTableMeta fail");
        ret = OB_ERR_UNEXPECTED;
      } else if (NULL != tblMeta) {
        meta_manager_->revert_table_meta(tblMeta);
        br_data->setTableMeta(NULL);
      }

      // recycle DB Meta of binlog record
      if (NULL != br_data->getDBMeta()) {
        meta_manager_->revert_db_meta(br_data->getDBMeta());
        br_data->setDBMeta(NULL);
      }
    }

    if (OB_SUCC(ret)) {
      br_pool_->free(br);
      br = NULL;
    }
  }

  return ret;
}

void ObLogResourceCollector::get_task_count(int64_t &part_trans_task_count, int64_t &br_count) const
{
  part_trans_task_count = ATOMIC_LOAD(&total_part_trans_task_count_);
  br_count = ATOMIC_LOAD(&br_count_);
}

int ObLogResourceCollector::revert_unserved_part_trans_task_(const int64_t thread_idx, PartTransTask &task)
{
  int ret = OB_SUCCESS;
  const logservice::TenantLSID &tenant_ls_id = task.get_tls_id();
  SortedRedoLogList &sorted_redo_list =  task.get_sorted_redo_list();
  DmlRedoLogNode *dml_redo_node = static_cast<DmlRedoLogNode *>(sorted_redo_list.head_);

  while (OB_SUCC(ret) && OB_NOT_NULL(dml_redo_node) && ! RCThread::is_stoped()) {
    if (dml_redo_node->is_stored()) {
      const palf::LSN &store_log_lsn = dml_redo_node->get_start_log_lsn();
      ObLogStoreKey store_key;
      std::string key;

      if (OB_FAIL(store_key.init(tenant_ls_id, store_log_lsn))) {
        LOG_ERROR("store_key init fail", KR(ret), K(store_key), K(tenant_ls_id), K(store_log_lsn));
      } else if (OB_FAIL(store_key.get_key(key))) {
        LOG_ERROR("get_storage_key fail", KR(ret), "key", key.c_str());
      } else if (OB_FAIL(del_store_service_data_(tenant_ls_id.get_tenant_id(), key))) {
        LOG_ERROR("del_store_service_data_ fail", KR(ret), K(task));
      } else {}
    }

    if (RCThread::is_stoped()) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCC(ret)) {
      dml_redo_node = static_cast<DmlRedoLogNode *>(dml_redo_node->get_next());
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(recycle_part_trans_task_(thread_idx, &task))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("recycle_part_trans_task_ failed", KR(ret), K(thread_idx), K(task));
      }
    } else {
      // no more access to task
    }
  }

  return ret;
}

void ObLogResourceCollector::do_stat_(PartTransTask &task,
    const bool need_accumulate_stat)
{
  int64_t cnt = 1;

  if (! need_accumulate_stat) {
    cnt = -1;
  }

  (void)ATOMIC_AAF(&total_part_trans_task_count_, cnt);

  if (task.is_ddl_trans()) {
    LOG_DEBUG("do_stat_ for ddl_trans", K_(ddl_part_trans_task_count), K(cnt), K(task), "lbt", lbt_oblog());
    (void)ATOMIC_AAF(&ddl_part_trans_task_count_, cnt);
  } else if (task.is_dml_trans()) {
    (void)ATOMIC_AAF(&dml_part_trans_task_count_, cnt);
  } else if (task.is_ls_heartbeat() || task.is_global_heartbeat()) {
    (void)ATOMIC_AAF(&hb_part_trans_task_count_, cnt);
  } else {
    (void)ATOMIC_AAF(&other_part_trans_task_count_, cnt);
  }
}

void ObLogResourceCollector::print_stat_info() const
{
  _LOG_INFO("[RESOURCE_COLLECTOR] [STAT] BR=%ld TOTAL_PART=%ld DDL=%ld DML=%ld HB=%ld OTHER=%ld",
      br_count_,
      total_part_trans_task_count_,
      ddl_part_trans_task_count_,
      dml_part_trans_task_count_,
      hb_part_trans_task_count_,
      other_part_trans_task_count_);
}

} // namespace libobcdc
} // namespace oceanbase
