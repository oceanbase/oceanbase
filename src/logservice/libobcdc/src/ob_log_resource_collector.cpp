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
 * ResourceCollector — Facade over 5 specialized sub-pools
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_resource_collector.h"

#include "storage/tx/ob_trans_define.h" // ObTransID

#include "ob_log_trace_id.h"            // ObLogTraceIdGuard
#include "ob_log_task_pool.h"           // ObLogTransTaskPool
#include "ob_log_binlog_record_pool.h"  // ObLogBRPool
#include "ob_log_trans_ctx_mgr.h"       // IObLogTransCtxMgr
#include "ob_log_store_service.h"       // IObStoreService
#include "ob_log_store_key.h"           // ObLogStoreKey
#include "ob_log_binlog_record.h"       // ObLogBR
#include "ob_log_meta_manager.h"        // IObLogMetaManager
#include "ob_log_instance.h"
#include "ob_log_tenant.h"
#include "ob_log_config.h"
#include "ob_log_trans_redo_dispatcher.h"
#include "ob_cdc_lob_aux_meta_storager.h"
#include "lib/allocator/ob_mod_define.h"             // ObModIds

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace libobcdc
{

///////////////////////////////////////////////////////////////////////////////
// Sub-pool handle() implementations
///////////////////////////////////////////////////////////////////////////////

int ObRCBRPool::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  set_cdc_thread_name("CDC-RC-BR", thread_index);
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  if (OB_ISNULL(host_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRCBRPool host is NULL", KR(ret));
  } else if (OB_FAIL(host_->handle_br_task_(data, thread_index))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_br_task_ fail", KR(ret), K(data), K(thread_index));
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    if (OB_NOT_NULL(host_) && OB_NOT_NULL(host_->err_handler_)) {
      host_->err_handler_->handle_error(ret,
          "ResourceCollector BR pool thread exits, thread_index=%ld, err=%d",
          thread_index, ret);
    }
    stop_flag = true;
  }

  return ret;
}

int ObRCLogEntryTaskPool::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  set_cdc_thread_name("CDC-RC-LogEntry", thread_index);
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  if (OB_ISNULL(host_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRCLogEntryTaskPool host is NULL", KR(ret));
  } else if (OB_FAIL(host_->handle_log_entry_task_dispatch_(data, thread_index))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_log_entry_task_dispatch_ fail", KR(ret), K(data), K(thread_index));
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    if (OB_NOT_NULL(host_) && OB_NOT_NULL(host_->err_handler_)) {
      host_->err_handler_->handle_error(ret,
          "ResourceCollector LogEntry pool thread exits, thread_index=%ld, err=%d",
          thread_index, ret);
    }
    stop_flag = true;
  }

  return ret;
}

int ObRCPartTransTaskPool::handle(PartTransTask *task, const int64_t thread_index)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  if (OB_ISNULL(host_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRCPartTransTaskPool host is NULL", KR(ret));
  } else if (OB_FAIL(host_->handle_part_trans_task_(task, thread_index))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_part_trans_task_ fail", KR(ret), KPC(task), K(thread_index));
    }
  }

  // Note: ObCdcSharedQueueThread::run() will call mark_stop_flag() if ret is fatal.
  // We call err_handler here to propagate the error to the instance for global stop.
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    if (OB_NOT_NULL(host_) && OB_NOT_NULL(host_->err_handler_)) {
      host_->err_handler_->handle_error(ret,
          "ResourceCollector PartTrans pool thread exits, thread_index=%ld, err=%d",
          thread_index, ret);
    }
  }

  return ret;
}

int ObRCStoreDeletePool::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  set_cdc_thread_name("CDC-RC-StoreDel", thread_index);
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  if (OB_ISNULL(host_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRCStoreDeletePool host is NULL", KR(ret));
  } else {
    ret = host_->handle_store_delete_task_dispatch_(data, thread_index);
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    if (OB_NOT_NULL(host_) && OB_NOT_NULL(host_->err_handler_)) {
      host_->err_handler_->handle_error(ret,
          "ResourceCollector StoreDelete pool thread exits, thread_index=%ld, err=%d",
          thread_index, ret);
    }
    stop_flag = true;
  }

  return ret;
}

int ObRCLobCleanPool::handle(ObCDCLobAuxDataCleanTask *task, const int64_t thread_index)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  if (OB_ISNULL(host_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRCLobCleanPool host is NULL", KR(ret));
  } else if (OB_FAIL(host_->handle_lob_clean_task_(task, thread_index))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_lob_clean_task_ fail", KR(ret), KPC(task), K(thread_index));
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    if (OB_NOT_NULL(host_) && OB_NOT_NULL(host_->err_handler_)) {
      host_->err_handler_->handle_error(ret,
          "ResourceCollector LobClean pool thread exits, thread_index=%ld, err=%d",
          thread_index, ret);
    }
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////
// ObLogResourceCollector — Constructor / Destructor
///////////////////////////////////////////////////////////////////////////////

ObLogResourceCollector::ObLogResourceCollector() :
    br_thread_pool_(),
    log_entry_thread_pool_(),
    part_trans_thread_pool_(),
    store_delete_thread_pool_(),
    lob_clean_thread_pool_(),
    inited_(false),
    stop_flag_(false),
    br_pool_(NULL),
    trans_ctx_mgr_(NULL),
    meta_manager_(NULL),
    store_service_(NULL),
    err_handler_(NULL),
    br_count_(0),
    log_entry_task_count_(0),
    total_part_trans_task_count_(0),
    ddl_part_trans_task_count_(0),
    dml_part_trans_task_count_(0),
    hb_part_trans_task_count_(0),
    other_part_trans_task_count_(0),
    total_delete_keys_(0),
    total_delete_operations_(0),
    total_delete_tasks_(0),
    pending_delete_tasks_(0),
    total_delete_time_us_(0),
    last_stat_delete_keys_(0),
    last_stat_delete_operations_(0),
    last_stat_delete_time_us_(0),
    batch_delete_buffers_lock_(ObLatchIds::OB_CDC_COMMON_LOCK),
    batch_delete_size_(0),
    flush_interval_ms_(0)
{
}

ObLogResourceCollector::~ObLogResourceCollector()
{
  destroy();
}

///////////////////////////////////////////////////////////////////////////////
// Lifecycle: init / destroy / start / stop / mark_stop_flag / configure
///////////////////////////////////////////////////////////////////////////////

int ObLogResourceCollector::init(const int64_t thread_num_for_br,
    const int64_t thread_num_for_log_entry_task,
    const int64_t thread_num_for_part_trans,
    const int64_t thread_num_for_store_delete,
    const int64_t thread_num_for_lob_clean,
    const int64_t queue_size,
    IObLogBRPool *br_pool,
    IObLogTransCtxMgr *trans_ctx_mgr,
    IObLogMetaManager *meta_manager,
    IObStoreService *store_service,
    IObLogErrHandler *err_handler)
{
  int ret = OB_SUCCESS;

  // Clamp log_entry_task threads: 0 means use 1 (backward compat with old "share with part_trans" semantics)
  const int64_t actual_log_entry_threads = (thread_num_for_log_entry_task > 0)
      ? thread_num_for_log_entry_task : 1;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ResourceCollector init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num_for_br <= 0)
      || OB_UNLIKELY(thread_num_for_part_trans <= 0)
      || OB_UNLIKELY(thread_num_for_store_delete <= 0)
      || OB_UNLIKELY(thread_num_for_lob_clean <= 0)
      || OB_UNLIKELY(queue_size <= 0)
      || OB_ISNULL(br_pool)
      || OB_ISNULL(trans_ctx_mgr)
      || OB_ISNULL(meta_manager)
      || OB_ISNULL(store_service)
      || OB_ISNULL(err_handler)) {
    LOG_ERROR("invalid arguments", K(thread_num_for_br), K(thread_num_for_log_entry_task),
        K(thread_num_for_part_trans), K(thread_num_for_store_delete),
        K(thread_num_for_lob_clean), K(queue_size),
        K(br_pool), K(trans_ctx_mgr), K(meta_manager), K(store_service), K(err_handler));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Set back-references on sub-pools
    br_thread_pool_.set_host(this);
    log_entry_thread_pool_.set_host(this);
    part_trans_thread_pool_.set_host(this);
    store_delete_thread_pool_.set_host(this);
    lob_clean_thread_pool_.set_host(this);

    // Initialize ObMQThread pools
    if (OB_FAIL(br_thread_pool_.init(thread_num_for_br, queue_size))) {
      LOG_ERROR("init BR thread pool fail", KR(ret), K(thread_num_for_br), K(queue_size));
    } else if (OB_FAIL(log_entry_thread_pool_.init(actual_log_entry_threads, queue_size))) {
      LOG_ERROR("init LogEntryTask thread pool fail", KR(ret), K(actual_log_entry_threads), K(queue_size));
    } else if (OB_FAIL(store_delete_thread_pool_.init(thread_num_for_store_delete, queue_size))) {
      LOG_ERROR("init StoreDelete thread pool fail", KR(ret), K(thread_num_for_store_delete), K(queue_size));
    }
    // Initialize ObCdcSharedQueueThread pools (pass &stop_flag_ as external stop signal)
    else if (OB_FAIL(part_trans_thread_pool_.init(thread_num_for_part_trans, queue_size,
        "CDC-RC-PartTrans", &stop_flag_))) {
      LOG_ERROR("init PartTrans thread pool fail", KR(ret), K(thread_num_for_part_trans), K(queue_size));
    } else if (OB_FAIL(lob_clean_thread_pool_.init(thread_num_for_lob_clean, queue_size,
        "CDC-RC-LobClean", &stop_flag_))) {
      LOG_ERROR("init LobClean thread pool fail", KR(ret), K(thread_num_for_lob_clean), K(queue_size));
    }
    // Initialize batch delete support
    else if (OB_FAIL(batch_delete_buffers_.create(64, ObModIds::OB_HASH_NODE))) {
      LOG_ERROR("batch_delete_buffers_ create fail", KR(ret));
    } else {
      // Initialize StoreDeleteTask allocator
      lib::ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "CDCRedoDltTask");
      if (OB_FAIL(store_delete_task_allocator_.init(
          common::OB_MALLOC_NORMAL_BLOCK_SIZE,
          store_delete_task_block_alloc_,
          mem_attr))) {
        LOG_ERROR("store_delete_task_allocator_ init fail", KR(ret));
      } else {
        store_delete_task_allocator_.set_nway(4);

        br_pool_ = br_pool;
        trans_ctx_mgr_ = trans_ctx_mgr;
        meta_manager_ = meta_manager;
        store_service_ = store_service;
        err_handler_ = err_handler;
        stop_flag_ = false;
        br_count_ = 0;
        log_entry_task_count_ = 0;
        total_part_trans_task_count_ = 0;
        ddl_part_trans_task_count_ = 0;
        dml_part_trans_task_count_ = 0;
        hb_part_trans_task_count_ = 0;
        other_part_trans_task_count_ = 0;

        // Load configuration parameters
        batch_delete_size_ = TCONF.storage_delete_batch_size.get();
        flush_interval_ms_ = TCONF.storage_batch_delete_interval_ms.get();

        inited_ = true;

        LOG_INFO("init ResourceCollector succ",
            K(thread_num_for_br), "log_entry_threads", actual_log_entry_threads,
            K(thread_num_for_part_trans), K(thread_num_for_store_delete),
            K(thread_num_for_lob_clean), K(queue_size));
      }
    }
  }

  return ret;
}

void ObLogResourceCollector::destroy()
{
  LOG_INFO("resource_collector destroy begin");

  if (inited_) {
    stop();
  }

  // Destroy sub-pools
  br_thread_pool_.destroy();
  log_entry_thread_pool_.destroy();
  part_trans_thread_pool_.destroy();
  store_delete_thread_pool_.destroy();
  lob_clean_thread_pool_.destroy();

  inited_ = false;
  stop_flag_ = false;
  br_pool_ = NULL;
  trans_ctx_mgr_ = NULL;
  meta_manager_ = NULL;
  store_service_ = NULL;
  err_handler_ = NULL;
  br_count_ = 0;
  log_entry_task_count_ = 0;
  total_part_trans_task_count_ = 0;
  ddl_part_trans_task_count_ = 0;
  dml_part_trans_task_count_ = 0;
  hb_part_trans_task_count_ = 0;
  other_part_trans_task_count_ = 0;
  store_delete_task_allocator_.destroy();
  for (common::hash::ObHashMap<uint64_t, TenantDeleteBuffer *>::iterator it = batch_delete_buffers_.begin();
      it != batch_delete_buffers_.end(); ++it) {
    if (NULL != it->second) {
      it->second->~TenantDeleteBuffer();
      ob_cdc_free(it->second);
    }
  }
  batch_delete_buffers_.destroy();
  batch_delete_size_ = 0;
  flush_interval_ms_ = 0;

  LOG_INFO("resource_collector destroy end");
}

int ObLogResourceCollector::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    // Start consumers first, then producers (bottom-up order)
    if (OB_FAIL(lob_clean_thread_pool_.start())) {
      LOG_ERROR("start LobClean pool fail", KR(ret));
    } else if (OB_FAIL(store_delete_thread_pool_.start())) {
      LOG_ERROR("start StoreDelete pool fail", KR(ret));
    } else if (OB_FAIL(part_trans_thread_pool_.start())) {
      LOG_ERROR("start PartTrans pool fail", KR(ret));
    } else if (OB_FAIL(log_entry_thread_pool_.start())) {
      LOG_ERROR("start LogEntryTask pool fail", KR(ret));
    } else if (OB_FAIL(br_thread_pool_.start())) {
      LOG_ERROR("start BR pool fail", KR(ret));
    } else {
      LOG_INFO("start ResourceCollector all sub-pools succ");
    }
  }

  return ret;
}

void ObLogResourceCollector::stop()
{
  if (inited_) {
    mark_stop_flag();
    // Stop producers first, then consumers (top-down order)
    br_thread_pool_.stop();
    log_entry_thread_pool_.stop();
    part_trans_thread_pool_.stop();
    store_delete_thread_pool_.stop();
    lob_clean_thread_pool_.stop();
    LOG_INFO("stop ResourceCollector all sub-pools succ");
  }
}

void ObLogResourceCollector::mark_stop_flag()
{
  if (inited_) {
    ATOMIC_STORE(&stop_flag_, true);
    br_thread_pool_.mark_stop_flag();
    log_entry_thread_pool_.mark_stop_flag();
    part_trans_thread_pool_.mark_stop_flag();
    store_delete_thread_pool_.mark_stop_flag();
    lob_clean_thread_pool_.mark_stop_flag();
    LOG_INFO("resource_collector mark_stop_flag");
  }
}

void ObLogResourceCollector::configure(const ObLogConfig &config)
{
  int ret = OB_SUCCESS;

  // Dynamic thread count adjustment (only for ObCdcSharedQueueThread pools)
  const int64_t new_part_trans = config.resource_collector_thread_num_for_part_trans;
  const int64_t new_lob_clean = config.resource_collector_thread_num_for_lob_clean;

  if (OB_FAIL(part_trans_thread_pool_.set_thread_count(new_part_trans))) {
    LOG_WARN("set_thread_count for PartTrans pool fail", KR(ret), K(new_part_trans));
  }

  ret = OB_SUCCESS;
  if (OB_FAIL(lob_clean_thread_pool_.set_thread_count(new_lob_clean))) {
    LOG_WARN("set_thread_count for LobClean pool fail", KR(ret), K(new_lob_clean));
  }

  // Reload batch delete params (atomic for concurrent readers)
  ATOMIC_STORE(&batch_delete_size_, static_cast<int64_t>(config.storage_delete_batch_size));
  ATOMIC_STORE(&flush_interval_ms_, static_cast<int64_t>(config.storage_batch_delete_interval_ms));

  LOG_INFO("ResourceCollector configure",
      K(new_part_trans), K(new_lob_clean),
      K_(batch_delete_size), K_(flush_interval_ms));
}

///////////////////////////////////////////////////////////////////////////////
// revert() — public API, push to sub-pools
///////////////////////////////////////////////////////////////////////////////

int ObLogResourceCollector::revert(PartTransTask *task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    do_stat_(*task, true/*need_accumulate_stat*/);

    if (OB_FAIL(part_trans_thread_pool_.push(task))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push PartTransTask to pool fail", KR(ret), K(task));
      }
    } else {
      // NOTE: After entering the queue, the task may be recycled at any time and cannot be further referenced
    }
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
      PartTransTask *part_trans_task = static_cast<PartTransTask *>(br->get_host());
      if (OB_ISNULL(part_trans_task)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("binlog record host is invalid", K(br), K(br->get_host()));
      } else {
        if (OB_UNLIKELY(TCONF.test_mode_on != 0)) {
          LOG_INFO("revert_record", K(record_type), KP(br), KP(br->get_data()), KP(part_trans_task), KPC(part_trans_task));
        }
        if (OB_FAIL(dec_ref_cnt_and_try_to_revert_task_(part_trans_task))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("dec_ref_cnt_and_try_to_revert_task_ fail", KR(ret), KPC(part_trans_task));
          }
        }
      }
    } else {
      if (OB_UNLIKELY(TCONF.test_mode_on != 0)) {
        if (EINSERT == record_type || EUPDATE == record_type || EDELETE == record_type || EPUT == record_type) {
          ObLogEntryTask *log_entry_task = static_cast<ObLogEntryTask *>(br->get_host());
          if (OB_NOT_NULL(log_entry_task)) {
            LOG_INFO("revert record", K(record_type), KP(br), KP(br->get_data()), KP(log_entry_task), KPC(log_entry_task));
          }
        }
      }

      // Recycle asynchronously: push to BR thread pool
      (void)ATOMIC_AAF(&br_count_, 1);

      if (OB_FAIL(push_to_br_pool_(br))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("push BR to pool fail", KR(ret), K(br), "record_type", print_record_type(record_type));
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

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_entry_task)) {
    LOG_ERROR("invalid argument", K(log_entry_task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    (void)ATOMIC_AAF(&log_entry_task_count_, 1);

    if (OB_FAIL(push_to_log_entry_task_pool_(log_entry_task))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push log entry task to pool fail", KR(ret), K(log_entry_task));
      }
    } else {
      // NOTE: After entering the queue, the task may be recycled at any time and cannot be further referenced
    }
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////
// Push helpers — wrap ObMQThread push with retry-on-timeout
///////////////////////////////////////////////////////////////////////////////

int ObLogResourceCollector::push_to_br_pool_(ObLogBR *br)
{
  int ret = OB_SUCCESS;
  static uint64_t br_push_seq = 0;
  uint64_t hash_value = ATOMIC_FAA(&br_push_seq, 1);

  while (OB_SUCC(ret) && ! is_stopped_()) {
    ret = br_thread_pool_.push(static_cast<void*>(br), hash_value, DATA_OP_TIMEOUT);

    if (OB_UNLIKELY(OB_TIMEOUT == ret)) {
      if (TC_REACH_TIME_INTERVAL(10 * _SEC_)) {
        LOG_WARN("push BR to pool timeout, retrying", KR(ret));
      }
      ob_usleep(static_cast<uint32_t>(1 * _MSEC_));
      ret = OB_SUCCESS;
    } else {
      break;
    }
  }

  if (is_stopped_() && OB_SUCC(ret)) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogResourceCollector::push_to_log_entry_task_pool_(ObLogEntryTask *task)
{
  int ret = OB_SUCCESS;
  static uint64_t log_entry_push_seq = 0;
  uint64_t hash_value = ATOMIC_FAA(&log_entry_push_seq, 1);

  while (OB_SUCC(ret) && ! is_stopped_()) {
    ret = log_entry_thread_pool_.push(static_cast<void*>(task), hash_value, DATA_OP_TIMEOUT);

    if (OB_UNLIKELY(OB_TIMEOUT == ret)) {
      if (TC_REACH_TIME_INTERVAL(10 * _SEC_)) {
        LOG_WARN("push LogEntryTask to pool timeout, retrying", KR(ret));
      }
      ob_usleep(static_cast<uint32_t>(1 * _MSEC_));
      ret = OB_SUCCESS;
    } else {
      break;
    }
  }

  if (is_stopped_() && OB_SUCC(ret)) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogResourceCollector::push_store_delete_task_(StoreDeleteTask *task)
{
  int ret = OB_SUCCESS;
  int64_t key_count = 0;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ResourceCollector has not been initialized");
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(task));
  } else if (task->keys_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", "key_count", task->keys_.count(), KPC(task));
  } else if (is_stopped_()) {
    ret = OB_IN_STOP_STATE;
  } else {
    static uint64_t store_delete_push_seq = 0;
    uint64_t hash_value = ATOMIC_FAA(&store_delete_push_seq, 1);
    // Save key count before pushing to thread pool, because once pushed,
    // the consumer thread may free the task at any time (use-after-free).
    key_count = task->keys_.count();

    while (OB_SUCC(ret) && ! is_stopped_()) {
      ret = store_delete_thread_pool_.push(static_cast<void*>(task), hash_value, DATA_OP_TIMEOUT);

      if (OB_UNLIKELY(OB_TIMEOUT == ret)) {
        if (TC_REACH_TIME_INTERVAL(10 * _SEC_)) {
          LOG_WARN("push StoreDeleteTask to pool timeout, retrying");
        }
        ob_usleep(static_cast<uint32_t>(1 * _MSEC_));
        ret = OB_SUCCESS;
      } else {
        break;
      }
    }

    if (is_stopped_() && OB_SUCC(ret)) {
      ret = OB_IN_STOP_STATE;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push_store_delete_task_ fail", KR(ret), KPC(task));
    }
    // Destroy and free task on error
    if (OB_NOT_NULL(task)) {
      task->~StoreDeleteTask();
      store_delete_task_allocator_.free(task);
      task = NULL;
    }
  } else {
    // Update statistics (use saved key_count to avoid use-after-free)
    ATOMIC_AAF(&total_delete_tasks_, 1);
    ATOMIC_AAF(&total_delete_keys_, key_count);
    ATOMIC_AAF(&pending_delete_tasks_, 1);
    LOG_DEBUG("push_store_delete_task_ succ", "key_count", key_count);
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////
// handle_xxx — Per-pool business logic (called by sub-pool handle() methods)
///////////////////////////////////////////////////////////////////////////////

int ObLogResourceCollector::handle_br_task_(void *data, const int64_t thread_index)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_stopped_())) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_ISNULL(data)) {
    LOG_ERROR("invalid argument", K(data));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // HEARTBEAT, BEGIN, COMMIT, INSERT, DELETE, UPDATE
    ObLogBR *task = static_cast<ObLogBR *>(data);
    int record_type = RecordType::EUNKNOWN;

    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ObLogBR task is NULL", KR(ret));
    } else if (OB_FAIL(task->get_record_type(record_type))) {
      LOG_ERROR("ObLogBR task get_record_type fail", KR(ret), KPC(task));
    } else {
      if (HEARTBEAT == record_type || EBEGIN == record_type || ECOMMIT == record_type) {
        // For BEGIN/COMMIT BR, mark as released and check if can recycle TransCtx
        if ((EBEGIN == record_type || ECOMMIT == record_type)) {
          // Host now directly points to TransCtx
          TransCtx *trans_ctx = static_cast<TransCtx *>(task->get_host());
          if (OB_ISNULL(trans_ctx)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("trans_ctx is NULL", K(record_type), KP(task), KPC(task));
          } else {
            bool can_recycle_trans_ctx = false;
            bool is_begin_br = (EBEGIN == record_type);

            if (OB_FAIL(trans_ctx->mark_begin_commit_br_released(is_begin_br, can_recycle_trans_ctx, stop_flag_))) {
              if (OB_IN_STOP_STATE != ret) {
                LOG_ERROR("mark_begin_commit_br_released fail", KR(ret));
              }
            } else if (can_recycle_trans_ctx) {
              // All resources are ready, trigger TransCtx recycling
              uint64_t tenant_id = trans_ctx->get_tenant_id();
              ObTransID trans_id = trans_ctx->get_trans_id();

              if (OB_FAIL(trans_ctx_mgr_->remove_trans_ctx(tenant_id, trans_id))) {
                LOG_ERROR("remove_trans_ctx fail", KR(ret), K(tenant_id), K(trans_id));
              }
            }
          }
        }
        br_pool_->free(task);
      } else {
        if (OB_FAIL(revert_dml_binlog_record_(*task))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("revert_dml_binlog_record_ fail", KR(ret), KPC(task));
          }
        } else {}
      }
      ATOMIC_DEC(&br_count_);
      task = NULL;
    }
  }

  return ret;
}

int ObLogResourceCollector::handle_log_entry_task_dispatch_(void *data, const int64_t thread_index)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_stopped_())) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_ISNULL(data)) {
    LOG_ERROR("invalid argument", K(data));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Handle empty LogEntryTask asynchronously
    ObLogEntryTask *log_entry_task = static_cast<ObLogEntryTask *>(data);
    PartTransTask *part_trans_task = NULL;

    if (OB_ISNULL(log_entry_task)) {
      LOG_ERROR("log_entry_task is NULL");
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(part_trans_task = static_cast<PartTransTask *>(log_entry_task->get_host()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("host of log_entry_task is invalid, failed cast to PartTransTask", KR(ret), KPC(log_entry_task));
    } else if (OB_FAIL(revert_log_entry_task_(log_entry_task))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("revert_log_entry_task_ fail", KR(ret), KPC(log_entry_task));
      }
      // NOTE: log_entry_task_count_ is not decremented on failure here.
      // The caller will invoke err_handler_ which triggers process exit,
      // so the counter leak is acceptable.
    } else if (OB_FAIL(
                   dec_ref_cnt_and_try_to_revert_task_(part_trans_task))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("dec_ref_cnt_and_try_to_revert_task_ fail", KR(ret), KPC(part_trans_task));
      }
    } else {
      ATOMIC_DEC(&log_entry_task_count_);
      log_entry_task = NULL;
    }
  }

  return ret;
}

int ObLogResourceCollector::handle_part_trans_task_(PartTransTask *task, const int64_t thread_index)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_stopped_())) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_UNLIKELY(TCONF.test_mode_on != 0)) {
      LOG_INFO("handle part trans task", K(thread_index), KPC(task));
    }

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
        bool can_recycle_trans_ctx = false;
        // Copy the Trans ID to avoid invalidating the Trans ID when the PartTransTask is recycled
        uint64_t tenant_id = task->get_tenant_id();
        ObTransID trans_id = task->get_trans_id();
        int64_t commit_version = task->get_trans_commit_version();

        if (OB_FAIL(trans_ctx_mgr_->get_trans_ctx(tenant_id, trans_id, trans_ctx, enable_create))) {
          LOG_ERROR("get trans_ctx fail", KR(ret), K(tenant_id), K(trans_id), K(*task));
        } else if (task->is_dml_trans() && trans_ctx->has_ddl_participant() && OB_FAIL(recycle_stored_redo_(*task))) {
          LOG_ERROR("recycle stored redo for dml_participant of dist ddl trans failed", KR(ret), KPC(task), KPC(trans_ctx));
        }
        // Increase the number of participants that can be recycled
        else if (OB_FAIL(trans_ctx->inc_revertable_participant_count(all_participant_revertable, can_recycle_trans_ctx, stop_flag_))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("trans_ctx.inc_revertable_participant_count fail", KR(ret), K(*trans_ctx));
          }
        }
        // Recycle all participants if all participants are available for recycling
        else if (all_participant_revertable) {
          PartTransTask *participants = trans_ctx->get_participant_objs();

          if (OB_FAIL(trans_ctx->revert_participants(stop_flag_))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("trans_ctx.revert_participants fail", KR(ret), K(*trans_ctx));
            }
          }
          // recycle all participants
          else if (OB_NOT_NULL(participants) && OB_FAIL(revert_participants_(thread_index, participants))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("revert_participants_ fail", KR(ret), K(thread_index), K(participants), K(trans_id));
            }
          } else {
            participants = NULL;
          }

          // Check if TransCtx can be recycled (only if all BEGIN/COMMIT BR are also released)
          if (OB_SUCC(ret) && can_recycle_trans_ctx) {
            if (OB_FAIL(trans_ctx_mgr_->remove_trans_ctx(tenant_id, trans_id))) {
              LOG_ERROR("remove_trans_ctx fail", KR(ret), K(tenant_id), K(trans_id), KP(trans_ctx));
            }
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
  }

  return ret;
}

int ObLogResourceCollector::handle_store_delete_task_dispatch_(void *data, const int64_t thread_index)
{
  int ret = OB_SUCCESS;
  StoreDeleteTask *task = static_cast<StoreDeleteTask *>(data);

  if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("StoreDeleteTask is NULL", KR(ret));
  } else if (OB_FAIL(handle_store_delete_task_(task))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_store_delete_task_ fail", KR(ret), KPC(task));
    }
  }

  // Free StoreDeleteTask after processing
  if (OB_NOT_NULL(task)) {
    // Update statistics: task removed from queue
    ATOMIC_AAF(&pending_delete_tasks_, -1);
    task->~StoreDeleteTask();
    store_delete_task_allocator_.free(task);
    task = NULL;
  }

  return ret;
}

int ObLogResourceCollector::handle_lob_clean_task_(ObCDCLobAuxDataCleanTask *task, const int64_t thread_index)
{
  int ret = OB_SUCCESS;
  UNUSED(thread_index);

  ObCDCLobAuxMetaStorager &lob_aux_meta_storager = TCTX.lob_aux_meta_storager_;
  if (OB_FAIL(lob_aux_meta_storager.clean_unused_data(task))) {
    LOG_ERROR("clean lob data fail", KR(ret));
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////
// Helper methods — business logic (mostly unchanged, references updated)
///////////////////////////////////////////////////////////////////////////////

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
    if (OB_UNLIKELY(TCONF.test_mode_on != 0)) {
      LOG_INFO("dec_ref_cnt_and_try_to_revert_task_ before dec", KPC(part_trans_task));
    }
    // Decrement the reference count of partition transaction task
    // The partition transaction task needs to be recycled if the reference count becomes 0
    // Cannot continue to reference partition transaction tasks after that time, since partitioned transaction tasks may be recalled at any time
    const bool need_revert_part_trans_task = (part_trans_task->dec_ref_cnt() == 0);

    if (need_revert_part_trans_task) {
      // Cascade: push to PartTrans thread pool
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

    if (OB_UNLIKELY(TCONF.test_mode_on != 0)) {
      LOG_INFO("LogEntryTask-free", "LogEntryTask", *log_entry_task, "addr", log_entry_task, K(data_len), K(is_log_entry_stored));
    }

    if (is_log_entry_stored) {
      const bool skip_recycle_data = TCONF.skip_recycle_data != 0;
      if (! skip_recycle_data) {
        ObLogStoreKey store_key;
        palf::LSN log_lsn;

        if (OB_FAIL(log_entry_task->get_log_lsn(log_lsn))) {
          LOG_ERROR("get_log_lsn fail", KR(ret), KPC(log_entry_task));
        } else if (OB_FAIL(store_key.init(log_entry_task->get_tls_id(), log_lsn))) {
          LOG_ERROR("store_key init fail", KR(ret), KPC(log_entry_task), "tls_id", log_entry_task->get_tls_id(), K(log_lsn));
        } else if (OB_FAIL(del_store_service_data_(store_key))) {
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

    while (OB_SUCC(ret) && OB_NOT_NULL(task) && ! is_stopped_()) {
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

    if (is_stopped_()) {
      ret = OB_IN_STOP_STATE;
    }

    task = NULL;
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
      if (OB_FAIL(revert_dll_all_binlog_records(false/*is_build_baseline*/, task))) {
        if (OB_IN_STOP_STATE != ret) {
          // Reclaim all Binlog Records within a DDL partitioned transaction
          LOG_ERROR("revert_dll_all_binlog_records fail", KR(ret), K(*task));
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
      if (OB_UNLIKELY(TCONF.test_mode_on != 0)) {
        LOG_INFO("revert_single_binlog_record_", KP(br));
      }

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

int ObLogResourceCollector::revert_dml_binlog_record_(ObLogBR &br)
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
      if (OB_FAIL(lob_aux_meta_storager.del(lob_data_out_row_ctx_list, stop_flag_))) {
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
    DmlStmtTask *merged_del = static_cast<DmlStmtTask*>(br.get_merged_delete_stmt());
    if (OB_NOT_NULL(merged_del)) {
      ObLogBR *del_br = merged_del->get_binlog_record();
      if (OB_NOT_NULL(del_br)) {
        if (OB_FAIL(revert_dml_binlog_record_(*del_br))) {
          LOG_ERROR("revert merged delete br failed", KR(ret));
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
    if (OB_UNLIKELY(TCONF.test_mode_on != 0)) {
      // print while revert each row
      // print before dec_row_ref_cnt in case of task recycled by other threads and LOG will coredump
      LOG_INFO("revert_dml_binlog_record", KP(&br), K(br), KP(log_entry_task), KPC(log_entry_task));
    }

    const int64_t row_ref_cnt = log_entry_task->dec_row_ref_cnt();
    const bool need_revert_log_entry_task = (row_ref_cnt == 0);

    if (need_revert_log_entry_task) {
      if (OB_FAIL(revert_log_entry_task_(log_entry_task))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("revert_log_entry_task_ fail", KR(ret), KPC(log_entry_task));
        }
      } else {
        log_entry_task = NULL;
      }

      if (OB_SUCC(ret)) {
        // Cascade: dec PartTransTask ref_cnt, may push to PartTrans pool
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

int ObLogResourceCollector::del_store_service_data_(const ObLogStoreKey &store_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!store_key.is_valid())) {
    LOG_ERROR("store_key is invalid", K(store_key));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_stopped_()) {
    ret = OB_IN_STOP_STATE;
  } else {
    const uint64_t tenant_id = store_key.get_tenant_id();
    const int64_t current_time_us = get_timestamp();
    const int64_t local_batch_delete_size = ATOMIC_LOAD(&batch_delete_size_);
    const int64_t local_flush_interval_ms = ATOMIC_LOAD(&flush_interval_ms_);
    bool should_flush = false;
    bool is_size_flush = false;
    common::ObArray<ObLogStoreKey> keys_to_flush;

    {
      common::SpinRLockGuard rlock_guard(batch_delete_buffers_lock_);

      TenantDeleteBuffer *buffer = NULL;
      if (OB_FAIL(batch_delete_buffers_.get_refactored(tenant_id, buffer))) {
        LOG_ERROR("batch_delete_buffers_ get_refactored fail for tenant, should be initialized at tenant creation",
            KR(ret), K(store_key));
      } else if (OB_ISNULL(buffer)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("batch_delete_buffers_ entry is NULL for tenant", KR(ret), K(store_key));
      } else {
        common::ObByteLockGuard lock_guard(buffer->lock_);

        if (buffer->keys_.empty() && buffer->last_update_time_us_ == 0) {
          buffer->last_update_time_us_ = current_time_us;
        }

        if (OB_FAIL(buffer->keys_.push_back(store_key))) {
          LOG_ERROR("buffer->keys_ push_back fail", KR(ret), K(tenant_id), K(store_key));
        }

        if (OB_SUCC(ret)) {
          const bool buffer_full = (buffer->keys_.count() >= local_batch_delete_size);
          const int64_t flush_interval_us = local_flush_interval_ms * 1000;
          const bool time_reached = (buffer->last_update_time_us_ > 0 &&
              (current_time_us - buffer->last_update_time_us_ >= flush_interval_us));
          should_flush = buffer_full || time_reached;
          is_size_flush = buffer_full;
        }

        if (OB_SUCC(ret) && should_flush) {
          if (OB_FAIL(keys_to_flush.assign(buffer->keys_))) {
            LOG_ERROR("keys_to_flush assign fail", KR(ret), K(tenant_id));
          } else {
            buffer->keys_.reset();
            buffer->last_update_time_us_ = 0;
          }
        }
      }
    }

    if (OB_SUCC(ret) && should_flush && !keys_to_flush.empty()) {
      if (OB_FAIL(submit_batch_delete_keys_(tenant_id, keys_to_flush, is_size_flush))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("submit_batch_delete_keys_ fail", KR(ret), K(tenant_id));
        }
      }
    }
  }

  return ret;
}

// @deprecated: should not use it case redo_storage_key don't contain trans_id anymore
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
    column_family_handle = tenant->get_redo_storage_cf_handle();
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
      // Push to LobClean thread pool
      if (OB_FAIL(lob_clean_thread_pool_.push(clean_task))) {
        LOG_ERROR("push lob clean task to pool failed", KR(ret), KPC(clean_task), K(clean_task_interval));
      } else {
        LOG_INFO("push lob data clean task succ", KPC(clean_task), K(clean_task_interval));
      }
    }
  }
  return ret;
}

int ObLogResourceCollector::recycle_stored_redo_(PartTransTask &task)
{
  int ret = OB_SUCCESS;
  const logservice::TenantLSID &tenant_ls_id = task.get_tls_id();
  SortedRedoLogList &sorted_redo_list =  task.get_sorted_redo_list();
  RedoNodeIterator redo_iter = sorted_redo_list.redo_iter_begin();

  while (OB_SUCC(ret) && redo_iter != sorted_redo_list.redo_iter_end() && ! is_stopped_()) {
    if (OB_UNLIKELY(!redo_iter.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expected valid redo iterator", KR(ret), K(sorted_redo_list), K(redo_iter));
    } else {
      DmlRedoLogNode *dml_redo_node = static_cast<DmlRedoLogNode *>(&(*redo_iter));
      if (OB_ISNULL(dml_redo_node)) {
        ret = OB_INVALID_DATA;
        LOG_ERROR("invalid dml_redo_node convert from redo_iter", KR(ret), K(redo_iter));
      } else if (dml_redo_node->is_stored()) {
        const palf::LSN &store_log_lsn = dml_redo_node->get_start_log_lsn();
        ObLogStoreKey store_key;

        if (OB_FAIL(store_key.init(tenant_ls_id, store_log_lsn))) {
          LOG_ERROR("store_key init fail", KR(ret), K(store_key), K(tenant_ls_id), K(store_log_lsn));
        } else if (OB_FAIL(del_store_service_data_(store_key))) {
          LOG_ERROR("del_store_service_data_ fail", KR(ret), K(task));
        }
      }
    }

    if (is_stopped_()) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCC(ret)) {
      redo_iter++;
    }
  }

  return ret;
}

int ObLogResourceCollector::revert_unserved_part_trans_task_(const int64_t thread_idx, PartTransTask &task)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(recycle_stored_redo_(task))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("recycle_stored_redo_ failed", KR(ret), K(thread_idx), K(task));
    }
  } else if (OB_FAIL(recycle_part_trans_task_(thread_idx, &task))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("recycle_part_trans_task_ failed", KR(ret), K(thread_idx), K(task));
    }
  } else {
    // no more access to task
  }

  return ret;
}

int ObLogResourceCollector::revert_dll_all_binlog_records(const bool is_build_baseline, PartTransTask *task)
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
    while (OB_SUCC(ret) && OB_NOT_NULL(stmt_task) && (is_build_baseline || ! is_stopped_())) {
      DdlStmtTask *next = static_cast<DdlStmtTask *>(stmt_task->get_next());
      ObLogBR *br = stmt_task->get_binlog_record();
      stmt_task->set_binlog_record(NULL);

      if (OB_FAIL(revert_single_binlog_record_(br))) {
        LOG_ERROR("revert_single_binlog_record_ fail", KR(ret));
      }

      stmt_task = next;
    }
    if (!is_build_baseline && is_stopped_()) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////
// Batch delete support (mostly unchanged)
///////////////////////////////////////////////////////////////////////////////

int ObLogResourceCollector::init_tenant_batch_delete_buffer(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    common::SpinWLockGuard wlock_guard(batch_delete_buffers_lock_);

    TenantDeleteBuffer *existing_buffer = NULL;

    if (OB_FAIL(batch_delete_buffers_.get_refactored(tenant_id, existing_buffer))) {
      if (OB_HASH_NOT_EXIST == ret) {
        void *buf = ob_cdc_malloc(sizeof(TenantDeleteBuffer));
        TenantDeleteBuffer *new_buffer = NULL;
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("allocate TenantDeleteBuffer fail", KR(ret), K(tenant_id));
        } else if (FALSE_IT(new_buffer = new(buf) TenantDeleteBuffer())) {
        } else if (OB_FAIL(new_buffer->keys_.reserve(ATOMIC_LOAD(&batch_delete_size_)))) {
          LOG_ERROR("reserve buffer capacity fail", KR(ret), K(tenant_id));
          new_buffer->~TenantDeleteBuffer();
          ob_cdc_free(new_buffer);
          new_buffer = NULL;
        } else if (OB_FAIL(batch_delete_buffers_.set_refactored(tenant_id, new_buffer))) {
          LOG_ERROR("batch_delete_buffers_ set_refactored fail", KR(ret), K(tenant_id));
          new_buffer->~TenantDeleteBuffer();
          ob_cdc_free(new_buffer);
          new_buffer = NULL;
        } else {
          LOG_DEBUG("init_tenant_batch_delete_buffer succ", K(tenant_id));
        }
      } else {
        LOG_ERROR("batch_delete_buffers_ get_refactored fail", KR(ret), K(tenant_id));
      }
    } else {
      LOG_DEBUG("init_tenant_batch_delete_buffer: buffer already exists", K(tenant_id));
    }
  }

  return ret;
}

void ObLogResourceCollector::clean_tenant_batch_delete_buffers(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_WARN("ResourceCollector has not been initialized, skip clean", K(tenant_id));
  } else {
    common::SpinWLockGuard wlock_guard(batch_delete_buffers_lock_);

    TenantDeleteBuffer *buffer = NULL;
    if (OB_FAIL(batch_delete_buffers_.get_refactored(tenant_id, buffer))) {
      if (OB_HASH_NOT_EXIST == ret) {
        LOG_TRACE("clean_tenant_batch_delete_buffers: buffer not exist", K(tenant_id));
      } else {
        LOG_WARN("batch_delete_buffers_ get_refactored fail", KR(ret), K(tenant_id));
      }
    } else {
      int64_t key_count = (NULL != buffer) ? buffer->keys_.count() : 0;
      int tmp_ret = batch_delete_buffers_.erase_refactored(tenant_id);
      if (OB_SUCCESS != tmp_ret && OB_HASH_NOT_EXIST != tmp_ret) {
        LOG_WARN("batch_delete_buffers_ erase_refactored fail", KR(tmp_ret), K(tenant_id));
      } else {
        if (NULL != buffer) {
          buffer->~TenantDeleteBuffer();
          ob_cdc_free(buffer);
          buffer = NULL;
        }
        LOG_INFO("clean_tenant_batch_delete_buffers succ", K(tenant_id), "key_count", key_count);
      }
    }
  }
}

int ObLogResourceCollector::flush_batch_delete_by_time()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ResourceCollector has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    ret = flush_batch_delete_by_time_();
  }

  return ret;
}

int ObLogResourceCollector::StoreDeleteTask::init(const uint64_t tenant_id, const common::ObArray<ObLogStoreKey> &keys)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  if (OB_FAIL(keys_.assign(keys))) {
    LOG_ERROR("keys_ assign fail", KR(ret), K(tenant_id), "key_count", keys.count());
  }
  return ret;
}

int ObLogResourceCollector::handle_store_delete_task_(StoreDeleteTask *task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(task)) {
    LOG_ERROR("StoreDeleteTask is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else if (task->keys_.empty()) {
    LOG_DEBUG("StoreDeleteTask has no keys to delete", KPC(task));
  } else {
    // Get tenant guard and cf_handle, verify tenant is still valid
    ObLogTenantGuard guard;
    ObLogTenant *tenant = NULL;
    void *column_family_handle = NULL;

    if (OB_FAIL(TCTX.get_tenant_guard(task->tenant_id_, guard))) {
      // Tenant may have been deleted, skip this delete operation
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("tenant not exist, skip delete operation", K(task->tenant_id_), "key_count", task->keys_.count());
        ret = OB_SUCCESS;  // Not an error, just skip
      } else {
        LOG_ERROR("get_tenant_guard fail", KR(ret), K(task->tenant_id_));
      }
    } else if (OB_ISNULL(tenant = guard.get_tenant())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant is NULL", K(task->tenant_id_));
    } else if (OB_ISNULL(column_family_handle = tenant->get_redo_storage_cf_handle())) {
      // Column family may have been dropped, skip this delete operation
      LOG_WARN("cf_handle is NULL, tenant may be dropping, skip delete operation",
          K(task->tenant_id_), "key_count", task->keys_.count());
    } else {
      // Execute batch delete with timing
      int64_t start_time_us = get_timestamp();
      if (OB_FAIL(store_service_->batch_delete(column_family_handle, task->keys_))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("store_service_ batch_delete fail", KR(ret), KPC(task), "key_count", task->keys_.count());
        }
      } else {
        // Update statistics
        int64_t elapsed_time_us = get_timestamp() - start_time_us;
        ATOMIC_AAF(&total_delete_operations_, 1);
        ATOMIC_AAF(&total_delete_time_us_, elapsed_time_us);
        LOG_TRACE("store_service_ batch_delete succ", KPC(task), "key_count", task->keys_.count(),
            "time_cost_us", elapsed_time_us);
      }
    }
  }

  return ret;
}

int ObLogResourceCollector::submit_batch_delete_keys_(const uint64_t tenant_id,
    common::ObArray<ObLogStoreKey> &keys, const bool is_size_flush)
{
  int ret = OB_SUCCESS;

  if (keys.empty()) {
    // nothing to submit
  } else {
    void *ptr = NULL;
    StoreDeleteTask *task = NULL;

    if (OB_ISNULL(ptr = store_delete_task_allocator_.alloc(sizeof(StoreDeleteTask)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("store_delete_task_allocator_ alloc fail", KR(ret), K(tenant_id), "key_count", keys.count());
    } else {
      task = new(ptr) StoreDeleteTask();
      if (OB_FAIL(task->init(tenant_id, keys))) {
        task->~StoreDeleteTask();
        store_delete_task_allocator_.free(task);
        task = NULL;
      } else if (OB_FAIL(push_store_delete_task_(task))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("push_store_delete_task_ fail", KR(ret), K(tenant_id), "key_count", keys.count());
        }
        task = NULL;
      } else {
        LOG_DEBUG("[BATCH_DELETE] keys submitted", K(tenant_id), "key_count", keys.count(),
            "trigger", is_size_flush ? "SIZE" : "TIME");
      }
    }
  }

  return ret;
}

int ObLogResourceCollector::flush_batch_delete_by_time_()
{
  int ret = OB_SUCCESS;
  const int64_t current_time_us = get_timestamp();
  const int64_t local_flush_interval_ms = ATOMIC_LOAD(&flush_interval_ms_);

  static const int64_t MAX_FLUSH_TENANTS = 128;
  uint64_t flush_tenant_ids[MAX_FLUSH_TENANTS];
  common::ObArray<ObLogStoreKey> flush_keys_arrays[MAX_FLUSH_TENANTS];
  int64_t flush_count = 0;

  {
    common::SpinRLockGuard rlock_guard(batch_delete_buffers_lock_);

    for (common::hash::ObHashMap<uint64_t, TenantDeleteBuffer *>::iterator it = batch_delete_buffers_.begin();
        OB_SUCC(ret) && it != batch_delete_buffers_.end(); ++it) {
      TenantDeleteBuffer *buffer = it->second;

      if (OB_NOT_NULL(buffer)) {
        common::ObByteLockGuard lock_guard(buffer->lock_);

        const int64_t flush_interval_us = local_flush_interval_ms * 1000;
        const bool time_reached = (!buffer->keys_.empty() &&
            buffer->last_update_time_us_ > 0 &&
            (current_time_us - buffer->last_update_time_us_ >= flush_interval_us));

        if (time_reached && flush_count < MAX_FLUSH_TENANTS) {
          if (OB_FAIL(flush_keys_arrays[flush_count].assign(buffer->keys_))) {
            LOG_ERROR("flush_keys assign fail", KR(ret), "tenant_id", it->first);
          } else {
            flush_tenant_ids[flush_count] = it->first;
            buffer->keys_.reset();
            buffer->last_update_time_us_ = 0;
            flush_count++;
          }
        }
      }
    }
  }

  int64_t flushed_tenant_count = 0;
  int64_t total_flushed_keys = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < flush_count; ++i) {
    const int64_t key_count = flush_keys_arrays[i].count();

    if (key_count > 0) {
      if (OB_FAIL(submit_batch_delete_keys_(flush_tenant_ids[i], flush_keys_arrays[i], false))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("submit_batch_delete_keys_ fail", KR(ret), "tenant_id", flush_tenant_ids[i]);
        }
      } else {
        flushed_tenant_count++;
        total_flushed_keys += key_count;
      }
    }
  }

  if (flushed_tenant_count > 0) {
    LOG_INFO("[BATCH_DELETE] time-based flush triggered", "tenant_count", flushed_tenant_count,
        "total_keys", total_flushed_keys);
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////
// Statistics
///////////////////////////////////////////////////////////////////////////////

void ObLogResourceCollector::do_stat_(PartTransTask &task,
    const bool need_accumulate_stat)
{
  int64_t cnt = 1;

  if (! need_accumulate_stat) {
    cnt = -1;
  }

  (void)ATOMIC_AAF(&total_part_trans_task_count_, cnt);

  if (task.is_ddl_trans()) {
    LOG_DEBUG("do_stat_ for ddl_trans", K_(ddl_part_trans_task_count), K(cnt), K(task), "lbt", lbt());
    (void)ATOMIC_AAF(&ddl_part_trans_task_count_, cnt);
  } else if (task.is_dml_trans()) {
    (void)ATOMIC_AAF(&dml_part_trans_task_count_, cnt);
  } else if (task.is_ls_heartbeat() || task.is_global_heartbeat()) {
    (void)ATOMIC_AAF(&hb_part_trans_task_count_, cnt);
  } else {
    (void)ATOMIC_AAF(&other_part_trans_task_count_, cnt);
  }
}

void ObLogResourceCollector::get_task_count(
    int64_t &part_trans_task_count,
    int64_t &br_count,
    int64_t &log_entry_task_count,
    int64_t &store_delete_task_count,
    int64_t &queue_task_count) const
{
  part_trans_task_count = ATOMIC_LOAD(&total_part_trans_task_count_);
  br_count = ATOMIC_LOAD(&br_count_);
  log_entry_task_count = ATOMIC_LOAD(&log_entry_task_count_);
  store_delete_task_count = ATOMIC_LOAD(&pending_delete_tasks_);

  // Sum all sub-pool queue depths
  queue_task_count = 0;

  int64_t tmp = 0;
  // ObMQThread::get_total_task_num is non-const but only reads queue totals, const_cast is safe
  const_cast<ObRCBRPool&>(br_thread_pool_).get_total_task_num(tmp);
  queue_task_count += tmp;

  tmp = 0;
  const_cast<ObRCLogEntryTaskPool&>(log_entry_thread_pool_).get_total_task_num(tmp);
  queue_task_count += tmp;

  tmp = 0;
  const_cast<ObRCStoreDeletePool&>(store_delete_thread_pool_).get_total_task_num(tmp);
  queue_task_count += tmp;

  queue_task_count += part_trans_thread_pool_.get_queue_count();
  queue_task_count += lob_clean_thread_pool_.get_queue_count();
}

void ObLogResourceCollector::print_stat_info() const
{
  int64_t total_pending_keys = 0;
  int64_t tenant_buffer_count = 0;
  {
    common::SpinRLockGuard rlock_guard(batch_delete_buffers_lock_);
    for (common::hash::ObHashMap<uint64_t, TenantDeleteBuffer *>::const_iterator it = batch_delete_buffers_.begin();
        it != batch_delete_buffers_.end(); ++it) {
      const TenantDeleteBuffer *buffer = it->second;
      if (NULL != buffer) {
        total_pending_keys += buffer->keys_.count();
      }
      tenant_buffer_count++;
    }
  }

  // Calculate incremental averages (since last stat)
  int64_t current_ops = ATOMIC_LOAD(&total_delete_operations_);
  int64_t current_keys = ATOMIC_LOAD(&total_delete_keys_);
  int64_t current_time_us = ATOMIC_LOAD(&total_delete_time_us_);

  int64_t delta_ops = current_ops - last_stat_delete_operations_;
  int64_t delta_keys = current_keys - last_stat_delete_keys_;
  int64_t delta_time_us = current_time_us - last_stat_delete_time_us_;

  int64_t avg_batch_size = (delta_ops > 0) ? (delta_keys / delta_ops) : 0;
  int64_t avg_time_us = (delta_ops > 0) ? (delta_time_us / delta_ops) : 0;
  int64_t avg_time_ms = avg_time_us / 1000;

  // Update snapshot values for next stat
  last_stat_delete_keys_ = current_keys;
  last_stat_delete_operations_ = current_ops;
  last_stat_delete_time_us_ = current_time_us;

  // Get per-pool queue depths
  int64_t br_queue_depth = 0;
  const_cast<ObRCBRPool&>(br_thread_pool_).get_total_task_num(br_queue_depth);

  int64_t log_entry_queue_depth = 0;
  const_cast<ObRCLogEntryTaskPool&>(log_entry_thread_pool_).get_total_task_num(log_entry_queue_depth);

  int64_t part_trans_queue_depth = part_trans_thread_pool_.get_queue_count();

  int64_t store_delete_queue_depth = 0;
  const_cast<ObRCStoreDeletePool&>(store_delete_thread_pool_).get_total_task_num(store_delete_queue_depth);

  int64_t lob_clean_queue_depth = lob_clean_thread_pool_.get_queue_count();

  int64_t total_queue_depth = br_queue_depth + log_entry_queue_depth + part_trans_queue_depth
      + store_delete_queue_depth + lob_clean_queue_depth;

  _LOG_INFO("[RESOURCE_COLLECTOR] [STAT] BR=%ld LOG_ENTRY=%ld TOTAL_PART=%ld DDL=%ld DML=%ld HB=%ld OTHER=%ld "
      "[QUEUE] TOTAL_DEPTH=%ld BR_Q=%ld LOGENTRY_Q=%ld PARTTRANS_Q=%ld STOREDEL_Q=%ld LOB_Q=%ld "
      "[DELETE] TOTAL_KEYS=%ld TOTAL_OPS=%ld TOTAL_TASKS=%ld PENDING_TASKS=%ld "
      "AVG_BATCH_SIZE=%ld AVG_TIME_MS=%ld "
      "[BUFFER] PENDING_KEYS=%ld BUFFER_TENANTS=%ld BATCH_SIZE=%ld FLUSH_INTERVAL_MS=%ld",
      br_count_,
      log_entry_task_count_,
      total_part_trans_task_count_,
      ddl_part_trans_task_count_,
      dml_part_trans_task_count_,
      hb_part_trans_task_count_,
      other_part_trans_task_count_,
      total_queue_depth,
      br_queue_depth,
      log_entry_queue_depth,
      part_trans_queue_depth,
      store_delete_queue_depth,
      lob_clean_queue_depth,
      current_keys,
      current_ops,
      ATOMIC_LOAD(&total_delete_tasks_),
      ATOMIC_LOAD(&pending_delete_tasks_),
      avg_batch_size,
      avg_time_ms,
      total_pending_keys,
      tenant_buffer_count,
      ATOMIC_LOAD(&batch_delete_size_),
      ATOMIC_LOAD(&flush_interval_ms_));
}

} // namespace libobcdc
} // namespace oceanbase
