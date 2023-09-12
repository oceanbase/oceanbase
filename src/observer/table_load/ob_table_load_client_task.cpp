/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/table_load/ob_table_load_exec_ctx.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/table_load/ob_table_load_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace table;

ObTableLoadClientTask::ObTableLoadClientTask()
  : tenant_id_(OB_INVALID_ID),
    user_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    allocator_("TLD_ClientTask"),
    session_info_(nullptr),
    exec_ctx_(nullptr),
    task_scheduler_(nullptr),
    next_trans_idx_(0),
    table_ctx_(nullptr),
    client_status_(ObTableLoadClientStatus::MAX_STATUS),
    error_code_(OB_SUCCESS),
    ref_count_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  free_session_ctx_.sessid_ = sql::ObSQLSessionInfo::INVALID_SESSID;
}

ObTableLoadClientTask::~ObTableLoadClientTask()
{
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
    task_scheduler_->~ObITableLoadTaskScheduler();
    allocator_.free(task_scheduler_);
    task_scheduler_ = nullptr;
  }
  if (nullptr != session_info_) {
    ObTableLoadUtils::free_session_info(session_info_, free_session_ctx_);
    session_info_ = nullptr;
  }
  if (nullptr != exec_ctx_) {
    exec_ctx_->~ObTableLoadClientExecCtx();
    allocator_.free(exec_ctx_);
    exec_ctx_ = nullptr;
  }
  if (nullptr != table_ctx_) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObTableLoadService::remove_ctx(table_ctx_))) {
      LOG_WARN("fail to remove table ctx", KR(ret), KP(table_ctx_));
    }
    ObTableLoadService::put_ctx(table_ctx_);
    table_ctx_ = nullptr;
  }
}

int ObTableLoadClientTask::init(uint64_t tenant_id, uint64_t user_id, uint64_t table_id,
                                int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadClientTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id ||
                         OB_INVALID_ID == user_id || 0 == timeout_us)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(user_id), K(table_id), K(timeout_us));
  } else {
    tenant_id_ = tenant_id;
    user_id_ = user_id;
    table_id_ = table_id;
    if (OB_FAIL(init_column_names_and_idxs())) {
      LOG_WARN("fail to init column names and idxs", KR(ret));
    } else if (OB_FAIL(ObTableLoadUtils::create_session_info(user_id_, session_info_,
                                                             free_session_ctx_))) {
      LOG_WARN("fail to create session info", KR(ret));
    } else if (OB_FAIL(init_exec_ctx(timeout_us))) {
      LOG_WARN("fail to init client exec ctx", KR(ret));
    } else if (OB_FAIL(task_allocator_.init("TLD_TaskPool", MTL_ID()))) {
      LOG_WARN("fail to init task allocator", KR(ret));
    } else if (OB_ISNULL(task_scheduler_ = OB_NEWx(ObTableLoadTaskThreadPoolScheduler,
                                                   (&allocator_), 1, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadTaskThreadPoolScheduler", KR(ret));
    } else if (OB_FAIL(task_scheduler_->init())) {
      LOG_WARN("fail to init task scheduler", KR(ret));
    } else if (OB_FAIL(task_scheduler_->start())) {
      LOG_WARN("fail to start task scheduler", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadClientTask::init_column_names_and_idxs()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(
        ObTableLoadSchema::get_table_schema(tenant_id_, table_id_, schema_guard, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K_(table_id));
  } else if (OB_FAIL(
               ObTableLoadSchema::get_column_names(table_schema, allocator_, column_names_))) {
    LOG_WARN("fail to get all column name", KR(ret));
  } else if (OB_UNLIKELY(column_names_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty column names", KR(ret));
  } else if (OB_FAIL(ObTableLoadSchema::get_column_idxs(table_schema, column_idxs_))) {
    LOG_WARN("failed to get all column idx", K(ret));
  } else if (OB_UNLIKELY(column_idxs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty column idxs", KR(ret));
  } else if (OB_UNLIKELY(column_names_.count() != column_idxs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column names and idxs", KR(ret), K(column_names_), K(column_idxs_));
  }
  return ret;
}

int ObTableLoadClientTask::init_exec_ctx(int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx_ = OB_NEWx(ObTableLoadClientExecCtx, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new client exec ctx", KR(ret));
  } else {
    exec_ctx_->allocator_ = &allocator_;
    exec_ctx_->session_info_ = session_info_;
    exec_ctx_->timeout_ts_ = ObTimeUtil::current_time() + timeout_us;
  }
  return ret;
}

int ObTableLoadClientTask::set_table_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_ctx));
  } else {
    obsys::ObWLockGuard guard(rw_lock_);
    if (OB_UNLIKELY(nullptr != table_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected set table ctx twice", KR(ret), KP(table_ctx_), KP(table_ctx));
    } else {
      table_ctx->inc_ref_count();
      table_ctx_ = table_ctx;
    }
  }
  return ret;
}

int ObTableLoadClientTask::get_table_ctx(ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  table_ctx = nullptr;
  obsys::ObRLockGuard guard(rw_lock_);
  if (OB_UNLIKELY(nullptr == table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table ctx", KR(ret));
  } else {
    table_ctx = table_ctx_;
    table_ctx->inc_ref_count();
  }
  return ret;
}

int ObTableLoadClientTask::add_trans_id(const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_ids_.push_back(trans_id))) {
    LOG_WARN("fail to push back trans id", KR(ret), K(trans_id));
  }
  return ret;
}

int ObTableLoadClientTask::get_next_trans_id(ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(trans_ids_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty trans id", KR(ret));
  } else {
    const int64_t trans_idx = ATOMIC_FAA(&next_trans_idx_, 1) % trans_ids_.count();
    trans_id = trans_ids_.at(trans_idx);
  }
  return ret;
}

int ObTableLoadClientTask::set_status_running()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(rw_lock_);
  if (OB_UNLIKELY(ObTableLoadClientStatus::MAX_STATUS != client_status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unexpected status", KR(ret), K(client_status_));
  } else {
    client_status_ = ObTableLoadClientStatus::RUNNING;
  }
  return ret;
}

int ObTableLoadClientTask::set_status_committing()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(rw_lock_);
  if (OB_UNLIKELY(ObTableLoadClientStatus::RUNNING != client_status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unexpected status", KR(ret), K(client_status_));
  } else {
    client_status_ = ObTableLoadClientStatus::COMMITTING;
  }
  return ret;
}

int ObTableLoadClientTask::set_status_commit()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(rw_lock_);
  if (OB_UNLIKELY(ObTableLoadClientStatus::COMMITTING != client_status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unexpected status", KR(ret), K(client_status_));
  } else {
    client_status_ = ObTableLoadClientStatus::COMMIT;
  }
  return ret;
}

int ObTableLoadClientTask::set_status_error(int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS == error_code)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(error_code));
  } else {
    obsys::ObWLockGuard guard(rw_lock_);
    if (ObTableLoadClientStatus::ERROR == client_status_ ||
        ObTableLoadClientStatus::ABORT == client_status_) {
      // ignore
    } else {
      client_status_ = ObTableLoadClientStatus::ERROR;
      error_code_ = error_code;
    }
  }
  return ret;
}

void ObTableLoadClientTask::set_status_abort()
{
  obsys::ObWLockGuard guard(rw_lock_);
  if (ObTableLoadClientStatus::ABORT == client_status_) {
    // ignore
  } else {
    client_status_ = ObTableLoadClientStatus::ABORT;
  }
}

int ObTableLoadClientTask::check_status(ObTableLoadClientStatus client_status)
{
  int ret = OB_SUCCESS;
  obsys::ObRLockGuard guard(rw_lock_);
  if (OB_UNLIKELY(client_status != client_status_)) {
    if (ObTableLoadClientStatus::ERROR == client_status_) {
      ret = error_code_;
    } else if (ObTableLoadClientStatus::ABORT == client_status_) {
      ret = OB_CANCELED;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
  }
  return ret;
}

ObTableLoadClientStatus ObTableLoadClientTask::get_status() const
{
  obsys::ObRLockGuard guard(rw_lock_);
  return client_status_;
}

void ObTableLoadClientTask::get_status(ObTableLoadClientStatus &client_status,
                                       int &error_code) const
{
  obsys::ObRLockGuard guard(rw_lock_);
  client_status = client_status_;
  error_code = error_code_;
}

int ObTableLoadClientTask::alloc_task(ObTableLoadTask *&task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientTask not init", KR(ret));
  } else {
    if (OB_ISNULL(task = task_allocator_.alloc(MTL_ID()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc task", KR(ret));
    }
  }
  return ret;
}

void ObTableLoadClientTask::free_task(ObTableLoadTask *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientTask not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null task", KR(ret));
  } else {
    task_allocator_.free(task);
  }
}

int ObTableLoadClientTask::add_task(ObTableLoadTask *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientTask not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null task", KR(ret));
  } else {
    if (OB_FAIL(task_scheduler_->add_task(0, task))) {
      LOG_WARN("fail to add task", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
