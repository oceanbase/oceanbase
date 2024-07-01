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

#include "ob_table_direct_load_rpc_executor.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/table_load/ob_table_load_client_service.h"
#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/table_load/ob_table_load_exec_ctx.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_service.h"
#include "sql/resolver/dml/ob_hint.h"

namespace oceanbase
{
namespace observer
{
using namespace observer;
using namespace omt;
using namespace share::schema;
using namespace sql;
using namespace table;

// begin

int ObTableDirectLoadBeginExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arg_.table_name_.empty() || arg_.parallel_ <= 0 ||
                  arg_.dup_action_ == ObLoadDupActionType::LOAD_INVALID_MODE ||
                  arg_.timeout_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObTableDirectLoadBeginExecutor::set_result_header()
{
  int ret = OB_SUCCESS;
  ret = ParentType::set_result_header();
  if (OB_SUCC(ret)) {
    this->result_.header_.addr_ = ObServer::get_instance().get_self();
  }
  return ret;
}

int ObTableDirectLoadBeginExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table direct load begin", K_(arg));
  ObTableLoadClientTaskParam param;
  ObTableLoadClientTask *client_task = nullptr;
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  } else if (OB_FAIL(resolve_param(param))) {
    LOG_WARN("fail to resolve param", KR(ret));
  } else if (OB_FAIL(ObTableLoadClientService::alloc_task(client_task))) {
    LOG_WARN("fail to alloc client task", KR(ret));
  } else if (OB_FAIL(client_task->init(param))) {
    LOG_WARN("fail to init client task", KR(ret), K(param));
  } else if (OB_FAIL(client_task->start())) {
    LOG_WARN("fail to start client task", KR(ret));
  } else if (OB_FAIL(ObTableLoadClientService::add_task(client_task))) {
    LOG_WARN("fail to add client task", KR(ret));
  }

  if (OB_SUCC(ret) && !arg_.is_async_) {
    ObTableLoadClientStatus client_status = ObTableLoadClientStatus::MAX_STATUS;
    int client_error_code = OB_SUCCESS;
    while (OB_SUCC(ret) && ObTableLoadClientStatus::RUNNING != client_status) {
      if (OB_UNLIKELY(THIS_WORKER.is_timeout())) {
        ret = OB_TIMEOUT;
        LOG_WARN("worker timeout", KR(ret));
      } else {
        client_task->get_status(client_status, client_error_code);
        switch (client_status) {
          case ObTableLoadClientStatus::RUNNING:
            break;
          case ObTableLoadClientStatus::INITIALIZING:
          case ObTableLoadClientStatus::WAITTING:
            ob_usleep(200LL * 1000); // sleep 200ms
            break;
          case ObTableLoadClientStatus::ERROR:
          case ObTableLoadClientStatus::ABORT:
            ret = OB_SUCCESS == client_error_code ? OB_CANCELED : client_error_code;
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected client status", KR(ret), K(client_status));
            break;
        }
      }
    }
  }

  // fill res
  if (OB_SUCC(ret)) {
    res_.table_id_ = client_task->param_.get_table_id();
    res_.task_id_ = client_task->task_id_;
    client_task->get_status(res_.status_, res_.error_code_);
  }
  if (nullptr != client_task) {
    ObTableLoadClientService::revert_task(client_task);
    client_task = nullptr;
  }
  return ret;
}

int ObTableDirectLoadBeginExecutor::resolve_param(ObTableLoadClientTaskParam &param)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ctx_.get_tenant_id();
  const uint64_t database_id = ctx_.get_database_id();
  uint64_t table_id = 0;
  ObLoadDupActionType dup_action = arg_.dup_action_;
  ObDirectLoadMethod::Type method = ObDirectLoadMethod::INVALID_METHOD;
  ObDirectLoadInsertMode::Type insert_mode = ObDirectLoadInsertMode::INVALID_INSERT_MODE;
  param.reset();
  if (OB_FAIL(ObTableLoadSchema::get_table_id(tenant_id, database_id, arg_.table_name_,
                                                     table_id))) {
    LOG_WARN("fail to get table id", KR(ret), K(tenant_id), K(database_id), K_(arg));
  } else if (arg_.load_method_.empty()) {
    method = ObDirectLoadMethod::FULL;
    insert_mode = ObDirectLoadInsertMode::NORMAL;
  } else {
    ObDirectLoadHint::LoadMethod load_method = ObDirectLoadHint::get_load_method_value(arg_.load_method_);
    switch (load_method) {
      case ObDirectLoadHint::FULL:
        method = ObDirectLoadMethod::FULL;
        insert_mode = ObDirectLoadInsertMode::NORMAL;
        break;
      case ObDirectLoadHint::INC:
        method = ObDirectLoadMethod::INCREMENTAL;
        insert_mode = ObDirectLoadInsertMode::NORMAL;
        break;
      case ObDirectLoadHint::INC_REPLACE:
        if (OB_UNLIKELY(ObLoadDupActionType::LOAD_STOP_ON_DUP != dup_action)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("replace or ignore for inc_replace load method not supported", KR(ret),
                   K(arg_.load_method_), K(arg_.dup_action_));
        } else {
          dup_action = ObLoadDupActionType::LOAD_REPLACE; //rewrite dup action
          method = ObDirectLoadMethod::INCREMENTAL;
          insert_mode = ObDirectLoadInsertMode::INC_REPLACE;
        }
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid load method", KR(ret), K(arg_.load_method_));
        break;
    }
  }
  if (OB_SUCC(ret)) {
    param.set_client_addr(ctx_.get_user_client_addr());
    param.set_tenant_id(tenant_id);
    param.set_user_id(ctx_.get_user_id());
    param.set_database_id(database_id);
    param.set_table_id(table_id);
    param.set_parallel(arg_.parallel_);
    param.set_max_error_row_count(arg_.max_error_row_count_);
    param.set_dup_action(dup_action);
    param.set_timeout_us(arg_.timeout_);
    param.set_heartbeat_timeout_us(arg_.heartbeat_timeout_);
    param.set_method(method);
    param.set_insert_mode(insert_mode);
  }
  return ret;
}

// commit

int ObTableDirectLoadCommitExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObTableDirectLoadCommitExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table direct load commit", K_(arg));
  ObTableLoadClientTask *client_task = nullptr;
  ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
  if (OB_FAIL(ObTableLoadClientService::get_task(key, client_task))) {
    LOG_WARN("fail to get client task", KR(ret), K(key));
  } else if (OB_FAIL(client_task->commit())) {
    LOG_WARN("fail to commit client task", KR(ret), K(key));
  }
  if (nullptr != client_task) {
    ObTableLoadClientService::revert_task(client_task);
    client_task = nullptr;
  }
  return ret;
}

// abort

int ObTableDirectLoadAbortExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObTableDirectLoadAbortExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table direct load abort", K_(arg));
  ObTableLoadClientTask *client_task = nullptr;
  ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
  if (OB_FAIL(ObTableLoadClientService::get_task(key, client_task))) {
    LOG_WARN("fail to get client task", KR(ret), K(key));
  } else {
    client_task->abort();
  }
  if (nullptr != client_task) {
    ObTableLoadClientService::revert_task(client_task);
    client_task = nullptr;
  }
  return ret;
}

// get_status

int ObTableDirectLoadGetStatusExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObTableDirectLoadGetStatusExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table direct load get status", K_(arg));
  ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
  ObTableLoadClientTask *client_task = nullptr;
  ObTableLoadClientTaskBrief *client_task_brief = nullptr;
  if (OB_FAIL(ObTableLoadClientService::get_task(key, client_task))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("fail to get client task", KR(ret), K(key));
    } else {
      ret = OB_SUCCESS;
      if (OB_FAIL(ObTableLoadClientService::get_task_brief(key, client_task_brief))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to get client task brief", KR(ret), K(key));
        }
      } else {
        res_.status_ = client_task_brief->client_status_;
        res_.error_code_ = client_task_brief->error_code_;
      }
    }
  } else {
    client_task->get_status(res_.status_, res_.error_code_);
  }
  if (nullptr != client_task) {
    ObTableLoadClientService::revert_task(client_task);
    client_task = nullptr;
  }
  if (nullptr != client_task_brief) {
    ObTableLoadClientService::revert_task_brief(client_task_brief);
    client_task_brief = nullptr;
  }
  return ret;
}

// insert

int ObTableDirectLoadInsertExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_ || arg_.payload_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObTableDirectLoadInsertExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("table direct load insert", K_(arg));
  ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
  ObTableLoadClientTask *client_task = nullptr;
  if (OB_FAIL(ObTableLoadClientService::get_task(key, client_task))) {
    LOG_WARN("fail to get client task", KR(ret), K(key));
  } else if (OB_FAIL(client_task->check_status(ObTableLoadClientStatus::RUNNING))) {
    LOG_WARN("fail to check status", KR(ret));
  } else {
    ObTableLoadObjRowArray obj_rows;
    if (OB_FAIL(decode_payload(arg_.payload_, obj_rows))) {
      LOG_WARN("fail to decode payload", KR(ret), K_(arg));
    } else if (OB_FAIL(client_task->write(obj_rows))) {
      LOG_WARN("fail to write", KR(ret));
    }
    if (OB_FAIL(ret)) {
      client_task->set_status_error(ret);
    }
  }
  if (nullptr != client_task) {
    ObTableLoadClientService::revert_task(client_task);
    client_task = nullptr;
  }
  return ret;
}

int ObTableDirectLoadInsertExecutor::decode_payload(const ObString &payload,
                                                    ObTableLoadObjRowArray &obj_row_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(payload.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(payload));
  } else {
    ObTableLoadSharedAllocatorHandle allocator_handle =
      ObTableLoadSharedAllocatorHandle::make_handle("TLD_share_alloc", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                    MTL_ID());
    const int64_t data_len = payload.length();
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator_handle->alloc(data_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(data_len));
    } else {
      MEMCPY(buf, payload.ptr(), data_len);
      obj_row_array.set_allocator(allocator_handle);
      if (OB_FAIL(obj_row_array.deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize obj rows", KR(ret));
      }
    }
  }
  return ret;
}

// heart_beat
int ObTableDirectLoadHeartBeatExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObTableDirectLoadHeartBeatExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table direct load heart beat", K_(arg));
  ObTableLoadClientTask *client_task = nullptr;
  ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
  if (OB_FAIL(ObTableLoadClientService::get_task(key, client_task))) {
    LOG_WARN("fail to get client task", KR(ret), K(key));
  } else {
    client_task->get_exec_ctx()->last_heartbeat_time_ = ObTimeUtil::current_time();
    client_task->get_status(res_.status_, res_.error_code_);
  }
  if (nullptr != client_task) {
    ObTableLoadClientService::revert_task(client_task);
    client_task = nullptr;
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
