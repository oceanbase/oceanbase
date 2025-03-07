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
#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/table_load/ob_table_load_service.h"

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
  if (OB_UNLIKELY(arg_.table_name_.empty() || arg_.parallel_ <= 0 || arg_.max_error_row_count_ < 0 ||
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
  } else if (OB_FAIL(init_param(param))) {
    LOG_WARN("fail to init param", KR(ret));
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
      client_task->heart_beat(); // 保持心跳
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
            ret = client_error_code;
            LOG_WARN("client status error", KR(ret), K(client_status), K(client_error_code));
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
    res_.table_id_ = client_task->get_table_id();
    res_.task_id_ = client_task->get_task_id();
    client_task->get_status(res_.status_, res_.error_code_);
  }
  if (nullptr != client_task) {
    ObTableLoadClientService::revert_task(client_task);
    client_task = nullptr;
  }
  return ret;
}

int ObTableDirectLoadBeginExecutor::init_param(ObTableLoadClientTaskParam &param)
{
  int ret = OB_SUCCESS;
  param.reset();
  OX(param.set_task_id(ObTableLoadClientService::generate_task_id()));
  OX(param.set_client_addr(ctx_.get_user_client_addr()));
  OX(param.set_tenant_id(ctx_.get_tenant_id()));
  OX(param.set_user_id(ctx_.get_user_id()));
  OX(param.set_database_id(ctx_.get_database_id()));
  OZ(param.set_table_name(arg_.table_name_));
  OX(param.set_parallel(arg_.parallel_));
  OX(param.set_max_error_row_count(arg_.max_error_row_count_));
  OX(param.set_dup_action(arg_.dup_action_));
  OX(param.set_timeout_us(arg_.timeout_));
  OX(param.set_heartbeat_timeout_us(arg_.heartbeat_timeout_));
  OZ(param.set_load_method(arg_.load_method_));
  OZ(param.set_column_names(arg_.column_names_));
  OZ(param.set_part_names(arg_.part_names_));
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
  ObTableLoadClientTaskBrief *client_task_brief = nullptr;
  ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
  if (OB_FAIL(ObTableLoadClientService::get_task(key, client_task))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("fail to get client task", KR(ret), K(key));
    } else {
      // 处理重试场景
      ret = OB_SUCCESS;
      if (OB_FAIL(ObTableLoadClientService::get_task_brief(key, client_task_brief))) {
        LOG_WARN("fail to get client task brief", KR(ret), K(key));
      } else if (ObTableLoadClientStatus::COMMIT == client_task_brief->client_status_) {
        LOG_INFO("client task is commit", KR(ret));
      } else {
        ret = client_task_brief->error_code_;
        LOG_WARN("client task is failed", KR(ret), KPC(client_task_brief));
      }
    }
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
        LOG_WARN("fail to get client task brief", KR(ret), K(key));
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
    ObTableLoadSharedAllocatorHandle allocator_handle;
    const int64_t data_len = payload.length();
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_FAIL(ObTableLoadSharedAllocatorHandle::make_handle(
          allocator_handle, "TLD_share_alloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()))) {
      LOG_WARN("fail to make allocator handle", KR(ret));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator_handle->alloc(data_len)))) {
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
    client_task->heart_beat();
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
