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
#include "observer/omt/ob_tenant.h"
#include "observer/table_load/ob_table_load_client_service.h"
#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_redef_table.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

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
ObTableDirectLoadBeginExecutor::ObTableDirectLoadBeginExecutor(
  ObTableDirectLoadExecContext &ctx, const ObTableDirectLoadRequest &request,
  ObTableDirectLoadResult &result)
  : ParentType(ctx, request, result), client_task_(nullptr), table_ctx_(nullptr)
{
}

ObTableDirectLoadBeginExecutor::~ObTableDirectLoadBeginExecutor()
{
  if (nullptr != client_task_) {
    ObTableLoadClientService::revert_task(client_task_);
    client_task_ = nullptr;
  }
  if (nullptr != table_ctx_) {
    ObTableLoadService::put_ctx(table_ctx_);
    table_ctx_ = nullptr;
  }
}

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
  const uint64_t tenant_id = ctx_.get_tenant_id();
  const uint64_t user_id = ctx_.get_user_id();
  const uint64_t database_id = ctx_.get_database_id();
  uint64_t table_id = 0;

  THIS_WORKER.set_timeout_ts(ObTimeUtil::current_time() + arg_.timeout_);
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  } else if (OB_FAIL(ObTableLoadSchema::get_table_id(tenant_id, database_id, arg_.table_name_,
                                                     table_id))) {
    LOG_WARN("fail to get table id", KR(ret), K(tenant_id), K(database_id), K_(arg));
  }

  // get the existing client task if it exists
  while (OB_SUCC(ret)) {
    ObTableLoadKey key(tenant_id, table_id);
    if (OB_FAIL(ObTableLoadClientService::get_task(key, client_task_))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get client task", KR(ret), K(key));
      } else {
        ret = OB_SUCCESS;
        client_task_ = nullptr;
        break;
      }
    } else {
      bool need_wait_finish = false;
      ObTableLoadClientStatus wait_client_status;
      ObTableLoadClientStatus client_status = client_task_->get_status();
      switch (client_status) {
        case ObTableLoadClientStatus::RUNNING:
        case ObTableLoadClientStatus::COMMITTING:
          if (arg_.force_create_) {
            if (OB_FAIL(ObTableLoadClientService::abort_task(client_task_))) {
              LOG_WARN("fail to abort client task", KR(ret));
            } else {
              need_wait_finish = true;
              wait_client_status = ObTableLoadClientStatus::ABORT;
            }
          }
          break;
        case ObTableLoadClientStatus::COMMIT:
        case ObTableLoadClientStatus::ABORT:
          need_wait_finish = true;
          wait_client_status = client_status;
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected client status", KR(ret), KPC(client_task_), K(client_status));
          break;
      }
      if (OB_FAIL(ret)) {
      } else if (!need_wait_finish) {
        break;
      } else {
        ObTableLoadUniqueKey task_key(table_id, client_task_->ddl_param_.task_id_);
        ObTableLoadClientService::revert_task(client_task_);
        client_task_ = nullptr;
        if (OB_FAIL(ObTableLoadClientService::wait_task_finish(task_key))) {
          LOG_WARN("fail to wait client task finish", KR(ret), K(task_key), K(wait_client_status));
        }
      }
    }
  }

  // create new client task if it does not exist
  if (OB_SUCC(ret) && nullptr == client_task_) {
    if (OB_FAIL(ObTableLoadService::check_support_direct_load(table_id))) {
      LOG_WARN("fail to check support direct load", KR(ret), K(table_id));
    }
    // create client task
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(client_task_ = ObTableLoadClientService::alloc_task())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc client task", KR(ret));
      } else if (OB_FAIL(client_task_->init(tenant_id, user_id, database_id, table_id,
                                            arg_.timeout_, arg_.heartbeat_timeout_))) {
        LOG_WARN("fail to init client task", KR(ret));
      } else {
        // create table ctx
        if (OB_FAIL(create_table_ctx())) {
          LOG_WARN("fail to create table ctx", KR(ret));
        } else {
          client_task_->ddl_param_ = table_ctx_->ddl_param_;
          if (OB_FAIL(client_task_->set_table_ctx(table_ctx_))) {
            LOG_WARN("fail to set table ctx", KR(ret));
          }
          if (OB_FAIL(ret)) {
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(ObTableLoadService::remove_ctx(table_ctx_))) {
              LOG_WARN("fail to remove ctx", KR(tmp_ret));
            }
          }
        }
      }
    }
    // begin
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_begin())) {
        LOG_WARN("fail to do begin", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTableLoadClientService::add_task(client_task_))) {
        LOG_WARN("fail to add client task", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      if (nullptr != table_ctx_) {
        ObTableLoadCoordinator::abort_ctx(table_ctx_);
      }
    }
  }

  // fill res
  if (OB_SUCC(ret)) {
    res_.table_id_ = client_task_->table_id_;
    res_.task_id_ = client_task_->ddl_param_.task_id_;
    if (OB_FAIL(res_.column_names_.assign(client_task_->column_names_))) {
      LOG_WARN("fail to assign column names", KR(ret));
    } else {
      client_task_->get_status(res_.status_, res_.error_code_);
    }
  }

  return ret;
}

int ObTableDirectLoadBeginExecutor::create_table_ctx()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = client_task_->tenant_id_;
  const uint64_t table_id = client_task_->table_id_;
  ObTableLoadDDLParam ddl_param;
  ObTableLoadParam param;
  // start redef table
  if (OB_SUCC(ret)) {
    ObTableLoadRedefTableStartArg start_arg;
    ObTableLoadRedefTableStartRes start_res;
    uint64_t data_version = 0;
    start_arg.tenant_id_ = tenant_id;
    start_arg.table_id_ = table_id;
    start_arg.parallelism_ = arg_.parallel_;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret));
    } else if (OB_FAIL(ObTableLoadRedefTable::start(start_arg, start_res,
                                                    *client_task_->get_session_info()))) {
      LOG_WARN("fail to start redef table", KR(ret), K(start_arg));
    } else {
      ddl_param.dest_table_id_ = start_res.dest_table_id_;
      ddl_param.task_id_ = start_res.task_id_;
      ddl_param.schema_version_ = start_res.schema_version_;
      ddl_param.snapshot_version_ = start_res.snapshot_version_;
      ddl_param.data_version_ = data_version;
    }
  }
  // init param
  if (OB_SUCC(ret)) {
    ObTenant *tenant = nullptr;
    if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id, tenant))) {
      LOG_WARN("fail to get tenant", KR(ret), K(tenant_id));
    } else {
      param.tenant_id_ = tenant_id;
      param.table_id_ = table_id;
      param.batch_size_ = 100;
      param.parallel_ = arg_.parallel_;
      param.session_count_ = MIN(arg_.parallel_, (int32_t)tenant->unit_max_cpu() * 2);
      param.max_error_row_count_ = arg_.max_error_row_count_;
      param.column_count_ = client_task_->column_names_.count();
      param.need_sort_ = true;
      param.px_mode_ = false;
      param.online_opt_stat_gather_ = false;
      param.dup_action_ = arg_.dup_action_;
      if (OB_FAIL(param.normalize())) {
        LOG_WARN("fail to normalize param", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(table_ctx_ = ObTableLoadService::alloc_ctx())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc table ctx", KR(ret), K(param));
    } else if (OB_FAIL(table_ctx_->init(param, ddl_param, client_task_->get_session_info()))) {
      LOG_WARN("fail to init table ctx", KR(ret));
    } else if (OB_FAIL(ObTableLoadCoordinator::init_ctx(table_ctx_, client_task_->column_idxs_,
                                                        client_task_->get_exec_ctx()))) {
      LOG_WARN("fail to coordinator init ctx", KR(ret));
    } else if (OB_FAIL(ObTableLoadService::add_ctx(table_ctx_))) {
      LOG_WARN("fail to add ctx", KR(ret));
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (ddl_param.is_valid()) {
        ObTableLoadRedefTableAbortArg abort_arg;
        abort_arg.tenant_id_ = param.tenant_id_;
        abort_arg.task_id_ = ddl_param.task_id_;
        if (OB_TMP_FAIL(
              ObTableLoadRedefTable::abort(abort_arg, *client_task_->get_session_info()))) {
          LOG_WARN("fail to abort redef table", KR(tmp_ret), K(abort_arg));
        }
      }
      if (nullptr != table_ctx_) {
        ObTableLoadService::free_ctx(table_ctx_);
        table_ctx_ = nullptr;
      }
    }
  }
  return ret;
}

int ObTableDirectLoadBeginExecutor::do_begin()
{
  int ret = OB_SUCCESS;
  ObTableLoadCoordinator coordinator(table_ctx_);
  if (OB_FAIL(coordinator.init())) {
    LOG_WARN("fail to init coordinator", KR(ret));
  } else if (OB_FAIL(coordinator.begin())) {
    LOG_WARN("fail to coordinator begin", KR(ret));
  }
  // start trans
  for (int64_t i = 1; OB_SUCC(ret) && i <= table_ctx_->param_.session_count_; ++i) {
    ObTableLoadSegmentID segment_id(i);
    ObTableLoadTransId trans_id;
    if (OB_FAIL(coordinator.start_trans(segment_id, trans_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(i));
    } else if (OB_FAIL(client_task_->add_trans_id(trans_id))) {
      LOG_WARN("fail to add trans id", KR(ret), K(trans_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(client_task_->set_status_running())) {
      LOG_WARN("fail to set status running", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    client_task_->set_status_error(ret);
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
  } else if (OB_FAIL(client_task->check_status(ObTableLoadClientStatus::RUNNING))) {
    LOG_WARN("fail to check status", KR(ret));
  } else if (OB_FAIL(ObTableLoadClientService::commit_task(client_task))) {
    LOG_WARN("fail to commit client task", KR(ret));
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
  } else if (OB_FAIL(ObTableLoadClientService::abort_task(client_task))) {
    LOG_WARN("fail to abort client task", KR(ret));
  }
  if (nullptr != client_task) {
    ObTableLoadClientService::revert_task(client_task);
    client_task = nullptr;
  }
  if (OB_SUCC(ret) && OB_FAIL(ObTableLoadClientService::wait_task_finish(key))) {
    LOG_WARN("fail to wait client task finish", KR(ret), K(key));
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
  ObTableLoadObjRowArray obj_rows;
  ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
  ObTableLoadClientTask *client_task = nullptr;
  if (OB_FAIL(decode_payload(arg_.payload_, obj_rows))) {
    LOG_WARN("fail to decode payload", KR(ret), K_(arg));
  } else if (OB_FAIL(ObTableLoadClientService::get_task(key, client_task))) {
    LOG_WARN("fail to get client task", KR(ret), K(key));
  } else if (OB_FAIL(client_task->check_status(ObTableLoadClientStatus::RUNNING))) {
    LOG_WARN("fail to check status", KR(ret));
  } else {
    ObTableLoadTableCtx *table_ctx = nullptr;
    if (OB_FAIL(client_task->get_table_ctx(table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret));
    } else {
      ObTableLoadCoordinator coordinator(table_ctx);
      ObTableLoadTransId trans_id;
      int64_t batch_id = client_task->get_next_batch_id();
      if (OB_FAIL(set_batch_seq_no(batch_id, obj_rows))) {
        LOG_WARN("fail to set batch seq no", KR(ret));
      } else if (OB_FAIL(client_task->get_next_trans_id(trans_id))) {
        LOG_WARN("fail to get next trans id", KR(ret));
      } else if (OB_FAIL(coordinator.init())) {
        LOG_WARN("fail to init coordinator", KR(ret));
      } else if (OB_FAIL(coordinator.write(trans_id, obj_rows))) {
        LOG_WARN("fail to coordinator write", KR(ret));
      }
    }
    if (nullptr != table_ctx) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
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

int ObTableDirectLoadInsertExecutor::set_batch_seq_no(int64_t batch_id,
                                                      ObTableLoadObjRowArray &obj_row_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj_row_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(obj_row_array));
  } else if (OB_UNLIKELY(batch_id > ObTableLoadSequenceNo::MAX_BATCH_ID ||
                         obj_row_array.count() > ObTableLoadSequenceNo::MAX_BATCH_SEQ_NO)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow", KR(ret), K(batch_id), K(obj_row_array.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_row_array.count(); ++i) {
      ObTableLoadObjRow &row = obj_row_array.at(i);
      row.seq_no_.sequence_no_ = batch_id;
      row.seq_no_.sequence_no_ <<= ObTableLoadSequenceNo::BATCH_ID_SHIFT;
      row.seq_no_.sequence_no_ |= i;
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
