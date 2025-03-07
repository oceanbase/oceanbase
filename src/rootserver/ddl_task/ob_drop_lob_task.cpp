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

#define USING_LOG_PREFIX RS

#include "ob_drop_lob_task.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "rootserver/ob_root_service.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

ObDropLobTask::ObDropLobTask()
  : ObDDLTask(DDL_DROP_LOB), wait_trans_ctx_(), root_service_(NULL), ddl_arg_()
{
}

ObDropLobTask::~ObDropLobTask()
{
}

int ObDropLobTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const uint64_t aux_lob_meta_table_id,
    const uint64_t data_table_id,
    const int64_t schema_version,
    const int64_t parent_task_id,
    const int64_t consumer_group_id,
    const obrpc::ObDDLArg &ddl_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0 || OB_INVALID_ID == data_table_id
      || schema_version <= 0 || parent_task_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(task_id), K(data_table_id),
        K(schema_version), K(parent_task_id));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", KR(ret));
  } else if (OB_FAIL(deep_copy_ddl_arg(allocator_, ddl_arg, ddl_arg_))) {
    LOG_WARN("deep copy drop index arg failed", KR(ret));
  } else {
    set_gmt_create(ObTimeUtility::current_time());
    tenant_id_ = tenant_id;
    object_id_ = data_table_id;
    target_object_id_ = aux_lob_meta_table_id;
    schema_version_ = schema_version;
    task_id_ = task_id;
    parent_task_id_ = parent_task_id;
    consumer_group_id_ = consumer_group_id;
    task_version_ = OB_DROP_LOB_TASK_VERSION;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    is_inited_ = true;
    ddl_tracing_.open();
  }
  return ret;
}

int ObDropLobTask::init(
    const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(!task_record.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(task_record));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", KR(ret));
  } else {
    tenant_id_ = task_record.tenant_id_;
    object_id_ = task_record.object_id_;
    target_object_id_ = task_record.target_object_id_;
    schema_version_ = task_record.schema_version_;
    task_id_ = task_record.task_id_;
    parent_task_id_ = task_record.parent_task_id_;
    task_version_ = task_record.task_version_;
    ret_code_ = task_record.ret_code_;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    if (nullptr != task_record.message_.ptr()) {
      int64_t pos = 0;
      if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(), task_record.message_.length(), pos))) {
        LOG_WARN("deserialize params from message failed", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      is_inited_ = true;
      // set up span during recover task
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

bool ObDropLobTask::is_valid() const
{
  return is_inited_ && !trace_id_.is_invalid();
}

int ObDropLobTask::prepare(const ObDDLTaskStatus new_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropLobTask has not been inited", KR(ret));
  } else if (OB_FAIL(switch_status(new_status, true, ret))) {
    LOG_WARN("switch status failed", KR(ret));
  }
  return ret;
}

int ObDropLobTask::drop_lob_impl()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start drop lob", KPC(this));
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  ObSqlString drop_lob_sql;
  if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root_service is nullptr", KR(ret));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema))) {
    LOG_WARN("get data table schema failed", KR(ret), K(object_id_));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("data table schema is null", KR(ret), K(object_id_));
  } else if (OB_FAIL(drop_lob_sql.assign(ddl_arg_.ddl_stmt_str_))) {
    LOG_WARN("assign user drop index sql failed", KR(ret));
  } else {
    int64_t ddl_rpc_timeout = 0;
    obrpc::ObDropLobArg arg;
    arg.tenant_id_ = tenant_id_;
    arg.exec_tenant_id_ = tenant_id_;
    arg.data_table_id_ = object_id_;
    arg.aux_lob_meta_table_id_ = target_object_id_;
    arg.task_id_ = task_id_;
    arg.session_id_ = data_table_schema->get_session_id();
    arg.ddl_stmt_str_ = drop_lob_sql.string();
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(2 * data_table_schema->get_all_part_num(), ddl_rpc_timeout))) {
      LOG_WARN("failed to get ddl rpc timeout", KR(ret));
    } else if (FALSE_IT(schema_guard.reset())) {
    } else if (OB_FAIL(root_service_->get_common_rpc_proxy().timeout(ddl_rpc_timeout).drop_lob(arg))) {
      LOG_WARN("drop lob failed", KR(ret), K(ddl_rpc_timeout));
    }
    LOG_INFO("finish drop lob", KR(ret), K(arg));
  }
  return ret;
}

int ObDropLobTask::drop_lob(const ObDDLTaskStatus new_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(drop_lob_impl())) {
    LOG_WARN("send drop lob rpc failed", KR(ret));
  } else if (OB_FAIL(switch_status(new_status, true, ret))) {
    LOG_WARN("switch status failed", KR(ret));
  }
  return ret;
}

int ObDropLobTask::succ()
{
  return cleanup();
}

int ObDropLobTask::fail()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(drop_lob_impl())) {
    LOG_WARN("drop lob impl failed", KR(ret));
  } else if (OB_FAIL(cleanup())) {
    LOG_WARN("cleanup failed", KR(ret));
  }
  return ret;
}

int ObDropLobTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", KR(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(root_service_->get_sql_proxy(), tenant_id_, task_id_))) {
    LOG_WARN("delete task record failed", KR(ret), K(task_id_), K(schema_version_));
  } else {
    need_retry_ = false;      // clean succ, stop the task
  }

  if (OB_SUCC(ret) && parent_task_id_ > 0) {
    const ObDDLTaskID parent_task_id(tenant_id_, parent_task_id_);
    root_service_->get_ddl_task_scheduler().on_ddl_task_finish(parent_task_id, get_task_key(), ret_code_, trace_id_);
  }
  LOG_INFO("clean task finished", KR(ret), K(*this));
  return ret;
}

int ObDropLobTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropLobTask has not been inited", KR(ret));
  } else if (!need_retry()) {
    // task is done
  } else if (OB_FAIL(check_switch_succ_())) {
    LOG_WARN("check need retry failed", KR(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
      case ObDDLTaskStatus::PREPARE:
        if (OB_FAIL(prepare(WAIT_TRANS_END_FOR_UNUSABLE))) {
          LOG_WARN("prepare failed", KR(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_TRANS_END_FOR_UNUSABLE:
        if (OB_FAIL(wait_trans_end(wait_trans_ctx_, DROP_SCHEMA))) {
          LOG_WARN("wait trans end failed", KR(ret));
        }
        break;
      case ObDDLTaskStatus::DROP_SCHEMA:
        if (OB_FAIL(drop_lob(SUCCESS))) {
          LOG_WARN("drop lob failed", KR(ret));
        }
        break;
      case ObDDLTaskStatus::SUCCESS:
        if (OB_FAIL(succ())) {
          LOG_WARN("do succ procedure failed", KR(ret));
        }
        break;
      case ObDDLTaskStatus::FAIL:
        if (OB_FAIL(fail())) {
          LOG_WARN("do fail procedure failed", KR(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, task status is not valid", KR(ret), K(task_status_));
    }
    ddl_tracing_.release_span_hierarchy();
  }
  return ret;
}

// switch to SUCCESS if lob table does not exist.
int ObDropLobTask::check_switch_succ_()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema_ptr = nullptr;
  bool is_index_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", KR(ret));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", KR(ret));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema_ptr))) {
    LOG_WARN("faild to get_table_schema", KR(ret), K(tenant_id_), K(object_id_));
  } else if (OB_ISNULL(data_table_schema_ptr)) {
    // drop lob task may retry because rpc timeout, and data table dropped before retry, so we should ignore this situation
    task_status_ = ObDDLTaskStatus::SUCCESS;
    LOG_INFO("data table may be dropped, we do not need to retry", KR(ret), K(object_id_));
  } else if (target_object_id_ != data_table_schema_ptr->get_aux_lob_meta_tid()) {
    // lob_meta_table and lob_piece_table will be deleted at same time.
    task_status_ = ObDDLTaskStatus::SUCCESS;
    LOG_INFO("lob has been dropped", KR(ret), KPC(this), KPC(data_table_schema_ptr));
  }
  return ret;
}

int ObDropLobTask::deep_copy_ddl_arg(common::ObIAllocator &allocator,
                                     const obrpc::ObDDLArg &src_ddl_arg,
                                     obrpc::ObDDLArg &dst_ddl_arg)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  const int64_t serialize_size = src_ddl_arg.get_serialize_size();
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", KR(ret), K(serialize_size));
  } else if (OB_FAIL(src_ddl_arg.ObDDLArg::serialize(buf, serialize_size, pos))) {
    LOG_WARN("serialize source index arg failed", KR(ret));
  } else if (OB_FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(dst_ddl_arg.ObDDLArg::deserialize(buf, serialize_size, pos))) {
    LOG_WARN("deserialize failed", KR(ret));
  }
  return ret;
}

int ObDropLobTask::serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_size, pos))) {
    LOG_WARN("ObDDLTask serialize failed", KR(ret));
  } else if (OB_FAIL(ddl_arg_.serialize(buf, buf_size, pos))) {
    LOG_WARN("serialize failed", KR(ret));
  }
  return ret;
}

int ObDropLobTask::deserialize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  obrpc::ObDDLArg tmp_ddl_arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, buf_size, pos))) {
    LOG_WARN("ObDDLTask deserlize failed", KR(ret));
  } else if (OB_FAIL(tmp_ddl_arg.deserialize(buf, buf_size, pos))) {
    LOG_WARN("deserialize failed", KR(ret));
  } else if (OB_FAIL(deep_copy_ddl_arg(allocator_, tmp_ddl_arg, ddl_arg_))) {
    LOG_WARN("deep copy drop index arg failed", KR(ret));
  }
  return ret;
}

int64_t ObDropLobTask::get_serialize_param_size() const
{
  return ddl_arg_.get_serialize_size() + ObDDLTask::get_serialize_param_size();
}