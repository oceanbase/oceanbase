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
#include "ob_ddl_retry_task.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/rc/context.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_ddl_sim_point.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "storage/tablelock/ob_table_lock_service.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;

ObDDLRetryTask::ObDDLRetryTask()
  : ObDDLTask(share::DDL_INVALID), ddl_arg_(nullptr), root_service_(nullptr), affected_rows_(0), 
    forward_user_message_(), allocator_(lib::ObLabel("RedefTask")), is_schema_change_done_(false)
{
}

ObDDLRetryTask::~ObDDLRetryTask()
{
  if (OB_NOT_NULL(ddl_arg_)) {
    ddl_arg_->~ObDDLArg();
    ddl_arg_ = nullptr;
  }
  if (nullptr != forward_user_message_.ptr()) {
    allocator_.free(forward_user_message_.ptr());
  }
  allocator_.reset();
}

int ObDDLRetryTask::deep_copy_ddl_arg(
    common::ObIAllocator &allocator, 
    const share::ObDDLType &ddl_type,
    const ObDDLArg *source_arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("source arg is null", K(ret));
  } else {
    int64_t pos = 0;
    char *serialize_buf = nullptr;
    char *ddl_arg_buf = nullptr;
    const int64_t serialize_size = source_arg->get_serialize_size();
    if (OB_ISNULL(serialize_buf = static_cast<char *>(allocator.alloc(serialize_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(serialize_size));
    } else if (ObDDLType::DDL_DROP_DATABASE == ddl_type) {
      if (OB_ISNULL(ddl_arg_buf = static_cast<char *>(allocator.alloc(sizeof(obrpc::ObDropDatabaseArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        ddl_arg_ = new(ddl_arg_buf)obrpc::ObDropDatabaseArg();
      }
    } else if (ObDDLType::DDL_DROP_TABLE == ddl_type) {
      if (OB_ISNULL(ddl_arg_buf = static_cast<char *>(allocator.alloc(sizeof(obrpc::ObDropTableArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        ddl_arg_ = new(ddl_arg_buf)obrpc::ObDropTableArg();
      }
    } else if (ObDDLType::DDL_TRUNCATE_TABLE == ddl_type) {
      if (OB_ISNULL(ddl_arg_buf = static_cast<char *>(allocator.alloc(sizeof(obrpc::ObTruncateTableArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        ddl_arg_ = new(ddl_arg_buf)obrpc::ObTruncateTableArg();
      }
    } else if (ObDDLType::DDL_DROP_PARTITION == ddl_type
            || ObDDLType::DDL_DROP_SUB_PARTITION == ddl_type
            || ObDDLType::DDL_TRUNCATE_PARTITION == ddl_type
            || ObDDLType::DDL_TRUNCATE_SUB_PARTITION == ddl_type
            || ObDDLType::DDL_RENAME_PARTITION == ddl_type
            || ObDDLType::DDL_RENAME_SUB_PARTITION == ddl_type) {
      if (OB_ISNULL(ddl_arg_buf = static_cast<char *>(allocator.alloc(sizeof(obrpc::ObAlterTableArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        ddl_arg_ = new(ddl_arg_buf)obrpc::ObAlterTableArg();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ddl type", K(ret), K(ddl_type));
    }
        
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ddl_arg_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl arg is nullptr", K(ret), K(ddl_type));
    } else if (OB_FAIL(source_arg->serialize(serialize_buf, serialize_size, pos))) {
      LOG_WARN("serialize alter table arg", K(ret), K(serialize_size));
    } else if (FALSE_IT(pos = 0)) {
    } else if (OB_FAIL(ddl_arg_->deserialize(serialize_buf, serialize_size, pos))) {
      LOG_WARN("deserialize ddl arg failed", K(ret), K(serialize_size));
    }
    
    if (OB_FAIL(ret)) {
      if (nullptr != ddl_arg_) {
        ddl_arg_->~ObDDLArg();
        ddl_arg_ = nullptr;
      }
      if (nullptr != serialize_buf) {
        allocator.free(serialize_buf);
        serialize_buf = nullptr;
      }
      if (nullptr != ddl_arg_buf) {
        allocator.free(ddl_arg_buf);
        ddl_arg_buf = nullptr;
      }
    }
  }
  return ret;
}

int ObDDLRetryTask::init_compat_mode(const share::ObDDLType &ddl_type,
                                     const obrpc::ObDDLArg *source_arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_arg) || !is_drop_schema_block_concurrent_trans(ddl_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KP(source_arg), K(ddl_type));
  } else if (ObDDLType::DDL_DROP_DATABASE == ddl_type) {
    compat_mode_ = static_cast<const obrpc::ObDropDatabaseArg *>(source_arg)->compat_mode_;
  } else if (ObDDLType::DDL_DROP_TABLE == ddl_type) {
    compat_mode_ = static_cast<const obrpc::ObDropTableArg *>(source_arg)->compat_mode_;
  } else if (ObDDLType::DDL_TRUNCATE_TABLE == ddl_type) {
    compat_mode_ = static_cast<const obrpc::ObTruncateTableArg *>(source_arg)->compat_mode_;
  } else if (ObDDLType::DDL_DROP_PARTITION == ddl_type
          || ObDDLType::DDL_DROP_SUB_PARTITION == ddl_type
          || ObDDLType::DDL_TRUNCATE_PARTITION == ddl_type
          || ObDDLType::DDL_TRUNCATE_SUB_PARTITION == ddl_type
          || ObDDLType::DDL_RENAME_PARTITION == ddl_type
          || ObDDLType::DDL_RENAME_SUB_PARTITION == ddl_type) {
    compat_mode_ = static_cast<const obrpc::ObAlterTableArg *>(source_arg)->compat_mode_;
  }
  return ret;
}

int ObDDLRetryTask::init(const uint64_t tenant_id, 
                         const int64_t task_id,
                         const uint64_t object_id,
                         const int64_t schema_version,
                         const int64_t consumer_group_id,
                         const int32_t sub_task_trace_id,
                         const share::ObDDLType &ddl_type,
                         const obrpc::ObDDLArg *ddl_arg, 
                         const int64_t task_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLRetryTask has already been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0 || OB_INVALID_ID == object_id
    || schema_version <= 0 || !is_drop_schema_block_concurrent_trans(ddl_type) || nullptr == ddl_arg 
    || ObDDLTaskStatus::PREPARE != task_status)) {
      ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(object_id),
      K(schema_version), K(ddl_type), KP(ddl_arg), K(task_status));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", K(ret));
    LOG_WARN("fail to init task table operator", K(ret));
  } else if (OB_FAIL(deep_copy_ddl_arg(allocator_, ddl_type, ddl_arg))) {
    LOG_WARN("deep copy ddl arg failed", K(ret));
  } else if (OB_FAIL(init_compat_mode(ddl_type, ddl_arg))) {
    LOG_WARN("init compat mode failed", K(ret));
  } else {
    set_gmt_create(ObTimeUtility::current_time());
    object_id_ = object_id;
    target_object_id_ = object_id;
    schema_version_ = schema_version;
    consumer_group_id_ = consumer_group_id;
    sub_task_trace_id_ = sub_task_trace_id;
    tenant_id_ = tenant_id;
    task_id_ = task_id;
    task_type_ = ddl_type;
    task_version_ = OB_DDL_RETRY_TASK_VERSION;
    task_status_ = static_cast<ObDDLTaskStatus>(task_status);
    is_schema_change_done_ = false;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    is_inited_ = true;
    ddl_tracing_.open();
  }
  return ret;
}

int ObDDLRetryTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task_record.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_record));
  } else if (OB_FAIL(DDL_SIM(task_record.tenant_id_, task_record.task_id_, DDL_TASK_INIT_BY_RECORD_FAILED))) {
    LOG_WARN("ddl sim failure", K(task_record.tenant_id_), K(task_record.task_id_));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", K(ret));
  } else {
    tenant_id_ = task_record.tenant_id_;
    object_id_ = task_record.object_id_;
    target_object_id_ = task_record.target_object_id_;
    schema_version_ = task_record.schema_version_;
    task_id_ = task_record.task_id_;
    task_type_ = task_record.ddl_type_;
    parent_task_id_ = task_record.parent_task_id_;
    task_version_ = task_record.task_version_;
    ret_code_ = task_record.ret_code_;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    is_schema_change_done_ = false; // do not worry about it, check_schema_change_done will correct it.
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    if (nullptr != task_record.message_) {
      int64_t pos = 0;
      if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(), task_record.message_.length(), pos))) {
        LOG_WARN("fail to deserialize params from message", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_compat_mode(task_type_, ddl_arg_))) {
    LOG_WARN("init compat mode failed", K(ret));
  } else {
    is_inited_ = true;

    // set up span during recover task
    ddl_tracing_.open_for_recovery();
  }
  return ret;
}

int ObDDLRetryTask::prepare(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(switch_status(next_task_status, true, ret))) {
    LOG_WARN("fail to switch status", K(ret));
  }
  return ret;
}

int ObDDLRetryTask::get_forward_user_message(const obrpc::ObRpcResultCode &rcode)
{
  int ret = OB_SUCCESS;
  ObString src;
  ObString tmp_str;
  ObSqlString dst;
  int64_t pos = 0;  
  char *tmp_buf = nullptr;
  int64_t alloc_size = 0;
  const int64_t serialize_size = rcode.get_serialize_size();
  // to check validity of ObRpcResultCode.
  ObWarningBuffer warning_buffer;
  if (warning_buffer.get_buffer_size() < rcode.warnings_.count()
    || common::OB_MAX_ERROR_MSG_LEN < strlen(rcode.msg_)) {
    LOG_WARN("invalid arg", K(ret), K(rcode));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rcode.warnings_.count(); i++) {
      if (warning_buffer.get_max_warn_len() < strlen(rcode.warnings_.at(i).msg_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arg", K(ret), K(rcode.warnings_.at(i)));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator_.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(serialize_size));
  } else if (OB_FAIL(rcode.serialize(tmp_buf, serialize_size, pos))) {
    LOG_WARN("serialize failed", K(ret));
  } else if (OB_FALSE_IT(src.assign(tmp_buf, serialize_size))) {
  } else if (OB_FAIL(ObDDLTaskRecordOperator::to_hex_str(src, dst))) {
    LOG_WARN("to hex str failed", K(ret));
  } else if (OB_FALSE_IT(tmp_str.assign(dst.ptr(), dst.length()))) {
    LOG_WARN("assign string failed", K(ret));
  } else if (OB_UNLIKELY((alloc_size = dst.length() + 1) < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(alloc_size), K(dst.length()), K(dst.ptr()));
  } else if (OB_FAIL(ob_write_string(allocator_, tmp_str, forward_user_message_, true /*cstyle, end with '\0'*/))) {
    LOG_WARN("ob write string failed", K(ret));
  }
  return ret;
}

int ObDDLRetryTask::check_schema_change_done()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_schema_change_done_) {
    // do nothing.
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, RETRY_TASK_CHECK_SCHEMA_CHANGED_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else {
    common::ObMySQLProxy &proxy = root_service_->get_sql_proxy();
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString query_string;
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_UNLIKELY(!proxy.is_inited())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, proxy is not inited", K(ret));
      } else if (OB_FAIL(query_string.assign_fmt(
          " SELECT status FROM %s WHERE task_id = %lu",
          OB_ALL_DDL_TASK_STATUS_TNAME, task_id_))) {
        LOG_WARN("assign query string failed", K(ret), KPC(this));
      } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, RETRY_TASK_CHECK_SCHEMA_CHANGED_SLOW))) {
        LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
      } else if (OB_FAIL(proxy.read(res, tenant_id_, query_string.ptr()))) {
        LOG_WARN("read record failed", K(ret), K(query_string));
      } else if (OB_UNLIKELY(nullptr == (result = res.get_result()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("get record failed", K(ret), K(query_string));
      } else {
        int64_t table_task_status = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "status", table_task_status, int64_t);
        is_schema_change_done_ = ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH == table_task_status ? true : is_schema_change_done_;
      }
    }
  }
  return ret;
}

int ObDDLRetryTask::drop_schema(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  ObDDLTaskStatus new_status = DROP_SCHEMA;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRetryTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_ISNULL(ddl_arg_) || lib::Worker::CompatMode::INVALID == compat_mode_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KP(ddl_arg_), K(compat_mode_));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, RETRY_TASK_DROP_SCHEMA_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(check_schema_change_done())) {
    LOG_WARN("check task finished failed", K(ret));
  } else if (is_schema_change_done_) {
    // schema has already changed.
    new_status = next_task_status;
  } else {
    lib::Worker::CompatMode save_compat_mode = THIS_WORKER.get_compatibility_mode();
    THIS_WORKER.set_compatibility_mode(compat_mode_);
    obrpc::ObCommonRpcProxy common_rpc_proxy = root_service_->get_common_rpc_proxy().to(GCTX.self_addr()).timeout(GCONF._ob_ddl_timeout);
    switch(task_type_) {
      case ObDDLType::DDL_DROP_DATABASE: {
        int64_t timeout_us = 0;
        obrpc::ObDropDatabaseRes drop_database_res;
        obrpc::ObDropDatabaseArg *arg = static_cast<obrpc::ObDropDatabaseArg *>(ddl_arg_);
        arg->is_add_to_scheduler_ = false;
        arg->task_id_ = task_id_;
        ObDDLUtil::get_ddl_rpc_timeout_for_database(tenant_id_, object_id_, timeout_us);
        if (OB_FAIL(common_rpc_proxy.timeout(timeout_us).drop_database(*arg, drop_database_res))) {
          LOG_WARN("fail to drop database", K(ret));
        } else {
          affected_rows_ = drop_database_res.affected_row_;
        }
        break;
      }
      case ObDDLType::DDL_DROP_TABLE: {
        obrpc::ObDDLRes res;
        obrpc::ObDropTableArg *arg = static_cast<obrpc::ObDropTableArg *>(ddl_arg_);
        arg->is_add_to_scheduler_ = false;
        arg->task_id_ = task_id_;
        if (OB_FAIL(common_rpc_proxy.drop_table(*arg, res))) {
          LOG_WARN("fail to drop table", K(ret));
        }
        break;
      }
      case ObDDLType::DDL_TRUNCATE_TABLE: {
        obrpc::ObDDLRes res;
        obrpc::ObTruncateTableArg *arg = static_cast<obrpc::ObTruncateTableArg *>(ddl_arg_);
        arg->is_add_to_scheduler_ = false;
        arg->task_id_ = task_id_;
        if (OB_FAIL(common_rpc_proxy.truncate_table(*arg, res))) {
          LOG_WARN("fail to truncate table", K(ret));
        }
        break;
      }
      case ObDDLType::DDL_DROP_PARTITION:
      case ObDDLType::DDL_DROP_SUB_PARTITION:
      case ObDDLType::DDL_RENAME_PARTITION:
      case ObDDLType::DDL_RENAME_SUB_PARTITION:
      case ObDDLType::DDL_TRUNCATE_PARTITION:
      case ObDDLType::DDL_TRUNCATE_SUB_PARTITION: {
        obrpc::ObAlterTableArg *arg = static_cast<obrpc::ObAlterTableArg *>(ddl_arg_);
        arg->is_add_to_scheduler_ = false;
        arg->task_id_ = task_id_;
        if (OB_FAIL(common_rpc_proxy.alter_table(*arg, alter_table_res_))) {
          LOG_WARN("fail to alter table", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ddl type", K(ret), K(task_type_));
        break;
      }
    }
    THIS_WORKER.set_compatibility_mode(save_compat_mode);
    if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
      ret = OB_SUCCESS;
    } else {
      if (OB_SUCC(ret)) {
        new_status = next_task_status;
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = get_forward_user_message(common_rpc_proxy.get_result_code()))) {
        LOG_WARN("get forward user message failed", K(ret), K(tmp_ret), K(forward_user_message_), K(common_rpc_proxy.get_result_code()));
      }
    }
  }
  if (new_status == next_task_status || OB_FAIL(ret)) {
    if (OB_FAIL(switch_status(new_status, true, ret))) {
      LOG_WARN("fail to switch task status", K(ret));
    }
  }
  return ret;
}

int ObDDLRetryTask::wait_alter_table(const ObDDLTaskStatus new_status)
{
  int ret = OB_SUCCESS;
  bool finish = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRetryTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_ISNULL(ddl_arg_) || lib::Worker::CompatMode::INVALID == compat_mode_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KP(ddl_arg_), K(compat_mode_));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, RETRY_TASK_WAIT_ALTER_TABLE_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else {
    switch (task_type_) {
    case ObDDLType::DDL_DROP_DATABASE:
    case ObDDLType::DDL_DROP_TABLE:
    case ObDDLType::DDL_TRUNCATE_TABLE: {
      finish = true;
      break;
    }
    case ObDDLType::DDL_DROP_PARTITION:
    case ObDDLType::DDL_DROP_SUB_PARTITION:
    case ObDDLType::DDL_RENAME_PARTITION:
    case ObDDLType::DDL_RENAME_SUB_PARTITION:
    case ObDDLType::DDL_TRUNCATE_PARTITION:
    case ObDDLType::DDL_TRUNCATE_SUB_PARTITION: {
      obrpc::ObAlterTableArg *arg = static_cast<obrpc::ObAlterTableArg *>(ddl_arg_);
      const uint64_t tenant_id = arg->exec_tenant_id_;
      common::ObSArray<ObDDLRes> &res_array = alter_table_res_.ddl_res_array_;
      while (OB_SUCC(ret) && res_array.count() > 0) {
        const int64_t task_id = res_array.at(res_array.count() - 1).task_id_;
        bool is_finish = false;
        if (OB_FAIL(sql::ObDDLExecutorUtil::wait_build_index_finish(tenant_id, task_id, is_finish))) {
          LOG_WARN("wait build index finish failed", K(ret), K(tenant_id), K(task_id));
        } else if (is_finish) {
          res_array.pop_back();
          LOG_INFO("index status is final", K(ret), K(task_id));
        } else {
          LOG_INFO("index status is not final", K(task_id));
          break;
        }
      }
      if (OB_SUCC(ret) && res_array.count() == 0) {
        finish = true;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ddl type", K(ret), K(task_type_));
      break;
    }
    }
  }

  if (OB_FAIL(ret) || finish) {
    if (OB_FAIL(switch_status(new_status, true, ret))) {
      LOG_WARN("fail to switch task status", K(ret));
    }
  }
  return ret;
}

int ObDDLRetryTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is nullptr", K(ret));
  } else if (OB_FAIL(report_error_code(forward_user_message_, affected_rows_))) {
    LOG_WARN("fail to report error code", K(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(root_service_->get_sql_proxy(), tenant_id_, task_id_))) {
    LOG_WARN("fail to delete record", K(ret), K(task_id_));
  } else {
    need_retry_ = false;
  }
  LOG_INFO("clean task finished", K(ret), K(*this));
  return ret;
}

int ObDDLRetryTask::succ()
{
  return cleanup();
}

int ObDDLRetryTask::fail()
{
  return cleanup();
}

int ObDDLRetryTask::check_health()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret));
  } else if (!root_service->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("RS not in service, do not retry", K(ret));
    need_retry_ = false;
  } else if (OB_FAIL(refresh_status())) {
    LOG_WARN("refresh status failed", K(ret));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", K(ret));
  }
  return ret;
}

int ObDDLRetryTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRetryTask has not been inited", K(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("check health failed", K(ret));
  } else if (!need_retry()) {
    // task finish, do nothing.
  } else {
    ddl_tracing_.restore_span_hierarchy();
    switch(task_status_) {
      case ObDDLTaskStatus::PREPARE: {
        if (OB_FAIL(prepare(ObDDLTaskStatus::DROP_SCHEMA))) {
          LOG_WARN("fail to prepare table redefinition task", K(ret));
        }
        break;
      }
      case ObDDLTaskStatus::DROP_SCHEMA: {
        if (OB_FAIL(drop_schema(ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH))) {
          LOG_WARN("fail to drop schema", K(ret));
        }
        break;
      }
      case ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH: {
        if (OB_FAIL(wait_alter_table(ObDDLTaskStatus::SUCCESS))) {
          LOG_WARN("failed to wait child task", K(ret));
        }
        break;
      }
      case ObDDLTaskStatus::FAIL: {
        if (OB_FAIL(fail())) {
          LOG_WARN("fail to do clean up", K(ret));
        }
        break;
      }
      case ObDDLTaskStatus::SUCCESS: {
        if (OB_FAIL(succ())) {
          LOG_WARN("fail to success", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table redefinition task state", K(task_status_));
        break;
      }
    }
    ddl_tracing_.release_span_hierarchy();
    if (OB_FAIL(ret)) {
      add_event_info("ddl retry task process fail");
      LOG_INFO("ddl retry task process fail", K(ret), K(snapshot_version_), K(object_id_), K(target_object_id_), K(schema_version_), "ddl_event_info", ObDDLEventInfo());
    }
  }
  return ret;
}

bool ObDDLRetryTask::is_valid() const
{
  return is_inited_ && !trace_id_.is_invalid();
}

int ObDDLRetryTask::serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_size, pos))) {
    LOG_WARN("ObDDLTask serialize failed", K(ret));
  } else if (OB_FAIL(ddl_arg_->serialize(buf, buf_size, pos))) {
    LOG_WARN("serialize table arg failed", K(ret));
  }
  return ret;
}

int ObDDLRetryTask::deserialize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize ObDDLTask", K(ret));
  } else if (ObDDLType::DDL_DROP_DATABASE == task_type_) {
    obrpc::ObDropDatabaseArg tmp_arg;
    if (OB_FAIL(tmp_arg.deserialize(buf, buf_size, pos))) {
      LOG_WARN("serialize table failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(tenant_id, tmp_arg))) {
      LOG_WARN("replace user tenant id failed", K(ret), K(tenant_id), K(tmp_arg));
    } else if (OB_FAIL(deep_copy_ddl_arg(allocator_, task_type_, &tmp_arg))) {
      LOG_WARN("deep copy table arg failed", K(ret));
    }
  } else if (ObDDLType::DDL_DROP_TABLE == task_type_) {
    obrpc::ObDropTableArg tmp_arg;
    if (OB_FAIL(tmp_arg.deserialize(buf, buf_size, pos))) {
      LOG_WARN("serialize table failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(tenant_id, tmp_arg))) {
      LOG_WARN("replace user tenant id failed", K(ret), K(tenant_id), K(tmp_arg));
    } else if (OB_FAIL(deep_copy_ddl_arg(allocator_, task_type_, &tmp_arg))) {
      LOG_WARN("deep copy table arg failed", K(ret));
    }
  } else if (ObDDLType::DDL_TRUNCATE_TABLE == task_type_) {
    obrpc::ObTruncateTableArg tmp_arg;
    if (OB_FAIL(tmp_arg.deserialize(buf, buf_size, pos))) {
      LOG_WARN("serialize table failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(tenant_id, tmp_arg))) {
      LOG_WARN("replace user tenant id failed", K(ret), K(tenant_id), K(tmp_arg));
    } else if (OB_FAIL(deep_copy_ddl_arg(allocator_, task_type_, &tmp_arg))) {
      LOG_WARN("deep copy table arg failed", K(ret));
    }
  } else if (ObDDLType::DDL_DROP_PARTITION == task_type_
          || ObDDLType::DDL_DROP_SUB_PARTITION == task_type_
          || ObDDLType::DDL_TRUNCATE_PARTITION == task_type_
          || ObDDLType::DDL_TRUNCATE_SUB_PARTITION == task_type_
          || ObDDLType::DDL_RENAME_PARTITION == task_type_
          || ObDDLType::DDL_RENAME_SUB_PARTITION == task_type_) {
    obrpc::ObAlterTableArg tmp_arg;
    if (OB_FAIL(tmp_arg.deserialize(buf, buf_size, pos))) {
      LOG_WARN("serialize table failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(task_type_, tenant_id, tmp_arg))) {
      LOG_WARN("replace user tenant id failed", K(ret), K(tenant_id), K(tmp_arg));
    } else if (OB_FAIL(deep_copy_ddl_arg(allocator_, task_type_, &tmp_arg))) {
      LOG_WARN("deep copy table arg failed", K(ret));
    }
  }
  return ret;
}

int64_t ObDDLRetryTask::get_serialize_param_size() const
{
  int64_t serialize_param_size = ObDDLTask::get_serialize_param_size();
  if (OB_NOT_NULL(ddl_arg_)) {
    serialize_param_size += ddl_arg_->get_serialize_size();
  }
  return serialize_param_size;
}

int ObDDLRetryTask::update_task_status_wait_child_task_finish(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const int64_t task_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql_string;
  int64_t curr_task_status = 0;
  int64_t execution_id = -1; /*unused*/
  int64_t ret_code = 0;
  const int64_t new_task_status = ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(DDL_SIM(tenant_id, task_id, RETRY_TASK_UPDATE_BY_CHILD_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(trans, tenant_id, task_id, curr_task_status, execution_id, ret_code))) {
    LOG_WARN("select for update failed", K(ret), K(tenant_id), K(task_id));
  } else if (OB_UNLIKELY(ObDDLTaskStatus::DROP_SCHEMA != curr_task_status)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status updated", K(ret), K(task_id), K(curr_task_status));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::update_task_status(trans, tenant_id, task_id, new_task_status))) {
    LOG_WARN("update task status failed", K(ret));
  } else {
    LOG_INFO("update task status to wait child task finish", K(ret));
  }
  return ret;
}

void ObDDLRetryTask::flt_set_task_span_tag() const
{
  FLT_SET_TAG(ddl_task_id, task_id_, ddl_parent_task_id, parent_task_id_, ddl_data_table_id, object_id_);
}

void ObDDLRetryTask::flt_set_status_span_tag() const
{
  switch (task_status_) {
  case ObDDLTaskStatus::PREPARE: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::DROP_SCHEMA: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::FAIL: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::SUCCESS: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  default: {
    break;
  }
  }
}
