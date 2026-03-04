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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_redef_table.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "storage/ddl/ob_ddl_server_client.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace sql;
using namespace obrpc;
namespace observer
{

int ObTableLoadRedefTable::create_hidden_table(const ObTableLoadRedefTableStartArg &arg,
                                               ObTableLoadRedefTableStartRes &res,
                                               ObSQLSessionInfo &session_info,
                                               ObCreateHiddenTableRes &create_table_res,
                                               int64_t &snapshot_version)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  uint64_t compat_version = GET_MIN_CLUSTER_VERSION();
  const share::ObDDLType ddl_type = arg.is_load_data_ ? share::DDL_DIRECT_LOAD : share::DDL_DIRECT_LOAD_INSERT;
  snapshot_version = OB_INVALID_VERSION;
  if (compat_version < CLUSTER_VERSION_4_2_5_5) {
    ObCreateHiddenTableArg create_table_arg;
    const bool need_reorder_column_id = false;
    if (OB_FAIL(create_table_arg.init(tenant_id, tenant_id, tenant_id, arg.table_id_,
                                    THIS_WORKER.get_group_id(), session_info.get_sessid_for_table(),
                                    arg.parallelism_, ddl_type, session_info.get_sql_mode(),
                                    session_info.get_tz_info_wrap().get_tz_info_offset(),
                                    session_info.get_local_nls_date_format(),
                                    session_info.get_local_nls_timestamp_format(),
                                    session_info.get_local_nls_timestamp_tz_format(),
                                    session_info.get_tz_info_wrap(),
                                    need_reorder_column_id))) {
      LOG_WARN("fail to init create hidden table arg", KR(ret));
    } else if (OB_FAIL(ObDDLServerClient::create_hidden_table(create_table_arg, create_table_res,
                       snapshot_version, session_info))) {
      LOG_WARN("failed to create hidden table", KR(ret), K(create_table_arg));
    } else if (OB_UNLIKELY(snapshot_version <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid snapshot version", KR(ret));
    }
  } else {
    ObCreateHiddenTableArgV2 create_table_argv2;
    if (OB_FAIL(create_table_argv2.init(tenant_id, tenant_id, tenant_id, arg.table_id_,
                                    THIS_WORKER.get_group_id(), session_info.get_sessid_for_table(),
                                    arg.parallelism_, ddl_type, session_info.get_sql_mode(),
                                    session_info.get_tz_info_wrap().get_tz_info_offset(),
                                    session_info.get_local_nls_date_format(),
                                    session_info.get_local_nls_timestamp_format(),
                                    session_info.get_local_nls_timestamp_tz_format(),
                                    session_info.get_tz_info_wrap()))) {
      LOG_WARN("fail to init create hidden table arg", KR(ret));
    } else if (OB_FAIL(ObDDLServerClient::create_hidden_table(create_table_argv2, create_table_res,
                snapshot_version, session_info))) {
      LOG_WARN("failed to create hidden table", KR(ret), K(create_table_argv2));
    } else if (OB_UNLIKELY(snapshot_version <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid snapshot version", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadRedefTable::check_table_consistency(const uint64_t tenant_id,
                                                   const uint64_t table_id,
                                                   const uint64_t dest_table_id,
                                                   const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id ||
                  OB_INVALID_ID == dest_table_id || OB_INVALID_VERSION == schema_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid agrs", KR(ret), K(tenant_id), K(table_id), K(dest_table_id),
             K(schema_version));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    const ObTableSchema *dest_table_schema = nullptr;
    if (OB_FAIL(ObTableLoadSchema::get_schema_guard(tenant_id, schema_guard, schema_version))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id), K(schema_version));
    } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(schema_guard, tenant_id, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(schema_guard, tenant_id, dest_table_id, dest_table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(dest_table_id));
    } else {
      ObArray<ObColDesc> column_descs;
      ObArray<ObColDesc> dest_column_descs;
      if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
        LOG_WARN("fail to get column ids", KR(ret), KPC(table_schema));
      } else if (OB_FAIL(dest_table_schema->get_column_ids(dest_column_descs))) {
        LOG_WARN("fail to get column ids", KR(ret), KPC(dest_table_schema));
      } else if (OB_UNLIKELY(column_descs.count() != dest_column_descs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column desc count not match", KR(ret), K(column_descs),
                 K(dest_column_descs));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
        const ObColDesc &col_desc = column_descs.at(i);
        const ObColDesc &dest_col_desc = dest_column_descs.at(i);
        if (OB_UNLIKELY(col_desc.col_id_ != dest_col_desc.col_id_ ||
                        col_desc.col_type_ != dest_col_desc.col_type_ ||
                        col_desc.col_order_ != dest_col_desc.col_order_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column desc not match", KR(ret), K(i), K(column_descs),
                   K(dest_column_descs));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadRedefTable::start(const ObTableLoadRedefTableStartArg &arg,
                                 ObTableLoadRedefTableStartRes &res, ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg));
  } else {
    const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
    DEBUG_SYNC(BEFORE_CREATE_HIDDEN_TABLE_IN_LOAD);
    ObCreateHiddenTableRes create_table_res;
    int64_t snapshot_version = OB_INVALID_VERSION;
    if (OB_FAIL(create_hidden_table(arg, res, session_info, create_table_res, snapshot_version))) {
      LOG_WARN("failed to create hidden table", KR(ret), K(arg));
    } else {
      res.dest_table_id_ = create_table_res.dest_table_id_;
      res.task_id_ = create_table_res.task_id_;
      res.schema_version_ = create_table_res.schema_version_;
      res.snapshot_version_ = snapshot_version;
      LOG_INFO("succeed to create hidden table", K(arg), K(res));
    }
    THIS_WORKER.set_timeout_ts(origin_timeout_ts);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_table_consistency(arg.tenant_id_, arg.table_id_, res.dest_table_id_, res.schema_version_))) {
        LOG_WARN("fail to check table consistenc", KR(ret), K(arg), K(res));
      }
    }
  }
  return ret;
}

int ObTableLoadRedefTable::finish(const ObTableLoadRedefTableFinishArg &arg,
                                  ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg));
  } else {
    const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
    ObCopyTableDependentsArg copy_table_dependents_arg;
    copy_table_dependents_arg.task_id_ = arg.task_id_;
    copy_table_dependents_arg.tenant_id_ = arg.tenant_id_;
    copy_table_dependents_arg.copy_indexes_ = true;
    copy_table_dependents_arg.copy_constraints_ = true;
    copy_table_dependents_arg.copy_triggers_ = false;
    copy_table_dependents_arg.ignore_errors_ = false;
    if (OB_FAIL(ObDDLServerClient::copy_table_dependents(copy_table_dependents_arg, session_info))) {
      LOG_WARN("failed to copy table dependents", KR(ret), K(copy_table_dependents_arg));
    } else {
      LOG_INFO("succeed to copy table dependents", K(copy_table_dependents_arg));
      ObFinishRedefTableArg finish_redef_table_arg;
      finish_redef_table_arg.task_id_ = arg.task_id_;
      finish_redef_table_arg.tenant_id_ = arg.tenant_id_;

      ObDDLBuildSingleReplicaResponseArg build_single_replica_response_arg;
      build_single_replica_response_arg.task_id_             = arg.task_id_;
      build_single_replica_response_arg.tenant_id_           = arg.tenant_id_;
      build_single_replica_response_arg.dest_tenant_id_      = arg.tenant_id_;
      build_single_replica_response_arg.source_table_id_     = arg.table_id_;
      build_single_replica_response_arg.dest_schema_id_      = arg.dest_table_id_;
      build_single_replica_response_arg.schema_version_      = arg.schema_version_;
      build_single_replica_response_arg.dest_schema_version_ = arg.schema_version_;
      build_single_replica_response_arg.ls_id_               = share::ObLSID(1);
      build_single_replica_response_arg.dest_ls_id_          = share::ObLSID(1);
      build_single_replica_response_arg.tablet_id_           = ObTableID(-1);
      build_single_replica_response_arg.snapshot_version_    = 1;
      build_single_replica_response_arg.execution_id_        = 1;
      build_single_replica_response_arg.ret_code_            = ret;
      if (OB_FAIL(ObDDLServerClient::finish_redef_table(
            finish_redef_table_arg, build_single_replica_response_arg, session_info))) {
        LOG_WARN("failed to finish redef table", KR(ret), K(finish_redef_table_arg));
        if (ret == OB_NOT_MASTER) { //sql cannot be retried here, so change errcode
          ret = OB_DIRECT_LOAD_COMMIT_ERROR;
        }
      } else {
        LOG_INFO("succeed to finish redef table", KR(ret), K(finish_redef_table_arg));
      }
    }
    THIS_WORKER.set_timeout_ts(origin_timeout_ts);
  }
  return ret;
}

int ObTableLoadRedefTable::abort(const ObTableLoadRedefTableAbortArg &arg,
                                 ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg));
  } else {
    const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
    ObTableLoadErrorMessageGuard guard;
    ObAbortRedefTableArg abort_redef_table_arg;
    abort_redef_table_arg.task_id_ = arg.task_id_;
    abort_redef_table_arg.tenant_id_ = arg.tenant_id_;
    if (OB_FAIL(ObDDLServerClient::abort_redef_table(abort_redef_table_arg, &session_info))) {
      LOG_WARN("failed to abort redef table", KR(ret), K(abort_redef_table_arg));
    } else {
      LOG_INFO("succeed to abort hidden table", K(arg));
    }
    THIS_WORKER.set_timeout_ts(origin_timeout_ts);
  }
  return ret;
}

ObTableLoadErrorMessageGuard::ObTableLoadErrorMessageGuard()
  : allocator_("TLD_ErrMessage"),
    error_code_(OB_SUCCESS),
    error_message_(nullptr),
    need_reset_(false)
{
  int ret = OB_SUCCESS;
  ObWarningBuffer *buf = ob_get_tsi_warning_buffer();
  if (nullptr != buf && buf->get_err_code() != OB_MAX_ERROR_CODE) {
    if (OB_ISNULL(error_message_ = static_cast<char *>(allocator_.alloc(ObWarningBuffer::WarningItem::STR_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      error_code_ = buf->get_err_code();
      MEMCPY(error_message_, buf->get_err_msg(), ObWarningBuffer::WarningItem::STR_LEN);
      need_reset_ = true;
    }
  }
}

ObTableLoadErrorMessageGuard::~ObTableLoadErrorMessageGuard()
{
  if (need_reset_) {
    FORWARD_USER_ERROR(error_code_, error_message_);
  }
}

}  // namespace observer
}  // namespace oceanbase
