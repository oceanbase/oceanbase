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
#include "ob_table_redefinition_task.h"
#include "lib/rc/context.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_ddl_checksum.h" 
#include "share/ob_ddl_sim_point.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ddl_task/ob_ddl_redefinition_task.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;

ObTableRedefinitionTask::ObTableRedefinitionTask()
  : ObDDLRedefinitionTask(ObDDLType::DDL_TABLE_REDEFINITION),
    has_rebuild_index_(false), has_rebuild_constraint_(false), has_rebuild_foreign_key_(false),
    allocator_(lib::ObLabel("RedefTask")),
    is_copy_indexes_(true), is_copy_triggers_(true), is_copy_constraints_(true), is_copy_foreign_keys_(true),
    is_ignore_errors_(false), is_do_finish_(false), target_cg_cnt_(0), use_heap_table_ddl_plan_(false),
    is_ddl_retryable_(true)
{
}

ObTableRedefinitionTask::~ObTableRedefinitionTask()
{
}

int ObTableRedefinitionTask::init(const ObTableSchema* src_table_schema,
                                  const ObTableSchema* dst_table_schema,
                                  const int64_t parent_task_id,
                                  const int64_t task_id,
                                  const share::ObDDLType &ddl_type,
                                  const int64_t parallelism,
                                  const int64_t consumer_group_id,
                                  const int32_t sub_task_trace_id,
                                  const ObAlterTableArg &alter_table_arg,
                                  const uint64_t tenant_data_version,
                                  const bool ddl_need_retry_at_executor,
                                  const int64_t task_status,
                                  const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableRedefinitionTask has already been inited", K(ret));
  } else if (OB_ISNULL(src_table_schema) || OB_ISNULL(dst_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src_table_schema), KP(dst_table_schema));
  } else if (OB_UNLIKELY( !src_table_schema->is_valid()
                        || !dst_table_schema->is_valid()
                        || task_id <= 0  || snapshot_version < 0 || tenant_data_version <= 0
                        || task_status < ObDDLTaskStatus::PREPARE || task_status > ObDDLTaskStatus::SUCCESS
                        || (snapshot_version > 0 && task_status < ObDDLTaskStatus::WAIT_TRANS_END))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(src_table_schema), KPC(dst_table_schema), K(task_id),
                                  K(task_status), K(snapshot_version));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, alter_table_arg, alter_table_arg_))) {
    LOG_WARN("deep copy alter table arg failed", K(ret));
  } else if (OB_FAIL(set_ddl_stmt_str(alter_table_arg_.ddl_stmt_str_))) {
    LOG_WARN("set ddl stmt str failed", K(ret));
  } else {
    set_gmt_create(ObTimeUtility::current_time());
    consumer_group_id_ = consumer_group_id;
    sub_task_trace_id_ = sub_task_trace_id;
    task_type_ = ddl_type;
    object_id_ = src_table_schema->get_table_id();
    target_object_id_ = dst_table_schema->get_table_id();

    /* only table restore set schema_serson = src, other use dst*/
    if (ObDDLType::DDL_TABLE_RESTORE == ddl_type) {
      schema_version_ = src_table_schema->get_schema_version();
    } else {
      schema_version_ = dst_table_schema->get_schema_version();
    }

    task_status_ = static_cast<ObDDLTaskStatus>(task_status);
    snapshot_version_ = snapshot_version;
    tenant_id_ = src_table_schema->get_tenant_id();
    task_version_ = OB_TABLE_REDEFINITION_TASK_VERSION;
    parent_task_id_ = parent_task_id;
    task_id_ = task_id;
    parallelism_ = parallelism;
    data_format_version_ = tenant_data_version;
    start_time_ = ObTimeUtility::current_time();
    // For common offline ddl, dest_tenant_id is also the tenant_id_, i.e., tenant id of the data table.
    // But for DDL_RESTORE_TABLE, dst_tenant_id_ is different to the tenant_id_.
    dst_tenant_id_ = dst_table_schema->get_tenant_id();
    dst_schema_version_ = dst_table_schema->get_schema_version();
    alter_table_arg_.alter_table_schema_.set_tenant_id(tenant_id_);
    alter_table_arg_.alter_table_schema_.set_schema_version(schema_version_);
    alter_table_arg_.exec_tenant_id_ = dst_tenant_id_;
    if (OB_FAIL(dst_table_schema->get_store_column_group_count(target_cg_cnt_))) {
      LOG_WARN("fail to get target cg cnt", K(ret), KPC(dst_table_schema));
    } else if (OB_FAIL(init_ddl_task_monitor_info(target_object_id_))) {
      LOG_WARN("init ddl task monitor info failed", K(ret));
    } else if (OB_FAIL(check_ddl_can_retry(ddl_need_retry_at_executor, dst_table_schema))) {
      LOG_WARN("check use heap table ddl plan failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_no_logging_param(tenant_id_, is_no_logging_))) {
      LOG_WARN("fail to get no logging param", K(ret), K(tenant_id_));
    } else {
      is_inited_ = true;
      ddl_tracing_.open();
    }
  }

  LOG_INFO("init table redefinition task finished", K(ret), KPC(this));
  return ret;
}

int ObTableRedefinitionTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  uint64_t src_tenant_id = common::OB_INVALID_ID;
  uint64_t dst_tenant_id = common::OB_INVALID_ID;
  int64_t src_schema_version = 0;
  int64_t dst_schema_version = 0;
  const uint64_t data_table_id = task_record.object_id_;
  const uint64_t dest_table_id = task_record.target_object_id_;
  task_type_ = task_record.ddl_type_; // put here to decide whether to replace user tenant id.
  int64_t pos = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableRedefinitionTask has already been inited", K(ret));
  } else if (!task_record.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(), task_record.message_.length(), pos))) {
    LOG_WARN("deserialize params from message failed", K(ret), K(task_record.message_), K(common::lbt()));
  } else if (OB_FAIL(set_ddl_stmt_str(task_record.ddl_stmt_str_))) {
    LOG_WARN("set ddl stmt str failed", K(ret));
  } else if (FALSE_IT(src_tenant_id = alter_table_arg_.alter_table_schema_.get_tenant_id())) {
  } else if (FALSE_IT(dst_tenant_id = task_record.tenant_id_)) {
  } else if (FALSE_IT(src_schema_version = alter_table_arg_.alter_table_schema_.get_schema_version())) {
  } else if (FALSE_IT(dst_schema_version = task_record.schema_version_)) {
  } else if (OB_UNLIKELY(common::OB_INVALID_ID == src_tenant_id
                      || common::OB_INVALID_ID == dst_tenant_id
                      || src_schema_version <= 0
                      || dst_schema_version <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(task_record), K(src_tenant_id), K(dst_tenant_id), K(src_schema_version), K(dst_schema_version));
  } else if (OB_UNLIKELY(ObDDLType::DDL_TABLE_RESTORE != task_record.ddl_type_
                      && (src_tenant_id != dst_tenant_id || src_schema_version != dst_schema_version))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(task_record), K(src_tenant_id), K(dst_tenant_id), K(src_schema_version), K(dst_schema_version));
  } else if (OB_UNLIKELY(ObDDLType::DDL_TABLE_RESTORE == task_record.ddl_type_
                      && (src_tenant_id == dst_tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(task_record), K(src_tenant_id), K(dst_tenant_id), K(src_schema_version), K(dst_schema_version));
  } else {
    parent_task_id_ = task_record.parent_task_id_;
    task_id_ = task_record.task_id_;
    object_id_ = data_table_id;
    target_object_id_ = dest_table_id;
    schema_version_ = src_schema_version;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    snapshot_version_ = task_record.snapshot_version_;
    execution_id_ = task_record.execution_id_;
    tenant_id_ = src_tenant_id;
    ret_code_ = task_record.ret_code_;
    start_time_ = ObTimeUtility::current_time();
    dst_tenant_id_ = dst_tenant_id;
    dst_schema_version_ = dst_schema_version;
    if (OB_FAIL(init_ddl_task_monitor_info(target_object_id_))) {
      LOG_WARN("init ddl task monitor info failed", K(ret));
    } else {
      is_inited_ = true;

      // set up span during recover task
      ddl_tracing_.open_for_recovery();
    }
  }

  LOG_INFO("init table redefinition task finished", K(ret), KPC(this));
  return ret;
}

int ObTableRedefinitionTask::update_complete_sstable_job_status(const common::ObTabletID &tablet_id,
                                                                const ObAddr &addr,
                                                                const int64_t snapshot_version,
                                                                const int64_t execution_id,
                                                                const int ret_code,
                                                                const ObDDLTaskInfo &addition_info)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);
  UNUSED(tablet_id);
  UNUSED(addition_info);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, UPDATE_COMPLETE_SSTABLE_FAILED))) {
    LOG_WARN("ddl sim failure", K(tenant_id_), K(task_id_));
  } else if (ObDDLTaskStatus::CHECK_TABLE_EMPTY == task_status_) {
    check_table_empty_job_ret_code_ = ret_code;
  } else {
    switch(task_type_) {
      case ObDDLType::DDL_DIRECT_LOAD:
      case ObDDLType::DDL_DIRECT_LOAD_INSERT: {
        complete_sstable_job_ret_code_ = ret_code;
        ret_code_ = ret_code;
        LOG_INFO("table redefinition task callback", K(addr), K(complete_sstable_job_ret_code_));
        break;
      }
      default : {
        if (OB_UNLIKELY(snapshot_version_ != snapshot_version)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, snapshot version is not equal", K(addr), K(ret), K(snapshot_version_), K(snapshot_version));
        } else if (execution_id < execution_id_) {
          ret = OB_TASK_EXPIRED;
          LOG_WARN("receive a mismatch execution result, ignore", K(addr), K(ret_code), K(execution_id), K(execution_id_));
        } else {
          complete_sstable_job_ret_code_ = ret_code;
          execution_id_ = execution_id; // update ObTableRedefinitionTask::execution_id_ from ObDDLRedefinitionSSTableBuildTask::execution_id_
          LOG_INFO("table redefinition task callback", K(addr), K(complete_sstable_job_ret_code_), K(execution_id_));
        }
        break;
      }
    }
  }
  return ret;
}

int ObTableRedefinitionTask::send_build_replica_request()
{
  int ret = OB_SUCCESS;
  switch (task_type_) {
    case DDL_DIRECT_LOAD:
    case DDL_DIRECT_LOAD_INSERT: {
      // do nothing
      break;
    }
    default: {
      if (OB_FAIL(send_build_replica_request_by_sql())) {
        LOG_WARN("failed to send build replica request", K(ret));
      }
      break;
    }
  }
  return ret;
}

int ObTableRedefinitionTask::send_build_replica_request_by_sql()
{
  int ret = OB_SUCCESS;
  bool modify_autoinc = false;
  ObRootService *root_service = GCTX.root_service_;
  int64_t new_execution_id = 0;
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_SEND_BUILD_REPLICA_REQUEST_FAILED))) {
    LOG_WARN("ddl sim failure", K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(check_modify_autoinc(modify_autoinc))) {
    LOG_WARN("failed to check modify autoinc", K(ret));
  } else if (OB_FAIL(ObDDLTask::push_execution_id(tenant_id_, task_id_, task_type_, is_ddl_retryable_, data_format_version_, new_execution_id))) {
    LOG_WARN("failed to fetch new execution id", K(ret));
  } else {
    ObSQLMode sql_mode = alter_table_arg_.sql_mode_;
    if (!modify_autoinc) {
      sql_mode = sql_mode | SMO_NO_AUTO_VALUE_ON_ZERO;
    }
    // get execute inner sql addr
    if (OB_FAIL(ObDDLUtil::get_sys_ls_leader_addr(GCONF.cluster_id, tenant_id_, alter_table_arg_.inner_sql_exec_addr_))) {
      LOG_WARN("get sys ls leader addr fail", K(ret), K(tenant_id_));
      ret = OB_SUCCESS; // ignore ret
    } else if (OB_FAIL(set_sql_exec_addr(alter_table_arg_.inner_sql_exec_addr_))) {
      LOG_WARN("failed to set sql execute addr", K(ret), K(alter_table_arg_.inner_sql_exec_addr_));
    }
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *orig_table_schema = nullptr;
    const ObTableSchema *hidden_table_schema = nullptr;
    ObDDLRedefinitionSSTableBuildTask task(
        task_id_,
        tenant_id_,
        object_id_,
        target_object_id_,
        schema_version_,
        snapshot_version_,
        new_execution_id,
        consumer_group_id_,
        sql_mode,
        trace_id_,
        parallelism_,
        use_heap_table_ddl_plan_,
        alter_table_arg_.mview_refresh_info_.is_mview_complete_refresh_,
        alter_table_arg_.mview_refresh_info_.mview_table_id_,
        GCTX.root_service_,
        alter_table_arg_.inner_sql_exec_addr_,
        data_format_version_,
        is_ddl_retryable_);
    if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, orig_table_schema))) {
      LOG_WARN("failed to get orig table schema", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(dst_tenant_id_, target_object_id_, hidden_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(target_object_id_));
    } else if (OB_FAIL(task.init(*orig_table_schema, *hidden_table_schema, alter_table_arg_.alter_table_schema_, alter_table_arg_.tz_info_wrap_, alter_table_arg_.based_schema_object_infos_))) {
      LOG_WARN("fail to init table redefinition sstable build task", K(ret));
    } else if (OB_FAIL(root_service->submit_ddl_single_replica_build_task(task))) {
      LOG_WARN("fail to submit ddl build single replica", K(ret));
    }
  }
  return ret;
}

int ObTableRedefinitionTask::check_build_replica_end(bool &is_end)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);
  if (INT64_MAX == complete_sstable_job_ret_code_) {
    // not complete
  } else if (OB_SUCCESS != complete_sstable_job_ret_code_) {
    ret_code_ = complete_sstable_job_ret_code_;
    is_end = true;
    LOG_WARN("complete sstable job failed", K(ret_code_), K(object_id_), K(target_object_id_));
    if (is_replica_build_need_retry(ret_code_) && is_ddl_retryable_) {
      build_replica_request_time_ = 0;
      complete_sstable_job_ret_code_ = INT64_MAX;
      ret_code_ = OB_SUCCESS;
      is_end = false;
      LOG_INFO("ddl need retry", K(*this));
    }
  } else {
    is_end = true;
    ret_code_ = complete_sstable_job_ret_code_;
  }
  return ret;
}

int ObTableRedefinitionTask::check_ddl_can_retry(const bool ddl_need_retry_at_executor, const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  is_ddl_retryable_ = true;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(table_schema));
  } else if (OB_FAIL(check_use_heap_table_ddl_plan(table_schema))) {
    LOG_WARN("check use heap table ddl plan failed", K(ret));
  } else if (ObDDLUtil::is_mview_not_retryable(data_format_version_, task_type_)) {
    is_ddl_retryable_ = false;
  } else {
    if (ObDDLUtil::use_idempotent_mode(data_format_version_)) {
      if (use_heap_table_ddl_plan_) {
        is_ddl_retryable_ = false;
        LOG_INFO("ddl schedule will not retry for heap table", K(use_heap_table_ddl_plan_), K_(task_id));
      } else if (ddl_need_retry_at_executor) {
        is_ddl_retryable_ = false;  // do not retry at ddl scheduler when ddl need retry at executor
        LOG_INFO("ddl schedule will not retry for ddl which will retry at table executor level", K(use_heap_table_ddl_plan_), K_(task_id));
      }
    }
  }
  return ret;
}

int ObTableRedefinitionTask::check_use_heap_table_ddl_plan(const ObTableSchema *target_table_schema)
{
  int ret = OB_SUCCESS;
  use_heap_table_ddl_plan_ = false;
  if (OB_ISNULL(target_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(target_table_schema));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, TABLE_REDEF_TASK_CHECK_USE_HEAP_PLAN_FAILED))) {
    LOG_WARN("ddl sim failure", K(tenant_id_), K(task_id_));
  } else if (target_table_schema->is_heap_table() &&
             (DDL_ALTER_PARTITION_BY == task_type_ || DDL_DROP_PRIMARY_KEY == task_type_ ||
              DDL_MVIEW_COMPLETE_REFRESH == task_type_)) {
    use_heap_table_ddl_plan_ = true;
  }
  return ret;
}

int ObTableRedefinitionTask::table_redefinition(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  bool is_build_replica_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version_ <= 0)) {
    is_build_replica_end = true; // switch to fail.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected snapshot", K(ret), KPC(this));
  }

  if (OB_SUCC(ret) && !is_build_replica_end && 0 == get_build_replica_request_time()) {
    bool need_exec_new_inner_sql = false;
    if (OB_FAIL(reap_old_replica_build_task(need_exec_new_inner_sql))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS; // retry
      } else {
        LOG_WARN("failed to reap old task", K(ret));
      }
    } else if (!need_exec_new_inner_sql) {
      is_build_replica_end = true;
    } else if (OB_FAIL(send_build_replica_request())) {
      if (OB_TASK_EXPIRED == ret) {
        is_build_replica_end = true;
      }
      LOG_WARN("fail to send build replica request", K(ret));
    } else {
      TCWLockGuard guard(lock_);
      build_replica_request_time_ = ObTimeUtility::current_time();
    }
  }
  DEBUG_SYNC(TABLE_REDEFINITION_REPLICA_BUILD);
  if (OB_SUCC(ret) && !is_build_replica_end) {
    if (OB_FAIL(check_build_replica_end(is_build_replica_end))) {
      LOG_WARN("check build replica end failed", K(ret));
    }
  }

  // overwrite ret
  if (is_build_replica_end) {
    ret = OB_SUCC(ret) ? complete_sstable_job_ret_code_ : ret;
    bool need_verify_checksum = true;
#ifdef ERRSIM
    // when the major compaction is delayed, skip verify column checksum
    need_verify_checksum = 0 == GCONF.errsim_ddl_major_delay_time;
#endif
    if (OB_SUCC(ret) && need_verify_checksum) {
      if (OB_FAIL(replica_end_check(ret))) {
        LOG_WARN("fail to check", K(ret));
      }
    }
    if (OB_FAIL(switch_status(next_task_status, true, ret))) {
      // overwrite ret
      LOG_WARN("fail to switch task status", K(ret));
    }
  }
  return ret;
}

int ObTableRedefinitionTask::replica_end_check(const int ret_code)
{
  int ret = OB_SUCCESS;
  switch(task_type_) {
    case DDL_DIRECT_LOAD :
    case DDL_DIRECT_LOAD_INSERT :
    case DDL_MVIEW_COMPLETE_REFRESH: {
      break;
    }
    default : {
      if (OB_FAIL(check_data_dest_tables_columns_checksum(get_execution_id()))) {
        LOG_WARN("fail to check the columns checksum of data table and destination table", K(ret));
      }
      break;
    }
  }
  return ret;
}

int ObTableRedefinitionTask::copy_table_indexes()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, REDEF_TASK_COPY_INDEX_FAILED))) {
    LOG_WARN("ddl sim failure", K(tenant_id_), K(task_id_));
  } else {
    const int64_t MAX_ACTIVE_TASK_CNT = 1;
    int64_t active_task_cnt = 0;
    // check if has rebuild index
    if (has_rebuild_index_) {
    } else if (OB_FAIL(ObDDLTaskRecordOperator::get_create_index_or_mlog_task_cnt(GCTX.root_service_->get_sql_proxy(), dst_tenant_id_, target_object_id_, active_task_cnt))) {
      LOG_WARN("failed to check index or mlog task cnt", K(ret));
    } else if (active_task_cnt >= MAX_ACTIVE_TASK_CNT) {
      ret = OB_EAGAIN;
    } else {
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema *table_schema = nullptr;
      ObSArray<uint64_t> index_ids;
      alter_table_arg_.ddl_task_type_ = share::REBUILD_INDEX_TASK;
      alter_table_arg_.table_id_ = object_id_;
      alter_table_arg_.hidden_table_id_ = target_object_id_;
      if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(dst_tenant_id_, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(dst_tenant_id_, target_object_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(dst_tenant_id_), K(target_object_id_));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, table schema must not be nullptr", K(ret), K(target_object_id_));
      } else {
        const common::ObIArray<ObAuxTableMetaInfo> &index_infos = table_schema->get_simple_index_infos();
        if ((index_infos.count() > 0) || (table_schema->mv_container_table()
                                          && table_schema->has_mlog_table())) {
          // if there is indexes in new tables, if so, the indexes is already rebuilt in new table
          for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
            if (OB_FAIL(index_ids.push_back(index_infos.at(i).table_id_))) {
              LOG_WARN("push back index id failed", K(ret));
            }
          }
          if (OB_SUCC(ret) && table_schema->has_mlog_table()) {
            if (OB_FAIL(index_ids.push_back(table_schema->get_mlog_tid()))) {
              LOG_WARN("failed to push back mlog tid", KR(ret), K(table_schema->get_mlog_tid()));
            }
          }
          LOG_INFO("indexes schema are already built", K(index_ids));
        } else {
          int64_t ddl_rpc_timeout = 0;
          int64_t all_tablet_count = 0;
          ObSchemaGetterGuard orig_schema_guard;
          if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, orig_schema_guard))) {
            LOG_WARN("get schema guard failed", K(ret), K(tenant_id_));
          } else if (OB_FAIL(generate_rebuild_index_arg_list(tenant_id_, object_id_, orig_schema_guard, alter_table_arg_))) {
            LOG_WARN("fail to generate rebuild index arg list", K(ret), K(tenant_id_), K(object_id_));
          } else if (OB_FAIL(get_orig_all_index_tablet_count(orig_schema_guard, all_tablet_count))) {
            LOG_WARN("get all tablet count failed", K(ret));
          } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(all_tablet_count, ddl_rpc_timeout))) {
            LOG_WARN("get ddl rpc timeout failed", K(ret));
          } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(ddl_rpc_timeout).
                execute_ddl_task(alter_table_arg_, index_ids))) {
            LOG_WARN("rebuild hidden table index failed", K(ret), K(ddl_rpc_timeout));
          }
        }
      }
      DEBUG_SYNC(TABLE_REDEFINITION_COPY_TABLE_INDEXES);
      if (OB_SUCC(ret) && index_ids.count() > 0) {
        ObSchemaGetterGuard new_schema_guard;
        if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(dst_tenant_id_, new_schema_guard))) {
          LOG_WARN("failed to refresh schema guard", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); ++i) {
          const uint64_t index_id = index_ids.at(i);	
          const ObTableSchema *index_schema = nullptr;
          ObDDLTaskRecord task_record;
          bool need_rebuild_index = true;
          SMART_VAR(ObCreateIndexArg, create_index_arg) {
            ObTraceIdGuard trace_id_guard(get_trace_id());
            ATOMIC_INC(&sub_task_trace_id_);
            ObDDLEventInfo ddl_event_info(sub_task_trace_id_);
            // this create index arg is not valid, only has nls format
            create_index_arg.nls_date_format_ = alter_table_arg_.nls_formats_[0];
            create_index_arg.nls_timestamp_format_ = alter_table_arg_.nls_formats_[1];
            create_index_arg.nls_timestamp_tz_format_ = alter_table_arg_.nls_formats_[2];
            if (OB_FAIL(new_schema_guard.get_table_schema(dst_tenant_id_, index_ids.at(i), index_schema))) {
              LOG_WARN("get table schema failed", K(ret), K(dst_tenant_id_), K(index_ids.at(i)));
            } else if (OB_ISNULL(index_schema)) {
              ret = OB_ERR_SYS;
              LOG_WARN("error sys, index schema must not be nullptr", K(ret), K(index_ids.at(i)));
            } else if (is_final_index_status(index_schema->get_index_status())) {
              // index status is final
              need_rebuild_index = false;
              LOG_INFO("index status is final", K(ret), K(task_id_), K(index_id), K(need_rebuild_index));
            } else if (active_task_cnt >= MAX_ACTIVE_TASK_CNT) {
              ret = OB_EAGAIN;
            } else {
              ObDDLType ddl_type = ObDDLType::DDL_CREATE_INDEX;
              if (index_schema->is_mlog_table()) {
                ddl_type = ObDDLType::DDL_CREATE_MLOG;
                create_index_arg.index_action_type_ = ObIndexArg::ADD_MLOG;
              } else {
                ddl_type = get_create_index_type(data_format_version_, *index_schema);
              }
              create_index_arg.index_type_ = index_schema->get_index_type();
              ObCreateDDLTaskParam param(dst_tenant_id_,
                                         ddl_type,
                                         table_schema,
                                         index_schema,
                                         0/*object_id*/,
                                         index_schema->get_schema_version(),
                                         parallelism_,
                                         consumer_group_id_,
                                         &allocator_,
                                         &create_index_arg,
                                         task_id_);
              param.sub_task_trace_id_ = sub_task_trace_id_;
              param.tenant_data_version_ = data_format_version_;
              if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param, *GCTX.sql_proxy_, task_record))) {
                if (OB_ENTRY_EXIST == ret) {
                  ret = OB_SUCCESS;
                  active_task_cnt += 1;
                } else {
                  LOG_WARN("submit ddl task failed", K(ret));
                }
              } else if (FALSE_IT(active_task_cnt += 1)) {
              } else if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
                LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
              }
            }
            if (OB_FAIL(ret)) {
              add_event_info("create table_redefinition index task fail");
              LOG_WARN("add build index task failed", K(ret), K(task_record), K(ddl_event_info));
            } else if (need_rebuild_index) {
              TCWLockGuard guard(lock_);
              const uint64_t task_key = index_ids.at(i);
              DependTaskStatus status;
              status.task_id_ = task_record.task_id_;
              if (OB_FAIL(dependent_task_result_map_.get_refactored(task_key, status))) {
                if (OB_HASH_NOT_EXIST != ret) {
                  LOG_WARN("get from dependent task map failed", K(ret));
                } else if (OB_FAIL(dependent_task_result_map_.set_refactored(task_key, status))) {
                  LOG_WARN("set dependent task map failed", K(ret), K(task_key));
                }
              }
              add_event_info("create table_redefinition index task succ");
              LOG_INFO("add build index task", K(ret), K(task_key), K(status), K(ddl_event_info));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        has_rebuild_index_ = true;
      }
    }
  }
  return ret;
}

int ObTableRedefinitionTask::copy_table_constraints()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  const ObTableSchema *table_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, REDEF_TASK_COPY_CONSTRAINT_FAILED))) {
    LOG_WARN("ddl sim failure", K(tenant_id_), K(task_id_));
  } else {
    if (has_rebuild_constraint_) {
      // do nothing
    } else {
      ObSArray<uint64_t> constraint_ids;
      ObSArray<uint64_t> new_constraint_ids;
      bool need_rebuild_constraint = true;
      if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(dst_tenant_id_, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(dst_tenant_id_, target_object_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(dst_tenant_id_), K(target_object_id_));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, table schema must not be nullptr", K(ret), K(target_object_id_));
      } else if (OB_FAIL(check_need_rebuild_constraint(*table_schema,
                                                       new_constraint_ids,
                                                       need_rebuild_constraint))) {
        LOG_WARN("failed to check need rebuild constraint", K(ret));
      } else if (need_rebuild_constraint) {
        alter_table_arg_.ddl_task_type_ = share::REBUILD_CONSTRAINT_TASK;
        alter_table_arg_.table_id_ = object_id_;
        alter_table_arg_.hidden_table_id_ = target_object_id_;
        int64_t ddl_rpc_timeout = 0;
        if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(dst_tenant_id_, target_object_id_, ddl_rpc_timeout))) {
          LOG_WARN("get ddl rpc timeout fail", K(ret));
        } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(ddl_rpc_timeout).
              execute_ddl_task(alter_table_arg_, constraint_ids))) {
          LOG_WARN("rebuild hidden table constraint failed", K(ret), K(ddl_rpc_timeout));
        }
      } else {
        LOG_INFO("constraint has already been built");
      }
      DEBUG_SYNC(TABLE_REDEFINITION_COPY_TABLE_CONSTRAINTS);
      if (OB_SUCC(ret) && constraint_ids.count() > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < constraint_ids.count(); ++i) {
          if (OB_FAIL(add_constraint_ddl_task(constraint_ids.at(i)))) {
            LOG_WARN("add constraint ddl task failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && new_constraint_ids.count() > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < new_constraint_ids.count(); ++i) {
          if (OB_FAIL(add_constraint_ddl_task(new_constraint_ids.at(i)))) {
            LOG_WARN("add constraint ddl task failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        has_rebuild_constraint_ = true;
      }
    }
  }
  return ret;
}

int ObTableRedefinitionTask::copy_table_foreign_keys()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, REDEF_TASK_COPY_FOREIGN_KEY_FAILED))) {
    LOG_WARN("ddl sim failure", K(tenant_id_), K(task_id_));
  } else {
    if (has_rebuild_foreign_key_) {
      // do nothing
    } else {
      if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(dst_tenant_id_, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_simple_table_schema(dst_tenant_id_, target_object_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(dst_tenant_id_), K(target_object_id_));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, table schema must not be nullptr", K(ret), K(target_object_id_));
      } else {
        const ObIArray<ObSimpleForeignKeyInfo> &fk_infos = table_schema->get_simple_foreign_key_info_array();
        ObSArray<uint64_t> fk_ids;
        LOG_INFO("get current fk infos", K(fk_infos));
        if (fk_infos.count() > 0) {
          for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos.count(); ++i) {
            if (OB_FAIL(fk_ids.push_back(fk_infos.at(i).foreign_key_id_))) {
              LOG_WARN("push back fk id failed", K(ret));
            }
          }
          LOG_INFO("foreign key is already built", K(fk_infos));
        } else {
          alter_table_arg_.ddl_task_type_ = share::REBUILD_FOREIGN_KEY_TASK;
          alter_table_arg_.table_id_ = object_id_;
          alter_table_arg_.hidden_table_id_ = target_object_id_;
          int64_t ddl_rpc_timeout = 0;
          if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(dst_tenant_id_, target_object_id_, ddl_rpc_timeout))) {
            LOG_WARN("get ddl rpc timeout fail", K(ret));
          } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(ddl_rpc_timeout).
                execute_ddl_task(alter_table_arg_, fk_ids))) {
            LOG_WARN("rebuild hidden table constraint failed", K(ret), K(ddl_rpc_timeout));
          }
        }
        DEBUG_SYNC(TABLE_REDEFINITION_COPY_TABLE_FOREIGN_KEYS);
        if (OB_SUCC(ret) && fk_ids.count() > 0) {
          for (int64_t i = 0; OB_SUCC(ret) && i < fk_ids.count(); ++i) {
            if (OB_FAIL(add_fk_ddl_task(fk_ids.at(i)))) {
              LOG_WARN("add foreign key ddl task failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          has_rebuild_foreign_key_ = true;
        }
      }
    }
  }
  return ret;
}

int ObTableRedefinitionTask::copy_table_dependent_objects(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  int64_t finished_task_cnt = 0;
  bool state_finish = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, REDEF_TASK_COPY_DEPENDENT_OBJECTS_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (!dependent_task_result_map_.created() && OB_FAIL(dependent_task_result_map_.create(MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("DepTasMap")))) {
    LOG_WARN("create dependent task map failed", K(ret));
  } else {
    if (get_is_copy_indexes() && OB_FAIL(copy_table_indexes())) {
      LOG_WARN("copy table indexes failed", K(ret));
    } else if (get_is_copy_constraints() && OB_FAIL(copy_table_constraints())) {
      LOG_WARN("copy table constraints failed", K(ret));
    } else if (get_is_copy_foreign_keys() && OB_FAIL(copy_table_foreign_keys())) {
      LOG_WARN("copy table foreign keys failed", K(ret));
    } else {
      // copy triggers(at current, not supported, skip it)
    }
  }

  if (OB_FAIL(ret)) {
    state_finish = true;
  } else {
    // wait copy dependent objects to be finished
    ObAddr unused_addr;
    TCRLockGuard guard(lock_);
    for (common::hash::ObHashMap<uint64_t, DependTaskStatus>::const_iterator iter = dependent_task_result_map_.begin();
        OB_SUCC(ret) && iter != dependent_task_result_map_.end(); ++iter) {
      const uint64_t task_key = iter->first;
      const int64_t target_object_id = -1;
      const int64_t child_task_id = iter->second.task_id_;
      if (iter->second.ret_code_ == INT64_MAX) {
        // maybe ddl already finish when switching rs
        HEAP_VAR(ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage, error_message) {
          int64_t unused_user_msg_len = 0;
          if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(dst_tenant_id_, child_task_id, target_object_id,
                  unused_addr, false /* is_ddl_retry_task */, *GCTX.sql_proxy_, error_message, unused_user_msg_len))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              LOG_INFO("ddl task not finish", K(dst_tenant_id_), K(task_key), K(child_task_id), K(target_object_id));
            } else {
              LOG_WARN("fail to get ddl error message", K(ret), K(task_key), K(child_task_id), K(target_object_id));
            }
          } else {
            finished_task_cnt++;
            if (error_message.ret_code_ != OB_SUCCESS) {
              ret = error_message.ret_code_;
              if (get_is_ignore_errors()) {
                ret = OB_SUCCESS;
              }
            }
          }
        }
      } else {
        finished_task_cnt++;
        if (iter->second.ret_code_ != OB_SUCCESS) {
          ret = iter->second.ret_code_;
          if (get_is_ignore_errors()) {
            ret = OB_SUCCESS;
          }
        }
      }
    }
    if (finished_task_cnt == dependent_task_result_map_.size() || OB_FAIL(ret)) {
      // 1. all child tasks finish.
      // 2. the parent task exits if any child task fails.
      state_finish = true;
    }
  }
  if (state_finish) {
    if (OB_FAIL(switch_status(next_task_status, true, ret))) {
      LOG_WARN("fail to switch status", K(ret));
    }
  }
  return ret;
}

int ObTableRedefinitionTask::take_effect(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ROOTSERVICE_EVENT_ADD("ddl_task", "before_table_redefinition_task_effect",
                   "tenant_id", tenant_id_,
                   "object_id", object_id_,
                   "target_object_id", target_object_id_);
  DEBUG_SYNC(BEFORE_TABLE_REDEFINITION_TASK_EFFECT);
#endif
  ObSArray<uint64_t> objs;
  int64_t ddl_rpc_timeout = 0;
  alter_table_arg_.ddl_task_type_ = ObDDLType::DDL_TABLE_RESTORE != task_type_ ?
                                    share::MAKE_DDL_TAKE_EFFECT_TASK : share::MAKE_RECOVER_RESTORE_TABLE_TASK_TAKE_EFFECT;
  alter_table_arg_.table_id_ = object_id_;
  alter_table_arg_.hidden_table_id_ = target_object_id_;
  // offline ddl is allowed on table with trigger(enable/disable).
  alter_table_arg_.need_rebuild_trigger_ = true;
  alter_table_arg_.task_id_ = task_id_;
  ObRootService *root_service = GCTX.root_service_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObDDLTaskStatus new_status = next_task_status;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_TAKE_EFFECT_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(dst_tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(dst_tenant_id_, target_object_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(dst_tenant_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema not exist", K(ret), K(target_object_id_));
  } else if (!table_schema->is_user_hidden_table()) {
    LOG_INFO("target schema took effect", K(target_object_id_));
  } else if (table_schema->is_heap_table()
      && !(DDL_ALTER_PARTITION_BY == task_type_ || DDL_DROP_PRIMARY_KEY == task_type_)
      && OB_FAIL(sync_tablet_autoinc_seq())) {
    if (OB_TIMEOUT == ret || OB_NOT_MASTER == ret) {
      ret = OB_SUCCESS;
      new_status = ObDDLTaskStatus::TAKE_EFFECT;
    } else {
      LOG_WARN("fail to sync tablet autoinc seq", K(ret));
    }
  } else if (OB_FAIL(sync_auto_increment_position())) {
    if (OB_NOT_MASTER == ret) {
      ret = OB_SUCCESS;
      new_status = ObDDLTaskStatus::TAKE_EFFECT;
    } else {
      LOG_WARN("sync auto increment position failed", K(ret), K(object_id_), K(target_object_id_));
    }
  } else if (ObDDLType::DDL_DIRECT_LOAD != task_type_ &&
             ObDDLType::DDL_DIRECT_LOAD_INSERT != task_type_ &&
             ObDDLType::DDL_MVIEW_COMPLETE_REFRESH != task_type_ &&
             OB_FAIL(sync_stats_info())) {//direct load no need sync stats info, because the stats have been regather
    LOG_WARN("fail to sync stats info", K(ret), K(object_id_), K(target_object_id_));
  } else if (alter_table_arg_.mview_refresh_info_.is_mview_complete_refresh_ &&
             OB_FAIL(alter_table_arg_.mview_refresh_info_.refresh_scn_.convert_for_inner_table_field(snapshot_version_))) {
    LOG_WARN("fail to convert scn", K(ret), K(snapshot_version_));
  } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(dst_tenant_id_, target_object_id_, ddl_rpc_timeout))) {
            LOG_WARN("get ddl rpc timeout fail", K(ret));
  } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(ddl_rpc_timeout).
      execute_ddl_task(alter_table_arg_, objs))) {
    int tmp_ret = OB_SUCCESS;
    bool has_took_effect_succ = false;
    if (OB_TMP_FAIL(check_take_effect_succ(has_took_effect_succ))) {
      LOG_WARN("check took effect failed", K(ret), K(tmp_ret), K(target_object_id_));
    } else if (has_took_effect_succ) {
      ret = OB_SUCCESS;
    }
    LOG_WARN("swap orig and hidden table state failed", K(ret), K(tmp_ret), K(has_took_effect_succ), K(target_object_id_));
  }
  DEBUG_SYNC(TABLE_REDEFINITION_TAKE_EFFECT);
  if (new_status == next_task_status || OB_FAIL(ret)) {
    if (OB_FAIL(switch_status(next_task_status, true, ret))) {
      LOG_WARN("fail to switch status", K(ret));
    }
  }
  char object_id_buffer[256];
  snprintf(object_id_buffer, sizeof(object_id_buffer), "object_id:%ld, target_object_id:%ld",
            object_id_, target_object_id_);
  ROOTSERVICE_EVENT_ADD("ddl scheduler", "table redefinition task take effect",
    "tenant_id", tenant_id_,
    "ret", ret,
    K_(trace_id),
    K_(task_id),
    "object_id", object_id_buffer,
    K_(schema_version),
    next_task_status);
  LOG_INFO("table redefinition task take effect", K(ret), "ddl_event_info", ObDDLEventInfo(), K(*this));
  return ret;
}

int ObTableRedefinitionTask::check_take_effect_succ(bool &has_took_effect_succ)
{
  int ret = OB_SUCCESS;
  has_took_effect_succ = false;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(dst_tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(dst_tenant_id_, target_object_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(dst_tenant_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema not exist", K(ret), K(target_object_id_));
  } else if (!table_schema->is_user_hidden_table()) {
    has_took_effect_succ = true;
    LOG_INFO("target schema took effect", K(target_object_id_));
  }
  return ret;
}

int ObTableRedefinitionTask::repending(const share::ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, TABLE_REDEF_TASK_REPENDING_FAILED))) {
    LOG_WARN("ddl sim failure", K(tenant_id_), K(task_id_));
  } else {
    switch (task_type_) {
      case DDL_DIRECT_LOAD:
      case DDL_DIRECT_LOAD_INSERT:
        if (get_is_do_finish() || get_is_abort()) {
          if (OB_FAIL(switch_status(next_task_status, true, ret))) {
            LOG_WARN("fail to switch status", K(ret));
          }
        }
        break;
      default:
        if (OB_FAIL(switch_status(next_task_status, true, ret))) {
          LOG_WARN("fail to switch status", K(ret));
        }
        break;
    }
  }
  return ret;
}

bool ObTableRedefinitionTask::check_task_status_is_pending(const share::ObDDLTaskStatus task_status)
{
  return task_status == ObDDLTaskStatus::REPENDING;
}

int ObTableRedefinitionTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("check task health failed", K(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    switch(task_status_) {
      case ObDDLTaskStatus::PREPARE:
        if (alter_table_arg_.mview_refresh_info_.is_mview_complete_refresh_ && parent_task_id_ > 0) {
          const ObDDLTaskID parent_task_id(tenant_id_, parent_task_id_);
          ObRootService *root_service = GCTX.root_service_;
          if (OB_ISNULL(root_service)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, root service must not be nullptr", K(ret));
          } else if (OB_FAIL(root_service->get_ddl_task_scheduler().on_ddl_task_prepare(parent_task_id, task_id_, trace_id_))) {
            LOG_WARN("fail to do parent task callback", KR(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(prepare(ObDDLTaskStatus::WAIT_TRANS_END))) {
            LOG_WARN("fail to prepare table redefinition task", K(ret));
          }
        }
        break;
      case ObDDLTaskStatus::WAIT_TRANS_END:
        if (OB_FAIL(wait_trans_end(wait_trans_ctx_, ObDDLTaskStatus::OBTAIN_SNAPSHOT))) {
          LOG_WARN("fail to wait trans end", K(ret));
        }
        break;
      case ObDDLTaskStatus::OBTAIN_SNAPSHOT:
        if (OB_FAIL(obtain_snapshot(ObDDLTaskStatus::CHECK_TABLE_EMPTY))) {
          LOG_WARN("fail to lock table", K(ret));
        }
        break;
      case ObDDLTaskStatus::CHECK_TABLE_EMPTY:
        if (OB_FAIL(check_table_empty(ObDDLTaskStatus::REPENDING))) {
          LOG_WARN("fail to check table empty", K(ret));
        }
        break;
      case ObDDLTaskStatus::REPENDING:
        if (OB_FAIL(repending(ObDDLTaskStatus::REDEFINITION))) {
          LOG_WARN("fail to repending", K(ret));
        }
        break;
      case ObDDLTaskStatus::REDEFINITION:
        if (OB_FAIL(table_redefinition(ObDDLTaskStatus::COPY_TABLE_DEPENDENT_OBJECTS))) {
          LOG_WARN("fail to do table redefinition", K(ret));
        }
        break;
      case ObDDLTaskStatus::COPY_TABLE_DEPENDENT_OBJECTS:
        if (OB_FAIL(copy_table_dependent_objects(ObDDLTaskStatus::MODIFY_AUTOINC))) {
          LOG_WARN("fail to copy table dependent objects", K(ret));
        }
        break;
      case ObDDLTaskStatus::MODIFY_AUTOINC:
        if (OB_FAIL(modify_autoinc(ObDDLTaskStatus::TAKE_EFFECT))) {
          LOG_WARN("fail to modify autoinc", K(ret));
        }
        break;
      case ObDDLTaskStatus::TAKE_EFFECT:
        if (OB_FAIL(take_effect(ObDDLTaskStatus::SUCCESS))) {
          LOG_WARN("fail to take effect", K(ret));
        }
        break;
      case ObDDLTaskStatus::FAIL:
        if (OB_FAIL(fail())) {
          LOG_WARN("fail to do clean up", K(ret));
        }
        break;
      case ObDDLTaskStatus::SUCCESS:
        if (OB_FAIL(success())) {
          LOG_WARN("fail to success", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table redefinition task state", K(task_status_));
        break;
    }
    ddl_tracing_.release_span_hierarchy();
  }
  return ret;
}

int ObTableRedefinitionTask::check_modify_autoinc(bool &modify_autoinc)
{
  int ret = OB_SUCCESS;
  modify_autoinc = false;
  AlterTableSchema &alter_table_schema = alter_table_arg_.alter_table_schema_;
  ObTableSchema::const_column_iterator iter = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator iter_end = alter_table_schema.column_end();
  AlterColumnSchema *alter_column_schema = nullptr;
  for(; OB_SUCC(ret) && iter != iter_end; iter++) {
    if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*iter))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is NULL", K(ret));
    } else if (alter_column_schema->is_autoincrement()) {
      modify_autoinc = true;
    }
  }
  return ret;
}

int64_t ObTableRedefinitionTask::get_serialize_param_size() const
{
  int8_t copy_indexes = static_cast<int8_t>(is_copy_indexes_);
  int8_t copy_triggers = static_cast<int8_t>(is_copy_triggers_);
  int8_t copy_constraints = static_cast<int8_t>(is_copy_constraints_);
  int8_t copy_foreign_keys = static_cast<int8_t>(is_copy_foreign_keys_);
  int8_t ignore_errors = static_cast<int8_t>(is_ignore_errors_);
  int8_t do_finish = static_cast<int8_t>(is_do_finish_);
  return alter_table_arg_.get_serialize_size() + ObDDLTask::get_serialize_param_size()
         + serialization::encoded_length_i8(copy_indexes) + serialization::encoded_length_i8(copy_triggers)
         + serialization::encoded_length_i8(copy_constraints) + serialization::encoded_length_i8(copy_foreign_keys)
         + serialization::encoded_length_i8(ignore_errors) + serialization::encoded_length_i8(do_finish)
         + serialization::encoded_length_i64(target_cg_cnt_)
         + serialization::encoded_length_i64(complete_sstable_job_ret_code_)
         + serialization::encoded_length_i8(use_heap_table_ddl_plan_)
         + serialization::encoded_length_i8(is_ddl_retryable_);
}

int ObTableRedefinitionTask::serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int8_t copy_indexes = static_cast<int8_t>(is_copy_indexes_);
  int8_t copy_triggers = static_cast<int8_t>(is_copy_triggers_);
  int8_t copy_constraints = static_cast<int8_t>(is_copy_constraints_);
  int8_t copy_foreign_keys = static_cast<int8_t>(is_copy_foreign_keys_);
  int8_t ignore_errors = static_cast<int8_t>(is_ignore_errors_);
  int8_t do_finish = static_cast<int8_t>(is_do_finish_);
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_len, pos))) {
    LOG_WARN("ObDDLTask serialize failed", K(ret));
  } else if (OB_FAIL(alter_table_arg_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize table arg failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, copy_indexes))) {
    LOG_WARN("fail to serialize is_copy_indexes", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, copy_triggers))) {
    LOG_WARN("fail to serialize is_copy_triggers", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, copy_constraints))) {
    LOG_WARN("fail to serialize is_copy_constraints", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, copy_foreign_keys))) {
    LOG_WARN("fail to serialize is_copy_foreign_keys", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, ignore_errors))) {
    LOG_WARN("fail to serialize is_ignore_errors", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, do_finish))) {
    LOG_WARN("fail to serialize is_do_finish", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, target_cg_cnt_))) {
    LOG_WARN("fail to serialize target_cg_cnt", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, complete_sstable_job_ret_code_))) {
    LOG_WARN("fail to serialize complete sstable job ret code", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, use_heap_table_ddl_plan_))) {
    LOG_WARN("fail to serialize use heap table ddl plan", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, is_ddl_retryable_))) {
    LOG_WARN("fail to serialize ddl can retry", K(ret));
  }
  FLOG_INFO("serialize message for table redefinition", K(ret),
      K(copy_indexes), K(copy_triggers), K(copy_constraints), K(copy_foreign_keys), K(ignore_errors), K(do_finish), K(*this));
  return ret;
}

int ObTableRedefinitionTask::deserialize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int8_t copy_indexes = 0;
  int8_t copy_triggers = 0;
  int8_t copy_constraints = 0;
  int8_t copy_foreign_keys = 0;
  int8_t ignore_errors = 0;
  int8_t do_finish = 0;
  obrpc::ObAlterTableArg tmp_arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(data_len));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, data_len, pos))) {
    LOG_WARN("ObDDLTask deserlize failed", K(ret));
  } else if (OB_FAIL(tmp_arg.deserialize(buf, data_len, pos))) {
    LOG_WARN("serialize table failed", K(ret));
  } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(task_type_, tenant_id, tmp_arg))) {
    LOG_WARN("replace user tenant id failed", K(ret), K(tenant_id), K(tmp_arg));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, tmp_arg, alter_table_arg_))) {
    LOG_WARN("deep copy table arg failed", K(ret));
  } else if (pos < data_len) {
    if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &copy_indexes))) {
      LOG_WARN("fail to deserialize is_copy_indexes_", K(ret));
    } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &copy_triggers))) {
      LOG_WARN("fail to deserialize is_copy_triggers_", K(ret));
    } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &copy_constraints))) {
      LOG_WARN("fail to deserialize is_copy_constraints_", K(ret));
    } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &copy_foreign_keys))) {
      LOG_WARN("fail to deserialize is_copy_foreign_keys_", K(ret));
    } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &ignore_errors))) {
      LOG_WARN("fail to deserialize is_ignore_errors_", K(ret));
    } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &do_finish))) {
      LOG_WARN("fail to deserialize is_do_finish_", K(ret));
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &target_cg_cnt_))) {
      LOG_WARN("fail to deserialize target_cg_ctn_", K(ret));
    }
    else {
      is_copy_indexes_ = static_cast<bool>(copy_indexes);
      is_copy_triggers_ = static_cast<bool>(copy_triggers);
      is_copy_constraints_ = static_cast<bool>(copy_constraints);
      is_copy_foreign_keys_ = static_cast<bool>(copy_foreign_keys);
      is_ignore_errors_ = static_cast<bool>(ignore_errors);
      is_do_finish_ = static_cast<bool>(do_finish);
    }
    if (OB_SUCC(ret) && pos < data_len) {
      if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &complete_sstable_job_ret_code_))) {
        LOG_WARN("fail to deserialize complete sstable job ret code", K(ret));
      }
    }
    if (OB_SUCC(ret) && pos < data_len) {
      int8_t use_heap_table_ddl_plan = false;
      if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &use_heap_table_ddl_plan))) {
        LOG_WARN("fail to deserialize use heap table ddl plan", K(ret));
      } else {
        use_heap_table_ddl_plan_ = use_heap_table_ddl_plan;
      }
    }
    if (OB_SUCC(ret) && pos < data_len) {
      int8_t ddl_can_retry = false;
      if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &ddl_can_retry))) {
        LOG_WARN("fail to deserialize ddl can retry", K(ret));
      } else {
        is_ddl_retryable_ = ddl_can_retry;
      }
    }
  }
  FLOG_INFO("deserialize message for table redefinition", K(ret),
      K(copy_indexes), K(copy_triggers), K(copy_constraints), K(copy_foreign_keys), K(ignore_errors), K(do_finish), K(*this));
  return ret;
}

int ObTableRedefinitionTask::assign(const ObTableRedefinitionTask *table_redef_task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_table_arg(allocator_, table_redef_task->alter_table_arg_, alter_table_arg_))) {
    LOG_WARN("assign alter_table_arg failed", K(ret));
  } else {
    task_version_ = table_redef_task->task_version_;
    parallelism_ = table_redef_task->parallelism_;
    set_is_copy_indexes(table_redef_task->get_is_copy_indexes());
    set_is_copy_triggers(table_redef_task->get_is_copy_triggers());
    set_is_copy_constraints(table_redef_task->get_is_copy_constraints());
    set_is_copy_foreign_keys(table_redef_task->get_is_copy_foreign_keys());
    set_is_ignore_errors(table_redef_task->get_is_ignore_errors());
    set_is_do_finish(table_redef_task->get_is_do_finish());
  }
  return ret;
}

int ObTableRedefinitionTask::collect_longops_stat(ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
  databuff_printf(stat_info_.message_, MAX_LONG_OPS_MESSAGE_LENGTH, pos, "TENANT_ID: %ld, TASK_ID: %ld, ", dst_tenant_id_, task_id_);
  switch (status) {
    case ObDDLTaskStatus::PREPARE: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: PREPARE"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_TRANS_END: {
      if (snapshot_version_ > 0) {
        if (OB_FAIL(databuff_printf(stat_info_.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: ACQUIRE SNAPSHOT, SNAPSHOT_VERSION: %ld",
                                    snapshot_version_))) {
          LOG_WARN("failed to print", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(stat_info_.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: WAIT TRANS END, PENDING_TX_ID: %ld",
                                    wait_trans_ctx_.get_pending_tx_id().get_id()))) {
          LOG_WARN("failed to print", K(ret));
        }
      }
      break;
    }
    case ObDDLTaskStatus::OBTAIN_SNAPSHOT: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: OBTAIN SNAPSHOT"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::CHECK_TABLE_EMPTY: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: CHECK TABLE EMPTY"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::REDEFINITION: {
      int64_t row_scanned = 0;
      int64_t row_sorted = 0;
      int64_t row_inserted_cg = 0;
      int64_t row_inserted_file = 0;

      if (OB_FAIL(gather_redefinition_stats(dst_tenant_id_, task_id_, *GCTX.sql_proxy_, row_scanned, row_sorted, row_inserted_cg, row_inserted_file))) {
        LOG_WARN("failed to gather redefinition stats", K(ret));
      }


      if (OB_FAIL(ret)){
      } else if (target_cg_cnt_> 1) {
        if (OB_FAIL(databuff_printf(stat_info_.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: REPLICA BUILD, PARALLELISM: %ld, ROW_SCANNED: %ld, ROW_SORTED: %ld, ROW_INSERTED_TMP_FILE: %ld, ROW_INSERTED: %ld out of %ld column group rows",
                                    ObDDLUtil::get_real_parallelism(parallelism_, alter_table_arg_.mview_refresh_info_.is_mview_complete_refresh_),
                                    row_scanned,
                                    row_sorted,
                                    row_inserted_file,
                                    row_inserted_cg,
                                    row_scanned * target_cg_cnt_))) {
          LOG_WARN("failed to print", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(stat_info_.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: REPLICA BUILD, PARALLELISM: %ld, ROW_SCANNED: %ld, ROW_SORTED: %ld, ROW_INSERTED: %ld",
                                    ObDDLUtil::get_real_parallelism(parallelism_, alter_table_arg_.mview_refresh_info_.is_mview_complete_refresh_),
                                    row_scanned,
                                    row_sorted,
                                    row_inserted_file))) {
          LOG_WARN("failed to print", K(ret));
        }
      }
      break;
    }
    case ObDDLTaskStatus::COPY_TABLE_DEPENDENT_OBJECTS: {
      char child_task_ids[MAX_LONG_OPS_MESSAGE_LENGTH];
      if (OB_FAIL(get_child_task_ids(child_task_ids, MAX_LONG_OPS_MESSAGE_LENGTH))) {
        if (ret == OB_SIZE_OVERFLOW) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get all child task ids", K(ret));
        }
      } else if (OB_FAIL(databuff_printf(stat_info_.message_,
                                         MAX_LONG_OPS_MESSAGE_LENGTH,
                                         pos,
                                         "STATUS: COPY DEPENDENT OBJECTS, CHILD TASK IDS: %s",
                                         child_task_ids))) {
        if (ret == OB_SIZE_OVERFLOW) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to print", K(ret));
        }
      }
      break;
    }
    case ObDDLTaskStatus::MODIFY_AUTOINC: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: MODIFY AUTOINC"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::TAKE_EFFECT: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: TAKE EFFECT"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::REPENDING: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: REPENDING"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::FAIL: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: CLEAN ON FAIL"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    case ObDDLTaskStatus::SUCCESS: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: CLEAN ON SUCCESS"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not expected status", K(ret), K(status), K(*this));
      break;
    }
  }

  // append direct load information to the message
  if (OB_SUCC(ret)
     && (check_is_load_data(get_task_type()))) {
    common::ObArenaAllocator allocator(lib::ObLabel("RedefTask"));
    sql::ObLoadDataStat job_stat;
    if (OB_FAIL(get_direct_load_job_stat(allocator, job_stat))) {
      LOG_WARN("failed to get direct load job_stat", KR(ret));
    } else if (job_stat.job_id_ > 0) {
      databuff_printf(stat_info_.message_, MAX_LONG_OPS_MESSAGE_LENGTH, pos,
          ", TABLE_ID: %ld, BATCH_SIZE: %ld, PARALLEL: %ld, MAX_ALLOWED_ERROR_ROWS: %ld"
          ", DETECTED_ERROR_ROWS: %ld, PROCESSED_ROWS: %ld, LOAD_STATUS: %.*s",
          job_stat.job_id_,
          job_stat.batch_size_,
          job_stat.parallel_,
          job_stat.max_allowed_error_rows_,
          job_stat.detected_error_rows_,
          job_stat.coordinator_.received_rows_,
          job_stat.coordinator_.status_.length(),
          job_stat.coordinator_.status_.ptr());
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_COLLECT_LONGOPS_STAT_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(copy_longops_stat(value))) {
    LOG_WARN("failed to collect common longops stat", K(ret));
  }
  return ret;
}

int ObTableRedefinitionTask::get_direct_load_job_stat(common::ObArenaAllocator &allocator,
                                                      sql::ObLoadDataStat &job_stat)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &sql_proxy = *GCTX.sql_proxy_;
  ObSqlString select_sql;
  sqlclient::ObMySQLResult *select_result = NULL;
  SMART_VAR(ObMySQLProxy::MySQLResult, select_res) {
    if (OB_FAIL(select_sql.assign_fmt(
        "SELECT JOB_ID, BATCH_SIZE, PARALLEL, MAX_ALLOWED_ERROR_ROWS, DETECTED_ERROR_ROWS, "
        "COORDINATOR_RECEIVED_ROWS, COORDINATOR_STATUS FROM %s WHERE TENANT_ID=%lu "
        "AND JOB_ID=%ld AND JOB_TYPE='direct' AND COORDINATOR_STATUS!='none'",
        OB_ALL_VIRTUAL_LOAD_DATA_STAT_TNAME, tenant_id_, object_id_))) {
      LOG_WARN("failed to assign sql", KR(ret));
    } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, TABLE_REDEF_TASK_GET_DIRECT_LOAD_JOB_STAT_FAILED))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, TABLE_REDEF_TASK_GET_DIRECT_LOAD_JOB_STAT_SLOW))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(sql_proxy.read(select_res, OB_SYS_TENANT_ID, select_sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K(select_sql));
    } else if (OB_ISNULL(select_result = select_res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", KR(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(select_result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next row", KR(ret));
        }
      } else {
        ObString load_status;
        EXTRACT_INT_FIELD_MYSQL(*select_result, "JOB_ID", job_stat.job_id_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*select_result, "BATCH_SIZE", job_stat.batch_size_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*select_result, "PARALLEL", job_stat.parallel_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*select_result, "MAX_ALLOWED_ERROR_ROWS", job_stat.max_allowed_error_rows_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*select_result, "DETECTED_ERROR_ROWS", job_stat.detected_error_rows_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*select_result, "COORDINATOR_RECEIVED_ROWS", job_stat.coordinator_.received_rows_, int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*select_result, "COORDINATOR_STATUS", load_status);
        if (OB_SUCC(ret)
            && OB_FAIL(ob_write_string(allocator, load_status, job_stat.coordinator_.status_))) {
          LOG_WARN("failed to write string", KR(ret));
        }
      }
    }
  }
  return ret;
}
