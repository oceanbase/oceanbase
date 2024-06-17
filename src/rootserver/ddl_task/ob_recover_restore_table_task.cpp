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
#include "ob_recover_restore_table_task.h"
#include "lib/rc/context.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ddl_task/ob_ddl_redefinition_task.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;

ObRecoverRestoreTableTask::ObRecoverRestoreTableTask()
  : ObTableRedefinitionTask()
{
}

ObRecoverRestoreTableTask::~ObRecoverRestoreTableTask()
{
}

int ObRecoverRestoreTableTask::init(
    const ObTableSchema* src_table_schema, const ObTableSchema* dst_table_schema,
    const int64_t task_id, const share::ObDDLType &ddl_type, const int64_t parallelism,
    const int64_t consumer_group_id, const int32_t sub_task_trace_id,
    const obrpc::ObAlterTableArg &alter_table_arg, const uint64_t tenant_data_version, const int64_t task_status, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(src_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src schema should not be null", K(ret));
  } else if (OB_ISNULL(dst_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dst_table schema should not be null", K(ret));
  } else if ((!src_table_schema->is_valid()) || (!dst_table_schema->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src_talbe or dst_table is invalid", K(ret), KPC(src_table_schema), KPC(dst_table_schema));
  } else if (OB_UNLIKELY(ObDDLType::DDL_TABLE_RESTORE != ddl_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ddl_type), KPC(src_table_schema), KPC(dst_table_schema));
  } else if (OB_FAIL(ObTableRedefinitionTask::init(src_table_schema, dst_table_schema, 0, task_id, ddl_type, parallelism, consumer_group_id,
                                                   sub_task_trace_id, alter_table_arg, tenant_data_version, task_status, 0/*snapshot*/))) {
    LOG_WARN("fail to init ObDropPrimaryKeyTask", K(ret));
  } else {
    execution_id_ = 1L;
    task_version_ = OB_RECOVER_RESTORE_TABLE_TASK_VERSION;
    set_is_ignore_errors(true);
  }
  LOG_INFO("init recover restore table ddl task finished", K(ret), KPC(this));
  return ret;
}

int ObRecoverRestoreTableTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRecoverRestoreTableTask has already been inited", K(ret));
  } else if (!task_record.is_valid() || ObDDLType::DDL_TABLE_RESTORE != task_record.ddl_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_FAIL(ObTableRedefinitionTask::init(task_record))) {
    LOG_WARN("deserialize to init task failed", K(ret), K(task_record));
  } else {
    set_is_ignore_errors(true);
  }
  LOG_INFO("init recover table restore ddl task finished", K(ret), KPC(this));
  return ret;
}

int ObRecoverRestoreTableTask::obtain_snapshot(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  ObDDLTaskStatus new_status = ObDDLTaskStatus::OBTAIN_SNAPSHOT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverRestoreTableTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (snapshot_version_ > 0) {
    // do nothing, already hold snapshot.
  } else if (OB_FAIL(ObDDLWaitTransEndCtx::calc_snapshot_with_gts(dst_tenant_id_, task_id_, 0/*trans_end_snapshot*/, snapshot_version_))) {
    // fetch snapshot.
    LOG_WARN("calc snapshot with gts failed", K(ret), K(dst_tenant_id_));
  } else if (snapshot_version_ <= 0) {
    // the snapshot version obtained here must be valid.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snapshot version is invalid", K(ret), K(snapshot_version_));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::update_snapshot_version(root_service->get_sql_proxy(),
                                                                      dst_tenant_id_,
                                                                      task_id_,
                                                                      snapshot_version_))) {
    LOG_WARN("update snapshot version failed", K(ret), K(dst_tenant_id_), K(task_id_), K(snapshot_version_));
  }

  if (OB_FAIL(ret)) {
    snapshot_version_ = 0; // reset snapshot if failed.
  } else {
    new_status = next_task_status;
  }
  if (new_status == next_task_status || OB_FAIL(ret)) {
    if (OB_FAIL(switch_status(new_status, true, ret))) {
      LOG_WARN("fail to switch task status", K(ret));
    }
  }
  return ret;
}

// update sstable complement status for all leaders
int ObRecoverRestoreTableTask::update_complete_sstable_job_status(const common::ObTabletID &tablet_id,
                                                                 const int64_t snapshot_version,
                                                                 const int64_t execution_id,
                                                                 const int ret_code,
                                                                 const ObDDLTaskInfo &addition_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
  } else if (ObDDLTaskStatus::REDEFINITION != task_status_) {
    // by pass, may be network delay
  } else if (snapshot_version != snapshot_version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snapshot version not match", K(ret), K(snapshot_version), K(snapshot_version_));
  } else if (execution_id < execution_id_) {
    LOG_INFO("receive a mismatch execution result, ignore", K(ret_code), K(execution_id), K(execution_id_));
  } else if (OB_FAIL(replica_builder_.set_partition_task_status(tablet_id,
                                                                ret_code,
                                                                addition_info.row_scanned_,
                                                                addition_info.row_inserted_))) {
    LOG_WARN("fail to set partition task status", K(ret));
  }
  return ret;
}

int ObRecoverRestoreTableTask::success()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(cleanup())) {
    LOG_WARN("clean up failed", K(ret));
  }
  return ret;
}

int ObRecoverRestoreTableTask::fail()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_arena("RestoreDDLClean");
  bool already_cleanuped = false;

  int64_t rpc_timeout = 0;
  obrpc::ObTableItem table_item;
  obrpc::ObDropTableArg drop_table_arg;
  obrpc::ObDDLRes drop_table_res;
  ObRootService *root_service = GCTX.root_service_;
  {
    bool is_oracle_mode = false;
    int64_t all_indexes_tablets_count = 0;
    const ObDatabaseSchema *db_schema = nullptr;
    const ObTableSchema *table_schema = nullptr;
    ObSchemaGetterGuard dst_tenant_schema_guard;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_ISNULL(root_service)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, root service must not be nullptr", K(ret));
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(dst_tenant_id_, dst_tenant_schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret), K(dst_tenant_id_));
    } else if (OB_FAIL(dst_tenant_schema_guard.get_table_schema(dst_tenant_id_, target_object_id_, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(dst_tenant_id_), K(target_object_id_));
    } else if (OB_ISNULL(table_schema)) {
      // already dropped.
      already_cleanuped = true;
      LOG_INFO("already dropped", K(ret), K(dst_tenant_id_), K(target_object_id_));
    } else if (OB_FAIL(dst_tenant_schema_guard.get_database_schema(dst_tenant_id_, table_schema->get_database_id(), db_schema))) {
      LOG_WARN("get db schema failed", K(ret), K(dst_tenant_id_), KPC(table_schema));
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(ret), K(dst_tenant_id_), "db_id", table_schema->get_database_id());
    } else if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("failed to check if oralce compat mode", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_all_indexes_tablets_count(dst_tenant_schema_guard, dst_tenant_id_, target_object_id_, all_indexes_tablets_count))) {
      LOG_WARN("get all indexes tablets count failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(all_indexes_tablets_count + table_schema->get_all_part_num(), rpc_timeout))) {
      LOG_WARN("get ddl rpc timeout failed", K(ret));
    } else if (OB_FAIL(ob_write_string(tmp_arena, db_schema->get_database_name_str(), table_item.database_name_))) {
      LOG_WARN("deep cpy database name failed", K(ret), "db_name", db_schema->get_database_name_str());
    } else if (OB_FAIL(ob_write_string(tmp_arena, table_schema->get_table_name_str(), table_item.table_name_))) {
      LOG_WARN("deep cpy table name failed", K(ret), "table_name", table_schema->get_table_name_str());
    } else {
      // for drop table item.
      table_item.mode_                   = table_schema->get_name_case_mode();
      table_item.is_hidden_              = table_schema->is_hidden_schema();
      // for drop table arg.
      drop_table_arg.tenant_id_          = dst_tenant_id_;
      drop_table_arg.exec_tenant_id_     = dst_tenant_id_;
      drop_table_arg.session_id_         = table_schema->get_session_id();
      drop_table_arg.table_type_         = table_schema->get_table_type();
      drop_table_arg.foreign_key_checks_ = false;
      drop_table_arg.force_drop_         = true;
      drop_table_arg.compat_mode_        = is_oracle_mode ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
    }
  }

  if (OB_SUCC(ret) && !already_cleanuped) {
    bool all_complement_dag_exit = true;
    if (OB_FAIL(check_and_cancel_complement_data_dag(all_complement_dag_exit))) {
      LOG_WARN("check and cancel complement data dag failed", K(ret));
    } else if (!all_complement_dag_exit) {
      if (REACH_COUNT_INTERVAL(1000L)) {
        LOG_INFO("wait all complement data dag exit", K(dst_tenant_id_), K(task_id_));
      }
    } else if (OB_FAIL(drop_table_arg.tables_.push_back(table_item))) {
      LOG_WARN("push back failed", K(ret), K(drop_table_arg));
    } else if (OB_FAIL(root_service->get_common_rpc_proxy().to(GCTX.self_addr())
        .timeout(rpc_timeout).drop_table(drop_table_arg, drop_table_res))) {
      LOG_WARN("drop table failed", K(ret), K(rpc_timeout), K(drop_table_arg));
    } else {
      already_cleanuped = true;
    }
  }
  if (OB_SUCC(ret) && already_cleanuped) {
    if (OB_FAIL(cleanup())) {
      LOG_WARN("clean up failed", K(ret));
    }
  }
  return ret;
}

int ObRecoverRestoreTableTask::check_health()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret));
  } else if (!root_service->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service, do not need retry", K(ret), K(object_id_), K(target_object_id_));
    need_retry_ = false;
  } else if (OB_FAIL(ObDDLUtil::check_tenant_status_normal(&root_service->get_sql_proxy(), tenant_id_))) {
    // switch to build failed if the source tenant is been dropped,
    // in order to remove the destination tenant's persistent task record.
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      const ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
      const ObDDLTaskStatus new_status = ObDDLTaskStatus::FAIL;
      int tmp_ret = switch_status(new_status, false, ret);
      LOG_INFO("switch status to build_failed", K(ret), K(tmp_ret), K_(task_status), K(old_status), K(new_status));
      ret = OB_SUCCESS;
    } else if (OB_STANDBY_READ_ONLY == ret) {
      // do not care about the role of the source tenant is expected.
      if (OB_FAIL(ObDDLRedefinitionTask::check_health())) {
        LOG_WARN("check health failed", K(ret), K_(task_status));
      }
    } else {
      LOG_WARN("check tenant status normal failed", K(ret), K_(tenant_id));
    }
  } else if (OB_FAIL(ObDDLRedefinitionTask::check_health())) {
    LOG_WARN("check health failed", K(ret), K_(task_status));
  }
  return ret;
}

int ObRecoverRestoreTableTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoverRestoreTableTask has not been inited", K(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("check task health failed", K(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    switch(task_status_) {
      case ObDDLTaskStatus::PREPARE:
        if (OB_FAIL(prepare(ObDDLTaskStatus::OBTAIN_SNAPSHOT))) {
          LOG_WARN("fail to prepare table redefinition task", K(ret));
        }
        break;
      case ObDDLTaskStatus::OBTAIN_SNAPSHOT:
        if (OB_FAIL(obtain_snapshot(ObDDLTaskStatus::REDEFINITION))) {
          LOG_WARN("fail to lock table", K(ret));
        }
        break;
      case ObDDLTaskStatus::REDEFINITION:
        if (OB_FAIL(wait_data_complement(ObDDLTaskStatus::COPY_TABLE_DEPENDENT_OBJECTS))) {
          LOG_WARN("fail to do table redefinition", K(ret));
        }
        break;
      case ObDDLTaskStatus::COPY_TABLE_DEPENDENT_OBJECTS:
        if (OB_FAIL(copy_table_dependent_objects(ObDDLTaskStatus::TAKE_EFFECT))) {
          LOG_WARN("fail to copy table dependent objects", K(ret));
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
