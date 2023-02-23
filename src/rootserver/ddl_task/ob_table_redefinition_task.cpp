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
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ddl_task/ob_ddl_redefinition_task.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "observer/ob_server_event_history_table_operator.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;

ObTableRedefinitionTask::ObTableRedefinitionTask()
  : ObDDLRedefinitionTask(), has_rebuild_index_(false), has_rebuild_constraint_(false), has_rebuild_foreign_key_(false), allocator_(lib::ObLabel("RedefTask"))
{
}

ObTableRedefinitionTask::~ObTableRedefinitionTask()
{
}

int ObTableRedefinitionTask::init(const uint64_t tenant_id, const int64_t task_id, const share::ObDDLType &ddl_type,
    const int64_t data_table_id, const int64_t dest_table_id, const int64_t schema_version, const int64_t parallelism,
    const ObAlterTableArg &alter_table_arg, const int64_t task_status, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableRedefinitionTask has already been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == data_table_id || OB_INVALID_ID == dest_table_id || schema_version <= 0 || task_status < ObDDLTaskStatus::PREPARE
      || task_status > ObDDLTaskStatus::SUCCESS || snapshot_version < 0 || (snapshot_version > 0 && task_status < ObDDLTaskStatus::WAIT_TRANS_END))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(data_table_id), K(dest_table_id), K(schema_version), K(task_status), K(snapshot_version));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, alter_table_arg, alter_table_arg_))) {
    LOG_WARN("deep copy alter table arg failed", K(ret));
  } else if (OB_FAIL(set_ddl_stmt_str(alter_table_arg_.ddl_stmt_str_))) {
    LOG_WARN("set ddl stmt str failed", K(ret));
  } else {
    task_type_ = ddl_type;
    object_id_ = data_table_id;
    target_object_id_ = dest_table_id;
    schema_version_ = schema_version;
    task_status_ = static_cast<ObDDLTaskStatus>(task_status);
    snapshot_version_ = snapshot_version;
    tenant_id_ = tenant_id;
    task_version_ = OB_TABLE_REDEFINITION_TASK_VERSION;
    task_id_ = task_id;
    parallelism_ = parallelism;
    cluster_version_ = GET_MIN_CLUSTER_VERSION();
    alter_table_arg_.exec_tenant_id_ = tenant_id_;
    is_inited_ = true;
  }
  return ret;
}

int ObTableRedefinitionTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  const uint64_t data_table_id = task_record.object_id_;
  const uint64_t dest_table_id = task_record.target_object_id_;
  const int64_t schema_version = task_record.schema_version_;
  int64_t pos = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableRedefinitionTask has already been inited", K(ret));
  } else if (!task_record.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_FAIL(deserlize_params_from_message(task_record.message_.ptr(), task_record.message_.length(), pos))) {
    LOG_WARN("deserialize params from message failed", K(ret));
  } else if (OB_FAIL(set_ddl_stmt_str(task_record.ddl_stmt_str_))) {
    LOG_WARN("set ddl stmt str failed", K(ret));
  } else {
    task_id_ = task_record.task_id_;
    task_type_ = task_record.ddl_type_;
    object_id_ = data_table_id;
    target_object_id_ = dest_table_id;
    schema_version_ = schema_version;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    snapshot_version_ = task_record.snapshot_version_;
    execution_id_ = task_record.execution_id_;
    tenant_id_ = task_record.tenant_id_;
    ret_code_ = task_record.ret_code_;
    alter_table_arg_.exec_tenant_id_ = tenant_id_;
    is_inited_ = true;
  }
  return ret;
}

int ObTableRedefinitionTask::update_complete_sstable_job_status(const common::ObTabletID &tablet_id,
                                                                const int64_t snapshot_version,
                                                                const int64_t execution_id,
                                                                const int ret_code)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);
  UNUSED(tablet_id);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (ObDDLTaskStatus::CHECK_TABLE_EMPTY == task_status_) {
    check_table_empty_job_ret_code_ = ret_code;
  } else if (OB_UNLIKELY(snapshot_version_ != snapshot_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, snapshot version is not equal", K(ret), K(snapshot_version_), K(snapshot_version));
  } else if (execution_id < execution_id_) {
    LOG_INFO("receive a mismatch execution result, ignore", K(ret_code), K(execution_id), K(execution_id_));
  } else {
    complete_sstable_job_ret_code_ = ret_code;
    LOG_INFO("table redefinition task callback", K(complete_sstable_job_ret_code_));
  }
  return ret;
}

int ObTableRedefinitionTask::send_build_replica_request()
{
  int ret = OB_SUCCESS;
  bool modify_autoinc = false;
  bool use_heap_table_ddl_plan = false;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(check_modify_autoinc(modify_autoinc))) {
    LOG_WARN("failed to check modify autoinc", K(ret));
  } else if (OB_FAIL(check_use_heap_table_ddl_plan(use_heap_table_ddl_plan))) {
    LOG_WARN("fail to check heap table ddl plan", K(ret));
  } else {
    ObSQLMode sql_mode = alter_table_arg_.sql_mode_;
    if (!modify_autoinc) {
      sql_mode = sql_mode | SMO_NO_AUTO_VALUE_ON_ZERO;
    }
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *orig_table_schema = nullptr;
    ObDDLRedefinitionSSTableBuildTask task(
        task_id_,
        tenant_id_,
        object_id_,
        target_object_id_,
        schema_version_,
        snapshot_version_,
        execution_id_,
        sql_mode,
        trace_id_,
        parallelism_,
        use_heap_table_ddl_plan,
        GCTX.root_service_);
    if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, orig_table_schema))) {
      LOG_WARN("failed to get orig table schema", K(ret));
    } else if (OB_FAIL(task.init(*orig_table_schema, alter_table_arg_.alter_table_schema_, alter_table_arg_.tz_info_wrap_))) {
      LOG_WARN("fail to init table redefinition sstable build task", K(ret));
    } else if (OB_FAIL(root_service->submit_ddl_single_replica_build_task(task))) {
      LOG_WARN("fail to submit ddl build single replica", K(ret));
    }
  }
  return ret;
}

int ObTableRedefinitionTask::check_build_replica_timeout()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (build_replica_request_time_ > 0) {
    const int64_t timeout = OB_MAX_DDL_SINGLE_REPLICA_BUILD_TIMEOUT;
    const int64_t current_time = ObTimeUtility::current_time();
    if (build_replica_request_time_ + timeout < current_time) {
      ret = OB_TIMEOUT;
    }
  }
  return ret;
}

int ObTableRedefinitionTask::check_build_replica_end(bool &is_end)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == complete_sstable_job_ret_code_) {
    // not complete
  } else if (OB_SUCCESS != complete_sstable_job_ret_code_) {
    ret_code_ = complete_sstable_job_ret_code_;
    is_end = true;
    LOG_WARN("complete sstable job failed", K(ret_code_), K(object_id_), K(target_object_id_));
    if (is_replica_build_need_retry(ret_code_)) {
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

int ObTableRedefinitionTask::check_use_heap_table_ddl_plan(bool &use_heap_table_ddl_plan)
{
  int ret = OB_SUCCESS;
  use_heap_table_ddl_plan = false;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *target_table_schema = nullptr;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(root_service->get_ddl_service()
                      .get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, target_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(target_object_id_));
  } else if (OB_ISNULL(target_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, table schema must not be nullptr", K(ret), K(target_object_id_));
  } else if (target_table_schema->is_heap_table() && 
    (DDL_ALTER_PARTITION_BY == task_type_ || DDL_DROP_PRIMARY_KEY == task_type_)) {
    use_heap_table_ddl_plan = true;
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

  if (OB_SUCC(ret) && !is_build_replica_end && 0 == build_replica_request_time_) {
    if (OB_FAIL(push_execution_id())) {
      LOG_WARN("failed to push execution id", K(ret));
    } else if (OB_FAIL(send_build_replica_request())) {
      LOG_WARN("fail to send build replica request", K(ret));
    } else {
      build_replica_request_time_ = ObTimeUtility::current_time();
    }
  }
  DEBUG_SYNC(TABLE_REDEFINITION_REPLICA_BUILD);
  if (OB_SUCC(ret) && !is_build_replica_end) {
    if (OB_FAIL(check_build_replica_end(is_build_replica_end))) {
      LOG_WARN("check build replica end failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && !is_build_replica_end) {
    if (OB_FAIL(check_build_replica_timeout())) {
      LOG_WARN("fail to check build replica timeout", K(ret));
    }
  }

  // overwrite ret
  if (is_build_replica_end) {
    ret = complete_sstable_job_ret_code_;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_data_dest_tables_columns_checksum(execution_id_))) {
        LOG_WARN("fail to check the columns checksum of data table and destination table", K(ret));
      }
    }
    if (OB_FAIL(switch_status(next_task_status, ret))) {
      LOG_WARN("fail to switch task status", K(ret));
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
  } else {
    // check if has rebuild index
    if (has_rebuild_index_) {
    } else {
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema *table_schema = nullptr;
      ObSArray<uint64_t> index_ids;
      alter_table_arg_.ddl_task_type_ = share::REBUILD_INDEX_TASK;
      alter_table_arg_.table_id_ = object_id_;
      alter_table_arg_.hidden_table_id_ = target_object_id_;
      if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(target_object_id_));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, table schema must not be nullptr", K(ret), K(target_object_id_));
      } else {
        const common::ObIArray<ObAuxTableMetaInfo> &index_infos = table_schema->get_simple_index_infos();
        if (index_infos.count() > 0) {
          // if there is indexes in new tables, if so, the indexes is already rebuilt in new table
          for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
            if (OB_FAIL(index_ids.push_back(index_infos.at(i).table_id_))) {
              LOG_WARN("push back index id failed", K(ret));
            }
          }
          LOG_INFO("indexes schema are already built", K(index_ids));
        } else {
          // if there is no indexes in new tables, we need to rebuild indexes in new table
          if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(ObDDLUtil::get_ddl_rpc_timeout()).
                execute_ddl_task(alter_table_arg_, index_ids))) {
            LOG_WARN("rebuild hidden table index failed", K(ret));
          }
        }
      }
      DEBUG_SYNC(TABLE_REDEFINITION_COPY_TABLE_INDEXES);
      if (OB_SUCC(ret) && index_ids.count() > 0) {
        ObSchemaGetterGuard new_schema_guard;
        if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, new_schema_guard))) {
          LOG_WARN("failed to refresh schema guard", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); ++i) {
          const uint64_t index_id = index_ids.at(i);	
          const ObTableSchema *index_schema = nullptr;
          ObDDLTaskRecord task_record;
          bool need_rebuild_index = true;
          SMART_VAR(ObCreateIndexArg, create_index_arg) {
            // this create index arg is not valid, only has nls format
            create_index_arg.nls_date_format_ = alter_table_arg_.nls_formats_[0];
            create_index_arg.nls_timestamp_format_ = alter_table_arg_.nls_formats_[1];
            create_index_arg.nls_timestamp_tz_format_ = alter_table_arg_.nls_formats_[2];
            if (OB_FAIL(new_schema_guard.get_table_schema(tenant_id_, index_ids.at(i), index_schema))) {
              LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(index_ids.at(i)));
            } else if (OB_ISNULL(index_schema)) {
              ret = OB_ERR_SYS;
              LOG_WARN("error sys, index schema must not be nullptr", K(ret), K(index_ids.at(i)));
            } else if (index_schema->can_read_index()) {
              // index is already built
              need_rebuild_index = false;
            } else {
              ObCreateDDLTaskParam param(tenant_id_,
                                         ObDDLType::DDL_CREATE_INDEX,
                                         table_schema,
                                         index_schema,
                                         0/*object_id*/,
                                         index_schema->get_schema_version(),
                                         parallelism_ / index_ids.count()/*parallelism*/,
                                         &allocator_,
                                         &create_index_arg,
                                         task_id_);
              if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param, *GCTX.sql_proxy_, task_record))) {
                if (OB_ENTRY_EXIST == ret) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("submit ddl task failed", K(ret));
                }
              } else if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
                LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
              }
            }
            if (OB_SUCC(ret) && need_rebuild_index) {
              const uint64_t task_key = index_ids.at(i);
              DependTaskStatus status;
              status.task_id_ = task_record.task_id_;
              if (OB_FAIL(dependent_task_result_map_.set_refactored(task_key, status))) {
                if (OB_HASH_EXIST == ret) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("set dependent task map failed", K(ret), K(task_key));
                }
              } else {
                LOG_INFO("add build index task", K(task_key));
              }
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
  } else {
    if (has_rebuild_constraint_) {
      // do nothing
    } else {
      ObSArray<uint64_t> constraint_ids;
      ObSArray<uint64_t> new_constraint_ids;
      bool need_rebuild_constraint = true;
      if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(target_object_id_));
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
        if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(ObDDLUtil::get_ddl_rpc_timeout()).
              execute_ddl_task(alter_table_arg_, constraint_ids))) {
          LOG_WARN("rebuild hidden table constraint failed", K(ret));
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
  } else {
    if (has_rebuild_foreign_key_) {
      // do nothing
    } else {
      if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, target_object_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(target_object_id_));
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
          if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(ObDDLUtil::get_ddl_rpc_timeout()).
                execute_ddl_task(alter_table_arg_, fk_ids))) {
            LOG_WARN("rebuild hidden table constraint failed", K(ret));
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
  } else if (!dependent_task_result_map_.created() && OB_FAIL(dependent_task_result_map_.create(MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("DepTasMap")))) {
    LOG_WARN("create dependent task map failed", K(ret));
  } else {
    if (OB_FAIL(copy_table_indexes())) {
      LOG_WARN("copy table indexes failed", K(ret));
    } else if (OB_FAIL(copy_table_constraints())) {
      LOG_WARN("copy table constraints failed", K(ret));
    } else if (OB_FAIL(copy_table_foreign_keys())) {
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
    for (common::hash::ObHashMap<uint64_t, DependTaskStatus>::const_iterator iter = dependent_task_result_map_.begin();
        iter != dependent_task_result_map_.end(); ++iter) {
      const uint64_t task_key = iter->first;
      const int64_t target_object_id = -1;
      const int64_t child_task_id = iter->second.task_id_;
      if (iter->second.ret_code_ == INT64_MAX) {
        // maybe ddl already finish when switching rs
        HEAP_VAR(ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage, error_message) {
          int64_t unused_user_msg_len = 0;
          if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(tenant_id_, child_task_id, target_object_id,
                  unused_addr, false /* is_ddl_retry_task */, *GCTX.sql_proxy_, error_message, unused_user_msg_len))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              LOG_INFO("ddl task not finish", K(task_key), K(child_task_id), K(target_object_id));
            } else {
              LOG_WARN("fail to get ddl error message", K(ret), K(task_key), K(child_task_id), K(target_object_id));
            }
          } else {
            finished_task_cnt++;
            if (error_message.ret_code_ != OB_SUCCESS) {
              ret = error_message.ret_code_;
            }
          }
        }
      } else {
        finished_task_cnt++;
        if (iter->second.ret_code_ != OB_SUCCESS) {
          ret = iter->second.ret_code_;
        }
      }
    }
    if (finished_task_cnt == dependent_task_result_map_.size()) {
      state_finish = true;
    }
  }
  if (state_finish) {
    if (OB_FAIL(switch_status(next_task_status, ret))) {
      LOG_WARN("fail to switch status", K(ret));
    }
  }
  return ret;
}

int ObTableRedefinitionTask::take_effect(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  SERVER_EVENT_ADD("ddl_task", "before_table_redefinition_task_effect",
                   "tenant_id", tenant_id_,
                   "object_id", object_id_,
                   "target_object_id", target_object_id_);
  DEBUG_SYNC(BEFORE_TABLE_REDEFINITION_TASK_EFFECT);
#endif
  ObSArray<uint64_t> objs;
  alter_table_arg_.ddl_task_type_ = share::MAKE_DDL_TAKE_EFFECT_TASK;
  alter_table_arg_.table_id_ = object_id_;
  alter_table_arg_.hidden_table_id_ = target_object_id_;
  // offline ddl is allowed on table with trigger(enable/disable).
  alter_table_arg_.need_rebuild_trigger_ = true;
  ObRootService *root_service = GCTX.root_service_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  bool use_heap_table_ddl_plan = false;
  ObDDLTaskStatus new_status = next_task_status;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema not exist", K(ret), K(target_object_id_));
  } else if (!table_schema->is_user_hidden_table()) {
    LOG_INFO("target schema took effect", K(target_object_id_));
  } else if (OB_FAIL(check_use_heap_table_ddl_plan(use_heap_table_ddl_plan))) {
    LOG_WARN("fail to check heap table ddl plan", K(ret));
  } else if (table_schema->is_heap_table() && !use_heap_table_ddl_plan && OB_FAIL(sync_tablet_autoinc_seq())) {
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
  } else if (OB_FAIL(sync_stats_info())) {
    LOG_WARN("fail to sync stats info", K(ret), K(object_id_), K(target_object_id_));
  } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(ObDDLUtil::get_ddl_rpc_timeout()).
      execute_ddl_task(alter_table_arg_, objs))) {
    LOG_WARN("fail to swap original and hidden table state", K(ret));
    if (OB_TIMEOUT == ret) {
      ret = OB_SUCCESS;
      new_status = ObDDLTaskStatus::TAKE_EFFECT;
    }
  }
  DEBUG_SYNC(TABLE_REDEFINITION_TAKE_EFFECT);
  if (new_status == next_task_status || OB_FAIL(ret)) {
    if (OB_FAIL(switch_status(next_task_status, ret))) {
      LOG_WARN("fail to switch status", K(ret));
    }
  }
  return ret;
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
    switch(task_status_) {
      case ObDDLTaskStatus::PREPARE:
        if (OB_FAIL(prepare(ObDDLTaskStatus::WAIT_TRANS_END))) {
          LOG_WARN("fail to prepare table redefinition task", K(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_TRANS_END:
        if (OB_FAIL(wait_trans_end(wait_trans_ctx_, ObDDLTaskStatus::LOCK_TABLE))) {
          LOG_WARN("fail to wait trans end", K(ret));
        }
        break;
      case ObDDLTaskStatus::LOCK_TABLE:
        if (OB_FAIL(lock_table(ObDDLTaskStatus::CHECK_TABLE_EMPTY))) {
          LOG_WARN("fail to lock table", K(ret));
        }
        break;
      case ObDDLTaskStatus::CHECK_TABLE_EMPTY:
        if (OB_FAIL(check_table_empty(ObDDLTaskStatus::REDEFINITION))) {
          LOG_WARN("fail to check table empty", K(ret));
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
