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
#include "ob_column_redefinition_task.h"
#include "lib/rc/context.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_ddl_checksum.h" 
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ddl_task/ob_ddl_redefinition_task.h"
#include "storage/tablelock/ob_table_lock_service.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObColumnRedefinitionTask::ObColumnRedefinitionTask()
  : ObDDLRedefinitionTask(), sstable_complete_request_time_(0), has_rebuild_index_(false), has_rebuild_constraint_(false), has_rebuild_foreign_key_(false), 
    is_sstable_complete_task_submitted_(false), allocator_(lib::ObLabel("RedefTask"))
{
}

ObColumnRedefinitionTask::~ObColumnRedefinitionTask()
{
}

int ObColumnRedefinitionTask::init(const uint64_t tenant_id, const int64_t task_id, const share::ObDDLType &ddl_type,
    const int64_t data_table_id, const int64_t dest_table_id, const int64_t schema_version, const int64_t parallelism,
    const obrpc::ObAlterTableArg &alter_table_arg, const int64_t task_status, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObColumnRedefinitionTask has already been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == data_table_id || OB_INVALID_ID == dest_table_id || schema_version <= 0 || task_status < ObDDLTaskStatus::PREPARE
      || task_status > ObDDLTaskStatus::SUCCESS || snapshot_version < 0 || (snapshot_version > 0 && task_status < ObDDLTaskStatus::WAIT_TRANS_END))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(data_table_id), K(dest_table_id), K(schema_version), K(task_status), K(snapshot_version));
    LOG_WARN("fail to init task table operator", K(ret));
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
    task_version_ = OB_COLUMN_REDEFINITION_TASK_VERSION;
    task_id_ = task_id;
    parallelism_ = parallelism;
    execution_id_ = 1L;
    cluster_version_ = GET_MIN_CLUSTER_VERSION();
    is_inited_ = true;
  }
  return ret;
}

int ObColumnRedefinitionTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  const uint64_t data_table_id = task_record.object_id_;
  const uint64_t dest_table_id = task_record.target_object_id_;
  const int64_t schema_version = task_record.schema_version_;
  int64_t pos = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObColumnRedefinitionTask has already been inited", K(ret));
  } else if (!task_record.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
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
    is_inited_ = true;
  }
  return ret;
}

int ObColumnRedefinitionTask::wait_data_complement(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  bool is_build_replica_end = false; 
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask is not inited", K(ret));
  } else if (ObDDLTaskStatus::REDEFINITION != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_UNLIKELY(snapshot_version_ <= 0)) {
    is_build_replica_end = true; // switch to fail.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected snapshot", K(ret), KPC(this));
  } else if (!is_sstable_complete_task_submitted_ && OB_FAIL(send_build_single_replica_request())) {
    LOG_WARN("fail to send build single replica request", K(ret));
  } else if (is_sstable_complete_task_submitted_ && OB_FAIL(check_build_single_replica(is_build_replica_end))) {
    LOG_WARN("fail to check build single replica", K(ret), K(is_build_replica_end));
  }
  DEBUG_SYNC(COLUMN_REDEFINITION_REPLICA_BUILD);
  if (is_build_replica_end) {
    ret = complete_sstable_job_ret_code_;
    if (OB_SUCC(ret) && OB_FAIL(check_data_dest_tables_columns_checksum(1/*execution_id*/))) {
      LOG_WARN("fail to check the columns checkum between data table and hidden one", K(ret));
    }
    if (OB_FAIL(switch_status(next_task_status, ret))) {
      LOG_WARN("fail to swith task status", K(ret));
    }
    LOG_INFO("wait data complement finished", K(ret), K(*this));
  }
  return ret;
}

// send the request of complementing data to each primary server through rpc
int ObColumnRedefinitionTask::send_build_single_replica_request()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
  } else {
    ObDDLSingleReplicaExecutorParam param;
    param.tenant_id_ = tenant_id_;
    param.type_ = task_type_;
    param.source_table_id_ = object_id_;
    param.dest_table_id_ = target_object_id_;
    param.schema_version_ = schema_version_;
    param.snapshot_version_ = snapshot_version_;
    param.task_id_ = task_id_;
    param.parallelism_ = alter_table_arg_.parallelism_;
    param.execution_id_ = execution_id_;
    param.cluster_version_ = cluster_version_;
    if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, object_id_, param.source_tablet_ids_))) {
      LOG_WARN("fail to get tablets", K(ret), K(tenant_id_), K(object_id_));
    } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, target_object_id_, param.dest_tablet_ids_))) {
      LOG_WARN("fail to get tablets", K(ret), K(tenant_id_), K(target_object_id_));
    } else if (OB_FAIL(replica_builder_.build(param))) {
      LOG_WARN("fail to send build single replica", K(ret));
    } else {
      LOG_INFO("start to build single replica", K(target_object_id_));
      is_sstable_complete_task_submitted_ = true;
      sstable_complete_request_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

// check whether all leaders have completed the complement task
int ObColumnRedefinitionTask::check_build_single_replica(bool &is_end)
{
  int ret = OB_SUCCESS;
  is_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(replica_builder_.check_build_end(is_end, complete_sstable_job_ret_code_))) {
    LOG_WARN("fail to check build end", K(ret));
  } else if (!is_end) {
    if (sstable_complete_request_time_ + OB_MAX_DDL_SINGLE_REPLICA_BUILD_TIMEOUT < ObTimeUtility::current_time()) {   // timeout, retry
      is_sstable_complete_task_submitted_ = false;
      sstable_complete_request_time_ = 0;
    }
  }
  return ret;
}

// update sstable complement status for all leaders
int ObColumnRedefinitionTask::update_complete_sstable_job_status(const common::ObTabletID &tablet_id,
                                                                 const int64_t snapshot_version,
                                                                 const int64_t execution_id,
                                                                 const int ret_code)
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
  } else if (OB_FAIL(replica_builder_.set_partition_task_status(tablet_id, ret_code))) {
    LOG_WARN("fail to set partition task status", K(ret));
  }
  return ret;
}

// Now, rebuild index table in schema and tablet.
// Next, we only rebuild index in schema and remap new index schema to old tablet by sending RPC(REMAP_INDEXES_AND_TAKE_EFFECT_TASK) to RS.
int ObColumnRedefinitionTask::copy_table_indexes()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
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
      DEBUG_SYNC(COLUMN_REDEFINITION_COPY_TABLE_INDEXES);
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
          SMART_VAR(obrpc::ObCreateIndexArg, create_index_arg) {
            // this create index arg is not valid, only has nls format
            create_index_arg.nls_date_format_ = alter_table_arg_.nls_formats_[0];
            create_index_arg.nls_timestamp_format_ = alter_table_arg_.nls_formats_[1];
            create_index_arg.nls_timestamp_tz_format_ = alter_table_arg_.nls_formats_[2];
            if (OB_FAIL(new_schema_guard.get_table_schema(tenant_id_, index_ids.at(i), index_schema))) {
              LOG_WARN("get table schema failed", K(ret));
            } else if (OB_ISNULL(index_schema)) {
              ret = OB_ERR_SYS;
              LOG_WARN("error sys, index schema must not be nullptr", K(ret), K(tenant_id_), K(index_ids.at(i)));
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
              if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param,
                                                                                       *GCTX.sql_proxy_,
                                                                                       task_record))) {
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
              status.task_id_ = task_record.task_id_; // child task id is used to judge whether child task finish.
              if (OB_FAIL(dependent_task_result_map_.set_refactored(task_key, status))) {
                if (OB_HASH_EXIST == ret) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("set dependent task map failed", K(ret), K(task_key));
                }
              } else {
                LOG_INFO("add build index task", K(task_record));
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

// we only need to rebuild constraints in schema without pushing it into ddl scheduler.
int ObColumnRedefinitionTask::copy_table_constraints()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  const ObTableSchema *table_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else {
    if (has_rebuild_constraint_) {
      // do nothing
    } else {
      ObSArray<uint64_t> constraint_ids;
      bool need_rebuild_constraint = false;
      if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(target_object_id_));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, table schema must not be nullptr", K(ret), K(target_object_id_));
      } else if (OB_FAIL(check_need_rebuild_constraint(*table_schema,
                                                       constraint_ids,
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
      DEBUG_SYNC(COLUMN_REDEFINITION_COPY_TABLE_CONSTRAINTS);
      if (OB_SUCC(ret)) {
        has_rebuild_constraint_ = true;
      }
    }
  }
  return ret;
}

int ObColumnRedefinitionTask::copy_table_foreign_keys()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
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
      } else if (table_schema->get_simple_foreign_key_info_array().empty()) {
        ObSArray<uint64_t> fk_ids;
        alter_table_arg_.ddl_task_type_ = share::REBUILD_FOREIGN_KEY_TASK;
        alter_table_arg_.table_id_ = object_id_;
        alter_table_arg_.hidden_table_id_ = target_object_id_;
        if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(ObDDLUtil::get_ddl_rpc_timeout()).
              execute_ddl_task(alter_table_arg_, fk_ids))) {
          LOG_WARN("rebuild hidden table constraint failed", K(ret));
        }
        DEBUG_SYNC(COLUMN_REDEFINITION_COPY_TABLE_FOREIGN_KEYS);
        if (OB_SUCC(ret)) {
          has_rebuild_foreign_key_ = true;
        }
      } else {
        has_rebuild_foreign_key_ = true;
      }
    }
  }
  return ret;
}

int ObColumnRedefinitionTask::copy_table_dependent_objects(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  int64_t finished_task_cnt = 0;
  bool state_finish = false;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (!dependent_task_result_map_.created() && OB_FAIL(dependent_task_result_map_.create(MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("DepTasMap")))) {
    LOG_WARN("create dependent task map failed", K(ret));
  } else {
    if (OB_FAIL(copy_table_indexes())) {
      LOG_WARN("fail to copy table indexes", K(ret));
    } else if (OB_FAIL(copy_table_constraints())) {
      LOG_WARN("fail to copy table constraints", K(ret));
    } else if (OB_FAIL(copy_table_foreign_keys())) {
      LOG_WARN("fail to copy table foreign keys", K(ret));
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

int ObColumnRedefinitionTask::take_effect(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  ObSArray<uint64_t> objs;
  alter_table_arg_.ddl_task_type_ = share::MAKE_DDL_TAKE_EFFECT_TASK;
  alter_table_arg_.table_id_ = object_id_;
  alter_table_arg_.hidden_table_id_ = target_object_id_;
  // offline ddl is allowed on table with trigger(enable/disable).
  alter_table_arg_.need_rebuild_trigger_ = true;
  ObRootService *root_service = GCTX.root_service_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObDDLTaskStatus new_status = next_task_status;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
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
  } else if (table_schema->is_heap_table() && OB_FAIL(sync_tablet_autoinc_seq())) {
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
  DEBUG_SYNC(COLUMN_REDEFINITION_TAKE_EFFECT);
  if (new_status == next_task_status || OB_FAIL(ret)) {
    if (OB_FAIL(switch_status(next_task_status, ret))) {
      LOG_WARN("fail to switch status", K(ret));
    }
  }
  return ret;
}

int ObColumnRedefinitionTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("check health failed", K(ret));
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
        if (OB_FAIL(lock_table(ObDDLTaskStatus::REDEFINITION))) {
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
  }
  return ret;
}
