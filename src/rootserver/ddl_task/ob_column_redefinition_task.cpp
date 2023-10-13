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
  : ObDDLRedefinitionTask(ObDDLType::DDL_COLUMN_REDEFINITION), has_rebuild_index_(false), has_rebuild_constraint_(false), has_rebuild_foreign_key_(false),
    allocator_(lib::ObLabel("RedefTask"))
{
}

ObColumnRedefinitionTask::~ObColumnRedefinitionTask()
{
}

int ObColumnRedefinitionTask::init(const uint64_t tenant_id, const int64_t task_id, const share::ObDDLType &ddl_type,
    const int64_t data_table_id, const int64_t dest_table_id, const int64_t schema_version, const int64_t parallelism, const int64_t consumer_group_id,
    const obrpc::ObAlterTableArg &alter_table_arg, const int64_t task_status, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_format_version = 0;
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
  } else if (OB_FAIL(ObShareUtil::fetch_current_data_version(*GCTX.sql_proxy_, tenant_id, tenant_data_format_version))) {
    LOG_WARN("get min data version failed", K(ret), K(tenant_id));
  } else {
    set_gmt_create(ObTimeUtility::current_time());
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
    consumer_group_id_ = consumer_group_id;
    execution_id_ = 1L;
    start_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(init_ddl_task_monitor_info(target_object_id_))) {
      LOG_WARN("init ddl task monitor info failed", K(ret));
    } else {
      dst_tenant_id_ = tenant_id_;
      dst_schema_version_ = schema_version_;
      alter_table_arg_.alter_table_schema_.set_tenant_id(tenant_id_);
      alter_table_arg_.alter_table_schema_.set_schema_version(schema_version_);
      alter_table_arg_.exec_tenant_id_ = dst_tenant_id_;
      data_format_version_ = tenant_data_format_version;
      is_inited_ = true;
      ddl_tracing_.open();
    }
  }
  return ret;
}

int ObColumnRedefinitionTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  const uint64_t data_table_id = task_record.object_id_;
  const uint64_t dest_table_id = task_record.target_object_id_;
  const int64_t schema_version = task_record.schema_version_;
  task_type_ = task_record.ddl_type_;
  int64_t pos = 0;
  const ObTableSchema *data_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObColumnRedefinitionTask has already been inited", K(ret));
  } else if (!task_record.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(deserlize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(), task_record.message_.length(), pos))) {
    LOG_WARN("deserialize params from message failed", K(ret));
  } else if (OB_FAIL(set_ddl_stmt_str(task_record.ddl_stmt_str_))) {
    LOG_WARN("set ddl stmt str failed", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
          task_record.tenant_id_, schema_guard, schema_version))) {
    LOG_WARN("fail to get schema guard", K(ret), K(schema_version));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(task_record.tenant_id_, data_table_id, data_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(data_table_id));
  } else if (OB_ISNULL(data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret), K(data_schema));
  } else {
    task_id_ = task_record.task_id_;
    object_id_ = data_table_id;
    target_object_id_ = dest_table_id;
    schema_version_ = schema_version;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    snapshot_version_ = task_record.snapshot_version_;
    execution_id_ = task_record.execution_id_;
    tenant_id_ = task_record.tenant_id_;
    ret_code_ = task_record.ret_code_;
    start_time_ = ObTimeUtility::current_time();
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    alter_table_arg_.alter_table_schema_.set_tenant_id(tenant_id_);
    alter_table_arg_.alter_table_schema_.set_schema_version(schema_version_);
    alter_table_arg_.exec_tenant_id_ = dst_tenant_id_;
    if (OB_FAIL(init_ddl_task_monitor_info(target_object_id_))) {
      LOG_WARN("init ddl task monitor info failed", K(ret));
    } else {
      is_inited_ = true;

      // set up span during recover task
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}


// update sstable complement status for all leaders
int ObColumnRedefinitionTask::update_complete_sstable_job_status(const common::ObTabletID &tablet_id,
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
    const int64_t MAX_ACTIVE_TASK_CNT = 1;
    int64_t active_task_cnt = 0;
    // check if has rebuild index
    if (has_rebuild_index_) {
    } else if (OB_FAIL(ObDDLTaskRecordOperator::get_create_index_task_cnt(GCTX.root_service_->get_sql_proxy(), tenant_id_, target_object_id_, active_task_cnt))) {
      LOG_WARN("failed to check index task cnt", K(ret));
    } else if (active_task_cnt >= MAX_ACTIVE_TASK_CNT) {
      ret = OB_EAGAIN;
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
          int64_t rpc_timeout = 0;
          int64_t all_tablet_count = 0;
          if (OB_FAIL(get_orig_all_index_tablet_count(schema_guard, all_tablet_count))) {
            LOG_WARN("get all tablet count failed", K(ret));
          } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(all_tablet_count, rpc_timeout))) {
            LOG_WARN("get ddl rpc timeout failed", K(ret));
          } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
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
            } else if (is_final_index_status(index_schema->get_index_status())) {
              // index status is final
              need_rebuild_index = false;
              LOG_INFO("index status is final", K(ret), K(task_id_), K(index_id), K(need_rebuild_index));
            } else if (active_task_cnt >= MAX_ACTIVE_TASK_CNT) {
              ret = OB_EAGAIN;
            } else {
              create_index_arg.index_type_ = index_schema->get_index_type();
              ObCreateDDLTaskParam param(tenant_id_,
                                         ObDDLType::DDL_CREATE_INDEX,
                                         table_schema,
                                         index_schema,
                                         0/*object_id*/,
                                         index_schema->get_schema_version(),
                                         parallelism_,
                                         consumer_group_id_,
                                         &allocator_,
                                         &create_index_arg,
                                         task_id_);
              if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param,
                                                                                       *GCTX.sql_proxy_,
                                                                                       task_record))) {
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
            if (OB_SUCC(ret) && need_rebuild_index) {
              TCWLockGuard guard(lock_);
              const uint64_t task_key = index_ids.at(i);
              DependTaskStatus status;
              status.task_id_ = task_record.task_id_; // child task id is used to judge whether child task finish.
              if (OB_FAIL(dependent_task_result_map_.get_refactored(task_key, status))) {
                if (OB_HASH_NOT_EXIST != ret) {
                  LOG_WARN("get from dependent task map failed", K(ret));
                } else if (OB_FAIL(dependent_task_result_map_.set_refactored(task_key, status))) {
                  LOG_WARN("set dependent task map failed", K(ret), K(task_key));
                }
              }
              LOG_INFO("add build index task", K(ret), K(task_key), K(status));
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
  int64_t rpc_timeout = 0;
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
        if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, target_object_id_, rpc_timeout))) {
          LOG_WARN("get ddl rpc timeout failed", K(ret));
        } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
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
  int64_t rpc_timeout = 0;
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
        if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, target_object_id_, rpc_timeout))) {
          LOG_WARN("get ddl rpc timeout failed", K(ret));
        } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
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

int ObColumnRedefinitionTask::serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_len, pos))) {
    LOG_WARN("ObDDLTask serialize failed", K(ret));
  } else if (OB_FAIL(alter_table_arg_.serialize(buf, buf_len, pos))) {
    LOG_WARN("alter_table_arg_ serialize failed", K(ret));
  }
  return ret;
}

int ObColumnRedefinitionTask::deserlize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  obrpc::ObAlterTableArg tmp_arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(tenant_id), K(data_len));
  } else if (OB_FAIL(ObDDLTask::deserlize_params_from_message(tenant_id, buf, data_len, pos))) {
    LOG_WARN("ObDDLTask deserlize failed", K(ret));
  } else if (OB_FAIL(tmp_arg.deserialize(buf, data_len, pos))) {
    LOG_WARN("serialize table failed", K(ret));
  } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(task_type_, tenant_id, tmp_arg))) {
    LOG_WARN("replace user tenant id failed", K(ret), K(tenant_id), K(tmp_arg));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, tmp_arg, alter_table_arg_))) {
    LOG_WARN("deep copy table arg failed", K(ret));
  }
  return ret;
}

int64_t ObColumnRedefinitionTask::get_serialize_param_size() const
{
  return alter_table_arg_.get_serialize_size() + ObDDLTask::get_serialize_param_size();
}

int ObColumnRedefinitionTask::copy_table_dependent_objects(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  int64_t finished_task_cnt = 0;
  bool state_finish = false;
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

int ObColumnRedefinitionTask::take_effect(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = 0;
  ObSArray<uint64_t> objs;
  alter_table_arg_.ddl_task_type_ = share::MAKE_DDL_TAKE_EFFECT_TASK;
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
  } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, target_object_id_, rpc_timeout))) {
    LOG_WARN("get ddl rpc timeout failed", K(ret));
  } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
      execute_ddl_task(alter_table_arg_, objs))) {
    LOG_WARN("fail to swap original and hidden table state", K(ret));
    if (OB_TIMEOUT == ret) {
      ret = OB_SUCCESS;
      new_status = ObDDLTaskStatus::TAKE_EFFECT;
    }
  }
  DEBUG_SYNC(COLUMN_REDEFINITION_TAKE_EFFECT);
  if (new_status == next_task_status || OB_FAIL(ret)) {
    if (OB_FAIL(switch_status(next_task_status, true, ret))) {
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
    ddl_tracing_.restore_span_hierarchy();
    switch(task_status_) {
      case ObDDLTaskStatus::PREPARE:
        if (OB_FAIL(prepare(ObDDLTaskStatus::WAIT_TRANS_END))) {
          LOG_WARN("fail to prepare table redefinition task", K(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_TRANS_END:
        if (OB_FAIL(wait_trans_end(wait_trans_ctx_, ObDDLTaskStatus::OBTAIN_SNAPSHOT))) {
          LOG_WARN("fail to wait trans end", K(ret));
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

int ObColumnRedefinitionTask::collect_longops_stat(ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
  databuff_printf(stat_info_.message_, MAX_LONG_OPS_MESSAGE_LENGTH, pos, "TENANT_ID: %ld, TASK_ID: %ld, ", tenant_id_, task_id_);
  switch(status) {
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
    case ObDDLTaskStatus::REDEFINITION: {
      int64_t row_scanned = 0;
      int64_t row_inserted = 0;
      if (OB_FAIL(replica_builder_.get_progress(row_scanned, row_inserted))) {
        LOG_WARN("failed to gather redefinition stats", K(ret));
      } else if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: REPLICA BUILD, ROW_SCANNED: %ld, ROW_INSERTED: %ld",
                                  row_scanned,
                                  row_inserted))) {
        LOG_WARN("failed to print", K(ret));
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
    case ObDDLTaskStatus::TAKE_EFFECT: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: ENABLE INDEX"))) {
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
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not expected status", K(ret), K(status), K(*this));
      break;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(copy_longops_stat(value))) {
    LOG_WARN("failed to collect common longops stat", K(ret));
  }

  return ret;
}

void ObColumnRedefinitionTask::flt_set_task_span_tag() const
{
  FLT_SET_TAG(ddl_task_id, task_id_, ddl_parent_task_id, parent_task_id_,
              ddl_data_table_id, object_id_, ddl_schema_version, schema_version_,
              ddl_snapshot_version, snapshot_version_, ddl_ret_code, ret_code_);
}

void ObColumnRedefinitionTask::flt_set_status_span_tag() const
{
  switch (task_status_) {
  case ObDDLTaskStatus::PREPARE: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::WAIT_TRANS_END: {
    FLT_SET_TAG(ddl_data_table_id, object_id_, ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::OBTAIN_SNAPSHOT: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::REDEFINITION: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::COPY_TABLE_DEPENDENT_OBJECTS: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::TAKE_EFFECT: {
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
