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

#include "ob_rebuild_index_task.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_ddl_sim_point.h"
#include "rootserver/ob_root_service.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"               // for OTTZ_MGR
#include "share/vector_index/ob_vector_index_util.h"
#include "src/storage/ddl/ob_ddl_lock.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

ObRebuildIndexTask::ObRebuildIndexTask()
  : ObDDLTask(DDL_REBUILD_INDEX), rebuild_index_arg_(), index_build_task_id_(-1), index_drop_task_id_(-1), new_index_id_(OB_INVALID_ID), target_object_name_()
{
}

ObRebuildIndexTask::~ObRebuildIndexTask()
{
}

int ObRebuildIndexTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const share::ObDDLType &ddl_type,
    const uint64_t data_table_id,
    const uint64_t index_table_id,  // domain index table id
    const int64_t schema_version,
    const int64_t parent_task_id,
    const int64_t consumer_group_id,
    const int32_t sub_task_trace_id,
    const int64_t parallelism,
    const uint64_t tenant_data_version,
    const ObTableSchema &index_schema,
    const obrpc::ObRebuildIndexArg &rebuild_index_arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObString tmp_table_name;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0 || OB_INVALID_ID == data_table_id
      || OB_INVALID_ID == index_table_id || schema_version <= 0 || parent_task_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(task_id), K(data_table_id),
                                  K(index_table_id), K(schema_version), K(parent_task_id));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", KR(ret));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_, rebuild_index_arg, rebuild_index_arg_))) {
    LOG_WARN("deep copy drop index arg failed", KR(ret));
  } else if (OB_FALSE_IT(tmp_table_name = index_schema.get_table_name())) {
  } else if (OB_FAIL(ob_write_string(allocator_, tmp_table_name, target_object_name_))) {
    LOG_WARN("Fail to copy parser name ", K(ret), K(target_object_name_), K(tmp_table_name));
  } else {
    tenant_id_ = tenant_id;
    object_id_ = data_table_id;
    target_object_id_ = index_table_id;
    schema_version_ = schema_version;
    task_id_ = task_id;
    task_type_ = ddl_type;
    parent_task_id_ = parent_task_id;
    consumer_group_id_ = consumer_group_id;
    sub_task_trace_id_ = sub_task_trace_id;
    task_version_ = OB_REBUILD_INDEX_TASK_VERSION;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    data_format_version_ = tenant_data_version;
    parallelism_ = parallelism;
    is_inited_ = true;
    ddl_tracing_.open();
  }
  return ret;
}

int ObRebuildIndexTask::init(
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
    task_type_ = task_record.ddl_type_;
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

bool ObRebuildIndexTask::is_valid() const
{
  return is_inited_ && !trace_id_.is_invalid();
}

int ObRebuildIndexTask::prepare(const ObDDLTaskStatus new_status)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(REBUILD_VEC_INDEX_PREPARE);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRebuildIndexTask has not been inited", KR(ret));
  } else if (ObDDLTaskStatus::PREPARE != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_FAIL(switch_status(new_status, true, ret))) {
    LOG_WARN("switch status failed", KR(ret));
  }
  return ret;
}

/*
  Drop new index table on failure, drop old index table on success.
*/
int ObRebuildIndexTask::drop_index_impl()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObDatabaseSchema *database_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  ObSqlString drop_index_sql;
  ObString index_name;
  const ObTableSchema *index_schema = nullptr;
  if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root_service is nullptr", KR(ret));
  } else if (new_index_id_ == OB_INVALID_ID) {
    index_drop_task_id_ = -1; // new index table maybe not build yet, drop nothing
    LOG_INFO("new index table not exist, maybe not build yet", K(ret), K(target_object_name_), K(new_index_id_));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().
                                                  get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, index_schema))) {
    LOG_WARN("get index schema failed", KR(ret), K(target_object_id_));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("index schema is null", KR(ret), K(target_object_id_));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, index_schema->get_database_id(), database_schema))) {
    LOG_WARN("get database schema failed", KR(ret), K(index_schema->get_database_id()));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("get data table schema failed", KR(ret), K(index_schema->get_data_table_id()));
  } else if (OB_UNLIKELY(nullptr == database_schema || nullptr == data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null schema", KR(ret), KP(database_schema), KP(data_table_schema));
  } else {
    // we set the drop_index_arg.index_name_ as index_name is following the reason:
    // 1. In the success process, the index table and the new index table have already swapped names.
    //    At this point, the index_name of the old index that needs to be deleted should be the new index name.
    // 2. In the failure process, the new table needs to be deleted, and the new table and the old table have not swapped names.
    //    At this point, the index_name is also the old table name.
    int64_t ddl_rpc_timeout = 0;
    obrpc::ObDropIndexArg drop_index_arg;
    obrpc::ObDropIndexRes drop_index_res;
    drop_index_arg.is_inner_          = true;                   // send to rs and set is_inner_ is true to submit drop vec index ddl task。RS need get all assistant index table to drop
    drop_index_arg.tenant_id_         = tenant_id_;
    drop_index_arg.exec_tenant_id_    = tenant_id_;
    drop_index_arg.table_id_          = new_index_id_;          // The ID of new table 3
    drop_index_arg.index_table_id_    = target_object_id_;      // The ID of old table 3
    drop_index_arg.index_name_        = target_object_name_;    // The name of old table 3
    drop_index_arg.session_id_        = data_table_schema->get_session_id();
    drop_index_arg.table_name_        = data_table_schema->get_table_name();
    drop_index_arg.database_name_     = database_schema->get_database_name_str();
    drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
    drop_index_arg.is_add_to_scheduler_ = true;
    drop_index_arg.task_id_           = task_id_; // parent task id
    drop_index_arg.is_vec_inner_drop_ = true;
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(index_schema->get_all_part_num() + data_table_schema->get_all_part_num(), ddl_rpc_timeout))) {
      LOG_WARN("failed to get ddl rpc timeout", KR(ret));
    } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DROP_INDEX_RPC_FAILED))) {
      LOG_WARN("ddl sim failure", KR(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(root_service_->get_common_rpc_proxy().timeout(ddl_rpc_timeout).drop_index(drop_index_arg, drop_index_res))) {
      LOG_WARN("drop index failed", KR(ret), K(ddl_rpc_timeout));
    } else {
      index_drop_task_id_ = drop_index_res.task_id_;
      LOG_INFO("success to submit drop vector index task", K(ret),
        K(new_index_id_), K(target_object_name_), K(index_drop_task_id_), K(drop_index_arg));
    }
  }
  return ret;
}

int ObRebuildIndexTask::rebuild_index_impl()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_db_in_recyclebin = false;
  const ObTableSchema *table_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;
  ObArenaAllocator dbms_vector_job_info_allocator;
  dbms_scheduler::ObDBMSSchedJobInfo job_info;
  if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root_service is nullptr", KR(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().
                                                  get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, index_schema))) {
    LOG_WARN("get index schema failed", KR(ret), K(target_object_id_));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("index schema is null", KR(ret), K(target_object_id_));
  } else if (index_schema->is_vec_delta_buffer_type() &&          // only hnsw index here, because ivf not support refresh
             OB_FAIL(ObVectorIndexUtil::get_dbms_vector_job_info(root_service_->get_sql_proxy(), tenant_id_,
                                                                 index_schema->get_table_id(),
                                                                 dbms_vector_job_info_allocator,
                                                                 schema_guard,
                                                                 job_info))) {
    LOG_WARN("fail to get dbms_vector job info", K(ret), K(tenant_id_), K(index_schema->get_table_id()));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_schema->get_data_table_id(), table_schema))) {
    LOG_WARN("get data table schema failed", KR(ret), K(index_schema->get_data_table_id()));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null schema", KR(ret), KP(table_schema));
  } else if (table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not create index of table in recyclebin.", KR(ret), K(table_schema));
  } else if (OB_FAIL(schema_guard.check_database_in_recyclebin(tenant_id_,
                                                               table_schema->get_database_id(),
                                                               is_db_in_recyclebin))) {
    LOG_WARN("check database in recyclebin failed", KR(ret), K(tenant_id_));
  } else if (is_db_in_recyclebin) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("Can not truncate index of db in recyclebin", KR(ret));
  } else {
    // parameters description：
    // 1. create_index_arg.index_name_
    //    The naming convention for creating the new table: assuming new index name ='idx1', the name of the new table 3 will be __idx_{datatable_id}_idx1.
    // 2. create_index_arg.index_table_id_
    //    The ID of the old table 3 is needed to find the old table schema when creating the index, in order to assign the schema to the new table.
    SMART_VAR(obrpc::ObCreateIndexArg, create_index_arg) {
      obrpc::ObAlterTableRes res;
      int64_t ddl_rpc_timeout = 0;
      ObRootService *root_service = GCTX.root_service_;
      create_index_arg.index_type_ = index_schema->get_index_type();
      create_index_arg.index_name_ = rebuild_index_arg_.index_name_;  // new index name was generated at ddl_service of rebuild_vec_index func
      create_index_arg.index_table_id_ = target_object_id_;           // old table 3 index ID;
      create_index_arg.database_name_ = rebuild_index_arg_.database_name_;
      create_index_arg.is_rebuild_index_ = true;
      create_index_arg.tenant_id_ = tenant_id_;
      create_index_arg.exec_tenant_id_ = tenant_id_;
      create_index_arg.table_name_ = table_schema->get_table_name();
      create_index_arg.index_action_type_ = obrpc::ObIndexArg::ADD_INDEX;
      create_index_arg.parallelism_ = parallelism_;
      create_index_arg.is_inner_ = true;  // is ddl task inner task
      create_index_arg.task_id_ = task_id_;
      ObColumnSortItem empty_item;
      create_index_arg.index_using_type_ = USING_BTREE;
      create_index_arg.index_columns_.push_back(empty_item);
      create_index_arg.index_option_.block_size_ = 1;
      create_index_arg.index_option_.index_status_ = INDEX_STATUS_UNAVAILABLE;
      create_index_arg.index_option_.progressive_merge_num_ = 1;
      create_index_arg.vidx_refresh_info_.exec_env_ = job_info.get_exec_env();

      if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, target_object_id_, ddl_rpc_timeout))) {
        LOG_WARN("get ddl rpc timeout failed", K(ret));
      } else if (OB_FAIL(root_service_->get_common_rpc_proxy().timeout(ddl_rpc_timeout).create_index(create_index_arg, res))) {
        LOG_WARN("fail to create vec index", K(ret), K(create_index_arg));
      } else {
        index_build_task_id_ = res.task_id_;  // create vector index task ID
        new_index_id_ = res.index_table_id_;  // new table 3 index ID
        LOG_INFO("success to create rebuild index task", K(ret), K(index_build_task_id_), K(new_index_id_), K(create_index_arg));
      }
    }
  }
  return ret;
}

int ObRebuildIndexTask::create_and_wait_rebuild_task_finish(const ObDDLTaskStatus new_status)
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  DEBUG_SYNC(REBUILD_VEC_INDEX_WAIT_CREATE_NEW_INDEX);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (ObDDLTaskStatus::REBUILD_SCHEMA != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", KR(ret), K(task_status_));
  } else if (-1 == index_build_task_id_ && OB_FAIL(rebuild_index_impl())) {
    LOG_WARN("send drop index rpc failed", KR(ret));
  } else if (OB_FAIL(check_ddl_task_finish(tenant_id_, index_build_task_id_, state_finished))) {
    LOG_WARN("check ddl task finish failed", K(ret), K(index_build_task_id_));
  }
  if (state_finished || OB_FAIL(ret)) {
    (void)switch_status(new_status, true, ret);
    LOG_INFO("rebuild_index_task wait_child_task_finish finished", KR(ret), K(*this));
  }

  return ret;
}

int ObRebuildIndexTask::update_task_message(common::ObISQLClient &proxy)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t pos = 0;
  ObString msg;
  common::ObArenaAllocator allocator("ObVecReBuild");
  const int64_t serialize_param_size = get_serialize_param_size();

  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_param_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(serialize_param_size));
  } else if (OB_FAIL(serialize_params_to_message(buf, serialize_param_size, pos))) {
    LOG_WARN("failed to serialize params to message", KR(ret));
  } else {
    msg.assign(buf, serialize_param_size);
    if (OB_FAIL(ObDDLTaskRecordOperator::update_message(root_service_->get_sql_proxy(), tenant_id_, task_id_, msg))) {
      LOG_WARN("failed to update message", KR(ret));
    }
  }
  return ret;
}

int ObRebuildIndexTask::get_new_index_table_id(
    ObSchemaGetterGuard &schema_guard,
    const int64_t tenant_id,
    const int64_t database_id,
    const int64_t data_table_id,
    const ObString &index_name,
    int64_t &index_id)
{
  int ret = OB_SUCCESS;

  char full_index_name_buf[OB_MAX_TABLE_NAME_LENGTH];
  const ObTableSchema *new_index_schema = nullptr;
  const bool is_index = true;
  ObString new_index_name;
  int64_t pos = 0;
  if (index_name.empty() || tenant_id == OB_INVALID_ID ||
      data_table_id == OB_INVALID_ID || database_id == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument",
      K(ret), K(index_name), K(tenant_id), K(data_table_id), K(database_id));
  } else if (OB_FAIL(databuff_printf(full_index_name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "__idx_%lu_%.*s",
                                     data_table_id,
                                     index_name.length(),
                                     index_name.ptr()))) {
    LOG_WARN("fail to printf current time", K(ret));
  } else if (OB_FALSE_IT(new_index_name.assign_ptr(full_index_name_buf,
                                                   static_cast<int32_t>(pos)))) {
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                   database_id,
                                                   new_index_name,
                                                   is_index,
                                                   new_index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(new_index_name));
  } else if (OB_ISNULL(new_index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(tenant_id), K(database_id), K(new_index_name));
  } else if (!new_index_schema->is_vec_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected, here should be vector index schema", K(ret), K(new_index_schema));
  } else if (new_index_schema->is_unavailable_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected of new index status is unaveliable", KR(ret));
  } else {
    index_id = new_index_schema->get_table_id();
  }
  return ret;
}

int ObRebuildIndexTask::switch_index_name(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_schema = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (ObDDLTaskStatus::SWITCH_INDEX_NAME != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", KR(ret), K(task_status_));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().
                                                  get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, index_schema))) {
    LOG_WARN("get old index schema failed", KR(ret), K(target_object_id_));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("index schema is null", KR(ret), K(target_object_id_));
  } else {
    int64_t rpc_timeout = 0;
    ObRootService *root_service = GCTX.root_service_;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    ObDDLTaskStatus new_status = next_task_status;
    ObSArray<uint64_t> unused_ids;
    const ObString origin_database_name = rebuild_index_arg_.database_name_;
    const ObString origin_table_name = index_schema->get_table_name();
    ObTZMapWrap tz_map_wrap;

    SMART_VAR(obrpc::ObAlterTableArg, alter_table_arg) {
      alter_table_arg.alter_table_schema_.set_tenant_id(tenant_id_);
      alter_table_arg.alter_table_schema_.set_origin_database_name(origin_database_name);
      alter_table_arg.alter_table_schema_.set_origin_table_name(origin_table_name);
      alter_table_arg.ddl_task_type_ = share::SWITCH_VEC_INDEX_NAME_TASK;
      alter_table_arg.table_id_ = target_object_id_; // Old index id, the id of the old table number 3.
      alter_table_arg.hidden_table_id_ = new_index_id_; // New index id, the id of the new table number 3, obtained after rebuilding the index.
      alter_table_arg.task_id_ = task_id_;  // rebuild index task id
      alter_table_arg.tz_info_wrap_.set_tz_info_offset(0);
      alter_table_arg.nls_formats_[ObNLSFormatEnum::NLS_DATE] = ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT;
      alter_table_arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
      alter_table_arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
      alter_table_arg.exec_tenant_id_ = tenant_id_;
      alter_table_arg.compat_mode_ = lib::Worker::CompatMode::MYSQL;
      if (OB_ISNULL(root_service)) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, root service must not be nullptr", K(ret));
      } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_SWITCH_INDEX_NAME_FAILED))) {
        LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
      } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id_, tz_map_wrap))) {
        LOG_WARN("get tenant timezone map failed", K(ret), K(tenant_id_));
      } else if (FALSE_IT(alter_table_arg.set_tz_info_map(tz_map_wrap.get_tz_map()))) {
      } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, target_object_id_, rpc_timeout))) {
        LOG_WARN("get ddl rpc timeout failed", K(ret));
      } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
          execute_ddl_task(alter_table_arg, unused_ids))) {
        LOG_WARN("fail to swap original and hidden table state", K(ret));
      } else {
        LOG_INFO("success to switch index name", K(ret), K(origin_table_name), K(alter_table_arg));
      }
      DEBUG_SYNC(REBUILD_VEC_INDEX_SWITCH_INDEX_NAME);
      if (new_status == next_task_status || OB_FAIL(ret)) {
        if (OB_FAIL(switch_status(next_task_status, true, ret))) {
          LOG_WARN("fail to switch status", K(ret));
        }
      }
    }
    LOG_DEBUG("switch_index_name finish", K(ret), K(task_id_), K(target_object_id_), K(new_index_id_), K(alter_table_arg));
  }
  return ret;
}

int ObRebuildIndexTask::create_and_wait_drop_task_finish(const ObDDLTaskStatus new_status)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(REBUILD_VEC_INDEX_WAIT_DROP_OLD_INDEX);
  // Although the names of the new and old indexes have been swapped, the table ID has not changed, so the old index still needs to be dropped.
  bool state_finished = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (ObDDLTaskStatus::DROP_SCHEMA != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", KR(ret), K(task_status_));
  } else if (-1 == index_drop_task_id_ && OB_FAIL(drop_index_impl())) {
    LOG_WARN("fail to build drop index task", K(ret));
  } else if (-1 == index_drop_task_id_) {
    state_finished = true;
    LOG_INFO("submit drop index task return task_id is -1", K(ret), K(index_drop_task_id_));
  } else if (OB_FAIL(check_ddl_task_finish(tenant_id_, index_drop_task_id_, state_finished))) {
    LOG_WARN("check drop task finish task failed", K(ret), K(state_finished));
  }
  if (state_finished || OB_FAIL(ret)) {
    (void)switch_status(new_status, true, ret);
    LOG_INFO("rebuild_index_task wait_drop_task_finish finished", KR(ret), K(*this));
  }
  return ret;
}

/*
  If the DDL task is completed, the DDL task ID will be reset to zero here.
*/
int ObRebuildIndexTask::check_ddl_task_finish(const int64_t tenant_id, int64_t &child_task_id, bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(child_task_id == OB_INVALID_ID || tenant_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("invalid argument", K(ret), K(child_task_id), K(tenant_id));
  } else {
    int64_t unused_user_msg_len = 0;
    const int64_t target_object_id = -1;
    const ObAddr unused_addr;
    ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
    if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(tenant_id,
                                                                      child_task_id,
                                                                      target_object_id,
                                                                      unused_addr,
                                                                      false /* is_ddl_retry_task */,
                                                                      *GCTX.sql_proxy_,
                                                                      error_message,
                                                                      unused_user_msg_len))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("ddl task not finish", K(ret), K(tenant_id), K(child_task_id), K(task_id_));
      } else {
        LOG_WARN("fail to get ddl error message", K(ret), K(tenant_id), K(child_task_id), K(task_id_));
      }
    } else {
      ret = error_message.ret_code_;
      is_finished = true;
      LOG_INFO("succ to wait task finish", K(ret));
    }
  }
  return ret;
}

int ObRebuildIndexTask::succ()
{
  return cleanup();
}

/*
  1. If the deletion of the old table fails in the above logic,
     then wait for the completion of the old table deletion process and no need to drop new table
  2. If it is not because the deletion of the old table failed, then here triggers the drop task of the new table.
  3. If the names of the old and new tables have already been swapped, then the old table should be deleted in case of failure,
     otherwise the new table should be deleted.
*/
int ObRebuildIndexTask::fail()
{
  int ret = OB_SUCCESS;
  bool is_finished = false;
  if (-1 == index_drop_task_id_ && OB_FAIL(drop_index_impl())) {
    LOG_WARN("drop index impl failed", KR(ret));
  } else if (-1 == index_drop_task_id_ ) {
    is_finished = true;
    LOG_INFO("submit drop index task return task_id is -1", K(ret), K(index_drop_task_id_));
  } else if (OB_FAIL(check_ddl_task_finish(tenant_id_, index_drop_task_id_, is_finished))) {
    LOG_WARN("fail to check drop index task finished", K(ret));
  }
  if (OB_FAIL(ret) || is_finished) {
    if (OB_FAIL(cleanup())) {
      LOG_WARN("cleanup failed", KR(ret));
    }
  }
  return ret;
}

int ObRebuildIndexTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_schema = nullptr;
    ObTableLockOwnerID owner_id;
    ObMySQLTransaction trans;
    const int64_t old_index_table_id = OB_INVALID_ID;
    const int64_t new_index_table_id = OB_INVALID_ID;
    const bool is_global_vector_index = false;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(object_id_));
    } else if (OB_ISNULL(data_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get table schema", K(ret), KPC(data_schema));
    } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), tenant_id_))) {
      LOG_WARN("start transaction failed", K(ret));
    } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE, task_id_))) {
      LOG_WARN("failed to get owner id", K(ret), K(task_id_));
    } else if (OB_FAIL(ObDDLLock::unlock_for_rebuild_index(*data_schema,
                                                old_index_table_id,
                                                new_index_table_id,
                                                is_global_vector_index,
                                                owner_id,
                                                trans))) {
      LOG_WARN("failed to unlock rebuild index ddl", K(ret), K(task_id_));
    }
    if (trans.is_started()) {
      int tmp_ret = trans.end(true/*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", KR(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(root_service_->get_sql_proxy(), tenant_id_, task_id_))) {
    LOG_WARN("delete task record failed", KR(ret), K(task_id_), K(schema_version_));
  } else {
    need_retry_ = false;     // clean succ, stop the task
  }
  if (OB_SUCC(ret) && parent_task_id_ > 0) {
    const ObDDLTaskID parent_task_id(tenant_id_, parent_task_id_);
    root_service_->get_ddl_task_scheduler().on_ddl_task_finish(parent_task_id, get_task_key(), ret_code_, trace_id_);
  }
  LOG_INFO("clean task finished", KR(ret), K(*this));
  return ret;
}

int ObRebuildIndexTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRebuildIndexTask has not been inited", KR(ret));
  } else if (!need_retry()) {
    // task is done
  } else {
    ddl_tracing_.restore_span_hierarchy();
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
      case ObDDLTaskStatus::PREPARE:
        if (OB_FAIL(prepare(REBUILD_SCHEMA))) {
          LOG_WARN("prepare failed", KR(ret));
        }
        break;
      case ObDDLTaskStatus::REBUILD_SCHEMA:
        if (OB_FAIL(create_and_wait_rebuild_task_finish(SWITCH_INDEX_NAME))) {
          LOG_WARN("rebuild index failed", KR(ret));
        }
        break;
      case ObDDLTaskStatus::SWITCH_INDEX_NAME:
        if (OB_FAIL(switch_index_name(DROP_SCHEMA))) {
          LOG_WARN("switch index status failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::DROP_SCHEMA:
        if (OB_FAIL(create_and_wait_drop_task_finish(SUCCESS))) {
          LOG_WARN("switch index status failed", K(ret));
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
    if (OB_FAIL(ret)) {
      add_event_info("rebuild index task process fail");
      LOG_INFO("rebuild index task process fail", "ddl_event_info", ObDDLEventInfo());
    }
  }
  return ret;
}

int ObRebuildIndexTask::deep_copy_index_arg(
    common::ObIAllocator &allocator,
    const obrpc::ObRebuildIndexArg &src_index_arg,
    obrpc::ObRebuildIndexArg &dst_index_arg)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  const int64_t serialize_size = src_index_arg.get_serialize_size();
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", KR(ret), K(serialize_size));
  } else if (OB_FAIL(src_index_arg.serialize(buf, serialize_size, pos))) {
    LOG_WARN("serialize source index arg failed", KR(ret));
  } else if (OB_FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(dst_index_arg.deserialize(buf, serialize_size, pos))) {
    LOG_WARN("deserialize failed", KR(ret));
  }
  if (OB_FAIL(ret) && nullptr != buf) {
    allocator.free(buf);
  }

  return ret;
}

int ObRebuildIndexTask::serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_len, pos))) {
    LOG_WARN("ObDDLTask serialize failed", KR(ret));
  } else if (OB_FAIL(rebuild_index_arg_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize failed", KR(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, index_build_task_id_, index_drop_task_id_, new_index_id_, target_object_name_);
  }
  return ret;
}

int ObRebuildIndexTask::deserialize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  obrpc::ObRebuildIndexArg tmp_rebuild_index_arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), KP(buf), K(data_len));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, data_len, pos))) {
    LOG_WARN("ObDDLTask deserlize failed", KR(ret));
  } else if (OB_FAIL(tmp_rebuild_index_arg.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize failed", KR(ret));
  } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(tenant_id, tmp_rebuild_index_arg))) {
    LOG_WARN("replace user tenant id failed", KR(ret), K(tenant_id), K(tmp_rebuild_index_arg));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_, tmp_rebuild_index_arg, rebuild_index_arg_))) {
    LOG_WARN("deep copy drop index arg failed", KR(ret));
  } else {
    ObString tmp_object_name;
    LST_DO_CODE(OB_UNIS_DECODE, index_build_task_id_, index_drop_task_id_, new_index_id_, tmp_object_name);
    if (OB_FAIL(ob_write_string(allocator_, tmp_object_name, target_object_name_))) {
      LOG_WARN("fail to write string", K(ret), K(tmp_object_name));
    }
  }
  return ret;
}

int64_t ObRebuildIndexTask::get_serialize_param_size() const
{
  int ret = OB_SUCCESS;
  int len = 0;
  len += ObDDLTask::get_serialize_param_size();
  len += rebuild_index_arg_.get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              index_build_task_id_,
              index_drop_task_id_,
              new_index_id_,
              target_object_name_);
  return len;
}
