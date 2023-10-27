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
#include "ob_ddl_redefinition_task.h"
#include "lib/rc/context.h"
#include "rootserver/ddl_task/ob_constraint_task.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ddl_task/ob_modify_autoinc_task.h"
#include "rootserver/ob_root_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_ddl_checksum.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_table_lock_rpc_client.h"
#include "share/scn.h"
#include "pl/sys_package/ob_dbms_stats.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::transaction::tablelock;

ObDDLRedefinitionSSTableBuildTask::ObDDLRedefinitionSSTableBuildTask(
    const int64_t task_id,
    const uint64_t tenant_id,
    const int64_t data_table_id,
    const int64_t dest_table_id,
    const int64_t schema_version,
    const int64_t snapshot_version,
    const int64_t execution_id,
    const int64_t consumer_group_id,
    const ObSQLMode &sql_mode,
    const common::ObCurTraceId::TraceId &trace_id,
    const int64_t parallelism,
    const bool use_heap_table_ddl_plan,
    ObRootService *root_service,
    const common::ObAddr &inner_sql_exec_addr)
  : is_inited_(false), tenant_id_(tenant_id), task_id_(task_id), data_table_id_(data_table_id),
    dest_table_id_(dest_table_id), schema_version_(schema_version), snapshot_version_(snapshot_version),
    execution_id_(execution_id), consumer_group_id_(consumer_group_id), sql_mode_(sql_mode), trace_id_(trace_id),
    parallelism_(parallelism), use_heap_table_ddl_plan_(use_heap_table_ddl_plan), root_service_(root_service),
    inner_sql_exec_addr_(inner_sql_exec_addr)
{
  set_retry_times(0); // do not retry
}

int ObDDLRedefinitionSSTableBuildTask::init(
    const ObTableSchema &orig_table_schema,
    const AlterTableSchema &alter_table_schema,
    const ObTimeZoneInfoWrap &tz_info_wrap)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(tz_info_wrap_.deep_copy(tz_info_wrap))) {
    LOG_WARN("fail to copy time zone info wrap", K(ret), K(tz_info_wrap));
  } else if (OB_FAIL(col_name_map_.init(orig_table_schema, alter_table_schema))) {
    LOG_WARN("failed to init column name map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDDLRedefinitionSSTableBuildTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletID unused_tablet_id;
  ObTraceIdGuard trace_id_guard(trace_id_);
  ObSqlString sql_string;
  ObSchemaGetterGuard schema_guard;
  const ObSysVariableSchema *sys_variable_schema = nullptr;
  ObDDLTaskKey task_key(tenant_id_, dest_table_id_, schema_version_);
  ObDDLTaskInfo info;
  bool oracle_mode = false;
  bool need_exec_new_inner_sql = true;
  const ObTableSchema *data_table_schema = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl redefinition sstable build task not inited", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(data_table_id_));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("fail to check formal guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(
      tenant_id_, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", K(ret), K(tenant_id_));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is NULL", K(ret));
  } else if (OB_FAIL(sys_variable_schema->get_oracle_mode(oracle_mode))) {
    LOG_WARN("get oracle mode failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id_, data_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(data_table_id_));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("error unexpected, table schema must not be nullptr", K(ret), K(tenant_id_), K(data_table_id_));
  } else {
    if (OB_FAIL(ObDDLUtil::generate_build_replica_sql(tenant_id_,
                                                      data_table_id_,
                                                      dest_table_id_,
                                                      data_table_schema->get_schema_version(),
                                                      snapshot_version_,
                                                      execution_id_,
                                                      task_id_,
                                                      parallelism_,
                                                      use_heap_table_ddl_plan_,
                                                      true/*use_schema_version_hint_for_src_table*/,
                                                      &col_name_map_,
                                                      sql_string))) {
      LOG_WARN("fail to generate build replica sql", K(ret));
    } else {
      ObTimeoutCtx timeout_ctx;
      common::ObCommonSqlProxy *user_sql_proxy = nullptr;
      int64_t affected_rows = 0;
      if (oracle_mode) {
        sql_mode_ = SMO_STRICT_ALL_TABLES | SMO_PAD_CHAR_TO_FULL_LENGTH;
      }
      ObSessionParam session_param;
      session_param.sql_mode_ = reinterpret_cast<int64_t *>(&sql_mode_);
      session_param.tz_info_wrap_ = &tz_info_wrap_;
      session_param.ddl_info_.set_is_ddl(true);
      session_param.ddl_info_.set_source_table_hidden(false);
      session_param.ddl_info_.set_dest_table_hidden(true);
      session_param.ddl_info_.set_heap_table_ddl(use_heap_table_ddl_plan_);
      session_param.use_external_session_ = true;  // means session id dispatched by session mgr
      session_param.consumer_group_id_ = consumer_group_id_;

      common::ObAddr *sql_exec_addr = nullptr;
      const int64_t DDL_INNER_SQL_EXECUTE_TIMEOUT = ObDDLUtil::calc_inner_sql_execute_timeout();
      if (inner_sql_exec_addr_.is_valid()) {
        sql_exec_addr = &inner_sql_exec_addr_;
        LOG_INFO("inner sql execute addr" , K(*sql_exec_addr));
      }
      if (oracle_mode) {
        user_sql_proxy = GCTX.ddl_oracle_sql_proxy_;
      } else {
        user_sql_proxy = GCTX.ddl_sql_proxy_;
      }
      LOG_INFO("execute sql" , K(sql_string), K(data_table_id_), K(tenant_id_),
              "is_strict_mode", is_strict_mode(sql_mode_), K(sql_mode_), K(parallelism_), K(DDL_INNER_SQL_EXECUTE_TIMEOUT));
      if (OB_FAIL(timeout_ctx.set_trx_timeout_us(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
        LOG_WARN("set trx timeout failed", K(ret));
      } else if (OB_FAIL(timeout_ctx.set_timeout(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
        LOG_WARN("set timeout failed", K(ret));
      } else {
        if (OB_FAIL(user_sql_proxy->write(tenant_id_, sql_string.ptr(), affected_rows,
                oracle_mode ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE, &session_param, sql_exec_addr))) {
          LOG_WARN("fail to execute build replica sql", K(ret), K(tenant_id_));
        } else if (OB_FAIL(ObCheckTabletDataComplementOp::check_finish_report_checksum(tenant_id_, dest_table_id_, execution_id_, task_id_))) {
          LOG_WARN("fail to check sstable checksum_report_finish",
            K(ret), K(tenant_id_), K(dest_table_id_), K(execution_id_), K(task_id_));
        }
      }
    }
  }
  if (OB_SUCCESS != (tmp_ret = root_service_->get_ddl_scheduler().on_sstable_complement_job_reply(unused_tablet_id, task_key, snapshot_version_, execution_id_, ret, info))) {
    LOG_WARN("fail to finish sstable complement", K(ret));
  }
  return ret;
}

ObAsyncTask *ObDDLRedefinitionSSTableBuildTask::deep_copy(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObDDLRedefinitionSSTableBuildTask *new_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl redefinition sstable build task not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == buf || buf_size < get_deep_copy_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_size), "required_deep_copy_size", get_deep_copy_size());
  } else {
    new_task = new (buf) ObDDLRedefinitionSSTableBuildTask(
        task_id_,
        tenant_id_,
        data_table_id_,
        dest_table_id_,
        schema_version_,
        snapshot_version_,
        execution_id_,
        consumer_group_id_,
        sql_mode_,
        trace_id_,
        parallelism_,
        use_heap_table_ddl_plan_,
        root_service_,
        inner_sql_exec_addr_);
    if (OB_FAIL(new_task->tz_info_wrap_.deep_copy(tz_info_wrap_))) {
      LOG_WARN("failed to copy tz info wrap", K(ret));
    } else if (OB_FAIL(new_task->col_name_map_.assign(col_name_map_))) {
      LOG_WARN("failed to assign column name map", K(ret));
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to init new task", K(ret));
      new_task->~ObDDLRedefinitionSSTableBuildTask();
      new_task = nullptr;
    } else {
      new_task->is_inited_ = true;
    }
  }
  return new_task;
}

int ObDDLRedefinitionTask::prepare(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  }
  // overwrite ret
  if (OB_FAIL(switch_status(next_task_status, true, ret))) {
    LOG_WARN("fail to switch status", K(ret));
  }
  return ret;
}

int ObDDLRedefinitionTask::check_table_empty(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  bool need_check_table_empty = false;
  bool is_check_replica_end = false;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(check_need_check_table_empty(need_check_table_empty))) {
    LOG_WARN("failed to check need check table empty", K(ret));
  } else if (need_check_table_empty) {
    if (!is_check_replica_end && 0 == check_table_empty_job_time_) {
      ObCheckConstraintValidationTask task(dst_tenant_id_, object_id_, -1/*constraint id*/, target_object_id_,
                                           schema_version_, trace_id_, task_id_, true/*check_table_empty*/,
                                           obrpc::ObAlterTableArg::AlterConstraintType::ADD_CONSTRAINT);
      if (OB_FAIL(root_service->submit_ddl_single_replica_build_task(task))) {
        LOG_WARN("submit ddl single replica build task failed", K(ret));
      } else {
        check_table_empty_job_time_ = ObTimeUtility::current_time();
        LOG_INFO("send check constraint request", K(object_id_), K(target_object_id_), K(schema_version_));
      }
    }
    if (OB_SUCC(ret) && !is_check_replica_end) {
      if (OB_FAIL(check_check_table_empty_end(is_check_replica_end))) {
        LOG_WARN("check build replica end failed", K(ret));
      } else if (is_check_replica_end) {
        ret = check_table_empty_job_ret_code_;
      }
    }
  }

  if (OB_FAIL(ret) || is_check_replica_end || !need_check_table_empty) {
    if (OB_FAIL(switch_status(next_task_status, true, ret))) {
      LOG_WARN("fail to switch status", K(ret));
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::hold_snapshot(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  ObSEArray<ObTabletID, 1> tablet_ids;
  SCN snapshot_scn;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *dest_table_schema = nullptr;
  ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(snapshot_version));
  } else if (OB_FAIL(snapshot_scn.convert_for_tx(snapshot_version))) {
    LOG_WARN("failed to convert", K(snapshot_version), K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(object_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, dest_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(target_object_id_));
  } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(dest_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(object_id_), K(target_object_id_), KP(data_table_schema), KP(dest_table_schema));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, object_id_, tablet_ids))) {
    LOG_WARN("failed to get data table snapshot", K(ret));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, target_object_id_, tablet_ids))) {
    LOG_WARN("failed to get dest table snapshot", K(ret));
  } else if (data_table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, data_table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
    LOG_WARN("failed to get data lob meta table snapshot", K(ret));
  } else if (data_table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, data_table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
    LOG_WARN("failed to get data lob piece table snapshot", K(ret));
  } else if (dest_table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, dest_table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
    LOG_WARN("failed to get dest lob meta table snapshot", K(ret));
  } else if (dest_table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, dest_table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
    LOG_WARN("failed to get dest lob piece table snapshot", K(ret));
  } else {
    ObDDLService &ddl_service = root_service->get_ddl_service();
    if (OB_FAIL(ddl_service.get_snapshot_mgr().batch_acquire_snapshot(
            ddl_service.get_sql_proxy(), SNAPSHOT_FOR_DDL, tenant_id_, schema_version_, snapshot_scn, nullptr, tablet_ids))) {
      LOG_WARN("batch acquire snapshot failed", K(ret), K(tablet_ids));
    }
  }
  LOG_INFO("hold snapshot finished", K(ret), K(snapshot_version), K(object_id_), K(target_object_id_), K(schema_version_));
  return ret;
}

int ObDDLRedefinitionTask::release_snapshot(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *dest_table_schema = nullptr;
  ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(object_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, dest_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(target_object_id_));
  } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(dest_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(object_id_), K(target_object_id_), KP(data_table_schema), KP(dest_table_schema));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, object_id_, tablet_ids))) {
    LOG_WARN("failed to get data table snapshot", K(ret));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, target_object_id_, tablet_ids))) {
    LOG_WARN("failed to get dest table snapshot", K(ret));
  } else if (data_table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, data_table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
    LOG_WARN("failed to get data lob meta table snapshot", K(ret));
  } else if (data_table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, data_table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
    LOG_WARN("failed to get data lob piece table snapshot", K(ret));
  } else if (dest_table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, dest_table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
    LOG_WARN("failed to get dest lob meta table snapshot", K(ret));
  } else if (dest_table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, dest_table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
    LOG_WARN("failed to get dest lob piece table snapshot", K(ret));
  } else if (OB_FAIL(batch_release_snapshot(snapshot_version, tablet_ids))) {
    LOG_WARN("failed to release snapshot", K(ret));
  }
  LOG_INFO("release snapshot finished", K(ret), K(snapshot_version), K(object_id_), K(target_object_id_), K(schema_version_));
  return ret;
}

// to hold snapshot, containing data in old table with new schema version.
int ObDDLRedefinitionTask::obtain_snapshot(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDDLTaskStatus new_status = ObDDLTaskStatus::OBTAIN_SNAPSHOT;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (snapshot_version_ > 0 && snapshot_held_) {
    // do nothing, already hold snapshot.
  } else if (!wait_trans_ctx_.is_inited()) {
    if (OB_FAIL(wait_trans_ctx_.init(tenant_id_, object_id_, ObDDLWaitTransEndCtx::WAIT_SCHEMA_TRANS, schema_version_))) {
      LOG_WARN("fail to init wait trans ctx", K(ret));
    }
  }
  // to get snapshot version.
  if (OB_SUCC(ret) && snapshot_version_ <= 0) {
    bool is_trans_end = false;
    const bool need_wait_trans_end = false;
    if (OB_FAIL(wait_trans_ctx_.try_wait(is_trans_end, snapshot_version_, need_wait_trans_end))) {
      LOG_WARN("just to get snapshot rather than wait trans end", K(ret));
    }
  }
  DEBUG_SYNC(DDL_REDEFINITION_HOLD_SNAPSHOT);
  // try hold snapshot
  if (OB_FAIL(ret)) {
  } else if (snapshot_version_ <= 0) {
    // the snapshot version obtained here must be valid.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snapshot version is invalid", K(ret), K(tenant_id_), K(object_id_), K(schema_version_));
  } else if (snapshot_version_ > 0 && !snapshot_held_) {
    if (OB_FAIL(ObDDLTaskRecordOperator::update_snapshot_version(root_service->get_sql_proxy(),
                                                                 tenant_id_,
                                                                 task_id_,
                                                                 snapshot_version_))) {
      LOG_WARN("update snapshot version failed", K(ret), K(task_id_));
    } else if (OB_FAIL(hold_snapshot(snapshot_version_))) {
      if (OB_SNAPSHOT_DISCARDED == ret) {
        snapshot_version_ = 0;
        snapshot_held_ = false;
        wait_trans_ctx_.reset();
      } else {
        LOG_WARN("hold snapshot version failed", K(ret));
      }
    } else {
      snapshot_held_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_SNAPSHOT_DISCARDED == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to obtain snapshot version", K(ret));
    }
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

// list the column type modifications that support to validate the checksum.
bool ObDDLRedefinitionTask::check_can_validate_column_checksum(
    const bool is_oracle_mode,
    const ObColumnSchemaV2 &src_column_schema,
    const ObColumnSchemaV2 &dest_column_schema)
{
  bool can_validate_column_checksum = false;
  ObObjType src_column_type = src_column_schema.get_data_type();
  ObObjType dest_column_type = dest_column_schema.get_data_type();
  ObCollationType src_cs_type = src_column_schema.get_collation_type();
  ObCollationType dest_cs_type = dest_column_schema.get_collation_type();
  if (is_oracle_mode) {
    // to do, add more column types modification,
    if (ObCharType == src_column_type && ob_is_char(dest_column_type, dest_cs_type) && src_cs_type == dest_cs_type) {
      can_validate_column_checksum = true;
    } else if (ObVarcharType == src_column_type && ob_is_varchar(dest_column_type, dest_cs_type) && src_cs_type == dest_cs_type) {
      can_validate_column_checksum = true;
    } else if (ObNCharType == src_column_type && ob_is_nchar(dest_column_type) && src_cs_type == dest_cs_type) {
      can_validate_column_checksum = true;
    } else if (ObNVarchar2Type == src_column_type && ob_is_nvarchar2(dest_column_type) && src_cs_type == dest_cs_type) {
      can_validate_column_checksum = true;
    } else if (ObTimestampNanoType == src_column_type && ObTimestampNanoType == dest_column_type) {
      can_validate_column_checksum = true;
    } else if (ObURowIDType == src_column_type && ObURowIDType == dest_column_type) {
      can_validate_column_checksum = true;
    }
  } else {
    // to do, add more column types modification, bigint->int->mediumint->smallint for example.
    if (!src_column_schema.is_autoincrement() && dest_column_schema.is_autoincrement()) {
      // modify to auto increment can lead to data change, cannot verify checksum
      can_validate_column_checksum = false;
    } else if (ObIntTC == ob_obj_type_class(src_column_type) && ObIntTC == ob_obj_type_class(dest_column_type)) {
      can_validate_column_checksum = true;
    } else if (ObUIntTC == ob_obj_type_class(src_column_type) && ObUIntTC == ob_obj_type_class(dest_column_type)) {
      can_validate_column_checksum = true;
    } else if (ObMediumIntType == src_column_type && ObInt32Type == dest_column_type) {
      can_validate_column_checksum = true;
    } else if (ObCharType == src_column_type && ob_is_char(dest_column_type, dest_cs_type) && src_cs_type == dest_cs_type) {
      can_validate_column_checksum = true;
    } else if (ObVarcharType == src_column_type && ob_is_varchar(dest_column_type, dest_cs_type) && src_cs_type == dest_cs_type) {
      can_validate_column_checksum = true;
    }
  }
  return can_validate_column_checksum;
}

// Find the columns that need to validate the checksum, and put their id into validate_checksum_columns_id,
// which maps from old column id to new column id. Note that column ids are different in new hidden table.
int ObDDLRedefinitionTask::get_validate_checksum_columns_id(const ObTableSchema &data_table_schema,
  const ObTableSchema &dest_table_schema, hash::ObHashMap<uint64_t, uint64_t> &validate_checksum_columns_id)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_FAIL(alter_table_arg_.alter_table_schema_.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check if oracle mode failed", K(ret), K(object_id_), "dest_table_id", target_object_id_);
  } else {
    ObSQLMode sql_mode = alter_table_arg_.sql_mode_;
    if (is_oracle_mode) {
      sql_mode = SMO_STRICT_ALL_TABLES;
    } else {
      sql_mode = sql_mode & (~SMO_PAD_CHAR_TO_FULL_LENGTH);
    }
    ObArray<uint64_t> column_ids;
    ObColumnNameMap col_name_map;
    if (OB_FAIL(data_table_schema.get_column_ids(column_ids))) {
      LOG_WARN("get column ids failed", K(ret), K(object_id_));
    } else if (OB_FAIL(col_name_map.init(data_table_schema, alter_table_arg_.alter_table_schema_))) {
      LOG_WARN("failed to build column name map", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      const uint64_t cur_column_id = column_ids.at(i);
      ObString dest_column_name;
      const ObColumnSchemaV2 *cur_column_schema = data_table_schema.get_column_schema(cur_column_id);
      const ObColumnSchemaV2 *dest_column_schema = NULL;
      if (OB_ISNULL(cur_column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current column schema is null", K(ret), "data_table_id", object_id_, K(cur_column_id));
      } else if (OB_SUCCESS == (col_name_map.get(cur_column_schema->get_column_name_str(), dest_column_name))) {
        dest_column_schema = dest_table_schema.get_column_schema(dest_column_name);
      }
      if (OB_FAIL(ret)) {
      } else if (cur_column_schema->is_hidden_pk_column_id(cur_column_id) || cur_column_schema->is_generated_column()) {
        // do nothing, notice that the destination column schema of hidden pk is null while adding primary key for no primary key table;
      } else if (nullptr == dest_column_schema) {
        if (DDL_DROP_COLUMN == task_type_ || DDL_COLUMN_REDEFINITION == task_type_
            || DDL_TABLE_REDEFINITION == task_type_ || DDL_ALTER_PARTITION_BY == task_type_) {
          // column does not exist due to drop column op.
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest column schema is null", K(ret), K(task_type_), "dest_table_id", target_object_id_, "dest_column_id", cur_column_id);
        }
      } else if (is_lob_storage(dest_column_schema->get_data_type())) {
        // ignore to validate the checksum of the dest column that may be LOB.
        // the checksum of dest column is calculated based on the LOB index if the dest column is LOB.
      } else {
        // For modification on primary key, partiiton key, column type, ..., we should consider two cases of checksum validation.
        // 1. the dest column is same as src one(column type, len, precision, scale,...);
        // 2. strict mode and dest_column_type != src_column_type, check_can_validate_column_checksum(src_column_type, dest_column_type) = true;
        if (!(!cur_column_schema->is_autoincrement() && dest_column_schema->is_autoincrement())
          && cur_column_schema->get_data_type() == dest_column_schema->get_data_type()
          && cur_column_schema->get_data_length() == dest_column_schema->get_data_length()
          && cur_column_schema->get_data_precision() == dest_column_schema->get_data_precision()
          && cur_column_schema->get_data_scale() == dest_column_schema->get_data_scale()
          && cur_column_schema->get_encoding_type() == dest_column_schema->get_encoding_type()
          && cur_column_schema->get_collation_type() == dest_column_schema->get_collation_type()) {
          // some special cases that ignores to check, including:
          // 1. all set/enum type, but diffenent order of extended_type_info.
          if (ob_is_enum_or_set_type(cur_column_schema->get_data_type()) &&
            !is_array_equal(cur_column_schema->get_extended_type_info(), dest_column_schema->get_extended_type_info())) {
            // ignore to check the checksum.
          } else if (OB_FAIL(validate_checksum_columns_id.set_refactored(cur_column_id, dest_column_schema->get_column_id()))) {
            LOG_WARN("fail to append the column to validate the checksum", K(cur_column_id), K(ret));
          } else {
            LOG_INFO("succeed to append the column to validate the checksum", K(ret), K(cur_column_id));
          }
        } else if ((cur_column_schema->get_data_type() == dest_column_schema->get_data_type()) &&
          (cur_column_schema->get_encoding_type() != dest_column_schema->get_encoding_type() ||
          cur_column_schema->get_collation_type() != dest_column_schema->get_collation_type())) {
            // do not validate the column checksum if encoding type and collation type change;
        } else if (is_strict_mode(sql_mode) && check_can_validate_column_checksum(is_oracle_mode, *cur_column_schema, *dest_column_schema)) {
          if (OB_FAIL(validate_checksum_columns_id.set_refactored(cur_column_id, dest_column_schema->get_column_id()))) {
            LOG_WARN("fail to append the column to validate the checksum", K(ret), K(is_oracle_mode), K(is_strict_mode(sql_mode)),
            K(cur_column_schema->get_data_type()), K(dest_column_schema->get_data_type()),
            K(cur_column_schema->get_data_length()), K(dest_column_schema->get_data_length()));
          } else {
            LOG_INFO("succeed to append the column to validate the checksum", K(is_oracle_mode), K(is_strict_mode(sql_mode)),
            K(cur_column_schema->get_data_type()), K(dest_column_schema->get_data_type()),
            K(cur_column_schema->get_data_length()), K(dest_column_schema->get_data_length()));
          }
        } else {
            // do nothing, ignore to validate the checksum of this column.
        }
      }
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::wait_data_complement(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  bool is_build_replica_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask is not inited", K(ret));
  } else if (ObDDLTaskStatus::REDEFINITION != task_status_) {
    ret = OB_TASK_EXPIRED;
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
    ret = OB_SUCC(ret) ? complete_sstable_job_ret_code_ : ret;
    if (OB_SUCC(ret) && OB_FAIL(check_data_dest_tables_columns_checksum(get_execution_id()))) {
      LOG_WARN("fail to check the columns checkum between data table and hidden one", K(ret));
    }
    if (OB_FAIL(switch_status(next_task_status, true, ret))) {
      LOG_WARN("fail to swith task status", K(ret));
    }
    LOG_INFO("wait data complement finished", K(ret), K(*this));
  }
  return ret;
}

int ObDDLRedefinitionTask::send_build_single_replica_request()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
  } else {
    ObDDLSingleReplicaExecutorParam param;
    param.tenant_id_ = tenant_id_;
    param.dest_tenant_id_ = dst_tenant_id_;
    param.type_ = task_type_;
    param.source_table_id_ = object_id_;
    param.dest_table_id_ = target_object_id_;
    param.schema_version_ = schema_version_;
    param.dest_schema_version_ = dst_schema_version_;
    param.snapshot_version_ = snapshot_version_;
    param.task_id_ = task_id_;
    param.parallelism_ = std::max(alter_table_arg_.parallelism_, 1L);
    param.execution_id_ = execution_id_;
    param.data_format_version_ = data_format_version_;
    param.consumer_group_id_ = alter_table_arg_.consumer_group_id_;
    if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, object_id_, param.source_tablet_ids_))) {
      LOG_WARN("fail to get tablets", K(ret), K(tenant_id_), K(object_id_));
    } else if (OB_FAIL(ObDDLUtil::get_tablets(dst_tenant_id_, target_object_id_, param.dest_tablet_ids_))) {
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
int ObDDLRedefinitionTask::check_build_single_replica(bool &is_end)
{
  int ret = OB_SUCCESS;
  is_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(replica_builder_.check_build_end(is_end, complete_sstable_job_ret_code_))) {
    LOG_WARN("fail to check build end", K(ret));
  } else if (!is_end) {
    if (sstable_complete_request_time_ + ObDDLUtil::calc_inner_sql_execute_timeout() < ObTimeUtility::current_time()) {   // timeout, retry
      is_sstable_complete_task_submitted_ = false;
      sstable_complete_request_time_ = 0;
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::check_data_dest_tables_columns_checksum(const int64_t execution_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard hold_buf_src_tenant_schema_guard;
  ObSchemaGetterGuard hold_buf_dst_tenant_schema_guard;
  ObSchemaGetterGuard *src_tenant_schema_guard = nullptr;
  ObSchemaGetterGuard *dst_tenant_schema_guard = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *dest_table_schema = nullptr;
  hash::ObHashMap<uint64_t, uint64_t> validate_checksum_columns_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(ObDDLUtil::get_tenant_schema_guard(tenant_id_, dst_tenant_id_,
      hold_buf_src_tenant_schema_guard, hold_buf_dst_tenant_schema_guard,
      src_tenant_schema_guard, dst_tenant_schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_), K(dst_tenant_id_));
  } else if (OB_FAIL(src_tenant_schema_guard->get_table_schema(tenant_id_, object_id_, data_table_schema))) {
    LOG_WARN("get data table schema failed", K(ret), K(tenant_id_), K(object_id_));
  } else if (OB_FAIL(dst_tenant_schema_guard->get_table_schema(dst_tenant_id_, target_object_id_, dest_table_schema))) {
    LOG_WARN("get data table schema failed", K(ret), K(dst_tenant_id_), K(target_object_id_));
  } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(dest_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_INFO("table is not exist", K(ret), K(object_id_), K(target_object_id_), KP(data_table_schema), KP(dest_table_schema));
  } else if (OB_FAIL(validate_checksum_columns_id.create(OB_MAX_COLUMN_NUMBER / 2, lib::ObLabel("DDLRedefTmp")))) {
    LOG_WARN("fail to create validate_checksum_columns_id set", K(ret));
  } else if (OB_FAIL(get_validate_checksum_columns_id(*data_table_schema, *dest_table_schema, validate_checksum_columns_id))) {
    LOG_WARN("fail to get columns id wvalidate the checksum", K(ret));
  } else {
    ObSqlString sql;
    hash::ObHashMap<int64_t, int64_t> data_table_column_checksums;
    hash::ObHashMap<int64_t, int64_t> dest_table_column_checksums;
    if (OB_FAIL(data_table_column_checksums.create(OB_MAX_COLUMN_NUMBER / 2, ObModIds::OB_CHECKSUM_CHECKER))) {
      LOG_WARN("fail to create datatable column checksum map", K(ret));
    } else if (OB_FAIL(dest_table_column_checksums.create(OB_MAX_COLUMN_NUMBER / 2, ObModIds::OB_CHECKSUM_CHECKER))) {
      LOG_WARN("fail to create desttable column checksum map", K(ret));
    } else if (OB_UNLIKELY(0 > execution_id || OB_INVALID_ID == object_id_ || !data_table_column_checksums.created())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(execution_id), K(object_id_), K(data_table_column_checksums.created()));
    } else if (OB_UNLIKELY(OB_INVALID_ID == target_object_id_ || !dest_table_column_checksums.created())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret),  "dest_table_id", target_object_id_, K(dest_table_column_checksums.created()));
    } else if (OB_FAIL(ObDDLChecksumOperator::get_table_column_checksum(dst_tenant_id_, execution_id, object_id_, task_id_, false/*replica build*/, data_table_column_checksums, GCTX.root_service_->get_sql_proxy()))) {
      LOG_WARN("fail to get table column checksum", K(ret), K(dst_tenant_id_), K(execution_id), "table_id", object_id_, K_(task_id), K(data_table_column_checksums.created()), KP(GCTX.root_service_));
    } else if (OB_FAIL(ObDDLChecksumOperator::get_table_column_checksum(dst_tenant_id_, execution_id, target_object_id_, task_id_, false /*replica build*/, dest_table_column_checksums, GCTX.root_service_->get_sql_proxy()))) {
      /**
       * For DDL_RESTORE_TABLE, dst tenant id is differen to source tenant id.
       * Meanwhile, the original tenant is a backup one, can not support write operation,
       * and its' checksum is recorded into to the dest tenant.
      */
      LOG_WARN("fail to get table column checksum", K(ret), K(dst_tenant_id_), K(execution_id), "table_id", target_object_id_, K_(task_id), K(dest_table_column_checksums.created()), KP(GCTX.root_service_));
    } else {
      uint64_t dest_column_id = 0;
      for (hash::ObHashMap<int64_t, int64_t>::const_iterator iter = data_table_column_checksums.begin();
          OB_SUCC(ret) && iter != data_table_column_checksums.end(); ++iter) {
        if (OB_FAIL(validate_checksum_columns_id.get_refactored(iter->first, dest_column_id))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("failed to get refactored", K(ret));
          } else {
            ret = OB_SUCCESS;
            LOG_INFO("ignore to validate the checksum of this column", "column_id", iter->first);
          }
        } else {
          int64_t dest_table_column_checksum = 0;
          if (OB_FAIL(dest_table_column_checksums.get_refactored(dest_column_id, dest_table_column_checksum))) {
            LOG_WARN("fail to get data table column checksum", K(ret), "column_id", iter->first,
            "column_name", data_table_schema->get_column_schema(iter->first)->get_column_name());
          } else if (dest_table_column_checksum == iter->second) {
            LOG_INFO("column checksum is equal", K(ret), "column_id", iter->first,  "column_name", data_table_schema->get_column_schema(iter->first)->get_column_name(),
            K(dest_table_column_checksum), "data_table_column_checksum", iter->second);
          } else {
            ret = OB_CHECKSUM_ERROR;
            LOG_WARN("column checksum is not equal", K(ret), K(object_id_), "dest_table_id", target_object_id_, "column_id", iter->first,
            "column_name", data_table_schema->get_column_schema(iter->first)->get_column_name(), K(dest_table_column_checksum), "data_table_column_checksum", iter->second);
          }
        }
      }
    }
    if (data_table_column_checksums.created()) {
      data_table_column_checksums.destroy();
    }
    if (dest_table_column_checksums.created()) {
      dest_table_column_checksums.destroy();
    }
  }
  if (validate_checksum_columns_id.created()) {
    validate_checksum_columns_id.destroy();
  }
  return ret;
}

int ObDDLRedefinitionTask::add_constraint_ddl_task(const int64_t constraint_id)
{
  int ret = OB_SUCCESS;
  SMART_VAR(obrpc::ObAlterTableArg, alter_table_arg) {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
    const ObConstraint *constraint = nullptr;
    ObRootService *root_service = GCTX.root_service_;
    const ObDatabaseSchema *database_schema = nullptr;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
    } else if (OB_ISNULL(root_service)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, root service must not be nullptr", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == constraint_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(constraint_id));
    } else if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(dst_tenant_id_, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret), K(dst_tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(dst_tenant_id_, target_object_id_, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(dst_tenant_id_), K(target_object_id_));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_SYS;
      LOG_WARN("table schema must not be nullptr", K(ret));
    } else if (OB_FAIL(alter_table_schema.assign(*table_schema))) {
      LOG_WARN("assign table schema failed", K(ret));
    } else if (OB_ISNULL(constraint = table_schema->get_constraint(constraint_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get constraint failed", K(ret), K(constraint_id));
    } else if (OB_FAIL(schema_guard.get_database_schema(dst_tenant_id_, table_schema->get_database_id(), database_schema))) {
      LOG_WARN("get database schema failed", K(ret), K(dst_tenant_id_));
    } else if (OB_FAIL(alter_table_arg.tz_info_wrap_.deep_copy(alter_table_arg_.tz_info_wrap_))) {
      LOG_WARN("deep copy timezone info failed", K(ret));
    } else if (OB_FAIL(alter_table_arg.set_nls_formats(alter_table_arg_.nls_formats_))) {
      LOG_WARN("set nls formats failed", K(ret));
    } else {
      alter_table_arg.exec_tenant_id_ = dst_tenant_id_;
      alter_table_arg.alter_constraint_type_ = obrpc::ObAlterTableArg::ADD_CONSTRAINT;
      alter_table_schema.clear_constraint();
      alter_table_schema.set_origin_database_name(database_schema->get_database_name_str());
      alter_table_schema.set_origin_table_name(table_schema->get_table_name_str());
      if (OB_FAIL(alter_table_schema.add_constraint(*constraint))) {
        LOG_WARN("add constraint failed", K(ret));
      } else {
        const bool need_check = constraint->is_validated();
        if (need_check) {
          //TODO: shanting not null
          ObDDLTaskRecord task_record;
          ObCreateDDLTaskParam param(dst_tenant_id_,
                                     ObDDLType::DDL_CHECK_CONSTRAINT,
                                     table_schema,
                                     nullptr,
                                     constraint_id,
                                     table_schema->get_schema_version(),
                                     0L/*parallelism*/,
                                     consumer_group_id_,
                                     &allocator_,
                                     &alter_table_arg,
                                     task_id_);
          if (OB_FAIL(root_service->get_ddl_task_scheduler().create_ddl_task(param,
                                                                             *GCTX.sql_proxy_,
                                                                             task_record))) {
            if (OB_ENTRY_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("submit ddl task failed", K(ret));
            }
          } else if (OB_FAIL(root_service->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
            LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
          }
          if (OB_SUCC(ret)) {
            TCWLockGuard guard(lock_);
            DependTaskStatus status;
            status.task_id_ = task_record.task_id_; // child task id, which is used to judge child task finish.
            if (OB_FAIL(dependent_task_result_map_.get_refactored(constraint_id, status))) {
              if (OB_HASH_NOT_EXIST != ret) {
                LOG_WARN("get from dependent task map failed", K(ret));
              } else if (OB_FAIL(dependent_task_result_map_.set_refactored(constraint_id, status))) {
                LOG_WARN("set dependent task map failed", K(ret), K(constraint_id));
              }
            }
            LOG_INFO("add constraint task", K(ret), K(constraint_id), K(status));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::add_fk_ddl_task(const int64_t fk_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard hold_buf_src_tenant_schema_guard;
  ObSchemaGetterGuard hold_buf_dst_tenant_schema_guard;
  ObSchemaGetterGuard *src_tenant_schema_guard = nullptr;
  ObSchemaGetterGuard *dst_tenant_schema_guard = nullptr;
  const ObTableSchema *orig_table_schema = nullptr;
  const ObTableSchema *hidden_table_schema = nullptr;
  SMART_VAR(obrpc::ObAlterTableArg, alter_table_arg) {
    AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
    ObConstraint *constraint = nullptr;
    ObRootService *root_service = GCTX.root_service_;
    const ObDatabaseSchema *database_schema = nullptr;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
    } else if (OB_ISNULL(root_service)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, root service must not be nullptr", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == fk_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(fk_id));
    } else if (OB_FAIL(root_service->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, dst_tenant_id_,
        hold_buf_src_tenant_schema_guard, hold_buf_dst_tenant_schema_guard,
        src_tenant_schema_guard, dst_tenant_schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret), K(tenant_id_), K(dst_tenant_id_));
    } else if (OB_FAIL(src_tenant_schema_guard->get_table_schema(tenant_id_, object_id_, orig_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(object_id_));
    } else if (OB_ISNULL(orig_table_schema)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, table schema must not be nullptr", K(ret));
    } else if (OB_FAIL(dst_tenant_schema_guard->get_table_schema(dst_tenant_id_, target_object_id_, hidden_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(dst_tenant_id_), K(target_object_id_));
    } else if (OB_ISNULL(hidden_table_schema)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, table schema must not be nullptr", K(ret), K(dst_tenant_id_), K(target_object_id_));
    } else if (OB_FAIL(alter_table_schema.assign(*hidden_table_schema))) {
      LOG_WARN("assign table schema failed", K(ret));
    } else if (OB_FAIL(src_tenant_schema_guard->get_database_schema(tenant_id_, orig_table_schema->get_database_id(), database_schema))) {
      LOG_WARN("get database schema failed", K(ret), K_(tenant_id));
    } else if (OB_FAIL(alter_table_arg.tz_info_wrap_.deep_copy(alter_table_arg_.tz_info_wrap_))) {
      LOG_WARN("deep copy timezone info failed", K(ret));
    } else if (OB_FAIL(alter_table_arg.set_nls_formats(alter_table_arg_.nls_formats_))) {
      LOG_WARN("set nls formats failed", K(ret));
    } else {
      obrpc::ObCreateForeignKeyArg fk_arg;
      ObForeignKeyInfo fk_info;
      bool found = false;
      const common::ObIArray<ObForeignKeyInfo> &fk_info_array = hidden_table_schema->get_foreign_key_infos();
      alter_table_schema.set_origin_database_name(database_schema->get_database_name_str());
      alter_table_schema.set_origin_table_name(orig_table_schema->get_table_name_str());
      alter_table_arg.table_id_ = object_id_;
      alter_table_arg.hidden_table_id_ = target_object_id_;
      alter_table_arg.exec_tenant_id_ = dst_tenant_id_;
      for (int64_t i = 0; OB_SUCC(ret) && i < fk_info_array.count(); ++i) {
        const ObForeignKeyInfo &tmp_fk_info	= fk_info_array.at(i);
        if (tmp_fk_info.foreign_key_id_ == fk_id) {
          fk_info = tmp_fk_info;
          found = true;
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (!found) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("cannot find foreign key in table", K(ret), K(fk_id), K(fk_info_array));
        } else {
          fk_arg.foreign_key_name_ = fk_info.foreign_key_name_;
          fk_arg.enable_flag_ = fk_info.enable_flag_;
          fk_arg.is_modify_enable_flag_ = fk_info.enable_flag_;
          fk_arg.ref_cst_type_ = fk_info.ref_cst_type_;
          fk_arg.ref_cst_id_ = fk_info.ref_cst_id_;
          fk_arg.validate_flag_ = fk_info.validate_flag_;
          fk_arg.is_modify_validate_flag_ = fk_info.validate_flag_;
          fk_arg.rely_flag_ = fk_info.rely_flag_;
          fk_arg.is_modify_rely_flag_ = fk_info.is_modify_rely_flag_;
          fk_arg.is_modify_fk_state_ = fk_info.is_modify_fk_state_;
          fk_arg.need_validate_data_ = fk_info.validate_flag_;
          fk_arg.name_generated_type_ = fk_info.name_generated_type_;
          ObDDLTaskRecord task_record;
          ObCreateDDLTaskParam param(dst_tenant_id_,
                                     ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT,
                                     hidden_table_schema,
                                     nullptr,
                                     fk_id,
                                     hidden_table_schema->get_schema_version(),
                                     0L/*parallelism*/,
                                     consumer_group_id_,
                                     &allocator_,
                                     &alter_table_arg,
                                     task_id_);
          if (OB_FAIL(alter_table_arg.foreign_key_arg_list_.push_back(fk_arg))) {
            LOG_WARN("push back foreign key arg failed", K(ret));
          } else if (OB_FAIL(root_service->get_ddl_task_scheduler().create_ddl_task(param, *GCTX.sql_proxy_, task_record))) {
            if (OB_ENTRY_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("submit ddl task failed", K(ret));
            }
          } else if (OB_FAIL(root_service->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
            LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
          }
          if (OB_SUCC(ret)) {
            TCWLockGuard guard(lock_);
            DependTaskStatus status;
            status.task_id_ = task_record.task_id_; // child task id, which is used to judge child task finish.
            if (OB_FAIL(dependent_task_result_map_.get_refactored(fk_id, status))) {
              if (OB_HASH_NOT_EXIST != ret) {
                LOG_WARN("get from dependent task map failed", K(ret));
              } else if (OB_FAIL(dependent_task_result_map_.set_refactored(fk_id, status))) {
                LOG_WARN("set dependent task map failed", K(ret), K(fk_id));
              }
            }
            LOG_INFO("add fk task", K(ret), K(fk_id), K(status));

          }
        }
      }
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::on_child_task_finish(
    const uint64_t child_task_key,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_UNLIKELY(common::OB_INVALID_ID == child_task_key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(child_task_key));
  } else {
    TCWLockGuard guard(lock_);
    int64_t org_ret = INT64_MAX;
    DependTaskStatus status;
    if (OB_FAIL(dependent_task_result_map_.get_refactored(child_task_key, status))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      LOG_WARN("get from dependent_task_result_map failed", K(ret), K(child_task_key));
    } else if (org_ret != INT64_MAX && org_ret != ret_code) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, ddl result triggers twice", K(ret), K(child_task_key));
    } else if (FALSE_IT(status.ret_code_ = ret_code)) {
    } else if (OB_FAIL(dependent_task_result_map_.set_refactored(child_task_key, status, true/*overwrite*/))) {
      LOG_WARN("set dependent_task_result_map failed", K(ret), K(child_task_key));
    } else {
      LOG_INFO("child task finish successfully", K(child_task_key));
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::sync_auto_increment_position()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard hold_buf_src_tenant_schema_guard;
  ObSchemaGetterGuard hold_buf_dst_tenant_schema_guard;
  ObSchemaGetterGuard *src_tenant_schema_guard = nullptr;
  ObSchemaGetterGuard *dst_tenant_schema_guard = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *dest_table_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (has_synced_autoincrement_) {
    // do nothing
  } else if (OB_FAIL(ObDDLUtil::get_tenant_schema_guard(tenant_id_, dst_tenant_id_,
      hold_buf_src_tenant_schema_guard, hold_buf_dst_tenant_schema_guard,
      src_tenant_schema_guard, dst_tenant_schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_), K(dst_tenant_id_));
  } else if (OB_FAIL(src_tenant_schema_guard->get_table_schema(tenant_id_, object_id_, data_table_schema))) {
    LOG_WARN("get data table schema failed", K(ret), K(tenant_id_), K(object_id_));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_table_schema is NULL", KR(ret), K_(tenant_id), K_(object_id));
  } else if (OB_FAIL(dst_tenant_schema_guard->get_table_schema(dst_tenant_id_, target_object_id_, dest_table_schema))) {
    LOG_WARN("get data table schema failed", KR(ret), K(dst_tenant_id_), K(target_object_id_));
  } else if (OB_ISNULL(dest_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_table_schema is NULL", K(ret), K_(dst_tenant_id), K_(target_object_id));
  } else {
    ObArray<uint64_t> column_ids;
    if (OB_FAIL(data_table_schema->get_column_ids(column_ids))) {
      LOG_WARN("get column ids failed", K(ret), K(object_id_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      uint64_t cur_column_id = column_ids.at(i);
      const ObColumnSchemaV2 *cur_column_schema = data_table_schema->get_column_schema(cur_column_id);
      const ObColumnSchemaV2 *dst_column_schema = NULL;
      if (OB_ISNULL(cur_column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current column schema is null", K(ret), K(target_object_id_), K(cur_column_id));
      } else if (cur_column_schema->is_autoincrement()
      && nullptr != (dst_column_schema = dest_table_schema->get_column_schema(cur_column_schema->get_column_name()))
      && dst_column_schema->is_autoincrement()) {
        // Worker timeout ts here is default value, i.e., INT64_MAX,
        // which leads to RPC-receiver worker timeout due to overflow when select val from __ALL_AUTO_INCREMENT.
        // More details, refer to comments in
        const int64_t save_timeout_ts = THIS_WORKER.get_timeout_ts();
        THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + max(GCONF.rpc_timeout, 1000 * 1000 * 20L));
        ObAutoincrementService &auto_inc_service = ObAutoincrementService::get_instance();
        uint64_t sequence_value = 0;
        AutoincParam param;
        param.tenant_id_ = dst_tenant_id_;
        param.autoinc_table_id_ = target_object_id_;
        param.autoinc_first_part_num_ = dest_table_schema->get_first_part_num();
        param.autoinc_table_part_num_ = dest_table_schema->get_all_part_num();
        param.autoinc_col_id_ = dst_column_schema->get_column_id();
        param.part_level_ = dest_table_schema->get_part_level();
        ObObjType column_type = dst_column_schema->get_data_type();
        param.autoinc_col_type_ = column_type;
        param.autoinc_desired_count_ = 0;
        param.autoinc_increment_ = 1;
        param.autoinc_offset_ = 1;
        param.auto_increment_cache_size_ = 1; // TODO(shuangcan): should we use the sysvar on session?
        param.autoinc_mode_is_order_ = dest_table_schema->is_order_auto_increment_mode();
        param.autoinc_auto_increment_ = dest_table_schema->get_auto_increment();
        param.autoinc_version_ = dest_table_schema->get_truncate_version();
        if (OB_FAIL(auto_inc_service.get_sequence_value(tenant_id_, object_id_, cur_column_id, param.autoinc_mode_is_order_, data_table_schema->get_truncate_version(), sequence_value))) {
          LOG_WARN("get sequence value failed", KR(ret), K(tenant_id_), K(object_id_), K(cur_column_id));
        } else if (FALSE_IT(param.global_value_to_sync_ = sequence_value - 1)) {
          // as sequence_value is an avaliable value. sync value will not be avaliable to user
        } else {
          for (int64_t retry_cnt = 100; OB_SUCC(ret) && retry_cnt > 0; retry_cnt--) {
            if (OB_FAIL(auto_inc_service.sync_insert_value_global(param))) {
              if (DDL_TABLE_RESTORE == task_type_ && share::ObIDDLTask::in_ddl_retry_white_list(ret)) {
                if (TC_REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
                  LOG_INFO("set auto increment position failed, retry", K(ret), K(dst_tenant_id_), K(target_object_id_), K(cur_column_id), K(param));
                }
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("set auto increment position failed", K(ret), K(dst_tenant_id_), K(target_object_id_), K(cur_column_id), K(param));
              }
            } else {
              break;
            }
          }
        }
        if (OB_SUCC(ret)) {
          has_synced_autoincrement_ = true;
          LOG_INFO("sync auto increment position succ", K(ret), K(sequence_value), K(object_id_),
          K(target_object_id_), K(dst_column_schema->get_column_id()));
        }
        THIS_WORKER.set_timeout_ts(save_timeout_ts);
      }
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::modify_autoinc(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  bool is_update_autoinc_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(check_update_autoinc_end(is_update_autoinc_end))) {
    LOG_WARN("update autoinc failed", K(ret));
  } else {
    ObSchemaGetterGuard hold_buf_src_tenant_schema_guard;
    ObSchemaGetterGuard hold_buf_dst_tenant_schema_guard;
    ObSchemaGetterGuard *src_tenant_schema_guard = nullptr;
    ObSchemaGetterGuard *dst_tenant_schema_guard = nullptr;
    AlterTableSchema &alter_table_schema = alter_table_arg_.alter_table_schema_;
    const ObTableSchema *orig_table_schema = nullptr;
    const ObTableSchema *new_table_schema = nullptr;
    uint64_t alter_autoinc_column_id = 0;
    ObColumnNameMap col_name_map;
    if (OB_FAIL(ObDDLUtil::get_tenant_schema_guard(tenant_id_, dst_tenant_id_,
        hold_buf_src_tenant_schema_guard, hold_buf_dst_tenant_schema_guard,
        src_tenant_schema_guard, dst_tenant_schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_), K(dst_tenant_id_));
    } else if (OB_FAIL(src_tenant_schema_guard->get_table_schema(tenant_id_, object_id_, orig_table_schema))) {
      LOG_WARN("get data table schema failed", K(ret), K(tenant_id_), K(object_id_));
    } else if (OB_FAIL(dst_tenant_schema_guard->get_table_schema(dst_tenant_id_, target_object_id_, new_table_schema))) {
      LOG_WARN("get data table schema failed", K(ret), K(dst_tenant_id_), K(target_object_id_));
    } else if (OB_ISNULL(orig_table_schema) || OB_ISNULL(new_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schemas should not be null", K(ret), K(orig_table_schema), K(new_table_schema));
    } else if (OB_FAIL(col_name_map.init(*orig_table_schema, alter_table_schema))) {
      LOG_WARN("failed to init column name map", K(ret));
    } else if (!is_update_autoinc_end && update_autoinc_job_time_ == 0) {
      ObTableSchema::const_column_iterator iter = alter_table_schema.column_begin();
      ObTableSchema::const_column_iterator iter_end = alter_table_schema.column_end();
      AlterColumnSchema *alter_column_schema = nullptr;
      for(; OB_SUCC(ret) && iter != iter_end; iter++) {
        if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*iter))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("iter is NULL", K(ret));
        } else {
          const ObString &orig_column_name = alter_column_schema->get_origin_column_name();
          if (!orig_column_name.empty()) {
            const ObColumnSchemaV2 *orig_column_schema = orig_table_schema->get_column_schema(orig_column_name);
            if (OB_ISNULL(orig_column_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("orig column schema is null", K(ret));
            } else if (alter_column_schema->is_autoincrement() && !orig_column_schema->is_autoincrement()) {
              ObString new_column_name;
              const ObColumnSchemaV2 *new_column_schema = nullptr;
              if (OB_FAIL(col_name_map.get(orig_column_name, new_column_name))) {
                LOG_WARN("invalid orig column name", K(ret), K(orig_table_schema), K(alter_table_schema));
              } else if (OB_ISNULL(new_column_schema = new_table_schema->get_column_schema(new_column_name))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("new column schema is null", K(ret), K(new_column_name), K(new_table_schema));
              } else {
                alter_autoinc_column_id = new_column_schema->get_column_id();
                alter_table_schema.set_autoinc_column_id(alter_autoinc_column_id);
                break; // there can only be one autoinc column
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (alter_autoinc_column_id != 0) { // there is an autoinc column
          ObObjType column_type = new_table_schema->get_column_schema(alter_autoinc_column_id)->get_data_type();
          ObUpdateAutoincSequenceTask task(tenant_id_, object_id_, target_object_id_, schema_version_,
                                          alter_autoinc_column_id, column_type, alter_table_arg_.sql_mode_,
                                          trace_id_, task_id_);
          if (OB_FAIL(root_service->submit_ddl_single_replica_build_task(task))) {
            LOG_WARN("fail to submit ObUpdateAutoincSequenceTask", K(ret));
          } else {
            update_autoinc_job_time_ = ObTimeUtility::current_time();
            LOG_INFO("submit ObUpdateAutoincSequenceTask success", K(object_id_), K(alter_autoinc_column_id));
          }
        } else {
          // no autoinc modify
          is_update_autoinc_end = true;
        }
      }
    }

    alter_autoinc_column_id = alter_table_schema.get_autoinc_column_id();
    if (OB_SUCC(ret) && is_update_autoinc_end && alter_autoinc_column_id != 0
        && OB_NOT_NULL(new_table_schema)) {
      const int64_t save_timeout_ts = THIS_WORKER.get_timeout_ts();
      THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + max(GCONF.rpc_timeout, 1000 * 1000 * 20L));
      ObAutoincrementService &auto_inc_service = ObAutoincrementService::get_instance();
      const uint64_t autoinc_val = alter_table_schema.get_auto_increment();
      AutoincParam param;
      param.tenant_id_ = dst_tenant_id_;
      param.autoinc_table_id_ = target_object_id_;
      param.autoinc_first_part_num_ = new_table_schema->get_first_part_num();
      param.autoinc_table_part_num_ = new_table_schema->get_all_part_num();
      param.autoinc_col_id_ = alter_autoinc_column_id;
      param.part_level_ = new_table_schema->get_part_level();
      ObObjType column_type = new_table_schema->get_column_schema(alter_autoinc_column_id)->get_data_type();
      param.autoinc_col_type_ = column_type;
      param.autoinc_desired_count_ = 0;
      param.autoinc_increment_ = 1;
      param.autoinc_offset_ = 1;
      param.global_value_to_sync_ = autoinc_val - 1;
      param.auto_increment_cache_size_ = 1; // TODO(shuangcan): should we use the sysvar on session?
      param.autoinc_version_ = new_table_schema->get_truncate_version();
      if (OB_FAIL(auto_inc_service.sync_insert_value_global(param))) {
        LOG_WARN("fail to clear autoinc cache", K(ret), K(param));
      }
      THIS_WORKER.set_timeout_ts(save_timeout_ts);
    }
  }

  if (OB_FAIL(ret) || is_update_autoinc_end) {
    if (OB_FAIL(switch_status(next_task_status, true, ret))) {
      LOG_WARN("fail to switch status", K(ret));
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", K(ret));
  } else if (OB_FAIL(remove_task_record())) {
    LOG_WARN("remove task record failed", K(ret));
  } else {
    need_retry_ = false;      // clean succ, stop the task
  }
  return ret;
}

int ObDDLRedefinitionTask::finish()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  ObSArray<uint64_t> objs;
  int64_t rpc_timeout = 0;
  int64_t all_orig_index_tablet_count = 0;
  alter_table_arg_.ddl_task_type_ = share::CLEANUP_GARBAGE_TASK;
  alter_table_arg_.table_id_ = object_id_;
  alter_table_arg_.hidden_table_id_ = target_object_id_;
  alter_table_arg_.task_id_ = task_id_;
  alter_table_arg_.alter_table_schema_.set_tenant_id(tenant_id_);
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (snapshot_version_ > 0 && OB_FAIL(release_snapshot(snapshot_version_))) {
    LOG_WARN("release snapshot failed", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema))) {
    LOG_WARN("get data table schema failed", K(ret), K(tenant_id_), K(object_id_));
  } else if (nullptr != data_table_schema) {
    if (OB_FAIL(get_orig_all_index_tablet_count(schema_guard, all_orig_index_tablet_count))) {
      LOG_WARN("get orig all tablet count failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(max(all_orig_index_tablet_count, data_table_schema->get_all_part_num()), rpc_timeout))) {
      LOG_WARN("get ddl rpc timeout failed", K(ret));
    } else if (data_table_schema->get_association_table_id() != OB_INVALID_ID &&
        OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
                  execute_ddl_task(alter_table_arg_, objs))) {
      LOG_WARN("cleanup garbage failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cleanup())) {
      LOG_WARN("clean up failed", K(ret));
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::fail()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(finish())) {
    LOG_WARN("finish failed", K(ret));
  } else {
    need_retry_ = false;
  }
  return ret;
}

int ObDDLRedefinitionTask::success()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(finish())) {
    LOG_WARN("finish failed", K(ret));
  } else {
    need_retry_ = false;
  }
  return ret;
}

int ObDDLRedefinitionTask::check_update_autoinc_end(bool &is_end)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == update_autoinc_job_ret_code_) {
    // not finish
  } else {
    is_end = true;
    ret_code_ = update_autoinc_job_ret_code_;
    ret = ret_code_;
  }
  return ret;
}

int ObDDLRedefinitionTask::check_check_table_empty_end(bool &is_end)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == check_table_empty_job_ret_code_) {
    // not finish
  } else if (OB_SUCCESS != check_table_empty_job_ret_code_) {
    ret_code_ = check_table_empty_job_ret_code_;
    is_end = true;
    LOG_WARN("check table empty job failed", K(ret_code_), K(object_id_), K(target_object_id_));
    if (is_replica_build_need_retry(ret_code_)) {
      check_table_empty_job_time_ = 0;
      check_table_empty_job_ret_code_ = INT64_MAX;
      ret_code_ = OB_SUCCESS;
      is_end = false;
      LOG_INFO("check table empty need retry", K(*this));
    }
  } else {
    is_end = true;
    ret_code_ = check_table_empty_job_ret_code_;
  }
  return ret;
}

int ObDDLRedefinitionTask::notify_update_autoinc_finish(const uint64_t autoinc_val, const int ret_code)
{
  int ret = OB_SUCCESS;
  update_autoinc_job_ret_code_ = ret_code;
  alter_table_arg_.alter_table_schema_.set_auto_increment(autoinc_val);
  return ret;
}

int ObDDLRedefinitionTask::check_health()
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
  } else if (OB_FAIL(refresh_status())) { // refresh task status
    LOG_WARN("refresh status failed", K(ret));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", K(ret));
  } else {
    ObSchemaGetterGuard hold_buf_src_tenant_schema_guard;
    ObSchemaGetterGuard hold_buf_dst_tenant_schema_guard;
    ObSchemaGetterGuard *src_tenant_schema_guard = nullptr;
    ObSchemaGetterGuard *dst_tenant_schema_guard = nullptr;
    bool is_source_table_exist = false;
    bool is_dest_table_exist = false;
    if (OB_FAIL(ObDDLUtil::get_tenant_schema_guard(tenant_id_, dst_tenant_id_,
        hold_buf_src_tenant_schema_guard, hold_buf_dst_tenant_schema_guard,
        src_tenant_schema_guard, dst_tenant_schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_), K(dst_tenant_id_));
    } else if (OB_FAIL(src_tenant_schema_guard->check_table_exist(tenant_id_, object_id_, is_source_table_exist))) {
      LOG_WARN("check data table exist failed", K(ret), K_(tenant_id), K(object_id_));
    } else if (OB_FAIL(dst_tenant_schema_guard->check_table_exist(dst_tenant_id_, target_object_id_, is_dest_table_exist))) {
      LOG_WARN("check index table exist failed", K(ret), K_(dst_tenant_id), K(is_dest_table_exist));
    } else if (!is_source_table_exist || !is_dest_table_exist) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table or dest table not exist", K(ret), K(is_source_table_exist), K(is_dest_table_exist));
    }
    if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
      const ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
      const ObDDLTaskStatus new_status = ObDDLTaskStatus::FAIL;
      switch_status(new_status, false, ret);
      LOG_WARN("switch status to build_failed", K(ret), K(old_status), K(new_status));
    }
    if (ObDDLTaskStatus::FAIL == static_cast<ObDDLTaskStatus>(task_status_)
        || ObDDLTaskStatus::SUCCESS == static_cast<ObDDLTaskStatus>(task_status_)) {
      ret = OB_SUCCESS; // allow clean up
    }
  }
  check_ddl_task_execute_too_long();
  return ret;
}

int ObDDLRedefinitionTask::get_estimated_timeout(const ObTableSchema *dst_table_schema, int64_t &estimated_timeout)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> tablet_ids;
  ObArray<ObObjectID> partition_ids;
  if (OB_ISNULL(dst_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(dst_table_schema));
  } else if (OB_FAIL(dst_table_schema->get_all_tablet_and_object_ids(tablet_ids, partition_ids))) {
    LOG_WARN("get all tablet and object ids failed", K(ret));
  } else {
    estimated_timeout = tablet_ids.count() * dst_table_schema->get_column_count() * 120L * 1000L; // 120ms for each column
    estimated_timeout = max(estimated_timeout, 9 * 1000 * 1000L);
    estimated_timeout = min(estimated_timeout, 3600 * 1000 * 1000L);
    estimated_timeout = max(estimated_timeout, GCONF.rpc_timeout);
    LOG_INFO("get estimate timeout", K(estimated_timeout));
  }
  return ret;
}

int ObDDLRedefinitionTask::get_orig_all_index_tablet_count(ObSchemaGetterGuard &schema_guard, int64_t &all_tablet_count)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  all_tablet_count = 0;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, orig_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(object_id_));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, table schema must not be nullptr", K(ret), K(object_id_));
  } else {
    const common::ObIArray<ObAuxTableMetaInfo> &orig_index_infos = orig_table_schema->get_simple_index_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_index_infos.count(); i++) {
      int64_t tablet_count = 0;
      int64_t table_id = orig_index_infos.at(i).table_id_;
      if (OB_FAIL(ObDDLUtil::get_tablet_count(tenant_id_, table_id, tablet_count))) {
        LOG_WARN("get tablet count fail", K(ret));
      } else {
        all_tablet_count += tablet_count;
      }
    }
  }
  return ret;
}

bool ObDDLRedefinitionTask::check_need_sync_stats_history() {
  return !is_double_table_long_running_ddl(task_type_);
}

bool ObDDLRedefinitionTask::check_need_sync_stats() {
  // bugfix:
  // shouldn't sync stats if the ddl task is from load data's direct_load
  return !(has_synced_stats_info_ || task_type_ == DDL_DIRECT_LOAD);
}

int ObDDLRedefinitionTask::sync_stats_info()
{
  int ret = OB_SUCCESS;
  if (check_need_sync_stats()) {
    ObSchemaGetterGuard hold_buf_src_tenant_schema_guard;
    ObSchemaGetterGuard hold_buf_dst_tenant_schema_guard;
    ObSchemaGetterGuard *src_tenant_schema_guard = nullptr;
    ObSchemaGetterGuard *dst_tenant_schema_guard = nullptr;
    ObMySQLTransaction trans;
    const ObTableSchema *data_table_schema = nullptr;
    const ObTableSchema *new_table_schema = nullptr;
    ObTimeoutCtx timeout_ctx;
    int64_t timeout = 0;
    const int64_t start_time = ObTimeUtility::current_time();
    if (OB_FAIL(ObDDLUtil::get_tenant_schema_guard(tenant_id_, dst_tenant_id_,
        hold_buf_src_tenant_schema_guard, hold_buf_dst_tenant_schema_guard,
        src_tenant_schema_guard, dst_tenant_schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_), K(dst_tenant_id_));
    } else if (OB_FAIL(src_tenant_schema_guard->get_table_schema(tenant_id_, object_id_, data_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret), K(tenant_id_), K(object_id_));
    } else if (OB_FAIL(dst_tenant_schema_guard->get_table_schema(dst_tenant_id_, target_object_id_, new_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret), K(dst_tenant_id_), K(target_object_id_));
    } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(new_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_FAIL(get_estimated_timeout(new_table_schema, timeout))) {
      LOG_WARN("get estimated timeout failed", K(ret));
    } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(timeout))) {
      LOG_WARN("set timeout ctx failed", K(ret));
    } else if (OB_FAIL(timeout_ctx.set_timeout(timeout))) {
      LOG_WARN("set timeout failed", K(ret));
    } else if (tenant_id_ == dst_tenant_id_
               && OB_FAIL(sync_stats_info_in_same_tenant(trans,
                                                         src_tenant_schema_guard,
                                                         *data_table_schema,
                                                         *new_table_schema))) {
      LOG_WARN("fail to sync stat in same tenant", K(ret));
    } else if (tenant_id_ != dst_tenant_id_
               && OB_FAIL(sync_stats_info_accross_tenant(trans,
                                                         dst_tenant_schema_guard,
                                                         *data_table_schema,
                                                         *new_table_schema))) {
      LOG_WARN("fail to sync stat accross tenant", K(ret));
    }

    if (trans.is_started()) {
      bool is_commit = (ret == OB_SUCCESS);
      int tmp_ret = trans.end(is_commit);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to end trans", K(ret), K(is_commit));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      } else {
        has_synced_stats_info_ = (ret == OB_SUCCESS);
      }
    }
    const int64_t end_time = ObTimeUtility::current_time();
    LOG_INFO("sync table stat finished", "cost_time", end_time - start_time);
  }
  return ret;
}

int ObDDLRedefinitionTask::sync_stats_info_in_same_tenant(common::ObMySQLTransaction &trans,
                                                          ObSchemaGetterGuard *src_tenant_schema_guard,
                                                          const ObTableSchema &data_table_schema,
                                                          const ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  bool need_sync_history = check_need_sync_stats_history();

  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(trans.start(&root_service->get_sql_proxy(), dst_tenant_id_))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_FAIL(sync_table_level_stats_info(trans, data_table_schema, need_sync_history))) {
    LOG_WARN("fail to sync table level stats", K(ret));
  } else if (DDL_ALTER_PARTITION_BY != task_type_
              && OB_FAIL(sync_partition_level_stats_info(trans,
                                                        data_table_schema,
                                                        new_table_schema,
                                                        need_sync_history))) {
    LOG_WARN("fail to sync partition level stats", K(ret));
  } else if (OB_FAIL(sync_column_level_stats_info(trans,
                                                  data_table_schema,
                                                  new_table_schema,
                                                  *src_tenant_schema_guard,
                                                  need_sync_history))) {
    LOG_WARN("fail to sync column level stats", K(ret));
  }

  return ret;
}

int ObDDLRedefinitionTask::sync_stats_info_accross_tenant(common::ObMySQLTransaction &trans,
                                                          ObSchemaGetterGuard *dst_tenant_schema_guard,
                                                          const ObTableSchema &data_table_schema,
                                                          const ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptTableStat, 4> src_part_stats;
  ObSEArray<ObOptKeyColumnStat, 4> src_column_stats;
  common::ObArenaAllocator allocator(lib::ObLabel("RedefTask"));
  ObRootService *root_service = GCTX.root_service_;
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(get_src_part_stats(data_table_schema, src_part_stats))) {
    LOG_WARN("fail to get src part stats", K(ret));
  } else if (OB_FAIL(get_src_column_stats(data_table_schema, allocator, src_column_stats))) {
    LOG_WARN("fail to get src column stats", K(ret));
  } else if (OB_FAIL(trans.start(&root_service->get_sql_proxy(), dst_tenant_id_))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_FAIL(sync_part_stats_info_accross_tenant(trans,
                                                         data_table_schema,
                                                         new_table_schema,
                                                         src_part_stats))) {
    LOG_WARN("fail to sync partition stats", K(ret));
  } else if (OB_FAIL(sync_column_stats_info_accross_tenant(trans,
                                                           dst_tenant_schema_guard,
                                                           data_table_schema,
                                                           new_table_schema,
                                                           src_column_stats))) {
    LOG_WARN("fail to sync column table level stat", K(ret));
  } else {
    LOG_INFO("succeed to sync accross tenant stat", K_(tenant_id), K_(dst_tenant_id), K_(object_id), K_(target_object_id));
  }

  return ret;
}

int ObDDLRedefinitionTask::get_src_part_stats(const ObTableSchema &data_table_schema,
                                              ObIArray<ObOptTableStat> &part_stats)
{
  int ret = OB_SUCCESS;
  ObOptTableStat::Key key;
  ObOptStatSqlService &stat_svr = ObOptStatManager::get_instance().get_stat_sql_service();
  key.tenant_id_ = tenant_id_;
  key.table_id_ = object_id_;
  if (!data_table_schema.is_partitioned_table()) {
    key.partition_id_ = object_id_;
  } else {
    key.partition_id_ = -1;
  }

  // fetch_table_stat return both the table-level and partition-level stats.
  if (FAILEDx(stat_svr.fetch_table_stat(tenant_id_, key, part_stats))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get table stat", K(ret), K_(tenant_id), K(key));
    } else {
      // no part stat
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObDDLRedefinitionTask::get_src_column_stats(const ObTableSchema &data_table_schema,
                                                ObIAllocator &allocator,
                                                ObIArray<ObOptKeyColumnStat> &column_stats)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> partition_ids;
  ObArray<uint64_t> column_ids;
  ObArray<ObObjectID> object_ids;
  ObOptStatSqlService &stat_svr = ObOptStatManager::get_instance().get_stat_sql_service();

  if (!data_table_schema.is_partitioned_table()) {
    if (OB_FAIL(partition_ids.push_back(object_id_))) {
      LOG_WARN("failed to push back partition id", K(ret));
    }
  } else if (OB_FAIL(partition_ids.push_back(-1))) {
    LOG_WARN("failed to push back partition id", K(ret));
  } else if (OB_FAIL(pl::ObDbmsStats::get_part_ids_from_schema(&data_table_schema, object_ids))) {
    LOG_WARN("fail to get all tablet and object ids", K(ret));
  } else {
    ARRAY_FOREACH(object_ids, i) {
      if (OB_FAIL(partition_ids.push_back(object_ids.at(i)))) {
        LOG_WARN("failed to push back partition id", K(ret));
      }
    }
  }

  ObTableSchema::const_column_iterator iter = data_table_schema.column_begin();
  ObTableSchema::const_column_iterator iter_end = data_table_schema.column_end();

  for (; OB_SUCC(ret) && iter != iter_end; iter++) {
    const ObColumnSchemaV2 *col = *iter;
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col is NULL", K(ret));
    } else if (col->get_column_id() < OB_APP_MIN_COLUMN_ID) {
      // bypass hidden column
    } else if (col->is_udt_hidden_column()) {
      // bypass udt hidden column
    } else if (OB_FAIL(column_ids.push_back(col->get_column_id()))) {
      LOG_WARN("failed to push back column id", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
    for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); j++) {
      void *buf1 = NULL;
      void *buf2 = NULL;
      if (OB_ISNULL(buf1 = allocator.alloc(sizeof(ObOptColumnStat::Key)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc column stat key", K(ret));
      } else if (OB_ISNULL(buf2 = allocator.alloc(sizeof(ObOptColumnStat)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc column stat", K(ret));
      } else {
        ObOptColumnStat::Key *key = new (buf1) ObOptColumnStat::Key(tenant_id_,
                                                                    object_id_,
                                                                    partition_ids.at(i),
                                                                    column_ids.at(j));
        ObOptColumnStat *stat = new (buf2) ObOptColumnStat();
        stat->set_table_id(key->table_id_);
        stat->set_partition_id(key->partition_id_);
        stat->set_column_id(key->column_id_);

        ObOptKeyColumnStat key_col_stat;
        key_col_stat.key_ = key;
        key_col_stat.stat_ = stat;
        if (OB_FAIL(column_stats.push_back(key_col_stat))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }

  if (FAILEDx(stat_svr.fetch_column_stat(tenant_id_,
                                         allocator,
                                         column_stats,
                                         true /* accross tenant query */))) {
    LOG_WARN("failed to get column stat", K(ret), K_(tenant_id), K_(object_id));
  }

  return ret;
}

int ObDDLRedefinitionTask::sync_part_stats_info_accross_tenant(common::ObMySQLTransaction &trans,
                                                               const ObTableSchema &data_table_schema,
                                                               const ObTableSchema &new_table_schema,
                                                               const ObIArray<ObOptTableStat> &part_stats)
{
  int ret = OB_SUCCESS;
  ObArray<ObObjectID> src_partition_ids;
  ObArray<ObObjectID> target_partition_ids;
  ObHashMap<int64_t, int64_t> part_ids_map;
  ObOptStatSqlService &stat_svr = ObOptStatManager::get_instance().get_stat_sql_service();
  common::ObArenaAllocator allocator(lib::ObLabel("RedefTask"));
  ObSEArray<ObOptTableStat *, 4> target_part_stats;
  if (part_stats.empty()) {
    LOG_INFO("partition stats are empty, no need to sync", K_(tenant_id), K_(dst_tenant_id), K_(object_id), K_(target_object_id));
  } else {
    // build partition id mapping table in order to replace the old partition
    // with new partition.
    if (OB_FAIL(part_ids_map.create(MAP_BUCKET_NUM, "RedefTask"))) {
      LOG_WARN("failed to create map", K(ret));
    } else if (!data_table_schema.is_partitioned_table()) {
      if (OB_FAIL(part_ids_map.set_refactored(object_id_, target_object_id_))) {
        LOG_WARN("failed to insert map", K(ret));
      }
    } else if (OB_FAIL(part_ids_map.set_refactored(-1, -1))) {
      LOG_WARN("failed to insert map", K(ret));
    } else if (OB_FAIL(pl::ObDbmsStats::get_part_ids_from_schema(&data_table_schema, src_partition_ids))) {
      LOG_WARN("fail to get all tablet and object ids", K(ret));
    } else if (OB_FAIL(pl::ObDbmsStats::get_part_ids_from_schema(&new_table_schema, target_partition_ids))) {
      LOG_WARN("fail to get all tablet and object ids", K(ret));
    } else {
      ARRAY_FOREACH(src_partition_ids, i) {
        const int64_t src_partition_id = src_partition_ids.at(i);
        const int64_t target_partition_id = target_partition_ids.at(i);
        if (OB_FAIL(part_ids_map.set_refactored(src_partition_id, target_partition_id))) {
          LOG_WARN("failed to insert map", K(ret));
        }
      }
    }

    // replace with new partition id.
    ARRAY_FOREACH(part_stats, i) {
      const ObOptTableStat &part_stat = part_stats.at(i);
      ObOptTableStat *target_part_stat = NULL;
      char *buf = NULL;
      if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(part_stat.size())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc part stat", K(ret));
      } else if (OB_FAIL(part_stat.deep_copy(buf, part_stat.size(), target_part_stat))) {
        LOG_WARN("failed to copy partition stat", K(ret));
      } else {
        const int64_t src_partition_id = part_stat.get_partition_id();
        int64_t target_partition_id = 0;
        target_part_stat->set_table_id(target_object_id_);
        target_part_stat->set_last_analyzed(0);
        if (OB_FAIL(part_ids_map.get_refactored(src_partition_id, target_partition_id))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("target partition not found", K(ret), K_(dst_tenant_id), K_(target_object_id), K(src_partition_id));
          } else {
            LOG_WARN("failed to get part_ids_map", K(ret));
          }
        } else if (FALSE_IT(target_part_stat->set_partition_id(target_partition_id))) {
        } else if (OB_FAIL(target_part_stats.push_back(target_part_stat))) {
          LOG_WARN("failed to push back partition stat", K(ret));
        }
      }
    }

    if (FAILEDx(stat_svr.update_table_stat(dst_tenant_id_,
                                          trans,
                                          target_part_stats,
                                          ObTimeUtility::current_time(),
                                          new_table_schema.is_index_table()))) {
      LOG_WARN("failed to update partition stats", K(ret), K_(dst_tenant_id), K_(target_object_id));
    }
  }

  return ret;
}

int ObDDLRedefinitionTask::sync_column_stats_info_accross_tenant(common::ObMySQLTransaction &trans,
                                                                 share::schema::ObSchemaGetterGuard *dst_tenant_schema_guard,
                                                                 const ObTableSchema &data_table_schema,
                                                                 const ObTableSchema &new_table_schema,
                                                                 const ObIArray<ObOptKeyColumnStat> &column_stats)
{
  int ret = OB_SUCCESS;
  ObArray<ObObjectID> src_partition_ids;
  ObArray<ObObjectID> target_partition_ids;
  ObHashMap<int64_t, int64_t> part_ids_map;
  ObOptStatSqlService &stat_svr = ObOptStatManager::get_instance().get_stat_sql_service();
  common::ObArenaAllocator allocator(lib::ObLabel("RedefTask"));
  ObSEArray<ObOptColumnStat *, 4> target_column_stats;
  if (column_stats.empty()) {
    LOG_INFO("column stats are empty, no need to sync", K_(tenant_id), K_(dst_tenant_id), K_(object_id), K_(target_object_id));
  } else {
    // build partition id mapping table in order to replace the old partition
    // with new partition.
    if (OB_FAIL(part_ids_map.create(MAP_BUCKET_NUM, "RedefTask"))) {
      LOG_WARN("failed to create map", K(ret));
    } else if (!data_table_schema.is_partitioned_table()) {
      if (OB_FAIL(part_ids_map.set_refactored(object_id_, target_object_id_))) {
        LOG_WARN("failed to insert map", K(ret));
      }
    } else if (OB_FAIL(part_ids_map.set_refactored(-1, -1))) {
      LOG_WARN("failed to insert map", K(ret));
    } else if (OB_FAIL(pl::ObDbmsStats::get_part_ids_from_schema(&data_table_schema, src_partition_ids))) {
      LOG_WARN("fail to get all tablet and object ids", K(ret));
    } else if (OB_FAIL(pl::ObDbmsStats::get_part_ids_from_schema(&new_table_schema, target_partition_ids))) {
      LOG_WARN("fail to get all tablet and object ids", K(ret));
    } else {
      ARRAY_FOREACH(src_partition_ids, i) {
        const int64_t src_partition_id = src_partition_ids.at(i);
        const int64_t target_partition_id = target_partition_ids.at(i);
        if (OB_FAIL(part_ids_map.set_refactored(src_partition_id, target_partition_id))) {
          LOG_WARN("failed to insert map", K(ret));
        }
      }
    }

    // replace with new partition id.
    ARRAY_FOREACH(column_stats, i) {
      const ObOptColumnStat *col_stat = column_stats.at(i).stat_;
      ObOptColumnStat *target_col_stat = NULL;
      if (OB_ISNULL(col_stat)) {
        // ignore empty column stat
      } else if (OB_ISNULL(target_col_stat = OB_NEWx(ObOptColumnStat, &allocator, allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create column stat", K(ret));
      } else if (OB_FAIL(target_col_stat->deep_copy(*col_stat))) {
        LOG_WARN("failed to deep copy", K(ret));
      } else {
        const int64_t src_partition_id = col_stat->get_partition_id();
        int64_t target_partition_id = 0;
        target_col_stat->set_table_id(target_object_id_);
        target_col_stat->set_last_analyzed(0);
        if (OB_FAIL(part_ids_map.get_refactored(src_partition_id, target_partition_id))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("target partition not found", K(ret), K_(dst_tenant_id), K_(target_object_id), K(src_partition_id));
          } else {
            LOG_WARN("failed to get part_ids_map", K(ret));
          }
        } else if (FALSE_IT(target_col_stat->set_partition_id(target_partition_id))) {
        } else if (OB_FAIL(target_column_stats.push_back(target_col_stat))) {
          LOG_WARN("failed to push back column stat", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (target_column_stats.empty()) {
      LOG_INFO("column stats are empty, no need to sync", K_(tenant_id), K_(dst_tenant_id), K_(object_id), K_(target_object_id));
    } else if (OB_FAIL(stat_svr.update_column_stat(dst_tenant_schema_guard,
                                                   dst_tenant_id_,
                                                   trans,
                                                   target_column_stats,
                                                   ObTimeUtility::current_time(),
                                                   false /* need update histogram table */))) {
      LOG_WARN("failed to update column stats", K(ret), K_(dst_tenant_id), K_(target_object_id));
    }
  }

  return ret;
}

int ObDDLRedefinitionTask::sync_table_level_stats_info(common::ObMySQLTransaction &trans,
                                                       const ObTableSchema &data_table_schema,
                                                       const bool need_sync_history/*default true*/)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  ObSqlString history_sql_string;
  int64_t affected_rows = 0;
  // for partitioned table, table-level stat is -1, for non-partitioned table, table-level stat is table id
  int64_t partition_id = -1;
  int64_t target_partition_id = -1;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(dst_tenant_id_);
  if (!data_table_schema.is_partitioned_table()) {
    partition_id = object_id_;
    target_partition_id = target_object_id_;
  }
  if (OB_FAIL(sql_string.assign_fmt("UPDATE %s SET table_id = %ld, partition_id = %ld"
      " WHERE tenant_id = %ld and table_id = %ld and partition_id = %ld",
      OB_ALL_TABLE_STAT_TNAME, target_object_id_, target_partition_id,
      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, dst_tenant_id_), object_id_, partition_id))) {
    LOG_WARN("fail to assign sql string", K(ret));
  } else if (OB_FAIL(trans.write(dst_tenant_id_, sql_string.ptr(), affected_rows))) {
    LOG_WARN("fail to update __all_table_stat", K(ret), K(sql_string));
  } else if (OB_UNLIKELY(affected_rows < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected_rows", K(ret), K(affected_rows));
  } else if (!need_sync_history) {// do not need to sync history.
  } else if (OB_FAIL(history_sql_string.assign_fmt("UPDATE %s SET table_id = %ld, partition_id = %ld"
      " WHERE tenant_id = %ld and table_id = %ld and partition_id = %ld",
      OB_ALL_TABLE_STAT_HISTORY_TNAME, target_object_id_, target_partition_id,
      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, dst_tenant_id_), object_id_, partition_id))) {
    LOG_WARN("fail to assign history sql string", K(ret));
  } else if (OB_FAIL(trans.write(dst_tenant_id_, history_sql_string.ptr(), affected_rows))) {
    LOG_WARN("fail to update __all_table_stat_history", K(ret), K(sql_string));
  }
  return ret;
}

int ObDDLRedefinitionTask::sync_partition_level_stats_info(common::ObMySQLTransaction &trans,
                                                           const ObTableSchema &data_table_schema,
                                                           const ObTableSchema &new_table_schema,
                                                           const bool need_sync_history/*default true*/)
{
  int ret = OB_SUCCESS;
  ObArray<ObObjectID> src_partition_ids;
  ObArray<ObObjectID> dest_partition_ids;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(dst_tenant_id_);
  if (!data_table_schema.is_partitioned_table()) {
    // if not partition table, no need to sync partition level stats
  } else if (OB_FAIL(pl::ObDbmsStats::get_part_ids_from_schema(&data_table_schema, src_partition_ids))) {
    LOG_WARN("fail to get all tablet and object ids", K(ret));
  } else if (OB_FAIL(pl::ObDbmsStats::get_part_ids_from_schema(&new_table_schema, dest_partition_ids))) {
    LOG_WARN("fail to get all tablet and object ids", K(ret));
  } else {
    const int64_t BATCH_SIZE = 256;
    int64_t batch_start = 0;
    while (OB_SUCC(ret) && batch_start < src_partition_ids.count()) {
      ObSqlString sql_string;
      ObSqlString history_sql_string;
      int64_t affected_rows = 0;
      const int64_t batch_end = std::min(src_partition_ids.count(), batch_start + BATCH_SIZE) - 1;
      if (OB_FAIL(generate_sync_partition_level_stats_sql(OB_ALL_TABLE_STAT_TNAME,
                                                          src_partition_ids,
                                                          dest_partition_ids,
                                                          batch_start,
                                                          batch_end,
                                                          sql_string))) {
        LOG_WARN("fail to generate sql", K(ret));
      } else if (OB_FAIL(trans.write(dst_tenant_id_, sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to update __all_table_stat", K(ret), K(sql_string));
      } else if (OB_UNLIKELY(affected_rows < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected_rows", K(ret), K(affected_rows));
      } else if (!need_sync_history) {// do not need to sync history
      } else if (OB_FAIL(generate_sync_partition_level_stats_sql(OB_ALL_TABLE_STAT_HISTORY_TNAME,
                                                                 src_partition_ids,
                                                                 dest_partition_ids,
                                                                 batch_start,
                                                                 batch_end,
                                                                 history_sql_string))) {
        LOG_WARN("fail to generate sql", K(ret));
      } else if (OB_FAIL(trans.write(dst_tenant_id_, history_sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to update __all_table_stat_history", K(ret), K(history_sql_string));
      } else if (OB_UNLIKELY(affected_rows < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected_rows", K(ret), K(affected_rows));
      }
      batch_start += BATCH_SIZE;
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::sync_column_level_stats_info(common::ObMySQLTransaction &trans,
                                                        const ObTableSchema &data_table_schema,
                                                        const ObTableSchema &new_table_schema,
                                                        ObSchemaGetterGuard &schema_guard,
                                                        const bool need_sync_history/*default true*/)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = alter_table_arg_.alter_table_schema_;
  ObColumnNameMap col_name_map;
  if (OB_FAIL(col_name_map.init(data_table_schema, alter_table_schema))) {
    LOG_WARN("failed to init column name map", K(ret));
  } else {
    ObTableSchema::const_column_iterator iter = data_table_schema.column_begin();
    ObTableSchema::const_column_iterator iter_end = data_table_schema.column_end();
    for (; OB_SUCC(ret) && iter != iter_end; iter++) {
      const ObColumnSchemaV2 *col = *iter;
      const ObColumnSchemaV2 *new_col = nullptr;
      ObString new_col_name;
      bool is_offline = false;
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col is NULL", K(ret));
      } else if (col->get_column_id() < OB_APP_MIN_COLUMN_ID) {
        // bypass hidden column
      } else if (col->is_udt_hidden_column()) {
        // bypass udt hidden column
      } else if (OB_FAIL(col_name_map.get(col->get_column_name_str(), new_col_name))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // the column is not in column name map, meaning it is dropped in this ddl
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get new column name", K(ret), K(*col));
        }
      } else if (OB_ISNULL(new_col = new_table_schema.get_column_schema(new_col_name))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new column schema should not be null", K(ret), K(new_col_name), K(new_table_schema));
      // if this column is changed in a offline rule, meaning the data will change in this ddl, therefore
      // the column stat will be invalidated.
      } else if (OB_FAIL(data_table_schema.check_alter_column_is_offline(col,
                                                                         const_cast<ObColumnSchemaV2 *>(new_col),
                                                                         schema_guard,
                                                                         is_offline))) {
        LOG_WARN("fail to check alter table is offline", K(ret), K(*col), K(*new_col));
      } else if (!is_offline) {
        if (OB_FAIL(sync_one_column_table_level_stats_info(trans,
                                                           data_table_schema,
                                                           col->get_column_id(),
                                                           new_col->get_column_id(),
                                                           need_sync_history))) {
          LOG_WARN("fail to sync table level stats info for one column", K(ret), K(*col), K(*new_col));
        } else if (DDL_ALTER_PARTITION_BY != task_type_ &&
                   OB_FAIL(sync_one_column_partition_level_stats_info(trans,
                                                                      data_table_schema,
                                                                      new_table_schema,
                                                                      col->get_column_id(),
                                                                      new_col->get_column_id(),
                                                                      need_sync_history))) {
          LOG_WARN("fail to sync partition level stats info for one column", K(ret), K(*col), K(*new_col));
        }
      }
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::sync_one_column_table_level_stats_info(common::ObMySQLTransaction &trans,
                                                                  const ObTableSchema &data_table_schema,
                                                                  const uint64_t old_col_id,
                                                                  const uint64_t new_col_id,
                                                                  const bool need_sync_history/*default true*/)
{
  int ret = OB_SUCCESS;
  ObSqlString column_sql_string;
  ObSqlString column_history_sql_string;
  ObSqlString histogram_sql_string;
  ObSqlString histogram_history_sql_string;
  int64_t affected_rows = 0;
  // for partitioned table, table-level stat is -1, for non-partitioned table, table-level stat is table id
  int64_t partition_id = -1;
  int64_t target_partition_id = -1;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(dst_tenant_id_);
  if (!data_table_schema.is_partitioned_table()) {
    partition_id = object_id_;
    target_partition_id = target_object_id_;
  }
  if (OB_FAIL(column_sql_string.assign_fmt("UPDATE %s SET table_id = %ld, partition_id = %ld, column_id = %ld"
      " WHERE tenant_id = %ld and table_id = %ld and partition_id = %ld and column_id = %ld",
      OB_ALL_COLUMN_STAT_TNAME, target_object_id_, target_partition_id, new_col_id,
      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, dst_tenant_id_), object_id_, partition_id,
      old_col_id))) {
    LOG_WARN("fail to assign sql string", K(ret));
  } else if (OB_FAIL(histogram_sql_string.assign_fmt("UPDATE %s SET table_id = %ld, partition_id = %ld, column_id = %ld"
      " WHERE tenant_id = %ld and table_id = %ld and partition_id = %ld and column_id = %ld",
      OB_ALL_HISTOGRAM_STAT_TNAME, target_object_id_, target_partition_id, new_col_id,
      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, dst_tenant_id_), object_id_, partition_id,
      old_col_id))) {
    LOG_WARN("fail to assign sql string", K(ret));
  } else if (OB_FAIL(trans.write(dst_tenant_id_, column_sql_string.ptr(), affected_rows))) {
    LOG_WARN("fail to update __all_column_stat", K(ret), K(column_sql_string));
  } else if (OB_FAIL(trans.write(dst_tenant_id_, histogram_sql_string.ptr(), affected_rows))) {
    LOG_WARN("fail to update __all_histogram_stat_history", K(ret), K(histogram_sql_string));
  } else if (!need_sync_history) { // do not need to sync history
  } else if (OB_FAIL(column_history_sql_string.assign_fmt(
      "UPDATE %s SET table_id = %ld, partition_id = %ld, column_id = %ld"
      " WHERE tenant_id = %ld and table_id = %ld and partition_id = %ld and column_id = %ld",
      OB_ALL_COLUMN_STAT_HISTORY_TNAME, target_object_id_, target_partition_id, new_col_id,
      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, dst_tenant_id_), object_id_, partition_id, old_col_id))) {
    LOG_WARN("fail to assign history sql string", K(ret));
  } else if (OB_FAIL(histogram_history_sql_string.assign_fmt(
      "UPDATE %s SET table_id = %ld, partition_id = %ld, column_id = %ld"
      " WHERE tenant_id = %ld and table_id = %ld and partition_id = %ld and column_id = %ld",
      OB_ALL_HISTOGRAM_STAT_HISTORY_TNAME, target_object_id_, target_partition_id, new_col_id,
      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, dst_tenant_id_), object_id_, partition_id, old_col_id))) {
    LOG_WARN("fail to assign history sql string", K(ret));
  } else if (OB_FAIL(trans.write(dst_tenant_id_, column_history_sql_string.ptr(), affected_rows))) {
    LOG_WARN("fail to update __all_column_stat_history", K(ret), K(column_history_sql_string));
  } else if (OB_FAIL(trans.write(dst_tenant_id_, histogram_history_sql_string.ptr(), affected_rows))) {
    LOG_WARN("fail to update __all_histogram_stat_history", K(ret), K(histogram_history_sql_string));
  }
  return ret;
}

int ObDDLRedefinitionTask::sync_one_column_partition_level_stats_info(common::ObMySQLTransaction &trans,
                                                                      const ObTableSchema &data_table_schema,
                                                                      const ObTableSchema &new_table_schema,
                                                                      const uint64_t old_col_id,
                                                                      const uint64_t new_col_id,
                                                                      const bool need_sync_history/*default true*/)
{
  int ret = OB_SUCCESS;
  ObArray<ObObjectID> src_partition_ids;
  ObArray<ObObjectID> dest_partition_ids;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(dst_tenant_id_);
  if (!data_table_schema.is_partitioned_table()) {
    // if not partition table, no need to sync partition level stats
  } else if (OB_FAIL(pl::ObDbmsStats::get_part_ids_from_schema(&data_table_schema, src_partition_ids))) {
    LOG_WARN("fail to get all tablet and object ids", K(ret));
  } else if (OB_FAIL(pl::ObDbmsStats::get_part_ids_from_schema(&new_table_schema, dest_partition_ids))) {
    LOG_WARN("fail to get all tablet and object ids", K(ret));
  } else {
    const int64_t BATCH_SIZE = 256;
    int64_t batch_start = 0;
    while (OB_SUCC(ret) && batch_start < src_partition_ids.count()) {
      ObSqlString column_sql_string;
      ObSqlString column_history_sql_string;
      ObSqlString histogram_sql_string;
      ObSqlString histogram_history_sql_string;
      int64_t affected_rows = 0;
      const int64_t batch_end = std::min(src_partition_ids.count(), batch_start + BATCH_SIZE) - 1;
      if (OB_FAIL(generate_sync_column_partition_level_stats_sql(OB_ALL_COLUMN_STAT_TNAME,
                                                                 src_partition_ids,
                                                                 dest_partition_ids,
                                                                 old_col_id,
                                                                 new_col_id,
                                                                 batch_start,
                                                                 batch_end,
                                                                 column_sql_string))) {
        LOG_WARN("fail to generate sql", K(ret));
      } else if (OB_FAIL(generate_sync_column_partition_level_stats_sql(OB_ALL_HISTOGRAM_STAT_TNAME,
                                                                 src_partition_ids,
                                                                 dest_partition_ids,
                                                                 old_col_id,
                                                                 new_col_id,
                                                                 batch_start,
                                                                 batch_end,
                                                                 histogram_sql_string))) {
        LOG_WARN("fail to generate sql", K(ret));
      } else if (OB_FAIL(trans.write(dst_tenant_id_, column_sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to update __all_column_stat", K(ret), K(column_sql_string));
      } else if (OB_FAIL(trans.write(dst_tenant_id_, histogram_sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to update __all_histogram_stat_history", K(ret), K(histogram_sql_string));
      } else if (!need_sync_history) {
      } else if (OB_FAIL(generate_sync_column_partition_level_stats_sql(OB_ALL_COLUMN_STAT_HISTORY_TNAME,
                                                                 src_partition_ids,
                                                                 dest_partition_ids,
                                                                 old_col_id,
                                                                 new_col_id,
                                                                 batch_start,
                                                                 batch_end,
                                                                 column_history_sql_string))) {
        LOG_WARN("fail to generate sql", K(ret));
      } else if (OB_FAIL(generate_sync_column_partition_level_stats_sql(OB_ALL_HISTOGRAM_STAT_HISTORY_TNAME,
                                                                 src_partition_ids,
                                                                 dest_partition_ids,
                                                                 old_col_id,
                                                                 new_col_id,
                                                                 batch_start,
                                                                 batch_end,
                                                                 histogram_history_sql_string))) {
        LOG_WARN("fail to generate sql", K(ret));
      } else if (OB_FAIL(trans.write(dst_tenant_id_, column_history_sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to update __all_column_stat_history", K(ret), K(column_history_sql_string));
      } else if (OB_FAIL(trans.write(dst_tenant_id_, histogram_history_sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to update __all_histogram_stat_history", K(ret), K(histogram_history_sql_string));
      }
      batch_start += BATCH_SIZE;
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::generate_sync_partition_level_stats_sql(const char *table_name,
                                                                   const ObIArray<ObObjectID> &src_partition_ids,
                                                                   const ObIArray<ObObjectID> &dest_partition_ids,
                                                                   const int64_t batch_start,
                                                                   const int64_t batch_end,
                                                                   ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  sql_string.reset();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(dst_tenant_id_);
  if (OB_UNLIKELY(src_partition_ids.count() != dest_partition_ids.count() || batch_end < batch_start
      || batch_end >= dest_partition_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_partition_ids), K(dest_partition_ids), K(batch_start), K(batch_end));
  } else if (OB_FAIL(sql_string.append_fmt("UPDATE %s SET table_id=%ld, partition_id=(case partition_id",
        table_name, target_object_id_))) {
    LOG_WARN("fail to append sql string", K(ret), K(table_name), K(target_object_id_));
  } else {
    for (int64_t i = batch_start; OB_SUCC(ret) && i <= batch_end; i++) {
      const uint64_t src_partition_id = src_partition_ids.at(i);
      const uint64_t dest_partition_id = dest_partition_ids.at(i);
      if (OB_FAIL(sql_string.append_fmt(" when %ld then %ld", src_partition_id, dest_partition_id))) {
        LOG_WARN("fail to append sql string", K(ret), K(src_partition_id), K(dest_partition_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_string.append_fmt(" else partition_id end) where tenant_id=%ld and table_id=%ld",
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, dst_tenant_id_),
          batch_start == 0 ? object_id_ : target_object_id_))) {
      LOG_WARN("fail to append sql string", K(ret), K(object_id_), K(dst_tenant_id_), K(exec_tenant_id));
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::generate_sync_column_partition_level_stats_sql(const char *table_name,
                                                                          const ObIArray<ObObjectID> &src_partition_ids,
                                                                          const ObIArray<ObObjectID> &dest_partition_ids,
                                                                          const uint64_t old_col_id,
                                                                          const uint64_t new_col_id,
                                                                          const int64_t batch_start,
                                                                          const int64_t batch_end,
                                                                          ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  sql_string.reset();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(dst_tenant_id_);
  if (OB_UNLIKELY(src_partition_ids.count() != dest_partition_ids.count() || batch_end < batch_start
      || batch_end >= dest_partition_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_partition_ids), K(dest_partition_ids), K(batch_start), K(batch_end));
  } else if (OB_FAIL(sql_string.append_fmt("UPDATE %s SET table_id=%ld, column_id=%ld, partition_id=(case partition_id",
        table_name, target_object_id_, new_col_id))) {
    LOG_WARN("fail to append sql string", K(ret), K(table_name), K(target_object_id_), K(new_col_id));
  } else {
    for (int64_t i = batch_start; OB_SUCC(ret) && i <= batch_end; i++) {
      const uint64_t src_partition_id = src_partition_ids.at(i);
      const uint64_t dest_partition_id = dest_partition_ids.at(i);
      if (OB_FAIL(sql_string.append_fmt(" when %ld then %ld", src_partition_id, dest_partition_id))) {
        LOG_WARN("fail to append sql string", K(ret), K(src_partition_id), K(dest_partition_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_string.append_fmt(" else partition_id end) where tenant_id=%ld and table_id=%ld and column_id=%ld",
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, dst_tenant_id_),
          batch_start == 0 ? object_id_ : target_object_id_, batch_start == 0 ? old_col_id : new_col_id))) {
      LOG_WARN("fail to append sql string", K(ret), K(object_id_), K(dst_tenant_id_), K(exec_tenant_id), K(old_col_id));
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::sync_tablet_autoinc_seq()
{
  int ret = OB_SUCCESS;
  if (!sync_tablet_autoinc_seq_ctx_.is_inited()
      && OB_FAIL(sync_tablet_autoinc_seq_ctx_.init(tenant_id_/*src_tenant_id*/, dst_tenant_id_, object_id_, target_object_id_))) {
    LOG_WARN("failed to init sync tablet autoinc seq ctx", K(ret));
  } else if (OB_FAIL(sync_tablet_autoinc_seq_ctx_.sync())) {
    LOG_WARN("failed to sync tablet autoinc seq", K(ret));
  }
  return ret;
}

int ObDDLRedefinitionTask::check_need_rebuild_constraint(const ObTableSchema &table_schema,
                                                         ObIArray<uint64_t> &constraint_ids,
                                                         bool &need_rebuild_constraint)
{
  int ret = OB_SUCCESS;
  need_rebuild_constraint = true;
  const int64_t CONSTRAINT_ID_BUCKET_NUM = 7;
  ObHashSet<uint64_t> new_constraints_id_set; // newly added csts has already added into dest table schema at do_offline_ddl_in_trans stage.
  const AlterTableSchema &alter_table_schema = alter_table_arg_.alter_table_schema_;
  if (OB_FAIL(new_constraints_id_set.create(CONSTRAINT_ID_BUCKET_NUM))) {
    LOG_WARN("create alter constraint id set failed", K(ret));
  } else if (obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg_.alter_constraint_type_
             || obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE == alter_table_arg_.alter_constraint_type_) {
    for (ObTableSchema::const_constraint_iterator iter = alter_table_schema.constraint_begin();
        OB_SUCC(ret) && iter != alter_table_schema.constraint_end(); ++iter) {
      if (OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is NULL", K(ret));
      } else if ((*iter)->get_constraint_type() != CONSTRAINT_TYPE_PRIMARY_KEY
        && OB_FAIL(new_constraints_id_set.set_refactored((*iter)->get_constraint_id()))) {
        LOG_WARN("push back constraint id failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();
        OB_SUCC(ret) && iter != table_schema.constraint_end(); ++iter) {
      if (OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is NULL", K(ret));
      } else if ((*iter)->get_constraint_type() != CONSTRAINT_TYPE_PRIMARY_KEY) {
        const uint64_t constraint_id = (*iter)->get_constraint_id();
        // There are some csts not included in alter_table_schema, which means csts have rebuilt successfully.
        need_rebuild_constraint = OB_HASH_NOT_EXIST == new_constraints_id_set.exist_refactored(constraint_id) ?
                                                        false : need_rebuild_constraint;
        if (OB_FAIL(constraint_ids.push_back(constraint_id))) {
          LOG_WARN("push back constraint id failed", K(ret));
        } else {
          LOG_INFO("constraint already built", K(**iter), K(constraint_ids));
        }
      }
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::check_need_check_table_empty(bool &need_check_table_empty)
{
  int ret = OB_SUCCESS;
  need_check_table_empty = false;
  const AlterTableSchema &alter_table_schema = alter_table_arg_.alter_table_schema_;
  ObTableSchema::const_column_iterator it_begin = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
  AlterColumnSchema *alter_column_schema = NULL;
  for(; OB_SUCC(ret) && !need_check_table_empty && it_begin != it_end; it_begin++) {
    if (OB_ISNULL(*it_begin)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("*it_begin is NULL", K(ret));
    } else {
      alter_column_schema = static_cast<AlterColumnSchema *>(*it_begin);
      if (OB_DDL_ADD_COLUMN == alter_column_schema->alter_type_
          && alter_column_schema->has_not_null_constraint()
          && alter_column_schema->get_orig_default_value().is_null()
          && !alter_column_schema->is_identity_column()
          && !alter_column_schema->is_xmltype()) { // xmltype main col orig default is always null
        need_check_table_empty = true;
      }
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::get_child_task_ids(char *buf, int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  MEMSET(buf, 0, len);
  TCRLockGuard guard(lock_);
  common::hash::ObHashMap<uint64_t, DependTaskStatus> ::const_iterator iter =
    dependent_task_result_map_.begin();
  if (OB_FAIL(databuff_printf(buf, MAX_LONG_OPS_MESSAGE_LENGTH, pos, "[ "))) {
    LOG_WARN("failed to print", K(ret));
  } else {
    while (OB_SUCC(ret) && iter != dependent_task_result_map_.end()) {
      const int64_t child_task_id = iter->second.task_id_;
      if (OB_FAIL(databuff_printf(buf,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "%ld ",
                                  child_task_id))) {
        LOG_WARN("failed to print", K(ret));
      }
      ++iter;
    }
    if (OB_SUCC(ret)) {
      databuff_printf(buf, MAX_LONG_OPS_MESSAGE_LENGTH, pos, "]");
    }
  }
  return ret;
}

ObSyncTabletAutoincSeqCtx::ObSyncTabletAutoincSeqCtx()
  : is_inited_(false), is_synced_(false), need_renew_location_(false), src_tenant_id_(OB_INVALID_ID), dst_tenant_id_(OB_INVALID_ID), orig_src_tablet_ids_(),
    src_tablet_ids_(), dest_tablet_ids_(), autoinc_params_()
{}

int ObSyncTabletAutoincSeqCtx::init(
    const uint64_t src_tenant_id,
    const uint64_t dst_tenant_id,
    int64_t src_table_id,
    int64_t dest_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == src_tenant_id || OB_INVALID_ID == dst_tenant_id
              || src_table_id == OB_INVALID_ID || dest_table_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_tenant_id), K(dst_tenant_id), K(src_table_id), K(dest_table_id));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(src_tenant_id, src_table_id, orig_src_tablet_ids_))) {
    LOG_WARN("failed to get data table snapshot", K(ret));
  } else if (OB_FAIL(src_tablet_ids_.assign(orig_src_tablet_ids_))) {
    LOG_WARN("failed to assign src_tablet_ids", K(ret));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(dst_tenant_id, dest_table_id, dest_tablet_ids_))) {
    LOG_WARN("failed to get dest table snapshot", K(ret));
  } else {
    src_tenant_id_ = src_tenant_id;
    dst_tenant_id_ = dst_tenant_id;
    is_synced_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObSyncTabletAutoincSeqCtx::sync()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSyncTabletAutoincSeqCtx has not been inited", K(ret));
  } else if (!is_synced_) {
    obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
    if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("rpc proxy or location_cache is null", K(ret), KP(srv_rpc_proxy));
    } else {
      while (OB_SUCC(ret) && src_tablet_ids_.count() > 0) {
        ObBatchGetTabletAutoincSeqProxy proxy(*srv_rpc_proxy, &obrpc::ObSrvRpcProxy::batch_get_tablet_autoinc_seq);
        obrpc::ObBatchGetTabletAutoincSeqArg arg;
        if (OB_FAIL(call_and_process_all_tablet_autoinc_seqs(proxy, arg, true/*is_get*/))) {
          LOG_WARN("failed to call and process", K(ret));
        }
      }
      while (OB_SUCC(ret) && autoinc_params_.count() > 0) {
        ObBatchSetTabletAutoincSeqProxy proxy(*srv_rpc_proxy, &obrpc::ObSrvRpcProxy::batch_set_tablet_autoinc_seq);
        obrpc::ObBatchSetTabletAutoincSeqArg arg;
        if (OB_FAIL(call_and_process_all_tablet_autoinc_seqs(proxy, arg, false/*is_get*/))) {
          LOG_WARN("failed to call and process", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        is_synced_ = true;
      }
    }
  }
  if (OB_LS_NOT_EXIST == ret || is_location_service_renew_error(ret)) {
    need_renew_location_ = true;
  }
  return ret;
}

int ObSyncTabletAutoincSeqCtx::build_ls_to_tablet_map(
    share::ObLocationService *location_service,
    const uint64_t tenant_id,
    const ObIArray<ObMigrateTabletAutoincSeqParam> &autoinc_params,
    const int64_t timeout,
    const bool force_renew,
    const bool by_src_tablet,
    ObHashMap<ObLSID, ObSEArray<ObMigrateTabletAutoincSeqParam, 1>> &map)
{
  int ret = OB_SUCCESS;
  map.reuse();
  bool is_cache_hit = false;
  const int64_t expire_renew_time = force_renew ? INT64_MAX : 0;
  ObTimeoutCtx timeout_ctx;
  if (nullptr == location_service || OB_INVALID_ID == tenant_id || autoinc_params.count() <= 0 || timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(location_service), K(tenant_id), K(autoinc_params), K(timeout));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(timeout))) {
    LOG_WARN("set trx timeout failed", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_timeout(timeout))) {
    LOG_WARN("set timeout failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); i++) {
      ObLSID ls_id;
      const ObMigrateTabletAutoincSeqParam &autoinc_param = autoinc_params.at(i);
      const ObTabletID &tablet_id = by_src_tablet ? autoinc_param.src_tablet_id_ : autoinc_param.dest_tablet_id_;
      ObSEArray<ObMigrateTabletAutoincSeqParam, 1> tmp_list;
      if (OB_FAIL(location_service->get(tenant_id, tablet_id, expire_renew_time, is_cache_hit, ls_id))) {
        LOG_WARN("fail to get log stream id", K(ret), K(tablet_id));
      } else if (OB_FAIL(map.get_refactored(ls_id, tmp_list))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get from map", K(ret), K(ls_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tmp_list.push_back(autoinc_param))) {
        LOG_WARN("failed to push tablet id", K(ret), K(tablet_id));
      } else if (OB_FAIL(map.set_refactored(ls_id, tmp_list, 1/*overwrite*/))) {
        LOG_WARN("failed to set map", K(ret), K(ls_id), K(tmp_list));
      }
    }
  }
  return ret;
}

template<typename P, typename A>
int ObSyncTabletAutoincSeqCtx::call_and_process_all_tablet_autoinc_seqs(P &proxy, A &arg, const bool is_get)
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = 0;
  const bool force_renew = false;
  const int64_t tablet_count = src_tablet_ids_.count();
  share::ObLocationService *location_service = nullptr;
  ObHashMap<ObLSID, ObSEArray<ObMigrateTabletAutoincSeqParam, 1>> ls_to_tablet_map;
  const uint64_t target_tenant_id = is_get ? src_tenant_id_ : dst_tenant_id_;
  if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("location_cache is null", K(ret));
  } else if (OB_FAIL(ls_to_tablet_map.create(MAP_BUCKET_NUM, "DDLRedefTmp"))) {
    LOG_WARN("failed to create map", K(ret));
  } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tablet_count, rpc_timeout))) {
    LOG_WARN("failed to get ddl rpc timeout", K(ret));
  } else {
    if (is_get) {
      ObSEArray<ObMigrateTabletAutoincSeqParam, 1> tmp_autoinc_params;
      if (OB_UNLIKELY(src_tablet_ids_.count() != dest_tablet_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet ids count", K(ret), K(src_tablet_ids_), K(dest_tablet_ids_));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < src_tablet_ids_.count(); i++) {
        ObMigrateTabletAutoincSeqParam autoinc_param;
        autoinc_param.src_tablet_id_ = src_tablet_ids_.at(i);
        autoinc_param.dest_tablet_id_ = dest_tablet_ids_.at(i);
        if (OB_FAIL(tmp_autoinc_params.push_back(autoinc_param))) {
          LOG_WARN("failed to push back", K(ret), K(autoinc_param));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(build_ls_to_tablet_map(location_service,
                                                         target_tenant_id,
                                                         tmp_autoinc_params,
                                                         rpc_timeout,
                                                         need_renew_location_,
                                                         true/*by src tablet*/,
                                                         ls_to_tablet_map))) {
        LOG_WARN("failed to build ls to tabmap", K(ret));
      }
    } else {
      if (OB_FAIL(build_ls_to_tablet_map(location_service,
                                         target_tenant_id,
                                         autoinc_params_,
                                         rpc_timeout,
                                         need_renew_location_,
                                         false/*by src tablet*/,
                                         ls_to_tablet_map))) {
        LOG_WARN("failed to build ls to tabmap", K(ret));
      }
    }
  }

  // prepeare rpc arg
  if (OB_SUCC(ret)) {
    ObHashMap<ObLSID, ObSEArray<ObMigrateTabletAutoincSeqParam, 1>>::hashtable::const_iterator map_iter = ls_to_tablet_map.begin();
    for (; OB_SUCC(ret) && map_iter != ls_to_tablet_map.end(); ++map_iter) {
      const ObLSID &ls_id = map_iter->first;
      ObAddr leader_addr;
      if (OB_FAIL(location_service->get_leader(GCONF.cluster_id,
                                               target_tenant_id,
                                               ls_id,
                                               force_renew,
                                               leader_addr))) {
        LOG_WARN("failed to get leader", K(ret));
      } else if (OB_FAIL(arg.init(target_tenant_id, ls_id, map_iter->second))) {
        LOG_WARN("failed to init arg", K(ret));
      } else if (OB_FAIL(proxy.call(leader_addr, rpc_timeout, target_tenant_id, arg))) {
        LOG_WARN("send rpc failed", K(ret), K(arg), K(leader_addr));
      }
    }

    // wait rpc and process result
    int tmp_ret = OB_SUCCESS;
    common::ObArray<int> tmp_ret_array;
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(tmp_ret_array))) {
      LOG_WARN("rpc proxy wait failed", K(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      const auto &result_array = proxy.get_results();
      if (tmp_ret_array.count() != ls_to_tablet_map.size() || result_array.count() != ls_to_tablet_map.size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result count not match", K(ret), K(ls_to_tablet_map.size()), K(tmp_ret_array.count()), K(result_array.count()));
      } else {
        ObHashMap<ObLSID, ObSEArray<ObMigrateTabletAutoincSeqParam, 1>>::hashtable::iterator map_iter = ls_to_tablet_map.begin();
        int64_t new_params_cnt = 0;

        // check and reserve first
        for (int64_t i = 0; OB_SUCC(ret) && i < result_array.count(); ++i, ++map_iter) {
          const int rpc_ret_code = tmp_ret_array.at(i);
          const auto *cur_result = result_array.at(i);
          if (OB_ISNULL(cur_result) || OB_UNLIKELY(map_iter == ls_to_tablet_map.end())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result it null", K(ret), K(i), KP(cur_result));
          } else if (OB_SUCCESS == rpc_ret_code) {
            if (OB_UNLIKELY(map_iter->second.count() != cur_result->autoinc_params_.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("result params count must be equal to request params count when rpc succ",
                K(ret), K(map_iter->second.count()), K(cur_result->autoinc_params_.count()));
            }
            for (int64_t j = 0; OB_SUCC(ret) && j < cur_result->autoinc_params_.count(); j++) {
              const ObMigrateTabletAutoincSeqParam &autoinc_param = cur_result->autoinc_params_.at(j);
              if (OB_SUCCESS == autoinc_param.ret_code_) {
                if (is_get) {
                  new_params_cnt++;
                }
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (is_get && OB_FAIL(autoinc_params_.reserve(new_params_cnt))) {
          LOG_WARN("failed to reserve new param cnt", K(ret));
        }

        // the following won't fail
        if (OB_SUCC(ret)) {
          if (is_get) {
            src_tablet_ids_.reuse();
            dest_tablet_ids_.reuse();
          } else {
            autoinc_params_.reuse();
          }
          tmp_ret = OB_SUCCESS; // last non-retry error or first retry error or success
          map_iter = ls_to_tablet_map.begin();
          for (int64_t i = 0; OB_SUCC(ret) && i < result_array.count(); ++i, ++map_iter) {
            const int rpc_ret_code = tmp_ret_array.at(i);
            const auto *cur_result = result_array.at(i);
            const ObIArray<ObMigrateTabletAutoincSeqParam> *result_params = nullptr;
            if (OB_SUCCESS == rpc_ret_code) {
              result_params = &cur_result->autoinc_params_;
            } else {
              for (int64_t j = 0; j < map_iter->second.count(); j++) {
                ObMigrateTabletAutoincSeqParam &autoinc_param = map_iter->second.at(j);
                autoinc_param.ret_code_ = rpc_ret_code;
              }
              result_params = &map_iter->second;
            }
            for (int64_t j = 0; OB_SUCC(ret) && j < result_params->count(); j++) {
              const ObMigrateTabletAutoincSeqParam &autoinc_param = result_params->at(j);
              if (OB_SUCCESS == autoinc_param.ret_code_) {
                if (is_get) {
                  if (OB_FAIL(autoinc_params_.push_back(autoinc_param))) {
                    LOG_WARN("failed to push autoinc param", K(ret), K(autoinc_param));
                  }
                } else {
                  // do nothing
                }
              } else {
                // reclaim on failure
                if (is_get) {
                  if (OB_FAIL(src_tablet_ids_.push_back(autoinc_param.src_tablet_id_))) {
                    LOG_WARN("failed to push src tablet id", K(ret), K(autoinc_param));
                  } else if (OB_FAIL(dest_tablet_ids_.push_back(autoinc_param.dest_tablet_id_))) {
                    LOG_WARN("failed to push dest tablet id", K(ret), K(autoinc_param));
                  }
                } else {
                  if (OB_FAIL(autoinc_params_.push_back(autoinc_param))) {
                    LOG_WARN("failed to push autoinc param", K(ret));
                  }
                }
                if (is_error_need_retry(autoinc_param.ret_code_)) {
                  if (tmp_ret == OB_SUCCESS) {
                    tmp_ret = autoinc_param.ret_code_;
                  }
                } else {
                  tmp_ret = autoinc_param.ret_code_;
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
        }
      }
    }

    if (ls_to_tablet_map.created()) {
      ls_to_tablet_map.destroy();
    }
  }
  if (OB_SUCC(ret) && is_get) {
    if (OB_UNLIKELY(orig_src_tablet_ids_.count() != autoinc_params_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid count", K(orig_src_tablet_ids_), K(autoinc_params_));
    }
  }
  return ret;
}

int ObDDLRedefinitionTask::reap_old_replica_build_task(bool &need_exec_new_inner_sql)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  const int64_t data_table_id = object_id_;
  const int64_t dest_table_id = target_object_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexBuildTask has not been inited", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(data_table_id));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("error unexpected, table schema must not be nullptr", K(ret));
  } else {
    const int64_t old_execution_id = get_execution_id();
    const ObTabletID unused_tablet_id;
    const ObDDLTaskInfo unused_addition_info;
    const int old_ret_code = OB_SUCCESS;
    ObAddr invalid_addr;
    if (old_execution_id < 0) {
      need_exec_new_inner_sql = true;
    } else if (OB_FAIL(ObCheckTabletDataComplementOp::check_and_wait_old_complement_task(tenant_id_, dest_table_id,
        task_id_, old_execution_id, invalid_addr, trace_id_,
        table_schema->get_schema_version(), snapshot_version_, need_exec_new_inner_sql))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to check and wait old complement task", K(ret));
      }
    } else if (!need_exec_new_inner_sql) {
      if (OB_FAIL(update_complete_sstable_job_status(unused_tablet_id, snapshot_version_, old_execution_id, old_ret_code, unused_addition_info))) {
        LOG_WARN("failed to wait and complete old task finished!", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObDDLRedefinitionTask::get_build_replica_request_time()
{
  TCRLockGuard guard(lock_);
  return build_replica_request_time_;
}
