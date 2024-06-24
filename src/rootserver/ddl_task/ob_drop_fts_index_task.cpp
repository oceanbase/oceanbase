/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "rootserver/ddl_task/ob_drop_fts_index_task.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "rootserver/ob_root_service.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace rootserver
{

ObDropFTSIndexTask::ObDropFTSIndexTask()
  : ObDDLTask(DDL_DROP_FTS_INDEX),
    root_service_(nullptr),
    rowkey_doc_(),
    doc_rowkey_(),
    domain_index_(),
    fts_doc_word_()
{
}

ObDropFTSIndexTask::~ObDropFTSIndexTask()
{
}

int ObDropFTSIndexTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const uint64_t data_table_id,
    const ObDDLType ddl_type,
    const ObFTSDDLChildTaskInfo &rowkey_doc,
    const ObFTSDDLChildTaskInfo &doc_rowkey,
    const ObFTSDDLChildTaskInfo &domain_index,
    const ObFTSDDLChildTaskInfo &fts_doc_word,
    const int64_t schema_version,
    const int64_t consumer_group_id)
{
  int ret = OB_SUCCESS;
  const bool is_fts_task = ddl_type == DDL_DROP_FTS_INDEX;

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
               || task_id <= 0
               || OB_INVALID_ID == data_table_id
               || !domain_index.is_valid()
               || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(data_table_id),
        K(domain_index), K(schema_version));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", K(ret));
  } else if (OB_FAIL(rowkey_doc_.deep_copy_from_other(rowkey_doc, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(rowkey_doc));
  } else if (OB_FAIL(doc_rowkey_.deep_copy_from_other(doc_rowkey, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(doc_rowkey));
  } else if (OB_FAIL(domain_index_.deep_copy_from_other(domain_index, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(domain_index));
  } else if (is_fts_task && OB_FAIL(fts_doc_word_.deep_copy_from_other(fts_doc_word, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(fts_doc_word));
  } else {
    task_type_ = ddl_type;
    set_gmt_create(ObTimeUtility::current_time());
    tenant_id_ = tenant_id;
    object_id_ = data_table_id;
    target_object_id_ = domain_index.table_id_;
    schema_version_ = schema_version;
    task_id_ = task_id;
    parent_task_id_ = 0; // no parent task
    consumer_group_id_ = consumer_group_id;
    task_version_ = OB_DROP_FTS_INDEX_TASK_VERSION;
    dst_tenant_id_ = tenant_id;
    dst_schema_version_ = schema_version;
    is_inited_ = true;
  }
  return ret;
}

int ObDropFTSIndexTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(!task_record.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret));
  } else {
    task_type_ = task_record.ddl_type_;
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
    pos = 0;
    if (OB_ISNULL(task_record.message_.ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, task record message is nullptr", K(ret), K(task_record));
    } else if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(),
            task_record.message_.length(), pos))) {
      LOG_WARN("deserialize params from message failed", K(ret));
    } else {
      is_inited_ = true;
      // set up span during recover task
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

int ObDropFTSIndexTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropIndexTask has not been inited", K(ret));
  } else if (!need_retry()) {
    // task is done
  } else if (OB_FAIL(check_switch_succ())) {
    LOG_WARN("check need retry failed", K(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
      case ObDDLTaskStatus::PREPARE:
        if (OB_FAIL(prepare(WAIT_CHILD_TASK_FINISH))) {
          LOG_WARN("fail to prepare", K(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH:
        if (OB_FAIL(check_and_wait_finish(SUCCESS))) {
          LOG_WARN("fail to check and wait task", K(ret));
        }
        break;
      case ObDDLTaskStatus::SUCCESS:
        if (OB_FAIL(succ())) {
          LOG_WARN("do succ procedure failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::FAIL:
        if (OB_FAIL(fail())) {
          LOG_WARN("do fail procedure failed", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, task status is not valid", K(ret), K(task_status_));
    }
    ddl_tracing_.release_span_hierarchy();
  }
  return ret;
}

int ObDropFTSIndexTask::serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_size, pos))) {
    LOG_WARN("fail to ObDDLTask::serialize", K(ret));
  } else if (OB_FAIL(rowkey_doc_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize aux rowkey doc table info", K(ret), K(rowkey_doc_));
  } else if (OB_FAIL(doc_rowkey_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize aux doc rowkey table info", K(ret), K(doc_rowkey_));
  } else if (OB_FAIL(domain_index_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize aux fts index table info", K(ret), K(domain_index_));
  } else if (OB_FAIL(fts_doc_word_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize aux doc word aux table info", K(ret), K(fts_doc_word_));
  }
  return ret;
}

int ObDropFTSIndexTask::deserialize_params_from_message(
    const uint64_t tenant_id,
    const char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  obrpc::ObDropIndexArg tmp_drop_index_arg;
  ObFTSDDLChildTaskInfo tmp_info;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, buf_size, pos))) {
    LOG_WARN("fail to ObDDLTask::deserialize", K(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize aux rowkey doc table info", K(ret));
  } else if (OB_FAIL(rowkey_doc_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize aux doc rowkey table info", K(ret));
  } else if (OB_FAIL(doc_rowkey_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize aux fts index table info", K(ret));
  } else if (OB_FAIL(domain_index_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize aux doc word table info", K(ret));
  } else if (OB_FAIL(fts_doc_word_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  }
  return ret;
}

int64_t ObDropFTSIndexTask::get_serialize_param_size() const
{
  return ObDDLTask::get_serialize_param_size()
       + rowkey_doc_.get_serialize_size()
       + doc_rowkey_.get_serialize_size()
       + domain_index_.get_serialize_size()
       + fts_doc_word_.get_serialize_size();
}

void ObDropFTSIndexTask::flt_set_task_span_tag() const
{
  // TODO: @hanxuan, add me for tracing.
}

void ObDropFTSIndexTask::flt_set_status_span_tag() const
{
  // TODO: @hanxuan, add me for tracing.
}

int ObDropFTSIndexTask::check_switch_succ()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_domain_index_exist = false;
  bool is_doc_word_exist = false;
  bool is_rowkey_doc_exist = false;
  bool is_doc_rowkey_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hasn't initialized", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", K(ret));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, domain_index_.table_id_, is_domain_index_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(domain_index_));
  } else if (OB_FAIL(is_fts_task() && schema_guard.check_table_exist(tenant_id_, fts_doc_word_.table_id_, is_doc_word_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(fts_doc_word_));
  } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, doc_rowkey_.table_id_, is_doc_rowkey_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(doc_rowkey_));
  } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, rowkey_doc_.table_id_, is_rowkey_doc_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(rowkey_doc_));
  } else if (!is_domain_index_exist && !is_doc_word_exist && !is_rowkey_doc_exist && !is_doc_rowkey_exist) {
    task_status_ = ObDDLTaskStatus::SUCCESS;
  }
  return ret;
}

int ObDropFTSIndexTask::prepare(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool has_finished = false;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropFTSIndexTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (0 == domain_index_.task_id_
      && OB_FAIL(create_drop_index_task(schema_guard, domain_index_.table_id_, domain_index_.index_name_, domain_index_.task_id_))) {
      LOG_WARN("fail to create drop index task", K(ret), K(domain_index_));
  } else if (is_fts_task()
      && 0 == fts_doc_word_.task_id_
      && OB_FAIL(create_drop_index_task(schema_guard, fts_doc_word_.table_id_, fts_doc_word_.index_name_, fts_doc_word_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(fts_doc_word_));
  } else if (OB_FAIL(wait_fts_child_task_finish(has_finished))) {
    LOG_WARN("fail to wait fts child task finish", K(ret));
  }
  if (has_finished) {
    // overwrite return code
    if (OB_FAIL(switch_status(new_status, true/*enable_flt*/, ret))) {
      LOG_WARN("fail to switch status", K(ret), K(new_status));
    }
  }
  return ret;
}

int ObDropFTSIndexTask::check_and_wait_finish(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool has_finished = false;
  if (OB_FAIL(create_drop_doc_rowkey_task())) {
    LOG_WARN("fail to create drop doc rowkey child task", K(ret));
  } else if (0 == rowkey_doc_.task_id_ && 0 == doc_rowkey_.task_id_) {
    // If there are other fulltext indexes, there is no need to drop the rowkey doc auxiliary table. And the task
    // status is set to success and skipped.
    has_finished = true;
  } else if (OB_FAIL(wait_doc_child_task_finish(has_finished))) {
    LOG_WARN("fail to wait doc child task finish", K(ret));
  }
  if (has_finished) {
    // overwrite return code
    if (OB_FAIL(switch_status(new_status, true/*enable_flt*/, ret))) {
      LOG_WARN("fail to switch status", K(ret), K(new_status));
    }
  }
  return ret;
}

int ObDropFTSIndexTask::check_drop_index_finish(
    const uint64_t tenant_id,
    const int64_t task_id,
    const int64_t table_id,
    bool &has_finished)
{
  int ret = OB_SUCCESS;
  const ObAddr unused_addr;
  int64_t unused_user_msg_len = 0;
  share::ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
  has_finished = false;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0 || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(table_id));
  } else if (OB_FAIL(share::ObDDLErrorMessageTableOperator::get_ddl_error_message(
                                                       tenant_id,
                                                       task_id,
                                                       -1/*target_object_id*/,
                                                       table_id,
                                                       *GCTX.sql_proxy_,
                                                       error_message,
                                                       unused_user_msg_len))) {
    LOG_WARN("fail to get ddl error message", K(ret), K(tenant_id), K(task_id), K(table_id));
  } else {
    ret = error_message.ret_code_;
    has_finished = true;
  }
  LOG_INFO("wait build index finish", K(ret), K(tenant_id), K(task_id), K(table_id), K(has_finished));
  return ret;
}

int ObDropFTSIndexTask::wait_child_task_finish(
    const common::ObIArray<ObFTSDDLChildTaskInfo> &child_task_ids,
    bool &has_finished)
{
  int ret = OB_SUCCESS;
  if (0 == child_task_ids.count()) {
    has_finished = true;
  } else {
    bool finished = true;
    for (int64_t i = 0; OB_SUCC(ret) && finished && i < child_task_ids.count(); ++i) {
      const ObFTSDDLChildTaskInfo &task_info = child_task_ids.at(i);
      finished = false;
      if (-1 == task_info.task_id_ || task_info.table_id_ == OB_INVALID_ID) {
        finished = true;
      } else if (OB_FAIL(check_drop_index_finish(tenant_id_, task_info.task_id_, task_info.table_id_, finished))) {
        LOG_WARN("fail to check fts index child task finish", K(ret));
      } else if (!finished) { // nothing to do
        LOG_INFO("child task hasn't been finished", K(tenant_id_), K(task_info));
      }
    }
    if (OB_SUCC(ret) && finished) {
      has_finished = true;
    }
  }
  return ret;
}

int ObDropFTSIndexTask::wait_fts_child_task_finish(bool &has_finished)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObFTSDDLChildTaskInfo, 2> fts_child_tasks;
  if (OB_FAIL(fts_child_tasks.push_back(domain_index_))) {
    LOG_WARN("fail to push back fts index child task", K(ret));
  } else if (is_fts_task() && OB_FAIL(fts_child_tasks.push_back(fts_doc_word_))) {
    LOG_WARN("fail to push back doc word child task", K(ret));
  } else if (OB_FAIL(wait_child_task_finish(fts_child_tasks, has_finished))) {
    LOG_WARN("fail to wait child task finish", K(ret), K(fts_child_tasks));
  }
  return ret;
}

int ObDropFTSIndexTask::wait_doc_child_task_finish(bool &has_finished)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObFTSDDLChildTaskInfo, 2> doc_child_tasks;
  if (OB_FAIL(doc_child_tasks.push_back(doc_rowkey_))) {
    LOG_WARN("fail to push back doc rowkey child task", K(ret));
  } else if (OB_FAIL(doc_child_tasks.push_back(rowkey_doc_))) {
    LOG_WARN("fail to push back rowkey doc child task", K(ret));
  } else if (OB_FAIL(wait_child_task_finish(doc_child_tasks, has_finished))) {
    LOG_WARN("fail to wait child task finish", K(ret), K(doc_child_tasks));
  }
  return ret;
}

int ObDropFTSIndexTask::create_drop_index_task(
    share::schema::ObSchemaGetterGuard &guard,
    const uint64_t index_tid,
    const common::ObString &index_name,
    int64_t &task_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  bool is_index_exist = false;
  if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_INVALID_ID == index_tid) {
    // nothing to do, just by pass.
    task_id = -1;
  } else if (OB_UNLIKELY(index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_name));
  } else if (OB_FAIL(guard.check_table_exist(tenant_id_, index_tid, is_index_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(index_tid));
  } else if (!is_index_exist) {
    // nothing to do, just by pass.
    task_id = -1;
  } else if (OB_FAIL(guard.get_table_schema(tenant_id_, index_tid, index_schema))) {
    LOG_WARN("fail to get index table schema", K(ret), K(tenant_id_), K(index_tid));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, index schema is nullptr", K(ret), KP(index_schema));
  } else if (OB_FAIL(guard.get_database_schema(tenant_id_, index_schema->get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K(index_schema->get_database_id()));
  } else if (OB_FAIL(guard.get_table_schema(tenant_id_, index_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret), K(index_schema->get_data_table_id()));
  } else if (OB_UNLIKELY(nullptr == database_schema || nullptr == data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, schema is nullptr", K(ret), KP(database_schema), KP(data_table_schema));
  } else {
    int64_t ddl_rpc_timeout_us = 0;
    obrpc::ObDropIndexArg arg;
    obrpc::ObDropIndexRes res;
    arg.is_inner_            = true;
    arg.tenant_id_           = tenant_id_;
    arg.exec_tenant_id_      = tenant_id_;
    arg.index_table_id_      = index_tid;
    arg.session_id_          = data_table_schema->get_session_id();
    arg.index_name_          = index_name;
    arg.table_name_          = data_table_schema->get_table_name();
    arg.database_name_       = database_schema->get_database_name_str();
    arg.index_action_type_   = obrpc::ObIndexArg::DROP_INDEX;
    arg.ddl_stmt_str_        = nullptr;
    arg.is_add_to_scheduler_ = true;
    arg.task_id_             = task_id_;
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(
            index_schema->get_all_part_num() + data_table_schema->get_all_part_num(), ddl_rpc_timeout_us))) {
      LOG_WARN("fail to get ddl rpc timeout", K(ret));
    } else if (OB_FAIL(root_service_->get_common_rpc_proxy().timeout(ddl_rpc_timeout_us).drop_index(arg, res))) {
      LOG_WARN("fail to drop index", K(ret), K(ddl_rpc_timeout_us), K(arg), K(res.task_id_));
    } else {
      task_id = res.task_id_;
    }
    LOG_INFO("drop index", K(ret), K(index_tid), K(index_name), K(task_id),
        "data table name", data_table_schema->get_table_name_str(),
        "database name", database_schema->get_database_name_str());
  }
  return ret;
}

int ObDropFTSIndexTask::create_drop_doc_rowkey_task()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (0 == rowkey_doc_.task_id_
      && OB_FAIL(create_drop_index_task(schema_guard, rowkey_doc_.table_id_, rowkey_doc_.index_name_, rowkey_doc_.task_id_))) {
      LOG_WARN("fail to create drop index task", K(ret), K(rowkey_doc_));
  } else if (0 == doc_rowkey_.task_id_
      && OB_FAIL(create_drop_index_task(schema_guard, doc_rowkey_.table_id_, doc_rowkey_.index_name_, doc_rowkey_.task_id_))) {
      LOG_WARN("fail to create drop index task", K(ret), K(doc_rowkey_));
  }
  return ret;
}

int ObDropFTSIndexTask::succ()
{
  return cleanup();
}

int ObDropFTSIndexTask::fail()
{
  return cleanup();
}

int ObDropFTSIndexTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", K(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(root_service_->get_sql_proxy(), tenant_id_, task_id_))) {
    LOG_WARN("delete task record failed", K(ret), K(task_id_), K(schema_version_));
  } else {
    need_retry_ = false;      // clean succ, stop the task
  }
  LOG_INFO("clean task finished", K(ret), K(*this));
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase
