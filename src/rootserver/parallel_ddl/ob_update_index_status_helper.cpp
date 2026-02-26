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

#include "rootserver/parallel_ddl/ob_update_index_status_helper.h"
#include "share/schema/ob_table_sql_service.h"
#include "src/storage/ddl/ob_ddl_lock.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObUpdateIndexStatusHelper::ObUpdateIndexStatusHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObUpdateIndexStatusArg &arg,
    obrpc::ObParallelDDLRes &res)
  : ObDDLHelper(schema_service, tenant_id, "[parallel update index status]"),
    arg_(arg),
    res_(res),
    orig_index_table_schema_(nullptr),
    new_data_table_schema_(nullptr),
    new_status_(arg_.status_),
    index_table_exist_(true),
    database_id_(OB_INVALID_ID)
{}

ObUpdateIndexStatusHelper::~ObUpdateIndexStatusHelper()
{
}

int ObUpdateIndexStatusHelper::lock_objects_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(lock_database_by_obj_name_())) {
    LOG_WARN("fail to lock databse by obj name", KR(ret));
  } else if (OB_FAIL(latest_schema_guard_.get_database_id(arg_.database_name_, database_id_))) {
    LOG_WARN("fail to get database id", KR(ret), K_(tenant_id), K_(arg_.database_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id_)) {
    ret = OB_ERR_PARALLEL_DDL_CONFLICT;
    LOG_WARN("invalid database_id, database name may changed", KR(ret), K_(tenant_id), K_(arg_.database_name));
  } else if (OB_FAIL(add_lock_object_by_id_(database_id_,
    share::schema::DATABASE_SCHEMA, transaction::tablelock::SHARE))) {
    LOG_WARN("fail to lock database id", KR(ret), K_(database_id));
  } else if (OB_FAIL(add_lock_object_by_id_(arg_.data_table_id_,
    share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
    LOG_WARN("fail to lock data table", KR(ret), K_(arg_.data_table_id));
  } else if (OB_FAIL(add_lock_object_by_id_(arg_.index_table_id_,
    share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
    LOG_WARN("fail to lock index table id", KR(ret), K_(arg_.index_table_id));
  } else if (OB_FAIL(lock_existed_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id", KR(ret));
  }
  RS_TRACE(lock_objects);
  return ret;
}

int ObUpdateIndexStatusHelper::lock_database_by_obj_name_()
{
  int ret = OB_SUCCESS;
  const ObString &database_name = arg_.database_name_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(add_lock_object_by_database_name_(database_name, transaction::tablelock::SHARE))) {
    LOG_WARN("fail to lock database by name", KR(ret), K_(tenant_id), K(database_name));
  } else if (OB_FAIL(lock_databases_by_name_())) {
    LOG_WARN("fail to lock databases by name", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObUpdateIndexStatusHelper::generate_schemas_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_data_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (INDEX_STATUS_INDEX_ERROR == new_status_ && arg_.convert_status_) {
    const ObTenantSchema *tenant_schema = nullptr;
    if (OB_FAIL(latest_schema_guard_.get_tenant_schema(tenant_id_, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", KR(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K_(tenant_id));
    } else if (tenant_schema->is_restore()) {
      new_status_ = INDEX_STATUS_RESTORE_INDEX_ERROR;
      LOG_INFO("conver error index status", KR(ret), K_(new_status));
    }
  }
  const ObDatabaseSchema *database_schema = NULL;
  if (FAILEDx(latest_schema_guard_.get_table_schema(arg_.index_table_id_, orig_index_table_schema_))) {
    LOG_WARN("fail to get index table schema", KR(ret), K_(arg_.index_table_id));
  } else if (OB_ISNULL(orig_index_table_schema_)) {
    ret = OB_ERR_OBJECT_NOT_EXIST;
    index_table_exist_ = false;
    LOG_WARN("index table schema is null, may be droped", KR(ret), K_(arg_.index_table_id));
  } else if (OB_UNLIKELY(database_id_ != orig_index_table_schema_->get_database_id())) {
    ret = OB_ERR_PARALLEL_DDL_CONFLICT;
    LOG_WARN("databse_id_ is not euqal to index_table's database_id",
             KR(ret), K_(database_id), K(orig_index_table_schema_->get_database_id()));
  } else if (OB_FAIL(latest_schema_guard_.get_database_schema(database_id_, database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret), K_(tenant_id), K_(database_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("databse_schema is null", KR(ret));
  } else if (OB_UNLIKELY(database_schema->get_database_name_str() != arg_.database_name_)) {
    ret = OB_ERR_PARALLEL_DDL_CONFLICT;
    LOG_WARN("database_schema's database name not equal to arg",
             KR(ret), K(database_schema->get_database_name_str()), K_(arg_.database_name));
  } else if (is_available_index_status(new_status_) && !orig_index_table_schema_->is_unavailable_index()) {
    ret = OB_EAGAIN;
    LOG_WARN("set index status to available, but previous status is not unavailable, which is not expected", KR(ret));
  } else if (OB_FAIL(latest_schema_guard_.get_table_schema(arg_.data_table_id_, orig_data_table_schema))) {
    LOG_WARN("fail to get data table schema", KR(ret), K_(arg_.data_table_id));
  } else if (OB_ISNULL(orig_data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail data table is null", KR(ret));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, *orig_data_table_schema, new_data_table_schema_))) {
    LOG_WARN("fail to alloc new table schema", KR(ret));
  } else {
    new_data_table_schema_->set_in_offline_ddl_white_list(arg_.in_offline_ddl_white_list_);
  }
  RS_TRACE(check_schemas);
  return ret;
}

int ObUpdateIndexStatusHelper::calc_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    // update index status
    schema_version_cnt_ = 1;
    // update data table schema version
    schema_version_cnt_++;
    // 1503
    schema_version_cnt_++;
  }
  return ret;
}

int ObUpdateIndexStatusHelper::operate_schemas_()
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_->get_schema_service();
  const ObString *ddl_stmt_str = arg_.ddl_stmt_str_.empty() ? nullptr : &arg_.ddl_stmt_str_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is nullptr", KR(ret));
  } else if (OB_ISNULL(orig_index_table_schema_)
             || OB_ISNULL(new_data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", KR(ret), KP(orig_index_table_schema_), KP(new_data_table_schema_));
  } else {
    ObSchemaService *schema_service = schema_service_->get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is nullptr", KR(ret));
    } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
      LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(schema_service->get_table_sql_service().update_index_status(
      *new_data_table_schema_, arg_.index_table_id_, new_status_, new_schema_version, get_trans_(), ddl_stmt_str))) {
      LOG_WARN("fail to update index status", KR(ret), K_(arg_.index_table_id), K_(arg_.data_table_id), K_(new_status));
    } else if (arg_.task_id_ != 0) {
      ObSchemaVersionGenerator *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
      int64_t consensus_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(tsi_generator->get_end_version(consensus_schema_version))) {
        LOG_WARN("fail to get end version", KR(ret), K_(tenant_id), K_(arg));
      } else if (OB_FAIL(ObDDLTaskRecordOperator::update_consensus_schema_version(
                         get_trans_(), tenant_id_, arg_.task_id_, consensus_schema_version))) {
        LOG_WARN("fail to update consensus_schema_version", KR(ret), K_(tenant_id), K_(arg_.task_id), K(consensus_schema_version));
      } else if (orig_index_table_schema_->get_index_status() != new_status_ && new_status_ == INDEX_STATUS_AVAILABLE) {
        ObTableLockOwnerID owner_id;
        if (OB_ISNULL(new_data_table_schema_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data_table_schema is null", KR(ret));
        } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                       arg_.task_id_))) {
          LOG_WARN("failed to get owner id", K(ret), K_(arg_.task_id));
        } else if (OB_FAIL(ObDDLLock::unlock_for_add_drop_index(*new_data_table_schema_,
                                                                orig_index_table_schema_->get_table_id(),
                                                                orig_index_table_schema_->is_global_index_table(),
                                                                owner_id,
                                                                get_trans_()))) {
          LOG_WARN("failed to unlock ddl lock", KR(ret));
        }
      }
    }
  }
  RS_TRACE(alter_schemas);
  return ret;
}

int ObUpdateIndexStatusHelper::init_()
{
  int ret = OB_SUCCESS;
  parallel_ddl_type_ = "[parallel update index status]";
  return ret;
}

int ObUpdateIndexStatusHelper::operation_before_commit_()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObUpdateIndexStatusHelper::clean_on_fail_commit_()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObUpdateIndexStatusHelper::construct_and_adjust_result_(int &return_ret)
{
  int ret = return_ret;
  if (FAILEDx(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObSchemaVersionGenerator *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
    if (OB_ISNULL(tsi_generator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsi schema version generator is null", KR(ret));
    } else {
      tsi_generator->get_current_version(res_.schema_version_);
    }
  }

  if (!index_table_exist_) {
    // regard as the index table has been dropped and scheduler will clean up the task record in success state.
    ret = OB_SUCCESS;
  }
  return ret;
}