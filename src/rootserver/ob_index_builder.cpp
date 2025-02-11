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
#include "ob_index_builder.h"

#include "share/ob_index_builder_util.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/ob_vec_index_builder_util.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "ob_root_service.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "storage/tablelock/ob_table_lock_service.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace palf;
namespace rootserver
{

ObIndexBuilder::ObIndexBuilder(ObDDLService &ddl_service)
  : ddl_service_(ddl_service)
{
}

ObIndexBuilder::~ObIndexBuilder()
{
}

int ObIndexBuilder::create_index(
    const ObCreateIndexArg &arg,
    obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start create index", K(arg));
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(do_create_index(arg, res))) {
    LOG_WARN("generate_schema failed", K(arg), K(ret));
  }
  if (OB_ERR_TABLE_EXIST == ret) {
    if (true == arg.if_not_exist_) {
      ret = OB_SUCCESS;
      LOG_USER_WARN(OB_ERR_KEY_NAME_DUPLICATE, arg.index_name_.length(), arg.index_name_.ptr());
    } else {
      ret = OB_ERR_KEY_NAME_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_KEY_NAME_DUPLICATE, arg.index_name_.length(), arg.index_name_.ptr());
    }
  }
  LOG_INFO("finish create index", K(arg), K(ret));
  return ret;
}

int ObIndexBuilder::drop_index_on_failed(const ObDropIndexArg &arg, obrpc::ObDropIndexRes &res)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  const bool is_index = false;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const ObTableSchema *data_table_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  bool is_db_in_recyclebin = false;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("failed to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("drop index on failed before version 4.3.3.0 is not supported", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop index on failed before version 4.3.3.0 is");
  } else if (OB_FALSE_IT(schema_guard.set_session_id(arg.session_id_))) {
  } else if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", K(ret), K(ddl_service_.is_inited()));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, arg.database_name_,
             arg.table_name_, is_index, data_table_schema, arg.is_hidden_))) {
    LOG_WARN("failed to get data table schema", K(arg), K(ret));
  } else if (NULL == data_table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to drop index on failed, data table not exist", K(ret), K(arg));
  } else if (arg.is_in_recyclebin_) {
    // internal delete index
  } else if (data_table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not drop index of table in recyclebin.", K(ret), K(arg));
  } else if (OB_FAIL(schema_guard.check_database_in_recyclebin(tenant_id,
            data_table_schema->get_database_id(), is_db_in_recyclebin))) {
    LOG_WARN("check database in recyclebin failed", K(ret), K(tenant_id));
  } else if (is_db_in_recyclebin) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("Can not drop index of db in recyclebin", K(ret), K(arg));
  } else if (!arg.is_add_to_scheduler_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not add to scheduler to drop, not expected", K(ret), K(arg));
  } else if (arg.index_ids_.count() <= 0) {
    res.task_id_ = -1; // no need to drop
    LOG_INFO("target indexes to be drop is empty", K(ret));
  } else {
    ObDDLOperator ddl_operator(ddl_service_.get_schema_service(), ddl_service_.get_sql_proxy());
    ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
    int64_t refreshed_schema_version = 0;
    ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
    ObDDLTaskRecord task_record;

    bool has_index_task = false;
    typedef common::ObSEArray<share::schema::ObTableSchema, 4> TableSchemaArray;
    SMART_VAR(TableSchemaArray, new_index_schemas) {
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_ids_.count(); ++i) {
        bool is_index_exist = false;
        const int64_t index_id = arg.index_ids_.at(i);
        const share::schema::ObTableSchema *index_table_schema= nullptr;
        if (OB_FAIL(schema_guard.check_table_exist(tenant_id, index_id, is_index_exist))) {
          LOG_WARN("check table exist failed", K(ret), K(tenant_id), K(index_id));
        } else if (!is_index_exist) { // skip
          LOG_INFO("vec index schema is nullptr", K(ret), K(index_id));
        } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_id, index_table_schema))) {
          LOG_WARN("fail to get index table schema", K(ret), K(tenant_id), K(index_id));
        } else if (OB_ISNULL(index_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index table nullptr", K(ret), K(index_id));
        } else if (OB_FAIL(new_index_schemas.push_back(*index_table_schema))) {
          LOG_WARN("fail to push index table schema", K(ret), KPC(index_table_schema));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (new_index_schemas.count() <= 0) {
        res.task_id_ = -1; // no need to drop
        LOG_INFO("target indexes to be drop is empty", K(ret));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
      } else {
        bool has_exist = false;
        const ObTableSchema &new_index_schema = new_index_schemas.at(new_index_schemas.count() - 1);
        if (OB_FAIL(submit_drop_index_task(trans, *data_table_schema, new_index_schemas, arg, nullptr/*inc_data_tablet_ids*/,
                                           nullptr/*del_data_tablet_ids*/, allocator, has_exist, task_record))) {
          LOG_WARN("submit drop index task failed", K(ret), K(task_record));
        } else if (has_exist) {
          res.task_id_ = task_record.task_id_;
        } else {
          res.tenant_id_ = new_index_schema.get_tenant_id();
          res.schema_version_ = new_index_schema.get_schema_version();
          res.task_id_ = task_record.task_id_;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObDDLTaskRecordOperator::update_parent_task_message(tenant_id,
            arg.task_id_, new_index_schemas.at(0), res.task_id_, res.task_id_,
            ObDDLUpdateParentTaskIDType::UPDATE_DROP_INDEX_TASK_ID, allocator, trans))) {
          LOG_WARN("fail to update parent task message", K(ret), K(arg.task_id_), K(res.task_id_));
        }
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN_RET(temp_ret, "trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
        LOG_WARN("fail to publish schema", K(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
        LOG_WARN("fail to schedule ddl task", K(tmp_ret), K(task_record));
      }
    }
  }
  LOG_INFO("finish drop index on failed", K(ret), K(arg));
  return ret;
}

int ObIndexBuilder::drop_index(const ObDropIndexArg &const_arg, obrpc::ObDropIndexRes &res)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = const_arg.tenant_id_;
  const bool is_index = false;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const ObTableSchema *table_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  bool is_db_in_recyclebin = false;
  bool ignore_for_domain_index = false;
  bool need_rename_index = true;
  ObTableType drop_table_type = USER_INDEX;
  uint64_t compat_version = 0;
  ObDropIndexArg arg;
  const bool is_mlog = (obrpc::ObIndexArg::DROP_MLOG == const_arg.index_action_type_);
  if (is_mlog) {
    need_rename_index = false;
    drop_table_type = MATERIALIZED_VIEW_LOG;
  }
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("failed to get data version", KR(ret), K(tenant_id));
  } else if (is_mlog && (compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("materialized view log before version 4.3 is not supported", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "materialized view log before version 4.3 is");
  } else if (OB_FAIL(arg.assign(const_arg))) {
    LOG_WARN("fail to assign const_arg", K(ret));
  } else if (OB_FALSE_IT(schema_guard.set_session_id(arg.session_id_))) {
  } else if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                         tenant_id, arg.database_name_, arg.table_name_,
                         is_index, table_schema, arg.is_hidden_))) {
    LOG_WARN("failed to get data table schema", K(arg), K(ret));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    if (!(ignore_for_domain_index = ignore_error_code_for_domain_index(ret, arg))) {
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(arg.database_name_), to_cstring(arg.table_name_));
    }
    LOG_WARN("table not found", K(arg), K(ret));
  } else if (arg.is_in_recyclebin_) {
    // internal delete index
  } else if (table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not drop index of table in recyclebin.", K(ret), K(arg));
  } else if (OB_FAIL(schema_guard.check_database_in_recyclebin(tenant_id,
            table_schema->get_database_id(), is_db_in_recyclebin))) {
    LOG_WARN("check database in recyclebin failed", K(ret), K(tenant_id));
  } else if (is_db_in_recyclebin) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("Can not drop index of db in recyclebin", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.check_fk_related_table_ddl(*table_schema, ObDDLType::DDL_DROP_INDEX))) {
    LOG_WARN("check whether foreign key related table executes ddl failed", K(ret));
  }
  // get vector rebuild drop index table id
  if (OB_SUCC(ret) && arg.is_add_to_scheduler_ && arg.is_vec_inner_drop_) {
    if (OB_FAIL(ObVectorIndexUtil::get_rebuild_drop_index_id_and_name(schema_guard, arg))) {
      LOG_WARN("fail to get vector drop index id", K(ret), K(arg));
    }
  }
  if (OB_SUCC(ret)) {
    ObString index_table_name;
    const uint64_t data_table_id = table_schema->get_table_id();
    const ObTableSchema *index_table_schema = NULL;
    if (OB_INVALID_ID != arg.index_table_id_) {
      LOG_DEBUG("drop index with index_table_id", K(arg.index_table_id_));
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, arg.index_table_id_, index_table_schema))) {
        LOG_WARN("fail to get index table schema", K(ret), K(tenant_id), K(arg.index_table_id_));
      } else if (OB_ISNULL(index_table_schema)) {
        ignore_for_domain_index = ignore_error_code_for_domain_index(OB_TABLE_NOT_EXIST, arg);
      }
    } else {
      if (is_mlog) {
        index_table_name = arg.index_name_;
      } else if (OB_FAIL(ObTableSchema::build_index_table_name(
          allocator, data_table_id, arg.index_name_, index_table_name))) {
        LOG_WARN("build_index_table_name failed", K(arg), K(data_table_id), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
            table_schema->get_database_id(),
            index_table_name,
            !is_mlog,/*is_index*/
            index_table_schema))) {
          LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(index_table_name), K(index_table_schema));
        }
      }
    }
    bool have_index = false;
    const common::ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema->get_foreign_key_infos();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(index_table_schema)) {
      if (is_mlog) {
        ret = OB_ERR_TABLE_NO_MLOG;
        LOG_WARN("table does not have a materialized view log", KR(ret),
            K(arg.database_name_), K(arg.index_name_));
        LOG_USER_ERROR(OB_ERR_TABLE_NO_MLOG, to_cstring(arg.database_name_), to_cstring(arg.index_name_));
      } else {
        ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
        LOG_WARN("index table schema should not be null", K(arg.index_name_), K(index_table_name), K(ret));
        if (!ignore_for_domain_index) {
          LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, arg.index_name_.length(), arg.index_name_.ptr());
        }
      }
    } else if (ObIndexType::INDEX_TYPE_HEAP_ORGANIZED_TABLE_PRIMARY == index_table_schema->get_index_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to drop index with a name reserved by the heap table", K(ret), KPC(index_table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "dropping index with a name reserved by the heap table is");
    } else if (OB_FAIL(ddl_service_.check_index_on_foreign_key(index_table_schema,
                                                               foreign_key_infos,
                                                               have_index))) {
      LOG_WARN("fail to check index on foreign key", K(ret), K(foreign_key_infos), KPC(index_table_schema));
    } else if (have_index) {
      if (index_table_schema->is_unique_index()) {
        ret = OB_ERR_ATLER_TABLE_ILLEGAL_FK;
      } else {
        ObString index_name;
        if (OB_FAIL(ObTableSchema::get_index_name(allocator,
          index_table_schema->get_data_table_id(),
          index_table_schema->get_table_name_str(),
          index_name))) {
          LOG_WARN("failed to build index table name", K(ret));
        } else {
          ret = OB_ERR_ATLER_TABLE_ILLEGAL_FK_DROP_INDEX;
          LOG_USER_ERROR(OB_ERR_ATLER_TABLE_ILLEGAL_FK_DROP_INDEX, index_name.length(), index_name.ptr());
        }
      }
      LOG_WARN("cannot delete index with foreign key dependency", K(ret));
    } else if (!arg.is_inner_ && index_table_schema->is_unavailable_index()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to drop a building index", K(ret), K(arg.is_inner_), KPC(index_table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "dropping a building index is");
    } else if (index_table_schema->is_vec_index() && compat_version < DATA_VERSION_4_3_3_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("drop vector index before version 4.3.3 is not supported", KR(ret), K(compat_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop vector index before version 4.3.3 is");
    } else if (arg.is_add_to_scheduler_) {
      ObDDLOperator ddl_operator(ddl_service_.get_schema_service(), ddl_service_.get_sql_proxy());
      ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
      int64_t refreshed_schema_version = 0;
      ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
      ObDDLTaskRecord task_record;
      bool has_other_domain_index = false;
      const bool is_vec_or_fts_or_multivalue_index = index_table_schema->is_fts_or_multivalue_index() || index_table_schema->is_vec_index();
      const bool is_inner_and_fts_index = arg.is_inner_ && !arg.is_parent_task_dropping_fts_index_ && index_table_schema->is_fts_index();
      const bool need_check_fts_index_conflict = !arg.is_inner_ && index_table_schema->is_fts_or_multivalue_index();
      const bool is_inner_and_multivalue_index = arg.is_inner_ && index_table_schema->is_multivalue_index();
      const bool is_inner_and_vec_index = arg.is_inner_ && !arg.is_vec_inner_drop_ && index_table_schema->is_vec_index();
      const bool need_check_vec_index_conflict = !arg.is_inner_ && index_table_schema->is_vec_index();
      const bool is_inner_and_fts_or_mulvalue_or_vector_index = is_inner_and_fts_index || is_inner_and_multivalue_index || is_inner_and_vec_index;
      bool has_index_task = false;
      typedef common::ObSEArray<share::schema::ObTableSchema, 4> TableSchemaArray;
      SMART_VAR(TableSchemaArray, new_index_schemas) {
        if (need_check_fts_index_conflict && OB_FAIL(ddl_service_.check_fts_index_conflict(table_schema->get_tenant_id(), table_schema->get_table_id()))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("failed to check fts index ", K(ret), K(arg));
          }
        } else if (need_check_vec_index_conflict && OB_FAIL(ddl_service_.check_vec_index_conflict(table_schema->get_tenant_id(), table_schema->get_table_id()))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("failed to check vec index ", K(ret), K(arg));
          }
        } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
          LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
        } else if ((index_table_schema->is_doc_id_rowkey() ||
                    index_table_schema->is_rowkey_doc_id())
            && OB_FAIL(check_has_none_shared_index_tables_for_fts_or_multivalue_index_(tenant_id, index_table_schema->get_data_table_id(), schema_guard,
                has_other_domain_index))) {
          LOG_WARN("fail to check has fts or multivalue index", K(ret), K(tenant_id), K(index_table_schema->get_index_type()), K(arg), KPC(index_table_schema));
        } else if ((index_table_schema->is_vec_rowkey_vid_type() ||
                   index_table_schema->is_vec_vid_rowkey_type())
            && OB_FAIL(check_has_none_shared_index_tables_for_vector_index_(tenant_id, index_table_schema->get_data_table_id(), schema_guard,
                has_other_domain_index))) {
          LOG_WARN("fail to check has vector index", K(ret), K(tenant_id), K(index_table_schema->get_index_type()), K(arg), KPC(index_table_schema));
        } else if (has_other_domain_index) {
          LOG_INFO("there are some other none share index table, and don't need to drop share index table",
              K(index_table_schema->get_index_type()), KPC(index_table_schema));
        } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
          LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
        } else if (!arg.is_inner_ &&
                   OB_FAIL(ObDDLTaskRecordOperator::check_has_index_or_mlog_task(trans, *index_table_schema, tenant_id, data_table_id, has_index_task))) {
          LOG_WARN("failed to check ddl conflict", K(ret));
        } else if (has_index_task) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support to drop a building or dropping index", K(ret), K(arg.is_inner_), KPC(index_table_schema));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "dropping a building or dropping index is");
        } else if (index_table_schema->is_doc_id_rowkey() || index_table_schema->is_rowkey_doc_id()) {
          if (OB_FAIL(ObDDLTaskRecordOperator::check_has_index_or_mlog_task(trans, *index_table_schema, tenant_id, data_table_id,
              has_other_domain_index))) {
            LOG_WARN("fail to check has rowkey doc or doc rowkey task", K(ret), K(tenant_id), K(arg), KPC(index_table_schema));
          } else if (has_other_domain_index) {
            ret = OB_EAGAIN;
            LOG_WARN("has doing other ddl task", K(ret), K(tenant_id), K(data_table_id), K(index_table_schema->get_table_id()));
          }
        }

        if (OB_FAIL(ret) || has_other_domain_index) {
        } else if (need_rename_index && OB_FAIL(ddl_service_.rename_dropping_index_name(
                                                      *index_table_schema,
                                                      is_inner_and_fts_or_mulvalue_or_vector_index,
                                                      arg,
                                                      schema_guard,
                                                      ddl_operator,
                                                      trans,
                                                      new_index_schemas))) {
          LOG_WARN("rename index name failed", K(ret));
        } else if (!need_rename_index && OB_FAIL(new_index_schemas.push_back(*index_table_schema))) {
          LOG_WARN("failed to assign index table schema to new index schema", KR(ret));
        } else if (is_inner_and_fts_or_mulvalue_or_vector_index && 0 == new_index_schemas.count()) {
          if (OB_FAIL(new_index_schemas.push_back(*index_table_schema))) {
            LOG_WARN("fail to push back index schema", K(ret), KPC(index_table_schema));
          }
        } else if (OB_UNLIKELY(!is_vec_or_fts_or_multivalue_index && new_index_schemas.count() != 1)
                || OB_UNLIKELY(is_inner_and_fts_or_mulvalue_or_vector_index && new_index_schemas.count() != 1)
                || OB_UNLIKELY(!arg.is_inner_ && index_table_schema->is_vec_delta_buffer_type() && new_index_schemas.count() != 5) // five index assistant table of vec index
                || OB_UNLIKELY(!arg.is_inner_ && index_table_schema->is_vec_ivfflat_centroid_index() && new_index_schemas.count() != 3)
                || OB_UNLIKELY(!arg.is_inner_ && index_table_schema->is_vec_ivfsq8_centroid_index() && new_index_schemas.count() != 4)
                || OB_UNLIKELY(!arg.is_inner_ && index_table_schema->is_vec_ivfpq_centroid_index() && new_index_schemas.count() != 4)
                || OB_UNLIKELY(!arg.is_inner_ && index_table_schema->is_fts_index_aux() && new_index_schemas.count() != 4)
                || OB_UNLIKELY(!arg.is_inner_ && index_table_schema->is_multivalue_index_aux() && new_index_schemas.count() != 3)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, invalid new index schema count", K(ret),
              "is vec or fts or multivalue index", is_vec_or_fts_or_multivalue_index,
              "is inner", arg.is_inner_,
              "count", new_index_schemas.count(),
              "is vec index", index_table_schema->is_vec_delta_buffer_type(),
              "is vec ivfflat index", index_table_schema->is_vec_ivfflat_centroid_index(),
              "is vec ivfsq8 index", index_table_schema->is_vec_ivfsq8_centroid_index(),
              "is vec ivfpq index", index_table_schema->is_vec_ivfpq_centroid_index(),
              "is fts index", index_table_schema->is_fts_index_aux(),
              "is multivalue index", index_table_schema->is_multivalue_index_aux(),
              K(new_index_schemas));
        }
        if (OB_SUCC(ret) && !has_other_domain_index) {
          bool has_exist = false;
          const ObTableSchema *data_table_schema = nullptr;
          const ObTableSchema &new_index_schema = new_index_schemas.at(new_index_schemas.count() - 1);
          if (is_mlog && table_schema->is_materialized_view() && table_schema->get_table_id() != new_index_schema.get_data_table_id()) {
            // drop mlog on mview
            if (OB_FAIL(schema_guard.get_table_schema(tenant_id, new_index_schema.get_data_table_id(), data_table_schema))) {
              LOG_WARN("fail to get index table schema", K(ret), K(tenant_id), K(arg.index_table_id_));
            } else if (OB_ISNULL(data_table_schema)) {
              ret = OB_TABLE_NOT_EXIST;
              LOG_WARN("table not found", K(ret), K(arg));
            }
          } else {
            data_table_schema = table_schema;
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(submit_drop_index_task(trans, *data_table_schema, new_index_schemas, arg, nullptr/*inc_data_tablet_ids*/,
                                             nullptr/*del_data_tablet_ids*/, allocator, has_exist, task_record))) {
            LOG_WARN("submit drop index task failed", K(ret), K(task_record));
          } else if (has_exist) {
            res.task_id_ = task_record.task_id_;
          } else {
            res.tenant_id_ = new_index_schema.get_tenant_id();
            res.index_table_id_ = new_index_schema.get_table_id();
            res.schema_version_ = new_index_schema.get_schema_version();
            res.task_id_ = task_record.task_id_;
          }
          if (OB_FAIL(ret)) {
          } else if (index_table_schema->is_vec_index() &&
                     arg.is_vec_inner_drop_ &&
                     OB_FAIL(ObDDLTaskRecordOperator::update_parent_task_message(tenant_id,
                        arg.task_id_, *index_table_schema, res.task_id_, res.task_id_,
                        ObDDLUpdateParentTaskIDType::UPDATE_VEC_REBUILD_DROP_INDEX_TASK_ID, allocator, trans))) {
            LOG_WARN("fail to update parent task message", K(ret), K(arg.task_id_), K(res.task_id_));
          }
        }
      }
      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN_RET(temp_ret, "trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
        }
      }
      if (OB_SUCC(ret) && !has_other_domain_index) {
        int tmp_ret = OB_SUCCESS;
        if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
          LOG_WARN("fail to publish schema", K(ret), K(tenant_id));
        } else if (OB_TMP_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
          LOG_WARN("fail to schedule ddl task", K(tmp_ret), K(task_record));
        }
      }
    } else {
      //construct an arg for drop table
      ObTableItem table_item;
      table_item.database_name_ = arg.database_name_;
      table_item.table_name_ = index_table_schema->get_table_name();
      table_item.is_hidden_ = index_table_schema->is_user_hidden_table();
      table_item.table_id_ = arg.index_table_id_;
      obrpc::ObDDLRes ddl_res;
      obrpc::ObDropTableArg drop_table_arg;
      drop_table_arg.tenant_id_ = tenant_id;
      drop_table_arg.if_exist_ = false;
      drop_table_arg.table_type_ = drop_table_type;
      drop_table_arg.ddl_stmt_str_ = arg.ddl_stmt_str_;
      drop_table_arg.force_drop_ = arg.is_in_recyclebin_;
      drop_table_arg.task_id_ = arg.task_id_;
      if (OB_FAIL(drop_table_arg.tables_.push_back(table_item))) {
        LOG_WARN("failed to add table item!", K(table_item), K(ret));
      } else if (OB_FAIL(ddl_service_.drop_table(drop_table_arg, ddl_res))) {
        if (OB_TABLE_NOT_EXIST == ret) {
          ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
          LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, arg.index_name_.length(), arg.index_name_.ptr());
          LOG_WARN("index not exist, can't drop it", K(arg), K(ret));
        } else {
          LOG_WARN("drop_table failed", K(arg), K(drop_table_arg), K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret) && ignore_for_domain_index) {
    // ignore error code and return success for fts index, while data table or index table isn't exist.
    res.task_id_ = -1; // just skip following steps.
    ret = OB_SUCCESS;
  }
  LOG_INFO("finish drop index", K(const_arg), K(arg), K(ret));
  return ret;
}

int ObIndexBuilder::do_create_global_index(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const obrpc::ObCreateIndexArg &arg,
    const share::schema::ObTableSchema &table_schema,
    obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  ObArray<ObColumnSchemaV2 *> gen_columns;
  const bool global_index_without_column_info = false;
  ObDDLTaskRecord task_record;
  ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
  HEAP_VARS_2((ObTableSchema, new_table_schema),
              (obrpc::ObCreateIndexArg, new_arg)) {
    ObTableSchema &index_schema = new_arg.index_schema_;
    ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
    int64_t refreshed_schema_version = 0;
    const uint64_t tenant_id = table_schema.get_tenant_id();
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else if (!new_table_schema.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to copy table schema", K(ret));
    } else if (OB_FAIL(new_arg.assign(arg))) {
      LOG_WARN("fail to assign arg", K(ret));
    } else if (!new_arg.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to copy create index arg", K(ret));
    } else if (OB_FAIL(ObIndexBuilderUtil::adjust_expr_index_args(
            new_arg, new_table_schema, allocator, gen_columns))) {
      LOG_WARN("fail to adjust expr index args", K(ret));
    } else if (OB_FAIL(generate_schema(
        new_arg, new_table_schema, global_index_without_column_info,
        true/*generate_id*/, index_schema))) {
      LOG_WARN("fail to generate schema", K(ret), K(new_arg));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
      LOG_WARN("get min data version failed", K(ret), K(tenant_id));
    } else {
      if (gen_columns.empty()) {
        if (OB_FAIL(ddl_service_.create_global_index(
                trans, new_arg, new_table_schema, tenant_data_version, index_schema))) {
          LOG_WARN("fail to create global index", K(ret));
        }
      } else {
        if (OB_FAIL(ddl_service_.create_global_inner_expr_index(
              trans, table_schema, tenant_data_version, new_table_schema, gen_columns, index_schema))) {
          LOG_WARN("fail to create global inner expr index", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(submit_build_index_task(trans,
                                                 new_arg,
                                                 &new_table_schema,
                                                 nullptr/*inc_data_tablet_ids*/,
                                                 nullptr/*del_data_tablet_ids*/,
                                                 &index_schema,
                                                 arg.parallelism_,
                                                 arg.consumer_group_id_,
                                                 tenant_data_version,
                                                 allocator,
                                                 task_record))) {
        LOG_WARN("fail to submit build global index task", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      res.index_table_id_ = index_schema.get_table_id();
      res.schema_version_ = index_schema.get_schema_version();
      res.task_id_ = task_record.task_id_;
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
        LOG_WARN("fail to publish schema", K(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
        LOG_WARN("fail to schedule ddl task", K(tmp_ret), K(task_record));
      }
    }
  }
  return ret;
}

int ObIndexBuilder::submit_build_index_task(
    ObMySQLTransaction &trans,
    const obrpc::ObCreateIndexArg &create_index_arg,
    const ObTableSchema *data_schema,
    const ObIArray<ObTabletID> *inc_data_tablet_ids,
    const ObIArray<ObTabletID> *del_data_tablet_ids,
    const ObTableSchema *index_schema,
    const int64_t parallelism,
    const int64_t group_id,
    const uint64_t tenant_data_version,
    common::ObIAllocator &allocator,
    ObDDLTaskRecord &task_record,
    const int64_t new_fetched_snapshot)
{
  int ret = OB_SUCCESS;
  ObTableLockOwnerID owner_id;
  ObCreateDDLTaskParam param(index_schema->get_tenant_id(),
                             get_create_index_type(tenant_data_version, *index_schema),
                             data_schema,
                             index_schema,
                             0/*object_id*/,
                             index_schema->get_schema_version(),
                             parallelism,
                             group_id,
                             &allocator,
                             &create_index_arg);
  param.tenant_data_version_ = tenant_data_version;
  param.fts_snapshot_version_ = new_fetched_snapshot;
  if (OB_UNLIKELY(nullptr == data_schema || nullptr == index_schema || tenant_data_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is invalid", K(ret), KP(data_schema), KP(index_schema), K(tenant_data_version));
  } else if (index_schema->is_rowkey_doc_id() && new_fetched_snapshot <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the fts snapshot version should be more than zero", K(ret), K(new_fetched_snapshot));
  } else {
    bool is_create_fts_index = share::schema::is_fts_index(create_index_arg.index_type_);
    if (is_create_fts_index) {
      param.type_ = ObDDLType::DDL_CREATE_FTS_INDEX;
    } else if (is_multivalue_index(create_index_arg.index_type_)) {
      param.type_ = ObDDLType::DDL_CREATE_MULTIVALUE_INDEX;
    } else if (share::schema::is_vec_hnsw_index(create_index_arg.index_type_)) {
      param.type_ = ObDDLType::DDL_CREATE_VEC_INDEX;
    } else if (share::schema::is_vec_ivfflat_index(create_index_arg.index_type_)) {
      param.type_ = ObDDLType::DDL_CREATE_VEC_IVFFLAT_INDEX;
    } else if (share::schema::is_vec_ivfsq8_index(create_index_arg.index_type_)) {
      param.type_ = ObDDLType::DDL_CREATE_VEC_IVFSQ8_INDEX;
    } else if (share::schema::is_vec_ivfpq_index(create_index_arg.index_type_)) {
      param.type_ = ObDDLType::DDL_CREATE_VEC_IVFPQ_INDEX;
    }
    if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param, trans, task_record))) {
      LOG_WARN("submit create index ddl task failed", K(ret));
    } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                   task_record.task_id_))) {
      LOG_WARN("failed to get owner id", K(ret), K(task_record.task_id_));
    } else if (OB_FAIL(ObDDLLock::lock_for_add_drop_index(
        *data_schema, inc_data_tablet_ids, del_data_tablet_ids, *index_schema, owner_id, trans))) {
      LOG_WARN("failed to lock online ddl lock", K(ret));
    }
  }
  return ret;
}


// if index_schemas has delta_buffer_table, than index_ith = domain_index_ith, ohterwide, index_ith = 0;
// ivfflat: centroid_ith, cid_vector_ith, rowkey_cid_ith
// ivfsq8 : centroid_ith, cid_vector_ith, rowkey_cid_ith, sq_meta_ith
// ivfpq  : centroid_ith, rowkey_cid_ith, pq_centroid_ith, pq_code_ith
int ObIndexBuilder::recognize_vec_ivf_index_schemas(
    const common::ObIArray<share::schema::ObTableSchema> &index_schemas,
    const bool is_vec_inner_drop,
    int64_t &index_ith,
    int64_t &centroid_ith,
    int64_t &cid_vector_ith,
    int64_t &rowkey_cid_ith,
    int64_t &sq_meta_ith,
    int64_t &pq_centroid_ith,
    int64_t &pq_code_ith)
{
  int ret = OB_SUCCESS;
  index_ith = 0;
  centroid_ith = -1;
  cid_vector_ith = -1;
  rowkey_cid_ith = -1;
  sq_meta_ith = -1;
  pq_centroid_ith = -1;
  pq_code_ith = -1;

  const int64_t VEC_DOMAIN_INDEX_TABLE_COUNT = 1; // delta_buffer_table
  const int64_t VEC_IVFFLAT_INDEX_TABLE_COUNT = 3;
  const int64_t VEC_IVFSQ8_INDEX_TABLE_COUNT = 4;
  const int64_t VEC_IVFPQ_INDEX_TABLE_COUNT = 4;

  if (OB_UNLIKELY(!is_vec_inner_drop && VEC_DOMAIN_INDEX_TABLE_COUNT != index_schemas.count() &&
                  VEC_IVFFLAT_INDEX_TABLE_COUNT != index_schemas.count() &&
                  VEC_IVFSQ8_INDEX_TABLE_COUNT != index_schemas.count() &&
                  VEC_IVFPQ_INDEX_TABLE_COUNT != index_schemas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_schemas));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_schemas.count(); ++i) {
      if (index_schemas.at(i).is_vec_ivfflat_centroid_index() ||
          index_schemas.at(i).is_vec_ivfsq8_centroid_index() ||
          index_schemas.at(i).is_vec_ivfpq_centroid_index()) {
        if (OB_UNLIKELY(-1 != centroid_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple centroid tables", K(ret), K(index_schemas));
        } else {
          centroid_ith = i;
          index_ith = centroid_ith; // if has domain index, index_ith = domain_index_ith
        }
      } else if (index_schemas.at(i).is_vec_ivfflat_cid_vector_index() ||
                 index_schemas.at(i).is_vec_ivfsq8_cid_vector_index()) {
        if (OB_UNLIKELY(-1 != cid_vector_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple cid_vector tables", K(ret), K(index_schemas));
        } else {
          cid_vector_ith = i;
        }
      } else if (index_schemas.at(i).is_vec_ivfflat_rowkey_cid_index() ||
                 index_schemas.at(i).is_vec_ivfsq8_rowkey_cid_index() ||
                 index_schemas.at(i).is_vec_ivfpq_rowkey_cid_index()) {
        if (OB_UNLIKELY(-1 != rowkey_cid_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple rowkey_cid tables", K(ret), K(index_schemas));
        } else {
          rowkey_cid_ith = i;
        }
      } else if (index_schemas.at(i).is_vec_ivfsq8_meta_index()) {
        if (OB_UNLIKELY(-1 != sq_meta_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple sq_meta tables", K(ret), K(index_schemas));
        } else {
          sq_meta_ith = i;
        }
      } else if (index_schemas.at(i).is_vec_ivfpq_pq_centroid_index()) {
        if (OB_UNLIKELY(-1 != pq_centroid_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple pq_centroid tables", K(ret), K(index_schemas));
        } else {
          pq_centroid_ith = i;
        }
      } else if (index_schemas.at(i).is_vec_ivfpq_code_index()) {
        if (OB_UNLIKELY(-1 != pq_code_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple pq_code tables", K(ret), K(index_schemas));
        } else {
          pq_code_ith = i;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected drop ivf vec index schema", K(ret), K(index_schemas.at(i)));
      }
    }
  }
  return ret;
}

// if index_schemas has delta_buffer_table, than index_ith = domain_index_ith, ohterwide, index_ith = 0;
int ObIndexBuilder::recognize_vec_hnsw_index_schemas(
      const common::ObIArray<share::schema::ObTableSchema> &index_schemas,
      const bool is_vec_inner_drop,
      int64_t &index_ith,
      int64_t &rowkey_vid_ith,
      int64_t &vid_rowkey_ith,
      int64_t &domain_index_ith,
      int64_t &index_id_ith,
      int64_t &snapshot_data_ith)
{
  int ret = OB_SUCCESS;
  index_ith = 0;
  rowkey_vid_ith = -1;
  vid_rowkey_ith = -1;
  domain_index_ith = -1;
  index_id_ith = -1;
  snapshot_data_ith = -1;
  const int64_t VEC_DOMAIN_INDEX_TABLE_COUNT = 1; // delta_buffer_table
  const int64_t VEC_INDEX_TABLE_COUNT = 5;
  if (OB_UNLIKELY(!is_vec_inner_drop &&
                  VEC_DOMAIN_INDEX_TABLE_COUNT != index_schemas.count() &&
                  VEC_INDEX_TABLE_COUNT != index_schemas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_schemas));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_schemas.count(); ++i) {
      if (index_schemas.at(i).is_vec_rowkey_vid_type()) {
        if (OB_UNLIKELY(-1 != rowkey_vid_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple vid rowkey tables", K(ret), K(index_schemas));
        } else {
          rowkey_vid_ith = i;
        }
      } else if (index_schemas.at(i).is_vec_vid_rowkey_type()) {
        if (OB_UNLIKELY(-1 != vid_rowkey_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple rowkey vid tables", K(ret), K(index_schemas));
        } else {
          vid_rowkey_ith = i;
        }
      } else if (index_schemas.at(i).is_vec_delta_buffer_type()) {
        if (OB_UNLIKELY(-1 != domain_index_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple vid rowkey tables", K(ret), K(index_schemas));
        } else {
          domain_index_ith = i;
          index_ith = domain_index_ith; // if has domain index, index_ith = domain_index_ith
        }
      } else if (index_schemas.at(i).is_vec_index_id_type()) {
        if (OB_UNLIKELY(-1 != index_id_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple index id tables", K(ret), K(index_schemas));
        } else {
          index_id_ith = i;
        }
      } else if (index_schemas.at(i).is_vec_index_snapshot_data_type()) {
        if (OB_UNLIKELY(-1 != snapshot_data_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple snapshot data tables", K(ret), K(index_schemas));
        } else {
          snapshot_data_ith = i;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected drop vec index schema", K(ret), K(index_schemas.at(i)));
      }
    }
  }
  return ret;
}

int ObIndexBuilder::submit_rebuild_index_task(
    ObMySQLTransaction &trans,
    const obrpc::ObRebuildIndexArg &rebuild_index_arg,
    const ObTableSchema *data_schema,
    const ObIArray<ObTabletID> *inc_data_tablet_ids,
    const ObIArray<ObTabletID> *del_data_tablet_ids,
    const ObTableSchema *index_schema,
    const int64_t parallelism,
    const int64_t group_id,
    const uint64_t tenant_data_version,
    common::ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_schema) || OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(index_schema), K(GCTX.root_service_));
  } else {
    ObTableLockOwnerID owner_id;
    const int64_t old_index_table_id = OB_INVALID_ID;
    const int64_t new_index_table_id = OB_INVALID_ID;
    const bool is_global_vector_index = false;
    ObCreateDDLTaskParam param(index_schema->get_tenant_id(),
                              ObDDLType::DDL_REBUILD_INDEX,
                              index_schema,
                              nullptr,
                              0/*object_id*/,
                              index_schema->get_schema_version(),
                              parallelism,
                              group_id,
                              &allocator,
                              &rebuild_index_arg);
    param.tenant_data_version_ = tenant_data_version;
    if (OB_UNLIKELY(nullptr == data_schema || nullptr == index_schema || tenant_data_version <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("schema is invalid", K(ret), KP(data_schema), KP(index_schema), K(tenant_data_version));
    } else if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param, trans, task_record))) {
      LOG_WARN("submit create index ddl task failed", K(ret));
    } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                  task_record.task_id_))) {
      LOG_WARN("failed to get owner id", K(ret), K(task_record.task_id_));
    } else if (OB_FAIL(ObDDLLock::lock_for_rebuild_index(*data_schema,
                                                        old_index_table_id,
                                                        new_index_table_id,
                                                        is_global_vector_index,
                                                        owner_id,
                                                        trans))) {
      LOG_WARN("failed to lock rebuild index ddl", K(ret));
    }
  }
  return ret;
}

int ObIndexBuilder::recognize_fts_or_multivalue_index_schemas(
      const common::ObIArray<share::schema::ObTableSchema> &index_schemas,
      const bool is_parent_task_dropping_fts,
      const bool is_parent_task_dropping_multivalue,
      int64_t &index_ith,
      int64_t &aux_doc_word_ith,
      int64_t &aux_rowkey_doc_ith,
      int64_t &domain_index_ith,
      int64_t &aux_doc_rowkey_ith,
      int64_t &aux_multivalue_ith)
{
  int ret = OB_SUCCESS;
  index_ith = 0;
  aux_doc_word_ith = -1;
  aux_rowkey_doc_ith = -1;
  domain_index_ith = -1;
  aux_doc_rowkey_ith = -1;
  aux_multivalue_ith = -1;

  if (OB_UNLIKELY(!(is_parent_task_dropping_fts || is_parent_task_dropping_multivalue)
    && 1 != index_schemas.count() && 4 != index_schemas.count() && 3 != index_schemas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_schemas));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_schemas.count(); ++i) {
      if (index_schemas.at(i).is_rowkey_doc_id()) {
        if (OB_UNLIKELY(-1 != aux_rowkey_doc_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple aux rowkey doc tables", K(ret), K(index_schemas));
        } else {
          aux_rowkey_doc_ith = i;
        }
      } else if (index_schemas.at(i).is_doc_id_rowkey()) {
        if (OB_UNLIKELY(-1 != aux_doc_rowkey_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple aux doc rowkey tables", K(ret), K(index_schemas));
        } else {
          aux_doc_rowkey_ith = i;
        }
      } else if (index_schemas.at(i).is_fts_index_aux()) {
        if (OB_UNLIKELY(-1 != domain_index_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple aux fts index tables", K(ret), K(index_schemas));
        } else {
          domain_index_ith = i;
          index_ith = domain_index_ith;
        }
      } else if (index_schemas.at(i).is_fts_doc_word_aux()) {
        if (OB_UNLIKELY(-1 != aux_doc_word_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple aux doc word tables", K(ret), K(index_schemas));
        } else {
          aux_doc_word_ith = i;
        }
      } else if (index_schemas.at(i).is_multivalue_index_aux()) {
        if (OB_UNLIKELY(-1 != aux_multivalue_ith)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, there are multiple aux doc word tables", K(ret), K(index_schemas));
        } else {
          aux_multivalue_ith = i;
          index_ith = aux_multivalue_ith;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpeted error, there are multiple user index tables", K(ret), K(index_schemas));
      }
    }
  }
  return ret;
}

int ObIndexBuilder::submit_drop_index_task(ObMySQLTransaction &trans,
                                           const ObTableSchema &data_schema,
                                           const common::ObIArray<share::schema::ObTableSchema> &index_schemas,
                                           const obrpc::ObDropIndexArg &arg,
                                           const common::ObIArray<common::ObTabletID> *inc_data_tablet_ids,
                                           const common::ObIArray<common::ObTabletID> *del_data_tablet_ids,
                                           common::ObIAllocator &allocator,
                                           bool &task_has_exist,
                                           ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t index_ith = 0;
  int64_t aux_doc_word_ith = -1;
  int64_t aux_rowkey_doc_ith = -1;
  int64_t fts_domain_index_ith = -1;
  int64_t aux_doc_rowkey_ith = -1;
  int64_t aux_multivalue_ith = -1;
  int64_t vec_rowkey_vid_ith = -1;
  int64_t vec_vid_rowkey_ith = -1;
  int64_t vec_domain_index_ith = -1;
  int64_t vec_index_id_ith = -1;
  int64_t vec_snapshot_data_ith = -1;
  int64_t vec_centroid_ith = -1;
  int64_t vec_cid_vector_ith = -1;
  int64_t vec_rowkey_cid_ith = -1;
  int64_t vec_sq_meta_ith = -1;
  int64_t vec_pq_centroid_ith = -1;
  int64_t vec_pq_code_ith = -1;

  const int64_t NORMAL_INDEX_COUNT = 1;
  const int64_t FTS_INDEX_COUNT = 4;
  const int64_t FTS_OR_MULTIVALUE_INDEX_COUNT = 3;
  const int64_t VEC_HNSW_INDEX_COUNT = 5;
  const int64_t VEC_IVFFLAT_INDEX_COUNT = 3;
  const int64_t VEC_IVFSQ8_INDEX_COUNT = 4;
  const int64_t VEC_IVFPQ_INDEX_COUNT = 4;

  if (OB_UNLIKELY(index_schemas.count() != NORMAL_INDEX_COUNT &&
                  !arg.is_parent_task_dropping_fts_index_ && index_schemas.count() != FTS_INDEX_COUNT &&
                  !arg.is_parent_task_dropping_multivalue_index_ && index_schemas.count() != FTS_OR_MULTIVALUE_INDEX_COUNT &&
                  !arg.is_vec_inner_drop_ && (index_schemas.count() != VEC_HNSW_INDEX_COUNT &&
                                              index_schemas.count() != VEC_IVFFLAT_INDEX_COUNT &&
                                              index_schemas.count() != VEC_IVFSQ8_INDEX_COUNT &&
                                              index_schemas.count() != VEC_IVFPQ_INDEX_COUNT))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index schema count", K(ret), K(index_schemas));
  } else if (index_schemas.at(0).is_fts_index()
    && OB_FAIL(recognize_fts_or_multivalue_index_schemas(index_schemas, arg.is_parent_task_dropping_fts_index_, arg.is_parent_task_dropping_multivalue_index_,
      index_ith, aux_doc_word_ith, aux_rowkey_doc_ith, fts_domain_index_ith, aux_doc_rowkey_ith, aux_multivalue_ith))) {
    LOG_WARN("fail to recognize index and aux table from schema array", K(ret));
  } else if (index_schemas.at(0).is_vec_hnsw_index()
    && OB_FAIL(recognize_vec_hnsw_index_schemas(index_schemas, arg.is_vec_inner_drop_, index_ith,
      vec_rowkey_vid_ith, vec_vid_rowkey_ith, vec_domain_index_ith, vec_index_id_ith, vec_snapshot_data_ith))) {
    LOG_WARN("fail to recognize index and aux table from schema array", K(ret));
  } else if (index_schemas.at(0).is_vec_ivf_index()
    && OB_FAIL(recognize_vec_ivf_index_schemas(index_schemas, arg.is_vec_inner_drop_, index_ith,
      vec_centroid_ith, vec_cid_vector_ith, vec_rowkey_cid_ith, vec_sq_meta_ith, vec_pq_centroid_ith, vec_pq_code_ith))) {
    LOG_WARN("fail to recognize index and aux table from schema array", K(ret));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(GCTX.root_service_));
  } else if (OB_UNLIKELY(index_ith < 0 || index_ith >= index_schemas.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, invalid array index", K(ret), K(index_ith));
  } else {
    const ObTableSchema &index_schema = index_schemas.at(index_ith);
    const bool is_drop_vec_task = (!arg.is_inner_ && index_schema.is_vec_domain_index()) || arg.is_vec_inner_drop_;  // inner drop or user drop
    const bool is_drop_fts_task = (!arg.is_inner_ && index_schema.is_fts_index_aux()) || arg.is_parent_task_dropping_fts_index_;
    const bool is_drop_multivalue_task = (!arg.is_inner_ && index_schema.is_multivalue_index_aux()) || arg.is_parent_task_dropping_multivalue_index_;
    const bool is_drop_fts_or_multivalue_task = is_drop_fts_task || is_drop_multivalue_task;

    if (OB_UNLIKELY(!index_schema.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(index_schema));
    } else if (OB_UNLIKELY(is_drop_vec_task && !arg.is_vec_inner_drop_ // if is inner_drop, because drop count no necessary equal to five, so ith maybe equal to -1
        && OB_FAIL(ObVectorIndexUtil::check_drop_vec_indexs_ith_valid(index_schema.get_index_type(), index_schemas.count(),
          vec_rowkey_vid_ith, vec_vid_rowkey_ith, vec_domain_index_ith, vec_index_id_ith, vec_snapshot_data_ith,
          vec_centroid_ith, vec_cid_vector_ith, vec_rowkey_cid_ith, vec_sq_meta_ith, vec_pq_centroid_ith, vec_pq_code_ith)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, invalid aux table id for vec index", K(ret), K(is_drop_vec_task),
          K(vec_rowkey_vid_ith), K(vec_vid_rowkey_ith), K(vec_index_id_ith), K(vec_snapshot_data_ith), K(index_schemas.count()));
    } else if (OB_UNLIKELY(is_drop_fts_task && !arg.is_parent_task_dropping_fts_index_
                                            && (aux_rowkey_doc_ith < 0 || aux_rowkey_doc_ith >= index_schemas.count()
                                             || aux_doc_rowkey_ith < 0 || aux_doc_rowkey_ith >= index_schemas.count()
                                             || aux_doc_word_ith < 0 || aux_doc_word_ith >= index_schemas.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, invalid aux table id for fts index", K(ret), K(is_drop_fts_task),
          K(aux_rowkey_doc_ith), K(aux_doc_rowkey_ith), K(aux_doc_word_ith), K(index_schemas.count()));
    } else if (OB_UNLIKELY(is_drop_multivalue_task && !arg.is_parent_task_dropping_multivalue_index_ && (aux_rowkey_doc_ith < 0 || aux_rowkey_doc_ith >= index_schemas.count()
                                                      || aux_doc_rowkey_ith < 0 || aux_doc_rowkey_ith >= index_schemas.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, invalid aux table id for multivalue index", K(ret), K(is_drop_multivalue_task),
          K(aux_rowkey_doc_ith), K(aux_doc_rowkey_ith), K(index_schemas.count()));
    } else if (!is_drop_fts_or_multivalue_task && !is_drop_vec_task) {
      // this isn't drop fts and isn't vec index task.
      const int64_t parent_task_id = arg.task_id_;
      ObTableLockOwnerID owner_id;
      const ObDDLType ddl_type = (ObIndexArg::DROP_MLOG == arg.index_action_type_) ?
                                  ObDDLType::DDL_DROP_MLOG : ObDDLType::DDL_DROP_INDEX;
      ObCreateDDLTaskParam param(index_schema.get_tenant_id(),
                                 ddl_type,
                                 &index_schema,
                                 nullptr,
                                 0/*object_id*/,
                                 index_schema.get_schema_version(),
                                 0/*parallelism*/,
                                 arg.consumer_group_id_,
                                 &allocator,
                                 &arg,
                                 parent_task_id);
      if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param, trans, task_record))) {
        if (OB_HASH_EXIST == ret) {
          task_has_exist = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("submit create index ddl task failed", K(ret));
        }
      } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                     task_record.task_id_))) {
        LOG_WARN("failed to get owner id", K(ret), K(task_record.task_id_));
      } else if (OB_FAIL(ObDDLLock::lock_for_add_drop_index(data_schema, inc_data_tablet_ids,
              del_data_tablet_ids, index_schema, owner_id, trans))) {
        LOG_WARN("failed to lock online ddl lock", K(ret));
      }
    } else if (is_drop_fts_or_multivalue_task) { // create dropping fts index parent task.
      ObDDLType ddl_type = is_drop_fts_task ? ObDDLType::DDL_DROP_FTS_INDEX : ObDDLType::DDL_DROP_MULVALUE_INDEX;
      ObCreateDDLTaskParam param(index_schema.get_tenant_id(),
                                 ddl_type,
                                 &index_schema,
                                 nullptr/*dest_table_schema*/,
                                 0/*object_id*/,
                                 index_schema.get_schema_version(),
                                 0/*parallelism*/,
                                 arg.consumer_group_id_,
                                 &allocator,
                                 &arg);
      param.aux_rowkey_doc_schema_ = (-1 == aux_rowkey_doc_ith) ? nullptr : &(index_schemas.at(aux_rowkey_doc_ith));
      param.aux_doc_rowkey_schema_ = (-1 == aux_doc_rowkey_ith) ? nullptr : &(index_schemas.at(aux_doc_rowkey_ith));
      if (is_drop_multivalue_task) {
        param.fts_index_aux_schema_ = (-1 == aux_multivalue_ith) ? nullptr : &(index_schemas.at(aux_multivalue_ith));
      } else if (is_drop_fts_task) {
        param.fts_index_aux_schema_ = (-1 == fts_domain_index_ith) ? nullptr : &(index_schemas.at(fts_domain_index_ith));
        param.aux_doc_word_schema_ = (-1 == aux_doc_word_ith) ? nullptr : &(index_schemas.at(aux_doc_word_ith));
      }
      if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param, trans, task_record))) {
        LOG_WARN("fail to create drop fts index task", K(ret), K(param));
      }
    } else if (is_drop_vec_task) {
      ObDDLType ddl_type = DDL_INVALID;
      if (index_schema.is_vec_hnsw_index()) {
        ddl_type = ObDDLType::DDL_DROP_VEC_INDEX;
      } else if (index_schema.is_vec_ivfflat_index()) {
        ddl_type = ObDDLType::DDL_DROP_VEC_IVFFLAT_INDEX;
      } else if (index_schema.is_vec_ivfsq8_index()) {
        ddl_type = ObDDLType::DDL_DROP_VEC_IVFSQ8_INDEX;
      } else if (index_schema.is_vec_ivfpq_index()) {
        ddl_type = ObDDLType::DDL_DROP_VEC_IVFPQ_INDEX;
      }
      ObCreateDDLTaskParam param(index_schema.get_tenant_id(),
                                 ddl_type,
                                 &index_schema,
                                 nullptr/*dest_table_schema*/,
                                 0/*object_id*/,
                                 index_schema.get_schema_version(),
                                 0/*parallelism*/,
                                 arg.consumer_group_id_,
                                 &allocator,
                                 &arg);
      param.vec_vid_rowkey_schema_ = vec_vid_rowkey_ith == -1 ? nullptr : &(index_schemas.at(vec_vid_rowkey_ith));
      param.vec_rowkey_vid_schema_ = vec_rowkey_vid_ith == -1 ? nullptr : &(index_schemas.at(vec_rowkey_vid_ith));
      param.vec_domain_index_schema_ = vec_domain_index_ith == -1 ? nullptr : &(index_schemas.at(vec_domain_index_ith));
      param.vec_index_id_schema_ = vec_index_id_ith == -1 ? nullptr : &(index_schemas.at(vec_index_id_ith));
      param.vec_snapshot_data_schema_ = vec_snapshot_data_ith == -1 ? nullptr : &(index_schemas.at(vec_snapshot_data_ith));
      param.vec_centroid_schema_ = vec_centroid_ith == -1 ? nullptr : &(index_schemas.at(vec_centroid_ith));
      param.vec_cid_vector_schema_ = vec_cid_vector_ith == -1 ? nullptr : &(index_schemas.at(vec_cid_vector_ith));
      param.vec_rowkey_cid_schema_ = vec_rowkey_cid_ith == -1 ? nullptr : &(index_schemas.at(vec_rowkey_cid_ith));
      param.vec_sq_meta_schema_ = vec_sq_meta_ith == -1 ? nullptr : &(index_schemas.at(vec_sq_meta_ith));
      param.vec_pq_centroid_schema_ = vec_pq_centroid_ith == -1 ? nullptr : &(index_schemas.at(vec_pq_centroid_ith));
      param.vec_pq_code_schema_ = vec_pq_code_ith == -1 ? nullptr : &(index_schemas.at(vec_pq_code_ith));

      if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param, trans, task_record))) {
        if (OB_HASH_EXIST == ret) {
          task_has_exist = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("submit drop vec index ddl task failed", K(ret), K(param));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected drop index task", K(ret), K(is_drop_fts_or_multivalue_task), K(is_drop_vec_task));
    }
  }
  return ret;
}

int ObIndexBuilder::do_create_local_index(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const obrpc::ObCreateIndexArg &create_index_arg,
    const share::schema::ObTableSchema &table_schema,
    obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnSchemaV2 *, 1> gen_columns;
  ObDDLTaskRecord task_record;
  ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
  HEAP_VARS_4((ObTableSchema, index_schema),
              (ObTableSchema, new_table_schema),
              (obrpc::ObCreateIndexArg, my_arg),
              (obrpc::ObCreateIndexArg, tmp_arg)) {
    ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
    int64_t refreshed_schema_version = 0;
    const uint64_t tenant_id = table_schema.get_tenant_id();
    uint64_t tenant_data_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
      LOG_WARN("get tenant data version failed", K(ret));
    } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else if (!new_table_schema.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to copy table schema", K(ret));
    } else if (OB_FAIL(my_arg.assign(create_index_arg))) {
      LOG_WARN("fail to assign arg", K(ret));
    } else if (!my_arg.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to copy create index arg", K(ret));
    } else {
      const bool global_index_without_column_info = true;
      // build a global index with local storage if both the data table and index table are non-partitioned
      if (INDEX_TYPE_NORMAL_GLOBAL == my_arg.index_type_) {
        my_arg.index_type_ = INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE;
        my_arg.index_schema_.set_index_type(INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE);
      } else if (INDEX_TYPE_UNIQUE_GLOBAL == my_arg.index_type_) {
        my_arg.index_type_ = INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE;
        my_arg.index_schema_.set_index_type(INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE);
      } else if (INDEX_TYPE_SPATIAL_GLOBAL == my_arg.index_type_) {
        if (tenant_data_version < DATA_VERSION_4_1_0_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.1, spatial index is not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.1, spatial index");
        } else {
          my_arg.index_type_ = INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE;
          my_arg.index_schema_.set_index_type(INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE);
        }
      }
      bool rowkey_doc_exist = false;
      bool is_generate_rowkey_doc = false;
      int64_t new_fetched_snapshot = 0;
      if (OB_FAIL(ret)) {
      } else if (share::schema::is_fts_or_multivalue_index(my_arg.index_type_)) {
        const ObTableSchema *rowkey_doc_schema = nullptr;
        if (OB_FAIL(tmp_arg.assign(my_arg))) {
          LOG_WARN("fail to assign arg", K(ret));
        } else if (!tmp_arg.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to copy create index arg", K(ret));
        } else if (FALSE_IT(tmp_arg.index_type_ = INDEX_TYPE_ROWKEY_DOC_ID_LOCAL)) {
        } else if (OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(tmp_arg, &allocator))) {
          LOG_WARN("failed to adjust fts index name", K(ret));
        } else if (OB_FAIL(ddl_service_.check_aux_index_schema_exist(tenant_id,
                                                                      tmp_arg,
                                                                      schema_guard,
                                                                      &new_table_schema,
                                                                      rowkey_doc_exist,
                                                                      rowkey_doc_schema))) {
          LOG_WARN("fail to check rowkey doc schema existence", K(ret));
        }
      }
      bool rowkey_vid_exist = false;
      if (OB_FAIL(ret)) {
      } else if (share::schema::is_vec_index(my_arg.index_type_)) {
        if (OB_FAIL(ObVectorIndexUtil::check_table_exist(new_table_schema, my_arg.index_name_))) {  // index_name should be domain index name， like 'idx1'
          if (OB_ERR_TABLE_EXIST != ret) {
            LOG_WARN("Failed to check vec table exist", K(ret), K(my_arg.index_name_));
          }
        } else if (share::schema::is_vec_hnsw_index(my_arg.index_type_)) {
          const ObTableSchema *rowkey_vid_schema = nullptr;
          if (OB_FAIL(tmp_arg.assign(my_arg))) {
            LOG_WARN("fail to assign arg", K(ret));
          } else if (!tmp_arg.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to copy create index arg", K(ret));
          } else if (!create_index_arg.is_rebuild_index_ &&
                    FALSE_IT(tmp_arg.index_type_ = INDEX_TYPE_VEC_ROWKEY_VID_LOCAL)) {
          } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator, tmp_arg.index_type_, tmp_arg.index_name_, tmp_arg.index_name_))) {
            LOG_WARN("failed to adjust vec index name", K(ret));
          } else if (OB_FAIL(ddl_service_.check_aux_index_schema_exist(tenant_id,
                                                                        tmp_arg,
                                                                        schema_guard,
                                                                        &new_table_schema,
                                                                        rowkey_vid_exist,
                                                                        rowkey_vid_schema))) {
            LOG_WARN("fail to check rowkey vid schema existence", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (share::schema::is_vec_hnsw_index(my_arg.index_type_) &&
                 !create_index_arg.is_rebuild_index_ &&
                 !rowkey_vid_exist &&
                 FALSE_IT(my_arg.index_type_ = INDEX_TYPE_VEC_ROWKEY_VID_LOCAL)) {
        // 1. generate rowkey vid schema if not exist
        // 2. otherwise generate vec index aux schema
      } else if (create_index_arg.is_rebuild_index_) {
        if (share::schema::is_vec_index(my_arg.index_type_)) {
          if (OB_FAIL(ObVectorIndexUtil::generate_index_schema_from_exist_table(tenant_id,
                                                                              schema_guard,
                                                                              ddl_service_,
                                                                              create_index_arg,
                                                                              table_schema,
                                                                              index_schema))) {
            LOG_WARN("fail to generate index schema from exist table", K(ret), K(tenant_id), K(create_index_arg));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index type to generate index schema from exist table", K(ret), K(my_arg.index_type_));
        }
      } else if (share::schema::is_fts_or_multivalue_index(my_arg.index_type_) &&
                 !rowkey_doc_exist &&
                 (FALSE_IT(my_arg.index_type_ = INDEX_TYPE_ROWKEY_DOC_ID_LOCAL) ||
                  FALSE_IT(is_generate_rowkey_doc = true))) {
        // 1. generate rowkey doc schema if not exist
        // 2. otherwise generate fts index aux schema
      } else if (share::schema::is_fts_index(my_arg.index_type_) &&
          OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(my_arg, &allocator))) {
        LOG_WARN("failed to adjust fts index name", K(ret));
      } else if (share::schema::is_vec_index(my_arg.index_type_) &&
          OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator, my_arg.index_type_, my_arg.index_name_, my_arg.index_name_))) {
        LOG_WARN("failed to adjust vec index name", K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::adjust_expr_index_args(
              my_arg, new_table_schema, allocator, gen_columns))) {
        LOG_WARN("fail to adjust expr index args", K(ret));
      } else if (is_generate_rowkey_doc &&
                 OB_FAIL(ObDDLLock::lock_table_in_trans(new_table_schema, transaction::tablelock::EXCLUSIVE, trans))) {
        LOG_WARN("fail to lock for offline ddl", K(ret), K(new_table_schema));
      } else if (OB_FAIL(generate_schema(my_arg, new_table_schema,
                                         global_index_without_column_info,
                                         true/*generate_id*/, index_schema))) {
        LOG_WARN("fail to generate schema", K(ret), K(my_arg));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(new_table_schema.check_create_index_on_hidden_primary_key(index_schema))) {
        LOG_WARN("failed to check create index on table", K(ret), K(index_schema));
      } else if (gen_columns.empty()) {
        if (OB_FAIL(ddl_service_.create_index_table(my_arg, tenant_data_version, index_schema, trans))) {
          LOG_WARN("fail to create index", K(ret), K(index_schema));
        }
      } else {
        if (OB_FAIL(ddl_service_.create_inner_expr_index(trans,
                                                         table_schema,
                                                         tenant_data_version,
                                                         new_table_schema,
                                                         gen_columns,
                                                         index_schema))) {
          LOG_WARN("fail to create inner expr index", K(ret));
        }
      }

      if (OB_SUCC(ret) && is_generate_rowkey_doc) {
        if (OB_FAIL(ObDDLUtil::obtain_snapshot(trans, new_table_schema, index_schema, new_fetched_snapshot))) {
          LOG_WARN("fail to obtain snapshot",
              K(ret), K(new_table_schema), K(index_schema), K(new_fetched_snapshot));
        }
      }
      LOG_INFO("create_index_arg.index_type_", K(create_index_arg.index_type_), K(create_index_arg.index_key_));
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(submit_build_index_task(trans,
                                                 create_index_arg,
                                                 &new_table_schema,
                                                 nullptr/*inc_data_tablet_ids*/,
                                                 nullptr/*del_data_tablet_ids*/,
                                                 &index_schema,
                                                 create_index_arg.parallelism_,
                                                 create_index_arg.consumer_group_id_,
                                                 tenant_data_version,
                                                 allocator,
                                                 task_record,
                                                 new_fetched_snapshot))) {
        LOG_WARN("failt to submit build local index task", K(ret));
      } else {
        res.index_table_id_ = index_schema.get_table_id();
        res.schema_version_ = index_schema.get_schema_version();
        res.task_id_ = task_record.task_id_;
      }

      if (OB_FAIL(ret)) {
      } else if (share::schema::is_vec_index(create_index_arg.index_type_) &&
                 create_index_arg.is_rebuild_index_ &&
                 OB_FAIL(ObDDLTaskRecordOperator::update_parent_task_message(tenant_id,
                        create_index_arg.task_id_, index_schema, res.index_table_id_, res.task_id_,
                        ObDDLUpdateParentTaskIDType::UPDATE_VEC_REBUILD_CREATE_INDEX_TASK_ID, allocator, trans))) {
        LOG_WARN("fail to update parent task message", K(ret), K(create_index_arg.task_id_), K(res.task_id_));
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
        LOG_WARN("fail to publish schema", K(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
        LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
      }
    }
  }
  return ret;
}

// not generate table_id for index, caller will do that
// if we pass the data_schema argument, the create_index_arg can not set database_name
// and table_name, which will used for getting data table schema in generate_schema
int ObIndexBuilder::do_create_index(
    const ObCreateIndexArg &arg,
    obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const bool is_index = false;
  const ObTableSchema *table_schema = NULL;
  uint64_t table_id = OB_INVALID_ID;
  bool in_tenant_space = true;
  schema_guard.set_session_id(arg.session_id_);
  const uint64_t tenant_id = arg.tenant_id_;
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
             tenant_id, arg.database_name_, arg.table_name_, is_index, table_schema))) {
    LOG_WARN("get_table_schema failed", K(arg), K(ret));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(arg.database_name_), to_cstring(arg.table_name_));
    LOG_WARN("table not exist", K(arg), K(ret));
  } else if (FALSE_IT(table_id = table_schema->get_table_id())) {
  } else if (!arg.is_inner_
             && share::schema::is_fts_or_multivalue_index(arg.index_type_)
             && OB_FAIL(ddl_service_.check_fts_index_conflict(table_schema->get_tenant_id(), table_id))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("failed to check fts index ", K(ret), K(arg));
    }
  } else if (!arg.is_inner_  && share::schema::is_vec_index(arg.index_type_)
             && OB_FAIL(ddl_service_.check_vec_index_conflict(table_schema->get_tenant_id(), table_id))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("failed to check vec index ", K(ret), K(arg));
    }
  } else if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(table_id, in_tenant_space))) {
    LOG_WARN("fail to check table in tenant space", K(ret), K(table_id));
  } else if (is_inner_table(table_id)) {
    // FIXME: create index for inner table is not supported yet.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("create index on inner table not supported", K(ret), K(table_id));
  } else if (!arg.is_inner_ && table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not add index on table in recyclebin", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.check_restore_point_allow(tenant_id, *table_schema))) {
    LOG_WARN("failed to check restore point allow.", K(ret), K(tenant_id), K(table_id));
  } else if (table_schema->get_index_tid_count() >= OB_MAX_AUX_TABLE_PER_MAIN_TABLE
             || table_schema->get_index_count() >= OB_MAX_INDEX_PER_TABLE) {
    ret = OB_ERR_TOO_MANY_KEYS;
    LOG_USER_ERROR(OB_ERR_TOO_MANY_KEYS, OB_MAX_INDEX_PER_TABLE);
    int64_t index_aux_count = table_schema->get_index_tid_count();
    int64_t index_count = table_schema->get_index_count();
    LOG_WARN("too many index or index aux for table",
             K(index_count), K(OB_MAX_INDEX_PER_TABLE), K(index_aux_count), K(OB_MAX_AUX_TABLE_PER_MAIN_TABLE), K(ret));
  } else if (OB_FAIL(ddl_service_.check_fk_related_table_ddl(*table_schema, ObDDLType::DDL_CREATE_INDEX))) {
    LOG_WARN("check whether the foreign key related table is executing ddl failed", K(ret));
  } else if (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_
             || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_
             || INDEX_TYPE_DOMAIN_CTXCAT_DEPRECATED == arg.index_type_
             || INDEX_TYPE_SPATIAL_LOCAL == arg.index_type_
             || is_fts_index(arg.index_type_)
             || is_multivalue_index(arg.index_type_)
             || is_vec_index(arg.index_type_)) {
    if (OB_FAIL(do_create_local_index(schema_guard, arg, *table_schema, res))) {
      LOG_WARN("fail to do create local index", K(ret), K(arg));
    }
  } else if (INDEX_TYPE_NORMAL_GLOBAL == arg.index_type_
             || INDEX_TYPE_UNIQUE_GLOBAL == arg.index_type_
             || INDEX_TYPE_SPATIAL_GLOBAL == arg.index_type_) {
    if (!table_schema->is_partitioned_table()
        && !arg.index_schema_.is_partitioned_table()
        && !table_schema->is_auto_partitioned_table()) {
      // create a global index with local storage when both the data table and index table are non-partitioned
      // specifically, if the data table is auto-partitioned, we will create auto-partitioned global index rather
      // than global local index.
      if (OB_FAIL(do_create_local_index(schema_guard, arg, *table_schema, res))) {
        LOG_WARN("fail to do create local index", K(ret));
      }
    } else {
      if (OB_FAIL(do_create_global_index(schema_guard, arg, *table_schema, res))) {
        LOG_WARN("fail to do create global index", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index type unexpected", K(ret), "index_type", arg.index_type_);
  }
  return ret;
}

/* after global index is introducted, the arguments of this interface become complicated
 * if this interface is modified, please update this comments
 * modified by wenduo, 2018/5/15
 *   ObIndexBuilder::generate_schema is invoked in thoe following three circumstances:
 *   1. invoked when Create index to build global index: this interface helps to specifiy partition columns,
 *      the column information of index schema is generated during the resolve stage, no need to generate
 *      column information of the index schema any more in this interface,
 *      the argument global_index_without_column_info is false for this situation
 *   2. invoked when Create table with index: this interface helps to specify partition columns,
 *      the column information of index schema is generated during the resolve stage, need to to generate
 *      column information of the index schema any more in this interface,
 *      the argument global_index_without_column_info is false for this situation
 *   3. invoked when Alter table with index, in this situation only non-partition global index can be build,
 *      column information is not filled in the index schema and the global_index_without_column_info is true.
 *   when generate_schema is invoked to build a local index or a global index with local storage,
 *   global_index_without_column_info is false.
 */
int ObIndexBuilder::generate_schema(
    const ObCreateIndexArg &arg,
    ObTableSchema &data_schema,
    const bool global_index_without_column_info,
    const bool generate_id,
    ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  // some items in arg may be invalid, don't check arg here(when create table with index, alter
  // table add index)
  if (OB_UNLIKELY(!ddl_service_.is_inited())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (OB_UNLIKELY(!data_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",  K(ret), K(data_schema));
  } else if (OB_FAIL(data_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check_if_oracle_compat_mode failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    //do some check
    if (OB_SUCC(ret)) {
      if (!GCONF.enable_sys_table_ddl) {
        if (!data_schema.is_user_table() && !data_schema.is_tmp_table()) {
          ret = OB_ERR_WRONG_OBJECT;
          LOG_USER_ERROR(OB_ERR_WRONG_OBJECT, to_cstring(arg.database_name_),
                         to_cstring(arg.table_name_), "BASE_TABLE");
          ObTableType table_type = data_schema.get_table_type();
          LOG_WARN("Not support to create index on non-normal table", K(table_type), K(arg), K(ret));
        } else if (OB_INVALID_ID != arg.index_table_id_ || OB_INVALID_ID != arg.data_table_id_) {
          char err_msg[number::ObNumber::MAX_PRINTABLE_SIZE];
          MEMSET(err_msg, 0, sizeof(err_msg));
          // create index specifying index_id can only be used when the configuration is on
          ret = OB_OP_NOT_ALLOW;
          (void)snprintf(err_msg, sizeof(err_msg),
                   "%s", "create index with index_table_id/data_table_id");
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (arg.index_columns_.count() <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("index columns can't be empty", "index columns", arg.index_columns_, K(ret));
      } else {
        // do something
      }

      if (OB_FAIL(ret)) {
      } else if (share::schema::is_fts_index(arg.index_type_)) {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(data_schema.get_tenant_id(),
                                         tenant_data_version))) {
          LOG_WARN("failed to get tenant data version", K(ret));
        } else if (tenant_data_version < DATA_VERSION_4_3_2_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.3.2, create fulltext index on existing table is not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.2, fulltext index");
        }
      } else if (is_multivalue_index(arg.index_type_)) {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(data_schema.get_tenant_id(),
                                         tenant_data_version))) {
          LOG_WARN("failed to get tenant data version", K(ret));
        } else if (tenant_data_version < DATA_VERSION_4_3_1_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.3.1, multivalue index is not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, multivalue index");
        }
      } else if (share::schema::is_vec_index(arg.index_type_)) {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(data_schema.get_tenant_id(),
                                         tenant_data_version))) {
          LOG_WARN("failed to get tenant data version", K(ret));
        } else if (tenant_data_version < DATA_VERSION_4_3_3_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.3.3, vector index is not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.3, vector index");
        }
      }
      if (OB_SUCC(ret)
          && (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_
              || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_
              || INDEX_TYPE_UNIQUE_MULTIVALUE_LOCAL == arg.index_type_
              || INDEX_TYPE_DOMAIN_CTXCAT_DEPRECATED == arg.index_type_
              || INDEX_TYPE_HEAP_ORGANIZED_TABLE_PRIMARY == arg.index_type_)) {
        if (OB_FAIL(sql::ObResolverUtils::check_unique_index_cover_partition_column(
                data_schema, arg))) {
          RS_LOG(WARN, "fail to check unique key cover partition column", K(ret));
        }
      }
      int64_t index_data_length = 0;
      bool is_ctxcat_added = false;
      bool is_mysql_func_index = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
        const ObColumnSchemaV2 *data_column = NULL;
        const ObColumnSortItem &sort_item = arg.index_columns_.at(i);
        if (NULL == (data_column = data_schema.get_column_schema(sort_item.column_name_))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("get_column_schema failed", "tenant_id", data_schema.get_tenant_id(),
                   "database_id", data_schema.get_database_id(),
                   "table_name", data_schema.get_table_name(),
                   "column name", sort_item.column_name_, K(ret));
        } else if (OB_INVALID_ID != sort_item.get_column_id()
                   && data_column->get_column_id() != sort_item.get_column_id()) {
          ret = OB_ERR_INVALID_COLUMN_ID;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_ID, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("Column ID specified by create index mismatch with data table Column ID",
                   "data_table_column_id", data_column->get_column_id(),
                   "user_specidifed_column_id", sort_item.get_column_id(),
                   K(ret));
        } else if (sort_item.prefix_len_ > 0) {
          if ((index_data_length += sort_item.prefix_len_) > OB_MAX_USER_ROW_KEY_LENGTH) {
            ret = OB_ERR_TOO_LONG_KEY_LENGTH;
            LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_USER_ROW_KEY_LENGTH);
            LOG_WARN("index table rowkey length over max_user_row_key_length",
                K(index_data_length), LITERAL_K(OB_MAX_USER_ROW_KEY_LENGTH), K(ret));
          }
        } else if (FALSE_IT(is_mysql_func_index |= !is_oracle_mode && data_column->is_func_idx_column())) {
        } else if (!is_oracle_mode && data_column->is_func_idx_column() && ob_is_text_tc(data_column->get_data_type())) {
          ret = OB_ERR_FUNCTIONAL_INDEX_ON_LOB;
          LOG_WARN("Cannot create a functional index on an expression that returns a BLOB or TEXT.", K(ret));
        } else if (data_column->is_key_forbid_lob() && !data_column->is_fulltext_column()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("index created direct on large text column should only be fulltext or string", K(arg.index_type_), K(ret));
        } else if (ob_is_roaringbitmap_tc(data_column->get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("index created on roaringbitmap column is not supported", K(arg.index_type_), K(ret));
        } else if (ObTimestampTZType == data_column->get_data_type()
                   && arg.is_unique_primary_index()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("TIMESTAMP WITH TIME ZONE column can't be primary/unique key", K(arg.index_type_), K(ret));
        } else if (data_column->get_meta_type().is_blob()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("fulltext index created on blob column is not supported", K(arg.index_type_), K(ret));
        } else if (data_column->get_meta_type().is_ext() || data_column->get_meta_type().is_user_defined_sql_type()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("index created on udt column is not supported", K(arg.index_type_), K(ret));
        } else if (ob_is_json_tc(data_column->get_data_type())) {
          if (data_column->is_multivalue_generated_array_column()) {
          } else if (!is_oracle_mode && data_column->is_func_idx_column()) {
            ret = OB_ERR_FUNCTIONAL_INDEX_ON_JSON_OR_GEOMETRY_FUNCTION;
            LOG_WARN("Cannot create a functional index on an expression that returns a JSON or GEOMETRY.",K(ret));
          } else {
            ret = OB_ERR_JSON_USED_AS_KEY;
            LOG_USER_ERROR(OB_ERR_JSON_USED_AS_KEY, sort_item.column_name_.length(), sort_item.column_name_.ptr());
            LOG_WARN("JSON column cannot be used in key specification.", K(arg.index_type_), K(ret));
          }
        } else if (data_column->is_string_type()) {
          int64_t length = 0;
          if (data_column->is_fulltext_column()) {
            if (!is_ctxcat_added) {
              length = OB_MAX_OBJECT_NAME_LENGTH;
              is_ctxcat_added = true;
            }
          } else if (data_column->is_string_lob()) {
            uint64_t tenant_data_version = 0;
            if (OB_FAIL(GET_MIN_DATA_VERSION(data_schema.get_tenant_id(),
                                            tenant_data_version))) {
              LOG_WARN("failed to get tenant data version", K(ret));
            } else if (tenant_data_version < DATA_VERSION_4_3_5_1) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("tenant data version is less than 4.3.5.1, string index is not supported", K(ret), K(tenant_data_version));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.5.1, string index");
            } else {
              length = 0;
            }
          } else if (OB_FAIL(data_column->get_byte_length(length, is_oracle_mode, false))) {
            LOG_WARN("fail to get byte length of column", K(ret));
          } else if (length < 0) {
            ret = OB_ERR_WRONG_KEY_COLUMN;
            LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
            LOG_WARN("byte_length of string type column is less than zero", K(length), K(ret));
          } else { /*do nothing*/ }

          if (OB_SUCC(ret)) {
            index_data_length += length;
            if (index_data_length > OB_MAX_USER_ROW_KEY_LENGTH) {
              ret = OB_ERR_TOO_LONG_KEY_LENGTH;
              LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, (OB_MAX_USER_ROW_KEY_LENGTH));
              LOG_WARN("index table rowkey length over max_user_row_key_length",
                       K(index_data_length), LITERAL_K(OB_MAX_USER_ROW_KEY_LENGTH), K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && is_mysql_func_index) {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(data_schema.get_tenant_id(), tenant_data_version))) {
          LOG_WARN("get tenant data version failed", K(ret));
        } else if (tenant_data_version < DATA_VERSION_4_2_0_0){
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant version is less than 4.2, functional index is not supported in mysql mode", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.2, functional index in mysql mode not supported");
        }
      }

      if (OB_SUCC(ret) && data_schema.is_auto_partitioned_table()) {
        if (arg.is_spatial_index()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support to create spatial index for auto-partitioned table", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "creating spatial index for auto-partitioned table is");
        } else if (INDEX_TYPE_DOMAIN_CTXCAT_DEPRECATED == arg.index_type_) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support to create domain index for auto-partitioned table", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "creating domain index for auto-partitioned table is");
        }
      }
    }

    if (OB_SUCC(ret)) {
      // column information of the global index is filled during the resolve stage
      const bool is_index_local_storage = share::schema::is_index_local_storage(arg.index_type_);
      const bool need_generate_index_schema_column = (is_index_local_storage || global_index_without_column_info);
      schema.set_table_mode(data_schema.get_table_mode_flag());
      schema.set_lob_inrow_threshold(data_schema.get_lob_inrow_threshold());
      schema.set_table_state_flag(data_schema.get_table_state_flag());
      schema.set_mv_mode(data_schema.get_mv_mode());
      schema.set_duplicate_attribute(data_schema.get_duplicate_scope(), data_schema.get_duplicate_read_consistency());
      if (INDEX_TYPE_HEAP_ORGANIZED_TABLE_PRIMARY == arg.index_type_) {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(data_schema.get_tenant_id(), tenant_data_version))) {
          LOG_WARN("get tenant data version failed", K(ret));
        } else if (tenant_data_version < DATA_VERSION_4_3_5_1) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.3.5.1, heap table is not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.5.1, heap table is");
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(set_basic_infos(arg, data_schema, schema))) {
        LOG_WARN("set_basic_infos failed", K(arg), K(data_schema), K(ret));
      } else if (need_generate_index_schema_column
                 && OB_FAIL(set_index_table_columns(arg, data_schema, schema))) {
        LOG_WARN("set_index_table_columns failed", K(arg), K(data_schema), K(ret));
      } else if (OB_FAIL(set_index_table_options(arg, data_schema, schema))) {
        LOG_WARN("set_index_table_options failed", K(arg), K(data_schema), K(ret));
      } else if (schema.is_global_index_table() &&
                 OB_FAIL(set_global_index_auto_partition_infos(data_schema, schema))) {
        LOG_WARN("fail to set auto partition infos", KR(ret), K(data_schema), K(schema));
      } else {
        if (!share::schema::is_built_in_vec_index(arg.index_type_)) {
          // only delta_buffer_table set vector_index_param
          schema.set_index_params(arg.index_schema_.get_index_params());
        }
        schema.set_name_generated_type(arg.index_schema_.get_name_generated_type());
        LOG_INFO("finish generate index schema", K(schema), K(arg), K(need_generate_index_schema_column), K(global_index_without_column_info));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (schema.is_index_local_storage() &&
               OB_FAIL(set_local_index_partition_schema(data_schema, schema))) {
      LOG_WARN("fail to assign partition schema", KR(ret), K(schema));
    } else if (OB_FAIL(ddl_service_.try_format_partition_schema(schema))) {
      LOG_WARN("fail to format partition schema", KR(ret), K(schema));
    } else if (generate_id) {
      if (OB_FAIL(ddl_service_.generate_object_id_for_partition_schema(schema))) {
        LOG_WARN("fail to generate object_id for partition schema", KR(ret), K(schema));
      } else if (OB_FAIL(ddl_service_.generate_tablet_id(schema))) {
        LOG_WARN("fail to fetch new table id", KR(ret), K(schema));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // create index column_group after schema generate
    if (OB_FAIL(create_index_column_group(arg, schema))) {
      LOG_WARN("fail to create cg for index", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    schema.set_micro_index_clustered(data_schema.get_micro_index_clustered());
    schema.set_enable_macro_block_bloom_filter(data_schema.get_enable_macro_block_bloom_filter());
  }
  if (OB_SUCC(ret) && OB_FAIL(ObDDLService::set_dbms_job_exec_env(arg, schema))) {
    LOG_WARN("fail to set dbms_job exec_env", K(ret), K(arg));
  }
  return ret;
}

int ObIndexBuilder::create_index_column_group(const obrpc::ObCreateIndexArg &arg, ObTableSchema &index_table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(index_table_schema.get_tenant_id(), compat_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else if (compat_version >= DATA_VERSION_4_3_0_0) {
    ObArray<uint64_t> column_ids; // not include virtual column
    index_table_schema.set_column_store(true);
    bool is_all_cg_exist = arg.exist_all_column_group_; //for compat
    bool is_each_cg_exist = false;
    ObColumnGroupSchema tmp_cg;
    /* check exist column group*/
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_cgs_.count(); ++i) {
        const obrpc::ObCreateIndexArg::ObIndexColumnGroupItem &cur_item = arg.index_cgs_.at(i);
      if (!cur_item.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid cg item", K(ret), K(cur_item));
      } else if (ObColumnGroupType::SINGLE_COLUMN_GROUP == cur_item.cg_type_) {
        is_each_cg_exist = true;
      } else if (ObColumnGroupType::ALL_COLUMN_GROUP == cur_item.cg_type_) {
        is_all_cg_exist = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column group type", K(ret), K(cur_item.cg_type_));
      }
    }
    /* build each column group */
    if (OB_FAIL(ret)) {
    } else if (!is_each_cg_exist) {
    } else if (OB_FAIL(ObSchemaUtils::build_add_each_column_group(index_table_schema, index_table_schema))) {
      LOG_WARN("failed to build all cg", K(ret));
    }
    /* build all column group*/
    tmp_cg.reset();
    if (OB_FAIL(ret)) {
    } else if (!is_all_cg_exist) {
    } else if (OB_FAIL(ObSchemaUtils::build_all_column_group(index_table_schema,
                                                             index_table_schema.get_tenant_id(),
                                                             ALL_COLUMN_GROUP_ID,
                                                             tmp_cg))) {
      LOG_WARN("failed to build all column group", K(ret));
    } else if (OB_FAIL(index_table_schema.add_column_group(tmp_cg))) {
      LOG_WARN("failed to add column group", K(ret), K(index_table_schema), K(tmp_cg));
    }

    if (OB_FAIL(ret)) { /* build empty default cg*/
    } else if (FALSE_IT(tmp_cg.reset())) {
    } else if (OB_FAIL(ObSchemaUtils::build_column_group(index_table_schema, index_table_schema.get_tenant_id(),
                                                         ObColumnGroupType::DEFAULT_COLUMN_GROUP,
                                                         OB_DEFAULT_COLUMN_GROUP_NAME, column_ids,
                                                         DEFAULT_TYPE_COLUMN_GROUP_ID, tmp_cg))) {
      LOG_WARN("fail to build default type column_group", KR(ret), "table_id", index_table_schema.get_table_id());
    } else if (OB_FAIL(index_table_schema.add_column_group(tmp_cg))) {
      LOG_WARN("failed to add column group", K(ret), K(index_table_schema), K(tmp_cg));
    } else if (OB_FAIL(ObSchemaUtils::alter_rowkey_column_group(index_table_schema))) { /* build rowkey cg*/
      LOG_WARN("fail to adjust for rowkey column group", K(ret), K(index_table_schema));
    } else if (OB_FAIL(ObSchemaUtils::alter_default_column_group(index_table_schema))) { /* set val in default cg*/
      LOG_WARN("fail to adjust for default column group", K(ret), K(index_table_schema));
    }
  } else if (arg.index_cgs_.count() > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data_version not support for create index with column group", K(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3, create index with column group");
  }

  if (FAILEDx(index_table_schema.adjust_column_group_array())) {
    LOG_WARN("fail to adjust column group array", K(ret), K(index_table_schema));
  }
  return ret;
}

int ObIndexBuilder::set_basic_infos(const ObCreateIndexArg &arg,
                                    const ObTableSchema &data_schema,
                                    ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObDatabaseSchema *database = NULL;
  const uint64_t tenant_id = data_schema.get_tenant_id();
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (OB_FAIL(ddl_service_.get_schema_service().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema_guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, data_schema.get_database_id(), database))) {
    LOG_WARN("fail to get database_schema", K(ret), K(tenant_id), "database_id", data_schema.get_database_id());
  } else if (OB_ISNULL(database)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database_schema is null", K(ret), "database_id", data_schema.get_database_id());
  } else if (!data_schema.is_valid()) {
    // some items in arg may be invalid, don't check arg
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_schema), K(ret));
  } else {
    ObString index_table_name = arg.index_name_;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const uint64_t table_schema_id = data_schema.get_table_id();
    const ObTablespaceSchema *tablespace_schema = NULL;
    if (table_schema_id != ((OB_INVALID_ID == arg.data_table_id_) ? table_schema_id : arg.data_table_id_)) {
      // need to check if the data table ids are the same when data table id is specified
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid data table id", K(table_schema_id), K(arg.data_table_id_), K(ret));
    } else if (OB_FAIL(ObTableSchema::build_index_table_name(
               allocator, table_schema_id, arg.index_name_, index_table_name))) {
      LOG_WARN("build_index_table_name failed", K(table_schema_id), K(arg), K(ret));
    } else if (OB_FAIL(schema.set_table_name(index_table_name))) {
      LOG_WARN("set_table_name failed", K(index_table_name), K(arg), K(ret));
    } else {
      schema.set_name_generated_type(arg.index_schema_.get_name_generated_type());
      schema.set_table_id(arg.index_table_id_);
      schema.set_table_type(USER_INDEX);
      schema.set_index_type(arg.index_type_);
      schema.set_index_status(arg.index_option_.index_status_);
      schema.set_data_table_id(table_schema_id);

      // priority same with data table schema
      schema.set_tenant_id(tenant_id);
      schema.set_database_id(data_schema.get_database_id());
      schema.set_tablegroup_id(OB_INVALID_ID);
      schema.set_load_type(data_schema.get_load_type());
      schema.set_def_type(data_schema.get_def_type());
      if (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_
          || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_
          || INDEX_TYPE_SPATIAL_LOCAL == arg.index_type_
          || INDEX_TYPE_HEAP_ORGANIZED_TABLE_PRIMARY == arg.index_type_
          || is_fts_index(arg.index_type_)
          || is_multivalue_index(arg.index_type_)
          || is_vec_index(arg.index_type_)) {
        schema.set_part_level(data_schema.get_part_level());
      } else {} // partition level is filled during resolve stage for global index
      schema.set_charset_type(data_schema.get_charset_type());
      schema.set_collation_type(data_schema.get_collation_type());
      schema.set_row_store_type(data_schema.get_row_store_type());
      schema.set_store_format(data_schema.get_store_format());
      schema.set_storage_format_version(data_schema.get_storage_format_version());
      schema.set_tablespace_id(arg.index_schema_.get_tablespace_id());
      if (OB_FAIL(schema.set_encryption_str(arg.index_schema_.get_encryption_str()))) {
        LOG_WARN("fail to set set_encryption_str", K(ret), K(arg));
      }

      if (data_schema.get_max_used_column_id() > schema.get_max_used_column_id()) {
        schema.set_max_used_column_id(data_schema.get_max_used_column_id());
      }
      //index table will not contain auto increment column
      schema.set_autoinc_column_id(0);
      schema.set_progressive_merge_num(data_schema.get_progressive_merge_num());
      schema.set_progressive_merge_round(data_schema.get_progressive_merge_round());
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema.set_compress_func_name(data_schema.get_compress_func_name()))) {
        LOG_WARN("set_compress_func_name failed", K(data_schema));
      } else if (OB_INVALID_ID != schema.get_tablespace_id()) {
        if (OB_FAIL(schema_guard.get_tablespace_schema(
            tenant_id, schema.get_tablespace_id(), tablespace_schema))) {
          LOG_WARN("fail to get tablespace schema", K(schema), K(ret));
        } else if (OB_UNLIKELY(NULL == tablespace_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablespace schema is null", K(ret), K(schema));
        } else if (OB_FAIL(schema.set_encrypt_key(tablespace_schema->get_encrypt_key()))) {
          LOG_WARN("fail to set encrypt key", K(ret), K(schema));
        } else {
          schema.set_master_key_id(tablespace_schema->get_master_key_id());
        }
      }
    }
  }
  return ret;
}

int ObIndexBuilder::set_global_index_auto_partition_infos(const share::schema::ObTableSchema &data_schema,
                                                          share::schema::ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionOption& index_part_option = schema.get_part_option();
  ObPartitionFuncType part_type = PARTITION_FUNC_TYPE_MAX;
  if (OB_UNLIKELY(!data_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_schema), KR(ret));
  } else if (OB_UNLIKELY(!schema.is_global_index_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index type", K(schema), KR(ret));
  } else if (data_schema.is_auto_partitioned_table()) {
    // for global index, auto_part could be true only if it is valid for auto-partitioning
    // and its data table enables auto-partitioning
    bool enable_auto_split = true;
    const int64_t auto_part_size = data_schema.get_part_option().get_auto_part_size();
    if (schema.get_part_level() == PARTITION_LEVEL_ZERO) {
      if (OB_UNLIKELY(!index_part_option.get_part_func_expr_str().empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not allow to use auto-partition clause to"
                 "set presetting partition key for creating index",
                                                KR(ret), K(schema), K(data_schema));
      } else {
        const ObRowkeyInfo &presetting_partition_keys = schema.get_index_info();
        part_type = presetting_partition_keys.get_size() > 1 ?
                          ObPartitionFuncType::PARTITION_FUNC_TYPE_RANGE_COLUMNS :
                          ObPartitionFuncType::PARTITION_FUNC_TYPE_RANGE;
        for (int64_t i = 0; enable_auto_split && OB_SUCC(ret) && i < presetting_partition_keys.get_size(); ++i) {
          const ObRowkeyColumn *partition_column = presetting_partition_keys.get_column(i);
          if (OB_ISNULL(partition_column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("the partition column is NULL, ", KR(ret), K(i), K(presetting_partition_keys));
          } else {
            ObObjType type = partition_column->get_meta_type().get_type();
            if (ObResolverUtils::is_partition_range_column_type(type)) {
              /* case: create index idx1 on t1(c1) global, c1 is double type*/
              part_type = ObPartitionFuncType::PARTITION_FUNC_TYPE_RANGE_COLUMNS;
            }
            enable_auto_split = ObResolverUtils::is_valid_partition_column_type(
                                  partition_column->get_meta_type().get_type(), part_type, false);
          }
        }
      }
    } else if (schema.get_part_level() == PARTITION_LEVEL_ONE) {
      if (OB_FAIL(schema.is_partition_key_match_rowkey_prefix(enable_auto_split))) {
        LOG_WARN("fail to check whether matching", KR(ret));
      } else if (enable_auto_split && !schema.is_valid_split_part_type()) {
        enable_auto_split = false;
      }
    } else if (schema.get_part_level() == PARTITION_LEVEL_TWO) {
      enable_auto_split = false;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid part level", KR(ret), K(data_schema), K(schema));
    }

    uint64_t data_version = 0;
    if (OB_FAIL(ret)) {
    } else if (!enable_auto_split) {
      schema.forbid_auto_partition();
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(data_schema.get_tenant_id(), data_version))) {
      LOG_WARN("get tenant data version failed", K(ret));
    } else if (data_version < DATA_VERSION_4_3_4_0){
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("current data version doesn't support to split partition", KR(ret), K(data_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version lower than 4.4 is");
    } else if (OB_FAIL(schema.enable_auto_partition(auto_part_size, part_type))) {
      LOG_WARN("fail to enable auto partition", KR(ret));
    }
  }

  return ret;
}

int ObIndexBuilder::set_index_table_columns(const ObCreateIndexArg &arg,
                                            const ObTableSchema &data_schema,
                                            ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (OB_FAIL(ObIndexBuilderUtil::set_index_table_columns(arg, data_schema, schema))) {
    LOG_WARN("fail to set index table columns", K(ret));
  } else {} // no more to do
  return ret;
}

int ObIndexBuilder::set_index_table_options(const obrpc::ObCreateIndexArg &arg,
                                            const share::schema::ObTableSchema &data_schema,
                                            share::schema::ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = data_schema.get_tenant_id();
  uint64_t tenant_data_version = 0;
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get min data version failed", K(ret), K(tenant_id));
  } else if (!data_schema.is_valid()) {
    // some items in arg may be invalid, don't check arg
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_schema), K(ret));
  } else {
    schema.set_block_size(arg.index_option_.block_size_);
    schema.set_tablet_size(data_schema.get_tablet_size());
    schema.set_pctfree(data_schema.get_pctfree());
    schema.set_index_attributes_set(arg.index_option_.index_attributes_set_);
    schema.set_is_use_bloomfilter(arg.index_option_.use_bloom_filter_);
    //schema.set_progressive_merge_num(arg.index_option_.progressive_merge_num_);
    schema.set_index_using_type(arg.index_using_type_);
    schema.set_row_store_type(data_schema.get_row_store_type());
    schema.set_store_format(data_schema.get_store_format());
    // set dop for index table
    schema.set_dop(arg.index_schema_.get_dop());
    if (OB_FAIL(schema.set_compress_func_name(data_schema.get_compress_func_name()))) {
      LOG_WARN("set_compress_func_name failed", K(ret),
               "compress method", data_schema.get_compress_func_name());
    } else if (OB_FAIL(schema.set_comment(arg.index_option_.comment_))) {
      LOG_WARN("set_comment failed", "comment", arg.index_option_.comment_, K(ret));
    } else if (schema.is_fts_doc_word_aux() || schema.is_fts_index_aux()) {
      if (tenant_data_version < DATA_VERSION_4_3_5_1) {
        if (OB_FAIL(schema.set_parser_name(arg.index_option_.parser_name_))) {
          LOG_WARN("set parser name failed", K(ret), "parser_name", arg.index_option_.parser_name_);
        }
      } else if (OB_FAIL(schema.set_parser_name_and_properties(arg.index_option_.parser_name_,
                                                               arg.index_option_.parser_properties_))) {
        LOG_WARN("fail to set parser name and properties", K(ret), "parser_name", arg.index_option_.parser_name_,
            "parser_properties", arg.index_option_.parser_properties_);
      }
    } else if (is_vec_ivf_index(schema.get_index_type()) && FALSE_IT(schema.set_lob_inrow_threshold(OB_MAX_LOB_INROW_THRESHOLD))) {
    }
  }
  return ret;
}

bool ObIndexBuilder::is_final_index_status(const ObIndexStatus index_status) const
{
  return (INDEX_STATUS_AVAILABLE == index_status
          || INDEX_STATUS_UNIQUE_INELIGIBLE == index_status
          || is_error_index_status(index_status));
}

int ObIndexBuilder::set_local_index_partition_schema(const share::schema::ObTableSchema &data_schema,
                                                     share::schema::ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;

  if (!index_schema.is_index_local_storage()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(index_schema));
  } else if ((data_schema.is_partitioned_table() || data_schema.is_auto_partitioned_table())) {
    if (OB_FAIL(index_schema.assign_partition_schema_without_auto_part_attr(data_schema))) {
      LOG_WARN("fail to assign basic partition schema", KR(ret), K(index_schema));
    }
  }
  return ret;
}

int ObIndexBuilder::check_has_none_shared_index_tables_for_fts_or_multivalue_index_(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    share::schema::ObSchemaGetterGuard &schema_guard,
    bool &has_fts_or_multivalue_index)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObSimpleTableSchemaV2 *, OB_MAX_AUX_TABLE_PER_MAIN_TABLE> indexs;
  has_fts_or_multivalue_index = false;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id or data table id", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_index_schemas_with_data_table_id(tenant_id, data_table_id, indexs))) {
    LOG_WARN("fail to get index schema with data table id", K(ret), K(tenant_id), K(data_table_id));
  } else {
    bool has_other_fts_index = false;
    for (int64_t i = 0; OB_SUCC(ret) && !has_fts_or_multivalue_index && i < indexs.count(); ++i) {
      const ObSimpleTableSchemaV2 *index_schema = indexs.at(i);
      if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, index schema is nullptr", K(ret), KP(index_schema), K(i), K(indexs));
      } else if (!index_schema->is_fts_index() && !index_schema->is_multivalue_index()) {
        continue; // The index isn't fulltext index / multivalue index, just skip.
      } else if (index_schema->get_index_status() == ObIndexStatus::INDEX_STATUS_INDEX_ERROR) {
        // The index is in drop state
        continue;
      } else if (index_schema->is_fts_index_aux() ||
                 index_schema->is_fts_doc_word_aux() ||
                 index_schema->is_multivalue_index_aux()) {
        // none-shared-index tables still exist. shared-index table for FTS/MULTI-VALUE index should not be deleted
        has_fts_or_multivalue_index = true;
      }
    }
  }
  return ret;
}

int ObIndexBuilder::check_has_none_shared_index_tables_for_vector_index_(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    share::schema::ObSchemaGetterGuard &schema_guard,
    bool &has_none_share_vector_index)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObSimpleTableSchemaV2 *, OB_MAX_AUX_TABLE_PER_MAIN_TABLE> indexs;
  has_none_share_vector_index = false;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id or data table id", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_index_schemas_with_data_table_id(tenant_id, data_table_id, indexs))) {
    LOG_WARN("fail to get index schema with data table id", K(ret), K(tenant_id), K(data_table_id));
  } else {
    bool has_other_fts_index = false;
    for (int64_t i = 0; OB_SUCC(ret) && !has_none_share_vector_index && i < indexs.count(); ++i) {
      const ObSimpleTableSchemaV2 *index_schema = indexs.at(i);
      if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, index schema is nullptr", K(ret), KP(index_schema), K(i), K(indexs));
      } else if (!index_schema->is_vec_index()) {
        continue; // The index isn't vector index table, just skip.
      } else if (index_schema->is_vec_index_id_type() ||
                 index_schema->is_vec_delta_buffer_type() ||
                 index_schema->is_vec_index_snapshot_data_type()) {
        // none-shared-index tables still exist. shared-index table for VEC index should not be deleted
        has_none_share_vector_index = true;
      }
    }
  }
  return ret;
}

bool ObIndexBuilder::ignore_error_code_for_domain_index(
    const int ret,
    const obrpc::ObDropIndexArg &arg,
    const share::schema::ObTableSchema *index_schema/*= nullptr*/)
{
  const bool is_domain_index = nullptr == index_schema ?
    true : (index_schema->is_vec_index() || index_schema->is_fts_index() || index_schema->is_multivalue_index());
  bool ignore = false;
  if (!arg.is_inner_ || !is_domain_index) {
    ignore = false;
  } else if (OB_TABLE_NOT_EXIST == ret) {
    ignore = true;
  }
  return ignore;
}

int ObIndexBuilder::set_index_table_column_store_if_need(
    share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_schema));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(tenant_id), K(table_id));
  } else if (compat_version >= DATA_VERSION_4_2_0_0) {
    table_schema.set_column_store(true);
    if (table_schema.get_column_group_count() == 0) {
      if (OB_FAIL(table_schema.add_default_column_group())) {
        LOG_WARN("fail to add default column group", KR(ret), K(tenant_id), K(table_id));
      }
    }
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
