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

#include "ob_mview_alter_service.h"
#include "share/schema/ob_mview_info.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "sql/resolver/ddl/ob_create_view_resolver.h"
#include "storage/mview/ob_mview_sched_job_utils.h"

namespace oceanbase
{
using namespace share::schema;
namespace rootserver
{

int ObMviewAlterService::alter_mview_or_mlog_in_trans(obrpc::ObAlterTableArg &alter_table_arg,
                                                      obrpc::ObAlterTableRes &res,
                                                      ObSchemaGetterGuard &schema_guard,
                                                      share::schema::ObMultiVersionSchemaService *schema_service,
                                                      common::ObMySQLProxy *sql_proxy,
                                                      const uint64_t tenant_data_version)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  const uint64_t tenant_id = alter_table_schema.get_tenant_id();
  const ObTableSchema *orig_table_schema = NULL;
  const ObString &origin_database_name = alter_table_schema.get_origin_database_name();
  const ObString &origin_table_name = alter_table_schema.get_origin_table_name();
  int64_t schema_version = 0;
  if (OB_ISNULL(schema_service) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_service), K(sql_proxy));
  } else if (origin_database_name.empty() || origin_table_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database name or table name is empty", K(alter_table_schema), K(origin_database_name),
             K(origin_table_name), K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, origin_database_name,
                                                   origin_table_name, false, orig_table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(origin_database_name),
             K(origin_table_name));
  } else if (NULL == orig_table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_table_schema is null", KR(ret), KP(orig_table_schema));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
  } else {
    ObDDLSQLTransaction trans(schema_service);
    ObDDLOperator ddl_operator(*schema_service, *sql_proxy);
    if (OB_FAIL(trans.start(sql_proxy, tenant_id, schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(schema_version));
    } else if (orig_table_schema->is_materialized_view() && OB_FAIL(alter_mview_attributes(tenant_id, orig_table_schema, alter_table_arg, ddl_operator, schema_guard, trans))) {
      LOG_WARN("failed to alter mview attributes", KR(ret), K(alter_table_arg));
    } else if (alter_table_arg.is_alter_mlog_attributes_ && OB_FAIL(alter_mlog_attributes(tenant_id, orig_table_schema, alter_table_arg, ddl_operator, schema_guard, trans))) {
      LOG_WARN("failed to alter mlog attributes", KR(ret), K(alter_table_arg));
    }

    const bool is_commit = OB_SUCC(ret);
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(is_commit))) {
        LOG_WARN("trans end failed", K(is_commit), K(temp_ret));
        ret = is_commit ? temp_ret : ret;
      }
    }
  }

  return ret;
}
int ObMviewAlterService::alter_mview_attributes(
    const uint64_t tenant_id,
    const ObTableSchema *orig_table_schema,
    obrpc::ObAlterTableArg &alter_table_arg,
    ObDDLOperator &ddl_operator,
    ObSchemaGetterGuard &schema_guard,
    ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  obrpc::ObAlterMViewArg &mv_arg = alter_table_arg.alter_mview_arg_;
  const ObSimpleDatabaseSchema *db_schema = NULL;
  const ObTableSchema *container_table_schema = NULL;
  ObMViewInfo mview_info;

  if (OB_ISNULL(orig_table_schema) || !mv_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid alter mview arg", KR(ret), K(orig_table_schema), K(mv_arg));
  } else if (!orig_table_schema->is_materialized_view()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is not mview", KR(ret), K(orig_table_schema));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id, orig_table_schema->get_data_table_id(), container_table_schema))) {
    LOG_WARN("failed to get mv container table schema", KR(ret));
  } else if (OB_ISNULL(container_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv container table schema is null", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get tenant data version", KR(ret), K(tenant_id));
  } else if (alter_table_arg.is_alter_mview_attributes_ && DATA_VERSION_4_3_5_1 > data_version) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.3.5.1, altering mv options is not supported",
             K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "tenant data version is less than 4.3.5.1, altering mv options is");
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id, orig_table_schema->get_database_id(), db_schema))) {
    LOG_WARN("failed to get database schema", KR(ret), "db_id",
             orig_table_schema->get_database_id());
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("db schema is NULL", KR(ret), K(tenant_id), "db_id",
             orig_table_schema->get_database_id());
  } else if (OB_FAIL(ObMViewInfo::fetch_mview_info(
                 trans, tenant_id, orig_table_schema->get_table_id(), mview_info, true))) {
    LOG_WARN("failed to fetch mview info", KR(ret), "mview_id", orig_table_schema->get_table_id());
  } else {
    HEAP_VAR(ObTableSchema, new_mview_schema)
    {
    HEAP_VAR(ObTableSchema, new_container_schema)
    {
      if (OB_FAIL(new_mview_schema.assign(*orig_table_schema))) {
        LOG_WARN("failed to assign schema", K(ret));
      } else if (OB_FAIL(new_container_schema.assign(*container_table_schema))) {
        LOG_WARN("failed to assign schema", K(ret));
      } else {
        bool need_alter_mview_schema = false;
        bool need_alter_mview_refresh_job = false;
        bool need_alter_mview_info = false;

        if (mv_arg.is_alter_on_query_computation()) {
          // if (mv_arg.enable_on_query_computation_ != mview_schema->mv_on_query_computation()) {
          //   new_mview_schema.set_mv_on_query_computation(
          //       (ObMVOnQueryComputationFlag)mv_arg.enable_on_query_computation_);
          //   need_alter_mview_schema = true;
          // }
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("alter the 'on query computation' attribute of materialized view is not "
                   "supported yet",
                   KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                         "alter the 'on query computation' attribute of materialized view is");
        }
        if (mv_arg.is_alter_query_rewrite()) {
          // if (mv_arg.enable_query_rewrite_ != mview_schema->mv_enable_query_rewrite()) {
          //   new_mview_schema.set_mv_enable_query_rewrite(
          //       (ObMVEnableQueryRewriteFlag)mv_arg.enable_query_rewrite_);
          //   need_alter_mview_schema = true;
          // }
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("alter the 'query rewrite' attribute of materialized view is not supported yet",
                   KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                         "alter the 'query rewrite' attribute of materialized view is");
        }
        if (mv_arg.is_alter_refresh_method()) {
          // if (mv_arg.refresh_method_ != mview_info.get_refresh_method()) {
          //   mview_info.set_refresh_method(mv_arg.refresh_method_);
          //   need_alter_mview_info = true;
          // }
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("alter the refresh method of materialized view is not supported yet", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter the refresh method of materialized view is");
        }
        if (mv_arg.is_alter_refresh_start()) {
          if (mv_arg.get_start_time().get_timestamp() != mview_info.get_refresh_start()) {
            mview_info.set_refresh_start(mv_arg.get_start_time().get_timestamp());
            need_alter_mview_info = true;
            need_alter_mview_refresh_job = true;
          }
        }
        if (mv_arg.is_alter_refresh_next()) {
          if (0 != mview_info.get_refresh_next().compare(mv_arg.get_next_time_expr())) {
            mview_info.set_refresh_next(mv_arg.get_next_time_expr());
            need_alter_mview_info = true;
            need_alter_mview_refresh_job = true;
          }
        }
        if (mv_arg.is_alter_refresh_dop()) {
          if (mv_arg.get_refresh_dop() != mview_info.get_refresh_dop()) {
            new_container_schema.set_dop(mv_arg.get_refresh_dop());
            mview_info.set_refresh_dop(mv_arg.get_refresh_dop());
            need_alter_mview_info = true;
          }
        }
        if (mv_arg.is_alter_nested_refresh_mode()) {
          if (mv_arg.get_nested_refresh_mode() != mview_info.get_nested_refresh_mode()) {
            mview_info.set_nested_refresh_mode(mv_arg.get_nested_refresh_mode());
            need_alter_mview_info = true;
            need_alter_mview_refresh_job = true;
          }
        }

        if (OB_FAIL(ddl_operator.alter_table_options(schema_guard, new_container_schema,
                                                     *container_table_schema, false, trans))) {
          LOG_WARN("failed to update container schema", KR(ret), K(new_mview_schema));
        } else if (need_alter_mview_schema &&
                   OB_FAIL(ddl_operator.alter_table_options(schema_guard, new_mview_schema,
                                                            *orig_table_schema, false, trans))) {
          LOG_WARN("failed to update mview schema", KR(ret), K(new_mview_schema));
        } else if (need_alter_mview_refresh_job &&
                   OB_FAIL(ObMViewSchedJobUtils::replace_mview_refresh_job(
                       trans, mview_info, db_schema->get_database_name_str(),
                       orig_table_schema->get_table_name_str(), mv_arg.get_exec_env()))) {
          LOG_WARN("failed to replace refresh job", KR(ret), K(mview_info));
        } else if (need_alter_mview_info &&
                   OB_FAIL(ObMViewInfo::update_mview_attribute(trans, mview_info))) {
          LOG_WARN("failed to update mview attributes", KR(ret), K(mview_info));
        }
        LOG_INFO("[ALTER MVIEW] alter mview attributes", KR(ret), K(mv_arg), K(mview_info),
                 K(need_alter_mview_schema), K(need_alter_mview_refresh_job),
                 K(need_alter_mview_info));
      }
    }
    }
  }

  return ret;
}

int ObMviewAlterService::alter_mlog_attributes(const uint64_t tenant_id,
                                               const ObTableSchema *orig_table_schema,
                                               obrpc::ObAlterTableArg &alter_table_arg,
                                               ObDDLOperator &ddl_operator,
                                               ObSchemaGetterGuard &schema_guard,
                                               ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  obrpc::ObAlterMLogArg &alter_mlog_arg = alter_table_arg.alter_mlog_arg_;
  const ObSimpleDatabaseSchema *db_schema = NULL;
  ObMLogInfo mlog_info;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *mlog_table_schema = nullptr;

  if (OB_ISNULL(orig_table_schema) || !alter_mlog_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(orig_table_schema), K(alter_mlog_arg));
  } else if (orig_table_schema->is_materialized_view()) {
    // nested materialized view
    const ObTableSchema *container_table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, orig_table_schema->get_data_table_id(),
                                              container_table_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(container_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null container table schema", KR(ret), KP(container_table_schema));
    } else {
      data_table_schema = container_table_schema;
    }
  } else {
    data_table_schema = orig_table_schema;
  }

  if (OB_FAIL(ret)) {
  } else if (!data_table_schema->has_mlog_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table doesn't have mlog", KR(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_schema->get_mlog_tid(),
                                                   mlog_table_schema))) {
    LOG_WARN("failed to get mlog schema", KR(ret));
  } else if (OB_ISNULL(mlog_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mlog table schema is null", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get tenant data version", KR(ret), K(tenant_id));
  } else if (DATA_VERSION_4_3_5_1 > data_version) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.3.5.1, altering mlog options is not supported",
             K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "tenant data version is less than 4.3.5.1, altering mlog options is");
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id, data_table_schema->get_database_id(), db_schema))) {
    LOG_WARN("failed to get database schema", KR(ret));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("failed to get database schema", KR(ret), "db_id",
             data_table_schema->get_database_id());
  } else if (OB_FAIL(ObMLogInfo::fetch_mlog_info(
                 trans, tenant_id, mlog_table_schema->get_table_id(), mlog_info, true))) {
    LOG_WARN("failed to get mlog info", KR(ret), K(mlog_table_schema));
  } else {
    HEAP_VAR(ObTableSchema, new_mlog_schema){
      if (OB_FAIL(new_mlog_schema.assign(*mlog_table_schema))) {
        LOG_WARN("failed to assign schema", K(ret));
      } else {
        bool need_alter_mlog_purge_job = false;
        bool need_alter_mlog_info = false;

        if (alter_mlog_arg.is_alter_table_dop()) {
          if (alter_mlog_arg.get_table_dop() != new_mlog_schema.get_dop()) {
            new_mlog_schema.set_dop(alter_mlog_arg.get_table_dop());
          }
        }

        if (alter_mlog_arg.is_alter_purge_start()) {
          if (alter_mlog_arg.get_start_time().get_timestamp() != mlog_info.get_purge_start()) {
            mlog_info.set_purge_start(alter_mlog_arg.get_start_time().get_timestamp());
            mlog_info.set_purge_mode(ObMLogPurgeMode::DEFERRED);
            need_alter_mlog_purge_job = true;
            need_alter_mlog_info = true;
          }
        }

        if (alter_mlog_arg.is_alter_purge_next()) {
          if (0 != mlog_info.get_purge_next().compare(alter_mlog_arg.get_next_time_expr())) {
            mlog_info.set_purge_next(alter_mlog_arg.get_next_time_expr());
            mlog_info.set_purge_mode(ObMLogPurgeMode::DEFERRED);
            need_alter_mlog_purge_job = true;
            need_alter_mlog_info = true;
          }
        }

        if (alter_mlog_arg.is_alter_lob_threshold()) {
          const int64_t old_threshold = new_mlog_schema.get_lob_inrow_threshold();
          const int64_t new_threshold = alter_mlog_arg.get_lob_threshold();
          if (new_threshold < old_threshold) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("new threshold value should be bigger than the old one", KR(ret), K(old_threshold), K(new_threshold));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "lob inrow threshold, should be bigger than the old one");
          } else if (new_threshold != old_threshold) {
            new_mlog_schema.set_lob_inrow_threshold(new_threshold);
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ddl_operator.alter_table_options(schema_guard, new_mlog_schema,
                                                     *mlog_table_schema, false, trans))) {
          LOG_WARN("failed to update container schema", KR(ret), K(new_mlog_schema));
        } else if (need_alter_mlog_purge_job &&
                   OB_FAIL(ObMViewSchedJobUtils::replace_mlog_purge_job(
                       trans, mlog_info, db_schema->get_database_name_str(),
                       orig_table_schema->get_table_name_str(), alter_mlog_arg.get_exec_env()))) {
          LOG_WARN("failed to replace mlog purge job", KR(ret), K(mlog_info));
        } else if (need_alter_mlog_info &&
                   OB_FAIL(ObMLogInfo::update_mlog_attribute(trans, mlog_info))) {
          LOG_WARN("failed to update mlog info", KR(ret), K(mlog_info));
        }

        LOG_INFO("[ALTER MLOG] alter mlog attributes", KR(ret), K(alter_table_arg));
      }
    }
  }

  return ret;
}

int ObMviewAlterService::update_mlog_in_modify_column(
    const ObTableSchema &new_table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const ObTableSchema *mlog_schema = NULL;
  ObTableSchema new_mlog_schema;
  if (!new_table_schema.has_mlog_table()) {
    // do nothing
  } else if (OB_FAIL(schema_guard.get_table_schema(new_table_schema.get_tenant_id(), new_table_schema.get_mlog_tid(), mlog_schema))) {
    LOG_WARN("fail to get mlog schema", K(ret), K(new_table_schema));
  } else if (OB_ISNULL(mlog_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mlog schema is null", K(ret), K(new_table_schema));
  } else if (OB_FAIL(new_mlog_schema.assign(*mlog_schema))) {
    LOG_WARN("fail to assign mlog schema", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_mlog_schema.get_column_count(); ++i) {
      ObColumnSchemaV2 *new_mlog_column = new_mlog_schema.get_column_schema_by_idx(i);
      const ObColumnSchemaV2 *base_table_column = NULL;
      if (OB_ISNULL(new_mlog_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null mlog column", K(ret));
      } else if (is_mlog_special_column(new_mlog_column->get_column_id())) {
        // do nothing
      } else if (OB_ISNULL(base_table_column = new_table_schema.get_column_schema(new_mlog_column->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null base column", K(ret));
      } else if (new_mlog_column->get_meta_type() == base_table_column->get_meta_type() &&
                 new_mlog_column->get_accuracy() == base_table_column->get_accuracy() &&
                 new_mlog_column->get_sub_data_type() == base_table_column->get_sub_data_type()) {
        // do nothing
      } else if (FALSE_IT(new_mlog_column->set_meta_type(base_table_column->get_meta_type()))) {
      } else if (FALSE_IT(new_mlog_column->set_accuracy(base_table_column->get_accuracy()))) {
      } else if (FALSE_IT(new_mlog_column->set_sub_data_type(base_table_column->get_sub_data_type()))) {
      } else if (OB_FAIL(ddl_operator.update_column_and_column_group(trans, *mlog_schema, *mlog_schema,
                                                                     *new_mlog_column, false))) {
        LOG_WARN("fail to update mlog column", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObSchemaOperationType operation_type = OB_DDL_ALTER_TABLE;
      if (OB_FAIL(ddl_operator.update_table_attribute(new_mlog_schema, trans, operation_type))) {
        LOG_WARN("fail to update mlog table schema version", K(ret));
      }
    }
  }
  return ret;
}

int ObMviewAlterService::update_mview_in_modify_column(
    const ObTableSchema &new_table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> mv_list;
  bool exists_nested_mv = false;
  if (!new_table_schema.table_referenced_by_mv()) {
    // do nothing
  } else if (OB_FAIL(ObMVDepUtils::get_referring_mv_of_base_table(trans, new_table_schema.get_tenant_id(), new_table_schema.get_table_id(), mv_list, exists_nested_mv))) {
    LOG_WARN("fail to get referring mv of base table", K(ret), K(new_table_schema));
  } else if (mv_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("referring mv list of base table is empty", K(ret), K(new_table_schema));
  } else if (exists_nested_mv) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("alter table column with nested materialized view is not supported",
             K(ret), K(new_table_schema), K(mv_list));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table column with nested materialized view");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_list.count(); ++i) {
      uint64_t mv_id = mv_list.at(i);
      if (OB_FAIL(update_mview_with_new_table(mv_id, new_table_schema, schema_guard, ddl_operator, trans))) {
        LOG_WARN("fail to update mview with new column", K(ret), K(mv_id));
      }
    }
  }
  return ret;
}

int ObMviewAlterService::update_mview_with_new_table(
    const uint64_t mv_id,
    const ObTableSchema &new_table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const ObTableSchema *mv_schema = NULL;
  uint64_t container_table_id = OB_INVALID_ID;
  const ObTableSchema *container_table_schema = NULL;
  HEAP_VARS_2((ObTableSchema, new_mv_schema), (ObTableSchema, new_container_schema)) {
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mv_id, mv_schema))) {
    LOG_WARN("fail to get mview schema", K(ret), K(mv_id));
  } else if (OB_ISNULL(mv_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mview schema is null", K(ret), K(mv_id));
  } else if (OB_INVALID_ID == (container_table_id = mv_schema->get_data_table_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get container_table_id", K(ret), K(tenant_id), K(mv_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, container_table_id, container_table_schema))) {
    LOG_WARN("fail to get container_table_schema", K(ret), K(tenant_id), K(mv_id), K(container_table_id));
  } else if (OB_ISNULL(container_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("container table is null", K(ret), K(tenant_id), K(mv_id), K(container_table_id));
  } else if (OB_FAIL(rebuild_container_schema_with_new_table(schema_guard,
                                                             *mv_schema,
                                                             *container_table_schema,
                                                             new_table_schema,
                                                             new_container_schema))) {
    LOG_WARN("fail to rebuild mv container schema with new table", KPC(mv_schema));
  } else if (OB_UNLIKELY(container_table_schema->get_column_count() != new_container_schema.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mview column count mismatch", K(ret), KPC(container_table_schema), K(new_container_schema));
  } else if (OB_FAIL(new_mv_schema.assign(*mv_schema))) {
    LOG_WARN("fail to assign mv schema", K(ret));
  } else {
    bool is_offline = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < container_table_schema->get_column_count(); ++i) {
      const ObColumnSchemaV2 *orig_column = container_table_schema->get_column_schema_by_idx(i);
      ObColumnSchemaV2 *new_column = new_container_schema.get_column_schema_by_idx(i);
      ObColumnSchemaV2 *mv_column = NULL;
      if (OB_ISNULL(orig_column) || OB_ISNULL(new_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret), K(i), KPC(container_table_schema), K(new_container_schema));
      } else if (OB_FAIL(container_table_schema->check_alter_column_is_offline(orig_column,
                                                                               new_column,
                                                                               schema_guard,
                                                                               is_offline))) {
        LOG_WARN("fail to check alter column is offline", K(ret), K(i), KPC(orig_column), KPC(new_column));
      } else if (OB_UNLIKELY(is_offline)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("cascade offline ddl on materialized view", K(ret), KPC(orig_column), KPC(new_column));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "cascade offline ddl on materialized view is");
      } else if (orig_column->get_meta_type() == new_column->get_meta_type() &&
                 orig_column->get_accuracy() == new_column->get_accuracy() &&
                 orig_column->get_sub_data_type() == new_column->get_sub_data_type()) {
        // do nothing
      } else if (OB_FAIL(ddl_operator.update_column_and_column_group(trans,
                                                                     *container_table_schema,
                                                                     new_container_schema,
                                                                     *new_column,
                                                                     false))) {
        LOG_WARN("fail to update column and column group for container schema", K(ret));
      } else if (NULL == (mv_column = new_mv_schema.get_column_schema(new_column->get_column_id()))) {
        // do nothing, column only exists in the container table
      } else if (FALSE_IT(mv_column->set_meta_type(new_column->get_meta_type()))) {
      } else if (FALSE_IT(mv_column->set_accuracy(new_column->get_accuracy()))) {
      } else if (FALSE_IT(mv_column->set_sub_data_type(new_column->get_sub_data_type()))) {
      } else if (OB_FAIL(ddl_operator.update_column_and_column_group(trans,
                                                                     *mv_schema,
                                                                     new_mv_schema,
                                                                     *mv_column,
                                                                     false))) {
        LOG_WARN("fail to update column and column group for mv schema", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObSchemaOperationType operation_type = OB_DDL_ALTER_TABLE;
      if (OB_FAIL(ddl_operator.update_table_attribute(new_mv_schema, trans, operation_type))) {
        LOG_WARN("fail to update mview schema version", K(ret));
      } else if (OB_FAIL(ddl_operator.update_table_attribute(new_container_schema, trans, operation_type))) {
        LOG_WARN("fail to update container schema version", K(ret));
      } else if (OB_FAIL(update_mlog_in_modify_column(new_container_schema, schema_guard, ddl_operator, trans))) {
        LOG_WARN("fail to update mlog column", K(ret), K(new_container_schema));
      }
    }
  }
  }
  return ret;
}

int ObMviewAlterService::rebuild_container_schema_with_new_table(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &orig_mv_schema,
    const ObTableSchema &orig_container_schema,
    const ObTableSchema &new_table_schema,
    ObTableSchema &container_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  if (OB_FAIL(schema_checker.init(schema_guard, orig_mv_schema.get_tenant_id()))) {
    LOG_WARN("fail to init schema checker", K(ret));
  } else if (OB_FAIL(schema_checker.add_ddl_tmp_schema(&new_table_schema))) {
    LOG_WARN("fail to add ddl tmp schema", K(ret));
  } else if (OB_FAIL(rebuild_mv_container_schema(schema_checker,
                                                 orig_mv_schema,
                                                 orig_container_schema,
                                                 container_schema))) {
    LOG_WARN("fail to rebuild mv schema with new table", K(ret));
  }
  return ret;
}

int ObMviewAlterService::rebuild_mv_container_schema(sql::ObSchemaChecker &schema_checker,
                                                     const ObTableSchema &orig_mv_schema,
                                                     const ObTableSchema &orig_container_schema,
                                                     ObTableSchema &new_container_schema)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  const uint64_t tenant_id = orig_mv_schema.get_tenant_id();
  param.set_mem_attr(tenant_id, "DDLResolver", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL)
        .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
  CREATE_WITH_TEMP_CONTEXT(param) {
    ObIAllocator &alloc = CURRENT_CONTEXT->get_arena_allocator();
    ObSelectStmt *view_stmt = NULL;
    ObStmtFactory stmt_factory(alloc);
    ObRawExprFactory expr_factory(alloc);
    ObQueryCtx *query_ctx = NULL;
    const ObTenantSchema *tenant_schema = NULL;
    SMART_VARS_3((ObSQLSessionInfo, session_info), (ObExecContext, exec_ctx, alloc),
                  (ObPhysicalPlanCtx, phy_plan_ctx, alloc)) {
      LinkExecCtxGuard link_guard(session_info, exec_ctx);
      if (OB_FAIL(session_info.init(0, 0, &alloc))) {
        LOG_WARN("failed to init session", K(ret));
      } else if (OB_FAIL(schema_checker.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("failed to get tenant_schema", K(ret));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(session_info.init_tenant(tenant_schema->get_tenant_name_str(), tenant_id))) {
        LOG_WARN("failed to init tenant", K(ret));
      } else if (OB_FAIL(session_info.load_all_sys_vars(*(schema_checker.get_schema_guard())))) {
        LOG_WARN("failed to load system variable", K(ret));
      } else if (OB_FAIL(session_info.load_default_configs_in_pc())) {
        LOG_WARN("failed to load default configs", K(ret));
      } else if (OB_FAIL(orig_mv_schema.get_local_session_var().update_session_vars_with_local(session_info))) {
        LOG_WARN("failed to update session_info vars", K(ret));
      } else if (OB_FALSE_IT(exec_ctx.set_my_session(&session_info))) {
      } else if (OB_FALSE_IT(exec_ctx.set_physical_plan_ctx(&phy_plan_ctx))) {
      } else if (OB_ISNULL(query_ctx = stmt_factory.get_query_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(query_ctx));
      } else if (OB_FALSE_IT(query_ctx->sql_schema_guard_.set_schema_guard(schema_checker.get_schema_guard()))) {
      } else if (OB_FALSE_IT(expr_factory.set_query_ctx(query_ctx))) {
      } else if (OB_FAIL(ObMVProvider::generate_mv_stmt(alloc,
                                                        stmt_factory,
                                                        expr_factory,
                                                        schema_checker,
                                                        session_info,
                                                        orig_mv_schema,
                                                        view_stmt))) {
        LOG_WARN("failed to generate mv stmt", K(ret));
      } else if (OB_FAIL(new_container_schema.assign(orig_container_schema))) {
        LOG_WARN("failed to assign mv schema", K(ret));
      } else if (OB_FAIL(update_mv_container_schema_with_stmt(view_stmt, session_info, new_container_schema))) {
        LOG_WARN("failed to update mv schema with stmt", K(ret));
      }
    }
  }
  return ret;
}

int ObMviewAlterService::update_mv_container_schema_with_stmt(const sql::ObSelectStmt *stmt,
                                                              sql::ObSQLSessionInfo &session_info,
                                                              share::schema::ObTableSchema &mv_container_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_item_size(); ++i) {
    const ObRawExpr *select_expr = stmt->get_select_item(i).expr_;
    ObColumnSchemaV2 *col_schema = mv_container_schema.get_column_schema(OB_APP_MIN_COLUMN_ID + i);
    bool is_nullable = true; // for the primary key of MV, we should remain not nullable
    if (OB_ISNULL(select_expr) || OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FALSE_IT(is_nullable = col_schema->is_nullable())) {
    } else if (OB_FAIL(ObCreateViewResolver::fill_column_meta_infos(*select_expr,
                                                                    mv_container_schema.get_charset_type(),
                                                                    mv_container_schema.get_table_id(),
                                                                    session_info,
                                                                    *col_schema,
                                                                    true))) {
      LOG_WARN("failed to fill column meta infos", K(ret));
    } else {
      col_schema->set_nullable(is_nullable);
      col_schema->set_charset_type(ObCharset::charset_type_by_coll(col_schema->get_collation_type()));
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
