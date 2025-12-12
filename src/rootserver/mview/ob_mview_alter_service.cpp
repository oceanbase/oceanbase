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
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "sql/resolver/ob_schema_checker.h"
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
  } else if (OB_FAIL(rebuild_mv_schema_with_new_table(schema_guard, *mv_schema, new_table_schema, new_mv_schema))) {
    LOG_WARN("fail to rebuild mv schema with new table", KPC(mv_schema));
  } else if (OB_FAIL(new_container_schema.assign(*container_table_schema))) {
    LOG_WARN("fail to assign container schema", K(ret));
  } else if (OB_UNLIKELY(mv_schema->get_column_count() != new_mv_schema.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mview column count mismatch", K(ret), KPC(mv_schema), K(new_mv_schema));
  } else {
    bool is_offline = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_mv_schema.get_column_count(); ++i) {
      const ObColumnSchemaV2 *orig_column = mv_schema->get_column_schema_by_idx(i);
      ObColumnSchemaV2 *new_column = new_mv_schema.get_column_schema_by_idx(i);
      if (OB_ISNULL(orig_column) || OB_ISNULL(new_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mview column is null", K(ret), KPC(mv_schema), K(new_mv_schema));
      } else if (OB_FAIL(mv_schema->check_alter_column_is_offline(orig_column, new_column, schema_guard, is_offline))) {
        LOG_TRACE("fail to check alter column is offline", K(i), KPC(orig_column), KPC(new_column));
      } else if (is_offline) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("cascade offline ddl on materialized view", K(ret), KPC(orig_column), KPC(new_column));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "cascade offline ddl on materialized view");
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_mv_schema.get_column_count(); ++i) {
      const ObColumnSchemaV2 *orig_column = mv_schema->get_column_schema_by_idx(i);
      ObColumnSchemaV2 *new_column = new_mv_schema.get_column_schema_by_idx(i);
      ObColumnSchemaV2 *container_column = NULL;
      if (OB_ISNULL(orig_column) || OB_ISNULL(new_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mview column is null", K(ret), KPC(mv_schema), K(new_mv_schema));
      } else if (orig_column->get_meta_type() == new_column->get_meta_type() &&
                 orig_column->get_accuracy() == new_column->get_accuracy() &&
                 orig_column->get_sub_data_type() == new_column->get_sub_data_type()) {
        // do nothing
      } else if (OB_FAIL(ddl_operator.update_column_and_column_group(trans, *mv_schema,
                                                                     new_mv_schema,
                                                                     *new_column, false))) {
        LOG_WARN("fail to update column and column group", K(ret));
      } else if (OB_ISNULL(container_column = new_container_schema.get_column_schema(new_column->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get container column", K(ret));
      } else if (FALSE_IT(container_column->set_meta_type(new_column->get_meta_type()))) {
      } else if (FALSE_IT(container_column->set_accuracy(new_column->get_accuracy()))) {
      } else if (FALSE_IT(container_column->set_sub_data_type(new_column->get_sub_data_type()))) {
      } else if (OB_FAIL(ddl_operator.update_column_and_column_group(trans, *container_table_schema,
                                                                     new_container_schema,
                                                                     *container_column, false))) {
        LOG_WARN("fail to update container column", K(ret));
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

int ObMviewAlterService::rebuild_mv_schema_with_new_table(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &orig_mv_schema,
    const ObTableSchema &new_table_schema,
    ObTableSchema &mv_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  if (OB_FAIL(schema_checker.init(schema_guard, orig_mv_schema.get_tenant_id()))) {
    LOG_WARN("fail to init schema checker", K(ret));
  } else if (OB_FAIL(schema_checker.add_ddl_tmp_schema(&new_table_schema))) {
    LOG_WARN("fail to add ddl tmp schema", K(ret));
  } else if (OB_FAIL(ObDDLResolver::rebuild_mv_schema(schema_checker, orig_mv_schema, mv_schema))) {
    LOG_WARN("fail to rebuild mv schema with new table", K(ret));
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
