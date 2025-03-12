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
#include "sql/resolver/mv/ob_mv_provider.h"
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
            need_alter_mlog_purge_job = true;
            need_alter_mlog_info = true;
          }
        }

        if (alter_mlog_arg.is_alter_purge_next()) {
          if (0 != mlog_info.get_purge_next().compare(alter_mlog_arg.get_next_time_expr())) {
            mlog_info.set_purge_next(alter_mlog_arg.get_next_time_expr());
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

} // namespace rootserver
} // namespace oceanbase
