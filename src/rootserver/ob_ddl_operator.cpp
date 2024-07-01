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
#include "rootserver/ob_ddl_operator.h"

#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "share/ob_version.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_cluster_version.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_tenant_sql_service.h"
#include "share/schema/ob_database_sql_service.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_tablegroup_sql_service.h"
#include "share/schema/ob_user_sql_service.h"
#include "share/schema/ob_priv_sql_service.h"
#include "share/schema/ob_outline_sql_service.h"
#include "share/schema/ob_label_se_policy_sql_service.h"
#include "share/schema/ob_profile_sql_service.h"
#include "share/schema/ob_routine_info.h"
#include "share/schema/ob_routine_sql_service.h"
#include "share/schema/ob_udt_info.h"
#include "share/schema/ob_udt_sql_service.h"
#include "share/schema/ob_directory_sql_service.h"
#include "share/schema/ob_package_info.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_security_audit_sql_service.h"
#include "share/sequence/ob_sequence_ddl_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_label_security.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/expr/ob_raw_expr_modify_column_name.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/resource_manager/ob_resource_plan_info.h"
#include "rootserver/ob_ddl_sql_generator.h"
#include "rootserver/ob_root_service.h"
#include "share/schema/ob_part_mgr_util.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/schema/ob_error_info.h"
#include "rootserver/ob_root_service.h"
#include "share/stat/ob_dbms_stats_preferences.h"
#include "share/ob_tenant_info_proxy.h"//ObAllTenantInfo
#include "rootserver/ob_tablet_drop.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_zone_merge_table_operator.h"
#include "share/ob_zone_merge_info.h"
#include "storage/tx/ob_i_ts_source.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "share/scn.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/schema/ob_mview_info.h"
#include "share/schema/ob_mview_refresh_stats_params.h"
#include "storage/mview/ob_mview_sched_job_utils.h"
#include "pl/ob_pl_persistent.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
using namespace sql;
using namespace storage;

namespace rootserver
{

#define GRANT_OBJ_PRIV_TO_USER(db_name, table_name, table_id, obj_type, priv)                                              \
{                                                                                                                          \
  if (OB_SUCC(ret)) {                                                                                                      \
    ObTablePrivSortKey table_priv_key;                                                                                     \
    table_priv_key.tenant_id_ = tenant_id;                                                                                 \
    table_priv_key.user_id_ = grantee_id;                                                                                  \
    table_priv_key.db_ = ObString(#db_name);                                                                               \
    table_priv_key.table_ = ObString(#table_name);                                                                         \
    ObPrivSet priv_set;                                                                                                    \
    priv_set = OB_PRIV_##priv;                                                                                             \
    ObObjPrivSortKey obj_priv_key;                                                                                         \
    obj_priv_key.tenant_id_ = tenant_id;                                                                                   \
    obj_priv_key.obj_id_ = table_id;                                                                                       \
    obj_priv_key.obj_type_ = static_cast<uint64_t>(obj_type);                                                              \
    obj_priv_key.col_id_ = OB_COMPACT_COLUMN_INVALID_ID;                                                                   \
    obj_priv_key.grantor_id_ = OB_ORA_SYS_USER_ID;                                                                         \
    obj_priv_key.grantee_id_ = grantee_id;                                                                                 \
    share::ObRawObjPrivArray priv_array;                                                                                   \
    if (OB_FAIL(priv_array.push_back(OBJ_PRIV_ID_##priv))) {                                                               \
      LOG_WARN("priv array push back failed", K(ret));                                                                     \
    } else if (OB_FAIL(this->grant_table(table_priv_key, priv_set, NULL, trans,                                             \
        priv_array, 0, obj_priv_key))){                                                                                     \
      LOG_WARN("fail to grant table", K(ret), K(table_priv_key), K(priv_set), K(priv_array), K(obj_priv_key));             \
    }                                                                                                                      \
  }                                                                                                                        \
}

ObSysStat::Item::Item(ObSysStat::ItemList &list, const char *name, const char *info)
  : name_(name), info_(info)
{
  value_.set_int(0);
  const bool add_success = list.add_last(this);
  if (!add_success) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "add last failed");
  }
}

#define MAX_ID_NAME_INFO(id) ObMaxIdFetcher::get_max_id_name(id), ObMaxIdFetcher::get_max_id_info(id)
ObSysStat::ObSysStat()
  : ob_max_used_tenant_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_TENANT_ID_TYPE)),
    ob_max_used_unit_config_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_UNIT_CONFIG_ID_TYPE)),
    ob_max_used_resource_pool_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_RESOURCE_POOL_ID_TYPE)),
    ob_max_used_unit_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_UNIT_ID_TYPE)),
    ob_max_used_server_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_SERVER_ID_TYPE)),
    ob_max_used_ddl_task_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_DDL_TASK_ID_TYPE)),
    ob_max_used_unit_group_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_UNIT_GROUP_ID_TYPE)),
    ob_max_used_normal_rowid_table_tablet_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE)),
    ob_max_used_extended_rowid_table_tablet_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE)),
    ob_max_used_ls_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_LS_ID_TYPE)),
    ob_max_used_ls_group_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_LS_GROUP_ID_TYPE)),
    ob_max_used_sys_pl_object_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_SYS_PL_OBJECT_ID_TYPE)),
    ob_max_used_object_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_OBJECT_ID_TYPE)),
    ob_max_used_rewrite_rule_version_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_REWRITE_RULE_VERSION_TYPE))
{
}

// set values after bootstrap
int ObSysStat::set_initial_values(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(tenant_id)) {
    ob_max_used_tenant_id_.value_.set_int(OB_USER_TENANT_ID);
    ob_max_used_unit_config_id_.value_.set_int(OB_USER_UNIT_CONFIG_ID);
    ob_max_used_resource_pool_id_.value_.set_int(OB_USER_RESOURCE_POOL_ID);
    ob_max_used_unit_id_.value_.set_int(OB_USER_UNIT_ID);
    ob_max_used_server_id_.value_.set_int(OB_INIT_SERVER_ID - 1);
    ob_max_used_ddl_task_id_.value_.set_int(OB_INIT_DDL_TASK_ID);
    ob_max_used_unit_group_id_.value_.set_int(OB_USER_UNIT_GROUP_ID);
  } else {
    const int64_t root_own_count = 6;
    for (int64_t i = 0; i < root_own_count && OB_SUCC(ret); ++i) {
      const bool remove_succeed = item_list_.remove_first();
      if (!remove_succeed) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("remove_succeed should be true", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ob_max_used_normal_rowid_table_tablet_id_.value_.set_int(ObTabletID::MIN_USER_NORMAL_ROWID_TABLE_TABLET_ID);
    ob_max_used_extended_rowid_table_tablet_id_.value_.set_int(ObTabletID::MIN_USER_EXTENDED_ROWID_TABLE_TABLET_ID);
    ob_max_used_ls_id_.value_.set_int(ObLSID::MIN_USER_LS_ID);
    ob_max_used_ls_group_id_.value_.set_int(ObLSID::MIN_USER_LS_GROUP_ID);
    ob_max_used_sys_pl_object_id_.value_.set_int(OB_MIN_SYS_PL_OBJECT_ID);
    // Use OB_INITIAL_TEST_DATABASE_ID to avoid confict when create tenant with initial user schema objects.
    ob_max_used_object_id_.value_.set_int(OB_INITIAL_TEST_DATABASE_ID);
    ob_max_used_rewrite_rule_version_.value_.set_int(OB_INIT_REWRITE_RULE_VERSION);
  }
  return ret;
}

ObDDLOperator::ObDDLOperator(
    ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy)
    : schema_service_(schema_service),
      sql_proxy_(sql_proxy)
{
}

ObDDLOperator::~ObDDLOperator()
{
}

int ObDDLOperator::create_tenant(ObTenantSchema &tenant_schema,
                                 const ObSchemaOperationType op,
                                 ObMySQLTransaction &trans,
                                 const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_DDL_ADD_TENANT != op
      && OB_DDL_ADD_TENANT_START != op
      && OB_DDL_ADD_TENANT_END != op) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid operation type", K(ret), K(op));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(OB_SYS_TENANT_ID, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret));
  } else {
    ObTenantStatus tenant_status = TENANT_STATUS_NORMAL;
    if (0 < tenant_schema.get_drop_tenant_time()) {
      // A dropping status tenant is created when standby cluster load snapshot.
      tenant_status = TENANT_STATUS_DROPPING;
    } else if (OB_DDL_ADD_TENANT_START == op) {
      if (tenant_schema.is_restore_tenant_status()) {
        tenant_status = TENANT_STATUS_RESTORE;
      } else if (tenant_schema.is_creating_standby_tenant_status()) {
        tenant_status = TENANT_STATUS_CREATING_STANDBY;
      } else {
        tenant_status = TENANT_STATUS_CREATING;
      }
    }
    tenant_schema.set_schema_version(new_schema_version);
    tenant_schema.set_status(tenant_status);
    if (OB_FAIL(schema_service->get_tenant_sql_service().insert_tenant(
        tenant_schema, op, trans, ddl_stmt_str))) {
      LOG_WARN("insert tenant failed", K(tenant_schema), K(ret));
    }
  }
  LOG_INFO("create tenant", K(ret), "tenant_id", tenant_schema.get_tenant_id(),
           "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::insert_tenant_merge_info(
    const ObSchemaOperationType op,
    const ObTenantSchema &tenant_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    // add zone merge info
    if ((OB_DDL_ADD_TENANT_START == op) || (OB_DDL_ADD_TENANT == op)) {
      HEAP_VARS_4((ObGlobalMergeInfo, global_info),
                  (ObZoneMergeInfoArray, merge_info_array),
                  (ObZoneArray, zone_list),
                  (ObZoneMergeInfo, tmp_merge_info)) {

        global_info.tenant_id_ = tenant_id;
        tmp_merge_info.tenant_id_ = tenant_id;
        if (OB_FAIL(tenant_schema.get_zone_list(zone_list))) {
          LOG_WARN("fail to get zone list", KR(ret));
        }

        for (int64_t i = 0; (i < zone_list.count()) && OB_SUCC(ret); ++i) {
          tmp_merge_info.zone_ = zone_list.at(i);
          if (OB_FAIL(merge_info_array.push_back(tmp_merge_info))) {
            LOG_WARN("fail to push_back", KR(ret));
          }
        }
        // add zone merge info of current tenant(sys tenant or meta tenant)
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObGlobalMergeTableOperator::insert_global_merge_info(trans,
              tenant_id, global_info))) {
            LOG_WARN("fail to insert global merge info of current tenant", KR(ret), K(global_info));
          } else if (OB_FAIL(ObZoneMergeTableOperator::insert_zone_merge_infos(
                     trans, tenant_id, merge_info_array))) {
            LOG_WARN("fail to insert zone merge infos of current tenant", KR(ret), K(tenant_id),
              K(merge_info_array));
          }
        }
        // add zone merge info of relative user tenant if current tenant is meta tenant
        if (OB_SUCC(ret) && is_meta_tenant(tenant_id)) {
          const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
          global_info.tenant_id_ = user_tenant_id;
          for (int64_t i = 0; i < merge_info_array.count(); ++i) {
            merge_info_array.at(i).tenant_id_ = user_tenant_id;
          }
          if (OB_FAIL(ObGlobalMergeTableOperator::insert_global_merge_info(trans,
              user_tenant_id, global_info))) {
            LOG_WARN("fail to insert global merge info of user tenant", KR(ret), K(global_info));
          } else if (OB_FAIL(ObZoneMergeTableOperator::insert_zone_merge_infos(
                    trans, user_tenant_id, merge_info_array))) {
            LOG_WARN("fail to insert zone merge infos of user tenant", KR(ret), K(user_tenant_id),
              K(merge_info_array));
          }
        }
      }
    }
  }

  return ret;
}


int ObDDLOperator::drop_tenant(const uint64_t tenant_id,
                               ObMySQLTransaction &trans,
                               const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null");
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(OB_SYS_TENANT_ID, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret));
  } else if (OB_FAIL(schema_service_impl->get_tenant_sql_service().delete_tenant(
      tenant_id, new_schema_version, trans, ddl_stmt_str))) {
    LOG_WARN("delete tenant failed", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDDLOperator::delay_to_drop_tenant(
    ObTenantSchema &new_tenant_schema,
    ObMySQLTransaction &trans,
    const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = new_tenant_schema.get_tenant_id();
  const int64_t timeout = THIS_WORKER.get_timeout_remain();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null");
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(
                     OB_SYS_TENANT_ID, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret));
  } else {
    new_tenant_schema.set_schema_version(new_schema_version);
    // FIXME: after 4.0, drop_tenant_time is not needed by backup&restore anymore.
    // So just record local timestmap for drop tenant.
    new_tenant_schema.set_drop_tenant_time(ObTimeUtility::current_time());
    new_tenant_schema.set_status(TENANT_STATUS_DROPPING);
    new_tenant_schema.set_in_recyclebin(false);
    if (OB_FAIL(schema_service_impl->get_tenant_sql_service().delay_to_drop_tenant(
                                     new_tenant_schema, trans, ddl_stmt_str))) {
      LOG_WARN("schema_service_impl delay to drop tenant failed", K(new_tenant_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_tenant_to_recyclebin(
    ObSqlString &new_tenant_name,
    ObTenantSchema &tenant_schema,
    ObMySQLTransaction &trans,
    const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  ObTenantSchema new_tenant_schema;
  if (OB_FAIL(new_tenant_schema.assign(tenant_schema))) {
    LOG_WARN("fail to assign", K(ret));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
  } else {
    ObRecycleObject recycle_object;
    recycle_object.set_original_name(tenant_schema.get_tenant_name_str());
    recycle_object.set_type(ObRecycleObject::TENANT);
    recycle_object.set_tenant_id(tenant_schema.get_tenant_id());
    // table_id is not usefull for recycled tenant, mock table_id as original tenant_id for DBA_RECYCLEBIN
    recycle_object.set_table_id(tenant_schema.get_tenant_id());
    recycle_object.set_tablegroup_id(OB_INVALID_ID);
    new_tenant_schema.set_in_recyclebin(true);
    if (OB_FAIL(schema_service_.gen_new_schema_version(OB_SYS_TENANT_ID, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (FALSE_IT(new_tenant_schema.set_schema_version(new_schema_version))) {
    } else if (OB_FAIL(new_tenant_schema.set_tenant_name(new_tenant_name.string()))) {
      LOG_WARN("set tenant name failed", K(ret));
    } else if (FALSE_IT(recycle_object.set_object_name(new_tenant_name.string()))) {
      LOG_WARN("fail to set object name", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_service_impl->insert_recyclebin_object(recycle_object,
                                                                trans))) {
        LOG_WARN("insert recycle object failed", K(ret));
      } else if (OB_FAIL(schema_service_impl->get_tenant_sql_service().drop_tenant_to_recyclebin(
        new_tenant_schema, trans, OB_DDL_DROP_TENANT_TO_RECYCLEBIN, ddl_stmt_str))) {
        LOG_WARN("alter_tenant failed,", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::check_tenant_exist(share::schema::ObSchemaGetterGuard &schema_guard,
                                      const ObString &tenant_name,
                                      bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  const ObTenantSchema *tenant = NULL;
  if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, tenant))) {
    LOG_WARN("fail get tenant info", K(ret));
  } else if (OB_ISNULL(tenant)) {
    is_exist = false;
  } else {
    is_exist = true;
  }
  return ret;
}

int ObDDLOperator::flashback_tenant_from_recyclebin(
                   const share::schema::ObTenantSchema &tenant_schema,
                   ObMySQLTransaction &trans,
                   const ObString &new_tenant_name,
                   share::schema::ObSchemaGetterGuard &schema_guard,
                   const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObArray<ObRecycleObject> recycle_objs;
  ObTenantSchema new_tenant_schema;
  ObString final_tenant_name;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_INVALID_ID == tenant_schema.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else if (OB_FAIL(schema_service->fetch_recycle_object(
      OB_SYS_TENANT_ID,
      tenant_schema.get_tenant_name(),
      ObRecycleObject::TENANT,
      trans,
      recycle_objs))) {
    LOG_WARN("get_recycle_object failed", K(ret));
  } else if (recycle_objs.size() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected recycle object num", K(ret),
             "tenant_name", tenant_schema.get_tenant_name_str(),
             "size", recycle_objs.size());
  } else if (OB_FAIL(new_tenant_schema.assign(tenant_schema))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    const ObRecycleObject &recycle_obj = recycle_objs.at(0);
    if (new_tenant_name.empty()) {
      final_tenant_name = recycle_obj.get_original_name();
    } else {
      final_tenant_name = new_tenant_name;
    }
    new_tenant_schema.set_in_recyclebin(false);
    if (OB_FAIL(new_tenant_schema.set_tenant_name(final_tenant_name))) {
      LOG_WARN("set tenant name failed", K(final_tenant_name));
    }
    if (OB_SUCC(ret)) {
      bool is_tenant_exist = false;
      int64_t new_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(check_tenant_exist(schema_guard,
                                     new_tenant_schema.get_tenant_name_str(),
                                     is_tenant_exist))) {
        LOG_WARN("fail to check tenant", K(ret));
      } else if (is_tenant_exist) {
        ret = OB_TENANT_EXIST;
        LOG_USER_ERROR(OB_TENANT_EXIST, new_tenant_schema.get_tenant_name_str().ptr());
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(OB_SYS_TENANT_ID,
                                                                new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(OB_SYS_TENANT_ID));
      } else if (FALSE_IT(new_tenant_schema.set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service->get_tenant_sql_service().alter_tenant(
          new_tenant_schema,
          trans,
          OB_DDL_FLASHBACK_TENANT,
          &ddl_stmt_str))) {
        LOG_WARN("update_tenant failed", K(ret), K(new_tenant_schema));
      } else if (OB_FAIL(schema_service->delete_recycle_object(
          OB_SYS_TENANT_ID,
          recycle_obj,
          trans))) {
        LOG_WARN("delete_recycle_object failed", K(ret), K(recycle_obj));
      }
    }
  }
  return ret;
}

int ObDDLOperator::purge_tenant_in_recyclebin(const share::schema::ObTenantSchema &tenant_schema,
                                              ObMySQLTransaction &trans,
                                              const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_INVALID_ID == tenant_schema.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else {
    ObArray<ObRecycleObject> recycle_objs;
    if (OB_FAIL(schema_service->fetch_recycle_object(
      OB_SYS_TENANT_ID,
      tenant_schema.get_tenant_name_str(),
      ObRecycleObject::TENANT,
      trans,
      recycle_objs))) {
      LOG_WARN("get_recycle_object failed", K(ret));
    } else if (recycle_objs.size() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected recycle object num", K(ret),
               "tenant_name", tenant_schema.get_tenant_name_str(),
               "size", recycle_objs.size());
    } else {
      const ObRecycleObject &recycle_obj = recycle_objs.at(0);
      share::schema::ObTenantSchema new_tenant_schema;
      if (OB_FAIL(new_tenant_schema.assign(tenant_schema))) {
        LOG_WARN("fail to assign", K(ret));
      } else if (OB_FAIL(delay_to_drop_tenant(
                         new_tenant_schema,
                         trans,
                         ddl_stmt_str))) {
        LOG_WARN("drop_table failed", K(ret));
      } else if (OB_FAIL(schema_service->delete_recycle_object(
                         OB_SYS_TENANT_ID,
                         recycle_obj,
                         trans))) {
        LOG_WARN("delete_recycle_object failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::rename_tenant(
    share::schema::ObTenantSchema &tenant_schema,
    common::ObMySQLTransaction &trans,
    const common::ObString *ddl_stmt_str /* = NULL */)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null");
  } else if (!tenant_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant schema", K(tenant_schema), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(
                     OB_SYS_TENANT_ID,
                     new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret));
  } else {
    tenant_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service_impl->get_tenant_sql_service().rename_tenant(
        tenant_schema, trans, ddl_stmt_str))) {
      LOG_WARN("rename tenant failed", K(ret), K(tenant_schema));
    }
  }
  return ret;
}

int ObDDLOperator::alter_tenant(ObTenantSchema &tenant_schema,
                                ObMySQLTransaction &trans,
                                const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null");
  } else if (!tenant_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant schema", K(tenant_schema), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(OB_SYS_TENANT_ID, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret));
  } else {
    tenant_schema.set_schema_version(new_schema_version);
    const ObSchemaOperationType op = OB_DDL_ALTER_TENANT;
    if (OB_FAIL(schema_service_impl->get_tenant_sql_service().alter_tenant(
        tenant_schema, trans, op, ddl_stmt_str))) {
      LOG_WARN("schema_service_impl alter_tenant failed", K(tenant_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::replace_sys_variable(ObSysVariableSchema &sys_variable_schema,
                                        const int64_t schema_version,
                                        ObMySQLTransaction &trans,
                                        const ObSchemaOperationType &operation_type,
                                        const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  sys_variable_schema.set_schema_version(schema_version);
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();

  if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema_version", K(ret), K(schema_version));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null");
  } else if (OB_FAIL(schema_service_impl->get_sys_variable_sql_service()
                     .replace_sys_variable(sys_variable_schema, trans, operation_type, ddl_stmt_str))) {
    LOG_WARN("schema_service_impl update sys variable failed", K(sys_variable_schema), K(operation_type), K(ret));
  }
  LOG_INFO("replace sys variable", K(ret),
           "tenant_id", sys_variable_schema.get_tenant_id(),
           "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::create_database(ObDatabaseSchema &database_schema,
                                   ObMySQLTransaction &trans,
                                   const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  //set the old database id
  const uint64_t tenant_id = database_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t new_database_id = database_schema.get_database_id();
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (OB_FAIL(schema_service->fetch_new_database_id(
      database_schema.get_tenant_id(), new_database_id))) {
    LOG_WARN("fetch new database id failed", K(database_schema.get_tenant_id()),
             K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    database_schema.set_database_id(new_database_id);
    database_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_database_sql_service().insert_database(
        database_schema, trans, ddl_stmt_str))) {
      LOG_WARN("insert database failed", K(database_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::alter_database(ObDatabaseSchema &new_database_schema,
                                  ObMySQLTransaction &trans,
                                  const ObSchemaOperationType op_type,
                                  const ObString *ddl_stmt_str/*=NULL*/,
                                  const bool need_update_schema_version/*=true*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(ERROR, "schema_service must not null");
  } else {
    if (need_update_schema_version) {
      const uint64_t tenant_id = new_database_schema.get_tenant_id();
      int64_t new_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else {
        new_database_schema.set_schema_version(new_schema_version);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_service->get_database_sql_service()
         .update_database(new_database_schema,
                          trans,
                          op_type,
                          ddl_stmt_str))) {
      RS_LOG(WARN, "update database failed", K(new_database_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_database(const ObDatabaseSchema &db_schema,
                                 ObMySQLTransaction &trans,
                                 const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = db_schema.get_tenant_id();
  const uint64_t database_id = db_schema.get_database_id();
  int64_t new_schema_version = OB_INVALID_VERSION;

  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", OB_P(schema_service_impl), K(ret));
  }
  //drop tables in recyclebin
  if (OB_SUCC(ret)) {
    if (OB_FAIL(purge_table_of_database(db_schema, trans))) {
      LOG_WARN("purge_table_in_db failed", K(ret));
    }
  }

  //delete triggers in database, 只删除trigger_database != base_table_database 的trigger
  // trigger_database == base_table_database 的trigger会在下面删除表的时候删除
  if (OB_SUCC(ret)) {
    ObArray<uint64_t> trigger_ids;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_trigger_ids_in_database(tenant_id, database_id, trigger_ids))) {
      LOG_WARN("get trigger infos in database failed", KR(ret), K(tenant_id), K(database_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < trigger_ids.count(); i++) {
        const ObTriggerInfo *tg_info = NULL;
        const uint64_t trigger_id = trigger_ids.at(i);
        if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
          LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
        } else if (OB_FAIL(schema_guard.get_trigger_info(tenant_id, trigger_id, tg_info))) {
          LOG_WARN("fail to get trigger info", KR(ret), K(tenant_id), K(trigger_id));
        } else if (OB_ISNULL(tg_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("trigger info is NULL", K(ret));
        } else {
          const ObSimpleTableSchemaV2 * tbl_schema = NULL;
          OZ (schema_guard.get_simple_table_schema(tenant_id, tg_info->get_base_object_id(), tbl_schema));
          CK (OB_NOT_NULL(tbl_schema));
          if (OB_SUCC(ret) && database_id != tbl_schema->get_database_id()) {
            OZ (drop_trigger(*tg_info, trans, NULL));
          }
        }
      }
    }
  }

  // delete tables in database
  if (OB_SUCC(ret)) {
    ObArray<uint64_t> table_ids;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_ids_in_database(tenant_id, database_id, table_ids))) {
      LOG_WARN("get tables in database failed", K(tenant_id), KT(database_id), K(ret));
    } else {
      // drop index tables first
      for (int64_t cycle = 0; OB_SUCC(ret) && cycle < 2; ++cycle) {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
          const ObTableSchema *table = NULL;
          const uint64_t table_id = table_ids.at(i);
          if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
            LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
          } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table))) {
            LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
          } else if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table is NULL", K(ret));
          } else if (table->is_in_recyclebin()) {
            // already been dropped before
          } else {
            bool is_delete_first = table->is_aux_table() || table->is_mlog_table();
            if ((0 == cycle ? is_delete_first : !is_delete_first)) {
              // drop triggers before drop table
              if (OB_FAIL(drop_trigger_cascade(*table, trans))) {
                LOG_WARN("drop trigger failed", K(ret), K(table->get_table_id()));
              } else if (OB_FAIL(drop_table(*table, trans, NULL, false, NULL, true))) {
                LOG_WARN("drop table failed", K(ret), K(table->get_table_id()));
              }
            }
          }
        }
      }
    }
  }

  // delete outlines in database
  if (OB_SUCC(ret)) {
    ObArray<const ObSimpleOutlineSchema *> outline_schemas;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_simple_outline_schemas_in_database(tenant_id, database_id, outline_schemas))) {
      LOG_WARN("get outlines in database failed", K(tenant_id), KT(database_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < outline_schemas.count(); ++i) {
        const ObSimpleOutlineSchema *outline_schema = outline_schemas.at(i);
        if (OB_ISNULL(outline_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("outline info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_outline_sql_service().delete_outline(tenant_id,
                                                                                         database_id,
                                                                                         outline_schema->get_outline_id(),
                                                                                         new_schema_version, trans))) {
          LOG_WARN("drop outline failed", KR(ret), "outline_id", outline_schema->get_outline_id());
        }
      }
    }
  }
  // delete synonyms in database
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    ObArray<const ObSimpleSynonymSchema *> synonym_schemas;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_simple_synonym_schemas_in_database(tenant_id, database_id, synonym_schemas))) {
      LOG_WARN("get synonyms in database failed", K(tenant_id), KT(database_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < synonym_schemas.count(); ++i) {
        const ObSimpleSynonymSchema *synonym_schema = synonym_schemas.at(i);
        if (OB_ISNULL(synonym_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("synonym info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_synonym_sql_service().delete_synonym(tenant_id,
                                                                                         database_id,
                                                                                         synonym_schema->get_synonym_id(),
                                                                                         new_schema_version, &trans))) {
          LOG_WARN("drop synonym failed", KR(ret), "synonym_id", synonym_schema->get_synonym_id());
        }
      }
    }
  }

  // delete packages in database
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    ObArray<const ObSimplePackageSchema*> package_schemas;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_simple_package_schemas_in_database(tenant_id, database_id, package_schemas))) {
       LOG_WARN("get packages in database failed", K(tenant_id), KT(database_id), K(ret));
     } else {
       ObArray<const ObSAuditSchema *> audits;
       common::ObSqlString public_sql_string;
       for (int64_t i = 0; OB_SUCC(ret) && i < package_schemas.count(); ++i) {
         const ObSimplePackageSchema *package_schema = package_schemas.at(i);
         if (OB_ISNULL(package_schema)) {
           ret = OB_ERR_UNEXPECTED;
           LOG_WARN("package info is NULL", K(ret));
         } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
         } else if (OB_FAIL(schema_service_impl->get_routine_sql_service().drop_package(
                                                                           package_schema->get_tenant_id(),
                                                                           package_schema->get_database_id(),
                                                                           package_schema->get_package_id(),
                                                                           new_schema_version, trans))) {
           LOG_WARN("drop package failed", KR(ret), "package_id", package_schema->get_package_id());
         } else if (OB_FAIL(pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans,
                                                                              tenant_id,
                                                                              package_schema->get_package_id(),
                                                                              package_schema->get_database_id()))) {
          LOG_WARN("fail to delete ddl from disk", K(ret));
         } else {
           // delete audit in package
           audits.reuse();
           public_sql_string.reuse();
           if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                              AUDIT_PACKAGE,
                                                              package_schema->get_package_id(),
                                                              audits))) {
             LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
           } else if (!audits.empty()) {
             for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
               const ObSAuditSchema *audit_schema = audits.at(i);
               if (OB_ISNULL(audit_schema)) {
                 ret = OB_ERR_UNEXPECTED;
                 LOG_WARN("audit_schema is NULL", K(ret));
               } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
                 LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
               } else if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
                   *audit_schema,
                   AUDIT_MT_DEL,
                   false,
                   new_schema_version,
                   NULL,
                   trans,
                   public_sql_string))) {
                 LOG_WARN("drop audit_schema failed",  KPC(audit_schema), K(ret));
               } else {
                 LOG_INFO("succ to delete audit_schema from drop package", KPC(audit_schema));
               }
             }
           } else {
             LOG_DEBUG("no need to delete audit_schema from drop package", K(audits), KPC(package_schema));
           }
         }
       }
     }
   }

  // delete routines in database
  if (OB_SUCC(ret)) {
    ObArray<uint64_t> routine_ids;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_routine_ids_in_database(tenant_id, database_id, routine_ids))) {
      LOG_WARN("get routines in database failed", K(tenant_id), KT(database_id), K(ret));
    } else {
      ObArray<const ObSAuditSchema *> audits;
      common::ObSqlString public_sql_string;
      for (int64_t i = 0; OB_SUCC(ret) && i < routine_ids.count(); ++i) {
        const ObRoutineInfo *routine_info = NULL;
        const uint64_t routine_id = routine_ids.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
           LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
        } else if (OB_FAIL(schema_guard.get_routine_info(tenant_id, routine_id, routine_info))) {
          LOG_WARN("fail to get routine with id", KR(ret), K(tenant_id), K(routine_id));
        } else if (OB_ISNULL(routine_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("routine info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_routine_sql_service().drop_routine(
                           *routine_info, new_schema_version, trans))) {
          LOG_WARN("drop routine failed", KR(ret), "routine_id", routine_id);
        } else if (OB_FAIL(pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans,
                                                                             routine_info->get_tenant_id(),
                                                                             routine_info->get_routine_id(),
                                                                             routine_info->get_database_id()))) {
          LOG_WARN("fail to delete ddl from disk", K(ret));
        } else {
          // delete audit in routine
          audits.reuse();
          public_sql_string.reuse();
          if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                             AUDIT_PROCEDURE,
                                                             routine_id,
                                                             audits))) {
            LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
          } else if (!audits.empty()) {
            for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
              const ObSAuditSchema *audit_schema = audits.at(i);
              if (OB_ISNULL(audit_schema)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("audit_schema is NULL", K(ret));
              } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
                LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
              } else if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
                  *audit_schema,
                  AUDIT_MT_DEL,
                  false,
                  new_schema_version,
                  NULL,
                  trans,
                  public_sql_string))) {
                LOG_WARN("drop audit_schema failed",  KPC(audit_schema), K(ret));
              } else {
                LOG_INFO("succ to delete audit_schema from drop package", KPC(audit_schema));
              }
            }
          } else {
            LOG_DEBUG("no need to delete audit_schema from drop package", K(audits), KPC(routine_info));
          }
         }
      }
    }
  }

  // delete udts in database
  if (OB_SUCC(ret)) {
    ObArray<uint64_t> udt_ids;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_udt_ids_in_database(tenant_id, database_id, udt_ids))) {
      LOG_WARN("get udts in database failed", K(tenant_id), KT(database_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < udt_ids.count(); ++i) {
        const ObUDTTypeInfo *udt_info = NULL;
        const uint64_t udt_id = udt_ids.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
           LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
        } else if (OB_FAIL(schema_guard.get_udt_info(tenant_id, udt_id, udt_info))) {
          LOG_WARN("fail to get routine with id", KR(ret), K(tenant_id), K(udt_id));
        } else if (OB_ISNULL(udt_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("routine info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_udt_sql_service().drop_udt(
                           *udt_info, new_schema_version, trans))) {
          LOG_WARN("drop routine failed", "routine_id", udt_info->get_type_id(), K(ret));
        }
        if (OB_SUCC(ret)) {
          if (udt_info->is_object_spec_ddl() &&
              OB_INVALID_ID != ObUDTObjectType::mask_object_id(udt_info->get_object_spec_id(tenant_id))) {
            OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                          ObUDTObjectType::mask_object_id(udt_info->get_object_spec_id(tenant_id)),
                          udt_info->get_database_id()));
            if (udt_info->has_type_body() &&
                OB_INVALID_ID != ObUDTObjectType::mask_object_id(udt_info->get_object_body_id(tenant_id))) {
              OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                            ObUDTObjectType::mask_object_id(udt_info->get_object_body_id(tenant_id)),
                            udt_info->get_database_id()));
            }
          } else if (udt_info->is_object_body_ddl() &&
                    OB_INVALID_ID != ObUDTObjectType::mask_object_id(udt_info->get_object_body_id(tenant_id))) {
            OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                                      ObUDTObjectType::mask_object_id(udt_info->get_object_body_id(tenant_id)),
                                      udt_info->get_database_id()));
          }
        }
      }
    }
  }

  // delete sequences in database
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    ObArray<const ObSequenceSchema*> sequence_schemas;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_sequence_schemas_in_database(tenant_id,
                                                                     database_id,
                                                                     sequence_schemas))) {
      LOG_WARN("get sequences in database failed",
               K(tenant_id), KT(database_id), K(ret));
    } else {
      ObArray<const ObSAuditSchema *> audits;
      common::ObSqlString public_sql_string;
      for (int64_t i = 0; OB_SUCC(ret) && i < sequence_schemas.count(); ++i) {
        const ObSequenceSchema *sequence_schema = sequence_schemas.at(i);
        if (OB_ISNULL(sequence_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sequence info is NULL", K(ret));
        } else if (sequence_schema->get_is_system_generated()) {
          continue;
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl
                           ->get_sequence_sql_service()
                           .drop_sequence(*sequence_schema, new_schema_version, &trans))) {
          LOG_WARN("drop sequence failed",
                   KR(ret), K(*sequence_schema));
        } else {
          // delete audit in table
          audits.reuse();
          public_sql_string.reuse();
          if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                             AUDIT_SEQUENCE,
                                                             sequence_schema->get_sequence_id(),
                                                             audits))) {
            LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
          } else if (!audits.empty()) {
            ObSchemaService *schema_service = schema_service_.get_schema_service();
            for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
              const ObSAuditSchema *audit_schema = audits.at(i);
              if (OB_ISNULL(audit_schema)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("audit_schema is NULL", K(ret));
              } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))){
                LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
              } else if (OB_FAIL(schema_service->get_audit_sql_service().handle_audit_metainfo(
                  *audit_schema,
                  AUDIT_MT_DEL,
                  false,
                  new_schema_version,
                  NULL,
                  trans,
                  public_sql_string))) {
                LOG_WARN("drop audit_schema failed",  KPC(audit_schema), K(ret));
              } else {
                LOG_INFO("succ to delete audit_schema from drop sequence", KPC(audit_schema));
              }
            }
          } else {
            LOG_DEBUG("no need to delete audit_schema from drop sequence", K(audits), KPC(sequence_schema));
          }
        }
      }
    }
  }

  // delete mock_fk_parent_tables in database
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    ObArray<uint64_t> mock_fk_parent_table_ids;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_mock_fk_parent_table_ids_in_database(tenant_id, database_id, mock_fk_parent_table_ids))) {
      LOG_WARN("fail to get mock_fk_parent_table_schemas in database", K(ret), K(tenant_id), K(database_id));
    } else {
      ObArray<ObMockFKParentTableSchema> mock_fk_parent_table_schema_array;
      int64_t new_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < mock_fk_parent_table_ids.count(); ++i) {
        ObMockFKParentTableSchema tmp_mock_fk_parent_table_schema;
        const uint64_t mock_fk_parent_table_id = mock_fk_parent_table_ids.at(i);
        const ObMockFKParentTableSchema *mock_fk_parent_table_schema = NULL;
        if (OB_FAIL(schema_guard.get_mock_fk_parent_table_schema_with_id(tenant_id,
                                                                         mock_fk_parent_table_id,
                                                                         mock_fk_parent_table_schema))) {
          LOG_WARN("fail to get mock fk parent table schema", KR(ret), K(tenant_id), K(mock_fk_parent_table_id));
        } else if (OB_ISNULL(mock_fk_parent_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mock fk parent table schema is NULL", KR(ret), K(tenant_id), K(mock_fk_parent_table_id));
        } else if (OB_FAIL(tmp_mock_fk_parent_table_schema.assign(*mock_fk_parent_table_schema))) {
          LOG_WARN("fail to assign mock_fk_parent_table_schema", K(ret), K(tenant_id), K(database_id), KPC(mock_fk_parent_table_schema));
        } else if (FALSE_IT(tmp_mock_fk_parent_table_schema.set_schema_version(new_schema_version))) {
        } else if (FALSE_IT(tmp_mock_fk_parent_table_schema.set_operation_type(ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_DROP_TABLE))) {
        } else if (OB_FAIL(mock_fk_parent_table_schema_array.push_back(tmp_mock_fk_parent_table_schema))) {
          LOG_WARN("push_back mock_fk_parent_table failed", K(ret), K(tmp_mock_fk_parent_table_schema));
        }
      }
      if (FAILEDx(deal_with_mock_fk_parent_tables(trans, schema_guard, mock_fk_parent_table_schema_array))) {
        LOG_WARN("drop mock_fk_parent_table failed", K(ret), K(mock_fk_parent_table_schema_array));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_impl->get_database_sql_service().delete_database(
        db_schema,
        new_schema_version,
        trans,
        ddl_stmt_str))) {
      LOG_WARN("delete database failed", KT(database_id), K(ret));
    }
  }
  return ret;
}

// When delete database to recyclebin, it is necessary to update schema version
// of each table, in case of hitting plan cache.
// The key of plan cache is current database name, table_id and schema version.
int ObDDLOperator::update_table_version_of_db(const ObDatabaseSchema &database_schema,
                                              ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = database_schema.get_tenant_id();
  const uint64_t database_id = database_schema.get_database_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObArray<uint64_t> table_ids;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service should not be null", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_database(tenant_id,
                                                            database_id,
                                                            table_ids))) {
    LOG_WARN("get_table_schemas_in_database failed", K(ret), K(tenant_id));
  }
  const int64_t table_count = table_ids.count();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < table_count; ++idx) {
    const ObTableSchema *table = NULL;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_ids.at(idx), table))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_ids.at(idx)));
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else if (table->is_index_table()) {
      continue;
    } else {
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      if (OB_FAIL(table->get_simple_index_infos(simple_index_infos))) {
        LOG_WARN("get_index_tid_array failed", K(ret));
      }
      ObSchemaGetterGuard tmp_schema_guard;
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        const ObTableSchema *index_table_schema = NULL;
        const uint64_t table_id = simple_index_infos.at(i).table_id_;
        if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, tmp_schema_guard))) {
          LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
        } else if (OB_FAIL(tmp_schema_guard.get_table_schema(tenant_id,
                                                             table_id,
                                                             index_table_schema))) {
          LOG_WARN("get_table_schema failed", KR(ret), "table id", table_id);
        } else if (OB_ISNULL(index_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema should not be null", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else {
          HEAP_VAR(ObTableSchema, new_index_schema) {
            if (OB_FAIL(new_index_schema.assign(*index_table_schema))) {
              LOG_WARN("fail to assign schema", KR(ret), K(tenant_id), K(table_id));
            } else {
              new_index_schema.set_schema_version(new_schema_version);
            }
            if (FAILEDx(schema_service->get_table_sql_service().update_table_options(
                trans,
                *index_table_schema,
                new_index_schema,
                OB_DDL_DROP_TABLE_TO_RECYCLEBIN,
                NULL))) {
              LOG_WARN("update_table_option failed", KR(ret), K(tenant_id), K(table_id));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        HEAP_VAR(ObTableSchema, new_ts) {
          ObSchemaOperationType op_type;
          if (OB_FAIL(new_ts.assign(*table))) {
            LOG_WARN("fail to assign schema", K(ret));
          } else {
            op_type = new_ts.is_view_table() ? OB_DDL_DROP_VIEW_TO_RECYCLEBIN : OB_DDL_DROP_TABLE_TO_RECYCLEBIN;
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else {
            new_ts.set_schema_version(new_schema_version);
            if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                trans, *table, new_ts, op_type, NULL))) {
              LOG_WARN("update_table_option failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_database_to_recyclebin(const ObDatabaseSchema &database_schema,
                                               ObMySQLTransaction &trans,
                                               const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = database_schema.get_tenant_id();
  const uint64_t database_id = database_schema.get_database_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
  } else {
    ObSqlString new_db_name;
    ObRecycleObject recycle_object;
    ObDatabaseSchema new_database_schema;
    ObSchemaService *schema_service = schema_service_.get_schema_service();
    recycle_object.set_type(ObRecycleObject::DATABASE);
    recycle_object.set_database_id(database_schema.get_database_id());
    recycle_object.set_table_id(OB_INVALID_ID);
    recycle_object.set_tablegroup_id(database_schema.get_default_tablegroup_id());
    if (OB_FAIL(recycle_object.set_original_name(database_schema.get_database_name_str()))) {
      LOG_WARN("fail to set original name for recycleb object", KR(ret), K(tenant_id), K(database_id));
    } else if (OB_FAIL(new_database_schema.assign(database_schema))) {
      LOG_WARN("fail to assign new database schema", KR(ret), K(tenant_id), K(database_id));
    } else if (FALSE_IT(new_database_schema.set_in_recyclebin(true))) {
    } else if (FALSE_IT(new_database_schema.set_default_tablegroup_id(OB_INVALID_ID))) {
     // It ensure that db schema version of insert recyclebin and alter database
     // is equal that updating table version and inserting recyclebin.
    } else if (OB_FAIL(update_table_version_of_db(database_schema, trans))) {
      LOG_WARN("update table version of db failed", K(ret), K(database_schema));
    } else if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_SYS;
      LOG_WARN("schema service should not be NULL");
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (FALSE_IT(new_database_schema.set_schema_version(new_schema_version))) {
    } else if (OB_FAIL(construct_new_name_for_recyclebin(new_database_schema, new_db_name))) {
      LOG_WARN("construct_new_name_for_recyclebin failed", K(ret));
    } else if (OB_FAIL(new_database_schema.set_database_name(new_db_name.string()))) {
      LOG_WARN("set database name failed", K(ret));
    } else if (FALSE_IT(recycle_object.set_object_name(new_db_name.string()))) {
    } else if (FALSE_IT(recycle_object.set_tenant_id(tenant_id))) {
    } else if (OB_FAIL(schema_service_impl->insert_recyclebin_object(recycle_object,
                                                              trans))) {
      LOG_WARN("insert recycle object failed", K(ret));
    } else if (OB_FAIL(alter_database(new_database_schema, trans,
                                      OB_DDL_DROP_DATABASE_TO_RECYCLEBIN,
                                      ddl_stmt_str,
                                      false /*no need_new_schema_version*/))) {
      LOG_WARN("alter_database failed,", K(ret));
    } else {
      ObSchemaGetterGuard schema_guard;
      ObArray<const ObSimpleTableSchemaV2 *> tables;
      if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schemas_in_database(tenant_id,
                                                                    database_id,
                                                                    tables))) {
        LOG_WARN("get tables in database failed", KR(ret), K(tenant_id), K(database_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
        const ObSimpleTableSchemaV2 *table_schema = tables.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table is NULL", K(ret));
        } else if (table_schema->is_view_table()
                  && OB_FAIL(ObDependencyInfo::delete_schema_object_dependency(
                            trans,
                            tenant_id,
                            table_schema->get_table_id(),
                            table_schema->get_schema_version(),
                            ObObjectType::VIEW))) {
          LOG_WARN("failed to delete_schema_object_dependency", K(ret), K(tenant_id),
          K(table_schema->get_table_id()));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::create_tablegroup(ObTablegroupSchema &tablegroup_schema,
                                     ObMySQLTransaction &trans,
                                     const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  uint64_t new_tablegroup_id = OB_INVALID_ID;
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (OB_FAIL(schema_service->fetch_new_tablegroup_id(
    tablegroup_schema.get_tenant_id(), new_tablegroup_id))) {
    LOG_WARN("failed to fetch new_tablegroup_id",
        "tenant_id", tablegroup_schema.get_tenant_id(), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    tablegroup_schema.set_tablegroup_id(new_tablegroup_id);
    tablegroup_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_tablegroup_sql_service().insert_tablegroup(
        tablegroup_schema, trans, ddl_stmt_str))) {
      LOG_WARN("insert tablegroup failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_tablegroup(const ObTablegroupSchema &tablegroup_schema,
                                   ObMySQLTransaction &trans,
                                   const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;

  ObSchemaGetterGuard schema_guard;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama schema_service_impl and schema manage must not null",
              "schema_service_impl", OB_P(schema_service_impl), K(ret));
  } else {
    // check whether tablegroup is empty, if not empty, return OB_TABLEGROUP_NOT_EMPTY
    bool not_empty = false;
    ObArray<const ObSimpleTableSchemaV2 *> tables;
    if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(
        tenant_id, tablegroup_id, tables))) {
      LOG_WARN("get table ids in tablegroup failed", K(tenant_id), KT(tablegroup_id), K(ret));
    } else if (tables.count() > 0) {
      // When tablegroup is dropped, there must not be table in tablegroup, otherwise it is failed to get tablegroup
      // schema when getting derived relation property by table. As locality and primary_zone is add in tablegroup
      // after 2.0
      //
      not_empty = true;
    }
    // check databases' default_tablegroup_id
    if (OB_SUCC(ret) && !not_empty) {
      if (OB_FAIL(schema_guard.check_database_exists_in_tablegroup(
          tenant_id, tablegroup_id, not_empty))) {
        LOG_WARN("failed to check whether database exists in table group",
                 K(tenant_id), KT(tablegroup_id), K(ret));
      }
    }
    // check tenants' default_tablegroup_id
    if (OB_SUCC(ret) && !not_empty) {
      const ObTenantSchema *tenant_schema = NULL;
      if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant info", K(ret), KT(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant schema is null", K(ret), KT(tenant_id));
      } else if (tablegroup_id == tenant_schema->get_default_tablegroup_id()) {
        not_empty = true;
      }
    }
    if (OB_SUCC(ret) && not_empty) {
      ret = OB_TABLEGROUP_NOT_EMPTY;
      LOG_WARN("tablegroup still has tables or is some databases' default tablegroup or is tenant default tablegroup, can't delete it",
          KT(tablegroup_id), K(ret));
    }
  }

  // delete tablegroup and log ddl operation
  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_impl->get_tablegroup_sql_service().
                       delete_tablegroup(tablegroup_schema,
                                         new_schema_version,
                                         trans, ddl_stmt_str))) {
      LOG_WARN("delete tablegroup failed", KT(tablegroup_id), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::alter_tablegroup(ObTablegroupSchema &new_schema,
                                    common::ObMySQLTransaction &trans,
                                    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (!new_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_schema));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema schema_service_impl must not null",
           "schema_service_impl", OB_P(schema_service_impl), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    new_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service_impl->get_tablegroup_sql_service().update_tablegroup(
                new_schema, trans, ddl_stmt_str))) {
      LOG_WARN("fail to get tablegroup sql service", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::alter_tablegroup(ObSchemaGetterGuard &schema_guard,
                                    ObTableSchema &new_table_schema,
                                    common::ObMySQLTransaction &trans,
                                    const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service_impl)) {
     ret = OB_ERR_SYS;
     RS_LOG(ERROR, "schema schema_service_impl must not null",
            "schema_service_impl", OB_P(schema_service_impl), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    new_table_schema.set_schema_version(new_schema_version);
    // check whether tablegroup is empty, if not empty, return OB_TABLEGROUP_NOT_EMPTY
    if (OB_FAIL(schema_service_impl->get_table_sql_service().update_tablegroup(
                schema_guard,
                new_table_schema,
                trans,
                ddl_stmt_str))) {
      RS_LOG(WARN, "alter tablegroup failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::handle_audit_metainfo(const share::schema::ObSAuditSchema &audit_schema,
                                         const ObSAuditModifyType modify_type,
                                         const bool need_update,
                                         const ObString *ddl_stmt_str,
                                         common::ObMySQLTransaction &trans,
                                         common::ObSqlString &public_sql_string)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not be null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(audit_schema.get_tenant_id(),
                                                            new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(audit_schema.get_tenant_id()));
  } else {
    ObAuditSqlService &tmp_audit_ss = schema_sql_service->get_audit_sql_service();
    if (OB_FAIL(tmp_audit_ss.handle_audit_metainfo(audit_schema,
                                                   modify_type,
                                                   need_update,
                                                   new_schema_version,
                                                   ddl_stmt_str,
                                                   trans,
                                                   public_sql_string))) {
      LOG_WARN("Failed to handle audit meta info", K(audit_schema), K(modify_type),
                                                   K(new_schema_version), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::get_user_id_for_inner_ur(ObUserInfo &user,
                                            bool &is_inner_ur,
                                            uint64_t &new_user_id)
{
  int ret = OB_SUCCESS;
  const char* ur_name = user.get_user_name();
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const ObSysVariableSchema *sys_variable_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  bool is_oracle_mode = false;
  const uint64_t tenant_id = user.get_tenant_id();
  is_inner_ur = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", K(ret));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", K(ret));
  } else if (OB_FAIL(sys_variable_schema->get_oracle_mode(is_oracle_mode))) {
    RS_LOG(WARN, "failed to get oracle mode", K(ret), K(tenant_id));
  } else if (is_oracle_mode) {
    if (STRCMP(ur_name, OB_ORA_LBACSYS_NAME) == 0) {
      new_user_id = OB_ORA_LBACSYS_USER_ID;
      is_inner_ur = true;
    } else if (STRCMP(ur_name, OB_ORA_CONNECT_ROLE_NAME) == 0) {
      new_user_id = OB_ORA_CONNECT_ROLE_ID;
      is_inner_ur = true;
    } else if (STRCMP(ur_name, OB_ORA_RESOURCE_ROLE_NAME) == 0) {
      new_user_id = OB_ORA_RESOURCE_ROLE_ID;
      is_inner_ur = true;
    } else if (STRCMP(ur_name, OB_ORA_DBA_ROLE_NAME) == 0) {
      new_user_id = OB_ORA_DBA_ROLE_ID;
      is_inner_ur = true;
    } else if (STRCMP(ur_name, OB_ORA_PUBLIC_ROLE_NAME) == 0) {
      new_user_id = OB_ORA_PUBLIC_ROLE_ID;
      is_inner_ur = true;
    } else if (STRCMP(ur_name, OB_ORA_STANDBY_REPLICATION_ROLE_NAME) == 0) {
      new_user_id = OB_ORA_STANDBY_REPLICATION_ROLE_ID;
      is_inner_ur = true;
    }
  }
  // both oracle mode and mysql mode
  if (OB_SUCC(ret)) {
    if (STRCMP(ur_name, OB_ORA_AUDITOR_NAME) == 0) {
      new_user_id = OB_ORA_AUDITOR_USER_ID;
      is_inner_ur = true;
    }
  }
  return ret;
}

int ObDDLOperator::create_user(ObUserInfo &user,
                               const ObString *ddl_stmt_str,
                               ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t new_user_id = user.get_user_id();
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  bool is_inner_ur;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (OB_FAIL(get_user_id_for_inner_ur(user, is_inner_ur, new_user_id))) {
      LOG_WARN("failed to fetch_new_user_id", "tennat_id", user.get_tenant_id(), K(ret));
  } else if (!is_inner_ur &&
             OB_FAIL(schema_service->fetch_new_user_id(user.get_tenant_id(), new_user_id))) {
    LOG_WARN("failed to fetch_new_user_id", "tennat_id", user.get_tenant_id(), K(ret));
  } else {
    user.set_user_id(new_user_id);
  }
  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = user.get_tenant_id();
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service->get_user_sql_service().create_user(
               user, new_schema_version, ddl_stmt_str, trans))) {
      LOG_WARN("insert user failed", K(user), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::create_table(ObTableSchema &table_schema,
                                ObMySQLTransaction &trans,
                                const ObString *ddl_stmt_str/*=NULL*/,
                                const bool need_sync_schema_version,
                                const bool is_truncate_table /*false*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(ERROR, "schema_service must not null");
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_table_sql_service().create_table(
        table_schema,
        trans,
        ddl_stmt_str,
        need_sync_schema_version,
        is_truncate_table))) {
      RS_LOG(WARN, "failed to create table", K(ret));
    } else if (OB_FAIL(sync_version_for_cascade_table(tenant_id,
               table_schema.get_depend_table_ids(), trans))) {
      RS_LOG(WARN, "fail to sync cascade depend table", K(ret));
    } else if (OB_FAIL(sync_version_for_cascade_mock_fk_parent_table(table_schema.get_tenant_id(), table_schema.get_depend_mock_fk_parent_table_ids(), trans))) {
      LOG_WARN("fail to sync cascade depend_mock_fk_parent_table_ids table", K(ret));
    }
  }

  // add audit in table if necessary
  if (OB_SUCC(ret) && !is_truncate_table && (table_schema.is_user_table() || table_schema.is_external_table())) {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    ObArray<const ObSAuditSchema *> audits;

    ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
    if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                              AUDIT_OBJ_DEFAULT,
                                                              OB_AUDIT_MOCK_USER_ID,
                                                              audits))) {
      LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(table_schema), K(ret));
    } else if (!audits.empty()) {
      common::ObSqlString public_sql_string;
      ObSAuditSchema new_audit_schema;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
        uint64_t new_audit_id = common::OB_INVALID_ID;
        const ObSAuditSchema *audit_schema = audits.at(i);
        if (OB_ISNULL(audit_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("audit_schema is NULL", K(ret));
        } else if (!audit_schema->is_access_operation_for_table()) {
          continue;
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->fetch_new_audit_id(tenant_id, new_audit_id))) {
          LOG_WARN("Failed to fetch new_audit_id", K(ret));
        } else if (OB_FAIL(new_audit_schema.assign(*audit_schema))) {
          LOG_WARN("fail to assign audit schema", KR(ret));
        } else {
          new_audit_schema.set_schema_version(new_schema_version);
          new_audit_schema.set_audit_id(new_audit_id);
          new_audit_schema.set_audit_type(AUDIT_TABLE);
          new_audit_schema.set_owner_id(table_schema.get_table_id());
          if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
              new_audit_schema,
              AUDIT_MT_ADD,
              false,
              new_schema_version,
              NULL,
              trans,
              public_sql_string))) {
            LOG_WARN("drop audit_schema failed",  K(new_audit_schema), K(ret));
          } else {
            LOG_INFO("succ to add audit_schema from default", K(new_audit_schema));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::sync_version_for_cascade_table(
    const uint64_t tenant_id,
    const ObIArray<uint64_t> &table_ids,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t id = OB_INVALID_ID;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(ERROR, "schema_service must not null");
  } else {
    for (int64_t i = 0; i < table_ids.count() && OB_SUCC(ret); i++) {
      id = table_ids.at(i);
      int64_t new_schema_version = OB_INVALID_VERSION;
      int64_t old_schema_version = OB_INVALID_VERSION;
      HEAP_VAR(ObTableSchema, table_schema) {
        ObRefreshSchemaStatus schema_status;
        schema_status.tenant_id_ = tenant_id;
        if (OB_FAIL(schema_service->get_table_schema_from_inner_table(
                      schema_status, id, trans, table_schema))) {
          LOG_WARN("get_table_schema failed", K(ret), K(id), K(tenant_id));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(id), K(tenant_id));
        } else {
          old_schema_version = table_schema.get_schema_version();
          if (OB_FAIL(schema_service->get_table_sql_service().sync_schema_version_for_history(
                      trans,
                      table_schema,
                      new_schema_version))) {
            RS_LOG(WARN, "fail to sync schema version", K(ret), K(id), K(tenant_id));
          } else {
            LOG_INFO("synced schema version for depend table", K(id), "from", old_schema_version, "to", new_schema_version);
          }
        }
      }
    }
  }

  return ret;
}

// Notice that, truncate table, offline ddl should sync origin sequence values.
int ObDDLOperator::create_sequence_in_create_table(ObTableSchema &table_schema,
                                                   common::ObMySQLTransaction &trans,
                                                   share::schema::ObSchemaGetterGuard &schema_guard,
                                                   const obrpc::ObSequenceDDLArg *sequence_ddl_arg)
{
  int ret = OB_SUCCESS;
  if (!(table_schema.is_user_table() || table_schema.is_oracle_tmp_table())) {
    // do nothing
  } else {
    for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
         OB_SUCC(ret) && iter != table_schema.column_end(); ++iter) {
      ObColumnSchemaV2 &column_schema = (**iter);
      if (!column_schema.is_identity_column()) {
        continue;
      } else {
        ObSequenceDDLProxy ddl_operator(schema_service_);
        char temp_sequence_name[OB_MAX_SEQUENCE_NAME_LENGTH + 1] = { 0 };
        int32_t len = snprintf(temp_sequence_name, sizeof(temp_sequence_name), "%s%lu%c%lu",
                              IDENTITY_COLUMN_SEQUENCE_OBJECT_NAME_PREFIX,
                              ObSchemaUtils::get_extract_schema_id(table_schema.get_tenant_id(), table_schema.get_table_id()),
                              '_',
                              column_schema.get_column_id());
        if (OB_UNLIKELY(len < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create sequence name fail", K(ret), K(column_schema));
        } else {
          ObString sequence_name = ObString::make_string(temp_sequence_name);
          ObSequenceSchema sequence_schema;
          if (nullptr != sequence_ddl_arg) {
            sequence_schema = sequence_ddl_arg->seq_schema_;
          } else {
            const ObSequenceSchema *tmp_sequence_schema = NULL;
            if (OB_FAIL(schema_guard.get_sequence_schema(table_schema.get_tenant_id(),
                                                         column_schema.get_sequence_id(),
                                                         tmp_sequence_schema))) {
              LOG_WARN("get sequence schema failed", K(ret), K(column_schema));
            } else if (OB_ISNULL(tmp_sequence_schema)) {
              ret = OB_NOT_INIT;
              LOG_WARN("sequence not found", K(ret), K(column_schema));
            } else if (OB_FAIL(sequence_schema.assign(*tmp_sequence_schema))) {
              LOG_WARN("fail to assign sequence schema", KR(ret));
            } else {}
          }
          if (OB_SUCC(ret)) {
            sequence_schema.set_database_id(table_schema.get_database_id());
            sequence_schema.set_sequence_name(sequence_name);
            if (nullptr == sequence_ddl_arg) {
              // In some scenes like trunctae table and offline ddl, should inherit the sequce object from origin table except sequence id, etc.
              // Validity check and set of option bitset are completed in creating origin table phase,
              // thus we do not have to check the validity of option_bitset again for the hidden table.
              if (OB_FAIL(ddl_operator.create_sequence_without_bitset(sequence_schema,
                                                                      trans,
                                                                      schema_guard,
                                                                      nullptr))) {
              LOG_WARN("create sequence fail", K(ret), K(table_schema));
              } else {/* do nothing. */}
            } else if (OB_FAIL(ddl_operator.create_sequence(sequence_schema,
                                                            sequence_ddl_arg->option_bitset_,
                                                            trans,
                                                            schema_guard,
                                                            NULL))) {
              LOG_WARN("create sequence fail", K(ret), K(table_schema));
            }
            if (OB_SUCC(ret)) {
              column_schema.set_sequence_id(sequence_schema.get_sequence_id());
              char sequence_string[OB_MAX_SEQUENCE_NAME_LENGTH + 1] = { 0 };
              uint64_t pure_sequence_id = ObSchemaUtils::get_extract_schema_id(table_schema.get_tenant_id(), sequence_schema.get_sequence_id());
              len = snprintf(sequence_string, sizeof(sequence_string), "%lu", pure_sequence_id);
              if (OB_UNLIKELY(len < 0)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("create sequence name fail", K(ret), K(table_schema));
              } else {
                ObObjParam cur_default_value;  // for desc table
                ObObjParam orig_default_value; // for store pure_sequence_id
                cur_default_value.set_varchar("SEQUENCE.NEXTVAL");
                cur_default_value.set_collation_type(ObCharset::get_system_collation());
                cur_default_value.set_collation_level(CS_LEVEL_IMPLICIT);
                cur_default_value.set_param_meta();
                orig_default_value.set_varchar(sequence_string);
                orig_default_value.set_collation_type(ObCharset::get_system_collation());
                orig_default_value.set_collation_level(CS_LEVEL_IMPLICIT);
                orig_default_value.set_param_meta();
                if (OB_FAIL(column_schema.set_cur_default_value(cur_default_value))) {
                  LOG_WARN("set current default value fail", K(ret));
                } else if (OB_FAIL(column_schema.set_orig_default_value(orig_default_value))) {
                  LOG_WARN("set origin default value fail", K(ret), K(column_schema));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

// Sequence_schema and table_schema have a one-to-one relationship.
// Sequence can only be changed through interfaces such as alter/drop table,
// So there is no need to increase the schema version number separately.
int ObDDLOperator::drop_sequence_in_drop_table(const ObTableSchema &table_schema,
                                               common::ObMySQLTransaction &trans,
                                               share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_user_table() || table_schema.is_oracle_tmp_table()) {
    for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
         OB_SUCC(ret) && iter != table_schema.column_end(); ++iter) {
      ObColumnSchemaV2 &column_schema = (**iter);
      if (OB_FAIL(drop_sequence_in_drop_column(column_schema, trans, schema_guard))) {
        LOG_WARN("drop sequence in drop column fail", K(ret), K(column_schema));
      }
    }
  }
  return ret;
}

int ObDDLOperator::create_sequence_in_add_column(const ObTableSchema &table_schema,
    ObColumnSchemaV2 &column_schema,
    ObMySQLTransaction &trans,
    ObSchemaGetterGuard &schema_guard,
    ObSequenceDDLArg &sequence_ddl_arg)
{
  int ret = OB_SUCCESS;
  if (column_schema.is_identity_column()) {
    ObSequenceDDLProxy ddl_operator(schema_service_);
    ObSequenceSchema sequence_schema = sequence_ddl_arg.sequence_schema();
    char temp_sequence_name[OB_MAX_SEQUENCE_NAME_LENGTH + 1] = { 0 };
    int32_t len = snprintf(temp_sequence_name, sizeof(temp_sequence_name), "%s%lu%c%lu",
                          "ISEQ$$_",
                          ObSchemaUtils::get_extract_schema_id(column_schema.get_tenant_id(), column_schema.get_table_id()),
                          '_',
                          column_schema.get_column_id());
    if (OB_UNLIKELY(len < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("create sequence name fail", K(ret), K(column_schema));
    } else {
      ObString sequence_name = ObString::make_string(temp_sequence_name);
      sequence_schema.set_database_id(table_schema.get_database_id());
      sequence_schema.set_sequence_name(sequence_name);
      if (OB_FAIL(ddl_operator.create_sequence(sequence_schema,
                                              sequence_ddl_arg.option_bitset_,
                                              trans,
                                              schema_guard,
                                              NULL))) {
        LOG_WARN("create sequence fail", K(ret));
      } else {
        column_schema.set_sequence_id(sequence_schema.get_sequence_id());
        char sequence_string[OB_MAX_SEQUENCE_NAME_LENGTH + 1] = { 0 };
        uint64_t pure_sequence_id = ObSchemaUtils::get_extract_schema_id(column_schema.get_tenant_id(), column_schema.get_sequence_id());
        len = snprintf(sequence_string, sizeof(sequence_string), "%lu", pure_sequence_id);
        if (OB_UNLIKELY(len < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create sequence name fail", K(ret), K(column_schema));
        } else {
          ObObjParam cur_default_value;  // for desc table
          ObObjParam orig_default_value; // for store pure_sequence_id
          cur_default_value.set_varchar("SEQUENCE.NEXTVAL");
          cur_default_value.set_collation_type(ObCharset::get_system_collation());
          cur_default_value.set_collation_level(CS_LEVEL_IMPLICIT);
          cur_default_value.set_param_meta();
          orig_default_value.set_varchar(sequence_string);
          orig_default_value.set_collation_type(ObCharset::get_system_collation());
          orig_default_value.set_collation_level(CS_LEVEL_IMPLICIT);
          orig_default_value.set_param_meta();
          if (OB_FAIL(column_schema.set_cur_default_value(cur_default_value))) {
            LOG_WARN("set current default value fail", K(ret));
          } else if (OB_FAIL(column_schema.set_orig_default_value(orig_default_value))) {
            LOG_WARN("set origin default value fail", K(ret), K(column_schema));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_sequence_in_drop_column(const ObColumnSchemaV2 &column_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (column_schema.is_identity_column()) {
    ObSequenceDDLProxy ddl_operator(schema_service_);
    const ObSequenceSchema *temp_sequence_schema = NULL;
    ObSequenceSchema sequence_schema;
    if (OB_FAIL(schema_guard.get_sequence_schema(column_schema.get_tenant_id(),
                                                 column_schema.get_sequence_id(),
                                                 temp_sequence_schema))) {
      LOG_WARN("get sequence schema fail", K(ret), K(column_schema));
      if (ret == OB_ERR_UNEXPECTED) {
        // sequence has been deleted externally.
        // Oracle does not allow sequences internally created to be deleted externally.
        // In the future, it will be solved by adding columns to the internal table,
        // and then the error code conversion can be removed.
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(temp_sequence_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sequence not exist", KR(ret), K(column_schema));
    } else if (OB_FAIL(sequence_schema.assign(*temp_sequence_schema))) {
      LOG_WARN("fail to assign sequence schema", KR(ret));
    } else if (OB_FAIL(ddl_operator.drop_sequence(sequence_schema,
                                           trans,
                                           schema_guard,
                                           NULL,
                                           FROM_TABLE_DDL))) {
      LOG_WARN("drop sequence fail", K(ret), K(column_schema));
    }
  }
  return ret;
}

int ObDDLOperator::reinit_autoinc_row(const ObTableSchema &table_schema,
                                      common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  uint64_t table_id = table_schema.get_table_id();
  ObString table_name = table_schema.get_table_name();
  int64_t truncate_version = table_schema.get_truncate_version();
  uint64_t column_id = table_schema.get_autoinc_column_id();
  ObAutoincrementService &autoinc_service = share::ObAutoincrementService::get_instance();

  if (0 != column_id) {
    bool is_oracle_mode = false;
    if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check is oracle mode",
                KR(ret), K(table_id), K(table_name), K(truncate_version), K(column_id));
    } else if (is_oracle_mode) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in oracle mode, autoic_column_id must be illegal",
              KR(ret), K(table_id), K(table_name), K(truncate_version), K(column_id));
    } else {
      // reinit auto_increment value
      uint64_t tenant_id = table_schema.get_tenant_id();
      if (OB_FAIL(autoinc_service.reinit_autoinc_row(tenant_id, table_id,
                                                     column_id, truncate_version, trans))) {
        LOG_WARN("failed to reint auto_increment",
                KR(ret), K(tenant_id), K(table_id), K(table_name), K(truncate_version), K(column_id));
      }
    }
  }
  int64_t finish_time = ObTimeUtility::current_time();
  LOG_INFO("finish reinit_auto_row", KR(ret), "cost_ts", finish_time - start_time);
  return ret;
}

int ObDDLOperator::try_reinit_autoinc_row(const ObTableSchema &table_schema,
                                          common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool need_reinit_inner_table = false;
  const uint64_t table_id = table_schema.get_table_id();
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const int64_t truncate_version = table_schema.get_truncate_version();
  const uint64_t column_id = table_schema.get_autoinc_column_id();
  ObAutoincrementService &autoinc_service = share::ObAutoincrementService::get_instance();
  if (OB_FAIL(autoinc_service.try_lock_autoinc_row(tenant_id, table_id, column_id, truncate_version,
                                                    need_reinit_inner_table, trans))) {
    LOG_WARN("fail to check inner autoinc version", KR(ret), K(tenant_id), K(table_id), K(column_id));
  } else if (need_reinit_inner_table) {
    if (OB_FAIL(autoinc_service.reset_autoinc_row(tenant_id, table_id, column_id,
                                                  truncate_version, trans))) {
      LOG_WARN("fail to reinit autoinc row", KR(ret), K(tenant_id), K(table_id), K(column_id));
    }
  }
  return ret;
}

// Notice: this function process index.
int ObDDLOperator::alter_table_drop_aux_column(
    ObTableSchema &new_table_schema,
    const ObColumnSchemaV2 &orig_column_schema,
    common::ObMySQLTransaction &trans,
    const ObTableType table_type)
{
  int ret = OB_SUCCESS;
  //should update the aux table
  const uint64_t tenant_id = orig_column_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  const bool is_index = USER_INDEX == table_type;
  ObSEArray<uint64_t, 16> aux_vp_tid_array; // for VP
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(ERROR, "schema_service must not null");
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    RS_LOG(WARN, "get schema guard failed", K(ret));
  } else if (!is_index
             && OB_FAIL(new_table_schema.get_aux_vp_tid_array(aux_vp_tid_array))) {
    LOG_WARN("get_aux_tid_array failed", K(ret), K(is_index));
  } else if (OB_FAIL(new_table_schema.get_simple_index_infos(
                     simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }

  //update all aux table schema
  int64_t N = is_index ? simple_index_infos.count() : aux_vp_tid_array.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObTableSchema *aux_table_schema = NULL;
    uint64_t tid = is_index ? simple_index_infos.at(i).table_id_ : aux_vp_tid_array.at(i);
    if (OB_FAIL(schema_guard.get_table_schema(
                tenant_id, tid, aux_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(tid));
    } else if (OB_ISNULL(aux_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else if (aux_table_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("aux table is in recyclebin", K(ret));
    } else {
      const ObColumnSchemaV2 *delete_column_schema =
          aux_table_schema->get_column_schema(orig_column_schema.get_column_id());
      if (NULL != delete_column_schema) {
        if (delete_column_schema->is_index_column()) {
          ret = OB_ERR_ALTER_INDEX_COLUMN;
          RS_LOG(WARN, "can't not drop index column", K(ret));
        } else {
          // Notice: when the last VP column is deleted, the VP table should be deleted.
          // If other VP column is hidden, the VP partition should be deleted.
          int64_t normal_column_count = 0;
          for (int64_t i = 0; OB_SUCC(ret) && (normal_column_count < 2) && (i < aux_table_schema->get_column_count()); ++i) {
            if (!aux_table_schema->get_column_schema_by_idx(i)->is_hidden()) {
              ++normal_column_count;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (normal_column_count < 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("normal_column_count is error", K(ret), K(normal_column_count));
          } else if (1 == normal_column_count) {
            // DROP AUX_VERTIAL_PARTITION_TABLE
            ObSEArray<uint64_t, 16> tmp_aux_vp_tid_array;
            if (OB_FAIL(drop_table(*aux_table_schema, trans))) {
              LOG_WARN("drop aux vertial partition table failed", K(ret), K(*aux_table_schema));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && (i < aux_vp_tid_array.count()); ++i) {
                if (aux_vp_tid_array.at(i) == aux_table_schema->get_table_id()) {
                  // skip
                } else if (OB_FAIL(tmp_aux_vp_tid_array.push_back(aux_vp_tid_array.at(i)))) {
                  LOG_WARN("push back to tmp_aux_vp_tid_array failed", K(ret), K(i), K(aux_vp_tid_array.at(i)));
                }
              }
              if (OB_SUCC(ret)) {
                // update aux_vp_tid_array of new_table_schema
                if (OB_FAIL(new_table_schema.set_aux_vp_tid_array(tmp_aux_vp_tid_array))) {
                  LOG_WARN("set aux_vp_tid_array to new_table_schema failed", K(ret), K(tmp_aux_vp_tid_array));
                }
              }
            }
          } else {
            ObTableSchema tmp_aux_table_schema;
            if (OB_FAIL(tmp_aux_table_schema.assign(*aux_table_schema))) {
              LOG_WARN("fail to assign schema", K(ret));
            } else if (OB_FAIL(update_prev_id_for_delete_column(
                *aux_table_schema,
                tmp_aux_table_schema,
                *delete_column_schema,
                trans))) {
              LOG_WARN("failed to update column previous id for delete column", K(ret));
            } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
              LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
            } else if (OB_FAIL(schema_service->get_table_sql_service().delete_single_column(
                new_schema_version,
                trans,
                *aux_table_schema,
                *delete_column_schema))) {
              RS_LOG(WARN, "failed to delete non-aux column!",
                  "table schema", *aux_table_schema, K(ret));
            } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
              LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
            } else if (OB_FAIL(schema_service->get_table_sql_service().sync_aux_schema_version_for_history(
                trans,
                *aux_table_schema,
                new_schema_version
                ))) {
              RS_LOG(WARN, "fail to update aux schema version for update column");
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::update_prev_id_for_delete_column(const ObTableSchema &origin_table_schema,
    ObTableSchema &new_table_schema,
    const ObColumnSchemaV2 &ori_column_schema,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  // When a transaction currently add/drop column: origin_table_schema don't update prev&next column ID, so it need fetch from new table.
  ObColumnSchemaV2 *new_origin_col = new_table_schema.get_column_schema(ori_column_schema.get_column_name());
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(ERROR, "schema_service must not null");
  } else if (OB_ISNULL(new_origin_col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get column from new table schema", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    ObColumnSchemaV2 *next_col = new_table_schema.get_column_schema_by_prev_next_id(new_origin_col->get_next_column_id());
    if (OB_ISNULL(next_col)) {
      // do nothing since local_column is tail column
    } else {
      next_col->set_prev_column_id(new_origin_col->get_prev_column_id());
      next_col->set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service->get_table_sql_service().update_single_column(
          trans,
          origin_table_schema,
          new_table_schema,
          *next_col,
          true /* record_ddl_operation */))) {
        LOG_WARN("Failed to update single column", K(ret), K(next_col->get_column_name_str()));
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_table_foreign_keys(share::schema::ObTableSchema &new_table_schema,
                                             common::ObMySQLTransaction &trans,
                                             bool in_offline_ddl_white_list)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("generate new schema version failed", K(ret));
  } else if (FALSE_IT(new_table_schema.set_schema_version(new_schema_version))) {
  } else if (OB_FAIL(schema_service->get_table_sql_service().update_foreign_key_state(
             trans, new_table_schema))) {
    LOG_WARN("update foreign keys enable option into inner table failed", K(ret));
  } else {
    uint64_t id = OB_INVALID_ID;
    const ObTableSchema *schema = NULL;
    const ObIArray<uint64_t> &table_ids = new_table_schema.get_depend_table_ids();
    for (int64_t i = 0; i < table_ids.count() && OB_SUCC(ret); i++) {
      ObSchemaGetterGuard schema_guard;
      id = table_ids.at(i);
      ObTableSchema tmp_schema;
      if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
        RS_LOG(WARN, "get schema guard failed", K(ret), K(id));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, id, schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(id));
      } else if (!schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is NULL", K(ret));
      } else if (OB_FAIL(tmp_schema.assign(*schema))) {
        LOG_WARN("fail to assign schema", K(ret), KPC(schema));
      } else if (FALSE_IT(tmp_schema.set_in_offline_ddl_white_list(in_offline_ddl_white_list))) {
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_service->get_table_sql_service().sync_schema_version_for_history(
                trans,
                tmp_schema,
                new_schema_version))) {
        RS_LOG(WARN, "fail to sync schema version", K(ret));
      } else {
        ObSchemaOperationType operation_type = OB_DDL_ALTER_TABLE;
        if (OB_FAIL(update_table_attribute(new_table_schema,
                                          trans,
                                          operation_type))) {
          LOG_WARN("failed to update data table schema attribute", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::add_table_foreign_keys(const share::schema::ObTableSchema &orig_table_schema,
                                          share::schema::ObTableSchema &inc_table_schema,
                                          common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("generate new schema version failed", K(ret));
  } else {
    inc_table_schema.set_schema_version(new_schema_version);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_service->get_table_sql_service().add_foreign_key(trans, inc_table_schema, false))) {
      LOG_WARN("insert foreign keys into inner tables failed", K(ret));
    } else if (OB_FAIL(schema_service->get_table_sql_service().update_foreign_key_state(trans, inc_table_schema))) {
      LOG_WARN("update foreign keys enable option into inner table failed", K(ret));
    } else if (OB_FAIL(sync_version_for_cascade_table(tenant_id, inc_table_schema.get_depend_table_ids(), trans))) {
      LOG_WARN("fail to sync cascade depend table", K(ret));
    } else if (OB_FAIL(sync_version_for_cascade_mock_fk_parent_table(orig_table_schema.get_tenant_id(), inc_table_schema.get_depend_mock_fk_parent_table_ids(), trans))) {
      LOG_WARN("fail to sync cascade depend_mock_fk_parent_table_ids table", K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::modify_check_constraints_state(
    const ObTableSchema &orig_table_schema,
    const ObTableSchema &inc_table_schema,
    ObTableSchema &new_table_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  UNUSED(orig_table_schema);
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObTableSchema::const_constraint_iterator iter = inc_table_schema.constraint_begin();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (inc_table_schema.constraint_end() == iter) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table doesn't have a check constraint", K(ret), K(inc_table_schema));
  } else if (inc_table_schema.constraint_end() != iter + 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update check constraint state couldn't be executed with other DDLs", K(ret), K(inc_table_schema));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    (*iter)->set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_table_sql_service().update_check_constraint_state(trans, new_table_schema, **iter))) {
      LOG_WARN("insert single constraint failed", K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::add_table_constraints(const ObTableSchema &inc_table_schema,
                                         ObTableSchema &new_table_schema,
                                         ObMySQLTransaction &trans,
                                         ObSArray<uint64_t> *cst_ids/*NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  }
  for (ObTableSchema::const_constraint_iterator iter = inc_table_schema.constraint_begin(); OB_SUCC(ret) &&
    iter != inc_table_schema.constraint_end(); iter ++) {
    uint64_t new_cst_id = OB_INVALID_ID;
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service->fetch_new_constraint_id(tenant_id, new_cst_id))) {
      LOG_WARN("failed to fetch new constraint id", K(ret));
    } else {
      (*iter)->set_schema_version(new_schema_version);
      (*iter)->set_tenant_id(new_table_schema.get_tenant_id());
      (*iter)->set_table_id(new_table_schema.get_table_id());
      (*iter)->set_constraint_id(new_cst_id);
      (*iter)->set_constraint_type((*iter)->get_constraint_type());
      if (OB_FAIL(schema_service->get_table_sql_service().insert_single_constraint(trans, new_table_schema, **iter))) {
        LOG_WARN("insert single constraint failed", K(ret));
      } else {
        if (OB_NOT_NULL(cst_ids)) {
          OZ(cst_ids->push_back(new_cst_id));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_default_partition_part_idx_for_external_table(const ObTableSchema &orig_table_schema,
                                                     const ObTableSchema &inc_table_schema,
                                                     ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const int64_t update_step = 1024;
  const int64_t part_num = orig_table_schema.get_part_option().get_part_num();
  int64_t max_part_idx = OB_INVALID_INDEX;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_UNLIKELY(!orig_table_schema.is_external_table() || !orig_table_schema.is_partitioned_table())
      || OB_ISNULL(orig_table_schema.get_part_array())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(inc_table_schema.get_max_part_idx(max_part_idx, true/*without_default*/))) {
    LOG_WARN("get max part idx failed", K(ret));
  }
  bool found = false;
  for (int i = 0; OB_SUCC(ret) && !found && i < part_num; i++) {
    ObPartition *default_part = orig_table_schema.get_part_array()[i];
    if (OB_ISNULL(default_part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret));
    } else {
      const ObIArray<common::ObNewRow>* orig_list_value = &(default_part->get_list_row_values());
      if (OB_ISNULL(orig_list_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("list row value is null", K(ret), K(orig_list_value));
      } else if (orig_list_value->count() == 1 && orig_list_value->at(0).get_count() >= 1
          && orig_list_value->at(0).get_cell(0).is_max_value()) {
        if (default_part->get_part_idx() <= max_part_idx) {
          default_part->set_part_idx(max_part_idx + update_step);
          ObTableSchema alter_part_schema;
          if (OB_FAIL(alter_part_schema.add_partition(*default_part))) {
            LOG_WARN("add partition failed", K(ret));
          } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
            LOG_WARN("fail to gen new schema version", KR(ret), K(tenant_id));
          } else if (OB_FAIL(schema_service->get_table_sql_service().rename_inc_part_info(trans,
                                                                                  orig_table_schema,
                                                                                  alter_part_schema,
                                                                                  new_schema_version,
                                                                                  true/*update part idx*/))) {
            LOG_WARN("rename inc part info failed", KR(ret));
          }
        }
        found = true;
      }
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!found)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update default partition part idx failed", K(ret));
  }
  return ret;
}

int ObDDLOperator::add_table_partitions(const ObTableSchema &orig_table_schema,
                                        ObTableSchema &inc_table_schema,
                                        ObTableSchema &new_table_schema,
                                        ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (orig_table_schema.is_external_table()
            && OB_FAIL(update_default_partition_part_idx_for_external_table(orig_table_schema, inc_table_schema, trans))) {
    LOG_WARN("update orig table schema failed", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().add_inc_partition_info(trans,
                                                                                    orig_table_schema,
                                                                                    inc_table_schema,
                                                                                    new_schema_version,
                                                                                    false,
                                                                                    false))) {
    LOG_WARN("add inc part info failed", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    const int64_t part_num = orig_table_schema.get_part_option().get_part_num();
    const int64_t inc_part_num = inc_table_schema.get_partition_num();
    const int64_t all_part_num = part_num + inc_part_num;
    new_table_schema.get_part_option().set_part_num(all_part_num);
    new_table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_table_sql_service()
                           .update_partition_option(trans, new_table_schema))) {
      LOG_WARN("update partition option failed", K(ret), K(part_num), K(inc_part_num));
    }
  }
  return ret;
}

int ObDDLOperator::add_table_subpartitions(const ObTableSchema &orig_table_schema,
                                           ObTableSchema &inc_table_schema,
                                           ObTableSchema &new_table_schema,
                                           ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObArray<ObPartition*> update_part_array;
  //FIXME:should move the related logic to ObDDLService
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));

  } else if (OB_FAIL(get_part_array_from_table(new_table_schema, inc_table_schema, update_part_array))) {
    LOG_WARN("fail to get_part_array_from_table", KR(ret));
  } else if (update_part_array.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update part array count is not more than 0", KR(ret), K(update_part_array.count()));
  } else if (update_part_array.count() != inc_table_schema.get_partition_num()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update_part_array count not equal inc_table part count",
              KR(ret), K(update_part_array.count()), K(inc_table_schema.get_partition_num()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < update_part_array.count(); i++) {
      ObPartition *part = update_part_array.at(i);
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update_part_array[i]", KR(ret), K(i));
      } else if (i >= inc_table_schema.get_partition_num()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update_part_array[i] out of inc_part_array", KR(ret), K(i), K(inc_table_schema.get_partition_num()));
      } else if (OB_ISNULL(inc_table_schema.get_part_array()[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc_table_part_array[i]", KR(ret), K(i));
      } else {
        const int64_t subpart_num = part->get_subpartition_num();
        const int64_t inc_subpart_num = inc_table_schema.get_part_array()[i]->get_subpartition_num();
        part->set_sub_part_num(subpart_num + inc_subpart_num);
        part->set_schema_version(new_schema_version);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_service->get_table_sql_service().add_inc_partition_info(trans,
                                                          orig_table_schema,
                                                          inc_table_schema,
                                                          new_schema_version,
                                                          false,
                                                          true))) {
        LOG_WARN("add inc part info failed", K(ret));
      }
      new_table_schema.set_schema_version(new_schema_version);
      if (FAILEDx(schema_service->get_table_sql_service().update_subpartition_option(trans,
          new_table_schema, update_part_array))) {
        LOG_WARN("update sub partition option failed");
      }
    }
  }
  return ret;
}

int ObDDLOperator::truncate_table(const ObString *ddl_stmt_str,
                                  const share::schema::ObTableSchema &orig_table_schema,
                                  const share::schema::ObTableSchema &new_table_schema,
                                  common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_truncate_table = true;
  bool is_truncate_partition = false;
  uint64_t table_id = new_table_schema.get_table_id();
  uint64_t schema_version = new_table_schema.get_schema_version();
  ObSchemaOperationType operation_type = OB_DDL_TRUNCATE_TABLE;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", KR(ret));
  } else if (new_table_schema.is_partitioned_table()) {
    if (OB_INVALID_VERSION == schema_version) {
      ret  = OB_ERR_UNEXPECTED;
      LOG_WARN("schema version is not legal", KR(ret), K(table_id), K(schema_version));
    } else if (OB_FAIL(schema_service->get_table_sql_service()
                                      .drop_inc_part_info(trans,
                                                          orig_table_schema,
                                                          orig_table_schema,
                                                          schema_version,
                                                          is_truncate_partition,
                                                          is_truncate_table))) {
      LOG_WARN("delete part info failed", KR(ret), K(table_id), K(schema_version));
    } else if (OB_FAIL(schema_service->get_table_sql_service()
                                      .add_inc_part_info(trans,
                                                        orig_table_schema,
                                                        new_table_schema,
                                                        schema_version,
                                                        is_truncate_table))) {
      LOG_WARN("add part info failed", KR(ret), K(table_id), K(schema_version));
    }
  }
  if (FAILEDx(schema_service->get_table_sql_service()
                            .update_table_attribute(trans,
                                                    new_table_schema,
                                                    operation_type,
                                                    false,
                                                    ddl_stmt_str))) {
    LOG_WARN("failed to update table schema attribute", KR(ret), K(table_id), K(schema_version));
  }
  return ret;
}

int ObDDLOperator::update_boundary_schema_version(const uint64_t &tenant_id,
                                                  const uint64_t &boundary_schema_version,
                                                  common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", KR(ret));
  } else {
    ObSchemaOperation schema_operation;
    schema_operation.tenant_id_ = tenant_id;
    schema_operation.op_type_ = OB_DDL_END_SIGN;
    share::schema::ObDDLSqlService ddl_sql_service(*schema_service);

    if (OB_FAIL(ddl_sql_service.log_nop_operation(schema_operation,
                                                  boundary_schema_version,
                                                  NULL,
                                                  trans))) {
      LOG_WARN("log end ddl operation failed", KR(ret), K(tenant_id), K(boundary_schema_version));
    }
  }
  return ret;
}

int ObDDLOperator::truncate_table_partitions(const share::schema::ObTableSchema &orig_table_schema,
                                             share::schema::ObTableSchema &inc_table_schema,
                                             share::schema::ObTableSchema &del_table_schema,
                                             common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", KR(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().truncate_part_info(
                     trans,
                     orig_table_schema,
                     inc_table_schema,
                     del_table_schema,
                     new_schema_version))) {
    LOG_WARN("delete inc part info failed", KR(ret));
  }

  return ret;
}

int ObDDLOperator::truncate_table_subpartitions(const share::schema::ObTableSchema &orig_table_schema,
                                                share::schema::ObTableSchema &inc_table_schema,
                                                share::schema::ObTableSchema &del_table_schema,
                                                common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", KR(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().truncate_subpart_info(
                     trans,
                     orig_table_schema,
                     inc_table_schema,
                     del_table_schema,
                     new_schema_version))) {
    LOG_WARN("delete inc part info failed", KR(ret));
  }

  return ret;
}

int ObDDLOperator::add_table_partitions(const ObTableSchema &orig_table_schema,
                                        ObTableSchema &alter_table_schema,
                                        const int64_t schema_version,
                                        ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(schema_version));
  } else {
    const int64_t inc_part_num = alter_table_schema.get_part_option().get_part_num();
    if (OB_FAIL(schema_service->get_table_sql_service().add_inc_partition_info(trans,
                                                                               orig_table_schema,
                                                                               alter_table_schema,
                                                                               schema_version,
                                                                               false,
                                                                               false))) {
      LOG_WARN("add inc part info failed", K(ret));
    } else {
      const int64_t part_num = orig_table_schema.get_part_option().get_part_num();
      const int64_t all_part_num = part_num + inc_part_num;
      ObTableSchema new_table_schema;
      if (OB_FAIL(new_table_schema.assign(orig_table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else {
        new_table_schema.get_part_option().set_part_num(all_part_num);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_service->get_table_sql_service()
                  .update_partition_option(trans, new_table_schema, schema_version))) {
        LOG_WARN("update partition option failed", K(ret), K(part_num), K(inc_part_num));
      }
    }
  }
  return ret;
}

int ObDDLOperator::get_part_array_from_table(const ObTableSchema &new_table_schema,
                                             const ObTableSchema &inc_table_schema,
                                             ObIArray<ObPartition*> &out_part_array)
{
  int ret = OB_SUCCESS;

  ObPartition **inc_part_array = inc_table_schema.get_part_array();
  const int64_t inc_part_sum = inc_table_schema.get_partition_num();
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_sum; i++) {
    ObPartition *inc_part = inc_part_array[i];
    if (OB_ISNULL(inc_part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inc_part_array[i] is null", KR(ret), K(i));
    } else {
      ObPartition **part_array = new_table_schema.get_part_array();
      const int64_t part_sum = new_table_schema.get_partition_num();
      int64_t j = 0;
      for (j = 0; OB_SUCC(ret) && j < part_sum; j++) {
        ObPartition *part = part_array[j];
        if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part_array[j] is NULL", K(ret), K(j));
        } else if (part->get_part_id() == inc_part->get_part_id()) {
          if (OB_FAIL(out_part_array.push_back(part))) {
            LOG_WARN("push back failed", KR(ret), K(j));
          }
          break;
        }
      }
      if (OB_SUCC(ret) && j >= part_sum) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc_part_array[i] is not in part_array",
                 KR(ret), K(i), KPC(inc_part), K(new_table_schema));
      }
    }
  }
  return ret;
}

bool ObDDLOperator::is_list_values_equal(const common::ObRowkey &fir_values,
                                         const common::ObIArray<common::ObNewRow> &sed_values)
{
  bool equal = false;
  int64_t s_count = sed_values.count();
  common::ObRowkey rowkey;
  for (int64_t j = 0; j < s_count; ++j) {
    rowkey.reset();
    rowkey.assign(sed_values.at(j).cells_, sed_values.at(j).count_);
    if (fir_values == rowkey) {
      equal = true;
      break;
    }
  }
  return equal;
}

int ObDDLOperator::check_part_equal(
    const share::schema::ObPartitionFuncType part_type,
    const share::schema::ObPartition *r_part,
    const share::schema::ObPartition *l_part,
    bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;

  if (OB_ISNULL(r_part) || OB_ISNULL(l_part)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is NULL", K(ret), K(r_part), K(l_part));
  } else if (share::schema::is_range_part(part_type)) {
    if (r_part->get_high_bound_val() == l_part->get_high_bound_val()) {
      is_equal = true;
    }
  } else if (share::schema::is_list_part(part_type)) {
    if (0 == r_part->get_list_row_values().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("list values is empty", K(ret));
    } else {
      common::ObRowkey rowkey;
      rowkey.assign(r_part->get_list_row_values().at(0).cells_,
                    r_part->get_list_row_values().at(0).count_);
      if (is_list_values_equal(rowkey, l_part->get_list_row_values())) {
        is_equal = true;
      }
    }
  }

  return ret;
}

int ObDDLOperator::rename_table_partitions(const ObTableSchema &orig_table_schema,
                                         ObTableSchema &inc_table_schema,
                                         ObTableSchema &new_table_schema,
                                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", KR(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().rename_inc_part_info(trans,
                                                                          orig_table_schema,
                                                                          inc_table_schema,
                                                                          new_schema_version,
                                                                          false))) {
    LOG_WARN("rename inc part info failed", KR(ret));
  }
  return ret;
}

int ObDDLOperator::rename_table_subpartitions(const ObTableSchema &orig_table_schema,
                                         ObTableSchema &inc_table_schema,
                                         ObTableSchema &new_table_schema,
                                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", KR(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().rename_inc_subpart_info(trans,
                                                                          orig_table_schema,
                                                                          inc_table_schema,
                                                                          new_schema_version))) {
    LOG_WARN("rename inc subpart info failed", KR(ret));
  }
  return ret;
}

int ObDDLOperator::drop_table_partitions(const ObTableSchema &orig_table_schema,
                                         ObTableSchema &inc_table_schema,
                                         ObTableSchema &new_table_schema,
                                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_truncate_table = false;
  bool is_truncate_partition = false;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service->get_table_sql_service().drop_inc_part_info(trans,
                                                                         orig_table_schema,
                                                                         inc_table_schema,
                                                                         new_schema_version,
                                                                         is_truncate_partition,
                                                                         is_truncate_table))) {
    LOG_WARN("delete inc part info failed", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    const int64_t part_num = orig_table_schema.get_part_option().get_part_num();
    const int64_t inc_part_num = inc_table_schema.get_partition_num();
    const int64_t all_part_num = part_num - inc_part_num;
    new_table_schema.get_part_option().set_part_num(all_part_num);
    new_table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_table_sql_service()
                            .update_partition_option(trans, new_table_schema))) {
      LOG_WARN("update partition option failed", K(ret), K(part_num), K(inc_part_num));
    } else if (orig_table_schema.is_external_table()) {
      if (inc_part_num > 0) {
        CK (OB_NOT_NULL(inc_table_schema.get_part_array()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++)  {
        if (OB_ISNULL(inc_table_schema.get_part_array()[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", K(ret));
        } else {
          OZ (ObExternalTableFileManager::get_instance().clear_inner_table_files_within_one_part(tenant_id,
                                                                                 orig_table_schema.get_table_id(),
                                                                                 inc_table_schema.get_part_array()[i]->get_part_id(),
                                                                                 trans));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_table_subpartitions(const ObTableSchema &orig_table_schema,
                                            ObTableSchema &inc_table_schema,
                                            ObTableSchema &new_table_schema,
                                            ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().drop_inc_subpart_info(trans,
                                                                         orig_table_schema,
                                                                         inc_table_schema,
                                                                         new_schema_version))) {
    LOG_WARN("delete inc part info failed", K(ret));
  } else {
    //FIXME:should move the related logic to ObDDLService
    ObArray<ObPartition*> update_part_array;
    if (OB_FAIL(get_part_array_from_table(new_table_schema, inc_table_schema, update_part_array))) {
      LOG_WARN("fail to get part array from tableschema", KR(ret));
    } else if (update_part_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update_part_array count less than 0", K(ret), K(update_part_array.count()));
    } else if (update_part_array.count() != inc_table_schema.get_partition_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update_part_array count not equal inc_table part count",
                KR(ret), K(update_part_array.count()), K(inc_table_schema.get_partition_num()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < update_part_array.count(); i++) {
        ObPartition *part = update_part_array.at(i);
        if (i >= inc_table_schema.get_partition_num()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("update_part_array[i] out of inc_part_array", KR(ret), K(i), K(inc_table_schema.get_partition_num()));
        } else if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("update_part_array[i]", KR(ret), K(i));
        } else if (OB_ISNULL(inc_table_schema.get_part_array()[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inc_table_part_array[i]", KR(ret), K(i));
        } else {
          const int64_t subpart_num = part->get_sub_part_num();
          const int64_t inc_subpart_num = inc_table_schema.get_part_array()[i]->get_subpartition_num();
          part->set_sub_part_num(subpart_num - inc_subpart_num);
          part->set_schema_version(new_schema_version);
        }
      }
      if (OB_SUCC(ret)) {
        new_table_schema.set_schema_version(new_schema_version);
        if (OB_FAIL(schema_service->get_table_sql_service().update_subpartition_option(trans,
            new_table_schema, update_part_array))) {
          LOG_WARN("update sub partition option failed");
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_table_constraints(const ObTableSchema &orig_table_schema,
                                          const ObTableSchema &inc_table_schema,
                                          ObTableSchema &new_table_schema,
                                          ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  UNUSED(orig_table_schema);

  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else {
    for (ObTableSchema::const_constraint_iterator iter = inc_table_schema.constraint_begin(); OB_SUCC(ret) &&
      iter != inc_table_schema.constraint_end(); iter ++) {
      (*iter)->set_tenant_id(orig_table_schema.get_tenant_id());
      (*iter)->set_table_id(orig_table_schema.get_table_id());
      if (nullptr == new_table_schema.get_constraint((*iter)->get_constraint_id())) {
        LOG_INFO("constraint has already been dropped", K(ret), K(**iter));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_service->get_table_sql_service().delete_single_constraint(
                           new_schema_version, trans, new_table_schema, **iter))) {
        RS_LOG(WARN, "failed to delete constraint", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_table_partitions(const ObTableSchema &orig_table_schema,
                                         ObTableSchema &inc_table_schema,
                                         const int64_t schema_version,
                                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  bool is_truncate_table = false;
  bool is_truncate_partition = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(schema_version));
  } else if (OB_FAIL(schema_service->get_table_sql_service().drop_inc_part_info(
                     trans,
                     orig_table_schema,
                     inc_table_schema,
                     schema_version,
                     is_truncate_partition,
                     is_truncate_table))) {
    LOG_WARN("delete inc part info failed", K(ret));
  } else {
    ObTableSchema new_table_schema;
    if (OB_FAIL(new_table_schema.assign(orig_table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else {
      const int64_t part_num = orig_table_schema.get_part_option().get_part_num();
      const int64_t inc_part_num = inc_table_schema.get_part_option().get_part_num();
      const int64_t all_part_num = part_num - inc_part_num;
      new_table_schema.get_part_option().set_part_num(all_part_num);
      if (OB_FAIL(schema_service->get_table_sql_service()
                              .update_partition_option(trans, new_table_schema, schema_version))) {
        LOG_WARN("update partition option failed", K(ret), K(part_num), K(inc_part_num));
      }
    }
  }
  return ret;
}

int ObDDLOperator::insert_column_groups(ObMySQLTransaction &trans, const ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().add_column_groups(trans, new_table_schema, new_schema_version))) {
    LOG_WARN("insert alter column group failed", K(ret), K(new_table_schema));
  }
  return ret;
}

int ObDDLOperator::insert_column_ids_into_column_group(ObMySQLTransaction &trans,
                                                      const ObTableSchema &new_table_schema,
                                                      const ObIArray<uint64_t> &column_ids,
                                                      const ObColumnGroupSchema &column_group)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().insert_column_ids_into_column_group(trans, new_table_schema, new_schema_version, column_ids, column_group))) {
    LOG_WARN("insert alter column group failed", K(ret), K(new_table_schema), K(column_group));
  }
  return ret;
}


int ObDDLOperator::insert_single_column(ObMySQLTransaction &trans,
                                        const ObTableSchema &new_table_schema,
                                        ObColumnSchemaV2 &new_column)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (FALSE_IT(new_column.set_schema_version(new_schema_version))) {
    //do nothing
  } else if (OB_FAIL(schema_service->get_table_sql_service().insert_single_column(
             trans, new_table_schema, new_column, true))) {
    LOG_WARN("insert single column failed", K(ret));
  }
  return ret;
}

int ObDDLOperator::delete_single_column(ObMySQLTransaction &trans,
                                        ObTableSchema &new_table_schema,
                                        const ObString &column_name)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObColumnSchemaV2 *orig_column = NULL;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_ISNULL(orig_column = new_table_schema.get_column_schema(column_name))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get column schema from table failed", K(column_name));
  } else if (OB_FAIL(new_table_schema.delete_column(column_name))) {
    //drop column will do some check on the new table schema
    RS_LOG(WARN, "failed to drop column schema", K(ret), K(column_name));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().delete_single_column(
      new_schema_version, trans, new_table_schema, *orig_column))) {
    RS_LOG(WARN, "failed to delete column", K(orig_column), K(ret));
  }
  return ret;
}

int ObDDLOperator::alter_table_create_index(const ObTableSchema &new_table_schema,
                                            ObIArray<ObColumnSchemaV2*> &gen_columns,
                                            ObTableSchema &index_schema,
                                            common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else {
    uint64_t index_table_id = OB_INVALID_ID;
    //index schema can't not create with specified table id
    if (OB_UNLIKELY(index_schema.get_table_id() != OB_INVALID_ID)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_id of index should be invalid", K(ret), K(index_schema.get_table_id()));
    } else if (OB_FAIL(schema_service->fetch_new_table_id(
            new_table_schema.get_tenant_id(), index_table_id))) {
      RS_LOG(WARN, "failed to update_max_used_table_id, ", K(ret));
    } else {
      index_schema.set_table_id(index_table_id);
    }
    if (OB_SUCC(ret)) {
      if (gen_columns.empty()) {
        //create normal index table.
        if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else {
          index_schema.set_schema_version(new_schema_version);
          if (OB_FAIL(schema_service->get_table_sql_service().create_table(index_schema, trans))) {
            RS_LOG(WARN, "alter table create index failed", K(index_schema), K(ret));
          }
        }
      } else {
        // First increase internal generated column, and then create an index on the column
        for (int64_t i = 0; OB_SUCC(ret) && i < gen_columns.count(); ++i) {
          ObColumnSchemaV2 *new_column_schema = gen_columns.at(i);
          if (OB_ISNULL(new_column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new column schema is null");
          } else if (OB_FAIL(insert_single_column(trans, new_table_schema, *new_column_schema))) {
            LOG_WARN("failed to create table schema, ", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else {
            index_schema.set_schema_version(new_schema_version);
            if (OB_FAIL(schema_service->get_table_sql_service().create_table(index_schema, trans))) {
              LOG_WARN("failed to create index schema", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::alter_table_drop_index(
    const ObTableSchema *index_table_schema,
    ObTableSchema &new_data_table_schema,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  //drop inner generated index column
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(index_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(index_table_schema));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(index_table_schema->get_tenant_id(), schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(drop_inner_generated_index_column(trans, schema_guard, *index_table_schema, new_data_table_schema))) {
    LOG_WARN("drop inner generated index column failed", K(ret));
  } else if (OB_FAIL(drop_table(*index_table_schema, trans))) {  // drop index table
    RS_LOG(WARN, "ddl_operator drop_table failed",
        "table schema", *index_table_schema, K(ret));
  }
  if (OB_SUCC(ret)) {
    RS_LOG(INFO, "finish drop index", K(*index_table_schema), K(ret));
  }
  return ret;
}

int ObDDLOperator::alter_table_alter_index(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const uint64_t database_id,
    const ObAlterIndexArg &alter_index_arg,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const uint64_t tenant_id = alter_index_arg.tenant_id_;
    int64_t new_schema_version = OB_INVALID_VERSION;
    RS_LOG(INFO, "start alter table alter index", K(alter_index_arg));
    const ObTableSchema *index_table_schema = NULL;
    ObString index_table_name;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const ObString &index_name = alter_index_arg.index_name_;

    //build index name and get index schema
    if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                      data_table_id,
                                                      index_name,
                                                      index_table_name))) {
      RS_LOG(WARN, "build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
    } else {
      const bool is_index = true;
      ObTableSchema new_index_table_schema;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                database_id,
                                                index_table_name,
                                                is_index,
                                                index_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(index_table_schema));
      } else if (OB_UNLIKELY(NULL == index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "get index table schema failed", K(tenant_id),
               K(database_id), K(index_table_name), K(ret));
      } else if (index_table_schema->is_in_recyclebin()) {
        ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
        RS_LOG(WARN, "index table is in recyclebin", K(ret));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(new_index_table_schema.assign(*index_table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else {
        new_index_table_schema.set_index_visibility(alter_index_arg.index_visibility_);
        new_index_table_schema.set_schema_version(new_schema_version);
        if(OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                    trans,
                    *index_table_schema,
                    new_index_table_schema,
                    index_table_schema->is_global_index_table() ? OB_DDL_ALTER_GLOBAL_INDEX: OB_DDL_ALTER_TABLE))) {
          RS_LOG(WARN, "schema service update_table_options failed",
                 K(*index_table_schema), K(ret));
        }
      }
    }
    RS_LOG(INFO, "finish alter table alter index", K(alter_index_arg), K(ret));
  }
  return ret;
}

// description: delete foreign key of table in a transaction
//
// @param [in] table_schema
// @param [in] drop_foreign_key_arg
// @param [in] trans
//
// @return oceanbase error code defined in lib/ob_errno.def
int ObDDLOperator::alter_table_drop_foreign_key(const ObTableSchema &table_schema,
                                                const ObDropForeignKeyArg &drop_foreign_key_arg,
                                                ObMySQLTransaction &trans,
                                                const ObForeignKeyInfo *&parent_table_mock_foreign_key_info,
                                                const bool parent_table_in_offline_ddl_white_list)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  ObTableSqlService *table_sql_service = NULL;
  const ObString &foreign_key_name = drop_foreign_key_arg.foreign_key_name_;
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (FALSE_IT(table_sql_service = &schema_service_impl->get_table_sql_service())) {
  } else {
    const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema.get_foreign_key_infos();
    const ObForeignKeyInfo *foreign_key_info = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
      if (0 == foreign_key_name.case_compare(foreign_key_infos.at(i).foreign_key_name_)
          && table_schema.get_table_id() == foreign_key_infos.at(i).child_table_id_) {
        foreign_key_info = &foreign_key_infos.at(i);
        break;
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(foreign_key_info)) {
      bool is_oracle_mode = false;
      if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
         table_schema.get_tenant_id(), table_schema.get_table_id(), is_oracle_mode))) {
       LOG_WARN("fail to check is oracle mode", K(ret), K(table_schema));
      } else if (is_oracle_mode) {
       ret = OB_ERR_NONEXISTENT_CONSTRAINT;
       LOG_WARN("Cannot drop foreign key constraint  - nonexistent constraint", K(ret), K(foreign_key_name), K(table_schema.get_table_name_str()));
      } else {
       ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
       LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, foreign_key_name.length(), foreign_key_name.ptr());
       LOG_WARN("Cannot drop foreign key constraint  - nonexistent constraint", K(ret), K(foreign_key_name), K(table_schema.get_table_name_str()));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(table_sql_service->drop_foreign_key(
                       new_schema_version, trans, table_schema, foreign_key_info, parent_table_in_offline_ddl_white_list))) {
      LOG_WARN("failed to drop foreign key", K(ret), K(foreign_key_name));
    } else if (foreign_key_info->is_parent_table_mock_) {
      parent_table_mock_foreign_key_info = foreign_key_info;
    }
  }
  return ret;
}

int ObDDLOperator::create_mock_fk_parent_table(
    ObMySQLTransaction &trans,
    const share::schema::ObMockFKParentTableSchema &mock_fk_parent_table_schema,
    const bool need_update_foreign_key)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_FAIL(schema_service_impl->get_table_sql_service().add_mock_fk_parent_table(
              &trans, mock_fk_parent_table_schema, need_update_foreign_key))) {
    LOG_WARN("insert mock_fk_parent_table failed", K(ret), K(mock_fk_parent_table_schema));
  } else if (need_update_foreign_key) { // if need_update_foreign_key, then need_sync_version_for_cascade_child_table
    ObArray<uint64_t> child_table_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < mock_fk_parent_table_schema.get_foreign_key_infos().count(); ++i) {
      if (OB_FAIL(child_table_ids.push_back(mock_fk_parent_table_schema.get_foreign_key_infos().at(i).child_table_id_))) {
        LOG_WARN("fail to push back child_table_id", K(ret), K(mock_fk_parent_table_schema.get_foreign_key_infos().at(i)));
      }
    }
    if (FAILEDx(sync_version_for_cascade_table(mock_fk_parent_table_schema.get_tenant_id(), child_table_ids, trans))) {
      LOG_WARN("fail to sync versin for children tables", K(ret), K(child_table_ids));
    }
  }
  return ret;
}

int ObDDLOperator::alter_mock_fk_parent_table(
    ObMySQLTransaction &trans,
    share::schema::ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_FAIL(schema_service_impl->get_table_sql_service().alter_mock_fk_parent_table(
              &trans, mock_fk_parent_table_schema))) {
    LOG_WARN("alter mock_fk_parent_table failed", K(ret), K(mock_fk_parent_table_schema));
  }
  return ret;
}

int ObDDLOperator::drop_mock_fk_parent_table(
    ObMySQLTransaction &trans,
    const share::schema::ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_FAIL(schema_service_impl->get_table_sql_service().drop_mock_fk_parent_table(
              &trans, mock_fk_parent_table_schema))) {
    LOG_WARN("drop mock_fk_parent_table failed", K(ret), K(mock_fk_parent_table_schema));
  }
  return ret;
}

int ObDDLOperator::replace_mock_fk_parent_table(
    ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const ObMockFKParentTableSchema *ori_mock_fk_parent_table_schema_ptr = NULL;
  if (OB_FAIL(schema_guard.get_mock_fk_parent_table_schema_with_id(
      mock_fk_parent_table_schema.get_tenant_id(), mock_fk_parent_table_schema.get_mock_fk_parent_table_id(),
      ori_mock_fk_parent_table_schema_ptr))) {
    LOG_WARN("check_mock_fk_parent_table_exist_by_id failed", K(ret), K(mock_fk_parent_table_schema.get_tenant_id()), K(mock_fk_parent_table_schema));
  } else if (OB_ISNULL(ori_mock_fk_parent_table_schema_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ori_mock_fk_parent_table_schema_ptr is null", K(ret), KPC(ori_mock_fk_parent_table_schema_ptr), K(mock_fk_parent_table_schema));
  } else if (mock_fk_parent_table_schema.get_foreign_key_infos().count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of foreign_key_infos in mock_fk_parent_table_schema is zero", K(ret), KPC(ori_mock_fk_parent_table_schema_ptr), K(mock_fk_parent_table_schema.get_foreign_key_infos().count()));
  } else if (OB_FAIL(schema_service_impl->get_table_sql_service().replace_mock_fk_parent_table(
                     &trans, mock_fk_parent_table_schema, ori_mock_fk_parent_table_schema_ptr))) {
    LOG_WARN("replace mock_fk_parent_table failed", K(ret), KPC(ori_mock_fk_parent_table_schema_ptr), K(mock_fk_parent_table_schema));
  } else { // update schema version of child tables and new parent table after replace mock_fk_parent_table with new parent table
    ObArray<uint64_t> child_table_ids;
    uint64_t new_parent_table_id = mock_fk_parent_table_schema.get_foreign_key_infos().at(0).parent_table_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < mock_fk_parent_table_schema.get_foreign_key_infos().count(); ++i) {
      if (OB_FAIL(child_table_ids.push_back(mock_fk_parent_table_schema.get_foreign_key_infos().at(i).child_table_id_))) {
        LOG_WARN("fail to push back child_table_id", K(ret), K(mock_fk_parent_table_schema.get_foreign_key_infos().at(i)));
      }
    }
    if (FAILEDx(sync_version_for_cascade_table(mock_fk_parent_table_schema.get_tenant_id(), child_table_ids, trans))) {
      LOG_WARN("fail to sync versin for children tables", K(ret), K(child_table_ids));
    }
    if (FAILEDx(schema_service_impl->get_table_sql_service().update_data_table_schema_version(trans, mock_fk_parent_table_schema.get_tenant_id(), new_parent_table_id, false))) {
      LOG_WARN("failed to update parent table schema version", K(ret), K(mock_fk_parent_table_schema.get_foreign_key_infos().at(0)));
    }
  }
  return ret;
}

int ObDDLOperator::sync_version_for_cascade_mock_fk_parent_table(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &table_ids,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t id = OB_INVALID_ID;
  const ObMockFKParentTableSchema *schema = NULL;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(ERROR, "schema_service must not null");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count() ; ++i) {
      ObSchemaGetterGuard schema_guard;
      id = table_ids.at(i);
      ObMockFKParentTableSchema tmp_schema;
      if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
        RS_LOG(WARN, "get schema guard failed", K(ret), K(tenant_id), K(id));
      } else if (OB_FAIL(schema_guard.get_mock_fk_parent_table_schema_with_id(tenant_id, id, schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(id));
      } else if (!schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is NULL", K(ret));
      } else if (OB_FAIL(tmp_schema.assign(*schema))) {
        LOG_WARN("fail to assign schema", K(ret), KPC(schema));
      } else if (OB_FAIL(schema_service->get_table_sql_service().update_mock_fk_parent_table_schema_version(
              &trans,
              tmp_schema))) {
        RS_LOG(WARN, "fail to sync schema version", K(ret), K(tmp_schema));
      }
    }
  }

  return ret;
}

int ObDDLOperator::deal_with_mock_fk_parent_table(
    ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(schema_service_.gen_new_schema_version(mock_fk_parent_table_schema.get_tenant_id(), new_schema_version))) {
    LOG_WARN("fail to gen new schema version", K(ret), K(mock_fk_parent_table_schema.get_tenant_id()));
  } else if (FALSE_IT(mock_fk_parent_table_schema.set_schema_version(new_schema_version))) {
  } else if (MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_DROP_PARENT_TABLE == mock_fk_parent_table_schema.get_operation_type()) {
    // One scenes :
    // 1. dropped real parent table
    if (OB_FAIL(create_mock_fk_parent_table(trans, mock_fk_parent_table_schema, true))) {
      LOG_WARN("create mock_fk_parent_table failed", K(ret), K(mock_fk_parent_table_schema));
    }
  } else if (MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_ADD_FK_IN_CHILD_TBALE == mock_fk_parent_table_schema.get_operation_type()) {
    // Two scenes :
    // 1. create child table with a fk references a mock fk parent table
    // 2. alter child table add fk references a mock fk parent table
    if (OB_FAIL(create_mock_fk_parent_table(trans, mock_fk_parent_table_schema, false))) {
      LOG_WARN("create mock_fk_parent_table failed", K(ret), K(mock_fk_parent_table_schema));
    }
  } else if (MOCK_FK_PARENT_TABLE_OP_DROP_TABLE == mock_fk_parent_table_schema.get_operation_type()) {
    // Three scenes :
    // 1. drop child table with a fk references a mock fk parent table existed
    // 2. drop fk from a child table with a fk references a mock fk parent table existed
    // 3. drop database
    if (OB_FAIL(drop_mock_fk_parent_table(trans, mock_fk_parent_table_schema))) {
      LOG_WARN("drop mock_fk_parent_table failed", K(ret), K(mock_fk_parent_table_schema.get_operation_type()), K(mock_fk_parent_table_schema));
    }
  } else if (MOCK_FK_PARENT_TABLE_OP_ADD_COLUMN == mock_fk_parent_table_schema.get_operation_type()
             || MOCK_FK_PARENT_TABLE_OP_DROP_COLUMN == mock_fk_parent_table_schema.get_operation_type()
             || MOCK_FK_PARENT_TABLE_OP_UPDATE_SCHEMA_VERSION == mock_fk_parent_table_schema.get_operation_type()) {
    // Three scenes :
    // 1. create child table with a fk references a mock fk parent table existed
    // 2. alter child table add fk references a mock fk parent table existed
    // 3. drop fk from a child table with a fk references a mock fk parent table existed
    if (OB_FAIL(alter_mock_fk_parent_table(trans, mock_fk_parent_table_schema))) {
      LOG_WARN("alter mock_fk_parent_table failed", K(ret), K(mock_fk_parent_table_schema.get_operation_type()), K(mock_fk_parent_table_schema));
    }
  } else if (MOCK_FK_PARENT_TABLE_OP_REPLACED_BY_REAL_PREANT_TABLE == mock_fk_parent_table_schema.get_operation_type()) {
    // Five scenes :
    // 1. create table (as select)
    // 2. create table like
    // 3. rename table
    // 4. alter table rename to
    // 5. flashback table to before drop
    if (OB_FAIL(replace_mock_fk_parent_table(trans, schema_guard, mock_fk_parent_table_schema))) {
      LOG_WARN("replace mock_fk_parent_table failed", K(ret), K(mock_fk_parent_table_schema.get_operation_type()), K(mock_fk_parent_table_schema));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation_type is INVALID", K(ret), K(mock_fk_parent_table_schema.get_operation_type()), K(mock_fk_parent_table_schema), K(lbt()));
  }
  return ret;
}

int ObDDLOperator::deal_with_mock_fk_parent_tables(
    ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObIArray<ObMockFKParentTableSchema> &mock_fk_parent_table_schema_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < mock_fk_parent_table_schema_array.count(); ++i) {
    if (OB_FAIL(deal_with_mock_fk_parent_table(trans, schema_guard, mock_fk_parent_table_schema_array.at(i)))) {
      LOG_WARN("deal_with_mock_fk_parent_tables failed", K(ret), K(mock_fk_parent_table_schema_array.at(i)));
    }
  }
  return ret;
}

int ObDDLOperator::alter_index_drop_options(const ObTableSchema &index_table_schema,
                                            const ObString &table_name,
                                            ObTableSchema &new_index_table_schema,
                                            ObMySQLTransaction &trans) {
  int ret = OB_SUCCESS;
  const int INVISIBLE = 1;
  const uint64_t DROPINDEX = 1;
  const uint64_t tenant_id = index_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (!index_table_schema.is_index_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index_table_schema is not index", K(ret));
  } else if (OB_FAIL(new_index_table_schema.assign(index_table_schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else {
    uint64_t INVISIBLEBEFORE = 0;
    if (!new_index_table_schema.is_index_visible()) {
      INVISIBLEBEFORE = 1;
    } else {
      INVISIBLEBEFORE = 0;
      new_index_table_schema.set_index_visibility(INVISIBLE);
    }
    new_index_table_schema.set_invisible_before(INVISIBLEBEFORE);
    new_index_table_schema.set_drop_index(DROPINDEX);

    ObSqlString sql;
    ObString index_name;
    if (OB_FAIL(ObTableSchema::get_index_name(allocator,
            index_table_schema.get_data_table_id(),
            index_table_schema.get_table_name_str(),
            index_name))) {
      LOG_WARN("failed to build index table name", K(ret));
    } else if (OB_FAIL(sql.append_fmt("DROP INDEX %.*s on %.*s",
            index_name.length(),
            index_name.ptr(),
            table_name.length(),
            table_name.ptr()))) {
      LOG_WARN("failed to append sql", K(ret));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else {
      ObString ddl_stmt_str = sql.string();
      new_index_table_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
              trans,
              index_table_schema,
              new_index_table_schema,
              OB_DDL_DROP_INDEX_TO_RECYCLEBIN,
              &ddl_stmt_str))) {
        RS_LOG(WARN, "schema service update_table_optinos failed", K(index_table_schema), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::alter_table_rename_index(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const uint64_t database_id,
    const obrpc::ObRenameIndexArg &rename_index_arg,
    const ObIndexStatus *new_index_status,
    common::ObMySQLTransaction &trans,
    schema::ObTableSchema &new_index_table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    RS_LOG(INFO, "start alter table rename index", K(rename_index_arg));
    const ObTableSchema *index_table_schema = NULL;
    ObString index_table_name;
    ObString new_index_table_name;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const ObString &index_name = rename_index_arg.origin_index_name_;
    const ObString &new_index_name = rename_index_arg.new_index_name_;

    if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                      data_table_id,
                                                      index_name,
                                                      index_table_name))) {
      RS_LOG(WARN, "build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
    } else if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                      data_table_id,
                                                      new_index_name,
                                                      new_index_table_name))) {
      RS_LOG(WARN, "build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
    } else {
      const bool is_index = true;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                database_id,
                                                index_table_name,
                                                is_index,
                                                index_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(index_table_schema));
      } else if (OB_FAIL(inner_alter_table_rename_index_(tenant_id, index_table_schema, new_index_table_name,
              new_index_status, trans, new_index_table_schema))) {
        LOG_WARN("fail to alter table rename index", K(ret), K(tenant_id), KPC(index_table_schema),
            K(new_index_table_name));
      } else if (is_fts_index_aux(index_table_schema->get_index_type())) {
        if (OB_FAIL(alter_table_rename_built_in_fts_index_(tenant_id,
                                                           data_table_id,
                                                           database_id,
                                                           index_name,
                                                           new_index_name,
                                                           new_index_status,
                                                           schema_guard,
                                                           trans,
                                                           allocator))) {
          LOG_WARN("failed to rename built in fts index", K(ret), K(tenant_id),
              K(data_table_id), K(database_id), K(index_name), K(new_index_name));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::alter_table_rename_built_in_fts_index_(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const uint64_t database_id,
    const ObString &index_name,
    const ObString &new_index_name,
    const ObIndexStatus *new_index_status,
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObMySQLTransaction &trans,
    ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  SMART_VARS_3((ObTableSchema, new_fts_doc_word_schema),
               (obrpc::ObCreateIndexArg, origin_index_arg),
               (obrpc::ObCreateIndexArg, new_index_arg)) {
    const ObTableSchema *origin_fts_doc_word_schema = NULL;
    origin_index_arg.index_name_ = index_name;
    origin_index_arg.index_type_ = INDEX_TYPE_FTS_DOC_WORD_LOCAL;
    new_index_arg.index_name_ = new_index_name;
    new_index_arg.index_type_ = INDEX_TYPE_FTS_DOC_WORD_LOCAL;
    ObString origin_fts_doc_word_index_table_name;
    ObString new_fts_doc_word_index_table_name;
    if (OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(origin_index_arg, &allocator))) {
      LOG_WARN("failed to generate origin fts doc word name", K(ret));
    } else if (OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(new_index_arg, &allocator))) {
      LOG_WARN("failed to generate new fts doc word name", K(ret));
    } else if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                             data_table_id,
                                                             origin_index_arg.index_name_,
                                                             origin_fts_doc_word_index_table_name))) {
      LOG_WARN("failed to build origin fts doc word table name", K(ret),
          K(data_table_id), K(origin_index_arg.index_name_));
    } else if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                             data_table_id,
                                                             new_index_arg.index_name_,
                                                             new_fts_doc_word_index_table_name))) {
      LOG_WARN("failed to build new fts doc word table name", K(ret),
          K(data_table_id), K(new_index_arg.index_name_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                     database_id,
                                                     origin_fts_doc_word_index_table_name,
                                                     true/*is_index*/,
                                                     origin_fts_doc_word_schema,
                                                     false/*is_hidden*/,
                                                     true/*is_built_in_index*/))) {
      LOG_WARN("failed to get origin fts_doc_word schema", K(ret));
    } else if (OB_FAIL(inner_alter_table_rename_index_(tenant_id,
                                                       origin_fts_doc_word_schema,
                                                       new_fts_doc_word_index_table_name,
                                                       new_index_status,
                                                       trans,
                                                       new_fts_doc_word_schema))) {
      LOG_WARN("fail to alter table rename fts doc word index",
          K(ret), K(tenant_id), KPC(origin_fts_doc_word_schema),
          K(new_fts_doc_word_schema));
    }
  }
  return ret;
}

int ObDDLOperator::alter_table_rename_index_with_origin_index_name(
    const uint64_t tenant_id,
    const uint64_t index_table_id,
    const ObString &new_index_name, // Attention!!! origin index name, don't use table name. For example, __idx_500005_{index_name}, please using index_name!!!
    const ObIndexStatus &new_index_status,
    common::ObMySQLTransaction &trans,
    share::schema::ObTableSchema &new_index_table_schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObString new_index_table_name;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_table_schema = nullptr;
  RS_LOG(INFO, "start alter table rename index", K(tenant_id), K(index_table_id), K(new_index_name));
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == index_table_id || new_index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(index_table_id), K(new_index_name));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(index_table_id));
  } else if (OB_ISNULL(index_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unecpected error, index table schema is nullptr", K(ret), K(index_table_id));
  } else if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                           index_table_schema->get_data_table_id(),
                                                           new_index_name,
                                                           new_index_table_name))) {
    LOG_WARN("fail to build new index name", K(ret), K(new_index_name), KPC(index_table_schema));
  } else if (OB_FAIL(inner_alter_table_rename_index_(tenant_id, index_table_schema, new_index_table_name, &new_index_status,
          trans, new_index_table_schema))) {
    LOG_WARN("fail to alter table rename index", K(ret), K(tenant_id), KPC(index_table_schema),
        K(new_index_table_name), K(new_index_status));
  }
  return ret;
}

int ObDDLOperator::inner_alter_table_rename_index_(
    const uint64_t tenant_id,
    const share::schema::ObTableSchema *index_table_schema,
    const ObString &new_index_name,
    const ObIndexStatus *new_index_status,
    common::ObMySQLTransaction &trans,
    share::schema::ObTableSchema &new_index_table_schema)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_ISNULL(index_table_schema)
          || OB_UNLIKELY(new_index_name.empty())
          || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(index_table_schema), KP(new_index_status), K(new_index_name),
        K(tenant_id));
  } else if (index_table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("index table is in recyclebin", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(new_index_table_schema.assign(*index_table_schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else {
    new_index_table_schema.set_schema_version(new_schema_version);
    if (nullptr != new_index_status) {
      new_index_table_schema.set_index_status(*new_index_status);
    }
    new_index_table_schema.set_name_generated_type(GENERATED_TYPE_USER);
    if (OB_FAIL(new_index_table_schema.set_table_name(new_index_name))) {
      RS_LOG(WARN, "failed to set new table name!", K(new_index_table_schema), K(ret));
    } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                trans,
                *index_table_schema,
                new_index_table_schema,
                index_table_schema->is_global_index_table() ? OB_DDL_RENAME_GLOBAL_INDEX: OB_DDL_RENAME_INDEX))) {
      RS_LOG(WARN, "schema service update_table_options failed", K(*index_table_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::alter_index_table_parallel(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const uint64_t database_id,
    const obrpc::ObAlterIndexParallelArg &alter_parallel_arg,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    int64_t new_schema_version = OB_INVALID_VERSION;
    RS_LOG(INFO, "start alter table alter index parallel", K(alter_parallel_arg));
    const ObTableSchema *index_table_schema = NULL;
    ObString index_table_name;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const ObString &index_name = alter_parallel_arg.index_name_;

    if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                      data_table_id,
                                                      index_name,
                                                      index_table_name))) {
      RS_LOG(WARN, "build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
    } else {
      const bool is_index = true;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                database_id,
                                                index_table_name,
                                                is_index,
                                                index_table_schema))) {
        LOG_WARN("fail to get table schema",
          K(ret), K(tenant_id), K(database_id), K(index_table_schema));
      } else if (OB_UNLIKELY(NULL == index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "get index table schema failed",
          K(tenant_id), K(database_id), K(index_table_name), K(ret));
      } else if (index_table_schema->is_in_recyclebin()) {
        ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
        LOG_WARN("index table is in recyclebin", K(ret));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else {
        ObTableSchema new_index_table_schema;
        if (OB_FAIL(new_index_table_schema.assign(*index_table_schema))) {
          LOG_WARN("fail to assign schema", K(ret));
        } else {
          new_index_table_schema.set_schema_version(new_schema_version);
        }
        if (OB_SUCC(ret)) {
          new_index_table_schema.set_dop(alter_parallel_arg.new_parallel_);
          if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                    trans,
                    *index_table_schema,
                    new_index_table_schema,
                    OB_DDL_ALTER_INDEX_PARALLEL))) {
            RS_LOG(WARN, "schema service update_table_options failed",
              K(*index_table_schema), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::alter_index_table_tablespace(const uint64_t data_table_id,
                                                const uint64_t database_id,
                                                const obrpc::ObAlterIndexTablespaceArg &alter_tablespace_arg,
                                                ObSchemaGetterGuard &schema_guard,
                                                common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else {
    const uint64_t tenant_id = alter_tablespace_arg.tenant_id_;
    int64_t new_schema_version = OB_INVALID_VERSION;
    const ObTableSchema *index_table_schema = NULL;
    ObString index_table_name;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const ObString &index_name = alter_tablespace_arg.index_name_;

    if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                      data_table_id,
                                                      index_name,
                                                      index_table_name))) {
      RS_LOG(WARN, "build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                database_id,
                                                index_table_name,
                                                true,
                                                index_table_schema))) {
      LOG_WARN("fail to get table schema",
        K(ret), K(tenant_id), K(database_id), K(index_table_schema));
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "get index table schema failed",
        K(tenant_id), K(database_id), K(index_table_name), K(ret));
    } else if (index_table_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("index table is in recyclebin", K(ret));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else {
      ObTableSchema new_index_table_schema;
      if (OB_FAIL(new_index_table_schema.assign(*index_table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else {
        new_index_table_schema.set_schema_version(new_schema_version);
        new_index_table_schema.set_tablespace_id(alter_tablespace_arg.tablespace_id_);
        new_index_table_schema.set_encryption_str(alter_tablespace_arg.encryption_);
        if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                        trans,
                        *index_table_schema,
                        new_index_table_schema,
                        OB_DDL_ALTER_TABLE))) {
          RS_LOG(WARN, "schema service update_table_options failed",
            K(*index_table_schema), K(ret));
        }
      }
    }
  }
  return ret;
}

//hualong delete later
//int ObDDLOperator::log_ddl_operation(ObSchemaOperation &ddl_operation,
//                                     ObMySQLTransaction &trans)
//{
//  int ret = OB_SUCCESS;
//  ObSchemaService *schema_service = schema_service_.get_schema_service();
//  if (OB_ISNULL(schema_service)) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("schema_service is NULL", K(ret));
//  } else if (OB_FAIL(schema_service->log_operation(ddl_operation, &trans))) {
//    RS_LOG(WARN, "failed to log ddl operation!", K(ret));
//  } else {
//    // do-nothing
//  }
//  return ret;
//}

int ObDDLOperator::alter_table_options(
    ObSchemaGetterGuard &schema_guard,
    ObTableSchema &new_table_schema,
    const ObTableSchema &table_schema,
    const bool need_update_aux_table,
    ObMySQLTransaction &trans,
    const ObIArray<ObTableSchema> *global_idx_schema_array/*=NULL*/,
    common::ObIArray<std::pair<uint64_t, int64_t>> *idx_schema_versions /*=NULL*/) // pair : <table_id, schema_version>
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema sql service must not be null",
           K(schema_service), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    new_table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
        trans,
        table_schema,
        new_table_schema,
        OB_DDL_ALTER_TABLE))) {
      RS_LOG(WARN, "failed to alter table option!", K(ret));
    } else if (need_update_aux_table) {
      bool has_aux_table_updated = false;
      if (nullptr != idx_schema_versions) {
        idx_schema_versions->reset();
      }
      if (OB_FAIL(update_aux_table(table_schema,
          new_table_schema,
          schema_guard,
          trans,
          USER_INDEX,
          has_aux_table_updated,
          global_idx_schema_array,
          idx_schema_versions))) {
        RS_LOG(WARN, "failed to update_index_table!", K(ret), K(table_schema), K(new_table_schema));
      } else if (OB_FAIL(update_aux_table(table_schema,
          new_table_schema,
          schema_guard,
          trans,
          AUX_VERTIAL_PARTITION_TABLE,
          has_aux_table_updated,
          NULL,
          idx_schema_versions))) {
        RS_LOG(WARN, "failed to update_aux_vp_table!", K(ret), K(table_schema), K(new_table_schema));
      } else if (OB_FAIL(update_aux_table(table_schema,
          new_table_schema,
          schema_guard,
          trans,
          AUX_LOB_META,
          has_aux_table_updated,
          NULL,
          idx_schema_versions))) {
        RS_LOG(WARN, "failed to update_aux_vp_table!", K(ret), K(table_schema), K(new_table_schema));
      } else if (OB_FAIL(update_aux_table(table_schema,
          new_table_schema,
          schema_guard,
          trans,
          AUX_LOB_PIECE,
          has_aux_table_updated,
          NULL,
          idx_schema_versions))) {
        RS_LOG(WARN, "failed to update_aux_vp_table!", K(ret), K(table_schema), K(new_table_schema));
      }

      if (OB_SUCC(ret) && has_aux_table_updated) {
        // update data table schema version
        if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(trans, tenant_id,
                    new_table_schema.get_table_id(), table_schema.get_in_offline_ddl_white_list(), new_schema_version))) {
          LOG_WARN("update data table schema version failed", K(ret));
        } else {
          new_table_schema.set_schema_version(new_schema_version);
        }
      }
    } // need_update_aux_table
  }
  return ret;
}

/*
 * the input value of has_aux_table_updated maybe true or false.
 * has_aux_table_updated represents that if any aux_table updated schema version,
 * aux_table including index table(s), lob meta table, lob piece table.
*/
int ObDDLOperator::update_aux_table(
    const ObTableSchema &table_schema,
    const ObTableSchema &new_table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObMySQLTransaction &trans,
    const ObTableType table_type,
    bool &has_aux_table_updated, /*OUTPUT*/
    const ObIArray<ObTableSchema> *global_idx_schema_array/*=NULL*/,
    common::ObIArray<std::pair<uint64_t, int64_t>> *idx_schema_versions /*=NULL*/) // pair : <table_id, schema_version>

{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const bool is_index = USER_INDEX == table_type;
  ObSEArray<uint64_t, 16> aux_tid_array;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  uint64_t lob_meta_table_id = OB_INVALID_ID;
  uint64_t lob_piece_table_id = OB_INVALID_ID;
  int64_t N = 0;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema sql service must not be null",
           K(schema_service), K(ret));
  } else {
    if (table_type == USER_INDEX) {
      if (OB_FAIL(new_table_schema.get_simple_index_infos(simple_index_infos))) {
        LOG_WARN("get_aux_tid_array failed", K(ret), K(table_type));
      } else {
        N = simple_index_infos.count();
      }
    } else if (table_type == AUX_VERTIAL_PARTITION_TABLE) {
      if (OB_FAIL(new_table_schema.get_aux_vp_tid_array(aux_tid_array))) {
        LOG_WARN("get_aux_tid_array failed", K(ret), K(table_type));
      } else {
        N = aux_tid_array.count();
      }
    } else if (table_type == AUX_LOB_META) {
      lob_meta_table_id = new_table_schema.get_aux_lob_meta_tid();
      N = (table_schema.has_lob_aux_table() && new_table_schema.has_lob_aux_table()) ? 1 : 0;
    } else if (table_type == AUX_LOB_PIECE) {
      lob_piece_table_id = new_table_schema.get_aux_lob_piece_tid();
      N = (table_schema.has_lob_aux_table() && new_table_schema.has_lob_aux_table()) ? 1 : 0;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table type", K(ret), K(table_type));
    }
  }
  if (OB_SUCC(ret)) {
    ObTableSchema new_aux_table_schema;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObTableSchema *aux_table_schema = NULL;
      if (is_index && OB_NOT_NULL(global_idx_schema_array) && !global_idx_schema_array->empty()) {
        for (int64_t j = 0; OB_SUCC(ret) && j < global_idx_schema_array->count(); ++j) {
          if (simple_index_infos.at(i).table_id_ == global_idx_schema_array->at(j).get_table_id()) {
            aux_table_schema = &(global_idx_schema_array->at(j));
            break;
          }
        }
      }
      uint64_t tid = 0;
      if (table_type == USER_INDEX) {
        tid = simple_index_infos.at(i).table_id_;
      } else if (table_type == AUX_VERTIAL_PARTITION_TABLE) {
        tid = aux_tid_array.at(i);
      } else if (table_type == AUX_LOB_META) {
        tid = lob_meta_table_id;
      } else if (table_type == AUX_LOB_PIECE) {
        tid = lob_piece_table_id;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(aux_table_schema)
                 && OB_FAIL(schema_guard.get_table_schema(tenant_id, tid, aux_table_schema))) {
        RS_LOG(WARN, "get_table_schema failed", K(tenant_id), "table id", tid, K(ret));
      } else if (OB_ISNULL(aux_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "table schema should not be null", K(ret));
      } else {
        new_aux_table_schema.reset();
        if (OB_FAIL(new_aux_table_schema.assign(*aux_table_schema))) {
          LOG_WARN("fail to assign schema", K(ret));
        } else {
          if (!aux_table_schema->is_global_index_table()) {
            // tablegroup of global index should not inherit the tablegroup of the data table.
            // the partitions numbers of all table of tablegroup are equal.
            new_aux_table_schema.set_tablegroup_id(new_table_schema.get_tablegroup_id());
          }
          new_aux_table_schema.set_database_id(new_table_schema.get_database_id());
          new_aux_table_schema.set_read_only(new_table_schema.is_read_only());
          new_aux_table_schema.set_progressive_merge_num(new_table_schema.get_progressive_merge_num());
          new_aux_table_schema.set_tablet_size(new_table_schema.get_tablet_size());
          new_aux_table_schema.set_pctfree(new_table_schema.get_pctfree());
          new_aux_table_schema.set_block_size(new_table_schema.get_block_size());
          new_aux_table_schema.set_row_store_type(new_table_schema.get_row_store_type());
          new_aux_table_schema.set_store_format(new_table_schema.get_store_format());
          new_aux_table_schema.set_progressive_merge_round(new_table_schema.get_progressive_merge_round());
          new_aux_table_schema.set_storage_format_version(new_table_schema.get_storage_format_version());
          // index table should only inherit table mode and table state flag from data table
          new_aux_table_schema.set_table_mode(new_table_schema.get_table_mode_flag());
          new_aux_table_schema.set_table_state_flag(new_table_schema.get_table_state_flag());
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(new_aux_table_schema.set_compress_func_name(new_table_schema.get_compress_func_name()))) {
          LOG_WARN("set_compress_func_name failed", K(new_table_schema));
        } else if (aux_table_schema->is_in_recyclebin()) {
          const uint64_t tenant_id = aux_table_schema->get_tenant_id();
          ObArray<ObRecycleObject> recycle_objs;
          ObRecycleObject::RecycleObjType recycle_type = ObRecycleObject::get_type_by_table_schema(*aux_table_schema);
          new_aux_table_schema.set_database_id(aux_table_schema->get_database_id());
          if (OB_INVALID_ID == tenant_id) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("tenant_id is invalid", K(ret));
          } else if (OB_FAIL(schema_service->fetch_recycle_object(
                  tenant_id,
                  aux_table_schema->get_table_name_str(),
                  recycle_type,
                  trans,
                  recycle_objs))) {
            LOG_WARN("get recycle object failed", K(tenant_id), K(ret));
          } else if (recycle_objs.size() != 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected recycle object num", K(ret), K(*aux_table_schema), "size", recycle_objs.size());
          } else if (OB_FAIL(schema_service->delete_recycle_object(
                  tenant_id,
                  recycle_objs.at(0),
                  trans))) {
            LOG_WARN("delete recycle object failed", K(ret));
          } else {
            ObRecycleObject &recycle_obj = recycle_objs.at(0);
            recycle_obj.set_database_id(new_table_schema.get_database_id());
            recycle_obj.set_tablegroup_id(new_table_schema.get_tablegroup_id());
            if (OB_FAIL(schema_service->insert_recyclebin_object(recycle_obj, trans))) {
              LOG_WARN("insert recyclebin object failed", K(ret));
            }
          }
        }
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else {
          has_aux_table_updated = true;
          new_aux_table_schema.set_schema_version(new_schema_version);
          if (OB_FAIL(schema_service->get_table_sql_service().only_update_table_options(
                  trans,
                  new_aux_table_schema,
                  OB_DDL_ALTER_TABLE))) {
            RS_LOG(WARN, "schema service update_table_options failed",
                K(*aux_table_schema), K(ret));
          } else if ((nullptr != idx_schema_versions) &&
              OB_FAIL(idx_schema_versions->push_back(std::make_pair(new_aux_table_schema.get_table_id(), new_schema_version)))) {
            RS_LOG(WARN, "fail to push_back array", K(ret), KPC(idx_schema_versions), K(new_schema_version));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::rename_table(const ObTableSchema &table_schema,
                                const ObString &new_table_name,
                                const uint64_t new_db_id,
                                const bool need_reset_object_status,
                                ObMySQLTransaction &trans,
                                const ObString *ddl_stmt_str,
                                int64_t &new_data_table_schema_version /*OUTPUT*/,
                                ObIArray<std::pair<uint64_t, int64_t>> &idx_schema_versions /*OUTPUT*/) // pair : table_id, schema_version
{
  int ret = OB_SUCCESS;
  idx_schema_versions.reset();
  const uint64_t tenant_id = table_schema.get_tenant_id();
  new_data_table_schema_version = OB_INVALID_VERSION;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema sql service must not be null",
           K(schema_service), K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    RS_LOG(WARN, "get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_data_table_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    ObTableSchema new_table_schema;
    if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else {
      new_table_schema.set_schema_version(new_data_table_schema_version);
    }
    if (need_reset_object_status) {
      new_table_schema.set_object_status(ObObjectStatus::INVALID);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(new_table_schema.set_table_name(new_table_name))) {
      RS_LOG(WARN, "failed to set new table name!", K(new_table_name), K(table_schema), K(ret));
    } else {
      new_table_schema.set_database_id(new_db_id);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
          trans,
          table_schema,
          new_table_schema,
          OB_DDL_TABLE_RENAME,
          ddl_stmt_str))) {
        RS_LOG(WARN, "failed to alter table option!", K(ret));
      } else {
        bool has_aux_table_updated = false;
        HEAP_VAR(ObTableSchema, new_aux_table_schema) {
          { // update index table
            ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
            if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
              RS_LOG(WARN, "get_index_tid_array failed", K(ret));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
                if (OB_FAIL(rename_aux_table(new_table_schema,
                                             simple_index_infos.at(i).table_id_,
                                             schema_guard,
                                             trans,
                                             new_aux_table_schema,
                                             has_aux_table_updated))) {
                  RS_LOG(WARN, "fail to rename update index table", K(ret));
                } else if (OB_FAIL(idx_schema_versions.push_back(std::make_pair(new_aux_table_schema.get_table_id(), new_aux_table_schema.get_schema_version())))) {
                  RS_LOG(WARN, "fail to push_back array", K(ret), K(idx_schema_versions), K(new_aux_table_schema.get_schema_version()));
                }
              }
            }
          }
          if (OB_SUCC(ret) && table_schema.has_lob_aux_table()) {
            uint64_t mtid = table_schema.get_aux_lob_meta_tid();
            uint64_t ptid = table_schema.get_aux_lob_piece_tid();
            if (OB_INVALID_ID == mtid || OB_INVALID_ID == ptid) {
              ret = OB_ERR_UNEXPECTED;
              RS_LOG(WARN, "Expect meta tid and piece tid valid", KR(ret), K(mtid), K(ptid));
            } else if (OB_FAIL(rename_aux_table(new_table_schema,
                                                mtid,
                                                schema_guard,
                                                trans,
                                                new_aux_table_schema,
                                                has_aux_table_updated))) {
              RS_LOG(WARN, "fail to rename update lob meta table", KR(ret), K(mtid));
            } else if (OB_FAIL(idx_schema_versions.push_back(std::make_pair(new_aux_table_schema.get_table_id(), new_aux_table_schema.get_schema_version())))) {
              RS_LOG(WARN, "fail to push_back array", K(ret), K(idx_schema_versions), K(new_aux_table_schema.get_schema_version()));
            } else if (OB_FAIL(rename_aux_table(new_table_schema,
                                                ptid,
                                                schema_guard,
                                                trans,
                                                new_aux_table_schema,
                                                has_aux_table_updated))) {
              RS_LOG(WARN, "fail to rename update lob piece table", KR(ret), K(ptid));
            } else if (OB_FAIL(idx_schema_versions.push_back(std::make_pair(new_aux_table_schema.get_table_id(), new_aux_table_schema.get_schema_version())))) {
              RS_LOG(WARN, "fail to push_back array", K(ret), K(idx_schema_versions), K(new_aux_table_schema.get_schema_version()));
            }
          }
        }

        if (OB_SUCC(ret) && has_aux_table_updated) {
          // update data table schema version
          if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_data_table_schema_version))) {
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(trans, tenant_id,
                      new_table_schema.get_table_id(), table_schema.get_in_offline_ddl_white_list(), new_data_table_schema_version))) {
            LOG_WARN("update data table schema version failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

/*
 * the input value of has_aux_table_updated maybe true or false.
 * has_aux_table_updated represents that if any aux_table updated schema version,
 * aux_table including index table(s), lob meta table, lob piece table.
*/
int ObDDLOperator::rename_aux_table(
    const ObTableSchema &new_table_schema,
    const uint64_t table_id,
    ObSchemaGetterGuard &schema_guard,
    ObMySQLTransaction &trans,
    ObTableSchema &new_aux_table_schema,
    bool &has_aux_table_updated /*OUTPUT*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const ObTableSchema *aux_table_schema = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(schema_guard.get_table_schema(
              tenant_id, table_id, aux_table_schema))) {
    RS_LOG(WARN, "get_table_schema failed", K(tenant_id),
            "table id", table_id, K(ret));
  } else if (OB_ISNULL(aux_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "table schema should not be null", K(ret));
  } else {
    new_aux_table_schema.reset();
    if (OB_FAIL(new_aux_table_schema.assign(*aux_table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else {
      new_aux_table_schema.set_database_id(new_table_schema.get_database_id());
    }
    if (OB_FAIL(ret)) {
    } else if (aux_table_schema->is_in_recyclebin()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aux table is in recycle bin while main table not in", K(ret), KPC(aux_table_schema));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else {
      has_aux_table_updated = true;
      new_aux_table_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service->get_table_sql_service().only_update_table_options(
              trans,
              new_aux_table_schema,
              OB_DDL_TABLE_RENAME))) {
        RS_LOG(WARN, "schema service update_table_options failed",
            K(*aux_table_schema), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_index_status(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const uint64_t index_table_id,
    const share::schema::ObIndexStatus status,
    const bool in_offline_ddl_white_list,
    common::ObMySQLTransaction &trans,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  ObTableSchema copy_data_table_schema;

  if (OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_table_id
      || status <= INDEX_STATUS_NOT_FOUND || status >= INDEX_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_table_id), K(index_table_id), K(status));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service should not be NULL");
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("get table schema failed", K(ret));
  } else if (nullptr == data_table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, table schema must not be nullptr", K(ret));
  } else if (OB_FAIL(copy_data_table_schema.assign(*data_table_schema))) {
    LOG_WARN("assign data table schema failed", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (FALSE_IT(copy_data_table_schema.set_in_offline_ddl_white_list(in_offline_ddl_white_list))) {
  } else if (OB_FAIL(schema_service->get_table_sql_service().update_index_status(
      copy_data_table_schema, index_table_id, status, new_schema_version, trans, ddl_stmt_str))) {
    LOG_WARN("update index status failed",
        K(ret), K(data_table_id), K(index_table_id), K(status));
  }
  return ret;
}

int ObDDLOperator::update_table_attribute(ObTableSchema &new_table_schema,
                                          common::ObMySQLTransaction &trans,
                                          const ObSchemaOperationType operation_type,
                                          const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const bool update_object_status_ignore_version = false;
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    new_table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service_impl->get_table_sql_service().update_table_attribute(
        trans,
        new_table_schema,
        operation_type,
        update_object_status_ignore_version,
        ddl_stmt_str))) {
      RS_LOG(WARN, "failed to update table attribute!" ,K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::update_single_column(common::ObMySQLTransaction &trans,
                                        const ObTableSchema &origin_table_schema,
                                        const ObTableSchema &new_table_schema,
                                        ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    column_schema.set_schema_version(new_schema_version);
    const ObColumnSchemaV2 *orig_column_schema = origin_table_schema.get_column_schema(column_schema.get_column_id());
    if (OB_FAIL(schema_service_impl->get_table_sql_service().update_single_column(
              trans, origin_table_schema, new_table_schema, column_schema,
              true /* record_ddl_operation */))) {
      RS_LOG(WARN, "failed to update single column", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::update_single_column_group(common::ObMySQLTransaction &trans,
                                              const ObTableSchema &origin_table_schema,
                                              const ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  bool is_each_cg_exist = false;
  const ObColumnSchemaV2 *orig_column_schema = nullptr;
  char cg_name[OB_MAX_COLUMN_GROUP_NAME_LENGTH] = {'\0'};
  ObString cg_name_str(OB_MAX_COLUMN_GROUP_NAME_LENGTH, 0, cg_name);
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  ObColumnGroupSchema *ori_cg = nullptr;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  orig_column_schema = origin_table_schema.get_column_schema(column_schema.get_column_id());
  if (!origin_table_schema.is_valid() || !column_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "Invalid arguemnt", K(ret), K(origin_table_schema), K(column_schema));
  } else if (OB_ISNULL(orig_column_schema)) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "column should not be null", K(ret), K(column_schema), K(origin_table_schema));
  } else if (orig_column_schema->get_column_name_str() == column_schema.get_column_name_str()) {
    /* now only rename column will use this func, other skip*/
  } else if (!origin_table_schema.is_column_store_supported()) {
    /* only support table need column group*/
  } else if (OB_FAIL(origin_table_schema.is_column_group_exist(OB_EACH_COLUMN_GROUP_NAME, is_each_cg_exist))) {
    RS_LOG(WARN, "fail check whether each cg exist", K(ret));
  } else if (!is_each_cg_exist) {
    /* if each cg not exist skip*/
  } else if (column_schema.is_virtual_generated_column()) {
    /* skip virtual generated_column*/
  } else if (OB_FAIL(orig_column_schema->get_each_column_group_name(cg_name_str))) {
    RS_LOG(WARN, "fail to get each column group name", K(ret));
  } else if (OB_FAIL(origin_table_schema.get_column_group_by_name(cg_name_str, ori_cg))) {
    RS_LOG(WARN, "column group cannot get", K(cg_name_str), K(origin_table_schema));
  } else if (OB_ISNULL(ori_cg)) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "column group should not be null", K(ret), K(cg_name_str),
           KPC(orig_column_schema), K(origin_table_schema));
  } else {
    ObColumnGroupSchema new_cg;
    if (OB_FAIL(new_cg.assign(*ori_cg))) {
      RS_LOG(WARN, "fail to assign column group", K(ret), K(ori_cg));
    } else {
      new_cg.set_schema_version(column_schema.get_schema_version());
      cg_name_str.set_length(0);
      if (OB_FAIL(column_schema.get_each_column_group_name(cg_name_str))) {
        RS_LOG(WARN, "fail to gen column group related column group name", K(ret), K(column_schema));
      } else if (OB_FAIL(new_cg.set_column_group_name(cg_name_str))) {
        RS_LOG(WARN, "fail to set column group name", K(ret), K(new_cg), K(cg_name_str));
      } else if (OB_FAIL(schema_service_impl->get_table_sql_service().update_single_column_group(trans,
                                                                                origin_table_schema,
                                                                                *ori_cg,
                                                                                new_cg))) {
        RS_LOG(WARN,"fail to update single column_group", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::batch_update_system_table_columns(
    common::ObMySQLTransaction &trans,
    const share::schema::ObTableSchema &orig_table_schema,
    share::schema::ObTableSchema &new_table_schema,
    const common::ObIArray<uint64_t> &add_column_ids,
    const common::ObIArray<uint64_t> &alter_column_ids,
    const common::ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t table_id = new_table_schema.get_table_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const bool update_object_status_ignore_version = false;
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema_service_impl must not null", KR(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id));
  } else {
    (void) new_table_schema.set_schema_version(new_schema_version);
    ObColumnSchemaV2 *new_column = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < add_column_ids.count(); i++) {
      const uint64_t column_id = add_column_ids.at(i);
      if (OB_ISNULL(new_column = new_table_schema.get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column", KR(ret), K(tenant_id), K(table_id), K(column_id));
      } else if (FALSE_IT(new_column->set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service_impl->get_table_sql_service().insert_single_column(
                 trans, new_table_schema, *new_column, false))) {
        LOG_WARN("fail to insert column", KR(ret), K(tenant_id), K(table_id), K(column_id));
      }
    } // end for

    for (int64_t i = 0; OB_SUCC(ret) && i < alter_column_ids.count(); i++) {
      const uint64_t column_id = alter_column_ids.at(i);
      if (OB_ISNULL(new_column = new_table_schema.get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column", KR(ret), K(tenant_id), K(table_id), K(column_id));
      } else if (FALSE_IT(new_column->set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service_impl->get_table_sql_service().update_single_column(
                 trans, orig_table_schema, new_table_schema, *new_column, false))) {
        LOG_WARN("fail to insert column", KR(ret), K(tenant_id), K(table_id), K(column_id));
      }
    } // end for

    if (FAILEDx(schema_service_impl->get_table_sql_service().update_table_attribute(
        trans, new_table_schema, OB_DDL_ALTER_TABLE, update_object_status_ignore_version, ddl_stmt_str))) {
      LOG_WARN("failed to update table attribute", KR(ret), K(tenant_id), K(table_id));
    }
  }
  return ret;
}

int ObDDLOperator::update_partition_option(common::ObMySQLTransaction &trans,
                                           ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(schema_service_impl->get_table_sql_service().update_partition_option(
        trans, table_schema, new_schema_version))) {
      RS_LOG(WARN, "failed to update partition option", K(table_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::update_check_constraint_state(common::ObMySQLTransaction &trans,
                                                 const ObTableSchema &table_schema,
                                                 ObConstraint &cst)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    cst.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service_impl->get_table_sql_service().update_check_constraint_state(trans,
                table_schema, cst))) {
      RS_LOG(WARN, "failed to update check constraint state", K(table_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::sync_aux_schema_version_for_history(common::ObMySQLTransaction &trans,
                                                      const ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = index_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(schema_service_impl->get_table_sql_service().sync_aux_schema_version_for_history(
                trans, index_schema, new_schema_version))) {
      RS_LOG(WARN, "failed to update check constraint state", K(index_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_obj_privs(
    const uint64_t tenant_id,
    const uint64_t obj_id,
    const uint64_t obj_type,
    ObMySQLTransaction &trans,
    ObMultiVersionSchemaService &schema_service,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_sql_service = schema_service.get_schema_service();
  ObArray<const ObObjPriv *> obj_privs;

  CK (OB_NOT_NULL(schema_sql_service));
  OZ (schema_guard.get_obj_priv_with_obj_id(tenant_id, obj_id, obj_type, obj_privs, true));
  for (int64_t i = 0; OB_SUCC(ret) && i < obj_privs.count(); ++i) {
    const ObObjPriv *obj_priv = obj_privs.at(i);
    int64_t new_schema_version = OB_INVALID_VERSION;

    if (OB_ISNULL(obj_priv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj_priv priv is NULL", K(ret), K(obj_priv));
    } else {
      OZ (schema_service.gen_new_schema_version(tenant_id, new_schema_version));
      OZ (schema_sql_service->get_priv_sql_service().delete_obj_priv(
                *obj_priv, new_schema_version, trans));
      // In order to prevent being deleted, but there is no time to refresh the schema.
      // for example, obj priv has deleted, but obj schema unrefresh
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}


int ObDDLOperator::drop_obj_privs(
    const uint64_t tenant_id,
    const uint64_t obj_id,
    const uint64_t obj_type,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;

  OZ (schema_service_.get_tenant_schema_guard(tenant_id, schema_guard));
  OZ (drop_obj_privs(tenant_id, obj_id, obj_type, trans, schema_service_, schema_guard));

  return ret;
}

int ObDDLOperator::drop_tablet_of_table(
    const ObTableSchema &table_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global variable is null", KR(ret), K(GCTX.srv_rpc_proxy_), K(GCTX.lst_operator_));
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    int64_t new_schema_version = OB_INVALID_VERSION;
    ObSEArray<const ObTableSchema*, 1> schemas;
    if (table_schema.is_vir_table()
        || table_schema.is_view_table()
        || is_inner_table(table_schema.get_table_id())) {
      // skip
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id));
    } else {
      ObTabletDrop tablet_drop(tenant_id, trans, new_schema_version);
      if (OB_FAIL(schemas.push_back(&table_schema))) {
        LOG_WARN("failed to push_back", KR(ret), K(table_schema));
      } else if (OB_FAIL(tablet_drop.init())) {
        LOG_WARN("fail to init tablet drop", KR(ret), K(table_schema));
      } else if (OB_FAIL(tablet_drop.add_drop_tablets_of_table_arg(schemas))) {
        LOG_WARN("failed to add drop tablets", KR(ret), K(tenant_id), K(table_schema));
      } else if (OB_FAIL(tablet_drop.execute())) {
        LOG_WARN("failed to execute", KR(ret), K(schemas), K(table_schema));
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_table(
    const ObTableSchema &table_schema,
    ObMySQLTransaction &trans,
    const ObString *ddl_stmt_str/*=NULL*/,
    const bool is_truncate_table/*false*/,
    DropTableIdHashSet *drop_table_set/*=NULL*/,
    const bool is_drop_db/*false*/,
    const bool delete_priv)
{
  int ret = OB_SUCCESS;
  bool tmp = false;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  if (OB_FAIL(ObDependencyInfo::modify_dep_obj_status(trans, tenant_id, table_schema.get_table_id(),
                                                      *this, schema_service_))) {
    LOG_WARN("failed to modify obj status", K(ret));
  } else if (OB_FAIL(drop_table_for_not_dropped_schema(
              table_schema, trans, ddl_stmt_str, is_truncate_table,
              drop_table_set, is_drop_db))) {
    LOG_WARN("drop table for not dropped shema failed", K(ret));
  } else if (table_schema.is_view_table()
            && OB_FAIL(ObDependencyInfo::delete_schema_object_dependency(
                      trans,
                      tenant_id,
                      table_schema.get_table_id(),
                      table_schema.get_schema_version(),
                      ObObjectType::VIEW))) {
    LOG_WARN("failed to delete_schema_object_dependency", K(ret), K(tenant_id),
    K(table_schema.get_table_id()));
  }

  if (OB_FAIL(ret)) {
  } else if ((table_schema.is_aux_table() || table_schema.is_mlog_table())
      && !is_inner_table(table_schema.get_table_id())) {
    ObSnapshotInfoManager snapshot_mgr;
    ObArray<ObTabletID> tablet_ids;
    SCN invalid_scn;
    if (OB_FAIL(snapshot_mgr.init(GCTX.self_addr()))) {
      LOG_WARN("fail to init snapshot mgr", K(ret));
    } else if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
      LOG_WARN("fail to get tablet ids", K(ret));
    // when a index or lob is dropped, it should release all snapshots acquired, otherwise
    // if a building index is dropped in another session, the index build task cannot release snapshots
    // because the task needs schema to know tablet ids.
    } else if (OB_FAIL(snapshot_mgr.batch_release_snapshot_in_trans(
            trans, SNAPSHOT_FOR_DDL, tenant_id, -1/*schema_version*/, invalid_scn/*snapshot_scn*/, tablet_ids))) {
      LOG_WARN("fail to release ddl snapshot acquired by this table", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (table_schema.is_external_table()) {
    if (OB_FAIL(ObExternalTableFileManager::get_instance().clear_inner_table_files(
                  table_schema.get_tenant_id(), table_schema.get_table_id(), trans))) {
      LOG_WARN("delete external table file list failed", K(ret));
    }
  } else {
    if (OB_FAIL(drop_tablet_of_table(table_schema, trans))) {
      LOG_WARN("fail to drop tablet", K(table_schema), KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t table_id = table_schema.get_table_id();
    if (table_schema.is_materialized_view()) {
      if (OB_FAIL(ObMViewSchedJobUtils::remove_mview_refresh_job(
          trans, tenant_id, table_id))) {
        LOG_WARN("failed to remove mview refresh job",
            KR(ret), K(tenant_id), K(table_id));
      }
    } else if (table_schema.is_mlog_table()) {
      if (OB_FAIL(ObMViewSchedJobUtils::remove_mlog_purge_job(
          trans, tenant_id, table_id))) {
        LOG_WARN("failed to remove mlog purge job",
            KR(ret), K(tenant_id), K(table_id));
      }
    }
  }

  return ret;
}

int ObDDLOperator::drop_table_for_not_dropped_schema(
    const ObTableSchema &table_schema,
    ObMySQLTransaction &trans,
    const ObString *ddl_stmt_str/*=NULL*/,
    const bool is_truncate_table/*false*/,
    DropTableIdHashSet *drop_table_set/*=NULL*/,
    const bool is_drop_db/*false*/,
    const bool delete_priv)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  }
  //delete all object privileges granted on the object
  uint64_t obj_type = static_cast<uint64_t>(ObObjectType::TABLE);
  uint64_t table_id = table_schema.get_table_id();
  if (OB_SUCC(ret) && !is_drop_db && delete_priv) {
    OZ (drop_obj_privs(tenant_id, table_id, obj_type, trans),tenant_id, table_id, obj_type);
  } else {
    LOG_WARN("do not cascade drop obj priv", K(ret), K(is_drop_db), K(delete_priv));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(cleanup_autoinc_cache(table_schema))) {
    LOG_WARN("fail cleanup auto inc global cache", K(ret));
  } else if (OB_FAIL(drop_sequence_in_drop_table(table_schema, trans, schema_guard))) {
    LOG_WARN("drop sequence in drop table fail", K(ret));
  } else if (OB_FAIL(drop_rls_object_in_drop_table(table_schema, trans, schema_guard))) {
    LOG_WARN("fail to drop rls object in drop table", K(ret));
  } else if (OB_FAIL(schema_service_impl->get_table_sql_service().drop_table(
                     table_schema,
                     new_schema_version,
                     trans,
                     ddl_stmt_str,
                     is_truncate_table,
                     is_drop_db,
                     &schema_guard,
                     drop_table_set))) {
    LOG_WARN("schema_service_impl drop_table failed", K(table_schema), K(ret));
  } else if (OB_FAIL(sync_version_for_cascade_table(tenant_id, table_schema.get_base_table_ids(), trans))
             || OB_FAIL(sync_version_for_cascade_table(tenant_id, table_schema.get_depend_table_ids(), trans))) {
    LOG_WARN("fail to sync versin for cascade tables", K(ret), K(tenant_id),
        K(table_schema.get_base_table_ids()), K(table_schema.get_depend_table_ids()));
  } else if (OB_FAIL(sync_version_for_cascade_mock_fk_parent_table(table_schema.get_tenant_id(), table_schema.get_depend_mock_fk_parent_table_ids(), trans))) {
    LOG_WARN("fail to sync cascade depend_mock_fk_parent_table_ids table", K(ret));
  }

  // delete audit in table
  if (OB_SUCC(ret) && (table_schema.is_user_table() || table_schema.is_external_table())) {
    ObArray<const ObSAuditSchema *> audits;
    if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                       AUDIT_TABLE,
                                                       table_schema.get_table_id(),
                                                       audits))) {
      LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(table_schema), K(ret));
    } else {
      common::ObSqlString public_sql_string;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
        const ObSAuditSchema *audit_schema = audits.at(i);
        if (OB_ISNULL(audit_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("audit_schema is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
            *audit_schema,
            AUDIT_MT_DEL,
            false,
            new_schema_version,
            NULL,
            trans,
            public_sql_string))) {
          LOG_WARN("drop audit_schema failed",  KPC(audit_schema), K(ret));
        }
      }
    }
  }
  return ret;
}

// ref
// When tables with auto-increment columns are frequently created or deleted, if the auto-increment column cache is not cleared, the memory will grow slowly.
// so every time when you drop table, if you bring auto-increment columns, clean up the corresponding cache.
int ObDDLOperator::cleanup_autoinc_cache(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObAutoincrementService &autoinc_service = share::ObAutoincrementService::get_instance();
  uint64_t tenant_id = table_schema.get_tenant_id();
  bool is_restore = false;
  if (OB_FAIL(schema_service_.check_tenant_is_restore(NULL, tenant_id, is_restore))) {
    LOG_WARN("fail to check if tenant is restore", KR(ret), K(tenant_id));
  } else if (is_restore) {
    // bugfix:
    // skip
  } else if (0 != table_schema.get_autoinc_column_id()) {
    uint64_t table_id = table_schema.get_table_id();
    uint64_t autoinc_column_id = table_schema.get_autoinc_column_id();
    LOG_INFO("begin to clear all auto-increment cache",
             K(tenant_id), K(table_id), K(autoinc_column_id));
    if (OB_FAIL(autoinc_service.clear_autoinc_cache_all(tenant_id,
                                                        table_id,
                                                        autoinc_column_id,
                                                        table_schema.is_order_auto_increment_mode()))) {
      LOG_WARN("failed to clear auto-increment cache",
               K(tenant_id), K(table_id));
    }
  }
  return ret;
}

bool ObDDLOperator::is_aux_object(const ObDatabaseSchema &schema)
{
  UNUSED(schema);
  return false;
}

bool ObDDLOperator::is_aux_object(const ObTableSchema &schema)
{
  return schema.is_aux_table();
}

bool ObDDLOperator::is_aux_object(const ObTriggerInfo &schema)
{
  UNUSED(schema);
  return false;
}

bool ObDDLOperator::is_aux_object(const ObTenantSchema &schema)
{
  UNUSED(schema);
  return false;
}

bool ObDDLOperator::is_global_index_object(const ObDatabaseSchema &schema)
{
  UNUSED(schema);
  return false;
}

bool ObDDLOperator::is_global_index_object(const ObTableSchema &schema)
{
  // For global local storage, local indexes are still seen, liboblog does not need to be synchronized
  return schema.is_global_index_table() && (!schema.is_index_local_storage());
}

bool ObDDLOperator::is_global_index_object(const ObTriggerInfo &schema)
{
  UNUSED(schema);
  return false;
}

bool ObDDLOperator::is_global_index_object(const ObTenantSchema &schema)
{
  UNUSED(schema);
  return false;
}

int ObDDLOperator::drop_table_to_recyclebin(const ObTableSchema &table_schema,
                                            ObSchemaGetterGuard &schema_guard,
                                            ObMySQLTransaction &trans,
                                            const ObString *ddl_stmt_str,/*= NULL*/
                                            const bool is_truncate_table)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  bool recycle_db_exist = false;
  // materialized view will not be dropped into recyclebin
  if (table_schema.get_table_type() == MATERIALIZED_VIEW) {
    LOG_WARN("bypass recyclebin for materialized view");
  } else if (OB_UNLIKELY(table_schema.has_mlog_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table with materialized view log should not come to recyclebin", KR(ret));
  } else if (OB_UNLIKELY(table_schema.get_table_type() == MATERIALIZED_VIEW_LOG)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("materialized view log should not come to recyclebin", KR(ret));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else if (OB_FAIL(schema_guard.check_database_exist(tenant_id,
                                                       OB_RECYCLEBIN_SCHEMA_ID,
                                                       recycle_db_exist))) {
    LOG_WARN("check database exist failed", K(ret), K(tenant_id));
  } else if (!recycle_db_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("__recyclebin db not exist", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(cleanup_autoinc_cache(table_schema))) {
    LOG_WARN("fail cleanup auto inc global cache", K(ret));
  } else if (OB_FAIL(ObDependencyInfo::modify_dep_obj_status(trans, tenant_id, table_schema.get_table_id(),
                                                             *this, schema_service_))) {
    LOG_WARN("failed to modify dep obj status", K(ret));
  } else if (table_schema.is_view_table()
            && OB_FAIL(ObDependencyInfo::delete_schema_object_dependency(
                      trans,
                      tenant_id,
                      table_schema.get_table_id(),
                      table_schema.get_schema_version(),
                      ObObjectType::VIEW))) {
    LOG_WARN("failed to delete_schema_object_dependency", K(ret), K(tenant_id),
    K(table_schema.get_table_id()));
  } else {
    ObTableSchema new_table_schema;
    if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else {
      ObSqlString new_table_name;
      //move to the recyclebin db
      new_table_schema.set_database_id(OB_RECYCLEBIN_SCHEMA_ID);
      uint64_t tablegroup_id = table_schema.get_tablegroup_id();
      if (OB_INVALID_ID != tablegroup_id) {
        const ObTablegroupSchema *tablegroup_schema = nullptr;
        if (OB_FAIL(schema_guard.get_tablegroup_schema(
                tenant_id,
                tablegroup_id,
                tablegroup_schema))) {
          LOG_WARN("get tablegroup schema failed", K(ret), K(tenant_id));
        } else {
          new_table_schema.set_tablegroup_id(OB_INVALID_ID);
        }
      }
      new_table_schema.set_schema_version(new_schema_version);
      ObSchemaOperationType op_type = OB_INVALID_DDL_OP;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(construct_new_name_for_recyclebin(new_table_schema, new_table_name))) {
        LOG_WARN("failed to construct new name for table", K(ret));
      } else if (OB_FAIL(new_table_schema.set_table_name(new_table_name.string()))) {
        LOG_WARN("failed to set new table name!", K(new_table_name), K(table_schema), K(ret));
      } else {
        ObRecycleObject recycle_object;
        recycle_object.set_object_name(new_table_name.string());
        recycle_object.set_original_name(table_schema.get_table_name_str());
        recycle_object.set_tenant_id(table_schema.get_tenant_id());
        recycle_object.set_database_id(table_schema.get_database_id());
        recycle_object.set_table_id(table_schema.get_table_id());
        recycle_object.set_tablegroup_id(table_schema.get_tablegroup_id());
        op_type = table_schema.is_view_table()
            ? OB_DDL_DROP_VIEW_TO_RECYCLEBIN : OB_DDL_DROP_TABLE_TO_RECYCLEBIN;
        if (is_truncate_table) {
          op_type = OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN;
        }
        if (OB_FAIL(recycle_object.set_type_by_table_schema(table_schema))) {
          LOG_WARN("set type by table schema failed", K(ret));
        } else if (OB_FAIL(schema_service_impl->insert_recyclebin_object(recycle_object,
                                                                         trans))) {
          LOG_WARN("insert recycle object failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(schema_service_impl->get_table_sql_service().update_table_options(
                    trans,
                    table_schema,
                    new_table_schema,
                    op_type,
                    ddl_stmt_str))) {
          LOG_WARN("failed to alter table option!", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::flashback_table_from_recyclebin(const ObTableSchema &table_schema,
                                                   ObTableSchema &new_table_schema,
                                                   ObMySQLTransaction &trans,
                                                   const uint64_t new_db_id,
                                                   const ObString &new_table_name,
                                                   const ObString *ddl_stmt_str,
                                                   ObSchemaGetterGuard &guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObArray<ObRecycleObject> recycle_objs;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObRecycleObject::RecycleObjType recycle_type = ObRecycleObject::get_type_by_table_schema(table_schema);
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_INVALID_ID == table_schema.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else if (OB_FAIL(schema_service->fetch_recycle_object(
      tenant_id,
      table_schema.get_table_name_str(),
      recycle_type,
      trans,
      recycle_objs))) {
    LOG_WARN("get_recycle_object failed", K(tenant_id), K(ret));
  } else if (recycle_objs.size() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected recycle object num", K(ret),
             "table_name", table_schema.get_table_name_str(),
             "size", recycle_objs.size());
  } else {
    const ObRecycleObject &recycle_obj = recycle_objs.at(0);
    if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else if (new_db_id != OB_INVALID_ID) {//flashback to new db
      new_table_schema.set_database_id(new_db_id);
      if (new_table_schema.is_aux_table()) {
        // should set the old name
        // When flashback table to new db, distinguish between empty index name and renaming flashback index
        if (!new_table_name.empty() && OB_FAIL(new_table_schema.set_table_name(new_table_name))) {
          LOG_WARN("set new table name failed", K(ret));
        } else if (new_table_name.empty() && OB_FAIL(new_table_schema.set_table_name(recycle_obj.get_original_name()))) {
          LOG_WARN("set new table name failed", K(ret));
        } else if (new_table_schema.is_index_table()) {
          const int VISIBLE = 0;
          const uint64_t DROPINDEX = 0;
          new_table_schema.set_drop_index(DROPINDEX);
          if (!table_schema.is_invisible_before()) {
            new_table_schema.set_index_visibility(VISIBLE);
          }
          new_table_schema.set_invisible_before(0);
        }
        if (OB_SUCC(ret) && new_table_schema.is_index_table()) {
          bool is_oracle_mode = false;
          if (OB_FAIL(new_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
            LOG_WARN("fail check if oracle mode", K(ret));
          } else if (is_oracle_mode) {
            ObString new_idx_name;
            if (OB_FAIL(ObTableSchema::create_new_idx_name_after_flashback(new_table_schema,
                                                                           new_idx_name,
                                                                           allocator,
                                                                           guard))) {
            } else if (OB_FAIL(new_table_schema.set_table_name(new_idx_name))) {
              LOG_WARN("set new table name failed", K(ret));
            }
          }
        }
      } else {
        if (!new_table_name.empty()) {
          if (OB_FAIL(new_table_schema.set_table_name(new_table_name))) {
            LOG_WARN("set new table name failed", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("database is valid, but no table name for data table",
                   K(ret), K(new_table_name));
        }
      }
    } else {
      //set original db_id
      const ObDatabaseSchema *db_schema = NULL;
      if (OB_FAIL(guard.get_database_schema(tenant_id,
                                            recycle_obj.get_database_id(),
                                            db_schema))) {
        LOG_WARN("get database schema failed", K(ret));
      } else if (NULL == db_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database not exist", K(recycle_obj), K(ret));
      } else if (db_schema->is_in_recyclebin()) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("flashback table to __recyclebin database is not allowed",
                 K(recycle_obj), K(*db_schema), K(ret));
      } else if (OB_FAIL(new_table_schema.set_table_name(recycle_obj.get_original_name()))) {
        LOG_WARN("set table name failed", K(ret), K(recycle_obj));
      } else {
        new_table_schema.set_database_id(recycle_obj.get_database_id());
      }

      if (OB_SUCC(ret) && new_table_schema.is_index_table()) {
        const int VISIBLE = 0;
        const uint64_t DROPINDEX = 0;
        new_table_schema.set_drop_index(DROPINDEX);
        if (!table_schema.is_invisible_before()) {
          new_table_schema.set_index_visibility(VISIBLE);
        }
        new_table_schema.set_invisible_before(0);
      }
      if (OB_SUCC(ret) && new_table_schema.is_index_table()) {
        bool is_oracle_mode = false;
        if (OB_FAIL(new_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
          LOG_WARN("fail check if oracle mode", K(ret));
        } else if (is_oracle_mode) {
          ObString new_idx_name;
          if (OB_FAIL(ObTableSchema::create_new_idx_name_after_flashback(new_table_schema,
                                                                         new_idx_name,
                                                                         allocator,
                                                                         guard))) {
          } else if (OB_FAIL(new_table_schema.set_table_name(new_idx_name))) {
            LOG_WARN("set new table name failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool is_table_exist = true;
      bool object_exist = false;
      uint64_t synonym_id = OB_INVALID_ID;
      const int64_t table_schema_version = OB_INVALID_VERSION; // Take the latest local schema_guard
      ObSchemaOperationType op_type = new_table_schema.is_view_table()
          ? OB_DDL_FLASHBACK_VIEW : OB_DDL_FLASHBACK_TABLE;
      if (new_table_schema.is_index_table()) {
        op_type = OB_DDL_FLASHBACK_INDEX;
      }
      if (OB_FAIL(schema_service_.check_synonym_exist(tenant_id,
                                                       new_table_schema.get_database_id(),
                                                       new_table_schema.get_table_name_str(),
                                                       object_exist,
                                                       synonym_id))) {
        LOG_WARN("fail to check synonym exist", K(new_table_schema), K(ret));
      } else if (object_exist) {
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by an existing object", K(new_table_schema), K(ret));
      } else if (OB_FAIL(schema_service_.check_table_exist(tenant_id,
                                                           new_table_schema.get_database_id(),
                                                           new_table_schema.get_table_name_str(),
                                                           new_table_schema.is_index_table(),
                                                           table_schema_version,
                                                           is_table_exist))) {
        LOG_WARN("check_table exist failed", K(ret));
      } else if (is_table_exist) {
        ret = OB_ERR_TABLE_EXIST;
        LOG_USER_ERROR(OB_ERR_TABLE_EXIST, recycle_obj.get_original_name().length(),
                       recycle_obj.get_original_name().ptr());
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (FALSE_IT(new_table_schema.set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
          trans,
          table_schema,
          new_table_schema,
          op_type,
          ddl_stmt_str))) {
        LOG_WARN("update_table_options failed", K(ret));
      } else if (OB_FAIL(schema_service->delete_recycle_object(tenant_id,
                                                               recycle_obj,
                                                               trans))) {
        LOG_WARN("delete_recycle_object failed", K(tenant_id), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::purge_table_with_aux_table(
    const ObTableSchema &table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObMySQLTransaction &trans,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_aux_table()) {
    if (OB_FAIL(purge_aux_table(table_schema, schema_guard, trans, USER_INDEX))) {
      LOG_WARN("purge_aux_table failed", K(ret), K(table_schema));
    } else if (OB_FAIL(purge_aux_table(table_schema, schema_guard, trans,
                                       AUX_VERTIAL_PARTITION_TABLE))) {
      LOG_WARN("purge_aux_table failed", K(ret), K(table_schema));
    } else if (OB_FAIL(purge_aux_table(table_schema, schema_guard, trans,
                                       AUX_LOB_META))) {
      LOG_WARN("purge_aux_lob_meta_table failed", K(ret), K(table_schema));
    } else if (OB_FAIL(purge_aux_table(table_schema, schema_guard, trans,
                                       AUX_LOB_PIECE))) {
      LOG_WARN("purge_aux_lob_piece_table failed", K(ret), K(table_schema));
    } else if (OB_FAIL(purge_table_trigger(table_schema, schema_guard, trans))) {
      LOG_WARN("purge_trigger failed", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(purge_table_in_recyclebin(table_schema,
                                          trans,
                                          ddl_stmt_str))) {
      LOG_WARN("purge table failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::purge_aux_table(
    const ObTableSchema &table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObMySQLTransaction &trans,
    const ObTableType table_type)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  ObSEArray<uint64_t, 16> aux_tid_array; // for aux_vp or aux_lob
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  bool is_index = false;
  if (USER_INDEX == table_type) {
    is_index = true;
    if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get_simple_index_infos failed", K(ret), K(table_schema));
    }
  } else if (AUX_LOB_META == table_type) {
    const uint64_t aux_lob_meta_tid = table_schema.get_aux_lob_meta_tid();
    if (OB_INVALID_ID != aux_lob_meta_tid && OB_FAIL(aux_tid_array.push_back(aux_lob_meta_tid))) {
      LOG_WARN("push back aux_lob_meta_tid failed", K(ret));
    }
  } else if (AUX_LOB_PIECE == table_type) {
    const uint64_t aux_lob_piece_tid = table_schema.get_aux_lob_piece_tid();
    if (OB_INVALID_ID != aux_lob_piece_tid && OB_FAIL(aux_tid_array.push_back(aux_lob_piece_tid))) {
      LOG_WARN("push back aux_lob_piece_tid failed", K(ret));
    }
  } else if (AUX_VERTIAL_PARTITION_TABLE == table_type) {
    if (OB_FAIL(table_schema.get_aux_vp_tid_array(aux_tid_array))) {
      LOG_WARN("get_aux_vp_tid_array failed", K(ret), K(table_schema));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table type", K(ret), K(table_type));
  }

  int64_t N = is_index ? simple_index_infos.count() : aux_tid_array.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObTableSchema *aux_table_schema = NULL;
    uint64_t tid = is_index ? simple_index_infos.at(i).table_id_ : aux_tid_array.at(i);
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, tid, aux_table_schema))) {
      LOG_WARN("get_table_schema failed", K(tenant_id), "table id", tid, K(ret));
    } else if (OB_ISNULL(aux_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else if (OB_FAIL(purge_table_in_recyclebin(*aux_table_schema,
                                                 trans,
                                                 NULL /*ddl_stmt_str*/))) {
      LOG_WARN("ddl_operator drop_table failed", K(*aux_table_schema), K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::purge_table_in_recyclebin(const ObTableSchema &table_schema,
                                             ObMySQLTransaction &trans,
                                             const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObArray<ObRecycleObject> recycle_objs;
  ObRecycleObject::RecycleObjType recycle_type = ObRecycleObject::get_type_by_table_schema(table_schema);

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_INVALID_ID == table_schema.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else if (OB_FAIL(schema_service->fetch_recycle_object(
             table_schema.get_tenant_id(),
             table_schema.get_table_name_str(),
             recycle_type,
             trans,
             recycle_objs))) {
    LOG_WARN("get_recycle_object failed", K(recycle_type), K(ret));
  } else if (recycle_objs.size() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected recycle object num", K(ret), K(recycle_objs.size()));
  } else if (OB_FAIL(schema_service->delete_recycle_object(
             table_schema.get_tenant_id(),
             recycle_objs.at(0),
             trans))) {
    LOG_WARN("delete_recycle_object failed", K(ret), "ObRecycleObject", recycle_objs.at(0));
  } else if (OB_FAIL(drop_table(table_schema, trans, ddl_stmt_str, false))) {
    LOG_WARN("drop table failed", K(ret));
  }
  return ret;
}

int ObDDLOperator::create_index_in_recyclebin(ObTableSchema &table_schema,
                                              ObSchemaGetterGuard &schema_guard,
                                              ObMySQLTransaction &trans,
                                              const ObString *ddl_stmt_str) {
  int ret = OB_SUCCESS;

  if (table_schema.get_table_type() != USER_INDEX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema type is not index", K(ret));
  } else {
    ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
    uint64_t tenant_id = table_schema.get_tenant_id();
    int64_t new_schema_version = OB_INVALID_VERSION;
    bool recycle_db_exist = false;
    if (OB_ISNULL(schema_service_impl)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("schema_service_impl must not be null", K(ret));
    } else if (OB_FAIL(schema_guard.check_database_exist(tenant_id,
               OB_RECYCLEBIN_SCHEMA_ID, recycle_db_exist))) {
      LOG_WARN("check database exist failed", K(ret));
    } else if (!recycle_db_exist) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("__recyclebin db not exist", K(ret));
    } else {
      ObSqlString new_table_name;
      ObTableSchema new_table_schema;
      if (OB_FAIL(new_table_schema.assign(table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else {
        new_table_schema.set_database_id(OB_RECYCLEBIN_SCHEMA_ID);
        new_table_schema.set_tablegroup_id(OB_INVALID_ID);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(construct_new_name_for_recyclebin(table_schema, new_table_name))) {
        LOG_WARN("failed to construct new name for table", K(ret));
      } else if (OB_FAIL(new_table_schema.set_table_name(new_table_name.string()))) {
        LOG_WARN("failed to set new table name!", K(new_table_name), K(table_schema), K(ret));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else {
        new_table_schema.set_schema_version(new_schema_version);
        ObRecycleObject recycle_object;
        recycle_object.set_object_name(new_table_name.string());
        recycle_object.set_original_name(table_schema.get_table_name_str());
        recycle_object.set_tenant_id(tenant_id);
        recycle_object.set_database_id(table_schema.get_database_id());
        recycle_object.set_table_id(table_schema.get_table_id());
        recycle_object.set_tablegroup_id(table_schema.get_tablegroup_id());
        if (OB_FAIL(recycle_object.set_type_by_table_schema(table_schema))) {
          LOG_WARN("set type by table schema failed", K(ret));
        } else if (OB_FAIL(schema_service_impl->insert_recyclebin_object(recycle_object,
                trans))) {
          LOG_WARN("insert recycle object failed", K(ret));
        } else if (OB_FAIL(schema_service_impl->get_table_sql_service().create_table(
                new_table_schema,
                trans,
                ddl_stmt_str,
                true,
                true))) {
          LOG_WARN("failed to create table in recyclebin", K(ret));
        } else if (OB_FAIL(sync_version_for_cascade_table(
                   tenant_id, new_table_schema.get_depend_table_ids(), trans))) {
          LOG_WARN("fail to sync cascade depend table", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_tablegroup_id_of_tables(const ObDatabaseSchema &database_schema,
                                                  ObMySQLTransaction &trans,
                                                  ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = database_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObArray<const ObTableSchema*> table_schemas;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_FAIL(schema_guard.get_table_schemas_in_database(tenant_id,
                                                         database_schema.get_database_id(),
                                                         table_schemas))) {
    LOG_WARN("get_table_schemas_in_database failed", K(ret), K(tenant_id));
  }
  bool tg_exist = false;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < table_schemas.count(); ++idx) {
    const ObTableSchema *table = table_schemas.at(idx);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else if (table->is_index_table()) {
      continue;
    } else if (OB_INVALID_ID != table->get_tablegroup_id() &&
        OB_FAIL(schema_guard.check_tablegroup_exist(table->get_tenant_id(),
                                                    table->get_tablegroup_id(), tg_exist))) {
      LOG_WARN("check_tablegroup_exist failed", K(ret), KPC(table));
    } else {
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      if (OB_FAIL(table->get_simple_index_infos(simple_index_infos))) {
        LOG_WARN("get_index_tid_array failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        const ObTableSchema *index_table_schema = NULL;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
            simple_index_infos.at(i).table_id_, index_table_schema))) {
          LOG_WARN("get_table_schema failed", K(tenant_id),
                   "table id", simple_index_infos.at(i).table_id_, K(ret));
        } else if (OB_ISNULL(index_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema should not be null", K(ret));
        } else {
          ObTableSchema new_index_schema;
          if (OB_FAIL(new_index_schema.assign(*index_table_schema))) {
            LOG_WARN("fail to assign schema", K(ret));
          } else {
            if (!tg_exist) {
              new_index_schema.set_tablegroup_id(OB_INVALID_ID);
            }
            if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
              LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
            } else if (FALSE_IT(new_index_schema.set_schema_version(new_schema_version))) {
            } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                trans, *index_table_schema, new_index_schema,
                OB_DDL_FLASHBACK_TABLE, NULL))) {
              LOG_WARN("update_table_option failed", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        HEAP_VAR(ObTableSchema, new_ts) {
          if (OB_FAIL(new_ts.assign(*table))) {
            LOG_WARN("fail to assign schema", K(ret));
          } else {
            if (!tg_exist) {
              new_ts.set_tablegroup_id(OB_INVALID_ID);
            }
            const ObSchemaOperationType op_type = new_ts.is_view_table()
                ? OB_DDL_FLASHBACK_VIEW : OB_DDL_FLASHBACK_TABLE;
            if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
                LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
            } else if (FALSE_IT(new_ts.set_schema_version(new_schema_version))) {
            } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                trans, *table, new_ts, op_type, NULL))) {
              LOG_WARN("update_table_option failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::flashback_database_from_recyclebin(const ObDatabaseSchema &database_schema,
                                                      ObMySQLTransaction &trans,
                                                      const ObString &new_db_name,
                                                      ObSchemaGetterGuard &schema_guard,
                                                      const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObArray<ObRecycleObject> recycle_objs;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_INVALID_ID == database_schema.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else if (OB_FAIL(schema_service->fetch_recycle_object(
      database_schema.get_tenant_id(),
      database_schema.get_database_name(),
      ObRecycleObject::DATABASE,
      trans,
      recycle_objs))) {
    LOG_WARN("get_recycle_object failed", K(ret));
  } else if (recycle_objs.size() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected recycle object num", K(ret));
  } else {
    const ObRecycleObject &recycle_obj = recycle_objs.at(0);
    uint64_t tg_id = OB_INVALID_ID;
    if (OB_INVALID_ID != recycle_obj.get_tablegroup_id()) {
      bool tg_exist = false;
      if (OB_FAIL(schema_guard.check_tablegroup_exist(recycle_obj.get_tenant_id(),
                                                      recycle_obj.get_tablegroup_id(),
                                                      tg_exist))) {
        LOG_WARN("check_tablegroup_exist failed", K(ret), K(recycle_obj));
      } else if (tg_exist) {
        tg_id = recycle_obj.get_tablegroup_id();
      }
    }
    if (OB_SUCC(ret)) {
      ObDatabaseSchema new_db_schema = database_schema;
      new_db_schema.set_in_recyclebin(false);
      new_db_schema.set_default_tablegroup_id(tg_id);
      if (!new_db_name.empty()) {
        if (OB_FAIL(new_db_schema.set_database_name(new_db_name))) {
          LOG_WARN("set database name failed", K(new_db_name));
        }
      } else {
        //set original db_id
        if (OB_FAIL(new_db_schema.set_database_name(recycle_obj.get_original_name()))) {
          LOG_WARN("set database name failed", K(recycle_obj));
        }
      }
      if (OB_SUCC(ret)) {
        bool is_database_exist = true;
        const uint64_t tenant_id = database_schema.get_tenant_id();
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(schema_guard.check_database_exist(database_schema.get_tenant_id(),
                                                      new_db_schema.get_database_name_str(),
                                                      is_database_exist))) {
          LOG_WARN("check database exist failed", K(ret), K(new_db_schema));
        } else if (is_database_exist) {
          ret = OB_DATABASE_EXIST;
          LOG_USER_ERROR(OB_DATABASE_EXIST, new_db_schema.get_database_name_str().length(),
                         new_db_schema.get_database_name_str().ptr());
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (FALSE_IT(new_db_schema.set_schema_version(new_schema_version))) {
        } else if (OB_FAIL(schema_service->get_database_sql_service().update_database(
            new_db_schema,
            trans,
            OB_DDL_FLASHBACK_DATABASE,
            &ddl_stmt_str))) {
          LOG_WARN("update_database failed", K(ret), K(new_db_schema));
        } else if (OB_FAIL(schema_service->delete_recycle_object(
            database_schema.get_tenant_id(),
            recycle_obj,
            trans))) {
          LOG_WARN("delete_recycle_object failed", K(ret), K(recycle_obj));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_tablegroup_id_of_tables(database_schema, trans, schema_guard))) {
        LOG_WARN("update tablegroup_id of tables failed", K(database_schema), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::purge_table_of_database(const ObDatabaseSchema &db_schema,
                                           ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = db_schema.get_tenant_id();
  const uint64_t database_id = db_schema.get_database_id();
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else {
    ObArray<ObRecycleObject> recycle_objs;
    if (OB_FAIL(schema_service->fetch_recycle_objects_of_db(tenant_id,
                                                            database_id,
                                                            trans,
                                                            recycle_objs))) {
      LOG_WARN("fetch recycle objects of db failed", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < recycle_objs.count(); ++i) {
        const ObRecycleObject &recycle_obj = recycle_objs.at(i);
        const ObTableSchema* table_schema = NULL;
        if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
          LOG_WARN("failed to get schema guard", K(ret));
        } else if (OB_FAIL(schema_guard.get_table_schema(recycle_obj.get_tenant_id(),
                                                         recycle_obj.get_table_id(),
                                                         table_schema))) {
          LOG_WARN("fail to get table_schema", KR(ret), K(recycle_obj));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table is not exist", K(ret), K(recycle_obj));
          LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(db_schema.get_database_name_str()),
                         to_cstring(recycle_obj.get_object_name()));
        } else if (OB_FAIL(purge_table_with_aux_table(*table_schema,
                                                      schema_guard,
                                                      trans,
                                                      NULL /*ddl_stmt_str */))) {
          LOG_WARN("purge table with index failed", K(ret), K(recycle_obj));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::purge_database_in_recyclebin(const ObDatabaseSchema &database_schema,
                                                ObMySQLTransaction &trans,
                                                const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_INVALID_ID == database_schema.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else {
    ObArray<ObRecycleObject> recycle_objs;
    if (OB_FAIL(schema_service->fetch_recycle_object(
       database_schema.get_tenant_id(),
       database_schema.get_database_name_str(),
       ObRecycleObject::DATABASE,
       trans,
       recycle_objs))) {
       LOG_WARN("get_recycle_object failed", K(ret));
     } else if (1 != recycle_objs.size()) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("unexpected recycle object num", K(ret));
     } else if (OB_FAIL(drop_database(database_schema,
                                      trans,
                                      ddl_stmt_str))) {
       LOG_WARN("drop_table failed", K(ret));
     } else if (OB_FAIL(schema_service->delete_recycle_object(
         database_schema.get_tenant_id(),
         recycle_objs.at(0),
         trans))) {
       LOG_WARN("delete_recycle_object failed", K(ret));
     }
  }
  return ret;
}

int ObDDLOperator::fetch_expire_recycle_objects(
    const uint64_t tenant_id,
    const int64_t expire_time,
    ObIArray<ObRecycleObject> &recycle_objs)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_expire_recycle_objects(tenant_id,
                                                          expire_time,
                                                          sql_proxy_,
                                                          recycle_objs))) {
    LOG_WARN("fetch expire recycle objects failed", K(ret),
             K(expire_time), K(tenant_id));
  }
  return ret;
}

int ObDDLOperator::init_tenant_env(
    const ObTenantSchema &tenant_schema,
    const ObSysVariableSchema &sys_variable,
    const share::ObTenantRole &tenant_role,
    const SCN &recovery_until_scn,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();

  if (OB_UNLIKELY(!recovery_until_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid recovery_until_scn", KR(ret), K(recovery_until_scn));
  } else if (OB_FAIL(init_tenant_tablegroup(tenant_id, trans))) {
    LOG_WARN("insert default tablegroup failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_databases(tenant_schema, sys_variable, trans))) {
    LOG_WARN("insert default databases failed,", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_profile(tenant_id, sys_variable, trans))) {
    LOG_WARN("fail to init tenant profile", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_users(tenant_schema, sys_variable, trans))) {
    LOG_WARN("insert default user failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_keystore(tenant_id, sys_variable, trans))) {
    LOG_WARN("fail to init tenant keystore", K(ret));
  } else if (OB_FAIL(init_tenant_sys_stats(tenant_id, trans))) {
    LOG_WARN("insert default sys stats failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_freeze_info(tenant_id, trans))) {
    LOG_WARN("insert freeze info failed", K(tenant_id), KR(ret));
  } else if (OB_FAIL(init_tenant_srs(tenant_id, trans))) {
    LOG_WARN("insert tenant srs failed", K(tenant_id), K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(init_sys_tenant_charset(trans))) {
      LOG_WARN("insert charset failed", K(tenant_id), K(ret));
    } else if (OB_FAIL(init_sys_tenant_collation(trans))) {
      LOG_WARN("insert collation failed", K(tenant_id), K(ret));
    } else if (OB_FAIL(init_sys_tenant_privilege(trans))) {
      LOG_WARN("insert privilege failed", K(tenant_id), K(ret));
    }
    //TODO [profile]
  }
  if (OB_SUCC(ret) && !is_user_tenant(tenant_id)) {
    uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
    if (OB_FAIL(init_tenant_config(tenant_id, init_configs, trans))) {
      LOG_WARN("insert tenant config failed", KR(ret), K(tenant_id));
    } else if (is_meta_tenant(tenant_id)
               && OB_FAIL(init_tenant_config(user_tenant_id, init_configs, trans))) {
      LOG_WARN("insert tenant config failed", KR(ret), K(user_tenant_id));
    }
  }

  if (OB_SUCC(ret) && is_meta_tenant(tenant_id)) {
    const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
    ObAllTenantInfo tenant_info;
    if (OB_FAIL(tenant_info.init(user_tenant_id, tenant_role, NORMAL_SWITCHOVER_STATUS, 0,
                SCN::base_scn(), SCN::base_scn(), SCN::base_scn(), recovery_until_scn))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_id), K(tenant_role));
    } else if (OB_FAIL(ObAllTenantInfoProxy::init_tenant_info(tenant_info, &trans))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_info));
    }
  }

  return ret;
}

int ObDDLOperator::init_tenant_tablegroup(const uint64_t tenant_id,
                                          ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    ObTablegroupSchema tg_schema;
    tg_schema.set_tenant_id(tenant_id);
    tg_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
    tg_schema.set_tablegroup_name(OB_SYS_TABLEGROUP_NAME);
    tg_schema.set_comment("system tablegroup");
    tg_schema.set_schema_version(OB_CORE_SCHEMA_VERSION);
    tg_schema.set_part_level(PARTITION_LEVEL_ZERO);
    tg_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(tg_schema.set_sharding(OB_PARTITION_SHARDING_ADAPTIVE))) {
      LOG_WARN("set sharding failed", K(ret), K(tg_schema));
    } else if (OB_FAIL(schema_service->get_tablegroup_sql_service().insert_tablegroup(tg_schema, trans))) {
      LOG_WARN("insert_tablegroup failed", K(tg_schema), K(ret));
    }
  }
  LOG_INFO("init tenant tablegroup", K(ret), K(tenant_id),
           "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::init_tenant_database(const ObTenantSchema &tenant_schema,
                                        const ObString &db_name,
                                        const uint64_t pure_db_id,
                                        const ObString &db_comment,
                                        ObMySQLTransaction &trans,
                                        const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (db_name.empty() || OB_INVALID_ID == pure_db_id || db_comment.empty()) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(db_name), K(pure_db_id), K(db_comment), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    ObSchemaService *schema_service = schema_service_.get_schema_service();
    ObDatabaseSchema db_schema;
    db_schema.set_tenant_id(tenant_id);
    db_schema.set_database_id(pure_db_id);
    db_schema.set_database_name(db_name);
    db_schema.set_comment(db_comment);
    db_schema.set_schema_version(new_schema_version);
    if (db_name == OB_RECYCLEBIN_SCHEMA_NAME
        || db_name == OB_PUBLIC_SCHEMA_NAME) {
      db_schema.set_read_only(true);
    }

    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_SYS;
      RS_LOG(ERROR, "schema_service must not null");
    } else if (OB_FAIL(ObSchema::set_charset_and_collation_options(tenant_schema.get_charset_type(),
                                                                   tenant_schema.get_collation_type(),
                                                                   db_schema))) {
      RS_LOG(WARN, "set charset and collation options failed", K(ret));
    } else if (OB_FAIL(schema_service->get_database_sql_service().insert_database(db_schema, trans))) {
      RS_LOG(WARN, "insert_database failed", K(db_schema), K(ret));
    }
  }

  // init database priv
  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = tenant_schema.get_tenant_id();
    ObOriginalDBKey db_key;
    db_key.tenant_id_ = tenant_id;
    db_key.user_id_ = is_oracle_mode ? OB_ORA_SYS_USER_ID : OB_SYS_USER_ID;
    db_key.db_ = db_name;

    ObSchemaService *schema_service = schema_service_.get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_SYS;
      RS_LOG(ERROR, "schema_service must not null");
    } else {
      ObSqlString ddl_stmt_str;
      ObString ddl_sql;
      ObNeedPriv need_priv;
      need_priv.db_ = db_name;
      need_priv.priv_set_ = OB_PRIV_DB_ACC;//is collect?
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      if (OB_FAIL(ObDDLSqlGenerator::gen_db_priv_sql(ObAccountArg(is_oracle_mode ? OB_ORA_SYS_USER_NAME : OB_SYS_USER_NAME,
                                                     OB_SYS_HOST_NAME),
                                                     need_priv,
                                                     true, /*is_grant*/
                                                     ddl_stmt_str))) {
        LOG_WARN("gen db priv sql failed", K(ret));
      } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_service->get_priv_sql_service().grant_database(
          db_key, OB_PRIV_DB_ACC, new_schema_version, &ddl_sql, trans))) {
        RS_LOG(WARN, "insert database privilege failed, ", K(ret));
      }
    }
  }
  LOG_INFO("init tenant database", K(ret),
           "tenant_id", tenant_schema.get_tenant_id(),
           "database_name", db_name,
           "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::init_tenant_databases(const ObTenantSchema &tenant_schema,
                                         const ObSysVariableSchema &sys_variable,
                                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const bool is_sys = OB_SYS_TENANT_ID == tenant_id;
  ObString oceanbase_schema(OB_SYS_DATABASE_NAME);
  ObString mysql_schema(OB_MYSQL_SCHEMA_NAME);
  ObString information_schema(OB_INFORMATION_SCHEMA_NAME);
  ObString recyclebin_schema(OB_RECYCLEBIN_SCHEMA_NAME);
  ObString public_schema(OB_PUBLIC_SCHEMA_NAME);
  ObString test_schema(OB_TEST_SCHEMA_NAME);
  ObString ora_sys_schema(OB_ORA_SYS_USER_NAME);
  ObString ora_lbacsys_schema(OB_ORA_LBACSYS_NAME);
  ObString ora_auditor_schema(OB_ORA_AUDITOR_NAME);
  bool is_oracle_mode = false;
  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", K(ret));
  } else if (OB_FAIL(init_tenant_database(tenant_schema, oceanbase_schema,
                                   OB_SYS_DATABASE_ID, "system database",
                                   trans, is_oracle_mode))) {
    RS_LOG(WARN, "insert default database failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_database(tenant_schema, recyclebin_schema,
                                          OB_RECYCLEBIN_SCHEMA_ID, "recyclebin schema",
                                          trans, is_oracle_mode))) {
    RS_LOG(WARN, "insert recyclebin schema failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_database(tenant_schema, public_schema,
                                          OB_PUBLIC_SCHEMA_ID, "public schema",
                                          trans, is_oracle_mode))) {
    RS_LOG(WARN, "insert public schema failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_optimizer_stats_info(sys_variable, tenant_id, trans))) {
    RS_LOG(WARN, "init tenant tenant optimizer control table", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_spm_configure(tenant_id, trans))) {
    RS_LOG(WARN, "init tenant spm configure failed", K(tenant_id), K(ret));
  } else {
    if (!is_oracle_mode) {
      if (OB_FAIL(init_tenant_database(tenant_schema, mysql_schema,
                                       OB_MYSQL_SCHEMA_ID, "MySql schema",
                                       trans, false))) {
        RS_LOG(WARN, "insert information_schema failed", K(tenant_id), K(ret));
      } else if (OB_FAIL(init_tenant_database(tenant_schema, information_schema,
                                              OB_INFORMATION_SCHEMA_ID, "information_schema",
                                              trans, false))) {
        RS_LOG(WARN, "insert mysql schema failed", K(tenant_id), K(ret));
      } else if (OB_FAIL(init_tenant_database(tenant_schema, test_schema,
                                              OB_INITIAL_TEST_DATABASE_ID, "test schema",
                                              trans, is_oracle_mode))) {
        RS_LOG(WARN, "insert test schema failed", K(tenant_id), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_oracle_mode || is_sys) {
      if (OB_FAIL(init_tenant_database(tenant_schema, ora_sys_schema,
          OB_ORA_SYS_DATABASE_ID, "oracle sys schema", trans, is_oracle_mode))) {
        RS_LOG(WARN, "insert oracle sys schema failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(init_tenant_database(tenant_schema, ora_lbacsys_schema,
          OB_ORA_LBACSYS_DATABASE_ID, "oracle sys schema", trans, is_oracle_mode))) {
        RS_LOG(WARN, "insert oracle lbacsys schema failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(init_tenant_database(tenant_schema, ora_auditor_schema,
          OB_ORA_AUDITOR_DATABASE_ID, "oracle sys schema", trans, is_oracle_mode))) {
        RS_LOG(WARN, "insert oracle lbacsys schema failed", K(ret), K(tenant_id));
      }
    }
  }

  return ret;
}

int ObDDLOperator::init_tenant_optimizer_stats_info(const ObSysVariableSchema &sys_variable,
                                                    uint64_t tenant_id,
                                                    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString prefs_sql;
  ObSqlString jobs_sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t expected_affected_rows1 = 0;
  int64_t expected_affected_rows2 = 0;
  int64_t affected_rows1 = 0;
  int64_t affected_rows2 = 0;
  if (OB_FAIL(ObDbmsStatsPreferences::gen_init_global_prefs_sql(prefs_sql,
                                                                false,
                                                                &expected_affected_rows1))) {
    LOG_WARN("failed gen init global prefs sql", K(ret), K(prefs_sql));
  } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::get_stats_maintenance_window_jobs_sql(
                                                                        sys_variable,
                                                                        tenant_id,
                                                                        jobs_sql,
                                                                        expected_affected_rows2))) {
    LOG_WARN("failed tto get stats maintenance window jobs sql", K(ret), K(jobs_sql));
  } else if (OB_UNLIKELY(prefs_sql.empty() || jobs_sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "get unexpected empty", K(ret), K(prefs_sql), K(jobs_sql));
  } else if (OB_FAIL(trans.write(exec_tenant_id, prefs_sql.ptr(), affected_rows1)) ||
             OB_FAIL(trans.write(exec_tenant_id, jobs_sql.ptr(), affected_rows2))) {
    RS_LOG(WARN, "execute sql failed", K(ret), K(prefs_sql), K(jobs_sql));
  } else if (OB_UNLIKELY(affected_rows1 != expected_affected_rows1 ||
                         affected_rows2 != expected_affected_rows2)) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "get unexpected affected_rows", K(ret), K(affected_rows1), K(affected_rows2),
                                           K(expected_affected_rows1), K(expected_affected_rows2));
  } else {/*do nothing*/}
  return ret;
}

int ObDDLOperator::init_tenant_spm_configure(uint64_t tenant_id,
                                             ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t extract_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (tenant_id, name, value) VALUES ",
                             OB_ALL_SPM_CONFIG_TNAME))) {
    RS_LOG(WARN, "sql assign failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("(%lu, \"%s\", NULL),", extract_tenant_id, "AUTO_CAPTURE_ACTION"))) {
    RS_LOG(WARN, "sql append failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("(%lu, \"%s\", NULL),", extract_tenant_id, "AUTO_CAPTURE_MODULE"))) {
    RS_LOG(WARN, "sql append failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("(%lu, \"%s\", NULL),", extract_tenant_id, "AUTO_CAPTURE_PARSING_SCHEMA_NAME"))) {
    RS_LOG(WARN, "sql append failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("(%lu, \"%s\", NULL),", extract_tenant_id, "AUTO_CAPTURE_SQL_TEXT"))) {
    RS_LOG(WARN, "sql append failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("(%lu, \"%s\", \"%s\"),", extract_tenant_id, "AUTO_SPM_EVOLVE_TASK", "OFF"))) {
    RS_LOG(WARN, "sql append failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("(%lu, \"%s\", \"%s\"),", extract_tenant_id, "AUTO_SPM_EVOLVE_TASK_INTERVAL", "3600"))) {
    RS_LOG(WARN, "sql append failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("(%lu, \"%s\", \"%s\"),", extract_tenant_id, "AUTO_SPM_EVOLVE_TASK_MAX_RUNTIME", "1800"))) {
    RS_LOG(WARN, "sql append failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("(%lu, \"%s\", \"%s\"),", extract_tenant_id, "SPACE_BUDGET_PERCENT", "10"))) {
    RS_LOG(WARN, "sql append failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("(%lu, \"%s\", \"%s\");", extract_tenant_id, "PLAN_RETENTION_WEEKS", "53"))) {
    RS_LOG(WARN, "sql append failed", K(ret));
  } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    RS_LOG(WARN, "execute sql failed", K(ret), K(sql));
  } else {/*do nothing*/}
  return ret;
}

/*
 * The following system permissions are not granted to dba and need to be extracted from the complete set of permissions
-----------------------------------------
ADMINISTER KEY MANAGEMENT
ALTER DATABASE LINK
ALTER PUBLIC DATABASE LINK
EXEMPT ACCESS POLICY
EXEMPT IDENTITY POLICY
EXEMPT REDACTION POLICY
INHERIT ANY PRIVILEGES
INHERIT ANY REMOTE PRIVILEGES
KEEP DATE TIME
KEEP SYSGUID
PURGE DBA_RECYCLEBIN
SYSBACKUP
SYSDBA
SYSDG
SYSKM
SYSOPER
SYSRAC
TRANSLATE ANY SQL
UNLIMITED TABLESPACE
------------------------------------------
resource role, pre define sys priv;
RESOURCE CREATE TABLE                             NO  YES YES
RESOURCE CREATE OPERATOR                          NO  YES YES
RESOURCE CREATE TYPE                              NO  YES YES
RESOURCE CREATE CLUSTER                           NO  YES YES
RESOURCE CREATE TRIGGER                           NO  YES YES
RESOURCE CREATE INDEXTYPE                         NO  YES YES
RESOURCE CREATE PROCEDURE                         NO  YES YES
RESOURCE CREATE SEQUENCE                          NO  YES YES*/
int ObDDLOperator::build_raw_priv_info_inner_user(
    uint64_t grantee_id,
    ObRawPrivArray &raw_priv_array,
    uint64_t &option)
{
  int ret = OB_SUCCESS;
  if (is_ora_dba_role(grantee_id) || is_ora_sys_user(grantee_id)) {
    option = ADMIN_OPTION;
    for (int i = PRIV_ID_NONE + 1; i < PRIV_ID_MAX && OB_SUCC(ret); i++) {
      if (i != PRIV_ID_EXEMPT_RED_PLY
          && i != PRIV_ID_SYSDBA
          && i != PRIV_ID_SYSOPER
          && i != PRIV_ID_SYSBACKUP
          && i != PRIV_ID_EXEMPT_ACCESS_POLICY) {
        OZ (raw_priv_array.push_back(i));
      }
    }
  } else if (is_ora_resource_role(grantee_id)) {
    option = NO_OPTION;
    OZ (raw_priv_array.push_back(PRIV_ID_CREATE_TABLE));
    OZ (raw_priv_array.push_back(PRIV_ID_CREATE_TYPE));
    OZ (raw_priv_array.push_back(PRIV_ID_CREATE_TRIG));
    OZ (raw_priv_array.push_back(PRIV_ID_CREATE_PROC));
    OZ (raw_priv_array.push_back(PRIV_ID_CREATE_SEQ));
  } else if (is_ora_connect_role(grantee_id) || is_ora_standby_replication_role(grantee_id)) {
    option = NO_OPTION;
    OZ (raw_priv_array.push_back(PRIV_ID_CREATE_SESSION));
  } else if (is_ora_public_role(grantee_id)) {
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("grantee_id error", K(ret), K(grantee_id));
  }
  return ret;
}

int ObDDLOperator::init_inner_user_privs(
    const uint64_t tenant_id,
    ObUserInfo &user,
    ObMySQLTransaction &trans,
    const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t grantee_id = user.get_user_id();
  uint64_t option = NO_OPTION;
  ObRawPrivArray raw_priv_array;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  }
  if (OB_SUCC(ret) && is_oracle_mode) {
    ObString empty_str;
    OZ (build_raw_priv_info_inner_user(grantee_id, raw_priv_array, option), grantee_id);
    OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version), tenant_id);
    OZ (schema_service->get_priv_sql_service().grant_sys_priv_to_ur(tenant_id,
                                                                    grantee_id,
                                                                    option,
                                                                    raw_priv_array,
                                                                    new_schema_version,
                                                                    &empty_str,
                                                                    trans,
                                                                    true /*is_grant*/,
                                                                    false),
        tenant_id, grantee_id, ADMIN_OPTION, raw_priv_array);
    if (is_ora_standby_replication_role(user.get_user_id())) {
      // #define GRANT_OBJ_PRIV_TO_USER(db_name, table_name, table_id, obj_type, priv)
      GRANT_OBJ_PRIV_TO_USER(OB_ORA_SYS_SCHEMA_NAME, OB_DBA_OB_TENANTS_ORA_TNAME, OB_DBA_OB_TENANTS_ORA_TID, ObObjectType::TABLE, SELECT);
      GRANT_OBJ_PRIV_TO_USER(OB_ORA_SYS_SCHEMA_NAME, OB_DBA_OB_ACCESS_POINT_ORA_TNAME, OB_DBA_OB_ACCESS_POINT_ORA_TID, ObObjectType::TABLE, SELECT);
      GRANT_OBJ_PRIV_TO_USER(OB_ORA_SYS_SCHEMA_NAME, OB_DBA_OB_LS_ORA_TNAME, OB_DBA_OB_LS_ORA_TID, ObObjectType::TABLE, SELECT);
      GRANT_OBJ_PRIV_TO_USER(OB_ORA_SYS_SCHEMA_NAME, OB_DBA_OB_LS_HISTORY_ORA_TNAME, OB_DBA_OB_LS_HISTORY_ORA_TID, ObObjectType::TABLE, SELECT);
      GRANT_OBJ_PRIV_TO_USER(OB_ORA_SYS_SCHEMA_NAME, OB_GV_OB_PARAMETERS_ORA_TNAME, OB_GV_OB_PARAMETERS_ORA_TID, ObObjectType::TABLE, SELECT);
      GRANT_OBJ_PRIV_TO_USER(OB_ORA_SYS_SCHEMA_NAME, OB_GV_OB_LOG_STAT_ORA_TNAME, OB_GV_OB_LOG_STAT_ORA_TID, ObObjectType::TABLE, SELECT);
      GRANT_OBJ_PRIV_TO_USER(OB_ORA_SYS_SCHEMA_NAME, OB_GV_OB_UNITS_ORA_TNAME, OB_GV_OB_UNITS_ORA_TID, ObObjectType::TABLE, SELECT);
    }
  }
  return ret;
}

int ObDDLOperator::init_tenant_user(const uint64_t tenant_id,
                                    const ObString &user_name,
                                    const ObString &pwd_raw,
                                    const uint64_t pure_user_id,
                                    const ObString &user_comment,
                                    ObMySQLTransaction &trans,
                                    const bool set_locked,
                                    const bool is_user,
                                    const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  ObString pwd_enc;
  char enc_buf[ENC_BUF_LEN] = {0};
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObUserInfo user;
  user.set_tenant_id(tenant_id);
  pwd_enc.assign_ptr(enc_buf, ENC_BUF_LEN);
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (pwd_raw.length() > 0
             && OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage2(pwd_raw, pwd_enc))) {
    LOG_WARN("Encrypt password failed", K(ret), K(pwd_raw));
  } else if (OB_FAIL(user.set_user_name(user_name))) {
    LOG_WARN("set user name failed", K(ret));
  } else if (OB_FAIL(user.set_host(OB_SYS_HOST_NAME))) {
    LOG_WARN("set host name failed", K(ret));
  } else if (OB_FAIL(user.set_passwd(pwd_enc))) {
    LOG_WARN("set user password failed", K(ret));
  } else if (OB_FAIL(user.set_info(user_comment))) {
    LOG_WARN("set user info failed", K(ret));
  } else {
    user.set_is_locked(set_locked);
    user.set_user_id(pure_user_id);
    if ((!is_oracle_mode || is_user) &&
        pure_user_id != OB_ORA_LBACSYS_USER_ID &&
        pure_user_id != OB_ORA_AUDITOR_USER_ID) {
      user.set_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT);
    }
    user.set_schema_version(OB_CORE_SCHEMA_VERSION);
    user.set_profile_id(OB_INVALID_ID);
    user.set_type((is_user) ? OB_USER : OB_ROLE);
  }
  if (OB_SUCC(ret)) {
    ObSqlString ddl_stmt_str;
    ObString ddl_sql;
    if (OB_FAIL(ObDDLSqlGenerator::gen_create_user_sql(ObAccountArg(user.get_user_name_str(),
                                                       user.get_host_name_str(),
                                                       user.is_role()),
                                                       user.get_passwd_str(),
                                                       ddl_stmt_str))) {
      LOG_WARN("gen create user sql failed", K(user), K(ret));
    } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service->get_user_sql_service().create_user(
                       user, new_schema_version, &ddl_sql, trans))) {
      LOG_WARN("insert user failed", K(user), K(ret));
    } else if ((!is_user || is_ora_sys_user(user.get_user_id()))
               && OB_FAIL(init_inner_user_privs(tenant_id, user, trans, is_oracle_mode))) {
      LOG_WARN("init user privs failed", K(user), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::init_tenant_users(const ObTenantSchema &tenant_schema,
                                     const ObSysVariableSchema &sys_variable,
                                     ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  ObString sys_user_name(OB_SYS_USER_NAME);
  ObString ora_sys_user_name(OB_ORA_SYS_USER_NAME);
  ObString ora_lbacsys_user_name(OB_ORA_LBACSYS_NAME);
  ObString ora_auditor_user_name(OB_ORA_AUDITOR_NAME);
  ObString ora_connect_role_name(OB_ORA_CONNECT_ROLE_NAME);
  ObString ora_resource_role_name(OB_ORA_RESOURCE_ROLE_NAME);
  ObString ora_dba_role_name(OB_ORA_DBA_ROLE_NAME);
  ObString ora_public_role_name(OB_ORA_PUBLIC_ROLE_NAME);
  ObString ora_standby_replication_role_name(OB_ORA_STANDBY_REPLICATION_ROLE_NAME);
  ObString sys_standby_name(OB_STANDBY_USER_NAME);
  char ora_lbacsys_password[ENCRYPT_KEY_LENGTH];
  char ora_auditor_password[ENCRYPT_KEY_LENGTH];
  bool is_oracle_mode = false;
  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    RS_LOG(WARN, "failed to get oracle mode", K(ret), K(tenant_id));
  } else if (is_oracle_mode) {
    // Only retain the three users that is SYS/LBACSYS/ORAAUDITOR in Oracle mode.
    if (OB_FAIL(share::ObKeyGenerator::generate_encrypt_key(ora_lbacsys_password, ENCRYPT_KEY_LENGTH))) {
      RS_LOG(WARN, "failed to generate lbacsys's password", K(ret), K(tenant_id));
    } else if (OB_FAIL(share::ObKeyGenerator::generate_encrypt_key(ora_auditor_password, ENCRYPT_KEY_LENGTH))) {
      RS_LOG(WARN, "failed to generate auditor's password", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id, ora_sys_user_name, ObString(""),
        OB_ORA_SYS_USER_ID, "oracle system administrator", trans, false, true, true))) {
      RS_LOG(WARN, "failed to init oracle sys user", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id, ora_lbacsys_user_name,
        ObString(ENCRYPT_KEY_LENGTH, ora_lbacsys_password),
        OB_ORA_LBACSYS_USER_ID, "oracle system administrator", trans, true, true, true))) {
      RS_LOG(WARN, "failed to init oracle sys user", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id, ora_auditor_user_name,
        ObString(ENCRYPT_KEY_LENGTH, ora_auditor_password),
        OB_ORA_AUDITOR_USER_ID, "oracle system administrator", trans, true, true, true))) {
      RS_LOG(WARN, "failed to init oracle sys user", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id, ora_connect_role_name, ObString(""),
         OB_ORA_CONNECT_ROLE_ID, "oracle connect role", trans, false, false, true))) {
      RS_LOG(WARN, "fail to init oracle connect role", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id, ora_resource_role_name, ObString(""),
         OB_ORA_RESOURCE_ROLE_ID, "oracle resource role", trans, false, false, true))) {
      RS_LOG(WARN, "fail to init oracle resource role", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id, ora_dba_role_name, ObString(""),
         OB_ORA_DBA_ROLE_ID, "oracle dba role", trans, false, false, true))) {
      RS_LOG(WARN, "fail to init oracle dba role", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id, ora_public_role_name, ObString(""),
         OB_ORA_PUBLIC_ROLE_ID, "oracle public role", trans, false, false, true))) {
      RS_LOG(WARN, "fail to init oracle public role", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id, ora_standby_replication_role_name, ObString(""),
         OB_ORA_STANDBY_REPLICATION_ROLE_ID, "oracle standby replication role", trans, false, false, true))) {
      RS_LOG(WARN, "fail to init oracle standby replication role", K(ret), K(tenant_id));
    }
  } else {
    if (OB_FAIL(init_tenant_user(tenant_id, sys_user_name, ObString(""), OB_SYS_USER_ID,
        "system administrator", trans))) {
      RS_LOG(WARN, "failed to init sys user", K(ret), K(tenant_id));
    }
#ifdef OB_BUILD_TDE_SECURITY
    if (OB_SUCC(ret)) {
      if (OB_FAIL(share::ObKeyGenerator::generate_encrypt_key(ora_auditor_password,
                                                              ENCRYPT_KEY_LENGTH))) {
        RS_LOG(WARN, "failed to generate auditor's password", K(ret), K(tenant_id));
      } else if (OB_FAIL(init_tenant_user(tenant_id, ora_auditor_user_name,
                                    ObString(ENCRYPT_KEY_LENGTH, ora_auditor_password),
                                    OB_ORA_AUDITOR_USER_ID, "system administrator", trans, true))) {
        RS_LOG(WARN, "failed to init mysql audit user", K(ret), K(tenant_id));
      }
    }
#endif
  }

  //TODO in standby cluster, temp logical, will be deleted after inner sql ready
//  if (OB_SUCC(ret) && is_sys_tenant(tenant_id)) {
//    const uint64_t user_id = 100;
//    if (OB_FAIL(init_tenant_user(tenant_id, sys_standby_name, "", user_id,
//            "system administrator", trans))) {
//      RS_LOG(WARN, "failed to init sys user", K(ret), K(tenant_id));
//    }
//  }
  return ret;
}

int ObDDLOperator::init_tenant_config(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t tenant_idx = !is_user_tenant(tenant_id) ? 0 : 1;
  if (is_user_tenant(tenant_id) && init_configs.count() == 1) {
    ret = OB_SUCCESS;
    LOG_WARN("no user config", KR(ret), K(tenant_idx), K(tenant_id), K(init_configs));
  } else if (OB_UNLIKELY(
      init_configs.count() < tenant_idx + 1
      || tenant_id != init_configs.at(tenant_idx).get_tenant_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid init_configs", KR(ret), K(tenant_idx), K(tenant_id), K(init_configs));
  } else if (OB_FAIL(init_tenant_config_(tenant_id, init_configs.at(tenant_idx), trans))) {
    LOG_WARN("fail to init tenant config", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_tenant_config_from_seed_(tenant_id, trans))) {
    LOG_WARN("fail to init tenant config from seed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObDDLOperator::init_tenant_config_(
    const uint64_t tenant_id,
    const common::ObConfigPairs &tenant_config,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard hard_code_config(TENANT_CONF(OB_SYS_TENANT_ID));
  int64_t config_cnt = tenant_config.get_configs().count();
  if (OB_UNLIKELY(tenant_id != tenant_config.get_tenant_id() || config_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant config", KR(ret), K(tenant_id), K(tenant_config));
  } else if (!hard_code_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get hard code config", KR(ret), K(tenant_id));
  } else {
    ObDMLSqlSplicer dml;
    ObConfigItem *item = NULL;
    char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "ANY";
    int64_t svr_port = 0;
    int64_t config_version = omt::ObTenantConfig::INITIAL_TENANT_CONF_VERSION + 1;
    FOREACH_X(config, tenant_config.get_configs(), OB_SUCC(ret)) {
      const ObConfigStringKey key(config->key_.ptr());
      if (OB_ISNULL(hard_code_config->get_container().get(key))
          || OB_ISNULL(item = *(hard_code_config->get_container().get(key)))) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("config not exist", KR(ret), KPC(config));
      } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
                 || OB_FAIL(dml.add_pk_column("zone", ""))
                 || OB_FAIL(dml.add_pk_column("svr_type", print_server_role(OB_SERVER)))
                 || OB_FAIL(dml.add_pk_column(K(svr_ip)))
                 || OB_FAIL(dml.add_pk_column(K(svr_port)))
                 || OB_FAIL(dml.add_pk_column("name", config->key_.ptr()))
                 || OB_FAIL(dml.add_column("data_type", item->data_type()))
                 || OB_FAIL(dml.add_column("value", config->value_.ptr()))
                 || OB_FAIL(dml.add_column("info", ""))
                 || OB_FAIL(dml.add_column("config_version", config_version))
                 || OB_FAIL(dml.add_column("section", item->section()))
                 || OB_FAIL(dml.add_column("scope", item->scope()))
                 || OB_FAIL(dml.add_column("source", item->source()))
                 || OB_FAIL(dml.add_column("edit_level", item->edit_level()))) {
        LOG_WARN("fail to add column", KR(ret), K(tenant_id), KPC(config));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret), K(tenant_id), KPC(config));
      }
    } // end foreach
    ObSqlString sql;
    int64_t affected_rows = 0;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    if (FAILEDx(dml.splice_batch_insert_sql(OB_TENANT_PARAMETER_TNAME, sql))) {
      LOG_WARN("fail to generate sql", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
    } else if (config_cnt != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows not match", KR(ret), K(tenant_id), K(config_cnt), K(affected_rows));
    }
  }
  return ret;
}

int ObDDLOperator::init_tenant_config_from_seed_(
    const uint64_t tenant_id,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  ObSqlString sql;
  const static char *from_seed = "select config_version, zone, svr_type, svr_ip, svr_port, name, "
                "data_type, value, info, section, scope, source, edit_level "
                "from __all_seed_parameter";
  ObSQLClientRetryWeak sql_client_retry_weak(&sql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    int64_t expected_rows = 0;
    int64_t config_version = omt::ObTenantConfig::INITIAL_TENANT_CONF_VERSION + 1;
    bool is_first = true;
    if (OB_FAIL(sql_client_retry_weak.read(result, OB_SYS_TENANT_ID, from_seed))) {
      LOG_WARN("read config from __all_seed_parameter failed", K(from_seed), K(ret));
    } else {
      sql.reset();
      if (OB_FAIL(sql.assign_fmt("INSERT IGNORE INTO %s "
          "(TENANT_ID, ZONE, SVR_TYPE, SVR_IP, SVR_PORT, NAME, DATA_TYPE, VALUE, INFO, "
          "SECTION, SCOPE, SOURCE, EDIT_LEVEL, CONFIG_VERSION) VALUES",
          OB_TENANT_PARAMETER_TNAME))) {
        LOG_WARN("sql assign failed", K(ret));
      }

      while (OB_SUCC(ret) && OB_SUCC(result.get_result()->next())) {
        common::sqlclient::ObMySQLResult *rs = result.get_result();
        if (OB_ISNULL(rs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("system config result is null", K(ret));
        } else {
          ObString var_zone, var_svr_type, var_svr_ip, var_name, var_data_type;
          ObString var_value, var_info, var_section, var_scope, var_source, var_edit_level;
          int64_t var_svr_port = 0;
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "zone", var_zone);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "svr_type", var_svr_type);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "svr_ip", var_svr_ip);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "name", var_name);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "data_type", var_data_type);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "value", var_value);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "info", var_info);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "section", var_section);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "scope", var_scope);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "source", var_source);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "edit_level", var_edit_level);
          EXTRACT_INT_FIELD_MYSQL(*rs, "svr_port", var_svr_port, int64_t);
          if (FAILEDx(sql.append_fmt("%s('%lu', '%.*s', '%.*s', '%.*s', %ld, '%.*s', '%.*s', '%.*s',"
              "'%.*s', '%.*s', '%.*s', '%.*s', '%.*s', %ld)",
              is_first ? " " : ", ",
              tenant_id,
              var_zone.length(), var_zone.ptr(),
              var_svr_type.length(), var_svr_type.ptr(),
              var_svr_ip.length(), var_svr_ip.ptr(), var_svr_port,
              var_name.length(), var_name.ptr(),
              var_data_type.length(), var_data_type.ptr(),
              var_value.length(), var_value.ptr(),
              var_info.length(), var_info.ptr(),
              var_section.length(), var_section.ptr(),
              var_scope.length(), var_scope.ptr(),
              var_source.length(), var_source.ptr(),
              var_edit_level.length(), var_edit_level.ptr(), config_version))) {
            LOG_WARN("sql append failed", K(ret));
          }
        }
        expected_rows++;
        is_first = false;
      } // while

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
        if (expected_rows > 0) {
          int64_t affected_rows = 0;
          if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
          } else if (OB_UNLIKELY(affected_rows < 0
                     || expected_rows < affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected affected_rows", KR(ret),  K(expected_rows), K(affected_rows));
          }
        }
      } else {
        LOG_WARN("failed to get result from result set", K(ret));
      }
    } // else
    LOG_INFO("init tenant config", K(ret), K(tenant_id),
               "cost", ObTimeUtility::current_time() - start);
  }
  return ret;

}

int ObDDLOperator::init_freeze_info(const uint64_t tenant_id,
                                    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  ObFreezeInfoProxy freeze_info_proxy(tenant_id);
  ObFreezeInfo frozen_status;
  frozen_status.set_initial_value(DATA_CURRENT_VERSION);
  // init freeze_info in __all_freeze_info
  if (OB_FAIL(freeze_info_proxy.set_freeze_info(trans, frozen_status))) {
    LOG_WARN("fail to set freeze info", KR(ret), K(frozen_status), K(tenant_id));
  }

  LOG_INFO("init freeze info", K(ret), K(tenant_id),
           "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::init_tenant_srs(const uint64_t tenant_id,
                                   ObMySQLTransaction &trans)
{
  // todo : import srs_id 0 in srs mgr init
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t start = ObTimeUtility::current_time();
  int64_t expected_rows = 1;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant version is less than 4.1, spatial reference system");
  } else {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s "
        "(SRS_VERSION, SRS_ID, SRS_NAME, ORGANIZATION, ORGANIZATION_COORDSYS_ID, DEFINITION, minX, maxX, minY, maxY, proj4text, DESCRIPTION) VALUES"
        R"((1, 0, '', NULL, NULL, '', -2147483648,2147483647,-2147483648,2147483647,'', NULL))",
        OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TNAME))) {
      LOG_WARN("sql assign failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (expected_rows != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected_rows", K(expected_rows), K(affected_rows));
      }
    }
  }

  LOG_INFO("init tenant srs", K(ret), K(tenant_id),
           "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::init_tenant_sys_stats(const uint64_t tenant_id,
                                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  ObSysStat sys_stat;
  if (OB_FAIL(sys_stat.set_initial_values(tenant_id))) {
    LOG_WARN("set initial values failed", K(ret));
  } else if (sys_stat.item_list_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not system stat item", KR(ret), K(tenant_id));
  } else if (OB_FAIL(replace_sys_stat(tenant_id, sys_stat, trans))) {
    LOG_WARN("replace system stat failed", K(ret));
  }
  LOG_INFO("init sys stat", K(ret), K(tenant_id),
           "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::replace_sys_stat(const uint64_t tenant_id,
                                    ObSysStat &sys_stat,
                                    ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (sys_stat.item_list_.is_empty()) {
    // skip
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s "
      "(TENANT_ID, ZONE, NAME, DATA_TYPE, VALUE, INFO, gmt_modified) VALUES ",
      OB_ALL_SYS_STAT_TNAME))) {
    LOG_WARN("sql append failed", K(ret));
  } else {
    DLIST_FOREACH_X(it, sys_stat.item_list_, OB_SUCC(ret)) {
      if (OB_ISNULL(it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("it is null", K(ret));
      } else {
        char buf[2L<<10] = "";
        int64_t pos = 0;
        if (OB_FAIL(it->value_.print_sql_literal(
                      buf, sizeof(buf), pos))) {
          LOG_WARN("print obj failed", K(ret), "obj", it->value_);
        } else {
          ObString value(pos, buf);
          uint64_t schema_id = OB_INVALID_ID;
          if (OB_FAIL(ObMaxIdFetcher::str_to_uint(value, schema_id))) {
            LOG_WARN("fail to convert str to uint", K(ret), K(value));
          } else if (FALSE_IT(schema_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema_id))) {
          } else if (OB_FAIL(sql.append_fmt("%s(%lu, '', '%s', %d, '%ld', '%s', now())",
              (it == sys_stat.item_list_.get_first()) ? "" : ", ",
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
              it->name_, it->value_.get_type(),
              static_cast<int64_t>(schema_id),
              it->info_))) {
            LOG_WARN("sql append failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("create system stat sql", K(sql));
      int64_t affected_rows = 0;
      if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (sys_stat.item_list_.get_size() != affected_rows
          && sys_stat.item_list_.get_size() != affected_rows / 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected_rows", K(affected_rows),
            "expected", sys_stat.item_list_.get_size());
      }
    }
  }
  return ret;
}

int ObDDLOperator::init_sys_tenant_charset(ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObCharsetWrapper *charset_wrap_arr = NULL;
  int64_t charset_wrap_arr_len = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("insert into %s "
      "(charset, description, default_collation, max_length) values ",
      OB_ALL_CHARSET_TNAME))) {
    LOG_WARN("sql append failed", K(ret));
  } else {
    ObCharset::get_charset_wrap_arr(charset_wrap_arr, charset_wrap_arr_len);
    if (OB_ISNULL(charset_wrap_arr) ||
        OB_UNLIKELY(ObCharset::VALID_CHARSET_TYPES != charset_wrap_arr_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("charset wrap array is NULL or charset_wrap_arr_len is not CHARSET_WRAPPER_COUNT",
                K(ret), K(charset_wrap_arr), K(charset_wrap_arr_len));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < charset_wrap_arr_len; ++i) {
        ObCharsetWrapper charset_wrap = charset_wrap_arr[i];
        if (OB_FAIL(sql.append_fmt("%s('%s', '%s', '%s', %ld)",
           (0 == i) ? "" : ", ", ObCharset::charset_name(charset_wrap.charset_),
           charset_wrap.description_,
           ObCharset::collation_name(ObCharset::get_default_collation(charset_wrap.charset_)),
                                     charset_wrap.maxlen_))) {
          LOG_WARN("sql append failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("create charset sql", K(sql));
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (charset_wrap_arr_len != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected affected_rows", K(affected_rows),
          "expected", (charset_wrap_arr_len));
    }
  }
  return ret;
}

int ObDDLOperator::init_sys_tenant_collation(ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObCollationWrapper *collation_wrap_arr = NULL;
  int64_t collation_wrap_arr_len = 0;
  ObSqlString sql;
  int64_t total_valid_collations = 0;
  if (OB_FAIL(sql.assign_fmt("insert into %s "
      "(collation, charset, id, `is_default`, is_compiled, sortlen) values ",
      OB_ALL_COLLATION_TNAME))) {
    LOG_WARN("sql append failed", K(ret));
  } else {
    ObCharset::get_collation_wrap_arr(collation_wrap_arr, collation_wrap_arr_len);
    if (OB_ISNULL(collation_wrap_arr) ||
        OB_UNLIKELY(ObCharset::VALID_COLLATION_TYPES != collation_wrap_arr_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("collation wrap array is NULL or collation_wrap_arr_len is not COLLATION_WRAPPER_COUNT",
                K(ret), K(collation_wrap_arr), K(collation_wrap_arr_len));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < collation_wrap_arr_len; ++i) {
        ObCollationWrapper collation_wrap = collation_wrap_arr[i];
        if (CS_TYPE_INVALID != collation_wrap.collation_) {
          if (OB_FAIL(sql.append_fmt("%s('%s', '%s', %ld, '%s', '%s', %ld)",
              (0 == total_valid_collations) ? "" : ", ",
              ObCharset::collation_name(collation_wrap.collation_),
              ObCharset::charset_name(collation_wrap.charset_),
              collation_wrap.id_,
              (true == collation_wrap.default_) ? "Yes" : "",
              (true == collation_wrap.compiled_) ? "Yes" : "",
              collation_wrap.sortlen_))) {
            LOG_WARN("sql append failed", K(ret));
          }
          total_valid_collations++;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("create collation sql", K(sql));
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (total_valid_collations != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected affected_rows", K(affected_rows),
          "expected", (collation_wrap_arr_len));
    }
  }
  return ret;
}

int ObDDLOperator::init_sys_tenant_privilege(ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;

  ObSqlString sql;
  int64_t row_count = 0;
  if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s "
      "(Privilege, Context, Comment) values ",
        OB_ALL_PRIVILEGE_TNAME))) {
    LOG_WARN("sql append failed", K(ret));
  } else {
    const PrivilegeRow *current_row = all_privileges;
    if (OB_ISNULL(current_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current_row is null", K(ret));
    } else {
      bool is_first_row = true;
      for (; OB_SUCC(ret) && NULL != current_row && NULL != current_row->privilege_;
           ++current_row) {
        if (OB_FAIL(sql.append_fmt("%s('%s', '%s', '%s')",
            is_first_row ? "" : ", ",
            current_row->privilege_,
            current_row->context_,
            current_row->comment_))) {
          LOG_WARN("sql append failed", K(ret));
        } else {
          ++row_count;
          is_first_row = false;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("create privileges sql", K(sql));
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (row_count != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected affected_rows", K(affected_rows));
    }
  }

  return ret;
}

//----Functions for managing privileges----
int ObDDLOperator::create_user(
    const share::schema::ObUserInfo &user_info,
    const ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not be null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_sql_service->get_user_sql_service().create_user(
                     user_info, new_schema_version, ddl_stmt_str, trans))) {
    LOG_WARN("Failed to create user", K(user_info), K(ret));
  }
  return ret;
}

int ObDDLOperator::drop_user(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const common::ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama sql service and schema manager must not be null",
              K(schema_sql_service), K(ret));
  }
  //delete user
  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_sql_service->get_user_sql_service().drop_user(tenant_id,
        user_id, new_schema_version, ddl_stmt_str, trans, schema_guard))) {
      LOG_WARN("Drop user from all user table error", K(tenant_id), K(user_id), K(ret));
    }
  }
  //delete db and table privileges of this user
  if (OB_SUCC(ret)) {
    if (OB_FAIL(drop_db_table_privs(tenant_id, user_id, trans))) {
      LOG_WARN("Drop db, table privileges of user error", K(tenant_id), K(user_id), K(ret));
    }
  }

  // oracle mode, if the user has a label security policy, the label granted to the user will be deleted synchronously.
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(drop_all_label_se_user_components(tenant_id, user_id,
                                                         OB_INVALID_ID, trans,
                                                         ObString(), schema_guard))) {
      LOG_WARN("fail to drop user label components cascaded", K(ret));
    }
  }


  // delete audit in user
  if (OB_SUCC(ret)) {
    ObArray<const ObSAuditSchema *> audits;
    ObSchemaGetterGuard schema_guard;
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                              AUDIT_STMT,
                                                              user_id,
                                                              audits))) {
      LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
    } else if (!audits.empty()) {
      common::ObSqlString public_sql_string;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
        const ObSAuditSchema *audit_schema = audits.at(i);
        if (OB_ISNULL(audit_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("audit_schema is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_audit_sql_service().handle_audit_metainfo(
            *audit_schema,
            AUDIT_MT_DEL,
            false,
            new_schema_version,
            NULL,
            trans,
            public_sql_string))) {
          LOG_WARN("drop audit_schema failed",  KPC(audit_schema), K(ret));
        } else {
          LOG_INFO("succ to delete audit_schema from drop user", KPC(audit_schema));
        }
      }
    } else {
      LOG_DEBUG("no need to delete audit_schema from drop user", K(user_id));
    }
  }

  return ret;
}

int ObDDLOperator::drop_db_table_privs(
    const uint64_t tenant_id,
    const uint64_t user_id,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  int64_t ddl_count = 0;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama sql service and schema manager must not be null",
              K(schema_sql_service), K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  }
  // delete database privileges of this user
  if (OB_SUCC(ret)) {
    ObArray<const ObDBPriv *> db_privs;
    if (OB_FAIL(schema_guard.get_db_priv_with_user_id(
        tenant_id, user_id, db_privs))) {
      LOG_WARN("Get database privileges of user to be deleted error",
                K(tenant_id), K(user_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < db_privs.count(); ++i) {
        const ObDBPriv *db_priv = db_privs.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_ISNULL(db_priv)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("db priv is NULL", K(ret), K(db_priv));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().delete_db_priv(
            db_priv->get_original_key(), new_schema_version, trans, schema_guard))) {
          LOG_WARN("Delete database privilege failed", "DB Priv", *db_priv, K(ret));
        }
      }
      ddl_count -= db_privs.count();
    }
  }
  // delete table privileges of this user MYSQL
  if (OB_SUCC(ret)) {
    ObArray<const ObTablePriv *> table_privs;
    if (OB_FAIL(schema_guard.get_table_priv_with_user_id(
                                 tenant_id, user_id, table_privs))) {
      LOG_WARN("Get table privileges of user to be deleted error",
                K(tenant_id), K(user_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_privs.count(); ++i) {
        const ObTablePriv *table_priv = table_privs.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_ISNULL(table_priv)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table priv is NULL", K(ret), K(table_priv));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().delete_table_priv(
            table_priv->get_sort_key(), new_schema_version, trans, schema_guard))) {
          LOG_WARN("Delete table privilege failed", "Table Priv", *table_priv, K(ret));
        }
      }
    }
  }

  // delete column privileges of this user MYSQL
  if (OB_SUCC(ret)) {
    ObArray<const ObColumnPriv *> column_privs;
    if (OB_FAIL(schema_guard.get_column_priv_with_user_id(
                                 tenant_id, user_id, column_privs))) {
      LOG_WARN("Get table privileges of user to be deleted error",
                K(tenant_id), K(user_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_privs.count(); ++i) {
        const ObColumnPriv *column_priv = column_privs.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        ObPrivSet empty_priv = 0;
        ObString dcl_stmt;
        if (OB_ISNULL(column_priv)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table priv is NULL", K(ret), K(column_priv));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_column(
            column_priv->get_sort_key(), column_priv->get_priv_id(), empty_priv,
            new_schema_version, &dcl_stmt, trans, false))) {
          LOG_WARN("Delete table privilege failed", K(column_priv), K(ret));
        }
      }
    }
  }

  // delete oracle table privileges of this user ORACLE
  if (OB_SUCC(ret)) {
    ObArray<const ObObjPriv *> obj_privs;

    OZ (schema_guard.get_obj_priv_with_grantee_id(
                tenant_id, user_id, obj_privs));
    OZ (schema_guard.get_obj_priv_with_grantor_id(
                tenant_id, user_id, obj_privs, false));
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_privs.count(); ++i) {
      const ObObjPriv *obj_priv = obj_privs.at(i);
      int64_t new_schema_version = OB_INVALID_VERSION;
      if (OB_ISNULL(obj_priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("obj_priv priv is NULL", K(ret), K(obj_priv));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().delete_obj_priv(
                 *obj_priv, new_schema_version, trans))) {
        LOG_WARN("Delete obj_priv privilege failed", "obj Priv", *obj_priv, K(ret));
      }
    }
  }

  // delete routine privileges of this user MYSQL
  if (OB_SUCC(ret)) {
    ObArray<const ObRoutinePriv *> routine_privs;
    if (OB_FAIL(schema_guard.get_routine_priv_with_user_id(
                                 tenant_id, user_id, routine_privs))) {
      LOG_WARN("Get table privileges of user to be deleted error",
                K(tenant_id), K(user_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < routine_privs.count(); ++i) {
        const ObRoutinePriv *routine_priv = routine_privs.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        ObPrivSet empty_priv = 0;
        ObString dcl_stmt;
        if (OB_ISNULL(routine_priv)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table priv is NULL", K(ret), K(routine_priv));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_routine(
            routine_priv->get_sort_key(), empty_priv, new_schema_version, &dcl_stmt, trans, 0, false))) {
          LOG_WARN("Delete table privilege failed", K(routine_priv), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::rename_user(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const ObAccountArg &new_account,
    const common::ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const ObUserInfo *user_info = NULL;
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("failed to get user info", K(ret), K(tenant_id), K(user_id));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else {
      ObUserInfo new_user_info;
      if (OB_FAIL(new_user_info.assign(*user_info))) {
        LOG_WARN("assign failed", K(ret));
      } else if (OB_FAIL(new_user_info.set_user_name(new_account.user_name_))) {
        LOG_WARN("set user name failed", K(ret));
      } else if (OB_FAIL(new_user_info.set_host(new_account.host_name_))) {
        LOG_WARN("set user host failed", K(ret));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().rename_user(
                  new_user_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to rename user", K(tenant_id), K(user_id), K(new_account), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::set_passwd(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const common::ObString &passwd,
    const ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("failed to get user info", K(ret));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_user_info;
      if (OB_FAIL(new_user_info.assign(*user_info))) {
        LOG_WARN("assign failed", K(ret));
      } else if (OB_FAIL(new_user_info.set_passwd(passwd))) {
        LOG_WARN("set passwd failed", K(ret));
      } else if (OB_FALSE_IT(new_user_info.set_password_last_changed(ObTimeUtility::current_time()))) {
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().set_passwd(
                        new_user_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to set passwd", K(tenant_id), K(user_id), K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::set_max_connections(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const uint64_t max_connections_per_hour,
    const uint64_t max_user_connections,
    const ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("failed to get user info", K(ret));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_user_info;
      if (OB_FAIL(new_user_info.assign(*user_info))) {
        LOG_WARN("assign failed", K(ret));
      }
      if (OB_SUCC(ret) && OB_INVALID_ID != max_connections_per_hour) {
        new_user_info.set_max_connections(max_connections_per_hour);
      }
      if (OB_SUCC(ret) && OB_INVALID_ID != max_user_connections) {
        new_user_info.set_max_user_connections(max_user_connections);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().set_max_connections(
                        new_user_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to set passwd", K(tenant_id), K(user_id), K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::alter_role(
    const uint64_t tenant_id,
    const uint64_t role_id,
    const common::ObString &passwd,
    const ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == role_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and role_id must not be null", K(tenant_id), K(role_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const ObUserInfo *role_info = NULL;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, role_id, role_info))) {
      LOG_WARN("failed to get role info", K(ret), K(role_id));
    } else if (OB_ISNULL(role_info)) {
      ret = OB_ROLE_NOT_EXIST;
      LOG_WARN("Role not exist", K(ret));
    } else {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_role_info;
      if (OB_FAIL(new_role_info.assign(*role_info))) {
        LOG_WARN("assign failed", K(ret));
      } else if (OB_FAIL(new_role_info.set_passwd(passwd))) {
        LOG_WARN("set passwd failed", K(ret));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().alter_role(
                         new_role_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to alter_role", K(tenant_id), K(role_id), K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::alter_user_default_role(const ObString &ddl_str,
                                           const ObUserInfo &schema,
                                           ObIArray<uint64_t> &role_id_array,
                                           ObIArray<uint64_t> &disable_flag_array,
                                           ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_sql_service = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;

  if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  } else if (!schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(schema.get_tenant_id(),
                                                            new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret));
  } else {
    if (OB_FAIL(schema_sql_service->get_priv_sql_service().alter_user_default_role(
                                                          schema,
                                                          new_schema_version,
                                                          &ddl_str,
                                                          role_id_array,
                                                          disable_flag_array,
                                                          trans))) {
      LOG_WARN("alter user default role failed", K(ret));
    }
  }

  LOG_DEBUG("alter_user_default_role", K(schema));
  return ret;
}

int ObDDLOperator::alter_user_profile(const ObString &ddl_str,
                                      ObUserInfo &schema,
                                      ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_sql_service = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;

  if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  } else if (!schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(schema.get_tenant_id(), new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret));
  } else {
    schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_sql_service->get_user_sql_service().alter_user_profile(schema, &ddl_str, trans))) {
      LOG_WARN("alter user profile failed", K(ret));
    }
  }

  LOG_DEBUG("alter_user_profile", K(schema));
  return ret;
}

int ObDDLOperator::alter_user_require(const uint64_t tenant_id,
    const uint64_t user_id,
    const obrpc::ObSetPasswdArg &arg,
    const common::ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("failed to get user info", K(ret));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_user_info;
      if (OB_FAIL(new_user_info.assign(*user_info))) {
        LOG_WARN("assign failed", K(ret));
      } else {
        new_user_info.set_ssl_type(arg.ssl_type_);
        new_user_info.set_ssl_cipher(arg.ssl_cipher_);
        new_user_info.set_x509_issuer(arg.x509_issuer_);
        new_user_info.set_x509_subject(arg.x509_subject_);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().alter_user_require(
                         new_user_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to alter_user_require", K(tenant_id), K(user_id), K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::grant_revoke_user(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const ObPrivSet priv_set,
    const bool grant,
    const bool is_from_inner_sql,
    const ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    ObPrivSet new_priv = priv_set;

    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info)) ||
        NULL == user_info) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else {
      if (grant) {
        new_priv = priv_set | user_info->get_priv_set();
      } else {
        new_priv = (~priv_set) & user_info->get_priv_set();
      }
      //no matter privilege change or not, write a sql
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_user_info;
      if (OB_FAIL(new_user_info.assign(*user_info))) {
        LOG_WARN("assign failed", K(ret));
      } else {
        new_user_info.set_priv_set(new_priv);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().grant_revoke_user(
                         new_user_info, new_schema_version, ddl_stmt_str, trans, is_from_inner_sql))) {
        LOG_WARN("Failed to grant or revoke user", K(tenant_id), K(user_id), K(grant), K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::lock_user(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const bool locked,
    const ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id is invalid", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info)) ||
          NULL == user_info) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else if (locked != user_info->get_is_locked()) {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_user_info;
      if (OB_FAIL(new_user_info.assign(*user_info))) {
        LOG_WARN("assign failed", K(ret));
      } else {
        new_user_info.set_is_locked(locked);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().lock_user(
                         new_user_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to lock user", K(tenant_id), K(user_id), K(locked), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::grant_database(
    const ObOriginalDBKey &db_priv_key,
    const ObPrivSet priv_set,
    const ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = db_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (!db_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db_priv_key is invalid", K(db_priv_key), K(ret));
  } else if (0 == priv_set) {
    //do nothing
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    ObPrivSet new_priv = priv_set;
    bool need_flush = true;
    ObPrivSet db_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(schema_guard.get_db_priv_set(db_priv_key, db_priv_set, true))) {
      LOG_WARN("get db priv set failed", K(ret));
    } else {
      new_priv |= db_priv_set;
      need_flush = (new_priv != db_priv_set);
      if (need_flush) {
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_database(db_priv_key,
                                                                              new_priv,
                                                                              new_schema_version,
                                                                              ddl_stmt_str,
                                                                              trans))) {
          LOG_WARN("Failed to grant database", K(db_priv_key), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::revoke_database(
    const ObOriginalDBKey &db_priv_key,
    const ObPrivSet priv_set,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = db_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (!db_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db_priv_key is invalid", K(db_priv_key), K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    ObPrivSet db_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(schema_guard.get_db_priv_set(db_priv_key, db_priv_set, true))) {
      LOG_WARN("get db priv set failed", K(ret));
    } else if (OB_PRIV_SET_EMPTY == db_priv_set) {
      ret = OB_ERR_NO_GRANT;
      LOG_WARN("No such grant to revoke", K(db_priv_key), K(ret));
    } else if (0 == priv_set) {
      //do nothing
    } else {
      ObPrivSet new_priv = db_priv_set & (~priv_set);
      if (db_priv_set & priv_set) {
        ObSqlString ddl_stmt_str;
        ObString ddl_sql;
        const ObUserInfo *user_info = NULL;
        ObNeedPriv need_priv;
        need_priv.db_ = db_priv_key.db_;
        need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
        need_priv.priv_set_ = db_priv_set & priv_set; //priv to revoke
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(schema_guard.get_user_info(tenant_id, db_priv_key.user_id_, user_info))) {
          LOG_WARN("get user info failed", K(tenant_id), K(db_priv_key), K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user not exist", K(db_priv_key), K(ret));
        } else if (OB_FAIL(ObDDLSqlGenerator::gen_db_priv_sql(ObAccountArg(user_info->get_user_name_str(), user_info->get_host_name_str()),
                                                              need_priv,
                                                              false, /*is_grant*/
                                                              ddl_stmt_str))) {
          LOG_WARN("gen_db_priv_sql failed", K(ret), K(need_priv));
        } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().revoke_database(
            db_priv_key, new_priv, new_schema_version, &ddl_sql, trans))) {
          LOG_WARN("Failed to revoke database", K(db_priv_key), K(ret));
        }
      }
    }
  }
  return ret;
}

/* According to the current obj priv, check if the user has all the permissions listed in obj_priv_array */
int ObDDLOperator::check_obj_privs_exists(
    ObSchemaGetterGuard &schema_guard,
    const share::schema::ObObjPrivSortKey &obj_priv_key, /* in: obj priv key */
    const ObRawObjPrivArray &obj_priv_array,      /* in: privs to be deleted */
    ObRawObjPrivArray &option_priv_array,         /* out: privs to be deleted cascade */
    bool &is_all)                                 /* out: obj priv array is all privs existed */
{
  int ret = OB_SUCCESS;
  ObPackedObjPriv obj_privs = 0;
  ObRawObjPriv raw_obj_priv = 0;
  bool exists = false;
  uint64_t option_out = false;
  is_all = false;
  int org_n = 0;
  OZ (schema_guard.get_obj_privs(obj_priv_key, obj_privs));
  for (int i = 0; i < obj_priv_array.count() && OB_SUCC(ret); i++) {
    raw_obj_priv = obj_priv_array.at(i);
    OZ (ObOraPrivCheck::raw_obj_priv_exists_with_info(raw_obj_priv,
                                                      obj_privs,
                                                      exists,
                                                      option_out),
        raw_obj_priv, obj_privs, ret);
    if (OB_SUCC(ret)) {
      if (!exists) {
      ret = OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT;
      } else if (option_out == GRANT_OPTION) {
        OZ (option_priv_array.push_back(raw_obj_priv));
      }
    }
  }
  OZ (ObPrivPacker::get_total_obj_privs(obj_privs, org_n));
  OX (is_all = org_n == obj_priv_array.count());
  return ret;
}

/** According to the current obj priv, check if the user has all the permissions listed in obj_priv_array, including the column permissions on the table.
 * check_obj_privs_exists_including_col_privs
 * is used to check the existence of object permissions
 * @param  {ObSchemaGetterGuard &} schema_guard                 : schema_guard
 * @param  {const schema::ObObjPrivSortKey &} obj_priv_key      : object for checking, accurate to the table
 * @param  {const ObRawObjPrivArray &} obj_priv_array           : permission for revoking
 * @param  {ObIArray<schema::ObObjPrivSortKey> &} new_key_array : There may be multiple columns on the obj_priv_key
 *         table object with independent column permissions, so we regenerate all the keys,
 *         which are accurate to the column
 * @param  {ObIArray<ObPackedObjPriv> &} new_packed_privs_array : Corresponds to new_key_array,
 *         permission to revoke
 * @param  {ObIArray<bool> &} is_all                            : Corresponds to new_key_array,
 *         indicating whether the permission to revoke is all the permissions owned by the key
 * @return {int}                                                : ret
 */
int ObDDLOperator::check_obj_privs_exists_including_col_privs(
    ObSchemaGetterGuard &schema_guard,
    const share::schema::ObObjPrivSortKey &obj_priv_key,
    const ObRawObjPrivArray &obj_priv_array,
    ObIArray<share::schema::ObObjPrivSortKey> &new_key_array,
    ObIArray<ObPackedObjPriv> &new_packed_privs_array,
    ObIArray<bool> &is_all)
{
  int ret = OB_SUCCESS;
  ObRawObjPriv raw_obj_priv_to_be_revoked = 0;
  ObPackedObjPriv packed_table_privs = 0;
  ObPackedObjPriv packed_table_privs_to_be_revoked = 0;
  ObSEArray<uint64_t, 4> col_id_array;
  ObSEArray<ObPackedObjPriv, 4> packed_col_privs_array;
  ObPackedObjPriv packed_total_matched_privs = 0;
  ObObjPrivSortKey new_col_key = obj_priv_key;
  ObPackedObjPriv packed_col_privs = 0;
  ObPackedObjPriv packed_col_privs_to_be_revoked = 0;
  bool exists = false;
  int org_n = 0;
  int own_priv_count = 0;
  int revoked_priv_count = 0;
  uint64_t option_out = false;
  bool is_all_single = false;
  new_key_array.reset();
  new_packed_privs_array.reset();
  is_all.reset();
  // 1. Find all object permissions based on grantee_id, grantor_id, obj_type, obj_id.
  OZ (build_table_and_col_priv_array_for_revoke_all(schema_guard,
                                                    obj_priv_key,
                                                    packed_table_privs,
                                                    col_id_array,
                                                    packed_col_privs_array));
  CK (col_id_array.count() == packed_col_privs_array.count());
  // 2. check permissions of table level.
  for (int i = 0; OB_SUCC(ret) && i < obj_priv_array.count(); ++i) {
    raw_obj_priv_to_be_revoked = obj_priv_array.at(i);
    // Check whether the table-level permission exists on the table
    OZ (ObOraPrivCheck::raw_obj_priv_exists_with_info(raw_obj_priv_to_be_revoked,
                                                      packed_table_privs,
                                                      exists,
                                                      option_out),
        raw_obj_priv_to_be_revoked, packed_table_privs, ret);
    if (OB_SUCC(ret)) {
      // If it exists, add the permission together with option to packed_table_privs_to_be_revoked,
      // and added in packed_total_matched_privs means that the permission was found
      // The permission may not exist as a table-level permission on the table
      if (exists) {
        OZ (ObPrivPacker::append_raw_obj_priv(option_out,
                                              raw_obj_priv_to_be_revoked,
                                              packed_table_privs_to_be_revoked));
        OZ (ObPrivPacker::append_raw_obj_priv(option_out,
                                              raw_obj_priv_to_be_revoked,
                                              packed_total_matched_privs));
      }
    }
  }
  // Record the table key and its permission to be revoke in the return value
  if (packed_table_privs_to_be_revoked) {
    OZ (new_key_array.push_back(obj_priv_key));
    OZ (new_packed_privs_array.push_back(packed_table_privs_to_be_revoked));
    OZ (ObPrivPacker::get_total_obj_privs(packed_table_privs, own_priv_count));
    OZ (ObPrivPacker::get_total_obj_privs(packed_table_privs_to_be_revoked, revoked_priv_count));
    OX (is_all_single = own_priv_count == revoked_priv_count);
    OZ (is_all.push_back(is_all_single));
  }
  // 3. Check column permissions
  for (int i = 0; OB_SUCC(ret) && i < col_id_array.count(); ++i) {
    // each column
    new_col_key.col_id_ = col_id_array.at(i);
    packed_col_privs = packed_col_privs_array.at(i);
    packed_col_privs_to_be_revoked = 0;
    for (int i = 0; OB_SUCC(ret) && i < obj_priv_array.count(); ++i) {
      raw_obj_priv_to_be_revoked = obj_priv_array.at(i);
      // Check only if the permission may be a column permission
      if (ObOraPrivCheck::raw_priv_can_be_granted_to_column(raw_obj_priv_to_be_revoked)) {
        // Check if the permission exists on the column
        OZ (ObOraPrivCheck::raw_obj_priv_exists_with_info(raw_obj_priv_to_be_revoked,
                                                          packed_col_privs,
                                                          exists,
                                                          option_out),
            raw_obj_priv_to_be_revoked, packed_col_privs, ret);
        if (OB_SUCC(ret)) {
          if (exists) {
            OZ (ObPrivPacker::append_raw_obj_priv(option_out,
                                                  raw_obj_priv_to_be_revoked,
                                                  packed_col_privs_to_be_revoked));
            OZ (ObPrivPacker::append_raw_obj_priv(option_out,
                                                  raw_obj_priv_to_be_revoked,
                                                  packed_total_matched_privs));
          }
        }
      }
    }
    // According to whether there are permissions in packed_col_privs_to_be_revoked, decide whether to keep the key
    if (OB_SUCC(ret)) {
      if (packed_col_privs_to_be_revoked) {
        OZ (new_key_array.push_back(new_col_key));
        OZ (new_packed_privs_array.push_back(packed_col_privs_to_be_revoked));
        OZ (ObPrivPacker::get_total_obj_privs(packed_col_privs, own_priv_count));
        OZ (ObPrivPacker::get_total_obj_privs(packed_col_privs_to_be_revoked, revoked_priv_count));
        OX (is_all_single = own_priv_count == revoked_priv_count);
        OZ (is_all.push_back(is_all_single));
      }
    }
  }
  // The three arrays should be the same size after processing.
  CK (new_key_array.count() == new_packed_privs_array.count());
  CK (new_key_array.count() == is_all.count());
  // According to the number of packed_total_matched_privs, determine whether to try to revoke a permission that does not exist
  OZ (ObPrivPacker::get_total_obj_privs(packed_total_matched_privs, org_n));
  if (OB_SUCC(ret)) {
    if (org_n < obj_priv_array.count()) {
      ret = OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT;
      LOG_WARN("try to revoke non exists privs", K(ret), K(org_n), K(obj_priv_array.count()));
    }
  }
  return ret;
}

/* According to the current obj priv, determine which ones need to be newly added priv array */
int ObDDLOperator::set_need_flush_ora(
    ObSchemaGetterGuard &schema_guard,
    const share::schema::ObObjPrivSortKey &obj_priv_key,   /* in: obj priv key*/
    const uint64_t option,                          /* in: new option */
    const ObRawObjPrivArray &obj_priv_array,        /* in: new privs used want to add */
    ObRawObjPrivArray &new_obj_priv_array)          /* out: new privs actually to be added */
{
  int ret = OB_SUCCESS;
  ObPackedObjPriv obj_privs = 0;
  ObRawObjPriv raw_obj_priv = 0;
  bool exists = false;
  OZ (schema_guard.get_obj_privs(obj_priv_key, obj_privs));
  for (int i = 0; i < obj_priv_array.count() && OB_SUCC(ret); i++) {
    raw_obj_priv = obj_priv_array.at(i);
    OZ (ObOraPrivCheck::raw_obj_priv_exists(raw_obj_priv,
                                            option,
                                            obj_privs,
                                            exists),
        raw_obj_priv, option, obj_privs, ret);
    if (OB_SUCC(ret) && !exists) {
      OZ (new_obj_priv_array.push_back(raw_obj_priv));
    }
  }
  return ret;
}

/* Only handle authorization for one object, for example, one table, one column */
int ObDDLOperator::grant_table(
    const ObTablePrivSortKey &table_priv_key,
    const ObPrivSet priv_set,
    const ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans,
    const share::ObRawObjPrivArray &obj_priv_array,
    const uint64_t option,
    const share::schema::ObObjPrivSortKey &obj_priv_key)
{
  int ret = OB_SUCCESS;
  ObRawObjPrivArray new_obj_priv_array;
  const uint64_t tenant_id = table_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (!table_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_priv_key is invalid", K(table_priv_key), K(ret));
  } else if (0 == priv_set && obj_priv_array.count() == 0) {
    //do nothing
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    ObPrivSet new_priv = priv_set;
    ObPrivSet table_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(schema_guard.get_table_priv_set(table_priv_key, table_priv_set))) {
      LOG_WARN("get table priv set failed", K(ret));
    } else {
      bool need_flush = true;
      new_priv |= table_priv_set;
      need_flush = (new_priv != table_priv_set);
      bool is_directory = false;
      if (obj_priv_array.count() > 0
          && static_cast<uint64_t>(ObObjectType::DIRECTORY) == obj_priv_key.obj_type_) {
        is_directory = true;
      }

      if (need_flush && !is_directory) {
        int64_t new_schema_version = OB_INVALID_VERSION;
        int64_t new_schema_version_ora = OB_INVALID_VERSION;
        if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (obj_priv_array.count() > 0) {
          OZ (set_need_flush_ora(schema_guard, obj_priv_key, option, obj_priv_array,
            new_obj_priv_array));
          if (new_obj_priv_array.count() > 0) {
            OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version_ora));
          }
        }
        OZ (schema_sql_service->get_priv_sql_service().grant_table(
            table_priv_key, new_priv, new_schema_version, ddl_stmt_str, trans,
            new_obj_priv_array, option, obj_priv_key, new_schema_version_ora, true, false),
            table_priv_key, ret, false);
      } else if (obj_priv_array.count() > 0) {
        OZ (set_need_flush_ora(schema_guard, obj_priv_key, option, obj_priv_array,
          new_obj_priv_array));
        if (new_obj_priv_array.count() > 0) {
          int64_t new_schema_version_ora = OB_INVALID_VERSION;
          OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version_ora));
          OZ (schema_sql_service->get_priv_sql_service().grant_table_ora_only(
            ddl_stmt_str, trans, new_obj_priv_array, option, obj_priv_key,
            new_schema_version_ora, false, false),table_priv_key, ret);
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::grant_routine(
    const ObRoutinePrivSortKey &routine_priv_key,
    const ObPrivSet priv_set,
    common::ObMySQLTransaction &trans,
    const uint64_t option,
    const bool gen_ddl_stmt)
{
  int ret = OB_SUCCESS;
  ObRawObjPrivArray new_obj_priv_array;
  const uint64_t tenant_id = routine_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (!routine_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("routine_priv_key is invalid", K(routine_priv_key), K(ret));
  } else if (0 == priv_set) {
    //do nothing
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    ObPrivSet new_priv = priv_set;
    ObPrivSet routine_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(schema_guard.get_routine_priv_set(routine_priv_key, routine_priv_set))) {
      LOG_WARN("get routine priv set failed", K(ret));
    } else {
      bool need_flush = true;
      new_priv |= routine_priv_set;
      need_flush = (new_priv != routine_priv_set);
      if (need_flush) {
        ObSqlString ddl_stmt_str;
        ObString ddl_sql;
        const ObUserInfo *user_info = NULL;
        ObNeedPriv need_priv;
        need_priv.db_ = routine_priv_key.db_;
        need_priv.table_ = routine_priv_key.routine_;
        need_priv.priv_level_ = OB_PRIV_ROUTINE_LEVEL;
        need_priv.priv_set_ = (~routine_priv_set) & new_priv;
        need_priv.obj_type_ = routine_priv_key.routine_type_ == ObRoutineType::ROUTINE_PROCEDURE_TYPE ?
                                                      ObObjectType::PROCEDURE : ObObjectType::FUNCTION;
        if (OB_FAIL(schema_guard.get_user_info(tenant_id, routine_priv_key.user_id_, user_info))) {
          LOG_WARN("get user info failed", K(routine_priv_key), K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user not exist", K(routine_priv_key), K(ret));
        } else if (gen_ddl_stmt == true && OB_FAIL(ObDDLSqlGenerator::gen_routine_priv_sql(
            ObAccountArg(user_info->get_user_name_str(), user_info->get_host_name_str()),
            need_priv, true, /*is_grant*/ ddl_stmt_str))) {
          LOG_WARN("gen_routine_priv_sql failed", K(ret), K(need_priv));
        } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
        } else {
          int64_t new_schema_version = OB_INVALID_VERSION;
          int64_t new_schema_version_ora = OB_INVALID_VERSION;
          if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_routine(
                routine_priv_key, new_priv, new_schema_version, &ddl_sql, trans, option, true))) {
            LOG_WARN("priv sql service grant routine failed", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::grant_column(
    ObSchemaGetterGuard &schema_guard,
    const ObColumnPrivSortKey &column_priv_key,
    const ObPrivSet priv_set,
    const ObString *ddl_stmt_str,
    common::ObMySQLTransaction &trans,
    const bool is_grant)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = column_priv_key.tenant_id_;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (!column_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_priv_key is invalid", K(column_priv_key), K(ret));
  } else if (0 == priv_set) {
    //do nothing
  } else {
    ObPrivSet new_priv = OB_PRIV_SET_EMPTY;
    ObPrivSet column_priv_set = OB_PRIV_SET_EMPTY;
    uint64_t column_priv_id = OB_INVALID_ID;
    if (OB_FAIL(schema_guard.get_column_priv_id(tenant_id, column_priv_key.user_id_, column_priv_key.db_,
                                                column_priv_key.table_, column_priv_key.column_, column_priv_id))) {
      LOG_WARN("get column priv id failed", K(ret));
    } else if (column_priv_id == OB_INVALID_ID) {
      if (!is_grant) {
        ret = OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT;
        LOG_WARN("revoke no such grant", K(ret), K(column_priv_key));
      } else {
        uint64_t new_column_priv_id = OB_INVALID_ID;
        if (OB_FAIL(schema_sql_service->fetch_new_priv_id(tenant_id, new_column_priv_id))) {
          LOG_WARN("fail to fetch new priv ids", KR(ret), K(tenant_id));
        } else if (OB_UNLIKELY(OB_INVALID_ID == new_column_priv_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("object_id is invalid", KR(ret), K(tenant_id));
        } else {
          column_priv_id = new_column_priv_id;
        }
      }
    } else if (OB_FAIL(schema_guard.get_column_priv_set(column_priv_key, column_priv_set))) {
      LOG_WARN("get table priv set failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      bool need_flush = true;
      if (is_grant) {
        new_priv = column_priv_set | priv_set;
      } else {
        new_priv = column_priv_set & (~priv_set);
      }
      need_flush = (new_priv != column_priv_set);

      if (need_flush) {
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_column(
                              column_priv_key, column_priv_id, new_priv, new_schema_version,
                              ddl_stmt_str, trans, is_grant))) {
          LOG_WARN("grant column failed", K(ret));
        }
      }
    }
  }

  return ret;
}

/* in: grantor, grantee, obj_type, obj_id
   out: table_packed_privs
        array of col_id which has col privs
        array of col_packed_privs */
int ObDDLOperator::build_table_and_col_priv_array_for_revoke_all(
    ObSchemaGetterGuard &schema_guard,
    const ObObjPrivSortKey &obj_priv_key,
    ObPackedObjPriv &packed_table_priv,
    ObSEArray<uint64_t, 4> &col_id_array,
    ObSEArray<ObPackedObjPriv, 4> &packed_privs_array)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObObjPriv *, 4> obj_priv_array;
  uint64_t col_id = 0;
  CK (obj_priv_key.is_valid());
  OZ (schema_guard.get_obj_privs_in_grantor_ur_obj_id(obj_priv_key.tenant_id_,
                                                      obj_priv_key,
                                                      obj_priv_array));
  for (int i = 0; i < obj_priv_array.count() && OB_SUCC(ret); i++) {
    const ObObjPriv *obj_priv = obj_priv_array.at(i);
    if (obj_priv != NULL) {
      col_id = obj_priv->get_col_id();
      if (col_id == OBJ_LEVEL_FOR_TAB_PRIV) {
        packed_table_priv = obj_priv->get_obj_privs();
      } else {
        OZ (col_id_array.push_back(col_id));
        OZ (packed_privs_array.push_back(obj_priv->get_obj_privs()));
      }
    }
  }

  return ret;
}

int ObDDLOperator::revoke_table_all(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const ObObjPrivSortKey &obj_priv_key,
    ObString &ddl_sql,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  share::ObPackedObjPriv packed_table_privs = 0;
  ObRawObjPrivArray raw_priv_array;
  ObRawObjPrivArray option_raw_array;
  ObSEArray<uint64_t, 4> col_id_array;
  ObSEArray<ObPackedObjPriv, 4> packed_privs_array;
  ObObjPrivSortKey new_key = obj_priv_key;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service,
        K(ret));
  }
  OZ (build_table_and_col_priv_array_for_revoke_all(schema_guard,
                                                    obj_priv_key,
                                                    packed_table_privs,
                                                    col_id_array,
                                                    packed_privs_array));
  if (OB_SUCC(ret)) {
    // 1. table-level permissions
    if (packed_table_privs > 0) {
      OZ (ObPrivPacker::raw_obj_priv_from_pack(packed_table_privs, raw_priv_array));
      OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
      OZ (schema_sql_service->get_priv_sql_service().revoke_table_ora(
        new_key, raw_priv_array, new_schema_version, &ddl_sql, trans, true));
      OZ (ObPrivPacker::raw_option_obj_priv_from_pack(packed_table_privs, option_raw_array));
      OZ (revoke_obj_cascade(schema_guard, new_key.grantee_id_,
          trans, new_key, option_raw_array));
    }
    // 2. column-level permissions
    for (int i = 0; i < col_id_array.count() && OB_SUCC(ret); i++) {
      new_key.col_id_ = col_id_array.at(i);
      OZ (ObPrivPacker::raw_obj_priv_from_pack(packed_privs_array.at(i), raw_priv_array));
      OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
      OZ (schema_sql_service->get_priv_sql_service().revoke_table_ora(
        new_key, raw_priv_array, new_schema_version, &ddl_sql, trans, true));
      OZ (ObPrivPacker::raw_option_obj_priv_from_pack(packed_privs_array.at(i),
          option_raw_array));
      OZ (revoke_obj_cascade(schema_guard, new_key.grantee_id_,
          trans, new_key, option_raw_array));
    }
  }
  return ret;
}

int ObDDLOperator::build_next_level_revoke_obj(
    ObSchemaGetterGuard &schema_guard,
    const ObObjPrivSortKey &old_key,
    ObObjPrivSortKey &new_key,
    ObIArray<const ObObjPriv *> &obj_privs)
{
  int ret = OB_SUCCESS;
  new_key = old_key;
  new_key.grantor_id_ = new_key.grantee_id_;
  OZ (schema_guard.get_obj_privs_in_grantor_obj_id(new_key.tenant_id_,
                                                   new_key,
                                                   obj_privs));
  return ret;
}

/* After processing the top-level revoke obj, then call this function to process revoke recursively.
   1. According to the obj key of the upper layer, change grantee to grantor and find new permissions that need to be reclaimed
   2. If there are permissions that need to be reclaimed, call revoke obj ora, if not, end.
   3. calling self. If a new grantee is found back to the original grantee, the end */
int ObDDLOperator::revoke_obj_cascade(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t start_grantee_id,     /* in: check circle */
    common::ObMySQLTransaction &trans,
    const ObObjPrivSortKey &old_key,     /* in: old key */
    ObRawObjPrivArray &old_array)        /* in: privs that have grantable option */
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = old_key.tenant_id_;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  ObObjPrivSortKey new_key;
  ObRawObjPrivArray grantable_array;
  ObRawObjPrivArray new_array;
  ObSEArray<const ObObjPriv *, 4> obj_privs;
  bool is_all = false;
  if (old_array.count() > 0) {
    OZ (build_next_level_revoke_obj(schema_guard, old_key, new_key, obj_privs));
    /* If there are multiple, it means that this user has delegated to multiple other users */
    if (obj_privs.count() > 0) {
      ObPackedObjPriv old_p_list;
      ObPackedObjPriv old_opt_p_list;
      ObPackedObjPriv privs_revoke_this_level;
      ObPackedObjPriv privs_revoke_next_level;

      OZ (ObPrivPacker::pack_raw_obj_priv_list(NO_OPTION, old_array, old_p_list));
      OZ (ObPrivPacker::pack_raw_obj_priv_list(GRANT_OPTION, old_array, old_opt_p_list));

      for (int i = 0; OB_SUCC(ret) && i < obj_privs.count(); i++) {
        const ObObjPriv* obj_priv = obj_privs.at(i);
        if (obj_priv != NULL) {
          /* 1. cross join grantee privs and grantor privs without option */
          privs_revoke_this_level = old_p_list & obj_priv->get_obj_privs();

          if (privs_revoke_this_level > 0) {
            /* 2. build new_Key */
            new_key.grantee_id_ = obj_priv->get_grantee_id();
            /* 2. build new array */
            OZ (ObPrivPacker::raw_obj_priv_from_pack(privs_revoke_this_level, new_array));
            OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
            OX (is_all = (new_array.count() == old_array.count()));
            OZ (schema_sql_service->get_priv_sql_service().revoke_table_ora(
                  new_key, new_array, new_schema_version, NULL, trans, is_all));
            /* 3. new grantee is equ org grantee. end */
            if (OB_SUCC(ret)) {
              if (new_key.grantee_id_ == start_grantee_id) {
              } else {
                /* 3. decide privs to be revoked recursively */
                privs_revoke_next_level = old_opt_p_list & obj_priv->get_obj_privs();
                if (privs_revoke_next_level > 0) {
                  OZ (ObPrivPacker::raw_obj_priv_from_pack(privs_revoke_next_level, new_array));
                  OZ (revoke_obj_cascade(schema_guard, start_grantee_id, trans,
                      new_key, new_array));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

/* Get all foreign keys of a user referencing the specified parent table */
int ObDDLOperator::build_fk_array_by_parent_table(
  uint64_t tenant_id,
  ObSchemaGetterGuard &schema_guard,
  const ObString &grantee_name,
  const ObString &db_name,
  const ObString &tab_name,
  ObIArray<ObDropForeignKeyArg> &drop_fk_array,
  ObIArray<uint64_t> &ref_tab_id_array)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;

  OZ (schema_guard.get_table_schema(tenant_id, db_name, tab_name, false, table_schema));
  if (OB_SUCC(ret)) {
    if (NULL == table_schema) {
      ret = OB_TABLE_NOT_EXIST;
    } else {
      uint64_t db_id = OB_INVALID_ID;
      OZ (schema_guard.get_database_id(tenant_id, grantee_name, db_id));
      /* Traverse all child tables referencing the parent table, if the owner of the child table is grantee, add drop fk array */
      const ObIArray<ObForeignKeyInfo> &fk_array = table_schema->get_foreign_key_infos();
      for (int i = 0; OB_SUCC(ret) && i < fk_array.count(); i++) {
        const ObForeignKeyInfo &fk_info = fk_array.at(i);
        const ObSimpleTableSchemaV2 *ref_table = NULL;
        OZ (schema_guard.get_simple_table_schema(tenant_id, fk_info.child_table_id_, ref_table));
        if (OB_SUCC(ret)) {
          if (ref_table == NULL) {
            ret = OB_TABLE_NOT_EXIST;
          } else if (ref_table->get_database_id() ==  db_id) {
            ObDropForeignKeyArg fk_arg;
            fk_arg.foreign_key_name_ = fk_info.foreign_key_name_;
            OZ (drop_fk_array.push_back(fk_arg));
            OZ (ref_tab_id_array.push_back(ref_table->get_table_id()));
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::drop_fk_cascade(
    uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    bool has_ref_priv,
    bool has_no_cascade,
    const ObString &grantee_name,
    const ObString &parent_db_name,
    const ObString &parent_tab_name,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (has_ref_priv) {
    ObSEArray<ObDropForeignKeyArg, 4> drop_fk_array;
    ObSEArray<uint64_t, 4> ref_tab_id_array;
    OZ (build_fk_array_by_parent_table(tenant_id,
                                       schema_guard,
                                       grantee_name,
                                       parent_db_name,
                                       parent_tab_name,
                                       drop_fk_array,
                                       ref_tab_id_array));
    if (OB_SUCC(ret)) {
      if (drop_fk_array.count() > 0) {
        if (has_no_cascade) {
          ret = OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE;
        } else {
          for (int i = 0; OB_SUCC(ret) && i < drop_fk_array.count(); i++) {
            const ObTableSchema *ref_tab = NULL;
            const ObDropForeignKeyArg &drop_fk = drop_fk_array.at(i);

            OZ (schema_guard.get_table_schema(tenant_id,
                ref_tab_id_array.at(i), ref_tab));
            if (OB_SUCC(ret)) {
              if (ref_tab == NULL) {
                ret = OB_TABLE_NOT_EXIST;
              } else {
                const ObForeignKeyInfo *parent_table_mock_foreign_key_info = NULL;
                OZ (alter_table_drop_foreign_key(*ref_tab, drop_fk, trans, parent_table_mock_foreign_key_info, ref_tab->get_in_offline_ddl_white_list()));
                if (OB_SUCC(ret) && NULL != parent_table_mock_foreign_key_info) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("parent_table_mock_foreign_key_info in oracle mode is unexpected", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::revoke_table(
    const ObTablePrivSortKey &table_priv_key,
    const ObPrivSet priv_set,
    common::ObMySQLTransaction &trans,
    const ObObjPrivSortKey &obj_priv_key,
    const share::ObRawObjPrivArray &obj_priv_array,
    const bool revoke_all_ora)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  bool is_oracle_mode = false;
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service,
        K(ret));
  } else if (!table_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db_priv_key is invalid", K(table_priv_key), K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to get compat mode", K(ret));
  } else {
    ObPrivSet table_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(schema_guard.get_table_priv_set(table_priv_key, table_priv_set))) {
      LOG_WARN("get table priv set failed", K(ret));
    } else if (OB_PRIV_SET_EMPTY == table_priv_set
               && !revoke_all_ora
               && obj_priv_array.count() == 0) {
      ObArray<const ObColumnPriv *> column_privs;
      if (OB_FAIL(schema_guard.get_column_priv_in_table(table_priv_key, column_privs))) {
        LOG_WARN("get column priv in table failed", K(ret));
      } else {
        if (column_privs.count() > 0) {
          //do nothing here, and will revoke column priv behind.
        } else {
          ret = OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT;
          LOG_WARN("No such grant to revoke", K(table_priv_key), K(ret));
        }
      }
    } else if (0 == priv_set && obj_priv_array.count() == 0) {
      // do-nothing
    } else {
      ObPrivSet new_priv = table_priv_set & (~priv_set);
      /* If there is an intersection between the existing permissions and the permissions that require revoke */
      if (0 != (table_priv_set & priv_set)) {
        ObSqlString ddl_stmt_str;
        ObString ddl_sql;
        const ObUserInfo *user_info = NULL;
        ObNeedPriv need_priv;
        share::ObRawObjPrivArray option_priv_array;

        need_priv.db_ = table_priv_key.db_;
        need_priv.table_ = table_priv_key.table_;
        need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
        need_priv.priv_set_ = table_priv_set & priv_set; //priv to revoke
        int64_t new_schema_version = OB_INVALID_VERSION;
        int64_t new_schema_version_ora = OB_INVALID_VERSION;
        bool is_all = false;
        bool has_ref_priv = false;
        if (OB_FAIL(check_obj_privs_exists(schema_guard, obj_priv_key,
            obj_priv_array, option_priv_array, is_all))) {
          LOG_WARN("priv not exits", K(obj_priv_array), K(ret));
        } else if (OB_FAIL(schema_guard.get_user_info(tenant_id, table_priv_key.user_id_, user_info))) {
          LOG_WARN("get user info failed", K(table_priv_key), K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user not exist", K(table_priv_key), K(ret));
        } else if (OB_FAIL(drop_fk_cascade(tenant_id,
                                           schema_guard,
                                           has_ref_priv,
                                           true, /* has no cascade */
                                           user_info->get_user_name_str(), /* grantee name */
                                           table_priv_key.db_,
                                           table_priv_key.table_,
                                           trans))) {
          LOG_WARN("drop fk cascase failed", K(table_priv_key), K(ret));
        } else if (OB_FAIL(ObDDLSqlGenerator::gen_table_priv_sql(
                ObAccountArg(user_info->get_user_name_str(), user_info->get_host_name_str()),
                need_priv,
                false, /*is_grant*/
                ddl_stmt_str))) {
          LOG_WARN("gen_table_priv_sql failed", K(ret), K(need_priv));
        } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id,
                           new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id,
                           new_schema_version_ora))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().revoke_table(
            table_priv_key, new_priv, new_schema_version, &ddl_sql, trans,
            new_schema_version_ora, obj_priv_key, obj_priv_array, is_all))) {
          LOG_WARN("Failed to revoke table", K(table_priv_key), K(ret));
        } else {
          OZ (revoke_obj_cascade(schema_guard, obj_priv_key.grantee_id_,
              trans, obj_priv_key, option_priv_array));
        }
        // In revoke all statement, if you have permission, it will come here, and the content of mysql will be processed first.
        if (OB_SUCC(ret) && revoke_all_ora) {
          OZ (revoke_table_all(schema_guard, tenant_id, obj_priv_key, ddl_sql, trans));
        }
      } else if (!is_oracle_mode) {
        //do nothing
      } else {
        ObSqlString ddl_stmt_str;
        ObString ddl_sql;
        const ObUserInfo *user_info = NULL;
        int64_t new_schema_version = OB_INVALID_VERSION;
        share::ObRawObjPrivArray option_priv_array;
        ObRawObjPrivArray raw_priv_array;
        ObArray<bool> is_all;
        ObArray<ObObjPrivSortKey> priv_key_array;
        ObArray<ObPackedObjPriv> packed_privs_array;
        ObRawObjPrivArray option_raw_array;
        // In oracle mode, need to check revoke permission exists, only need to reclaim oracle permission
        // Due to the existence of column permissions, one obj_priv_key is expanded into multiple,
        // and each key represents a column
        OZ (check_obj_privs_exists_including_col_privs(schema_guard, obj_priv_key,
            obj_priv_array, priv_key_array, packed_privs_array, is_all));
        OZ (schema_guard.get_user_info(tenant_id, table_priv_key.user_id_, user_info));
        if (OB_SUCC(ret) && user_info == NULL) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user not exist", K(table_priv_key), K(ret));
        }

        OZ (ObDDLSqlGenerator::gen_table_priv_sql_ora(
            ObAccountArg(user_info->get_user_name_str(), user_info->get_host_name_str()),
            table_priv_key,
            revoke_all_ora,
            obj_priv_array,
            false,
            ddl_stmt_str));
        OX (ddl_sql = ddl_stmt_str.string());
        if (OB_SUCC(ret)) {
          if (revoke_all_ora) {
            OZ (revoke_table_all(schema_guard, tenant_id, obj_priv_key, ddl_sql, trans));
          } else if (obj_priv_array.count() > 0) {
            // Revoke table permissions and column permissions one by one
            for (int i = 0; OB_SUCC(ret) && i < priv_key_array.count(); ++i) {
              const ObObjPrivSortKey &priv_key = priv_key_array.at(i);
              OZ (ObPrivPacker::raw_obj_priv_from_pack(packed_privs_array.at(i), raw_priv_array));
              OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
              OZ (schema_sql_service->get_priv_sql_service().revoke_table_ora(
                priv_key, raw_priv_array, new_schema_version,
                &ddl_sql, trans, is_all.at(i)));
              OZ (ObPrivPacker::raw_option_obj_priv_from_pack(packed_privs_array.at(i),
                  option_raw_array));
              OZ (revoke_obj_cascade(schema_guard, priv_key.grantee_id_,
                  trans, priv_key, option_raw_array));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::revoke_routine(
    const ObRoutinePrivSortKey &routine_priv_key,
    const ObPrivSet priv_set,
    common::ObMySQLTransaction &trans,
    bool report_error,
    const bool gen_ddl_stmt)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = routine_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service,
        K(ret));
  } else if (!routine_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db_priv_key is invalid", K(routine_priv_key), K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    ObPrivSet routine_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(schema_guard.get_routine_priv_set(routine_priv_key, routine_priv_set))) {
      LOG_WARN("get routine priv set failed", K(ret));
    } else if (OB_PRIV_SET_EMPTY == routine_priv_set) {
      if (report_error) {
        ret = OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT;
        LOG_WARN("No such grant to revoke", K(routine_priv_key), K(routine_priv_set), K(ret));
      }
    } else if (0 == priv_set) {
      // do-nothing
    } else {
      ObPrivSet new_priv = routine_priv_set & (~priv_set);
      /* If there is an intersection between the existing permissions and the permissions that require revoke */
      if ((routine_priv_set & priv_set) != 0) {
        ObSqlString ddl_stmt_str;
        ObString ddl_sql;
        const ObUserInfo *user_info = NULL;
        ObNeedPriv need_priv;
        share::ObRawObjPrivArray option_priv_array;

        need_priv.db_ = routine_priv_key.db_;
        need_priv.table_ = routine_priv_key.routine_;
        need_priv.priv_level_ = OB_PRIV_ROUTINE_LEVEL;
        need_priv.priv_set_ = routine_priv_set & priv_set; //priv to revoke
        need_priv.obj_type_ = routine_priv_key.routine_type_ == ObRoutineType::ROUTINE_PROCEDURE_TYPE ?
                                                      ObObjectType::PROCEDURE : ObObjectType::FUNCTION;
        int64_t new_schema_version = OB_INVALID_VERSION;
        int64_t new_schema_version_ora = OB_INVALID_VERSION;
        bool is_all = false;
        bool has_ref_priv = false;
        if (OB_FAIL(schema_guard.get_user_info(tenant_id, routine_priv_key.user_id_, user_info))) {
          LOG_WARN("get user info failed", K(routine_priv_key), K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user not exist", K(routine_priv_key), K(ret));
        } else if (gen_ddl_stmt == true && OB_FAIL(ObDDLSqlGenerator::gen_routine_priv_sql(
            ObAccountArg(user_info->get_user_name_str(), user_info->get_host_name_str()),
            need_priv,
            false, /*is_grant*/
            ddl_stmt_str))) {
          LOG_WARN("gen_routine_priv_sql failed", K(ret), K(need_priv));
        } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id,
                           new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().revoke_routine(
            routine_priv_key, new_priv, new_schema_version, &ddl_sql, trans))) {
          LOG_WARN("Failed to revoke routine", K(routine_priv_key), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::get_flush_role_array(
  const uint64_t option,
  const common::ObIArray<uint64_t> &org_role_ids,
  bool &need_flush,
  bool is_grant,
  const ObUserInfo &user_info,
  common::ObIArray<uint64_t> &role_ids)
{
  int ret = OB_SUCCESS;
  need_flush = false;
  if (org_role_ids.count() > 0) {
    if (is_grant) {
      for (int64_t i = 0; OB_SUCC(ret) && i < org_role_ids.count(); ++i) {
        const uint64_t role_id = org_role_ids.at(i);
        if (!user_info.role_exists(role_id, option)) {
          need_flush = true;
          OZ (role_ids.push_back(role_id));
        }
      }
    } else {
      need_flush = true;
      OZ (role_ids.assign(org_role_ids));
    }
  }
  return ret;
}

int ObDDLOperator::grant_revoke_role(
    const uint64_t tenant_id,
    const ObUserInfo &user_info,
    const common::ObIArray<uint64_t> &org_role_ids,
    // When specified_role_info is not empty, use it as role_info instead of reading it in the schema.
    const ObUserInfo *specified_role_info,
    common::ObMySQLTransaction &trans,
    const bool log_operation,
    const bool is_grant,
    const uint64_t option)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObString ddl_sql;
  bool is_oracle_mode = false;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to get compat mode", K(ret));
  } else {
    common::ObSEArray<uint64_t, 8> role_ids;
    bool need_flush = false;
    OZ (get_flush_role_array(option,
                             org_role_ids,
                             need_flush,
                             is_grant,
                             user_info,
                             role_ids));
    if (OB_SUCC(ret) && need_flush) {
      common::ObSqlString sql_string;
      if (OB_FAIL(sql_string.append_fmt(is_grant ? "GRANT ": "REVOKE "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_NOT_NULL(specified_role_info)) {
        // Use single specified role info
        if (OB_FAIL(sql_string.append_fmt("%s", specified_role_info->get_user_name()))) {
          LOG_WARN("append sql failed", K(ret));
        }
      } else {
        // Use role info obtained from schema
        for (int64_t i = 0; OB_SUCC(ret) && i < role_ids.count(); ++i) {
          const uint64_t role_id = role_ids.at(i);
          const ObUserInfo *role_info = NULL;
          if (0 != i) {
            if (OB_FAIL(sql_string.append_fmt(","))) {
              LOG_WARN("append sql failed", K(ret));
            }
          }
          if (FAILEDx(schema_guard.get_user_info(tenant_id, role_id, role_info))) {
            LOG_WARN("Failed to get role info", K(ret), K(tenant_id), K(role_id));
          } else if (NULL == role_info) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("role doesn't exist", K(ret), K(role_id));
          } else if (is_oracle_mode ?
                       OB_FAIL(sql_string.append_fmt("%s", role_info->get_user_name()))
                     : OB_FAIL(sql_string.append_fmt("`%s`@`%s`",
                                                     role_info->get_user_name(),
                                                     role_info->get_host_name()))) {
            LOG_WARN("append sql failed", K(ret));
          } else if (!is_grant) {
            for (int64_t j = 0; OB_SUCC(ret) && j < user_info.get_proxied_user_info_cnt(); j++) {
              const ObProxyInfo *proxy_info = user_info.get_proxied_user_info_by_idx(j);
              if (OB_ISNULL(proxy_info)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected error", K(ret));
              }
              for (int64_t k = 0; OB_SUCC(ret) && k < proxy_info->role_id_cnt_; k++) {
                if (proxy_info->get_role_id_by_idx(k) == role_id) {
                  OZ (schema_service->get_priv_sql_service().grant_proxy_role(tenant_id, user_info.get_user_id(),
                                                proxy_info->user_id_, role_id, new_schema_version, trans, is_grant));
                }
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (is_oracle_mode ? OB_FAIL(sql_string.append_fmt(is_grant ? " TO %s": " FROM %s",
                                                           user_info.get_user_name()))
                           : OB_FAIL(sql_string.append_fmt(is_grant ? " TO `%s`@`%s`": " FROM `%s`@`%s`",
                                                           user_info.get_user_name(),
                                                           user_info.get_host_name()))) {
          LOG_WARN("append sql failed", K(ret));
        } else if (is_grant && option != NO_OPTION && OB_FAIL(sql_string.append_fmt(
                                                                          " WITH ADMIN OPTION"))) {
          LOG_WARN("append sql failed", K(ret));
        } else {
          ddl_sql = sql_string.string();
          LOG_WARN("wang sql ", K(ddl_sql), K(sql_string));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(schema_service->get_priv_sql_service().grant_revoke_role(tenant_id,
            user_info,
            role_ids,
            specified_role_info,
            new_schema_version,
            log_operation ? &ddl_sql : NULL,
            trans,
            is_grant,
            schema_guard,
            option))) {
          LOG_WARN("Failed to revoke role", K(user_info), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::get_flush_priv_array(
    const uint64_t option,
    const share::ObRawPrivArray &priv_array,
    const ObSysPriv *sys_priv,
    share::ObRawPrivArray &new_priv_array,
    bool &need_flush,
    const bool is_grant,
    const ObUserInfo &user_info)
{
  int ret = OB_SUCCESS;
  int64_t raw_priv = 0;
  bool exists = false;

  need_flush = FALSE;
  if (is_grant) {
    if (sys_priv == NULL) {
      need_flush = true;
      OZ (new_priv_array.assign(priv_array));
    } else {
      ARRAY_FOREACH(priv_array, idx) {
        raw_priv = priv_array.at(idx);
        OZ (ObOraPrivCheck::raw_sys_priv_exists(option,
                                                raw_priv,
                                                sys_priv->get_priv_array(),
                                                exists));
        if (OB_SUCC(ret) && !exists) {
          need_flush = true;
          OZ (new_priv_array.push_back(raw_priv));
        }
      }
    }
  }
  else {
    need_flush = true;
    if (sys_priv == NULL) {
      ret = OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO;
      LOG_USER_ERROR(OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO,
                     user_info.get_user_name_str().length(),
                     user_info.get_user_name_str().ptr());
      LOG_WARN("revoke sys priv fail, sys priv not exists", K(priv_array), K(ret));
    } else {
      ARRAY_FOREACH(priv_array, idx) {
        raw_priv = priv_array.at(idx);
        OZ (ObOraPrivCheck::raw_sys_priv_exists(option,
                                                raw_priv,
                                                sys_priv->get_priv_array(),
                                                exists));
        if (OB_SUCC(ret) && !exists) {
          ret = OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO;
          LOG_USER_ERROR(OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO,
                     user_info.get_user_name_str().length(),
                     user_info.get_user_name_str().ptr());
          LOG_WARN("revoke sys priv fail, sys priv not exists", K(priv_array), K(ret));
        }
        OZ (new_priv_array.push_back(raw_priv));
      }
    }
  }
  return ret;
}

int ObDDLOperator::grant_sys_priv_to_ur(
    const uint64_t tenant_id,
    const uint64_t grantee_id,
    const ObSysPriv* sys_priv,
    const uint64_t option,
    const ObRawPrivArray priv_array,
    common::ObMySQLTransaction &trans,
    const bool is_grant,
    const common::ObString *ddl_stmt_str,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObRawPrivArray new_priv_array;
  bool need_flush;
  const ObUserInfo *user_info = NULL;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  }
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version), tenant_id);
  OZ (schema_guard.get_user_info(tenant_id, grantee_id, user_info));
  OZ (get_flush_priv_array(option,
                           priv_array,
                           sys_priv,
                           new_priv_array,
                           need_flush,
                           is_grant,
                           *user_info));
  if (OB_SUCC(ret) && need_flush) {
    if (is_grant) {
      CK (new_priv_array.count() > 0);
      OZ (schema_service->get_priv_sql_service().grant_sys_priv_to_ur(tenant_id,
                                                                      grantee_id,
                                                                      option,
                                                                      new_priv_array,
                                                                      new_schema_version,
                                                                      ddl_stmt_str,
                                                                      trans,
                                                                      is_grant,
                                                                      false));
    } else {
      int n_cnt = 0;
      bool revoke_all_flag = false;
      /* revoke */
      CK(sys_priv != NULL);
      OZ (ObPrivPacker::get_total_privs(sys_priv->get_priv_array(), n_cnt));
      revoke_all_flag = (n_cnt == new_priv_array.count());
        /* revoke all */
      OZ (schema_service->get_priv_sql_service().grant_sys_priv_to_ur(tenant_id,
                                                                      grantee_id,
                                                                      option,
                                                                      new_priv_array,
                                                                      new_schema_version,
                                                                      ddl_stmt_str,
                                                                      trans,
                                                                      is_grant,
                                                                      revoke_all_flag),
           tenant_id, grantee_id, new_priv_array, is_grant, revoke_all_flag);
    }
  }
  return ret;
}

//----End of functions for managing privileges----

//----Functions for managing outlines----
int ObDDLOperator::create_outline(ObOutlineInfo &outline_info,
                                  ObMySQLTransaction &trans,
                                  const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  uint64_t new_outline_id = OB_INVALID_ID;
  const uint64_t tenant_id = outline_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } // else if (!outline_info.is_valid()) {
  //   ret = OB_INVALID_ARGUMENT;
  //   LOG_ERROR("outline is invalid", K(outline_info), K(ret));
  // }
  else if (OB_FAIL(schema_service->fetch_new_outline_id(tenant_id, new_outline_id))) {
    LOG_WARN("failed to fetch new_outline_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    outline_info.set_outline_id(new_outline_id);
    outline_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_outline_sql_service().insert_outline(
        outline_info, trans, ddl_stmt_str))) {
      LOG_WARN("insert outline info failed", K(outline_info), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::replace_outline(ObOutlineInfo &outline_info,
                                   ObMySQLTransaction &trans,
                                   const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = outline_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    outline_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_outline_sql_service().replace_outline(
        outline_info, trans, ddl_stmt_str))) {
      LOG_WARN("replace outline info failed", K(outline_info), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDDLOperator::alter_outline(ObOutlineInfo &outline_info,
                                 ObMySQLTransaction &trans,
                                 const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = outline_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    outline_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_outline_sql_service().alter_outline(
        outline_info, trans, ddl_stmt_str))) {
      LOG_WARN("alter outline failed", K(outline_info), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDDLOperator::drop_outline(const uint64_t tenant_id,
                                const uint64_t database_id,
                                const uint64_t outline_id,
                                ObMySQLTransaction &trans,
                                const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id
                  || OB_INVALID_ID == outline_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_id), K(outline_id), K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_outline_sql_service().delete_outline(
      tenant_id,
      database_id,
      outline_id,
      new_schema_version,
      trans,
      ddl_stmt_str))) {
    LOG_WARN("drop outline failed", K(tenant_id), K(outline_id), KT(outline_id), K(ret));
  } else {/*do nothing*/}
  return ret;
}

//----Functions for managing dblinks----
int ObDDLOperator::create_dblink(ObDbLinkBaseInfo &dblink_info,
                                 ObMySQLTransaction &trans,
                                 const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t tenant_id = dblink_info.get_tenant_id();
  uint64_t new_dblink_id = OB_INVALID_ID;
  int64_t schema_version = -1;
  int64_t is_deleted = 0;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_new_dblink_id(tenant_id, new_dblink_id))) {
    LOG_WARN("failed to fetch new_dblink_id", K(tenant_id), K(ret));
  } else if (FALSE_IT(dblink_info.set_dblink_id(new_dblink_id))) {
    // nothing.
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, schema_version))) {
    LOG_WARN("failed to gen new_schema_version", K(tenant_id), K(ret));
  } else if (FALSE_IT(dblink_info.set_schema_version(schema_version))) {
    // nothing.
  } else if (OB_FAIL(schema_service->get_dblink_sql_service().insert_dblink(
                     dblink_info, is_deleted, trans, ddl_stmt_str))) {
    LOG_WARN("failed to insert dblink", K(dblink_info.get_dblink_name()), K(ret));
  }
  return ret;
}

int ObDDLOperator::drop_dblink(ObDbLinkBaseInfo &dblink_info,
                               ObMySQLTransaction &trans,
                               const common::ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t tenant_id = dblink_info.get_tenant_id();
  uint64_t dblink_id = dblink_info.get_dblink_id();
  int64_t schema_version = -1;
  int64_t is_deleted = 1;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, schema_version))) {
    LOG_WARN("failed to gen new_schema_version", K(tenant_id), K(ret));
  } else if (FALSE_IT(dblink_info.set_schema_version(schema_version))) {
    // nothing.
  } else if (OB_FAIL(schema_service->get_dblink_sql_service().insert_dblink(
                     dblink_info, is_deleted, trans, ddl_stmt_str))) {
    LOG_WARN("failed to insert dblink", K(dblink_info.get_dblink_name()), K(ret));
  } else if (OB_FAIL(schema_service->get_dblink_sql_service().delete_dblink(
                     tenant_id, dblink_id, trans))) {
    LOG_WARN("failed to delete dblink", K(dblink_info.get_dblink_name()), K(ret));
  }
  return ret;
}
//----End of functions for managing dblinks----

//----Functions for managing synonym----
int ObDDLOperator::create_synonym(ObSynonymInfo &synonym_info,
                                  ObMySQLTransaction &trans,
                                  const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  uint64_t new_synonym_id = OB_INVALID_ID;
  const uint64_t tenant_id = synonym_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_new_synonym_id(tenant_id, new_synonym_id))) {
    LOG_WARN("failed to fetch new_synonym_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    synonym_info.set_synonym_id(new_synonym_id);
    synonym_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_synonym_sql_service().insert_synonym(
        synonym_info, &trans, ddl_stmt_str))) {
      LOG_WARN("insert synonym info failed", K(synonym_info.get_synonym_name_str()), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::replace_synonym(ObSynonymInfo &synonym_info,
                                   ObMySQLTransaction &trans,
                                   const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = synonym_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    synonym_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_synonym_sql_service().replace_synonym(
        synonym_info, &trans, ddl_stmt_str))) {
      LOG_WARN("replace synonym info failed", K(synonym_info.get_synonym_name_str()), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDDLOperator::drop_synonym(const uint64_t tenant_id,
                                const uint64_t database_id,
                                const uint64_t synonym_id,
                                ObMySQLTransaction &trans,
                                const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                  || OB_INVALID_ID == synonym_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_id), K(synonym_id), K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_synonym_sql_service().delete_synonym(
      tenant_id,
      database_id,
      synonym_id,
      new_schema_version,
      &trans,
      ddl_stmt_str))) {
    LOG_WARN("drop synonym failed", K(tenant_id), K(synonym_id), KT(synonym_id), K(ret));
  } else {/*do nothing*/}
  return ret;
}


//----End of functions for managing outlines----

int ObDDLOperator::create_routine(ObRoutineInfo &routine_info,
                                  ObMySQLTransaction &trans,
                                  ObErrorInfo &error_info,
                                  ObIArray<ObDependencyInfo> &dep_infos,
                                  const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t new_routine_id = OB_INVALID_ID;
  const uint64_t tenant_id = routine_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id
          && OB_FAIL(schema_service->fetch_new_sys_pl_object_id(tenant_id, new_routine_id))) {
    LOG_WARN("failed to fetch new_routine_id", K(tenant_id), K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id
          && OB_FAIL(schema_service->fetch_new_routine_id(tenant_id, new_routine_id))) {
    LOG_WARN("failed to fetch new_routine_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    routine_info.set_routine_id(new_routine_id);
    routine_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_routine_sql_service().create_routine(routine_info,
                                                                         &trans,
                                                                         ddl_stmt_str))) {
      LOG_WARN("insert routine info failed", K(routine_info), K(ret));
    }
  }
  OZ (insert_dependency_infos(trans, dep_infos, tenant_id, routine_info.get_routine_id(),
                                routine_info.get_schema_version(),
                                routine_info.get_owner_id()));

  // add audit in routine if necessary
  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    ObArray<const ObSAuditSchema *> audits;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                       AUDIT_OBJ_DEFAULT,
                                                       OB_AUDIT_MOCK_USER_ID,
                                                       audits))) {
      LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
    } else if (!audits.empty()) {
      ObSchemaService *schema_service = schema_service_.get_schema_service();
      common::ObSqlString public_sql_string;
      ObSAuditSchema new_audit_schema;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
        uint64_t new_audit_id = common::OB_INVALID_ID;
        const ObSAuditSchema *audit_schema = audits.at(i);
        if (OB_ISNULL(audit_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("audit_schema is NULL", K(ret));
        } else if (!audit_schema->is_access_operation_for_procedure()) {
          continue;
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))){
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service->fetch_new_audit_id(tenant_id, new_audit_id))) {
          LOG_WARN("Failed to fetch new_audit_id", K(ret));
        } else if (OB_FAIL(new_audit_schema.assign(*audit_schema))) {
          LOG_WARN("fail to assign audit schema", KR(ret));
        } else {
          new_audit_schema.set_schema_version(new_schema_version);
          new_audit_schema.set_audit_id(new_audit_id);
          new_audit_schema.set_audit_type(AUDIT_PROCEDURE);
          new_audit_schema.set_owner_id(routine_info.get_routine_id());
          if (OB_FAIL(schema_service->get_audit_sql_service().handle_audit_metainfo(
              new_audit_schema,
              AUDIT_MT_ADD,
              false,
              new_schema_version,
              NULL,
              trans,
              public_sql_string))) {
            LOG_WARN("add audit_schema failed",  K(new_audit_schema), K(ret));
          } else {
            LOG_INFO("succ to add audit_schema from routine", K(new_audit_schema));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &routine_info))) {
      LOG_WARN("insert create routine error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObDDLOperator::replace_routine(ObRoutineInfo &routine_info,
                                   const ObRoutineInfo *old_routine_info,
                                   ObMySQLTransaction &trans,
                                   ObErrorInfo &error_info,
                                   ObIArray<ObDependencyInfo> &dep_infos,
                                   const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  CK(OB_NOT_NULL(schema_service));
  CK(OB_NOT_NULL(old_routine_info));
  const uint64_t tenant_id = routine_info.get_tenant_id();
  int64_t del_param_schema_version = OB_INVALID_VERSION;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (old_routine_info->get_routine_params().count() > 0) {
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, del_param_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    routine_info.set_routine_id(old_routine_info->get_routine_id());
    routine_info.set_schema_version(new_schema_version);
  }
  if (OB_SUCC(ret) && ERROR_STATUS_NO_ERROR == error_info.get_error_status()) {
    OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, old_routine_info->get_tenant_id(),
                                                          old_routine_info->get_routine_id(),
                                                          old_routine_info->get_database_id()));
  }
  OZ (ObDependencyInfo::delete_schema_object_dependency(trans, old_routine_info->get_tenant_id(),
                                     old_routine_info->get_routine_id(),
                                     new_schema_version,
                                     old_routine_info->get_object_type()));
  if (OB_SUCC(ret)
      && OB_FAIL(schema_service->get_routine_sql_service().replace_routine(routine_info,
                                                                           old_routine_info,
                                                                           del_param_schema_version,
                                                                           &trans,
                                                                           ddl_stmt_str))) {
    LOG_WARN("replace routine info failed", K(routine_info), K(ret));
  }

  OZ (insert_dependency_infos(trans, dep_infos, tenant_id, routine_info.get_routine_id(),
                                routine_info.get_schema_version(),
                                routine_info.get_owner_id()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &routine_info))) {
      LOG_WARN("replace routine error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObDDLOperator::alter_routine(const ObRoutineInfo &routine_info,
                                 ObMySQLTransaction &trans,
                                 ObErrorInfo &error_info,
                                 const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  UNUSEDx(ddl_stmt_str);
  if (OB_FAIL(error_info.handle_error_info(trans, &routine_info))) {
    LOG_WARN("drop routine error info failed.", K(ret), K(error_info));
  }
  return ret;
}

int ObDDLOperator::drop_routine(const ObRoutineInfo &routine_info,
                                ObMySQLTransaction &trans,
                                ObErrorInfo &error_info,
                                const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = routine_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(drop_obj_privs(tenant_id, routine_info.get_routine_id(),
                                    static_cast<uint64_t>(routine_info.get_routine_type()),
                                    trans))) {
    LOG_WARN("fail to drop_obj_privs", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_routine_sql_service().drop_routine(
                     routine_info, new_schema_version, trans, ddl_stmt_str))) {
    LOG_WARN("drop routine info failed", K(routine_info), K(ret));
  }
  OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, routine_info.get_tenant_id(),
                            routine_info.get_routine_id(), routine_info.get_database_id()));
  OZ (ObDependencyInfo::delete_schema_object_dependency(trans, routine_info.get_tenant_id(),
                                     routine_info.get_routine_id(),
                                     new_schema_version,
                                     routine_info.get_object_type()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &routine_info))) {
      LOG_WARN("drop routine error info failed.", K(ret), K(error_info));
    }
  }

  // delete audit in procedure
  if (OB_SUCC(ret)) {
    ObArray<const ObSAuditSchema *> audits;
    ObSchemaGetterGuard schema_guard;
    ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                              AUDIT_PROCEDURE,
                                                              routine_info.get_routine_id(),
                                                              audits))) {
      LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
    } else if (!audits.empty()) {
      common::ObSqlString public_sql_string;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
        const ObSAuditSchema *audit_schema = audits.at(i);
        if (OB_ISNULL(audit_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("audit_schema is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
            *audit_schema,
            AUDIT_MT_DEL,
            false,
            new_schema_version,
            NULL,
            trans,
            public_sql_string))) {
          LOG_WARN("drop audit_schema failed",  KPC(audit_schema), K(ret));
        } else {
          LOG_INFO("succ to delete audit_schema from drop procedure", KPC(audit_schema));
        }
      }
    } else {
      LOG_DEBUG("no need to delete audit_schema from drop procedure", K(audits), K(routine_info));
    }
  }
  return ret;
}

int ObDDLOperator::update_routine_info(ObRoutineInfo &routine_info,
                                      int64_t tenant_id,
                                      int64_t parent_id,
                                      int64_t owner_id,
                                      int64_t database_id,
                                      int64_t routine_id = OB_INVALID_ID)
{
  int ret = OB_SUCCESS;
  uint64_t new_routine_id = routine_id;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_INVALID_ID == new_routine_id
        && OB_SYS_TENANT_ID == tenant_id
        && OB_FAIL(schema_service->fetch_new_sys_pl_object_id(tenant_id, new_routine_id))) {
    LOG_WARN("failed to fetch new_routine_id", K(tenant_id), K(ret));
  } else if (OB_INVALID_ID == new_routine_id
        && OB_SYS_TENANT_ID != tenant_id
        && OB_FAIL(schema_service->fetch_new_routine_id(tenant_id, new_routine_id))) {
    LOG_WARN("failed to fetch new_routine_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret));
  } else {
    routine_info.set_database_id(database_id);
    if (lib::Worker::CompatMode::ORACLE == compat_mode) {
      routine_info.set_owner_id(owner_id);
    }
    routine_info.set_package_id(parent_id);
    routine_info.set_routine_id(new_routine_id);
    routine_info.set_schema_version(new_schema_version);
  }
  return ret;
}

int ObDDLOperator::create_package(const ObPackageInfo *old_package_info,
                                  ObPackageInfo &new_package_info,
                                  ObMySQLTransaction &trans,
                                  ObSchemaGetterGuard &schema_guard,
                                  ObIArray<ObRoutineInfo> &public_routine_infos,
                                  ObErrorInfo &error_info,
                                  ObIArray<ObDependencyInfo> &dep_infos,
                                  const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t new_package_id = OB_INVALID_ID;
  const uint64_t tenant_id = new_package_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  bool is_replace = false;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else {
    if (OB_NOT_NULL(old_package_info)) {
      is_replace = true;
      new_package_id = old_package_info->get_package_id();
      if (old_package_info->is_package() && OB_FAIL(del_routines_in_package(*old_package_info, trans, schema_guard))) {
        LOG_WARN("del routines in package failed", K(ret), K(old_package_info));
      } else {}
    } else {
      if (OB_SYS_TENANT_ID == tenant_id) {
        if (OB_FAIL(schema_service->fetch_new_sys_pl_object_id(tenant_id, new_package_id))) {
          LOG_WARN("failed to fetch new_package_id", K(tenant_id), K(ret));
        }
      } else {
        if (OB_FAIL(schema_service->fetch_new_package_id(tenant_id, new_package_id))) {
          LOG_WARN("failed to fetch new_package_id", K(tenant_id), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      new_package_info.set_package_id(new_package_id);
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (FALSE_IT(new_package_info.set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service->get_routine_sql_service().create_package(new_package_info,
          &trans, is_replace, ddl_stmt_str))) {
        LOG_WARN("insert package info failed", K(new_package_info), K(ret));
      } else {
        ARRAY_FOREACH(public_routine_infos, routine_idx) {
          ObRoutineInfo &routine_info = public_routine_infos.at(routine_idx);
          if (new_package_info.is_package()) {
            if (OB_FAIL(update_routine_info(routine_info,
                                            tenant_id,
                                            new_package_info.get_package_id(),
                                            new_package_info.get_owner_id(),
                                            new_package_info.get_database_id(),
                                            routine_info.get_routine_id()))) {
              LOG_WARN("failed to update routine info", K(ret));
            } else if (OB_FAIL(schema_service->get_routine_sql_service().create_routine(routine_info,
                                                                         &trans, NULL))) {
              LOG_WARN("insert routine info failed", K(routine_info), K(ret));
            }
          } else if (OB_INVALID_ID == routine_info.get_routine_id()) {
            OZ (update_routine_info(routine_info,
                                    tenant_id,
                                    routine_info.get_package_id(),
                                    routine_info.get_owner_id(),
                                    routine_info.get_database_id()));
            OZ (schema_service->get_routine_sql_service().create_routine(routine_info, &trans, NULL));
          } else {
            // update routine route sql
            int64_t new_schema_version = OB_INVALID_VERSION;
            OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
            OX (routine_info.set_schema_version(new_schema_version));
            OZ (schema_service->get_routine_sql_service().update_routine(routine_info, &trans));
          }
        }
      }
    }

    // add audit in package if necessary
    if (OB_SUCC(ret) && new_package_info.is_package()) {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObArray<const ObSAuditSchema *> audits;
      if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                         AUDIT_OBJ_DEFAULT,
                                                         OB_AUDIT_MOCK_USER_ID,
                                                         audits))) {
        LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
      } else if (!audits.empty()) {
        ObSchemaService *schema_service = schema_service_.get_schema_service();
        common::ObSqlString public_sql_string;
        ObSAuditSchema new_audit_schema;
        for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
          uint64_t new_audit_id = common::OB_INVALID_ID;
          const ObSAuditSchema *audit_schema = audits.at(i);
          if (OB_ISNULL(audit_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("audit_schema is NULL", K(ret));
          } else if (!audit_schema->is_access_operation_for_package()) {
            continue;
          } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))){
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_service->fetch_new_audit_id(tenant_id, new_audit_id))) {
            LOG_WARN("Failed to fetch new_audit_id", K(ret));
          } else if (OB_FAIL(new_audit_schema.assign(*audit_schema))) {
            LOG_WARN("fail to assign audit schema", KR(ret));
          } else {
            new_audit_schema.set_schema_version(new_schema_version);
            new_audit_schema.set_audit_id(new_audit_id);
            new_audit_schema.set_audit_type(AUDIT_PACKAGE);
            new_audit_schema.set_owner_id(new_package_info.get_package_id());
            if (OB_FAIL(schema_service->get_audit_sql_service().handle_audit_metainfo(
                new_audit_schema,
                AUDIT_MT_ADD,
                false,
                new_schema_version,
                NULL,
                trans,
                public_sql_string))) {
              LOG_WARN("add audit_schema failed",  K(new_audit_schema), K(ret));
            } else {
              LOG_INFO("succ to add audit_schema from package", K(new_audit_schema));
            }
          }
        }
      }
    }
    if (is_replace) {
      if (OB_SUCC(ret) && ERROR_STATUS_NO_ERROR == error_info.get_error_status()) {
        // when recreate package header, need clear package body cache if exist package body
        if (PACKAGE_TYPE == old_package_info->get_type()) {
          const ObPackageInfo *del_package_info = NULL;
          if (OB_FAIL(schema_guard.get_package_info(tenant_id,
                                                    old_package_info->get_database_id(),
                                                    old_package_info->get_package_name(),
                                                    PACKAGE_BODY_TYPE,
                                                    old_package_info->get_compatibility_mode(),
                                                    del_package_info))) {
            LOG_WARN("get package body info failed", K(ret));
          } else if (OB_NOT_NULL(del_package_info)) {
            OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans,
                                                                  tenant_id,
                                                                  del_package_info->get_package_id(),
                                                                  del_package_info->get_database_id()));
          }
        } else {
          // do nothing
        }
        OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans,
                                                              tenant_id,
                                                              old_package_info->get_package_id(),
                                                              old_package_info->get_database_id()));
      }
      OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                     old_package_info->get_package_id(),
                                     new_schema_version,
                                     old_package_info->get_object_type()));
    }
    OZ (insert_dependency_infos(trans, dep_infos, tenant_id, new_package_info.get_package_id(),
                                new_package_info.get_schema_version(),
                                new_package_info.get_owner_id()));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(error_info.handle_error_info(trans, &new_package_info))) {
        LOG_WARN("insert create package error info failed.", K(ret), K(error_info));
      }
    }
  }
  return ret;
}

int ObDDLOperator::alter_package(ObPackageInfo &package_info,
                                 ObSchemaGetterGuard &schema_guard,
                                 ObMySQLTransaction &trans,
                                 ObIArray<ObRoutineInfo> &public_routine_infos,
                                 ObErrorInfo &error_info,
                                 const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  UNUSEDx(ddl_stmt_str);
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = package_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t package_id = package_info.get_package_id();
  OV (OB_NOT_NULL(schema_service_impl), OB_ERR_SYS);
  OV (OB_INVALID_ID != tenant_id && OB_INVALID_ID != package_id, OB_INVALID_ARGUMENT);
  if (OB_SUCC(ret)) {
    if (public_routine_infos.count() > 0) {
      bool need_create = false;
      ObArray<const ObRoutineInfo *> routine_infos;
      OZ (schema_guard.get_routine_infos_in_package(tenant_id,
                                                    public_routine_infos.at(0).get_package_id(),
                                                    routine_infos));
      OX (need_create = (0 == routine_infos.count()));
      // update routine route sql
      ARRAY_FOREACH(public_routine_infos, routine_idx) {
        ObRoutineInfo &routine_info = public_routine_infos.at(routine_idx);
        if (need_create) {
          CK (OB_INVALID_ID == routine_info.get_routine_id());
          OZ (update_routine_info(routine_info,
                                  tenant_id,
                                  routine_info.get_package_id(),
                                  routine_info.get_owner_id(),
                                  routine_info.get_database_id()));
          OZ (schema_service_impl->get_routine_sql_service().create_routine(routine_info, &trans, NULL));
        } else {
          OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
          OX (routine_info.set_schema_version(new_schema_version));
          OZ (schema_service_impl->get_routine_sql_service().update_routine(routine_info, &trans));
        }
      }
    }
    // if alter package, we need push up schema version, because we need update package state
    OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
    OX (package_info.set_schema_version(new_schema_version));
    OZ (schema_service_impl->get_routine_sql_service().alter_package(package_info, &trans, ddl_stmt_str));
  }

  OZ (error_info.handle_error_info(trans, &package_info));

  return ret;
}

int ObDDLOperator::drop_package(const ObPackageInfo &package_info,
                                ObMySQLTransaction &trans,
                                ObSchemaGetterGuard &schema_guard,
                                ObErrorInfo &error_info,
                                const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = package_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t package_id = package_info.get_package_id();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == package_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (package_info.is_package()
        && OB_FAIL(del_routines_in_package(package_info, trans, schema_guard))) {
      LOG_WARN("del routines in package failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (PACKAGE_TYPE == package_info.get_type()) {
      // delete privs on package
      OZ (drop_obj_privs(tenant_id,
                         package_info.get_package_id(),
                         static_cast<uint64_t>(ObObjectType::PACKAGE),
                         trans));
      uint64_t database_id = package_info.get_database_id();
      int64_t compatible_mode = package_info.get_compatibility_mode();
      const ObString &package_name = package_info.get_package_name();
      const ObPackageInfo *package_body_info = NULL;
      if (OB_FAIL(schema_guard.get_package_info(tenant_id, database_id, package_name, PACKAGE_BODY_TYPE,
                                                compatible_mode, package_body_info))) {
        LOG_WARN("get package body info failed", K(tenant_id), K(database_id), K(package_name), K(ret));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (NULL != package_body_info) {
        if (OB_FAIL(schema_service_impl->get_routine_sql_service().drop_package(package_body_info->get_tenant_id(),
                                                                                package_body_info->get_database_id(),
                                                                                package_body_info->get_package_id(),
                                                                                new_schema_version, trans))) {
          LOG_WARN("drop package body info failed", K(package_body_info), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // package spec
        OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                                        package_info.get_package_id(),
                                                        new_schema_version,
                                                        package_info.get_object_type()));
        OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id, package_info.get_package_id(),
                                                              package_info.get_database_id()));
        if (OB_NOT_NULL(package_body_info)) {
          OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                                        package_body_info->get_package_id(),
                                                        new_schema_version,
                                                        package_body_info->get_object_type()));
          OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id, package_body_info->get_package_id(),
                                                                package_body_info->get_database_id()));
        }
      }
    } else {
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id, package_info.get_package_id(),
                                                            package_info.get_database_id()));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_impl->get_routine_sql_service().drop_package(package_info.get_tenant_id(),
                                                                                   package_info.get_database_id(),
                                                                                   package_info.get_package_id(),
                                                                                   new_schema_version, trans, ddl_stmt_str))) {
      LOG_WARN("drop package info failed", K(package_info), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &package_info))) {
      LOG_WARN("delete drop package error info failed", K(ret), K(error_info));
    }
  }

    // delete audit in package
  if (OB_SUCC(ret)) {
    ObArray<const ObSAuditSchema *> audits;
    if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                       AUDIT_PACKAGE,
                                                       package_info.get_package_id(),
                                                       audits))) {
      LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
    } else if (!audits.empty()) {
      common::ObSqlString public_sql_string;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
        const ObSAuditSchema *audit_schema = audits.at(i);
        if (OB_ISNULL(audit_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("audit_schema is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
            *audit_schema,
            AUDIT_MT_DEL,
            false,
            new_schema_version,
            NULL,
            trans,
            public_sql_string))) {
          LOG_WARN("drop audit_schema failed",  KPC(audit_schema), K(ret));
        } else {
          LOG_INFO("succ to delete audit_schema from drop package", KPC(audit_schema));
        }
      }
    } else {
      LOG_DEBUG("no need to delete audit_schema from drop package", K(audits), K(package_info));
    }
  }
  return ret;
}

int ObDDLOperator::del_routines_in_package(const ObPackageInfo &package_info,
                                           ObMySQLTransaction &trans,
                                           ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = package_info.get_tenant_id();
  uint64_t package_id = package_info.get_package_id();
  if (OB_INVALID_ID == tenant_id
      || OB_INVALID_ID == package_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObArray<const ObRoutineInfo *> routines;
    if (OB_FAIL(schema_guard.get_routine_infos_in_package(tenant_id, package_id, routines))) {
      LOG_WARN("get routines in package failed", K(tenant_id), K(package_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < routines.count(); ++i) {
        const ObRoutineInfo *routine_info = routines.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_ISNULL(routine_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("routine info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_routine_sql_service().drop_routine(
                           *routine_info, new_schema_version, trans))) {
          LOG_WARN("drop routine failed", "routine_id", routine_info->get_routine_id(), K(ret));
        }
      }
    }
  }
  return ret;
}

// functions for managing trigger

int ObDDLOperator::create_trigger(ObTriggerInfo &trigger_info,
                                  ObMySQLTransaction &trans,
                                  ObErrorInfo &error_info,
                                  ObIArray<ObDependencyInfo> &dep_infos,
                                  int64_t &table_schema_version,
                                  const ObString *ddl_stmt_str,
                                  bool is_update_table_schema_version,
                                  bool is_for_truncate_table)
{
  int ret = OB_SUCCESS;
  table_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = trigger_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  bool is_replace = false;
  OV (OB_NOT_NULL(schema_service));
  if (!is_for_truncate_table) {
    // If create_trigger through truncate table, trigger_info already has its own trigger_id.
    // but there is no such trigger in the internal table at this time, so is_replace must be false
    OX (is_replace = (OB_INVALID_ID != trigger_info.get_trigger_id()));
    if (!is_replace) {
      OZ (fill_trigger_id(*schema_service, trigger_info), trigger_info.get_trigger_name());
    }
  }
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version),
      tenant_id, trigger_info.get_trigger_name());
  OX (trigger_info.set_schema_version(new_schema_version));
  OZ (schema_service->get_trigger_sql_service().create_trigger(trigger_info, is_replace,
                                                              trans, ddl_stmt_str),
      trigger_info.get_trigger_name(), is_replace);
  if (!trigger_info.is_system_type() && is_update_table_schema_version) {
    uint64_t base_table_id = trigger_info.get_base_object_id();
    OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
    OX (table_schema_version = new_schema_version);
    OZ (schema_service->get_table_sql_service().update_data_table_schema_version(
        trans, tenant_id, base_table_id, false/*in offline ddl white list*/, new_schema_version),
        base_table_id, trigger_info.get_trigger_name());
  }
  if (OB_FAIL(ret)) {
  } else if (0 == dep_infos.count()) {
    // create trigger in mysql mode or create trigger when truncate table, dep_infos.count() is 0,
    // no need to deal with dependencies.
  } else {
    OZ (ObDependencyInfo::delete_schema_object_dependency(trans, trigger_info.get_tenant_id(),
                                                          trigger_info.get_trigger_id(),
                                                          trigger_info.get_schema_version(),
                                                          trigger_info.get_object_type()));
    OZ (insert_dependency_infos(trans, dep_infos, trigger_info.get_tenant_id(), trigger_info.get_trigger_id(),
                                trigger_info.get_schema_version(), trigger_info.get_owner_id()));

    if (OB_SUCC(ret) && is_replace && ERROR_STATUS_NO_ERROR == error_info.get_error_status()) {
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(
          trans, tenant_id, share::schema::ObTriggerInfo::get_trigger_spec_package_id(trigger_info.get_trigger_id()),
          trigger_info.get_database_id()));
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(
          trans, tenant_id, share::schema::ObTriggerInfo::get_trigger_body_package_id(trigger_info.get_trigger_id()),
          trigger_info.get_database_id()));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &trigger_info))) {
      LOG_WARN("insert trigger error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObDDLOperator::drop_trigger(const ObTriggerInfo &trigger_info,
                                ObMySQLTransaction &trans,
                                const ObString *ddl_stmt_str,
                                bool is_update_table_schema_version,
                                const bool in_offline_ddl_white_list)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = trigger_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  OV (OB_NOT_NULL(schema_service));
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version),
      tenant_id, trigger_info.get_trigger_name());
  OZ (schema_service->get_trigger_sql_service().drop_trigger(trigger_info, false,
                                                             new_schema_version,
                                                             trans, ddl_stmt_str),
      trigger_info.get_trigger_name());
  OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                                        trigger_info.get_trigger_id(),
                                                        new_schema_version,
                                                        trigger_info.get_object_type()));
  OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                share::schema::ObTriggerInfo::get_trigger_spec_package_id(trigger_info.get_trigger_id()),
                trigger_info.get_database_id()));
  OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                share::schema::ObTriggerInfo::get_trigger_body_package_id(trigger_info.get_trigger_id()),
                trigger_info.get_database_id()));
  if (OB_SUCC(ret) && !trigger_info.is_system_type() && is_update_table_schema_version) {
    uint64_t base_table_id = trigger_info.get_base_object_id();
    OZ (schema_service->get_table_sql_service().update_data_table_schema_version(trans,
        tenant_id, base_table_id, in_offline_ddl_white_list),
        base_table_id, trigger_info.get_trigger_name());
  }
  if (OB_SUCC(ret)) {
    ObErrorInfo error_info;
    if (OB_FAIL(error_info.handle_error_info(trans, &trigger_info))) {
      LOG_WARN("delete trigger error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObDDLOperator::alter_trigger(ObTriggerInfo &trigger_info,
                                 ObMySQLTransaction &trans,
                                 const ObString *ddl_stmt_str,
                                 bool is_update_table_schema_version)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = trigger_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  OV (OB_NOT_NULL(schema_service));
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version), tenant_id,
      trigger_info.get_trigger_name());
  OX (trigger_info.set_schema_version(new_schema_version));
  OZ (schema_service->get_trigger_sql_service().alter_trigger(trigger_info, new_schema_version,
                                                              trans, ddl_stmt_str),
      trigger_info.get_trigger_name());
  if (OB_SUCC(ret) && !trigger_info.is_system_type() && is_update_table_schema_version) {
      uint64_t base_table_id = trigger_info.get_base_object_id();
      OZ (schema_service->get_table_sql_service().update_data_table_schema_version(
          trans, tenant_id, base_table_id, false/*in offline ddl white list*/),
          base_table_id, trigger_info.get_trigger_name());
  }
  OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(
      trans, tenant_id, share::schema::ObTriggerInfo::get_trigger_spec_package_id(trigger_info.get_trigger_id()),
      trigger_info.get_database_id()));
  OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(
      trans, tenant_id, share::schema::ObTriggerInfo::get_trigger_body_package_id(trigger_info.get_trigger_id()),
      trigger_info.get_database_id()));
  ObErrorInfo error_info;
  OZ (error_info.handle_error_info(trans, &trigger_info), error_info);
  return ret;
}

int ObDDLOperator::drop_trigger_to_recyclebin(const ObTriggerInfo &trigger_info,
                                              ObSchemaGetterGuard &schema_guard,
                                              ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t tenant_id = trigger_info.get_tenant_id();
  uint64_t recyclebin_id = OB_RECYCLEBIN_SCHEMA_ID;
  uint64_t base_database_id = OB_INVALID_ID;
  bool recyclebin_exist = false;
  int64_t new_schema_version = OB_INVALID_VERSION;
  const ObSimpleTableSchemaV2 *base_table_schema = NULL;
  ObTriggerInfo new_trigger_info;
  ObSqlString new_trigger_name;
  ObRecycleObject recyclebin_object;
  OV (OB_NOT_NULL(schema_service));
  OV (OB_INVALID_TENANT_ID != tenant_id, OB_INVALID_ARGUMENT);
  OZ (schema_guard.check_database_exist(tenant_id, recyclebin_id, recyclebin_exist));
  OV (recyclebin_exist);
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
  OZ (new_trigger_info.deep_copy(trigger_info));
  OX (new_trigger_info.set_database_id(recyclebin_id));
  OX (new_trigger_info.set_schema_version(new_schema_version));
  OZ (construct_new_name_for_recyclebin(new_trigger_info, new_trigger_name));
  OZ (new_trigger_info.set_trigger_name(new_trigger_name.string()));
  OZ (schema_guard.get_simple_table_schema(tenant_id, trigger_info.get_base_object_id(), base_table_schema),
      trigger_info.get_base_object_id());
  OV (OB_NOT_NULL(base_table_schema), trigger_info.get_base_object_id());
  OX (base_database_id = base_table_schema->get_database_id());
  OX (recyclebin_object.set_tenant_id(tenant_id));
  OX (recyclebin_object.set_database_id(base_database_id));
  OX (recyclebin_object.set_table_id(trigger_info.get_trigger_id()));
  OX (recyclebin_object.set_tablegroup_id(OB_INVALID_ID));
  OZ (recyclebin_object.set_object_name(new_trigger_name.string()));
  OZ (recyclebin_object.set_original_name(trigger_info.get_trigger_name()));
  OX (recyclebin_object.set_type(ObRecycleObject::TRIGGER));
  OZ (schema_service->insert_recyclebin_object(recyclebin_object, trans));
  OZ (schema_service->get_trigger_sql_service().drop_trigger(new_trigger_info, true,
                                                             new_schema_version, trans),
      trigger_info.get_trigger_name());
  return ret;
}

int ObDDLOperator::flashback_trigger(const ObTriggerInfo &trigger_info,
                                     uint64_t new_database_id,
                                     const ObString &new_table_name,
                                     ObSchemaGetterGuard &schema_guard,
                                     ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t tenant_id = trigger_info.get_tenant_id();
  const ObDatabaseSchema *database_schema = NULL;
  ObSEArray<ObRecycleObject, 1> recycle_objects;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObString new_trigger_name;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObTriggerInfo new_trigger_info;
  OV (OB_NOT_NULL(schema_service));
  OZ (schema_service->fetch_recycle_object(tenant_id, trigger_info.get_trigger_name(),
                                           ObRecycleObject::TRIGGER, trans, recycle_objects));
  OV (1 == recycle_objects.count(), OB_ERR_UNEXPECTED, recycle_objects.count());
  OZ (new_trigger_info.deep_copy(trigger_info));
  // database id.
  if (OB_INVALID_ID == new_database_id) {
    OX (new_database_id = recycle_objects.at(0).get_database_id());
  }
  OZ (schema_guard.get_database_schema(tenant_id, new_database_id, database_schema), new_database_id);
  OV (OB_NOT_NULL(database_schema), new_database_id);
  OV (!database_schema->is_in_recyclebin(), OB_OP_NOT_ALLOW, new_database_id);
  OX (new_trigger_info.set_database_id(new_database_id));
  // trigger name.
  OZ (build_flashback_object_name(new_trigger_info, NULL, "OBTRG",
                                  schema_guard, allocator, new_trigger_name));
  OX (new_trigger_info.set_trigger_name(new_trigger_name));
  // other operation.
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version), tenant_id);
  OZ (schema_service->delete_recycle_object(tenant_id, recycle_objects.at(0), trans));
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (new_table_name.empty()) {
    // If new_table_name is empty, it means that the table does not have a rename, and rebuild_trigger_package is not required.
    // Otherwise, table rename is required, and rebuild_trigger_package is required.
    OZ (schema_service->get_trigger_sql_service().flashback_trigger(new_trigger_info,
                                                                    new_schema_version, trans),
        new_trigger_info.get_trigger_id());
  } else {
    OZ (schema_service->get_trigger_sql_service()
                          .rebuild_trigger_package(new_trigger_info,
                                                   database_schema->get_database_name(),
                                                   new_table_name, new_schema_version,
                                                   trans, OB_DDL_FLASHBACK_TRIGGER),
        database_schema->get_database_name(), new_table_name);
  }
  return ret;
}

int ObDDLOperator::purge_table_trigger(const ObTableSchema &table_schema,
                                       ObSchemaGetterGuard &schema_guard,
                                       ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const ObIArray<uint64_t> &trigger_id_list = table_schema.get_trigger_list();
  const ObTriggerInfo *trigger_info = NULL;
  for (int i = 0; OB_SUCC(ret) && i < trigger_id_list.count(); i++) {
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_id_list.at(i), trigger_info), trigger_id_list.at(i));
    OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_id_list.at(i));
    OZ (purge_trigger(*trigger_info, trans), trigger_id_list.at(i));
  }
  return ret;
}

int ObDDLOperator::purge_trigger(const ObTriggerInfo &trigger_info, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObRecycleObject::RecycleObjType recycle_type = ObRecycleObject::TRIGGER;
  ObArray<ObRecycleObject> recycle_objects;
  OV (OB_NOT_NULL(schema_service));
  OZ (schema_service->fetch_recycle_object(trigger_info.get_tenant_id(),
                                           trigger_info.get_trigger_name(),
                                           recycle_type, trans, recycle_objects));
  OV (1 == recycle_objects.count(), OB_ERR_UNEXPECTED, recycle_objects.count());
  OZ (schema_service->delete_recycle_object(trigger_info.get_tenant_id(),
                                            recycle_objects.at(0), trans));
  OZ (drop_trigger(trigger_info, trans, NULL));
  return ret;
}

int ObDDLOperator::rebuild_trigger_package(const ObTriggerInfo &trigger_info,
                                           const ObString &database_name,
                                           const ObString &table_name,
                                           ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = trigger_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  OV (OB_NOT_NULL(schema_service));
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version),
      tenant_id, trigger_info.get_trigger_name());
  OZ (schema_service->get_trigger_sql_service().rebuild_trigger_package(trigger_info,
                                                                        database_name, table_name,
                                                                        new_schema_version, trans),
      trigger_info.get_trigger_name());
  return ret;
}

int ObDDLOperator::fill_trigger_id(ObSchemaService &schema_service, ObTriggerInfo &trigger_info)
{
  int ret = OB_SUCCESS;
  uint64_t new_trigger_id = OB_INVALID_ID;
  OV (OB_INVALID_ID == trigger_info.get_trigger_id());
  OZ (schema_service.fetch_new_trigger_id(trigger_info.get_tenant_id(), new_trigger_id),
      trigger_info.get_trigger_name());
  OX (trigger_info.set_trigger_id(new_trigger_id));
  return ret;
}

/* [data_table_prefix]_[data_table_id]_RECYCLE_[object_type_prefix]_[timestamp].
 * data_table_prefix: if not null, use '[data_table_prefix]_[data_table_id]' as prefix,
 *                    otherwise, skip this part.
 * object_type_prefix: OBIDX / OBCHECK / OBTRG ...
 */
template <typename SchemaType>
int ObDDLOperator::build_flashback_object_name(const SchemaType &object_schema,
                                               const char *data_table_prefix,
                                               const char *object_type_prefix,
                                               ObSchemaGetterGuard &schema_guard,
                                               ObIAllocator &allocator,
                                               ObString &object_name)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  int64_t saved_pos = 0;
  bool object_exist = true;
  static const char *RECYCLE = "RECYCLE";
  static const int64_t INT64_STR_LEN = 20;
  OV (OB_NOT_NULL(object_type_prefix));
  if (OB_NOT_NULL(data_table_prefix)) {
    OX (buf_len += (STRLEN(data_table_prefix) + 1 +   // [data_table_prefix]_
                    INT64_STR_LEN + 1));              // [data_table_id]_
  }
  OX (buf_len += (STRLEN(RECYCLE) + 1 +               // RECYCLE_
                  STRLEN(object_type_prefix) + 1 +    // [object_type_prefix]_
                  INT64_STR_LEN));                    // [timestamp]
  OV (OB_NOT_NULL(buf = static_cast<char *>(allocator.alloc(buf_len))),
      OB_ALLOCATE_MEMORY_FAILED, buf_len);
  if (OB_NOT_NULL(data_table_prefix)) {
    OZ (BUF_PRINTF("%s_%lu_", data_table_prefix, object_schema.get_data_table_id()));
  }
  OZ (BUF_PRINTF("%s_%s_", RECYCLE, object_type_prefix));
  OX (saved_pos = pos);
  while (OB_SUCC(ret) && object_exist) {
    OX (pos = saved_pos);
    OZ (BUF_PRINTF("%ld", ObTimeUtility::current_time()));
    OX (object_name.assign(buf, static_cast<int32_t>(pos)));
    OZ (schema_guard.check_flashback_object_exist(object_schema, object_name, object_exist));
  }
  return ret;
}

// functions for managing udt

int ObDDLOperator::create_udt(ObUDTTypeInfo &udt_info,
                              ObMySQLTransaction &trans,
                              ObErrorInfo &error_info,
                              ObIArray<share::schema::ObRoutineInfo> &public_routine_infos,
                              ObSchemaGetterGuard &schema_guard,
                              ObIArray<ObDependencyInfo> &dep_infos,
                              const ObString *ddl_stmt_str)
{
  UNUSED(schema_guard);
  int ret = OB_SUCCESS;
  uint64_t new_udt_id = OB_INVALID_ID;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObUDTObjectType *obj_info = NULL;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id
        && !is_inner_pl_udt_id(new_udt_id = udt_info.get_type_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get new system udt id", K(tenant_id), K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id
        && OB_FAIL(schema_service->fetch_new_udt_id(tenant_id, new_udt_id))) {
    LOG_WARN("failed to fetch new_udt_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    // object spec and collection
    if (udt_info.is_object_spec_ddl() || !udt_info.is_object_type()) {
      udt_info.set_type_id(new_udt_id);
      if (udt_info.is_object_spec_ddl()
          && 0 < udt_info.get_object_type_infos().count()) {
        OX (obj_info = udt_info.get_object_type_infos().at(0));
        CK (OB_NOT_NULL(obj_info));
        // set object spec id
        uint64_t new_obj_id = OB_INVALID_ID;
        if (OB_SYS_TENANT_ID == tenant_id) {
          OZ (schema_service->fetch_new_sys_pl_object_id(tenant_id, new_obj_id));
        } else {
          OZ (schema_service->fetch_new_udt_id(tenant_id, new_obj_id));
        }
        OX (obj_info->set_coll_type(new_obj_id));
      }
    } else { // object body
      CK (2 == udt_info.get_object_type_infos().count());
      OX (obj_info = udt_info.get_object_type_infos().at(1));
      CK (OB_NOT_NULL(obj_info));
      // set object body id
      OX (obj_info->set_coll_type(new_udt_id));
      // udt_info.set_type_id(new_udt_id);
    }
    udt_info.set_schema_version(new_schema_version);
    if (FAILEDx(schema_service->get_udt_sql_service().create_udt(udt_info,
                                                                 &trans,
                                                                 ddl_stmt_str))) {
      LOG_WARN("insert udt info failed", K(udt_info), K(ret));
    } else {
      ARRAY_FOREACH(public_routine_infos, routine_idx) {
        ObRoutineInfo &routine_info = public_routine_infos.at(routine_idx);
        if (udt_info.is_object_spec_ddl()) {
          if (OB_FAIL(update_routine_info(routine_info,
                                          tenant_id,
                                          udt_info.get_type_id(),
                                          udt_info.get_database_id(),
                                          udt_info.get_database_id()))) {
            LOG_WARN("failed to update routine info", K(ret));
          } else if (OB_FAIL(schema_service->get_routine_sql_service().create_routine(routine_info,
                                                                        &trans, NULL))) {
            LOG_WARN("insert routine info failed", K(routine_info), K(ret));
          }
        } else {
          // update routine route sql
          int64_t new_schema_version = OB_INVALID_VERSION;
          OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
          OX (routine_info.set_schema_version(new_schema_version));
          OZ (schema_service->get_routine_sql_service().update_routine(routine_info, &trans));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_inner_pl_udt_id(new_udt_id)) {
    OZ (insert_dependency_infos(trans, dep_infos, tenant_id, udt_info.get_type_id(),
                                new_schema_version,
                                udt_info.get_database_id()));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &udt_info))) {
      LOG_WARN("insert trigger error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObDDLOperator::replace_udt(ObUDTTypeInfo &udt_info,
                               const ObUDTTypeInfo *old_udt_info,
                               ObMySQLTransaction &trans,
                               ObErrorInfo &error_info,
                               ObIArray<share::schema::ObRoutineInfo> &public_routine_infos,
                               ObSchemaGetterGuard &schema_guard,
                               ObIArray<ObDependencyInfo> &dep_infos,
                               const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  int64_t del_param_schema_version = OB_INVALID_VERSION;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  CK(OB_NOT_NULL(schema_service));
  CK(OB_NOT_NULL(old_udt_info));
  if (old_udt_info->get_attrs().count() > 0 || OB_NOT_NULL(old_udt_info->get_coll_info())) {
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, del_param_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    udt_info.set_schema_version(new_schema_version);
    udt_info.set_type_id(old_udt_info->get_type_id());
  }

  if (OB_SUCC(ret)) {
    ObUDTObjectType *old_obj_info = NULL, *new_obj_info = NULL;
    if (udt_info.is_object_spec_ddl()) {
      // create or replace xxx is object(a number), no object type info
      // CK (0 < old_udt_info->get_object_type_infos().count());
      // CK (0 < udt_info.get_object_type_infos().count());
      if (udt_info.get_object_type_infos().count() > 0) {
        OX (new_obj_info = udt_info.get_object_type_infos().at(0));
        if (old_udt_info->get_object_type_infos().count() > 0) {
          OX (old_obj_info = old_udt_info->get_object_type_infos().at(0));
          OX (new_obj_info->set_coll_type(old_obj_info->get_coll_type()));
        } else {
          uint64_t new_obj_id = OB_INVALID_ID;
          if (OB_SYS_TENANT_ID == tenant_id) {
            OZ (schema_service->fetch_new_sys_pl_object_id(tenant_id, new_obj_id));
          } else {
            OZ (schema_service->fetch_new_udt_id(tenant_id, new_obj_id));
          }
          OX (new_obj_info->set_coll_type(new_obj_id));
        }
      }
    } else if (udt_info.is_object_body_ddl()) {
      CK (2 == old_udt_info->get_object_type_infos().count());
      CK (2 == udt_info.get_object_type_infos().count());
      OX (old_obj_info = old_udt_info->get_object_type_infos().at(1));
      OX (new_obj_info = udt_info.get_object_type_infos().at(1));
      OX (new_obj_info->set_coll_type(old_obj_info->get_coll_type()));
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret) && public_routine_infos.count() > 0 && ERROR_STATUS_NO_ERROR == error_info.get_error_status()) {
    if (OB_INVALID_ID != ObUDTObjectType::mask_object_id(old_udt_info->get_object_spec_id(tenant_id))) {
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(
          trans, tenant_id, ObUDTObjectType::mask_object_id(old_udt_info->get_object_spec_id(tenant_id)),
          old_udt_info->get_database_id()));
      if (old_udt_info->has_type_body()
          && OB_INVALID_ID != ObUDTObjectType::mask_object_id(old_udt_info->get_object_body_id(tenant_id))) {
        OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(
            trans, tenant_id, ObUDTObjectType::mask_object_id(old_udt_info->get_object_body_id(tenant_id)),
            old_udt_info->get_database_id()));
      }
    }
  }
  OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                     old_udt_info->get_type_id(),
                                     new_schema_version,
                                     // old_udt_info->get_object_type())); TODO: type body id design flaw
                                     udt_info.get_object_type()));
  OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                     udt_info.get_type_id(),
                                     new_schema_version,
                                     udt_info.get_object_type()));
  if (OB_SUCC(ret)) {
    // Discuss whether the built-in functions of the object type need to be deleted and then inserted in several situations
    // 1. spec spec replace , need delete, Because the definition of the function may have changed
    // 2. spec body replace, need delete, Although the function definition cannot be changed,
    // the parameters may change, because some member functions will add a self parameter,
    // which needs to be added to the __all_routine_param table.
    // 3. body body replace, need delete, Because the function body may have changed. type_id has also changed
    // bool need_del_routines = true;
    // if ((udt_info.is_object_type() && udt_info.is_object_body_ddl()
    //      && old_udt_info->is_object_type() && old_udt_info->is_object_body_ddl())) {
    //           need_del_routines = false;
    // }
    if (udt_info.is_object_spec_ddl() && OB_FAIL(del_routines_in_udt(udt_info, trans, schema_guard))) {
      LOG_WARN("failed to delete functions", K(ret));
    } else if (OB_FAIL(schema_service->get_udt_sql_service().replace_udt(udt_info,
                                                                   old_udt_info,
                                                                   del_param_schema_version,
                                                                   &trans,
                                                                   ddl_stmt_str))) {
      LOG_WARN("replace udt info failed", K(udt_info), K(ret));
    } else {
      ARRAY_FOREACH(public_routine_infos, routine_idx) {
        ObRoutineInfo &routine_info = public_routine_infos.at(routine_idx);
        if (udt_info.is_object_spec_ddl()) {
          if (OB_FAIL(update_routine_info(routine_info,
                                        tenant_id,
                                        udt_info.get_type_id(),
                                        udt_info.get_database_id(),
                                        udt_info.get_database_id()))) {
            LOG_WARN("failed to update routine info", K(ret));
          } else if (OB_FAIL(schema_service->get_routine_sql_service().create_routine(routine_info,
                                                                        &trans, NULL))) {
            LOG_WARN("insert routine info failed", K(routine_info), K(ret));
          }
        } else {
          // update routine route sql
          int64_t new_schema_version = OB_INVALID_VERSION;
          OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
          OX (routine_info.set_schema_version(new_schema_version));
          OZ (schema_service->get_routine_sql_service().update_routine(routine_info, &trans));
        }
      }
    }
  }
  OZ (insert_dependency_infos(trans, dep_infos, tenant_id, udt_info.get_type_id(),
                                udt_info.get_schema_version(),
                                udt_info.get_database_id()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &udt_info))) {
      LOG_WARN("insert trigger error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObDDLOperator::drop_udt(const ObUDTTypeInfo &udt_info,
                            ObMySQLTransaction &trans,
                            ObSchemaGetterGuard &schema_guard,
                            const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  const int64_t type_id = udt_info.get_type_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(drop_obj_privs(tenant_id, type_id,
                                    static_cast<uint64_t>(ObObjectType::TYPE),
                                    trans))) {
    LOG_WARN("fail to drop_obj_privs", K(ret), K(udt_info), K(tenant_id));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (udt_info.is_object_spec_ddl() && OB_FAIL(del_routines_in_udt(udt_info,
                                                                      trans, schema_guard))) {
    LOG_WARN("fail to drop routines in udt", K(ret));
  } else if (OB_FAIL(schema_service->get_udt_sql_service().drop_udt(udt_info,
                                                                    new_schema_version,
                                                                    trans, ddl_stmt_str))) {
    LOG_WARN("drop udt info failed", K(udt_info), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (udt_info.is_object_spec_ddl() &&
        OB_INVALID_ID != ObUDTObjectType::mask_object_id(udt_info.get_object_spec_id(tenant_id))) {
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                    ObUDTObjectType::mask_object_id(udt_info.get_object_spec_id(tenant_id)),
                    udt_info.get_database_id()));
      if (udt_info.has_type_body() &&
          OB_INVALID_ID != ObUDTObjectType::mask_object_id(udt_info.get_object_body_id(tenant_id))) {
        OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                      ObUDTObjectType::mask_object_id(udt_info.get_object_body_id(tenant_id)),
                      udt_info.get_database_id()));
      }
    } else if (udt_info.is_object_body_ddl() &&
               OB_INVALID_ID != ObUDTObjectType::mask_object_id(udt_info.get_object_body_id(tenant_id))) {
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                    ObUDTObjectType::mask_object_id(udt_info.get_object_body_id(tenant_id)),
                    udt_info.get_database_id()));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (udt_info.is_object_spec_ddl()
             && OB_FAIL(ObDependencyInfo::delete_schema_object_dependency(
                    trans, tenant_id, type_id, new_schema_version, ObObjectType::TYPE_BODY))) {
    LOG_WARN("delete dependencies of type body related to the type spec failed",
             K(ret), K(tenant_id), K(type_id));
  } else if (OB_FAIL(ObDependencyInfo::delete_schema_object_dependency(
                 trans, tenant_id, type_id, new_schema_version, udt_info.get_object_type()))) {
    LOG_WARN("delete dependency of the type itself failed", K(ret), K(tenant_id), K(type_id));
  }

  if (OB_SUCC(ret)) {
    ObErrorInfo error_info;
    if (OB_FAIL(error_info.handle_error_info(trans, &udt_info))) {
      LOG_WARN("insert trigger error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObDDLOperator::del_routines_in_udt(const ObUDTTypeInfo &udt_info,
                                           ObMySQLTransaction &trans,
                                           ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = udt_info.get_tenant_id();
  uint64_t udt_id = udt_info.get_type_id();
  if (OB_INVALID_ID == tenant_id
      || OB_INVALID_ID == udt_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObArray<const ObRoutineInfo *> routines;
    if (OB_FAIL(schema_guard.get_routine_infos_in_udt(tenant_id, udt_id, routines))) {
      LOG_WARN("get routines in udt failed", K(tenant_id), K(udt_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < routines.count(); ++i) {
        const ObRoutineInfo *routine_info = routines.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_ISNULL(routine_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("routine info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_routine_sql_service().drop_routine(
                           *routine_info, new_schema_version, trans))) {
          LOG_WARN("drop routine failed", "routine_id", routine_info->get_routine_id(), K(ret));
        }
      }
    }
  }
  return ret;
}

//----Functions for managing UDF----
int ObDDLOperator::create_user_defined_function(share::schema::ObUDF &udf_info,
                                                common::ObMySQLTransaction &trans,
                                                const common::ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  uint64_t new_udf_id = OB_INVALID_ID;
  const uint64_t tenant_id = udf_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must exist", K(ret));
  } else if (OB_FAIL(schema_service->fetch_new_udf_id(tenant_id, new_udf_id))) {
    LOG_WARN("failed to fetch new_udf_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    udf_info.set_udf_id(new_udf_id);
    udf_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_udf_sql_service().insert_udf(udf_info, &trans, ddl_stmt_str))) {
      LOG_WARN("insert udf info failed", K(udf_info.get_name_str()), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_user_defined_function(const uint64_t tenant_id,
                                              const common::ObString &name,
                                              common::ObMySQLTransaction &trans,
                                              const common::ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must exist", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_udf_sql_service().delete_udf(
      tenant_id,
      name,
      new_schema_version,
      &trans,
      ddl_stmt_str))) {
    LOG_WARN("drop udf failed", K(tenant_id), K(name), K(ret));
  } else {/*do nothing*/}
  return ret;
}
//----End of functions for managing UDF----

int ObDDLOperator::insert_ori_schema_version(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t &ori_schema_version)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_INVALID_VERSION == ori_schema_version
      || !is_valid_tenant_id(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema version" , K(ret), K(tenant_id), K(table_id), K(ori_schema_version));
  } else if (OB_FAIL(schema_service->get_table_sql_service().insert_ori_schema_version(
             trans, tenant_id, table_id, ori_schema_version))) {
    LOG_WARN("insert_ori_schema_version failed", K(ret), K(tenant_id), K(table_id), K(ori_schema_version));
  }
  return ret;
}

int ObDDLOperator::insert_temp_table_info(ObMySQLTransaction &trans, const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (table_schema.is_ctas_tmp_table() || table_schema.is_tmp_table()) {
    if (is_inner_table(table_schema.get_table_id())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("create tmp sys table not allowed", K(ret), "table_id", table_schema.get_table_id());
    } else if (OB_FAIL(schema_service->get_table_sql_service().insert_temp_table_info(trans, table_schema))) {
      LOG_WARN("insert_temp_table_info failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::delete_temp_table_info(ObMySQLTransaction &trans, const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (is_inner_table(table_schema.get_table_id())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("create tmp sys table not allowed", K(ret), "table_id", table_schema.get_table_id());
  } else if (OB_FAIL(schema_service->get_table_sql_service().delete_from_all_temp_table(
              trans, table_schema.get_tenant_id(), table_schema.get_table_id()))) {
    LOG_WARN("insert_temp_table_info failed", K(ret));
  }
  return ret;
}

int ObDDLOperator::drop_trigger_cascade(const ObTableSchema &table_schema,
                                        ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObIArray<uint64_t> &trigger_list = table_schema.get_trigger_list();
  const ObTriggerInfo *trigger_info = NULL;
  uint64_t tenant_id = table_schema.get_tenant_id();
  uint64_t trigger_id = OB_INVALID_ID;
  OZ (schema_service_.get_tenant_schema_guard(tenant_id, schema_guard), tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < trigger_list.count(); i++) {
    OX (trigger_id = trigger_list.at(i));
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_id, trigger_info));
    OV (OB_NOT_NULL(trigger_info));
    OZ (drop_trigger(*trigger_info, trans, NULL), trigger_info);
  }
  return ret;
}

int ObDDLOperator::drop_inner_generated_index_column(ObMySQLTransaction &trans,
                                                    ObSchemaGetterGuard &schema_guard,
                                                    const ObTableSchema &index_schema,
                                                    ObTableSchema &new_data_table_schema)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *data_table = NULL;
  const ObColumnSchemaV2 *index_col = NULL;
  const uint64_t tenant_id = index_schema.get_tenant_id();
  uint64_t data_table_id = index_schema.get_data_table_id();
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(data_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table schema is unknown", K(data_table_id));
  } else if (!new_data_table_schema.is_valid()) {
    if (OB_FAIL(new_data_table_schema.assign(*data_table))) {
      LOG_WARN("fail to assign schema", K(ret));
    }
  }
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  } else {
    new_data_table_schema.set_in_offline_ddl_white_list(index_schema.get_in_offline_ddl_white_list());
  }
  for (ObTableSchema::const_column_iterator iter = index_schema.column_begin();
       OB_SUCC(ret) && iter != index_schema.column_end();
       ++iter) {
    ObColumnSchemaV2 *column_schema = (*iter);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), KPC(column_schema), K(index_schema));
    } else if (OB_UNLIKELY(is_shadow_column(column_schema->get_column_id()))) {
      continue;// skip the shadow rowkeys for unique index.
    // Generated columns on index table are converted to normal column,
    // we need to get column schema from data table here.
    } else if (OB_ISNULL(index_col = data_table->get_column_schema(
        tenant_id, column_schema->get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get index column schema failed", K(ret), K(tenant_id), KPC(column_schema));
    } else if (index_col->is_hidden() && index_col->is_generated_column() && !index_col->is_rowkey_column()) {
      // delete the generated column generated internally when the index is created,
      // This kind of generated column is hidden.
      // delete generated column in data table for spatial index
      bool exist_index = false;
      for (int64_t j = 0; OB_SUCC(ret) && !exist_index && j < simple_index_infos.count(); ++j) {
        const ObColumnSchemaV2 *tmp_col = NULL;
        if (simple_index_infos.at(j).table_id_ != index_schema.get_table_id()) {
          // If there are other indexes on the hidden column, they cannot be deleted.
          if (OB_FAIL(schema_guard.get_column_schema(tenant_id,
              simple_index_infos.at(j).table_id_, index_col->get_column_id(), tmp_col))) {
            LOG_WARN("get column schema from schema guard failed", KR(ret), K(tenant_id),
                     K(simple_index_infos.at(j).table_id_), K(index_col->get_column_id()));
          } else if (tmp_col != NULL) {
            exist_index = true;
          }
        }
      }
      // There are no other indexes, delete the hidden column.
      if (OB_SUCC(ret) && !exist_index) {
				// if generate column is not the last column // 1. update prev_column_id // 2. update inner table
        if (OB_FAIL(update_prev_id_for_delete_column(*data_table, new_data_table_schema, *index_col, trans))) {
          LOG_WARN("failed to update column previous id for delete column", K(ret));
        } else if (OB_FAIL(delete_single_column(trans, new_data_table_schema, index_col->get_column_name_str()))) {
          LOG_WARN("delete index inner generated column failed", K(new_data_table_schema), K(*index_col));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(alter_table_options(schema_guard,
                                    new_data_table_schema,
                                    *data_table,
                                    false,
                                    trans))) {
      LOG_WARN("alter table options failed", K(ret), K(new_data_table_schema));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < simple_index_infos.count(); ++j) {
        if (simple_index_infos.at(j).table_id_ == index_schema.get_table_id()) {
          simple_index_infos.remove(j);
          if (OB_FAIL(new_data_table_schema.set_simple_index_infos(simple_index_infos))) {
            LOG_WARN("fail to set simple index infos", K(ret));
          }
          break;
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::create_tablespace(ObTablespaceSchema &tablespace_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  uint64_t new_ts_id = OB_INVALID_ID;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = tablespace_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    tablespace_schema.set_schema_version(new_schema_version);
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(schema_service->fetch_new_tablespace_id(
          tablespace_schema.get_tenant_id(), new_ts_id))) {
    LOG_WARN("failed to fetch_new_table_id", K(ret));
  } else if (OB_INVALID_ID == new_ts_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema id", K(ret));
  } else if (FALSE_IT(tablespace_schema.set_tablespace_id(new_ts_id))) {
  } else if(OB_FAIL(schema_service->get_tablespace_sql_service().create_tablespace(
      tablespace_schema, trans, ddl_stmt_str))) {
    LOG_WARN("create tablespace failed", K(ret));
  }

  return ret;
}

int ObDDLOperator::alter_tablespace(ObTablespaceSchema &tablespace_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = tablespace_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    tablespace_schema.set_schema_version(new_schema_version);
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if(OB_FAIL(schema_service->get_tablespace_sql_service().alter_tablespace(
      tablespace_schema, trans, ddl_stmt_str))) {
    LOG_WARN("alter tablespace failed", K(ret));
  } else {
    // Change of tablespace state, change the cascading table_schema attribute under tablespace
    // Currently only supports encryption, it needs to be changed, so there is no more judgment here
    common::ObArray<const share::schema::ObTableSchema *> table_schemas;
    if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", K(ret));
    } else {
      for (int i = 0; i < table_schemas.count() && OB_SUCC(ret); ++i) {
        if (table_schemas.at(i)->get_tablespace_id() != OB_INVALID_ID &&
          table_schemas.at(i)->get_tablespace_id() == tablespace_schema.get_tablespace_id()) {
          if (OB_FAIL(update_tablespace_table(table_schemas.at(i), trans, tablespace_schema))) {
            LOG_WARN("fail to push back table schema", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_tablespace_table(
    const share::schema::ObTableSchema *table_schema,
    common::ObMySQLTransaction &trans,
    ObTablespaceSchema &tablespace_schema)
{
  int ret = OB_SUCCESS;
  // Currently this interface only supports cascading update encryption
  share::schema::ObTableSchema new_table_schema;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  int64_t tenant_id = table_schema->get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(new_table_schema.assign(*table_schema))) {
    LOG_WARN("fail to assign table schema", K(ret));
  } else {
    new_table_schema.set_schema_version(new_schema_version);
    new_table_schema.set_encryption_str(tablespace_schema.get_encryption_name());
    if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(trans,
      *table_schema, new_table_schema, OB_DDL_ALTER_TABLE, NULL))) {
      LOG_WARN("fail to update data table schema version", K(ret),
          K(new_table_schema.get_table_id()));
    }
  }
  return ret;
}

int ObDDLOperator::drop_tablespace(ObTablespaceSchema &tablespace_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema *> tables;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t tablespace_id = tablespace_schema.get_tablespace_id();
  const uint64_t tenant_id = tablespace_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    tablespace_schema.set_schema_version(new_schema_version);
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablespace(tenant_id, tablespace_id, tables))) {
    LOG_WARN("get tables in tablespace failed", K(tenant_id), KT(tablespace_id), K(ret));
  } else if (tables.count() > 0) {
    ret = OB_TABLESPACE_DELETE_NOT_EMPTY;
    LOG_WARN("can not delete a tablespace which is not empty", K(ret));
  } else if (OB_FAIL(schema_service->get_tablespace_sql_service().drop_tablespace(
      tablespace_schema, trans, ddl_stmt_str))) {
    LOG_WARN("drop tablespace failed", K(ret));
  }
  return ret;
}

// revise column info of check constraints
int ObDDLOperator::revise_constraint_column_info(
    obrpc::ObSchemaReviseArg arg,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.csts_array_.count(); ++i) {
      arg.csts_array_.at(i).set_schema_version(new_schema_version);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, arg.table_id_, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(arg.table_id_));
    } else if (nullptr == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, table schema must not be nullptr", K(ret));
    } else if (OB_FAIL(schema_service->get_table_sql_service().revise_check_cst_column_info(
                trans, *table_schema, arg.csts_array_))) {
      LOG_WARN("insert single constraint failed", K(ret), K(arg.csts_array_));
    } else if (OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(
               trans, tenant_id, arg.table_id_, table_schema->get_in_offline_ddl_white_list()))) {
      LOG_WARN("update data_table_schema version failed", K(ret), K(arg.table_id_));
    }
  }
  return ret;
}

// revise info of not null constraints
int ObDDLOperator::revise_not_null_constraint_info(
    obrpc::ObSchemaReviseArg arg,
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  const uint64_t table_id = arg.table_id_;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const ObTableSchema *ori_table_schema = NULL;
  ObSEArray<const ObColumnSchemaV2 *, 16> not_null_cols;
  const bool update_object_status_ignore_version = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, ori_table_schema))) {
    LOG_WARN("get table schema faield", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(ori_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_id), K(tenant_id));
  } else {
    bool is_heap_table = ori_table_schema->is_heap_table();
    ObTableSchema::const_column_iterator col_iter = ori_table_schema->column_begin();
    ObTableSchema::const_column_iterator col_iter_end = ori_table_schema->column_end();
    for (; col_iter != col_iter_end && OB_SUCC(ret); col_iter++) {
      if (OB_ISNULL(*col_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret), KPC(ori_table_schema));
      } else if (!(*col_iter)->is_nullable() && !(*col_iter)->is_hidden()) {
        if (!is_heap_table && (*col_iter)->is_rowkey_column()) {
          // do nothing for rowkey columns.
          // not filter rowkey column of no_pk_table since it may be partition key and can be null.
        } else if (OB_UNLIKELY((*col_iter)->has_not_null_constraint())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column with not null attr should not have not null constraint", K(ret),
                    KPC(*col_iter));
        } else if (OB_FAIL(not_null_cols.push_back(*col_iter))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret) || 0 == not_null_cols.count()) {
    // do nothing
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(arg.tenant_id_, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(arg.tenant_id_));
  } else {
    bool is_oracle_mode = false;
    uint64_t new_cst_id = OB_INVALID_ID;
    if (OB_FAIL(ori_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail check if oracle mode", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < not_null_cols.count(); ++i) {
      ObArenaAllocator allocator("ReviseNotNulCst");
      ObString cst_name;
      ObString check_expr_str;
      ObConstraint cst;
      const ObColumnSchemaV2 *col_schema = not_null_cols.at(i);
      uint64_t column_id = col_schema->get_column_id();
      bool cst_name_generated = false;
      ObColumnSchemaV2 new_col_schema;
      if (OB_FAIL(ObTableSchema::create_cons_name_automatically_with_dup_check(cst_name,
            ori_table_schema->get_table_name_str(),
            allocator,
            CONSTRAINT_TYPE_NOT_NULL,
            schema_guard,
            tenant_id,
            ori_table_schema->get_database_id(),
            10, /* retry_times */
            cst_name_generated,
            true))) {
        LOG_WARN("create cons name automatically failed", K(ret));
      } else if (OB_UNLIKELY(!cst_name_generated)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("duplicate name constraint already exists", K(ret), KPC(ori_table_schema));
      } else if (OB_FAIL(ObResolverUtils::create_not_null_expr_str(
                  col_schema->get_column_name_str(), allocator, check_expr_str, is_oracle_mode))) {
        LOG_WARN("create not null expr str failed", K(ret));
      } else if (OB_FAIL(schema_service->fetch_new_constraint_id(tenant_id, new_cst_id))) {
        LOG_WARN("failed to fetch new constraint id", K(ret));
      } else if (OB_FAIL(new_col_schema.assign(*col_schema))) {
        LOG_WARN("fail to assign column schema", KR(ret));
      } else {
        const bool only_history = false;
        const bool need_to_deal_with_cst_cols = true;
        const bool do_cst_revise = true;
        cst.set_tenant_id(tenant_id);
        cst.set_table_id(table_id);
        cst.set_constraint_id(new_cst_id);
        cst.set_schema_version(new_schema_version);
        cst.set_constraint_name(cst_name);
        cst.set_name_generated_type(GENERATED_TYPE_SYSTEM);
        cst.set_check_expr(check_expr_str);
        cst.set_constraint_type(CONSTRAINT_TYPE_NOT_NULL);
        cst.set_rely_flag(false);
        cst.set_enable_flag(true);
        cst.set_validate_flag(CST_FK_VALIDATED);

        new_col_schema.set_schema_version(new_schema_version);
        new_col_schema.add_not_null_cst();
        new_col_schema.set_nullable(true);
        if (OB_FAIL(cst.assign_not_null_cst_column_id(column_id))) {
          LOG_WARN("assign not null constraint column id failed", K(ret));
        } else if (OB_FAIL(schema_service->get_table_sql_service().add_single_constraint(
                  trans, cst, only_history,need_to_deal_with_cst_cols, do_cst_revise))) {
          LOG_WARN("add single constraint failed", K(ret), K(cst));
        } else if (OB_FAIL(schema_service->get_table_sql_service().update_single_column(
          trans, *ori_table_schema, *ori_table_schema, new_col_schema, false))) {
          LOG_WARN("update single column failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObTableSchema new_table_schema;
      if (OB_FAIL(new_table_schema.assign(*ori_table_schema))) {
        LOG_WARN("assign table schema failed", K(ret));
      } else {
        new_table_schema.set_schema_version(new_schema_version);
        if (OB_FAIL(schema_service->get_table_sql_service().update_table_attribute(
            trans, new_table_schema, OB_DDL_ADD_CONSTRAINT, update_object_status_ignore_version))) {
          LOG_WARN("update table attribute faield", K(ret));
        }
      }
    }
  }
  LOG_INFO("revise not null constraint info", K(ret), K(arg));
  return ret;
}

int ObDDLOperator::create_keystore(ObKeystoreSchema &keystore_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  uint64_t new_ts_id = OB_INVALID_ID;
  const uint64_t tenant_id = keystore_schema.get_tenant_id();
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    keystore_schema.set_schema_version(new_schema_version);
  }
  if (OB_FAIL(ret)) {
   /*do nothing*/
  } else if (is_mysql_inner_keystore_id(keystore_schema.get_keystore_id())) {
    /* If the id is equal to the internal keystore id, no fetch is required */
  } else if (OB_FAIL(schema_service->fetch_new_keystore_id(
      keystore_schema.get_tenant_id(), new_ts_id))) {
    LOG_WARN("failed to fetch_new_table_id", K(ret));
  } else if (OB_INVALID_ID == new_ts_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema id", K(ret));
  } else if (FALSE_IT(keystore_schema.set_keystore_id(new_ts_id))) {
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_service->get_keystore_sql_service().create_keystore(
        keystore_schema, trans, ddl_stmt_str))) {
      LOG_WARN("create keystore failed", K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::alter_keystore(ObKeystoreSchema &keystore_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObString *ddl_stmt_str,
    bool &is_set_key,
    bool is_kms)
{
  int ret = OB_SUCCESS;
  uint64_t new_ms_id = OB_INVALID_ID;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = keystore_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    keystore_schema.set_schema_version(new_schema_version);
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (is_set_key && !is_kms) {
    if (OB_FAIL(schema_service->fetch_new_master_key_id(
        keystore_schema.get_tenant_id(), new_ms_id))) {
      LOG_WARN("failed to fetch_new_master_id", K(ret));
    } else if (OB_INVALID_ID == new_ms_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid master id", K(ret));
    } else if (FALSE_IT(keystore_schema.set_master_key_id(new_ms_id))) {
    }
  }

  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if(OB_FAIL(schema_service->get_keystore_sql_service().alter_keystore(
      keystore_schema, trans, ddl_stmt_str))) {
    LOG_WARN("alter keystore failed", K(ret));
  } else {
    // The keystore state changes, the table_schema version needs to be pushed up.
    common::ObArray<const share::schema::ObSimpleTableSchemaV2 *> table_schemas;
    if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", K(ret));
    }
    common::ObArray<const share::schema::ObSimpleTableSchemaV2 *> need_encrypt_table_schemas;
    for (int i = 0; i < table_schemas.count() && OB_SUCC(ret); ++i) {
      if (table_schemas.at(i)->need_encrypt()) {
        ret = need_encrypt_table_schemas.push_back(table_schemas.at(i));
      }
    }
    for (int i = 0; i < need_encrypt_table_schemas.count() && OB_SUCC(ret); ++i) {
      const ObSimpleTableSchemaV2 *table_schema = need_encrypt_table_schemas.at(i);
      if (OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(trans,
          tenant_id, table_schema->get_table_id(), table_schema->get_in_offline_ddl_white_list()))) {
        LOG_WARN("fail to update data table schema version", K(ret), "id", table_schema->get_table_id());
      }
    }
  }
  return ret;
}

int ObDDLOperator::handle_label_se_policy_function(ObSchemaOperationType ddl_type,
                                                   const ObString &ddl_stmt_str,
                                                   ObSchemaGetterGuard &schema_guard,
                                                   ObLabelSePolicySchema &schema,
                                                   ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  ObSchemaService *schema_sql_service = NULL;

  if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  }

  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;

    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret));
    } else {
      schema.set_schema_version(new_schema_version);
    }
  }

  if (OB_SUCC(ret)) {
    bool is_policy_name_exist = false;
    bool is_column_name_exist = false;
    const ObLabelSePolicySchema *old_schema = NULL;
    const ObLabelSePolicySchema *temp_schema = NULL;

    //check if the schema is already existed
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard.get_label_se_policy_schema_by_name(
                    tenant_id, schema.get_policy_name_str(), old_schema))) {
        LOG_WARN("fail to check schema exist", K(ret));
      } else if (FALSE_IT(is_policy_name_exist = OB_NOT_NULL(old_schema))) {
      } else if (!schema.get_column_name_str().empty()
                 && OB_FAIL(schema_guard.get_label_se_policy_schema_by_column_name(
                              tenant_id, schema.get_column_name_str(), temp_schema))) {
        LOG_WARN("fail to check schema exist", K(ret));
      } else if (FALSE_IT(is_column_name_exist = OB_NOT_NULL(temp_schema))) {
      }
    }

    //other stuff case by case
    if (OB_SUCC(ret)) {
      uint64_t label_se_policy_id = OB_INVALID_ID;

      switch (ddl_type) {
      case OB_DDL_CREATE_LABEL_SE_POLICY:
        if (OB_UNLIKELY(is_policy_name_exist | is_column_name_exist)) {
          ret = is_policy_name_exist ? OB_OBJECT_NAME_EXIST : OB_ERR_COLUMN_DUPLICATE;
          LOG_WARN("policy already existed", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->fetch_new_label_se_policy_id(tenant_id, label_se_policy_id))) {
          LOG_WARN("fail to fetch new label se policy id", K(tenant_id), K(ret));
        } else {
          schema.set_label_se_policy_id(label_se_policy_id);
        }
        break;
      case OB_DDL_ALTER_LABEL_SE_POLICY:
      case OB_DDL_DROP_LABEL_SE_POLICY:
        if (OB_UNLIKELY(!is_policy_name_exist)) {
          ret = OB_OBJECT_NAME_NOT_EXIST;
          LOG_WARN("policy not existed", K(ret), K(tenant_id));
        } else {
          schema.set_label_se_policy_id(old_schema->get_label_se_policy_id());
          schema.set_column_name(old_schema->get_column_name_str());
          if (schema.get_default_options() < 0) {
            schema.set_default_options(old_schema->get_default_options());
          }
          if (schema.get_flag() < 0) {
            schema.set_flag(old_schema->get_flag());
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown ddl type", K(ret), K(ddl_type));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_sql_service->get_label_se_policy_sql_service()
                       .apply_new_schema(schema, trans, ddl_type, ddl_stmt_str))) {
      LOG_WARN("create policy info failed", K(ret));
    }
  }

  LOG_DEBUG("ddl operator create plan label security policy", K(schema));
  return ret;
}

int ObDDLOperator::handle_label_se_component_function(ObSchemaOperationType ddl_type,
                                                      const ObString &ddl_stmt_str,
                                                      const ObString &policy_name,
                                                      ObSchemaGetterGuard &schema_guard,
                                                      ObLabelSeComponentSchema &schema,
                                                      ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  ObSchemaService *schema_sql_service = NULL;

  if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  }

  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;

    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret));
    } else {
      schema.set_schema_version(new_schema_version);
    }
  }

  if (OB_SUCC(ret)) {

    //check and get policy id of the policy_name
    uint64_t policy_id = OB_INVALID_ID;
    if (OB_SUCC(ret)) {
      const ObLabelSePolicySchema *policy_schema = NULL;
      if (OB_FAIL(schema_guard.get_label_se_policy_schema_by_name(tenant_id,
                                                                  policy_name,
                                                                  policy_schema))) {
        LOG_WARN("fail to check schema exist", K(ret));
      } else if (OB_ISNULL(policy_schema)) {
        ret = OB_ERR_POLICY_STRING_NOT_FOUND;
        LOG_WARN("the schema exists, but the schema pointer is NULL", K(ret));
      } else {
        policy_id = policy_schema->get_label_se_policy_id();
      }
    }

    bool is_schema_exist = false;
    const ObLabelSeComponentSchema *old_schema = NULL;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard.get_label_se_component_schema_by_comp_num(tenant_id,
                                                                         policy_id,
                                                                         schema.get_comp_type(),
                                                                         schema.get_comp_num(),
                                                                         old_schema))) {
        LOG_WARN("fail to check schema exist", K(ret));
      } else {
        is_schema_exist = (old_schema != NULL);
      }
    }

    //check short name unique
    if (OB_SUCC(ret) && !schema.get_short_name_str().empty()
        && (!is_schema_exist || old_schema->get_short_name_str() != schema.get_short_name_str())) {
      bool is_exist = false;
      if (OB_FAIL(schema_service_.check_label_se_component_short_name_exist(tenant_id,
                                                                            policy_id,
                                                                            schema.get_comp_type(),
                                                                            schema.get_short_name_str(),
                                                                            is_exist))) {
        LOG_WARN("check component short name failed", K(ret));
      } else if (is_exist) {
        ret = OB_OBJECT_NAME_EXIST; //TODO [label]
      }
    }
    //check the long name unique
    if (OB_SUCC(ret) && !schema.get_long_name_str().empty()
        && (!is_schema_exist || old_schema->get_long_name_str() != schema.get_long_name_str())) {
      bool is_exist = false;
      if (OB_FAIL(schema_service_.check_label_se_component_long_name_exist(tenant_id,
                                                                           policy_id,
                                                                           schema.get_comp_type(),
                                                                           schema.get_long_name_str(),
                                                                           is_exist))) {
        LOG_WARN("check component long name failed", K(ret));
      } else if (is_exist) {
        ret = OB_OBJECT_NAME_EXIST; //TODO [label]
      }
    }

    //other stuff case by case
    if (OB_SUCC(ret)) {
      uint64_t label_se_component_id = OB_INVALID_ID;

      schema.set_label_se_policy_id(policy_id);

      switch (ddl_type) {
      case OB_DDL_CREATE_LABEL_SE_LEVEL:
        if (OB_UNLIKELY(is_schema_exist)) {
          ret = OB_OBJECT_NAME_EXIST;
          LOG_WARN("level already existed", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->fetch_new_label_se_component_id(tenant_id, label_se_component_id))) {
          LOG_WARN("fail to fetch new label se policy id", K(tenant_id), K(ret));
        } else {
          schema.set_label_se_component_id(label_se_component_id);
        }
        break;
      case OB_DDL_ALTER_LABEL_SE_LEVEL:
      case OB_DDL_DROP_LABEL_SE_LEVEL:
        if (OB_UNLIKELY(!is_schema_exist)) {
          ret = OB_OBJECT_NAME_NOT_EXIST;
          LOG_WARN("level not exist", K(ret), K(schema));
        } else {
          schema.set_label_se_component_id(old_schema->get_label_se_component_id());
          if (schema.get_short_name_str().empty()) {
            schema.set_short_name(old_schema->get_short_name_str());
          }
          if (schema.get_long_name_str().empty()) {
            schema.set_long_name(old_schema->get_long_name_str());
          }
          if (schema.get_parent_name_str().empty()) {
            schema.set_parent_name(old_schema->get_parent_name_str());
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown ddl type", K(ret), K(ddl_type));
      }
    }

    // When deleting component, it need to check whether the component is used in the label
    if (OB_SUCC(ret) && OB_DDL_DROP_LABEL_SE_LEVEL == ddl_type) {
      ObSEArray<const ObLabelSeLabelSchema *, 4> label_schemas;
      if (OB_FAIL(schema_guard.get_label_se_label_infos_in_tenant(tenant_id, label_schemas))) {
        LOG_WARN("fail to get label se schemas", K(ret));
      }

      const ObLabelSeLabelSchema *label_schema = NULL;
      ObLabelSeDecomposedLabel label_comps;
      ObLabelSeLabelCompNums label_nums;

      for (int64_t i = 0; OB_SUCC(ret) && i < label_schemas.count(); ++i) {
        label_comps.reset();
        if (OB_ISNULL(label_schema = label_schemas[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("label schema is NULL", K(ret));
        } else if (OB_FAIL(ObLabelSeResolver::resolve_label_text(label_schema->get_label_str(),
                                                                 label_comps))) {
          LOG_WARN("fail to resolve label text", K(ret));
        } else if (OB_FAIL(ObLabelSeUtil::convert_label_comps_name_to_num(tenant_id,
                                                                          policy_id,
                                                                          schema_guard,
                                                                          label_comps,
                                                                          label_nums))) {
          LOG_WARN("fail to convert comp names to num", K(ret));
        } else if (label_nums.level_num_ == schema.get_comp_num()) {
          ret = OB_ERR_LBAC_ERROR;
          LOG_USER_ERROR(OB_ERR_LBAC_ERROR, "level in use by existing labels");
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_sql_service->get_label_se_policy_sql_service()
                       .apply_new_schema(schema, trans, ddl_type, ddl_stmt_str))) {
      LOG_WARN("create policy info failed", K(ret));
    }
  }
  LOG_DEBUG("ddl operator create plan label security policy", K(schema));
  return ret;
}


int ObDDLOperator::handle_label_se_label_function(ObSchemaOperationType ddl_type,
                                                  const ObString &ddl_stmt_str,
                                                  const ObString &policy_name,
                                                  ObSchemaGetterGuard &schema_guard,
                                                  ObLabelSeLabelSchema &schema,
                                                  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  ObSchemaService *schema_sql_service = NULL;

  if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  }

  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;

    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret));
    } else {
      schema.set_schema_version(new_schema_version);
    }
  }

  if (OB_SUCC(ret)) {
    //check and get policy id of the policy_name
    uint64_t policy_id = OB_INVALID_ID;
    if (OB_SUCC(ret)) {
      const ObLabelSePolicySchema *policy_schema = NULL;
      if (OB_FAIL(schema_guard.get_label_se_policy_schema_by_name(tenant_id,
                                                                  policy_name,
                                                                  policy_schema))) {
        LOG_WARN("fail to check schema exist", K(ret));
      } else if (OB_ISNULL(policy_schema)) {
        ret = OB_ERR_POLICY_STRING_NOT_FOUND;
        LOG_WARN("the schema exists, but the schema pointer is NULL", K(ret));
      } else {
        policy_id = policy_schema->get_label_se_policy_id();
      }
    }

    //check label schema secondly
    const ObLabelSeLabelSchema *old_schema = NULL;
    const ObLabelSeLabelSchema *temp_schema = NULL;
    bool is_label_tag_exist = false;
    bool is_label_name_exist = false;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard.get_label_se_label_by_label_tag(tenant_id,
                                                               schema.get_label_tag(),
                                                               old_schema))) {
        LOG_WARN("fail to check schema exist", K(ret));
      } else if (FALSE_IT(is_label_tag_exist = OB_NOT_NULL(old_schema))) {
      } else if (!schema.get_label_str().empty()
                 && OB_FAIL(schema_guard.get_label_se_label_schema_by_name(tenant_id,
                                                                  schema.get_label_str(),
                                                                  temp_schema))) {
        LOG_WARN("fail to check schema exist", K(ret));
      } else if (FALSE_IT(is_label_name_exist = OB_NOT_NULL(temp_schema))) {
      }
    }

    //other stuff case by case
    if (OB_SUCC(ret)) {
      uint64_t label_se_label_id = OB_INVALID_ID;

      schema.set_label_se_policy_id(policy_id);

      switch (ddl_type) {
      case OB_DDL_CREATE_LABEL_SE_LABEL:
        if (OB_UNLIKELY(is_label_tag_exist | is_label_name_exist)) {
          ret = OB_OBJECT_NAME_EXIST;
          LOG_WARN("policy already existed", K(ret), K(schema));
        } else if (OB_FAIL(schema_sql_service->fetch_new_label_se_label_id(tenant_id, label_se_label_id))) {
          LOG_WARN("fail to fetch new label se policy id", K(schema), K(ret));
        } else {
          schema.set_label_se_label_id(label_se_label_id);
        }
        break;
      case OB_DDL_ALTER_LABEL_SE_LABEL:
      case OB_DDL_DROP_LABEL_SE_LABEL:
        if (OB_UNLIKELY(!is_label_tag_exist)) {
          ret = OB_OBJECT_NAME_NOT_EXIST;
          LOG_WARN("policy not existed", K(ret), K(schema));
        } else {
          schema.set_label_se_label_id(old_schema->get_label_se_label_id());
          if (schema.get_label_str().empty()) {
            schema.set_label(old_schema->get_label());
          } else if (old_schema->get_label_str() == schema.get_label_str()) {
            //do nothing
          } else if (OB_UNLIKELY(is_label_name_exist)) {
            ret = OB_OBJECT_NAME_EXIST;
            LOG_WARN("policy existed", K(ret), K(schema));
          }
          if (schema.get_flag() < 0) {
            schema.set_flag(old_schema->get_flag());
          } else {
            if (ddl_type == OB_DDL_ALTER_LABEL_SE_LABEL
                && old_schema->get_flag() != 0 && schema.get_flag() == 0) {
              ret = OB_ERR_LBAC_ERROR;
              LOG_USER_ERROR(OB_ERR_LBAC_ERROR, "cannot change data label to user label");
            }
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown ddl type", K(ret), K(ddl_type));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_sql_service->get_label_se_policy_sql_service()
                       .apply_new_schema(schema, trans, ddl_type, ddl_stmt_str))) {
      LOG_WARN("create policy info failed", K(ret));
    }
  }

  LOG_DEBUG("ddl operator handle label schema", K(schema));
  return ret;
}

int ObDDLOperator::drop_all_label_se_table_column(uint64_t tenant_id,
                                                  uint64_t policy_id,
                                                  ObMySQLTransaction &trans,
                                                  ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObTableSchema *, 32> tables;
  const ObLabelSePolicySchema *policy = NULL;
  ObString policy_column_name;
  ObSchemaService *schema_service = NULL;

  if (OB_ISNULL(schema_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, tables))) {
    LOG_WARN("fail to get all table schemas", K(ret));
  } else if (OB_FAIL(schema_guard.get_label_se_policy_schema_by_id(tenant_id, policy_id, policy))) {
    LOG_WARN("fail to get policy schema", K(ret));
  } else if (OB_ISNULL(policy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get policy schema", K(ret));
  } else {
    policy_column_name = policy->get_column_name_str();
  }
  for (int64_t t_i = 0; OB_SUCC(ret) && t_i < tables.count(); ++t_i) {
    const ObTableSchema *table = NULL;
    ObTableSchema new_table_schema;
    if (OB_ISNULL(table = tables.at(t_i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is NULL", K(ret));
    } else if (OB_FAIL(new_table_schema.assign(*table))) {
      LOG_WARN("fail to assign table schema", K(ret));
    }
    for (int64_t c_j = 0; OB_SUCC(ret) && c_j < table->get_label_se_column_ids().count(); ++c_j) {
      const ObColumnSchemaV2 *column = NULL;
      column = schema_guard.get_column_schema(table->get_tenant_id(),
                                              table->get_table_id(),
                                              table->get_label_se_column_ids().at(c_j));
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column schema", K(ret));
      } else if (0 == column->get_column_name_str().compare(policy_column_name)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("drop policy with table column", K(ret), K(table->get_table_name_str()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop policy with table column");
        /*
        int64_t new_schema_version = OB_INVALID_SCHEMA_VERSION;
        if (table->is_index_table()) {
          ret = OB_ERR_ALTER_INDEX_COLUMN;
          LOG_WARN("can't not drop index column", K(ret));
        } else if (OB_FAIL(update_prev_id_for_delete_column(*table, new_table_schema, *column, trans))) {
          LOG_WARN("fail to update prev id for delete column", K(ret));
        } else if (OB_FAIL(new_table_schema.delete_column(column->get_column_name_str()))) {
          LOG_WARN("fail to delete column", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service->get_table_sql_service().delete_single_column(
                             new_schema_version, trans, *table, *column))) {
          LOG_WARN("fail to delete column", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service->get_table_sql_service()
                           .sync_aux_schema_version_for_history(
                             trans, *table, new_schema_version ))) {
          LOG_WARN("fail to sync aux schema version column", K(ret));
        }
        */
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_all_label_se_labels_in_policy(uint64_t tenant_id,
                                                      uint64_t policy_id,
                                                      ObMySQLTransaction &trans,
                                                      const ObString &ddl_stmt_str,
                                                      ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;

  ObSchemaService *schema_sql_service = NULL;

  if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  }

  ObSEArray<const ObLabelSeLabelSchema*, 16> labels;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard.get_label_se_label_infos_in_tenant(tenant_id, labels))) {
      LOG_WARN("fail to get user level infos in tenant", K(ret), K(tenant_id));
    }
  }

  const ObLabelSeLabelSchema *label_schema = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;

  for (int64_t i = 0; OB_SUCC(ret) && i < labels.count(); ++i) {

    if (OB_ISNULL(label_schema = labels.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is NULL", K(ret));
    }

    if (OB_SUCC(ret) && label_schema->get_label_se_policy_id() == policy_id) {
      ObLabelSeLabelSchema schema;
      if (OB_FAIL(schema.assign(*label_schema))) {
        LOG_WARN("fail to assign label schema", KR(ret));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret));
      } else {
        schema.set_schema_version(new_schema_version);
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(schema_sql_service->get_label_se_policy_sql_service()
                           .apply_new_schema(schema, trans, OB_DDL_DROP_LABEL_SE_LABEL, ddl_stmt_str))) {
          LOG_WARN("apply schema failed", K(ret));
        }
      }
    }

  }

  return ret;
}

int ObDDLOperator::drop_all_label_se_components_in_policy(uint64_t tenant_id,
                                                          uint64_t policy_id,
                                                          ObMySQLTransaction &trans,
                                                          const ObString &ddl_stmt_str,
                                                          ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;

  ObSchemaService *schema_sql_service = NULL;

  if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  }

  ObSEArray<const ObLabelSeComponentSchema*, 16> components;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard.get_label_se_component_infos_in_tenant(tenant_id, components))) {
      LOG_WARN("fail to get user level infos in tenant", K(ret), K(tenant_id));
    }
  }

  const ObLabelSeComponentSchema *component_schema = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;

  for (int64_t i = 0; OB_SUCC(ret) && i < components.count(); ++i) {

    if (OB_ISNULL(component_schema = components.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is NULL", K(ret));
    }

    if (OB_SUCC(ret) && component_schema->get_label_se_policy_id() == policy_id) {
      ObLabelSeComponentSchema schema;
      ObSchemaOperationType ddl_type = OB_INVALID_DDL_OP;
      if (OB_FAIL(schema.assign(*component_schema))) {
        LOG_WARN("fail to assign component_schema", KR(ret));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret));
      } else {
        schema.set_schema_version(new_schema_version);
      }

      if (OB_SUCC(ret)) {
        switch (component_schema->get_comp_type()) {
        case static_cast<int64_t>(ObLabelSeComponentSchema::CompType::LEVEL):
          ddl_type = OB_DDL_DROP_LABEL_SE_LEVEL;
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("compartment and group not supported", K(ret), K(component_schema->get_comp_type()));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(schema_sql_service->get_label_se_policy_sql_service()
                           .apply_new_schema(schema, trans, ddl_type, ddl_stmt_str))) {
          LOG_WARN("apply schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_all_label_se_user_components(uint64_t tenant_id,
                                                     uint64_t user_id,
                                                     uint64_t policy_id,
                                                     ObMySQLTransaction &trans,
                                                     const ObString &ddl_stmt_str,
                                                     ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;

  ObSchemaService *schema_sql_service = NULL;

  if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  }

  ObSEArray<const ObLabelSeUserLevelSchema*, 4> user_levels;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard.get_label_se_user_level_infos_in_tenant(tenant_id, user_levels))) {
      LOG_WARN("fail to get user level infos in tenant", K(ret), K(tenant_id));
    }
  }

  const ObLabelSeUserLevelSchema *user_level_schema = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;

  for (int64_t i = 0; OB_SUCC(ret) && i < user_levels.count(); ++i) {

    if (OB_ISNULL(user_level_schema = user_levels.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is NULL", K(ret));
    }

    if (OB_SUCC(ret) && (user_level_schema->get_user_id() == user_id
                         || user_level_schema->get_label_se_policy_id() == policy_id)) {
      ObLabelSeUserLevelSchema schema;
      if (OB_FAIL(schema.assign(*user_level_schema))) {
        LOG_WARN("fail to assign ObLabelSeUserLevelSchema", KR(ret));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret));
      } else {
        schema.set_schema_version(new_schema_version);
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(schema_sql_service->get_label_se_policy_sql_service()
                           .apply_new_schema(schema, trans, OB_DDL_DROP_LABEL_SE_USER_LEVELS, ddl_stmt_str))) {
          LOG_WARN("apply schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::handle_label_se_user_level_function(ObSchemaOperationType ddl_type,
                                                       const ObString &ddl_stmt_str,
                                                       const ObString &policy_name,
                                                       ObSchemaGetterGuard &schema_guard,
                                                       ObLabelSeUserLevelSchema &schema,
                                                       ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  ObSchemaService *schema_sql_service = NULL;

  if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  }

  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;

    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret));
    } else {
      schema.set_schema_version(new_schema_version);
    }
  }

  uint64_t policy_id = OB_INVALID_ID;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLabelSeResolver::resolve_policy_name(tenant_id,
                                                       policy_name,
                                                       schema_guard,
                                                       policy_id))) {
      LOG_WARN("fail to resovle policy name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bool is_usera_exist = false;
    if (OB_FAIL(schema_service_.check_user_exist(tenant_id, schema.get_user_id(), is_usera_exist))) {
      LOG_WARN("fail to check schema exist", K(ret));
    } else if (OB_UNLIKELY(!is_usera_exist)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("the schema exists, but the schema pointer is NULL", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const ObLabelSeUserLevelSchema *old_schema = NULL;
    bool is_schema_exist = false;

    //check if the schema is already existed
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard.get_label_se_user_level_by_id(tenant_id,
                                                             schema.get_user_id(),
                                                             policy_id,
                                                             old_schema))) {
        LOG_WARN("fail to check schema exist", K(ret));
      } else {
        is_schema_exist = (old_schema != NULL);
      }
    }

    if (OB_SUCC(ret)) {
      schema.set_label_se_policy_id(policy_id);

      if (OB_DDL_CREATE_LABEL_SE_USER_LEVELS != ddl_type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ddl type", K(ret), K(ddl_type));
      } else {
        if (!is_schema_exist) {
          //new schema id
          uint64_t label_se_user_level_id = OB_INVALID_ID;

          if (OB_FAIL(schema_sql_service->fetch_new_label_se_user_level_id(tenant_id, label_se_user_level_id))) {
            LOG_WARN("fail to fetch new label se policy id", K(tenant_id), K(ret));
          } else {
            schema.set_label_se_user_level_id(label_se_user_level_id);
          }
        } else {
          //use old schema id
          ddl_type = OB_DDL_ALTER_LABEL_SE_USER_LEVELS;
          schema.set_label_se_user_level_id(old_schema->get_label_se_user_level_id());
          if (schema.get_maximum_level() < 0) {
            schema.set_maximum_level(old_schema->get_maximum_level());
          }
          if (schema.get_minimum_level() < 0) {
            schema.set_minimum_level(old_schema->get_minimum_level());
          }
          if (schema.get_default_level() < 0) {
            schema.set_default_level(old_schema->get_default_level());
          }
          if (schema.get_row_level() < 0) {
            schema.set_row_level(old_schema->get_row_level());
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_sql_service->get_label_se_policy_sql_service()
                       .apply_new_schema(schema, trans, ddl_type, ddl_stmt_str))) {
      LOG_WARN("create policy info failed", K(ret));
    }
  }

  LOG_DEBUG("ddl operator handle user level schema", K(schema));
  return ret;
}

int ObDDLOperator::handle_profile_function(
    ObProfileSchema &schema,
    ObMySQLTransaction &trans,
    share::schema::ObSchemaOperationType ddl_type,
    const ObString &ddl_stmt_str,
    ObSchemaGetterGuard &schema_guard
    )
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  ObSchemaService *schema_sql_service = NULL;
  if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  }
  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret));
    } else {
      schema.set_schema_version(new_schema_version);
    }
  }
  if (OB_SUCC(ret)) {
    bool is_schema_exist = false;
    const ObProfileSchema *old_schema = NULL;
    //check if the schema is already existed
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard.get_profile_schema_by_name(tenant_id,
                                                          schema.get_profile_name_str(),
                                                          old_schema))) {
        LOG_WARN("fail to check schema exist", K(ret));
      } else {
        is_schema_exist = (old_schema != NULL);
      }
    }
    //other stuff case by case
    if (OB_SUCC(ret)) {
      uint64_t profile_id = OB_INVALID_ID;
      switch (ddl_type) {
      case OB_DDL_CREATE_PROFILE:
        if (OB_UNLIKELY(is_schema_exist)) {
          ret = OB_ERR_PROFILE_STRING_ALREADY_EXISTS;
          LOG_USER_ERROR(OB_ERR_PROFILE_STRING_ALREADY_EXISTS, schema.get_profile_name_str().length(), schema.get_profile_name_str().ptr());
        } else if (is_oracle_inner_profile_id(schema.get_profile_id())) {
          /*do nothing*/
        } else if (OB_FAIL(schema_sql_service->fetch_new_profile_id(tenant_id, profile_id))) {
          LOG_WARN("fail to fetch new profile id", K(tenant_id), K(ret));
        } else {
          schema.set_profile_id(profile_id);
        }
        break;
      case OB_DDL_ALTER_PROFILE:
      case OB_DDL_DROP_PROFILE:
        if (OB_UNLIKELY(!is_schema_exist)) {
          ret = OB_ERR_PROFILE_STRING_DOES_NOT_EXIST;
          LOG_USER_ERROR(OB_ERR_PROFILE_STRING_DOES_NOT_EXIST, schema.get_profile_name_str().length(), schema.get_profile_name_str().ptr());
        } else {
          schema.set_profile_id(old_schema->get_profile_id());
          if (schema.get_failed_login_attempts() == ObProfileSchema::INVALID_VALUE) {
            schema.set_failed_login_attempts(old_schema->get_failed_login_attempts());
          }
          if (schema.get_password_lock_time() == ObProfileSchema::INVALID_VALUE) {
            schema.set_password_lock_time(old_schema->get_password_lock_time());
          }
          if (schema.get_password_life_time() == ObProfileSchema::INVALID_VALUE) {
            schema.set_password_life_time(old_schema->get_password_life_time());
          }
          if (schema.get_password_grace_time() == ObProfileSchema::INVALID_VALUE) {
            schema.set_password_grace_time(old_schema->get_password_grace_time());
          }
        }
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unknown ddl type", K(ret), K(ddl_type));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_sql_service->get_profile_sql_service()
                       .apply_new_schema(schema, trans, ddl_type, ddl_stmt_str))) {
      LOG_WARN("apply profile failed", K(ret));
    }
  }

  LOG_DEBUG("ddl operator", K(schema));
  return ret;
}

//----Functions for directory object----
int ObDDLOperator::create_directory(const ObString &ddl_str,
                                    const uint64_t user_id,
                                    share::schema::ObDirectorySchema &schema,
                                    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = schema.get_tenant_id();
  uint64_t new_directory_id = OB_INVALID_ID;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service should not be null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_new_directory_id(tenant_id, new_directory_id))) {
    LOG_WARN("failed to fetch new_directory_id", K(tenant_id), K(ret));
  } else if (FALSE_IT(schema.set_directory_id(new_directory_id))) {
    // do nothing
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, schema_version))) {
    LOG_WARN("failed to gen new_schema_version", K(tenant_id), K(ret));
  } else if (FALSE_IT(schema.set_schema_version(schema_version))) {
    // do nothing
  } else if (OB_FAIL(schema_service->get_directory_sql_service().apply_new_schema(
      schema, trans, ObSchemaOperationType::OB_DDL_CREATE_DIRECTORY, ddl_str))) {
    LOG_WARN("failed to create directory", K(schema.get_directory_name()), K(ret));
  } else {
    // after directory created, we should grant read/write/execute privilege to user
    ObTablePrivSortKey table_priv_key;
    table_priv_key.tenant_id_ = tenant_id;
    table_priv_key.user_id_ = user_id;

    ObPrivSet priv_set;
    priv_set = OB_PRIV_READ | OB_PRIV_WRITE | OB_PRIV_EXECUTE;

    ObObjPrivSortKey obj_priv_key;
    obj_priv_key.tenant_id_ = tenant_id;
    obj_priv_key.obj_id_ = new_directory_id;
    obj_priv_key.obj_type_ = static_cast<uint64_t>(ObObjectType::DIRECTORY);
    obj_priv_key.col_id_ = OB_COMPACT_COLUMN_INVALID_ID;
    obj_priv_key.grantor_id_ = OB_ORA_SYS_USER_ID;
    obj_priv_key.grantee_id_ = user_id;

    share::ObRawObjPrivArray priv_array;
    priv_array.push_back(OBJ_PRIV_ID_READ);
    priv_array.push_back(OBJ_PRIV_ID_WRITE);
    priv_array.push_back(OBJ_PRIV_ID_EXECUTE);
    if (OB_FAIL(this->grant_table(table_priv_key, priv_set, NULL, trans,
        priv_array, 0, obj_priv_key))) {
      LOG_WARN("fail to grant table", K(ret), K(table_priv_key), K(priv_set), K(obj_priv_key));
    }
  }
  return ret;
}

int ObDDLOperator::alter_directory(const ObString &ddl_str,
                                   share::schema::ObDirectorySchema &schema,
                                   common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service should not be null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("failed to gen new schema_version", K(ret), K(tenant_id));
  } else if (FALSE_IT(schema.set_schema_version(new_schema_version))) {
    // do nothing
  } else if (OB_FAIL(schema_service->get_directory_sql_service().apply_new_schema(
      schema, trans, ObSchemaOperationType::OB_DDL_ALTER_DIRECTORY, ddl_str))) {
    LOG_WARN("failed to alter directory", K(schema), K(ret));
  }
  return ret;
}

int ObDDLOperator::drop_directory(const ObString &ddl_str,
                                  share::schema::ObDirectorySchema &schema,
                                  common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t directory_id = schema.get_directory_id();
  const uint64_t directory_type = static_cast<uint64_t>(ObObjectType::DIRECTORY);
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(this->drop_obj_privs(tenant_id, directory_id, directory_type, trans))) {
    LOG_WARN("failed to drop obj privs for directory", K(ret),
        K(tenant_id), K(directory_id), K(directory_type));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("failed to gen new schema_version", K(ret), K(tenant_id));
  } else if (FALSE_IT(schema.set_schema_version(new_schema_version))) {
    // do nothing
  } else if (OB_FAIL(schema_service->get_directory_sql_service().apply_new_schema(
      schema, trans, ObSchemaOperationType::OB_DDL_DROP_DIRECTORY, ddl_str))) {
    LOG_WARN("failed to drop directory", K(schema), K(ret));
  }
  return ret;
}
//----End of functions for directory object----


int ObDDLOperator::alter_user_proxy(const ObUserInfo* client_user_info,
                                    const ObUserInfo* proxy_user_info,
                                    const uint64_t flags,
                                    const bool is_grant,
                                    const ObIArray<uint64_t> &role_ids,
                                    ObIArray<ObUserInfo> &users_to_update,
                                    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  ObArray<uint64_t> cur_role_ids;
  if (OB_ISNULL(schema_sql_service) || OB_ISNULL(client_user_info) || OB_ISNULL(proxy_user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(client_user_info->get_tenant_id(), new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(client_user_info->get_tenant_id()));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < client_user_info->get_proxied_user_info_cnt(); i++) {
      const ObProxyInfo *proxy_info = client_user_info->get_proxied_user_info_by_idx(i);
      if (OB_ISNULL(proxy_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else if (proxy_info->user_id_ == proxy_user_info->get_user_id()) {
        found = true;
        for (int64_t j = 0; OB_SUCC(ret) && j < proxy_info->role_id_cnt_; j++) {
          OZ (cur_role_ids.push_back(proxy_info->get_role_id_by_idx(j)));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!found) {
      if (!is_grant) {
        ret = OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT;
        LOG_WARN("revoke no such grant", K(ret));
      }
    }
  }
  ObArray<uint64_t> role_to_add;
  ObArray<uint64_t> role_to_del;
  for (int64_t i = 0; OB_SUCC(ret) && i < role_ids.count(); i++) {
    bool found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < cur_role_ids.count(); j++) {
      if (cur_role_ids.at(j) == role_ids.at(i)) {
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      if (OB_FAIL(role_to_add.push_back(role_ids.at(i)))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < cur_role_ids.count(); i++) {
    bool found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < role_ids.count(); j++) {
      if (cur_role_ids.at(i) == role_ids.at(j)) {
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      if (OB_FAIL(role_to_del.push_back(cur_role_ids.at(i)))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_proxy(client_user_info->get_tenant_id(),
                client_user_info->get_user_id(), proxy_user_info->get_user_id(), flags, new_schema_version, trans, is_grant))) {
      LOG_WARN("grant proxy failed", KPC(proxy_user_info), KPC(client_user_info), K(new_schema_version), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < role_to_add.count(); i++) {
        if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_proxy_role(client_user_info->get_tenant_id(),
                  client_user_info->get_user_id(), proxy_user_info->get_user_id(), role_to_add.at(i), new_schema_version, trans, true/*grant*/))) {
          LOG_WARN("grant proxy role failed", KPC(proxy_user_info), KPC(client_user_info), K(new_schema_version), K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < role_to_del.count(); i++) {
        if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_proxy_role(client_user_info->get_tenant_id(),
                  client_user_info->get_user_id(), proxy_user_info->get_user_id(), role_to_del.at(i), new_schema_version, trans, false/*delete*/))) {
          LOG_WARN("grant proxy role failed", KPC(proxy_user_info), KPC(client_user_info), K(new_schema_version), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool found_client_user = false;
      bool found_proxy_user = false;
      for (int64_t i = 0; OB_SUCC(ret) && !(found_client_user && found_proxy_user) && i < users_to_update.count(); i++) {
        if (users_to_update.at(i).get_user_id() == client_user_info->get_user_id()) {
          found_client_user = true;
        }
        if (users_to_update.at(i).get_user_id() == proxy_user_info->get_user_id()) {
          found_proxy_user = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (!found_client_user) {
          if (OB_FAIL(users_to_update.push_back(*client_user_info))) {
            LOG_WARN("fail to push back", K(ret));
          } else if (is_grant) {
            users_to_update.at(users_to_update.count() - 1).set_proxy_activated_flag(ObProxyActivatedFlag::PROXY_BEEN_ACTIVATED_BEFORE);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (client_user_info->get_user_id() == proxy_user_info->get_user_id()) {
          //skip
        } else if (!found_proxy_user) {
          if (OB_FAIL(users_to_update.push_back(*proxy_user_info))) {
            LOG_WARN("fail to push back", K(ret));
          } else if (is_grant) {
            users_to_update.at(users_to_update.count() - 1).set_proxy_activated_flag(ObProxyActivatedFlag::PROXY_BEEN_ACTIVATED_BEFORE);
          }
        }
      }
    }
  }

  return ret;
}

//----Functions for rls object----
int ObDDLOperator::create_rls_policy(ObRlsPolicySchema &schema,
                                     ObMySQLTransaction &trans,
                                     const ObString &ddl_stmt_str,
                                     bool is_update_table_schema,
                                     const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  uint64_t new_rls_policy_id = OB_INVALID_ID;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_new_rls_policy_id(tenant_id, new_rls_policy_id))) {
    LOG_WARN("failed to fetch new_rls_policy_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    schema.set_rls_policy_id(new_rls_policy_id);
    if (OB_FAIL(schema.set_ids_cascade())) {
      LOG_WARN("fail to set_ids_cascade", K(schema), K(ret));
    } else if (OB_FAIL(schema_service->get_rls_sql_service().apply_new_schema(
        schema, new_schema_version, trans, OB_DDL_CREATE_RLS_POLICY, ddl_stmt_str))) {
      LOG_WARN("fail to create rls policy", K(schema), K(ret));
    } else if (!is_update_table_schema) {
      // do nothing
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_FAIL(update_rls_table_schema(*table_schema, OB_DDL_CREATE_RLS_POLICY, trans))) {
      LOG_WARN("fail to update table schema", KR(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_rls_policy(const ObRlsPolicySchema &schema,
                                   ObMySQLTransaction &trans,
                                   const ObString &ddl_stmt_str,
                                   bool is_update_table_schema,
                                   const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_rls_sql_service().apply_new_schema(
      schema, new_schema_version, trans, OB_DDL_DROP_RLS_POLICY, ddl_stmt_str))) {
    LOG_WARN("fail to drop rls policy", K(schema), K(ret));
  } else if (!is_update_table_schema) {
    // do nothing
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(update_rls_table_schema(*table_schema, OB_DDL_DROP_RLS_POLICY, trans))) {
    LOG_WARN("fail to update table schema", KR(ret));
  }
  return ret;
}

int ObDDLOperator::alter_rls_policy(const ObRlsPolicySchema &schema,
                                    ObMySQLTransaction &trans,
                                    const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_rls_sql_service().apply_new_schema(
      schema, new_schema_version, trans, OB_DDL_ALTER_RLS_POLICY, ddl_stmt_str))) {
    LOG_WARN("fail to alter rls policy", K(schema), K(ret));
  } else if (OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(
      trans, tenant_id, schema.get_table_id(), false/*in offline ddl white list*/))) {
    LOG_WARN("fail to update table schema", KR(ret));
  }
  return ret;
}

int ObDDLOperator::create_rls_group(ObRlsGroupSchema &schema,
                                    ObMySQLTransaction &trans,
                                    const ObString &ddl_stmt_str,
                                    bool is_update_table_schema,
                                    const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  uint64_t new_rls_group_id = OB_INVALID_ID;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_new_rls_group_id(tenant_id, new_rls_group_id))) {
    LOG_WARN("failed to fetch new_rls_group_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    schema.set_rls_group_id(new_rls_group_id);
    if (OB_FAIL(schema_service->get_rls_sql_service().apply_new_schema(
        schema, new_schema_version, trans, OB_DDL_CREATE_RLS_GROUP, ddl_stmt_str))) {
      LOG_WARN("fail to create rls group", K(schema), K(ret));
    } else if (!is_update_table_schema) {
      // do nothing
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_FAIL(update_rls_table_schema(*table_schema, OB_DDL_CREATE_RLS_GROUP, trans))) {
      LOG_WARN("fail to update table schema", KR(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_rls_group(const ObRlsGroupSchema &schema,
                                  ObMySQLTransaction &trans,
                                  const ObString &ddl_stmt_str,
                                  bool is_update_table_schema,
                                  const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_rls_sql_service().apply_new_schema(
      schema, new_schema_version, trans, OB_DDL_DROP_RLS_GROUP, ddl_stmt_str))) {
    LOG_WARN("fail to drop rls group", K(schema), K(ret));
  } else if (!is_update_table_schema) {
    // do nothing
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(update_rls_table_schema(*table_schema, OB_DDL_DROP_RLS_GROUP, trans))) {
    LOG_WARN("fail to update table schema", KR(ret));
  }
  return ret;
}

int ObDDLOperator::create_rls_context(ObRlsContextSchema &schema,
                                      ObMySQLTransaction &trans,
                                      const ObString &ddl_stmt_str,
                                      bool is_update_table_schema,
                                      const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  uint64_t new_rls_context_id = OB_INVALID_ID;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_new_rls_context_id(tenant_id, new_rls_context_id))) {
    LOG_WARN("failed to fetch new_rls_context_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    schema.set_rls_context_id(new_rls_context_id);
    if (OB_FAIL(schema_service->get_rls_sql_service().apply_new_schema(
        schema, new_schema_version, trans, OB_DDL_CREATE_RLS_CONTEXT, ddl_stmt_str))) {
      LOG_WARN("fail to create rls context", K(schema), K(ret));
    } else if (!is_update_table_schema) {
      // do nothing
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_FAIL(update_rls_table_schema(*table_schema, OB_DDL_CREATE_RLS_CONTEXT, trans))) {
      LOG_WARN("fail to update table schema", KR(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_rls_context(const ObRlsContextSchema &schema,
                                    ObMySQLTransaction &trans,
                                    const ObString &ddl_stmt_str,
                                    bool is_update_table_schema,
                                    const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_rls_sql_service().apply_new_schema(
      schema, new_schema_version, trans, OB_DDL_DROP_RLS_CONTEXT, ddl_stmt_str))) {
    LOG_WARN("fail to drop rls context", K(schema), K(ret));
  } else if (!is_update_table_schema) {
    // do nothing
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(update_rls_table_schema(*table_schema, OB_DDL_DROP_RLS_CONTEXT, trans))) {
    LOG_WARN("fail to update table schema", KR(ret));
  }
  return ret;
}

int ObDDLOperator::drop_rls_sec_column(const ObRlsPolicySchema &schema,
                                       const ObRlsSecColumnSchema &column_schema,
                                       ObMySQLTransaction &trans,
                                       const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_rls_sql_service().drop_rls_sec_column(
      schema, column_schema, new_schema_version, trans, ddl_stmt_str))) {
    LOG_WARN("fail to drop rls policy", K(schema), K(ret));
  }
  return ret;
}

int ObDDLOperator::update_rls_table_schema(const ObTableSchema &table_schema,
                                           const ObSchemaOperationType ddl_type,
                                           ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  bool need_add_flag = false;
  bool need_del_flag = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else {
    switch(ddl_type) {
    case OB_DDL_CREATE_RLS_POLICY:
    case OB_DDL_CREATE_RLS_GROUP:
    case OB_DDL_CREATE_RLS_CONTEXT:
      if (!table_schema.has_table_flag(CASCADE_RLS_OBJECT_FLAG)) {
        need_add_flag = true;
      }
      break;
    case OB_DDL_DROP_RLS_POLICY:
      if (!table_schema.get_rls_group_ids().empty() ||
          !table_schema.get_rls_context_ids().empty()) {
        // do nothing
      } else if (1 == table_schema.get_rls_policy_ids().count()) {
        need_del_flag = true;
      }
      break;
    case OB_DDL_DROP_RLS_GROUP:
      if (!table_schema.get_rls_policy_ids().empty() ||
          !table_schema.get_rls_context_ids().empty()) {
        // do nothing
      } else if (1 == table_schema.get_rls_group_ids().count()) {
        need_del_flag = true;
      }
      break;
    case OB_DDL_DROP_RLS_CONTEXT:
      if (!table_schema.get_rls_policy_ids().empty() ||
          !table_schema.get_rls_group_ids().empty()) {
        // do nothing
      } else if (1 == table_schema.get_rls_context_ids().count()) {
        need_del_flag = true;
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown ddl type", KR(ret), K(ddl_type));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(need_add_flag && need_del_flag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted statue", KR(ret), K(ddl_type), K(table_schema));
  } else if (need_add_flag || need_del_flag) {
    share::schema::ObTableSchema new_table_schema;
    if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("failed to assign schema", K(ret));
    } else if (FALSE_IT(new_table_schema.add_or_del_table_flag(CASCADE_RLS_OBJECT_FLAG,
                                                               need_add_flag))) {
    } else if (OB_FAIL(update_table_attribute(new_table_schema, trans, OB_DDL_ALTER_TABLE))) {
      LOG_WARN("failed to update table attribute", K(ret));
    }
  } else {
    if (OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(
            trans, table_schema.get_tenant_id(), table_schema.get_table_id(), false))) {
      LOG_WARN("fail to update table schema", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_rls_object_in_drop_table(const ObTableSchema &table_schema,
                                                 ObMySQLTransaction &trans,
                                                 ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema.get_tenant_id();
  uint64_t table_id = table_schema.get_table_id();
  ObString empty_str;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_rls_policy_ids().count(); ++i) {
    const ObRlsPolicySchema *policy_schema = NULL;
    uint64_t policy_id = table_schema.get_rls_policy_ids().at(i);
    OZ (schema_guard.get_rls_policy_schema_by_id(tenant_id, policy_id, policy_schema));
    CK (OB_NOT_NULL(policy_schema));
    OZ (drop_rls_policy(*policy_schema, trans, empty_str, false, NULL));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_rls_group_ids().count(); ++i) {
    const ObRlsGroupSchema *group_schema = NULL;
    uint64_t group_id = table_schema.get_rls_group_ids().at(i);
    OZ (schema_guard.get_rls_group_schema_by_id(tenant_id, group_id, group_schema));
    CK (OB_NOT_NULL(group_schema));
    OZ (drop_rls_group(*group_schema, trans, empty_str, false, NULL));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_rls_context_ids().count(); ++i) {
    const ObRlsContextSchema *context_schema = NULL;
    uint64_t context_id = table_schema.get_rls_context_ids().at(i);
    OZ (schema_guard.get_rls_context_schema_by_id(tenant_id, context_id, context_schema));
    CK (OB_NOT_NULL(context_schema));
    OZ (drop_rls_context(*context_schema, trans, empty_str, false, NULL));
  }
  return ret;
}

//----End of functions for rls object----

int ObDDLOperator::init_tenant_profile(int64_t tenant_id,
                                        const share::schema::ObSysVariableSchema &sys_variable,
                                        common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", K(ret), K(tenant_id));
  } else if (!is_oracle_mode) {
    /*do nothing*/
  } else {
    ObProfileSchema profile_schema;
    profile_schema.set_tenant_id(tenant_id);
    profile_schema.set_profile_id(OB_ORACLE_TENANT_INNER_PROFILE_ID);
    profile_schema.set_password_lock_time(USECS_PER_DAY);
    profile_schema.set_failed_login_attempts(ObProfileSchema::UNLIMITED_VALUE);
    profile_schema.set_password_life_time(ObProfileSchema::UNLIMITED_VALUE);
    profile_schema.set_password_grace_time(ObProfileSchema::UNLIMITED_VALUE);
    profile_schema.set_password_verify_function("NULL");
    profile_schema.set_profile_name("DEFAULT");
    ObSchemaService *schema_service = schema_service_.get_schema_service();
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid schema service", K(ret));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (FALSE_IT(profile_schema.set_schema_version(new_schema_version))) {
    } else if (OB_FAIL(schema_service->get_profile_sql_service().apply_new_schema(
          profile_schema, trans, ObSchemaOperationType::OB_DDL_CREATE_PROFILE, NULL))) {
      LOG_WARN("create default profile failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::init_tenant_keystore(int64_t tenant_id,
                                        const share::schema::ObSysVariableSchema &sys_variable,
                                        common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", K(ret), K(tenant_id));
  } else if (is_oracle_mode) {
    // nothing
  } else {
    ObSchemaService *schema_service = schema_service_.get_schema_service();
    ObKeystoreSchema keystore_schema;
    keystore_schema.set_keystore_id(OB_MYSQL_TENANT_INNER_KEYSTORE_ID);
    keystore_schema.set_tenant_id(tenant_id);
    keystore_schema.set_status(2);
    keystore_schema.set_keystore_name("mysql_keystore");
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid schema service", K(ret));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (FALSE_IT(keystore_schema.set_schema_version(new_schema_version))) {
    } else if (OB_FAIL(schema_service->get_keystore_sql_service().create_keystore(
          keystore_schema, trans, NULL))) {
      LOG_WARN("create keystore failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::insert_dependency_infos(common::ObMySQLTransaction &trans,
                                           ObIArray<ObDependencyInfo> &dep_infos,
                                           uint64_t tenant_id,
                                           uint64_t dep_obj_id,
                                           uint64_t schema_version, uint64_t owner_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == owner_id
   || OB_INVALID_ID == dep_obj_id
   || OB_INVALID_ID == tenant_id
   || OB_INVALID_SCHEMA_VERSION == schema_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("illegal schema version or owner id", K(ret), K(schema_version),
                                                   K(owner_id), K(dep_obj_id));
  } else {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < dep_infos.count(); ++i) {
      ObDependencyInfo & dep = dep_infos.at(i);
      dep.set_tenant_id(tenant_id);
      dep.set_dep_obj_id(dep_obj_id);
      dep.set_dep_obj_owner_id(owner_id);
      dep.set_schema_version(schema_version);
      OZ (dep.insert_schema_object_dependency(trans));
    }
  }

  return ret;
}

int ObDDLOperator::update_table_status(const ObTableSchema &orig_table_schema,
                                       const int64_t schema_version,
                                       const ObObjectStatus new_status,
                                       const bool update_object_status_ignore_version,
                                       ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t data_version = 0;
  ObTableSchema new_schema;
  const ObSchemaOperationType op = OB_DDL_ALTER_TABLE;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(schema_version));
  } else if (!update_object_status_ignore_version && OB_FAIL(GET_MIN_DATA_VERSION(orig_table_schema.get_tenant_id(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (!update_object_status_ignore_version && data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version and feature mismatch", K(ret));
  } else if (OB_FAIL(new_schema.assign(orig_table_schema))) {
    LOG_WARN("failed to assign table schema", K(ret));
  } else if (FALSE_IT(new_schema.set_object_status(new_status))) {
  } else if (FALSE_IT(new_schema.set_schema_version(schema_version))) {
  } else if (new_schema.get_column_count() > 0
             && FALSE_IT(new_schema.set_view_column_filled_flag(ObViewColumnFilledFlag::FILLED))) {
    /*
    *Except for drop view, there is no way to reduce the column count,
    *and there is no need to consider the table mode of this view before
    */
  } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_attribute(trans, new_schema, op, update_object_status_ignore_version) )) {
    LOG_WARN("update table status failed", K(ret));
  }
  return ret;
}

int ObDDLOperator::update_view_columns(const ObTableSchema &view_schema,
                                        common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t data_version = 0;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(view_schema.get_tenant_id(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version and feature mismatch", K(ret));
  } else if (OB_FAIL(schema_service->get_table_sql_service().update_view_columns(trans, view_schema))) {
    LOG_WARN("failed to add columns", K(ret));
  }
  return ret;
}

// only used in upgrading
int ObDDLOperator::reset_view_status(common::ObMySQLTransaction &trans,
                                     const uint64_t tenant_id,
                                     const ObTableSchema *table)
{
  int ret = OB_SUCCESS;
  ObObjectStatus new_status = ObObjectStatus::INVALID;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  int64_t schema_version = OB_INVALID_VERSION;
  const bool update_object_status_ignore_version = true;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else {
    if (OB_ISNULL(table) || !table->is_view_table()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get wrong schema", K(ret), KP(table));
    } else if (OB_FAIL(schema_service->gen_new_schema_version(tenant_id, schema_version, schema_version))) {
      LOG_WARN("failed to gen new schema version", K(ret));
    } else if (OB_FAIL(update_table_status(*table,
                                            schema_version,
                                            new_status,
                                            update_object_status_ignore_version,
                                            trans))) {
      LOG_WARN("failed to update table status", K(ret));
    }
  }
  return ret;
}


// only used in upgrading
int ObDDLOperator::try_add_dep_info_for_synonym(const ObSimpleSynonymSchema *synonym_info,
                                                common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  //add def obj info if exists
  bool ref_exists = false;
  ObObjectType ref_type = ObObjectType::INVALID;
  uint64_t ref_obj_id = OB_INVALID_ID;
  uint64_t ref_schema_version = share::OB_INVALID_SCHEMA_VERSION;
  if (OB_ISNULL(synonym_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unecpected synonym info", K(ret));
  } else if (OB_FAIL(ObSQLUtils::find_synonym_ref_obj(synonym_info->get_object_database_id(),
                                                      synonym_info->get_object_name_str(),
                                                      synonym_info->get_tenant_id(),
                                                      ref_exists,
                                                      ref_obj_id,
                                                      ref_type,
                                                      ref_schema_version))) {
    LOG_WARN("failed to find synonym ref obj", K(ret));
  } else {
    if (ref_exists) {
      ObDependencyInfo dep;
      dep.set_dep_obj_id(synonym_info->get_synonym_id());
      dep.set_dep_obj_type(ObObjectType::SYNONYM);
      dep.set_ref_obj_id(ref_obj_id);
      dep.set_ref_obj_type(ref_type);
      dep.set_dep_timestamp(-1);
      dep.set_ref_timestamp(ref_schema_version);
      dep.set_tenant_id(synonym_info->get_tenant_id());
      if (OB_FAIL(dep.insert_schema_object_dependency(trans))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          LOG_TRACE("synonym have dep info before", K(*synonym_info));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::exchange_table_partitions(const share::schema::ObTableSchema &orig_table_schema,
                                             share::schema::ObTableSchema &inc_table_schema,
                                             share::schema::ObTableSchema &del_table_schema,
                                             common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().exchange_part_info(
                     trans,
                     orig_table_schema,
                     inc_table_schema,
                     del_table_schema,
                     new_schema_version))) {
    LOG_WARN("exchange part info failed", K(ret));
  }
  return ret;
}

int ObDDLOperator::exchange_table_subpartitions(const share::schema::ObTableSchema &orig_table_schema,
                                                share::schema::ObTableSchema &inc_table_schema,
                                                share::schema::ObTableSchema &del_table_schema,
                                                common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().exchange_subpart_info(
                     trans,
                     orig_table_schema,
                     inc_table_schema,
                     del_table_schema,
                     new_schema_version))) {
    LOG_WARN("delete inc part info failed", K(ret));
  }
  return ret;
}

int ObDDLOperator::get_target_auto_inc_sequence_value(const uint64_t tenant_id,
                                                      const uint64_t table_id,
                                                      const uint64_t column_id,
                                                      uint64_t &sequence_value,
                                                      common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  sequence_value = OB_INVALID_ID;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id || OB_INVALID_ID == column_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(column_id));
  } else {
    ObSqlString sql;
    const uint64_t exec_tenant_id = tenant_id;
    const char *table_name = OB_ALL_AUTO_INCREMENT_TNAME;
    if (OB_FAIL(sql.assign_fmt(" SELECT  sequence_value FROM %s WHERE tenant_id = %lu AND sequence_key = %lu"
                               " AND column_id = %lu FOR UPDATE",
                               table_name,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                               column_id))) {
      LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(table_id), K(column_id));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        uint64_t sequence_table_id = OB_ALL_AUTO_INCREMENT_TID;
        if (OB_FAIL(trans.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("failed to read data", K(ret));
        } else if (NULL == (result = res.get_result())) {
          LOG_WARN("failed to get result", K(ret));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("failed to get next", K(ret));
          if (OB_ITER_END == ret) {
            // auto-increment column has been deleted
            ret = OB_SCHEMA_ERROR;
            LOG_WARN("failed to get next", K(ret));
          }
        } else if (OB_FAIL(result->get_uint("sequence_value", sequence_value))) {
          LOG_WARN("failed to get int_value.", K(ret));
        }
        if (OB_SUCC(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_ITER_END != (tmp_ret = result->next())) {
            if (OB_SUCCESS == tmp_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("more than one row", K(ret), K(tenant_id), K(table_id), K(column_id));
            } else {
              ret = tmp_ret;
              LOG_WARN("fail to iter next row", K(ret), K(tenant_id), K(table_id), K(column_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::set_target_auto_inc_sync_value(const uint64_t tenant_id,
                                                  const uint64_t table_id,
                                                  const uint64_t column_id,
                                                  const uint64_t new_sequence_value,
                                                  const uint64_t new_sync_value,
                                                  common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id || OB_INVALID_ID == column_id || new_sequence_value < 0 || new_sync_value < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(column_id), K(new_sequence_value), K(new_sync_value));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    const char *table_name = OB_ALL_AUTO_INCREMENT_TNAME;
    if (OB_FAIL(sql.assign_fmt(
                "UPDATE %s SET sequence_value = %lu, sync_value = %lu WHERE tenant_id=%lu AND sequence_key=%lu AND column_id=%lu",
                table_name, new_sequence_value, new_sync_value,
                ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), ObSchemaUtils::get_extract_schema_id(tenant_id, table_id), column_id))) {
      LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(table_id), K(column_id), K(new_sequence_value), K(new_sync_value));
    } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute", K(ret), K(sql));
    }
  }
  return ret;
}

int ObDDLOperator::get_target_sequence_sync_value(const uint64_t tenant_id,
                                                  const uint64_t sequence_id,
                                                  common::ObMySQLTransaction &trans,
                                                  ObIAllocator &allocator,
                                                  common::number::ObNumber &next_value)
{
  int ret = OB_SUCCESS;
  next_value.set_zero();
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == sequence_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(sequence_id));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_impl->get_sequence_sql_service().get_sequence_sync_value(tenant_id,
                                                                                             sequence_id,
                                                                                             true,/*is select for update*/
                                                                                             trans,
                                                                                             allocator,
                                                                                             next_value))) {
    LOG_WARN("fail to get sequence sync value", K(ret), K(tenant_id), K(sequence_id));
  }
  return ret;
}

int ObDDLOperator::alter_target_sequence_start_with(const ObSequenceSchema &sequence_schema, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  if (OB_UNLIKELY(!sequence_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sequence_schema));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_impl->get_sequence_sql_service().alter_sequence_start_with(sequence_schema, trans))) {
    LOG_WARN("fail to alter sequence start with", K(ret), K(sequence_schema));
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
