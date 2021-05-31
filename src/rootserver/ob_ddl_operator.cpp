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
#include "share/partition_table/ob_partition_table_operator.h"
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
#include "share/schema/ob_profile_sql_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/config/ob_server_config.h"
#include "share/ob_partition_modify.h"
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/expr/ob_raw_expr_modify_column_name.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
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

namespace oceanbase {

using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
using namespace sql;

namespace rootserver {

ObSysStat::Item::Item(ObSysStat::ItemList& list, const char* name, const char* info) : name_(name), info_(info)
{
  value_.set_int(0);
  const bool add_success = list.add_last(this);
  if (!add_success) {
    LOG_WARN("add last failed");
  }
}

#define MAX_ID_NAME_INFO(id) ObMaxIdFetcher::get_max_id_name(id), ObMaxIdFetcher::get_max_id_info(id)
ObSysStat::ObSysStat()
    : ob_max_used_tenant_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_TENANT_ID_TYPE)),
      ob_max_used_unit_config_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_UNIT_CONFIG_ID_TYPE)),
      ob_max_used_resource_pool_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_RESOURCE_POOL_ID_TYPE)),
      ob_max_used_unit_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_UNIT_ID_TYPE)),
      ob_max_used_server_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_SERVER_ID_TYPE)),
      ob_max_used_database_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_DATABASE_ID_TYPE)),
      ob_max_used_tablegroup_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_TABLEGROUP_ID_TYPE)),
      ob_max_used_table_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_TABLE_ID_TYPE)),
      ob_max_used_user_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_USER_ID_TYPE)),
      ob_max_used_outline_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_OUTLINE_ID_TYPE)),
      ob_max_used_sequence_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_SEQUENCE_ID_TYPE)),
      ob_max_used_synonym_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_SYNONYM_ID_TYPE)),
      ob_max_used_udf_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_UDF_ID_TYPE)),
      ob_max_used_constraint_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_CONSTRAINT_ID_TYPE)),
      ob_max_used_profile_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_PROFILE_ID_TYPE)),
      ob_max_used_dblink_id_(item_list_, MAX_ID_NAME_INFO(OB_MAX_USED_DBLINK_ID_TYPE))
{}

// set values after bootstrap
int ObSysStat::set_initial_values(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID == tenant_id) {
    ob_max_used_tenant_id_.value_.set_int(OB_USER_TENANT_ID);
    ob_max_used_unit_config_id_.value_.set_int(OB_USER_UNIT_CONFIG_ID);
    ob_max_used_resource_pool_id_.value_.set_int(OB_USER_RESOURCE_POOL_ID);
    ob_max_used_unit_id_.value_.set_int(OB_USER_UNIT_ID);
    ob_max_used_server_id_.value_.set_int(OB_INIT_SERVER_ID);
  } else {
    const int64_t root_own_count = 5;
    for (int64_t i = 0; i < root_own_count && OB_SUCC(ret); ++i) {
      const bool remove_succeed = item_list_.remove_first();
      if (!remove_succeed) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("remove_succeed should be true", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ob_max_used_database_id_.value_.set_int(combine_id(tenant_id, OB_USER_START_DATABASE_ID));
    uint64_t tablegroup_id = OB_USER_TABLEGROUP_ID;
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2000) {
      // The encode patten changed after 2.0, so tablegroup is set by new encode patten.
      tablegroup_id = tablegroup_id | OB_TABLEGROUP_MASK;
    }
    ob_max_used_tablegroup_id_.value_.set_int(combine_id(tenant_id, tablegroup_id));
    ob_max_used_table_id_.value_.set_int(combine_id(tenant_id, OB_MIN_USER_TABLE_ID));
    ob_max_used_user_id_.value_.set_int(combine_id(tenant_id, OB_USER_ID));
    ob_max_used_outline_id_.value_.set_int(combine_id(tenant_id, OB_USER_OUTLINE_ID));
    ob_max_used_sequence_id_.value_.set_int(combine_id(tenant_id, OB_USER_SEQUENCE_ID));
    ob_max_used_synonym_id_.value_.set_int(combine_id(tenant_id, OB_USER_SYNONYM_ID));
    ob_max_used_udf_id_.value_.set_int(combine_id(tenant_id, OB_USER_UDF_ID));
    ob_max_used_constraint_id_.value_.set_int(combine_id(tenant_id, OB_USER_CONSTRAINT_ID));
    ob_max_used_profile_id_.value_.set_int(combine_id(tenant_id, OB_USER_PROFILE_ID));
    ob_max_used_dblink_id_.value_.set_int(combine_id(tenant_id, OB_USER_DBLINK_ID));
  }
  return ret;
}

ObDDLOperator::ObDDLOperator(ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& sql_proxy)
    : schema_service_(schema_service), sql_proxy_(sql_proxy)
{}

ObDDLOperator::~ObDDLOperator()
{}

int64_t ObDDLOperator::get_last_operation_schema_version() const
{
  return schema_service_.get_schema_service() == NULL
             ? common::OB_INVALID_VERSION
             : schema_service_.get_schema_service()->get_last_operation_schema_version();
}

int ObDDLOperator::get_tenant_last_operation_schema_version(const uint64_t tenant_id, int64_t& schema_version) const
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  schema_version = schema_service_.get_schema_service() == NULL
                       ? common::OB_INVALID_VERSION
                       : schema_service_.get_schema_service()->get_last_operation_schema_version();
  return ret;
}

int ObDDLOperator::create_tenant(ObTenantSchema& tenant_schema, const ObSchemaOperationType op,
    ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_DDL_ADD_TENANT != op && OB_DDL_ADD_TENANT_START != op && OB_DDL_ADD_TENANT_END != op) {
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
      tenant_status = tenant_schema.is_restore() ? TENANT_STATUS_RESTORE : TENANT_STATUS_CREATING;
    }
    tenant_schema.set_schema_version(new_schema_version);
    tenant_schema.set_status(tenant_status);
    if (OB_FAIL(schema_service->get_tenant_sql_service().insert_tenant(tenant_schema, op, trans, ddl_stmt_str))) {
      LOG_WARN("insert tenant failed", K(tenant_schema), K(ret));
    } else {
    }
  }
  LOG_INFO("create tenant",
      K(ret),
      "tenant_id",
      tenant_schema.get_tenant_id(),
      "cost",
      ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::drop_tenant(
    const uint64_t tenant_id, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();

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
    ObTenantSchema& new_tenant_schema, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = new_tenant_schema.get_tenant_id();
  const int64_t timeout = THIS_WORKER.get_timeout_remain();
  int64_t gts_value = 0;
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null");
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(OB_SYS_TENANT_ID, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret));
  } else if (OB_FAIL(ObMultiClusterUtil::get_gts(tenant_id, gts_value, timeout))) {
    LOG_WARN("fail to get gts", K(ret), K(tenant_id), K(timeout));
  } else {
    new_tenant_schema.set_schema_version(new_schema_version);
    new_tenant_schema.set_drop_tenant_time(gts_value);
    new_tenant_schema.set_status(TENANT_STATUS_DROPPING);
    new_tenant_schema.set_in_recyclebin(false);
    if (OB_FAIL(schema_service_impl->get_tenant_sql_service().delay_to_drop_tenant(
            new_tenant_schema, trans, ddl_stmt_str))) {
      LOG_WARN("schema_service_impl delay to drop tenant failed", K(new_tenant_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_tenant_to_recyclebin(ObSqlString& new_tenant_name, ObTenantSchema& tenant_schema,
    ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
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
    recycle_object.set_table_id(OB_INVALID_ID);
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
      if (OB_FAIL(schema_service_impl->insert_recyclebin_object(recycle_object, trans))) {
        LOG_WARN("insert recycle object failed", K(ret));
      } else if (OB_FAIL(schema_service_impl->get_tenant_sql_service().drop_tenant_to_recyclebin(
                     new_tenant_schema, trans, OB_DDL_DROP_TENANT_TO_RECYCLEBIN, ddl_stmt_str))) {
        LOG_WARN("alter_tenant failed,", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::check_tenant_exist(
    share::schema::ObSchemaGetterGuard& schema_guard, const ObString& tenant_name, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  const ObTenantSchema* tenant = NULL;
  if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, tenant))) {
    LOG_WARN("fail get tenant info", K(ret));
  } else if (OB_ISNULL(tenant)) {
    is_exist = false;
  } else {
    is_exist = true;
  }
  return ret;
}

int ObDDLOperator::flashback_tenant_from_recyclebin(const share::schema::ObTenantSchema& tenant_schema,
    ObMySQLTransaction& trans, const ObString& new_tenant_name, share::schema::ObSchemaGetterGuard& schema_guard,
    const ObString& ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
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
                 OB_SYS_TENANT_ID, tenant_schema.get_tenant_name(), ObRecycleObject::TENANT, trans, recycle_objs))) {
    LOG_WARN("get_recycle_object failed", K(ret));
  } else if (recycle_objs.size() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected recycle object num",
        K(ret),
        "tenant_name",
        tenant_schema.get_tenant_name_str(),
        "size",
        recycle_objs.size());
  } else if (OB_FAIL(new_tenant_schema.assign(tenant_schema))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    const ObRecycleObject& recycle_obj = recycle_objs.at(0);
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
      if (OB_FAIL(check_tenant_exist(schema_guard, new_tenant_schema.get_tenant_name_str(), is_tenant_exist))) {
        LOG_WARN("fail to check tenant", K(ret));
      } else if (is_tenant_exist) {
        ret = OB_TENANT_EXIST;
        LOG_USER_ERROR(OB_TENANT_EXIST, new_tenant_schema.get_tenant_name_str().ptr());
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(OB_SYS_TENANT_ID, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(OB_SYS_TENANT_ID));
      } else if (FALSE_IT(new_tenant_schema.set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service->get_tenant_sql_service().alter_tenant(
                     new_tenant_schema, trans, OB_DDL_FLASHBACK_TENANT, &ddl_stmt_str))) {
        LOG_WARN("update_tenant failed", K(ret), K(new_tenant_schema));
      } else if (OB_FAIL(schema_service->delete_recycle_object(OB_SYS_TENANT_ID, recycle_obj, trans))) {
        LOG_WARN("delete_recycle_object failed", K(ret), K(recycle_obj));
      }
    }
  }
  return ret;
}

int ObDDLOperator::purge_tenant_in_recyclebin(
    const share::schema::ObTenantSchema& tenant_schema, ObMySQLTransaction& trans, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_INVALID_ID == tenant_schema.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else {
    ObArray<ObRecycleObject> recycle_objs;
    if (OB_FAIL(schema_service->fetch_recycle_object(
            OB_SYS_TENANT_ID, tenant_schema.get_tenant_name_str(), ObRecycleObject::TENANT, trans, recycle_objs))) {
      LOG_WARN("get_recycle_object failed", K(ret));
    } else if (recycle_objs.size() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected recycle object num",
          K(ret),
          "tenant_name",
          tenant_schema.get_tenant_name_str(),
          "size",
          recycle_objs.size());
    } else {
      const ObRecycleObject& recycle_obj = recycle_objs.at(0);
      share::schema::ObTenantSchema new_tenant_schema;
      if (OB_FAIL(new_tenant_schema.assign(tenant_schema))) {
        LOG_WARN("fail to assign", K(ret));
      } else if (OB_FAIL(delay_to_drop_tenant(new_tenant_schema, trans, ddl_stmt_str))) {
        LOG_WARN("drop_table failed", K(ret));
      } else if (OB_FAIL(schema_service->delete_recycle_object(OB_SYS_TENANT_ID, recycle_obj, trans))) {
        LOG_WARN("delete_recycle_object failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::rename_tenant(share::schema::ObTenantSchema& tenant_schema, common::ObMySQLTransaction& trans,
    const common::ObString* ddl_stmt_str /* = NULL */)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();

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
    if (OB_FAIL(schema_service_impl->get_tenant_sql_service().rename_tenant(tenant_schema, trans, ddl_stmt_str))) {
      LOG_WARN("rename tenant failed", K(ret), K(tenant_schema));
    }
  }
  return ret;
}

int ObDDLOperator::alter_tenant(
    ObTenantSchema& tenant_schema, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();

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
    if (OB_FAIL(schema_service_impl->get_tenant_sql_service().alter_tenant(tenant_schema, trans, op, ddl_stmt_str))) {
      LOG_WARN("schema_service_impl alter_tenant failed", K(tenant_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::replace_sys_variable(ObSysVariableSchema& sys_variable_schema, const int64_t schema_version,
    ObMySQLTransaction& trans, const ObSchemaOperationType& operation_type, const common::ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  sys_variable_schema.set_schema_version(schema_version);
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();

  if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema_version", K(ret), K(schema_version));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null");
  } else if (OB_FAIL(check_var_schema_options(sys_variable_schema))) {
    LOG_WARN("failed to check var schema option", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_sys_variable_sql_service().replace_sys_variable(
                 sys_variable_schema, trans, operation_type, ddl_stmt_str))) {
    LOG_WARN("schema_service_impl update sys variable failed", K(sys_variable_schema), K(operation_type), K(ret));
  }
  LOG_INFO("replace sys variable",
      K(ret),
      "tenant_id",
      sys_variable_schema.get_tenant_id(),
      "cost",
      ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::check_var_schema_options(const ObSysVariableSchema& sys_variable_schema) const
{
  int ret = OB_SUCCESS;
  if (0 == sys_variable_schema.get_real_sysvar_count()) {
  } else if (OB_SYS_TENANT_ID == sys_variable_schema.get_tenant_id()) {
    ObString gts_name(share::OB_SV_TIMESTAMP_SERVICE);
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_variable_schema.get_sysvar_count(); ++i) {
      const ObSysVarSchema* sysvar_schema = sys_variable_schema.get_sysvar_schema(i);
      if (OB_ISNULL(sysvar_schema)) {
        // not allowed to print error
      } else {
        if (gts_name != sysvar_schema->get_name()) {
        } else {
          ObObj gts_obj;
          ObArenaAllocator allocator(ObModIds::OB_GTS_SWITCH_GETTER);
          int64_t gts_value = 0;
          if (OB_FAIL(sysvar_schema->get_value(&allocator, NULL, gts_obj))) {
            LOG_WARN("fail to get value", KR(ret));
          } else if (OB_FAIL(gts_obj.get_int(gts_value))) {
            LOG_WARN("fail to get int", KR(ret), K(gts_obj));
          } else if (transaction::TS_SOURCE_LTS != gts_value) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("not allowed to set sys tenant time service to non-lts", KR(ret), K(gts_value), K(sysvar_schema));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set sys tenant time service to non-lts");
          } else {
            LOG_DEBUG("set sys tenant time service to lts", K(sysvar_schema));
          }
        }
      }
    }
  } else {
    ObString gts_name(share::OB_SV_TIMESTAMP_SERVICE);
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_variable_schema.get_sysvar_count(); ++i) {
      const ObSysVarSchema* sysvar_schema = sys_variable_schema.get_sysvar_schema(i);
      if (OB_ISNULL(sysvar_schema)) {
        // Don't return error.
      } else {
        if (gts_name != sysvar_schema->get_name()) {
        } else {
          ObObj gts_obj;
          ObArenaAllocator allocator(ObModIds::OB_GTS_SWITCH_GETTER);
          int64_t gts_value = 0;
          if (OB_FAIL(sysvar_schema->get_value(&allocator, NULL, gts_obj))) {
            LOG_WARN("failed to get value", KR(ret));
          } else if (OB_FAIL(gts_obj.get_int(gts_value))) {
            LOG_WARN("failed to get int", KR(ret), K(gts_obj));
          } else if (transaction::TS_SOURCE_GTS != gts_value) {
            // If not GTS, it is necessary to check whether standby cluster exist and whether backup enable.
            rootserver::ObRootService* root_service = GCTX.root_service_;
            if (ObServerConfig::get_instance().enable_log_archive) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN(
                  "set tenant time service not gts not allowed when log archive is enabled", KR(ret), K(sysvar_schema));
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set tenant time service not gts when log archive is enabled");
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::clear_tenant_partition_table(const uint64_t tenant_id, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null");
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else {
    const int64_t partition_idx = 0;
    if (OB_SUCC(ret)) {
      const ObTableSchema* meta_table = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(combine_id(OB_SYS_TENANT_ID, OB_ALL_META_TABLE_TID), meta_table))) {
        LOG_WARN("get meta table schema failed", K(ret));
      } else if (OB_ISNULL(meta_table)) {
        LOG_WARN("NULL meta_table schema");
      } else {
        int64_t partition_cnt = meta_table->get_all_part_num();
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_cnt; ++i) {
          if (OB_FAIL(schema_service_impl->delete_partition_table(OB_ALL_META_TABLE_TID, tenant_id, i, sql_client))) {
            LOG_WARN("clear sys tenant partition table failed", K(ret), K(tenant_id), "partition_id", i);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_service_impl->delete_partition_table(
              OB_ALL_TENANT_META_TABLE_TID, tenant_id, partition_idx, sql_client))) {
        LOG_WARN("clear tenant meta partition table failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(schema_service_impl->delete_partition_table(
              OB_ALL_ROOT_TABLE_TID, tenant_id, partition_idx, sql_client))) {
        LOG_WARN("clear sys tenant partition table failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::create_database(
    ObDatabaseSchema& database_schema, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  // set the old database id
  const uint64_t tenant_id = database_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t new_database_id = database_schema.get_database_id();
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (OB_FAIL(schema_service->fetch_new_database_id(database_schema.get_tenant_id(), new_database_id))) {
    LOG_WARN("fetch new database id failed", K(database_schema.get_tenant_id()), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    database_schema.set_database_id(new_database_id);
    database_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_database_sql_service().insert_database(database_schema, trans, ddl_stmt_str))) {
      LOG_WARN("insert database failed", K(database_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::alter_database(ObDatabaseSchema& new_database_schema, ObMySQLTransaction& trans,
    const ObSchemaOperationType op_type, const ObString* ddl_stmt_str /*=NULL*/,
    const bool need_update_schema_version /*=true*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
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
    } else if (OB_FAIL(schema_service->get_database_sql_service().update_database(
                   new_database_schema, trans, op_type, ddl_stmt_str))) {
      RS_LOG(WARN, "update database failed", K(new_database_schema), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_database(const ObDatabaseSchema& db_schema, ObMySQLTransaction& trans,
    ObSchemaGetterGuard& schema_guard, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = db_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t database_id = db_schema.get_database_id();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  bool is_delay_delete = false;

  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl",
        OB_P(schema_service_impl),
        K(ret));
  }
  // drop tables in recyclebin
  if (OB_SUCC(ret)) {
    if (OB_FAIL(purge_table_of_database(db_schema, trans, schema_guard))) {
      LOG_WARN("purge_table_in_db failed", K(ret));
    }
  }

  // delete tables in database
  if (OB_SUCC(ret)) {
    ObArray<const ObTableSchema*> tables;
    ObString new_name;
    if (OB_FAIL(schema_guard.get_table_schemas_in_database(tenant_id, database_id, tables))) {
      LOG_WARN("get tables in database failed", K(tenant_id), KT(database_id), K(ret));
    } else if (OB_FAIL(check_is_delay_delete(tenant_id, is_delay_delete))) {
      LOG_WARN("check is delay delete failed", K(ret), K(tenant_id));
    } else {
      // drop index tables first
      common::ObSqlString public_sql_string;
      for (int64_t cycle = 0; OB_SUCC(ret) && cycle < 2; ++cycle) {
        for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
          const ObTableSchema* table = tables.at(i);
          if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table is NULL", K(ret));
          } else if (table->is_materialized_view() || table->is_in_recyclebin()) {
            // already been dropped before
          } else if ((0 == cycle ? table->is_aux_table() : !table->is_aux_table())) {
            if (OB_FAIL(drop_table(*table, trans, NULL, false, NULL, true))) {
              LOG_WARN("drop delay deleted table failed", K(ret), K(table->get_table_id()));
            }
          }
        }
      }
    }
  }

  // delete outlines in database
  if (OB_SUCC(ret)) {
    ObArray<const ObOutlineInfo*> outlines;
    if (OB_FAIL(schema_guard.get_outline_infos_in_database(tenant_id, database_id, outlines))) {
      LOG_WARN("get outlines in database failed", K(tenant_id), KT(database_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < outlines.count(); ++i) {
        const ObOutlineInfo* outline_info = outlines.at(i);
        if (OB_ISNULL(outline_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("outline info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_outline_sql_service().drop_outline(
                       *outline_info, new_schema_version, trans))) {
          LOG_WARN("drop outline failed", "outline_id", outline_info->get_outline_id(), K(ret));
        }
      }
    }
  }
  // delete synonyms in database
  if (OB_SUCC(ret)) {
    ObArray<const ObSynonymInfo*> synonyms;
    if (OB_FAIL(schema_guard.get_synonym_infos_in_database(tenant_id, database_id, synonyms))) {
      LOG_WARN("get synonyms in database failed", K(tenant_id), KT(database_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < synonyms.count(); ++i) {
        const ObSynonymInfo* synonym_info = synonyms.at(i);
        if (OB_ISNULL(synonym_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("synonym info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_synonym_sql_service().drop_synonym(
                       *synonym_info, new_schema_version, &trans))) {
          LOG_WARN("drop synonym failed", "synonym_id", synonym_info->get_synonym_id(), K(ret));
        }
      }
    }
  }

  // delete sequences in database
  if (OB_SUCC(ret)) {
    ObArray<const ObSequenceSchema*> sequences;
    if (OB_FAIL(schema_guard.get_sequence_infos_in_database(tenant_id, database_id, sequences))) {
      LOG_WARN("get sequences in database failed", K(tenant_id), KT(database_id), K(ret));
    } else {
      common::ObSqlString public_sql_string;
      for (int64_t i = 0; OB_SUCC(ret) && i < sequences.count(); ++i) {
        const ObSequenceSchema* sequence_info = sequences.at(i);
        if (OB_ISNULL(sequence_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sequence info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_sequence_sql_service().drop_sequence(
                       *sequence_info, new_schema_version, &trans))) {
          LOG_WARN("drop sequence failed", K(*sequence_info), K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    ObString new_name;
    if (is_delay_delete &&
        OB_FAIL(create_new_name_for_delay_delete_database(new_name, &db_schema, allocator, schema_guard))) {
      LOG_WARN("create new name for delay delete db failed", K(ret));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_impl->get_database_sql_service().delete_database(
                   db_schema, new_schema_version, trans, ddl_stmt_str, is_delay_delete, &new_name))) {
      LOG_WARN("delete database failed", KT(database_id), K(ret));
    }
  }
  return ret;
}

// When delete database to recyclebin, it is necessary to update schema version
// of each table, in case of hitting plan cache.
// The key of plan cache is current database name, table_id and schema version.
int ObDDLOperator::update_table_version_of_db(
    const ObDatabaseSchema& database_schema, ObMySQLTransaction& trans, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = database_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObArray<const ObTableSchema*> table_schemas;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service should not be null", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_database(
                 database_schema.get_tenant_id(), database_schema.get_database_id(), table_schemas))) {
    LOG_WARN("get_table_schemas_in_database failed", K(ret));
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < table_schemas.count(); ++idx) {
    const ObTableSchema* table = table_schemas.at(idx);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else if (table->is_index_table() || table->is_materialized_view()) {
      continue;
    } else {
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      if (OB_FAIL(table->get_simple_index_infos(simple_index_infos))) {
        LOG_WARN("get_index_tid_array failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        const ObTableSchema* index_table_schema = NULL;
        if (OB_FAIL(schema_guard.get_table_schema(simple_index_infos.at(i).table_id_, index_table_schema))) {
          LOG_WARN("get_table_schema failed", "table id", simple_index_infos.at(i).table_id_, K(ret));
        } else if (OB_ISNULL(index_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema should not be null", K(ret));
        } else if (index_table_schema->is_materialized_view()) {
          continue;
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else {
          ObTableSchema new_index_schema;
          if (OB_FAIL(new_index_schema.assign(*index_table_schema))) {
            LOG_WARN("fail to assign schema", K(ret));
          } else {
            new_index_schema.set_schema_version(new_schema_version);
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                         trans, *index_table_schema, new_index_schema, OB_DDL_DROP_TABLE_TO_RECYCLEBIN, NULL))) {
            LOG_WARN("update_table_option failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObTableSchema new_ts;
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
          if (OB_FAIL(
                  schema_service->get_table_sql_service().update_table_options(trans, *table, new_ts, op_type, NULL))) {
            LOG_WARN("update_table_option failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_database_to_recyclebin(const ObDatabaseSchema& database_schema, ObMySQLTransaction& trans,
    ObSchemaGetterGuard& guard, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = database_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
  } else {
    ObDatabaseSchema new_database_schema = database_schema;
    ObSqlString new_db_name;
    ObRecycleObject recycle_object;
    recycle_object.set_original_name(database_schema.get_database_name_str());
    recycle_object.set_type(ObRecycleObject::DATABASE);
    recycle_object.set_database_id(database_schema.get_database_id());
    recycle_object.set_table_id(OB_INVALID_ID);
    recycle_object.set_tablegroup_id(database_schema.get_default_tablegroup_id());
    new_database_schema.set_in_recyclebin(true);
    new_database_schema.set_default_tablegroup_id(OB_INVALID_ID);
    // It ensure that db schema version of insert recyclebin and alter database
    // is equal that updating table version and inserting recyclebin.
    ObSchemaService* schema_service = schema_service_.get_schema_service();
    if (OB_FAIL(update_table_version_of_db(database_schema, trans, guard))) {
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
    } else if (FALSE_IT(recycle_object.set_tenant_id(database_schema.get_tenant_id()))) {
    } else if (OB_FAIL(schema_service_impl->insert_recyclebin_object(recycle_object, trans))) {
      LOG_WARN("insert recycle object failed", K(ret));
    } else if (OB_FAIL(alter_database(new_database_schema,
                   trans,
                   OB_DDL_DROP_DATABASE_TO_RECYCLEBIN,
                   ddl_stmt_str,
                   false /*no need_new_schema_version*/))) {
      LOG_WARN("alter_database failed,", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::create_tablegroup(
    ObTablegroupSchema& tablegroup_schema, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  uint64_t new_tablegroup_id = OB_INVALID_ID;
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (OB_FAIL(schema_service->fetch_new_tablegroup_id(tablegroup_schema.get_tenant_id(), new_tablegroup_id))) {
    LOG_WARN("failed to fetch new_tablegroup_id", "tenant_id", tablegroup_schema.get_tenant_id(), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
    if (OB_INVALID_ID != tablegroup_id && !is_new_tablegroup_id(tablegroup_id)) {
      // tablegroup_id is specified, if tablegroup is created before 2.x, it needs change
      // new tablegroup id.
      new_tablegroup_id = new_tablegroup_id & (~OB_TABLEGROUP_MASK);
    }
    tablegroup_schema.set_tablegroup_id(new_tablegroup_id);
    tablegroup_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(
            schema_service->get_tablegroup_sql_service().insert_tablegroup(tablegroup_schema, trans, ddl_stmt_str))) {
      LOG_WARN("insert tablegroup failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_tablegroup(
    const ObTablegroupSchema& tablegroup_schema, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;

  ObSchemaGetterGuard schema_guard;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama schema_service_impl and schema manage must not null",
        "schema_service_impl",
        OB_P(schema_service_impl),
        K(ret));
  }

  bool is_delay_delete = false;
  if (FAILEDx(check_is_delay_delete(tenant_id, is_delay_delete))) {
    LOG_WARN("fail to check is delay delete", K(ret), K(tenant_id));
  } else if (!is_new_tablegroup_id(tablegroup_schema.get_tablegroup_id())) {
    is_delay_delete = false;
  }

  // check whether tablegroup is empty, if not empty, return OB_TABLEGROUP_NOT_EMPTY
  if (OB_SUCC(ret)) {
    bool not_empty = false;
    ObArray<const ObSimpleTableSchemaV2*> tables;
    if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, tables))) {
      LOG_WARN("get table ids in tablegroup failed", K(tenant_id), KT(tablegroup_id), K(ret));
    } else if (tables.count() > 0) {
      // When tablegroup is dropped, there must not be table in tablegroup, otherwise it is failed to get tablegroup
      // schema when getting derived relation property by table. As locality and primary_zone is add in tablegroup
      // after 2.0
      not_empty = true;
      if (is_delay_delete) {
        // If all table in tablegroup are delay delete object, tablegroup can delete.
        not_empty = false;
        for (int64_t i = 0; !not_empty && OB_SUCC(ret) && i < tables.count(); i++) {
          const ObSimpleTableSchemaV2* table = tables.at(i);
          if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table is null", K(ret), K(i));
          } else if (!table->is_dropped_schema()) {
            not_empty = true;
          }
        }
      }
    }
    // check databases' default_tablegroup_id
    if (OB_SUCC(ret) && !not_empty) {
      if (OB_FAIL(schema_guard.check_database_exists_in_tablegroup(tenant_id, tablegroup_id, not_empty))) {
        LOG_WARN("failed to check whether database exists in table group", K(tenant_id), KT(tablegroup_id), K(ret));
      }
    }
    // check tenants' default_tablegroup_id
    if (OB_SUCC(ret) && !not_empty) {
      const ObTenantSchema* tenant_schema = NULL;
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
      LOG_WARN("tablegroup still has tables or is some databases' default tablegroup or is tenant default tablegroup, "
               "can't delete it",
          KT(tablegroup_id),
          K(ret));
    }
  }

  // delete tablegroup and log ddl operation
  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    ObString new_name;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (is_delay_delete && OB_FAIL(create_new_name_for_delay_delete_tablegroup(
                                      new_name, &tablegroup_schema, allocator, schema_guard))) {
      LOG_WARN("create new name for delay delete tablegroup failed", K(ret));
    } else if (OB_FAIL(schema_service_impl->get_tablegroup_sql_service().delete_tablegroup(
                   tablegroup_schema, new_schema_version, trans, ddl_stmt_str, is_delay_delete, &new_name))) {
      LOG_WARN("delete tablegroup failed", KT(tablegroup_id), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::alter_tablegroup(
    ObTablegroupSchema& new_schema, common::ObMySQLTransaction& trans, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  if (!new_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_schema));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema schema_service_impl must not null", "schema_service_impl", OB_P(schema_service_impl), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    new_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service_impl->get_tablegroup_sql_service().update_tablegroup(new_schema, trans, ddl_stmt_str))) {
      LOG_WARN("fail to get tablegroup sql service", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_restore_point(const uint64_t tenant_id, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE TENANT_ID = %ld AND SNAPSHOT_TYPE = %d",
          OB_ALL_ACQUIRED_SNAPSHOT_TNAME,
          tenant_id,
          ObSnapShotType::SNAPSHOT_FOR_RESTORE_POINT))) {
    LOG_WARN("sql assign failed", K(ret));
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  }
  return ret;
}

int ObDDLOperator::alter_tablegroup(ObSchemaGetterGuard& schema_guard, ObTableSchema& new_table_schema,
    common::ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    RS_LOG(ERROR, "schema schema_service_impl must not null", "schema_service_impl", OB_P(schema_service_impl), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    new_table_schema.set_schema_version(new_schema_version);
    // check whether tablegroup is empty, if not empty, return OB_TABLEGROUP_NOT_EMPTY
    if (OB_FAIL(schema_service_impl->get_table_sql_service().update_tablegroup(
            schema_guard, new_table_schema, trans, ddl_stmt_str))) {
      RS_LOG(WARN, "alter tablegroup failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::get_user_id_for_inner_ur(ObUserInfo& user, bool& is_inner_ur, uint64_t& new_user_id)
{
  int ret = OB_SUCCESS;
  const char* ur_name = user.get_user_name();
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  const ObSysVariableSchema* sys_variable_schema = NULL;
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
    }
    if (is_inner_ur) {
      new_user_id = combine_id(tenant_id, new_user_id);
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

int ObDDLOperator::create_user(ObUserInfo& user, const ObString* ddl_stmt_str, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  uint64_t new_user_id = user.get_user_id();
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  bool is_inner_ur;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (OB_FAIL(get_user_id_for_inner_ur(user, is_inner_ur, new_user_id))) {
    LOG_WARN("failed to fetch_new_user_id", "tennat_id", user.get_tenant_id(), K(ret));
  } else if (!is_inner_ur && OB_FAIL(schema_service->fetch_new_user_id(user.get_tenant_id(), new_user_id))) {
    LOG_WARN("failed to fetch_new_user_id", "tennat_id", user.get_tenant_id(), K(ret));
  } else {
    user.set_user_id(combine_id(user.get_tenant_id(), new_user_id));
  }
  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = user.get_tenant_id();
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(
                   schema_service->get_user_sql_service().create_user(user, new_schema_version, ddl_stmt_str, trans))) {
      LOG_WARN("insert user failed", K(user), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::create_table(ObTableSchema& table_schema, ObMySQLTransaction& trans,
    const ObString* ddl_stmt_str /*=NULL*/, const bool need_sync_schema_version, const bool is_truncate_table /*false*/)
{
  int ret = OB_SUCCESS;
  const uint64_t fetch_tenant_id =
      is_inner_table(table_schema.get_table_id()) ? OB_SYS_TENANT_ID : table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(ERROR, "schema_service must not null");
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(fetch_tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(fetch_tenant_id));
  } else {
    table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_table_sql_service().create_table(
            table_schema, trans, ddl_stmt_str, need_sync_schema_version, is_truncate_table))) {
      RS_LOG(WARN, "failed to create table", K(ret));
    } else if (OB_FAIL(sync_version_for_cascade_table(table_schema.get_depend_table_ids(), trans))) {
      RS_LOG(WARN, "fail to sync cascade depend table", K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::sync_version_for_cascade_table(const ObIArray<uint64_t>& table_ids, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  uint64_t id = OB_INVALID_ID;
  const ObTableSchema* schema = NULL;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(ERROR, "schema_service must not null");
  } else {
    for (int64_t i = 0; i < table_ids.count() && OB_SUCC(ret); i++) {
      ObSchemaGetterGuard schema_guard;
      id = table_ids.at(i);
      const uint64_t fetch_tenant_id = is_inner_table(id) ? OB_SYS_TENANT_ID : extract_tenant_id(id);
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObTableSchema tmp_schema;
      if (OB_FAIL(schema_service_.get_tenant_schema_guard(fetch_tenant_id, schema_guard))) {
        RS_LOG(WARN, "get schema guard failed", K(ret), K(id));
      } else if (OB_FAIL(schema_guard.get_table_schema(id, schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(id));
      } else if (!schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is NULL", K(ret));
      } else if (OB_FAIL(tmp_schema.assign(*schema))) {
        LOG_WARN("fail to assign schema", K(ret), KPC(schema));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(fetch_tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(fetch_tenant_id));
      } else if (OB_FAIL(schema_service->get_table_sql_service().sync_schema_version_for_history(
                     trans, tmp_schema, new_schema_version))) {
        RS_LOG(WARN, "fail to sync schema version", K(ret));
      } else {
        LOG_INFO("synced schema version for depend table",
            K(id),
            "from",
            schema->get_schema_version(),
            "to",
            new_schema_version);
      }
    }
  }

  return ret;
}

// wrapper for alter column effects
// if column is in
// 1. update index if modified column is in index
// 2. update materialized view if modified column is in materialized view
// but 2 is disabled for now
int ObDDLOperator::alter_table_update_index_and_view_column(ObSchemaService& schema_service,
    const ObTableSchema& new_table_schema, const ObColumnSchemaV2& new_column_schema, common::ObMySQLTransaction& trans,
    const ObIArray<ObTableSchema>* global_idx_schema_array /*=NULL*/)
{
  int ret = OB_SUCCESS;
  // disable cascading update materialized view
  // if (OB_FAIL(alter_table_update_view_column(schema_service, new_table_schema, new_column_schema, trans))) {
  //  LOG_WARN("fail to update view column", K(ret), K(new_table_schema), K(new_column_schema));
  //} else
  if (OB_FAIL(alter_table_update_aux_column(
          schema_service, new_table_schema, new_column_schema, trans, USER_INDEX, global_idx_schema_array))) {
    LOG_WARN("fail to update index column", K(ret), K(new_table_schema), K(new_column_schema));
  }
  return ret;
}

int ObDDLOperator::modify_part_func_expr_for_global_index(const share::schema::ObColumnSchemaV2& orig_column,
    const share::schema::ObColumnSchemaV2& alter_column, share::schema::ObTableSchema& new_table_schema,
    const common::ObTimeZoneInfo& tz_info, common::ObIAllocator& allocator, common::ObMySQLTransaction& trans,
    ObIArray<ObTableSchema>* global_idx_schema_array /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_ISNULL(global_idx_schema_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global_idx_schema_array is NULL", K(ret));
  } else if (OB_FAIL(new_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
  } else {
    const ObTableSchema* index_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      if (OB_FAIL(schema_guard.get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("get table_schema failed", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema should not be null", K(ret));
      } else if (index_schema->is_in_recyclebin()) {
        ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
        LOG_WARN("index table is in recyclebin", K(ret));
      } else if (index_schema->is_global_index_table() && index_schema->is_partitioned_table()) {
        int64_t new_schema_version = OB_INVALID_VERSION;
        const ObColumnSchemaV2* origin_column_schema = index_schema->get_column_schema(orig_column.get_column_id());
        if (OB_ISNULL(origin_column_schema)) {
          // skip, this column is not in global index
        } else if (origin_column_schema->is_tbl_part_key_column()) {
          ObTableSchema new_index_schema;
          if (OB_FAIL(new_index_schema.assign(*index_schema))) {
            LOG_WARN("assign index_schema failed", K(ret));
          } else if (OB_FAIL(modify_part_func_expr(
                         *origin_column_schema, alter_column, new_index_schema, tz_info, allocator))) {
            LOG_WARN("modify part func expr failed", K(ret));
          } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else if (FALSE_IT(new_index_schema.set_schema_version(new_schema_version))) {
          } else if (OB_FAIL(schema_service->get_table_sql_service().update_partition_option(
                         trans, new_index_schema, new_index_schema.get_schema_version()))) {
            LOG_WARN("update partition option failed", K(ret), K(new_index_schema), K(*index_schema));
          } else if (OB_FAIL(global_idx_schema_array->push_back(new_index_schema))) {
            LOG_WARN("fail to push_back to global_idx_schema_array", K(ret), K(new_index_schema));
          }
        }
      } else {
        // skip
      }
    }  // end of for
  }    // end of else

  return ret;
}

// aux schema column
int ObDDLOperator::alter_table_update_aux_column(ObSchemaService& schema_service, const ObTableSchema& new_table_schema,
    const ObColumnSchemaV2& new_column_schema, common::ObMySQLTransaction& trans, const ObTableType table_type,
    const ObIArray<ObTableSchema>* global_idx_schema_array /*=NULL*/)
{
  int ret = OB_SUCCESS;
  UNUSED(table_type);
  // update column in aux table
  ObSchemaGetterGuard schema_guard;
  ObColumnSchemaV2 new_aux_column_schema;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();

  if (table_type != USER_INDEX) {
    ret = OB_NOT_SUPPORTED;
    RS_LOG(WARN, "table type not supporrted", K(ret), K(table_type));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    RS_LOG(WARN, "get schema guard failed", K(ret));
  } else if (OB_FAIL(new_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
  } else {
    // update all aux table schema
    const ObTableSchema* aux_table_schema = NULL;
    int64_t N = simple_index_infos.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      aux_table_schema = NULL;
      if (OB_NOT_NULL(global_idx_schema_array) && !global_idx_schema_array->empty()) {
        for (int64_t j = 0; OB_SUCC(ret) && j < global_idx_schema_array->count(); ++j) {
          if (simple_index_infos.at(i).table_id_ == global_idx_schema_array->at(j).get_table_id()) {
            aux_table_schema = &(global_idx_schema_array->at(j));
            break;
          }
        }
      }
      uint64_t tid = simple_index_infos.at(i).table_id_;
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(aux_table_schema) && OB_FAIL(schema_guard.get_table_schema(tid, aux_table_schema))) {
        RS_LOG(WARN, "get_table_schema failed", K(ret), K(tid));
      } else if (OB_ISNULL(aux_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "aux schema should not be null", K(ret));
      } else if (aux_table_schema->is_in_recyclebin()) {
        ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
        LOG_WARN("aux table is in recyclebin", K(ret));
      } else {
        const uint64_t tenant_id = aux_table_schema->get_tenant_id();
        int64_t new_schema_version = OB_INVALID_VERSION;
        const ObColumnSchemaV2* origin_column_schema =
            aux_table_schema->get_column_schema(new_column_schema.get_column_id());
        if (NULL != origin_column_schema) {
          // exist such column in aux schema
          new_aux_column_schema = new_column_schema;
          new_aux_column_schema.set_table_id(aux_table_schema->get_table_id());
          new_aux_column_schema.set_autoincrement(false);
          // save the rowkey postion and aux postion
          new_aux_column_schema.set_rowkey_position(origin_column_schema->get_rowkey_position());
          new_aux_column_schema.set_index_position(origin_column_schema->get_index_position());
          new_aux_column_schema.set_tbl_part_key_pos(origin_column_schema->get_tbl_part_key_pos());
          // will only update some attribute, not include rowkey postion or aux position
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else if (FALSE_IT(new_aux_column_schema.set_schema_version(new_schema_version))) {
          } else if (OB_FAIL(schema_service.get_table_sql_service().update_single_column(
                         trans, *aux_table_schema, *aux_table_schema, new_aux_column_schema))) {
            RS_LOG(WARN, "schema service update aux column failed failed", "table schema", *aux_table_schema, K(ret));
          } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_service.get_table_sql_service().sync_aux_schema_version_for_history(
                         trans, *aux_table_schema, new_schema_version))) {
            RS_LOG(WARN, "fail to update aux schema version for update column");
          }
        }
      }
    }  // end of for
  }    // end of else
  return ret;
}

// Notice: this function process index.
int ObDDLOperator::alter_table_drop_aux_column(ObSchemaService& schema_service, ObTableSchema& new_table_schema,
    const ObColumnSchemaV2& orig_column_schema, common::ObMySQLTransaction& trans, const ObTableType table_type)
{
  int ret = OB_SUCCESS;
  UNUSED(table_type);
  // should update the aux table
  const uint64_t tenant_id = orig_column_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  ObSchemaGetterGuard schema_guard;

  if (table_type != USER_INDEX) {
    ret = OB_NOT_SUPPORTED;
    RS_LOG(WARN, "table type not supporrted", K(ret), K(table_type));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    RS_LOG(WARN, "get schema guard failed", K(ret));
  } else if (OB_FAIL(new_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
  }
  // update all aux table schema
  int64_t N = simple_index_infos.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObTableSchema* aux_table_schema = NULL;
    uint64_t tid = simple_index_infos.at(i).table_id_;
    if (OB_FAIL(schema_guard.get_table_schema(tid, aux_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tid));
    } else if (OB_ISNULL(aux_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else if (aux_table_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("aux table is in recyclebin", K(ret));
    } else {
      const ObColumnSchemaV2* delete_column_schema =
          aux_table_schema->get_column_schema(orig_column_schema.get_column_id());
      if (NULL != delete_column_schema) {
        if (delete_column_schema->is_index_column()) {
          ret = OB_ERR_ALTER_INDEX_COLUMN;
          RS_LOG(WARN, "can't not drop index column", K(ret));
        } else {
          // Notice: when the last VP column is deleted, the VP table should be deleted.
          // If other VP column is hidden, the VP partition should be deleted.
          int64_t normal_column_count = 0;
          for (int64_t i = 0; OB_SUCC(ret) && (normal_column_count < 2) && (i < aux_table_schema->get_column_count());
               ++i) {
            if (!aux_table_schema->get_column_schema_by_idx(i)->is_hidden()) {
              ++normal_column_count;
            }
          }
          if (normal_column_count <= 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("normal_column_count is error", K(ret), K(normal_column_count));
          } else {
            ObTableSchema tmp_aux_table_schema;
            if (OB_FAIL(tmp_aux_table_schema.assign(*aux_table_schema))) {
              LOG_WARN("fail to assign schema", K(ret));
            } else if (OB_FAIL(update_prev_id_for_delete_column(
                           *aux_table_schema, tmp_aux_table_schema, *delete_column_schema, schema_service, trans))) {
              LOG_WARN("failed to update column previous id for delete column", K(ret));
            } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
              LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
            } else if (OB_FAIL(schema_service.get_table_sql_service().delete_single_column(
                           new_schema_version, trans, *aux_table_schema, *delete_column_schema))) {
              RS_LOG(WARN, "failed to delete non-aux column!", "table schema", *aux_table_schema, K(ret));
            } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
              LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
            } else if (OB_FAIL(schema_service.get_table_sql_service().sync_aux_schema_version_for_history(
                           trans, *aux_table_schema, new_schema_version))) {
              RS_LOG(WARN, "fail to update aux schema version for update column");
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::generate_tmp_idx_schemas(const ObTableSchema& new_table_schema, ObIArray<ObTableSchema>& idx_schemas)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;

  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    RS_LOG(WARN, "get schema guard failed", K(ret));
  } else if (OB_FAIL(new_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema* index_table_schema = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(simple_index_infos.at(i).table_id_, index_table_schema))) {
        RS_LOG(WARN, "get_table_schema failed", K(ret), "table id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "table schema should not be null", K(ret));
      } else {
        if (OB_FAIL(idx_schemas.push_back(*index_table_schema))) {
          RS_LOG(WARN, "fail to push back to idx_schemas", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::check_new_column_for_index(
    ObIArray<ObTableSchema>& idx_schemas, const ObColumnSchemaV2& new_column_schema)
{
  int ret = OB_SUCCESS;
  int idx_cnt = idx_schemas.count();
  ObTableSchema* index_table_schema = NULL;
  ObColumnSchemaV2 copy_index_column_schema;

  for (int64_t i = 0; OB_SUCC(ret) && i < idx_cnt; ++i) {
    index_table_schema = &idx_schemas.at(i);
    if (OB_ISNULL(index_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "table schema should not be null", K(ret));
    } else {
      const ObColumnSchemaV2* origin_idx_column_schema =
          index_table_schema->get_column_schema(new_column_schema.get_column_id());
      if (NULL == origin_idx_column_schema) {
        RS_LOG(INFO,
            "index table do not contain this column",
            "column_name",
            new_column_schema.get_column_name_str(),
            "index_table",
            index_table_schema->get_table_name_str());
        continue;
      } else if (!origin_idx_column_schema->is_rowkey_column()) {
        RS_LOG(INFO,
            "ingore not rowkey column",
            "column_name",
            new_column_schema.get_column_name_str(),
            "index_table",
            index_table_schema->get_table_name_str());
      } else {
        copy_index_column_schema.reset();
        copy_index_column_schema = new_column_schema;
        copy_index_column_schema.set_rowkey_position(origin_idx_column_schema->get_rowkey_position());
        copy_index_column_schema.set_index_position(origin_idx_column_schema->get_index_position());
        copy_index_column_schema.set_tbl_part_key_pos(origin_idx_column_schema->get_tbl_part_key_pos());
        if (OB_FAIL(index_table_schema->alter_column(copy_index_column_schema))) {
          RS_LOG(WARN, "failed to alter index column schema", K(copy_index_column_schema), K(ret));
        } else if (!index_table_schema->is_valid()) {
          ret = OB_SCHEMA_ERROR;
          RS_LOG(WARN, "idx table schema is invalid!", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::resolve_timestamp_column(AlterColumnSchema* alter_column_schema, ObTableSchema& new_table_schema,
    ObColumnSchemaV2& new_column_schema, const common::ObTimeZoneInfoWrap& tz_info_wrap,
    const common::ObString* nls_formats, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alter_column_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter_column_schema is NULL", K(ret));
  } else if (ObTimestampType != new_column_schema.get_data_type() ||
             false == alter_column_schema->check_timestamp_column_order_) {
    // nothing to do
  } else {
    bool is_first_timestamp = false;
    ObTableSchema::const_column_iterator it_begin = new_table_schema.column_begin();
    ObTableSchema::const_column_iterator it_end = new_table_schema.column_end();
    bool found = false;
    for (; OB_SUCC(ret) && it_begin != it_end && !found; it_begin++) {
      if (OB_ISNULL(it_begin)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("it_begin should not be NULL", K(ret));
      } else if (OB_ISNULL(*it_begin)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("*it_begin should not be NULL", K(ret));
      } else {
        if (ObTimestampType == (*it_begin)->get_data_type()) {
          if (0 == (*it_begin)->get_column_name_str().case_compare(new_column_schema.get_column_name_str())) {
            is_first_timestamp = true;
          }
          found = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_DDL_ALTER_COLUMN == alter_column_schema->alter_type_) {
        // drop default or set default
        // mysql seem like set default now will couse a parser error;
        if (is_first_timestamp && alter_column_schema->is_drop_default_) {
          // new_column_schema is orig_column_schema;
          // if default value is now(), on update current timestamp is false;
          if (!new_column_schema.is_nullable() && !IS_DEFAULT_NOW_OBJ(new_column_schema.get_cur_default_value())) {
            new_column_schema.set_on_update_current_timestamp(true);
          } else {
            // do nothing
          }
        }
      } else {
        bool is_set_default = alter_column_schema->is_set_default_;
        bool is_set_null = alter_column_schema->is_set_nullable_;
        if (is_first_timestamp && !is_set_null && !is_set_default &&
            !new_column_schema.is_on_update_current_timestamp()) {
          new_column_schema.set_nullable(false);
          new_column_schema.get_cur_default_value().set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
          new_column_schema.set_on_update_current_timestamp(true);
        } else if (!is_set_null) {
          new_column_schema.set_nullable(false);
          if (!is_set_default) {
            if (alter_column_schema->is_no_zero_date_) {
              ret = OB_INVALID_DEFAULT;
              LOG_USER_ERROR(OB_INVALID_DEFAULT,
                  alter_column_schema->get_column_name_str().length(),
                  alter_column_schema->get_column_name_str().ptr());
            } else {
              int64_t zero_date = ObTimeConverter::ZERO_DATETIME;
              ObTimeConverter::round_datetime(alter_column_schema->get_data_scale(), zero_date);
              new_column_schema.get_cur_default_value().set_timestamp(zero_date);
            }
          } else if (new_column_schema.get_cur_default_value().is_null()) {
            ret = OB_INVALID_DEFAULT;
            LOG_USER_ERROR(OB_INVALID_DEFAULT,
                new_column_schema.get_column_name_str().length(),
                new_column_schema.get_column_name_str().ptr());
          }
        } else {
          new_column_schema.set_nullable(true);
          if (!is_set_default) {
            new_column_schema.get_cur_default_value().set_null();
          }
        }
        if (OB_SUCC(ret) && OB_DDL_ADD_COLUMN == alter_column_schema->alter_type_) {
          ObObj cur_default_value = new_column_schema.get_cur_default_value();
          if (IS_DEFAULT_NOW_OBJ(cur_default_value) || alter_column_schema->is_default_expr_v2_column()) {
            if (OB_FAIL(ObDDLResolver::calc_default_value(
                    *alter_column_schema, cur_default_value, tz_info_wrap, nls_formats, allocator))) {
              LOG_WARN("fail to calc default now expr", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(alter_column_schema->set_orig_default_value(cur_default_value))) {
              OB_LOG(WARN, "fail to set orig default value", K(cur_default_value), K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::resolve_orig_default_value(ObColumnSchemaV2& alter_column_schema,
    const ObTimeZoneInfoWrap& tz_info_wrap, const common::ObString* nls_formats, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  const ObObj& cur_default_value = alter_column_schema.get_cur_default_value();
  if (!cur_default_value.is_null()) {
    if (OB_FAIL(alter_column_schema.set_orig_default_value(cur_default_value))) {
      LOG_WARN("fail to set orig default value for alter table", K(ret), K(cur_default_value));
    }
  } else if (alter_column_schema.is_nullable()) {
    ObObj null_obj;
    null_obj.set_null();
    if (OB_FAIL(alter_column_schema.set_orig_default_value(null_obj))) {
      LOG_WARN("fail to set origin default value", K(ret));
    }
  } else {
    ObObj default_value;
    default_value.set_type(alter_column_schema.get_data_type());
    if (OB_FAIL(default_value.build_not_strict_default_value())) {
      LOG_WARN("failed to build not strict default value", K(ret));
    } else if (OB_FAIL(alter_column_schema.set_orig_default_value(default_value))) {
      LOG_WARN("failed to set orig default value", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObObj orig_default_value = alter_column_schema.get_orig_default_value();
    if (IS_DEFAULT_NOW_OBJ(orig_default_value) || alter_column_schema.is_default_expr_v2_column()) {
      if (OB_FAIL(ObDDLResolver::calc_default_value(
              alter_column_schema, orig_default_value, tz_info_wrap, nls_formats, allocator))) {
        LOG_WARN("fail to calc default now expr", K(ret));
      } else if (OB_FAIL(alter_column_schema.set_orig_default_value(orig_default_value))) {
        LOG_WARN("fail to set orig default value", K(orig_default_value), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::deal_default_value_padding(ObColumnSchemaV2& column_schema, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObString str;
  if (column_schema.get_orig_default_value().is_null() || column_schema.get_data_type() != ObCharType ||
      column_schema.get_collation_type() != CS_TYPE_BINARY) {
    // nothing to do;
  } else if (OB_FAIL(column_schema.get_orig_default_value().get_string(str))) {
    LOG_WARN("fail to get string", K(ret));
  } else {
    int64_t strlen = ObCharset::strlen_char(column_schema.get_collation_type(), str.ptr(), str.length());
    if (strlen >= column_schema.get_data_length()) {
      // nothing to do
      // check_default_value_length will check length;
    } else {
      char* ptr = NULL;
      int64_t real_size = str.length() + column_schema.get_data_length() - strlen;
      if (NULL == (ptr = static_cast<char*>(allocator.alloc(real_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(ptr, str.ptr(), str.length());
        memset(ptr + str.length(), OB_PADDING_BINARY, column_schema.get_data_length() - strlen);
        ObString new_string(real_size, ptr);
        ObObj new_default_value;
        new_default_value.set_binary(new_string);
        column_schema.set_orig_default_value(new_default_value);
      }
    }
  }
  return ret;
}

int ObDDLOperator::check_not_null_attribute(const ObTableSchema& table_schema,
    const ObColumnSchemaV2& old_column_schema, const ObColumnSchemaV2& new_column_schema)
{
  int ret = OB_SUCCESS;
  if (old_column_schema.is_nullable() && !new_column_schema.is_nullable()) {
    ret = OB_ER_INVALID_USE_OF_NULL;
    LOG_WARN("Alter table change nullable column to not nullable is dangerous",
        K(ret),
        table_schema.get_table_name(),
        new_column_schema.get_column_name());
  }
  return ret;
}

int ObDDLOperator::check_generated_column_modify_authority(
    const ObColumnSchemaV2& old_column_schema, const AlterColumnSchema& alter_column_schema)
{
  int ret = OB_SUCCESS;
  if (old_column_schema.is_generated_column() && alter_column_schema.is_generated_column()) {
    ObString old_def;
    ObString alter_def;
    if (OB_FAIL(old_column_schema.get_cur_default_value().get_string(old_def))) {
      LOG_WARN("get old generated column definition failed", K(ret), K(old_column_schema));
    } else if (OB_FAIL(alter_column_schema.get_cur_default_value().get_string(alter_def))) {
      LOG_WARN("get new generated column definition failed", K(ret), K(alter_column_schema));
    } else if (!ObCharset::case_insensitive_equal(old_def, alter_def)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify generated column definition");
      LOG_WARN("generated column schema definition changed", K(ret), K(old_column_schema), K(alter_column_schema));
    }
  } else if (old_column_schema.is_generated_column() || alter_column_schema.is_generated_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Changing the STORED status for generated columns");
  }
  return ret;
}

// don't allow alter materialized view related columns
// this rule will be change in the next implemation.
int ObDDLOperator::validate_update_column_for_materialized_view(
    const ObTableSchema& orig_table_schema, const ObColumnSchemaV2& orig_column_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> mv_ids;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_mv_ids(tenant_id, mv_ids))) {
    LOG_WARN("fail to get all mv ids", K(ret), "vesion", orig_table_schema.get_schema_version());
  } else {
    uint64_t mv_id = OB_INVALID_ID;
    const ObTableSchema* mv = NULL;
    for (int64_t i = 0; i < mv_ids.count() && OB_SUCC(ret); i++) {
      mv_id = mv_ids.at(i);
      if (OB_FAIL(schema_guard.get_table_schema(mv_id, mv))) {
        LOG_WARN("fail to get table schema", K(ret), K(mv_id));
      } else if (mv && mv->has_table(orig_table_schema.get_table_id()) &&
                 mv->get_column_schema(orig_table_schema.get_table_id(), orig_column_schema.get_column_id())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("don't allow update column if it's in materialized view", K(ret), K(*mv), K(orig_column_schema));
      }
    }
  }

  return ret;
}

int ObDDLOperator::rebuild_constraint_check_expr(const share::schema::ObColumnSchemaV2& orig_column,
    const share::schema::ObColumnSchemaV2& alter_column, const share::schema::ObConstraint& cst,
    share::schema::ObTableSchema& table_schema, const common::ObTimeZoneInfo& tz_info, common::ObIAllocator& allocator,
    ObString& new_check_expr_str, bool& need_modify_check_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory expr_factory(allocator);
  ObSQLSessionInfo default_session;
  ObRawExpr* expr = NULL;
  const ParseNode* node = NULL;
  ObArray<ObQualifiedName> columns;
  const ObColumnSchemaV2* col_schema = NULL;
  ObString orig_check_expr = cst.get_check_expr_str();
  if (OB_FAIL(default_session.init(0, 0, 0, &allocator))) {
    LOG_WARN("init empty session failed", K(ret));
  } else if (OB_FAIL(default_session.load_default_sys_variable(false, false))) {
    LOG_WARN("session load default system variable failed", K(ret));
  } else if (orig_check_expr.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check expr is empty", K(ret));
  } else {
    char* new_check_expr_buf = NULL;
    int64_t outer_pos = 0;
    if (OB_ISNULL(new_check_expr_buf = static_cast<char*>(allocator.alloc(OB_MAX_SQL_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new_check_expr_buf", K(ret));
    } else {
      ObRawExprModifyColumnName modifyColumnName(alter_column.get_column_name_str(), orig_column.get_column_name_str());
      if (OB_FAIL(ObRawExprUtils::parse_bool_expr_node_from_str(orig_check_expr, expr_factory.get_allocator(), node))) {
        LOG_WARN("parse expr node from string failed", K(ret));
      } else if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_check_constraint_expr(
                     expr_factory, default_session, *node, expr, columns))) {
        LOG_WARN("build generated column expr failed", K(ret), K(orig_check_expr));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
        const ObQualifiedName& q_name = columns.at(i);
        if (0 == orig_column.get_column_name_str().case_compare(q_name.col_name_)) {
          need_modify_check_expr = true;
        }
        if (OB_UNLIKELY(!q_name.database_name_.empty() || OB_UNLIKELY(!q_name.tbl_name_.empty()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid generated_column column name", K(q_name));
        } else if (OB_ISNULL(col_schema = table_schema.get_column_schema(q_name.col_name_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is null", K(ret), K(q_name.col_name_));
        } else if (OB_FAIL(ObRawExprUtils::init_column_expr(*col_schema, *q_name.ref_expr_))) {
          LOG_WARN("init column expr failed", K(ret), K((*col_schema).get_column_name_str()));
        } else {
          q_name.ref_expr_->set_ref_id(table_schema.get_table_id(), col_schema->get_column_id());
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(modifyColumnName.modifyColumnName(*expr))) {
        LOG_WARN("modifyColumnName modify column name failed", K(ret));
      } else {
        SMART_VAR(char[OB_MAX_SQL_LENGTH], expr_str_buf)
        {
          MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
          int64_t inner_pos = 0;
          ObRawExprPrinter expr_printer(expr_str_buf, OB_MAX_SQL_LENGTH, &inner_pos, &tz_info);
          if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
            LOG_WARN("print expr definition failed", K(ret));
          } else if (OB_FAIL(databuff_printf(new_check_expr_buf,
                         OB_MAX_SQL_LENGTH,
                         outer_pos,
                         "%.*s",
                         static_cast<int>(inner_pos),
                         expr_str_buf))) {
            LOG_WARN("fail to print expr_str_buf", K(ret), K(expr_str_buf));
          }
        }
      }
      if (OB_SUCC(ret)) {
        new_check_expr_str.assign_ptr(new_check_expr_buf, static_cast<int32_t>(outer_pos));
      }
    }
  }

  return ret;
}

int ObDDLOperator::modify_constraint_check_expr(const share::schema::ObColumnSchemaV2& orig_column,
    const share::schema::ObColumnSchemaV2& alter_column, share::schema::ObTableSchema& table_schema,
    const common::ObTimeZoneInfo& tz_info, common::ObIAllocator& allocator, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  bool need_modify_check_expr = false;
  ObString new_check_expr_str;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  }
  for (; OB_SUCC(ret) && iter != table_schema.constraint_end(); ++iter) {
    need_modify_check_expr = false;
    if ((*iter)->get_check_expr_str().empty()) {
      continue;
    }
    if (OB_FAIL(rebuild_constraint_check_expr(orig_column,
            alter_column,
            **iter,
            table_schema,
            tz_info,
            allocator,
            new_check_expr_str,
            need_modify_check_expr))) {
      LOG_WARN("fail to gen constraint check expr", K(ret));
    } else if (need_modify_check_expr) {
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else {
        (*iter)->set_schema_version(new_schema_version);
        (*iter)->set_check_expr(new_check_expr_str);
        (*iter)->set_is_modify_check_expr(true);
        if (OB_FAIL(
                schema_service->get_table_sql_service().update_check_constraint_state(trans, table_schema, **iter))) {
          LOG_WARN("update check expr constraint failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::modify_part_func_expr(const share::schema::ObColumnSchemaV2& orig_column,
    const share::schema::ObColumnSchemaV2& alter_column, share::schema::ObTableSchema& table_schema,
    const common::ObTimeZoneInfo& tz_info, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;

  if (!table_schema.is_partitioned_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part level", K(ret), K(table_schema.get_part_level()));
  } else if (PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
    if (OB_FAIL(modify_func_expr_column_name(orig_column, alter_column, table_schema, tz_info, allocator, false))) {
      LOG_WARN("fail to modify func expr column name", K(ret), K(orig_column), K(alter_column), K(table_schema));
    }
  } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
    if (OB_FAIL(modify_func_expr_column_name(orig_column, alter_column, table_schema, tz_info, allocator, false))) {
      LOG_WARN("fail to modify func expr column name for partition level one",
          K(ret),
          K(orig_column),
          K(alter_column),
          K(table_schema));
    } else if (OB_FAIL(
                   modify_func_expr_column_name(orig_column, alter_column, table_schema, tz_info, allocator, true))) {
      LOG_WARN("fail to modify func expr column name for partition level two",
          K(ret),
          K(orig_column),
          K(alter_column),
          K(table_schema));
    }
  }

  return ret;
}

int ObDDLOperator::modify_func_expr_column_name(const ObColumnSchemaV2& orig_column,
    const ObColumnSchemaV2& alter_column, ObTableSchema& table_schema, const ObTimeZoneInfo& tz_info,
    common::ObIAllocator& allocator, bool is_sub_part)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory expr_factory(allocator);
  ObSQLSessionInfo default_session;
  ObRawExpr* expr = NULL;
  ObArray<ObQualifiedName> columns;
  const ObColumnSchemaV2* col_schema = NULL;
  ObPartitionOption& part_option = table_schema.get_part_option();
  ObPartitionOption& sub_part_option = table_schema.get_sub_part_option();
  ObString orig_part_expr;
  ObArray<ObString> expr_strs;

  if (!is_sub_part) {
    orig_part_expr = part_option.get_part_func_expr_str();
  } else {
    orig_part_expr = sub_part_option.get_part_func_expr_str();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(default_session.init(0, 0, 0, &allocator))) {
    LOG_WARN("init empty session failed", K(ret));
  } else if (OB_FAIL(default_session.load_default_sys_variable(false, false))) {
    LOG_WARN("session load default system varialbe failed", K(ret));
  } else if (orig_part_expr.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition func expr is empty", K(ret));
  } else if (OB_FAIL(split_on(orig_part_expr, ',', expr_strs))) {
    LOG_WARN("fail to split func expr", K(ret), K(orig_part_expr));
  } else {
    char* new_part_func_expr_buf = NULL;
    int64_t outer_pos = 0;
    if (OB_ISNULL(new_part_func_expr_buf = static_cast<char*>(allocator.alloc(OB_MAX_SQL_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new_part_func_expr", K(ret));
    } else {
      ObRawExprModifyColumnName modifyColumnName(alter_column.get_column_name_str(), orig_column.get_column_name_str());
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_strs.count(); ++i) {
        expr = NULL;
        columns.reset();
        if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(
                expr_strs.at(i), expr_factory, default_session, expr, columns))) {
          LOG_WARN("build generated column expr failed", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
          const ObQualifiedName& q_name = columns.at(i);
          if (OB_UNLIKELY(!q_name.database_name_.empty() || OB_UNLIKELY(!q_name.tbl_name_.empty()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invaild generated_column column name", K(q_name));
          } else if (OB_ISNULL(col_schema = table_schema.get_column_schema(q_name.col_name_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is null", K(ret), K(q_name.col_name_));
          } else if (OB_FAIL(ObRawExprUtils::init_column_expr(*col_schema, *q_name.ref_expr_))) {
            LOG_WARN("init column expr failed", K(ret));
          } else {
            q_name.ref_expr_->set_ref_id(table_schema.get_table_id(), col_schema->get_column_id());
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(modifyColumnName.modifyColumnName(*expr))) {
          LOG_WARN("modifyColumnName modify column name failed", K(ret));
        } else {
          SMART_VAR(char[OB_MAX_SQL_LENGTH], expr_str_buf)
          {
            MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
            int64_t inner_pos = 0;
            ObRawExprPrinter expr_printer(expr_str_buf, OB_MAX_SQL_LENGTH, &inner_pos, &tz_info);
            if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
              LOG_WARN("print expr definition failed", K(ret));
            } else if (0 == i && OB_FAIL(databuff_printf(new_part_func_expr_buf,
                                     OB_MAX_SQL_LENGTH,
                                     outer_pos,
                                     "%.*s",
                                     static_cast<int>(inner_pos),
                                     expr_str_buf))) {
              LOG_WARN("fail to print expr_str_buf", K(ret), K(i), K(expr_str_buf));
            } else if (0 != i && OB_FAIL(databuff_printf(new_part_func_expr_buf,
                                     OB_MAX_SQL_LENGTH,
                                     outer_pos,
                                     ", %.*s",
                                     static_cast<int>(inner_pos),
                                     expr_str_buf))) {
              LOG_WARN("fail to print expr_str_buf", K(ret), K(i), K(expr_str_buf));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObString new_part_func_expr_str;
        if (FALSE_IT(new_part_func_expr_str.assign_ptr(new_part_func_expr_buf, static_cast<int32_t>(outer_pos)))) {
        } else if (!is_sub_part && OB_FAIL(part_option.set_part_expr(new_part_func_expr_str))) {
          LOG_WARN("set part expr failed", K(ret));
        } else if (is_sub_part && OB_FAIL(sub_part_option.set_part_expr(new_part_func_expr_str))) {
          LOG_WARN("set sub part expr failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::modify_generated_column_default_value(ObColumnSchemaV2& generated_column,
    common::ObString& column_name, const ObString& new_column_name, const ObTableSchema& table_schema,
    const ObTimeZoneInfo& tz_info)
{
  int ret = OB_SUCCESS;
  if (generated_column.is_generated_column()) {
    ObString col_def;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    ObRawExprFactory expr_factory(allocator);
    ObSQLSessionInfo default_session;
    ObRawExpr* expr = NULL;

    if (OB_FAIL(default_session.init(0, 0, 0, &allocator))) {
      LOG_WARN("init empty session failed", K(ret));
    } else if (OB_FAIL(default_session.load_default_sys_variable(false, false))) {
      LOG_WARN("session load default system variable failed", K(ret));
    } else if (OB_FAIL(generated_column.get_cur_default_value().get_string(col_def))) {
      LOG_WARN("get cur default value failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(
                   col_def, expr_factory, default_session, table_schema, expr))) {
      LOG_WARN("build generated column expr failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObRawExprModifyColumnName modifyColumnName(new_column_name, column_name);
      if (OB_FAIL(modifyColumnName.modifyColumnName(*expr))) {
        LOG_WARN("modifyColumnName modify column name failed", K(ret));
      } else {
        SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf)
        {
          MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
          ObString expr_def;
          int64_t pos = 0;
          ObObj default_value;
          ObRawExprPrinter expr_printer(expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos, &tz_info);
          if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
            LOG_WARN("print expr definition failed", K(ret));
          } else if (FALSE_IT(expr_def.assign_ptr(expr_str_buf, static_cast<int32_t>(pos)))) {
          } else if (FALSE_IT(default_value.set_varchar(expr_def))) {
          } else if (OB_FAIL(generated_column.set_cur_default_value(default_value))) {
            LOG_WARN("set cur default value failed", K(ret));
          } else if (OB_FAIL(generated_column.set_orig_default_value(default_value))) {
            LOG_WARN("set original default value failed", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::update_prev_id_for_delete_column(const ObTableSchema& origin_table_schema,
    ObTableSchema& new_table_schema, const ObColumnSchemaV2& ori_column_schema, ObSchemaService& schema_service,
    common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  // When a transaction currently add/drop column: origin_table_schema don't update prev&next column ID, so it need
  // fetch from new table.
  ObColumnSchemaV2* new_origin_col = new_table_schema.get_column_schema(ori_column_schema.get_column_name());
  if (OB_ISNULL(new_origin_col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get column from new table schema", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    ObColumnSchemaV2* next_col =
        new_table_schema.get_column_schema_by_prev_next_id(new_origin_col->get_next_column_id());
    if (OB_ISNULL(next_col)) {
      // do nothing since local_column is tail column
    } else {
      next_col->set_prev_column_id(new_origin_col->get_prev_column_id());
      next_col->set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service.get_table_sql_service().update_single_column(
              trans, origin_table_schema, new_table_schema, *next_col))) {
        LOG_WARN("Failed to update single column", K(ret), K(next_col->get_column_name_str()));
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_prev_id_for_add_column(const ObTableSchema& origin_table_schema,
    ObTableSchema& new_table_schema, AlterColumnSchema& alter_column_schema, ObSchemaService& schema_service,
    common::ObMySQLTransaction& trans)
{
  UNUSED(schema_service);
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  const bool is_first = alter_column_schema.is_first_;
  const bool is_after = (!alter_column_schema.get_prev_column_name().empty());
  const bool is_before = (!alter_column_schema.get_next_column_name().empty());
  const bool is_last = !(is_first || is_after || is_before);
  if (is_last) {
    // do nothing
  } else {
    ObString pos_column_name;
    const uint64_t alter_column_id = alter_column_schema.get_column_id();
    if (is_first) {
      // this first means the first of no hidden/shdow column.
      ObColumnIterByPrevNextID iter(new_table_schema);
      const ObColumnSchemaV2* head_col = NULL;
      const ObColumnSchemaV2* col = NULL;
      bool is_first = false;
      while (OB_SUCC(ret) && OB_SUCC(iter.next(col))) {
        if (OB_ISNULL(col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column is null", K(ret));
        } else if (col->is_shadow_column() || col->is_hidden()) {
          // do nothing
        } else if (!is_first) {
          head_col = col;
          is_first = true;
        }
      }
      if (ret != OB_ITER_END) {
        LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
      } else {
        ret = OB_SUCCESS;
        if (OB_ISNULL(head_col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Failed to get first column", K(ret));
        } else {
          alter_column_schema.set_next_column_name(head_col->get_column_name());
        }
      }
    }
    if (OB_SUCC(ret)) {
      pos_column_name =
          (is_after ? alter_column_schema.get_prev_column_name() : alter_column_schema.get_next_column_name());
      ObColumnSchemaV2* pos_column_schema = new_table_schema.get_column_schema(pos_column_name);
      ObColumnSchemaV2* update_column_schema = NULL;
      if (OB_ISNULL(pos_column_schema)) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
            pos_column_name.length(),
            pos_column_name.ptr(),
            new_table_schema.get_table_name_str().length(),
            new_table_schema.get_table_name_str().ptr());
        LOG_WARN("pos column is NULL", K(pos_column_name));
      } else {
        if (is_after) {
          // add column after
          alter_column_schema.set_prev_column_id(pos_column_schema->get_column_id());
          update_column_schema =
              new_table_schema.get_column_schema_by_prev_next_id(pos_column_schema->get_next_column_id());
          if (OB_NOT_NULL(update_column_schema)) {
            update_column_schema->set_prev_column_id(alter_column_id);
          }
        } else {
          // add column before / first
          alter_column_schema.set_prev_column_id(pos_column_schema->get_prev_column_id());
          update_column_schema = pos_column_schema;
          update_column_schema->set_prev_column_id(alter_column_id);
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(update_column_schema)) {
            // alter column is the last column
          } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else {
            update_column_schema->set_schema_version(new_schema_version);
            if (OB_FAIL(schema_service.get_table_sql_service().update_single_column(
                    trans, origin_table_schema, new_table_schema, *update_column_schema))) {
              LOG_WARN("Failed to update single column", K(ret), K(update_column_schema->get_column_name_str()));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::alter_table_column(const ObTableSchema& origin_table_schema,
    const AlterTableSchema& alter_table_schema, ObTableSchema& new_table_schema,
    const common::ObTimeZoneInfoWrap& tz_info_wrap, const common::ObString* nls_formats,
    common::ObMySQLTransaction& trans, common::ObIAllocator& allocator,
    ObIArray<ObTableSchema>* global_idx_schema_array /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  const ObTenantSchema* tenant_schema = NULL;
  const ObSysVariableSchema* sys_variable_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  bool is_oracle_mode = false;
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  LOG_DEBUG("check before alter table column", K(origin_table_schema), K(alter_table_schema), K(new_table_schema));
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(new_table_schema.get_tenant_id(), tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), "tenant_id", new_table_schema.get_tenant_id());
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", K(ret), "tenant_id", new_table_schema.get_tenant_id());
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", K(ret));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", K(ret));
  } else if (OB_FAIL(sys_variable_schema->get_oracle_mode(is_oracle_mode))) {
    RS_LOG(WARN, "failed to get oracle mode", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tz_info_wrap.get_time_zone_info()) ||
             OB_ISNULL(tz_info_wrap.get_time_zone_info()->get_tz_info_map())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tz_info_wrap", K(tz_info_wrap), K(ret));
  } else {
    AlterColumnSchema* alter_column_schema;
    ObTableSchema::const_column_iterator it_begin = alter_table_schema.column_begin();
    ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();

    share::ObWorker::CompatMode compat_mode =
        (is_oracle_mode ? share::ObWorker::CompatMode::ORACLE : share::ObWorker::CompatMode::MYSQL);
    CompatModeGuard tmpCompatModeGuard(compat_mode);

    common::hash::ObPlacementHashSet<ObColumnNameHashWrapper, common::OB_MAX_COLUMN_NUMBER> update_column_name_set;
    ObArray<ObTableSchema> idx_schema_array;
    if (OB_FAIL(generate_tmp_idx_schemas(new_table_schema, idx_schema_array))) {
      LOG_WARN("generate tmp idx schemas failed", K(ret));
    }

    // Extended type info is resolved in session collation type, then we convert it to
    // system collation in ObDDLResolver::fill_extended_type_info().
    const ObCollationType cur_extended_type_info_collation = ObCharset::get_system_collation();
    for (; OB_SUCC(ret) && it_begin != it_end; it_begin++) {
      if (OB_ISNULL(*it_begin)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("*it_begin is NULL", K(ret));
      } else {
        alter_column_schema = static_cast<AlterColumnSchema*>(*it_begin);
        ObObjTypeClass col_tc = alter_column_schema->get_data_type_class();
        ObCollationType collation_type = new_table_schema.get_collation_type();
        ObCharsetType charset_type = new_table_schema.get_charset_type();
        const ObString& orig_column_name = alter_column_schema->get_origin_column_name();
        // cnolumn that has been alter, change or modify
        const ObColumnSchemaV2* orig_column_schema = NULL;
        switch (alter_column_schema->alter_type_) {
          case OB_DDL_ADD_COLUMN: {
            // fill column collation
            if (ObStringTC == col_tc) {
              if (OB_FAIL(ObDDLResolver::check_and_fill_column_charset_info(
                      *alter_column_schema, charset_type, collation_type))) {
                RS_LOG(WARN, "failed to fill column charset info");
              } else if (OB_FAIL(ObDDLResolver::check_string_column_length(*alter_column_schema, is_oracle_mode))) {
                RS_LOG(WARN, "failed to check string column length");
              }
            } else if (ObRawTC == col_tc) {
              if (OB_FAIL(ObDDLResolver::check_raw_column_length(*alter_column_schema))) {
                RS_LOG(WARN, "failed to check raw column length", K(ret), K(*alter_column_schema));
              }
            } else if (ob_is_text_tc(alter_column_schema->get_data_type())) {
              if (OB_FAIL(ObDDLResolver::check_and_fill_column_charset_info(*alter_column_schema,
                      new_table_schema.get_charset_type(),
                      new_table_schema.get_collation_type()))) {
                RS_LOG(WARN, "failed to fill column charset info");
              } else if (OB_FAIL(ObDDLResolver::check_text_column_length_and_promote(*alter_column_schema))) {
                RS_LOG(WARN, "failed to check text or blob column length");
              }
            } else if (ObEnumSetTC == col_tc) {
              if (OB_FAIL(ObDDLResolver::check_and_fill_column_charset_info(
                      *alter_column_schema, charset_type, collation_type))) {
                LOG_WARN("fail to check and fill column charset info", K(ret), K(*alter_column_schema));
              } else if (OB_FAIL(ObResolverUtils::check_extended_type_info(allocator,
                             alter_column_schema->get_extended_type_info(),
                             cur_extended_type_info_collation,
                             alter_column_schema->get_column_name_str(),
                             alter_column_schema->get_data_type(),
                             alter_column_schema->get_collation_type(),
                             alter_table_schema.get_sql_mode()))) {
                LOG_WARN("fail to fill extended type info", K(ret), K(*alter_column_schema));
              } else if (OB_FAIL(ObDDLResolver::calc_enum_or_set_data_length(*alter_column_schema))) {
                LOG_WARN("fail to calc data length", K(ret), K(*alter_column_schema));
              }
            }

            if (OB_SUCC(ret)) {
              // need to check column duplicate?
              if (OB_FAIL(ObDDLResolver::check_default_value(alter_column_schema->get_cur_default_value(),
                      tz_info_wrap,
                      nls_formats,
                      allocator,
                      new_table_schema,
                      *alter_column_schema))) {
                RS_LOG(WARN, "fail to check default value", KPC(alter_column_schema), K(ret));
              } else if (OB_FAIL(
                             resolve_orig_default_value(*alter_column_schema, tz_info_wrap, nls_formats, allocator))) {
                RS_LOG(WARN, "fail to resolve default value", K(ret));
              } else if (alter_column_schema->is_primary_key_) {
                if (new_table_schema.get_rowkey_column_num() > 0) {
                  if (new_table_schema.is_no_pk_table()) {
                    ret = OB_NOT_SUPPORTED;
                    RS_LOG(WARN, "not support to add primary key!", K(ret));
                  } else {
                    ret = OB_ERR_MULTIPLE_PRI_KEY;
                    RS_LOG(WARN, "multiple primary key defined", K(ret));
                  }
                }
              }
            }
            if (OB_SUCC(ret)) {
              int64_t max_used_column_id = new_table_schema.get_max_used_column_id();
              const uint64_t tenant_id = new_table_schema.get_tenant_id();
              if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
                LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
              } else if (is_inner_table(new_table_schema.get_table_id()) &&
                         (OB_INVALID_ID == alter_column_schema->get_column_id() ||
                             alter_column_schema->get_column_id() != max_used_column_id + 1)) {
                // 225 is barrier version, after this adding column in system table need specify column_id
                ret = OB_OP_NOT_ALLOW;
                LOG_WARN("inner table should add column at last and specify column_id",
                    K(ret),
                    KPC(alter_column_schema),
                    K(max_used_column_id));
                LOG_USER_ERROR(OB_OP_NOT_ALLOW, "inner table add column without column_id");
              } else {
                alter_column_schema->set_column_id(++max_used_column_id);
                alter_column_schema->set_rowkey_position(0);
                alter_column_schema->set_index_position(0);
                alter_column_schema->set_not_part_key();
                alter_column_schema->set_table_id(new_table_schema.get_table_id());
                alter_column_schema->set_tenant_id(new_table_schema.get_tenant_id());
                alter_column_schema->set_cur_default_value_colloation(alter_column_schema->get_collation_type());
                alter_column_schema->set_ori_default_value_colloation(alter_column_schema->get_collation_type());
                new_table_schema.set_max_used_column_id(max_used_column_id);
                // every operation need a schema version and should be different
                alter_column_schema->set_schema_version(new_schema_version);
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(update_prev_id_for_add_column(
                      origin_table_schema, new_table_schema, *alter_column_schema, *schema_service, trans))) {
                LOG_WARN("failed to update column previous id", K(ret), K(alter_column_schema->get_column_name_str()));
              }
            }
            if (OB_SUCC(ret)) {
              const ObColumnSchemaV2* mem_col = NULL;
              if (OB_FAIL(new_table_schema.add_column(*alter_column_schema))) {
                if (OB_ERR_COLUMN_DUPLICATE == ret) {
                  const ObString& column_name = alter_column_schema->get_column_name_str();
                  LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, column_name.length(), column_name.ptr());
                  RS_LOG(WARN, "duplicate column name", K(column_name), K(ret));
                }
                RS_LOG(WARN, "failed to add new column", K(ret));
              } else if (OB_FAIL(resolve_timestamp_column(alter_column_schema,
                             new_table_schema,
                             *alter_column_schema,
                             tz_info_wrap,
                             nls_formats,
                             allocator))) {
                RS_LOG(WARN, "fail to resolve timestamp column", K(ret));
              } else if (OB_FAIL(deal_default_value_padding(*alter_column_schema, allocator))) {
                RS_LOG(WARN, "fail to deal default value padding", KPC(alter_column_schema), K(ret));
              } else if (OB_FAIL(new_table_schema.check_primary_key_cover_partition_column())) {
                RS_LOG(WARN, "fail to check primary key cover partition column", K(ret));
              } else if (OB_ISNULL(
                             mem_col = new_table_schema.get_column_schema(alter_column_schema->get_column_id()))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("mem_col is NULL", K(ret));
              } else {
                alter_column_schema->set_prev_column_id(mem_col->get_prev_column_id());
                if (OB_FAIL(schema_service->get_table_sql_service().insert_single_column(
                        trans, new_table_schema, *alter_column_schema))) {
                  RS_LOG(WARN, "failed to add column", K(alter_column_schema), K(ret));
                }
              }
            }

            break;
          }
          case OB_DDL_CHANGE_COLUMN: {
            orig_column_schema = new_table_schema.get_column_schema(orig_column_name);
            const ObColumnSchemaV2* column_schema_from_old_table_schema =
                origin_table_schema.get_column_schema(orig_column_name);
            ObColumnNameHashWrapper orig_column_key(orig_column_name);
            // Changing a column that is added in the same alter table clause is not allowed.
            if (NULL == column_schema_from_old_table_schema) {
              ret = OB_ERR_BAD_FIELD_ERROR;
              LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                  orig_column_name.length(),
                  orig_column_name.ptr(),
                  origin_table_schema.get_table_name_str().length(),
                  origin_table_schema.get_table_name_str().ptr());
              RS_LOG(WARN, "failed to find old column schema!", K(ret), K(orig_column_name));
            } else if (OB_HASH_EXIST == update_column_name_set.exist_refactored(orig_column_key)) {
              // column that has been modified, can't not modify again
              ret = OB_ERR_BAD_FIELD_ERROR;
              LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                  orig_column_name.length(),
                  orig_column_name.ptr(),
                  origin_table_schema.get_table_name_str().length(),
                  origin_table_schema.get_table_name_str().ptr());
              RS_LOG(WARN, "column that has been altered, can't not update again");
            } else if (OB_FAIL(check_generated_column_modify_authority(*orig_column_schema, *alter_column_schema))) {
              LOG_WARN("check generated column modify authority", KPC(orig_column_schema), KPC(alter_column_schema));
            }

            if (OB_SUCC(ret)) {
              if (orig_column_schema->has_generated_column_deps()) {
                ObTableSchema::const_column_iterator tmp_begin = origin_table_schema.column_begin();
                ObTableSchema::const_column_iterator tmp_end = origin_table_schema.column_end();
                for (; OB_SUCC(ret) && tmp_begin != tmp_end; tmp_begin++) {
                  if (OB_ISNULL(*tmp_begin)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("*tmp_begin is NULL", K(ret));
                  } else {
                    ObColumnSchemaV2* tmp_column_schema = static_cast<ObColumnSchemaV2*>(*tmp_begin);
                    if (tmp_column_schema->has_cascaded_column_id(orig_column_schema->get_column_id())) {
                      ObColumnSchemaV2 new_generated_column_schema = *tmp_column_schema;
                      const uint64_t tenant_id = orig_column_schema->get_tenant_id();
                      ObColumnSchemaV2 deps_column = *orig_column_schema;
                      if (OB_FAIL(modify_generated_column_default_value(new_generated_column_schema,
                              const_cast<ObString&>(deps_column.get_column_name_str()),
                              alter_column_schema->get_column_name_str(),
                              origin_table_schema,
                              *tz_info_wrap.get_time_zone_info()))) {
                        LOG_WARN("modify generated column value failed", K(ret));
                      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
                        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
                      } else {
                        new_generated_column_schema.set_schema_version(new_schema_version);
                        if (OB_FAIL(new_table_schema.alter_column(new_generated_column_schema))) {
                          RS_LOG(WARN, "failed to change column", K(ret));
                        } else if (OB_FAIL(schema_service->get_table_sql_service().update_single_column(
                                       trans, origin_table_schema, new_table_schema, new_generated_column_schema))) {
                          LOG_WARN("generated column failed to alter column", K(ret));
                        }
                      }
                    }
                  }
                }
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(validate_update_column_for_materialized_view(origin_table_schema, *orig_column_schema))) {
                LOG_WARN("fail to validate update column for materialized view", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              // fill column collation
              if (ObStringTC == alter_column_schema->get_data_type_class()) {
                if (OB_FAIL(ObDDLResolver::check_and_fill_column_charset_info(
                        *alter_column_schema, charset_type, collation_type))) {
                  RS_LOG(WARN, "failed to fill column charset info");
                } else if (OB_FAIL(ObDDLResolver::check_string_column_length(*alter_column_schema, is_oracle_mode))) {
                  RS_LOG(WARN, "failed to check string column length");
                }
              } else if (ObRawType == alter_column_schema->get_data_type()) {
                if (OB_FAIL(ObDDLResolver::check_raw_column_length(*alter_column_schema))) {
                  RS_LOG(WARN, "failed to check raw column length", K(ret), K(*alter_column_schema));
                }
              } else if (ObEnumSetTC == col_tc) {
                if (OB_FAIL(ObDDLResolver::check_and_fill_column_charset_info(
                        *alter_column_schema, charset_type, collation_type))) {
                  LOG_WARN("fail to check and fill column charset info", K(ret), K(*alter_column_schema));
                } else if (OB_FAIL(ObResolverUtils::check_extended_type_info(allocator,
                               alter_column_schema->get_extended_type_info(),
                               cur_extended_type_info_collation,
                               alter_column_schema->get_column_name_str(),
                               alter_column_schema->get_data_type(),
                               alter_column_schema->get_collation_type(),
                               alter_table_schema.get_sql_mode()))) {
                  LOG_WARN("fail to fill extended type info", K(ret), K(*alter_column_schema));
                } else if (OB_FAIL(ObDDLResolver::check_type_info_incremental_change(
                               *orig_column_schema, *alter_column_schema))) {
                  LOG_WARN("fail to check incremental change", K(ret));
                } else if (OB_FAIL(ObDDLResolver::calc_enum_or_set_data_length(*alter_column_schema))) {
                  LOG_WARN("fail to calc data length", K(ret), K(*alter_column_schema));
                }
              } else if (ob_is_text_tc(alter_column_schema->get_data_type())) {
                if (OB_FAIL(ObDDLResolver::check_and_fill_column_charset_info(*alter_column_schema,
                        new_table_schema.get_charset_type(),
                        new_table_schema.get_collation_type()))) {
                  RS_LOG(WARN, "failed to fill column charset info");
                } else if (OB_FAIL(ObDDLResolver::check_text_column_length_and_promote(*alter_column_schema))) {
                  RS_LOG(WARN, "failed to check text or blob column length");
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(ObDDLResolver::check_default_value(alter_column_schema->get_cur_default_value(),
                      tz_info_wrap,
                      nls_formats,
                      allocator,
                      new_table_schema,
                      *alter_column_schema))) {
                LOG_WARN("fail to check default value", KPC(alter_column_schema), K(ret));
              } else if (alter_column_schema->is_primary_key_) {
                if (new_table_schema.get_rowkey_column_num() > 0) {
                  if (new_table_schema.is_no_pk_table()) {
                    ret = OB_NOT_SUPPORTED;
                    RS_LOG(WARN, "not support to add primary key!", K(ret));
                  } else {
                    ret = OB_ERR_MULTIPLE_PRI_KEY;
                    RS_LOG(WARN, "multiple primary key defined", K(ret));
                  }
                }
              }
              if (OB_SUCC(ret) && alter_column_schema->is_autoincrement_) {
                if (alter_column_schema->is_autoincrement()) {
                  if (orig_column_schema->get_column_id() != new_table_schema.get_autoinc_column_id()) {
                    // not supported now; from non-auto-increment column to auto-increment column
                    ret = OB_NOT_SUPPORTED;
                    RS_LOG(WARN,
                        "from non-auto-increment column to auto-increment column",
                        "alter_column_id",
                        alter_column_schema->get_column_id(),
                        "auto_inc_column_id",
                        new_table_schema.get_autoinc_column_id(),
                        K(ret));
                  }
                }
              }
            }
            if (OB_SUCC(ret) && (0 != orig_column_schema->get_column_name_str().case_compare(
                                          alter_column_schema->get_column_name_str()))) {
              if (orig_column_schema->is_tbl_part_key_column() && OB_FAIL(modify_part_func_expr(*orig_column_schema,
                                                                      *alter_column_schema,
                                                                      new_table_schema,
                                                                      *tz_info_wrap.get_time_zone_info(),
                                                                      allocator))) {
                LOG_WARN("modify part func expr failed", K(ret));
              } else if (OB_FAIL(modify_part_func_expr_for_global_index(*orig_column_schema,
                             *alter_column_schema,
                             new_table_schema,
                             *tz_info_wrap.get_time_zone_info(),
                             allocator,
                             trans,
                             global_idx_schema_array))) {
                LOG_WARN("failed to modify part_func_expr for global_index", K(ret));
              }
            }

            if (OB_SUCC(ret) && (0 != orig_column_schema->get_column_name_str().case_compare(
                                          alter_column_schema->get_column_name_str()))) {
              if (OB_FAIL(modify_constraint_check_expr(*orig_column_schema,
                      *alter_column_schema,
                      new_table_schema,
                      *tz_info_wrap.get_time_zone_info(),
                      allocator,
                      trans))) {
                LOG_WARN("failed to modify check_expr constraint", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(
                      check_can_alter_column_type(*orig_column_schema, *alter_column_schema, origin_table_schema))) {
                LOG_WARN("fail to check can alter column type", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              // copy attributes that can be change by alter table change ...
              ObColumnSchemaV2 new_column_schema = *orig_column_schema;
              const uint64_t tenant_id = orig_column_schema->get_tenant_id();
              if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
                LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
              } else {
                new_column_schema.set_column_name(alter_column_schema->get_column_name_str());
                new_column_schema.set_charset_type(alter_column_schema->get_charset_type());
                new_column_schema.set_collation_type(alter_column_schema->get_collation_type());
                new_column_schema.set_data_type(alter_column_schema->get_data_type());
                new_column_schema.set_data_length(alter_column_schema->get_data_length());
                new_column_schema.set_data_precision(alter_column_schema->get_data_precision());
                new_column_schema.set_data_scale(alter_column_schema->get_data_scale());
                new_column_schema.set_cur_default_value(alter_column_schema->get_cur_default_value());
                new_column_schema.set_cur_default_value_colloation(new_column_schema.get_collation_type());
                new_column_schema.set_ori_default_value_colloation(new_column_schema.get_collation_type());
                new_column_schema.set_nullable(alter_column_schema->is_nullable());
                new_column_schema.set_autoincrement(alter_column_schema->is_autoincrement());
                new_column_schema.set_column_flags(alter_column_schema->get_column_flags());
                new_column_schema.set_comment(alter_column_schema->get_comment_str());
                new_column_schema.set_schema_version(new_schema_version);
                new_column_schema.set_on_update_current_timestamp(
                    alter_column_schema->is_on_update_current_timestamp());
                new_column_schema.set_extended_type_info(alter_column_schema->get_extended_type_info());
              }

              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(check_not_null_attribute(new_table_schema, *orig_column_schema, new_column_schema))) {
                RS_LOG(WARN, "fail to check_not_null_attribute", K(ret));
              } else if (OB_FAIL(resolve_timestamp_column(alter_column_schema,
                             new_table_schema,
                             new_column_schema,
                             tz_info_wrap,
                             nls_formats,
                             allocator))) {
                RS_LOG(WARN, "fail to resolve timestamp column", K(ret));
              } else if (OB_FAIL(new_table_schema.alter_column(new_column_schema))) {
                RS_LOG(WARN, "failed to change column", K(ret));
              } else if (OB_FAIL(check_new_column_for_index(idx_schema_array, new_column_schema))) {
                RS_LOG(WARN, "failed to check new column for index", K(ret));
              } else if (OB_FAIL(new_table_schema.check_primary_key_cover_partition_column())) {
                RS_LOG(WARN, "fail to check primary key cover partition column", K(ret));
              } else if (OB_FAIL(schema_service->get_table_sql_service().update_single_column(
                             trans, origin_table_schema, new_table_schema, new_column_schema))) {
                RS_LOG(WARN, "failed to alter column", K(alter_column_schema), K(ret));
              } else if (OB_FAIL(alter_table_update_index_and_view_column(
                             *schema_service, new_table_schema, new_column_schema, trans, global_idx_schema_array))) {
                RS_LOG(WARN, "failed to update index column", K(ret));
              } else {
                if (OB_HASH_EXIST == update_column_name_set.exist_refactored(orig_column_key)) {
                  ret = OB_HASH_EXIST;
                  RS_LOG(WARN, "duplicate index name", K(ret), K(orig_column_name));
                } else if (OB_FAIL(update_column_name_set.set_refactored(orig_column_key))) {
                  RS_LOG(WARN, "failed to add index_name to hash set.", K(orig_column_name), K(ret));
                }
              }
            }
            break;
          }
          case OB_DDL_MODIFY_COLUMN: {
            LOG_DEBUG("check alter column schema", KPC(alter_column_schema));
            orig_column_schema = new_table_schema.get_column_schema(orig_column_name);
            const ObColumnSchemaV2* column_schema_from_old_table_schema =
                origin_table_schema.get_column_schema(orig_column_name);
            ObColumnNameHashWrapper orig_column_key(orig_column_name);
            // Changing a column that is added in the same alter table clause is not allowed.
            if (NULL == column_schema_from_old_table_schema) {
              ret = OB_ERR_BAD_FIELD_ERROR;
              LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                  orig_column_name.length(),
                  orig_column_name.ptr(),
                  origin_table_schema.get_table_name_str().length(),
                  origin_table_schema.get_table_name_str().ptr());
              RS_LOG(WARN,
                  "failed to find old column schema!",
                  K(new_table_schema),
                  K(orig_column_name),
                  K(origin_table_schema),
                  K(ret));
            } else if (OB_HASH_EXIST == update_column_name_set.exist_refactored(orig_column_key)) {
              // column that has been modified, can't not modify again
              ret = OB_ERR_BAD_FIELD_ERROR;
              LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                  orig_column_name.length(),
                  orig_column_name.ptr(),
                  origin_table_schema.get_table_name_str().length(),
                  origin_table_schema.get_table_name_str().ptr());
              RS_LOG(WARN, "column that has been altered, can't not update again");
            } else if (OB_FAIL(check_generated_column_modify_authority(*orig_column_schema, *alter_column_schema))) {
              LOG_WARN("check generated column modify authority", KPC(orig_column_schema), KPC(alter_column_schema));
            }
            if (OB_SUCC(ret)) {
              // fill column collation
              if (ObStringTC == alter_column_schema->get_data_type_class()) {
                if (OB_FAIL(ObDDLResolver::check_and_fill_column_charset_info(
                        *alter_column_schema, charset_type, collation_type))) {
                  RS_LOG(WARN, "failed to fill column charset info");
                } else if (ObStringTC == alter_column_schema->get_data_type_class() &&
                           OB_FAIL(ObDDLResolver::check_string_column_length(*alter_column_schema, is_oracle_mode))) {
                  RS_LOG(WARN, "failed to check string column length");
                }
              } else if (ObRawTC == col_tc) {
                if (OB_FAIL(ObDDLResolver::check_raw_column_length(*alter_column_schema))) {
                  RS_LOG(WARN, "failed to check raw column length", K(ret), K(*alter_column_schema));
                }
              } else if (ObEnumSetTC == col_tc) {
                if (OB_FAIL(ObDDLResolver::check_and_fill_column_charset_info(
                        *alter_column_schema, charset_type, collation_type))) {
                  LOG_WARN("fail to check and fill column charset info", K(ret), K(*alter_column_schema));
                } else if (OB_FAIL(ObResolverUtils::check_extended_type_info(allocator,
                               alter_column_schema->get_extended_type_info(),
                               cur_extended_type_info_collation,
                               alter_column_schema->get_column_name_str(),
                               alter_column_schema->get_data_type(),
                               alter_column_schema->get_collation_type(),
                               alter_table_schema.get_sql_mode()))) {
                  LOG_WARN("fail to fill extended type info", K(ret), K(*alter_column_schema));
                } else if (OB_FAIL(ObDDLResolver::check_type_info_incremental_change(
                               *orig_column_schema, *alter_column_schema))) {
                  LOG_WARN("fail to check incremental change", K(ret));
                } else if (OB_FAIL(ObDDLResolver::calc_enum_or_set_data_length(*alter_column_schema))) {
                  LOG_WARN("fail to calc data length", K(ret), K(*alter_column_schema));
                }
              } else if (ob_is_text_tc(alter_column_schema->get_data_type())) {
                if (OB_FAIL(ObDDLResolver::check_and_fill_column_charset_info(*alter_column_schema,
                        new_table_schema.get_charset_type(),
                        new_table_schema.get_collation_type()))) {
                  RS_LOG(WARN, "failed to fill column charset info");
                } else if (OB_FAIL(ObDDLResolver::check_text_column_length_and_promote(*alter_column_schema))) {
                  RS_LOG(WARN, "failed to check text or blob column length");
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(check_generated_column_modify_authority(*orig_column_schema, *alter_column_schema))) {
                LOG_WARN("check generated column modify authority failed",
                    K(ret),
                    KPC(orig_column_schema),
                    KPC(alter_column_schema));
              } else if (OB_FAIL(ObDDLResolver::check_default_value(alter_column_schema->get_cur_default_value(),
                             tz_info_wrap,
                             nls_formats,
                             allocator,
                             new_table_schema,
                             *alter_column_schema))) {
                LOG_WARN("fail to check default value", KPC(alter_column_schema), K(ret));
              } else if (alter_column_schema->is_primary_key_) {
                if (new_table_schema.get_rowkey_column_num() > 0) {
                  if (new_table_schema.is_no_pk_table()) {
                    ret = OB_NOT_SUPPORTED;
                    RS_LOG(WARN, "not support to add primary key!", K(ret));
                  } else {
                    ret = OB_ERR_MULTIPLE_PRI_KEY;
                    RS_LOG(WARN, "multiple primary key defined", K(ret));
                  }
                }
              }
              if (OB_SUCC(ret) && alter_column_schema->is_autoincrement_) {
                if (alter_column_schema->is_autoincrement()) {
                  if (orig_column_schema->get_column_id() != new_table_schema.get_autoinc_column_id()) {
                    // not supported now; from non-auto-increment column to auto-increment column
                    ret = OB_NOT_SUPPORTED;
                    RS_LOG(WARN, "from non-auto-increment column to auto-increment column", K(ret));
                  }
                }
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(validate_update_column_for_materialized_view(origin_table_schema, *orig_column_schema))) {
                LOG_WARN("fail to validate update column for materialized view", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(
                      check_can_alter_column_type(*orig_column_schema, *alter_column_schema, origin_table_schema))) {
                LOG_WARN("fail to check can alter column type", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              ObColumnSchemaV2 new_column_schema = *orig_column_schema;
              // copy attributes that can be change by alter table modify ...
              const uint64_t tenant_id = orig_column_schema->get_tenant_id();
              if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
                LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
              } else {
                new_column_schema.set_charset_type(alter_column_schema->get_charset_type());
                new_column_schema.set_collation_type(alter_column_schema->get_collation_type());
                new_column_schema.set_data_type(alter_column_schema->get_data_type());
                new_column_schema.set_data_length(alter_column_schema->get_data_length());
                new_column_schema.set_data_precision(alter_column_schema->get_data_precision());
                new_column_schema.set_data_scale(alter_column_schema->get_data_scale());
                new_column_schema.set_cur_default_value(alter_column_schema->get_cur_default_value());
                new_column_schema.set_cur_default_value_colloation(new_column_schema.get_collation_type());
                new_column_schema.set_ori_default_value_colloation(new_column_schema.get_collation_type());
                new_column_schema.set_nullable(alter_column_schema->is_nullable());
                new_column_schema.set_autoincrement(alter_column_schema->is_autoincrement());
                new_column_schema.set_comment(alter_column_schema->get_comment_str());
                new_column_schema.set_column_flags(alter_column_schema->get_column_flags());
                new_column_schema.set_on_update_current_timestamp(
                    alter_column_schema->is_on_update_current_timestamp());
                new_column_schema.set_schema_version(new_schema_version);
                new_column_schema.set_extended_type_info(alter_column_schema->get_extended_type_info());
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(check_modify_column_when_upgrade(new_column_schema, *orig_column_schema))) {
                LOG_WARN(
                    "fail to check modify column when upgrade", K(ret), K(new_column_schema), K(*orig_column_schema));
              } else if (OB_FAIL(check_not_null_attribute(new_table_schema, *orig_column_schema, new_column_schema))) {
                RS_LOG(WARN, "fail to check_not_null_attribute", K(ret));
              } else if (OB_FAIL(resolve_timestamp_column(alter_column_schema,
                             new_table_schema,
                             new_column_schema,
                             tz_info_wrap,
                             nls_formats,
                             allocator))) {
                RS_LOG(WARN, "fail to resolve timestamp column", K(ret));
              } else if (OB_FAIL(new_table_schema.alter_column(new_column_schema))) {
                RS_LOG(WARN, "failed to change column", K(ret));
              } else if (OB_FAIL(check_new_column_for_index(idx_schema_array, new_column_schema))) {
                RS_LOG(WARN, "failed to check new column for index", K(ret));
              } else if (OB_FAIL(new_table_schema.check_primary_key_cover_partition_column())) {
                RS_LOG(WARN, "fail to check primary key cover partition column", K(ret));
              } else if (OB_FAIL(schema_service->get_table_sql_service().update_single_column(
                             trans, origin_table_schema, new_table_schema, new_column_schema))) {
                RS_LOG(WARN, "failed to alter column", K(alter_column_schema), K(ret));
              } else if (OB_FAIL(alter_table_update_index_and_view_column(
                             *schema_service, new_table_schema, new_column_schema, trans))) {
                RS_LOG(WARN, "failed to update index column", K(ret));
              } else {
                if (OB_HASH_EXIST == update_column_name_set.exist_refactored(orig_column_key)) {
                  ret = OB_HASH_EXIST;
                  RS_LOG(WARN, "duplicate index name", K(ret), K(orig_column_name));
                } else if (OB_FAIL(update_column_name_set.set_refactored(orig_column_key))) {
                  RS_LOG(WARN, "failed to add index_name to hash set.", K(orig_column_name), K(ret));
                }
              }
            }
            break;
          }
          case OB_DDL_ALTER_COLUMN: {
            orig_column_schema = new_table_schema.get_column_schema(orig_column_name);
            const ObColumnSchemaV2* column_schema_from_old_table_schema =
                origin_table_schema.get_column_schema(orig_column_name);
            ObColumnNameHashWrapper orig_column_key(orig_column_name);
            // Changing a column that is added in the same alter table clause is not allowed.
            if (NULL == column_schema_from_old_table_schema) {
              ret = OB_ERR_BAD_FIELD_ERROR;
              LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                  orig_column_name.length(),
                  orig_column_name.ptr(),
                  origin_table_schema.get_table_name_str().length(),
                  origin_table_schema.get_table_name_str().ptr());
              RS_LOG(WARN, "failed to find old column schema!", K(ret), K(orig_column_name));
            } else if (OB_HASH_EXIST == update_column_name_set.exist_refactored(orig_column_key)) {
              ret = OB_ERR_BAD_FIELD_ERROR;
              LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                  orig_column_name.length(),
                  orig_column_name.ptr(),
                  origin_table_schema.get_table_name_str().length(),
                  origin_table_schema.get_table_name_str().ptr());
              RS_LOG(WARN, "column that has been altered, can't not update again");
            } else if (OB_FAIL(check_generated_column_modify_authority(*orig_column_schema, *alter_column_schema))) {
              LOG_WARN("check generated column modify authority", KPC(orig_column_schema), KPC(alter_column_schema));
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(validate_update_column_for_materialized_view(origin_table_schema, *orig_column_schema))) {
                LOG_WARN("fail to validate update column for materialized view", K(ret));
              }
            }

            // column that has been modified, can't not modify again
            if (OB_SUCC(ret)) {
              ObColumnSchemaV2 new_column_schema = *orig_column_schema;
              // orig_column_schema is passed in resolve_timestamp_column to verify whether the now() is dropped.
              if (OB_FAIL(resolve_timestamp_column(alter_column_schema,
                      new_table_schema,
                      new_column_schema,
                      tz_info_wrap,
                      nls_formats,
                      allocator))) {
                RS_LOG(WARN, "fail to resolve timestamp column", K(ret));
              } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
                LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
              } else {
                // new schema version
                new_column_schema.set_schema_version(new_schema_version);
                ObObj default_value;
                if (alter_column_schema->is_drop_default_) {
                  default_value.set_null();
                  new_column_schema.del_column_flag(DEFAULT_EXPR_V2_COLUMN_FLAG);
                  if (OB_FAIL(new_column_schema.set_cur_default_value(default_value))) {
                    RS_LOG(WARN, "failed to set current default value");
                  }
                } else {
                  default_value = alter_column_schema->get_cur_default_value();
                  if (!default_value.is_null() && ob_is_text_tc(new_column_schema.get_data_type())) {
                    ret = OB_INVALID_DEFAULT;
                    LOG_USER_ERROR(OB_INVALID_DEFAULT,
                        new_column_schema.get_column_name_str().length(),
                        new_column_schema.get_column_name_str().ptr());
                    RS_LOG(WARN, "BLOB, TEXT column can't have a default value!", K(default_value), K(ret));
                  } else if (!new_column_schema.is_nullable() && default_value.is_null()) {
                    ret = OB_INVALID_DEFAULT;
                    LOG_USER_ERROR(OB_INVALID_DEFAULT,
                        new_column_schema.get_column_name_str().length(),
                        new_column_schema.get_column_name_str().ptr());
                    RS_LOG(WARN, "not null column with default value null!", K(ret));
                  } else if (OB_FAIL(ObDDLResolver::check_default_value(default_value,
                                 tz_info_wrap,
                                 nls_formats,
                                 allocator,
                                 new_table_schema,
                                 new_column_schema))) {
                    LOG_WARN("fail to check default value", K(new_column_schema), K(ret));
                  } else if (OB_FAIL(new_column_schema.set_cur_default_value(default_value))) {
                    RS_LOG(WARN, "failed to set current default value");
                  }
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(new_table_schema.alter_column(new_column_schema))) {
                  RS_LOG(WARN, "failed to change column", K(ret));
                } else if (OB_FAIL(new_table_schema.check_primary_key_cover_partition_column())) {
                  RS_LOG(WARN, "failed to check primary key cover partition column", K(ret));
                } else if (OB_FAIL(schema_service->get_table_sql_service().update_single_column(
                               trans, origin_table_schema, new_table_schema, new_column_schema))) {
                  RS_LOG(WARN, "failed to alter column", K(alter_column_schema), K(ret));
                } else if (OB_FAIL(alter_table_update_index_and_view_column(
                               *schema_service, new_table_schema, new_column_schema, trans))) {
                  RS_LOG(WARN, "failed to update index column", K(ret));
                } else {
                  if (OB_HASH_EXIST == update_column_name_set.exist_refactored(orig_column_key)) {
                    ret = OB_HASH_EXIST;
                    RS_LOG(WARN, "duplicate index name", K(ret), K(orig_column_name));
                  } else if (OB_FAIL(update_column_name_set.set_refactored(orig_column_key))) {
                    RS_LOG(WARN, "failed to add index_name to hash set.", K(orig_column_name), K(ret));
                  }
                }
              }
            }
            break;
          }
          case OB_DDL_DROP_COLUMN: {
            orig_column_schema = origin_table_schema.get_column_schema(orig_column_name);
            ObColumnNameHashWrapper orig_column_key(orig_column_name);

            if (NULL == orig_column_schema || NULL == new_table_schema.get_column_schema(orig_column_name)) {
              ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
              LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, orig_column_name.length(), orig_column_name.ptr());
              RS_LOG(WARN,
                  "failed to find old column schema!",
                  K(orig_column_schema),
                  K(new_table_schema),
                  K(ret),
                  K(orig_column_name));
            } else if (OB_HASH_EXIST == update_column_name_set.exist_refactored(orig_column_key)) {
              // column that has been modified, can't not modify again
              ret = OB_ERR_BAD_FIELD_ERROR;
              LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                  orig_column_name.length(),
                  orig_column_name.ptr(),
                  origin_table_schema.get_table_name_str().length(),
                  origin_table_schema.get_table_name_str().ptr());
              RS_LOG(WARN, "column that has been altered, can't not update again");
            } else if (orig_column_schema->has_generated_column_deps()) {
              ret = OB_ERR_DEPENDENT_BY_GENERATED_COLUMN;
              LOG_USER_ERROR(OB_ERR_DEPENDENT_BY_GENERATED_COLUMN, orig_column_name.length(), orig_column_name.ptr());
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(validate_update_column_for_materialized_view(origin_table_schema, *orig_column_schema))) {
                LOG_WARN("fail to validate update column for materialized view", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              // First process the index table and the secondary table drop column
              if (OB_FAIL(alter_table_drop_aux_column(
                      *schema_service, new_table_schema, *orig_column_schema, trans, USER_INDEX))) {
                RS_LOG(WARN, "failed to drop index column");
              } else if (OB_FAIL(update_prev_id_for_delete_column(
                             origin_table_schema, new_table_schema, *orig_column_schema, *schema_service, trans))) {
                LOG_WARN("failed to update column previous id for delele column", K(ret));
              } else if (OB_FAIL(new_table_schema.delete_column(orig_column_name))) {
                // drop column will do some check on the new table schema
                RS_LOG(WARN, "failed to drop column schema!", K(ret));
              } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
                LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
              } else if (OB_FAIL(schema_service->get_table_sql_service().delete_single_column(
                             new_schema_version, trans, new_table_schema, *orig_column_schema))) {
                RS_LOG(WARN, "failed to delete column", K(alter_column_schema), K(ret));
              }
            }
            break;
          }
          default: {
            ret = OB_INVALID_ARGUMENT;
            RS_LOG(WARN, "unhandled operator type!", K_(alter_column_schema->alter_type));
            break;
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::split_table_partitions(ObTableSchema& new_schema, const ObTableSchema& origin_table_schema,
    const ObTableSchema& inc_schema, const ObSplitInfo& split_info, const bool is_alter_tablegroup,
    ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  bool is_delay_delete = false;
  ObArray<ObPartition> split_part_array;
  LOG_DEBUG(
      "start to split table partition", K(ret), K(new_schema), K(split_info), K(inc_schema), K(origin_table_schema));

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(check_is_delay_delete(tenant_id, is_delay_delete))) {
    LOG_WARN("check is delay delete failed", K(ret), K(tenant_id));
  } else {
    if (is_delay_delete) {
      ObString delay_deleted_name;
      for (int64_t i = 0; OB_SUCC(ret) && i < split_info.source_part_ids_.count(); ++i) {
        bool check_dropped_schema = false;
        ObPartIteratorV2 iter(origin_table_schema, check_dropped_schema);
        const ObPartition* part = NULL;
        ObPartition tmp_part;
        while (OB_SUCC(ret) && OB_SUCC(iter.next(part))) {
          if (OB_ISNULL(part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret), K(part));
          } else if (part->get_part_id() == split_info.source_part_ids_.at(i)) {
            break;
          }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("find part failed", K(ret));
        } else if (OB_FAIL(tmp_part.assign(*part))) {
          LOG_WARN("assign part failed", K(ret));
        } else if (OB_FAIL(create_new_name_for_delay_delete_table_partition(
                       delay_deleted_name, &origin_table_schema, allocator))) {
          LOG_WARN("fail to create new name for delay delete", K(ret));
        } else if (OB_FAIL(tmp_part.set_part_name(delay_deleted_name))) {
          LOG_WARN("set table name failed", K(ret), K(delay_deleted_name));
        } else if (OB_FAIL(split_part_array.push_back(tmp_part))) {
          LOG_WARN("push back to split_part_array failed", K(ret), K(delay_deleted_name));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_service->get_table_sql_service().split_part_info(
                   trans, new_schema, new_schema.get_schema_version(), split_info))) {
      LOG_WARN("add inc part info failed", K(ret));
    } else if (inc_schema.get_all_part_num() != 1  // abnormal split
               && OB_FAIL(schema_service->get_table_sql_service().remove_source_partition(trans,
                      split_info,
                      new_schema.get_schema_version(),
                      NULL,
                      &origin_table_schema,
                      is_delay_delete,
                      split_part_array))) {
      LOG_WARN("fail to remove source partition", K(ret));
    } else if (!is_alter_tablegroup && OB_FAIL(schema_service->get_table_sql_service().add_partition_key(
                                           trans, origin_table_schema, inc_schema, new_schema.get_schema_version()))) {
      LOG_WARN("fail to add partition key", K(ret));
    } else {
      if (OB_FAIL(schema_service->get_table_sql_service().update_partition_option(
              trans, new_schema, new_schema.get_schema_version()))) {
        LOG_WARN("update partition option failed", K(ret), K(new_schema), K(origin_table_schema));
      } else if (!is_alter_tablegroup) {
        LOG_INFO("update partition option success", K(ret), K(new_schema));
        ObSchemaOperation opt;
        opt.tenant_id_ = new_schema.get_tenant_id();
        opt.database_id_ = new_schema.get_database_id();
        opt.tablegroup_id_ = new_schema.get_tablegroup_id();
        opt.table_id_ = new_schema.get_table_id();
        if (is_delay_delete) {
          opt.op_type_ = OB_DDL_DELAY_DELETE_TABLE_PARTITION;
        } else {
          opt.op_type_ = OB_DDL_ALTER_TABLE;
        }
        opt.schema_version_ = new_schema.get_schema_version();
        if (OB_FAIL(schema_service->get_table_sql_service().log_operation_wrapper(opt, trans))) {
          LOG_WARN("fail to log operation", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::add_table_foreign_keys(const share::schema::ObTableSchema& orig_table_schema,
    share::schema::ObTableSchema& inc_table_schema, share::schema::ObTableSchema& new_table_schema,
    common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  UNUSED(orig_table_schema);
  UNUSED(new_table_schema);
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();

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
    } else if (OB_FAIL(schema_service->get_table_sql_service().update_foreign_key(trans, inc_table_schema))) {
      LOG_WARN("update foreign keys enable option into inner table failed", K(ret));
    } else if (OB_FAIL(sync_version_for_cascade_table(inc_table_schema.get_depend_table_ids(), trans))) {
      LOG_WARN("fail to sync cascade depend table", K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::modify_check_constraints_state(const ObTableSchema& orig_table_schema,
    const ObTableSchema& inc_table_schema, ObTableSchema& new_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  UNUSED(orig_table_schema);
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
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
    if (OB_FAIL(
            schema_service->get_table_sql_service().update_check_constraint_state(trans, new_table_schema, **iter))) {
      LOG_WARN("insert single constraint failed", K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::add_table_constraints(const ObTableSchema& orig_table_schema, const ObTableSchema& inc_table_schema,
    ObTableSchema& new_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  UNUSED(orig_table_schema);
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  }
  for (ObTableSchema::const_constraint_iterator iter = inc_table_schema.constraint_begin();
       OB_SUCC(ret) && iter != inc_table_schema.constraint_end();
       iter++) {
    const int64_t constraint_id = new_table_schema.get_max_used_constraint_id() + 1;
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else {
      (*iter)->set_schema_version(new_schema_version);
      (*iter)->set_tenant_id(orig_table_schema.get_tenant_id());
      (*iter)->set_table_id(orig_table_schema.get_table_id());
      (*iter)->set_constraint_id(constraint_id);
      (*iter)->set_constraint_type(CONSTRAINT_TYPE_CHECK);
      if (OB_FAIL(schema_service->get_table_sql_service().insert_single_constraint(trans, new_table_schema, **iter))) {
        LOG_WARN("insert single constraint failed", K(ret));
      } else {
        new_table_schema.set_max_used_constraint_id(constraint_id);
      }
    }
  }
  return ret;
}

int ObDDLOperator::add_table_partitions(const ObTableSchema& orig_table_schema, const ObTableSchema& inc_table_schema,
    ObTableSchema& new_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().add_inc_part_info(
                 trans, orig_table_schema, inc_table_schema, new_schema_version, false))) {
    LOG_WARN("add inc part info failed", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    const int64_t part_num = orig_table_schema.get_part_option().get_part_num();
    const int64_t inc_part_num = inc_table_schema.get_partition_num();
    const int64_t all_part_num = part_num + inc_part_num;
    new_table_schema.get_part_option().set_part_num(all_part_num);
    new_table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_table_sql_service().update_partition_option(trans, new_table_schema))) {
      LOG_WARN("update partition option failed", K(ret), K(part_num), K(inc_part_num));
    }
  }
  return ret;
}

int ObDDLOperator::add_table_subpartitions(const ObTableSchema& orig_table_schema, ObTableSchema& inc_table_schema,
    ObTableSchema& new_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObArray<ObPartition*> update_part_array;
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
        KR(ret),
        K(update_part_array.count()),
        K(inc_table_schema.get_partition_num()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < update_part_array.count(); i++) {
      ObPartition* part = update_part_array.at(i);
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
        inc_table_schema.get_part_array()[i]->set_max_used_sub_part_id(part->get_max_used_sub_part_id());
        const int64_t subpart_num = part->get_subpartition_num();
        const int64_t inc_subpart_num = inc_table_schema.get_part_array()[i]->get_subpartition_num();
        part->set_sub_part_num(subpart_num + inc_subpart_num);
        part->set_schema_version(new_schema_version);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_service->get_table_sql_service().add_inc_subpart_info(
              trans, orig_table_schema, inc_table_schema, new_schema_version))) {
        LOG_WARN("add inc part info failed", K(ret));
      }
      new_table_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service->get_table_sql_service().update_subpartition_option(
              trans, new_table_schema, update_part_array))) {
        LOG_WARN("update sub partition option failed");
      }
    }
  }
  return ret;
}

int ObDDLOperator::truncate_table_partitions(const share::schema::ObTableSchema& orig_table_schema,
    share::schema::ObTableSchema& inc_table_schema, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  bool is_delay_delete = false;
  ObTableSchema tmp_inc_table_schema;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", KR(ret));
  } else if (OB_FAIL(check_is_delay_delete(tenant_id, is_delay_delete))) {
    LOG_WARN("check is delay delete failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().truncate_part_info(
                 trans, orig_table_schema, inc_table_schema, new_schema_version, is_delay_delete))) {
    LOG_WARN("delete inc part info failed", KR(ret));
  }

  return ret;
}

int ObDDLOperator::truncate_table_subpartitions(const share::schema::ObTableSchema& orig_table_schema,
    share::schema::ObTableSchema& inc_table_schema, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  bool is_delay_delete = false;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", KR(ret));
  } else if (OB_FAIL(check_is_delay_delete(tenant_id, is_delay_delete))) {
    LOG_WARN("check is delay delete failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service->get_table_sql_service().truncate_subpart_info(
                 trans, orig_table_schema, inc_table_schema, new_schema_version, is_delay_delete))) {
    LOG_WARN("delete inc part info failed", KR(ret));
  }

  return ret;
}

int ObDDLOperator::add_table_partitions(const ObTableSchema& orig_table_schema, const ObTableSchema& alter_table_schema,
    const int64_t schema_version, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(schema_version));
  } else {
    const int64_t inc_part_num = alter_table_schema.get_part_option().get_part_num();
    if (OB_FAIL(schema_service->get_table_sql_service().add_inc_part_info(
            trans, orig_table_schema, alter_table_schema, schema_version, false))) {
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
      } else if (OB_FAIL(schema_service->get_table_sql_service().update_partition_option(
                     trans, new_table_schema, schema_version))) {
        LOG_WARN("update partition option failed", K(ret), K(part_num), K(inc_part_num));
      }
    }
  }
  return ret;
}

int ObDDLOperator::add_tablegroup_partitions(const ObTablegroupSchema& orig_tablegroup_schema,
    const ObTablegroupSchema& inc_tablegroup_schema, const int64_t new_schema_version,
    ObTablegroupSchema& new_tablegroup_schema, ObMySQLTransaction& trans, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (new_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(new_schema_version));
  } else {
    const int64_t inc_part_num = inc_tablegroup_schema.get_part_option().get_part_num();
    if (OB_FAIL(schema_service->get_tablegroup_sql_service().add_inc_part_info(
            trans, orig_tablegroup_schema, inc_tablegroup_schema, new_schema_version))) {
      LOG_WARN("add inc part info failed", K(ret));
    } else if (OB_FAIL(new_tablegroup_schema.assign(orig_tablegroup_schema))) {
      LOG_WARN("failed to assign schema", K(ret), K(orig_tablegroup_schema));
    } else {
      const int64_t part_num = orig_tablegroup_schema.get_part_option().get_part_num();
      const int64_t all_part_num = part_num + inc_part_num;
      new_tablegroup_schema.get_part_option().set_part_num(all_part_num);
      new_tablegroup_schema.set_schema_version(new_schema_version);
      ObSchemaOperationType opt_type = OB_DDL_ALTER_TABLEGROUP_PARTITION;
      if (OB_FAIL(schema_service->get_tablegroup_sql_service().update_partition_option(
              trans, new_tablegroup_schema, opt_type, ddl_stmt_str))) {
        LOG_WARN("update partition option failed", K(ret), K(part_num), K(inc_part_num));
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_partition_cnt_within_partition_table(
    ObTableSchema& new_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  const int64_t partition_cnt_within_partition_table =
      new_table_schema.get_part_option().get_partition_cnt_within_partition_table();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (-1 == partition_cnt_within_partition_table) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parttion cnt", K(ret), K(partition_cnt_within_partition_table));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    new_table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_table_sql_service().update_partition_cnt_within_partition_table(
            trans, new_table_schema))) {
      LOG_WARN("update partition_cnt failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::get_part_array_from_table(const ObTableSchema& new_table_schema,
    const ObTableSchema& inc_table_schema, ObIArray<ObPartition*>& out_part_array)
{
  int ret = OB_SUCCESS;

  ObPartition** inc_part_array = inc_table_schema.get_part_array();
  const int64_t inc_part_sum = inc_table_schema.get_partition_num();
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_sum; i++) {
    ObPartition* inc_part = inc_part_array[i];
    if (OB_ISNULL(inc_part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inc_part_array[i] is null", KR(ret), K(i));
    } else {
      ObPartition** part_array = new_table_schema.get_part_array();
      const int64_t part_sum = new_table_schema.get_partition_num();
      int64_t j = 0;
      for (j = 0; OB_SUCC(ret) && j < part_sum; j++) {
        ObPartition* part = part_array[j];
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
        LOG_WARN("inc_part_array[i] is not in part_array", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_max_used_part_id(const bool is_add, const ObTableSchema& orig_table_schema,
    const ObTableSchema& inc_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;

  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else {
    ObTableSchema new_table_schema;
    if (OB_FAIL(new_table_schema.assign(orig_table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else {
      const int64_t inc_part_num = inc_table_schema.get_partition_num();
      int64_t max_used_part_id = new_table_schema.get_part_option().get_max_used_part_id();
      bool need_update = is_add || -1 == max_used_part_id;
      if (-1 == max_used_part_id) {
        max_used_part_id = new_table_schema.get_part_option().get_part_num() - 1;
      }
      if (need_update) {
        if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else {
          int64_t new_max_used_part_id = is_add ? max_used_part_id + inc_part_num : max_used_part_id;
          new_table_schema.get_part_option().set_max_used_part_id(new_max_used_part_id);
          new_table_schema.set_schema_version(new_schema_version);
          if (OB_FAIL(schema_service->get_table_sql_service().update_max_used_part_id(trans, new_table_schema))) {
            LOG_WARN("update partition option failed",
                K(ret),
                K(max_used_part_id),
                K(new_max_used_part_id),
                K(inc_part_num));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_max_used_subpart_id(const bool is_add, const ObTableSchema& orig_table_schema,
    const ObTableSchema& inc_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;

  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else {
    ObTableSchema new_table_schema;
    if (OB_FAIL(new_table_schema.assign(orig_table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else {
      ObArray<ObPartition*> update_part_array;

      if (OB_FAIL(get_part_array_from_table(new_table_schema, inc_table_schema, update_part_array))) {
        LOG_WARN("fail to get part array from tableschema", KR(ret));
      } else if (update_part_array.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update_part_array count is not more than 0", KR(ret), K(update_part_array.count()));
      } else if (update_part_array.count() != inc_table_schema.get_partition_num()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update_part_array count not equal inc_table part count",
            KR(ret),
            K(update_part_array.count()),
            K(inc_table_schema.get_partition_num()));
      } else {
        if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < update_part_array.count(); i++) {
          ObPartition* part = update_part_array.at(i);
          if (OB_ISNULL(part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("update_part_array[i]", KR(ret), K(i));
          } else if (OB_ISNULL(inc_table_schema.get_part_array()[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("inc_table_part_array[i]", KR(ret), K(i));
          } else {
            int64_t max_used_subpart_id = part->get_max_used_sub_part_id();
            const int64_t inc_subpart_num = inc_table_schema.get_part_array()[i]->get_subpartition_num();
            if (is_add) {
              int64_t new_max_used_subpart_id = is_add ? max_used_subpart_id + inc_subpart_num : max_used_subpart_id;
              if (new_max_used_subpart_id > OB_MAX_PART_ID) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("new max used part id larger than max part id", K(ret));
              } else {
                part->set_max_used_sub_part_id(new_max_used_subpart_id);
                part->set_schema_version(new_schema_version);
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        new_table_schema.set_schema_version(new_schema_version);
        if (OB_FAIL(schema_service->get_table_sql_service().update_subpartition_option(
                trans, new_table_schema, update_part_array))) {
          LOG_WARN("update sub partition option failed", K(ret));
        } else if (OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(
                       trans, orig_table_schema.get_table_id()))) {
          LOG_WARN("update table schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::add_dropped_part_to_partition_schema(
    const ObIArray<int64_t>& part_ids, const ObPartitionSchema& orig_part_schema, ObPartitionSchema& alter_part_schema)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
    ObPartition part;
    part.set_part_id(part_ids.at(i));
    if (OB_FAIL(alter_part_schema.add_partition(part))) {
      LOG_WARN("add partition failed!", K(ret), K(part));
    }
  }
  if (OB_SUCC(ret)) {
    alter_part_schema.set_part_level(orig_part_schema.get_part_level());
    if (OB_FAIL(alter_part_schema.get_part_option().assign(orig_part_schema.get_part_option()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to assign part_option", K(ret));
    } else {
      alter_part_schema.get_part_option().set_part_num(alter_part_schema.get_partition_num());
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t inc_part_num = alter_part_schema.get_part_option().get_part_num();
    ObPartition** inc_part_array = alter_part_schema.get_part_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
      ObPartition* inc_part = inc_part_array[i];
      ObDroppedPartIterator iter(orig_part_schema);
      const ObPartition* part = NULL;
      while (OB_SUCC(ret) && OB_SUCC(iter.next(part))) {
        if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(part), K(ret));
        } else if (inc_part->get_part_id() == part->get_part_id()) {
          if (OB_FAIL(inc_part->assign(*part))) {
            LOG_WARN("failed to assign partition", K(ret), K(part), K(inc_part));
          }
          break;
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("part should exists", K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::add_dropped_subpart_to_partition_schema(
    const ObIArray<int64_t>& subpartition_ids,  // subpartition_ids is order
    const ObPartitionSchema& orig_part_schema, ObPartitionSchema& alter_part_schema)
{
  int ret = OB_SUCCESS;
  if (subpartition_ids.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpartition_ids count is less than 0", K(ret));
  } else if (subpartition_ids.count() > 0) {
    ObPartition part;
    part.set_part_id(extract_part_idx(subpartition_ids.at(0)));
    for (int64_t i = 0; OB_SUCC(ret) && i < subpartition_ids.count(); ++i) {
      ObSubPartition subpart;
      int64_t part_id = extract_part_idx(subpartition_ids.at(i));
      int64_t subpart_id = extract_subpart_idx(subpartition_ids.at(i));
      if (part.get_part_id() != part_id) {
        if (OB_FAIL(alter_part_schema.add_partition(part))) {
          LOG_WARN("add partition failed!", K(ret), K(part));
        }
        part.reset();
        part.set_part_id(part_id);
      }
      subpart.set_sub_part_id(subpart_id);
      if (OB_SUCC(ret) && OB_FAIL(part.add_partition(subpart))) {
        LOG_WARN("add subpartition failed!", K(ret), K(part), K(subpart));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(alter_part_schema.add_partition(part))) {
      LOG_WARN("add partition failed!", K(ret), K(part));
    }
  }

  if (OB_SUCC(ret)) {
    alter_part_schema.set_part_level(orig_part_schema.get_part_level());
    if (OB_FAIL(alter_part_schema.get_part_option().assign(orig_part_schema.get_part_option()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to assign part_option", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t inc_part_num = alter_part_schema.get_partition_num();
    ObPartition** inc_part_array = alter_part_schema.get_part_array();
    const int64_t part_num = orig_part_schema.get_partition_num();
    ObPartition** part_array = orig_part_schema.get_part_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
      int64_t j = 0, l = 0;
      ObPartition* inc_part = inc_part_array[i];
      for (j = 0; OB_SUCC(ret) && j < part_num; j++) {
        const ObPartition* part = part_array[j];
        if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part_array[j] is null", K(part), K(j), K(ret));
        } else if (OB_ISNULL(inc_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inc_part_array[i] is null", K(inc_part), K(i), K(ret));
        } else if (inc_part->get_part_id() == part->get_part_id()) {
          const int64_t inc_subpart_num = inc_part->get_subpartition_num();
          ObSubPartition** inc_subpart_array = inc_part->get_subpart_array();
          const int64_t subpart_num = part->get_dropped_subpartition_num();
          ObSubPartition** subpart_array = part->get_dropped_subpart_array();
          for (int64_t k = 0; OB_SUCC(ret) && k < inc_subpart_num; k++) {
            ObSubPartition* inc_subpart = inc_subpart_array[k];
            for (l = 0; OB_SUCC(ret) && l < subpart_num; l++) {
              const ObSubPartition* subpart = subpart_array[l];
              if (OB_ISNULL(subpart)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("subpart_array[l] is null", K(part), K(l), K(ret));
              } else if (OB_ISNULL(inc_subpart)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("inc_subpart_array[k] is null", K(part), K(inc_subpart_array[k]), K(ret));
              } else if (inc_subpart->get_sub_part_id() == subpart->get_sub_part_id()) {
                if (OB_FAIL(inc_subpart->assign(*subpart))) {
                  LOG_WARN("failed to assign partition", K(ret), K(part), K(inc_part));
                }
                break;
              }
            }
            if (OB_SUCC(ret) && subpart_num == l) {
              ret = OB_PARTITION_NOT_EXIST;
              LOG_WARN("subpart should exists", K(ret));
            }
          }
          break;
        }
      }

      if (OB_SUCC(ret) && part_num == j) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("part should exists", K(ret));
      }
    }
  }
  return ret;
}

// This function is used to filter the partitions that cannot be force drop alone when the object type to be deleted by
// inspection is a partition on the table.
int ObDDLOperator::filter_part_id_to_be_dropped(share::schema::ObSchemaGetterGuard& schema_guard,
    const ObIArray<int64_t>& partition_ids, ObIArray<int64_t>& part_ids_for_delay_delete_part,
    const share::schema::ObTableSchema& orig_table_schema)
{
  int ret = OB_SUCCESS;

  // Filter the parts in the dropped part array in the tablegroup, and delete them together when the part of the
  // tablegroup is deleted
  ObTableSchema inc_table_schema;
  const uint64_t tablegroup_id = orig_table_schema.get_tablegroup_id();
  const ObTablegroupSchema* tablegroup_schema = NULL;
  if (OB_INVALID_ID == tablegroup_id || !orig_table_schema.get_binding()) {
    part_ids_for_delay_delete_part.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
      if (OB_FAIL(part_ids_for_delay_delete_part.push_back(partition_ids.at(i)))) {
        LOG_WARN("push back failed", K(ret), K(partition_ids.at(i)));
      }
    }
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
    LOG_WARN("get tablegroup schema failed", K(ret), K(tablegroup_id));
  } else if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp_tablegroup_schema is null", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(add_dropped_part_to_partition_schema(partition_ids, orig_table_schema, inc_table_schema))) {
    LOG_WARN("fail to add dropped part to partition schema", K(ret));
  } else {
    const int64_t part_num = inc_table_schema.get_part_option().get_part_num();
    ObPartition** part_array = inc_table_schema.get_part_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
      ObPartition* alter_part = part_array[i];
      bool found = false;
      ObDroppedPartIterator iter(*tablegroup_schema);
      const ObPartition* tablegroup_part = NULL;
      while (OB_SUCC(ret) && !found && OB_SUCC(iter.next(tablegroup_part))) {
        if (OB_FAIL(check_part_equal(
                orig_table_schema.get_part_option().get_part_func_type(), tablegroup_part, alter_part, found))) {
          LOG_WARN("check_part_equal failed", K(ret));
        }
      }                          // end while
      if (OB_ITER_END == ret) {  // !found
        ret = OB_SUCCESS;
        if (OB_FAIL(part_ids_for_delay_delete_part.push_back(alter_part->get_part_id()))) {
          LOG_WARN("push back to part_ids failed", K(ret), K(alter_part->get_part_id()));
        }
      }
    }  // end for
  }

  return ret;
}

int ObDDLOperator::force_drop_table_and_partitions(const uint64_t table_id,
    share::schema::ObSchemaGetterGuard& schema_guard, const common::ObIArray<int64_t>& partition_ids,
    const common::ObIArray<int64_t>& subpartition_ids, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* orig_table_schema = NULL;
  bool is_only_delete_part = true;
  ObSArray<int64_t> part_ids_for_delay_delete_part;
  if (partition_ids.count() > 0 || subpartition_ids.count() > 0) {
    // partition is delay deleted, but table not.
    is_only_delete_part = true;
  } else {
    // table is deplay deleted.
    is_only_delete_part = false;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, orig_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(table_id));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_table_schema is null", K(ret), K(table_id));
  } else if (is_only_delete_part) {
    if (partition_ids.count() > 0) {
      ObTableSchema alter_table_schema;
      if (OB_FAIL(add_dropped_part_to_partition_schema(partition_ids, *orig_table_schema, alter_table_schema))) {
        LOG_WARN("fail to add dropped part to partition schema", K(ret));
      } else if (OB_FAIL(drop_table_partitions_for_inspection(*orig_table_schema, alter_table_schema, trans))) {
        LOG_WARN("failed to drop table partitions for inspection", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (subpartition_ids.count() > 0) {
      ObTableSchema alter_table_schema;
      if (OB_FAIL(add_dropped_subpart_to_partition_schema(subpartition_ids, *orig_table_schema, alter_table_schema))) {
        LOG_WARN("fail to add dropped subpart to partition schema", K(ret));
      } else if (OB_FAIL(drop_table_subpartitions_for_inspection(*orig_table_schema, alter_table_schema, trans))) {
        LOG_WARN("failed to drop table partitions for inspection", K(ret));
      }
    }
  } else {  // !is_only_delete_part
    if (OB_FAIL(drop_table_for_inspection(*orig_table_schema, trans))) {
      LOG_WARN("failed to drop table for inspection", K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::force_drop_database(
    const uint64_t db_id, share::schema::ObSchemaGetterGuard& schema_guard, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema* orig_database_schema = NULL;
  if (OB_FAIL(schema_guard.get_database_schema(db_id, orig_database_schema))) {
    LOG_WARN("get database schema failed", K(ret), K(db_id));
  } else if (OB_ISNULL(orig_database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_database_schema is null", K(ret), K(db_id));
  } else if (OB_FAIL(drop_database_for_inspection(schema_guard, *orig_database_schema, trans))) {
    LOG_WARN("failed to drop db for inspection", K(ret));
  }
  return ret;
}

bool ObDDLOperator::is_list_values_equal(
    const common::ObRowkey& fir_values, const common::ObIArray<common::ObNewRow>& sed_values)
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

int ObDDLOperator::check_part_equal(const share::schema::ObPartitionFuncType part_type,
    const share::schema::ObPartition* r_part, const share::schema::ObPartition* l_part, bool& is_equal)
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
      rowkey.assign(r_part->get_list_row_values().at(0).cells_, r_part->get_list_row_values().at(0).count_);
      if (is_list_values_equal(rowkey, l_part->get_list_row_values())) {
        is_equal = true;
      }
    }
  }

  return ret;
}

int ObDDLOperator::drop_tablegroup_for_inspection(
    const ObTablegroupSchema& tablegroup_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_tablegroup_sql_service().drop_tablegroup_for_inspection(
                 tablegroup_schema, new_schema_version, trans))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tablegroup_schema));
  }

  return ret;
}

int ObDDLOperator::drop_database_for_inspection(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObDatabaseSchema& orig_database_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_database_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  const uint64_t db_id = orig_database_schema.get_database_id();
  ObArray<uint64_t> table_ids;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_database(tenant_id, db_id, table_ids))) {
    LOG_WARN("get tables in database failed", K(ret), K(tenant_id), K(db_id));
  } else if (table_ids.count() > 0) {
    // In order to avoid db being deleted, the table in db is still exist, it need to wait for the table to be deleted
    // first
    ret = OB_EAGAIN;
    LOG_WARN("should delete table schema first", K(ret), K(db_id));
  } else if (OB_FAIL(schema_service->get_database_sql_service().drop_database_for_inspection(
                 trans, orig_database_schema, new_schema_version))) {
    LOG_WARN("drop db for inspection failed", K(ret));
  }

  return ret;
}

int ObDDLOperator::drop_table_for_inspection(const ObTableSchema& orig_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  const uint64_t table_id = orig_table_schema.get_table_id();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (orig_table_schema.get_index_tid_count() > 0) {
    // When the inspection is doing delayed deletion according to table_id, in order to avoid the data table being
    // deleted and the index table is still there, it is necessary to wait for the index table to be deleted.
    ret = OB_EAGAIN;
    LOG_WARN("should delete index schema first", K(ret), K(table_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().drop_table_for_inspection(
                 trans, orig_table_schema, new_schema_version))) {
    LOG_WARN("drop table for inspection failed", K(ret));
  } else if (orig_table_schema.is_in_recyclebin()) {
    ObRecycleObject::RecycleObjType recycle_type = ObRecycleObject::get_type_by_table_schema(orig_table_schema);
    const ObRecycleObject* recycle_obj = NULL;
    ObArray<ObRecycleObject> recycle_objs;
    if (NULL == recycle_obj) {
      if (OB_FAIL(schema_service->fetch_recycle_object(orig_table_schema.get_tenant_id(),
              orig_table_schema.get_table_name_str(),
              recycle_type,
              trans,
              recycle_objs))) {
        LOG_WARN("get_recycle_object failed", K(ret), K(recycle_type));
      } else if (recycle_objs.size() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected recycle object num", K(ret), K(recycle_objs.size()));
      } else {
        recycle_obj = &recycle_objs.at(0);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_service->delete_recycle_object(orig_table_schema.get_tenant_id(), *recycle_obj, trans))) {
        LOG_WARN("delete_recycle_object failed", K(ret), K(*recycle_obj));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if ((orig_table_schema.is_index_table() || orig_table_schema.is_materialized_view()) &&
             OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(
                 trans, orig_table_schema.get_data_table_id()))) {
    LOG_WARN("update data_table schema version failed", K(ret), K(orig_table_schema));
  }

  return ret;
}

int ObDDLOperator::drop_table_partitions_for_inspection(
    const ObTableSchema& orig_table_schema, const ObTableSchema& inc_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().drop_part_info_for_inspection(
                 trans, orig_table_schema, inc_table_schema, new_schema_version))) {
    LOG_WARN("delete inc part info failed", K(ret));
  } else {
    if (OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(
            trans, orig_table_schema.get_table_id()))) {
      LOG_WARN("update partition option failed", K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::drop_table_subpartitions_for_inspection(
    const ObTableSchema& orig_table_schema, const ObTableSchema& inc_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().drop_subpart_info_for_inspection(
                 trans, orig_table_schema, inc_table_schema, new_schema_version))) {
    LOG_WARN("delete inc part info failed", K(ret));
  } else {
    if (OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(
            trans, orig_table_schema.get_table_id()))) {
      LOG_WARN("update partition option failed", K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::drop_table_partitions(const ObTableSchema& orig_table_schema, ObTableSchema& inc_table_schema,
    ObTableSchema& new_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  bool is_delay_delete = false;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_is_delay_delete(orig_table_schema.get_tenant_id(), is_delay_delete))) {
    LOG_WARN("failed to check is delay delete", K(ret));
  } else if (is_delay_delete) {
    const int64_t inc_part_num = inc_table_schema.get_partition_num();
    ObPartition** part_array = inc_table_schema.get_part_array();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), K(inc_table_schema));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
        if (OB_ISNULL(part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part array is null", K(ret), K(i), K(inc_table_schema));
        } else {
          ObString delay_deleted_table_name;
          if (OB_FAIL(create_new_name_for_delay_delete_table_partition(
                  delay_deleted_table_name, &orig_table_schema, allocator))) {
            LOG_WARN("fail to create new name for delay delete", K(ret));
          } else if (OB_FAIL(part_array[i]->set_part_name(delay_deleted_table_name))) {
            LOG_WARN("set part name failed", K(ret), K(delay_deleted_table_name));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service->get_table_sql_service().drop_inc_part_info(
                 trans, orig_table_schema, inc_table_schema, new_schema_version, is_delay_delete, false))) {
    LOG_WARN("delete inc part info failed", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    const int64_t part_num = orig_table_schema.get_part_option().get_part_num();
    const int64_t inc_part_num = inc_table_schema.get_partition_num();
    const int64_t all_part_num = part_num - inc_part_num;
    new_table_schema.get_part_option().set_part_num(all_part_num);
    new_table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_table_sql_service().update_partition_option(trans, new_table_schema))) {
      LOG_WARN("update partition option failed", K(ret), K(part_num), K(inc_part_num));
    }
  }
  return ret;
}

int ObDDLOperator::drop_table_subpartitions(const ObTableSchema& orig_table_schema, ObTableSchema& inc_table_schema,
    ObTableSchema& new_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  bool is_delay_delete = false;
  ObArenaAllocator allocator("DropSubPart");

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_is_delay_delete(orig_table_schema.get_tenant_id(), is_delay_delete))) {
    LOG_WARN("failed to check is delay delete", K(ret));
  } else if (OB_FAIL(schema_service->get_table_sql_service().drop_inc_subpart_info(
                 trans, orig_table_schema, inc_table_schema, new_schema_version, is_delay_delete))) {
    LOG_WARN("delete inc part info failed", K(ret));
  } else {
    ObArray<ObPartition*> update_part_array;
    if (OB_FAIL(get_part_array_from_table(new_table_schema, inc_table_schema, update_part_array))) {
      LOG_WARN("fail to get part array from tableschema", KR(ret));
    } else if (update_part_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update_part_array count less than 0", K(ret), K(update_part_array.count()));
    } else if (update_part_array.count() != inc_table_schema.get_partition_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update_part_array count not equal inc_table part count",
          KR(ret),
          K(update_part_array.count()),
          K(inc_table_schema.get_partition_num()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < update_part_array.count(); i++) {
        ObPartition* part = update_part_array.at(i);
        if (i >= inc_table_schema.get_partition_num()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "update_part_array[i] out of inc_part_array", KR(ret), K(i), K(inc_table_schema.get_partition_num()));
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
        if (OB_FAIL(schema_service->get_table_sql_service().update_subpartition_option(
                trans, new_table_schema, update_part_array))) {
          LOG_WARN("update sub partition option failed");
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_table_constraints(const ObTableSchema& orig_table_schema, const ObTableSchema& inc_table_schema,
    ObTableSchema& new_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  UNUSED(orig_table_schema);

  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else {
    for (ObTableSchema::const_constraint_iterator iter = inc_table_schema.constraint_begin();
         OB_SUCC(ret) && iter != inc_table_schema.constraint_end();
         iter++) {
      (*iter)->set_tenant_id(orig_table_schema.get_tenant_id());
      (*iter)->set_table_id(orig_table_schema.get_table_id());
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_service->get_table_sql_service().delete_single_constraint(
                     new_schema_version, trans, new_table_schema, **iter))) {
        RS_LOG(WARN, "failed to delete constraint", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_table_partitions(const ObTableSchema& orig_table_schema, ObTableSchema& inc_table_schema,
    const int64_t schema_version, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  bool is_delay_delete = false;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(schema_version));
  } else if (OB_FAIL(check_is_delay_delete(orig_table_schema.get_tenant_id(), is_delay_delete))) {
    LOG_WARN("failed to check is delay delete", K(ret));
  } else if (is_delay_delete) {
    const ObPartitionOption& part_expr = inc_table_schema.get_part_option();
    ObPartition** part_array = inc_table_schema.get_part_array();
    const int64_t inc_part_num = part_expr.get_part_num();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), K(inc_table_schema));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
        if (OB_ISNULL(part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part array is null", K(ret), K(i), K(inc_table_schema));
        } else {
          ObString delay_deleted_table_name;
          if (OB_FAIL(create_new_name_for_delay_delete_table_partition(
                  delay_deleted_table_name, &orig_table_schema, allocator))) {
            LOG_WARN("fail to create new name for delay delete", K(ret));
          } else if (OB_FAIL(part_array[i]->set_part_name(delay_deleted_table_name))) {
            LOG_WARN("set part name failed", K(ret), K(delay_deleted_table_name));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service->get_table_sql_service().drop_inc_part_info(
                 trans, orig_table_schema, inc_table_schema, schema_version, is_delay_delete, false))) {
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
      // update max_used_part_id
      int64_t max_used_part_id = orig_table_schema.get_part_option().get_max_used_part_id();
      if (-1 == max_used_part_id) {
        max_used_part_id = orig_table_schema.get_part_option().get_part_num() - 1;
      }
      new_table_schema.get_part_option().set_max_used_part_id(max_used_part_id);
      if (OB_FAIL(schema_service->get_table_sql_service().update_partition_option(
              trans, new_table_schema, schema_version))) {
        LOG_WARN("update partition option failed", K(ret), K(part_num), K(inc_part_num));
      }
    }
  }
  return ret;
}

int ObDDLOperator::drop_tablegroup_partitions_for_inspection(const ObTablegroupSchema& orig_tablegroup_schema,
    const ObTablegroupSchema& inc_tablegroup_schema, ObTablegroupSchema& new_tablegroup_schema,
    ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_tablegroup_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_tablegroup_sql_service().drop_part_info_for_inspection(
                 trans, orig_tablegroup_schema, inc_tablegroup_schema, new_schema_version))) {
    LOG_WARN("delete inc part info failed", K(ret));
  } else {
    new_tablegroup_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_tablegroup_sql_service().update_tablegroup_schema_version(
            trans, new_tablegroup_schema))) {
      LOG_WARN("update partition option failed", K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::drop_tablegroup_partitions(const ObTablegroupSchema& orig_tablegroup_schema,
    ObTablegroupSchema& inc_tablegroup_schema, const int64_t new_schema_version,
    ObTablegroupSchema& new_tablegroup_schema, ObMySQLTransaction& trans, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  bool is_delay_delete = false;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (new_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(new_schema_version));
  } else if (!is_new_tablegroup_id(orig_tablegroup_schema.get_tablegroup_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't drop partition while tablegroup is created before ver 2.0", K(ret), K(orig_tablegroup_schema));
  } else if (OB_FAIL(check_is_delay_delete(inc_tablegroup_schema.get_tenant_id(), is_delay_delete))) {
    LOG_WARN("failed to check is delay delete", K(ret));
  } else if (is_delay_delete) {
    const ObPartitionOption& part_expr = inc_tablegroup_schema.get_part_option();
    ObPartition** part_array = inc_tablegroup_schema.get_part_array();
    const int64_t inc_part_num = part_expr.get_part_num();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), K(inc_tablegroup_schema));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
        if (OB_ISNULL(part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part array is null", K(ret), K(i), K(inc_tablegroup_schema));
        } else {
          ObString delay_deleted_table_name;
          if (OB_FAIL(create_new_name_for_delay_delete_tablegroup_partition(
                  delay_deleted_table_name, &orig_tablegroup_schema, allocator))) {
            LOG_WARN("fail to create new name for delay delete", K(ret));
          } else if (OB_FAIL(part_array[i]->set_part_name(delay_deleted_table_name))) {
            LOG_WARN("set part name failed", K(ret), K(delay_deleted_table_name));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service->get_tablegroup_sql_service().drop_inc_part_info(
                 trans, orig_tablegroup_schema, inc_tablegroup_schema, new_schema_version, is_delay_delete))) {
    LOG_WARN("delete inc part info failed", K(ret));
  } else {
    new_tablegroup_schema = orig_tablegroup_schema;
    const int64_t part_num = orig_tablegroup_schema.get_part_option().get_part_num();
    const int64_t inc_part_num = inc_tablegroup_schema.get_part_option().get_part_num();
    const int64_t all_part_num = part_num - inc_part_num;
    new_tablegroup_schema.get_part_option().set_part_num(all_part_num);
    // update max_used_part_id
    int64_t max_used_part_id = orig_tablegroup_schema.get_part_option().get_max_used_part_id();
    if (-1 == max_used_part_id) {
      max_used_part_id = orig_tablegroup_schema.get_part_option().get_part_num() - 1;
    }
    new_tablegroup_schema.get_part_option().set_max_used_part_id(max_used_part_id);
    new_tablegroup_schema.set_schema_version(new_schema_version);
    ObSchemaOperationType opt_type = OB_DDL_ALTER_TABLEGROUP_PARTITION;
    if (is_delay_delete) {
      opt_type = OB_DDL_DELAY_DELETE_TABLEGROUP_PARTITION;
    }
    if (OB_FAIL(schema_service->get_tablegroup_sql_service().update_partition_option(
            trans, new_tablegroup_schema, opt_type, ddl_stmt_str))) {
      LOG_WARN("update partition option failed", K(ret), K(part_num), K(inc_part_num));
    }
  }
  return ret;
}

int ObDDLOperator::insert_single_column(
    ObMySQLTransaction& trans, const ObTableSchema& new_table_schema, ObColumnSchemaV2& new_column)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (FALSE_IT(new_column.set_schema_version(new_schema_version))) {
    // do nothing
  } else if (OB_FAIL(
                 schema_service->get_table_sql_service().insert_single_column(trans, new_table_schema, new_column))) {
    LOG_WARN("insert single column failed", K(ret));
  }
  return ret;
}

int ObDDLOperator::delete_single_column(
    ObMySQLTransaction& trans, ObTableSchema& new_table_schema, const ObString& column_name)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObColumnSchemaV2* orig_column = NULL;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_ISNULL(orig_column = new_table_schema.get_column_schema(column_name))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get column schema from table failed", K(column_name));
  } else if (OB_FAIL(new_table_schema.delete_column(column_name))) {
    // drop column will do some check on the new table schema
    RS_LOG(WARN, "failed to drop column schema", K(ret), K(column_name));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().delete_single_column(
                 new_schema_version, trans, new_table_schema, *orig_column))) {
    RS_LOG(WARN, "failed to delete column", K(orig_column), K(ret));
  }
  return ret;
}

int ObDDLOperator::alter_table_create_index(const ObTableSchema& new_table_schema, const int64_t frozen_version,
    ObIArray<ObColumnSchemaV2*>& gen_columns, ObTableSchema& index_schema, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else {
    uint64_t index_table_id = OB_INVALID_ID;
    // index schema can't not create with specified table id
    if (OB_UNLIKELY(index_schema.get_table_id() != OB_INVALID_ID)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_id of index should be invalid", K(ret), K(index_schema.get_table_id()));
    } else if (OB_FAIL(schema_service->fetch_new_table_id(new_table_schema.get_tenant_id(), index_table_id))) {
      RS_LOG(WARN, "failed to update_max_used_table_id, ", K(ret));
    } else {
      index_schema.set_table_id(index_table_id);
    }
    if (OB_SUCC(ret)) {
      index_schema.set_create_mem_version(frozen_version);
      if (gen_columns.empty()) {
        // create normal index table.
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
          ObColumnSchemaV2* new_column_schema = gen_columns.at(i);
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

int ObDDLOperator::alter_table_drop_index(const uint64_t data_table_id, const uint64_t database_id,
    const ObDropIndexArg& drop_index_arg, ObTableSchema& new_data_table_schema, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = extract_tenant_id(database_id);
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const uint64_t tenant_id = drop_index_arg.tenant_id_;
    RS_LOG(INFO, "start drop index", K(drop_index_arg));
    const ObTableSchema* index_table_schema = NULL;
    ObString index_table_name;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const ObString& index_name = drop_index_arg.index_name_;

    // build index name and get index schema
    if (OB_FAIL(ObTableSchema::build_index_table_name(allocator, data_table_id, index_name, index_table_name))) {
      RS_LOG(WARN, "build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
    } else {
      const bool is_index = true;
      if (OB_FAIL(
              schema_guard.get_table_schema(tenant_id, database_id, index_table_name, is_index, index_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(index_table_schema));
      } else if (NULL == index_table_schema) {
        ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
        LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, index_name.length(), index_name.ptr());
        RS_LOG(WARN, "get index table schema failed", K(tenant_id), K(database_id), K(index_table_name), K(ret));
      } else if (index_table_schema->is_in_recyclebin()) {
        ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
        LOG_WARN("index table is in recyclebin", K(ret));
      }
    }
    // drop inner generated index column
    if (OB_SUCC(ret)) {
      if (OB_FAIL(drop_inner_generated_index_column(trans, schema_guard, *index_table_schema, new_data_table_schema))) {
        LOG_WARN("drop inner generated index column failed", K(ret));
      }
    }
    // drop index table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(drop_table(*index_table_schema, trans))) {
        RS_LOG(WARN, "ddl_operator drop_table failed", "table schema", *index_table_schema, K(ret));
      }
    }
    RS_LOG(INFO, "finish drop index", K(drop_index_arg), K(ret));
  }
  return ret;
}

int ObDDLOperator::alter_table_alter_index(const uint64_t data_table_id, const uint64_t database_id,
    const ObAlterIndexArg& alter_index_arg, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = extract_tenant_id(data_table_id);
  ObSchemaService* schema_service = schema_service_.get_schema_service();
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
    const ObTableSchema* index_table_schema = NULL;
    ObString index_table_name;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const ObString& index_name = alter_index_arg.index_name_;

    // build index name and get index schema
    if (OB_FAIL(ObTableSchema::build_index_table_name(allocator, data_table_id, index_name, index_table_name))) {
      RS_LOG(WARN, "build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
    } else {
      const bool is_index = true;
      ObTableSchema new_index_table_schema;
      if (OB_FAIL(
              schema_guard.get_table_schema(tenant_id, database_id, index_table_name, is_index, index_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(index_table_schema));
      } else if (OB_UNLIKELY(NULL == index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "get index table schema failed", K(tenant_id), K(database_id), K(index_table_name), K(ret));
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
        if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(trans,
                *index_table_schema,
                new_index_table_schema,
                index_table_schema->is_global_index_table() ? OB_DDL_ALTER_GLOBAL_INDEX : OB_DDL_ALTER_TABLE))) {
          RS_LOG(WARN, "schema service update_table_options failed", K(*index_table_schema), K(ret));
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
int ObDDLOperator::alter_table_drop_foreign_key(
    const ObTableSchema& table_schema, const ObDropForeignKeyArg& drop_foreign_key_arg, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  ObTableSqlService* table_sql_service = NULL;
  const ObString& foreign_key_name = drop_foreign_key_arg.foreign_key_name_;
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (FALSE_IT(table_sql_service = &schema_service_impl->get_table_sql_service())) {
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(table_sql_service->drop_foreign_key(new_schema_version, trans, table_schema, foreign_key_name))) {
    LOG_WARN("failed to drop foreign key", K(ret), K(foreign_key_name));
  }
  return ret;
}

int ObDDLOperator::alter_index_drop_options(const ObTableSchema& index_table_schema, const ObString& table_name,
    ObTableSchema& new_index_table_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const int INVISIBLE = 1;
  const uint64_t DROPINDEX = 1;
  const uint64_t tenant_id = index_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObSchemaService* schema_service = schema_service_.get_schema_service();
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
    if (OB_FAIL(ObTableSchema::get_index_name(
            allocator, index_table_schema.get_data_table_id(), index_table_schema.get_table_name_str(), index_name))) {
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
              trans, index_table_schema, new_index_table_schema, OB_DDL_DROP_INDEX_TO_RECYCLEBIN, &ddl_stmt_str))) {
        RS_LOG(WARN, "schema service update_table_optinos failed", K(index_table_schema), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::alter_table_rename_index(const uint64_t data_table_id, const uint64_t database_id,
    const obrpc::ObRenameIndexArg& rename_index_arg, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = extract_tenant_id(data_table_id);
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const uint64_t tenant_id = rename_index_arg.tenant_id_;
    int64_t new_schema_version = OB_INVALID_VERSION;
    RS_LOG(INFO, "start alter table rename index", K(rename_index_arg));
    const ObTableSchema* index_table_schema = NULL;
    ObString index_table_name;
    ObString new_index_table_name;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const ObString& index_name = rename_index_arg.origin_index_name_;
    const ObString& new_index_name = rename_index_arg.new_index_name_;

    if (OB_FAIL(ObTableSchema::build_index_table_name(allocator, data_table_id, index_name, index_table_name))) {
      RS_LOG(WARN, "build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
    } else if (OB_FAIL(ObTableSchema::build_index_table_name(
                   allocator, data_table_id, new_index_name, new_index_table_name))) {
      RS_LOG(WARN, "build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
    } else {
      const bool is_index = true;
      if (OB_FAIL(
              schema_guard.get_table_schema(tenant_id, database_id, index_table_name, is_index, index_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(index_table_schema));
      } else if (OB_UNLIKELY(NULL == index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "get index table schema failed", K(tenant_id), K(database_id), K(index_table_name), K(ret));
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
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(new_index_table_schema.set_table_name(new_index_table_name))) {
          RS_LOG(WARN, "failed to set new table name!", K(new_index_table_schema), K(ret));
        } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(trans,
                       *index_table_schema,
                       new_index_table_schema,
                       index_table_schema->is_global_index_table() ? OB_DDL_RENAME_GLOBAL_INDEX
                                                                   : OB_DDL_RENAME_INDEX))) {
          RS_LOG(WARN, "schema service update_table_options failed", K(*index_table_schema), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::alter_index_table_parallel(const uint64_t data_table_id, const uint64_t database_id,
    const obrpc::ObAlterIndexParallelArg& alter_parallel_arg, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = extract_tenant_id(data_table_id);
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const uint64_t tenant_id = alter_parallel_arg.tenant_id_;
    int64_t new_schema_version = OB_INVALID_VERSION;
    RS_LOG(INFO, "start alter table alter index parallel", K(alter_parallel_arg));
    const ObTableSchema* index_table_schema = NULL;
    ObString index_table_name;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const ObString& index_name = alter_parallel_arg.index_name_;

    if (OB_FAIL(ObTableSchema::build_index_table_name(allocator, data_table_id, index_name, index_table_name))) {
      RS_LOG(WARN, "build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
    } else {
      const bool is_index = true;
      if (OB_FAIL(
              schema_guard.get_table_schema(tenant_id, database_id, index_table_name, is_index, index_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(index_table_schema));
      } else if (OB_UNLIKELY(NULL == index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "get index table schema failed", K(tenant_id), K(database_id), K(index_table_name), K(ret));
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
                  trans, *index_table_schema, new_index_table_schema, OB_DDL_ALTER_INDEX_PARALLEL))) {
            RS_LOG(WARN, "schema service update_table_options failed", K(*index_table_schema), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// hualong delete later
// int ObDDLOperator::log_ddl_operation(ObSchemaOperation &ddl_operation,
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

int ObDDLOperator::alter_table_options(ObSchemaGetterGuard& schema_guard, ObTableSchema& new_table_schema,
    const ObTableSchema& table_schema, const bool update_index_table, ObMySQLTransaction& trans,
    const ObIArray<ObTableSchema>* global_idx_schema_array /*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema sql service must not be null", K(schema_service), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    new_table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
            trans, table_schema, new_table_schema, OB_DDL_ALTER_TABLE))) {
      RS_LOG(WARN, "failed to alter table option!", K(ret));
    } else if (update_index_table) {
      if (OB_FAIL(update_aux_table(
              table_schema, new_table_schema, schema_guard, trans, USER_INDEX, global_idx_schema_array))) {
        RS_LOG(WARN, "failed to update_index_table!", K(ret), K(table_schema), K(new_table_schema));
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_aux_table(const ObTableSchema& table_schema, const ObTableSchema& new_table_schema,
    ObSchemaGetterGuard& schema_guard, ObMySQLTransaction& trans, const ObTableType table_type,
    const ObIArray<ObTableSchema>* global_idx_schema_array /*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  UNUSED(table_type);
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema sql service must not be null", K(schema_service), K(ret));
  } else if (OB_FAIL(new_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableSchema new_aux_table_schema;
    int64_t N = simple_index_infos.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObTableSchema* aux_table_schema = NULL;
      if (OB_NOT_NULL(global_idx_schema_array) && !global_idx_schema_array->empty()) {
        for (int64_t j = 0; OB_SUCC(ret) && j < global_idx_schema_array->count(); ++j) {
          if (simple_index_infos.at(i).table_id_ == global_idx_schema_array->at(j).get_table_id()) {
            aux_table_schema = &(global_idx_schema_array->at(j));
            break;
          }
        }
      }
      uint64_t tid = simple_index_infos.at(i).table_id_;
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(aux_table_schema) && OB_FAIL(schema_guard.get_table_schema(tid, aux_table_schema))) {
        RS_LOG(WARN, "get_table_schema failed", "table id", tid, K(ret));
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
          new_aux_table_schema.set_table_mode(new_table_schema.get_table_mode());
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
                         tenant_id, aux_table_schema->get_table_name_str(), recycle_type, trans, recycle_objs))) {
            LOG_WARN("get recycle object failed", K(tenant_id), K(ret));
          } else if (recycle_objs.size() != 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected recycle object num", K(ret), K(*aux_table_schema), "size", recycle_objs.size());
          } else if (OB_FAIL(schema_service->delete_recycle_object(tenant_id, recycle_objs.at(0), trans))) {
            LOG_WARN("delete recycle object failed", K(ret));
          } else {
            ObRecycleObject& recycle_obj = recycle_objs.at(0);
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
          new_aux_table_schema.set_schema_version(new_schema_version);
          if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                  trans, *aux_table_schema, new_aux_table_schema, OB_DDL_ALTER_TABLE))) {
            RS_LOG(WARN, "schema service update_table_options failed", K(*aux_table_schema), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::rename_table(const ObTableSchema& table_schema, const ObString& new_table_name,
    const uint64_t new_db_id, ObMySQLTransaction& trans, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema sql service must not be null", K(schema_service), K(ret));
  } else if (table_schema.has_materialized_view()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not allow rename for depend table", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    RS_LOG(WARN, "get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    bool update_index_table = false;
    ObTableSchema new_table_schema;
    if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else {
      new_table_schema.set_schema_version(new_schema_version);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(new_table_schema.set_table_name(new_table_name))) {
      RS_LOG(WARN, "failed to set new table name!", K(new_table_name), K(table_schema), K(ret));
    } else if (new_db_id != table_schema.get_database_id()) {
      update_index_table = true;
      new_table_schema.set_database_id(new_db_id);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
              trans, table_schema, new_table_schema, OB_DDL_TABLE_RENAME, ddl_stmt_str))) {
        RS_LOG(WARN, "failed to alter table option!", K(ret));
      } else if (update_index_table) {
        ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
        if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
          RS_LOG(WARN, "get_index_tid_array failed", K(ret));
        } else {
          ObTableSchema new_index_table_schema;
          for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
            const ObTableSchema* index_table_schema = NULL;
            // As the db_id may be modified, the delay deleted index will be see.
            if (OB_FAIL(schema_guard.get_table_schema(simple_index_infos.at(i).table_id_, index_table_schema))) {
              RS_LOG(WARN, "get_table_schema failed", "table id", simple_index_infos.at(i).table_id_, K(ret));
            } else if (OB_ISNULL(index_table_schema)) {
              ret = OB_ERR_UNEXPECTED;
              RS_LOG(WARN, "table schema should not be null", K(ret));
            } else {
              new_index_table_schema.reset();
              if (OB_FAIL(new_index_table_schema.assign(*index_table_schema))) {
                LOG_WARN("fail to assign schema", K(ret));
              } else {
                new_index_table_schema.set_database_id(new_table_schema.get_database_id());
                new_index_table_schema.set_tablegroup_id(new_table_schema.get_tablegroup_id());
              }
              if (OB_FAIL(ret)) {
              } else if (index_table_schema->is_in_recyclebin()) {
                const uint64_t tenant_id = index_table_schema->get_tenant_id();
                ObArray<ObRecycleObject> recycle_objs;
                ObRecycleObject::RecycleObjType recycle_type =
                    ObRecycleObject::get_type_by_table_schema(*index_table_schema);
                new_index_table_schema.set_database_id(index_table_schema->get_database_id());
                if (OB_INVALID_ID == tenant_id) {
                  ret = OB_INVALID_ARGUMENT;
                  LOG_WARN("tenant_id is invalid", K(ret));
                } else if (OB_FAIL(schema_service->fetch_recycle_object(tenant_id,
                               index_table_schema->get_table_name_str(),
                               recycle_type,
                               trans,
                               recycle_objs))) {
                  LOG_WARN("get recycle object failed", K(tenant_id), K(ret));
                } else if (recycle_objs.size() != 1) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN(
                      "unexpected recycle object num", K(ret), K(*index_table_schema), "size", recycle_objs.size());
                } else if (OB_FAIL(schema_service->delete_recycle_object(tenant_id, recycle_objs.at(0), trans))) {
                  LOG_WARN("delete recycle object failed", K(ret));
                } else {
                  ObRecycleObject& recycle_obj = recycle_objs.at(0);
                  recycle_obj.set_database_id(new_table_schema.get_database_id());
                  recycle_obj.set_tablegroup_id(new_table_schema.get_tablegroup_id());
                  if (OB_FAIL(schema_service->insert_recyclebin_object(recycle_obj, trans))) {
                    LOG_WARN("insert recyclebin object failed", K(ret));
                  }
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
                LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
              } else {
                new_index_table_schema.set_schema_version(new_schema_version);
                if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                        trans, *index_table_schema, new_index_table_schema, OB_DDL_TABLE_RENAME))) {
                  RS_LOG(WARN, "schema service update_table_options failed", K(*index_table_schema), K(ret));
                }
              }
            }
          }  // end for
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_index_status(const uint64_t data_table_id, const uint64_t index_table_id,
    const share::schema::ObIndexStatus status, const int64_t create_mem_version, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = extract_tenant_id(data_table_id);
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_table_id || status <= INDEX_STATUS_NOT_FOUND ||
      status >= INDEX_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_table_id), K(index_table_id), K(status));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service should not be NULL");
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().update_index_status(
                 data_table_id, index_table_id, status, create_mem_version, new_schema_version, trans))) {
    LOG_WARN(
        "update index status failed", K(ret), K(data_table_id), K(index_table_id), K(status), K(create_mem_version));
  }
  return ret;
}

int ObDDLOperator::update_table_attribute(ObTableSchema& new_table_schema, common::ObMySQLTransaction& trans,
    const ObSchemaOperationType operation_type, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    RS_LOG(WARN, "schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    new_table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service_impl->get_table_sql_service().update_table_attribute(
            trans, new_table_schema, operation_type, ddl_stmt_str))) {
      RS_LOG(WARN, "failed to update table attribute!", K(ret));
    }
  }
  return ret;
}

// The following situations require delayed deletion
// 1. backup enable
// 2. standalone cluster enable
int ObDDLOperator::check_is_delay_delete(const int64_t tenant_id, bool& is_delay_delete)
{
  int ret = OB_SUCCESS;
  is_delay_delete = false;
  // check whether backup enable
  if (OB_SUCC(ret)) {
    int64_t reserved_schema_version = -1;
    ObBackupInfoMgr& bk_info = ObBackupInfoMgr::get_instance();

    if (OB_FAIL(bk_info.get_delay_delete_schema_version(
            tenant_id, schema_service_, is_delay_delete, reserved_schema_version))) {
      LOG_WARN("get delay delete snaptshot version failed", KR(ret));
    }
  }
  return ret;
}

int ObDDLOperator::create_new_name_for_delay_delete_database(common::ObString& new_name,
    const ObDatabaseSchema* db_schema, common::ObIAllocator& allocator, ObSchemaGetterGuard& guard)
{
  int ret = OB_SUCCESS;
  char temp_str_buf[number::ObNumber::MAX_PRINTABLE_SIZE];
  bool is_dup_new_name_exist = true;
  uint64_t db_id = OB_INVALID_ID;

  if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    while (OB_SUCC(ret) && is_dup_new_name_exist) {
      if (snprintf(temp_str_buf, sizeof(temp_str_buf), "DELAY_DELETE_%ld", ObTimeUtility::current_time()) < 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("failed to generate buffer for temp_str_buf", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, ObString::make_string(temp_str_buf), new_name))) {
        LOG_WARN("Can not malloc space for new name", K(ret));
      } else {
        // check whether db with the same name exist in the same tenant.
        if (OB_FAIL(guard.check_database_exist(db_schema->get_tenant_id(), new_name, is_dup_new_name_exist, &db_id))) {
          LOG_WARN("failed to check db exist", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::create_new_name_for_delay_delete_table(common::ObString& new_name, const ObTableSchema* table_schema,
    common::ObIAllocator& allocator, ObSchemaGetterGuard& guard)
{
  int ret = OB_SUCCESS;
  char temp_str_buf[number::ObNumber::MAX_PRINTABLE_SIZE];
  bool is_dup_new_name_exist = true;
  ObString new_index_name;

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    while (OB_SUCC(ret) && is_dup_new_name_exist) {
      if (snprintf(temp_str_buf, sizeof(temp_str_buf), "DELAY_DELETE_%ld", ObTimeUtility::current_time()) < 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("failed to generate buffer for temp_str_buf", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, ObString::make_string(temp_str_buf), new_name))) {
        LOG_WARN("Can not malloc space for new name", K(ret));
      } else if (table_schema->is_index_table()) {
        // Check whether the new index table name conflicts, index table needs rename and adding prefix by calling
        // build_index_table_name
        if (OB_FAIL(ObTableSchema::build_index_table_name(
                allocator, table_schema->get_data_table_id(), new_name, new_index_name))) {
          LOG_WARN("build index_table_name failed", K(ret), K(table_schema->get_data_table_id()), K(new_name));
        } else if (FALSE_IT(new_name = new_index_name)) {
        } else if (OB_FAIL(guard.check_table_exist(table_schema->get_tenant_id(),
                       table_schema->get_database_id(),
                       new_name,
                       true,  // index
                       ObSchemaGetterGuard::ALL_TYPES,
                       is_dup_new_name_exist))) {
          LOG_WARN("check index table exist failed",
              K(ret),
              K(table_schema->get_tenant_id()),
              K(table_schema->get_database_id()),
              K(new_name));
        }
      } else {  // check whether the name of the table which is not index table conflicts
        if (OB_FAIL(guard.check_table_exist(table_schema->get_tenant_id(),
                table_schema->get_database_id(),
                new_name,
                false,  // not index
                ObSchemaGetterGuard::ALL_TYPES,
                is_dup_new_name_exist))) {
          LOG_WARN("check table exist failed",
              K(ret),
              K(table_schema->get_tenant_id()),
              K(table_schema->get_database_id()),
              K(new_name));
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::create_new_name_for_delay_delete_table_partition(
    common::ObString& new_name, const ObTableSchema* table_schema, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  char temp_str_buf[number::ObNumber::MAX_PRINTABLE_SIZE];
  bool is_dup_new_name_exist = true;

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    while (OB_SUCC(ret) && is_dup_new_name_exist) {
      if (snprintf(temp_str_buf, sizeof(temp_str_buf), "DELAY_DELETE_%ld", ObTimeUtility::current_time()) < 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("failed to generate buffer for temp_str_buf", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, ObString::make_string(temp_str_buf), new_name))) {
        LOG_WARN("Can not malloc space for new name", K(ret));
      } else {
        // check whether the partition with the same name exist in the same table.
        ObPartition** orig_part_array = table_schema->get_part_array();
        const int64_t orig_part_num = table_schema->get_part_option().get_part_num();
        if (table_schema->get_part_level() == PARTITION_LEVEL_ZERO) {
          // means non-partitioned table or index etc, do nothing
          is_dup_new_name_exist = false;
        } else {
          if (OB_ISNULL(orig_part_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret));
          } else {
            is_dup_new_name_exist = false;
            for (int64_t i = 0; OB_SUCC(ret) && i < orig_part_num && !is_dup_new_name_exist; ++i) {
              if (OB_ISNULL(orig_part_array[i])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("NULL ptr", K(ret));
              } else if (ObCharset::case_insensitive_equal(orig_part_array[i]->get_part_name(), new_name)) {
                is_dup_new_name_exist = true;
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::create_new_name_for_delay_delete_tablegroup(common::ObString& new_name,
    const ObTablegroupSchema* tablegroup_schema, common::ObIAllocator& allocator, ObSchemaGetterGuard& guard)
{
  int ret = OB_SUCCESS;
  char temp_str_buf[number::ObNumber::MAX_PRINTABLE_SIZE];
  bool is_dup_new_name_exist = true;
  uint64_t tablegroup_id = 0;

  if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    while (OB_SUCC(ret) && is_dup_new_name_exist) {
      if (snprintf(temp_str_buf, sizeof(temp_str_buf), "DELAY_DELETE_%ld", ObTimeUtility::current_time()) < 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("failed to generate buffer for temp_str_buf", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, ObString::make_string(temp_str_buf), new_name))) {
        LOG_WARN("Can not malloc space for new name", K(ret));
      } else {
        // check whether the tablegroup with the same name exist in the same tenant.
        if (OB_FAIL(guard.check_tablegroup_exist(
                tablegroup_schema->get_tenant_id(), new_name, is_dup_new_name_exist, &tablegroup_id))) {
          LOG_WARN("failed to check tablegroup exist", K(ret));
        }
      }
    }
  }

  return ret;
}

// Rename the partitions with the same name using uniform naming rules for all delayed deleted objects:
// DELAY_DELETE_timestamp
int ObDDLOperator::create_new_name_for_delay_delete_tablegroup_partition(
    common::ObString& new_name, const ObTablegroupSchema* tablegroup_schema, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  char temp_str_buf[number::ObNumber::MAX_PRINTABLE_SIZE];
  bool is_dup_new_name_exist = true;
  if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    while (OB_SUCC(ret) && is_dup_new_name_exist) {
      if (snprintf(temp_str_buf, sizeof(temp_str_buf), "DELAY_DELETE_%ld", ObTimeUtility::current_time()) < 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("failed to generate buffer for temp_str_buf", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, ObString::make_string(temp_str_buf), new_name))) {
        LOG_WARN("Can not malloc space for new name", K(ret));
      } else {
        // check whether the partition with the same name exist in the same tablegroup.
        ObPartition** orig_part_array = tablegroup_schema->get_part_array();
        const int64_t orig_part_num = tablegroup_schema->get_part_option().get_part_num();
        if (tablegroup_schema->get_part_level() == PARTITION_LEVEL_ZERO) {
          // means non-partitioned tablegroup, do nothing
          is_dup_new_name_exist = false;
        } else {
          if (OB_ISNULL(orig_part_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret));
          } else {
            is_dup_new_name_exist = false;
            for (int64_t i = 0; OB_SUCC(ret) && i < orig_part_num && !is_dup_new_name_exist; ++i) {
              if (OB_ISNULL(orig_part_array[i])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("NULL ptr", K(ret));
              } else {
                if (ObCharset::case_insensitive_equal(orig_part_array[i]->get_part_name(), new_name)) {
                  is_dup_new_name_exist = true;
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

int ObDDLOperator::drop_obj_privs(const uint64_t tenant_id, const uint64_t obj_id, const uint64_t obj_type,
    ObMySQLTransaction& trans, ObMultiVersionSchemaService& schema_service, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_sql_service = schema_service.get_schema_service();
  ObArray<const ObObjPriv*> obj_privs;

  CK(OB_NOT_NULL(schema_sql_service));
  OZ(schema_guard.get_obj_priv_with_obj_id(tenant_id, obj_id, obj_type, obj_privs, true));
  for (int64_t i = 0; OB_SUCC(ret) && i < obj_privs.count(); ++i) {
    const ObObjPriv* obj_priv = obj_privs.at(i);
    int64_t new_schema_version = OB_INVALID_VERSION;

    if (OB_ISNULL(obj_priv)) {
      LOG_WARN("obj_priv priv is NULL", K(ret), K(obj_priv));
    } else {
      OZ(schema_service.gen_new_schema_version(tenant_id, new_schema_version));
      OZ(schema_sql_service->get_priv_sql_service().delete_obj_priv(*obj_priv, new_schema_version, trans));
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
    const uint64_t tenant_id, const uint64_t obj_id, const uint64_t obj_type, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;

  OZ(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard));
  OZ(drop_obj_privs(tenant_id, obj_id, obj_type, trans, schema_service_, schema_guard));

  return ret;
}

int ObDDLOperator::drop_table(const ObTableSchema& table_schema, ObMySQLTransaction& trans,
    const ObString* ddl_stmt_str /*=NULL*/, const bool is_truncate_table /*false*/,
    DropTableIdHashSet* drop_table_set /*=NULL*/, const bool is_drop_db /*false*/)
{
  int ret = OB_SUCCESS;
  bool is_delay_delete = false;
  if (OB_FAIL(check_is_delay_delete(table_schema.get_tenant_id(), is_delay_delete))) {
    LOG_WARN("check is delay delete failed", K(ret), K(table_schema.get_tenant_id()));
  } else if (is_delay_delete) {
    // do nothing
  } else if (table_schema.is_dropped_schema() && OB_FAIL(drop_table_for_inspection(table_schema, trans))) {
    LOG_WARN("drop table for dropped shema failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (!table_schema.is_dropped_schema() &&
             OB_FAIL(drop_table_for_not_dropped_schema(
                 table_schema, trans, ddl_stmt_str, is_truncate_table, drop_table_set, is_drop_db))) {
    LOG_WARN("drop table for not dropped shema failed", K(ret));
  }

  return ret;
}

// This function is specifically used to deal with tables that have not been deleted delayed
int ObDDLOperator::drop_table_for_not_dropped_schema(const ObTableSchema& table_schema, ObMySQLTransaction& trans,
    const ObString* ddl_stmt_str /*=NULL*/, const bool is_truncate_table /*false*/,
    DropTableIdHashSet* drop_table_set /*=NULL*/, const bool is_drop_db /*false*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  bool is_delay_delete = false;
  ObString new_name;

  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(check_is_delay_delete(tenant_id, is_delay_delete))) {
    LOG_WARN("check is delay delete failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  }
  // delete all object privileges granted on the object
  uint64_t obj_type = static_cast<uint64_t>(ObObjectType::TABLE);
  uint64_t table_id = table_schema.get_table_id();
  if (OB_SUCC(ret) && !is_drop_db) {
    OZ(drop_obj_privs(tenant_id, table_id, obj_type, trans), tenant_id, table_id, obj_type);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(cleanup_autoinc_cache(table_schema))) {
    LOG_WARN("fail cleanup auto inc global cache", K(ret));
  } else if (is_delay_delete &&
             OB_FAIL(create_new_name_for_delay_delete_table(new_name, &table_schema, allocator, schema_guard))) {
    LOG_WARN("create new name for delay delete table failed", K(ret));
  } else if (OB_FAIL(schema_service_impl->get_table_sql_service().drop_table(table_schema,
                 new_schema_version,
                 trans,
                 ddl_stmt_str,
                 is_truncate_table,
                 is_drop_db,
                 &schema_guard,
                 drop_table_set,
                 is_delay_delete,
                 &new_name))) {
    LOG_WARN("schema_service_impl drop_table failed", K(table_schema), K(ret));
  } else if (OB_FAIL(sync_version_for_cascade_table(table_schema.get_base_table_ids(), trans)) ||
             OB_FAIL(sync_version_for_cascade_table(table_schema.get_depend_table_ids(), trans))) {
    LOG_WARN("fail to sync versin for cascade tables",
        K(ret),
        K(table_schema.get_base_table_ids()),
        K(table_schema.get_depend_table_ids()));
  }

  return ret;
}

// When tables with auto-increment columns are frequently created or deleted, if the auto-increment column cache is not
// cleared, the memory will grow slowly. so every time when you drop table, if you bring auto-increment columns, clean
// up the corresponding cache.
int ObDDLOperator::cleanup_autoinc_cache(const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  ObAutoincrementService& autoinc_service = share::ObAutoincrementService::get_instance();
  if (0 != table_schema.get_autoinc_column_id()) {
    uint64_t tenant_id = table_schema.get_tenant_id();
    uint64_t table_id = table_schema.get_table_id();
    uint64_t autoinc_column_id = table_schema.get_autoinc_column_id();
    LOG_INFO("begin to clear all auto-increment cache", K(tenant_id), K(table_id), K(autoinc_column_id));
    if (OB_FAIL(autoinc_service.clear_autoinc_cache_all(tenant_id, table_id, autoinc_column_id))) {
      LOG_WARN("failed to clear auto-increment cache", K(tenant_id), K(table_id));
    }
  }
  return ret;
}

bool ObDDLOperator::is_aux_object(const ObDatabaseSchema& schema)
{
  UNUSED(schema);
  return false;
}

bool ObDDLOperator::is_aux_object(const ObTableSchema& schema)
{
  return schema.is_aux_table();
}

bool ObDDLOperator::is_aux_object(const ObTenantSchema& schema)
{
  UNUSED(schema);
  return false;
}

bool ObDDLOperator::is_global_index_object(const ObDatabaseSchema& schema)
{
  UNUSED(schema);
  return false;
}

bool ObDDLOperator::is_global_index_object(const ObTableSchema& schema)
{
  // For global local storage, local indexes are still seen, liboblog does not need to be synchronized
  return schema.is_global_index_table() && (!schema.is_index_local_storage());
}

bool ObDDLOperator::is_global_index_object(const ObTenantSchema& schema)
{
  UNUSED(schema);
  return false;
}

int ObDDLOperator::drop_table_to_recyclebin(const ObTableSchema& table_schema, ObSchemaGetterGuard& schema_guard,
    ObMySQLTransaction& trans, const ObString* ddl_stmt_str, /*= NULL*/
    const bool is_truncate_table)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  bool recycle_db_exist = false;
  // materialized view will not be dropped into recyclebin
  if (table_schema.get_table_type() == MATERIALIZED_VIEW) {
    LOG_WARN("bypass recyclebin for materialized view");
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else if (OB_FAIL(
                 schema_guard.check_database_exist(combine_id(tenant_id, OB_RECYCLEBIN_SCHEMA_ID), recycle_db_exist))) {
    LOG_WARN("check database exist failed", K(ret));
  } else if (!recycle_db_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("__recyclebin db not exist", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(cleanup_autoinc_cache(table_schema))) {
    LOG_WARN("fail cleanup auto inc global cache", K(ret));
  } else {
    ObTableSchema new_table_schema;
    if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else {
      ObSqlString new_table_name;
      // move to the recyclebin db
      new_table_schema.set_database_id(combine_id(table_schema.get_tenant_id(), OB_RECYCLEBIN_SCHEMA_ID));
      uint64_t tablegroup_id = table_schema.get_tablegroup_id();
      if (OB_INVALID_ID != tablegroup_id) {
        const ObTablegroupSchema* tablegroup_schema = nullptr;
        if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
          LOG_WARN("get tablegroup schema failed", K(ret));
        } else if (!tablegroup_schema->get_binding()) {
          // If the tablegroup has no PG attribute, set the tablgroup_id of the table to INVALID
          // if not, because all tables in PG share the storage structure, and the relationship with tablegroup cannot
          // be released, the tablegroup_id of the table is not modified
          new_table_schema.set_tablegroup_id(OB_INVALID_ID);
        }
      }
      new_table_schema.set_schema_version(new_schema_version);
      ObSchemaOperationType op_type;
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
        op_type = table_schema.is_view_table() ? OB_DDL_DROP_VIEW_TO_RECYCLEBIN : OB_DDL_DROP_TABLE_TO_RECYCLEBIN;
        if (is_truncate_table) {
          op_type = OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN;
        }
        if (OB_FAIL(recycle_object.set_type_by_table_schema(table_schema))) {
          LOG_WARN("set type by table schema failed", K(ret));
        } else if (OB_FAIL(schema_service_impl->insert_recyclebin_object(recycle_object, trans))) {
          LOG_WARN("insert recycle object failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(schema_service_impl->get_table_sql_service().update_table_options(
                trans, table_schema, new_table_schema, op_type, ddl_stmt_str))) {
          LOG_WARN("failed to alter table option!", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::flashback_table_from_recyclebin(const ObTableSchema& table_schema, ObMySQLTransaction& trans,
    const uint64_t new_db_id, const ObString& new_table_name, const ObString* ddl_stmt_str, ObSchemaGetterGuard& guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
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
                 tenant_id, table_schema.get_table_name_str(), recycle_type, trans, recycle_objs))) {
    LOG_WARN("get_recycle_object failed", K(tenant_id), K(ret));
  } else if (recycle_objs.size() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected recycle object num",
        K(ret),
        "table_name",
        table_schema.get_table_name_str(),
        "size",
        recycle_objs.size());
  } else {
    const ObRecycleObject& recycle_obj = recycle_objs.at(0);
    ObTableSchema new_table_schema;
    if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else if (new_db_id != OB_INVALID_ID) {  // flashback to new db
      new_table_schema.set_database_id(new_db_id);
      if (new_table_schema.is_aux_table()) {
        // should set the old name
        // When flashback table to new db, distinguish between empty index name and renaming flashback index
        if (!new_table_name.empty() && OB_FAIL(new_table_schema.set_table_name(new_table_name))) {
          LOG_WARN("set new table name failed", K(ret));
        } else if (new_table_name.empty() &&
                   OB_FAIL(new_table_schema.set_table_name(recycle_obj.get_original_name()))) {
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
            if (OB_FAIL(ObTableSchema::create_new_idx_name_after_flashback(
                    new_table_schema, new_idx_name, allocator, guard))) {
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
          LOG_WARN("database is valid, but no table name for data table", K(ret), K(new_table_name));
        }
      }
    } else {
      // set original db_id
      const ObDatabaseSchema* db_schema = NULL;
      if (OB_FAIL(guard.get_database_schema(tenant_id, recycle_obj.get_database_id(), db_schema))) {
        LOG_WARN("get database schema failed", K(ret));
      } else if (NULL == db_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database not exist", K(recycle_obj), K(ret));
      } else if (db_schema->is_in_recyclebin()) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("flashback table to __recyclebin database is not allowed", K(recycle_obj), K(*db_schema), K(ret));
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
          if (OB_FAIL(ObTableSchema::create_new_idx_name_after_flashback(
                  new_table_schema, new_idx_name, allocator, guard))) {
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
      const int64_t table_schema_version = OB_INVALID_VERSION;  // Take the latest local schema_guard
      ObSchemaOperationType op_type = new_table_schema.is_view_table() ? OB_DDL_FLASHBACK_VIEW : OB_DDL_FLASHBACK_TABLE;
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
        LOG_USER_ERROR(
            OB_ERR_TABLE_EXIST, recycle_obj.get_original_name().length(), recycle_obj.get_original_name().ptr());
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (FALSE_IT(new_table_schema.set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_options(
                     trans, table_schema, new_table_schema, op_type, ddl_stmt_str))) {
        LOG_WARN("update_table_options failed", K(ret));
      } else if (OB_FAIL(schema_service->delete_recycle_object(tenant_id, recycle_obj, trans))) {
        LOG_WARN("delete_recycle_object failed", K(tenant_id), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::purge_table_with_aux_table(const ObTableSchema& table_schema, ObSchemaGetterGuard& schema_guard,
    ObMySQLTransaction& trans, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_aux_table()) {
    if (OB_FAIL(purge_aux_table(table_schema, schema_guard, trans, USER_INDEX))) {
      LOG_WARN("purge_aux_table failed", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(purge_table_in_recyclebin(table_schema, trans, ddl_stmt_str))) {
      LOG_WARN("purge table failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::purge_aux_table(const ObTableSchema& table_schema, ObSchemaGetterGuard& schema_guard,
    ObMySQLTransaction& trans, const ObTableType table_type)
{
  int ret = OB_SUCCESS;
  UNUSED(table_type);
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get_aux_tid_array failed", K(ret));
  }
  int64_t N = simple_index_infos.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObTableSchema* aux_table_schema = NULL;
    uint64_t tid = simple_index_infos.at(i).table_id_;
    if (OB_FAIL(schema_guard.get_table_schema(tid, aux_table_schema))) {
      LOG_WARN("get_table_schema failed", "table id", tid, K(ret));
    } else if (OB_ISNULL(aux_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else if (OB_FAIL(purge_table_in_recyclebin(*aux_table_schema, trans, NULL /*ddl_stmt_str*/))) {
      LOG_WARN("ddl_operator drop_table failed", K(*aux_table_schema), K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::purge_table_in_recyclebin(
    const ObTableSchema& table_schema, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObArray<ObRecycleObject> recycle_objs;
  ObRecycleObject::RecycleObjType recycle_type = ObRecycleObject::get_type_by_table_schema(table_schema);

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_INVALID_ID == table_schema.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else if (OB_FAIL(schema_service->fetch_recycle_object(
                 table_schema.get_tenant_id(), table_schema.get_table_name_str(), recycle_type, trans, recycle_objs))) {
    LOG_WARN("get_recycle_object failed", K(recycle_type), K(ret));
  } else if (recycle_objs.size() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected recycle object num", K(ret), K(recycle_objs.size()));
  } else if (OB_FAIL(schema_service->delete_recycle_object(table_schema.get_tenant_id(), recycle_objs.at(0), trans))) {
    LOG_WARN("delete_recycle_object failed", K(ret), "ObRecycleObject", recycle_objs.at(0));
  } else if (OB_FAIL(drop_table(table_schema, trans, ddl_stmt_str, false))) {
    LOG_WARN("drop table failed", K(ret));
  }
  return ret;
}

int ObDDLOperator::create_index_in_recyclebin(ObTableSchema& table_schema, ObSchemaGetterGuard& schema_guard,
    ObMySQLTransaction& trans, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  if (table_schema.get_table_type() != USER_INDEX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema type is not index", K(ret));
  } else {
    ObSchemaService* schema_service_impl = schema_service_.get_schema_service();
    uint64_t tenant_id = OB_INVALID_ID;
    int64_t new_schema_version = OB_INVALID_VERSION;
    bool recycle_db_exist = false;
    if (OB_ISNULL(schema_service_impl)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("schema_service_impl must not be null", K(ret));
    } else if (OB_INVALID_ID == (tenant_id = table_schema.get_tenant_id())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant_id is invalid", K(ret));
    } else if (OB_FAIL(schema_guard.check_database_exist(
                   combine_id(tenant_id, OB_RECYCLEBIN_SCHEMA_ID), recycle_db_exist))) {
      LOG_WARN("check database exist failed", K(ret));
    } else if (!recycle_db_exist) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("__recyclebin db not exist", K(ret));
    }

    if (OB_SUCC(ret)) {
      ObSqlString new_table_name;
      ObTableSchema new_table_schema;
      if (OB_FAIL(new_table_schema.assign(table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else {
        new_table_schema.set_database_id(combine_id(table_schema.get_tenant_id(), OB_RECYCLEBIN_SCHEMA_ID));
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
        recycle_object.set_tenant_id(table_schema.get_tenant_id());
        recycle_object.set_database_id(table_schema.get_database_id());
        recycle_object.set_table_id(table_schema.get_table_id());
        recycle_object.set_tablegroup_id(table_schema.get_tablegroup_id());
        if (OB_FAIL(recycle_object.set_type_by_table_schema(table_schema))) {
          LOG_WARN("set type by table schema failed", K(ret));
        } else if (OB_FAIL(schema_service_impl->insert_recyclebin_object(recycle_object, trans))) {
          LOG_WARN("insert recycle object failed", K(ret));
        } else if (OB_FAIL(schema_service_impl->get_table_sql_service().create_table(
                       new_table_schema, trans, ddl_stmt_str, true, true))) {
          LOG_WARN("failed to create table in recyclebin", K(ret));
        } else if (OB_FAIL(sync_version_for_cascade_table(new_table_schema.get_depend_table_ids(), trans))) {
          LOG_WARN("fail to sync cascade depend table", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::update_tablegroup_id_of_tables(
    const ObDatabaseSchema& database_schema, ObMySQLTransaction& trans, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = database_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObArray<const ObTableSchema*> table_schemas;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_FAIL(schema_guard.get_table_schemas_in_database(
          database_schema.get_tenant_id(), database_schema.get_database_id(), table_schemas))) {
    LOG_WARN("get_table_schemas_in_database failed", K(ret));
  }
  bool tg_exist = false;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < table_schemas.count(); ++idx) {
    const ObTableSchema* table = table_schemas.at(idx);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else if (table->is_index_table()) {
      continue;
    } else if (OB_INVALID_ID != table->get_tablegroup_id() &&
               OB_FAIL(schema_guard.check_tablegroup_exist(table->get_tablegroup_id(), tg_exist))) {
      LOG_WARN("check_tablegroup_exist failed", K(ret));
    } else {
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      if (OB_FAIL(table->get_simple_index_infos(simple_index_infos))) {
        LOG_WARN("get_index_tid_array failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        const ObTableSchema* index_table_schema = NULL;
        if (OB_FAIL(schema_guard.get_table_schema(simple_index_infos.at(i).table_id_, index_table_schema))) {
          LOG_WARN("get_table_schema failed", "table id", simple_index_infos.at(i).table_id_, K(ret));
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
                           trans, *index_table_schema, new_index_schema, OB_DDL_FLASHBACK_TABLE, NULL))) {
              LOG_WARN("update_table_option failed", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObTableSchema new_ts;
        if (OB_FAIL(new_ts.assign(*table))) {
          LOG_WARN("fail to assign schema", K(ret));
        } else {
          if (!tg_exist) {
            new_ts.set_tablegroup_id(OB_INVALID_ID);
          }
          const ObSchemaOperationType op_type = new_ts.is_view_table() ? OB_DDL_FLASHBACK_VIEW : OB_DDL_FLASHBACK_TABLE;
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
  return ret;
}

int ObDDLOperator::flashback_database_from_recyclebin(const ObDatabaseSchema& database_schema,
    ObMySQLTransaction& trans, const ObString& new_db_name, ObSchemaGetterGuard& schema_guard,
    const ObString& ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObArray<ObRecycleObject> recycle_objs;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_INVALID_ID == database_schema.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else if (OB_FAIL(schema_service->fetch_recycle_object(database_schema.get_tenant_id(),
                 database_schema.get_database_name(),
                 ObRecycleObject::DATABASE,
                 trans,
                 recycle_objs))) {
    LOG_WARN("get_recycle_object failed", K(ret));
  } else if (recycle_objs.size() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected recycle object num", K(ret));
  } else {
    const ObRecycleObject& recycle_obj = recycle_objs.at(0);
    uint64_t tg_id = OB_INVALID_ID;
    if (OB_INVALID_ID != recycle_obj.get_tablegroup_id()) {
      bool tg_exist = false;
      if (OB_FAIL(schema_guard.check_tablegroup_exist(recycle_obj.get_tablegroup_id(), tg_exist))) {
        LOG_WARN("check_tablegroup_exist failed", K(ret));
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
        // set original db_id
        if (OB_FAIL(new_db_schema.set_database_name(recycle_obj.get_original_name()))) {
          LOG_WARN("set database name failed", K(recycle_obj));
        }
      }
      if (OB_SUCC(ret)) {
        bool is_database_exist = true;
        const uint64_t tenant_id = database_schema.get_tenant_id();
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(schema_guard.check_database_exist(
                database_schema.get_tenant_id(), new_db_schema.get_database_name_str(), is_database_exist))) {
          LOG_WARN("check database exist failed", K(ret), K(new_db_schema));
        } else if (is_database_exist) {
          ret = OB_DATABASE_EXIST;
          LOG_USER_ERROR(OB_DATABASE_EXIST,
              new_db_schema.get_database_name_str().length(),
              new_db_schema.get_database_name_str().ptr());
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (FALSE_IT(new_db_schema.set_schema_version(new_schema_version))) {
        } else if (OB_FAIL(schema_service->get_database_sql_service().update_database(
                       new_db_schema, trans, OB_DDL_FLASHBACK_DATABASE, &ddl_stmt_str))) {
          LOG_WARN("update_database failed", K(ret), K(new_db_schema));
        } else if (OB_FAIL(
                       schema_service->delete_recycle_object(database_schema.get_tenant_id(), recycle_obj, trans))) {
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

int ObDDLOperator::purge_table_of_database(
    const ObDatabaseSchema& db_schema, ObMySQLTransaction& trans, ObSchemaGetterGuard& guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else {
    ObArray<ObRecycleObject> recycle_objs;
    if (OB_FAIL(schema_service->fetch_recycle_objects_of_db(
            db_schema.get_tenant_id(), db_schema.get_database_id(), trans, recycle_objs))) {
      LOG_WARN("fetch recycle objects of db failed", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < recycle_objs.count(); ++i) {
        const ObRecycleObject& recycle_obj = recycle_objs.at(i);
        const ObTableSchema* table_schema = NULL;
        if (OB_FAIL(guard.get_table_schema(recycle_obj.get_table_id(), table_schema))) {
          LOG_WARN("get table schema failed", K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table is not exist", K(ret), K(recycle_obj));
          LOG_USER_ERROR(OB_TABLE_NOT_EXIST,
              to_cstring(db_schema.get_database_name_str()),
              to_cstring(recycle_obj.get_object_name()));
        } else if (OB_FAIL(purge_table_with_aux_table(*table_schema, guard, trans, NULL /*ddl_stmt_str */))) {
          LOG_WARN("purge table with index failed", K(ret), K(recycle_obj));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::purge_database_in_recyclebin(const ObDatabaseSchema& database_schema, ObMySQLTransaction& trans,
    ObSchemaGetterGuard& guard, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_INVALID_ID == database_schema.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret));
  } else {
    ObArray<ObRecycleObject> recycle_objs;
    if (OB_FAIL(schema_service->fetch_recycle_object(database_schema.get_tenant_id(),
            database_schema.get_database_name_str(),
            ObRecycleObject::DATABASE,
            trans,
            recycle_objs))) {
      LOG_WARN("get_recycle_object failed", K(ret));
    } else if (1 != recycle_objs.size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected recycle object num", K(ret));
    } else if (OB_FAIL(drop_database(database_schema, trans, guard, ddl_stmt_str))) {
      LOG_WARN("drop_table failed", K(ret));
    } else if (OB_FAIL(
                   schema_service->delete_recycle_object(database_schema.get_tenant_id(), recycle_objs.at(0), trans))) {
      LOG_WARN("delete_recycle_object failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::fetch_expire_recycle_objects(
    const uint64_t tenant_id, const int64_t expire_time, ObIArray<ObRecycleObject>& recycle_objs)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_expire_recycle_objects(tenant_id, expire_time, sql_proxy_, recycle_objs))) {
    LOG_WARN("fetch expire recycle objects failed", K(ret), K(expire_time), K(tenant_id));
  }
  return ret;
}

// Unify the processing of upgrade and bootstrap
int ObDDLOperator::finish_schema_split_v2(ObMySQLTransaction& trans, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_INVALID_ID == tenant_id || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    share::schema::ObDDLSqlService ddl_sql_service(*schema_service);
    if (OB_FAIL(ddl_sql_service.finish_schema_split_v2(trans, tenant_id, new_schema_version))) {
      LOG_WARN("log ddl operation failed", K(ret), K(tenant_id), K(new_schema_version));
    }
  }
  LOG_INFO(
      "log ddl operation", K(ret), K(tenant_id), K(new_schema_version), "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::init_tenant_env(
    const ObTenantSchema& tenant_schema, const ObSysVariableSchema& sys_variable, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();

  if (OB_FAIL(init_tenant_tablegroup(tenant_id, trans))) {
    LOG_WARN("insert default tablegroup failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_databases(tenant_schema, sys_variable, trans))) {
    RS_LOG(WARN, "insert default databases failed,", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_profile(tenant_id, sys_variable, trans))) {
    RS_LOG(WARN, "fail to init tenant profile", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_users(tenant_schema, sys_variable, trans))) {
    LOG_WARN("insert default user failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_sys_stats(tenant_id, trans))) {
    LOG_WARN("insert default sys stats failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_config(tenant_id, trans))) {
    LOG_WARN("insert tenant config failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_cgroup(tenant_id, trans))) {
    LOG_WARN("insert tenant groups failed", K(tenant_id), K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(init_sys_tenant_charset(trans))) {
      LOG_WARN("insert charset failed", K(tenant_id), K(ret));
    } else if (OB_FAIL(init_sys_tenant_collation(trans))) {
      LOG_WARN("insert collation failed", K(tenant_id), K(ret));
    } else if (OB_FAIL(init_sys_tenant_privilege(trans))) {
      LOG_WARN("insert privilege failed", K(tenant_id), K(ret));
    }
    // TODO [profile]
  }

  return ret;
}

int ObDDLOperator::init_tenant_tablegroup(const uint64_t tenant_id, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    ObTablegroupSchema tg_schema;
    tg_schema.set_tenant_id(tenant_id);
    tg_schema.set_tablegroup_id(combine_id(tenant_id, OB_SYS_TABLEGROUP_ID));
    tg_schema.set_binding(false);  // The binding property of tablegroups involved in the system tables is false.
    tg_schema.set_tablegroup_name(OB_SYS_TABLEGROUP_NAME);
    tg_schema.set_comment("system tablegroup");
    tg_schema.set_schema_version(OB_CORE_SCHEMA_VERSION);
    tg_schema.set_part_level(PARTITION_LEVEL_ZERO);
    tg_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_tablegroup_sql_service().insert_tablegroup(tg_schema, trans))) {
      LOG_WARN("insert_tablegroup failed", K(tg_schema), K(ret));
    }
  }
  LOG_INFO("init tenant tablegroup", K(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::init_tenant_database(const ObTenantSchema& tenant_schema, const ObString& db_name,
    const uint64_t pure_db_id, const ObString& db_comment, ObMySQLTransaction& trans, const bool is_oracle_mode)
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
    ObSchemaService* schema_service = schema_service_.get_schema_service();
    ObDatabaseSchema db_schema;
    db_schema.set_tenant_id(tenant_id);
    db_schema.set_database_id(combine_id(tenant_id, pure_db_id));
    db_schema.set_database_name(db_name);
    db_schema.set_comment(db_comment);
    db_schema.set_schema_version(new_schema_version);
    if (db_name == OB_RECYCLEBIN_SCHEMA_NAME || ObSchemaUtils::is_public_database(db_name, is_oracle_mode)) {
      db_schema.set_read_only(true);
    }

    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_SYS;
      RS_LOG(ERROR, "schema_service must not null");
    } else if (OB_FAIL(ObSchema::set_charset_and_collation_options(
                   tenant_schema.get_charset_type(), tenant_schema.get_collation_type(), db_schema))) {
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
    db_key.user_id_ =
        is_oracle_mode ? combine_id(tenant_id, OB_ORA_SYS_USER_ID) : combine_id(tenant_id, OB_SYS_USER_ID);
    db_key.db_ = db_name;

    ObSchemaService* schema_service = schema_service_.get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_SYS;
      RS_LOG(ERROR, "schema_service must not null");
    } else {
      ObSqlString ddl_stmt_str;
      ObString ddl_sql;
      ObNeedPriv need_priv;
      need_priv.db_ = db_name;
      need_priv.priv_set_ = OB_PRIV_DB_ACC;  // is collect?
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      if (OB_FAIL(ObDDLSqlGenerator::gen_db_priv_sql(
              ObAccountArg(is_oracle_mode ? OB_ORA_SYS_USER_NAME : OB_SYS_USER_NAME, OB_SYS_HOST_NAME),
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
  LOG_INFO("init tenant database",
      K(ret),
      "tenant_id",
      tenant_schema.get_tenant_id(),
      "database_name",
      db_name,
      "cost",
      ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::init_tenant_databases(
    const ObTenantSchema& tenant_schema, const ObSysVariableSchema& sys_variable, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const bool is_sys = OB_SYS_TENANT_ID == tenant_id;
  ObString oceanbase_schema(OB_SYS_DATABASE_NAME);
  ObString mysql_schema(OB_MYSQL_SCHEMA_NAME);
  ObString information_schema(OB_INFORMATION_SCHEMA_NAME);
  ObString recyclebin_schema(OB_RECYCLEBIN_SCHEMA_NAME);
  ObString mysql_public_schema(OB_PUBLIC_SCHEMA_NAME);
  ObString ora_public_schema(OB_ORA_PUBLIC_SCHEMA_NAME);
  ObString test_schema(OB_TEST_SCHEMA_NAME);
  ObString ora_sys_schema(OB_ORA_SYS_USER_NAME);
  ObString ora_lbacsys_schema(OB_ORA_LBACSYS_NAME);
  ObString ora_auditor_schema(OB_ORA_AUDITOR_NAME);
  bool is_oracle_mode = false;
  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", K(ret));
  } else if (OB_FAIL(init_tenant_database(
                 tenant_schema, oceanbase_schema, OB_SYS_DATABASE_ID, "system database", trans, is_oracle_mode))) {
    RS_LOG(WARN, "insert default database failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(init_tenant_database(tenant_schema,
                 recyclebin_schema,
                 OB_RECYCLEBIN_SCHEMA_ID,
                 "recyclebin schema",
                 trans,
                 is_oracle_mode))) {
    RS_LOG(WARN, "insert recyclebin schema failed", K(tenant_id), K(ret));
  } else {
    if (!is_oracle_mode) {
      if (OB_FAIL(
              init_tenant_database(tenant_schema, mysql_schema, OB_MYSQL_SCHEMA_ID, "MySql schema", trans, false))) {
        RS_LOG(WARN, "insert information_schema failed", K(tenant_id), K(ret));
      } else if (OB_FAIL(init_tenant_database(tenant_schema,
                     information_schema,
                     OB_INFORMATION_SCHEMA_ID,
                     "information_schema",
                     trans,
                     false))) {
        RS_LOG(WARN, "insert mysql schema failed", K(tenant_id), K(ret));
      } else if (OB_FAIL(init_tenant_database(
                     tenant_schema, test_schema, OB_USER_DATABASE_ID, "test schema", trans, is_oracle_mode))) {
        RS_LOG(WARN, "insert test schema failed", K(tenant_id), K(ret));
      } else if (OB_FAIL(init_tenant_database(tenant_schema,
                     mysql_public_schema,
                     OB_PUBLIC_SCHEMA_ID,
                     "public schema",
                     trans,
                     is_oracle_mode))) {
        RS_LOG(WARN, "insert public schema failed", K(tenant_id), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_oracle_mode || is_sys) {
      if (OB_FAIL(init_tenant_database(
              tenant_schema, ora_sys_schema, OB_ORA_SYS_DATABASE_ID, "oracle sys schema", trans, is_oracle_mode))) {
        RS_LOG(WARN, "insert oracle sys schema failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(init_tenant_database(tenant_schema,
                     ora_lbacsys_schema,
                     OB_ORA_LBACSYS_DATABASE_ID,
                     "oracle sys schema",
                     trans,
                     is_oracle_mode))) {
        RS_LOG(WARN, "insert oracle lbacsys schema failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(init_tenant_database(tenant_schema,
                     ora_auditor_schema,
                     OB_ORA_AUDITOR_DATABASE_ID,
                     "oracle sys schema",
                     trans,
                     is_oracle_mode))) {
        RS_LOG(WARN, "insert oracle lbacsys schema failed", K(ret), K(tenant_id));
      } else if (is_oracle_mode) {
        if (OB_FAIL(init_tenant_database(
                tenant_schema, ora_public_schema, OB_PUBLIC_SCHEMA_ID, "public shcema", trans, is_oracle_mode))) {
          RS_LOG(WARN, "insert public schema failed", K(tenant_id), K(ret));
        }
      }
    }
  }

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
RESOURCE CREATE INDEXTYPE                         NO  YES YES
RESOURCE CREATE SEQUENCE                          NO  YES YES*/
int ObDDLOperator::build_raw_priv_info_inner_user(uint64_t grantee_id, ObRawPrivArray& raw_priv_array, uint64_t& option)
{
  int ret = OB_SUCCESS;
  if (is_ora_dba_role(grantee_id) || is_ora_sys_user(grantee_id)) {
    option = ADMIN_OPTION;
    for (int i = PRIV_ID_NONE + 1; i < PRIV_ID_MAX && OB_SUCC(ret); i++) {
      if (i != PRIV_ID_EXEMPT_RED_PLY && i != PRIV_ID_SYSDBA && i != PRIV_ID_SYSOPER && i != PRIV_ID_SYSBACKUP) {
        OZ(raw_priv_array.push_back(i));
      }
    }
  } else if (is_ora_resource_role(grantee_id)) {
    option = NO_OPTION;
    OZ(raw_priv_array.push_back(PRIV_ID_CREATE_TABLE));
    OZ(raw_priv_array.push_back(PRIV_ID_CREATE_TYPE));
    OZ(raw_priv_array.push_back(PRIV_ID_CREATE_TRIG));
    OZ(raw_priv_array.push_back(PRIV_ID_CREATE_PROC));
    OZ(raw_priv_array.push_back(PRIV_ID_CREATE_SEQ));
  } else if (is_ora_connect_role(grantee_id)) {
    option = NO_OPTION;
    OZ(raw_priv_array.push_back(PRIV_ID_CREATE_SESSION));
  } else if (is_ora_public_role(grantee_id)) {
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("grantee_id error", K(ret), K(grantee_id));
  }
  return ret;
}

int ObDDLOperator::init_inner_user_privs(
    const uint64_t tenant_id, ObUserInfo& user, ObMySQLTransaction& trans, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t grantee_id = user.get_user_id();
  uint64_t option = NO_OPTION;
  ObRawPrivArray raw_priv_array;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  }
  if (OB_SUCC(ret) && is_oracle_mode) {
    ObString empty_str;
    OZ(build_raw_priv_info_inner_user(grantee_id, raw_priv_array, option), grantee_id);
    OZ(schema_service_.gen_new_schema_version(tenant_id, new_schema_version), tenant_id);
    OZ(schema_service->get_priv_sql_service().grant_sys_priv_to_ur(tenant_id,
           grantee_id,
           option,
           raw_priv_array,
           new_schema_version,
           &empty_str,
           trans,
           true /*is_grant*/,
           false),
        tenant_id,
        grantee_id,
        ADMIN_OPTION,
        raw_priv_array);
  }
  return ret;
}

int ObDDLOperator::init_tenant_user(const uint64_t tenant_id, const ObString& user_name, const char* password,
    const uint64_t pure_user_id, const ObString& user_comment, ObMySQLTransaction& trans, const bool set_locked,
    const bool is_user, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  ObString pwd_enc;
  char enc_buf[ENC_BUF_LEN] = {0};
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObUserInfo user;
  ObString pwd_raw(password);
  user.set_tenant_id(tenant_id);
  pwd_enc.assign_ptr(enc_buf, ENC_BUF_LEN);
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null");
  } else if (pwd_raw.length() > 0 && OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage2(pwd_raw, pwd_enc))) {
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
    user.set_user_id(combine_id(tenant_id, pure_user_id));
    if (!is_oracle_mode || is_user) {
      user.set_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT);
    }
    user.set_schema_version(OB_CORE_SCHEMA_VERSION);
    user.set_profile_id(OB_INVALID_ID);
    user.set_type((is_user) ? OB_USER : OB_ROLE);
  }
  if (OB_SUCC(ret)) {
    ObSqlString ddl_stmt_str;
    ObString ddl_sql;
    if (OB_FAIL(ObDDLSqlGenerator::gen_create_user_sql(
            ObAccountArg(user.get_user_name_str(), user.get_host_name_str(), user.is_role()),
            user.get_passwd_str(),
            ddl_stmt_str))) {
      LOG_WARN("gen create user sql failed", K(user), K(ret));
    } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service->get_user_sql_service().create_user(user, new_schema_version, &ddl_sql, trans))) {
      LOG_WARN("insert user failed", K(user), K(ret));
    } else if ((!is_user || is_ora_sys_user(user.get_user_id())) &&
               OB_FAIL(init_inner_user_privs(tenant_id, user, trans, is_oracle_mode))) {
      LOG_WARN("init user privs failed", K(user), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::init_tenant_users(
    const ObTenantSchema& tenant_schema, const ObSysVariableSchema& sys_variable, ObMySQLTransaction& trans)
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
  bool is_oracle_mode = false;
  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    RS_LOG(WARN, "failed to get oracle mode", K(ret), K(tenant_id));
  } else if (is_oracle_mode) {
    // Only retain the three users that is SYS/LBACSYS/ORAAUDITOR in Oracle mode.
    if (OB_FAIL(init_tenant_user(tenant_id,
            ora_sys_user_name,
            "",
            OB_ORA_SYS_USER_ID,
            "oracle system administrator",
            trans,
            false,
            true,
            true))) {
      RS_LOG(WARN, "failed to init oracle sys user", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id,
                   ora_lbacsys_user_name,
                   OB_ORA_LBACSYS_NAME,
                   OB_ORA_LBACSYS_USER_ID,
                   "oracle system administrator",
                   trans,
                   true,
                   true,
                   true))) {
      RS_LOG(WARN, "failed to init oracle sys user", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id,
                   ora_auditor_user_name,
                   OB_ORA_AUDITOR_NAME,
                   OB_ORA_AUDITOR_USER_ID,
                   "oracle system administrator",
                   trans,
                   true,
                   true,
                   true))) {
      RS_LOG(WARN, "failed to init oracle sys user", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id,
                   ora_connect_role_name,
                   "",
                   OB_ORA_CONNECT_ROLE_ID,
                   "oracle connect role",
                   trans,
                   false,
                   false,
                   true))) {
      RS_LOG(WARN, "fail to init oracle connect role", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id,
                   ora_resource_role_name,
                   "",
                   OB_ORA_RESOURCE_ROLE_ID,
                   "oracle resource role",
                   trans,
                   false,
                   false,
                   true))) {
      RS_LOG(WARN, "fail to init oracle resource role", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id,
                   ora_dba_role_name,
                   "",
                   OB_ORA_DBA_ROLE_ID,
                   "oracle dba role",
                   trans,
                   false,
                   false,
                   true))) {
      RS_LOG(WARN, "fail to init oracle dba role", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id,
                   ora_public_role_name,
                   "",
                   OB_ORA_PUBLIC_ROLE_ID,
                   "oracle public role",
                   trans,
                   false,
                   false,
                   true))) {
      RS_LOG(WARN, "fail to init oracle public role", K(ret), K(tenant_id));
    }
  } else {
    if (OB_FAIL(init_tenant_user(tenant_id, sys_user_name, "", OB_SYS_USER_ID, "system administrator", trans))) {
      RS_LOG(WARN, "failed to init sys user", K(ret), K(tenant_id));
    } else if (OB_FAIL(init_tenant_user(tenant_id,
                   ora_auditor_user_name,
                   OB_ORA_AUDITOR_NAME,
                   OB_ORA_AUDITOR_USER_ID,
                   "system administrator",
                   trans,
                   true))) {
      RS_LOG(WARN, "failed to init mysql audit user", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObDDLOperator::init_tenant_config(const uint64_t tenant_id, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  ObSqlString sql;
  const static char* from_seed = "select config_version, zone, svr_type, svr_ip, svr_port, name, "
                                 "data_type, value, info, section, scope, source, edit_level "
                                 "from __all_seed_parameter";
  ObSQLClientRetryWeak sql_client_retry_weak(&sql_proxy_, false);
  SMART_VAR(ObMySQLProxy::MySQLResult, result)
  {
    int64_t max_version = 0, expected_rows = 0;
    bool is_first = true;
    if (OB_FAIL(sql_client_retry_weak.read(result, OB_SYS_TENANT_ID, from_seed))) {
      LOG_WARN("read config from __all_seed_parameter failed", K(from_seed), K(ret));
    } else {
      sql.reset();
      if (OB_FAIL(sql.assign_fmt("INSERT INTO %s "
                                 "(ZONE, SVR_TYPE, SVR_IP, SVR_PORT, NAME, DATA_TYPE, VALUE, INFO, "
                                 "SECTION, SCOPE, SOURCE, EDIT_LEVEL, CONFIG_VERSION) VALUES",
              OB_TENANT_PARAMETER_TNAME))) {
        LOG_WARN("sql assign failed", K(ret));
      }

      while (OB_SUCC(ret) && OB_SUCC(result.get_result()->next())) {
        common::sqlclient::ObMySQLResult* rs = result.get_result();
        if (OB_ISNULL(rs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("system config result is null", K(ret));
        } else {
          ObString var_zone, var_svr_type, var_svr_ip, var_name, var_data_type;
          ObString var_value, var_info, var_section, var_scope, var_source, var_edit_level;
          int64_t var_config_version = 0, var_svr_port = 0;
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
          EXTRACT_INT_FIELD_MYSQL(*rs, "config_version", var_config_version, int64_t);
          max_version = std::max(max_version, var_config_version);
          if (OB_FAIL(sql.append_fmt("%s('%.*s', '%.*s', '%.*s', %ld, '%.*s', '%.*s', '%.*s',"
                                     "'%.*s', '%.*s', '%.*s', '%.*s', '%.*s', %ld)",
                  is_first ? " " : ", ",
                  var_zone.length(),
                  var_zone.ptr(),
                  var_svr_type.length(),
                  var_svr_type.ptr(),
                  var_svr_ip.length(),
                  var_svr_ip.ptr(),
                  var_svr_port,
                  var_name.length(),
                  var_name.ptr(),
                  var_data_type.length(),
                  var_data_type.ptr(),
                  var_value.length(),
                  var_value.ptr(),
                  var_info.length(),
                  var_info.ptr(),
                  var_section.length(),
                  var_section.ptr(),
                  var_scope.length(),
                  var_scope.ptr(),
                  var_source.length(),
                  var_source.ptr(),
                  var_edit_level.length(),
                  var_edit_level.ptr(),
                  var_config_version))) {
            LOG_WARN("sql append failed", K(ret));
          }
        }
        expected_rows++;
        is_first = false;
      }  // while

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (expected_rows > 0) {
          int64_t affected_rows = 0;
          if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", K(ret), K(sql));
          } else if (expected_rows != affected_rows) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected affected_rows", K(expected_rows), K(affected_rows));
          } else if (OB_FAIL(OTC_MGR.set_tenant_config_version(tenant_id, max_version))) {
            LOG_WARN("failed to set tenant config version", K(tenant_id), K(ret));
          }
        }
      } else {
        LOG_WARN("failed to get result from result set", K(ret));
      }
    }  // else
    LOG_INFO("init tenant config", K(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start);
  }
  return ret;
}

int ObDDLOperator::init_tenant_cgroup(const uint64_t tenant_id, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  ObResourceManagerProxy proxy;
  ObObj comments;
  for (int i = 0; i < ObPlanDirective::INTERNAL_GROUP_NAME_COUNT && OB_SUCC(ret); ++i) {
    const ObString& group_name = ObPlanDirective::INTERNAL_GROUP_NAME[i];
    if (OB_FAIL(proxy.create_consumer_group(trans, tenant_id, group_name, comments, i))) {
      LOG_WARN("fail create default consumer group", K(group_name), K(i), K(tenant_id), K(ret));
    }
  }
  LOG_INFO("init tenant cgroup", K(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::init_tenant_sys_stats(const uint64_t tenant_id, ObMySQLTransaction& trans)
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
  LOG_INFO("init sys stat", K(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObDDLOperator::replace_sys_stat(const uint64_t tenant_id, ObSysStat& sys_stat, ObISQLClient& trans)
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
    DLIST_FOREACH_X(it, sys_stat.item_list_, OB_SUCC(ret))
    {
      if (OB_ISNULL(it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("it is null", K(ret));
      } else {
        char buf[OB_MAX_CONFIG_VALUE_LEN] = "";
        int64_t pos = 0;
        if (OB_FAIL(it->value_.print_sql_literal(buf, OB_MAX_CONFIG_VALUE_LEN, pos))) {
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
                         it->name_,
                         it->value_.get_type(),
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
      } else if (sys_stat.item_list_.get_size() != affected_rows &&
                 sys_stat.item_list_.get_size() != affected_rows / 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected_rows", K(affected_rows), "expected", sys_stat.item_list_.get_size());
      }
    }
  }
  return ret;
}

int ObDDLOperator::init_sys_tenant_charset(ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const ObCharsetWrapper* charset_wrap_arr = NULL;
  int64_t charset_wrap_arr_len = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("insert into %s "
                             "(charset, description, default_collation, max_length) values ",
          OB_ALL_CHARSET_TNAME))) {
    LOG_WARN("sql append failed", K(ret));
  } else {
    ObCharset::get_charset_wrap_arr(charset_wrap_arr, charset_wrap_arr_len);
    if (OB_ISNULL(charset_wrap_arr) || OB_UNLIKELY(ObCharset::CHARSET_WRAPPER_COUNT != charset_wrap_arr_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("charset wrap array is NULL or charset_wrap_arr_len is not CHARSET_WRAPPER_COUNT",
          K(ret),
          K(charset_wrap_arr),
          K(charset_wrap_arr_len));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < charset_wrap_arr_len; ++i) {
        ObCharsetWrapper charset_wrap = charset_wrap_arr[i];
        if (OB_FAIL(sql.append_fmt("%s('%s', '%s', '%s', %ld)",
                (0 == i) ? "" : ", ",
                ObCharset::charset_name(charset_wrap.charset_),
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
      LOG_ERROR("unexpected affected_rows", K(affected_rows), "expected", (charset_wrap_arr_len));
    }
  }
  return ret;
}

int ObDDLOperator::init_sys_tenant_collation(ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const ObCollationWrapper* collation_wrap_arr = NULL;
  int64_t collation_wrap_arr_len = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("insert into %s "
                             "(collation, charset, id, `is_default`, is_compiled, sortlen) values ",
          OB_ALL_COLLATION_TNAME))) {
    LOG_WARN("sql append failed", K(ret));
  } else {
    ObCharset::get_collation_wrap_arr(collation_wrap_arr, collation_wrap_arr_len);
    if (OB_ISNULL(collation_wrap_arr) || OB_UNLIKELY(ObCharset::COLLATION_WRAPPER_COUNT != collation_wrap_arr_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("collation wrap array is NULL or collation_wrap_arr_len is not COLLATION_WRAPPER_COUNT",
          K(ret),
          K(collation_wrap_arr),
          K(collation_wrap_arr_len));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < collation_wrap_arr_len; ++i) {
        ObCollationWrapper collation_wrap = collation_wrap_arr[i];
        if (OB_FAIL(sql.append_fmt("%s('%s', '%s', %ld, '%s', '%s', %ld)",
                (0 == i) ? "" : ", ",
                ObCharset::collation_name(collation_wrap.collation_),
                ObCharset::charset_name(collation_wrap.charset_),
                collation_wrap.id_,
                (true == collation_wrap.default_) ? "Yes" : "",
                (true == collation_wrap.compiled_) ? "Yes" : "",
                collation_wrap.sortlen_))) {
          LOG_WARN("sql append failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("create collation sql", K(sql));
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (collation_wrap_arr_len != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected affected_rows", K(affected_rows), "expected", (collation_wrap_arr_len));
    }
  }
  return ret;
}

int ObDDLOperator::init_sys_tenant_privilege(ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;

  ObSqlString sql;
  int64_t row_count = 0;
  if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s "
                             "(Privilege, Context, Comment) values ",
          OB_ALL_PRIVILEGE_TNAME))) {
    LOG_WARN("sql append failed", K(ret));
  } else {
    const PrivilegeRow* current_row = all_privileges;
    if (OB_ISNULL(current_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current_row is null", K(ret));
    } else {
      bool is_first_row = true;
      for (; OB_SUCC(ret) && NULL != current_row && NULL != current_row->privilege_; ++current_row) {
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
    const share::schema::ObUserInfo& user_info, const ObString* ddl_stmt_str, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
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

int ObDDLOperator::drop_user(const uint64_t tenant_id, const uint64_t user_id, const common::ObString* ddl_stmt_str,
    common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama sql service and schema manager must not be null", K(schema_sql_service), K(ret));
  }
  // delete user
  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_sql_service->get_user_sql_service().drop_user(
                   tenant_id, user_id, new_schema_version, ddl_stmt_str, trans, schema_guard))) {
      LOG_WARN("Drop user from all user table error", K(tenant_id), K(user_id), K(ret));
    }
  }
  // delete db and table privileges of this user
  if (OB_SUCC(ret)) {
    if (OB_FAIL(drop_db_table_privs(tenant_id, user_id, trans))) {
      LOG_WARN("Drop db, table privileges of user error", K(tenant_id), K(user_id), K(ret));
    }
  }

  return ret;
}

int ObDDLOperator::drop_db_table_privs(
    const uint64_t tenant_id, const uint64_t user_id, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  int64_t ddl_count = 0;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama sql service and schema manager must not be null", K(schema_sql_service), K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  }
  // delete database privileges of this user
  if (OB_SUCC(ret)) {
    ObArray<const ObDBPriv*> db_privs;
    if (OB_FAIL(schema_guard.get_db_priv_with_user_id(tenant_id, user_id, db_privs))) {
      LOG_WARN("Get database privileges of user to be deleted error", K(tenant_id), K(user_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < db_privs.count(); ++i) {
        const ObDBPriv* db_priv = db_privs.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_ISNULL(db_priv)) {
          LOG_WARN("db priv is NULL", K(ret), K(db_priv));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().delete_db_priv(
                       db_priv->get_original_key(), new_schema_version, trans))) {
          LOG_WARN("Delete database privilege failed", "DB Priv", *db_priv, K(ret));
        }
      }
      ddl_count -= db_privs.count();
    }
  }
  // delete table privileges of this user MYSQL
  if (OB_SUCC(ret)) {
    ObArray<const ObTablePriv*> table_privs;
    if (OB_FAIL(schema_guard.get_table_priv_with_user_id(tenant_id, user_id, table_privs))) {
      LOG_WARN("Get table privileges of user to be deleted error", K(tenant_id), K(user_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_privs.count(); ++i) {
        const ObTablePriv* table_priv = table_privs.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_ISNULL(table_priv)) {
          LOG_WARN("table priv is NULL", K(ret), K(table_priv));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().delete_table_priv(
                       table_priv->get_sort_key(), new_schema_version, trans))) {
          LOG_WARN("Delete table privilege failed", "Table Priv", *table_priv, K(ret));
        }
      }
    }
  }

  // delete oracle table privileges of this user ORACLE
  if (OB_SUCC(ret)) {
    ObArray<const ObObjPriv*> obj_privs;

    OZ(schema_guard.get_obj_priv_with_grantee_id(tenant_id, user_id, obj_privs));
    OZ(schema_guard.get_obj_priv_with_grantor_id(tenant_id, user_id, obj_privs, false));
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_privs.count(); ++i) {
      const ObObjPriv* obj_priv = obj_privs.at(i);
      int64_t new_schema_version = OB_INVALID_VERSION;
      if (OB_ISNULL(obj_priv)) {
        LOG_WARN("obj_priv priv is NULL", K(ret), K(obj_priv));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().delete_obj_priv(
                     *obj_priv, new_schema_version, trans))) {
        LOG_WARN("Delete obj_priv privilege failed", "obj Priv", *obj_priv, K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::rename_user(const uint64_t tenant_id, const uint64_t user_id, const ObAccountArg& new_account,
    const common::ObString* ddl_stmt_str, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl must not null", "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const ObUserInfo* user_info = NULL;
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("failed to get user info", K(ret), K(tenant_id), K(user_id));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else {
      ObUserInfo new_user_info = *user_info;
      new_user_info.set_user_name(new_account.user_name_);
      new_user_info.set_host(new_account.host_name_);
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().rename_user(
                     new_user_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to rename user", K(tenant_id), K(user_id), K(new_account), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::set_passwd(const uint64_t tenant_id, const uint64_t user_id, const common::ObString& passwd,
    const ObString* ddl_stmt_str, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null", "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const ObUserInfo* user_info = NULL;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("failed to get user info", K(ret));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_user_info = *user_info;
      new_user_info.set_passwd(passwd);
      new_user_info.set_password_last_changed(ObTimeUtility::current_time());
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().set_passwd(
                     new_user_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to set passwd", K(tenant_id), K(user_id), K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::alter_user_default_role(const ObString& ddl_str, const ObUserInfo& schema,
    ObIArray<uint64_t>& role_id_array, ObIArray<uint64_t>& disable_flag_array, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_sql_service = NULL;
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
    if (OB_FAIL(schema_sql_service->get_priv_sql_service().alter_user_default_role(
            schema, new_schema_version, &ddl_str, role_id_array, disable_flag_array, trans))) {
      LOG_WARN("alter user default role failed", K(ret));
    }
  }

  LOG_DEBUG("alter_user_default_role", K(schema));
  return ret;
}

int ObDDLOperator::alter_user_profile(const ObString& ddl_str, ObUserInfo& schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_sql_service = NULL;
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

int ObDDLOperator::alter_user_require(const uint64_t tenant_id, const uint64_t user_id,
    const obrpc::ObSetPasswdArg& arg, const common::ObString* ddl_stmt_str, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null", "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const ObUserInfo* user_info = NULL;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("failed to get user info", K(ret));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_user_info = *user_info;
      new_user_info.set_ssl_type(arg.ssl_type_);
      new_user_info.set_ssl_cipher(arg.ssl_cipher_);
      new_user_info.set_x509_issuer(arg.x509_issuer_);
      new_user_info.set_x509_subject(arg.x509_subject_);
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().alter_user_require(
                     new_user_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to alter_user_require", K(tenant_id), K(user_id), K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::grant_revoke_user(const uint64_t tenant_id, const uint64_t user_id, const ObPrivSet priv_set,
    const bool grant, const ObString* ddl_stmt_str, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null", "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    ObPrivSet new_priv = priv_set;

    const ObUserInfo* user_info = NULL;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info)) || NULL == user_info) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else {
      if (grant) {
        new_priv = priv_set | user_info->get_priv_set();
      } else {
        new_priv = (~priv_set) & user_info->get_priv_set();
      }
      // no matter privilege change or not, write a sql
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_user_info = *user_info;
      new_user_info.set_priv_set(new_priv);
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().grant_revoke_user(
                     new_user_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to grant or revoke user", K(tenant_id), K(user_id), K(grant), K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::lock_user(const uint64_t tenant_id, const uint64_t user_id, const bool locked,
    const ObString* ddl_stmt_str, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id is invalid", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null", "schema_service_impl", schema_sql_service, K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    const ObUserInfo* user_info = NULL;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info)) || NULL == user_info) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("User not exist", K(ret));
    } else if (locked != user_info->get_is_locked()) {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_user_info = *user_info;
      new_user_info.set_is_locked(locked);
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().lock_user(
                     new_user_info, new_schema_version, ddl_stmt_str, trans))) {
        LOG_WARN("Failed to lock user", K(tenant_id), K(user_id), K(locked), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLOperator::grant_database(const ObOriginalDBKey& db_priv_key, const ObPrivSet priv_set,
    const ObString* ddl_stmt_str, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = db_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null", "schema_service_impl", schema_sql_service, K(ret));
  } else if (!db_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db_priv_key is invalid", K(db_priv_key), K(ret));
  } else if (0 == priv_set) {
    // do nothing
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
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_database(
                       db_priv_key, new_priv, new_schema_version, ddl_stmt_str, trans))) {
          LOG_WARN("Failed to grant database", K(db_priv_key), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::revoke_database(
    const ObOriginalDBKey& db_priv_key, const ObPrivSet priv_set, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = db_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null", "schema_service_impl", schema_sql_service, K(ret));
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
      // do nothing
    } else {
      ObPrivSet new_priv = db_priv_set & (~priv_set);
      if (db_priv_set & priv_set) {
        ObSqlString ddl_stmt_str;
        ObString ddl_sql;
        const ObUserInfo* user_info = NULL;
        ObNeedPriv need_priv;
        need_priv.db_ = db_priv_key.db_;
        need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
        need_priv.priv_set_ = db_priv_set & priv_set;  // priv to revoke
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(schema_guard.get_user_info(db_priv_key.user_id_, user_info))) {
          LOG_WARN("get user info failed", K(db_priv_key), K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user not exist", K(db_priv_key), K(ret));
        } else if (OB_FAIL(ObDDLSqlGenerator::gen_db_priv_sql(
                       ObAccountArg(user_info->get_user_name_str(), user_info->get_host_name_str()),
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
int ObDDLOperator::check_obj_privs_exists(ObSchemaGetterGuard& schema_guard,
    const schema::ObObjPrivSortKey& obj_priv_key, /* in: obj priv key */
    const ObRawObjPrivArray& obj_priv_array,      /* in: privs to be deleted */
    ObRawObjPrivArray& option_priv_array,         /* out: privs to be deleted cascade */
    bool& is_all)                                 /* out: obj priv array is all privs existed */
{
  int ret = OB_SUCCESS;
  ObPackedObjPriv obj_privs = 0;
  ObRawObjPriv raw_obj_priv = 0;
  bool exists = false;
  uint64_t option_out = false;
  is_all = false;
  int org_n = 0;
  OZ(schema_guard.get_obj_privs(obj_priv_key, obj_privs));
  for (int i = 0; i < obj_priv_array.count() && OB_SUCC(ret); i++) {
    raw_obj_priv = obj_priv_array.at(i);
    OZ(ObOraPrivCheck::raw_obj_priv_exists_with_info(raw_obj_priv, obj_privs, exists, option_out),
        raw_obj_priv,
        obj_privs,
        ret);
    if (OB_SUCC(ret)) {
      if (!exists) {
        ret = OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT;
      } else if (option_out == GRANT_OPTION) {
        OZ(option_priv_array.push_back(raw_obj_priv));
      }
    }
  }
  OZ(ObPrivPacker::get_total_obj_privs(obj_privs, org_n));
  OX(is_all = org_n == obj_priv_array.count());
  return ret;
}

/* According to the current obj priv, determine which ones need to be newly added priv array */
int ObDDLOperator::set_need_flush_ora(ObSchemaGetterGuard& schema_guard,
    const schema::ObObjPrivSortKey& obj_priv_key, /* in: obj priv key*/
    const uint64_t option,                        /* in: new option */
    const ObRawObjPrivArray& obj_priv_array,      /* in: new privs used want to add */
    ObRawObjPrivArray& new_obj_priv_array)        /* out: new privs actually to be added */
{
  int ret = OB_SUCCESS;
  ObPackedObjPriv obj_privs = 0;
  ObRawObjPriv raw_obj_priv = 0;
  bool exists = false;
  OZ(schema_guard.get_obj_privs(obj_priv_key, obj_privs));
  for (int i = 0; i < obj_priv_array.count() && OB_SUCC(ret); i++) {
    raw_obj_priv = obj_priv_array.at(i);
    OZ(ObOraPrivCheck::raw_obj_priv_exists(raw_obj_priv, option, obj_privs, exists),
        raw_obj_priv,
        option,
        obj_privs,
        ret);
    if (OB_SUCC(ret) && !exists) {
      OZ(new_obj_priv_array.push_back(raw_obj_priv));
    }
  }
  return ret;
}

/* Only handle authorization for one object, for example, one table, one column */
int ObDDLOperator::grant_table(const ObTablePrivSortKey& table_priv_key, const ObPrivSet priv_set,
    const ObString* ddl_stmt_str, common::ObMySQLTransaction& trans, const share::ObRawObjPrivArray& obj_priv_array,
    const uint64_t option, const share::schema::ObObjPrivSortKey& obj_priv_key)
{
  int ret = OB_SUCCESS;
  ObRawObjPrivArray new_obj_priv_array;
  const uint64_t tenant_id = table_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null", "schema_service_impl", schema_sql_service, K(ret));
  } else if (!table_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_priv_key is invalid", K(table_priv_key), K(ret));
  } else if (0 == priv_set && obj_priv_array.count() == 0) {
    // do nothing
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
      if (need_flush) {
        int64_t new_schema_version = OB_INVALID_VERSION;
        int64_t new_schema_version_ora = OB_INVALID_VERSION;
        if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (obj_priv_array.count() > 0) {
          OZ(set_need_flush_ora(schema_guard, obj_priv_key, option, obj_priv_array, new_obj_priv_array));
          if (new_obj_priv_array.count() > 0) {
            OZ(schema_service_.gen_new_schema_version(tenant_id, new_schema_version_ora));
          }
        }
        OZ(schema_sql_service->get_priv_sql_service().grant_table(table_priv_key,
               new_priv,
               new_schema_version,
               ddl_stmt_str,
               trans,
               new_obj_priv_array,
               option,
               obj_priv_key,
               new_schema_version_ora,
               true,
               false),
            table_priv_key,
            ret,
            false);
      } else if (obj_priv_array.count() > 0) {
        OZ(set_need_flush_ora(schema_guard, obj_priv_key, option, obj_priv_array, new_obj_priv_array));
        if (new_obj_priv_array.count() > 0) {
          int64_t new_schema_version_ora = OB_INVALID_VERSION;
          OZ(schema_service_.gen_new_schema_version(tenant_id, new_schema_version_ora));
          OZ(schema_sql_service->get_priv_sql_service().grant_table_ora_only(
                 ddl_stmt_str, trans, new_obj_priv_array, option, obj_priv_key, new_schema_version_ora, false, false),
              table_priv_key,
              ret);
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
int ObDDLOperator::build_table_and_col_priv_array_for_revoke_all(ObSchemaGetterGuard& schema_guard,
    const ObObjPrivSortKey& obj_priv_key, ObPackedObjPriv& packed_table_priv, ObSEArray<uint64_t, 4>& col_id_array,
    ObSEArray<ObPackedObjPriv, 4>& packed_privs_array)
{
  int ret = OB_SUCCESS;
  ObPackedObjPriv packed_obj_privs;
  ObSEArray<const ObObjPriv*, 4> obj_priv_array;
  uint64_t col_id;
  CK(obj_priv_key.is_valid());
  OZ(schema_guard.get_obj_privs_in_grantor_ur_obj_id(obj_priv_key.tenant_id_, obj_priv_key, obj_priv_array));
  for (int i = 0; i < obj_priv_array.count() && OB_SUCC(ret); i++) {
    const ObObjPriv* obj_priv = obj_priv_array.at(i);
    if (obj_priv != NULL) {
      col_id = obj_priv->get_col_id();
      if (col_id == COL_ID_FOR_TAB_PRIV) {
        packed_table_priv = obj_priv->get_obj_privs();
      } else {
        OZ(col_id_array.push_back(col_id));
        OZ(packed_privs_array.push_back(obj_priv->get_obj_privs()));
      }
    }
  }

  return ret;
}

int ObDDLOperator::revoke_table_all(ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
    const ObObjPrivSortKey& obj_priv_key, ObString& ddl_sql, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  share::ObPackedObjPriv packed_table_privs = 0;
  ObRawObjPrivArray raw_priv_array;
  ObRawObjPrivArray option_raw_array;
  ObSEArray<uint64_t, 4> col_id_array;
  ObSEArray<ObPackedObjPriv, 4> packed_privs_array;
  ObObjPrivSortKey new_key = obj_priv_key;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null", "schema_service_impl", schema_sql_service, K(ret));
  }
  OZ(build_table_and_col_priv_array_for_revoke_all(
      schema_guard, obj_priv_key, packed_table_privs, col_id_array, packed_privs_array));
  if (OB_SUCC(ret)) {
    // 1. table-level permissions
    if (packed_table_privs > 0) {
      OZ(ObPrivPacker::raw_obj_priv_from_pack(packed_table_privs, raw_priv_array));
      OZ(schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
      OZ(schema_sql_service->get_priv_sql_service().revoke_table_ora(
          new_key, raw_priv_array, new_schema_version, &ddl_sql, trans, true));
      OZ(ObPrivPacker::raw_option_obj_priv_from_pack(packed_table_privs, option_raw_array));
      OZ(revoke_obj_cascade(schema_guard, new_key.grantee_id_, trans, new_key, option_raw_array));
    }
    // 2. column-level permissions
    for (int i = 0; i < col_id_array.count() && OB_SUCC(ret); i++) {
      new_key.col_id_ = col_id_array.at(i);
      OZ(ObPrivPacker::raw_obj_priv_from_pack(packed_privs_array.at(i), raw_priv_array));
      OZ(schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
      OZ(schema_sql_service->get_priv_sql_service().revoke_table_ora(
          new_key, raw_priv_array, new_schema_version, &ddl_sql, trans, true));
      OZ(ObPrivPacker::raw_option_obj_priv_from_pack(packed_privs_array.at(i), option_raw_array));
      OZ(revoke_obj_cascade(schema_guard, new_key.grantee_id_, trans, new_key, option_raw_array));
    }
  }
  return ret;
}

int ObDDLOperator::build_next_level_revoke_obj(ObSchemaGetterGuard& schema_guard, const ObObjPrivSortKey& old_key,
    ObObjPrivSortKey& new_key, ObIArray<const ObObjPriv*>& obj_privs)
{
  int ret = OB_SUCCESS;
  new_key = old_key;
  new_key.grantor_id_ = new_key.grantee_id_;
  OZ(schema_guard.get_obj_privs_in_grantor_obj_id(new_key.tenant_id_, new_key, obj_privs));
  return ret;
}

/* After processing the top-level revoke obj, then call this function to process revoke recursively.
   1. According to the obj key of the upper layer, change grantee to grantor and find new permissions that need to be
   reclaimed
   2. If there are permissions that need to be reclaimed, call revoke obj ora, if not, end.
   3. calling self. If a new grantee is found back to the original grantee, the end */
int ObDDLOperator::revoke_obj_cascade(ObSchemaGetterGuard& schema_guard,
    const uint64_t start_grantee_id,                                    /* in: check circle */
    common::ObMySQLTransaction& trans, const ObObjPrivSortKey& old_key, /* in: old key */
    ObRawObjPrivArray& old_array)                                       /* in: privs that have grantable option */
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = old_key.tenant_id_;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  ObObjPrivSortKey new_key;
  ObRawObjPrivArray grantable_array;
  ObRawObjPrivArray new_array;
  ObSEArray<const ObObjPriv*, 4> obj_privs;
  bool is_all = false;
  if (old_array.count() > 0) {
    OZ(build_next_level_revoke_obj(schema_guard, old_key, new_key, obj_privs));
    /* If there are multiple, it means that this user has delegated to multiple other users */
    if (obj_privs.count() > 0) {
      ObPackedObjPriv old_p_list;
      ObPackedObjPriv old_opt_p_list;
      ObPackedObjPriv privs_revoke_this_level;
      ObPackedObjPriv privs_revoke_next_level;

      OZ(ObPrivPacker::pack_raw_obj_priv_list(NO_OPTION, old_array, old_p_list));
      OZ(ObPrivPacker::pack_raw_obj_priv_list(GRANT_OPTION, old_array, old_opt_p_list));

      for (int i = 0; OB_SUCC(ret) && i < obj_privs.count(); i++) {
        const ObObjPriv* obj_priv = obj_privs.at(i);
        if (obj_priv != NULL) {
          /* 1. cross join grantee privs and grantor privs without option */
          privs_revoke_this_level = old_p_list & obj_priv->get_obj_privs();

          if (privs_revoke_this_level > 0) {
            /* 2. build new_Key */
            new_key.grantee_id_ = obj_priv->get_grantee_id();
            /* 2. build new array */
            OZ(ObPrivPacker::raw_obj_priv_from_pack(privs_revoke_this_level, new_array));
            OZ(schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
            OX(is_all = (new_array.count() == old_array.count()));
            OZ(schema_sql_service->get_priv_sql_service().revoke_table_ora(
                new_key, new_array, new_schema_version, NULL, trans, is_all));
            /* 3. new grantee is equ org grantee. end */
            if (OB_SUCC(ret)) {
              if (new_key.grantee_id_ == start_grantee_id) {
              } else {
                /* 3. decide privs to be revoked recursively */
                privs_revoke_next_level = old_opt_p_list & obj_priv->get_obj_privs();
                if (privs_revoke_next_level > 0) {
                  OZ(ObPrivPacker::raw_obj_priv_from_pack(privs_revoke_next_level, new_array));
                  OZ(revoke_obj_cascade(schema_guard, start_grantee_id, trans, new_key, new_array));
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
int ObDDLOperator::build_fk_array_by_parent_table(uint64_t tenant_id, ObSchemaGetterGuard& schema_guard,
    const ObString& grantee_name, const ObString& db_name, const ObString& tab_name,
    ObIArray<ObDropForeignKeyArg>& drop_fk_array, ObIArray<uint64_t>& ref_tab_id_array)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;

  OZ(schema_guard.get_table_schema(tenant_id, db_name, tab_name, false, table_schema));
  if (OB_SUCC(ret)) {
    if (NULL == table_schema) {
      ret = OB_TABLE_NOT_EXIST;
    } else {
      uint64_t db_id = OB_INVALID_ID;
      OZ(schema_guard.get_database_id(tenant_id, grantee_name, db_id));
      /* Traverse all child tables referencing the parent table, if the owner of the child table is grantee, add drop fk
       * array */
      const ObIArray<ObForeignKeyInfo>& fk_array = table_schema->get_foreign_key_infos();
      for (int i = 0; OB_SUCC(ret) && i < fk_array.count(); i++) {
        const ObForeignKeyInfo& fk_info = fk_array.at(i);
        const ObSimpleTableSchemaV2* ref_table = NULL;
        OZ(schema_guard.get_table_schema(fk_info.child_table_id_, ref_table));
        if (OB_SUCC(ret)) {
          if (ref_table == NULL) {
            ret = OB_TABLE_NOT_EXIST;
          } else if (ref_table->get_database_id() == db_id) {
            ObDropForeignKeyArg fk_arg;
            fk_arg.foreign_key_name_ = fk_info.foreign_key_name_;
            OZ(drop_fk_array.push_back(fk_arg));
            OZ(ref_tab_id_array.push_back(ref_table->get_table_id()));
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::drop_fk_cascade(uint64_t tenant_id, ObSchemaGetterGuard& schema_guard, bool has_ref_priv,
    bool has_no_cascade, const ObString& grantee_name, const ObString& parent_db_name, const ObString& parent_tab_name,
    ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  if (has_ref_priv) {
    ObSEArray<ObDropForeignKeyArg, 4> drop_fk_array;
    ObSEArray<uint64_t, 4> ref_tab_id_array;
    OZ(build_fk_array_by_parent_table(
        tenant_id, schema_guard, grantee_name, parent_db_name, parent_tab_name, drop_fk_array, ref_tab_id_array));
    if (OB_SUCC(ret)) {
      if (drop_fk_array.count() > 0) {
        if (has_no_cascade) {
          ret = OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE;
        } else {
          for (int i = 0; OB_SUCC(ret) && i < drop_fk_array.count(); i++) {
            const ObTableSchema* ref_tab = NULL;
            const ObDropForeignKeyArg& drop_fk = drop_fk_array.at(i);

            OZ(schema_guard.get_table_schema(ref_tab_id_array.at(i), ref_tab));
            if (OB_SUCC(ret)) {
              if (ref_tab == NULL) {
                ret = OB_TABLE_NOT_EXIST;
              } else {
                OZ(alter_table_drop_foreign_key(*ref_tab, drop_fk, trans));
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLOperator::revoke_table(const ObTablePrivSortKey& table_priv_key, const ObPrivSet priv_set,
    common::ObMySQLTransaction& trans, const ObObjPrivSortKey& obj_priv_key,
    const share::ObRawObjPrivArray& obj_priv_array, const bool revoke_all_ora)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService* schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null", "schema_service_impl", schema_sql_service, K(ret));
  } else if (!table_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db_priv_key is invalid", K(table_priv_key), K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    ObPrivSet table_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(schema_guard.get_table_priv_set(table_priv_key, table_priv_set))) {
      LOG_WARN("get table priv set failed", K(ret));
    } else if (OB_PRIV_SET_EMPTY == table_priv_set && !revoke_all_ora && obj_priv_array.count() == 0) {
      ret = OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT;
      LOG_WARN("No such grant to revoke", K(table_priv_key), K(ret));
    } else if (0 == priv_set && obj_priv_array.count() == 0) {
      // do-nothing
    } else {
      ObPrivSet new_priv = table_priv_set & (~priv_set);
      /* If there is an intersection between the existing permissions and the permissions that require revoke */
      if (table_priv_set & priv_set) {
        ObSqlString ddl_stmt_str;
        ObString ddl_sql;
        const ObUserInfo* user_info = NULL;
        ObNeedPriv need_priv;
        share::ObRawObjPrivArray option_priv_array;

        need_priv.db_ = table_priv_key.db_;
        need_priv.table_ = table_priv_key.table_;
        need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
        need_priv.priv_set_ = table_priv_set & priv_set;  // priv to revoke
        int64_t new_schema_version = OB_INVALID_VERSION;
        int64_t new_schema_version_ora = OB_INVALID_VERSION;
        bool is_all = false;
        bool has_ref_priv = false;
        if (OB_FAIL(check_obj_privs_exists(schema_guard, obj_priv_key, obj_priv_array, option_priv_array, is_all))) {
          LOG_WARN("priv not exits", K(obj_priv_array), K(ret));
        } else if (OB_FAIL(schema_guard.get_user_info(table_priv_key.user_id_, user_info))) {
          LOG_WARN("get user info failed", K(table_priv_key), K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user not exist", K(table_priv_key), K(ret));
        } else if (OB_FAIL(drop_fk_cascade(tenant_id,
                       schema_guard,
                       has_ref_priv,
                       true,                           /* has no cascade */
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
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version_ora))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().revoke_table(table_priv_key,
                       new_priv,
                       new_schema_version,
                       &ddl_sql,
                       trans,
                       new_schema_version_ora,
                       obj_priv_key,
                       obj_priv_array,
                       is_all))) {
          LOG_WARN("Failed to revoke table", K(table_priv_key), K(ret));
        } else {
          OZ(revoke_obj_cascade(schema_guard, obj_priv_key.grantee_id_, trans, obj_priv_key, option_priv_array));
        }
        // In revoke all statement, if you have permission, it will come here, and the content of mysql will be
        // processed first.
        if (OB_SUCC(ret) && revoke_all_ora) {
          OZ(revoke_table_all(schema_guard, tenant_id, obj_priv_key, ddl_sql, trans));
        }
      } else {
        ObSqlString ddl_stmt_str;
        ObString ddl_sql;
        const ObUserInfo* user_info = NULL;
        int64_t new_schema_version = OB_INVALID_VERSION;
        share::ObRawObjPrivArray option_priv_array;
        bool is_all;
        // In oracle mode, you need to check whether the revoke permission exists, and only need to reclaim the oracle
        // permission
        OZ(check_obj_privs_exists(schema_guard, obj_priv_key, obj_priv_array, option_priv_array, is_all));
        OZ(schema_guard.get_user_info(table_priv_key.user_id_, user_info));
        if (OB_SUCC(ret) && user_info == NULL) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user not exist", K(table_priv_key), K(ret));
        }

        OZ(ObDDLSqlGenerator::gen_table_priv_sql_ora(
            ObAccountArg(user_info->get_user_name_str(), user_info->get_host_name_str()),
            table_priv_key,
            revoke_all_ora,
            obj_priv_array,
            false,
            ddl_stmt_str));
        OX(ddl_sql = ddl_stmt_str.string());
        if (OB_SUCC(ret)) {
          if (revoke_all_ora) {
            OZ(revoke_table_all(schema_guard, tenant_id, obj_priv_key, ddl_sql, trans));
          } else if (obj_priv_array.count() > 0) {
            OZ(schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
            OZ(schema_sql_service->get_priv_sql_service().revoke_table_ora(
                obj_priv_key, obj_priv_array, new_schema_version, &ddl_sql, trans, is_all));
            OZ(revoke_obj_cascade(schema_guard, obj_priv_key.grantee_id_, trans, obj_priv_key, option_priv_array));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::get_flush_role_array(const uint64_t option, const common::ObIArray<uint64_t>& org_role_ids,
    bool& need_flush, bool is_grant, const ObUserInfo& user_info, common::ObIArray<uint64_t>& role_ids)
{
  int ret = OB_SUCCESS;
  need_flush = false;
  if (org_role_ids.count() > 0) {
    if (is_grant) {
      for (int64_t i = 0; OB_SUCC(ret) && i < org_role_ids.count(); ++i) {
        const uint64_t role_id = org_role_ids.at(i);
        if (!user_info.role_exists(role_id, option)) {
          need_flush = true;
          OZ(role_ids.push_back(role_id));
        }
      }
    } else {
      need_flush = true;
      OZ(role_ids.assign(org_role_ids));
    }
  }
  return ret;
}

int ObDDLOperator::grant_revoke_role(const uint64_t tenant_id, const ObUserInfo& user_info,
    const common::ObIArray<uint64_t>& org_role_ids,
    // When specified_role_info is not empty, use it as role_info instead of reading it in the schema.
    const ObUserInfo* specified_role_info, common::ObMySQLTransaction& trans, const bool is_grant,
    const uint64_t option)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObString ddl_sql;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    common::ObSEArray<uint64_t, 8> role_ids;
    bool need_flush = false;
    OZ(get_flush_role_array(option, org_role_ids, need_flush, is_grant, user_info, role_ids));
    if (OB_SUCC(ret) && need_flush) {
      common::ObSqlString sql_string;
      if (OB_FAIL(sql_string.append_fmt(is_grant ? "GRANT " : "REVOKE "))) {
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
          const ObUserInfo* role_info = NULL;
          if (0 != i) {
            if (OB_FAIL(sql_string.append_fmt(","))) {
              LOG_WARN("append sql failed", K(ret));
            }
          }
          if (FAILEDx(schema_guard.get_user_info(role_id, role_info))) {
            LOG_WARN("Failed to get role info", K(ret), K(role_id));
          } else if (NULL == role_info) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("role doesn't exist", K(ret), K(role_id));
          } else if (OB_FAIL(sql_string.append_fmt("%s", role_info->get_user_name()))) {
            LOG_WARN("append sql failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql_string.append_fmt(is_grant ? " TO %s" : " FROM %s", user_info.get_user_name()))) {
          LOG_WARN("append sql failed", K(ret));
        } else if (is_grant && option != NO_OPTION && OB_FAIL(sql_string.append_fmt(" WITH ADMIN OPTION"))) {
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
                &ddl_sql,
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

int ObDDLOperator::get_flush_priv_array(const uint64_t option, const share::ObRawPrivArray& priv_array,
    const ObSysPriv* sys_priv, share::ObRawPrivArray& new_priv_array, bool& need_flush, const bool is_grant,
    const ObUserInfo& user_info)
{
  int ret = OB_SUCCESS;
  int64_t raw_priv;
  bool exists = false;

  need_flush = FALSE;
  if (is_grant) {
    if (sys_priv == NULL) {
      need_flush = true;
      OZ(new_priv_array.assign(priv_array));
    } else {
      ARRAY_FOREACH(priv_array, idx)
      {
        raw_priv = priv_array.at(idx);
        OZ(ObOraPrivCheck::raw_sys_priv_exists(option, raw_priv, sys_priv->get_priv_array(), exists));
        if (OB_SUCC(ret) && !exists) {
          need_flush = true;
          OZ(new_priv_array.push_back(raw_priv));
        }
      }
    }
  } else {
    need_flush = true;
    if (sys_priv == NULL) {
      ret = OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO;
      LOG_USER_ERROR(OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO,
          user_info.get_user_name_str().length(),
          user_info.get_user_name_str().ptr());
      LOG_WARN("revoke sys priv fail, sys priv not exists", K(priv_array), K(ret));
    } else {
      ARRAY_FOREACH(priv_array, idx)
      {
        raw_priv = priv_array.at(idx);
        OZ(ObOraPrivCheck::raw_sys_priv_exists(option, raw_priv, sys_priv->get_priv_array(), exists));
        if (OB_SUCC(ret) && !exists) {
          ret = OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO;
          LOG_USER_ERROR(OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO,
              user_info.get_user_name_str().length(),
              user_info.get_user_name_str().ptr());
          LOG_WARN("revoke sys priv fail, sys priv not exists", K(priv_array), K(ret));
        }
        OZ(new_priv_array.push_back(raw_priv));
      }
    }
  }
  return ret;
}

int ObDDLOperator::grant_sys_priv_to_ur(const uint64_t tenant_id, const uint64_t grantee_id, const ObSysPriv* sys_priv,
    const uint64_t option, const ObRawPrivArray priv_array, common::ObMySQLTransaction& trans, const bool is_grant,
    const common::ObString* ddl_stmt_str, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObRawPrivArray new_priv_array;
  bool need_flush;
  const ObUserInfo* user_info = NULL;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  }
  OZ(schema_service_.gen_new_schema_version(tenant_id, new_schema_version), tenant_id);
  OZ(schema_guard.get_user_info(grantee_id, user_info));
  OZ(get_flush_priv_array(option, priv_array, sys_priv, new_priv_array, need_flush, is_grant, *user_info));
  if (OB_SUCC(ret) && need_flush) {
    if (is_grant) {
      CK(new_priv_array.count() > 0);
      OZ(schema_service->get_priv_sql_service().grant_sys_priv_to_ur(
          tenant_id, grantee_id, option, new_priv_array, new_schema_version, ddl_stmt_str, trans, is_grant, false));
    } else {
      int n_cnt = 0;
      bool revoke_all_flag = false;
      /* revoke */
      CK(sys_priv != NULL);
      OZ(ObPrivPacker::get_total_privs(sys_priv->get_priv_array(), n_cnt));
      revoke_all_flag = (n_cnt == new_priv_array.count());
      /* revoke all */
      OZ(schema_service->get_priv_sql_service().grant_sys_priv_to_ur(tenant_id,
             grantee_id,
             option,
             new_priv_array,
             new_schema_version,
             ddl_stmt_str,
             trans,
             is_grant,
             revoke_all_flag),
          tenant_id,
          grantee_id,
          new_priv_array,
          is_grant,
          revoke_all_flag);
    }
  }
  return ret;
}

//----End of functions for managing privileges----

//----Functions for managing outlines----
int ObDDLOperator::create_outline(
    ObOutlineInfo& outline_info, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  uint64_t new_outline_id = OB_INVALID_ID;
  const uint64_t tenant_id = outline_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  }  // else if (!outline_info.is_valid()) {
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
    if (OB_FAIL(schema_service->get_outline_sql_service().insert_outline(outline_info, trans, ddl_stmt_str))) {
      LOG_WARN("insert outline info failed", K(outline_info), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::replace_outline(
    ObOutlineInfo& outline_info, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = outline_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    outline_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_outline_sql_service().replace_outline(outline_info, trans, ddl_stmt_str))) {
      LOG_WARN("replace outline info failed", K(outline_info), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDDLOperator::alter_outline(
    ObOutlineInfo& outline_info, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = outline_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    outline_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_outline_sql_service().alter_outline(outline_info, trans, ddl_stmt_str))) {
      LOG_WARN("alter outline failed", K(outline_info), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDDLOperator::drop_outline(const uint64_t tenant_id, const uint64_t database_id, const uint64_t outline_id,
    ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || OB_INVALID_ID == outline_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_id), K(outline_id), K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_outline_sql_service().delete_outline(
                 tenant_id, database_id, outline_id, new_schema_version, trans, ddl_stmt_str))) {
    LOG_WARN("drop outline failed", K(tenant_id), K(outline_id), KT(outline_id), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

//----Functions for managing dblinks----
int ObDDLOperator::create_dblink(
    ObDbLinkBaseInfo& dblink_info, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
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

int ObDDLOperator::drop_dblink(
    ObDbLinkBaseInfo& dblink_info, ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
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
  } else if (OB_FAIL(schema_service->get_dblink_sql_service().delete_dblink(tenant_id, dblink_id, trans))) {
    LOG_WARN("failed to delete dblink", K(dblink_info.get_dblink_name()), K(ret));
  }
  return ret;
}
//----End of functions for managing dblinks----

//----Functions for managing synonym----
int ObDDLOperator::create_synonym(
    ObSynonymInfo& synonym_info, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  uint64_t new_synonym_id = OB_INVALID_ID;
  const uint64_t tenant_id = synonym_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
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
    if (OB_FAIL(schema_service->get_synonym_sql_service().insert_synonym(synonym_info, &trans, ddl_stmt_str))) {
      LOG_WARN("insert synonym info failed", K(synonym_info.get_synonym_name_str()), K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::replace_synonym(
    ObSynonymInfo& synonym_info, ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = synonym_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    synonym_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_synonym_sql_service().replace_synonym(synonym_info, &trans, ddl_stmt_str))) {
      LOG_WARN("replace synonym info failed", K(synonym_info.get_synonym_name_str()), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDDLOperator::drop_synonym(const uint64_t tenant_id, const uint64_t database_id, const uint64_t synonym_id,
    ObMySQLTransaction& trans, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == synonym_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_id), K(synonym_id), K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_synonym_sql_service().delete_synonym(
                 tenant_id, database_id, synonym_id, new_schema_version, &trans, ddl_stmt_str))) {
    LOG_WARN("drop synonym failed", K(tenant_id), K(synonym_id), KT(synonym_id), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

//----End of functions for managing outlines----

/* [data_table_prefix]_[data_table_id]_RECYCLE_[object_type_prefix]_[timestamp].
 * data_table_prefix: if not null, use '[data_table_prefix]_[data_table_id]' as prefix,
 *                    otherwise, skip this part.
 * object_type_prefix: OBIDX / OBCHECK / OBTRG ...
 */
template <typename SchemaType>
int ObDDLOperator::build_flashback_object_name(const SchemaType& object_schema, const char* data_table_prefix,
    const char* object_type_prefix, ObSchemaGetterGuard& schema_guard, ObIAllocator& allocator, ObString& object_name)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  int64_t saved_pos = 0;
  bool object_exist = true;
  static const char* RECYCLE = "RECYCLE";
  static const int64_t INT64_STR_LEN = 20;
  OV(OB_NOT_NULL(object_type_prefix));
  if (OB_NOT_NULL(data_table_prefix)) {
    OX(buf_len += (STRLEN(data_table_prefix) + 1 +  // [data_table_prefix]_
                   INT64_STR_LEN + 1));             // [data_table_id]_
  }
  OX(buf_len += (STRLEN(RECYCLE) + 1 +             // RECYCLE_
                 STRLEN(object_type_prefix) + 1 +  // [object_type_prefix]_
                 INT64_STR_LEN));                  // [timestamp]
  OV(OB_NOT_NULL(buf = static_cast<char*>(allocator.alloc(buf_len))), OB_ALLOCATE_MEMORY_FAILED, buf_len);
  if (OB_NOT_NULL(data_table_prefix)) {
    OZ(BUF_PRINTF("%s_%lu_", data_table_prefix, object_schema.get_data_table_id()));
  }
  OZ(BUF_PRINTF("%s_%s_", RECYCLE, object_type_prefix));
  OX(saved_pos = pos);
  while (OB_SUCC(ret) && object_exist) {
    OX(pos = saved_pos);
    OZ(BUF_PRINTF("%ld", ObTimeUtility::current_time()));
    OX(object_name.assign(buf, static_cast<int32_t>(pos)));
    OZ(schema_guard.check_flashback_object_exist(object_schema, object_name, object_exist));
  }
  return ret;
}

//----Functions for managing UDF----
int ObDDLOperator::create_user_defined_function(
    share::schema::ObUDF& udf_info, common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  uint64_t new_udf_id = OB_INVALID_ID;
  const uint64_t tenant_id = udf_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
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

int ObDDLOperator::drop_user_defined_function(const uint64_t tenant_id, const common::ObString& name,
    common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must exist", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_udf_sql_service().delete_udf(
                 tenant_id, name, new_schema_version, &trans, ddl_stmt_str))) {
    LOG_WARN("drop udf failed", K(tenant_id), K(name), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}
//----End of functions for managing UDF----

int ObDDLOperator::split_partition_finish(const ObTableSchema& table_schema, const ObSplitPartitionArg& arg,
    const share::ObSplitProgress split_status, common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  int64_t table_id = table_schema.get_table_id();
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  bool need_process_dest_partition = false;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (PHYSICAL_SPLIT_FINISH != split_status && LOGICAL_SPLIT_FINISH != split_status) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("split status not right", K(ret), K(split_status));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("valid schema service", K(ret));
  } else {
    ObTableSchema copy_schema;
    if (OB_FAIL(copy_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else if (PHYSICAL_SPLIT_FINISH == split_status) {
      need_process_dest_partition = true;
      copy_schema.set_partition_status(PARTITION_STATUS_ACTIVE);
    } else {
      copy_schema.set_partition_status(PARTITION_STATUS_PHYSICAL_SPLITTING);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service->get_table_sql_service().update_partition_option(
                   trans, copy_schema, new_schema_version))) {
      LOG_WARN("fail to update table status", K(ret), K(table_id));
    } else if (need_process_dest_partition) {
      if (OB_FAIL(schema_service->get_table_sql_service().modify_dest_partition(
              trans, table_schema, arg, new_schema_version))) {
        LOG_WARN("fail to split partition finish", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = table_schema.get_tenant_id();
      opt.database_id_ = table_schema.get_database_id();
      opt.tablegroup_id_ = table_schema.get_tablegroup_id();
      opt.table_id_ = table_schema.get_table_id();
      if (PHYSICAL_SPLIT_FINISH == split_status) {
        opt.op_type_ = OB_DDL_FINISH_SPLIT;
      } else {
        opt.op_type_ = OB_DDL_FINISH_LOGICAL_SPLIT;
      }
      opt.schema_version_ = new_schema_version;
      if (OB_FAIL(schema_service->get_table_sql_service().log_operation_wrapper(opt, trans))) {
        LOG_WARN("fail to log operation", K(ret));
      }
    }
  }

  return ret;
}

int ObDDLOperator::split_tablegroup_partitions(const ObTablegroupSchema& origin_tablegroup_schema,
    const ObTablegroupSchema& new_tablegroup_schema, const ObSplitInfo& split_info, ObMySQLTransaction& trans,
    const ObString* ddl_str, ObSchemaOperationType opt_type)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = origin_tablegroup_schema.get_tenant_id();
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  bool is_delay_delete = false;
  ObArray<ObPartition> split_part_array;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is NULL", K(ret));
  } else if (!is_new_tablegroup_id(origin_tablegroup_schema.get_tablegroup_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't split partition while tablegroup is created before ver 2.0", K(ret), K(origin_tablegroup_schema));
  } else if (OB_FAIL(check_is_delay_delete(tenant_id, is_delay_delete))) {
    LOG_WARN("check is delay delete failed", K(ret), K(tenant_id));
  } else if (is_delay_delete) {
    ObString delay_deleted_name;
    for (int64_t i = 0; OB_SUCC(ret) && i < split_info.source_part_ids_.count(); ++i) {
      bool check_dropped_schema = false;
      ObPartIteratorV2 iter(origin_tablegroup_schema, check_dropped_schema);
      const ObPartition* part = NULL;
      ObPartition tmp_part;
      while (OB_SUCC(ret) && OB_SUCC(iter.next(part))) {
        if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(part));
        } else if (part->get_part_id() == split_info.source_part_ids_.at(i)) {
          break;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("find part failed", K(ret));
      } else if (OB_FAIL(tmp_part.assign(*part))) {
        LOG_WARN("assign part failed", K(ret));
      } else if (OB_FAIL(create_new_name_for_delay_delete_tablegroup_partition(
                     delay_deleted_name, &origin_tablegroup_schema, allocator))) {
        LOG_WARN("fail to create new name for delay delete", K(ret));
      } else if (OB_FAIL(tmp_part.set_part_name(delay_deleted_name))) {
        LOG_WARN("set table name failed", K(ret), K(delay_deleted_name));
      } else if (OB_FAIL(split_part_array.push_back(tmp_part))) {
        LOG_WARN("push back to split_part_array failed", K(ret), K(delay_deleted_name));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service->get_table_sql_service().split_part_info(trans,
                 new_tablegroup_schema,
                 new_tablegroup_schema.get_schema_version(),
                 split_info,
                 new_tablegroup_schema.get_binding()))) {
    LOG_WARN("fail to split partition info", K(ret));
  } else if (1 != new_tablegroup_schema.get_partition_num() &&
             OB_FAIL(schema_service->get_table_sql_service().remove_source_partition(trans,
                 split_info,
                 new_tablegroup_schema.get_schema_version(),
                 &origin_tablegroup_schema,
                 NULL,
                 is_delay_delete,
                 split_part_array))) {
    LOG_WARN("fail to remove source partition", K(ret));
  } else if (OB_FAIL(schema_service->get_tablegroup_sql_service().update_partition_option(
                 trans, new_tablegroup_schema, opt_type, ddl_str))) {
    LOG_WARN("fail to update partition option", K(ret));
  } else {
    LOG_INFO("update partition option success", K(ret), K(new_tablegroup_schema));
  }
  return ret;
}

int ObDDLOperator::split_tablegroup_partition_finish(
    const ObTablegroupSchema& tablegroup_schema, const share::ObSplitProgress split_status, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  ObSchemaGetterGuard schema_guard;
  uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  bool need_process_dest_partition = false;
  if (!tablegroup_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablegroup_schema));
  } else if (PHYSICAL_SPLIT_FINISH != split_status && LOGICAL_SPLIT_FINISH != split_status) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("split status not right", K(ret), K(split_status));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("valid schema service", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    ObTablegroupSchema copy_schema = tablegroup_schema;
    copy_schema.set_schema_version(new_schema_version);
    ObPartitionStatus partition_status;
    ObSchemaOperationType type;
    if (PHYSICAL_SPLIT_FINISH == split_status) {
      need_process_dest_partition = true;
      partition_status = PARTITION_STATUS_ACTIVE;
      type = OB_DDL_FINISH_SPLIT_TABLEGROUP;
    } else {
      partition_status = PARTITION_STATUS_PHYSICAL_SPLITTING;
      type = OB_DDL_FINISH_LOGICAL_SPLIT_TABLEGROUP;
    }
    copy_schema.set_partition_status(partition_status);
    ObTablegroupSchema copy_tablegroup_schema = tablegroup_schema;
    copy_tablegroup_schema.set_schema_version(new_schema_version);
    ObArray<const ObTableSchema*> table_schemas;
    if (OB_FAIL(schema_service->get_tablegroup_sql_service().update_partition_option_without_log(trans, copy_schema))) {
      LOG_WARN("fail to update table status", K(ret), K(tablegroup_id));
    } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tablegroup", K(ret));
    } else if (0 < table_schemas.count() &&
               OB_FAIL(schema_service->get_table_sql_service().batch_update_partition_option(
                   trans, tenant_id, table_schemas, new_schema_version, partition_status))) {
      LOG_WARN("fail to batch update table schema", K(ret));
    } else if (need_process_dest_partition) {
      if (OB_FAIL(schema_service->get_tablegroup_sql_service().modify_dest_partition(trans, copy_tablegroup_schema))) {
        LOG_WARN("fail to modify dest partition", K(ret), K(tablegroup_schema));
      } else if (0 < table_schemas.count() &&
                 OB_FAIL(schema_service->get_table_sql_service().batch_modify_dest_partition(
                     trans, tenant_id, table_schemas, new_schema_version))) {
        LOG_WARN("fail to batch modify dest partition", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_service->get_tablegroup_sql_service().ddl_log(trans, type, copy_tablegroup_schema))) {
      LOG_WARN("fail to log operation", K(ret));
    } else {
      LOG_INFO(
          "split tablegroup partition logical or physical finished", K(ret), K(split_status), K(tablegroup_schema));
    }
  }

  return ret;
}

int ObDDLOperator::batch_update_max_used_part_id(ObISQLClient& trans, ObSchemaGetterGuard& schema_guard,
    const int64_t schema_version, const ObTablegroupSchema& tablegroup_schema, const ObAlterTablegroupArg& arg)
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema*> table_schemas;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  uint64_t tg_id = tablegroup_schema.get_tablegroup_id();
  ObTablegroupSchema copy_schema = tablegroup_schema;
  copy_schema.set_schema_version(schema_version);
  if (!tablegroup_schema.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tg_id, table_schemas))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(tg_id));
  } else if (OB_FAIL(schema_service->get_table_sql_service().batch_update_max_used_part_id(
                 trans, tenant_id, schema_version, table_schemas, arg.alter_tablegroup_schema_))) {
    LOG_WARN("fail to update max used part id", K(ret));
  } else if (OB_FAIL(schema_service->get_tablegroup_sql_service().update_max_used_part_id(
                 trans, schema_version, tablegroup_schema, arg.alter_tablegroup_schema_))) {
    LOG_WARN("fail to update max used part_id", K(ret), K(tablegroup_schema));
  } else if (OB_FAIL(schema_service->get_tablegroup_sql_service().ddl_log(
                 trans, OB_DDL_ALTER_TABLEGROUP_PARTITION, copy_schema))) {
    LOG_WARN("fail to record log", K(ret));
  } else {
    LOG_INFO("batch update max part id success", K(arg));
  }
  return ret;
}

int ObDDLOperator::insert_ori_schema_version(
    ObMySQLTransaction& trans, const uint64_t table_id, const int64_t& ori_schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_VERSION == ori_schema_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema version", K(ori_schema_version));
  } else {
    ObSchemaService* schema_service = schema_service_.get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid schema service", K(ret));
    } else if (OB_FAIL(schema_service->get_table_sql_service().insert_ori_schema_version(
                   trans, table_id, ori_schema_version))) {
      LOG_WARN("insert_ori_schema_version failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::insert_temp_table_info(ObMySQLTransaction& trans, const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (false == table_schema.is_tmp_table()) {
    // do nothing...
  } else if (is_inner_table(table_schema.get_table_id())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("create tmp sys table not allowed", K(ret), "table_id", table_schema.get_table_id());
  } else if (OB_FAIL(schema_service->get_table_sql_service().insert_temp_table_info(trans, table_schema))) {
    LOG_WARN("insert_temp_table_info failed", K(ret));
  }
  return ret;
}

int ObDDLOperator::check_is_change_column_type(const share::schema::ObColumnSchemaV2& src_column,
    const share::schema::ObColumnSchemaV2& dst_column, bool& is_change_column_type)
{
  int ret = OB_SUCCESS;
  is_change_column_type = src_column.get_data_type() != dst_column.get_data_type();
  LOG_INFO("check is change column type", K(is_change_column_type), K(src_column), K(dst_column));
  return ret;
}

int ObDDLOperator::check_column_in_index(
    const uint64_t column_id, const share::schema::ObTableSchema& table_schema, bool& is_in_index)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  ObSchemaGetterGuard schema_guard;
  ObArray<ObColDesc> column_ids;
  is_in_index = false;
  if (OB_UNLIKELY(!table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_schema));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(table_schema.get_rowkey_column_ids(column_ids))) {
    LOG_WARN("fail to get rowkey column ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count() && !is_in_index; ++i) {
      if (column_id == column_ids.at(i).col_id_) {
        is_in_index = true;
        LOG_WARN("column in data table rowkey columns", K(column_id));
      }
    }
    if (OB_SUCC(ret) && !is_in_index) {
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
        LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count() && !is_in_index; ++i) {
        const ObTableSchema* index_table_schema = NULL;
        if (OB_FAIL(schema_guard.get_table_schema(simple_index_infos.at(i).table_id_, index_table_schema))) {
          LOG_WARN("fail to get table schema", K(ret), "table id", simple_index_infos.at(i).table_id_);
        } else if (OB_ISNULL(index_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("index table schema must not be NULL", K(ret));
        } else {
          column_ids.reuse();
          if (OB_FAIL(index_table_schema->get_column_ids(column_ids))) {
            LOG_WARN("fail to get column ids", K(ret));
          }
          for (int64_t j = 0; OB_SUCC(ret) && !is_in_index && j < column_ids.count(); ++j) {
            if (column_id == column_ids.at(j).col_id_) {
              is_in_index = true;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !is_in_index) {
      column_ids.reuse();
      if (OB_FAIL(table_schema.get_column_ids(column_ids))) {
        LOG_WARN("fail to get column ids", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !is_in_index && j < column_ids.count(); ++j) {
          const ObColumnSchemaV2* column_schema = NULL;
          if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_ids.at(j).col_id_))) {
            ret = OB_SCHEMA_ERROR;
            LOG_WARN("column schema must not be NULL", K(ret), K(column_ids.at(j)));
          } else if (column_schema->is_generated_column()) {
            ObArray<uint64_t> ref_column_ids;
            if (OB_FAIL(column_schema->get_cascaded_column_ids(ref_column_ids))) {
              LOG_WARN("fail to get cascade column ids", K(ret));
            } else {
              is_in_index = has_exist_in_array(ref_column_ids, column_id);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLOperator::check_can_alter_column_type(const share::schema::ObColumnSchemaV2& src_column,
    const share::schema::ObColumnSchemaV2& dst_column, const share::schema::ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  bool is_change_column_type = false;
  bool is_in_index = false;
  if (OB_FAIL(check_is_change_column_type(src_column, dst_column, is_change_column_type))) {
    LOG_WARN("fail to check is change column type", K(ret), K(src_column), K(dst_column));
  } else if (is_change_column_type) {
    if (OB_FAIL(check_column_in_index(src_column.get_column_id(), table_schema, is_in_index))) {
      LOG_WARN("fail to check column is in index table", K(ret));
    } else if (is_in_index) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cannot modify column in index table", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::drop_inner_generated_index_column(ObMySQLTransaction& trans, ObSchemaGetterGuard& schema_guard,
    const ObTableSchema& index_schema, ObTableSchema& new_data_table_schema)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* data_table = NULL;
  const ObColumnSchemaV2* index_col = NULL;
  uint64_t data_table_id = index_schema.get_data_table_id();
  const ObIndexInfo& index_info = index_schema.get_index_info();
  if (OB_FAIL(schema_guard.get_table_schema(data_table_id, data_table))) {
    LOG_WARN("get table schema failed", K(data_table_id));
  } else if (OB_ISNULL(data_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table schema is unknown", K(data_table_id));
  } else if (!new_data_table_schema.is_valid()) {
    if (OB_FAIL(new_data_table_schema.assign(*data_table))) {
      LOG_WARN("fail to assign schema", K(ret));
    }
  }
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (OB_FAIL(new_data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_info.get_size(); ++i) {
    // Generated columns on index table are converted to normal column,
    // we need to get column schema from data table here.
    if (OB_ISNULL(index_col = data_table->get_column_schema(index_info.get_column(i)->column_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get index column schema failed", K(ret));
    } else if (index_col->is_hidden() && index_col->is_generated_column()) {
      // It is necessary to delete the generated column generated internally when the index is created. This type of
      // generated column is hidden.
      bool exist_index = false;
      for (int64_t j = 0; OB_SUCC(ret) && !exist_index && j < simple_index_infos.count(); ++j) {
        const ObColumnSchemaV2* tmp_col = NULL;
        if (simple_index_infos.at(j).table_id_ != index_schema.get_table_id()) {
          // If there are other indexes on the hidden column, they cannot be deleted, so you need to check if there are
          // other indexes
          if (OB_FAIL(schema_guard.get_column_schema(
                  simple_index_infos.at(j).table_id_, index_col->get_column_id(), tmp_col))) {
            LOG_WARN("get column schema from schema guard failed",
                K(simple_index_infos.at(j).table_id_),
                K(index_col->get_column_id()));
          } else if (tmp_col != NULL) {
            exist_index = true;
          }
        }
      }
      // No other indexes exist, delete hidden columns
      if (OB_SUCC(ret) && !exist_index) {
        // Generate column if it is not the last column
        // 1. Update prev_column_id
        // 2. Update internal table
        ObSchemaService* schema_service = schema_service_.get_schema_service();
        if (OB_FAIL(update_prev_id_for_delete_column(
                *data_table, new_data_table_schema, *index_col, *schema_service, trans))) {
          LOG_WARN("failed to update column previous id for delete column", K(ret));
        } else if (OB_FAIL(delete_single_column(trans, new_data_table_schema, index_col->get_column_name_str()))) {
          LOG_WARN("delete index inner generated column failed", K(new_data_table_schema), K(*index_col));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(alter_table_options(schema_guard, new_data_table_schema, *data_table, false, trans))) {
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

int ObDDLOperator::handle_profile_function(ObProfileSchema& schema, ObMySQLTransaction& trans,
    share::schema::ObSchemaOperationType ddl_type, const ObString& ddl_stmt_str, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  ObSchemaService* schema_sql_service = NULL;
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
    const ObProfileSchema* old_schema = NULL;
    // check if the schema is already existed
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard.get_profile_schema_by_name(tenant_id, schema.get_profile_name_str(), old_schema))) {
        LOG_WARN("fail to check schema exist", K(ret));
      } else {
        is_schema_exist = (old_schema != NULL);
      }
    }
    // other stuff case by case
    if (OB_SUCC(ret)) {
      uint64_t profile_id = OB_INVALID_ID;
      switch (ddl_type) {
        case OB_DDL_CREATE_PROFILE:
          if (OB_UNLIKELY(is_schema_exist)) {
            ret = OB_ERR_PROFILE_STRING_ALREADY_EXISTS;
            LOG_USER_ERROR(OB_ERR_PROFILE_STRING_ALREADY_EXISTS,
                schema.get_profile_name_str().length(),
                schema.get_profile_name_str().ptr());
          } else if (schema.get_profile_id() == combine_id(tenant_id, OB_ORACLE_TENANT_INNER_PROFILE_ID)) {
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
            LOG_USER_ERROR(OB_ERR_PROFILE_STRING_DOES_NOT_EXIST,
                schema.get_profile_name_str().length(),
                schema.get_profile_name_str().ptr());
          } else {
            schema.set_profile_id(old_schema->get_profile_id());
            if (schema.get_failed_login_attempts() < 0) {
              schema.set_failed_login_attempts(old_schema->get_failed_login_attempts());
            }
            if (schema.get_password_lock_time() < 0) {
              schema.set_password_lock_time(old_schema->get_password_lock_time());
            }
            if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2276) {
              if (schema.get_password_life_time() > 0) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "Password_life_time of profile");
              } else if (schema.get_password_grace_time() > 0) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "Password_grace_time of profile");
              }
            } else {
              if (schema.get_password_life_time() < 0) {
                schema.set_password_life_time(old_schema->get_password_life_time());
              }
              if (schema.get_password_grace_time() < 0) {
                schema.set_password_grace_time(old_schema->get_password_grace_time());
              }
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
    if (OB_FAIL(
            schema_sql_service->get_profile_sql_service().apply_new_schema(schema, trans, ddl_type, ddl_stmt_str))) {
      LOG_WARN("apply profile failed", K(ret));
    }
  }

  LOG_DEBUG("ddl operator", K(schema));
  return ret;
}

int ObDDLOperator::init_tenant_profile(
    int64_t tenant_id, const share::schema::ObSysVariableSchema& sys_variable, common::ObMySQLTransaction& trans)
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
    profile_schema.set_profile_id(combine_id(tenant_id, OB_ORACLE_TENANT_INNER_PROFILE_ID));
    profile_schema.set_password_lock_time(1);
    profile_schema.set_failed_login_attempts(10000000);
    profile_schema.set_password_life_time(INT64_MAX);
    profile_schema.set_password_grace_time(INT64_MAX);
    profile_schema.set_password_verify_function("NULL");
    profile_schema.set_profile_name("DEFAULT");
    ObSchemaService* schema_service = schema_service_.get_schema_service();
    int64_t new_schema_version = OB_INVALID_VERSION;
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid schema service", K(ret));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (FALSE_IT(profile_schema.set_schema_version(new_schema_version))) {
    } else if (OB_FAIL(schema_service->get_profile_sql_service().apply_new_schema(
                   profile_schema, trans, ObSchemaOperationType::OB_DDL_CREATE_PROFILE, NULL))) {
      LOG_WARN("create profile failed", K(ret));
    }
  }
  return ret;
}

int ObDDLOperator::update_table_schema_version(
    common::ObMySQLTransaction& trans, const share::schema::ObTableSchema& new_table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  if (!new_table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_table_schema));
  } else if (OB_FAIL(schema_service->get_table_sql_service().update_table_schema_version(
                 trans, new_table_schema, OB_DDL_UPDATE_TABLE_SCHEMA_VERSION, NULL))) {
    LOG_WARN("fail to update table schema version", KR(ret), K(new_table_schema));
  }
  return ret;
}

int ObDDLOperator::check_modify_column_when_upgrade(
    const share::schema::ObColumnSchemaV2& new_column, const share::schema::ObColumnSchemaV2& orig_column)
{
  int ret = OB_SUCCESS;
  if (obrpc::OB_UPGRADE_STAGE_DBUPGRADE != GCTX.get_upgrade_stage()) {
    // do nothing
  } else {
    ObColumnSchemaV2 tmp_column = new_column;
    tmp_column.set_schema_version(orig_column.get_schema_version());
    tmp_column.set_data_length(orig_column.get_data_length());
    tmp_column.set_data_precision(orig_column.get_data_precision());
    tmp_column.set_data_scale(orig_column.get_data_scale());
    if (OB_FAIL(tmp_column.get_assign_ret())) {
      LOG_WARN("assign failed", K(ret), K(new_column));
    } else if (tmp_column != orig_column) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("can only modify column's length", K(ret), K(new_column), K(orig_column));
    } else if (new_column.get_data_length() < orig_column.get_data_length() ||
               new_column.get_data_precision() < orig_column.get_data_precision() ||
               new_column.get_data_scale() < orig_column.get_data_scale()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("can only increase column's length", K(ret), K(new_column), K(orig_column));
    }
  }
  return ret;
}

int ObDDLOperator::insert_dependency_infos(common::ObMySQLTransaction& trans, ObIArray<ObDependencyInfo>& dep_infos,
    uint64_t tenant_id, uint64_t dep_obj_id, uint64_t schema_version, uint64_t owner_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == owner_id || OB_INVALID_ID == dep_obj_id || OB_INVALID_ID == tenant_id ||
      OB_INVALID_SCHEMA_VERSION == schema_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("illegal schema version or owner id", K(ret), K(schema_version), K(owner_id), K(dep_obj_id));
  } else if (IS_CLUSTER_VERSION_BEFORE_3100) {
    // do nothing
    LOG_DEBUG("all_dependency schema only support after version 3.1");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dep_infos.count(); ++i) {
      ObDependencyInfo& dep = dep_infos.at(i);
      dep.set_tenant_id(tenant_id);
      dep.set_dep_obj_id(dep_obj_id);
      dep.set_dep_obj_owner_id(owner_id);
      dep.set_schema_version(schema_version);
      OZ(dep.insert_schema_object_dependency(trans));
    }
  }

  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
