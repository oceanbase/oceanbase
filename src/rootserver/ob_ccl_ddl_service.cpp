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
#include "rootserver/ob_ccl_ddl_service.h"
#include "rootserver/ob_ccl_ddl_operator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver {

int ObCclDDLService::check_create_ccl_valid(const obrpc::ObCreateCCLRuleArg &arg,
                                            ObSchemaGetterGuard& schema_guard,
                                            const ObCCLRuleSchema *&exist_ccl_rule_schema) {
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObCCLRuleSchema *ccl_rule_schema =
        const_cast<ObCCLRuleSchema *>(&arg.ccl_rule_schema_);
    uint64_t tenant_id = ccl_rule_schema->get_tenant_id();
    ObString ccl_rule_name = ccl_rule_schema->get_ccl_rule_name();
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    const ObTableSchema *table_schema = NULL;
    if (OB_FAIL(
            ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
                tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (arg.affect_databases_name_.empty() &&
               !arg.affect_tables_name_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("arg' affect_tables_name_ must belong one of databse",
               K(arg.affect_databases_name_), K(arg.affect_tables_name_));
    } else {
      // 1.check user exist
      const ObUserInfo *user_info = NULL;
      if (arg.ccl_rule_schema_.get_affect_user_name() != "%") {
        if (OB_FAIL(schema_guard.get_user_info(
                tenant_id, arg.ccl_rule_schema_.get_affect_user_name(),
                arg.ccl_rule_schema_.get_affect_host(), user_info))) {
          LOG_WARN("get_user_id failed", K(ret),
                   K(arg.ccl_rule_schema_.get_affect_user_name()),
                   K(arg.ccl_rule_schema_.get_affect_host()));
        } else if (NULL == user_info) {
          if (OB_FAIL(schema_guard.get_user_info(
                tenant_id, arg.ccl_rule_schema_.get_affect_user_name(),
                ObString("%"), user_info))) {
            LOG_WARN("get_user_id failed", K(ret),
                     K(arg.ccl_rule_schema_.get_affect_user_name()),
                     K(arg.ccl_rule_schema_.get_affect_host()));
          } else if (NULL == user_info) {
            ret = OB_USER_NOT_EXIST; // no such user
            LOG_WARN("Try to create ccl rule on a not exist user", K(ret),
                     K(tenant_id),
                     K(arg.ccl_rule_schema_.get_affect_user_name()));
          }
        }
      }

      // 2. ccl check database & table exist
      if (OB_SUCC(ret)) {
        ObString database_name = arg.ccl_rule_schema_.get_affect_database();
        if (!database_name.empty()) {
          if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_name,
                                                       db_schema))) {
            LOG_WARN("get database schema failed", K(ret));
          } else if (NULL == db_schema) {
            ret = OB_ERR_BAD_DATABASE;
            LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(),
                           database_name.ptr());
          } else if (db_schema->is_in_recyclebin()) {
            ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
            LOG_WARN("Can not create ccl rule of db in recyclebin", K(ret),
                     K(arg), K(*db_schema));
          } else if (OB_INVALID_ID == db_schema->get_database_id()) {
            ret = OB_ERR_BAD_DATABASE;
            LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema),
                     K(ret));
          } else {
            ObString table_name = arg.ccl_rule_schema_.get_affect_table();
            if (!table_name.empty()) {
              if (OB_FAIL(schema_guard.get_table_schema(
                      tenant_id, database_name, table_name, false,
                      table_schema))) {
                LOG_WARN("get database schema failed", K(ret));
              } else if (NULL == table_schema) {
                ret = OB_ERR_BAD_TABLE;
                LOG_USER_ERROR(OB_ERR_BAD_TABLE, table_name.length(),
                               table_name.ptr());
              } else if (table_schema->is_in_recyclebin()) {
                ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
                LOG_WARN("Can not create ccl rule of table in recyclebin",
                         K(ret), K(arg), K(*table_schema));
              } else if (OB_INVALID_ID == table_schema->get_table_id()) {
                ret = OB_ERR_BAD_TABLE;
                LOG_WARN("table id is invalid", K(tenant_id), K(*table_schema),
                         K(ret));
              }
            }
          }
        }

        // 3. check ccl exist
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(schema_guard.get_ccl_rule_with_name(
                       tenant_id, ccl_rule_schema->get_ccl_rule_name(),
                       exist_ccl_rule_schema))) {
          LOG_WARN("failed to get ccl rule from shcema_guard", K(ret));
        } else if (OB_NOT_NULL(exist_ccl_rule_schema)) {
          if (arg.if_not_exist_) {
            ret = OB_SUCCESS;
            LOG_USER_WARN(OB_CCL_RULE_EXIST, ccl_rule_name.length(), ccl_rule_name.ptr());
          } else {
            ret = OB_CCL_RULE_EXIST;
            LOG_USER_ERROR(OB_CCL_RULE_EXIST, ccl_rule_name.length(), ccl_rule_name.ptr());
          }
        }
      }
    }
  }

  return ret;
}

int ObCclDDLService::check_drop_ccl_valid(
    const obrpc::ObDropCCLRuleArg &arg, ObSchemaGetterGuard &schema_guard,
    const ObCCLRuleSchema *&ccl_rule_schema) {
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  ObString ccl_rule_name = arg.ccl_rule_name_;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_ccl_rule_with_name(
                 tenant_id, arg.ccl_rule_name_, ccl_rule_schema))) {
    LOG_WARN("failed to get ccl rule from shcema_guard", K(ret));
  } else if (OB_ISNULL(ccl_rule_schema)) {
    if (arg.if_exist_) {
      ret = OB_SUCCESS;
      LOG_USER_WARN(OB_CCL_RULE_NOT_EXIST, ccl_rule_name.length(), ccl_rule_name.ptr());
    } else {
      ret = OB_CCL_RULE_NOT_EXIST;
      LOG_USER_ERROR(OB_CCL_RULE_NOT_EXIST, ccl_rule_name.length(), ccl_rule_name.ptr());
    }
  }
  return ret;
}

int ObCclDDLService::create_ccl_ddl(const obrpc::ObCreateCCLRuleArg &arg) {
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = arg.ccl_rule_schema_.get_tenant_id();
  const ObCCLRuleSchema *ccl_rule_schema = NULL;
  if (OB_FAIL(GET_MIN_DATA_VERSION(arg.exec_tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < MOCK_DATA_VERSION_4_3_5_3 ||
              (data_version>=DATA_VERSION_4_4_0_0 && data_version < DATA_VERSION_4_4_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ccl not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ccl");
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard in inner table failed", K(ret));
  } else if (OB_FAIL(check_create_ccl_valid(arg, schema_guard, ccl_rule_schema))) {
    LOG_WARN("fail to check create ccl valid", K(ret));
  } else if (OB_NOT_NULL(ccl_rule_schema)) {
    // create if not exists may reach here, ret == OB_SUCCESS
    LOG_WARN("ccl_rule_schema is not null", K(ret));
  } else {
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    ObCclDDLOperator ddl_operator(ddl_service_->get_schema_service(), ddl_service_->get_sql_proxy());
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id,
                                                refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret),
               K(tenant_id));
    } else if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id,
                                   refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id),
               K(refreshed_schema_version));
    } else if (OB_FAIL(ddl_operator.create_ccl_rule(*const_cast<ObCCLRuleSchema*>(&arg.ccl_rule_schema_), trans, &arg.ddl_stmt_str_))) {
      LOG_WARN("failed to create ccl rule", K(tenant_id),
               K(ccl_rule_schema), K(ret));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret,
                 K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObCclDDLService::drop_ccl_ddl(const obrpc::ObDropCCLRuleArg &arg) {
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = arg.tenant_id_;
  const ObCCLRuleSchema *ccl_rule_schema = NULL;
  if (OB_FAIL(GET_MIN_DATA_VERSION(arg.exec_tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < MOCK_DATA_VERSION_4_3_5_3 ||
              (data_version>=DATA_VERSION_4_4_0_0 && data_version < DATA_VERSION_4_4_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ccl not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ccl");
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard in inner table failed", K(ret));
  } else if (OB_FAIL(check_drop_ccl_valid(arg, schema_guard, ccl_rule_schema))) {
    LOG_WARN("fail to check create ccl valid", K(ret));
  } else if (OB_NOT_NULL(ccl_rule_schema)) {
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    ObCclDDLOperator ddl_operator(ddl_service_->get_schema_service(), ddl_service_->get_sql_proxy());
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id,
                                                refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret),
               K(tenant_id));
    } else if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id,
                                   refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id),
               K(refreshed_schema_version));
    } else if (OB_FAIL(ddl_operator.drop_ccl_rule(arg.tenant_id_, *ccl_rule_schema, trans, &arg.ddl_stmt_str_))) {
      LOG_WARN("failed to drop ccl rule", K(tenant_id),
               K(ccl_rule_schema), K(ret));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret,
                 K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase