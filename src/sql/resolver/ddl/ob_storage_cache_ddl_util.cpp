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
#define USING_LOG_PREFIX STORAGE
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_alter_table_resolver.h"
#include "sql/resolver/ddl/ob_storage_cache_ddl_util.h"
namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace sql
{
int ObDDLResolver::get_storage_cache_tbl_schema(const ObTableSchema *&tbl_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    if (OB_ISNULL(create_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("create table stmt is null", KR(ret));
    } else if (OB_ISNULL(tbl_schema = &create_table_stmt->get_create_table_arg().schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("create table schema is null", KR(ret));
    }
  } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
    // get table schema from schema_checker_, the table schema is not in alter_table_schema
    // because the alter_table_schema does not contain the part level and is_user_table
    ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
    if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alter table stmt is null", KR(ret));
    } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                        alter_table_stmt->get_org_database_name(),
                                                        alter_table_stmt->get_org_table_name(),
                                                        false,
                                                        tbl_schema))) {
      LOG_WARN("table is not exist", K(alter_table_stmt->get_org_database_name()), K(alter_table_stmt->get_org_table_name()), K(ret));
    }
  } else if (stmt::T_CREATE_INDEX == stmt_->get_stmt_type()) {
    ObCreateIndexStmt *create_index_stmt = static_cast<ObCreateIndexStmt *>(stmt_);
    if (OB_ISNULL(create_index_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("create index stmt is null", KR(ret));
    } else if (OB_ISNULL(tbl_schema = &create_index_stmt->get_create_index_arg().index_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", KP(tbl_schema), K(ret));
    }
  }
  return ret;
}

// @param is_alter_add_index: Since alter table includes multiple operations, not just alter add index,
// use this parameter to mark the current statement as an alter add index statement.
// true if the statement is alter add index, false otherwise. Cause alter table is not only
int ObDDLResolver::set_default_storage_cache_policy(const bool is_alter_add_index)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicy storage_cache_policy;
  bool use_default_storage_cache_policy = false;
  if (OB_ISNULL(stmt_)) {
  ret = OB_ERR_UNEXPECTED;
  LOG_WARN("stmt is null", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else if (storage_cache_policy_.empty()) {
    if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
      ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
      if (OB_ISNULL(create_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("create_table_stmt is null", K(ret));
      } else if (create_table_stmt->get_create_table_arg().schema_.is_user_table()) {
        use_default_storage_cache_policy = true;
      }
    } else if (stmt::T_CREATE_INDEX == stmt_->get_stmt_type()) {
      if (LOCAL_INDEX == index_scope_ || (NOT_SPECIFIED == index_scope_ && is_mysql_mode())) {
        // When create local index, the default storage cache policy is NONE
        // In mysql mode, if the index scope is not specified, the index is regarded as local index
        storage_cache_policy.set_global_policy(ObStorageCacheGlobalPolicy::PolicyType::NONE_POLICY);
      } else {
        use_default_storage_cache_policy = true;
      }
    } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type() && is_alter_add_index) {
      ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alter table stmt is null", K(ret));
      } else if (GLOBAL_INDEX == index_scope_) {
        use_default_storage_cache_policy = true;
      } else if (LOCAL_INDEX == index_scope_ || (NOT_SPECIFIED == index_scope_ && is_mysql_mode())) {
        storage_cache_policy.set_global_policy(ObStorageCacheGlobalPolicy::PolicyType::NONE_POLICY);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index scope", K(ret), K(index_scope_));
      }
    }
  }
  if (OB_SUCC(ret) && use_default_storage_cache_policy) {
    ObString default_storage_cache_policy;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(session_info_->get_effective_tenant_id()));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "tenant config is invalid", K(ret));
    } else if (FALSE_IT(default_storage_cache_policy = tenant_config->default_storage_cache_policy.str())) {
    } else if (default_storage_cache_policy.empty()) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "tenant config is invalid", K(ret), K(default_storage_cache_policy));
    } else if (default_storage_cache_policy.case_compare("AUTO") == 0) {
      storage_cache_policy.set_global_policy(ObStorageCacheGlobalPolicy::PolicyType::AUTO_POLICY);
    } else if (default_storage_cache_policy.case_compare("HOT") == 0) {
      storage_cache_policy.set_global_policy(ObStorageCacheGlobalPolicy::PolicyType::HOT_POLICY);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid default storage cache policy", K(ret), K(default_storage_cache_policy));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "default storage cache policy");
    }
  }

  if (OB_SUCC(ret)) {
    char cache_policy_str[OB_MAX_STORAGE_CACHE_POLICY_LENGTH] = {0};
    int64_t pos = 0;
    if (OB_FAIL(storage_cache_policy.to_json_string(cache_policy_str, OB_MAX_STORAGE_CACHE_POLICY_LENGTH, pos))) {
      LOG_WARN("failed to convert storage cache policy to string", K(ret), K(pos), K(storage_cache_policy));
    } else if (OB_UNLIKELY(pos == 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("covert pos for storage cache policy is invalid", K(ret), K(pos), K(storage_cache_policy));
    } else if (OB_FAIL(ob_write_string(*allocator_, ObString(pos, cache_policy_str), storage_cache_policy_))) {
      LOG_WARN("failed to set storage cache policy to create table schema", K(ret), K(cache_policy_str));
    }
  }
  return ret;
}

int ObDDLResolver::resolve_storage_cache_attribute(const ParseNode *node, ObResolverParams &params, const bool is_index_option)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *tbl_schema = nullptr;
  uint64_t tenant_data_version = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("storage cache policy is not supported in data version less than 4.3.5.2", K(ret),
        K(tenant_data_version));
    LOG_USER_ERROR(
        OB_NOT_SUPPORTED, "Storage cache policy in data version less than 4.3.5.2 is");
  } else if (OB_FAIL(get_storage_cache_tbl_schema(tbl_schema))) {
    LOG_WARN("failed to get storage cache table schema", K(ret));
  } else if (!tbl_schema->is_user_table() || tbl_schema->is_external_table()) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "only allow to alter storage cache policy for user table or external table", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObStorageCachePolicy cache_policy;
    switch (node->type_) {
      case T_GLOBAL_OPTION: {
        ObString string_v = ObString(node->children_[0]->str_len_, node->children_[0]->str_value_).trim_space_only();
        ObStorageCacheGlobalPolicy::PolicyType global_policy = ObStorageCacheGlobalPolicy::get_type(string_v);
        if (ObStorageCacheGlobalPolicy::MAX_POLICY == global_policy) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "This global storage cache policy is");
          LOG_WARN(
              "failed. storage cache policy type is not supported", K(ret), KPHEX(string_v.ptr(), string_v.length()));
        } else {
          cache_policy.set_global_policy(global_policy);
        }
        break;
      }
      case T_STORAGE_CACHE_TIME_POLICY_OPTIONS: {
        for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
          if (OB_ISNULL(node->children_[i])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "node is null", K(ret));
          } else if (OB_FAIL(resolve_storage_cache_time_attribute(node->children_[i], params, cache_policy))) {
            LOG_WARN("failed to resolve storage cache time policy options after time attribute", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          cache_policy.set_global_policy(ObStorageCacheGlobalPolicy::PolicyType::TIME_POLICY);
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid storage cache policy attribute", K(ret), K(node->type_));
      }
    }
    if (OB_SUCC(ret)) {
      char cache_policy_str[OB_MAX_STORAGE_CACHE_POLICY_LENGTH] = {0};
      int64_t pos = 0;
      if (OB_FAIL(cache_policy.to_json_string(cache_policy_str, OB_MAX_STORAGE_CACHE_POLICY_LENGTH, pos))) {
        LOG_WARN("failed to conver storage cache policy to json string", K(ret), K(pos), K(cache_policy));
      } else if (OB_UNLIKELY(pos == 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to convert storage cache policy to string", K(ret));
      } else if (is_index_option) {
        if (OB_FAIL(ob_write_string(*allocator_, ObString(pos, cache_policy_str), index_storage_cache_policy_))) {
          LOG_WARN("failed to set storage cache policy to create table schema", K(ret), K(cache_policy_str));
        }
      } else {
        if (OB_FAIL(ob_write_string(*allocator_, ObString(pos, cache_policy_str), storage_cache_policy_))) {
          LOG_WARN("failed to set storage cache policy to create table schema", K(ret), K(cache_policy_str));
        } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type() && !is_index_option) {
          if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::STORAGE_CACHE_POLICY))) {
            SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLResolver::resolve_storage_cache_time_attribute(const ParseNode *node, ObResolverParams &params, ObStorageCachePolicy &cache_policy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse node is null", K(ret));
  } else if (T_HOT_RETENTION != node->type_ && (node->num_child_ != 1 || OB_ISNULL(node->children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num child is invalid", K(ret), K(node->num_child_));
  } else {
    switch (node->type_) {
      case T_BOUNDARY_COLUMN: {
        ObString name;
        if (OB_FAIL(resolve_column_name(name, node->children_[0]))) {
          LOG_WARN("resolve column name failed", K(ret));
        } else if (OB_FAIL(cache_policy.set_column_name(name.ptr(), name.length()))) {
          LOG_WARN("failed to set column name", K(ret), K(name));
        }
        break;
      }
      case T_BOUNDARY_COLUMN_UNIT: {
        ObString string_v = ObString(node->children_[0]->str_len_, node->children_[0]->str_value_).trim_space_only();
        ObBoundaryColumnUnit::ColumnUnitType column_unit = ObBoundaryColumnUnit::get_type(string_v);
        if (ObBoundaryColumnUnit::MAX_UNIT == column_unit) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "This column unit is");
          LOG_WARN("failed. boundary column unit type is not supported", K(ret), K(string_v));
        } else if(OB_FAIL(cache_policy.set_column_unit(column_unit))) {
          LOG_WARN("failed to set column unit", K(ret), K(column_unit));
        }
        break;
      }
      case T_GRANULARITY: {
        ObString string_v = ObString(node->children_[0]->str_len_, node->children_[0]->str_value_).trim_space_only();
        ObStorageCacheGranularity::GranularityType granularity = ObStorageCacheGranularity::get_type(string_v);
        if (ObStorageCacheGranularity::MAX_GRANULARITY == granularity) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "This granularity is");
          LOG_WARN("failed. granularity type is not supported", K(ret), K(string_v));
        } else if (OB_FAIL(cache_policy.set_granularity(granularity))) {
          LOG_WARN("failed to set granularity", K(ret), K(granularity));
        }
        break;
      }
      case T_HOT_RETENTION: {
        if (node->num_child_ != 2 || OB_ISNULL(node->children_[0]) || OB_ISNULL(node->children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse node is invalid", K(ret), K(node->num_child_));
        } else {
          const uint64_t hot_retention_interval = static_cast<uint64_t>(node->children_[0]->value_);
          ObDateUnitType hot_retention_unit = static_cast<ObDateUnitType>(node->children_[1]->value_);

          if (hot_retention_interval == 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "hot retention interval");
            LOG_WARN("hot_rentention_interval should be greater than 0", K(ret),
                K(hot_retention_interval));
          } else if (ObDateUnitType::DATE_UNIT_MAX == hot_retention_unit) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "This hot retention unit is");
            LOG_WARN(
                "failed. hot retention unit type is not supported", K(ret), K(hot_retention_unit));
          } else if (OB_FAIL(cache_policy.set_hot_retention_interval(hot_retention_interval))) {
            LOG_WARN("failed to set hot retention interval", K(ret), K(hot_retention_interval));
          } else if (OB_FAIL(cache_policy.set_hot_retention_unit(hot_retention_unit))) {
            LOG_WARN("failed to set hot retention unit", K(ret), K(hot_retention_unit));
          }
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid storage cache time policy option", K(ret), K(node->type_));
      }
    }
  }
  return ret;
}
// This function is used for create table and create index when resolve table option
// If the storage cache policy is not set, the default storage cache policy will be set.
int ObDDLResolver::check_and_set_default_storage_cache_policy()
{
  int ret = OB_SUCCESS;
  bool need_set_default_storage_cache_policy = false;
  const ObTableSchema *tbl_schema = nullptr;
  if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type() ||
      stmt::T_CREATE_INDEX == stmt_->get_stmt_type()) {
    if (OB_FAIL(get_storage_cache_tbl_schema(tbl_schema))) {
      LOG_WARN("failed to get storage cache tbl schema", K(ret));
    } else if (OB_NOT_NULL(tbl_schema)) {
      // When creating a table/index, if the table/index is a user table,
      // set the default storage cache policy to true
      // need_set_default_storage_cache_policy means this stmt should has a default storage cache policy (not the tenant config)
      if (tbl_schema->is_user_table()) {
        need_set_default_storage_cache_policy = true;
      }
    }
  }
  LOG_TRACE("[SCP] check and set default storage cache policy", KR(ret), K(need_set_default_storage_cache_policy));

  if (OB_SUCC(ret) && need_set_default_storage_cache_policy) {
    if (OB_ISNULL(storage_cache_policy_) && OB_FAIL(set_default_storage_cache_policy())) {
      SQL_RESV_LOG(WARN, "set default storage cache policy failed", K(ret), K_(storage_cache_policy));
    } else if (OB_NOT_NULL(storage_cache_policy_)) {
      ObStorageCachePolicy storage_cache_policy;
      if (OB_FAIL(storage_cache_policy.load_from_string(storage_cache_policy_))) {
        SQL_RESV_LOG(WARN, "load storage cache policy failed", K(ret), K_(storage_cache_policy));
      } else if (ObStorageCacheGlobalPolicy::NONE_POLICY == storage_cache_policy.get_global_policy()) {
        if (stmt::T_CREATE_INDEX == stmt_->get_stmt_type()) {
          if (!(LOCAL_INDEX == index_scope_ || (is_mysql_mode() && NOT_SPECIFIED == index_scope_))) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("only allow to set NONE_POLICY for local index", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
// Only used to check storage cache policy for create table/index statement
int ObDDLResolver::check_create_stmt_storage_cache_policy(const ObString &storage_cache_policy_str, const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_version = 0;
  if (OB_ISNULL(table_schema) || OB_ISNULL(stmt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema or stmt is null", K(ret), K(table_schema), K(stmt_));
  } else if (storage_cache_policy_str.empty()) {
    // do nothing
  } else if (!(table_schema->is_user_table() || table_schema->is_index_table())) {
    // This is used to check the validity of the manually specified storage cache policy.
    // Since only user data tables and index tables are currently allowed to be specified,
    // only these two types of tables are checked here.
    // do nothing
  } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type() ||
             stmt::T_CREATE_INDEX == stmt_->get_stmt_type()) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), tenant_version))) {
      LOG_WARN("failed to get data version", K(ret));
    } else if (tenant_version >= DATA_VERSION_4_3_5_2) {
      if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
        if (is_storage_cache_policy_none(storage_cache_policy_str)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only allow to set NONE_POLICY for user index", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type() && is_storage_cache_policy_auto(storage_cache_policy_str)) {
        // do nothing
      } else if (stmt::T_CREATE_INDEX == stmt_->get_stmt_type() && is_storage_cache_policy_none(storage_cache_policy_str)) {
        // do nothing
      } else {
        ObStorageCachePolicy storage_cache_policy;
        if (OB_FAIL(storage_cache_policy.load_from_string(storage_cache_policy_str))) {
          LOG_WARN("failed to load storage cache policy", K(ret));
        } else if (OB_FAIL(check_storage_cache_policy(storage_cache_policy, table_schema))) {
          LOG_WARN("failed to check storage cache policy", K(ret));
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stmt type for check storage cache policy", K(ret), K(storage_cache_policy_str), K(stmt_->get_stmt_type()), K(table_schema->get_table_id()));
  }
  return ret;
}

// Only used to check storage cache policy for alter table statement
int ObDDLResolver::check_alter_stmt_storage_cache_policy(const ObTableSchema *ori_table_schema)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
  ObSArray<obrpc::ObCreateIndexArg*> &add_index_arg_list = alter_table_stmt->get_index_arg_list();
  const ObSArray<obrpc::ObIndexArg*> &alter_index_arg_list = alter_table_stmt->get_alter_index_arg_list();

  if (OB_ISNULL(ori_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ori table schema is null", KR(ret));
  } else if (OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema checker is null", KR(ret), KPC(ori_table_schema));
  } else if (!ori_table_schema->is_user_table()) {
    // do nothing
  } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
    if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alter table stmt is null", KR(ret));
    } else {
      // Check alter table add index
      if (add_index_arg_list.count() > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < add_index_arg_list.count(); ++i) {
          const ObTableSchema *add_index_schema = nullptr;
          ObCreateIndexArg *add_index_arg = add_index_arg_list.at(i);
          if (OB_ISNULL(add_index_arg)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("index arg is null", K(ret));
          } else if (obrpc::ObIndexArg::ADD_INDEX == add_index_arg->index_action_type_) {
            if (OB_ISNULL(add_index_arg->index_option_.storage_cache_policy_)) {
              // do nothing
            } else {
              add_index_schema = &add_index_arg->index_schema_;
              ObStorageCachePolicy add_index_storage_cache_policy;
              if (OB_FAIL(add_index_storage_cache_policy.load_from_string(add_index_arg->index_option_.storage_cache_policy_))) {
                LOG_WARN("failed to load storage cache policy", K(ret));
              } else if (OB_FAIL(check_storage_cache_policy(add_index_storage_cache_policy, add_index_schema))) {
                LOG_WARN("failed to check storage cache policy", K(ret));
              }
            }
          }
        }
      }
      // Check alter table alter index
      if (OB_FAIL(ret)) {
      } else if (alter_index_arg_list.count() > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < alter_index_arg_list.count(); ++i) {
          ObIndexArg *alter_index_arg = alter_index_arg_list.at(i);
          const ObTableSchema *alter_index_schema = nullptr;
          if (OB_ISNULL(alter_index_arg)) {
            ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index arg is null", K(ret));
          } else if (obrpc::ObIndexArg::ALTER_INDEX == alter_index_arg->index_action_type_) {
            // Since the alter table on the resolver side cannot distinguish between various behaviors,
            // alter_index_arg_list will be reused by other behaviors.
            // Therefore, no judgment is made here on behaviors other than alter_index.
            ObString index_table_name;
            if (OB_FAIL(ObTableSchema::build_index_table_name(*allocator_,
                                                              ori_table_schema->get_table_id(),
                                                              alter_index_arg->index_name_,
                                                              index_table_name))) {
              LOG_WARN("build_index_table_name failed", K(ret), K(ori_table_schema->get_table_id()), K(alter_index_arg->index_name_));
            } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                                 alter_table_stmt->get_org_database_name(),
                                                                 index_table_name,
                                                                 true /* index table */,
                                                                 alter_index_schema))) {
              LOG_WARN("failed to get table schema", K(ret), K(alter_index_arg->index_name_), K(index_table_name));
            } else if (OB_ISNULL(alter_index_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("alter index schema is null", K(ret));
            } else if (OB_ISNULL(alter_index_arg->storage_cache_policy_)) {
              // do nothing
            } else {
              ObStorageCachePolicy alter_index_storage_cache_policy;
              if (OB_FAIL(alter_index_storage_cache_policy.load_from_string(alter_index_arg->storage_cache_policy_))) {
                LOG_WARN("failed to load storage cache policy", K(ret));
              } else if (OB_FAIL(check_storage_cache_policy(alter_index_storage_cache_policy, alter_index_schema))) {
                LOG_WARN("failed to check storage cache policy", K(ret));
              }
            }
          }
        }
      }
      // Check alter table storage_cache_policy
      if (OB_FAIL(ret)) {
      } else {
        ObStorageCachePolicy storage_cache_policy;
        AlterTableSchema &alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
        if (OB_FAIL(storage_cache_policy.load_from_string(alter_table_schema.get_storage_cache_policy()))) {
          LOG_WARN("failed to load storage cache policy", K(ret));
        } else if (OB_FAIL(check_storage_cache_policy(storage_cache_policy, ori_table_schema))) {
          LOG_WARN("failed to check storage cache policy", K(ret), K(storage_cache_policy), K(alter_table_stmt->get_alter_table_arg()));
        }
      }
    }
  }
  return ret;
}

// @param storage_cache_policy: The storage cache policy to be checked
int ObDDLResolver::check_storage_cache_policy(ObStorageCachePolicy &storage_cache_policy, const ObTableSchema *tbl_schema)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = nullptr;
  bool is_create_stmt = false;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(stmt_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"unexpected null value", K(ret), K_(schema_checker), K_(stmt));
  } else if (OB_ISNULL(tbl_schema)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "table schema is null", K(ret));
  } else if (!(tbl_schema->is_user_table() || tbl_schema->is_index_table())) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "only allow to set storage cache policy for user table", K(ret));
  } else if (FALSE_IT(is_create_stmt = (stmt::T_CREATE_TABLE == stmt_->get_stmt_type() || stmt::T_CREATE_INDEX == stmt_->get_stmt_type()))) {
  } else if (storage_cache_policy.is_time_policy() && !tbl_schema->is_partitioned_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Time storage cache policy for non-partitioned table is");
    LOG_WARN("time storage cache policy for non-partitioned table", K(ret), K(tbl_schema->get_table_name()));
  }
  // Check the global storage cache policy is valid
  if (OB_SUCC(ret) && storage_cache_policy.is_global_policy()) {
    // User table can not set NONE policy
    // Local index and global index can set NONE policy
    if (tbl_schema->is_user_table()) {
      if (ObStorageCacheGlobalPolicy::NONE_POLICY == storage_cache_policy.get_global_policy()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("only storage cache policy of local index can be set as NONE policy", K(ret), K_(index_scope), K(tbl_schema->get_table_type()), K(tbl_schema));
      }
    }
  }
  // Check validation of time storage cache policy
  if (OB_SUCC(ret) && storage_cache_policy.is_time_policy()) {
    // Check if the column is time type
    ObString column_name(storage_cache_policy.get_column_name());
    if (OB_ISNULL(column_schema = tbl_schema->get_column_schema(column_name))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "boudary column of storage cache policy, the column does not exist");
      LOG_WARN("storage cache policy column is not exists", K(ret), K(column_name));
    } else if (!column_schema->is_part_key_column() && !column_schema->is_subpart_key_column()) {
      /* not part key, skip*/
    } else {
      const ObPartitionOption &part_option = column_schema->is_part_key_column() ?
                                            tbl_schema->get_part_option() : tbl_schema->get_sub_part_option();
      ObString part_func_str = ObString(part_option.get_part_func_expr_str());
      // remove the '`' in the part_func_str, example: `part_func_expr` -> part_func_expr
      if (part_func_str.length() > 2 && part_func_str[part_func_str.length() - 1] == '`' && part_func_str[0] == '`') {
        ++part_func_str;
        part_func_str.assign(part_func_str.ptr(), part_func_str.length() - 1);
      }
      const ObString &boundary_column_name = ObString(column_schema->get_column_name());
      if (!(ObColumnNameHashWrapper(part_func_str) == ObColumnNameHashWrapper(boundary_column_name))) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("cache policy not support part key using expr", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Using expr as part key is");
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      switch(ob_obj_type_class(column_schema->get_data_type())) {
        // If the column is int/bigint type, check if the column unit is given
        case ObIntTC: {
          if (!ObBoundaryColumnUnit::is_valid(storage_cache_policy.get_column_unit())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "storage_cache_policy, when column type is int/bigint, the boundary column unit should be given");
            LOG_WARN("invalid column unit for storage cache policy, when column type is int/bigint, "
                    "the boundary column unit should be given",
                K(ret), K(column_name), K(column_schema->get_data_type()), K(storage_cache_policy));
          }
          break;
        }
        case ObDateTimeTC:
        case ObOTimestampTC:
        case ObDateTC:
        case ObYearTC:
        case ObMySQLDateTC:
        case ObMySQLDateTimeTC: {
          if (ObBoundaryColumnUnit::is_valid(storage_cache_policy.get_column_unit())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "storage_cache_policy, storage cache policy boundary column unit should be removed");
            LOG_WARN("invalid column unit for storage cache policy, when column type is datetime/timestamp/time/date, "
                    "the boundary column unit should not be provided",
                K(ret), K(column_name), K(column_schema->get_data_type()));
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid column type for storage cache policy", K(ret), K(column_schema->get_data_type()), K(column_schema->get_data_type()), K(storage_cache_policy.get_column_name()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "The type of boundary column is");
          break;
        }
      }
    }
    // Check the partition level is valid
    if (OB_SUCC(ret) && storage_cache_policy.is_time_policy()) {
      // Check if the column is the firstpartition key column
      // and the partition function is range or range columns
      ObPartitionLevel part_level = tbl_schema->get_part_level();
      uint64_t column_id = OB_INVALID_ID;
      if (PARTITION_LEVEL_ONE == part_level) {
        if (!tbl_schema->is_range_part()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Using a time storage cache policy on a non-range partitioned table is");
          LOG_WARN("the table should be range partitioned table if the storage cache policy is time policy", K(ret));
        } else {
          // Check if the column is the first partition key column
          const ObPartitionKeyInfo &part_key_info = tbl_schema->get_partition_key_info();
          column_id = column_schema->get_column_id();
          if (OB_FAIL(check_column_is_first_part_key(part_key_info, column_id))) {
            LOG_WARN("check column is first part key failed", K(ret), K(column_id));
          }
        }
      } else if (PARTITION_LEVEL_TWO == part_level) {
        // When the partition level is two
        // Check if the column is the first partition key column or the first subpartition key column
        if (tbl_schema->is_range_part()) {
          if (tbl_schema->is_range_subpart()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Using time storage cache policy on a table with both range partition and range subpartition is");
            LOG_WARN("range partition and range subpartition are not supported for time storage cache policy", K(ret));
          } else if (!tbl_schema->is_range_subpart()) {
            // Check if the column is the first partition key column
            if (OB_FAIL(check_column_is_first_part_key(tbl_schema->get_partition_key_info(), column_schema->get_column_id()))) {
              LOG_WARN("check column is first subpartition key failed", K(ret), K(column_schema->get_column_id()));
            }
          }
        } else if (!tbl_schema->is_range_part()) {
          if (tbl_schema->is_range_subpart()) {
            if (OB_FAIL(check_column_is_first_part_key(tbl_schema->get_subpartition_key_info(), column_schema->get_column_id()))) {
              LOG_WARN("check column is first subpartition key failed", K(ret), K(column_schema->get_column_id()));
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Using time storage cache policy on partition tables without range partitions is");
            LOG_WARN("range partition and range subpartition are not supported for storage cache policy", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLResolver::resolve_partition_storage_cache_policy_element(const ObString &storage_cache_policy_str, ObStorageCachePolicyType &storage_cache_policy_type)
{
  int ret = OB_SUCCESS;
  storage_cache_policy_type = ObStorageCachePolicyType::MAX_POLICY;
  if (OB_UNLIKELY(storage_cache_policy_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache policy str", KR(ret), K(storage_cache_policy_str));
  } else {
    if (OB_FAIL(get_storage_cache_policy_type_from_part_str(storage_cache_policy_str, storage_cache_policy_type))) {
      LOG_WARN("get storage cache policy type failed", KR(ret), K(storage_cache_policy_str));
    } else if (storage_cache_policy_type == ObStorageCachePolicyType::MAX_POLICY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "storage_cache_policy for partition must be 'HOT', 'AUTO' or 'NONE'");
      LOG_WARN("invalid storage cache policy", KR(ret), K(storage_cache_policy_str));
    }
  }
  return ret;
}

int ObDDLResolver::resolve_storage_cache_policy_in_part_list(const ParseNode *node,
                                                             const int64_t tenant_id,
                                                             const bool is_template_subpartition,
                                                             ObBasePartition &partition)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyType storage_cache_policy_type = ObStorageCachePolicyType::MAX_POLICY;
  uint64_t tenant_data_version = 0;
  if (OB_ISNULL(node)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("parse node for storage cache policy element in partition list is null", KR(ret));
  } else if (GCTX.is_shared_storage_mode() &&
             lib::is_mysql_mode() &&
             OB_NOT_NULL(node->children_[ELEMENT_STORAGE_CACHE_POLICY])) {
    if (is_template_subpartition) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "storage cache policy in subpartition template");
    } else {
      const ParseNode *storage_cache_policy_node = node->children_[ELEMENT_STORAGE_CACHE_POLICY];
      if (T_STORAGE_CACHE_POLICY_IN_PART_LIST != storage_cache_policy_node->type_ ||
          1 != storage_cache_policy_node->num_child_ ||
          OB_ISNULL(storage_cache_policy_node->children_[0])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid parse tree when resolve storage cache policy as partition element",
            KR(ret), K(storage_cache_policy_node->num_child_), K(storage_cache_policy_node->type_));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
        LOG_WARN("get data version failed", KR(ret), K(tenant_id));
      } else if (tenant_data_version < DATA_VERSION_4_4_1_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("storage cache policy int partition list is not supported in versions before 4.4.1.0", KR(ret));
      } else {
        const ObString new_storage_cache_policy(static_cast<int32_t>(storage_cache_policy_node->children_[0]->str_len_),
            storage_cache_policy_node->children_[0]->str_value_);
        if (OB_FAIL(resolve_partition_storage_cache_policy_element(new_storage_cache_policy, storage_cache_policy_type))) {
          LOG_WARN("fail to resolve storage cache in part definition", KR(ret), K(partition));
        } else if (FALSE_IT(partition.set_part_storage_cache_policy_type(storage_cache_policy_type))) {
          LOG_WARN("failed to set part storage_cache_policy", K(ret));
        }
      }
    }
  }
  return ret;
}

/*************************ObAlterTableResolver*************************/
int ObAlterTableResolver::resolve_alter_partition_storage_cache_policy(const ParseNode &node,
    const share::schema::ObTableSchema &orig_table_schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionLevel part_level = orig_table_schema.get_part_level();
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  if (T_ALTER_PARTITION_STORAGE_CACHE_POLICY != node.type_
      || 2 != node.num_child_
      || OB_ISNULL(node.children_[0])
      || OB_ISNULL(node.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree when alter partition storage cache policy",
        K(ret), K(node.num_child_), K(node.type_));
  } else if (!orig_table_schema.is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user table", KR(ret), K(orig_table_schema));
  } else if (PARTITION_LEVEL_ZERO == part_level) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_USER_ERROR(OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED);
    LOG_WARN("unsupport management on non partitioned table", KR(ret), K(orig_table_schema));
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter table stmt should not be null", KR(ret));
  } else {
    AlterTableSchema &alter_table_schema =
        alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    const ObPartition *part = nullptr;
    ObString new_storage_cache_policy(static_cast<int32_t>(node.children_[1]->str_len_),
        node.children_[1]->str_value_);
    ObStorageCachePolicyType storage_cache_policy_type = ObStorageCachePolicyType::MAX_POLICY;
    if (OB_FAIL(get_storage_cache_policy_type_from_part_str(new_storage_cache_policy, storage_cache_policy_type))) {
      LOG_WARN("get storage cache policy type failed", KR(ret), K(new_storage_cache_policy));
    } else if (storage_cache_policy_type == ObStorageCachePolicyType::MAX_POLICY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "storage_cache_policy, storage_cache_policy for partition must be 'HOT', 'AUTO' or 'NONE'");
      LOG_WARN("invalid storage cache policy", KR(ret), K(new_storage_cache_policy));
    } else {
      str_toupper(new_storage_cache_policy.ptr(), new_storage_cache_policy.length());
      const ObPartitionOption &ori_part_option = orig_table_schema.get_part_option();
      ParseNode *name_list = node.children_[0];

      if (OB_ISNULL(name_list)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("name_list is null", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < name_list->num_child_; ++i) {
          ObPartition inc_part;
          ObString partition_name(static_cast<int32_t>(name_list->children_[i]->str_len_), name_list->children_[i]->str_value_);
          if (OB_FAIL(orig_table_schema.get_partition_by_name(partition_name, part))) {
            LOG_WARN("get part by name failed", KR(ret), K(partition_name), K(orig_table_schema));
          } else if (OB_ISNULL(part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part is null", KR(ret), K(partition_name), K(orig_table_schema));
          } else if (OB_FAIL(inc_part.assign(*part))) {
            LOG_WARN("inc part assign failed", KR(ret), K(inc_part), K(*part));
          } else if (FALSE_IT(inc_part.set_part_storage_cache_policy_type(storage_cache_policy_type))) {
          } else if (OB_FAIL(alter_table_schema.add_partition(inc_part))) {
            LOG_WARN("alter table add inc part failed", KR(ret), K(alter_table_schema), K(inc_part));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(alter_table_schema.get_part_option().assign(ori_part_option))) {
            LOG_WARN("alter table set part option failed", KR(ret), K(alter_table_schema),
                K(ori_part_option));
          } else {
            alter_table_schema.get_part_option().set_part_num(alter_table_schema.get_partition_num());
            alter_table_schema.set_part_level(part_level);
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_subpartition_storage_cache_policy(const ParseNode &node,
                                                      const share::schema::ObTableSchema &orig_table_schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionLevel part_level = orig_table_schema.get_part_level();
  uint64_t tenant_data_version = 0;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  if (T_ALTER_SUBPARTITION_STORAGE_CACHE_POLICY != node.type_
      || 2 != node.num_child_
      || OB_ISNULL(node.children_[0])
      || OB_ISNULL(node.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", KR(ret));
  } else if (!orig_table_schema.is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user table", KR(ret), K(orig_table_schema));
  } else if (orig_table_schema.has_sub_part_template_def()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on subpartition template table", KR(ret), K(orig_table_schema));
  } else if (PARTITION_LEVEL_ZERO == part_level || PARTITION_LEVEL_ONE == part_level) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_USER_ERROR(OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED);
    LOG_WARN("unsupport management on non-subpartitioned table", KR(ret));
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter table stmt should not be null", KR(ret));
  } else {
    AlterTableSchema &alter_table_schema =
        alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    ObString new_storage_cache_policy(static_cast<int32_t>(node.children_[1]->str_len_),
        node.children_[1]->str_value_);
    ObStorageCachePolicyType storage_cache_policy_type = ObStorageCachePolicyType::MAX_POLICY;
    if (OB_FAIL(get_storage_cache_policy_type_from_part_str(new_storage_cache_policy, storage_cache_policy_type))) {
      LOG_WARN("get storage cache policy type failed", KR(ret), K(new_storage_cache_policy));
    } else {
      str_toupper(new_storage_cache_policy.ptr(), new_storage_cache_policy.length());

      const ObPartitionOption &ori_part_option = orig_table_schema.get_part_option();
      ParseNode *name_list = node.children_[0];
      if (OB_ISNULL(name_list)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("name_list is null", KR(ret));
      } else {
        const ObPartition *part = nullptr;
        const ObSubPartition *subpart = nullptr;
        for (int64_t i = 0; OB_SUCC(ret) && i < name_list->num_child_; ++i) {
          ObPartition inc_part;
          ObSubPartition inc_subpart;
          ObString subpart_name(static_cast<int32_t>(name_list->children_[i]->str_len_), name_list->children_[i]->str_value_);
          if (OB_FAIL(orig_table_schema.get_subpartition_by_name(subpart_name, part, subpart))) {
            LOG_WARN("get part by name failed", KR(ret), K(subpart_name), K(orig_table_schema));
          } else if (OB_ISNULL(part) || OB_ISNULL(subpart)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part or subpart is null", KR(ret), K(subpart_name), K(orig_table_schema));
          } else if (OB_FAIL(inc_subpart.assign(*subpart))) {
            LOG_WARN("inc subpart assign failed", KR(ret), K(inc_subpart), K(*subpart));
          } else if (FALSE_IT(inc_subpart.set_part_storage_cache_policy_type(storage_cache_policy_type))) {
          } else if (FALSE_IT(inc_part.set_part_id(part->get_part_id()))) {
          } else if (FALSE_IT(inc_part.set_part_idx(part->get_part_idx()))) {
          } else if (OB_FAIL(inc_part.add_partition(inc_subpart))) {
            LOG_WARN("inc part add inc subpart failed", KR(ret), K(inc_part), K(inc_subpart));
          } else if (OB_FAIL(alter_table_schema.add_partition(inc_part))) {
            LOG_WARN("alter table add inc part failed", KR(ret), K(alter_table_schema), K(inc_part));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(alter_table_schema.get_part_option().assign(ori_part_option))) {
            LOG_WARN("alter table set part option failed", KR(ret), K(alter_table_schema),
                K(ori_part_option));
          } else {
            alter_table_schema.get_part_option().set_part_num(alter_table_schema.get_partition_num());
            alter_table_schema.set_part_level(part_level);
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_index_storage_cache_policy(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (T_INDEX_ALTER_STORAGE_CACHE_POLICY != node.type_
      || 2 != node.num_child_
      || OB_ISNULL(node.children_[0])
      || OB_ISNULL(node.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree when alter index storage cache policy",
        K(ret), K(node.num_child_), K(node.type_));
  } else {
    ParseNode *index_node = node.children_[0];
    ParseNode *policy_node = node.children_[1];
    if (OB_ISNULL(index_node) || T_IDENT != index_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid index node", KP(index_node), K(ret));
    } else if (OB_ISNULL(policy_node)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid storage cache policy node", K(ret), K(node.num_child_), K(policy_node->type_));
    } else {
      ObString alter_index_name;
      alter_index_name.assign_ptr(index_node->str_value_,
                                 static_cast<int32_t>(index_node->str_len_));
      // construct ObAlterIndexArg
      ObAlterIndexArg *alter_index_arg = nullptr;
      void *tmp_ptr = nullptr;
      if (OB_UNLIKELY(nullptr == (tmp_ptr = allocator_->alloc(sizeof(obrpc::ObAlterIndexArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
      } else if (OB_FAIL(resolve_storage_cache_attribute(policy_node, params_, true/*is_index_option*/))) {
        LOG_WARN("fail to resolve storage cache policy attribute", K(ret));
      } else {
        alter_index_arg = new (tmp_ptr) ObAlterIndexArg();
        alter_index_arg->tenant_id_ = session_info_->get_effective_tenant_id();
        alter_index_arg->index_name_ = alter_index_name;
        alter_index_arg->storage_cache_policy_ = index_storage_cache_policy_;
      }
      if (OB_SUCC(ret)) {
        ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (OB_FAIL(alter_table_stmt->add_index_arg(alter_index_arg))) {
          SQL_RESV_LOG(WARN, "add index to alter_index_list failed!", K(ret));
        }
      }
    }
  }
  // reset storage cache policy, cause it will be reused in alter table options and alter add index options
  index_storage_cache_policy_.reset();
  return ret;
}


int ObStorageCacheUtil::print_storage_cache_policy_element(const ObBasePartition *partition, char* buf, const int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = OB_INVALID_VERSION;
  if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition is null", K(ret), K(partition));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(partition->get_tenant_id(), compat_version))) {
    LOG_WARN("get min data_version failed", K(ret), K(partition->get_tenant_id()));
  } else if (compat_version < DATA_VERSION_4_4_1_0) {
    // do nothing
  } else {
    ObStorageCachePolicyType storage_cache_policy_type = partition->get_part_storage_cache_policy_type();
    if (is_hot_or_auto_policy(storage_cache_policy_type)) {
      const char *storage_cache_policy_str = nullptr;
      if (OB_FAIL(ObStorageCacheGlobalPolicy::safely_get_str(storage_cache_policy_type, storage_cache_policy_str))) {
        LOG_WARN("failed to get storage cache policy str", K(ret), K(storage_cache_policy_type));
      } else if (OB_ISNULL(storage_cache_policy_str)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage cache policy str is null", K(ret), K(storage_cache_policy_type));
      } else {
        ObString storage_cache_policy(storage_cache_policy_str);
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " storage_cache_policy = \'%.*s\'",
                                    storage_cache_policy.length(), storage_cache_policy.ptr()))) {
          LOG_WARN("failed to print storage cache policy", K(ret), K(storage_cache_policy_type));
        }
      }
    }
  }
  return ret;
}

int ObStorageCacheUtil::print_table_storage_cache_policy(const ObTableSchema &table_schema, char* buf, const int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicy storage_cache_policy;
  if (OB_ISNULL(buf) || buf_len <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(buf_len), K(pos));
  } else if (is_storage_cache_policy_default(table_schema.get_storage_cache_policy())) {
    // skip not set
  } else if (OB_FAIL(storage_cache_policy.load_from_string(table_schema.get_storage_cache_policy()))) {
    LOG_WARN("failed to load storage cache policy", K(ret));
  } else if (storage_cache_policy.is_global_policy()) {
    if (ObStorageCacheGlobalPolicy::MAX_POLICY == storage_cache_policy.get_global_policy()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage cache policy", K(ret), K(storage_cache_policy));
    } else {
      const char *global_policy_str = nullptr;
      if (OB_FAIL(ObStorageCacheGlobalPolicy::safely_get_str(storage_cache_policy.get_global_policy(), global_policy_str))) {
        LOG_WARN("failed to get global policy", K(ret), K(storage_cache_policy));
      } else if (OB_ISNULL(global_policy_str)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("global policy str is null", K(ret), K(storage_cache_policy));
      } else {
        ObString global_policy(global_policy_str);
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "storage_cache_policy (global = \'%.*s\')  ",
                                  global_policy.length(), global_policy.ptr()))) {
          LOG_WARN("failed to print storage cache policy", K(ret), K(storage_cache_policy));
        }
      }
    }
  } else if (storage_cache_policy.is_time_policy()) {
    if (OB_ISNULL(storage_cache_policy.get_column_name()) ||
        0 == storage_cache_policy.get_hot_retention_interval()||
        !storage_cache_policy.is_valid_date_unit_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage cache policy", K(ret), K(storage_cache_policy));
    } else {
      ObString column_name(storage_cache_policy.get_column_name());
      const char *column_unit_str = nullptr;
      if (ObBoundaryColumnUnit::is_valid(storage_cache_policy.get_column_unit())) {
        if (OB_FAIL(ObBoundaryColumnUnit::safely_get_str(storage_cache_policy.get_column_unit(), column_unit_str))) {
          LOG_WARN("failed to get column unit", K(ret), K(storage_cache_policy));
        } else if (OB_ISNULL(column_unit_str)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column unit str is null", K(ret), K(storage_cache_policy));
        }
      }
      ObString column_unit(column_unit_str);
      ObString hot_retention_unit(ob_date_unit_type_str_upper(storage_cache_policy.get_hot_retention_unit()));

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "storage_cache_policy (boundary_column = %.*s, ",
                                  column_name.length(), column_name.ptr()))) {
        LOG_WARN("failed to print boudary column", K(ret), K(storage_cache_policy), K(pos));
      } else if (ObBoundaryColumnUnit::is_valid(storage_cache_policy.get_column_unit()) &&
                OB_FAIL(databuff_printf(buf, buf_len, pos, "boundary_column_unit = \'%.*s\', ",
                                                            column_unit.length(), column_unit.ptr()))) {
        LOG_WARN("failed to pint column unit", K(ret), K(storage_cache_policy), K(pos));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "hot_retention = %lu ",
                                                            storage_cache_policy.get_hot_retention_interval()))) {
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s)  ", hot_retention_unit.length(),
                                                                      hot_retention_unit.ptr()))) {
        LOG_WARN("failed to print storage cache policy", K(ret), K(storage_cache_policy), K(pos));
      }
    }
  }
  return ret;
}

int ObStorageCacheUtil::check_alter_column_validation(const AlterColumnSchema *alter_column_schema, const ObTableSchema &orig_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_DDL_CHANGE_COLUMN == alter_column_schema->alter_type_ ||
      OB_DDL_MODIFY_COLUMN == alter_column_schema->alter_type_ ) {
    const ObColumnSchemaV2 *orig_column = nullptr;
    if (OB_ISNULL(orig_column = orig_table_schema.get_column_schema(alter_column_schema->get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(ret), KPC(alter_column_schema), K(orig_table_schema));
    }
    // check for storage cache policy
    ObStorageCachePolicy cache_policy;
    if (OB_FAIL(ret)) {
    } else if (is_storage_cache_policy_default(orig_table_schema.get_storage_cache_policy())) {
      // not set storage cache policy, skip it
    } else if (OB_FAIL(cache_policy.load_from_string(orig_table_schema.get_storage_cache_policy()))) {
      LOG_WARN("failed to load cache policy form str", K(ret));
    } else if (cache_policy.is_time_policy()) {
      // time storage cache policy, check column name
      const ObString &ori_column_name = orig_column->get_column_name_str();
      const ObString &boundary_column_name = ObString(cache_policy.get_column_name());
      if (ObColumnNameHashWrapper(ori_column_name) == ObColumnNameHashWrapper(boundary_column_name)) {
        if (!ObStorageCacheUtil::is_type_change_allow(orig_column->get_data_type(), alter_column_schema->get_data_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Changing storage cache policy boundary column type is");
        LOG_WARN("not supported column type change on the boundary column, please remove the storage cache policy first.",
            K(ret), K(orig_column), KPC(alter_column_schema), K(cache_policy.get_column_name()));
        }
      }
    }
  }
  return ret;
}

int ObStorageCacheUtil::get_tenant_table_ids(const uint64_t tenant_id, ObIArray<uint64_t> &table_id_array)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  share::schema::ObMultiVersionSchemaService &schema_service = share::schema::ObMultiVersionSchemaService::get_instance();
  if (!schema_service.is_tenant_full_schema(tenant_id)) {
    ret = OB_EAGAIN;
    LOG_INFO("tenant does not has a full schema already, maybe server is restart, need retry!");
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id, table_id_array))) {
    LOG_WARN("fail to get table ids in tenant", KR(ret), K(tenant_id));
  }
  return ret;
}

bool ObStorageCacheUtil::is_type_change_allow(const ObObjType &src_type, const ObObjType &dst_type)
{
  bool ret = true;
  if ((!ob_is_datetime_tc(src_type) && !ob_is_int_tc(src_type)) ||
      (!ob_is_datetime_tc(dst_type) && !ob_is_int_tc(dst_type))) {
    ret = false;
    LOG_WARN("invalid type", K(ret),K(src_type), K(dst_type));
  } else if (ob_is_datetime_tc(src_type) && !ob_is_datetime_tc(dst_type)) { /* not allow time to other */
    ret = false;
    LOG_WARN("not allow to change tpe`", K(ret),K(src_type), K(dst_type));
  } else if (ob_is_int_tc(src_type) && ob_is_int_tc(dst_type) && /* not allow int decrease precison*/
             dst_type < src_type) {
    ret = false;
    LOG_WARN("not allow to decrease precision", K(ret),K(src_type), K(dst_type));
  }
  return ret;
}

int ObStorageCacheUtil::check_alter_partiton_storage_cache_policy(const share::schema::ObTableSchema &orig_table_schema,
                                                             const obrpc::ObAlterTableArg &alter_table_arg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  const ObPartitionLevel part_level = orig_table_schema.get_part_level();
  const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  const int64_t part_num = alter_table_schema.get_partition_num();
  ObPartition **part_array = alter_table_schema.get_part_array();
  ObPartition *inc_part = nullptr;
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  if (OB_FAIL(GET_MIN_DATA_VERSION(orig_table_schema.get_tenant_id(), tenant_data_version))) {
    LOG_WARN("get data version failed", KR(ret), K(orig_table_schema.get_tenant_id()));
  } else if (tenant_data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cluster version and feature mismatch", KR(ret));
  } else if (!orig_table_schema.is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user table", KR(ret), K(orig_table_schema));
  } else if (PARTITION_LEVEL_ZERO == part_level) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_USER_ERROR(OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED);
    LOG_WARN("unsupport management on non_partition table", KR(ret));
  } else if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part_array is null", KR(ret), KP(part_array));
  } else {
    // check if the partition name is existed
    const ObPartition *part = nullptr;
    for (int64_t i = 0; i < part_num; ++i) {
      inc_part = part_array[i];
      if (OB_ISNULL(inc_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc_part is null", KR(ret), KP(part_array));
      } else {
        const ObString &partition_name = inc_part->get_part_name();
        if (OB_FAIL(orig_table_schema.get_partition_by_name(partition_name, part))) {
          LOG_WARN("get part by name failed", KR(ret), K(partition_name), K(orig_table_schema));
        }
      }
    }
  }
  return ret;
}

int ObStorageCacheUtil::check_alter_subpartiton_storage_cache_policy(const share::schema::ObTableSchema &orig_table_schema,
                                                                const obrpc::ObAlterTableArg &alter_table_arg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  const ObPartitionLevel part_level = orig_table_schema.get_part_level();
  const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;

  if (OB_FAIL(GET_MIN_DATA_VERSION(orig_table_schema.get_tenant_id(), tenant_data_version))) {
    LOG_WARN("get data version failed", KR(ret), K(orig_table_schema.get_tenant_id()));
  } else if (tenant_data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cluster version and feature mismatch", KR(ret));
  } else if (!orig_table_schema.is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user table", KR(ret), K(orig_table_schema));
  } else if (PARTITION_LEVEL_ZERO == part_level || PARTITION_LEVEL_ONE == part_level) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_USER_ERROR(OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED);
    LOG_WARN("unsupport management on non_subpartitioned table", KR(ret));
  } else {
    const int64_t part_num = alter_table_schema.get_partition_num();
    ObPartition **part_array = alter_table_schema.get_part_array();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_array is null", KR(ret), KP(part_array));
    } else {
      // check if the subpartition name is existed
      const ObPartition *part = nullptr;
      const ObSubPartition *subpart = nullptr;
      for (int64_t i = 0; i < part_num; ++i) {
        ObPartition *inc_part = part_array[i];
        if (OB_ISNULL(inc_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inc_part is null", KR(ret), KP(part_array));
        } else {
          const int64_t subpart_num = inc_part->get_subpartition_num();
          ObSubPartition **subpart_array = inc_part->get_subpart_array();
          if (OB_ISNULL(subpart_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("subpart_array is null", KR(ret), KP(part_array));
          } else {
            for (int64_t j = 0; j < subpart_num; ++j) {
              if (OB_ISNULL(subpart_array[j])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("subpart_array is null", KR(ret), KP(part_array));
              } else {
                const ObString &subpartition_name = subpart_array[j]->get_part_name();
                if (OB_FAIL(orig_table_schema.get_subpartition_by_name(subpartition_name, part, subpart))) {
                  LOG_WARN("get subpart by name failed", KR(ret), K(subpartition_name), K(orig_table_schema));
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
int ObStorageCacheUtil::get_range_part_level(const share::schema::ObTableSchema &tbl_schema, const ObStorageCachePolicy &storage_cache_policy,
                                             int32_t &part_level)
{
  int ret = OB_SUCCESS;
  if (storage_cache_policy.is_time_policy() && !tbl_schema.is_partitioned_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tbl_schema));
  } else {
    const share::schema::ObColumnSchemaV2 *column_schema = nullptr;
    ObString column_name(storage_cache_policy.get_column_name());
    if (OB_ISNULL(column_schema = tbl_schema.get_column_schema(column_name))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "Boundary column not exist");
      LOG_WARN("storage cache policy boundary column does not exist", K(ret), K(column_name));
    } else if (storage_cache_policy.is_time_policy()) {
      // Check if the column is the firstpartition key column
      // and the partition function is range or range columns
      share::schema::ObPartitionLevel table_part_level = tbl_schema.get_part_level();
      uint64_t column_id = OB_INVALID_ID;
      if (share::schema::PARTITION_LEVEL_ONE == table_part_level) {
        if (!tbl_schema.is_range_part()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("range partition type is not supported for storage cache policy", K(ret));
        } else {
          const ObPartitionKeyInfo &part_key_info = tbl_schema.get_partition_key_info();
          column_id = column_schema->get_column_id();
          if (OB_FAIL(check_column_is_first_part_key(part_key_info, column_id))) {
            LOG_WARN("check column is first part key failed", K(ret), K(column_id));
          } else {
            part_level = share::schema::PARTITION_LEVEL_ONE;
          }
        }
      } else if (share::schema::PARTITION_LEVEL_TWO == table_part_level) {
        // When the partition level is two
        // Check if the column is the first partition key column or the first subpartition key column
        if (tbl_schema.is_range_part() && tbl_schema.is_range_subpart()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("range partition and range subpartition are not supported for storage cache policy", K(ret));
        } else if (tbl_schema.is_range_part() && !tbl_schema.is_range_subpart()) {
          if (OB_FAIL(check_column_is_first_part_key(tbl_schema.get_partition_key_info(), column_schema->get_column_id()))) {
            LOG_WARN("check column is first subpartition key failed", K(ret), K(column_schema->get_column_id()));
          } else {
            part_level = share::schema::PARTITION_LEVEL_ONE;
          }
        } else if (!tbl_schema.is_range_part() && tbl_schema.is_range_subpart()) {
          if (OB_FAIL(check_column_is_first_part_key(tbl_schema.get_subpartition_key_info(), column_schema->get_column_id()))) {
            LOG_WARN("check column is first subpartition key failed", K(ret), K(column_schema->get_column_id()));
          } else {
            part_level = share::schema::PARTITION_LEVEL_TWO;
          }
        } else if (!tbl_schema.is_range_part() && !tbl_schema.is_range_subpart()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("there must be a range type partition in the partition and the sub_partition for storage cache policy", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObStorageCacheUtil::check_column_is_first_part_key(const ObPartitionKeyInfo &part_key_info, const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  uint64_t pkey_col_id = OB_INVALID_ID;
  if (OB_FAIL(part_key_info.get_column_id(0, pkey_col_id))) {
    LOG_WARN("get_column_id failed", "index", 0, K(ret));
  } else if (pkey_col_id != column_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the column is not the first part key column", K(ret), K(pkey_col_id), K(column_id));
  }
  return ret;
}
} //end of namespace sql
} //end of namespace oceanbase
