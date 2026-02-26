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

 #define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_alter_location_resolver.h"
#include "sql/resolver/ddl/ob_create_location_stmt.h"
#include "lib/restore/ob_storage_info.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "share/external_table/ob_external_table_utils.h"

namespace oceanbase
{
namespace sql
{
ObAlterLocationResolver::ObAlterLocationResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObAlterLocationResolver::~ObAlterLocationResolver()
{
}

int ObAlterLocationResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCreateLocationStmt *create_location_stmt = NULL;
  uint64_t data_version = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_4_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("external location not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external location");
  } else if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_ALTER_LOCATION)
      || OB_UNLIKELY(node->num_child_ != LOCATION_NODE_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node children", K(ret), K(node), K(node->children_));
  } else if (OB_ISNULL(create_location_stmt = create_stmt<ObCreateLocationStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to get create location stmt", K(ret));
  } else {
    stmt_ = create_location_stmt;
    create_location_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    create_location_stmt->set_user_id(session_info_->get_user_id());
    create_location_stmt->set_or_replace(true);
  }

  // location name
  ObString location_name;
  uint64_t alter_type = OB_INVALID_ID;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_BIN;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  int max_name_length = lib::is_oracle_mode() ? OB_MAX_LOCATION_NAME_LENGTH : OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL;
  if (OB_SUCC(ret)) {
    ParseNode *child_node = node->children_[LOCATION_NAME];
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (child_node->str_len_ >= max_name_length) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(child_node->str_len_), child_node->str_value_);
    } else if (FALSE_IT(location_name.assign_ptr(child_node->str_value_, static_cast<int32_t>(child_node->str_len_)))) {
      // do nothing
    } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
      LOG_WARN("failed to get name case mode", K(ret));
    } else if (is_mysql_mode() && OB_LOWERCASE_AND_INSENSITIVE == case_mode
               && OB_FAIL(ObCharset::tolower(cs_type, location_name, location_name, *allocator_))) {
      LOG_WARN("failed to lower string", K(ret));
    } else if (OB_FAIL(create_location_stmt->set_location_name(location_name))) {
      LOG_WARN("set location name failed", K(ret));
    } else {
      alter_type = child_node->value_;
    }
  }

  // get schema
  const ObLocationSchema *location_schema = NULL;
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard *schema_guard = NULL;
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    if (NULL == (schema_guard = schema_checker_->get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is null", K(ret));
    } else if (OB_FAIL(schema_guard->get_location_schema_by_name(tenant_id, location_name, location_schema))) {
      LOG_WARN("failed to get schema by location name", K(ret), K(tenant_id), K(location_name));
    } else if (OB_ISNULL(location_schema)) {
      ret = OB_LOCATION_OBJ_NOT_EXIST;
      LOG_WARN("location object does't exist", K(ret), K(tenant_id), K(location_name));
    }
  }

  // url
  ObString location_url;
  if (OB_SUCC(ret)) {
    if (alter_type == 1) {
      ParseNode *child_node = node->children_[LOCATION_MODIFY];
      if (OB_ISNULL(child_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse tree", K(ret));
      } else if (child_node->str_len_ >= OB_MAX_LOCATION_URL_LENGTH) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(child_node->str_len_), child_node->str_value_);
      } else if (FALSE_IT(location_url.assign_ptr(child_node->str_value_, static_cast<int32_t>(child_node->str_len_)))){

      } else {
        ObString access_info = location_schema->get_location_access_info_str();
        const bool is_hdfs_type = location_url.prefix_match(OB_HDFS_PREFIX);
        ObHDFSStorageInfo hdfs_storage_info;
        ObBackupStorageInfo back_up_backup;
        ObObjectStorageInfo *storage_info = nullptr;
        if (OB_LIKELY(is_hdfs_type)) {
          storage_info = &hdfs_storage_info;
        } else {
          storage_info = &back_up_backup;
        }
        // verify url
        if (OB_FAIL(storage_info->set(location_url.ptr(), access_info.ptr()))) {
          LOG_WARN("failed validate url and access info", K(ret));
        } else {
          create_location_stmt->set_location_url(location_url);
          create_location_stmt->set_location_access_info(access_info);
        }
      }
    } else {
      location_url = location_schema->get_location_url_str();
    }
  }

  // credential info
  ObSqlString credential_params;
  ObString cur_sql = session_info_->get_current_query_string();
  if (OB_SUCC(ret) && alter_type == 2) {
    ParseNode *child_node = node->children_[LOCATION_MODIFY];
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (T_CREDENTIAL_OPTION_LIST != child_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument.", K(ret), K(child_node->type_));
    } else {
      ParseNode *option_node = NULL;
      int32_t num = child_node->num_child_;
      for (int32_t i = 0; OB_SUCC(ret) && i < num; ++i) {
        option_node = child_node->children_[i];
        if (OB_ISNULL(option_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid argument.", K(ret));
        } else if (OB_FAIL(ObExternalTableUtils::get_credential_field_name(
                            credential_params, option_node->value_))) {
          LOG_WARN("failed to get field name", K(ret), K(option_node->value_));
        } else {
          ObString tmp;
          tmp.assign_ptr(option_node->str_value_, static_cast<int32_t>(option_node->str_len_));
          credential_params.append(tmp);
          if (i != num - 1) {
            credential_params.append(common::SEPERATE_SYMBOL);
          }
          if (OB_SUCC(ret) && option_node->value_ >= 1 && option_node->value_ <= 3) {
            ObString masked_sql;
            if (OB_FAIL(ObDCLResolver::mask_password_for_passwd_node(
                          allocator_, cur_sql, option_node, masked_sql, true))) {
              LOG_WARN("fail to gen masked sql", K(ret));
            } else {
              cur_sql = masked_sql;
            }
          }
        }
      }
    }
    // check url and aksk
    // url like: oss://bucket/...?host=xxxx&access_id=xxx&access_key=xxx
    const bool is_hdfs_type = location_url.prefix_match(OB_HDFS_PREFIX);
    ObHDFSStorageInfo hdfs_storage_info;
    ObBackupStorageInfo back_up_backup;
    ObObjectStorageInfo *storage_info = nullptr;
    if (OB_LIKELY(is_hdfs_type)) {
      storage_info = &hdfs_storage_info;
    } else {
      storage_info = &back_up_backup;
    }
    char storage_info_buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
    ObString uri_cstr = location_url;
    ObString storage_info_cstr = credential_params.string();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(storage_info->set(uri_cstr.ptr(), storage_info_cstr.ptr()))) {
        LOG_WARN("failed to set storage info", K(ret));
      } else if (OB_FAIL(storage_info->get_storage_info_str(storage_info_buf, sizeof(storage_info_buf)))) {
        LOG_WARN("failed to get storage info str", K(ret));
      } else if (OB_FAIL(create_location_stmt->set_location_url(location_url))) {
        LOG_WARN("failed to set external file location", K(ret));
      } else if (OB_FAIL(create_location_stmt->set_location_access_info(storage_info_buf))) {
        LOG_WARN("failed to set external file location access info", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    create_location_stmt->set_masked_sql(cur_sql);
  }

  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ(schema_checker_->check_ora_ddl_priv(
       session_info_->get_effective_tenant_id(),
       session_info_->get_priv_user_id(),
       ObString(""),
       stmt::T_CREATE_LOCATION,
       session_info_->get_enable_role_array()),
       session_info_->get_effective_tenant_id(), session_info_->get_user_id());
  }
  return ret;
}
} // end namespace sql
} // end namespace oceanbase
