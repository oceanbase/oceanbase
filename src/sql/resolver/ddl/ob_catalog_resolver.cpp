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
#include "sql/resolver/ddl/ob_catalog_resolver.h"

#include "share/catalog/ob_catalog_properties.h"
#include "share/catalog/ob_catalog_utils.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ddl/ob_catalog_stmt.h"
#include "share/ob_license_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

ObCatalogResolver::ObCatalogResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObCatalogResolver::~ObCatalogResolver()
{
}

int ObCatalogResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCatalogStmt *stmt = NULL;
  stmt::StmtType stmt_type = stmt::T_NONE;
  uint64_t data_version = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("external catalog not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external catalog");
  } else if (OB_FAIL(ObLicenseUtils::check_olap_allowed(tenant_id))) {
    ret = OB_LICENSE_SCOPE_EXCEEDED;
    LOG_WARN("catalog is not allowed", KR(ret));
    LOG_USER_ERROR(OB_LICENSE_SCOPE_EXCEEDED, "catalog is not supported due to the absence of the OLAP module");
  } else if (OB_ISNULL(stmt = create_stmt<ObCatalogStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create create_catalog_stmt", K(ret));
  } else {
    obrpc::ObCatalogDDLArg &arg = stmt->get_ddl_arg();
    // tenant_id, user_id
    arg.schema_.set_tenant_id(session_info_->get_effective_tenant_id());
    arg.user_id_ = session_info_->get_user_id();
    // stmt_type
    switch (parse_tree.type_) {
      case T_CREATE_CATALOG:
        arg.ddl_type_ = OB_DDL_CREATE_CATALOG;
        stmt->set_stmt_type(stmt::T_CREATE_CATALOG);
        ret = resolve_create_catalog(parse_tree, arg);
        break;
      case T_ALTER_CATALOG:
        // TODO@LINYI
        // arg.ddl_type_ = OB_DDL_ALTER_CATALOG;
        // stmt_type = stmt::T_ALTER_CATALOG;
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported ddl type", K(ret), K(parse_tree.type_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter catalog ddl");
        break;
      case T_DROP_CATALOG:
        arg.ddl_type_ = OB_DDL_DROP_CATALOG;
        stmt->set_stmt_type(stmt::T_DROP_CATALOG);
        ret = resolve_drop_catalog(parse_tree, arg);
        break;
      case T_SET_CATALOG:
        stmt->set_stmt_type(stmt::T_SET_CATALOG);
        ret = resolve_set_catalog(parse_tree, arg);
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported ddl type", K(ret), K(parse_tree.type_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specified ddl type");
    }
  }
  // Check oracle privileges
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()
      && parse_tree.type_ != T_SET_CATALOG) {
    CK (schema_checker_ != NULL);
    OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                            session_info_->get_priv_user_id(),
                                            ObString(),
                                            stmt->get_stmt_type(),
                                            session_info_->get_enable_role_array()));
  }
  return ret;
}

int ObCatalogResolver::resolve_create_catalog(const ParseNode &parse_tree, obrpc::ObCatalogDDLArg &arg)
{
  int ret = OB_SUCCESS;
  ParseNode *name_node = NULL;
  ParseNode *properties_node = NULL;
  if (OB_UNLIKELY(parse_tree.type_ != T_CREATE_CATALOG) || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(parse_tree.num_child_ != CREATE_NUM_CHILD)
      || OB_ISNULL(name_node = parse_tree.children_[CATALOG_NAME])
      || OB_ISNULL(properties_node = parse_tree.children_[PROPERTIES])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  }
  if (OB_SUCC(ret) && parse_tree.children_[IF_NOT_EXIST] != NULL) {
    arg.if_not_exist_ = true;
  }
  OZ(resolve_catalog_name(*name_node, arg));
  OZ(resolve_catalog_properties(*properties_node, arg));
  return ret;
}

int ObCatalogResolver::resolve_drop_catalog(const ParseNode &parse_tree, obrpc::ObCatalogDDLArg &arg)
{
  int ret = OB_SUCCESS;
  ParseNode *name_node = NULL;
  uint64_t catalog_id = OB_INVALID_ID;
  ObString catalog_name;
  if (OB_UNLIKELY(parse_tree.type_ != T_DROP_CATALOG) || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(parse_tree.num_child_ != DROP_NUM_CHILD)
      || OB_ISNULL(name_node = parse_tree.children_[CATALOG_NAME])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  }
  if (OB_SUCC(ret) && parse_tree.children_[IF_EXIST] != NULL) {
    arg.if_exist_ = true;
  }
  OZ(resolve_catalog_name(*name_node, arg));
  catalog_name = arg.schema_.get_catalog_name_str();
  OZ(schema_checker_->get_catalog_id_name(session_info_->get_effective_tenant_id(),
                                          catalog_name,
                                          catalog_id,
                                          NULL,
                                          arg.if_exist_));
  OX(arg.schema_.set_catalog_id(catalog_id));
  OZ(arg.schema_.set_catalog_name(catalog_name));
  return ret;
}

int ObCatalogResolver::resolve_set_catalog(const ParseNode &parse_tree, obrpc::ObCatalogDDLArg &arg)
{
  int ret = OB_SUCCESS;
  ParseNode *name_node = NULL;
  uint64_t catalog_id = OB_INVALID_ID;
  ObString catalog_name;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  share::schema::ObSessionPrivInfo session_priv;
  if (OB_UNLIKELY(parse_tree.type_ != T_SET_CATALOG) || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(parse_tree.num_child_ != SET_NUM_CHILD)
      || OB_ISNULL(name_node = parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)
             || OB_ISNULL(schema_guard = schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(schema_checker_), K(session_info_));
  }
  OZ(resolve_catalog_name(*name_node, arg));
  if (arg.schema_.get_catalog_id() != OB_INTERNAL_CATALOG_ID) {
    catalog_name = arg.schema_.get_catalog_name_str();
    OZ(schema_checker_->get_catalog_id_name(session_info_->get_effective_tenant_id(),
                                            catalog_name,
                                            catalog_id));
    OX(arg.schema_.set_catalog_id(catalog_id));
    OZ(arg.schema_.set_catalog_name(catalog_name));
    OZ(session_info_->get_session_priv_info(session_priv));
    OZ(schema_guard->check_catalog_access(session_priv, session_info_->get_enable_role_array(), catalog_name));
  }
  return ret;
}

int ObCatalogResolver::resolve_catalog_name(const ParseNode &name_node, obrpc::ObCatalogDDLArg &arg)
{
  int ret = OB_SUCCESS;
  ObString catalog_name;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_BIN;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  catalog_name.assign_ptr(name_node.str_value_, static_cast<int32_t>(name_node.str_len_));
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(*allocator_,
                                                                        session_info_->get_dtc_params(),
                                                                        catalog_name))) {
    LOG_WARN("failed to convert sql text", K(ret), K(catalog_name));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("failed to get name case mode", K(ret));
  } else if (is_mysql_mode() && OB_LOWERCASE_AND_INSENSITIVE == case_mode
             && OB_FAIL(ObCharset::tolower(cs_type, catalog_name, catalog_name, *allocator_))) {
    LOG_WARN("failed to lower string", K(ret));
  } else if (OB_FAIL(arg.schema_.set_catalog_name(catalog_name))) {
    LOG_WARN("failed to set catalog name", K(ret));
  }
  if (OB_SUCC(ret) && ObCatalogUtils::is_internal_catalog_name(catalog_name, case_mode)) {
    if (stmt_->stmt_type_ == stmt::T_CREATE_CATALOG || stmt_->stmt_type_ == stmt::T_DROP_CATALOG) {
      ret = OB_CATALOG_DDL_ON_INTERNAL;
      LOG_WARN("create or drop internal catalog is not allowed", K(ret));
    } else if (stmt_->stmt_type_ == stmt::T_SET_CATALOG) {
      arg.schema_.set_catalog_id(OB_INTERNAL_CATALOG_ID);
    }
  }
  return ret;
}

int ObCatalogResolver::resolve_catalog_properties(const ParseNode &properties_node, obrpc::ObCatalogDDLArg &arg)
{
  int ret = OB_SUCCESS;
  ObCatalogProperties::CatalogType type;
  ObString catalog_properties;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObCatalogProperties::resolve_catalog_type(properties_node, type))) {
    LOG_WARN("failed to resolve catalog type", K(ret));
  } else {
    switch (type)
    {
      case ObCatalogProperties::CatalogType::ODPS_TYPE:
      {
        ObODPSCatalogProperties properties;
        OZ(properties.resolve_catalog_properties(properties_node));
        OZ(properties.encrypt(*allocator_));
        OZ(properties.to_string_with_alloc(catalog_properties, *allocator_));
        break;
      }
      default:
      {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported catalog type", K(type));
      }
    }
    OZ(arg.schema_.set_catalog_properties(catalog_properties));
  }
  return ret;
}

} //end namespace sql
} //end namespace oceanbase
