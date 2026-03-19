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

#include "sql/resolver/ddl/ob_alter_view_resolver.h"
#include "sql/resolver/ddl/ob_alter_view_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/virtual_table/ob_table_columns.h"
#include "sql/executor/ob_maintain_dependency_info_task.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

ObAlterViewResolver::ObAlterViewResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObAlterViewResolver::~ObAlterViewResolver()
{
}

int ObAlterViewResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info should not be null", K(ret));
  } else if (T_ALTER_VIEW != parse_tree.type_ ||
             3 != parse_tree.num_child_ ||
             OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), K(parse_tree.type_), K(parse_tree.num_child_));
  } else {
    ObAlterViewStmt *alter_view_stmt = NULL;
    if (OB_ISNULL(alter_view_stmt = create_stmt<ObAlterViewStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create alter view stmt", K(ret));
    } else {
      stmt_ = alter_view_stmt;
      if (OB_FAIL(resolve_alter_view(parse_tree))) {
        LOG_WARN("failed to resolve view compile", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterViewResolver::resolve_alter_view(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObAlterViewStmt *alter_view_stmt = static_cast<ObAlterViewStmt*>(stmt_);
  ObString view_name;
  ObString database_name;
  const ObTableSchema *view_schema = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  bool if_exists = false;

  if (OB_ISNULL(alter_view_stmt) || OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(alter_view_stmt), KP(session_info_), KP(schema_checker_));
  } else if (OB_ISNULL(schema_guard = schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", K(ret));
  } else {
    // Check ALTER_ACTION node
    ParseNode *action_node = parse_tree.children_[ALTER_ACTION];
    if (OB_ISNULL(action_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alter action node is null", K(ret));
    } else {
      // Check IF EXISTS flag
      ParseNode *if_exists_node = parse_tree.children_[IF_EXISTS];
      if_exists = (NULL != if_exists_node);
      ParseNode *view_name_node = parse_tree.children_[VIEW_NAME];
      if (OB_ISNULL(view_name_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("view name node is null", K(ret));
      } else if (OB_FAIL(resolve_table_relation_node(view_name_node, view_name, database_name))) {
        LOG_WARN("failed to resolve table relation node", K(ret));
      } else {
        uint64_t tenant_id = session_info_->get_effective_tenant_id();
        uint64_t database_id = OB_INVALID_ID;
        if (OB_FAIL(schema_guard->get_database_id(tenant_id, database_name, database_id))) {
          LOG_WARN("failed to get database id", K(ret), K(tenant_id), K(database_name));
        } else if (OB_INVALID_ID == database_id) {
          if (if_exists) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_BAD_DATABASE;
            LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
          }
        } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id, database_id, view_name,
                                                            false /* not index table */, view_schema))) {
          LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(database_id), K(view_name));
        } else if (OB_ISNULL(view_schema)) {
          if (if_exists) {
            LOG_INFO("view not found with IF EXISTS, skip", K(view_name));
            ret = OB_SUCCESS;
          } else {
            ObCStringHelper helper;
            ret = OB_TABLE_NOT_EXIST;
            LOG_USER_ERROR(OB_TABLE_NOT_EXIST, helper.convert(database_name), helper.convert(view_name));
          }
        } else if (!view_schema->is_view_table() || view_schema->is_materialized_view()) {
          ret = OB_ERR_WRONG_OBJECT;
          ObCStringHelper helper;
          LOG_USER_ERROR(OB_ERR_WRONG_OBJECT, helper.convert(database_name), helper.convert(view_name), "VIEW");
        } else {
          obrpc::ObAlterViewArg &alter_view_arg = alter_view_stmt->get_alter_view_arg();
          alter_view_arg.tenant_id_ = tenant_id;
          alter_view_arg.view_id_ = view_schema->get_table_id();
          alter_view_arg.database_id_ = database_id;
          alter_view_arg.exec_tenant_id_ = tenant_id;
          if (OB_FAIL(ob_write_string(*allocator_, database_name, alter_view_arg.database_name_))) {
            LOG_WARN("failed to copy database name", K(ret));
          } else if (OB_FAIL(ob_write_string(*allocator_, view_name, alter_view_arg.view_name_))) {
            LOG_WARN("failed to copy view name", K(ret));
          } else if (OB_FAIL(ob_write_string(*allocator_, session_info_->get_current_query_string(),
                                             alter_view_arg.ddl_stmt_str_))) {
            LOG_WARN("failed to copy ddl stmt str", K(ret));
          } else if (T_ALTER_VIEW_COMPILE == action_node->type_) {
            if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_2_1) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("version lower than 4.4.2.1 does not support this operation", K(ret));
            } else {
              alter_view_arg.alter_view_action_ = obrpc::ObAlterViewArg::COMPILE_VIEW;
              int tmp_ret = OB_SUCCESS;
              ObSelectStmt *expanded_view_stmt = NULL;
              if (OB_FAIL(observer::ObTableColumns::resolve_view_definition(
                      allocator_, session_info_, schema_guard, *view_schema, expanded_view_stmt,
                      *params_.expr_factory_, *params_.stmt_factory_,  false /* throw_error */,
                      &alter_view_arg.alter_view_compile_args_))) {
                if (OB_ERR_UNKNOWN_TABLE != ret && OB_ERR_VIEW_INVALID != ret) {
                  LOG_WARN("failed to resolve view definition",K(ret), K(alter_view_arg.view_name_), K(alter_view_arg.database_name_));
                } else {
                  alter_view_arg.target_view_error_code_ = ret;
                  ret = OB_SUCCESS;
                }
              } else {
                LOG_TRACE("resolve view compile", K(alter_view_arg.view_id_), K(alter_view_arg.database_id_),
                  K(alter_view_arg.database_name_), K(alter_view_arg.view_name_));
              }
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("only COMPILE action is supported now", K(ret), K(action_node->type_));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this ALTER VIEW action");
          }
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
