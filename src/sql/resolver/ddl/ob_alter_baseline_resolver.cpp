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

#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server_struct.h"
#include "sql/resolver/ddl/ob_alter_baseline_resolver.h"
#include "sql/resolver/ddl/ob_alter_baseline_stmt.h"
#include "sql/parser/parse_node.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace obrpc;
namespace sql {
int ObAlterBaselineResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObAlterBaselineStmt* stmt = NULL;
  if (OB_UNLIKELY(T_ALTER_BASELINE != parse_tree.type_ || parse_tree.num_child_ != 4) ||
      OB_ISNULL(parse_tree.children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "type", get_type_name(parse_tree.type_), "child_num", parse_tree.num_child_);
  } else if (NULL == (stmt = create_stmt<ObAlterBaselineStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ObAlterBaselineStmt failed", K(ret));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    ParseNode* tenant_name_node = parse_tree.children_[0];
    ParseNode* sql_id_node = parse_tree.children_[1];
    ParseNode* baseline_id_node = parse_tree.children_[2];
    ParseNode* update_field_node = parse_tree.children_[3];
    // tenant_name
    if (NULL == tenant_name_node) {
      // do nothing
    } else if (NULL == tenant_name_node->children_ || T_TENANT_NAME != tenant_name_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid argument", K(ret));
    } else if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid argument", K(GCTX.schema_service_));
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_NOT_INIT;
      SERVER_LOG(WARN, "should have set session info", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   session_info_->get_effective_tenant_id(), schema_guard))) {
      SERVER_LOG(WARN, "get_schema_guard failed", K(ret));
    } else {
      uint64_t tenant_id = 0;
      ObString tenant_name;
      if (OB_ISNULL(tenant_name_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid argument", K(tenant_name_node->children_[0]), K(ret));
      } else {
        tenant_name.assign_ptr(tenant_name_node->children_[0]->str_value_,
            static_cast<ObString::obstr_size_t>(tenant_name_node->children_[0]->str_len_));
        if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id))) {
          SERVER_LOG(WARN, "tenant not exist", K(tenant_name), K(ret));
        } else {
          stmt->alter_baseline_arg_.plan_baseline_info_.key_.tenant_id_ = tenant_id;
        }
      }
    }

    // sql_id
    if (OB_FAIL(ret)) {
      // do_nothing
    } else if (OB_ISNULL(sql_id_node)) {
      // do_nothing
    } else if (OB_ISNULL(sql_id_node->children_) || OB_ISNULL(sql_id_node->children_[0]) ||
               T_SQL_ID != sql_id_node->type_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      stmt->alter_baseline_arg_.plan_baseline_info_.sql_id_.assign_ptr(sql_id_node->children_[0]->str_value_,
          static_cast<ObString::obstr_size_t>(sql_id_node->children_[0]->str_len_));
    }

    // baseline_id
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(baseline_id_node)) {
      // do nothing
    } else if (OB_ISNULL(baseline_id_node->children_) || OB_ISNULL(baseline_id_node->children_[0]) ||
               T_BASELINE_ID != baseline_id_node->type_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      stmt->alter_baseline_arg_.plan_baseline_info_.plan_baseline_id_ = baseline_id_node->children_[0]->value_;
    }

    // update field
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(update_field_node)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("alter baseline attribute can't NULL", K(ret));
    } else if (T_ASSIGN_ITEM != update_field_node->type_ || OB_ISNULL(update_field_node->children_) ||
               2 != update_field_node->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      ParseNode* attr_name_node = update_field_node->children_[0];
      ParseNode* attr_value_node = update_field_node->children_[1];
      if (OB_ISNULL(attr_name_node) || OB_ISNULL(attr_value_node)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(attr_name_node), K(attr_value_node), K(ret));
      } else {
        ObString name;
        name.assign_ptr(attr_name_node->str_value_, static_cast<ObString::obstr_size_t>(attr_name_node->str_len_));
        if (0 == name.case_compare("outline_data")) {
          if (T_VARCHAR != attr_value_node->type_ && T_CHAR != attr_value_node->type_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid update field type", K(attr_value_node->type_), K(name), K(ret));
          } else {
            stmt->alter_baseline_arg_.field_update_bitmap_ = ObAlterPlanBaselineArg::OUTLINE_DATA;
            stmt->alter_baseline_arg_.plan_baseline_info_.outline_data_.assign_ptr(
                attr_value_node->str_value_, static_cast<ObString::obstr_size_t>(attr_value_node->str_len_));
          }
        }
      }
    }  // update field end
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
