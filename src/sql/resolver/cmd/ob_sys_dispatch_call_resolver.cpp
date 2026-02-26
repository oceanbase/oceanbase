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

#include "sql/resolver/cmd/ob_sys_dispatch_call_resolver.h"

#include "lib/utility/ob_macro_utils.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/cmd/ob_sys_dispatch_call_config.h"
#include "sql/resolver/cmd/ob_sys_dispatch_call_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{

using namespace share::schema;

namespace sql
{

const char *const ObSysDispatchCallResolver::WHITELIST[][2] = {
#define ENTRY(package, subroutine) { #package, #subroutine },
  SYS_DISPATCH_CALL_WHITELIST(ENTRY)
#undef ENTRY
};

int ObSysDispatchCallResolver::check_sys_dispatch_call_priv(const ParseNode &name_node)
{
  int ret = OB_SUCCESS;
  ObString package_name;
  ObString routine_name;

  if (OB_UNLIKELY(T_SP_ACCESS_NAME != name_node.type_)
      || OB_UNLIKELY(3 != name_node.num_child_)
      || OB_NOT_NULL(name_node.children_[0])
      || OB_ISNULL(name_node.children_[1])
      || OB_ISNULL(name_node.children_[2])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sys dispatch call name node is invalid",
             K(name_node.type_),
             K(name_node.num_child_),
             K(name_node.children_));
  } else if (FALSE_IT(package_name.assign_ptr(name_node.children_[1]->str_value_,
                                              name_node.children_[1]->str_len_))) {
  } else if (FALSE_IT(routine_name.assign_ptr(name_node.children_[2]->str_value_,
                                              name_node.children_[2]->str_len_))) {
  } else {
    bool found = false;
    for (int64_t i = 0; i < ARRAYSIZEOF(WHITELIST); ++i) {
      if (package_name.case_compare(WHITELIST[i][0]) == 0
          && ('*' == WHITELIST[i][1][0]
              || 0 == routine_name.case_compare(WHITELIST[i][1]))) {
        found = true;
        break;
      }
    }
    if (!found) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("this subroutine is not supported by sys dispatch call",
               K(package_name),
               K(routine_name));
    }
  }
  return ret;
}

int ObSysDispatchCallResolver::check_supported_cluster_version() const
{
  int ret = OB_SUCCESS;
  if ((GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_2_5_4
       && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0)
      || (GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_3_5_3
          && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_0_0)
      || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_0_0) {
    // supported cluster version for sys dispatch call
    // [4.2.5.4, 4.3.0.0) || [4.3.5.3, 4.4.0.0) || [4.4.0.0, +∞)
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sys dispatch call is not supported for this cluster version",
             "cluster_version", GET_MIN_CLUSTER_VERSION());
  }
  return ret;
}

int ObSysDispatchCallResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSysDispatchCallStmt *stmt = nullptr;
  const ParseNode *name_node = nullptr;
  const ParseNode *designated_tenant_node = nullptr;
  ObString call_stmt;
  ObString tenant_name;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  const ObTenantSchema *tenant_schema = nullptr;

  if (OB_UNLIKELY(OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is nullptr", K(schema_checker_), K(session_info_));
  } else if (OB_FAIL(check_supported_cluster_version())) {
    LOG_WARN("sys dispatch call is not supported");
  } else if (OB_SYS_TENANT_ID != session_info_->get_effective_tenant_id()) {
    // check tenant privilege for dispatch call, should be sys tenant
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("SYS_DISPATCH_CALL is only supported in sys tenant");
  } else if (OB_UNLIKELY(T_SP_SYS_DISPATCH_CALL != parse_tree.type_)
             || OB_UNLIKELY(3 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse tree type is invalid", K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(name_node = parse_tree.children_[0])
             || OB_ISNULL(designated_tenant_node = parse_tree.children_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("name node or tenant node is nullptr", K(name_node), K(designated_tenant_node));
  } else if (OB_FAIL(check_sys_dispatch_call_priv(*name_node))) {
    // check system package subroutine call whitelist
    LOG_WARN("check sys dispatch call privilege failed");
  } else if (OB_ISNULL(stmt = create_stmt<ObSysDispatchCallStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ObSysDispatchCallStmt failed");
  } else if (FALSE_IT(stmt_ = stmt)) {
  } else if (FALSE_IT(call_stmt.assign_ptr(parse_tree.str_value_, strlen(parse_tree.str_value_)))) {
  } else if (FALSE_IT(stmt->set_call_stmt(call_stmt))) {
    // this call statement would be dispatched to another tenant
  } else if (FALSE_IT(tenant_name.assign_ptr(designated_tenant_node->str_value_,
                                             designated_tenant_node->str_len_))) {
  } else if (OB_FAIL(schema_checker_->get_tenant_id(tenant_name, tenant_id))) {
    LOG_WARN("get tenant id failed", K(tenant_name));
  } else if (OB_FAIL(schema_checker_->get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is nullptr", K(tenant_id));
    // fill the tenant id and oracle tenant flag into stmt afterward
  } else if (FALSE_IT(stmt->set_designated_tenant_id(tenant_id))) {
  } else if (FALSE_IT(stmt->set_designated_tenant_name(tenant_name))) {
  } else if (FALSE_IT(stmt->set_tenant_compat_mode(tenant_schema->get_compatibility_mode()))) {
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
