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

#include "ob_start_trans_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_trans_character.h"
#include "share/schema/ob_sys_variable_mgr.h" // ObSimpleSysVariableSchema

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObStartTransResolver::ObStartTransResolver(ObResolverParams &params)
  : ObTCLResolver(params)
{
}

ObStartTransResolver::~ObStartTransResolver()
{
}

int ObStartTransResolver::resolve(const ParseNode &parse_node)
{
  int ret = OB_SUCCESS;
  ObStartTransStmt *start_stmt = NULL;
  if (OB_UNLIKELY(T_BEGIN != parse_node.type_ || 2 != parse_node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected val", K(parse_node.type_), K(parse_node.num_child_), K(ret));
  } else if (OB_UNLIKELY(NULL == (start_stmt = create_stmt<ObStartTransStmt>()))) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create select stmt", K(ret));
  } else if (OB_ISNULL(schema_checker_)
             || OB_ISNULL(schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", KR(ret));
  } else {
    stmt_ = start_stmt;
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    // Use initial simple sys variable while create a tenant to avoid cyclic dependence
    const share::schema::ObSimpleSysVariableSchema *sys_variable = NULL;
    share::schema::ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
    if (OB_FAIL(schema_guard->get_sys_variable_schema(tenant_id, sys_variable))) {
      LOG_WARN("fail to get sys variable schema", K(tenant_id), K(ret));
    } else if (OB_ISNULL(sys_variable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys variable should not be null", K(tenant_id), K(ret));
    } else if (OB_UNLIKELY(!session_info_->has_user_super_privilege()
                           && sys_variable->get_read_only()
                           && IS_READ_WRITE(parse_node.children_[0]->value_))) {
      ret = OB_ERR_OPTION_PREVENTS_STATEMENT;
      LOG_WARN("the server is running with read_only, cannot execute stmt");
    } else {
      if (IS_READ_ONLY(parse_node.children_[0]->value_)) {
        start_stmt->set_read_only(true);
      } else if (IS_READ_WRITE(parse_node.children_[0]->value_)) {
        start_stmt->set_read_only(false);
      } else {
        start_stmt->set_read_only(session_info_->get_tx_read_only());
      }
      if (IS_WITH_SNAPSHOT(parse_node.children_[0]->value_)) {
        start_stmt->set_with_consistent_snapshot(true);
      }
      // hint
      auto hint = parse_node.children_[1];
      if (hint) {
        start_stmt->set_hint(ObString(hint->str_len_, hint->str_value_));
      }
    }
  }
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */
