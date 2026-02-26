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
#include "sql/resolver/ddl/ob_drop_ccl_rule_resolver.h"

#include "sql/ob_sql_utils.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_drop_ccl_rule_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObDropCCLRuleResolver::ObDropCCLRuleResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObDropCCLRuleResolver::~ObDropCCLRuleResolver()
{
}

int ObDropCCLRuleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObDropCCLRuleStmt *drop_ccl_rule_stmt = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t data_version = 0;
  if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_DROP_CCL_RULE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node children", K(node), K(node->children_));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < MOCK_DATA_VERSION_4_3_5_3 ||
              (data_version>=DATA_VERSION_4_4_0_0 && data_version < DATA_VERSION_4_4_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ccl not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ccl");
  } else if (OB_ISNULL(drop_ccl_rule_stmt = create_stmt<ObDropCCLRuleStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create create_database_stmt", K(ret));
  } else {
    stmt_ = drop_ccl_rule_stmt;
    drop_ccl_rule_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    // 1.resolve if exists
    if (OB_SUCC(ret) && lib::is_mysql_mode()) {
      if (node->children_[IF_EXIST] != NULL) {
        if (node->children_[IF_EXIST]->type_ != T_IF_EXISTS) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid parse tree", K(ret), K(node->children_[IF_EXIST]->type_));
        } else {
          drop_ccl_rule_stmt->set_if_exists(true);
        }
      }
    }

    // 2. resolve ccl rule name
    if (OB_SUCC(ret)) {
      ObString ccl_rule_name;
      ParseNode *ccl_rule_name_node = node->children_[CCL_RULE_NAME];
      if (OB_ISNULL(ccl_rule_name_node)) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("ccl rule name parse node should not be null", K(ccl_rule_name_node));
      } else {
        ccl_rule_name.assign_ptr(ccl_rule_name_node->str_value_,
                                 static_cast<int32_t>(ccl_rule_name_node->str_len_));
        drop_ccl_rule_stmt->set_ccl_rule_name(ccl_rule_name);
      }
    }

    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_DROP_CCL_RULE,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  }
  return ret;
}

} //end namespace sql
} //end namespace oceanbase
