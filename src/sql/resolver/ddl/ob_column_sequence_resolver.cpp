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

#include "sql/resolver/ddl/ob_column_sequence_resolver.h"
#include "sql/resolver/ddl/ob_sequence_stmt.h"
#include "sql/resolver/ddl/ob_sequence_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

/**
 *  CREATE SEQUENCE schema.sequence_name
 *      (create_sequence_option_list,...)
 */

ObColumnSequenceResolver::ObColumnSequenceResolver(ObResolverParams &params)
  : ObStmtResolver(params)
{
}

ObColumnSequenceResolver::~ObColumnSequenceResolver()
{
}

// 什么也不做，属于identity_column
// 解析在create_table_resolver或alter_table_resolver里面
int ObColumnSequenceResolver::resolve(const ParseNode &parse_tree)
{
  UNUSED(parse_tree);
  return OB_SUCCESS;
}

int ObColumnSequenceResolver::resolve_sequence_without_name(ObColumnSequenceStmt *&mystmt, ParseNode *&node)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session_info is null", K(ret));
  } else if (OB_ISNULL(mystmt = create_stmt<ObColumnSequenceStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create select stmt");
  } else {
    ObString db_name = session_info_->get_database_name();
    // inner sequnece, name as ISEQ$$_tableid_columnid, it can't comfirm when resolve
    mystmt->set_database_name(db_name);
    mystmt->set_tenant_id(session_info_->get_effective_tenant_id());
    mystmt->set_is_system_generated();

    if (ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_));
      OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          db_name,
          stmt::T_CREATE_SEQUENCE,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(node)) {
      if (OB_UNLIKELY(T_SEQUENCE_OPTION_LIST != node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid option node type", K(node->type_), K(ret));
      } else {
        ObSequenceResolver<ObColumnSequenceStmt> resolver;
        if (OB_FAIL(resolver.resolve_sequence_options(mystmt, node))) {
          LOG_WARN("resolve sequence options failed", K(ret));
        }
      }
    }
  }
  return ret;
}

} /* sql */
} /* oceanbase */
