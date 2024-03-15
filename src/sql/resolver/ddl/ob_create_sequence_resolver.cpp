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

#include "sql/resolver/ddl/ob_create_sequence_resolver.h"
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

ObCreateSequenceResolver::ObCreateSequenceResolver(ObResolverParams &params)
  : ObStmtResolver(params)
{
}

ObCreateSequenceResolver::~ObCreateSequenceResolver()
{
}

int ObCreateSequenceResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateSequenceStmt *mystmt = NULL;

  bool if_not_exists = false;
  if (OB_UNLIKELY(T_CREATE_SEQUENCE != parse_tree.type_)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(2 != parse_tree.num_child_ && 3 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param",
             K(parse_tree.type_),
             K(parse_tree.num_child_),
             K(parse_tree.children_),
             K(ret));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "session_info is null.", K(ret));
  } else if (3 == parse_tree.num_child_ && OB_NOT_NULL(parse_tree.children_[2])) {
    if (T_IF_NOT_EXISTS != parse_tree.children_[2]->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", K(ret), K(parse_tree.children_[1]));
    } else {
      if_not_exists = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObCreateSequenceStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create select stmt", K(ret));
    } else {
      stmt_ = mystmt;
    }
  }

  /* sequence name */
  if (OB_SUCC(ret)) {
    ObString sequence_name;
    ObString db_name;
    if (OB_FAIL(resolve_ref_factor(parse_tree.children_[0],
                                   session_info_,
                                   sequence_name,
                                   db_name))) {
      LOG_WARN("fail parse sequence name", K(ret));
    } else if (sequence_name.length() > OB_MAX_SEQUENCE_NAME_LENGTH) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, sequence_name.length(), sequence_name.ptr());
    } else {
      mystmt->set_sequence_name(sequence_name);
      mystmt->set_database_name(db_name);
      mystmt->set_tenant_id(session_info_->get_effective_tenant_id());
      mystmt->set_ignore_exists_error(if_not_exists);
    }
    
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_));
      OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          db_name,
          stmt::T_CREATE_SEQUENCE,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  }

  /* sequence options */
  if (OB_SUCC(ret) && NULL != parse_tree.children_[1]) {
    if (OB_UNLIKELY(T_SEQUENCE_OPTION_LIST != parse_tree.children_[1]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid option node type",
                K(parse_tree.children_[1]->type_), K(ret));
    } else {
      ObSequenceResolver<ObCreateSequenceStmt> resolver;
      if (OB_FAIL(resolver.resolve_sequence_options(session_info_->get_effective_tenant_id(),
                                                    mystmt, parse_tree.children_[1]))) {
        LOG_WARN("resolve sequence options failed", K(ret));
      }
    }
  }
  return ret;
}


} /* sql */
} /* oceanbase */
