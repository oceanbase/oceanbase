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

#include "sql/resolver/ddl/ob_alter_sequence_resolver.h"
#include "sql/resolver/ddl/ob_sequence_stmt.h"
#include "sql/resolver/ddl/ob_sequence_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

/**
 *  ALTER SEQUENCE schema.sequence_name
 *      (alter_sequence_option_list,...)
 */

ObAlterSequenceResolver::ObAlterSequenceResolver(ObResolverParams &params)
  : ObStmtResolver(params)
{
}

ObAlterSequenceResolver::~ObAlterSequenceResolver()
{
}

int ObAlterSequenceResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObAlterSequenceStmt *mystmt = NULL;

  if (OB_UNLIKELY(T_ALTER_SEQUENCE != parse_tree.type_)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(2 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param",
             K(parse_tree.type_),
             K(parse_tree.num_child_),
             K(parse_tree.children_),
             K(ret));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "session_info is null.", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObAlterSequenceStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to alter select stmt");
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
      LOG_WARN("parse ref factor failed", K(ret));
    } else if (sequence_name.length() > OB_MAX_SEQUENCE_NAME_LENGTH) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, sequence_name.length(), sequence_name.ptr());
    } else {
      uint64_t sequence_id = 0;
      (void)(schema_checker_->get_sequence_id(session_info_->get_effective_tenant_id(),
                                              db_name,
                                              sequence_name,
                                              sequence_id));
      mystmt->set_sequence_id(sequence_id);
      mystmt->set_sequence_name(sequence_name);
      mystmt->set_database_name(db_name);
      mystmt->set_tenant_id(session_info_->get_effective_tenant_id());
    
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        OZ (schema_checker_->check_ora_ddl_priv(
              session_info_->get_effective_tenant_id(),
              session_info_->get_priv_user_id(),
              db_name,
              sequence_id,
              static_cast<uint64_t>(ObObjectType::SEQUENCE),
              stmt::T_ALTER_SEQUENCE,
              session_info_->get_enable_role_array()),
              session_info_->get_effective_tenant_id(), session_info_->get_user_id());
      }
    } 
  }
  
  /* sequence options */
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(parse_tree.children_[1])) {
      if (OB_UNLIKELY(T_SEQUENCE_OPTION_LIST != parse_tree.children_[1]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid option node type",
                  K(parse_tree.children_[1]->type_), K(ret));
      } else {
        ObSequenceResolver<ObAlterSequenceStmt> resolver;
        ret = resolver.resolve_sequence_options(session_info_->get_effective_tenant_id(), mystmt,
                                                parse_tree.children_[1]);
      }
    } else {
      ret = OB_ERR_REQUIRE_ALTER_SEQ_OPTION;
      LOG_USER_ERROR(OB_ERR_REQUIRE_ALTER_SEQ_OPTION);
    }
  }
  return ret;
}


} /* sql */
} /* oceanbase */
