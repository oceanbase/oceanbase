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

#include "sql/resolver/ddl/ob_drop_sequence_resolver.h"
#include "sql/resolver/ddl/ob_sequence_stmt.h"
#include "sql/resolver/ddl/ob_sequence_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

/**
 *  DROP SEQUENCE schema.sequence_name
 *      (drop_sequence_option_list,...)
 */

ObDropSequenceResolver::ObDropSequenceResolver(ObResolverParams &params)
  : ObStmtResolver(params)
{
}

ObDropSequenceResolver::~ObDropSequenceResolver()
{
}

int ObDropSequenceResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObDropSequenceStmt *mystmt = NULL;

  bool if_exists = false;
  if (OB_UNLIKELY(T_DROP_SEQUENCE != parse_tree.type_)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(1 != parse_tree.num_child_ && 2 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param",
             K(parse_tree.type_),
             K(parse_tree.num_child_),
             K(parse_tree.children_),
             K(ret));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "session_info is null.", K(ret));
  } else if ((2 == parse_tree.num_child_) && OB_NOT_NULL(parse_tree.children_[1])) {
    if (T_IF_EXISTS != parse_tree.children_[1]->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", K(ret), K(parse_tree.children_[1]));
    } else {
      if_exists = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObDropSequenceStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to drop select stmt", K(ret));
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
      LOG_WARN("invalid parse_tree", K(ret));
    } else if (sequence_name.length() > OB_MAX_SEQUENCE_NAME_LENGTH) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, sequence_name.length(), sequence_name.ptr());
    } else {
      mystmt->set_sequence_name(sequence_name);
      mystmt->set_database_name(db_name);
      mystmt->set_tenant_id(session_info_->get_effective_tenant_id());
      mystmt->set_ignore_exists_error(if_exists);
    }
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_));
      OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          db_name,
          stmt::T_DROP_SEQUENCE,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  }
  
  
  return ret;
}


} /* sql */
} /* oceanbase */
