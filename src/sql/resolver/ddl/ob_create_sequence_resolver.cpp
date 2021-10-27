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

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {

/**
 *  CREATE SEQUENCE schema.sequence_name
 *      (create_sequence_option_list,...)
 */

ObCreateSequenceResolver::ObCreateSequenceResolver(ObResolverParams& params) : ObStmtResolver(params)
{}

ObCreateSequenceResolver::~ObCreateSequenceResolver()
{}

int ObCreateSequenceResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateSequenceStmt* mystmt = NULL;

  if (OB_UNLIKELY(T_CREATE_SEQUENCE != parse_tree.type_) || OB_ISNULL(parse_tree.children_) ||
      OB_UNLIKELY(3 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret), K(parse_tree.type_), K(parse_tree.num_child_), K(parse_tree.children_));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "session_info is null.", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObCreateSequenceStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create select stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  bool allow_existence = false;
  /* if_not_exists */
  if (OB_UNLIKELY(NULL != parse_tree.children_[0])) {
    allow_existence = true;
  } else {/* NULL == parse_tree.children_[0]*/
    // allow_existence = false;
  }

  /* sequence name */
  if (OB_SUCC(ret)) {
    ObString sequence_name;
    ObString db_name;
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    if (OB_FAIL(resolve_ref_factor(parse_tree.children_[1], session_info_, sequence_name, db_name))) {
      LOG_WARN("fail parse sequence name", K(ret));
    } else if (sequence_name.length() > OB_MAX_SEQUENCE_NAME_LENGTH) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, sequence_name.length(), sequence_name.ptr());
    } else {
      bool exists = true;
      uint64_t sequence_id = OB_INVALID_ID;
      uint64_t database_id = OB_INVALID_ID;
      if (OB_ISNULL(schema_checker_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_schecker is null", K(ret));
      } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, database_id))) {
        LOG_WARN("fail to get database id", K(ret));
      } else if (OB_FAIL(schema_checker_->check_sequence_exist_with_name(tenant_id, database_id, sequence_name, exists, sequence_id))) {
        LOG_WARN("fail to check_sequence_exist_with_name", K(ret));
      } else if (exists) {
        if (!allow_existence) {
          ret = OB_ERR_EXIST_OBJECT;
          LOG_WARN("Name is already used by an existing object", K(ret), K(sequence_name));
        } else {
          mystmt->set_rpc_flag(false);
          // however, in this case, there is no seq_item in the arg of mystmt (mystmt->get_arg().get_seq_items().size() == 0)
          // thus, while resolving sequence options, it should be considered that identifying invalid case and this case
        }
      } else {
        mystmt->set_rpc_flag(true);
        obrpc::ObSequenceItem sequence_item;
        sequence_item.set_sequence_name(sequence_name);
        sequence_item.set_database_name(db_name);
        sequence_item.set_tenant_id(tenant_id);
        mystmt->get_arg().get_seq_items().push_back(sequence_item);
      }
    }

    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK(OB_NOT_NULL(schema_checker_));
      OZ(schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
              session_info_->get_priv_user_id(),
              db_name,
              stmt::T_CREATE_SEQUENCE,
              session_info_->get_enable_role_array()),
              session_info_->get_effective_tenant_id(),
              session_info_->get_user_id());
    }
  }

  /* sequence options */
  if (OB_SUCC(ret) && NULL != parse_tree.children_[2] && mystmt->get_arg().get_seq_items().size() == 1) {
    if (OB_UNLIKELY(T_SEQUENCE_OPTION_LIST != parse_tree.children_[2]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid option node type", K(ret), K(parse_tree.children_[2]->type_));
    } else {
      ObSequenceResolver<ObCreateSequenceStmt> resolver;
      ret = resolver.resolve_sequence_options(mystmt, parse_tree.children_[2]);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
