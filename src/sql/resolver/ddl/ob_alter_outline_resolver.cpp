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
#include "sql/resolver/ddl/ob_alter_outline_resolver.h"

#include "sql/resolver/ddl/ob_alter_outline_stmt.h"
#include "share/schema/ob_outline_sql_service.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObAlterOutlineResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode *>(&parse_tree);
  ObAlterOutlineStmt *alter_outline_stmt = NULL;
  uint64_t compat_version = 0;
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ or allocator_ is NULL",
             KP(session_info_), K(allocator_), K(ret));
  } else if (OB_ISNULL(node)
      || OB_UNLIKELY(T_ALTER_OUTLINE != node->type_)
      || OB_UNLIKELY(OUTLINE_CHILD_COUNT != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(node), K(node->children_));
  } else if (OB_UNLIKELY(NULL == (alter_outline_stmt = create_stmt<ObAlterOutlineStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create alter_outline_stmt", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(MTL_ID()));
  } else {
    stmt_ = alter_outline_stmt;

    // resovle outline type
    bool is_format_otl = false;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(node->children_[3])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node children", K(node->children_[3]), K(node->children_));
    } else {
      is_format_otl = (node->children_[3]->value_
                        == ObOutlineType::OUTLINE_TYPE_FORMAT);
      alter_outline_stmt->set_format_outline(is_format_otl);
    }
    if (OB_SUCC(ret) && is_format_otl && !oceanbase::share::schema::ObOutlineSqlService::is_formatoutline_compat(compat_version)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "format outline not supported under oceanbase 4.3.4");
      LOG_WARN("format outline not supported under oceanbase 4.3.4", K(ret));
    }
    //resolve database_name and outline_name
    if (OB_SUCC(ret)) {
      ObString db_name;
      ObString outline_name;
      if (OB_FAIL(resolve_outline_name(node->children_[0], db_name, outline_name))) {
        LOG_WARN("fail to resolve outline name", K(ret));
      } else {
        alter_outline_stmt->set_database_name(db_name);
        alter_outline_stmt->set_outline_name(outline_name);
      }
    }
    //resolve outline_stmt
    if (OB_SUCC(ret)) {
      if (!is_format_otl && OB_FAIL(resolve_outline_stmt(node->children_[1],
                                       alter_outline_stmt->get_outline_stmt(),
                                       alter_outline_stmt->get_outline_sql()))) {
        LOG_WARN("fail to resolve outline stmt", K(ret));
      } else if (is_format_otl && OB_FAIL(resolve_outline_stmt(node->children_[1],
                                       alter_outline_stmt->get_outline_stmt(),
                                       alter_outline_stmt->get_format_outline_sql()))) {
        LOG_WARN("fail to resolve outline stmt", K(ret));
      }
    }

    //set outline_target
    if (OB_SUCC(ret)) {
      if (OB_FAIL(resolve_outline_target(node->children_[2], alter_outline_stmt->get_target_sql()))) {
        LOG_WARN("fail to resolve outline target", K(ret));
      }
    }
    
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_)); 
      OZ (schema_checker_->check_ora_ddl_priv(
            session_info_->get_effective_tenant_id(),
            session_info_->get_priv_user_id(),
            ObString(""),
            stmt::T_ALTER_OUTLINE,
            session_info_->get_enable_role_array()),
            session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  }

  return ret;
}

}//sql
}//oceanbase
