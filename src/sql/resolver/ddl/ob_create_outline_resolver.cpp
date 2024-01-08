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
#include "sql/resolver/ddl/ob_create_outline_resolver.h"

#include "share/ob_version.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/ddl/ob_create_outline_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{


int ObCreateOutlineResolver::resolve_sql_id(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || (node->type_ != T_CHAR && node->type_ != T_VARCHAR)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql id");
  } else {
    create_outline_stmt.get_sql_id() = ObString::make_string(node->str_value_);
  }
  return ret;
}

int ObCreateOutlineResolver::resolve_hint(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt)
{
  int ret = OB_SUCCESS;
  if (node == NULL) {
    ret = OB_INVALID_OUTLINE;
    LOG_USER_ERROR(OB_INVALID_OUTLINE, "Hint is not correct, please check");
    LOG_WARN("hint is not correct");
  }
  if (OB_SUCC(ret)) {
    if (node->type_ != T_HINT_OPTION_LIST) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      char *buf = (char *)allocator_->alloc(node->str_len_ + 4);
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("cannot alloc mem");
      } else {
        MEMCPY(buf, "/*+", 3);
        MEMCPY(buf + 3, node->str_value_, node->str_len_);
        buf[node->str_len_ + 3] = '\0';
        create_outline_stmt.get_hint() = ObString::make_string(buf);
        if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
            *allocator_, session_info_->get_dtc_params(), create_outline_stmt.get_hint()))) {
          LOG_WARN("fail to convert sql text", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (int32_t i = 0; i < node->num_child_; i ++) {
      ParseNode *hint_node = node->children_[i];
      if (!hint_node) {
       continue;
      }
      if (hint_node->type_ == T_MAX_CONCURRENT) {
        if (OB_ISNULL(hint_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child of max concurrent node should not be NULL", K(ret));
        } else if (hint_node->children_[0]->value_ >= 0) {
          create_outline_stmt.set_max_concurrent(hint_node->children_[0]->value_);
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObCreateOutlineResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode *>(&parse_tree);
  ObCreateOutlineStmt *create_outline_stmt = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ or allocator_ is NULL",
             KP(session_info_), K(allocator_), K(ret));
  } else if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_CREATE_OUTLINE)
      || OB_UNLIKELY(node->num_child_ != OUTLINE_CHILD_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(node), K(node->children_));
  } else if (OB_UNLIKELY(NULL == (create_outline_stmt = create_stmt<ObCreateOutlineStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create create_outline_stmt", K(ret));
  } else {
    stmt_ = create_outline_stmt;
    //set is_replace
    if (node->children_[0] != NULL) {
      create_outline_stmt->set_replace();
    }
    //set owner
    create_outline_stmt->set_owner(session_info_->get_user_name());
    create_outline_stmt->set_owner_id(session_info_->get_user_id());
    //set server version
    ObString server_version;
    if (OB_FAIL(ob_write_string(*allocator_, ObString(build_version()), server_version))) {
      LOG_WARN("failed to write string", K(ret));
    } else {
      create_outline_stmt->set_server_version(server_version);
    }

    //resolve database_name and outline_name
    if (OB_SUCC(ret)) {
      ObString db_name;
      ObString outline_name;
      if (OB_FAIL(resolve_outline_name(node->children_[1], db_name, outline_name))) {
        LOG_WARN("fail to resolve outline name", K(ret));
      } else {
        create_outline_stmt->set_database_name(db_name);
        create_outline_stmt->set_outline_name(outline_name);
      }
    }

    if (node->children_[2]->value_ == 1) {
      //resolve outline_stmt
      if (OB_SUCC(ret)) {
        if (OB_FAIL(resolve_outline_stmt(node->children_[3], create_outline_stmt->get_outline_stmt(),
                                         create_outline_stmt->get_outline_sql()))) {
          LOG_WARN("fail to resolve outline stmt", K(ret));
        }
      }
      //set outline_target
      if (OB_SUCC(ret)) {
        if (OB_FAIL(resolve_outline_target(node->children_[4], create_outline_stmt->get_target_sql()))) {
          LOG_WARN("fail to resolve outline target", K(ret));
        }
      }
    } else {
      if (OB_FAIL(resolve_hint(node->children_[3], *create_outline_stmt))) {
        LOG_WARN("fail to resolve hint", K(ret));
      } else if (OB_FAIL(resolve_sql_id(node->children_[4], *create_outline_stmt))) {
        LOG_WARN("fail to resolve sql id", K(ret));
      }
    }
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_)); 
      OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_CREATE_OUTLINE,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  }
  

  return ret;
}

}//sql
}//oceanbase
