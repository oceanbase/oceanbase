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
#include "sql/resolver/ddl/ob_drop_outline_resolver.h"

#include "sql/resolver/ddl/ob_drop_outline_stmt.h"
#include "share/schema/ob_outline_sql_service.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
int ObDropOutlineResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode *>(&parse_tree);
  ObDropOutlineStmt *drop_outline_stmt = NULL;
  uint64_t compat_version = 0;
  if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_DROP_OUTLINE)
      || OB_UNLIKELY(node->num_child_ != OUTLINE_CHILD_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(node), K(node->children_));
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL");
  } else if (OB_ISNULL(drop_outline_stmt = create_stmt<ObDropOutlineStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create drop_outline_stmt", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(MTL_ID()));
  } else {
    stmt_ = drop_outline_stmt;
    //resolve database_name and outline_name
    if (OB_SUCC(ret)) {
      ObString db_name;
      ObString outline_name;
      // resovle outline type
      bool is_format_otl = false;

      if (OB_FAIL(resolve_outline_name(node->children_[0], db_name, outline_name))) {
        LOG_WARN("fail to resolve outline name", K(ret));
      } else if (OB_ISNULL(node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid node children", K(node->children_[1]), K(node->children_));
      } else {
        is_format_otl = (node->children_[1]->value_
                         == ObOutlineType::OUTLINE_TYPE_FORMAT);
        static_cast<ObDropOutlineStmt *>(stmt_)->set_database_name(db_name);
        static_cast<ObDropOutlineStmt *>(stmt_)->set_outline_name(outline_name);
        static_cast<ObDropOutlineStmt *>(stmt_)->set_tenant_id(params_.session_info_->get_effective_tenant_id());
        static_cast<ObDropOutlineStmt *>(stmt_)->set_is_format(is_format_otl);
      }
      if (OB_SUCC(ret) && is_format_otl && !oceanbase::share::schema::ObOutlineSqlService::is_formatoutline_compat(compat_version)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "format outline not supported under oceanbase 4.3.4");
        LOG_WARN("format outline not supported under oceanbase 4.3.4", K(ret));
      }
    }
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_)); 
      OZ (schema_checker_->check_ora_ddl_priv(
            params_.session_info_->get_effective_tenant_id(),
            params_.session_info_->get_priv_user_id(),
            ObString(""),
            stmt::T_DROP_OUTLINE,
            session_info_->get_enable_role_array()),
            params_.session_info_->get_effective_tenant_id(), 
            params_.session_info_->get_priv_user_id());
    }
  }
  return ret;
}
}//namespace sql
}//namespace oceanbase
