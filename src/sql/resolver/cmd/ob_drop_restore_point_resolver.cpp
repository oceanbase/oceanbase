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

#include "sql/resolver/cmd/ob_drop_restore_point_resolver.h"
#include "sql/resolver/cmd/ob_drop_restore_point_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/cmd/ob_variable_set_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

/**
 *  DROP RESTORE POINT restore_point_name
 *
 */

ObDropRestorePointResolver::ObDropRestorePointResolver(ObResolverParams &params)
  : ObSystemCmdResolver(params)
{
}

ObDropRestorePointResolver::~ObDropRestorePointResolver()
{
}

int ObDropRestorePointResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObDropRestorePointStmt *mystmt = NULL;

  if (OB_UNLIKELY(T_DROP_RESTORE_POINT != parse_tree.type_)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(1 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(parse_tree.type_), K(parse_tree.num_child_),
        K(parse_tree.children_), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObDropRestorePointStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create select stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                            session_info_->get_priv_user_id(),
                                            ObString("")/*database_name*/,
                                            stmt::T_DROP_RESTORE_POINT,
                                            session_info_->get_enable_role_array()));
  }

  /* tenant name */
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(T_IDENT != parse_tree.children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid parse_tree", K(ret));
    } else {
      ObString restore_point_name;
      restore_point_name.assign_ptr((char *)(parse_tree.children_[0]->str_value_),
                             static_cast<int32_t>(parse_tree.children_[0]->str_len_));
      if (restore_point_name.length() >= OB_MAX_RESERVED_POINT_NAME_LENGTH) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, restore_point_name.length(),
            restore_point_name.ptr());
      } else {
        mystmt->set_restore_point_name(restore_point_name);
      }
    }
  }

  return ret;
}


} /* sql */
} /* oceanbase */
