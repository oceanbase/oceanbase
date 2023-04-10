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
#include "sql/resolver/dcl/ob_rename_user_resolver.h"

#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObRenameUserResolver::ObRenameUserResolver(ObResolverParams &params)
    : ObDCLResolver(params)
{
}

ObRenameUserResolver::~ObRenameUserResolver()
{
}

int ObRenameUserResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObRenameUserStmt *rename_user_stmt = NULL;

  if (OB_ISNULL(params_.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Session info is not inited", K(ret));
  } else if (node != NULL && T_RENAME_USER == node->type_ && node->num_child_ > 0) {
    if (OB_ISNULL(rename_user_stmt = create_stmt<ObRenameUserStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to create ObRenameUserStmt", K(ret));
    } else {
      stmt_ = rename_user_stmt;
      rename_user_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
      for (int i = 0; i < node->num_child_ && OB_SUCCESS == ret; ++i) {
        ParseNode *rename_info = NULL;
        if (OB_ISNULL(rename_info = node->children_[i])) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("Child should not be NULL", K(ret), K(i));
        } else if (4 == rename_info->num_child_ && T_RENAME_INFO == rename_info->type_
                   && NULL != rename_info->children_[0] && NULL != rename_info->children_[2]) {
          ObString from_user(rename_info->children_[0]->str_len_, rename_info->children_[0]->str_value_);
          ObString from_host;
          ObString to_user(rename_info->children_[2]->str_len_, rename_info->children_[2]->str_value_);
          ObString to_host;
          if (NULL == rename_info->children_[1]) {
            from_host.assign_ptr(OB_DEFAULT_HOST_NAME, static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
          } else {
            from_host.assign_ptr(rename_info->children_[1]->str_value_,
                                 static_cast<int32_t>(rename_info->children_[1]->str_len_));
          }
          if (NULL == rename_info->children_[3]) {
            to_host.assign_ptr(OB_DEFAULT_HOST_NAME, static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
          } else {
            to_host.assign_ptr(rename_info->children_[3]->str_value_,
                               static_cast<int32_t>(rename_info->children_[3]->str_len_));
          }
          if ((0 == to_user.case_compare(OB_RESTORE_USER_NAME)) || (0 == from_user.case_compare(OB_RESTORE_USER_NAME))) {
            ret = OB_ERR_NO_PRIVILEGE;
            LOG_WARN("__oceanbase_inner_restore_user is reserved", K(ret));
          } else if (from_user.length() > OB_MAX_USER_NAME_LENGTH) {
            LOG_USER_ERROR(OB_WRONG_USER_NAME_LENGTH, from_user.length(), from_user.ptr());
          } else if (to_user.length() > OB_MAX_USER_NAME_LENGTH) {
            LOG_USER_ERROR(OB_WRONG_USER_NAME_LENGTH, to_user.length(), to_user.ptr());
          } else if (OB_FAIL(check_dcl_on_inner_user(node->type_,
                                                     session_info_->get_priv_user_id(),
                                                     from_user,
                                                     from_host))) {
            LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user",
                     K(ret), K(session_info_->get_user_name()), K(from_user));
          } else if (OB_FAIL(rename_user_stmt->add_rename_info(from_user, from_host, to_user, to_host))) {
            LOG_WARN("Failed to add user to ObRenameUserStmt", K(ret));
          } else {
            //do nothing
          }
        } else {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("sql_parser parse rename_info error", K(ret));
        }
      } // end for
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rename user ParseNode error", K(ret));
  }
  return ret;
}
