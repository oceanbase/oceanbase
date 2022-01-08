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
#include "sql/resolver/prepare/ob_deallocate_resolver.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObDeallocateResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (parse_tree.num_child_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "num_child", parse_tree.num_child_, K(ret));
  } else {
    ObDeallocateStmt *deallocate_stmt = NULL;
    if (OB_ISNULL(deallocate_stmt = create_stmt<ObDeallocateStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("create deallocate stmt failed",K(ret));
    } else {
      stmt_ = deallocate_stmt;
      //resolver stmt name
      if (OB_ISNULL(parse_tree.children_[0])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(parse_tree.children_[0]), K(ret));
      } else {
        ObString stmt_name;
        ObString name(parse_tree.children_[0]->str_len_, parse_tree.children_[0]->str_value_);
        ObPsStmtId ps_id = OB_INVALID_ID;
        if (OB_FAIL(ob_simple_low_to_up(*params_.allocator_, name, stmt_name))) {
          LOG_WARN("failed to write string", K(ret));
        } else if(OB_FAIL(session_info_->get_prepare_id(stmt_name, ps_id))) {
          LOG_WARN("failed to get prepare id", K(ret));
        } else {
          deallocate_stmt->set_prepare_id(ps_id);
          deallocate_stmt->set_prepare_name(stmt_name);
        }
      }
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
