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
#include "sql/resolver/cmd/ob_drop_event_resolver.h"
#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
namespace sql
{
ObDropEventResolver::ObDropEventResolver(ObResolverParams &params)
    : ObCMDResolver(params)
{
}

int ObDropEventResolver::resolve(const ParseNode &parse_tree)
{

  int ret = OB_SUCCESS;

  CHECK_COMPATIBILITY_MODE(session_info_);
  ObDropEventStmt *drop_event_stmt = NULL;
  if ((2 != parse_tree.num_child_) || T_EVENT_JOB_DROP != parse_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("has 2 child",
             "actual_num", parse_tree.num_child_,
             "type", parse_tree.type_,
             K(ret));
  } else if (OB_ISNULL(params_.session_info_) || OB_ISNULL(params_.schema_checker_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Session info should not be NULL", K(ret), KP(params_.session_info_),
             KP(params_.schema_checker_));
  } else if (OB_ISNULL(drop_event_stmt = create_stmt<ObDropEventStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to drop ObDropEventStmt", K(ret));
	} else {
    stmt_ = drop_event_stmt;
    ObDropEventStmt &stmt = *(static_cast<ObDropEventStmt *>(stmt_));
    const uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
    stmt.set_tenant_id(tenant_id);
    const ParseNode *if_exists                = parse_tree.children_[0];
    const ParseNode *sp_name_node             = parse_tree.children_[1];
    if (OB_NOT_NULL(if_exists)) {
      stmt.set_if_exists(true);
    } else {
      stmt.set_if_exists(false);
    }

    if (OB_ISNULL(sp_name_node) || T_SP_NAME != sp_name_node->type_ || 2 != sp_name_node->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("alter event need event name node", K(ret));
    } else {

     ObString db_name, event_name;
      if (OB_FAIL(ObResolverUtils::resolve_sp_name(*session_info_, *sp_name_node, db_name, event_name))) {
        LOG_WARN("get sp name failed", K(ret), KP(sp_name_node));
      } else if (0 != db_name.compare(session_info_->get_database_name())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop event in non-current database");
        LOG_WARN("not support drop other database", K(ret), K(db_name), K(session_info_->get_database_name()));
      } else {
        stmt.set_event_database(db_name);
        char *event_name_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_NAME_MAX_LENGTH));
        if (OB_ISNULL(event_name_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("faild to alloc memory for event name", K(ret));
        } else {
          memset(event_name_buf, 0, OB_EVENT_NAME_MAX_LENGTH);
          snprintf(event_name_buf, OB_EVENT_NAME_MAX_LENGTH, "%lu.%s", session_info_->get_database_id(), event_name.ptr());
          stmt.set_event_name(event_name_buf);
        }
      }
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
