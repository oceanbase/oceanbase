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
#include "sql/resolver/ddl/ob_drop_dblink_resolver.h"

#include "sql/ob_sql_utils.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_drop_dblink_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObDropDbLinkResolver::ObDropDbLinkResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObDropDbLinkResolver::~ObDropDbLinkResolver()
{
}

int ObDropDbLinkResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObDropDbLinkStmt *drop_dblink_stmt = NULL;
  if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_DROP_DBLINK)
      || OB_UNLIKELY(node->num_child_ != DBLINK_NODE_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node children", K(ret), K(node), K(node->children_));
  } else if (OB_ISNULL(drop_dblink_stmt = create_stmt<ObDropDbLinkStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create drop_dblink_stmt", K(ret));
  } else {
    stmt_ = drop_dblink_stmt;
    drop_dblink_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
  }
   if (!lib::is_oracle_mode() && OB_SUCC(ret)) {
    uint64_t compat_version = 0;
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
    } else if (compat_version < DATA_VERSION_4_2_0_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("mysql dblink is not supported when MIN_DATA_VERSION is below DATA_VERSION_4_2_0_0", K(ret));
    } else if (NULL != node->children_[IF_EXIST]) {
      if (T_IF_EXISTS != node->children_[IF_EXIST]->type_) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "invalid argument.",
                      K(ret), K(node->children_[1]->type_));
      } else {
        drop_dblink_stmt->set_if_exist(true);
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObString dblink_name;
    ParseNode *name_node = node->children_[DBLINK_NAME];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (name_node->str_len_ >= OB_MAX_DBLINK_NAME_LENGTH) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(name_node->str_len_), name_node->str_value_);
    } else {
      dblink_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
      drop_dblink_stmt->set_dblink_name(dblink_name);
    }
  }
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_DROP_DBLINK,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
  }
  
  LOG_INFO("resolve drop dblink finish", K(ret));
  return ret;
}

} //end namespace sql
} //end namespace oceanbase
