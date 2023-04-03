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
#include "ob_drop_package_resolver.h"
#include "ob_drop_package_stmt.h"
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
int ObDropPackageResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  const ParseNode *name_node = NULL;
  ObString db_name;
  ObString package_name;
  ObDropPackageStmt *package_stmt = NULL;
  if (OB_UNLIKELY(parse_tree.type_ != T_PACKAGE_DROP)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(parse_tree.num_child_ != DROP_PACKAGE_NODE_CHILD_COUNT)
      || OB_ISNULL(name_node = parse_tree.children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse tree is invalid", "type", get_type_name(parse_tree.type_),
             K_(parse_tree.children), K_(parse_tree.num_child), K(name_node));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is null");
  } else if (OB_ISNULL(schema_checker_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is null");
  } else if (OB_FAIL(ObResolverUtils::resolve_sp_name(*session_info_, *name_node, db_name, package_name))) {
    LOG_WARN("resolve package name failed", K(ret));
  } else if (ObSchemaChecker::is_ora_priv_check() 
             && OB_FAIL(schema_checker_->check_ora_ddl_priv(
                                      session_info_->get_effective_tenant_id(),
                                      session_info_->get_priv_user_id(),
                                      db_name,
                                      stmt::T_DROP_ROUTINE,
                                      session_info_->get_enable_role_array()))) {
    LOG_WARN("failed to check privilege in drop package", K(ret));
  } else if (OB_ISNULL(package_stmt = create_stmt<ObDropPackageStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create drop package stmt failed");
  } else {
    bool is_drop_body = static_cast<bool>(parse_tree.value_);
    obrpc::ObDropPackageArg &package_arg = package_stmt->get_drop_package_arg();
    package_arg.tenant_id_ = session_info_->get_effective_tenant_id();
    package_arg.db_name_ = db_name;
    package_arg.package_name_ = package_name;
    if (is_drop_body) {
      package_arg.package_type_ = share::schema::PACKAGE_BODY_TYPE;
    } else {
      package_arg.package_type_ = share::schema::PACKAGE_TYPE;
    }
    package_arg.compatible_mode_ = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                         : COMPATIBLE_MYSQL_MODE;
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase




