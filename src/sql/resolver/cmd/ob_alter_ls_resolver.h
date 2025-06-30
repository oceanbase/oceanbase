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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_ALTER_LS_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_ALTER_LS_RESOLVER_

#include "sql/resolver/cmd/ob_alter_ls_stmt.h"
#include "sql/resolver/cmd/ob_system_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObAlterLSStmt;
class ObAlterLSResolver : public ObSystemCmdResolver
{
public:
  ObAlterLSResolver(ObResolverParams &params) :  ObSystemCmdResolver(params) {}
  virtual ~ObAlterLSResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_create_ls_(const ParseNode &parse_tree, ObAlterLSStmt *stmt);
  int resolve_modify_ls_(const ParseNode &parse_tree, ObAlterLSStmt *stmt);
  int resolve_drop_ls_(const ParseNode &parse_tree, ObAlterLSStmt *stmt);
  int resolve_ls_attr_(
      const ParseNode &parse_tree,
      uint64_t &unit_group_id,
      ObZone &primary_zone,
      share::ObAlterLSArg::UnitListArg &unit_list);
};
}// namespace sql
}// namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_ALTER_LS_RESOLVER_ */