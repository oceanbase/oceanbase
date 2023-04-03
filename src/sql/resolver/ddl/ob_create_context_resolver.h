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

#ifndef _OB_SQL_RESOLVER_DDL_CREATE_CONTEXT_RESOLVER_H_
#define _OB_SQL_RESOLVER_DDL_CREATE_CONTEXT_RESOLVER_H_

#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_context_stmt.h"
#include "sql/resolver/ob_stmt.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObCreateContextResolver : public ObDDLResolver
{
  static const int64_t ROOT_NUM_CHILD = 4;
  static const int64_t OR_REPLACE_NODE = 0;
  static const int64_t CONTEXT_NAMESPACE = 1;
  static const int64_t TRUSTED_PACKAGE_NAME = 2;
  static const int64_t ACCESSED_TYPE = 3;
public:
  explicit ObCreateContextResolver(ObResolverParams &params);
  virtual ~ObCreateContextResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_context_namespace(const ParseNode &namespace_node,
                                ObString &ctx_namespace);
  int check_context_namespace(const ObString &ctx_namespace);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateContextResolver);
};

}  // namespace sql
}  // namespace oceanbase

#endif  //_OB_SQL_RESOLVER_DDL_CREATE_CONTEXT_RESOLVER_H_
