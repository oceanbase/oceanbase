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

#ifndef _OB_CREATE_SYNONYM_RESOLVER_H
#define _OB_CREATE_SYNONYM_RESOLVER_H
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{
class ObCreateSynonymStmt;
class ObCreateSynonymResolver: public ObDDLResolver
{
public:
  explicit ObCreateSynonymResolver(ObResolverParams &params);
  virtual ~ObCreateSynonymResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  int check_valid(const ObCreateSynonymStmt *synonym_stmt);
  DISALLOW_COPY_AND_ASSIGN(ObCreateSynonymResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_CREATE_TABLE_RESOLVER_H */
