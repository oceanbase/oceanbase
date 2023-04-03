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

#ifndef OCEANBASE_SQL_OB_CREATE_TABLEGROUP_RESOLVER_
#define OCEANBASE_SQL_OB_CREATE_TABLEGROUP_RESOLVER_
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_tablegroup_resolver.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{
class ObCreateTablegroupResolver: public ObTableGroupResolver
{
  static const int64_t IF_NOT_EXIST = 0;
  static const int64_t TG_NAME = 1;
  static const int64_t TABLEGROUP_OPTION = 2;
  static const int64_t PARTITION_OPTION = 3;
public:
  explicit ObCreateTablegroupResolver(ObResolverParams &params);
  virtual ~ObCreateTablegroupResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateTablegroupResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_CREATE_TABLEGROUP_RESOLVER_ */
