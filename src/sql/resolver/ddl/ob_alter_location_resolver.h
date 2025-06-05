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

#ifndef OCEANBASE_SQL_OB_ALTER_LOCATION_RESOLVER_H_
#define OCEANBASE_SQL_OB_ALTER_LOCATION_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObAlterLocationResolver: public ObDDLResolver
{
  static const int64_t LOCATION_NAME = 0;
  static const int64_t LOCATION_MODIFY = 1;
  static const int64_t LOCATION_NODE_COUNT = 2;
public:
  explicit ObAlterLocationResolver(ObResolverParams &params);
  virtual ~ObAlterLocationResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterLocationResolver);
};
} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_OB_ALTER_LOCATION_RESOLVER_H_