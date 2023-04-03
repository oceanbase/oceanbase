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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PROCEDURE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PROCEDURE_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{
class ObDropProcedureResolver: public ObDDLResolver
{
public:
  explicit ObDropProcedureResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual ~ObDropProcedureResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropProcedureResolver);
  // function members

private:
  // data members
};

class ObDropFunctionResolver: public ObDDLResolver
{
public:
  explicit ObDropFunctionResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual ~ObDropFunctionResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropFunctionResolver);
};
} // end namespace sql
} // end namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PROCEDURE_RESOLVER_H_ */
