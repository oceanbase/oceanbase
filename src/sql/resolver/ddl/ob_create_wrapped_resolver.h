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

#ifdef OB_BUILD_ORACLE_PL

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_WRAPPED_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_WRAPPED_RESOLVER_H_

#include "lib/utility/ob_macro_utils.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ddl/ob_create_package_resolver.h"
#include "sql/resolver/ddl/ob_create_routine_resolver.h"
#include "sql/resolver/ddl/ob_create_udt_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObCreateWrappedResolver
{
public:
  explicit ObCreateWrappedResolver(const ObResolverParams &params) : params_(params) {}
  virtual ~ObCreateWrappedResolver() {}
  DISABLE_COPY_ASSIGN(ObCreateWrappedResolver);

protected:
 int resolve_base64_cipher(ObIAllocator &allocator,
                           const ObSQLSessionInfo &session,
                           const ParseNode &base64_cipher_node,
                           const bool is_or_replace,
                           const bool is_noneditionable,
                           ParseResult &plain_parse_result);
 int check_object_name_match(const ParseNode *n1, const ParseNode *n2);
 int check_plwrap_version_compatible();

private:
  const ObResolverParams &params_;
};

class ObCreateWrappedPackageResolver : public ObCreatePackageResolver,
                                       public ObCreateWrappedResolver
{
public:
  explicit ObCreateWrappedPackageResolver(ObResolverParams &params)
      : ObCreatePackageResolver(params), ObCreateWrappedResolver(params) {}
  virtual ~ObCreateWrappedPackageResolver() {}
  DISABLE_COPY_ASSIGN(ObCreateWrappedPackageResolver);

  virtual int resolve(const ParseNode &cipher_parse_tree) override;
};

class ObCreateWrappedPackageBodyResolver : public ObCreatePackageBodyResolver,
                                           public ObCreateWrappedResolver
{
public:
  explicit ObCreateWrappedPackageBodyResolver(ObResolverParams &params)
      : ObCreatePackageBodyResolver(params), ObCreateWrappedResolver(params) {}
  virtual ~ObCreateWrappedPackageBodyResolver() {}
  DISABLE_COPY_ASSIGN(ObCreateWrappedPackageBodyResolver);

  virtual int resolve(const ParseNode &cipher_parse_tree) override;
};

class ObCreateWrappedTypeResolver : public ObCreateUDTResolver,
                                    public ObCreateWrappedResolver
{
public:
  explicit ObCreateWrappedTypeResolver(ObResolverParams &params)
      : ObCreateUDTResolver(params), ObCreateWrappedResolver(params) {}
  virtual ~ObCreateWrappedTypeResolver() {}
  DISABLE_COPY_ASSIGN(ObCreateWrappedTypeResolver);

  virtual int resolve(const ParseNode &cipher_parse_tree) override;
};

class ObCreateWrappedTypeBodyResolver : public ObCreateUDTBodyResolver,
                                        public ObCreateWrappedResolver
{
public:
  explicit ObCreateWrappedTypeBodyResolver(ObResolverParams &params)
      : ObCreateUDTBodyResolver(params), ObCreateWrappedResolver(params) {}
  virtual ~ObCreateWrappedTypeBodyResolver() {}
  DISABLE_COPY_ASSIGN(ObCreateWrappedTypeBodyResolver);

  virtual int resolve(const ParseNode &cipher_parse_tree) override;
};

class ObCreateWrappedFunctionResolver : public ObCreateFunctionResolver,
                                        public ObCreateWrappedResolver
{
public:
  explicit ObCreateWrappedFunctionResolver(ObResolverParams &params)
      : ObCreateFunctionResolver(params), ObCreateWrappedResolver(params) {}
  virtual ~ObCreateWrappedFunctionResolver() {}
  DISABLE_COPY_ASSIGN(ObCreateWrappedFunctionResolver);

  virtual int resolve(const ParseNode &cipher_parse_tree) override;
};

class ObCreateWrappedProcedureResolver : public ObCreateProcedureResolver,
                                         public ObCreateWrappedResolver
{
public:
  explicit ObCreateWrappedProcedureResolver(ObResolverParams &params)
      : ObCreateProcedureResolver(params), ObCreateWrappedResolver(params) {}
  virtual ~ObCreateWrappedProcedureResolver() {}
  DISABLE_COPY_ASSIGN(ObCreateWrappedProcedureResolver);

  virtual int resolve(const ParseNode &cipher_parse_tree) override;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_WRAPPED_RESOLVER_H_ */
#endif /* OB_BUILD_ORACLE_PL */
