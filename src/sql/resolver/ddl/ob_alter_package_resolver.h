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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_PACKAGE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_PACKAGE_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
namespace oceanbase
{
namespace pl
{
class ObPLPackageAST;
class ObPLCompiler;
}
namespace sql
{
class ObAlterPackageStmt;
class ObAlterPackageResolver: public ObDDLResolver
{
public:
  explicit ObAlterPackageResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual ~ObAlterPackageResolver() {}

  virtual int resolve(const ParseNode &parse_tree);

private:
  int resolve_alter_clause(const ParseNode &alter_clause,
                           const ObString &db_name,
                           const ObString &package_name,
                           ObAlterPackageStmt &alter_stmt);
  int resolve_alter_compile_clause(const ParseNode &alter_clause,
                                   const ObString &db_name,
                                   const ObString &package_name,
                                   ObAlterPackageStmt &alter_stmt);
  int analyze_package(pl::ObPLCompiler &compiler,
                      const pl::ObPLBlockNS *parent_ns,
                      pl::ObPLPackageAST &package_ast,
                      const ObString& db_name, 
                      const ObPackageInfo *package_info,
                      share::schema::ObErrorInfo &error_info,
                      bool &has_error);
  int compile_package(const ObString& db_name,
                      const ObString &package_name,
                      int16_t compile_flag,
                      ObAlterPackageStmt &alter_stmt);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterPackageResolver);
};
} //namespace sql
} //namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_PACKAGE_RESOLVER_H_ */
