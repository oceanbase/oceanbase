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

#ifndef OCEANBASE_SQL_OB_PACKAGE_EXECUTOR_H_
#define OCEANBASE_SQL_OB_PACKAGE_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"
#include "share/schema/ob_package_info.h"

#define DEF_SIMPLE_EXECUTOR(name)                          \
  class name##Executor                                     \
  {                                                        \
  public:                                                  \
    name##Executor() {}                                    \
    virtual ~name##Executor() {}                           \
    int execute(ObExecContext &ctx, name##Stmt &stmt);     \
  private:                                                 \
    DISALLOW_COPY_AND_ASSIGN(name##Executor);              \
  }

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreatePackageStmt;
class ObAlterPackageStmt;
class ObDropPackageStmt;

class ObCompilePackageInf
{
public:
  ObCompilePackageInf() {}
  virtual ~ObCompilePackageInf() {}
  static int compile_package(sql::ObExecContext &ctx,
                      uint64_t tenant_id,
                      const ObString &db_name,
                      oceanbase::share::schema::ObPackageType type,
                      const ObString &package_name,
                      int64_t schema_version);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCompilePackageInf);
};

class ObCreatePackageExecutor : ObCompilePackageInf
{
public:
  ObCreatePackageExecutor() {}
  virtual ~ObCreatePackageExecutor() {}
  int execute(ObExecContext &ctx, ObCreatePackageStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreatePackageExecutor);
};

class ObAlterPackageExecutor : ObCompilePackageInf
{
public:
  ObAlterPackageExecutor() {}
  virtual ~ObAlterPackageExecutor() {}
  int execute(ObExecContext &ctx, ObAlterPackageStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterPackageExecutor);
};

DEF_SIMPLE_EXECUTOR(ObDropPackage);
}//namespace sql
}//namespace oceanbase
#endif /* OCEANBASE_SQL_OB_PACKAGE_EXECUTOR_H_ */
