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

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_UDT_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_UDT_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"

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
class ObCreateUDTStmt;
class ObDropUDTStmt;

class ObCreateUDTExecutor
{
public:
  ObCreateUDTExecutor() {}
  virtual ~ObCreateUDTExecutor() {}
  int execute(ObExecContext &ctx, ObCreateUDTStmt &stmt);
  static int compile_udt(sql::ObExecContext &ctx,
                  const ObString &db_name,
                  const ObString &udt_name,
                  int64_t type_code,
                  int64_t schema_version);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateUDTExecutor);
};

//参考alter system定义
DEF_SIMPLE_EXECUTOR(ObDropUDT);

}//namespace sql
}//namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_ENGINE_CMD_OB_UDT_EXECUTOR_H_ */
