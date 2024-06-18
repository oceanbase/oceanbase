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

#ifndef OCEANBASE_SQL_OB_TRIGGER_EXECUTOR_H_
#define OCEANBASE_SQL_OB_TRIGGER_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_rpc_struct.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreateTriggerStmt;
class ObAlterTriggerStmt;
class ObDropTriggerStmt;

class ObCompileTriggerInf
{
public:
  ObCompileTriggerInf() {}
  virtual ~ObCompileTriggerInf() {}
  static int compile_trigger(sql::ObExecContext &ctx,
                      uint64_t tenant_id,
                      uint64_t db_id,
                      const ObString &trigger_name,
                      int64_t schema_version);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCompileTriggerInf);
};

class ObCreateTriggerExecutor : ObCompileTriggerInf
{
public:
  ObCreateTriggerExecutor() {}
  virtual ~ObCreateTriggerExecutor() {}
  int execute(ObExecContext &ctx, ObCreateTriggerStmt &stmt);
  int analyze_dependencies(share::schema::ObSchemaGetterGuard &schema_guard,
                           ObSQLSessionInfo *session_info,
                           common::ObMySQLProxy *sql_proxy,
                           ObIAllocator &allocator,
                           obrpc::ObCreateTriggerArg &arg);
private:
  ObCreateTriggerExecutor(const ObCreateTriggerExecutor&);
  void operator=(const ObCreateTriggerExecutor&);
};

class ObDropTriggerExecutor
{
public:
  ObDropTriggerExecutor() {}
  virtual ~ObDropTriggerExecutor() {}
  int execute(ObExecContext &ctx, ObDropTriggerStmt &stmt);
private:
  ObDropTriggerExecutor(const ObDropTriggerExecutor&);
  void operator=(const ObDropTriggerExecutor&);
};

class ObAlterTriggerExecutor : ObCompileTriggerInf
{
public:
  ObAlterTriggerExecutor() {}
  virtual ~ObAlterTriggerExecutor() {}
  int execute(ObExecContext &ctx, ObAlterTriggerStmt &stmt);
private:
  ObAlterTriggerExecutor(const ObAlterTriggerExecutor&);
  void operator=(const ObAlterTriggerExecutor&);
};

} // namespace sql
} // namespace oceanbase
#endif /* OCEANBASE_SQL_OB_TRIGGER_EXECUTOR_H_ */
