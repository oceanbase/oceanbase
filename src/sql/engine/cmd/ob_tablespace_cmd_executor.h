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

#ifndef OCEANBASE_SQL_ENGINE_CMD_TABLESPACE_CMD_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_TABLESPACE_CMD_EXECUTOR_
#include "lib/string/ob_string.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObCommonRpcProxy;
struct ObTablespaceDDLArg;
}
namespace sql
{
class ObExecContext;
class ObCreateTablespaceStmt;
class ObDropTablespaceStmt;
class ObAlterTablespaceStmt;
class ObCreateTablespaceExecutor
{
public:
  ObCreateTablespaceExecutor() {}
  virtual ~ObCreateTablespaceExecutor() {}
  int execute(ObExecContext &ctx, ObCreateTablespaceStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateTablespaceExecutor);
};


class ObAlterTablespaceExecutor
{
public:
  ObAlterTablespaceExecutor() {}
  virtual ~ObAlterTablespaceExecutor() {}
  int execute(ObExecContext &ctx, ObAlterTablespaceStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterTablespaceExecutor);
};


class ObDropTablespaceExecutor
{
public:
  ObDropTablespaceExecutor() {}
  virtual ~ObDropTablespaceExecutor() {}
  int execute(ObExecContext &ctx, ObDropTablespaceStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropTablespaceExecutor);
};
}
}
#endif //OCEANBASE_SQL_ENGINE_CMD_USER_CMD_EXECUTOR_
