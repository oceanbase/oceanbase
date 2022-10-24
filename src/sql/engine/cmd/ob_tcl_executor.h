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

#ifndef OCRANBASE_SQL_ENGINE_CMD_OB_TCL_CMD_EXECUTOR_
#define OCRANBASE_SQL_ENGINE_CMD_OB_TCL_CMD_EXECUTOR_

#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObEndTransStmt;
class ObEndTransExecutor
{
public:
  ObEndTransExecutor() {}
  virtual ~ObEndTransExecutor() {}
  int execute(ObExecContext &ctx, ObEndTransStmt &stmt);
private:
  int end_trans(ObExecContext &ctx, ObEndTransStmt &stmt);
  DISALLOW_COPY_AND_ASSIGN(ObEndTransExecutor);
};

class ObStartTransStmt;
class ObStartTransExecutor
{
public:
  ObStartTransExecutor() {}
  virtual ~ObStartTransExecutor() {}
  int execute(ObExecContext &ctx, ObStartTransStmt &stmt);
private:
  int start_trans(ObExecContext &ctx, ObStartTransStmt &stmt);
  DISALLOW_COPY_AND_ASSIGN(ObStartTransExecutor);
};

class ObCreateSavePointStmt;
class ObCreateSavePointExecutor
{
public:
  ObCreateSavePointExecutor() {}
  virtual ~ObCreateSavePointExecutor() {}
  int execute(ObExecContext &ctx, ObCreateSavePointStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateSavePointExecutor);
};

class ObRollbackSavePointStmt;
class ObRollbackSavePointExecutor
{
public:
  ObRollbackSavePointExecutor() {}
  virtual ~ObRollbackSavePointExecutor() {}
  int execute(ObExecContext &ctx, ObRollbackSavePointStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObRollbackSavePointExecutor);
};

class ObReleaseSavePointStmt;
class ObReleaseSavePointExecutor
{
public:
  ObReleaseSavePointExecutor() {}
  virtual ~ObReleaseSavePointExecutor() {}
  int execute(ObExecContext &ctx, ObReleaseSavePointStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObReleaseSavePointExecutor);
};

}
}
#endif // OCRANBASE_SQL_ENGINE_CMD_OB_TCL_CMD_EXECUTOR_
