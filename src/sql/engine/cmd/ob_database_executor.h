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

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DATABASE_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DATABASE_EXECUTOR_H_
#include "share/ob_define.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreateDatabaseStmt;
class ObDropDatabaseStmt;
class ObUseDatabaseStmt;
class ObAlterDatabaseStmt;
class ObFlashBackDatabaseStmt;
class ObPurgeDatabaseStmt;
class ObCreateDatabaseExecutor
{
public:
  ObCreateDatabaseExecutor();
  virtual ~ObCreateDatabaseExecutor();
  int execute(ObExecContext &ctx, ObCreateDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateDatabaseExecutor);
};

///////////////////////
class ObUseDatabaseExecutor
{
public:
  ObUseDatabaseExecutor();
  virtual ~ObUseDatabaseExecutor();
  int execute(ObExecContext &ctx, ObUseDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObUseDatabaseExecutor);
};

///////////////////////
class ObAlterDatabaseExecutor
{
public:
  ObAlterDatabaseExecutor();
  virtual ~ObAlterDatabaseExecutor();
  int execute(ObExecContext &ctx, ObAlterDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterDatabaseExecutor);
};

/////////////////////
class ObDropDatabaseExecutor
{
public:
  ObDropDatabaseExecutor();
  virtual ~ObDropDatabaseExecutor();
  int execute(ObExecContext &ctx, ObDropDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropDatabaseExecutor);
};


/* *
 *
 * */
class ObFlashBackDatabaseExecutor
{
public:
  ObFlashBackDatabaseExecutor() {}
  virtual ~ObFlashBackDatabaseExecutor() {}
  int execute(ObExecContext &ctx, ObFlashBackDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackDatabaseExecutor);
};

class ObPurgeDatabaseExecutor
{
public:
  ObPurgeDatabaseExecutor() {}
  virtual ~ObPurgeDatabaseExecutor() {}
  int execute(ObExecContext &ctx, ObPurgeDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPurgeDatabaseExecutor);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DATABASE_EXECUTOR_H_ */
