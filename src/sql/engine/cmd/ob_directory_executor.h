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

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DIRECTORY_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DIRECTORY_EXECUTOR_H_
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreateDirectoryStmt;
class ObAlterDirectoryStmt;
class ObDropDirectoryStmt;

class ObCreateDirectoryExecutor
{
public:
  ObCreateDirectoryExecutor() {}
  virtual ~ObCreateDirectoryExecutor() {}
  int execute(ObExecContext &ctx, ObCreateDirectoryStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateDirectoryExecutor);
};

class ObDropDirectoryExecutor
{
public:
  ObDropDirectoryExecutor() {}
  virtual ~ObDropDirectoryExecutor() {}
  int execute(ObExecContext &ctx, ObDropDirectoryStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropDirectoryExecutor);
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DIRECTORY_EXECUTOR_H_
