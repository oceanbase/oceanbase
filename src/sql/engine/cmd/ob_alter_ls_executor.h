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

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_LS_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_LS_EXECUTOR_

#include "sql/resolver/cmd/ob_alter_ls_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObAlterLSExecutor
{
public:
  ObAlterLSExecutor() {}
  virtual ~ObAlterLSExecutor() {}
  int execute(ObExecContext &ctx, ObAlterLSStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterLSExecutor);
};
}
}
#endif // OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_LS_EXECUTOR_