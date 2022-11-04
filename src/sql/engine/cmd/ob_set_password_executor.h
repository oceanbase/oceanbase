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

#ifndef OCEANBASE_SQL_SET_PASSWORD_EXECUTOR_
#define OCEANBASE_SQL_SET_PASSWORD_EXECUTOR_

namespace oceanbase
{
namespace common
{
class ObString;
class ObSqlString;
}
namespace obrpc
{
class ObCommonRpcProxy;
}

namespace sql
{
class ObExecContext;
class ObSetPasswordStmt;

class ObSetPasswordExecutor
{
public:
  ObSetPasswordExecutor();
  virtual ~ObSetPasswordExecutor();
  int execute(ObExecContext &ctx, ObSetPasswordStmt &stmt);
};

}
}
#endif /* __OB_SQL_SET_PASSWORD_EXECUTOR_H__ */
//// end of header file

