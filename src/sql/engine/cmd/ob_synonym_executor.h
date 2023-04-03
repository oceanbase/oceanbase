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

#ifndef OCEANBASE_SQL_OB_SYNONYM_EXECUTOR_
#define OCEANBASE_SQL_OB_SYNONYM_EXECUTOR_
#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObExprCtx;
// namespace sqlclient
// {
class ObMySQLProxy;
// }
}
namespace sql
{
class ObExecContext;
class ObRawExpr;
class ObCreateSynonymStmt;
class ObCreateSynonymExecutor
{
public:
  const static int OB_DEFAULT_ARRAY_SIZE = 16;
  ObCreateSynonymExecutor(){}
  virtual ~ObCreateSynonymExecutor(){}
  int execute(ObExecContext &ctx, ObCreateSynonymStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateSynonymExecutor);
};

class ObDropSynonymStmt;
class ObDropSynonymExecutor
{
public:
  ObDropSynonymExecutor(){}
  virtual ~ObDropSynonymExecutor(){}
  int execute(ObExecContext &ctx, ObDropSynonymStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropSynonymExecutor);
};

} //end namespace sql
} //end namespace oceanbase


#endif //OCEANBASE_SQL_OB_SYNONYM_EXECUTOR_
