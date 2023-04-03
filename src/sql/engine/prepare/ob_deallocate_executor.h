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

#ifndef OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_DEALLOCATE_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_DEALLOCATE_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"


namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObDeallocateStmt;

class ObDeallocateExecutor
{
public:
  ObDeallocateExecutor() {}
  virtual ~ObDeallocateExecutor() {}
  int execute(ObExecContext &ctx, ObDeallocateStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDeallocateExecutor);
};

}
}



#endif /* OCEANBASE_SRC_SQL_ENGINE_PREPARE_OB_DEALLOCATE_EXECUTOR_H_ */
