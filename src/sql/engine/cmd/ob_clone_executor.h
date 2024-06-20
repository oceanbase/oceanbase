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

#ifndef OCEANBASE_SQL_OB_CLONE_EXECUTOR_
#define OCEANBASE_SQL_OB_CLONE_EXECUTOR_

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCloneTenantStmt;

class ObCloneTenantExecutor
{
public:
  ObCloneTenantExecutor() {}
  virtual ~ObCloneTenantExecutor() {}
  int execute(ObExecContext &ctx, ObCloneTenantStmt &stmt);
  int wait_clone_tenant_finished_(ObExecContext &ctx,
                                  const int64_t job_id);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCloneTenantExecutor);
};

} //end namespace sql
} //end namespace oceanbase


#endif //OCEANBASE_SQL_OB_CLONE_EXECUTOR_
