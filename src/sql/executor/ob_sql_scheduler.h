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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_SQL_SCHEDULER_
#define OCEANBASE_SQL_EXECUTOR_OB_SQL_SCHEDULER_

#include "share/ob_define.h"

namespace oceanbase {
namespace sql {
class ObPhysicalPlan;
class ObExecContext;
class ObSqlScheduler {
public:
  ObSqlScheduler()
  {}
  virtual ~ObSqlScheduler()
  {}

  virtual int schedule(ObExecContext& ctx, ObPhysicalPlan* phy_plan) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlScheduler);
}; /* end class */

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_EXECUTOR_OB_SQL_SCHEDULER_*/
