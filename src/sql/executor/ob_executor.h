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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_
#define OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_

#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObPhysicalPlan;
class ObExecContext;
class ObPhyOperator;

class ObExecutor
{
public:
  ObExecutor()
    : inited_(false),
      phy_plan_(NULL),
      execution_id_(common::OB_INVALID_ID)
  {
    /* add your code here */
  }
  ~ObExecutor() {};
  int init(ObPhysicalPlan *plan);
  void reset();
  int execute_plan(ObExecContext &ctx);
  int close(ObExecContext &ctx);
private:
  // disallow copy
  ObExecutor(const ObExecutor &other);
  ObExecutor &operator=(const ObExecutor &ohter);
private:
  int execute_remote_single_partition_plan(ObExecContext &ctx);
  int execute_distributed_plan(ObExecContext &ctx);
  int execute_static_cg_px_plan(ObExecContext &ctx);
private:
  bool inited_;
  ObPhysicalPlan *phy_plan_;
  // 用于distributed scheduler
  uint64_t execution_id_;
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_ */
