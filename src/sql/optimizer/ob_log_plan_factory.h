/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_LOG_PLAN_FACTORY_H
#define _OB_LOG_PLAN_FACTORY_H 1
#include "share/ob_define.h"
#include "lib/list/ob_obj_store.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
}  // namespace common

namespace sql
{
class ObLogPlan;
class ObOptimizerContext;
class ObDMLStmt;
class ObLogPlanFactory
{
public:
  explicit ObLogPlanFactory(common::ObIAllocator &allocator);
  ~ObLogPlanFactory();
  ObLogPlan *create(ObOptimizerContext &ctx, const ObDMLStmt &stmt);
  void destroy();
private:
  common::ObIAllocator &allocator_;
  common::ObObjStore<ObLogPlan*, common::ObIAllocator&, true> plan_store_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogPlanFactory);
};
}
}
#endif // _OB_LOG_PLAN_FACTORY_H
