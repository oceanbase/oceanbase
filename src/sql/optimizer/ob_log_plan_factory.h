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
