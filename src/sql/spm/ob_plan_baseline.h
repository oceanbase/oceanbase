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

#ifndef OCEANBASE_SQL_SPM_OB_PLAN_BASELINE_
#define OCEANBASE_SQL_SPM_OB_PLAN_BASELINE_

#include "sql/spm/ob_spm_define.h"
#include "lib/container/ob_2d_array.h"

namespace oceanbase
{
namespace sql
{


class ObPlanBaselineSet
{
public:
  ObPlanBaselineSet();
  virtual ~ObPlanBaselineSet();
private:
  // PlanBaselineItemArray pbi_array_;
};

} //namespace sql end
} //namespace oceanbase end

#endif
