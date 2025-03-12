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

#ifndef OB_COLUMN_STORE_REFINE_ESTIMATOR_H
#define OB_COLUMN_STORE_REFINE_ESTIMATOR_H

#include "share/stat/ob_basic_stats_estimator.h"
namespace oceanbase
{
namespace common
{

class ObColumnStoreRefineEstimator : public ObBasicStatsEstimator
{
public:
  explicit ObColumnStoreRefineEstimator(ObExecContext &ctx, ObIAllocator &allocator);

  int estimate(const ObOptStatGatherParam &param,
               ObOptStat &opt_stat);

};

} // end of namespace common
} // end of namespace oceanbase

#endif /*#endif OB_COLUMN_STORE_REFINE_ESTIMATOR_H */
