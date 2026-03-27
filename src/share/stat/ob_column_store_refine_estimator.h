/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
