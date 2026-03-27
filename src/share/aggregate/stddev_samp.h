/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_SHARE_AGGREGATE_STDDEV_SAMP_H_
 #define OCEANBASE_SHARE_AGGREGATE_STDDEV_SAMP_H_

 #include "statistics.h"

 namespace oceanbase
 {
 namespace share
 {
 namespace aggregate
 {
 namespace helper
 {
 template int init_statistics_aggregate<STDDEV_SAMP>(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                                     ObIAllocator &allocator, IAggregate *&agg);
 } // end helper
 } // end aggregate
 } // end share
 } // end oceanbase
 #endif // OCEANBASE_SHARE_AGGREGATE_ARG_MAX_H_