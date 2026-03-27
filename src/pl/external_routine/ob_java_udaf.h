/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UDAF_H_
#define OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UDAF_H_

#include "sql/engine/aggregate/ob_aggregate_processor.h"

namespace oceanbase
{

namespace common
{

class ObDatum;

}

namespace sql
{

class ObAggrInfo;
class ObEvalCtx;

}

namespace pl
{

class ObJavaUDAFExecutor
{
  private:
  using GroupConcatExtraResult = sql::ObAggregateProcessor::GroupConcatExtraResult;

public:
  ObJavaUDAFExecutor(const sql::ObAggrInfo &aggr_info,
                     sql::ObEvalCtx &eval_ctx,
                     GroupConcatExtraResult &extra_result)
    : aggr_info_(aggr_info),
      eval_ctx_(eval_ctx),
      extra_result_(extra_result)
  {  }

  int init();
  int execute(common::ObDatum &result);

private:
  bool is_inited_ = false;
  const sql::ObAggrInfo &aggr_info_;
  sql::ObEvalCtx &eval_ctx_;
  GroupConcatExtraResult &extra_result_;
  jobject loader_ = nullptr;
};

}  // namespace pl
}  // namespace oceanbase

#endif  // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UDAF_H_
