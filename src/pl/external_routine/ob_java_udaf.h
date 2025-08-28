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
