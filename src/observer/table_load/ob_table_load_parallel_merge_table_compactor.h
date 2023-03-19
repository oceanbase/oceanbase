// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "observer/table_load/ob_table_load_parallel_merge_ctx.h"
#include "observer/table_load/ob_table_load_table_compactor.h"

namespace oceanbase
{
namespace observer
{

class ObTableLoadParallelMergeTableCompactor : public ObTableLoadTableCompactor
{
public:
  ObTableLoadParallelMergeTableCompactor();
  virtual ~ObTableLoadParallelMergeTableCompactor();
  int start() override;
  void stop() override;
private:
  int inner_init() override;
  int handle_parallel_merge_success();
  int build_result();
private:
  class ParallelMergeCb : public ObTableLoadParallelMergeCb
  {
  public:
    ParallelMergeCb();
    virtual ~ParallelMergeCb() = default;
    int init(ObTableLoadParallelMergeTableCompactor *table_compactor);
    int on_success() override;
  private:
    ObTableLoadParallelMergeTableCompactor *table_compactor_;
  };
private:
  ObTableLoadParallelMergeCtx parallel_merge_ctx_;
  ParallelMergeCb parallel_merge_cb_;
};

} // namespace observer
} // namespace oceanbase
