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
