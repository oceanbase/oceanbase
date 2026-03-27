/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SPARSE_BMW_ITER_H_
#define OB_SPARSE_BMW_ITER_H_

#include "lib/container/ob_heap.h"
#include "ob_sparse_daat_iter.h"
#include "ob_sparse_retrieval_util.h"

namespace oceanbase
{
namespace storage
{
class ObSRBMWIterImpl : public ObSRBlockMaxTopKIterImpl
{
public:
  ObSRBMWIterImpl();
  virtual ~ObSRBMWIterImpl() {}
protected:
  virtual int top_k_search() override;
  int next_pivot(int64_t &pivot_iter_idx);
  int evaluate_pivot(const int64_t pivot_iter_idx);
  int evaluate_pivot_range(const int64_t pivot_iter_idx, bool &is_candidate);
private:
  int project_rows_from_top_k_heap(const int64_t capacity, int64_t &count);
  int unify_dim_iters_for_next_round();

protected:
  DISALLOW_COPY_AND_ASSIGN(ObSRBMWIterImpl);
};


} // namespace storage
} // namespace oceanbase
#endif
