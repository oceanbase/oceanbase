/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
