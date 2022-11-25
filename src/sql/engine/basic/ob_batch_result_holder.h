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

#ifndef OCEANBASE_BASIC_OB_BATCH_RESULT_HOLDER_H_
#define OCEANBASE_BASIC_OB_BATCH_RESULT_HOLDER_H_

#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace sql
{

// Batch result backup && restore, backup rows by shadow copy.
class ObBatchResultHolder
{
public:
  ObBatchResultHolder() : exprs_(NULL), eval_ctx_(NULL), datums_(NULL),
                          saved_size_(0), inited_(false)
  {
  }

  int init(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx);
  int save(int64_t size);
  int restore();
  bool is_saved() const { return saved_size_ > 0; }
  void reset() { saved_size_ = 0; }
  int check_datum_modified();

private:
  const common::ObIArray<ObExpr *> *exprs_;
  ObEvalCtx *eval_ctx_;
  ObDatum *datums_;
  int64_t saved_size_;
  bool inited_;
};


} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_BATCH_RESULT_HOLDER_H_
