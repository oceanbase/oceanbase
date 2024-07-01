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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SORT_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SORT_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/engine/sort/ob_sort_op_impl.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

struct ObDASSortIterParam : public ObDASIterParam
{
public:
  ObDASSortIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_SORT),
      sort_ctdef_(nullptr),
      child_(nullptr),
      limit_param_() {}
  virtual ~ObDASSortIterParam() {}
  const ObDASSortCtDef *sort_ctdef_;
  ObDASIter *child_;
  common::ObLimitParam limit_param_;

  virtual bool is_valid() const override
  {
    return ObDASIterParam::is_valid() && nullptr != sort_ctdef_ && nullptr != child_;
  }
};

class ObDASSortIter : public ObDASIter
{
public:
  ObDASSortIter()
    : ObDASIter(ObDASIterType::DAS_ITER_SORT),
      sort_impl_(),
      sort_memctx_(),
      sort_ctdef_(nullptr),
      sort_row_(),
      child_(nullptr),
      sort_finished_(false),
      limit_param_(),
      input_row_cnt_(0),
      output_row_cnt_(0),
      fake_skip_(nullptr)
  {}
  virtual ~ObDASSortIter() {}

  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int do_sort(bool is_vectorized);

private:
  ObSortOpImpl sort_impl_;
  lib::MemoryContext sort_memctx_;
  const ObDASSortCtDef *sort_ctdef_;
  ObSEArray<ObExpr *, 2> sort_row_;
  ObDASIter *child_;
  bool sort_finished_;
  // limit param was set only once at do_table_scan of TSC, which means it should not be reset at reuse,
  // input row cnt and output row cnt are the same as well.
  common::ObLimitParam limit_param_;
  int64_t input_row_cnt_;
  int64_t output_row_cnt_;
  ObBitVector *fake_skip_;
};


}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SORT_ITER_H_ */
