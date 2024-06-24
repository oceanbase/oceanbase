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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_GLOBAL_LOOKUP_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_GLOBAL_LOOKUP_ITER_H_

#include "sql/das/iter/ob_das_lookup_iter.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

struct ObDASGlobalLookupIterParam : public ObDASLookupIterParam
{
public:
  ObDASGlobalLookupIterParam()
    : ObDASLookupIterParam(true /*global lookup*/),
      can_retry_(false),
      calc_part_id_(nullptr)
  {}
  bool can_retry_;
  const ObExpr *calc_part_id_;

  virtual bool is_valid() const override
  {
    return ObDASLookupIterParam::is_valid() && calc_part_id_ != nullptr;
  }
};

class ObDASGlobalLookupIter : public ObDASLookupIter
{
public:
  ObDASGlobalLookupIter()
    : ObDASLookupIter(ObDASIterType::DAS_ITER_GLOBAL_LOOKUP),
      can_retry_(false),
      calc_part_id_(nullptr),
      index_ordered_idx_(0)
  {}
  virtual ~ObDASGlobalLookupIter() {}

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int add_rowkey() override;
  virtual int add_rowkeys(int64_t count) override;
  virtual int do_index_lookup() override;
  virtual int check_index_lookup() override;
  virtual void reset_lookup_state();

private:
  bool can_retry_;
  const ObExpr *calc_part_id_;
  int64_t index_ordered_idx_; // used for keep order of global lookup
};

}  // namespace sql
}  // namespace oceanbase



#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_GLOBAL_LOOKUP_ITER_H_ */
