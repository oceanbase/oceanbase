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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_OR_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_OR_ITER_H_

#include "sql/das/iter/ob_das_index_merge_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

class ObDASIndexMergeOrIter : public ObDASIndexMergeIter
{

public:
  ObDASIndexMergeOrIter()
    : ObDASIndexMergeIter(),
      child_match_against_exprs_()
  {}

  virtual ~ObDASIndexMergeOrIter() {}

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual bool can_limit_pushdown(const ObDASPushDownTopN &push_down_topn) override;

private:
  int sort_get_next_row();
  int sort_get_next_rows(int64_t &count, int64_t capacity);

private:
  int fill_default_values(const common::ObIArray<ObExpr*> &exprs) const;
  int extract_match_against_exprs(const common::ObIArray<ObExpr*> &exprs, common::ObIArray<ObExpr*> &match_against_exprs) const;

private:
  // need to fill default value for columns that do not have corresponding output when union merge,
  // only relevance score in fulltext search for now.
  common::ObFixedArray<ExprFixedArray, common::ObIAllocator> child_match_against_exprs_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_OR_ITER_H_ */
