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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_AND_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_AND_ITER_H_

#include "sql/das/iter/ob_das_index_merge_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

class ObDASIndexMergeAndIter : public ObDASIndexMergeIter
{

public:
  ObDASIndexMergeAndIter()
    : ObDASIndexMergeIter(),
      can_be_shorted_(false),
      shorted_child_idx_(OB_INVALID_INDEX)
  {}

  virtual ~ObDASIndexMergeAndIter() {}

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int bitmap_get_next_row();
  int bitmap_get_next_rows(int64_t &count, int64_t capacity);
  int sort_get_next_row();
  int sort_get_next_rows(int64_t &count, int64_t capacity);
  // short circuit path
  int shorted_get_next_row();
  int shorted_get_next_rows(int64_t &count, int64_t capacity);

  int check_can_be_shorted();
  int get_child_stores_last_valid_rowkey(uint64_t &last_valid_rowkey) const;
  int locate_child_stores_to_valid_rowkey(uint64_t rowkey);

private:
  bool can_be_shorted_;
  int64_t shorted_child_idx_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_AND_ITER_H_ */
