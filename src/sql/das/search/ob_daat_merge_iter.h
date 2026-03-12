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

#ifndef SQL_DAS_SEARCH_OB_DAAT_MERGE_ITER_H_
#define SQL_DAS_SEARCH_OB_DAAT_MERGE_ITER_H_

#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase

{
namespace sql
{

// Operator wrapper iter for document-at-a-time query processing
class ObDAATMergeIter
{
public:
  ObDAATMergeIter();
  virtual ~ObDAATMergeIter() {}
  int init(const int64_t iter_idx, ObIDASSearchOp &op);
  void reuse();
  int next();
  int advance_to(const ObDASRowID &target);
  int advance_shallow(const ObDASRowID &target, const bool inclusive);
  int to_shallow();
  int get_curr_id(ObDASRowID &curr_id) const;
  int get_curr_score(double &score) const;
  int get_curr_block_max_info(const MaxScoreTuple *&max_score_tuple) const;
  int get_max_score(double &max_score);
  int64_t get_iter_idx() const { return iter_idx_; }
  bool in_shallow_status() const { return in_shallow_status_; }
  bool iter_end() const { return iter_end_; }
  TO_STRING_KV(
      K_(curr_id),
      K_(curr_score),
      KP_(curr_block_max_info),
      K_(max_score),
      K_(iter_idx),
      K_(in_shallow_status),
      K_(iter_end));
private:
  ObIDASSearchOp *op_;
  ObDASRowID curr_id_;
  double curr_score_;
  const MaxScoreTuple *curr_block_max_info_;
  double max_score_;
  int64_t iter_idx_;
  bool max_score_calculated_;
  bool in_shallow_status_;
  bool iter_end_;
  bool is_inited_;
};


} // namespace sql
} // namespace oceanbase
#endif
