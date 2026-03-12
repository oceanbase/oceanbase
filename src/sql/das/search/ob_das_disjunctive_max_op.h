/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OB_DAS_DISJUNCTIVE_MAX_OP_H_
#define OCEANBASE_SQL_OB_DAS_DISJUNCTIVE_MAX_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "ob_das_topk_utils.h"

namespace oceanbase
{
namespace sql
{

struct ObDASDisjunctiveMaxOpParam : public ObIDASSearchOpParam
{
  ObDASDisjunctiveMaxOpParam(
      const ObIArray<ObIDASSearchOp *> &children_ops)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_DISJUNCTIVE_MAX),
      limit_(-1),
      children_ops_(&children_ops)
  {}
  virtual ~ObDASDisjunctiveMaxOpParam() {}

  bool is_valid() const
  {
    return limit_ > 0 && children_ops_->count() > 0;
  }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
  INHERIT_TO_STRING_KV("ObDASDisjunctiveMaxOpParam", ObIDASSearchOpParam,
      K_(limit));

  int64_t limit_;
  const ObIArray<ObIDASSearchOp *> *children_ops_;
};

class ObDASDisjunctiveMaxOp : public ObIDASSearchOp
{
public:
  ObDASDisjunctiveMaxOp(ObDASSearchCtx &search_ctx);
  virtual ~ObDASDisjunctiveMaxOp() {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override { return OB_NOT_SUPPORTED; };
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override
  { return OB_NOT_SUPPORTED; }
  int do_calc_max_score(double &threshold) override
  { return OB_NOT_SUPPORTED; }

private:
  int load_results();
private:
  ObDASTopKItemCmp cmp_;
  ObDASTopKHeap heap_;
  ObDASTopKHashMap map_;
  ObSEArray<ObDocIdExt, 32> id_cache_;
  ObDASRowID curr_id_;
  int64_t limit_;
  bool is_loaded_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_DISJUNCTIVE_MAX_OP_H_
