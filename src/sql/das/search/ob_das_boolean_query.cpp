/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS
#include "lib/container/ob_heap.h"
#include "ob_das_boolean_query.h"
#include "ob_das_conjunction_op.h"
#include "ob_das_disjunction_op.h"
#include "ob_das_disjunction_filter_op.h"
#include "ob_das_bmm_op.h"
#include "ob_das_bmw_op.h"
#include "lib/utility/ob_sort.h"

namespace oceanbase
{
namespace sql
{
ERRSIM_POINT_DEF(DISABLE_BMM_FILTER_PUSHDOWN, "disable bmm filter pushdown");
namespace {
// Comparator for int64_t with get_error_code() method required by ObBinaryHeap
// Returns a < b to create a max heap (largest element at top)
class Int64LessComp
{
public:
  int get_error_code() { return OB_SUCCESS; }
  bool operator()(int64_t a, int64_t b)
  {
    return a < b;
  }
};
} // anonymous namespace

OB_SERIALIZE_MEMBER((ObDASBooleanQueryCtDef, ObIDASSearchCtDef),
                    must_,
                    filter_,
                    should_,
                    must_not_,
                    min_should_match_);

OB_SERIALIZE_MEMBER(ObDASBooleanQueryCtDef::SubClauseInfo,
                    exist_,
                    offset_,
                    count_);

OB_SERIALIZE_MEMBER((ObDASBooleanQueryRtDef, ObIDASSearchRtDef));

int ObDASBooleanQueryRtDef::compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  cost.reset();
  ObDASSearchCost min_required_cost;
  ObDASSearchCost min_should_cost;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (OB_FAIL(compute_required_cost(search_ctx, min_required_cost))) {
    LOG_WARN("failed to compute required cost", KR(ret));
  } else if (min_required_cost.is_valid() && ctdef->min_should_match_ == 0) {
    // when min_should_match = 0, should is handled in ReqOpt, no need to consider its cost
    cost = min_required_cost;
  } else if (OB_FAIL(compute_should_cost(search_ctx, min_should_cost))) {
    LOG_WARN("failed to compute should cost", KR(ret));
  } else if (!min_required_cost.is_valid() && !min_should_cost.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid cost", KR(ret));
  } else if (!min_required_cost.is_valid()) {
    cost = min_should_cost;
  } else if (!min_should_cost.is_valid()) {
    cost = min_required_cost;
  } else if (OB_FAIL(ObDASSearchCost::min(min_required_cost, min_should_cost, cost))) {
    LOG_WARN("failed to get min cost", KR(ret), K(min_required_cost), K(min_should_cost));
  }
  return ret;
}

int ObDASBooleanQueryRtDef::compute_required_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  cost.reset();
  ObBooleanSubClause<ObIDASSearchRtDef> must_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses;
  if (OB_FAIL(must(must_clauses))) {
    LOG_WARN("failed to get must clauses", KR(ret));
  } else if (OB_FAIL(filter(filter_clauses))) {
    LOG_WARN("failed to get filter clauses", KR(ret));
  } else {
    #define ITERATE_CLAUSES(clauses)                                                                                     \
    do {                                                                                                                 \
      for (int i = 0; OB_SUCC(ret) && i < clauses.count(); i++) {                                                        \
        ObDASSearchCost clause_cost;                                                                                     \
        ObIDASSearchRtDef *clause = nullptr;                                                                             \
        if (OB_FAIL(clauses.get(i, clause))) {                                                                           \
          LOG_WARN("failed to get clause", KR(ret), K(i));                                                               \
        } else if (OB_ISNULL(clause)) {                                                                                  \
          ret = OB_ERR_UNEXPECTED;                                                                                       \
          LOG_WARN("unexpected nullptr clause", KR(ret), K(i));                                                          \
        } else if (OB_FAIL(clause->get_cost(search_ctx, clause_cost))) {                                                 \
          LOG_WARN("failed to get cost", KR(ret));                                                                       \
        } else if (!clause_cost.is_valid()) {                                                                            \
          ret = OB_ERR_UNEXPECTED;                                                                                       \
          LOG_WARN("invalid cost", KR(ret));                                                                             \
        } else if (!cost.is_valid()) {                                                                                   \
          cost = clause_cost;                                                                                            \
        } else if (OB_FAIL(ObDASSearchCost::min(cost, clause_cost, cost))) {                                             \
          LOG_WARN("failed to min cost", KR(ret), K(cost), K(clause_cost));                                              \
        }                                                                                                                \
      }                                                                                                                  \
    } while (0);
    ITERATE_CLAUSES(must_clauses);
    ITERATE_CLAUSES(filter_clauses);
    #undef ITERATE_CLAUSES
  }
  return ret;
}

int ObDASBooleanQueryRtDef::compute_should_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  cost.reset();
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  ObBooleanSubClause<ObIDASSearchRtDef> should_clauses;
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (OB_FAIL(should(should_clauses))) {
    LOG_WARN("failed to get should clauses", KR(ret));
  } else if (should_clauses.empty()) {
  } else {
    const int64_t should_cnt = should_clauses.count();
    const int64_t least_clause_cnt = should_cnt - ctdef->min_should_match_ + 1;
    Int64LessComp cmp;
    ObBinaryHeap<int64_t, Int64LessComp> max_heap(cmp);
    for (int64_t i = 0; OB_SUCC(ret) && i < should_cnt; ++i) {
      ObDASSearchCost clause_cost;
      ObIDASSearchRtDef *clause = nullptr;
      if (OB_FAIL(should_clauses.get(i, clause))) {
        LOG_WARN("failed to get clause", KR(ret), K(i));
      } else if (OB_ISNULL(clause)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr clause", KR(ret), K(i));
      } else if (OB_FAIL(clause->get_cost(search_ctx, clause_cost))) {
        LOG_WARN("failed to get should clause cost", KR(ret), K(i));
      } else if (!clause_cost.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid should clause cost", KR(ret), K(i));
      } else {
        int64_t cur_cost = clause_cost.cost();
        if (OB_FAIL(max_heap.push(cur_cost))) {
          LOG_WARN("failed to push cost to heap", KR(ret), K(cur_cost));
        } else if (max_heap.count() > least_clause_cnt) {
          if (OB_FAIL(max_heap.pop())) {
            LOG_WARN("failed to pop from heap", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t sum_cost = 0;
      while (!max_heap.empty() && OB_SUCC(ret)) {
        sum_cost += max_heap.top();
        if (OB_FAIL(max_heap.pop())) {
          LOG_WARN("failed to pop from heap", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        cost = ObDASSearchCost(sum_cost);
      }
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObBooleanSubClause<ObIDASSearchRtDef> must_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> must_not_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> should_clauses;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  ObDASSearchCost self_cost;
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (OB_FAIL(get_cost(search_ctx, self_cost))) {
    LOG_WARN("failed to get cost", KR(ret));
  } else if (!self_cost.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid cost", KR(ret));
  } else if (OB_FAIL(ObDASSearchCost::min(lead_cost, self_cost, lead_cost))) {
    LOG_WARN("failed to get min cost", KR(ret), K(lead_cost), K(self_cost));
  } else if (OB_FAIL(must(must_clauses))) {
    LOG_WARN("failed to get must clauses", KR(ret));
  } else if (OB_FAIL(must_not(must_not_clauses))) {
    LOG_WARN("failed to get must not clauses", KR(ret));
  } else if (OB_FAIL(filter(filter_clauses))) {
    LOG_WARN("failed to get filter clauses", KR(ret));
  } else if (OB_FAIL(should(should_clauses))) {
    LOG_WARN("failed to get should clauses", KR(ret));
  } else if (should_clauses.count() == 0) {
    // full conjunction query
    ObIDASSearchOp *req_op = nullptr;
    if (OB_FAIL(req(must_clauses, filter_clauses, lead_cost, ctdef->is_top_level_scoring(), search_ctx, req_op))) {
      LOG_WARN("failed to generate required op", KR(ret));
    } else if (OB_FAIL(excl(req_op, must_not_clauses, lead_cost, search_ctx, op))) {
      LOG_WARN("failed to generate excluded op", KR(ret));
    }
  } else if (must_clauses.count() == 0 && filter_clauses.count() == 0) {
    // full disjunction query
    ObIDASSearchOp *opt_op = nullptr;
    if (OB_FAIL(opt(should_clauses, ctdef->min_should_match_, ctdef->is_scoring(), lead_cost,
                    ctdef->is_top_level_scoring(), search_ctx, opt_op))) {
      LOG_WARN("failed to generate optional op", KR(ret));
    } else if (OB_FAIL(excl(opt_op, must_not_clauses, lead_cost, search_ctx, op))) {
      LOG_WARN("failed to generate excluded op", KR(ret));
    }
  } else {
    // mixed conjunction and disjunction query
    ObIDASSearchOp *req_op = nullptr;
    ObIDASSearchOp *excl_op = nullptr;
    ObIDASSearchOp *opt_op = nullptr;

    // Push filter ops directly into BMMOp when must is empty (only filter as required) and
    // BMMOp would be created for the should clauses, to enable early pivot pruning within BMMOp.
    const bool can_push_filter_to_should =
        must_clauses.empty() &&
        must_not_clauses.empty() &&
        !filter_clauses.empty() &&
        !DISABLE_BMM_FILTER_PUSHDOWN &&
        should_clauses.count() == 1;
    bool can_push_filter = false;
    if (can_push_filter_to_should) {
      ObIDASSearchRtDef *should_rtdef = nullptr;
      const ObIDASSearchCtDef *should_ctdef = nullptr;
      if (OB_FAIL(should_clauses.get(0, should_rtdef))) {
        LOG_WARN("failed to get should clause", KR(ret));
      } else if (OB_ISNULL(should_rtdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr to should clause", KR(ret));
      } else if (OB_FAIL(should_rtdef->can_pushdown_filter_to_bmm(can_push_filter))) {
        LOG_WARN("failed to check filter pushdown for should clause", KR(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (can_push_filter) {
      ObIDASSearchOp *pushed_filter_op = nullptr;
      if (OB_FAIL(generate_filter_op(filter_clauses, lead_cost, search_ctx, pushed_filter_op))) {
        LOG_WARN("failed to generate filter op for bmm pushdown", KR(ret));
      } else if (OB_FAIL(opt(should_clauses, ctdef->min_should_match_, ctdef->is_scoring(), lead_cost,
                             ctdef->is_top_level_scoring(), search_ctx, opt_op,
                             ctdef->min_should_match_ == 0, pushed_filter_op))) {
        LOG_WARN("failed to generate opt op with pushed filter", KR(ret));
      } else {
        op = opt_op;
      }
    } else if (OB_FAIL(
            req(must_clauses, filter_clauses, lead_cost, ctdef->is_top_level_scoring(), search_ctx, req_op))) {
      LOG_WARN("failed to generate required op", KR(ret));
    } else if (OB_FAIL(excl(req_op, must_not_clauses, lead_cost, search_ctx, excl_op))) {
      LOG_WARN("failed to generate excluded op", KR(ret));
    } else if (OB_FAIL(opt(should_clauses, ctdef->min_should_match_, ctdef->is_scoring(), lead_cost,
                           ctdef->is_top_level_scoring(), search_ctx, opt_op))) {
      LOG_WARN("failed to generate optional op", KR(ret));
    } else if (ctdef->min_should_match_ > 0) {
      ObArray<ObIDASSearchOp *> required;
      if (OB_FAIL(construct_op_array(required, excl_op, opt_op))) {
        LOG_WARN("failed to construct op array", KR(ret));
      } else if (OB_FAIL(generate_conjunction_op(required, search_ctx, op))) {
        LOG_WARN("failed to generate conjunction op", KR(ret));
      }
    } else if (ctdef->min_should_match_ == 0) {
      if (OB_FAIL(generate_req_opt_op(excl_op, opt_op, search_ctx, op))) {
        LOG_WARN("failed to generate req opt op", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected min should match", KR(ret), K(ctdef->min_should_match_));
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::req(const ObBooleanSubClause<ObIDASSearchRtDef> &must,
                                const ObBooleanSubClause<ObIDASSearchRtDef> &filter,
                                const ObDASSearchCost& lead_cost,
                                const bool is_top_level,
                                ObDASSearchCtx &search_ctx,
                                ObIDASSearchOp *&op)
{
  // TODO: block max conjunction
  int ret = OB_SUCCESS;
  ObArray<ObIDASSearchRtDef *> required_clauses;
  ObArray<ObIDASSearchOp *> required_ops;
  bool filter_pushed_down = false;
  #define FILL_REQUIRED_CLAUSES(source)                                                                                  \
  do {                                                                                                                   \
    for (int i = 0; OB_SUCC(ret) && i < source.count(); i++) {                                                           \
      ObIDASSearchRtDef *clause = nullptr;                                                                               \
      if (OB_FAIL(source.get(i, clause))) {                                                                              \
        LOG_WARN("failed to get clause", KR(ret), K(i));                                                                 \
      } else if (OB_ISNULL(clause)) {                                                                                    \
        ret = OB_ERR_UNEXPECTED;                                                                                         \
        LOG_WARN("unexpected nullptr clause", KR(ret), K(i));                                                            \
      } else if (OB_FAIL(required_clauses.push_back(clause))) {                                                          \
        LOG_WARN("failed to push back clause", KR(ret), K(i));                                                           \
      }                                                                                                                  \
    }                                                                                                                    \
  } while (0);

  FILL_REQUIRED_CLAUSES(must);
  const bool can_pushdown_filter = 1 == must.count() && !filter.empty() && !DISABLE_BMM_FILTER_PUSHDOWN;
  if (OB_SUCC(ret) && can_pushdown_filter) {
    ObIDASSearchRtDef *must_clause = nullptr;
    bool can_pushdown = false;
    if (OB_FAIL(must.get(0, must_clause))) {
      LOG_WARN("failed to get must clause", KR(ret));
    } else if (OB_ISNULL(must_clause)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr must clause", KR(ret));
    } else if (OB_FAIL(must_clause->can_pushdown_filter_to_bmm(can_pushdown))) {
      LOG_WARN("failed to check filter pushdown for must clause", KR(ret));
    } else if (can_pushdown) {
      ObIDASSearchOp *filter_op = nullptr;
      if (OB_FAIL(generate_filter_op(filter, lead_cost, search_ctx, filter_op))) {
        LOG_WARN("failed to generate filter op for bmm pushdown", KR(ret));
      } else {
        must_clause->set_pushdown_filter(false, filter_op);
        filter_pushed_down = true;
      }
    }
  }

  if (OB_SUCC(ret) && !filter_pushed_down) {
    FILL_REQUIRED_CLAUSES(filter);
  }
  #undef FILL_REQUIRED_CLAUSES

  if (OB_SUCC(ret)) {
    ObDASRtDefCostComparator comparator(search_ctx);
    lib::ob_sort(required_clauses.begin(), required_clauses.end(), comparator);
    for (int i = 0; OB_SUCC(ret) && i < required_clauses.count(); i++) {
      ObIDASSearchRtDef *clause = nullptr;
      ObIDASSearchOp *required_op = nullptr;
      if (OB_FAIL(required_clauses.at(i, clause))) {
        LOG_WARN("failed to get clause", KR(ret), K(i));
      } else if (OB_ISNULL(clause)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr clause", KR(ret), K(i));
      } else if (OB_FAIL(clause->generate_op(lead_cost, search_ctx, required_op))) {
        LOG_WARN("failed to generate required op", KR(ret));
      } else if (OB_FAIL(required_ops.push_back(required_op))) {
        LOG_WARN("failed to push back required op", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (required_ops.size() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected required ops", KR(ret), K(required_ops.size()));
    } else if (1 == required_ops.size()) {
      op = required_ops.at(0);
    } else if (OB_FAIL(generate_conjunction_op(required_ops, search_ctx, op))) {
      LOG_WARN("failed to generate conjunction op", KR(ret));
    }
  }

  return ret;
}

int ObDASBooleanQueryRtDef::opt(const ObBooleanSubClause<ObIDASSearchRtDef> &should,
                                const int64_t min_should_match,
                                const bool need_score,
                                const ObDASSearchCost& lead_cost,
                                const bool is_top_level,
                                ObDASSearchCtx &search_ctx,
                                ObIDASSearchOp *&op,
                                const bool query_optional,
                                ObIDASSearchOp *filter_op)
{
  int ret = OB_SUCCESS;
  ObArray<ObIDASSearchOp *> optional_ops;
  if (OB_UNLIKELY(nullptr != filter_op && should.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected should clauses with pushdown filter op", KR(ret), K(should.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < should.count(); i++) {
    ObIDASSearchRtDef *clause = nullptr;
    ObIDASSearchOp *optional_op = nullptr;
    if (OB_FAIL(should.get(i, clause))) {
      LOG_WARN("failed to get clause", KR(ret), K(i));
    } else if (OB_ISNULL(clause)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr clause", KR(ret), K(i));
    } else if (nullptr != filter_op && FALSE_IT(clause->set_pushdown_filter(query_optional, filter_op))) {
    } else if (OB_FAIL(clause->generate_op(lead_cost, search_ctx, optional_op))) {
      LOG_WARN("failed to generate optional op", KR(ret));
    } else if (OB_FAIL(optional_ops.push_back(optional_op))) {
      LOG_WARN("failed to push back optional op", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (optional_ops.size() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected optional clauses", KR(ret), K(optional_ops.size()));
  } else if (1 == optional_ops.size()) {
    op = optional_ops.at(0);
  } else if (need_score && is_top_level) {
    if (min_should_match > 1) {
      ObDASBMWOp *bmw_op = nullptr;
      ObDASBMWOpParam bmw_op_param(optional_ops, min_should_match, allocator_);
      if (OB_FAIL(search_ctx.create_op(bmw_op_param, bmw_op))) {
        LOG_WARN("failed to create bmw op", KR(ret));
      } else {
        op = bmw_op;
      }
    } else {
      ObDASBMMOp *bmm_op = nullptr;
      ObDASBMMOpParam bmm_op_param(optional_ops, false, nullptr, allocator_);
      if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
        LOG_WARN("failed to create bmm op", KR(ret));
      } else {
        op = bmm_op;
      }
    }
  } else if (need_score) {
    ObDASDisjunctionOp *disjunction_op = nullptr;
    ObDASDisjunctionOpParam disjunction_op_param(optional_ops, min_should_match, false, lead_cost);
    if (OB_FAIL(search_ctx.create_op(disjunction_op_param, disjunction_op))) {
      LOG_WARN("failed to create disjunction op", KR(ret));
    } else {
      op = disjunction_op;
    }
  } else {
    ObDASDisjunctionFilterOp *disjunction_filter_op = nullptr;
    ObDASDisjunctionFilterOpParam disjunction_filter_op_param(optional_ops, min_should_match, lead_cost);
    if (OB_FAIL(search_ctx.create_op(disjunction_filter_op_param, disjunction_filter_op))) {
      LOG_WARN("failed to create disjunction filter op", KR(ret));
    } else {
      op = disjunction_filter_op;
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::excl(ObIDASSearchOp *main,
                                 const ObBooleanSubClause<ObIDASSearchRtDef> &prohibited,
                                 ObDASSearchCost lead_cost,
                                 ObDASSearchCtx &search_ctx,
                                 ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(main)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr main op", KR(ret));
  } else if (prohibited.empty()) {
    op = main;
  } else {
    ObIDASSearchOp *optional_op = nullptr;
    if (OB_FAIL(opt(prohibited, 1 /*min_should_match*/, false /*need_score*/, lead_cost, false /*is_top_level*/, search_ctx, optional_op))) {
      LOG_WARN("failed to generate optional op", KR(ret));
    } else if (OB_FAIL(generate_req_excl_op(main, optional_op, search_ctx, op))) {
      LOG_WARN("failed to generate req excl op", KR(ret));
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_conjunction_op(const ObArray<ObIDASSearchOp *> &required,
                                                    ObDASSearchCtx &search_ctx,
                                                    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASConjunctionOp *conjunction_op = nullptr;
  ObDASConjunctionOpParam conjunction_op_param(required);
  if (OB_FAIL(search_ctx.create_op(conjunction_op_param, conjunction_op))) {
    LOG_WARN("failed to create conjunction op", K(ret));
  } else {
    op = conjunction_op;
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_disjunction_op(const ObArray<ObIDASSearchOp *> &optional_ops,
                                                    const int64_t min_should_match,
                                                    ObDASSearchCost lead_cost,
                                                    ObDASSearchCtx &search_ctx,
                                                    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASDisjunctionOp *disjunction_op = nullptr;
  ObDASDisjunctionOpParam disjunction_op_param(optional_ops, min_should_match, false, lead_cost);
  if (OB_FAIL(search_ctx.create_op(disjunction_op_param, disjunction_op))) {
    LOG_WARN("failed to create disjunction op", K(ret));
  } else {
    op = disjunction_op;
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_req_excl_op(ObIDASSearchOp *required,
                                                 ObIDASSearchOp *excluded,
                                                 ObDASSearchCtx &search_ctx,
                                                 ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASReqExclOp *req_excl_op = nullptr;
  ObDASReqExclOpParam req_excl_op_param(required, excluded);
  if (OB_FAIL(search_ctx.create_op(req_excl_op_param, req_excl_op))) {
    LOG_WARN("failed to create req excl op", K(ret));
  } else {
    op = req_excl_op;
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_req_opt_op(ObIDASSearchOp *required,
                                                ObIDASSearchOp *optional,
                                                ObDASSearchCtx &search_ctx,
                                                ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASReqOptOp *req_opt_op = nullptr;
  ObDASReqOptOpParam req_opt_op_param(required, optional, true);
  if (OB_FAIL(search_ctx.create_op(req_opt_op_param, req_opt_op))) {
    LOG_WARN("failed to create req opt op", K(ret));
  } else {
    op = req_opt_op;
  }
  return ret;
}

int ObDASBooleanQueryCtDef::must(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const
{
  int ret = OB_SUCCESS;
  if (must_.offset_ + must_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(must_.offset_), K(must_.count_), K(children_cnt_));
  } else if (must_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>((ObIDASSearchCtDef **)(children_ + must_.offset_), must_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>();
  }
  return ret;
}

int ObDASBooleanQueryCtDef::filter(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const
{
  int ret = OB_SUCCESS;
  if (filter_.offset_ + filter_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(filter_.offset_), K(filter_.count_), K(children_cnt_));
  } else if (filter_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>((ObIDASSearchCtDef **)(children_ + filter_.offset_), filter_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>();
  }
  return ret;
}

int ObDASBooleanQueryCtDef::should(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const
{
  int ret = OB_SUCCESS;
  if (should_.offset_ + should_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(should_.offset_), K(should_.count_), K(children_cnt_));
  } else if (should_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>((ObIDASSearchCtDef **)(children_ + should_.offset_), should_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>();
  }
  return ret;
}

int ObDASBooleanQueryCtDef::must_not(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const
{
  int ret = OB_SUCCESS;
  if (must_not_.offset_ + must_not_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(must_not_.offset_), K(must_not_.count_), K(children_cnt_));
  } else if (must_not_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>((ObIDASSearchCtDef **)(children_ + must_not_.offset_), must_not_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>();
  }
  return ret;
}

int ObDASBooleanQueryRtDef::must(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const
{
  int ret = OB_SUCCESS;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (ctdef->must_.offset_ + ctdef->must_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(ctdef->must_.offset_), K(ctdef->must_.count_), K(children_cnt_));
  } else if (ctdef->must_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>((ObIDASSearchRtDef **)(children_ + ctdef->must_.offset_), ctdef->must_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>();
  }
  return ret;
}

int ObDASBooleanQueryRtDef::filter(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const
{
  int ret = OB_SUCCESS;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (ctdef->filter_.offset_ + ctdef->filter_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(ctdef->filter_.offset_), K(ctdef->filter_.count_), K(children_cnt_));
  } else if (ctdef->filter_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>((ObIDASSearchRtDef **)(children_ + ctdef->filter_.offset_), ctdef->filter_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>();
  }
  return ret;
}

int ObDASBooleanQueryRtDef::should(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const
{
  int ret = OB_SUCCESS;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (ctdef->should_.offset_ + ctdef->should_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(ctdef->should_.offset_), K(ctdef->should_.count_), K(children_cnt_));
  } else if (ctdef->should_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>((ObIDASSearchRtDef **)(children_ + ctdef->should_.offset_), ctdef->should_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>();
  }
  return ret;
}

int ObDASBooleanQueryRtDef::must_not(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const
{
  int ret = OB_SUCCESS;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (ctdef->must_not_.offset_ + ctdef->must_not_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(ctdef->must_not_.offset_), K(ctdef->must_not_.count_), K(children_cnt_));
  } else if (ctdef->must_not_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>((ObIDASSearchRtDef **)(children_ + ctdef->must_not_.offset_), ctdef->must_not_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>();
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_filter_op(
    const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&filter_op)
{
  int ret = OB_SUCCESS;
  filter_op = nullptr;
  if (filter_clauses.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty filter clauses", KR(ret));
  } else if (1 == filter_clauses.count()) {
    ObIDASSearchRtDef *clause = nullptr;
    if (OB_FAIL(filter_clauses.get(0, clause))) {
      LOG_WARN("failed to get filter clause", KR(ret));
    } else if (OB_ISNULL(clause)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr filter clause", KR(ret));
    } else if (OB_FAIL(clause->generate_op(lead_cost, search_ctx, filter_op))) {
      LOG_WARN("failed to generate filter op", KR(ret));
    }
  } else {
    ObArray<ObIDASSearchOp *> filter_ops;
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_clauses.count(); ++i) {
      ObIDASSearchRtDef *clause = nullptr;
      ObIDASSearchOp *op = nullptr;
      if (OB_FAIL(filter_clauses.get(i, clause))) {
        LOG_WARN("failed to get filter clause", KR(ret), K(i));
      } else if (OB_ISNULL(clause)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr filter clause", KR(ret), K(i));
      } else if (OB_FAIL(clause->generate_op(lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate filter op", KR(ret), K(i));
      } else if (OB_FAIL(filter_ops.push_back(op))) {
        LOG_WARN("failed to push back filter op", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_conjunction_op(filter_ops, search_ctx, filter_op))) {
        LOG_WARN("failed to generate conjunction filter op", KR(ret));
      }
    }
  }
  return ret;
}

template <typename... Args>
int ObDASBooleanQueryRtDef::construct_op_array(ObArray<ObIDASSearchOp *> &ops, ObIDASSearchOp *op, Args... rest_ops)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ops.push_back(op))) {
    LOG_WARN("failed to push back op", KR(ret));
  } else if (OB_FAIL(construct_op_array(ops, rest_ops...))) {
    LOG_WARN("failed to construct op array", KR(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase