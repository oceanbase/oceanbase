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

#ifndef DEV_SRC_SQL_DAS_SEARCH_OB_DAS_BOOLEAN_QUERY_H_
#define DEV_SRC_SQL_DAS_SEARCH_OB_DAS_BOOLEAN_QUERY_H_

#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_req_excl_op.h"
#include "sql/das/search/ob_das_req_opt_op.h"

namespace oceanbase {
namespace sql {

template<typename T>
class ObBooleanSubClause
{
public:
  ObBooleanSubClause() : clauses_(nullptr), count_(0) {}
  ObBooleanSubClause(T **clauses, int64_t count) : clauses_(clauses), count_(count) {}
  ~ObBooleanSubClause() {}
  int get(int64_t idx, T *&clause) const {
    int ret = OB_SUCCESS;
    if (idx < 0 || idx >= count_ || OB_ISNULL(clauses_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), K(idx), K(clauses_), K(count_));
    } else {
      clause = clauses_[idx];
    }
    return ret;
  }
  int64_t count() const { return count_; }
  bool empty() const { return count_ == 0; }
private:
  T **clauses_;
  int64_t count_;
};

struct ObDASBooleanQueryCtDef : ObIDASSearchCtDef {
  friend struct ObDASBooleanQueryRtDef;
  OB_UNIS_VERSION(1);

public:
  ObDASBooleanQueryCtDef(common::ObIAllocator &alloc)
      : ObIDASSearchCtDef(alloc, DAS_OP_BOOLEAN_QUERY), must_(), filter_(), should_(), must_not_(), min_should_match_(0) {}
  virtual ~ObDASBooleanQueryCtDef() {}
  int must(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const;
  int filter(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const;
  int should(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const;
  int must_not(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const;

  void set_must(int64_t offset, int64_t count) { must_ = SubClauseInfo(true, offset, count); }
  void set_filter(int64_t offset, int64_t count) { filter_ = SubClauseInfo(true, offset, count); }
  void set_should(int64_t offset, int64_t count) { should_ = SubClauseInfo(true, offset, count); }
  void set_must_not(int64_t offset, int64_t count) { must_not_ = SubClauseInfo(true, offset, count); }
  void set_min_should_match(int64_t val) { min_should_match_ = val; }

private:
  struct SubClauseInfo {
    OB_UNIS_VERSION(1);
  public:
    SubClauseInfo() : exist_(false), offset_(0), count_(0) {}
    SubClauseInfo(bool exist, int64_t offset, int64_t count) : exist_(exist), offset_(offset), count_(count) {}
    ~SubClauseInfo() {}
    TO_STRING_KV(K(exist_), K(offset_), K(count_));
    bool exist_;
    int64_t offset_;
    int64_t count_;
  };

  SubClauseInfo must_;
  SubClauseInfo filter_;
  SubClauseInfo should_;
  SubClauseInfo must_not_;
  int64_t min_should_match_;
};

struct ObDASBooleanQueryRtDef : ObIDASSearchRtDef {
  OB_UNIS_VERSION(1);

public:
  ObDASBooleanQueryRtDef(const ObDASOpType &op_type = DAS_OP_BOOLEAN_QUERY)
      : ObIDASSearchRtDef(op_type),
        allocator_(ObMemAttr(MTL_ID(), "DASBoolQuery")) {}
  virtual ~ObDASBooleanQueryRtDef() {}
  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override;
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override;

private:
  int compute_required_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost);
  int compute_should_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost);
  int excl(ObIDASSearchOp *main, const ObBooleanSubClause<ObIDASSearchRtDef> &prohibited, ObDASSearchCost lead_cost,
           ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int req(const ObBooleanSubClause<ObIDASSearchRtDef> &must, const ObBooleanSubClause<ObIDASSearchRtDef> &filter,
          const ObDASSearchCost& lead_cost, const bool is_top_level, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int opt(const ObBooleanSubClause<ObIDASSearchRtDef> &should, const int64_t min_should_match, const bool need_score,
          const ObDASSearchCost& lead_cost, const bool is_top_level, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op,
          const bool query_optional = false, ObIDASSearchOp *filter_op = nullptr);
  int generate_filter_op(const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
                         ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&filter_op);
  int generate_conjunction_op(const ObArray<ObIDASSearchOp *> &required,
                              ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int generate_disjunction_op(const ObArray<ObIDASSearchOp *> &optional_ops, const int64_t min_should_match, ObDASSearchCost lead_cost,
                              ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int generate_req_excl_op(ObIDASSearchOp *required, ObIDASSearchOp *excluded,
                           ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int generate_req_opt_op(ObIDASSearchOp *required, ObIDASSearchOp *optional,
                          ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int must(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const;
  int filter(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const;
  int should(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const;
  int must_not(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const;
  template<typename... Args>
  int construct_op_array(ObArray<ObIDASSearchOp*> &ops, ObIDASSearchOp *op, Args... rest_ops);
  inline int construct_op_array(ObArray<ObIDASSearchOp*> &ops) { return OB_SUCCESS; };
private:
  ObArenaAllocator allocator_; // op-level lifetime
};

struct ObDASRtDefCostComparator
{
public:
  ObDASRtDefCostComparator(ObDASSearchCtx &search_ctx) : search_ctx_(search_ctx) {}
  ~ObDASRtDefCostComparator() {}

  bool operator()(ObIDASSearchRtDef *lhs, ObIDASSearchRtDef *rhs)
  {
    bool less = false;
    if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
      // nullptr should be sorted to the end
      less = (lhs != nullptr);
    } else {
      ObDASSearchCost lhs_cost;
      ObDASSearchCost rhs_cost;
      int ret_lhs = lhs->get_cost(search_ctx_, lhs_cost);
      int ret_rhs = rhs->get_cost(search_ctx_, rhs_cost);

      if (OB_SUCCESS != ret_lhs || OB_SUCCESS != ret_rhs) {
        // If get_cost fails, treat as invalid cost
        less = (OB_SUCCESS == ret_lhs && lhs_cost.is_valid());
      } else if (!lhs_cost.is_valid() && !rhs_cost.is_valid()) {
        // Both invalid, maintain order
        less = false;
      } else if (!lhs_cost.is_valid()) {
        // lhs invalid, rhs valid - invalid should go after valid
        less = false;
      } else if (!rhs_cost.is_valid()) {
        // lhs valid, rhs invalid - valid should go before invalid
        less = true;
      } else {
        // Both valid, compare cost values (ascending order: smaller cost first)
        less = lhs_cost.cost() < rhs_cost.cost();
      }
    }
    return less;
  }

private:
  ObDASSearchCtx &search_ctx_;
};

} // namespace sql
} // namespace oceanbase

#endif // DEV_SRC_SQL_DAS_SEARCH_OB_DAS_BOOLEAN_QUERY_H_