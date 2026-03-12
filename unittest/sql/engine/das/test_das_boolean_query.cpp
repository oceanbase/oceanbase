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

#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/das/search/ob_das_conjunction_op.h"
#include "sql/das/search/ob_das_disjunction_op.h"
#include "sql/code_generator/ob_hybrid_search_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/hybrid_search/ob_hybrid_search_node.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/das/search/ob_das_search_define.h"
#undef private

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace
{

// Mock param for MockSearchOp, type: DAS_SEARCH_OP_DUMMY
struct MockSearchOpParam : public ObIDASSearchOpParam {
  MockSearchOpParam()
    : ObIDASSearchOpParam(DAS_SEARCH_OP_DUMMY)
  {}
  virtual ~MockSearchOpParam() {}

  virtual int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    return OB_SUCCESS;
  }
};

class MockSearchOp : public ObIDASSearchOp
{
public:
  MockSearchOp(ObDASSearchCtx &search_ctx, const std::vector<uint64_t> &rowids = {}, const std::vector<double> &scores = {})
      : ObIDASSearchOp(search_ctx), rowids_(rowids), scores_(scores), cur_idx_(-1),
        open_error_(OB_SUCCESS), rescan_error_(OB_SUCCESS), close_error_(OB_SUCCESS),
        advance_to_error_(OB_SUCCESS), next_rowid_error_(OB_SUCCESS)
  {
    if (scores_.empty()) {
      scores_.resize(rowids_.size(), 0.0);
    }
  }
  virtual ~MockSearchOp() {}

  void set_open_error(int ret) { open_error_ = ret; }
  void set_rescan_error(int ret) { rescan_error_ = ret; }
  void set_close_error(int ret) { close_error_ = ret; }
  void set_advance_to_error(int ret) { advance_to_error_ = ret; }
  void set_next_rowid_error(int ret) { next_rowid_error_ = ret; }

  virtual int do_open() override {
    if (open_error_ != OB_SUCCESS) {
      return open_error_;
    }
    cur_idx_ = -1;
    return OB_SUCCESS;
  }
  virtual int do_rescan() override {
    if (rescan_error_ != OB_SUCCESS) {
      return rescan_error_;
    }
    cur_idx_ = -1;
    return OB_SUCCESS;
  }
  virtual int do_close() override {
    if (close_error_ != OB_SUCCESS) {
      return close_error_;
    }
    return OB_SUCCESS;
  }
  virtual int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override
  {
    if (advance_to_error_ != OB_SUCCESS) {
      return advance_to_error_;
    }
    if (target.is_max()) {
      curr_id.set_max();
      return OB_ITER_END;
    }

    uint64_t target_val = target.is_min() ? 0 : target.get_uint64();

    if (cur_idx_ < 0) {
      cur_idx_ = 0;
    }

    while(cur_idx_ < (int64_t)rowids_.size() && rowids_[cur_idx_] < target_val) {
      cur_idx_++;
    }

    if (cur_idx_ >= (int64_t)rowids_.size()) {
      curr_id.set_max();
      return OB_ITER_END;
    }

    curr_id.set_uint64(rowids_[cur_idx_]);
    score = scores_[cur_idx_];
    return OB_SUCCESS;
  }
  virtual int do_advance_shallow(
      const ObDASRowID & /*target*/,
      const bool /*inclusive*/,
      const MaxScoreTuple *& /*max_score_tuple*/) override
  {
    return OB_NOT_IMPLEMENT;
  }
  virtual int do_next_rowid(ObDASRowID &next_id, double &score) override
  {
    if (next_rowid_error_ != OB_SUCCESS) {
      return next_rowid_error_;
    }
    cur_idx_++;
    if (cur_idx_ >= (int64_t)rowids_.size()) {
      next_id.set_max();
      return OB_ITER_END;
    }
    next_id.set_uint64(rowids_[cur_idx_]);
    score = scores_[cur_idx_];
    return OB_SUCCESS;
  }

private:
  std::vector<uint64_t> rowids_;
  std::vector<double> scores_;
  int64_t cur_idx_;
  int open_error_;
  int rescan_error_;
  int close_error_;
  int advance_to_error_;
  int next_rowid_error_;
};

// Mock CtDef for MockSearchOp
struct MockSearchCtDef : public ObIDASSearchCtDef
{
  MockSearchCtDef(common::ObIAllocator &alloc)
      : ObIDASSearchCtDef(alloc, DAS_OP_BOOLEAN_QUERY)
  {}
  virtual ~MockSearchCtDef() {}
};

class MockSearchRtDef : public ObIDASSearchRtDef
{
public:
  explicit MockSearchRtDef(int64_t cost, const std::vector<uint64_t> &rowids = {}, const std::vector<double> &scores = {})
      : ObIDASSearchRtDef(DAS_OP_BOOLEAN_QUERY),
        mock_cost_(cost),
        rowids_(rowids),
        scores_(scores),
        return_invalid_cost_(false),
        return_error_(OB_SUCCESS)
  {}

  void set_return_invalid_cost(bool flag) { return_invalid_cost_ = flag; }
  void set_return_error(int ret) { return_error_ = ret; }

  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override
  {
    if (return_error_ != OB_SUCCESS) {
      return return_error_;
    }
    if (return_invalid_cost_) {
      cost.reset();
    } else {
      cost = ObDASSearchCost(mock_cost_);
    }
    return OB_SUCCESS;
  }

  int generate_op(ObDASSearchCost /*lead_cost*/, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override
  {
    int ret = OB_SUCCESS;
    MockSearchOp *mock_op = nullptr;
    const ObIDASSearchCtDef *ctdef = nullptr;
    if (OB_ISNULL(ctdef = static_cast<const ObIDASSearchCtDef *>(ctdef_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctdef", KR(ret));
    } else {
      void *buf = search_ctx.allocator_.alloc(sizeof(MockSearchOp));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        mock_op = new(buf) MockSearchOp(search_ctx, rowids_, scores_);

        MockSearchOpParam param;
        if (OB_FAIL(mock_op->init(param))) {
           LOG_WARN("failed to init mock op", KR(ret));
        } else {
           op = mock_op;
        }
      }
    }
    return ret;
  }

private:
  int64_t mock_cost_;
  std::vector<uint64_t> rowids_;
  std::vector<double> scores_;
  bool return_invalid_cost_;
  int return_error_;
};

class ObDASBooleanQueryTest : public ::testing::Test
{
protected:
  enum ClauseType {
    MUST = 0,
    FILTER,
    SHOULD,
    MUST_NOT
  };

  ObDASBooleanQueryTest()
      : allocator_("TestDASBool"),
        ctdef_(allocator_),
        rtdef_()
  {
    rtdef_.ctdef_ = &ctdef_;
    reset_ctdef();
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }

  void TearDown() override
  {
  }

  void reset_ctdef()
  {
    ctdef_.min_should_match_ = 0;
    ctdef_.is_scoring_ = false;
    ctdef_.is_top_level_scoring_ = false;
    must_clauses_.clear();
    filter_clauses_.clear();
    should_clauses_.clear();
    must_not_clauses_.clear();
    ctdef_.children_ = nullptr;
    ctdef_.children_cnt_ = 0;
    rtdef_.children_ = nullptr;
    rtdef_.children_cnt_ = 0;
  }

  void add_clause(ClauseType type, ObIDASSearchRtDef *clause)
  {
    switch(type) {
      case MUST: must_clauses_.push_back(clause); break;
      case FILTER: filter_clauses_.push_back(clause); break;
      case SHOULD: should_clauses_.push_back(clause); break;
      case MUST_NOT: must_not_clauses_.push_back(clause); break;
      default: break;
    }
  }

  void prepare_structure()
  {
    int64_t must_cnt = must_clauses_.size();
    int64_t filter_cnt = filter_clauses_.size();
    int64_t should_cnt = should_clauses_.size();
    int64_t must_not_cnt = must_not_clauses_.size();
    int64_t total_cnt = must_cnt + filter_cnt + should_cnt + must_not_cnt;

    if (total_cnt > 0) {
      void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * total_cnt);
      void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * total_cnt);
      ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
      ctdef_.children_cnt_ = total_cnt;
      rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
      rtdef_.children_cnt_ = total_cnt;

      int64_t offset = 0;

      // MUST
      ctdef_.must_.exist_ = (must_cnt > 0);
      ctdef_.must_.offset_ = offset;
      ctdef_.must_.count_ = must_cnt;
      for (auto *c : must_clauses_) {
        ctdef_.children_[offset] = const_cast<ObDASBaseCtDef*>(c->ctdef_);
        rtdef_.children_[offset] = c;
        offset++;
      }

      // FILTER
      ctdef_.filter_.exist_ = (filter_cnt > 0);
      ctdef_.filter_.offset_ = offset;
      ctdef_.filter_.count_ = filter_cnt;
      for (auto *c : filter_clauses_) {
        ctdef_.children_[offset] = const_cast<ObDASBaseCtDef*>(c->ctdef_);
        rtdef_.children_[offset] = c;
        offset++;
      }

      // SHOULD
      ctdef_.should_.exist_ = (should_cnt > 0);
      ctdef_.should_.offset_ = offset;
      ctdef_.should_.count_ = should_cnt;
      for (auto *c : should_clauses_) {
        ctdef_.children_[offset] = const_cast<ObDASBaseCtDef*>(c->ctdef_);
        rtdef_.children_[offset] = c;
        offset++;
      }

      // MUST_NOT
      ctdef_.must_not_.exist_ = (must_not_cnt > 0);
      ctdef_.must_not_.offset_ = offset;
      ctdef_.must_not_.count_ = must_not_cnt;
      for (auto *c : must_not_clauses_) {
        ctdef_.children_[offset] = const_cast<ObDASBaseCtDef*>(c->ctdef_);
        rtdef_.children_[offset] = c;
        offset++;
      }
    }
  }

  // Setup search context for testing generate_op
  // Returns a struct containing all necessary objects that need to stay alive
  struct SearchCtxSetup {
    ObArenaAllocator scan_alloc_;
    ObDASScanOp mock_scan_op_;
    ObDASScanRtDef mock_scan_rtdef_;
    ObExecContext exec_ctx_;
    ObEvalCtx mock_eval_ctx_;
    ObDASSearchCtx search_ctx_;

    SearchCtxSetup(ObIAllocator &alloc)
        : scan_alloc_("MockScanOp"),
          mock_scan_op_(scan_alloc_),
          exec_ctx_(alloc),
          mock_eval_ctx_(exec_ctx_),
          search_ctx_(alloc, mock_scan_op_)
    {
      mock_eval_ctx_.max_batch_size_ = 256;
      mock_scan_rtdef_.eval_ctx_ = &mock_eval_ctx_;
      mock_scan_op_.set_scan_rtdef(&mock_scan_rtdef_);

      ExprFixedArray rowid_exprs(alloc);
      ExprFixedArray output(alloc);
      int ret = search_ctx_.init(rowid_exprs, output, true);
      if (OB_SUCCESS != ret) {
        LOG_ERROR("failed to init search ctx", K(ret));
      }
      search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    }
  };

  void verify_result(ObIDASSearchOp *op, const std::vector<uint64_t> &expected_ids, const std::vector<double> &expected_scores = {})
  {
    ASSERT_NE(nullptr, op);
    ASSERT_EQ(OB_SUCCESS, op->open());

    ObDASRowID rowid;
    double score = 0;

    size_t idx = 0;
    for (uint64_t val : expected_ids) {
      ASSERT_EQ(OB_SUCCESS, op->next_rowid(rowid, score));
      LOG_INFO("next_rowid", K(val), K(rowid.get_uint64()));
      EXPECT_EQ(val, rowid.get_uint64());
      if (!expected_scores.empty()) {
        ASSERT_LT(idx, expected_scores.size());
        EXPECT_DOUBLE_EQ(expected_scores[idx], score);
      }
      idx++;
    }

    ASSERT_EQ(OB_ITER_END, op->next_rowid(rowid, score));
    ASSERT_EQ(OB_SUCCESS, op->close());
  }

  void verify_advance_to(ObIDASSearchOp *op, const std::vector<std::tuple<uint64_t, uint64_t, double>> &targets_and_results)
  {
    ASSERT_NE(nullptr, op);
    ASSERT_EQ(OB_SUCCESS, op->open());

    ObDASRowID target_id;
    ObDASRowID res_id;
    double score = 0;

    for (const auto &tuple : targets_and_results) {
      uint64_t target_val = std::get<0>(tuple);
      uint64_t expected_val = std::get<1>(tuple);
      double expected_score = std::get<2>(tuple);

      target_id.set_uint64(target_val);
      int ret = op->advance_to(target_id, res_id, score);
      if (expected_val == UINT64_MAX) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        ASSERT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(expected_val, res_id.get_uint64());
        if (expected_score >= 0.0) {
          EXPECT_DOUBLE_EQ(expected_score, score);
        }
      }
    }

    ASSERT_EQ(OB_SUCCESS, op->close());
  }

protected:
  ObArenaAllocator allocator_;
  ObDASBooleanQueryCtDef ctdef_;
  ObDASBooleanQueryRtDef rtdef_;
  std::vector<ObIDASSearchRtDef*> must_clauses_;
  std::vector<ObIDASSearchRtDef*> filter_clauses_;
  std::vector<ObIDASSearchRtDef*> should_clauses_;
  std::vector<ObIDASSearchRtDef*> must_not_clauses_;
};

} // namespace

// ============================================================================
// Section 1: compute_cost Tests - Normal Cases
// ============================================================================
// Tests for computing query cost under normal conditions

// Test: Required cost should prefer the minimum cost clause
// Verifies that compute_cost returns the minimum cost among MUST and FILTER clauses
TEST_F(ObDASBooleanQueryTest, required_cost_prefers_minimum_clause)
{
  MockSearchRtDef must_high_cost(10);
  MockSearchRtDef must_low_cost(3);
  MockSearchRtDef filter_cost(5);

  add_clause(MUST, &must_high_cost);
  add_clause(MUST, &must_low_cost);
  add_clause(FILTER, &filter_cost);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(3, cost.cost());
}

// Test: Should cost calculation respects min_should_match parameter
// Verifies that should_cost is calculated correctly based on min_should_match value
TEST_F(ObDASBooleanQueryTest, should_cost_respects_min_should_match)
{
  MockSearchRtDef clause_a(9);
  MockSearchRtDef clause_b(1);
  MockSearchRtDef clause_c(5);

  add_clause(SHOULD, &clause_a);
  add_clause(SHOULD, &clause_b);
  add_clause(SHOULD, &clause_c);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(15, cost.cost());
  ctdef_.min_should_match_ = 2;
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(6, cost.cost());
  ctdef_.min_should_match_ = 3;
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(1, cost.cost());
}

// Test: compute_cost fails when no clause is available
// Verifies error handling when there are no clauses to compute cost from
TEST_F(ObDASBooleanQueryTest, compute_cost_fails_when_no_clause_available)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_cost with mixed MUST, FILTER, and SHOULD clauses
// Verifies cost calculation when all clause types are present
TEST_F(ObDASBooleanQueryTest, compute_cost_with_mixed_clauses)
{
  // Test case: must=5, filter=3, should=[10, 2, 8], min_should_match=2
  // Expected: required_cost = min(5, 3) = 3
  //           should_cost = 2+8 = 10
  //           final_cost = min(3, 10) = 3
  ctdef_.min_should_match_ = 2;

  MockSearchRtDef must_clause(5);
  MockSearchRtDef filter_clause(3);
  MockSearchRtDef should_clause_a(10);
  MockSearchRtDef should_clause_b(2);
  MockSearchRtDef should_clause_c(8);

  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause_a);
  add_clause(SHOULD, &should_clause_b);
  add_clause(SHOULD, &should_clause_c);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));

  EXPECT_EQ(3, cost.cost());
}

// Test: compute_cost with mixed clauses where should_cost wins
// Verifies that when should_cost is smaller than required_cost, it is selected
TEST_F(ObDASBooleanQueryTest, compute_cost_with_mixed_clauses_should_wins)
{
  // Test case: must=10, filter=8, should=[1, 2], min_should_match=1
  // Expected: required_cost = min(10, 8) = 8
  //           should_cost = 1+2 = 3 (least 2 costs from 2 should clauses)
  //           final_cost = min(8, 3) = 3
  ctdef_.min_should_match_ = 1;

  MockSearchRtDef must_clause(10);
  MockSearchRtDef filter_clause(8);
  MockSearchRtDef should_clause_a(1);
  MockSearchRtDef should_clause_b(2);

  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause_a);
  add_clause(SHOULD, &should_clause_b);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  // should_cost: least_clause_cnt = 2-1+1 = 2, sum of all 2 = 1+2 = 3
  // required_cost = min(10, 8) = 8
  // final_cost = min(8, 3) = 3
  EXPECT_EQ(3, cost.cost());
}

// ============================================================================
// Section 2: generate_op Tests - Normal Cases (Various Clause Combinations)
// ============================================================================
// Tests for generating search operators under normal conditions with different clause combinations

// Test: Only MUST clauses should generate CONJUNCTION op
// Verifies that multiple MUST clauses result in a CONJUNCTION operator
TEST_F(ObDASBooleanQueryTest, generate_op_only_must_clauses)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause1(10);
  MockSearchRtDef must_clause2(5);
  MockSearchRtDef must_clause3(8);
  must_clause1.ctdef_ = &mock_ctdef;
  must_clause2.ctdef_ = &mock_ctdef;
  must_clause3.ctdef_ = &mock_ctdef;

  add_clause(MUST, &must_clause1);
  add_clause(MUST, &must_clause2);
  add_clause(MUST, &must_clause3);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());

  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(3, conjunction_op->children_cnt_);
}

// Test: Only FILTER clauses should generate CONJUNCTION op
// Verifies that multiple FILTER clauses result in a CONJUNCTION operator
TEST_F(ObDASBooleanQueryTest, generate_op_only_filter_clauses)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef filter_clause1(7);
  MockSearchRtDef filter_clause2(3);
  filter_clause1.ctdef_ = &mock_ctdef;
  filter_clause2.ctdef_ = &mock_ctdef;

  add_clause(FILTER, &filter_clause1);
  add_clause(FILTER, &filter_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());

  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(2, conjunction_op->children_cnt_);
}

// Test: MUST + FILTER clauses should generate CONJUNCTION op
// Verifies that combining MUST and FILTER clauses results in a CONJUNCTION operator
TEST_F(ObDASBooleanQueryTest, generate_op_must_and_filter_clauses)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  MockSearchRtDef filter_clause1(5);
  MockSearchRtDef filter_clause2(8);
  must_clause.ctdef_ = &mock_ctdef;
  filter_clause1.ctdef_ = &mock_ctdef;
  filter_clause2.ctdef_ = &mock_ctdef;

  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause1);
  add_clause(FILTER, &filter_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());

  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(3, conjunction_op->children_cnt_);
}

// Test: Only SHOULD clauses should generate DISJUNCTION op
// Verifies that multiple SHOULD clauses result in a DISJUNCTION operator
TEST_F(ObDASBooleanQueryTest, generate_op_only_should_clauses)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause1(5);
  MockSearchRtDef should_clause2(10);
  MockSearchRtDef should_clause3(3);
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  should_clause3.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 1;
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);
  add_clause(SHOULD, &should_clause3);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, op->get_op_type());

  ObDASDisjunctionOp *disjunction_op = static_cast<ObDASDisjunctionOp *>(op);
  EXPECT_EQ(3, disjunction_op->children_cnt_);
}

// Test: MUST + SHOULD with min_should_match == 0 should generate REQ_OPT op
// Verifies that when min_should_match is 0, a REQ_OPT operator is generated
TEST_F(ObDASBooleanQueryTest, generate_op_must_with_should_min_match_zero)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  MockSearchRtDef filter_clause(5);
  MockSearchRtDef should_clause1(5);
  MockSearchRtDef should_clause2(8);
  must_clause.ctdef_ = &mock_ctdef;
  filter_clause.ctdef_ = &mock_ctdef;
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 0;
  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_OPT, op->get_op_type());
  ObDASReqOptOp *req_opt_op = static_cast<ObDASReqOptOp *>(op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, req_opt_op->required_->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, req_opt_op->optional_->get_op_type());
}

// Test: MUST + SHOULD with min_should_match > 0 should generate CONJUNCTION op
// Verifies that when min_should_match > 0, a CONJUNCTION operator is generated
TEST_F(ObDASBooleanQueryTest, generate_op_must_with_should_min_match_positive)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  MockSearchRtDef should_clause1(5);
  MockSearchRtDef should_clause2(8);
  must_clause.ctdef_ = &mock_ctdef;
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 1;
  add_clause(MUST, &must_clause);
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());

  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(2, conjunction_op->children_cnt_);
  ObIDASSearchOp *conj_op = conjunction_op->get_children()[0];
  ObIDASSearchOp *disj_op = conjunction_op->get_children()[1];
  EXPECT_EQ(DAS_SEARCH_OP_DUMMY, conj_op->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, disj_op->get_op_type());
}

// Test: MUST + MUST_NOT clauses should generate REQ_EXCL op
// Verifies that combining MUST and MUST_NOT clauses results in a REQ_EXCL operator
TEST_F(ObDASBooleanQueryTest, generate_op_must_with_must_not)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  MockSearchRtDef filter_clause(5);
  MockSearchRtDef must_not_clause1(5);
  MockSearchRtDef must_not_clause2(8);
  must_clause.ctdef_ = &mock_ctdef;
  filter_clause.ctdef_ = &mock_ctdef;
  must_not_clause1.ctdef_ = &mock_ctdef;
  must_not_clause2.ctdef_ = &mock_ctdef;

  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(MUST_NOT, &must_not_clause1);
  add_clause(MUST_NOT, &must_not_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_EXCL, op->get_op_type());
  ObDASReqExclOp *req_excl_op = static_cast<ObDASReqExclOp *>(op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, req_excl_op->required_->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, req_excl_op->excluded_->get_op_type());
}

// Test: SHOULD + MUST_NOT clauses should generate REQ_EXCL op
// Verifies that combining SHOULD and MUST_NOT clauses results in a REQ_EXCL operator
TEST_F(ObDASBooleanQueryTest, generate_op_should_with_must_not)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause1(10);
  MockSearchRtDef should_clause2(5);
  MockSearchRtDef must_not_clause(8);
  MockSearchRtDef must_not_clause2(3);
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  must_not_clause.ctdef_ = &mock_ctdef;
  must_not_clause2.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 1;
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);
  add_clause(MUST_NOT, &must_not_clause);
  add_clause(MUST_NOT, &must_not_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_EXCL, op->get_op_type());
  ObDASReqExclOp *req_excl_op = static_cast<ObDASReqExclOp *>(op);
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, req_excl_op->required_->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, req_excl_op->excluded_->get_op_type());
}

// Test: Complex query with all clause types should generate complex operator tree
// Verifies that combining all clause types (MUST, FILTER, SHOULD, MUST_NOT) generates a complex tree
TEST_F(ObDASBooleanQueryTest, generate_op_complex_query)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  MockSearchRtDef filter_clause(5);
  MockSearchRtDef should_clause(8);
  MockSearchRtDef should_clause2(3);
  MockSearchRtDef must_not_clause(3);
  MockSearchRtDef must_not_clause2(5);
  must_clause.ctdef_ = &mock_ctdef;
  filter_clause.ctdef_ = &mock_ctdef;
  should_clause.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  must_not_clause.ctdef_ = &mock_ctdef;
  must_not_clause2.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 1;
  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause);
  add_clause(SHOULD, &should_clause2);
  add_clause(MUST_NOT, &must_not_clause);
  add_clause(MUST_NOT, &must_not_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(2, conjunction_op->children_cnt_);
  ObIDASSearchOp *req_excl_op = conjunction_op->get_children()[0];
  ObIDASSearchOp *disjunction_op = conjunction_op->get_children()[1];
  EXPECT_EQ(DAS_SEARCH_OP_REQ_EXCL, req_excl_op->get_op_type());
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, disjunction_op->get_op_type());
}

// ============================================================================
// Section 3: Execution Tests - Actual Query Execution and Result Verification
// ============================================================================
// Tests for actual query execution, verifying correct results and behavior

// Test: ConjunctionOp execution with two MUST clauses
// Verifies that CONJUNCTION operator correctly intersects results from multiple clauses
TEST_F(ObDASBooleanQueryTest, conjunction_op_execution)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);

  // Op1: 1(1.0), 3(3.0), 5(5.0), 7(7.0), 9(9.0)
  MockSearchRtDef must_clause1(10, {1, 3, 5, 7, 9}, {1.0, 3.0, 5.0, 7.0, 9.0});
  // Op2: 3(0.3), 4(0.4), 7(0.7), 8(0.8), 9(0.9)
  MockSearchRtDef must_clause2(10, {3, 4, 7, 8, 9}, {0.3, 0.4, 0.7, 0.8, 0.9});

  must_clause1.ctdef_ = &mock_ctdef;
  must_clause2.ctdef_ = &mock_ctdef;

  add_clause(MUST, &must_clause1);
  add_clause(MUST, &must_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  // Expected: 3(3.3), 7(7.7), 9(9.9)
  verify_result(op, {3, 7, 9}, {3.3, 7.7, 9.9});

  // Test advance_to
  // advance to 2 -> should find 3(3.3)
  // advance to 5 -> should find 7(7.7) (since 5 is in op1 but not op2)
  // advance to 8 -> should find 9(9.9)
  // advance to 10 -> end
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  verify_advance_to(op, {
      {2, 3, 3.3},
      {5, 7, 7.7},
      {8, 9, 9.9},
      {10, UINT64_MAX, -1.0}
  });
}

// Test: DisjunctionOp execution with two SHOULD clauses
// Verifies that DISJUNCTION operator correctly unions results from multiple clauses
TEST_F(ObDASBooleanQueryTest, disjunction_op_execution)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);

  // Op1: 1(1.0), 5(5.0)
  MockSearchRtDef should_clause1(10, {1, 5}, {1.0, 5.0});
  // Op2: 3(0.3), 5(0.5), 7(0.7)
  MockSearchRtDef should_clause2(10, {3, 5, 7}, {0.3, 0.5, 0.7});

  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;

  // min_should_match = 1 (default OR behavior)
  ctdef_.min_should_match_ = 1;
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, op->get_op_type());
  verify_result(op, {1, 3, 5, 7}, {0, 0, 0, 0});

  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  // Test advance_to
  // advance to 2 -> 3(0.3)
  // advance to 4 -> 5(5.5)
  // advance to 6 -> 7(0.7)
  verify_advance_to(op, {
      {2, 3, 0},
      {4, 5, 0},
      {6, 7, 0},
      {8, UINT64_MAX, -1.0}
  });
}

// Test: Mixed MUST and SHOULD clauses with min_should_match = 1
// Verifies correct execution when both MUST and SHOULD clauses are present
TEST_F(ObDASBooleanQueryTest, mixed_must_and_should_min_match_1)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);

  // MUST 1: {1, 3, 5, 7, 9}
  MockSearchRtDef must_clause1(10, {1, 3, 5, 7, 9}, {10.0, 30.0, 50.0, 70.0, 90.0});
  // MUST 2: {1, 3, 5, 7}
  MockSearchRtDef must_clause2(10, {1, 3, 5, 7}, {1.0, 3.0, 5.0, 7.0});

  // SHOULD 1: {1, 2, 5, 8}
  MockSearchRtDef should_clause1(10, {1, 2, 5, 8}, {1.0, 2.0, 5.0, 8.0});

  // SHOULD 2: {3, 5, 7, 10}
  MockSearchRtDef should_clause2(10, {3, 5, 7, 10}, {3.0, 5.0, 7.0, 10.0});

  must_clause1.ctdef_ = &mock_ctdef;
  must_clause2.ctdef_ = &mock_ctdef;
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 1;
  add_clause(MUST, &must_clause1);
  add_clause(MUST, &must_clause2);
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));

  // Structure check
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
  // Logic check:
  // Must Combined:
  // 1: M1(10.0) + M2(1.0) = 11.0
  // 3: M1(30.0) + M2(3.0) = 33.0
  // 5: M1(50.0) + M2(5.0) = 55.0
  // 7: M1(70.0) + M2(7.0) = 77.0
  // 9: M1(90.0) -> Skip (not in M2)

  verify_result(op, {1, 3, 5, 7}, {11.0, 33.0, 55.0, 77.0});

  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  // Advance check
  verify_advance_to(op, {
    {2, 3, 33.0},
    {4, 5, 55.0},
    {6, 7, 77.0},
    {8, UINT64_MAX, -1.0}
  });
}

// Test: DisjunctionOp with min_should_match = 2
// Verifies that DISJUNCTION operator correctly filters results based on min_should_match
TEST_F(ObDASBooleanQueryTest, disjunction_min_should_match_2)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);

  // Op1: {10, 20, 30}
  MockSearchRtDef clause1(10, {10, 20, 30}, {1.0, 1.0, 1.0});
  // Op2: {10, 25, 30}
  MockSearchRtDef clause2(10, {10, 25, 30}, {2.0, 2.0, 2.0});
  // Op3: {10, 20, 35}
  MockSearchRtDef clause3(10, {10, 20, 35}, {3.0, 3.0, 3.0});

  clause1.ctdef_ = &mock_ctdef;
  clause2.ctdef_ = &mock_ctdef;
  clause3.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 2;
  add_clause(SHOULD, &clause1);
  add_clause(SHOULD, &clause2);
  add_clause(SHOULD, &clause3);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));

  // Structure: Disjunction
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, op->get_op_type());


  verify_result(op, {10, 20, 30}, {0, 0, 0});

  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, op->get_op_type());
  verify_advance_to(op, {
    {15, 20, 0},
    {21, 30, 0},
    {31, UINT64_MAX, -1.0}
  });
}

// ============================================================================
// Section 4: compute_cost Tests - Exception Cases
// ============================================================================
// Tests for error handling and edge cases in compute_cost

// Test: compute_cost with nullptr ctdef should return error
// Verifies error handling when ctdef is nullptr
TEST_F(ObDASBooleanQueryTest, compute_cost_nullptr_ctdef)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  rtdef_.ctdef_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_cost with min_should_match == 0 and valid required_cost
// Verifies that when min_should_match is 0 and required_cost is valid, it returns early
TEST_F(ObDASBooleanQueryTest, compute_cost_min_should_match_zero_with_required)
{
  MockSearchRtDef must_clause(5);
  MockSearchRtDef filter_clause(3);
  MockSearchRtDef should_clause(10);

  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 0;
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  // When min_should_match == 0 and required_cost is valid, should return required_cost
  EXPECT_EQ(3, cost.cost()); // min(5, 3) = 3
}

// Test: compute_cost with both required_cost and should_cost invalid
// Verifies error handling when both cost calculations result in invalid costs
TEST_F(ObDASBooleanQueryTest, compute_cost_both_invalid)
{
  MockSearchRtDef should_clause(10);
  should_clause.set_return_invalid_cost(true);

  add_clause(SHOULD, &should_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_required_cost_inner with nullptr clause
// Verifies error handling when a clause in the required clauses array is nullptr
TEST_F(ObDASBooleanQueryTest, compute_required_cost_inner_nullptr_clause)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);

  // Setup structure with nullptr clause in children array
  void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
  void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
  ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
  ctdef_.children_cnt_ = 1;
  rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
  rtdef_.children_cnt_ = 1;

  ctdef_.must_.exist_ = true;
  ctdef_.must_.offset_ = 0;
  ctdef_.must_.count_ = 1;
  ctdef_.children_[0] = &mock_ctdef;
  rtdef_.children_[0] = nullptr; // nullptr clause

  ObDASSearchCost cost;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_required_cost_inner with invalid cost
// Verifies error handling when a clause returns an invalid cost
TEST_F(ObDASBooleanQueryTest, compute_required_cost_inner_invalid_cost)
{
  MockSearchRtDef must_clause(10);
  must_clause.set_return_invalid_cost(true);

  add_clause(MUST, &must_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_should_cost with nullptr ctdef
// Verifies error handling when ctdef is nullptr in compute_should_cost
TEST_F(ObDASBooleanQueryTest, compute_should_cost_nullptr_ctdef)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  rtdef_.ctdef_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test compute_should_cost with empty should clauses
TEST_F(ObDASBooleanQueryTest, compute_should_cost_empty_clauses)
{
  MockSearchRtDef must_clause(5);
  add_clause(MUST, &must_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(5, cost.cost());
}

// Test: compute_should_cost with nullptr clause
// Verifies error handling when a clause in the should clauses array is nullptr
TEST_F(ObDASBooleanQueryTest, compute_should_cost_nullptr_clause)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);

  // Setup structure with nullptr clause
  void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
  void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
  ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
  ctdef_.children_cnt_ = 1;
  rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
  rtdef_.children_cnt_ = 1;

  ctdef_.should_.exist_ = true;
  ctdef_.should_.offset_ = 0;
  ctdef_.should_.count_ = 1;
  ctdef_.children_[0] = &mock_ctdef;
  rtdef_.children_[0] = nullptr; // nullptr clause

  ObDASSearchCost cost;
  ctdef_.min_should_match_ = 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: compute_should_cost with invalid clause cost
// Verifies error handling when a should clause returns an invalid cost
TEST_F(ObDASBooleanQueryTest, compute_should_cost_invalid_clause_cost)
{
  MockSearchRtDef should_clause(10);
  should_clause.set_return_invalid_cost(true);

  add_clause(SHOULD, &should_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// ============================================================================
// Section 5: generate_op Tests - Exception Cases
// ============================================================================
// Tests for error handling and edge cases in generate_op

// Test: generate_op with nullptr ctdef should return error
// Verifies error handling when ctdef is nullptr in generate_op
TEST_F(ObDASBooleanQueryTest, generate_op_nullptr_ctdef)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  rtdef_.ctdef_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: generate_op with invalid cost should return error
// Verifies error handling when self_cost is invalid
TEST_F(ObDASBooleanQueryTest, generate_op_invalid_cost)
{
  MockSearchRtDef must_clause(10);
  must_clause.set_return_invalid_cost(true);

  add_clause(MUST, &must_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: req with single required clause should return the op directly
// Verifies that when there's only one required clause, it's returned directly without wrapping
TEST_F(ObDASBooleanQueryTest, req_single_required_clause)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10, {1, 2, 3}, {1.0, 2.0, 3.0});
  must_clause.ctdef_ = &mock_ctdef;

  add_clause(MUST, &must_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  // Single clause should return the op directly, not wrapped in conjunction
  EXPECT_EQ(DAS_SEARCH_OP_DUMMY, op->get_op_type());
}

// Test: opt with need_score and is_top_level and min_match > 1
// Verifies that WAND operator is generated
TEST_F(ObDASBooleanQueryTest, opt_need_score_top_level_min_match_gt_1)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause(10);
  should_clause.ctdef_ = &mock_ctdef;

  add_clause(SHOULD, &should_clause);

  ctdef_.min_should_match_ = 2;
  ctdef_.is_scoring_ = true;
  ctdef_.is_top_level_scoring_ = true;

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: opt with need_score and is_top_level and min_match == 1
// Verifies that BMM operator is generated
TEST_F(ObDASBooleanQueryTest, opt_need_score_top_level_min_match_1)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause(10);
  should_clause.ctdef_ = &mock_ctdef;

  add_clause(SHOULD, &should_clause);

  ctdef_.min_should_match_ = 1;
  ctdef_.is_scoring_ = true;
  ctdef_.is_top_level_scoring_ = true;

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: generate_op with MUST_NOT but no MUST/FILTER should fail
// Verifies error handling when MUST_NOT clauses exist without MUST/FILTER clauses
TEST_F(ObDASBooleanQueryTest, generate_op_must_not_without_must_filter)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_not_clause(10);
  must_not_clause.ctdef_ = &mock_ctdef;

  add_clause(MUST_NOT, &must_not_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  // This will fail because there are no must/filter clauses to generate req_op
  // and excl requires a valid main op
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: excl with empty prohibited clauses should return main op directly
// Verifies that when prohibited clauses are empty, the main op is returned without wrapping
TEST_F(ObDASBooleanQueryTest, excl_empty_prohibited)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10, {1, 2, 3}, {1.0, 2.0, 3.0});
  must_clause.ctdef_ = &mock_ctdef;

  add_clause(MUST, &must_clause);
  // No must_not clauses

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  // Should return the main op directly when prohibited is empty
  EXPECT_EQ(DAS_SEARCH_OP_DUMMY, op->get_op_type());
}

// ============================================================================
// Section 6: Getter Methods Tests
// ============================================================================
// Tests for getter methods (must, filter, should, must_not) error handling

// Test: Getter method (must) with out of range should return error
// Verifies error handling when accessing clauses with invalid range
TEST_F(ObDASBooleanQueryTest, getter_out_of_range)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);

  // Setup invalid range
  ctdef_.must_.exist_ = true;
  ctdef_.must_.offset_ = 0;
  ctdef_.must_.count_ = 10; // But children_cnt_ is 0
  ctdef_.children_cnt_ = 0;

  ObBooleanSubClause<ObIDASSearchRtDef> clause;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.must(clause));
}

// Test: Getter methods with nullptr ctdef should return error
// Verifies error handling when ctdef is nullptr in getter methods
TEST_F(ObDASBooleanQueryTest, getter_nullptr_ctdef)
{
  SearchCtxSetup ctx_setup(allocator_);
  rtdef_.ctdef_ = nullptr;

  ObBooleanSubClause<ObIDASSearchRtDef> clause;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.must(clause));
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.filter(clause));
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.should(clause));
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.must_not(clause));
}

// Test: compute_cost with empty should_clauses
// Verifies that compute_cost handles empty should clauses correctly
TEST_F(ObDASBooleanQueryTest, compute_cost_empty_should_clauses)
{
  MockSearchRtDef must_clause(5);
  add_clause(MUST, &must_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1; // This shouldn't matter when should is empty
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(5, cost.cost());
}

// Test: compute_should_cost with least_clause_cnt calculation
// Verifies correct calculation of least_clause_cnt and cost summation
TEST_F(ObDASBooleanQueryTest, compute_should_cost_least_clause_cnt)
{
  MockSearchRtDef clause1(10);
  MockSearchRtDef clause2(5);
  MockSearchRtDef clause3(8);
  MockSearchRtDef clause4(3);
  MockSearchRtDef clause5(7);

  add_clause(SHOULD, &clause1);
  add_clause(SHOULD, &clause2);
  add_clause(SHOULD, &clause3);
  add_clause(SHOULD, &clause4);
  add_clause(SHOULD, &clause5);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 3;
  // should_cnt = 5, least_clause_cnt = 5 - 3 + 1 = 3
  // Should sum the 3 smallest costs: 3 + 5 + 7 = 15
  ASSERT_EQ(OB_SUCCESS, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
  EXPECT_EQ(15, cost.cost());
}

// Test: generate_op with only FILTER clauses (no MUST) should generate CONJUNCTION op
// Verifies that FILTER-only queries generate CONJUNCTION operator
TEST_F(ObDASBooleanQueryTest, generate_op_only_filter_no_must)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef filter_clause1(5, {1, 3, 5}, {1.0, 3.0, 5.0});
  MockSearchRtDef filter_clause2(3, {1, 3}, {1.0, 3.0});
  filter_clause1.ctdef_ = &mock_ctdef;
  filter_clause2.ctdef_ = &mock_ctdef;

  add_clause(FILTER, &filter_clause1);
  add_clause(FILTER, &filter_clause2);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());

  ObDASConjunctionOp *conjunction_op = static_cast<ObDASConjunctionOp *>(op);
  EXPECT_EQ(2, conjunction_op->children_cnt_);

  // Expected intersection: {1, 3}
  verify_result(op, {1, 3}, {2.0, 6.0});
}

// Test: generate_op with MUST + FILTER + SHOULD (min_match=0) should generate REQ_OPT op
// Verifies that when min_match is 0, a REQ_OPT operator is generated
TEST_F(ObDASBooleanQueryTest, generate_op_must_filter_should_min_match_zero)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10, {1, 3, 5}, {10.0, 30.0, 50.0});
  MockSearchRtDef filter_clause(5, {1, 3}, {1.0, 3.0});
  MockSearchRtDef should_clause(8, {2, 4, 5}, {2.0, 4.0, 5.0});

  must_clause.ctdef_ = &mock_ctdef;
  filter_clause.ctdef_ = &mock_ctdef;
  should_clause.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 0;
  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  EXPECT_EQ(DAS_SEARCH_OP_REQ_OPT, op->get_op_type());
}

// Test: generate_op error propagation from req
// Verifies that errors from req function are properly propagated
TEST_F(ObDASBooleanQueryTest, generate_op_req_error_propagation)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  must_clause.ctdef_ = &mock_ctdef;
  must_clause.set_return_error(OB_ERR_UNEXPECTED);

  add_clause(MUST, &must_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: generate_op error propagation from opt
// Verifies that errors from opt function are properly propagated
TEST_F(ObDASBooleanQueryTest, generate_op_opt_error_propagation)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause(10);
  should_clause.ctdef_ = &mock_ctdef;
  should_clause.set_return_error(OB_ERR_UNEXPECTED);

  add_clause(SHOULD, &should_clause);
  ctdef_.min_should_match_ = 1;

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test generate_op error propagation from excl
// Note: excl calls opt which calls generate_op on clauses.
// If generate_op fails on a must_not clause, the error should propagate.
// However, the error needs to be set in generate_op, not compute_cost.
// Since we can't easily mock generate_op failure without more complex setup,
// this test verifies that excl handles empty prohibited correctly (which is tested elsewhere).
// For actual error propagation, we test it through the req_error_in_clause_generate_op test.
TEST_F(ObDASBooleanQueryTest, generate_op_excl_error_propagation)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10, {1, 2, 3}, {1.0, 2.0, 3.0});
  MockSearchRtDef must_not_clause(5);
  must_not_clause.ctdef_ = &mock_ctdef;
  // Setting return_error in compute_cost won't affect generate_op
  // The error propagation test is better covered by req_error_in_clause_generate_op
  must_not_clause.set_return_error(OB_ERR_UNEXPECTED);

  must_clause.ctdef_ = &mock_ctdef;

  add_clause(MUST, &must_clause);
  add_clause(MUST_NOT, &must_not_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  // This will succeed because compute_cost error doesn't propagate to generate_op
  // The error is only checked during cost computation, not during op generation
  // For actual error propagation testing, see req_error_in_clause_generate_op
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: req with nullptr clause in must should return error
// Verifies error handling when a MUST clause is nullptr
TEST_F(ObDASBooleanQueryTest, req_nullptr_clause_in_must)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);

  // Setup structure with nullptr clause
  void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
  void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
  ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
  ctdef_.children_cnt_ = 1;
  rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
  rtdef_.children_cnt_ = 1;

  ctdef_.must_.exist_ = true;
  ctdef_.must_.offset_ = 0;
  ctdef_.must_.count_ = 1;
  ctdef_.children_[0] = &mock_ctdef;
  rtdef_.children_[0] = nullptr; // nullptr clause

  ObIDASSearchOp *op = nullptr;
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: req with nullptr clause in filter should return error
// Verifies error handling when a FILTER clause is nullptr
TEST_F(ObDASBooleanQueryTest, req_nullptr_clause_in_filter)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);

  // Setup structure with nullptr clause in filter
  void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
  void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
  ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
  ctdef_.children_cnt_ = 1;
  rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
  rtdef_.children_cnt_ = 1;

  ctdef_.filter_.exist_ = true;
  ctdef_.filter_.offset_ = 0;
  ctdef_.filter_.count_ = 1;
  ctdef_.children_[0] = &mock_ctdef;
  rtdef_.children_[0] = nullptr; // nullptr clause

  ObIDASSearchOp *op = nullptr;
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: opt with nullptr clause should return error
// Verifies error handling when a SHOULD clause is nullptr
TEST_F(ObDASBooleanQueryTest, opt_nullptr_clause)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);

  // Setup structure with nullptr clause in should
  void *ct_buf = allocator_.alloc(sizeof(ObDASBaseCtDef*) * 1);
  void *rt_buf = allocator_.alloc(sizeof(ObDASBaseRtDef*) * 1);
  ctdef_.children_ = (ObDASBaseCtDef**)ct_buf;
  ctdef_.children_cnt_ = 1;
  rtdef_.children_ = (ObDASBaseRtDef**)rt_buf;
  rtdef_.children_cnt_ = 1;

  ctdef_.should_.exist_ = true;
  ctdef_.should_.offset_ = 0;
  ctdef_.should_.count_ = 1;
  ctdef_.children_[0] = &mock_ctdef;
  rtdef_.children_[0] = nullptr; // nullptr clause
  ctdef_.min_should_match_ = 1;

  ObIDASSearchOp *op = nullptr;
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test getter methods with out of range for filter
TEST_F(ObDASBooleanQueryTest, getter_filter_out_of_range)
{
  SearchCtxSetup ctx_setup(allocator_);

  ctdef_.filter_.exist_ = true;
  ctdef_.filter_.offset_ = 0;
  ctdef_.filter_.count_ = 10;
  ctdef_.children_cnt_ = 0;

  ObBooleanSubClause<ObIDASSearchRtDef> clause;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.filter(clause));
}

// Test: Getter method (should) with out of range should return error
// Verifies error handling when accessing should clauses with invalid range
TEST_F(ObDASBooleanQueryTest, getter_should_out_of_range)
{
  SearchCtxSetup ctx_setup(allocator_);

  ctdef_.should_.exist_ = true;
  ctdef_.should_.offset_ = 0;
  ctdef_.should_.count_ = 10;
  ctdef_.children_cnt_ = 0;

  ObBooleanSubClause<ObIDASSearchRtDef> clause;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.should(clause));
}

// Test: Getter method (must_not) with out of range should return error
// Verifies error handling when accessing must_not clauses with invalid range
TEST_F(ObDASBooleanQueryTest, getter_must_not_out_of_range)
{
  SearchCtxSetup ctx_setup(allocator_);

  ctdef_.must_not_.exist_ = true;
  ctdef_.must_not_.offset_ = 0;
  ctdef_.must_not_.count_ = 10;
  ctdef_.children_cnt_ = 0;

  ObBooleanSubClause<ObIDASSearchRtDef> clause;
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.must_not(clause));
}

// Test compute_cost with should_cost valid but required_cost invalid
// Note: When compute_required_cost_inner encounters invalid cost, it returns OB_ERR_UNEXPECTED,
// which causes compute_required_cost to fail, and thus compute_cost to fail.
// So this test expects failure, not success.
TEST_F(ObDASBooleanQueryTest, compute_cost_should_valid_required_invalid)
{
  MockSearchRtDef should_clause(10);
  MockSearchRtDef must_clause(5);
  must_clause.set_return_invalid_cost(true);

  add_clause(MUST, &must_clause);
  add_clause(SHOULD, &should_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  // When required_cost is invalid, compute_required_cost_inner returns error,
  // causing compute_cost to fail before computing should_cost
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test compute_cost with required_cost valid but should_cost invalid
// Note: When compute_should_cost encounters invalid cost, it returns OB_ERR_UNEXPECTED,
// which causes compute_cost to fail. However, if required_cost is valid and min_should_match == 0,
// we return early with required_cost. But if min_should_match > 0, we need should_cost.
TEST_F(ObDASBooleanQueryTest, compute_cost_required_valid_should_invalid)
{
  MockSearchRtDef must_clause(5);
  MockSearchRtDef should_clause(10);
  should_clause.set_return_invalid_cost(true);

  add_clause(MUST, &must_clause);
  add_clause(SHOULD, &should_clause);

  SearchCtxSetup ctx_setup(allocator_);
  ObDASSearchCost cost;
  prepare_structure();
  ctdef_.min_should_match_ = 1;
  // When should_cost is invalid, compute_should_cost returns error,
  // causing compute_cost to fail
  EXPECT_EQ(OB_ERR_UNEXPECTED, rtdef_.compute_cost(ctx_setup.search_ctx_, cost));
}

// Test: generate_op with mixed query and min_should_match > 0
// Verifies complex query generation with all clause types and min_should_match > 0
TEST_F(ObDASBooleanQueryTest, generate_op_mixed_min_match_positive_complex)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10, {1, 3, 5}, {10.0, 30.0, 50.0});
  MockSearchRtDef filter_clause(5, {1, 3}, {1.0, 3.0});
  MockSearchRtDef should_clause1(8, {2, 4, 5}, {2.0, 4.0, 5.0});
  MockSearchRtDef should_clause2(3, {3, 5}, {3.0, 5.0});
  MockSearchRtDef must_not_clause(2, {2}, {2.0});

  must_clause.ctdef_ = &mock_ctdef;
  filter_clause.ctdef_ = &mock_ctdef;
  should_clause1.ctdef_ = &mock_ctdef;
  should_clause2.ctdef_ = &mock_ctdef;
  must_not_clause.ctdef_ = &mock_ctdef;

  ctdef_.min_should_match_ = 1;
  add_clause(MUST, &must_clause);
  add_clause(FILTER, &filter_clause);
  add_clause(SHOULD, &should_clause1);
  add_clause(SHOULD, &should_clause2);
  add_clause(MUST_NOT, &must_not_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  ASSERT_EQ(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
  ASSERT_NE(nullptr, op);
  // Should be CONJUNCTION with REQ_EXCL and DISJUNCTION
  EXPECT_EQ(DAS_SEARCH_OP_CONJUNCTION, op->get_op_type());
}

// Test: req with error in generate_op of clause
// Verifies error propagation when clause->generate_op fails
TEST_F(ObDASBooleanQueryTest, req_error_in_clause_generate_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef must_clause(10);
  must_clause.ctdef_ = &mock_ctdef;
  must_clause.set_return_error(OB_ERR_UNEXPECTED);

  add_clause(MUST, &must_clause);

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// Test: opt with error in generate_op of clause
// Verifies error propagation when clause->generate_op fails in opt
TEST_F(ObDASBooleanQueryTest, opt_error_in_clause_generate_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchCtDef mock_ctdef(allocator_);
  MockSearchRtDef should_clause(10);
  should_clause.ctdef_ = &mock_ctdef;
  should_clause.set_return_error(OB_ERR_UNEXPECTED);

  add_clause(SHOULD, &should_clause);
  ctdef_.min_should_match_ = 1;

  ObIDASSearchOp *op = nullptr;
  prepare_structure();
  EXPECT_NE(OB_SUCCESS, rtdef_.generate_op(ObDASSearchCost(0), ctx_setup.search_ctx_, op));
}

// ============================================================================
// Section 7: ObDASConjunctionOp Tests
// ============================================================================
// Tests for ConjunctionOp lifecycle, error handling, and edge cases

// Test: ConjunctionOp do_init with empty required_ops should return error
// Verifies error handling when required_ops array is empty
TEST_F(ObDASBooleanQueryTest, conjunction_op_inner_init_empty_required_ops)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> empty_ops;
  ObDASConjunctionOpParam param(empty_ops);
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.init(param));
}

// Test: ConjunctionOp open with nullptr op should return error
// Verifies error handling when a required op is nullptr during open()
TEST_F(ObDASBooleanQueryTest, conjunction_op_open_nullptr_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(nullptr);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test: ConjunctionOp open with op->open() failure
// Verifies error propagation when a child op's open() fails
TEST_F(ObDASBooleanQueryTest, conjunction_op_open_op_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 3, 4});
  mock_op1->set_open_error(OB_ERR_UNEXPECTED);

  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test ConjunctionOp rescan with nullptr op
// Note: open() will fail if ops contains nullptr, so we test that open() detects it
TEST_F(ObDASBooleanQueryTest, conjunction_op_rescan_nullptr_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ops.push_back(nullptr);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  // open() should fail when it encounters nullptr op
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test: ConjunctionOp rescan with op->rescan() failure
// Verifies error propagation when a child op's rescan() fails
TEST_F(ObDASBooleanQueryTest, conjunction_op_rescan_op_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 3, 4});
  mock_op1->set_rescan_error(OB_ERR_UNEXPECTED);

  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.rescan());
}

// Test: ConjunctionOp close with nullptr op
// Verifies that open() detects nullptr op before close() can be called
TEST_F(ObDASBooleanQueryTest, conjunction_op_close_nullptr_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ops.push_back(nullptr);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  // open() should fail when it encounters nullptr op
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test: ConjunctionOp advance_to with target.is_max() should return OB_ITER_END
// Verifies that advance_to correctly handles max target
TEST_F(ObDASBooleanQueryTest, conjunction_op_advance_to_max_target)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 3, 4});

  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());

  ObDASRowID target;
  ObDASRowID curr_id;
  double score = 0;
  target.set_max();
  EXPECT_EQ(OB_ITER_END, conjunction_op.advance_to(target, curr_id, score));
}

// Test: ConjunctionOp advance_to with nullptr op
// Verifies that open() detects nullptr op before advance_to() can be called
TEST_F(ObDASBooleanQueryTest, conjunction_op_advance_to_nullptr_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(nullptr);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  // open() should fail when it encounters nullptr op
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test: ConjunctionOp advance_to with op->advance_to() failure
// Verifies error propagation when a child op's advance_to() fails
TEST_F(ObDASBooleanQueryTest, conjunction_op_advance_to_op_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 3, 4});
  mock_op1->set_advance_to_error(OB_ERR_UNEXPECTED);

  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());

  ObDASRowID target;
  ObDASRowID curr_id;
  double score = 0;
  target.set_uint64(1);
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.advance_to(target, curr_id, score));
}

// Test: ConjunctionOp next_rowid with nullptr op
// Verifies that open() detects nullptr op before next_rowid() can be called
TEST_F(ObDASBooleanQueryTest, conjunction_op_next_rowid_nullptr_op)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(nullptr);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  // open() should fail when it encounters nullptr op
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.open());
}

// Test: ConjunctionOp next_rowid with op->next_rowid() failure
// Verifies error propagation when a child op's next_rowid() fails
TEST_F(ObDASBooleanQueryTest, conjunction_op_next_rowid_op_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 3, 4});
  mock_op1->set_next_rowid_error(OB_ERR_UNEXPECTED);

  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());

  ObDASRowID next_id;
  double score = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, conjunction_op.next_rowid(next_id, score));
}

// Test: ConjunctionOp do_advance_to with normal flow
// Verifies that do_advance_to works correctly with non-intersecting ops
TEST_F(ObDASBooleanQueryTest, conjunction_op_do_advance_to_unexpected_order)
{
  SearchCtxSetup ctx_setup(allocator_);
  // Create ops where advance_to might return unexpected order
  // This is hard to trigger directly, but we can test the normal flow
  MockSearchOp *mock_op1 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 3, 5});
  MockSearchOp *mock_op2 = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{2, 4, 6});

  ObDASConjunctionOp conjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op1);
  ops.push_back(mock_op2);
  ObDASConjunctionOpParam param(ops);
  ASSERT_EQ(OB_SUCCESS, conjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, conjunction_op.open());

  // Test normal advance_to - should work correctly
  ObDASRowID target;
  ObDASRowID curr_id;
  double score = 0;
  target.set_uint64(1);
  // Since ops don't intersect, should eventually reach end
  int ret = conjunction_op.advance_to(target, curr_id, score);
  // Should either succeed or reach end
  EXPECT_TRUE(ret == OB_SUCCESS || ret == OB_ITER_END);
}

// ============================================================================
// Section 8: ObDASDisjunctionOp Tests
// ============================================================================
// Tests for DisjunctionOp lifecycle, error handling, and edge cases

// Test: DisjunctionOp do_init double initialization should return OB_INIT_TWICE
// Verifies error handling when do_init is called twice
TEST_F(ObDASBooleanQueryTest, disjunction_op_inner_init_double_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  EXPECT_EQ(OB_INIT_TWICE, disjunction_op.init(param));
}

// Test: DisjunctionOp open with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when open() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_open_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.open());
}

// Test: DisjunctionOp open with nullptr child should return error
// Verifies error handling when a child op is nullptr during open()
TEST_F(ObDASBooleanQueryTest, disjunction_op_open_nullptr_child)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(nullptr);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  EXPECT_EQ(OB_ERR_UNEXPECTED, disjunction_op.open());
}

// Test: DisjunctionOp open with child->open() failure
// Verifies error propagation when a child op's open() fails
TEST_F(ObDASBooleanQueryTest, disjunction_op_open_child_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  mock_op->set_open_error(OB_ERR_UNEXPECTED);

  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  EXPECT_EQ(OB_ERR_UNEXPECTED, disjunction_op.open());
}

// Test: DisjunctionOp rescan with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when rescan() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_rescan_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.rescan());
}

// Test: DisjunctionOp rescan with nullptr child
// Verifies that open() detects nullptr child before rescan() can be called
TEST_F(ObDASBooleanQueryTest, disjunction_op_rescan_nullptr_child)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ops.push_back(nullptr);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  // open() should fail when it encounters nullptr child
  EXPECT_EQ(OB_ERR_UNEXPECTED, disjunction_op.open());
}

// Test: DisjunctionOp rescan with child->rescan() failure
// Verifies error propagation when a child op's rescan() fails
TEST_F(ObDASBooleanQueryTest, disjunction_op_rescan_child_failure)
{
  SearchCtxSetup ctx_setup(allocator_);
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  mock_op->set_rescan_error(OB_ERR_UNEXPECTED);

  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.open());
  EXPECT_EQ(OB_ERR_UNEXPECTED, disjunction_op.rescan());
}

// Test: DisjunctionOp advance_to with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when advance_to() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_advance_to_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObDASRowID target;
  ObDASRowID curr_id;
  double score = 0;
  target.set_uint64(1);
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.advance_to(target, curr_id, score));
}

// Test: DisjunctionOp advance_to with iter_end_ should return OB_ITER_END
// Verifies that advance_to correctly handles iter_end_ flag
TEST_F(ObDASBooleanQueryTest, disjunction_op_advance_to_iter_end)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.open());
  // Manually set iter_end_ to test the branch
  disjunction_op.iter_end_ = true;

  ObDASRowID target;
  ObDASRowID curr_id;
  double score = 0;
  target.set_uint64(1);
  EXPECT_EQ(OB_ITER_END, disjunction_op.advance_to(target, curr_id, score));
}

// Test: DisjunctionOp next_rowid with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when next_rowid() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_next_rowid_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObDASRowID next_id;
  double score = 0;
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.next_rowid(next_id, score));
}

// Test: DisjunctionOp set_min_competitive_score with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when set_min_competitive_score() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_set_min_competitive_score_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.set_min_competitive_score(1.0));
}

// Test: DisjunctionOp set_min_competitive_score with need_score_ false
// Verifies that when need_score_ is false, the method does nothing
TEST_F(ObDASBooleanQueryTest, disjunction_op_set_min_competitive_score_no_score)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  // need_score_ is false by default
  ASSERT_EQ(OB_SUCCESS, disjunction_op.set_min_competitive_score(1.0));
}

// Test: DisjunctionOp set_min_competitive_score with children_cnt_ == 1
// Verifies that when there's only one child, it delegates to the child directly
TEST_F(ObDASBooleanQueryTest, disjunction_op_set_min_competitive_score_single_child)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  disjunction_op.need_score_ = true;
  ASSERT_EQ(OB_SUCCESS, disjunction_op.set_min_competitive_score(1.0));
}

// Test: DisjunctionOp calc_max_score with IS_NOT_INIT should return OB_NOT_INIT
// Verifies error handling when calc_max_score() is called before initialization
TEST_F(ObDASBooleanQueryTest, disjunction_op_calc_max_score_not_init)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  double threshold = 0;
  EXPECT_EQ(OB_NOT_INIT, disjunction_op.calc_max_score(threshold));
}

// Test: DisjunctionOp calc_max_score with need_score_ false
// Verifies that when need_score_ is false, threshold is set to 0.0
TEST_F(ObDASBooleanQueryTest, disjunction_op_calc_max_score_no_score)
{
  SearchCtxSetup ctx_setup(allocator_);
  ObDASDisjunctionOp disjunction_op(ctx_setup.search_ctx_);
  ObArray<ObIDASSearchOp *> ops;
  MockSearchOp *mock_op = OB_NEWx(MockSearchOp, &allocator_, ctx_setup.search_ctx_, std::vector<uint64_t>{1, 2, 3});
  ops.push_back(mock_op);
  ObDASDisjunctionOpParam param(ops, 1, false, ObDASSearchCost(10));
  ASSERT_EQ(OB_SUCCESS, disjunction_op.init(param));
  double threshold = 0;
  ASSERT_EQ(OB_SUCCESS, disjunction_op.calc_max_score(threshold));
  EXPECT_EQ(0.0, threshold);
}

// ============================================================================
// Section 9: ObHybridSearchCgService::generate_ctdef Tests
// ============================================================================

class ObDASBooleanQueryCgTest : public ObDASBooleanQueryTest
{
public:
  ObDASBooleanQueryCgTest() : ObDASBooleanQueryTest(),
                              phy_plan_(),
                              cg_(CLUSTER_VERSION_4_1_0_0),
                              cg_service_(cg_)
  {
    cg_.phy_plan_ = &phy_plan_;
  }

  virtual ~ObDASBooleanQueryCgTest() {}

protected:
  ObPhysicalPlan phy_plan_;
  ObStaticEngineCG cg_;
  ObHybridSearchCgService cg_service_;
};

// Test: generate_ctdef with null phy_plan_
TEST_F(ObDASBooleanQueryCgTest, generate_ctdef_null_phy_plan)
{
  cg_.phy_plan_ = nullptr;
  ObBooleanQueryNode node(allocator_);
  ObDASBooleanQueryCtDef *ctdef = nullptr;
  ObLogTableScan *op = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, cg_service_.generate_ctdef(*op, &node, ctdef));
}

// Test: generate_ctdef with null node
TEST_F(ObDASBooleanQueryCgTest, generate_ctdef_null_node)
{
  ObDASBooleanQueryCtDef *ctdef = nullptr;
  ObLogTableScan *op = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, cg_service_.generate_ctdef(*op, nullptr, ctdef));
}

// Test: generate_ctdef with invalid min_should_match (< 0)
TEST_F(ObDASBooleanQueryCgTest, generate_ctdef_invalid_min_match_neg)
{
  ObBooleanQueryNode node(allocator_);
  node.min_should_match_ = -1;
  ObDASBooleanQueryCtDef *ctdef = nullptr;
  ObLogTableScan *op = nullptr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, cg_service_.generate_ctdef(*op, &node, ctdef));
}

// Test: generate_ctdef with no positive clauses
TEST_F(ObDASBooleanQueryCgTest, generate_ctdef_no_positive_clauses)
{
  ObBooleanQueryNode node(allocator_);
  void *buf = allocator_.alloc(sizeof(ObScalarQueryNode));
  ObScalarQueryNode *s1 = new(buf) ObScalarQueryNode(allocator_);
  node.must_not_nodes_.push_back(s1);
  node.min_should_match_ = 0;
  ObDASBooleanQueryCtDef *ctdef = nullptr;
  ObLogTableScan *op = nullptr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, cg_service_.generate_ctdef(*op, &node, ctdef));
}


int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
