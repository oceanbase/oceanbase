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

#ifndef OCEANBASE_SQL_ENGINE_JOIN_MERGE_JOIN_H_
#define OCEANBASE_SQL_ENGINE_JOIN_MERGE_JOIN_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "common/row/ob_row.h"
#include "common/row/ob_row_store.h"
#include "sql/engine/join/ob_join.h"
namespace oceanbase {
namespace sql {
class ObExecContext;
class ObMergeJoin : public ObJoin {
  OB_UNIS_VERSION_V(1);
  friend class TestObMergeJoin;

private:
  enum ObJoinState {
    // this state need not execute actually.
    JS_JOIN_END = 0,
    // these states need judge weither OB_ITER_END or not only.
    JS_JOIN_BEGIN,
    JS_LEFT_JOIN,
    JS_RIGHT_JOIN_CACHE,
    JS_RIGHT_JOIN,
    JS_READ_CACHE,  // join left row with cached right rows instead of next rows from op.
    // JS_GOING_END_ONLY is devision value to separate state that need different judgement,
    // so we can use if statement likes below to execute different judgement:
    // if (join_ctx.state_ < JUDGE_ITER_END_ONLY) {
    //   // do some judgements
    // } else {
    //   // do other judgements
    // }
    JS_GOING_END_ONLY,
    // these states need judge weither OB_ITER_END or not,
    // and then judge rows are euqal or not on equal conds.
    JS_FULL_CACHE,   // have append all right rows that can joined with current left row to cache.
    JS_EMPTY_CACHE,  // have not got a pair rows that can joined, read next left or right row.
    JS_FILL_CACHE,   // have got a pair rows that can joined, continue append more right rows to cache.
    // ALWAYS keep this enum value as the LAST value, which used to declare static array.
    JS_STATE_COUNT
  };
  enum ObFuncType { FT_ROWS_EQUAL = 0, FT_ROWS_DIFF, FT_ITER_GOING, FT_ITER_END, FT_TYPE_COUNT };
  static const int64_t MERGE_DIRECTION_ASC;
  static const int64_t MERGE_DIRECTION_DESC;

  class ObMergeJoinCtx : public ObJoinCtx {
    friend class ObMergeJoin;

  public:
    explicit ObMergeJoinCtx(ObExecContext& ctx);
    virtual ~ObMergeJoinCtx()
    {}
    void reset()
    {
      state_ = JS_JOIN_BEGIN;
      stored_row_ = NULL;
      right_cache_.reset();
      right_cache_iter_.reset();
      right_iter_end_ = false;
      equal_cmp_ = 0;
      left_row_matched_ = false;
    }
    void set_tenant_id(uint64_t tenant_id)
    {
      right_cache_.set_tenant_id(tenant_id);
    }
    virtual void destroy()
    {
      right_cache_.~ObRowStore();
      ObPhyOperatorCtx::destroy_base();
    }

  private:
    ObJoinState state_;
    common::ObNewRow right_cache_row_buf_;  // similar with cur_row_, used for iterator of right_cache_.
    common::ObRowStore::StoredRow* stored_row_;
    common::ObRowStore right_cache_;
    common::ObRowStore::Iterator right_cache_iter_;
    bool right_iter_end_;
    int64_t equal_cmp_;
    // for anti/semi join, when equal_condition is equal, we should remain until all equal right rows scaned,
    // when right table/cache scan end,
    // for anti join, if equal_condition and other_condition matched, not output this row, else output
    // for semi join, if equal_condition and other_condition matched, output this row, else not output
    bool left_row_matched_;
  };

public:
  explicit ObMergeJoin(common::ObIAllocator& alloc);
  virtual ~ObMergeJoin();
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& exec_ctx) const;
  virtual int switch_iterator(ObExecContext& ctx) const override;
  inline void set_left_unique(const bool u)
  {
    is_left_unique_ = u;
  }
  inline bool is_left_unique()
  {
    return is_left_unique_;
  }

  int set_merge_directions(const common::ObIArray<ObOrderDirection>& merge_directions)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(merge_directions_.init(merge_directions.count()))) {
      SQL_ENG_LOG(WARN, "fail to init merge direction", K(ret));
    }
    ARRAY_FOREACH(merge_directions, i)
    {
      if (OB_FAIL(add_merge_direction(merge_directions.at(i)))) {
        SQL_ENG_LOG(WARN, "failed to add merge direction", K(ret), K(i));
      }
    }
    return ret;
  }

  int add_merge_direction(ObOrderDirection direction)
  {
    return merge_directions_.push_back(is_ascending_direction(direction) ? MERGE_DIRECTION_ASC : MERGE_DIRECTION_DESC);
  }

private:
  // state operation and transfer functions.
  typedef int (ObMergeJoin::*state_operation_func_type)(ObMergeJoinCtx& join_ctx) const;
  typedef int (ObMergeJoin::*state_function_func_type)(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& exec_ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& exec_ctx) const;
  virtual int inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const;

private:
  // JS_JOIN_END state operation and transfer functions.
  int join_end_operate(ObMergeJoinCtx& join_ctx) const;
  int join_end_func_end(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // JS_JOIN_BEGIN state operation and transfer functions.
  int join_begin_operate(ObMergeJoinCtx& join_ctx) const;
  int join_begin_func_going(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_begin_func_end(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // JS_LEFT_JOIN state operation and transfer functions.
  int left_join_operate(ObMergeJoinCtx& join_ctx) const;
  int left_join_func_going(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int left_join_func_end(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // JS_RIGHT_JOIN_CACHE state operation and transfer functions.
  int right_join_cache_operate(ObMergeJoinCtx& join_ctx) const;
  int right_join_cache_func_going(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int right_join_cache_func_end(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // JS_RIGHT_JOIN state operation and transfer functions.
  int right_join_operate(ObMergeJoinCtx& join_ctx) const;
  int right_join_func_going(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int right_join_func_end(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // JS_READ_CACHE state operation and transfer functions.
  int read_cache_operate(ObMergeJoinCtx& join_ctx) const;
  int read_cache_func_going(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_cache_func_end(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // JS_FULL_CACHE state operation and transfer functions.
  int full_cache_operate(ObMergeJoinCtx& join_ctx) const;
  int full_cache_func_equal(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int full_cache_func_diff(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int full_cache_func_end(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // JS_EMPTY_CACHE_LEFT & JS_EMPTY_CACHE_RIGHT state operation and transfer functions.
  int empty_cache_operate(ObMergeJoinCtx& join_ctx) const;
  int empty_cache_func_equal(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int empty_cache_func_diff(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int empty_cache_func_end(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // JS_FILL_CACHE state operation and transfer functions.
  int fill_cache_operate(ObMergeJoinCtx& join_ctx) const;
  int fill_cache_func_equal(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int fill_cache_func_diff_end(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // implementation functions used by state operations and transfer functions.
  int get_next_right_cache_row(ObMergeJoinCtx& join_ctx) const;
  int trans_to_read_cache(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int trans_to_fill_cache(ObMergeJoinCtx& join_ctx, const common::ObNewRow*& row) const;

  inline bool is_skip_cache() const
  {
    return is_left_unique_ && INNER_JOIN == join_type_;
  }

private:
  state_operation_func_type state_operation_func_[JS_STATE_COUNT];
  state_function_func_type state_function_func_[JS_STATE_COUNT][FT_TYPE_COUNT];
  common::ObFixedArray<int64_t, common::ObIAllocator> merge_directions_;
  bool is_left_unique_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeJoin);
};
// int ObMergeJoin::set_merge_directions(common::ObIArray &merge_directions)
//{
//  int ret = OB_SUCCESS;
//  if (OB_FAIL(merge_directions_.init(merge_directions.count()))) {
//    SQL_ENG_LOG(WARN, "fail to init merge direction", K(ret));
//  }
//  ARRAY_FOREACH(merge_directions, i) {
//    if (OB_FAIL(add_merge_direction(merge_directions.at(i)))) {
//      LOG_WARN("failed to add merge direction", K(ret), K(i));
//    }
//  }
//  return ret;
//}
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_JOIN_MERGE_JOIN_H_ */
