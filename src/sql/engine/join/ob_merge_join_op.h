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

#ifndef OCEANBASE_SQL_ENGINE_JOIN_OB_MERGE_JOIN_OP_
#define OCEANBASE_SQL_ENGINE_JOIN_OB_MERGE_JOIN_OP_

#include "ob_join_op.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "share/datum/ob_datum_funcs.h"

namespace oceanbase
{
namespace sql
{
class ObMergeJoinSpec: public ObJoinSpec
{
  OB_UNIS_VERSION_V(1);
public:
  struct EqualConditionInfo {
    OB_UNIS_VERSION(1);
  public:
    EqualConditionInfo()
      : expr_(NULL), ns_cmp_func_(NULL), is_opposite_(false)
    {}
    TO_STRING_KV(K(expr_), KP(ns_cmp_func_), K(is_opposite_));

    ObExpr *expr_;
    union {
      common::ObDatumCmpFuncType ns_cmp_func_;
      sql::serializable_function ser_eval_func_;
    };
    //表示equal condition 左右子表达式是否分别来自join算子左节点和右节点,
    //如果是, 则is_opposite_ = false, 如果不是, 则is_opposite_ = true;
    bool is_opposite_;
  };

public:
 ObMergeJoinSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
   : ObJoinSpec(alloc, type),
     equal_cond_infos_(alloc),
     merge_directions_(alloc),
     is_left_unique_(false),
     left_child_fetcher_all_exprs_(alloc),
     right_child_fetcher_all_exprs_(alloc)
  {}

  virtual ~ObMergeJoinSpec() {};

  inline bool is_skip_cache() const { return is_left_unique_ && INNER_JOIN == join_type_; }

  int set_merge_directions(const common::ObIArray<ObOrderDirection> &merge_directions)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(merge_directions_.init(merge_directions.count()))) {
      SQL_ENG_LOG(WARN, "fail to init merge direction", K(ret));
    }
    ARRAY_FOREACH(merge_directions, i) {
      if (OB_FAIL((add_merge_direction(merge_directions.at(i))))) {
        SQL_ENG_LOG(WARN, "failed to add merge direction", K(ret), K(i));
      }
    }
    return ret;
  }

private:
  static const int64_t MERGE_DIRECTION_ASC;
  static const int64_t MERGE_DIRECTION_DESC;
  int add_merge_direction(ObOrderDirection direction)
  { return merge_directions_.push_back(is_ascending_direction(direction) ?
                                       MERGE_DIRECTION_ASC : MERGE_DIRECTION_DESC); }

public:
  common::ObFixedArray<EqualConditionInfo, common::ObIAllocator> equal_cond_infos_;
  common::ObFixedArray<int64_t, common::ObIAllocator> merge_directions_;
  bool is_left_unique_;
  ExprFixedArray left_child_fetcher_all_exprs_;
  ExprFixedArray right_child_fetcher_all_exprs_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeJoinSpec);
};

typedef common::ObFixedArray<ObDatum *, common::ObIAllocator> DatumPtrFixedArray;

class ObMergeJoinOp: public ObJoinOp
{
private:
  struct ObStoredJoinRow : public ObChunkDatumStore::StoredRow
  {
    bool &get_extra_info()
    {
      static_assert(sizeof(ObStoredJoinRow) == sizeof(ObChunkDatumStore::StoredRow),
          "sizeof StoredJoinRow must be the save with StoredRow");
      return *reinterpret_cast<bool *>(get_extra_payload());
    }

    const bool &is_match() const
    { return *reinterpret_cast<const bool *>(get_extra_payload()); }

    void set_is_match(bool is_match) { get_extra_info() = is_match; }
  };

  struct JoinRowList
  {
    JoinRowList() : start_(0), end_(0) {}
    JoinRowList(int64_t start) : start_(start), end_(start) {}
    int64_t count() const { return end_ - start_; }
    int64_t start_;
    int64_t end_;
    TO_STRING_KV(K(start_), K(end_));
  };
  // a list is a group with same value of equal_conds_param
  typedef std::pair<JoinRowList, JoinRowList> RowsListPair;
  typedef std::pair<ObStoredJoinRow *, ObStoredJoinRow *> RowsPair;


  struct ChildRowFetcher
  {
    ChildRowFetcher() : has_last_row_(false), has_backup_row_(false),
                        reach_end_(false), left_row_joined_(NULL), child_(NULL)
    {
    }

    int init(ObOperator &child, common::ObIAllocator &alloc, bool *left_row_joined)
    {
      child_ = &child;
      left_row_joined_ = left_row_joined;
      return store_row_.init(alloc, child.get_spec().output_.count());
    }

    int next()
    {
      int ret = common::OB_SUCCESS;
      if (OB_UNLIKELY(reach_end_)) {
        ret = common::OB_ITER_END;
      } else if (has_last_row_) {
        has_last_row_ = false;
        ret = store_row_.restore(child_->get_spec().output_, child_->get_eval_ctx());
      } else {
        if (has_backup_row_) {
          has_backup_row_ = false;
          ret = store_row_.restore(child_->get_spec().output_, child_->get_eval_ctx());
        }
        if (OB_SUCC(ret)) {
          if (NULL != left_row_joined_) {
            *left_row_joined_ = false;
          }
          ret = child_->get_next_row();
          if (common::OB_ITER_END == ret) {
            reach_end_ = true;
          }
        }
      }
      return ret;
    }

    int backup()
    {
      int ret = common::OB_SUCCESS;
      if (!has_backup_row_) {
        has_backup_row_ = true;
        if (!reach_end_) {
          ret = store_row_.shadow_copy(child_->get_spec().output_, child_->get_eval_ctx());
        }
      }
      return ret;
    }

    int restore()
    {
      int ret = common::OB_SUCCESS;
      if (has_backup_row_ && !reach_end_) {
        ret = store_row_.restore(child_->get_spec().output_, child_->get_eval_ctx());
      }
      return ret;
    }

    int save_last()
    {
      has_last_row_ = true;
      return backup();
    }

    void reuse()
    {
      has_last_row_ = false;
      has_backup_row_ = false;
      reach_end_ = false;
    }

    // reset && release referenced memory
    void reset()
    {
      reuse();
      store_row_.reset();
      child_ = NULL;
    }

    // indicate that we should get row from %store_row_
    bool has_last_row_;
    // indicate that we should restore store_row_ first when get child row, to make sure child's
    // output not overwrite by us.
    bool has_backup_row_;
    bool reach_end_; // child iterator end
    bool *left_row_joined_;
    ObChunkDatumStore::ShadowStoredRow store_row_;
    ObOperator *child_;
  };

  struct ChildBatchFecther
  {
    ChildBatchFecther(ObIArray<RowsListPair> &match_groups,
        ObMergeJoinOp &merge_join_op,
        common::ObIAllocator &allocator) :
        cur_idx_(0), brs_(), batch_size_(0), child_(NULL),
        match_groups_(match_groups), merge_join_op_(merge_join_op),
        all_exprs_(NULL), datum_store_(), backup_datums_(),
        backup_rows_cnt_(0), brs_holder_(),
        equal_param_idx_(allocator),
        row_list_buffer_(NULL), row_list_buffer_size_(0), row_list_buffer_cnt_(0)
    {}
    int init(const uint64_t tenant_id, bool is_left, ObOperator *child,
             const ObIArray<ObMergeJoinSpec::EqualConditionInfo> &equal_cond_infos,
             const ExprFixedArray *all_exprs);
    template<bool need_store_unmatch, bool is_left>
    int get_next_small_group(int64_t &cmp_res);
    template<bool is_left>
    int get_next_equal_group(JoinRowList &row_list,
                             const ObChunkDatumStore::StoredRow *stored_row,
                             const bool is_unique);
    int get_next_batch(const int64_t max_row_cnt);
    int backup_remain_rows();
    int get_next_nonskip_row(bool &got_next_batch);
    bool iter_end() { return brs_.end_ && 0 == brs_.size_; }
    int push_back_list_row(ObStoredJoinRow *stored_row, JoinRowList &list);
    int get_list_row(int64_t idx, ObStoredJoinRow *&stored_row);
    // for operator rescan
    void reuse()
    {
      cur_idx_ = 0;
      brs_.skip_ = NULL;
      brs_.size_ = 0;
      brs_.end_ = false;
      datum_store_.reset();
      backup_datums_.reuse();
      backup_rows_cnt_ = 0;
      row_list_buffer_cnt_ = 0;
      brs_holder_.reset();
    }
    // for destroy
    void reset()
    {
      cur_idx_ = 0;
      brs_.skip_ = NULL;
      brs_.size_ = 0;
      brs_.end_ = false;
      datum_store_.reset();
      backup_datums_.reset();
      backup_rows_cnt_ = 0;
      row_list_buffer_cnt_ = 0;
      brs_holder_.reset();
    }
    int64_t cur_idx_;
    ObBatchRows brs_;
    int64_t batch_size_;
    ObOperator *child_;
    ObIArray<RowsListPair> &match_groups_;
    ObMergeJoinOp &merge_join_op_;
    const ExprFixedArray *all_exprs_;
    ObChunkDatumStore datum_store_;
    // When the other fetcher is end, we start to iterator this fetcher and outptu until end.
    // At the beginning, there are some rows in the current batch that not output yet.
    // We need store these rows and output first, then get batch from child and output directly.
    ObSEArray<ObDatum *, 256, common::ModulePageAllocator, true> backup_datums_;
    int64_t backup_rows_cnt_;
    ObBatchResultHolder brs_holder_;

    common::ObFixedArray<int64_t, common::ObIAllocator> equal_param_idx_;

    ObStoredJoinRow **row_list_buffer_;
    int64_t row_list_buffer_size_;
    int64_t row_list_buffer_cnt_;
  };

  enum ObJoinState {
    // this state need not execute actually.
    JS_JOIN_END = 0,
    // these states need judge weither OB_ITER_END or not only.
    JS_JOIN_BEGIN,
    JS_LEFT_JOIN,
    JS_RIGHT_JOIN_CACHE,
    JS_RIGHT_JOIN,
    JS_READ_CACHE,    // join left row with cached right rows instead of next rows from op.
    // JS_GOING_END_ONLY is division value to separate state that need different judgement,
    JS_GOING_END_ONLY,
    // these states need judge weither OB_ITER_END or not,
    // and then judge rows are euqal or not on equal conds.
    JS_FULL_CACHE,    // have append all right rows that can joined with current left row to cache.
    JS_EMPTY_CACHE,   // have not got a pair rows that can joined, read next left or right row.
    JS_FILL_CACHE,    // have got a pair rows that can joined, continue append more right rows to cache.
    // ALWAYS keep this enum value as the LAST value, which used to declare static array.
    JS_STATE_COUNT
  };
  enum ObFuncType {
    FT_ROWS_EQUAL = 0,
    FT_ROWS_DIFF,
    FT_ITER_GOING,
    FT_ITER_END,
    FT_TYPE_COUNT
  };

  // iter ate side for JS_EMPTY_CACHE state.
  enum ObMJIterateSide {
    ITER_LEFT = 0b01,
    ITER_RIGHT = 0b10,
    ITER_BOTH = 0b11
  };
public:
  ObMergeJoinOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObMergeJoinOp() { reset(); };

  virtual int inner_open() override;
  virtual int inner_switch_iterator() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_close() override { return OB_SUCCESS; }

  void reset()
  {
    if (MY_SPEC.is_vectorized()) {
      cmp_res_ = 0;
      match_groups_.reset();
      output_idx_ = 0;
      output_cache_.reset();
      batch_join_state_ = BJS_JOIN_BEGIN;
      left_brs_fetcher_.reuse();
      right_brs_fetcher_.reuse();
    } else {
      state_ = JS_JOIN_BEGIN;
      stored_row_ = NULL;
      right_cache_.reset();
      right_cache_iter_.reset();
      empty_cache_iter_side_ = ITER_BOTH;
      equal_cmp_ = 0;
      left_row_matched_ = false;
      left_fetcher_.reuse();
      right_fetcher_.reuse();
    }
  }
  virtual void destroy() override
  {
    match_groups_.reset();
    output_cache_.reset();
    left_brs_fetcher_.reset();
    right_brs_fetcher_.reset();
    right_cache_.reset();
    right_cache_iter_.reset();
    left_fetcher_.reset();
    right_fetcher_.reset();
    ObJoinOp::destroy();
  }
private:
  // JS_JOIN_END state operation and transfer functions.
  int join_end_operate();
  int join_end_func_end();
  // JS_JOIN_BEGIN state operation and transfer functions.
  int join_begin_operate();
  int join_begin_func_going();
  int join_begin_func_end();
  // JS_LEFT_JOIN state operation and transfer functions.
  int left_join_operate();
  int left_join_func_going();
  int left_join_func_end();
  // JS_RIGHT_JOIN_CACHE state operation and transfer functions.
  int right_join_cache_operate();
  int right_join_cache_func_going();
  int right_join_cache_func_end();
  // JS_RIGHT_JOIN state operation and transfer functions.
  int right_join_operate();
  int right_join_func_going();
  int right_join_func_end();
  // JS_READ_CACHE state operation and transfer functions.
  int read_cache_operate();
  int read_cache_func_going();
  int read_cache_func_end();
  // JS_FULL_CACHE state operation and transfer functions.
  int full_cache_operate();
  int full_cache_func_equal();
  int full_cache_func_diff();
  int full_cache_func_end();
  // JS_EMPTY_CACHE_LEFT & JS_EMPTY_CACHE_RIGHT state operation and transfer functions.
  int empty_cache_operate();
  int empty_cache_func_equal();
  int empty_cache_func_diff();
  int empty_cache_func_end();
  // JS_FILL_CACHE state operation and transfer functions.
  int fill_cache_operate();
  int fill_cache_func_equal();
  int fill_cache_func_diff_end();
  // implementation functions used by state operations and transfer functions.
  int get_next_right_cache_row();
  int trans_to_read_cache();
  int trans_to_fill_cache();


  int calc_equal_conds(int64_t &cmp_res);

  int blank_right_row()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(right_fetcher_.backup())) {
      SQL_ENG_LOG(WARN, "back right row failed", K(ret));
    } else if (OB_FAIL(ObJoinOp::blank_row(right_->get_spec().output_))) {
      SQL_ENG_LOG(WARN, "blank right row failed", K(ret));
    }
    return ret;
  }

  int blank_left_row()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(left_fetcher_.backup())) {
      SQL_ENG_LOG(WARN, "back left row failed", K(ret));
    } else if (OB_FAIL(ObJoinOp::blank_row(left_->get_spec().output_))) {
      SQL_ENG_LOG(WARN, "blank left row failed", K(ret));
    }
    return ret;
  }
  static int create_join_rows_list(JoinRowList *&row_list, ObIAllocator &allocator);
  int calc_equal_conds_with_batch_idx(int64_t &cmp_res);
  template<bool is_left_table_stored_row>
  int calc_equal_conds_with_stored_row(const ObChunkDatumStore::StoredRow *stored_row,
                                       int64_t batch_idx, int64_t &cmp_res);
  int store_group_first_row(ChildBatchFecther &child_fetcher,
                            JoinRowList &row_list,
                            ObChunkDatumStore::StoredRow *&stored_row,
                            ObEvalCtx::BatchInfoScopeGuard &guard);
  int iterate_both_chidren(ObEvalCtx::BatchInfoScopeGuard &guard);
  int batch_join_begin();
  int batch_join_both();
  template<bool left_empty_allowed, bool right_empty_allowed, ObJoinType join_type>
  int match_group_rows();
  int output_cache_rows(const int64_t max_row_cnt);
  template <bool need_blank_left, bool need_blank_right>
  int output_cache_rows(const int64_t max_row_cnt);
  int output_side_rows(ChildBatchFecther &batch_fetcher, const ExprFixedArray *blank_exprs,
                      const int64_t max_row_cnt);
  bool has_enough_datums();
  int64_t get_output_cache_remain_rows_cnt() const { return output_cache_.count() - output_idx_; }
  bool output_equal_rows_directly() { return MY_SPEC.join_type_ <= FULL_OUTER_JOIN; }
  bool need_store_left_unmatch_rows()
  {
    ObJoinType join_type = MY_SPEC.join_type_;
    return (LEFT_OUTER_JOIN == join_type || FULL_OUTER_JOIN == join_type
            || LEFT_ANTI_JOIN == join_type);
  }
  typedef int (ObMergeJoinOp::*state_operation_func_type)();
  typedef int (ObMergeJoinOp::*state_function_func_type)();
private:
  state_operation_func_type state_operation_func_[JS_STATE_COUNT];
  state_function_func_type state_function_func_[JS_STATE_COUNT][FT_TYPE_COUNT];
private:
  ObJoinState state_;

  lib::MemoryContext mem_context_;
  ObChunkDatumStore right_cache_;
  ObChunkDatumStore::Iterator right_cache_iter_;
  ObStoredJoinRow *stored_row_;
  ChildRowFetcher left_fetcher_;
  ChildRowFetcher right_fetcher_;
  ObMJIterateSide empty_cache_iter_side_;
  int64_t equal_cmp_;
  //for anti/semi join, when equal_condition is equal, we should remain until all equal right rows scaned,
  //when right table/cache scan end,
  //for anti join, if equal_condition and other_condition matched, not output this row， else output
  //for semi join, if equal_condition and other_condition matched, output this row, else not output
  bool left_row_matched_;

  // members for batch execution:
  enum BatchJoinState {
    BJS_JOIN_END = 0,
    BJS_JOIN_BEGIN,
    BJS_JOIN_BOTH,
    BJS_OUTPUT_STORE, // when one child is end, output rows in store, then go to OUTPUT_LEFT/RIGHT
    BJS_OUTPUT_LEFT,
    BJS_OUTPUT_RIGHT,
    BJS_STATE_COUNT
  };

  int64_t cmp_res_;
  ObSEArray<RowsListPair, 256, common::ModulePageAllocator, true> match_groups_;
  int64_t output_idx_;
  ObSEArray<RowsPair, 256, common::ModulePageAllocator, true> output_cache_;
  BatchJoinState batch_join_state_;

  ObBatchResultHolder left_first_row_holder_;
  ObBatchResultHolder right_first_row_holder_;

  ChildBatchFecther left_brs_fetcher_;
  ChildBatchFecther right_brs_fetcher_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeJoinOp);
};

} // end namespace sql
} // end namespace oceanbase
#endif
