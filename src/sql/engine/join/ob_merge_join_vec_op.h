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

#ifndef OCEANBASE_SQL_ENGINE_JOIN_OB_MERGE_JOIN_VEC_OP_
#define OCEANBASE_SQL_ENGINE_JOIN_OB_MERGE_JOIN_VEC_OP_

#include "ob_join_vec_op.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/basic/ob_vector_result_holder.h"
#include "sql/engine/ob_phy_operator_type.h"

namespace oceanbase
{
namespace sql
{
class ObMergeJoinVecMemChecker
{
public:
  ObMergeJoinVecMemChecker(int64_t row_cnt):
    cur_row_cnt_(row_cnt)
    {}
  bool operator()(int64_t max_row_cnt)
  {
    return cur_row_cnt_ > max_row_cnt;
  }
  int64_t cur_row_cnt_;
};

class ObMergeJoinVecSpec: public ObJoinVecSpec
{
  OB_UNIS_VERSION_V(1);
public:
  struct EqualConditionInfo {
    OB_UNIS_VERSION(1);
  public:
    EqualConditionInfo()
      : expr_(NULL), ns_cmp_func_(NULL), is_opposite_(false)
    {}
    TO_STRING_KV(K(expr_), K(ns_cmp_func_), K(is_opposite_));

    ObExpr *expr_;
    union {
      NullSafeRowCmpFunc ns_cmp_func_;
      sql::serializable_function ser_eval_func_;
    };
    //表示equal condition 左右子表达式是否分别来自join算子左节点和右节点,
    //如果是, 则is_opposite_ = false, 如果不是, 则is_opposite_ = true;
    bool is_opposite_;
  };

public:
 ObMergeJoinVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
   : ObJoinVecSpec(alloc, type),
     equal_cond_infos_(alloc),
     merge_directions_(alloc),
     left_child_fetcher_all_exprs_(alloc),
     right_child_fetcher_all_exprs_(alloc)
  {}

  virtual ~ObMergeJoinVecSpec() {};
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
  {
    return merge_directions_.push_back(is_ascending_direction(direction) ?
                                       MERGE_DIRECTION_ASC : MERGE_DIRECTION_DESC);
  }

public:
  common::ObFixedArray<EqualConditionInfo, common::ObIAllocator> equal_cond_infos_;
  common::ObFixedArray<int64_t, common::ObIAllocator> merge_directions_;
  ExprFixedArray left_child_fetcher_all_exprs_;
  ExprFixedArray right_child_fetcher_all_exprs_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeJoinVecSpec);
};
struct RowGroup
{
  RowGroup() : start_(-1), end_(-1), cur_(-1) {}
  RowGroup(int64_t start, int64_t end) : start_(start), end_(end), cur_(start) {}
  inline int64_t count() const { return end_ - start_; }
  inline bool has_next() const { return cur_ < end_; }
  inline bool iter_end() const { return cur_ >= end_; }
  inline bool is_empty() const { return end_ == start_ && start_ == -1; }
  inline void rescan() { cur_ = start_; }
  inline void reset() { start_ = end_ = cur_ = -1; }
  int64_t start_;
  int64_t end_;
  int64_t cur_;
  TO_STRING_KV(K(start_), K(end_), K(cur_));
};

struct ObJoinTracker
{
  ObJoinTracker():
  left_row_id_array_(nullptr),
  right_row_id_array_(nullptr),
  last_left_row_id_(-2),
  group_boundary_row_id_array_(nullptr),
  group_boundary_row_id_array_idx_(0),
  cur_group_boundary_row_id_(-2),
  cur_group_idx_(-1),
  last_left_row_matched_(false),
  right_match_all_output_group_idx_(-1),
  row_id_array_size_(0)
  {}

  inline void start_trace()
  {
    group_boundary_row_id_array_idx_ = 0;
    cur_group_boundary_row_id_ = group_boundary_row_id_array_[group_boundary_row_id_array_idx_];
  }

  inline bool reach_cur_group_end(int64_t row_id)
  {
    bool is_crossing = false;
    if (row_id >= 0) {
      is_crossing = row_id >= cur_group_boundary_row_id_;
    } else {
      is_crossing = (row_id == -1 || last_left_row_id_ == -1) && last_left_row_id_ != row_id;
    }
    if (is_crossing) {
      cur_group_boundary_row_id_ = group_boundary_row_id_array_[++group_boundary_row_id_array_idx_];
    }
    return is_crossing;
  }

  inline int set_group_end_row_id(int64_t row_id)
  {
    int ret = OB_SUCCESS;
    if (group_boundary_row_id_array_idx_ < 0 || group_boundary_row_id_array_idx_ > row_id_array_size_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      group_boundary_row_id_array_[group_boundary_row_id_array_idx_++] = row_id;
    }
    return ret;
  }

  inline void clean_row_id_arrays()
  {
    if (OB_NOT_NULL(left_row_id_array_)) {
      MEMSET(left_row_id_array_, 0, sizeof(int64_t) * row_id_array_size_);
    }
    if (OB_NOT_NULL(right_row_id_array_)) {
      MEMSET(right_row_id_array_, 0, sizeof(int64_t) * row_id_array_size_);
    }
    if (OB_NOT_NULL(group_boundary_row_id_array_)) {
      MEMSET(group_boundary_row_id_array_, 0, sizeof(int64_t) * (row_id_array_size_ + 1));
    }
  }

  inline void reset()
  {
    group_boundary_row_id_array_idx_ = 0;
    cur_group_boundary_row_id_ = -2;
    cur_group_idx_ = -1;
    last_left_row_matched_ = false;
    last_left_row_id_ = -2;
    right_match_all_output_group_idx_ = -1;
    clean_row_id_arrays();
  }

  int64_t *left_row_id_array_;
  int64_t *right_row_id_array_;
  int64_t last_left_row_id_;
  int64_t *group_boundary_row_id_array_;
  int64_t group_boundary_row_id_array_idx_;
  int64_t cur_group_boundary_row_id_;
  int64_t cur_group_idx_;
  bool last_left_row_matched_;
  int64_t right_match_all_output_group_idx_;
  int64_t row_id_array_size_;
  TO_STRING_KV(K(left_row_id_array_), K(right_row_id_array_), K(group_boundary_row_id_array_),
  K(last_left_row_id_), K(group_boundary_row_id_array_idx_), K(cur_group_boundary_row_id_),
  K(cur_group_idx_), K(last_left_row_matched_), K(right_match_all_output_group_idx_),
  K(row_id_array_size_));
};

class ObMergeJoinVecOp: public ObJoinVecOp
{
private:
  typedef std::pair<RowGroup, RowGroup> GroupPair;
  typedef std::pair<int64_t, int64_t> RowsPair;
  enum JoinState {
    JOIN_END = 0,
    JOIN_BEGIN,
    JOIN_BOTH,
    MATCH_GROUP_PROCESS,
    OUTPUT_CACHED_ROWS,
    OUTPUT_MATCH_GROUP_ROWS_DIRECT,
    OUTPUT_LEFT_UNTIL_END,
    OUTPUT_RIGHT_UNTIL_END,
    STATE_COUNT
  };
  using EqualCondInfoArray =
      common::ObFixedArray<ObMergeJoinVecSpec::EqualConditionInfo, common::ObIAllocator>;
  using EvalInfoPtrArray = common::ObFixedArray<ObEvalInfo *, common::ObIAllocator>;
  class ObMergeJoinCursor
  {
  public:
    ObMergeJoinCursor(ObIAllocator *allocator, ObMergeJoinVecOp &mj_op, ObEvalCtx &eval_ctx) :
        mj_op_(mj_op),
        cur_batch_idx_(-1),
        next_stored_row_id_(0),
        stored_match_row_cnt_(0),
        cur_brs_(nullptr),
        store_brs_(),
        all_exprs_(nullptr),
        equal_key_exprs_(nullptr),
        store_rows_(nullptr),
        mocked_null_row_(nullptr),
        max_batch_size_(-1),
        source_(nullptr),
        eval_ctx_(eval_ctx),
        row_store_(nullptr),
        row_store_reader_(),
        result_hldr_(),
        small_row_group_(),
        reach_end_(false),
        saved_(false),
        restored_(false),
        allocator_(allocator),
        mem_bound_raito_(0.0),
        equal_key_idx_(allocator),
        col_equal_group_boundary_(nullptr)
    {}
    ~ObMergeJoinCursor() { destroy(); }
    int init(bool is_left, const uint64_t tenant_id, ObOperator *child,
             const ExprFixedArray *all_exprs,
             const EqualCondInfoArray &equal_cond_infos,
             ObIOEventObserver &io_event_observer, double mem_bound_raito);
    int init_row_store(const uint64_t tenant_id, ObIOEventObserver &io_event_observer);
    int init_equal_key_exprs(bool is_left, const EqualCondInfoArray &equal_cond_infos);
    int init_stored_batch_rows();
    int init_col_equal_group_boundary(const EqualCondInfoArray &equal_cond_infos);
    int compare(const ObMergeJoinVecOp::ObMergeJoinCursor &other,
                const common::ObIArray<int64_t> &merge_directions, int &cmp) const;
    inline void row_store_finish_add() { row_store_.finish_add_row(false); }
    int save_cur_batch();
    int restore_cur_batch();
    int eval_all_exprs();
    inline bool is_small_group_empty() { return small_row_group_.is_empty(); }
    RowGroup get_small_group() {
      return RowGroup(small_row_group_.start_, small_row_group_.end_);
    }
    template<bool need_store_equal_group>
    int get_equal_group(RowGroup &group);
    template <bool need_store_uneuqal, bool need_flip>
    int find_small_group(const ObMergeJoinVecOp::ObMergeJoinCursor &other,
                         const common::ObFixedArray<int64_t, common::ObIAllocator> &merge_directions,
                         int &cmp);
    inline int get_equal_group_end_idx_with_store_row(ObCompactRow *l_stored_row,
                                                      int64_t &equal_end_idx,
                                                      bool &all_find);
    inline int get_equal_group_end_idx_in_cur_batch(int64_t &equal_end_idx, bool &all_find);
    inline void reset_small_row_group() { small_row_group_.reset(); }
    inline bool has_next_row() { return !reach_end_; }
    int init_mocked_null_row();
    int get_next_valid_batch();
    int store_one_row(int64_t batch_idx, ObCompactRow *&stored_row);
    int store_rows_of_cur_batch(int64_t &stored_row_cnt);
    const RowMeta &get_row_meta() { return row_store_.get_row_meta(); }
    int start_match();
    inline int init_ouput_vectors(int64_t max_vec_size);
    int get_matching_rows_and_store_ptr(int64_t start_id, int64_t end_id,
                                        int64_t row_ptr_idx,
                                        int64_t *row_id_array,
                                        int64_t row_id_array_size);
    int duplicate_store_row_ptr(int64_t stored_row_id, int64_t ptr_idx,
                                int64_t dup_cnt, int64_t *row_id_array,
                                int64_t row_id_array_size);
    int fill_null_row_ptr(int64_t ptr_idx, int64_t cnt, int64_t *row_id_array);
    int fill_vec_with_stored_rows(int64_t size);
    int get_next_batch_from_source();
    int update_store_mem_bound();
    inline int64_t get_stored_row_cnt() { return row_store_.get_row_cnt(); }
    void set_row_store_it_age(ObTempBlockStore::IterationAge *age) { row_store_reader_.set_iteration_age(age); }
    void clean_row_store()
    {
      row_store_reader_.init(&row_store_);
      row_store_.reuse();
      next_stored_row_id_ = 0;
      stored_match_row_cnt_ = 0;
    }
    // for operator rescan
    void reuse()
    {
      cur_batch_idx_ = -1;
      cur_brs_ = nullptr;
      next_stored_row_id_ = 0;
      stored_match_row_cnt_ = 0;
      row_store_reader_.reset();
      result_hldr_.reset();
      small_row_group_.reset();
      reach_end_ = false;
      saved_ = false;
      restored_ = false;
      if (OB_NOT_NULL(store_rows_)) {
        MEMSET(store_rows_, NULL, sizeof(ObCompactRow *) * max_batch_size_);
      }
      row_store_.reuse();
    }
    void destroy()
    {
      reuse();
      equal_key_idx_.~ObFixedArray();
      if (OB_NOT_NULL(store_brs_.skip_)) {
        allocator_->free(store_brs_.skip_);
        store_brs_.skip_ = nullptr;
      }
      if (OB_NOT_NULL(store_rows_)) {
        allocator_->free(store_rows_);
        store_rows_ = nullptr;
      }
      if (OB_NOT_NULL(col_equal_group_boundary_)) {
        allocator_->free(col_equal_group_boundary_);
        col_equal_group_boundary_ = nullptr;
      }
      if (OB_NOT_NULL(mocked_null_row_)) {
        allocator_->free(mocked_null_row_);
        mocked_null_row_ = nullptr;
      }
      allocator_ = nullptr;
      equal_key_exprs_.reset();
      row_store_.reset();
    }
    TO_STRING_KV(K(cur_batch_idx_), K(cur_brs_), K(reach_end_), K(saved_), K(restored_));

  public:
    ObMergeJoinVecOp &mj_op_;
    int64_t cur_batch_idx_;
    int64_t next_stored_row_id_;
    int64_t stored_match_row_cnt_;
    ObBatchRows *cur_brs_;
    ObBatchRows store_brs_;
    const ExprFixedArray *all_exprs_;
    ExprFixedArray equal_key_exprs_;
    ObCompactRow **store_rows_;
    ObCompactRow *mocked_null_row_;

  private:
    int64_t max_batch_size_;
    ObOperator *source_;
    ObEvalCtx &eval_ctx_;
    ObRATempRowStore row_store_;
    ObRATempRowStore::RAReader row_store_reader_;
    ObVectorsResultHolder result_hldr_;
    RowGroup small_row_group_;
    bool reach_end_;
    bool saved_;
    bool restored_;
    ObIAllocator *allocator_;
    double mem_bound_raito_;
    common::ObFixedArray<int64_t, common::ObIAllocator> equal_key_idx_;
    int64_t *col_equal_group_boundary_;
  };

public:
  ObMergeJoinVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObMergeJoinVecOp() { destroy(); }
  virtual int inner_switch_iterator() override;
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() { return common::OB_NOT_IMPLEMENT; }
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_close() override
  {
    int ret = OB_SUCCESS;
    right_match_cursor_ = nullptr;
    left_match_cursor_ = nullptr;
    is_right_drive_ = false;
    reset();
    if (nullptr != rj_match_vec_) {
      allocator_->free(rj_match_vec_);
      rj_match_vec_ = nullptr;
    }
    if (OB_NOT_NULL(tracker_.left_row_id_array_)) {
      allocator_->free(tracker_.left_row_id_array_);
      tracker_.left_row_id_array_ = nullptr;
    }
    if (OB_NOT_NULL(tracker_.right_row_id_array_)) {
      allocator_->free(tracker_.right_row_id_array_);
      tracker_.right_row_id_array_ = nullptr;
    }
    if (OB_NOT_NULL(tracker_.group_boundary_row_id_array_)) {
      allocator_->free(tracker_.group_boundary_row_id_array_);
      tracker_.group_boundary_row_id_array_ = nullptr;
    }
    left_cursor_.~ObMergeJoinCursor();
    right_cursor_.~ObMergeJoinCursor();
    sql_mem_processor_.unregister_profile();
    allocator_ = nullptr;
    destroy_mem_context();
    return ret;
  }

  void reset()
  {
    join_state_ = JOIN_BEGIN;
    group_idx_ = -1;
    group_pairs_.reset();
    output_cache_.reset();
    left_cursor_.reuse();
    right_cursor_.reuse();
    tracker_.reset();
    cur_right_group_ = nullptr;
    cur_left_group_ = nullptr;
    iter_end_ = false;
    max_output_cnt_ = 0;
    output_cache_idx_ = 0;
    rj_match_vec_size_ = 0;
  }
  virtual void destroy() override
  {
    inner_close();
    ObJoinVecOp::destroy();
  }
private:
  inline const ObMergeJoinVecSpec::EqualConditionInfo& get_equal_cond_info(int cond_idx) const
  {
    return MY_SPEC.equal_cond_infos_.at(cond_idx);
  }

  int init_mem_context();
  void destroy_mem_context()
  {
    if (nullptr != mem_context_) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }
  inline bool has_enough_match_rows()
  {
    return (left_cursor_.stored_match_row_cnt_ + right_cursor_.stored_match_row_cnt_ ) * 0.5 >= max_output_cnt_;
  }
  void set_row_store_it_age(ObTempBlockStore::IterationAge *age)
  {
    if (nullptr != age) {
      age->inc();
    }
    left_cursor_.set_row_store_it_age(age);
    right_cursor_.set_row_store_it_age(age);
  }
  int join_begin();
  template <bool need_store_left_unequal_group,
            bool need_store_left_equal_group,
            bool need_store_right_unequal_group,
            bool need_store_right_equal_group>
  int join_both();
  inline int init_output_vector(int64_t size);
  int output_cached_rows();
  int output_group_pairs();
  int output_one_side_until_end(ObMergeJoinCursor &cursor, ObMergeJoinCursor &blank_cursor);
  int expand_match_flags_if_necessary(const int64_t size);
  int process_dump() { return OB_SUCCESS; };
  int calc_other_cond_and_output_directly(bool &can_output);
  template<ObJoinType join_type>
  int calc_other_cond_and_cache_rows();
  template<bool need_trace>
  int fill_vec_for_calc_other_conds(int64_t &row_cnt, ObBatchRows &brs);
  int match_process(bool &can_output);
  inline void assign_row_group_to_left_right(int64_t group_idx);

private:
  JoinState join_state_;
  lib::MemoryContext mem_context_;
  ObIAllocator *allocator_;
  ObBitVector *rj_match_vec_; // bitmap to check whether it is matched during full outer join
  int64_t rj_match_vec_size_;
  RowGroup *cur_right_group_;
  RowGroup *cur_left_group_;
  ObMergeJoinCursor *right_match_cursor_;
  ObMergeJoinCursor *left_match_cursor_;
  ObSEArray<GroupPair, 256> group_pairs_;
  int64_t group_idx_;
  ObSEArray<RowsPair, 256> output_cache_;
  int64_t output_row_num_;
  ObMergeJoinCursor left_cursor_;
  ObMergeJoinCursor right_cursor_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  bool iter_end_;
  int64_t max_output_cnt_;
  int64_t output_cache_idx_;
  ObJoinTracker tracker_;
  bool is_right_drive_;
  ObTempBlockStore::IterationAge rows_it_age_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeJoinVecOp);
};

} // end namespace sql
} // end namespace oceanbase
#endif
