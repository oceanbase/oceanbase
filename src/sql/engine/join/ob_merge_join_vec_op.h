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
     right_child_fetcher_all_exprs_(alloc),
     left_child_fetcher_equal_keys_(alloc),
     right_child_fetcher_equal_keys_(alloc),
     left_child_fetcher_equal_keys_idx_(alloc),
     right_child_fetcher_equal_keys_idx_(alloc)
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
  int init_equal_keys(const int equal_conds_count)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(left_child_fetcher_equal_keys_.init(equal_conds_count))) {
      LOG_WARN("init left_child_fetcher_equal_keys_ failed", K(ret));
    } else if (OB_FAIL(right_child_fetcher_equal_keys_.init(equal_conds_count))) {
      LOG_WARN("init right_child_fetcher_equal_keys_ failed", K(ret));
    } else if (OB_FAIL(left_child_fetcher_equal_keys_idx_.init(equal_conds_count))) {
      LOG_WARN("init left_child_fetcher_equal_keys_idx_ failed", K(ret));
    } else if (OB_FAIL(right_child_fetcher_equal_keys_idx_.init(equal_conds_count))) {
      LOG_WARN("init right_child_fetcher_equal_keys_idx_ failed", K(ret));
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
  ExprFixedArray left_child_fetcher_equal_keys_;
  ExprFixedArray right_child_fetcher_equal_keys_;
  common::ObFixedArray<int64_t, common::ObIAllocator> left_child_fetcher_equal_keys_idx_;
  common::ObFixedArray<int64_t, common::ObIAllocator> right_child_fetcher_equal_keys_idx_;
  
private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeJoinVecSpec);
};
struct RowGroup
{
  RowGroup() : start_(-1), end_(-1), cur_(-1), calced_size_(0) {}
  RowGroup(int64_t start, int64_t end) : start_(start), end_(end), cur_(start), calced_size_(0) {}
  inline void copy(RowGroup *other)
  {
    start_ = other->start_;
    end_ = other->end_;
    cur_ = start_;
    calced_size_ = 0;
  }
  inline int64_t count() { return end_ - start_; }
  inline bool iter_end() const { return (cur_ >= end_); }
  inline bool is_empty() const { return end_ == start_ && start_ == -1; }
  inline void rescan() { cur_ = start_; }
  inline void reset() { start_ = end_ = cur_ = -1; }
  inline bool all_calced() { return calced_size_ == end_ - start_; }
  int64_t start_;
  int64_t end_;
  int64_t cur_;
  int64_t calced_size_;
  TO_STRING_KV(K(start_), K(end_), K(cur_), K(calced_size_));
};

class ObMergeJoinVecOp: public ObJoinVecOp
{
public:
  typedef std::pair<RowGroup, RowGroup> GroupPair;
  typedef std::pair<int64_t, int64_t> RowPair;
  class ObMergeJoinCursor;
  struct ObJoinTracker
  {
    ObJoinTracker(ObMergeJoinVecOp &mj_op,
                  const ObJoinType join_type,
                  const bool is_right_drive,
                  ObMergeJoinCursor *left_match_cursor,
                  ObMergeJoinCursor *right_match_cursor)
        : join_type_(join_type),
          is_right_drive_(is_right_drive),
          allocator_(mj_op.allocator_),
          group_pairs_(mj_op.group_pairs_),
          output_cache_(mj_op.output_cache_),
          left_match_cursor_(left_match_cursor),
          right_match_cursor_(right_match_cursor),
          cur_left_group_(nullptr),
          cur_right_group_(nullptr),
          cur_group_idx_(-1) {}
    virtual ~ObJoinTracker()
    {
      left_match_cursor_ = nullptr;
      right_match_cursor_ = nullptr;
      cur_left_group_ = nullptr;
      cur_right_group_ = nullptr;
      allocator_ = nullptr;
      cur_group_idx_ = -1;
    }
    virtual int fill_match_pair(int64_t max_pair_cnt, ObBatchRows &brs) = 0;
    virtual int match_proc(ObBatchRows &brs) = 0;
    inline int next_group_pair()
    {
      int ret = OB_SUCCESS;
      if (++cur_group_idx_ < group_pairs_.count()) {
        cur_left_group_ = is_right_drive_ ? &group_pairs_.at(cur_group_idx_).second
                                          : &group_pairs_.at(cur_group_idx_).first;
        cur_right_group_ = is_right_drive_
                              ? &group_pairs_.at(cur_group_idx_).first
                              : &group_pairs_.at(cur_group_idx_).second;
      } else {
        ret = OB_ITER_END;
      }
      return ret;
    }
    virtual void reuse() = 0;
 
  protected:
    const ObJoinType join_type_;
    const bool is_right_drive_;
    ObIAllocator *allocator_;
    ObSEArray<GroupPair, 256> &group_pairs_;
    ObSEArray<RowPair, 256> &output_cache_;
    ObMergeJoinCursor *left_match_cursor_;
    ObMergeJoinCursor *right_match_cursor_;
    RowGroup *cur_left_group_;
    RowGroup *cur_right_group_;
    int64_t cur_group_idx_;
  };
  struct ObCommonJoinTracker : public ObJoinTracker
  {
    ObCommonJoinTracker(ObMergeJoinVecOp &mj_op, const ObJoinType join_type,
                        const bool need_trace, const bool is_right_drive,
                        ObMergeJoinCursor *left_match_cursor,
                        ObMergeJoinCursor *right_match_cursor)
        : ObJoinTracker(mj_op, join_type, is_right_drive, left_match_cursor,
                        right_match_cursor),
          need_trace_(need_trace), left_row_id_array_(nullptr),
          right_row_id_array_(nullptr), last_left_row_id_(-2),
          group_boundary_row_id_array_(nullptr),
          group_boundary_row_id_array_idx_(0), cur_group_boundary_row_id_(-2),
          trace_group_idx_(-1), last_left_row_matched_(false),
          right_match_all_output_group_idx_(-1), row_id_array_size_(0),
          rj_match_vec_(nullptr), rj_match_vec_size_(0) {}

    ~ObCommonJoinTracker()
    {
      reuse();
      if (nullptr != rj_match_vec_) {
        allocator_->free(rj_match_vec_);
        rj_match_vec_ = nullptr;
      }
      if (OB_NOT_NULL(left_row_id_array_)) {
        allocator_->free(left_row_id_array_);
        left_row_id_array_ = nullptr;
      }
      if (OB_NOT_NULL(right_row_id_array_)) {
        allocator_->free(right_row_id_array_);
        right_row_id_array_ = nullptr;
      }
      if (OB_NOT_NULL(group_boundary_row_id_array_)) {
        allocator_->free(group_boundary_row_id_array_);
        group_boundary_row_id_array_ = nullptr;
      }
    }
    int init(int64_t max_batch_size);
    inline void start_trace()
    {
      group_boundary_row_id_array_idx_ = 0;
      cur_group_boundary_row_id_ = group_boundary_row_id_array_[group_boundary_row_id_array_idx_];
    }
    int fill_match_pair(int64_t max_pair_cnt, ObBatchRows &brs) override final;
    int match_proc(ObBatchRows &brs) override final;
    int init_match_flags();
    int expand_match_flags_if_necessary(const int64_t size);
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
      if (group_boundary_row_id_array_idx_ < 0 ||
          group_boundary_row_id_array_idx_ > row_id_array_size_) {
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
    inline bool all_groups_consumed() { return cur_group_idx_ >= group_pairs_.count(); }
    inline void reuse() override final
    {
      group_boundary_row_id_array_idx_ = 0;
      cur_group_boundary_row_id_ = -2;
      cur_group_idx_ = -1;
      trace_group_idx_ = -1;
      last_left_row_matched_ = false;
      last_left_row_id_ = -2;
      right_match_all_output_group_idx_ = -1;
      clean_row_id_arrays();
    }
    const bool need_trace_;
    int64_t *left_row_id_array_;
    int64_t *right_row_id_array_;
    int64_t last_left_row_id_;
    int64_t *group_boundary_row_id_array_;
    int64_t group_boundary_row_id_array_idx_;
    int64_t cur_group_boundary_row_id_;
    int64_t trace_group_idx_;
    bool last_left_row_matched_;
    int64_t right_match_all_output_group_idx_;
    int64_t row_id_array_size_;
    ObBitVector *rj_match_vec_; // bitmap to check whether it is matched during full outer join
    int64_t rj_match_vec_size_;
    TO_STRING_KV(K(join_type_), K(is_right_drive_), K(need_trace_),
                 K(left_row_id_array_), K(right_row_id_array_),
                 K(group_boundary_row_id_array_), K(last_left_row_id_),
                 K(group_boundary_row_id_array_idx_),
                 K(cur_group_boundary_row_id_), K(cur_group_idx_),
                 K(last_left_row_matched_),
                 K(right_match_all_output_group_idx_), K(row_id_array_size_));
  };

  struct ObSemiAntiJoinTracker : public ObJoinTracker
  {
    struct SemiAntiMatchPair
    {
      SemiAntiMatchPair()
          : is_semi_join_(false), left_row_matched_(false), left_row_id_(-2),
            right_group_(), cur_left_group_(nullptr), tracker_(nullptr) {}
      void init(bool is_semi_join, ObSemiAntiJoinTracker *tracker)
      {
        is_semi_join_ = is_semi_join;
        tracker_ = tracker;
        reuse();
      }
      void match() { left_row_matched_ = true; }
      int next_left_row(int64_t vec_idx);
      int next_row_pair(bool need_trace, int64_t vec_idx, bool &calc_skip, bool &iter_end);
      int get_next_group_pair();
      void reuse()
      {
        left_row_matched_ = false;
        left_row_id_ = -2;
        right_group_.reset();
        cur_left_group_ = nullptr;
      }
      bool is_semi_join_;
      bool left_row_matched_;
      int64_t left_row_id_;
      RowGroup right_group_;
      RowGroup *cur_left_group_;
      ObSemiAntiJoinTracker *tracker_;
    };
    struct RowidReverseCompartor
    {
      bool operator()(const int64_t &lhs, const int64_t &rhs) const
      { return lhs > rhs; }
    };
    ObSemiAntiJoinTracker(ObMergeJoinVecOp &mj_op, ObJoinType join_type,
                          bool is_right_drive,
                          ObMergeJoinCursor *left_match_cursor,
                          ObMergeJoinCursor *right_match_cursor)
        : ObJoinTracker(mj_op, join_type, is_right_drive, left_match_cursor,
                        right_match_cursor),
          semi_anti_match_pair_array_(nullptr),
          semi_anti_match_pair_ptr_array_(nullptr), intermediate_cache_(),
          output_group_idx_(0), match_pair_cnt_(0), match_pair_array_size_(0) {}
    ~ObSemiAntiJoinTracker()
    {
      reuse();
      if (OB_NOT_NULL(semi_anti_match_pair_array_)) {
        allocator_->free(semi_anti_match_pair_array_);
        semi_anti_match_pair_array_ = nullptr;
      }
      if (OB_NOT_NULL(semi_anti_match_pair_ptr_array_)) {
        allocator_->free(semi_anti_match_pair_ptr_array_);
        semi_anti_match_pair_ptr_array_ = nullptr;
      }
    }
    int init(int64_t tenant_id, int64_t max_batch_size);
    int fill_match_pair(int64_t max_pair_cnt, ObBatchRows &brs) override final;
    int match_proc(ObBatchRows &brs) override final;
    inline int get_cur_group(int64_t &group_idx, RowGroup *&left, RowGroup *&right)
    {
      int ret = OB_SUCCESS;
      if (cur_group_idx_ < group_pairs_.count()) {
        group_idx = cur_group_idx_;
        left = is_right_drive_ ? &group_pairs_.at(group_idx).second
                               : &group_pairs_.at(group_idx).first;
        right = is_right_drive_ ? &group_pairs_.at(group_idx).first
                                : &group_pairs_.at(group_idx).second;
      } else {
        ret = OB_ITER_END;
      }
      return ret;
    }
    inline int get_group(int64_t group_idx, RowGroup *&left, RowGroup *&right)
    {
      int ret = OB_SUCCESS;
      left = is_right_drive_ ? &group_pairs_.at(group_idx).second
                             : &group_pairs_.at(group_idx).first;
      right = is_right_drive_ ? &group_pairs_.at(group_idx).first
                              : &group_pairs_.at(group_idx).second;
      return ret;
    }
    inline int reorder_output_rows();
    inline void reuse() override final
    {
      match_pair_cnt_ = match_pair_array_size_;
      cur_group_idx_ = -1;
      output_group_idx_ = 0;
      if (OB_NOT_NULL(semi_anti_match_pair_array_)) {
        for (int i = 0; i < match_pair_array_size_; ++i) {
          semi_anti_match_pair_array_[i].reuse();
        }
      }
      if (OB_NOT_NULL(semi_anti_match_pair_ptr_array_)) {
        MEMSET(semi_anti_match_pair_ptr_array_, NULL,
               sizeof(SemiAntiMatchPair *) * match_pair_array_size_);
      }
      if (OB_NOT_NULL(semi_anti_match_pair_array_)) {
        for (int64_t i = 0; i < match_pair_array_size_; ++i) {
          semi_anti_match_pair_ptr_array_[i] = &semi_anti_match_pair_array_[i];
        }
      }
      intermediate_cache_.reuse();
    }
    SemiAntiMatchPair *semi_anti_match_pair_array_;
    SemiAntiMatchPair **semi_anti_match_pair_ptr_array_;
    ObSEArray<int64_t, 256> intermediate_cache_; // for semi join or anti join, to reorder output
    int64_t output_group_idx_;
    int64_t match_pair_cnt_;
    int64_t match_pair_array_size_;
    TO_STRING_KV(K(is_right_drive_), K(cur_left_group_), K(cur_right_group_),
                 K(join_type_),
                 KP(semi_anti_match_pair_array_),
                 KP(semi_anti_match_pair_ptr_array_),
                 K(cur_group_idx_), K(group_pairs_),
                 K(output_cache_),
                 K(match_pair_cnt_),
                 K(match_pair_array_size_));
  };
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
        equal_key_exprs_arry_ptr_(nullptr),
        equal_key_idx_arry_ptr_(nullptr),
        col_equal_group_boundary_(nullptr)
    {}
    ~ObMergeJoinCursor() { destroy(); }
    int init(bool is_left, const uint64_t tenant_id, ObOperator *child,
             const ExprFixedArray *all_exprs,
             const ExprFixedArray *equal_keys,
             const common::ObFixedArray<int64_t, common::ObIAllocator> *key_idx,
             const EqualCondInfoArray &equal_cond_infos,
             ObIOEventObserver &io_event_observer, double mem_bound_raito);
    int init_equal_key_exprs(bool is_left, const EqualCondInfoArray &equal_cond_infos);
    inline int init_row_store(const uint64_t tenant_id, ObIOEventObserver &io_event_observer);
    inline int init_stored_batch_rows();
    inline int init_store_rows_array();
    inline int init_col_equal_group_boundary();
    int compare(const ObMergeJoinVecOp::ObMergeJoinCursor &other,
                const common::ObIArray<int64_t> &merge_directions, int &cmp) const;
    inline void row_store_finish_add() { row_store_.finish_add_row(false); }
    int save_cur_batch();
    int restore_cur_batch();
    int eval_all_exprs();
    inline bool is_small_group_empty() { return small_row_group_.is_empty(); }
    RowGroup get_small_group()
    {
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
    inline int init_ouput_vectors(int64_t max_vec_size);
    int flat_group(int64_t start_id, int64_t cnt,
                   int64_t row_ptr_idx,
                   int64_t *row_id_array);
    int duplicate_store_row_ptr(int64_t stored_row_id, int64_t ptr_idx,
                                int64_t dup_cnt, int64_t *row_id_array);
    void flat_group_with_null_as_placeholder(int64_t ptr_idx, int64_t cnt,
                                             int64_t *row_id_array,
                                             int64_t start_row_id,
                                             ObBatchRows &brs);
    void fill_null_row_ptr(int64_t ptr_idx, int64_t cnt, int64_t *row_id_array);
    int fill_vec_with_stored_rows(int64_t size);
    int get_next_batch_from_source(int64_t batch_size);
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
      equal_key_idx_arry_ptr_ = nullptr;
      equal_key_exprs_arry_ptr_ = nullptr;
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
      equal_key_idx_.~ObFixedArray();
      equal_key_exprs_.~ObFixedArray();
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
    int64_t max_batch_size_;
  private:
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
    const ExprFixedArray *equal_key_exprs_arry_ptr_;
    const common::ObFixedArray<int64_t, common::ObIAllocator> *equal_key_idx_arry_ptr_;
    int64_t *col_equal_group_boundary_;
  };
  
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
    reset();
    if (OB_NOT_NULL(tracker_)) {
      tracker_->reuse();
      if (MY_SPEC.join_type_ >= LEFT_SEMI_JOIN &&
          MY_SPEC.join_type_ <= RIGHT_ANTI_JOIN &&
          MY_SPEC.other_join_conds_.count() > 0) {
        ObSemiAntiJoinTracker *tracker = static_cast<ObSemiAntiJoinTracker *>(tracker_);
        tracker->reuse();
        tracker->~ObSemiAntiJoinTracker();
      } else {
        ObCommonJoinTracker *tracker = static_cast<ObCommonJoinTracker *>(tracker_);
        tracker->reuse();
        tracker->~ObCommonJoinTracker();
      }
      allocator_->free(tracker_);
      tracker_ = nullptr;
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
    group_pairs_.reset();
    output_cache_.reset();
    left_cursor_.reuse();
    right_cursor_.reuse();
    if (OB_NOT_NULL(tracker_)) {
      tracker_->reuse();
    }
    iter_end_ = false;
    max_output_cnt_ = 0;
    output_cache_idx_ = 0;
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
    return (left_cursor_.stored_match_row_cnt_ +
            right_cursor_.stored_match_row_cnt_) *
               0.1 >= max_output_cnt_;
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
  int output_one_side_until_end(ObMergeJoinCursor &cursor, ObMergeJoinCursor &blank_cursor);
  int process_dump() { return OB_SUCCESS; };
  int calc_other_cond_and_output_directly(bool &can_output);
  int calc_other_cond_and_cache_rows();
  int flat_group_pair_and_project_onto_vec(ObBatchRows &brs);
  int match_process(bool &can_output);

private:
  JoinState join_state_;
  lib::MemoryContext mem_context_;
  ObIAllocator *allocator_;
  ObMergeJoinCursor *right_match_cursor_;
  ObMergeJoinCursor *left_match_cursor_;
  ObSEArray<GroupPair, 256> group_pairs_;
  ObSEArray<RowPair, 256> output_cache_;
  int64_t output_row_num_;
  ObMergeJoinCursor left_cursor_;
  ObMergeJoinCursor right_cursor_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  bool iter_end_;
  int64_t max_output_cnt_;
  int64_t output_cache_idx_;
  ObJoinTracker *tracker_;
  ObTempBlockStore::IterationAge rows_it_age_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeJoinVecOp);
};

} // end namespace sql
} // end namespace oceanbase
#endif
