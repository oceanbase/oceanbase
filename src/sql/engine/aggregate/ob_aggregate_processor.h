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

#ifndef OCEANBASE_SQL_ENGINE_AGGREGATE_PROCESSOR_H
#define OCEANBASE_SQL_ENGINE_AGGREGATE_PROCESSOR_H
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_vector.h"
#include "lib/list/ob_dlist.h"
#include "lib/hash/ob_hashset.h"
#include "sql/engine/expr/ob_expr.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/sort/ob_sort_op_impl.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "lib/string/ob_fixed_length_string.h"
#include "share/stat/ob_topk_hist_estimator.h"
#include "sql/engine/user_defined_function/ob_pl_user_defined_agg_function.h"
#include "sql/engine/expr/ob_expr_dll_udf.h"
#include "sql/engine/expr/ob_rt_datum_arith.h"

namespace oceanbase
{
namespace common
{
  class ObHybridHistograms;
}
namespace sql
{

class ObMaterialOpImpl;

struct RemovalInfo
{
  RemovalInfo()
    : max_min_index_(-1),
      is_index_change_(false),
      is_inv_aggr_(false),
      null_cnt_(0),
      is_out_of_range_(false)
  {
  }
  ~RemovalInfo() {}
  void reset() {
    max_min_index_ = -1;
    is_index_change_ = false;
    is_inv_aggr_ = false;
    null_cnt_ = 0;
    is_out_of_range_ = false;
  }
  void max_min_update(const int64_t max_min_index) {
    if (is_index_change_) {
      max_min_index_ = max_min_index;
      is_index_change_ = false;
    }
  }
  TO_STRING_KV(K_(max_min_index), K_(is_index_change), K_(is_inv_aggr));
  int64_t max_min_index_; // extreme index position
  bool is_index_change_;  // whether the extreme value index position changes
  bool is_inv_aggr_;      // whether the aggregate function support single line inverse
  int64_t null_cnt_;      // count of null in frame for calculating sum
  bool is_out_of_range_;  // whether out of range when calculateing
};

struct ObAggrInfo
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObAggrInfo()
  : alloc_(NULL),
    expr_(NULL),
    real_aggr_type_(T_INVALID),
    has_distinct_(false),
    is_implicit_first_aggr_(false),
    has_order_by_(false),
    param_exprs_(),
    distinct_collations_(),
    distinct_cmp_funcs_(),
    group_concat_param_count_(common::OB_INVALID_COUNT),
    sort_collations_(),
    sort_cmp_funcs_(),
    separator_expr_(NULL),
    window_size_param_expr_(NULL),
    item_size_param_expr_(NULL),
    is_need_deserialize_row_(false),
    pl_agg_udf_type_id_(common::OB_INVALID_ID),
    pl_agg_udf_params_type_(),
    pl_result_type_(),
    dll_udf_(NULL),
    bucket_num_param_expr_(NULL),
    rollup_idx_(INT64_MAX),
    grouping_idxs_(),
    group_idxs_(),
    format_json_(false),
    strict_json_(false),
    absent_on_null_(false),
    returning_type_(INT64_MAX),
    with_unique_keys_(false)
  {}
  ObAggrInfo(common::ObIAllocator &alloc)
  : expr_(NULL),
    real_aggr_type_(T_INVALID),
    has_distinct_(false),
    is_implicit_first_aggr_(false),
    has_order_by_(false),
    param_exprs_(alloc),
    distinct_collations_(alloc),
    distinct_cmp_funcs_(alloc),
    group_concat_param_count_(common::OB_INVALID_COUNT),
    sort_collations_(alloc),
    sort_cmp_funcs_(alloc),
    separator_expr_(NULL),
    window_size_param_expr_(NULL),
    item_size_param_expr_(NULL),
    is_need_deserialize_row_(false),
    pl_agg_udf_type_id_(common::OB_INVALID_ID),
    pl_agg_udf_params_type_(alloc),
    pl_result_type_(),
    bucket_num_param_expr_(NULL),
    grouping_idxs_(),
    group_idxs_(),
    format_json_(false),
    strict_json_(false),
    absent_on_null_(false),
    returning_type_(INT64_MAX),
    with_unique_keys_(false)
  {}
  virtual ~ObAggrInfo();

  inline ObObjType get_first_child_type() const;
  inline bool is_number() const;
  inline bool is_implicit_first_aggr() const { return is_implicit_first_aggr_; }
  int64_t get_child_output_count() const
  {
    return is_implicit_first_aggr() ? 1 : ((T_FUN_COUNT == get_expr_type() && param_exprs_.empty()) ? 0 : param_exprs_.count());
  }
  int eval_aggr(ObChunkDatumStore::ShadowStoredRow &curr_row_results, ObEvalCtx &ctx) const;
  int eval_param_batch(const ObBatchRows &brs, ObEvalCtx &ctx) const;
  inline void set_implicit_first_aggr() { is_implicit_first_aggr_ = true; }
  inline ObExprOperatorType get_expr_type() const
  { return (T_INVALID == real_aggr_type_ && NULL != expr_) ? expr_->type_ : real_aggr_type_; }
  inline void set_allocator(common::ObIAllocator *alloc)
  {
    alloc_ = alloc;
    param_exprs_.set_allocator(alloc);
    distinct_collations_.set_allocator(alloc);
    distinct_cmp_funcs_.set_allocator(alloc);
    sort_collations_.set_allocator(alloc);
    sort_cmp_funcs_.set_allocator(alloc);
    pl_agg_udf_params_type_.set_allocator(alloc);
    grouping_idxs_.set_allocator(alloc);
    group_idxs_.set_allocator(alloc);
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;

  common::ObIAllocator *alloc_;
  ObExpr *expr_;
  ObExprOperatorType real_aggr_type_;//used for wf
  bool has_distinct_;
  bool is_implicit_first_aggr_;
  bool has_order_by_;

  //distinct exprs or group concat/group aggr/keep aggr exprs can use multi exprs.
  ExprFixedArray param_exprs_;//sort expr is also in param_exprs
  ObSortCollations distinct_collations_;
  ObSortFuncs distinct_cmp_funcs_;

  //used for group concat/group aggr/keep aggr
  int64_t group_concat_param_count_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funcs_;
  ObExpr *separator_expr_;

  //used for top_k_fre_hist
  ObExpr *window_size_param_expr_;
  ObExpr *item_size_param_expr_;
  bool is_need_deserialize_row_;

  //for pl agg udf
  int64_t pl_agg_udf_type_id_;
  common::ObFixedArray<ObExprResType, common::ObIAllocator> pl_agg_udf_params_type_;
  ObExprResType pl_result_type_;

  ObAggDllUdfInfo *dll_udf_;
  //used for hybrid_hist
  ObExpr *bucket_num_param_expr_;

  // for grouping
  int64_t rollup_idx_;

  // for grouping_id
  // select grouping(c2,c1) from t1 group by rollup(c1,c2);
  // the idxs of grouping's params are stored in grouping_idxs_;  
  ObFixedArray<int64_t, common::ObIAllocator> grouping_idxs_;
  // for group_id
  // for example: select group_id() from t1 groupby c1, rollup(c1,c2);  the idx of c1 is in group_idxs_;
  ObFixedArray<int64_t, common::ObIAllocator> group_idxs_;

  //used for json aggregate function in oracle mode
  bool format_json_;
  bool strict_json_;
  bool absent_on_null_;
  int64_t returning_type_;
  bool with_unique_keys_;
};

typedef common::ObFixedArray<ObAggrInfo, common::ObIAllocator> AggrInfoFixedArray;

inline ObObjType ObAggrInfo::get_first_child_type() const
{
 // OB_ASSERT(param_exprs_.count() == 1);
  return param_exprs_.at(0)->datum_meta_.type_;
}

inline bool ObAggrInfo::is_number() const
{
//  OB_ASSERT(param_exprs_.count() == 1);
  const ObObjType tmp_type = param_exprs_.at(0)->datum_meta_.type_;
  return (ObNumberType == tmp_type || ObUNumberType == tmp_type || ObNumberFloatType == tmp_type);
}

typedef common::ObFixedArray<ObDatum, common::ObIAllocator> DatumFixedArray;

class ObAggregateProcessor
{
public:
  // Context structure for each aggregation function.
  class IAggrFuncCtx
  {
  public:
    virtual ~IAggrFuncCtx() {}
    virtual int64_t to_string(char *, const int64_t) const { return 0; };
  };

  class LinearInterAggrFuncCtx : public IAggrFuncCtx
  {
  public:
    LinearInterAggrFuncCtx() : IAggrFuncCtx(), linear_inter_(NULL)
    {
    }

    virtual ~LinearInterAggrFuncCtx()
    {
      if (NULL != linear_inter_) {
        linear_inter_->~ObRTDatumArith();
        linear_inter_= NULL;
      }
    }

    ObRTDatumArith *linear_inter_;
  };

  // Context structure for one aggregation function of one group, only some functions need this:
  //  with distinct: need this for distinct calculate
  //  group concat:  need context to hold the input cells.
  //  rank: need context to hold the input cells.
  //  percent_rank: need context to hold the input cells.
  //  dense_rank: need context to hold the input cells.
  //  cume_dist: need context to hold the input cells.
  //  max keep(): need context to hold the input cells.
  //  min keep(): need context to hold the input cells.
  //  sum keep(): need context to hold the input cells.
  //  count keep(): need context to hold the input cells.
  //
  //
  // We only implement distinct logic here, no common interface abstracted here,
  // use the derived class directly.
  class ExtraResult
  {
  public:
    // %alloc is used to initialize the structures, can not be used to hold the data
    explicit ExtraResult(common::ObIAllocator &alloc, ObMonitorNode &op_monitor_info)
      : alloc_(alloc), op_monitor_info_(op_monitor_info), unique_sort_op_(NULL)
    {}
    virtual ~ExtraResult();
    virtual void reuse();
    int init_distinct_set(const uint64_t tenant_id,
                          const ObAggrInfo &aggr_info,
                          ObEvalCtx &eval_ctx,
                          const bool need_rewind,
                          ObIOEventObserver *io_event_observer);
    DECLARE_VIRTUAL_TO_STRING;
  protected:
    common::ObIAllocator &alloc_;
    ObMonitorNode &op_monitor_info_;
  public:
    // for distinct calculate may be replace by hash based distinct in the future.
    ObUniqueSortImpl *unique_sort_op_;
  };

  struct TopKFreHistExtraResult : public ExtraResult
  {
  public:
    TopKFreHistExtraResult(common::ObIAllocator &alloc, ObMonitorNode &op_monitor_info)
      : ExtraResult(alloc, op_monitor_info), topk_fre_hist_() {}
    virtual ~TopKFreHistExtraResult() {}
    virtual void reuse()
    {
      topk_fre_hist_.~ObTopKFrequencyHistograms();
      new (&topk_fre_hist_) ObTopKFrequencyHistograms();
      ExtraResult::reuse();
    }
    ObTopKFrequencyHistograms topk_fre_hist_;
  };

  class GroupConcatExtraResult : public ExtraResult
  {
  public:
    explicit GroupConcatExtraResult(common::ObIAllocator &alloc, ObMonitorNode &op_monitor_info)
      : ExtraResult(alloc, op_monitor_info), row_count_(0), iter_idx_(0), row_store_(ObModIds::OB_SQL_AGGR_FUN_GROUP_CONCAT), sort_op_(NULL), separator_datum_(NULL), bool_mark_(alloc)
    {
    }
    virtual ~GroupConcatExtraResult();
    void reuse_self();
    virtual void reuse() override;

    int init(const uint64_t tenant_id, const ObAggrInfo &aggr_info,
             ObEvalCtx &eval_ctx, const bool need_rewind, int64_t dir_id,
             ObIOEventObserver *io_event_observer);

    int add_row(const ObIArray<ObExpr *> &expr, ObEvalCtx &eval_ctx)
    {
      int ret = sort_op_ != NULL ? sort_op_->add_row(expr) : row_store_.add_row(expr, &eval_ctx);
      if (OB_SUCC(ret)) {
        row_count_++;
      }
      return ret;
    }
    int add_row(const ObChunkDatumStore::StoredRow &sr)
    {
      int ret = sort_op_ != NULL ? sort_op_->add_stored_row(sr) : row_store_.add_row(sr);
      if (OB_SUCC(ret)) {
        row_count_++;
      }
      return ret;
    }
    int get_next_row(const ObChunkDatumStore::StoredRow *&sr)
    {
      int ret = sort_op_ != NULL
                ? sort_op_->get_next_row(sr)
                : row_store_iter_.get_next_row(sr);
      if (OB_SUCC(ret)) {
        iter_idx_++;
      }
      return ret;
    }
    int finish_add_row();

    // iterate twice with rollup process, rewind is needed.
    int rewind();
    bool empty() const { return 0 == row_count_; }
    bool is_iterated() const { return iter_idx_ > 0; }
    int64_t get_row_count() const { return row_count_; }//TODO(jiangxiu.wt): fix it in previous versions
    ObDatum *&get_separator_datum() { return separator_datum_; }
    int reserve_bool_mark_count(int64_t count) { return bool_mark_.reserve(count); }
    int64_t get_bool_mark_size() { return bool_mark_.count(); }//TODO(jiangxiu.wt): fix it in previous versions
    int get_bool_mark(int64_t col_index, bool &is_bool);
    int set_bool_mark(int64_t col_index, bool is_bool);
    DECLARE_VIRTUAL_TO_STRING;

  public:
    int64_t row_count_;
    int64_t iter_idx_;
    ObChunkDatumStore row_store_;
    ObChunkDatumStore::Iterator row_store_iter_;

    ObSortOpImpl *sort_op_;
    ObDatum *separator_datum_;
    common::ObFixedArray<bool, common::ObIAllocator> bool_mark_;
  };

  class HybridHistExtraResult : public ExtraResult
  {
  public:
    explicit HybridHistExtraResult(common::ObIAllocator &alloc, ObMonitorNode &op_monitor_info)
      : ExtraResult(alloc, op_monitor_info),
        sort_row_count_(0),
        material_row_count_(0),
        sort_op_(nullptr),
        mat_op_(nullptr)
    {}
    virtual ~HybridHistExtraResult();
    void reuse_self();
    virtual void reuse() override;

    int init(const uint64_t tenant_id, const ObAggrInfo &aggr_info,
             ObEvalCtx &eval_ctx, const bool need_rewind,
             ObIOEventObserver *io_event_observer, ObSqlWorkAreaProfile &profile,
             ObMonitorNode &op_monitor_info);

    int add_sort_row(const ObIArray<ObExpr *> &expr, ObEvalCtx &eval_ctx);
    int add_sort_row(const ObChunkDatumStore::StoredRow &sr);
    int get_next_row_from_sort(const ObChunkDatumStore::StoredRow *&sr);
    int finish_add_sort_row();
    int add_material_row(const ObDatum *src_datums,
                         const int64_t datum_cnt,
                         const int64_t extra_size,
                         const ObChunkDatumStore::StoredRow *&store_row);
    int get_next_row_from_material(const ObChunkDatumStore::StoredRow *&sr);
    int finish_add_material_row();

    // bool empty() const { return 0 == row_count_; }
    int64_t get_sort_row_count() const { return sort_row_count_; }
    int64_t get_material_row_count() const { return material_row_count_; }
    DECLARE_VIRTUAL_TO_STRING;

  public:
    int64_t sort_row_count_;
    int64_t material_row_count_;

    ObSortOpImpl *sort_op_;
    ObMaterialOpImpl *mat_op_;
  };

  struct DllUdfExtra : public ExtraResult
  {
    explicit DllUdfExtra(common::ObIAllocator &alloc, ObMonitorNode &op_monitor_info)
        : ExtraResult(alloc, op_monitor_info), udf_fun_(NULL)
    {
    }

    virtual ~DllUdfExtra();

    ObAggUdfFunction *udf_fun_;
    ObUdfFunction::ObUdfCtx udf_ctx_;
  };

  class AggrCell
  {
  public:
    AggrCell()
    : tiny_num_int_(0),
      extra_(NULL),
      iter_result_(),
      flags_(0),
      collect_buf_(NULL),
      collect_buf_len_(0),
      need_advance_collect_(false),
      advance_collect_result_()
    {
      iter_result_.set_null();
      advance_collect_result_.set_null();
    }
    ~AggrCell();

    void destroy();

    void inc_row_count() { ++row_count_; }
    void dec_row_count() { --row_count_; }
    int64_t get_row_count() { return row_count_; }
    void add_row_count(const int64_t value) { row_count_ += value; }
    ObDatum &get_iter_result() { return iter_result_; }
    int64_t get_tiny_num_int() { return tiny_num_int_; }
    uint64_t get_tiny_num_uint() { return tiny_num_uint_; }
    void set_iter_result(const ObDatum &value) { iter_result_ = value; }
    void set_tiny_num_int(const int64_t value) { tiny_num_int_ = value; }
    void set_tiny_num_uint(const uint64_t value) { tiny_num_uint_ = value; }
    void set_tiny_num_used() { is_tiny_num_used_ = true; }
    bool is_tiny_num_used() const { return is_tiny_num_used_; }
    bool get_is_evaluated() const { return is_evaluated_; }
    void set_is_evaluated(const bool is_evaluated) { is_evaluated_ = is_evaluated; }
    int collect_result(const ObObjTypeClass tc, ObEvalCtx &eval_ctx, const ObAggrInfo &aggr_info);
    ExtraResult *get_extra() { return extra_; }
    void set_extra(ExtraResult *extra) { extra_ = extra; }
    inline const char* get_buf() { return iter_result_.ptr_
                                          ? (iter_result_.ptr_ - 2 * sizeof(int64_t)) : nullptr; }
    inline void set_buf(char *buf) { iter_result_.ptr_ = buf + 2 * sizeof(int64_t); }
    int64_t to_string(char *buf, const int64_t buf_len) const;
    int deep_copy_advance_collect_result(const ObDatum &datum, ObIAllocator &alloc);
    ObDatum &get_advance_collect_result() { return advance_collect_result_; }
    void set_need_advance_collect() { need_advance_collect_ = true; }
    bool get_need_advance_collect() const { return need_advance_collect_; }
    void set_is_advance_evaluated()  { is_advance_evaluated_ = true; }
    bool get_is_advance_evaluated() const { return is_advance_evaluated_; }
    inline void reuse(const bool release_mem = true)
    {
      UNUSED(release_mem);
      tiny_num_int_ = 0;
      flags_ = 0;
      iter_result_.set_null();
      if (NULL != extra_) {
        extra_->reuse();
      }
      advance_collect_result_.set_null();
    }
    inline void reuse_extra()
    {
      if (NULL != extra_) {
        extra_->reuse();
      }
    }

  private:
    //for int fast path
    union {
      int64_t tiny_num_int_;
      uint64_t tiny_num_uint_;
      //for avg/count
      int64_t row_count_;
    };
    ExtraResult *extra_;
    ObDatum iter_result_;
    union {
      uint32_t flags_;
      struct {
        int32_t is_tiny_num_used_ : 1;
        int32_t is_evaluated_ : 1;
        int32_t is_advance_evaluated_ : 1;
      };
    };
    char *collect_buf_;
    int64_t collect_buf_len_;
    bool need_advance_collect_;
    ObDatum advance_collect_result_;
  };


  struct ObSelector
  {
    ObSelector() : brs_(nullptr), selector_array_(nullptr), count_(0) {}
    ObSelector(const ObBatchRows *brs, const uint16_t *selector_array_, uint16_t count)
      : brs_(brs), selector_array_(selector_array_), count_(count) {}
    ~ObSelector() {}
    bool is_valid() const { return nullptr != selector_array_; }
    uint16_t begin() const {
      return 0;
    }
    uint16_t end() const {
      return count_;
    }
    uint16_t next(uint16_t &i) const
    {
      return ++i;
    }
    uint16_t get_batch_index(uint16_t i) const { return selector_array_[i]; }
    int add_batch(const ObIArray<ObExpr *> *param_exprs, ObSortOpImpl *unique_sort_op,
                  GroupConcatExtraResult *extra_info, ObEvalCtx &eval_ctx) const;
    int add_batch(const ObIArray<ObExpr *> *param_exprs,
                  HybridHistExtraResult *extra_info,
                  ObEvalCtx &eval_ctx) const;
    TO_STRING_KV(K_(count));
    const ObBatchRows *brs_;
    const uint16_t *selector_array_;
    uint16_t count_;
  };

  struct ObBatchRowsSlice // for batch hash group by
  {
    ObBatchRowsSlice() : brs_(nullptr), begin_pos_(0), end_pos_(0) {}
    ObBatchRowsSlice(const ObBatchRows *brs, uint16_t begin_pos, uint16_t end_pos)
    : brs_(brs), begin_pos_(begin_pos), end_pos_(end_pos) {}
    ~ObBatchRowsSlice() {}
    bool is_valid() const { return nullptr != brs_; }
    uint16_t begin() const {
      uint16_t i = begin_pos_;
      while (i < end_pos_ && brs_->skip_->at(i)) {
        ++i;
      }
      return i;
    }
    uint16_t end() const {
      return end_pos_;
    }
    uint16_t next(uint16_t &i) const {
      ++i;
      while (i < end_pos_ && brs_->skip_->at(i)) {
        ++i;
      }
      return i;
    }
    uint16_t get_batch_index(uint16_t i) const { return i; }
    int add_batch(const ObIArray<ObExpr *> *param_exprs, ObSortOpImpl *unique_sort_op,
                  GroupConcatExtraResult *extra_info, ObEvalCtx &eval_ctx) const;
    int add_batch(const ObIArray<ObExpr *> *param_exprs,
                  HybridHistExtraResult *extra_info,
                  ObEvalCtx &eval_ctx) const;
    TO_STRING_KV(K_(begin_pos), K_(end_pos));
    const ObBatchRows *brs_;
    uint16_t begin_pos_;
    uint16_t end_pos_;
  };

  struct GroupRow
  {
    GroupRow()
      : aggr_cells_(nullptr), n_cells_(0), groupby_store_row_(nullptr)  { }
    ~GroupRow()
    {
      destroy();
    }

    TO_STRING_KV(K_(n_cells));
    inline void destroy()
    {
      for (int64_t i = 0; i < n_cells_; ++i) {
        aggr_cells_[i].destroy();
      }
      groupby_store_row_ = nullptr;
    }
    inline void reuse(const bool release_mem = true)
    {
      for (int64_t i = 0; i < n_cells_; ++i) {
        aggr_cells_[i].reuse(release_mem);
      }
      groupby_store_row_ = nullptr;
    }

    AggrCell *aggr_cells_;
    int32_t n_cells_;
    ObChunkDatumStore::StoredRow *groupby_store_row_;
  };

public:
  ObAggregateProcessor(ObEvalCtx &eval_ctx,
                       ObIArray<ObAggrInfo> &aggr_infos,
                       const lib::ObLabel &label,
                       ObMonitorNode &op_monitor_info,
                       const int64_t tenant_id);
  ~ObAggregateProcessor() { destroy(); };

  int init();
  void destroy();
  void reuse();
  int process_batch(const ObBatchRows *brs, GroupRow &group_rows, const uint16_t* selector_array, uint16_t count);
  void set_dir_id(int64_t dir_id) { dir_id_ = dir_id; }
  int process_batch(GroupRow &group_rows, const ObBatchRows &brs, uint16_t begin, uint16_t end);
  int inner_process(GroupRow &group_row, int64_t start_idx, int64_t end_idx, bool is_prepare);

  int prefetch_group_rows(GroupRow &group_rows);
  int prepare(GroupRow &group_row);
  int process(GroupRow &group_row, bool is_prepare = false);
  int collect(const int64_t group_id = 0,
              const ObExpr *diff_expr = NULL,
              const int64_t max_group_cnt = INT64_MIN);
  int collect_group_row(GroupRow *group_row,
                        const int64_t group_id = 0,
                        const ObExpr *diff_expr = NULL,
                        const int64_t max_group_cnt = INT64_MIN);
  OB_INLINE int prepare_in_batch_mode(GroupRow *group_rows);
  // Todo: check group_id, diff_expr usage
  int collect_scalar_batch(const ObBatchRows &brs, const int64_t group_id,
                    const ObExpr *diff_expr, const int64_t max_cnt);
  int collect_result_batch(const ObIArray<ObExpr *> &group_exprs,
                           const int64_t output_batch_size,
                           ObBatchRows &output_brs,
                           int64_t &cur_group_id);
  int advance_collect_result(int64_t cur_group_id);
  int process_distinct_batch(const int64_t group_id,
                            AggrCell &aggr_cell,
                            const ObAggrInfo &aggr_info,
                            const int64_t max_cnt);
  int eval_aggr_param_batch(const ObBatchRows &brs);

  // used by ScalarAggregate operator when there's no input rows
  int collect_for_empty_set();

  inline void set_partial_rollup_idx(int64_t start, int64_t end)
  {
    start_partial_rollup_idx_ = start;
    end_partial_rollup_idx_ = end;
  }
  inline void set_in_window_func() { in_window_func_ = true; }
  inline bool has_distinct() const { return has_distinct_; }
  inline bool has_order_by() const { return has_order_by_; }

  RemovalInfo &get_removal_info() { return removal_info_; }

  inline int64_t get_aggr_used_size() const { return aggr_alloc_.used(); }
  inline int64_t get_aggr_hold_size() const { return aggr_alloc_.total(); }
  inline common::ObIAllocator &get_aggr_alloc() { return aggr_alloc_; }
  inline void set_tenant_id(const uint64_t tenant_id) { aggr_alloc_.set_tenant_id(tenant_id); }
  int generate_group_row(GroupRow *&new_group_row, const int64_t group_id);
  int init_one_group(const int64_t group_id = 0, bool fill_pos = false);
  int init_group_rows(const int64_t num_group_col, bool is_empty = false);
  int rollup_process(const int64_t group_id,
                     const int64_t rollup_group_id,
                     const int64_t max_group_cnt,
                     const ObExpr *diff_expr = NULL);
  int rollup_batch_process(const int64_t group_row_id,
                          const int64_t rollup_group_row_id,
                          int64_t diff_group_idx,
                          const int64_t max_group_cnt = INT64_MIN);
  int rollup_base_process(GroupRow *group_row,
                          GroupRow *rollup_row,
                          const ObExpr *diff_expr /*= NULL*/,
                          int64_t diff_group_idx = -1,
                          const int64_t max_group_cnt = INT64_MIN);
  OB_INLINE int reuse_group(const int64_t group_id,
                            const bool release_mem = true);
  inline int get_group_row(const int64_t group_id, GroupRow *&group_row)
  { return group_rows_.at(group_id, group_row); }
  inline int64_t get_group_rows_count() const//TODO(jiangxiu.wt): fix it in previous versions
  { return group_rows_.count(); }
  inline int swap_group_row(const int a, const int b) // dangerous
  {
    int ret = OB_SUCCESS;
    if (a == b) { // do nothing
    } else if (group_rows_.count() > common::max(a, b)) {
      GroupRow * groupb = group_rows_.at(b);
      group_rows_.at(b) = group_rows_.at(a);
      group_rows_.at(a) = groupb;

    } else {
      ret = OB_ARRAY_OUT_OF_RANGE;
    }
    return ret;
  }
  OB_INLINE int clone_cell(AggrCell &aggr_cell,
                           int64_t need_size,
                           ObDatum *in_target_cell);
  OB_INLINE int clone_aggr_cell(AggrCell &aggr_cell, const ObDatum &src_cell, const bool is_number = false);
  int clone_cell_for_wf(ObDatum &target_cell,
                        const ObDatum &src_cell,
                        const bool is_number/*false*/);
  static int get_llc_size();

  typedef int (ObAggregateProcessor::*process_fun)(GroupRow &group_row);
  typedef int (ObAggregateProcessor::*collect_fun)(const int64_t group_id, const ObExpr *diff_expr);

  void set_rollup_info(
    ObRollupStatus rollup_status,
    ObExpr *rollup_id_expr)
  {
    rollup_status_ = rollup_status;
    rollup_id_expr_ = rollup_id_expr;
  }
  void set_3stage_info(
    const ObThreeStageAggrStage aggr_stage,
    const int64_t aggr_code_idx,
    ObIArray<int64_t> *dist_aggr_group_idxes,
    ObExpr *aggr_code_expr)
  {
    aggr_stage_ = aggr_stage;
    aggr_code_idx_ = aggr_code_idx;
    distinct_aggr_count_ = dist_aggr_group_idxes->count();
    dist_aggr_group_idxes_ = dist_aggr_group_idxes;
    aggr_code_expr_ = aggr_code_expr;
  }
  inline void set_io_event_observer(ObIOEventObserver *observer)
  {
    io_event_observer_ = observer;
  }
  // used in optimizer statistic gathering.
  static int llc_add_value(const uint64_t value, const common::ObString &llc_bitmap_buf);
  inline void set_op_eval_infos(ObIArray<ObEvalInfo *> *eval_infos)
  {
    op_eval_infos_ = eval_infos;
  }
  inline ObIArray<ObAggrInfo> &get_aggr_infos() { return aggr_infos_; }
  int single_row_agg(GroupRow &group_row, ObEvalCtx &eval_ctx);
  int single_row_agg_batch(GroupRow **group_rows, ObEvalCtx &eval_ctx, const int64_t batch_size, const ObBitVector *skip);
  // deal with a single aggr info
  int fast_single_row_agg(ObEvalCtx &eval_ctx, ObAggrInfo &aggr_info);
  // deal with all aggr infos
  int fast_single_row_agg(ObEvalCtx &eval_ctx, ObIArray<ObAggrInfo> &aggr_infos);
  int fast_single_row_agg_batch(ObEvalCtx &eval_ctx, const int64_t batch_size, const ObBitVector *skip);
  inline void set_support_fast_single_row_agg(const bool flag) { support_fast_single_row_agg_ = flag; }
  void set_need_advance_collect() { need_advance_collect_ = true; }
  bool get_need_advance_collect() const { return need_advance_collect_; }
  static int llc_add_value(const uint64_t value, char *llc_bitmap_buf, int64_t size);
private:
  template <typename T>
  int inner_process_batch(GroupRow &group_rows, T &selector, int64_t start_idx, int64_t end_idx);
  template <typename T>
  int inner_process_three_stage_batch(GroupRow &group_rows, T &selector);
  template <typename T>
  int process_aggr_batch_result(
      const ObIArray<ObExpr *> *param_exprs,
      AggrCell &aggr_cell, const ObAggrInfo &aggr_info, const T &param);
  template <typename T>
  int number_accumulator(
      const ObDatumVector &src, ObDataBuffer &allocator1, ObDataBuffer &allocator2,
      number::ObNumber &result, uint32_t *sum_digits, bool &all_skip, const T &param);
  template <typename T>
  int max_calc_batch(
      AggrCell &aggr_cell,
      ObDatum &dst,
      const ObDatumVector &src,
      common::ObDatumCmpFuncType cmp_func,
      const bool is_number,
      const T &param);
  template <typename T>
  int min_calc_batch(
      AggrCell &aggr_cell,
      ObDatum &dst,
      const ObDatumVector &src,
      common::ObDatumCmpFuncType cmp_func,
      const bool is_number,
      const T &param);
  template <typename T>
  int add_calc_batch(
      ObDatum &dst, const ObDatumVector &src,
      AggrCell &aggr_cell, const ObAggrInfo &aggr_info,
      const T &param);
  template <typename T>
  int approx_count_calc_batch(
    ObDatum &dst,
    const ObIArray<ObExpr *> *param_exprs,
    const T &selector
  );
  template <typename T>
  int approx_count_merge_calc_batch(
    ObDatum &dst,
    AggrCell &aggr_cell,
    const ObDatumVector &arg_datums,
    const bool is_number,
    const T &selector
  );
  template <typename T>
  int group_extra_aggr_calc_batch(
    const ObIArray<ObExpr *> *param_exprs,
    AggrCell &aggr_cell,
    const ObAggrInfo &aggr_info,
    GroupConcatExtraResult *extra_info,
    const T &selector
  );
  template <typename T>
  int top_fre_hist_calc_batch(
    const ObAggrInfo &aggr_info,
    TopKFreHistExtraResult *extra_info,
    const ObDatumVector &arg_datums,
    const T &selector
  );
  template <typename T>
  int bitwise_calc_batch(
    const ObDatumVector &src,
    AggrCell &aggr_cell,
    const ObItemType &aggr_func,
    const T &param);
  int precompute_distinct_aggr_result(
    AggrCell &aggr_cell,
    const ObAggrInfo &aggr_info,
    const int64_t max_cnt);
  int extend_concat_str_buf(const ObString &pad_str,
                            const ObCollationType cs_type,
                            const int64_t pos,
                            const int64_t group_concat_cur_row_num,
                            int64_t &append_len,
                            bool &buf_is_full);
  OB_INLINE bool need_extra_info(const ObExprOperatorType expr_type);
  // function members
  int prepare_aggr_result(const ObChunkDatumStore::StoredRow &stored_row,
                          const ObIArray<ObExpr *> *param_exprs,
                          AggrCell &aggr_cell, const ObAggrInfo &aggr_info);
  int process_aggr_result(const ObChunkDatumStore::StoredRow &stored_row,
                          const ObIArray<ObExpr *> *param_exprs,
                          AggrCell &aggr_cell, const ObAggrInfo &aggr_info);
  int collect_aggr_result(AggrCell &aggr_cell,
                          const ObExpr *diff_expr,
                          const ObAggrInfo &aggr_info,
                          const int64_t cur_group_id = 0,
                          const int64_t max_group_cnt = INT64_MIN);
  int process_aggr_result_from_distinct(AggrCell &aggr_cell, const ObAggrInfo &aggr_info);

  OB_INLINE int clone_number_cell(const number::ObNumber &src_cell,
                                  AggrCell &aggr_cell);

  int init_group_extra_aggr_info(
        AggrCell &aggr_cell,
        const ObAggrInfo &aggr_info);
  int max_calc(AggrCell &aggr_cell,
               ObDatum &base,
               const ObDatum &other,
               common::ObDatumCmpFuncType cmp_func,
               const bool is_number);
  int min_calc(AggrCell &aggr_cell,
               ObDatum &base,
               const ObDatum &other,
               common::ObDatumCmpFuncType cmp_func,
               const bool is_number);

  int prepare_add_calc(const ObDatum &iter_value, AggrCell &aggr_cell, const ObAggrInfo &aggr_info);
  int add_calc(const ObDatum &iter_value, AggrCell &aggr_cell, const ObAggrInfo &aggr_info);
  int sub_calc(const ObDatum &iter_value, AggrCell &aggr_cell, const ObAggrInfo &aggr_info);
  int rollup_add_calc(AggrCell &aggr_cell, AggrCell &rollup_cell, const ObAggrInfo &aggr_info);
  int search_op_expr(ObExpr *upper_expr,
                     const ObItemType dst_op,
                     ObExpr *&res_expr);
  int linear_inter_calc(const ObAggrInfo &aggr_info,
                        const ObDatum &prev_datum,
                        const ObDatum &curr_datum,
                        const number::ObNumber &factor,
                        ObDatum &res);
  int get_percentile_param(const ObAggrInfo &aggr_info,
                           const ObDatum &param,
                           const int64_t not_null_start_loc,
                           const int64_t total_row_count,
                           int64_t &dest_loc,
                           bool &need_linear_inter,
                           number::ObNumber &factor,
                           ObDataBuffer &allocator);
  int rollup_add_calc(AggrCell &aggr_cell, AggrCell &rollup_cell);
  int rollup_add_number_calc(ObDatum &aggr_result, AggrCell &aggr_cell);
  int rollup_aggregation(AggrCell &aggr_cell, AggrCell &rollup_cell,
                         const ObExpr *diff_expr, const ObAggrInfo &aggr_info,
                         int64_t cur_rollup_group_idx,
                         const int64_t max_group_cnt = INT64_MIN);
  int rollup_distinct(AggrCell &aggr_cell, AggrCell &rollup_cell);
  int compare_calc(const ObDatum &left_value,
                   const ObDatum &right_value,
                   const ObAggrInfo &aggr_info,
                   int64_t index,
                   int &compare_result,
                   bool &is_asc);
  int check_rows_equal(const ObChunkDatumStore::LastStoredRow &prev_row,
                       const ObChunkDatumStore::StoredRow &cur_row,
                       const ObAggrInfo &aggr_info,
                       bool &is_equal);
  int get_wm_concat_result(const ObAggrInfo &aggr_info,
                           GroupConcatExtraResult *&extra,
                           bool is_keep_group_concat,
                           ObDatum &concat_result);
  int get_pl_agg_udf_result(const ObAggrInfo &aggr_info,
                            GroupConcatExtraResult *&extra,
                            ObDatum &result);
  int convert_datum_to_obj(const ObAggrInfo &aggr_info,
                           const ObChunkDatumStore::StoredRow &stored_row,
                           ObObj *tmp_obj,
                           int64_t obj_cnt);

  int init_topk_fre_histogram_item(const ObAggrInfo &aggr_info,
                                    ObTopKFrequencyHistograms *topk_fre_hist);
  int get_top_k_fre_hist_result(ObTopKFrequencyHistograms &top_k_fre_hist,
                                const ObObjMeta &obj_meta,
                                bool has_lob_header,
                                ObDatum &result_datum);

  int compute_hybrid_hist_result(const ObAggrInfo &aggr_info,
                                 HybridHistExtraResult *&extra,
                                 ObDatum &result);

  int get_hybrid_hist_result(ObHybridHistograms *hybrid_hist,
                             bool has_lob_header,
                             ObDatum &result_datum);

  int get_json_arrayagg_result(const ObAggrInfo &aggr_info,
                               GroupConcatExtraResult *&extra,
                               ObDatum &concat_result);
  int get_json_objectagg_result(const ObAggrInfo &aggr_info,
                                GroupConcatExtraResult *&extra,
                                ObDatum &concat_result);
  int get_ora_json_arrayagg_result(const ObAggrInfo &aggr_info,
                                   GroupConcatExtraResult *&extra,
                                   ObDatum &concat_result);
  int get_ora_json_objectagg_result(const ObAggrInfo &aggr_info,
                                   GroupConcatExtraResult *&extra,
                                   ObDatum &concat_result);

  int get_ora_xmlagg_result(const ObAggrInfo &aggr_info,
                            GroupConcatExtraResult *&extra,
                            ObDatum &concat_result);

  int check_key_valid(common::hash::ObHashSet<ObString> &view_key_names, const ObString& key);

  int shadow_truncate_string_for_hist(const ObObjMeta obj_meta,
                                      ObDatum &datum,
                                      int32_t *origin_str_len = NULL);

  OB_INLINE void clear_op_evaluated_flag()
  {
    if (OB_NOT_NULL(op_eval_infos_)) {
      for (int i = 0; i < op_eval_infos_->count(); i++) {
        op_eval_infos_->at(i)->clear_evaluated_flag();
      }
    }
  }

  // HyperLogLogCount-related functions
  int llc_init(AggrCell &aggr_cell);
  int llc_init_empty(ObExpr &expr, ObEvalCtx &eval_ctx);
  /** (@ banliu.zyd)
   * 对一行计算HyperLogLogCount所需要的hash值，如果行中存在某列有NULL值，has_null_cell会置true
   * @note 需要NULL值判断的原因是APPROX_COUNT_DISTINCT统计时不考虑存在NULL的行，而计算hash值
   * 和判断是否有NULL值可以同时进行以提高效率
   * @param[in] oprands 带计算hash值的行
   * @param[in] cs_type 对于字符串计算hash值时需要的collation
   * @param[out] has_null_cell 为true如果该行某列为NULL值
   * @return 计算出的hash值，如果传出的has_null_cell为true那么这个值无效
   */
  static int llc_calc_hash_value(const ObChunkDatumStore::StoredRow &stored_row,
                                 const ObIArray<ObExpr *> &param_exprs,
                                 bool &has_null_cell,
                                 uint64_t &hash_value);
  static int llc_add(ObDatum &result, const ObDatum &new_value);
  void set_expr_datum_null(ObExpr *expr);

  IAggrFuncCtx *get_aggr_func_ctx(const ObAggrInfo &info) const
  {
    int64_t idx = &info - aggr_infos_.get_data();
    return idx >= 0 && idx < aggr_func_ctxs_.count() ? aggr_func_ctxs_.at(idx) : NULL;
  }

  // HyperLogLogCount-related data members
  // banliu.zyd: hllc算法中桶数这里取相对合理的值(1<<10)。
  static const int8_t LLC_BUCKET_BITS = 10;
  static const int64_t LLC_NUM_BUCKETS = (1 << LLC_BUCKET_BITS);

  //  const static int64_t CONCAT_STR_BUF_LEN  = common::OB_MAX_VARCHAR_LENGTH;
  const static int64_t STORED_ROW_MAGIC_NUM  = 0xaaaabbbbccccdddd;
//  typedef common::hash::ObHashSet<AggrDistinctItem, common::hash::NoPthreadDefendMode> AggrDistinctBucket;
//
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAggregateProcessor);
private:
  static const int64_t GROUP_ROW_SIZE = sizeof(GroupRow);
  static const int64_t GROUP_CELL_SIZE = sizeof(AggrCell);
  static const int64_t BATCH_GROUP_SIZE = 16;
  static const int64_t SHIFT_OFFSET = 63;
  // data members
  bool has_distinct_;
  bool has_order_by_;
  bool has_group_concat_;
  bool in_window_func_;
  bool has_extra_;

  ObEvalCtx &eval_ctx_;
  common::ObArenaAllocator aggr_alloc_;
  int64_t cur_batch_group_idx_;
  char *cur_batch_group_buf_;
  ObIArray<ObAggrInfo> &aggr_infos_;
  ObFixedArray<IAggrFuncCtx *, common::ObIAllocator> aggr_func_ctxs_;
  common::ObSegmentArray<GroupRow *, OB_MALLOC_MIDDLE_BLOCK_SIZE,
                         common::ModulePageAllocator> group_rows_;

  uint64_t concat_str_max_len_;
  uint64_t cur_concat_buf_len_;
  char *concat_str_buf_;
  ObBitVector *skip_;

  // for three stage
  int64_t aggr_code_idx_;
  int64_t distinct_aggr_count_;
  ObIArray<int64_t> *dist_aggr_group_idxes_;
  ObExpr *aggr_code_expr_;
  ObThreeStageAggrStage aggr_stage_;

  // for Rollup Distributor and Rollup Collector
  ObRollupStatus rollup_status_;
  ObExpr *rollup_id_expr_;
  int64_t start_partial_rollup_idx_; // rollup partial idx
  int64_t end_partial_rollup_idx_; // rollup partial idx

  int64_t dir_id_;
  ObChunkDatumStore::ShadowStoredRow *tmp_store_row_;
  ObIOEventObserver *io_event_observer_;
  RemovalInfo removal_info_;
  bool support_fast_single_row_agg_;
  ObIArray<ObEvalInfo *> *op_eval_infos_;
  ObSqlWorkAreaProfile profile_;
  ObMonitorNode &op_monitor_info_;
  bool need_advance_collect_;
};

struct ObAggregateCalcFunc
{
  const static int64_t STORED_ROW_MAGIC_NUM  = 0xaaaabbbbccccdddd;
  static int add_calc(const ObDatum &left_value, const ObDatum &right_value,
      ObDatum &result_datum, const ObObjTypeClass type, ObIAllocator &allocator);
  static int clone_number_cell(const number::ObNumber &src_cell,
      ObDatum &target_cell, ObIAllocator &allocator);
};

OB_INLINE bool ObAggregateProcessor::need_extra_info(const ObExprOperatorType expr_type)
{
  bool need_extra = false;
  switch (expr_type) {
    case T_FUN_GROUP_CONCAT:
    case T_FUN_GROUP_RANK:
    case T_FUN_GROUP_DENSE_RANK:
    case T_FUN_GROUP_PERCENT_RANK:
    case T_FUN_GROUP_CUME_DIST:
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_MEDIAN:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_SUM:
    case T_FUN_KEEP_COUNT:
    case T_FUN_KEEP_WM_CONCAT:
    case T_FUN_WM_CONCAT:
    case T_FUN_PL_AGG_UDF:
    case T_FUN_AGG_UDF:
    case T_FUN_HYBRID_HIST:
    case T_FUN_TOP_FRE_HIST:
    case T_FUN_JSON_ARRAYAGG:
    case T_FUN_ORA_JSON_ARRAYAGG:
    case T_FUN_JSON_OBJECTAGG:
    case T_FUN_ORA_JSON_OBJECTAGG:
    case T_FUN_ORA_XMLAGG:
    {
      need_extra = true;
      break;
    }
    default:
      break;
  }
  return need_extra;
}

OB_INLINE int ObAggregateProcessor::clone_cell(
  AggrCell &aggr_cell,
  int64_t need_size,
  ObDatum *in_target_cell)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t curr_size = 0;
  //length + magic num + data
  ObDatum &target_cell = OB_ISNULL(in_target_cell) ? aggr_cell.get_iter_result() : *in_target_cell;
  // we can't decide reuse memory on target_cell.len_, because for null datum we
  // also have reserved buffer
  if (OB_NOT_NULL(aggr_cell.get_buf()) || 0 < target_cell.len_) {
    if (OB_NOT_NULL(aggr_cell.get_buf())) {
      target_cell.ptr_ = aggr_cell.get_buf() + 2 * sizeof(int64_t);
    }
    void *data_ptr = const_cast<char *>(target_cell.ptr_);
    const int64_t data_length = target_cell.len_;
    if (OB_ISNULL((char *)data_ptr - sizeof(int64_t))
        || OB_ISNULL((char *)data_ptr - sizeof(int64_t) - sizeof(int64_t))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(ERROR, "clone_cell use aggr_alloc, need has meta",
                KP(data_ptr), K(ret));
    } else if (OB_UNLIKELY(*((int64_t *)(data_ptr) - 1) != STORED_ROW_MAGIC_NUM)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(ERROR, "aggr_alloc memory is mismatch, maybe some one make bad things",
                "curr_magic", *((int64_t *)(data_ptr)), K(ret));
    } else if (OB_UNLIKELY((curr_size = *((int64_t *)(data_ptr) - 2)) < data_length)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(ERROR, "target obj size is overflow", K(curr_size),
                "target_size", data_length, K(ret));
    } else {
      if (need_size > curr_size) {
        need_size = need_size * 2;
        void *buff_ptr = NULL;
        if (OB_ISNULL(buff_ptr = static_cast<char*>(aggr_alloc_.alloc(need_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "fall to alloc buff", K(need_size), K(ret));
        } else {
          aggr_cell.set_buf((char*)buff_ptr);
          ((int64_t *)buff_ptr)[0] = need_size;
          ((int64_t *)buff_ptr)[1] = STORED_ROW_MAGIC_NUM;
          buf = (char *)((int64_t *)(buff_ptr) + 2);
          SQL_LOG(DEBUG, "succ to alloc buff", K(need_size), K(target_cell));
        }
      } else {
        buf = (char *)(data_ptr);
      }
    }
  } else {
    void *buff_ptr = NULL;
    if (OB_ISNULL(buff_ptr = static_cast<char*>(aggr_alloc_.alloc(need_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "fall to alloc buff", K(need_size), K(ret));
    } else {
      aggr_cell.set_buf((char*)buff_ptr);
      ((int64_t *)buff_ptr)[0] = need_size;
      ((int64_t *)buff_ptr)[1] = STORED_ROW_MAGIC_NUM;
      buf = (char *)((int64_t *)(buff_ptr) + 2);
      SQL_LOG(DEBUG, "succ to alloc buff", K(need_size), K(target_cell));
    }
  }

  if (OB_SUCC(ret)) {
    // To reuse prealloc memory, we must use specialize deep_copy method
    // Otherwise for null value, we won't reserve its orgin ptr in ObDatum::deep_copy
    target_cell.ptr_ = buf;
  }
  OX(SQL_LOG(DEBUG, "succ to clone cell", K(target_cell), K(curr_size), K(need_size)));
  return ret;
}

// Note: performance critial, aggregation specific
OB_INLINE int ObAggregateProcessor::clone_aggr_cell(AggrCell &aggr_cell, const ObDatum &src_cell,
    const bool is_number/*false*/)
{
  int ret = OB_SUCCESS;
  int64_t need_size = sizeof(int64_t) * 2 +
      (is_number ? number::ObNumber::MAX_BYTE_LEN : src_cell.len_);
  if (OB_FAIL(clone_cell(aggr_cell, need_size, nullptr))) {
    SQL_LOG(WARN, "failed to clone cell", K(ret));
  } else {
    const char *buf = aggr_cell.get_buf() + 2 * sizeof(int64_t);
    memcpy(const_cast<char *> (buf), src_cell.ptr_, src_cell.len_);
    aggr_cell.get_iter_result().pack_ = src_cell.pack_;
  }
  OX(SQL_LOG(DEBUG, "succ to clone cell", K(src_cell), K(need_size)));
  return ret;
}

OB_INLINE int ObAggregateProcessor::prepare_in_batch_mode(GroupRow *group_rows)
{
  int ret = OB_SUCCESS;
  // for sort-based group by operator, for performance reason,
  // after producing a group, we will invoke reuse_group() function to clear the group
  // thus, we do not need to allocate the group space again here, simply reuse the space
  // process aggregate columns

  for (int64_t i = 0; OB_SUCC(ret) && i < group_rows->n_cells_; ++i) {
    const ObAggrInfo &aggr_info = aggr_infos_.at(i);
    AggrCell &aggr_cell = group_rows->aggr_cells_[i];
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "expr info is null", K(aggr_cell), K(ret));
    }
    //OX(LOG_DEBUG("finish prepare", K(aggr_cell)));
  }
  return ret;
}

// TODO: qubin.qb remove release_mem flag and never release memory (always reuse)
// So far Merge Group By vectorization version does NOT release memory and have
// better performance. For non-vectorization version, release memory logic(original)
// is still kept, as its rollup logic makes it too complicated to change.
OB_INLINE int ObAggregateProcessor::reuse_group(const int64_t group_id,
                                                const bool release_mem /* = true */)
{
  int ret = OB_SUCCESS;
  GroupRow *group_row = NULL;
  if (OB_FAIL(group_rows_.at(group_id, group_row))) {
    SQL_LOG(WARN, "fail to get stored row", K(ret));
  } else if (OB_ISNULL(group_row)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "stored_row is null", K(group_row));
  } else {
    group_row->reuse(release_mem);
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_AGGREGATE_PROCESSOR_H */
