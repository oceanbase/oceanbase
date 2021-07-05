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

namespace oceanbase {
namespace sql {

struct ObAggrInfo {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObAggrInfo()
      : expr_(NULL),
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
        separator_datum_(nullptr),
        linear_inter_expr_(NULL)
  {}
  virtual ~ObAggrInfo();

  inline ObObjType get_first_child_type() const;
  inline bool is_number() const;
  inline bool is_implicit_first_aggr() const
  {
    return is_implicit_first_aggr_;
  }
  int64_t get_child_output_count() const
  {
    return is_implicit_first_aggr()
               ? 1
               : ((T_FUN_COUNT == get_expr_type() && param_exprs_.empty()) ? 0 : param_exprs_.count());
  }
  int eval_aggr(ObChunkDatumStore::ShadowStoredRow<>& curr_row_results, ObEvalCtx& ctx) const;
  inline void set_implicit_first_aggr()
  {
    is_implicit_first_aggr_ = true;
  }
  inline ObExprOperatorType get_expr_type() const
  {
    return (T_INVALID == real_aggr_type_ && NULL != expr_) ? expr_->type_ : real_aggr_type_;
  }
  inline void set_allocator(common::ObIAllocator* alloc)
  {
    param_exprs_.set_allocator(alloc);
    distinct_collations_.set_allocator(alloc);
    distinct_cmp_funcs_.set_allocator(alloc);
    sort_collations_.set_allocator(alloc);
    sort_cmp_funcs_.set_allocator(alloc);
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;

  ObExpr* expr_;
  ObExprOperatorType real_aggr_type_;  // used for wf
  bool has_distinct_;
  bool is_implicit_first_aggr_;
  bool has_order_by_;

  // distinct exprs or group concat/group aggr/keep aggr exprs can use multi exprs.
  ExprFixedArray param_exprs_;  // sort expr is also in param_exprs
  ObSortCollations distinct_collations_;
  ObSortFuncs distinct_cmp_funcs_;

  // used for group concat/group aggr/keep aggr
  int64_t group_concat_param_count_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funcs_;
  ObExpr* separator_expr_;
  ObDatum* separator_datum_;

  // used for median/percentile_cont aggr
  ObExpr* linear_inter_expr_;
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

class ObAggregateProcessor {
public:
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
  class ExtraResult {
  public:
    // %alloc is used to initialize the structures, can not be used to hold the data
    explicit ExtraResult(common::ObIAllocator& alloc) : alloc_(alloc), unique_sort_op_(NULL)
    {}
    virtual ~ExtraResult();
    virtual void reuse();
    int init_distinct_set(
        const uint64_t tenant_id, const ObAggrInfo& aggr_info, ObEvalCtx& eval_ctx, const bool need_rewind);
    DECLARE_VIRTUAL_TO_STRING;

  protected:
    common::ObIAllocator& alloc_;

  public:
    // for distinct calculate may be replace by hash based distinct in the future.
    ObUniqueSortImpl* unique_sort_op_;
  };

  class GroupConcatExtraResult : public ExtraResult {
  public:
    explicit GroupConcatExtraResult(common::ObIAllocator& alloc)
        : ExtraResult(alloc), row_count_(0), iter_idx_(0), sort_op_(NULL)
    {}
    virtual ~GroupConcatExtraResult();
    void reuse_self();
    virtual void reuse() override;

    int init(const uint64_t tenant_id, const ObAggrInfo& aggr_info, ObEvalCtx& eval_ctx, const bool need_rewind);

    int add_row(const ObIArray<ObExpr*>& expr, ObEvalCtx& eval_ctx)
    {
      int ret = sort_op_ != NULL ? sort_op_->add_row(expr) : row_store_.add_row(expr, &eval_ctx);
      if (OB_SUCC(ret)) {
        row_count_++;
      }
      return ret;
    }
    int add_row(const ObChunkDatumStore::StoredRow& sr)
    {
      int ret = sort_op_ != NULL ? sort_op_->add_stored_row(sr) : row_store_.add_row(sr);
      if (OB_SUCC(ret)) {
        row_count_++;
      }
      return ret;
    }
    int get_next_row(const ObChunkDatumStore::StoredRow*& sr)
    {
      int ret = sort_op_ != NULL ? sort_op_->get_next_row(sr) : row_store_iter_.get_next_row(sr);
      if (OB_SUCC(ret)) {
        iter_idx_++;
      }
      return ret;
    }
    int finish_add_row();

    // iterate twice with rollup process, rewind is needed.
    int rewind();
    bool empty() const
    {
      return 0 == row_count_;
    }
    bool is_iterated() const
    {
      return iter_idx_ > 0;
    }
    int get_row_count() const
    {
      return row_count_;
    }
    DECLARE_VIRTUAL_TO_STRING;

  private:
    int64_t row_count_;
    int64_t iter_idx_;

    ObChunkDatumStore row_store_;
    ObChunkDatumStore::Iterator row_store_iter_;

    ObSortOpImpl* sort_op_;
  };

  class AggrCell {
  public:
    AggrCell()
        : curr_row_results_(),
          row_count_(0),
          tiny_num_int_(0),
          is_tiny_num_used_(false),
          llc_bitmap_(),
          iter_result_(),
          alloc_(NULL),
          extra_(NULL)
    {
      iter_result_.set_null();
    }
    ~AggrCell();

    void destroy();

    void inc_row_count()
    {
      ++row_count_;
    }
    int64_t get_row_count()
    {
      return row_count_;
    }
    void add_row_count(const int64_t value)
    {
      row_count_ += value;
    }
    ObDatum& get_iter_result()
    {
      return iter_result_;
    }
    int64_t get_tiny_num_int()
    {
      return tiny_num_int_;
    }
    uint64_t get_tiny_num_uint()
    {
      return tiny_num_uint_;
    }
    void set_iter_result(const ObDatum& value)
    {
      iter_result_ = value;
    }
    ObDatum& get_llc_bitmap()
    {
      return llc_bitmap_;
    }
    void set_llc_bitmap(const ObDatum& value)
    {
      llc_bitmap_ = value;
    }
    void set_tiny_num_int(const int64_t value)
    {
      tiny_num_int_ = value;
    }
    void set_tiny_num_uint(const uint64_t value)
    {
      tiny_num_uint_ = value;
    }
    void set_tiny_num_used()
    {
      is_tiny_num_used_ = true;
    }
    bool is_tiny_num_used() const
    {
      return is_tiny_num_used_;
    }
    int collect_result(const ObObjTypeClass tc, ObEvalCtx& eval_ctx, const ObAggrInfo& aggr_info);
    ExtraResult* get_extra()
    {
      return extra_;
    }
    void set_extra(ExtraResult* extra)
    {
      extra_ = extra;
    }
    int64_t to_string(char* buf, const int64_t buf_len) const;
    inline void reuse()
    {
      row_count_ = 0;
      tiny_num_int_ = 0;
      is_tiny_num_used_ = false;
      iter_result_.reset();
      iter_result_.set_null();
      llc_bitmap_.reset();
      if (NULL != extra_) {
        extra_->reuse();
      }
      curr_row_results_.reset();
    }
    inline void set_allocator(common::ObIAllocator* alloc)
    {
      alloc_ = alloc;
      ;
    }

  public:
    ObChunkDatumStore::ShadowStoredRow<> curr_row_results_;

  private:
    // for avg/count
    int64_t row_count_;

    // for int fast path
    union {
      int64_t tiny_num_int_;
      uint64_t tiny_num_uint_;
    };
    bool is_tiny_num_used_;

    // for T_FUN_APPROX_COUNT_DISTINCT
    ObDatum llc_bitmap_;

    ObDatum iter_result_;
    common::ObIAllocator* alloc_;
    ExtraResult* extra_;
  };

  struct GroupRow {
    GroupRow() : aggr_cells_(nullptr), n_cells_(0), groupby_store_row_(nullptr)
    {}
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
    inline void reuse()
    {
      for (int64_t i = 0; i < n_cells_; ++i) {
        aggr_cells_[i].reuse();
      }
      groupby_store_row_ = nullptr;
    }

    AggrCell* aggr_cells_;
    int32_t n_cells_;
    ObChunkDatumStore::StoredRow* groupby_store_row_;
  };

public:
  ObAggregateProcessor(ObEvalCtx& eval_ctx, ObIArray<ObAggrInfo>& aggr_infos);
  ~ObAggregateProcessor()
  {
    destroy();
  };

  int init();
  void destroy();
  void reuse();

  int prepare(GroupRow& group_row);
  int process(GroupRow& group_row);
  int collect(const int64_t group_id = 0, const ObExpr* diff_expr = NULL);

  // used by ScalarAggregate operator when there's no input rows
  int collect_for_empty_set();

  inline void set_in_window_func()
  {
    in_window_func_ = true;
  }
  inline bool has_distinct() const
  {
    return has_distinct_;
  }
  inline bool has_order_by() const
  {
    return has_order_by_;
  }

  inline int64_t get_aggr_used_size() const
  {
    return aggr_alloc_.used();
  }
  inline int64_t get_aggr_hold_size() const
  {
    return aggr_alloc_.total();
  }
  inline common::ObIAllocator& get_aggr_alloc()
  {
    return aggr_alloc_;
  }
  inline void set_tenant_id(const uint64_t tenant_id)
  {
    aggr_alloc_.set_tenant_id(tenant_id);
  }
  int init_one_group(const int64_t group_id = 0);
  int init_group_rows(const int64_t num_group_col);
  int rollup_process(const int64_t group_id, const ObExpr* diff_expr = NULL);
  int reuse_group(const int64_t group_id);
  inline int get_group_row(const int64_t group_id, GroupRow*& group_row)
  {
    return group_rows_.at(group_id, group_row);
  }
  int clone_cell(ObDatum& target_cell, const ObDatum& src_cell, const bool is_number = false);
  static int get_llc_size();

private:
  int extend_concat_str_buf(const ObString& pad_str, const int64_t pos, const int64_t group_concat_cur_row_num,
      int64_t& append_len, bool& buf_is_full);
  OB_INLINE bool need_extra_info(const ObExprOperatorType expr_type);
  // function members
  int prepare_aggr_result(const ObChunkDatumStore::StoredRow& stored_row, const ObIArray<ObExpr*>* param_exprs,
      AggrCell& aggr_cell, const ObAggrInfo& aggr_info);
  int process_aggr_result(const ObChunkDatumStore::StoredRow& stored_row, const ObIArray<ObExpr*>* param_exprs,
      AggrCell& aggr_cell, const ObAggrInfo& aggr_info);
  int collect_aggr_result(AggrCell& aggr_cell, const ObExpr* diff_expr, const ObAggrInfo& aggr_info);
  int process_aggr_result_from_distinct(AggrCell& aggr_cell, const ObAggrInfo& aggr_info);

  int clone_number_cell(const number::ObNumber& src_cell, ObDatum& target_cell);

  int max_calc(ObDatum& base, const ObDatum& other, common::ObDatumCmpFuncType cmp_func, const bool is_number);
  int min_calc(ObDatum& base, const ObDatum& other, common::ObDatumCmpFuncType cmp_func, const bool is_number);
  int prepare_add_calc(const ObDatum& iter_value, AggrCell& aggr_cell, const ObAggrInfo& aggr_info);
  int add_calc(const ObDatum& iter_value, AggrCell& aggr_cell, const ObAggrInfo& aggr_info);
  int rollup_add_calc(AggrCell& aggr_cell, AggrCell& rollup_cell, const ObAggrInfo& aggr_info);
  int search_op_expr(ObExpr* upper_expr, const ObItemType dst_op, ObExpr*& res_expr);
  int linear_inter_calc(const ObAggrInfo& aggr_info, const ObDatum& prev_datum, const ObDatum& curr_datum,
      const number::ObNumber& factor, ObDatum& res);
  int get_percentile_param(const ObAggrInfo& aggr_info, const ObDatum& param, const int64_t not_null_start_loc,
      const int64_t total_row_count, int64_t& dest_loc, bool& need_linear_inter, number::ObNumber& factor,
      ObDataBuffer& allocator);
  int rollup_add_calc(AggrCell& aggr_cell, AggrCell& rollup_cell);
  int rollup_add_number_calc(ObDatum& aggr_result, ObDatum& rollup_result);
  int rollup_aggregation(
      AggrCell& aggr_cell, AggrCell& rollup_cell, const ObExpr* diff_expr, const ObAggrInfo& aggr_info);
  int rollup_distinct(AggrCell& aggr_cell, AggrCell& rollup_cell);
  int compare_calc(const ObDatum& left_value, const ObDatum& right_value, const ObAggrInfo& aggr_info, int64_t index,
      int& compare_result, bool& is_asc);
  int check_rows_equal(const ObChunkDatumStore::LastStoredRow<>& prev_row, const ObChunkDatumStore::StoredRow& cur_row,
      const ObAggrInfo& aggr_info, bool& is_equal);
  int get_wm_concat_result(
      const ObAggrInfo& aggr_info, GroupConcatExtraResult*& extra, bool is_keep_group_concat, ObDatum& concat_result);

  // HyperLogLogCount-related functions
  int llc_init(ObDatum& datum);
  int llc_init_empty(ObExpr& expr, ObEvalCtx& eval_ctx);
  static uint64_t llc_calc_hash_value(
      const ObChunkDatumStore::StoredRow& stored_row, const ObIArray<ObExpr*>& param_exprs, bool& has_null_cell);
  static int llc_add_value(const uint64_t value, const common::ObString& llc_bitmap_buf);
  static int llc_add(ObDatum& result, const ObDatum& new_value);

  static const int8_t LLC_BUCKET_BITS = 10;
  static const int64_t LLC_NUM_BUCKETS = (1 << LLC_BUCKET_BITS);

  //  const static int64_t CONCAT_STR_BUF_LEN  = common::OB_MAX_VARCHAR_LENGTH;
  const static int64_t STORED_ROW_MAGIC_NUM = 0xaaaabbbbccccdddd;
  //  typedef common::hash::ObHashSet<AggrDistinctItem, common::hash::NoPthreadDefendMode> AggrDistinctBucket;
  //
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAggregateProcessor);

private:
  static const int64_t GROUP_ROW_SIZE = sizeof(GroupRow);
  static const int64_t GROUP_CELL_SIZE = sizeof(AggrCell);
  // data members
  bool has_distinct_;
  bool has_order_by_;
  bool has_group_concat_;
  bool in_window_func_;
  bool has_extra_;

  ObEvalCtx& eval_ctx_;
  common::ObArenaAllocator aggr_alloc_;
  ObIArray<ObAggrInfo>& aggr_infos_;
  common::ObSEArray<GroupRow*, 1> group_rows_;

  uint64_t concat_str_max_len_;
  uint64_t cur_concat_buf_len_;
  char* concat_str_buf_;
  //  common::ObArenaAllocator aggr_udf_buf_;
  //  common::ObSEArray<ObAggUdfMeta, 16> aggr_udf_metas_;
  //  common::hash::ObHashMap<int64_t, ObAggUdfExeUnit, common::hash::NoPthreadDefendMode> aggr_udf_;
};

// Used for calc hash for columns
class ObGroupRowItem {
public:
  ObGroupRowItem() : group_id_(0), group_row_ptr_(NULL), groupby_datums_hash_(0), next_(NULL)
  {}

  ~ObGroupRowItem()
  {}
  inline uint64_t hash() const
  {
    return groupby_datums_hash_;
  }
  ObGroupRowItem*& next()
  {
    return *reinterpret_cast<ObGroupRowItem**>(&next_);
  };

  TO_STRING_KV(K_(group_id), KPC_(group_row), K_(groupby_datums_hash), KP_(group_exprs), KP_(next));

public:
  int64_t group_id_;
  union {
    void* group_row_ptr_;
    ObAggregateProcessor::GroupRow* group_row_;
    ExprFixedArray* group_exprs_;
  };
  uint64_t groupby_datums_hash_;
  void* next_;
};

class ObGroupRowHashTable : public ObExtendHashTable<ObGroupRowItem> {
public:
  ObGroupRowHashTable() : ObExtendHashTable(), eval_ctx_(nullptr), cmp_funcs_(nullptr)
  {}

  const ObGroupRowItem* get(const ObGroupRowItem& item) const;
  int init(ObIAllocator* allocator, lib::ObMemAttr& mem_attr, ObEvalCtx* eval_ctx,
      const common::ObIArray<ObCmpFunc>* cmp_funcs, int64_t initial_size = INITIAL_SIZE);

private:
  bool compare(const ObGroupRowItem& left, const ObGroupRowItem& right) const;

private:
  ObEvalCtx* eval_ctx_;
  const common::ObIArray<ObCmpFunc>* cmp_funcs_;
};

struct ObAggregateCalcFunc {
  const static int64_t STORED_ROW_MAGIC_NUM = 0xaaaabbbbccccdddd;
  static int add_calc(const ObDatum& left_value, const ObDatum& right_value, ObDatum& result_datum,
      const ObObjTypeClass type, ObIAllocator& allocator);
  static int clone_number_cell(const number::ObNumber& src_cell, ObDatum& target_cell, ObIAllocator& allocator);
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
    case T_FUN_WM_CONCAT: {
      need_extra = true;
      break;
    }
    default:
      break;
  }
  return need_extra;
}

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_AGGREGATE_PROCESSOR_H */
