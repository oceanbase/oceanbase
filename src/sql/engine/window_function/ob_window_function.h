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

#ifndef _OB_WINDOW_FUNCTION_H
#define _OB_WINDOW_FUNCTION_H 1

#include "lib/container/ob_array.h"
#include "lib/container/ob_fixed_array.h"
#include "common/row/ob_row.h"
#include "sql/parser/ob_item_type.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/aggregate/ob_aggregate_function.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"

namespace oceanbase {
namespace common {
class ObExprCtx;
}
namespace sql {
class ObPhysicalPlan;
class ObSqlExpression;
struct ExtBound {
  OB_UNIS_VERSION_V(1);

public:
  ExtBound() : my_phy_plan_(NULL), is_preceding_(false), is_unbounded_(false), is_nmb_literal_(false), sql_expr_(NULL)
  {
    MEMSET(sql_exprs_, 0, sizeof(ObSqlExpression*) * BOUND_EXPR_MAX);
  }
  TO_STRING_EMPTY();
  ObPhysicalPlan* my_phy_plan_;
  bool is_preceding_;
  bool is_unbounded_;
  bool is_nmb_literal_;
  ObSqlExpression* sql_expr_;
  ObSqlExpression* sql_exprs_[BOUND_EXPR_MAX];
};
struct FuncInfo {
  OB_UNIS_VERSION_V(1);

public:
  FuncInfo()
      : my_phy_plan_(NULL),
        func_type_(T_MAX),
        aggr_column_(NULL),
        is_distinct_(false),
        is_ignore_null_(false),
        is_from_first_(false),
        win_type_(WINDOW_MAX),
        result_index_(-1)
  {}

  virtual ~FuncInfo()
  {}

  inline int set_allocator(common::ObIAllocator* alloc)
  {
    int ret = common::OB_SUCCESS;
    params_.set_allocator(alloc);
    partition_cols_.set_allocator(alloc);
    sort_cols_.set_allocator(alloc);
    return ret;
  }

  inline int init(int64_t params_cnt, int64_t cols_cnt, int64_t sort_cnt)
  {
    int ret = common::OB_SUCCESS;
    if (params_cnt < 0 || cols_cnt < 0 || sort_cnt < 0) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid cnt", K(params_cnt), K(cols_cnt), K(sort_cnt));
    } else if (params_cnt > 0 && OB_FAIL(params_.init(params_cnt))) {
      SQL_ENG_LOG(WARN, "fail to init array", K(ret), K(params_cnt));
    } else if (cols_cnt > 0 && OB_FAIL(partition_cols_.init(cols_cnt))) {
      SQL_ENG_LOG(WARN, "fail to init array", K(ret), K(cols_cnt));
    } else if (sort_cnt > 0 && OB_FAIL(sort_cols_.init(sort_cnt))) {
      SQL_ENG_LOG(WARN, "fail to init array", K(ret), K(sort_cnt));
    } else { /* do nothing. */
    }
    return ret;
  }

  TO_STRING_KV(K_(func_type), K_(win_type), K_(is_distinct), K_(is_ignore_null), K_(upper), K_(lower), K_(result_index),
      K_(partition_cols), K_(sort_cols));
  ObPhysicalPlan* my_phy_plan_;
  ObItemType func_type_;
  ObAggregateExpression* aggr_column_;
  bool is_distinct_;
  bool is_ignore_null_;
  bool is_from_first_;
  common::ObFixedArray<ObSqlExpression*, common::ObIAllocator> params_;
  WindowType win_type_;
  ExtBound upper_;
  ExtBound lower_;
  int64_t result_index_;
  common::ObFixedArray<common::ObColumnInfo, common::ObIAllocator> partition_cols_;
  common::ObFixedArray<ObSortColumn, common::ObIAllocator> sort_cols_;
};

class ObWindowFunction : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

private:
  struct WinFrame {
    WinFrame(const int64_t head = -1, const int64_t tail = -1) : head_(head), tail_(tail)
    {}
    int64_t head_;
    int64_t tail_;
    TO_STRING_KV(K(head_), K(tail_));
  };

  class IGetRow {
  public:
    inline int at(const int64_t row_idx, const common::ObNewRow*& row)
    {
      int ret = common::OB_SUCCESS;
      row = NULL;
      if (OB_FAIL(inner_get_row(row_idx, row))) {
        row = NULL;
        SQL_ENG_LOG(ERROR, "get row failed", K(row_idx), K(ret));
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(ERROR, "get row failed", K(row_idx), K(ret));
      }
      return ret;
    }

  protected:
    virtual int inner_get_row(const int64_t row_idx, const common::ObNewRow*& row) = 0;
  };

  class RowReader;
  class RowsWrapper : public IGetRow {
    friend class RowReader;

  public:
    RowsWrapper() : rows_buf_(NULL /*allocator*/, true /*keep_projector*/), begin_idx_(0), row_cnt_(0)
    {}
    inline int add_row(const common::ObNewRow& row)
    {
      row_cnt_++;
      return rows_buf_.add_row(row);
    }
    inline int64_t count() const
    {
      return row_cnt_;
    }
    inline int reset_buf(const uint64_t tenant_id)
    {
      begin_idx_ = row_cnt_;
      rows_buf_.reset();
      const int64_t mem_limit = 0;
      const int64_t mem_ctx_id = common::ObCtxIds::WORK_AREA;
      const char* label = common::ObModIds::OB_SQL_WINDOW_ROW_STORE;
      return rows_buf_.init(mem_limit, tenant_id, mem_ctx_id, label);
    }
    inline int reset(const uint64_t tenant_id)
    {
      begin_idx_ = 0;
      row_cnt_ = 0;
      return reset_buf(tenant_id);
    }
    TO_STRING_KV(K_(begin_idx), K_(row_cnt));

  private:
    virtual int inner_get_row(const int64_t row_idx, const common::ObNewRow*& row)
    {
      return rows_buf_.get_row(row_idx - begin_idx_, row);
    }

  private:
    ObRARowStore rows_buf_;
    int64_t begin_idx_;
    int64_t row_cnt_;
  };

  class RowReader : public IGetRow {
  public:
    RowReader(RowsWrapper& rw) : rw_(rw), reader_(rw.rows_buf_)
    {}

  private:
    virtual int inner_get_row(const int64_t row_idx, const common::ObNewRow*& row)
    {
      return reader_.get_row(row_idx - rw_.begin_idx_, row);
    }

  private:
    RowsWrapper& rw_;
    ObRARowStore::Reader reader_;
  };

  class ObWindowFunctionCtx;
  struct FuncCtx {
    FuncCtx() : func_info_(NULL), w_ctx_(NULL), part_first_row_(0), part_iter_end_(false)
    {}
    // assign once
    FuncInfo* func_info_;
    ObWindowFunctionCtx* w_ctx_;

    // init them when invoke input_first_row
    int64_t part_first_row_;
    bool part_iter_end_;
    RowsWrapper rw_;
    TO_STRING_KV(KP(this), K(*func_info_), K_(part_first_row), K_(part_iter_end), K_(rw));
  };

  class BaseFunc : public common::ObDLinkBase<BaseFunc> {
  public:
    virtual ~BaseFunc()
    {}
    WinFrame last_valid_frame_;
    FuncCtx* func_ctx_;
    void reset_for_restart()
    {
      last_valid_frame_.head_ = last_valid_frame_.tail_ = -1;
      reset_for_restart_self();
    }
    virtual bool is_agg() const = 0;
    virtual int init_all_first()
    {
      return common::OB_SUCCESS;
    }
    TO_STRING_EMPTY();

  protected:
    virtual void reset_for_restart_self()
    {}
  };

  class AggFunc : public BaseFunc {
  public:
    virtual ~AggFunc();
    int trans(const common::ObNewRow& row)
    {
      return trans_self(row);
    }
    virtual bool can_inv() const
    {
      return false;
    }
    int inv_trans(const common::ObNewRow& row)
    {
      int ret = common::OB_SUCCESS;
      if (!can_inv()) {
        ret = common::OB_NOT_SUPPORTED;
      } else {
        ret = inv_trans_self(row);
      }
      return ret;
    };
    virtual int final(common::ObObj& val);
    virtual bool is_agg() const
    {
      return true;
    }
    virtual int init_all_first();
    TO_STRING_EMPTY();

  protected:
    virtual int trans_self(const common::ObNewRow& row);
    virtual int inv_trans_self(const common::ObNewRow& row)
    {
      UNUSED(row);
      int ret = common::OB_SUCCESS;
      ret = common::OB_NOT_SUPPORTED;
      return ret;
    }
    virtual void reset_for_restart_self();

  private:
    bool prepare_;
    ObAggregateFunction aggr_func_;
    ObAggrExprList aggr_columns_;
    int64_t result_index_;
    common::ObObj* result_;
    common::ObObj result_buf_;
  };

  class NonAggFunc : public BaseFunc {
  public:
    virtual int eval(RowReader& assist_reader, const int64_t row_idx, const common::ObNewRow& row,
        const WinFrame& frame, common::ObObj& val) = 0;
    virtual bool is_agg() const
    {
      return false;
    }
    TO_STRING_EMPTY();
  };

  class NonAggFuncRowNumber : public NonAggFunc {
  public:
    virtual int eval(RowReader& assist_reader, const int64_t row_idx, const common::ObNewRow& row,
        const WinFrame& frame, common::ObObj& val);
    virtual void reset_for_restart_self()
    {}
  };

  class NonAggFuncNtile : public NonAggFunc {
  public:
    virtual int eval(RowReader& assist_reader, const int64_t row_idx, const common::ObNewRow& row,
        const WinFrame& frame, common::ObObj& val);
    virtual void reset_for_restart_self()
    {}
  };

  class NonAggFuncNthValue : public NonAggFunc {
  public:
    virtual int eval(RowReader& assist_reader, const int64_t row_idx, const common::ObNewRow& row,
        const WinFrame& frame, common::ObObj& val);
    virtual void reset_for_restart_self()
    {}
  };

  class NonAggFuncLeadOrLag : public NonAggFunc {
  public:
    virtual int eval(RowReader& assist_reader, const int64_t row_idx, const common::ObNewRow& row,
        const WinFrame& frame, common::ObObj& val);
    virtual void reset_for_restart_self();
  };

  class NonAggFuncRankLike : public NonAggFunc {
  public:
    virtual int eval(RowReader& assist_reader, const int64_t row_idx, const common::ObNewRow& row,
        const WinFrame& frame, common::ObObj& val);
    virtual void reset_for_restart_self();
    int64_t rank_of_prev_row_;
  };

  class NonAggFuncCumeDist : public NonAggFunc {
  public:
    virtual int eval(RowReader& assist_reader, const int64_t row_idx, const common::ObNewRow& row,
        const WinFrame& frame, common::ObObj& val);
    virtual void reset_for_restart_self()
    {}
  };

  class Utils {
  public:
    static int copy_new_row(common::ObIAllocator& allocator, const common::ObNewRow& src, common::ObNewRow*& dst);
    static int clone_cell(common::ObIAllocator& allocator, const common::ObObj& cell, common::ObObj& cell_clone);
    static int convert_stored_row(const common::ObNewRow& stored_row, common::ObNewRow& row);
    static int check_same_partition(const common::ObIArray<common::ObColumnInfo>& partition_cols,
        const common::ObNewRow& row1, const common::ObNewRow& row2, bool& is_same);
    static bool need_restart_agg(const AggFunc& agg_func, const WinFrame& last_valid_frame, const WinFrame& new_frame);
    static bool valid_frame(const WinFrame& part_frame, const WinFrame& frame);
    static bool same_frame(const WinFrame& left, const WinFrame& right);
    static void prune_frame(const WinFrame& part_frame, WinFrame& frame);
    static int invoke_agg(AggFunc& agg_func, const bool use_trans, const common::ObNewRow& row);
  };

  class FuncAllocer {
  public:
    template <class FuncType>
    int alloc(BaseFunc*& return_func);
    common::ObIAllocator* local_allocator_;
  };

  class ObWindowFunctionCtx : public ObPhyOperatorCtx {
    friend class ObWindowFunction;
    typedef common::ObDList<BaseFunc> BaseFuncList;

  public:
    explicit ObWindowFunctionCtx(ObExecContext& ctx);
    virtual ~ObWindowFunctionCtx()
    {}

    virtual void destroy()
    {
      rw_.~RowsWrapper();
      func_list_.~BaseFuncList();
      local_allocator_.~ObArenaAllocator();
    }
    int init();
    void reset_for_next_part()
    {
      last_output_row_ = -1;
    }
    void reset()
    {
      next_part_first_row_ = NULL;
      op_ = NULL;
      all_first_ = true;
    }
    int update_next_part_first_row(const common::ObNewRow& row);
    int get_next_row(const common::ObNewRow*& row);
    int get_pos(RowReader& assist_reader, FuncCtx* func_ctx, const int64_t row_idx, const common::ObNewRow& row,
        const bool is_rows, const bool is_upper, const bool is_preceding, const bool is_unbounded,
        const bool is_nmb_literal, ObSqlExpression* sql_expr, ObSqlExpression** sql_exprs, int64_t& pos,
        bool& got_null_val);
    int input_one_row(FuncCtx* func_ctx);
    int input_first_row();
    int compute(RowReader& assist_reader, BaseFunc* func, const int64_t row_idx, common::ObObj& val);

  private:
    int parallel_winbuf_process();
    int get_whole_msg(bool is_end, ObWinbufWholeMsg& whole, const ObNewRow* res_row = NULL);

  private:
    RowsWrapper rw_;
    BaseFuncList func_list_;
    common::ObArenaAllocator local_allocator_;
    common::ObNewRow* next_part_first_row_;
    int64_t last_output_row_;
    // In sql syntax, next_output_row and upper and lower bounds can be a combination of any positional relationship,
    // such as
    /*
      a.
        next_output_row
        |
        upper
        |
        lower
      b.
        upper
        |
        next_output_row
        |
        lower
      c.
        upper
        |
        lower
        |
        next_output_row

      In addition to the above three, the positional relationship between upper and lower can also
      be reversed, with more combinations

    */

    ObWindowFunction* op_;
    bool all_first_;
    bool rescan_;
    bool finish_parallel_;
  };

public:
  explicit ObWindowFunction(common::ObIAllocator& alloc);
  virtual ~ObWindowFunction();

  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& ctx) const;
  virtual int register_to_datahub(ObExecContext& ctx) const override;
  inline int init(int64_t func_info_cnt)
  {
    int ret = common::OB_SUCCESS;
    ret = init_array_size<>(func_infos_, func_info_cnt);
    return ret;
  }
  inline int add_func_info(const FuncInfo& func_info)
  {
    int ret = common::OB_SUCCESS;
    ret = func_infos_.push_back(func_info);
    return ret;
  }
  void set_parallel(bool is_parallel)
  {
    is_parallel_ = is_parallel;
  }
  static int get_param_int_value(ObExprCtx& expr_ctx, const ObObj& tmp_obj, int64_t& value);

private:
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief: called by get_next_row(), get a row from the child operator or row_store
   * @param: ctx[in], execute context
   * @param: row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObWindowFunction);

private:
  common::ObFixedArray<FuncInfo, common::ObIAllocator> func_infos_;
  bool is_parallel_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_WINDOW_FUNCTION_H */
