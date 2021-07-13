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

#ifndef _OB_WINDOW_FUNCTION_OP_H
#define _OB_WINDOW_FUNCTION_OP_H 1

#include "lib/container/ob_array.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/parser/ob_item_type.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/basic/ob_ra_datum_store.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase {
namespace sql {
struct WinFuncInfo {
  OB_UNIS_VERSION_V(1);

public:
  struct ExtBound {
    OB_UNIS_VERSION_V(1);

  public:
    ExtBound()
        : is_preceding_(false),
          is_unbounded_(false),
          is_nmb_literal_(false),
          between_value_expr_(NULL),
          range_bound_expr_(NULL)
    {}
    TO_STRING_KV(
        K_(is_preceding), K_(is_unbounded), K_(is_nmb_literal), KPC_(between_value_expr), KPC_(range_bound_expr));

    bool is_preceding_;
    bool is_unbounded_;
    bool is_nmb_literal_;         // only support is_nmb_literal now
    ObExpr* between_value_expr_;  // when row/range between a preceding and b following, store a or b
    ObExpr* range_bound_expr_;    // when order by c1 range between a preceding and b following, store (c1 - a) or (c1 +
                                  // b)
  };

public:
  WinFuncInfo() : win_type_(WINDOW_MAX), func_type_(T_MAX), is_ignore_null_(false), is_from_first_(false), expr_(NULL)
  {}

  virtual ~WinFuncInfo()
  {}

  inline void set_allocator(common::ObIAllocator* alloc)
  {
    aggr_info_.set_allocator(alloc);
    param_exprs_.set_allocator(alloc);
    partition_exprs_.set_allocator(alloc);
    sort_exprs_.set_allocator(alloc);
    sort_collations_.set_allocator(alloc);
    sort_cmp_funcs_.set_allocator(alloc);
  }

  inline int init(int64_t params_cnt, int64_t cols_cnt, int64_t sort_cnt)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(params_cnt < 0 || cols_cnt < 0 || sort_cnt < 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid cnt", K(params_cnt), K(cols_cnt), K(sort_cnt));
    } else if (params_cnt > 0 && OB_FAIL(param_exprs_.init(params_cnt))) {
      SQL_ENG_LOG(WARN, "fail to init array", K(ret), K(params_cnt));
    } else if (cols_cnt > 0 && OB_FAIL(partition_exprs_.init(cols_cnt))) {
      SQL_ENG_LOG(WARN, "fail to init array", K(ret), K(cols_cnt));
    } else if (sort_cnt > 0 && (OB_FAIL(sort_exprs_.init(sort_cnt)))) {
      SQL_ENG_LOG(WARN, "fail to init array", K(ret), K(sort_cnt));
    }
    return ret;
  }

  TO_STRING_KV(K_(win_type), K_(func_type), K_(is_ignore_null), K_(is_from_first), KPC_(expr), K_(aggr_info), K_(upper),
      K_(lower), K_(param_exprs), K_(partition_exprs), K_(sort_exprs), K_(sort_collations), K_(sort_cmp_funcs));
  WindowType win_type_;
  ObItemType func_type_;
  bool is_ignore_null_;
  bool is_from_first_;

  ObExpr* expr_;  // same as aggr_info_.expr_
  ObAggrInfo aggr_info_;
  ExtBound upper_;
  ExtBound lower_;

  ExprFixedArray param_exprs_;
  ExprFixedArray partition_exprs_;

  ExprFixedArray sort_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funcs_;
};

typedef common::ObFixedArray<WinFuncInfo, common::ObIAllocator> WFInfoFixedArray;

class ObWindowFunctionSpec : public ObOpSpec {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObWindowFunctionSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObOpSpec(alloc, type), wf_infos_(alloc), all_expr_(alloc), is_parallel_(false)
  {}
  DECLARE_VIRTUAL_TO_STRING;
  virtual int register_to_datahub(ObExecContext& ctx) const override;

public:
  WFInfoFixedArray wf_infos_;
  ExprFixedArray all_expr_;  // child output + all sort expr
  bool is_parallel_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObWindowFunctionSpec);
};

class ObWindowFunctionOp : public ObOperator {
public:
  struct Frame {
    Frame(const int64_t head = -1, const int64_t tail = -1) : head_(head), tail_(tail)
    {}
    static bool valid_frame(const Frame& part_frame, const Frame& frame);
    static bool same_frame(const Frame& left, const Frame& right);
    static void prune_frame(const Frame& part_frame, Frame& frame);
    static bool need_restart_aggr(const bool can_inv, const Frame& last_valid_frame, const Frame& new_frame);
    TO_STRING_KV(K(head_), K(tail_));

    int64_t head_;
    int64_t tail_;
  };

  class RowsStore {
  public:
    RowsStore() : rows_buf_(NULL /*allocator*/), begin_idx_(0), row_cnt_(0)
    {}
    ~RowsStore()
    {
      destory();
    }
    void destory()
    {
      rows_buf_.reset();
    }
    inline int add_row(
        const common::ObIArray<ObExpr*>& exprs, ObEvalCtx* ctx, ObRADatumStore::StoredRow** stored_row = nullptr)
    {
      int ret = rows_buf_.add_row(exprs, ctx, stored_row);
      ++row_cnt_;
      SQL_ENG_LOG(DEBUG, "add_row", K_(row_cnt), K(ret));
      return ret;
    }
    inline int add_row(
        const common::ObIArray<common::ObDatum>& datums, ObRADatumStore::StoredRow** stored_row = nullptr)
    {
      int ret = rows_buf_.add_row(datums, stored_row);
      ++row_cnt_;
      SQL_ENG_LOG(DEBUG, "add_row", K_(row_cnt), K(ret));
      return ret;
    }

    inline int64_t count() const
    {
      return row_cnt_;
    }
    inline int reset_buf(const uint64_t tenant_id)
    {
      // row_cnt_ no need reset
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
      rows_buf_.reset();
      return reset_buf(tenant_id);
    }
    inline int get_row(const int64_t row_idx, const ObRADatumStore::StoredRow*& sr)
    {
      int ret = common::OB_SUCCESS;
      if (OB_FAIL(rows_buf_.get_row(row_idx - begin_idx_, sr))) {
        sr = NULL;
        SQL_ENG_LOG(WARN, "get row failed", K(row_idx), K(begin_idx_), K(ret));
      } else if (OB_ISNULL(sr)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "get row failed", K(row_idx), K(ret));
      } else {
        SQL_ENG_LOG(DEBUG, "get row", K(row_idx), KPC(sr));
      }
      return ret;
    }
    TO_STRING_KV(K_(begin_idx), K_(row_cnt), K_(rows_buf));

  public:
    ObRADatumStore rows_buf_;
    int64_t begin_idx_;
    int64_t row_cnt_;
  };

  class RowsReader {
  public:
    RowsReader(RowsStore& rows_store) : rows_store_(rows_store), reader_(rows_store.rows_buf_)
    {}
    inline int get_row(const int64_t row_idx, const ObRADatumStore::StoredRow*& sr)
    {
      int ret = common::OB_SUCCESS;
      if (OB_FAIL(reader_.get_row(row_idx - rows_store_.begin_idx_, sr))) {
        sr = NULL;
        SQL_ENG_LOG(WARN, "get row failed", K(row_idx), K(ret));
      } else if (OB_ISNULL(sr)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "get row failed", K(row_idx), K(ret));
      } else {
        SQL_ENG_LOG(DEBUG, "get row", K(row_idx), KPC(sr));
      }
      return ret;
    }

  private:
    RowsStore& rows_store_;
    ObRADatumStore::Reader reader_;
  };

  class WinFuncCell : public common::ObDLinkBase<WinFuncCell> {
  public:
    WinFuncCell(WinFuncInfo& wf_info, ObWindowFunctionOp& op)
        : wf_info_(wf_info),
          op_(op),
          wf_idx_(0),
          part_first_row_idx_(0),
          part_values_(op.local_allocator_),
          last_valid_frame_()
    {}
    virtual ~WinFuncCell()
    {
      part_rows_store_.destory();
    }
    void reset_for_restart()
    {
      last_valid_frame_.head_ = last_valid_frame_.tail_ = -1;
      reset_for_restart_self();
    }
    virtual bool is_aggr() const = 0;
    VIRTUAL_TO_STRING_KV(K_(wf_idx), K_(wf_info), K_(part_first_row_idx), K_(part_rows_store), K_(last_valid_frame));

  protected:
    virtual void reset_for_restart_self()
    {}

  public:
    WinFuncInfo& wf_info_;
    ObWindowFunctionOp& op_;

    int64_t wf_idx_;
    int64_t part_first_row_idx_;
    ObChunkDatumStore::LastStoredRow<> part_values_;
    RowsStore part_rows_store_;

    Frame last_valid_frame_;
  };

  class AggrCell : public WinFuncCell {
  public:
    AggrCell(WinFuncInfo& wf_info, ObWindowFunctionOp& op, ObIArray<ObAggrInfo>& aggr_infos)
        : WinFuncCell(wf_info, op),
          finish_prepared_(false),
          aggr_processor_(op_.eval_ctx_, aggr_infos),
          result_(),
          got_result_(false)
    {}
    virtual ~AggrCell()
    {
      aggr_processor_.destroy();
    }
    int trans(const ObRADatumStore::StoredRow& row)
    {
      return trans_self(row);
    }
    virtual bool can_inv() const
    {
      return false;
    }
    int inv_trans(const ObRADatumStore::StoredRow& row)
    {
      int ret = common::OB_SUCCESS;
      if (!can_inv()) {
        ret = common::OB_NOT_SUPPORTED;
      } else {
        ret = inv_trans_self(row);
      }
      return ret;
    };
    int invoke_aggr(const bool use_trans, const ObRADatumStore::StoredRow& row)
    {
      return use_trans ? trans(row) : inv_trans(row);
    }

    virtual int final(common::ObDatum& val);
    virtual bool is_aggr() const
    {
      return true;
    }
    DECLARE_VIRTUAL_TO_STRING;

  protected:
    virtual int trans_self(const ObRADatumStore::StoredRow& row);
    virtual int inv_trans_self(const ObRADatumStore::StoredRow& row)
    {
      UNUSED(row);
      int ret = common::OB_SUCCESS;
      ret = common::OB_NOT_SUPPORTED;
      return ret;
    }
    virtual void reset_for_restart_self() override
    {
      finish_prepared_ = false;
      aggr_processor_.reuse();
      result_.reset();
      got_result_ = false;
    }

  public:
    bool finish_prepared_;
    ObAggregateProcessor aggr_processor_;
    ObDatum result_;
    bool got_result_;
  };

  class NonAggrCell : public WinFuncCell {
  public:
    NonAggrCell(WinFuncInfo& wf_info, ObWindowFunctionOp& op) : WinFuncCell(wf_info, op)
    {}
    virtual int eval(RowsReader& assist_reader, const int64_t row_idx, const ObRADatumStore::StoredRow& row,
        const Frame& frame, common::ObDatum& val) = 0;
    virtual bool is_aggr() const
    {
      return false;
    }
  };

  class NonAggrCellRowNumber : public NonAggrCell {
  public:
    NonAggrCellRowNumber(WinFuncInfo& wf_info, ObWindowFunctionOp& op) : NonAggrCell(wf_info, op)
    {}
    virtual int eval(RowsReader& assist_reader, const int64_t row_idx, const ObRADatumStore::StoredRow& row,
        const Frame& frame, common::ObDatum& val);
  };

  class NonAggrCellNtile : public NonAggrCell {
  public:
    NonAggrCellNtile(WinFuncInfo& wf_info, ObWindowFunctionOp& op) : NonAggrCell(wf_info, op)
    {}
    virtual int eval(RowsReader& assist_reader, const int64_t row_idx, const ObRADatumStore::StoredRow& row,
        const Frame& frame, common::ObDatum& val);
  };

  class NonAggrCellNthValue : public NonAggrCell {
  public:
    NonAggrCellNthValue(WinFuncInfo& wf_info, ObWindowFunctionOp& op) : NonAggrCell(wf_info, op)
    {}
    virtual int eval(RowsReader& assist_reader, const int64_t row_idx, const ObRADatumStore::StoredRow& row,
        const Frame& frame, common::ObDatum& val);
  };

  class NonAggrCellLeadOrLag : public NonAggrCell {
  public:
    NonAggrCellLeadOrLag(WinFuncInfo& wf_info, ObWindowFunctionOp& op) : NonAggrCell(wf_info, op)
    {}
    virtual int eval(RowsReader& assist_reader, const int64_t row_idx, const ObRADatumStore::StoredRow& row,
        const Frame& frame, common::ObDatum& val);
  };

  class NonAggrCellRankLike : public NonAggrCell {
  public:
    NonAggrCellRankLike(WinFuncInfo& wf_info, ObWindowFunctionOp& op) : NonAggrCell(wf_info, op), rank_of_prev_row_(0)
    {}
    virtual int eval(RowsReader& assist_reader, const int64_t row_idx, const ObRADatumStore::StoredRow& row,
        const Frame& frame, common::ObDatum& val);
    virtual void reset_for_restart_self() override
    {
      rank_of_prev_row_ = 0;
    }
    DECLARE_VIRTUAL_TO_STRING;

    int64_t rank_of_prev_row_;
  };

  class NonAggrCellCumeDist : public NonAggrCell {
  public:
    NonAggrCellCumeDist(WinFuncInfo& wf_info, ObWindowFunctionOp& op) : NonAggrCell(wf_info, op)
    {}
    virtual int eval(RowsReader& assist_reader, const int64_t row_idx, const ObRADatumStore::StoredRow& row,
        const Frame& frame, common::ObDatum& val);
  };

  typedef common::ObDList<WinFuncCell> WinFuncCellList;

  class FuncAllocer {
  public:
    template <class FuncType>
    int alloc(WinFuncCell*& return_func, WinFuncInfo& wf_info, ObWindowFunctionOp& op, const int64_t tenant_id);
    common::ObIAllocator* local_allocator_;
  };

public:
  ObWindowFunctionOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObOperator(exec_ctx, spec, input),
        local_allocator_(),
        rows_store_(),
        wf_list_(),
        next_row_(),
        next_row_valid_(false),
        curr_row_collect_values_(local_allocator_),
        last_output_row_idx_(common::OB_INVALID_INDEX),
        finish_parallel_(false),
        child_iter_end_(false),
        iter_end_(false)
  {}
  virtual ~ObWindowFunctionOp()
  {}
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int rescan() override;
  virtual int switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;

protected:
  int init();

  inline int reset_for_scan(const int64_t tenant_id)
  {
    last_output_row_idx_ = common::OB_INVALID_INDEX;
    next_row_valid_ = false;
    child_iter_end_ = false;
    return rows_store_.reset(tenant_id);
  }
  int reset_for_part_scan(const int64_t tenant_id);

  int get_pos(RowsReader& assist_reader, WinFuncCell& func_ctx, const int64_t row_idx,
      const ObRADatumStore::StoredRow& row, const bool is_upper, int64_t& pos, bool& got_null_val);
  bool all_outputed() const
  {
    static_assert(-1 == static_cast<int64_t>(common::OB_INVALID_INDEX), "invalid index is not -1");
    return rows_store_.count() == static_cast<int64_t>(last_output_row_idx_) + 1;
  }
  int fetch_child_row();
  int input_one_row(WinFuncCell& func_ctx, bool& part_end);
  int compute(RowsReader& row_reader, WinFuncCell& wf_cell, const int64_t row_idx, common::ObDatum& val);
  int check_same_partition(
      const ExprFixedArray& other_exprs, bool& is_same_part, const ExprFixedArray* curr_exprs = NULL);
  int check_same_partition(WinFuncCell& cell, bool& same);
  int collect_result(const int64_t idx, common::ObDatum& in_datum, WinFuncCell& wf_cell);
  inline ObExprPtrIArray& get_all_expr()
  {
    return *const_cast<ExprFixedArray*>(&(MY_SPEC.all_expr_));
  }
  inline int64_t get_part_end_idx() const
  {
    return rows_store_.count() - 1;
  }
  static int get_param_int_value(
      ObExpr& expr, ObEvalCtx& eval_ctx, bool& is_null, int64_t& value, const bool need_number_type = false);
  int parallel_winbuf_process();
  int get_whole_msg(bool is_end, ObWinbufWholeMsg& whole, const ObRADatumStore::StoredRow* row = NULL);
  int copy_datum_row(const ObRADatumStore::StoredRow& row, ObWinbufPieceMsg& piece, int64_t buf_len, char* buf);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObWindowFunctionOp);

private:
  common::ObArenaAllocator local_allocator_;

  RowsStore rows_store_;
  WinFuncCellList wf_list_;
  // shadow copy the next and restore it before get next row from child.
  ObChunkDatumStore::ShadowStoredRow<> next_row_;
  bool next_row_valid_;  // TODO
  DatumFixedArray curr_row_collect_values_;

  int64_t last_output_row_idx_;
  bool finish_parallel_;
  bool child_iter_end_;
  bool iter_end_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_WINDOW_FUNCTION_OP_H */
