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
#include "lib/lock/ob_scond.h"
#include "objit/common/ob_item_type.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/basic/ob_ra_datum_store.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"
#include "sql/engine/px/datahub/components/ob_dh_second_stage_reporting_wf.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{
class ObRDWFPieceMsgCtx;
class ObRDWFPartialInfo;
class WinFuncInfo
{
  OB_UNIS_VERSION_V(1);
public:
  struct ExtBound
  {
    OB_UNIS_VERSION_V(1);
  public:
    ExtBound()
      : is_preceding_(false),
        is_unbounded_(false),
        is_nmb_literal_(false),
        between_value_expr_(NULL),
        range_bound_expr_(NULL)
    {
    }
    TO_STRING_KV(K_(is_preceding), K_(is_unbounded), K_(is_nmb_literal), KPC_(between_value_expr),
                 KPC_(range_bound_expr));

    bool is_preceding_;
    bool is_unbounded_;
    bool is_nmb_literal_;//only support is_nmb_literal now
    ObExpr *between_value_expr_;//when row/range between a preceding and b following, store a or b
    ObExpr *range_bound_expr_;//when order by c1 range between a preceding and b following, store (c1 - a) or (c1 + b)
  };
public:
  WinFuncInfo()
    : win_type_(WINDOW_MAX),
      func_type_(T_MAX),
      is_ignore_null_(false),
      is_from_first_(false),
      remove_type_(common::REMOVE_INVALID),
      expr_(NULL),
      can_push_down_(false)
  {
  }

  virtual ~WinFuncInfo() {}

  inline void set_allocator(common::ObIAllocator *alloc)
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

  TO_STRING_KV(K_(win_type), K_(func_type), K_(is_ignore_null), K_(is_from_first), K_(remove_type),
               KPC_(expr), K_(aggr_info), K_(upper), K_(lower), K_(param_exprs),
               K_(partition_exprs), K_(sort_exprs), K_(sort_collations), K_(sort_cmp_funcs),
               K_(can_push_down));
  WindowType win_type_;
  ObItemType func_type_;
  bool is_ignore_null_;
  bool is_from_first_;
  uint64_t remove_type_;

  ObExpr *expr_;//same as aggr_info_.expr_
  ObAggrInfo aggr_info_;
  ExtBound upper_;
  ExtBound lower_;

  ExprFixedArray param_exprs_;
  ExprFixedArray partition_exprs_;

  ExprFixedArray sort_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funcs_;
  bool can_push_down_;
};

typedef common::ObFixedArray<WinFuncInfo, common::ObIAllocator> WFInfoFixedArray;

// TODO:@xiaofeng.lby, add a max/min row, use it to decide whether following rows can by pass
typedef common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode> ReportingWFHashSet;

// for sync_wait to reset whole_msg_provider in get_participator_whole_msg
struct ObWFParticipatorSharedInfo
{
  int ret_;
  int64_t sqc_thread_count_;
  int64_t process_cnt_;
  ObSpinLock lock_;
  common::SimpleCond cond_;
};

// set task_count to ObWindowFunctionOpInput for wf pushdown participator
class ObWindowFunctionOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObWindowFunctionOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec), local_task_count_(1), total_task_count_(1),
      wf_participator_shared_info_(0) {};
  virtual ~ObWindowFunctionOpInput() = default;
  virtual int init(ObTaskInfo &task_info) override { UNUSED(task_info); return common::OB_SUCCESS; }
  virtual void reset() override { local_task_count_ = 1; total_task_count_ = 1;}

  void set_local_task_count(uint64_t task_count) { local_task_count_ = task_count; }
  int64_t get_local_task_count() const { return local_task_count_; }

  void set_total_task_count(uint64_t task_count) { total_task_count_ = task_count; }
  int64_t get_total_task_count() const { return total_task_count_; }


  ObWFParticipatorSharedInfo *get_wf_participator_shared_info()
  {
    return reinterpret_cast<ObWFParticipatorSharedInfo *>(wf_participator_shared_info_);
  }
  int64_t &get_sqc_thread_count()
  {
    return reinterpret_cast<ObWFParticipatorSharedInfo *>(
        wf_participator_shared_info_)->sqc_thread_count_;
  }
  int64_t &get_process_cnt()
  {
    return reinterpret_cast<ObWFParticipatorSharedInfo *>(
        wf_participator_shared_info_)->process_cnt_;
  }
  void set_error_code(int in_ret)
  {
    ObWFParticipatorSharedInfo *shared_info = reinterpret_cast<ObWFParticipatorSharedInfo *>(
        wf_participator_shared_info_);
    ATOMIC_SET(&shared_info->ret_, in_ret);
  }
  int init_wf_participator_shared_info(ObIAllocator &alloc, int64_t task_cnt)
  {
    int ret = OB_SUCCESS;
    ObWFParticipatorSharedInfo *shared_info = nullptr;
    if (OB_ISNULL(shared_info = reinterpret_cast<ObWFParticipatorSharedInfo *>(
        alloc.alloc(sizeof(ObWFParticipatorSharedInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "fail to alloc memory", K(ret));
    } else {
      wf_participator_shared_info_ = reinterpret_cast<int64_t>(shared_info);
      shared_info->sqc_thread_count_ = task_cnt;
      shared_info->process_cnt_ = 0;
      shared_info->ret_ = OB_SUCCESS;
      new (&shared_info->cond_)common::SimpleCond(
          common::ObWaitEventIds::SQL_WF_PARTICIPATOR_COND_WAIT);
      new (&shared_info->lock_)ObSpinLock(
          common::ObLatchIds::SQL_WF_PARTICIPATOR_COND_LOCK);
    }
    return ret;
  }
  int sync_wait(ObExecContext &ctx, ObReportingWFWholeMsg::WholeMsgProvider *whole_msg_provider);
protected:
  DISALLOW_COPY_AND_ASSIGN(ObWindowFunctionOpInput);
public:
  int64_t local_task_count_;
  int64_t total_task_count_;
  uint64_t wf_participator_shared_info_;
};

class ObWindowFunctionSpec : public ObOpSpec
{
public:
  OB_UNIS_VERSION_V(1);
public:
  enum WindowFunctionRoleType
  {
    NORMAL = 0,
    PARTICIPATOR = 1, // for wf adaptive pushdown
    CONSOLIDATOR = 2 // for wf adaptive pushdown
  };
  ObWindowFunctionSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      wf_infos_(alloc),
      all_expr_(alloc),
      single_part_parallel_(false),
      range_dist_parallel_(false),
      rd_wfs_(alloc),
      rd_coord_exprs_(alloc),
      rd_sort_collations_(alloc),
      rd_sort_cmp_funcs_(alloc),
      rd_pby_sort_cnt_(0),
      role_type_(0),
      wf_aggr_status_expr_(NULL)
  {
  }
  DECLARE_VIRTUAL_TO_STRING;
  virtual int register_to_datahub(ObExecContext &ctx) const override;
  // Generate the patch for each partial result, in range distribution window function.
  // Called in PX COORD
  int rd_generate_patch(ObRDWFPieceMsgCtx &ctx) const;

  // compare rd_sort_collations_, %l, %r may be null
  // return -1, 0, 1
  template <typename STORE_ROW_L, typename STORE_ROW_R>
  int rd_sort_cmp(const STORE_ROW_L *l,
                  const STORE_ROW_R *r,
                  const int64_t begin,
                  const int64_t end,
                  int &cmp_ret) const;

  template <typename STORE_ROW_L, typename STORE_ROW_R>
  int rd_pby_cmp(const STORE_ROW_L *l, const STORE_ROW_R *r, int &cmp_ret) const
  {
    return rd_sort_cmp(l, r, 0, rd_pby_sort_cnt_, cmp_ret);
  }

  template <typename STORE_ROW_L, typename STORE_ROW_R>
  int rd_oby_cmp(const STORE_ROW_L *l, const STORE_ROW_R *r, int &cmp_ret) const
  {
    return rd_sort_cmp(l, r, rd_pby_sort_cnt_, rd_sort_collations_.count(), cmp_ret);
  }

  template <typename STORE_ROW_L, typename STORE_ROW_R>
  int rd_pby_oby_cmp(const STORE_ROW_L *l, const STORE_ROW_R *r, int &cmp_ret) const
  {
    return rd_sort_cmp(l, r, 0, rd_sort_collations_.count(), cmp_ret);
  }

  int64_t get_role_type() const { return role_type_; }
  bool is_push_down() const  { return PARTICIPATOR == role_type_|| CONSOLIDATOR == role_type_; }
  bool is_participator() const { return PARTICIPATOR == role_type_; }
  bool is_consolidator() const { return CONSOLIDATOR == role_type_; }

public:
  WFInfoFixedArray wf_infos_;
  ExprFixedArray all_expr_; //child output + all sort expr

  bool single_part_parallel_;
  bool range_dist_parallel_;

  // `rd`: is abbreviation for range distribution
  //
  // %rd_wfs_ are range distribution window functions (index array of %wf_info_), the have the same
  // PBY, and the same OBY prefix, the first one (rd_wfs_[0]) has the longest OBY.
  common::ObFixedArray<int64_t, common::ObIAllocator> rd_wfs_;
  // all exprs seed to PX COORD in range distribution window function, organized as:
  //  PBY + OBY + rd_wfs_
  ExprFixedArray rd_coord_exprs_;
  ObSortCollations rd_sort_collations_;
  ObSortFuncs rd_sort_cmp_funcs_;
  int64_t rd_pby_sort_cnt_;
  int64_t role_type_;
  ObExpr *wf_aggr_status_expr_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObWindowFunctionSpec);
};


class ObWindowFunctionOp : public ObOperator
{
public:
  typedef common::hash::ObHashMap<int64_t, int64_t> WFPbyExprCntIdxHashMap;
  typedef common::ObSArray<uint64_t> PbyHashValueArray;
  enum class ProcessStatus
  {
    PARTIAL,
    COORINDATE,
    FINAL
  };

  struct Frame
  {
    Frame(const int64_t head = -1, const int64_t tail = -1)
      : head_(head), tail_(tail) {}
    static bool valid_frame(const Frame &part_frame, const Frame &frame);
    static bool same_frame(const Frame &left, const Frame &right);
    static void prune_frame(const Frame &part_frame, Frame &frame);
    static bool need_restart_aggr(const bool can_inv,
                                  const  Frame &last_valid_frame,
                                  const Frame &new_frame,
                                  RemovalInfo &removal_info,
                                  const uint64_t &remove_type);
    TO_STRING_KV(K(head_), K(tail_));

    int64_t head_;
    int64_t tail_;
  };

  class RowsStore
  {
  public:
    RowsStore() :
      ra_rs_(NULL /*allocator*/),
      begin_idx_(0),
      row_cnt_(0),
      stored_row_cnt_(0),
      output_row_idx_(0),
      need_output_(false) {}
    ~RowsStore() { destroy(); }
    void destroy() { ra_rs_.reset(); }
    inline int add_row(const common::ObIArray<ObExpr*> &exprs,
                       ObEvalCtx *ctx,
                       ObRADatumStore::StoredRow **stored_row = nullptr,
                       bool add_row_cnt = true)
    {
      int ret = ra_rs_.add_row(exprs, ctx, stored_row);
      stored_row_cnt_++;
      row_cnt_ += add_row_cnt;
      SQL_ENG_LOG(DEBUG, "add_row", K(ret), K_(row_cnt), K_(stored_row_cnt));
      return ret;
    }
    inline int add_row(const common::ObIArray<common::ObDatum> &datums,
                       ObRADatumStore::StoredRow **stored_row = nullptr,
                       bool add_row_cnt = true)
    {
      int ret = ra_rs_.add_row(datums, stored_row);
      row_cnt_ += add_row_cnt;
      ++stored_row_cnt_;
      SQL_ENG_LOG(DEBUG, "add_row", K(ret), K_(row_cnt), K_(stored_row_cnt), K(add_row_cnt));
      return ret;
    }
    inline int add_row_with_index(int64_t idx,
                                  const common::ObIArray<ObExpr*> &exprs,
                                  ObEvalCtx *ctx,
                                  ObRADatumStore::StoredRow **stored_row = nullptr,
                                  bool add_row_cnt = true)
    {
      int ret = OB_SUCCESS;
      UNUSED(idx); // use BatchInfoScopeGuard instead
      SQL_ENG_LOG(DEBUG, "add row with index", K(idx), K((void*)this),
                  K(begin_idx_), K(output_row_idx_),
      K(row_cnt_), K(stored_row_cnt_), K(ObToStringExprRow(*ctx, exprs)));
      if (OB_FAIL(add_row(exprs, ctx, stored_row, add_row_cnt))) {
        LOG_WARN("fail to add_row", K(ret));
      }
      return ret;
    }
    inline int64_t count() const { return row_cnt_; }
    // return row count which computed but not outputed
    inline int64_t to_output_rows() const {
      return row_cnt_ - output_row_idx_;
    }
    // return row count which window function not computed
    inline int64_t to_compute_rows() const { return stored_row_cnt_ - row_cnt_; }
    inline bool is_empty() const { return stored_row_cnt_ == begin_idx_; }
    inline int reset_buf(const uint64_t tenant_id)
    {
      //row_cnt_ no need reset
      begin_idx_ = row_cnt_;
      ra_rs_.reset();
      const int64_t mem_limit = 0;
      const int64_t mem_ctx_id = common::ObCtxIds::WORK_AREA;
      const char *label = common::ObModIds::OB_SQL_WINDOW_ROW_STORE;
      return ra_rs_.init(mem_limit, tenant_id, mem_ctx_id, label);
    }
    inline int reset(const uint64_t tenant_id)
    {
      begin_idx_ = 0;
      output_row_idx_ = 0;
      stored_row_cnt_ = 0;
      row_cnt_ = 0;
      return reset_buf(tenant_id);
    }
    inline int get_row(const int64_t row_idx, const ObRADatumStore::StoredRow *&sr)
    {
      int ret = common::OB_SUCCESS;
      if (OB_UNLIKELY(row_idx >= stored_row_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "row idx out of range", K(ret), K(row_idx),
                    K(begin_idx_), K(row_cnt_), K(stored_row_cnt_));
      } else if (OB_FAIL(ra_rs_.get_row(row_idx - begin_idx_, sr))) {
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
    TO_STRING_KV(K_(begin_idx), K_(output_row_idx), K_(row_cnt), K_(stored_row_cnt),
                 K_(need_output), K_(ra_rs));
  public:
    ObRADatumStore ra_rs_;
    // record begin idx of current partition. always zero for rows_store_
    int64_t begin_idx_;
    // cnt of rows computed
    int64_t row_cnt_;
    /* In non-batch execution, rows of only one partition is stored in rows_store_.
     * If get row of next partition from child, store it in next_row_ and compute current_part.
     * While in batch execution, we get a batch of rows from child, and it may contain multiple parts.
     * All these rows are stored in a rows_store, but we only compute one partition one time.
     * row_cnt_ is the last index of the partition we compute currently.
     * stored_row_cnt_ is the count of all rows stored in this rows_store.
    */
    int64_t stored_row_cnt_;
    // [begin_idx_, output_row_idx_) => rows output already
    // [output_row_idx_, row_cnt_) => rows computed already but not output
    // [row_cnt_, stored_row_cnt_) => rows not computed yet
    int64_t output_row_idx_;
    // record whether the input row of consolidator need to output
    bool need_output_;
  };

  struct Stores
  {
    Stores() : processed_(NULL), cur_(NULL), first_(NULL), last_(NULL)
    {
    }

    virtual ~Stores()
    {
      foreach_store([](RowsStore *&s) { s->~RowsStore(); s = NULL; return OB_SUCCESS; });
    }

    int reset(const int64_t tenant_id)
    {
      return foreach_store([&](RowsStore *&s) { return s->reset(tenant_id); });
    }

    void destroy()
    {
      foreach_store([](RowsStore *&s) { s->destroy(); return OB_SUCCESS; });
    }

    template <typename OP>
    static int apply_valid_ptr(RowsStore *&ptr, OP op)
    {
      return NULL == ptr ? common::OB_SUCCESS : op(ptr);
    }

    template <typename OP>
    int foreach_store(OP op)
    {
      int ret = common::OB_SUCCESS;
      if (OB_SUCC(apply_valid_ptr(processed_, op))
          && OB_SUCC(apply_valid_ptr(cur_, op))
          && OB_SUCC(apply_valid_ptr(first_, op))
          && OB_SUCC(apply_valid_ptr(last_, op))) {
      }
      return ret;
    }

    TO_STRING_KV(K(processed_), K(cur_), K(first_), K(last_));


    // `processed_` is rows calculated but not outputed, only used in vectorized execution
    RowsStore *processed_;
    // current operation rows
    RowsStore *cur_;

    // first and last partition for range distribute window function
    RowsStore *first_;
    RowsStore *last_;
  };
  typedef RowsStore *Stores::*StoreMemberPtr;

  class RowsReader
  {
  public:
    RowsReader(RowsStore &rows_store)
      : rows_store_(rows_store), reader_(rows_store.ra_rs_) {}
    inline int get_row(const int64_t row_idx, const ObRADatumStore::StoredRow *&sr)
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
    RowsStore &rows_store_;
    ObRADatumStore::Reader reader_;
  };

  class WinFuncCell : public common::ObDLinkBase<WinFuncCell>
  {
  public:
    WinFuncCell(WinFuncInfo &wf_info, ObWindowFunctionOp &op)
      : wf_info_(wf_info), op_(op), wf_idx_(0), part_first_row_idx_(0),
        part_values_(op.local_allocator_), last_valid_frame_()
    {}
    virtual ~WinFuncCell()
    {
    }
    // 开始计算新一组数据时调用reset
    void reset_for_restart()
    {
      last_valid_frame_.head_ = last_valid_frame_.tail_ = -1;
      reset_for_restart_self();
    }
    int reset_res(const int64_t tenant_id);
    virtual bool is_aggr() const = 0;
    VIRTUAL_TO_STRING_KV(K_(wf_idx), K_(wf_info), K_(part_first_row_idx),
                         K_(res), K_(last_valid_frame));
  protected:
    virtual void reset_for_restart_self() {}

  public:
    WinFuncInfo &wf_info_;
    ObWindowFunctionOp &op_;

    int64_t wf_idx_;
    int64_t part_first_row_idx_;
    ObChunkDatumStore::LastStoredRow part_values_;
    Stores res_;

    Frame last_valid_frame_;
  };

  class AggrCell : public WinFuncCell
  {
  public:
    AggrCell(WinFuncInfo &wf_info, ObWindowFunctionOp &op, ObIArray<ObAggrInfo> &aggr_infos, const int64_t tenant_id)
      : WinFuncCell(wf_info, op),
        finish_prepared_(false),
        aggr_processor_(op_.eval_ctx_, aggr_infos, "WindowAggProc", op.get_monitor_info(), tenant_id),
        result_(),
        got_result_(false),
        remove_type_(wf_info.remove_type_)
    {}
    virtual ~AggrCell() { aggr_processor_.destroy(); }
    int trans(const ObRADatumStore::StoredRow &row)
    {
      return trans_self(row);
    }
    virtual bool can_inv() const
    {
      return common::REMOVE_INVALID != remove_type_;
    }
    int inv_trans(const ObRADatumStore::StoredRow &row)
    {
      int ret = common::OB_SUCCESS;
      if (!can_inv()) {
        ret = common::OB_NOT_SUPPORTED;
      } else {
        ret = inv_trans_self(row);
      }
      return ret;
    };
    int invoke_aggr(const bool use_trans, const ObRADatumStore::StoredRow &row)
    {  return use_trans ? trans(row) : inv_trans(row); }

    virtual int final(common::ObDatum &val);
    virtual bool is_aggr() const { return true; }
    DECLARE_VIRTUAL_TO_STRING;
  protected:
    // whether aggregate function support single line translate and inverse translate.
    virtual int trans_self(const ObRADatumStore::StoredRow &row);
    virtual int inv_trans_self(const ObRADatumStore::StoredRow &row);
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
    uint64_t remove_type_;
  };

  class NonAggrCell : public WinFuncCell
  {
  public:
    NonAggrCell(WinFuncInfo &wf_info, ObWindowFunctionOp &op) : WinFuncCell(wf_info, op) {}
    virtual int eval(RowsReader &assist_reader,
                     const int64_t row_idx,
                     const ObRADatumStore::StoredRow &row,
                     const Frame &frame,
                     common::ObDatum &val) = 0;
    virtual bool is_aggr() const { return false; }
  };

  class NonAggrCellRowNumber : public NonAggrCell
  {
  public:
    NonAggrCellRowNumber(WinFuncInfo &wf_info, ObWindowFunctionOp &op)
      : NonAggrCell(wf_info, op) {}
    virtual int eval(RowsReader &assist_reader,
                     const int64_t row_idx,
                     const ObRADatumStore::StoredRow &row,
                     const Frame &frame,
                     common::ObDatum &val);
  };

  class NonAggrCellNtile : public NonAggrCell
  {
  public:
    NonAggrCellNtile(WinFuncInfo &wf_info, ObWindowFunctionOp &op)
      : NonAggrCell(wf_info, op) {}
    virtual int eval(RowsReader &assist_reader,
                     const int64_t row_idx,
                     const ObRADatumStore::StoredRow &row,
                     const Frame &frame,
                     common::ObDatum &val);
  };

  class NonAggrCellNthValue : public NonAggrCell
  {
  public:
    NonAggrCellNthValue(WinFuncInfo &wf_info, ObWindowFunctionOp &op)
      : NonAggrCell(wf_info, op) {}
    virtual int eval(RowsReader &assist_reader,
                     const int64_t row_idx,
                     const ObRADatumStore::StoredRow &row,
                     const Frame &frame,
                     common::ObDatum &val);
  };

  class NonAggrCellLeadOrLag : public NonAggrCell
  {
  public:
    NonAggrCellLeadOrLag(WinFuncInfo &wf_info, ObWindowFunctionOp &op)
      : NonAggrCell(wf_info, op) {}
    virtual int eval(RowsReader &assist_reader,
                     const int64_t row_idx,
                     const ObRADatumStore::StoredRow &row,
                     const Frame &frame,
                     common::ObDatum &val);
  };

  class NonAggrCellRankLike : public NonAggrCell
  {
  public:
    NonAggrCellRankLike(WinFuncInfo &wf_info, ObWindowFunctionOp &op)
      : NonAggrCell(wf_info, op), rank_of_prev_row_(0) {}
    virtual int eval(RowsReader &assist_reader,
                     const int64_t row_idx,
                     const ObRADatumStore::StoredRow &row,
                     const Frame &frame,
                     common::ObDatum &val);
    virtual void reset_for_restart_self() override { rank_of_prev_row_ = 0; }
    DECLARE_VIRTUAL_TO_STRING;

    int64_t rank_of_prev_row_;
  };

  class NonAggrCellCumeDist : public NonAggrCell
  {
  public:
    NonAggrCellCumeDist(WinFuncInfo &wf_info, ObWindowFunctionOp &op)
      : NonAggrCell(wf_info, op) {}
    virtual int eval(RowsReader &assist_reader,
                     const int64_t row_idx,
                     const ObRADatumStore::StoredRow &row,
                     const Frame &frame,
                     common::ObDatum &val);
  };

  typedef common::ObDList<WinFuncCell> WinFuncCellList;

  class FuncAllocer
  {
  public:
    template<class FuncType>
    int alloc(WinFuncCell *&return_func, WinFuncInfo &wf_info,
              ObWindowFunctionOp &op, const int64_t tenant_id);
    common::ObIAllocator *local_allocator_;
  };
public:
  ObWindowFunctionOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      local_allocator_(),
      stat_(ProcessStatus::PARTIAL),
      input_rows_(),
      wf_list_(),
      next_row_(),
      curr_row_collect_values_(local_allocator_),
      all_expr_datums_copy_(local_allocator_),
      all_expr_datums_(local_allocator_),
      restore_row_cnt_(0),
      last_output_row_idx_(common::OB_INVALID_INDEX),
      child_iter_end_(false),
      iter_end_(false),
      dir_id_(-1),
      first_part_saved_(false),
      last_part_saved_(false),
      rd_patch_(NULL),
      first_part_outputed_(false),
      patch_first_(false),
      patch_last_(false),
      first_row_same_order_cache_(SAME_ORDER_CACHE_DEFAULT),
      last_row_same_order_cache_(SAME_ORDER_CACHE_DEFAULT),
      last_computed_part_rows_(0),
      last_aggr_status_(0),
      pby_set_count_(0),
      next_wf_pby_expr_cnt_to_transmit_(common::OB_INVALID_COUNT),
      pby_expr_cnt_idx_array_(),
      pby_hash_values_(),
      participator_whole_msg_array_(),
      pby_hash_values_sets_()
  {
  }
  virtual ~ObWindowFunctionOp() {}
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;

  // merge aggregated result %src0, %src1 to %res
  // only count, sum, min, max, rank, dense_rank supported
  static int merge_aggregated_result(ObDatum &res,
                                     const WinFuncInfo &wf_info,
                                     common::ObIAllocator &alloc,
                                     const ObDatum &src0,
                                     const ObDatum &src1);

  static int rank_add(ObDatum &res,
                      const WinFuncInfo &info,
                      common::ObIAllocator &alloc,
                      const ObDatum &rank,
                      const int64_t val);

protected:
  int init();

  // Window function inner_get_next_row()/inner_get_next_batch() are implemented in three steps:
  //
  // 1. partial_next_row/parallel_next_batch: calculate the input's rows window function result.
  //    Result may not outputed because it's may be partial result.
  // 2. coordinate(): PX COORD is involved to get information of other workers and generate
  //    the final result.
  // 3. Output the final result by final_next_row()/final_next_batch()
  int partial_next_row();
  int do_partial_next_batch(const int64_t max_row_cnt, bool &do_output);
  int partial_next_batch(const int64_t max_row_cnt);
  int coordinate();
  int final_next_row();
  int final_next_batch(const int64_t max_row_cnt);

  template <typename OP>
  int foreach_stores(OP op);

  int create_row_store(RowsStore *&s);
  int create_stores(Stores &s);
  int set_it_age(Stores &s);
  int unset_it_age(Stores &s);
  int reset_for_scan(const int64_t tenant_id);

  int reset_for_part_scan(const int64_t tenant_id);

  int get_pos(RowsReader &assist_reader,
              WinFuncCell &func_ctx,
              const int64_t row_idx,
              const ObRADatumStore::StoredRow &row,
              const bool is_upper,
              int64_t &pos,
              bool &got_null_val);
  bool all_outputed() const
  {
    static_assert(-1 == static_cast<int64_t>(common::OB_INVALID_INDEX),
                  "invalid index is not -1");
    bool is_all_outputed =
         input_rows_.cur_->count() == static_cast<int64_t>(last_output_row_idx_) + 1;
    return is_all_outputed;
  }

  int fetch_child_row();
  int input_one_row(WinFuncCell &func_ctx, bool &part_end);
  int compute(RowsReader &row_reader, WinFuncCell &wf_cell, const int64_t row_idx,
              common::ObDatum &val);
  int compute_push_down_by_pass(WinFuncCell &wf_cell, common::ObDatum &val);
  int check_same_partition(const ExprFixedArray &other_exprs,
                           bool &is_same_part,
                           const ExprFixedArray *curr_exprs = NULL);
  int check_same_partition(WinFuncCell &cell, bool &same);
  int collect_result(const int64_t idx, common::ObDatum &in_datum, WinFuncCell &wf_cell);
  inline ObExprPtrIArray &get_all_expr()
  { return *const_cast<ExprFixedArray *>(&(MY_SPEC.all_expr_)); }
  // shanting attention!
  inline int64_t get_part_end_idx() const { return input_rows_.cur_->count() - 1; }
  static int get_param_int_value(ObExpr &expr, ObEvalCtx &eval_ctx, bool &is_null, int64_t &value,
                                 const bool need_number_type = false,
                                 const bool need_check_valid = false);
  int parallel_winbuf_process();
  int get_whole_msg(bool is_end, ObWinbufWholeMsg &whole,
      const ObRADatumStore::StoredRow *row = NULL);
  int copy_datum_row(const ObRADatumStore::StoredRow &row, ObWinbufPieceMsg &piece,
      int64_t buf_len, char *buf);
  void restore_all_expr_datums();
  int store_all_expr_datums(int64_t store_begin_idx, int64_t store_num);
  int get_next_partition(int64_t &check_times);

  int process_child_batch(
      const int64_t batch_idx,
      const ObBatchRows *child_brs,
      int64_t &check_times);

  int64_t next_nonskip_row_index(int64_t cur_idx, const ObBatchRows &child_brs);
  int get_next_batch_from_child(int64_t batch_size, const ObBatchRows *&child_brs);
  int compute_wf_values(const WinFuncCell *end, int64_t &check_times);
  int check_wf_same_partition(WinFuncCell *&end);

  // for pushdown
  int detect_computing_method(const int64_t row_idx, const int64_t wf_idx,
      bool &is_result_datum_null, bool &is_pushdown_bypass);

  // For participator, add aggr result row to input rows
  int found_part_end(const WinFuncCell *end, RowsStore *rows_store, bool add_row_cnt = true);
  int found_new_part(const bool update_part_first_row_idx);
  int save_part_first_row_idx();
  int output_row();
  int output_row(int64_t idx,
                 StoreMemberPtr store_member = &Stores::cur_,
                 const ObRADatumStore::StoredRow **all_expr_row = NULL);
  int output_rows_store_rows(const int64_t output_row_cnt,
                             RowsStore &rows_store, RowsStore &part_rows_store,
                             ObEvalCtx::BatchInfoScopeGuard &guard);
  int output_batch_rows(const int64_t output_row_cnt);

  // output the first/last partition rows of range distribution parallel
  int rd_output_final();
  int rd_output_final_batch(const int64_t max_row_cnt);
  // get the result of first/last partition and apply the patch in range distribution parallel
  int rd_output_final_row(const int64_t idx, const bool patch_first, const bool patch_last);

  void reset_first_row_same_order_cache()
  {
    first_row_same_order_cache_ = 0 == first_row_same_order_cache_
        ? SAME_ORDER_CACHE_DEFAULT
        : first_row_same_order_cache_;
  }
  bool first_row_same_order(const ObRADatumStore::StoredRow *row);
  void reset_last_row_same_order_cache()
  {
    last_row_same_order_cache_ = 0 != last_row_same_order_cache_
        ? SAME_ORDER_CACHE_DEFAULT
        : last_row_same_order_cache_;
  }
  bool last_row_same_order(const ObRADatumStore::StoredRow *row);

  // Send partial aggregation info to PX COORD and get the patch info of partial partitions,
  // in range distribution parallelism.
  int rd_fetch_patch();
  int set_compute_result_for_invalid_frame(WinFuncCell &wf_cell, ObDatum &val);

  // Send the first dop part values of each pushdown wfs to PX COORD
  // and get all the part values of each pushdown wfs to be caculate
  int participator_coordinate(const int64_t pushdown_wf_idx, const ExprFixedArray &pby_exprs);
  int get_participator_whole_msg(
      const PbyHashValueArray &pby_hash_value_array, ObReportingWFWholeMsg &whole);
  int build_pby_hash_values_for_transmit();
  int build_participator_whole_msg_array();
  int send_empty_piece_data();
  int calc_part_exprs_hash(const common::ObIArray<ObExpr *> *exprs_,
      const ObChunkDatumStore::StoredRow *row_, uint64_t &hash_value);
  int detect_aggr_status();
  bool skip_calc(const int64_t wf_idx);
  int check_interval_valid(ObExpr &expr);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObWindowFunctionOp);
private:
  common::ObArenaAllocator local_allocator_;
  // this allocator will be reset in rescan
  common::ObArenaAllocator rescan_alloc_;

  ProcessStatus stat_;

  // only `cur_` is used for non-batch execution.
  Stores input_rows_;
  WinFuncCellList wf_list_;
  // shadow copy the next and restore it before get next row from child.
  ObChunkDatumStore::ShadowStoredRow next_row_; // for backup & restore datum ptr of child output
  DatumFixedArray curr_row_collect_values_;
  common::ObFixedArray<ObDatum*, common::ObIAllocator> all_expr_datums_copy_;
  common::ObFixedArray<ObDatum*, common::ObIAllocator> all_expr_datums_;
  int64_t restore_row_cnt_;
  // 上一个输出行
  int64_t last_output_row_idx_;
  bool child_iter_end_;
  bool iter_end_;
  int64_t dir_id_;

  // Members for range distribution parallel execution
  // `rd`: is abbreviation for range distribution

  bool first_part_saved_;
  bool last_part_saved_;

  ObRDWFPartialInfo *rd_patch_;
  common::ObArenaAllocator patch_alloc_;

  bool first_part_outputed_;
  bool patch_first_;
  bool patch_last_;

  const static int64_t SAME_ORDER_CACHE_DEFAULT = -2;
  // current output row is same order by with rd_patch_->first_row_
  // -2 (default) for never compared
  int64_t first_row_same_order_cache_;
  // current output row is same order by with rd_patch_->last_row_
  // -2 (default) for never compared
  int64_t last_row_same_order_cache_;

  int64_t last_computed_part_rows_;
  // row store iteration age to prevent output row datum released dring the same batch
  ObRADatumStore::IterationAge output_rows_it_age_;


  // Members for reporting wf push down, use for pushdown paricipator transmit pieces to datahub begin
  int64_t last_aggr_status_; // aggr_status  of last input row for participator

  // Use for wf participator, the count of different pby_set in wf op
  // ( pby1(c1, c2, c3), pby2(c1, c2), pby3(c1, c2), pby4(c1), pby5(c1)) is 3
  int64_t pby_set_count_;

  // next part expr count of pieces to send to datahub for wf pushdown participator
  int64_t next_wf_pby_expr_cnt_to_transmit_;

  // Use for wf participator, the idx of different pby_set of pushdown wf
  // index of array : wf_idx - 1 (because wf_idx is start from 1)
  // value of array : the idx of different pby_set of pushdown wf (value is -1 if isn't pushdown wf)
  ObArray<int64_t> pby_expr_cnt_idx_array_;

  // Use for wf participator, to transmit pieces to datahub
  ObArray<PbyHashValueArray *> pby_hash_values_;

  // Use to store msg recieved from datahub
  ObArray<ObReportingWFWholeMsg *> participator_whole_msg_array_;

  // Use to decide whether compute or bypass, generated from ObReportingWFWholeMsg
  ObArray<ReportingWFHashSet *> pby_hash_values_sets_;
  // Members for reporting wf push down, use for pushdown paricipator transmit pieces to datahub end
};



template <typename STORE_ROW_L, typename STORE_ROW_R>
int ObWindowFunctionSpec::rd_sort_cmp(const STORE_ROW_L *l,
                                      const STORE_ROW_R *r,
                                      const int64_t begin,
                                      const int64_t end,
                                      int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  // NULL last
  if (NULL == l && NULL == r) {
    cmp_ret = 0;
  } else if (NULL == l) {
    cmp_ret = 1;
  } else if (NULL == r) {
    cmp_ret = -1;
  } else {
    for (int64_t i = begin; 0 == cmp_ret && i < end && OB_SUCC(ret); i++) {
      if (OB_FAIL(rd_sort_cmp_funcs_.at(i).cmp_func_(l->cells()[i], r->cells()[i], cmp_ret))) {
        SQL_ENG_LOG(WARN, "compare failed", K(ret));
      } else if (!rd_sort_collations_.at(i).is_ascending_) {
        cmp_ret = cmp_ret * (-1);
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_WINDOW_FUNCTION_OP_H */
