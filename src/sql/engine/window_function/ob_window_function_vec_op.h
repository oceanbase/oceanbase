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

#ifndef OCEANBASE_WINDOW_FUNCTION_VEC_OP_H_
#define OCEANBASE_WINDOW_FUNCTION_VEC_OP_H_

#include "lib/container/ob_se_array.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/window_function/row_store.h"
#include "sql/engine/window_function/win_expr.h"
#include "sql/engine/basic/ob_vector_result_holder.h"
#include "sql/engine/window_function/ob_window_function_op.h"
#include "sql/engine/px/datahub/components/ob_dh_second_stage_reporting_wf.h"

namespace oceanbase
{
namespace sql
{
namespace winfunc
{
  template<typename T>
  class WinExprWrapper;
} // end winfunc

class ObWindowFunctionVecOp;
class SPWinFuncPXWholeMsg;

class RDWinFuncPXPieceMsgCtx;
class RDWinFuncPXPartialInfo;
// copy from ObWindowFunctionSpec
class ObWindowFunctionVecSpec: public ObWindowFunctionSpec
{
  OB_UNIS_VERSION_V(1);
public:
  static const int64_t RD_MIN_BATCH_SIZE = 4;
public:
  ObWindowFunctionVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type) :
    ObWindowFunctionSpec(alloc, type)
  {}

  virtual int register_to_datahub(ObExecContext &ctx) const override;

  int rd_generate_patch(RDWinFuncPXPieceMsgCtx &msg_ctx, ObEvalCtx &eval_ctx) const;

  int rd_sort_cmp(RowMeta &row_meta, const ObCompactRow *l_row, const ObCompactRow *r_row, const int64_t begin,
                  const int64_t end, int &cmp_ret) const;

  int rd_pby_oby_cmp(RowMeta &row_meta, const ObCompactRow *l_row, const ObCompactRow *r_row,
                     int &cmp_ret) const
  {
    return rd_sort_cmp(row_meta, l_row, r_row, 0, rd_sort_collations_.count(), cmp_ret);
  }

  int rd_pby_cmp(RowMeta &row_meta, const ObCompactRow *l_row, const ObCompactRow *r_row,
                 int &cmp_ret) const
  {
    return rd_sort_cmp(row_meta, l_row, r_row, 0, rd_pby_sort_cnt_, cmp_ret);
  }

  int rd_oby_cmp(RowMeta &row_meta, const ObCompactRow *l_row, const ObCompactRow *r_row, int &cmp_ret) const
  {
    return rd_sort_cmp(row_meta, l_row, r_row, rd_pby_sort_cnt_, rd_sort_collations_.count(),
                       cmp_ret);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObWindowFunctionVecSpec);
};

class WinFuncColExpr : public common::ObDLinkBase<WinFuncColExpr>
{
public:
  WinFuncColExpr(WinFuncInfo &wf_info, ObWindowFunctionVecOp &op, const int64_t wf_idx) :
    wf_info_(wf_info), op_(op), wf_idx_(wf_idx), part_first_row_idx_(-1), wf_expr_(nullptr),
    res_(nullptr), pby_row_mapped_idxes_(nullptr), reordered_pby_row_idx_(nullptr), wf_res_row_meta_(),
    res_rows_(nullptr), agg_ctx_(nullptr), aggr_rows_(nullptr), non_aggr_results_(nullptr),
    null_nonaggr_results_(nullptr)
  {}
  void destroy() { reset(); }
  void reset();
  void reset_for_scan();
  ~WinFuncColExpr() { destroy(); }
  // init agg_ctx_ & alloc aggr_rows_
  int init_aggregate_ctx(const int64_t tenant_id);
  int init_non_aggregate_ctx();
  int init_res_rows(const int64_t tenant_id);
  int32_t non_aggr_reserved_row_size() const;
  // need reset agg_ctx.allocator_
  int reset_for_partition(const int64_t batch_size, const ObBitVector &skip);
  WinFuncInfo &wf_info_;
  ObWindowFunctionVecOp &op_;
  int64_t wf_idx_;
  int64_t part_first_row_idx_;
  // LastCompactRow pby_row_;
  winfunc::IWinExpr *wf_expr_;
  // results of window function
  winfunc::RowStores *res_;
  int32_t *pby_row_mapped_idxes_;
  int32_t *reordered_pby_row_idx_;
  RowMeta wf_res_row_meta_;
  const ObCompactRow **res_rows_; // tmp results rows pointers
  // only valid for aggregate functions
  aggregate::RuntimeContext *agg_ctx_;
  aggregate::AggrRowPtr *aggr_rows_;
  // only valid for non-aggregate functions
  char *non_aggr_results_;
  ObBitVector *null_nonaggr_results_;

  TO_STRING_KV(K_(wf_info), K_(wf_idx), K_(part_first_row_idx));
};

// copy from ObWindowFunctionOpInput
class ObWindowFunctionVecOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObWindowFunctionVecOpInput(ObExecContext &ctx, const ObOpSpec &spec) :
    ObOpInput(ctx, spec), local_task_count_(1), total_task_count_(1),
    wf_participator_shared_info_(0)
  {}
  virtual int init(ObTaskInfo &task_info) override
  {
    int ret = OB_SUCCESS;
    return ret;
  }

  virtual void reset() override
  {
    local_task_count_ = 1;
    total_task_count_ = 1;
  }

  int64_t get_total_task_count() const { return total_task_count_; }

  void set_total_task_count(int64_t total_count) { total_task_count_ = total_count; }

  void set_local_task_count(int64_t task_count) { local_task_count_ = task_count; }

  int64_t get_local_task_count() const { return local_task_count_; }

  ObWFParticipatorSharedInfo *get_wf_participator_shared_info()
  {
    return reinterpret_cast<ObWFParticipatorSharedInfo *>(wf_participator_shared_info_);
  }

  int64_t &get_sqc_thread_count()
  {
    return get_wf_participator_shared_info()->sqc_thread_count_;
  }

  int64_t &get_process_cnt()
  {
    return get_wf_participator_shared_info()->process_cnt_;
  }

  void set_error_code(int ret_code)
  {
    ATOMIC_SET(&(get_wf_participator_shared_info()->ret_), ret_code);
  }

  int init_wf_participator_shared_info(ObIAllocator &alloc, int64_t task_cnt);

  int sync_wait(ObExecContext &ctx, ObReportingWFWholeMsg::WholeMsgProvider *msg_provider);
private:
  DISALLOW_COPY_AND_ASSIGN(ObWindowFunctionVecOpInput);

public:
  int64_t local_task_count_;
  int64_t total_task_count_;
  uint64_t wf_participator_shared_info_;
};

class ObWindowFunctionVecOp: public ObOperator
{
private:
  using WinFuncColExprList = common::ObDList<WinFuncColExpr>;
  using PbyHashValueArray = common::ObSArray<uint64_t>;
private:
  enum class ProcessStatus
  {
    PARTIAL,
    COORDINATE,
    FINAL
  };

public:
  ObWindowFunctionVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input):
      ObOperator(exec_ctx, spec, input),
      local_allocator_(nullptr),
      stat_(ProcessStatus::PARTIAL),
      input_stores_(),
      wf_list_(),
      all_expr_vector_copy_(),
      backuped_size_(0),
      last_output_row_idx_(common::OB_INVALID_INDEX),
      child_iter_end_(false),
      iter_end_(false),
      dir_id_(-1),
      first_part_saved_(false),
      last_part_saved_(false),
      rd_patch_(nullptr),
      first_part_outputed_(false),
      patch_first_(false),
      patch_last_(false),
      last_computed_part_rows_(0),
      rd_coord_row_meta_(nullptr),
      last_aggr_status_(0),
      pby_set_count_(0),
      next_wf_pby_expr_cnt_to_transmit_(common::OB_INVALID_COUNT),
      pby_expr_cnt_idx_array_(),
      pby_hash_values_(),
      participator_whole_msg_array_(),
      pby_hash_values_sets_(),
      input_row_meta_(),
      max_pby_col_cnt_(0),
      pby_row_mapped_idx_arr_(nullptr),
      last_row_idx_arr_(nullptr),
      all_part_exprs_(),
      batch_ctx_(),
      sp_merged_row_(nullptr),
      all_wf_res_row_meta_(nullptr),
      mem_context_(nullptr),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_, op_monitor_info_),
      global_mem_limit_version_(0),
      amm_periodic_cnt_(0),
      store_it_age_()
  {}

  virtual ~ObWindowFunctionVecOp() {}
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_get_next_row() override
  {
    return OB_NOT_IMPLEMENT;
  }
  virtual void destroy() override;
public:
  int get_part_end_idx() { return input_stores_.cur_->count(); }
  const RowMeta &get_input_row_meta() const { return input_row_meta_; }
  inline ObExprPtrIArray &get_all_expr()
  {
    return const_cast<ExprFixedArray &>(MY_SPEC.all_expr_);
  }

  static bool all_supported_winfuncs(const ObIArray<ObWinFunRawExpr *> &win_exprs);
private:
  struct cell_info
  {
    bool is_null_;
    int32_t len_;
    const char *payload_;
    cell_info(bool is_null, int32_t len, const char *payload) :
      is_null_(is_null), len_(len), payload_(payload)
    {}
    cell_info(): is_null_(true), len_(0), payload_(nullptr) {}
    TO_STRING_KV(K_(is_null), K_(len), KP_(payload));
  };
  int init();

  int create_stores(const int64_t tenant_id);

  void reset_stores();

  void destroy_stores();

  int reset_for_scan(const int64_t tenant_id);

  int build_pby_hash_values_for_transmit();

  int build_participator_whole_msg_array();

  int setup_participator_pby_hash_sets(WFInfoFixedArray &wf_infos,
                                       ObWindowFunctionVecOpInput *op_input);

  int get_next_batch_from_child(int64_t batch_size, const ObBatchRows *&child_brs);

  int mapping_pby_row_to_idx_arr(const ObBatchRows &child_brs, const ObCompactRow *last_row);
  template <typename ColumnFormat>
  int mapping_pby_col_to_idx_arr(int32_t col_id, const ObExpr &part_expr, const ObBatchRows &brs,
                                 const cell_info *last_part_res);
  int eval_prev_part_exprs(const ObCompactRow *last_row, ObIAllocator &alloc,
                           const ObExprPtrIArray &part_exprs,
                           common::ObIArray<cell_info> &last_part_infos);

  OB_INLINE int save_pby_row_for_wf(WinFuncColExpr *end_wf, const int64_t batch_idx);

  int detect_and_report_aggr_status(const ObBatchRows &child_brs, const int64_t start_idx,
                                    const int64_t end_idx);

  OB_INLINE int update_part_first_row_idx(WinFuncColExpr *end);

  int partial_next_batch(const int64_t max_row_cnt);

  int do_partial_next_batch(const int64_t max_row_cnt, bool &do_output);

  int coordinate();

  int final_next_batch(const int64_t max_row_cnt);

  int get_next_partition(int64_t &check_times);

  int output_batch_rows(const int64_t output_row_cnt);

  int add_aggr_res_row_for_participator(WinFuncColExpr *end, winfunc::RowStore &store);

  int compute_wf_values(WinFuncColExpr *end, int64_t &check_times);

  int set_null_results_of_wf(WinFuncColExpr &wf, const int64_t batch_size,
                             const ObBitVector &nullres_skip);

  int calc_bypass_pushdown_rows_of_wf(WinFuncColExpr &wf, const int64_t batch_size,
                                      const ObBitVector &pushdown_skip);

  int process_child_batch(const int64_t batch_idx, const ObBatchRows *kchild_brs,
                          int64_t &check_times);

  int check_same_partition(WinFuncColExpr &wf_col, bool &same);

  int find_same_partition_of_wf(WinFuncColExpr *&end_wf);

  int detect_nullres_or_pushdown_rows(WinFuncColExpr &wf, ObBitVector &nullres_skip,
                                      ObBitVector &pushdown_skip, ObBitVector &wf_skip);

  // output rows stored in input_stores_
  int output_stored_rows(const int64_t out_processed_cnt, const int64_t out_cur_cnt,
                         winfunc::RowStores &store, int64_t &output_cnt);

  int output_stored_rows(const int64_t out_processed_cnt, const int64_t out_cur_cnt,
                         WinFuncColExpr &wf_col, int64_t &outputed_cnt);

  int attach_rows_to_output(const ObCompactRow **rows, int64_t row_cnt);

  int attach_row_to_output(const ObCompactRow *row);

  int output_wf_rows(WinFuncColExpr &wf, const ObCompactRow **stored_rows);

  int get_last_input_row_of_prev_batch(const ObCompactRow *&last_row);

  // for single partition parallel execution, collect partition aggr results and do merging.
  int collect_sp_partial_results();

  int collect_wf_res_row(const int64_t batch_idx, const int64_t stored_row_idx, ObCompactRow *&res_row);

  // send piece msg and wait for whole response
  int sp_get_whole_msg(bool is_empty, SPWinFuncPXWholeMsg &msg, ObCompactRow *sending_row);

  int sp_merge_partial_results(SPWinFuncPXWholeMsg &msg);

  // rwf stands for reporting window function
  int rwf_get_whole_msg(const PbyHashValueArray *hash_value_arr, ObReportingWFWholeMsg &who_msg);

  int rwf_participator_coordinate(const int64_t pushdown_wf_idx);

  int rwf_calc_pby_row_hash(const ObBatchRows &child_brs, const ObIArray<ObExpr *> &pby_exprs,
                            uint64_t &hash_value);

  int rwf_update_aggr_status_code(const int64_t start_idx, const int64_t end_idx);

  int rwf_send_empty_piece_data();

  // rd stands for range distribution
  int rd_fetch_patch();

  typedef winfunc::RowStore* winfunc::RowStores::*StoreMemberPtr;

  int rd_build_partial_info_row(int64_t idx, bool is_first_part,
                                ObIAllocator &alloc, ObCompactRow *&build_row);

  int rd_output_final_batch(const int64_t max_row_cnt);

  int rd_apply_patches(const int64_t max_row_cnt);

  int rd_patch_result(const int64_t idx, bool patch_first, bool patch_last);

  int rd_find_first_row_upper_bound(int64_t batch_size, int64_t &upper_bound);
  int rd_find_last_row_lower_bound(int64_t batch_size, int64_t &lower_bound);

  template <typename PartialMerge, typename ResFmt>
  int rd_merge_result(PartialMerge &part_res, WinFuncInfo &info, int64_t rd_col_id,
                      int64_t first_row_same_order_upper_bound,
                      int64_t last_row_same_order_lower_bound);



  OB_INLINE int64_t next_nonskip_row_index(int64_t cur_idx, const ObBatchRows &brs);

  int init_batch_ctx();

  int restore_child_vectors();

  int backup_child_vectors(int64_t batch_size);

  inline bool need_dump() const
  {
    return sql_mem_processor_.get_data_size() + local_mem_used() > sql_mem_processor_.get_mem_bound();
  }

  int init_mem_context();

  void destroy_mem_context();

  int update_mem_limit_version_periodically();

  inline double get_input_rows_mem_bound_ratio() const
  {
    return MY_SPEC.input_rows_mem_bound_ratio_;
  }
  int64_t local_mem_used() const
  {
    return local_allocator_->used();
  }
public:
  struct OpBatchCtx { // values used to help batch-calculation
    const ObCompactRow **stored_rows_;
    ObBitVector *nullres_skip_;
    ObBitVector *pushdown_skip_;
    ObBitVector *calc_wf_skip_;
    // used for window function evaluations
    // or tmp expr evaluation
    ObBitVector *bound_eval_skip_;
    int64_t *upper_pos_arr_;
    int64_t *lower_pos_arr_;
    LastCompactRow *tmp_wf_res_row_;
    LastCompactRow *tmp_input_row_;
    void *all_exprs_backup_buf_; // memory for backup/restore during compute_wf_values
    int32_t all_exprs_backup_buf_len_;

    OpBatchCtx() :
      stored_rows_(nullptr), nullres_skip_(nullptr), pushdown_skip_(nullptr),
      calc_wf_skip_(nullptr), bound_eval_skip_(nullptr), upper_pos_arr_(nullptr),
      lower_pos_arr_(nullptr), tmp_wf_res_row_(nullptr), tmp_input_row_(nullptr),
      all_exprs_backup_buf_(nullptr), all_exprs_backup_buf_len_(0)
    {}

    void reset()
    {
      if (tmp_wf_res_row_ != nullptr) {
        tmp_wf_res_row_->reset();
      }
      if (tmp_input_row_ != nullptr) {
        tmp_input_row_->reset();
      }
    }
  };
public:
  ObWindowFunctionVecOp::OpBatchCtx &get_batch_ctx() { return batch_ctx_; }
private:
  friend class WinFuncColExpr;
  template<typename T> friend class winfunc::WinExprWrapper;
  friend class winfunc::RowStores;

  friend class winfunc::StoreGuard;
private:
  common::ObArenaAllocator *local_allocator_;
  // this allocator will be reset in rescan
  common::ObArenaAllocator rescan_alloc_;
  ProcessStatus stat_;

  // only `cur_` is used for non-batch execution.
  winfunc::RowStores input_stores_;
  WinFuncColExprList wf_list_;
  ObVectorsResultHolder all_expr_vector_copy_;
  int64_t backuped_size_;

  int64_t last_output_row_idx_; // TODO: useless, remove this
  bool child_iter_end_;
  bool iter_end_;
  int64_t dir_id_;

  // Members for range distribution parallel execution
  // `rd`: is abbreviation for range distribution
  bool first_part_saved_;
  bool last_part_saved_;
  RDWinFuncPXPartialInfo *rd_patch_;
  common::ObArenaAllocator patch_alloc_; // TODO: maybe not used

  bool first_part_outputed_;
  bool patch_first_;
  bool patch_last_;

  int64_t last_computed_part_rows_;
  RowMeta *rd_coord_row_meta_;
  // row store iteration age to prevent output row datum released during the same batch

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

  // row meta for input row
  RowMeta input_row_meta_;
  // used for mapping pby rows to idx array
  int64_t max_pby_col_cnt_;
  int32_t *pby_row_mapped_idx_arr_;
  int32_t *last_row_idx_arr_;
  ObSEArray<ObExpr *, 8> all_part_exprs_;

  OpBatchCtx batch_ctx_;

  LastCompactRow *sp_merged_row_; // for single partition parallel execution
  RowMeta *all_wf_res_row_meta_;
  // for auto memory management
  lib::MemoryContext mem_context_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;

  // Each RowsStore may trigger a new mem_limit fetch, this records newest mem_limit version
  // Synchronize this version to others to let them update the mem_limit
  int64_t global_mem_limit_version_;
  // Only increase, not decrease, used for update_max_available_mem_size_periodically
  // Means the total count of rows which have been added to the each ra datum store
  int64_t amm_periodic_cnt_;

  ObTempBlockStore::IterationAge store_it_age_;
};

} // end sql
} // end oceanbase

#endif // OCEANBASE_WINDOW_FUNCTION_VEC_OP_H_