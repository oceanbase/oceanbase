/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_TABLE_GLOBAL_INDEX_LOOKUP_EXECUTOR_H_
#define OCEANBASE_OBSERVER_TABLE_GLOBAL_INDEX_LOOKUP_EXECUTOR_H_

#include "ob_table_scan_executor.h"
#include "sql/engine/table/ob_index_lookup_op_impl.h"

namespace oceanbase
{
namespace table
{
class ObTableGlobalIndexLookupExecutor : public ObIndexLookupOpImpl
{
public:
  ObTableGlobalIndexLookupExecutor(ObTableApiScanExecutor *scan_executor);
  int open();
  int close();
  void destroy();
  // implement of virtual func
  virtual void do_clear_evaluated_flag();
  virtual int get_next_row_from_index_table() override;
  virtual int process_data_table_rowkey() override;
  virtual int do_index_lookup() override;
  virtual int get_next_row_from_data_table() override;
  virtual int get_next_rows_from_data_table(int64_t &count, int64_t capacity) override;
  virtual int process_next_index_batch_for_row() override;
  virtual int process_next_index_batch_for_rows(int64_t &count) override;
  virtual int process_data_table_rowkeys(const int64_t size, const ObBitVector *skip) override;
  virtual bool need_next_index_batch() const override {  return need_force_index_scan_stop_ || !index_end_; }
  virtual int check_lookup_row_cnt() override;
  virtual int do_index_table_scan_for_rows(const int64_t max_row_cnt,
                                           const int64_t start_group_idx,
                                           const int64_t default_row_batch_cnt) override;
  virtual bool is_group_scan() const override { return false; }
  virtual int init_group_range(int64_t cur_group_idx, int64_t group_size) override { return common::OB_NOT_IMPLEMENT; }
  virtual void update_state_in_output_rows_state(int64_t &count) override { lookup_row_cnt_ += count; }
  virtual void update_states_in_finish_state() override {}
  virtual void update_states_after_finish_state() override {}
  virtual int64_t get_index_group_cnt() const override { return 0; }
  virtual int64_t get_lookup_group_cnt() const override { return 0; }
  virtual void inc_index_group_cnt() override { /* do nothing */ }
  virtual void inc_lookup_group_cnt() override { /* do nothing */ }
  virtual ObEvalCtx& get_eval_ctx() override { return scan_executor_->eval_ctx_; }
  virtual const ExprFixedArray& get_output_expr() override { return get_ctdef().output_exprs_; }

  int build_data_table_range(common::ObNewRange &lookup_range);
private:
  static const int64_t GLOBAL_INDEX_LOOKUP_MEM_LIMIT_PERCENT = 50;
  static const int64_t DEFAULT_BATCH_ROW_COUNT = 10000;
private:
  OB_INLINE const ObTableApiScanSpec& get_spec() const { return scan_executor_->get_spec(); }
  OB_INLINE const ObTableApiScanCtDef& get_ctdef() { return get_spec().get_ctdef(); }
  OB_INLINE ObExpr* get_calc_part_id_expr() { return get_ctdef().calc_part_id_expr_; }
  OB_INLINE ObDASTableLocMeta* get_loc_meta() { return get_ctdef().lookup_loc_meta_; }
  OB_INLINE const ObDASScanCtDef* get_lookup_ctdef() { return get_ctdef().lookup_ctdef_; }
  bool has_das_scan_task(const ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_op);
  int64_t get_memory_limit(uint64_t tenant_id) { return lib::get_tenant_memory_limit(tenant_id) * GLOBAL_INDEX_LOOKUP_MEM_LIMIT_PERCENT / 100; }

private:
  common::ObArenaAllocator allocator_;
  ObTableApiScanExecutor *scan_executor_;
  ObTableCtx &tb_ctx_;
  ObDASRef das_ref_;
  DASOpResultIter lookup_result_;
  bool need_force_index_scan_stop_;
  ObNewRange lookup_range_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableGlobalIndexLookupExecutor);
};

} // table
} // oceanbase
#endif // OCEANBASE_OBSERVER_TABLE_GLOBAL_INDEX_LOOKUP_EXECUTOR_H_
