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

#ifndef OCEANBASE_BASIC_OB_GROUP_JOIN_BUFFER_V2_H_
#define OCEANBASE_BASIC_OB_GROUP_JOIN_BUFFER_V2_H_

#include "sql/engine/basic/ob_vector_result_holder.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/ob_operator.h"
#include "sql/das/ob_das_context.h"
namespace oceanbase
{
namespace sql
{

class ObDriverRowBuffer
{
public:
 ObDriverRowBuffer();
 ~ObDriverRowBuffer() {}

public:
  int get_next_left_row();
  int rescan_left();
  int fill_cur_row_group_param();

  void bind_group_params_to_das_ctx(GroupParamBackupGuard &guard);
  int get_next_left_batch(int64_t max_rows, const ObBatchRows *&batch_rows);
  int get_cur_group_id() const { return cur_group_idx_; }
  int get_group_rescan_cnt() const { return group_rescan_cnt_; }

  int init(ObOperator *op, const int64_t max_group_size, const int64_t group_scan_size, const common::ObIArray<ObDynamicParamSetter> *rescan_params);
  void destroy();
  void reset();
  bool is_multi_level() const { return is_multi_level_; }

  const GroupParamArray *get_rescan_params_info() const { return &rescan_params_info_; }
private:
  bool need_fill_group_buffer() { return !(left_store_iter_.is_valid() && left_store_iter_.has_next()); }
  int batch_fill_group_buffer(const int64_t max_row_cnt);
  int get_next_batch_from_store(int64_t max_rows, int64_t &read_rows);


  void reset_buffer_state();
  bool is_full() { return left_store_.get_row_cnt() >= group_scan_size_; }
  int add_row_to_store();
  int deep_copy_dynamic_obj();
  int init_group_params();
  bool is_inited() { return is_inited_; }

  int init_left_batch_rows();
private:
  // the elements of join buffer
  ObOperator *op_;
  ObOperator *left_;
  ObBatchRows left_brs_;
  const ObOpSpec *spec_;
  ObExecContext *ctx_;
  ObEvalCtx *eval_ctx_;
  const common::ObIArray<ObDynamicParamSetter> *rescan_params_;
  ObTempRowStore left_store_;
  ObTempRowStore::Iterator left_store_iter_;
  lib::MemoryContext mem_context_; // for dynamic param copying, will reset after each group rescan

  int64_t cur_group_idx_;
  int64_t group_rescan_cnt_;
  common::ObArrayWrap<ObSqlArrayObj> group_params_;
  GroupParamArray rescan_params_info_;
  ObVectorsResultHolder last_batch_;

  int64_t max_group_size_;
  int64_t group_scan_size_;
  int64_t left_store_read_;
  union {
    uint64_t flags_;
    struct {
      uint64_t is_inited_                                  : 1;
      uint64_t need_check_above_                           : 1;
      uint64_t is_multi_level_                             : 1;
      uint64_t is_left_end_                                : 1;
      uint64_t save_last_row_                              : 1;
      uint64_t save_last_batch_                            : 1;
      uint64_t skip_rescan_right_                          : 1;
      uint64_t reserved_                                   : 57;
    };
  };
};

class ObDriverRowIterator
{
public:
  ObDriverRowIterator();
  ~ObDriverRowIterator() { }
  int get_next_left_row();
  int rescan_left();
  void bind_group_params_to_das_ctx(GroupParamBackupGuard &guard);
  int fill_cur_row_group_param();
  int get_next_left_batch(int64_t max_rows, const ObBatchRows *&batch_rows);
  int save_right_batch(const common::ObIArray<ObExpr *> &exprs);
  int right_rows_extend(int64_t rows_cnt, int64_t &times);
  void set_real_ouptut_right_batch_times(int64_t times)
  {
    right_extended_times_ = times;
  }
  int drive_row_extend(int size);
  int extend_left_next_batch_rows(int64_t &expect_rows_cnt, int64_t times);
  int restore_drive_row(int from_idx, int to_idx);
  int64_t get_left_batch_idx() { return l_idx_; }
  int64_t get_left_valid_rows_cnt()
  {
    return left_brs_ == nullptr
               ? 0
               : left_brs_->size_ - left_brs_->skip_->accumulate_bit_cnt(left_brs_->size_);
  }

  int get_cur_group_id() const { return join_buffer_.get_cur_group_id(); }
  int get_group_rescan_cnt() const { return join_buffer_.get_group_rescan_cnt(); }
  int64_t get_left_batch_size() { return left_brs_ == nullptr ? 0 : left_brs_->size_; }

  int init(ObOperator *op, const int64_t op_group_scan_size,
          const common::ObIArray<ObDynamicParamSetter> *rescan_params, 
          bool is_group_rescan, bool need_backup_left);
  void destroy();

  void reset();
  bool is_multi_level_group_rescan() const { return is_group_rescan_ && join_buffer_.is_multi_level(); }
  const GroupParamArray *get_rescan_params_info() const { return join_buffer_.get_rescan_params_info(); }
private:
  int get_min_vec_size_from_drive_row(int &min_vec_size);

private:
  // NOTE: alloc a new batchrows here
  const ObBatchRows *left_brs_;
  int64_t l_idx_;
  ObOperator *op_;
  ObOperator *left_;
  ObDriverRowBuffer join_buffer_;
  ObVectorsResultHolder left_batch_;
  ObVectorsResultHolder right_batch_;
  int64_t right_extended_times_;
  const common::ObIArray<ObDynamicParamSetter> *rescan_params_;
  bool is_group_rescan_;
  ObEvalCtx *eval_ctx_;
  int64_t op_max_batch_size_;
  // for nlj, the left batches will be extended by a single row, so we need backup it
  bool need_backup_left_;
  int left_expr_extend_size_;
  ObExecContext *ctx_;
};

} // end namespace sql 
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_GROUP_JOIN_BUFFER_V2_H_