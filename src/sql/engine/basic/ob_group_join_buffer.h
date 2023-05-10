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

#ifndef OCEANBASE_BASIC_OB_GROUP_JOIN_BUFFER_H_
#define OCEANBASE_BASIC_OB_GROUP_JOIN_BUFFER_H_

#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace sql
{
struct ObBatchRowDatums
{
  ObBatchRowDatums() { reset(); }
  int init(const ObExprPtrIArray *exprs, common::ObIAllocator *alloc, int32_t batch_size);
  void reset()
  {
    alloc_ = NULL;
    exprs_ = NULL;
    batch_size_ = 0;
    datums_ = NULL;
    skip_ = NULL;
    size_ = 0;
    saved_size_ = 0;
    is_inited_ = false;
  }
  void clear_saved_size() { saved_size_ = 0; }
  void from_exprs(ObEvalCtx &ctx, ObBitVector *skip, int64_t size);
  void extend_save(ObEvalCtx &ctx, int64_t size);
  void to_exprs(ObEvalCtx &ctx);
  void to_exprs(ObEvalCtx &ctx, int64_t from_idx, int64_t to_idx);
  ObDatum &get_datum(int64_t row_id, int64_t col_id)
  {
    return datums_[col_id * batch_size_ + row_id];
  }
  ObBitVector *get_skip() { return skip_; }
  int32_t get_size() { return size_; }
  bool is_inited() const { return is_inited_; }

private:
  common::ObIAllocator *alloc_;
  const ObExprPtrIArray *exprs_;
  int32_t batch_size_;
  ObDatum *datums_;
  ObBitVector *skip_;
  int32_t size_;
  int32_t saved_size_; // record  the saved size, include extend saved size
  bool is_inited_;
};

class ObGroupJoinBufffer
{
public:
  ObGroupJoinBufffer();
  ~ObGroupJoinBufffer() {} // does not free memory
  int init(ObOperator *op,
           const int64_t max_group_size,
           const int64_t group_scan_size,
           const common::ObIArray<ObDynamicParamSetter> *rescan_params,
           const common::ObIArray<ObDynamicParamSetter> *left_rescan_params,
           const common::ObIArray<ObDynamicParamSetter> *right_rescan_params);
  bool is_inited() const { return is_inited_; }
  bool is_full() const { return left_store_.get_row_cnt() >= group_scan_size_; }
  bool need_fill_group_buffer() { return !(left_store_iter_.is_valid() && left_store_iter_.has_next()); }
  bool is_multi_level() const { return is_multi_level_; }
  int has_next_left_row(bool &has_next);
  int init_above_group_params();
  int fill_cur_row_group_param();
  int drain_left();
  int rescan_left();
  int rescan_right();
  int fill_group_buffer();
  int batch_fill_group_buffer(const int64_t max_row_cnt, const ObBatchRows *&batch_rows);
  int get_next_row_from_store();
  int get_next_batch_from_store(int64_t max_rows, int64_t &read_rows);
  ObBatchRowDatums &get_last_batch() { return last_batch_; }
  void destroy();
private:
  int init_group_params();
  int deep_copy_dynamic_obj();
  int bind_group_params_to_store();
  int prepare_rescan_params();
  int get_next_left_iter();
  int add_row_to_store();
  int build_above_group_params(const common::ObIArray<ObDynamicParamSetter> &above_rescan_params,
                               common::ObIArray<ObSqlArrayObj *> &above_group_params,
                               int64_t &group_size);
  int set_above_group_size();
  void reset_buffer_state();
  int backup_above_params(common::ObIArray<ObObjParam> &left_params_backup,
                          common::ObIArray<ObObjParam> &right_params_backup);
  int restore_above_params(common::ObIArray<ObObjParam> &left_params_backup,
                           common::ObIArray<ObObjParam> &right_params_backup);

private:
  ObOperator *op_;
  const ObOpSpec *spec_;
  ObExecContext *ctx_;
  ObEvalCtx *eval_ctx_;
  ObOperator *left_;
  ObOperator * right_;
  const common::ObIArray<ObDynamicParamSetter> *rescan_params_;
  const common::ObIArray<ObDynamicParamSetter> *left_rescan_params_;
  const common::ObIArray<ObDynamicParamSetter> *right_rescan_params_;
  lib::MemoryContext mem_context_; // for dynamic param copying, will reset after each group rescan
  // buffer for rows read from left child
  ObChunkDatumStore left_store_;
  ObChunkDatumStore::Iterator left_store_iter_;
  // for multi level batch rescan
  //           NLJ 1
  //           / \
  //      TSC 1   NLJ 2
  //              / \
  //         TSC 2   TSC 3
  // During NLJ 2's rescan, NLJ 1 may have supplied a batch of params to TSC 2 and TSC 3.
  // Thus, NLJ 2 need to keep track of NLJ 1's batch rescan pace and make sure only output
  // rows corresponding to NLJ 2's current rescan.
  //
  // store batch index corresponding to NLJ above this op
  common::ObSEArray<int64_t, 2> left_store_group_idx_;
  // for NLJ 2, above_left_batch_params_ stores batch rescan params used by TSC 2 and supplied by NLJ 1
  common::ObSEArray<ObSqlArrayObj *, 2> above_left_group_params_;
  // for NLJ 2, above_right_batch_params_ stores batch rescan params used by TSC 3 and supplied by NLJ 1
  common::ObSEArray<ObSqlArrayObj *, 2> above_right_group_params_;
  // batch rescan params supplied by this op
  common::ObArrayWrap<ObSqlArrayObj> group_params_;
  // for NLJ 2, we need to rewrite params in above_right_batch_params_ and align them with our rescan pace,
  // above_bnlj_params_ stores batch params supplied by NLJ 1 and overwritten by NLJ 2
  common::ObArrayWrap<ObSqlArrayObj> above_group_params_;
  ObChunkDatumStore::ShadowStoredRow last_row_;
  ObBatchRowDatums last_batch_;
  int64_t right_cnt_;
  int64_t cur_group_idx_;
  // rows read from left_store_iter_
  int64_t left_store_read_;
  // index used when filling group buffer,
  // see fill_group_buffer() and batch_fill_group_buffer()
  int64_t above_group_idx_for_expand_;
  // index used when reading from right child,
  // see get_next_row_from_store() and get_next_batch_from_store()
  int64_t above_group_idx_for_read_;
  int64_t above_group_size_;
  int64_t max_group_size_;
  int64_t group_scan_size_;
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
} // end namespace sql
} // end namespace oceanbase
#endif // OCEANBASE_BASIC_OB_GROUP_JOIN_BUFFER_H_
