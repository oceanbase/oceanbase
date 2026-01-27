/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "partition_by_comparator.h"
#include "ob_window_function_vec_op.h"

namespace oceanbase
{
namespace sql
{
namespace winfunc {

int PartitionByComparator::init(
    const ObMemAttr &attr, const WFInfoFixedArray &wf_infos,
    ObArenaAllocator *local_allocator, int64_t max_batch_size,
    WinFuncColExprList &wf_list, ObEvalCtx *eval_ctx, ObExprPtrIArray *all_expr,
    RowMeta *input_row_meta, char *all_exprs_backup_buf,
    int32_t all_exprs_backup_buf_len, const ObCompactRow **stored_rows,
    common::ObFixedArray<ObEvalInfo *, common::ObIAllocator>* eval_infos) {
  int ret = OB_SUCCESS;
  all_part_exprs_.set_attr(attr);
  eval_ctx_ = eval_ctx;
  all_exprs_ = all_expr;
  input_row_meta_ = input_row_meta;
  all_exprs_backup_buf_ = all_exprs_backup_buf;
  all_exprs_backup_buf_len_ = all_exprs_backup_buf_len;
  stored_rows_ = stored_rows;
  eval_infos_ = eval_infos;

  for (int wf_idx = 1; OB_SUCC(ret) && wf_idx <= wf_infos.count(); wf_idx++) {
    const WinFuncInfo &wf_info = wf_infos.at(wf_idx - 1);
    for (int j = 0; OB_SUCC(ret) && j < wf_info.partition_exprs_.count(); j++) {
      if (OB_FAIL(add_var_to_array_no_dup(all_part_exprs_,
                                          wf_info.partition_exprs_.at(j)))) {
        LOG_WARN("add element failed", K(ret));
      }
    }
  }

  max_pby_col_cnt_ = all_part_exprs_.count();
  if (OB_SUCC(ret) && max_pby_col_cnt_ > 0) {
    // init pby row mapped idx array
    int32_t arr_buf_size =
        max_pby_col_cnt_ * sizeof(int32_t) * (max_batch_size + 1);
    void *arr_buf = local_allocator->alloc(arr_buf_size);
    int32_t last_row_idx_arr_offset =
        max_pby_col_cnt_ * sizeof(int32_t) * max_batch_size;
    if (OB_ISNULL(arr_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      MEMSET(arr_buf, -1, arr_buf_size);
      pby_row_mapped_idx_arr_ = reinterpret_cast<int32_t *>(arr_buf);
      last_row_idx_arr_ = reinterpret_cast<int32_t *>((char *)arr_buf +
                                                      last_row_idx_arr_offset);
      for (WinFuncColExpr *it = wf_list.get_first();
           OB_SUCC(ret) && it != wf_list.get_header(); it = it->get_next()) {
        WinFuncPartitionByInfo &pby_info = it->pby_info_;

        pby_info.pby_row_mapped_idxes_ = (int32_t *)local_allocator->alloc(
            max_pby_col_cnt_ * sizeof(int32_t));
        if (OB_ISNULL(pby_info.pby_row_mapped_idxes_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(max_pby_col_cnt_),
                   K(sizeof(int32_t) * max_pby_col_cnt_));
        } else {
          MEMSET(pby_info.pby_row_mapped_idxes_, -1,
                 sizeof(int32_t) * max_pby_col_cnt_);
        }
        // we may have wf info part exprs like:
        // win_expr(T_WIN_FUN_RANK()), partition_by([testwn1.c], [testwn1.a],
        // [testwn1.b]), win_expr(T_WIN_FUN_RANK()), partition_by([testwn1.b],
        // [testwn1.a]) if so, we need a idx array to correctly compare
        // partition exprs

        // partition exprs may have `partition_by(t.a, t.a)`
        // in this case, reorderd_pby_row_idx_ will be [0, 0]
        bool same_part_order =
            (it->wf_info_.partition_exprs_.count() <= all_part_exprs_.count());
        for (int i = 0;
             OB_SUCC(ret) && i < it->wf_info_.partition_exprs_.count() &&
             same_part_order;
             i++) {
          same_part_order =
              (it->wf_info_.partition_exprs_.at(i) == all_part_exprs_.at(i));
        }
        if (OB_UNLIKELY(!same_part_order)) {
          LOG_TRACE("orders of partition exprs are different", K(it->wf_info_),
                    K(it->wf_info_.partition_exprs_), K(all_part_exprs_));
          const ObExprPtrIArray &part_exprs = it->wf_info_.partition_exprs_;

          pby_info.reordered_pby_row_idx_ = (int32_t *)local_allocator->alloc(
              sizeof(int32_t) * part_exprs.count());
          if (OB_ISNULL(pby_info.reordered_pby_row_idx_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            for (int i = 0; OB_SUCC(ret) && i < part_exprs.count(); i++) {
              int32_t idx = -1;
              for (int j = 0; idx == -1 && j < all_part_exprs_.count(); j++) {
                if (all_part_exprs_.at(j) == part_exprs.at(i)) {
                  idx = j;
                }
              }
              if (OB_UNLIKELY(idx == -1)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid part expr idx", K(ret));
              } else {
                pby_info.reordered_pby_row_idx_[i] = idx;
              }
            }
          }
        } else {
          pby_info.reordered_pby_row_idx_ = nullptr;
        }
      }
    }
  }
  LOG_TRACE("init partition by comparator", K(ret), K(max_pby_col_cnt_), K(local_allocator));
  return ret;
}

namespace {
int eval_single_row(const ObCompactRow *row, const ObCompactRow **input_stored_rows, ObEvalCtx& eval_ctx, RowMeta& input_row_meta, ObExprPtrIArray& all_exprs)
{
  int ret = OB_SUCCESS;
  input_stored_rows[0] = row;
  for (int i = 0; OB_SUCC(ret) && i < all_exprs.count(); i++) { // do not project const expr!!!
    if (all_exprs.at(i)->is_const_expr()) {// do nothing
    } else if (OB_FAIL(all_exprs.at(i)->init_vector_for_write(
                 eval_ctx, all_exprs.at(i)->get_default_res_format(), 1))) {
      LOG_WARN("init vector failed", K(ret));
    } else if (OB_FAIL(all_exprs.at(i)->get_vector(eval_ctx)->from_rows(input_row_meta, input_stored_rows, 1, i))) {
      LOG_WARN("from rows failed", K(ret));
    } else {
      all_exprs.at(i)->set_evaluated_projected(eval_ctx);
    }
  }
  return ret;
}
} // namespace

template <typename ColumnFmt>
int PartitionByComparator::mapping_pby_col_to_idx_arr(int32_t col_id, const ObExpr &part_expr,
                                                      const ObBatchRows &brs,
                                                      const cell_info *last_part_res)
{
  int ret = OB_SUCCESS;
  int32_t val_idx = col_id, step = max_pby_col_cnt_;
  const char *prev_data = nullptr, *cur_data = nullptr;
  int32_t prev_len = 0, cur_len = 0;
  int32_t prev = -1;
  int cmp_ret = 0;
  bool prev_is_null = false, cur_is_null = false;
  ColumnFmt *column = static_cast<ColumnFmt *>(part_expr.get_vector(*eval_ctx_));
  for (int i = 0; OB_SUCC(ret) && i < brs.size_; i++, val_idx +=step) {
    if (brs.skip_->at(i)) {
      continue;
    } else if (OB_LIKELY(prev != -1)) {
      column->get_payload(i, cur_is_null, cur_data, cur_len);
      if (OB_FAIL(part_expr.basic_funcs_->row_null_first_cmp_(
            part_expr.obj_meta_, part_expr.obj_meta_,
            prev_data, prev_len, prev_is_null,
            cur_data, cur_len, cur_is_null,
            cmp_ret))) {
        LOG_WARN("null first cmp failed", K(ret));
      } else if (cmp_ret == 0) {
        pby_row_mapped_idx_arr_[val_idx] = prev;
      } else {
        int32_t new_idx = prev + 1;
        pby_row_mapped_idx_arr_[val_idx] = new_idx;
        prev = new_idx;
        prev_data = cur_data;
        prev_len = cur_len;
        prev_is_null = cur_is_null;
      }
    } else if (last_part_res != nullptr) {
      column->get_payload(i, cur_is_null, cur_data, cur_len);
      prev_is_null = last_part_res->is_null_;
      prev_len = last_part_res->len_;
      prev_data = last_part_res->payload_;
      if (OB_FAIL(part_expr.basic_funcs_->row_null_first_cmp_(
            part_expr.obj_meta_, part_expr.obj_meta_,
            prev_data, prev_len, prev_is_null,
            cur_data, cur_len, cur_is_null, cmp_ret))) {
        LOG_WARN("compare failed", K(ret));
      } else if (cmp_ret == 0) {
        prev = last_row_idx_arr_[col_id];
        pby_row_mapped_idx_arr_[val_idx] = prev;
      } else {
        prev = last_row_idx_arr_[col_id] + 1;
        pby_row_mapped_idx_arr_[val_idx] = prev;
        prev_data = cur_data;
        prev_len = cur_len;
        prev_is_null = cur_is_null;
      }
    } else {
      pby_row_mapped_idx_arr_[val_idx] = i;
      column->get_payload(i, prev_is_null, prev_data, prev_len);
      prev = i;
    }
  }
  return ret;
}

int PartitionByComparator::eval_prev_part_exprs(const ObCompactRow *last_row, ObIAllocator &alloc,
                                                const ObExprPtrIArray &part_exprs,
                                                common::ObIArray<cell_info> &last_part_infos)
{
  int ret = OB_SUCCESS;
  bool backuped_child_vector = false;
  ObDataBuffer backup_alloc(all_exprs_backup_buf_, all_exprs_backup_buf_len_);
  ObVectorsResultHolder tmp_holder(&backup_alloc);
  for (int i = 0; OB_SUCC(ret) && last_row != nullptr && i < part_exprs.count(); i++) {
    ObExpr *part_expr = part_exprs.at(i);
    int part_expr_field_idx = -1;
    for (int j = 0; j < all_exprs_->count() && part_expr_field_idx == -1; j++) {
      if (part_expr == all_exprs_->at(j)) {
        part_expr_field_idx = j;
      }
    }
    if (part_expr_field_idx != -1) { // partition expr is child input
      const char *payload = nullptr;
      bool is_null = false;
      int32_t len = 0;
      last_row->get_cell_payload(*input_row_meta_, part_expr_field_idx, payload, len);
      is_null = last_row->is_null(part_expr_field_idx);
      if (OB_FAIL(last_part_infos.push_back(cell_info(is_null, len, payload)))) {
        LOG_WARN("push back element failed", K(ret));
      }
    } else {
      if (backuped_child_vector) {
      } else if (OB_FAIL(tmp_holder.init(*all_exprs_, *eval_ctx_))) {
        LOG_WARN("init result holder failed", K(ret));
      } else if (OB_FAIL(tmp_holder.save(1))) {
        LOG_WARN("save vector results failed", K(ret));
      } else {
        backuped_child_vector = true;
        if (OB_FAIL(eval_single_row(last_row, stored_rows_, *eval_ctx_, *input_row_meta_, *all_exprs_))) {
          LOG_WARN("attach row failed", K(ret));
        }
      }
      int64_t mock_skip_data = 0;
      ObBitVector *mock_skip = to_bit_vector(&mock_skip_data);
      EvalBound tmp_bound(1, true);
      char *part_res_buf = nullptr;
      const char *payload = nullptr;
      int32_t len = 0;
      bool is_null = false;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(part_expr->eval_vector(*eval_ctx_, *mock_skip, tmp_bound))) {
        LOG_WARN("eval vector failed", K(ret));
      } else {
        ObIVector *part_res_vec = part_expr->get_vector(*eval_ctx_);
        part_res_vec->get_payload(0, is_null, payload, len);
        if (is_null || len <= 0) {
          part_res_buf = nullptr;
          len = 0;
        } else if (OB_ISNULL(part_res_buf = (char *)alloc.alloc(len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(part_res_buf, payload, len);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(last_part_infos.push_back(cell_info(is_null, len, part_res_buf)))) {
          LOG_WARN("push back element failed", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (backuped_child_vector && OB_FAIL(tmp_holder.restore())) {
    LOG_WARN("restore vector results failed", K(ret));
  } else if (backuped_child_vector) {
    // clear evaluated flags anyway
    clear_evaluated_flag();
  }
  return ret;
}

// In vectorization 2.0, data accessing of expr is specified by `VectorFormat`,
// there's no easy way to access a complete partition row with multiple columns.
// In order to easily calculate partition, we map pby expr to idx array here.
// For each partition expr, pby_mapped_idx_arr[row_idx] = (get_payload(row_idx) == get_payload(row_idx - 1) ?
//                                                           pby_mapped_idx_arr[row_idx-1]
//                                                           : row_idx);
// For example, suppose partition exprs are `<int, int, int>` and inputs are:
//  1, 2, 3
//  1, 2, 3
//  1, 2, 4
//  1, 2, 4
//  2, 3, 1
//  2, 3, 1
// mapped idx arrays are:
//  0, 0, 0
//  0, 0, 0
//  0, 0, 2
//  0, 0, 2
//  4, 4, 4
//  4, 4, 4
int PartitionByComparator::mapping_pby_row_to_idx_arr(
    const ObBatchRows &child_brs, const ObCompactRow *last_row,
    const int64_t tenant_id) {
#define MAP_FIXED_COL_CASE(vec_tc)                                                                 \
  case (vec_tc): {                                                                                 \
    ret = mapping_pby_col_to_idx_arr<ObFixedLengthFormat<RTCType<vec_tc>>>(                        \
      i, *part_exprs.at(i), child_brs, last_part_cell);                                            \
  } break

  int ret = OB_SUCCESS;
  ObIArray<ObExpr *> &part_exprs = all_part_exprs_;
  ObArenaAllocator tmp_mem_alloc(ObModIds::OB_SQL_WINDOW_LOCAL, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                 tenant_id,
                                 ObCtxIds::WORK_AREA);
  ObSEArray<cell_info, 8> part_cell_infos;
  // calculate last part expr of previous batch first
  // memset all idx to -1
  if (max_pby_col_cnt_ > 0) {
    MEMSET(pby_row_mapped_idx_arr_, -1, child_brs.size_ * sizeof(int32_t) * max_pby_col_cnt_);
    if (OB_FAIL(eval_prev_part_exprs(last_row, tmp_mem_alloc, part_exprs, part_cell_infos))) {
      LOG_WARN("eval last partition exprs of last row failed", K(ret));
    }
    bool prev_row_not_null = (last_row != nullptr);
    for (int i = 0; OB_SUCC(ret) && i < part_exprs.count(); i++) {
      const cell_info *last_part_cell = (prev_row_not_null ? &(part_cell_infos.at(i)) : nullptr);
      if (OB_ISNULL(part_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret));
      } else if (OB_FAIL(part_exprs.at(i)->eval_vector(*eval_ctx_, child_brs))) {
        LOG_WARN("eval vector failed", K(ret));
      } else {
        VectorFormat fmt = part_exprs.at(i)->get_format(*eval_ctx_);
        VecValueTypeClass tc = part_exprs.at(i)->get_vec_value_tc();
        switch (fmt) {
        case VEC_FIXED: {
          switch (tc) {
            LST_DO_CODE(MAP_FIXED_COL_CASE, FIXED_VEC_LIST);
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vector type class", K(tc));
          }
          }
          break;
        }
        case VEC_UNIFORM: {
          ret = mapping_pby_col_to_idx_arr<ObUniformFormat<false>>(i, *part_exprs.at(i), child_brs,
                                                                   last_part_cell);
          break;
        }
        case VEC_UNIFORM_CONST: {
          ret = mapping_pby_col_to_idx_arr<ObUniformFormat<true>>(i, *part_exprs.at(i), child_brs,
                                                                  last_part_cell);
          break;
        }
        case VEC_DISCRETE: {
          ret = mapping_pby_col_to_idx_arr<ObDiscreteFormat>(i, *part_exprs.at(i), child_brs,
                                                             last_part_cell);
          break;
        }
        case VEC_CONTINUOUS: {
          ret = mapping_pby_col_to_idx_arr<ObContinuousFormat>(i, *part_exprs.at(i), child_brs,
                                                               last_part_cell);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected data format", K(ret), K(fmt), K(tc));
        }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("mapping pby col to idx array failed", K(ret), K(i), K(*part_exprs.at(i)));
        }
      }
    }

    // record last_row_idx_arr_
    for (int i = child_brs.size_ - 1; i >= 0; i--) {
      if (child_brs.skip_->at(i)) {
      } else {
        MEMCPY(last_row_idx_arr_, &(pby_row_mapped_idx_arr_[i * max_pby_col_cnt_]),
               max_pby_col_cnt_ * sizeof(int32_t));
        break;
      }
    }
  }
  return ret;
#undef MAP_FIXED_COL_CASE
}

int PartitionByComparator::save_pby_row_for_wf(WinFuncColExpr* start, WinFuncColExpr *end_wf, const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  if (max_pby_col_cnt_ > 0) {
    int32_t offset = batch_idx * max_pby_col_cnt_;
    int32_t *pby_row_idxes = &(pby_row_mapped_idx_arr_[offset]);
    for (WinFuncColExpr *it = start; it != end_wf; it = it->get_next()) {
      MEMCPY(it->pby_info_.pby_row_mapped_idxes_, pby_row_idxes, sizeof(int32_t) * max_pby_col_cnt_);
    }
  }
  return ret;
}

int PartitionByComparator::check_same_partition(WinFuncColExpr &wf_col, bool &same)
{
  int ret = OB_SUCCESS;
  same = (wf_col.wf_info_.partition_exprs_.count() <= 0);
  if (!same) {
    int64_t part_cnt = wf_col.wf_info_.partition_exprs_.count();
    int64_t row_idx = eval_ctx_->get_batch_idx();
    int32_t offset = max_pby_col_cnt_ * row_idx;
    int32_t *pby_row_idxes = &(pby_row_mapped_idx_arr_[offset]);
    if (OB_LIKELY(wf_col.pby_info_.reordered_pby_row_idx_ == nullptr)) {
      same = (MEMCMP(pby_row_idxes, wf_col.pby_info_.pby_row_mapped_idxes_, sizeof(int32_t) * part_cnt) == 0);
    } else {
      same = true;
      for (int i = 0; i < part_cnt && same; i++) {
        int32_t idx = wf_col.pby_info_.reordered_pby_row_idx_[i];
        same = (wf_col.pby_info_.pby_row_mapped_idxes_[idx] == pby_row_idxes[idx]);
      }
    }
  }
  return ret;
}

int PartitionByComparator::find_same_partition_of_wf(WinFuncColExprList& wf_infos, WinFuncColExpr*& end)
{
  int ret = OB_SUCCESS;
  bool same = false;
  end = wf_infos.get_header();
  for (WinFuncColExpr *it = wf_infos.get_first(); OB_SUCC(ret) && it != wf_infos.get_header(); it = it->get_next()) {
    if (OB_FAIL(check_same_partition(*it, same))) {
      LOG_WARN("check same partition failed", K(ret));
    } else if (same) {
      end = it;
      break;
    }
  }
  return ret;
}

// same as ObOperator::clear_evaluated_flag()
void PartitionByComparator::clear_evaluated_flag() {
  for (int i = 0; i < eval_infos_->count(); i++) {
    eval_infos_->at(i)->clear_evaluated_flag();
  }
}

} // namespace winfunc
} // namespace sql
} // namespace oceanbase