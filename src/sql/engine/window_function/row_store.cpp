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
#define USING_LOG_PREFIX SQL_ENG

#include "row_store.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/window_function/ob_window_function_vec_op.h"

namespace oceanbase
{
namespace sql
{

namespace winfunc
{

StoreGuard::StoreGuard(ObWindowFunctionVecOp &op): op_(op)
{

  op_.input_stores_.set_it_age(&op_.store_it_age_);
  for (WinFuncColExpr *it = op_.wf_list_.get_first(); it != op_.wf_list_.get_header(); it = it->get_next()) {
    it->res_->set_it_age(&op_.store_it_age_);
  }
  op_.store_it_age_.inc();
}

int RowStore::init(const int64_t max_batch_size, const RowMeta &row_meta,
                   const lib::ObMemAttr &mem_attr, const int64_t mem_limit, bool enable_dump)
{
  int ret = OB_SUCCESS;
  void *ptr_buf = nullptr;
  if (OB_FAIL(ra_rs_.init(row_meta, max_batch_size, mem_attr, mem_limit, enable_dump, NONE_COMPRESSOR))) {
    LOG_WARN("init ra temp row store failed", K(ret));
  } else if (OB_FAIL(ra_reader_.init(&ra_rs_))) {
    LOG_WARN("init ra reader failed", K(ret));
  } else if (OB_ISNULL(ptr_buf = allocator_->alloc(sizeof(ObCompactRow *) * max_batch_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    row_ptrs_ = static_cast<ObCompactRow **>(ptr_buf);
  }
  return ret;
}
int RowStore::add_batch_rows(const ObIArray<ObExpr *> &exprs, const RowMeta &row_meta,
                             ObEvalCtx &eval_ctx, const EvalBound &bound, const ObBitVector &skip,
                             bool add_row_cnt, ObCompactRow **stored_rows /*null*/, bool is_input)
{
  int ret = OB_SUCCESS;
  bool need_addding = (skip.accumulate_bit_cnt(bound) < bound.range_size());
  if (OB_SUCC(ret) && need_addding) {
    int64_t stored_row_cnt = 0;
    int added_cnt = bound.range_size() - skip.accumulate_bit_cnt(bound);
    if (OB_FAIL(ra_rs_.add_batch(exprs, eval_ctx, bound, skip, stored_row_cnt,
                                 (stored_rows == nullptr ? row_ptrs_ : stored_rows)))) {
      SQL_ENG_LOG(WARN, "add batch rows failed", K(ret));
    } else if (OB_UNLIKELY(stored_row_cnt != added_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected added row count", K(stored_row_cnt), K(added_cnt));
    } else if (FALSE_IT(stored_row_cnt_ += added_cnt)) {
      // do nothing
    } else if (add_row_cnt) {
      row_cnt_ += added_cnt;
    }
    // process dump after adding batch rows
    if (OB_SUCC(ret) && OB_FAIL(store_set_.process_dump(is_input))) {
      LOG_WARN("process dump failed", K(ret));
    }
  }
  return ret;
}

int RowStore::get_batch_rows(const int64_t start_idx, const int64_t end_idx,
                             const ObCompactRow **stored_rows)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = 0;
  if (OB_UNLIKELY(end_idx - start_idx == 0)) {
    //do nothing
  } else if (OB_FAIL(ra_reader_.get_batch_rows(start_idx, end_idx, read_rows, stored_rows))) {
    SQL_ENG_LOG(WARN, "get batch rows failed", K(ret), K(start_idx), K(end_idx));
  } else if (OB_UNLIKELY(read_rows != end_idx - start_idx)) {
    SQL_ENG_LOG(WARN, "unexpected read rows", K(ret), K(read_rows), K(end_idx), K(start_idx));
  }
  return ret;
}

int RowStore::attach_rows(const ObIArray<ObExpr *> &exprs, const RowMeta &row_meta,
                          ObEvalCtx &eval_ctx, const int64_t start_idx, const int64_t end_idx,
                          bool use_reserve_buf)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObIVector *, 8> vec_ptrs;
  if (OB_UNLIKELY(end_idx - start_idx == 0)) {
    // do nothing
  } else {
    const ObCompactRow **stored_rows = const_cast<const ObCompactRow **>(row_ptrs_);
    if (OB_FAIL(get_batch_rows(start_idx, end_idx, stored_rows))) {
      LOG_WARN("get batch rows failed", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      if (exprs.at(i)->is_const_expr()) { // do nothing
      } else if (OB_FAIL(exprs.at(i)->init_vector(eval_ctx, exprs.at(i)->get_default_res_format(),
                                           end_idx - start_idx, use_reserve_buf))) {
        LOG_WARN("init vector for write failed", K(ret));
      } else if (OB_UNLIKELY(!is_valid_format(exprs.at(i)->get_format(eval_ctx)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid format", K(ret));
      } else if (OB_FAIL(exprs.at(i)->get_vector(eval_ctx)->from_rows(row_meta, stored_rows,
                                                                      end_idx - start_idx, i))) {
        LOG_WARN("from rows failed", K(ret));
      }
    }
    // set evaluated and projected after attaching rows
    for (int i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      exprs.at(i)->set_evaluated_projected(eval_ctx);
    }
  }
  return ret;
}

int RowStore::attach_rows(ObExpr *expr, const RowMeta &row_meta, ObEvalCtx &eval_ctx,
                          const int64_t start_idx, const int64_t end_idx, bool use_reserve_buf)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr *, 1> tmp_exprs;
  if (OB_FAIL(tmp_exprs.push_back(expr))) {
    LOG_WARN("push back element failed", K(ret));
  } else if (OB_FAIL(
               attach_rows(tmp_exprs, row_meta, eval_ctx, start_idx, end_idx, use_reserve_buf))) {
    LOG_WARN("attach rows failed", K(ret));
  }
  return ret;
}

int RowStore::process_dump(const int64_t target_size, const int64_t g_mem_limit_versiom,
                           int64_t &dumped_size)
{
  int ret = OB_SUCCESS;
  local_mem_limit_version_ = g_mem_limit_versiom;
  int64_t mem_used = ra_rs_.get_mem_used();
  dumped_size = 0;
  if (ra_rs_.is_empty_save_block_cnt()) {
    LOG_DEBUG("no need dumping"); // CHANGE TO DEBUG
  } else if (OB_FAIL(ra_rs_.dump(false, target_size))) {
    LOG_WARN("dump store failed", K(ret));
  } else {
    dumped_size = mem_used - ra_rs_.get_mem_used();
    // reset reader after dumping
    ra_reader_.reset();
    LOG_TRACE("dumped store", K(*this), K(dumped_size), K(local_mem_limit_version_), K(target_size));
  }
  return ret;
}

// int RowStore::add_row(const ObCompactRow *row, ObCompactRow *&stored_row, bool inc_row_cnt, bool is_input /* false */)
// {
//   int ret = OB_SUCCESS;

//   if (OB_FAIL(ra_rs_.add_row(row, stored_row))) {
//     LOG_WARN("add row failed", K(ret));
//   } else if (OB_FAIL(store_set_.process_dump(is_input))) {
//     LOG_WARN("process dump failed", K(ret));
//   } else {
//     row_cnt_ += inc_row_cnt;
//     stored_row_cnt_ += 1;
//   }
//   return ret;
// }

// ================ RowStores ================
int RowStores::process_dump(bool is_input)
{
#define LOG_STORE(store)                                                                           \
  if (store != nullptr) { LOG_DEBUG("store info", K(*store)); }
#define CALC_DUMP_SIZE(store)                                                                      \
  if (store != nullptr) { target_dump_size += store->ra_rs_.get_mem_used(); }

#define DUMP_STORE(store)                                                                          \
  do {                                                                                             \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (target_dump_size > 0 && store != nullptr && store->ra_rs_.get_mem_used() > 0) {     \
      if (OB_FAIL(                                                                                 \
            store->process_dump(target_dump_size, op_->global_mem_limit_version_, dumped_size))) { \
        LOG_WARN("process dump failed", K(ret));                                                   \
      } else {                                                                                     \
        target_dump_size -= dumped_size;                                                           \
      }                                                                                            \
    }                                                                                              \
  } while (0)

  int ret = OB_SUCCESS;
  bool need_dump = false;
  int dump_errsim = OB_E(EventTable::EN_FORCE_WINFUNC_STORE_DUMP) OB_SUCCESS;
  bool force_dump = (dump_errsim == OB_ALLOCATE_MEMORY_FAILED);
  bool force_no_dump = (dump_errsim != OB_SUCCESS) && !force_dump;
  int64_t target_dump_size = 0;
  if (OB_UNLIKELY(force_no_dump)) {
    // do nothing
  } else if (OB_UNLIKELY(force_dump)) {
    LST_DO_CODE(CALC_DUMP_SIZE, first_, processed_, cur_); // dump all
  } else if (OB_ISNULL(op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("row stores not inited", K(ret));
  } else if (OB_FAIL(op_->update_mem_limit_version_periodically())) {
    LOG_WARN("update global memory limit version failed", K(ret));
  } else if (need_check_dump(op_->global_mem_limit_version_)) {
    // op_local_mem is needed for winfunc operator to work properly, it wont't change after opening operator.
    int64_t op_local_mem = op_->local_mem_used();
    if (is_input) {
      const static double MEM_DISCOUNT_FOR_PART_END = 0.8;
      const double mem_bound_ratio =
        op_->get_input_rows_mem_bound_ratio() * MEM_DISCOUNT_FOR_PART_END;
      target_dump_size = -(op_->sql_mem_processor_.get_mem_bound() * mem_bound_ratio - op_local_mem);
      LST_DO_CODE(CALC_DUMP_SIZE, first_, processed_, cur_);
    } else { // for result
      // dump BIG_BLOCK_SIZE * 2 at least, because dump is already needed at this time,
      // ensure the minimum amount of memory for each dump.
      const static int64_t MIN_DUMP_SIZE = (256L << 10) * 2;
      target_dump_size = (op_->sql_mem_processor_.get_data_size() - op_->sql_mem_processor_.get_mem_bound());
      target_dump_size = MAX(target_dump_size, MIN_DUMP_SIZE);
    }
  }
  // TODO: chang log level
  LST_DO_CODE(LOG_STORE, first_, processed_, cur_, last_);
  if (OB_UNLIKELY(target_dump_size > 0)) {
    LOG_TRACE("dumping winfunc store", K(target_dump_size), K(force_dump), K(force_no_dump),
              K(op_->global_mem_limit_version_), K(dump_errsim),
              K(op_->sql_mem_processor_.get_data_size()),
              K(op_->sql_mem_processor_.get_mem_bound()), K(op_->local_mem_used()));
    // dumping order: first_, processed_, cur_
    int64_t dumped_size = 0;
    LST_DO_CODE(DUMP_STORE, first_, processed_, cur_);
    if (OB_SUCC(ret)) { op_->sql_mem_processor_.set_number_pass(1); }
  }
  return ret;

#undef LOG_STORE
#undef DUMP_STORE
#undef CALC_DUMP_SIZE
}

bool RowStores::need_check_dump(int64_t g_mem_limit_version)
{
#define CHECK_VERSION(store)\
  if (store != nullptr) {\
    ret |= (store->local_mem_limit_version_ != g_mem_limit_version);\
  }

  bool ret = false;
  LST_DO_CODE(CHECK_VERSION, first_, last_, cur_, processed_);
  return ret;

#undef CHECK_VERSION
}

int RowStores::setup_mem_mgr()
{
#define SETUP_STORE(store)                                                                         \
  if (store != nullptr) {                                                                          \
    store->ra_rs_.set_dir_id(op_->sql_mem_processor_.get_dir_id());                                \
    store->ra_rs_.set_mem_stat(&(op_->sql_mem_processor_));                                        \
    store->ra_rs_.set_io_event_observer(&(op_->io_event_observer_));                               \
  }

  int ret = OB_SUCCESS;
  if (OB_ISNULL(op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("empty operator", K(ret));
  } else {
    LST_DO_CODE(SETUP_STORE, first_, last_, processed_, cur_);
    LOG_TRACE("trace init sql mem mgr for window function",
                  K(op_->profile_.get_cache_size()), K(op_->profile_.get_expect_size()), K(ret));
  }

#undef SETUP_STORE
  return ret;
}

void RowStores::set_it_age(ObTempBlockStore::IterationAge *age)
{
  if (cur_ != nullptr) { cur_->ra_reader_.set_iteration_age(age); }
  if (processed_ != nullptr) { processed_->ra_reader_.set_iteration_age(age); }
  if (first_ != nullptr) { first_->ra_reader_.set_iteration_age(age); }
  if (last_ != nullptr) { last_->ra_reader_.set_iteration_age(age); }
}
} // end winfunc
} // end sql
} // end oceanbase