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

#ifndef OCEANBASE_WINDOW_FUNCTION_ROWSTORE_H_
#define OCEANBASE_WINDOW_FUNCTION_ROWSTORE_H_

#include "lib/container/ob_2d_array.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/basic/ob_temp_row_store.h"

namespace oceanbase
{
namespace sql
{

class ObWindowFunctionVecOp;

namespace winfunc
{
class RowStores;

class StoreGuard
{
public:
  StoreGuard(ObWindowFunctionVecOp &op);
  ~StoreGuard() {}
private:
  ObWindowFunctionVecOp &op_;
};

struct RowStore
{
public:
  RowStore(const int64_t tenant_id, ObIAllocator *store_alloc, ObIAllocator *arena_alloc, RowStores &store_set) :
    store_set_(store_set),
    allocator_(arena_alloc),
    ra_rs_(store_alloc),
    ra_reader_(),
    row_cnt_(0),
    stored_row_cnt_(0),
    output_row_idx_(0),
    row_ptrs_(nullptr),
    local_mem_limit_version_(0)
  {}

  ~RowStore() { destroy(); }

  int init(const int64_t max_batch_size, const RowMeta &row_meta, const lib::ObMemAttr &mem_attr,
           const int64_t mem_limit, bool enable_dump);

  void destroy()
  {
    reset();
    ra_rs_.~ObRATempRowStore();
    allocator_ = nullptr;
  }

  void reset()
  {
    row_cnt_ = 0;
    stored_row_cnt_ = 0;
    output_row_idx_ = 0;
    ra_reader_.reset();
    ra_rs_.reset();
    local_mem_limit_version_ = 0;
  }

  inline int64_t count() const { return row_cnt_; }
  // computed but not outputed row cnt
  inline int64_t to_output_rows() const { return row_cnt_ - output_row_idx_; }
  // not computed row cnt
  inline int64_t to_compute_rows() const { return stored_row_cnt_ - row_cnt_; }
  inline bool is_empty() const { return stored_row_cnt_ == 0; }

  OB_INLINE int get_row(const int64_t row_idx, const ObCompactRow *&sr)
  {
    return ra_reader_.get_row(row_idx, sr);
  }

  int add_batch_rows(const ObIArray<ObExpr *> &exprs, const RowMeta &row_meta, ObEvalCtx &eval_ctx,
                     const EvalBound &bound, const ObBitVector &skip, bool add_row_cnt,
                     ObCompactRow **stored_rows = nullptr, bool is_input = false);
  int attach_rows(const ObIArray<ObExpr *> &exprs, const RowMeta &row_meta, ObEvalCtx &eval_ctx,
                  const int64_t start_idx, const int64_t end_idx, bool use_reserv_buf);

  int attach_rows(ObExpr *expr, const RowMeta &row_meta, ObEvalCtx &eval_ctx,
                  const int64_t start_idx, const int64_t end_idx, bool use_reserve_buf);

  int get_batch_rows(const int64_t start_idx, const int64_t end_idx,
                     const ObCompactRow **stored_rows);

  int process_dump(const int64_t target_size, const int64_t g_mem_limit_versiom, int64_t &dumped_size);

public:
  RowStores &store_set_;
  ObIAllocator *allocator_;
  sql::ObRATempRowStore ra_rs_;
  sql::ObRATempRowStore::RAReader ra_reader_;
  // record begin idx of current partition. always zero for rows_store_  TODO: maybe useless
  // int64_t begin_idx_;
  // cnt of rows computed
  // `ObWinfowFunctionVecOp::compute_wf_values` uses row_cnt_ to inlcude rows participating computing
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
  ObCompactRow **row_ptrs_;
  int64_t local_mem_limit_version_;

  TO_STRING_KV(K_(row_cnt), K_(stored_row_cnt), K_(output_row_idx), K(ra_rs_.get_mem_used()),
               K(ra_rs_.get_mem_hold()), K_(local_mem_limit_version));
};

struct RowStores
{
  RowStores() : op_(nullptr),processed_(NULL), cur_(NULL), first_(NULL), last_(NULL)
  {}

  void set_operator(ObWindowFunctionVecOp *op) { op_ = op; }
  int init(const int64_t max_batch_size, const RowMeta &row_meta, const lib::ObMemAttr &mem_attr,
           const int64_t mem_limit, bool enable_dump)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(processed_) || OB_ISNULL(cur_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null store", K(processed_), K(cur_));
    } else if (OB_FAIL(processed_->init(max_batch_size, row_meta, mem_attr, mem_limit, enable_dump))) {
      SQL_LOG(WARN, "init store failed", K(ret));
    } else if (OB_FAIL(cur_->init(max_batch_size, row_meta, mem_attr, mem_limit, enable_dump))) {
      SQL_LOG(WARN, "init store failed", K(ret));
    } else if (OB_NOT_NULL(first_) || OB_NOT_NULL(last_)) {
      if (OB_ISNULL(first_) || OB_ISNULL(last_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid null store", K(first_), K(last_));
      } else if (OB_FAIL(first_->init(max_batch_size, row_meta, mem_attr, mem_limit, enable_dump))) {
        SQL_LOG(WARN, "init store failed", K(ret));
      } else if (OB_FAIL(last_->init(max_batch_size, row_meta, mem_attr, mem_limit, enable_dump))) {
        SQL_LOG(WARN, "init store failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(setup_mem_mgr())) {
      SQL_LOG(WARN, "setup memory manager failed", K(ret));
    }
    return ret;
  }

  int process_dump(bool is_input);

  virtual ~RowStores()
  {
    destroy();
  }

  void swap_cur_processed()
  {
    std::swap(cur_, processed_);
  }

  void swap_first_processed()
  {
    std::swap(first_, processed_);
  }

  void swap_first_cur()
  {
    std::swap(first_, cur_);
  }

  void swap_last_cur()
  {
    std::swap(last_, cur_);
  }

  void set_it_age(ObTempBlockStore::IterationAge *age);

  void reset()
  {
#define RESET_STORE(store)                                                                         \
  do {                                                                                             \
    if (store != nullptr) { store->reset(); }                                                      \
  } while (false)

    LST_DO_CODE(RESET_STORE, processed_, cur_, first_, last_);
#undef RESET_STORE
  }

  void destroy()
  {
#define DESTROY_STORE(store)                                                                       \
  do {                                                                                             \
    if ((store) != nullptr) { store->destroy(); }                                                  \
  } while (false)

    LST_DO_CODE(DESTROY_STORE, processed_, cur_, first_, last_);
    processed_ = nullptr;
    cur_ = nullptr;
    first_ = nullptr;
    last_ = nullptr;
    op_ = nullptr;
#undef DESTROY_STORE
  }

  TO_STRING_KV(K(processed_), K(first_), K(last_), K(cur_));
private:
  int setup_mem_mgr();

  bool need_check_dump(int64_t g_mem_limit_version);
public:
  ObWindowFunctionVecOp *op_;
  // `processed_` is rows calculated but not outputed, only used in vectorized execution
  RowStore *processed_;
  // current operation rows
  RowStore *cur_;
  // first and last partition for range distribute window function
  RowStore *first_;
  RowStore *last_;
};
} // end winfunc
} // end sql
} // end oceanbase
#endif // OCEANBASE_WINDOW_FUNCTION_ROWSTORE_H_