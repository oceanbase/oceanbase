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
#ifndef SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HASH_JOIN_STRUCT_H_
#define SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HASH_JOIN_STRUCT_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_scond.h"
#include "share/ob_define.h"
#include "sql/ob_sql_define.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/ob_batch_rows.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/engine/join/hash_join/ob_hj_partition.h"

namespace oceanbase
{
namespace sql
{

static const uint64_t END_ITEM = UINT64_MAX >> 1;

struct ObHJStoredRow : public ObCompactRow
{
  const static int64_t HASH_VAL_BIT = 63;
  const static int64_t STORED_ROW_PTR_BIT = 63;
  const static int64_t HASH_VAL_MASK = UINT64_MAX >> (64 - HASH_VAL_BIT);
  struct ExtraInfo
  {
    union {
      // %hash_value_ is set when row is add to chunk datum store.
      struct {
        uint64_t hash_val_:HASH_VAL_BIT;
        uint64_t is_match_:1;
      };
      // %next_row_ is set when stored row added to hash table.
      struct {
        uint64_t next_row_:STORED_ROW_PTR_BIT;
      };
      uint64_t v_;
    };

    ObHJStoredRow *get_next() const
    {
      return reinterpret_cast<ObHJStoredRow *>(next_row_);
    }

    void set_next(const ObHJStoredRow *ptr)
    {
      next_row_ = reinterpret_cast<uint64_t>(ptr);
    }
  };

  STATIC_ASSERT(sizeof(uint64_t) == sizeof(ExtraInfo), "unexpected size");

  ExtraInfo &get_extra_info(const RowMeta &row_meta)
  {
    static_assert(sizeof(ObHJStoredRow) == sizeof(ObCompactRow),
        "sizeof ObHJStoredRow must be the save with ObCompactRow");
    return *reinterpret_cast<ExtraInfo *>(get_extra_payload(row_meta));
  }

  const ExtraInfo &get_extra_info(const RowMeta &row_meta) const
  {
    return *reinterpret_cast<const ExtraInfo *>(get_extra_payload(row_meta));
  }

  uint64_t get_hash_value(const RowMeta &row_meta) const {
    return get_extra_info(row_meta).hash_val_;
  }

  void set_hash_value(const RowMeta &row_meta, const uint64_t hash_val)
  {
    get_extra_info(row_meta).hash_val_ = hash_val & HASH_VAL_MASK;
  }

  bool is_match(const RowMeta &row_meta) const
  {
    return get_extra_info(row_meta).is_match_;
  }

  void set_is_match(const RowMeta &row_meta, bool is_match)
  {
    get_extra_info(row_meta).is_match_ = is_match;
  }

  ObHJStoredRow *get_next(const RowMeta &row_meta) const
  {
    return get_extra_info(row_meta).get_next();
  }

  void set_next(const RowMeta &row_meta, ObHJStoredRow *ptr)
  {
    get_extra_info(row_meta).set_next(ptr);
  }

  static int convert_one_row_to_exprs(const ExprFixedArray &exprs,
                                      ObEvalCtx &eval_ctx,
                                      const RowMeta &row_meta,
                                      const ObHJStoredRow *row,
                                      const int64_t batch_idx);
  static int attach_rows(const ObExprPtrIArray &exprs,
                         ObEvalCtx &ctx,
                         const RowMeta &row_meta,
                         const ObHJStoredRow **srows,
                         const uint16_t selector[],
                         const int64_t size);
};

struct OutputInfo {
public:
  OutputInfo() : left_result_rows_(NULL),
                 selector_(NULL), selector_cnt_(0), first_probe_(true)
  {}
  void reuse() {
    first_probe_ = true;
    selector_cnt_ = 0;
  }
public:
  const ObHJStoredRow **left_result_rows_;
  uint16_t *selector_;
  uint16_t selector_cnt_;
  //indicates the batch data on the current probe side, whether it is the first time to probe
  bool first_probe_;
  int64_t max_output_ctx_;
};

struct ProbeBatchRows {
public:
  ProbeBatchRows() : from_stored_(false), stored_rows_(NULL),
                     brs_(), hash_vals_(NULL), key_data_(NULL)
  {}

  // support int64_t & int128_t
  int set_key_data(const ExprFixedArray *probe_keys,
                   ObEvalCtx *eval_ctx,
                   OutputInfo &output_info)
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(1 == probe_keys->count() || 2 == probe_keys->count());
    int64_t *dst = reinterpret_cast<int64_t *>(key_data_);
    int64_t key_cnt = probe_keys->count();
    for (int64_t key_idx  = 0; key_idx < key_cnt; key_idx++) {
      ObIVector *key_vec = probe_keys->at(key_idx)->get_vector(*eval_ctx);
      int64_t len = probe_keys->at(key_idx)->res_buf_len_;
      OB_ASSERT(8 == len);
      VectorFormat format = key_vec->get_format();
      LOG_DEBUG("set key data", K(format), K(len));
      if (VEC_FIXED == format) {
        if (1 == probe_keys->count()) {
          ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(key_vec);
          MEMCPY(key_data_, vec->get_data(),
                 8 * (output_info.selector_[output_info.selector_cnt_ - 1] + 1));
        } else {
          ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(key_vec);
          int64_t *src = reinterpret_cast<int64_t *>(vec->get_data());
          for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
            int64_t idx = output_info.selector_[i];
            dst[2 * idx + key_idx] = src[idx];
            LOG_DEBUG("key data", K(i), K(idx), K(key_idx), K(src[idx]));
          }
        }
      } else if (VEC_UNIFORM == format) {
        ObUniformBase *vec = static_cast<ObUniformBase *>(key_vec);
        for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
          int64_t idx = output_info.selector_[i];
          ObDatum &datum = vec->get_datums()[idx];
          OB_ASSERT(!datum.is_null());
          //MEMCPY(key_data_ + idx * len, datum.ptr_, len);
          dst[key_cnt * idx + key_idx] = datum.get_int();
          LOG_DEBUG("key data", K(i), K(idx), K(key_idx), K(datum), K(datum.get_int()));
        }
      } else if (VEC_UNIFORM_CONST == format) {
        ObUniformBase *vec = static_cast<ObUniformBase *>(key_vec);
        ObDatum &datum = vec->get_datums()[0];
        OB_ASSERT(!datum.is_null());
        for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
          int64_t idx = output_info.selector_[i];
          dst[key_cnt * idx + key_idx] = datum.get_int();
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid format", K(format), K(ret));
      }
    }

    return ret;
  }

public:
  bool from_stored_;
  const ObHJStoredRow **stored_rows_;
  ObBatchRows brs_; // replace right_brs_
  uint64_t *hash_vals_;
  // just used for normalized hash table
  char *key_data_;
};

struct JoinTableCtx {
public:
  JoinTableCtx() : eval_ctx_(NULL), join_type_(UNKNOWN_JOIN), is_shared_(false),
                   contain_ns_equal_(false), join_conds_(NULL), build_output_(NULL), probe_output_(NULL),
                   calc_exprs_(NULL), probe_opt_(false), build_keys_(NULL), probe_keys_(NULL),
                   build_key_proj_(NULL), probe_key_proj_(NULL), cur_bkid_(-1),
                   cur_tuple_(reinterpret_cast<void *>(END_ITEM)), max_output_cnt_(NULL),
                   cur_items_(NULL), stored_rows_(NULL), max_batch_size_(0),
                   output_info_(NULL), probe_batch_rows_(NULL)
  {}
  void reuse() {
    cur_bkid_ = -1;
    cur_tuple_ = reinterpret_cast<void *>(END_ITEM);
  }
  void reset()
  {
    build_row_meta_.reset();
    probe_row_meta_.reset();
  }
  bool need_mark_match() {
    return FULL_OUTER_JOIN == join_type_
           || LEFT_OUTER_JOIN == join_type_
           || LEFT_ANTI_JOIN == join_type_;
  }
  bool need_probe_del_match() {
    return LEFT_SEMI_JOIN == join_type_ || LEFT_ANTI_JOIN == join_type_;
  }
  void clear_one_row_eval_flag(int64_t batch_idx) {
    FOREACH_CNT(e, *calc_exprs_) {
      if ((*e)->is_batch_result()) {
        (*e)->get_evaluated_flags(*eval_ctx_).unset(batch_idx);
        (*e)->unset_null(*eval_ctx_, batch_idx);
      } else {
        (*e)->get_eval_info(*eval_ctx_).clear_evaluated_flag();
      }
    }
  }
public:
  ObEvalCtx *eval_ctx_;
  ObJoinType join_type_;
  bool is_shared_;
  bool contain_ns_equal_;

  const ExprFixedArray *join_conds_;
  const ExprFixedArray *build_output_;
  const ExprFixedArray *probe_output_;
  const ExprFixedArray *calc_exprs_;

  bool probe_opt_;
  const ExprFixedArray *build_keys_;
  const ExprFixedArray *probe_keys_;
  // In opt mode, the project subscript used to store the key in child output
  const ObFixedArray<int64_t, common::ObIAllocator> *build_key_proj_;
  const ObFixedArray<int64_t, common::ObIAllocator> *probe_key_proj_;
  RowMeta build_row_meta_;
  RowMeta probe_row_meta_;

  // used for output remain unmatch rows
  int64_t cur_bkid_;
  void *cur_tuple_;
  int64_t *max_output_cnt_;

  void **cur_items_;
  //template buffer for build table
  const ObHJStoredRow **stored_rows_;
  int64_t max_batch_size_;

  OutputInfo *output_info_;
  ProbeBatchRows *probe_batch_rows_;
};

struct ObHJSharedTableInfo
{
  int64_t sqc_thread_count_;

  ObSpinLock lock_;
  common::SimpleCond cond_;
  int64_t process_cnt_;
  int64_t close_cnt_;
  int64_t init_val_;
  int64_t sync_val_;

  int ret_;

  bool read_null_in_naaj_;
  bool non_preserved_side_is_not_empty_;

  int64_t total_memory_row_count_;
  int64_t total_memory_size_;
  int64_t open_cnt_;
  int open_ret_;
};

struct JoinPartitionRowIter {
public:
  JoinPartitionRowIter(ObHJPartition *hj_part,
                       int64_t row_bound = INT64_MAX,
                       int64_t mem_bound = INT64_MAX)
   : part_(hj_part), row_bound_(row_bound), mem_bound_(mem_bound), cur_mem_(0), cur_row_cnt_(0)
  {}

  int get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                     ObEvalCtx &ctx,
                     const int64_t max_rows,
                     int64_t &read_rows,
                     const ObHJStoredRow **stored_row) {
    int ret = OB_SUCCESS;
    if (mem_bound_ < INT64_MAX
        && (cur_mem_ > mem_bound_ || cur_row_cnt_ >= row_bound_)) {
      ret = OB_ITER_END;
    } else {
      int64_t max_read_rows = min(max_rows, row_bound_ - cur_row_cnt_);
      ret = part_->get_next_batch(exprs, ctx, max_read_rows, read_rows, stored_row);
    }
    if (OB_SUCC(ret)) {
      cur_row_cnt_ += read_rows;
      for (int64_t i = 0; OB_SUCC(ret) && i < read_rows; ++i) {
        cur_mem_ += stored_row[i]->get_row_size();
      }
    }
    return ret;
  }

  int get_next_batch(const ObHJStoredRow **stored_row,
                     const int64_t max_rows,
                     int64_t &read_rows) {
    int ret = OB_SUCCESS;
    if (mem_bound_ < INT64_MAX
        && (cur_mem_ > mem_bound_ || cur_row_cnt_ >= row_bound_)) {
      ret = OB_ITER_END;
    } else {
      int64_t max_read_rows = min(max_rows, row_bound_ - cur_row_cnt_);
      ret = part_->get_next_batch(stored_row, max_read_rows, read_rows);
    }
    if (OB_SUCC(ret)) {
      cur_row_cnt_ += read_rows;
      for (int64_t i = 0; OB_SUCC(ret) && i < read_rows; ++i) {
        cur_mem_ += stored_row[i]->get_row_size();
      }
    }
    return ret;
  }

public:
  ObHJPartition *part_;
  // used for NEST_LOOP MODE
  int64_t row_bound_;
  int64_t mem_bound_;
  // indicates how much memory the current iterator has read
  int64_t cur_mem_;
  int64_t cur_row_cnt_;
};

} // end namespace sql
} // end namespace oceanbase
#endif /* SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HASH_JOIN_STRUCT_H_*/
