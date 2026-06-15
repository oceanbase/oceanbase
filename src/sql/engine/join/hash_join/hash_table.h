/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_
#define SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_

#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/join/hash_join/ob_hash_join_struct.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace sql
{
struct Int64Key {
  inline void init_data(const ObFixedArray<int64_t, common::ObIAllocator> *key_proj,
                 const RowMeta &row_meta,
                 const ObHJStoredRow *row) {
    OB_ASSERT(1 == key_proj->count());
    data_ = *(reinterpret_cast<const int64_t *>(row->get_cell_payload(row_meta, key_proj->at(0))));
  }
  inline bool operator==(const Int64Key &other) const {
    return data_ == other.data_;
  }
  TO_STRING_KV(K_(data));

public:
  int64_t data_;
}__attribute__ ((packed));

struct Int128Key {
  inline void init_data(const ObFixedArray<int64_t, common::ObIAllocator> *key_proj,
                 const RowMeta &row_meta,
                 const ObHJStoredRow *row) {
    OB_ASSERT(2 == key_proj->count());
    data_0_ = *(reinterpret_cast<const int64_t *>(row->get_cell_payload(row_meta, key_proj->at(0))));
    data_1_ = *(reinterpret_cast<const int64_t *>(row->get_cell_payload(row_meta, key_proj->at(1))));
  }
  inline bool operator==(const Int128Key &other) const {
    return data_0_ == other.data_0_ && data_1_ == other.data_1_;
  }
  TO_STRING_KV(K_(data_0), K_(data_1));

public:
  int64_t data_0_;
  int64_t data_1_;
}__attribute__ ((packed));

struct GenericBucket {
public:
  GenericBucket() : val_(0)
  {}
  explicit inline GenericBucket(uint64_t value) : val_(value)
  {}
  explicit inline GenericBucket(uint64_t salt, ObHJStoredRow *row_ptr)
      : row_ptr_(reinterpret_cast<uint64_t>(row_ptr) & POINTER_MASK), salt_(salt)
  {}
  inline void init(const JoinTableCtx &ctx, uint64_t salt, ObHJStoredRow *row_ptr)
  {
    // ctx is used for normalized bucket
    UNUSED(ctx);
    row_ptr_ = reinterpret_cast<uint64_t>(row_ptr) & POINTER_MASK;
    salt_ = salt;
  }
  inline void reset()
  {
    val_ = 0;
  }
  inline bool used() const
  {
    return 0 != val_;
  }
  inline uint64_t get_salt()
  {
    return salt_;
  }
  inline ObHJStoredRow *get_stored_row()
  {
    return reinterpret_cast<ObHJStoredRow *>(row_ptr_);
  }
  inline void set_salt(uint64_t salt)
  {
    salt_ = salt;
  }
  inline void set_row_ptr(ObHJStoredRow *row_ptr)
  {
    row_ptr_ = reinterpret_cast<uint64_t>(row_ptr) & POINTER_MASK;
  }
  static inline uint64_t extract_salt(const uint64_t hash_val)
  {
    return (hash_val & SALT_MASK) >> POINTER_BIT;
  }
  static inline uint64_t extract_row_ptr(const uint64_t hash_val)
  {
    return (hash_val & POINTER_MASK);
  }
  TO_STRING_KV(K_(row_ptr), K_(salt));

public:
  // Upper 16 bits are salt
  static constexpr const uint64_t SALT_MASK = 0xFFFF000000000000;
  // Lower 48 bits are the pointer
  static constexpr const uint64_t POINTER_MASK = 0x0000FFFFFFFFFFFF;
  static constexpr const uint64_t POINTER_BIT = 48;
  union {
    struct { // FARM COMPAT WHITELIST
      uint64_t row_ptr_ : 48;
      uint64_t salt_ : 16;
    };
    uint64_t val_;
  };
};

struct RobinBucket : public GenericBucket {
public:
  RobinBucket() : GenericBucket(), dist_(0)
  {}
  RobinBucket(uint64_t salt, ObHJStoredRow *row_ptr) : GenericBucket(salt, row_ptr), dist_(0)
  {}
  ~RobinBucket()
  {}
  OB_INLINE void set_dist(uint32_t new_dist)
  {
    dist_ = new_dist;
  }
  OB_INLINE void inc_dist()
  {
    dist_ += 1;
  }
  OB_INLINE uint32_t get_dist() {
    return dist_;
  }
  TO_STRING_KV(K_(this->row_ptr), K_(this->salt), K_(dist));

  uint32_t dist_;
};

template <typename T>
struct NormalizedRobinBucket : public RobinBucket {
public:
  NormalizedRobinBucket<T>() : RobinBucket()
  {}
  NormalizedRobinBucket<T>(uint64_t salt, ObHJStoredRow *row_ptr) : RobinBucket(salt, row_ptr)
  {}
  ~NormalizedRobinBucket()
  {}
  inline void init(const JoinTableCtx &ctx, uint64_t salt, ObHJStoredRow *row_ptr)
  {
    this->salt_ = salt;
    this->row_ptr_ = reinterpret_cast<uint64_t>(row_ptr) & POINTER_MASK;
    ;
    // normalized key
    key_.init_data(ctx.build_key_proj_, ctx.build_row_meta_, row_ptr);
  }
  inline void init_data(const JoinTableCtx &ctx, ObHJStoredRow *row_ptr)
  {
    key_.init_data(ctx.build_key_proj_, ctx.build_row_meta_, row_ptr);
  }
  TO_STRING_KV(K_(this->row_ptr), K_(this->salt), K_(this->dist), K_(key));

public:
  T key_;
};

template <typename T>
struct NormalizedBucket : public GenericBucket {
public:
  NormalizedBucket<T>() : GenericBucket()
  {}
  NormalizedBucket<T>(uint64_t salt, ObHJStoredRow *row_ptr) : GenericBucket(salt, row_ptr)
  {}
  ~NormalizedBucket()
  {}
  inline void init(const JoinTableCtx &ctx, uint64_t salt, ObHJStoredRow *row_ptr)
  {
    this->salt_ = salt;
    this->row_ptr_ = reinterpret_cast<uint64_t>(row_ptr) & POINTER_MASK;
    ;
    // normalized key
    key_.init_data(ctx.build_key_proj_, ctx.build_row_meta_, row_ptr);
  }
  inline void init_data(const JoinTableCtx &ctx, ObHJStoredRow *row_ptr)
  {
    key_.init_data(ctx.build_key_proj_, ctx.build_row_meta_, row_ptr);
  }
  TO_STRING_KV(K_(this->row_ptr), K_(this->salt), K_(key));

public:
  T key_;
};

template<typename Bucket>
struct ProberBase {
  virtual ~ProberBase<Bucket>() {};
  int calc_join_conditions(
      JoinTableCtx &ctx, ObHJStoredRow *left_row, const int64_t batch_idx, bool &matched);
  int calc_other_join_conditions_batch(JoinTableCtx &ctx, const ObHJStoredRow **rows_ptr,
      const uint16_t *sel, const uint16_t sel_cnt);
  virtual int equal(JoinTableCtx &ctx, Bucket *bucket, const int64_t batch_idx, bool &is_equal) = 0;

  virtual int equal_batch(JoinTableCtx &ctx,
                    const uint16_t *sel,
                    const uint16_t sel_cnt,
                    bool is_opt) = 0;
private:
  int convert_exprs_batch_one(const ExprFixedArray &exprs,
                              ObEvalCtx &eval_ctx,
                              const RowMeta &row_meta,
                              const ObHJStoredRow *row,
                              const int64_t batch_idx);
};

template <typename T>
struct NormalizedProber final : public ProberBase<NormalizedBucket<T>> {
  using Bucket = NormalizedBucket<T>;
  NormalizedProber<T>()
  {}
  ~NormalizedProber()
  {}
  static int64_t get_normalized_key_size()
  {
    return sizeof(T);
  }
  int equal(JoinTableCtx &ctx, Bucket *bucket, const int64_t batch_idx, bool &is_equal) override
  {
    char *key_data = ctx.probe_batch_rows_->key_data_;
    is_equal = (bucket->key_ == *(reinterpret_cast<T *>(key_data + batch_idx * sizeof(T))));
    LOG_DEBUG("is equal",
        K(batch_idx),
        K(*bucket),
        K(bucket->key_),
        K(*(reinterpret_cast<T *>(key_data + batch_idx * sizeof(T)))));
    return OB_SUCCESS;
  }
  int equal_batch(
      JoinTableCtx &ctx, const uint16_t *sel, const uint16_t sel_cnt, bool is_opt) override
  {
    UNUSED(is_opt);
    char *key_data = ctx.probe_batch_rows_->key_data_;
    for (int64_t i = 0; i < sel_cnt; i++) {
      uint16_t batch_idx = sel[i];
      ctx.cmp_ret_map_[i] = (reinterpret_cast<Bucket *>(ctx.unmatched_bkts_[i])->key_ ==
                                *(reinterpret_cast<T *>(key_data + batch_idx * sizeof(T))))
                                ? 0
                                : 1;
      LOG_DEBUG("is equal",
          K(i),
          K(*reinterpret_cast<Bucket *>(ctx.unmatched_bkts_[i])),
          K(*(reinterpret_cast<T *>(key_data + batch_idx * sizeof(T)))));
    }
    return OB_SUCCESS;
  }
};

template <typename T>
struct NormalizedRobinProber final : public ProberBase<NormalizedRobinBucket<T>> {
  using Bucket = NormalizedRobinBucket<T>;
  NormalizedRobinProber<T>()
  {}
  ~NormalizedRobinProber()
  {}
  static int64_t get_normalized_key_size()
  {
    return sizeof(T);
  }
  int equal(JoinTableCtx &ctx, Bucket *bucket, const int64_t batch_idx, bool &is_equal) override
  {
    char *key_data = ctx.probe_batch_rows_->key_data_;
    is_equal = (bucket->key_ == *(reinterpret_cast<T *>(key_data + batch_idx * sizeof(T))));
    LOG_DEBUG("is equal",
        K(batch_idx),
        K(*bucket),
        K(bucket->key_),
        K(*(reinterpret_cast<T *>(key_data + batch_idx * sizeof(T)))));
    return OB_SUCCESS;
  }
  int equal_batch(
      JoinTableCtx &ctx, const uint16_t *sel, const uint16_t sel_cnt, bool is_opt) override
  {
    UNUSED(is_opt);
    char *key_data = ctx.probe_batch_rows_->key_data_;
    for (int64_t i = 0; i < sel_cnt; i++) {
      uint16_t batch_idx = sel[i];
      ctx.cmp_ret_map_[i] = (reinterpret_cast<Bucket *>(ctx.unmatched_bkts_[i])->key_ ==
                                *(reinterpret_cast<T *>(key_data + batch_idx * sizeof(T))))
                                ? 0
                                : 1;
      LOG_DEBUG("is equal",
          K(i),
          K(*reinterpret_cast<Bucket *>(ctx.unmatched_bkts_[i])),
          K(*(reinterpret_cast<T *>(key_data + batch_idx * sizeof(T)))));
    }
    return OB_SUCCESS;
  }
};


template <typename Bucket>
struct GenericProber final : public ProberBase<Bucket> {
  template <int LEN>
  OB_INLINE static bool fixed_len_not_equal(const char *l_data, const char *r_data) {
    if constexpr (LEN == 1) {
      return *reinterpret_cast<const uint8_t*>(l_data) != *reinterpret_cast<const uint8_t*>(r_data);
    } else if constexpr (LEN == 2) {
      return *reinterpret_cast<const uint16_t*>(l_data) != *reinterpret_cast<const uint16_t*>(r_data);
    } else if constexpr (LEN == 4) {
      return *reinterpret_cast<const uint32_t*>(l_data) != *reinterpret_cast<const uint32_t*>(r_data);
    } else {
      return *reinterpret_cast<const uint64_t*>(l_data) != *reinterpret_cast<const uint64_t*>(r_data);
    }
  }

  template <int LEN, bool HAS_NULL, bool NULL_SAFE>
  OB_INLINE static bool compare_fixed_batch_impl(
      const uint16_t *sel,
      const uint16_t sel_cnt,
      ObCompactRow **unmatched_rows,
      const int64_t build_col_idx,
      const int64_t offset,
      const RowMeta &build_row_meta,
      ObFixedLengthBase *r_vec,
      const int64_t r_len,
      int *cmp_ret) {
    const char *r_data = r_vec->get_data();
    bool need_fallback = false;
    for (int64_t i = 0; !need_fallback && i < sel_cnt; ++i) {
      const int64_t curr_idx = sel[i];
      ObCompactRow *it = unmatched_rows[i];

      if constexpr (HAS_NULL) {
        const bool l_isnull = it->is_null(build_col_idx);
        const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
        if (l_isnull != r_isnull) {
          cmp_ret[i] |= 1;
          continue;
        } else if (l_isnull && r_isnull) {
          cmp_ret[i] |= (NULL_SAFE ? 0 : 1);
          continue;
        }
      }

      const char *l_cell = (offset >= 0) ? it->get_fixed_cell_payload(offset)
                                          : it->get_cell_payload(build_row_meta, build_col_idx);
      if (OB_UNLIKELY(LEN == 0)) {
        need_fallback = (0 != memcmp(l_cell, r_data + r_len * curr_idx, r_len));
      } else {
        need_fallback = fixed_len_not_equal<LEN>(l_cell, r_data + r_len * curr_idx);
      }
    }
    return need_fallback;
  }

  template <bool HAS_NULL, bool NULL_SAFE>
  OB_INLINE static bool compare_fixed_batch(
      const uint16_t *sel,
      const uint16_t sel_cnt,
      ObCompactRow **unmatched_rows,
      const int64_t build_col_idx,
      const int64_t offset,
      const RowMeta &build_row_meta,
      ObFixedLengthBase *r_vec,
      const int64_t r_len,
      int *cmp_ret) {
    switch (r_len) {
      case 1: return compare_fixed_batch_impl<1, HAS_NULL, NULL_SAFE>(
                  sel, sel_cnt, unmatched_rows, build_col_idx, offset, build_row_meta, r_vec, r_len, cmp_ret);
      case 2: return compare_fixed_batch_impl<2, HAS_NULL, NULL_SAFE>(
                  sel, sel_cnt, unmatched_rows, build_col_idx, offset, build_row_meta, r_vec, r_len, cmp_ret);
      case 4: return compare_fixed_batch_impl<4, HAS_NULL, NULL_SAFE>(
                  sel, sel_cnt, unmatched_rows, build_col_idx, offset, build_row_meta, r_vec, r_len, cmp_ret);
      case 8: return compare_fixed_batch_impl<8, HAS_NULL, NULL_SAFE>(
                  sel, sel_cnt, unmatched_rows, build_col_idx, offset, build_row_meta, r_vec, r_len, cmp_ret);
      default: return compare_fixed_batch_impl<0, HAS_NULL, NULL_SAFE>(
                  sel, sel_cnt, unmatched_rows, build_col_idx, offset, build_row_meta, r_vec, r_len, cmp_ret);
    }
  }

  static int64_t get_normalized_key_size()
  {
    return 0;
  }
  int equal(JoinTableCtx &ctx, Bucket *bucket, const int64_t batch_idx, bool &is_equal)
  {
    int ret = OB_SUCCESS;
    is_equal = true;
    ObHJStoredRow *build_sr = bucket->get_stored_row();
    int cmp_ret = 0;
    for (int64_t i = 0; is_equal && i < ctx.build_key_proj_->count(); i++) {
      int64_t build_col_idx = ctx.build_key_proj_->at(i);
      ObExpr *probe_key = ctx.probe_keys_->at(i);
      ObIVector *vec = probe_key->get_vector(*ctx.eval_ctx_);
      const char *r_v = NULL;
      ObLength r_len = 0;
      build_sr->get_cell_payload(ctx.build_row_meta_, build_col_idx, r_v, r_len);
      vec->null_first_cmp(*probe_key, batch_idx,
                          build_sr->is_null(build_col_idx),
                          r_v, r_len, cmp_ret);
      is_equal = (cmp_ret == 0);
      LOG_DEBUG("generic probe equal", K(cmp_ret), K(is_equal));
    }

    return ret;
  }
  int equal_batch(
      JoinTableCtx &ctx, const uint16_t *sel, const uint16_t sel_cnt, bool is_opt)
  {
    int ret = OB_SUCCESS;
    MEMSET(ctx.cmp_ret_map_, 0, sizeof(int) * sel_cnt);
    MEMSET(ctx.cmp_ret_for_one_col_, 0, sizeof(int) * sel_cnt);

    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.build_key_proj_->count(); i++) {
      int64_t build_col_idx = ctx.build_key_proj_->at(i);
      ObExpr *probe_key = ctx.probe_keys_->at(i);
      bool need_fallback = false;
      ObIVector *vec = probe_key->get_vector(*ctx.eval_ctx_);
      ObExpr *build_key = ctx.build_keys_->at(i);
      NullSafeRowCmpFunc cmp_func = nullptr;
      // join key type on build and probe may be different
      if (ctx.probe_cmp_funcs_.at(i) != nullptr) {
        cmp_func = ctx.probe_cmp_funcs_.at(i);
      } else {
        cmp_func = ctx.build_cmp_funcs_.at(i);
      }

      bool not_null_cmp = (!ctx.build_cols_have_null_->at(i) && !ctx.probe_cols_have_null_->at(i));

      if (not_null_cmp && OB_FAIL(inner_process_column_not_null(probe_key, *ctx.eval_ctx_, sel, sel_cnt,
                                                  reinterpret_cast<ObCompactRow **>(ctx.unmatched_rows_),
                                                  build_col_idx, ctx.build_row_meta_,
                                                  cmp_func, build_key->obj_meta_, probe_key->obj_meta_,
                                                  ctx.cmp_ret_map_))) {
        LOG_WARN("failed to compare with no null", K(ret));
      } else if (!not_null_cmp) {
        ret = ctx.is_ns_equal_cond_->at(i) ?
                inner_process_column<true>(probe_key, *ctx.eval_ctx_,
                                           sel, sel_cnt,
                                           reinterpret_cast<ObCompactRow **>(ctx.unmatched_rows_),
                                           build_col_idx, ctx.build_row_meta_,
                                           cmp_func, build_key->obj_meta_, probe_key->obj_meta_,
                                           ctx.cmp_ret_map_) :
                inner_process_column<false>(probe_key, *ctx.eval_ctx_,
                                            sel, sel_cnt,
                                            reinterpret_cast<ObCompactRow **>(ctx.unmatched_rows_),
                                            build_col_idx, ctx.build_row_meta_,
                                            cmp_func, build_key->obj_meta_, probe_key->obj_meta_,
                                            ctx.cmp_ret_map_);
      }
    }
    return ret;
  }

  int inner_process_column_not_null(const ObExpr *probe_key,
                                    ObEvalCtx &eval_ctx,
                                    const uint16_t *sel,
                                    const uint16_t sel_cnt,
                                    ObCompactRow **unmatched_rows,
                                    const int64_t build_col_idx,
                                    const RowMeta &build_row_meta,
                                    NullSafeRowCmpFunc cmp_func,
                                    const ObObjMeta &build_obj_meta,
                                    const ObObjMeta &probe_obj_meta,
                                    int *cmp_ret) {
    int ret = OB_SUCCESS;
    switch (probe_key->get_format(eval_ctx)) {
      case VEC_FIXED : {
        ObFixedLengthBase *r_vec = static_cast<ObFixedLengthBase *> (probe_key->get_vector(eval_ctx));
        int64_t r_len = r_vec->get_length();
        const int64_t offset = build_row_meta.fixed_expr_reordered()
                                ? build_row_meta.get_fixed_cell_offset(build_col_idx) : -1;
        bool need_fallback = compare_fixed_batch<false, false>(
            sel, sel_cnt, unmatched_rows, build_col_idx, offset, build_row_meta, r_vec, r_len, cmp_ret);
        if (OB_UNLIKELY(need_fallback)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < sel_cnt; ++i) {
            const int64_t curr_idx = sel[i];
            ObCompactRow *it = unmatched_rows[i];
            int tmp_cmp_ret = 0;
            if (OB_FAIL(cmp_func(build_obj_meta, probe_obj_meta,
                                     it->get_cell_payload(build_row_meta, build_col_idx), r_len, false,
                                     r_vec->get_data() + r_len * curr_idx, r_len, false,
                                     tmp_cmp_ret))) {
              LOG_WARN("failed to compare with cmp func", K(ret));
            } else {
              cmp_ret[i] |= (tmp_cmp_ret != 0);
            }
          }
        }
        break;
      }
      case VEC_DISCRETE : {
        ObDiscreteBase *r_vec = static_cast<ObDiscreteBase *> (probe_key->get_vector(eval_ctx));
        const int64_t real_col_idx = build_row_meta.fixed_expr_reordered()
                                    ? (build_row_meta.project_idx(build_col_idx) - build_row_meta.fixed_cnt_)
                                      : build_col_idx;
        const int64_t len_off = build_row_meta.var_offsets_off_ + sizeof(int32_t) * real_col_idx;
        const int64_t var_off = build_row_meta.var_data_off_;
        for (int64_t i = 0; OB_SUCC(ret) && i < sel_cnt; ++i) {
          const int64_t curr_idx = sel[i];
          ObCompactRow *it = unmatched_rows[i];
          const int64_t r_len = r_vec->get_lens()[curr_idx];
          const int32_t off1 = *reinterpret_cast<int32_t *>(it->payload() + len_off + sizeof(int32_t));
          const int32_t off2 = *reinterpret_cast<int32_t *>(it->payload() + len_off);
          int32_t l_len = off1 - off2;
          const char *var_data = it->payload() + var_off;
          int tmp_cmp_ret = 0;
          if (l_len == r_len && 0 == memcmp(var_data + off2, r_vec->get_ptrs()[curr_idx], r_len)) {
          } else if (OB_FAIL(cmp_func(build_obj_meta, probe_obj_meta,
                                      var_data + off2, l_len, false,
                                      r_vec->get_ptrs()[curr_idx], r_len, false,
                                      tmp_cmp_ret))) {
            LOG_WARN("failed to compare with cmp func", K(ret));
          } else {
            cmp_ret[i] |= (tmp_cmp_ret != 0);
          }
        }
        break;
      }
      case VEC_CONTINUOUS :
      case VEC_UNIFORM :
      case VEC_UNIFORM_CONST : {
        ObIVector *r_vec = probe_key->get_vector(eval_ctx);
        for (int64_t i = 0; OB_SUCC(ret) && i < sel_cnt; ++i) {
          const int64_t curr_idx = sel[i];
          ObCompactRow *it = unmatched_rows[i];
          const int64_t l_len = it->get_length(build_row_meta, build_col_idx);
          const int64_t r_len = r_vec->get_length(curr_idx);
          int tmp_cmp_ret = 0;
          if (l_len == r_len && 0 == memcmp(it->get_cell_payload(build_row_meta, build_col_idx),
                                            r_vec->get_payload(curr_idx),
                                            r_len)) {
          } else if (OB_FAIL(cmp_func(build_obj_meta, probe_obj_meta,
                                it->get_cell_payload(build_row_meta, build_col_idx), l_len, false,
                                r_vec->get_payload(curr_idx), r_len, false,
                                tmp_cmp_ret))) {
            LOG_WARN("failed to compare with cmp func", K(ret));
          } else {
            cmp_ret[i] |= (tmp_cmp_ret != 0);
          }
        }
        break;
      }
      default :
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid data format", K(ret), K(probe_key->get_format(eval_ctx)));
    }
    return ret;
  }

  template <bool NULL_SAFE>
  int inner_process_column(const ObExpr *probe_key,
                           ObEvalCtx &eval_ctx,
                           const uint16_t *sel,
                           const uint16_t sel_cnt,
                           ObCompactRow **unmatched_rows,
                           const int64_t build_col_idx,
                           const RowMeta &build_row_meta,
                           NullSafeRowCmpFunc cmp_func,
                           const ObObjMeta &build_obj_meta,
                           const ObObjMeta &probe_obj_meta,
                           int *cmp_ret) {
    int ret = OB_SUCCESS;
    switch (probe_key->get_format(eval_ctx)) {
      case VEC_FIXED : {
        ObFixedLengthBase *r_vec = static_cast<ObFixedLengthBase *> (probe_key->get_vector(eval_ctx));
        int64_t r_len = r_vec->get_length();
        const int64_t offset = build_row_meta.fixed_expr_reordered()
                                ? build_row_meta.get_fixed_cell_offset(build_col_idx) : -1;
        bool need_fallback = compare_fixed_batch<true, NULL_SAFE>(
            sel, sel_cnt, unmatched_rows, build_col_idx, offset, build_row_meta, r_vec, r_len, cmp_ret);
        if (OB_UNLIKELY(need_fallback)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < sel_cnt; ++i) {
            const int64_t curr_idx = sel[i];
            ObCompactRow *it = unmatched_rows[i];
            int tmp_cmp_ret = 0;
            const bool l_isnull = it->is_null(build_col_idx);
            const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
            if (l_isnull != r_isnull) {
              cmp_ret[i] |= 1;
            } else if (l_isnull && r_isnull) {
              cmp_ret[i] |= (NULL_SAFE ? 0 : 1);
            } else if (OB_FAIL(cmp_func(build_obj_meta, probe_obj_meta,
                                     it->get_cell_payload(build_row_meta, build_col_idx), r_len, false,
                                     r_vec->get_data() + r_len * curr_idx, r_len, false,
                                     tmp_cmp_ret))) {
              LOG_WARN("failed to compare with cmp func", K(ret));
            } else {
              cmp_ret[i] |= (tmp_cmp_ret != 0);
            }
          }
        }
        break;
      }
      case VEC_DISCRETE : {
        ObDiscreteBase *r_vec = static_cast<ObDiscreteBase *> (probe_key->get_vector(eval_ctx));
        for (int64_t i = 0; OB_SUCC(ret) && i < sel_cnt; ++i) {
          const int64_t curr_idx = sel[i];
          ObCompactRow *it = unmatched_rows[i];
          const bool l_isnull = it->is_null(build_col_idx);
          const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
          if (l_isnull != r_isnull) {
            cmp_ret[i] |= 1;
          } else if (l_isnull && r_isnull) {
            cmp_ret[i] |= (NULL_SAFE ? 0 : 1);
          } else {
            const int64_t r_len = r_vec->get_lens()[curr_idx];
            const int64_t l_len = it->get_length(build_row_meta, build_col_idx);
            if (r_len == l_len
                && 0 == memcmp(it->get_cell_payload(build_row_meta, build_col_idx),
                               r_vec->get_ptrs()[curr_idx],
                               r_len)) {
            } else {
              int tmp_cmp_ret = 0;
              if (OB_FAIL(cmp_func(build_obj_meta, probe_obj_meta,
                                   it->get_cell_payload(build_row_meta, build_col_idx), l_len, false,
                                   r_vec->get_ptrs()[curr_idx], r_len, false,
                                   tmp_cmp_ret))) {
                LOG_WARN("failed to compare with cmp func", K(ret));
              } else {
                cmp_ret[i] |= (tmp_cmp_ret != 0);
              }
            }
          }
        }
        break;
      }
      case VEC_CONTINUOUS :
      case VEC_UNIFORM :
      case VEC_UNIFORM_CONST : {
        ObIVector *r_vec = probe_key->get_vector(eval_ctx);
        for (int64_t i = 0; OB_SUCC(ret) && i < sel_cnt; ++i) {
          const int64_t curr_idx = sel[i];
          ObCompactRow *it = unmatched_rows[i];
          const bool l_isnull = it->is_null(build_col_idx);
          const bool r_isnull = r_vec->is_null(curr_idx);
          if (l_isnull != r_isnull) {
            cmp_ret[i] |= 1;
          } else if (l_isnull && r_isnull) {
            cmp_ret[i] |= (NULL_SAFE ? 0 : 1);
          } else {
            const int64_t l_len = it->get_length(build_row_meta, build_col_idx);
            const int64_t r_len = r_vec->get_length(curr_idx);
            if (l_len == r_len
                && 0 == memcmp(it->get_cell_payload(build_row_meta, build_col_idx),
                               r_vec->get_payload(curr_idx),
                               r_len)) {
            } else {
              int tmp_cmp_ret = 0;
              if (OB_FAIL(cmp_func(build_obj_meta, probe_obj_meta,
                                   it->get_cell_payload(build_row_meta, build_col_idx), l_len, false,
                                   r_vec->get_payload(curr_idx), r_len, false,
                                   tmp_cmp_ret))) {
                LOG_WARN("failed to compare with cmp func", K(ret));
              } else {
                cmp_ret[i] |= (tmp_cmp_ret != 0);
              }
            }
          }
        }
        break;
      }
      default :
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid data format", K(ret), K(probe_key->get_format(eval_ctx)));
    }
    return ret;
  }
};

struct IHashTable {

  virtual int init(ObIAllocator &alloc, const int64_t max_batch_size) = 0;
  virtual int build_prepare(int64_t row_count, int64_t bucket_count) = 0;
  virtual int insert_batch(JoinTableCtx &ctx,
                           ObHJStoredRow **stored_rows,
                           const int64_t size,
                           int64_t &used_buckets,
                           int64_t &collisions) = 0;
  virtual int probe_prepare(JoinTableCtx &ctx, OutputInfo &output_info) = 0;
  virtual int probe_batch(JoinTableCtx &ctx, OutputInfo &output_info) = 0;
  virtual int project_matched_rows(JoinTableCtx &ctx, OutputInfo &output_info) = 0;
  virtual void reset() = 0;
  virtual void free(ObIAllocator *alloc) = 0;

  virtual int64_t get_row_count() const = 0;
  virtual int64_t get_used_buckets() const = 0;
  virtual int64_t get_nbuckets() const = 0;
  virtual int64_t get_collisions() const = 0;
  virtual int64_t get_mem_used() const = 0;
  virtual int64_t get_one_bucket_size() const = 0;
  virtual int64_t get_normalized_key_size() const = 0;
  virtual void set_diag_info(int64_t used_buckets, int64_t collisions) = 0;
};

// Open addressing hash table implement:
//
//   buckets:
//   .......
//   +----------------------+
//   | 0          | NUL     |
//   +----------------------+    +---------------+       +--------------+
//   | salt       | row_ptr |--->| ObHJStoredRow |------>| END_ROW_PTR  |
//   +----------------------+    +---------------+       +--------------+
//   | 0          | NULL    |
//   +----------------------+
//   ......
//
// Buckets is array of <salt, store_row_ptr> pair, store rows linked in one bucket are
// the same key value.
template <typename Bucket, typename Prober>
struct HashTable : public IHashTable
{
  HashTable()
      : buckets_(nullptr),
        nbuckets_(0),
        bucket_mask_(0),
        bit_cnt_(0),
        row_count_(0),
        collisions_(0),
        used_buckets_(0),
        inited_(false),
        ht_alloc_(nullptr),
        magic_(0),
        is_shared_(false)
  {
  }
  int init(ObIAllocator &alloc, const int64_t max_batch_size) override;
  int build_prepare(int64_t row_count, int64_t bucket_count) override;
  virtual int insert_batch(JoinTableCtx &ctx,
                           ObHJStoredRow **stored_rows,
                           const int64_t size,
                           int64_t &used_buckets,
                           int64_t &collisions) override;
  int probe_prepare(JoinTableCtx &ctx, OutputInfo &output_info) override;
  int probe_batch(JoinTableCtx &ctx, OutputInfo &output_info) override {
    return ctx.probe_opt_  ? probe_batch_opt(ctx, output_info)
           : !ctx.need_probe_del_match() ? probe_batch_normal(ctx, output_info, (0 != ctx.other_conds_->count()))
                                         : probe_batch_del_match(ctx, output_info, (0 != ctx.other_conds_->count()));
  }
  int project_matched_rows(JoinTableCtx &ctx, OutputInfo &output_info) override;
  void reset() override;
  void free(ObIAllocator *alloc);

  int64_t get_row_count() const override { return row_count_; };
  int64_t get_used_buckets() const override { return used_buckets_; }
  int64_t get_nbuckets() const override { return nbuckets_; }
  int64_t get_collisions() const override { return collisions_; }
  int64_t get_mem_used() const override {
    int64_t size = 0;
    size = sizeof(*this);
    if (NULL != buckets_) {
      size += buckets_->mem_used();
    }
    return size;
  }
  int64_t get_one_bucket_size() const { return sizeof(Bucket); };
  int64_t get_normalized_key_size() const {
    return Prober::get_normalized_key_size();
  }
  virtual void set_diag_info(int64_t used_buckets, int64_t collisions) override {
    used_buckets_ += used_buckets;
    collisions_ += collisions;
  }
  using BucketArray =
    common::ObSegmentArray<Bucket, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>;
protected:
  // need to compare bucket key on insert
  static inline int key_equal(
      JoinTableCtx &ctx, ObHJStoredRow *left_row, ObHJStoredRow *right_row, bool &equal);

private:
  int init_probe_key_data(JoinTableCtx &ctx, OutputInfo &output_info);
  int probe_batch_opt(JoinTableCtx &ctx, OutputInfo &output_info);
  int probe_batch_normal(JoinTableCtx &ctx, OutputInfo &output_info, bool has_other_conds);
  int probe_batch_del_match(JoinTableCtx &ctx, OutputInfo &output_info, bool has_other_conds);
  OB_INLINE int set(JoinTableCtx &ctx, ObHJStoredRow *row_ptr, int64_t &used_buckets, int64_t &collisions);
  OB_INLINE void find(JoinTableCtx &ctx, const uint64_t &batch_idx, const uint64_t &hash_val,
      Bucket *&bucket, bool is_del_match, bool &is_find);
  OB_INLINE void find_batch(JoinTableCtx &ctx, uint16_t &unmatched_cnt, bool is_del_matched);

protected:
  static const int64_t MAGIC_CODE = 0x123654abcd134;
  BucketArray *buckets_;
  int64_t nbuckets_;
  int64_t bucket_mask_;
  int64_t bit_cnt_;
  int64_t row_count_;
  int64_t collisions_;
  int64_t used_buckets_;
  bool inited_;
  ModulePageAllocator *ht_alloc_;
  int64_t magic_;
  bool is_shared_;
  // TODO shengle support add null key rows for normalized mode
  // NullArray *null_key_rows_;
  Prober prober_;
};

template <typename Bucket, typename Prober>
struct RobinHashTable final : public HashTable<Bucket, Prober> {
public:
  int insert_batch(JoinTableCtx &ctx, ObHJStoredRow **stored_rows, const int64_t size,
      int64_t &used_buckets, int64_t &collisions) override;
  inline virtual void set_diag_info(int64_t used_buckets, int64_t collisions) override
  {
    ATOMIC_AAF(&this->used_buckets_, used_buckets);
    ATOMIC_AAF(&this->collisions_, collisions);
  }

private:
  // Put bucket in buckets_[pos], and move the subsequent buckets back.
  OB_INLINE void set_and_shift_up(JoinTableCtx &ctx, ObHJStoredRow *row_ptr, const RowMeta &row_meta, uint64_t salt, uint64_t dist, uint64_t pos);
};

struct GenericSharedHashTable final : public HashTable<GenericBucket, GenericProber<GenericBucket>> {
  using Bucket = GenericBucket;

public:
  inline int insert_batch(JoinTableCtx &ctx, ObHJStoredRow **stored_rows, const int64_t size,
      int64_t &used_buckets, int64_t &collisions) override;
  inline virtual void set_diag_info(int64_t used_buckets, int64_t collisions) override
  {
    ATOMIC_AAF(&this->used_buckets_, used_buckets);
    ATOMIC_AAF(&this->collisions_, collisions);
  }

private:
  inline int atomic_set(JoinTableCtx &ctx, const uint64_t hash_val, const RowMeta &row_meta,
      ObHJStoredRow *row_ptr, int64_t &used_buckets, int64_t &collisions);
};

template <typename T>
struct NormalizedSharedHashTable final
    : public HashTable<NormalizedBucket<T>, NormalizedProber<T>> {
  using Bucket = NormalizedBucket<T>;

public:
  int insert_batch(JoinTableCtx &ctx, ObHJStoredRow **stored_rows, const int64_t size,
      int64_t &used_buckets, int64_t &collisions) override;
  virtual void set_diag_info(int64_t used_buckets, int64_t collisions) override
  {
    ATOMIC_AAF(&this->used_buckets_, used_buckets);
    ATOMIC_AAF(&this->collisions_, collisions);
  }

private:
  inline int atomic_set(JoinTableCtx &ctx, const uint64_t hash_val, ObHJStoredRow *row_ptr,
      int64_t &used_buckets, int64_t &collisions);
};

//using DirectInt8Table = HashTable<DirectBucket<int8_t>, NormalizedProber<int8_t>>;
//using DirectInt16Table = HashTable<DirectBucket<int16_t>, NormalizedProber<int16_t>>;
//using NormalizedInt32Table = HashTable<NormalizedBucket<int32_t>, NormalizedProber<int32_t>>;
using NormalizedInt64Table = HashTable<NormalizedBucket<Int64Key>, NormalizedProber<Int64Key>>;
using NormalizedInt128Table = HashTable<NormalizedBucket<Int128Key>, NormalizedProber<Int128Key>>;
//using NormalizedFloatTable = HashTable<NormalizedBucket<float>, NormalizedProber<float>>;
//using NormalizedDoubleTable =  HashTable<NormalizedBucket<double>, NormalizedProber<double>>;
using GenericTable = HashTable<GenericBucket, GenericProber<GenericBucket>>;
using NormalizedSharedInt64Table = NormalizedSharedHashTable<Int64Key>;
using NormalizedSharedInt128Table = NormalizedSharedHashTable<Int128Key>;

// robin hashtable
using GenericRobinTable = RobinHashTable<RobinBucket, GenericProber<RobinBucket>>;
using NormalizedInt64RobinTable = RobinHashTable<NormalizedRobinBucket<Int64Key>, NormalizedRobinProber<Int64Key>>;
using NormalizedInt128RobinTable = RobinHashTable<NormalizedRobinBucket<Int128Key>, NormalizedRobinProber<Int128Key>>;

} // end namespace sql
} // end namespace oceanbase

#include "sql/engine/join/hash_join/hash_table.ipp"

#endif /* SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_*/
