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

#ifndef SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_
#define SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_

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
      ObIVector *vec = probe_key->get_vector(*ctx.eval_ctx_);
      ObExpr *build_key = ctx.build_keys_->at(i);
      // join key type on build and probe may be different
      if (ctx.probe_cmp_funcs_.at(i) != nullptr) {
        NullSafeRowCmpFunc cmp_func = ctx.probe_cmp_funcs_.at(i);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < sel_cnt; row_idx++) {
          int64_t batch_idx = sel[row_idx];
          const char *l_v = NULL;
          const char *r_v = NULL;
          ObLength l_len = 0;
          ObLength r_len = 0;
          bool l_is_null = ctx.unmatched_rows_[row_idx]->is_null(build_col_idx);
          bool r_is_null = false;
          ctx.unmatched_rows_[row_idx]->get_cell_payload(
              ctx.build_row_meta_, build_col_idx, l_v, l_len);
          vec->get_payload(batch_idx, r_is_null, r_v, r_len);
          int cmp_ret = 0;
          if (!ctx.is_ns_equal_cond_->at(i) && (l_is_null || r_is_null)) {
            cmp_ret = 1;
          } else if (OB_FAIL(cmp_func(build_key->obj_meta_,
                  probe_key->obj_meta_,
                  l_v,
                  l_len,
                  l_is_null,
                  r_v,
                  r_len,
                  r_is_null,
                  cmp_ret))) {
            LOG_WARN("failed to compare with cmp func", K(ret));
          }
          ctx.cmp_ret_map_[row_idx] |= (cmp_ret != 0);
        }
      } else {
        if (ctx.is_ns_equal_cond_->at(i)) {
          if (OB_FAIL(vec->null_first_cmp_batch_rows(*probe_key,
                  sel,
                  sel_cnt,
                  reinterpret_cast<ObCompactRow **>(ctx.unmatched_rows_),
                  build_col_idx,
                  ctx.build_row_meta_,
                  ctx.cmp_ret_for_one_col_))) {
            LOG_WARN("failed to compare with ns equal cond", K(ret));
          }
        } else if (is_opt &&
                  (!ctx.build_cols_have_null_->at(i) && !ctx.probe_cols_have_null_->at(i))) {
          if (OB_FAIL(vec->no_null_cmp_batch_rows(*probe_key,
                  sel,
                  sel_cnt,
                  reinterpret_cast<ObCompactRow **>(ctx.unmatched_rows_),
                  build_col_idx,
                  ctx.build_row_meta_,
                  ctx.cmp_ret_for_one_col_))) {
            LOG_WARN("failed to compare with no null", K(ret));
          }
        } else {
          if (OB_FAIL(vec->null_first_cmp_batch_rows(*probe_key,
                  sel,
                  sel_cnt,
                  reinterpret_cast<ObCompactRow **>(ctx.unmatched_rows_),
                  build_col_idx,
                  ctx.build_row_meta_,
                  ctx.cmp_ret_for_one_col_))) {
            LOG_WARN("failed to compare with have null", K(ret));
          }
          // have null and not ns equal, need to check null is not match null
          if (ctx.build_cols_have_null_->at(i)) {
            for (int64_t row_idx = 0; row_idx < sel_cnt; row_idx++) {
              ObHJStoredRow *row_ptr = ctx.unmatched_rows_[row_idx];
              if (row_ptr->is_null(build_col_idx)) {
                ctx.cmp_ret_for_one_col_[row_idx] = 1;
              }
            }
          }
          if (ctx.probe_cols_have_null_->at(i)) {
            VectorFormat format = vec->get_format();
            if (is_uniform_format(format)) {
              const uint64_t idx_mask = VEC_UNIFORM_CONST == format ? 0 : UINT64_MAX;
              const ObDatum *datums = static_cast<ObUniformBase *>(vec)->get_datums();
              for (int64_t row_idx = 0; row_idx < sel_cnt; row_idx++) {
                int64_t batch_idx = sel[row_idx];
                if (datums[batch_idx & idx_mask].is_null()) {
                  ctx.cmp_ret_for_one_col_[row_idx] = 1;
                }
              }
            } else {
              ObBitVector *nulls = static_cast<ObBitmapNullVectorBase *>(vec)->get_nulls();
              for (int64_t row_idx = 0; row_idx < sel_cnt; row_idx++) {
                int64_t batch_idx = sel[row_idx];
                if (nulls->at(batch_idx)) {
                  ctx.cmp_ret_for_one_col_[row_idx] = 1;
                }
              }
            }
          }
        }
        for (int64_t row_idx = 0; row_idx < sel_cnt; row_idx++) {
          ctx.cmp_ret_map_[row_idx] |= ctx.cmp_ret_for_one_col_[row_idx];
        }
      }
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
