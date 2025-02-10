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

#ifndef _OB_EXPR_IN_H_
#define _OB_EXPR_IN_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace sql
{
struct StrKey
{
  StrKey() : hash_(0), len_(0), ptr_(nullptr) {}
  StrKey(int64_t len, const char *ptr) : len_(len), ptr_(ptr) {}
  StrKey(const ObString &str) : len_(str.length()), ptr_(str.ptr()) {}
  StrKey(uint64_t hash, const ObString &str) : hash_(hash), len_(str.length()), ptr_(str.ptr()) {}
  OB_INLINE void make_key(uint64_t hash, int64_t len, const char *ptr)
  {
    hash_ = hash;
    len_ = len;
    ptr_ = ptr;
  }
  OB_INLINE StrKey &operator=(const StrKey &other)
  {
    make_key(other.hash_, other.len_, other.ptr_);
    return *this;
  }
  uint64_t hash_;
  int64_t len_;
  const char *ptr_;
} __attribute__((packed));

struct StrBkt
{
  StrBkt() : is_used_(false), key_() {}
  StrBkt(bool is_used, const StrKey &key) : is_used_(is_used), key_(key) {}
  OB_INLINE StrBkt &operator=(const StrKey &key)
  {
    key_ = key;
    is_used_ = true;
    return *this;
  }
  bool is_used_;
  StrKey key_;
} __attribute__((packed));

struct IntBkt
{
  IntBkt() : is_used_(false), key_(0) {}
  IntBkt(bool is_used, int64_t key) : is_used_(is_used), key_(key) {}
  OB_INLINE IntBkt &operator=(const uint64_t &key)
  {
    key_ = key;
    is_used_ = true;
    return *this;
  }
  bool is_used_;
  uint64_t key_;
} __attribute__((packed));

template <typename Key> struct KeyComparator;
template <> struct KeyComparator<StrBkt>
{
  OB_INLINE static bool cmp(const StrBkt &lhs,
                            const StrKey &rhs,
                            ObCollationType cs_type,
                            bool cmp_end_space = false)
  {
    bool is_equal = false;
    if (lhs.key_.hash_ != rhs.hash_) {
      is_equal = false;
    } else {
      if (lhs.key_.len_ == rhs.len_) {
        is_equal = !MEMCMP(lhs.key_.ptr_, rhs.ptr_, rhs.len_);
      }
      if (!is_equal) {
        is_equal = !ObCharset::strcmpsp(cs_type,
                                    lhs.key_.ptr_, lhs.key_.len_,
                                    rhs.ptr_, rhs.len_, cmp_end_space);
      }
    }
    return is_equal;
  }
  OB_INLINE static bool is_empty(const StrBkt &bkt) { return !bkt.is_used_; }
};
template <> struct KeyComparator<IntBkt>
{
  OB_INLINE static bool cmp(const IntBkt &lhs,
                            uint64_t rhs,
                            ObCollationType cs_type,
                            bool cmp_end_space = false)
  {
    return lhs.key_ == rhs;
  }
  OB_INLINE static bool is_empty(const IntBkt &bkt) { return !bkt.is_used_; }
};
// infer bucket type for primitive types
template <typename T> struct BktType;
template <> struct BktType<uint64_t>
{
  using type = IntBkt;
};
template <> struct BktType<StrKey>
{
  using type = StrBkt;
};
template <typename T> struct BktType
{
  using type = uint64_t;
};

template <typename KeyType>
class ObColumnHashSet
{
public:
  using bucket_t = typename BktType<KeyType>::type;
  using key_cmparator = KeyComparator<bucket_t>;
  ObColumnHashSet()
      : inited_(false), buckets_(nullptr), bucket_mask_(0), capacity_(0), size_(0),
        cs_type_(CS_TYPE_INVALID), cmp_end_space_(false), alloc_()
  {
  }
  ~ObColumnHashSet() {}

  int init(uint64_t capacity,
           int64_t tenant_id,
           common::ObCollationType cs_type = CS_TYPE_INVALID,
           bool cmp_end_space = false)
  {
    int ret = OB_SUCCESS;
    cs_type_ = cs_type;
    cmp_end_space_ = cmp_end_space;
    if (inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "init twice");
    } else {
      alloc_.set_tenant_id(tenant_id);
      alloc_.set_label("ObColumnHashSet");
      if (OB_FAIL(alloc_mem(capacity))) {
        COMMON_LOG(WARN, "failed to alloc when init");
      } else {
        inited_ = true;
      }
    }
    return ret;
  }
  void clear()
  {
    size_ = 0;
    for (uint64_t i = 0; i < capacity_; ++i) {
      new (&buckets_[i]) bucket_t();
    }
  }
  OB_INLINE void destroy()
  {
    inited_ = false;
    bucket_mask_ = 0;
    capacity_ = 0;
    size_ = 0;
    alloc_.free(buckets_); // unused for ObArenaAllocator
    buckets_ = nullptr;
    alloc_.reset();
  }
  OB_INLINE bool inited() const { return inited_; }
  OB_INLINE uint64_t size() const { return size_; }
  OB_INLINE int insert(uint64_t hash, const KeyType key)
  {
    int ret = OB_SUCCESS;
    uint64_t offset = hash & bucket_mask_;
    while ((!key_cmparator::is_empty(buckets_[offset]))
           && (!key_cmparator::cmp(buckets_[offset], key, cs_type_, cmp_end_space_))) {
      offset = (offset + 1) & bucket_mask_;
    }
    if (key_cmparator::is_empty(buckets_[offset])) {
      buckets_[offset] = key;
      size_++;
    }
    return ret;
  }

  OB_INLINE bool exists(uint64_t hash, const KeyType key) const
  {
    bool find = false;
    uint64_t offset = hash & bucket_mask_;
    while (!key_cmparator::is_empty(buckets_[offset])) {
      if (key_cmparator::cmp(buckets_[offset], key, cs_type_, cmp_end_space_)) {
        find = true;
        break;
      }
      offset = (offset + 1) & bucket_mask_;
    }
    return find;
  }

private:
  uint64_t normalize_capacity(uint64_t n) { return max(MIN_BUCKET_SIZE, next_pow2(2 * n)); }

  int alloc_mem(uint64_t capacity)
  {
    int ret = OB_SUCCESS;
    uint64_t new_capacity = normalize_capacity(capacity);
    void *buf = nullptr;
    if (OB_ISNULL(buf = alloc_.alloc_aligned(sizeof(bucket_t) * new_capacity, CACHE_LINE_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to allocate bucket memory", K(new_capacity));
    } else {
      buckets_ = static_cast<bucket_t *>(buf);
      capacity_ = new_capacity;
      bucket_mask_ = capacity_ - 1;
      for (size_t i = 0; i < new_capacity; ++i) {
        new (&buckets_[i]) bucket_t();
      }
      COMMON_LOG(DEBUG, "alloc capacity ", K(capacity_));
    }
    return ret;
  }

private:
  static constexpr uint64_t KEY_MASK = 1UL << 63;
  static constexpr int64_t MIN_BUCKET_SIZE = 128;
  static constexpr int64_t CACHE_LINE_SIZE = 64;
  static constexpr int64_t MAX_SEEK_TIMES = 8;

private:
  bool inited_{false};
  bucket_t *buckets_{nullptr};
  uint64_t bucket_mask_{0};
  uint64_t capacity_{0};
  uint64_t size_{0};
  common::ObCollationType cs_type_{CS_TYPE_INVALID};
  bool cmp_end_space_{false};
  common::ObArenaAllocator alloc_;
};

//成员解释：
//idx_ 的二进制数代表hash表选中Row的那些列作为key， 例如向量(1,2,3)存储在idx_为3的hash表中，binary(3)=011，即低2位被选中,key=(1,2)
//row_dimension_ 存储Row的维度
//hash_funcs_ 长度为row_dimension_， datum的hash函数，obj不会设置和使用
//cmp_funcs_ 长度为row_dimension_， datum的compare函数，obj不会设置和使用
struct HashMapMeta {
  HashMapMeta() : idx_(-1), row_dimension_(-1), hash_funcs_(NULL), cmp_funcs_(NULL) {}
  ~HashMapMeta() {}
  int idx_;
  int64_t row_dimension_;
  void **hash_funcs_;
  void **cmp_funcs_;
};

/*
*通用的行结构体，封装datum或者obj，datum的比较方法需要依靠外界传入
*参数idx代表被选取的列，出现在hashtable的比较方法和hash方法中
*/
template <class T>
class Row
{
public:
  Row() : elems_(NULL) {}
  ~Row() {}
  bool equal_key(const Row &other, void **cmp_funcs, const int idx) const;
  //hash 函数和cmp函数，为datum和obj特化
  int hash_key(void **hash_funcs, const int idx, uint64_t seed, uint64_t &hash_val) const;
  int compare_with_null(const Row &other, void **cmp_funcs, const int64_t row_dimension, int &exist_ret) const;
  int set_elem(T *elems);
  T &get_elem(int idx) const { return elems_[idx]; }
  T *get_elems() const { return elems_; }
  TO_STRING_KV(KP(elems_));
private:
  T *elems_;
};
/*
*将Row封装为hash表的key
*/
template <class T>
struct RowKey
{
public:
  RowKey() : row_(), hash_val_(0), meta_(NULL) {}
  ~RowKey() {}
  bool operator==(const RowKey<T> &other) const;
  int hash(uint64_t &hash_val, uint64_t seed = 0) const;
  Row<T> row_;//指向in_ctx中存储的数据
  uint64_t hash_val_;
  HashMapMeta *meta_;//进入hash表达时候被设置为此hash表的meta
};

template <class T>
class ObExprInHashMap
{
public:
const static int HASH_CMP_TRUE = 0;
const static int HASH_CMP_FALSE = -1;
const static int HASH_CMP_UNKNOWN = 1;
  ObExprInHashMap() : map_(), meta_() {}
  ~ObExprInHashMap() {}
  void destroy()
  {
    if (map_.created()) {
      //将map_管理的ObArray 释放
      for (auto it = map_.begin(); it != map_.end(); ++it) {
        it->second.destroy();
      }
      (void)map_.destroy();
    }
  }
  int create(int param_num)
  {
    ObMemAttr attr(MTL_ID(), common::ObModIds::OB_HASH_BUCKET);
    return map_.create(param_num * 2, attr);
  }
  int set_refactored(const Row<T> &row);
  int exist_refactored(const Row<T> &row, int &exist_ret);
  void set_meta_idx(int idx) { meta_.idx_ = idx; }
  void set_meta_dimension(int64_t row_dimension) { meta_.row_dimension_ = row_dimension; }
  void set_meta_hash(void **hash_funcs) { meta_.hash_funcs_ = hash_funcs; }
  void set_meta_cmp(void **cmp_funcs) { meta_.cmp_funcs_= cmp_funcs; }
private:
  common::hash::ObHashMap<RowKey<T>,
                         common::ObArray<Row<T>>,
                         common::hash::NoPthreadDefendMode> map_;
  HashMapMeta meta_;
};

using normal_inkey_t = uint64_t;

template <class T>
class ObExprInHashSet
{
public:
  ObExprInHashSet() : set_(), meta_() {}
  ~ObExprInHashSet() {}
  void destroy()
  {
    if (set_.created()) {
      (void)set_.destroy();
    }
  }
  bool inited(){
    return set_.created();
  }
  inline int64_t size() const { return set_.size();}
  inline bool use_open_set() { return use_open_set_; }
  int create(int param_num)
  {
    return set_.create(param_num);
  }
  int set_refactored(const Row<T> &row);
  int exist_refactored(uint64_t hash_val, const Row<T> &row, bool &is_exist);
  int exist_refactored(const Row<T> &row, bool &is_exist);
  void set_meta_idx(int idx) { meta_.idx_ = idx; }
  void set_meta_dimension(int64_t row_dimension) { meta_.row_dimension_ = row_dimension; }
  void set_meta_hash(void **hash_funcs) { meta_.hash_funcs_ = hash_funcs; }
  void set_meta_cmp(void **cmp_funcs) { meta_.cmp_funcs_= cmp_funcs; }
private:
  common::hash::ObHashSet<RowKey<T>, common::hash::NoPthreadDefendMode> set_;
  bool use_open_set_;
  HashMapMeta meta_;
};

class ObExecContext;
class ObSubQueryIterator;

class ObExprInOrNotIn : public ObVectorExprOperator
{
  class ObExprInCtx : public ObExprOperatorCtx
  {
  public:
    ObExprInCtx()
      : ObExprOperatorCtx(),
        row_dimension_(-1),
        right_has_null_(false),
        hash_func_buff_(NULL),
        cmp_functions_(NULL),
        funcs_ptr_set_(false),
        ctx_hash_null_(false),
        hash_vals_inited_(false),
        hash_vals(NULL),
        int_ht_(),
        str_ht_(),
        use_colht_(false),
        right_datums_(NULL),
        static_engine_hashset_(),
        static_engine_hashset_vecs_(NULL),
        cmp_type_(),
        param_exist_null_(false),
        hash_calc_disabled_(false),
        cmp_types_()
    {
    }
    virtual ~ObExprInCtx()
    {
      static_engine_hashset_.destroy();
      if (int_ht_.inited()) {
        (void)int_ht_.destroy();
      }
      if (str_ht_.inited()) {
        (void)str_ht_.destroy();
      }
      alloc_.reset();
      if (OB_NOT_NULL(static_engine_hashset_vecs_)) {
        for (int i = 0; i < (1 << row_dimension_); ++i) {
          static_engine_hashset_vecs_[i].destroy();
        }
        static_engine_hashset_vecs_ = NULL;
      }
    }
  public:
    int init_hash_vals(int64_t size);
    int init_hashset(VecValueTypeClass vec_tc,int64_t param_num, bool use_colhashset,
                     common::ObCollationType cs_type, bool cmp_end_space);
    bool need_rebuild_hashset();
    int init_static_engine_hashset(int64_t param_num);
    int init_static_engine_hashset_vecs(int64_t param_num,
                                        int64_t row_dimension,
                                        ObExecContext *exec_ctx);

    template<typename KeyType>
    int colht_prefetch(uint64_t *&hash_val, int begin, int end);
    int add_to_static_engine_hashset(const Row<common::ObDatum> &row);
    int add_to_static_engine_hashset_vecs(const Row<common::ObDatum> &row,
                                          const int idx);
    template<typename ResVec>
    int colht_probe_batch(int32_t begin, int32_t end, uint64_t *&hash_val,
                                      const normal_inkey_t *&key, bool is_not_expr_in,
                                      ResVec *&res_vec);

    int exist_in_static_engine_hashset(uint64_t hash_val, const Row<common::ObDatum> &row,
                                          bool &is_exist);
    int exist_in_static_engine_hashset(const Row<common::ObDatum> &row,
                                       bool &is_exist);
    int exist_in_static_engine_hashset_vecs(const Row<common::ObDatum> &row,
                                             const int idx,
                                             int &exist_ret);
    int init_hashset_vecs_all_null(const int64_t row_dimension,
                                   ObExecContext *exec_ctx);
    inline int set_hashset_vecs_all_null_true(const int64_t idx);
    int get_hashset_vecs_all_null(const int64_t idx,  bool &is_all_null) const;
    inline void set_param_exist_null(bool exist_null);
    inline bool is_param_exist_null() const;
    inline int set_cmp_types(const ObExprCalcType &cmp_type,
                             const int64_t row_dimension);
    int64_t get_static_engine_hashset_size() const { return static_engine_hashset_.size(); }
    inline int64_t get_colht_size() const { return int_ht_.size() + str_ht_.size(); }
    inline const ObExprCalcType &get_cmp_types(const int64_t idx) const;

    bool is_hash_calc_disabled() const { return hash_calc_disabled_; }
    void disable_hash_calc(void) { hash_calc_disabled_ = true; }
    int init_right_datums(int64_t param_num,
                          int64_t row_dimension,
                          ObExecContext *exec_ctx);
    int init_cmp_funcs(int64_t func_cnt,
                       ObExecContext *exec_ctx);
    int set_right_datum(int64_t row_num,
                        int64_t col_num,
                        const int right_param_num,
                        const common::ObDatum &datum);
    inline common::ObDatum *get_datum_row(int64_t row_num) { return right_datums_[row_num]; }
    void set_hash_funcs_ptr(int64_t idx, void **hash_funcs_ptr)
    {
      static_engine_hashset_vecs_[idx].set_meta_hash(hash_funcs_ptr);
    }
    void set_cmp_funcs_ptr(int64_t idx, void **cmp_funcs_ptr) {
      static_engine_hashset_vecs_[idx].set_meta_cmp(cmp_funcs_ptr);
    }
    void set_hash_funcs_ptr_for_set(void **hash_funcs_ptr) {
      static_engine_hashset_.set_meta_hash(hash_funcs_ptr);
    }
    void set_cmp_funcs_ptr_for_set(void **cmp_funcs_ptr) {
      static_engine_hashset_.set_meta_cmp(cmp_funcs_ptr);
    }
    int row_dimension_;
    bool right_has_null_;
    void **hash_func_buff_;
    void **cmp_functions_;
    bool funcs_ptr_set_;
    bool ctx_hash_null_;
    bool hash_vals_inited_;
    uint64_t *hash_vals; // probe item hash values for batch hashing
    ObColumnHashSet<uint64_t> int_ht_;
    ObColumnHashSet<StrKey> str_ht_;
    bool use_colht_;
  private:
    common::ObDatum **right_datums_;//IN 右边的常量储存于此
    ObExprInHashSet<common::ObDatum> static_engine_hashset_;
    ObExprInHashMap<common::ObDatum> *static_engine_hashset_vecs_;
    common::ObFixedArray<bool, common::ObIAllocator> hashset_vecs_all_null_; //下标代表的列全为null
    ObExprCalcType cmp_type_;
    // mysql> select 1 in (2.0, 1 / 0);
    // +-------------------+
    // | 1 in (2.0, 1 / 0) |
    // +-------------------+
    // |              NULL |
    // +-------------------+
    // but we can't detect the const expr "1 / 0" will return null in visit_in_expr(),
    // so we must put the "exist null" property in ObExprInCtx, not in ObExprIn.
    bool param_exist_null_;
    bool hash_calc_disabled_;
    common::ObFixedArray<ObExprCalcType, common::ObIAllocator> cmp_types_;
    common::ObArenaAllocator alloc_;
  };

  OB_UNIS_VERSION_V(1);
public:
  ObExprInOrNotIn(common::ObIAllocator &alloc,
                  ObExprOperatorType type,
                  const char *name);
  virtual ~ObExprInOrNotIn() {};

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  int cg_expr_without_row(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const;
  int cg_expr_with_row(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  // like "select 1 from dual where (select 1, 2) in ((1,2), (3,4))"
  int cg_expr_with_subquery(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  static void set_datum_result(const bool is_expr_in,
                               const bool is_exist,
                               const bool param_exists_null,
                               ObDatum &result);
  template<typename ResVec>
  static void set_vector_result(const bool is_expr_in,
                                const bool is_exist,
                                const bool param_exist_null,
                                ResVec *res_vec,
                                const int64_t &idx);
  virtual bool need_rt_ctx() const override { return true; }
public:
  static int eval_in_with_row(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_in_without_row(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObDatum &expr_datum);
  static int eval_in_with_row_fallback(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &expr_datum);
  static int eval_in_without_row_fallback(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         ObDatum &expr_datum);
  static int eval_batch_in_without_row_fallback(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                const ObBitVector &skip,
                                                const int64_t batch_size);
  static int eval_batch_in_without_row(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const ObBitVector &skip,
                                       const int64_t batch_size);
  template <typename LeftVec, typename ResVec>
  static int inner_eval_vector_in_without_row_fallback(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                const ObBitVector &skip,
                                                const EvalBound &bound);
  template <typename LeftVec, typename ResVec>
  static int inner_eval_vector_in_without_row(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound);
  template <typename LeftVec, typename ResVec>
  static int probe_col(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const EvalBound &bound,
                            LeftVec *&input_left_vec,
                            ObExprInCtx *&in_ctx,
                            ResVec *&res_vec);
  template <typename ResVec, typename KeyType>
  static int probe_item(bool is_op_in,
                    ObExprInCtx *in_ctx,
                    ObColumnHashSet<KeyType> &colht,
                    int idx,
                    const KeyType &key,
                    ResVec *&res_vec,
                    ObBitVector& eval_flags);
  template <typename LeftVec, typename ResVec, typename RawKeyType>
  static int probe_fixed_col(const ObBitVector &skip,
                      const EvalBound &bound,
                      ObBitVector &eval_flags,
                      LeftVec *&input_left_vec,
                      ResVec *&res_vec,
                      ObExprInCtx *&in_ctx,
                      bool is_op_in);
  static int eval_vector_in_without_row_fallback(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                const ObBitVector &skip,
                                                const EvalBound &bound);
  static int eval_vector_in_without_row(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound);
  // like "select 1 from dual where (select 1, 2) in ((1,2), (3,4))"
  static int eval_in_with_subquery(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_for_row_static_engine(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        ObDatum &expr_datum,
                                        ObExpr **l_row);
  static int eval_pl_udt_in(EVAL_FUNC_ARG_DECL);
  static int build_right_hash_without_row(const int64_t in_id,
                                          const int64_t right_param_num,
                                          const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObExecContext *exec_ctx,
                                          ObExprInCtx *&in_ctx,
                                          bool &cnt_null,
                                          bool use_colht = false);
  static int build_hash_set(const int64_t right_param_num,
                               const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObExecContext *exec_ctx,
                               ObExprInCtx *&in_ctx,
                               bool &cnt_null);
public:
  inline void set_param_all_const(bool all_const);
  inline void set_param_all_same_type(bool all_same_type);
  inline void set_param_all_same_cs_type(bool all_same_cs_type);
  inline void set_param_is_ext_type_oracle(bool all_is_ext);
  // for func visit_in_expr , when left param is subquery set true
  inline void set_param_is_subquery();
  // now only support ref_col IN (const, const, ...)
  inline void set_param_can_vectorized();
  static inline bool is_all_space(const char *ptr, const int64_t remain_len);
protected:
  inline bool is_param_all_const() const;
  inline bool is_param_all_same_type() const;
  inline bool is_param_all_same_cs_type() const;
  inline bool is_param_is_ext_type_oracle() const;
  inline bool is_param_is_subquery() const;
  inline bool is_param_can_vectorized() const;
  inline bool need_hash(ObExecContext *exec_ctx) const;
  //获取subquery row的type信息
  int get_param_types(const ObRawExpr &param,
                       const bool is_iter,
                       common::ObIArray<sql::ObExprResType> &types) const;
  //从subquery iter中获取row
  static int setup_row(ObExpr **expr, ObEvalCtx &ctx, const bool is_iter,
                       const int64_t cmp_func_cnt, ObSubQueryIterator *&iter, ObExpr **&row);
  static void check_right_can_cmp_mem(const ObDatum &datum, 
                                      const common::ObObjMeta &meta, 
                                      bool &can_cmp_mem, 
                                      bool &cnt_null);
  static void check_left_can_cmp_mem(const ObExpr &expr, 
                                     const ObDatum *datum, 
                                     const ObBitVector &skip, 
                                     const ObBitVector &eval_flags, 
                                     const int64_t batch_size,
                                     bool &can_cmp_mem);
  static void check_left_can_cmp_mem(const ObExpr &expr,
                                     const ObBitVector &skip,
                                     const ObBitVector &eval_flags,
                                     const EvalBound &bound,
                                     bool &can_cmp_mem);
protected:
  typedef uint32_t ObExprInParamFlag;
  static const uint32_t IN_PARAM_ALL_CONST = 1U << 0;
  static const uint32_t IN_PARAM_ALL_SAME_TYPE = 1U << 1;
  static const uint32_t IN_PARAM_ALL_SAME_CS_TYPE = 1U << 2;
  static const uint32_t IN_PARAM_IS_EXT_ORACLE = 1U << 3;
  static const uint32_t IN_PARAM_IS_SUBQUERY = 1u << 4;
  static const uint32_t IN_PARAM_CAN_VECTORIZED = 1u << 5;
  ObExprInParamFlag param_flags_;
  DISALLOW_COPY_AND_ASSIGN(ObExprInOrNotIn);
};

class ObExprIn : public ObExprInOrNotIn
{
public:
  explicit ObExprIn(common::ObIAllocator &alloc);
  virtual ~ObExprIn() {};
protected:
};

class ObExprNotIn : public ObExprInOrNotIn
{
public:
  explicit ObExprNotIn(common::ObIAllocator &alloc);
  virtual ~ObExprNotIn() {};
protected:
};

inline void ObExprInOrNotIn::ObExprInCtx::set_param_exist_null(bool exist_null)
{
  param_exist_null_ = exist_null;
}

inline bool ObExprInOrNotIn::ObExprInCtx::is_param_exist_null() const
{
  return param_exist_null_;
}

inline void ObExprInOrNotIn::set_param_all_const(bool all_const)
{
  if (all_const) {
    param_flags_ |= IN_PARAM_ALL_CONST;
  } else {
    param_flags_ &= ~IN_PARAM_ALL_CONST;
  }
}

inline void ObExprInOrNotIn::set_param_all_same_type(bool all_same_type)
{
  if (all_same_type) {
    param_flags_ |= IN_PARAM_ALL_SAME_TYPE;
  } else {
    param_flags_ &= ~IN_PARAM_ALL_SAME_TYPE;
  }
}

inline void ObExprInOrNotIn::set_param_all_same_cs_type(bool all_same_cs_type)
{
  if (all_same_cs_type) {
    param_flags_ |= IN_PARAM_ALL_SAME_CS_TYPE;
  } else {
    param_flags_ &= ~IN_PARAM_ALL_SAME_CS_TYPE;
  }
}
inline void ObExprInOrNotIn::set_param_is_ext_type_oracle(bool all_is_ext)
{
  if (all_is_ext) {
    param_flags_ |= IN_PARAM_IS_EXT_ORACLE;
  } else {
    param_flags_ &= ~IN_PARAM_IS_EXT_ORACLE;
  }
}

inline void ObExprInOrNotIn::set_param_is_subquery()
{
  param_flags_ |= IN_PARAM_IS_SUBQUERY;
}

inline void ObExprInOrNotIn::set_param_can_vectorized()
{
  param_flags_ |= IN_PARAM_CAN_VECTORIZED;
}

inline bool ObExprInOrNotIn::is_param_all_const() const
{
  return param_flags_ & IN_PARAM_ALL_CONST;
}

inline bool ObExprInOrNotIn::is_param_all_same_type() const
{
  return param_flags_ & IN_PARAM_ALL_SAME_TYPE;
}

inline bool ObExprInOrNotIn::is_param_all_same_cs_type() const
{
  return param_flags_ & IN_PARAM_ALL_SAME_CS_TYPE;
}
inline bool ObExprInOrNotIn::is_param_is_ext_type_oracle() const
{
  return param_flags_ & IN_PARAM_IS_EXT_ORACLE;
}

inline bool ObExprInOrNotIn::is_param_is_subquery() const
{
  return param_flags_ & IN_PARAM_IS_SUBQUERY;
}

inline bool ObExprInOrNotIn::is_param_can_vectorized() const
{
  return param_flags_ & IN_PARAM_CAN_VECTORIZED;
}
}
}
#endif  /* _OB_EXPR_IN_H_ */
