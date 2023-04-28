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

namespace oceanbase
{
namespace sql
{
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
  RowKey() : row_(), meta_(NULL) {}
  ~RowKey() {}
  bool operator==(const RowKey<T> &other) const;
  int hash(uint64_t &hash_val, uint64_t seed = 0) const;
  Row<T> row_;//指向in_ctx中存储的数据
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
  int create(int param_num) {return map_.create(param_num * 2, common::ObModIds::OB_HASH_BUCKET);}
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
  int64_t size() const { return set_.size(); }
  int create(int param_num) { return set_.create(param_num); }
  int set_refactored(const Row<T> &row);
  int exist_refactored(const Row<T> &row, bool &is_exist);
  void set_meta_idx(int idx) { meta_.idx_ = idx; }
  void set_meta_dimension(int64_t row_dimension) { meta_.row_dimension_ = row_dimension; }
  void set_meta_hash(void **hash_funcs) { meta_.hash_funcs_ = hash_funcs; }
  void set_meta_cmp(void **cmp_funcs) { meta_.cmp_funcs_= cmp_funcs; }
private:
  common::hash::ObHashSet<RowKey<T>, common::hash::NoPthreadDefendMode> set_;
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
        right_has_null(false),
        hash_func_buff_(NULL),
        funcs_ptr_set(false),
        ctx_hash_null_(false),
        right_objs_(NULL),
        right_datums_(NULL),
        hashset_(),
        hashset_vecs_(NULL),
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
      if (hashset_.created()) {
        (void)hashset_.destroy();
      }
      static_engine_hashset_.destroy();
      if (OB_NOT_NULL(hashset_vecs_)) {
        for (int i = 0; i < (1 << row_dimension_); ++i) {
          hashset_vecs_[i].destroy();
        }
        hashset_vecs_ = NULL;
      }
      if (OB_NOT_NULL(static_engine_hashset_vecs_)) {
        for (int i = 0; i < (1 << row_dimension_); ++i) {
          static_engine_hashset_vecs_[i].destroy();
        }
        static_engine_hashset_vecs_ = NULL;
      }
    }
  public:
    int init_hashset(int64_t param_num);
    int init_hashset_vecs(int64_t param_num,
                          int64_t row_dimension,
                          ObExecContext *exec_ctx);
    int init_static_engine_hashset(int64_t param_num);
    int init_static_engine_hashset_vecs(int64_t param_num,
                                        int64_t row_dimension,
                                        ObExecContext *exec_ctx);
    int add_to_hashset(const common::ObObj &obj);
    int add_to_hashset_vecs(const Row<common::ObObj> &row, const int idx);
    int add_to_static_engine_hashset(const Row<common::ObDatum> &row);
    int add_to_static_engine_hashset_vecs(const Row<common::ObDatum> &row,
                                          const int idx);
    int exist_in_hashset(const common::ObObj &obj, bool &is_exist) const;
    int exist_in_hashset_vecs(const Row<common::ObObj> &objs,
                                       const int idx,
                                       int &exist_ret) const;
    int exist_in_static_engine_hashset(const Row<common::ObDatum> &row,
                                       bool &is_exist);
    int exist_in_static_engine_hashset_vecs(const Row<common::ObDatum> &row,
                                             const int idx,
                                             int &exist_ret);
    int init_cmp_types(const int64_t row_dimension, ObExecContext *exec_ctx);
    int init_hashset_vecs_all_null(const int64_t row_dimension,
                                   ObExecContext *exec_ctx);
    inline void set_cmp_type(const ObExprCalcType &cmp_type);
    inline int set_hashset_vecs_all_null_true(const int64_t idx);
    inline const ObExprCalcType &get_cmp_type() const;
    int get_hashset_vecs_all_null(const int64_t idx,  bool &is_all_null) const;
    inline void set_param_exist_null(bool exist_null);
    inline bool is_param_exist_null() const;
    inline int set_cmp_types(const ObExprCalcType &cmp_type,
                             const int64_t row_dimension);
    int64_t get_static_engine_hashset_size() const { return static_engine_hashset_.size(); }
    inline const ObExprCalcType &get_cmp_types(const int64_t idx) const;

    bool is_hash_calc_disabled() const { return hash_calc_disabled_; }
    void disable_hash_calc(void) { hash_calc_disabled_ = true; }
    int init_right_objs(int64_t param_num,
                          int64_t row_dimension,
                          ObExecContext *exec_ctx);
    int init_right_datums(int64_t param_num,
                          int64_t row_dimension,
                          ObExecContext *exec_ctx);
    int set_right_obj(int64_t row_num,
                      int64_t col_num,
                      const int right_param_num,
                      const common::ObObj &obj);
    int set_right_datum(int64_t row_num,
                        int64_t col_num,
                        const int right_param_num,
                        const common::ObDatum &datum);
    common::ObObj *get_obj_row(int64_t row_num) { return right_objs_[row_num]; }
    common::ObDatum *get_datum_row(int64_t row_num) { return right_datums_[row_num]; }
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
    bool right_has_null;
    void **hash_func_buff_;
    bool funcs_ptr_set;
    bool ctx_hash_null_;
  private:
    common::ObObj **right_objs_;//IN 右边的常量储存于此
    common::ObDatum **right_datums_;//IN 右边的常量储存于此
    common::hash::ObHashSet<common::ObObj,
                  common::hash::NoPthreadDefendMode> hashset_;
    ObExprInHashMap<common::ObObj> *hashset_vecs_;
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
  int cg_expr_without_row(common::ObIAllocator &allocator, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const;
  int cg_expr_with_row(common::ObIAllocator &allocator, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  // like "select 1 from dual where (select 1, 2) in ((1,2), (3,4))"
  int cg_expr_with_subquery(common::ObIAllocator &allocator, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  static void set_datum_result(const bool is_expr_in,
                               const bool is_exist,
                               const bool param_exists_null,
                               ObDatum &result);
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
  int to_type(const common::ObObjType expect_type, const common::ObCollationType expect_cs_type,
              common::ObCastCtx &cast_ctx, const common::ObObj &in_obj, common::ObObj &out_obj) const;

  virtual void set_result(common::ObObj &result, bool is_exist, bool param_exist_null) const = 0;
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
                       common::ObIArray<common::ObObjMeta> &types) const;
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
  virtual void set_result(common::ObObj &result, bool is_exist, bool param_exist_null) const;
};

class ObExprNotIn : public ObExprInOrNotIn
{
public:
  explicit ObExprNotIn(common::ObIAllocator &alloc);
  virtual ~ObExprNotIn() {};
protected:
  virtual void set_result(common::ObObj &result, bool is_exist, bool param_exist_null) const;
};

inline void ObExprInOrNotIn::ObExprInCtx::set_cmp_type(const ObExprCalcType &cmp_type)
{
  if (cmp_type_.is_null()) {
    cmp_type_ = cmp_type;
  }
}

inline const ObExprCalcType &ObExprInOrNotIn::ObExprInCtx::get_cmp_type() const
{
  return cmp_type_;
}

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
