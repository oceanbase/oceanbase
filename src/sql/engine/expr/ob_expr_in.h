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

namespace oceanbase {
namespace sql {
// Member explanation:
// The binary number of idx_ represents the columns of the Row selected in the hash table as the key.
// For example, the vector (1,2,3) is stored in the hash table with idx_=3, binary(3)=011,
// that is, the lower 2 bits are selected ,key=(1,2)
// row_dimension_ store the dimension of Row
// hash_funcs_ length is row_dimension_, the hash function of datum, obj will not be set and used
// cmp_funcs_ length is row_dimension_, compare function of datum, obj will not be set and used
struct HashMapMeta {
  HashMapMeta() : idx_(-1), row_dimension_(-1), hash_funcs_(NULL), cmp_funcs_(NULL)
  {}
  ~HashMapMeta()
  {}
  int idx_;
  int64_t row_dimension_;
  void** hash_funcs_;
  void** cmp_funcs_;
};

/*
 *  Common row structure, encapsulating datum or obj, the comparison
 *  method of datum needs to rely on external input
 *  The parameter idx represents the selected column, which appears in the
 *  comparison method and the hash method of the hashtable
 */
template <class T>
class Row {
public:
  Row() : elems_(NULL)
  {}
  ~Row()
  {}
  bool equal_key(const Row& other, void** cmp_funcs, const int idx) const;
  uint64_t hash_key(void** hash_funcs, const int idx, uint64_t seed) const;
  int compare_with_null(const Row& other, void** cmp_funcs, const int64_t row_dimension, int& exist_ret) const;
  int set_elem(T* elems);
  T& get_elem(int idx) const
  {
    return elems_[idx];
  }
  T* get_elems() const
  {
    return elems_;
  }
  TO_STRING_KV(KP(elems_));

private:
  T* elems_;
};
template <class T>
struct RowKey {
public:
  RowKey() : row_(), meta_(NULL)
  {}
  ~RowKey()
  {}
  bool operator==(const RowKey<T>& other) const;
  uint64_t hash(uint64_t seed = 0) const;
  Row<T> row_;
  HashMapMeta* meta_;
};

template <class T>
class ObExprInHashMap {
public:
  const static int HASH_CMP_TRUE = 0;
  const static int HASH_CMP_FALSE = -1;
  const static int HASH_CMP_UNKNOWN = 1;
  ObExprInHashMap() : map_(), meta_()
  {}
  ~ObExprInHashMap()
  {}
  void destroy()
  {
    if (map_.created()) {
      for (auto it = map_.begin(); it != map_.end(); ++it) {
        it->second.destroy();
      }
      (void)map_.destroy();
    }
  }
  int create(int param_num)
  {
    return map_.create(param_num * 2, common::ObModIds::OB_HASH_BUCKET);
  }
  int set_refactored(const Row<T>& row);
  int exist_refactored(const Row<T>& row, int& exist_ret);
  void set_meta_idx(int idx)
  {
    meta_.idx_ = idx;
  }
  void set_meta_dimension(int64_t row_dimension)
  {
    meta_.row_dimension_ = row_dimension;
  }
  void set_meta_hash(void** hash_funcs)
  {
    meta_.hash_funcs_ = hash_funcs;
  }
  void set_meta_cmp(void** cmp_funcs)
  {
    meta_.cmp_funcs_ = cmp_funcs;
  }

private:
  common::hash::ObHashMap<RowKey<T>, common::ObArray<Row<T>>, common::hash::NoPthreadDefendMode> map_;
  HashMapMeta meta_;
};

template <class T>
class ObExprInHashSet {
public:
  ObExprInHashSet() : set_(), meta_()
  {}
  ~ObExprInHashSet()
  {}
  void destroy()
  {
    if (set_.created()) {
      (void)set_.destroy();
    }
  }
  int64_t size() const
  {
    return set_.size();
  }
  int create(int param_num)
  {
    return set_.create(param_num);
  }
  int set_refactored(const Row<T>& row);
  int exist_refactored(const Row<T>& row, bool& is_exist);
  void set_meta_idx(int idx)
  {
    meta_.idx_ = idx;
  }
  void set_meta_dimension(int64_t row_dimension)
  {
    meta_.row_dimension_ = row_dimension;
  }
  void set_meta_hash(void** hash_funcs)
  {
    meta_.hash_funcs_ = hash_funcs;
  }
  void set_meta_cmp(void** cmp_funcs)
  {
    meta_.cmp_funcs_ = cmp_funcs;
  }

private:
  common::hash::ObHashSet<RowKey<T>, common::hash::NoPthreadDefendMode> set_;
  HashMapMeta meta_;
};

class ObExecContext;
class ObSubQueryIterator;

class ObExprInOrNotIn : public ObVectorExprOperator {
  class ObExprInCtx : public ObExprOperatorCtx {
  public:
    ObExprInCtx()
        : ObExprOperatorCtx(),
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
    {}
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
    int init_hashset_vecs(int64_t param_num, int64_t row_dimension, ObExecContext* exec_ctx);
    int init_static_engine_hashset(int64_t param_num);
    int init_static_engine_hashset_vecs(int64_t param_num, int64_t row_dimension, ObExecContext* exec_ctx);
    int add_to_hashset(const common::ObObj& obj);
    int add_to_hashset_vecs(const Row<common::ObObj>& row, const int idx);
    int add_to_static_engine_hashset(const Row<common::ObDatum>& row);
    int add_to_static_engine_hashset_vecs(const Row<common::ObDatum>& row, const int idx);
    int exist_in_hashset(const common::ObObj& obj, bool& is_exist) const;
    int exist_in_hashset_vecs(const Row<common::ObObj>& objs, const int idx, int& exist_ret) const;
    int exist_in_static_engine_hashset(const Row<common::ObDatum>& row, bool& is_exist);
    int exist_in_static_engine_hashset_vecs(const Row<common::ObDatum>& row, const int idx, int& exist_ret);
    int init_cmp_types(const int64_t row_dimension, ObExecContext* exec_ctx);
    int init_hashset_vecs_all_null(const int64_t row_dimension, ObExecContext* exec_ctx);
    inline void set_cmp_type(const ObExprCalcType& cmp_type);
    inline int set_hashset_vecs_all_null_true(const int64_t idx);
    inline const ObExprCalcType& get_cmp_type() const;
    int get_hashset_vecs_all_null(const int64_t idx, bool& is_all_null) const;
    inline void set_param_exist_null(bool exist_null);
    inline bool is_param_exist_null() const;
    inline int set_cmp_types(const ObExprCalcType& cmp_type, const int64_t row_dimension);
    int64_t get_static_engine_hashset_size() const
    {
      return static_engine_hashset_.size();
    }
    inline const ObExprCalcType& get_cmp_types(const int64_t idx) const;

    bool is_hash_calc_disabled() const
    {
      return hash_calc_disabled_;
    }
    void disable_hash_calc(void)
    {
      hash_calc_disabled_ = true;
    }
    int init_right_objs(int64_t param_num, int64_t row_dimension, ObExecContext* exec_ctx);
    int init_right_datums(int64_t param_num, int64_t row_dimension, ObExecContext* exec_ctx);
    int set_right_obj(int64_t row_num, int64_t col_num, const int right_param_num, const common::ObObj& obj);
    int set_right_datum(int64_t row_num, int64_t col_num, const int right_param_num, const common::ObDatum& datum);
    common::ObObj* get_obj_row(int64_t row_num)
    {
      return right_objs_[row_num];
    }
    common::ObDatum* get_datum_row(int64_t row_num)
    {
      return right_datums_[row_num];
    }
    void set_hash_funcs_ptr(int64_t idx, void** hash_funcs_ptr)
    {
      static_engine_hashset_vecs_[idx].set_meta_hash(hash_funcs_ptr);
    }
    void set_cmp_funcs_ptr(int64_t idx, void** cmp_funcs_ptr)
    {
      static_engine_hashset_vecs_[idx].set_meta_cmp(cmp_funcs_ptr);
    }
    void set_hash_funcs_ptr_for_set(void** hash_funcs_ptr)
    {
      static_engine_hashset_.set_meta_hash(hash_funcs_ptr);
    }
    void set_cmp_funcs_ptr_for_set(void** cmp_funcs_ptr)
    {
      static_engine_hashset_.set_meta_cmp(cmp_funcs_ptr);
    }
    int row_dimension_;
    bool right_has_null;
    void** hash_func_buff_;
    bool funcs_ptr_set;
    bool ctx_hash_null_;

  private:
    common::ObObj** right_objs_;
    common::ObDatum** right_datums_;
    common::hash::ObHashSet<common::ObObj, common::hash::NoPthreadDefendMode> hashset_;
    ObExprInHashMap<common::ObObj>* hashset_vecs_;
    ObExprInHashSet<common::ObDatum> static_engine_hashset_;
    ObExprInHashMap<common::ObDatum>* static_engine_hashset_vecs_;
    common::ObFixedArray<bool, common::ObIAllocator> hashset_vecs_all_null_;
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
  ObExprInOrNotIn(common::ObIAllocator& alloc, ObExprOperatorType type, const char* name);
  virtual ~ObExprInOrNotIn(){};
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const;

  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;

  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  int cg_expr_without_row(common::ObIAllocator& allocator, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  int cg_expr_with_row(common::ObIAllocator& allocator, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  // like "select 1 from dual where (select 1, 2) in ((1,2), (3,4))"
  int cg_expr_with_subquery(common::ObIAllocator& allocator, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  static void set_datum_result(
      const bool is_expr_in, const bool is_exist, const bool param_exists_null, ObDatum& result);
  virtual bool need_rt_ctx() const override
  {
    return true;
  }

public:
  static int eval_in_with_row(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int eval_in_without_row(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int eval_in_with_row_fallback(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int eval_in_without_row_fallback(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  // like "select 1 from dual where (select 1, 2) in ((1,2), (3,4))"
  static int eval_in_with_subquery(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int calc_for_row_static_engine(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, ObExpr** l_row);

public:
  inline void set_param_all_const(bool all_const);
  inline void set_param_all_same_type(bool all_same_type);
  inline void set_param_all_same_cs_type(bool all_same_cs_type);
  inline void set_param_is_ext_type_oracle(bool all_is_ext);
  // for func visit_in_expr , when left param is subquery set true
  inline void set_param_is_subquery();
  // static bool nd_hash_;
protected:
  int calc(common::ObObj& result, const common::ObObj* objs, const common::ObIArray<ObExprCalcType>& cmp_types,
      int64_t param_num, common::ObCompareCtx& cmp_ctx, common::ObExprCtx& expr_ctx, common::ObCastCtx& cast_ctx) const;
  // like "select 1 from dual where (select 1, 2) in ((1,2), (3,4))"
  int calc_for_subquery(common::ObObj& result, const common::ObObj* objs,
      const common::ObIArray<ObExprCalcType>& cmp_types, int64_t param_num, common::ObCompareCtx& cmp_ctx,
      common::ObExprCtx& expr_ctx, common::ObCastCtx& cast_ctx) const;
  int calc_for_row(const common::ObObj* objs, const common::ObIArray<ObExprCalcType>& cmp_types,
      common::ObCompareCtx& cmp_ctx, common::ObExprCtx& expr_ctx, common::ObCastCtx& cast_ctx,
      common::ObNewRow* left_row, bool& set_cnt_equal, bool& set_cnt_null) const;
  // need fall back to cacl() if invalid expr exist in in list. e.g.:
  //   c1 in (1, 2, 3, 4/0, 5)
  //   if c1 is 1, we should return match, not zero divisor error.
  int hash_calc(common::ObObj& result, const common::ObObj* objs, const common::ObIArray<ObExprCalcType>& cmp_types,
      int64_t param_num, common::ObExprCtx& expr_ctx, bool& fall_back) const;

  int hash_calc_for_vector(common::ObObj& result, const common::ObObj* objs,
      const common::ObIArray<ObExprCalcType>& cmp_types, int64_t param_num, common::ObExprCtx& expr_ctx,
      bool& fall_back) const;

  int to_type(const common::ObObjType expect_type, const common::ObCollationType expect_cs_type,
      common::ObCastCtx& cast_ctx, const common::ObObj& in_obj, common::ObObj& out_obj) const;

  virtual void set_result(common::ObObj& result, bool is_exist, bool param_exist_null) const = 0;
  inline bool is_param_all_const() const;
  inline bool is_param_all_same_type() const;
  inline bool is_param_all_same_cs_type() const;
  inline bool is_param_is_ext_type_oracle() const;
  inline bool is_param_is_subquery() const;
  inline bool need_hash(ObExecContext* exec_ctx) const;

  int get_param_types(const ObRawExpr& param, const bool is_iter, common::ObIArray<common::ObObjMeta>& types) const;
  static int setup_row(ObExpr** expr, ObEvalCtx& ctx, const bool is_iter, const int64_t cmp_func_cnt,
      ObSubQueryIterator*& iter, ObExpr**& row);

protected:
  typedef uint32_t ObExprInParamFlag;
  static const uint32_t IN_PARAM_ALL_CONST = 1U << 0;
  static const uint32_t IN_PARAM_ALL_SAME_TYPE = 1U << 1;
  static const uint32_t IN_PARAM_ALL_SAME_CS_TYPE = 1U << 2;
  static const uint32_t IN_PARAM_IS_EXT_ORACLE = 1U << 3;
  static const uint32_t IN_PARAM_IS_SUBQUERY = 1u << 4;
  ObExprInParamFlag param_flags_;
  DISALLOW_COPY_AND_ASSIGN(ObExprInOrNotIn);
};

class ObExprIn : public ObExprInOrNotIn {
public:
  explicit ObExprIn(common::ObIAllocator& alloc);
  virtual ~ObExprIn(){};

protected:
  virtual void set_result(common::ObObj& result, bool is_exist, bool param_exist_null) const;
};

class ObExprNotIn : public ObExprInOrNotIn {
public:
  explicit ObExprNotIn(common::ObIAllocator& alloc);
  virtual ~ObExprNotIn(){};

protected:
  virtual void set_result(common::ObObj& result, bool is_exist, bool param_exist_null) const;
};

inline void ObExprInOrNotIn::ObExprInCtx::set_cmp_type(const ObExprCalcType& cmp_type)
{
  if (cmp_type_.is_null()) {
    cmp_type_ = cmp_type;
  }
}

inline const ObExprCalcType& ObExprInOrNotIn::ObExprInCtx::get_cmp_type() const
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
}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_EXPR_IN_H_ */
