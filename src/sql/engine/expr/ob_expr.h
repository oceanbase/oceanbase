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

#ifndef OCEANBASE_EXPR_OB_EXPR_H_
#define OCEANBASE_EXPR_OB_EXPR_H_

#include "lib/container/ob_2d_array.h"
#include "lib/allocator/ob_allocator.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/ob_serializable_function.h"
#include "sql/parser/ob_item_type.h"

namespace oceanbase {
namespace common {
class ObObjParam;
}
namespace sql {

using lib::is_mysql_mode;
using lib::is_oracle_mode;

class ObExecContext;
class ObIExprExtraInfo;
using common::ObDatum;

typedef ObItemType ObExprOperatorType;

struct ObDatumMeta {
  OB_UNIS_VERSION(1);

public:
  ObDatumMeta() : type_(common::ObNullType), cs_type_(common::CS_TYPE_INVALID), scale_(-1), precision_(-1)
  {}
  ObDatumMeta(const common::ObObjType type, const common::ObCollationType cs_type, const int8_t scale)
      : type_(type), cs_type_(cs_type), scale_(scale), precision_(-1)
  {}
  ObDatumMeta(const common::ObObjType type, const common::ObCollationType cs_type, const int8_t scale,
      const common::ObPrecision prec)
      : type_(type), cs_type_(cs_type), scale_(scale), precision_(prec)
  {}

  void reset()
  {
    new (this) ObDatumMeta();
  }
  TO_STRING_KV(K_(type), K_(cs_type), K_(scale), K_(precision));

  common::ObObjType type_;
  common::ObCollationType cs_type_;
  int8_t scale_;
  union {
    common::ObPrecision precision_;
    common::ObLengthSemantics length_semantics_;
  };
  OB_INLINE bool is_clob() const
  {
    return (is_oracle_mode() && (static_cast<uint8_t>(common::ObLongTextType) == type_) &&
            (common::CS_TYPE_BINARY != cs_type_));
  }
  OB_INLINE bool is_varchar() const
  {
    return ((static_cast<uint8_t>(common::ObVarcharType) == type_) && (common::CS_TYPE_BINARY != cs_type_));
  }
  OB_INLINE bool is_char() const
  {
    return ((static_cast<uint8_t>(common::ObCharType) == type_) && (common::CS_TYPE_BINARY != cs_type_));
  }
};

// Expression evaluate result info
struct ObEvalInfo {
  void clear_evaluated_flag()
  {
    if (evaluated_) {
      evaluated_ = false;
    }
  }
  DECLARE_TO_STRING;

  union {
    struct {
      // is already evaluated
      uint16_t evaluated_ : 1;
      // is projected
      uint16_t projected_ : 1;
      // all datums are not null.
      // may not set even all datums are not null,
      // e.g.: the null values are filtered by skip bitmap.
      uint16_t notnull_ : 1;
      // pointer is point to reserved buffer in frame.
      uint16_t point_to_frame_ : 1;
    };
    uint16_t flag_;
  };
  // result count (set to batch_size in batch_eval)
  uint16_t cnt_;
};

// expression evaluate context
struct ObEvalCtx {
  friend class ObExpr;
  ObEvalCtx(ObExecContext& exec_ctx, common::ObArenaAllocator& res_alloc, common::ObArenaAllocator& tmp_alloc);

  common::ObArenaAllocator& get_reset_tmp_alloc()
  {
#ifndef NDEBUG
    tmp_alloc_.reset();
#else
    if (tmp_alloc_.used() > common::OB_MALLOC_MIDDLE_BLOCK_SIZE) {
      tmp_alloc_.reset_remain_one_page();
    }
#endif
    return tmp_alloc_;
  }

private:
  // Allocate expression result memory.
  void* alloc_expr_res(const int64_t size)
  {
    return expr_res_alloc_.alloc(size);
  }

public:
  char** frames_;
  ObExecContext& exec_ctx_;

private:
  // Expression result allocator, never reset.
  common::ObArenaAllocator& expr_res_alloc_;

  // Temporary allocator for expression evaluating, may be reset immediately after ObExpr::eval().
  // Can not use allocator for expression result. (ObExpr::get_str_res_mem() is used for result).
  common::ObArenaAllocator& tmp_alloc_;
};

typedef uint64_t (*ObExprHashFuncType)(const common::ObDatum& datum, const uint64_t seed);
typedef int (*ObExprCmpFuncType)(const common::ObDatum& datum1, const common::ObDatum& datum2);
struct ObExprBasicFuncs {
  // Default hash method:
  //    murmur for non string tyeps
  //    mysql string hash for string types
  // Try not to use it unless you need to be compatible with ObObj::hash()/ObObj::varchar_hash(),
  // use murmur_hash_ instead.
  ObExprHashFuncType default_hash_;
  // For murmur/xx/wy functions, the specified hash method is used for all tyeps.
  ObExprHashFuncType murmur_hash_;
  ObExprHashFuncType xx_hash_;
  ObExprHashFuncType wy_hash_;
  ObExprCmpFuncType null_first_cmp_;
  ObExprCmpFuncType null_last_cmp_;
};

struct ObDynReserveBuf {
  static const uint32_t MAGIC_NUM = 0xD928e5bf;
  static bool supported(const common::ObObjType& type)
  {
    const common::ObObjTypeClass tc = common::ob_obj_type_class(type);
    return common::ObStringTC == tc || common::ObTextTC == tc || common::ObRawTC == tc || common::ObRowIDTC == tc ||
           common::ObLobTC == tc;
  }

  ObDynReserveBuf() = default;

  uint32_t magic_;
  uint32_t len_;
  char* mem_;
};
static_assert(16 == sizeof(ObDynReserveBuf), "ObDynReserveBuf size can not be changed");

typedef common::ObFixedArray<common::ObString, common::ObIAllocator> ObStrValues;

struct ObExpr {
  OB_UNIS_VERSION(1);

public:
  const static uint32_t INVALID_EXP_CTX_ID = UINT32_MAX;

  ObExpr();
  OB_INLINE int eval(ObEvalCtx& ctx, common::ObDatum*& datum) const;
  int eval_enumset(ObEvalCtx& ctx, const common::ObIArray<common::ObString>& str_values, const uint64_t cast_mode,
      common::ObDatum*& datum) const;

  void reset()
  {
    new (this) ObExpr();
  }
  ObDatum& locate_expr_datum(ObEvalCtx& ctx) const
  {
    // performance critical, do not check pointer validity.
    return *reinterpret_cast<ObDatum*>(ctx.frames_[frame_idx_] + datum_off_);
  }

  ObEvalInfo& get_eval_info(ObEvalCtx& ctx) const
  {
    return *reinterpret_cast<ObEvalInfo*>(ctx.frames_[frame_idx_] + eval_info_off_);
  }

  // locate expr datum && reset ptr_ to reserved buf
  OB_INLINE ObDatum& locate_datum_for_write(ObEvalCtx& ctx) const;
  OB_INLINE ObDatum& locate_param_datum(ObEvalCtx& ctx, int param_index) const
  {
    return args_[param_index]->locate_expr_datum(ctx);
  }

  // Get result memory for string type.
  // Dynamic allocated memory is allocated if reserved buffer if not enough.
  char* get_str_res_mem(ObEvalCtx& ctx, const int64_t size) const
  {
    return OB_LIKELY(size <= res_buf_len_) ? ctx.frames_[frame_idx_] + res_buf_off_ : alloc_str_res_mem(ctx, size);
  }

  // Evaluate all parameters, assign the first sizeof...(args) parameters to %args.
  //
  // e.g.:
  //  arg_cnt_ = 2;
  //
  //  eval_param_values(ctx):
  //    call: args_[0]->eval(), args_[1]->eval()
  //
  //  eval_param_values(ctx, param0);
  //    call:args_[0]->eval(param0), args_[1]->eval();
  //
  //  eval_param_values(ctx, param0, param1, param2
  //    call: args[0]->eval(param0), args[1]->eval(param1)
  //    keep param2 unchanged.
  template <typename... TS>
  OB_INLINE int eval_param_value(ObEvalCtx& ctx, TS&... args) const;

  // deep copy %datum to reserve buffer or new allocated buffer if reserved buffer is not enough.
  OB_INLINE int deep_copy_datum(ObEvalCtx& ctx, const common::ObDatum& datum) const;

  typedef common::ObIArray<ObExpr> ObExprIArray;
  static ObExprIArray*& get_serialize_array()
  {
    static lib::CoVar<ObExprIArray*> g_expr_ser_array;
    return g_expr_ser_array;
  }

  TO_STRING_KV("type", get_type_name(type_), K_(datum_meta), K_(obj_meta), K_(obj_datum_map), KP_(eval_func),
      KP_(inner_functions), K_(inner_func_cnt), K_(arg_cnt), K_(parent_cnt), K_(frame_idx), K_(datum_off),
      K_(res_buf_off), K_(res_buf_len), K_(expr_ctx_id), K_(extra), KP(this));

private:
  char* alloc_str_res_mem(ObEvalCtx& ctx, const int64_t size) const;

public:
  typedef int (*EvalFunc)(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  typedef int (*EvalEnumSetFunc)(const ObExpr& expr, const common::ObIArray<common::ObString>& str_values,
      const uint64_t cast_mode, ObEvalCtx& ctx, ObDatum& expr_datum);

  const static uint64_t MAGIC_NUM = 0x6367614D72707845L;  // string of "ExprMagc"
  uint64_t magic_;
  ObExprOperatorType type_;
  // meta data of datum
  ObDatumMeta datum_meta_;
  // meta data of ObObj, used for ObObj converting.
  common::ObObjMeta obj_meta_;
  // max length of datum value. can be less than 0
  int32_t max_length_;
  // type of ObObj memory layout to ObDatum memory layout mapping,
  // used to convert ObDatum to ObObj and vice versa.
  common::ObObjDatumMapType obj_datum_map_;
  // expr evaluate function
  union {
    EvalFunc eval_func_;
    // helper union member for eval_func_ serialize && deserialize
    sql::serializable_function ser_eval_func_;
  };
  // aux evaluate functions for eval_func_, array of any function pointers, which interpreted
  // by eval_func_.
  // mysql row operand use the inner function array
  // to compare condition pairs. e.g.:
  //     (a, b, c) = (1, 2, 3)
  //     arg_cnt_ is 6
  //     inner_func_cnt_ is 3
  union {
    void** inner_functions_;
    // helper member for inner_functions_ serialize && deserialize
    sql::serializable_function* ser_inner_functions_;
  };
  uint32_t inner_func_cnt_;
  ObExpr** args_;
  uint32_t arg_cnt_;
  ObExpr** parents_;
  uint32_t parent_cnt_;
  // frame index
  uint32_t frame_idx_;
  // offset of ObDatum in frame
  uint32_t datum_off_;
  // offset of ObEvalInfo
  uint32_t eval_info_off_;
  // reserve buffer offset in frame
  uint32_t res_buf_off_;
  // reserve buffer length
  uint32_t res_buf_len_;
  // expr context id
  uint32_t expr_ctx_id_;
  // extra info, reinterpreted by each expr
  union {
    uint64_t extra_;
    int64_t div_calc_scale_;
    ObIExprExtraInfo* extra_info_;
  };
  ObExprBasicFuncs* basic_funcs_;
};

// helper template to access ObExpr::extra_
template <typename T>
struct ObExprExtraInfoAccess {
  static T& get_info(int64_t& v)
  {
    return *reinterpret_cast<T*>(&v);
  }
  static const T& get_info(const int64_t& v)
  {
    return *reinterpret_cast<const T*>(&v);
  }
  static T& get_info(ObExpr& e)
  {
    return get_info(*reinterpret_cast<int64_t*>(&e.extra_));
  }
  static const T& get_info(const ObExpr& e)
  {
    return get_info(*reinterpret_cast<const int64_t*>(&e.extra_));
  }
};

// Wrap expression string result buffer allocation to allocator interface.
// Please try not to use this, if you mast use it, make sure it only allocate one time and is
// for expression result.
class ObExprStrResAlloc : public common::ObIAllocator {
public:
  ObExprStrResAlloc(const ObExpr& expr, ObEvalCtx& ctx) : off_(0), expr_(expr), ctx_(ctx)
  {}

  void* alloc(const int64_t size) override;

private:
  int64_t off_;
  const ObExpr& expr_;
  ObEvalCtx& ctx_;
};

struct ObDatumObj {
public:
  void set_scale(common::ObScale scale)
  {
    meta_.scale_ = scale;
  }
  TO_STRING_KV(K_(meta));

public:
  ObDatumMeta meta_;
  common::ObDatum datum_;
};

typedef common::ObIArray<ObExpr*> ObExprPtrIArray;

// copy from ObObjParam
struct ObDatumObjParam : public ObDatumObj {
  ObDatumObjParam() : ObDatumObj(), accuracy_(), res_flags_(0), flag_()
  {}
  ObDatumObjParam(const ObDatumObj& other) : ObDatumObj(other), accuracy_(), res_flags_(0), flag_()
  {}

  TO_STRING_KV(K_(accuracy), K_(res_flags), K_(datum), K_(meta));

public:
  int from_objparam(const common::ObObjParam& objparam);

  int to_objparam(common::ObObjParam& obj_param);

  OB_INLINE void set_datum(const common::ObDatum& datum)
  {
    datum_ = datum;
  }

  OB_INLINE void set_meta(const ObDatumMeta& meta)
  {
    meta_ = meta;
  }
  // accuracy.
  OB_INLINE void set_accuracy(const common::ObAccuracy& accuracy)
  {
    accuracy_.set_accuracy(accuracy);
  }
  OB_INLINE void set_length(common::ObLength length)
  {
    accuracy_.set_length(length);
  }
  OB_INLINE void set_precision(common::ObPrecision precision)
  {
    accuracy_.set_precision(precision);
  }
  OB_INLINE void set_length_semantics(common::ObLengthSemantics length_semantics)
  {
    accuracy_.set_length_semantics(length_semantics);
  }
  OB_INLINE void set_scale(common::ObScale scale)
  {
    ObDatumObj::set_scale(scale);
    accuracy_.set_scale(scale);
  }
  OB_INLINE void set_udt_id(uint64_t id)
  {
    accuracy_.set_accuracy(id);
  }
  OB_INLINE const common::ObAccuracy& get_accuracy() const
  {
    return accuracy_;
  }
  OB_INLINE common::ObLength get_length() const
  {
    return accuracy_.get_length();
  }
  OB_INLINE common::ObPrecision get_precision() const
  {
    return accuracy_.get_precision();
  }
  OB_INLINE common::ObScale get_scale() const
  {
    return accuracy_.get_scale();
  }
  OB_INLINE uint64_t get_udt_id() const
  {
    return meta_.type_ == static_cast<uint8_t>(common::ObExtendType) ? accuracy_.get_accuracy()
                                                                     : common::OB_INVALID_INDEX;
  }
  OB_INLINE void set_result_flag(uint32_t flag)
  {
    res_flags_ |= flag;
  }
  OB_INLINE void unset_result_flag(uint32_t flag)
  {
    res_flags_ &= (~flag);
  }
  OB_INLINE bool has_result_flag(uint32_t flag) const
  {
    return res_flags_ & flag;
  }
  OB_INLINE uint32_t get_result_flag() const
  {
    return res_flags_;
  }

  OB_INLINE const common::ParamFlag& get_param_flag() const
  {
    return flag_;
  }
  OB_INLINE void set_need_to_check_type(bool flag)
  {
    flag_.need_to_check_type_ = flag;
  }
  OB_INLINE bool need_to_check_type() const
  {
    return flag_.need_to_check_type_;
  }

  OB_INLINE void set_need_to_check_bool_value(bool flag)
  {
    flag_.need_to_check_bool_value_ = flag;
  }
  OB_INLINE bool need_to_check_bool_value() const
  {
    return flag_.need_to_check_bool_value_;
  }

  OB_INLINE void set_expected_bool_value(bool b_value)
  {
    flag_.expected_bool_value_ = b_value;
  }
  OB_INLINE bool expected_bool_value() const
  {
    return flag_.expected_bool_value_;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

  static uint32_t accuracy_offset_bits()
  {
    return offsetof(ObDatumObjParam, accuracy_) * 8;
  }
  static uint32_t res_flags_offset_bits()
  {
    return offsetof(ObDatumObjParam, res_flags_) * 8;
  }
  static uint32_t flag_offset_bits()
  {
    return offsetof(ObDatumObjParam, flag_) * 8;
  }

private:
  common::ObAccuracy accuracy_;
  uint32_t res_flags_;  // BINARY, NUM, NOT_NULL, TIMESTAMP, etc
                        // reference: src/lib/regex/include/mysql_com.h
  common::ParamFlag flag_;
};

typedef common::Ob2DArray<ObDatumObjParam, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObWrapperAllocator>
    DatumParamStore;

// Helper function for print datum which interpreted by expression
// Can only be used in log message like this:
//    LOG_WARN(....., "datum", DATUM2STR(*expr, *datum))
struct ObToStringDatum {
  ObToStringDatum(const ObExpr& e, const common::ObDatum& d) : e_(e), d_(d)
  {}

  DECLARE_TO_STRING;

  const ObExpr& e_;
  const common::ObDatum& d_;
};
typedef ObToStringDatum DATUM2STR;

// Helper function for expression evaluate && print result
// Can only be used in log message like this:
//    LOG_WARN(....., "datum", EXPR2STR(eval_ctx_, *expr))
//
// do not call EXPR2STR/ObToStringExpr in eval_func_ of ObExpr,
// since EXPR2STR/ObToStringExpr will call eval_func_ again,
// which can form a infinite loop.
struct ObToStringExpr {
  ObToStringExpr(ObEvalCtx& ctx, const ObExpr& e) : c_(ctx), e_(e)
  {}

  DECLARE_TO_STRING;

  ObEvalCtx& c_;
  const ObExpr& e_;
};
typedef ObToStringExpr EXPR2STR;

// Helper function for expression array evaluate && print result
// Can only be used in log message like this:
//    LOG_WARN(......, "row", ROWEXPR2STR(eval_ctx_, output_)
struct ObToStringExprRow {
  ObToStringExprRow(ObEvalCtx& ctx, const common::ObIArray<ObExpr*>& exprs) : c_(ctx), exprs_(exprs)
  {
    for (int64_t i = 0; i < exprs_.count(); i++) {
      common::ObDatum* datum = NULL;
      ObExpr* e = exprs_.at(i);
      if (OB_NOT_NULL(e)) {
        IGNORE_RETURN e->eval(c_, datum);
      }
    }
  }

  DECLARE_TO_STRING;

  ObEvalCtx& c_;
  const common::ObIArray<ObExpr*>& exprs_;
};
typedef ObToStringExprRow ROWEXPR2STR;

OB_INLINE ObDatum& ObExpr::locate_datum_for_write(ObEvalCtx& ctx) const
{
  // performance critical, do not check pointer validity.
  char* frame = ctx.frames_[frame_idx_];
  OB_ASSERT(NULL != frame);
  ObDatum* expr_datum = (ObDatum*)(frame + datum_off_);
  if (expr_datum->ptr_ != frame + res_buf_off_) {
    expr_datum->ptr_ = frame + res_buf_off_;
  }
  return *expr_datum;
}

template <typename... TS>
OB_INLINE int ObExpr::eval_param_value(ObEvalCtx& ctx, TS&... args) const
{
  int ret = common::OB_SUCCESS;
  common::ObDatum** params[] = {&args...};
  common::ObDatum* tmp = NULL;
  for (int param_index = 0; OB_SUCC(ret) && param_index < arg_cnt_; param_index++) {
    if (OB_FAIL(args_[param_index]->eval(ctx, param_index < ARRAYSIZEOF(params) ? *params[param_index] : tmp))) {
      SQL_LOG(WARN, "evaluate parameter failed", K(ret), K(param_index));
    }
  }
  return ret;
}

OB_INLINE int ObExpr::eval(ObEvalCtx& ctx, common::ObDatum*& datum) const
{
  // performance critical, do not check %frame_idx_ and %frame again. (checked in CG)
  int ret = common::OB_SUCCESS;
  char* frame = ctx.frames_[frame_idx_];
  OB_ASSERT(NULL != frame);
  datum = (ObDatum*)(frame + datum_off_);
  ObEvalInfo* eval_info = (ObEvalInfo*)(frame + eval_info_off_);

  // do nothing for const/column reference expr or already evaluated expr
  if (NULL != eval_func_ && !eval_info->evaluated_) {
    if (datum->ptr_ != frame + res_buf_off_) {
      datum->ptr_ = frame + res_buf_off_;
    }
    ret = eval_func_(*this, ctx, *datum);
    if (OB_LIKELY(common::OB_SUCCESS == ret)) {
      eval_info->evaluated_ = true;
    } else {
      datum->set_null();
    }
  }
  return ret;
}

OB_INLINE int ObExpr::deep_copy_datum(ObEvalCtx& ctx, const common::ObDatum& datum) const
{
  int ret = common::OB_SUCCESS;
  // shadow copy datum first, because %datum may overlay with %dst
  common::ObDatum src = datum;
  ObDatum& dst = this->locate_datum_for_write(ctx);
  dst.pack_ = src.pack_;
  if (!src.null_) {
    if (OB_UNLIKELY(src.len_ > res_buf_len_)) {
      // only string datum may exceed %res_buf_len_;
      if (OB_ISNULL(dst.ptr_ = get_str_res_mem(ctx, src.len_))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        MEMMOVE(const_cast<char*>(dst.ptr_), src.ptr_, src.len_);
      }
    } else {
      MEMMOVE(const_cast<char*>(dst.ptr_), src.ptr_, src.len_);
    }
  }
  return ret;
}

}  // end namespace sql
namespace common {
namespace serialization {

// ObExpr pointer serialize && deserialize.
// Convert pointer to index of expr array, serialize && deserialize the index.
// The serialized index is the real index + 1, to make room for NULL.
inline int64_t encoded_length(sql::ObExpr*)
{
  return sizeof(uint32_t);
}

inline int encode(char* buf, const int64_t buf_len, int64_t& pos, sql::ObExpr* expr)
{
  int ret = common::OB_SUCCESS;
  uint32_t idx = 0;
  if (NULL != expr) {
    sql::ObExpr::ObExprIArray* array = sql::ObExpr::get_serialize_array();
    if (OB_UNLIKELY(NULL == array || array->empty() || expr < &array->at(0) ||
                    (idx = expr - &array->at(0) + 1) > array->count())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "expr not in array", K(ret), KP(array), KP(idx), KP(expr));
    }
  }
  if (OB_SUCC(ret)) {
    ret = encode_i32(buf, buf_len, pos, idx);
  }
  return ret;
}

inline int decode(const char* buf, const int64_t data_len, int64_t& pos, sql::ObExpr*& expr)
{
  int ret = common::OB_SUCCESS;
  uint32_t idx = 0;
  ret = decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&idx));
  if (OB_SUCC(ret)) {
    if (0 == idx) {
      expr = NULL;
    } else {
      sql::ObExpr::ObExprIArray* array = sql::ObExpr::get_serialize_array();
      if (OB_UNLIKELY(NULL == array) || OB_UNLIKELY(idx > array->count())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "expr index out of expr array range", K(ret), KP(array), K(idx));
      } else {
        expr = &array->at(idx - 1);
      }
    }
  }
  return ret;
}

template <>
struct EnumEncoder<false, sql::ObExpr*> {
  static int encode(char* buf, const int64_t buf_len, int64_t& pos, sql::ObExpr* expr)
  {
    return serialization::encode(buf, buf_len, pos, expr);
  }

  static int decode(const char* buf, const int64_t data_len, int64_t& pos, sql::ObExpr*& expr)
  {
    return serialization::decode(buf, data_len, pos, expr);
  }

  static int64_t encoded_length(sql::ObExpr* expr)
  {
    return serialization::encoded_length(expr);
  }
};

//
// Simple c array wrapper for serialize && deserialize. e.g.:
//  OB_SERIALIZE_MEMBER(Foo, make_ser_carray(items_, item_cnt_))
//
template <typename T, typename U>
struct ObSerCArray {
  ObSerCArray(T& data, U& cnt) : data_(data), cnt_(cnt)
  {}
  T& data_;
  U& cnt_;
};

template <typename T, typename U>
ObSerCArray<T, U> make_ser_carray(T& data, U& cnt)
{
  return ObSerCArray<T, U>(data, cnt);
}

template <typename T, typename U>
inline int64_t encoded_length(const ObSerCArray<T* const, U>& array)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(array.data_, array.cnt_);
  return len;
}

template <typename T, typename U>
inline int encode(char* buf, const int64_t buf_len, int64_t& pos, const ObSerCArray<T* const, U>& array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(array.cnt_ > 0 && NULL == array.data_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "array not empty but data is NULL", K(ret), KP(array.data_), K(array.cnt_));
  } else {
    OB_UNIS_ENCODE_ARRAY(array.data_, array.cnt_);
  }
  return ret;
}

template <typename T, typename U>
inline int decode(const char* buf, const int64_t data_len, int64_t& pos, const ObSerCArray<T*, U>& array)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(array.cnt_);
  if (OB_SUCC(ret)) {
    if (0 == array.cnt_) {
      array.data_ = NULL;
    } else {
      const int64_t alloc_size = sizeof(*array.data_) * array.cnt_;
      array.data_ = static_cast<T*>(CURRENT_CONTEXT.get_arena_allocator().alloc(alloc_size));
      if (OB_ISNULL(array.data_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "alloc memory failed", K(ret), K(alloc_size));
      } else {
        int64_t idx = 0;
        for (; OB_SUCC(ret) && idx < array.cnt_; idx++) {
          new (&array.data_[idx]) T();
          OB_UNIS_DECODE(array.data_[idx]);
        }
        // deconstruct the constructed objects.
        if (OB_SUCCESS != ret) {
          for (int64_t i = idx; i >= 0; i--) {
            array.data_[i].~T();
          }
        }
      }
    }
  }
  return ret;
}

}  // end namespace serialization
}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_EXPR_OB_EXPR_H_
