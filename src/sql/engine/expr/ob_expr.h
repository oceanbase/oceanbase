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
#include "lib/oblog/ob_log.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/ob_serializable_function.h"
#include "objit/common/ob_item_type.h"
#include "sql/engine/ob_bit_vector.h"
#include "common/ob_common_utility.h"

namespace oceanbase
{
namespace common
{
 class ObObjParam;
}

namespace storage
{
  // forward declaration for friends
  class ObVectorStore;
}
namespace sql
{

using lib::is_oracle_mode;
using lib::is_mysql_mode;

class ObExecContext;
struct ObIExprExtraInfo;
struct ObSqlDatumArray;
class ObDatumCaster;
using common::ObDatum;
using common::ObDatumVector;

typedef ObItemType ObExprOperatorType;

struct ObDatumMeta
{
  OB_UNIS_VERSION(1);
public:
  ObDatumMeta() : type_(common::ObNullType), cs_type_(common::CS_TYPE_INVALID)
                  , scale_(-1), precision_(-1)
  {
  }
  ObDatumMeta(const common::ObObjType type,
              const common::ObCollationType cs_type,
              const int8_t scale)
    : type_(type), cs_type_(cs_type), scale_(scale), precision_(-1)
  {
  }
  ObDatumMeta(const common::ObObjType type,
              const common::ObCollationType cs_type,
              const int8_t scale,
              const common::ObPrecision prec)
    : type_(type), cs_type_(cs_type), scale_(scale), precision_(prec)
  {
  }

  void reset() { new (this) ObDatumMeta(); }
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
    return (is_oracle_mode()
            && (static_cast<uint8_t>(common::ObLongTextType) == type_)
            && (common::CS_TYPE_BINARY != cs_type_));
  }
  OB_INLINE bool is_varchar() const
  {
    return ((static_cast<uint8_t>(common::ObVarcharType) == type_)
            && (common::CS_TYPE_BINARY != cs_type_));
  }
  OB_INLINE bool is_char() const
  {
    return ((static_cast<uint8_t>(common::ObCharType) == type_)
            && (common::CS_TYPE_BINARY != cs_type_));
  }
  OB_INLINE bool is_binary() const
  {
    return ((static_cast<uint8_t>(common::ObCharType) == type_)
        && (common::CS_TYPE_BINARY == cs_type_));
  }
  OB_INLINE bool is_ext_sql_array() const
  {
    //in ObObj, scale_ and meta extend flag union in the same structure,
    //for extend type meta, scale_ means extend flag
    return ((static_cast<uint8_t>(common::ObExtendType) == type_)
            && (common::T_EXT_SQL_ARRAY == static_cast<uint8_t>(scale_)));
  }
  OB_INLINE common::ObObjType get_type() const { return type_; }
};

// Expression evaluate result info
struct ObEvalInfo
{
  void clear_evaluated_flag()
  {
    if (evaluated_ || projected_) {
      evaluated_ = 0;
      projected_ = 0;
    }
  }
  DECLARE_TO_STRING;

  inline bool in_frame_notnull() const { return notnull_ && point_to_frame_; }

	union {
		struct {
			// is already evaluated
			uint16_t evaluated_:1;
			// is projected
			uint16_t projected_:1;
			// all datums are not null.
			// may not set even all datums are not null,
      // e.g.: the null values are filtered by skip bitmap.
			uint16_t notnull_:1;
			// pointer is point to reserved buffer in frame.
			uint16_t point_to_frame_:1;
		};
		uint16_t flag_;
	};
	// result count (set to batch_size in batch_eval)
	uint16_t cnt_;
};

// expression evaluate context
struct ObEvalCtx
{
  friend struct ObExpr;
  friend class ObOperator;
  friend class ObSubPlanFilterOp; // FIXME qubin.qb: remove this line from friend
  friend class oceanbase::storage::ObVectorStore;
  friend class ObDatumCaster;
  class TempAllocGuard
  {
  public:
    TempAllocGuard(ObEvalCtx &eval_ctx) : ctx_(eval_ctx), used_flag_(eval_ctx.tmp_alloc_used_)
    {
      alloc_ = &ctx_.get_reset_tmp_alloc();
      ctx_.tmp_alloc_used_ = true;
    }
    ~TempAllocGuard() { ctx_.tmp_alloc_used_ = used_flag_; }
    common::ObArenaAllocator &get_allocator() { return *alloc_; }
  private:
    ObEvalCtx &ctx_;
    common::ObArenaAllocator *alloc_;
    bool used_flag_;
  };
  class BatchInfoScopeGuard
  {
  public:
    explicit BatchInfoScopeGuard(ObEvalCtx &eval_ctx)
    {
      batch_idx_ptr_ = &eval_ctx.batch_idx_;
      batch_size_ptr_ = &eval_ctx.batch_size_;
      batch_idx_default_val_ = eval_ctx.batch_idx_;
      batch_size_default_val_ = eval_ctx.batch_size_;
    }
    ~BatchInfoScopeGuard()
    {
       *batch_idx_ptr_ = batch_idx_default_val_;
       *batch_size_ptr_ = batch_size_default_val_;
    }
    inline void set_batch_idx(int64_t v) { *batch_idx_ptr_ = v; }
    inline void set_batch_size(int64_t v) { *batch_size_ptr_ = v; }

  private:
    int64_t *batch_idx_ptr_ = nullptr;
    int64_t *batch_size_ptr_ = nullptr;
    int64_t batch_idx_default_val_ = 0;
    int64_t batch_size_default_val_ = 0;
  };
  explicit ObEvalCtx(ObExecContext &exec_ctx, ObIAllocator *allocator = NULL);
  explicit ObEvalCtx(ObEvalCtx &eval_ctx);
  virtual ~ObEvalCtx();

  OB_INLINE int64_t get_batch_idx() { return batch_idx_; }
  OB_INLINE int64_t get_batch_size() { return batch_size_; }
  OB_INLINE bool is_vectorized() const { return 0 != max_batch_size_; }
  OB_INLINE ObArenaAllocator &get_expr_res_alloc() { return expr_res_alloc_; }
  OB_INLINE void reuse(const int64_t batch_size)
  {
    batch_idx_ = 0;
    batch_size_ = batch_size;
  }

  OB_INLINE void set_max_batch_size(const int64_t max_batch_size)
  {
    max_batch_size_ = max_batch_size;
  }
  int init_datum_caster();

  TO_STRING_KV(K_(batch_idx), K_(batch_size), K_(max_batch_size), KP(frames_));
private:
  // Allocate expression result memory.
  void *alloc_expr_res(const int64_t size) { return expr_res_alloc_.alloc(size); }

  // set current row evaluating in the batch
  OB_INLINE void set_batch_idx(const int64_t batch_idx)
  {
    // NOTICE: NEVER USE this routine. Use ObEvalCtx::BatchInfoScopeGuard to
    // protect and update batch_idx_. The reason this API is still here is
    // storage layer WON'T use ObEvalCtx::BatchInfoScopeGuard
    batch_idx_ = batch_idx;
  }
  // Note: use set_batch_size() for performance reason.
  // For other path, use ObEvalCtx::BatchInfoScopeGuard instead to protect and
  // update batch_size_.
  OB_INLINE void set_batch_size(const int64_t batch_size)
  {
    batch_size_ = batch_size;
  }
  //change to private interface, please use TempAllocGuard::get_allocator() instead
  common::ObArenaAllocator &get_reset_tmp_alloc()
  {
#ifndef NDEBUG
    if (!tmp_alloc_used_) {
      tmp_alloc_.reset();
    }
#else
    if (!tmp_alloc_used_ && tmp_alloc_.used() > common::OB_MALLOC_MIDDLE_BLOCK_SIZE) {
      tmp_alloc_.reset_remain_one_page();
    }
#endif
    return tmp_alloc_;
  }

public:
  char **frames_;
  // Used for das, the semantics is the same as max_batch_size_ in spec
  int64_t max_batch_size_;
  ObExecContext &exec_ctx_;
  // Temporary allocator for expression evaluating, may be reset immediately after ObExpr::eval().
  // Can not use allocator for expression result. (ObExpr::get_str_res_mem() is used for result).
  common::ObArenaAllocator &tmp_alloc_;
  ObDatumCaster *datum_caster_;
  bool &tmp_alloc_used_;
private:
  int64_t batch_idx_;
  int64_t batch_size_;
  // Expression result allocator, never reset.
  common::ObArenaAllocator &expr_res_alloc_;
};


typedef int (*ObExprHashFuncType)(const common::ObDatum &datum, const uint64_t seed, uint64_t &res);

// batch datum hash functions, %seeds, %hash_values may be the same.
typedef void (*ObBatchDatumHashFunc)(uint64_t *hash_values,
                                     common::ObDatum *datums,
                                     const bool is_batch_datum,
                                     const ObBitVector &skip,
                                     int64_t size,
                                     const uint64_t *seeds,
                                     const bool is_batch_seed);

typedef int (*ObExprCmpFuncType)(const common::ObDatum &datum1, const common::ObDatum &datum2, int& cmp_ret);
struct ObExprBasicFuncs
{
  // Default hash method:
  //    murmur for non string tyeps
  //    mysql string hash for string types
  // Try not to use it unless you need to be compatible with ObObj::hash()/ObObj::varchar_hash(),
  // use murmur_hash_ instead.
  ObExprHashFuncType default_hash_;
  ObBatchDatumHashFunc default_hash_batch_;
  // For murmur/xx/wy functions, the specified hash method is used for all types.
  ObExprHashFuncType murmur_hash_;
  ObBatchDatumHashFunc murmur_hash_batch_;
  ObExprHashFuncType xx_hash_;
  ObBatchDatumHashFunc xx_hash_batch_;
  ObExprHashFuncType wy_hash_;
  ObBatchDatumHashFunc wy_hash_batch_;

  ObExprCmpFuncType null_first_cmp_;
  ObExprCmpFuncType null_last_cmp_;

  /* murmur_hash_v2_ is more efficient than murmur_hash_
     If there is no problem of compatibility, use hash_v2_ is a better choice

     For example, if we calc hash of NUMBER,

      murmur_hash_ calcs like that :
          if (datum.is_null()) {
            const int null_type = ObNullType;
            v = murmurhash64A(&null_type, sizeof(null_type), seed);
          } else {
            uint64_t tmp_v = murmurhash64A(datum.get_number_desc().se_, 1, seed);
            v = murmurhash64A(datum.get_number_digits(), static_cast<uint64_t>(sizeof(uint32_t)* datum.get_number_desc().len_), tmp_v);
          }

      murmur_hash_v2_ calc like that :
          v =  murmurhash64A(datum.ptr_, datum.len_, seed);
  */
  ObExprHashFuncType murmur_hash_v2_;
  ObBatchDatumHashFunc murmur_hash_v2_batch_;
};


struct ObDynReserveBuf
{
  static const uint32_t MAGIC_NUM = 0xD928e5bf;
  static bool supported(const common::ObObjType &type)
  {
    const common::ObObjTypeClass tc = common::ob_obj_type_class(type);
    return common::ObStringTC == tc || common::ObTextTC == tc
           || common::ObRawTC == tc
           || common::ObRowIDTC == tc
           || common::ObLobTC == tc
           || common::ObJsonTC == tc
           || common::ObGeometryTC == tc
           || common::ObUserDefinedSQLTC == tc;
  }

  ObDynReserveBuf() = default;

  uint32_t magic_;
  uint32_t len_;
  char *mem_;
};
static_assert(16 == sizeof(ObDynReserveBuf), "ObDynReserveBuf size can not be changed");


typedef common::ObFixedArray<common::ObString, common::ObIAllocator> ObStrValues;

#define BATCH_EVAL_FUNC_ARG_DECL const ObExpr &expr, ObEvalCtx &ctx, \
  const ObBitVector &skip, const int64_t size
#define BATCH_EVAL_FUNC_ARG_LIST expr, ctx, skip, size

#define EVAL_FUNC_ARG_DECL const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum
#define EVAL_FUNC_ARG_LIST expr, ctx, expr_datum


#ifndef NDEBUG
#define CHECK_STRING_LENGTH(expr, datum)   \
  if (OB_SUCC(ret) && 0 == datum.len_ && !datum.is_null() && is_oracle_mode() &&\
      (ob_is_string_tc(expr.datum_meta_.type_) || ob_is_raw(expr.datum_meta_.type_))) {  \
    SQL_ENG_LOG(ERROR, "unexpected datum length", K(expr)); \
  }
#else
#define CHECK_STRING_LENGTH(expr, datum)
#endif


// default evaluate batch function which call eval() for every datum of batch.
extern int expr_default_eval_batch_func(BATCH_EVAL_FUNC_ARG_DECL);

struct ObExpr
{
  OB_UNIS_VERSION(1);
public:

  const static uint32_t INVALID_EXP_CTX_ID = UINT32_MAX;

  ObExpr();
  OB_INLINE int eval(ObEvalCtx &ctx, common::ObDatum *&datum) const;
  int eval_enumset(ObEvalCtx &ctx,
                   const common::ObIArray<common::ObString> &str_values,
                   const uint64_t cast_mode,
                   common::ObDatum *&datum) const;

  OB_INLINE int eval_batch(ObEvalCtx &ctx,
                           const ObBitVector &skip,
                           const int64_t size) const;

  void reset() { new (this) ObExpr(); }
  ObDatum &locate_expr_datum(ObEvalCtx &ctx) const
  {
    // performance critical, do not check pointer validity.
    return reinterpret_cast<ObDatum *>(ctx.frames_[frame_idx_] + datum_off_)[get_datum_idx(ctx)];
  }

  ObDatum &locate_expr_datum(ObEvalCtx &ctx, const int64_t batch_idx) const
  {
    int64_t idx = batch_idx_mask_ & batch_idx;
    return reinterpret_cast<ObDatum *>(ctx.frames_[frame_idx_] + datum_off_)[idx];
  }

  ObDatumVector locate_expr_datumvector(ObEvalCtx &ctx) const
  {
    ObDatumVector datumsvector;
    datumsvector.set_batch(is_batch_result());
    datumsvector.datums_ = reinterpret_cast<ObDatum *>(ctx.frames_[frame_idx_] + datum_off_);
    return datumsvector;
  }

  ObDatum *locate_batch_datums(ObEvalCtx &ctx) const
  {
    return reinterpret_cast<ObDatum *>(ctx.frames_[frame_idx_] + datum_off_);
  }

  // batch version of locate_param_datum.  Array size is batch_size_ in ObEvalCtx
  OB_INLINE ObDatumVector locate_param_datumvector(ObEvalCtx &ctx, int param_index) const
  {
      return args_[param_index]->locate_expr_datumvector(ctx);
  }

  ObBitVector &get_evaluated_flags(ObEvalCtx &ctx) const
  {
    return *to_bit_vector(ctx.frames_[frame_idx_] + eval_flags_off_);
  }

  ObBitVector &get_pvt_skip(ObEvalCtx &ctx) const
  {
    return *to_bit_vector(ctx.frames_[frame_idx_] + pvt_skip_off_);
  }

  ObEvalInfo &get_eval_info(ObEvalCtx &ctx) const
  {
    return *reinterpret_cast<ObEvalInfo *>(ctx.frames_[frame_idx_] + eval_info_off_);
  }

  OB_INLINE char *get_rev_buf(ObEvalCtx &ctx) const
  {
    return ctx.frames_[frame_idx_] + res_buf_off_;
  }

  // locate expr datum && reset ptr_ to reserved buf
  OB_INLINE ObDatum &locate_datum_for_write(ObEvalCtx &ctx) const;

  // locate batch datums and reset datum ptr_ to reserved buf
  inline ObDatum *locate_datums_for_update(ObEvalCtx &ctx, const int64_t size) const;

  // reset ptr in ObDatum to reserved buf
  OB_INLINE void reset_ptr_in_datum(ObEvalCtx &ctx, const int64_t datum_idx) const;

  OB_INLINE ObDatum &locate_param_datum(ObEvalCtx &ctx, int param_index) const
  {
      return args_[param_index]->locate_expr_datum(ctx);
  }

  // Get result memory for string type.
  // Dynamic allocated memory is allocated if reserved buffer if not enough.
  char *get_str_res_mem(ObEvalCtx &ctx, const int64_t size) const
  {
    return get_str_res_mem(ctx, size, get_datum_idx(ctx));
  }

  char *get_str_res_mem(ObEvalCtx &ctx, const int64_t size, const int64_t datum_idx) const
  {
    return OB_LIKELY(size <= res_buf_len_)
        ? ctx.frames_[frame_idx_] + res_buf_off_ + res_buf_len_ * (batch_idx_mask_ & datum_idx)
        : alloc_str_res_mem(ctx, size, datum_idx);
  }

  void cur_str_resvered_buf(ObEvalCtx &ctx, const int64_t datum_idx, char *&buffer, int64_t &len) const
  {
    buffer = nullptr;
    len = 0;
    int64_t idx = batch_idx_mask_ & datum_idx;
    if (OB_UNLIKELY(!ObDynReserveBuf::supported(datum_meta_.type_))) {
      SQL_ENG_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "unexpected alloc string result memory called", K(*this));
    } else {
      ObDynReserveBuf *drb = reinterpret_cast<ObDynReserveBuf *>(
        ctx.frames_[frame_idx_] + dyn_buf_header_offset_ + sizeof(ObDynReserveBuf) * idx);
      if (OB_LIKELY(drb->len_ >= res_buf_len_)) {
        buffer = drb->mem_;
        len = drb->len_;
      } else {
        len = res_buf_len_;
        buffer = ctx.frames_[frame_idx_] + res_buf_off_ + res_buf_len_ * idx;
      }
    }
  }

  // Get pre allocate result memory buffer, only used for string result
  void cur_str_resvered_buf(ObEvalCtx &ctx, char *&buffer, int64_t &len) const
  {
    cur_str_resvered_buf(ctx, get_datum_idx(ctx), buffer, len);
  }

  bool is_variable_res_buf()
  {
    return OBJ_DATUM_STRING == obj_datum_map_;
  };

  inline bool is_const_expr() const { return is_static_const_ || is_dynamic_const_; }

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
  template <typename ...TS>
  OB_INLINE int eval_param_value(ObEvalCtx &ctx, TS &...args) const;

  // batch version for eval_param_value
  template <typename ...TS>
  OB_INLINE int eval_batch_param_value(ObEvalCtx &ctx, const ObBitVector &skip,
                                       const int64_t size, TS &...args) const;

  OB_INLINE int deep_copy_self_datum(ObEvalCtx &ctx) const;

  // deep copy %datum to reserve buffer or new allocated buffer if reserved buffer is not enough.
  OB_INLINE int deep_copy_datum(ObEvalCtx &ctx, const common::ObDatum &datum) const;

  bool is_batch_result() const { return batch_result_; }
  int64_t get_datum_idx(ObEvalCtx &ctx) const
  {
    return batch_idx_mask_ & ctx.get_batch_idx();
  }

  // mark contian = TRUE if it meets the following rules:
  //   - expr itself equal to input expr
  //   - expr's children equals to input expr
  // Note: NO stack-overflow check as it is done when resolve raw expr
  int contain_expr (const ObExpr * expr, bool & is_contain)
  {
    int ret = OB_SUCCESS;
    if (this == expr) {
      is_contain = true;
    } else {
      if (arg_cnt_ > 0) {
        for (auto i = 0; !is_contain && i < arg_cnt_; i++) {
          if (args_[i] != nullptr) {
            ret = args_[i]->contain_expr(expr, is_contain);
          }
        }
      } else {
        // do nothing
      }
    }
    return ret;
  }
  OB_INLINE void clear_evaluated_flag(ObEvalCtx &ctx)
  {
    if (is_batch_result()) {
      get_evaluated_flags(ctx).unset(ctx.get_batch_idx());
    } else {
      get_eval_info(ctx).clear_evaluated_flag();
    }
  }

  OB_INLINE void set_evaluated_flag(ObEvalCtx &ctx) const
  {
    get_eval_info(ctx).evaluated_ = true;
    if (batch_result_) {
      get_evaluated_flags(ctx).set(ctx.get_batch_idx());
    }
  }

  OB_INLINE void set_evaluated_projected(ObEvalCtx &ctx) const
  {
    get_eval_info(ctx).evaluated_ = true;
    get_eval_info(ctx).projected_ = true;
  }

  typedef common::ObIArray<ObExpr> ObExprIArray;
  OB_INLINE static ObExprIArray *&get_serialize_array()
  {
    RLOCAL_INLINE(ObExprIArray *, g_expr_ser_array);
    return g_expr_ser_array;
  }

  TO_STRING_KV("type", get_type_name(type_),
              K_(datum_meta),
              K_(obj_meta),
              K_(obj_datum_map),
              K_(flag),
              KP_(eval_func),
              KP_(eval_batch_func),
              KP_(inner_functions),
              K_(inner_func_cnt),
              K_(arg_cnt),
              K_(parent_cnt),
              K_(frame_idx),
              K_(datum_off),
              K_(res_buf_off),
              K_(dyn_buf_header_offset),
              K_(res_buf_len),
              K_(eval_flags_off),
              K_(pvt_skip_off),
              K_(expr_ctx_id),
              K_(extra),
              K_(batch_idx_mask),
              KP(this));

private:
  char *alloc_str_res_mem(ObEvalCtx &ctx, const int64_t size, const int64_t idx) const;
  // reset datums pointer to reserved buffer.
  void reset_datums_ptr(char *frame, const int64_t size) const;
  void reset_datum_ptr(char *frame, const int64_t size, const int64_t idx) const;
  int eval_one_datum_of_batch(ObEvalCtx &ctx, common::ObDatum *&datum) const;
  int do_eval_batch(ObEvalCtx &ctx, const ObBitVector &skip, const int64_t size) const;


public:
  typedef int (*EvalFunc) (const ObExpr &expr,
                           ObEvalCtx &ctx,
                           ObDatum &expr_datum);
  typedef int (*EvalBatchFunc) (const ObExpr &expr,
                                ObEvalCtx &ctx,
                                const ObBitVector &skip,
                                const int64_t size);
  typedef int (*EvalEnumSetFunc) (const ObExpr &expr,
                                  const common::ObIArray<common::ObString> &str_values,
                                  const uint64_t cast_mode,
                                  ObEvalCtx &ctx,
                                  ObDatum &expr_datum);

  const static uint64_t MAGIC_NUM = 0x6367614D72707845L; // string of "ExprMagc"
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

  union {
    struct {
      uint64_t batch_result_:1;
      uint64_t is_called_in_sql_:1;
      uint64_t is_static_const_:1; // is const during the whole execution
      uint64_t is_boolean_:1; // to distinguish result of this expr between and int tc
      uint64_t is_dynamic_const_:1; // is const during the subplan execution, including exec param
      uint64_t need_stack_check_:1; // the expression tree depth needs to check whether the stack overflows
    };
    uint64_t flag_;
  };
  // expr evaluate function
  union {
    EvalFunc eval_func_;
    // helper union member for eval_func_ serialize && deserialize
    sql::serializable_function ser_eval_func_;
  };
  union {
    EvalBatchFunc eval_batch_func_;
    sql::ser_eval_batch_function ser_eval_batch_func_;
  };
  // aux evaluate functions for eval_func_, array of any function pointers, which interpreted
  // by eval_func_.
  // mysql row operand use the inner function array
  // to compare condition pairs. e.g.:
  //     (a, b, c) = (1, 2, 3)
  //     arg_cnt_ is 6
  //     inner_func_cnt_ is 3
  union {
    void **inner_functions_;
    // helper member for inner_functions_ serialize && deserialize
    sql::serializable_function *ser_inner_functions_;
  };
  uint32_t inner_func_cnt_;
  ObExpr **args_;
  uint32_t arg_cnt_;
  ObExpr **parents_;
  uint32_t parent_cnt_;
  // frame index
  uint32_t frame_idx_;
  // offset of ObDatum in frame
  uint32_t datum_off_;
  // offset of ObEvalInfo
  uint32_t eval_info_off_;
  // Dynamic buf header offset
  uint32_t dyn_buf_header_offset_;
  // reserve buffer offset in frame
  uint32_t res_buf_off_;
  // reserve buffer length for each datum
  uint32_t res_buf_len_;
  // evaluated flags for batch
  uint32_t eval_flags_off_;
  // private skip bit vector
  uint32_t pvt_skip_off_;
  // expr context id
  uint32_t expr_ctx_id_;
  // extra info, reinterpreted by each expr
  union {
    uint64_t extra_;
    struct {
      int64_t div_calc_scale_          :   16;
      int64_t is_error_div_by_zero_    :    1;
      int64_t reserved_                :   47;
    };
  };
  ObExprBasicFuncs *basic_funcs_;
  uint64_t batch_idx_mask_;
  ObIExprExtraInfo *extra_info_;
};

// helper template to access ObExpr::extra_
template <typename T>
struct ObExprExtraInfoAccess
{
  static T &get_info(int64_t &v) { return *reinterpret_cast<T *>(&v); }
  static const T &get_info(const int64_t &v) { return *reinterpret_cast<const T*>(&v); }
  static T &get_info(ObExpr &e) { return get_info(*reinterpret_cast<int64_t *>(&e.extra_)); }
  static const T &get_info(const ObExpr &e)
  { return get_info(*reinterpret_cast<const int64_t *>(&e.extra_)); }
};

// Wrap expression string result buffer allocation to allocator interface.
// Please try not to use this, if you mast use it, make sure it only allocate one time and is
// for expression result.
class ObExprStrResAlloc : public common::ObIAllocator
{
public:
  ObExprStrResAlloc(const ObExpr &expr, ObEvalCtx &ctx) : off_(0), expr_(expr), ctx_(ctx)
  {
  }

  void *alloc(const int64_t size) override;
  void* alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override { UNUSED(ptr); }
private:
  int64_t off_;
  const ObExpr &expr_;
  ObEvalCtx &ctx_;
};

struct ObDatumObj
{
public:
  void set_scale(common::ObScale scale) {
    meta_.scale_ = scale;
  }
  TO_STRING_KV(K_(meta));

public:
  ObDatumMeta meta_;
  common::ObDatum datum_;
};

typedef common::ObIArray<ObExpr *> ObExprPtrIArray;
typedef common::ObFixedArray<ObExpr *, common::ObIAllocator> ExprFixedArray;

// copy from ObObjParam
struct ObDatumObjParam : public ObDatumObj
{
  ObDatumObjParam() : ObDatumObj(), accuracy_(), res_flags_(0), flag_()
  {
  }
  ObDatumObjParam(const ObDatumObj &other)
    : ObDatumObj(other), accuracy_(), res_flags_(0), flag_()
  {
  }

  TO_STRING_KV(K_(accuracy), K_(res_flags), K_(datum), K_(meta));
public:
  int from_objparam(const common::ObObjParam &objparam, common::ObIAllocator *allocator = nullptr);

  int to_objparam(common::ObObjParam &obj_param, common::ObIAllocator *allocator = nullptr);
  int construct_array_param_datum(const common::ObObjParam &obj_param, common::ObIAllocator &alloc);
  int construct_sql_array_obj(common::ObObjParam &obj_param, common::ObIAllocator &allocator);
  template<typename T>
  int alloc_datum_reserved_buff(const T &obj_meta, common::ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    common::ObObjType real_type = obj_meta.is_ext_sql_array() ? common::ObIntType : obj_meta.get_type();
    common::ObObjDatumMapType obj_datum_map = common::ObDatum::get_obj_datum_map_type(real_type);
    if (OB_LIKELY(common::OBJ_DATUM_NULL != obj_datum_map)) {
      uint32_t def_res_len = common::ObDatum::get_reserved_size(obj_datum_map);
      if (OB_ISNULL(datum_.ptr_ = static_cast<char *>(allocator.alloc(def_res_len)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "fail to alloc memory", K(def_res_len), K(ret));
      }
    }
    return ret;
  }
  void set_sql_array_datum(const ObSqlDatumArray *datum_array)
  {
    //you can't use datum_.extend_obj_ here,
    //datum_.extend_obj_m is just to asign an ObObj pointer
    //here you need to set the address of the constructed datum array
    datum_.set_int(reinterpret_cast<int64_t>(datum_array));
  }
  const ObSqlDatumArray *get_sql_datum_array() const
  {
    ObSqlDatumArray *datum_array = nullptr;
    if (meta_.is_ext_sql_array()) {
      //you can't call get_ext() here, get_ext() in Datum is to get an ObObj pointer
      //here you need to get the address of the constructed datum array
      int64_t array_addr = datum_.get_int();
      datum_array = reinterpret_cast<ObSqlDatumArray*>(array_addr);
    }
    return datum_array;
  }
  ObSqlDatumArray *get_sql_datum_array()
  {
    ObSqlDatumArray *datum_array = nullptr;
    if (meta_.is_ext_sql_array()) {
      //you can't call get_ext() here, get_ext() in Datum is to get an ObObj pointer
      //here you need to get the address of the constructed datum array
      int64_t array_addr = datum_.get_int();
      datum_array = reinterpret_cast<ObSqlDatumArray*>(array_addr);
    }
    return datum_array;
  }

  OB_INLINE void set_datum(const common::ObDatum &datum)
  {
    datum_ = datum;
  }

  OB_INLINE void set_meta(const ObDatumMeta &meta)
  {
    accuracy_.scale_ = meta.scale_;
    accuracy_.precision_ = meta.precision_;
    meta_ = meta;
  }
  // accuracy.
  OB_INLINE void set_accuracy(const common::ObAccuracy &accuracy) { accuracy_.set_accuracy(accuracy); }
  OB_INLINE void set_length(common::ObLength length) { accuracy_.set_length(length); }
  OB_INLINE void set_precision(common::ObPrecision precision) { accuracy_.set_precision(precision); }
  OB_INLINE void set_length_semantics(common::ObLengthSemantics length_semantics) { accuracy_.set_length_semantics(length_semantics); }
  OB_INLINE void set_scale(common::ObScale scale) {
    ObDatumObj::set_scale(scale);
    accuracy_.set_scale(scale);
  }
  OB_INLINE void set_udt_id(uint64_t id) {
    accuracy_.set_accuracy(id);
  }
  OB_INLINE const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  OB_INLINE common::ObLength get_length() const { return accuracy_.get_length(); }
  OB_INLINE common::ObPrecision get_precision() const { return accuracy_.get_precision(); }
  OB_INLINE common::ObScale get_scale() const { return accuracy_.get_scale(); }
  OB_INLINE uint64_t get_udt_id() const {
    return meta_.type_ == static_cast<uint8_t>(common::ObExtendType)
           ? accuracy_.get_accuracy()
           : common::OB_INVALID_INDEX;
  }
  OB_INLINE void set_result_flag(uint32_t flag) { res_flags_ |= flag; }
  OB_INLINE void unset_result_flag(uint32_t flag) { res_flags_ &= (~flag); }
  OB_INLINE bool has_result_flag(uint32_t flag) const { return res_flags_ & flag; }
  OB_INLINE uint32_t get_result_flag() const { return res_flags_; }

  OB_INLINE const common::ParamFlag &get_param_flag() const { return flag_; }
  OB_INLINE void set_need_to_check_type(bool flag) { flag_.need_to_check_type_ = flag; }
  OB_INLINE bool need_to_check_type() const { return flag_.need_to_check_type_; }

  OB_INLINE void set_need_to_check_bool_value(bool flag) { flag_.need_to_check_bool_value_ = flag; }
  OB_INLINE bool need_to_check_bool_value() const { return flag_.need_to_check_bool_value_; }

  OB_INLINE void set_expected_bool_value(bool b_value) { flag_.expected_bool_value_ = b_value; }
  OB_INLINE bool expected_bool_value() const { return flag_.expected_bool_value_; }

  OB_INLINE bool is_nop_value() const { return false; }

  NEED_SERIALIZE_AND_DESERIALIZE;

  static uint32_t accuracy_offset_bits() { return offsetof(ObDatumObjParam, accuracy_) * 8; }
  static uint32_t res_flags_offset_bits() { return offsetof(ObDatumObjParam, res_flags_) * 8; }
  static uint32_t flag_offset_bits() { return offsetof(ObDatumObjParam, flag_) * 8; }

private:
  common::ObAccuracy accuracy_;
  uint32_t res_flags_;  // BINARY, NUM, NOT_NULL, TIMESTAMP, etc
                        // reference: src/lib/regex/include/mysql_com.h
  common::ParamFlag flag_;
};

typedef common::Ob2DArray<ObDatumObjParam,
                          common::OB_MALLOC_BIG_BLOCK_SIZE,
                          common::ObWrapperAllocator,
                          false,
                          common::ObSEArray<ObDatumObjParam *, 1, common::ObWrapperAllocator, false>
                          > DatumParamStore;

// Helper function for print datum which interpreted by expression
// Can only be used in log message like this:
//    LOG_WARN(....., "datum", DATUM2STR(*expr, *datum))
struct ObToStringDatum
{
  ObToStringDatum(const ObExpr &e, const common::ObDatum &d) : e_(e), d_(d) {}

  DECLARE_TO_STRING;

  const ObExpr &e_;
  const common::ObDatum  &d_;
};
typedef ObToStringDatum DATUM2STR;

// Helper function for expression evaluate && print result
// Can only be used in log message like this:
//    LOG_WARN(....., "datum", EXPR2STR(eval_ctx_, *expr))
//
// 注意: 在ObExpr的eval_func_定义中不要使用该封装打印表达式值,
// 因为实现的eval_func中, 打印日志时, 如果调用ObToStringExpr(ctx, expr),
// 该函数又会调用eval_func计算, 不断循环调用, 且一直没走到设置evaluated_
// 标记为true的逻辑, 最终会导致爆栈;
// bug:
struct ObToStringExpr
{
  ObToStringExpr(ObEvalCtx &ctx, const ObExpr &e) : c_(ctx), e_(e) {}

  DECLARE_TO_STRING;

  ObEvalCtx &c_;
  const ObExpr &e_;
};
typedef ObToStringExpr EXPR2STR;

// Helper function for expression array evaluate && print result
// Can only be used in log message like this:
//    LOG_WARN(......, "row", ROWEXPR2STR(eval_ctx_, output_)
struct ObToStringExprRow
{
  ObToStringExprRow(ObEvalCtx &ctx, const common::ObIArray<ObExpr *> &exprs)
      : c_(ctx), exprs_(exprs) { }

  DECLARE_TO_STRING;

  ObEvalCtx &c_;
  const common::ObIArray<ObExpr *> &exprs_;
};
typedef ObToStringExprRow ROWEXPR2STR;

OB_INLINE ObDatum &ObExpr::locate_datum_for_write(ObEvalCtx &ctx) const
{
  // performance critical, do not check pointer validity.
	char *frame = ctx.frames_[frame_idx_];
  OB_ASSERT(NULL != frame);
  const int64_t idx = get_datum_idx(ctx);
  ObDatum *expr_datum = reinterpret_cast<ObDatum *>(frame + datum_off_) + idx;
  char *data_pos = frame + res_buf_off_ + res_buf_len_ * idx;
  if (expr_datum->ptr_ != data_pos) {
    expr_datum->ptr_ = data_pos;
  }
  return *expr_datum;
}

inline ObDatum *ObExpr::locate_datums_for_update(ObEvalCtx &ctx,
                                                 const int64_t size) const
{
  char *frame = ctx.frames_[frame_idx_];
  OB_ASSERT(NULL != frame);
  ObDatum *datums = reinterpret_cast<ObDatum *>(frame + datum_off_);
  char *ptr = frame + res_buf_off_;
  ObDatum *d = datums;
  for (int64_t i = 0; i < size; i++) {
    if (d->ptr_ != ptr) {
      d->ptr_ = ptr;
    }
    ptr += res_buf_len_;
    d++;
  }
  return datums;
}

OB_INLINE void ObExpr::reset_ptr_in_datum(ObEvalCtx &ctx, const int64_t datum_idx) const
{
  OB_ASSERT(datum_idx >= 0);
  char *frame = ctx.frames_[frame_idx_];
  OB_ASSERT(NULL != frame);
  ObDatum *expr_datum = reinterpret_cast<ObDatum *>(frame + datum_off_) + datum_idx;
  char *data_pos = frame + res_buf_off_ + res_buf_len_ * datum_idx;
  expr_datum->ptr_ = data_pos;
}

template <typename ...TS>
OB_INLINE int ObExpr::eval_param_value(ObEvalCtx &ctx, TS &...args) const
{
  int ret = common::OB_SUCCESS;
  common::ObDatum **params[] = { &args... };
  common::ObDatum *tmp = NULL;
  for (int param_index = 0; OB_SUCC(ret) && param_index < arg_cnt_; param_index++) {
_Pragma("GCC diagnostic push")
_Pragma("GCC diagnostic ignored \"-Warray-bounds\"")
    if (OB_FAIL(args_[param_index]->eval(
                ctx, param_index < ARRAYSIZEOF(params) ? *params[param_index] : tmp))) {
      SQL_LOG(WARN, "evaluate parameter failed", K(ret), K(param_index));
    }
_Pragma("GCC diagnostic pop")
  }
  return ret;
}

template <typename ...TS>
OB_INLINE int ObExpr::eval_batch_param_value(ObEvalCtx &ctx, const ObBitVector &skip,
                                       const int64_t size, TS &...args) const
{
  int ret = common::OB_SUCCESS;
  ObDatumVector *params[] = { &args...};
  for (int param_index = 0; OB_SUCC(ret) && param_index < arg_cnt_; param_index++) {
    if (OB_FAIL(args_[param_index]->eval_batch(ctx, skip, size))) {
      SQL_LOG(WARN, "evaluate parameter failed", K(ret), K(param_index));
    } else if (param_index < ARRAYSIZEOF(params)) {
      *params[param_index] = locate_param_datumvector(ctx, param_index);
    }
  }
  return ret;
}


OB_INLINE int ObExpr::eval(ObEvalCtx &ctx, common::ObDatum *&datum) const
{
  // performance critical, do not check %frame_idx_ and %frame again. (checked in CG)
	int ret = common::OB_SUCCESS;
	char *frame = ctx.frames_[frame_idx_];
  OB_ASSERT(NULL != frame);
	datum = (ObDatum *)(frame + datum_off_);
  ObEvalInfo *eval_info = (ObEvalInfo *)(frame + eval_info_off_);
  if (is_batch_result()) {
    if (NULL == eval_func_ || eval_info->projected_) {
      datum = datum + ctx.get_batch_idx();
    } else {
      ret = eval_one_datum_of_batch(ctx, datum);
    }
  } else if (NULL != eval_func_ && !eval_info->evaluated_) {
	// do nothing for const/column reference expr or already evaluated expr
    if (OB_UNLIKELY(need_stack_check_) && OB_FAIL(check_stack_overflow())) {
      SQL_LOG(WARN, "failed to check stack overflow", K(ret));
    } else {
      if (datum->ptr_ != frame + res_buf_off_) {
        datum->ptr_ = frame + res_buf_off_;
      }
      ret = eval_func_(*this, ctx, *datum);
      CHECK_STRING_LENGTH((*this), (*datum));
      if (OB_LIKELY(common::OB_SUCCESS == ret)) {
        eval_info->evaluated_ = true;
      } else {
        datum->set_null();
      }
    }
	}
	return ret;
}

OB_INLINE int ObExpr::eval_batch(ObEvalCtx &ctx,
                                 const ObBitVector &skip,
                                 const int64_t size) const
{
  int ret = common::OB_SUCCESS;
  const ObEvalInfo &info = get_eval_info(ctx);
  if (!is_batch_result()) {
    if (skip.accumulate_bit_cnt(size) < size) {
      common::ObDatum *datum = NULL;
      ret = eval(ctx, datum);
    } else {
      // all skiped
    }
  } else if (info.projected_ || NULL == eval_batch_func_) {
    // expr values is projected by child or has no evaluate func, do nothing.
  } else if (size > 0) {
    ret = do_eval_batch(ctx, skip, size);
  }
  return ret;
}

OB_INLINE int ObExpr::deep_copy_self_datum(ObEvalCtx &ctx) const
{
  int ret = OB_SUCCESS;
  const ObDatum &datum = locate_expr_datum(ctx);
  if (OB_FAIL(deep_copy_datum(ctx, datum))) {
    SQL_LOG(WARN, "fail to deep copy datum", K(ret), K(ctx), K(datum));
  }

  return ret;
}

OB_INLINE int ObExpr::deep_copy_datum(ObEvalCtx &ctx, const common::ObDatum &datum) const
{
  int ret = common::OB_SUCCESS;
  // shadow copy datum first, because %datum may overlay with %dst
  common::ObDatum src = datum;
  ObDatum &dst = this->locate_datum_for_write(ctx);
  dst.pack_ = src.pack_;
  if (!src.null_) {
    if (OB_UNLIKELY(src.len_ > res_buf_len_)) {
      // only string datum may exceed %res_buf_len_;
      if (OB_ISNULL(dst.ptr_ = get_str_res_mem(ctx, src.len_))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        MEMMOVE(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
      }
    } else {
      MEMMOVE(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
    }
  }
  return ret;
}

inline const char *get_vectorized_row_str(ObEvalCtx &eval_ctx,
                                          const ObExprPtrIArray &exprs,
                                          int64_t index)
{
  char *buffer = NULL;
  int64_t str_len = 0;
  CStringBufMgr &mgr = CStringBufMgr::get_thread_local_instance();
  mgr.inc_level();
  const int64_t buf_len = mgr.acquire(buffer);
  if (OB_ISNULL(buffer)) {
    LIB_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "buffer is NULL");
  } else {
    databuff_printf(buffer, buf_len, str_len, "vectorized_rows(%ld)=", index);
    str_len += to_string(ROWEXPR2STR(eval_ctx, exprs), buffer + str_len, buf_len - str_len - 1);
    if (str_len >= 0 && str_len < buf_len) {
      buffer[str_len] = '\0';
    } else {
      buffer[0] = '\0';
    }
    mgr.update_position(str_len + 1);
  }
  mgr.try_clear_list();
  mgr.dec_level();
  return buffer;
}

#define PRINT_VECTORIZED_ROWS(parMod, level, eval_ctx, exprs, batch_size, args...)       \
    do { if (IS_LOG_ENABLED(level)) {                                                            \
    [&](const char *_fun_name_) __attribute__((GET_LOG_FUNC_ATTR(level))) {                      \
    if (OB_UNLIKELY(OB_LOGGER.need_to_print(::oceanbase::common::OB_LOG_ROOT::M_##parMod,        \
                                            OB_LOG_LEVEL_##level)))                              \
    {                                                                                            \
      int64_t _batch_size = common::min(batch_size, (eval_ctx).get_batch_size());                \
      ObEvalCtx::BatchInfoScopeGuard _batch_info_guard(eval_ctx);                                \
      _batch_info_guard.set_batch_size(_batch_size);                                             \
      for (int64_t i = 0; i < _batch_size; ++i) {                                                \
        _batch_info_guard.set_batch_idx(i);                                                      \
        ::oceanbase::common::OB_PRINT("["#parMod"] ", OB_LOG_LEVEL(level),                       \
                                      get_vectorized_row_str(eval_ctx, exprs, i),                \
                                      LOG_KVS(args)); }                                          \
      }                                                                                          \
    }(__FUNCTION__); } } while (false)
} // end namespace sql
namespace common
{
namespace serialization
{

// ObExpr pointer serialize && deserialize.
// Convert pointer to index of expr array, serialize && deserialize the index.
// The serialized index is the real index + 1, to make room for NULL.
inline int64_t encoded_length(sql::ObExpr *)
{
  return sizeof(uint32_t);
}

inline int encode(char *buf, const int64_t buf_len, int64_t &pos, sql::ObExpr *expr)
{
  int ret = common::OB_SUCCESS;
  uint32_t idx = 0;
  if (NULL != expr) {
    sql::ObExpr::ObExprIArray *array = sql::ObExpr::get_serialize_array();
    if (OB_UNLIKELY(NULL == array || array->empty() || expr < &array->at(0)
                    || (idx = static_cast<uint32_t>(expr - &array->at(0) + 1)) > array->count())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "expr not in array", K(ret), KP(array), KP(idx), KP(expr));
    }
  }
  if (OB_SUCC(ret)) {
    ret = encode_i32(buf, buf_len, pos, idx);
  }
  return ret;
}

inline int decode(const char *buf, const int64_t data_len, int64_t &pos, sql::ObExpr *&expr)
{
  int ret = common::OB_SUCCESS;
  uint32_t idx = 0;
  ret = decode_i32(buf, data_len, pos, reinterpret_cast<int32_t *>(&idx));
  if (OB_SUCC(ret)) {
    if (0 == idx) {
      expr = NULL;
    } else {
      sql::ObExpr::ObExprIArray *array = sql::ObExpr::get_serialize_array();
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
struct EnumEncoder<false, sql::ObExpr *>
{
  static int encode(char *buf, const int64_t buf_len, int64_t &pos, sql::ObExpr *expr)
  {
    return serialization::encode(buf, buf_len, pos, expr);
  }

  static int decode(const char *buf, const int64_t data_len, int64_t &pos, sql::ObExpr *&expr)
  {
    return serialization::decode(buf, data_len, pos, expr);
  }

  static int64_t encoded_length(sql::ObExpr *expr)
  {
    return serialization::encoded_length(expr);
  }
};


//
// Simple c array wrapper for serialize && deserialize. e.g.:
//  OB_SERIALIZE_MEMBER(Foo, make_ser_carray(items_, item_cnt_))
//
template <typename T, typename U>
struct ObSerCArray
{
  ObSerCArray(T &data, U &cnt) : data_(data), cnt_(cnt) {}
  T &data_;
  U &cnt_;
};

template <typename T, typename U>
ObSerCArray<T, U> make_ser_carray(T &data, U &cnt) { return ObSerCArray<T, U>(data, cnt); }


template <typename T, typename U>
inline int64_t encoded_length(const ObSerCArray<T *const, U> &array)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(array.data_, array.cnt_);
  return len;
}

template <typename T, typename U>
inline int encode(char *buf, const int64_t buf_len, int64_t &pos,
                  const ObSerCArray<T *const, U> &array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(array.cnt_ > 0 && NULL == array.data_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "array not empty but data is NULL",
            K(ret), KP(array.data_), K(array.cnt_));
  } else {
    OB_UNIS_ENCODE_ARRAY(array.data_, array.cnt_);
  }
  return ret;
}

template <typename T, typename U>
inline int decode(const char *buf, const int64_t data_len, int64_t &pos,
                  const ObSerCArray<T *, U> &array)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(array.cnt_);
  if (OB_SUCC(ret)) {
    if (0 == array.cnt_) {
      array.data_ = NULL;
    } else {
      const int64_t alloc_size = sizeof(*array.data_) * array.cnt_;
      array.data_ = static_cast<T *>(
          CURRENT_CONTEXT->get_arena_allocator().alloc(alloc_size));
      if (OB_ISNULL(array.data_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "alloc memory failed", K(ret), K(alloc_size));
      } else {
        int64_t idx = 0;
        for ( ; OB_SUCC(ret) && idx < array.cnt_; idx++) {
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
} // end namespace serialization
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_EXPR_OB_EXPR_H_
