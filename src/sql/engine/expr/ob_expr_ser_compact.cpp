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
 *
 * Compact serialize opt record codecs and receiver frame-layout replay.
 * Request contexts keep expression reference encoding consistent across records.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_ser_compact.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"

#include "sql/engine/expr/ob_expr_extra_info_factory.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/das/ob_das_define.h"
#include "sql/engine/ob_ser_compact_bindings.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int encode_das_expr_frame_head(char *buf, int64_t buf_len, int64_t &pos, ObExecContext &ctx,
                               const ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(expr_frame_info.need_ctx_cnt_);
  // expr extra info
  ObExprExtraSerializeInfo expr_info;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  expr_info.current_time_ = &plan_ctx->get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx->get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx->get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx->get_last_refresh_scns();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_info.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize expr extra info", K(ret));
  }
  return ret;
}

int decode_das_expr_frame_head(const char *buf, int64_t data_len, int64_t &pos, ObExecContext &ctx,
                               const ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  int64_t need_ctx_cnt = 0;
  OB_UNIS_DECODE(need_ctx_cnt);
  const_cast<ObExprFrameInfo &>(expr_frame_info).need_ctx_cnt_ = need_ctx_cnt;
  ObExprExtraSerializeInfo expr_info;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  expr_info.current_time_ = &plan_ctx->get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx->get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx->get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx->get_last_refresh_scns();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_info.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize expr extra info", K(ret));
  }
  return ret;
}

int64_t das_expr_frame_head_size(ObExecContext &ctx, const ObExprFrameInfo &expr_frame_info)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(expr_frame_info.need_ctx_cnt_);
  ObExprExtraSerializeInfo expr_info;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  expr_info.current_time_ = &plan_ctx->get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx->get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx->get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx->get_last_refresh_scns();
  len += expr_info.get_serialize_size();
  return len;
}

// File-local helpers live in a unique named namespace (not an anonymous one):
// this repository is built with unity build, and anonymous-namespace symbols
// of this file would share the blob with other translation units.
namespace expr_ser_compact_detail
{


int encode_func_idx_vi64(char *buf, const int64_t buf_len, int64_t &pos, const void *func)
{
  int ret = OB_SUCCESS;
  const uint64_t idx = ObFuncSerialization::get_serialize_index(const_cast<void *>(func));
  if (OB_UNLIKELY(common::OB_INVALID_INDEX == idx)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "function not serializable", K(ret), KP(func));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(idx)))) {
    SQL_LOG(WARN, "encode func idx failed", K(ret));
  }
  return ret;
}

// Decode one eval function index, mirroring the legacy decode semantics:
// eval func is strict (unknown index is an error), batch/vector funcs fall
// back to the default func when the index is not registered.
enum V4FuncKind
{
  V4_FUNC_EVAL = 0,
  V4_FUNC_BATCH,
  V4_FUNC_VECTOR
};

int decode_func_idx_vi64(const char *buf, const int64_t data_len, int64_t &pos, void *&func, const V4FuncKind kind)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  func = NULL;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &idx))) {
    SQL_LOG(WARN, "decode func idx failed", K(ret));
  } else if (0 == idx) {
    func = NULL;
  } else if (OB_ISNULL(func = ObFuncSerialization::get_serialize_func(static_cast<uint64_t>(idx)))) {
    if (V4_FUNC_EVAL == kind) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "eval function not found by idx", K(ret), K(idx));
    } else if (V4_FUNC_BATCH == kind) {
      // explicit cast: a second overload defined in ob_expr.cpp is visible in
      // this unity blob and would make the bare name ambiguous
      func = reinterpret_cast<void *>(static_cast<ObExpr::EvalBatchFunc>(sql::expr_default_eval_batch_func));
    } else {
      func = reinterpret_cast<void *>(static_cast<ObExpr::EvalVectorFunc>(sql::expr_default_eval_vector_func));
    }
  }
  return ret;
}

struct V4ExprRefDecoder
{
  V4ExprRefDecoder(const char *buf, const int64_t data_len, int64_t &pos) : buf_(buf), data_len_(data_len), pos_(pos)
  {}

  int operator()(ObExpr *&ref) const
  {
    return serialization::decode(buf_, data_len_, pos_, ref);
  }

  const char *buf_;
  const int64_t data_len_;
  int64_t &pos_;
};

struct V4InnerFuncDecoder
{
  V4InnerFuncDecoder(const char *buf, const int64_t data_len, int64_t &pos) : buf_(buf), data_len_(data_len), pos_(pos)
  {}

  int operator()(void *&func) const
  {
    int ret = decode_func_idx_vi64(buf_, data_len_, pos_, func, V4_FUNC_EVAL);
    if (OB_SUCC(ret) && OB_ISNULL(func)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "inner function not found", K(ret));
    }
    return ret;
  }

  const char *buf_;
  const int64_t data_len_;
  int64_t &pos_;
};

template <typename Item, typename ItemDecoder>
int decode_v4_pointer_array(const char *buf, const int64_t data_len, int64_t &pos, Item *&items, uint32_t &item_cnt,
                            ItemDecoder decode_item)
{
  int ret = OB_SUCCESS;
  int32_t cnt = 0;
  Item *decoded_items = NULL;
  if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &cnt))) {
    SQL_LOG(WARN, "decode v4 pointer array count failed", K(ret), K(pos), K(data_len));
  } else if (OB_UNLIKELY(cnt < 0 || cnt > data_len - pos)) {
    ret = OB_DESERIALIZE_ERROR;
    SQL_LOG(WARN, "invalid v4 pointer array count", K(ret), K(cnt), K(sizeof(Item)));
  } else if (cnt > 0) {
    const int64_t alloc_size = sizeof(Item) * static_cast<int64_t>(cnt);
    void *mem = CURRENT_CONTEXT->get_arena_allocator().alloc(alloc_size);
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate v4 pointer array failed", K(ret), K(cnt), K(alloc_size));
    } else {
      decoded_items = static_cast<Item *>(mem);
      MEMSET(decoded_items, 0, alloc_size);
      for (int32_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
        if (OB_FAIL(decode_item(decoded_items[i]))) {
          SQL_LOG(WARN, "decode v4 pointer array item failed", K(ret), K(i), K(cnt));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    items = decoded_items;
    item_cnt = static_cast<uint32_t>(cnt);
  }
  return ret;
}

int64_t expr_ref_array_len(ObExpr *const *exprs, const uint32_t cnt)
{
  // Uses the compact request encoding context.
  int64_t len = serialization::encoded_length_vi32(static_cast<int32_t>(cnt));
  for (uint32_t i = 0; i < cnt; ++i) { len += serialization::encoded_length(exprs[i]); }
  return len;
}

int64_t func_idx_array_vi64_len(const sql::serializable_function *funcs, const uint32_t cnt)
{
  int64_t len = serialization::encoded_length_vi32(static_cast<int32_t>(cnt));
  for (uint32_t i = 0; i < cnt; ++i) {
    const uint64_t idx = ObFuncSerialization::get_serialize_index(reinterpret_cast<void *>(funcs[i]));
    len += serialization::encoded_length_vi64(static_cast<int64_t>(common::OB_INVALID_INDEX == idx ? 0 : idx));
  }
  return len;
}

enum ObDatumMetaCompactFamily : int8_t
{
  V4_META_DERIVED = 0,
  V4_META_PRECISION_SCALE,
  V4_META_STRING,
  V4_META_FULL,
};

// The compact family is a pure function of datum type, so no form discriminator is
// needed on wire. Keep this a strict whitelist: every special/new type falls
// back to the complete ObDatumMeta envelope.
ObDatumMetaCompactFamily datum_meta_compact_family(const ObObjType type)
{
  ObDatumMetaCompactFamily family = V4_META_FULL;
  if (ob_is_int_uint_tc(type) || ObYearType == type || ObDateType == type) {
    family = V4_META_DERIVED;
  } else if (ob_is_float_tc(type) || ob_is_double_tc(type) || ob_is_number_tc(type) || ob_is_decimal_int_tc(type)
             || ObDateTimeType == type || ObTimestampType == type || ObTimeType == type) {
    family = V4_META_PRECISION_SCALE;
  } else if (ObVarcharType == type || ObCharType == type) {
    family = V4_META_STRING;
  }
  return family;
}

int64_t datum_meta_compact_size(const ObDatumMeta &meta)
{
  int64_t len = serialization::encoded_length_vi32(static_cast<int32_t>(meta.type_));
  switch (datum_meta_compact_family(meta.type_)) {
  case V4_META_DERIVED: len += serialization::encoded_length_vi32(static_cast<int32_t>(meta.precision_)); break;
  case V4_META_PRECISION_SCALE:
    len += serialization::encoded_length_vi32(static_cast<int32_t>(meta.precision_));
    len += serialization::encoded_length_i8(meta.scale_);
    break;
  case V4_META_STRING:
    len += serialization::encoded_length_vi32(static_cast<int32_t>(meta.cs_type_));
    len += serialization::encoded_length_vi32(static_cast<int32_t>(meta.precision_));
    // cg_expr_basic() preserves the raw result scale for Varchar/Char.
    len += serialization::encoded_length_i8(meta.scale_);
    break;
  case V4_META_FULL: len += meta.get_serialize_size(); break;
  default: break;
  }
  return len;
}

int encode_datum_meta_compact(const ObDatumMeta &meta, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(static_cast<int32_t>(meta.type_));
  switch (datum_meta_compact_family(meta.type_)) {
  case V4_META_DERIVED:
    OB_UNIS_ENCODE(static_cast<int32_t>(meta.precision_));
    break;
  case V4_META_PRECISION_SCALE:
    OB_UNIS_ENCODE(static_cast<int32_t>(meta.precision_));
    OB_UNIS_ENCODE(meta.scale_);
    break;
  case V4_META_STRING:
    OB_UNIS_ENCODE(static_cast<int32_t>(meta.cs_type_));
    OB_UNIS_ENCODE(static_cast<int32_t>(meta.precision_));
    OB_UNIS_ENCODE(meta.scale_);
    break;
  case V4_META_FULL:
    OB_UNIS_ENCODE(meta);
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected datum meta compact family", K(ret), K(meta));
    break;
  }
  return ret;
}

int decode_datum_meta_compact(ObDatumMeta &meta, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int32_t type_val = static_cast<int32_t>(ObNullType);
  OB_UNIS_DECODE(type_val);
  if (OB_SUCC(ret)
      && OB_UNLIKELY(type_val < static_cast<int32_t>(ObNullType) || type_val >= static_cast<int32_t>(ObMaxType))) {
    ret = OB_DESERIALIZE_ERROR;
    SQL_LOG(WARN, "invalid datum type in compact meta", K(ret), K(type_val));
  }
  if (OB_SUCC(ret)) {
    meta.reset();
    meta.type_ = static_cast<ObObjType>(type_val);
    int32_t precision = -1;
    int32_t cs_type = static_cast<int32_t>(CS_TYPE_INVALID);
    switch (datum_meta_compact_family(meta.type_)) {
    case V4_META_DERIVED:
      OB_UNIS_DECODE(precision);
      if (OB_SUCC(ret)) { meta.precision_ = static_cast<ObPrecision>(precision); }
      break;
    case V4_META_PRECISION_SCALE:
      OB_UNIS_DECODE(precision);
      OB_UNIS_DECODE(meta.scale_);
      if (OB_SUCC(ret)) { meta.precision_ = static_cast<ObPrecision>(precision); }
      break;
    case V4_META_STRING:
      OB_UNIS_DECODE(cs_type);
      OB_UNIS_DECODE(precision);
      OB_UNIS_DECODE(meta.scale_);
      if (OB_SUCC(ret)
          && OB_UNLIKELY(cs_type < static_cast<int32_t>(CS_TYPE_INVALID)
                         || cs_type >= static_cast<int32_t>(CS_TYPE_MAX))) {
        ret = OB_DESERIALIZE_ERROR;
        SQL_LOG(WARN, "invalid collation type in compact meta", K(ret), K(cs_type));
      } else if (OB_SUCC(ret)) {
        meta.cs_type_ = static_cast<ObCollationType>(cs_type);
        meta.precision_ = static_cast<ObPrecision>(precision);
      }
      break;
    case V4_META_FULL: {
      const ObObjType outer_type = meta.type_;
      OB_UNIS_DECODE(meta);
      if (OB_SUCC(ret) && OB_UNLIKELY(outer_type != meta.type_)) {
        ret = OB_DESERIALIZE_ERROR;
        SQL_LOG(WARN, "datum meta compact fallback type mismatch", K(ret), K(outer_type), K(meta.type_));
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected datum meta compact family", K(ret), K(meta));
      break;
    }
  }
  return ret;
}

// whether members obj_meta_/max_length_/res_buf_len_ can be reconstructed
// from datum_meta_ (true for plain fixed length data types).
// NOTE: enum/set and placeholder types (ObNullType, VEC_TC_UNKNOWN) are
// excluded: their obj_meta_ does not follow the plain-type rebuild rule
// (e.g. enum/set datum_meta_.cs_type_ is overridden at cg time).
// (PoC: ObExpr::v4_fixed_length_meta_derivable, ob_expr.h inline.)
OB_INLINE bool v4_fixed_length_meta_derivable(const ObExpr &expr)
{
  const common::VecValueTypeClass tc =
    common::get_vec_value_tc(expr.datum_meta_.type_, expr.datum_meta_.scale_, expr.datum_meta_.precision_);
  const common::ObObjDatumMapType datum_map = common::ObDatum::get_obj_datum_map_type(expr.datum_meta_.type_);
  const uint32_t reserved_size = common::ObDatum::get_reserved_size(datum_map, expr.datum_meta_.precision_);
  const common::ObScale expected_scale = ob_is_bit_tc(expr.datum_meta_.type_) ?
                                           static_cast<common::ObScale>(expr.datum_meta_.length_semantics_) :
                                           static_cast<common::ObScale>(expr.datum_meta_.scale_);
  return common::is_fixed_length_vec(tc) && common::VEC_TC_ENUM_SET != tc && common::VEC_TC_UNKNOWN != tc
         && common::ObNullType != expr.datum_meta_.type_ && expr.obj_meta_.get_type() == expr.datum_meta_.type_
         && expr.obj_meta_.get_collation_level() == common::CS_LEVEL_NUMERIC
         && expr.obj_meta_.get_collation_type() == expr.datum_meta_.cs_type_
         && expr.obj_meta_.get_scale() == expected_scale && expr.max_length_ == static_cast<int32_t>(reserved_size)
         && expr.res_buf_len_ == reserved_size;
}

// PoC: ObExpr::build_v4_bitmap.
uint16_t build_v4_bitmap(const ObExpr &expr, bool as_hole)
{
  uint16_t bitmap = 0;
  if (as_hole) {
    // hole placeholder (mark serialization): only layout sizing contribution
    // (datum_meta_/flag_/frame_idx_/res_buf_len_) is put on wire.
    bitmap |= V4_IS_HOLE | V4_HAS_RES_BUF_LEN;
  } else {
    if (!v4_fixed_length_meta_derivable(expr)) {
      bitmap |= V4_HAS_OBJ_META | V4_HAS_RES_BUF_LEN;
      if (-1 != expr.max_length_) { // ctor default (UINT32_MAX, i.e. -1) carries no info
        bitmap |= V4_HAS_MAX_LENGTH;
      }
    }
    if (expr.arg_cnt_ > 0) { bitmap |= V4_HAS_ARGS; }
    if (expr.parent_cnt_ > 0) { bitmap |= V4_HAS_PARENTS; }
    if (expr.inner_func_cnt_ > 0) { bitmap |= V4_HAS_INNER_FUNCS; }
    // T_QUESTIONMARK stores param_idx in extra_ even when it is 0, the receiver
    // replays the param frame layout from it, so always carry it.
    if (0 != expr.extra_ || T_QUESTIONMARK == expr.type_) { bitmap |= V4_HAS_EXTRA; }
    if (ObExpr::INVALID_EXP_CTX_ID != expr.expr_ctx_id_) { bitmap |= V4_HAS_EXPR_CTX_ID; }
    if (NULL != expr.extra_info_) { bitmap |= V4_HAS_EXTRA_INFO; }
    if (OB_INVALID_INDEX_INT64 != expr.local_session_var_id_) { bitmap |= V4_HAS_LOCAL_VAR; }
    if (expr.attrs_cnt_ > 0) { bitmap |= V4_HAS_ATTRS; }
    if (NULL != expr.eval_func_ || NULL != expr.eval_batch_func_ || NULL != expr.eval_vector_func_) {
      bitmap |= V4_HAS_EVAL_FUNCS;
    }
  }
  return bitmap;
}

// PoC: ObExpr::get_serialize_size_v4_body, extended with the compact datum-meta
// prefix. Uses the compact request encoding context.
int64_t get_serialize_size_compact_body(const ObExpr &expr, const bool as_hole)
{
  const uint16_t bitmap = build_v4_bitmap(expr, as_hole);
  int64_t len = 0;
  len += serialization::encoded_length_vi32(static_cast<int32_t>(as_hole ? T_INVALID : expr.type_));
  len += get_serialize_size_v4(&expr.datum_meta_);
  len += serialization::encoded_length_vi64(static_cast<int64_t>(expr.flag_));
  len += serialization::encoded_length_vi32(static_cast<int32_t>(expr.frame_idx_));
  len += serialization::encoded_length_vi64(static_cast<int64_t>(bitmap));
  if (bitmap & V4_HAS_OBJ_META) { len += expr.obj_meta_.get_serialize_size(); }
  if (bitmap & V4_HAS_MAX_LENGTH) { len += serialization::encoded_length_vi32(expr.max_length_); }
  if (bitmap & V4_HAS_RES_BUF_LEN) {
    len += serialization::encoded_length_vi32(static_cast<int32_t>(expr.res_buf_len_));
  }
  if (bitmap & V4_HAS_ARGS) { len += expr_ref_array_len(expr.args_, expr.arg_cnt_); }
  if (bitmap & V4_HAS_PARENTS) { len += expr_ref_array_len(expr.parents_, expr.parent_cnt_); }
  if (bitmap & V4_HAS_INNER_FUNCS) { len += func_idx_array_vi64_len(expr.ser_inner_functions_, expr.inner_func_cnt_); }
  if (bitmap & V4_HAS_EXTRA) { len += serialization::encoded_length_vi64(static_cast<int64_t>(expr.extra_)); }
  if (bitmap & V4_HAS_EXPR_CTX_ID) {
    len += serialization::encoded_length_vi64(static_cast<int64_t>(expr.expr_ctx_id_));
  }
  if (bitmap & V4_HAS_EXTRA_INFO) {
    const ObExprOperatorType type = expr.extra_info_->type_;
    len += serialization::encoded_length_vi32(static_cast<int32_t>(type));
    if (T_INVALID != type) { len += expr.extra_info_->get_serialize_size(); }
  }
  if (bitmap & V4_HAS_LOCAL_VAR) { len += serialization::encoded_length_vi64(expr.local_session_var_id_); }
  if (bitmap & V4_HAS_ATTRS) { len += expr_ref_array_len(expr.attrs_, expr.attrs_cnt_); }
  if (bitmap & V4_HAS_EVAL_FUNCS) {
    const uint64_t idx0 = ObFuncSerialization::get_serialize_index(reinterpret_cast<void *>(expr.ser_eval_func_));
    const uint64_t idx1 = ObFuncSerialization::get_serialize_index(reinterpret_cast<void *>(expr.ser_eval_batch_func_));
    const uint64_t idx2 =
      ObFuncSerialization::get_serialize_index(reinterpret_cast<void *>(expr.ser_eval_vector_func_));
    len += serialization::encoded_length_vi64(static_cast<int64_t>(idx0));
    len += serialization::encoded_length_vi64(static_cast<int64_t>(idx1));
    len += serialization::encoded_length_vi64(static_cast<int64_t>(idx2));
  }
  return len;
}

// Compact ObExpr body decoder. DatumMeta is a nested standard compact serialize opt record; the
// remaining bitmap body preserves the compact semantics.
int decode_compact_body(ObExpr &expr, const char *buf, const int64_t data_len, int64_t &pos,
                        int64_t *decoded_bitmap = NULL)
{
  int ret = OB_SUCCESS;
  int32_t type = T_INVALID;
  int64_t flag = 0;
  int32_t frame_idx = 0;
  int64_t bitmap = 0;
  OB_UNIS_DECODE(type);
  if (OB_SUCC(ret)) {
    expr.type_ = static_cast<ObExprOperatorType>(type);
    ObDatumMetaCompact compact_meta(expr.datum_meta_);
    DEF_UNIS_VERSION_DECODE_REC(record, expr.datum_meta_, compact_meta);
    OB_UNIS_DECODE(record);
    if (OB_FAIL(ret)) {
    } else if (!record.decoded()) {
      ret = OB_DESERIALIZE_ERROR;
    } else if (record.version() != ObDatumMetaCompact::unis_declared_version()) {
      ret = OB_NOT_SUPPORTED;
      SQL_LOG(WARN, "compact Expr requires compact DatumMeta", K(ret), K(record.version()));
    } else {
      ObDatumMetaReplayCtx replay_ctx;
      ret = compact_meta.replay_to(expr.datum_meta_, replay_ctx);
    }
    LST_DO_CODE(OB_UNIS_DECODE, flag, frame_idx, bitmap);
    if (OB_SUCC(ret)) {
      expr.flag_ = static_cast<uint64_t>(flag);
      expr.frame_idx_ = static_cast<uint32_t>(frame_idx);
    }
  }
  const bool is_hole = 0 != (bitmap & V4_IS_HOLE);
  if (bitmap & V4_HAS_OBJ_META) { OB_UNIS_DECODE(expr.obj_meta_); }
  if (bitmap & V4_HAS_MAX_LENGTH) { OB_UNIS_DECODE(expr.max_length_); }
  if (OB_SUCC(ret) && (bitmap & V4_HAS_RES_BUF_LEN)) {
    int32_t res_buf_len = 0;
    OB_UNIS_DECODE(res_buf_len);
    if (OB_SUCC(ret)) {
      expr.res_buf_len_ = static_cast<uint32_t>(res_buf_len);
    }
  }
  if (OB_FAIL(ret)) {
  } else if ((bitmap & V4_HAS_ARGS)
             && OB_FAIL(decode_v4_pointer_array(buf, data_len, pos, expr.args_, expr.arg_cnt_,
                                                V4ExprRefDecoder(buf, data_len, pos)))) {
    SQL_LOG(WARN, "decode compact expr arguments failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if ((bitmap & V4_HAS_PARENTS)
             && OB_FAIL(decode_v4_pointer_array(buf, data_len, pos, expr.parents_, expr.parent_cnt_,
                                                V4ExprRefDecoder(buf, data_len, pos)))) {
    SQL_LOG(WARN, "decode compact expr parents failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if ((bitmap & V4_HAS_INNER_FUNCS)
             && OB_FAIL(decode_v4_pointer_array(buf, data_len, pos, expr.inner_functions_, expr.inner_func_cnt_,
                                                V4InnerFuncDecoder(buf, data_len, pos)))) {
    SQL_LOG(WARN, "decode compact expr inner functions failed", K(ret));
  }
  if (OB_SUCC(ret) && (bitmap & V4_HAS_EXTRA)) {
    int64_t extra = 0;
    OB_UNIS_DECODE(extra);
    if (OB_SUCC(ret)) {
      expr.extra_ = static_cast<uint64_t>(extra);
    }
  }
  if (OB_SUCC(ret) && (bitmap & V4_HAS_EXPR_CTX_ID)) {
    int64_t ctx_id = 0;
    OB_UNIS_DECODE(ctx_id);
    if (OB_SUCC(ret)) {
      expr.expr_ctx_id_ = static_cast<uint32_t>(ctx_id);
    }
  }
  if (OB_SUCC(ret) && (bitmap & V4_HAS_EXTRA_INFO)) {
    int32_t info_type = T_INVALID;
    if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &info_type))) {
      SQL_LOG(WARN, "decode compact expr extra-info type failed", K(ret));
    } else if (T_INVALID != info_type
               && OB_FAIL(ObExprExtraInfoFactory::alloc(CURRENT_CONTEXT->get_arena_allocator(),
                                                        static_cast<ObExprOperatorType>(info_type),
                                                        expr.extra_info_))) {
      SQL_LOG(WARN, "allocate compact expr extra info failed", K(ret), K(info_type));
    } else if (T_INVALID != info_type && OB_FAIL(expr.extra_info_->deserialize(buf, data_len, pos))) {
      SQL_LOG(WARN, "decode compact expr extra info failed", K(ret), K(info_type));
    }
  }
  if (bitmap & V4_HAS_LOCAL_VAR) { OB_UNIS_DECODE(expr.local_session_var_id_); }
  if (OB_FAIL(ret)) {
  } else if ((bitmap & V4_HAS_ATTRS)
             && OB_FAIL(decode_v4_pointer_array(buf, data_len, pos, expr.attrs_, expr.attrs_cnt_,
                                                V4ExprRefDecoder(buf, data_len, pos)))) {
    SQL_LOG(WARN, "decode compact expr attributes failed", K(ret));
  }
  if (OB_SUCC(ret) && (bitmap & V4_HAS_EVAL_FUNCS)) {
    void *eval_func = NULL;
    void *eval_batch_func = NULL;
    void *eval_vector_func = NULL;
    if (OB_FAIL(decode_func_idx_vi64(buf, data_len, pos, eval_func, V4_FUNC_EVAL))) {
      SQL_LOG(WARN, "decode compact expr eval function failed", K(ret));
    } else if (OB_FAIL(decode_func_idx_vi64(buf, data_len, pos, eval_batch_func, V4_FUNC_BATCH))) {
      SQL_LOG(WARN, "decode compact expr batch function failed", K(ret));
    } else if (OB_FAIL(decode_func_idx_vi64(buf, data_len, pos, eval_vector_func, V4_FUNC_VECTOR))) {
      SQL_LOG(WARN, "decode compact expr vector function failed", K(ret));
    } else {
      expr.eval_func_ = reinterpret_cast<ObExpr::EvalFunc>(eval_func);
      expr.eval_batch_func_ = reinterpret_cast<ObExpr::EvalBatchFunc>(eval_batch_func);
      expr.eval_vector_func_ = reinterpret_cast<ObExpr::EvalVectorFunc>(eval_vector_func);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_hole) {
    const uint64_t f = expr.flag_;
    expr.flag_ = 0;
    expr.batch_result_ = (f & 0x1) != 0;
    expr.is_fixed_length_data_ = (f >> 6) & 0x1;
    const bool use_420_compat_func = GET_PHY_PLAN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0
                                     && GET_PHY_PLAN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0;
    expr.basic_funcs_ = ObDatumFuncs::get_basic_func(
      expr.datum_meta_.type_, expr.datum_meta_.cs_type_, expr.datum_meta_.scale_, lib::is_oracle_mode(),
      false /*has_lob_header*/, expr.datum_meta_.precision_, use_420_compat_func);
  } else {
    expr.obj_datum_map_ = ObDatum::get_obj_datum_map_type(expr.datum_meta_.type_);
    expr.vec_value_tc_ =
      common::get_vec_value_tc(expr.datum_meta_.type_, expr.datum_meta_.scale_, expr.datum_meta_.precision_);
    if (0 == (bitmap & V4_HAS_OBJ_META)) {
      expr.res_buf_len_ = ObDatum::get_reserved_size(expr.obj_datum_map_, expr.datum_meta_.precision_);
      expr.max_length_ = static_cast<int32_t>(expr.res_buf_len_);
      expr.obj_meta_.set_type(expr.datum_meta_.type_);
      expr.obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
      expr.obj_meta_.set_collation_type(expr.datum_meta_.cs_type_);
      expr.obj_meta_.set_scale(ob_is_bit_tc(expr.datum_meta_.type_) ?
                                 static_cast<ObScale>(expr.datum_meta_.length_semantics_) :
                                 expr.datum_meta_.scale_);
    } else if (0 == (bitmap & V4_HAS_MAX_LENGTH)) {
      expr.max_length_ = -1;
    }
    const bool use_420_compat_func = GET_PHY_PLAN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0
                                     && GET_PHY_PLAN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0;
    expr.basic_funcs_ = ObDatumFuncs::get_basic_func(
      expr.datum_meta_.type_, expr.datum_meta_.cs_type_, expr.datum_meta_.scale_, lib::is_oracle_mode(),
      expr.obj_meta_.has_lob_header(), expr.datum_meta_.precision_, use_420_compat_func);
    if (OB_ISNULL(expr.basic_funcs_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "compact expr basic functions are null", K(ret), K(expr.type_), K(expr.datum_meta_));
    }
  }
  if (OB_SUCC(ret) && expr.is_batch_result()) { expr.batch_idx_mask_ = UINT64_MAX; }
  if (OB_SUCC(ret) && OB_NOT_NULL(decoded_bitmap)) { *decoded_bitmap = bitmap; }
  return ret;
}

// ---------------------------------------------------------------------------
// Frame layout replay and shared CG partial-frame validation.
// ---------------------------------------------------------------------------

// Replays ObStaticEngineExprCG::cg_all_frame_layout on the receiver side.
// All size primitives mirror the CG ones in ob_static_engine_expr_cg.h exactly
// (they are simple functions of the expr members that are still on wire);
// the per-frame size verification at the end guards against any divergence.
struct ObExprFrameLayoutReplayer
{
  ObExprFrameLayoutReplayer(common::ObIArray<ObExpr> &rt_exprs, const int64_t batch_size,
                            const bool partial_frame = false) :
    rt_exprs_(rt_exprs), batch_size_(batch_size), partial_frame_(partial_frame)
  {}

  // ---- size primitives (mirror ObStaticEngineExprCG statics) ----
  int64_t datums_cnt(const ObExpr &e) const
  {
    return e.is_batch_result() ? batch_size_ : 1;
  }
  int64_t skip_size(const ObExpr &e) const
  {
    return e.is_batch_result() ? ObBitVector::memory_size(batch_size_) : 1;
  }
  int64_t bitmap_size(const ObExpr &e) const
  {
    return ObBitVector::memory_size(e.is_batch_result() ? batch_size_ : 1);
  }
  int64_t datums_header_size(const ObExpr &e) const
  {
    return static_cast<int64_t>(sizeof(ObDatum)) * datums_cnt(e);
  }
  int64_t dyn_buf_header_size(const ObExpr &e) const
  {
    return ObDynReserveBuf::supported(e.datum_meta_.type_) ?
             static_cast<int64_t>(sizeof(ObDynReserveBuf)) * datums_cnt(e) :
             0;
  }
  int64_t ptrs_size(const ObExpr &e) const
  {
    return e.is_fixed_length_data_ ? 0 : static_cast<int64_t>(sizeof(char *)) * datums_cnt(e);
  }
  int64_t offsets_size(const ObExpr &e) const
  {
    return e.is_fixed_length_data_ ? 0 : static_cast<int64_t>(sizeof(uint32_t)) * (datums_cnt(e) + 1);
  }
  int64_t reserve_data_consume(const ObExpr &e) const
  {
    return e.res_buf_len_
           + (ObDynReserveBuf::supported(e.datum_meta_.type_) && e.res_buf_len_ > 0 ?
                static_cast<int64_t>(sizeof(ObDynReserveBuf)) :
                0);
  }

  // max frame space occupied by one expr (mirror of ObStaticEngineExprCG::frame_max_offset)
  int64_t expr_max_offset(const ObExpr &e, const bool rich) const
  {
    return ObStaticEngineExprCG::frame_max_offset(e, std::max<int64_t>(batch_size_, 1), rich);
  }

  // Recover the full layout independently, then repeat CG's partial-frame trim.
  int check_frame(const ObFrameInfo &frame, const common::ObIArray<int64_t> &members, const int64_t replayed_size,
                  const ObExprFrameReplayKind kind) const
  {
    int ret = OB_SUCCESS;
    const ObExpr *farthest = nullptr;
    int64_t max_extent = 0;
    for (int64_t i = 0; partial_frame_ && EXPR_FRAME_REPLAY_CONST != kind && i < members.count(); ++i) {
      const ObExpr &e = rt_exprs_.at(members.at(i));
      if (T_INVALID != e.type_) {
        const int64_t extent = expr_max_offset(e, frame.use_rich_format_);
        if (nullptr == farthest || max_extent < extent) {
          farthest = &e;
          max_extent = extent;
        }
      }
    }
    ObFrameInfo expected = frame;
    if (OB_UNLIKELY(replayed_size < 0 || replayed_size > UINT32_MAX || members.count() > UINT32_MAX)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      expected.expr_cnt_ = members.count();
      expected.frame_size_ = replayed_size;
      if (partial_frame_ && EXPR_FRAME_REPLAY_CONST != kind) {
        ObStaticEngineExprCG::trim_frame(expected, farthest, std::max<int64_t>(batch_size_, 1), frame.use_rich_format_,
                                         false, EXPR_FRAME_REPLAY_DYNAMIC == kind);
      }
      if (OB_UNLIKELY(expected.expr_cnt_ != frame.expr_cnt_ || expected.frame_size_ != frame.frame_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replayed frame metadata differs from CG", K(ret), K(frame), K(expected), K(kind));
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(max_extent > static_cast<int64_t>(frame.frame_size_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replayed expr extent exceeds trimmed frame", K(ret), K(max_extent), K(frame));
      }
    }
    return ret;
  }

  // item layout (cg_frame_layout + arrange_datum_data): const/dynamic frames and
  // non-vectorized datum frames
  int replay_item_frame(const ObFrameInfo &frame, const common::ObIArray<int64_t> &members,
                        const ObExprFrameReplayKind kind)
  {
    int ret = OB_SUCCESS;
    const int64_t item =
      static_cast<int64_t>(sizeof(ObDatum) + sizeof(ObEvalInfo)) + (frame.use_rich_format_ ? sizeof(VectorHeader) : 0);
    // Compact streams the complete member list, including trailing holes.
    int64_t data_off = members.count() * item;
    for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
      ObExpr &e = rt_exprs_.at(members.at(i));
      e.datum_off_ = static_cast<uint32_t>(i * item);
      e.eval_info_off_ = e.datum_off_ + sizeof(ObDatum);
      if (frame.use_rich_format_) {
        e.vector_header_off_ = e.eval_info_off_ + sizeof(ObEvalInfo);
        if (e.is_fixed_length_data_) { e.len_ = e.res_buf_len_; }
      }
      const int64_t consume = reserve_data_consume(e);
      if (consume > 0) {
        data_off += consume;
        e.res_buf_off_ = static_cast<uint32_t>(data_off - e.res_buf_len_);
        e.dyn_buf_header_offset_ = e.res_buf_off_ - sizeof(ObDynReserveBuf);
      } else {
        e.res_buf_off_ = 0;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_frame(frame, members, data_off, kind))) {
      LOG_WARN("check replayed item frame failed", K(ret), K(frame));
    }
    return ret;
  }

  // param frame (cg_param_frame_layout): addressed by param index (extra_)
  int replay_param_frame(const ObFrameInfo &frame, const common::ObIArray<int64_t> &members,
                         const int64_t original_param_cnt, const int64_t const_frame_cnt)
  {
    int ret = OB_SUCCESS;
    const int64_t item =
      static_cast<int64_t>(sizeof(ObDatum) + sizeof(ObEvalInfo)) + (frame.use_rich_format_ ? sizeof(VectorHeader) : 0);
    const int64_t cnt_per_frame = common::MAX_FRAME_SIZE / item;
    const ObExpr *farthest = nullptr;
    if (OB_UNLIKELY(original_param_cnt < 0 || frame.expr_cnt_ > cnt_per_frame
                    || static_cast<int64_t>(frame.frame_size_) != static_cast<int64_t>(frame.expr_cnt_) * item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid compact parameter frame slots", K(ret), K(frame), K(original_param_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
      ObExpr &e = rt_exprs_.at(members.at(i));
      if (T_INVALID == e.type_) {
        continue; // hole: param frames have no per-expr data, nothing to replay
      } else if (OB_UNLIKELY(T_QUESTIONMARK != e.type_ || e.extra_ > static_cast<uint64_t>(INT64_MAX))) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      const int64_t pidx = static_cast<int64_t>(e.extra_);
      const int64_t original_frame_cnt = original_param_cnt / cnt_per_frame + (original_param_cnt % cnt_per_frame != 0);
      const int64_t param_frame_idx = pidx < original_param_cnt ?
                                        pidx / cnt_per_frame :
                                        original_frame_cnt + (pidx - original_param_cnt) / cnt_per_frame;
      const int64_t datum_idx =
        pidx < original_param_cnt ? pidx % cnt_per_frame : (pidx - original_param_cnt) % cnt_per_frame;
      if (OB_UNLIKELY(param_frame_idx != static_cast<int64_t>(frame.frame_idx_) - const_frame_cnt
                      || datum_idx >= frame.expr_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("compact parameter slot out of bounds", K(ret), K(frame), K(pidx), K(param_frame_idx), K(datum_idx));
        break;
      }
      e.datum_off_ = static_cast<uint32_t>(datum_idx * item);
      e.eval_info_off_ = e.datum_off_ + sizeof(ObDatum);
      if (frame.use_rich_format_) {
        e.vector_header_off_ = e.eval_info_off_ + sizeof(ObEvalInfo);
        if (e.is_fixed_length_data_) {
          e.len_ =
            ObDatum::get_reserved_size(ObDatum::get_obj_datum_map_type(e.datum_meta_.type_), e.datum_meta_.precision_);
        }
      }
      e.res_buf_off_ = 0;
      e.res_buf_len_ = 0;
      if (nullptr == farthest || farthest->datum_off_ < e.datum_off_) { farthest = &e; }
    }
    if (OB_SUCC(ret) && partial_frame_) {
      // Every allocated parameter frame has slots. The trim result depends only
      // on the farthest selected slot (or one slot for an unused frame), so an
      // independent capacity bound suffices without trusting the received count.
      ObFrameInfo expected = frame;
      expected.expr_cnt_ = cnt_per_frame;
      expected.frame_size_ = cnt_per_frame * item;
      ObStaticEngineExprCG::trim_frame(expected, farthest, std::max<int64_t>(batch_size_, 1), frame.use_rich_format_,
                                       true, true);
      if (OB_UNLIKELY(expected.expr_cnt_ != frame.expr_cnt_ || expected.frame_size_ != frame.frame_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replayed parameter trim differs from CG", K(ret), K(frame), K(expected));
      }
    }
    return ret;
  }

  // vectorized datum frame (cg_frame_layout_vector_version + arrange_datums_data)
  int replay_vector_frame(const ObFrameInfo &frame, const common::ObIArray<int64_t> &members)
  {
    int ret = OB_SUCCESS;
    int64_t cur = 0;
    // §datums + pvt skip
    for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
      ObExpr &e = rt_exprs_.at(members.at(i));
      e.datum_off_ = static_cast<uint32_t>(cur);
      cur += skip_size(e) + datums_header_size(e);
    }
    if (frame.use_rich_format_) {
      // §lens/offsets
      for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
        ObExpr &e = rt_exprs_.at(members.at(i));
        if (!e.is_fixed_length_data_) {
          e.len_arr_off_ = static_cast<uint32_t>(cur);
          cur += offsets_size(e);
        } else {
          e.len_ = e.res_buf_len_;
        }
      }
      // §ptrs
      for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
        ObExpr &e = rt_exprs_.at(members.at(i));
        if (!e.is_fixed_length_data_) {
          e.offset_off_ = static_cast<uint32_t>(cur);
          cur += ptrs_size(e);
        }
      }
      // §vector header
      for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
        ObExpr &e = rt_exprs_.at(members.at(i));
        e.vector_header_off_ = static_cast<uint32_t>(cur);
        cur += sizeof(VectorHeader);
      }
      // §null bitmap
      for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
        ObExpr &e = rt_exprs_.at(members.at(i));
        e.null_bitmap_off_ = static_cast<uint32_t>(cur);
        cur += bitmap_size(e);
      }
      // §cont dyn buf
      for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
        ObExpr &e = rt_exprs_.at(members.at(i));
        if (!e.is_fixed_length_data_) {
          e.cont_buf_off_ = static_cast<uint32_t>(cur);
          cur += sizeof(ObDynReserveBuf);
        }
      }
    }
    // §eval info
    for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
      ObExpr &e = rt_exprs_.at(members.at(i));
      e.eval_info_off_ = static_cast<uint32_t>(cur);
      cur += sizeof(ObEvalInfo);
    }
    // §eval flags
    for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
      ObExpr &e = rt_exprs_.at(members.at(i));
      e.eval_flags_off_ = static_cast<uint32_t>(cur);
      cur += bitmap_size(e);
    }
    // §dyn buf header
    for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
      ObExpr &e = rt_exprs_.at(members.at(i));
      e.dyn_buf_header_offset_ = static_cast<uint32_t>(cur);
      cur += dyn_buf_header_size(e);
    }
    // §reserved datum data
    int64_t data = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
      ObExpr &e = rt_exprs_.at(members.at(i));
      e.pvt_skip_off_ = e.datum_off_ + static_cast<uint32_t>(datums_header_size(e));
      e.res_buf_off_ = static_cast<uint32_t>(cur + data);
      data += e.res_buf_len_ * datums_cnt(e);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_frame(frame, members, cur + data, EXPR_FRAME_REPLAY_VECTOR))) {
      LOG_WARN("check replayed vector frame failed", K(ret), K(frame));
    }
    return ret;
  }

  common::ObIArray<ObExpr> &rt_exprs_;
  const int64_t batch_size_;
  const bool partial_frame_;
};

} // end namespace expr_ser_compact_detail

using expr_ser_compact_detail::build_v4_bitmap;
using expr_ser_compact_detail::get_serialize_size_compact_body;
using expr_ser_compact_detail::decode_compact_body;
using expr_ser_compact_detail::datum_meta_compact_size;
using expr_ser_compact_detail::decode_datum_meta_compact;
using expr_ser_compact_detail::encode_datum_meta_compact;
using expr_ser_compact_detail::encode_func_idx_vi64;

int ObExprReplayCtx::prepare(const common::ObIArray<ObExprCompactHeader> &headers)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(prepared_ || headers.count() != exprs_.count())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid expr replay prepare", K(ret), K(prepared_), K(headers.count()), K(exprs_.count()));
  } else {
    const common::ObIArray<ObFrameInfo> *frame_arrays[4] = {&const_frames_, &param_frames_, &dynamic_frames_,
                                                            &datum_frames_};
    const int64_t frame_count =
      const_frames_.count() + param_frames_.count() + dynamic_frames_.count() + datum_frames_.count();
    if (OB_FAIL(frame_states_.prepare_allocate(frame_count))) {
      SQL_LOG(WARN, "allocate expr frame replay states failed", K(ret), K(frame_count));
    }
    for (int64_t a = 0; OB_SUCC(ret) && a < ARRAYSIZEOF(frame_arrays); ++a) {
      const common::ObIArray<ObFrameInfo> &frames = *frame_arrays[a];
      for (int64_t i = 0; OB_SUCC(ret) && i < frames.count(); ++i) {
        const ObFrameInfo &frame = frames.at(i);
        if (OB_UNLIKELY(frame.frame_idx_ >= frame_count) || OB_NOT_NULL(frame_states_.at(frame.frame_idx_).frame_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid or duplicate expr frame index", K(ret), K(frame), K(frame_count));
        } else {
          ObExprFrameReplayState &state = frame_states_.at(frame.frame_idx_);
          state.frame_ = &frame;
          state.kind_ = frame_arrays[a] == &const_frames_   ? EXPR_FRAME_REPLAY_CONST :
                        frame_arrays[a] == &param_frames_   ? EXPR_FRAME_REPLAY_PARAM :
                        frame_arrays[a] == &dynamic_frames_ ? EXPR_FRAME_REPLAY_DYNAMIC :
                        cg_batch_size_ > 0                  ? EXPR_FRAME_REPLAY_VECTOR :
                                                              EXPR_FRAME_REPLAY_ITEM;
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_.count(); ++i) {
      ObExpr &expr = exprs_.at(i);
      if (OB_UNLIKELY(expr.frame_idx_ >= frame_count)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "expr frame index has no frame metadata", K(ret), K(i), K(expr.frame_idx_));
      } else {
        ObExprFrameReplayState &state = frame_states_.at(expr.frame_idx_);
        if (EXPR_FRAME_REPLAY_PARAM == state.kind_ && T_INVALID != expr.type_) { expr.res_buf_len_ = 0; }
        if (OB_FAIL(state.members_.push_back(i))) {
          SQL_LOG(WARN, "save expr frame replay member failed", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      replayed_cnt_ = 0;
      prepared_ = true;
    }
  }
  return ret;
}

int ObExprReplayCtx::replay(ObExpr &dst)
{
  int ret = OB_SUCCESS;
  ObExprFrameReplayState *state = &frame_states_.at(dst.frame_idx_);
  if (!state->replayed_) {
    expr_ser_compact_detail::ObExprFrameLayoutReplayer replayer(exprs_, cg_batch_size_, partial_frame_);
    if (EXPR_FRAME_REPLAY_PARAM == state->kind_) {
      if (OB_FAIL(
            replayer.replay_param_frame(*state->frame_, state->members_, original_param_cnt_, const_frames_.count()))) {
        SQL_LOG(WARN, "replay compact parameter frame failed", K(ret));
      }
    } else if (EXPR_FRAME_REPLAY_VECTOR == state->kind_) {
      if (OB_FAIL(replayer.replay_vector_frame(*state->frame_, state->members_))) {
        SQL_LOG(WARN, "replay compact vector frame failed", K(ret));
      }
    } else {
      if (OB_FAIL(replayer.replay_item_frame(*state->frame_, state->members_, state->kind_))) {
        SQL_LOG(WARN, "replay compact item frame failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) { state->replayed_ = true; }
  }
  if (OB_SUCC(ret)) { ++replayed_cnt_; }
  return ret;
}

int ObExprReplayCtx::check() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!prepared_ || replayed_cnt_ != exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "compact expr replay is incomplete", K(ret), K(prepared_), K(replayed_cnt_), K(exprs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < frame_states_.count(); ++i) {
    const ObExprFrameReplayState &state = frame_states_.at(i);
    if (!state.members_.empty() && !state.replayed_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "expr frame was not replayed", K(ret), K(state));
    } else if (state.members_.empty()) {
      expr_ser_compact_detail::ObExprFrameLayoutReplayer replayer(exprs_, cg_batch_size_, partial_frame_);
      if (EXPR_FRAME_REPLAY_PARAM == state.kind_) {
        ret = replayer.replay_param_frame(*state.frame_, state.members_, original_param_cnt_, const_frames_.count());
      } else {
        ret = replayer.check_frame(*state.frame_, state.members_, 0, state.kind_);
      }
    }
  }
  return ret;
}

int ObExprCompactHeader::replay_to(ObExpr &dst, ObExprReplayCtx &ctx) const
{
  return ctx.replay(dst);
}

OB_DEF_SERIALIZE(ObDatumMetaCompact)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "datum meta compact record is not bound for encode", K(ret));
  } else if (OB_FAIL(encode_datum_meta_compact(*meta_, buf, buf_len, pos))) {
    SQL_LOG(WARN, "encode compact datum-meta body failed", K(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDatumMetaCompact)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "datum meta compact record is not bound for decode", K(ret));
  } else if (OB_FAIL(decode_datum_meta_compact(*meta_, buf, data_len, pos))) {
    SQL_LOG(WARN, "decode datum meta compact body failed", K(ret), K(pos), K(data_len));
  } else if (OB_UNLIKELY(pos != data_len)) {
    ret = OB_DESERIALIZE_ERROR;
    SQL_LOG(WARN, "datum meta compact body length mismatch", K(ret), K(pos), K(data_len));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDatumMetaCompact)
{
  return OB_ISNULL(meta_) ? 0 : datum_meta_compact_size(*meta_);
}

int ObDatumMetaCompact::replay_to(ObDatumMeta &dst, ObDatumMetaReplayCtx &) const
{
  const expr_ser_compact_detail::ObDatumMetaCompactFamily family =
    expr_ser_compact_detail::datum_meta_compact_family(dst.type_);
  if (expr_ser_compact_detail::V4_META_DERIVED == family) {
    dst.cs_type_ = CS_TYPE_BINARY;
    dst.scale_ = 0;
  } else if (expr_ser_compact_detail::V4_META_PRECISION_SCALE == family) {
    dst.cs_type_ = CS_TYPE_BINARY;
  }
  return OB_SUCCESS;
}

int encode_v4(const ObDatumMeta *meta, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "datum meta is null", K(ret));
  } else if (serialization::using_v4_encoding()) {
    ObDatumMetaCompact compact(*meta);
    OB_UNIS_ENCODE(compact);
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, *meta))) {
    SQL_LOG(WARN, "encode legacy datum-meta record failed", K(ret));
  }
  return ret;
}

int64_t get_serialize_size_v4(const ObDatumMeta *meta)
{
  int64_t len = 0;
  if (OB_ISNULL(meta)) {
  } else if (serialization::using_v4_encoding()) {
    ObDatumMetaCompact compact(*meta);
    OB_UNIS_ADD_LEN(compact);
  } else {
    len = serialization::encoded_length(*meta);
  }
  return len;
}

ObExprCompactHeader::ObExprCompactHeader(const ObExpr &expr, const bool as_hole) :
  type_(as_hole ? T_INVALID : expr.type_), datum_meta_(expr.datum_meta_), flag_(expr.flag_),
  frame_idx_(expr.frame_idx_), bitmap_(build_v4_bitmap(expr, as_hole)), expr_(const_cast<ObExpr *>(&expr))
{}

OB_DEF_SERIALIZE(ObExprCompactHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "compact expr header is not bound for encode", K(ret));
  } else {
    serialization::SerCompactEncodingGuard compact_guard(true);
    const ObExpr &expr = *expr_;
    OB_UNIS_ENCODE(type_);
    if (OB_SUCC(ret) && OB_FAIL(encode_v4(&datum_meta_, buf, buf_len, pos))) {
      SQL_LOG(WARN, "encode compact expr datum meta failed", K(ret));
    }
    LST_DO_CODE(OB_UNIS_ENCODE, flag_, frame_idx_, bitmap_);
    if (bitmap_ & V4_HAS_OBJ_META) { OB_UNIS_ENCODE(expr.obj_meta_); }
    if (bitmap_ & V4_HAS_MAX_LENGTH) { OB_UNIS_ENCODE(expr.max_length_); }
    if (bitmap_ & V4_HAS_RES_BUF_LEN) { OB_UNIS_ENCODE(static_cast<int32_t>(expr.res_buf_len_)); }
    if (OB_SUCC(ret) && (bitmap_ & V4_HAS_ARGS)) {
      OB_UNIS_ENCODE(static_cast<int32_t>(expr.arg_cnt_));
      for (uint32_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) { OB_UNIS_ENCODE(expr.args_[i]); }
    }
    if (OB_SUCC(ret) && (bitmap_ & V4_HAS_PARENTS)) {
      OB_UNIS_ENCODE(static_cast<int32_t>(expr.parent_cnt_));
      for (uint32_t i = 0; OB_SUCC(ret) && i < expr.parent_cnt_; ++i) { OB_UNIS_ENCODE(expr.parents_[i]); }
    }
    if (OB_SUCC(ret) && (bitmap_ & V4_HAS_INNER_FUNCS)) {
      OB_UNIS_ENCODE(static_cast<int32_t>(expr.inner_func_cnt_));
      for (uint32_t i = 0; OB_SUCC(ret) && i < expr.inner_func_cnt_; ++i) {
        if (OB_FAIL(
              encode_func_idx_vi64(buf, buf_len, pos, reinterpret_cast<const void *>(expr.ser_inner_functions_[i])))) {
          SQL_LOG(WARN, "encode compact expr inner function failed", K(ret), K(i));
        }
      }
    }
    if (bitmap_ & V4_HAS_EXTRA) { OB_UNIS_ENCODE(static_cast<int64_t>(expr.extra_)); }
    if (bitmap_ & V4_HAS_EXPR_CTX_ID) { OB_UNIS_ENCODE(static_cast<int64_t>(expr.expr_ctx_id_)); }
    if (OB_SUCC(ret) && (bitmap_ & V4_HAS_EXTRA_INFO)) {
      const ObExprOperatorType type = expr.extra_info_->type_;
      OB_UNIS_ENCODE(static_cast<int32_t>(type));
      if (T_INVALID != type) { OB_UNIS_ENCODE(*expr.extra_info_); }
    }
    if (bitmap_ & V4_HAS_LOCAL_VAR) { OB_UNIS_ENCODE(expr.local_session_var_id_); }
    if (OB_SUCC(ret) && (bitmap_ & V4_HAS_ATTRS)) {
      OB_UNIS_ENCODE(static_cast<int32_t>(expr.attrs_cnt_));
      for (uint32_t i = 0; OB_SUCC(ret) && i < expr.attrs_cnt_; ++i) { OB_UNIS_ENCODE(expr.attrs_[i]); }
    }
    if (OB_SUCC(ret) && (bitmap_ & V4_HAS_EVAL_FUNCS)) {
      if (OB_FAIL(encode_func_idx_vi64(buf, buf_len, pos, reinterpret_cast<const void *>(expr.ser_eval_func_)))) {
        SQL_LOG(WARN, "encode compact expr eval function failed", K(ret));
      } else if (OB_FAIL(encode_func_idx_vi64(buf, buf_len, pos,
                                              reinterpret_cast<const void *>(expr.ser_eval_batch_func_)))) {
        SQL_LOG(WARN, "encode compact expr batch function failed", K(ret));
      } else if (OB_FAIL(encode_func_idx_vi64(buf, buf_len, pos,
                                              reinterpret_cast<const void *>(expr.ser_eval_vector_func_)))) {
        SQL_LOG(WARN, "encode compact expr vector function failed", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObExprCompactHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "compact expr header is not bound for decode", K(ret));
  } else {
    serialization::CompactExprPtrGuard compact_guard(true);
    int64_t bitmap = 0;
    if (OB_FAIL(decode_compact_body(*expr_, buf, data_len, pos, &bitmap))) {
      SQL_LOG(WARN, "decode compact expr header body failed", K(ret), K(pos), K(data_len));
    } else if (OB_UNLIKELY(pos != data_len)) {
      ret = OB_DESERIALIZE_ERROR;
      SQL_LOG(WARN, "compact expr header body length mismatch", K(ret), K(pos), K(data_len));
    } else if (OB_UNLIKELY(bitmap < 0 || bitmap > UINT16_MAX)) {
      ret = OB_DESERIALIZE_ERROR;
      SQL_LOG(WARN, "compact expr bitmap is out of range", K(ret), K(bitmap));
    } else {
      type_ = expr_->type_;
      datum_meta_ = expr_->datum_meta_;
      flag_ = expr_->flag_;
      frame_idx_ = expr_->frame_idx_;
      bitmap_ = static_cast<uint16_t>(bitmap);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprCompactHeader)
{
  int64_t len = 0;
  if (OB_NOT_NULL(expr_)) {
    serialization::SerCompactEncodingGuard compact_guard(true);
    len = get_serialize_size_compact_body(*expr_, 0 != (bitmap_ & V4_IS_HOLE));
  }
  return len;
}

// ---------------------------------------------------------------------------
// Single-expr standard UNIS compact serialize opt codec.
// ---------------------------------------------------------------------------

int encode_expr_v4(const ObExpr &expr, char *buf, const int64_t buf_len, int64_t &pos, const bool as_hole)
{
  ObExprCompactHeader header(expr, as_hole);
  return header.serialize(buf, buf_len, pos);
}

int64_t expr_v4_size(const ObExpr &expr, bool as_hole)
{
  ObExprCompactHeader header(expr, as_hole);
  return header.get_serialize_size();
}

int encode_v4(const ObExpr *expr, char *buf, const int64_t buf_len, int64_t &pos, const bool as_hole)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "expr is null", K(ret));
  } else if (serialization::using_v4_encoding()) {
    ObExprCompactHeader header(*expr, as_hole);
    OB_UNIS_ENCODE(header);
  } else if (as_hole && OB_FAIL(serialization::encode(buf, buf_len, pos, ObEmptyExpr::instance()))) {
    SQL_LOG(WARN, "encode legacy empty expr failed", K(ret));
  } else if (!as_hole && OB_FAIL(serialization::encode(buf, buf_len, pos, *expr))) {
    SQL_LOG(WARN, "encode legacy expr failed", K(ret));
  }
  return ret;
}

int64_t get_serialize_size_v4(const ObExpr *expr, const bool as_hole)
{
  int64_t len = 0;
  if (OB_ISNULL(expr)) {
  } else if (serialization::using_v4_encoding()) {
    ObExprCompactHeader header(*expr, as_hole);
    OB_UNIS_ADD_LEN(header);
  } else if (as_hole) {
    len = serialization::encoded_length(ObEmptyExpr::instance());
  } else {
    len = serialization::encoded_length(*expr);
  }
  return len;
}

// ---------------------------------------------------------------------------
// Frame layout replay (PoC ob_expr_frame_info.cpp, moved here unchanged).
// ---------------------------------------------------------------------------
int replay_expr_frame_layout(common::ObIArray<ObExpr> &rt_exprs, const common::ObIArray<ObFrameInfo> &const_frames,
                             const common::ObIArray<ObFrameInfo> &param_frames,
                             const common::ObIArray<ObFrameInfo> &dynamic_frames,
                             const common::ObIArray<ObFrameInfo> &datum_frames, const int64_t cg_batch_size,
                             const int64_t original_param_cnt, const bool partial_frame)
{
  int ret = OB_SUCCESS;
  ObArray<ObExprCompactHeader> headers;
  if (OB_FAIL(headers.prepare_allocate(rt_exprs.count()))) {
    SQL_LOG(WARN, "allocate compact expr headers failed", K(ret), K(rt_exprs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rt_exprs.count(); ++i) {
    headers.at(i) = ObExprCompactHeader(rt_exprs.at(i));
  }
  ObExprReplayCtx replay_ctx(rt_exprs, const_frames, param_frames, dynamic_frames, datum_frames, cg_batch_size,
                             original_param_cnt, partial_frame);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(replay_ctx.prepare(headers))) {
    SQL_LOG(WARN, "prepare compact expr frame layout replay failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rt_exprs.count(); ++i) {
    if (OB_FAIL(headers.at(i).replay_to(rt_exprs.at(i), replay_ctx))) {
      SQL_LOG(WARN, "replay compact expr frame layout failed", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(replay_ctx.check())) {
    SQL_LOG(WARN, "finish compact expr frame layout replay failed", K(ret));
  }
  return ret;
}

// ---------------------------------------------------------------------------
// DAS Expr-frame adapter built on the legacy frame metadata codec.
// ---------------------------------------------------------------------------
int ObDASExprFrameLayout::init(ObExecContext &ctx, const bool partial_frame)
{
  int ret = OB_SUCCESS;
  const ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  const ObPhysicalPlan *plan = OB_NOT_NULL(plan_ctx) ? plan_ctx->get_phy_plan() : nullptr;
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan is missing for compact serialize opt", K(ret));
  } else {
    cg_batch_size_ = plan->get_batch_size();
    partial_frame_ = partial_frame;
    original_param_cnt_ = plan_ctx->get_original_param_cnt();
    frame_cnt_ =
      ctx.get_ori_frame_cnt() != 0 && ctx.get_ori_frames() != nullptr ? ctx.get_ori_frame_cnt() : ctx.get_frame_cnt();
  }
  return ret;
}

int ObDASExprFrameLayout::serialize_meta(char *buf, const int64_t buf_len, int64_t &pos,
                                         const ObExprFrameInfo &info) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, cg_batch_size_, original_param_cnt_, frame_cnt_, partial_frame_);
  const ObIArray<ObFrameInfo> *arrays[] = {&info.const_frame_, &info.param_frame_, &info.dynamic_frame_,
                                           &info.datum_frame_};
  for (int64_t a = 0; OB_SUCC(ret) && a < ARRAYSIZEOF(arrays); ++a) {
    OB_UNIS_ENCODE(arrays[a]->count());
    for (int64_t i = 0; OB_SUCC(ret) && i < arrays[a]->count(); ++i) { OB_UNIS_ENCODE(arrays[a]->at(i)); }
  }
  return ret;
}

int ObDASExprFrameLayout::deserialize_meta(const char *buf, const int64_t data_len, int64_t &pos,
                                           const ObExprFrameInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &cg_batch_size_))
      || OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &original_param_cnt_))
      || OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &frame_cnt_))
      || OB_FAIL(serialization::decode_bool(buf, data_len, pos, &partial_frame_))) {
    LOG_WARN("decode expression frame layout failed", K(ret));
  } else if (cg_batch_size_ < 0 || original_param_cnt_ < 0 || frame_cnt_ < 0
             || frame_cnt_ > das::OB_DAS_MAX_PACKET_SIZE / static_cast<int64_t>(sizeof(char *))) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid expression frame layout", K(ret), K(cg_batch_size_), K(original_param_cnt_), K(frame_cnt_));
  }
  ObExprFrameInfo &decoded = const_cast<ObExprFrameInfo &>(info);
  ObIArray<ObFrameInfo> *arrays[] = {&decoded.const_frame_, &decoded.param_frame_, &decoded.dynamic_frame_,
                                     &decoded.datum_frame_};
  for (int64_t a = 0; OB_SUCC(ret) && a < ARRAYSIZEOF(arrays); ++a) {
    int64_t count = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("decode frame array count failed", K(ret), K(a));
    } else if (count < 0 || count > frame_cnt_ || count > data_len - pos
               || count > das::OB_DAS_MAX_PACKET_SIZE / static_cast<int64_t>(sizeof(ObFrameInfo))) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("invalid frame array count", K(ret), K(a), K(count));
    } else if (FALSE_IT(arrays[a]->reuse())) {
    } else if (OB_FAIL(arrays[a]->prepare_allocate(count))) {
      LOG_WARN("allocate frame metadata failed", K(ret), K(a), K(count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(arrays[a]->at(i).deserialize(buf, data_len, pos))) {
        LOG_WARN("decode frame metadata failed", K(ret), K(a), K(i));
      } else if (arrays[a]->at(i).frame_idx_ >= frame_cnt_) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("frame index outside layout", K(ret), K(a), K(i), K(frame_cnt_));
      }
    }
  }
  if (OB_SUCC(ret)) { decoded.partial_frame_ = partial_frame_; }
  return ret;
}

int64_t ObDASExprFrameLayout::meta_size(const ObExprFrameInfo &info) const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, cg_batch_size_, original_param_cnt_, frame_cnt_, partial_frame_);
  const ObIArray<ObFrameInfo> *arrays[] = {&info.const_frame_, &info.param_frame_, &info.dynamic_frame_,
                                           &info.datum_frame_};
  for (int64_t a = 0; a < ARRAYSIZEOF(arrays); ++a) {
    OB_UNIS_ADD_LEN(arrays[a]->count());
    for (int64_t i = 0; i < arrays[a]->count(); ++i) { OB_UNIS_ADD_LEN(arrays[a]->at(i)); }
  }
  return len;
}

int encode_das_exprs(char *buf, int64_t buf_len, int64_t &pos, const ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr> &exprs = expr_frame_info.rt_exprs_;
  const int64_t expr_cnt = serialized_expr_count(expr_frame_info, serialization::using_v4_encoding());
  if (OB_UNLIKELY(expr_cnt < 0 || expr_cnt > exprs.count() || expr_cnt > INT32_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DAS expr count", K(ret), K(expr_cnt), K(exprs.count()));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, static_cast<int32_t>(expr_cnt)))) {
    LOG_WARN("encode DAS expr count failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr_cnt; ++i) {
    if (OB_FAIL(encode_v4(&exprs.at(i), buf, buf_len, pos, !expr_frame_member_selected(expr_frame_info, i)))) {
      LOG_WARN("encode DAS expr failed", K(ret), K(i));
    }
  }
  return ret;
}

int64_t das_exprs_size(const ObExprFrameInfo &expr_frame_info)
{
  const int64_t expr_cnt = serialized_expr_count(expr_frame_info, serialization::using_v4_encoding());
  int64_t len = serialization::encoded_length_i32(static_cast<int32_t>(expr_cnt));
  for (int64_t i = 0; i < expr_cnt; ++i) {
    len += get_serialize_size_v4(&expr_frame_info.rt_exprs_.at(i), !expr_frame_member_selected(expr_frame_info, i));
  }
  return len;
}

} // end namespace sql
} // end namespace oceanbase
