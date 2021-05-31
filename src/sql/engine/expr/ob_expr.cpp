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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/expr/ob_expr_extra_info_factory.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase {
using namespace common;
namespace sql {

// ObPrecision and ObLengthSemantics are union field in ObDatumMeta,
// need to be same size to make sure serialization works.
STATIC_ASSERT(sizeof(ObPrecision) == sizeof(ObLengthSemantics), "size of ObPrecision and ObLengthSemantics mismatch");

OB_SERIALIZE_MEMBER(ObDatumMeta, type_, cs_type_, scale_, precision_);

ObEvalCtx::ObEvalCtx(ObExecContext& exec_ctx, ObArenaAllocator& res_alloc, ObArenaAllocator& tmp_alloc)
    : frames_(exec_ctx.get_frames()), exec_ctx_(exec_ctx), expr_res_alloc_(res_alloc), tmp_alloc_(tmp_alloc)

{}

DEF_TO_STRING(ObEvalInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(evaluated), K_(projected), K_(notnull), K_(point_to_frame), K_(cnt));
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE(ObExpr)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      type_,
      datum_meta_,
      obj_meta_,
      max_length_,
      obj_datum_map_,
      ser_eval_func_,
      serialization::make_ser_carray(ser_inner_functions_, inner_func_cnt_),
      serialization::make_ser_carray(args_, arg_cnt_),
      serialization::make_ser_carray(parents_, parent_cnt_),
      frame_idx_,
      datum_off_,
      res_buf_off_,
      res_buf_len_,
      expr_ctx_id_);
  if (OB_SUCC(ret)) {
    if (ObExprExtraInfoFactory::is_registered(type_)) {
      if (OB_ISNULL(extra_info_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid extra_", K(extra_info_), K(ret));
      } else if (extra_info_->serialize(buf, buf_len, pos)) {
        LOG_WARN("fail to serialize calc part info", K(ret), K(*this));
      }
    } else {
      OB_UNIS_ENCODE(extra_)
    }
  }

  LST_DO_CODE(OB_UNIS_ENCODE, eval_info_off_);

  return ret;
}

OB_DEF_DESERIALIZE(ObExpr)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      type_,
      datum_meta_,
      obj_meta_,
      max_length_,
      obj_datum_map_,
      ser_eval_func_,
      serialization::make_ser_carray(ser_inner_functions_, inner_func_cnt_),
      serialization::make_ser_carray(args_, arg_cnt_),
      serialization::make_ser_carray(parents_, parent_cnt_),
      frame_idx_,
      datum_off_,
      res_buf_off_,
      res_buf_len_,
      expr_ctx_id_);
  if (OB_SUCC(ret)) {
    if (ObExprExtraInfoFactory::is_registered(type_)) {
      if (OB_FAIL(ObExprExtraInfoFactory::alloc(CURRENT_CONTEXT.get_arena_allocator(), type_, extra_info_))) {
        LOG_WARN("fail to alloc expr extra info", K(ret), K(type_));
      } else if (OB_NOT_NULL(extra_info_)) {
        if (OB_FAIL(extra_info_->deserialize(buf, data_len, pos))) {
          LOG_WARN("failed to deserialize calc partition info", K(ret));
        }
      }
    } else {
      OB_UNIS_DECODE(extra_);
    }
  }

  LST_DO_CODE(OB_UNIS_DECODE, eval_info_off_);
  if (0 == eval_info_off_ && OB_SUCC(ret)) {
    // compatible with 3.0, ObExprDatum::flag_ is ObEvalInfo
    eval_info_off_ = datum_off_ + sizeof(ObDatum);
  }

  if (OB_SUCC(ret)) {
    basic_funcs_ = ObDatumFuncs::get_basic_func(datum_meta_.type_, datum_meta_.cs_type_);
    CK(NULL != basic_funcs_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExpr)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      type_,
      datum_meta_,
      obj_meta_,
      max_length_,
      obj_datum_map_,
      ser_eval_func_,
      serialization::make_ser_carray(ser_inner_functions_, inner_func_cnt_),
      serialization::make_ser_carray(args_, arg_cnt_),
      serialization::make_ser_carray(parents_, parent_cnt_),
      frame_idx_,
      datum_off_,
      res_buf_off_,
      res_buf_len_,
      expr_ctx_id_);
  if (ObExprExtraInfoFactory::is_registered(type_)) {
    if (OB_ISNULL(extra_info_)) {
      LOG_ERROR("invalid expr extra_", K(extra_), K(*this));
    } else {
      len += extra_info_->get_serialize_size();
    }
  } else {
    OB_UNIS_ADD_LEN(extra_);
  }

  LST_DO_CODE(OB_UNIS_ADD_LEN, eval_info_off_);

  return len;
}

ObExpr::ObExpr()
    : magic_(MAGIC_NUM),
      type_(T_INVALID),
      datum_meta_(),
      obj_meta_(),
      max_length_(UINT32_MAX),
      obj_datum_map_(OBJ_DATUM_NULL),
      eval_func_(NULL),
      inner_functions_(NULL),
      inner_func_cnt_(0),
      args_(NULL),
      arg_cnt_(0),
      parents_(NULL),
      parent_cnt_(0),
      frame_idx_(0),
      datum_off_(0),
      eval_info_off_(0),
      res_buf_off_(0),
      res_buf_len_(0),
      expr_ctx_id_(INVALID_EXP_CTX_ID),
      extra_(0),
      basic_funcs_(NULL)
{}

char* ObExpr::alloc_str_res_mem(ObEvalCtx& ctx, const int64_t size) const
{
  char* mem = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObDynReserveBuf::supported(datum_meta_.type_))) {
    LOG_ERROR("unexpected alloc string result memory called", K(size), K(*this));
  } else {
    ObDynReserveBuf* drb =
        reinterpret_cast<ObDynReserveBuf*>(ctx.frames_[frame_idx_] + res_buf_off_ - sizeof(ObDynReserveBuf));
    if (OB_LIKELY(drb->len_ >= size)) {
      mem = drb->mem_;
    } else {
      const int64_t alloc_size = next_pow2(size);
      if (OB_UNLIKELY(alloc_size > UINT32_MAX)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(size), K(alloc_size), K(ret));
      } else if (OB_ISNULL(mem = static_cast<char*>(ctx.alloc_expr_res(alloc_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(ret));
      } else {
        // When extend memory, the old memory can not free, because the old memory may
        // still be referenced. see: ob_datum_cast.cpp::common_copy_string
        if (0 == drb->len_) {
          drb->magic_ = ObDynReserveBuf::MAGIC_NUM;
        }
        drb->len_ = alloc_size;
        drb->mem_ = mem;
      }
      LOG_DEBUG("extend expr result memory", K(ret), K(size), K(alloc_size), KP(this), KP(mem));
    }
  }
  return mem;
};

int ObExpr::eval_enumset(ObEvalCtx& ctx, const common::ObIArray<common::ObString>& str_values, const uint64_t cast_mode,
    common::ObDatum*& datum) const
{
  // performance critical, do not check %frame_idx_ and %frame again. (checked in CG)
  int ret = common::OB_SUCCESS;
  char* frame = ctx.frames_[frame_idx_];
  OB_ASSERT(NULL != frame);
  datum = (ObDatum*)(frame + datum_off_);
  ObEvalInfo* eval_info = (ObEvalInfo*)(frame + eval_info_off_);

  // do nothing for const/column reference expr or already evaluated expr
  if (!eval_info->evaluated_) {
    if (datum->ptr_ != frame + res_buf_off_) {
      datum->ptr_ = frame + res_buf_off_;
    }
    const common::ObObjTypeClass in_tc = args_[0]->obj_meta_.get_type_class();
    EvalEnumSetFunc eval_func;
    if (OB_FAIL(ObDatumCast::get_enumset_cast_function(in_tc, obj_meta_.get_type(), eval_func))) {
      SQL_LOG(WARN, "fail to get_enumset_cast_function", K(ret));
    } else {
      ret = eval_func(*this, str_values, cast_mode, ctx, *datum);
    }

    if (OB_LIKELY(common::OB_SUCCESS == ret)) {
      eval_info->evaluated_ = true;
    } else {
      datum->set_null();
    }
  }
  return ret;
}

void* ObExprStrResAlloc::alloc(const int64_t size)
{
  void* mem = expr_.get_str_res_mem(ctx_, off_ + size);
  if (NULL != mem) {
    mem = static_cast<char*>(mem) + off_;
    off_ += size;
  }
  return mem;
}

int ObDatumObjParam::from_objparam(const ObObjParam& objparam)
{
  int ret = OB_SUCCESS;
  meta_ = ObDatumMeta(objparam.meta_.get_type(), objparam.meta_.get_collation_type(), objparam.meta_.get_scale());
  if (OB_FAIL(datum_.from_obj(objparam))) {
    LOG_WARN("fail to init datum", K(objparam), K(ret));
  } else {
    accuracy_ = objparam.get_accuracy();
    res_flags_ = objparam.get_result_flag();
    flag_ = objparam.get_param_flag();
  }

  return ret;
}

int ObDatumObjParam::to_objparam(common::ObObjParam& obj_param)
{
  int ret = OB_SUCCESS;
  ObObjMeta meta;
  meta.set_type(meta_.type_);
  meta.set_collation_type(meta_.cs_type_);
  meta.set_scale(meta_.scale_);
  if (OB_FAIL(datum_.to_obj(obj_param, meta))) {
    LOG_WARN("failed to transform datum to obj", K(ret));
  } else {
    obj_param.set_accuracy(accuracy_);
    obj_param.set_result_flag(res_flags_);
    obj_param.set_param_flag(flag_);
    obj_param.set_param_meta();
  }
  return ret;
}

DEF_TO_STRING(ObToStringDatum)
{
  ObObj obj;
  int ret = d_.to_obj(obj, e_.obj_meta_);
  UNUSED(ret);
  return obj.to_string(buf, buf_len);
}

DEF_TO_STRING(ObToStringExpr)
{
  ObDatum* datum = NULL;
  int ret = e_.eval(c_, datum);
  UNUSED(ret);
  return ObToStringDatum(e_, *datum).to_string(buf, buf_len);
}

DEF_TO_STRING(ObToStringExprRow)
{
  int64_t pos = 0;
  J_ARRAY_START();
  for (int64_t i = 0; i < exprs_.count(); i++) {
    ObExpr* expr = exprs_.at(i);
    if (OB_LIKELY(expr != NULL)) {
      J_OBJ_START();
      J_KV(KP(expr));
      J_COMMA();
      pos += ObToStringExpr(c_, *expr).to_string(buf + pos, buf_len - pos);
      J_OBJ_END();
    } else {
      J_OBJ_START();
      J_OBJ_END();
    }
    if (i != exprs_.count() - 1) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  return pos;
}

}  // end namespace sql
}  // end namespace oceanbase
