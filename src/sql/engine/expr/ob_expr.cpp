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
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

// ObPrecision and ObLengthSemantics are union field in ObDatumMeta,
// need to be same size to make sure serialization works.
STATIC_ASSERT(sizeof(ObPrecision) == sizeof(ObLengthSemantics),
              "size of ObPrecision and ObLengthSemantics mismatch");

OB_SERIALIZE_MEMBER(ObDatumMeta, type_, cs_type_, scale_, precision_);


ObEvalCtx::ObEvalCtx(ObExecContext &exec_ctx, ObIAllocator *allocator)
  : frames_(exec_ctx.get_frames()),
    max_batch_size_(0),
    exec_ctx_(exec_ctx),
    tmp_alloc_(exec_ctx.get_eval_tmp_allocator()),
    datum_caster_(NULL),
    tmp_alloc_used_(exec_ctx.get_tmp_alloc_used()),
    batch_idx_(0),
    batch_size_(0),
    expr_res_alloc_((dynamic_cast<ObArenaAllocator*>(allocator) != NULL) ? (*(dynamic_cast<ObArenaAllocator*>(allocator))) : exec_ctx.get_eval_res_allocator())
{
}

ObEvalCtx::ObEvalCtx(ObEvalCtx &eval_ctx)
  : frames_(eval_ctx.frames_),
    max_batch_size_(eval_ctx.max_batch_size_),
    exec_ctx_(eval_ctx.exec_ctx_),
    tmp_alloc_(eval_ctx.tmp_alloc_),
    datum_caster_(NULL),
    tmp_alloc_used_(eval_ctx.tmp_alloc_used_),
    batch_idx_(eval_ctx.get_batch_idx()),
    batch_size_(eval_ctx.get_batch_size()),
    expr_res_alloc_(eval_ctx.expr_res_alloc_)
{
}

ObEvalCtx::~ObEvalCtx()
{
  if (NULL != datum_caster_) {
    datum_caster_->destroy();
    datum_caster_ = NULL;
  }
}

int ObEvalCtx::init_datum_caster()
{
  int ret = OB_SUCCESS;
  if (NULL == datum_caster_) {
    ObDatumCaster *datum_caster = NULL;
    void *buf = NULL;
    if (OB_ISNULL(buf = exec_ctx_.get_allocator().alloc(sizeof(ObDatumCaster)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (FALSE_IT(datum_caster = new(buf) ObDatumCaster())) {
    } else if (OB_FAIL(datum_caster->init(exec_ctx_))) {
      LOG_WARN("init datum caster failed", K(ret));
    } else {
      datum_caster_ = datum_caster;
    }
  }
  return ret;
}

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
              expr_ctx_id_,
              extra_);

  LST_DO_CODE(OB_UNIS_ENCODE,
              eval_info_off_,
              flag_,
              ser_eval_batch_func_,
              eval_flags_off_,
              pvt_skip_off_);

  if (OB_SUCC(ret)) {
    ObExprOperatorType type = T_INVALID;
    if (nullptr != extra_info_) {
      type = extra_info_->type_;
    }
    // Add a type before extra_info to determine whether extra_info is empty
    OB_UNIS_ENCODE(type)
    if (OB_FAIL(ret)) {
    } else if (T_INVALID != type) {
      OB_UNIS_ENCODE(*extra_info_);
    }
    OB_UNIS_ENCODE(dyn_buf_header_offset_)
  }

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
              expr_ctx_id_,
              extra_);

  LST_DO_CODE(OB_UNIS_DECODE,
              eval_info_off_,
              flag_,
              ser_eval_batch_func_,
              eval_flags_off_,
              pvt_skip_off_);
  if (0 == eval_info_off_ && OB_SUCC(ret)) {
    // compatible with 3.0, ObExprDatum::flag_ is ObEvalInfo
    eval_info_off_ = datum_off_ + sizeof(ObDatum);
  }

  if (OB_SUCC(ret)) {
    ObExprOperatorType type = T_INVALID;
    // Add a type before extra_info to determine whether extra_info is empty
    OB_UNIS_DECODE(type);
    if (OB_FAIL(ret)) {
    } else if (T_INVALID != type) {
      OZ (ObExprExtraInfoFactory::alloc(CURRENT_CONTEXT->get_arena_allocator(),
                                        type, extra_info_));
      CK (OB_NOT_NULL(extra_info_));
      OB_UNIS_DECODE(*extra_info_);
    }
  }

  if (OB_SUCC(ret)) {
    basic_funcs_ = ObDatumFuncs::get_basic_func(datum_meta_.type_, datum_meta_.cs_type_, datum_meta_.scale_,
                                                lib::is_oracle_mode(), obj_meta_.has_lob_header());
    CK(NULL != basic_funcs_);
  }
  if (is_batch_result()) {
    batch_idx_mask_ = UINT64_MAX;
  }
  OB_UNIS_DECODE(dyn_buf_header_offset_);
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
              expr_ctx_id_,
              extra_);

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              eval_info_off_,
              flag_,
              ser_eval_batch_func_,
              eval_flags_off_,
              pvt_skip_off_);

  ObExprOperatorType type = T_INVALID;
  if (nullptr != extra_info_) {
    type = extra_info_->type_;
  }
  OB_UNIS_ADD_LEN(type);
  if (T_INVALID != type) {
    OB_UNIS_ADD_LEN(*extra_info_);
  }
  OB_UNIS_ADD_LEN(dyn_buf_header_offset_);
  return len;
}

ObExpr::ObExpr()
    : magic_(MAGIC_NUM),
    type_(T_INVALID),
    datum_meta_(),
    obj_meta_(),
    max_length_(UINT32_MAX),
    obj_datum_map_(OBJ_DATUM_NULL),
    flag_(0),
    eval_func_(NULL),
    eval_batch_func_(NULL),
    inner_functions_(NULL),
    inner_func_cnt_(0),
    args_(NULL),
    arg_cnt_(0),
    parents_(NULL),
    parent_cnt_(0),
    frame_idx_(0),
    datum_off_(0),
    eval_info_off_(0),
    dyn_buf_header_offset_(0),
    res_buf_off_(0),
    res_buf_len_(0),
    eval_flags_off_(0),
    pvt_skip_off_(0),
    expr_ctx_id_(INVALID_EXP_CTX_ID),
    extra_(0),
    basic_funcs_(NULL),
    batch_idx_mask_(0),
    extra_info_(NULL)
{
  is_called_in_sql_ = 1;
}

char *ObExpr::alloc_str_res_mem(ObEvalCtx &ctx, const int64_t size, const int64_t idx) const
{
  char *mem = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObDynReserveBuf::supported(datum_meta_.type_))) {
    LOG_ERROR("unexpected alloc string result memory called", K(size), K(*this));
  } else {
    ObDynReserveBuf *drb = reinterpret_cast<ObDynReserveBuf *>(
        ctx.frames_[frame_idx_] + dyn_buf_header_offset_ + sizeof(ObDynReserveBuf) * idx);
    if (OB_LIKELY(drb->len_ >= size)) {
      mem = drb->mem_;
    } else {
      const int64_t alloc_size = next_pow2(size);
      if (OB_UNLIKELY(alloc_size > UINT32_MAX)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(size), K(alloc_size), K(ret));
      } else if (OB_ISNULL(mem = static_cast<char *>(ctx.alloc_expr_res(alloc_size)))) {
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
      LOG_DEBUG("extend expr result memory",
                K(ret), K(size), K(alloc_size), KP(this), KP(mem));
    }
  }
  return mem;
};


int ObExpr::eval_enumset(ObEvalCtx &ctx,
    const common::ObIArray<common::ObString> &str_values, const uint64_t cast_mode,
    common::ObDatum *&datum) const
{
  // performance critical, do not check %frame_idx_ and %frame again. (checked in CG)
  int ret = common::OB_SUCCESS;
  char *frame = ctx.frames_[frame_idx_];
  OB_ASSERT(NULL != frame);
  datum = (ObDatum *)(frame + datum_off_);
  ObEvalInfo *eval_info = (ObEvalInfo *)(frame + eval_info_off_);
  const common::ObObjTypeClass in_tc = args_[0]->obj_meta_.get_type_class();
  EvalEnumSetFunc eval_func;

  // do nothing for const/column reference expr or already evaluated expr
  if (is_batch_result()) {
    // Evaluate one datum within a batch.
    bool need_evaluate = false;
    if (eval_info->projected_){
      datum = datum + ctx.get_batch_idx();
    } else {
      ObBitVector* evaluated_flags = to_bit_vector(frame + eval_flags_off_);
      if (!eval_info->evaluated_) {
        evaluated_flags->reset(ctx.get_batch_size());
        reset_datums_ptr(frame, ctx.get_batch_size());
        eval_info->evaluated_ = true;
        eval_info->cnt_ = ctx.get_batch_size();
        eval_info->point_to_frame_ = true;
        eval_info->notnull_ = true;
      }
      if (!evaluated_flags->at(ctx.get_batch_idx())) {
        need_evaluate = true;
      }
      datum = reinterpret_cast<ObDatum *>(frame + datum_off_) + ctx.get_batch_idx();
      if (need_evaluate) {
        if (OB_FAIL(ObDatumCast::get_enumset_cast_function(in_tc, obj_meta_.get_type(), eval_func))) {
          LOG_WARN("failed to find eval_func", K(ret));
        } else {
          ret = eval_func(*this, str_values, cast_mode, ctx, *datum);
        }
        if (OB_SUCC(ret)) {
          evaluated_flags->set(ctx.get_batch_idx());
        } else {
          datum->set_null();
        }
        if (datum->is_null()) {
          eval_info->notnull_ = false;
        }
      }
    }
  } else {
    if (!eval_info->evaluated_) {
      if (datum->ptr_ != frame + res_buf_off_) {
        datum->ptr_ = frame + res_buf_off_;
      }
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
  }
  return ret;
}


void *ObExprStrResAlloc::alloc(const int64_t size)
{
  void *mem = expr_.get_str_res_mem(ctx_, off_ + size);
  if (NULL != mem) {
    mem = static_cast<char *>(mem) + off_;
    off_ += size;
  }
  return mem;
}

int ObDatumObjParam::from_objparam(const ObObjParam &objparam, ObIAllocator *allocator) {
  int ret = OB_SUCCESS;
  meta_ = ObDatumMeta(objparam.meta_.get_type(),
                      objparam.meta_.get_collation_type(),
                      objparam.meta_.get_scale());
  if (OB_UNLIKELY(objparam.is_ext_sql_array())) {
    if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("construct array param datum need allocator", K(ret));
    } else if (OB_FAIL(construct_array_param_datum(objparam, *allocator))) {
      LOG_WARN("construct array param datum", K(ret));
    }
  } else if (OB_FAIL(datum_.from_obj(objparam))) {
    LOG_WARN("fail to init datum", K(objparam), K(ret));
  }
  if (OB_SUCC(ret)) {
    accuracy_ = objparam.get_accuracy();
    res_flags_ = objparam.get_result_flag();
    flag_ = objparam.get_param_flag();
    if (objparam.has_lob_header()) {
      set_result_flag(HAS_LOB_HEADER_FLAG);
    }
  }

  return ret;
}

int ObDatumObjParam::to_objparam(common::ObObjParam &obj_param, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  ObObjMeta meta;
  meta.set_type(meta_.type_);
  meta.set_collation_type(meta_.cs_type_);
  meta.set_scale(meta_.scale_);
  if (res_flags_ & HAS_LOB_HEADER_FLAG) {
    meta.set_has_lob_header();
  }
  if (OB_UNLIKELY(meta_.is_ext_sql_array())) {
    if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("construct sql array obj need allocator", K(ret));
    } else if (OB_FAIL(construct_sql_array_obj(obj_param, *allocator))) {
      LOG_WARN("construct sql array obj failed", K(ret));
    }
  } else if (OB_FAIL(datum_.to_obj(obj_param, meta))) {
    LOG_WARN("failed to transform datum to obj", K(ret));
  }
  if (OB_SUCC(ret)) {
    obj_param.set_accuracy(accuracy_);
    obj_param.set_result_flag(res_flags_);
    obj_param.set_param_flag(flag_);
    obj_param.set_param_meta();
  }
  return ret;
}

int ObDatumObjParam::construct_array_param_datum(const ObObjParam &obj_param, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj_param.is_ext_sql_array())) {
    ObSqlDatumArray *datum_array = NULL;
    const ObSqlArrayObj *array_obj = reinterpret_cast<const ObSqlArrayObj*>(obj_param.get_ext());
    datum_array = ObSqlDatumArray::alloc(allocator, array_obj->count_);
    if (OB_ISNULL(datum_array)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate datum array buffer failed", K(ret));
    } else {
      datum_array->element_ = array_obj->element_;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < array_obj->count_; ++i) {
      // 每一个datum数据都有自己的类型，datum_array->element_不再准确，但是这个只适用于insert values场景
      ObObjDatumMapType obj_datum_map = ObDatum::get_obj_datum_map_type(
          array_obj->data_[i].get_type());
      if (OB_LIKELY(OBJ_DATUM_NULL != obj_datum_map)) {
        uint32_t def_res_len = ObDatum::get_reserved_size(obj_datum_map);
        if (OB_ISNULL(datum_array->data_[i].ptr_ = static_cast<char *>(allocator.alloc(def_res_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(def_res_len), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(datum_array->data_[i].from_obj(array_obj->data_[i]))) {
          LOG_WARN("fail to convert obj param", K(ret), K(array_obj->data_[i]));
        } else {
          LOG_DEBUG("construct datum array", K(array_obj->data_[i]), K(datum_array->data_[i]));
        }
      }
    }

    if (OB_SUCC(ret)) {
      //you can't use datum_.extend_obj_ here, datum_.extend_obj_m is just to asign an ObObj pointer
      //here you need to set the address of the constructed datum array
      datum_.set_int(reinterpret_cast<int64_t>(datum_array));
    }
  }
  return ret;
}

int ObDatumObjParam::construct_sql_array_obj(ObObjParam &obj_param, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(meta_.is_ext_sql_array())) {
    //you can't call get_ext() here, get_ext() in Datum is to get an ObObj pointer
    //here you need to get the address of the constructed datum array
    const ObSqlDatumArray *datum_array = reinterpret_cast<const ObSqlDatumArray *>(datum_.get_int());
    ObSqlArrayObj *array_obj = ObSqlArrayObj::alloc(allocator, datum_array->count_);
    if (OB_ISNULL(array_obj)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate array buffer failed", K(ret), K(datum_array->count_));
    } else {
      array_obj->element_ = datum_array->element_;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_array->count_; ++i) {
      if (OB_FAIL(datum_array->data_[i].to_obj(array_obj->data_[i],
                                               datum_array->element_.get_meta_type()))) {
        LOG_WARN("fail to convert obj param", K(ret), K(datum_array->data_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      obj_param.set_extend(reinterpret_cast<int64_t>(array_obj), T_EXT_SQL_ARRAY);
    }
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
  int64_t pos = 0;
  if (c_.is_vectorized() && c_.get_batch_size() <= 0) {
    // vectorize execution but batch_size not specified, do nothing
    J_OBJ_START();
    J_KV("Expr print ERROR", "vectorize execution but batch_size not specified");
    J_OBJ_END();
  } else if (c_.is_vectorized() && c_.get_batch_size() > 0 &&
               c_.get_batch_idx() >= c_.get_batch_size()) {
    // vectorize execution but batch_idx is not set correctly, do nothing
    J_OBJ_START();
    J_KV("Expr print ERROR", "vectorize execution but batch_idx is not set correctly");
    J_COMMA();
    J_KV("batch_idx", c_.get_batch_idx());
    J_OBJ_END();
  } else {
    ObDatum *datum = NULL;
    int ret = e_.eval(c_, datum);
    UNUSED(ret);
    pos = ObToStringDatum(e_, *datum).to_string(buf, buf_len);
  }
  return pos;
}

DEF_TO_STRING(ObToStringExprRow)
{
  int64_t pos = 0;
  J_ARRAY_START();
  for (int64_t i = 0; i < exprs_.count(); i++) {
    ObExpr *expr = exprs_.at(i);
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

void ObExpr::reset_datums_ptr(char *frame, const int64_t size) const
{
  // TODO bin.lb: on reset ptr from info->cnt_ to size if info->point_to_frame_ ?
  ObDatum *datum = reinterpret_cast<ObDatum *>(frame + datum_off_);
  ObDatum *datum_end = datum + size;
  char *ptr = frame + res_buf_off_;
  for (; datum < datum_end; datum += 1) {
    if (datum->ptr_ != ptr) {
      datum->ptr_ = ptr;
    }
    ptr += res_buf_len_;
  }
}

void ObExpr::reset_datum_ptr(char *frame, const int64_t size, const int64_t idx) const
{
  ObDatum *datum = reinterpret_cast<ObDatum *>(frame + datum_off_);
  datum += idx;
  char *ptr = frame + res_buf_off_ + (batch_idx_mask_ & idx) * res_buf_len_;
  if (datum->ptr_ != ptr && idx < size) {
    datum->ptr_ = ptr;
  }
}

int ObExpr::eval_one_datum_of_batch(ObEvalCtx &ctx, common::ObDatum *&datum) const
{
  int ret = OB_SUCCESS;
  char *frame = ctx.frames_[frame_idx_];
  ObEvalInfo *info = reinterpret_cast<ObEvalInfo *>(frame + eval_info_off_);
  bool need_evaluate = false;

  if (info->projected_ || NULL == eval_func_) {
    // do nothing
  } else if (!info->evaluated_) {
    need_evaluate = true;
    to_bit_vector(frame + eval_flags_off_)->reset(ctx.get_batch_size());
    reset_datums_ptr(frame, ctx.get_batch_size());
    info->evaluated_ = true;
    info->cnt_ = ctx.get_batch_size();
    info->point_to_frame_ = true;
    info->notnull_ = true;
  } else {
    ObBitVector *evaluated_flags = to_bit_vector(frame + eval_flags_off_);
    if (!evaluated_flags->at(ctx.get_batch_idx())) {
      need_evaluate = true;
    }
  }
  datum = reinterpret_cast<ObDatum *>(frame + datum_off_) + ctx.get_batch_idx();
  if (need_evaluate) {
    if (OB_UNLIKELY(need_stack_check_) && OB_FAIL(check_stack_overflow())) {
      SQL_LOG(WARN, "failed to check stack overflow", K(ret));
    } else {
      reset_datum_ptr(frame, ctx.get_batch_size(), ctx.get_batch_idx());
      ret = eval_func_(*this, ctx, *datum);
      CHECK_STRING_LENGTH((*this), (*datum));
      if (OB_SUCC(ret)) {
        ObBitVector *evaluated_flags = to_bit_vector(frame + eval_flags_off_);
        evaluated_flags->set(ctx.get_batch_idx());
      } else {
        datum->set_null();
      }
      if (datum->is_null()) {
        info->notnull_ = false;
      }
    }
  }

  return ret;
}

int ObExpr::do_eval_batch(ObEvalCtx &ctx,
                          const ObBitVector &skip,
                          const int64_t size) const
{

  int ret = OB_SUCCESS;
  char *frame = ctx.frames_[frame_idx_];
  ObEvalInfo *info = reinterpret_cast<ObEvalInfo *>(frame + eval_info_off_);
  bool need_evaluate = false;
  if (info->projected_ || NULL == eval_batch_func_) {
    // expr values is projected by child or has no evaluate func, do nothing.
  } else if (!info->evaluated_) {
    need_evaluate = true;
    to_bit_vector(frame + eval_flags_off_)->reset(size);
  } else {
    // check all datum evaluated
    const ObBitVector *evaluated_vec = to_bit_vector(frame + eval_flags_off_);
    need_evaluate = !ObBitVector::bit_op_zero(
        skip, *evaluated_vec, size,
        [](const uint64_t l, const uint64_t r) { return ~(l | r); });
  }

  if (need_evaluate) {
    // Set ObDatum::ptr_ point to reserved buffer in the first evaluation.
    // FIXME bin.lb: maybe we can optimize this by ObEvalInfo::point_to_frame_
    if (!info->evaluated_) {
      reset_datums_ptr(frame, size);
      info->notnull_ = false;
      info->point_to_frame_ = true;
    }
    if (OB_UNLIKELY(need_stack_check_) && OB_FAIL(check_stack_overflow())) {
      SQL_LOG(WARN, "failed to check stack overflow", K(ret));
    } else {
      ret = (*eval_batch_func_)(*this, ctx, skip, size);
      if (OB_SUCC(ret)) {
        if (!info->evaluated_) {
          info->cnt_ = size;
          info->evaluated_ = true;
        }
        #ifndef NDEBUG
          if (is_oracle_mode() && (ob_is_string_tc(datum_meta_.type_) || ob_is_raw(datum_meta_.type_))) {
            ObDatum *datum = reinterpret_cast<ObDatum *>(frame + datum_off_);
            for (int64_t i = 0; i < size; i++) {
              if (!skip.contain(i) && 0 == datum[i].len_ && !datum[i].is_null()) {
                SQL_ENG_LOG(ERROR, "unexpected datum length", KPC(this));
              }
            }
          }
        #endif
      } else {
        ObDatum *datum = reinterpret_cast<ObDatum *>(frame + datum_off_);
        ObDatum *datum_end = datum + size;
        for (; datum < datum_end; datum += 1) {
          datum->set_null();
        }
      }
    }
  }
  return ret;
}

int expr_default_eval_batch_func(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 const ObBitVector &skip,
                                 const int64_t size)
{
  int ret = OB_SUCCESS;

  char *frame = ctx.frames_[expr.frame_idx_];
  ObBitVector *evaluated_flags = to_bit_vector(frame + expr.eval_flags_off_);
  ObDatum *datum = reinterpret_cast<ObDatum *>(frame + expr.datum_off_);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(size);
  bool got_null = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
    if (!skip.contain(i) && !evaluated_flags->contain(i)) {
      // set current evaluate index
      batch_info_guard.set_batch_idx(i);
      ret = expr.eval_func_(expr, ctx, datum[i]);
      CHECK_STRING_LENGTH(expr, datum[i]);
      evaluated_flags->set(i);
      if (datum[i].is_null()) {
        got_null = true;
      }
    }
  }
  // reset evaluate index
  if (got_null) {
    expr.get_eval_info(ctx).notnull_ = false;
  }
  return ret;
}

int eval_question_mark_func(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL plan ctx", K(ret));
  } else {
    const auto &param_store = ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store();
    if (expr.extra_ >= param_store.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param store idx", K(ret), K(expr), K(param_store.count()));
    } else {
      const ObObj &v = param_store.at(expr.extra_);
      if (v.get_type() != expr.datum_meta_.type_ && !v.is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("obj type miss match", K(ret), K(v), K(expr));
      } else if (OB_FAIL(expr_datum.from_obj(v, expr.obj_datum_map_))) {
        LOG_WARN("set obj to datum failed", K(ret));
      } else if (is_lob_storage(v.get_type()) &&
                 OB_FAIL(ob_adjust_lob_datum(v, expr.obj_meta_, expr.obj_datum_map_,
                                             ctx.exec_ctx_.get_allocator(), expr_datum))) {
        LOG_WARN("adjust lob datum failed", K(ret), K(v.get_meta()), K(expr.obj_meta_));
      }
    }
  }
  return ret;
}

int eval_assign_question_mark_func(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session) || OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(session));
  } else {
    ObCastMode cast_mode = CM_NONE;
    const auto &param_store = ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store();
    if (expr.extra_ >= param_store.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param store idx", K(ret), K(expr), K(param_store.count()));
    } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(
                       session->get_stmt_type(), session, cast_mode))) {
      LOG_WARN("get default cast mode failed", K(ret));
    } else {
      ObObj dst_obj;
      ObDatumObjParam datum_param;
      const ObObj &v = param_store.at(expr.extra_);
      const common::ObObjMeta dst_meta = expr.obj_meta_;
      ObIAllocator &allocator = ctx.exec_ctx_.get_allocator();
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
      cast_mode = lib::is_oracle_mode() ?
      (cast_mode | CM_CHARSET_CONVERT_IGNORE_ERR) : (cast_mode | CM_COLUMN_CONVERT);
      if (ctx.exec_ctx_.get_physical_plan_ctx()->is_ignore_stmt()) {
        cast_mode = cast_mode | CM_WARN_ON_FAIL | CM_CHARSET_CONVERT_IGNORE_ERR;
      }
      if ((v.is_blob() || dst_meta.is_blob()) && lib::is_oracle_mode()) {
        cast_mode |= CM_ENABLE_BLOB_CAST;
      }
      ObCastCtx cast_ctx(&allocator, &dtc_params, cast_mode, dst_meta.get_collation_type());
      if (OB_FAIL(ObObjCaster::to_type(dst_meta.get_type(), cast_ctx, v, dst_obj))) {
        LOG_WARN("failed to cast obj to dst type", K(ret), K(v), K(dst_meta));
      } else if (OB_FAIL(datum_param.alloc_datum_reserved_buff(dst_meta, allocator))) {
        LOG_WARN("alloc datum reserved buffer failed", K(ret));
      } else if (OB_FAIL(datum_param.from_objparam(dst_obj, &allocator))) {
        LOG_WARN("fail to convert obj param", K(ret), K(dst_obj));
      } else {
        expr_datum = datum_param.datum_;
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
