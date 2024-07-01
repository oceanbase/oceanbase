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
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_uniform_vector.h"
#include "share/vector/ob_discrete_vector.h"
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
    LST_DO_CODE(OB_UNIS_ENCODE,
                vector_header_off_,
                offset_off_,
                len_arr_off_,
                cont_buf_off_,
                null_bitmap_off_,
                vec_value_tc_,
                ser_eval_vector_func_);
    OB_UNIS_ENCODE(local_session_var_id_);
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
    basic_funcs_ = ObDatumFuncs::get_basic_func(datum_meta_.type_, datum_meta_.cs_type_,
                                                datum_meta_.scale_, lib::is_oracle_mode(),
                                                obj_meta_.has_lob_header(), datum_meta_.precision_);
    CK(NULL != basic_funcs_);
  }
  if (is_batch_result()) {
    batch_idx_mask_ = UINT64_MAX;
  }
  OB_UNIS_DECODE(dyn_buf_header_offset_);
  LST_DO_CODE(OB_UNIS_DECODE,
              vector_header_off_,
              offset_off_,
              len_arr_off_,
              cont_buf_off_,
              null_bitmap_off_,
              vec_value_tc_,
              ser_eval_vector_func_);

  OB_UNIS_DECODE(local_session_var_id_);
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
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              vector_header_off_,
              offset_off_,
              len_arr_off_,
              cont_buf_off_,
              null_bitmap_off_,
              vec_value_tc_,
              ser_eval_vector_func_);

  OB_UNIS_ADD_LEN(local_session_var_id_);
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
    eval_vector_func_(NULL),
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
    extra_info_(NULL),
    vector_header_off_(UINT32_MAX),
    offset_off_(UINT32_MAX),
    len_arr_off_(UINT32_MAX),
    cont_buf_off_(UINT32_MAX),
    null_bitmap_off_(UINT32_MAX),
    vec_value_tc_(MAX_VEC_TC),
    local_session_var_id_(OB_INVALID_INDEX_INT64)
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
    if (eval_info->projected_) {
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
        if (enable_rich_format()) {
          ret = init_vector(ctx, VEC_UNIFORM, ctx.get_batch_size());
        }
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
  if (ob_is_user_defined_sql_type(meta_.type_) &&
      meta_.cs_type_ == CS_TYPE_INVALID) {
    // xmltype
    meta.set_collation_level(CS_LEVEL_EXPLICIT);
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
        uint32_t def_res_len = ObDatum::get_reserved_size(obj_datum_map, array_obj->data_[i].get_precision());
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

DEF_TO_STRING(ObExprArrayVecStringer)
{
  int64_t pos = 0;
  const char *payload = nullptr;
  int32_t payload_len = 0;
  ObDatum d;
  J_ARRAY_START();
  for (int i = 0; i < exprs_.count(); i++) {
    ObExpr *expr = exprs_.at(i);
    if (OB_LIKELY(expr != NULL)) {
      J_OBJ_START();
      J_KV(KP(expr));
      J_COMMA();
      if (expr->get_vector(ctx_)->is_null(ctx_.get_batch_idx())) {
        d.set_null();
      } else {
        expr->get_vector(ctx_)->get_payload(ctx_.get_batch_idx(), payload, payload_len);
        d.ptr_ = payload;
        d.len_ = payload_len;
      }
      pos += ObToStringDatum(*expr, d).to_string(buf + pos, buf_len - pos);
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

void ObExpr::reset_discretes_ptr(char *frame, const int64_t size, char** ptrs) const
{
  char *ptr = frame + res_buf_off_;
  for (int64_t i = 0; i < size; i += 1) {
    if (ptrs[i] != ptr) {
      ptrs[i] = ptr;
    }
    ptr += res_buf_len_;
  }
}


int ObExpr::eval_one_datum_of_batch(ObEvalCtx &ctx, common::ObDatum *&datum) const
{
  int ret = OB_SUCCESS;
  char *frame = ctx.frames_[frame_idx_];
  ObEvalInfo *info = reinterpret_cast<ObEvalInfo *>(frame + eval_info_off_);
  bool need_evaluate = false;

  if (info->projected_ || NULL == eval_func_ || info->evaluated_) {
    if (UINT32_MAX != vector_header_off_) {
      ret = cast_to_uniform(ctx.get_batch_size(), ctx);
    }
  }
  if (OB_FAIL(ret)) {
  } else if ((info->projected_ || NULL == eval_func_)) {
    // do nothing
  } else if (!info->evaluated_) {
    need_evaluate = true;
    to_bit_vector(frame + eval_flags_off_)->reset(ctx.get_batch_size());
    reset_datums_ptr(frame, ctx.get_batch_size());
    info->evaluated_ = true;
    info->cnt_ = ctx.get_batch_size();
    info->point_to_frame_ = true;
    info->notnull_ = true;
    if (enable_rich_format()) {
      ret = init_vector(ctx, VEC_UNIFORM, ctx.get_batch_size());
    } else if (UINT32_MAX != vector_header_off_) {
      get_vector_header(ctx).format_ = VEC_INVALID;
    }
  } else {
    ObBitVector *evaluated_flags = to_bit_vector(frame + eval_flags_off_);
    if (!evaluated_flags->at(ctx.get_batch_idx())) {
      need_evaluate = true;
    }
  }
  datum = reinterpret_cast<ObDatum *>(frame + datum_off_) + ctx.get_batch_idx();
  if (OB_SUCC(ret) && need_evaluate) {
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
      info->evaluated_ = true;
      info->cnt_ = size;
      if (enable_rich_format()) {
        ret = init_vector(ctx, VEC_UNIFORM, size);
      } else if (UINT32_MAX != vector_header_off_) {
        get_vector_header(ctx).format_ = VEC_INVALID;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(need_stack_check_) && OB_FAIL(check_stack_overflow())) {
      SQL_LOG(WARN, "failed to check stack overflow", K(ret));
    } else {
      if (OB_UNLIKELY(enable_rich_format())) {
        ret = (*eval_vector_func_)(*this, ctx, skip, EvalBound(size));
        // for shared expr, may be not use uniform format when first time eval expr
        // so, we should cast to uniform here
        if (OB_SUCC(ret)) {
          ret = cast_to_uniform(size, ctx);
        }
      } else {
        ret = (*eval_batch_func_)(*this, ctx, skip, size);
      }
      if (OB_SUCC(ret)) {
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
  } else {
    // no need evaluate more, cast to uniform for use rich format,
    // because this expr may calc by eval_vector
    if (UINT32_MAX != vector_header_off_) {
      ret = cast_to_uniform(size, ctx);
    }
  }
  return ret;
}

int ObExpr::cast_to_uniform(const int64_t size, ObEvalCtx &ctx) const
{
  int ret = OB_SUCCESS;
  VectorHeader &vec_header = get_vector_header(ctx);
  LOG_DEBUG("cast to uniform", K(this), K(*this), K(vec_header.format_), K(size), K(ctx), K(lbt()));
  if (VEC_INVALID == vec_header.format_) {
    // do nothing
  } else {
    ObIVector *vec = reinterpret_cast<ObIVector *>(vec_header.vector_buf_);
    ObDatum *datums = locate_batch_datums(ctx);
    switch(vec_header.format_) {
      case VEC_FIXED:{
        ObFixedLengthBase *fix_vec = static_cast<ObFixedLengthBase *>(vec);
        char *ptr = fix_vec->get_data();
        ObLength len = fix_vec->get_length();
        const ObBitVector *nulls = fix_vec->get_nulls();
        for (int i = 0; i < size; i++) {
          ObDatum *datum = datums + i;
          if (datum->ptr_ != ptr) {
            datum->ptr_ = ptr;
          }
          ptr += len;
          datum->pack_ = (nulls->at(i) ? 0 : len);
          datum->null_ = nulls->at(i);
        }
        OZ(init_vector(ctx, VEC_UNIFORM, size));
        break;
      }
      case VEC_DISCRETE: {
        ObDiscreteBase *disc_vec = static_cast<ObDiscreteBase *>(vec);
        char **ptrs = disc_vec->get_ptrs();
        int32_t *lens = disc_vec->get_lens();
        const ObBitVector *nulls = disc_vec->get_nulls();
        for (int i = 0; i < size; i++) {
          ObDatum *datum = datums + i;
          datum->ptr_ = ptrs[i];
          datum->pack_ = (nulls->at(i) ? 0 : lens[i]);
          datum->null_ = nulls->at(i);
        }
        OZ(init_vector(ctx, VEC_UNIFORM, size));
        break;
      }
      case VEC_CONTINUOUS: {
        ObContinuousBase *cont_vec = static_cast<ObContinuousBase *>(vec);
        const uint32_t *offsets = cont_vec->get_offsets();
        const char *data = cont_vec->get_data();
        const ObBitVector *nulls = cont_vec->get_nulls();
        for (int i = 0; i < size; i++) {
          ObDatum *datum = datums + i;
          datum->ptr_ = data + offsets[i];
          datum->pack_ = (nulls->at(i) ? 0 : offsets[i + 1] - offsets[i]);
          datum->null_ = nulls->at(i);
        }
        OZ(init_vector(ctx, VEC_UNIFORM, size));
        break;
      }
      case VEC_UNIFORM:
      case VEC_UNIFORM_CONST : {
        ObUniformBase *uni_vec = static_cast<ObUniformBase *>(vec);
        ObDatum *src = uni_vec->get_datums();
        ObDatum *dst = locate_batch_datums(ctx);
        if (src != dst) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected the src and dst to be equal", K(ret), K(src), K(dst));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid vector format", K(vec_header.format_));
      }
    }
  }
  return ret;
}

int ObExpr::init_vector(ObEvalCtx &ctx,
                        const VectorFormat format,
                        const int64_t size,
                        const bool use_reserve_buf/*false*/) const
{
  int ret = OB_SUCCESS;
  VectorHeader &vec_header = get_vector_header(ctx);
  vec_header.set_format(format);
  char *vector_buf = vec_header.vector_buf_;
  const VecValueTypeClass value_tc = get_vec_value_tc();
  common::ObIVector *vec = NULL;
  if (OB_UNLIKELY(!batch_result_ && format != VEC_UNIFORM_CONST)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected format", K(ret), K(format), KPC(this));
  } else if (VEC_FIXED == format) {
    char *data = get_res_buf(ctx);
    ObBitVector &nulls = get_nulls(ctx);
    nulls.reset(size);
    switch(value_tc) {
      #define FIXED_VECTOR_INIT_SWITCH(value_tc) \
      case value_tc:                             \
        static_assert(sizeof(RTVectorType<VEC_FIXED, value_tc>) <= ObIVector::MAX_VECTOR_STRUCT_SIZE,\
                      "vector size exceeds MAX_VECTOR_STRUCT_SIZE");                                 \
        new(vector_buf)RTVectorType<VEC_FIXED, value_tc>(data, &nulls);       \
        break;
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTEGER);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_UINTEGER);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_FLOAT);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_DOUBLE);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_FIXED_DOUBLE);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_DATETIME);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_DATE);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIME);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_YEAR);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_UNKNOWN);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_BIT);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TZ);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TINY);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_YM);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_DS);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT32);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT64);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT128);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT256);
      FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT512);
      #undef FIXED_VECTOR_INIT_SWITCH
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid vector value type class", K(value_tc), K(format), K(ret));
    }
  } else if (VEC_CONTINUOUS == format) {
    char *data = get_continuous_vector_data(ctx);
    uint32_t *offsets = get_continuous_vector_offsets(ctx);
    ObBitVector &nulls = get_nulls(ctx);
    nulls.reset(size);
    switch(value_tc) {
      #define CONTINUOUS_VECTOR_INIT_SWITCH(value_tc)                                  \
      case value_tc:                                                                         \
        static_assert(sizeof(RTVectorType<VEC_CONTINUOUS, value_tc>)                   \
                      <= ObIVector::MAX_VECTOR_STRUCT_SIZE, "vector size exceeds MAX_VECTOR_STRUCT_SIZE"); \
        new(vector_buf)RTVectorType<VEC_CONTINUOUS, value_tc>(                         \
                               offsets, data, &nulls);                       \
        break;
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_STRING);
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_RAW);
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_LOB);
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_JSON);
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_GEO);
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_UDT);
      CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
      #undef CONTINUOUS_VECTOR_INIT_SWITCH
      default:
        ret = OB_INVALID_ARGUMENT;
        SQL_LOG(WARN, "invalid vector value type class", K(value_tc), K(format), K(ret));
    }
  } else if (VEC_DISCRETE == format) {
    char **ptrs = get_discrete_vector_ptrs(ctx);
    int32_t *lens = get_discrete_vector_lens(ctx);
    ObBitVector &nulls = get_nulls(ctx);
    nulls.reset(size);
    if (use_reserve_buf) {
      reset_discretes_ptr(ctx.frames_[frame_idx_], size, get_discrete_vector_ptrs(ctx));
    }
    switch(value_tc) {
      #define DISCRETE_VECTOR_INIT_SWITCH(value_tc)                                          \
      case value_tc:                                                                         \
        static_assert(sizeof(RTVectorType<VEC_DISCRETE, value_tc>)                       \
                      <= ObIVector::MAX_VECTOR_STRUCT_SIZE, "vector size exceeds MAX_VECTOR_STRUCT_SIZE"); \
        new(vector_buf)RTVectorType<VEC_DISCRETE, value_tc>(                             \
                               lens, ptrs, &nulls);                          \
        break;
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_STRING);
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_RAW);
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_LOB);
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_JSON);
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_GEO);
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_UDT);
      DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
      #undef DISCRETE_VECTOR_INIT_SWITCH
      default:
        ret = OB_INVALID_ARGUMENT;
        SQL_LOG(WARN, "invalid vector value type class", K(value_tc), K(format), K(ret));
    }
  } else if (VEC_UNIFORM == format) {
    if (use_reserve_buf) {
      reset_datums_ptr(ctx.frames_[frame_idx_], size);
    }
    switch(value_tc) {
      #define UNIFORM_VECTOR_INIT_SWITCH(value_tc)                 \
      case value_tc:                                                \
        static_assert(sizeof(RTVectorType<VEC_UNIFORM, value_tc>) <= ObIVector::MAX_VECTOR_STRUCT_SIZE, \
                      "vector size exceeds MAX_VECTOR_STRUCT_SIZE");       \
        new(vector_buf)RTVectorType<VEC_UNIFORM, value_tc>(         \
                               locate_batch_datums(ctx), &get_eval_info(ctx));                  \
        break;
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_NULL);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_INTEGER);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_UINTEGER);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_FLOAT);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DOUBLE);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_FIXED_DOUBLE);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DATETIME);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DATE);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_TIME);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_YEAR);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_UNKNOWN);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_BIT);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TZ);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TINY);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_YM);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_DS);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT32);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT64);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT128);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT256);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT512);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_STRING);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_RAW);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_LOB);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_JSON);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_GEO);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_UDT);
      UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
      #undef UNIFORM_VECTOR_INIT_SWITCH
      default:
        ret = OB_INVALID_ARGUMENT;
        SQL_LOG(WARN, "invalid vector value type class", K(value_tc), K(format), K(ret));
    }
  } else if (VEC_UNIFORM_CONST == format) {
    if (use_reserve_buf) {
      reset_datums_ptr(ctx.frames_[frame_idx_], 1/*size*/);
    }
    ObDatum *datums = locate_batch_datums(ctx);
    ObEvalInfo *eval_info = &get_eval_info(ctx);
    ret = vec_header.init_uniform_const_vector(get_vec_value_tc(), datums, eval_info);
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "invalid vector format", K(format), K(ret));
  }
  if (OB_SUCC(ret)) {
    ObVectorBase *vector = reinterpret_cast<ObVectorBase *> (vector_buf);
    vector->set_max_row_cnt(size);
  }
  return ret;
}

int VectorHeader::init_uniform_const_vector(VecValueTypeClass vec_value_tc,
                                            ObDatum *datum,
                                            ObEvalInfo *eval_info)
{
  int ret = OB_SUCCESS;
  format_ = VEC_UNIFORM_CONST;
  switch(vec_value_tc) {
    #define UNIFORM_CONST_VECTOR_INIT_SWITCH(value_tc)                  \
    case value_tc:                                                \
      new(vector_buf_)RTVectorType<VEC_UNIFORM_CONST, value_tc>(datum, eval_info); \
      break;
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_NULL);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_INTEGER);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_UINTEGER);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_FLOAT);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DOUBLE);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_FIXED_DOUBLE);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DATETIME);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DATE);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_TIME);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_YEAR);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_UNKNOWN);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_BIT);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TZ);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TINY);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_YM);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_DS);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT32);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT64);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT128);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT256);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT512);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_STRING);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_RAW);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_LOB);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_JSON);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_GEO);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_UDT);
    UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
    #undef UNIFORM_CONST_VECTOR_INIT_SWITCH
    default:
      ret = OB_INVALID_ARGUMENT;
      SQL_LOG(WARN, "invalid vector value type class", K(vec_value_tc), K(ret));
  }

  return ret;
}

int ObExpr::eval_vector(ObEvalCtx &ctx,
                        const ObBitVector &skip,
                        const EvalBound &bound) const
{
  int ret = common::OB_SUCCESS;
  #define BATCH_SIZE() batch_result_ ? bound.batch_size() : 1
  //TODO shengle CHECK_BOUND(bound); check skip and all_rows_active wheth match
  ObEvalInfo &info = get_eval_info(ctx);
  char *frame = ctx.frames_[frame_idx_];
  int64_t const_skip = 1;
  if (skip.accumulate_bit_cnt(bound) < bound.range_size()) {
    const_skip = 0;
  }
  const ObBitVector *rt_skip = batch_result_ ? &skip : to_bit_vector(&const_skip);
  bool need_evaluate = false;
  // in old operator, rowset_v2 expr eval param use eval_vector,
  // the param expr has been evaluated by old eval_batch interface,
  // and not init vector, here we should init vector
  if (VEC_INVALID == get_format(ctx)
      && (info.projected_ || true == info.evaluated_ || NULL == eval_vector_func_)) {
    ret = init_vector(ctx, batch_result_ ? VEC_UNIFORM : VEC_UNIFORM_CONST, BATCH_SIZE());
  }
  if (OB_FAIL(ret)) {
  } else if ((batch_result_ && info.projected_) || NULL == eval_batch_func_
             || (!batch_result_ && info.evaluated_)) {
    // expr values is projected by child or has no evaluate func, do nothing.
  } else if (!info.evaluated_) {
    // if const_skip == 1, no need to evaluated expr, just `init_vector`
    need_evaluate = batch_result_ || (const_skip == 0);
    get_evaluated_flags(ctx).reset(BATCH_SIZE());
    info.notnull_ = false;
    info.point_to_frame_ = true;
    info.evaluated_ = batch_result_ ? true : false;
    VectorFormat format = (expr_default_eval_vector_func == eval_vector_func_ && is_batch_result())
                          ? VEC_UNIFORM
                          : get_default_res_format();
    ret = init_vector_for_write(ctx, format, BATCH_SIZE());
  } else {
    // check all datum evaluated
    const ObBitVector &evaluated_vec = get_evaluated_flags(ctx);
    need_evaluate = !ObBitVector::bit_op_zero(
        *rt_skip, evaluated_vec, BATCH_SIZE(),
        [](const uint64_t l, const uint64_t r) { return ~(l | r); });
  }
  LOG_DEBUG("need evaluate", K(need_evaluate));
  if (OB_SUCC(ret) && need_evaluate) {
    if (OB_UNLIKELY(need_stack_check_) && OB_FAIL(check_stack_overflow())) {
      SQL_LOG(WARN, "failed to check stack overflow", K(ret));
    } else if (OB_FAIL(
                 (*eval_vector_func_)(*this, ctx, *rt_skip, batch_result_ ? bound : EvalBound(1)))) {
      set_all_null(ctx, BATCH_SIZE());
    } else {
      info.evaluated_ = true;
    }
  }

  return ret;
}

void ObExpr::set_all_null(ObEvalCtx &ctx, const int64_t size) const {
  if (is_uniform_format(get_format(ctx))) {
    char *frame = ctx.frames_[frame_idx_];
    ObDatum *datum = reinterpret_cast<ObDatum *>(frame + datum_off_);
    ObDatum *datum_end = datum + size;
    for (; datum < datum_end; datum += 1) {
      datum->set_null();
    }
  } else {
    get_nulls(ctx).set_all(size);
  }
  get_vector(ctx)->set_has_null();
}

int expr_default_eval_batch_func(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 const ObBitVector &skip,
                                 const EvalBound &bound)
{
  int ret = OB_SUCCESS;

  char *frame = ctx.frames_[expr.frame_idx_];
  ObBitVector *evaluated_flags = to_bit_vector(frame + expr.eval_flags_off_);
  ObDatum *datum = reinterpret_cast<ObDatum *>(frame + expr.datum_off_);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(bound.batch_size());
  bool got_null = false;
  for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
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

int expr_default_eval_batch_func(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 const ObBitVector &skip,
                                 const int64_t size)
{
  return expr_default_eval_batch_func(expr, ctx, skip, EvalBound(size));
}

int expr_default_eval_vector_func(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (!expr.batch_result_) {
    ObDatum *datum = NULL;
    ret = expr.eval(ctx, datum);
  } else if (OB_LIKELY(bound.is_full_size())) {
    ret = (*expr.eval_batch_func_)(expr, ctx, skip, bound.batch_size());
  } else {
    ret = expr_default_eval_batch_func(expr, ctx, skip, bound);
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
      ObAccuracy res_acc;
      if (dst_meta.is_decimal_int()) {
        res_acc.scale_ = expr.datum_meta_.scale_;
        res_acc.precision_ = expr.datum_meta_.precision_;
        cast_ctx.res_accuracy_ = &res_acc;
      }
      cast_ctx.exec_ctx_ = &ctx.exec_ctx_;
      if (OB_FAIL(ObObjCaster::to_type(dst_meta.get_type(), cast_ctx, v, dst_obj))) {
        LOG_WARN("failed to cast obj to dst type", K(ret), K(v), K(dst_meta));
      } else if (OB_FAIL(datum_param.alloc_datum_reserved_buff(
                          dst_meta, expr.datum_meta_.precision_, allocator))) {
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

template <typename VectorType>
int ToStrVectorHeader::to_string_helper(const VectorHeader &header, char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const VectorType *vector = reinterpret_cast<const VectorType *>(header.vector_buf_);
  J_COMMA();
  BUF_PRINTF("meta: ");
  pos += vector->to_string(buf + pos, buf_len - pos);
  BUF_PRINTF(", data: ");
  J_ARRAY_START();
  for (int64_t i = bound_.start(); i < bound_.end(); i++) {
    J_OBJ_START();
    if (nullptr != skip_ && skip_->at(i)) {
      BUF_PRINTF("idx: %ld, data: skipped", i);
    } else {
      BUF_PRINTF("idx: %ld, null: %d", i, vector->is_null(i));
      if (!vector->is_null(i)) {
        ObLength length = vector->get_length(i);
        BUF_PRINTF(", len: %d, ptr: %p, hex: ", length, vector->get_payload(i));
        hex_print(vector->get_payload(i), length, buf, buf_len, pos);
        ObDatum tmp_datum(vector->get_payload(i), length, vector->is_null(i));
        ObObj tmp_obj;
        if (OB_SUCCESS == tmp_datum.to_obj(tmp_obj, expr_.obj_meta_, expr_.obj_datum_map_)) {
          BUF_PRINTF(", value: ");
          pos += tmp_obj.to_string(buf + pos, buf_len - pos);
        }
      }
    }
    J_OBJ_END();
    if (VEC_UNIFORM_CONST == header.format_) {
      break;
    }
    if (i != bound_.end() - 1) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  return pos;
}

DEF_TO_STRING(ToStrVectorHeader)
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (NULL != skip_ && OB_FAIL(expr_.eval_vector(ctx_, *skip_, bound_))) {
    LOG_WARN("fail to eval_vector", K(ret));
  } else {
    const VectorHeader header = expr_.get_vector_header(ctx_);
    J_OBJ_START();
    switch (header.format_) {
    case VEC_FIXED: {
      J_KV("format", "VEC_FIXED");
      pos += to_string_helper<ObFixedLengthBase>(header, buf + pos, buf_len - pos);
      break;
    }
    case VEC_DISCRETE: {
      J_KV("format", "VEC_DISCRETE");
      pos += to_string_helper<ObDiscreteBase>(header, buf + pos, buf_len - pos);
      break;
    }
    case VEC_UNIFORM: {
      J_KV("format", "VEC_UNIFORM");
      ObDatum d;
      pos += to_string_helper<UniformFormat>(header, buf + pos, buf_len - pos);
      break;
    }
    case VEC_UNIFORM_CONST: {
      J_KV("format", "VEC_UNIFORM_CONST");
      pos += to_string_helper<ConstUniformFormat>(header, buf + pos, buf_len - pos);
      break;
    }
    default: J_KV(K_(header.format));
    }
    J_OBJ_END();
  }
  return pos;
}

} // end namespace sql
} // end namespace oceanbase
