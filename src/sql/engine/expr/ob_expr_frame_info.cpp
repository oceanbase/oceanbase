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
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "sql/engine/expr/ob_expr_extra_info_factory.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER(ObFrameInfo,
                    expr_cnt_,
                    frame_idx_,
                    frame_size_,
                    zero_init_pos_,
                    zero_init_size_,
                    use_rich_format_);

int ObExprFrameInfo::assign(const ObExprFrameInfo &other,
                            common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  need_ctx_cnt_ = other.need_ctx_cnt_;

  if (OB_FAIL(rt_exprs_.assign(other.rt_exprs_))) {
    LOG_WARN("failed to copy rt exprs", K(ret));
  } else if (OB_FAIL(const_frame_.assign(other.const_frame_))) {
    LOG_WARN("failed to copy const frame ptrs", K(ret));
  } else if (OB_FAIL(param_frame_.assign(other.param_frame_))) {
    LOG_WARN("failed to copy param frames", K(ret));
  } else if (OB_FAIL(dynamic_frame_.assign(other.dynamic_frame_))) {
    LOG_WARN("failed to copy dynamic frames", K(ret));
  } else if (OB_FAIL(datum_frame_.assign(other.datum_frame_))) {
    LOG_WARN("failed to copy datum frame", K(ret));
  } else if (OB_FAIL(const_frame_ptrs_.prepare_allocate(other.const_frame_ptrs_.count()))) {
    LOG_WARN("failed to prepare allocate array", K(ret));
  } else {
    char *frame_mem = NULL;
    int64_t item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    if (const_frame_.count() > 0 && const_frame_.at(0).use_rich_format_) {
      item_size += sizeof(VectorHeader);
    }
    for (int i = 0; OB_SUCC(ret) && i < other.const_frame_.count(); i++) {
      if (OB_ISNULL(frame_mem = (char *)allocator.alloc(other.const_frame_.at(i).frame_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        MEMCPY(frame_mem, other.const_frame_ptrs_.at(i), other.const_frame_.at(i).frame_size_);
        const_frame_ptrs_.at(i) = frame_mem;
      }
      // set datum ptr
      for (int j = 0; OB_SUCC(ret) && j < other.const_frame_.at(i).expr_cnt_; j++) {
          char *other_frame_mem = other.const_frame_ptrs_.at(i);
          int64_t other_frame_size = other.const_frame_.at(i).frame_size_;
          ObDatum *other_expr_datum = reinterpret_cast<ObDatum *>
                                      (other_frame_mem + j * item_size);
          ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                      (frame_mem + j * item_size);
          // 在mysql模式下, 空串len为0, 且ptr = NULL, 当ptr为NULL时, copy时不需要再改变ptr值
          if (NULL == other_expr_datum->ptr_) {
            // do nothing
          } else if ((other_expr_datum->ptr_ < other_frame_mem
                     || other_expr_datum->ptr_ >= other_frame_mem + other_frame_size)) {
            // if datum's ptr_ does not locate in frame_mem, it must be deep copied separately
            void *extra_buf = NULL;
            if (0 == other_expr_datum->len_) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("length is null and ptr not null", K(ret), K(*other_expr_datum));
            } else if (OB_ISNULL(extra_buf = allocator.alloc(other_expr_datum->len_))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate memory", K(ret));
            } else {
              MEMCPY(extra_buf, other_expr_datum->ptr_, other_expr_datum->len_);
              expr_datum->ptr_ = static_cast<char *>(extra_buf);
            }
          } else {
            expr_datum->ptr_
              = frame_mem + (other_expr_datum->ptr_ - other.const_frame_ptrs_.at(i));
          }
      } // end for
    } // for end
    // init const vector header
    if (const_frame_.count() > 0 && const_frame_.at(0).use_rich_format_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < other.rt_exprs_.count(); i++) {
        ObExpr &expr = other.rt_exprs_.at(i);
        if (IS_CONST_LITERAL(expr.type_)) {
          char *frame = const_frame_ptrs_.at(expr.frame_idx_);
          VecValueTypeClass vec_tc = expr.get_vec_value_tc();
          ObDatum *datum = reinterpret_cast<ObDatum*>(frame + expr.datum_off_);
          ObEvalInfo *eval_info = reinterpret_cast<ObEvalInfo *>(frame + expr.eval_info_off_);
          VectorHeader *vec_header = reinterpret_cast<VectorHeader *>(frame + expr.vector_header_off_);
          ret = vec_header->init_uniform_const_vector(vec_tc, datum, eval_info);
        }
      }
    }

    if (OB_SUCC(ret)) {
      // deep copy extra info & inner function ptrs
      for (int i = 0; OB_SUCC(ret) && i < other.rt_exprs_.count(); i++) {
        if (ObExprExtraInfoFactory::is_registered(other.rt_exprs_.at(i).type_)) {
          if (OB_ISNULL(other.rt_exprs_.at(i).extra_info_)) {
            // do nothing
          } else if (OB_FAIL(other.rt_exprs_.at(i).extra_info_->deep_copy(allocator,
                                                                other.rt_exprs_.at(i).type_,
                                                                rt_exprs_.at(i).extra_info_))) {
            LOG_WARN("failed to deep copy extra info", K(ret));
          }
        }

        // copy inner function ptrs
        if (OB_SUCC(ret) && other.rt_exprs_.at(i).inner_func_cnt_ > 0) {
          int64_t func_cnt = other.rt_exprs_.at(i).inner_func_cnt_;
          void *funcs_buf = NULL;
          if (OB_ISNULL(funcs_buf = allocator.alloc(sizeof(void *) * func_cnt))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          } else {
            MEMCPY(funcs_buf, other.rt_exprs_.at(i).inner_functions_, func_cnt * sizeof(void *));
            rt_exprs_.at(i).inner_functions_ = (void **)funcs_buf;
          }
        }
      } // end for
    }
  }
  // allocate args memory for all rt exprs
  if (OB_SUCC(ret)) {
    for (int i = 0; OB_SUCC(ret) && i < rt_exprs_.count(); i++) {
      if (rt_exprs_.at(i).arg_cnt_ > 0) {
        ObExpr **arg_buf = NULL;
        int64_t buf_size = rt_exprs_.at(i).arg_cnt_ * sizeof(ObExpr *);
        if (OB_ISNULL(arg_buf = (ObExpr **)allocator.alloc(buf_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          MEMSET(arg_buf, 0, buf_size);
          rt_exprs_.at(i).args_ = arg_buf;
        }
      }

      // allocate parents memory for all rt exprs
      if (OB_SUCC(ret) && rt_exprs_.at(i).parent_cnt_ > 0) {
        ObExpr **parent_buf = NULL;
        int64_t buf_size = rt_exprs_.at(i).parent_cnt_ * sizeof(ObExpr *);
        if (OB_ISNULL(parent_buf = (ObExpr **)allocator.alloc(buf_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          MEMSET(parent_buf, 0, buf_size);
          rt_exprs_.at(i).parents_ = parent_buf;
        }
      }
    } // end for
  }
  // replace arg ptrs and parent ptrs for all rt exprs
  for (int i = 0; OB_SUCC(ret) && i < other.rt_exprs_.count(); i++) {
    if (other.rt_exprs_.at(i).arg_cnt_ > 0) {
      for (int j = 0; OB_SUCC(ret) && j < other.rt_exprs_.at(i).arg_cnt_; j++) {
        int64_t pos = other.rt_exprs_.at(i).args_[j] - &(other.rt_exprs_.at(0));
        if (pos >= rt_exprs_.count() || pos < 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("rt expr pos is invalid", K(pos), KP(&other.rt_exprs_.at(0)),
                                             KP(other.rt_exprs_.at(i).args_[j]));
        } else {
          rt_exprs_.at(i).args_[j] = &rt_exprs_.at(pos);
        }
      } // end for
    }
    if (other.rt_exprs_.at(i).parent_cnt_ > 0) {
      for (int j = 0; OB_SUCC(ret) && j < other.rt_exprs_.at(i).parent_cnt_; j++) {
        int64_t pos = other.rt_exprs_.at(i).parents_[j] - &(other.rt_exprs_.at(0));
        if (pos >= rt_exprs_.count() || pos < 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("rt expr pos is invalid", K(pos), KP(&other.rt_exprs_.at(0)),
                                             KP(other.rt_exprs_.at(i).args_[j]));
        } else {
          rt_exprs_.at(i).parents_[j] = &rt_exprs_.at(pos);
        }
      } // end for
    }
  } // end for
  if (OB_SUCC(ret)) {
    // deep copy extra info & inner function ptrs
    for (int i = 0; OB_SUCC(ret) && i < other.rt_exprs_.count(); i++) {
      if (ObExprExtraInfoFactory::is_registered(other.rt_exprs_.at(i).type_)) {
        if (OB_ISNULL(other.rt_exprs_.at(i).extra_info_)) {
          // do nothing
        } else if (OB_FAIL(other.rt_exprs_.at(i).extra_info_->deep_copy(allocator,
                                                              other.rt_exprs_.at(i).type_,
                                                              rt_exprs_.at(i).extra_info_))) {
          LOG_WARN("failed to deep copy extra info", K(ret));
        }
      }

      // copy inner function ptrs
      if (OB_SUCC(ret) && other.rt_exprs_.at(i).inner_func_cnt_ > 0) {
        int64_t func_cnt = other.rt_exprs_.at(i).inner_func_cnt_;
        void *funcs_buf = NULL;
        if (OB_ISNULL(funcs_buf = allocator.alloc(sizeof(void *) * func_cnt))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          MEMCPY(funcs_buf, other.rt_exprs_.at(i).inner_functions_, func_cnt * sizeof(void *));
          rt_exprs_.at(i).inner_functions_ = (void **)funcs_buf;
        }
      }
    } // end for
  }

  return ret;
}

int ObExprFrameInfo::pre_alloc_exec_memory(ObExecContext &exec_ctx, ObIAllocator *allocator) const
{
  int ret = OB_SUCCESS;
  uint64_t frame_cnt = 0;
  char **frames = NULL;
  ObPhysicalPlanCtx *phy_ctx = NULL;
  if (NULL == (phy_ctx = exec_ctx.get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(phy_ctx), K(ret));
  } else if (OB_FAIL(alloc_frame(allocator != NULL ? *allocator : exec_ctx.get_allocator(),
                                 phy_ctx->get_param_frame_ptrs(),
                                 frame_cnt,
                                 frames))) {
    LOG_WARN("fail to alloc frame", K(ret));
  } else {
    exec_ctx.set_frame_cnt(frame_cnt);
    exec_ctx.set_frames(frames);
  }

  return ret;
}

#define ALLOC_FRAME_MEM(frame_info_arr)                          \
    for (int64_t i = 0; OB_SUCC(ret) && i < frame_info_arr.count(); i++) { \
      const ObFrameInfo &frame_info = frame_info_arr.at(i); \
      char *frame_mem = static_cast<char *>(exec_allocator.alloc(frame_info.frame_size_)); \
      if (OB_ISNULL(frame_mem)) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        LOG_WARN("alloc memory failed", K(ret), K(frame_info.frame_size_)); \
      } else if (frame_info.zero_init_size_ > 0) { \
        MEMSET(frame_mem + frame_info.zero_init_pos_, 0, frame_info.zero_init_size_);\
      } \
      frames[frame_idx++] = frame_mem; \
    } \

// 分配frame内存, 并将所有frame指针按每个frame idx的序存放到frames数组中
// 1. const frame内存来自plan中共享的内存, 直接将plan中存放的指针拿来使用
// 2. param frame内存来自编译期参数化后生成, 这里可直接获取
// 3. dynamic frame和datum frame内存在这里进行预分配
int ObExprFrameInfo::alloc_frame(ObIAllocator &exec_allocator,
                                 const ObIArray<char *> &param_frame_ptrs,
                                 uint64_t &frame_cnt,
                                 char **&frames) const
{
  int ret = common::OB_SUCCESS;
  frame_cnt = const_frame_ptrs_.count()
              + param_frame_.count()
              + dynamic_frame_.count()
              + datum_frame_.count();
  if (frame_cnt == 0) {
    // do nothing
  } else if (NULL == (frames = static_cast<char **>(exec_allocator.alloc(
                                         frame_cnt * sizeof(char *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(frame_cnt));
  } else {
    int64_t frame_idx = 0; //frame idx
    OB_ASSERT(const_frame_ptrs_.count() == const_frame_.count());
    // Param frame size maybe small than param frame ptrs, because the pre-calculated
    // exprs may be evaluate again in plan cache constraints check after param store extended.
    // After param_frame_ptrs from physical_plan_ctx is reset in ObPlanCacheValue::choose_plan,
    // can check OB_ASSERT(param_frame_.empty() || param_frame_ptrs.count() == param_frame_.count());
    OB_ASSERT(param_frame_.empty()
              || param_frame_ptrs.count() >= param_frame_.count());
    for (int64_t i = 0 ; i < const_frame_ptrs_.count(); i++) {
      frames[frame_idx++] = const_frame_ptrs_.at(i);
    }
    if (!param_frame_.empty()) {
      for (int64_t i = 0 ; i < param_frame_.count(); i++) {
        frames[frame_idx++] = param_frame_ptrs.at(i);
      }
    }
    int64_t begin_idx = frame_idx;
    ALLOC_FRAME_MEM(dynamic_frame_);
    //for subquery core
    //提前将frame中的datum置为null
    int64_t item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    if (dynamic_frame_.count() > 0 && dynamic_frame_.at(0).use_rich_format_) {
      item_size += sizeof(VectorHeader);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dynamic_frame_.count(); ++i) {
      char *cur_frame = frames[begin_idx + i];
      for (int64_t j = 0; j < dynamic_frame_.at(i).expr_cnt_; ++j) {
        ObDatum *datum = reinterpret_cast<ObDatum *>(cur_frame + j * item_size);
        datum->set_null();
      }
    }
    ALLOC_FRAME_MEM(datum_frame_);
  }

  return ret;
}
#undef ALLOC_FRAME_MEM

OB_SERIALIZE_MEMBER(ObEmptyExpr);

int ObExprFrameInfo::get_expr_idx_in_frame(ObExpr *expr, int64_t &expr_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame info is null or expr is null", K(ret));
  } else {
    const ObIArray<ObExpr> *array = &rt_exprs_;
    int64_t idx = 0;
    if (OB_UNLIKELY(NULL == array || array->empty() || expr < &array->at(0)
                    || (idx = expr - &array->at(0) + 1) > array->count())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "expr not in array", K(ret), KP(array), KP(idx), KP(expr));
    } else {
      expr_idx = idx;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExprFrameInfo)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("serialize expr frame info", KP(buf), K(buf_len), K(pos), K(*this));
  OB_UNIS_ENCODE(need_ctx_cnt_);
  ObIArray<ObExpr> *seri_arr_bak = ObExpr::get_serialize_array();
  ObExpr::get_serialize_array() = const_cast<ObArray<ObExpr> *>(&rt_exprs_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, rt_exprs_.count()))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (nullptr == ObExpr::get_serialize_array()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialize array is null", K(ret), K(pos), K(rt_exprs_.count()));
  } else {
    LOG_TRACE("get serialize array", K(ObExpr::get_serialize_array()), K(rt_exprs_.count()));
    for (int64_t i = 0; i < rt_exprs_.count() && OB_SUCC(ret); ++i) {
      const ObExpr &expr = rt_exprs_.at(i);
      if (OB_FAIL(expr.serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize expr", K(ret), K(i), K(rt_exprs_.count()));
      }
    }
  }
  OB_UNIS_ENCODE(const_frame_ptrs_.count());
  OZ(ObPxTreeSerializer::serialize_frame_info(buf, buf_len, pos, const_frame_,
                                              const_frame_ptrs_.get_data(),
                                              const_frame_ptrs_.count()));
  OB_UNIS_ENCODE(param_frame_);
  OB_UNIS_ENCODE(datum_frame_);
  OB_UNIS_ENCODE(dynamic_frame_);
  ObExpr::get_serialize_array() = seri_arr_bak;

  return ret;
}

OB_DEF_DESERIALIZE(ObExprFrameInfo)
{
  int ret = OB_SUCCESS;
  int32_t expr_cnt = 0;
  OB_UNIS_DECODE(need_ctx_cnt_);
  ObIArray<ObExpr> &exprs = const_cast<ObArray<ObExpr> &>(rt_exprs_);
  ObIArray<ObExpr> *seri_arr_bak = ObExpr::get_serialize_array();
  ObExpr::get_serialize_array() = &exprs;
  if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &expr_cnt))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(exprs.prepare_allocate(expr_cnt))) {
    LOG_WARN("failed to prepare allocator expr", K(ret));
  } else {
    for (int64_t i = 0; i < expr_cnt && OB_SUCC(ret); ++i) {
      ObExpr &expr = exprs.at(i);
      if (OB_FAIL(expr.deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to serialize expr", K(ret));
      }
    }
  }

  int64_t const_frame_cnt = 0;
  char **frames = NULL;
  OB_UNIS_DECODE(const_frame_cnt);
  if (0 != const_frame_cnt) {
    CK(OB_NOT_NULL(frames = static_cast<char**>(
             allocator_.alloc(sizeof(char*) * const_frame_cnt))));
    OX(MEMSET(frames, 0, sizeof(char*) * const_frame_cnt));
  }
  OZ(ObPxTreeSerializer::deserialize_frame_info(buf, data_len, pos,
     allocator_, const_frame_, &const_frame_ptrs_, frames, const_frame_cnt));

  OB_UNIS_DECODE(param_frame_);
  OB_UNIS_DECODE(datum_frame_);
  OB_UNIS_DECODE(dynamic_frame_);
  ObExpr::get_serialize_array() = seri_arr_bak;
  if (const_frame_.count() > 0 && const_frame_.at(0).use_rich_format_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < rt_exprs_.count(); i++) {
      ObExpr &expr = rt_exprs_.at(i);
      if (IS_CONST_LITERAL(expr.type_)) {
        char *frame = const_frame_ptrs_.at(expr.frame_idx_);
        VecValueTypeClass vec_tc = expr.get_vec_value_tc();
        ObDatum *datum = reinterpret_cast<ObDatum*>(frame + expr.datum_off_);
        ObEvalInfo *eval_info = reinterpret_cast<ObEvalInfo *>(frame + expr.eval_info_off_);
        VectorHeader *vec_header = reinterpret_cast<VectorHeader *>(frame + expr.vector_header_off_);
        ret = vec_header->init_uniform_const_vector(vec_tc, datum, eval_info);
      }
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprFrameInfo)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  OB_UNIS_ADD_LEN(need_ctx_cnt_);
  ObIArray<ObExpr> &exprs = const_cast<ObArray<ObExpr> &>(rt_exprs_);
  int32_t expr_cnt = exprs.count();
  ObIArray<ObExpr> *seri_arr_bak = ObExpr::get_serialize_array();
  ObExpr::get_serialize_array() = &exprs;
  len += serialization::encoded_length_i32(expr_cnt);
  for (int64_t i = 0; i < exprs.count(); ++i) {
    ObExpr &expr = exprs.at(i);
    len += expr.get_serialize_size();
  }

  OB_UNIS_ADD_LEN(const_frame_ptrs_.count());
  len += ObPxTreeSerializer::get_serialize_frame_info_size(
         const_frame_, const_frame_ptrs_.get_data(), const_frame_ptrs_.count());
  LOG_DEBUG("trace end get ser expr frame info size", K(ret), K(len));

  OB_UNIS_ADD_LEN(param_frame_);
  OB_UNIS_ADD_LEN(datum_frame_);
  OB_UNIS_ADD_LEN(dynamic_frame_);
  ObExpr::get_serialize_array() = seri_arr_bak;

  return len;
}

int ObPreCalcExprFrameInfo::assign(const ObPreCalcExprFrameInfo &other,
                                   common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObExprFrameInfo::assign(other, allocator))) {
    LOG_WARN("fail to assign expr frame info", K(ret), K(other));
  } else if (OB_FAIL(pre_calc_rt_exprs_.prepare_allocate(other.pre_calc_rt_exprs_.count()))) {
    LOG_WARN("failed to prepare allocate rt exprs", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < other.pre_calc_rt_exprs_.count(); i++) {
    int64_t pos = other.pre_calc_rt_exprs_.at(i) - &(other.rt_exprs_.at(0));
    if (pos >= rt_exprs_.count() || pos < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("rt expr pos is invalid", K(pos), KP(&other.rt_exprs_.at(0)),
                                         KP(other.pre_calc_rt_exprs_.at(i)));
    } else {
      pre_calc_rt_exprs_.at(i) = &rt_exprs_.at(pos);
    }
  } // for end

  return ret;
}

int ObPreCalcExprFrameInfo::eval(ObExecContext &exec_ctx,
                                 ObIArray<ObDatumObjParam> &res_datum_params)
{
  int ret = OB_SUCCESS;
  res_datum_params.reuse();
  if (pre_calc_rt_exprs_.empty()) {
    /* do nothing */
  } else if (OB_FAIL(pre_alloc_exec_memory(exec_ctx))) {
    LOG_WARN("failed to pre alloc exec memory", K(ret));
  } else if (OB_FAIL(exec_ctx.init_expr_op(need_ctx_cnt_))) {
    LOG_WARN("failed to init expr op ctx", K(ret));
  } else if (OB_FAIL(do_normal_eval(exec_ctx, res_datum_params))) {
    LOG_WARN("do normal eval failed", K(ret));
  }
  // reset expr op anyway
  exec_ctx.reset_expr_op();
  return ret;
}

OB_INLINE int ObPreCalcExprFrameInfo::do_normal_eval(ObExecContext &exec_ctx,
                                                     ObIArray<ObDatumObjParam> &res_datum_params)
{
  int ret = OB_SUCCESS;
  ObEvalCtx eval_ctx(exec_ctx);
  ObDatum *res_datum = NULL;
  ObDatumObjParam datum_param;
  ObExpr *rt_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < pre_calc_rt_exprs_.count(); ++i) {
    if (OB_ISNULL(rt_expr = pre_calc_rt_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null rt exprs", K(ret));
    } else if (OB_FAIL(rt_expr->eval(eval_ctx, res_datum))) {
      LOG_WARN("failed to eval", K(ret));
    } else {
      datum_param.set_datum(*res_datum);
      datum_param.set_meta(rt_expr->datum_meta_);
      if (rt_expr->obj_meta_.has_lob_header()) {
        datum_param.set_result_flag(HAS_LOB_HEADER_FLAG);
      }
      if (OB_FAIL(res_datum_params.push_back(datum_param))) {
        LOG_WARN("failed to push back obj param", K(ret));
      }
    } // else end
  } // for end
  return ret;
}

OB_NOINLINE int ObPreCalcExprFrameInfo::do_batch_stmt_eval(ObExecContext &exec_ctx,
                                                           ObIArray<ObDatumObjParam> &res_datum_params)
{
  int ret = OB_SUCCESS;
  ObEvalCtx eval_ctx(exec_ctx);
  ObDatum *res_datum = NULL;
  ObDatumObjParam datum_param;
  int64_t group_cnt = exec_ctx.get_sql_ctx()->get_batch_params_count();
  ObSqlDatumArray *datum_array = nullptr;
  //construct sql array obj
  for (int64_t i = 0; OB_SUCC(ret) && i < pre_calc_rt_exprs_.count(); ++i) {
    ObExpr *rt_expr = NULL;
    if (OB_ISNULL(rt_expr = pre_calc_rt_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null rt exprs", K(ret));
    } else if (OB_ISNULL(datum_array = ObSqlDatumArray::alloc(exec_ctx.get_allocator(), group_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate array buffer failed", K(ret), K(group_cnt));
    } else {
      datum_array->element_.set_obj_type(rt_expr->datum_meta_.type_);
      datum_array->element_.set_collation_type(rt_expr->datum_meta_.cs_type_);
      datum_array->element_.set_scale(rt_expr->datum_meta_.scale_);
      const ObObjTypeClass &dst_tc = ob_obj_type_class(rt_expr->datum_meta_.type_);
      if (ObStringTC == dst_tc || ObTextTC == dst_tc) {
        datum_array->element_.set_length_semantics(rt_expr->datum_meta_.length_semantics_);
      } else {
        datum_array->element_.set_precision(rt_expr->datum_meta_.precision_);
      }
    }
    if (OB_SUCC(ret)) {
      datum_param.meta_.type_ = ObExtendType;
      datum_param.meta_.scale_ = T_EXT_SQL_ARRAY;
      if (OB_FAIL(datum_param.alloc_datum_reserved_buff(datum_param.meta_, // is_ext_sql_array
                                                        PRECISION_UNKNOWN_YET, // precision is unknown
                                                        exec_ctx.get_allocator()))) {
        LOG_WARN("alloc datum reserved buffer failed", K(ret));
      } else if (FALSE_IT(datum_param.set_sql_array_datum(datum_array))) {
        //always false, do nothing
      } else if (OB_FAIL(res_datum_params.push_back(datum_param))) {
        LOG_WARN("failed to push back obj param", K(ret));
      }
    } // else end
  } // for end
  //replace array param to the real param and eval the pre calc expr
  for (int64_t group_id = 0; OB_SUCC(ret) && group_id < group_cnt; ++group_id) {
    if (OB_FAIL(exec_ctx.get_physical_plan_ctx()->replace_batch_param_datum(group_id, 0,
                              exec_ctx.get_physical_plan_ctx()->get_datum_param_store().count()))) {
      LOG_WARN("replace batch param frame failed", K(ret));
    }
    //params of each group need to clear the datum evaluted flags before calc the pre_calc_expr
    //otherwise, the result of pre_calc_expr will be the result of the last group param
    clear_datum_evaluted_flags(exec_ctx.get_frames());
    for (int64_t i = 0; OB_SUCC(ret) && i < pre_calc_rt_exprs_.count(); ++i) {
      ObExpr *rt_expr = pre_calc_rt_exprs_.at(i);
      datum_array = res_datum_params.at(i).get_sql_datum_array();
      if (OB_FAIL(rt_expr->eval(eval_ctx, res_datum))) {
        LOG_WARN("failed to eval", K(ret));
      } else if (OB_FAIL(datum_array->data_[group_id].deep_copy(*res_datum,
                                                                exec_ctx.get_allocator()))) {
        LOG_WARN("deep copy datum failed", K(ret));
      } else {
        LOG_DEBUG("do batch stmt eval", K(datum_array->data_[group_id]), KPC(res_datum), KPC(rt_expr));
      }
    }
  }
  return ret;
}

void ObPreCalcExprFrameInfo::clear_datum_evaluted_flags(char **frames)
{
  int64_t datum_frame_idx = const_frame_ptrs_.count() + param_frame_.count() + dynamic_frame_.count();
  int64_t item_size = 0;
  if (datum_frame_.count() > 0 && datum_frame_.at(0).use_rich_format_) {
    item_size = sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader);
  } else {
    item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  }
  for (int64_t i = 0; i < datum_frame_.count(); ++i) {
    char *cur_frame = frames[datum_frame_idx + i];
    for (int64_t j = 0; j < datum_frame_.at(i).expr_cnt_; ++j) {
      char *datum_ptr = cur_frame + j * item_size;
      ObEvalInfo *eval_info = reinterpret_cast<ObEvalInfo *>(datum_ptr + sizeof(ObDatum));
      eval_info->clear_evaluated_flag();
    }
  }
}

int ObPreCalcExprFrameInfo::eval_expect_err(ObExecContext &exec_ctx,
                                            bool &all_eval_err)
{
  int ret = OB_SUCCESS;
  all_eval_err = true;
  if (pre_calc_rt_exprs_.empty()) {
    /* do nothing */
  } else if (OB_FAIL(pre_alloc_exec_memory(exec_ctx))) {
    LOG_WARN("failed to pre alloc exec memory", K(ret));
  } else if (OB_FAIL(exec_ctx.init_expr_op(need_ctx_cnt_))) {
    LOG_WARN("failed to init expr op ctx", K(ret));
  } else {
    ObEvalCtx eval_ctx(exec_ctx);
    ObDatum *res_datum = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && all_eval_err && i < pre_calc_rt_exprs_.count(); ++i) {
      if (OB_ISNULL(pre_calc_rt_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null rt exprs", K(ret));
      } else if (OB_LIKELY(OB_SUCCESS != pre_calc_rt_exprs_.at(i)->eval(eval_ctx, res_datum))) {
        // eval error is expected
      } else {
        all_eval_err = false;
      }// else end
    } // for end
    // reset expr op anyway
    exec_ctx.reset_expr_op();
  }
  return ret;
}

ObTempExprCtx::~ObTempExprCtx()
{
  if (expr_op_ctx_store_ != NULL) {
    ObExprOperatorCtx **it = expr_op_ctx_store_;
    ObExprOperatorCtx **it_end = &expr_op_ctx_store_[expr_op_size_];
    for (; it != it_end; ++it) {
      if (NULL != (*it)) {
        (*it)->~ObExprOperatorCtx();
      }
    }
    expr_op_ctx_store_ = NULL;
    expr_op_size_ = 0;
    frame_cnt_ = 0;
  }
}

int ObTempExpr::eval(ObExecContext &exec_ctx, const ObNewRow &row, ObObj &result) const
{
  int ret = OB_SUCCESS;
  ObDatum *res_datum = NULL;
  char **frames = NULL;
  ObTempExprCtx *temp_expr_ctx = NULL;
  OZ(exec_ctx.get_temp_expr_eval_ctx(*this, temp_expr_ctx));
  CK(OB_NOT_NULL(temp_expr_ctx));
  OX(ObSQLUtils::clear_expr_eval_flags(rt_exprs_.at(expr_idx_), *temp_expr_ctx));
  OZ(row_to_frame(row, *temp_expr_ctx));
  if (OB_SUCC(ret)) {
    ObTempExprCtxReplaceGuard exec_ctx_backup_guard(exec_ctx, *temp_expr_ctx);
    OZ(rt_exprs_.at(expr_idx_).eval(*temp_expr_ctx, res_datum));
    OZ(res_datum->to_obj(result, rt_exprs_.at(expr_idx_).obj_meta_));
    if (!exec_ctx.use_temp_expr_ctx_cache()) {
      temp_expr_ctx->~ObTempExprCtx();
    }
    LOG_DEBUG("temp expr result", K(result), K(row), K(rt_exprs_));
  }

  return ret;
}

int ObTempExpr::row_to_frame(const ObNewRow &row, ObTempExprCtx &temp_expr_ctx) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < idx_col_arr_.count(); i++) {
    const RowIdxColumnPair &idx_col = idx_col_arr_.at(i);
    ObObj &v = row.cells_[idx_col.idx_];
    ObExpr &expr = rt_exprs_.at(idx_col.expr_pos_);
    ObDatum &expr_datum = expr.locate_datum_for_write(temp_expr_ctx);
    if (v.get_type() != expr.datum_meta_.type_ && !v.is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj type miss match", K(ret), K(v), K(idx_col), K(row));
    } else if (OB_FAIL(expr_datum.from_obj(v, expr.obj_datum_map_))) {
      LOG_WARN("fail to from obj", K(v), K(idx_col), K(row), K(ret));
    } else if (is_lob_storage(v.get_type()) &&
               OB_FAIL(ob_adjust_lob_datum(v, expr.obj_meta_, expr.obj_datum_map_,
                                           temp_expr_ctx.exec_ctx_.get_allocator(), expr_datum))) {
      LOG_WARN("adjust lob datum failed", K(ret), K(v.get_meta()), K(expr.obj_meta_));
    }
  }

  return ret;
}

int ObTempExpr::deep_copy(ObIAllocator &allocator, ObTempExpr *&dst) const
{
  int ret = OB_SUCCESS;
  char *buf = static_cast<char *>(allocator.alloc(sizeof(ObTempExpr)));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem in temp expr deep copy", K(ret));
  }
  OX(dst = new(buf)ObTempExpr(allocator));
  OZ(dst->assign(*this, allocator));
  OX(dst->expr_idx_ = expr_idx_);
  OZ(dst->idx_col_arr_.assign(this->idx_col_arr_));

  return ret;
}

OB_SERIALIZE_MEMBER(RowIdxColumnPair, idx_, expr_pos_);
OB_SERIALIZE_MEMBER((ObTempExpr, ObExprFrameInfo), expr_idx_, idx_col_arr_);
}
}

