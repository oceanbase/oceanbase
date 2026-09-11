/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Definitions of the ObPxTreeSerializer templates declared in ob_px_util.h.
// Included only by the .cpp files that actually call them (ob_px_util.cpp
// itself, ob_dfo.cpp, ob_expr_frame_info.cpp, ob_das_task.cpp, ob_task.cpp):
// each calling TU implicitly instantiates what it uses, so the link no longer
// depends on unity-build grouping or a hand-maintained explicit-instantiation
// list.
#ifndef OCEANBASE_SQL_ENGINE_PX_OB_PX_TREE_SERIALIZER_IMPL_IPP_
#define OCEANBASE_SQL_ENGINE_PX_OB_PX_TREE_SERIALIZER_IMPL_IPP_

#include "sql/engine/px/ob_px_util.h"
#include "lib/utility/ob_serialization_helper.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

//serialize plan tree for engine3.0
template <bool SERIALIZE_PLAN_PART>
int ObPxTreeSerializer::serialize_frame_info(char *buf,
                                       int64_t buf_len,
                                       int64_t &pos,
                                       const ObIArray<ObFrameInfo> &all_frames,
                                       char **frames,
                                       const int64_t frame_cnt,
                                       bool no_ser_data/* = false*/)
{
  int ret = OB_SUCCESS;
  int64_t need_extra_mem_size = 0;
  if (SERIALIZE_PLAN_PART) {
    OB_UNIS_ENCODE(all_frames.count());
    for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
      OB_UNIS_ENCODE(all_frames.at(i));
    }
  }
  int64_t item_size = 0;
  if ((all_frames.count() > 0) && all_frames.at(0).use_rich_format_) {
    item_size = sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader);
  } else {
    item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  }
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    //TODO shengle seri can opt, only serialize: sizeof(ObDatum) + sizeof(ObEvalInfo)
    if (frame_info.frame_idx_ >= frame_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("frame index exceed frame count", K(ret), K(frame_cnt), K(frame_info.frame_idx_));
    } else {
      char *frame_buf = frames[frame_info.frame_idx_];
      int64_t expr_mem_size = no_ser_data ? 0 : frame_info.expr_cnt_ * item_size;
      OB_UNIS_ENCODE(expr_mem_size);
      if (pos + expr_mem_size > buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos),
          K(expr_mem_size), K(buf_len));
      } else if (!no_ser_data && 0 < expr_mem_size) {
        MEMCPY(buf + pos, frame_buf, expr_mem_size);
        pos += expr_mem_size;
      }
      for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
        ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                  (frame_buf + j * item_size);
        need_extra_mem_size += no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      }
    }
  }
  OB_UNIS_ENCODE(need_extra_mem_size);
  int64_t expr_datum_size = 0;
  int64_t ser_mem_size = 0;
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    char *frame_buf = frames[frame_info.frame_idx_];
    for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
      ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                (frame_buf + j * item_size);
      expr_datum_size = no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      OB_UNIS_ENCODE(expr_datum_size);
      if (pos + expr_datum_size > buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos),
          K(expr_datum_size), K(buf_len));
      } else if (0 < expr_datum_size) {
        // TODO: longzhong.wlz 这里可能有兼容性问题，暂时这样，后面看着改
        MEMCPY(buf + pos, expr_datum->ptr_, expr_datum_size);
        pos += expr_datum_size;
        ser_mem_size += expr_datum_size;
      }
    }
  }
  if (OB_SUCC(ret) && ser_mem_size != need_extra_mem_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: serialize size is not match", K(ret),
      K(ser_mem_size), K(need_extra_mem_size));
  }
  return ret;
}

template <bool SERIALIZE_PLAN_PART>
int ObPxTreeSerializer::serialize_expr_frame_info(char *buf,
                                       int64_t buf_len,
                                       int64_t &pos,
                                       ObExecContext &ctx,
                                       const ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("trace start ser expr frame info", K(ret), K(buf_len), K(pos));
  if (SERIALIZE_PLAN_PART) {
    int64_t need_ctx_cnt = expr_frame_info.need_ctx_cnt_;
    OB_UNIS_ENCODE(need_ctx_cnt);
  }
  // expr extra info
  ObExprExtraSerializeInfo expr_info;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  expr_info.current_time_ = &plan_ctx->get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx->get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx->get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx->get_last_refresh_scns();
  if (OB_SUCC(ret) && OB_FAIL(expr_info.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize expr extra info", K(ret));
  }
  // rt exprs
  if (SERIALIZE_PLAN_PART) {
    const ObIArray<ObExpr> &exprs = expr_frame_info.rt_exprs_;
    const int32_t expr_cnt = expr_frame_info.is_mark_serialize()
        ? expr_frame_info.ser_expr_marks_.count()
        : exprs.count();
    ObExpr::get_serialize_array() = &(const_cast<ObIArray<ObExpr> &>(exprs));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, expr_cnt))) {
      LOG_WARN("fail to encode op type", K(ret));
    } else if (nullptr == ObExpr::get_serialize_array()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("serialize array is null", K(ret), K(pos), K(expr_cnt));
    } else {
      if (!expr_frame_info.is_mark_serialize()) {
        LOG_TRACE("exprs normal serialization", K(expr_cnt));
        for (int64_t i = 0; i < expr_cnt && OB_SUCC(ret); ++i) {
          if (OB_FAIL(exprs.at(i).serialize(buf, buf_len, pos))) {
            LOG_WARN("failed to serialize expr", K(ret), K(i), K(exprs.at(i)));
          }
        }
      } else {
        LOG_TRACE("exprs mark serialization", K(expr_cnt), K(exprs.count()));
        for (int64_t i = 0; i < expr_cnt && OB_SUCC(ret); ++i) {
          if (expr_frame_info.ser_expr_marks_.at(i)) {
            if (OB_FAIL(exprs.at(i).serialize(buf, buf_len, pos))) {
              LOG_WARN("failed to serialize expr", K(ret), K(i), K(exprs.at(i)));
            }
          } else if (OB_FAIL(ObEmptyExpr::instance().serialize(buf, buf_len, pos))) {
            LOG_WARN("serialize empty expr failed", K(ret), K(i));
          }
        }
      }
    }
  }
  // frames
  int64_t frame_count = 0;
  char **frames = nullptr;
  if (ctx.get_ori_frame_cnt() != 0 && ctx.get_ori_frames() != nullptr) {
    frame_count = ctx.get_ori_frame_cnt();
    frames = ctx.get_ori_frames();
  } else {
    frame_count = ctx.get_frame_cnt();
    frames = ctx.get_frames();
  }
  OB_UNIS_ENCODE(frame_count);
  OZ(serialize_frame_info<SERIALIZE_PLAN_PART>(buf, buf_len, pos, expr_frame_info.const_frame_, frames, frame_count));
  OZ(serialize_frame_info<SERIALIZE_PLAN_PART>(buf, buf_len, pos, expr_frame_info.param_frame_, frames, frame_count));
  OZ(serialize_frame_info<SERIALIZE_PLAN_PART>(buf, buf_len, pos, expr_frame_info.dynamic_frame_, frames, frame_count));
  OZ(serialize_frame_info<SERIALIZE_PLAN_PART>(buf, buf_len, pos, expr_frame_info.datum_frame_, frames, frame_count, true));
  LOG_DEBUG("trace end ser expr frame info", K(ret), K(buf_len), K(pos));
  return ret;
}

template <bool DESERIALIZE_PLAN_PART>
int ObPxTreeSerializer::deserialize_frame_info(const char *buf,
                                       int64_t data_len,
                                       int64_t &pos,
                                       ObIAllocator &allocator,
                                       const ObIArray<ObFrameInfo> &all_frames,
                                       ObIArray<char *> *char_ptrs,
                                       char **&frames,
                                       int64_t &frame_cnt,
                                       bool no_deser_data)
{
  int ret = OB_SUCCESS;
  int64_t frame_info_cnt = all_frames.count();
  if (DESERIALIZE_PLAN_PART) {
    OB_UNIS_DECODE(frame_info_cnt);
    ObIArray<ObFrameInfo> &non_const_all_frames = const_cast<ObIArray<ObFrameInfo> &>(all_frames);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(non_const_all_frames.reserve(frame_info_cnt))) {
      LOG_WARN("failed to reserve const frame", K(ret));
    } else {
      ObFrameInfo frame_info;
      for (int64_t i = 0; i < frame_info_cnt && OB_SUCC(ret); ++i) {
        OB_UNIS_DECODE(frame_info);
        if (OB_FAIL(non_const_all_frames.push_back(frame_info))) {
          LOG_WARN("failed to push back frame", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && nullptr != char_ptrs && OB_FAIL(char_ptrs->reserve(frame_info_cnt))) {
    LOG_WARN("failed to reserve const frame", K(ret));
  }
  int64_t need_extra_mem_size = 0;
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    int64_t expr_mem_size = 0;
    OB_UNIS_DECODE(expr_mem_size);
    if (frame_info.frame_idx_ >= frame_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("frame index exceed frame count", K(ret), K(frame_cnt), K(frame_info.frame_idx_));
    } else if (0 < frame_info.expr_cnt_) {
      char *frame_buf = nullptr;
      if (nullptr == (frame_buf = static_cast<char*>(allocator.alloc(frame_info.frame_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate frame buf", K(ret));
      } else if (pos + expr_mem_size > data_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos),
          K(frame_info.frame_size_), K(data_len));
      } else {
        MEMSET(frame_buf, 0, frame_info.frame_size_);
        frames[frame_info.frame_idx_] = frame_buf;
        if (nullptr != char_ptrs && OB_FAIL(char_ptrs->push_back(frame_buf))) {
          LOG_WARN("failed to push back frame buf", K(ret));
        }
        if (!no_deser_data && 0 < expr_mem_size) {
          MEMCPY(frame_buf, buf + pos, expr_mem_size);
          pos += expr_mem_size;
        }
      }
    }
  }
  OB_UNIS_DECODE(need_extra_mem_size);
  int64_t expr_datum_size = 0;
  int64_t des_mem_size = 0;
  char *expr_datum_buf = nullptr;
  if (0 < need_extra_mem_size
      && nullptr == (expr_datum_buf = static_cast<char*>(allocator.alloc(need_extra_mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  }
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    int64_t item_size = 0;
    if (frame_info.use_rich_format_) {
      item_size = sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader);
    } else {
      item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    }
    char *frame_buf = frames[frame_info.frame_idx_];
    for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
      ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                (frame_buf + j * item_size);
      OB_UNIS_DECODE(expr_datum_size);
      if (pos + expr_datum_size > data_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos),
          K(expr_datum_size), K(data_len));
      } else if (0 == expr_datum_size) {
        // 对于该序列化数据，datum的len_为0， 前面已将ObDatum反序列化, 不需要再做其他处理
      } else {
        // TODO: longzhong.wlz 之前说这里有兼容性问题，后续一并处理，暂时先这样
        MEMCPY(expr_datum_buf, buf + pos, expr_datum_size);
        expr_datum->ptr_ = expr_datum_buf;
        pos += expr_datum_size;
        des_mem_size += expr_datum_size;
        expr_datum_buf += expr_datum_size;
      }
    }
  }
  if (OB_SUCC(ret) && des_mem_size != need_extra_mem_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: serialize size is not match", K(ret),
      K(des_mem_size), K(need_extra_mem_size));
  }
  return ret;
}

template <bool DESERIALIZE_PLAN_PART>
int ObPxTreeSerializer::deserialize_expr_frame_info(const char *buf,
                                       int64_t data_len,
                                       int64_t &pos,
                                       ObExecContext &ctx,
                                       const ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  int32_t expr_cnt = 0;
  if (DESERIALIZE_PLAN_PART) {
    int64_t need_ctx_cnt = 0;
    OB_UNIS_DECODE(need_ctx_cnt);
    const_cast<ObExprFrameInfo &>(expr_frame_info).need_ctx_cnt_ = need_ctx_cnt;
  }
  // deserialize expr extra info.
  ObExprExtraSerializeInfo expr_info;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  expr_info.current_time_ = &plan_ctx->get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx->get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx->get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx->get_last_refresh_scns();
  if (OB_SUCC(ret) && OB_FAIL(expr_info.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize expr extra info", K(ret));
  }

  if (DESERIALIZE_PLAN_PART) {
    ObIArray<ObExpr> &exprs = const_cast<ObArray<ObExpr> &>(expr_frame_info.rt_exprs_);
    ObExpr::get_serialize_array() = &(const_cast<ObIArray<ObExpr> &>(exprs));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &expr_cnt))) {
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
  }

  // frames
  int64_t frame_cnt = 0;
  char **frames = nullptr;
  const ObIArray<char*> *param_frame_ptrs = &plan_ctx->get_param_frame_ptrs();
  OB_UNIS_DECODE(frame_cnt);
  ObIArray<char *> *const_char_ptrs = DESERIALIZE_PLAN_PART ?
                              &(const_cast<ObExprFrameInfo &>(expr_frame_info)).const_frame_ptrs_ :
                              NULL;
  if (OB_FAIL(ret)) {
  } else if (nullptr == (frames = static_cast<char**>(
              ctx.get_allocator().alloc(sizeof(char*) * frame_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed allocate frames", K(ret));
  } else if (FALSE_IT(MEMSET(frames, 0, sizeof(char*) * frame_cnt))) {
  } else if (OB_FAIL(deserialize_frame_info<DESERIALIZE_PLAN_PART>(
      buf, data_len, pos, ctx.get_allocator(), expr_frame_info.const_frame_,
      const_char_ptrs, frames, frame_cnt))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else if (OB_FAIL(deserialize_frame_info<DESERIALIZE_PLAN_PART>(
      buf, data_len, pos, ctx.get_allocator(), expr_frame_info.param_frame_,
      const_cast<ObIArray<char*>*>(param_frame_ptrs), frames, frame_cnt))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else if (OB_FAIL(deserialize_frame_info<DESERIALIZE_PLAN_PART>(
      buf, data_len, pos, ctx.get_allocator(),expr_frame_info.dynamic_frame_,
      nullptr, frames, frame_cnt))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else if (OB_FAIL(deserialize_frame_info<DESERIALIZE_PLAN_PART>(
      buf, data_len, pos, ctx.get_allocator(),expr_frame_info.datum_frame_,
      nullptr, frames, frame_cnt, true))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else {
    ctx.set_frames(frames);
    ctx.set_frame_cnt(frame_cnt);
    // init const vector
    ObEvalCtx eval_ctx(ctx);
    const ObIArray<ObExpr> &exprs = expr_frame_info.rt_exprs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      const ObExpr &expr = exprs.at(i);
      if (expr.is_const_expr()
          && UINT32_MAX != expr.vector_header_off_
          && T_OP_ROW != expr.type_) {
        // set const expr vector format to VEC_INVALID to avoid const expr check failed
        expr.get_vector_header(eval_ctx).format_ = VEC_INVALID;
        ret = expr.init_vector(eval_ctx, VEC_UNIFORM_CONST, 1/*size*/);
      }
    }
  }
  return ret;
}

template <bool SERIALIZE_PLAN_PART>
int64_t ObPxTreeSerializer::get_serialize_frame_info_size(
                                       const ObIArray<ObFrameInfo> &all_frames,
                                       char **frames,
                                       int64_t frame_cnt,
                                       bool no_ser_data/* = false*/)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  int64_t need_extra_mem_size = 0;
  int64_t item_size = 0;
  if ((all_frames.count() > 0) && all_frames.at(0).use_rich_format_) {
    item_size = sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader);
  } else {
    item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  }
  if (SERIALIZE_PLAN_PART) {
    OB_UNIS_ADD_LEN(all_frames.count());
    for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
      OB_UNIS_ADD_LEN(all_frames.at(i));
    }
  }
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    if (frame_info.frame_idx_ >= frame_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("frame index exceed frame count", K(ret), K(frame_cnt), K(frame_info.frame_idx_));
    } else {
      int64_t expr_mem_size = no_ser_data ? 0 : frame_info.expr_cnt_ * item_size;
      OB_UNIS_ADD_LEN(expr_mem_size);
      len += expr_mem_size;
      char *frame_buf = frames[frame_info.frame_idx_];
      for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
        ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                  (frame_buf + j * item_size);
        need_extra_mem_size += no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      }
    }
  }
  OB_UNIS_ADD_LEN(need_extra_mem_size);
  int64_t expr_datum_size = 0;
  int64_t ser_mem_size = 0;
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    const ObFrameInfo &frame_info = all_frames.at(i);
    char *frame_buf = frames[frame_info.frame_idx_];
    for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
      ObDatum *expr_datum = reinterpret_cast<ObDatum *>
                                (frame_buf + j * item_size);
      expr_datum_size = no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      OB_UNIS_ADD_LEN(expr_datum_size);
      if (0 < expr_datum_size) {
        // 这里可能有兼容性问题，暂时这样，后面看着改
        len += expr_datum_size;
        ser_mem_size += expr_datum_size;
      }
    }
  }
  if (OB_SUCC(ret) && ser_mem_size != need_extra_mem_size) {
    LOG_ERROR("unexpected status: serialize size is not match", K(ret),
      K(ser_mem_size), K(need_extra_mem_size));
  }
  return len;
}

template <bool SERIALIZE_PLAN_PART>
int64_t ObPxTreeSerializer::get_serialize_expr_frame_info_size(
  ObExecContext &ctx,
  const ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (SERIALIZE_PLAN_PART) {
    int64_t need_ctx_cnt = expr_frame_info.need_ctx_cnt_;
    OB_UNIS_ADD_LEN(need_ctx_cnt);
  }
  ObExprExtraSerializeInfo expr_info;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  expr_info.current_time_ = &plan_ctx->get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx->get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx->get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx->get_last_refresh_scns();
  len += expr_info.get_serialize_size();

  if (SERIALIZE_PLAN_PART) {
    const ObIArray<ObExpr> &exprs = expr_frame_info.rt_exprs_;
    int32_t expr_cnt = expr_frame_info.is_mark_serialize()
        ? expr_frame_info.ser_expr_marks_.count()
        : exprs.count();
    ObExpr::get_serialize_array() = &(const_cast<ObIArray<ObExpr> &>(exprs));
    len += serialization::encoded_length_i32(expr_cnt);
    if (!expr_frame_info.is_mark_serialize()) {
      for (int64_t i = 0; i < expr_cnt; ++i) {
        len += exprs.at(i).get_serialize_size();
      }
    } else {
      for (int64_t i = 0; i < expr_cnt; ++i) {
        if (expr_frame_info.ser_expr_marks_.at(i)) {
          len += exprs.at(i).get_serialize_size();
        } else {
          len += ObEmptyExpr::instance().get_serialize_size();
        }
      }
    }
  }
  int64_t frame_count = 0;
  char **frames = nullptr;
  if (ctx.get_ori_frame_cnt() != 0 && ctx.get_ori_frames() != nullptr) {
    frame_count = ctx.get_ori_frame_cnt();
    frames = ctx.get_ori_frames();
  } else {
    frame_count = ctx.get_frame_cnt();
    frames = ctx.get_frames();
  }
  OB_UNIS_ADD_LEN(frame_count);
  len += get_serialize_frame_info_size<SERIALIZE_PLAN_PART>(expr_frame_info.const_frame_, frames, frame_count);
  len += get_serialize_frame_info_size<SERIALIZE_PLAN_PART>(expr_frame_info.param_frame_, frames, frame_count);
  len += get_serialize_frame_info_size<SERIALIZE_PLAN_PART>(expr_frame_info.dynamic_frame_, frames, frame_count);
  len += get_serialize_frame_info_size<SERIALIZE_PLAN_PART>(expr_frame_info.datum_frame_, frames, frame_count, true);
  LOG_DEBUG("trace end get ser expr frame info size", K(ret), K(len));
  return len;
}

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_PX_OB_PX_TREE_SERIALIZER_IMPL_IPP_ */
