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

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/ob_das_task.h"
#include "sql/das/ob_das_deserialize_cache.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_scan_ctdef_compact.h"
#include "sql/engine/expr/ob_expr_ser_compact.h"
#include "sql/engine/ob_ser_compact_bindings.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
namespace sql
{
int ObDASRemoteInfo::serialize_cacheable_ctdef_expr(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObExprSerializeArrayGuard expr_array_guard(has_expr_, frame_info_);
  const bool request_v4 = serialization::using_v4_encoding();
  const bool compact_expr = request_v4 && has_expr_;

  if (has_expr_ && OB_ISNULL(frame_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame info is null for cacheable DAS expr encode", K(ret));
  } else if (has_expr_ && OB_FAIL(encode_das_expr_frame_head(buf, buf_len, pos, *exec_ctx_, *frame_info_))) {
    LOG_WARN("serialize compact DAS expr frame head failed", K(ret));
  }

  const int64_t ctdef_expr_len_pos = pos;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(reserve_len_prefix(buf, buf_len, pos))) {
    LOG_WARN("reserve ctdef expr length prefix failed", K(ret), K(pos), K(buf_len));
  }
  const int64_t ctdef_expr_begin = pos;

  if (OB_SUCC(ret) && has_expr_ && OB_FAIL(encode_das_exprs(buf, buf_len, pos, *frame_info_))) {
    LOG_WARN("serialize cacheable DAS expression members failed", K(ret));
  }

  OB_UNIS_ENCODE(ctdefs_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < ctdefs_.count(); ++i) {
    const ObDASBaseCtDef *ctdef = ctdefs_.at(i);
    OB_UNIS_ENCODE(ctdef->op_type_);
    if (request_v4 && DAS_OP_TABLE_SCAN == ctdef->op_type_) {
      if (OB_FAIL(das_compact::serialize_scan_ctdef(static_cast<const ObDASScanCtDef &>(*ctdef), buf, buf_len, pos,
                                                    force_inline_ctdef_))) {
        LOG_WARN("serialize compact DAS ScanCtDef failed", K(ret), K(i));
      }
    } else {
      OB_UNIS_ENCODE(*ctdef);
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ctdefs_.count(); ++i) {
    const ObDASBaseCtDef *ctdef = ctdefs_.at(i);
    OB_UNIS_ENCODE(ctdef->children_cnt_);
    for (int64_t j = 0; OB_SUCC(ret) && j < ctdef->children_cnt_; ++j) {
      const ObDASBaseCtDef *child_ctdef = ctdef->children_[j];
      OB_UNIS_ENCODE(child_ctdef);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fill_len_prefix(buf, ctdef_expr_len_pos, pos - ctdef_expr_begin))) {
    LOG_WARN("fill ctdef expr length prefix failed", K(ret), K(ctdef_expr_len_pos), K(pos));
  }

  if (OB_SUCC(ret) && has_expr_
      && OB_FAIL(encode_expr_frame_tail(buf, buf_len, pos, *exec_ctx_, *frame_info_,
                                            compact_expr && frame_info_->rt_exprs_.count() > 0))) {
    LOG_WARN("serialize DAS expression frame tail failed", K(ret));
  }
  return ret;
}

int ObDASRemoteInfo::deserialize_cacheable_ctdef_expr(const char *buf, int64_t data_len, int64_t &pos,
                                                      ObEvalCtx *&eval_ctx, ObIArray<ObExpr> *&active_exprs,
                                                      SerCompactState &ser_compact_state)
{
  int ret = OB_SUCCESS;
  active_exprs = nullptr;
  ser_compact_state = SerCompactState::UNKNOWN;
  if (has_expr_ && OB_ISNULL(frame_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame info is null for DAS expr decode", K(ret));
  } else if (has_expr_ && OB_FAIL(decode_das_expr_frame_head(buf, data_len, pos, *exec_ctx_, *frame_info_))) {
    LOG_WARN("deserialize DAS expr frame head failed", K(ret));
  }
  int64_t blob_len = 0;
  if (OB_SUCC(ret) && OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &blob_len))) {
    LOG_WARN("decode CtDef/expr blob length failed", K(ret));
  }
  const int64_t blob_begin = pos;
  if (OB_SUCC(ret) && (blob_len < 0 || pos > data_len || blob_len > data_len - pos)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid CtDef/expr blob length", K(ret), K(blob_len), K(pos), K(data_len));
  }
  const int64_t blob_end = OB_SUCC(ret) ? blob_begin + blob_len : pos;
  UnisRecordView first_record;
  bool has_first_record = false;
  bool compact_layout = false;
  if (OB_SUCC(ret) && has_expr_) {
    int64_t first_pos = blob_begin;
    int32_t expr_cnt = 0;
    if (OB_FAIL(serialization::decode_i32(buf, blob_end, first_pos, &expr_cnt))) {
      LOG_WARN("decode cached expr count failed", K(ret));
    } else if (OB_UNLIKELY(expr_cnt < 0)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("invalid cached expr count", K(ret), K(expr_cnt));
    } else if (expr_cnt > 0) {
      if (OB_FAIL(first_record.decode(buf, blob_end, first_pos))) {
        LOG_WARN("decode first cached expr header failed", K(ret));
      } else if (first_record.version() < 1 || first_record.version() > serialization::SER_COMPACT_OPT_VERSION) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported cached expr version", K(ret), K(first_record.version()));
      } else {
        has_first_record = true;
        compact_layout = first_record.version() == serialization::SER_COMPACT_OPT_VERSION;
      }
    }
  }
  ObDASExprFrameLayout layout;
  ObString layout_key;
  int64_t payload_pos = blob_end;
  if (OB_SUCC(ret) && compact_layout) {
    if (OB_FAIL(layout.deserialize_meta(buf, data_len, payload_pos, *frame_info_))) {
      LOG_WARN("decode cached expression layout failed", K(ret));
    } else {
      layout_key.assign_ptr(buf + blob_end, static_cast<int32_t>(payload_pos - blob_end));
    }
  }
  bool cache_hit = false;
  if (OB_SUCC(ret)
      && OB_FAIL(deser_ctdef_expr_with_cache(*this, buf, data_len, pos, blob_begin, blob_len, active_exprs, cache_hit,
                                             ser_compact_state, layout_key,
                                             has_first_record ? &first_record : nullptr))) {
    LOG_WARN("deserialize cacheable CtDef/expr failed", K(ret));
  }
  {
    ObExprSerializeArrayGuard active_guard(has_expr_ ? active_exprs : nullptr);
    if (OB_SUCC(ret)
        && (pos != blob_end || (has_expr_ && OB_ISNULL(active_exprs))
            || (has_first_record && compact_layout != (ser_compact_state == SerCompactState::V4)))) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("inconsistent cached expression segment", K(ret), K(pos), K(blob_end));
    }
    if (OB_SUCC(ret)) { pos = payload_pos; }
    if (OB_SUCC(ret) && has_expr_
        && OB_FAIL(decode_expr_frame_data(buf, data_len, pos, *exec_ctx_, *frame_info_, layout, compact_layout))) {
      LOG_WARN("decode cached expression frame data failed", K(ret));
    }
    if (OB_SUCC(ret) && !cache_hit && compact_layout
        && OB_FAIL(replay_expr_frame_layout(
          *active_exprs, frame_info_->const_frame_, frame_info_->param_frame_, frame_info_->dynamic_frame_,
          frame_info_->datum_frame_, layout.cg_batch_size_, layout.original_param_cnt_, layout.partial_frame_))) {
      LOG_WARN("replay cached expression layout failed", K(ret));
    }
    if (OB_SUCC(ret) && has_expr_) {
      if (OB_FAIL(exec_ctx_->init_expr_op(active_exprs->count()))) {
        LOG_WARN("initialize cached expression operators failed", K(ret));
      } else if (OB_ISNULL(eval_ctx = OB_NEWx(ObEvalCtx, (&exec_ctx_->get_allocator()), (*exec_ctx_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < active_exprs->count(); ++i) {
        const ObExpr &expr = active_exprs->at(i);
        if (expr.is_const_expr() && UINT32_MAX != expr.vector_header_off_ && T_OP_ROW != expr.type_
            && T_INVALID != expr.type_ && OB_FAIL(expr.init_vector(*eval_ctx, VEC_UNIFORM_CONST, 1))) {
          LOG_WARN("initialize cached constant vector failed", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !cache_hit) {
    ObDasDeserializeCacheObj *obj = static_cast<ObDasDeserializeCacheObj *>(ctdef_cache_obj_guard_.get_cache_obj());
    ObDasCtdefExprCacheVal *val = OB_NOT_NULL(obj) ? obj->get_ctdef_expr_value() : nullptr;
    if (OB_ISNULL(val)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(val->ctdefs_.assign(ctdefs_))) {
      LOG_WARN("retain staged CtDef references failed", K(ret));
    } else {
      pending_ctdef_expr_begin_ = blob_begin;
      pending_ctdef_expr_len_ = blob_len;
      pending_ctdef_layout_ = layout_key;
      pending_ctdef_expr_cache_miss_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    ctdefs_.reuse();
    active_exprs = nullptr;
    ser_compact_state = SerCompactState::UNKNOWN;
    if (!cache_hit) { release_cached_ctdef_obj(); }
  }
  return ret;
}

int ObDASRemoteInfo::decode_cacheable_ctdef_expr(const char *buf, const int64_t data_len, int64_t &pos,
                                                 ObIAllocator &ctdef_alloc, ObIArray<ObExpr> &exprs,
                                                 SerCompactState &ser_compact_state,
                                                 const UnisRecordView *first_record)
{
  int ret = OB_SUCCESS;
  ser_compact_state = SerCompactState::UNKNOWN;
  ctdefs_.reuse();
  ObExprSerializeArrayGuard expr_array_guard(has_expr_ ? &exprs : nullptr);
  SerCompactState state = SerCompactState::UNKNOWN;
  serialization::CompactExprPtrGuard request_guard(false);

  if (has_expr_) {
    int32_t expr_cnt = 0;
    if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &expr_cnt))) {
      LOG_WARN("decode DAS expr count failed", K(ret));
    } else if (OB_UNLIKELY(expr_cnt < 0)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("invalid DAS expr count", K(ret), K(expr_cnt));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(exprs.prepare_allocate(expr_cnt))) {
      LOG_WARN("allocate DAS expr array failed", K(ret), K(expr_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_cnt; ++i) {
      ObExprCompactHeader header(exprs.at(i));
      DEF_UNIS_VERSION_DECODE_REC(record, exprs.at(i), header);
      if (i == 0 && OB_NOT_NULL(first_record)) {
        OB_UNIS_DECODE_PARSED(record, *first_record);
      } else {
        OB_UNIS_DECODE(record);
      }
      if (OB_FAIL(ret)) {
      } else if (!record.decoded()) {
        ret = OB_DESERIALIZE_ERROR;
      } else {
        const SerCompactState decoded_state = record.version() == ObExprCompactHeader::unis_declared_version() ?
                                                SerCompactState::V4 :
                                                SerCompactState::LEGACY;
        if (SerCompactState::UNKNOWN != state && state != decoded_state) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("inconsistent cached Expr serialization mode", K(ret), K(i), K(state), K(decoded_state));
        } else {
          state = decoded_state;
        }
      }
    }
    const bool compact_expr_v4 = SerCompactState::V4 == state;
    request_guard.set(compact_expr_v4);
  }

  {
    int64_t ctdef_cnt = 0;
    const das_compact::ObScanCtDefDecodeCtx scan_ctdef_decode_ctx = {exec_ctx_};
    if (OB_SUCC(ret) && OB_FAIL(serialization::decode(buf, data_len, pos, ctdef_cnt))) {
      LOG_WARN("decode DAS CtDef count failed", K(ret));
    }
    if (OB_SUCC(ret)
        && OB_UNLIKELY(ctdef_cnt < 0
                       || ctdef_cnt > das::OB_DAS_MAX_PACKET_SIZE / static_cast<int64_t>(sizeof(ObDASBaseCtDef *)))) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("invalid DAS CtDef count", K(ret), K(ctdef_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctdef_cnt; ++i) {
      ObDASBaseCtDef *ctdef = nullptr;
      ObDASOpType op_type = DAS_OP_INVALID;
      if (OB_FAIL(serialization::decode(buf, data_len, pos, op_type))) {
        LOG_WARN("decode DAS CtDef op type failed", K(ret), K(i));
      }
      if (OB_FAIL(ret)) {
      } else {
        if (OB_FAIL(ObDASTaskFactory::create_das_ctdef(op_type, ctdef_alloc, ctdef))) {
          LOG_WARN("create DAS CtDef failed", K(ret), K(op_type), K(i));
        }
      }
      if (OB_SUCC(ret) && DAS_OP_TABLE_SCAN == op_type) {
        if (OB_FAIL(das_compact::deserialize_scan_ctdef(static_cast<ObDASScanCtDef &>(*ctdef), buf, data_len, pos,
                                                        &scan_ctdef_decode_ctx, &state))) {
          LOG_WARN("deserialize DAS ScanCtDef failed", K(ret), K(i));
        } else {
          request_guard.set(SerCompactState::V4 == state);
        }
      } else if (OB_SUCC(ret)) {
        if (OB_FAIL(serialization::decode(buf, data_len, pos, *ctdef))) {
          LOG_WARN("deserialize DAS CtDef failed", K(ret), K(i), K(op_type));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ctdefs_.push_back(ctdef))) {
        LOG_WARN("stage DAS CtDef failed", K(ret), K(i));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ctdefs_.count(); ++i) {
      ObDASBaseCtDef *ctdef = const_cast<ObDASBaseCtDef *>(ctdefs_.at(i));
      if (OB_FAIL(serialization::decode(buf, data_len, pos, ctdef->children_cnt_))) {
        LOG_WARN("decode DAS CtDef child count failed", K(ret), K(i));
      } else if (OB_UNLIKELY(ctdef->children_cnt_ < 0 || ctdef->children_cnt_ > ctdefs_.count())) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("invalid DAS CtDef child count", K(ret), K(i), K(ctdef->children_cnt_), K(ctdefs_.count()));
      } else if (ctdef->children_cnt_ > 0) {
        if (OB_ISNULL(ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef *, &ctdef_alloc, ctdef->children_cnt_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate ctdef children_ failed", K(ret), K(ctdef->children_cnt_));
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < ctdef->children_cnt_; ++j) {
        const ObDASBaseCtDef *child_ctdef = nullptr;
        if (OB_FAIL(serialization::decode(buf, data_len, pos, child_ctdef))) {
          LOG_WARN("decode DAS child CtDef failed", K(ret), K(i), K(j));
        } else {
          ctdef->children_[j] = const_cast<ObDASBaseCtDef *>(child_ctdef);
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(pos != data_len)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("DAS CtDef/expr blob length mismatch", K(ret), K(pos), K(data_len));
  }
  if (OB_FAIL(ret)) {
    exprs.reuse();
    ctdefs_.reuse();
  } else {
    ser_compact_state = state;
  }
  return ret;
}

int64_t ObDASRemoteInfo::get_cacheable_ctdef_expr_serialize_size() const
{
  int64_t len = 0;
  OB_ASSERT(!has_expr_ || OB_NOT_NULL(frame_info_));
  if (has_expr_ && OB_ISNULL(frame_info_)) { return 0; }
  ObExprSerializeArrayGuard expr_array_guard(has_expr_, frame_info_);
  const bool request_v4 = serialization::using_v4_encoding();
  const bool compact_expr = request_v4 && has_expr_;
  len += serialization::OB_SERIALIZE_SIZE_NEED_BYTES;

  if (has_expr_) { len += das_expr_frame_head_size(*exec_ctx_, *frame_info_); }

  if (has_expr_) { len += das_exprs_size(*frame_info_); }

  OB_UNIS_ADD_LEN(ctdefs_.count());
  for (int64_t i = 0; i < ctdefs_.count(); ++i) {
    const ObDASBaseCtDef *ctdef = ctdefs_.at(i);
    OB_UNIS_ADD_LEN(ctdef->op_type_);
    if (request_v4 && DAS_OP_TABLE_SCAN == ctdef->op_type_) {
      len += das_compact::scan_ctdef_size(static_cast<const ObDASScanCtDef &>(*ctdef), force_inline_ctdef_);
    } else {
      OB_UNIS_ADD_LEN(*ctdef);
    }
  }

  for (int64_t i = 0; i < ctdefs_.count(); ++i) {
    const ObDASBaseCtDef *ctdef = ctdefs_.at(i);
    OB_UNIS_ADD_LEN(ctdef->children_cnt_);
    for (int64_t j = 0; j < ctdef->children_cnt_; ++j) {
      const ObDASBaseCtDef *child_ctdef = ctdef->children_[j];
      OB_UNIS_ADD_LEN(child_ctdef);
    }
  }

  if (has_expr_) {
    len += expr_frame_tail_size(*exec_ctx_, *frame_info_, compact_expr && frame_info_->rt_exprs_.count() > 0);
  }

  return len;
}

} // namespace sql
} // namespace oceanbase
