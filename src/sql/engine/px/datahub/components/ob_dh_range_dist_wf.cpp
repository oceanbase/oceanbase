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

#include "sql/engine/px/datahub/components/ob_dh_range_dist_wf.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/window_function/ob_window_function_op.h"

namespace oceanbase
{
namespace sql
{

OB_DEF_SERIALIZE_SIZE(ObRDWFPartialInfo)
{
  int64_t len = 0;
  auto size_func = [&](ObStoredDatumRow *row) {
    int64_t row_size = NULL == row ? 0 : row->row_size_;
    OB_UNIS_ADD_LEN(row_size);
    if (NULL != row) {
      len += row->row_size_;
    }
  };

  LST_DO_CODE(OB_UNIS_ADD_LEN, sqc_id_, thread_id_);
  size_func(first_row_);
  size_func(last_row_);

  return len;
}

OB_DEF_SERIALIZE(ObRDWFPartialInfo)
{
  int ret = OB_SUCCESS;
  auto ser_row_func = [&](ObStoredDatumRow *row) {
    int64_t row_size = NULL == row ? 0 : row->row_size_;
    OB_UNIS_ENCODE(row_size);
    if (OB_SUCC(ret) && NULL != row) {
      if (buf_len - pos < row->row_size_) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        MEMCPY(buf + pos, row, row->row_size_);
        reinterpret_cast<ObStoredDatumRow *>(buf + pos)->unswizzling(
            reinterpret_cast<char *>(row));
        pos += row->row_size_;
      }
    }
  };

  LST_DO_CODE(OB_UNIS_ENCODE, sqc_id_, thread_id_);
  ser_row_func(first_row_);
  ser_row_func(last_row_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRDWFPartialInfo)
{
  int ret = OB_SUCCESS;
  auto desc_row_func = [&](ObStoredDatumRow *&row) {
    int64_t row_size = 0;
    OB_UNIS_DECODE(row_size);
    if (OB_SUCC(ret) && row_size > 0) {
      auto src = reinterpret_cast<const ObStoredDatumRow *>(buf + pos);
      row = static_cast<ObStoredDatumRow *>(alloc_.alloc(src->row_size_));
      if (NULL == row) {
        LOG_WARN("allocate memory failed", K(ret));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(row, src, src->row_size_);
        row->swizzling();
        if (row_size != src->row_size_
            ||data_len < pos + row_size) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected size", K(ret), K(pos), K(data_len), K(row_size), K(src->row_size_));
        } else {
          pos += src->row_size_;
        }
      }
    }
  };

  LST_DO_CODE(OB_UNIS_DECODE, sqc_id_, thread_id_);
  desc_row_func(first_row_);
  desc_row_func(last_row_);

  return ret;
}

OB_SERIALIZE_MEMBER((ObRDWFPieceMsg, ObRDWFPieceMsgBase), info_);

OB_DEF_SERIALIZE_SIZE(ObRDWFWholeMsg)
{
  int64_t len = ObRDWFWholeMsgBase::get_serialize_size();
  int64_t cnt = infos_.count();
  OB_UNIS_ADD_LEN(cnt);
  FOREACH_CNT(info, infos_) {
    OB_UNIS_ADD_LEN((**info));
  }
  return len;
}

OB_DEF_SERIALIZE(ObRDWFWholeMsg)
{
  int ret = ObRDWFWholeMsgBase::serialize(buf, buf_len, pos);
  if (OB_SUCC(ret)) {
    int64_t cnt = infos_.count();
    OB_UNIS_ENCODE(cnt);
    FOREACH_CNT_X(info, infos_, OB_SUCC(ret)) {
      OB_UNIS_ENCODE(**info);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObRDWFWholeMsg)
{
  int ret = ObRDWFWholeMsgBase::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    int64_t cnt = 0;
    OB_UNIS_DECODE(cnt);
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
      ObRDWFPartialInfo *info = OB_NEWx(ObRDWFPartialInfo, (&arena_alloc_), arena_alloc_);
      if (NULL == info) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        OB_UNIS_DECODE(*info);
        OZ(infos_.push_back(info));
      }
    }
  }
  return ret;
}

ObStoredDatumRow *ObRDWFPartialInfo::dup_store_row(common::ObArenaAllocator &alloc,
                                                   const ObStoredDatumRow &row)
{
  ObStoredDatumRow *s = static_cast<ObStoredDatumRow *>(alloc.alloc(row.row_size_));
  if (NULL != s) {
    MEMCPY(s, &row, row.row_size_);
    s->unswizzling(reinterpret_cast<char *>(const_cast<ObStoredDatumRow *>(&row)));
    s->swizzling();
  }
  return s;
}

ObRDWFPartialInfo *ObRDWFPartialInfo::dup(common::ObArenaAllocator &alloc) const
{
  int ret = OB_SUCCESS;
  ObRDWFPartialInfo *info = OB_NEWx(ObRDWFPartialInfo, (&alloc), alloc);
  if (NULL == info) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc memory failed", K(ret));
  } else {
    info->sqc_id_ = sqc_id_;
    info->thread_id_ = thread_id_;
    if (NULL != first_row_) {
      info->first_row_ = dup_store_row(alloc, *first_row_);
      OV(NULL != info->first_row_, OB_ALLOCATE_MEMORY_FAILED);
    }
    if (NULL != last_row_ && OB_SUCC(ret)) {
      info->last_row_ = dup_store_row(alloc, *last_row_);
      OV(NULL != info->last_row_, OB_ALLOCATE_MEMORY_FAILED);
    }
  }
  return OB_SUCCESS == ret ? info : NULL;
}

int ObRDWFPieceMsgCtx::alloc_piece_msg_ctx(const ObRDWFPieceMsg &pkt,
                                           ObPxCoordInfo &,
                                           ObExecContext &ctx,
                                           int64_t task_cnt,
                                           ObPieceMsgCtx *&msg_ctx)
{
  int ret = OB_SUCCESS;
  msg_ctx = OB_NEWx(ObRDWFPieceMsgCtx,
                    (&ctx.get_allocator()),
                    pkt.op_id_,
                    task_cnt,
                    ctx.get_physical_plan_ctx()->get_timeout_timestamp(),
                    ctx);
  if (OB_ISNULL(msg_ctx)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  }
  return ret;
}

int ObRDWFPieceMsgCtx::formalize_store_row()
{
  int ret = OB_SUCCESS;
  auto formalize = [&](ObStoredDatumRow *&s) {
    typedef ObRDWFPartialInfo::RowExtType ExtType;
    int64_t extra_payload_size = sizeof(ExtType);
    if (NULL != s) {
      int64_t size = ObChunkDatumStore::Block::row_store_size(
          s->cells(), s->cnt_, extra_payload_size);
      auto ns = static_cast<ObStoredDatumRow *>(arena_alloc_.alloc(size));
      if (NULL == ns) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(size));
      } else {
        *ns = *s;
        ns->row_size_ = size;
        ns->extra_payload<ExtType>() = s->extra_payload<ExtType>();
        char *buf = (char *)ns->get_extra_payload() + extra_payload_size;
        int64_t buf_len = size - (buf - (char *)ns);
        int64_t pos = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < s->cnt_; i++) {
          OZ(ns->cells()[i].deep_copy(s->cells()[i], buf, buf_len, pos));
        }
      }
      if (OB_SUCC(ret)) {
        s = ns;
      }
    }
  };
  FOREACH_CNT_X(info, infos_, OB_SUCC(ret)) {
    formalize((*info)->first_row_);
    if (OB_SUCC(ret)) {
      formalize((*info)->last_row_);
    }
  }
  return ret;
}

int ObRDWFPieceMsgCtx::send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  ObOperatorKit *op_kit = exec_ctx_.get_operator_kit(op_id_);
  if (NULL == op_kit || NULL == op_kit->spec_ || PHY_WINDOW_FUNCTION != op_kit->spec_->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no window function operator", K(ret), KP(op_kit), K(op_id_));
  } else {
    auto wf = static_cast<const ObWindowFunctionSpec *>(op_kit->spec_);
    if (OB_FAIL(wf->rd_generate_patch(*this))) {
      LOG_WARN("calculate range distribution window function final res failed", K(ret));
    } else if (OB_FAIL(formalize_store_row())) {
      LOG_WARN("formalize store row failed", K(ret));
    } else {
      LOG_DEBUG("after formalize", K(infos_));
    }
  }
  ObRDWFWholeMsg *responses = NULL;
  if (OB_SUCC(ret)) {
    responses = static_cast<ObRDWFWholeMsg *>(
        arena_alloc_.alloc(sizeof(ObRDWFWholeMsg) * sqcs.count()));
    OV(NULL != responses, OB_ALLOCATE_MEMORY_FAILED);
    for (int64_t i = 0; OB_SUCC(ret) && i < sqcs.count(); i++) {
      new (&responses[i])ObRDWFWholeMsg();
    }
  }
  if (OB_SUCC(ret)) {
    // order by sqc_id_, thread_id_
    std::sort(infos_.begin(), infos_.end(), [](ObRDWFPartialInfo *l,
                                               ObRDWFPartialInfo *r) {
        return std::tie(l->sqc_id_, l->thread_id_) < std::tie(r->sqc_id_, r->thread_id_);
    });
    for (int64_t i = 0; OB_SUCC(ret) && i < sqcs.count(); i++) {
      auto &sqc = *sqcs.at(i);
      auto &msg = responses[i];
      msg.op_id_ = op_id_;
      auto it = std::lower_bound(infos_.begin(), infos_.end(), sqc.get_sqc_id(),
                                 [&](ObRDWFPartialInfo *info, int64_t id)
                                 { return info->sqc_id_ < id; });
      if (it == infos_.end() || (*it)->sqc_id_ != sqc.get_sqc_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sqc not found", K(ret), K(sqc));
      } else {
        while (OB_SUCC(ret) && it != infos_.end() && (*it)->sqc_id_ == sqc.get_sqc_id()) {
          OZ(msg.infos_.push_back(*it));
          it++;
        }
      }
      auto ch = sqc.get_qc_channel();
      CK(NULL != ch);
      OZ(ch->send(msg, timeout_ts_));
      OZ(ch->flush(true /* wait */, false /* wait response */));
    }
    OZ(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs));
  }
  for (int64_t i = 0; NULL != responses && i < sqcs.count(); i++) {
    responses[i].~ObRDWFWholeMsg();
  }
  return ret;
}

void ObRDWFPieceMsgCtx::reset_resource()
{
  received_ = 0;
  infos_.reset();
  arena_alloc_.reset();
}

int ObRDWFWholeMsg::assign(const ObRDWFWholeMsg &msg)
{
  int ret = OB_SUCCESS;
  op_id_ = msg.op_id_;
  FOREACH_CNT_X(info, msg.infos_, OB_SUCC(ret)) {
    CK(NULL != *info);
    auto dup_info = (*info)->dup(arena_alloc_);
    OV(NULL != dup_info, OB_ALLOCATE_MEMORY_FAILED);
    OZ(infos_.push_back(dup_info));
  }
  return ret;
}

int ObRDWFPieceMsgListener::on_message(ObRDWFPieceMsgCtx &ctx,
                                       common::ObIArray<ObPxSqcMeta *> &sqcs,
                                       const ObRDWFPieceMsg &pkt)
{
  int ret = OB_SUCCESS;
  CK(pkt.op_id_ == ctx.op_id_);
  CK(ctx.received_ < ctx.task_cnt_);
  if (OB_SUCC(ret)) {
    ctx.received_ += 1;
    LOG_TRACE("get range distribution window function piece msg", K(pkt.info_));
    ObRDWFPartialInfo *info = pkt.info_.dup(ctx.arena_alloc_);
    OV(NULL != info, OB_ALLOCATE_MEMORY_FAILED);
    OZ(ctx.infos_.push_back(info));
  }

  if (OB_SUCC(ret) && ctx.received_ == ctx.task_cnt_) {
    if (OB_FAIL(ctx.send_whole_msg(sqcs))) {
      LOG_WARN("fail to send whole msg", K(ret));
    }
    IGNORE_RETURN ctx.reset_resource();
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
