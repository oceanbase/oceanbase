/**
 * Copyright (c) 2024 OceanBase
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
#include "sql/engine/px/datahub/components/ob_dh_join_filter_count_row.h"
#include "sql/engine/px/ob_px_scheduler.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

OB_SERIALIZE_MEMBER(ObJoinFilterNdv, valid_, count_);
OB_SERIALIZE_MEMBER((ObJoinFilterCountRowPieceMsg, ObDatahubPieceMsg), each_sqc_has_full_data_, sqc_id_,
                    total_rows_, ndv_info_);
OB_SERIALIZE_MEMBER((ObJoinFilterCountRowWholeMsg, ObDatahubWholeMsg), total_rows_, ndv_info_);

void ObJoinFilterNdv::gather_piece_ndv(const ObJoinFilterNdv &piece_ndv, ObJoinFilterNdv &total_ndv)
{
  if (!piece_ndv.valid_) {
    // if one thread's ndv is not valid, the global ndv info is invalid
    total_ndv.valid_ = false;
    total_ndv.count_ = 0;
  } else if (!total_ndv.valid_) {
  } else {
    total_ndv.count_ += piece_ndv.count_;
  }
}

int ObJoinFilterCountRowPieceMsgListener::on_message(ObJoinFilterCountRowPieceMsgCtx &piece_ctx,
                                                     common::ObIArray<ObPxSqcMeta *> &sqcs,
                                                     const ObJoinFilterCountRowPieceMsg &pkt)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("receive a piece msg", K(pkt));
  if (pkt.op_id_ != piece_ctx.op_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece msg", K(ret), K(pkt), K(piece_ctx));
  } else if (piece_ctx.received_ >= piece_ctx.task_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("receive too much piece msg", K(pkt), K(piece_ctx.received_), K(piece_ctx.task_cnt_));
  } else if (FALSE_IT(piece_ctx.received_ += pkt.piece_count_)) {
  } else if (!pkt.each_sqc_has_full_data_) {
    // if each sqc only has partial data of left, gather all k(k=dop) thread's result
    piece_ctx.total_rows_ += pkt.total_rows_;
    if (piece_ctx.ndv_info_.empty()) {
      if (OB_FAIL(piece_ctx.ndv_info_.prepare_allocate(pkt.ndv_info_.count()))) {
        LOG_WARN("failed to prepare_allocate");
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < pkt.ndv_info_.count(); ++i) {
        ObJoinFilterNdv::gather_piece_ndv(pkt.ndv_info_.at(i), piece_ctx.ndv_info_.at(i));
      }
      if (piece_ctx.task_cnt_ == piece_ctx.received_) {
        LOG_TRACE("send whole msg", K(pkt), K(piece_ctx.total_rows_));
        if (OB_FAIL(piece_ctx.send_whole_msg(sqcs))) {
          LOG_WARN("send whole msg failed", K(ret));
        }
        IGNORE_RETURN piece_ctx.reset_resource();
      }
    }
  } else {
    // for shared hash join(bc2host none shuffle way), each sqc has full data, gather result for
    // each sqc inner
    for (int64_t i = 0; i < piece_ctx.sqc_row_infos_.count() && OB_SUCC(ret); ++i) {
      if (pkt.sqc_id_ == piece_ctx.sqc_row_infos_.at(i).sqc_id_) {
        JoinFilterSqcRowInfo &sqc_row_info = piece_ctx.sqc_row_infos_.at(i);
        if (sqc_row_info.received_ >= sqc_row_info.expected_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("receive too much piece msg for one sqc", K(pkt), K(sqc_row_info));
        } else if (sqc_row_info.ndv_info_.empty()
                   && OB_FAIL(sqc_row_info.ndv_info_.prepare_allocate(pkt.ndv_info_.count()))) {
          LOG_WARN("failed to prepare_allocate");
        } else {
          sqc_row_info.received_ += pkt.piece_count_;
          sqc_row_info.total_rows_ += pkt.total_rows_;
          for (int64_t i = 0; i < pkt.ndv_info_.count(); ++i) {
            ObJoinFilterNdv::gather_piece_ndv(pkt.ndv_info_.at(i), sqc_row_info.ndv_info_.at(i));
          }
          if (sqc_row_info.expected_ == sqc_row_info.received_) {
            LOG_TRACE("send whole msg to one sqc", K(pkt), K(sqc_row_info));
            if (OB_FAIL(piece_ctx.send_whole_msg_to_one_sqc(sqcs.at(i), sqc_row_info))) {
              LOG_WARN("send whole msg failed", K(ret));
            }
          }
        }
        break;
      }
    }
    if (piece_ctx.task_cnt_ == piece_ctx.received_) {
      IGNORE_RETURN piece_ctx.reset_resource();
    }
  }
  return ret;
}

int ObJoinFilterCountRowPieceMsgCtx::alloc_piece_msg_ctx(const ObJoinFilterCountRowPieceMsg &pkt,
                                                         ObPxCoordInfo &coord_info,
                                                         ObExecContext &ctx, int64_t task_cnt,
                                                         ObPieceMsgCtx *&msg_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObPxSqcMeta *> sqcs;
  ObDfo *target_dfo = nullptr;
  ObJoinFilterCountRowPieceMsgCtx *join_filter_count_row_ctx = nullptr;
  void *buf = ctx.get_allocator().alloc(sizeof(ObJoinFilterCountRowPieceMsgCtx));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate for piece msg ctx");
  } else if (OB_FAIL(coord_info.dfo_mgr_.find_dfo_edge(pkt.target_dfo_id_, target_dfo))) {
    LOG_WARN("fail find dfo", K(pkt), K(ret));
  } else if (OB_FAIL(target_dfo->get_sqcs(sqcs))) {
    LOG_WARN("fail get qc-sqc channel for QC", K(ret));
  } else {
    msg_ctx = new (buf) ObJoinFilterCountRowPieceMsgCtx(
        pkt.op_id_, task_cnt, ctx.get_physical_plan_ctx()->get_timeout_timestamp());
    join_filter_count_row_ctx = static_cast<ObJoinFilterCountRowPieceMsgCtx *>(msg_ctx);
    if (pkt.each_sqc_has_full_data_) {
      // for shared hash join(bc2host none shuffle way), each sqc has full data, piece msg is only
      // gathered each sqc inner
      if (OB_FAIL(join_filter_count_row_ctx->sqc_row_infos_.prepare_allocate(sqcs.count()))) {
        LOG_WARN("failed to prepare_allocate sqc_row_infos_");
      } else {
        for (int64_t i = 0; i < sqcs.count() && OB_SUCC(ret); ++i) {
          if (OB_ISNULL(sqcs.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null sqc");
          } else {
            JoinFilterSqcRowInfo &sqc_row_info = join_filter_count_row_ctx->sqc_row_infos_.at(i);
            sqc_row_info.sqc_id_ = sqcs.at(i)->get_sqc_id();
            sqc_row_info.expected_ = sqcs.at(i)->get_task_count();
          }
        }
      }
    } else {
      // if each sqc only has partial data of left, no need to init sqc_row_infos_
    }
    LOG_TRACE("allocate piece msg ctx", K(pkt));
  }
  return ret;
}

int ObJoinFilterCountRowPieceMsgCtx::send_whole_msg(ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  ObJoinFilterCountRowWholeMsg whole_msg;
  whole_msg.total_rows_ = total_rows_;
  whole_msg.op_id_ = op_id_;
  if (OB_FAIL(whole_msg.ndv_info_.assign(ndv_info_))) {
    LOG_WARN("failed to assign");
  }
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    dtl::ObDtlChannel *ch = sqcs.at(idx)->get_qc_channel();
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expected", K(ret));
    } else if (OB_FAIL(ch->send(whole_msg, timeout_ts_))) {
      LOG_WARN("fail push data to channel", K(ret));
    } else if (OB_FAIL(ch->flush(true, false))) {
      LOG_WARN("fail flush dtl data", K(ret));
    } else {
      LOG_DEBUG("dispatched barrier whole msg", K(idx), K(cnt), K(whole_msg), K(*ch));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs))) {
    LOG_WARN("failed to wait response", K(ret));
  }
  return ret;
}

int ObJoinFilterCountRowPieceMsgCtx::send_whole_msg_to_one_sqc(ObPxSqcMeta *sqc,
                                                               JoinFilterSqcRowInfo &sqc_row_info)
{
  int ret = OB_SUCCESS;
  ObJoinFilterCountRowWholeMsg whole_msg;
  whole_msg.total_rows_ = sqc_row_info.total_rows_;
  whole_msg.op_id_ = op_id_;
  dtl::ObDtlChannel *ch = sqc->get_qc_channel();
  if (OB_FAIL(whole_msg.ndv_info_.assign(sqc_row_info.ndv_info_))) {
    LOG_WARN("failed to assign");
  } else if (OB_ISNULL(ch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expected", K(ret));
  } else if (OB_FAIL(ch->send(whole_msg, timeout_ts_))) {
    LOG_WARN("fail push data to channel", K(ret));
  } else if (OB_FAIL(ch->flush(true, false))) {
    LOG_WARN("fail flush dtl data", K(ret));
  } else {
    LOG_DEBUG("dispatched jf count row whole msg", K(whole_msg), K(*ch));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ch->flush())) {
    LOG_WARN("failed to wait for channel", K(ret), "peer", ch->get_peer());
  }
  return ret;
}
