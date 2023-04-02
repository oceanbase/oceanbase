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

#ifndef __OB_SQL_ENG_PX_DH_BARRIER_H__
#define __OB_SQL_ENG_PX_DH_BARRIER_H__

#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"

namespace oceanbase
{
namespace sql
{

class ObBarrierPieceMsg;
class ObBarrierWholeMsg;
typedef ObPieceMsgP<ObBarrierPieceMsg> ObBarrierPieceMsgP;
typedef ObWholeMsgP<ObBarrierWholeMsg> ObBarrierWholeMsgP;
class ObBarrierPieceMsgListener;
class ObBarrierPieceMsgCtx;
class ObPxCoordInfo;

/* 各种 datahub 子类消息定义如下 */
class ObBarrierPieceMsg
  : public ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_BARRIER_PIECE_MSG>

{
  OB_UNIS_VERSION_V(1);
public:
  using PieceMsgListener = ObBarrierPieceMsgListener;
  using PieceMsgCtx = ObBarrierPieceMsgCtx;
public:
  ObBarrierPieceMsg() = default;
  ~ObBarrierPieceMsg() = default;
  void reset()
  {
  }
  INHERIT_TO_STRING_KV("meta", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_BARRIER_PIECE_MSG>,
                       K_(op_id));
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObBarrierPieceMsg);
};


class ObBarrierWholeMsg
    : public ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_BARRIER_WHOLE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider = ObWholeMsgProvider<ObBarrierWholeMsg>;
public:
  ObBarrierWholeMsg() : ready_state_(0) {}
  ~ObBarrierWholeMsg() = default;
  int assign(const ObBarrierWholeMsg &other)
  {
    ready_state_ = other.ready_state_;
    return common::OB_SUCCESS;
  }
  void reset()
  {
    ready_state_ = 0;
  }
  VIRTUAL_TO_STRING_KV(K_(ready_state));
  int ready_state_; // 占位符，并不真用到
};

class ObBarrierPieceMsgCtx : public ObPieceMsgCtx
{
public:
  ObBarrierPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts)
    : ObPieceMsgCtx(op_id, task_cnt, timeout_ts), received_(0) {}
  ~ObBarrierPieceMsgCtx() = default;
  virtual int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) override;
  virtual void reset_resource() override;
  static int alloc_piece_msg_ctx(const ObBarrierPieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);
  INHERIT_TO_STRING_KV("meta", ObPieceMsgCtx, K_(received));
  int received_; // 已经收到的 piece 数量
private:
  DISALLOW_COPY_AND_ASSIGN(ObBarrierPieceMsgCtx);
};

class ObBarrierPieceMsgListener
{
public:
  ObBarrierPieceMsgListener() = default;
  ~ObBarrierPieceMsgListener() = default;
  static int on_message(
      ObBarrierPieceMsgCtx &ctx,
      common::ObIArray<ObPxSqcMeta *> &sqcs,
      const ObBarrierPieceMsg &pkt);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObBarrierPieceMsgListener);
};

}
}
#endif /* __OB_SQL_ENG_PX_DH_BARRIER_H__ */
//// end of header file

