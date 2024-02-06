/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef __OB_SQL_ENG_PX_DH_INIT_CHANNEL_H__
#define __OB_SQL_ENG_PX_DH_INIT_CHANNEL_H__
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "share/ob_cluster_version.h"
namespace oceanbase
{
namespace sql
{
class ObInitChannelPieceMsg;
class ObInitChannelWholeMsg;
typedef ObPieceMsgP<ObInitChannelPieceMsg> ObInitChannelPieceMsgP;
typedef ObWholeMsgP<ObInitChannelWholeMsg> ObInitChannelWholeMsgP;
class ObInitChannelPieceMsgListener;
class ObInitChannelPieceMsgCtx;
class ObPxCoordInfo;
class ObInitChannelPieceMsg
  : public ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_INIT_CHANNEL_PIECE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using PieceMsgListener = ObInitChannelPieceMsgListener;
  using PieceMsgCtx = ObInitChannelPieceMsgCtx;
public:
  ObInitChannelPieceMsg() : piece_count_(0) {}
  ~ObInitChannelPieceMsg() = default;
  void reset()
  {
    op_id_ = OB_INVALID_ID;
    source_dfo_id_ = OB_INVALID_ID;
    thread_id_ = 0;
    target_dfo_id_ = OB_INVALID_ID;
    child_dfo_ = nullptr;
    ATOMIC_SET(&piece_count_, 0);
  }
  //VIRTUAL_TO_STRING_KV("piece_count", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_INIT_CHANNEL_PIECE_MSG>);
public:
  int64_t channel_id_;
  int64_t piece_count_;
  DISALLOW_COPY_AND_ASSIGN(ObInitChannelPieceMsg);
};
class ObInitChannelWholeMsg
    : public ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_INIT_CHANNEL_WHOLE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider = ObWholeMsgProvider<ObInitChannelWholeMsg>;
public:
  ObInitChannelWholeMsg() : ready_state_(0)
  {}
  ~ObInitChannelWholeMsg() = default;
  int assign(const ObInitChannelWholeMsg &other, common::ObIAllocator *allocator = NULL);
  void reset()
  {
    ready_state_ = 0;
  }
  VIRTUAL_TO_STRING_KV(K_(ready_state), K_(op_id));
  int ready_state_; // not used now
};
class ObInitChannelPieceMsgCtx : public ObPieceMsgCtx
{
public:
  ObInitChannelPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts, int64_t tenant_id)
    : ObPieceMsgCtx(op_id, task_cnt, timeout_ts), received_(0),
                    tenant_id_(tenant_id)/*, whole_msg_()*/ {}
  ~ObInitChannelPieceMsgCtx() = default;
  INHERIT_TO_STRING_KV("meta", ObPieceMsgCtx, K_(received));
  virtual int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) override;
  virtual void reset_resource() override;
  static int alloc_piece_msg_ctx(const ObInitChannelPieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);
  static bool enable_dh_channel_sync(const bool channel_sync_enabled);
  int received_; // 已经收到的 piece 数量
  int64_t tenant_id_;
  ObInitChannelWholeMsg whole_msg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObInitChannelPieceMsgCtx);
};
class ObInitChannelPieceMsgListener
{
public:
  ObInitChannelPieceMsgListener() = default;
  ~ObInitChannelPieceMsgListener() = default;
  static int on_message(
      ObInitChannelPieceMsgCtx &ctx,
      common::ObIArray<ObPxSqcMeta *> &sqcs,
      const ObInitChannelPieceMsg &pkt);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObInitChannelPieceMsgListener);
};

}
}
#endif /* __OB_SQL_ENG_PX_DH_INIT_CHANNEL_H__ */
