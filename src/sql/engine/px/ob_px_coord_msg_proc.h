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

#ifndef _OB_SQL_PX_COORD_MSG_PROC_H_
#define _OB_SQL_PX_COORD_MSG_PROC_H_

#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_msg.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObBarrierWholeMsg;
class ObBarrierPieceMsg;
class ObWinbufWholeMsg;
class ObWinbufPieceMsg;
class ObRollupKeyWholeMsg;
class ObRollupKeyPieceMsg;
class ObDynamicSamplePieceMsg;
class ObDynamicSampleWholeMsg;
class ObRDWFPieceMsg;
class ObRDWFWholeMsg;
class ObInitChannelPieceMsg;
class ObInitChannelWholeMsg;
class ObReportingWFPieceMsg;
class ObReportingWFWholeMsg;
class ObOptStatsGatherPieceMsg;
class ObOptStatsGatherWholeMsg;
class SPWinFuncPXPieceMsg;
class SPWinFuncPXWholeMsg;
class RDWinFuncPXPieceMsg;
class RDWinFuncPXWholeMsg;
// 抽象出本接口类的目的是为了 MsgProc 和 ObPxCoord 解耦
class ObIPxCoordMsgProc
{
public:
  // msg processor callback
  virtual int on_sqc_init_msg(ObExecContext &ctx, const ObPxInitSqcResultMsg &pkt) = 0;
  virtual int on_sqc_finish_msg(ObExecContext &ctx, const ObPxFinishSqcResultMsg &pkt) = 0;
  virtual int on_eof_row(ObExecContext &ctx) = 0;
  virtual int on_sqc_init_fail(ObDfo &dfo, ObPxSqcMeta &sqc) = 0;
  virtual int on_interrupted(ObExecContext &ctx, const ObInterruptCode &ic) = 0;
  virtual int on_piece_msg(ObExecContext &ctx, const ObBarrierPieceMsg &pkt) = 0;
  virtual int on_piece_msg(ObExecContext &ctx, const ObWinbufPieceMsg &pkt) = 0;
  virtual int on_piece_msg(ObExecContext &ctx, const ObDynamicSamplePieceMsg &pkt) = 0;
  virtual int on_piece_msg(ObExecContext &ctx, const ObRollupKeyPieceMsg &pkt) = 0;
  virtual int on_piece_msg(ObExecContext &ctx, const ObRDWFPieceMsg &pkt) = 0;
  virtual int on_piece_msg(ObExecContext &ctx, const ObInitChannelPieceMsg &pkt) = 0;
  virtual int on_piece_msg(ObExecContext &ctx, const ObReportingWFPieceMsg &pkt) = 0;
  virtual int on_piece_msg(ObExecContext &ctx, const ObOptStatsGatherPieceMsg &pkt) = 0;
  virtual int on_piece_msg(ObExecContext &ctx, const SPWinFuncPXPieceMsg &pkt) = 0;
  virtual int on_piece_msg(ObExecContext &ctx, const RDWinFuncPXPieceMsg &pkt) = 0;
};

class ObIPxSubCoordMsgProc
{
public:
  // 收到 TransmitDataChannel 消息，通知 ObPxTransmit 可以发送数据了
  virtual int on_transmit_data_ch_msg(
      const ObPxTransmitDataChannelMsg &pkt) const = 0;
  // 收到 ReceiveDataChannel 消息，通知 ObPxReceive 可以接收数据了
  virtual int on_receive_data_ch_msg(
      const ObPxReceiveDataChannelMsg &pkt) const = 0;
  virtual int on_create_filter_ch_msg(
      const ObPxCreateBloomFilterChannelMsg &pkt) const = 0;
  virtual int on_whole_msg(
      const ObBarrierWholeMsg &pkt) const = 0;
  virtual int on_whole_msg(
      const ObWinbufWholeMsg &pkt) const = 0;
  virtual int on_whole_msg(
      const ObDynamicSampleWholeMsg &pkt) const = 0;
  virtual int on_whole_msg(
      const ObRollupKeyWholeMsg &pkt) const = 0;
  virtual int on_whole_msg(
      const ObRDWFWholeMsg &pkt) const = 0;
  virtual int on_whole_msg(
      const ObInitChannelWholeMsg &pkt) const = 0;
  virtual int on_whole_msg(
      const ObReportingWFWholeMsg &pkt) const = 0;
  virtual int on_whole_msg(
      const ObOptStatsGatherWholeMsg &pkt) const = 0;
  virtual int on_whole_msg(
      const SPWinFuncPXWholeMsg &pkt) const = 0;
  virtual int on_whole_msg(
      const RDWinFuncPXWholeMsg &pkt) const = 0;
  // SQC 被中断
  virtual int on_interrupted(const ObInterruptCode &ic) const = 0;
};

class ObPxRpcInitSqcArgs;
class ObSqcCtx;
class ObPxSubCoordMsgProc : public ObIPxSubCoordMsgProc
{
public:
  ObPxSubCoordMsgProc(ObPxRpcInitSqcArgs &sqc_arg, ObSqcCtx &sqc_ctx)
      : sqc_ctx_(sqc_ctx) { UNUSED(sqc_arg); }
  ~ObPxSubCoordMsgProc() = default;
  virtual int on_transmit_data_ch_msg(
      const ObPxTransmitDataChannelMsg &pkt) const;
  virtual int on_receive_data_ch_msg(
      const ObPxReceiveDataChannelMsg &pkt) const;
  virtual int on_create_filter_ch_msg(
      const ObPxCreateBloomFilterChannelMsg &pkt) const;
  virtual int on_interrupted(
      const common::ObInterruptCode &pkt) const;
  virtual int on_whole_msg(
      const ObBarrierWholeMsg &pkt) const;
  virtual int on_whole_msg(
      const ObWinbufWholeMsg &pkt) const;
  virtual int on_whole_msg(
      const ObDynamicSampleWholeMsg &pkt) const;
  virtual int on_whole_msg(
      const ObRollupKeyWholeMsg &pkt) const;
  virtual int on_whole_msg(
      const ObRDWFWholeMsg &pkt) const;
  virtual int on_whole_msg(
      const ObInitChannelWholeMsg &pkt) const;
  virtual int on_whole_msg(
      const ObReportingWFWholeMsg &pkt) const;
  virtual int on_whole_msg(
      const ObOptStatsGatherWholeMsg &pkt) const;
   virtual int on_whole_msg(
      const SPWinFuncPXWholeMsg &pkt) const;
   virtual int on_whole_msg(
      const RDWinFuncPXWholeMsg &pkt) const;
 private:
   ObSqcCtx &sqc_ctx_;
};

}
}


#endif
