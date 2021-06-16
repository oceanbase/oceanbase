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

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObBarrierWholeMsg;
class ObBarrierPieceMsg;
class ObWinbufWholeMsg;
class ObWinbufPieceMsg;
class ObIPxCoordMsgProc {
public:
  // msg processor callback
  virtual int on_sqc_init_msg(ObExecContext& ctx, const ObPxInitSqcResultMsg& pkt) = 0;
  virtual int on_sqc_finish_msg(ObExecContext& ctx, const ObPxFinishSqcResultMsg& pkt) = 0;
  virtual int on_eof_row(ObExecContext& ctx) = 0;
  virtual int on_sqc_init_fail(ObDfo& dfo, ObPxSqcMeta& sqc) = 0;
  virtual int on_interrupted(ObExecContext& ctx, const ObInterruptCode& ic) = 0;
  virtual int on_piece_msg(ObExecContext& ctx, const ObBarrierPieceMsg& pkt) = 0;
  virtual int on_piece_msg(ObExecContext& ctx, const ObWinbufPieceMsg& pkt) = 0;
};

class ObIPxSubCoordMsgProc {
public:
  virtual int on_transmit_data_ch_msg(const ObPxTransmitDataChannelMsg& pkt) const = 0;
  virtual int on_receive_data_ch_msg(const ObPxReceiveDataChannelMsg& pkt) const = 0;
  virtual int on_whole_msg(const ObBarrierWholeMsg& pkt) const = 0;
  virtual int on_whole_msg(const ObWinbufWholeMsg& pkt) const = 0;
  virtual int on_interrupted(const ObInterruptCode& ic) const = 0;
};

class ObPxRpcInitSqcArgs;
class ObSqcCtx;
class ObPxSubCoordMsgProc : public ObIPxSubCoordMsgProc {
public:
  ObPxSubCoordMsgProc(ObPxRpcInitSqcArgs& sqc_arg, ObSqcCtx& sqc_ctx) : sqc_arg_(sqc_arg), sqc_ctx_(sqc_ctx)
  {}
  ~ObPxSubCoordMsgProc() = default;
  virtual int on_transmit_data_ch_msg(const ObPxTransmitDataChannelMsg& pkt) const;
  virtual int on_receive_data_ch_msg(const ObPxReceiveDataChannelMsg& pkt) const;
  virtual int on_interrupted(const common::ObInterruptCode& pkt) const;
  virtual int on_whole_msg(const ObBarrierWholeMsg& pkt) const;
  virtual int on_whole_msg(const ObWinbufWholeMsg& pkt) const;

private:
  ObPxRpcInitSqcArgs& sqc_arg_;
  ObSqcCtx& sqc_ctx_;
};

// update the error code if it is OB_HASH_NOT_EXIST or OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER
OB_INLINE void update_error_code(int& current_error_code, const int new_error_code)
{
  if (new_error_code != ObPxTask::TASK_DEFAULT_RET_VALUE) {
    if ((OB_SUCCESS == current_error_code) || ((OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER == current_error_code ||
                                                   OB_GOT_SIGNAL_ABORTING == current_error_code) &&
                                                  OB_SUCCESS != new_error_code)) {
      current_error_code = new_error_code;
    }
  }
}
}  // namespace sql
}  // namespace oceanbase

#endif
