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

#ifndef _OB_SQL_DH_DTL_PROC_H_
#define _OB_SQL_DH_DTL_PROC_H_

#include "sql/dtl/ob_dtl_processor.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"

namespace oceanbase
{
namespace sql
{

class ObPxCoordMsgProc;
class ObExecContext;
class ObIPxSubCoordMsgProc;
class ObIPxCoordMsgProc;
////////////////////////////  FOR QC ////////////////////////////

template <typename PieceMsg>
class ObPieceMsgP : public dtl::ObDtlPacketProc<PieceMsg>
{
public:
  ObPieceMsgP(ObExecContext &ctx, ObIPxCoordMsgProc &msg_proc)
      : ctx_(ctx), msg_proc_(msg_proc) {}
  virtual ~ObPieceMsgP() = default;
  int process(const PieceMsg &pkt) override
  {
    // FIXME on_piece_msg理论上可以模板化处理.
    // 暂时通过重载绕过.
    return msg_proc_.on_piece_msg(ctx_, pkt);
  }
private:
  ObExecContext &ctx_;
  ObIPxCoordMsgProc &msg_proc_;
};


////////////////////////////  FOR SQC ////////////////////////////
template <typename WholeMsg>
class ObWholeMsgP : public dtl::ObDtlPacketProc<WholeMsg>
{
public:
  ObWholeMsgP(ObIPxSubCoordMsgProc &msg_proc)
      : msg_proc_(msg_proc) {}
  virtual ~ObWholeMsgP() = default;
  int process(const WholeMsg &pkt) override
  {
    return msg_proc_.on_whole_msg(pkt);
  }
private:
  ObIPxSubCoordMsgProc &msg_proc_;
};

}
}

#endif
