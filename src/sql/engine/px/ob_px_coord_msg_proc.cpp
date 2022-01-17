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
#include "sql/engine/px/ob_sqc_ctx.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

template <typename WholeMsg>
class ObDhWholeeMsgProc {
public:
  ObDhWholeeMsgProc() = default;
  ~ObDhWholeeMsgProc() = default;
  int on_whole_msg(ObSqcCtx& sqc_ctx, const WholeMsg& pkt) const
  {
    int ret = OB_SUCCESS;
    ObPxDatahubDataProvider* p = nullptr;
    if (OB_FAIL(sqc_ctx.get_whole_msg_provider(pkt.op_id_, p))) {
      LOG_WARN("fail get whole msg provider", K(ret));
    } else {
      typename WholeMsg::WholeMsgProvider* provider = static_cast<typename WholeMsg::WholeMsgProvider*>(p);
      if (provider->add_msg(pkt)) {
        LOG_WARN("fail set whole msg to provider", K(ret));
      }
    }
    return ret;
  }
};

int ObPxSubCoordMsgProc::on_transmit_data_ch_msg(const ObPxTransmitDataChannelMsg& pkt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sqc_ctx_.transmit_data_ch_provider_.add_msg(pkt))) {
    LOG_WARN("fail set transmit channel msg to ch provider", K(ret));
  }
  return ret;
}

int ObPxSubCoordMsgProc::on_receive_data_ch_msg(const ObPxReceiveDataChannelMsg& pkt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sqc_ctx_.receive_data_ch_provider_.add_msg(pkt))) {
    LOG_WARN("fail set receive channel msg to ch provider", K(ret));
  }
  return ret;
}

int ObPxSubCoordMsgProc::on_interrupted(const ObInterruptCode& ic) const
{
  int ret = OB_SUCCESS;
  sqc_ctx_.interrupted_ = true;
  ret = ic.code_;
  LOG_TRACE("sqc received a interrupt and throw out of msg proc", K(ic));
  return ret;
}

int ObPxSubCoordMsgProc::on_whole_msg(const ObBarrierWholeMsg& pkt) const
{
  ObDhWholeeMsgProc<ObBarrierWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, pkt);
}
int ObPxSubCoordMsgProc::on_whole_msg(const ObWinbufWholeMsg& pkt) const
{
  ObDhWholeeMsgProc<ObWinbufWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, pkt);
}
